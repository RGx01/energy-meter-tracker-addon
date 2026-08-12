"""
Tests for kraken_ingester.py (Chunk 4a) — fake async client + real in-memory
BlockStore. No network.
"""

import asyncio
import unittest
from datetime import datetime, timedelta, timezone

from block_store import BlockStore
from kraken_ingester import KrakenIngester, normalise_to_naive_utc, _STATE_LAST_POLL


def run(coro):
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


class FakeClient:
    """Async client stub exposing get_consumption.

    import_rows / export_rows are lists of {consumption, interval_start,
    interval_end}. Records calls for assertions. Optionally raises.
    """

    def __init__(self, import_rows=None, export_rows=None, raise_on_import=None):
        self._import = import_rows or []
        self._export = export_rows or []
        self.calls = []
        self._raise_on_import = raise_on_import

    async def get_consumption(self, mpan, serial, *, period_from=None,
                              period_to=None, order_by="period"):
        self.calls.append((mpan, serial, period_from, period_to))
        if self._raise_on_import and mpan == self._raise_on_import:
            raise RuntimeError("boom")
        # Distinguish import vs export by which mpan was configured first.
        return self._export if mpan == "EXPORTMPAN" else self._import


class _Base(unittest.TestCase):
    def setUp(self):
        self.store = BlockStore(":memory:")
        self.store.insert_config_period({"meters": {"electricity_main": {
            "meta": {"timezone": "Europe/London", "billing_day": 1,
                     "block_minutes": 30, "currency_symbol": "£",
                     "currency_code": "GBP", "sub_meter": False},
            "standing_charge": 0.5,
            "channels": {"import": {"sensor": "sensor.i"}}}}})
        self.pid = self.store.get_current_config_period_id()

    def tearDown(self):
        self.store.close()

    def _block(self, block_start, imp_kwh=2.0):
        end = (datetime.fromisoformat(block_start) + timedelta(minutes=30)).isoformat()
        self.store._conn.execute(
            """INSERT INTO blocks (block_start, block_end, meter_id,
                 config_period_id, imp_kwh) VALUES (?,?,?,?,?)""",
            (block_start, end, "electricity_main", self.pid, imp_kwh))
        self.store._conn.commit()


class TestNormalise(unittest.TestCase):
    def test_z(self):
        self.assertEqual(normalise_to_naive_utc("2026-05-01T00:30:00Z"),
                         "2026-05-01T00:30:00")

    def test_bst_offset(self):
        self.assertEqual(normalise_to_naive_utc("2026-03-30T02:00:00+01:00"),
                         "2026-03-30T01:00:00")

    def test_bad_raises(self):
        with self.assertRaises(ValueError):
            normalise_to_naive_utc("not-a-time")


class TestWindow(_Base):
    def test_first_run_uses_backfill(self):
        ing = KrakenIngester(FakeClient(), self.store,
                             import_mpan="M", import_serial="S",
                             backfill_days=7)
        now = datetime(2026, 5, 10, 12, 0, 0, tzinfo=timezone.utc)
        frm, to = ing._compute_window(now)
        self.assertTrue(frm.startswith("2026-05-03"))  # 7 days back
        self.assertTrue(to.startswith("2026-05-10"))

    def test_incremental_uses_last_poll_with_overlap(self):
        self.store.set_kraken_state(_STATE_LAST_POLL, "2026-05-10T00:00:00")
        ing = KrakenIngester(FakeClient(), self.store,
                             import_mpan="M", import_serial="S")
        now = datetime(2026, 5, 10, 12, 0, 0, tzinfo=timezone.utc)
        frm, to = ing._compute_window(now)
        # last_poll 00:00 minus 6h overlap → previous day 18:00
        self.assertTrue(frm.startswith("2026-05-09T18:00"))

    def test_window_extends_back_to_oldest_unsettled(self):
        # An unsettled block days before last_poll (e.g. lagging export) pulls the
        # window start back to it, so the 6h poll re-fetches the lag every cycle.
        self.store.set_kraken_state(_STATE_LAST_POLL, "2026-05-10T00:00:00")
        self._block("2026-05-07T09:00:00")           # imp_kwh_api NULL → unsettled
        ing = KrakenIngester(FakeClient(), self.store,
                             import_mpan="M", import_serial="S", backfill_days=400)
        now = datetime(2026, 5, 10, 12, 0, 0, tzinfo=timezone.utc)
        frm, _to = ing._compute_window(now)
        self.assertTrue(frm.startswith("2026-05-07T09:00"))   # pulled back, not 05-09 18:00

    def test_window_floors_oldest_at_backfill_horizon(self):
        # A very old stuck-unsettled block can't blow the window past the horizon.
        self.store.set_kraken_state(_STATE_LAST_POLL, "2026-05-10T00:00:00")
        self._block("2020-01-01T00:00:00")
        ing = KrakenIngester(FakeClient(), self.store,
                             import_mpan="M", import_serial="S", backfill_days=30)
        now = datetime(2026, 5, 10, 12, 0, 0, tzinfo=timezone.utc)
        frm, _to = ing._compute_window(now)
        self.assertTrue(frm.startswith("2026-04-10"))         # now − 30d, not 2020


class TestPoll(_Base):
    def _rows(self, *triples):
        return [{"consumption": c, "interval_start": s, "interval_end": e}
                for c, s, e in triples]

    def test_empty_window_skips_fetch(self):
        # Fresh DB, no backfill → window is from==to. Octopus 400s on that, so
        # the poll must skip the API call entirely and return cleanly.
        client = FakeClient(import_rows=self._rows(
            (1.5, "2026-06-03T05:30:00Z", "2026-06-03T06:00:00Z")))
        ing = KrakenIngester(client, self.store, import_mpan="M",
                             import_serial="S", backfill_days=0)
        now = datetime(2026, 6, 3, 6, 0, 50, tzinfo=timezone.utc)
        s = run(ing.poll(now=now))
        self.assertTrue(s.get("skipped_empty_window"))
        self.assertEqual(s["import_rows"], 0)
        self.assertEqual(s["errors"], [])
        self.assertEqual(len(client.calls), 0)   # no API call made

    def test_stores_settled_figures(self):
        self._block("2026-05-01T00:00:00")
        self._block("2026-05-01T00:30:00")
        client = FakeClient(import_rows=self._rows(
            (1.5, "2026-05-01T00:00:00Z", "2026-05-01T00:30:00Z"),
            (1.8, "2026-05-01T00:30:00Z", "2026-05-01T01:00:00Z"),
        ))
        ing = KrakenIngester(client, self.store, import_mpan="M",
                             import_serial="S", billing_source="api")
        s = run(ing.poll())
        self.assertEqual(s["import_rows"], 2)
        self.assertEqual(s["stored"], 2)
        r = self.store.get_block_by_start("2026-05-01T00:30:00", "electricity_main")
        self.assertEqual(r["imp_kwh_api"], 1.8)
        self.assertEqual(r["needs_pass2_rerun"], 1)   # api → re-run queued

    def test_explicit_window_overrides_and_no_cursor_advance(self):
        # Retry path: explicit window, advance_cursor=False → settles the span
        # but does NOT move last_poll_utc.
        self._block("2026-04-01T00:00:00")
        self.store.set_kraken_state(_STATE_LAST_POLL, "2026-05-20T00:00:00")
        client = FakeClient(import_rows=self._rows(
            (2.2, "2026-04-01T00:00:00Z", "2026-04-01T00:30:00Z")))
        ing = KrakenIngester(client, self.store, import_mpan="M",
                             import_serial="S", billing_source="api")
        s = run(ing.poll(window=("2026-04-01T00:00:00Z", "2026-04-02T00:00:00Z"),
                         advance_cursor=False))
        self.assertEqual(client.calls[0][2], "2026-04-01T00:00:00Z")
        self.assertEqual(s["stored"], 1)
        self.assertEqual(self.store.get_kraken_state(_STATE_LAST_POLL),
                         "2026-05-20T00:00:00")

    def test_oldest_unsettled_block_start(self):
        self._block("2026-05-01T00:00:00")
        self._block("2026-05-02T00:00:00")
        self.store._conn.execute(
            "UPDATE blocks SET imp_kwh_api=1.0 WHERE block_start='2026-05-01T00:00:00'")
        self.store._conn.commit()
        self.assertEqual(self.store.get_oldest_unsettled_block_start(),
                         "2026-05-02T00:00:00")

    def test_dry_run_counts_but_writes_nothing(self):
        self._block("2026-05-01T00:00:00")
        client = FakeClient(import_rows=self._rows(
            (1.5, "2026-05-01T00:00:00Z", "2026-05-01T00:30:00Z")))
        ing = KrakenIngester(client, self.store, import_mpan="M",
                             import_serial="S")
        s = run(ing.poll(dry_run=True))
        self.assertEqual(s["import_rows"], 1)
        self.assertEqual(s["stored"], 1)        # WOULD store (classify counts it)
        self.assertTrue(s["dry_run"])
        # but the DB is untouched:
        r = self.store.get_block_by_start("2026-05-01T00:00:00", "electricity_main")
        self.assertIsNone(r["imp_kwh_api"])              # nothing written
        self.assertEqual(r["needs_pass2_rerun"], 0)      # no flag set
        self.assertIsNone(self.store.get_kraken_state(_STATE_LAST_POLL))  # no advance

    def test_dry_run_counts_missing_and_review(self):
        # One matching block with drift, one interval with no block.
        self._block("2026-05-01T00:00:00", imp_kwh=2.0)
        client = FakeClient(import_rows=self._rows(
            (1.5, "2026-05-01T00:00:00Z", "2026-05-01T00:30:00Z"),   # -25% drift
            (1.0, "2026-05-01T05:00:00Z", "2026-05-01T05:30:00Z"),   # no block
        ))
        ing = KrakenIngester(client, self.store, import_mpan="M",
                             import_serial="S", drift_block_percent=2.0)
        s = run(ing.poll(dry_run=True))
        self.assertEqual(s["stored"], 1)             # would store the matching one
        self.assertEqual(s["skipped_no_block"], 1)   # the unmatched one
        self.assertEqual(s["flagged_review"], 1)     # drift > threshold
        # DB still clean
        r = self.store.get_block_by_start("2026-05-01T00:00:00", "electricity_main")
        self.assertIsNone(r["imp_kwh_api"])

    def test_missing_block_counted_not_stored(self):
        # No block exists at this interval.
        client = FakeClient(import_rows=self._rows(
            (1.5, "2026-05-01T00:00:00Z", "2026-05-01T00:30:00Z")))
        ing = KrakenIngester(client, self.store, import_mpan="M",
                             import_serial="S")
        s = run(ing.poll())
        self.assertEqual(s["stored"], 0)
        self.assertEqual(s["skipped_no_block"], 1)

    def test_drift_flags_review(self):
        self._block("2026-05-01T00:00:00", imp_kwh=2.0)
        client = FakeClient(import_rows=self._rows(
            (1.5, "2026-05-01T00:00:00Z", "2026-05-01T00:30:00Z")))  # -25%
        ing = KrakenIngester(client, self.store, import_mpan="M",
                             import_serial="S", drift_block_percent=2.0)
        s = run(ing.poll())
        self.assertEqual(s["flagged_review"], 1)

    def test_cursor_advances_to_latest_interval(self):
        self._block("2026-05-01T00:00:00")
        self._block("2026-05-01T00:30:00")
        client = FakeClient(import_rows=self._rows(
            (1.5, "2026-05-01T00:00:00Z", "2026-05-01T00:30:00Z"),
            (1.8, "2026-05-01T00:30:00Z", "2026-05-01T01:00:00Z"),
        ))
        ing = KrakenIngester(client, self.store, import_mpan="M",
                             import_serial="S")
        run(ing.poll())
        self.assertEqual(self.store.get_kraken_state(_STATE_LAST_POLL),
                         "2026-05-01T00:30:00")

    def test_import_fetch_failure_captured(self):
        client = FakeClient(raise_on_import="M")
        ing = KrakenIngester(client, self.store, import_mpan="M",
                             import_serial="S")
        s = run(ing.poll())
        self.assertTrue(any("import fetch failed" in e for e in s["errors"]))
        self.assertEqual(s["stored"], 0)

    def test_export_settled_upserted(self):
        self._block("2026-05-01T00:00:00")
        client = FakeClient(
            import_rows=self._rows((1.5, "2026-05-01T00:00:00Z", "2026-05-01T00:30:00Z")),
            export_rows=self._rows((0.4, "2026-05-01T00:00:00Z", "2026-05-01T00:30:00Z")))
        ing = KrakenIngester(client, self.store, import_mpan="M",
                             import_serial="S", export_mpan="EXPORTMPAN",
                             export_serial="ES", billing_source="api")
        s = run(ing.poll())
        self.assertEqual(s["export_rows"], 1)
        self.assertEqual(s["export_stored"], 1)
        r = self.store.get_block_by_start("2026-05-01T00:00:00", "electricity_main")
        self.assertEqual(r["imp_kwh_api"], 1.5)
        self.assertEqual(r["exp_kwh_api"], 0.4)

    def test_export_dry_run_no_write(self):
        self._block("2026-05-01T00:00:00")
        client = FakeClient(
            import_rows=self._rows((1.5, "2026-05-01T00:00:00Z", "2026-05-01T00:30:00Z")),
            export_rows=self._rows((0.4, "2026-05-01T00:00:00Z", "2026-05-01T00:30:00Z")))
        ing = KrakenIngester(client, self.store, import_mpan="M",
                             import_serial="S", export_mpan="EXPORTMPAN",
                             export_serial="ES")
        s = run(ing.poll(dry_run=True))
        self.assertEqual(s["export_rows"], 1)
        self.assertEqual(s["export_stored"], 1)   # WOULD store
        r = self.store.get_block_by_start("2026-05-01T00:00:00", "electricity_main")
        self.assertIsNone(r["exp_kwh_api"])        # but nothing written
        self.assertIsNone(r["imp_kwh_api"])

    def test_bst_row_maps_to_correct_block(self):
        # API returns a BST-offset interval; must map to the naive-UTC block.
        self._block("2026-03-30T01:00:00")
        client = FakeClient(import_rows=self._rows(
            (0.9, "2026-03-30T02:00:00+01:00", "2026-03-30T02:30:00+01:00")))
        ing = KrakenIngester(client, self.store, import_mpan="M",
                             import_serial="S")
        s = run(ing.poll())
        self.assertEqual(s["stored"], 1)
        r = self.store.get_block_by_start("2026-03-30T01:00:00", "electricity_main")
        self.assertEqual(r["imp_kwh_api"], 0.9)


    def test_dry_run_captures_review_samples(self):
        self._block("2026-05-01T00:00:00", imp_kwh=2.0)   # will drift -25%
        client = FakeClient(import_rows=self._rows(
            (1.5, "2026-05-01T00:00:00Z", "2026-05-01T00:30:00Z")))
        ing = KrakenIngester(client, self.store, import_mpan="M",
                             import_serial="S", drift_block_percent=2.0)
        s = run(ing.poll(dry_run=True))
        samples = s["review_samples"]
        self.assertEqual(len(samples), 1)
        self.assertEqual(samples[0]["channel"], "import")
        self.assertEqual(samples[0]["cad_kwh"], 2.0)
        self.assertEqual(samples[0]["dcc_kwh"], 1.5)
        self.assertAlmostEqual(samples[0]["drift_pct"], -25.0, places=3)


if __name__ == "__main__":
    unittest.main()

class TestOutageBackfill(_Base):
    """BL-8: settled rows for periods with NO block (a long outage) must
    materialise the block rather than be dropped as `skipped_no_block`."""

    def _rows(self, *triples):
        return [{"consumption": c, "interval_start": s, "interval_end": e}
                for c, s, e in triples]

    def setUp(self):
        super().setUp()
        self.store._conn.execute(
            "UPDATE config_periods SET effective_from = '2020-01-01T00:00:00'")
        self.store._conn.commit()

    def test_missing_block_is_backfilled(self):
        # no blocks exist for these intervals (the outage)
        client = FakeClient(import_rows=self._rows(
            (1.5, "2026-05-01T00:00:00Z", "2026-05-01T00:30:00Z"),
            (1.8, "2026-05-01T00:30:00Z", "2026-05-01T01:00:00Z"),
        ))
        ing = KrakenIngester(client, self.store, import_mpan="M",
                             import_serial="S", billing_source="api")
        s = run(ing.poll())
        self.assertEqual(s["backfilled"], 2)
        self.assertEqual(s["skipped_no_block"], 0)
        self.assertEqual(self.store.count_backfilled_blocks(), 2)
        r = self.store._conn.execute(
            "SELECT imp_kwh, imp_kwh_api, needs_pass2_rerun FROM blocks "
            "WHERE block_start = '2026-05-01T00:00:00'").fetchone()
        self.assertAlmostEqual(r["imp_kwh"], 1.5)
        self.assertEqual(r["needs_pass2_rerun"], 1)   # PASS 2 will price it

    def test_dry_run_never_backfills(self):
        client = FakeClient(import_rows=self._rows(
            (1.5, "2026-05-01T00:00:00Z", "2026-05-01T00:30:00Z")))
        ing = KrakenIngester(client, self.store, import_mpan="M",
                             import_serial="S", billing_source="api")
        s = run(ing.poll(dry_run=True))
        self.assertEqual(s.get("backfilled", 0), 0)
        self.assertEqual(self.store.count_backfilled_blocks(), 0)

    def test_backfill_can_be_disabled(self):
        client = FakeClient(import_rows=self._rows(
            (1.5, "2026-05-01T00:00:00Z", "2026-05-01T00:30:00Z")))
        ing = KrakenIngester(client, self.store, import_mpan="M",
                             import_serial="S", billing_source="api",
                             backfill_missing=False)
        s = run(ing.poll())
        self.assertEqual(s.get("backfilled", 0), 0)
        self.assertEqual(s["skipped_no_block"], 1)   # old behaviour