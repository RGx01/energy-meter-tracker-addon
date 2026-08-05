"""Tests for kraken_mini.py (Chunk 7) — Mini boundary reader."""

import asyncio
import unittest
from datetime import datetime

from kraken_mini import MiniBoundaryReader, _parse_readat


def run(coro):
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


class FakeMiniClient:
    """Returns scripted telemetry; records calls. get_telemetry returns the
    points whose readAt falls within [start,end]."""
    def __init__(self, points, rate_limit=None):
        self._points = points          # list of {"readAt","consumption"}
        self._rate_limit = rate_limit
        self.telemetry_calls = 0
        self.rate_calls = 0

    async def get_telemetry(self, device_id, start, end):
        self.telemetry_calls += 1
        return list(self._points)

    async def get_rate_limit(self):
        self.rate_calls += 1
        return self._rate_limit


class TestParseReadAt(unittest.TestCase):
    def test_offset_normalised(self):
        dt = _parse_readat("2026-05-01T01:00:00+01:00")
        self.assertEqual(dt, datetime(2026, 5, 1, 0, 0, 0))

    def test_bad_returns_none(self):
        self.assertIsNone(_parse_readat("nonsense"))
        self.assertIsNone(_parse_readat(None))


class TestBracket(unittest.TestCase):
    def test_brackets_boundary(self):
        pts = [
            {"value": 100.0, "ts": "2026-05-01T11:59:00"},
            {"value": 100.5, "ts": "2026-05-01T11:59:50"},
            {"value": 101.0, "ts": "2026-05-01T12:00:30"},
        ]
        b = datetime(2026, 5, 1, 12, 0, 0)
        pre, post = MiniBoundaryReader._bracket(pts, b)
        self.assertEqual(pre["value"], 100.5)   # latest before
        self.assertEqual(post["value"], 101.0)  # first after

    def test_no_post_returns_partial(self):
        pts = [{"value": 100.0, "ts": "2026-05-01T11:59:00"}]
        b = datetime(2026, 5, 1, 12, 0, 0)
        pre, post = MiniBoundaryReader._bracket(pts, b)
        self.assertIsNotNone(pre)
        self.assertIsNone(post)


class TestReadAtBoundary(unittest.TestCase):
    def _client(self, **kw):
        # consumption is in Wh (the raw telemetry unit). 100000 Wh @ 11:59,
        # 102000 Wh @ 12:01 → after the ÷1000 kWh conversion: 100.0 / 102.0 kWh.
        return FakeMiniClient([
            {"readAt": "2026-05-01T11:59:00Z", "consumption": 100000.0},
            {"readAt": "2026-05-01T12:01:00Z", "consumption": 102000.0},
        ], **kw)

    def test_interpolates_at_boundary(self):
        c = self._client()
        r = MiniBoundaryReader(c, "dev1")
        out = run(r.read_at_boundary("2026-05-01T12:00:00"))
        self.assertIsNotNone(out)
        # 12:00 is 1min into a 2min window → halfway → ~101.0 kWh (from 102000Wh/100000Wh)
        self.assertAlmostEqual(out["value"], 101.0, places=2)
        self.assertTrue(out["interpolated"])

    def test_returns_none_when_no_post_point(self):
        # Only pre-boundary points; should time out → None. Use a now_fn that
        # jumps past the deadline immediately so the test doesn't sleep.
        c = FakeMiniClient([{"readAt": "2026-05-01T11:59:00Z", "consumption": 100000.0}])
        r = MiniBoundaryReader(c, "dev1")
        ticks = iter([datetime(2026, 5, 1, 12, 0, 0),
                      datetime(2026, 5, 1, 13, 0, 0)])  # 2nd call is past deadline
        out = run(r.read_at_boundary("2026-05-01T12:00:00",
                                     now_fn=lambda: next(ticks)))
        self.assertIsNone(out)

    def test_bad_boundary_returns_none(self):
        c = self._client()
        r = MiniBoundaryReader(c, "dev1")
        self.assertIsNone(run(r.read_at_boundary("not-a-date")))

    def test_rate_limit_skips_burst(self):
        # On the Nth boundary, low remaining budget → skip (None), no telemetry.
        c = self._client(rate_limit={"remaining": 5})
        r = MiniBoundaryReader(c, "dev1")
        r._boundary_count = 7   # next call is the 8th → triggers check
        out = run(r.read_at_boundary("2026-05-01T12:00:00"))
        self.assertIsNone(out)
        self.assertEqual(c.telemetry_calls, 0)   # skipped before fetching


class _ScriptedClient:
    """get_telemetry returns the Nth scripted batch per call (last batch sticks).
    Each batch is a list of (readAt_iso, consumption)."""
    def __init__(self, batches):
        self.batches = batches
        self.telemetry_calls = 0

    async def get_telemetry(self, device_id, start, end):
        i = min(self.telemetry_calls, len(self.batches) - 1)
        self.telemetry_calls += 1
        return [{"readAt": ra, "consumption": c} for ra, c in self.batches[i]]


class TestCollectInto(unittest.TestCase):
    B = "2026-06-03T07:30:00"

    def _bdt(self, **kw):
        from datetime import timedelta
        return datetime.fromisoformat(self.B) + timedelta(**kw)

    def test_brackets_then_stops(self):
        c = _ScriptedClient([
            [("2026-06-03T07:29:40Z", 100.0)],                       # pre only
            [("2026-06-03T07:29:40Z", 100.0),
             ("2026-06-03T07:30:20Z", 101.0)],                       # post lands
        ])
        r = MiniBoundaryReader(c, "dev")
        reads = []
        run(r.collect_into(reads, self.B, self._bdt(seconds=-20)))
        run(r.collect_into(reads, self.B, self._bdt(seconds=25)))
        n3 = run(r.collect_into(reads, self.B, self._bdt(seconds=35)))
        self.assertTrue(r._got_post)
        self.assertEqual(n3, 0)                       # no-op after bracketed
        self.assertEqual([x["ts"] for x in reads],
                         ["2026-06-03T07:29:40", "2026-06-03T07:30:20"])

    def test_dedup(self):
        c = _ScriptedClient([[("2026-06-03T07:29:40Z", 100.0)]])
        r = MiniBoundaryReader(c, "dev")
        reads = []
        run(r.collect_into(reads, self.B, self._bdt(seconds=-15)))
        # same point again next tick → not duplicated
        run(r.collect_into(reads, self.B, self._bdt(seconds=-5)))
        self.assertEqual(len(reads), 1)

    def test_call_cap(self):
        from datetime import timedelta
        c = _ScriptedClient([[("2026-06-03T07:28:00Z", 50.0)]])  # never a post point
        r = MiniBoundaryReader(c, "dev")
        buf = []
        # Advance well past any defer window each tick so pacing never blocks.
        for k in range(15):
            run(r.collect_into(buf, self.B, self._bdt(seconds=20 + k * 120)))
        self.assertEqual(r._boundary_calls, 10)       # hard cap
        self.assertFalse(r._got_post)

    def test_drift_defers_call(self):
        from datetime import timedelta
        c = _ScriptedClient([[("2026-06-03T07:29:40Z", 100.0)]])  # always 80s behind
        r = MiniBoundaryReader(c, "dev")
        buf = []
        run(r.collect_into(buf, self.B, self._bdt(seconds=60)))   # drift ~80s
        self.assertIsNotNone(r._next_call_after)
        before = c.telemetry_calls
        run(r.collect_into(buf, self.B, self._bdt(seconds=65)))   # within defer → skip
        self.assertEqual(c.telemetry_calls, before)
        run(r.collect_into(buf, self.B, r._next_call_after))      # at defer time → call
        self.assertGreater(c.telemetry_calls, before)

    def test_consumption_converted_wh_to_kwh(self):
        # Raw consumption is Wh; collect_into must store kWh (÷1000). A register
        # of 30248274 Wh → 30248.274 kWh.
        c = _ScriptedClient([
            [("2026-06-03T07:29:40Z", 30248274.0),
             ("2026-06-03T07:30:20Z", 30248275.0)],
        ])
        r = MiniBoundaryReader(c, "dev")
        reads = []
        run(r.collect_into(reads, self.B, self._bdt(seconds=25)))
        self.assertAlmostEqual(reads[0]["value"], 30248.274, places=3)
        self.assertAlmostEqual(reads[1]["value"], 30248.275, places=3)
        # The delta is 1 Wh = 0.001 kWh, NOT 1 kWh.
        self.assertAlmostEqual(reads[1]["value"] - reads[0]["value"], 0.001, places=4)


class TestStoreMiniImport(unittest.TestCase):
    def setUp(self):
        from block_store import BlockStore
        self.store = BlockStore(":memory:")
        self.store.insert_config_period({"meters": {"electricity_main": {
            "meta": {"timezone": "Europe/London", "billing_day": 1,
                     "block_minutes": 30, "currency_symbol": "£",
                     "currency_code": "GBP", "sub_meter": False},
            "standing_charge": 0.5,
            "channels": {"import": {"sensor": "sensor.i"}}}}})
        self.pid = self.store.get_current_config_period_id()
        self.store._conn.execute(
            """INSERT INTO blocks (block_start, block_end, meter_id,
                 config_period_id, imp_kwh) VALUES (?,?,?,?,?)""",
            ("2026-05-01T00:00:00", "2026-05-01T00:30:00",
             "electricity_main", self.pid, 0.0))
        self.store._conn.commit()

    def tearDown(self):
        self.store.close()

    def test_stores_provisional(self):
        res = self.store.store_mini_import("2026-05-01T00:00:00",
                                           "electricity_main", 1.234)
        self.assertEqual(res["status"], "stored")
        row = self.store.get_block_by_start("2026-05-01T00:00:00", "electricity_main")
        self.assertAlmostEqual(row["imp_kwh"], 1.234)
        self.assertEqual(row["is_provisional"], 1)
        self.assertEqual(row["source"], "kraken_mini")
        self.assertEqual(row["needs_pass2_rerun"], 1)

    def test_missing_block(self):
        res = self.store.store_mini_import("2099-01-01T00:00:00",
                                           "electricity_main", 1.0)
        self.assertEqual(res["status"], "missing_block")


class _TeleClient:
    """get_telemetry stub that can raise (optionally a specific exception)."""

    def __init__(self, raise_=False, rows=None, exc=None):
        self.raise_ = raise_
        self.rows = rows or []
        self.exc = exc or RuntimeError("telemetry down")

    async def get_telemetry(self, device_id, start_iso, end_iso):
        if self.raise_:
            raise self.exc
        return self.rows


class TestMiniFetchDedupe(unittest.TestCase):
    """A persistent telemetry failure (e.g. GraphQL 403) must not flood the log:
    the reader flags the failure once and clears on recovery."""

    _D0 = datetime(2026, 7, 1, 0, 0, 0)
    _D1 = datetime(2026, 7, 1, 0, 2, 0)

    def test_failure_sets_flag_then_recovers(self):
        c = _TeleClient(raise_=True)
        r = MiniBoundaryReader(c, "dev0")
        self.assertEqual(run(r._fetch(self._D0, self._D1)), [])
        self.assertTrue(r._fetch_failing)
        self.assertEqual(run(r._fetch(self._D0, self._D1)), [])   # still failing
        self.assertTrue(r._fetch_failing)
        c.raise_ = False                                          # recover
        self.assertEqual(run(r._fetch(self._D0, self._D1)), [])
        self.assertFalse(r._fetch_failing)

    def test_cooldown_error_flags_quietly(self):
        from kraken_api_client import KrakenCooldownError
        c = _TeleClient(raise_=True, exc=KrakenCooldownError("cooldown", status=403))
        r = MiniBoundaryReader(c, "dev0")
        self.assertEqual(run(r._fetch(self._D0, self._D1)), [])
        self.assertTrue(r._fetch_failing)


if __name__ == "__main__":
    unittest.main()