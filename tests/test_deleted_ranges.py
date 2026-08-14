"""
test_deleted_ranges.py
======================
BL-8 phase 2 — deliberate-deletion persistence (deleted_ranges tombstone).

A manual delete records a deleted_range so the AUTOMATIC create paths (BL-8
backfill, the poll's gap-scan) skip it — a deliberate delete stays deleted instead
of being re-created as if it were an outage hole. A TARGETED user fill overrides +
clears the tombstone; a blanket fill respects it. Consulted only at block-creation
and window-scan points, never by billing/chart reads.

All tests use an in-memory SQLite database — no files are written to disk.
"""

import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(__file__))
from block_store import BlockStore


class TestDeletedRangesTombstone(unittest.TestCase):
    """A deliberate delete records a deleted_range so auto-backfill and the gap-scan
    skip it — deletes stay deleted. Targeted fills override + clear the tombstone;
    blanket fills respect it."""

    def _store(self):
        st = BlockStore(":memory:")
        with st._conn:
            cp = st._conn.execute(
                "INSERT INTO config_periods (effective_from, billing_day, "
                "block_minutes, timezone, currency_symbol, currency_code) "
                "VALUES ('2024-01-01T00:00:00',1,30,'UTC','£','GBP')").lastrowid
            st._conn.execute(
                "INSERT INTO meters (config_period_id, meter_id, is_sub_meter) "
                "VALUES (?, 'electricity_main', 0)", (cp,))
        st._conn.commit()
        self._cp = cp
        return st

    def _add(self, st, starts):
        for s in starts:
            st._conn.execute(
                "INSERT INTO blocks (block_start, block_end, meter_id, "
                "config_period_id, imp_kwh) VALUES (?,?, 'electricity_main', ?, 1.0)",
                (s, s, self._cp))
        st._conn.commit()

    def test_is_slot_tombstoned_inside_outside_and_star(self):
        st = self._store()
        st.add_deleted_range("electricity_main", "2026-08-12T10:00:00",
                             "2026-08-12T11:00:00")
        self.assertTrue(st.is_slot_tombstoned("electricity_main", "2026-08-12T10:00:00"))
        self.assertTrue(st.is_slot_tombstoned("electricity_main", "2026-08-12T10:30:00"))
        # end is exclusive
        self.assertFalse(st.is_slot_tombstoned("electricity_main", "2026-08-12T11:00:00"))
        self.assertFalse(st.is_slot_tombstoned("electricity_main", "2026-08-12T09:30:00"))
        # '*' matches every meter
        st.add_deleted_range("*", "2026-08-13T00:00:00", "2026-08-14T00:00:00")
        self.assertTrue(st.is_slot_tombstoned("sub_meter_ev", "2026-08-13T05:00:00"))

    def test_gap_scan_skips_tombstoned_hole(self):
        st = self._store()
        self._add(st, ["2026-08-12T09:00:00", "2026-08-12T09:30:00",
                       "2026-08-12T11:00:00"])   # hole at 10:00/10:30
        self.assertEqual(st.get_oldest_gap_start(since_iso="2026-08-01T00:00:00"),
                         "2026-08-12T10:00:00")
        st.add_deleted_range("electricity_main", "2026-08-12T10:00:00",
                             "2026-08-12T11:00:00")
        self.assertIsNone(st.get_oldest_gap_start(since_iso="2026-08-01T00:00:00"))

    def test_create_backfill_block_gated_and_overridable(self):
        st = self._store()
        st.add_deleted_range("electricity_main", "2026-08-12T10:00:00",
                             "2026-08-12T11:00:00")
        # gated: automatic / blanket caller (default) is refused
        self.assertIsNone(st.create_backfill_block(
            "2026-08-12T10:00:00", "electricity_main", 2.0))
        # targeted caller overrides and the block is created
        bid = st.create_backfill_block(
            "2026-08-12T10:00:00", "electricity_main", 2.0, override_tombstone=True)
        self.assertIsNotNone(bid)
        # a non-tombstoned slot is unaffected
        self.assertIsNotNone(st.create_backfill_block(
            "2026-08-12T12:00:00", "electricity_main", 2.0))

    def test_clear_whole_and_split_subspan(self):
        st = self._store()
        st.add_deleted_range("electricity_main", "2026-08-12T20:00:00",
                             "2026-08-12T23:00:00")
        # clear a middle sub-span → splits into 20:00-21:00 and 22:00-23:00
        lifted = st.clear_deleted_range("electricity_main", "2026-08-12T21:00:00",
                                        "2026-08-12T22:00:00")
        self.assertEqual(lifted, 1)
        spans = sorted((r["start_utc"], r["end_utc"]) for r in st.get_deleted_ranges())
        self.assertEqual(spans, [("2026-08-12T20:00:00", "2026-08-12T21:00:00"),
                                 ("2026-08-12T22:00:00", "2026-08-12T23:00:00")])
        self.assertTrue(st.is_slot_tombstoned("electricity_main", "2026-08-12T20:30:00"))
        self.assertFalse(st.is_slot_tombstoned("electricity_main", "2026-08-12T21:30:00"))
        # clearing the remainder wholly removes it
        self.assertEqual(st.clear_deleted_range("electricity_main",
                         "2026-08-12T20:00:00", "2026-08-12T21:00:00"), 1)

    def test_targeted_clear_then_backfill_restores(self):
        # Simulates what a TARGETED fill (run_gap_fill_job / apply_csv_import /
        # re-import) does: lift the tombstone, after which the default-gated create
        # path fills again. A blanket/auto caller (no clear) stays blocked.
        st = self._store()
        st.add_deleted_range("electricity_main", "2026-08-12T10:00:00",
                             "2026-08-12T11:00:00")
        self.assertIsNone(st.create_backfill_block(
            "2026-08-12T10:00:00", "electricity_main", 2.0))       # gated (blanket/auto)
        st.clear_deleted_range("electricity_main", "2026-08-12T10:00:00",
                               "2026-08-12T11:00:00")              # targeted lift
        self.assertIsNotNone(st.create_backfill_block(
            "2026-08-12T10:00:00", "electricity_main", 2.0))       # now fills
        self.assertFalse(st.is_slot_tombstoned("electricity_main",
                                               "2026-08-12T10:00:00"))

    def test_delete_writes_tombstone_star_for_all_meters(self):
        st = self._store()
        self._add(st, ["2026-08-12T00:00:00", "2026-08-12T12:00:00",
                       "2026-08-12T23:30:00"])
        res = st.delete_blocks_for_date_range("2026-08-12", "2026-08-12",
                                              meter_id=None, tz_name="UTC")
        self.assertEqual(res["deleted"], 3)
        rows = st.get_deleted_ranges()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["meter_id"], "*")
        self.assertEqual(rows[0]["reason"], "user_delete")
        self.assertTrue(st.is_slot_tombstoned("electricity_main", "2026-08-12T12:00:00"))
        # and the poll gap-scan now steps over the just-deleted day
        self.assertIsNone(st.get_oldest_gap_start(since_iso="2026-08-01T00:00:00"))


class TestContiguousRangeDelete(unittest.TestCase):
    """The delete's from/to times are the ends of ONE contiguous span, NOT a per-day
    time-of-day window. `14 00:00 → 15 12:00` deletes the continuous run, not
    00:00–12:00 on both days (the delete-scoping bug)."""

    def _store(self):
        st = BlockStore(":memory:")
        with st._conn:
            cp = st._conn.execute(
                "INSERT INTO config_periods (effective_from, billing_day, "
                "block_minutes, timezone, currency_symbol, currency_code) "
                "VALUES ('2019-01-01T00:00:00',1,30,'UTC','£','GBP')").lastrowid
            st._conn.execute("INSERT INTO meters (config_period_id, meter_id, "
                             "is_sub_meter) VALUES (?, 'electricity_main', 0)", (cp,))
        # two full UTC days at 30-min grid
        import datetime as dt
        t = dt.datetime(2020, 1, 1, 0, 0)
        end = dt.datetime(2020, 1, 3, 0, 0)
        while t < end:
            st._conn.execute(
                "INSERT INTO blocks (block_start, block_end, meter_id, "
                "config_period_id, imp_kwh) VALUES (?,?, 'electricity_main', ?, 1.0)",
                (t.isoformat(), t.isoformat(), cp))
            t += dt.timedelta(minutes=30)
        st._conn.commit()
        return st

    def _present(self, st):
        return [r[0] for r in st._conn.execute(
            "SELECT block_start FROM blocks WHERE meter_id='electricity_main' "
            "ORDER BY block_start")]

    def test_contiguous_not_per_day(self):
        st = self._store()
        res = st.delete_blocks_for_date_range(
            "2020-01-01", "2020-01-02", meter_id="electricity_main",
            from_time="00:00", to_time="12:00", tz_name="UTC")
        # contiguous 01 00:00 .. 02 12:00 inclusive = 48 + 25 = 73 (NOT 50 per-day)
        self.assertEqual(res["deleted"], 73)
        remaining = self._present(st)
        # everything up to 02 12:00 gone; 02 12:30..23:30 survives, contiguous
        self.assertEqual(remaining[0], "2020-01-02T12:30:00")
        self.assertEqual(remaining[-1], "2020-01-02T23:30:00")
        self.assertEqual(len(remaining), 23)

    def test_contiguous_tombstone_single_range(self):
        st = self._store()
        st.delete_blocks_for_date_range(
            "2020-01-01", "2020-01-02", meter_id=None,
            from_time="00:00", to_time="12:00", tz_name="UTC")
        rows = st.get_deleted_ranges()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["start_utc"], "2020-01-01T00:00:00")
        # inclusive end extended by one block to cover the last deleted slot
        self.assertEqual(rows[0]["end_utc"], "2020-01-02T12:30:00")
        # a surviving block is NOT tombstoned; a deleted one is
        self.assertFalse(st.is_slot_tombstoned("electricity_main", "2020-01-02T12:30:00"))
        self.assertTrue(st.is_slot_tombstoned("electricity_main", "2020-01-02T00:00:00"))

    def test_whole_day_unchanged(self):
        st = self._store()
        res = st.delete_blocks_for_date_range(
            "2020-01-01", "2020-01-02", meter_id="electricity_main",
            from_time="00:00", to_time="23:59", tz_name="UTC")
        self.assertEqual(res["deleted"], 96)          # both full days
        self.assertEqual(self._present(st), [])

    def test_local_datetime_to_utc_dst(self):
        from block_store import local_datetime_to_utc, utc_to_local_label
        # BST: 12:00 local -> 11:00 UTC
        self.assertEqual(local_datetime_to_utc("2026-04-15", "12:00", "Europe/London"),
                         "2026-04-15T11:00:00")
        # and back for display
        self.assertEqual(utc_to_local_label("2026-08-11T23:00:00", "Europe/London"),
                         "2026-08-12 00:00")


if __name__ == "__main__":
    unittest.main()
