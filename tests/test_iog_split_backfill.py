"""
test_iog_split_backfill.py — BL-9 one-shot historical EV/Home split backfill.

The split seam only fires on finalise/settlement, so blocks priced before it shipped
(existing dispatch history + already-settled blocks) carry no EV/Home columns and the
billing summary shows no split. _run_historical_iog_split_backfill walks those blocks
and carves the split IN PLACE. Guarantees checked here:
  * the block_store paging finds dispatched MAIN-meter blocks missing the split, and
    excludes sub-meters and non-dispatched slots;
  * the batch writer is additive + idempotent (never touches imp_rate/imp_cost);
  * the runner carves the split, leaves the inc bill byte-identical, and marks done;
  * it defers (no done marker) when the import schedule isn't ready, and marks done
    with nothing to do when there's no dispatch history.
"""

import asyncio
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import engine
from block_store import BlockStore
from kraken_rates import RateSchedule


def _run(coro):
    return asyncio.new_event_loop().run_until_complete(coro)


class TestIogSplitBackfill(unittest.TestCase):

    def setUp(self):
        self.st = BlockStore(":memory:")
        engine._store = self.st
        engine._kraken_rate_schedules = {}
        self.st._conn.execute(
            "INSERT INTO config_periods (id, effective_from, billing_day, "
            "block_minutes, timezone, currency_symbol, currency_code) "
            "VALUES (1, '2026-01-01T00:00:00', 1, 30, 'UTC', '£', 'GBP')")
        self.st._conn.commit()
        # A dispatched main-meter block, priced before the split shipped (no imp_kwh_ev).
        self._insert_block("2026-01-01T13:00:00", "electricity_main",
                           imp_kwh=3.0, imp_rate=0.05, imp_cost=0.15, sub=0)
        self._completed("2026-01-01T13:00:00", 2.0)
        # An import schedule so _apply_iog_split can carve.
        engine._kraken_rate_schedules = {"import": RateSchedule(
            [("2026-01-01T00:00:00", None, 0.05)])}

    def tearDown(self):
        engine._store = None
        engine._kraken_rate_schedules = {}

    def _insert_block(self, start, meter_id, *, imp_kwh, imp_rate, imp_cost, sub):
        c = self.st._conn
        c.execute("INSERT OR IGNORE INTO meters (meter_id, config_period_id, "
                  "is_sub_meter) VALUES (?,1,?)", (meter_id, sub))
        end = start[:11] + "13:30:00" if start.endswith("13:00:00") else start
        c.execute(
            "INSERT INTO blocks (block_start, block_end, meter_id, config_period_id, "
            "imp_kwh, imp_rate, imp_cost) VALUES (?,?,?,?,?,?,?)",
            (start, end, meter_id, 1, imp_kwh, imp_rate, imp_cost))
        c.commit()

    def _completed(self, slot_start, energy):
        self.st._conn.execute(
            "INSERT INTO dispatch_history (slot_start, kind, provider, source, "
            "energy_kwh, first_seen, last_seen) VALUES (?,?,?,?,?,?,?)",
            (slot_start, "completed", "test", None, energy,
             slot_start, slot_start))
        self.st._conn.commit()

    def _row(self, start="2026-01-01T13:00:00", meter="electricity_main"):
        return self.st._conn.execute(
            "SELECT imp_kwh_ev, imp_cost_ev, imp_rate_ev, imp_ev_band, imp_home_band, "
            "imp_rate, imp_cost FROM blocks WHERE block_start=? AND meter_id=?",
            (start, meter)).fetchone()

    # ── block_store paging ───────────────────────────────────────────────────
    def test_paging_finds_dispatched_main_block(self):
        rows = self.st.get_blocks_missing_iog_split()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["start"], "2026-01-01T13:00:00")
        self.assertEqual(self.st.count_blocks_missing_iog_split(), 1)

    def test_paging_excludes_sub_meter(self):
        self._insert_block("2026-01-01T13:00:00", "sub_ev",
                           imp_kwh=2.0, imp_rate=0.05, imp_cost=0.10, sub=1)
        starts = [(r["start"], r["meter_id"])
                  for r in self.st.get_blocks_missing_iog_split()]
        self.assertNotIn(("2026-01-01T13:00:00", "sub_ev"), starts)

    def test_paging_excludes_undispatched(self):
        self._insert_block("2026-01-01T18:00:00", "electricity_main",
                           imp_kwh=1.0, imp_rate=0.30, imp_cost=0.30, sub=0)
        starts = [r["start"] for r in self.st.get_blocks_missing_iog_split()]
        self.assertNotIn("2026-01-01T18:00:00", starts)

    # ── batch writer: additive + idempotent ─────────────────────────────────
    def test_writer_additive_and_idempotent(self):
        n = self.st.set_blocks_iog_split([
            ("2026-01-01T13:00:00", "electricity_main", 2.0, 0.10, 0.05,
             "off_peak", "off_peak")])
        self.assertEqual(n, 1)
        r = self._row()
        self.assertEqual(r["imp_kwh_ev"], 2.0)
        self.assertAlmostEqual(r["imp_cost"], 0.15)     # inc untouched
        self.assertAlmostEqual(r["imp_rate"], 0.05)
        # Second write is a no-op (NULL-only guard) — never re-carves.
        n2 = self.st.set_blocks_iog_split([
            ("2026-01-01T13:00:00", "electricity_main", 9.9, 9.9, 9.9, "x", "y")])
        self.assertEqual(n2, 0)
        self.assertEqual(self._row()["imp_kwh_ev"], 2.0)

    # ── runner ───────────────────────────────────────────────────────────────
    def test_runner_carves_and_marks_done(self):
        filled = _run(engine._run_historical_iog_split_backfill())
        self.assertEqual(filled, 1)
        r = self._row()
        self.assertEqual(r["imp_kwh_ev"], 2.0)
        self.assertAlmostEqual(r["imp_cost_ev"], 0.10)   # 2 * 0.05 overlay
        self.assertAlmostEqual(r["imp_cost"], 0.15)      # inc byte-identical
        st = self.st.get_meta(engine._IOG_SPLIT_BACKFILL_MARKER, {})
        self.assertTrue(st.get("done"))
        # Idempotent second run: nothing left, still done.
        self.assertEqual(_run(engine._run_historical_iog_split_backfill()), 0)

    def test_runner_defers_without_import_schedule(self):
        engine._kraken_rate_schedules = {}               # not ready
        self.assertEqual(_run(engine._run_historical_iog_split_backfill()), 0)
        st = self.st.get_meta(engine._IOG_SPLIT_BACKFILL_MARKER, {}) or {}
        self.assertFalse(st.get("done"))                 # deferred, NOT done
        self.assertIsNone(self._row()["imp_kwh_ev"])

    def test_runner_no_dispatch_marks_done(self):
        self.st._conn.execute("DELETE FROM dispatch_history")
        self.st._conn.commit()
        self.assertEqual(_run(engine._run_historical_iog_split_backfill()), 0)
        st = self.st.get_meta(engine._IOG_SPLIT_BACKFILL_MARKER, {})
        self.assertTrue(st.get("done"))

    # ── the render read-path surfaces the split (regression: the lightweight chart
    #    fetch omitted the columns, so 'Import — total grid' fell back to plain rows) ──
    def test_lightweight_fetch_surfaces_split(self):
        self.st.set_blocks_iog_split([
            ("2026-01-01T13:00:00", "electricity_main", 2.0, 0.10, 0.05,
             "off_peak", "off_peak")])
        blks = self.st.get_blocks_lightweight(
            "2026-01-01T00:00:00", "2026-01-02T00:00:00")
        imp = blks[0]["meters"]["electricity_main"]["channels"]["import"]
        self.assertEqual(imp["kwh_ev"], 2.0)
        self.assertAlmostEqual(imp["cost_ev"], 0.10)
        self.assertAlmostEqual(imp["rate_ev"], 0.05)
        self.assertEqual(imp["ev_band"], "off_peak")
        self.assertEqual(imp["home_band"], "off_peak")

    def test_lightweight_fetch_omits_split_when_null(self):
        # An undispatched block never got the carve → keys absent, not present-as-None
        # (a present None would zero a real import downstream).
        self._insert_block("2026-01-01T18:00:00", "electricity_main",
                           imp_kwh=1.0, imp_rate=0.30, imp_cost=0.30, sub=0)
        blks = self.st.get_blocks_lightweight(
            "2026-01-01T17:00:00", "2026-01-01T19:00:00")
        imp = blks[0]["meters"]["electricity_main"]["channels"]["import"]
        self.assertNotIn("kwh_ev", imp)
        self.assertNotIn("ev_band", imp)


if __name__ == "__main__":
    unittest.main()
