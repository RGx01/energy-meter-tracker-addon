"""
test_iog_seam.py — engine._apply_iog_split, the reconcile-seam hook (BL-9).
Verifies the WIRING (reads grid-clipped EV from dispatch_history, detects capped
via ev_device schedules, reads the cap-day boundary, mutates imp_ch) — the rate
math itself is covered by test_iog_cap. Key guarantees: non-IOG / non-dispatch
slots are untouched, and UNCAPPED IOG totals are byte-identical.
"""

import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import engine
from block_store import BlockStore
from kraken_rates import RateSchedule


class TestApplyIogSplit(unittest.TestCase):

    def setUp(self):
        self.st = BlockStore(":memory:")
        engine._store = self.st
        engine._kraken_rate_schedules = {}

    def tearDown(self):
        engine._store = None
        engine._kraken_rate_schedules = {}

    def _house(self):
        return RateSchedule([
            ("2026-01-01T00:00:00", "2026-01-01T05:30:00", 0.07),   # night
            ("2026-01-01T05:30:00", "2026-01-01T23:30:00", 0.30),   # day
            ("2026-01-01T23:30:00", "2026-01-02T05:30:00", 0.07),   # night
        ])

    def _flat(self, r):
        return RateSchedule([("2026-01-01T00:00:00", None, r)])

    def _completed(self, slot_start, energy, raw_start=None, raw_end=None):
        self.st._conn.execute(
            "INSERT INTO dispatch_history (slot_start, kind, provider, source, "
            "energy_kwh, first_seen, last_seen, raw_start, raw_end) "
            "VALUES (?,?,?,?,?,?,?,?,?)",
            (slot_start, "completed", "test", None, energy,
             "2026-01-01T00:00:00", "2026-01-01T00:00:00", raw_start, raw_end))
        self.st._conn.commit()

    def _imp(self, rate, cost):
        return {"kwh": 3.0, "rate": rate, "cost": cost}

    def _apply(self, imp, start="2026-01-01T13:00:00", end="2026-01-01T13:30:00",
              overlay=0.30):
        engine._apply_iog_split(imp, start, end, 3.0, overlay, "UTC")

    # ── no-ops ──────────────────────────────────────────────────────────────
    def test_non_iog_is_noop(self):
        engine._kraken_rate_schedules = {}          # no import schedule
        imp = self._imp(0.30, 0.90)
        self._apply(imp)
        self.assertNotIn("kwh_ev", imp)
        self.assertEqual(imp["cost"], 0.90)

    def test_no_dispatch_is_noop(self):
        engine._kraken_rate_schedules = {"import": self._house()}
        imp = self._imp(0.30, 0.90)
        self._apply(imp)                              # no completed row for the slot
        self.assertNotIn("kwh_ev", imp)
        self.assertEqual(imp["cost"], 0.90)

    # ── uncapped: carve only, totals unchanged ──────────────────────────────
    def test_uncapped_carves_ev_totals_unchanged(self):
        engine._kraken_rate_schedules = {"import": self._house()}   # no ev_device
        self._completed("2026-01-01T13:00:00", 2.0)
        imp = self._imp(0.05, 0.15)                   # overlay already off-peak (3*0.05)
        self._apply(imp, overlay=0.05)
        self.assertEqual(imp["kwh_ev"], 2.0)
        self.assertAlmostEqual(imp["cost_ev"], 0.10)  # 2*0.05
        self.assertAlmostEqual(imp["cost"], 0.15)     # UNCHANGED
        self.assertAlmostEqual(imp["rate"], 0.05)
        self.assertEqual(imp["ev_band"], "off_peak")  # uncapped: whole slot off-peak

    # ── capped: re-prices ───────────────────────────────────────────────────
    def _capped(self):
        engine._kraken_rate_schedules = {
            "import": self._house(),
            "ev_device_off_peak": self._flat(0.05),
            "ev_device_peak": self._flat(0.25)}

    def test_capped_within_cap_freebie(self):
        self._capped()
        # 0.5h completed dispatch → under 6h → no boundary → within cap
        self._completed("2026-01-01T13:00:00", 2.0,
                        "2026-01-01T13:00:00", "2026-01-01T13:30:00")
        imp = self._imp(0.30, 0.90)                   # overlay peak (out of window)
        self._apply(imp)
        self.assertAlmostEqual(imp["cost"], 0.17)     # 2*0.05 + 1*0.07 (house freebie)
        self.assertAlmostEqual(imp["cost_ev"], 0.10)
        self.assertEqual(imp["ev_band"], "off_peak")
        self.assertEqual(imp["home_band"], "off_peak")

    def test_capped_over_cap_ev_peak(self):
        self._capped()
        # 7.5h completed dispatch in the cap-day → boundary 18:00; slot 18:30 over
        self._completed("2026-01-01T18:30:00", 2.0,
                        "2026-01-01T12:00:00", "2026-01-01T19:30:00")
        imp = self._imp(0.30, 0.90)
        self._apply(imp, start="2026-01-01T18:30:00", end="2026-01-01T19:00:00")
        self.assertAlmostEqual(imp["cost"], 0.80)     # 2*0.25 + 1*0.30
        self.assertAlmostEqual(imp["rate_ev"], 0.25)
        self.assertEqual(imp["ev_band"], "peak")
        self.assertEqual(imp["home_band"], "day")     # freebie withdrawn out-of-window


if __name__ == "__main__":
    unittest.main()
