"""
test_measured_apply.py — BL-53 step 3 helper apply_measured_to_block (still inert in prod).

Writes Octopus's billed cost as authoritative: rate keyed on cost/kWh (NOT the label),
EV+house split summing to the bill exactly, rate_source='measured'.
"""
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import engine
import pricing_segments as ps
from block_store import BlockStore


class _Sched:
    def is_empty(self):
        return False

    def day_rate_bounds(self, ts):
        return (5.493, 32.3092)   # pence (off, peak)


class TestMeasuredApply(unittest.TestCase):

    SLOT = "2026-08-23T10:00:00"

    def setUp(self):
        self._save = (engine._store, engine._kraken_rate_schedules)
        self.st = BlockStore(":memory:")
        engine._store = self.st
        engine._kraken_rate_schedules = {"import": _Sched()}
        self.st._conn.execute(
            "INSERT OR IGNORE INTO config_periods (id, effective_from, billing_day, "
            "block_minutes, timezone) VALUES (1,'2020-01-01T00:00:00',1,30,'UTC')")

    def tearDown(self):
        (engine._store, engine._kraken_rate_schedules) = self._save

    def _blk(self, kwh, evk, rate=0.05493):
        self.st._conn.execute(
            "INSERT INTO blocks (block_start, block_end, meter_id, config_period_id, "
            "imp_kwh, imp_rate, imp_cost, imp_kwh_ev, rate_source) "
            "VALUES (?,?,?,?,?,?,?,?,?)",
            (self.SLOT, self.SLOT, "electricity_main", 1, kwh, rate,
             round(kwh*rate, 6), evk, "reconciled"))
        self.st._conn.commit()

    def _row(self):
        return self.st._conn.execute(
            "SELECT imp_rate, imp_cost, imp_rate_ev, imp_cost_ev, imp_ev_band, "
            "imp_home_band, rate_source, imp_rate_exc, imp_cost_exc, imp_cost_remainder FROM blocks "
            "WHERE block_start=?", (self.SLOT,)).fetchone()

    def test_peak_bill_writes_measured_and_splits_exact(self):
        # 23rd bump: block currently off-peak; Octopus bills STANDARD £1.0294 / 3.186 kWh
        self._blk(3.186, 2.24, rate=0.05493)
        ok = engine.apply_measured_to_block(
            self.SLOT, cost_incl=1.029372, cost_excl=0.980355, label="STANDARD_RATE")
        self.assertTrue(ok)
        r = self._row()
        self.assertEqual(r["rate_source"], "measured")
        self.assertAlmostEqual(r["imp_rate"], round(1.029372/3.186, 6), places=6)
        self.assertAlmostEqual(r["imp_cost"], 1.029372, places=6)
        self.assertEqual(r["imp_ev_band"], "peak")       # cost ~0.323 → peak band
        self.assertEqual(r["imp_home_band"], "day")
        self.assertAlmostEqual(r["imp_rate_exc"], round(0.980355/3.186, 6), places=6)
        segs = [ps.Segment(**x) for x in
                self.st.get_block_segments(self.SLOT, "electricity_main")]
        self.assertAlmostEqual(ps.total_cost(segs), 1.029372, places=5)   # sums to the bill
        self.assertAlmostEqual(ps.attribution_cost(segs, "ev"),
                               round(2.24 * (1.029372/3.186), 6), places=5)

    def test_cost_beats_label(self):
        # label says STANDARD but the COST is the off-peak amount (25/08 23:00 signature).
        # Band must follow the COST (off_peak), not the label.
        self._blk(1.0, 0.5, rate=0.05493)
        engine.apply_measured_to_block(
            self.SLOT, cost_incl=0.05493, cost_excl=0.052314, label="STANDARD_RATE")
        r = self._row()
        self.assertEqual(r["imp_ev_band"], "off_peak")
        self.assertEqual(r["imp_home_band"], "off_peak")
        self.assertAlmostEqual(r["imp_rate"], 0.05493, places=6)

    def test_mixed_uses_4band_clean_no_rescale(self):
        # A genuine over-cap boundary slot (label='mixed'): keep the true 4-band split at the
        # schedule's CLEAN per-band rates — NOT rescaled to the bill (clean-rate invariant).
        # imp_cost stays the bill; the split reconciles to it via imp_cost_remainder.
        import iog_cap
        self._blk(3.0, 2.0, rate=0.05493)          # 2 kWh EV, 1 kWh house
        engine._kraken_rate_schedules = {
            "import": _Sched(),
            "ev_device_off_peak": _Sched(), "ev_device_peak": _Sched()}
        # EV 1@off_peak(0.055) + 1@peak(0.323), house 1@day(0.323) — CLEAN rates
        _raw = [(1.0, 0.055, "off_peak", "ev"),
                (1.0, 0.323, "peak", "ev"),
                (1.0, 0.323, "day", "house")]
        _save = iog_cap.compute_iog_split
        iog_cap.compute_iog_split = lambda *a, **k: {
            "segments": _raw, "classification": {"ev": "mixed", "house": "day"}}
        try:
            bill_incl, bill_excl = 0.900, 0.857
            ok = engine.apply_measured_to_block(
                self.SLOT, cost_incl=bill_incl, cost_excl=bill_excl, label="mixed")
        finally:
            iog_cap.compute_iog_split = _save
        self.assertTrue(ok)
        segs = [ps.Segment(**x) for x in
                self.st.get_block_segments(self.SLOT, "electricity_main")]
        self.assertEqual(len(segs), 3)
        self.assertEqual({s.band for s in segs}, {"off_peak", "peak", "day"})
        # CLEAN rates preserved (NOT rescaled) — every segment rate is a stub band value
        for sg in segs:
            self.assertIn(round(sg.inc_rate, 3), (0.055, 0.323))
        r = self._row()
        self.assertEqual(r["rate_source"], "measured")
        self.assertAlmostEqual(r["imp_cost"], bill_incl, places=6)   # cost stays the bill
        self.assertEqual(r["imp_ev_band"], "mixed")
        # EV cost = clean sum (1x0.055 + 1x0.323); house remainder absorbs the bill delta
        self.assertAlmostEqual(r["imp_cost_ev"], round(0.055 + 0.323, 6), places=6)
        self.assertAlmostEqual(r["imp_cost_remainder"], round(bill_incl - (0.055 + 0.323), 6), places=6)

    def test_no_kwh_is_noop(self):
        self._blk(0.0, 0.0, rate=0.05493)
        self.assertFalse(engine.apply_measured_to_block(
            self.SLOT, cost_incl=0.5, cost_excl=0.48, label="STANDARD_RATE"))
        self.assertEqual(self._row()["rate_source"], "reconciled")  # untouched


if __name__ == "__main__":
    unittest.main()
