"""
Phase-1 Δ3: chart_emit.day_rate_series is the ONE place "what rate applies each half-hour"
lives for presentation — the chart only plots it. Reads rates (never band labels): EV shows
the priced rate where it charged, off-peak when idle, holds peak to the noon reset once the
cap breaks; bump peaks show but don't latch; non-IOG days get no override (None series).
"""
import os, sys, unittest
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import chart_emit

OFF, PEAK, BLEND = 0.05493, 0.323092, 0.12


def _blk(rate, evsegs=None, rate_ev=None):
    imp = {"rate": rate}
    if evsegs is not None:
        imp["segments"] = evsegs
    if rate_ev is not None:
        imp["rate_ev"] = rate_ev
    return {"meters": {"electricity_main": {"channels": {"import": imp}}}}


def _ev(kwh, rate, band):
    return {"kwh": kwh, "inc_rate": rate, "band": band, "attribution": "ev"}


class TestChartEmit(unittest.TestCase):
    def test_non_iog_no_override(self):
        db = [(0, _blk(OFF)), (20, _blk(PEAK))]     # no EV anywhere
        s = chart_emit.day_rate_series(db, slots=48, block_minutes=30)
        self.assertTrue(all(v is None for v in s["ev"]))
        self.assertTrue(all(v is None for v in s["house"]))

    def test_capped_off_blend_peak_hold_reset(self):
        db = [(2, _blk(OFF, [_ev(2.0, OFF, "off_peak")])),                 # within cap
              (4, _blk(BLEND, [_ev(1.0, OFF, "off_peak"), _ev(1.0, PEAK, "peak")])),  # boundary
              (5, _blk(PEAK, [_ev(2.0, PEAK, "peak")]))]                   # over cap
        s = chart_emit.day_rate_series(db, slots=48, block_minutes=30)
        self.assertAlmostEqual(s["ev"][2], OFF, places=4)
        self.assertTrue(OFF < s["ev"][4] < PEAK)          # blended boundary
        self.assertAlmostEqual(s["ev"][5], PEAK, places=4)
        self.assertAlmostEqual(s["ev"][10], PEAK, places=4)   # HELD across idle to noon
        self.assertAlmostEqual(s["ev"][23], PEAK, places=4)
        self.assertAlmostEqual(s["ev"][24], OFF, places=4)    # noon reset → off-peak
        self.assertAlmostEqual(s["house"][5], PEAK, places=4) # house line = main rate

    def test_bump_shows_peak_but_does_not_latch(self):
        db = [(2, _blk(OFF, [_ev(2.0, OFF, "off_peak")])),
              (30, _blk(PEAK, [_ev(1.0, PEAK, "off_peak")])),   # bump: peak rate, off_peak band
              (31, _blk(PEAK, []))]                             # idle after bump
        s = chart_emit.day_rate_series(db, slots=48, block_minutes=30)
        self.assertAlmostEqual(s["ev"][30], PEAK, places=4)    # bump → peak
        self.assertAlmostEqual(s["ev"][31], OFF, places=4)     # not latched (no real break)


if __name__ == "__main__":
    unittest.main()
