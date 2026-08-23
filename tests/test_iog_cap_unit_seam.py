"""
Watch #12 ROOT CAUSE: the pence→£ seam INSIDE compute_iog_split. Every other capped test drives
price_slot directly with £ literals, so this seam (schedule.resolve() [pence] → price_slot [£])
was never exercised — which is exactly how the 2026-08-18 pence break (imp_rate_ev=6.9) shipped.

This drives compute_iog_split with PENCE-valued RateSchedules (as production builds them via
from_api_records) and asserts the split comes out in £/kWh.
"""
import os, sys, unittest
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import iog_cap
from kraken_rates import RateSchedule

# Real IOG-SMB-TOU pence rates (what the API returns, what RateSchedule stores):
OFF_P, PEAK_P = 6.89997, 32.30920      # pence/kWh off-peak / peak

def _sched(*periods):
    return RateSchedule(list(periods))


class TestComputeIogSplitUnitSeam(unittest.TestCase):
    def _run(self):
        # import schedule: off-peak + peak in the same day so day_rate_bounds -> (off, peak) pence
        imp = _sched(("2025-05-01T00:00:00", "2025-05-01T05:00:00", OFF_P),
                     ("2025-05-01T05:00:00", None, PEAK_P))
        ev_off = _sched(("2025-05-01T00:00:00", None, OFF_P))
        ev_peak = _sched(("2025-05-01T00:00:00", None, PEAK_P))
        return iog_cap.compute_iog_split(
            "2025-05-01T02:00:00", "2025-05-01T02:30:00",
            chosen_kwh=4.0, ev_kwh=3.5, overlay_rate=round(OFF_P/100.0, 6),
            is_boost=False, capped=True, boundary=None,
            import_sched=imp, ev_off_sched=ev_off, ev_peak_sched=ev_peak)

    def test_capped_split_is_pounds_not_pence(self):
        r = self._run()
        self.assertIsNotNone(r)
        # off-peak EV band ~0.069 £/kWh — NOT 6.9 (pence)
        self.assertLess(r["imp_rate_ev"], 1.0, f"imp_rate_ev={r['imp_rate_ev']} looks like pence")
        self.assertAlmostEqual(r["imp_rate_ev"], round(OFF_P/100.0, 6), places=4)
        # blended main rate also £, well under the £3 sanity ceiling that used to mask this
        self.assertLess(r["imp_rate"], 3.0)
        # cost reconciles at £: EV 3.5 kWh at off-peak ≈ £0.2415
        self.assertAlmostEqual(r["imp_cost_ev"], round(3.5 * OFF_P/100.0, 6), places=4)


if __name__ == "__main__":
    unittest.main()
