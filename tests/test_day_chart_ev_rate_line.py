"""
BL-9 charts: the day chart's EV (dispatch) rate LINE follows the stored dispatch EV
rate (imp_rate_ev), so it diverges from the house rate line once the 6-hour cap pushes
EV charging to peak (the 4-rate rule). On an uncapped IOG account imp_rate_ev equals the
house rate, so the line sits on the house line (no divergence). A block with no stored
rate_ev (non-IOG / pre-split) falls back to the main rate — no crash, no divergence.
"""

import unittest
import energy_charts as ec

MAIN = "electricity_main"
OFF, PEAK = 0.05493, 0.323092


class TestDayChartEvRateLine(unittest.TestCase):

    def _blk(self, kwh, cost, rate, rate_ev=None):
        imp = {"kwh": kwh, "cost": cost, "rate": rate}
        if rate_ev is not None:
            imp["rate_ev"] = rate_ev
        return {"start": "2026-07-26T12:00:00",
                "meters": {MAIN: {"meta": {}, "standing_charge": 0.0,
                                  "channels": {"import": imp}}}}

    def _html(self, block, ev_kwh=2.0):
        return ec.build_day_chart_html(
            "2026-07-26", [(24, block)],
            {MAIN: "#1f77b4", "ev_dispatch": "#8b5cf6"},
            block_minutes=30, currency="£",
            ev_slot_map={block["start"]: {"kwh": ev_kwh}})

    def test_capped_ev_rate_line_diverges_to_peak(self):
        # house off-peak, EV pushed to peak by the cap → EV rate line carries the peak.
        html = self._html(self._blk(3.0, 0.16, OFF, rate_ev=PEAK))
        self.assertIn("0.323092", html)      # EV rate line diverged to peak
        self.assertIn("0.05493", html)       # house line stays off-peak

    def test_uncapped_ev_rate_line_matches_house(self):
        # uncapped: rate_ev == house rate → no peak value anywhere.
        html = self._html(self._blk(3.0, 0.16, OFF, rate_ev=OFF))
        self.assertNotIn("0.323092", html)
        self.assertIn("0.05493", html)

    def test_missing_rate_ev_falls_back_to_house(self):
        # no rate_ev stored (non-IOG / pre-split) → EV line uses the main rate, no crash.
        html = self._html(self._blk(3.0, 0.16, OFF))
        self.assertIn("0.05493", html)
        self.assertNotIn("0.323092", html)


if __name__ == "__main__":
    unittest.main()
