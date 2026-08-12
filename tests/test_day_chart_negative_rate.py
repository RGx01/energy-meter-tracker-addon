"""Agile plunge-price: a NEGATIVE import rate must render below the axis on the
day chart, not clip flat at zero.

Two halves:
  1. build_day_chart_html carries the negative rate into the rate trace (server side —
     the rate is never floored to 0).
  2. generate_daily_import_export_charts' client-side _alignY2 gives the rate axis
     room below zero on an import-only day (previously hard-coded y2min = 0, which
     clipped the plunge-price rate line flat at the baseline even though the cost
     already showed the credit).
"""
import unittest
import energy_charts as ec


class TestDayChartNegativeRateLine(unittest.TestCase):

    def _blk(self, kwh, cost, rate):
        return {"meters": {"electricity_main": {"meta": {}, "standing_charge": 0.5,
                "channels": {"import": {"kwh": kwh, "cost": cost, "rate": rate}}}}}

    def _html(self, day_blocks):
        return ec.build_day_chart_html("2026-07-26", day_blocks,
                                       {"electricity_main": "#1f77b4"},
                                       block_minutes=30, currency="£")

    def test_negative_rate_reaches_the_rate_trace(self):
        # A plunge slot (rate -0.05, a credit) alongside a normal slot: the rate trace
        # must carry -0.05, not a clamped 0.
        db = [(12, self._blk(1.0, -0.05, -0.05)),
              (20, self._blk(2.0, 0.42, 0.21))]
        html = self._html(db)
        self.assertIn("-0.05", html)

    def test_alignY2_drops_rate_axis_below_zero_for_negative_rate(self):
        # The page-level axis alignment must handle a negative rate axis on an
        # import-only day, not the old unconditional y2min = 0 clamp.
        blocks = [{"start": "2026-07-26T12:00:00",
                   "meters": {"electricity_main": {
                       "meta": {"block_minutes": 30}, "standing_charge": 0.5,
                       "channels": {"import": {"kwh": 1.0, "cost": -0.05, "rate": -0.05}}}}}]
        page = ec.generate_daily_import_export_charts(blocks)
        self.assertIn("rateMin", page)                               # new branch present
        self.assertNotIn("y1top = y1range[1]; y2min = 0;", page)     # old one-line clamp gone


if __name__ == "__main__":
    unittest.main()
