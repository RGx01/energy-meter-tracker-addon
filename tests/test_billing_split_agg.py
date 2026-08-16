"""
test_billing_split_agg.py — calculate_billing_summary_for_period builds the BL-9
house/EV split (ev_by_rate / home_by_rate) alongside main_import_raw, with the
ex-VAT figure derived from each block's own exc/inc ratio. EV + Home reconstruct
the grid total; a block with no EV contributes Home only.
"""

import os
import sys
import unittest
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from energy_charts import calculate_billing_summary_for_period

P_START = datetime(2026, 1, 1, 0, 0, 0)
P_END = datetime(2026, 1, 2, 0, 0, 0)


def _block(start, imp):
    return {"start": start,
            "meters": {"electricity_main": {"meta": {"timezone": "UTC"},
                                            "channels": {"import": imp}}}}


class TestBillingSplitAgg(unittest.TestCase):

    def _summary(self, blocks):
        return calculate_billing_summary_for_period(blocks, P_START, P_END)

    def test_ev_home_split_and_exc(self):
        # 30 kWh import @ £1.70 inc (£1.619048 exc). EV 20 kWh @ 0.05 = £1.00;
        # Home 10 kWh remainder = £0.70 @ 0.07.
        s = self._summary([_block("2026-01-01T13:00:00",
            {"kwh": 30.0, "rate": 0.0567, "cost": 1.70, "cost_exc": 1.619048,
             "kwh_ev": 20.0, "cost_ev": 1.00, "rate_ev": 0.05})])
        ev = s["ev_by_rate"]
        home = s["home_by_rate"]
        self.assertEqual(ev[0.05]["kwh"], 20.0)
        self.assertEqual(ev[0.05]["cost"], 1.00)
        self.assertAlmostEqual(ev[0.05]["cost_exc"], 0.95, places=2)   # 1.00 * (1.619/1.70)
        self.assertEqual(home[0.07]["kwh"], 10.0)
        self.assertEqual(home[0.07]["cost"], 0.70)
        self.assertAlmostEqual(home[0.07]["cost_exc"], 0.67, places=2)

    def test_no_ev_is_home_only(self):
        s = self._summary([_block("2026-01-01T18:00:00",
            {"kwh": 1.0, "rate": 0.30, "cost": 0.30})])       # no split
        self.assertEqual(s["ev_by_rate"], {})
        self.assertEqual(s["home_by_rate"][0.30]["kwh"], 1.0)

    def test_ev_and_home_reconstruct_total(self):
        s = self._summary([
            _block("2026-01-01T02:00:00",   # off-peak dispatched: EV 2 + home 1
                {"kwh": 3.0, "rate": 0.05, "cost": 0.17,
                 "kwh_ev": 2.0, "cost_ev": 0.10, "rate_ev": 0.05}),
            _block("2026-01-01T18:00:00",   # daytime home, no EV
                {"kwh": 1.0, "rate": 0.30, "cost": 0.30}),
        ])
        ev_kwh = sum(v["kwh"] for v in s["ev_by_rate"].values())
        home_kwh = sum(v["kwh"] for v in s["home_by_rate"].values())
        raw_kwh = sum(v["kwh"] for v in s["main_import_raw"].values())
        self.assertAlmostEqual(ev_kwh + home_kwh, raw_kwh)   # 2 + (1+1) == 4
        self.assertAlmostEqual(ev_kwh, 2.0)
        self.assertAlmostEqual(home_kwh, 2.0)


    def test_mixed_band_collapses_to_transition(self):
        # A boundary block (mixed bands, blended rate) → the single transition
        # bucket, NOT a per-blend row; clean groupings stay empty here.
        s = self._summary([_block("2026-01-01T13:00:00",
            {"kwh": 3.0, "rate": 0.19, "cost": 0.57,
             "kwh_ev": 2.0, "cost_ev": 0.30, "rate_ev": 0.15,
             "ev_band": "mixed", "home_band": "mixed"})])
        self.assertEqual(s["ev_by_rate"], {})
        self.assertEqual(s["home_by_rate"], {})
        self.assertAlmostEqual(s["ev_transition"]["kwh"], 2.0)
        self.assertAlmostEqual(s["ev_transition"]["cost"], 0.30)
        self.assertAlmostEqual(s["home_transition"]["kwh"], 1.0)
        self.assertAlmostEqual(s["home_transition"]["cost"], 0.27)   # 0.57 - 0.30

    def test_clean_bands_still_grouped_by_rate(self):
        # Non-mixed / unbanded blocks keep exact per-rate rows and empty transition.
        s = self._summary([_block("2026-01-01T02:00:00",
            {"kwh": 3.0, "rate": 0.05, "cost": 0.17,
             "kwh_ev": 2.0, "cost_ev": 0.10, "rate_ev": 0.05, "ev_band": "off_peak"})])
        self.assertEqual(s["ev_by_rate"][0.05]["kwh"], 2.0)
        self.assertEqual(s["ev_transition"]["kwh"], 0.0)


if __name__ == "__main__":
    unittest.main()
