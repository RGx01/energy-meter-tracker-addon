"""
test_billing_split_polish.py — BL-9 billing-summary polish:
  * ex-VAT fallback: a block with an EV split but no stored cost_exc (not yet settled)
    derives exc as inc ÷ (1+VAT) instead of contributing £0 (which diluted the rate);
  * clean-band collapse: near-identical rate keys (per-half-hour settlement jitter)
    fold into one EV + one Home row, genuine bands stay separate, transition untouched.
"""

import os
import sys
import unittest
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from energy_charts import calculate_billing_summary_for_period, _bill_split_rows

P_START = datetime(2026, 1, 1)
P_END = datetime(2026, 1, 2)


def _block(start, imp):
    return {"start": start,
            "meters": {"electricity_main": {"meta": {"timezone": "UTC"},
                                            "channels": {"import": imp}}}}


class TestExcFallback(unittest.TestCase):

    def test_missing_cost_exc_uses_vat_fallback(self):
        # 20 kWh EV @ 0.05 = £1.00 inc, no cost_exc stored → exc = 1.00 / 1.05.
        s = calculate_billing_summary_for_period([_block("2026-01-01T02:00:00",
            {"kwh": 30.0, "rate": 0.0567, "cost": 1.70,
             "kwh_ev": 20.0, "cost_ev": 1.00, "rate_ev": 0.05})],
            P_START, P_END)                       # store=None → 5% default
        ev = s["ev_by_rate"][0.05]
        self.assertAlmostEqual(ev["cost_exc"], 1.00 / 1.05, places=2)
        # NOT zero (the dilution bug) and NOT the inc figure.
        self.assertGreater(ev["cost_exc"], 0.9)

    def test_present_cost_exc_still_exact(self):
        s = calculate_billing_summary_for_period([_block("2026-01-01T02:00:00",
            {"kwh": 30.0, "rate": 0.0567, "cost": 1.70, "cost_exc": 1.619048,
             "kwh_ev": 20.0, "cost_ev": 1.00, "rate_ev": 0.05})], P_START, P_END)
        # exact per-block ratio (1.619048/1.70), not the flat fallback
        self.assertAlmostEqual(s["ev_by_rate"][0.05]["cost_exc"],
                               1.00 * (1.619048 / 1.70), places=2)


class TestBandCollapse(unittest.TestCase):

    def _rows(self, summary, exc=False):
        html = _bill_split_rows(summary, "£", exc=exc)
        return [l for l in (html or "").splitlines() if "<tr>" in l]

    def test_jittered_rates_fold_to_one_band(self):
        # Peak jittered across 0.3230/0.3231/0.3232 (settlement rounding) → ONE EV row.
        summary = {"ev_by_rate": {
            0.3230: {"kwh": 1.0, "cost": 0.323, "cost_exc": 0.307},
            0.3231: {"kwh": 1.0, "cost": 0.3231, "cost_exc": 0.307},
            0.3232: {"kwh": 1.0, "cost": 0.3232, "cost_exc": 0.307}},
            "home_by_rate": {}}
        rows = self._rows(summary)
        self.assertEqual(len(rows), 1)
        self.assertIn("3.000", rows[0])           # 3 kWh folded

    def test_genuine_bands_stay_separate(self):
        summary = {"ev_by_rate": {
            0.0549: {"kwh": 10.0, "cost": 0.549, "cost_exc": 0.523},
            0.3231: {"kwh": 1.0, "cost": 0.3231, "cost_exc": 0.307}},
            "home_by_rate": {
            0.0549: {"kwh": 5.0, "cost": 0.2745, "cost_exc": 0.261}}}
        rows = self._rows(summary)
        # EV off-peak, Home off-peak, EV peak = 3 rows (bands not merged)
        self.assertEqual(len(rows), 3)

    def test_transition_rows_survive_collapse(self):
        summary = {"ev_by_rate": {0.05: {"kwh": 10.0, "cost": 0.5, "cost_exc": 0.48}},
                   "home_by_rate": {},
                   "ev_transition": {"kwh": 2.0, "cost": 0.30, "cost_exc": 0.29},
                   "home_transition": {"kwh": 2.0, "cost": 0.37, "cost_exc": 0.35}}
        html = _bill_split_rows(summary, "£")
        self.assertEqual(html.count("(transition)"), 2)


if __name__ == "__main__":
    unittest.main()
