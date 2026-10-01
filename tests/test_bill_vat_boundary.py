"""A bill period spanning 1 Oct 2026's VAT change (5% -> 0%) — 4.5.19.

4.5.18's bill-method breakdown took ONE VAT rate per period (its first half-hour's): the
3 Sep - 2 Oct card divided 1 Oct's 0%-VAT standing charge by 1.05 (£0.4576/day), labelled
the VAT "@ 5%", and listed 1 Oct's ex-VAT EV/Home rows as separate 0.0523 rows beside
September's. Figures are the IOG-SMB tariff's published rates.
"""
import os
import sys
import unittest
from datetime import date

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import energy_charts as ec                              # noqa: E402
import vat_calendar as vc                               # noqa: E402


def _blk(start, kwh, inc, exc, standing):
    return {"start": start, "meters": {"electricity_main": {
        "standing_charge": standing,
        "channels": {"import": {"kwh": kwh, "rate": inc, "rate_exc": exc,
                                "cost": round(kwh * inc, 6), "cost_exc": round(kwh * exc, 6)}}}}}


BLOCKS = [_blk("2026-09-30T12:00:00", 2.0, 0.05493, 0.052314, 0.504559),    # 5%
          _blk("2026-09-30T23:00:00", 1.0, 0.052314, 0.052314, 0.480532),   # 00:00 BST 1 Oct, 0%
          _blk("2026-10-01T12:00:00", 1.0, 0.052314, 0.052314, 0.480532)]   # 0%
STANDING = {date(2026, 9, 30): 0.504559, date(2026, 10, 1): 0.480532}


class TheBreakdownTakesEachDatesVat(unittest.TestCase):

    def setUp(self):
        self.bm = ec._bill_method_breakdown(BLOCKS, period_vat=0.05, standing_inc_by_day=STANDING,
                                            vat_at=lambda w: vc.resolve_vat(w))

    def test_1_october_standing_is_not_divided_by_1_05(self):
        self.assertEqual(self.bm["standing_rows"], [{"days": 2, "rate_exc": 0.4805, "cost_exc": 0.96}])
        self.assertAlmostEqual(self.bm["standing_exc"], 0.96, places=2)

    def test_one_ex_vat_row_either_side_of_the_change(self):
        self.assertEqual(self.bm["rows"], [{"rate_exc": 0.0523, "kwh": 4.0, "cost_exc": 0.209}])

    def test_the_vat_line_names_both_rates(self):
        self.assertEqual(self.bm["vat_label"], "VAT @ 5% to 30 Sep, 0% from 1 Oct")
        self.assertIsNone(self.bm["vat_rate"])

    def test_vat_is_inc_minus_exc(self):
        inc = 2 * 0.05493 + 2 * 0.052314 + 0.504559 + 0.480532
        exc = 4 * 0.052314 + 0.504559 / 1.05 + 0.480532
        self.assertAlmostEqual(self.bm["vat_amount"], round(inc - exc, 2), places=2)
        self.assertAlmostEqual(self.bm["inc_total"], round(inc, 2), places=2)

    def test_a_single_vat_period_is_unchanged(self):
        """GUARD: a period wholly at 5% reads as it always did (fails on 4.5.18 only because
        `vat_at` is new there)."""
        bm = ec._bill_method_breakdown(BLOCKS[:1], period_vat=0.05,
                                       standing_inc_by_day={date(2026, 9, 30): 0.504559},
                                       vat_at=lambda w: vc.resolve_vat(w))
        self.assertEqual(bm["vat_label"], "VAT @ 5%")
        self.assertEqual(bm["vat_rate"], 0.05)
        self.assertEqual(bm["standing_rows"], [{"days": 1, "rate_exc": 0.4805, "cost_exc": 0.48}])


class TheSplitRowsBandOnTheExVatRate(unittest.TestCase):

    def test_one_ev_row_either_side_of_the_change(self):
        summary = {"ev_by_rate": {
            0.05493:  {"kwh": 564.329, "cost": 30.999, "cost_exc": 29.522, "cost_exc_raw": 29.5222},
            0.052314: {"kwh": 9.820, "cost": 0.514, "cost_exc": 0.514, "cost_exc_raw": 0.51372}},
            "home_by_rate": {
            0.05493:  {"kwh": 430.783, "cost": 23.663, "cost_exc": 22.536, "cost_exc_raw": 22.5360},
            0.052314: {"kwh": 7.132, "cost": 0.373, "cost_exc": 0.373, "cost_exc_raw": 0.37310}}}
        html = ec._bill_split_rows(summary, "£", exc=True)
        self.assertEqual(html.count("<td>EV</td>"), 1, html)
        self.assertEqual(html.count("<td>Home</td>"), 1, html)
        self.assertIn("<td>574.149</td>", html)

    def test_the_inc_table_still_shows_both_inc_rates(self):
        """GUARD: the inc-VAT table keeps 5.493 and 5.2314 as their own rows."""
        summary = {"ev_by_rate": {
            0.05493:  {"kwh": 1.0, "cost": 0.05493, "cost_exc": 0.052314},
            0.052314: {"kwh": 1.0, "cost": 0.052314, "cost_exc": 0.052314}}, "home_by_rate": {}}
        html = ec._bill_split_rows(summary, "£")
        self.assertEqual(html.count("<td>EV</td>"), 2, html)


if __name__ == "__main__":
    unittest.main()
