"""Agile has ~48 distinct half-hourly rates a day → hundreds across a bill period,
which makes the billing summary's per-rate breakdown unreadable. When a channel has
more than _MAX_RATE_ROWS distinct rates, the rows collapse to a single kWh-weighted
average row. The Total row (kWh + cost) must be unchanged either way.
"""
import unittest
import energy_charts as ec


class TestBillRateRowsCollapse(unittest.TestCase):

    def _chans(self, n):
        # n distinct rates, 1 kWh each, cost = rate × 1 kWh.
        return {round(0.10 + 0.01 * i, 4): {"kwh": 1.0, "cost": round(0.10 + 0.01 * i, 2)}
                for i in range(n)}

    def test_collapses_above_threshold(self):
        chans = self._chans(6)                       # > 5 → collapse
        html, tk, tc = ec._bill_rate_rows(chans, "£")
        self.assertIn("avg of 6 rates", html)
        self.assertEqual(html.count("<tr>"), 1)      # one row, not six
        # totals identical to summing the same rounded per-rate values
        exp_tk = round(sum(round(chans[r]["kwh"], 3) for r in chans), 3)
        exp_tc = round(sum(round(chans[r]["cost"], 2) for r in chans), 2)
        self.assertEqual((tk, tc), (exp_tk, exp_tc))

    def test_no_collapse_at_threshold(self):
        chans = self._chans(5)                       # == 5 → keep detail
        html, tk, tc = ec._bill_rate_rows(chans, "£")
        self.assertNotIn("avg of", html)
        self.assertEqual(html.count("<tr>"), 5)

    def test_collapsed_average_is_kwh_weighted(self):
        # 5 kWh @ 0.10 + 1 kWh @ 0.40 over 6 rates → avg = 0.90 / 6 = 0.15
        chans = {round(0.10, 4): {"kwh": 5.0, "cost": 0.50}}
        for i in range(1, 6):
            chans[round(0.40 + 0.001 * i, 4)] = {"kwh": 0.0, "cost": 0.0}
        chans[round(0.40, 4)] = {"kwh": 1.0, "cost": 0.40}
        html, tk, tc = ec._bill_rate_rows(chans, "£")
        self.assertIn("avg of", html)
        self.assertIn("0.1500", html)                # 0.90 / 6.0


class TestBillMethodCollapse(unittest.TestCase):

    def _blocks(self, n):
        return [{"start": f"2026-07-26T{h:02d}:00:00",
                 "meters": {"electricity_main": {
                     "meta": {}, "standing_charge": 0.5,
                     "channels": {"import": {"kwh": 1.0, "rate": round(0.10 + 0.01 * h, 4),
                                             "cost": round(0.10 + 0.01 * h, 4)}}}}}
                for h in range(n)]

    def test_collapses_and_keeps_total_exc(self):
        bm = ec._bill_method_breakdown(self._blocks(6))
        self.assertEqual(len(bm["rows"]), 1)
        self.assertTrue(bm["rows"][0].get("collapsed"))
        self.assertEqual(bm["rows"][0]["n_rates"], 6)
        # the single row's cost matches Total (exc) (same raw sum, 3dp vs 2dp)
        self.assertAlmostEqual(bm["rows"][0]["cost_exc"], bm["energy_exc"], places=1)

    def test_no_collapse_for_few_rates(self):
        bm = ec._bill_method_breakdown(self._blocks(3))
        self.assertEqual(len(bm["rows"]), 3)
        self.assertFalse(any(r.get("collapsed") for r in bm["rows"]))


class TestSidePanelRateCollapse(unittest.TestCase):
    """The day-chart side panel (Direct import + per-device columns) uses a {rate: kwh}
    breakdown via _collapse_rate_kwh — it must fold on Agile with the SAME threshold as
    the billing summary, for both the house (Direct import) line and each device line."""

    def test_collapses_above_threshold_kwh_weighted(self):
        # 6 rates → one row; avg is kWh-weighted: (0.10*5 + 0.40*1)/6 = 0.90/6 = 0.15
        rk = {0.10: 5.0, 0.40: 1.0, 0.20: 0.0, 0.30: 0.0, 0.25: 0.0, 0.35: 0.0}
        # add non-trivial kwh to the other four so there are >5 counted rates
        rk = {0.10: 5.0, 0.40: 1.0, 0.20: 0.001, 0.30: 0.001, 0.25: 0.001, 0.35: 0.001}
        out = ec._collapse_rate_kwh(rk)
        self.assertEqual(len(out), 1)
        kwh, rate, n = out[0]
        self.assertEqual(n, 6)
        self.assertAlmostEqual(kwh, sum(rk.values()), places=4)
        # weighted avg = Σ rate*kwh / Σ kwh
        exp = sum(r * k for r, k in rk.items()) / sum(rk.values())
        self.assertAlmostEqual(rate, exp, places=6)

    def test_no_collapse_at_threshold(self):
        rk = {round(0.10 + 0.01 * i, 4): 1.0 for i in range(5)}   # 5 rates
        out = ec._collapse_rate_kwh(rk)
        self.assertEqual(len(out), 5)
        self.assertTrue(all(n is None for _, _, n in out))

    def test_trivial_kwh_rates_do_not_count(self):
        # 5 real rates + 3 negligible (kwh <= 0.0001) → NOT collapsed (only 5 count),
        # and the negligible ones are dropped from the output entirely.
        rk = {round(0.10 + 0.01 * i, 4): 1.0 for i in range(5)}
        rk.update({0.90: 0.00005, 0.91: 0.0, 0.92: 0.00001})
        out = ec._collapse_rate_kwh(rk)
        self.assertEqual(len(out), 5)

    def test_negative_agile_rate_survives_the_average(self):
        # A plunge rate (negative) with 6 rates → collapses, and the weighted average
        # can be negative — it must not be clamped.
        rk = {-0.05: 20.0, 0.02: 1.0, 0.03: 1.0, 0.10: 1.0, 0.12: 1.0, 0.15: 1.0}
        out = ec._collapse_rate_kwh(rk)
        self.assertEqual(len(out), 1)
        _, rate, n = out[0]
        self.assertEqual(n, 6)
        self.assertLess(rate, 0.0)      # plunge dominates → negative avg, not clamped


if __name__ == "__main__":
    unittest.main()
