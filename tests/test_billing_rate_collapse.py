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

    def _rate_blocks(self, rate_kwhs):
        # rate_kwhs: list of (inc_rate, kwh); builds one block each, cost = rate*kwh
        out=[]
        for i,(r,k) in enumerate(rate_kwhs):
            out.append({"start": f"2026-02-12T{i%24:02d}:{'30' if i%2 else '00'}:00",
                        "meters": {"electricity_main": {"meta": {}, "standing_charge": 0.5,
                            "channels": {"import": {"kwh": k, "rate": r, "cost": round(r*k,6)}}}}})
        return out

    def test_B2_jitter_bands_merge_to_one_row(self):
        # rate-from-cost jitter: 0.070000 vs 0.070003 (same off-peak tariff) + a peak band.
        bm = ec._bill_method_breakdown(self._rate_blocks(
            [(0.070000, 52.0), (0.070003, 855.0), (0.2812, 6.6)]), period_vat=0.05)
        offpeak = [r for r in bm["rows"] if r["rate_exc"] < 0.15]
        self.assertEqual(len(offpeak), 1)                       # one merged off-peak row, not two
        self.assertAlmostEqual(offpeak[0]["kwh"], 907.0, places=1)   # 52 + 855
        self.assertEqual(len([r for r in bm["rows"] if r["rate_exc"] >= 0.15]), 1)  # peak separate

    def test_B2_real_rate_change_stays_separate(self):
        # a genuine in-period off-peak change (>_SPLIT_BAND_EPS): 0.070 -> 0.085 must NOT merge.
        bm = ec._bill_method_breakdown(self._rate_blocks(
            [(0.070, 100.0), (0.085, 80.0)]), period_vat=0.05)
        self.assertEqual(len(bm["rows"]), 2)                    # two distinct bands
        self.assertLess(bm["rows"][0]["rate_exc"], bm["rows"][1]["rate_exc"])

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


class TestBillMethodStandingCharge(unittest.TestCase):
    """A standing-charge RATE change mid-period (a price-cap or tariff switch) must
    show as separate lines in the ex-VAT bill method — not one averaged rate that
    matches neither. (March: £0.476317/day inc until the switch, then £0.504559/day.)"""

    def _blocks(self):
        return [{"start": "2026-03-05T12:00:00", "meters": {"electricity_main": {
                 "meta": {}, "channels": {"import": {"kwh": 1.0, "rate": 0.30, "cost": 0.30}}}}}]

    def test_standing_change_breaks_into_two_rows(self):
        sib = {}
        for d in range(1, 17):  sib[f"2026-03-{d:02d}"] = 0.476317   # 16 days, old cap
        for d in range(17, 32): sib[f"2026-03-{d:02d}"] = 0.504559   # 15 days, new cap
        bm = ec._bill_method_breakdown(self._blocks(), standing_inc_by_day=sib)
        self.assertEqual(len(bm["standing_rows"]), 2)
        self.assertEqual(sorted(s["days"] for s in bm["standing_rows"]), [15, 16])
        rates = sorted(s["rate_exc"] for s in bm["standing_rows"])
        self.assertAlmostEqual(rates[0], 0.476317 / 1.05, places=3)
        self.assertAlmostEqual(rates[1], 0.504559 / 1.05, places=3)
        # no money is lost/created: the rows sum to the overall standing total
        self.assertAlmostEqual(sum(s["cost_exc"] for s in bm["standing_rows"]),
                               bm["standing_exc"], places=2)

    def test_single_standing_rate_stays_one_row(self):
        sib = {f"2026-03-{d:02d}": 0.476317 for d in range(1, 32)}
        bm = ec._bill_method_breakdown(self._blocks(), standing_inc_by_day=sib)
        self.assertEqual(len(bm["standing_rows"]), 1)
        self.assertEqual(bm["standing_rows"][0]["days"], 31)


if __name__ == "__main__":
    unittest.main()
