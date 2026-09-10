"""C1 — the four-bucket Home/EV settled split parser (design §4).

Regression anchor: the summed-cost measurement read (`_parse_measurement_node`) gets the
right TOTAL but discards the split and per-bucket rate; the old probe grabbed the FIRST
bucket (ECO7_DAY=peak) instead of the value-bearing one. `_parse_breakdown_node` must read
Home = Σ ECO7_*, EV = Σ EV_DEVICE_*, with band+rate from whichever bucket carries the value.
"""
import os, sys, unittest
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from kraken_api_client import KrakenAPIClient

OFF_P, PEAK_P = 5.49297, 32.30924   # pence
OFF, PEAK = round(OFF_P / 100, 6), round(PEAK_P / 100, 6)


def _stat(label, value, rate_p):
    return {"type": "TOU_BUCKET_COST", "label": label, "value": value,
            "costInclTax": {"estimatedAmount": value * rate_p,
                            "pricePerUnit": {"amount": rate_p}},
            "costExclTax": {"estimatedAmount": value * rate_p * 0.95238,
                            "pricePerUnit": {"amount": rate_p * 0.95238}}}


def _node(start, stats):
    # all four buckets are always returned; only the active ones carry a value
    return {"startAt": start, "endAt": start, "value": sum(s["value"] for s in stats),
            "metaData": {"statistics": stats}}


class TestBreakdownParse(unittest.TestCase):
    def _p(self, stats, start="2026-08-30T12:00:00Z"):
        return KrakenAPIClient._parse_breakdown_node(_node(start, stats))

    def test_30aug_all_offpeak_split(self):
        # 30 Aug 13:00 local: home in ECO7_NIGHT, EV in EV_DEVICE_OFF_PEAK (both off-peak).
        # ECO7_DAY is present with value 0 (the bucket the probe wrongly grabbed).
        r = self._p([
            _stat("CONSUMPTION_CHARGE_ECO7_DAY_B", 0.0, PEAK_P),
            _stat("CONSUMPTION_CHARGE_ECO7_NIGHT_B", 1.980, OFF_P),
            _stat("CONSUMPTION_CHARGE_EV_DEVICE_OFF_PEAK_B", 3.296, OFF_P),
            _stat("CONSUMPTION_CHARGE_EV_DEVICE_PEAK_B", 0.0, PEAK_P),
        ])
        self.assertAlmostEqual(r["home_kwh"], 1.980, places=3)
        self.assertAlmostEqual(r["ev_kwh"], 3.296, places=3)
        self.assertAlmostEqual(r["home_rate"], OFF, places=5)   # NIGHT, not DAY (probe bug)
        self.assertAlmostEqual(r["ev_rate"], OFF, places=5)
        self.assertEqual(r["home_band"], "off_peak")
        self.assertEqual(r["ev_band"], "off_peak")

    def test_mixed_bands_home_peak_ev_offpeak(self):
        # The case the summed read CANNOT represent: home drew at PEAK while the EV
        # charged OFF_PEAK in a dispatch — each side must carry its own band + rate.
        r = self._p([
            _stat("CONSUMPTION_CHARGE_ECO7_DAY_B", 0.5, PEAK_P),
            _stat("CONSUMPTION_CHARGE_ECO7_NIGHT_B", 0.0, OFF_P),
            _stat("CONSUMPTION_CHARGE_EV_DEVICE_OFF_PEAK_B", 2.0, OFF_P),
            _stat("CONSUMPTION_CHARGE_EV_DEVICE_PEAK_B", 0.0, PEAK_P),
        ])
        self.assertEqual(r["home_band"], "peak")
        self.assertEqual(r["ev_band"], "off_peak")
        self.assertAlmostEqual(r["home_rate"], PEAK, places=5)
        self.assertAlmostEqual(r["ev_rate"], OFF, places=5)
        self.assertAlmostEqual(r["home_kwh"], 0.5, places=3)
        self.assertAlmostEqual(r["ev_kwh"], 2.0, places=3)

    def test_home_only_no_ev(self):
        r = self._p([
            _stat("CONSUMPTION_CHARGE_ECO7_DAY_B", 1.2, PEAK_P),
            _stat("CONSUMPTION_CHARGE_ECO7_NIGHT_B", 0.0, OFF_P),
        ])
        self.assertAlmostEqual(r["home_kwh"], 1.2, places=3)
        self.assertAlmostEqual(r["ev_kwh"], 0.0, places=6)
        self.assertEqual(r["home_band"], "peak")
        self.assertIsNone(r["ev_rate"])
        self.assertIsNone(r["ev_band"])

    def test_standing_charge_ignored_and_nonnode(self):
        r = self._p([
            {"type": "STANDING_CHARGE_COST", "label": "STANDING",
             "value": 0.0, "costInclTax": {"estimatedAmount": 47.0, "pricePerUnit": {"amount": 47.0}}},
            _stat("CONSUMPTION_CHARGE_ECO7_NIGHT_B", 0.3, OFF_P),
        ])
        self.assertAlmostEqual(r["home_kwh"], 0.3, places=3)
        self.assertEqual(r["home_band"], "off_peak")
        self.assertIsNone(KrakenAPIClient._parse_breakdown_node({}))  # no startAt


if __name__ == "__main__":
    unittest.main()
