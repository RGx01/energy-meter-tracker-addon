"""Tests for csv_import.py — Octopus CSV parse + rate-from-cost derivation."""
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import csv_import as ci


OCTOPUS_CSV = (
    "Consumption (kwh), Estimated Cost Inc. Tax (p), Standing Charge Inc. Tax (p), Start, End\n"
    "0.108000,0.7560378,1.12,2024-07-01T01:00:00+01:00,2024-07-01T01:30:00+01:00\n"
    "0.127000,0.88904445,1.12,2024-07-01T01:30:00+01:00,2024-07-01T02:00:00+01:00\n"
    "0.105000,0.73503675,1.12,2024-07-01T02:00:00+01:00,2024-07-01T02:30:00+01:00\n"
)


def blk(start, kwh, cost, standing=None):
    return {"channel": "import", "block_start": start, "block_end": start,
            "kwh": kwh, "cost": cost, "standing": standing}


class TestParse(unittest.TestCase):
    def test_parses_octopus_format(self):
        r = ci.parse_octopus_csv(OCTOPUS_CSV, "import")
        self.assertTrue(r["ok"])
        self.assertEqual(len(r["blocks"]), 3)
        b = r["blocks"][0]
        # +01:00 (BST) 01:00 → 00:00 UTC, naive.
        self.assertEqual(b["block_start"], "2024-07-01T00:00:00")
        self.assertEqual(b["kwh"], 0.108)
        # pence → £ inc-VAT.
        self.assertAlmostEqual(b["cost"], 0.007560378, places=9)
        self.assertAlmostEqual(b["standing"], 0.0112, places=6)

    def test_derived_rate_is_offpeak(self):
        r = ci.parse_octopus_csv(OCTOPUS_CSV, "import")
        b = r["blocks"][0]
        rate = b["cost"] / b["kwh"]          # £/kWh inc-VAT
        self.assertAlmostEqual(rate, 0.0700035, places=6)   # ~7p — IOG off-peak

    def test_naive_timestamp_rejected(self):
        csv = ("Consumption (kwh),Estimated Cost Inc. Tax (p),Start,End\n"
               "1.0,7.0,2024-07-01T01:00:00,2024-07-01T01:30:00\n")   # no offset
        r = ci.parse_octopus_csv(csv, "import")
        self.assertEqual(len(r["blocks"]), 0)
        self.assertEqual(len(r["errors"]), 1)

    def test_missing_columns(self):
        r = ci.parse_octopus_csv("Foo,Bar\n1,2\n", "import")
        self.assertFalse(r["ok"])

    def test_rate_column_derives_cost(self):
        # Unit Rate (p/kWh) present → cost = rate × kWh (pence → £).
        csv = ("Start,End,Consumption (kWh),Unit Rate (p/kWh)\n"
               "2024-07-01T01:00:00+01:00,2024-07-01T01:30:00+01:00,2.0,24.5\n")
        r = ci.parse_octopus_csv(csv, "import")
        b = r["blocks"][0]
        self.assertAlmostEqual(b["rate"], 0.245, places=6)          # £/kWh
        self.assertAlmostEqual(b["cost"], 0.49, places=6)           # 0.245 × 2.0

    def test_rate_takes_priority_over_cost(self):
        # Both columns present → rate wins (cost = rate × kWh, not the cost column).
        csv = ("Start,End,Consumption (kWh),Unit Rate (p/kWh),Estimated Cost Inc. Tax (p)\n"
               "2024-07-01T01:00:00+01:00,2024-07-01T01:30:00+01:00,2.0,10.0,999.0\n")
        r = ci.parse_octopus_csv(csv, "import")
        b = r["blocks"][0]
        self.assertAlmostEqual(b["cost"], 0.20, places=6)          # 0.10 × 2.0, NOT 9.99

    def test_cost_used_when_no_rate(self):
        # No rate column → fall back to the explicit cost column (Octopus export).
        csv = ("Start,End,Consumption (kWh),Estimated Cost Inc. Tax (p)\n"
               "2024-07-01T01:00:00+01:00,2024-07-01T01:30:00+01:00,2.0,49.0\n")
        r = ci.parse_octopus_csv(csv, "import")
        b = r["blocks"][0]
        self.assertIsNone(b["rate"])
        self.assertAlmostEqual(b["cost"], 0.49, places=6)

    def test_exvat_columns_parsed_rate_first(self):
        # BL-23 (4.2 Slice D): optional ex-VAT columns, rate-first, disambiguated from
        # the inc columns (Inc. vs Exc. Tax) by the exclude filter.
        csv = ("Start,End,Consumption (kWh),Unit Rate (p/kWh),Unit Rate Exc. Tax (p/kWh),"
               "Standing Charge Inc. Tax (p),Standing Charge Exc. Tax (p)\n"
               "2024-07-01T01:00:00+01:00,2024-07-01T01:30:00+01:00,2.0,26.1537,24.9083,47.52,45.26\n")
        b = ci.parse_octopus_csv(csv, "import")["blocks"][0]
        self.assertAlmostEqual(b["rate"], 0.261537, places=6)          # inc rate
        self.assertAlmostEqual(b["rate_exc"], 0.249083, places=6)      # exc rate
        self.assertAlmostEqual(b["cost_exc"], 0.249083 * 2.0, places=6)  # rate_exc × kWh
        self.assertAlmostEqual(b["standing_exc"], 0.4526, places=6)
        self.assertEqual(b["exc_source"], "csv")

    def test_exc_cost_column_when_no_exc_rate(self):
        # Exc cost column used when no exc rate; inc/exc cost columns disambiguated.
        csv = ("Start,End,Consumption (kWh),Estimated Cost Inc. Tax (p),Estimated Cost Exc. Tax (p)\n"
               "2024-07-01T01:00:00+01:00,2024-07-01T01:30:00+01:00,2.0,52.31,49.82\n")
        b = ci.parse_octopus_csv(csv, "import")["blocks"][0]
        self.assertAlmostEqual(b["cost"], 0.5231, places=6)            # from inc-cost col
        self.assertAlmostEqual(b["cost_exc"], 0.4982, places=6)        # from exc-cost col
        self.assertEqual(b["exc_source"], "csv")

    def test_inc_only_csv_has_no_exc(self):
        # Back-compat: an inc-only CSV yields no exc — behaviour unchanged.
        b = ci.parse_octopus_csv(OCTOPUS_CSV, "import")["blocks"][0]
        self.assertIsNone(b.get("cost_exc"))
        self.assertIsNone(b.get("rate_exc"))
        self.assertIsNone(b.get("exc_source"))


class TestDeriveBanded(unittest.TestCase):
    def _banded(self):
        blocks = []
        # off-peak: 6 blocks at ~7p (with rounding noise)
        for i, c in enumerate([0.0700, 0.0701, 0.0699, 0.0700, 0.0702, 0.0698]):
            blocks.append(blk(f"2024-07-01T0{i}:00:00", 1.0, c))
        # peak: 4 blocks at ~25p
        for i, c in enumerate([0.2500, 0.2502, 0.2498, 0.2501]):
            blocks.append(blk(f"2024-07-01T1{i}:00:00", 1.0, c))
        # a DISPATCH block at a peak-time slot but billed off-peak (7p)
        blocks.append(blk("2024-07-01T20:00:00", 1.0, 0.0700))
        return blocks

    def test_two_tiers_offpeak_lower(self):
        d = ci.derive_rates(self._banded())
        self.assertEqual(len(d["periods"]), 1)
        p = d["periods"][0]
        self.assertEqual(p["kind"], "banded")
        tiers = {t["label"]: t for t in p["tiers"]}
        self.assertLess(tiers["off_peak"]["rate"], tiers["peak"]["rate"])
        self.assertAlmostEqual(tiers["off_peak"]["rate"], 0.07, places=3)
        self.assertAlmostEqual(tiers["peak"]["rate"], 0.25, places=3)

    def test_dispatch_counted_offpeak(self):
        d = ci.derive_rates(self._banded())
        # 6 off-peak + 1 dispatch = 7 kWh off-peak; 4 peak.
        self.assertAlmostEqual(d["off_peak_kwh"], 7.0, places=6)
        self.assertAlmostEqual(d["peak_kwh"], 4.0, places=6)
        self.assertEqual(d["flags"]["2024-07-01T20:00:00"]["tier"], "off_peak")

    def test_reconciles_to_csv_cost(self):
        blocks = self._banded()
        d = ci.derive_rates(blocks)
        rec = ci.reconcile(blocks, d["flags"])
        self.assertTrue(rec["ok"])
        self.assertLess(rec["pct_diff"], 1.0)


class TestDeriveFlatAndAgile(unittest.TestCase):
    def test_flat_single_tier(self):
        blocks = [blk(f"2024-03-01T0{i}:00:00", 1.0, c)
                  for i, c in enumerate([0.2400, 0.2405, 0.2398, 0.2402])]
        d = ci.derive_rates(blocks)
        p = d["periods"][0]
        self.assertEqual(p["kind"], "flat")
        self.assertEqual(len(p["tiers"]), 1)
        self.assertEqual(d["off_peak_kwh"], 0.0)      # no banding

    def test_agile_non_banded(self):
        costs = [0.05, 0.10, 0.15, 0.21, 0.28, 0.34, 0.41]
        blocks = [blk(f"2024-03-01T0{i}:00:00", 1.0, c) for i, c in enumerate(costs)]
        d = ci.derive_rates(blocks)
        self.assertEqual(d["periods"][0]["kind"], "non_banded")
        self.assertEqual(d["off_peak_kwh"], 0.0)
        # every block still has a per-block rate for the bill
        self.assertIsNotNone(d["flags"]["2024-03-01T00:00:00"]["rate"])


class TestEdgeAndStanding(unittest.TestCase):
    def test_zero_consumption_excluded(self):
        blocks = [blk("2024-03-01T00:00:00", 0.0, 0.0),
                  blk("2024-03-01T00:30:00", 1.0, 0.07)]
        d = ci.derive_rates(blocks)
        self.assertNotIn("2024-03-01T00:00:00", d["flags"])   # no rate forced
        self.assertIn("2024-03-01T00:30:00", d["flags"])

    def test_standing_summed_per_day(self):
        blocks = [blk("2024-03-01T00:00:00", 1.0, 0.07, standing=0.0112),
                  blk("2024-03-01T00:30:00", 1.0, 0.07, standing=0.0112)]
        d = ci.derive_rates(blocks)
        self.assertAlmostEqual(d["periods"][0]["standing_daily"], 0.0224, places=6)


class TestPeriods(unittest.TestCase):
    def test_two_tariff_periods(self):
        blocks = ([blk(f"2024-02-0{i+1}T00:00:00", 1.0, 0.07) for i in range(3)]
                  + [blk(f"2024-08-0{i+1}T00:00:00", 1.0, 0.10) for i in range(3)])
        periods = [("2024-01-01T00:00:00", "2024-06-01T00:00:00"),
                   ("2024-06-01T00:00:00", "2025-01-01T00:00:00")]
        d = ci.derive_rates(blocks, periods=periods)
        self.assertEqual(len(d["periods"]), 2)
        self.assertAlmostEqual(d["periods"][0]["tiers"][0]["rate"], 0.07, places=4)
        self.assertAlmostEqual(d["periods"][1]["tiers"][0]["rate"], 0.10, places=4)


class TestTemplateGeneration(unittest.TestCase):
    """The gap/blank templates must round-trip cleanly through the same parser
    that imports them (schema alignment) and carry DST-correct local offsets."""

    def test_gap_template_roundtrips_to_utc_slots(self):
        text = ci.gap_template_csv("2024-07-01T00:00:00", "2024-07-01T02:00:00",
                                   block_minutes=30, tz_name="Europe/London")
        parsed = ci.parse_octopus_csv(text, "import")
        self.assertTrue(parsed["ok"])
        self.assertEqual([b["block_start"] for b in parsed["blocks"]],
                         ["2024-07-01T00:00:00", "2024-07-01T00:30:00",
                          "2024-07-01T01:00:00", "2024-07-01T01:30:00"])

    def test_gap_template_headers_and_blank_data(self):
        text = ci.gap_template_csv("2024-07-01T00:00:00", "2024-07-01T00:30:00")
        lines = text.strip().splitlines()
        self.assertEqual(
            lines[0],
            "Start,End,Consumption (kWh),Unit Rate (p/kWh),"
            "Estimated Cost Inc. Tax (p),Standing Charge Inc. Tax (p),"
            "Unit Rate Exc. Tax (p/kWh),Standing Charge Exc. Tax (p)")   # CSV v2
        self.assertTrue(lines[1].endswith(",,,,,,"))   # six blank data columns

    def test_gap_template_still_imports_inc_only(self):
        # v2 header is additive: a gap template with blank exc columns parses fine and
        # yields no exc — back-compatible.
        text = ci.gap_template_csv("2024-07-01T00:00:00", "2024-07-01T01:00:00")
        parsed = ci.parse_octopus_csv(text, "import")
        self.assertTrue(parsed["ok"])
        self.assertTrue(all(b.get("cost_exc") is None for b in parsed["blocks"]))

    def test_gap_template_local_offsets_track_dst(self):
        summer = ci.gap_template_csv("2024-07-01T00:00:00", "2024-07-01T00:30:00", tz_name="Europe/London")
        winter = ci.gap_template_csv("2024-01-01T00:00:00", "2024-01-01T00:30:00", tz_name="Europe/London")
        self.assertIn("+01:00", summer)   # BST
        self.assertIn("+00:00", winter)   # GMT

    def test_blank_template_parses_with_example_values(self):
        parsed = ci.parse_octopus_csv(ci.blank_template_csv(), "import")
        self.assertTrue(parsed["ok"])
        self.assertEqual(len(parsed["blocks"]), 4)
        b0 = parsed["blocks"][0]
        self.assertGreater(b0["kwh"], 0)
        self.assertIsNotNone(b0["cost"])
        # v2: the example ex-VAT rate parses through to cost_exc.
        self.assertIsNotNone(b0["cost_exc"])
        self.assertEqual(b0["exc_source"], "csv")
        self.assertLess(b0["cost_exc"], b0["cost"])   # exc < inc

    def test_slots_template_arbitrary_slots(self):
        # Non-contiguous slots (the escape hatch): one row per start, blank data,
        # local DST-correct offsets, round-trips back through the parser to UTC.
        starts = ["2025-03-05T00:00:00", "2025-03-05T00:30:00", "2025-07-01T13:00:00"]
        text = ci.slots_template_csv(starts, block_minutes=30, tz_name="Europe/London")
        lines = text.strip().splitlines()
        self.assertEqual(len(lines), 1 + len(starts))               # header + 3 rows
        self.assertTrue(lines[1].endswith(",,,,"))                  # four blank data cols
        self.assertIn("+00:00", text)                              # GMT (March 5)
        self.assertIn("+01:00", text)                              # BST (July 1)
        parsed = ci.parse_octopus_csv(text, "import")
        self.assertEqual([b["block_start"] for b in parsed["blocks"]], starts)

    def test_slots_template_skips_unparseable(self):
        text = ci.slots_template_csv(["", None, "not-a-date", "2025-03-05T00:00:00"])
        self.assertEqual(len(text.strip().splitlines()), 2)         # header + 1 valid


if __name__ == "__main__":
    unittest.main()
