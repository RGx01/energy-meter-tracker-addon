"""
Device-breakdown bucket labels: BOTH supplier vocabularies, and refusal on a third.

#481. On 2026-09-28 Octopus renamed the getDeviceConsumptionBreakdown statistic labels
from the v1 enum names (CONSUMPTION_CHARGE_EV_DEVICE_OFF_PEAK_B, ..._ECO7_NIGHT_B) to
display strings (EV LOW RATE, HOME LOW RATE, EV/HOME STANDARD RATE) — retrospectively,
so the API serves v2 even for slots that settled under v1.

The old parser classified EV by `"EV_DEVICE" in label` with an unconditional `else: home`,
and derived the band from `label_band(lab)` where an unrecognised label returns None and
fell through to "peak". So every v2 bucket became home/peak: the EV split silently
vanished account-wide, and off-peak energy was stamped peak. Nothing logged, nothing
flagged, and the affected blocks were stamped `measured` so no later pass revisited them.

These tests pin the vocabulary handling AND the refusal path.
"""
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from kraken_api_client import KrakenAPIClient, bucket_side, label_band


def _stat(label, value, ppu_pence, cost_pence=0.0):
    return {"type": "TOU_BUCKET_COST", "label": label, "value": value,
            "costInclTax": {"estimatedAmount": cost_pence,
                            "pricePerUnit": {"amount": ppu_pence}},
            "costExclTax": {"estimatedAmount": cost_pence,
                            "pricePerUnit": {"amount": ppu_pence}}}


def _node(stats, start="2026-09-27T02:30:00Z"):
    return {"startAt": start, "metaData": {"statistics": stats}}


class TestBucketSide(unittest.TestCase):
    def test_v2_display_labels(self):
        """The vocabulary Octopus switched to — this is what #481 missed."""
        self.assertEqual(bucket_side("EV LOW RATE"), "ev")
        self.assertEqual(bucket_side("EV STANDARD RATE"), "ev")
        self.assertEqual(bucket_side("HOME LOW RATE"), "home")
        self.assertEqual(bucket_side("HOME STANDARD RATE"), "home")

    def test_v1_enum_labels_still_classify(self):
        """No regression: a cached or replayed v1 payload must parse as before."""
        self.assertEqual(bucket_side("CONSUMPTION_CHARGE_EV_DEVICE_OFF_PEAK_B"), "ev")
        self.assertEqual(bucket_side("CONSUMPTION_CHARGE_EV_DEVICE_PEAK_B"), "ev")
        self.assertEqual(bucket_side("CONSUMPTION_CHARGE_ECO7_NIGHT_B"), "home")
        self.assertEqual(bucket_side("CONSUMPTION_CHARGE_ECO7_DAY_B"), "home")

    def test_pre_smb_single_bucket_is_house(self):
        """Pre-migration IOG serves OFF_PEAK / STANDARD_RATE with no device buckets."""
        self.assertEqual(bucket_side("OFF_PEAK"), "home")
        self.assertEqual(bucket_side("STANDARD_RATE"), "home")
        self.assertEqual(bucket_side("CONSUMPTION"), "home")

    def test_unknown_label_refuses(self):
        """A fourth vocabulary must return None, not silently become home."""
        self.assertIsNone(bucket_side("SOLAR DIVERTER TARIFF"))
        self.assertIsNone(bucket_side("WHATEVER_COMES_NEXT"))
        self.assertIsNone(bucket_side(""))
        self.assertIsNone(bucket_side(None))


class TestLabelBandV2(unittest.TestCase):
    def test_low_rate_is_off_peak(self):
        self.assertIs(label_band("EV LOW RATE"), True)
        self.assertIs(label_band("HOME LOW RATE"), True)

    def test_standard_rate_is_peak(self):
        self.assertIs(label_band("EV STANDARD RATE"), False)
        self.assertIs(label_band("HOME STANDARD RATE"), False)

    def test_v1_bands_unchanged(self):
        self.assertIs(label_band("CONSUMPTION_CHARGE_EV_DEVICE_OFF_PEAK_B"), True)
        self.assertIs(label_band("CONSUMPTION_CHARGE_ECO7_NIGHT_B"), True)
        self.assertIs(label_band("CONSUMPTION_CHARGE_ECO7_DAY_B"), False)
        self.assertIs(label_band("OFF_PEAK"), True)
        self.assertIs(label_band("STANDARD_RATE"), False)


class TestParseBreakdownNode(unittest.TestCase):
    """The reported slot: 2026-09-27T02:30 — HOME LOW RATE 2.99113 + EV LOW RATE 3.26387."""

    def test_v2_node_carves_ev_and_bands_off_peak(self):
        p = KrakenAPIClient._parse_breakdown_node(_node([
            _stat("HOME LOW RATE", 2.99113, 5.49297, 16.43),
            _stat("EV LOW RATE", 3.26387, 5.49297, 17.93),
        ]))
        self.assertIsNotNone(p)
        self.assertAlmostEqual(p["ev_kwh"], 3.26387, places=5,
                               msg="#481: EV bucket booked as home — the whole defect")
        self.assertAlmostEqual(p["home_kwh"], 2.99113, places=5)
        self.assertEqual(p["ev_band"], "off_peak",
                         msg="#481: LOW RATE is off-peak, was being stamped peak")
        self.assertEqual(p["home_band"], "off_peak")
        self.assertAlmostEqual(p["ev_rate"], 0.054930, places=6)

    def test_v2_standard_rate_node_is_peak(self):
        p = KrakenAPIClient._parse_breakdown_node(_node([
            _stat("HOME STANDARD RATE", 0.002, 32.30924, 0.065),
        ]))
        self.assertEqual(p["home_band"], "peak")
        self.assertAlmostEqual(p["home_kwh"], 0.002, places=6)
        self.assertAlmostEqual(p["ev_kwh"], 0.0, places=9)

    def test_v1_node_unchanged(self):
        p = KrakenAPIClient._parse_breakdown_node(_node([
            _stat("CONSUMPTION_CHARGE_ECO7_NIGHT_B", 0.153, 5.493, 0.84),
            _stat("CONSUMPTION_CHARGE_EV_DEVICE_OFF_PEAK_B", 3.497, 5.493, 19.21),
        ]))
        self.assertAlmostEqual(p["ev_kwh"], 3.497, places=5)
        self.assertAlmostEqual(p["home_kwh"], 0.153, places=5)
        self.assertEqual(p["ev_band"], "off_peak")

    def test_unknown_label_returns_none_rather_than_a_wrong_split(self):
        """The guard: refuse the node so nothing is cached and the carve survives."""
        p = KrakenAPIClient._parse_breakdown_node(_node([
            _stat("HOME LOW RATE", 2.0, 5.49297, 11.0),
            _stat("BATTERY DISCHARGE CREDIT", 1.5, 5.49297, 8.2),
        ]))
        self.assertIsNone(p, "an unrecognised bucket must withhold the split, not guess")

    def test_standing_charge_row_still_skipped(self):
        p = KrakenAPIClient._parse_breakdown_node(_node([
            {"type": "STANDING_CHARGE_COST", "label": "STANDING CHARGE", "value": 1.0},
            _stat("EV LOW RATE", 3.0, 5.49297, 16.5),
        ]))
        self.assertIsNotNone(p)
        self.assertAlmostEqual(p["ev_kwh"], 3.0, places=6)


if __name__ == "__main__":
    unittest.main()
