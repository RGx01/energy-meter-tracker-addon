"""Band classification from Octopus statistic labels — region-suffix agnostic.

Octopus suffixes the newer tariffs' bucket labels with the GSP group letter (A-P, no
I or O): a Southern account sees ..._ECO7_DAY_H, an East-Midlands one ..._ECO7_DAY_B.
Fourteen regions means the suffix must never be enumerated, so these tests assert the
property (suffix-independence) rather than the two suffixes we happen to have seen.
"""
import unittest

from kraken_api_client import KrakenAPIClient, label_band

# Every GSP group letter in use. Not a lookup table the code may consult — the point is
# that the code must never need one, so the tests sweep all of them.
GSP_SUFFIXES = list("ABCDEFGHJKLMNP") + ["Z"]      # Z: a region that does not exist yet


def node(*stats, kwh=1.0, start="2026-09-03T18:00:00Z"):
    return {"startAt": start, "endAt": start, "value": kwh,
            "metaData": {"statistics": list(stats)}}


def stat(label, incl, excl=None, type_="CONSUMPTION_COST", value=None):
    s = {"type": type_, "label": label,
         "costInclTax": {"estimatedAmount": str(incl)},
         "costExclTax": {"estimatedAmount": str(excl if excl is not None else incl)}}
    if value is not None:
        s["value"] = value
    return s


def smb_buckets(suffix, *, day=0.0, night=0.0, ev_off=0.0, ev_peak=0.0, values=False):
    """The four IOG-SMB buckets Octopus returns on EVERY slot, zero where unused."""
    def v(x):
        return {"value": x} if values else {}
    return [
        stat(f"CONSUMPTION_CHARGE_ECO7_DAY_{suffix}", day, **v(day)),
        stat(f"CONSUMPTION_CHARGE_ECO7_NIGHT_{suffix}", night, **v(night)),
        stat(f"CONSUMPTION_CHARGE_EV_DEVICE_OFF_PEAK_{suffix}", ev_off, **v(ev_off)),
        stat(f"CONSUMPTION_CHARGE_EV_DEVICE_PEAK_{suffix}", ev_peak, **v(ev_peak)),
    ]


class TestLabelBand(unittest.TestCase):
    def test_legacy_iog_vocabulary_unsuffixed(self):
        self.assertIs(label_band("OFF_PEAK"), True)
        self.assertIs(label_band("STANDARD_RATE"), False)

    def test_flat_tariff_label_has_no_band(self):
        # A flat tariff's bare CONSUMPTION label carries no band. None (not False) so
        # _tariff_rate_for resolves by time of day instead of taking the day's high rate.
        self.assertIsNone(label_band("CONSUMPTION"))

    def test_unknown_and_empty(self):
        for lab in (None, "", "SOMETHING_NEW_ENTIRELY"):
            self.assertIsNone(label_band(lab))

    def test_off_peak_tested_before_peak(self):
        # "PEAK" is a substring of "OFF_PEAK" — order in the classifier matters.
        self.assertIs(label_band("CONSUMPTION_CHARGE_EV_DEVICE_OFF_PEAK_H"), True)
        self.assertIs(label_band("CONSUMPTION_CHARGE_EV_DEVICE_PEAK_H"), False)

    def test_home_and_ev_spell_off_peak_differently(self):
        # Same band, two words: home off-peak is NIGHT, EV off-peak is OFF_PEAK.
        self.assertIs(label_band("CONSUMPTION_CHARGE_ECO7_NIGHT_B"), True)
        self.assertIs(label_band("CONSUMPTION_CHARGE_EV_DEVICE_OFF_PEAK_B"), True)
        self.assertIs(label_band("CONSUMPTION_CHARGE_ECO7_DAY_B"), False)

    def test_every_region_suffix_classifies_identically(self):
        for sfx in GSP_SUFFIXES:
            self.assertIs(label_band(f"CONSUMPTION_CHARGE_ECO7_NIGHT_{sfx}"), True, sfx)
            self.assertIs(label_band(f"CONSUMPTION_CHARGE_ECO7_DAY_{sfx}"), False, sfx)
            self.assertIs(label_band(f"CONSUMPTION_CHARGE_EV_DEVICE_OFF_PEAK_{sfx}"), True, sfx)
            self.assertIs(label_band(f"CONSUMPTION_CHARGE_EV_DEVICE_PEAK_{sfx}"), False, sfx)


class TestParseMeasurementNodeBand(unittest.TestCase):
    """The band must come from the bucket that CARRIES the charge, not the label set."""

    def parse(self, n):
        return KrakenAPIClient._parse_measurement_node(n)

    def test_smb_off_peak_slot_is_off_peak_in_every_region(self):
        # THE REGRESSION: all four labels are present on every SMB slot, so the old
        # label-SET test ("OFF_PEAK" not in labels) resolved EVERY slot to peak.
        for sfx in GSP_SUFFIXES:
            r = self.parse(node(*smb_buckets(sfx, ev_off=4.493), kwh=1.0))
            self.assertIs(r["off_peak"], True, f"region {sfx}")

    def test_smb_peak_slot_is_peak_in_every_region(self):
        for sfx in GSP_SUFFIXES:
            r = self.parse(node(*smb_buckets(sfx, day=31.70), kwh=1.0))
            self.assertIs(r["off_peak"], False, f"region {sfx}")

    def test_smb_home_night_is_off_peak(self):
        r = self.parse(node(*smb_buckets("H", night=4.493), kwh=1.0))
        self.assertIs(r["off_peak"], True)

    def test_smb_blended_slot_is_ambiguous_not_peak(self):
        # House on peak + EV dispatched off-peak in the same half-hour. Neither band
        # owns the slot; None lets the caller fall back to billed cost / time of day.
        r = self.parse(node(*smb_buckets("H", day=20.0, ev_off=4.493), kwh=2.0))
        self.assertIsNone(r["off_peak"])

    def test_per_bucket_value_wins_over_cost(self):
        # The settlement read asks for `value` (kWh). A zero-cost bucket that still
        # carries kWh (a within-cap freebee) must set the band.
        r = self.parse(node(*smb_buckets("H", ev_off=0.0, values=True)[0:3],
                            stat("CONSUMPTION_CHARGE_EV_DEVICE_OFF_PEAK_H", 0.0, value=1.5),
                            kwh=1.5))
        self.assertIs(r["off_peak"], True)

    def test_legacy_iog_unchanged(self):
        self.assertIs(self.parse(node(stat("OFF_PEAK", 5.493,
                                           type_="TOU_BUCKET_COST")))["off_peak"], True)
        self.assertIs(self.parse(node(stat("STANDARD_RATE", 32.31,
                                           type_="TOU_BUCKET_COST")))["off_peak"], False)

    def test_export_standard_rate_stays_false_not_none(self):
        # _billed_rate's published-rate branch distinguishes False from None; export
        # slots arrive labelled STANDARD_RATE and must not become "unlabelled".
        r = self.parse(node(stat("STANDARD_RATE", 15.0, type_="TOU_BUCKET_COST")))
        self.assertIs(r["off_peak"], False)

    def test_no_statistics_is_unlabelled(self):
        r = self.parse(node(kwh=1.2))
        self.assertIsNone(r["off_peak"])
        self.assertIsNone(r["cost_incl"])
        self.assertEqual(r["buckets"], [])

    def test_standing_charge_never_sets_a_band(self):
        # STANDING_CHARGE_STANDARD_x contains "STANDARD" — it must be excluded before
        # classification, or it would stamp every slot peak.
        r = self.parse(node(stat("STANDING_CHARGE_STANDARD_H", 0.92,
                                 type_="STANDING_CHARGE_COST"),
                            *smb_buckets("H", ev_off=4.493), kwh=1.0))
        self.assertIs(r["off_peak"], True)
        self.assertAlmostEqual(r["standing_incl"], 0.0092, places=6)


if __name__ == "__main__":
    unittest.main()
