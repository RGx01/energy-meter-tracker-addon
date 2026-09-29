"""Tests for .build/check_graphql_field_watch.py — the schema-additions watcher for the IOG
reconstruction-relevant GraphQL surface. Pure diff/scoping logic; no network."""
import importlib.util, os, unittest

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_SCRIPT = os.path.join(_ROOT, ".build", "check_graphql_field_watch.py")
_spec = importlib.util.spec_from_file_location("fieldwatch", _SCRIPT)
fw = importlib.util.module_from_spec(_spec); _spec.loader.exec_module(fw)


def _t(name, fields):
    return {"name": name, "kind": "OBJECT", "fields": [{"name": f} for f in fields]}

SCHEMA = {"types": [
    _t("UpsideDispatchType", ["startDt", "endDt", "delta", "meta"]),        # watched (dispatch)
    _t("UpsideDispatchMetaType", ["location", "source"]),                    # watched (dispatch)
    _t("ConsumptionMeasurementType", ["value", "startAt", "metaData"]),      # watched (measurement/consumption)
    _t("AccountType", ["number", "balance"]),                                # NOT watched
    _t("__Directive", ["name"]),                                             # introspection meta — skipped
]}


class TestFieldWatch(unittest.TestCase):
    def test_watched_fields_scopes_to_reconstruction_surface(self):
        w = fw.watched_fields(SCHEMA)
        self.assertIn("UpsideDispatchType", w)
        self.assertIn("UpsideDispatchMetaType", w)
        self.assertIn("ConsumptionMeasurementType", w)
        self.assertNotIn("AccountType", w)            # not reconstruction-relevant
        self.assertNotIn("__Directive", w)            # introspection meta excluded
        self.assertEqual(w["UpsideDispatchMetaType"], ["location", "source"])

    def test_diff_flags_added_and_removed(self):
        baseline = fw.watched_fields(SCHEMA)
        # simulate Octopus adding a per-slot EV field + a dispatch 'type', and removing one
        changed = {"types": [
            _t("UpsideDispatchType", ["startDt", "endDt", "delta", "meta", "type"]),   # +type
            _t("UpsideDispatchMetaType", ["location", "source", "chargePointId"]),      # +chargePointId
            _t("ConsumptionMeasurementType", ["value", "startAt"]),                     # -metaData
        ]}
        report = fw.diff_watch(fw.watched_fields(changed), baseline)
        self.assertEqual(report["UpsideDispatchType"]["added"], ["type"])
        self.assertEqual(report["UpsideDispatchMetaType"]["added"], ["chargePointId"])
        self.assertEqual(report["ConsumptionMeasurementType"]["removed"], ["metaData"])

    def test_no_change_is_empty(self):
        base = fw.watched_fields(SCHEMA)
        self.assertEqual(fw.diff_watch(fw.watched_fields(SCHEMA), base), {})

    def test_new_watched_type_shows_all_fields_added(self):
        base = {}
        cur = fw.watched_fields(SCHEMA)
        report = fw.diff_watch(cur, base)
        # every watched type is entirely "added" against an empty baseline
        self.assertEqual(set(report), set(cur))
        self.assertEqual(report["UpsideDispatchMetaType"]["added"], ["location", "source"])


if __name__ == "__main__":
    unittest.main()


class TestEnumMembershipWatch(unittest.TestCase):
    """#481: an added ENUM VALUE must be reported, not just an added field.

    On 2026-09-28 Octopus added EV_BOOST to AllBandSubCategories in the same release that
    relabelled the device-breakdown buckets. The enum addition was the only machine-readable
    warning available; the watcher introspected `fields` only and no band/rate-type name matched
    WATCH_PATTERNS, so it saw nothing.
    """

    def _mod(self):
        import importlib.util
        spec = importlib.util.spec_from_file_location("fw", _SCRIPT)
        m = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(m)
        return m

    def test_band_and_ratetype_enums_are_watched(self):
        m = self._mod()
        for t in ("AllBandSubCategories",
                  "NonBespokeElectricityRateTypeChoices",
                  "BespokeNonHalfHourlyElectricityUnitRateRateType"):
            self.assertTrue(m._matches(t), f"{t} must be in scope — it carries the band vocabulary")

    def test_bare_rate_types_are_not_watched(self):
        """`rate` alone would match most of the tariff surface and drown the diff.

        GUARD, not a discriminator — this passed before the change too. It exists so a later
        widening of WATCH_PATTERNS to bare "rate" is a deliberate decision, not a slip."""
        m = self._mod()
        self.assertFalse(m._matches("StandardUnitRate"))
        self.assertFalse(m._matches("DayNightRate"))

    def test_enum_values_are_collected_and_namespaced(self):
        m = self._mod()
        schema = {"types": [
            {"name": "AllBandSubCategories", "kind": "ENUM", "fields": None,
             "enumValues": [{"name": "EV_OFF_PEAK"}, {"name": "EV_BOOST"}]},
            {"name": "ConsumptionStatistic", "kind": "OBJECT",
             "fields": [{"name": "label"}, {"name": "value"}], "enumValues": None},
        ]}
        cur = m.watched_fields(schema)
        self.assertEqual(cur["AllBandSubCategories"], ["enum:EV_BOOST", "enum:EV_OFF_PEAK"])
        self.assertEqual(cur["ConsumptionStatistic"], ["label", "value"])

    def test_added_enum_value_is_reported(self):
        """GUARD, not a discriminator — diff_watch is generic over member-name strings, so it
        already handled this shape. Pins the end-to-end report a seeded baseline would produce."""
        m = self._mod()
        baseline = {"AllBandSubCategories": ["enum:EV_OFF_PEAK", "enum:EV_PEAK"]}
        current = {"AllBandSubCategories": ["enum:EV_BOOST", "enum:EV_OFF_PEAK", "enum:EV_PEAK"]}
        rep = m.diff_watch(current, baseline)
        self.assertIn("AllBandSubCategories", rep)
        self.assertEqual(rep["AllBandSubCategories"]["added"], ["enum:EV_BOOST"])
        self.assertEqual(rep["AllBandSubCategories"]["removed"], [])
