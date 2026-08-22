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
