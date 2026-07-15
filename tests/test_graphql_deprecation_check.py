"""
Tests for .build/check_graphql_deprecations.py (the CI deprecation checker).

Covers the two pure pieces — extracting EMT's field sets from source, and
detecting which deprecated schema members EMT depends on — without any network
call. The live fetch is exercised only in CI.
"""
import importlib.util
import os
import unittest

_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.dirname(_HERE)
_SCRIPT = os.path.join(_ROOT, ".build", "check_graphql_deprecations.py")
_SOURCE = os.path.join(_ROOT, "kraken_api_client.py")

_spec = importlib.util.spec_from_file_location("dep_check", _SCRIPT)
dep_check = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(dep_check)


def _fld(name, dep=False, reason=None):
    return {"name": name, "isDeprecated": dep, "deprecationReason": reason}


# A synthetic schema exercising every branch. Removal dates chosen distinct.
FIXTURE = {"__schema": {"types": [
    {"name": "Query", "kind": "OBJECT", "enumValues": None, "fields": [
        # deprecated but EMT does NOT use it → excluded
        _fld("electricVehicles", True,
             "Use 'flexSupportedDevices'.\n- Scheduled for removal on or after 2026-10-15."),
        # deprecated AND EMT uses it → flagged
        _fld("smartMeterTelemetry", True,
             "Going away.\n- Scheduled for removal on or after 2027-01-01."),
        # EMT uses it, not deprecated → not flagged
        _fld("devices", False, None),
    ]},
    # deprecated field whose (type, name) is in _DEPRECATION_IGNORE → excluded
    {"name": "DeviceStatusType", "kind": "OBJECT", "enumValues": None, "fields": [
        _fld("status", True,
             "noise.\n- Scheduled for removal on or after 2026-09-01."),
    ]},
    # generic name `id`, matched only via _EMT_GRAPHQL_TYPED_FIELDS → flagged
    {"name": "SmartFlexDevice", "kind": "OBJECT", "enumValues": None, "fields": [
        _fld("id", True,
             "id gone.\n- Scheduled for removal on or after 2026-12-01."),
    ]},
    # deprecated enum value EMT compares literally → flagged
    {"name": "KrakenFlexDeviceTypes", "kind": "ENUM", "fields": None, "enumValues": [
        _fld("ELECTRIC_VEHICLES", True,
             "renamed.\n- Scheduled for removal on or after 2026-08-01."),
        _fld("CHARGE_POINTS", False, None),
    ]},
]}}


class TestExtractUsedFields(unittest.TestCase):
    def setUp(self):
        (self.names, self.typed, self.enums,
         self.ignore) = dep_check.extract_used_fields(_SOURCE)

    def test_bare_field_names(self):
        self.assertIn("smartMeterTelemetry", self.names)
        self.assertIn("flexPlannedDispatches", self.names)
        self.assertIn("devices", self.names)

    def test_typed_fields(self):
        self.assertIn(("SmartFlexDevice", "id"), self.typed)

    def test_enums_include_union(self):
        # _EMT_GRAPHQL_ENUMS = _CHARGING_DEVICE_TYPES | {"TEN_SECONDS", "LIVE"}
        self.assertIn("ELECTRIC_VEHICLES", self.enums)   # from the union member
        self.assertIn("LIVE", self.enums)

    def test_ignore_set(self):
        self.assertIn(("DeviceStatusType", "status"), self.ignore)


class TestFindDeprecations(unittest.TestCase):
    def setUp(self):
        (names, typed, enums, ignore) = dep_check.extract_used_fields(_SOURCE)
        self.hits = dep_check.find_deprecations(FIXTURE, names, typed, enums, ignore)
        self.by_key = {(h["type"], h["field"]): h for h in self.hits}

    def test_only_emt_used_deprecations_flagged(self):
        got = set(self.by_key)
        self.assertEqual(got, {
            ("Query", "smartMeterTelemetry"),
            ("SmartFlexDevice", "id"),
            ("KrakenFlexDeviceTypes", "ELECTRIC_VEHICLES"),
        })

    def test_unused_deprecation_excluded(self):
        self.assertNotIn(("Query", "electricVehicles"), self.by_key)

    def test_ignore_set_excluded(self):
        self.assertNotIn(("DeviceStatusType", "status"), self.by_key)

    def test_non_deprecated_not_flagged(self):
        self.assertNotIn(("Query", "devices"), self.by_key)

    def test_removal_date_parsed(self):
        self.assertEqual(self.by_key[("Query", "smartMeterTelemetry")]["removal"],
                         "2027-01-01")

    def test_enum_kind_marked(self):
        self.assertEqual(
            self.by_key[("KrakenFlexDeviceTypes", "ELECTRIC_VEHICLES")]["kind"], "enum")

    def test_sorted_by_removal_date(self):
        removals = [h["removal"] for h in self.hits]
        self.assertEqual(removals, sorted(removals))


if __name__ == "__main__":
    unittest.main()