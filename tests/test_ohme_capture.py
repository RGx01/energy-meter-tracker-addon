"""Issue #286 — OHME (official HA integration) IOG off-peak capture.

The official `ohme` select reports the underscore SLUG as its state
('smart_charge' / 'max_charge' / 'paused'); only the DISPLAY is "Smart charge".
EMT matched the space form ("smart charge") so every tick resolved to `idle`,
captured nothing, and the whole charge was billed at peak. These tests pin the
slug matching and the Status-sensor gate so that regression can't recur.
"""
import os
import sys
import types
import unittest
from datetime import datetime
from unittest.mock import MagicMock

# ── Stubs so engine.py imports without HA/filesystem (mirrors the other tests) ─
eio = types.ModuleType("energy_engine_io")
eio.ensure_dir = lambda *a, **kw: None
eio.load_json = lambda *a, **kw: a[1] if len(a) > 1 else {}
eio.save_json_atomic = lambda *a, **kw: None
eio.save_file = lambda *a, **kw: None
sys.modules["energy_engine_io"] = eio

ec = types.ModuleType("energy_charts")
ec.generate_net_heatmap = lambda *a, **kw: ""
ec.generate_daily_import_export_charts = lambda *a, **kw: ""
sys.modules["energy_charts"] = ec

hc = types.ModuleType("ha_client")
hc.HAClient = MagicMock
sys.modules["ha_client"] = hc

from block_store import BlockStore, migrate_json_to_sqlite
_boot_store = BlockStore(":memory:")
_boot_store.insert_config_period({"meters": {"electricity_main": {"meta": {
    "timezone": "UTC", "billing_day": 1, "block_minutes": 30,
    "currency_symbol": "£", "currency_code": "GBP"}}}})
bs = types.ModuleType("block_store")
bs.BlockStore = BlockStore
bs.open_block_store = lambda path: _boot_store
bs.migrate_json_to_sqlite = migrate_json_to_sqlite
sys.modules["block_store"] = bs

sys.path.insert(0, os.path.dirname(__file__))
import engine
from kraken_api_client import detect_ohme_charge_mode, _norm_ohme_state


class TestOhmeModeInterpretation(unittest.TestCase):
    """The official select's raw state is the underscore slug (#286)."""

    def test_slug_forms_resolve(self):
        self.assertEqual(engine._ohme_interpret_mode("official", "smart_charge"), "smart")
        self.assertEqual(engine._ohme_interpret_mode("official", "max_charge"), "boost")
        self.assertEqual(engine._ohme_interpret_mode("official", "paused"), "idle")

    def test_display_forms_also_resolve(self):
        # Belt and braces: if any source ever hands us the display form.
        self.assertEqual(engine._ohme_interpret_mode("official", "Smart charge"), "smart")
        self.assertEqual(engine._ohme_interpret_mode("official", "Max charge"), "boost")

    def test_286_regression_smart_charge_is_not_idle(self):
        # The exact bug: 'smart_charge' used to fall through to idle.
        self.assertNotEqual(engine._ohme_interpret_mode("official", "smart_charge"), "idle")

    def test_danr_binary(self):
        self.assertEqual(engine._ohme_interpret_mode("danr", "on"), "smart")
        self.assertEqual(engine._ohme_interpret_mode("danr", "off"), "idle")


class TestOhmeStatusCharging(unittest.TestCase):
    def test_only_charging_is_active(self):
        self.assertTrue(engine._ohme_status_charging("charging"))
        for s in ("paused", "plugged_in", "finished", "unplugged", "", None):
            self.assertFalse(engine._ohme_status_charging(s))


class TestOhmeCaptureSlots(unittest.TestCase):
    NOW = datetime(2026, 7, 11, 14, 20, 0)   # slot 14:00

    def _slot(self):
        return engine._ohme_slot_for_now(self.NOW)

    def test_non_ohme_returns_none(self):
        self.assertIsNone(engine._ohme_capture_slots(
            "MYENERGI_V2", [], True, "smart", "charging", self.NOW))

    def test_verified_smart_and_charging_captures_current_slot(self):
        out = engine._ohme_capture_slots("OHME", [], True, "smart", "charging", self.NOW)
        self.assertEqual(out, [(self._slot(), "ohme_verified")])

    def test_verified_smart_but_not_charging_captures_nothing(self):
        # Plugged in on Smart charge but paused this tick → no draw → skip.
        self.assertEqual(
            engine._ohme_capture_slots("OHME", [], True, "smart", "paused", self.NOW), [])

    def test_verified_smart_no_status_sensor_falls_back_to_mode(self):
        # No Status sensor (status=None) → mode-only capture (old behaviour, but
        # now with the mode string fixed).
        out = engine._ohme_capture_slots("OHME", [], True, "smart", None, self.NOW)
        self.assertEqual(out, [(self._slot(), "ohme_verified")])

    def test_boost_is_vetoed_even_while_charging(self):
        # Max charge is a manual boost, not off-peak — never captured.
        self.assertEqual(
            engine._ohme_capture_slots("OHME", [], True, "boost", "charging", self.NOW), [])

    def test_idle_captures_nothing(self):
        self.assertEqual(
            engine._ohme_capture_slots("OHME", [], True, "idle", "charging", self.NOW), [])

    def test_optimistic_path_when_no_charge_mode_sensor(self):
        # sensor_present=False → optimistic branch (returns a list, not None).
        out = engine._ohme_capture_slots("OHME", [], False, None, None, self.NOW)
        self.assertIsInstance(out, list)


class TestOhmeDetection(unittest.TestCase):
    def test_official_select_slug_detected_and_status_found(self):
        d = detect_ohme_charge_mode([
            {"entity_id": "select.ohme_home_pro_charge_mode", "state": "smart_charge"},
            {"entity_id": "sensor.ohme_home_pro_status", "state": "charging"},
        ])
        self.assertTrue(d["found"])
        self.assertEqual(d["integration"], "official")
        self.assertEqual(d["charge_mode_entity"], "select.ohme_home_pro_charge_mode")
        self.assertEqual(d["status_entity"], "sensor.ohme_home_pro_status")
        self.assertIs(d["is_boost"], False)          # smart_charge → not boost

    def test_max_charge_slug_flags_boost(self):
        d = detect_ohme_charge_mode(
            [{"entity_id": "select.ohme_x_charge_mode", "state": "max_charge"}])
        self.assertIs(d["is_boost"], True)

    def test_normaliser(self):
        self.assertEqual(_norm_ohme_state("Smart charge"), "smart_charge")
        self.assertEqual(_norm_ohme_state("smart_charge"), "smart_charge")


class TestOhmeDispatchHistory(unittest.TestCase):
    """BL-10 — OHME dispatches are accumulated into dispatch_history so the
    smart-charging card (which reads it and colours by billed rate) covers OHME."""

    def setUp(self):
        self.store = BlockStore(":memory:")
        self._orig = engine._store
        engine._store = self.store

    def tearDown(self):
        engine._store = self._orig

    def _rows(self):
        return self.store.get_dispatch_history(
            "2026-07-18T00:00:00", "2026-07-20T00:00:00")

    def test_records_completed_and_planned_even_for_non_smart_source(self):
        # OHME source labels are unreliable — a 'bump-charge' planned slot and a
        # completed dispatch must BOTH land in history (include_all path).
        planned = [{"source": "bump-charge", "start": "2026-07-18T23:30:00Z",
                    "end": "2026-07-19T00:00:00Z", "delta": -3.4}]
        completed = [{"source": "unknown", "start": "2026-07-18T20:00:00Z",
                      "end": "2026-07-18T20:30:00Z", "delta": -3.1}]
        n = engine._record_ohme_dispatch_history("OHME", planned, completed)
        self.assertGreaterEqual(n, 2)
        by_kind = {}
        for r in self._rows():
            by_kind.setdefault(r["kind"], []).append(r)
        self.assertIn("planned", by_kind)     # kept despite non-smart source
        self.assertIn("completed", by_kind)
        self.assertEqual(by_kind["planned"][0]["slot_start"], "2026-07-18T23:30:00")
        self.assertEqual(by_kind["completed"][0]["slot_start"], "2026-07-18T20:00:00")

    def test_no_store_is_safe(self):
        engine._store = None
        self.assertEqual(engine._record_ohme_dispatch_history("OHME", [], []), 0)


class TestPlannedSlotIncludeAll(unittest.TestCase):
    """include_all bypasses the smart-charge source filter (OHME history)."""

    P = [{"source": "bump-charge", "start": "2026-07-18T23:30:00Z",
          "end": "2026-07-19T00:00:00Z", "delta": -3.4}]

    def test_default_filters_non_smart_source(self):
        self.assertEqual(engine._planned_dispatch_slot_energy(self.P), {})

    def test_include_all_keeps_non_smart_source(self):
        out = engine._planned_dispatch_slot_energy(self.P, include_all=True)
        self.assertIn("2026-07-18T23:30:00", out)
        b = engine._planned_dispatch_slot_bounds(self.P, include_all=True)
        self.assertIn("2026-07-18T23:30:00", b)


if __name__ == "__main__":
    unittest.main()