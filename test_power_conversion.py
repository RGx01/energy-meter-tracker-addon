"""
Tests for engine._power_value_to_kw — the single converter every power-sensor
path now routes through (main power_history + live gauge, sub-meter inverter/
device history + gauges).

Covers the two bugs behind the user report:
  • magnitude — a sensor declaring unit 'kW' but emitting W-scale numbers
    (1400 for 1.4 kW); a unit override forces the real unit.
  • direction — a sensor wired import-negative; invert negates the result.
Plus the backward-compatible 2-arg behaviour the existing callers rely on.
"""
import sys
import os
import types
import unittest
from unittest.mock import MagicMock

# ── Minimal stubs so engine.py imports without HA/filesystem ─────────────────
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

K = engine._power_value_to_kw


class TestPowerConversion(unittest.TestCase):
    # ── declared unit drives it ──────────────────────────────────────────────
    def test_watts_divided(self):
        self.assertEqual(K(1400, "W"), 1.4)

    def test_kw_as_is(self):
        self.assertEqual(K(1.4, "kW"), 1.4)

    def test_unit_case_insensitive(self):
        self.assertEqual(K(1400, "w"), 1.4)
        self.assertEqual(K(1.4, "KW"), 1.4)

    # ── missing unit → magnitude heuristic ───────────────────────────────────
    def test_missing_unit_large_is_watts(self):
        self.assertEqual(K(1400, ""), 1.4)
        self.assertEqual(K(1400, None), 1.4)

    def test_missing_unit_small_is_kw(self):
        self.assertEqual(K(1.4, ""), 1.4)

    # ── the user's bug: declared 'kW' but W-scale value ──────────────────────
    def test_mislabelled_kw_needs_override(self):
        # Sensor says kW, emits 1400 → without override it stays 1400 (the bug).
        self.assertEqual(K(1400, "kW"), 1400.0)
        # Override forces W → corrected to 1.4 kW.
        self.assertEqual(K(1400, "kW", "W"), 1.4)

    def test_override_kw_on_w_labelled(self):
        # Inverse: sensor says W but actually emits kW-scale; force kW.
        self.assertEqual(K(1.4, "W", "kW"), 1.4)

    def test_override_case_insensitive(self):
        self.assertEqual(K(1400, "kW", "w"), 1.4)
        self.assertEqual(K(1400, "kW", " W "), 1.4)

    def test_blank_override_falls_back_to_unit(self):
        self.assertEqual(K(1400, "W", ""), 1.4)
        self.assertEqual(K(1400, "W", None), 1.4)

    def test_garbage_override_ignored(self):
        # Unknown override string must not be trusted — fall back to the unit.
        self.assertEqual(K(1400, "W", "potato"), 1.4)

    # ── direction ────────────────────────────────────────────────────────────
    def test_invert_negates(self):
        self.assertEqual(K(1400, "W", None, True), -1.4)
        self.assertEqual(K(1.4, "kW", None, True), -1.4)

    def test_invert_combines_with_override(self):
        # The reporting user's exact case: declared kW, W-scale, import-negative.
        # Override → W (1.4), invert → -1.4 → splits as export-side cleanly.
        self.assertEqual(K(1400, "kW", "W", True), -1.4)

    def test_invert_false_default(self):
        self.assertEqual(K(1400, "W", None, False), 1.4)

    # ── robustness ───────────────────────────────────────────────────────────
    def test_non_numeric_returns_none(self):
        self.assertIsNone(K("unavailable", "W"))
        self.assertIsNone(K(None, "kW"))

    def test_negative_input_preserved(self):
        self.assertEqual(K(-1400, "W"), -1.4)

    def test_two_arg_backward_compatible(self):
        # Existing callers pass only (value, unit) — must be unchanged.
        self.assertEqual(K(1400, "W"), 1.4)
        self.assertEqual(K(1.4, "kW"), 1.4)
        self.assertEqual(K(2500, None), 2.5)


if __name__ == "__main__":
    unittest.main()