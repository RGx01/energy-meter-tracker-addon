"""Tests for engine.probe_recorder_statistics (read-only historical-import spike)."""
import os
import sys
import types
import asyncio
import unittest
from datetime import datetime, timezone, timedelta
from unittest.mock import MagicMock

# ── Stubs so engine imports without HA/filesystem (mirrors test_ohme_capture) ─
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
_boot = BlockStore(":memory:")
_boot.insert_config_period({"meters": {"electricity_main": {"meta": {
    "timezone": "UTC", "billing_day": 1, "block_minutes": 30,
    "currency_symbol": "£", "currency_code": "GBP"}}}})
bs = types.ModuleType("block_store")
bs.BlockStore = BlockStore
bs.open_block_store = lambda path: _boot
bs.migrate_json_to_sqlite = migrate_json_to_sqlite
sys.modules["block_store"] = bs

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import engine


class _FakeHA:
    """Minimal async stand-in exposing the two statistics methods."""
    def __init__(self, meta, stats):
        self._meta, self._stats = meta, stats

    async def get_statistic_ids(self, statistic_type=None):
        return self._meta

    async def get_statistics(self, ids, start, end, period="hour", types=None):
        return {k: v for k, v in self._stats.items() if k in ids}


class TestProbeOrchestrator(unittest.TestCase):
    def setUp(self):
        self._orig = engine._engine_ha

    def tearDown(self):
        engine._engine_ha = self._orig

    def _run(self, *a, **kw):
        return asyncio.run(engine.probe_recorder_statistics(*a, **kw))

    def test_no_ha(self):
        engine._engine_ha = None
        r = self._run(["sensor.x"])
        self.assertFalse(r["ok"])
        self.assertEqual(r["reason"], "no_ha")

    def test_no_ids(self):
        engine._engine_ha = _FakeHA([], {})
        r = self._run([])
        self.assertTrue(r["ok"])
        self.assertEqual(r["reports"], [])

    def test_reports_built_and_metadata_joined(self):
        t0 = datetime(2026, 1, 1, tzinfo=timezone.utc)
        rows = [{"start": (t0 + timedelta(hours=i)).timestamp() * 1000.0,
                 "sum": float(i)} for i in range(24)]
        meta = [{"statistic_id": "sensor.ev", "unit_of_measurement": "kWh",
                 "has_sum": True, "has_mean": False}]
        engine._engine_ha = _FakeHA(meta, {"sensor.ev": rows})
        r = self._run(["sensor.ev"], days=30)
        self.assertTrue(r["ok"])
        self.assertEqual(len(r["reports"]), 1)
        rep = r["reports"][0]
        self.assertEqual(rep["statistic_id"], "sensor.ev")
        self.assertEqual(rep["value_kind"], "energy_sum")   # metadata joined
        self.assertEqual(rep["unit"], "kWh")
        self.assertEqual(rep["count"], 24)

    def test_missing_sensor_reports_not_found(self):
        engine._engine_ha = _FakeHA([], {})
        r = self._run(["sensor.nope"])
        self.assertTrue(r["ok"])
        self.assertFalse(r["reports"][0]["found"])

    def test_stats_failure_is_caught(self):
        class _Boom(_FakeHA):
            async def get_statistics(self, *a, **kw):
                raise RuntimeError("ws error")
        engine._engine_ha = _Boom([], {})
        r = self._run(["sensor.x"])
        self.assertFalse(r["ok"])
        self.assertEqual(r["reason"], "stats_failed")


if __name__ == "__main__":
    unittest.main()