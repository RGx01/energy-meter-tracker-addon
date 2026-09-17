"""Regression: a fresh API-only install can import its whole history.

The backward API import tiles UP TO `go_live` — EMT's oldest existing block — so it
never re-fetches a range already present (the contiguity rule). On a fresh install
with no sensors there are NO blocks, so `go_live` was None and both the plan and the
apply refused with `reason: "no_go_live"`. The Historical Import panel rendered that
raw enum in red and Whole history was impossible, while Date range and Gap fill (which
tolerate a missing ceiling via `bounded`) both worked.

An empty store has neither thing the ceiling protects: no live capture to stop short
of, and no existing range to avoid re-fetching. So the ceiling becomes the current
half-hour boundary instead.
"""
import os
import sys
import types
import asyncio
import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

# ── Stubs so engine imports without HA/filesystem ────────────────────────────
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

from block_store import BlockStore

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import engine

CFG = {"meters": {"electricity_main": {"meta": {
    "timezone": "UTC", "billing_day": 1, "block_minutes": 30,
    "currency_symbol": "£", "currency_code": "GBP", "sub_meter": False}}}}


def _fresh_store():
    st = BlockStore(":memory:")
    st.insert_config_period(CFG)
    return st


class TestImportCeiling(unittest.TestCase):
    def setUp(self):
        self._s, self._d, self._c = (engine._store, engine._kraken_discovery,
                                     engine._kraken_client)
        engine._store = _fresh_store()

    def tearDown(self):
        (engine._store, engine._kraken_discovery,
         engine._kraken_client) = self._s, self._d, self._c

    def test_empty_store_ceiling_is_now_not_a_refusal(self):
        iso, src = engine._import_ceiling()
        self.assertEqual(src, "now", "an empty store must not refuse")
        self.assertIsNotNone(iso)

    def test_empty_store_ceiling_snaps_to_the_half_hour(self):
        iso, _ = engine._import_ceiling()
        dt = datetime.fromisoformat(iso)
        self.assertIn(dt.minute, (0, 30),
                      "must not import the partial in-progress slot")
        self.assertEqual((dt.second, dt.microsecond), (0, 0))

    def test_empty_store_ceiling_is_not_in_the_future(self):
        iso, _ = engine._import_ceiling()
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        self.assertLessEqual(datetime.fromisoformat(iso), now)

    def test_existing_blocks_still_win(self):
        engine._store._conn.execute(
            "INSERT INTO blocks (block_start, block_end, meter_id, config_period_id, "
            "imp_kwh, imp_rate, imp_cost) VALUES (?,?,?,?,?,?,?)",
            ("2026-05-01T00:00:00", "2026-05-01T00:30:00", "electricity_main", 1,
             1.0, 0.25, 0.25))
        engine._store._conn.commit()
        iso, src = engine._import_ceiling()
        self.assertEqual((iso, src), ("2026-05-01T00:00:00", "blocks"),
                         "the contiguity ceiling must be unchanged where it applies")


class TestPlanOnAFreshInstall(unittest.TestCase):
    def setUp(self):
        self._s, self._d, self._c = (engine._store, engine._kraken_discovery,
                                     engine._kraken_client)
        engine._store = _fresh_store()
        engine._kraken_client = object()
        engine._kraken_discovery = {"import": {"mpan": "IM", "serial": "IMS"}}

    def tearDown(self):
        (engine._store, engine._kraken_discovery,
         engine._kraken_client) = self._s, self._d, self._c

    def _plan(self, **kw):
        return asyncio.run(engine.plan_api_import(**kw))

    def test_whole_history_plans_instead_of_refusing(self):
        frm = (datetime.now(timezone.utc).replace(tzinfo=None)
               - timedelta(days=30)).isoformat()
        r = self._plan(requested_from=frm)
        self.assertTrue(r["ok"], f"fresh install must be able to plan: {r}")
        self.assertEqual(r.get("ceiling_source"), "now")
        self.assertNotEqual(r.get("reason"), "no_go_live")

    def test_plan_produces_a_real_window_and_chunks(self):
        frm = (datetime.now(timezone.utc).replace(tzinfo=None)
               - timedelta(days=30)).isoformat()
        ch = self._plan(requested_from=frm)["channels"]["import"]
        self.assertTrue(ch["ok"], ch)
        self.assertGreaterEqual(ch["chunk_count"], 1)
        self.assertLess(ch["window"]["from"], ch["window"]["to"])

    def test_no_api_still_refuses(self):
        engine._kraken_client = None
        engine._kraken_discovery = None
        r = self._plan()
        self.assertFalse(r["ok"])
        self.assertEqual(r["reason"], "no_api")

    def test_the_no_go_live_code_is_retired(self):
        """It leaked to the UI as a raw enum; nothing should emit it any more."""
        frm = (datetime.now(timezone.utc).replace(tzinfo=None)
               - timedelta(days=30)).isoformat()
        for r in (self._plan(), self._plan(requested_from=frm)):
            self.assertNotEqual(r.get("reason"), "no_go_live")


if __name__ == "__main__":
    unittest.main()
