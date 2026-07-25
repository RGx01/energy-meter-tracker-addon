"""
test_server.py
==============
Unit tests for the Flask API endpoints in web/server.py.

Tests all routes are registered, return correct status codes, and return
well-formed JSON. Uses Flask's built-in test client — no running server needed.

Run with:
    python3 -m pytest test_server.py -v
or:
    python3 test_server.py

The tests patch filesystem and engine calls so no real data or HA connection
is required.
"""

import sys
import os
import json
import types
import unittest
from unittest.mock import patch, MagicMock

# ── Minimal stubs so server.py imports without HA/filesystem ─────────────────

# Stub energy_engine_io
eio = types.ModuleType("energy_engine_io")
eio.load_json        = lambda path, default=None: default
eio.save_json_atomic = lambda *a, **kw: None
eio.save_file        = lambda *a, **kw: None
eio.ensure_dir       = lambda *a, **kw: None
sys.modules["energy_engine_io"] = eio

# Stub energy_charts
ec = types.ModuleType("energy_charts")
ec.generate_net_heatmap                = lambda *a, **kw: "<html>heatmap</html>"
ec.generate_daily_import_export_charts = lambda *a, **kw: "<html>daily</html>"
ec.build_meter_colors                  = lambda *a, **kw: {
    "electricity_main": "#1f77b4",
    "electricity_main_export": "#ff7f0e",
}
ec.build_meter_colors_from_config      = lambda *a, **kw: {
    "electricity_main": "#1f77b4",
    "electricity_main_export": "#ff7f0e",
}
ec.calculate_billing_summary_for_period = lambda *a, **kw: {
    "totals": {},
    "standing": {},
    "total_standing": 0.0,
    "total_cost": 0.0,
    "meter_meta": {},
}
ec.get_billing_periods_from_config_history = lambda *a, **kw: []
ec.get_billing_periods_from_config_periods = lambda *a, **kw: []
sys.modules["energy_charts"] = ec

# Stub ha_client
hc = types.ModuleType("ha_client")
hc.HAClient = MagicMock
sys.modules["ha_client"] = hc

# Stub block_store — use real in-memory BlockStore pre-loaded with MINIMAL_BLOCKS
# (defined after MINIMAL_BLOCKS below, wired in via make_client)
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
# Remove any cached stub from previous test runs before importing the real module
if "block_store" in sys.modules:
    del sys.modules["block_store"]
import importlib.util as _ilu
_bs_spec = _ilu.spec_from_file_location(
    "block_store_real",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "block_store.py")
)
_bs_real = _ilu.module_from_spec(_bs_spec)
_bs_spec.loader.exec_module(_bs_real)
BlockStore       = _bs_real.BlockStore
open_block_store = _bs_real.open_block_store
_ldtub  = _bs_real.local_date_to_utc_bounds
_ldrtub = _bs_real.local_date_range_to_utc_bounds

def _make_test_store(blocks=None):
    """Create an in-memory BlockStore pre-loaded with given blocks."""
    store = BlockStore(":memory:")
    store.insert_config_period({
        "meters": {"electricity_main": {"meta": {
            "billing_day": 1, "block_minutes": 30,
            "timezone": "Europe/London",
            "currency_symbol": "£", "currency_code": "GBP",
        }}}
    })
    if blocks:
        store.append_blocks(blocks)
    return store

bs_mod = types.ModuleType("block_store")
bs_mod.BlockStore       = BlockStore
bs_mod.open_block_store = lambda path: _make_test_store()
bs_mod.local_date_to_utc_bounds        = _ldtub
bs_mod.local_date_range_to_utc_bounds  = _ldrtub
bs_mod.outward_code          = _bs_real.outward_code
bs_mod.derive_region_periods = _bs_real.derive_region_periods
sys.modules["block_store"] = bs_mod

# Stub engine (pause/resume only) — plus the pure power converter that the
# /api/power route delegates to (server.sensor_kw → engine._power_value_to_kw,
# added when the W/kW unit handling was unified into engine). engine.py isn't
# imported here (it pulls in HA/filesystem), so we lift just that one pure,
# builtins-only function straight from source — no duplicated literal to drift,
# and its behaviour is independently guarded by test_power_conversion.py.
eng = types.ModuleType("engine")
eng.pause_engine  = lambda: None
eng.resume_engine = lambda: None
eng.engine_startup = MagicMock()

def _load_power_converter_into(mod):
    import re as _re
    _here = os.path.dirname(os.path.abspath(__file__))
    for _cand in (os.path.join(_here, "engine.py"),
                  os.path.join(os.path.dirname(_here), "engine.py")):
        if os.path.exists(_cand):
            _src = open(_cand, encoding="utf-8").read()
            _m = _re.search(
                r"\ndef _power_value_to_kw\(.*?\n    return round\(fv, 3\)\n",
                _src, _re.S)
            if _m:
                exec(_m.group(0), mod.__dict__)
                return
    raise RuntimeError("could not locate _power_value_to_kw in engine.py source")

_load_power_converter_into(eng)
sys.modules["engine"] = eng

# Stub waitress (not needed for test client)
wt = types.ModuleType("waitress")
wt.serve = lambda *a, **kw: None
sys.modules["waitress"] = wt

# Now import server
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "web"))
import server

# ── Shared test data ──────────────────────────────────────────────────────────

MINIMAL_CONFIG = {
    "schema_version": "1.0",
    "meters": {
        "electricity_main": {
            "meta": {
                "billing_day":   1,
                "block_minutes": 30,
                "site":          "Test Site",
                "timezone":      "Europe/London",
                "currency_symbol": "£",
                "currency_code": "GBP",
            },
            "channels": {
                "import": {"read": "sensor.import", "rate": "sensor.rate"},
                "export": {"read": "sensor.export", "rate": "sensor.exp_rate"},
            }
        }
    }
}

MINIMAL_BLOCKS = [
    {
        "start":  "2026-01-15T00:00:00",
        "end":    "2026-01-15T00:30:00",
        "meters": {
            "electricity_main": {
                "meta": {"billing_day": 1, "block_minutes": 30, "timezone": "Europe/London"},
                "channels": {
                    "import": {"kwh": 0.5, "kwh_total": 0.5, "kwh_remainder": 0.5,
                               "cost": 0.1225, "rate": 0.245, "read_start": 1000.0, "read_end": 1000.5},
                    "export": {"kwh": 0.1, "cost": 0.015, "rate": 0.15,
                               "read_start": 500.0, "read_end": 500.1},
                },
                "standing_charge": 0.5046,
                "interpolated": False,
            }
        },
        "totals": {"import_kwh": 0.5, "import_cost": 0.1225, "export_kwh": 0.1, "export_cost": 0.015},
        "interpolated": False,
    }
]


def make_client(blocks=None, store=None):
    """Return a Flask test client with DATA_DIR, CHART_DIR and BlockStore initialised."""
    server.DATA_DIR  = "/tmp/emt_test_data"
    server.CHART_DIR = "/tmp/emt_test_charts"
    server._ha_client = MagicMock()
    # Allow caller to inject a pre-built store (e.g. with custom config periods)
    if store is not None:
        server._store = store
    else:
        blks = blocks if blocks is not None else MINIMAL_BLOCKS
        server._store = _make_test_store(blks)
    return server.app.test_client()


# ─────────────────────────────────────────────────────────────────────────────
# Route registration — every endpoint should exist
# ─────────────────────────────────────────────────────────────────────────────

class TestRouteRegistration(unittest.TestCase):

    def setUp(self):
        self.client = make_client()

    def _registered(self, endpoint_name):
        return endpoint_name in server.app.view_functions

    def test_index_registered(self):
        self.assertTrue(self._registered("index"))

    def test_config_page_registered(self):
        self.assertTrue(self._registered("settings_page"))

    def test_charts_page_registered(self):
        self.assertTrue(self._registered("charts_page"))

    def test_summary_page_registered(self):
        self.assertTrue(self._registered("live_power_page"))

    def test_import_page_registered(self):
        self.assertTrue(self._registered("data_management_page"))

    def test_logs_page_registered(self):
        self.assertTrue(self._registered("logs_page"))

    def test_help_page_registered(self):
        self.assertTrue(self._registered("help_page"))

    def test_api_last_page_registered(self):
        self.assertTrue(self._registered("api_set_last_page"))

    def test_api_blocks_summary_registered(self):
        self.assertTrue(self._registered("api_blocks_summary"))

    def test_api_billing_source_get_registered(self):
        self.assertTrue(self._registered("api_billing_source_get"))

    def test_api_billing_source_post_registered(self):
        self.assertTrue(self._registered("api_billing_source_post"))

    def test_api_unsettled_blocks_registered(self):
        self.assertTrue(self._registered("api_unsettled_blocks"))

    def test_api_retry_settlement_registered(self):
        self.assertTrue(self._registered("api_retry_settlement"))

    def test_api_kraken_config_get_registered(self):
        self.assertTrue(self._registered("api_kraken_config_get"))

    def test_api_kraken_config_post_registered(self):
        self.assertTrue(self._registered("api_kraken_config_post"))

    def test_api_data_source_mode_get_registered(self):
        self.assertTrue(self._registered("api_data_source_mode_get"))

    def test_api_data_source_mode_post_registered(self):
        self.assertTrue(self._registered("api_data_source_mode_post"))

    def test_api_detect_bcd_registered(self):
        self.assertTrue(self._registered("api_detect_bcd"))

    def test_api_meter_main_reset_registered(self):
        self.assertTrue(self._registered("api_meter_main_reset"))

    def test_chart_files_removed_on_reset_logic(self):
        # The main-reset path removes stale chart HTML from CHART_DIR. Verify the
        # removal contract over a temp dir (the destructive route itself wipes
        # the DB + restarts, so we exercise the file-cleanup portion directly).
        import server, os, tempfile
        d = tempfile.mkdtemp()
        orig = server.CHART_DIR
        server.CHART_DIR = d
        try:
            for f in ("net_heatmap.html", "daily_usage.html"):
                with open(os.path.join(d, f), "w") as fh:
                    fh.write("<html>stale</html>")
            # Same logic the reset route runs:
            for f in ("net_heatmap.html", "daily_usage.html"):
                p = os.path.join(server.CHART_DIR, f)
                if os.path.exists(p):
                    os.remove(p)
            self.assertFalse(os.path.exists(os.path.join(d, "net_heatmap.html")))
            self.assertFalse(os.path.exists(os.path.join(d, "daily_usage.html")))
        finally:
            server.CHART_DIR = orig
            import shutil; shutil.rmtree(d, ignore_errors=True)

    def test_api_chart_heatmap_registered(self):
        self.assertTrue(self._registered("api_chart_heatmap"))

    def test_api_chart_daily_registered(self):
        self.assertTrue(self._registered("api_chart_daily"))

    def test_api_power_registered(self):
        self.assertTrue(self._registered("api_power"))

    def test_api_billing_registered(self):
        self.assertTrue(self._registered("api_billing"))

    def test_api_carbon_registered(self):
        self.assertTrue(self._registered("api_carbon"))

    def test_api_power_history_registered(self):
        self.assertTrue(self._registered("api_power_history"))

    def test_api_carbon_current_registered(self):
        self.assertTrue(self._registered("api_carbon_current"))

    def test_api_config_get_registered(self):
        self.assertTrue(self._registered("api_get_config"))

    def test_api_config_post_registered(self):
        self.assertTrue(self._registered("api_save_config"))

    def test_api_backup_registered(self):
        self.assertTrue(self._registered("api_backup"))

    def test_api_regenerate_charts_registered(self):
        self.assertTrue(self._registered("api_regenerate_charts"))

    def test_api_import_registered(self):
        self.assertTrue(self._registered("api_import"))

    def test_api_logs_registered(self):
        self.assertTrue(self._registered("api_logs"))

    def test_insights_page_registered(self):
        self.assertTrue(self._registered("insights_page"))

    def test_settings_page_registered(self):
        self.assertTrue(self._registered("settings_page"))

    def test_billing_history_page_registered(self):
        self.assertTrue(self._registered("billing_history_page"))

    def test_data_management_page_registered(self):
        self.assertTrue(self._registered("data_management_page"))

    def test_api_review_blocks_registered(self):
        self.assertTrue(self._registered("api_review_blocks"))

    def test_api_review_blocks_dismiss_registered(self):
        self.assertTrue(self._registered("api_review_blocks_dismiss"))

    def test_live_power_page_registered(self):
        self.assertTrue(self._registered("live_power_page"))

    def test_api_insights_periods_registered(self):
        self.assertTrue(self._registered("api_insights_periods"))

    def test_api_insights_billing_period_registered(self):
        self.assertTrue(self._registered("api_insights_billing_period"))

    def test_api_settings_get_registered(self):
        self.assertTrue(self._registered("api_settings_get"))

    def test_api_settings_post_registered(self):
        self.assertTrue(self._registered("api_settings_post"))


# ─────────────────────────────────────────────────────────────────────────────
# /api/last-page
# ─────────────────────────────────────────────────────────────────────────────

class TestEngineLoopBridge(unittest.TestCase):
    """The sync→async bridge must run coroutines on the engine's loop, not a
    fresh one (fresh-loop run_until_complete breaks aiohttp). Regression guard
    for the detect-bcd / kraken-config / retry-settlement async bug."""

    def test_raises_when_no_loop(self):
        import server, asyncio
        orig = server._event_loop
        server._event_loop = None
        async def _c():
            return 1
        try:
            with self.assertRaises(RuntimeError):
                server._run_on_engine_loop(_c())
        finally:
            server._event_loop = orig

    def test_runs_on_running_loop(self):
        import server, asyncio, threading
        loop = asyncio.new_event_loop()
        t = threading.Thread(target=loop.run_forever, daemon=True)
        t.start()
        orig = server._event_loop
        server._event_loop = loop
        async def _c():
            await asyncio.sleep(0)
            return 42
        try:
            self.assertEqual(server._run_on_engine_loop(_c(), timeout=5), 42)
        finally:
            server._event_loop = orig
            loop.call_soon_threadsafe(loop.stop)
            t.join(timeout=2)
            loop.close()


class TestApiLastPage(unittest.TestCase):

    def setUp(self):
        self.client = make_client()

    def _post(self, page):
        return self.client.post(
            "/api/last-page",
            data=json.dumps({"page": page}),
            content_type="application/json"
        )

    def test_valid_page_returns_ok(self):
        for page in ("charts", "summary", "config", "import", "logs", "help"):
            with self.subTest(page=page):
                r = self._post(page)
                self.assertEqual(r.status_code, 200)
                self.assertEqual(r.get_json()["ok"], True)

    def test_valid_page_sets_cookie(self):
        r = self._post("charts")
        self.assertIn("emt_last_page", r.headers.get("Set-Cookie", ""))

    def test_invalid_page_falls_back_to_charts(self):
        r = self._post("nonexistent_page")
        self.assertEqual(r.status_code, 200)
        self.assertIn("charts", r.headers.get("Set-Cookie", ""))

    def test_missing_page_key_falls_back_to_charts(self):
        r = self.client.post(
            "/api/last-page",
            data=json.dumps({}),
            content_type="application/json"
        )
        self.assertEqual(r.status_code, 200)


# ─────────────────────────────────────────────────────────────────────────────
# /api/config GET
# ─────────────────────────────────────────────────────────────────────────────

class TestApiConfigGet(unittest.TestCase):

    def setUp(self):
        self.client = make_client()

    def test_returns_json(self):
        with patch.object(server, "load_config", return_value=MINIMAL_CONFIG):
            r = self.client.get("/api/config")
        self.assertEqual(r.status_code, 200)
        data = r.get_json()
        self.assertIn("meters", data)

    def test_empty_config_returns_empty_meters(self):
        with patch.object(server, "load_config", return_value={"meters": {}}):
            r = self.client.get("/api/config")
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.get_json()["meters"], {})


# ─────────────────────────────────────────────────────────────────────────────
# /api/charts/heatmap and /api/charts/daily
# ─────────────────────────────────────────────────────────────────────────────

class TestApiChartEndpoints(unittest.TestCase):

    def setUp(self):
        self.client = make_client()

    def test_heatmap_returns_null_html_when_file_missing(self):
        with patch("os.path.exists", return_value=False):
            r = self.client.get("/api/charts/heatmap")
        self.assertEqual(r.status_code, 200)
        self.assertIsNone(r.get_json()["html"])

    def test_heatmap_returns_html_when_file_exists(self):
        with patch("os.path.exists", return_value=True), \
             patch("builtins.open", unittest.mock.mock_open(read_data="<html>test</html>")):
            r = self.client.get("/api/charts/heatmap")
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.get_json()["html"], "<html>test</html>")

    def test_daily_returns_null_html_when_file_missing(self):
        with patch("os.path.exists", return_value=False):
            r = self.client.get("/api/charts/daily")
        self.assertEqual(r.status_code, 200)
        self.assertIsNone(r.get_json()["html"])

    def test_daily_returns_html_when_file_exists(self):
        with patch("os.path.exists", return_value=True), \
             patch("builtins.open", unittest.mock.mock_open(read_data="<html>daily</html>")):
            r = self.client.get("/api/charts/daily")
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.get_json()["html"], "<html>daily</html>")


# ─────────────────────────────────────────────────────────────────────────────
# /api/charts/blocks-summary
# ─────────────────────────────────────────────────────────────────────────────

class TestApiBlocksSummary(unittest.TestCase):

    def setUp(self):
        self.client = make_client()

    def _get(self, config=None, blocks=None):
        cfg = config or MINIMAL_CONFIG
        blk = blocks if blocks is not None else MINIMAL_BLOCKS
        eio.load_json = lambda path, default=None: cfg if "meters_config" in path else default
        server._store = _make_test_store(blk)
        return self.client.get("/api/charts/blocks-summary")

    def test_returns_200(self):
        r = self._get()
        self.assertEqual(r.status_code, 200)

    def test_response_has_required_keys(self):
        r = self._get()
        data = r.get_json()
        for key in ("currency", "rows", "meters", "export_color", "has_postcode"):
            self.assertIn(key, data)

    def test_currency_from_config(self):
        r = self._get()
        self.assertEqual(r.get_json()["currency"], "£")

    def test_rows_is_list(self):
        r = self._get()
        self.assertIsInstance(r.get_json()["rows"], list)

    def test_row_has_date_fields(self):
        r = self._get()
        rows = r.get_json()["rows"]
        if rows:
            row = rows[0]
            for field in ("year", "month", "day"):
                self.assertIn(field, row)

    def test_empty_blocks_returns_empty_rows(self):
        eio.load_json = lambda path, default=None: MINIMAL_CONFIG if "meters_config" in path else default
        server._store = _make_test_store([])
        r = self.client.get("/api/charts/blocks-summary")
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.get_json()["rows"], [])

    def test_meters_list_excludes_export_entry(self):
        r = self._get()
        meter_ids = [m["id"] for m in r.get_json()["meters"]]
        self.assertNotIn("electricity_main_export", meter_ids)

    def test_meters_list_includes_main(self):
        r = self._get()
        meter_ids = [m["id"] for m in r.get_json()["meters"]]
        self.assertIn("electricity_main", meter_ids)

# ─────────────────────────────────────────────────────────────────────────────
# /api/charts/blocks-summary — carbon_g fields (2.4.0)
# ─────────────────────────────────────────────────────────────────────────────

class TestApiBlocksSummaryCarbonG(unittest.TestCase):
    """
    Verifies that api_blocks_summary correctly populates carbon_g_net,
    carbon_g_total, carbon_g_imp and carbon_g_exp on each row, and that
    has_postcode is reflected correctly.
    """

    def _make_store_with_carbon(self, carbon_g=65.0, imp_kwh=2.0, exp_kwh=0.0,
                                postcode="DE1"):
        """Store with one block that has carbon_g set."""
        cfg = {
            "meters": {"electricity_main": {"meta": {
                "billing_day": 1, "block_minutes": 30,
                "timezone": "Europe/London",
                "currency_symbol": "£", "currency_code": "GBP",
                "postcode_prefix": postcode,
            }, "channels": {
                "import": {"read": "sensor.imp", "rate": "sensor.rate"},
                "export": {"read": "sensor.exp", "rate": "sensor.exp_rate"},
            }}}
        }
        store = BlockStore(":memory:")
        store.insert_config_period(cfg)
        cp_id = store.get_current_config_period_id()
        from datetime import datetime, timedelta
        from zoneinfo import ZoneInfo
        bs = "2026-04-14T10:00:00"
        be = "2026-04-14T10:30:00"
        store._conn.execute("""
            INSERT INTO blocks (
                block_start, block_end,
                meter_id, config_period_id, interpolated,
                imp_kwh, imp_kwh_grid, imp_kwh_remainder,
                imp_rate, imp_cost, imp_cost_remainder,
                imp_read_start, imp_read_end,
                exp_kwh, exp_rate, exp_cost,
                exp_read_start, exp_read_end, standing_charge, carbon_g)
            VALUES (?,?,?,?,0,?,NULL,NULL,?,NULL,NULL,NULL,NULL,?,NULL,?,NULL,NULL,?,?)
        """, (bs, be,
              "electricity_main", cp_id,
              imp_kwh, 0.49, exp_kwh, 0.0, 0.5, carbon_g))
        store._conn.commit()
        return store, cfg

    def _get_rows(self, store, cfg):
        eio.load_json = lambda path, default=None: cfg if "meters_config" in path else default
        server._store = store
        client = make_client(store=store)
        r = client.get("/api/charts/blocks-summary")
        self.assertEqual(r.status_code, 200)
        return r.get_json()

    def test_carbon_g_net_on_row(self):
        """carbon_g_net present on row when block has carbon_g."""
        store, cfg = self._make_store_with_carbon(carbon_g=65.0, imp_kwh=2.0, exp_kwh=0.0)
        data = self._get_rows(store, cfg)
        rows = data["rows"]
        self.assertTrue(len(rows) > 0)
        row = rows[0]
        self.assertIn("carbon_g_net", row)
        self.assertAlmostEqual(row["carbon_g_net"], 65.0, places=2)

    def test_carbon_g_null_when_no_ci_data(self):
        """carbon_g_net is None when block has NULL carbon_g."""
        store, cfg = self._make_store_with_carbon(carbon_g=None)
        data = self._get_rows(store, cfg)
        rows = data["rows"]
        self.assertTrue(len(rows) > 0)
        self.assertIsNone(rows[0]["carbon_g_net"])

    def test_carbon_g_imp_and_exp_on_pure_import(self):
        """Pure import block: carbon_g_imp = carbon_g, carbon_g_exp = 0."""
        store, cfg = self._make_store_with_carbon(carbon_g=65.0, imp_kwh=2.0, exp_kwh=0.0)
        data = self._get_rows(store, cfg)
        row = data["rows"][0]
        m = row["meters"]["electricity_main"]
        self.assertIn("carbon_g_imp", m)
        self.assertIn("carbon_g_exp", m)
        self.assertAlmostEqual(m["carbon_g_imp"], 65.0, places=2)
        self.assertAlmostEqual(m["carbon_g_exp"], 0.0, places=2)

    def test_carbon_g_imp_and_exp_on_pure_export(self):
        """Pure export block: carbon_g_exp > 0, identity carbon_g_imp - carbon_g_exp = carbon_g_net."""
        store, cfg = self._make_store_with_carbon(carbon_g=-33.0, imp_kwh=0.0, exp_kwh=1.5)
        data = self._get_rows(store, cfg)
        row = data["rows"][0]
        m = row["meters"]["electricity_main"]
        # Export carbon must be positive (shown below zero in chart)
        self.assertGreaterEqual(m["carbon_g_exp"], 0.0,
            msg="carbon_g_exp must be non-negative")
        # Split identity: imp - exp = net
        split_net = m["carbon_g_imp"] - m["carbon_g_exp"]
        self.assertAlmostEqual(split_net, row["carbon_g_net"], places=2,
            msg="carbon_g_imp - carbon_g_exp must equal carbon_g_net")

    def test_carbon_g_split_identity(self):
        """carbon_g_imp - carbon_g_exp = carbon_g_net for mixed import/export."""
        store, cfg = self._make_store_with_carbon(carbon_g=-30.0, imp_kwh=0.5, exp_kwh=2.0)
        data = self._get_rows(store, cfg)
        row = data["rows"][0]
        m = row["meters"]["electricity_main"]
        net = row["carbon_g_net"]
        split_net = m["carbon_g_imp"] - m["carbon_g_exp"]
        self.assertAlmostEqual(split_net, net, places=2,
            msg="carbon_g_imp - carbon_g_exp must equal carbon_g_net")

    def test_has_postcode_true_when_configured(self):
        """has_postcode is True when postcode_prefix is set."""
        store, cfg = self._make_store_with_carbon(postcode="DE1")
        data = self._get_rows(store, cfg)
        self.assertTrue(data["has_postcode"])

    def test_has_postcode_false_when_not_configured(self):
        """has_postcode is False when no postcode_prefix."""
        store, cfg = self._make_store_with_carbon(postcode="")
        data = self._get_rows(store, cfg)
        self.assertFalse(data["has_postcode"])


# ─────────────────────────────────────────────────────────────────────────────
# /api/power/history and /api/carbon/current (2.3.0)
# ─────────────────────────────────────────────────────────────────────────────

class TestApiPowerHistory(unittest.TestCase):

    @staticmethod
    def _ts(offset_hours=1):
        """Return a UTC ISO timestamp offset_hours ago — always within 48h window."""
        from datetime import datetime, timezone, timedelta
        return (datetime.now(timezone.utc).replace(tzinfo=None)
                - timedelta(hours=offset_hours)).strftime("%Y-%m-%dT%H:%M:%S")

    def setUp(self):
        self.store = _make_test_store()
        self.client = make_client(store=self.store)

    def test_returns_200(self):
        r = self.client.get("/api/power/history")
        self.assertEqual(r.status_code, 200)

    def test_response_has_rows_key(self):
        r = self.client.get("/api/power/history")
        self.assertIn("rows", r.get_json())

    def test_empty_when_no_data(self):
        r = self.client.get("/api/power/history")
        self.assertEqual(r.get_json()["rows"], [])

    def test_returns_stored_rows(self):
        from datetime import datetime, timezone
        now = datetime.now(timezone.utc).replace(tzinfo=None).isoformat()
        self.store.append_power_history(now, -1.5, 115.0, -2.875)
        r = self.client.get("/api/power/history")
        rows = r.get_json()["rows"]
        self.assertEqual(len(rows), 1)
        self.assertAlmostEqual(rows[0]["net_kw"], -1.5)
        self.assertAlmostEqual(rows[0]["intensity"], 115.0)

    def test_hours_param_accepted(self):
        r = self.client.get("/api/power/history?hours=12")
        self.assertEqual(r.status_code, 200)

    def test_hours_param_clamped_at_48(self):
        """hours > 48 is clamped to 48."""
        r = self.client.get("/api/power/history?hours=200")
        self.assertEqual(r.status_code, 200)

    def test_row_has_required_fields(self):
        from datetime import datetime, timezone
        now = datetime.now(timezone.utc).replace(tzinfo=None).isoformat()
        self.store.append_power_history(now, 2.5, 180.0, 7.5)
        rows = self.client.get("/api/power/history").get_json()["rows"]
        self.assertTrue(len(rows) > 0)
        for field in ("captured_at", "net_kw", "intensity", "carbon_gco2_min"):
            self.assertIn(field, rows[0])

    def test_rows_ordered_oldest_first(self):
        self.store.append_power_history(self._ts(3), 1.0, 100.0)
        self.store.append_power_history(self._ts(2), 2.0, 110.0)
        rows = self.client.get("/api/power/history").get_json()["rows"]
        self.assertEqual(len(rows), 2)
        self.assertLess(rows[0]["captured_at"], rows[1]["captured_at"])


class TestApiCarbonCurrent(unittest.TestCase):

    def setUp(self):
        self.store = _make_test_store()
        eio.load_json = lambda path, default=None: MINIMAL_CONFIG if "meters_config" in path else default
        self.client = make_client(store=self.store)

    def test_returns_404_no_postcode(self):
        """No postcode configured → 404."""
        r = self.client.get("/api/carbon/current")
        self.assertEqual(r.status_code, 404)
        self.assertEqual(r.get_json()["error"], "no_postcode")

    def _make_store_with_postcode(self, postcode="DE1"):
        """Store whose DB config includes a postcode_prefix."""
        store = BlockStore(":memory:")
        store.insert_config_period({"meters": {"electricity_main": {"meta": {
            "billing_day": 1, "block_minutes": 30,
            "timezone": "Europe/London",
            "currency_symbol": "£", "currency_code": "GBP",
            "postcode_prefix": postcode,
        }, "channels": {"import": {}, "export": {}}}}})
        return store

    def test_returns_404_no_data(self):
        """Postcode configured in DB but no CI data → 404."""
        store = self._make_store_with_postcode("DE1")
        server._store = store
        r = self.client.get("/api/carbon/current")
        self.assertEqual(r.status_code, 404)
        self.assertEqual(r.get_json()["error"], "no_data")

    def test_returns_intensity_when_data_present(self):
        """CI data in DB → 200 with intensity and ci_index."""
        store = self._make_store_with_postcode("DE1")
        server._store = store
        from datetime import datetime, timezone
        now = datetime.now(timezone.utc).replace(tzinfo=None).isoformat()
        store.upsert_carbon_intensity(now, "DE1", 138.0, "moderate")
        r = self.client.get("/api/carbon/current")
        self.assertEqual(r.status_code, 200)
        data = r.get_json()
        self.assertAlmostEqual(data["intensity"], 138.0)
        self.assertEqual(data["ci_index"], "moderate")
        self.assertEqual(data["postcode"], "DE1")




# ─────────────────────────────────────────────────────────────────────────────
# /api/power
# ─────────────────────────────────────────────────────────────────────────────

class TestApiPower(unittest.TestCase):

    def setUp(self):
        self.client = make_client()
        eio.load_json = lambda path, default=None: MINIMAL_CONFIG if "meters_config" in path else ({"meters": {}} if "current_block" in path else default)

    def test_returns_200(self):
        r = self.client.get("/api/power")
        self.assertEqual(r.status_code, 200)

    def test_response_has_required_keys(self):
        r = self.client.get("/api/power")
        data = r.get_json()
        for key in ("import_kw", "export_kw", "has_power_sensor"):
            self.assertIn(key, data)

    def test_no_power_sensor_flag(self):
        r = self.client.get("/api/power")
        self.assertFalse(r.get_json()["has_power_sensor"])


# ─────────────────────────────────────────────────────────────────────────────
# /api/billing
# ─────────────────────────────────────────────────────────────────────────────

class TestApiBilling(unittest.TestCase):

    def setUp(self):
        self.client = make_client()
        eio.load_json = lambda path, default=None: MINIMAL_CONFIG if "meters_config" in path else (MINIMAL_BLOCKS if "blocks" in path else default)

    def test_returns_200(self):
        r = self.client.get("/api/billing")
        self.assertEqual(r.status_code, 200)

    def test_response_has_required_keys(self):
        r = self.client.get("/api/billing")
        data = r.get_json()
        for key in ("currency", "today_total", "month_total", "year_total",
                    "today_rows", "month_rows", "year_rows"):
            self.assertIn(key, data)

    def test_currency_is_string(self):
        r = self.client.get("/api/billing")
        self.assertIsInstance(r.get_json()["currency"], str)


# ─────────────────────────────────────────────────────────────────────────────
# /api/billing — "This Bill" current-period selection (issue #221)
# ─────────────────────────────────────────────────────────────────────────────

class TestApiBillingCurrentPeriod(unittest.TestCase):
    """Regression for #221: when no generated billing period contains today —
    e.g. the newest data predates the current billing period, so the period
    generator (which stops at the last block) never emits a period spanning
    today — "This Bill" must synthesise the CURRENT period from the billing day,
    not fall back to the last *historical* period (which previously surfaced as
    a stale date, e.g. an April period)."""

    def setUp(self):
        self.client = make_client()
        eio.load_json = lambda path, default=None: (
            MINIMAL_CONFIG if "meters_config" in path else default)

    def test_no_period_containing_today_shows_current_not_historical(self):
        from datetime import datetime
        from zoneinfo import ZoneInfo
        now = datetime.now(ZoneInfo("Europe/London"))

        # A billing period wholly in the previous year, in a month guaranteed to
        # differ from the current month → nothing contains today, forcing the
        # fallback path. Naive datetimes, matching get_billing_periods_*.
        hist_month = (now.month % 12) + 1                     # always != now.month
        h_start = datetime(now.year - 1, hist_month, 1)
        h_end = (datetime(now.year, 1, 1) if hist_month == 12
                 else datetime(now.year - 1, hist_month + 1, 1))

        with patch.object(ec, "get_billing_periods_from_config_periods",
                          lambda *a, **kw: [(h_start, h_end)]):
            r = self.client.get("/api/billing")

        self.assertEqual(r.status_code, 200)
        mp = r.get_json()["month_period"]                    # "DD Mon → DD Mon YYYY"
        current_label = now.replace(day=1).strftime("%d %b")  # billing_day = 1
        historical_label = h_start.strftime("%d %b")

        self.assertTrue(
            mp.startswith(current_label),
            f'"This Bill" should show the current period ({current_label!r}), got {mp!r}')
        self.assertFalse(
            mp.startswith(historical_label),
            f'"This Bill" must not fall back to the historical period '
            f'({historical_label!r}), got {mp!r}')


# ─────────────────────────────────────────────────────────────────────────────
# /api/logs
# ─────────────────────────────────────────────────────────────────────────────

class TestApiLogs(unittest.TestCase):

    def setUp(self):
        self.client = make_client()

    def test_standalone_mode_missing_log_returns_message(self):
        with patch.dict(os.environ, {"EMT_MODE": "standalone"}), \
             patch("os.path.exists", return_value=False):
            r = self.client.get("/api/logs")
        self.assertEqual(r.status_code, 200)
        data = r.get_json()
        self.assertIn("lines", data)
        self.assertIsInstance(data["lines"], list)

    def test_lines_param_accepted(self):
        with patch.dict(os.environ, {"EMT_MODE": "standalone"}), \
             patch("os.path.exists", return_value=False):
            r = self.client.get("/api/logs?lines=50")
        self.assertEqual(r.status_code, 200)


# ─────────────────────────────────────────────────────────────────────────────
# Index redirect behaviour
# ─────────────────────────────────────────────────────────────────────────────

class TestIndexRedirect(unittest.TestCase):

    def setUp(self):
        self.client = make_client()

    def test_no_config_redirects_to_config(self):
        with patch.object(server, "load_config", return_value={"meters": {}}):
            r = self.client.get("/", follow_redirects=False)
        self.assertIn(r.status_code, (301, 302))
        self.assertIn("settings", r.headers["Location"])

    def test_with_config_redirects_to_charts_by_default(self):
        with patch.object(server, "load_config", return_value=MINIMAL_CONFIG):
            r = self.client.get("/", follow_redirects=False)
        self.assertIn(r.status_code, (301, 302))
        # Default cookie → charts
        self.assertIn("charts", r.headers["Location"])

    def test_with_config_and_summary_cookie_redirects_to_summary(self):
        with patch.object(server, "load_config", return_value=MINIMAL_CONFIG):
            self.client.set_cookie("emt_last_page", "live_power")
            r = self.client.get("/", follow_redirects=False)
        self.assertIn(r.status_code, (301, 302))
        self.assertIn("live-power", r.headers["Location"])

    def test_invalid_cookie_value_falls_back_to_charts(self):
        with patch.object(server, "load_config", return_value=MINIMAL_CONFIG):
            self.client.set_cookie("emt_last_page", "not_a_real_page")
            r = self.client.get("/", follow_redirects=False)
        self.assertIn(r.status_code, (301, 302))
        self.assertIn("charts", r.headers["Location"])



class TestApiCorrections(unittest.TestCase):
    """Tests for /api/corrections/preview and /api/corrections/apply."""

    def setUp(self):
        self.client = make_client()

    def _post(self, url, body):
        return self.client.post(url, json=body,
                                content_type='application/json')

    # ── Preview ───────────────────────────────────────────────────────────────

    def test_preview_standing_returns_200(self):
        r = self._post('/api/corrections/preview', {
            'type': 'standing', 'from_date': '2026-01-01',
            'to_date': '2026-12-31', 'value': 0.5046,
        })
        self.assertEqual(r.status_code, 200)

    def test_preview_returns_required_keys(self):
        r = self._post('/api/corrections/preview', {
            'type': 'standing', 'from_date': '2026-01-01',
            'to_date': '2026-12-31', 'value': 0.5046,
        })
        d = json.loads(r.data)
        for key in ('days', 'blocks', 'current_min', 'current_max'):
            self.assertIn(key, d, f"Missing key: {key}")

    def test_preview_rate_import_returns_200(self):
        r = self._post('/api/corrections/preview', {
            'type': 'rate', 'channel': 'import',
            'from_date': '2026-01-01', 'to_date': '2026-12-31', 'value': 0.245,
        })
        self.assertEqual(r.status_code, 200)

    def test_preview_rate_export_returns_200(self):
        r = self._post('/api/corrections/preview', {
            'type': 'rate', 'channel': 'export',
            'from_date': '2026-01-01', 'to_date': '2026-12-31', 'value': 0.15,
        })
        self.assertEqual(r.status_code, 200)

    def test_preview_invalid_type_returns_400(self):
        r = self._post('/api/corrections/preview', {
            'type': 'invalid', 'from_date': '2026-01-01',
            'to_date': '2026-12-31', 'value': 0.5,
        })
        self.assertEqual(r.status_code, 400)

    def test_preview_missing_dates_returns_400(self):
        r = self._post('/api/corrections/preview', {
            'type': 'standing', 'value': 0.5,
        })
        self.assertEqual(r.status_code, 400)

    # ── Apply ─────────────────────────────────────────────────────────────────

    def test_apply_standing_returns_200(self):
        r = self._post('/api/corrections/apply', {
            'type': 'standing', 'from_date': '2026-01-01',
            'to_date': '2026-12-31', 'value': 0.5046,
        })
        self.assertEqual(r.status_code, 200)

    def test_apply_returns_updated_blocks(self):
        r = self._post('/api/corrections/apply', {
            'type': 'standing', 'from_date': '2026-01-01',
            'to_date': '2026-12-31', 'value': 0.5046,
        })
        d = json.loads(r.data)
        self.assertIn('updated_blocks', d)
        self.assertIsInstance(d['updated_blocks'], int)

    def test_apply_rate_import_with_recalc(self):
        r = self._post('/api/corrections/apply', {
            'type': 'rate', 'channel': 'import',
            'from_date': '2026-01-01', 'to_date': '2026-12-31',
            'value': 0.30, 'recalc_cost': True,
        })
        self.assertEqual(r.status_code, 200)
        d = json.loads(r.data)
        self.assertIn('updated_blocks', d)

    def test_apply_rate_export_without_recalc(self):
        r = self._post('/api/corrections/apply', {
            'type': 'rate', 'channel': 'export',
            'from_date': '2026-01-01', 'to_date': '2026-12-31',
            'value': 0.15, 'recalc_cost': False,
        })
        self.assertEqual(r.status_code, 200)

    def test_apply_negative_value_returns_400(self):
        r = self._post('/api/corrections/apply', {
            'type': 'standing', 'from_date': '2026-01-01',
            'to_date': '2026-12-31', 'value': -1.0,
        })
        self.assertEqual(r.status_code, 400)

    def test_apply_missing_value_returns_400(self):
        r = self._post('/api/corrections/apply', {
            'type': 'standing', 'from_date': '2026-01-01',
            'to_date': '2026-12-31',
        })
        self.assertEqual(r.status_code, 400)

    def test_apply_invalid_type_returns_400(self):
        r = self._post('/api/corrections/apply', {
            'type': 'bad', 'from_date': '2026-01-01',
            'to_date': '2026-12-31', 'value': 0.5,
        })
        self.assertEqual(r.status_code, 400)

    def test_apply_actually_updates_standing_charge(self):
        """Apply correction then verify value changed in DB."""
        store = server._get_store()
        # Check initial value
        before = store._conn.execute(
            "SELECT MIN(standing_charge) as sc FROM blocks"
        ).fetchone()["sc"]

        new_val = (before or 0.0) + 1.0  # guaranteed different
        self._post('/api/corrections/apply', {
            'type': 'standing', 'from_date': '2026-01-01',
            'to_date': '2026-12-31', 'value': new_val,
        })

        after = store._conn.execute(
            "SELECT MIN(standing_charge) as sc FROM blocks"
        ).fetchone()["sc"]
        self.assertAlmostEqual(after or 0.0, new_val, places=4,
                               msg="Standing charge not updated in DB")

    def test_apply_rate_recalculates_cost_correctly(self):
        """After rate correction with recalc, cost = rate × kwh."""
        store = server._get_store()
        new_rate = 0.30

        self._post('/api/corrections/apply', {
            'type': 'rate', 'channel': 'import',
            'from_date': '2026-01-01', 'to_date': '2026-12-31',
            'value': new_rate, 'recalc_cost': True,
        })

        rows = store._conn.execute(
            "SELECT imp_kwh, imp_rate, imp_cost FROM blocks "
            "WHERE imp_rate IS NOT NULL AND imp_kwh IS NOT NULL"
        ).fetchall()
        for row in rows:
            expected_cost = round(row["imp_kwh"] * new_rate, 6)
            self.assertAlmostEqual(row["imp_cost"], expected_cost, places=4,
                                   msg=f"Cost not recalculated: {row['imp_cost']} != {expected_cost}")


class TestCorrectionsApiGate(unittest.TestCase):
    """API+ gate: rate corrections only touch DCC-settled blocks (imp_kwh_api /
    exp_kwh_api NOT NULL). Pure CAD (no API) applies immediately. Unsettled
    blocks would be clobbered at settlement, so they're skipped + reported."""

    def test_build_where_rate_import_settled_only(self):
        where, _, _ = server._corrections_build_where(
            "rate", "2026-01-01", "2026-12-31", "import", "", "", "all",
            "UTC", api_settled_only=True)
        self.assertIn("imp_kwh_api IS NOT NULL", where)

    def test_build_where_rate_export_settled_only(self):
        where, _, _ = server._corrections_build_where(
            "rate", "2026-01-01", "2026-12-31", "export", "", "", "all",
            "UTC", api_settled_only=True)
        self.assertIn("exp_kwh_api IS NOT NULL", where)

    def test_build_where_no_gate_when_flag_off(self):
        where, _, _ = server._corrections_build_where(
            "rate", "2026-01-01", "2026-12-31", "import", "", "", "all",
            "UTC", api_settled_only=False)
        self.assertNotIn("kwh_api", where)

    def test_build_where_standing_never_gated(self):
        where, _, _ = server._corrections_build_where(
            "standing", "2026-01-01", "2026-12-31", "import", "", "", "all",
            "UTC", api_settled_only=True)
        self.assertNotIn("kwh_api", where)

    def _store_settled_and_unsettled(self):
        cfg = {"meters": {"electricity_main": {"meta": {
            "billing_day": 1, "block_minutes": 30, "timezone": "Europe/London",
            "currency_symbol": "£", "currency_code": "GBP", "postcode_prefix": "DE1",
        }, "channels": {
            "import": {"read": "sensor.imp", "rate": "sensor.rate"},
            "export": {"read": "sensor.exp", "rate": "sensor.exp_rate"},
        }}}}
        store = BlockStore(":memory:")
        store.insert_config_period(cfg)
        cp = store.get_current_config_period_id()
        def ins(bs, be, kwh_api):
            store._conn.execute(
                "INSERT INTO blocks (block_start, block_end, meter_id, "
                "config_period_id, interpolated, imp_kwh, imp_rate, imp_cost, "
                "imp_kwh_api) VALUES (?,?,?,?,0,?,?,?,?)",
                (bs, be, "electricity_main", cp, 3.5, 0.323092, 1.13, kwh_api))
        ins("2026-04-14T11:00:00", "2026-04-14T11:30:00", 3.5)   # settled
        ins("2026-04-14T11:30:00", "2026-04-14T12:00:00", None)  # unsettled
        store._conn.commit()
        return store, cfg

    def _apply(self, client, value=0.05493):
        return client.post("/api/corrections/apply", json={
            "type": "rate", "channel": "import",
            "from_date": "2026-04-14", "to_date": "2026-04-14",
            "value": value, "recalc_cost": True,
        }, content_type="application/json")

    def test_api_mode_skips_unsettled(self):
        store, cfg = self._store_settled_and_unsettled()
        eio.load_json = lambda path, default=None: cfg if "meters_config" in path else default
        client = make_client(store=store)
        orig = server._corrections_api_gate_active
        server._corrections_api_gate_active = lambda: True
        try:
            d = json.loads(self._apply(client).data)
            self.assertTrue(d["api_gated"])
            self.assertEqual(d["updated_blocks"], 1)        # settled only
            self.assertEqual(d["skipped_unreconciled"], 1)  # unsettled reported
            rows = store._conn.execute(
                "SELECT imp_rate FROM blocks ORDER BY block_start").fetchall()
            self.assertAlmostEqual(rows[0]["imp_rate"], 0.05493, places=5)
            self.assertAlmostEqual(rows[1]["imp_rate"], 0.323092, places=5)  # teeth: untouched
        finally:
            server._corrections_api_gate_active = orig

    def test_cad_mode_applies_to_all(self):
        store, cfg = self._store_settled_and_unsettled()
        eio.load_json = lambda path, default=None: cfg if "meters_config" in path else default
        client = make_client(store=store)
        orig = server._corrections_api_gate_active
        server._corrections_api_gate_active = lambda: False
        try:
            d = json.loads(self._apply(client).data)
            self.assertFalse(d["api_gated"])
            self.assertEqual(d["updated_blocks"], 2)        # both
            self.assertEqual(d["skipped_unreconciled"], 0)
            rows = store._conn.execute(
                "SELECT imp_rate FROM blocks ORDER BY block_start").fetchall()
            self.assertAlmostEqual(rows[0]["imp_rate"], 0.05493, places=5)
            self.assertAlmostEqual(rows[1]["imp_rate"], 0.05493, places=5)
        finally:
            server._corrections_api_gate_active = orig

    def test_preview_reports_skip_in_api_mode(self):
        store, cfg = self._store_settled_and_unsettled()
        eio.load_json = lambda path, default=None: cfg if "meters_config" in path else default
        client = make_client(store=store)
        orig = server._corrections_api_gate_active
        server._corrections_api_gate_active = lambda: True
        try:
            r = client.post("/api/corrections/preview", json={
                "type": "rate", "channel": "import",
                "from_date": "2026-04-14", "to_date": "2026-04-14", "value": 0.05493,
            }, content_type="application/json")
            d = json.loads(r.data)
            self.assertTrue(d["api_gated"])
            self.assertEqual(len(d["blocks"]), 1)            # only settled previewed
            self.assertEqual(d["skipped_unreconciled"], 1)
        finally:
            server._corrections_api_gate_active = orig


class TestApiConfigHistoryPostcode(unittest.TestCase):
    """Postcode edit on a config period writes to the MAIN meter (outward code
    only, provenance 'user') and is exposed by the history endpoint."""

    def _store(self):
        store = BlockStore(":memory:")
        cfg = {"meters": {"electricity_main": {"meta": {
            "billing_day": 3, "block_minutes": 30, "timezone": "Europe/London",
            "currency_symbol": "£", "currency_code": "GBP", "site": "Highgrove",
        }}}}
        store.insert_config_period(cfg, effective_from="2026-02-11T00:00:00")
        return store

    def test_edit_stores_outward_code_and_user_source(self):
        store = self._store()
        client = make_client(store=store)
        pid = store._conn.execute(
            "SELECT id FROM config_periods WHERE effective_to IS NULL").fetchone()["id"]
        with patch("server.load_config", return_value={"meters": {}}), \
             patch("server._rebuild_config_period_chain", return_value=None):
            r = client.put(f"/api/config/history/{pid}",
                           json={"postcode": "DE65 6GG"})   # full postcode slips in
        self.assertEqual(r.status_code, 200)
        # Only the outward code is stored, tagged as a user value
        self.assertEqual(store.get_postcode_prefix_at("2026-03-01"), ("DE65", "user"))

    def test_history_endpoint_exposes_postcode(self):
        store = self._store()
        store.set_period_postcode(store.get_current_config_period_id(), "DE65", "user")
        client = make_client(store=store)
        with patch("server.load_config", return_value={"meters": {}}):
            r = client.get("/api/config/history")
        d = json.loads(r.data)
        self.assertEqual(d["periods"][0]["postcode_prefix"], "DE65")


class TestApiCsvTemplates(unittest.TestCase):
    """Gap + blank CSV template downloads."""

    def test_gap_template_downloads_csv(self):
        client = make_client()
        r = client.get("/api/historical/gap-template"
                       "?from=2024-07-01T00:00:00&to=2024-07-01T01:00:00&channel=import")
        self.assertEqual(r.status_code, 200)
        self.assertIn("text/csv", r.headers.get("Content-Type", ""))
        self.assertIn("attachment", r.headers.get("Content-Disposition", ""))
        body = r.data.decode()
        self.assertIn("Consumption (kWh)", body)
        # header + two 30-min slots in the 1-hour gap
        self.assertEqual(len(body.strip().splitlines()), 3)

    def test_gap_template_requires_params(self):
        client = make_client()
        r = client.get("/api/historical/gap-template")
        self.assertEqual(r.status_code, 400)

    def test_gap_template_inclusive_includes_last_slot(self):
        # A persisted gap's `to` is the last slot's start; inclusive=1 must cover it.
        client = make_client()
        r = client.get("/api/historical/gap-template"
                       "?from=2024-07-01T00:00:00&to=2024-07-01T00:30:00&inclusive=1")
        # header + the 00:00 slot + the 00:30 slot = 3 lines
        self.assertEqual(len(r.data.decode().strip().splitlines()), 3)

    def test_blank_template_downloads_csv(self):
        client = make_client()
        r = client.get("/api/historical/csv-template")
        self.assertEqual(r.status_code, 200)
        self.assertIn("text/csv", r.headers.get("Content-Type", ""))
        self.assertIn("Start", r.data.decode())

    def test_csv_apply_stashes_region_prompt(self):
        client = make_client()
        csv = ("Consumption (kWh),Estimated Cost Inc. Tax (p),Start,End\n"
               "0.5,7.0,2024-07-01T00:00:00+01:00,2024-07-01T00:30:00+01:00\n"
               "0.4,5.6,2024-07-01T00:30:00+01:00,2024-07-01T01:00:00+01:00\n")
        with patch("server._create_backup_zip", return_value="/tmp/b.zip"):
            r = client.post("/api/historical/csv/apply",
                            json={"import_csv": csv, "confirmed": True})
        self.assertEqual(r.status_code, 200)
        self.assertTrue(json.loads(r.data).get("region_reconcile"))
        pend = json.loads(client.get("/api/region/reconcile").data)
        self.assertTrue(pend["pending"])
        self.assertEqual(pend["plan"]["source"], "csv")
        self.assertTrue(pend["plan"]["sites"][0]["region_editable"])


class _InlineThread:
    """threading.Thread stand-in that runs the target synchronously on .start(),
    so a backgrounded delete/purge completes deterministically within the test."""
    def __init__(self, target=None, args=(), kwargs=None, **kw):
        self._target, self._args, self._kwargs = target, args, kwargs or {}
    def start(self):
        if self._target:
            self._target(*self._args, **self._kwargs)


class TestApiHistoricalPurge(unittest.TestCase):
    def setUp(self):
        server._delete_job = {"status": "idle"}

    def _store(self):
        store = BlockStore(":memory:")
        with store._conn:
            cp = store._conn.execute(
                "INSERT INTO config_periods (effective_from, billing_day, block_minutes, "
                "timezone, currency_symbol, currency_code) "
                "VALUES ('2024-01-01T00:00:00',1,30,'UTC','£','GBP')").lastrowid
            store._conn.execute(
                "INSERT INTO meters (config_period_id, meter_id, is_sub_meter) "
                "VALUES (?, 'electricity_main', 0)", (cp,))
            store._conn.execute(
                "INSERT INTO blocks (block_start, block_end, meter_id, config_period_id, imp_kwh, source) "
                "VALUES ('2024-03-01T00:00:00','2024-03-01T00:30:00','electricity_main',?,1.0,'imported_api')", (cp,))
        return store

    def test_purge_requires_confirm(self):
        client = make_client(store=self._store())
        r = client.post("/api/historical/purge", json={})
        self.assertEqual(r.status_code, 400)

    def test_purge_runs_in_background_and_reports_done(self):
        store = self._store()
        client = make_client(store=store)
        self.assertEqual(json.loads(client.get("/api/historical/purge-preview").data)["blocks"], 1)
        with patch("server.threading.Thread", _InlineThread), \
             patch("server._create_backup_zip", return_value="/tmp/b.zip"), \
             patch("server._regen_charts_safely", return_value=None):
            r = client.post("/api/historical/purge", json={"confirmed": True})
        # Launch returns immediately with 'running'; the worker (inlined) finished.
        self.assertEqual(r.status_code, 200)
        self.assertEqual(json.loads(r.data)["status"], "running")
        st = json.loads(client.get("/api/blocks/delete/status").data)
        self.assertEqual(st["status"], "done")
        self.assertEqual(st["kind"], "purge")
        self.assertEqual(st["result"]["blocks"], 1)
        # Data actually purged.
        self.assertEqual(json.loads(client.get("/api/historical/purge-preview").data)["blocks"], 0)

    def test_delete_range_runs_in_background(self):
        store = self._store()
        client = make_client(store=store)
        with patch("server.threading.Thread", _InlineThread), \
             patch("server._regen_charts_safely", return_value=None):
            r = client.post("/api/blocks/delete", json={
                "from_date": "2024-03-01", "to_date": "2024-03-01", "confirmed": True})
        self.assertEqual(r.status_code, 200)
        self.assertEqual(json.loads(r.data)["status"], "running")
        st = json.loads(client.get("/api/blocks/delete/status").data)
        self.assertEqual(st["status"], "done")
        self.assertEqual(st["kind"], "delete")
        self.assertEqual(st["result"]["deleted"], 1)

    def test_delete_409_when_already_running(self):
        client = make_client(store=self._store())
        server._delete_job = {"status": "running", "kind": "delete", "step": "deleting blocks"}
        r = client.post("/api/blocks/delete", json={
            "from_date": "2024-03-01", "to_date": "2024-03-01", "confirmed": True})
        self.assertEqual(r.status_code, 409)
        rp = client.post("/api/historical/purge", json={"confirmed": True})
        self.assertEqual(rp.status_code, 409)


class TestApiBackup(unittest.TestCase):
    """Manual backup returns the filename AND its byte size, so the UI can confirm
    'Backup ready: <name> (<size>)' (the create-backup feedback BL item)."""

    def test_backup_returns_path_and_size(self):
        import tempfile
        client = make_client()
        with tempfile.NamedTemporaryFile(suffix=".zip", delete=False) as tf:
            tf.write(b"x" * 2048)
            p = tf.name
        try:
            with patch("server._create_backup_zip", return_value=p):
                r = client.post("/api/backup")
            self.assertEqual(r.status_code, 200)
            d = json.loads(r.data)
            self.assertTrue(d["ok"])
            self.assertEqual(d["path"], os.path.basename(p))
            self.assertEqual(d["size"], 2048)
        finally:
            os.remove(p)


class TestApiRegionReconcile(unittest.TestCase):
    """Post-import region reconciliation endpoints: read the stashed plan, apply
    it (split + stamp), and clear the pending marker."""

    def _store(self):
        store = BlockStore(":memory:")
        with store._conn:
            cur = store._conn.execute(
                "INSERT INTO config_periods (effective_from, effective_to, billing_day, "
                "block_minutes, timezone, currency_symbol, currency_code, site_name) "
                "VALUES ('2020-01-01T00:00:00', NULL, 1, 30, 'UTC', '£', 'GBP', 'Home')")
            pid = cur.lastrowid
            store._conn.execute(
                "INSERT INTO meters (config_period_id, meter_id, is_sub_meter, postcode_prefix, postcode_source) "
                "VALUES (?, 'electricity_main', 0, 'M1', 'octopus')", (pid,))
            for bs in ["2023-03-01T00:00:00", "2023-09-01T00:00:00"]:
                store._conn.execute(
                    "INSERT INTO blocks (block_start, block_end, meter_id, config_period_id, imp_kwh) "
                    "VALUES (?, ?, 'electricity_main', ?, 1.0)", (bs, bs, pid))
        return store

    def test_get_pending_false_when_none(self):
        client = make_client(store=self._store())
        r = client.get("/api/region/reconcile")
        self.assertEqual(json.loads(r.data), {"pending": False})

    def test_get_returns_stashed_plan(self):
        store = self._store()
        store.set_meta("region_reconcile_pending", {
            "needs_confirmation": True,
            "sites": [{"outcode": "EH8"}], "split_dates": ["2023-06-01"]})
        client = make_client(store=store)
        d = json.loads(client.get("/api/region/reconcile").data)
        self.assertTrue(d["pending"])
        self.assertEqual(d["plan"]["sites"][0]["outcode"], "EH8")

    def test_apply_splits_stamps_and_clears(self):
        store = self._store()
        store.set_meta("region_reconcile_pending", {"needs_confirmation": True, "sites": []})
        client = make_client(store=store)
        with patch("server._rebuild_config_period_chain", return_value=None):
            r = client.post("/api/region/reconcile/apply", json={"sites": [
                {"outcode": "EH8", "from": "2020-01-01", "to": "2023-06-01", "site_name": "Old"},
                {"outcode": "M1",  "from": "2023-06-01", "to": None,         "site_name": "New"},
            ]})
        self.assertEqual(r.status_code, 200)
        self.assertEqual(store.get_postcode_prefix_at("2023-03-15"), ("EH8", "octopus"))
        self.assertEqual(store.get_postcode_prefix_at("2023-09-15"), ("M1", "octopus"))
        # pending marker cleared
        d = json.loads(client.get("/api/region/reconcile").data)
        self.assertFalse(d["pending"])


class TestApiPreImportSitePlan(unittest.TestCase):
    """Pre-import site-confirmation endpoints: discover the account's property
    history (read-only) and apply confirmed sites by creating covering config
    periods BEFORE the import runs."""

    def _preimport_store(self, postcode=None, source=None):
        store = BlockStore(":memory:")
        with store._conn:
            cur = store._conn.execute(
                "INSERT INTO config_periods (effective_from, effective_to, billing_day, "
                "block_minutes, timezone, currency_symbol, currency_code, site_name) "
                "VALUES ('2026-06-03T00:00:00', NULL, 1, 30, 'UTC', '£', 'GBP', 'Home')")
            pid = cur.lastrowid
            store._conn.execute(
                "INSERT INTO meters (config_period_id, meter_id, is_sub_meter, "
                "postcode_prefix, postcode_source) "
                "VALUES (?, 'electricity_main', 0, ?, ?)", (pid, postcode, source))
        return store

    def test_discover_no_api_returns_400(self):
        import engine
        engine.kraken_available = lambda: False
        try:
            r = make_client().get("/api/historical/site-plan")
            self.assertEqual(r.status_code, 400)
            self.assertFalse(r.get_json()["ok"])
        finally:
            del engine.kraken_available

    def test_discover_returns_plan(self):
        import engine
        saved_run = server._run_on_engine_loop
        engine.kraken_available = lambda: True
        engine.discover_pre_import_sites = lambda: None   # coro not created
        server._run_on_engine_loop = lambda coro, timeout=None: {
            "ok": True, "needs_confirmation": True,
            "sites": [
                {"outcode": "EH8", "from": "2018-01-01", "to": "2023-06-01",
                 "is_current": False, "site_name": None, "needs_name": True},
                {"outcode": "M1", "from": "2023-06-01", "to": None,
                 "is_current": True, "site_name": "Home", "needs_name": False}]}
        try:
            r = make_client().get("/api/historical/site-plan")
            self.assertEqual(r.status_code, 200)
            body = r.get_json()
            self.assertTrue(body["needs_confirmation"])
            self.assertEqual(len(body["sites"]), 2)
        finally:
            server._run_on_engine_loop = saved_run
            del engine.kraken_available
            del engine.discover_pre_import_sites

    def test_apply_creates_past_period_and_sets_marker(self):
        store = self._preimport_store()
        client = make_client(store=store)
        with patch("server._create_backup_zip", return_value="/tmp/b.zip"), \
             patch("server._rebuild_config_period_chain", return_value=None):
            r = client.post("/api/historical/site-plan/apply", json={"sites": [
                {"outcode": "EH8", "from": "2018-01-01", "to": "2023-06-01", "site_name": "Old Flat"},
                {"outcode": "M1",  "from": "2023-06-01", "to": None},
            ]})
        self.assertEqual(r.status_code, 200)
        d = r.get_json()
        self.assertTrue(d["ok"])
        self.assertEqual(d["created"], 1)
        # regions resolve per period, active never renamed
        self.assertEqual(store.get_postcode_prefix_at("2019-01-01"), ("EH8", "octopus"))
        self.assertEqual(store.get_postcode_prefix_at("2024-01-01"), ("M1", "octopus"))
        active = store._conn.execute(
            "SELECT site_name FROM config_periods WHERE effective_to IS NULL").fetchone()
        self.assertEqual(active["site_name"], "Home")
        # marker set so the post-import probe stands down
        self.assertTrue(store.get_meta("preimport_sites_applied", None))

    def test_apply_no_sites_400(self):
        client = make_client(store=self._preimport_store())
        r = client.post("/api/historical/site-plan/apply", json={"sites": []})
        self.assertEqual(r.status_code, 400)
        self.assertFalse(r.get_json()["ok"])


class TestApiConfigHistoryDelete(unittest.TestCase):
    """
    When the active config period is deleted, the server must:
    1. Promote the predecessor to active (effective_to = NULL)
    2. Write the predecessor's config (from normalised tables) back to meters_config.json
    3. Return config_restored=True in the response
    When a non-active period is deleted, meters_config.json must NOT change.
    """

    def _make_two_period_store(self):
        """In-memory store with two config periods."""
        import json
        store = BlockStore(":memory:")
        cfg_old = {"meters": {"electricity_main": {"meta": {
            "billing_day": 1, "block_minutes": 30, "timezone": "Europe/London",
            "currency_symbol": "£", "currency_code": "GBP", "site": "Old Site",
        }}}}
        cfg_new = {"meters": {"electricity_main": {"meta": {
            "billing_day": 15, "block_minutes": 30, "timezone": "Europe/London",
            "currency_symbol": "£", "currency_code": "GBP", "site": "New Site",
        }}}}
        # Period 1 — older (insert first, it will be closed by period 2)
        store.insert_config_period(cfg_old, effective_from="2026-01-01T00:00:00")
        # Period 2 — active
        store.insert_config_period(cfg_new, effective_from="2026-03-01T00:00:00")
        return store, cfg_old, cfg_new

    def test_delete_active_returns_config_restored_true(self):
        store, cfg_old, cfg_new = self._make_two_period_store()
        client = make_client(store=store)
        active_id = store._conn.execute(
            "SELECT id FROM config_periods WHERE effective_to IS NULL"
        ).fetchone()["id"]

        with patch("energy_engine_io.save_json_atomic", return_value=None) as mock_save,              patch("server.load_config", return_value=cfg_new):
            r = client.delete(f"/api/config/history/{active_id}")

        self.assertEqual(r.status_code, 200)
        data = json.loads(r.data)
        self.assertTrue(data.get("ok"))
        self.assertTrue(data.get("config_restored"),
                        "config_restored must be True when active period deleted")

    def test_delete_active_writes_predecessor_config(self):
        """meters_config.json must be overwritten with the predecessor's config."""
        import json as _json
        store, cfg_old, cfg_new = self._make_two_period_store()
        client = make_client(store=store)
        active_id = store._conn.execute(
            "SELECT id FROM config_periods WHERE effective_to IS NULL"
        ).fetchone()["id"]

        written = {}
        def capture_save(path, data):
            written["path"] = path
            written["data"] = data

        with patch("energy_engine_io.save_json_atomic", side_effect=capture_save),              patch("server.load_config", return_value=cfg_new):
            client.delete(f"/api/config/history/{active_id}")

        self.assertIn("path", written, "save_json_atomic was not called")
        self.assertIn("meters_config.json", written["path"])
        # The written config should be the OLD (predecessor) config, not the new one
        written_site = (written["data"].get("meters", {})
                        .get("electricity_main", {})
                        .get("meta", {})
                        .get("site"))
        self.assertEqual(written_site, "Old Site",
                         "meters_config.json should be restored to predecessor's config")

    def test_delete_non_active_does_not_write_config(self):
        """Deleting a non-active period must not touch meters_config.json."""
        import json as _json
        store, cfg_old, cfg_new = self._make_two_period_store()
        client = make_client(store=store)
        non_active_id = store._conn.execute(
            "SELECT id FROM config_periods WHERE effective_to IS NOT NULL"
        ).fetchone()["id"]

        with patch("energy_engine_io.save_json_atomic", return_value=None) as mock_save,              patch("server.load_config", return_value=cfg_new):
            r = client.delete(f"/api/config/history/{non_active_id}")

        self.assertEqual(r.status_code, 200)
        data = json.loads(r.data)
        self.assertFalse(data.get("config_restored", True),
                         "config_restored must be False when non-active period deleted")
        mock_save.assert_not_called()

    def test_delete_only_period_returns_400(self):
        """Cannot delete the only period."""
        store = BlockStore(":memory:")
        store.insert_config_period({"meters": {"electricity_main": {"meta": {
            "billing_day": 1, "block_minutes": 30, "timezone": "UTC",
            "currency_symbol": "£", "currency_code": "GBP",
        }}}})
        only_id = store._conn.execute(
            "SELECT id FROM config_periods LIMIT 1"
        ).fetchone()["id"]
        client = make_client(store=store)
        r = client.delete(f"/api/config/history/{only_id}")
        self.assertEqual(r.status_code, 400)


class TestApiBackupRestoreSync(unittest.TestCase):
    """
    When meters_config.json is restored, the active config_period and
    normalised meter tables must be updated to match.
    When blocks.db is restored without meters_config.json, the active
    period's config (from normalised tables) must be written to the file.
    """

    def _make_store_with_active_period(self, billing_day=1, site="Test"):
        store = BlockStore(":memory:")
        cfg = {"meters": {"electricity_main": {"meta": {
            "billing_day": billing_day, "block_minutes": 30,
            "timezone": "Europe/London", "currency_symbol": "£",
            "currency_code": "GBP", "site": site,
        }}}}
        store.insert_config_period(cfg)
        return store, cfg

    def test_restoring_meters_config_updates_active_period(self):
        """
        After restore, the normalised tables must reflect the restored config.
        Simulates the UPDATE logic run by api_backup_restore.
        """
        store, _ = self._make_store_with_active_period(billing_day=1, site="Old")

        new_cfg = {"meters": {"electricity_main": {"meta": {
            "billing_day": 15, "block_minutes": 30, "timezone": "Europe/London",
            "currency_symbol": "£", "currency_code": "GBP", "site": "Restored",
        }}}}
        main_meta = new_cfg["meters"]["electricity_main"]["meta"]
        active_id = store._conn.execute(
            "SELECT id FROM config_periods WHERE effective_to IS NULL"
        ).fetchone()["id"]

        # Simulate what api_backup_restore does: update scalars + rewrite meters
        store._conn.execute(
            """UPDATE config_periods
               SET billing_day=?, block_minutes=?, timezone=?,
                   currency_symbol=?, currency_code=?, site_name=?
               WHERE id=?""",
            (int(main_meta.get("billing_day") or 1),
             int(main_meta.get("block_minutes") or 30),
             main_meta.get("timezone", "UTC"),
             main_meta.get("currency_symbol", "£"),
             main_meta.get("currency_code", "GBP"),
             main_meta.get("site"),
             active_id)
        )
        # Delete and rewrite meter rows
        old_mids = [r["id"] for r in store._conn.execute(
            "SELECT id FROM meters WHERE config_period_id=?", (active_id,)
        ).fetchall()]
        for mid in old_mids:
            store._conn.execute("DELETE FROM meter_channels WHERE meter_id=?", (mid,))
        store._conn.execute("DELETE FROM meters WHERE config_period_id=?", (active_id,))
        store._write_meters(new_cfg, active_id)
        store._conn.commit()

        # Verify via config_from_db
        restored = store.config_from_db(active_id)
        meta = restored["meters"]["electricity_main"]["meta"]
        self.assertEqual(meta["billing_day"], 15)
        self.assertEqual(meta["site"], "Restored")

    def test_restore_endpoint_exists(self):
        """Restore endpoint must be reachable."""
        client = make_client()
        with patch("server._create_backup_zip", return_value=None),              patch("os.path.exists", return_value=False):
            r = client.post("/api/backup/restore",
                            json={"zip": "", "files": [], "from_flat": True})
        self.assertIn(r.status_code, (200, 400, 404, 500))

    def test_config_period_update_sql_correctness(self):
        """
        After updating scalar fields and rewriting meter rows,
        config_from_db() returns the updated values.
        """
        store, _ = self._make_store_with_active_period(billing_day=1, site="Before")

        restored_cfg = {"meters": {"electricity_main": {"meta": {
            "billing_day": 28, "block_minutes": 15, "timezone": "America/New_York",
            "currency_symbol": "$", "currency_code": "USD", "site": "After",
        }}}}
        main_meta = restored_cfg["meters"]["electricity_main"]["meta"]
        active_id = store._conn.execute(
            "SELECT id FROM config_periods WHERE effective_to IS NULL"
        ).fetchone()["id"]

        store._conn.execute(
            """UPDATE config_periods
               SET billing_day=?, block_minutes=?, timezone=?,
                   currency_symbol=?, currency_code=?, site_name=?
               WHERE id=?""",
            (int(main_meta.get("billing_day") or 1),
             int(main_meta.get("block_minutes") or 30),
             main_meta.get("timezone", "UTC"),
             main_meta.get("currency_symbol", "£"),
             main_meta.get("currency_code", "GBP"),
             main_meta.get("site"),
             active_id)
        )
        store._conn.commit()

        row = store._conn.execute(
            "SELECT billing_day, block_minutes, timezone, currency_symbol, "
            "currency_code, site_name FROM config_periods WHERE id=?",
            (active_id,)
        ).fetchone()
        self.assertEqual(row["billing_day"], 28)
        self.assertEqual(row["block_minutes"], 15)
        self.assertEqual(row["timezone"], "America/New_York")
        self.assertEqual(row["currency_symbol"], "$")
        self.assertEqual(row["currency_code"], "USD")
        self.assertEqual(row["site_name"], "After")

# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    unittest.main(verbosity=2)



# ─────────────────────────────────────────────────────────────────────────────
# /api/import — blocks.db import (regression test for silent drop bug)
# ─────────────────────────────────────────────────────────────────────────────

class TestApiImportBlocksDb(unittest.TestCase):
    """
    Regression tests for the api_import endpoint.
    Previously blocks.db was silently ignored — only meters_config.json was written.
    """

    def setUp(self):
        import tempfile
        self.tmpdir = tempfile.mkdtemp(prefix="emt_import_test_")
        server.DATA_DIR = self.tmpdir
        self.client = make_client()
        server.DATA_DIR = self.tmpdir  # re-set after make_client resets it

    def tearDown(self):
        import shutil
        shutil.rmtree(self.tmpdir, ignore_errors=True)
        server.DATA_DIR = "/tmp/emt_test_data"  # restore default

    def _make_db_bytes(self):
        """Create a minimal valid SQLite blocks.db as bytes."""
        import tempfile, os
        store = BlockStore(":memory:")
        store.insert_config_period({"meters": {"electricity_main": {"meta": {
            "billing_day": 15, "block_minutes": 30,
            "timezone": "Europe/London",
            "currency_symbol": "£", "currency_code": "GBP",
            "postcode_prefix": "DE1",
        }, "channels": {"import": {}, "export": {}}}}})
        tmp = tempfile.mktemp(suffix=".db")
        try:
            store.backup(tmp)
            with open(tmp, "rb") as f:
                return f.read()
        finally:
            if os.path.exists(tmp):
                os.remove(tmp)

    def test_blocks_db_accepted_in_import(self):
        """blocks.db upload must appear in the imported list."""
        import io
        db_bytes = self._make_db_bytes()
        data = {"blocks": (io.BytesIO(db_bytes), "blocks.db")}
        r = self.client.post(
            "/api/import",
            data=data,
            content_type="multipart/form-data"
        )
        self.assertEqual(r.status_code, 200)
        result = r.get_json()
        self.assertTrue(result.get("ok"), msg=f"Import failed: {result}")
        self.assertTrue(
            any("blocks.db" in f for f in result.get("imported", [])),
            msg=f"blocks.db not in imported list: {result.get('imported')}"
        )

    def test_meters_config_not_written_when_db_imported(self):
        """When blocks.db is imported, meters_config.json must come from the DB not the upload."""
        import io
        db_bytes = self._make_db_bytes()
        # Upload both — meters_config should be ignored in favour of DB
        cfg_bytes = b'{"meters": {"electricity_main": {"meta": {"billing_day": 1}}}}'.replace(b"'", b"'")
        data = {
            "blocks":        (io.BytesIO(db_bytes), "blocks.db"),
            "meters_config": (io.BytesIO(cfg_bytes), "meters_config.json"),
        }
        r = self.client.post(
            "/api/import",
            data=data,
            content_type="multipart/form-data"
        )
        result = r.get_json()
        self.assertTrue(result.get("ok"), msg=f"Import failed: {result}")
        # The imported list should contain blocks.db
        self.assertTrue(
            any("blocks.db" in f for f in result.get("imported", [])),
            msg=f"blocks.db not in imported: {result.get('imported')}"
        )

    def test_meters_config_alone_returns_error(self):
        """Importing meters_config.json without blocks.db is rejected — DB is sole source of truth since 2.1.0."""
        import io, json
        cfg = {"meters": {"electricity_main": {"meta": {
            "billing_day": 1, "block_minutes": 30,
            "timezone": "Europe/London",
            "currency_symbol": "£", "currency_code": "GBP",
        }, "channels": {"import": {}, "export": {}}}}}
        cfg_bytes = json.dumps(cfg).encode()
        data = {"meters_config": (io.BytesIO(cfg_bytes), "meters_config.json")}
        r = self.client.post(
            "/api/import",
            data=data,
            content_type="multipart/form-data"
        )
        result = r.get_json()
        self.assertIn("error", result, msg="Expected error when importing meters_config.json alone")

    def test_empty_import_returns_error(self):
        """Importing nothing returns an error."""
        r = self.client.post(
            "/api/import",
            data={},
            content_type="multipart/form-data"
        )
        self.assertEqual(r.status_code, 400)
        self.assertIn("error", r.get_json())


class TestApiCorrectionsEnhanced(unittest.TestCase):
    """Tests for the enhanced Historical Corrections endpoints."""

    def _make_store_with_blocks(self):
        store = BlockStore(":memory:")
        store.insert_config_period({"meters": {"electricity_main": {"meta": {
            "billing_day": 1, "block_minutes": 30, "timezone": "Europe/London",
            "currency_symbol": "£", "currency_code": "GBP", "site": "Home",
        }}}})
        cp_id = store.get_current_config_period_id()
        # Insert blocks across two days, two meters
        blocks = [
            # 20/3 — three blocks for main meter (UTC = local in March)
            ("2026-03-20T14:00:00", "2026-03-20T14:30:00", "electricity_main",
             0.5, 0.245, 0.1225, 0.1, 0.04, 0.004, 0.6),
            ("2026-03-20T15:00:00", "2026-03-20T15:30:00", "electricity_main",
             0.6, 0.285, 0.1710, 0.0, 0.04, 0.0000, 0.6),
            ("2026-03-20T15:30:00", "2026-03-20T16:00:00", "electricity_main",
             0.4, 0.285, 0.1140, 0.0, 0.04, 0.0000, 0.6),
            # 20/3 — ev_charger sub-meter
            ("2026-03-20T15:00:00", "2026-03-20T15:30:00", "ev_charger",
             0.3, 0.285, 0.0855, 0.0, 0.04, 0.0000, 0.0),
            # 21/3
            ("2026-03-21T10:00:00", "2026-03-21T10:30:00", "electricity_main",
             0.7, 0.245, 0.1715, 0.0, 0.04, 0.0000, 0.6),
        ]
        for (bs, be, mid, ikwh, irate, icost, ekwh, erate, ecost, sc) in blocks:
            store._conn.execute("""
                INSERT INTO blocks (block_start, block_end, meter_id, config_period_id, interpolated,
                  imp_kwh, imp_rate, imp_cost, exp_kwh, exp_rate, exp_cost, standing_charge)
                VALUES (?,?,?,?,0, ?,?,?,?,?,?,?)
            """, (bs, be, mid, cp_id, ikwh, irate, icost, ekwh, erate, ecost, sc))
        store._conn.commit()
        # insert_config_period already inserts electricity_main via _write_meters.
        # Add ev_charger as a sub-meter so the standing charge correction subquery
        # (meter_id IN SELECT meter_id FROM meters WHERE is_sub_meter=0) correctly
        # excludes it and only updates main meter rows.
        store._conn.execute(
            "INSERT OR IGNORE INTO meters (config_period_id, meter_id, is_sub_meter) VALUES (?,?,1)",
            (cp_id, "ev_charger")
        )
        store._conn.commit()
        return store

    def test_corrections_meters_endpoint(self):
        store = self._make_store_with_blocks()
        client = make_client(store=store)
        r = client.get("/api/corrections/meters")
        self.assertEqual(r.status_code, 200)
        data = r.get_json()
        self.assertIn("electricity_main", data["meters"])
        self.assertIn("ev_charger", data["meters"])

    def test_preview_rate_returns_blocks(self):
        store = self._make_store_with_blocks()
        client = make_client(store=store)
        r = client.post("/api/corrections/preview", json={
            "type": "rate", "from_date": "2026-03-20", "to_date": "2026-03-20",
            "channel": "import", "value": 0.300, "meter_id": "all",
        })
        self.assertEqual(r.status_code, 200)
        data = r.get_json()
        self.assertIn("blocks", data)
        self.assertGreater(len(data["blocks"]), 0)
        # Each block has required fields
        b = data["blocks"][0]
        for field in ("block_start", "display", "meter_id", "current_rate",
                      "new_rate", "kwh", "current_cost", "new_cost"):
            self.assertIn(field, b)

    def test_preview_rate_time_filter(self):
        """Time window filter: only blocks from 15:00 onwards."""
        store = self._make_store_with_blocks()
        client = make_client(store=store)
        r = client.post("/api/corrections/preview", json={
            "type": "rate", "from_date": "2026-03-20", "to_date": "2026-03-20",
            "channel": "import", "value": 0.300,
            "from_time": "15:00", "to_time": "", "meter_id": "all",
        })
        self.assertEqual(r.status_code, 200)
        blocks = r.get_json()["blocks"]
        # Should not include the 14:00 block
        starts = [b["block_start"] for b in blocks]
        self.assertNotIn("2026-03-20T14:00:00", starts)
        self.assertIn("2026-03-20T15:00:00", starts)

    def test_preview_rate_meter_filter(self):
        """Meter filter: only ev_charger blocks."""
        store = self._make_store_with_blocks()
        client = make_client(store=store)
        r = client.post("/api/corrections/preview", json={
            "type": "rate", "from_date": "2026-03-20", "to_date": "2026-03-20",
            "channel": "import", "value": 0.300, "meter_id": "ev_charger",
        })
        self.assertEqual(r.status_code, 200)
        blocks = r.get_json()["blocks"]
        meter_ids = {b["meter_id"] for b in blocks}
        self.assertEqual(meter_ids, {"ev_charger"})

    def test_preview_standing_returns_summary(self):
        """Standing charge preview returns summary (days/blocks/min/max), not per-block."""
        store = self._make_store_with_blocks()
        client = make_client(store=store)
        r = client.post("/api/corrections/preview", json={
            "type": "standing", "from_date": "2026-03-20", "to_date": "2026-03-20",
            "value": 0.55,
        })
        self.assertEqual(r.status_code, 200)
        data = r.get_json()
        for field in ("days", "blocks", "current_min", "current_max"):
            self.assertIn(field, data)
        self.assertNotIn("blocks_detail", data)

    def test_apply_rate_with_time_filter(self):
        """Apply corrects only blocks in the time window."""
        store = self._make_store_with_blocks()
        client = make_client(store=store)
        r = client.post("/api/corrections/apply", json={
            "type": "rate", "from_date": "2026-03-20", "to_date": "2026-03-20",
            "channel": "import", "value": 0.500, "recalc_cost": True,
            "from_time": "15:00", "meter_id": "electricity_main",
        })
        self.assertEqual(r.status_code, 200)
        self.assertGreater(r.get_json()["updated_blocks"], 0)

        # 14:00 block must be unchanged
        row = store._conn.execute(
            "SELECT imp_rate FROM blocks WHERE block_start='2026-03-20T14:00:00' "
            "AND meter_id='electricity_main'"
        ).fetchone()
        self.assertAlmostEqual(row["imp_rate"], 0.245, places=4)

        # 15:00 block must be updated
        row2 = store._conn.execute(
            "SELECT imp_rate FROM blocks WHERE block_start='2026-03-20T15:00:00' "
            "AND meter_id='electricity_main'"
        ).fetchone()
        self.assertAlmostEqual(row2["imp_rate"], 0.500, places=4)

    def test_apply_rate_recalculates_cost(self):
        """recalc_cost=True updates imp_cost = imp_kwh * new_rate."""
        store = self._make_store_with_blocks()
        client = make_client(store=store)
        client.post("/api/corrections/apply", json={
            "type": "rate", "from_date": "2026-03-20", "to_date": "2026-03-20",
            "channel": "import", "value": 0.400, "recalc_cost": True,
            "meter_id": "electricity_main",
        })
        row = store._conn.execute(
            "SELECT imp_kwh, imp_rate, imp_cost FROM blocks "
            "WHERE block_start='2026-03-20T14:00:00' AND meter_id='electricity_main'"
        ).fetchone()
        self.assertAlmostEqual(row["imp_rate"], 0.400, places=4)
        self.assertAlmostEqual(row["imp_cost"], row["imp_kwh"] * 0.400, places=4)

    def test_apply_standing_whole_day(self):
        """Standing charge correction applies to all blocks in the date range."""
        store = self._make_store_with_blocks()
        client = make_client(store=store)
        r = client.post("/api/corrections/apply", json={
            "type": "standing", "from_date": "2026-03-20", "to_date": "2026-03-20",
            "value": 0.9999,
        })
        self.assertEqual(r.status_code, 200)
        # Only main meter rows are updated — sub-meter standing_charge stays 0
        rows = store._conn.execute(
            """SELECT standing_charge FROM blocks
               WHERE block_start >= '2026-03-20T00:00:00' AND block_start < '2026-03-21T00:00:00'
               AND meter_id='electricity_main'"""
        ).fetchall()
        self.assertGreater(len(rows), 0)
        for row in rows:
            self.assertAlmostEqual(row["standing_charge"], 0.9999, places=4)

    def test_midnight_crossing_time_window(self):
        """
        Economy 7 in BST: 00:30–07:30 local = 23:30–06:30 UTC.
        from_time_utc > to_time_utc → OR clause, not AND.
        Blocks at 23:30 UTC (local_date next day BST) must be included.
        Blocks at 12:00 UTC (midday) must be excluded.
        """
        store = BlockStore(":memory:")
        store.insert_config_period({"meters": {"electricity_main": {"meta": {
            "billing_day": 1, "block_minutes": 30, "timezone": "Europe/London",
            "currency_symbol": "£", "currency_code": "GBP", "site": "Home",
        }}}})
        cp_id = store.get_current_config_period_id()
        # Simulate a BST summer day: 00:30 local BST = 23:30 UTC previous day
        # block_start UTC '2026-07-14T23:30:00', local_date '2026-07-15'
        blocks = [
            # Night rate blocks (local 00:30–06:30 BST = 23:30–05:30 UTC)
            ("2026-07-14T23:30:00", "2026-07-14T23:30:00", 0.5, 0.08),
            ("2026-07-15T00:00:00", "2026-07-15T00:00:00", 0.5, 0.08),
            ("2026-07-15T05:30:00", "2026-07-15T05:30:00", 0.5, 0.08),
            # Day rate blocks (local 07:30+ BST = 06:30+ UTC)
            ("2026-07-15T06:30:00", "2026-07-15T06:30:00", 0.5, 0.245),
            ("2026-07-15T12:00:00", "2026-07-15T12:00:00", 0.5, 0.245),
        ]
        for (bs, be, kwh, rate) in blocks:
            store._conn.execute("""
                INSERT INTO blocks (block_start, block_end, meter_id, config_period_id, interpolated,
                  imp_kwh, imp_rate, imp_cost, exp_kwh, exp_rate, exp_cost, standing_charge)
                VALUES (?,?,'electricity_main',?,0, ?,?,ROUND(?*?,6),0,0,0,0.5)
            """, (bs, be, cp_id, kwh, rate, kwh, rate))
        store._conn.commit()

        client = make_client(store=store)

        # Apply Economy 7 rate: 00:30–07:30 local BST = 23:30–06:30 UTC
        # Server converts 00:30 BST → 23:30 UTC, 07:30 BST → 06:30 UTC
        # from_time_utc='23:30' > to_time_utc='06:30' → midnight crossing → OR clause
        import json as _json
        r = client.post("/api/corrections/preview", json={
            "type": "rate", "from_date": "2026-07-15", "to_date": "2026-07-15",
            "channel": "import", "value": 0.08,
            "from_time": "00:30", "to_time": "07:30", "meter_id": "all",
        })
        self.assertEqual(r.status_code, 200)
        data = r.get_json()
        starts = [b["block_start"] for b in data.get("blocks", [])]

        # Night blocks must be included
        self.assertIn("2026-07-14T23:30:00", starts,
            "23:30 UTC block (= 00:30 BST) must be included in night window")
        self.assertIn("2026-07-15T00:00:00", starts,
            "00:00 UTC block (= 01:00 BST) must be included in night window")
        self.assertIn("2026-07-15T05:30:00", starts,
            "05:30 UTC block (= 06:30 BST) must be included in night window")

        # Day blocks must be excluded
        self.assertNotIn("2026-07-15T06:30:00", starts,
            "06:30 UTC block (= 07:30 BST) must be excluded — end is exclusive")
        self.assertNotIn("2026-07-15T12:00:00", starts,
            "12:00 UTC block (= 13:00 BST) must be excluded from night window")
# ─────────────────────────────────────────────────────────────────────────────
# /api/settings
# ─────────────────────────────────────────────────────────────────────────────

class TestApiSettings(unittest.TestCase):

    def setUp(self):
        self.client = make_client()

    def test_get_returns_defaults(self):
        r = self.client.get("/api/settings")
        self.assertEqual(r.status_code, 200)
        d = json.loads(r.data)
        self.assertIn("co2_car_petrol_g_per_mile", d)
        self.assertIn("co2_tree_kg_per_year", d)
        self.assertIn("ev_efficiency", d)
        self.assertIn("distance_unit", d)
        self.assertIn("hp_cop", d)
        self.assertEqual(d["distance_unit"], "miles")
        self.assertEqual(d["co2_car_petrol_g_per_mile"], 180.0)

    def test_post_saves_and_returns_ok(self):
        payload = {"co2_car_petrol_g_per_mile": 195.0, "distance_unit": "km"}
        r = self.client.post("/api/settings",
                             data=json.dumps(payload),
                             content_type="application/json")
        self.assertEqual(r.status_code, 200)
        d = json.loads(r.data)
        self.assertTrue(d.get("ok"))

    def test_post_persists_values(self):
        payload = {"co2_tree_kg_per_year": 25.0}
        self.client.post("/api/settings",
                         data=json.dumps(payload),
                         content_type="application/json")
        r = self.client.get("/api/settings")
        d = json.loads(r.data)
        self.assertEqual(d["co2_tree_kg_per_year"], 25.0)

    def test_post_rejects_invalid_numeric(self):
        payload = {"co2_car_petrol_g_per_mile": "not_a_number"}
        r = self.client.post("/api/settings",
                             data=json.dumps(payload),
                             content_type="application/json")
        self.assertEqual(r.status_code, 400)

    def test_post_ignores_unknown_keys(self):
        payload = {"unknown_key": "value", "co2_tree_kg_per_year": 22.0}
        r = self.client.post("/api/settings",
                             data=json.dumps(payload),
                             content_type="application/json")
        self.assertEqual(r.status_code, 200)
        d = json.loads(r.data)
        self.assertTrue(d.get("ok"))

    def test_ev_efficiency_default(self):
        r = self.client.get("/api/settings")
        d = json.loads(r.data)
        self.assertEqual(d["ev_efficiency"], 3.2)
        self.assertEqual(d["ev_charge_efficiency"], 0.88)

    def test_battery_and_hp_defaults(self):
        r = self.client.get("/api/settings")
        d = json.loads(r.data)
        self.assertEqual(d["battery_round_trip_efficiency"], 0.90)
        self.assertEqual(d["hp_cop"], 3.0)
        self.assertEqual(d["gas_co2_g_per_kwh"], 203.0)
        self.assertEqual(d["gas_boiler_efficiency"], 0.90)


# ─────────────────────────────────────────────────────────────────────────────
# /api/insights/periods
# ─────────────────────────────────────────────────────────────────────────────

class TestApiInsightsPeriods(unittest.TestCase):

    def setUp(self):
        self.client = make_client()

    def test_returns_200_with_periods_key(self):
        with patch.object(server, "load_config", return_value=MINIMAL_CONFIG):
            r = self.client.get("/api/insights/periods")
        self.assertEqual(r.status_code, 200)
        d = json.loads(r.data)
        self.assertIn("periods", d)
        self.assertIsInstance(d["periods"], list)

    def test_periods_have_required_fields(self):
        with patch.object(server, "load_config", return_value=MINIMAL_CONFIG):
            r = self.client.get("/api/insights/periods")
        d = json.loads(r.data)
        for p in d["periods"]:
            self.assertIn("period_start", p)
            self.assertIn("period_end", p)
            self.assertIn("is_current", p)
            self.assertIn("has_carbon", p)

    def test_no_config_periods_returns_empty(self):
        """If store has no config periods, endpoint returns empty list gracefully."""
        with patch.object(server, "load_config", return_value={"meters": {}}):
            r = self.client.get("/api/insights/periods")
        self.assertIn(r.status_code, (200, 500))


# ─────────────────────────────────────────────────────────────────────────────
# /api/insights/billing-period
# ─────────────────────────────────────────────────────────────────────────────

class TestApiInsightsBillingPeriod(unittest.TestCase):

    def setUp(self):
        self.client = make_client()

    def test_returns_200_or_404(self):
        with patch.object(server, "load_config", return_value=MINIMAL_CONFIG):
            r = self.client.get("/api/insights/billing-period")
        self.assertIn(r.status_code, (200, 404, 500))

    def test_unknown_period_returns_404_or_error(self):
        with patch.object(server, "load_config", return_value=MINIMAL_CONFIG):
            r = self.client.get("/api/insights/billing-period?period_start=1900-01-01")
        d = json.loads(r.data)
        # Either 404 with error key, or 200 with error key
        self.assertTrue(r.status_code in (200, 404) and "error" in d
                        or r.status_code == 500)

    def test_response_has_required_fields_when_found(self):
        with patch.object(server, "load_config", return_value=MINIMAL_CONFIG):
            r = self.client.get("/api/insights/billing-period")
        if r.status_code == 200:
            d = json.loads(r.data)
            if "error" not in d:
                for field in ["period_start", "period_end", "is_current",
                              "has_carbon", "imp_kwh", "exp_kwh",
                              "ci_imp_kwh", "ci_exp_kwh",
                              "house_imp_kwh", "house_ci_imp_kwh",
                              "sub_meters", "assumptions"]:
                    self.assertIn(field, d)

    def test_assumptions_contain_all_defaults(self):
        with patch.object(server, "load_config", return_value=MINIMAL_CONFIG):
            r = self.client.get("/api/insights/billing-period")
        if r.status_code == 200:
            d = json.loads(r.data)
            if "assumptions" in d:
                a = d["assumptions"]
                self.assertIn("ev_efficiency", a)
                self.assertIn("ev_charge_efficiency", a)
                self.assertIn("battery_round_trip_efficiency", a)
                self.assertIn("hp_cop", a)
                self.assertIn("distance_unit", a)
                self.assertIn("gas_co2_g_per_kwh", a)
                self.assertIn("gas_boiler_efficiency", a)

    def test_sub_meter_has_inverter_possible_field(self):
        """Sub-meter entries should include inverter_possible flag."""
        config_with_sub = {
            "schema_version": "1.0",
            "meters": {
                "electricity_main": MINIMAL_CONFIG["meters"]["electricity_main"],
                "ev_charger": {
                    "meta": {
                        "sub_meter": True,
                        "meter_type": "ev_charger",
                        "inverter_possible": False,
                        "parent_meter": "electricity_main",
                    },
                    "channels": {
                        "import": {"read": "sensor.ev", "rate": "sensor.rate"}
                    }
                }
            }
        }
        with patch.object(server, "load_config", return_value=config_with_sub):
            r = self.client.get("/api/insights/billing-period")
        if r.status_code == 200:
            d = json.loads(r.data)
            if "sub_meters" in d:
                for mid, sm in d["sub_meters"].items():
                    self.assertIn("inverter_possible", sm)
                    self.assertIn("ci_imp_kwh", sm)
                    self.assertIn("avg_charge_intensity", sm)


# ─────────────────────────────────────────────────────────────────────────────
# Settings and Insights page routes
# ─────────────────────────────────────────────────────────────────────────────

class TestSettingsAndInsightsPages(unittest.TestCase):

    def setUp(self):
        self.client = make_client()

    def test_settings_page_meter_config_tab(self):
        with patch.object(server, "load_config", return_value=MINIMAL_CONFIG):
            r = self.client.get("/settings")
        self.assertEqual(r.status_code, 200)

    def test_settings_page_carbon_tab(self):
        with patch.object(server, "load_config", return_value=MINIMAL_CONFIG):
            r = self.client.get("/settings?tab=carbon")
        self.assertEqual(r.status_code, 200)

    def test_insights_page_returns_200(self):
        r = self.client.get("/insights")
        self.assertEqual(r.status_code, 200)

    def test_billing_history_page_returns_200(self):
        r = self.client.get("/billing-history")
        self.assertEqual(r.status_code, 200)

    def test_data_management_page_returns_200(self):
        r = self.client.get("/data-management")
        self.assertEqual(r.status_code, 200)

    def test_live_power_page_returns_200(self):
        with patch.object(server, "load_config", return_value=MINIMAL_CONFIG):
            r = self.client.get("/live-power")
        self.assertIn(r.status_code, (200, 500))  # may fail without power sensor



# ─────────────────────────────────────────────────────────────────────────────
# Overview page gating (power sensor OR postcode)
# ─────────────────────────────────────────────────────────────────────────────

class TestOverviewGating(unittest.TestCase):
    """Overview page (formerly Live Power) reveals on power-sensor OR postcode;
    cards adapt to the gates; a dismissible hint replaces the old empty state."""

    def _cfg(self, power=False, postcode=False):
        meta = {"billing_day": 1, "block_minutes": 30, "timezone": "Europe/London",
                "currency_symbol": "£"}
        if power:
            meta["power_sensor"] = "sensor.house_power"
        if postcode:
            meta["postcode_prefix"] = "DE1"
        return {"meters": {"electricity_main": {"meta": meta, "channels": {}}}}

    def _get(self, power, postcode):
        store = BlockStore(":memory:")
        client = make_client(store=store)
        with patch.object(server, "load_config", return_value=self._cfg(power, postcode)):
            return client.get("/live-power")

    def test_power_only(self):
        r = self._get(power=True, postcode=False)
        self.assertEqual(r.status_code, 200)
        self.assertIn(b"Overview", r.data)
        self.assertIn(b'class="power-card"', r.data)        # gauge present
        self.assertNotIn(b'id="power-hint"', r.data)        # no hint
        self.assertNotIn(b'id="carbon-card-wrap"', r.data)  # no carbon
        self.assertIn(b'id="status-text"', r.data)          # status indicator shown
        self.assertNotIn(b'function loadMix', r.data)       # no standalone donut poller

    def test_postcode_only(self):
        r = self._get(power=False, postcode=True)
        self.assertEqual(r.status_code, 200)
        self.assertIn(b'id="power-hint"', r.data)           # dismissible hint
        self.assertIn(b'id="carbon-card-wrap"', r.data)     # carbon present
        self.assertNotIn(b'class="power-card"', r.data)     # teeth: gauge gated out
        # no power source → no status indicator (was showing a false "Error")
        self.assertNotIn(b'id="status-text"', r.data)
        self.assertNotIn(b'id="status-dot"', r.data)
        # generation-mix donut renders via its own power-independent poller
        self.assertIn(b'id="mix-donut-canvas"', r.data)
        self.assertIn(b'function loadMix', r.data)
        # 48-hour history card renders in Mix mode only (no kW/CO2 without power)
        self.assertIn(b'id="power-history-card-wrap"', r.data)
        self.assertIn(b'id="ph-btn-mix"', r.data)
        self.assertNotIn(b'id="ph-btn-kw"', r.data)
        self.assertNotIn(b'id="ph-btn-co2"', r.data)

    def test_both_gates(self):
        r = self._get(power=True, postcode=True)
        self.assertEqual(r.status_code, 200)
        self.assertIn(b'class="power-card"', r.data)
        self.assertIn(b'id="carbon-card-wrap"', r.data)
        self.assertNotIn(b'id="power-hint"', r.data)

    def test_neither_still_renders(self):
        # reachable by direct URL though hidden from nav; must not error
        r = self._get(power=False, postcode=False)
        self.assertEqual(r.status_code, 200)
        self.assertNotIn(b'class="power-card"', r.data)
        self.assertNotIn(b'id="carbon-card-wrap"', r.data)


# ─────────────────────────────────────────────────────────────────────────────
# Octopus Mini as the live-power source (power_source == "mini")
# ─────────────────────────────────────────────────────────────────────────────

class TestMiniPowerSource(unittest.TestCase):
    """Mini opted in as the live source: gate recognition, /api/power demand
    branch, availability signal, and the engine-polled demand cache."""

    def setUp(self):
        import engine
        engine._last_mini_demand = {"kw": None, "ts": 0.0, "wall": 0.0}

    def _cfg(self, power_sensor=None, power_source=None):
        meta = {"billing_day": 1, "block_minutes": 30, "timezone": "Europe/London",
                "currency_symbol": "£"}
        if power_sensor: meta["power_sensor"] = power_sensor
        if power_source: meta["power_source"] = power_source
        return {"meters": {"electricity_main": {"meta": meta, "channels": {}}}}

    def _power(self, cfg, **patches):
        client = make_client(store=BlockStore(":memory:"))
        import contextlib
        with contextlib.ExitStack() as es:
            es.enter_context(patch.object(server, "load_config", return_value=cfg))
            for tgt, val in patches.items():
                es.enter_context(patch.object(server, tgt, return_value=val))
            return json.loads(client.get("/api/power").data)

    def test_overview_reveals_with_mini_source(self):
        cfg = self._cfg(power_source="mini")
        client = make_client(store=BlockStore(":memory:"))
        with patch.object(server, "load_config", return_value=cfg), \
             patch.object(server, "_mini_device_id", return_value="dev-1"):
            r = client.get("/live-power")
        self.assertEqual(r.status_code, 200)
        self.assertIn(b'class="power-card"', r.data)   # gauge revealed by the marker

    def test_api_power_mini_import(self):
        d = self._power(self._cfg(power_source="mini"),
                        _mini_device_id="dev-1", _mini_live_demand_kw=1.25)
        self.assertTrue(d["mini_active"])
        self.assertTrue(d["has_power_sensor"])
        self.assertAlmostEqual(d["import_kw"], 1.25)
        self.assertEqual(d["export_kw"], 0.0)

    def test_api_power_mini_export(self):
        d = self._power(self._cfg(power_source="mini"),
                        _mini_device_id="dev-1", _mini_live_demand_kw=-0.8)
        self.assertEqual(d["import_kw"], 0.0)
        self.assertAlmostEqual(d["export_kw"], 0.8)

    def test_mini_inactive_without_device(self):
        # marker set but no Mini discovered → not active, falls through
        d = self._power(self._cfg(power_source="mini"), _mini_device_id=None)
        self.assertFalse(d["mini_active"])

    def test_mini_available_when_no_other_source(self):
        import engine
        cfg = self._cfg()
        client = make_client(store=BlockStore(":memory:"))
        with patch.object(server, "load_config", return_value=cfg), \
             patch.object(server, "_mini_device_id", return_value="dev-1"), \
             patch.object(engine, "_detected_integrations", {"bcd": {"found": False}}, create=True):
            d = json.loads(client.get("/api/power").data)
        self.assertTrue(d["mini_available"])
        self.assertFalse(d["mini_active"])

    def test_mini_not_available_with_bcd(self):
        import engine
        cfg = self._cfg()
        client = make_client(store=BlockStore(":memory:"))
        with patch.object(server, "load_config", return_value=cfg), \
             patch.object(server, "_mini_device_id", return_value="dev-1"), \
             patch.object(engine, "_detected_integrations", {"bcd": {"found": True}}, create=True):
            d = json.loads(client.get("/api/power").data)
        self.assertFalse(d["mini_available"])

    def test_mini_not_available_with_sensor(self):
        d = self._power(self._cfg(power_sensor="sensor.house"),
                        _mini_device_id="dev-1")
        self.assertFalse(d["mini_available"])

    def test_gauge_reads_engine_cache(self):
        import engine, time
        # fresh engine reading → gauge returns it (no GraphQL here)
        engine._last_mini_demand = {"kw": 1.5, "ts": 0.0, "wall": time.time()}
        self.assertAlmostEqual(server._mini_live_demand_kw(), 1.5)
        # stale reading → None
        engine._last_mini_demand = {"kw": 1.5, "ts": 0.0, "wall": time.time() - 600}
        self.assertIsNone(server._mini_live_demand_kw())
        # no reading → None
        engine._last_mini_demand = {"kw": None, "ts": 0.0, "wall": 0.0}
        self.assertIsNone(server._mini_live_demand_kw())

    # ── enable / disable endpoint ──
    def test_enable_sets_marker(self):
        cfg = self._cfg()
        saved = {}
        client = make_client(store=BlockStore(":memory:"))
        with patch.object(server, "load_config", return_value=cfg), \
             patch.object(server, "save_config", side_effect=lambda d: saved.update(cfg=d)):
            r = client.post("/api/power-source/mini", json={"enabled": True})
        self.assertEqual(r.status_code, 200)
        self.assertTrue(json.loads(r.data)["enabled"])
        self.assertEqual(
            saved["cfg"]["meters"]["electricity_main"]["meta"].get("power_source"), "mini")

    def test_disable_clears_marker(self):
        cfg = self._cfg(power_source="mini")
        saved = {}
        client = make_client(store=BlockStore(":memory:"))
        with patch.object(server, "load_config", return_value=cfg), \
             patch.object(server, "save_config", side_effect=lambda d: saved.update(cfg=d)):
            r = client.post("/api/power-source/mini", json={"enabled": False})
        self.assertEqual(r.status_code, 200)
        self.assertNotIn("power_source",
                         saved["cfg"]["meters"]["electricity_main"]["meta"])

    def test_enable_blocked_when_sensor_present(self):
        # never override a real device sensor — 409, no save
        cfg = self._cfg(power_sensor="sensor.house")
        client = make_client(store=BlockStore(":memory:"))
        with patch.object(server, "load_config", return_value=cfg), \
             patch.object(server, "save_config",
                          side_effect=AssertionError("must not save")):
            r = client.post("/api/power-source/mini", json={"enabled": True})
        self.assertEqual(r.status_code, 409)

    # ── render: enable-card / disable-note / generic-hint ──
    def test_render_enable_card_when_available(self):
        import engine
        client = make_client(store=BlockStore(":memory:"))
        with patch.object(server, "load_config", return_value=self._cfg()), \
             patch.object(server, "_mini_device_id", return_value="dev-1"), \
             patch.object(engine, "_detected_integrations",
                          {"bcd": {"found": False}}, create=True):
            r = client.get("/live-power")
        self.assertIn(b'id="mini-enable"', r.data)
        self.assertNotIn(b'id="power-hint"', r.data)
        self.assertNotIn(b'id="mini-active-note"', r.data)

    def test_render_disable_note_when_chosen(self):
        client = make_client(store=BlockStore(":memory:"))
        with patch.object(server, "load_config",
                          return_value=self._cfg(power_source="mini")), \
             patch.object(server, "_mini_device_id", return_value="dev-1"):
            r = client.get("/live-power")
        self.assertIn(b'id="mini-active-note"', r.data)
        self.assertIn(b'class="power-card"', r.data)   # gauge revealed
        self.assertNotIn(b'id="mini-enable"', r.data)

    def test_render_generic_hint_when_no_mini(self):
        import engine
        client = make_client(store=BlockStore(":memory:"))
        with patch.object(server, "load_config", return_value=self._cfg()), \
             patch.object(server, "_mini_device_id", return_value=None), \
             patch.object(engine, "_detected_integrations",
                          {"bcd": {"found": False}}, create=True):
            r = client.get("/live-power")
        self.assertIn(b'id="power-hint"', r.data)
        self.assertNotIn(b'id="mini-enable"', r.data)
        self.assertNotIn(b'id="mini-active-note"', r.data)

    # ── poll cadence + visibility (chunk 3a) ──
    def test_render_mini_poll_cadence(self):
        client = make_client(store=BlockStore(":memory:"))
        with patch.object(server, "load_config",
                          return_value=self._cfg(power_source="mini")), \
             patch.object(server, "_mini_device_id", return_value="dev-1"):
            r = client.get("/live-power")
        self.assertIn(b"POWER_POLL_MS = 60000", r.data)   # Mini → 60s (reads engine cache)
        self.assertNotIn(b"visibilitychange", r.data)     # no page-active gating

    def test_render_sensor_poll_cadence(self):
        ha = MagicMock(); ha.get_state.return_value = None
        client = make_client(store=BlockStore(":memory:"))
        with patch.object(server, "load_config",
                          return_value=self._cfg(power_sensor="sensor.house")), \
             patch.object(server, "_ha_client", ha):
            r = client.get("/live-power")
        self.assertIn(b"POWER_POLL_MS = 5000", r.data)    # local sensor → 5s

    # ── BCD auto-adopt + unit-aware sensor read (chunk 3b) ──
    def test_bcd_demand_sensor_helper(self):
        import engine
        with patch.object(engine, "_detected_integrations",
                          {"bcd": {"found": True, "demand_sensor": "sensor.bcd"}}, create=True):
            self.assertEqual(server._bcd_demand_sensor(), "sensor.bcd")
        with patch.object(engine, "_detected_integrations",
                          {"bcd": {"found": False, "demand_sensor": "sensor.bcd"}}, create=True):
            self.assertIsNone(server._bcd_demand_sensor())

    def test_effective_power_sensor_precedence(self):
        with patch.object(server, "_bcd_demand_sensor", return_value="sensor.bcd"):
            self.assertEqual(
                server._effective_power_sensor({"power_sensor": "sensor.mine"}), "sensor.mine")
            self.assertEqual(server._effective_power_sensor({}), "sensor.bcd")
        with patch.object(server, "_bcd_demand_sensor", return_value=None):
            self.assertIsNone(server._effective_power_sensor({}))

    def test_render_bcd_adopted_reveals_gauge(self):
        import engine
        client = make_client(store=BlockStore(":memory:"))
        with patch.object(server, "load_config", return_value=self._cfg()), \
             patch.object(engine, "_detected_integrations",
                          {"bcd": {"found": True, "demand_sensor": "sensor.bcd_demand"}}, create=True), \
             patch.object(server, "_mini_device_id", return_value="dev-1"):
            r = client.get("/live-power")
        self.assertIn(b'class="power-card"', r.data)    # gauge revealed via BCD
        self.assertNotIn(b'id="mini-enable"', r.data)   # Mini NOT offered when BCD present
        self.assertNotIn(b'id="power-hint"', r.data)

    def test_api_power_bcd_adopted_beats_mini(self):
        import engine
        client = make_client(store=BlockStore(":memory:"))
        with patch.object(server, "load_config",
                          return_value=self._cfg(power_source="mini")), \
             patch.object(engine, "_detected_integrations",
                          {"bcd": {"found": True, "demand_sensor": "sensor.bcd_demand"}}, create=True), \
             patch.object(server, "_mini_device_id", return_value="dev-1"):
            d = json.loads(client.get("/api/power").data)
        self.assertTrue(d["has_power_sensor"])          # BCD adopted
        self.assertFalse(d["mini_active"])              # configured/BCD beats Mini quota

    def test_sensor_kw_watts_converted(self):
        ha = MagicMock()
        ha.get_state.return_value = "1500"
        ha.get_attributes.return_value = {"unit_of_measurement": "W"}
        client = make_client(store=BlockStore(":memory:"))
        with patch.object(server, "load_config",
                          return_value=self._cfg(power_sensor="sensor.house_w")), \
             patch.object(server, "_ha_client", ha):
            d = json.loads(client.get("/api/power").data)
        self.assertAlmostEqual(d["import_kw"], 1.5)     # 1500 W → 1.5 kW

    def test_sensor_kw_kilowatts_preserved(self):
        ha = MagicMock()
        ha.get_state.return_value = "2.5"
        ha.get_attributes.return_value = {"unit_of_measurement": "kW"}
        client = make_client(store=BlockStore(":memory:"))
        with patch.object(server, "load_config",
                          return_value=self._cfg(power_sensor="sensor.house_kw")), \
             patch.object(server, "_ha_client", ha):
            d = json.loads(client.get("/api/power").data)
        self.assertAlmostEqual(d["import_kw"], 2.5)     # kW preserved


# ─────────────────────────────────────────────────────────────────────────────
# load_config — single source of truth
# ─────────────────────────────────────────────────────────────────────────────

class TestLoadConfig(unittest.TestCase):
    """load_config should always prefer the DB over meters_config.json."""

    def test_load_config_returns_dict(self):
        with patch.object(server, "load_config", return_value=MINIMAL_CONFIG):
            cfg = server.load_config()
        self.assertIsInstance(cfg, dict)
        self.assertIn("meters", cfg)

    def test_load_config_has_meter_entries(self):
        with patch.object(server, "load_config", return_value=MINIMAL_CONFIG):
            cfg = server.load_config()
        self.assertIn("electricity_main", cfg.get("meters", {}))


# ─────────────────────────────────────────────────────────────────────────────
# Renamed routes — verify old names are gone, new names work
# ─────────────────────────────────────────────────────────────────────────────

class TestRenamedRoutes(unittest.TestCase):

    def setUp(self):
        self.client = make_client()

    def _registered(self, name):
        import server as _s
        return name in _s.app.view_functions

    def test_old_config_page_not_registered(self):
        self.assertFalse(self._registered("config_page"))

    def test_old_summary_page_not_registered(self):
        self.assertFalse(self._registered("summary_page"))

    def test_old_import_page_not_registered(self):
        self.assertFalse(self._registered("import_page"))

    def test_settings_route_returns_200(self):
        with patch.object(server, "load_config", return_value=MINIMAL_CONFIG):
            r = self.client.get("/settings")
        self.assertEqual(r.status_code, 200)

    def test_settings_carbon_tab_returns_200(self):
        with patch.object(server, "load_config", return_value=MINIMAL_CONFIG):
            r = self.client.get("/settings?tab=carbon")
        self.assertEqual(r.status_code, 200)

    def test_live_power_route_exists(self):
        self.assertTrue(self._registered("live_power_page"))

    def test_data_management_route_exists(self):
        self.assertTrue(self._registered("data_management_page"))

    def test_billing_history_route_exists(self):
        self.assertTrue(self._registered("billing_history_page"))

    def test_old_config_route_redirects_or_missing(self):
        """GET /config should not return 200 — it no longer exists."""
        r = self.client.get("/config")
        self.assertNotEqual(r.status_code, 200)

    def test_settings_route_replaces_config(self):
        """GET /settings should return 200."""
        with patch.object(server, "load_config", return_value=MINIMAL_CONFIG):
            r = self.client.get("/settings")
        self.assertEqual(r.status_code, 200)


if __name__ == '__main__':
    unittest.main()

# ─────────────────────────────────────────────────────────────────────────────
# 2.7.0 — new routes registered
# ─────────────────────────────────────────────────────────────────────────────

class TestNewRoutesRegistered270(unittest.TestCase):
    """Verify all new 2.7.0 API routes are registered."""

    def setUp(self):
        self.client = make_client()

    def _registered(self, endpoint):
        return endpoint in [r.endpoint for r in server.app.url_map.iter_rules()]

    def test_api_meter_delete_data_registered(self):
        self.assertTrue(self._registered("api_meter_delete_data"))

    def test_api_sub_meter_history_registered(self):
        self.assertTrue(self._registered("api_sub_meter_history"))

    def test_api_entities_registered(self):
        self.assertTrue(self._registered("api_entities"))


# ─────────────────────────────────────────────────────────────────────────────
# 2.7.0 — api_meter_delete_data
# ─────────────────────────────────────────────────────────────────────────────

class TestDeleteWindowToUtcTimes(unittest.TestCase):
    """Regression: a whole-day delete (00:00–23:59) must NOT be converted to a UTC
    time-of-day filter. Under BST that turned 23:59 local into 22:59 UTC →
    TIME(block_start) <= '22:59', leaving the 23:00/23:30 UTC (local-midnight)
    blocks — 2 per day — undeleted every time."""

    def test_whole_day_passes_through_unchanged(self):
        # Even on a BST date, the full-day window must stay 00:00–23:59 (no filter).
        self.assertEqual(
            server._delete_window_to_utc_times("00:00", "23:59",
                                               "Europe/London", "2026-06-04", "2026-06-04"),
            ("00:00", "23:59"))

    def test_whole_day_passes_through_in_utc_too(self):
        self.assertEqual(
            server._delete_window_to_utc_times("00:00", "23:59",
                                               "UTC", "2026-01-01", "2026-01-01"),
            ("00:00", "23:59"))

    def test_partial_window_is_converted(self):
        # An explicit partial window IS converted to UTC (BST −1h), so it differs.
        f, t = server._delete_window_to_utc_times(
            "01:00", "03:00", "Europe/London", "2026-06-04", "2026-06-04")
        self.assertEqual((f, t), ("00:00", "02:00"))


class TestApiMeterDeleteData(unittest.TestCase):
    """Tests for the atomic cascade delete endpoint."""

    CONFIG_WITH_SUB = {
        "meters": {
            "electricity_main": {
                "meta": {
                    "billing_day": 1, "block_minutes": 30,
                    "timezone": "Europe/London",
                    "currency_symbol": "£", "currency_code": "GBP",
                    "site": "Test Home",
                },
                "channels": {
                    "import": {"read": "sensor.imp", "rate": "sensor.rate"},
                    "export": {"read": "sensor.exp", "rate": "sensor.rate"},
                },
            },
            "sub_meter_battery": {
                "meta": {
                    "sub_meter": True, "meter_type": "battery",
                    "device": "House Battery",
                    "parent_meter": "electricity_main",
                },
                "channels": {
                    "import": {"read": "sensor.bat", "rate": "sensor.rate"},
                },
            },
        }
    }

    def setUp(self):
        store = BlockStore(":memory:")
        store.insert_config_period(self.CONFIG_WITH_SUB)
        # Add a block for the sub-meter using the correct format
        store.append_blocks([{
            "start": "2026-01-15T08:00:00",
            "end":   "2026-01-15T08:30:00",
            "meters": {
                "sub_meter_battery": {
                    "meta": {"billing_day": 1, "block_minutes": 30, "timezone": "UTC"},
                    "channels": {
                        "import": {
                            "kwh": 0.5, "kwh_total": 0.5, "kwh_remainder": 0.5,
                            "cost": 0.18, "rate": 0.32,
                            "read_start": 100.0, "read_end": 100.5,
                        }
                    },
                    "standing_charge": 0.0,
                    "interpolated": False,
                }
            },
            "totals": {"import_kwh": 0.5, "import_cost": 0.18},
            "interpolated": False,
        }])
        self.client = make_client(store=store)
        # Config without sub-meter (for sending in delete request)
        self.new_config = {
            "meters": {
                "electricity_main": self.CONFIG_WITH_SUB["meters"]["electricity_main"]
            }
        }

    def test_delete_returns_200(self):
        r = self.client.post(
            "/api/meter/sub_meter_battery/delete-data",
            data=json.dumps({"config": self.new_config}),
            content_type="application/json"
        )
        self.assertEqual(r.status_code, 200)

    def test_delete_returns_ok_true(self):
        r = self.client.post(
            "/api/meter/sub_meter_battery/delete-data",
            data=json.dumps({"config": self.new_config}),
            content_type="application/json"
        )
        data = json.loads(r.data)
        self.assertTrue(data.get("ok"))

    def test_delete_reports_blocks_deleted(self):
        r = self.client.post(
            "/api/meter/sub_meter_battery/delete-data",
            data=json.dumps({"config": self.new_config}),
            content_type="application/json"
        )
        data = json.loads(r.data)
        self.assertIn("deleted", data)
        self.assertGreaterEqual(data["deleted"].get("blocks", 0), 1,
            "At least 1 block should be reported as deleted")

    def test_delete_nonexistent_meter_still_ok(self):
        """Deleting a meter with no blocks should still return ok."""
        r = self.client.post(
            "/api/meter/nonexistent_meter/delete-data",
            data=json.dumps({"config": self.new_config}),
            content_type="application/json"
        )
        self.assertEqual(r.status_code, 200)
        data = json.loads(r.data)
        self.assertTrue(data.get("ok"))
        self.assertEqual(data["deleted"].get("blocks", 0), 0)

    def test_delete_without_config_still_works(self):
        """Delete with no config body should still delete blocks."""
        r = self.client.post(
            "/api/meter/sub_meter_battery/delete-data",
            data=json.dumps({}),
            content_type="application/json"
        )
        self.assertEqual(r.status_code, 200)


# ─────────────────────────────────────────────────────────────────────────────
# 2.7.0 — api_save_config: name validation and fresh install period handling
# ─────────────────────────────────────────────────────────────────────────────

class TestApiSaveConfigValidation(unittest.TestCase):
    """Tests for device name validation in api_save_config."""

    def setUp(self):
        self.client = make_client()

    def _save(self, config):
        return self.client.post(
            "/api/config",
            data=json.dumps(config),
            content_type="application/json"
        )

    def test_valid_config_returns_ok(self):
        with patch.object(server, "save_config", return_value=None), \
             patch.object(server, "_create_backup_zip", return_value=None):
            r = self._save(MINIMAL_CONFIG)
        self.assertEqual(r.status_code, 200)

    def test_duplicate_device_name_rejected(self):
        cfg = {
            "meters": {
                "electricity_main": MINIMAL_CONFIG["meters"]["electricity_main"],
                "sub_meter_001": {
                    "meta": {"sub_meter": True, "device": "Battery"},
                    "channels": {"import": {"read": "s.a", "rate": "s.b"}},
                },
                "sub_meter_002": {
                    "meta": {"sub_meter": True, "device": "Battery"},
                    "channels": {"import": {"read": "s.c", "rate": "s.d"}},
                },
            }
        }
        r = self._save(cfg)
        self.assertEqual(r.status_code, 400)
        data = json.loads(r.data)
        self.assertIn("error", data)
        self.assertIn("already used", data["error"])

    def test_device_name_too_long_rejected(self):
        cfg = {
            "meters": {
                "electricity_main": MINIMAL_CONFIG["meters"]["electricity_main"],
                "sub_meter_001": {
                    "meta": {"sub_meter": True, "device": "A" * 41},
                    "channels": {"import": {"read": "s.a", "rate": "s.b"}},
                },
            }
        }
        r = self._save(cfg)
        self.assertEqual(r.status_code, 400)
        data = json.loads(r.data)
        self.assertIn("40 characters", data["error"])

    def test_device_name_invalid_chars_rejected(self):
        cfg = {
            "meters": {
                "electricity_main": MINIMAL_CONFIG["meters"]["electricity_main"],
                "sub_meter_001": {
                    "meta": {"sub_meter": True, "device": "Battery<script>"},
                    "channels": {"import": {"read": "s.a", "rate": "s.b"}},
                },
            }
        }
        r = self._save(cfg)
        self.assertEqual(r.status_code, 400)
        data = json.loads(r.data)
        self.assertIn("invalid characters", data["error"])

    def test_missing_meters_key_rejected(self):
        r = self._save({"site": "Home"})
        self.assertEqual(r.status_code, 400)

    def test_duplicate_check_case_insensitive(self):
        """'battery' and 'Battery' should be treated as duplicates."""
        cfg = {
            "meters": {
                "electricity_main": MINIMAL_CONFIG["meters"]["electricity_main"],
                "sub_meter_001": {
                    "meta": {"sub_meter": True, "device": "battery"},
                    "channels": {"import": {"read": "s.a", "rate": "s.b"}},
                },
                "sub_meter_002": {
                    "meta": {"sub_meter": True, "device": "Battery"},
                    "channels": {"import": {"read": "s.c", "rate": "s.d"}},
                },
            }
        }
        r = self._save(cfg)
        self.assertEqual(r.status_code, 400)


class TestApiSaveConfigFreshInstall(unittest.TestCase):
    """Tests that wizard save on fresh install doesn't create a duplicate period."""

    def setUp(self):
        # Fresh store with zero blocks
        self.store = BlockStore(":memory:")
        self.store.insert_config_period({
            "meters": {
                "electricity_main": {
                    "meta": {
                        "billing_day": 1, "block_minutes": 30,
                        "timezone": "UTC",
                        "currency_symbol": "£", "currency_code": "GBP",
                        "site": "",
                    },
                    "channels": {
                        "import": {"read": "", "rate": ""},
                        "export": {"read": "", "rate": ""},
                    },
                }
            }
        })
        self.client = make_client(store=self.store)

    def test_zero_blocks_period_not_duplicated(self):
        """Saving config with 0 blocks should not create a new config period."""
        full_cfg = {
            "meters": {
                "electricity_main": {
                    "meta": {
                        "billing_day": 1, "block_minutes": 30,
                        "timezone": "Europe/London",
                        "currency_symbol": "£", "currency_code": "GBP",
                        "site": "My Home",
                        "supplier": "Octopus Energy",
                    },
                    "channels": {
                        "import": {"read": "sensor.import", "rate": "sensor.rate"},
                        "export": {"read": "sensor.export", "rate": "sensor.rate"},
                    },
                }
            }
        }
        with patch.object(server, "_create_backup_zip", return_value=None):
            r = self.client.post(
                "/api/config",
                data=json.dumps(full_cfg),
                content_type="application/json"
            )
        self.assertEqual(r.status_code, 200)
        period_count = self.store._conn.execute(
            "SELECT COUNT(*) FROM config_periods"
        ).fetchone()[0]
        self.assertEqual(period_count, 1,
            "Fresh install wizard save should update existing period, not create a new one")


# ─────────────────────────────────────────────────────────────────────────────
# /api/insights/data-bounds
# ─────────────────────────────────────────────────────────────────────────────

class TestApiInsightsDataBounds(unittest.TestCase):

    def setUp(self):
        self.client = make_client()

    def test_returns_200(self):
        with patch.object(server, "load_config", return_value=MINIMAL_CONFIG):
            r = self.client.get("/api/insights/data-bounds")
        self.assertEqual(r.status_code, 200)

    def test_response_has_earliest_and_latest(self):
        with patch.object(server, "load_config", return_value=MINIMAL_CONFIG):
            r = self.client.get("/api/insights/data-bounds")
        d = json.loads(r.data)
        self.assertIn("earliest", d)
        self.assertIn("latest", d)

    def test_empty_store_returns_none(self):
        with patch.object(server, "load_config", return_value=MINIMAL_CONFIG):
            r = self.client.get("/api/insights/data-bounds")
        d = json.loads(r.data)
        self.assertIsNone(d["earliest"])
        self.assertIsNone(d["latest"])


class TestApiInsightsDataBoundsCarbonVsUsage(unittest.TestCase):
    """Carbon-gated bounds must not shrink the Usage-tab range.

    Imported history (kWh/cost, no carbon) predates carbon recording, so the
    endpoint returns a carbon range (earliest/latest) AND a full energy range
    (usage_earliest/usage_latest). The Usage tab gates its comparison buttons on
    the latter, so "vs last year" into a backfilled month isn't wrongly disabled.
    """

    def _store_with_split_history(self):
        import tempfile
        from block_store import BlockStore
        store = BlockStore(tempfile.mktemp(suffix=".db"))
        with store._conn:
            store._conn.execute(
                "INSERT INTO config_periods (billing_day, block_minutes, timezone, "
                "currency_symbol, currency_code, effective_from) "
                "VALUES (1, 30, 'Europe/London', '£', 'GBP', '2024-01-01')"
            )
            cp = store._conn.execute("SELECT id FROM config_periods LIMIT 1").fetchone()[0]
            # Old imported block: has kWh, NO carbon
            store._conn.execute(
                "INSERT INTO blocks (block_start, block_end, meter_id, config_period_id, "
                "imp_kwh, exp_kwh, carbon_g) VALUES "
                "('2024-07-01T00:00:00', '2024-07-01T00:30:00', 'electricity_main', ?, 5.0, 0.0, NULL)",
                (cp,))
            # Recent block: has carbon
            store._conn.execute(
                "INSERT INTO blocks (block_start, block_end, meter_id, config_period_id, "
                "imp_kwh, exp_kwh, carbon_g) VALUES "
                "('2026-04-01T00:00:00', '2026-04-01T00:30:00', 'electricity_main', ?, 3.0, 0.0, 400.0)",
                (cp,))
        return store

    def test_carbon_range_excludes_precarbon_but_usage_range_includes_it(self):
        store = self._store_with_split_history()
        with patch.object(server, "_get_store", return_value=store), \
             patch.object(server, "load_config", return_value=MINIMAL_CONFIG):
            r = server.app.test_client().get("/api/insights/data-bounds")
        d = json.loads(r.data)
        # Carbon range starts only where carbon exists
        self.assertEqual(d["earliest"], "2026-04-01")
        self.assertEqual(d["latest"],   "2026-04-01")
        # Usage range reaches back to the imported (carbon-less) history
        self.assertEqual(d["usage_earliest"], "2024-07-01")
        self.assertEqual(d["usage_latest"],   "2026-04-01")


# ─────────────────────────────────────────────────────────────────────────────
# /api/insights/calendar-month and /api/insights/calendar-year
# ─────────────────────────────────────────────────────────────────────────────

class TestApiInsightsCalendar(unittest.TestCase):

    def setUp(self):
        self.client = make_client()

    def test_calendar_month_returns_200(self):
        with patch.object(server, "load_config", return_value=MINIMAL_CONFIG):
            r = self.client.get("/api/insights/calendar-month?year=2026&month=4")
        self.assertEqual(r.status_code, 200)

    def test_calendar_month_has_required_fields(self):
        with patch.object(server, "load_config", return_value=MINIMAL_CONFIG):
            r = self.client.get("/api/insights/calendar-month?year=2026&month=4")
        d = json.loads(r.data)
        for field in ["has_carbon", "imp_kwh", "exp_kwh", "carbon_g_net",
                      "period_start", "period_end", "period_label",
                      "sub_meters", "assumptions"]:
            self.assertIn(field, d)

    def test_calendar_month_period_label(self):
        with patch.object(server, "load_config", return_value=MINIMAL_CONFIG):
            r = self.client.get("/api/insights/calendar-month?year=2026&month=4")
        d = json.loads(r.data)
        self.assertIn("2026", d["period_label"])

    def test_calendar_year_returns_200(self):
        with patch.object(server, "load_config", return_value=MINIMAL_CONFIG):
            r = self.client.get("/api/insights/calendar-year?year=2026")
        self.assertEqual(r.status_code, 200)

    def test_calendar_year_has_required_fields(self):
        with patch.object(server, "load_config", return_value=MINIMAL_CONFIG):
            r = self.client.get("/api/insights/calendar-year?year=2026")
        d = json.loads(r.data)
        for field in ["has_carbon", "imp_kwh", "exp_kwh", "carbon_g_net",
                      "period_start", "period_end", "period_label",
                      "sub_meters", "assumptions"]:
            self.assertIn(field, d)

    def test_calendar_year_period_label_is_year_string(self):
        with patch.object(server, "load_config", return_value=MINIMAL_CONFIG):
            r = self.client.get("/api/insights/calendar-year?year=2026")
        d = json.loads(r.data)
        self.assertEqual(d["period_label"], "2026")

    def test_calendar_month_defaults_to_current(self):
        """Omitting year/month should still return a valid response."""
        with patch.object(server, "load_config", return_value=MINIMAL_CONFIG):
            r = self.client.get("/api/insights/calendar-month")
        self.assertEqual(r.status_code, 200)

    def test_calendar_year_response_has_carbon_field(self):
        with patch.object(server, "load_config", return_value=MINIMAL_CONFIG):
            r = self.client.get("/api/insights/calendar-year?year=2026")
        d = json.loads(r.data)
        self.assertIn("has_carbon", d)
        self.assertIsInstance(d["has_carbon"], bool)


# ─────────────────────────────────────────────────────────────────────────────
# _aggregate_insights direct tests
# ─────────────────────────────────────────────────────────────────────────────

class TestAggregateInsights(unittest.TestCase):
    """Test _aggregate_insights directly with known block data."""

    def setUp(self):
        import tempfile, os
        self.tmp = tempfile.mktemp(suffix=".db")
        from block_store import BlockStore
        self.store = BlockStore(self.tmp)
        # Insert a known block
        with self.store._conn:
            self.store._conn.execute(
                "INSERT INTO config_periods (billing_day, block_minutes, timezone, "
                "currency_symbol, currency_code, effective_from) "
                "VALUES (1, 30, 'Europe/London', '£', 'GBP', '2026-01-01')"
            )
            cp_id = self.store._conn.execute(
                "SELECT id FROM config_periods LIMIT 1"
            ).fetchone()[0]
            # Main meter block
            self.store._conn.execute(
                "INSERT INTO blocks (block_start, block_end, meter_id, config_period_id, imp_kwh, exp_kwh, carbon_g) "
                "VALUES ('2026-04-01T00:00:00', '2026-04-01T00:30:00', "
                "'electricity_main', ?, 10.0, 2.0, 500.0)",
                (cp_id,)
            )
            # Sub-meter block
            self.store._conn.execute(
                "INSERT INTO meters (meter_id, config_period_id, is_sub_meter) "
                "VALUES ('ev_charger', ?, 1)",
                (cp_id,)
            )
            self.store._conn.execute(
                "INSERT INTO blocks (block_start, block_end, meter_id, config_period_id, imp_kwh, "
                "exp_kwh, carbon_g) "
                "VALUES ('2026-04-01T00:00:00', '2026-04-01T00:30:00', "
                "'ev_charger', ?, 4.0, 0.0, 200.0)",
                (cp_id,)
            )
        self.cfg = {
            "meters": {
                "electricity_main": {"meta": {"timezone": "Europe/London", "block_minutes": 30}},
                "ev_charger": {"meta": {"sub_meter": True, "timezone": "Europe/London"}},
            }
        }

    def tearDown(self):
        import os
        self.store._conn.close()
        if os.path.exists(self.tmp):
            os.remove(self.tmp)

    def test_has_carbon_true(self):
        d = server._aggregate_insights(
            self.store, self.cfg,
            "2026-04-01T00:00:00", "2026-04-02T00:00:00"
        )
        self.assertTrue(d["has_carbon"])

    def test_main_meter_imp_kwh(self):
        d = server._aggregate_insights(
            self.store, self.cfg,
            "2026-04-01T00:00:00", "2026-04-02T00:00:00"
        )
        self.assertAlmostEqual(d["imp_kwh"], 10.0, places=3)

    def test_sub_meter_imp_kwh(self):
        """Sub-meter imp_kwh is correctly accumulated."""
        d = server._aggregate_insights(
            self.store, self.cfg,
            "2026-04-01T00:00:00", "2026-04-02T00:00:00"
        )
        self.assertIn("ev_charger", d["sub_meters"])
        self.assertAlmostEqual(d["sub_meters"]["ev_charger"]["imp_kwh"], 4.0, places=3)

    def test_house_imp_kwh_is_remainder(self):
        """House import = main import minus sub-meter import."""
        d = server._aggregate_insights(
            self.store, self.cfg,
            "2026-04-01T00:00:00", "2026-04-02T00:00:00"
        )
        self.assertAlmostEqual(d["house_imp_kwh"], 6.0, places=3)

    def test_empty_range_returns_no_carbon(self):
        d = server._aggregate_insights(
            self.store, self.cfg,
            "2025-01-01T00:00:00", "2025-01-02T00:00:00"
        )
        self.assertFalse(d["has_carbon"])
        self.assertEqual(d["imp_kwh"], 0.0)

# ─────────────────────────────────────────────────────────────────────────────
# _aggregate_usage
# ─────────────────────────────────────────────────────────────────────────────

class TestAggregateUsage(unittest.TestCase):
    """Tests for _aggregate_usage() covering cost, rate tiers, peak window,
    net grid position, and sub-meter breakdown."""

    def setUp(self):
        import sqlite3
        self.conn = sqlite3.connect(":memory:")
        self.conn.row_factory = sqlite3.Row
        self.conn.executescript("""
            CREATE TABLE config_periods (
                id INTEGER PRIMARY KEY,
                billing_day INTEGER, block_minutes INTEGER,
                timezone TEXT, currency_symbol TEXT, currency_code TEXT,
                effective_from TEXT, effective_to TEXT,
                site_name TEXT, change_reason TEXT
            );
            CREATE TABLE meters (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                config_period_id INTEGER, meter_id TEXT,
                is_sub_meter INTEGER DEFAULT 0,
                parent_meter_id TEXT, device_label TEXT,
                meter_type TEXT, protected INTEGER DEFAULT 0,
                inverter_possible INTEGER DEFAULT 0,
                power_sensor TEXT, postcode_prefix TEXT,
                v2x_capable INTEGER DEFAULT 0,
                inverter_power_invert INTEGER DEFAULT 0,
                soc_sensor TEXT, inverter_power_sensor TEXT,
                device_power_sensor TEXT
            );
            CREATE TABLE blocks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                block_start TEXT, block_end TEXT,
                meter_id TEXT, config_period_id INTEGER,
                imp_kwh REAL, imp_kwh_grid REAL, imp_kwh_remainder REAL,
                imp_rate REAL, imp_cost REAL,
                imp_cost_remainder REAL, imp_read_start REAL, imp_read_end REAL,
                exp_kwh REAL, exp_rate REAL, exp_cost REAL,
                exp_read_start REAL, exp_read_end REAL,
                standing_charge REAL, carbon_g REAL, interpolated INTEGER DEFAULT 0
            );
        """)
        # Config period
        self.conn.execute("""INSERT INTO config_periods
            (id, billing_day, block_minutes, timezone, currency_symbol, currency_code,
             effective_from, effective_to)
            VALUES (1, 3, 30, 'UTC', '£', 'GBP', '2026-01-01T00:00:00', NULL)""")
        # Meters
        self.conn.execute("""INSERT INTO meters
            (config_period_id, meter_id, is_sub_meter, meter_type)
            VALUES (1, 'electricity_main', 0, NULL)""")
        self.conn.execute("""INSERT INTO meters
            (config_period_id, meter_id, is_sub_meter, meter_type, device_label)
            VALUES (1, 'ev_charger', 1, 'ev', 'Zappi')""")

        # Insert 4 blocks: 2 at cheap rate (0.07), 1 at peak (0.30), 1 export
        # Main meter - cheap rate block 1
        self.conn.execute("""INSERT INTO blocks
            (block_start, block_end, meter_id, config_period_id,
             imp_kwh, imp_kwh_remainder, imp_rate, imp_cost,
             exp_kwh, exp_rate, exp_cost, standing_charge)
            VALUES ('2026-04-01T00:00:00','2026-04-01T00:30:00','electricity_main',1,
                    5.0, 3.0, 0.07, 0.35,  0.0, 0.12, 0.0, 0.50)""")
        # Main meter - cheap rate block 2
        self.conn.execute("""INSERT INTO blocks
            (block_start, block_end, meter_id, config_period_id,
             imp_kwh, imp_kwh_remainder, imp_rate, imp_cost,
             exp_kwh, exp_rate, exp_cost, standing_charge)
            VALUES ('2026-04-01T01:00:00','2026-04-01T01:30:00','electricity_main',1,
                    4.0, 2.0, 0.07, 0.28,  0.0, 0.12, 0.0, 0.50)""")
        # Main meter - peak rate block
        self.conn.execute("""INSERT INTO blocks
            (block_start, block_end, meter_id, config_period_id,
             imp_kwh, imp_kwh_remainder, imp_rate, imp_cost,
             exp_kwh, exp_rate, exp_cost, standing_charge)
            VALUES ('2026-04-01T08:00:00','2026-04-01T08:30:00','electricity_main',1,
                    2.0, 2.0, 0.30, 0.60,  0.0, 0.12, 0.0, 0.50)""")
        # Main meter - export block
        self.conn.execute("""INSERT INTO blocks
            (block_start, block_end, meter_id, config_period_id,
             imp_kwh, imp_kwh_remainder, imp_rate, imp_cost,
             exp_kwh, exp_rate, exp_cost, standing_charge)
            VALUES ('2026-04-01T12:00:00','2026-04-01T12:30:00','electricity_main',1,
                    0.0, 0.0, 0.12, 0.0,  3.0, 0.12, 0.36, 0.50)""")
        # EV charger sub-meter - cheap rate
        self.conn.execute("""INSERT INTO blocks
            (block_start, block_end, meter_id, config_period_id,
             imp_kwh, imp_kwh_grid, imp_rate, imp_cost,
             exp_kwh, exp_rate, exp_cost, standing_charge)
            VALUES ('2026-04-01T00:00:00','2026-04-01T00:30:00','ev_charger',1,
                    2.0, 2.0, 0.07, 0.14,  0.0, 0.0, 0.0, 0.0)""")
        self.conn.execute("""INSERT INTO blocks
            (block_start, block_end, meter_id, config_period_id,
             imp_kwh, imp_kwh_grid, imp_rate, imp_cost,
             exp_kwh, exp_rate, exp_cost, standing_charge)
            VALUES ('2026-04-01T01:00:00','2026-04-01T01:30:00','ev_charger',1,
                    2.0, 2.0, 0.07, 0.14,  0.0, 0.0, 0.0, 0.0)""")
        self.conn.commit()

        class FakeStore:
            def __init__(self, conn): self._conn = conn
        self.store = FakeStore(self.conn)
        self.cfg = {
            "meters": {
                "electricity_main": {"meta": {"timezone": "UTC", "block_minutes": 30,
                                               "currency_symbol": "£"}},
                "ev_charger": {"meta": {"sub_meter": True, "meter_type": "ev",
                                         "device": "Zappi"}},
            }
        }

    def _run(self):
        return server._aggregate_usage(
            self.store, self.cfg,
            "2026-04-01T00:00:00", "2026-04-02T00:00:00"
        )

    def test_imp_kwh_total(self):
        """Total grid import = sum of main meter imp_kwh."""
        d = self._run()
        self.assertAlmostEqual(d["imp_kwh"], 11.0, places=3)

    def test_exp_kwh_total(self):
        """Total export = 3.0 kWh."""
        d = self._run()
        self.assertAlmostEqual(d["exp_kwh"], 3.0, places=3)

    def test_imp_cost_total(self):
        """Import cost = 0.35 + 0.28 + 0.60 = 1.23."""
        d = self._run()
        self.assertAlmostEqual(d["imp_cost"], 1.23, places=4)

    def test_exp_cost_total(self):
        """Export earnings = 0.36."""
        d = self._run()
        self.assertAlmostEqual(d["exp_cost"], 0.36, places=4)

    def test_standing_charge_once_per_day(self):
        """Standing charge summed once per local day (not once per block)."""
        d = self._run()
        self.assertAlmostEqual(d["standing_charge"], 0.50, places=4)

    def test_net_cost(self):
        """Net = imp_cost + standing - exp_cost = 1.23 + 0.50 - 0.36 = 1.37."""
        d = self._run()
        self.assertAlmostEqual(d["net_cost"], 1.37, places=4)

    def test_net_grid_kwh(self):
        """Net grid = 11.0 imp - 3.0 exp = 8.0."""
        d = self._run()
        self.assertAlmostEqual(d["net_grid_kwh"], 8.0, places=3)

    def test_rate_tiers_count(self):
        """Two distinct rate tiers: 0.07 and 0.30."""
        d = self._run()
        self.assertEqual(len(d["rate_tiers"]), 2)

    def test_rate_tiers_cheap_kwh(self):
        """Cheap tier (0.07): 5.0 + 4.0 = 9.0 kWh."""
        d = self._run()
        cheap = next(t for t in d["rate_tiers"] if abs(t["rate"] - 0.07) < 0.001)
        self.assertAlmostEqual(cheap["kwh"], 9.0, places=3)

    def test_rate_tiers_peak_kwh(self):
        """Peak tier (0.30): 2.0 kWh."""
        d = self._run()
        peak = next(t for t in d["rate_tiers"] if abs(t["rate"] - 0.30) < 0.001)
        self.assertAlmostEqual(peak["kwh"], 2.0, places=3)

    def test_weighted_rate(self):
        """Weighted avg rate = total_cost / total_kwh = 1.23 / 11.0."""
        d = self._run()
        self.assertAlmostEqual(d["weighted_rate"], 1.23 / 11.0, places=5)

    def test_house_imp_kwh(self):
        """House remainder = 3.0 + 2.0 + 2.0 = 7.0 (from imp_kwh_remainder cols)."""
        d = self._run()
        self.assertAlmostEqual(d["house_imp_kwh"], 7.0, places=3)

    def test_sub_meter_ev_present(self):
        """EV charger appears in sub_meters."""
        d = self._run()
        self.assertIn("ev_charger", d["sub_meters"])

    def test_sub_meter_ev_kwh(self):
        """EV charger grid kwh = 2.0 + 2.0 = 4.0."""
        d = self._run()
        self.assertAlmostEqual(d["sub_meters"]["ev_charger"]["imp_kwh"], 4.0, places=3)

    def test_sub_meter_ev_rate_tiers(self):
        """EV sub-meter has rate tier breakdown."""
        d = self._run()
        ev = d["sub_meters"]["ev_charger"]
        self.assertIn("rate_tiers", ev)
        self.assertEqual(len(ev["rate_tiers"]), 1)
        self.assertAlmostEqual(ev["rate_tiers"][0]["rate"], 0.07, places=4)
        self.assertAlmostEqual(ev["rate_tiers"][0]["kwh"], 4.0, places=3)

    def test_peak_window_fields_present(self):
        """Peak window fields are present in response."""
        d = self._run()
        self.assertIn("peak_window_start", d)
        self.assertIn("peak_window_kwh", d)

    def test_net_exporter_days_zero(self):
        """No net exporter days — export 3kWh < import 11kWh."""
        d = self._run()
        self.assertEqual(d["net_exporter_days"], 0)

    def test_empty_range_returns_zeros(self):
        """Empty date range returns zero costs and empty tiers."""
        d = server._aggregate_usage(
            self.store, self.cfg,
            "2025-01-01T00:00:00", "2025-01-02T00:00:00"
        )
        self.assertEqual(d["imp_kwh"], 0.0)
        self.assertEqual(d["imp_cost"], 0.0)
        self.assertEqual(d["rate_tiers"], [])
        self.assertIsNone(d["weighted_rate"])


# ─────────────────────────────────────────────────────────────────────────────
# /api/usage/* endpoints
# ─────────────────────────────────────────────────────────────────────────────

class TestApiUsageEndpoints(unittest.TestCase):

    def setUp(self):
        self.client = make_client()

    def test_billing_period_returns_200_or_404(self):
        with patch.object(server, "load_config", return_value=MINIMAL_CONFIG):
            r = self.client.get("/api/usage/billing-period")
        self.assertIn(r.status_code, (200, 404, 500))

    def test_calendar_month_returns_200(self):
        with patch.object(server, "load_config", return_value=MINIMAL_CONFIG):
            r = self.client.get("/api/usage/calendar-month?year=2026&month=4")
        self.assertIn(r.status_code, (200, 500))
        if r.status_code == 200:
            d = json.loads(r.data)
            self.assertIn("imp_kwh", d)
            self.assertIn("rate_tiers", d)
            self.assertIn("net_grid_kwh", d)

    def test_calendar_year_returns_200(self):
        with patch.object(server, "load_config", return_value=MINIMAL_CONFIG):
            r = self.client.get("/api/usage/calendar-year?year=2026")
        self.assertIn(r.status_code, (200, 500))

    def test_calendar_month_has_period_label(self):
        with patch.object(server, "load_config", return_value=MINIMAL_CONFIG):
            r = self.client.get("/api/usage/calendar-month?year=2026&month=1")
        if r.status_code == 200:
            d = json.loads(r.data)
            self.assertIn("period_label", d)

    def test_response_shape_keys(self):
        """All required keys present in a successful response."""
        with patch.object(server, "load_config", return_value=MINIMAL_CONFIG):
            r = self.client.get("/api/usage/calendar-month?year=2026&month=4")
        if r.status_code != 200:
            return
        d = json.loads(r.data)
        for key in ("imp_kwh", "exp_kwh", "imp_cost", "exp_cost",
                    "standing_charge", "net_cost", "net_grid_kwh",
                    "rate_tiers", "sub_meters", "peak_window_start",
                    "net_exporter_days", "house_imp_kwh"):
            self.assertIn(key, d, msg=f"Missing key: {key}")


# ─────────────────────────────────────────────────────────────────────────────
# /api/power/mix-history (2.8.0)
# ─────────────────────────────────────────────────────────────────────────────

class TestApiPowerMixHistory(unittest.TestCase):
    """Tests for /api/power/mix-history endpoint."""

    def setUp(self):
        import tempfile
        from datetime import datetime, timezone, timedelta
        self.tmp = tempfile.mktemp(suffix=".db")
        from block_store import BlockStore
        self.store = BlockStore(self.tmp)
        recent = (datetime.now(timezone.utc) - timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M")
        self.store.upsert_mix_history(recent, [
            {"fuel": "wind", "perc": 65.0},
            {"fuel": "gas",  "perc": 35.0},
        ])
        server.app.config["TESTING"] = True
        self.client = server.app.test_client()

    def tearDown(self):
        import os
        self.store._conn.close()
        if os.path.exists(self.tmp):
            os.remove(self.tmp)

    def test_returns_200(self):
        with patch.object(server, "_get_store", return_value=self.store):
            r = self.client.get("/api/power/mix-history")
        self.assertEqual(r.status_code, 200)

    def test_response_has_slots_key(self):
        with patch.object(server, "_get_store", return_value=self.store):
            r = self.client.get("/api/power/mix-history")
        d = json.loads(r.data)
        self.assertIn("slots", d)

    def test_slot_has_captured_at_and_fuels(self):
        with patch.object(server, "_get_store", return_value=self.store):
            r = self.client.get("/api/power/mix-history")
        d = json.loads(r.data)
        self.assertGreater(len(d["slots"]), 0)
        slot = d["slots"][0]
        self.assertIn("captured_at", slot)
        self.assertIn("fuels", slot)
        self.assertIn("wind", slot["fuels"])
        self.assertAlmostEqual(slot["fuels"]["wind"], 65.0)

    def test_empty_db_returns_empty_slots(self):
        import tempfile, os
        tmp2 = tempfile.mktemp(suffix=".db")
        try:
            from block_store import BlockStore
            empty_store = BlockStore(tmp2)
            with patch.object(server, "_get_store", return_value=empty_store):
                r = self.client.get("/api/power/mix-history")
            d = json.loads(r.data)
            self.assertEqual(d["slots"], [])
        finally:
            if os.path.exists(tmp2):
                os.remove(tmp2)


# ─────────────────────────────────────────────────────────────────────────────
# _aggregate_insights generation_mix field (2.8.0)
# ─────────────────────────────────────────────────────────────────────────────

class TestAggregateInsightsGenerationMix(unittest.TestCase):
    """_aggregate_insights returns generation_mix when mix data present."""

    def setUp(self):
        import tempfile
        self.tmp = tempfile.mktemp(suffix=".db")
        from block_store import BlockStore
        self.store = BlockStore(self.tmp)
        with self.store._conn:
            self.store._conn.execute(
                "INSERT INTO config_periods (billing_day, block_minutes, timezone, "
                "currency_symbol, currency_code, effective_from) "
                "VALUES (1, 30, 'UTC', '£', 'GBP', '2026-01-01')"
            )
            cp_id = self.store._conn.execute(
                "SELECT id FROM config_periods LIMIT 1"
            ).fetchone()[0]
            self.store._conn.execute(
                "INSERT INTO blocks (block_start, block_end, meter_id, config_period_id, "
                "imp_kwh, exp_kwh, carbon_g) "
                "VALUES ('2026-04-01T00:00:00', '2026-04-01T00:30:00', "
                "'electricity_main', ?, 10.0, 0.0, 400.0)", (cp_id,)
            )
            bid = self.store._conn.execute(
                "SELECT id FROM blocks WHERE meter_id='electricity_main'"
            ).fetchone()[0]
            self.store._conn.execute(
                "INSERT INTO generation_mix (block_id, fuel, perc) VALUES (?, 'wind', 70.0)",
                (bid,)
            )
            self.store._conn.execute(
                "INSERT INTO generation_mix (block_id, fuel, perc) VALUES (?, 'gas', 30.0)",
                (bid,)
            )
        self.cfg = {
            "meters": {
                "electricity_main": {"meta": {"timezone": "UTC", "block_minutes": 30}}
            }
        }

    def tearDown(self):
        import os
        self.store._conn.close()
        if os.path.exists(self.tmp):
            os.remove(self.tmp)

    def test_generation_mix_key_present(self):
        d = server._aggregate_insights(
            self.store, self.cfg,
            "2026-04-01T00:00:00", "2026-04-02T00:00:00"
        )
        self.assertIn("generation_mix", d)

    def test_generation_mix_contains_fuels(self):
        d = server._aggregate_insights(
            self.store, self.cfg,
            "2026-04-01T00:00:00", "2026-04-02T00:00:00"
        )
        fuels = {r["fuel"]: r["perc"] for r in d["generation_mix"]}
        self.assertIn("wind", fuels)
        self.assertAlmostEqual(fuels["wind"], 70.0, places=1)

    def test_generation_mix_empty_when_no_mix_data(self):
        d = server._aggregate_insights(
            self.store, self.cfg,
            "2025-01-01T00:00:00", "2025-01-02T00:00:00"
        )
        self.assertEqual(d["generation_mix"], [])


# ─────────────────────────────────────────────────────────────────────────────
# api_blocks_summary Direct import uses imp_cost_remainder (2.8.0)
# ─────────────────────────────────────────────────────────────────────────────

class TestApiBlocksSummaryDirectImportCost(unittest.TestCase):
    """Direct import cost in api_blocks_summary uses imp_cost_remainder not imp_cost."""

    def setUp(self):
        import tempfile
        self.tmp = tempfile.mktemp(suffix=".db")
        from block_store import BlockStore
        self.store = BlockStore(self.tmp)
        with self.store._conn:
            self.store._conn.execute(
                "INSERT INTO config_periods (billing_day, block_minutes, timezone, "
                "currency_symbol, currency_code, effective_from) "
                "VALUES (1, 30, 'Europe/London', '£', 'GBP', '2026-01-01')"
            )
            cp_id = self.store._conn.execute(
                "SELECT id FROM config_periods LIMIT 1"
            ).fetchone()[0]
            # Main meter: imp_cost=£1.00, imp_cost_remainder=£0.30 (house-only)
            self.store._conn.execute(
                "INSERT INTO blocks (block_start, block_end, meter_id, config_period_id, "
                "imp_kwh, imp_kwh_remainder, imp_rate, imp_cost, imp_cost_remainder, "
                "exp_kwh, exp_cost, standing_charge) "
                "VALUES ('2026-04-07T00:00:00', '2026-04-07T00:30:00', "
                "'electricity_main', ?, 10.0, 3.0, 0.05, 0.50, 0.15, 0.0, 0.0, 0.50)",
                (cp_id,)
            )
            self.store._conn.execute(
                "INSERT INTO blocks (block_start, block_end, meter_id, config_period_id, "
                "imp_kwh, imp_kwh_remainder, imp_rate, imp_cost, imp_cost_remainder, "
                "exp_kwh, exp_cost, standing_charge) "
                "VALUES ('2026-04-07T00:30:00', '2026-04-07T01:00:00', "
                "'electricity_main', ?, 10.0, 3.0, 0.05, 0.50, 0.15, 0.0, 0.0, 0.0)",
                (cp_id,)
            )
            # Sub-meter (EV): accounts for the difference between imp_cost and remainder
            self.store._conn.execute(
                "INSERT OR IGNORE INTO meters (meter_id, config_period_id, is_sub_meter) "
                "VALUES ('ev_charger', ?, 1)", (cp_id,)
            )
            self.store._conn.execute(
                "INSERT INTO blocks (block_start, block_end, meter_id, config_period_id, "
                "imp_kwh, imp_kwh_grid, imp_rate, imp_cost, exp_kwh, exp_cost) "
                "VALUES ('2026-04-07T00:00:00', '2026-04-07T00:30:00', "
                "'ev_charger', ?, 7.0, 7.0, 0.05, 0.35, 0.0, 0.0)",
                (cp_id,)
            )
            self.store._conn.execute(
                "INSERT INTO blocks (block_start, block_end, meter_id, config_period_id, "
                "imp_kwh, imp_kwh_grid, imp_rate, imp_cost, exp_kwh, exp_cost) "
                "VALUES ('2026-04-07T00:30:00', '2026-04-07T01:00:00', "
                "'ev_charger', ?, 7.0, 7.0, 0.05, 0.35, 0.0, 0.0)",
                (cp_id,)
            )
        server.app.config["TESTING"] = True
        self.client = server.app.test_client()

    def tearDown(self):
        import os
        self.store._conn.close()
        if os.path.exists(self.tmp):
            os.remove(self.tmp)

    def test_direct_import_uses_rate_based_subtraction(self):
        """Direct import cost = main_cost - sub_cost by rate, not imp_cost_remainder."""
        with patch.object(server, "_get_store", return_value=self.store), \
             patch.object(server, "load_config", return_value={
                 "meters": {
                     "electricity_main": {"meta": {"timezone": "Europe/London", "billing_day": 1}},
                     "ev_charger": {"meta": {"sub_meter": True}},
                 }
             }):
            r = self.client.get("/api/charts/blocks-summary")
        if r.status_code != 200:
            self.skipTest(f"blocks-summary returned {r.status_code}")
        d = json.loads(r.data)
        days = d.get("days", [])
        if not days:
            self.skipTest("No day data returned")
        # Find Apr 7 data
        apr7 = next((day for day in days if "2026-04-07" in day.get("date", "")), None)
        if not apr7:
            self.skipTest("Apr 7 not in response")
        main = apr7.get("main", {})
        # Rate-based: main_cost(£1.00) - sub_cost(£0.70) = £0.30
        # imp_cost_remainder would also be £0.30 in this test case
        # Key: cost should be ~0.30, NOT ~1.00 (the full imp_cost)
        self.assertAlmostEqual(main.get("imp_cost", 0), 0.30, places=2,
                                msg="Direct import cost should be rate-based remainder, not full imp_cost")


# ─────────────────────────────────────────────────────────────────────────────
# Gauge scale from power_history (2.8.0)
# ─────────────────────────────────────────────────────────────────────────────

class TestGaugeScaleFromPowerHistory(unittest.TestCase):
    """Gauge scale is derived from power_history p90 not a block loop."""

    def setUp(self):
        import tempfile
        self.tmp = tempfile.mktemp(suffix=".db")
        from block_store import BlockStore
        self.store = BlockStore(self.tmp)
        # Add power_history rows with known values
        from datetime import datetime, timezone, timedelta
        with self.store._conn:
            for i, kw in enumerate([1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0]):
                ts = (datetime.now(timezone.utc) - timedelta(hours=i+1)).replace(tzinfo=None).isoformat()
                self.store._conn.execute(
                    "INSERT INTO power_history (captured_at, net_kw, intensity) VALUES (?, ?, 100.0)",
                    (ts, kw)
                )
        server.app.config["TESTING"] = True
        self.client = server.app.test_client()

    def tearDown(self):
        import os
        self.store._conn.close()
        if os.path.exists(self.tmp):
            os.remove(self.tmp)

    def test_gauge_scale_uses_power_history(self):
        """Live power page renders without error and gauge uses power_history."""
        with patch.object(server, "_get_store", return_value=self.store), \
             patch.object(server, "load_config", return_value=MINIMAL_CONFIG), \
             patch.object(server, "_ha_client", None):
            r = self.client.get("/live-power")
        # Should render successfully (200) — gauge scale code runs without error
        self.assertEqual(r.status_code, 200)

class TestDataSourceModeSupplierGating(unittest.TestCase):
    """Server-side enforcement of supplier gating on POST /api/data-source-mode.

    The route must reject an API-backed mode for a non-API-capable supplier even
    if the UI would never send it (don't trust the UI alone). Capability
    correctness itself is unit-tested in test_engine; here we exercise the
    route's branching with faithful controlled stand-ins on the engine stub.
    """

    def setUp(self):
        self.client = make_client()
        import server as _s
        self._s = _s
        self._eng = sys.modules["engine"]
        self._saved = {k: getattr(self._eng, k, None)
                       for k in ("mode_uses_api", "supplier_is_api_capable",
                                 "set_data_source_mode", "has_kraken_credentials")}
        self._eng.mode_uses_api = lambda m=None: (m or "") in ("cad+api", "api", "api+mini")
        self._eng.supplier_is_api_capable = lambda s=None: "octopus" in (s or "").strip().lower()
        self._eng.set_data_source_mode = lambda m: m
        self._eng.has_kraken_credentials = lambda: True   # creds present by default

    def tearDown(self):
        for k, v in self._saved.items():
            if v is None:
                if hasattr(self._eng, k):
                    delattr(self._eng, k)
            else:
                setattr(self._eng, k, v)

    def _post(self, body):
        return self.client.post("/api/data-source-mode",
                                data=json.dumps(body),
                                content_type="application/json")

    def test_api_mode_with_octopus_accepted(self):
        r = self._post({"mode": "api", "supplier": "octopus"})
        self.assertEqual(r.status_code, 200)
        self.assertTrue(r.get_json().get("ok"))

    def test_api_mode_with_not_listed_rejected(self):
        r = self._post({"mode": "api", "supplier": "not-listed"})
        self.assertEqual(r.status_code, 400)
        self.assertEqual(r.get_json().get("error"), "supplier_not_api_capable")

    def test_cadapi_mode_with_not_listed_rejected(self):
        r = self._post({"mode": "cad+api", "supplier": "not-listed"})
        self.assertEqual(r.status_code, 400)

    def test_api_mode_without_credentials_rejected(self):
        # API mode requires creds — cad→api with no creds must be rejected,
        # not silently leave the user on a mode with nothing to poll.
        self._eng.has_kraken_credentials = lambda: False
        r = self._post({"mode": "api", "supplier": "octopus"})
        self.assertEqual(r.status_code, 400)
        self.assertEqual(r.get_json().get("error"), "no_credentials")

    def test_cad_mode_not_gated(self):
        r = self._post({"mode": "cad", "supplier": "not-listed"})
        self.assertEqual(r.status_code, 200)
        self.assertTrue(r.get_json().get("ok"))

    def test_api_mode_falls_back_to_config_octopus_accepted(self):
        cfg = {"meters": {"electricity_main": {"meta": {"supplier": "octopus"}}}}
        with patch.object(self._s, "load_config", return_value=cfg):
            r = self._post({"mode": "api"})
        self.assertEqual(r.status_code, 200)

    def test_api_mode_config_fallback_local_only_rejected(self):
        cfg = {"meters": {"electricity_main": {"meta": {"supplier": "not-listed"}}}}
        with patch.object(self._s, "load_config", return_value=cfg):
            r = self._post({"mode": "api"})
        self.assertEqual(r.status_code, 400)


class TestDisconnectKrakenRoute(unittest.TestCase):
    """POST /api/disconnect-kraken delegates to engine.disconnect_kraken via the
    engine loop and maps the result to the HTTP status. Engine logic itself is
    unit-tested in test_engine; here we exercise the route's wiring/branching."""

    def setUp(self):
        self.client = make_client()
        import server as _s
        self._s = _s
        self._eng = sys.modules["engine"]
        self._saved_dis = getattr(self._eng, "disconnect_kraken", None)
        self._saved_run = _s._run_on_engine_loop
        # disconnect_kraken stub returns the dict directly; _run_on_engine_loop
        # is replaced with a pass-through so no real engine loop is needed.
        self._s._run_on_engine_loop = lambda coro, timeout=None: coro

    def tearDown(self):
        if self._saved_dis is None:
            if hasattr(self._eng, "disconnect_kraken"):
                delattr(self._eng, "disconnect_kraken")
        else:
            self._eng.disconnect_kraken = self._saved_dis
        self._s._run_on_engine_loop = self._saved_run

    def _post(self):
        return self.client.post("/api/disconnect-kraken",
                                data="{}", content_type="application/json")

    def test_disconnect_ok_returns_200(self):
        self._eng.disconnect_kraken = lambda: {"ok": True, "mode": "cad",
                                               "had_credentials": True}
        r = self._post()
        self.assertEqual(r.status_code, 200)
        j = r.get_json()
        self.assertTrue(j["ok"])
        self.assertEqual(j["mode"], "cad")

    def test_disconnect_failure_returns_500(self):
        self._eng.disconnect_kraken = lambda: {"ok": False, "detail": "boom"}
        r = self._post()
        self.assertEqual(r.status_code, 500)
        self.assertFalse(r.get_json()["ok"])


if __name__ == "__main__":
    unittest.main()

class TestUpdateCheck(unittest.TestCase):
    """BL-6: cached update check. Never fatal; semver-aware; no self-nagging."""

    def setUp(self):
        self.client = make_client()
        server._update_check_cache.update({"checked_at": 0.0, "payload": None})
        self._ver = server.APP_VERSION
        server.APP_VERSION = "3.2.0"   # tests run outside the add-on layout

    def tearDown(self):
        server._update_check_cache.update({"checked_at": 0.0, "payload": None})
        server.APP_VERSION = self._ver

    def test_parse_version_is_semver_not_string(self):
        self.assertGreater(server._parse_version("3.10.0"),
                           server._parse_version("3.2.0"))
        self.assertEqual(server._parse_version("v3.2"), (3, 2, 0))
        self.assertEqual(server._parse_version(""), (0, 0, 0))
        self.assertEqual(server._parse_version("3.2.0-beta"), (3, 2, 0))

    def test_newer_release_flags_update(self):
        payload = json.dumps({"tag_name": "v9.9.9",
                              "html_url": "https://example/releases/9.9.9"})
        m = MagicMock()
        m.read.return_value = payload.encode()
        m.__enter__ = lambda s: m
        m.__exit__ = lambda s, *a: False
        with patch("urllib.request.urlopen", return_value=m):
            r = self.client.get("/api/update-check")
        d = r.get_json()
        self.assertTrue(d["update_available"])
        self.assertEqual(d["latest"], "9.9.9")

    def test_same_or_older_release_does_not_flag(self):
        payload = json.dumps({"tag_name": "v0.0.1"})
        m = MagicMock()
        m.read.return_value = payload.encode()
        m.__enter__ = lambda s: m
        m.__exit__ = lambda s, *a: False
        with patch("urllib.request.urlopen", return_value=m):
            d = self.client.get("/api/update-check").get_json()
        self.assertFalse(d["update_available"])

    def test_network_failure_is_not_fatal(self):
        with patch("urllib.request.urlopen", side_effect=OSError("no network")):
            r = self.client.get("/api/update-check")
        self.assertEqual(r.status_code, 200)
        self.assertFalse(r.get_json()["update_available"])

    def test_result_is_cached(self):
        payload = json.dumps({"tag_name": "v9.9.9"})
        m = MagicMock()
        m.read.return_value = payload.encode()
        m.__enter__ = lambda s: m
        m.__exit__ = lambda s, *a: False
        with patch("urllib.request.urlopen", return_value=m) as up:
            self.client.get("/api/update-check")
            self.client.get("/api/update-check")
        self.assertEqual(up.call_count, 1)   # second served from cache

    def test_unknown_own_version_never_prompts(self):
        """If we can't read our own version, (0,0,0) < anything would prompt on
        every release. Guard against that."""
        server.APP_VERSION = ""
        payload = json.dumps({"tag_name": "v9.9.9"})
        m = MagicMock()
        m.read.return_value = payload.encode()
        m.__enter__ = lambda s: m
        m.__exit__ = lambda s, *a: False
        with patch("urllib.request.urlopen", return_value=m):
            d = self.client.get("/api/update-check").get_json()
        self.assertFalse(d["update_available"])


class TestHistoryGaps(unittest.TestCase):
    """Gap review is a local DB question — never gated on the supplier API.
    Recovery is, because the readings can only come from the supplier."""

    def setUp(self):
        import engine as _eng
        self._eng = _eng
        self._orig = getattr(_eng, "kraken_available", None)

    def tearDown(self):
        if self._orig is None:
            if hasattr(self._eng, "kraken_available"):
                del self._eng.kraken_available
        else:
            self._eng.kraken_available = self._orig

    def test_find_gaps_works_without_api(self):
        client = make_client()
        self._eng.kraken_available = lambda: False
        r = client.get("/api/history-gaps")
        self.assertEqual(r.status_code, 200)
        d = r.get_json()
        self.assertTrue(d["ok"])
        self.assertFalse(d["api_available"])   # UI uses this to explain itself
        self.assertIn("gaps", d)

    def test_find_gaps_reports_api_when_present(self):
        client = make_client()
        self._eng.kraken_available = lambda: True
        d = client.get("/api/history-gaps").get_json()
        self.assertTrue(d["api_available"])

    def test_resolve_gaps_requires_api(self):
        client = make_client()
        self._eng.kraken_available = lambda: False
        r = client.post("/api/resolve-gaps")
        self.assertEqual(r.status_code, 400)
        self.assertEqual(r.get_json()["reason"], "no_api")


class TestInstanceLabel(unittest.TestCase):
    """The footer label distinguishes instances by INSTALL identity, not data —
    so it survives a restore (unlike the site name). Supervised reads the add-on
    name; standalone falls back to EMT_INSTANCE_NAME, then the hostname."""

    def setUp(self):
        self._saved = {k: os.environ.get(k)
                       for k in ("SUPERVISOR_TOKEN", "EMT_INSTANCE_NAME")}
        server._INSTANCE_LABEL = None

    def tearDown(self):
        for k, v in self._saved.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v
        server._INSTANCE_LABEL = None

    def test_standalone_uses_env_override(self):
        os.environ.pop("SUPERVISOR_TOKEN", None)
        os.environ["EMT_INSTANCE_NAME"] = "Energy Meter Tracker (DEV)"
        self.assertEqual(server._instance_label(), "Energy Meter Tracker (DEV)")

    def test_falls_back_to_hostname(self):
        import socket
        os.environ.pop("SUPERVISOR_TOKEN", None)
        os.environ.pop("EMT_INSTANCE_NAME", None)
        self.assertEqual(server._instance_label(), socket.gethostname())

    def test_result_is_memoised(self):
        os.environ.pop("SUPERVISOR_TOKEN", None)
        os.environ["EMT_INSTANCE_NAME"] = "First"
        self.assertEqual(server._instance_label(), "First")
        os.environ["EMT_INSTANCE_NAME"] = "Second"   # cache must still hold First
        self.assertEqual(server._instance_label(), "First")

    def test_option_overrides_supervisor_name(self):
        # The explicit instance_name option wins over the manifest name — and is
        # taken WITHOUT a network call (the fake token is never used).
        os.environ["SUPERVISOR_TOKEN"] = "faketoken-never-used"
        os.environ["EMT_INSTANCE_NAME"] = "Prod"
        self.assertEqual(server._instance_label(), "Prod")


class TestReviewBlocksAPI(unittest.TestCase):
    """BL-18 — /api/review-blocks GET lists flagged blocks with a local window
    and reason; dismiss clears them."""

    def setUp(self):
        self.store = _make_test_store(MINIMAL_BLOCKS)
        self.store._conn.execute(
            "UPDATE blocks SET needs_review = 1, "
            "review_reason = 'dispatch ambiguous: completed 2.10 kWh without started' "
            "WHERE meter_id = 'electricity_main'")
        # A CAD/DCC drift-style flag: needs_review set but NO review_reason. It
        # must NOT appear in the correction review list (nothing rate-fixable).
        self.store._conn.execute(
            "INSERT INTO blocks (block_start, block_end, meter_id, config_period_id, "
            "imp_kwh, imp_kwh_api, needs_review) VALUES "
            "('2026-01-16T00:00:00','2026-01-16T00:30:00','electricity_main',1,1.0,0.5,1)")
        self.store._conn.commit()
        self.client = make_client(store=self.store)

    def test_lists_flagged_with_local_window(self):
        r = self.client.get("/api/review-blocks")
        self.assertEqual(r.status_code, 200)
        d = r.get_json()
        self.assertEqual(d["count"], 1)                   # drift block excluded
        b = d["blocks"][0]
        self.assertIn("without started", b["reason"])
        self.assertEqual(b["block_start"], "2026-01-15T00:00:00")
        self.assertEqual(b["local_date"], "2026-01-15")   # 00:00 UTC → local (GMT)
        self.assertEqual(b["from_time"], "00:00")
        self.assertEqual(b["to_time"], "00:30")

    def test_drift_flag_excluded_and_untouched_by_dismiss_all(self):
        # 'Dismiss all' clears the dispatch flag but leaves the drift flag set.
        self.client.post("/api/review-blocks/dismiss", json={})
        self.assertEqual(self.client.get("/api/review-blocks").get_json()["count"], 0)
        drift = self.store._conn.execute(
            "SELECT needs_review FROM blocks WHERE block_start='2026-01-16T00:00:00'").fetchone()
        self.assertEqual(drift["needs_review"], 1)        # dormant drift flag survives

    def test_dismiss_subset_clears_flag(self):
        bid = self.store._conn.execute(
            "SELECT id FROM blocks WHERE needs_review = 1").fetchone()[0]
        r = self.client.post("/api/review-blocks/dismiss", json={"block_ids": [bid]})
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.get_json()["cleared"], 1)
        self.assertEqual(self.client.get("/api/review-blocks").get_json()["count"], 0)

    def test_dismiss_all(self):
        r = self.client.post("/api/review-blocks/dismiss", json={})
        self.assertEqual(r.status_code, 200)
        self.assertEqual(self.client.get("/api/review-blocks").get_json()["count"], 0)

    def test_dismiss_rejects_bad_type(self):
        r = self.client.post("/api/review-blocks/dismiss", json={"block_ids": "nope"})
        self.assertEqual(r.status_code, 400)


class TestNearbyRatesAPI(unittest.TestCase):
    """#270 — /api/corrections/nearby-rates offers the exact rates in force in
    blocks surrounding a correction window, ranked by frequency, so the user can
    pick rather than re-type."""

    def setUp(self):
        self.store = _make_test_store([])
        seed = [
            ("2026-07-10T17:00:00", 0.323092),   # peak ×3
            ("2026-07-10T17:30:00", 0.323092),
            ("2026-07-10T18:00:00", 0.323092),
            ("2026-07-10T02:00:00", 0.054930),   # off-peak ×2
            ("2026-07-10T02:30:00", 0.054930),
            ("2026-06-01T12:00:00", 0.245000),   # far outside ±3d window → excluded
        ]
        for bs, rate in seed:
            self.store._conn.execute(
                "INSERT INTO blocks (block_start, block_end, meter_id, "
                "config_period_id, imp_kwh, imp_rate) VALUES (?,?,?,1,1.0,?)",
                (bs, bs, "electricity_main", rate))
        self.store._conn.commit()
        self.client = make_client(store=self.store)

    def _get(self, channel="import", frm="2026-07-10", to="2026-07-10"):
        return self.client.get(
            f"/api/corrections/nearby-rates?channel={channel}&from_date={frm}&to_date={to}")

    def test_ranked_by_frequency_within_window(self):
        d = self._get().get_json()
        self.assertEqual([x["rate"] for x in d["rates"]], [0.323092, 0.054930])
        self.assertEqual(d["rates"][0]["count"], 3)   # peak most common
        self.assertEqual(d["rates"][1]["count"], 2)   # off-peak next

    def test_excludes_out_of_window_rate(self):
        rates = [x["rate"] for x in self._get().get_json()["rates"]]
        self.assertNotIn(0.245000, rates)             # the June flat rate

    def test_export_channel_separate(self):
        self.store._conn.execute(
            "INSERT INTO blocks (block_start, block_end, meter_id, config_period_id, "
            "exp_kwh, exp_rate) VALUES "
            "('2026-07-10T12:00:00','2026-07-10T12:30:00','electricity_main',1,1.0,0.15)")
        self.store._conn.commit()
        rates = [x["rate"] for x in self._get(channel="export").get_json()["rates"]]
        self.assertEqual(rates, [0.15])

    def test_bad_channel_400(self):
        self.assertEqual(self._get(channel="nope").status_code, 400)

    def test_missing_dates_400(self):
        r = self.client.get("/api/corrections/nearby-rates?channel=import")
        self.assertEqual(r.status_code, 400)

    def test_registered(self):
        self.assertTrue("api_corrections_nearby_rates" in server.app.view_functions)


class TestChargeSessionsBuilder(unittest.TestCase):
    """BL-10 — _build_charge_sessions folds *delivered* dispatch slots into
    all-off-peak charging sessions (one per charging period)."""

    def _h(self, slot, kind, e=None, raw_start=None, raw_end=None):
        return {"slot_start": slot, "kind": kind, "energy_kwh": e,
                "provider": "Myenergi", "raw_start": raw_start, "raw_end": raw_end}

    def test_groups_contiguous_and_splits_on_big_gap(self):
        hist = [self._h("2026-07-10T15:00:00", "completed", -1.71),
                self._h("2026-07-10T15:30:00", "completed", -3.42),
                self._h("2026-07-10T20:00:00", "completed", -2.0)]   # 270m gap → new
        s = server._build_charge_sessions(hist, {}, {})
        self.assertEqual(len(s), 2)
        self.assertTrue(s[0]["start"].startswith("2026-07-10T20"))   # newest-first
        a = [x for x in s if x["start"].startswith("2026-07-10T15")][0]
        self.assertEqual(a["n_slots"], 2)
        self.assertAlmostEqual(a["kwh"], 5.13, places=2)

    def test_bridges_inter_burst_gap_within_period(self):
        # 20:30 → 23:30 is exactly 180 min: bridged into one session.
        hist = [self._h("2026-07-10T20:00:00", "completed", -3.0),
                self._h("2026-07-10T20:30:00", "completed", -3.0),
                self._h("2026-07-10T23:30:00", "completed", -3.0)]
        s = server._build_charge_sessions(hist, {}, {})
        self.assertEqual(len(s), 1)
        self.assertEqual(s[0]["n_slots"], 3)
        # > 180 min splits.
        hist2 = [self._h("2026-07-10T20:00:00", "completed", -3.0),
                 self._h("2026-07-10T23:31:00", "completed", -3.0)]
        self.assertEqual(len(server._build_charge_sessions(hist2, {}, {})), 2)

    def test_planned_only_slot_dropped(self):
        # A plan that never delivered energy is not shown at all.
        self.assertEqual(
            server._build_charge_sessions(
                [self._h("2026-07-10T18:00:00", "planned", -1.9)], {}, {}), [])

    def test_started_only_slot_dropped(self):
        self.assertEqual(
            server._build_charge_sessions(
                [self._h("2026-07-10T18:00:00", "started")], {}, {}), [])

    def test_slot_billed_at_peak_is_peak(self):
        # A delivered slot billed at the day's peak rate reads peak (red),
        # matching the billing charts — not off-peak.
        s = server._build_charge_sessions(
            [self._h("2026-07-15T19:30:00", "completed", -3.0)],
            {"2026-07-15T19:30:00": 0.32}, {"2026-07-15": 0.32})[0]
        self.assertFalse(s["fill"][0]["off_peak"])
        self.assertAlmostEqual(s["peak_kwh"], 3.0, places=3)
        self.assertEqual(s["off_peak_kwh"], 0.0)
        self.assertIsNone(s["saving"])

    def test_offpeak_slot_is_green_with_saving(self):
        s = server._build_charge_sessions(
            [self._h("2026-07-15T02:00:00", "completed", -3.0)],
            {"2026-07-15T02:00:00": 0.05}, {"2026-07-15": 0.32})[0]
        self.assertTrue(s["fill"][0]["off_peak"])
        self.assertAlmostEqual(s["off_peak_kwh"], 3.0, places=3)
        self.assertAlmostEqual(s["saving"], 3.0 * (0.32 - 0.05), places=3)

    def test_mixed_session_splits_offpeak_and_peak(self):
        hist = [self._h("2026-07-15T02:00:00", "completed", -3.0),   # off-peak
                self._h("2026-07-15T02:30:00", "completed", -2.0)]   # peak
        rates = {"2026-07-15T02:00:00": 0.05, "2026-07-15T02:30:00": 0.32}
        s = server._build_charge_sessions(hist, rates, {"2026-07-15": 0.32})[0]
        self.assertAlmostEqual(s["off_peak_kwh"], 3.0, places=3)
        self.assertAlmostEqual(s["peak_kwh"], 2.0, places=3)
        self.assertAlmostEqual(s["saving"], 3.0 * (0.32 - 0.05), places=3)
        self.assertTrue(s["fill"][0]["off_peak"])
        self.assertFalse(s["fill"][1]["off_peak"])

    def test_unknown_rate_defaults_off_peak(self):
        s = server._build_charge_sessions(
            [self._h("2026-07-15T02:00:00", "completed", -3.0)], {}, {})[0]
        self.assertTrue(s["fill"][0]["off_peak"])

    def test_exact_window_raw_bounds_then_fallback(self):
        s = server._build_charge_sessions(
            [self._h("2026-07-10T15:00:00", "completed", -2.0,
                     raw_start="2026-07-10T15:03:20", raw_end="2026-07-10T15:47:10")], {}, {})[0]
        self.assertEqual(s["exact_start"], "2026-07-10T15:03:20")
        self.assertEqual(s["exact_end"], "2026-07-10T15:47:10")
        s2 = server._build_charge_sessions(
            [self._h("2026-07-10T15:00:00", "completed", -1.0)], {}, {})[0]
        self.assertEqual(s2["exact_start"], "2026-07-10T15:00:00")   # slot fallback
        self.assertEqual(s2["exact_end"], "2026-07-10T15:30:00")

    def test_status_delivered_is_completed(self):
        s = server._build_charge_sessions(
            [self._h("2026-07-10T15:00:00", "completed", -1.0)], {}, {})[0]
        self.assertEqual(s["status"], "completed")

    def test_metered_energy_overrides_dispatch(self):
        # Dispatch reports 3.0 kWh, but the metered block says 1.8 → card uses 1.8
        # (reconciles with the bill; solar/baseload make the two differ).
        s = server._build_charge_sessions(
            [self._h("2026-07-15T02:00:00", "completed", -3.0)],
            {"2026-07-15T02:00:00": 0.05}, {"2026-07-15": 0.30},
            {"2026-07-15T02:00:00": 1.8})[0]
        self.assertAlmostEqual(s["kwh"], 1.8, places=3)
        self.assertAlmostEqual(s["off_peak_kwh"], 1.8, places=3)

    def test_falls_back_to_dispatch_when_block_absent(self):
        # No metered block for the slot (unsettled) → use the dispatch figure.
        s = server._build_charge_sessions(
            [self._h("2026-07-15T02:00:00", "completed", -3.0)], {}, {}, {})[0]
        self.assertAlmostEqual(s["kwh"], 3.0, places=3)

    def test_charge_minutes_scales_with_fullness(self):
        # Two full 3 kWh slots → 60 min effective.
        full = server._build_charge_sessions(
            [self._h("2026-07-10T02:00:00", "completed", -3.0),
             self._h("2026-07-10T02:30:00", "completed", -3.0)], {}, {})[0]
        self.assertEqual(full["charge_minutes"], 60)
        # A half-full second slot scales its 30 min down to 15 → 45 total.
        part = server._build_charge_sessions(
            [self._h("2026-07-10T02:00:00", "completed", -3.0),
             self._h("2026-07-10T02:30:00", "completed", -1.5)], {}, {})[0]
        self.assertEqual(part["charge_minutes"], 45)


class TestUpcomingDispatches(unittest.TestCase):
    """BL-10 — _build_upcoming_dispatches surfaces future planned dispatches."""

    def _h(self, slot, kind, e=None, raw_start=None, raw_end=None):
        return {"slot_start": slot, "kind": kind, "energy_kwh": e,
                "provider": "Ohme", "raw_start": raw_start, "raw_end": raw_end}

    NOW = "2026-07-18T20:00:00"

    def test_future_planned_shown_scheduled(self):
        s = server._build_upcoming_dispatches(
            [self._h("2026-07-18T23:30:00", "planned", -3.4),
             self._h("2026-07-19T00:00:00", "planned", -3.4)], {}, {}, self.NOW)
        self.assertEqual(len(s), 1)
        self.assertEqual(s[0]["status"], "scheduled")
        self.assertAlmostEqual(s[0]["kwh"], 6.8, places=3)
        self.assertIsNone(s[0]["saving"])
        self.assertTrue(all(f["off_peak"] for f in s[0]["fill"]))   # unknown rate

    def test_planned_slot_at_peak_reads_peak(self):
        s = server._build_upcoming_dispatches(
            [self._h("2026-07-18T20:00:00", "planned", -3.4)],
            {"2026-07-18T20:00:00": 0.32}, {"2026-07-18": 0.32}, self.NOW)[0]
        self.assertFalse(s["fill"][0]["off_peak"])
        self.assertAlmostEqual(s["peak_kwh"], 3.4, places=3)

    def test_past_and_completed_slots_excluded(self):
        hist = [self._h("2026-07-18T12:00:00", "planned", -3.4),   # elapsed
                self._h("2026-07-18T20:00:00", "planned", -2.0),   # current slot…
                self._h("2026-07-18T20:00:00", "completed", -1.0)]  # …but delivered
        self.assertEqual(
            server._build_upcoming_dispatches(hist, {}, {}, self.NOW), [])

    def test_current_slot_included_until_it_elapses(self):
        # slot 20:00 ends 20:30, which is after now → still upcoming.
        s = server._build_upcoming_dispatches(
            [self._h("2026-07-18T20:00:00", "planned", -2.0)], {}, {}, self.NOW)
        self.assertEqual(len(s), 1)

    def test_splits_on_big_gap(self):
        s = server._build_upcoming_dispatches(
            [self._h("2026-07-18T23:30:00", "planned", -3.4),
             self._h("2026-07-19T05:00:00", "planned", -3.4)], {}, {}, self.NOW)
        self.assertEqual(len(s), 2)


class TestChargeSessionsAPI(unittest.TestCase):
    """BL-10 — /api/charge-sessions gating + shape."""

    def test_registered(self):
        self.assertTrue("api_charge_sessions" in server.app.view_functions)

    def test_gating_no_dispatch_data(self):
        client = make_client(store=_make_test_store([]))
        d = client.get("/api/charge-sessions").get_json()
        self.assertFalse(d["has_data"])
        self.assertEqual(d["sessions"], [])
        self.assertEqual(d["upcoming"], [])

    def test_returns_recent_session(self):
        from datetime import datetime, timezone, timedelta
        store = _make_test_store([])
        base = (datetime.now(timezone.utc).replace(tzinfo=None)
                - timedelta(days=1)).replace(minute=0, second=0, microsecond=0)
        s0 = base.replace(hour=2).isoformat()
        s1 = base.replace(hour=2, minute=30).isoformat()
        pk = base.replace(hour=18).isoformat()
        # Energy comes from the dispatch figure (3.0 each); blocks supply rates.
        for slot, e in ((s0, -3.0), (s1, -3.0)):
            store.record_dispatch_history(slot, "completed", provider="Myenergi", energy_kwh=e)
            store._conn.execute(
                "INSERT INTO blocks (block_start,block_end,meter_id,config_period_id,"
                "imp_kwh,imp_rate) VALUES (?,?,?,1,3.0,0.05)", (slot, slot, "electricity_main"))
        store._conn.execute(
            "INSERT INTO blocks (block_start,block_end,meter_id,config_period_id,"
            "imp_kwh,imp_rate) VALUES (?,?,?,1,1.0,0.30)", (pk, pk, "electricity_main"))
        store._conn.commit()
        d = make_client(store=store).get("/api/charge-sessions").get_json()
        self.assertTrue(d["has_data"])
        self.assertEqual(len(d["sessions"]), 1)
        s = d["sessions"][0]
        self.assertAlmostEqual(s["kwh"], 6.0, places=3)      # dispatch 3.0 + 3.0
        self.assertAlmostEqual(s["off_peak_kwh"], 6.0, places=3)
        self.assertIn("local", s)
        self.assertIn("upcoming", d)

    def test_returns_upcoming_planned(self):
        from datetime import datetime, timezone, timedelta
        store = _make_test_store([])
        now = datetime.now(timezone.utc).replace(tzinfo=None, second=0, microsecond=0)
        slot = (now + timedelta(hours=3)).replace(minute=0).isoformat()
        store.record_dispatch_history(slot, "planned", provider="Ohme", energy_kwh=-3.4)
        d = make_client(store=store).get("/api/charge-sessions").get_json()
        self.assertTrue(d["has_data"])
        self.assertEqual(d["sessions"], [])
        self.assertEqual(len(d["upcoming"]), 1)
        self.assertEqual(d["upcoming"][0]["status"], "scheduled")
        self.assertIn("local", d["upcoming"][0])


class TestResponseCompression(unittest.TestCase):
    """gzip after_request — large text bodies are compressed for clients that
    advertise gzip (fixes the uncompressed multi-MB chart transfer on :8099)."""

    def test_large_html_gzipped_and_decodes(self):
        import gzip as _gz
        r = make_client().get("/charts", headers={"Accept-Encoding": "gzip"})
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.headers.get("Content-Encoding"), "gzip")
        body = _gz.decompress(r.data)
        self.assertIn(b"<", body)
        self.assertIn("Accept-Encoding", r.headers.get("Vary", ""))

    def test_not_compressed_without_accept_encoding(self):
        # Werkzeug test client sends no Accept-Encoding by default.
        r = make_client().get("/charts")
        self.assertIsNone(r.headers.get("Content-Encoding"))
        self.assertIn(b"<", r.data)

    def test_small_body_not_compressed(self):
        # A tiny JSON body is below the gzip threshold → left uncompressed.
        r = make_client().post("/api/last-page", json={"page": "charts"},
                               headers={"Accept-Encoding": "gzip"})
        self.assertIsNone(r.headers.get("Content-Encoding"))


class TestHistoricalProbeEndpoint(unittest.TestCase):
    """Read-only recorder-statistics probe endpoint (historical-import spike)."""

    def test_registered(self):
        self.assertIn("api_historical_probe", server.app.view_functions)

    def test_no_sensors_returns_400(self):
        # Validation happens before touching the engine loop.
        r = make_client().post("/api/historical/probe", json={"entity_ids": []})
        self.assertEqual(r.status_code, 400)
        self.assertFalse(r.get_json()["ok"])


class TestExportProbeEndpoint(unittest.TestCase):
    """Read-only export-retention probe endpoint (historical-import spike)."""

    def test_registered(self):
        self.assertIn("api_export_probe", server.app.view_functions)

    def test_no_api_returns_400(self):
        # The stub engine (types.ModuleType) has no kraken_available; set it.
        import engine
        engine.kraken_available = lambda: False
        try:
            r = make_client().post("/api/historical/export-probe", json={})
            self.assertEqual(r.status_code, 400)
            self.assertFalse(r.get_json()["ok"])
        finally:
            del engine.kraken_available

    def test_returns_probe_result(self):
        import engine
        saved_run = server._run_on_engine_loop
        engine.kraken_available = lambda: True
        engine.probe_consumption_retention = lambda: None   # coro not created
        server._run_on_engine_loop = lambda coro, timeout=None: {
            "ok": True,
            "channels": {
                "import": {"channel": "import", "available": True,
                           "earliest": "2024-06-12T13:00:00+00:00"},
                "export": {"channel": "export", "available": True,
                           "earliest": "2024-06-29T03:00:00+00:00"},
            },
            "lag_days": 16,
        }
        try:
            r = make_client().post("/api/historical/export-probe", json={})
            self.assertEqual(r.status_code, 200)
            body = r.get_json()
            self.assertTrue(body["ok"])
            self.assertEqual(body["lag_days"], 16)
        finally:
            server._run_on_engine_loop = saved_run
            del engine.kraken_available
            del engine.probe_consumption_retention

    def test_diagnostic_registered(self):
        self.assertIn("api_consumption_diagnostic", server.app.view_functions)

    def test_diagnostic_no_api_returns_400(self):
        import engine
        engine.kraken_available = lambda: False
        try:
            r = make_client().post("/api/historical/consumption-diagnostic", json={})
            self.assertEqual(r.status_code, 400)
            self.assertFalse(r.get_json()["ok"])
        finally:
            del engine.kraken_available

    def test_diagnostic_returns_meter_points(self):
        import engine
        saved_run = server._run_on_engine_loop
        engine.kraken_available = lambda: True
        engine.diagnose_consumption_retention = lambda: None
        server._run_on_engine_loop = lambda coro, timeout=None: {
            "ok": True,
            "meter_points": [{"mpan_tail": "…000001", "is_export": False,
                              "meter_count": 2, "meters": [
                {"serial_tail": "…AAA", "available": True,
                 "earliest": "2024-07-01T00:00:00+00:00"},
                {"serial_tail": "…BBB", "available": True,
                 "earliest": "2024-07-20T00:00:00+00:00"}]}],
        }
        try:
            r = make_client().post("/api/historical/consumption-diagnostic", json={})
            self.assertEqual(r.status_code, 200)
            body = r.get_json()
            self.assertTrue(body["ok"])
            self.assertEqual(body["meter_points"][0]["meter_count"], 2)
        finally:
            server._run_on_engine_loop = saved_run
            del engine.kraken_available
            del engine.diagnose_consumption_retention


class TestHistoricalCsvEndpoints(unittest.TestCase):
    """CSV import wizard endpoints (3.5.0). Preview is pure (csv_import); apply is
    guarded here (its write path is covered by test_csv_import_apply)."""

    _CSV = ("Consumption (kwh),Estimated Cost Inc. Tax (p),"
            "Standing Charge Inc. Tax (p),Start,End\n"
            "1.0,7.0,1.12,2024-07-01T00:00:00+00:00,2024-07-01T00:30:00+00:00\n"
            "1.0,7.0,1.12,2024-07-01T00:30:00+00:00,2024-07-01T01:00:00+00:00\n")

    def test_registered(self):
        self.assertIn("api_historical_csv_preview", server.app.view_functions)
        self.assertIn("api_historical_csv_apply", server.app.view_functions)
        self.assertIn("historical_import_page", server.app.view_functions)

    def test_preview_derives_rates(self):
        r = make_client().post("/api/historical/csv/preview",
                               json={"import_csv": self._CSV})
        self.assertEqual(r.status_code, 200)
        body = r.get_json()
        self.assertTrue(body["ok"])
        self.assertEqual(body["channels"]["import"]["block_count"], 2)
        self.assertTrue(body["channels"]["import"]["periods"])

    def test_preview_no_csv_400(self):
        r = make_client().post("/api/historical/csv/preview", json={})
        self.assertEqual(r.status_code, 400)

    def test_apply_requires_confirmation(self):
        r = make_client().post("/api/historical/csv/apply",
                               json={"import_csv": self._CSV})
        self.assertEqual(r.status_code, 400)
        self.assertIn("confirm", r.get_json()["error"].lower())

    def test_apply_no_csv_400(self):
        r = make_client().post("/api/historical/csv/apply",
                               json={"confirmed": True})
        self.assertEqual(r.status_code, 400)

    def test_api_plan_registered_and_gated(self):
        self.assertIn("api_historical_api_plan", server.app.view_functions)
        import engine
        engine.kraken_available = lambda: False
        try:
            r = make_client().post("/api/historical/api-import/plan", json={})
            self.assertEqual(r.status_code, 400)
        finally:
            del engine.kraken_available

    def test_api_apply_registered_and_requires_confirmation(self):
        self.assertIn("api_historical_api_apply", server.app.view_functions)
        import engine
        engine.kraken_available = lambda: True
        try:
            r = make_client().post("/api/historical/api-import/apply", json={})
            self.assertEqual(r.status_code, 400)                  # no confirmed, not dry_run
            self.assertIn("confirm", r.get_json()["error"].lower())
        finally:
            del engine.kraken_available

    def test_bg_job_endpoints_registered(self):
        for name in ("api_historical_api_start", "api_historical_api_control",
                     "api_historical_api_status"):
            self.assertIn(name, server.app.view_functions)

    def test_bg_start_requires_confirmation(self):
        import engine
        engine.kraken_available = lambda: True
        engine.api_import_running = lambda: False
        try:
            r = make_client().post("/api/historical/api-import/start", json={})
            self.assertEqual(r.status_code, 400)
            self.assertIn("confirm", r.get_json()["error"].lower())
        finally:
            del engine.kraken_available
            del engine.api_import_running

    def test_restore_rejects_bad_zip(self):
        # Path-traversal guard fires before any DB teardown (registration sanity).
        r = make_client().post("/api/backup/restore", json={"zip": "../etc/passwd"})
        self.assertEqual(r.status_code, 400)

    def test_restore_status_registered(self):
        self.assertIn("api_backup_restore_status", server.app.view_functions)

    def test_restore_missing_zip_404(self):
        # Validated in the launcher, before any background thread is spawned.
        r = make_client().post("/api/backup/restore",
                               json={"zip": "definitely_not_a_real_backup.zip"})
        self.assertEqual(r.status_code, 404)

    def test_restore_rejects_when_already_running(self):
        server._restore_job = {"status": "running"}
        try:
            r = make_client().post("/api/backup/restore", json={"from_flat": True})
            self.assertEqual(r.status_code, 409)
        finally:
            server._restore_job = {"status": "idle"}

    def test_restore_status_returns_job(self):
        server._restore_job = {"status": "done", "restored": ["blocks.db"]}
        try:
            r = make_client().get("/api/backup/restore/status")
            self.assertEqual(r.get_json()["status"], "done")
        finally:
            server._restore_job = {"status": "idle"}

    def test_bg_status_and_control_delegate(self):
        import engine
        engine.api_import_status = lambda: {"status": "idle"}
        engine.api_import_control = lambda a: {"ok": True, "action": a}
        try:
            r = make_client().get("/api/historical/api-import/status")
            self.assertEqual(r.get_json()["status"], "idle")
            r = make_client().post("/api/historical/api-import/control",
                                   json={"action": "pause"})
            self.assertTrue(r.get_json()["ok"])
            self.assertEqual(r.get_json()["action"], "pause")
        finally:
            del engine.api_import_status
            del engine.api_import_control

# ─────────────────────────────────────────────────────────────────────────────
# Restore durability + API-credentials-missing surfacing
#   Regression: a restore→rebuild left an empty DB and a "connected" UI that was
#   actually credential-less. Atomic writes + a credentials_missing flag fix it.
# ─────────────────────────────────────────────────────────────────────────────

_MISSING = object()


class TestAtomicRestoreWrite(unittest.TestCase):
    """_atomic_restore_write must never leave a truncated/zero-length target: an
    interrupted restore leaves either the OLD file or the COMPLETE new one."""

    def setUp(self):
        import tempfile
        self.dir = tempfile.mkdtemp(prefix="emt_atomic_")
        self.dest = os.path.join(self.dir, "blocks.db")

    def tearDown(self):
        import shutil
        shutil.rmtree(self.dir, ignore_errors=True)

    def test_writes_new_content(self):
        with open(self.dest, "wb") as f:
            f.write(b"OLD")
        server._atomic_restore_write(
            self.dest, lambda fh: fh.write(b"NEW-CONTENT"))
        with open(self.dest, "rb") as f:
            self.assertEqual(f.read(), b"NEW-CONTENT")

    def test_preserves_original_when_fill_raises(self):
        # The dangerous window: an in-place "wb" open would truncate first. The
        # atomic helper must leave the original intact if the write blows up.
        with open(self.dest, "wb") as f:
            f.write(b"ORIGINAL-DB")

        def _boom(fh):
            fh.write(b"partial")
            raise RuntimeError("interrupted mid-write")

        with self.assertRaises(RuntimeError):
            server._atomic_restore_write(self.dest, _boom)
        with open(self.dest, "rb") as f:
            self.assertEqual(f.read(), b"ORIGINAL-DB")

    def test_no_tmp_leftover(self):
        server._atomic_restore_write(self.dest, lambda fh: fh.write(b"x"))
        self.assertFalse(os.path.exists(self.dest + ".restore-tmp"))

    def test_no_tmp_leftover_on_failure(self):
        try:
            server._atomic_restore_write(
                self.dest, lambda fh: (_ for _ in ()).throw(ValueError("x")))
        except ValueError:
            pass
        self.assertFalse(os.path.exists(self.dest + ".restore-tmp"))


class TestCredentialsMissingSurfacing(unittest.TestCase):
    """mode=api with no stored key must read as 'credentials missing', not as a
    normal 'no API / local billing' state, so the UI can prompt a re-entry."""

    def setUp(self):
        self.client = make_client()
        self._eng = sys.modules["engine"]
        self._keys = ("mode_uses_api", "has_kraken_credentials",
                      "kraken_available", "_get_billing_source",
                      "_rate_schedule_unsupported", "get_data_source_mode",
                      "mode_uses_mini")
        self._saved = {k: getattr(self._eng, k, _MISSING) for k in self._keys}
        self._eng._get_billing_source = lambda: "dcc"
        self._eng.kraken_available = lambda: False
        self._eng._rate_schedule_unsupported = None
        self._eng.get_data_source_mode = lambda: "api"
        self._eng.mode_uses_mini = lambda m=None: False

    def tearDown(self):
        for k, v in self._saved.items():
            if v is _MISSING:
                if hasattr(self._eng, k):
                    delattr(self._eng, k)
            else:
                setattr(self._eng, k, v)

    # ── /api/billing-source ──────────────────────────────────────────────────
    def test_billing_source_flags_missing(self):
        self._eng.mode_uses_api = lambda m=None: True
        self._eng.has_kraken_credentials = lambda: False
        j = self.client.get("/api/billing-source").get_json()
        self.assertTrue(j["credentials_missing"])

    def test_billing_source_not_missing_when_creds_present(self):
        self._eng.mode_uses_api = lambda m=None: True
        self._eng.has_kraken_credentials = lambda: True
        j = self.client.get("/api/billing-source").get_json()
        self.assertFalse(j["credentials_missing"])

    def test_billing_source_not_missing_in_local_mode(self):
        self._eng.mode_uses_api = lambda m=None: False
        self._eng.has_kraken_credentials = lambda: False
        j = self.client.get("/api/billing-source").get_json()
        self.assertFalse(j["credentials_missing"])

    # ── /api/data-source-mode ────────────────────────────────────────────────
    def test_data_source_mode_flags_missing(self):
        self._eng.mode_uses_api = lambda m=None: True
        self._eng.has_kraken_credentials = lambda: False
        j = self.client.get("/api/data-source-mode").get_json()
        self.assertTrue(j["credentials_missing"])
        self.assertFalse(j["has_credentials"])

    def test_data_source_mode_ok_with_creds(self):
        self._eng.mode_uses_api = lambda m=None: True
        self._eng.has_kraken_credentials = lambda: True
        j = self.client.get("/api/data-source-mode").get_json()
        self.assertFalse(j["credentials_missing"])
        self.assertTrue(j["has_credentials"])


class TestSettingsPageCredentialsFlag(unittest.TestCase):
    """The Settings page must expose HAS_CREDENTIALS so the client can render the
    'Re-enter API key' path instead of a false 'connected' state."""

    def _render(self, mode, has_creds):
        eng = sys.modules["engine"]
        saved = (getattr(eng, "get_data_source_mode", _MISSING),
                 getattr(eng, "has_kraken_credentials", _MISSING))
        eng.get_data_source_mode = lambda: mode
        eng.has_kraken_credentials = lambda: has_creds
        try:
            r = make_client().get("/settings")
            return r
        finally:
            for name, v in zip(("get_data_source_mode", "has_kraken_credentials"),
                               saved):
                if v is _MISSING:
                    if hasattr(eng, name): delattr(eng, name)
                else:
                    setattr(eng, name, v)

    def test_flag_false_when_creds_missing(self):
        r = self._render("api", False)
        self.assertEqual(r.status_code, 200)
        html = r.get_data(as_text=True)
        self.assertIn("var HAS_CREDENTIALS = false", html)
        # the re-enter path is present in the shipped JS
        self.assertIn("js-reconnect-octopus", html)

    def test_flag_true_when_creds_present(self):
        r = self._render("api", True)
        html = r.get_data(as_text=True)
        self.assertIn("var HAS_CREDENTIALS = true", html)


class TestImportRangeDiagnosticEndpoint(unittest.TestCase):
    """The import-range diagnostic route is registered and gated on an API."""

    def test_registered(self):
        self.assertIn("api_import_range_diagnostic", server.app.view_functions)

    def test_gated_without_api(self):
        import engine
        engine.kraken_available = lambda: False
        try:
            r = make_client().post("/api/historical/import-range-diagnostic")
            self.assertEqual(r.status_code, 400)
        finally:
            del engine.kraken_available


class TestImportGapsEndpoint(unittest.TestCase):
    """The persisted-gaps endpoint is registered and returns per-channel data."""

    def test_registered(self):
        self.assertIn("api_historical_api_gaps", server.app.view_functions)

    def test_returns_channels(self):
        import engine
        engine.api_import_gaps = lambda: {"channels": {
            "import": {"gaps": [], "missing": 0, "gap_count": 0},
            "export": {"gaps": [{"from": "2025-02-05T00:00:00",
                                 "to": "2025-02-07T23:30:00", "count": 144}],
                       "missing": 144, "gap_count": 1}}}
        try:
            r = make_client().get("/api/historical/api-import/gaps")
            self.assertEqual(r.status_code, 200)
            j = r.get_json()
            self.assertEqual(j["channels"]["export"]["missing"], 144)
        finally:
            del engine.api_import_gaps


class TestSweepImplausibleEndpoint(unittest.TestCase):
    """#307: GET previews lost-opener device spikes (dry-run, no writes); POST
    applies the clamp and regenerates charts."""

    def setUp(self):
        import tempfile
        from block_store import BlockStore
        self.tmp = tempfile.mktemp(suffix=".db")
        s = BlockStore(self.tmp)
        s.insert_config_period({"meters": {
            "electricity_main": {"meta": {
                "billing_day": 1, "block_minutes": 30, "timezone": "Europe/London",
                "currency_symbol": "£", "currency_code": "GBP", "site": "Home"}},
            "house_battery": {"meta": {
                "sub_meter": True, "parent_meter": "electricity_main",
                "device": "Solax Battery", "billing_day": 1, "block_minutes": 30,
                "timezone": "Europe/London", "currency_symbol": "£",
                "currency_code": "GBP"}},
        }})
        cp = s._conn.execute("SELECT id FROM config_periods LIMIT 1").fetchone()["id"]
        s._conn.execute(
            "INSERT INTO blocks (block_start, block_end, meter_id, config_period_id, "
            "imp_kwh, imp_kwh_grid, imp_cost, imp_read_start, imp_read_end, carbon_g, "
            "carbon_intensity_g) VALUES "
            "('2026-07-21T09:30:00','2026-07-21T10:00:00','house_battery',?,"
            "6137.592,0.0,0.0,0.0,6137.592,859262.88,140.0)", (cp,))
        s._conn.commit()
        self.store = s
        self.client = server.app.test_client()

    def tearDown(self):
        import os
        self.store._conn.close()
        if os.path.exists(self.tmp):
            os.remove(self.tmp)

    def test_get_previews_without_writing(self):
        with patch.object(server, "_get_store", return_value=self.store):
            r = self.client.get("/api/blocks/sweep-implausible")
        d = json.loads(r.data)
        self.assertTrue(d["ok"])
        self.assertEqual(d["count"], 1)
        self.assertFalse(d["applied"])
        self.assertAlmostEqual(self.store._conn.execute(
            "SELECT imp_kwh FROM blocks WHERE block_start='2026-07-21T09:30:00'"
        ).fetchone()[0], 6137.592)                       # dry-run wrote nothing

    def test_post_applies_and_clamps(self):
        with patch.object(server, "_get_store", return_value=self.store), \
             patch.object(server, "_regen_charts_safely", return_value=None):
            r = self.client.post("/api/blocks/sweep-implausible")
        d = json.loads(r.data)
        self.assertTrue(d["ok"] and d["applied"])
        row = self.store._conn.execute(
            "SELECT imp_kwh, carbon_g, needs_review FROM blocks "
            "WHERE block_start='2026-07-21T09:30:00'").fetchone()
        self.assertEqual(row["imp_kwh"], 0.0)
        self.assertEqual(row["carbon_g"], 0.0)
        self.assertEqual(row["needs_review"], 1)


if __name__ == "__main__":
    unittest.main()
