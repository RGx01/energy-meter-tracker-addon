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
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from block_store import BlockStore, open_block_store

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
sys.modules["block_store"] = bs_mod

# Stub engine (pause/resume only)
eng = types.ModuleType("engine")
eng.pause_engine  = lambda: None
eng.resume_engine = lambda: None
eng.engine_startup = MagicMock()
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
        self.assertTrue(self._registered("config_page"))

    def test_charts_page_registered(self):
        self.assertTrue(self._registered("charts_page"))

    def test_summary_page_registered(self):
        self.assertTrue(self._registered("summary_page"))

    def test_import_page_registered(self):
        self.assertTrue(self._registered("import_page"))

    def test_logs_page_registered(self):
        self.assertTrue(self._registered("logs_page"))

    def test_help_page_registered(self):
        self.assertTrue(self._registered("help_page"))

    def test_api_last_page_registered(self):
        self.assertTrue(self._registered("api_set_last_page"))

    def test_api_blocks_summary_registered(self):
        self.assertTrue(self._registered("api_blocks_summary"))

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


# ─────────────────────────────────────────────────────────────────────────────
# /api/last-page
# ─────────────────────────────────────────────────────────────────────────────

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
        for key in ("currency", "rows", "meters", "export_color"):
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
        self.assertIn("config", r.headers["Location"])

    def test_with_config_redirects_to_charts_by_default(self):
        with patch.object(server, "load_config", return_value=MINIMAL_CONFIG):
            r = self.client.get("/", follow_redirects=False)
        self.assertIn(r.status_code, (301, 302))
        # Default cookie → charts
        self.assertIn("charts", r.headers["Location"])

    def test_with_config_and_summary_cookie_redirects_to_summary(self):
        with patch.object(server, "load_config", return_value=MINIMAL_CONFIG):
            self.client.set_cookie("emt_last_page", "summary")
            r = self.client.get("/", follow_redirects=False)
        self.assertIn(r.status_code, (301, 302))
        self.assertIn("summary", r.headers["Location"])

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


class TestApiConfigHistoryDelete(unittest.TestCase):
    """
    When the active config period is deleted, the server must:
    1. Promote the predecessor to active (effective_to = NULL)
    2. Write the predecessor's full_config_json back to meters_config.json
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
        # Period 1 — older, non-active
        store._conn.execute("""
            INSERT INTO config_periods
            (effective_from, effective_to, billing_day, block_minutes, timezone,
             currency_symbol, currency_code, site_name, change_reason, full_config_json)
            VALUES ('2026-01-01T00:00:00', '2026-03-01T00:00:00', 1, 30,
                    'Europe/London', '£', 'GBP', 'Old Site', NULL, ?)
        """, (json.dumps(cfg_old),))
        # Period 2 — active
        store._conn.execute("""
            INSERT INTO config_periods
            (effective_from, effective_to, billing_day, block_minutes, timezone,
             currency_symbol, currency_code, site_name, change_reason, full_config_json)
            VALUES ('2026-03-01T00:00:00', NULL, 15, 30,
                    'Europe/London', '£', 'GBP', 'New Site', NULL, ?)
        """, (json.dumps(cfg_new),))
        store._conn.commit()
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
        """meters_config.json must be overwritten with the predecessor's full_config_json."""
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

# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    unittest.main(verbosity=2)