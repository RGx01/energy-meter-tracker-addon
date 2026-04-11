"""
test_block_store.py
===================
Unit tests for block_store.py.

All tests use an in-memory SQLite database — no files are written to disk.

Run with:
    python3 -B test_block_store.py
or:
    python3 -m pytest test_block_store.py -v
"""

import json
import sys
import os
import unittest
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(__file__))
from block_store import BlockStore, open_block_store, migrate_json_to_sqlite


# ─────────────────────────────────────────────────────────────────────────────
# Fixtures
# ─────────────────────────────────────────────────────────────────────────────

EXAMPLE_CONFIG = {
    "meters": {
        "electricity_main": {
            "meta": {
                "site": "Test Home",
                "timezone": "Europe/London",
                "billing_day": 1,
                "block_minutes": 30,
                "currency_symbol": "£",
                "currency_code": "GBP",
            },
            "channels": {
                "import": {"sensor": "sensor.import"},
                "export": {"sensor": "sensor.export"},
            },
        }
    }
}

EXAMPLE_CONFIG_WITH_SUB = {
    "meters": {
        "electricity_main": {
            "meta": {
                "site": "Test Home",
                "timezone": "Europe/London",
                "billing_day": 15,
                "block_minutes": 30,
                "currency_symbol": "£",
                "currency_code": "GBP",
            },
            "channels": {
                "import": {"sensor": "sensor.import"},
                "export": {"sensor": "sensor.export"},
            },
        },
        "zappi_ev": {
            "meta": {
                "sub_meter": True,
                "device": "Zappi EV Charger",
                "parent_meter": "electricity_main",
                "block_minutes": 30,
                "timezone": "Europe/London",
                "currency_symbol": "£",
                "currency_code": "GBP",
            },
            "channels": {
                "import": {"sensor": "sensor.zappi"},
            },
        },
    }
}


def make_block(start_iso: str,
               imp_kwh: float = 0.5,
               exp_kwh: float = 0.1,
               standing: float = 0.5,
               interpolated: bool = False,
               meter_id: str = "electricity_main") -> dict:
    """Build a minimal finalised block dict matching the engine output shape."""
    end_dt = datetime.fromisoformat(start_iso) + timedelta(minutes=30)
    end_iso = end_dt.isoformat()
    return {
        "start": start_iso,
        "end":   end_iso,
        "interpolated": interpolated,
        "meters": {
            meter_id: {
                "meta": {
                    "block_minutes":  30,
                    "timezone":       "Europe/London",
                    "billing_day":    1,
                    "currency_symbol":"£",
                    "currency_code":  "GBP",
                    "sub_meter":      False,
                },
                "standing_charge": standing,
                "interpolated":    interpolated,
                "channels": {
                    "import": {
                        "kwh":        imp_kwh,
                        "kwh_remainder": imp_kwh - 0.05,
                        "rate":       0.245,
                        "cost":       round(imp_kwh * 0.245, 4),
                        "cost_remainder": round((imp_kwh - 0.05) * 0.245, 4),
                        "read_start": 1000.0,
                        "read_end":   1000.0 + imp_kwh,
                    },
                    "export": {
                        "kwh":        exp_kwh,
                        "rate":       0.15,
                        "cost":       round(exp_kwh * 0.15, 4),
                        "read_start": 500.0,
                        "read_end":   500.0 + exp_kwh,
                    },
                },
            }
        },
        "totals": {
            "import_kwh":  imp_kwh,
            "import_cost": round(imp_kwh * 0.245, 4),
            "export_kwh":  exp_kwh,
            "export_cost": round(exp_kwh * 0.15, 4),
        },
    }


def make_block_with_sub(start_iso: str) -> dict:
    """Block with main meter + sub-meter."""
    end_iso = (datetime.fromisoformat(start_iso) + timedelta(minutes=30)).isoformat()
    return {
        "start": start_iso,
        "end":   end_iso,
        "interpolated": False,
        "meters": {
            "electricity_main": {
                "meta": {"block_minutes": 30, "timezone": "Europe/London",
                         "billing_day": 15, "currency_symbol": "£",
                         "currency_code": "GBP", "sub_meter": False},
                "standing_charge": 0.5,
                "interpolated": False,
                "channels": {
                    "import": {
                        "kwh": 1.0, "kwh_remainder": 0.7,
                        "rate": 0.245, "cost": 0.245,
                        "cost_remainder": round(0.7 * 0.245, 4),
                        "read_start": 1000.0, "read_end": 1001.0,
                    },
                    "export": {
                        "kwh": 0.1, "rate": 0.15, "cost": 0.015,
                        "read_start": 500.0, "read_end": 500.1,
                    },
                },
            },
            "zappi_ev": {
                "meta": {"block_minutes": 30, "timezone": "Europe/London",
                         "billing_day": 15, "currency_symbol": "£",
                         "currency_code": "GBP", "sub_meter": True},
                "standing_charge": 0.0,
                "interpolated": False,
                "channels": {
                    "import": {
                        "kwh": 0.3, "kwh_grid": 0.3,
                        "rate": 0.245, "cost": round(0.3 * 0.245, 4),
                        "read_start": 200.0, "read_end": 200.3,
                    },
                },
            },
        },
        "totals": {
            "import_kwh": 0.7, "import_cost": round(0.7 * 0.245, 4),
            "export_kwh": 0.1, "export_cost": 0.015,
        },
    }


def new_store() -> BlockStore:
    """Return a fresh in-memory BlockStore."""
    return BlockStore(":memory:")


# ─────────────────────────────────────────────────────────────────────────────
# Tests: schema and setup
# ─────────────────────────────────────────────────────────────────────────────

class TestSchema(unittest.TestCase):

    def test_schema_created(self):
        store = new_store()
        cur = store._conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        )
        tables = {r["name"] for r in cur.fetchall()}
        self.assertIn("blocks", tables)
        self.assertIn("config_periods", tables)
        self.assertIn("meters", tables)
        self.assertIn("reads", tables)
        self.assertIn("store_meta", tables)
        store.close()

    def test_schema_version_recorded(self):
        store = new_store()
        cur = store._conn.execute(
            "SELECT value FROM store_meta WHERE key = 'schema_version'"
        )
        row = cur.fetchone()
        self.assertIsNotNone(row)
        self.assertEqual(row["value"], "1")
        store.close()

    def test_wal_mode(self):
        store = new_store()
        cur = store._conn.execute("PRAGMA journal_mode")
        # in-memory DB may report 'memory' not 'wal' — just check it doesn't error
        self.assertIsNotNone(cur.fetchone())
        store.close()

    def test_foreign_keys_on(self):
        store = new_store()
        cur = store._conn.execute("PRAGMA foreign_keys")
        self.assertEqual(cur.fetchone()[0], 1)
        store.close()

    def test_indexes_created(self):
        store = new_store()
        cur = store._conn.execute(
            "SELECT name FROM sqlite_master WHERE type='index'"
        )
        indexes = {r["name"] for r in cur.fetchall()}
        self.assertIn("idx_blocks_start", indexes)
        self.assertIn("idx_blocks_date", indexes)
        self.assertIn("idx_blocks_ym", indexes)
        self.assertIn("idx_blocks_meter", indexes)
        self.assertIn("idx_reads_captured", indexes)
        store.close()


# ─────────────────────────────────────────────────────────────────────────────
# Tests: config periods
# ─────────────────────────────────────────────────────────────────────────────

class TestConfigPeriods(unittest.TestCase):

    def setUp(self):
        self.store = new_store()

    def tearDown(self):
        self.store.close()

    def test_insert_config_period(self):
        pid = self.store.insert_config_period(EXAMPLE_CONFIG)
        self.assertEqual(pid, 1)

    def test_config_period_fields_extracted(self):
        self.store.insert_config_period(EXAMPLE_CONFIG)
        cp = self.store.get_config_period(1)
        self.assertEqual(cp["billing_day"], 1)
        self.assertEqual(cp["block_minutes"], 30)
        self.assertEqual(cp["timezone"], "Europe/London")
        self.assertEqual(cp["currency_symbol"], "£")
        self.assertEqual(cp["currency_code"], "GBP")
        self.assertEqual(cp["site_name"], "Test Home")
        self.assertIsNone(cp["effective_to"])

    def test_config_from_db_roundtrip(self):
        """config_from_db() should reproduce the original config dict."""
        self.store.insert_config_period(EXAMPLE_CONFIG)
        period_id = self.store.get_current_config_period_id()
        restored = self.store.config_from_db(period_id)
        # Check top-level structure
        self.assertIn("meters", restored)
        self.assertIn("electricity_main", restored["meters"])
        # Check billing scalar fields round-trip
        main_meta = restored["meters"]["electricity_main"]["meta"]
        orig_meta  = EXAMPLE_CONFIG["meters"]["electricity_main"]["meta"]
        self.assertEqual(main_meta["billing_day"],     orig_meta["billing_day"])
        self.assertEqual(main_meta["timezone"],        orig_meta["timezone"])
        self.assertEqual(main_meta["currency_symbol"], orig_meta["currency_symbol"])
        # Check channel sensors
        imp = restored["meters"]["electricity_main"]["channels"]["import"]
        orig_imp = EXAMPLE_CONFIG["meters"]["electricity_main"]["channels"]["import"]
        self.assertEqual(imp.get("read"), orig_imp.get("read"))
        self.assertEqual(imp.get("rate"), orig_imp.get("rate"))

    def test_second_config_period_closes_first(self):
        self.store.insert_config_period(EXAMPLE_CONFIG,
                                        effective_from="2026-01-01T00:00:00")
        config2 = json.loads(json.dumps(EXAMPLE_CONFIG))
        config2["meters"]["electricity_main"]["meta"]["billing_day"] = 15
        self.store.insert_config_period(config2,
                                        effective_from="2026-03-15T00:00:00",
                                        change_reason="Supplier change")
        cp1 = self.store.get_config_period(1)
        cp2 = self.store.get_config_period(2)
        self.assertEqual(cp1["effective_to"], "2026-03-15T00:00:00")
        self.assertIsNone(cp2["effective_to"])
        self.assertEqual(cp2["billing_day"], 15)
        self.assertEqual(cp2["change_reason"], "Supplier change")

    def test_get_current_config_period_id(self):
        self.store.insert_config_period(EXAMPLE_CONFIG)
        pid = self.store.get_current_config_period_id()
        self.assertEqual(pid, 1)

    def test_get_current_config_period_id_after_update(self):
        self.store.insert_config_period(EXAMPLE_CONFIG,
                                        effective_from="2026-01-01T00:00:00")
        config2 = json.loads(json.dumps(EXAMPLE_CONFIG))
        self.store.insert_config_period(config2,
                                        effective_from="2026-03-15T00:00:00")
        self.assertEqual(self.store.get_current_config_period_id(), 2)

    def test_get_config_period_for_date_current(self):
        self.store.insert_config_period(EXAMPLE_CONFIG,
                                        effective_from="2026-01-01T00:00:00")
        cp = self.store.get_config_period_for_date("2026-04-01")
        self.assertIsNotNone(cp)
        self.assertEqual(cp["id"], 1)

    def test_get_config_period_for_date_historical(self):
        self.store.insert_config_period(EXAMPLE_CONFIG,
                                        effective_from="2026-01-01T00:00:00")
        config2 = json.loads(json.dumps(EXAMPLE_CONFIG))
        config2["meters"]["electricity_main"]["meta"]["billing_day"] = 15
        self.store.insert_config_period(config2,
                                        effective_from="2026-03-15T00:00:00")
        # Date before the change
        cp = self.store.get_config_period_for_date("2026-02-01")
        self.assertEqual(cp["id"], 1)
        self.assertEqual(cp["billing_day"], 1)
        # Date after the change
        cp2 = self.store.get_config_period_for_date("2026-04-01")
        self.assertEqual(cp2["id"], 2)
        self.assertEqual(cp2["billing_day"], 15)

    def test_no_config_period_returns_none(self):
        self.assertIsNone(self.store.get_current_config_period_id())
        self.assertIsNone(self.store.get_config_period(999))


# ─────────────────────────────────────────────────────────────────────────────
# Tests: append and read blocks
# ─────────────────────────────────────────────────────────────────────────────

class TestAppendBlock(unittest.TestCase):

    def setUp(self):
        self.store = new_store()
        self.store.insert_config_period(EXAMPLE_CONFIG)

    def tearDown(self):
        self.store.close()

    def test_append_block_inserts_rows(self):
        block = make_block("2026-03-01T00:00:00")
        self.store.append_block(block)
        self.assertEqual(self.store.count_meter_rows(), 1)

    def test_append_block_count(self):
        for i in range(5):
            dt = datetime(2026, 3, 1) + timedelta(minutes=30 * i)
            self.store.append_block(make_block(dt.isoformat()))
        self.assertEqual(self.store.count_blocks(), 5)

    def test_append_block_idempotent(self):
        """INSERT OR IGNORE prevents duplicate meter rows."""
        block = make_block("2026-03-01T00:00:00")
        self.store.append_block(block)
        self.store.append_block(block)
        self.assertEqual(self.store.count_meter_rows(), 1)

    def test_append_block_with_sub_meter(self):
        self.store.insert_config_period(EXAMPLE_CONFIG_WITH_SUB,
                                        effective_from="2026-03-01T00:00:00")
        block = make_block_with_sub("2026-03-01T00:00:00")
        self.store.append_block(block, config_period_id=2)
        self.assertEqual(self.store.count_meter_rows(), 2)

    def test_append_block_no_config_raises(self):
        store2 = new_store()
        with self.assertRaises(RuntimeError):
            store2.append_block(make_block("2026-03-01T00:00:00"))
        store2.close()

    def test_append_blocks_bulk(self):
        blocks = [
            make_block((datetime(2026, 3, 1) + timedelta(minutes=30 * i)).isoformat())
            for i in range(48)
        ]
        inserted = self.store.append_blocks(blocks)
        self.assertEqual(inserted, 48)
        self.assertEqual(self.store.count_blocks(), 48)

    def test_block_fields_stored_correctly(self):
        block = make_block("2026-03-01T06:00:00", imp_kwh=1.234, exp_kwh=0.567)
        self.store.append_block(block)
        blocks = self.store.get_all_blocks()
        self.assertEqual(len(blocks), 1)
        b = blocks[0]
        self.assertEqual(b["start"], "2026-03-01T06:00:00")
        imp = b["meters"]["electricity_main"]["channels"]["import"]
        self.assertAlmostEqual(imp["kwh"], 1.234, places=4)
        exp = b["meters"]["electricity_main"]["channels"]["export"]
        self.assertAlmostEqual(exp["kwh"], 0.567, places=4)

    def test_standing_charge_stored(self):
        block = make_block("2026-03-01T00:00:00", standing=0.9876)
        self.store.append_block(block)
        blocks = self.store.get_all_blocks()
        sc = blocks[0]["meters"]["electricity_main"]["standing_charge"]
        self.assertAlmostEqual(sc, 0.9876, places=4)

    def test_interpolated_flag_stored(self):
        block = make_block("2026-03-01T00:00:00", interpolated=True)
        self.store.append_block(block)
        blocks = self.store.get_all_blocks()
        self.assertTrue(blocks[0]["interpolated"])

    def test_kwh_remainder_stored(self):
        block = make_block("2026-03-01T00:00:00", imp_kwh=1.0)
        self.store.append_block(block)
        blocks = self.store.get_all_blocks()
        imp = blocks[0]["meters"]["electricity_main"]["channels"]["import"]
        self.assertIn("kwh_remainder", imp)
        self.assertAlmostEqual(imp["kwh_remainder"], 0.95, places=4)

    def test_config_fields_joined(self):
        block = make_block("2026-03-01T00:00:00")
        self.store.append_block(block)
        blocks = self.store.get_all_blocks()
        b = blocks[0]
        self.assertEqual(b["_billing_day"], 1)
        self.assertEqual(b["_timezone"], "Europe/London")
        self.assertEqual(b["_currency_symbol"], "£")


# ─────────────────────────────────────────────────────────────────────────────
# Tests: query methods
# ─────────────────────────────────────────────────────────────────────────────

class TestQueryMethods(unittest.TestCase):

    def setUp(self):
        self.store = new_store()
        self.store.insert_config_period(EXAMPLE_CONFIG)
        # Insert 3 days of blocks: 48 blocks per day
        self.base = datetime(2026, 3, 1)
        for i in range(48 * 3):
            dt = self.base + timedelta(minutes=30 * i)
            self.store.append_block(make_block(dt.isoformat()))

    def tearDown(self):
        self.store.close()

    def test_count_blocks(self):
        self.assertEqual(self.store.count_blocks(), 48 * 3)

    def test_get_all_blocks_count(self):
        blocks = self.store.get_all_blocks()
        self.assertEqual(len(blocks), 48 * 3)

    def test_get_all_blocks_ordered(self):
        blocks = self.store.get_all_blocks()
        starts = [b["start"] for b in blocks]
        self.assertEqual(starts, sorted(starts))

    def test_get_last_block(self):
        last = self.store.get_last_block()
        self.assertIsNotNone(last)
        expected = (self.base + timedelta(minutes=30 * (48 * 3 - 1))).isoformat()
        self.assertEqual(last["start"], expected)

    def test_get_last_block_empty(self):
        store2 = new_store()
        store2.insert_config_period(EXAMPLE_CONFIG)
        self.assertIsNone(store2.get_last_block())
        store2.close()

    def test_get_blocks_for_range(self):
        start = datetime(2026, 3, 1)
        end   = datetime(2026, 3, 1, 23, 59, 59)
        blocks = self.store.get_blocks_for_range(start, end)
        self.assertEqual(len(blocks), 48)

    def test_get_blocks_for_range_partial(self):
        start = datetime(2026, 3, 1, 6, 0, 0)
        end   = datetime(2026, 3, 1, 11, 59, 59)
        blocks = self.store.get_blocks_for_range(start, end)
        self.assertEqual(len(blocks), 12)  # 6 hours * 2 blocks/hour

    def test_get_blocks_for_range_meter_filter(self):
        blocks = self.store.get_blocks_for_range(
            datetime(2026, 3, 1), datetime(2026, 3, 3, 23, 59),
            meter_id="electricity_main"
        )
        self.assertEqual(len(blocks), 48 * 3)

    def test_get_blocks_for_range_meter_filter_no_match(self):
        blocks = self.store.get_blocks_for_range(
            datetime(2026, 3, 1), datetime(2026, 3, 3, 23, 59),
            meter_id="nonexistent"
        )
        self.assertEqual(len(blocks), 0)

    def test_get_blocks_for_date(self):
        blocks = self.store.get_blocks_for_date("2026-03-02")
        self.assertEqual(len(blocks), 48)

    def test_get_blocks_for_date_no_match(self):
        blocks = self.store.get_blocks_for_date("2026-04-01")
        self.assertEqual(len(blocks), 0)

    def test_get_blocks_for_month(self):
        blocks = self.store.get_blocks_for_month(2026, 3)
        self.assertEqual(len(blocks), 48 * 3)

    def test_get_blocks_for_month_no_match(self):
        blocks = self.store.get_blocks_for_month(2025, 1)
        self.assertEqual(len(blocks), 0)

    def test_get_local_dates(self):
        dates = self.store.get_local_dates()
        self.assertEqual(len(dates), 3)
        self.assertIn("2026-03-01", dates)
        self.assertIn("2026-03-02", dates)
        self.assertIn("2026-03-03", dates)

    def test_get_local_dates_ordered(self):
        dates = self.store.get_local_dates()
        self.assertEqual(dates, sorted(dates))


# ─────────────────────────────────────────────────────────────────────────────
# Tests: block reconstruction fidelity
# ─────────────────────────────────────────────────────────────────────────────

class TestBlockFidelity(unittest.TestCase):
    """Verify round-trip fidelity: block dict in == block dict out."""

    def setUp(self):
        self.store = new_store()
        self.store.insert_config_period(EXAMPLE_CONFIG)

    def tearDown(self):
        self.store.close()

    def test_start_end_preserved(self):
        block = make_block("2026-03-15T12:30:00")
        self.store.append_block(block)
        out = self.store.get_all_blocks()[0]
        self.assertEqual(out["start"], "2026-03-15T12:30:00")
        self.assertEqual(out["end"],   "2026-03-15T13:00:00")

    def test_import_kwh_preserved(self):
        block = make_block("2026-03-01T00:00:00", imp_kwh=3.14159)
        self.store.append_block(block)
        out = self.store.get_all_blocks()[0]
        imp = out["meters"]["electricity_main"]["channels"]["import"]
        self.assertAlmostEqual(imp["kwh"], 3.14159, places=4)

    def test_export_kwh_preserved(self):
        block = make_block("2026-03-01T00:00:00", exp_kwh=2.71828)
        self.store.append_block(block)
        out = self.store.get_all_blocks()[0]
        exp = out["meters"]["electricity_main"]["channels"]["export"]
        self.assertAlmostEqual(exp["kwh"], 2.71828, places=4)

    def test_rate_preserved(self):
        block = make_block("2026-03-01T00:00:00")
        self.store.append_block(block)
        out = self.store.get_all_blocks()[0]
        imp = out["meters"]["electricity_main"]["channels"]["import"]
        self.assertAlmostEqual(imp["rate"], 0.245, places=4)

    def test_read_start_end_preserved(self):
        block = make_block("2026-03-01T00:00:00", imp_kwh=0.5)
        self.store.append_block(block)
        out = self.store.get_all_blocks()[0]
        imp = out["meters"]["electricity_main"]["channels"]["import"]
        self.assertAlmostEqual(imp["read_start"], 1000.0, places=4)
        self.assertAlmostEqual(imp["read_end"],   1000.5, places=4)

    def test_sub_meter_round_trip(self):
        self.store.insert_config_period(EXAMPLE_CONFIG_WITH_SUB,
                                        effective_from="2026-03-01T00:00:00")
        block = make_block_with_sub("2026-03-01T00:00:00")
        self.store.append_block(block, config_period_id=2)
        out = self.store.get_all_blocks()
        # Should have 1 block with 2 meters
        self.assertEqual(len(out), 1)
        meters = out[0]["meters"]
        self.assertIn("electricity_main", meters)
        self.assertIn("zappi_ev", meters)

    def test_sub_meter_kwh_grid(self):
        self.store.insert_config_period(EXAMPLE_CONFIG_WITH_SUB,
                                        effective_from="2026-03-01T00:00:00")
        block = make_block_with_sub("2026-03-01T00:00:00")
        self.store.append_block(block, config_period_id=2)
        out = self.store.get_all_blocks()
        zappi_imp = out[0]["meters"]["zappi_ev"]["channels"]["import"]
        self.assertAlmostEqual(zappi_imp["kwh_grid"], 0.3, places=4)

    def test_supplier_and_v2x_round_trip(self):
        """
        supplier survives via config_periods (historical record per billing period).
        v2x_capable survives via meters table (per-meter property).
        """
        cfg = {"meters": {
            "electricity_main": {"meta": {
                "billing_day": 1, "block_minutes": 30, "timezone": "UTC",
                "currency_symbol": "£", "currency_code": "GBP",
                "supplier": "Octopus Energy",
                "v2x_capable": False,
            }, "channels": {"import": {"read": "s.imp", "rate": "s.rate"}}},
            "ev_charger": {"meta": {
                "sub_meter": True, "parent_meter": "electricity_main",
                "v2x_capable": True,
            }, "channels": {"import": {"read": "s.ev", "rate": "s.rate"}}},
        }}
        store2 = new_store()
        store2.insert_config_period(cfg)
        pid = store2.get_current_config_period_id()
        out = store2.config_from_db(pid)
        store2.close()

        # supplier comes from config_periods — available on main meter meta
        self.assertEqual(out["meters"]["electricity_main"]["meta"].get("supplier"),
                         "Octopus Energy",
                         "supplier must be stored on config_periods and returned in main meter meta")
        # v2x_capable comes from meters table
        self.assertFalse(out["meters"]["electricity_main"]["meta"].get("v2x_capable", False))
        self.assertTrue(out["meters"]["ev_charger"]["meta"].get("v2x_capable"),
                        "v2x_capable must be stored on meters table")

    def test_sub_meter_meta_flags_in_retrieved_blocks(self):
        """
        Blocks retrieved from DB must include sub_meter, parent_meter, device
        in meter.meta — the charts and billing rely on these to identify
        sub-meters. This requires _select_blocks to JOIN the meters table.
        """
        self.store.insert_config_period(EXAMPLE_CONFIG_WITH_SUB,
                                        effective_from="2026-03-01T00:00:00")
        block = make_block_with_sub("2026-03-01T00:00:00")
        self.store.append_block(block, config_period_id=2)
        out = self.store.get_all_blocks()
        meters = out[0]["meters"]

        # Main meter: sub_meter must be absent or False
        main_meta = meters["electricity_main"]["meta"]
        self.assertFalse(main_meta.get("sub_meter", False),
            "Main meter must not be flagged as sub_meter")

        # Sub-meter: must have sub_meter=True, parent_meter, device
        zappi_meta = meters["zappi_ev"]["meta"]
        self.assertTrue(zappi_meta.get("sub_meter"),
            "zappi_ev must be flagged as sub_meter in retrieved block meta")
        self.assertEqual(zappi_meta.get("parent_meter"), "electricity_main",
            "parent_meter must be populated from meters table")
        self.assertEqual(zappi_meta.get("device"), "Zappi EV Charger",
            "device label must be populated from meters table")

    def test_main_meter_no_sub_meter_flag(self):
        """Main meter without sub-meters must not have sub_meter in meta."""
        out = self.store.get_all_blocks()
        if out:
            main_meta = out[0]["meters"]["electricity_main"]["meta"]
            self.assertFalse(main_meta.get("sub_meter", False))


# ─────────────────────────────────────────────────────────────────────────────
# Tests: local date calculation
# ─────────────────────────────────────────────────────────────────────────────

class TestLocalDate(unittest.TestCase):
    """Verify UTC timestamps are converted to the correct local date."""

    def setUp(self):
        self.store = new_store()
        self.store.insert_config_period(EXAMPLE_CONFIG)

    def tearDown(self):
        self.store.close()

    def test_midnight_utc_is_correct_london_date(self):
        # 2026-03-15 00:00:00 UTC = 2026-03-15 00:00:00 GMT (no DST yet)
        block = make_block("2026-03-15T00:00:00")
        self.store.append_block(block)
        dates = self.store.get_local_dates()
        self.assertIn("2026-03-15", dates)

    def test_late_utc_before_dst_is_same_date(self):
        # 2026-03-14 23:30:00 UTC = 2026-03-14 23:30:00 GMT
        block = make_block("2026-03-14T23:30:00")
        self.store.append_block(block)
        dates = self.store.get_local_dates()
        self.assertIn("2026-03-14", dates)

    def test_blocks_for_date_uses_local_not_utc(self):
        # 2026-03-29 00:30 UTC = 01:30 BST (BST starts at 01:00 UTC on 29th)
        # So local London date is 2026-03-29, not 2026-03-28
        block = make_block("2026-03-29T00:30:00")
        self.store.append_block(block)
        blocks_29 = self.store.get_blocks_for_date("2026-03-29")
        blocks_28 = self.store.get_blocks_for_date("2026-03-28")
        self.assertEqual(len(blocks_29), 1)
        self.assertEqual(len(blocks_28), 0)


# ─────────────────────────────────────────────────────────────────────────────
# Tests: reads (Phase 2+ table, Phase 1 just verifies table exists and works)
# ─────────────────────────────────────────────────────────────────────────────

class TestReads(unittest.TestCase):

    def setUp(self):
        self.store = new_store()
        self.store.insert_config_period(EXAMPLE_CONFIG)

    def tearDown(self):
        self.store.close()

    def test_insert_read(self):
        self.store.insert_read(
            meter_id="electricity_main",
            channel="import",
            captured_at="2026-03-01T06:00:00",
            reading_kwh=1000.5,
            rate=0.245,
        )
        reads = self.store.get_reads_for_range(
            datetime(2026, 3, 1), datetime(2026, 3, 1, 23, 59)
        )
        self.assertEqual(len(reads), 1)
        self.assertAlmostEqual(reads[0]["reading_kwh"], 1000.5, places=4)

    def test_read_block_id_initially_null(self):
        self.store.insert_read(
            meter_id="electricity_main",
            channel="import",
            captured_at="2026-03-01T06:00:00",
            reading_kwh=1000.5,
        )
        reads = self.store.get_reads_for_range(
            datetime(2026, 3, 1), datetime(2026, 3, 2)
        )
        self.assertIsNone(reads[0]["block_id"])

    def test_link_reads_to_block(self):
        # Insert a block and some reads
        self.store.append_block(make_block("2026-03-01T06:00:00"))
        for i in range(6):
            ts = (datetime(2026, 3, 1, 6, 0) + timedelta(minutes=5 * i)).isoformat()
            self.store.insert_read("electricity_main", "import", ts, 1000.0 + i * 0.1)

        # Get the block's DB id
        cur = self.store._conn.execute(
            "SELECT id FROM blocks WHERE block_start = '2026-03-01T06:00:00'"
        )
        block_db_id = cur.fetchone()["id"]

        linked = self.store.link_reads_to_block(
            block_start="2026-03-01T06:00:00",
            block_end="2026-03-01T06:30:00",
            block_id=block_db_id,
        )
        self.assertEqual(linked, 6)

        reads = self.store.get_reads_for_block(block_db_id)
        self.assertEqual(len(reads), 6)
        for r in reads:
            self.assertEqual(r["block_id"], block_db_id)

    def test_get_reads_for_range_meter_filter(self):
        self.store.insert_read("electricity_main", "import",
                               "2026-03-01T06:00:00", 1000.0)
        self.store.insert_read("zappi_ev", "import",
                               "2026-03-01T06:00:00", 200.0)
        reads = self.store.get_reads_for_range(
            datetime(2026, 3, 1), datetime(2026, 3, 2),
            meter_id="electricity_main"
        )
        self.assertEqual(len(reads), 1)
        self.assertEqual(reads[0]["meter_id"], "electricity_main")

    def test_purge_reads_older_than(self):
        from datetime import timezone
        # Insert an old read
        old_ts = "2020-01-01T00:00:00"
        self.store.insert_read("electricity_main", "import", old_ts, 1000.0)
        # Insert a recent read
        recent_ts = datetime.now(timezone.utc).replace(tzinfo=None).isoformat()
        self.store.insert_read("electricity_main", "import", recent_ts, 1001.0)

        deleted = self.store.purge_reads_older_than(days=30)
        self.assertEqual(deleted, 1)

        reads = self.store.get_reads_for_range(
            datetime(2019, 1, 1), datetime(2021, 1, 1)
        )
        self.assertEqual(len(reads), 0)


# ─────────────────────────────────────────────────────────────────────────────
# Tests: migration
# ─────────────────────────────────────────────────────────────────────────────

class TestMigration(unittest.TestCase):

    def _make_blocks_json(self, n: int, tmp_path: str) -> str:
        """Write n blocks to a temp JSON file and return the path."""
        blocks = []
        for i in range(n):
            dt = datetime(2026, 1, 1) + timedelta(minutes=30 * i)
            blocks.append(make_block(dt.isoformat()))
        path = tmp_path
        with open(path, "w") as f:
            json.dump(blocks, f)
        return path

    def setUp(self):
        self.store = new_store()
        import tempfile
        self.tmp = tempfile.mktemp(suffix=".json")

    def tearDown(self):
        self.store.close()
        try:
            os.remove(self.tmp)
        except Exception:
            pass

    def test_migrate_creates_config_period(self):
        self._make_blocks_json(10, self.tmp)
        migrate_json_to_sqlite(self.tmp, self.store, EXAMPLE_CONFIG)
        self.assertIsNotNone(self.store.get_current_config_period_id())

    def test_migrate_inserts_all_blocks(self):
        self._make_blocks_json(48, self.tmp)
        count = migrate_json_to_sqlite(self.tmp, self.store, EXAMPLE_CONFIG)
        self.assertEqual(count, 48)
        self.assertEqual(self.store.count_blocks(), 48)

    def test_migrate_effective_from_is_oldest_block(self):
        blocks = [make_block("2025-06-15T00:00:00"),
                  make_block("2025-06-16T00:00:00"),
                  make_block("2025-06-14T00:00:00")]
        with open(self.tmp, "w") as f:
            json.dump(blocks, f)
        migrate_json_to_sqlite(self.tmp, self.store, EXAMPLE_CONFIG)
        cp = self.store.get_config_period(1)
        # effective_from is snapped to midnight in Europe/London (UTC+1 BST)
        # so 2025-06-14 00:00 London = 2025-06-13T23:00:00 UTC
        self.assertIn(cp["effective_from"], ["2025-06-13T23:00:00", "2025-06-14T00:00:00"])

    def test_migrate_idempotent(self):
        """Running migration twice should not duplicate blocks."""
        self._make_blocks_json(10, self.tmp)
        migrate_json_to_sqlite(self.tmp, self.store, EXAMPLE_CONFIG)
        # Second run: because effective_from will be set to the same oldest block,
        # but config_period will be a new row. Blocks use INSERT OR IGNORE.
        migrate_json_to_sqlite(self.tmp, self.store, EXAMPLE_CONFIG)
        # Still 10 unique block_start values
        self.assertEqual(self.store.count_blocks(), 10)

    def test_migrate_missing_file(self):
        count = migrate_json_to_sqlite("/nonexistent/path.json",
                                       self.store, EXAMPLE_CONFIG)
        self.assertEqual(count, 0)

    def test_migrate_empty_blocks(self):
        with open(self.tmp, "w") as f:
            json.dump([], f)
        count = migrate_json_to_sqlite(self.tmp, self.store, EXAMPLE_CONFIG)
        self.assertEqual(count, 0)
        self.assertEqual(self.store.count_blocks(), 0)

    def test_migrate_change_reason_recorded(self):
        self._make_blocks_json(5, self.tmp)
        migrate_json_to_sqlite(self.tmp, self.store, EXAMPLE_CONFIG)
        cp = self.store.get_config_period(1)
        self.assertEqual(cp["change_reason"], "Migrated from blocks.json")

    def test_migrate_billing_day_extracted(self):
        config = json.loads(json.dumps(EXAMPLE_CONFIG))
        config["meters"]["electricity_main"]["meta"]["billing_day"] = 15
        self._make_blocks_json(5, self.tmp)
        migrate_json_to_sqlite(self.tmp, self.store, config)
        cp = self.store.get_config_period(1)
        self.assertEqual(cp["billing_day"], 15)


# ─────────────────────────────────────────────────────────────────────────────
# Tests: backup
# ─────────────────────────────────────────────────────────────────────────────

class TestBackup(unittest.TestCase):

    def setUp(self):
        import tempfile
        self.tmp_db  = tempfile.mktemp(suffix=".db")
        self.tmp_bak = tempfile.mktemp(suffix=".db")
        self.store = open_block_store(self.tmp_db)
        self.store.insert_config_period(EXAMPLE_CONFIG)
        for i in range(5):
            dt = datetime(2026, 3, 1) + timedelta(minutes=30 * i)
            self.store.append_block(make_block(dt.isoformat()))

    def tearDown(self):
        self.store.close()
        for p in (self.tmp_db, self.tmp_bak):
            try:
                os.remove(p)
            except Exception:
                pass

    def test_backup_creates_file(self):
        self.store.backup(self.tmp_bak)
        self.assertTrue(os.path.exists(self.tmp_bak))

    def test_backup_has_same_block_count(self):
        self.store.backup(self.tmp_bak)
        bak = open_block_store(self.tmp_bak)
        self.assertEqual(bak.count_blocks(), 5)
        bak.close()

    def test_backup_has_config_periods(self):
        self.store.backup(self.tmp_bak)
        bak = open_block_store(self.tmp_bak)
        self.assertIsNotNone(bak.get_current_config_period_id())
        bak.close()


# ─────────────────────────────────────────────────────────────────────────────
# Runner
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    loader = unittest.TestLoader()
    suite  = unittest.TestSuite()
    for cls in [
        TestSchema,
        TestConfigPeriods,
        TestAppendBlock,
        TestQueryMethods,
        TestBlockFidelity,
        TestLocalDate,
        TestReads,
        TestMigration,
        TestBackup,
    ]:
        suite.addTests(loader.loadTestsFromTestCase(cls))

    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    sys.exit(0 if result.wasSuccessful() else 1)


class TestBillingTotalsVsBlockMethod(unittest.TestCase):
    """Verify get_billing_totals_for_range matches calculate_billing_summary_for_period."""

    def setUp(self):
        import sys, types
        eio = types.ModuleType("energy_engine_io")
        eio.load_json = lambda *a, **kw: {}
        sys.modules.setdefault("energy_engine_io", eio)
        import energy_charts as ec
        self.ec = ec

        self.store = BlockStore(":memory:")
        self.store.insert_config_period({"meters": {"electricity_main": {"meta": {
            "billing_day": 3, "block_minutes": 30, "timezone": "Europe/London",
            "currency_symbol": "£", "currency_code": "GBP", "site": "Home",
        }}}})
        self.cp_id = self.store._conn.execute(
            "SELECT id FROM config_periods LIMIT 1"
        ).fetchone()["id"]

    def _insert_block(self, block_start_iso, imp_kwh, imp_cost, exp_kwh, exp_cost, standing):
        from datetime import datetime
        from zoneinfo import ZoneInfo
        tz = ZoneInfo("Europe/London")
        bs = datetime.fromisoformat(block_start_iso)
        local_date = bs.replace(tzinfo=ZoneInfo("UTC")).astimezone(tz).date().isoformat()
        self.store._conn.execute("""
            INSERT INTO blocks
            (block_start, block_end, local_date, local_year, local_month, local_day,
             meter_id, config_period_id, interpolated,
             imp_kwh, imp_kwh_grid, imp_kwh_remainder,
             imp_rate, imp_cost, imp_cost_remainder,
             imp_read_start, imp_read_end,
             exp_kwh, exp_rate, exp_cost,
             exp_read_start, exp_read_end, standing_charge)
            VALUES (?,?,?,?,?,?,?,?,?, ?,NULL,NULL, NULL,?,NULL, NULL,NULL, ?,NULL,?, NULL,NULL,?)
        """, (
            block_start_iso, block_start_iso, local_date,
            int(local_date[:4]), int(local_date[5:7]), int(local_date[8:10]),
            "electricity_main", self.cp_id, 0,
            imp_kwh, imp_cost, exp_kwh, exp_cost, standing
        ))
        self.store._conn.commit()

    def test_totals_match_block_method(self):
        """SQL aggregation should match calculate_billing_summary_for_period."""
        from datetime import datetime
        # Insert 3 days of blocks (2 blocks per day, UTC times)
        # Jan 1: 00:00 UTC and 00:30 UTC (= Jan 1 BST since UTC=BST in Jan)
        blocks_data = [
            # (block_start UTC, imp_kwh, imp_cost, exp_kwh, exp_cost, standing)
            ("2026-03-01T00:00:00", 1.0, 0.245, 0.0, 0.0, 0.50),
            ("2026-03-01T00:30:00", 1.2, 0.294, 0.0, 0.0, 0.50),
            ("2026-03-02T00:00:00", 0.8, 0.196, 0.2, 0.03, 0.50),
            ("2026-03-02T00:30:00", 0.9, 0.220, 0.0, 0.0,  0.50),
            ("2026-03-03T00:00:00", 1.1, 0.270, 0.0, 0.0,  0.50),
            ("2026-03-03T00:30:00", 0.7, 0.172, 0.3, 0.045, 0.50),
        ]
        for bd in blocks_data:
            self._insert_block(*bd)

        start = datetime(2026, 3, 1, 0, 0, 0)
        end   = datetime(2026, 3, 4, 0, 0, 0)

        # SQL method
        sql_t = self.store.get_billing_totals_for_range(start, end)

        # Expected from manual calculation
        self.assertAlmostEqual(sql_t["imp_kwh"],  1.0+1.2+0.8+0.9+1.1+0.7, places=3)
        self.assertAlmostEqual(sql_t["imp_cost"], 0.245+0.294+0.196+0.220+0.270+0.172, places=3)
        self.assertAlmostEqual(sql_t["exp_kwh"],  0.0+0.0+0.2+0.0+0.0+0.3, places=3)
        self.assertAlmostEqual(sql_t["exp_cost"], 0.0+0.0+0.03+0.0+0.0+0.045, places=3)
        # Standing charge: 0.50 per day × 3 days = 1.50 (NOT 6 × 0.50 = 3.00)
        self.assertAlmostEqual(sql_t["standing"], 1.50, places=3,
                               msg="Standing charge should be summed once per local day")

    def test_standing_charge_bst_boundary(self):
        """Block at 23:00 UTC = 00:00 BST next day should count for the BST day."""
        from datetime import datetime
        # After BST starts (after March 29 2026 01:00 UTC), UTC+1 applies.
        # Block at 2026-04-01T23:00:00 UTC = 2026-04-02 00:00:00 BST (next local day)
        # Block at 2026-04-02T00:00:00 UTC = 2026-04-02 01:00:00 BST (same local day)
        # Both are on local date 2026-04-02 → standing should be counted once
        self._insert_block("2026-04-01T23:00:00", 1.0, 0.245, 0.0, 0.0, 0.60)
        self._insert_block("2026-04-02T00:00:00", 0.5, 0.122, 0.0, 0.0, 0.60)

        # Test get_billing_totals_for_local_date_range (local_date-based, correct)
        sql_t = self.store.get_billing_totals_for_local_date_range("2026-04-02", "2026-04-02")
        self.assertAlmostEqual(sql_t["standing"], 0.60, places=3,
                               msg="Blocks crossing UTC midnight but same BST day should count once")
        self.assertAlmostEqual(sql_t["imp_kwh"], 1.5, places=3,
                               msg="Both BST blocks should be included in local_date range")

    def test_bst_block_included_in_local_date_range(self):
        """get_blocks_for_local_date_range includes 23:xx UTC blocks for the next local day."""
        self._insert_block("2026-04-01T23:00:00", 2.0, 0.490, 0.0, 0.0, 0.50)  # local_date Apr 2
        self._insert_block("2026-04-02T00:00:00", 1.0, 0.245, 0.0, 0.0, 0.50)  # local_date Apr 2

        # local_date range Apr 2 only → should get BOTH blocks (1st is 23:00 UTC Apr 1)
        blocks = self.store.get_blocks_for_local_date_range("2026-04-02", "2026-04-02")
        self.assertEqual(len(blocks), 2,
                         "get_blocks_for_local_date_range should include 23:00 UTC block via local_date")

        # block_start range Apr 2 00:00 → would miss the 23:00 UTC block
        from datetime import datetime
        blocks_utc = self.store.get_blocks_for_range(datetime(2026,4,2,0,0,0), datetime(2026,4,2,23,59,59))
        self.assertEqual(len(blocks_utc), 1,
                         "get_blocks_for_range misses the 23:00 UTC block (known limitation)")


if __name__ == "__main__":
    unittest.main()


class TestCurrentBlock(unittest.TestCase):
    """Tests for save_current_block / load_current_block / clear_current_block."""

    def setUp(self):
        import sys, types
        eio = types.ModuleType("energy_engine_io"); eio.load_json = lambda *a,**kw: {}
        sys.modules.setdefault("energy_engine_io", eio)
        self.store = BlockStore(":memory:")

    def _make_block(self, start="2026-04-05T00:00:00", end="2026-04-05T00:30:00"):
        return {
            "start": start, "end": end,
            "interpolated": False,
            "_last_checkpoint": "2026-04-05T00:10:00",
            "meters": {
                "electricity_main": {
                    "meta": {},
                    "standing_charge": 0.5046,
                    "channels": {
                        "import": {
                            "reads": [
                                {"ts": "2026-04-05T00:00:00", "value": 28000.0},
                                {"ts": "2026-04-05T00:10:00", "value": 28000.5},
                            ],
                            "rates": [
                                {"ts": "2026-04-05T00:00:00", "value": 0.245},
                            ],
                        },
                        "export": {
                            "reads": [{"ts": "2026-04-05T00:00:00", "value": 10000.0}],
                            "rates": [{"ts": "2026-04-05T00:00:00", "value": 0.0}],
                        },
                    },
                }
            },
        }

    def test_save_and_load_roundtrip(self):
        block = self._make_block()
        self.store.save_current_block(block)
        loaded = self.store.load_current_block()

        self.assertEqual(loaded["start"], block["start"])
        self.assertEqual(loaded["end"], block["end"])
        self.assertEqual(loaded["_last_checkpoint"], block["_last_checkpoint"])
        self.assertFalse(loaded["interpolated"])

    def test_reads_roundtrip(self):
        block = self._make_block()
        self.store.save_current_block(block)
        loaded = self.store.load_current_block()

        imp_reads = loaded["meters"]["electricity_main"]["channels"]["import"]["reads"]
        self.assertEqual(len(imp_reads), 2)
        self.assertAlmostEqual(imp_reads[0]["value"], 28000.0, places=3)
        self.assertAlmostEqual(imp_reads[1]["value"], 28000.5, places=3)

    def test_rates_roundtrip(self):
        block = self._make_block()
        self.store.save_current_block(block)
        loaded = self.store.load_current_block()

        imp_rates = loaded["meters"]["electricity_main"]["channels"]["import"]["rates"]
        self.assertEqual(len(imp_rates), 1)
        self.assertAlmostEqual(imp_rates[0]["value"], 0.245, places=4)

    def test_standing_charge_roundtrip(self):
        block = self._make_block()
        self.store.save_current_block(block)
        loaded = self.store.load_current_block()

        sc = loaded["meters"]["electricity_main"]["standing_charge"]
        self.assertAlmostEqual(sc, 0.5046, places=4)

    def test_gap_marker_roundtrip(self):
        """Gap marker stored as gap_detected_at + is_gap_seed rows, not a JSON blob."""
        block = self._make_block()
        block["_gap_marker"] = {
            "detected_at": "2026-04-05T00:05:00",
            "pre_reads": {
                "electricity_main": {"import": {"ts": "2026-04-04T23:55:00", "value": 27999.9}}
            },
            "last_known_rates": {
                "electricity_main": {"import": {"ts": "2026-04-04T23:55:00", "value": 0.245}}
            },
        }
        self.store.save_current_block(block)

        # Verify storage is relational — gap_detected_at column, not a blob
        row = self.store._conn.execute(
            "SELECT gap_detected_at FROM current_block WHERE id=1"
        ).fetchone()
        self.assertEqual(row["gap_detected_at"], "2026-04-05T00:05:00",
                         "gap_detected_at must be stored as a column, not a JSON blob")
        # Verify gap_marker blob column no longer exists — schema is fully relational
        cols = [r[1] for r in self.store._conn.execute(
            "PRAGMA table_info(current_block)"
        ).fetchall()]
        self.assertNotIn("gap_marker", cols,
            "gap_marker blob must not exist — gap state stored as gap_detected_at + is_gap_seed rows")

        # Verify gap seed rows exist
        seed_rows = self.store._conn.execute(
            "SELECT * FROM current_reads WHERE is_gap_seed > 0"
        ).fetchall()
        self.assertGreater(len(seed_rows), 0, "Gap seed rows must be stored in current_reads")

        # Verify full roundtrip
        loaded = self.store.load_current_block()
        self.assertIn("_gap_marker", loaded)
        self.assertEqual(loaded["_gap_marker"]["detected_at"], "2026-04-05T00:05:00")
        pre = loaded["_gap_marker"]["pre_reads"]
        self.assertAlmostEqual(
            pre["electricity_main"]["import"]["value"], 27999.9, places=3
        )

    def test_no_gap_marker_absent(self):
        block = self._make_block()
        self.store.save_current_block(block)
        loaded = self.store.load_current_block()
        self.assertNotIn("_gap_marker", loaded)
        # Verify gap_detected_at is NULL
        row = self.store._conn.execute(
            "SELECT gap_detected_at FROM current_block WHERE id=1"
        ).fetchone()
        self.assertIsNone(row["gap_detected_at"])

    def test_save_overwrites_previous(self):
        block1 = self._make_block(start="2026-04-05T00:00:00")
        block2 = self._make_block(start="2026-04-05T00:30:00", end="2026-04-05T01:00:00")
        self.store.save_current_block(block1)
        self.store.save_current_block(block2)
        loaded = self.store.load_current_block()
        self.assertEqual(loaded["start"], "2026-04-05T00:30:00")

    def test_save_replaces_reads(self):
        """Each save replaces all reads — no accumulation across saves."""
        block1 = self._make_block()
        self.store.save_current_block(block1)
        block2 = self._make_block()
        block2["meters"]["electricity_main"]["channels"]["import"]["reads"] = [
            {"ts": "2026-04-05T00:25:00", "value": 28001.0}
        ]
        self.store.save_current_block(block2)
        loaded = self.store.load_current_block()
        imp_reads = loaded["meters"]["electricity_main"]["channels"]["import"]["reads"]
        self.assertEqual(len(imp_reads), 1)
        self.assertAlmostEqual(imp_reads[0]["value"], 28001.0, places=3)

    def test_load_empty_returns_empty_dict(self):
        loaded = self.store.load_current_block()
        self.assertEqual(loaded, {})

    def test_clear_removes_state(self):
        self.store.save_current_block(self._make_block())
        self.store.clear_current_block()
        loaded = self.store.load_current_block()
        self.assertEqual(loaded, {})

    def test_get_cumulative_totals_empty(self):
        totals = self.store.get_cumulative_totals()
        self.assertEqual(totals["import_kwh"], 0.0)
        self.assertEqual(totals["export_kwh"], 0.0)

    def test_get_cumulative_totals_no_sub_meters(self):
        """Without sub-meters, totals equal direct SUM of main meter blocks."""
        self.store.insert_config_period({"meters": {"electricity_main": {"meta": {
            "billing_day": 1, "block_minutes": 30, "timezone": "UTC",
            "currency_symbol": "£", "currency_code": "GBP",
        }}}})
        cp_id = self.store._conn.execute(
            "SELECT id FROM config_periods LIMIT 1"
        ).fetchone()["id"]
        self.store._conn.execute("""
            INSERT INTO blocks (block_start, block_end, local_date, local_year,
            local_month, local_day, meter_id, config_period_id, interpolated,
            imp_kwh, imp_cost, exp_kwh, exp_cost, standing_charge)
            VALUES ('2026-01-01T00:00:00','2026-01-01T00:30:00','2026-01-01',
            2026,1,1,'electricity_main',?,0, 1.5,0.368, 0.3,0.024, 0.5)
        """, (cp_id,))
        self.store._conn.execute("""
            INSERT INTO blocks (block_start, block_end, local_date, local_year,
            local_month, local_day, meter_id, config_period_id, interpolated,
            imp_kwh, imp_cost, exp_kwh, exp_cost, standing_charge)
            VALUES ('2026-01-01T00:30:00','2026-01-01T01:00:00','2026-01-01',
            2026,1,1,'electricity_main',?,0, 2.0,0.490, 0.0,0.0, 0.5)
        """, (cp_id,))
        self.store._conn.commit()

        totals = self.store.get_cumulative_totals()
        self.assertAlmostEqual(totals["import_kwh"],  3.5,   places=4)
        self.assertAlmostEqual(totals["import_cost"], 0.858, places=4)
        self.assertAlmostEqual(totals["export_kwh"],  0.3,   places=4)
        self.assertAlmostEqual(totals["export_cost"], 0.024, places=4)

    def test_billing_totals_no_double_counting(self):
        """
        get_billing_totals_for_local_date_range must not double-count sub-meters.
        electricity_main.imp_kwh already includes sub-meter consumption.
        """
        cfg = {"meters": {
            "electricity_main": {"meta": {
                "billing_day": 1, "block_minutes": 30, "timezone": "UTC",
                "currency_symbol": "£", "currency_code": "GBP",
            }, "channels": {
                "import": {"read": "sensor.main", "rate": "sensor.rate"},
                "export": {"read": "sensor.exp",  "rate": "sensor.exprate"},
            }},
            "ev_charger": {"meta": {
                "sub_meter": True, "parent_meter": "electricity_main",
            }, "channels": {
                "import": {"read": "sensor.ev", "rate": "sensor.rate"},
            }},
        }}
        self.store.insert_config_period(cfg)
        cp_id = self.store._conn.execute(
            "SELECT id FROM config_periods LIMIT 1"
        ).fetchone()["id"]

        # main: 3.0 kWh total, remainder=1.0 (house), cost=0.735
        # ev:   2.0 kWh, all from grid, no independent cost
        self.store._conn.execute("""
            INSERT INTO blocks (block_start, block_end, local_date, local_year,
            local_month, local_day, meter_id, config_period_id, interpolated,
            imp_kwh, imp_kwh_remainder, imp_cost, exp_kwh, exp_cost, standing_charge)
            VALUES ('2026-01-01T00:00:00','2026-01-01T00:30:00','2026-01-01',
            2026,1,1,'electricity_main',?,0, 3.0,1.0,0.735, 0.2,0.024, 0.5)
        """, (cp_id,))
        self.store._conn.execute("""
            INSERT INTO blocks (block_start, block_end, local_date, local_year,
            local_month, local_day, meter_id, config_period_id, interpolated,
            imp_kwh, imp_kwh_grid, imp_cost, exp_kwh, exp_cost, standing_charge)
            VALUES ('2026-01-01T00:00:00','2026-01-01T00:30:00','2026-01-01',
            2026,1,1,'ev_charger',?,0, 2.0,2.0,0.0, 0.0,0.0, 0.0)
        """, (cp_id,))
        self.store._conn.commit()

        t = self.store.get_billing_totals_for_local_date_range('2026-01-01', '2026-01-01')

        # imp_kwh: remainder(1.0) + ev_grid(2.0) = 3.0, NOT 3.0+2.0=5.0
        self.assertAlmostEqual(t["imp_kwh"], 3.0, places=3,
            msg="Billing totals must not double-count sub-meter imp_kwh")
        self.assertNotAlmostEqual(t["imp_kwh"], 5.0, places=1,
            msg="5.0 kWh indicates double-counting bug")
        # cost from main meter only
        self.assertAlmostEqual(t["imp_cost"], 0.735, places=3)
        # export from main meter only
        self.assertAlmostEqual(t["exp_kwh"], 0.2, places=3)
        self.assertAlmostEqual(t["exp_cost"], 0.024, places=3)
        # standing from main meter only
        self.assertAlmostEqual(t["standing"], 0.5, places=3)

    def test_get_cumulative_totals_with_sub_meters(self):
        """
        With sub-meters, totals must NOT double-count.
        electricity_main.imp_kwh already includes sub-meter consumption.
        get_cumulative_totals should use:
          - main meter: imp_kwh_remainder (house-only grid load)
          - sub-meter:  imp_kwh_grid (sub-meter grid portion), or imp_kwh
          TOTAL = remainder + sub_grid ≈ main.imp_kwh
        """
        cfg = {"meters": {
            "electricity_main": {"meta": {
                "billing_day": 1, "block_minutes": 30, "timezone": "UTC",
                "currency_symbol": "£", "currency_code": "GBP",
            }, "channels": {
                "import": {"read": "sensor.main", "rate": "sensor.rate"},
                "export": {"read": "sensor.exp",  "rate": "sensor.exprate"},
            }},
            "ev_charger": {"meta": {
                "sub_meter": True, "parent_meter": "electricity_main",
            }, "channels": {
                "import": {"read": "sensor.ev", "rate": "sensor.rate"},
            }},
        }}
        self.store.insert_config_period(cfg)
        cp_id = self.store._conn.execute(
            "SELECT id FROM config_periods LIMIT 1"
        ).fetchone()["id"]

        # One block: main draws 3.0 kWh total, EV uses 2.0, house uses 1.0
        # main: imp_kwh=3.0, imp_kwh_remainder=1.0 (house only), imp_cost=0.735
        # ev:   imp_kwh=2.0, imp_kwh_grid=2.0 (all from grid), no independent cost
        self.store._conn.execute("""
            INSERT INTO blocks (block_start, block_end, local_date, local_year,
            local_month, local_day, meter_id, config_period_id, interpolated,
            imp_kwh, imp_kwh_remainder, imp_cost, exp_kwh, exp_cost, standing_charge)
            VALUES ('2026-01-01T00:00:00','2026-01-01T00:30:00','2026-01-01',
            2026,1,1,'electricity_main',?,0, 3.0,1.0,0.735, 0.0,0.0, 0.5)
        """, (cp_id,))
        self.store._conn.execute("""
            INSERT INTO blocks (block_start, block_end, local_date, local_year,
            local_month, local_day, meter_id, config_period_id, interpolated,
            imp_kwh, imp_kwh_grid, imp_cost, exp_kwh, exp_cost, standing_charge)
            VALUES ('2026-01-01T00:00:00','2026-01-01T00:30:00','2026-01-01',
            2026,1,1,'ev_charger',?,0, 2.0,2.0,0.0, 0.0,0.0, 0.0)
        """, (cp_id,))
        self.store._conn.commit()

        totals = self.store.get_cumulative_totals()

        # Correct: remainder(1.0) + ev_grid(2.0) = 3.0 kWh total grid import
        self.assertAlmostEqual(totals["import_kwh"], 3.0, places=4,
            msg="Sub-meter must not double-count: total grid = house(1) + ev_grid(2) = 3")
        # Cost only from main meter
        self.assertAlmostEqual(totals["import_cost"], 0.735, places=4,
            msg="Import cost must come from main meter only")
        # NOT 5.0 (3.0 + 2.0 double-counted)
        self.assertNotAlmostEqual(totals["import_kwh"], 5.0, places=1,
            msg="5.0 would indicate double-counting bug")


class TestUpgradePaths(unittest.TestCase):
    """
    Verify that the 1.x→2.1.0 and 2.0→2.1.0 upgrade paths work correctly:
    - New DB tables are created automatically (CREATE TABLE IF NOT EXISTS)
    - Config is loaded from file when DB has no periods (1.x path)
    - Config is loaded from DB when periods exist (2.0 path)
    - current_block.json migration seeds the DB correctly
    """

    def setUp(self):
        import sys, types
        eio = types.ModuleType("energy_engine_io"); eio.load_json = lambda *a,**kw: {}
        sys.modules.setdefault("energy_engine_io", eio)

    def test_new_tables_created_on_existing_db(self):
        """
        Simulates a 2.0.0 DB that lacks current_block and current_reads tables.
        Opening it with BlockStore should create them via CREATE TABLE IF NOT EXISTS.
        """
        import tempfile, os, sqlite3
        with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as f:
            db_path = f.name
        try:
            # Create a minimal 2.0.0-style DB with blocks and config_periods only
            conn = sqlite3.connect(db_path)
            conn.execute("""CREATE TABLE config_periods (
                id INTEGER PRIMARY KEY, effective_from TEXT, effective_to TEXT,
                billing_day INTEGER, block_minutes INTEGER, timezone TEXT,
                currency_symbol TEXT, currency_code TEXT, site_name TEXT,
                change_reason TEXT, full_config_json TEXT NOT NULL)""")
            conn.execute("""CREATE TABLE blocks (
                id INTEGER PRIMARY KEY, block_start TEXT, block_end TEXT,
                local_date TEXT NOT NULL, local_year INTEGER, local_month INTEGER,
                local_day INTEGER, meter_id TEXT, config_period_id INTEGER,
                interpolated INTEGER, imp_kwh REAL, imp_cost REAL,
                exp_kwh REAL, exp_cost REAL, standing_charge REAL NOT NULL DEFAULT 0)""")
            conn.commit()
            conn.close()

            # Opening with BlockStore should add missing tables
            store = BlockStore(db_path)
            # Verify new tables exist
            tables = {r[0] for r in store._conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()}
            self.assertIn("current_block", tables,
                "current_block table must be created on 2.0→2.1 upgrade")
            self.assertIn("current_reads", tables,
                "current_reads table must be created on 2.0→2.1 upgrade")
        finally:
            os.unlink(db_path)

    def test_empty_db_has_no_current_block(self):
        """Fresh DB: load_current_block returns empty dict."""
        store = BlockStore(":memory:")
        self.assertEqual(store.load_current_block(), {})

    def test_config_period_none_when_empty(self):
        """Fresh DB (1.x path): get_current_config_period_id returns None."""
        store = BlockStore(":memory:")
        self.assertIsNone(store.get_current_config_period_id())

    def test_config_period_present_after_insert(self):
        """After insert_config_period (2.0 path): get_current_config_period_id returns id."""
        import json
        store = BlockStore(":memory:")
        cfg = {"meters": {"electricity_main": {"meta": {
            "billing_day": 1, "block_minutes": 30, "timezone": "UTC",
            "currency_symbol": "£", "currency_code": "GBP",
        }}}}
        store.insert_config_period(cfg)
        self.assertIsNotNone(store.get_current_config_period_id())

    def test_current_block_migration_from_file(self):
        """
        Simulates 2.0→2.1 current_block.json migration:
        load_current_block() returns empty, then file is loaded and saved to DB.
        """
        import json
        store = BlockStore(":memory:")
        # DB is empty (no current block)
        self.assertEqual(store.load_current_block(), {})

        # Simulate file content (as written by 2.0.0 engine)
        cb_from_file = {
            "start": "2026-04-05T00:00:00",
            "end":   "2026-04-05T00:30:00",
            "interpolated": False,
            "_last_checkpoint": "2026-04-05T00:15:00",
            "meters": {
                "electricity_main": {
                    "meta": {},
                    "standing_charge": 0.50,
                    "channels": {
                        "import": {
                            "reads": [{"ts": "2026-04-05T00:00:00", "value": 28000.0}],
                            "rates": [{"ts": "2026-04-05T00:00:00", "value": 0.245}],
                        }
                    }
                }
            }
        }

        # Migration step: save file content to DB
        store.save_current_block(cb_from_file)

        # Verify it round-trips correctly
        loaded = store.load_current_block()
        self.assertEqual(loaded["start"], "2026-04-05T00:00:00")
        self.assertEqual(loaded["_last_checkpoint"], "2026-04-05T00:15:00")
        reads = loaded["meters"]["electricity_main"]["channels"]["import"]["reads"]
        self.assertEqual(len(reads), 1)
        self.assertAlmostEqual(reads[0]["value"], 28000.0, places=3)

    def test_cumulative_totals_from_empty_db(self):
        """Fresh DB (or 2.1.0 after removing file): totals are all zero."""
        store = BlockStore(":memory:")
        totals = store.get_cumulative_totals()
        self.assertEqual(totals["import_kwh"], 0.0)
        self.assertEqual(totals["export_kwh"], 0.0)
        self.assertEqual(totals["import_cost"], 0.0)
        self.assertEqual(totals["export_cost"], 0.0)


class TestNormalisedMeters(unittest.TestCase):
    """Tests for the normalised meters/meter_channels tables."""

    def setUp(self):
        import sys, types
        eio = types.ModuleType("energy_engine_io"); eio.load_json = lambda *a, **kw: {}
        sys.modules.setdefault("energy_engine_io", eio)
        self.store = BlockStore(":memory:")

    def _cfg(self, billing_day=1, site="Home", sub_meters=None):
        cfg = {"meters": {"electricity_main": {"meta": {
            "billing_day": billing_day, "block_minutes": 30,
            "timezone": "Europe/London", "currency_symbol": "£",
            "currency_code": "GBP", "site": site,
        }, "channels": {
            "import": {"read": "sensor.import_kwh", "rate": "sensor.import_rate",
                       "standing_charge_sensor": "sensor.standing"},
            "export": {"read": "sensor.export_kwh", "rate": "sensor.export_rate"},
        }}}}
        if sub_meters:
            for mid, label in sub_meters.items():
                cfg["meters"][mid] = {"meta": {
                    "sub_meter": True, "parent_meter": "electricity_main",
                    "device": label, "protected": True,
                }, "channels": {
                    "import": {"read": f"sensor.{mid}_kwh", "rate": "sensor.import_rate"},
                }}
        return cfg

    def test_insert_creates_meter_rows(self):
        cfg = self._cfg()
        self.store.insert_config_period(cfg)
        rows = self.store._conn.execute("SELECT * FROM meters").fetchall()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["meter_id"], "electricity_main")
        self.assertEqual(rows[0]["is_sub_meter"], 0)

    def test_insert_creates_channel_rows(self):
        cfg = self._cfg()
        self.store.insert_config_period(cfg)
        rows = self.store._conn.execute("SELECT * FROM meter_channels ORDER BY channel").fetchall()
        self.assertEqual(len(rows), 2)
        channels = {r["channel"] for r in rows}
        self.assertEqual(channels, {"import", "export"})

    def test_import_sensors_stored(self):
        cfg = self._cfg()
        self.store.insert_config_period(cfg)
        ch = self.store._conn.execute(
            "SELECT * FROM meter_channels WHERE channel='import'"
        ).fetchone()
        self.assertEqual(ch["read_sensor"], "sensor.import_kwh")
        self.assertEqual(ch["rate_sensor"], "sensor.import_rate")
        self.assertEqual(ch["standing_charge_sensor"], "sensor.standing")

    def test_sub_meter_flags_stored(self):
        cfg = self._cfg(sub_meters={"ev_charger": "EV Charger"})
        self.store.insert_config_period(cfg)
        sub = self.store._conn.execute(
            "SELECT * FROM meters WHERE meter_id='ev_charger'"
        ).fetchone()
        self.assertEqual(sub["is_sub_meter"], 1)
        self.assertEqual(sub["parent_meter_id"], "electricity_main")
        self.assertEqual(sub["device_label"], "EV Charger")
        self.assertEqual(sub["protected"], 1)

    def test_config_from_db_roundtrip_simple(self):
        """config_from_db reproduces sensor entity IDs correctly."""
        cfg = self._cfg()
        self.store.insert_config_period(cfg)
        pid = self.store.get_current_config_period_id()
        out = self.store.config_from_db(pid)
        imp = out["meters"]["electricity_main"]["channels"]["import"]
        self.assertEqual(imp["read"], "sensor.import_kwh")
        self.assertEqual(imp["rate"], "sensor.import_rate")
        self.assertEqual(imp["standing_charge_sensor"], "sensor.standing")

    def test_config_from_db_roundtrip_sub_meter(self):
        """Sub-meter flags and parent_meter survive the roundtrip."""
        cfg = self._cfg(sub_meters={"ev_charger": "EV Charger"})
        self.store.insert_config_period(cfg)
        pid = self.store.get_current_config_period_id()
        out = self.store.config_from_db(pid)
        self.assertIn("ev_charger", out["meters"])
        meta = out["meters"]["ev_charger"]["meta"]
        self.assertTrue(meta.get("sub_meter"))
        self.assertEqual(meta.get("parent_meter"), "electricity_main")
        self.assertEqual(meta.get("device"), "EV Charger")
        self.assertTrue(meta.get("protected"))

    def test_config_from_db_billing_scalars(self):
        """Billing scalars from config_periods appear on every meter's meta."""
        cfg = self._cfg(billing_day=15, site="Test Home")
        self.store.insert_config_period(cfg)
        pid = self.store.get_current_config_period_id()
        out = self.store.config_from_db(pid)
        meta = out["meters"]["electricity_main"]["meta"]
        self.assertEqual(meta["billing_day"], 15)
        self.assertEqual(meta["site"], "Test Home")
        self.assertEqual(meta["timezone"], "Europe/London")

    def test_channel_meta_stored_and_retrieved(self):
        """mpan/tariff in channel meta round-trips through meter_channels columns."""
        cfg = self._cfg()
        cfg["meters"]["electricity_main"]["channels"]["import"]["meta"] = {
            "mpan": "1234567890123", "tariff": "Agile",
        }
        self.store.insert_config_period(cfg)
        pid = self.store.get_current_config_period_id()
        out = self.store.config_from_db(pid)
        ch_meta = out["meters"]["electricity_main"]["channels"]["import"].get("meta", {})
        self.assertEqual(ch_meta.get("mpan"), "1234567890123")
        self.assertEqual(ch_meta.get("tariff"), "Agile")

    def test_second_period_has_own_meter_rows(self):
        """Each config period gets its own set of meter rows."""
        cfg1 = self._cfg(billing_day=1, site="Period 1")
        cfg2 = self._cfg(billing_day=15, site="Period 2",
                         sub_meters={"ev_charger": "EV"})
        self.store.insert_config_period(cfg1)
        self.store.insert_config_period(cfg2)

        periods = self.store._conn.execute(
            "SELECT id FROM config_periods ORDER BY effective_from"
        ).fetchall()
        p1_id, p2_id = periods[0]["id"], periods[1]["id"]

        out1 = self.store.config_from_db(p1_id)
        out2 = self.store.config_from_db(p2_id)

        self.assertNotIn("ev_charger", out1["meters"])
        self.assertIn("ev_charger", out2["meters"])
        self.assertEqual(out1["meters"]["electricity_main"]["meta"]["billing_day"], 1)
        self.assertEqual(out2["meters"]["electricity_main"]["meta"]["billing_day"], 15)

    def test_delete_period_cascades_meters(self):
        """Deleting a config period removes its meter and channel rows."""
        cfg1 = self._cfg(site="First")
        cfg2 = self._cfg(site="Second")
        self.store.insert_config_period(cfg1)
        self.store.insert_config_period(cfg2)

        p1_id = self.store._conn.execute(
            "SELECT id FROM config_periods ORDER BY effective_from LIMIT 1"
        ).fetchone()["id"]

        self.store.delete_config_period(p1_id)

        remaining = self.store._conn.execute(
            "SELECT config_period_id FROM meters"
        ).fetchall()
        period_ids = {r["config_period_id"] for r in remaining}
        self.assertNotIn(p1_id, period_ids,
            "Meter rows for deleted period must be removed")

    def test_save_config_rewrites_meter_rows(self):
        """
        Saving a config that removes a sub-meter deletes the old meter row.
        """
        cfg_with_sub = self._cfg(sub_meters={"ev_charger": "EV Charger"})
        self.store.insert_config_period(cfg_with_sub)
        pid = self.store.get_current_config_period_id()

        # Verify ev_charger exists
        count_before = self.store._conn.execute(
            "SELECT COUNT(*) FROM meters WHERE config_period_id=?", (pid,)
        ).fetchone()[0]
        self.assertEqual(count_before, 2)  # main + ev_charger

        # Remove sub-meter from config (simulate save_config removing a meter)
        cfg_no_sub = self._cfg()
        with self.store._conn:
            self.store._conn.execute(
                """UPDATE config_periods
                   SET billing_day=?, block_minutes=?, timezone=?,
                       currency_symbol=?, currency_code=?, site_name=?
                   WHERE id=?""",
                (1, 30, "Europe/London", "£", "GBP", "Home", pid)
            )
            old_mids = [r["id"] for r in self.store._conn.execute(
                "SELECT id FROM meters WHERE config_period_id=?", (pid,)
            ).fetchall()]
            for mid in old_mids:
                self.store._conn.execute(
                    "DELETE FROM meter_channels WHERE meter_id=?", (mid,))
            self.store._conn.execute(
                "DELETE FROM meters WHERE config_period_id=?", (pid,))
            self.store._write_meters(cfg_no_sub, pid)

        count_after = self.store._conn.execute(
            "SELECT COUNT(*) FROM meters WHERE config_period_id=?", (pid,)
        ).fetchone()[0]
        self.assertEqual(count_after, 1,
            "ev_charger meter row must be removed when absent from new config")


class TestMigration(unittest.TestCase):
    """Tests for migrate_full_config_json — 2.0→2.1 upgrade path."""

    def setUp(self):
        import sys, types
        eio = types.ModuleType("energy_engine_io"); eio.load_json = lambda *a, **kw: {}
        sys.modules.setdefault("energy_engine_io", eio)

    def _make_v20_db(self, path):
        """Create a minimal 2.0.0-style DB with full_config_json and gap_marker blob."""
        import sqlite3, json
        conn = sqlite3.connect(path)
        conn.execute("""CREATE TABLE config_periods (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            effective_from TEXT NOT NULL, effective_to TEXT,
            billing_day INTEGER NOT NULL DEFAULT 1,
            block_minutes INTEGER NOT NULL DEFAULT 30,
            timezone TEXT NOT NULL DEFAULT 'UTC',
            currency_symbol TEXT NOT NULL DEFAULT '£',
            currency_code TEXT NOT NULL DEFAULT 'GBP',
            site_name TEXT, change_reason TEXT,
            full_config_json TEXT NOT NULL)""")
        conn.execute("""CREATE TABLE blocks (
            id INTEGER PRIMARY KEY, block_start TEXT, block_end TEXT,
            local_date TEXT NOT NULL, local_year INTEGER, local_month INTEGER,
            local_day INTEGER, meter_id TEXT, config_period_id INTEGER,
            interpolated INTEGER, imp_kwh REAL, imp_cost REAL,
            exp_kwh REAL, exp_cost REAL, standing_charge REAL NOT NULL DEFAULT 0)""")
        conn.execute("""CREATE TABLE meters (
            id INTEGER PRIMARY KEY AUTOINCREMENT, meter_id TEXT NOT NULL,
            is_sub_meter INTEGER NOT NULL DEFAULT 0, device_label TEXT,
            parent_meter_id TEXT, config_period_id INTEGER NOT NULL)""")
        conn.execute("""CREATE TABLE current_block (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            block_start TEXT, block_end TEXT, last_checkpoint TEXT,
            gap_marker TEXT, interpolated INTEGER NOT NULL DEFAULT 0)""")
        conn.execute("""CREATE TABLE current_reads (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            captured_at TEXT NOT NULL, meter_id TEXT NOT NULL,
            channel TEXT NOT NULL, channel_type TEXT NOT NULL DEFAULT 'read',
            value REAL NOT NULL, standing_charge REAL)""")
        # Minimal config
        cfg = {"meters": {"electricity_main": {"meta": {
            "billing_day": 1, "block_minutes": 30, "timezone": "Europe/London",
            "currency_symbol": "£", "currency_code": "GBP", "site": "Home",
        }, "channels": {
            "import": {"read": "sensor.import", "rate": "sensor.rate",
                        "meta": {"mpan": "1234567890", "tariff": "Agile"}},
            "export": {"read": "sensor.export", "rate": "sensor.rate"},
        }}}}
        conn.execute(
            "INSERT INTO config_periods "
            "(effective_from, billing_day, block_minutes, timezone, "
            "currency_symbol, currency_code, full_config_json) "
            "VALUES ('2026-01-01T00:00:00', 1, 30, 'Europe/London', '£', 'GBP', ?)",
            (json.dumps(cfg),)
        )
        # Gap marker blob
        gap = {
            "detected_at": "2026-04-05T12:00:00",
            "pre_reads": {"electricity_main": {"import": {"ts": "2026-04-05T11:55:00", "value": 28000.0}}},
            "last_known_rates": {"electricity_main": {"import": {"ts": "2026-04-05T11:55:00", "value": 0.245}}},
        }
        conn.execute(
            "INSERT INTO current_block (id, block_start, block_end, gap_marker, interpolated) "
            "VALUES (1, '2026-04-05T12:00:00', '2026-04-05T12:30:00', ?, 0)",
            (json.dumps(gap),)
        )
        conn.commit()
        conn.close()

    def test_migrate_populates_meters_table(self):
        import tempfile, os
        with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as f:
            path = f.name
        try:
            self._make_v20_db(path)
            store = BlockStore(path)
            store.migrate_full_config_json()
            count = store._conn.execute("SELECT COUNT(*) FROM meters").fetchone()[0]
            self.assertGreater(count, 0, "meters table must be populated after migration")
        finally:
            os.unlink(path)

    def test_migrate_drops_full_config_json(self):
        import tempfile, os
        with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as f:
            path = f.name
        try:
            self._make_v20_db(path)
            store = BlockStore(path)
            store.migrate_full_config_json()
            cols = [r[1] for r in store._conn.execute(
                "PRAGMA table_info(config_periods)"
            ).fetchall()]
            self.assertNotIn("full_config_json", cols,
                "full_config_json column must be dropped after migration")
        finally:
            os.unlink(path)

    def test_migrate_drops_gap_marker_blob(self):
        import tempfile, os
        with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as f:
            path = f.name
        try:
            self._make_v20_db(path)
            store = BlockStore(path)
            store.migrate_full_config_json()
            cols = [r[1] for r in store._conn.execute(
                "PRAGMA table_info(current_block)"
            ).fetchall()]
            self.assertNotIn("gap_marker", cols,
                "gap_marker blob column must be dropped after migration")
            self.assertIn("gap_detected_at", cols,
                "gap_detected_at column must exist after migration")
        finally:
            os.unlink(path)

    def test_migrate_preserves_gap_detected_at(self):
        import tempfile, os
        with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as f:
            path = f.name
        try:
            self._make_v20_db(path)
            store = BlockStore(path)
            store.migrate_full_config_json()
            row = store._conn.execute(
                "SELECT gap_detected_at FROM current_block WHERE id=1"
            ).fetchone()
            self.assertEqual(row["gap_detected_at"], "2026-04-05T12:00:00",
                "gap_detected_at must be populated from migrated gap_marker blob")
        finally:
            os.unlink(path)

    def test_migrate_seeds_gap_seed_rows(self):
        import tempfile, os
        with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as f:
            path = f.name
        try:
            self._make_v20_db(path)
            store = BlockStore(path)
            store.migrate_full_config_json()
            seeds = store._conn.execute(
                "SELECT * FROM current_reads WHERE is_gap_seed > 0"
            ).fetchall()
            self.assertGreater(len(seeds), 0,
                "Gap seed rows must be written to current_reads during migration")
        finally:
            os.unlink(path)

    def test_migrate_adds_is_gap_seed_column(self):
        import tempfile, os
        with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as f:
            path = f.name
        try:
            self._make_v20_db(path)
            store = BlockStore(path)
            store.migrate_full_config_json()
            cols = [r[1] for r in store._conn.execute(
                "PRAGMA table_info(current_reads)"
            ).fetchall()]
            self.assertIn("is_gap_seed", cols,
                "is_gap_seed column must be added to current_reads during migration")
        finally:
            os.unlink(path)

    def test_migrate_adds_mpan_tariff_columns(self):
        import tempfile, os
        with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as f:
            path = f.name
        try:
            self._make_v20_db(path)
            store = BlockStore(path)
            store.migrate_full_config_json()
            cols = [r[1] for r in store._conn.execute(
                "PRAGMA table_info(meter_channels)"
            ).fetchall()]
            self.assertIn("mpan",   cols, "mpan column must be added to meter_channels")
            self.assertIn("tariff", cols, "tariff column must be added to meter_channels")
        finally:
            os.unlink(path)

    def test_migrate_populates_mpan_tariff(self):
        import tempfile, os
        with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as f:
            path = f.name
        try:
            self._make_v20_db(path)
            store = BlockStore(path)
            store.migrate_full_config_json()
            ch = store._conn.execute(
                "SELECT mpan, tariff FROM meter_channels WHERE channel='import'"
            ).fetchone()
            self.assertEqual(ch["mpan"],   "1234567890")
            self.assertEqual(ch["tariff"], "Agile")
        finally:
            os.unlink(path)

    def test_migrate_config_from_db_roundtrip(self):
        """After migration, config_from_db returns correct sensor IDs."""
        import tempfile, os
        with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as f:
            path = f.name
        try:
            self._make_v20_db(path)
            store = BlockStore(path)
            store.migrate_full_config_json()
            pid = store._conn.execute(
                "SELECT id FROM config_periods LIMIT 1"
            ).fetchone()["id"]
            cfg = store.config_from_db(pid)
            self.assertIn("electricity_main", cfg["meters"])
            imp = cfg["meters"]["electricity_main"]["channels"]["import"]
            self.assertEqual(imp["read"], "sensor.import")
            self.assertEqual(imp["rate"], "sensor.rate")
            self.assertEqual(imp.get("meta", {}).get("mpan"), "1234567890")
        finally:
            os.unlink(path)

    def test_migrate_idempotent(self):
        """Running migration twice has no ill effects."""
        import tempfile, os
        with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as f:
            path = f.name
        try:
            self._make_v20_db(path)
            store = BlockStore(path)
            result1 = store.migrate_full_config_json()
            result2 = store.migrate_full_config_json()
            self.assertEqual(result2, 0, "Second migration must return 0 (nothing to do)")
        finally:
            os.unlink(path)



class TestBillingTotalsSubMeterNullGrid(unittest.TestCase):
    """Regression test: sub-meter with NULL imp_kwh_grid must not double-count."""

    def setUp(self):
        self.store = BlockStore(":memory:")
        cfg = {"meters": {
            "electricity_main": {"meta": {
                "billing_day": 1, "block_minutes": 30, "timezone": "UTC",
                "currency_symbol": "£", "currency_code": "GBP",
            }, "channels": {
                "import": {"read": "s.imp", "rate": "s.rate"},
            }},
            "house_battery": {"meta": {
                "sub_meter": True, "parent_meter": "electricity_main",
            }, "channels": {
                "import": {"read": "s.bat", "rate": "s.rate"},
            }},
        }}
        self.store.insert_config_period(cfg)
        self.cp = self.store._conn.execute(
            "SELECT id FROM config_periods LIMIT 1"
        ).fetchone()["id"]

    def test_null_grid_no_fallback_to_raw_kwh(self):
        """
        Sub-meter with imp_kwh populated but imp_kwh_grid=NULL must NOT fall back
        to imp_kwh — that would double-count since main meter imp_kwh already
        includes sub-meter consumption. Only COALESCE(imp_kwh_grid, 0) is used.
        """
        # main: 13.0 kWh raw, 2.5 remainder
        self.store._conn.execute("""
            INSERT INTO blocks (block_start, block_end, local_date, local_year,
            local_month, local_day, meter_id, config_period_id, interpolated,
            imp_kwh, imp_kwh_remainder, imp_cost, standing_charge)
            VALUES ('2026-03-01T00:00:00','2026-03-01T00:30:00','2026-03-01',
            2026,3,1,'electricity_main',?,0, 13.0,2.5,1.17,0.5)
        """, (self.cp,))
        # battery: 10.5 kWh raw but imp_kwh_grid=NULL (older block)
        self.store._conn.execute("""
            INSERT INTO blocks (block_start, block_end, local_date, local_year,
            local_month, local_day, meter_id, config_period_id, interpolated,
            imp_kwh, imp_kwh_grid, imp_cost, standing_charge)
            VALUES ('2026-03-01T00:00:00','2026-03-01T00:30:00','2026-03-01',
            2026,3,1,'house_battery',?,0, 10.5,NULL,0.0,0.0)
        """, (self.cp,))
        self.store._conn.commit()

        t = self.store.get_billing_totals_for_local_date_range('2026-03-01', '2026-03-01')

        # 2.5 (main remainder) + 0 (battery NULL grid → 0) = 2.5, not 2.5+10.5=13.0
        self.assertAlmostEqual(t["imp_kwh"], 2.5, places=3,
            msg="Sub-meter NULL imp_kwh_grid must not fall back to imp_kwh")
        self.assertNotAlmostEqual(t["imp_kwh"], 13.0, places=1,
            msg="13.0 indicates double-counting bug")

    def test_set_grid_is_included(self):
        """Sub-meter with imp_kwh_grid set should be included in total."""
        self.store._conn.execute("""
            INSERT INTO blocks (block_start, block_end, local_date, local_year,
            local_month, local_day, meter_id, config_period_id, interpolated,
            imp_kwh, imp_kwh_remainder, imp_cost, standing_charge)
            VALUES ('2026-03-01T00:00:00','2026-03-01T00:30:00','2026-03-01',
            2026,3,1,'electricity_main',?,0, 13.0,2.5,1.17,0.5)
        """, (self.cp,))
        self.store._conn.execute("""
            INSERT INTO blocks (block_start, block_end, local_date, local_year,
            local_month, local_day, meter_id, config_period_id, interpolated,
            imp_kwh, imp_kwh_grid, imp_cost, standing_charge)
            VALUES ('2026-03-01T00:00:00','2026-03-01T00:30:00','2026-03-01',
            2026,3,1,'house_battery',?,0, 10.5,10.5,0.0,0.0)
        """, (self.cp,))
        self.store._conn.commit()

        t = self.store.get_billing_totals_for_local_date_range('2026-03-01', '2026-03-01')

        # 2.5 remainder + 10.5 grid = 13.0 total grid draw
        self.assertAlmostEqual(t["imp_kwh"], 13.0, places=3,
            msg="Sub-meter with imp_kwh_grid set must be included in total")

    def test_submeter_cost_included_when_grid_set(self):
        """Sub-meter imp_cost must be included in billing totals when imp_kwh_grid is set."""
        self.store._conn.execute("""
            INSERT INTO blocks (block_start, block_end, local_date, local_year,
            local_month, local_day, meter_id, config_period_id, interpolated,
            imp_kwh, imp_kwh_remainder, imp_cost, standing_charge)
            VALUES ('2026-03-01T00:00:00','2026-03-01T00:30:00','2026-03-01',
            2026,3,1,'electricity_main',?,0, 13.0,2.5,1.17,0.5)
        """, (self.cp,))
        # Battery: 10.5 kWh grid, cost £0.73
        self.store._conn.execute("""
            INSERT INTO blocks (block_start, block_end, local_date, local_year,
            local_month, local_day, meter_id, config_period_id, interpolated,
            imp_kwh, imp_kwh_grid, imp_cost, standing_charge)
            VALUES ('2026-03-01T00:00:00','2026-03-01T00:30:00','2026-03-01',
            2026,3,1,'house_battery',?,0, 10.5,10.5,0.73,0.0)
        """, (self.cp,))
        self.store._conn.commit()

        t = self.store.get_billing_totals_for_local_date_range('2026-03-01', '2026-03-01')

        # Total cost = main (1.17) + battery (0.73) = 1.90
        self.assertAlmostEqual(t["imp_cost"], 1.90, places=3,
            msg="Sub-meter imp_cost must be included when imp_kwh_grid is set")

    def test_submeter_cost_excluded_when_grid_null(self):
        """Sub-meter imp_cost must NOT be included when imp_kwh_grid is NULL."""
        self.store._conn.execute("""
            INSERT INTO blocks (block_start, block_end, local_date, local_year,
            local_month, local_day, meter_id, config_period_id, interpolated,
            imp_kwh, imp_kwh_remainder, imp_cost, standing_charge)
            VALUES ('2026-03-01T00:00:00','2026-03-01T00:30:00','2026-03-01',
            2026,3,1,'electricity_main',?,0, 13.0,2.5,1.17,0.5)
        """, (self.cp,))
        # Battery: imp_kwh_grid=NULL (old block) — cost must not be included
        self.store._conn.execute("""
            INSERT INTO blocks (block_start, block_end, local_date, local_year,
            local_month, local_day, meter_id, config_period_id, interpolated,
            imp_kwh, imp_kwh_grid, imp_cost, standing_charge)
            VALUES ('2026-03-01T00:00:00','2026-03-01T00:30:00','2026-03-01',
            2026,3,1,'house_battery',?,0, 10.5,NULL,0.73,0.0)
        """, (self.cp,))
        self.store._conn.commit()

        t = self.store.get_billing_totals_for_local_date_range('2026-03-01', '2026-03-01')

        # Only main meter cost — battery has no imp_kwh_grid so excluded
        self.assertAlmostEqual(t["imp_cost"], 1.17, places=3,
            msg="Sub-meter imp_cost must be excluded when imp_kwh_grid is NULL")


class TestBlockDeletion(unittest.TestCase):
    """Tests for delete_blocks_for_date_range and count_blocks_for_date_range."""

    def _make_store(self):
        store = BlockStore(":memory:")
        store.insert_config_period({"meters": {"electricity_main": {"meta": {
            "billing_day": 1, "block_minutes": 30, "timezone": "UTC",
            "currency_symbol": "£", "currency_code": "GBP",
        }, "channels": {"import": {"read": "s.imp", "rate": "s.rate"}}},
        "ev_charger": {"meta": {"sub_meter": True, "parent_meter": "electricity_main"},
                       "channels": {"import": {"read": "s.ev", "rate": "s.rate"}}},
        }})
        cp_id = store.get_current_config_period_id()
        rows = [
            ("2026-03-01T00:00:00", "electricity_main", "2026-03-01", 1.0),
            ("2026-03-01T00:30:00", "electricity_main", "2026-03-01", 1.0),
            ("2026-03-01T00:00:00", "ev_charger",       "2026-03-01", 0.5),
            ("2026-03-02T00:00:00", "electricity_main", "2026-03-02", 2.0),
            ("2026-03-02T00:30:00", "electricity_main", "2026-03-02", 2.0),
            ("2026-03-03T00:00:00", "electricity_main", "2026-03-03", 3.0),
        ]
        for (bs, mid, ld, kwh) in rows:
            store._conn.execute("""
                INSERT INTO blocks (block_start, block_end, meter_id, config_period_id,
                  local_date, local_year, local_month, local_day, interpolated,
                  imp_kwh, imp_rate, imp_cost, standing_charge)
                VALUES (?,?,?,?,?,2026,3,1,0,?,0.07,?,0.5)
            """, (bs, bs, mid, cp_id, ld, kwh, kwh * 0.07))
        store._conn.commit()
        return store

    def test_count_preview_all_meters(self):
        store = self._make_store()
        r = store.count_blocks_for_date_range("2026-03-01", "2026-03-02")
        self.assertEqual(r["blocks"], 5)
        self.assertEqual(r["dates"], 2)

    def test_count_preview_single_meter(self):
        store = self._make_store()
        r = store.count_blocks_for_date_range("2026-03-01", "2026-03-01", "electricity_main")
        self.assertEqual(r["blocks"], 2)
        self.assertEqual(r["dates"], 1)

    def test_count_preview_no_match(self):
        store = self._make_store()
        r = store.count_blocks_for_date_range("2026-04-01", "2026-04-30")
        self.assertEqual(r["blocks"], 0)
        self.assertEqual(r["dates"], 0)

    def test_delete_all_meters(self):
        store = self._make_store()
        r = store.delete_blocks_for_date_range("2026-03-01", "2026-03-02")
        self.assertEqual(r["deleted"], 5)
        self.assertEqual(r["dates"], 2)
        remaining = store._conn.execute("SELECT COUNT(*) FROM blocks").fetchone()[0]
        self.assertEqual(remaining, 1, "Only Mar 3 block should remain")

    def test_delete_single_meter(self):
        store = self._make_store()
        r = store.delete_blocks_for_date_range("2026-03-01", "2026-03-01", "ev_charger")
        self.assertEqual(r["deleted"], 1)
        remaining = store._conn.execute(
            "SELECT COUNT(*) FROM blocks WHERE meter_id='ev_charger'"
        ).fetchone()[0]
        self.assertEqual(remaining, 0)
        # Other meters untouched
        main_count = store._conn.execute(
            "SELECT COUNT(*) FROM blocks WHERE meter_id='electricity_main'"
        ).fetchone()[0]
        self.assertEqual(main_count, 5)

    def test_delete_no_match_is_safe(self):
        store = self._make_store()
        r = store.delete_blocks_for_date_range("2026-05-01", "2026-05-31")
        self.assertEqual(r["deleted"], 0)
        total = store._conn.execute("SELECT COUNT(*) FROM blocks").fetchone()[0]
        self.assertEqual(total, 6, "No blocks should have been removed")

    def test_invalid_date_range_raises(self):
        store = self._make_store()
        with self.assertRaises(ValueError):
            store.delete_blocks_for_date_range("2026-03-10", "2026-03-01")

    def test_missing_dates_raises(self):
        store = self._make_store()
        with self.assertRaises(ValueError):
            store.delete_blocks_for_date_range("", "2026-03-01")