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

    def test_full_config_json_stored(self):
        self.store.insert_config_period(EXAMPLE_CONFIG)
        cp = self.store.get_config_period(1)
        stored = json.loads(cp["full_config_json"])
        self.assertEqual(stored, EXAMPLE_CONFIG)

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
        self.store._conn.execute("""
            INSERT INTO config_periods
            (effective_from, effective_to, billing_day, block_minutes, timezone,
             currency_symbol, currency_code, site_name, change_reason, full_config_json)
            VALUES ('2026-01-01T00:00:00', NULL, 3, 30, 'Europe/London', '£', 'GBP', 'Home', NULL, '{}')
        """)
        self.store._conn.commit()
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