"""
test_pv_solar.py
================
Coverage for the 3.0.5 battery PV/solar dial feature — the persistence side.

The feature added a dedicated ``pv_power_sensor`` column to the ``meters`` table
(the HA entity id whose value drives the small PV arc on a battery card). These
tests assert:

* it round-trips through insert_config_period → config_from_db like the other
  meta markers (power_source / rate_source), including clearing to *absent*;
* it is independent of its sibling sensor columns (soc / inverter);
* a pre-3.0.5 database whose meters table predates the column gains it
  automatically on open, without losing existing meter rows.

Run with:
    python3 -m pytest test_pv_solar.py -v
or:
    python3 -B test_pv_solar.py
"""

import os
import sys
import copy
import sqlite3
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(__file__))
from block_store import BlockStore
from test_block_store import EXAMPLE_CONFIG, new_store


class TestPvPowerSensorRoundTrip(unittest.TestCase):

    def setUp(self):
        self.store = new_store()

    def tearDown(self):
        self.store.close()

    def test_pv_power_sensor_round_trips(self):
        """meta.pv_power_sensor (dedicated column) must survive the DB
        write/read round-trip, and clearing it must round-trip to absent
        rather than leaving a stale value."""
        cfg = copy.deepcopy(EXAMPLE_CONFIG)
        cfg["meters"]["electricity_main"]["meta"]["pv_power_sensor"] = \
            "sensor.solax_pv_power"
        self.store.insert_config_period(cfg)
        pid = self.store.get_current_config_period_id()
        restored = self.store.config_from_db(pid)
        self.assertEqual(
            restored["meters"]["electricity_main"]["meta"].get("pv_power_sensor"),
            "sensor.solax_pv_power")

        # Clearing it round-trips to absent (not a stale entity id).
        cfg2 = copy.deepcopy(EXAMPLE_CONFIG)
        cfg2["meters"]["electricity_main"]["meta"].pop("pv_power_sensor", None)
        self.store.insert_config_period(cfg2)
        restored2 = self.store.config_from_db(
            self.store.get_current_config_period_id())
        self.assertNotIn(
            "pv_power_sensor",
            restored2["meters"]["electricity_main"]["meta"])

    def test_pv_power_sensor_independent_of_sibling_sensors(self):
        """pv_power_sensor is a separate column from soc_sensor and
        inverter_power_sensor — setting all three must not cross-contaminate."""
        cfg = copy.deepcopy(EXAMPLE_CONFIG)
        meta = cfg["meters"]["electricity_main"]["meta"]
        meta["soc_sensor"] = "sensor.batt_soc"
        meta["inverter_power_sensor"] = "sensor.inverter_power"
        meta["pv_power_sensor"] = "sensor.pv_power"
        self.store.insert_config_period(cfg)
        restored = self.store.config_from_db(
            self.store.get_current_config_period_id())
        rmeta = restored["meters"]["electricity_main"]["meta"]
        self.assertEqual(rmeta.get("soc_sensor"), "sensor.batt_soc")
        self.assertEqual(rmeta.get("inverter_power_sensor"), "sensor.inverter_power")
        self.assertEqual(rmeta.get("pv_power_sensor"), "sensor.pv_power")


class TestPvPowerSensorSchemaMigration(unittest.TestCase):

    def test_column_added_on_open_preserving_rows(self):
        """A pre-3.0.5 DB whose meters table lacks pv_power_sensor must gain the
        column automatically when opened by current code, and existing meter
        rows must survive the migration."""
        fd, path = tempfile.mkstemp(suffix=".db")
        os.close(fd)
        try:
            store = BlockStore(path)
            store.insert_config_period(EXAMPLE_CONFIG)
            store.close()

            # Simulate the pre-3.0.5 schema by dropping the column directly.
            raw = sqlite3.connect(path)
            raw.execute("ALTER TABLE meters DROP COLUMN pv_power_sensor")
            raw.commit()
            cols = {r[1] for r in raw.execute(
                "PRAGMA table_info(meters)").fetchall()}
            self.assertNotIn("pv_power_sensor", cols)
            n_before = raw.execute("SELECT COUNT(*) FROM meters").fetchone()[0]
            self.assertGreater(n_before, 0,
                               "fixture should have populated the meters table")
            raw.close()

            # Reopen with the current code — _ensure_schema runs on open.
            store2 = BlockStore(path)
            cols2 = {r[1] for r in store2._conn.execute(
                "PRAGMA table_info(meters)").fetchall()}
            self.assertIn("pv_power_sensor", cols2,
                          "migration must re-add the pv_power_sensor column")
            n_after = store2._conn.execute(
                "SELECT COUNT(*) FROM meters").fetchone()[0]
            self.assertEqual(n_after, n_before,
                             "existing meter rows must survive the migration")
            store2.close()
        finally:
            os.remove(path)

    def test_ensure_schema_idempotent_for_pv_column(self):
        """Re-running the schema guard must not error or duplicate the column."""
        store = new_store()
        try:
            store._ensure_schema()  # second run on an already-current schema
            cols = [r[1] for r in store._conn.execute(
                "PRAGMA table_info(meters)").fetchall()]
            self.assertEqual(cols.count("pv_power_sensor"), 1)
        finally:
            store.close()


if __name__ == "__main__":
    unittest.main()