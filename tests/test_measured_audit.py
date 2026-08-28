"""
test_measured_audit.py — BL-53 step 3 gate (audit_measured_costs).

Read-only audit comparing every cached measured_cost against its block's current
price: agree / band-flip / material £ delta / absent-cost, across the whole set.
"""
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import engine
from block_store import BlockStore

PEAK = 0.323092
OFF = 0.05493


class _AuditSched:
    def is_empty(self):
        return False

    def day_rate_bounds(self, ts):
        return (5.493, 32.3092)   # pence → (off, peak)


class TestMeasuredAudit(unittest.TestCase):

    def setUp(self):
        self._save = (engine._store, engine._kraken_discovery,
                      engine._kraken_rate_schedules)
        self.st = BlockStore(":memory:")
        engine._store = self.st
        engine._kraken_discovery = {"import": {"mpan": "X"}}
        engine._kraken_rate_schedules = {"import": _AuditSched()}
        self.st._conn.execute(
            "INSERT OR IGNORE INTO config_periods (id, effective_from, billing_day, "
            "block_minutes, timezone) VALUES (1,'2020-01-01T00:00:00',1,30,'UTC')")

    def tearDown(self):
        (engine._store, engine._kraken_discovery,
         engine._kraken_rate_schedules) = self._save

    def _blk(self, slot, rate, cost):
        self.st._conn.execute(
            "INSERT INTO blocks (block_start, block_end, meter_id, config_period_id, "
            "imp_kwh, imp_rate, imp_cost, imp_kwh_api) VALUES (?,?,?,?,?,?,?,?)",
            (slot, slot, "electricity_main", 1, 1.0, rate, cost, 1.0))

    def _meas(self, slot, cost_incl, label):
        self.st.upsert_measured_cost(slot, mpan="X", cost_incl=cost_incl,
                                     cost_excl=None, label=label, kwh=1.0)

    def test_tallies(self):
        # agree: peak block, STANDARD, cost matches
        self._blk("2026-08-10T20:00:00", PEAK, 1.0);  self._meas("2026-08-10T20:00:00", 1.0, "STANDARD_RATE")
        # band-flip: off-peak block, billed STANDARD (peak)
        self._blk("2026-08-10T02:00:00", OFF, 0.05);  self._meas("2026-08-10T02:00:00", 0.32, "STANDARD_RATE")
        # material: peak block, same band, but £ delta > threshold
        self._blk("2026-08-10T21:00:00", PEAK, 1.0);  self._meas("2026-08-10T21:00:00", 1.10, "STANDARD_RATE")
        # absent: measured row with no cost
        self._blk("2026-08-10T22:00:00", PEAK, 1.0);  self._meas("2026-08-10T22:00:00", None, "STANDARD_RATE")
        # label-only: off-peak block billed STANDARD label BUT cost agrees (measurement
        # label noise — the 25/08 23:00 signature). Must NOT count as a reprice.
        self._blk("2026-08-10T03:00:00", OFF, 0.05);  self._meas("2026-08-10T03:00:00", 0.05, "STANDARD_RATE")
        self.st._conn.commit()
        r = engine.audit_measured_costs()
        self.assertEqual(r["cached"], 5)
        self.assertEqual(r["agree"], 1)
        self.assertEqual(r["band_flips"], 1)      # real reprice (band + £)
        self.assertEqual(r["label_only"], 1)      # band label differs, £ agrees
        self.assertEqual(r["material"], 1)
        self.assertEqual(r["absent"], 1)
        # net delta = (1.0-1.0)+(0.32-0.05)+(1.10-1.0)+(0.05-0.05) = 0.37
        self.assertAlmostEqual(r["net_delta"], 0.37, places=4)

    def test_measured_block_excluded(self):
        # a block already rate_source='measured' must not be audited (already authoritative)
        self._blk("2026-08-10T20:00:00", PEAK, 1.0)
        self.st._conn.execute("UPDATE blocks SET rate_source='measured' WHERE block_start=?",
                              ("2026-08-10T20:00:00",))
        self._meas("2026-08-10T20:00:00", 1.0, "STANDARD_RATE")
        self.st._conn.commit()
        r = engine.audit_measured_costs()
        self.assertEqual(r["cached"], 0)


if __name__ == "__main__":
    unittest.main()
