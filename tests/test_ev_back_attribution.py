"""
test_ev_back_attribution.py — BL-9 fix: back-attribute the EV/Home split on blocks whose
COMPLETED dispatch arrived AFTER the block was priced.

`imp_kwh_ev` is stamped once, at pricing, from the completed dispatch known then. Octopus
reports a slot's completed energy hours later — often after the block was priced with only a
'planned' dispatch — so the real charge is never attributed: it sits in Home, and the
reconcile re-stamp skips it (it only touches rows where imp_kwh_ev IS NOT NULL). Two
instances that priced the same slot at different times relative to that late record then
disagree permanently (the prod vs prod-dev split mismatch). `_attribute_missing_ev_split`
re-runs the IOG split on such blocks so they converge deterministically.
"""

import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import engine
from block_store import BlockStore

OFF = 0.05493


class _Sched:
    def is_empty(self):
        return False

    def off_peak_rate_near(self, ts):
        return 5.493

    def resolve(self, ts):
        return 5.493


class TestEvBackAttribution(unittest.TestCase):

    SLOT = "2026-08-17T02:00:00"

    def setUp(self):
        self.st = BlockStore(":memory:")
        self.st._conn.execute(
            "INSERT OR IGNORE INTO config_periods (id, effective_from, billing_day, "
            "block_minutes, timezone) VALUES (1, '2020-01-01T00:00:00', 1, 30, 'UTC')")
        self.st._conn.execute(
            "INSERT OR IGNORE INTO meters (meter_id, config_period_id, is_sub_meter) "
            "VALUES ('electricity_main', 1, 0)")
        engine._store = self.st
        engine._kraken_rate_schedules = {"import": _Sched()}

    def tearDown(self):
        engine._store = None
        engine._kraken_rate_schedules = {}

    def _block(self, slot=None, imp_kwh=3.0, source=None, ev=None):
        slot = slot or self.SLOT
        self.st._conn.execute(
            "INSERT INTO blocks (block_start, block_end, meter_id, config_period_id, "
            "imp_kwh, imp_rate, imp_cost, imp_kwh_ev, source) VALUES (?,?,?,1,?,?,?,?,?)",
            (slot, slot, "electricity_main", imp_kwh, OFF, round(imp_kwh * OFF, 6), ev, source))

    def _completed(self, energy=-2.0, slot=None):
        slot = slot or self.SLOT
        self.st._conn.execute(
            "INSERT INTO dispatch_history (slot_start, kind, energy_kwh, first_seen, "
            "last_seen) VALUES (?, 'completed', ?, ?, ?)", (slot, energy, slot, slot))
        self.st._conn.commit()

    def _row(self, slot=None):
        return self.st._conn.execute(
            "SELECT imp_kwh_ev, imp_cost_ev, imp_rate_ev, imp_ev_band FROM blocks "
            "WHERE block_start=? AND meter_id='electricity_main'",
            (slot or self.SLOT,)).fetchone()

    def test_attributes_completed_after_pricing(self):
        self._block(imp_kwh=3.0, ev=None)
        self._completed(energy=-2.0)
        self.assertEqual(engine._attribute_missing_ev_split(), 1)
        r = self._row()
        self.assertAlmostEqual(r["imp_kwh_ev"], 2.0, places=5)
        self.assertAlmostEqual(r["imp_rate_ev"], OFF, places=5)
        self.assertAlmostEqual(r["imp_cost_ev"], round(2.0 * OFF, 6), places=6)
        self.assertEqual(r["imp_ev_band"], "off_peak")

    def test_clips_ev_to_grid_import(self):
        self._block(imp_kwh=1.5, ev=None)
        self._completed(energy=-4.0)
        engine._attribute_missing_ev_split()
        self.assertAlmostEqual(self._row()["imp_kwh_ev"], 1.5, places=5)

    def test_noop_without_completed_dispatch(self):
        self._block(imp_kwh=3.0, ev=None)
        self.assertEqual(engine._attribute_missing_ev_split(), 0)
        self.assertIsNone(self._row()["imp_kwh_ev"])

    def test_leaves_already_attributed(self):
        self._block(imp_kwh=3.0, ev=1.0)
        self._completed(energy=-2.0)
        self.assertEqual(engine._attribute_missing_ev_split(), 0)
        self.assertAlmostEqual(self._row()["imp_kwh_ev"], 1.0, places=5)

    def test_skips_imported_history(self):
        self._block(imp_kwh=3.0, ev=None, source="imported_api")
        self._completed(energy=-2.0)
        self.assertEqual(engine._attribute_missing_ev_split(), 0)
        self.assertIsNone(self._row()["imp_kwh_ev"])

    def test_idempotent(self):
        self._block(imp_kwh=3.0, ev=None)
        self._completed(energy=-2.0)
        self.assertEqual(engine._attribute_missing_ev_split(), 1)
        self.assertEqual(engine._attribute_missing_ev_split(), 0)

    def test_noop_when_capped(self):
        engine._kraken_rate_schedules = {"import": _Sched(),
                                         "ev_device_off_peak": _Sched(),
                                         "ev_device_peak": _Sched()}
        self._block(imp_kwh=3.0, ev=None)
        self._completed(energy=-2.0)
        self.assertEqual(engine._attribute_missing_ev_split(), 0)


if __name__ == "__main__":
    unittest.main()
