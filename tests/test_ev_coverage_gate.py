"""
test_ev_coverage_gate.py — BL-9 charts: the synthetic dispatch-EV is coverage-based.
It fills a slot only when the PHYSICAL EV device has no block there. So: an active EV
meter suppresses it (as before), a retired one lets it take over from the cutover, and
a battery / other sub-meter never blocks it.
"""

import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from block_store import BlockStore
from energy_charts import _dispatch_ev_slot_map, _ev_meter_id

SLOT = "2026-01-01T13:00:00"


def _store_with_dispatch():
    st = BlockStore(":memory:")
    st._conn.execute(
        "INSERT INTO dispatch_history (slot_start, kind, provider, source, energy_kwh, "
        "first_seen, last_seen) VALUES (?, 'completed', 'test', NULL, 2.0, ?, ?)",
        (SLOT, SLOT, SLOT))
    st._conn.commit()
    return st


def _block(extra_meters=None):
    meters = {"electricity_main": {"channels": {"import": {
        "kwh": 3.0, "kwh_total": 3.0, "cost": 0.17, "rate": 0.0567}}}}
    if extra_meters:
        meters.update(extra_meters)
    return {"start": SLOT, "meters": meters}


CFG_NO_EV  = {"meters": {"electricity_main": {"meta": {}}}}
CFG_EV     = {"meters": {"electricity_main": {"meta": {}},
                         "sub_ev": {"meta": {"sub_meter": True, "meter_type": "ev_charger"}}}}
CFG_BAT    = {"meters": {"electricity_main": {"meta": {}},
                         "sub_bat": {"meta": {"sub_meter": True, "meter_type": "battery"}}}}


class TestEvCoverageGate(unittest.TestCase):

    def test_ev_meter_id(self):
        self.assertIsNone(_ev_meter_id(CFG_NO_EV))
        self.assertEqual(_ev_meter_id(CFG_EV), "sub_ev")
        self.assertIsNone(_ev_meter_id(CFG_BAT))

    def test_no_ev_device_synthesises(self):
        m = _dispatch_ev_slot_map(_store_with_dispatch(), [_block()], CFG_NO_EV)
        self.assertIn(SLOT, m)

    def test_active_ev_device_suppresses(self):
        # EV device HAS a block at the slot → covered → no synthetic
        blk = _block({"sub_ev": {"channels": {"import": {"kwh": 2.0}}}})
        m = _dispatch_ev_slot_map(_store_with_dispatch(), [blk], CFG_EV)
        self.assertNotIn(SLOT, m)

    def test_retired_ev_device_lets_synthetic_take_over(self):
        # EV device configured but has NO block at the slot (retired/decommissioned)
        m = _dispatch_ev_slot_map(_store_with_dispatch(), [_block()], CFG_EV)
        self.assertIn(SLOT, m)

    def test_battery_does_not_block_ev_synthetic(self):
        blk = _block({"sub_bat": {"channels": {"import": {"kwh": 1.0}}}})
        m = _dispatch_ev_slot_map(_store_with_dispatch(), [blk], CFG_BAT)
        self.assertIn(SLOT, m)


if __name__ == "__main__":
    unittest.main()
