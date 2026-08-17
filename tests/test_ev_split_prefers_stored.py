"""
BL-9 unification: every EV/house cost surface must agree with the bill. The choke point
is energy_charts._dispatch_ev_slot_map — it feeds BOTH the billing summary's
'Breakdown by meter → EV (from dispatch)' line AND the day chart. It now prefers the
STORED, bill-authoritative split (imp_kwh_ev/imp_cost_ev/imp_rate_ev — the 4-rate figures)
over the pro-rata carve, falling back to pro-rata only where a block has no stored split.
On an uncapped account the two are identical, so it stays byte-identical there.
"""

import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from block_store import BlockStore
from energy_charts import _dispatch_ev_slot_map

SLOT = "2026-01-01T13:00:00"
CFG = {"meters": {"electricity_main": {"meta": {}}}}


def _store():
    st = BlockStore(":memory:")
    st._conn.execute(
        "INSERT INTO dispatch_history (slot_start, kind, provider, source, energy_kwh, "
        "first_seen, last_seen) VALUES (?, 'completed', 'test', NULL, 2.0, ?, ?)",
        (SLOT, SLOT, SLOT))
    st._conn.commit()
    return st


def _block(imp):
    return {"start": SLOT, "meters": {"electricity_main": {"channels": {"import": imp}}}}


class TestDispatchEvSlotMapPrefersStored(unittest.TestCase):

    def test_capped_uses_stored_cost_and_rate(self):
        # Block blended at 0.21 (£0.63/3kWh); the STORED split prices the 2 kWh EV at
        # peak 0.25 (£0.50), NOT the pro-rata £0.42 at the blended 0.21.
        m = _dispatch_ev_slot_map(_store(), [_block({
            "kwh": 3.0, "kwh_total": 3.0, "cost": 0.63, "rate": 0.21,
            "kwh_ev": 2.0, "cost_ev": 0.50, "rate_ev": 0.25})], CFG)
        self.assertIn(SLOT, m)
        self.assertAlmostEqual(m[SLOT]["kwh"], 2.0)
        self.assertAlmostEqual(m[SLOT]["cost"], 0.50)    # stored, not pro-rata 0.42
        self.assertAlmostEqual(m[SLOT]["rate"], 0.25)    # rate_ev, not the blended 0.21

    def test_no_stored_split_falls_back_to_prorata(self):
        # No stored columns (un-backfilled / non-IOG) → pro-rata carve at the block rate.
        m = _dispatch_ev_slot_map(_store(), [_block({
            "kwh": 3.0, "kwh_total": 3.0, "cost": 0.63, "rate": 0.21})], CFG)
        self.assertAlmostEqual(m[SLOT]["cost"], 0.42)    # 0.63 * 2/3
        self.assertAlmostEqual(m[SLOT]["rate"], 0.21)

    def test_uncapped_stored_equals_prorata(self):
        # Uncapped IOG: EV and house share the off-peak rate, so stored == pro-rata.
        m = _dispatch_ev_slot_map(_store(), [_block({
            "kwh": 3.0, "kwh_total": 3.0, "cost": 0.165, "rate": 0.055,
            "kwh_ev": 2.0, "cost_ev": 0.11, "rate_ev": 0.055})], CFG)
        self.assertAlmostEqual(m[SLOT]["cost"], 0.11)    # == pro-rata 0.165*2/3
        self.assertAlmostEqual(m[SLOT]["rate"], 0.055)

    def test_stored_grid_clipped(self):
        # Stored EV kWh exceeding the slot's grid import is clipped, cost scaled with it.
        m = _dispatch_ev_slot_map(_store(), [_block({
            "kwh": 1.5, "kwh_total": 1.5, "cost": 0.30, "rate": 0.20,
            "kwh_ev": 2.0, "cost_ev": 0.50, "rate_ev": 0.25})], CFG)
        self.assertAlmostEqual(m[SLOT]["kwh"], 1.5)       # clipped to grid
        self.assertAlmostEqual(m[SLOT]["cost"], 0.50 * (1.5 / 2.0))   # cost scaled


if __name__ == "__main__":
    unittest.main()
