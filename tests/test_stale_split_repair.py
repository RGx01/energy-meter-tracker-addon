"""
test_stale_split_repair.py — BL-9 repair for a stale EV/house split on an UNCAPPED account.

A reprice (e.g. reconcile reverting a negligible smart-charge slot off-peak→peak) rewrote
imp_rate/imp_cost but not the split columns, so imp_rate_ev kept the old band and the home
remainder derived a phantom rate above the tariff peak. repair_stale_iog_split re-derives
the split at the block's own rate; a consistent split is left untouched, and it's idempotent.
The caller (engine) gates it to uncapped accounts, so on a capped tariff imp_rate_ev's
legitimate divergence is never touched — this test covers the store mechanics.
"""

import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from block_store import BlockStore


class TestStaleSplitRepair(unittest.TestCase):

    def setUp(self):
        self.st = BlockStore(":memory:")
        self.st._conn.execute(
            "INSERT OR IGNORE INTO config_periods (id, effective_from, billing_day, "
            "block_minutes, timezone) VALUES (1, '2020-01-01T00:00:00', 1, 30, 'UTC')")
        self.st._conn.commit()

    def _insert(self, start, *, rate, rate_ev, kwh=1.557, kwh_ev=0.23, cost=None):
        cost = kwh * rate if cost is None else cost
        self.st._conn.execute(
            "INSERT INTO blocks (block_start, block_end, meter_id, config_period_id, "
            "imp_kwh, imp_rate, imp_cost, imp_kwh_ev, imp_cost_ev, imp_rate_ev, "
            "imp_ev_band, imp_home_band) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
            (start, start, "electricity_main", 1, kwh, rate, round(cost, 6),
             kwh_ev, round(kwh_ev * rate_ev, 6), rate_ev, "off_peak", "off_peak"))
        self.st._conn.commit()

    def _row(self, start):
        return self.st._conn.execute(
            "SELECT imp_rate, imp_rate_ev, imp_cost_ev, imp_kwh_ev FROM blocks "
            "WHERE block_start=?", (start,)).fetchone()

    def test_repairs_stale_split(self):
        # block reverted to peak (0.323092) but EV split frozen at off-peak (0.05493).
        self._insert("2026-08-16T12:00:00", rate=0.323092, rate_ev=0.05493)
        res = self.st.repair_stale_iog_split()
        self.assertEqual(res["repaired"], 1)
        r = self._row("2026-08-16T12:00:00")
        self.assertAlmostEqual(r["imp_rate_ev"], 0.323092, places=6)     # now = block rate
        self.assertAlmostEqual(r["imp_cost_ev"], 0.23 * 0.323092, places=6)

    def test_consistent_split_untouched(self):
        # EV rate already equals the block rate (healthy uncapped block).
        self._insert("2026-08-16T02:00:00", rate=0.05493, rate_ev=0.05493)
        self.assertEqual(self.st.repair_stale_iog_split()["repaired"], 0)
        self.assertAlmostEqual(self._row("2026-08-16T02:00:00")["imp_rate_ev"],
                               0.05493, places=6)

    def test_idempotent(self):
        self._insert("2026-08-16T12:00:00", rate=0.323092, rate_ev=0.05493)
        self.assertEqual(self.st.repair_stale_iog_split()["repaired"], 1)
        self.assertEqual(self.st.repair_stale_iog_split()["repaired"], 0)  # no longer stale

    def test_dry_run_writes_nothing(self):
        self._insert("2026-08-16T12:00:00", rate=0.323092, rate_ev=0.05493)
        res = self.st.repair_stale_iog_split(dry_run=True)
        self.assertEqual((res["examined"], res["repaired"]), (1, 0))
        self.assertAlmostEqual(self._row("2026-08-16T12:00:00")["imp_rate_ev"],
                               0.05493, places=6)   # untouched


if __name__ == "__main__":
    unittest.main()
