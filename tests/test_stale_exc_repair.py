"""
test_stale_exc_repair.py — BL-23/BL-9 one-off repair for stale ex-VAT.

A pre-fix reconcile rewrote imp_rate without re-stamping imp_rate_exc/imp_cost_exc,
leaving some blocks with an ex-VAT rate inconsistent with their inc rate (e.g. an
off-peak exc rate on a block reverted to peak). repair_stale_exc finds those
(implausible exc/inc ratio) and re-derives them from the VAT calendar; healthy blocks
and genuine VAT bands are left untouched, and it's idempotent.
"""

import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from block_store import BlockStore


class TestStaleExcRepair(unittest.TestCase):

    def setUp(self):
        self.st = BlockStore(":memory:")
        self.st._conn.execute(
            "INSERT OR IGNORE INTO config_periods (id, effective_from, billing_day, "
            "block_minutes, timezone) VALUES (1, '2020-01-01T00:00:00', 1, 30, 'UTC')")
        self.st._conn.commit()

    def _insert(self, start, *, rate, rate_exc, cost, kwh=1.0):
        self.st._conn.execute(
            "INSERT INTO blocks (block_start, block_end, meter_id, config_period_id, "
            "imp_kwh, imp_rate, imp_cost, imp_rate_exc, imp_cost_exc, exc_source) "
            "VALUES (?,?,?,?,?,?,?,?,?,?)",
            (start, start, "electricity_main", 1, kwh, rate, cost, rate_exc,
             round(cost * (rate_exc / rate), 6) if rate else None, "tariff"))
        self.st._conn.commit()

    def _row(self, start):
        return self.st._conn.execute(
            "SELECT imp_rate, imp_rate_exc, imp_cost, imp_cost_exc, exc_source "
            "FROM blocks WHERE block_start=?", (start,)).fetchone()

    def test_repairs_stale_row(self):
        # peak inc 0.323092 but exc left at the OFF-PEAK rate 0.052314 → ratio 0.16.
        self._insert("2026-08-13T20:00:00", rate=0.323092, rate_exc=0.052314,
                     cost=0.052664, kwh=0.163)
        res = self.st.repair_stale_exc()
        self.assertEqual(res["repaired"], 1)
        r = self._row("2026-08-13T20:00:00")
        self.assertAlmostEqual(r["imp_rate_exc"], 0.323092 / 1.05, places=5)
        self.assertAlmostEqual(r["imp_cost_exc"], 0.052664 / 1.05, places=5)
        self.assertEqual(r["exc_source"], "tariff-repair")

    def test_healthy_row_untouched(self):
        # correct 5% VAT (ratio 0.952) — must not be touched.
        self._insert("2026-08-13T02:00:00", rate=0.05493, rate_exc=0.052314,
                     cost=0.05493)
        res = self.st.repair_stale_exc()
        self.assertEqual(res["repaired"], 0)
        r = self._row("2026-08-13T02:00:00")
        self.assertAlmostEqual(r["imp_rate_exc"], 0.052314, places=6)   # unchanged
        self.assertEqual(r["exc_source"], "tariff")

    def test_genuine_20pct_vat_not_flagged(self):
        # A real 20% VAT band is ratio 0.833 — above the 0.80 floor, so NOT repaired.
        self._insert("2026-08-13T03:00:00", rate=0.30, rate_exc=0.25, cost=0.30)
        self.assertEqual(self.st.repair_stale_exc()["repaired"], 0)

    def test_idempotent(self):
        self._insert("2026-08-13T20:00:00", rate=0.323092, rate_exc=0.052314,
                     cost=0.052664, kwh=0.163)
        self.assertEqual(self.st.repair_stale_exc()["repaired"], 1)
        self.assertEqual(self.st.repair_stale_exc()["repaired"], 0)   # no longer stale

    def test_dry_run_writes_nothing(self):
        self._insert("2026-08-13T20:00:00", rate=0.323092, rate_exc=0.052314,
                     cost=0.052664, kwh=0.163)
        res = self.st.repair_stale_exc(dry_run=True)
        self.assertEqual(res["examined"], 1)
        self.assertEqual(res["repaired"], 0)
        self.assertAlmostEqual(self._row("2026-08-13T20:00:00")["imp_rate_exc"],
                               0.052314, places=6)   # untouched


if __name__ == "__main__":
    unittest.main()
