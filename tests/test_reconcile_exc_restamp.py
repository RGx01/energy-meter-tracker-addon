"""
test_reconcile_exc_restamp.py — BL-23/BL-9 root-cause fix.

The settlement reconciliation pass rewrites imp_rate/imp_cost when it reverts or
restores a slot, but historically LEFT imp_rate_exc/imp_cost_exc at the OLD band's
value. That stale ex-VAT figure (e.g. an off-peak exc rate on a block reverted to
peak) then showed a wrong rate in the billing summary's EV/Home split and understated
Total (exc). The reconcile now re-stamps the exc columns from the new rate — or NULLs
them (fall back to inc÷VAT) when no exc schedule covers the slot — so exc is never
left inconsistent with the inc rate it was rewritten to.
"""

import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import engine
from block_store import BlockStore

PEAK_INC = 32.3092          # pence (schedule.resolve)
PEAK_EXC = 32.3092 / 1.05   # pence (schedule.exc.resolve, 5% VAT)


class _ExcSched:
    def resolve(self, ts):
        return PEAK_EXC


class _Sched:
    """Import schedule stub in pence, optionally with an .exc sub-schedule."""
    def __init__(self, with_exc=True):
        if with_exc:
            self.exc = _ExcSched()

    def is_empty(self):
        return False

    def off_peak_rate_near(self, ts):
        return 5.493

    def resolve(self, ts):
        return PEAK_INC


class TestReconcileRestampsExc(unittest.IsolatedAsyncioTestCase):

    SLOT = "2020-01-01T20:00:00"

    def _seed_stale(self, st):
        # Block currently off-peak with a STALE off-peak exc, planned-only dispatch
        # (no started/completed) → reconcile reverts it to peak.
        st._conn.execute(
            "INSERT OR IGNORE INTO config_periods (id, effective_from, billing_day, "
            "block_minutes, timezone) VALUES (1, '2020-01-01T00:00:00', 1, 30, 'UTC')")
        st._conn.execute(
            "INSERT INTO blocks (block_start, block_end, meter_id, config_period_id, "
            "imp_kwh, imp_rate, imp_cost, imp_rate_exc, imp_cost_exc, exc_source, "
            "rate_corrected, imp_kwh_api) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
            (self.SLOT, self.SLOT, "electricity_main", 1, 1.0, 0.05493, 0.05493,
             0.052314, 0.052314, "tariff", 0, 1.0))
        st.upsert_dispatch_slot(self.SLOT, off_peak=True, provider="Myenergi",
                                source="smart-charge", state="planned")
        st.record_dispatch_history(self.SLOT, "planned", provider="Myenergi")
        st._conn.commit()

    async def _run(self, st, with_exc=True):
        engine._store = st
        engine._RECONCILE_SETTLE_HOURS = 0.0
        engine._kraken_rate_schedules = {"import": _Sched(with_exc)}
        try:
            return await engine.reconcile_dispatch_overlay()
        finally:
            engine._store = None

    def _row(self, st):
        return st._conn.execute(
            "SELECT imp_rate, imp_rate_exc, imp_cost_exc FROM blocks WHERE block_start=?",
            (self.SLOT,)).fetchone()

    async def test_revert_restamps_exc_to_new_band(self):
        st = BlockStore(":memory:")
        self._seed_stale(st)
        res = await self._run(st, with_exc=True)
        self.assertEqual(res["reverted"], 1)
        r = self._row(st)
        self.assertAlmostEqual(r["imp_rate"], 0.323092, places=5)          # peak inc
        # exc re-stamped to the PEAK exc — NOT left at the stale off-peak 0.052314
        self.assertAlmostEqual(r["imp_rate_exc"], 0.323092 / 1.05, places=5)
        self.assertAlmostEqual(r["imp_cost_exc"], 1.0 * 0.323092 / 1.05, places=5)

    async def test_revert_nulls_exc_when_no_exc_schedule(self):
        st = BlockStore(":memory:")
        self._seed_stale(st)
        res = await self._run(st, with_exc=False)
        self.assertEqual(res["reverted"], 1)
        r = self._row(st)
        self.assertAlmostEqual(r["imp_rate"], 0.323092, places=5)
        # no exc schedule → stale exc NULLed (falls back to inc÷VAT), never left wrong
        self.assertIsNone(r["imp_rate_exc"])
        self.assertIsNone(r["imp_cost_exc"])


if __name__ == "__main__":
    unittest.main()
