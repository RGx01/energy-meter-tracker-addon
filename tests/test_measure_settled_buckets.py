"""#115 — measure_settled sources the settled cost + Home/EV split from the four-bucket
breakdown on IOG-SMB (no single-label ladder), and leaves the non-SMB single-cost path
intact. The rate itself is snapped to the agreement later, at apply."""
import os, sys, asyncio, unittest
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import engine
from block_store import BlockStore

OFF = 0.05493


def _run(coro):
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


class _Sched:
    def is_empty(self): return False
    def day_rate_bounds(self, ts): return (5.493, 32.3092)


class _BucketClient:
    def __init__(self, node): self._node = node
    async def recover_device_breakdown(self, mpan, missing, **k):
        return {s: self._node for s in missing}
    async def recover_measurement_costs(self, *a, **k):
        raise AssertionError("single-cost path used on SMB")   # must NOT be called


class _SingleCostClient:
    def __init__(self, node): self._node = node
    async def recover_measurement_costs(self, mpan, missing, **k):
        return {s: self._node for s in missing}
    async def recover_device_breakdown(self, *a, **k):
        raise AssertionError("bucket path used on non-SMB")     # must NOT be called


class TestMeasureSettledBuckets(unittest.TestCase):
    SLOT = "2026-09-09T02:00:00"

    def setUp(self):
        self._save = (engine._store, engine._kraken_client, engine._kraken_discovery,
                      engine._kraken_rate_schedules, engine._kraken_current_agreement_from,
                      engine._MEASURED_FETCH_ENABLED)
        self.st = BlockStore(":memory:")
        engine._store = self.st
        engine._kraken_current_agreement_from = "2026-08-01T00:00:00"
        engine._MEASURED_FETCH_ENABLED = True
        self.st._conn.execute(
            "INSERT OR IGNORE INTO config_periods (id, effective_from, billing_day, "
            "block_minutes, timezone) VALUES (1,'2020-01-01T00:00:00',1,30,'UTC')")
        self.st._conn.execute(
            "INSERT INTO blocks (block_start, block_end, meter_id, config_period_id, "
            "imp_kwh, imp_kwh_api, imp_rate, imp_cost, rate_source, rate_corrected) "
            "VALUES (?,?,?,1,?,?,?,?,'reconciled',0)",
            (self.SLOT, self.SLOT, "electricity_main", 5.276, 5.276, OFF, round(5.276*OFF, 6)))
        self.st.upsert_dispatch_slot(self.SLOT, off_peak=True, source="smart-charge-completed")
        self.st._conn.commit()

    def tearDown(self):
        (engine._store, engine._kraken_client, engine._kraken_discovery,
         engine._kraken_rate_schedules, engine._kraken_current_agreement_from,
         engine._MEASURED_FETCH_ENABLED) = self._save

    def test_smb_sources_cost_and_split_from_buckets(self):
        engine._kraken_discovery = {"import": {"mpan": "m", "tariff_code": "IOG-SMB-FIX-12M"}}
        engine._kraken_rate_schedules = {"import": _Sched(), "ev_device_off_peak": _Sched()}
        node = {"home_kwh": 1.980, "ev_kwh": 3.296,
                "home_cost": round(1.980*OFF, 6), "ev_cost": round(3.296*OFF, 6),
                "home_rate": OFF, "ev_rate": OFF,
                "home_rate_exc": round(OFF*0.95238, 6), "ev_rate_exc": round(OFF*0.95238, 6),
                "home_band": "off_peak", "ev_band": "off_peak"}
        engine._kraken_client = _BucketClient(node)
        res = _run(engine.measure_settled_dispatched_blocks())
        self.assertEqual(res["stored"], 1)
        mc = self.st._conn.execute(
            "SELECT cost_incl FROM measured_cost WHERE slot_start=?", (self.SLOT,)).fetchone()
        self.assertAlmostEqual(mc["cost_incl"], round(1.980*OFF, 6) + round(3.296*OFF, 6), places=5)
        bd = self.st.get_measured_breakdown(self.SLOT)
        self.assertAlmostEqual(bd["ev_kwh"], 3.296, places=3)     # split from the BILL
        self.assertAlmostEqual(bd["home_kwh"], 1.980, places=3)

    def test_non_smb_keeps_single_cost_path(self):
        engine._kraken_discovery = {"import": {"mpan": "m", "tariff_code": "INTELLI-VAR"}}
        engine._kraken_rate_schedules = {"import": _Sched()}   # no ev_device buckets -> not SMB
        node = {"cost_incl": round(5.276*OFF, 6), "cost_excl": round(5.276*OFF*0.95238, 6),
                "off_peak": True, "kwh": 5.276}
        engine._kraken_client = _SingleCostClient(node)
        res = _run(engine.measure_settled_dispatched_blocks())
        self.assertEqual(res["stored"], 1)
        mc = self.st._conn.execute(
            "SELECT cost_incl, label FROM measured_cost WHERE slot_start=?", (self.SLOT,)).fetchone()
        self.assertAlmostEqual(mc["cost_incl"], round(5.276*OFF, 6), places=5)
        self.assertEqual(mc["label"], "OFF_PEAK")


if __name__ == "__main__":
    unittest.main()
