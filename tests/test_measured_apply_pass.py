"""
test_measured_apply_pass.py — BL-53 step 3 apply_measured_settled.

Applies cached measured costs to the whole backlog; material discrepancies are
applied AND review-flagged (dispute trail), agreements applied silently.
"""
import os, sys, unittest
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import engine
from block_store import BlockStore

PEAK, OFF = 0.323092, 0.05493


class _Sched:
    def is_empty(self): return False
    def day_rate_bounds(self, ts): return (5.493, 32.3092)


class TestApplyPass(unittest.TestCase):
    def setUp(self):
        self._save = (engine._store, engine._kraken_discovery,
                      engine._kraken_rate_schedules, engine._MEASURED_APPLY)
        self.st = BlockStore(":memory:")
        engine._store = self.st
        engine._kraken_discovery = {"import": {"mpan": "X"}}
        engine._kraken_rate_schedules = {"import": _Sched()}
        engine._MEASURED_APPLY = True
        self.st._conn.execute(
            "INSERT OR IGNORE INTO config_periods (id, effective_from, billing_day, "
            "block_minutes, timezone) VALUES (1,'2020-01-01T00:00:00',1,30,'UTC')")

    def tearDown(self):
        (engine._store, engine._kraken_discovery,
         engine._kraken_rate_schedules, engine._MEASURED_APPLY) = self._save

    def _blk(self, slot, rate, kwh=1.0, evk=0.5):
        self.st._conn.execute(
            "INSERT INTO blocks (block_start,block_end,meter_id,config_period_id,"
            "imp_kwh,imp_rate,imp_cost,imp_kwh_ev,imp_kwh_api,rate_source) "
            "VALUES (?,?,?,?,?,?,?,?,?,?)",
            (slot, slot, "electricity_main", 1, kwh, rate, round(kwh*rate,6), evk, kwh,
             "reconciled"))

    def _meas(self, slot, cost_incl, label, kwh=1.0):
        self.st.upsert_measured_cost(slot, mpan="X", cost_incl=cost_incl,
                                     cost_excl=round(cost_incl/1.05,6), label=label, kwh=kwh)

    def test_backlog_apply_and_flag(self):
        # agree: off-peak block, off-peak bill, cost matches
        self._blk("2026-08-10T02:00:00", OFF); self._meas("2026-08-10T02:00:00", OFF, "OFF_PEAK")
        # material band-flip (the 14/08 signature): off-peak block, billed peak
        self._blk("2026-08-14T19:00:00", OFF); self._meas("2026-08-14T19:00:00", PEAK, "STANDARD_RATE")
        self.st._conn.commit()
        res = engine.apply_measured_settled()
        self.assertEqual(res["applied"], 2)
        self.assertEqual(res["flagged"], 1)          # only the material flip
        rows = {r["block_start"]: r for r in self.st._conn.execute(
            "SELECT block_start, imp_rate, rate_source, needs_review, review_reason "
            "FROM blocks WHERE meter_id='electricity_main'")}
        a = rows["2026-08-10T02:00:00"]; f = rows["2026-08-14T19:00:00"]
        # both now measured
        self.assertEqual(a["rate_source"], "measured")
        self.assertEqual(f["rate_source"], "measured")
        # agree: no review flag, rate unchanged (still off-peak)
        self.assertEqual(a["needs_review"], 0)
        self.assertAlmostEqual(a["imp_rate"], OFF, places=5)
        # material flip: applied to peak AND review-flagged with a reason
        self.assertAlmostEqual(f["imp_rate"], PEAK, places=5)
        self.assertEqual(f["needs_review"], 1)
        self.assertIn("billing error", f["review_reason"])

    def test_idempotent(self):
        self._blk("2026-08-14T19:00:00", OFF); self._meas("2026-08-14T19:00:00", PEAK, "STANDARD_RATE")
        self.st._conn.commit()
        self.assertEqual(engine.apply_measured_settled()["applied"], 1)
        self.assertEqual(engine.apply_measured_settled()["applied"], 0)  # measured now → excluded


if __name__ == "__main__":
    unittest.main()
