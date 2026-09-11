"""#115 — settled = authority. apply_measured_settled applies the bill for EVERY cached
settled slot (both bands), with NO age-gate and NO review-flag. apply_measured_to_block
NEVER derives a cost/kWh rate: it snaps to the tariff agreement, or skips the slot.
(Replaces the retired test_measured_age_gate / test_measured_apply_pass / test_bl53_step2.)"""
import os, sys, unittest
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import engine
from block_store import BlockStore

OFF, PK = 0.05493, 0.323092


class _Sched:
    def is_empty(self): return False
    def day_rate_bounds(self, ts): return (5.493, 32.3092)   # pence (off, peak)


class _NoBoundsSched:
    def is_empty(self): return False
    def day_rate_bounds(self, ts): return (None, None)


class TestMeasuredSettledV2(unittest.TestCase):
    def setUp(self):
        self._save = (engine._store, engine._kraken_rate_schedules, engine._kraken_discovery,
                      engine._kraken_current_agreement_from, engine._MEASURED_APPLY)
        self.st = BlockStore(":memory:")
        engine._store = self.st
        engine._kraken_rate_schedules = {"import": _Sched()}
        engine._kraken_discovery = {"import": {"mpan": "m"}}
        engine._kraken_current_agreement_from = "2026-08-01T00:00:00"
        engine._MEASURED_APPLY = True
        self.st._conn.execute(
            "INSERT OR IGNORE INTO config_periods (id, effective_from, billing_day, "
            "block_minutes, timezone) VALUES (1,'2020-01-01T00:00:00',1,30,'UTC')")

    def tearDown(self):
        (engine._store, engine._kraken_rate_schedules, engine._kraken_discovery,
         engine._kraken_current_agreement_from, engine._MEASURED_APPLY) = self._save

    def _blk(self, slot, kwh, rate):
        self.st._conn.execute(
            "INSERT INTO blocks (block_start, block_end, meter_id, config_period_id, "
            "imp_kwh, imp_rate, imp_cost, rate_source, rate_corrected) "
            "VALUES (?,?,?,1,?,?,?,'reconciled',0)",
            (slot, slot, "electricity_main", kwh, rate, round(kwh * rate, 6)))
        self.st._conn.commit()

    def _meas(self, slot, cost_incl, label):
        self.st.upsert_measured_cost(slot, mpan="m", cost_incl=cost_incl,
                                     cost_excl=round(cost_incl * 0.95238, 6), label=label)

    def _rate(self, slot):
        return self.st._conn.execute(
            "SELECT imp_rate, rate_source, needs_review FROM blocks WHERE block_start=?",
            (slot,)).fetchone()

    def test_settlement_recosts_physical_device_submeter(self):
        # SMB device-cost fix: a settled block whose ev_charger sub-meter was costed by PASS 2
        # at the pre-settlement PEAK must have the device RE-COSTED off-peak when the bill
        # settles the slot off-peak (device == main == bill) — not left at the stale peak.
        slot = "2026-09-08T04:00:00"
        self._blk(slot, 3.0, PK)                              # main provisionally PEAK
        self.st._conn.execute(                               # device sub-meter, PASS-2 costed PEAK
            "INSERT INTO blocks (block_start, block_end, meter_id, config_period_id, "
            "imp_kwh, imp_rate, imp_cost, rate_source, rate_corrected) "
            "VALUES (?,?,?,1,?,?,?,'reconciled',0)",
            (slot, slot, "ev_charger", 2.0, PK, round(2.0 * PK, 6)))
        self.st._conn.commit()
        self._meas(slot, round(3.0 * OFF, 6), "OFF_PEAK")     # bill: off-peak
        engine.apply_measured_settled()
        self.assertAlmostEqual(self._rate(slot)["imp_rate"], OFF, places=5)   # main off-peak
        dev = self.st._conn.execute(
            "SELECT imp_rate, imp_cost FROM blocks WHERE meter_id='ev_charger' AND block_start=?",
            (slot,)).fetchone()
        self.assertAlmostEqual(dev["imp_rate"], OFF, places=5)                # device re-costed
        self.assertAlmostEqual(dev["imp_cost"], round(2.0 * OFF, 6), places=6)

    def test_applies_both_bands_no_defer_no_flag(self):
        peak = "2026-09-09T13:00:00"           # recent, currently off-peak, bill says PEAK
        self._blk(peak, 3.0, OFF); self._meas(peak, round(3.0 * PK, 6), "STANDARD_RATE")
        off = "2026-09-08T02:00:00"            # off-peak, bill agrees
        self._blk(off, 2.0, OFF); self._meas(off, round(2.0 * OFF, 6), "OFF_PEAK")
        res = engine.apply_measured_settled()
        self.assertEqual(res["applied"], 2)              # BOTH applied (no age-gate defer)
        self.assertNotIn("deferred", res)
        self.assertNotIn("flagged", res)
        pr = self._rate(peak)
        self.assertAlmostEqual(pr["imp_rate"], PK, places=5)     # snapped to CLEAN agreement peak
        self.assertEqual(pr["rate_source"], "measured")
        self.assertEqual(pr["needs_review"] or 0, 0)             # NOT review-flagged
        self.assertAlmostEqual(self._rate(off)["imp_rate"], OFF, places=5)

    def test_no_agreement_bounds_does_not_settle(self):
        engine._kraken_rate_schedules = {"import": _NoBoundsSched()}
        slot = "2026-09-09T13:00:00"
        self._blk(slot, 3.0, OFF); self._meas(slot, round(3.0 * PK, 6), "STANDARD_RATE")
        res = engine.apply_measured_settled()
        self.assertEqual(res["applied"], 0)              # skipped — never a cost/kWh rate
        r = self._rate(slot)
        self.assertAlmostEqual(r["imp_rate"], OFF, places=5)     # unchanged
        self.assertNotEqual(r["rate_source"], "measured")        # NOT settled


if __name__ == "__main__":
    unittest.main()
