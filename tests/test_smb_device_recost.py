"""4.5.7 one-off device-cost heal: re-cost physical sub-meter blocks (ev_charger / battery) to
their parent MAIN rate wherever they DRIFTED — the measured settlement flipped the main off-peak
but left the device at the stale peak. Idempotent; no-op for a synthetic-EV account (no sub-meters).
"""
import os, sys, asyncio, unittest
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import engine
from block_store import BlockStore

OFF, PK = 0.05493, 0.323092


class TestSmbDeviceRecost(unittest.TestCase):
    def setUp(self):
        self._save = engine._store
        self.st = BlockStore(":memory:"); engine._store = self.st
        self.st._conn.execute(
            "INSERT OR IGNORE INTO config_periods (id, effective_from, billing_day, "
            "block_minutes, timezone) VALUES (1,'2020-01-01T00:00:00',1,30,'UTC')")

    def tearDown(self):
        engine._store = self._save

    def _main(self, slot, kwh, rate):
        self.st._conn.execute(
            "INSERT INTO blocks (block_start, block_end, meter_id, config_period_id, "
            "imp_kwh, imp_rate, imp_cost, imp_rate_exc, rate_source) "
            "VALUES (?,?,?,1,?,?,?,?,'measured')",
            (slot, slot, "electricity_main", kwh, rate, round(kwh * rate, 6), round(rate / 1.05, 6)))

    def _dev(self, slot, meter, kwh, rate):
        self.st._conn.execute(
            "INSERT INTO blocks (block_start, block_end, meter_id, config_period_id, "
            "imp_kwh, imp_rate, imp_cost) VALUES (?,?,?,1,?,?,?)",
            (slot, slot, meter, kwh, rate, round(kwh * rate, 6)))

    def _dev_state(self, slot, meter):
        return self.st._conn.execute(
            "SELECT imp_rate, imp_cost FROM blocks WHERE meter_id=? AND block_start=?",
            (meter, slot)).fetchone()

    def test_heals_drifted_devices_and_is_idempotent(self):
        slot = "2026-09-06T10:00:00"
        self._main(slot, 3.668, OFF)                 # main settled OFF-peak
        self._dev(slot, "ev_charger", 1.39, PK)      # device stuck at PEAK (drift)
        self._dev(slot, "house_battery", 0.5, PK)    # battery also drifted
        self.st._conn.commit()
        res = asyncio.run(engine.run_smb_device_recost())
        self.assertTrue(res["ok"]); self.assertEqual(res["re_costed"], 2)
        ev = self._dev_state(slot, "ev_charger")
        self.assertAlmostEqual(ev["imp_rate"], OFF, places=5)
        self.assertAlmostEqual(ev["imp_cost"], round(1.39 * OFF, 6), places=6)
        self.assertAlmostEqual(self._dev_state(slot, "house_battery")["imp_rate"], OFF, places=5)
        # idempotent — nothing left to re-cost
        self.assertEqual(asyncio.run(engine.run_smb_device_recost(force=True))["re_costed"], 0)

    def test_noop_when_device_already_matches_main(self):
        slot = "2026-09-06T02:00:00"
        self._main(slot, 2.0, OFF); self._dev(slot, "ev_charger", 1.0, OFF)
        self.st._conn.commit()
        self.assertEqual(asyncio.run(engine.run_smb_device_recost(force=True))["re_costed"], 0)

    def test_synthetic_only_account_is_noop(self):
        slot = "2026-09-06T02:00:00"
        self._main(slot, 2.0, OFF)                    # no sub-meter block at all
        self.st._conn.commit()
        self.assertEqual(asyncio.run(engine.run_smb_device_recost(force=True))["re_costed"], 0)


if __name__ == "__main__":
    unittest.main()
