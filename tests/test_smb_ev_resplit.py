"""4.5.9 one-off EV-split heal: re-split settled OFF-PEAK slots where Octopus's four-bucket
EV_DEVICE total absorbed a concurrent HOME-BATTERY grid charge inside a dispatch window, so
imp_kwh_ev over-read the car's actual session. The car's completed dispatch is the measured
ceiling; the excess EV kWh moves back to HOUSE at the same off-peak rate (block TOTAL kWh +
cost byte-identical). Cost-neutral, idempotent, skips mixed/boundary slots."""
import os, sys, asyncio, unittest
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import engine
from block_store import BlockStore

OFF, PK = 0.05493, 0.323092


class TestSmbEvResplit(unittest.TestCase):
    def setUp(self):
        self._save = engine._store
        self.st = BlockStore(":memory:"); engine._store = self.st
        self.st._conn.execute(
            "INSERT OR IGNORE INTO config_periods (id, effective_from, billing_day, "
            "block_minutes, timezone) VALUES (1,'2020-01-01T00:00:00',1,30,'UTC')")

    def tearDown(self):
        engine._store = self._save

    def _main(self, slot, kwh, ev_kwh, rate=OFF):
        self.st._conn.execute(
            "INSERT INTO blocks (block_start, block_end, meter_id, config_period_id, "
            "imp_kwh, imp_rate, imp_cost, imp_kwh_ev, imp_cost_ev, imp_rate_ev, rate_source) "
            "VALUES (?,?,?,1,?,?,?,?,?,?,'measured')",
            (slot, slot, "electricity_main", kwh, rate, round(kwh * rate, 6),
             ev_kwh, round(ev_kwh * rate, 6), rate))

    def _segs(self, slot, ev_kwh, house_kwh, ev_band="off_peak", house_band="off_peak",
              ev_rate=OFF, house_rate=OFF):
        segs = []
        if ev_kwh > 0:
            segs.append((ev_kwh, ev_rate, round(ev_rate / 1.05, 6), ev_band, "ev"))
        if house_kwh > 0:
            segs.append((house_kwh, house_rate, round(house_rate / 1.05, 6), house_band, "house"))
        self.st.set_block_segments(slot, "electricity_main", segs)

    def _dispatch(self, slot, energy):
        self.st._conn.execute(
            "INSERT INTO dispatch_history (slot_start, kind, energy_kwh, first_seen, last_seen) "
            "VALUES (?,?,?,?,?)",
            (slot, "completed", -abs(energy), slot, slot))

    def _state(self, slot):
        r = self.st._conn.execute(
            "SELECT imp_kwh, imp_kwh_ev, imp_cost FROM blocks WHERE block_start=? "
            "AND meter_id='electricity_main'", (slot,)).fetchone()
        segs = self.st.get_block_segments(slot, "electricity_main")
        return r, segs

    def test_caps_ev_to_dispatch_and_is_cost_neutral_and_idempotent(self):
        slot = "2026-09-04T02:30:00"
        self._main(slot, 4.347, 3.182)               # imp_kwh_ev inflated by concurrent battery
        self._segs(slot, 3.182, 1.165)               # ev 3.182 + house 1.165 (both off_peak)
        self._dispatch(slot, 1.37)                   # car's actual session = 1.37
        self.st._conn.commit()
        cost_before = self._state(slot)[0]["imp_cost"]

        res = asyncio.run(engine.run_smb_ev_resplit())
        self.assertTrue(res["ok"]); self.assertEqual(res["resplit"], 1)
        self.assertEqual(res["skipped_mixed"], 0); self.assertEqual(res["zeroed_no_dispatch"], 0)

        r, segs = self._state(slot)
        self.assertAlmostEqual(r["imp_kwh_ev"], 1.37, places=5)          # capped to dispatch
        self.assertAlmostEqual(r["imp_cost"], cost_before, places=6)      # TOTAL cost unchanged
        ev = sum(s["kwh"] for s in segs if s["attribution"] == "ev")
        house = sum(s["kwh"] for s in segs if s["attribution"] == "house")
        self.assertAlmostEqual(ev, 1.37, places=5)
        self.assertAlmostEqual(house, 4.347 - 1.37, places=5)            # excess moved to house
        self.assertAlmostEqual(ev + house, 4.347, places=5)              # sum(segments) == grid
        # idempotent
        self.assertEqual(asyncio.run(engine.run_smb_ev_resplit(force=True))["resplit"], 0)

    def test_no_dispatch_zeroes_ev_to_house(self):
        slot = "2026-09-04T03:00:00"
        self._main(slot, 3.0, 2.0)                    # bill bucket said EV but NO dispatch
        self._segs(slot, 2.0, 1.0)
        self.st._conn.commit()                        # (no dispatch row)
        cost_before = self._state(slot)[0]["imp_cost"]
        res = asyncio.run(engine.run_smb_ev_resplit())
        self.assertEqual(res["resplit"], 1); self.assertEqual(res["zeroed_no_dispatch"], 1)
        r, segs = self._state(slot)
        self.assertIsNone(r["imp_kwh_ev"])                               # EV zeroed -> house
        self.assertAlmostEqual(r["imp_cost"], cost_before, places=6)
        self.assertAlmostEqual(sum(s["kwh"] for s in segs), 3.0, places=5)
        self.assertEqual([s["attribution"] for s in segs], ["house"])

    def test_clean_slot_within_dispatch_is_noop(self):
        slot = "2026-09-05T02:30:00"
        self._main(slot, 3.0, 1.5); self._segs(slot, 1.5, 1.5)
        self._dispatch(slot, 1.5)                     # EV == dispatch -> nothing to cap
        self.st._conn.commit()
        self.assertEqual(asyncio.run(engine.run_smb_ev_resplit(force=True))["resplit"], 0)

    def test_mixed_band_slot_is_skipped(self):
        slot = "2026-09-04T11:30:00"                  # a boundary/mixed slot
        self._main(slot, 4.0, 3.0)
        self._segs(slot, 3.0, 1.0, ev_band="peak", ev_rate=PK)   # EV on PEAK band -> not neutral
        self._dispatch(slot, 1.0)
        self.st._conn.commit()
        res = asyncio.run(engine.run_smb_ev_resplit(force=True))
        self.assertEqual(res["resplit"], 0); self.assertEqual(res["skipped_mixed"], 1)
        self.assertAlmostEqual(self._state(slot)[0]["imp_kwh_ev"], 3.0, places=5)


if __name__ == "__main__":
    unittest.main()
