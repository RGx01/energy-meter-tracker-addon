"""4.5.9 recurring predicted-split carve: an unsettled dispatched block whose completed
dispatch landed AFTER finalise is left house-only (imp_kwh_ev NULL, single 'house' segment)
until settlement. run_iog_split_carve fills the PREDICTED EV/Home split from the dispatch
(grid-clipped), ADDITIVELY (imp_cost/imp_rate untouched; segments re-split at the block's own
rate). Only unsettled blocks; settled/corrected are settlement's realm."""
import os, sys, asyncio, unittest
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import engine
from block_store import BlockStore

OFF = 0.05493


class _Sched:
    def is_empty(self): return False
    def day_rate_bounds(self, ts): return (5.493, 32.3092)   # pence
    def is_off_peak(self, ts): return True
    def resolve(self, ts): return 5.493


class _EvOff:
    def resolve(self, ts): return 5.493


class _EvPeak:
    def resolve(self, ts): return 32.3092


class TestIogSplitCarve(unittest.TestCase):
    def setUp(self):
        self._save = (engine._store, engine._kraken_rate_schedules)
        self.st = BlockStore(":memory:"); engine._store = self.st
        engine._kraken_rate_schedules = {"import": _Sched(),
                                         "ev_device_off_peak": _EvOff(),
                                         "ev_device_peak": _EvPeak()}
        self.st._conn.execute(
            "INSERT OR IGNORE INTO config_periods (id, effective_from, billing_day, "
            "block_minutes, timezone) VALUES (1,'2020-01-01T00:00:00',1,30,'UTC')")

    def tearDown(self):
        (engine._store, engine._kraken_rate_schedules) = self._save

    def _blk(self, slot, kwh, rate=OFF, rate_source="schedule"):
        self.st._conn.execute(
            "INSERT INTO blocks (block_start, block_end, meter_id, config_period_id, "
            "imp_kwh, imp_rate, imp_cost, imp_kwh_ev, rate_source) "
            "VALUES (?,?,?,1,?,?,?,NULL,?)",
            (slot, slot, "electricity_main", kwh, rate, round(kwh * rate, 6), rate_source))
        # single house segment (what the finalise seam left when dispatch was absent)
        self.st.set_block_segments(slot, "electricity_main",
                                   [(kwh, rate, round(rate / 1.05, 6), "off_peak", "house")])

    def _dispatch(self, slot, energy):
        self.st._conn.execute(
            "INSERT INTO dispatch_history (slot_start, kind, energy_kwh, first_seen, last_seen) "
            "VALUES (?,?,?,?,?)", (slot, "completed", -abs(energy), slot, slot))

    def _state(self, slot):
        r = self.st._conn.execute(
            "SELECT imp_kwh, imp_kwh_ev, imp_cost FROM blocks WHERE block_start=? "
            "AND meter_id='electricity_main'", (slot,)).fetchone()
        return r, self.st.get_block_segments(slot, "electricity_main")

    def test_carves_unsettled_dispatched_block_additively(self):
        slot = "2026-09-10T02:00:00"
        self._blk(slot, 3.45)                    # unsettled, house-only, imp_kwh_ev NULL
        self._dispatch(slot, 3.39)               # completed dispatch arrived after finalise
        self.st._conn.commit()
        cost0 = self._state(slot)[0]["imp_cost"]

        res = asyncio.run(engine.run_iog_split_carve())
        self.assertTrue(res["ok"]); self.assertEqual(res["carved"], 1)

        r, segs = self._state(slot)
        self.assertAlmostEqual(r["imp_kwh_ev"], 3.39, places=5)         # EV = dispatch (grid-clipped)
        self.assertAlmostEqual(r["imp_cost"], cost0, places=6)          # TOTAL cost untouched
        attrs = sorted(s["attribution"] for s in segs)
        self.assertEqual(attrs, ["ev", "house"])                        # split, not house-only
        self.assertAlmostEqual(sum(s["kwh"] for s in segs), 3.45, places=5)   # segments == grid
        ev = next(s for s in segs if s["attribution"] == "ev")
        self.assertAlmostEqual(ev["kwh"], 3.39, places=5)
        # idempotent
        self.assertEqual(asyncio.run(engine.run_iog_split_carve())["carved"], 0)

    def test_grid_clips_ev_to_slot_import(self):
        slot = "2026-09-10T03:00:00"
        self._blk(slot, 1.20)                    # only 1.2 kWh of grid this slot
        self._dispatch(slot, 3.0)                # dispatch claims 3.0 -> clip to grid
        self.st._conn.commit()
        asyncio.run(engine.run_iog_split_carve())
        self.assertAlmostEqual(self._state(slot)[0]["imp_kwh_ev"], 1.20, places=5)

    def test_settled_block_is_not_touched(self):
        slot = "2026-09-08T02:00:00"
        self._blk(slot, 3.0, rate_source="measured")   # SETTLED — settlement's realm
        self._dispatch(slot, 2.0)
        self.st._conn.commit()
        res = asyncio.run(engine.run_iog_split_carve())
        self.assertEqual(res["carved"], 0)
        self.assertIsNone(self._state(slot)[0]["imp_kwh_ev"])

    def test_no_dispatch_stays_house(self):
        slot = "2026-09-10T12:00:00"
        self._blk(slot, 2.0)                     # no dispatch this slot
        self.st._conn.commit()
        res = asyncio.run(engine.run_iog_split_carve())
        self.assertEqual(res["carved"], 0)
        r, segs = self._state(slot)
        self.assertIsNone(r["imp_kwh_ev"])
        self.assertEqual([s["attribution"] for s in segs], ["house"])


if __name__ == "__main__":
    unittest.main()
