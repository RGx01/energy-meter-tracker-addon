"""C2 (revised 4.5.9) — the settled EV/house split takes its RATE/band from the BILL's four
buckets, but its QUANTITY is bounded by the car's completed-dispatch session (measured is king).
Octopus's EV_DEVICE bucket over-attributes on slots where a home battery grid-charged inside the
dispatch window (no battery bucket), so the bill's ev_kwh is capped to the dispatch. A completed
dispatch that lands AFTER a block is priced leaves imp_kwh_ev NULL; the bill split still recovers
the EV, but never beyond the dispatched session. No dispatch -> house-only."""
import os, sys, tempfile, unittest
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from block_store import BlockStore
from kraken_rates import RateSchedule
import engine

OFF, PK = 5.49297, 32.30924  # pence


def _sched():
    from datetime import datetime as DT, timedelta as TD
    periods = []; d, end = DT(2026, 8, 1), DT(2026, 9, 30)
    while d < end:
        ds = d.strftime("%Y-%m-%d"); nx = (d + TD(days=1)).strftime("%Y-%m-%d")
        periods += [(f"{ds}T00:00:00", f"{ds}T05:30:00", OFF),
                    (f"{ds}T05:30:00", f"{ds}T23:30:00", PK),
                    (f"{ds}T23:30:00", f"{nx}T00:00:00", OFF)]
        d += TD(days=1)
    return RateSchedule(periods)


class TestC2SettledSplit(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False); self.tmp.close()
        self.store = BlockStore(self.tmp.name)
        with self.store._conn:
            self.store._conn.execute(
                "INSERT INTO config_periods (id, effective_from, block_minutes, timezone) "
                "VALUES (1,'2026-08-01T00:00:00',30,'Europe/London')")
            # block: 5 kWh, EV split NULL (late dispatch), not yet settled
            self.store._conn.execute(
                "INSERT INTO blocks (block_start, block_end, meter_id, config_period_id, "
                "imp_kwh, imp_kwh_api, imp_rate, imp_cost, imp_kwh_ev, is_provisional, interpolated) "
                "VALUES ('2026-09-08T02:00:00','2026-09-08T02:30:00','electricity_main',1,"
                "5.0,5.0,?,?,NULL,0,0)", (round(OFF/100,6), round(5.0*OFF/100,6)))   # DCC-settled
        self._prev = (engine._store, engine._kraken_rate_schedules)
        engine._store = self.store
        engine._kraken_rate_schedules = {"import": _sched()}

    def tearDown(self):
        engine._store, engine._kraken_rate_schedules = self._prev
        try: os.unlink(self.tmp.name)
        except OSError: pass

    def _apply(self):
        cost = round(5.0 * OFF / 100, 6)
        return engine.apply_measured_to_block("2026-09-08T02:00:00", cost_incl=cost,
                                              label="OFF_PEAK")

    def _ev(self):
        return self.store._conn.execute(
            "SELECT imp_kwh_ev FROM blocks WHERE block_start='2026-09-08T02:00:00'").fetchone()[0]

    def test_split_from_bill_when_breakdown_cached(self):
        # bill bucket (3.0) within the car's dispatch session (3.0) -> EV = 3.0 from the bill.
        self.store.record_dispatch_history("2026-09-08T02:00:00", "completed",
                                           source="unknown", energy_kwh=-3.0)
        self.store.upsert_measured_breakdown(
            "2026-09-08T02:00:00", mpan="m", home_kwh=2.0, home_rate=round(OFF/100,6),
            ev_kwh=3.0, ev_rate=round(OFF/100,6))
        self._apply()
        self.assertAlmostEqual(self._ev(), 3.0, places=6)   # EV split from the BILL

    def test_bill_bucket_capped_to_dispatch(self):
        # 4.5.9: a battery grid-charged inside the dispatch -> Octopus's EV_DEVICE bucket over-
        # attributes (3.0), but the car's completed dispatch was only 1.37. EV caps to 1.37.
        self.store.record_dispatch_history("2026-09-08T02:00:00", "completed",
                                           source="unknown", energy_kwh=-1.37)
        self.store.upsert_measured_breakdown(
            "2026-09-08T02:00:00", mpan="m", home_kwh=2.0, home_rate=round(OFF/100,6),
            ev_kwh=3.0, ev_rate=round(OFF/100,6))
        self._apply()
        self.assertAlmostEqual(self._ev(), 1.37, places=6)  # capped to the measured session

    def test_house_only_when_no_breakdown_and_no_dispatch(self):
        self._apply()                                        # no breakdown cached
        self.assertIsNone(self._ev())                        # house-only (unchanged behaviour)


if __name__ == "__main__":
    unittest.main()
