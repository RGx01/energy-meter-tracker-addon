"""4.5.11: first-time / bulk SMB history import — run_measured_history_drain fetches Octopus's
billed four-bucket breakdown for imported capped-tariff slots (oldest-first) and settles them,
so filled history reads the real cost/EV-House split/band instead of schedule-provisional. Reuses
recover_device_breakdown + apply_measured_settled; agreement-floored; skips not-yet-billed slots."""
import os, sys, asyncio, unittest
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import engine
from block_store import BlockStore

OFF = 0.05493


class _Sched:
    def is_empty(self): return False
    def day_rate_bounds(self, ts): return (5.493, 32.3092)   # pence (off, peak)


class _FakeClient:
    """Returns Octopus's device breakdown for requested slots (all off-peak here)."""
    def __init__(self, kwh_by_slot):
        self.kwh = kwh_by_slot
        self.calls = []

    async def recover_device_breakdown(self, mpan, starts, **kw):
        self.calls.append(list(starts))
        out = {}
        for s in starts:
            k = self.kwh.get(s)
            if k is None:
                continue                                  # not billed → absent
            home, ev = k
            out[s] = {"home_kwh": home, "ev_kwh": ev,
                      "home_cost": round(home * OFF, 6), "ev_cost": round(ev * OFF, 6),
                      "home_rate": OFF, "ev_rate": OFF,
                      "home_rate_exc": round(OFF / 1.05, 6), "ev_rate_exc": round(OFF / 1.05, 6),
                      "home_band": "off_peak", "ev_band": "off_peak"}
        return out


class TestMeasuredHistoryDrain(unittest.TestCase):
    def setUp(self):
        self._save = (engine._store, engine._kraken_client, engine._kraken_discovery,
                      engine._kraken_rate_schedules, engine._kraken_current_agreement_from,
                      engine._MEASURED_APPLY, engine._MEASURED_FETCH_ENABLED,
                      engine._MEASURED_HISTORY_DRAIN_PACE)
        self.st = BlockStore(":memory:"); engine._store = self.st
        engine._kraken_discovery = {"import": {"mpan": "m", "tariff_code": "E-1R-IOG-SMB-FIX"}}
        engine._kraken_rate_schedules = {"import": _Sched()}
        engine._kraken_current_agreement_from = "2026-07-05T00:00:00"
        engine._MEASURED_APPLY = True; engine._MEASURED_FETCH_ENABLED = True
        engine._MEASURED_HISTORY_DRAIN_PACE = 0
        self.st._conn.execute(
            "INSERT OR IGNORE INTO config_periods (id, effective_from, billing_day, "
            "block_minutes, timezone) VALUES (1,'2020-01-01T00:00:00',1,30,'UTC')")

    def tearDown(self):
        (engine._store, engine._kraken_client, engine._kraken_discovery,
         engine._kraken_rate_schedules, engine._kraken_current_agreement_from,
         engine._MEASURED_APPLY, engine._MEASURED_FETCH_ENABLED,
         engine._MEASURED_HISTORY_DRAIN_PACE) = self._save

    def _imported_block(self, slot, kwh, rate=0.323092):
        # an imported SMB slot: settled kWh (imp_kwh_api), schedule-priced provisional (peak here)
        self.st._conn.execute(
            "INSERT INTO blocks (block_start, block_end, meter_id, config_period_id, "
            "imp_kwh, imp_kwh_api, imp_rate, imp_cost, rate_source) "
            "VALUES (?,?,?,1,?,?,?,?,'schedule')",
            (slot, slot, "electricity_main", kwh, kwh, rate, round(kwh * rate, 6)))
        self.st._conn.commit()

    def _row(self, slot):
        return self.st._conn.execute(
            "SELECT imp_rate, imp_cost, rate_source FROM blocks WHERE block_start=? "
            "AND meter_id='electricity_main'", (slot,)).fetchone()

    def test_drain_settles_imported_history_from_the_bill(self):
        s1, s2 = "2026-08-01T02:00:00", "2026-08-01T02:30:00"
        self._imported_block(s1, 3.0)                     # provisional peak (schedule)
        self._imported_block(s2, 2.0)
        engine._kraken_client = _FakeClient({s1: (1.0, 2.0), s2: (1.5, 0.5)})

        res = asyncio.run(engine.run_measured_history_drain())
        self.assertTrue(res["ok"]); self.assertEqual(res["settled"], 2)

        for s, kwh in ((s1, 3.0), (s2, 2.0)):
            r = self._row(s)
            self.assertEqual(r["rate_source"], "measured")                # settled from the bill
            self.assertAlmostEqual(r["imp_rate"], OFF, places=5)          # snapped to off-peak
            self.assertAlmostEqual(r["imp_cost"], round(kwh * OFF, 6), places=6)
            mc = self.st.get_measured_breakdown(s)
            self.assertIsNotNone(mc)                                      # breakdown cached

    def test_recent_unbilled_slot_is_left_for_the_live_pass(self):
        from datetime import datetime, timezone
        recent = datetime.now(timezone.utc).replace(tzinfo=None).strftime("%Y-%m-%dT%H:00:00")
        self._imported_block(recent, 3.0)
        engine._kraken_client = _FakeClient({recent: (1.0, 2.0)})
        res = asyncio.run(engine.run_measured_history_drain())
        self.assertEqual(res["settled"], 0)                              # excluded by the recency cutoff
        self.assertEqual(self._row(recent)["rate_source"], "schedule")   # untouched

    def test_noop_when_not_capped(self):
        engine._kraken_discovery = {"import": {"mpan": "m", "tariff_code": "E-1R-FLAT"}}
        engine._kraken_rate_schedules = {"import": _Sched()}   # no ev_device schedule
        self._imported_block("2026-08-01T02:00:00", 3.0)
        engine._kraken_client = _FakeClient({"2026-08-01T02:00:00": (1.0, 2.0)})
        res = asyncio.run(engine.run_measured_history_drain())
        self.assertEqual(res.get("skipped"), "not applicable")


if __name__ == "__main__":
    unittest.main()
