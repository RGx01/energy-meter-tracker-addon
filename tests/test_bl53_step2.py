"""BL-53 step 2 — the settlement-time measured-cost fetch. Compute-and-log mode:
caches Octopus's billed cost per settled dispatched slot and LOGS the reprice it
would make, without writing any rate. Only IOG dispatched settled slots; skips
already-cached slots."""
import asyncio
import os
import tempfile
import unittest

import engine
from block_store import BlockStore
from kraken_rates import RateSchedule



def _run(coro):
    """Fresh event loop per call — robust when a prior test module closed the
    shared default loop (test-isolation, not shared-loop reuse)."""
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()

class _MockClient:
    def __init__(self, nodes):
        self.nodes = nodes
        self.fetched = []

    async def recover_measurement_costs(self, mpan, starts, **kw):
        self.fetched.append(list(starts))
        return {s: self.nodes[s] for s in starts if s in self.nodes}


class TestMeasureSettled(unittest.TestCase):
    def setUp(self):
        self._save = (engine._store, engine._kraken_client, engine._kraken_discovery,
                      engine._MEASURED_APPLY, engine._kraken_rate_schedules)
        self.p = tempfile.mktemp(suffix=".db")
        st = BlockStore(self.p); st._conn.execute("PRAGMA foreign_keys=OFF")
        c = st._conn
        # two SETTLED dispatched blocks (imp_kwh_api set) + off_peak=1 slots, priced off-peak
        for bs, kwh in (("2026-08-23T10:00:00", 3.186), ("2026-08-23T10:30:00", 4.553)):
            c.execute("INSERT INTO blocks (block_start,block_end,config_period_id,meter_id,"
                      "imp_kwh,imp_rate,imp_cost,imp_kwh_api,rate_corrected,rate_source) "
                      "VALUES (?,?,?,?,?,?,?,?,?,?)",
                      (bs, bs[:-2]+"30", 1, "electricity_main", kwh, 0.05493,
                       round(kwh*0.05493,6), kwh, 0, "schedule"))
            st.upsert_dispatch_slot(bs, off_peak=True, provider="Myenergi",
                                    source="smart-charge-completed", state="completed")
        c.commit()
        engine._store = st
        engine._kraken_discovery = {"import": {"mpan": "M", "tariff_code": "E-1R-IOG-SMB-FIX-12M-26-03-17-B"}}
        engine._MEASURED_APPLY = False
        engine._kraken_rate_schedules = {"import": RateSchedule([
            ("2026-08-22T22:30:00", "2026-08-23T04:30:00", 5.493),
            ("2026-08-23T04:30:00", "2026-08-23T22:30:00", 32.3092),
            ("2026-08-23T22:30:00", "2026-08-24T04:30:00", 5.493)])}
        # Octopus bills BOTH slots at STANDARD (peak) — cost = kwh × 0.323092
        self.nodes = {
            "2026-08-23T10:00:00": {"cost_incl": round(3.186*0.323092,6), "cost_excl": 0.98,
                                    "off_peak": False, "kwh": 3.186},
            "2026-08-23T10:30:00": {"cost_incl": round(4.553*0.323092,6), "cost_excl": 1.40,
                                    "off_peak": False, "kwh": 4.553},
        }

    def tearDown(self):
        (engine._store, engine._kraken_client, engine._kraken_discovery,
         engine._MEASURED_APPLY, engine._kraken_rate_schedules) = self._save
        try: os.remove(self.p)
        except OSError: pass

    def _run(self):
        return _run(
            engine.measure_settled_dispatched_blocks())

    def test_caches_cost_and_logs_without_writing(self):
        engine._kraken_client = _MockClient(self.nodes)
        res = self._run()
        self.assertEqual(res["stored"], 2)
        self.assertEqual(res["would_reprice"], 2)      # off-peak 0.05493 → peak 0.32309
        # measured_cost cached
        got = engine._store.get_measured_cost("2026-08-23T10:00:00", mpan="M")
        self.assertEqual(got["label"], "STANDARD_RATE")
        self.assertAlmostEqual(got["cost_incl"], round(3.186*0.323092,6), places=6)
        # COMPUTE-AND-LOG: block rate/source UNCHANGED
        r = engine._store._conn.execute(
            "SELECT imp_rate, rate_source FROM blocks WHERE block_start='2026-08-23T10:00:00'").fetchone()
        self.assertAlmostEqual(r["imp_rate"], 0.05493, places=6)   # not repriced
        self.assertEqual(r["rate_source"], "schedule")             # not 'measured'

    def test_same_band_rounding_not_flagged(self):
        # a currently off-peak slot Octopus also bills OFF_PEAK, tiny kWh → cost/kWh
        # differs at the 6th decimal but the BAND agrees → must NOT flag.
        c = engine._store._conn
        c.execute("INSERT INTO blocks (block_start,block_end,config_period_id,meter_id,"
                  "imp_kwh,imp_rate,imp_cost,imp_kwh_api,rate_corrected,rate_source) "
                  "VALUES ('2026-08-23T02:00:00','2026-08-23T02:30:00',1,'electricity_main',"
                  "0.103,0.05493,0.005658,0.103,0,'schedule')")
        engine._store.upsert_dispatch_slot("2026-08-23T02:00:00", off_peak=True,
                                           provider="Myenergi", source="smart-charge-completed",
                                           state="completed")
        c.commit()
        self.nodes["2026-08-23T02:00:00"] = {"cost_incl": 0.0057, "cost_excl": 0.0054,
                                             "off_peak": True, "kwh": 0.103}
        engine._kraken_client = _MockClient(self.nodes)
        res = self._run()
        self.assertEqual(res["would_reprice"], 2)   # only the two band flips
        self.assertEqual(res["stored"], 3)

    def test_skips_already_cached(self):
        engine._store.upsert_measured_cost("2026-08-23T10:00:00", mpan="M",
                                           cost_incl=1.0, label="STANDARD_RATE")
        mc = _MockClient(self.nodes); engine._kraken_client = mc
        res = self._run()
        self.assertEqual(res["fetched"], 1)                        # only the uncached slot
        self.assertEqual(mc.fetched[0], ["2026-08-23T10:30:00"])

    def test_non_iog_noop(self):
        engine._kraken_discovery = {"import": {"mpan": "M", "tariff_code": "E-1R-AGILE-FLEX-22-11-25-B"}}
        engine._kraken_client = _MockClient(self.nodes)
        self.assertEqual(self._run(), {})

    def test_gate_measured_block_not_refetched(self):
        engine._store._conn.execute(
            "UPDATE blocks SET rate_source='measured' WHERE block_start='2026-08-23T10:00:00'")
        engine._store._conn.commit()
        mc = _MockClient(self.nodes); engine._kraken_client = mc
        res = self._run()
        self.assertEqual(res["fetched"], 1)                        # measured block excluded
        self.assertEqual(mc.fetched[0], ["2026-08-23T10:30:00"])


if __name__ == "__main__":
    unittest.main()
