"""
4.5.22 (#505) — pick up Octopus's restated costs after the 1 Oct 2026 VAT change.

From 1 Oct 2026 Octopus's half-hourly measurement costs still carried 5% VAT (costInclTax =
costExclTax x 1.05) although its tariff rates and its statement are 0%, and a settled block took
that cost. EMT never re-asks for a cached slot nor re-applies a measured block, so Octopus's
restatement could never reach it. run_measured_vat_resync detects it per install: a live fetch of
new post-change slots that agree with the VAT calendar is a HINT; the whole period since the change
is then re-fetched and healed only if EVERY slot agrees; otherwise nothing is written and it waits.

New code: these exercise it. The local-midnight cases check both sides of the change.
"""
import asyncio
import json
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import engine                                    # noqa: E402
from block_store import BlockStore              # noqa: E402

MPAN = "1234567890123"
OFF_EXC = 0.052314                               # £/kWh, Oct 2026 off-peak (0% VAT: inc == exc)
KEY = "2026-09-30T23:00:00"                      # 1 Oct 2026 00:00 UK-local, in naive UTC
OCT = ["2026-10-05T01:00:00", "2026-10-05T01:30:00", "2026-10-06T02:00:00",
       "2026-09-30T23:00:00"]                    # the last: 00:00 local on 1 Oct — after the change
SEPT = "2026-09-30T22:30:00"                     # 23:30 local on 30 Sep — 5%, correctly
KWH = 3.0


class _Sched:
    def is_empty(self):
        return False

    def day_rate_bounds(self, ts):
        return (5.2314, 30.7707)                 # pence, 0% VAT


class _Client:
    """recover_measurement_costs stub: `costs` maps slot -> (cost_incl, cost_excl)."""
    def __init__(self, costs):
        self.costs, self.calls = costs, 0

    async def recover_measurement_costs(self, mpan, slots):
        self.calls += 1
        return {s: {"cost_incl": self.costs[s][0], "cost_excl": self.costs[s][1],
                    "off_peak": True, "kwh": KWH} for s in slots if s in self.costs}


def _five(slot):                                 # Octopus's stale pair
    exc = round(KWH * OFF_EXC, 6)
    return round(exc * 1.05, 6), exc


def _zero(slot):                                 # Octopus's restated pair
    exc = round(KWH * OFF_EXC, 6)
    return exc, exc


class _Base(unittest.TestCase):

    def setUp(self):
        self._save = (engine._store, engine._kraken_rate_schedules, engine._kraken_client,
                      engine._kraken_discovery)
        self.st = BlockStore(":memory:")
        engine._store = self.st
        engine._kraken_rate_schedules = {"import": _Sched()}
        engine._kraken_discovery = {"import": {"mpan": MPAN,
                                               "tariff_code": "E-1R-INTELLI-VAR-22-10-14-B"}}
        self.st._conn.execute(
            "INSERT OR IGNORE INTO config_periods (id, effective_from, billing_day, "
            "block_minutes, timezone) VALUES (1,'2020-01-01T00:00:00',1,30,'UTC')")
        for slot in OCT + [SEPT]:
            inc, exc = _five(slot)
            self.st._conn.execute(
                "INSERT INTO blocks (block_start, block_end, meter_id, config_period_id, imp_kwh, "
                "imp_kwh_api, imp_rate, imp_cost, imp_cost_exc, rate_source, rate_corrected) "
                "VALUES (?,?,'electricity_main',1,?,?,?,?,?,'measured',0)",
                (slot, slot, KWH, KWH, OFF_EXC, inc, exc))
            self.st.upsert_measured_cost(slot, mpan=MPAN, cost_incl=inc, cost_excl=exc,
                                         label="OFF_PEAK", kwh=KWH)
        self.st._conn.commit()

    def tearDown(self):
        (engine._store, engine._kraken_rate_schedules, engine._kraken_client,
         engine._kraken_discovery) = self._save

    def _hint(self):
        engine._vat_resync_note_fetch(self.st, [(s, *_zero(s)) for s in OCT[:3]])

    def _run(self, client):
        engine._kraken_client = client
        return asyncio.run(engine.run_measured_vat_resync())

    def _cost(self, slot):
        return self.st._conn.execute("SELECT imp_cost FROM blocks WHERE block_start=?",
                                     (slot,)).fetchone()[0]

    def _state(self):
        return json.loads(self.st.get_kraken_state(engine._VAT_RESYNC_STATE_KEY) or "{}")


class TheCalendarCheck(_Base):

    def test_the_change_is_1_oct_local_midnight(self):
        self.assertEqual(engine._vat_resync_change(self.st), KEY)

    def test_both_sides_of_local_midnight(self):
        self.assertTrue(engine._vat_pair_agrees(self.st, SEPT, *_five(SEPT)))         # 5% on 30 Sep
        self.assertFalse(engine._vat_pair_agrees(self.st, OCT[3], *_five(OCT[3])))    # 5% on 1 Oct
        self.assertTrue(engine._vat_pair_agrees(self.st, OCT[3], *_zero(OCT[3])))

    def test_a_tiny_pair_says_nothing(self):
        self.assertIsNone(engine._vat_pair_agrees(self.st, OCT[0], 0.0006, 0.0005))


class TheHint(_Base):

    def test_agreeing_new_slots_set_it(self):
        self._hint()
        self.assertTrue(self._state().get("hint"))

    def test_one_disagreeing_slot_withholds_it(self):
        engine._vat_resync_note_fetch(self.st, [(s, *_zero(s)) for s in OCT[:3]]
                                      + [(OCT[3], *_five(OCT[3]))])
        self.assertFalse(self._state().get("hint"))

    def test_pre_change_slots_do_not_count(self):
        engine._vat_resync_note_fetch(self.st, [(SEPT, *_five(SEPT))] * 5)
        self.assertFalse(self._state().get("hint"))

    def test_without_a_hint_it_waits_and_fetches_nothing(self):
        client = _Client({s: _zero(s) for s in OCT})
        res = self._run(client)
        self.assertEqual(client.calls, 0)
        self.assertIn("waiting", res)


class APartRestatement(_Base):
    """Octopus part-way through: one slot still at 5%. Nothing may change."""

    def test_nothing_is_written(self):
        self._hint()
        costs = {s: _zero(s) for s in OCT}
        costs[OCT[3]] = _five(OCT[3])
        res = self._run(_Client(costs))
        self.assertEqual(res["stuck"]["disagree"], 1)
        for s in OCT:
            self.assertAlmostEqual(self._cost(s), _five(s)[0], places=6)               # still 5%
        cached = self.st._conn.execute("SELECT cost_incl FROM measured_cost WHERE slot_start=?",
                                       (OCT[0],)).fetchone()[0]
        self.assertAlmostEqual(cached, _five(OCT[0])[0], places=6)                     # cache too

    def test_a_missing_slot_also_holds_it(self):
        self._hint()
        res = self._run(_Client({s: _zero(s) for s in OCT[:3]}))                       # one absent
        self.assertEqual(res["stuck"]["unverified"], 1)
        self.assertAlmostEqual(self._cost(OCT[0]), _five(OCT[0])[0], places=6)

    def test_it_waits_a_day_before_asking_again(self):
        self._hint()
        costs = {s: _zero(s) for s in OCT}
        costs[OCT[3]] = _five(OCT[3])
        self._run(_Client(costs))
        again = _Client({s: _zero(s) for s in OCT})
        self._run(again)
        self.assertEqual(again.calls, 0)


class TheWholePeriodRestated(_Base):

    def test_every_october_block_is_healed(self):
        self._hint()
        res = self._run(_Client({s: _zero(s) for s in OCT}))
        self.assertEqual(res["reapplied"], len(OCT))
        for s in OCT:
            self.assertAlmostEqual(self._cost(s), _zero(s)[0], places=6)
        self.assertTrue(self._state().get("done"))

    def test_september_is_not_touched(self):
        self._hint()
        self._run(_Client({s: _zero(s) for s in OCT}))
        self.assertAlmostEqual(self._cost(SEPT), _five(SEPT)[0], places=6)

    def test_a_corrected_block_is_not_touched(self):
        with self.st._conn:
            self.st._conn.execute("UPDATE blocks SET rate_corrected=1 WHERE block_start=?", (OCT[0],))
        self._hint()
        self._run(_Client({s: _zero(s) for s in OCT}))
        self.assertAlmostEqual(self._cost(OCT[0]), _five(OCT[0])[0], places=6)

    def test_once_done_it_stops(self):
        self._hint()
        self._run(_Client({s: _zero(s) for s in OCT}))
        again = _Client({s: _zero(s) for s in OCT})
        self.assertEqual(self._run(again).get("skipped"), "already done")
        self.assertEqual(again.calls, 0)

    def test_an_account_never_broken_is_marked_done_without_a_fetch(self):
        for s in OCT:                                                    # everything already 0%
            inc, exc = _zero(s)
            with self.st._conn:
                self.st._conn.execute("UPDATE blocks SET imp_cost=? WHERE block_start=?", (inc, s))
            self.st.upsert_measured_cost(s, mpan=MPAN, cost_incl=inc, cost_excl=exc,
                                         label="OFF_PEAK", kwh=KWH)
        client = _Client({})
        self.assertEqual(self._run(client).get("healed"), 0)
        self.assertEqual(client.calls, 0)
        self.assertTrue(self._state().get("done"))


class TheLiveFetchGivesTheHint(_Base):
    """measure_settled_dispatched_blocks feeds what it stores to the hint."""

    def test_restated_new_slots_set_the_hint(self):
        new = ["2026-10-08T01:00:00", "2026-10-08T01:30:00", "2026-10-08T02:00:00"]
        for slot in new:
            self.st._conn.execute(
                "INSERT INTO blocks (block_start, block_end, meter_id, config_period_id, imp_kwh, "
                "imp_kwh_api, imp_rate, imp_cost, rate_source, rate_corrected) "
                "VALUES (?,?,'electricity_main',1,?,?,?,?,'schedule',0)",
                (slot, slot, KWH, KWH, OFF_EXC, round(KWH * OFF_EXC, 6)))
            self.st.upsert_dispatch_slot(slot, off_peak=True, provider="Myenergi",
                                         source="smart-charge", state="completed")
        self.st._conn.commit()
        engine._kraken_client = _Client({s: _zero(s) for s in new})
        asyncio.run(engine.measure_settled_dispatched_blocks())
        self.assertTrue(self._state().get("hint"))


class TheHistoryDrainGivesTheHint(_Base):
    """On IOG-SMB the history drain settles every half-hour from Octopus's four buckets,
    dispatched or not, so what it stores is a hint source too."""

    def setUp(self):
        super().setUp()
        self._save2 = (engine._kraken_current_agreement_from, engine._MEASURED_HISTORY_DRAIN_PACE)
        engine._kraken_discovery = {"import": {"mpan": MPAN, "tariff_code": "E-1R-IOG-SMB-FIX"}}
        engine._kraken_current_agreement_from = "2026-07-05T00:00:00"
        engine._MEASURED_HISTORY_DRAIN_PACE = 0

    def tearDown(self):
        engine._kraken_current_agreement_from, engine._MEASURED_HISTORY_DRAIN_PACE = self._save2
        super().tearDown()

    def test_restated_undispatched_slots_set_the_hint(self):
        new = ["2026-10-07T10:00:00", "2026-10-07T10:30:00", "2026-10-07T11:00:00"]
        for slot in new:                                   # settled, no dispatch row at all
            self.st._conn.execute(
                "INSERT INTO blocks (block_start, block_end, meter_id, config_period_id, imp_kwh, "
                "imp_kwh_api, imp_rate, imp_cost, rate_source, rate_corrected) "
                "VALUES (?,?,'electricity_main',1,?,?,?,?,'schedule',0)",
                (slot, slot, KWH, KWH, OFF_EXC, round(KWH * OFF_EXC, 6)))
        self.st._conn.commit()

        class _Buckets:
            async def recover_device_breakdown(self, mpan, starts, **kw):
                return {s: {"home_kwh": KWH, "ev_kwh": 0.0,
                            "home_cost": round(KWH * OFF_EXC, 6), "ev_cost": 0.0,
                            "home_rate": OFF_EXC, "ev_rate": None,
                            "home_rate_exc": OFF_EXC, "ev_rate_exc": None,
                            "home_band": "off_peak", "ev_band": None} for s in starts if s in new}
        engine._kraken_client = _Buckets()
        asyncio.run(engine.run_measured_history_drain())
        self.assertTrue(self._state().get("hint"))


if __name__ == "__main__":
    unittest.main()
