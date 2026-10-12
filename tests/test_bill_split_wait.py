"""
4.5.23 — a bill that drops the car's half-hour is not settled yet.

On 2026-10-10 Octopus's device breakdown, fetched ~29 h after the night, put every dispatched
half-hour wholly on the house (EV 0 kWh) and billed 05:30-06:00 BST at the standard rate. Days
later it served the true split. EMT cached the provisional answer and applied it as the
authority, so EMT's own EV split for the night was replaced for good.

Now, where EMT's split has the car drawing and the bill says the EV drew nothing, the bill is held
back (not cached, not applied, EMT's split kept) and asked for again on later passes; after 28 days
EMT stops asking and keeps its own split. The one-off run_bill_split_refetch re-fetches the bills
cached that way before 4.5.23.

Discriminating (fail on the parent): test_a_bill_with_no_ev_is_held_back,
test_after_28_days_emt_keeps_its_split_and_stops_asking, test_the_drain_holds_it_back_too,
test_a_held_slot_is_not_drain_backlog, test_the_one_off_refetch_runs_under_its_own_marker,
test_the_one_off_restores_the_split. The rest are guards.
"""
import asyncio
import os
import sys
import unittest
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import engine                                    # noqa: E402
from block_store import BlockStore               # noqa: E402

OFF, PEAK = 0.05493, 0.323092
MPAN = "2000000000001"


def _run(coro):
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


def _slot(days_ago):
    t = datetime.now(timezone.utc).replace(tzinfo=None, minute=0, second=0, microsecond=0)
    return (t - timedelta(days=days_ago)).replace(hour=2).isoformat()


class _Sched:
    def is_empty(self): return False
    def day_rate_bounds(self, ts): return (5.493, 32.3092)


def _node(home, ev, rate=OFF, band="off_peak"):
    return {"home_kwh": home, "ev_kwh": ev, "home_rate": rate, "ev_rate": rate if ev else None,
            "home_rate_exc": rate, "ev_rate_exc": rate if ev else None,
            "home_cost": round(home * rate, 6), "ev_cost": round(ev * rate, 6),
            "home_band": band, "ev_band": band if ev else None}


class _Client:
    def __init__(self, by_slot=None, node=None):
        self.by_slot, self.node, self.asked = by_slot or {}, node, []

    async def recover_device_breakdown(self, mpan, slots, **kw):
        self.asked.extend(slots)
        return {s: (self.node if self.node is not None else self.by_slot.get(s))
                for s in slots if self.node is not None or s in self.by_slot}


class _Harness(unittest.TestCase):
    KWH, EV = 6.672, 3.13

    def setUp(self):
        self._save = (engine._store, engine._kraken_client, engine._kraken_discovery,
                      engine._kraken_rate_schedules, engine._kraken_current_agreement_from,
                      engine._MEASURED_FETCH_ENABLED, engine._MEASURED_APPLY)
        self.st = BlockStore(":memory:")
        engine._store = self.st
        engine._kraken_current_agreement_from = "2026-01-01T00:00:00"
        engine._MEASURED_FETCH_ENABLED = engine._MEASURED_APPLY = True
        engine._kraken_discovery = {"import": {"mpan": MPAN, "tariff_code": "IOG-SMB-FIX-12M"}}
        engine._kraken_rate_schedules = {"import": _Sched(), "ev_device_off_peak": _Sched()}
        self.st._conn.execute(
            "INSERT OR IGNORE INTO config_periods (id, effective_from, billing_day, "
            "block_minutes, timezone) VALUES (1,'2020-01-01T00:00:00',1,30,'UTC')")
        self.st._conn.commit()

    def tearDown(self):
        (engine._store, engine._kraken_client, engine._kraken_discovery,
         engine._kraken_rate_schedules, engine._kraken_current_agreement_from,
         engine._MEASURED_FETCH_ENABLED, engine._MEASURED_APPLY) = self._save

    def _block(self, slot, ev=EV, rate_source="schedule"):
        self.st._conn.execute(
            "INSERT INTO blocks (block_start, block_end, meter_id, config_period_id, imp_kwh, "
            "imp_kwh_api, imp_rate, imp_cost, imp_kwh_ev, imp_cost_ev, rate_source, "
            "rate_corrected) VALUES (?,?,?,1,?,?,?,?,?,?,?,0)",
            (slot, slot, "electricity_main", self.KWH, self.KWH, OFF, round(self.KWH * OFF, 6),
             ev, round(ev * OFF, 6) if ev is not None else None, rate_source))
        self.st.upsert_dispatch_slot(slot, off_peak=True, source="smart-charge-completed")
        self.st._conn.execute("UPDATE dispatch_slots SET energy_completed = -3.23 "
                              "WHERE slot_start = ?", (slot,))
        self.st._conn.commit()

    def _cached(self, slot):
        return self.st._conn.execute("SELECT * FROM measured_cost WHERE slot_start = ?",
                                     (slot,)).fetchone()

    def _main(self, slot):
        return self.st._conn.execute(
            "SELECT imp_kwh_ev, imp_cost, rate_source FROM blocks WHERE block_start = ? "
            "AND meter_id = 'electricity_main'", (slot,)).fetchone()


class TheLivePass(_Harness):

    def test_a_bill_with_no_ev_is_held_back(self):
        slot = _slot(1)
        self._block(slot)
        res = _run(self._pass(_Client(node=_node(self.KWH, 0.0))))
        self.assertEqual(res["held"], 1)
        self.assertIsNone(self._cached(slot), "the house-only bill was cached")
        self.assertAlmostEqual(self._main(slot)["imp_kwh_ev"], self.EV, places=6)
        self.assertIn(slot, engine._bill_split_wait(self.st))

    def test_it_is_asked_again_and_settles_once_octopus_has_the_car(self):
        """GUARD of the new path's second half."""
        slot = _slot(1)
        self._block(slot)
        _run(self._pass(_Client(node=_node(self.KWH, 0.0))))
        res = _run(self._pass(_Client(node=_node(self.KWH - 3.1304, 3.1304))))
        self.assertEqual(res["stored"], 1)
        self.assertAlmostEqual(self._cached(slot)["ev_kwh"], 3.1304, places=6)
        self.assertNotIn(slot, engine._bill_split_wait(self.st))

    def test_after_28_days_emt_keeps_its_split_and_stops_asking(self):
        slot = _slot(30)
        self._block(slot)
        engine._bill_split_wait_save(self.st, {slot: {"first": _slot(29)}})
        client = _Client(node=_node(self.KWH, 0.0))
        _run(self._pass(client))
        self.assertNotIn(slot, client.asked, "still asking after 28 days")
        self.assertIsNone(self._cached(slot))
        self.assertAlmostEqual(self._main(slot)["imp_kwh_ev"], self.EV, places=6)
        self.assertTrue(engine._bill_split_wait(self.st)[slot].get("kept"))

    def test_a_bill_with_no_ev_where_emt_had_none_is_cached_as_before(self):
        """GUARD: no disagreement, nothing held — the bill is the authority."""
        slot = _slot(1)
        self._block(slot, ev=None)
        res = _run(self._pass(_Client(node=_node(self.KWH, 0.0))))
        self.assertEqual((res["stored"], res.get("held", 0)), (1, 0))
        self.assertIsNotNone(self._cached(slot))

    def test_a_bill_with_the_car_is_cached_as_before(self):
        """GUARD: the ordinary case."""
        slot = _slot(1)
        self._block(slot)
        res = _run(self._pass(_Client(node=_node(3.54, 3.13))))
        self.assertEqual((res["stored"], res.get("held", 0)), (1, 0))

    def _pass(self, client):
        engine._kraken_client = client
        return engine.measure_settled_dispatched_blocks()


class TheDrain(_Harness):

    def setUp(self):
        super().setUp()
        self._pace = engine._MEASURED_HISTORY_DRAIN_PACE
        engine._MEASURED_HISTORY_DRAIN_PACE = 0

    def tearDown(self):
        engine._MEASURED_HISTORY_DRAIN_PACE = self._pace
        super().tearDown()

    def test_the_drain_holds_it_back_too(self):
        slot = _slot(2)
        self._block(slot)
        engine._kraken_client = _Client(node=_node(self.KWH, 0.0))
        _run(engine.run_measured_history_drain(max_batches=3))
        self.assertIsNone(self._cached(slot), "the drain cached the house-only bill")
        self.assertIn(slot, engine._bill_split_wait(self.st))

    def test_a_held_slot_is_not_drain_backlog(self):
        """Otherwise the drain is rescheduled every tick for a slot it cannot settle."""
        slot = _slot(2)
        self._block(slot)
        self.assertEqual(engine._measured_history_backlog(), 1)
        engine._bill_split_wait_save(self.st, {slot: {"first": _slot(1)}})
        self.assertEqual(engine._measured_history_backlog(), 0)


class TheOneOff(_Harness):
    """run_bill_split_refetch: #481's re-fetch, re-armed for bills cached before the hold-back."""

    def _cache_house_only(self, slot, rate=OFF, label="OFF_PEAK"):
        self.st.upsert_measured_cost(slot, mpan=MPAN, cost_incl=round(self.KWH * rate, 6),
                                     cost_excl=round(self.KWH * rate, 6), label=label,
                                     kwh=self.KWH)
        self.st.upsert_measured_breakdown(slot, mpan=MPAN, home_kwh=self.KWH, home_rate=rate,
                                          ev_kwh=0.0, ev_rate=None)

    def test_the_one_off_refetch_runs_under_its_own_marker(self):
        """Prod has #481's marker set: the 4.5.23 pass must still run, once."""
        slot = _slot(2)
        self._block(slot, ev=None, rate_source="measured")
        self._cache_house_only(slot)
        self.st.set_kraken_state(engine._EV_BUCKET_REFETCH_DONE_KEY, "2026-09-29T00:00:00")
        engine._kraken_client = _Client(node=_node(3.54, 3.13))
        res = _run(engine.run_bill_split_refetch())
        self.assertEqual(res.get("refetched"), 1, res)
        self.assertTrue(self.st.get_kraken_state(engine._BILL_SPLIT_REFETCH_DONE_KEY))
        self.assertEqual(_run(engine.run_bill_split_refetch()).get("skipped"), "already done")



class TheOneOffEndToEnd(unittest.TestCase):
    """The one-off and the recurring split heal together, on test_settled_ev_split's tariff:
    a night half-hour cached house-only (the provisional bill), then Octopus's true buckets."""
    SLOT = "2026-09-01T02:00:00"

    def setUp(self):
        from kraken_rates import RateSchedule
        self._save = (engine._store, engine._kraken_rate_schedules, engine._kraken_discovery,
                      engine._kraken_client)
        st = BlockStore(":memory:")
        st.insert_config_period({"meters": {"electricity_main": {"meta": {
            "timezone": "Europe/London", "billing_day": 1, "block_minutes": 30,
            "currency_symbol": "£", "currency_code": "GBP", "sub_meter": False}}}})
        engine._store = self.st = st
        engine._kraken_discovery = {"import": {"mpan": MPAN, "tariff_code": "E-1R-IOG-SMB-X-B",
                                               "product_code": "IOG-SMB-X"}}
        night = ("2026-08-31T22:30:00", "2026-09-01T04:30:00")
        day = ("2026-09-01T04:30:00", "2026-09-01T22:30:00")
        engine._kraken_rate_schedules = {
            "import": RateSchedule([night + (5.493,), day + (32.3092,)]),
            "ev_device_off_peak": RateSchedule([night + (5.493,)]),
            "ev_device_peak": RateSchedule([day + (32.3092,)])}
        st._conn.execute(
            "INSERT INTO blocks (block_start, block_end, meter_id, config_period_id, imp_kwh, "
            "imp_rate, imp_cost, imp_kwh_api, rate_source, rate_corrected) "
            "VALUES (?,?,?,1,6.672,?,?,6.672,'measured',0)",
            (self.SLOT, "2026-09-01T02:30:00", "electricity_main", OFF, round(6.672 * OFF, 6)))
        st.upsert_measured_cost(self.SLOT, mpan=MPAN, cost_incl=round(6.672 * OFF, 6),
                                cost_excl=round(6.672 * OFF, 6), label="OFF_PEAK", kwh=6.672)
        st.upsert_measured_breakdown(self.SLOT, mpan=MPAN, home_kwh=6.672, home_rate=OFF,
                                     ev_kwh=0.0, ev_rate=None)
        st.upsert_dispatch_slot(self.SLOT, off_peak=True, source="smart-charge-completed")
        st._conn.execute("UPDATE dispatch_slots SET energy_completed = -3.23 WHERE slot_start = ?",
                         (self.SLOT,))
        st.record_dispatch_history(self.SLOT, "completed", provider="Myenergi", source="unknown",
                                   energy_kwh=-3.23)
        st._conn.commit()

    def tearDown(self):
        (engine._store, engine._kraken_rate_schedules, engine._kraken_discovery,
         engine._kraken_client) = self._save

    def test_the_one_off_restores_the_split(self):
        """A night half-hour house-only in the cache; Octopus now has about half of it on the car."""
        engine._kraken_client = _Client(node=_node(3.5416, 3.1304))
        _run(engine.run_bill_split_refetch())
        self.assertAlmostEqual(self.st.get_measured_breakdown(self.SLOT)["ev_kwh"], 3.1304,
                               places=4)
        engine.run_settled_ev_split_heal()
        ev = self.st._conn.execute(
            "SELECT imp_kwh_ev FROM blocks WHERE block_start = ? AND meter_id = 'electricity_main'",
            (self.SLOT,)).fetchone()[0]
        self.assertAlmostEqual(ev, 3.1304, places=3)


class TheRule(unittest.TestCase):
    """GUARD (new code): what counts as the bill dropping the car."""

    def test_no_ev_on_the_bill_against_emts_split(self):
        self.assertTrue(engine._bill_drops_ev(3.13, 0.0))

    def test_any_ev_on_the_bill_is_a_split(self):
        self.assertFalse(engine._bill_drops_ev(3.13, 0.2))

    def test_no_split_of_emts_own_is_no_disagreement(self):
        self.assertFalse(engine._bill_drops_ev(None, 0.0))
        self.assertFalse(engine._bill_drops_ev(0.01, 0.0))


if __name__ == "__main__":
    unittest.main()
