"""BL-68: a bill that says "nothing was drawn" is an ANSWER, not a gap.

`recover_device_breakdown` returns a parsed node for a dispatched-but-idle half-hour —
every bucket present, every value zero — and counts it `recovered`. `measure_settled`
used to throw that away as `absent`, writing nothing, so `measured_slots_missing` handed
the slot back on the very next pass. Observed live on prod-dev: from 01:40 onward, every
hour, `candidates=11 fetched=11 stored=0 absent=11` — eleven idle slots re-fetched
forever, six GraphQL calls a pass, re-proving a fact already known.

Caching the zero ends it: `missing` goes empty and the pass returns before any fetch.

The safety property that makes it sound: believe a zero ONLY when the block's own settled
kWh agrees. A bill that has not run yet also reports zero, and caching THAT would be a
permanent lie about a half-hour that really did draw energy.
"""
import os, sys, asyncio, unittest
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import engine
from block_store import BlockStore

OFF = 0.05493
ZERO_NODE = {"home_kwh": 0.0, "ev_kwh": 0.0, "home_cost": 0.0, "ev_cost": 0.0,
             "home_rate": None, "ev_rate": None, "home_rate_exc": None,
             "ev_rate_exc": None, "home_band": None, "ev_band": None}


def _run(coro):
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


class _Sched:
    def is_empty(self): return False
    def day_rate_bounds(self, ts): return (5.493, 32.3092)


class _CountingClient:
    """Records every breakdown fetch so a test can assert one did NOT happen."""

    def __init__(self, node):
        self._node = node
        self.calls = 0
        self.slots_asked = []

    async def recover_device_breakdown(self, mpan, missing, **k):
        self.calls += 1
        self.slots_asked.append(list(missing))
        return {} if self._node is None else {s: self._node for s in missing}

    async def recover_measurement_costs(self, *a, **k):
        raise AssertionError("single-cost path used on SMB")


class _Base(unittest.TestCase):
    SLOT = "2026-09-09T02:00:00"

    def setUp(self):
        self._save = (engine._store, engine._kraken_client, engine._kraken_discovery,
                      engine._kraken_rate_schedules, engine._kraken_current_agreement_from,
                      engine._MEASURED_FETCH_ENABLED)
        self.st = BlockStore(":memory:")
        engine._store = self.st
        engine._kraken_current_agreement_from = "2026-08-01T00:00:00"
        engine._MEASURED_FETCH_ENABLED = True
        engine._kraken_discovery = {"import": {"mpan": "m", "tariff_code": "IOG-SMB-FIX-12M"}}
        engine._kraken_rate_schedules = {"import": _Sched(), "ev_device_off_peak": _Sched()}
        self.st._conn.execute(
            "INSERT OR IGNORE INTO config_periods (id, effective_from, billing_day, "
            "block_minutes, timezone) VALUES (1,'2020-01-01T00:00:00',1,30,'UTC')")
        self.st._conn.commit()

    def tearDown(self):
        (engine._store, engine._kraken_client, engine._kraken_discovery,
         engine._kraken_rate_schedules, engine._kraken_current_agreement_from,
         engine._MEASURED_FETCH_ENABLED) = self._save

    def _block(self, kwh, slot=None):
        slot = slot or self.SLOT
        self.st._conn.execute(
            "INSERT INTO blocks (block_start, block_end, meter_id, config_period_id, "
            "imp_kwh, imp_kwh_api, imp_rate, imp_cost, rate_source, rate_corrected) "
            "VALUES (?,?,?,1,?,?,?,?,'schedule',0)",
            (slot, slot, "electricity_main", kwh, kwh, OFF, round(kwh * OFF, 6)))
        self.st.upsert_dispatch_slot(slot, off_peak=True, source="smart-charge-completed")
        self.st._conn.commit()
        return slot

    def _mc(self, slot=None):
        return self.st._conn.execute(
            "SELECT cost_incl, cost_excl, kwh, label, home_kwh, home_rate, ev_kwh, ev_rate "
            "FROM measured_cost WHERE slot_start=?", (slot or self.SLOT,)).fetchone()


class TestBillSaysZero(_Base):
    """The block drew nothing and the bill agrees — a settled fact, so cache it."""

    def test_zero_is_stored_not_counted_absent(self):
        self._block(0.0)
        engine._kraken_client = _CountingClient(ZERO_NODE)
        res = _run(engine.measure_settled_dispatched_blocks())
        self.assertEqual(res["zero"], 1)
        self.assertEqual(res["absent"], 0)
        self.assertEqual(res["stored"], 0)

    def test_the_cached_row_records_the_bill_s_actual_answer(self):
        self._block(0.0)
        engine._kraken_client = _CountingClient(ZERO_NODE)
        _run(engine.measure_settled_dispatched_blocks())
        r = self._mc()
        self.assertIsNotNone(r, "the bill answered; nothing was written")
        self.assertEqual(float(r["cost_incl"]), 0.0)
        self.assertEqual(float(r["cost_excl"]), 0.0)
        self.assertEqual(float(r["kwh"]), 0.0)
        self.assertEqual(r["label"], "ZERO")

    def test_breakdown_columns_stay_null(self):
        """An empty half-hour has no split to state. NULL keeps the row clear of
        slots_with_bill_split() and run_settled_ev_split_heal()."""
        self._block(0.0)
        engine._kraken_client = _CountingClient(ZERO_NODE)
        _run(engine.measure_settled_dispatched_blocks())
        r = self._mc()
        for col in ("home_kwh", "home_rate", "ev_kwh", "ev_rate"):
            self.assertIsNone(r[col], "%s should stay NULL on a zero slot" % col)

    def test_the_zero_row_is_not_a_bill_stated_split(self):
        self._block(0.0)
        engine._kraken_client = _CountingClient(ZERO_NODE)
        _run(engine.measure_settled_dispatched_blocks())
        self.assertNotIn(self.SLOT,
                         self.st.slots_with_bill_split("2026-09-01", "2026-09-30"))

    def test_second_pass_does_not_touch_the_network(self):
        """THE regression. Before BL-68 this re-fetched every hour, forever."""
        self._block(0.0)
        c = _CountingClient(ZERO_NODE)
        engine._kraken_client = c
        _run(engine.measure_settled_dispatched_blocks())
        self.assertEqual(c.calls, 1)
        res = _run(engine.measure_settled_dispatched_blocks())
        self.assertEqual(c.calls, 1, "the slot was re-fetched after the bill already answered")
        self.assertEqual(res["fetched"], 0)

    def test_the_block_is_left_alone(self):
        """No stamp: imp_kwh=0/imp_cost=0 already agrees with the bill, and
        apply_measured_to_block rightly refuses an empty half-hour."""
        self._block(0.0)
        engine._kraken_client = _CountingClient(ZERO_NODE)
        _run(engine.measure_settled_dispatched_blocks())
        engine.apply_measured_settled()
        rs = self.st._conn.execute(
            "SELECT rate_source, imp_cost FROM blocks WHERE block_start=?",
            (self.SLOT,)).fetchone()
        self.assertEqual(rs["rate_source"], "schedule")
        self.assertEqual(float(rs["imp_cost"]), 0.0)


class TestBillHasNotRunYet(_Base):
    """The block DID draw energy but the bill reports zero — that is a lagging bill,
    not a settled zero. Caching it would be a permanent lie, so keep retrying."""

    def test_zero_against_a_non_empty_block_is_absent_not_zero(self):
        self._block(5.276)
        engine._kraken_client = _CountingClient(ZERO_NODE)
        res = _run(engine.measure_settled_dispatched_blocks())
        self.assertEqual(res["absent"], 1)
        self.assertEqual(res["zero"], 0)

    def test_nothing_is_cached(self):
        self._block(5.276)
        engine._kraken_client = _CountingClient(ZERO_NODE)
        _run(engine.measure_settled_dispatched_blocks())
        self.assertIsNone(self._mc(), "a bill that has not run yet must not be cached")

    def test_it_is_retried_on_the_next_pass(self):
        self._block(5.276)
        c = _CountingClient(ZERO_NODE)
        engine._kraken_client = c
        _run(engine.measure_settled_dispatched_blocks())
        _run(engine.measure_settled_dispatched_blocks())
        self.assertEqual(c.calls, 2)
        self.assertEqual(c.slots_asked[-1], [self.SLOT])


class TestGenuinelyAbsent(_Base):
    """No node at all — the bill said nothing. Unchanged behaviour."""

    def test_missing_node_is_absent_and_uncached(self):
        self._block(0.0)
        engine._kraken_client = _CountingClient(None)
        res = _run(engine.measure_settled_dispatched_blocks())
        self.assertEqual(res["absent"], 1)
        self.assertEqual(res["zero"], 0)
        self.assertIsNone(self._mc())

    def test_missing_node_is_retried(self):
        self._block(0.0)
        c = _CountingClient(None)
        engine._kraken_client = c
        _run(engine.measure_settled_dispatched_blocks())
        _run(engine.measure_settled_dispatched_blocks())
        self.assertEqual(c.calls, 2)


class TestNormalSlotUnaffected(_Base):
    """Guard the happy path: a real bill still stores cost AND split as before."""

    def test_energy_bearing_slot_still_stores_both(self):
        self._block(5.276)
        node = {"home_kwh": 1.980, "ev_kwh": 3.296,
                "home_cost": round(1.980 * OFF, 6), "ev_cost": round(3.296 * OFF, 6),
                "home_rate": OFF, "ev_rate": OFF,
                "home_rate_exc": round(OFF * 0.95238, 6),
                "ev_rate_exc": round(OFF * 0.95238, 6),
                "home_band": "off_peak", "ev_band": "off_peak"}
        engine._kraken_client = _CountingClient(node)
        res = _run(engine.measure_settled_dispatched_blocks())
        self.assertEqual(res["stored"], 1)
        self.assertEqual(res["zero"], 0)
        r = self._mc()
        # the stored cost is the sum of the bill's OWN per-bucket figures, each already
        # rounded — not the total re-rounded, which lands a rounding unit away (0.289810
        # vs 0.289811). Assert what the bill composes, not what the maths would prefer.
        self.assertAlmostEqual(float(r["cost_incl"]),
                               round(node["home_cost"] + node["ev_cost"], 6), places=6)
        self.assertAlmostEqual(float(r["ev_kwh"]), 3.296, places=6)
        self.assertIn(self.SLOT,
                      self.st.slots_with_bill_split("2026-09-01", "2026-09-30"))


if __name__ == "__main__":
    unittest.main()
