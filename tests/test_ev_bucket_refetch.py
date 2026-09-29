"""
#481 one-off recovery: re-fetch breakdowns cached while the bucket labels were unknown.

The 2026-09-28 rename made the parser sum EV energy into the home bucket, so affected
`measured_cost` rows carry ev_kwh = 0 and a home figure that is really home+EV. The
division was summed, not mislabelled, so it cannot be reconstructed locally — but the
rename is retrospective, so a re-fetch under the corrected parser returns the true split.

These tests pin the SCOPE (only rows that are demonstrably wrong) and the hand-off to
run_settled_ev_split_heal, which does the block half.
"""
import asyncio
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import engine
from block_store import BlockStore


class _FakeClient:
    """Returns the true buckets Octopus serves today for the requested slots."""
    def __init__(self, by_slot):
        self.by_slot = by_slot
        self.asked = None

    async def recover_device_breakdown(self, mpan, slots, **kw):
        self.asked = list(slots)
        return {s: self.by_slot[s] for s in slots if s in self.by_slot}


def _node(home_kwh, ev_kwh, rate=0.05493):
    return {"home_kwh": home_kwh, "ev_kwh": ev_kwh,
            "home_rate": rate, "ev_rate": rate,
            "home_rate_exc": rate / 1.05, "ev_rate_exc": rate / 1.05,
            "home_cost": round(home_kwh * rate, 6), "ev_cost": round(ev_kwh * rate, 6),
            "home_band": "off_peak", "ev_band": "off_peak"}


class TestEvBucketRefetch(unittest.TestCase):
    MPAN = "2000000000001"

    def setUp(self):
        self._save = (engine._store, engine._kraken_client,
                      engine._kraken_discovery, engine._import_is_smb_capped)
        self.st = BlockStore(":memory:")
        engine._store = self.st
        engine._kraken_discovery = {"import": {"mpan": self.MPAN}}
        engine._import_is_smb_capped = lambda: True

    def tearDown(self):
        (engine._store, engine._kraken_client,
         engine._kraken_discovery, engine._import_is_smb_capped) = self._save

    def _cache(self, slot, home, ev):
        self.st.upsert_measured_cost(slot, mpan=self.MPAN, cost_incl=1.0, cost_excl=0.95,
                                     label="STANDARD_RATE", kwh=home + ev)
        self.st.upsert_measured_breakdown(slot, mpan=self.MPAN, home_kwh=home,
                                          home_rate=0.05493, ev_kwh=ev, ev_rate=0.05493)

    def _dispatch(self, slot, completed):
        self.st._conn.execute(
            "INSERT OR REPLACE INTO dispatch_slots "
            "(slot_start, off_peak, provider, source, captured_at, energy_completed) "
            "VALUES (?,1,'Myenergi','smart-charge','2026-09-28T00:00:00',?)",
            (slot, completed))
        self.st._conn.commit()

    def _run(self, fake):
        engine._kraken_client = fake
        return asyncio.get_event_loop().run_until_complete(
            engine.run_ev_bucket_refetch(force=True))

    def test_corrupted_row_is_refetched_and_split_restored(self):
        slot = "2026-09-27T02:30:00"
        self._cache(slot, home=6.255, ev=0.0)        # the summed-into-home corruption
        self._dispatch(slot, -3.21)
        res = self._run(_FakeClient({slot: _node(2.99113, 3.26387)}))
        self.assertEqual(res["refetched"], 1)
        row = self.st._conn.execute(
            "SELECT home_kwh, ev_kwh, label FROM measured_cost WHERE slot_start=?",
            (slot,)).fetchone()
        self.assertAlmostEqual(row["ev_kwh"], 3.26387, places=5)
        self.assertAlmostEqual(row["home_kwh"], 2.99113, places=5)
        self.assertEqual(row["label"], "OFF_PEAK",
                         msg="#481 also stamped off-peak energy STANDARD_RATE")

    def test_zero_ev_without_a_completed_dispatch_is_left_alone(self):
        """A half-hour the car genuinely didn't charge must not be touched."""
        slot = "2026-09-27T14:00:00"
        self._cache(slot, home=0.4, ev=0.0)          # no dispatch row at all
        fake = _FakeClient({})
        res = self._run(fake)
        self.assertEqual(res["refetched"], 0)
        self.assertIsNone(fake.asked, "must not even ask about an undispatched slot")

    def test_row_that_already_has_ev_is_not_refetched(self):
        slot = "2026-09-25T00:00:00"
        self._cache(slot, home=0.153, ev=3.497)
        self._dispatch(slot, -3.497)
        fake = _FakeClient({slot: _node(9.9, 9.9)})
        res = self._run(fake)
        self.assertEqual(res["refetched"], 0)
        row = self.st._conn.execute(
            "SELECT home_kwh, ev_kwh FROM measured_cost WHERE slot_start=?", (slot,)).fetchone()
        self.assertAlmostEqual(row["ev_kwh"], 3.497, places=5, msg="a good row was clobbered")

    def test_still_house_only_after_refetch_is_accepted_not_looped(self):
        """An out-of-app bump is legitimately not EV — accept the zero and stop asking."""
        slot = "2026-09-27T10:00:00"
        self._cache(slot, home=4.989, ev=0.0)
        self._dispatch(slot, -2.53)
        res = self._run(_FakeClient({slot: _node(4.989, 0.0)}))
        self.assertEqual(res["refetched"], 0)
        self.assertEqual(res["still_house_only"], 1)

    def test_marker_makes_it_one_off(self):
        slot = "2026-09-27T03:00:00"
        self._cache(slot, home=6.328, ev=0.0)
        self._dispatch(slot, -3.46)
        fake = _FakeClient({slot: _node(2.831, 3.497)})
        self.assertEqual(self._run(fake)["refetched"], 1)
        engine._kraken_client = fake
        again = asyncio.get_event_loop().run_until_complete(engine.run_ev_bucket_refetch())
        self.assertEqual(again.get("skipped"), "already done")


if __name__ == "__main__":
    unittest.main()
