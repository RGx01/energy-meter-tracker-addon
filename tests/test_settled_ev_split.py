"""Regression: the settled bill's EV bucket must not be discarded when no dispatch
data exists for the slot.

`apply_measured_to_block` caps the EV quantity at the car's own completed-dispatch
session — measured is king (4.5.9), because Octopus's EV_DEVICE bucket over-attributes
when another load (a home battery) draws concurrently inside the dispatch window.

But a dispatch ceiling of ZERO is ambiguous: either the car did not charge (a live slot
we were watching), or we hold no dispatch data for that time at all. Imported history is
always the latter — Octopus serves a short rolling dispatch window and no history. The
old code treated both as "no EV", so a fresh install settled its IOG-SMB era from the
bill with the right total cost and a house-only split, throwing away the very bucket the
bill had supplied: 438 slots settled, 111 carrying EV, 0 with imp_kwh_ev.
"""
import os
import sys
import types
import unittest
from unittest.mock import MagicMock

eio = types.ModuleType("energy_engine_io")
eio.ensure_dir = lambda *a, **kw: None
eio.load_json = lambda *a, **kw: a[1] if len(a) > 1 else {}
eio.save_json_atomic = lambda *a, **kw: None
eio.save_file = lambda *a, **kw: None
sys.modules["energy_engine_io"] = eio
ec = types.ModuleType("energy_charts")
ec.generate_net_heatmap = lambda *a, **kw: ""
ec.generate_daily_import_export_charts = lambda *a, **kw: ""
sys.modules["energy_charts"] = ec
hc = types.ModuleType("ha_client"); hc.HAClient = MagicMock
sys.modules["ha_client"] = hc

from block_store import BlockStore
from kraken_rates import RateSchedule

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import engine

SLOT = "2026-09-01T02:00:00"          # inside the IOG off-peak window
CFG = {"meters": {"electricity_main": {"meta": {
    "timezone": "Europe/London", "billing_day": 1, "block_minutes": 30,
    "currency_symbol": "£", "currency_code": "GBP", "sub_meter": False}}}}


class _Base(unittest.TestCase):
    def setUp(self):
        self._s, self._sch, self._d = (engine._store, engine._kraken_rate_schedules,
                                       engine._kraken_discovery)
        st = BlockStore(":memory:"); st.insert_config_period(CFG)
        engine._store = st
        engine._kraken_discovery = {"import": {"mpan": "IM", "tariff_code": "E-1R-IOG-SMB-X-B",
                                               "product_code": "IOG-SMB-X"}}
        _night = ("2026-08-31T22:30:00", "2026-09-01T04:30:00")
        _day = ("2026-09-01T04:30:00", "2026-09-01T22:30:00")
        engine._kraken_rate_schedules = {
            "import": RateSchedule([_night + (5.493,), _day + (32.3092,)]),
            "ev_device_off_peak": RateSchedule([_night + (5.493,)]),
            "ev_device_peak": RateSchedule([_day + (32.3092,)]),
        }

    def tearDown(self):
        (engine._store, engine._kraken_rate_schedules,
         engine._kraken_discovery) = self._s, self._sch, self._d

    def _block(self, *, kwh=6.0, source="imported_api", api=None, corrected=0):
        engine._store._conn.execute(
            "INSERT INTO blocks (block_start, block_end, meter_id, config_period_id, "
            "imp_kwh, imp_rate, imp_cost, source, imp_kwh_api, rate_corrected) "
            "VALUES (?,?,?,?,?,?,?,?,?,?)",
            (SLOT, "2026-09-01T02:30:00", "electricity_main", 1, kwh, 0.05493,
             round(kwh * 0.05493, 6), source, api, corrected))
        engine._store._conn.commit()

    def _bill(self, *, ev_kwh, cost=0.329580):
        engine._store.upsert_measured_cost(SLOT, mpan="IM", cost_incl=cost,
                                           cost_excl=round(cost / 1.05, 6),
                                           label="OFF_PEAK", kwh=6.0)
        engine._store.upsert_measured_breakdown(
            SLOT, home_kwh=round(6.0 - ev_kwh, 6), home_rate=0.05493,
            ev_kwh=ev_kwh, ev_rate=0.05493, mpan="IM")

    def _dispatch(self, kwh):
        engine._store.record_dispatch_history(SLOT, "completed", provider="Myenergi",
                                              source="unknown", energy_kwh=-abs(kwh))

    def _ev(self):
        r = engine._store._conn.execute(
            "SELECT imp_kwh_ev FROM blocks WHERE block_start=? AND meter_id='electricity_main'",
            (SLOT,)).fetchone()
        return r["imp_kwh_ev"]

    def _apply(self):
        return engine.apply_measured_to_block(SLOT, cost_incl=0.329580,
                                              cost_excl=0.313886, label="OFF_PEAK")


class TestAmbiguousCeiling(_Base):
    def test_imported_no_dispatch_takes_the_bill_split(self):
        """THE regression: no dispatch data anywhere -> the bill is the only evidence."""
        self._block(); self._bill(ev_kwh=4.5)
        self.assertTrue(self._apply())
        self.assertAlmostEqual(self._ev() or 0, 4.5, places=6,
                               msg="the bill's EV bucket must not be discarded")

    def test_bill_ev_is_grid_clipped(self):
        self._block(kwh=3.0); self._bill(ev_kwh=4.5)
        self._apply()
        self.assertAlmostEqual(self._ev() or 0, 3.0, places=6,
                               msg="EV can never exceed the slot's grid import")

    def test_imported_with_no_ev_bucket_stays_house(self):
        self._block(); self._bill(ev_kwh=0.0)
        self._apply()
        self.assertIsNone(self._ev(), "no EV in the bill -> house-only")


class TestDispatchCapStillHolds(_Base):
    """4.5.9 'measured is king' must be unchanged wherever dispatch data exists."""

    def test_live_slot_with_a_zero_dispatch_record_stays_house(self):
        # A live block we WERE watching, with a completed record of ~nothing: the car
        # genuinely did not charge, so the bill's bucket must not override it.
        self._block(source="kraken_api", api=6.0)
        self._bill(ev_kwh=4.5)
        self._dispatch(0.0)
        self._apply()
        self.assertIsNone(self._ev(), "a live slot with dispatch evidence stays house-only")

    def test_dispatch_caps_an_over_reading_bill(self):
        # The battery-absorbed case the cap exists for: bill says 4.5, car did 2.0.
        self._block(source="kraken_api", api=6.0)
        self._bill(ev_kwh=4.5)
        self._dispatch(2.0)
        self._apply()
        self.assertAlmostEqual(self._ev() or 0, 2.0, places=6,
                               msg="dispatch remains the ceiling where it is known")


class TestSettledEvSplitHeal(_Base):
    def test_heals_a_block_settled_house_only(self):
        self._block(); self._bill(ev_kwh=4.5)
        engine._store._conn.execute(
            "UPDATE blocks SET rate_source='measured' WHERE block_start=?", (SLOT,))
        engine._store._conn.commit()
        self.assertIsNone(self._ev())
        r = engine.run_settled_ev_split_heal()
        self.assertEqual(r.get("healed"), 1, r)
        self.assertAlmostEqual(self._ev() or 0, 4.5, places=6)

    def test_is_idempotent(self):
        self._block(); self._bill(ev_kwh=4.5)
        engine._store._conn.execute(
            "UPDATE blocks SET rate_source='measured' WHERE block_start=?", (SLOT,))
        engine._store._conn.commit()
        engine.run_settled_ev_split_heal()
        self.assertEqual(engine.run_settled_ev_split_heal().get("healed"), 0,
                         "steady state must be a no-op")

    def test_never_touches_a_corrected_block(self):
        self._block(corrected=1); self._bill(ev_kwh=4.5)
        engine._store._conn.execute(
            "UPDATE blocks SET rate_source='corrected' WHERE block_start=?", (SLOT,))
        engine._store._conn.commit()
        self.assertEqual(engine.run_settled_ev_split_heal().get("healed"), 0)
        self.assertIsNone(self._ev())


if __name__ == "__main__":
    unittest.main()
