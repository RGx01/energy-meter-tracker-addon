"""
Regression: every device is priced on the MAIN meter's EFFECTIVE import rate
(base tariff + dispatch overlay), for both cost and the displayed rate.

Devices no longer make any per-device rate decision. All device grid import is
costed in PASS 2 (_apply_pass2), which runs after every meter is finalised and
sets each device's rate and cost from the parent's finalised rate. So a device
follows the main's off-peak dispatch rate whether or not it drew much itself,
with no dependency on meter order, and there is no per-device over-report floor
to re-qualify — only the MAIN meter qualifies the overlay, on grid draw.

These guards originally caught a prod-dev bug where a sub-floor follower (a
battery idling at ~0 kWh while the EV charged) was stranded on the main's PEAK
base rate. Under the PASS 2 model that cannot happen: the device inherits the
main's effective rate directly.

Style mirrors test_pass2_ev_priority.py; the finalise harness mirrors
TestDispatchOverlayAtFinalise in test_engine.py.
"""
import sys
import os
import json
import types
import unittest
from datetime import datetime
from unittest.mock import MagicMock, patch

# ── Minimal stubs so engine.py imports without HA/filesystem ─────────────────
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

hc = types.ModuleType("ha_client")
hc.HAClient = MagicMock
sys.modules["ha_client"] = hc

from block_store import BlockStore
from kraken_rates import RateSchedule

sys.path.insert(0, os.path.dirname(__file__))
import engine

OFF_PEAK = 0.05493
PEAK = 0.323092
SLOT = "2026-06-07T15:00:00"   # out-of-window (peak) smart-charge dispatch slot
SLOT_END = "2026-06-07T15:30:00"
NO_SLOT = "2026-06-07T16:00:00"   # peak block with NO captured dispatch slot
NO_SLOT_END = "2026-06-07T16:30:00"


class _HA:
    def get_state(self, e):
        return None


class TestFollowMainDeviceOverlay(unittest.TestCase):
    def setUp(self):
        # Main on the API (so the device base resolves the schedule per-slot);
        # battery and EV both on "use main meter rate".
        self.cfg = {"meters": {
            "electricity_main": {
                "meta": {"timezone": "Europe/London", "billing_day": 1,
                         "block_minutes": 30, "currency_symbol": "£",
                         "sub_meter": False},
                "channels": {
                    "import": {"read": "sensor.main_imp", "rate": "",
                               "rate_source": "api"},
                    "export": {"read": "", "rate": ""}}},
            "house_battery": {
                "meta": {"sub_meter": True, "parent_meter": "electricity_main",
                         "meter_type": "battery"},
                "channels": {"import": {"read": "sensor.batt_imp",
                                        "rate_source": "main"}}},
            "ev_charger": {
                "meta": {"sub_meter": True, "parent_meter": "electricity_main",
                         "meter_type": "ev"},
                "channels": {"import": {"read": "sensor.ev_imp",
                                        "rate_source": "main"}}},
        }}
        self._orig_cfg_path = engine.CONFIG_PATH
        engine.CONFIG_PATH = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "_test_meters_config.json")
        with open(engine.CONFIG_PATH, "w") as f:
            json.dump(self.cfg, f)
        self._lj = patch.object(engine, "load_json",
                                side_effect=lambda *a, **k: self.cfg)
        self._lj.start()
        self._orig_store = engine._store
        self._orig_sched = engine._kraken_rate_schedules
        self._orig_apply = engine._DISPATCH_OVERLAY_APPLY
        engine._store = BlockStore(":memory:")
        engine._store.insert_config_period(self.cfg)
        engine.set_data_source_mode("api")
        engine._DISPATCH_OVERLAY_APPLY = True
        engine._kraken_rate_schedules = {"import": RateSchedule([
            ("2026-06-06T22:30:00", "2026-06-07T04:30:00", 5.493),
            ("2026-06-07T04:30:00", "2026-06-07T22:30:00", 32.3092)])}
        engine._store.upsert_dispatch_slot(
            SLOT, off_peak=True, provider="MYENERGI_V2", source="smart-charge")

    def tearDown(self):
        self._lj.stop()
        engine._store = self._orig_store
        engine._kraken_rate_schedules = self._orig_sched
        engine._DISPATCH_OVERLAY_APPLY = self._orig_apply
        try:
            os.remove(engine.CONFIG_PATH)
        except OSError:
            pass
        engine.CONFIG_PATH = self._orig_cfg_path

    def _finalise(self, start, end, main, batt, ev):
        """Finalise one block with given import deltas (kWh) per meter."""
        blk = engine.create_block(datetime.fromisoformat(start),
                                  datetime.fromisoformat(end), 30,
                                  seed_meters=True)

        def _reads(meter, lo, delta):
            blk["meters"][meter]["channels"]["import"]["reads"] = [
                {"ts": start, "value": lo},
                {"ts": end, "value": round(lo + delta, 6)}]
        _reads("electricity_main", 100.0, main)
        _reads("house_battery", 10.0, batt)
        _reads("ev_charger", 20.0, ev)
        engine.finalise_block(_HA(), block_data=blk)

    def _rate(self, start, meter):
        row = engine._store._conn.execute(
            "SELECT imp_rate FROM blocks WHERE block_start=? AND meter_id=?",
            (start, meter)).fetchone()
        return row["imp_rate"] if row else None

    def _cost(self, start, meter):
        row = engine._store._conn.execute(
            "SELECT imp_cost FROM blocks WHERE block_start=? AND meter_id=?",
            (start, meter)).fetchone()
        return row["imp_cost"] if row else None

    def _settle(self, start, dcc_main_kwh):
        """Reconcile a finalised block against a DCC main-import figure."""
        blk = engine._store.get_block_dict_by_start(start)
        blk["meters"]["electricity_main"]["imp_kwh_api"] = dcc_main_kwh
        engine._rerun_pass2_for_settled_block(
            blk, billing_source="api", standing_resolver=None)
        engine.append_block_replace(blk)

    def test_subfloor_battery_follows_main_overlay(self):
        # main 4.0 kWh (>floor), EV 3.4 kWh (>floor), battery 0.05 kWh (<floor).
        self._finalise(SLOT, SLOT_END, main=4.0, batt=0.05, ev=3.4)
        self.assertAlmostEqual(
            self._rate(SLOT, "electricity_main"), OFF_PEAK, places=5,
            msg="main drew above floor in a smart-charge slot -> off-peak")
        self.assertAlmostEqual(
            self._rate(SLOT, "ev_charger"), OFF_PEAK, places=5,
            msg="EV above floor -> off-peak (unaffected by the fix)")
        # THE REGRESSION: the battery barely drew, but as a follow-main device it
        # must inherit main's off-peak overlay rather than being left on peak.
        self.assertAlmostEqual(
            self._rate(SLOT, "house_battery"), OFF_PEAK, places=5,
            msg="follow-main battery below its OWN floor must still inherit "
                "main's off-peak overlay (regression: was wrongly peak)")

    def test_no_dispatch_slot_follower_stays_peak(self):
        # No captured slot: nobody is overlaid, so the follower stays on main's
        # base (peak). Guards against the fix over-applying off-peak.
        self._finalise(NO_SLOT, NO_SLOT_END, main=4.0, batt=0.05, ev=3.4)
        self.assertAlmostEqual(
            self._rate(NO_SLOT, "house_battery"), PEAK, places=5,
            msg="no smart-charge slot -> follower stays on main's base (peak)")
        self.assertAlmostEqual(
            self._rate(NO_SLOT, "electricity_main"), PEAK, places=5,
            msg="no slot -> main itself stays peak")

    def _finalise_ordered(self, start, end, order, main, batt, ev):
        """Finalise one block whose meters dict is in an EXPLICIT order.

        Reproduces the live prod-dev ordering, where the current block presents
        the sub-meters BEFORE the main meter. The follow-main overlay reads the
        parent's just-finalised import kWh for its floor check, so it must not
        depend on dict insertion order.
        """
        blk = engine.create_block(datetime.fromisoformat(start),
                                  datetime.fromisoformat(end), 30,
                                  seed_meters=True)
        # Re-key the meters dict into the requested order (same channel objects).
        blk["meters"] = {name: blk["meters"][name] for name in order}

        def _reads(meter, lo, delta):
            blk["meters"][meter]["channels"]["import"]["reads"] = [
                {"ts": start, "value": lo},
                {"ts": end, "value": round(lo + delta, 6)}]
        _reads("electricity_main", 100.0, main)
        _reads("house_battery", 10.0, batt)
        _reads("ev_charger", 20.0, ev)
        engine.finalise_block(_HA(), block_data=blk)

    def test_subfloor_battery_follows_main_when_submeters_first(self):
        # The block presents sub-meters BEFORE the main meter — the live ordering
        # behind the original prod-dev report. Because devices are priced in PASS 2
        # (after every meter is finalised), block-meter order cannot affect a
        # device's rate: the sub-floor battery still inherits the main's off-peak
        # rate. Guards against any future change that reintroduces per-PASS-1
        # device pricing (which was order-sensitive).
        self._finalise_ordered(
            SLOT, SLOT_END,
            order=("house_battery", "ev_charger", "electricity_main"),
            main=4.0, batt=0.05, ev=3.4)
        self.assertAlmostEqual(
            self._rate(SLOT, "house_battery"), OFF_PEAK, places=5,
            msg="follow-main battery must inherit main's off-peak overlay even "
                "when sub-meters are finalised before the main (ordering fix)")
        self.assertAlmostEqual(
            self._rate(SLOT, "electricity_main"), OFF_PEAK, places=5,
            msg="main is above the floor -> off-peak regardless of order")
        self.assertAlmostEqual(
            self._rate(SLOT, "ev_charger"), OFF_PEAK, places=5,
            msg="EV is above the floor -> off-peak regardless of order")


    def test_settlement_prices_device_grid_on_main_offpeak_not_peak(self):
        # The COST path at reconcile: a block finalises with the main sub-floor
        # (over-report guard leaves it on PEAK), then DCC settlement raises the
        # main import above the floor. The main is re-overlaid to off-peak, and
        # the device's grid import must be re-costed at that off-peak rate.
        #
        # Regression: settlement applied the overlay to the main's COST but left
        # its stored rate on the pre-overlay PEAK base, so PASS 2 re-priced every
        # device's grid import at peak while the main billed off-peak — a real
        # overcharge on every reconciled smart-charge block.
        self._finalise(SLOT, SLOT_END, main=0.05, batt=0.0, ev=0.05)
        # sub-floor at finalise -> main (and its follower EV) on peak, correctly
        self.assertAlmostEqual(self._rate(SLOT, "electricity_main"), PEAK, places=5)
        self._settle(SLOT, dcc_main_kwh=3.0)
        # main re-overlaid to off-peak, and its stored rate now reflects that
        self.assertAlmostEqual(
            self._rate(SLOT, "electricity_main"), OFF_PEAK, places=5,
            msg="settled main effective rate must be off-peak")
        self.assertAlmostEqual(
            self._cost(SLOT, "electricity_main"), round(3.0 * OFF_PEAK, 6), places=5)
        # THE COST REGRESSION: the EV's grid import settles at the main's off-peak
        # rate, not the stale peak base.
        self.assertAlmostEqual(
            self._cost(SLOT, "ev_charger"), round(0.05 * OFF_PEAK, 6), places=5,
            msg="device grid import must settle at the main's off-peak rate "
                "(regression: PASS 2 read the main's stale pre-overlay peak rate)")


    def test_own_rate_on_device_is_ignored_priced_at_main(self):
        # A device carrying its OWN rate (a rate sensor / explicit per-slot rates)
        # is still priced at the main meter's effective rate — the per-device
        # own-rate path was removed. Pins the "devices always follow the main
        # meter" contract for both rate and cost.
        blk = engine.create_block(datetime.fromisoformat(SLOT),
                                  datetime.fromisoformat(SLOT_END), 30,
                                  seed_meters=True)

        def _reads(meter, lo, delta):
            blk["meters"][meter]["channels"]["import"]["reads"] = [
                {"ts": SLOT, "value": lo},
                {"ts": SLOT_END, "value": round(lo + delta, 6)}]
        _reads("electricity_main", 100.0, 4.0)
        _reads("house_battery", 10.0, 0.0)
        _reads("ev_charger", 20.0, 1.5)
        # Give the EV its OWN rate (0.20) — must be ignored in favour of main's.
        blk["meters"]["ev_charger"]["channels"]["import"]["rates"] = [
            {"ts": SLOT, "value": 0.20}, {"ts": SLOT_END, "value": 0.20}]
        engine.finalise_block(_HA(), block_data=blk)
        # main is above the floor in a smart-charge slot -> off-peak
        self.assertAlmostEqual(self._rate(SLOT, "electricity_main"), OFF_PEAK, places=5)
        # EV priced at the MAIN's off-peak rate, NOT its own 0.20
        self.assertAlmostEqual(
            self._rate(SLOT, "ev_charger"), OFF_PEAK, places=5,
            msg="device own rate must be ignored; priced at main's effective rate")
        self.assertAlmostEqual(
            self._cost(SLOT, "ev_charger"), round(1.5 * OFF_PEAK, 6), places=5)


    def test_zero_draw_devices_follow_main_rate(self):
        # A device that drew NOTHING in a block must still show the main meter's
        # effective rate (cost 0), not a stale/collapsed compute_channel rate.
        # Regression (found on a live 04:30 off-peak->peak boundary block): PASS 2
        # skips zero-draw devices, so they kept the sub-meter reconstruction's
        # running-minimum rate, which collapses to the adjacent off-peak rate at a
        # boundary block — leaving the battery/EV off-peak while the main was peak.
        self._finalise(NO_SLOT, NO_SLOT_END, main=1.0, batt=0.0, ev=0.0)
        main_rate = self._rate(NO_SLOT, "electricity_main")
        self.assertAlmostEqual(main_rate, PEAK, places=5,
                               msg="main resolves to peak in a no-slot peak block")
        self.assertAlmostEqual(
            self._rate(NO_SLOT, "house_battery"), main_rate, places=5,
            msg="zero-draw battery must follow the main's rate, not collapse off-peak")
        self.assertAlmostEqual(
            self._rate(NO_SLOT, "ev_charger"), main_rate, places=5,
            msg="zero-draw EV must follow the main's rate")


class TestCompletedDispatchEnergy(unittest.TestCase):
    """#253 groundwork: _completed_dispatch_slot_energy distributes a completed
    dispatch's delta across the slots it covers, with NO source filter (completed
    dispatches come back source='unknown'), signed like planned (negative)."""

    def test_single_slot_completed_energy(self):
        out = engine._completed_dispatch_slot_energy(
            [{"start": "2026-07-07T02:00:00", "end": "2026-07-07T02:30:00",
              "delta": -3.2, "meta": {"source": "unknown"}}])
        self.assertAlmostEqual(out["2026-07-07T02:00:00"], -3.2, places=3)

    def test_multi_slot_distributes_evenly(self):
        out = engine._completed_dispatch_slot_energy(
            [{"start": "2026-07-07T02:00:00", "end": "2026-07-07T03:00:00",
              "delta": -4.0}])
        self.assertAlmostEqual(out["2026-07-07T02:00:00"], -2.0, places=3)
        self.assertAlmostEqual(out["2026-07-07T02:30:00"], -2.0, places=3)

    def test_no_source_filter(self):
        # unlike the planned helper, unknown/absent source is still counted
        out = engine._completed_dispatch_slot_energy(
            [{"start": "2026-07-07T02:00:00", "end": "2026-07-07T02:30:00",
              "delta": -1.5}])
        self.assertIn("2026-07-07T02:00:00", out)

    def test_null_delta_maps_none(self):
        out = engine._completed_dispatch_slot_energy(
            [{"start": "2026-07-07T02:00:00", "end": "2026-07-07T02:30:00",
              "delta": None}])
        self.assertIsNone(out["2026-07-07T02:00:00"])


class TestCompletedCaptureWhenPlannedEmpty(unittest.IsolatedAsyncioTestCase):
    """Regression: completed dispatches arrive AFTER the charge, when
    flexPlannedDispatches is already empty (planned=0). An early return on empty
    planned skipped the completed capture entirely — energy_completed never
    populated. Completed must still annotate existing slots when planned=0."""

    async def test_completed_captured_with_planned_empty(self):
        from unittest.mock import AsyncMock
        from block_store import BlockStore
        st = BlockStore(":memory:")
        # a slot captured earlier as planned (off_peak) — the overnight charge
        st.upsert_dispatch_slot("2026-07-07T04:30:00", off_peak=True,
                                provider="Myenergi", source="smart-charge",
                                state="planned", energy_planned=-3.25)
        client = AsyncMock()
        client.get_dispatches = AsyncMock(return_value={
            "provider": "Myenergi", "planned": [],      # post-charge: empty
            "completed": [{"start": "2026-07-07T04:30:00+00:00",
                           "end": "2026-07-07T05:00:00+00:00", "delta": -3.21}],
        })
        client.last_deprecations = None
        for attr, val in (("_store", st), ("_kraken_client", client),
                          ("_kraken_discovery", {"x": 1}),
                          ("_kraken_account_number", "ACC"),
                          ("_deprecations_surfaced", True)):
            setattr(engine, attr, val)
        try:
            await engine._capture_dispatch_slots()
            r = st.get_dispatch_slot("2026-07-07T04:30:00")
            self.assertEqual(r["state"], "completed")
            self.assertAlmostEqual(r["energy_completed"], -3.21, places=3)
            self.assertAlmostEqual(r["energy_planned"], -3.25, places=3)  # preserved
            self.assertEqual(r["off_peak"], 1)                            # preserved
        finally:
            engine._kraken_client = None
            engine._store = None


if __name__ == "__main__":
    unittest.main()

class TestDeriveStartedSlots(unittest.TestCase):
    """§11.2: current slot becomes 'started' iff intelligent state is
    SMART_CONTROL_IN_PROGRESS AND a planned dispatch is active now. A bump never
    enters that state, so it never starts."""

    def _planned(self, s, e):
        return [{"start": s, "end": e, "source": "smart-charge"}]

    def test_started_when_in_progress_and_planned_active(self):
        now = datetime(2026, 7, 7, 2, 15)  # inside 02:00-02:30
        out = engine._derive_started_slots(
            now, self._planned("2026-07-07T02:00:00", "2026-07-07T03:00:00"),
            "SMART_CONTROL_IN_PROGRESS")
        self.assertEqual(out, ["2026-07-07T02:00:00"])

    def test_not_started_when_state_only_capable(self):
        now = datetime(2026, 7, 7, 2, 15)
        out = engine._derive_started_slots(
            now, self._planned("2026-07-07T02:00:00", "2026-07-07T03:00:00"),
            "SMART_CONTROL_CAPABLE")
        self.assertEqual(out, [])

    def test_not_started_when_no_planned_active_now(self):
        now = datetime(2026, 7, 7, 5, 15)  # after the planned window
        out = engine._derive_started_slots(
            now, self._planned("2026-07-07T02:00:00", "2026-07-07T03:00:00"),
            "SMART_CONTROL_IN_PROGRESS")
        self.assertEqual(out, [])

    def test_not_started_when_state_none(self):
        now = datetime(2026, 7, 7, 2, 15)
        self.assertEqual(engine._derive_started_slots(
            now, self._planned("2026-07-07T02:00:00", "2026-07-07T03:00:00"),
            None), [])

    def test_second_half_hour_slot(self):
        now = datetime(2026, 7, 7, 2, 45)  # inside 02:30-03:00
        out = engine._derive_started_slots(
            now, self._planned("2026-07-07T02:00:00", "2026-07-07T03:00:00"),
            "SMART_CONTROL_IN_PROGRESS")
        self.assertEqual(out, ["2026-07-07T02:30:00"])


if __name__ == "__main__":
    unittest.main()