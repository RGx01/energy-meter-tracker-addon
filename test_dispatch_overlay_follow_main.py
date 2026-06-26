"""
Regression: a "use main meter rate" device must follow the MAIN meter's dispatch
overlay, not re-qualify the overlay against its OWN draw.

Bug (caught pre-3.0-release on a prod-dev DB): the dispatch overlay's 0.1 kWh
over-report floor was evaluated against the device's own import. A follower that
barely draws during a smart-charge slot — e.g. a battery idling at ~0 kWh while
the EV charges — fell below the floor and was left on the main meter's PEAK base
rate, while the main meter and the EV (both above the floor) were repriced
off-peak. A rate_source == 'main' device is supposed to inherit main's EFFECTIVE
rate, so the floor must be evaluated against the MAIN meter's draw.

Fixed by sourcing the overlay floor from the parent's finalised import kWh in the
device-overlay branch of finalise_block. EV behaviour is unchanged (its own draw
already clears the floor, and main's draw is always >= the EV's).

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


if __name__ == "__main__":
    unittest.main()