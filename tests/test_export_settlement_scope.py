"""
Regression (BL-62): an EXPORT-only settlement must never re-price the IMPORT channel.

`needs_pass2_rerun` is a BLOCK-level flag with no channel. `upsert_kraken_block`
consults imp_kwh_api / exp_kwh_api to decide whether *that channel's* figure changed,
then raises one shared flag; `_drain_pass2_queue` reloads the whole block and re-runs
every channel. So a Kraken poll that settles only EXPORT queues the block, and the
import channel is re-run against a `chosen` kWh that is just the unchanged CAD figure —
nothing to re-cost, but the old code still re-resolved the dispatch overlay and re-ran
the IOG split, which can move a historical rate.

The prod case this reproduces (2026-09-13 12:00 BST):
  * block finalised at peak (no completed dispatch yet)
  * completed dispatch arrives an hour later, 0.08 kWh against a 0.012 kWh draw
  * ~30 h later a restart forces the 6-hourly Kraken poll, which settles EXPORT for
    the whole day (48/48 slots) while IMPORT is still unsettled (imp_kwh_api NULL)
  * the drain re-runs the block; the dispatch overlay REFUSES (0.012 < the 0.10
    over-report floor, and says so in the log) but `_apply_iog_split` carries no floor,
    so the capped seam repriced the slot 0.323092 -> 0.054917

Style and harness mirror test_dispatch_overlay_follow_main.py.
"""
import sys
import os
import json
import shutil
import tempfile
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

# Midday, out of the IOG off-peak window -> schedule base is PEAK.
SUB = "2026-06-07T11:00:00"        # sub-floor draw (the prod case)
SUB_END = "2026-06-07T11:30:00"
BIG = "2026-06-07T12:00:00"        # above-floor draw
BIG_END = "2026-06-07T12:30:00"


class _HA:
    def get_state(self, e):
        return None


class TestExportOnlySettlementScope(unittest.TestCase):
    HAS_EXPORT = True

    def setUp(self):
        _channels = {"import": {"read": "sensor.main_imp", "rate": "",
                                "rate_source": "api"}}
        if self.HAS_EXPORT:
            _channels["export"] = {"read": "sensor.main_exp", "rate": "",
                                   "rate_source": "api"}
        self.cfg = {"meters": {
            "electricity_main": {
                "meta": {"timezone": "Europe/London", "billing_day": 1,
                         "block_minutes": 30, "currency_symbol": "£",
                         "sub_meter": False},
                "channels": _channels},
        }}
        self._orig_cfg_path = engine.CONFIG_PATH
        self._cfg_dir = tempfile.mkdtemp(prefix="emt-test-cfg-")
        engine.CONFIG_PATH = os.path.join(self._cfg_dir, "meters_config.json")
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
        # CAPPED IOG-SMB: import + both ev_device legs present, so _apply_iog_split
        # takes the capped branch (the one that re-prices rate/cost).
        _day = ("2026-06-07T04:30:00", "2026-06-07T22:30:00")
        engine._kraken_rate_schedules = {
            "import": RateSchedule([
                ("2026-06-06T22:30:00", "2026-06-07T04:30:00", 5.493),
                (_day[0], _day[1], 32.3092)]),
            "export": RateSchedule([("2026-06-06T00:00:00", None, 12.0)]),
            "ev_device_off_peak": RateSchedule([(_day[0], _day[1], 5.493)]),
            "ev_device_peak": RateSchedule([(_day[0], _day[1], 32.3092)]),
        }

    def tearDown(self):
        self._lj.stop()
        engine._store = self._orig_store
        engine._kraken_rate_schedules = self._orig_sched
        engine._DISPATCH_OVERLAY_APPLY = self._orig_apply
        shutil.rmtree(self._cfg_dir, ignore_errors=True)
        engine.CONFIG_PATH = self._orig_cfg_path

    # ── helpers ──────────────────────────────────────────────────────────────
    def _finalise(self, start, end, imp, exp):
        blk = engine.create_block(datetime.fromisoformat(start),
                                  datetime.fromisoformat(end), 30,
                                  seed_meters=True)
        ch = blk["meters"]["electricity_main"]["channels"]
        ch["import"]["reads"] = [{"ts": start, "value": 100.0},
                                 {"ts": end, "value": round(100.0 + imp, 6)}]
        if "export" in ch:
            ch["export"]["reads"] = [{"ts": start, "value": 50.0},
                                     {"ts": end, "value": round(50.0 + exp, 6)}]
        engine.finalise_block(_HA(), block_data=blk)

    def _late_dispatch(self, start, kwh):
        """A completed dispatch that lands AFTER the block was priced."""
        engine._store.upsert_dispatch_slot(
            start, off_peak=True, provider="MYENERGI_V2",
            source="smart-charge-completed")
        engine._store.record_dispatch_history(
            start, "completed", provider="Myenergi", source="unknown",
            energy_kwh=-abs(kwh))

    def _row(self, start, *cols):
        r = engine._store._conn.execute(
            f"SELECT {','.join(cols)} FROM blocks "
            "WHERE block_start=? AND meter_id='electricity_main'",
            (start,)).fetchone()
        return tuple(r[c] for c in cols)

    def _settle(self, start, *, imp_api=None, exp_api=None, resolver=None):
        blk = engine._store.get_block_dict_by_start(start)
        main = blk["meters"]["electricity_main"]
        if imp_api is not None:
            main["imp_kwh_api"] = imp_api
        if exp_api is not None:
            main["exp_kwh_api"] = exp_api
        engine._rerun_pass2_for_settled_block(
            blk, billing_source="api", rate_resolver=resolver,
            standing_resolver=None)
        engine.append_block_replace(blk)

    # ── the regression ───────────────────────────────────────────────────────
    def test_export_only_settlement_leaves_subfloor_import_rate(self):
        """THE prod case: sub-floor dispatch slot, export settles, import must not move."""
        self._finalise(SUB, SUB_END, imp=0.012, exp=0.818)
        self.assertAlmostEqual(self._row(SUB, "imp_rate")[0], PEAK, places=6,
                               msg="priced peak at finalise (no dispatch yet)")
        self._late_dispatch(SUB, 0.08)
        self._settle(SUB, exp_api=0.818)          # EXPORT only — imp_kwh_api stays NULL
        rate, kwh = self._row(SUB, "imp_rate", "imp_kwh")
        self.assertAlmostEqual(
            rate, PEAK, places=6,
            msg="export-only settlement must not re-resolve the import overlay/split "
                "(regression: repriced 0.323092 -> 0.054917)")
        self.assertAlmostEqual(kwh, 0.012, places=6,
                               msg="import kWh untouched — nothing settled it")

    def test_export_only_settlement_leaves_above_floor_import_rate(self):
        """The rule is not about the floor: no import settlement => no import re-price."""
        self._finalise(BIG, BIG_END, imp=4.0, exp=0.5)
        self.assertAlmostEqual(self._row(BIG, "imp_rate")[0], PEAK, places=6)
        self._late_dispatch(BIG, 3.5)
        self._settle(BIG, exp_api=0.5)
        self.assertAlmostEqual(
            self._row(BIG, "imp_rate")[0], PEAK, places=6,
            msg="an export settlement carries no information about import pricing")

    def test_export_only_settlement_still_settles_export(self):
        """The drain's actual purpose survives: export IS re-costed to its DCC figure."""
        self._finalise(SUB, SUB_END, imp=0.012, exp=0.818)
        self._settle(SUB, exp_api=0.9)
        exp_kwh, exp_cost = self._row(SUB, "exp_kwh", "exp_cost")
        self.assertAlmostEqual(exp_kwh, 0.9, places=6,
                               msg="export kWh takes the settled figure")
        self.assertAlmostEqual(exp_cost, round(0.9 * 0.12, 6), places=6,
                               msg="export cost re-derived from the settled kWh")

    def test_import_settlement_still_reprices(self):
        """Guard against over-fixing: a REAL import settlement still re-resolves."""
        self._finalise(BIG, BIG_END, imp=4.0, exp=0.5)
        self._late_dispatch(BIG, 3.5)
        self._settle(BIG, imp_api=4.0)
        self.assertAlmostEqual(
            self._row(BIG, "imp_rate")[0], OFF_PEAK, places=6,
            msg="import settled -> the overlay/split must still run (above the floor, "
                "this is the legitimate dispatch re-price)")

    def test_unsettled_import_with_no_rate_still_repairs(self):
        """`_resolve_block_rate`'s zero/missing-rate REPAIR must still reach a gap block."""
        self._finalise(SUB, SUB_END, imp=0.012, exp=0.818)
        with engine._store._conn:
            engine._store._conn.execute(
                "UPDATE blocks SET imp_rate = 0, imp_cost = 0 "
                "WHERE block_start=? AND meter_id='electricity_main'", (SUB,))
        # The live drain always passes _kraken_rate_resolver; mirror that.
        self._settle(SUB, exp_api=0.818,
                     resolver=lambda ch, bs: PEAK if ch == "import" else 0.12)
        self.assertAlmostEqual(
            self._row(SUB, "imp_rate")[0], PEAK, places=6,
            msg="a zero stored rate must still fall through to the resolver — the "
                "preserve-on-unsettled rule must not strand a gap block at £0")

    def test_cad_billing_source_still_reresolves(self):
        """Switching to CAD re-materialises every block; that must still re-resolve.

        `use_dcc` is False there, so imp_kwh_api being NULL says nothing about whether
        a re-price is wanted — the whole point of the switch is to re-derive. The new
        rule is gated on `use_dcc` precisely so it cannot swallow this path."""
        self._finalise(BIG, BIG_END, imp=4.0, exp=0.5)
        self._late_dispatch(BIG, 3.5)
        blk = engine._store.get_block_dict_by_start(BIG)
        engine._rerun_pass2_for_settled_block(
            blk, billing_source="cad", standing_resolver=None)
        engine.append_block_replace(blk)
        self.assertAlmostEqual(
            self._row(BIG, "imp_rate")[0], OFF_PEAK, places=6,
            msg="a CAD re-materialisation must still re-resolve the overlay")


class TestImportOnlyAccountUnaffected(TestExportOnlySettlementScope):
    """The rule is channel-agnostic: an account with NO export meter behaves exactly
    as before. Every inherited guard re-runs against an import-only config; the
    export-specific assertions are neutralised below."""
    HAS_EXPORT = False

    def test_export_only_settlement_still_settles_export(self):
        self.skipTest("no export channel on this account")

    def test_import_only_settlement_reprices_as_before(self):
        """Import settles on an export-less account -> the overlay/split still run."""
        self._finalise(BIG, BIG_END, imp=4.0, exp=0.0)
        self.assertAlmostEqual(self._row(BIG, "imp_rate")[0], PEAK, places=6)
        self._late_dispatch(BIG, 3.5)
        self._settle(BIG, imp_api=4.0)
        self.assertAlmostEqual(
            self._row(BIG, "imp_rate")[0], OFF_PEAK, places=6,
            msg="no export channel must not change the import settlement path")

    def test_import_only_unsettled_rerun_preserves_rate(self):
        """A re-run raised by something OTHER than settlement (billing-source switch,
        Mini import, BL-19 grid sweep) on an unsettled import block: preserve."""
        self._finalise(BIG, BIG_END, imp=4.0, exp=0.0)
        self._late_dispatch(BIG, 3.5)
        self._settle(BIG)                       # nothing settled at all
        self.assertAlmostEqual(
            self._row(BIG, "imp_rate")[0], PEAK, places=6,
            msg="no settled import figure -> keep the finalised rate")


if __name__ == "__main__":
    unittest.main()
