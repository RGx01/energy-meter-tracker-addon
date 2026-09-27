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

    def test_import_kwh_settlement_holds_band_and_recosts(self):
        """#475: a settled kWh re-costs and re-splits, but must NOT re-band.

        Supersedes `test_import_settlement_still_reprices` (4.5.13/BL-62 model), which
        asserted that a settled import kWh re-resolved the overlay to off-peak. That is
        the defect: Octopus has not priced this slot yet — only its QUANTITY is known —
        so the predicted band must stand until a settled COST arrives. Quantity is a
        measurement and follows; price is a supplier decision and is held.
        """
        self._finalise(BIG, BIG_END, imp=4.0, exp=0.5)
        self.assertAlmostEqual(self._row(BIG, "imp_rate")[0], PEAK, places=6,
                               msg="priced peak at finalise (no dispatch yet)")
        self._late_dispatch(BIG, 3.5)
        self._settle(BIG, imp_api=4.5)
        rate, kwh, cost = self._row(BIG, "imp_rate", "imp_kwh", "imp_cost")
        self.assertAlmostEqual(
            rate, PEAK, places=6,
            msg="settled kWh must not move the band — no supplier price has arrived")
        self.assertAlmostEqual(
            kwh, 4.5, places=6,
            msg="the settled QUANTITY is authoritative and is taken")
        self.assertAlmostEqual(
            cost, round(4.5 * PEAK, 6), places=6,
            msg="cost re-derived from the settled kWh at the HELD band rate")

    # ── #475: the settled-kWh band hold, in detail ───────────────────────────
    def _segments(self, start, meter="electricity_main"):
        return engine._store._conn.execute(
            "SELECT seq, kwh, inc_rate, exc_rate, band, attribution "
            "FROM block_segments WHERE block_start=? AND meter_id=? AND channel='import' "
            "ORDER BY seq", (start, meter)).fetchall()

    def test_settled_kwh_holds_exc_vat_rate_too(self):
        """The hold applies to the ex-VAT rate as well as the inc-VAT one.

        `imp_rate_exc` is a second pricing column written alongside `imp_rate`. A hold that
        pinned only the inc rate would leave the exc figure re-derived from the moved band,
        so the ex-VAT bill would disagree with the inc-VAT bill on the same block.

        This harness's finalise path leaves `imp_rate_exc` NULL (no VAT calendar is
        seeded), and asserting "still NULL" would pass under either model — a vacuous
        test. So seed the exc rate explicitly to the peak band's ex-VAT value first;
        the assertion then discriminates.
        """
        self._finalise(BIG, BIG_END, imp=4.0, exp=0.5)
        peak_exc = round(PEAK / 1.05, 6)
        with engine._store._conn:
            engine._store._conn.execute(
                "UPDATE blocks SET imp_rate_exc = ?, imp_cost_exc = ? "
                "WHERE block_start=? AND meter_id='electricity_main'",
                (peak_exc, round(4.0 * peak_exc, 6), BIG))
        self._late_dispatch(BIG, 3.5)
        self._settle(BIG, imp_api=4.5)
        rate_inc, rate_exc = self._row(BIG, "imp_rate", "imp_rate_exc")
        self.assertAlmostEqual(
            rate_exc, peak_exc, places=6,
            msg="the ex-VAT rate must be held on the peak band")
        # The discriminating assertion: inc and exc must describe the SAME band. The
        # pre-fix code moved imp_rate to off-peak and left imp_rate_exc on the peak
        # value, so the two columns disagreed about what this block cost — checking
        # only that exc was "unchanged" would pass for that wrong reason.
        self.assertAlmostEqual(
            rate_exc, round(rate_inc / 1.05, 6), places=6,
            msg="inc and exc rates must describe the same band (pre-fix: imp_rate "
                "re-banded to off-peak while imp_rate_exc stayed on peak)")

    def test_settled_kwh_segments_agree_with_block_cost(self):
        """Segments are the seam the hold has to reach, not just the columns.

        NOTE: this is an INVARIANT guard, not a discriminator for the original defect —
        it holds under the old model too (there, block and segments moved to off-peak
        together). It exists because the FIX broke it twice during development.

        `_persist_block_segments` prefers `imp_ch["segments"]` over rebuilding from the
        block's columns, so pinning `imp_rate` alone left off-peak segments behind: the
        block billed peak while its own breakdown billed off-peak, and the two
        disagreed on the same row. Assert the invariant directly — segment costs sum to
        the block cost, and no segment carries a rate the block does not.
        """
        self._finalise(BIG, BIG_END, imp=4.0, exp=0.5)
        self._late_dispatch(BIG, 3.5)
        self._settle(BIG, imp_api=4.5)
        rate, cost = self._row(BIG, "imp_rate", "imp_cost")
        segs = self._segments(BIG)
        if not segs:
            self.skipTest("no segments persisted for this block shape")
        seg_cost = round(sum(r["kwh"] * r["inc_rate"] for r in segs), 6)
        self.assertAlmostEqual(
            seg_cost, cost, places=5,
            msg="segment costs must sum to the block cost (regression: the seam kept "
                "off-peak segments after the block was held at peak)")
        for r in segs:
            self.assertAlmostEqual(
                r["inc_rate"], rate, places=6,
                msg="every segment must carry the held band rate, not a re-resolved one")

    def test_settled_kwh_twice_is_stable(self):
        """Idempotence: a second settlement of the same figure must not drift the band.

        Also an INVARIANT guard rather than a discriminator — it passes under the old
        model as well. Kept because a hold that decayed on re-drain would be silent.

        The poll re-upserts a rolling window every cycle, so a held block is re-drained
        repeatedly. A hold that only survived the first pass would decay silently.
        """
        self._finalise(BIG, BIG_END, imp=4.0, exp=0.5)
        self._late_dispatch(BIG, 3.5)
        self._settle(BIG, imp_api=4.5)
        first = self._row(BIG, "imp_rate", "imp_kwh", "imp_cost")
        self._settle(BIG, imp_api=4.5)
        second = self._row(BIG, "imp_rate", "imp_kwh", "imp_cost")
        for a, b, name in zip(first, second, ("imp_rate", "imp_kwh", "imp_cost")):
            self.assertAlmostEqual(a, b, places=6,
                                   msg=f"{name} drifted on a repeat settlement")

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

    def test_import_only_kwh_settlement_holds_band(self):
        """#475 on an export-less account: the hold is channel-config-agnostic."""
        self._finalise(BIG, BIG_END, imp=4.0, exp=0.0)
        self.assertAlmostEqual(self._row(BIG, "imp_rate")[0], PEAK, places=6)
        self._late_dispatch(BIG, 3.5)
        self._settle(BIG, imp_api=4.5)
        rate, kwh, cost = self._row(BIG, "imp_rate", "imp_kwh", "imp_cost")
        self.assertAlmostEqual(
            rate, PEAK, places=6,
            msg="no export channel must not change the settled-kWh band hold")
        self.assertAlmostEqual(kwh, 4.5, places=6)
        self.assertAlmostEqual(cost, round(4.5 * PEAK, 6), places=6)

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
