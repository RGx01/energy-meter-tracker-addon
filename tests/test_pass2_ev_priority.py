"""
Issue #212 — EV grid-attribution priority.

When an EV and a battery both charge in the same half-hour and grid import is less
than their combined draw (solar covering the rest), the EV must claim grid FIRST —
it's the intended grid load, especially in an IOG/smart-charge dispatch slot — and
the battery clips to the remainder. The old "biggest draw first" order handed the
whole grid pool to a simultaneously-charging battery, so the car's grid charge was
relabelled battery-sourced and vanished from the grid view.
"""
import sys
import os
import types
import unittest
from unittest.mock import MagicMock

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

from block_store import BlockStore, migrate_json_to_sqlite
_boot_store = BlockStore(":memory:")
_boot_store.insert_config_period({"meters": {"electricity_main": {"meta": {
    "timezone": "UTC", "billing_day": 1, "block_minutes": 30,
    "currency_symbol": "£", "currency_code": "GBP"}}}})
bs = types.ModuleType("block_store")
bs.BlockStore = BlockStore
bs.open_block_store = lambda path: _boot_store
bs.migrate_json_to_sqlite = migrate_json_to_sqlite
sys.modules["block_store"] = bs

sys.path.insert(0, os.path.dirname(__file__))
import engine


def _make_block(main_kwh, ev_kwh, batt_kwh):
    """A slot with the main meter, an EV (meter_type 'ev') and a battery."""
    def _sub(kwh, mtype):
        return {
            "meta": {"sub_meter": True, "meter_type": mtype,
                     "parent_meter": "electricity_main"},
            "channels": {"import": {"kwh": kwh, "rate": 0.08,
                                    "cost": round(kwh * 0.08, 6)}},
            "standing_charge": 0.0,
        }
    return {
        "start": "2026-06-21T14:30:00", "end": "2026-06-21T15:00:00",
        "interpolated": False,
        "meters": {
            "electricity_main": {
                "meta": {"sub_meter": False},
                "channels": {
                    "import": {"kwh": main_kwh, "kwh_total": main_kwh,
                               "rate": 0.08, "cost": round(main_kwh * 0.08, 6)},
                    "export": {"kwh": 0.0, "rate": 0.12, "cost": 0.0},
                },
                "standing_charge": 0.0,
            },
            "house_battery": _sub(batt_kwh, "battery"),
            "ev_charger":    _sub(ev_kwh, "ev"),
        },
    }


class TestPass2EvGridPriority(unittest.TestCase):
    def _imp(self, block, meter):
        return block["meters"][meter]["channels"]["import"]

    def test_ev_claims_grid_before_larger_battery(self):
        # PianSom's case: battery draw (5.566) EXCEEDS EV draw (4.0). Under the old
        # biggest-first rule the battery claimed all 5.566 grid and the EV got 0.
        # EV-first must flip it: EV takes its 4.0, battery clips to the 1.566 left.
        block = _make_block(main_kwh=5.566, ev_kwh=4.0, batt_kwh=5.566)
        engine._apply_pass2(block)
        ev   = self._imp(block, "ev_charger")
        batt = self._imp(block, "house_battery")
        self.assertAlmostEqual(ev["kwh_grid"], 4.0, places=3,
                               msg="EV must claim grid first")
        self.assertAlmostEqual(batt["kwh_grid"], 1.566, places=3,
                               msg="battery clips to the grid the EV left")
        # The battery's non-grid charge is its own solar self-supply, not the EV's.
        self.assertAlmostEqual(batt["kwh"] - batt["kwh_grid"], 4.0, places=3)

    def test_ev_fully_grid_when_grid_covers_it(self):
        # Dispatch slot where grid comfortably covers the car: EV fully on grid,
        # battery takes the remainder.
        block = _make_block(main_kwh=6.0, ev_kwh=4.0, batt_kwh=3.0)
        engine._apply_pass2(block)
        self.assertAlmostEqual(self._imp(block, "ev_charger")["kwh_grid"], 4.0, places=3)
        self.assertAlmostEqual(self._imp(block, "house_battery")["kwh_grid"], 2.0, places=3)

    def test_grid_conservation(self):
        # The split never invents or loses grid: sub-meter grid shares sum to the
        # parent grid import (no remainder left for the main in this all-sub slot).
        block = _make_block(main_kwh=5.566, ev_kwh=4.0, batt_kwh=5.566)
        engine._apply_pass2(block)
        total_sub_grid = (self._imp(block, "ev_charger")["kwh_grid"]
                          + self._imp(block, "house_battery")["kwh_grid"])
        self.assertAlmostEqual(total_sub_grid, 5.566, places=3)


def _make_block_x(main_kwh, ev_kwh, batt_kwh=0.0, *, interpolated=False,
                  settled_api=None):
    """Like _make_block but with control over the `interpolated` flag and whether
    the main meter is DCC-settled (imp_kwh_api present)."""
    b = _make_block(main_kwh, ev_kwh, batt_kwh)
    b["interpolated"] = interpolated
    if settled_api is not None:
        b["meters"]["electricity_main"]["imp_kwh_api"] = settled_api
    return b


class TestPass2GridInvariantBL19(unittest.TestCase):
    """BL-19 — a sub-meter's grid share must never exceed the parent's grid
    import once that import is authoritative (live-metered or DCC-settled),
    even on a gap-fill (interpolated) block."""

    def _imp(self, block, meter):
        return block["meters"][meter]["channels"]["import"]

    def test_settled_interpolated_block_clamps_ev_grid(self):
        # prod-dev 10-Jul 16:30: gap-fill left EV=2.053 kWh, but the main DCC-
        # settled to 0.157 kWh. The `interpolated` flag is set, yet the settled
        # main is the true grid boundary — so the EV grid MUST clamp to 0.157,
        # the rest being behind-the-meter solar.
        block = _make_block_x(0.157, 2.053, 0.0, interpolated=True, settled_api=0.157)
        engine._apply_pass2(block)
        ev = self._imp(block, "ev_charger")
        self.assertAlmostEqual(ev["kwh_grid"], 0.157, places=3,
                               msg="EV grid must clamp to the settled main import")
        self.assertAlmostEqual(ev["kwh_battery"], 2.053 - 0.157, places=3,
                               msg="overflow spills to self-consumption")

    def test_unsettled_gap_block_preserved_as_is(self):
        # Same gap-fill EV=2.053 but the main has NOT settled (imp_kwh_api None):
        # the main is itself an estimate that may under-shoot, so we preserve the
        # attribution as-is (settlement re-runs PASS 2 and re-clamps later). This
        # guards against under-billing a real overnight grid draw.
        block = _make_block_x(0.157, 2.053, 0.0, interpolated=True, settled_api=None)
        engine._apply_pass2(block)
        ev = self._imp(block, "ev_charger")
        self.assertAlmostEqual(ev["kwh_grid"], 2.053, places=3,
                               msg="unsettled gap block keeps as-is attribution")
        self.assertAlmostEqual(ev["kwh_battery"], 0.0, places=3)

    def test_live_overflow_block_always_clamps(self):
        # A live (non-interpolated) block always clamps — unchanged behaviour,
        # asserted explicitly so the invariant can't regress.
        block = _make_block_x(0.157, 2.053, 0.0, interpolated=False, settled_api=None)
        engine._apply_pass2(block)
        self.assertAlmostEqual(self._imp(block, "ev_charger")["kwh_grid"], 0.157,
                               places=3)

    def test_settled_interpolated_no_violation_unchanged(self):
        # Settled interpolated block where EV < main grid (the 16:00/17:00 blocks
        # in the same prod-dev run): no violation, EV keeps its full draw on grid.
        block = _make_block_x(3.979, 2.052, 0.0, interpolated=True, settled_api=3.979)
        engine._apply_pass2(block)
        self.assertAlmostEqual(self._imp(block, "ev_charger")["kwh_grid"], 2.052,
                               places=3)


if __name__ == "__main__":
    unittest.main()