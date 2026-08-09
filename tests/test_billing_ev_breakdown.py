"""
BL-22 on the Billing tab: the derived-EV breakdown split.

Covers the two pure helpers in energy_charts.py that give the Billing tab an
"EV (from dispatch)" line (summary tables) + per-day trace WITHOUT moving a bill:
  - _dispatch_ev_slot_map        — per-slot EV, grid-clipped + cost-apportioned, gated
  - _inject_ev_breakdown_into_summary — splits the Direct-import remainder; the bill
    anchors (main_import_raw / standing / total_cost) stay byte-identical.

Validated end-to-end on a pure-API dev DB: July bill anchors byte-identical, EV
breakdown 406.795 kWh reconciling with the Direct-import remainder.
"""

import sys
import os
import types
import copy
import unittest
import sqlite3

# Stub energy_engine_io so imports work outside HA
eio = types.ModuleType("energy_engine_io")
eio.load_json = lambda *a, **kw: {}
sys.modules["energy_engine_io"] = eio

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import energy_charts as ec


def _summary():
    """Minimal no-sub-meter billing summary: one 'Home / Import' remainder with two
    rate tiers, plus the untouchable bill anchors."""
    return {
        "meters": {"Home / Import": {
            0.05: {"kwh": 8.0, "cost": 0.44, "read_start": None, "read_end": None},
            0.30: {"kwh": 2.0, "cost": 0.60, "read_start": None, "read_end": None},
        }},
        "totals": {"Home / Import": {"kwh": 10.0, "cost": 1.04, "is_submeter": False,
                                     "read_start": None, "read_end": None}},
        "main_import_raw": {0.05: {"kwh": 8.0, "cost": 0.44},
                            0.30: {"kwh": 2.0, "cost": 0.60}},
        "standing": {"2026-08-04": 0.50},
        "total_cost": 1.54,
        "meter_meta": {"Home / Import": {"is_submeter": False}},
    }


class TestInjectEvBreakdown(unittest.TestCase):

    def test_split_keeps_bill_anchors_byte_identical(self):
        s = _summary()
        anchors = {"main_import_raw": copy.deepcopy(s["main_import_raw"]),
                   "standing": copy.deepcopy(s["standing"]),
                   "total_cost": s["total_cost"]}
        ev_map = {"2026-08-04T00:00:00": {"kwh": 4.0, "cost": 0.22, "rate": 0.05}}
        applied = ec._inject_ev_breakdown_into_summary(
            s, [{"start": "2026-08-04T00:00:00"}], ev_map)
        self.assertTrue(applied)
        # Bill anchors untouched — the bill cannot move.
        self.assertEqual(s["main_import_raw"], anchors["main_import_raw"])
        self.assertEqual(s["standing"], anchors["standing"])
        self.assertEqual(s["total_cost"], anchors["total_cost"])

    def test_split_reconciles_to_original_direct(self):
        s = _summary()
        ev_map = {"2026-08-04T00:00:00": {"kwh": 4.0, "cost": 0.22, "rate": 0.05}}
        ec._inject_ev_breakdown_into_summary(s, [{"start": "2026-08-04T00:00:00"}], ev_map)
        ev = s["totals"]["EV (from dispatch) / Import"]
        self.assertTrue(ev["is_submeter"])
        self.assertAlmostEqual(ev["kwh"], 4.0, places=3)
        self.assertAlmostEqual(ev["cost"], 0.22, places=2)
        # Direct remainder reduced; Direct + EV == original 10.0 kWh / 1.04 cost.
        rem = s["totals"]["Home / Import"]
        self.assertAlmostEqual(rem["kwh"] + ev["kwh"], 10.0, places=3)
        self.assertAlmostEqual(rem["cost"] + ev["cost"], 1.04, places=2)
        # Per-rate: the 0.05 tier lost exactly the EV; the 0.30 tier is untouched.
        self.assertAlmostEqual(s["meters"]["Home / Import"][0.05]["kwh"], 4.0, places=3)
        self.assertAlmostEqual(s["meters"]["Home / Import"][0.30]["kwh"], 2.0, places=3)

    def test_noop_when_submeter_present(self):
        s = _summary()
        s["totals"]["Zappi / Import"] = {"kwh": 1.0, "cost": 0.05, "is_submeter": True}
        before = copy.deepcopy(s)
        applied = ec._inject_ev_breakdown_into_summary(
            s, [{"start": "2026-08-04T00:00:00"}],
            {"2026-08-04T00:00:00": {"kwh": 4.0, "cost": 0.22, "rate": 0.05}})
        self.assertFalse(applied)
        self.assertEqual(s, before)

    def test_noop_without_ev(self):
        s = _summary()
        before = copy.deepcopy(s)
        self.assertFalse(ec._inject_ev_breakdown_into_summary(s, [{"start": "x"}], {}))
        self.assertEqual(s, before)


class TestDispatchEvSlotMap(unittest.TestCase):

    def _store(self, dispatches):
        conn = sqlite3.connect(":memory:"); conn.row_factory = sqlite3.Row
        conn.execute("CREATE TABLE dispatch_history (slot_start TEXT, kind TEXT, energy_kwh REAL)")
        conn.executemany("INSERT INTO dispatch_history VALUES (?,?,?)", dispatches)
        conn.commit()

        class FakeStore:
            def __init__(s, c): s._conn = c
        return FakeStore(conn)

    def _blk(self, slot, kwh, cost, rate):
        return {"start": slot, "meters": {"electricity_main": {"channels": {
            "import": {"kwh_total": kwh, "cost": cost, "rate": rate}}}}}

    def test_clips_and_apportions(self):
        store = self._store([("2026-08-04T00:00:00", "completed", -4.0),
                             ("2026-08-04T00:30:00", "completed", -6.0)])
        blocks = [self._blk("2026-08-04T00:00:00", 5.0, 0.35, 0.07),
                  self._blk("2026-08-04T00:30:00", 4.0, 0.28, 0.07)]
        m = ec._dispatch_ev_slot_map(store, blocks, {"meters": {"electricity_main": {"meta": {}}}})
        self.assertAlmostEqual(m["2026-08-04T00:00:00"]["kwh"], 4.0, places=3)   # 4 < 5
        self.assertAlmostEqual(m["2026-08-04T00:00:00"]["cost"], 0.28, places=4)  # 0.35*4/5
        self.assertAlmostEqual(m["2026-08-04T00:30:00"]["kwh"], 4.0, places=3)   # 6 clipped to 4
        self.assertAlmostEqual(m["2026-08-04T00:30:00"]["cost"], 0.28, places=4)

    def test_gated_off_when_submeter_configured(self):
        store = self._store([("2026-08-04T00:00:00", "completed", -4.0)])
        blocks = [self._blk("2026-08-04T00:00:00", 5.0, 0.35, 0.07)]
        cfg = {"meters": {"electricity_main": {"meta": {}},
                          "ev": {"meta": {"sub_meter": True, "meter_type": "ev_charger"}}}}
        self.assertEqual(ec._dispatch_ev_slot_map(store, blocks, cfg), {})


class TestBankersRoundingLadder(unittest.TestCase):
    """BL-24 groundwork: the pure Octopus-style rounding ladder. Wired to nothing —
    proves the mechanics only."""

    def test_bankers_round_half_to_even(self):
        # Exact decimal halves round to the nearest EVEN — the float built-in can't
        # (0.015 is 0.01499… in binary); Decimal(str(x)) fixes that.
        self.assertEqual(ec.bankers_round(0.015, 2), 0.02)   # 2 is even
        self.assertEqual(ec.bankers_round(0.025, 2), 0.02)   # 2 is even
        self.assertEqual(ec.bankers_round(0.035, 2), 0.04)   # 4 is even
        self.assertEqual(ec.bankers_round(0.005, 2), 0.00)   # 0 is even
        self.assertEqual(ec.bankers_round(2.5, 0), 2.0)
        self.assertEqual(ec.bankers_round(3.5, 0), 4.0)

    def test_ladder_rounds_per_slot_then_totals(self):
        # Two slots at 10p/kWh exc: 2 kWh → 20p, 1.5 kWh → 15p; exc 35p.
        out = ec.octopus_bill_total([(2.0, 0.10), (1.5, 0.10)], vat_rate=0.05)
        self.assertEqual(out["exc_pence"], 35.0)
        self.assertEqual(out["inc_pence"], round(35 * 1.05))   # 36.75 → 37 half-even? 36.75→37
        self.assertEqual(out["exc_gbp"], 0.35)

    def test_ladder_per_slot_rounding_differs_from_raw_sum(self):
        # 0.005 kWh @ £1/kWh over two slots: each slot 0.5p → rounds to 0p (half-even),
        # total 0p — vs a naive raw sum of 1p. Proves per-slot half-even is applied.
        out = ec.octopus_bill_total([(0.005, 1.0), (0.005, 1.0)])
        self.assertEqual(out["exc_pence"], 0.0)

    def test_bill_slots_prefers_stored_exc_else_falls_back(self):
        blocks = [
            {"meters": {"electricity_main": {"channels": {"import": {
                "kwh": 2.0, "rate": 0.2100, "cost_exc": 0.40}}}}},   # exc stored → 0.20/kWh
            {"meters": {"electricity_main": {"channels": {"import": {
                "kwh": 1.0, "rate": 0.2100}}}}},                     # no exc → 0.21/1.05 = 0.20
        ]
        slots = ec.bill_slots_from_blocks(blocks, vat_rate=0.05)
        self.assertAlmostEqual(slots[0][1], 0.20, places=6)   # from stored exc
        self.assertAlmostEqual(slots[1][1], 0.20, places=6)   # from inc ÷ 1.05


if __name__ == "__main__":
    unittest.main()
