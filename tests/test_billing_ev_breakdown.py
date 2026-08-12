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

    def test_sums_slots_and_rounds_total(self):
        # Two slots at 10p/kWh exc: 2 kWh + 1.5 kWh → 35p exc; VAT on top.
        out = ec.octopus_bill_total([(2.0, 0.10), (1.5, 0.10)], vat_rate=0.05)
        self.assertEqual(out["exc_pence"], 35.0)
        self.assertEqual(out["inc_pence"], round(35 * 1.05))   # 36.75 → 37
        self.assertEqual(out["exc_gbp"], 0.35)

    def test_sums_raw_no_per_slot_rounding(self):
        # Corrected method (verified vs a real bill): tiny slots are summed RAW, not
        # rounded per half-hour. 0.005 kWh @ £1/kWh × 2 = 1.0p — NOT 0p.
        out = ec.octopus_bill_total([(0.005, 1.0), (0.005, 1.0)])
        self.assertEqual(out["exc_pence"], 1.0)

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


class TestBillMethodBreakdown(unittest.TestCase):
    """BL-24: the opt-in ex-VAT bill-method breakdown on the Billing summary — per-rate
    exc rows (Guy Lipman ladder), ex-VAT standing, a VAT row, the inc total. Additive:
    the Total Bill is untouched; the section only renders when present."""

    def _blk(self, kwh, cost, cost_exc=None, rate_exc=None, standing_exc=None,
             start="2026-08-01T00:00:00", rate=None):
        imp = {"kwh": kwh, "cost": cost}
        if rate is not None:
            imp["rate"] = rate
        if cost_exc is not None:
            imp["cost_exc"] = cost_exc
        if rate_exc is not None:
            imp["rate_exc"] = rate_exc
        m = {"channels": {"import": imp}}
        if standing_exc is not None:
            m["standing_charge_exc"] = standing_exc
        return {"start": start, "meters": {"electricity_main": m}}

    def test_bands_by_clean_inc_rate_not_derived_exc(self):
        # Regression: measurement slots carry a real per-slot cost_exc whose cost_exc÷kWh
        # jitters with Octopus's rounding. Banding on that derived rate shattered the two
        # real bands into 0.3075/0.3077/0.3078/… — band on the CLEAN inc rate instead.
        blocks = [
            # off-peak (inc 0.05493) — same tariff band, DIFFERENT effective exc rates
            self._blk(3.000, 0.164790, rate=0.05493, cost_exc=0.156680,   # ~0.05223/kWh
                      start="2026-07-26T02:30:00"),
            self._blk(0.001, 0.000055, rate=0.05493, cost_exc=0.000052,   # ~0.052/kWh (jitter)
                      start="2026-07-26T03:00:00"),
            # peak (inc 0.323092) — tiny slots that used to spawn 0.3075/0.3078/0.3080
            self._blk(4.000, 1.292368, rate=0.323092, cost_exc=1.230827,
                      start="2026-07-26T18:00:00"),
            self._blk(0.032, 0.010339, rate=0.323092, cost_exc=0.009847,
                      start="2026-07-26T18:30:00"),
            self._blk(0.104, 0.033602, rate=0.323092, cost_exc=0.032002,
                      start="2026-07-26T19:00:00"),
        ]
        bm = ec._bill_method_breakdown(blocks)
        self.assertEqual(len(bm["rows"]), 2)                          # two clean bands, not six
        kwhs = sorted(r["kwh"] for r in bm["rows"])
        self.assertAlmostEqual(kwhs[0], 3.001, places=3)              # off-peak 3.0+0.001
        self.assertAlmostEqual(kwhs[1], 4.136, places=3)              # peak 4.0+0.032+0.104

    def test_bill_slots_prefers_stored_rate_exc(self):
        slots = ec.bill_slots_from_blocks([self._blk(2.0, 0.42, rate_exc=0.20)])
        self.assertAlmostEqual(slots[0][1], 0.20, places=6)        # stored ex rate wins

    def test_breakdown_bands_standing_vat_and_total(self):
        # band 0.20: 2 kWh → 40.00p; band 0.60: 1 kWh → 60.00p; energy £1.00.
        # standing 1 day @ £0.40; subtotal £1.40; VAT 5% → £0.07; inc £1.47.
        blocks = [self._blk(2.0, 0.42, rate_exc=0.20, standing_exc=0.40,
                            start="2026-08-01T00:00:00"),
                  self._blk(1.0, 0.63, rate_exc=0.60, start="2026-08-01T18:00:00")]
        bm = ec._bill_method_breakdown(blocks)
        self.assertEqual(len(bm["rows"]), 2)
        self.assertAlmostEqual(bm["energy_exc"], 1.00, places=2)
        self.assertEqual(bm["standing_days"], 1)
        self.assertAlmostEqual(bm["standing_exc"], 0.40, places=2)
        self.assertAlmostEqual(bm["subtotal_exc"], 1.40, places=2)
        self.assertAlmostEqual(bm["vat_amount"], 0.07, places=2)
        self.assertAlmostEqual(bm["inc_total"], 1.47, places=2)
        # bands sum to the energy total (round-per-band-then-sum).
        self.assertAlmostEqual(sum(r["cost_exc"] for r in bm["rows"]), bm["energy_exc"], places=2)

    def test_vat_derived_from_pair(self):
        bm = ec._bill_method_breakdown([self._blk(2.0, 0.42, cost_exc=0.40, rate_exc=0.20)])
        self.assertAlmostEqual(bm["vat_rate"], 0.05, places=3)     # 0.42/0.40 − 1

    def test_partial_coverage(self):
        blocks = [self._blk(2.0, 0.42, rate_exc=0.20), self._blk(2.0, 0.42)]  # 2nd inc-only
        self.assertAlmostEqual(ec._bill_method_breakdown(blocks)["coverage"], 0.5, places=3)

    def test_none_when_no_import(self):
        self.assertIsNone(ec._bill_method_breakdown([]))

    def test_render_section_only_when_present(self):
        s = _summary()
        # Default off: inc-VAT import display, no ex-VAT bill method.
        default_html = ec.render_billing_summary(s)
        self.assertNotIn("Total incl. VAT", default_html)
        self.assertIn("Total incl. standing charge", default_html)
        s["bill_method"] = {
            "rows": [{"rate_exc": 0.20, "kwh": 2.0, "cost_exc": 0.40}],
            "energy_exc": 0.40, "standing_days": 1, "standing_rate_exc": 0.40,
            "standing_exc": 0.40, "subtotal_exc": 0.80, "vat_rate": 0.05,
            "vat_amount": 0.04, "inc_total": 0.84, "coverage": 1.0}
        html = ec.render_billing_summary(s)
        # ex-VAT bill method REPLACES the inc-VAT import display.
        self.assertIn("Standing charge (exc)", html)
        self.assertIn("VAT @ 5%", html)
        self.assertIn("Total incl. VAT", html)
        self.assertNotIn("Total incl. standing charge", html)      # inc display replaced
        self.assertNotIn("Bill-style rounding", html)              # header dropped
        self.assertNotIn("penny-perfect", html)                    # footnote dropped
        self.assertIn("Total Bill", html)                          # existing total untouched


class TestDayChartPlungePrice(unittest.TestCase):
    """Regression (reported after 4.1.3): build_day_chart_html must keep an Agile
    plunge-price CREDIT (negative import cost) in the per-day chart + sidebar, not clamp
    it to 0 — the bug where a −£1.09 day's sidebar read +£0.22 (positive slots only). The
    4.1.3 negative-cost fix reached the aggregation/billing-summary paths but not this one."""

    def _blk(self, kwh, cost, rate):
        return {"meters": {"electricity_main": {"meta": {}, "standing_charge": 0.5,
                "channels": {"import": {"kwh": kwh, "cost": cost, "rate": rate}}}}}

    def _import_cost(self, day_blocks):
        import re
        html = ec.build_day_chart_html("2026-07-26", day_blocks,
                                       {"electricity_main": "#1f77b4"},
                                       block_minutes=30, currency="£")
        m = re.search(r"Import cost: £([-0-9.]+)", html)
        return float(m.group(1)) if m else None

    def test_negative_day_credit_survives(self):
        # negative slots −1.31 + positive 0.22 → signed −1.09, NOT +0.22.
        db = [(0, self._blk(5.0, -1.31, -0.262)), (36, self._blk(1.0, 0.22, 0.22))]
        self.assertAlmostEqual(self._import_cost(db), -1.09, places=2)

    def test_positive_day_unchanged(self):
        # Guard: a normal positive day is byte-identical (the fix is a no-op there).
        db = [(0, self._blk(2.0, 0.60, 0.30)), (36, self._blk(1.0, 0.30, 0.30))]
        self.assertAlmostEqual(self._import_cost(db), 0.90, places=2)


class TestDayChartExcTable(unittest.TestCase):
    """BL-24 opt-in: the per-slot data table gains ex-VAT import columns when Bill
    Rounding is on. Default off ⇒ no exc data in the JSON (byte-identical table)."""

    def _blk(self, kwh, cost, rate, rate_exc=None, cost_exc=None):
        ch = {"kwh": kwh, "cost": cost, "rate": rate}
        if rate_exc is not None:
            ch["rate_exc"] = rate_exc
        if cost_exc is not None:
            ch["cost_exc"] = cost_exc
        return {"meters": {"electricity_main": {"meta": {}, "standing_charge": 0.5,
                "channels": {"import": ch}}}}

    def _html(self, day_blocks, bill_rounding):
        return ec.build_day_chart_html("2026-07-26", day_blocks,
                                       {"electricity_main": "#1f77b4"},
                                       block_minutes=30, currency="£",
                                       bill_rounding=bill_rounding)

    def test_default_off_no_exc_payload(self):
        db = [(0, self._blk(2.0, 0.42, 0.21, rate_exc=0.19))]
        html = self._html(db, bill_rounding=False)
        self.assertNotIn('"bill_rounding"', html)       # no new JSON keys
        self.assertNotIn('"exc_ratio"', html)
        self.assertNotIn('"exc_approx"', html)

    def test_on_emits_ratio_from_stored_exc(self):
        db = [(0, self._blk(2.0, 0.42, 0.21, rate_exc=0.19))]   # ratio 0.19/0.21
        html = self._html(db, bill_rounding=True)
        self.assertIn('"bill_rounding":true', html)
        self.assertIn('"exc_ratio":[0.9047619', html)           # 0.19/0.21, not inc/1.05
        self.assertIn('"exc_approx":[false', html)

    def test_on_flags_fallback_when_no_exc(self):
        db = [(0, self._blk(2.0, 0.42, 0.21))]                  # no rate_exc/cost_exc
        html = self._html(db, bill_rounding=True)
        self.assertIn('"exc_ratio":[0.95238095', html)          # inc ÷ 1.05 fallback (5% default)
        self.assertIn('"exc_approx":[true', html)

    def test_fallback_uses_period_vat_not_hardcoded(self):
        # A 0% VAT period: the fallback must be inc ÷ 1.0 (exc == inc), NOT inc ÷ 1.05.
        db = [(0, self._blk(2.0, 0.42, 0.21))]                  # uncaptured → fallback
        html0 = ec.build_day_chart_html("2026-11-15", db, {"electricity_main": "#1f77b4"},
                                        block_minutes=30, currency="£",
                                        bill_rounding=True, fallback_vat=0.0)
        self.assertIn('"exc_ratio":[1.0', html0)
        self.assertIn('"fallback_invat":1.0', html0)
        # a 20% period scales by 1/1.2
        html20 = ec.build_day_chart_html("2026-11-15", db, {"electricity_main": "#1f77b4"},
                                         block_minutes=30, currency="£",
                                         bill_rounding=True, fallback_vat=0.20)
        self.assertIn('"fallback_invat":0.83333333', html20)


if __name__ == "__main__":
    unittest.main()
