"""
test_insights_rate_tiers.py
===========================
Issue #371: Usage Insights' rate-period breakdown must behave like the Billing view.

Two defects, both fixed here:
  1. Agile plunge-price slots (NEGATIVE import rate — a credit) were dropped from
     the distribution by a `rate > 0` gate. They must be kept (`rate != 0`), sign
     intact, so a plunge period reads as a negative-rate tier / credit.
  2. Agile's ~48 rates/day produced a wall of bars. Above _MAX_RATE_ROWS the tiers
     must collapse to ONE cost-weighted-average row — the same fold the Billing
     summary applies (energy_charts._bill_rate_rows) — flagged for the renderer.

`_collapse_rate_tiers` is unit-tested directly; the gate fix is exercised end-to-end
through `_aggregate_usage` on an in-memory store.
"""

import os
import sys
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "web"))

import server                      # noqa: E402
from block_store import BlockStore  # noqa: E402
import energy_charts as _ec         # noqa: E402


class TestCollapseRateTiers(unittest.TestCase):

    def test_passthrough_below_threshold(self):
        tiers = [{"rate": 0.10, "kwh": 1.0, "cost": 0.10, "blocks": 1},
                 {"rate": 0.30, "kwh": 2.0, "cost": 0.60, "blocks": 2}]
        self.assertEqual(server._collapse_rate_tiers(tiers), tiers)

    def test_collapse_above_threshold_weighted_avg(self):
        tiers = [{"rate": r / 100.0, "kwh": 1.0, "cost": r / 100.0, "blocks": 1}
                 for r in (-3, 5, 10, 15, 20, 30)]           # 6 > _MAX_RATE_ROWS
        self.assertGreater(len(tiers), _ec._MAX_RATE_ROWS)
        out = server._collapse_rate_tiers(tiers)
        self.assertEqual(len(out), 1)
        self.assertTrue(out[0]["collapsed"])
        self.assertEqual(out[0]["n_rates"], 6)
        # cost-weighted average == Σcost / Σkwh
        self.assertAlmostEqual(out[0]["rate"], out[0]["cost"] / out[0]["kwh"], places=6)

    def test_all_plunge_collapses_negative(self):
        tiers = [{"rate": -r / 100.0, "kwh": 1.0, "cost": -r / 100.0, "blocks": 1}
                 for r in (1, 2, 3, 4, 5, 6)]
        out = server._collapse_rate_tiers(tiers)
        self.assertLess(out[0]["rate"], 0)      # sign preserved (a credit)
        self.assertLess(out[0]["cost"], 0)


class TestAggregateInsightsRateTiers(unittest.TestCase):

    def _store(self, tz="UTC"):
        st = BlockStore(":memory:")
        with st._conn:
            cp = st._conn.execute(
                "INSERT INTO config_periods (effective_from, billing_day, "
                "block_minutes, timezone, currency_symbol, currency_code) "
                "VALUES ('2024-01-01T00:00:00',1,30,?,'£','GBP')", (tz,)).lastrowid
            st._conn.execute("INSERT INTO meters (config_period_id, meter_id, "
                             "is_sub_meter, meter_type) VALUES (?, 'electricity_main', 0, '')",
                             (cp,))
        self._cp = cp
        st._conn.commit()
        return st

    def _blk(self, st, start, imp_kwh, rate, cost):
        st._conn.execute(
            "INSERT INTO blocks (block_start, block_end, meter_id, config_period_id, "
            "imp_kwh, imp_rate, imp_cost, source) "
            "VALUES (?,?, 'electricity_main', ?, ?, ?, ?, 'imported_api')",
            (start, start, self._cp, imp_kwh, rate, cost))

    def _agg(self, st):
        cfg = {"meters": {"electricity_main": {}}}
        return server._aggregate_usage(
            st, cfg, "2025-01-01T00:00:00", "2025-01-02T00:00:00", "UTC")

    def test_plunge_slot_kept_in_distribution(self):
        st = self._store()
        self._blk(st, "2025-01-01T02:00:00", 1.0,  0.30,  0.30)   # peak
        self._blk(st, "2025-01-01T03:00:00", 2.0,  0.07,  0.14)   # off-peak
        self._blk(st, "2025-01-01T13:00:00", 1.5, -0.05, -0.075)  # PLUNGE (credit)
        st._conn.commit()
        d = self._agg(st)
        rates = [t["rate"] for t in d["rate_tiers"]]
        self.assertIn(-0.05, rates)                       # negative tier survived
        # main import cost carries the credit (0.30 + 0.14 - 0.075)
        self.assertAlmostEqual(d["imp_cost"], 0.365, places=3)

    def test_many_rates_collapse(self):
        st = self._store()
        for i, r in enumerate([-0.03, 0.05, 0.09, 0.14, 0.22, 0.35, 0.41]):
            self._blk(st, f"2025-01-01T{i:02d}:00:00", 1.0, r, r)
        st._conn.commit()
        d = self._agg(st)
        self.assertEqual(len(d["rate_tiers"]), 1)
        self.assertTrue(d["rate_tiers"][0]["collapsed"])
        self.assertEqual(d["rate_tiers"][0]["n_rates"], 7)

    def test_capped_boundary_rate_tiers_from_segments(self):
        # P4.2: with segments, a cap-BOUNDARY block splits into its two REAL bands rather than
        # contributing a unique blended rate that shatters/collapses the Usage-Stats breakdown.
        import pricing_segments as ps
        st = self._store()
        OFF, PEAK = 0.05493, 0.323092
        def seg1(start, kwh, rate):
            self._blk(st, start, kwh, rate, round(kwh * rate, 6))
            st.set_block_segments(start, "electricity_main",
                [ps.Segment(kwh, rate, None, "day" if rate > 0.1 else "off_peak", "house")])
        seg1("2025-01-01T02:00:00", 2.0, OFF)
        seg1("2025-01-01T03:00:00", 2.0, OFF)
        seg1("2025-01-01T18:00:00", 1.0, PEAK)
        bcost = round(1.5 * OFF + 1.5 * PEAK, 6)     # boundary: 1.5 off + 1.5 peak → blend ~0.189
        self._blk(st, "2025-01-01T05:00:00", 3.0, round(bcost / 3.0, 6), bcost)
        st.set_block_segments("2025-01-01T05:00:00", "electricity_main",
            [ps.Segment(1.5, OFF, None, "off_peak", "house"),
             ps.Segment(1.5, PEAK, None, "day", "house")])
        st._conn.commit()
        tiers = sorted((self._agg(st).get("rate_tiers") or []), key=lambda t: t["rate"])
        rates = [round(t["rate"], 4) for t in tiers if t["kwh"] > 1e-6]
        self.assertEqual(len(rates), 2, rates)                       # two bands, not collapsed/shattered
        self.assertTrue(rates[0] < 0.1 and rates[1] > 0.3, rates)    # off-peak + peak
        self.assertFalse(any(0.1 < r < 0.3 for r in rates), rates)   # NO mid-rate blend tier
        kwh_by = {round(t["rate"], 4): t["kwh"] for t in tiers if t["kwh"] > 1e-6}
        self.assertAlmostEqual(kwh_by[rates[0]], 5.5, places=3)      # off-peak: 2+2+1.5
        self.assertAlmostEqual(kwh_by[rates[1]], 2.5, places=3)      # peak: 1+1.5

    def test_synthetic_ev_device_sources_from_segments(self):
        # BL-27: the sensor-less synthetic EV device now prices from the EV-attributed
        # SEGMENTS, not the imp_*_ev columns. Seed a deliberately WRONG imp_cost_ev column
        # and correct segments — the device must reflect the segments (0.60, not 99).
        import pricing_segments as ps
        st = self._store()
        st._conn.execute(
            "INSERT INTO blocks (block_start, block_end, meter_id, config_period_id, "
            "imp_kwh, imp_rate, imp_cost, imp_kwh_ev, imp_cost_ev, imp_rate_ev, source) "
            "VALUES (?,?, 'electricity_main', ?, 3.0, 0.31, 0.923092, 2.0, 99.0, 99.0, "
            "'imported_api')",
            ("2025-01-01T02:00:00", "2025-01-01T02:00:00", self._cp))
        st.set_block_segments("2025-01-01T02:00:00", "electricity_main", [
            ps.Segment(2.0, 0.30, None, "peak", "ev"),
            ps.Segment(1.0, 0.323092, None, "day", "house")])
        st._conn.execute("INSERT INTO dispatch_history (slot_start, kind, energy_kwh, "
                         "first_seen, last_seen) VALUES ('2025-01-01T02:00:00', "
                         "'completed', 2.0, '2025-01-01T02:00:00', '2025-01-01T02:00:00')")
        st._conn.commit()
        ev = self._agg(st)["sub_meters"]["ev_dispatch"]
        self.assertAlmostEqual(ev["imp_kwh"], 2.0, places=5)
        self.assertAlmostEqual(ev["imp_cost"], 0.60, places=5)   # segments, NOT the 99 column


class TestInsightsSyntheticEvCarbon(unittest.TestCase):
    """Δ5-wire: _aggregate_insights exposes an ADDITIVE `ev_carbon` block sourced from the
    synthetic dispatch device (single-source), price-free. The credit is the draw gap
    (completed dispatch − grid-clipped imp_kwh_ev) × block intensity."""

    def _store(self):
        st = BlockStore(":memory:")
        with st._conn:
            cp = st._conn.execute(
                "INSERT INTO config_periods (effective_from, billing_day, block_minutes, "
                "timezone, currency_symbol, currency_code) "
                "VALUES ('2024-01-01T00:00:00',1,30,'UTC','£','GBP')").lastrowid
            st._conn.execute("INSERT INTO meters (config_period_id, meter_id, is_sub_meter, "
                             "meter_type) VALUES (?, 'electricity_main', 0, '')", (cp,))
        self._cp = cp
        st._conn.commit()
        return st

    def _agg(self, st):
        cfg = {"meters": {"electricity_main": {}}}
        return server._aggregate_insights(st, cfg,
                                          "2025-01-01T00:00:00", "2025-01-02T00:00:00")

    def test_ev_saving_from_draw_gap(self):
        st = self._store()
        # clipped billing EV = 2.0 (imp_kwh_ev); completed dispatch (unclipped) = 2.3 → 0.3 behind
        st._conn.execute(
            "INSERT INTO blocks (block_start, block_end, meter_id, config_period_id, "
            "imp_kwh, imp_rate, imp_cost, imp_kwh_ev, carbon_intensity_g, carbon_g, source) "
            "VALUES (?,?, 'electricity_main', ?, 3.0, 0.30, 0.90, 2.0, 140.0, 420.0, 'imported_api')",
            ("2025-01-01T02:00:00", "2025-01-01T02:00:00", self._cp))
        st._conn.execute("INSERT INTO dispatch_history (slot_start, kind, energy_kwh, "
                         "first_seen, last_seen) VALUES ('2025-01-01T02:00:00','completed',"
                         "2.3,'2025-01-01T02:00:00','2025-01-01T02:00:00')")
        st._conn.commit()
        ev = self._agg(st)["ev_carbon"]
        self.assertEqual(ev["source"], "synthetic_dispatch")
        self.assertEqual(ev["credit_blocks"], 1)
        self.assertAlmostEqual(ev["saving_g"], round(0.3 * 140.0, 4), places=4)   # draw gap × intensity
        self.assertAlmostEqual(ev["grid_g"],  round(2.0 * 140.0, 4), places=4)    # clipped × intensity
        self.assertAlmostEqual(ev["clipped_kwh"], 2.0, places=3)

    def test_non_iog_no_ev_carbon(self):
        # no completed dispatch → ev_carbon is None → template renders nothing (byte-identical)
        st = self._store()
        st._conn.execute(
            "INSERT INTO blocks (block_start, block_end, meter_id, config_period_id, "
            "imp_kwh, imp_rate, imp_cost, imp_kwh_ev, carbon_intensity_g, carbon_g, source) "
            "VALUES (?,?, 'electricity_main', ?, 3.0, 0.30, 0.90, 2.0, 140.0, 420.0, 'imported_api')",
            ("2025-01-01T02:00:00", "2025-01-01T02:00:00", self._cp))
        st._conn.commit()
        self.assertIsNone(self._agg(st)["ev_carbon"])


if __name__ == "__main__":
    unittest.main()
