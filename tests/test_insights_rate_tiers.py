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


if __name__ == "__main__":
    unittest.main()
