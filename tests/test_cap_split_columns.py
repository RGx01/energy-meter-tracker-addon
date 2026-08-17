"""
test_cap_split_columns.py — BL-9 IOG house/EV split storage columns.
The dispatch-derived EV portion of a block's import is stored on the main block
(imp_kwh_ev / imp_cost_ev / imp_rate_ev); house is the remainder. Additive,
nullable, populated for all IOG tariffs at pricing time.
"""

import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from block_store import BlockStore

_COLS = ("imp_kwh_ev", "imp_cost_ev", "imp_rate_ev")


class TestCapSplitColumns(unittest.TestCase):

    def _store(self):
        st = BlockStore(":memory:")
        with st._conn:
            cp = st._conn.execute(
                "INSERT INTO config_periods (effective_from, billing_day, "
                "block_minutes, timezone, currency_symbol, currency_code) "
                "VALUES ('2024-01-01T00:00:00',1,30,'UTC','£','GBP')").lastrowid
            st._conn.execute("INSERT INTO meters (config_period_id, meter_id, "
                             "is_sub_meter) VALUES (?, 'electricity_main', 0)", (cp,))
        self._cp = cp
        st._conn.commit()
        return st

    def _cols(self, st):
        return {r[1] for r in st._conn.execute(
            "PRAGMA table_info(blocks)").fetchall()}

    def test_columns_present_on_fresh_db(self):
        st = self._store()
        cols = self._cols(st)
        for c in _COLS:
            self.assertIn(c, cols)

    def test_default_null_when_unset(self):
        st = self._store()
        st._conn.execute(
            "INSERT INTO blocks (block_start, block_end, meter_id, "
            "config_period_id, imp_kwh, source) VALUES "
            "('2026-01-01T13:00:00','2026-01-01T13:30:00','electricity_main',?,3.0,'kraken_api')",
            (self._cp,))
        row = st._conn.execute(
            "SELECT imp_kwh_ev, imp_cost_ev, imp_rate_ev FROM blocks").fetchone()
        self.assertIsNone(row["imp_kwh_ev"])
        self.assertIsNone(row["imp_cost_ev"])
        self.assertIsNone(row["imp_rate_ev"])

    def test_write_read_roundtrip(self):
        st = self._store()
        # 3 kWh import, 2 kWh EV @ 0.05 = 0.10, house 1 kWh @ 0.07 = 0.07 → 0.17
        st._conn.execute(
            "INSERT INTO blocks (block_start, block_end, meter_id, config_period_id, "
            "imp_kwh, imp_cost, imp_rate, imp_kwh_ev, imp_cost_ev, imp_rate_ev, source) "
            "VALUES ('2026-01-01T13:00:00','2026-01-01T13:30:00','electricity_main',?,"
            "3.0, 0.17, 0.056667, 2.0, 0.10, 0.05, 'kraken_api')",
            (self._cp,))
        row = st._conn.execute(
            "SELECT imp_kwh, imp_cost, imp_kwh_ev, imp_cost_ev, imp_rate_ev "
            "FROM blocks").fetchone()
        self.assertEqual(row["imp_kwh_ev"], 2.0)
        self.assertEqual(row["imp_cost_ev"], 0.10)
        self.assertEqual(row["imp_rate_ev"], 0.05)
        # house remainder is derivable, not stored
        self.assertAlmostEqual(row["imp_kwh"] - row["imp_kwh_ev"], 1.0)
        self.assertAlmostEqual(row["imp_cost"] - row["imp_cost_ev"], 0.07)


if __name__ == "__main__":
    unittest.main()
