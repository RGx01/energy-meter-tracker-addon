"""
test_cap_split_readpath.py — the stored IOG house/EV split (imp_*_ev) is surfaced
on the read block's import channel (kwh_ev/cost_ev/rate_ev) via _row_to_block, so
the billing summary can render the EV-vs-Home breakdown. Absent when NULL.
"""

import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from block_store import BlockStore


class TestCapSplitReadPath(unittest.TestCase):

    def _store(self):
        st = BlockStore(":memory:")
        with st._conn:
            cp = st._conn.execute(
                "INSERT INTO config_periods (effective_from, billing_day, "
                "block_minutes, timezone, currency_symbol, currency_code) "
                "VALUES ('2024-01-01T00:00:00',1,30,'UTC','£','GBP')").lastrowid
            st._conn.execute("INSERT INTO meters (config_period_id, meter_id, "
                             "is_sub_meter) VALUES (?, 'electricity_main', 0)", (cp,))
        st._conn.commit()
        self._cp = cp
        return st

    def _append(self, st, imp_extra):
        imp = {"kwh": 3.0, "rate": 0.056667, "cost": 0.17}
        imp.update(imp_extra)
        st.append_block({"start": "2026-01-01T13:00:00", "end": "2026-01-01T13:30:00",
                         "meters": {"electricity_main": {"channels": {"import": imp}}}},
                        self._cp)

    def _imp_channel(self, st):
        b = st.get_block_dict_by_start("2026-01-01T13:00:00")
        return b["meters"]["electricity_main"]["channels"]["import"]

    def test_split_surfaced_on_channel(self):
        st = self._store()
        self._append(st, {"kwh_ev": 2.0, "cost_ev": 0.10, "rate_ev": 0.05})
        imp = self._imp_channel(st)
        self.assertEqual(imp["kwh_ev"], 2.0)
        self.assertEqual(imp["cost_ev"], 0.10)
        self.assertEqual(imp["rate_ev"], 0.05)

    def test_absent_when_null(self):
        st = self._store()
        self._append(st, {})                      # no EV split
        imp = self._imp_channel(st)
        self.assertNotIn("kwh_ev", imp)
        self.assertNotIn("cost_ev", imp)
        self.assertNotIn("rate_ev", imp)
        self.assertEqual(imp["kwh"], 3.0)         # rest intact


if __name__ == "__main__":
    unittest.main()
