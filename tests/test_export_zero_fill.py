"""
test_export_zero_fill.py
========================
Historical-export hygiene: on an imported day that already has SOME export, blank
(NULL) export slots are set to 0 — a solar meter exports nothing overnight and the
source (PDF/CSV/API) only listed the daytime export, so the blanks are confirmed
zeros. Day-scoped and imported-only, so it can't invent data for an un-imported day
or touch a live block awaiting DCC settlement. Without it those zeros show as a
permanent, un-fillable export "gap".

In-memory SQLite only.
"""

import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(__file__))
from block_store import BlockStore


class TestZeroFillImportedExportBlanks(unittest.TestCase):

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

    def _blk(self, st, start, exp, source="imported_api"):
        st._conn.execute(
            "INSERT INTO blocks (block_start, block_end, meter_id, config_period_id, "
            "imp_kwh, exp_kwh, source) VALUES (?,?, 'electricity_main', ?, 1.0, ?, ?)",
            (start, start, self._cp, exp, source))

    def _exp(self, st, bs):
        return st._conn.execute(
            "SELECT exp_kwh, exp_cost FROM blocks WHERE block_start=? "
            "AND meter_id='electricity_main'", (bs,)).fetchone()

    def test_fills_blanks_on_export_day_only(self):
        st = self._store()
        # Day A (imported): daytime export + blank overnight -> fill the blank to 0
        self._blk(st, "2025-01-01T03:00:00", None)
        self._blk(st, "2025-01-01T12:00:00", 1.5)
        # Day B (imported): NO export at all -> leave untouched (could be un-imported)
        self._blk(st, "2025-01-02T03:00:00", None)
        self._blk(st, "2025-01-02T12:00:00", None)
        # A LIVE block (kraken_api) with blank export -> must NOT be touched
        self._blk(st, "2025-01-01T13:00:00", None, source="kraken_api")
        st._conn.commit()

        n = st.zero_fill_imported_export_blanks("UTC")
        self.assertEqual(n, 1)                                      # only Day A's blank
        self.assertEqual(self._exp(st, "2025-01-01T03:00:00")["exp_kwh"], 0.0)
        self.assertEqual(self._exp(st, "2025-01-01T03:00:00")["exp_cost"], 0.0)  # £0
        self.assertIsNone(self._exp(st, "2025-01-02T03:00:00")["exp_kwh"])  # no-export day
        self.assertIsNone(self._exp(st, "2025-01-01T13:00:00")["exp_kwh"])  # live block

    def test_idempotent(self):
        st = self._store()
        self._blk(st, "2025-01-01T03:00:00", None)
        self._blk(st, "2025-01-01T12:00:00", 1.5)
        st._conn.commit()
        self.assertEqual(st.zero_fill_imported_export_blanks("UTC"), 1)
        self.assertEqual(st.zero_fill_imported_export_blanks("UTC"), 0)

    def test_auto_resolves_timezone(self):
        # tz_name=None resolves from the newest config period (no crash, runs)
        st = self._store()
        self._blk(st, "2025-01-01T03:00:00", None)
        self._blk(st, "2025-01-01T12:00:00", 1.5)
        st._conn.commit()
        self.assertEqual(st.zero_fill_imported_export_blanks(), 1)


if __name__ == "__main__":
    unittest.main()
