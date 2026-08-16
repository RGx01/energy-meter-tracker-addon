"""
test_unsettled_export_gate.py
=============================
The 'awaiting DCC settlement' count must not flag EXPORT for an account that
can't settle export. A FIT/deemed-export user has no Octopus outgoing agreement,
so their export never DCC-settles — counting it produced a permanent ~2-week
rolling backlog (JW: 671 unsettled, ~530 of them export that can never settle).

The gate is the persisted `export_settlement_expected` flag, set at Octopus
discovery from whether an outgoing/export agreement exists:
  - export NOT expected (FIT) -> export blocks are not counted; import still is.
  - export expected (SEG/Outgoing) -> export lag still counts, even before the
    first settlement lands (new-user-safe: the flag is set from the agreement,
    not from whether anything has settled yet).
  - flag absent (older DB / pre-discovery) -> defaults to expected=TRUE, so a
    genuine gap is never silenced.
"""

import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from block_store import BlockStore


class TestUnsettledExportGate(unittest.TestCase):

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

    def _blk(self, st, start, *, imp_kwh_api=None, exp_kwh=0.0,
             exp_kwh_api=None, source=None):
        st._conn.execute(
            "INSERT INTO blocks (block_start, block_end, meter_id, config_period_id, "
            "imp_kwh, imp_kwh_api, exp_kwh, exp_kwh_api, source, finalised_from_cad) "
            "VALUES (?,?, 'electricity_main', ?, 1.0, ?, ?, ?, ?, 0)",
            (start, start, self._cp, imp_kwh_api, exp_kwh, exp_kwh_api, source))

    # ── FIT: import settled, export never will ──────────────────────────────
    def test_fit_export_not_counted_when_flag_false(self):
        st = self._store()
        # 3 export-bearing blocks: import IS settled, export blank (never settles).
        for h in (12, 13, 14):
            self._blk(st, f"2025-06-01T{h:02d}:00:00",
                      imp_kwh_api=0.0, exp_kwh=2.0, exp_kwh_api=None)
        st._conn.commit()
        st.set_meta("export_settlement_expected", False)
        self.assertEqual(st.count_unsettled_blocks(), 0)
        # …but the SAME data with export expected DOES count all three.
        st.set_meta("export_settlement_expected", True)
        self.assertEqual(st.count_unsettled_blocks(), 3)

    def test_import_unsettled_always_counts(self):
        # An import-awaiting block counts regardless of the export flag.
        st = self._store()
        self._blk(st, "2025-06-02T00:00:00", imp_kwh_api=None, exp_kwh=0.0)
        st._conn.commit()
        st.set_meta("export_settlement_expected", False)
        self.assertEqual(st.count_unsettled_blocks(), 1)
        st.set_meta("export_settlement_expected", True)
        self.assertEqual(st.count_unsettled_blocks(), 1)

    # ── New SEG: agreement exists, nothing settled yet ──────────────────────
    def test_new_seg_export_counts_before_first_settlement(self):
        st = self._store()
        for h in (12, 13):
            self._blk(st, f"2025-06-03T{h:02d}:00:00",
                      imp_kwh_api=0.0, exp_kwh=1.5, exp_kwh_api=None)
        st._conn.commit()
        st.set_meta("export_settlement_expected", True)   # from the outgoing agreement
        self.assertEqual(st.count_unsettled_blocks(), 2)  # counted even pre-settlement

    # ── Established SEG: some export settled, one still lagging ──────────────
    def test_established_seg_counts_only_the_lagging_block(self):
        st = self._store()
        self._blk(st, "2025-06-04T10:00:00", imp_kwh_api=0.0,
                  exp_kwh=2.0, exp_kwh_api=2.0)              # settled export
        self._blk(st, "2025-06-04T11:00:00", imp_kwh_api=0.0,
                  exp_kwh=2.0, exp_kwh_api=None)             # export still lagging
        st._conn.commit()
        st.set_meta("export_settlement_expected", True)
        self.assertEqual(st.count_unsettled_blocks(), 1)

    # ── Default when the flag has never been written ────────────────────────
    def test_flag_absent_defaults_to_expected(self):
        st = self._store()
        self._blk(st, "2025-06-05T12:00:00", imp_kwh_api=0.0,
                  exp_kwh=2.0, exp_kwh_api=None)
        st._conn.commit()
        # No set_meta call at all -> default TRUE -> export counted.
        self.assertTrue(st._export_settlement_expected())
        self.assertEqual(st.count_unsettled_blocks(), 1)


if __name__ == "__main__":
    unittest.main()
