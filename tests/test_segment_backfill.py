"""
test_segment_backfill.py — BL-27 one-shot backfill: derive block_segments for existing
history from the legacy imp_* columns. Paged/idempotent; excludes sub-meters; the derived
segments project back to the block's stored figures.
"""

import asyncio
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import engine
from block_store import BlockStore
import pricing_segments as ps


def _run(coro):
    return asyncio.new_event_loop().run_until_complete(coro)


class TestSegmentBackfill(unittest.TestCase):

    def setUp(self):
        self.st = BlockStore(":memory:")
        engine._store = self.st
        self.st._conn.execute(
            "INSERT INTO config_periods (id, effective_from, billing_day, block_minutes, "
            "timezone, currency_symbol, currency_code) "
            "VALUES (1,'2026-01-01T00:00:00',1,30,'UTC','£','GBP')")
        self.st._conn.execute(
            "INSERT OR IGNORE INTO meters (meter_id, config_period_id, is_sub_meter) "
            "VALUES ('electricity_main',1,0),('sub_batt',1,1)")
        # a split block, a plain block, and a sub-meter block (must be ignored)
        self._blk("2026-08-01T02:00:00", kwh=3.0, rate=0.05, cost=0.15,
                  kwh_ev=2.0, cost_ev=0.10, rate_ev=0.05)
        self._blk("2026-08-01T18:00:00", kwh=1.0, rate=0.30, cost=0.30)
        self._blk("2026-08-01T18:00:00", kwh=0.5, rate=0.30, cost=0.15, meter="sub_batt")
        self.st._conn.commit()

    def tearDown(self):
        engine._store = None

    def _blk(self, start, *, kwh, rate, cost, kwh_ev=None, cost_ev=None, rate_ev=None,
             meter="electricity_main"):
        self.st._conn.execute(
            "INSERT INTO blocks (block_start, block_end, meter_id, config_period_id, "
            "imp_kwh, imp_rate, imp_cost, imp_kwh_ev, imp_cost_ev, imp_rate_ev) "
            "VALUES (?,?,?,1,?,?,?,?,?,?)",
            (start, start, meter, kwh, rate, cost, kwh_ev, cost_ev, rate_ev))

    def test_backfill_derives_and_marks_done(self):
        self.assertEqual(self.st.count_blocks_missing_segments(), 2)   # 2 main, sub excluded
        filled = _run(engine._run_historical_segment_backfill())
        self.assertEqual(filled, 2)
        self.assertEqual(self.st.count_blocks_missing_segments(), 0)
        self.assertTrue(self.st.get_meta(engine._SEGMENT_BACKFILL_MARKER, {}).get("done"))
        # sub-meter got nothing
        self.assertEqual(self.st.get_block_segments("2026-08-01T18:00:00", "sub_batt"), [])
        # split block projects back to its stored figures
        segs = [ps.Segment(**r) for r in
                self.st.get_block_segments("2026-08-01T02:00:00", "electricity_main")]
        self.assertAlmostEqual(ps.attribution_kwh(segs, "ev"), 2.0)
        self.assertAlmostEqual(ps.total_cost(segs), 0.15)

    def test_idempotent(self):
        self.assertEqual(_run(engine._run_historical_segment_backfill()), 2)
        self.assertEqual(_run(engine._run_historical_segment_backfill()), 0)   # done


if __name__ == "__main__":
    unittest.main()
