"""
test_segment_live_write.py — BL-27 16a: the live pricing point writes block_segments, so
new/settled blocks carry segments alongside the imp_* columns (history is backfilled). The
helper derives from the just-priced main import channel and reconciles to it.
"""

import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import engine
from block_store import BlockStore
import pricing_segments as ps


class TestPersistBlockSegments(unittest.TestCase):

    def setUp(self):
        self.st = BlockStore(":memory:")
        engine._store = self.st

    def tearDown(self):
        engine._store = None

    def test_writes_segments_for_capped_channel(self):
        # a capped main import channel (EV peak + house day) → 2 segments, reconciling.
        imp = {"kwh": 3.0, "cost": round(2 * 0.30 + 1 * 0.323092, 6), "rate": 0.32,
               "rate_exc": round(0.32 / 1.05, 6),
               "kwh_ev": 2.0, "cost_ev": 0.60, "rate_ev": 0.30,
               "ev_band": "peak", "home_band": "day"}
        engine._persist_block_segments("2026-09-01T18:00:00", "electricity_main", imp)
        segs = [ps.Segment(**r) for r in
                self.st.get_block_segments("2026-09-01T18:00:00", "electricity_main")]
        self.assertEqual(len(segs), 2)
        self.assertAlmostEqual(ps.total_cost(segs), imp["cost"], places=5)
        self.assertAlmostEqual(ps.attribution_kwh(segs, "ev"), 2.0)
        self.assertAlmostEqual(ps.attribution_cost(segs, "ev"), 0.60, places=5)
        # exc came from the channel's rate_exc ratio
        self.assertTrue(all(s.exc_rate is not None for s in segs))

    def test_rewrite_replaces(self):
        imp = {"kwh": 1.0, "cost": 0.30, "rate": 0.30}
        engine._persist_block_segments("2026-09-01T13:00:00", "electricity_main", imp)
        imp2 = {"kwh": 2.0, "cost": 0.60, "rate": 0.30}
        engine._persist_block_segments("2026-09-01T13:00:00", "electricity_main", imp2)
        segs = self.st.get_block_segments("2026-09-01T13:00:00", "electricity_main")
        self.assertEqual(len(segs), 1)                 # replaced, not appended
        self.assertAlmostEqual(segs[0]["kwh"], 2.0)

    def test_prefers_seam_segments_full_fidelity(self):
        # BL-27 16c: when the pricing seam supplies band segments (a boundary block's four
        # bands), persist stores THOSE — not the collapsed 1–2 the columns would yield — and
        # applies the block's exc ratio to each.
        imp = {"kwh": 3.0, "cost": 0.485, "rate": round(0.485 / 3.0, 6),
               "rate_exc": round(0.485 / 3.0 / 1.05, 6),
               "kwh_ev": 2.0, "cost_ev": 0.30, "rate_ev": 0.15,   # columns = blended EV
               "ev_band": "mixed", "home_band": "mixed",
               "segments": [(1.0, 0.05, "off_peak", "ev"), (1.0, 0.25, "peak", "ev"),
                            (0.5, 0.07, "off_peak", "house"), (0.5, 0.30, "day", "house")]}
        engine._persist_block_segments("2026-09-01T13:00:00", "electricity_main", imp)
        segs = [ps.Segment(**r) for r in
                self.st.get_block_segments("2026-09-01T13:00:00", "electricity_main")]
        self.assertEqual(len(segs), 4)                         # full fidelity, not collapsed
        self.assertAlmostEqual(ps.total_cost(segs), 0.485, places=6)
        self.assertAlmostEqual(ps.attribution_cost(segs, "ev"), 0.30, places=6)
        self.assertTrue(all(s.exc_rate is not None and s.exc_rate < s.inc_rate for s in segs))
        # every segment is at a REAL tariff rate — no blended 0.15 EV row
        self.assertEqual(sorted({s.inc_rate for s in segs}), [0.05, 0.07, 0.25, 0.30])

    def test_no_store_is_noop(self):
        engine._store = None
        engine._persist_block_segments("x", "m", {"kwh": 1.0, "cost": 0.1, "rate": 0.1})  # no crash


if __name__ == "__main__":
    unittest.main()
