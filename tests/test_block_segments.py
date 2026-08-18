"""
test_block_segments.py — BL-27 storage: the block_segments child table round-trips the
priced-segment decomposition, replaces cleanly, orders by position, and the projections
(pricing_segments) reconstruct the legacy imp_* views from the stored rows.
"""

import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from block_store import BlockStore
import pricing_segments as ps

SLOT = "2026-01-01T18:00:00"
MID = "electricity_main"


class TestBlockSegments(unittest.TestCase):

    def setUp(self):
        self.st = BlockStore(":memory:")

    def _segs(self):
        # over-cap block: EV 4 @ peak, house 2 @ day.
        return ps.import_segments(
            ev_kwh=4.0, house_kwh=2.0,
            house_offpeak_rate=0.05, house_day_rate=0.30,
            ev_offpeak_rate=0.05, ev_peak_rate=0.32,
            ev_offpeak_frac=0.0, house_offpeak_frac=0.0)

    def test_round_trip_and_order(self):
        segs = self._segs()
        n = self.st.set_block_segments(SLOT, MID, segs)
        self.assertEqual(n, len(segs))
        got = self.st.get_block_segments(SLOT, MID)
        self.assertEqual(len(got), len(segs))
        # position order preserved; values intact
        for src, dst in zip(segs, got):
            self.assertAlmostEqual(dst["kwh"], src.kwh)
            self.assertAlmostEqual(dst["inc_rate"], src.inc_rate)
            self.assertEqual(dst["band"], src.band)
            self.assertEqual(dst["attribution"], src.attribution)

    def test_replace_is_clean(self):
        self.st.set_block_segments(SLOT, MID, self._segs())
        self.st.set_block_segments(SLOT, MID, [(1.0, 0.10, 0.095, "day", "house")])
        got = self.st.get_block_segments(SLOT, MID)
        self.assertEqual(len(got), 1)                 # old rows gone, not appended
        self.assertAlmostEqual(got[0]["kwh"], 1.0)

    def test_projections_reconstruct_legacy_views(self):
        # The stored rows, read back as Segments, reproduce the imp_* projections.
        self.st.set_block_segments(SLOT, MID, self._segs())
        got = [ps.Segment(**r) for r in self.st.get_block_segments(SLOT, MID)]
        self.assertAlmostEqual(ps.attribution_kwh(got, "ev"), 4.0)          # imp_kwh_ev
        self.assertAlmostEqual(ps.attribution_cost(got, "ev"), 4 * 0.32)    # imp_cost_ev
        self.assertAlmostEqual(ps.attribution_rate(got, "ev"), 0.32)        # imp_rate_ev
        self.assertAlmostEqual(ps.total_cost(got), 4 * 0.32 + 2 * 0.30)     # imp_cost
        self.assertAlmostEqual(ps.total_kwh(got), 6.0)

    def test_empty_when_none_stored(self):
        self.assertEqual(self.st.get_block_segments("2020-01-01T00:00:00", MID), [])
        self.assertEqual(self.st.count_block_segments(), 0)

    def test_channel_isolation(self):
        self.st.set_block_segments(SLOT, MID, self._segs(), channel="import")
        self.assertEqual(self.st.get_block_segments(SLOT, MID, channel="export"), [])


if __name__ == "__main__":
    unittest.main()
