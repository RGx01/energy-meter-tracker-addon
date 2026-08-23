"""
P3.1: block-delete paths must clean the block_segments child table.

block_segments is keyed by block_start (no block_id FK), so it is NOT cascade-cleaned when
blocks are deleted. Before P3.1 a range/meter/imported delete left orphaned segments, breaking
the Σ block_segments.kwh == grid invariant and mis-feeding every segment-reading surface.
"""
import os, sys, unittest
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from block_store import BlockStore
import pricing_segments as ps


class TestDeleteCleansSegments(unittest.TestCase):
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

    def _blk(self, st, start, source="imported_api"):
        st._conn.execute(
            "INSERT INTO blocks (block_start, block_end, meter_id, config_period_id, "
            "imp_kwh, imp_rate, imp_cost, source) VALUES (?,?, 'electricity_main', ?, "
            "3.0, 0.30, 0.90, ?)", (start, start, self._cp, source))
        st.set_block_segments(start, "electricity_main", [
            ps.Segment(2.0, 0.30, None, "peak", "ev"),
            ps.Segment(1.0, 0.323, None, "day", "house")])

    def _seg_count(self, st, start):
        return st._conn.execute(
            "SELECT COUNT(*) n FROM block_segments WHERE block_start=? AND meter_id='electricity_main'",
            (start,)).fetchone()["n"]

    def test_range_delete_cleans_segments_of_deleted_only(self):
        st = self._store()
        keep  = "2025-01-01T00:00:00"
        drop1 = "2025-01-05T02:00:00"
        drop2 = "2025-01-05T03:00:00"
        for s in (keep, drop1, drop2):
            self._blk(st, s)
        st._conn.commit()
        self.assertEqual(self._seg_count(st, drop1), 2)   # present before

        res = st.delete_blocks_for_date_range("2025-01-05", "2025-01-05", tz_name="UTC")
        self.assertEqual(res["deleted"], 2)
        self.assertEqual(res["segments_deleted"], 4)       # 2 blocks × 2 segments
        self.assertEqual(self._seg_count(st, drop1), 0)    # deleted block's segments gone
        self.assertEqual(self._seg_count(st, drop2), 0)
        self.assertEqual(self._seg_count(st, keep), 2)     # survivor untouched
        # invariant: no orphans anywhere
        orphans = st._conn.execute(
            "SELECT COUNT(*) n FROM block_segments s WHERE NOT EXISTS "
            "(SELECT 1 FROM blocks b WHERE b.block_start=s.block_start AND b.meter_id=s.meter_id)"
        ).fetchone()["n"]
        self.assertEqual(orphans, 0)

    def test_purge_imported_cleans_segments(self):
        st = self._store()
        self._blk(st, "2025-02-01T00:00:00", source="imported_api")
        self._blk(st, "2025-02-01T00:30:00", source="live")   # a non-imported survivor
        st._conn.commit()
        res = st.purge_imported_history()
        self.assertEqual(res["block_segments"], 2)            # only the imported block's segs
        self.assertEqual(self._seg_count(st, "2025-02-01T00:00:00"), 0)
        self.assertEqual(self._seg_count(st, "2025-02-01T00:30:00"), 2)


if __name__ == "__main__":
    unittest.main()
