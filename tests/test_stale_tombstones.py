"""
test_stale_tombstones.py — 4.5.6.

`prune_stale_deleted_ranges` removes orphaned deletion tombstones whose range is
already fully populated with blocks (deleted in the past, then re-created by another
path that never cleared the tombstone), while leaving genuinely-still-empty and
partially-filled deletions untouched. Self-heals the Data Management list without
needing the (now IOG-gated) re-import path.
"""
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from block_store import BlockStore

M = "electricity_main"


class TestPruneStaleTombstones(unittest.TestCase):
    def setUp(self):
        self.st = BlockStore(":memory:")
        self.st._conn.execute(
            "INSERT OR IGNORE INTO config_periods (id, effective_from, billing_day, "
            "block_minutes, timezone) VALUES (1, '2020-01-01T00:00:00', 1, 30, 'UTC')")

    def _block(self, start):
        self.st._conn.execute(
            "INSERT INTO blocks (block_start, block_end, meter_id, config_period_id, "
            "imp_kwh, imp_rate, imp_cost) VALUES (?,?,?,1,?,?,?)",
            (start, start, M, 0.1, 0.05493, 0.005493))
        self.st._conn.commit()

    def test_fully_populated_tombstone_pruned(self):
        # [10:00,11:00) = two 30-min slots, both present → tombstone is stale.
        self._block("2026-08-10T10:00:00")
        self._block("2026-08-10T10:30:00")
        self.st.add_deleted_range(M, "2026-08-10T10:00:00", "2026-08-10T11:00:00")
        self.assertEqual(self.st.prune_stale_deleted_ranges(), 1)
        self.assertEqual(self.st.get_deleted_ranges(), [])

    def test_empty_tombstone_kept(self):
        # No blocks in the range → a genuine still-empty deletion, must survive.
        self.st.add_deleted_range(M, "2026-08-12T12:00:00", "2026-08-12T13:00:00")
        self.assertEqual(self.st.prune_stale_deleted_ranges(), 0)
        self.assertEqual(len(self.st.get_deleted_ranges()), 1)

    def test_partially_filled_tombstone_kept(self):
        # Only one of two slots present → not fully re-created, leave the tombstone.
        self._block("2026-08-14T14:00:00")           # 14:30 slot missing
        self.st.add_deleted_range(M, "2026-08-14T14:00:00", "2026-08-14T15:00:00")
        self.assertEqual(self.st.prune_stale_deleted_ranges(), 0)
        self.assertEqual(len(self.st.get_deleted_ranges()), 1)

    def test_all_meters_star_row_checks_main(self):
        # A '*' (all-meters) tombstone is judged against the main meter's blocks.
        self._block("2026-08-16T20:00:00")
        self._block("2026-08-16T20:30:00")
        self.st.add_deleted_range("*", "2026-08-16T20:00:00", "2026-08-16T21:00:00")
        self.assertEqual(self.st.prune_stale_deleted_ranges(), 1)


if __name__ == "__main__":
    unittest.main()
