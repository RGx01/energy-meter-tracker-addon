"""
Tests for the redesigned block-delete path (delete_blocks_for_date_range +
count_blocks_for_date_range).

Pins the four behaviours we agreed:
  1. The "all" sentinel (and None / "") deletes EVERY meter for the range —
     the old code passed "all" through as a literal meter_id filter, matching
     no row and silently deleting nothing.
  2. Deleting a MAIN meter for a range pulls in its sub-meter (device) blocks,
     so a day can't be left half-deleted with orphan device lines.
  3. The delete cascades to `reads` (FK to blocks(id), no ON DELETE CASCADE —
     so a block with live reads would otherwise raise) and `generation_mix`.
  4. A TAIL delete (removing the most recent finalised block) clears the live
     engine state (current_block + current_reads) so the next block re-anchors;
     a purely historical delete leaves that state untouched.
Plus: preview (count) uses the same resolution, so it can't disagree.
"""

import os
import sys
import unittest

# Sibling test modules live in this dir; pytest runs with the repo root on the
# path (package mode), so add tests/ so `import test_block_store` resolves —
# same line the other cross-importing test files use.
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from test_block_store import new_store, EXAMPLE_CONFIG_WITH_SUB, make_block_with_sub, make_block

MAIN = "electricity_main"
DEV  = "zappi_ev"
D16, D17, D18 = "2025-05-16", "2025-05-17", "2025-05-18"


def _ts(day, hhmm="12:00:00"):
    return f"{day}T{hhmm}"


class DeleteBlocksBase(unittest.TestCase):
    def setUp(self):
        self.store = new_store()
        self.store.insert_config_period(EXAMPLE_CONFIG_WITH_SUB)
        # Three days, each producing a main + device (zappi_ev) block row.
        for day in (D16, D17, D18):
            self.store.append_block(make_block_with_sub(_ts(day)))

    def tearDown(self):
        self.store.close()

    # helpers --------------------------------------------------------------
    def _rows(self, where="", params=()):
        return self.store._conn.execute(
            f"SELECT meter_id, block_start FROM blocks {where} ORDER BY block_start, meter_id",
            params,
        ).fetchall()

    def _meter_ids_on(self, day):
        return sorted(r["meter_id"] for r in self._rows(
            "WHERE date(block_start) = ?", (day,)))

    def _block_id(self, meter_id, day):
        r = self.store._conn.execute(
            "SELECT id FROM blocks WHERE meter_id=? AND date(block_start)=?",
            (meter_id, day),
        ).fetchone()
        return r["id"] if r else None

    def _delete(self, frm, to, meter_id=None):
        return self.store.delete_blocks_for_date_range(
            frm, to, meter_id, tz_name="UTC")


class TestSentinelAndDeviceInclusion(DeleteBlocksBase):

    def test_setup_has_both_meters_each_day(self):
        self.assertEqual(self._meter_ids_on(D16), [MAIN, DEV])
        self.assertEqual(len(self._rows()), 6)  # 3 days x 2 meters

    def test_all_sentinel_deletes_every_meter(self):
        # "all", None and "" must all mean "no meter filter" → both rows gone.
        for sentinel in ("all", None, ""):
            with self.subTest(sentinel=sentinel):
                self.setUp()  # fresh per sentinel
                res = self._delete(D16, D16, meter_id=sentinel)
                self.assertEqual(res["deleted"], 2)
                self.assertEqual(self._meter_ids_on(D16), [])
                self.assertEqual(self._meter_ids_on(D17), [MAIN, DEV])

    def test_main_meter_delete_includes_device_blocks(self):
        res = self._delete(D16, D16, meter_id=MAIN)
        self.assertEqual(res["deleted"], 2)              # main + device
        self.assertEqual(self._meter_ids_on(D16), [])    # device went too

    def test_device_only_delete_leaves_main(self):
        res = self._delete(D16, D16, meter_id=DEV)
        self.assertEqual(res["deleted"], 1)
        self.assertEqual(self._meter_ids_on(D16), [MAIN])


class TestCascade(DeleteBlocksBase):

    def test_reads_cascade_and_no_fk_error(self):
        keep_id = self._block_id(MAIN, D17)
        drop_id = self._block_id(MAIN, D16)
        self.store._conn.execute(
            "INSERT INTO reads (captured_at, meter_id, channel, reading_kwh, block_id) "
            "VALUES (?,?,?,?,?)", (_ts(D16), MAIN, "import", 1000.0, drop_id))
        self.store._conn.execute(
            "INSERT INTO reads (captured_at, meter_id, channel, reading_kwh, block_id) "
            "VALUES (?,?,?,?,?)", (_ts(D17), MAIN, "import", 1001.0, keep_id))
        self.store._conn.commit()

        res = self._delete(D16, D16, meter_id="all")   # must NOT raise (FK on)
        self.assertEqual(res["reads_deleted"], 1)

        remaining = self.store._conn.execute(
            "SELECT block_id FROM reads").fetchall()
        self.assertEqual([r["block_id"] for r in remaining], [keep_id])

    def test_generation_mix_cascade(self):
        drop_id = self._block_id(MAIN, D16)
        keep_id = self._block_id(MAIN, D17)
        for bid, fuel in ((drop_id, "wind"), (drop_id, "solar"), (keep_id, "gas")):
            self.store._conn.execute(
                "INSERT INTO generation_mix (block_id, fuel, perc) VALUES (?,?,?)",
                (bid, fuel, 25.0))
        self.store._conn.commit()

        res = self._delete(D16, D16, meter_id="all")
        self.assertEqual(res["generation_mix_deleted"], 2)
        survivors = self.store._conn.execute(
            "SELECT block_id FROM generation_mix").fetchall()
        self.assertEqual([r["block_id"] for r in survivors], [keep_id])


class TestTailReseed(DeleteBlocksBase):

    def _seed_live_state(self):
        self.store._conn.execute(
            "INSERT INTO current_block (id, block_start) VALUES (1, ?)",
            (_ts(D18, "12:30:00"),))
        self.store._conn.execute(
            "INSERT INTO current_reads (captured_at, meter_id, channel, value) "
            "VALUES (?,?,?,?)", (_ts(D18, "12:30:00"), MAIN, "import", 1234.0))
        self.store._conn.commit()

    def _live_counts(self):
        cb = self.store._conn.execute("SELECT COUNT(*) c FROM current_block").fetchone()["c"]
        cr = self.store._conn.execute("SELECT COUNT(*) c FROM current_reads").fetchone()["c"]
        return cb, cr

    def test_tail_delete_reseeds_live_state(self):
        self._seed_live_state()
        res = self._delete(D18, D18, meter_id="all")   # newest day = tail
        self.assertTrue(res["reseeded"])
        self.assertEqual(self._live_counts(), (0, 0))

    def test_historical_delete_preserves_live_state(self):
        self._seed_live_state()
        res = self._delete(D16, D16, meter_id="all")   # 17th & 18th remain after
        self.assertFalse(res["reseeded"])
        self.assertEqual(self._live_counts(), (1, 1))

    def test_device_tail_delete_reseeds(self):
        # Deleting the newest block even for one meter is still a tail delete.
        self._seed_live_state()
        res = self._delete(D18, D18, meter_id=DEV)
        self.assertTrue(res["reseeded"])


class TestPreviewParity(DeleteBlocksBase):

    def test_preview_count_matches_delete(self):
        prev = self.store.count_blocks_for_date_range(D16, D16, "all", tz_name="UTC")
        res  = self._delete(D16, D16, meter_id="all")
        self.assertEqual(prev["blocks"], res["deleted"])
        self.assertEqual(prev["blocks"], 2)

    def test_preview_main_includes_devices(self):
        prev = self.store.count_blocks_for_date_range(D16, D16, MAIN, tz_name="UTC")
        self.assertEqual(prev["blocks"], 2)   # main + device, same as delete


class TestRecomputeScope(DeleteBlocksBase):
    """delete_blocks_for_date_range reports a recompute scope only for a
    device-only delete (parent survives), so the caller knows to rebuild the
    parent's remainder."""

    def test_device_delete_returns_parent_scope(self):
        res = self._delete(D16, D16, meter_id=DEV)
        self.assertEqual(res["recompute_parent"], MAIN)
        self.assertIsNotNone(res["recompute_from"])
        self.assertIsNotNone(res["recompute_to"])

    def test_all_delete_has_no_recompute(self):
        res = self._delete(D16, D16, meter_id="all")
        self.assertIsNone(res["recompute_parent"])

    def test_main_delete_has_no_recompute(self):
        # Main delete takes the devices with it — whole window gone, nothing to fix.
        res = self._delete(D16, D16, meter_id=MAIN)
        self.assertIsNone(res["recompute_parent"])

    def test_device_delete_no_match_has_no_recompute(self):
        # Nothing deleted (date with no blocks) → no recompute scope.
        res = self._delete("2030-01-01", "2030-01-01", meter_id=DEV)
        self.assertEqual(res["deleted"], 0)
        self.assertIsNone(res["recompute_parent"])


class TestPreviewLocalDateCount(unittest.TestCase):
    """The day count in delete/preview must be in LOCAL time. A BST local day
    starts at 23:00 UTC the previous calendar date, so counting distinct UTC
    date(block_start) reported 2 days for a single local day (the bug seen on
    the Delete Blocks screen)."""

    def test_bst_local_day_counts_as_one_day(self):
        store = new_store()
        store.insert_config_period({"meters": {"electricity_main": {"meta": {
            "timezone": "Europe/London", "billing_day": 1, "block_minutes": 30,
            "currency_symbol": "£", "currency_code": "GBP"}}}})
        # local 2026-06-18 (BST) spans UTC [2026-06-17T23:00, 2026-06-18T23:00):
        # the first two half-hours carry a UTC date of the 17th.
        for ts in ("2026-06-17T23:00:00", "2026-06-17T23:30:00",
                   "2026-06-18T00:00:00", "2026-06-18T12:00:00"):
            store.append_block(make_block(ts, meter_id="electricity_main"))

        prev = store.count_blocks_for_date_range(
            "2026-06-18", "2026-06-18", "all", tz_name="Europe/London")
        self.assertEqual(prev["blocks"], 4)
        self.assertEqual(prev["dates"], 1)   # one LOCAL day, not two UTC dates

        res = store.delete_blocks_for_date_range(
            "2026-06-18", "2026-06-18", "all", tz_name="Europe/London")
        self.assertEqual(res["deleted"], 4)
        self.assertEqual(res["dates"], 1)
        store.close()


if __name__ == "__main__":
    unittest.main()