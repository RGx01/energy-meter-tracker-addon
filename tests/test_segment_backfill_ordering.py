"""
Ordering guard: the segment backfill derives segments from the legacy split columns
(segments_from_legacy reads imp_kwh_ev), so it must NOT start until the IOG split backfill
has filled those columns — otherwise a dispatched block gets a permanent house-only segment
(columns say split, segments say house-only: a run-order-dependent divergence). This asserts
_maybe_backfill_historical_segments DEFERS while the split backfill is not done, and proceeds
once it is.
"""
import asyncio, os, sys, unittest
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from block_store import BlockStore
import engine


class TestSegmentBackfillOrdering(unittest.TestCase):
    def setUp(self):
        self.st = BlockStore(":memory:")
        self._saved = engine._store
        engine._store = self.st
        engine._segment_backfill_running = False

    def tearDown(self):
        engine._store = self._saved
        engine._segment_backfill_running = False

    def _maybe_under_loop(self):
        async def _run():
            engine._maybe_backfill_historical_segments()
            started = engine._segment_backfill_running   # True only if it got past the guard
            # cancel any scheduled drain task so it can't touch the store after teardown
            for t in asyncio.all_tasks() - {asyncio.current_task()}:
                t.cancel()
            return started
        return asyncio.run(_run())

    def test_defers_while_split_backfill_not_done(self):
        self.st.set_meta(engine._IOG_SPLIT_BACKFILL_MARKER, {})     # split NOT done
        self.assertFalse(self._maybe_under_loop())                  # deferred

    def test_proceeds_once_split_backfill_done(self):
        self.st.set_meta(engine._IOG_SPLIT_BACKFILL_MARKER,
                         {"done": True, "scope": engine._IOG_SPLIT_BACKFILL_SCOPE})
        self.assertTrue(self._maybe_under_loop())                   # got past the guard


if __name__ == "__main__":
    unittest.main()
