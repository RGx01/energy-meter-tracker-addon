"""
4.5.3-B: on an API-only account with no live source (no Mini device AND no local
sensor), an empty block at a passed boundary finalises as "nothing to finalise"
WITHOUT advancing the opener, so ensure_correct_block re-rolled the SAME boundary
every tick forever. Guards that a non-advancing (empty) finalise rolls the opener
forward to the current window, while an already-advanced block (incl. gap
catch-up) is left untouched.
"""
import os, sys, unittest
from datetime import datetime
from unittest import mock
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import engine

STUCK = "2026-08-26T08:30:00"
NOWWIN = "2026-08-26T09:00:00"


def _patches(load_after_finalise_start):
    st = mock.MagicMock()
    calls = {"n": 0, "saved": []}
    def _load():
        calls["n"] += 1
        start = STUCK if calls["n"] == 1 else load_after_finalise_start
        return {"start": start, "end": "x", "meters": {}}
    st.load_current_block.side_effect = _load
    st.save_current_block.side_effect = lambda b: calls["saved"].append(b)
    def _create(start, end, block_minutes=30, seed_meters=False):
        return {"start": engine.iso(start), "end": engine.iso(end), "meters": {}}
    return st, calls, _create


class TestEnsureBlockAdvance(unittest.TestCase):
    def _run(self, load_after_finalise_start):
        st, calls, _create = _patches(load_after_finalise_start)
        now = datetime(2026, 8, 26, 9, 2, 1)          # 121s past the 09:00 boundary
        cb = {"start": STUCK, "end": "x", "meters": {}}
        with mock.patch.object(engine, "_store", st), \
             mock.patch.object(engine, "get_block_minutes", lambda: 30), \
             mock.patch.object(engine, "finalise_block", lambda *a, **k: None), \
             mock.patch.object(engine, "detect_gap", lambda *a, **k: []), \
             mock.patch.object(engine, "create_block", _create), \
             mock.patch.object(engine, "mode_uses_api", lambda *a, **k: True), \
             mock.patch.object(engine, "_has_local_import_sensor", lambda: False):
            out = engine.ensure_correct_block(None, cb, now)
        return out, calls

    def test_stuck_empty_block_rolls_forward(self):
        out, calls = self._run(load_after_finalise_start=STUCK)
        self.assertEqual(out["start"], NOWWIN, "opener must advance past the boundary")
        self.assertTrue(calls["saved"] and calls["saved"][-1]["start"] == NOWWIN,
                        "advanced block must be persisted")

    def test_advanced_block_left_untouched(self):
        out, calls = self._run(load_after_finalise_start="2026-08-26T09:00:00")
        self.assertEqual(out["start"], "2026-08-26T09:00:00")
        self.assertFalse(calls["saved"], "must not force-advance an already-advanced opener")


if __name__ == "__main__":
    unittest.main()
