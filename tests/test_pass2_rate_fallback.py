"""
Device re-attribution over a freshly gap-filled range runs PASS 2 on house blocks whose
legacy imp_rate is still NULL (the verify reprice hasn't landed). _apply_pass2 must not
crash (float * None) and must cost from the priced segments, else cost/kWh, else 0 — not
silently zero a priced block. Plus the attribution job must wait for a running verify.
"""
import os, sys, unittest
from unittest import mock
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import engine


def _block(rate, kwh=1.0, cost=None):
    imp = {"kwh": kwh, "rate": rate}
    if cost is not None:
        imp["cost"] = cost
    return {"start": "2026-01-15T00:00:00", "meters": {
        "electricity_main": {"channels": {"import": imp}},
        "ev_charger": {"meta": {"sub_meter": True, "parent_meter": "electricity_main"},
                       "channels": {"import": {"kwh": 0.4}}},
    }}


class TestPass2RateFallback(unittest.TestCase):
    def _store_with_segments(self, segs):
        st = mock.MagicMock()
        st.get_block_segments.return_value = segs
        return st

    def test_null_rate_uses_segment_rate(self):
        st = self._store_with_segments([{"kwh": 1.0, "inc_rate": 0.30}])
        with mock.patch.object(engine, "get_store", lambda: st):
            b = _block(None)
            engine._apply_pass2(b)
        imp = b["meters"]["electricity_main"]["channels"]["import"]
        # remainder = grid 1.0 - sub 0.4 = 0.6 @ segment 0.30 = 0.18
        self.assertAlmostEqual(imp["kwh_remainder"], 0.6, places=6)
        self.assertAlmostEqual(imp["cost_remainder"], 0.18, places=6)

    def test_null_rate_no_segments_uses_cost_over_kwh(self):
        st = self._store_with_segments([])          # no priced segments
        with mock.patch.object(engine, "get_store", lambda: st):
            b = _block(None, kwh=1.0, cost=0.25)     # implied rate 0.25/kWh
            engine._apply_pass2(b)
        imp = b["meters"]["electricity_main"]["channels"]["import"]
        self.assertAlmostEqual(imp["cost_remainder"], 0.6 * 0.25, places=6)

    def test_null_rate_nothing_priced_is_zero_not_crash(self):
        st = self._store_with_segments([])
        with mock.patch.object(engine, "get_store", lambda: st):
            b = _block(None, kwh=1.0)                # no cost, no segments
            engine._apply_pass2(b)                   # must not raise
        imp = b["meters"]["electricity_main"]["channels"]["import"]
        self.assertEqual(imp["cost_remainder"], 0.0)

    def test_priced_rate_unaffected(self):
        b = _block(0.30)
        engine._apply_pass2(b)                        # no store needed
        imp = b["meters"]["electricity_main"]["channels"]["import"]
        self.assertAlmostEqual(imp["cost_remainder"], 0.18, places=6)

    def test_verify_running_flags_active_states(self):
        for st in ("running", "waiting", "paused"):
            engine._verify_job = {"status": st}
            self.assertTrue(engine._verify_running(), st)
        for st in ("done", "idle", "error"):
            engine._verify_job = {"status": st}
            self.assertFalse(engine._verify_running(), st)


if __name__ == "__main__":
    unittest.main()
