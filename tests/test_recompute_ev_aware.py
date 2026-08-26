"""
BL-46: recompute_remainders_for_window (run on device delete) must be EV-aware —
re-derive the house remainder as grid − imp_kwh_ev − surviving subs, not grid −
subs. Without this a device delete folds the dispatch EV back into the house line
(imp_kwh_remainder read wrong; observed on dev as kwh/2).
"""
import os, sys, unittest
from unittest import mock
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import engine


class TestRecomputeEvAware(unittest.TestCase):
    def _run(self, kwh, ev, cost, cost_ev):
        block = {"start": "2026-08-26T01:30:00",
                 "meters": {"electricity_main": {"channels": {"import": {
                     "kwh": kwh, "kwh_ev": ev, "cost": cost, "cost_ev": cost_ev}}}}}
        captured = {}
        store = mock.MagicMock()
        store._conn.execute.return_value.fetchall.return_value = [{"block_start": "2026-08-26T01:30:00"}]
        store.get_block_dict_by_start.return_value = block
        with mock.patch.object(engine, "_store", store), \
             mock.patch.object(engine, "_apply_pass2", lambda b: None), \
             mock.patch.object(engine, "_recompute_pass3_totals", lambda b: None), \
             mock.patch.object(engine, "_recompute_block_carbon", lambda b: None), \
             mock.patch.object(engine, "append_block_replace", lambda b: captured.update(b=b)), \
             mock.patch.object(engine, "generate_charts", lambda *a, **k: None):
            n = engine.recompute_remainders_for_window(
                "electricity_main", "2026-08-26T01:30:00", "2026-08-26T02:00:00")
        self.assertEqual(n, 1)
        return captured["b"]["meters"]["electricity_main"]["channels"]["import"]

    def test_ev_carved_from_house_remainder(self):
        # grid 6.27, EV 3.39 → house = 2.88 (NOT 6.27). cost likewise.
        imp = self._run(6.27, 3.39, 0.44, 0.24)
        self.assertAlmostEqual(imp["kwh_remainder"], 2.88, places=3)
        self.assertAlmostEqual(imp["cost_remainder"], 0.20, places=3)

    def test_no_ev_block_is_full_grid(self):
        # no EV → house = full grid (0.155), not halved.
        imp = self._run(0.155, None, 0.011, None)
        self.assertAlmostEqual(imp["kwh_remainder"], 0.155, places=3)

    def test_ev_never_exceeds_grid_floor(self):
        imp = self._run(3.0, 3.0, 0.2, 0.2)
        self.assertAlmostEqual(imp["kwh_remainder"], 0.0, places=3)


if __name__ == "__main__":
    unittest.main()
