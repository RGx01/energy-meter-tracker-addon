"""
_hybrid_ev_by_block is the single per-block EV authority shared by every surface
(charts/spiral, Usage Stats, usage + carbon insights). It is HYBRID across the dispatch
seam: synthetic (dispatch, `imp_kwh_ev IS NOT NULL`) supersedes the physical EV meter per
block; before the seam the recorded physical device is authority. These tests pin the seam
semantics and the non-IOG byte-identity guard.
"""
import os, sys, unittest
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import test_server as _ts          # applies import stubs + imports server
server = _ts.server

H = server._hybrid_ev_by_block

B1 = "2026-07-06T22:00:00"   # pre-seam  (no dispatch -> imp_kwh_ev NULL)
B2 = "2026-07-07T23:00:00"   # post-seam (dispatch    -> imp_kwh_ev set)
B3 = "2026-07-08T00:30:00"   # post-seam


class TestHybridEvResolver(unittest.TestCase):

    def test_synthetic_wins_when_present(self):
        out = H({B2: (3.0, 0.21)}, {B2: (2.95, 0.20)})   # physical present too
        self.assertEqual(out[B2]["source"], "synthetic")
        self.assertAlmostEqual(out[B2]["kwh"], 3.0)
        self.assertAlmostEqual(out[B2]["cost"], 0.21)    # physical ignored (superseded)

    def test_recorded_when_no_synthetic(self):
        out = H({B1: (None, None)}, {B1: (4.2, 0.30)})
        self.assertEqual(out[B1]["source"], "recorded")
        self.assertAlmostEqual(out[B1]["kwh"], 4.2)
        self.assertAlmostEqual(out[B1]["cost"], 0.30)

    def test_neither_present_omitted(self):
        out = H({B1: (None, None)}, {})
        self.assertNotIn(B1, out)

    def test_non_iog_no_physical_is_empty(self):
        out = H({B1: (None, None), B2: (None, None)}, None)
        self.assertEqual(out, {})

    def test_synthetic_zero_supersedes_no_fallback(self):
        out = H({B2: (0.0, 0.0)}, {B2: (5.0, 0.40)})
        self.assertNotIn(B2, out)

    def test_seam_mix_per_block_sources(self):
        main = {B1: (None, None), B2: (3.0, 0.21), B3: (1.5, 0.10)}
        phys = {B1: (4.0, 0.28), B2: (2.9, 0.20), B3: (1.4, 0.09)}
        out = H(main, phys)
        self.assertEqual(out[B1]["source"], "recorded")
        self.assertEqual(out[B2]["source"], "synthetic")
        self.assertEqual(out[B3]["source"], "synthetic")
        self.assertAlmostEqual(out[B1]["kwh"], 4.0)
        self.assertAlmostEqual(out[B2]["kwh"], 3.0)

    def test_physical_only_meter_all_recorded(self):
        main = {B1: (None, None), B2: (None, None)}
        phys = {B1: (4.0, 0.28), B2: (2.0, 0.14)}
        out = H(main, phys)
        self.assertEqual({k: v["source"] for k, v in out.items()},
                         {B1: "recorded", B2: "recorded"})


if __name__ == "__main__":
    unittest.main()
