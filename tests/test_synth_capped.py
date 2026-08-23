"""
test_synth_capped.py — prove the synthetic capped fixture is TRUSTWORTHY before task-16
reader tests lean on it: its stored columns and segments agree, the cap cases are present,
and device attribution reconciles to the grid.
"""

import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))   # tests/ for synth_capped
from block_store import BlockStore
import pricing_segments as ps
import synth_capped as sc


class TestSynthCappedFixture(unittest.TestCase):

    def setUp(self):
        self.st = BlockStore(":memory:")
        self.starts = sc.build_capped_day(self.st)

    def _segs(self, start):
        return [ps.Segment(**r) for r in self.st.get_block_segments(start, sc.MAIN)]

    def test_columns_and_segments_agree(self):
        for start in self.starts:
            r = self.st._conn.execute(
                "SELECT imp_kwh, imp_cost, imp_kwh_ev, imp_cost_ev FROM blocks "
                "WHERE block_start=? AND meter_id=?", (start, sc.MAIN)).fetchone()
            segs = self._segs(start)
            self.assertAlmostEqual(ps.total_kwh(segs), r["imp_kwh"], places=5)
            self.assertAlmostEqual(ps.total_cost(segs), r["imp_cost"], places=5)
            self.assertAlmostEqual(ps.attribution_kwh(segs, "ev"), r["imp_kwh_ev"] or 0, places=5)
            self.assertAlmostEqual(ps.attribution_cost(segs, "ev"), r["imp_cost_ev"] or 0, places=5)

    def test_cap_cases_present(self):
        bands = set()
        for start in self.starts:
            for s in self._segs(start):
                bands.add((s.attribution, s.band))
        # the 4-rate matrix is exercised: EV off-peak & peak, house off-peak & day
        self.assertIn(("ev", "off_peak"), bands)
        self.assertIn(("ev", "peak"), bands)
        self.assertIn(("house", "off_peak"), bands)
        self.assertIn(("house", "day"), bands)

    def test_boundary_block_has_four_segments(self):
        segs = self._segs("2026-09-01T16:00:00")
        self.assertEqual(len(segs), 4)                  # EV-off, EV-peak, house-off, house-day

    def test_device_attribution_reconciles(self):
        # over-cap block with EV + battery: attribute the physical devices onto segments.
        start = "2026-09-01T20:00:00"
        segs = self._segs(start)
        devs = [{"meter_id": r["meter_id"],
                 "attribution": "ev" if r["meter_id"] == sc.EV else "house",
                 "grid_kwh": r["imp_kwh"]}
                for r in self.st._conn.execute(
                    "SELECT meter_id, imp_kwh FROM blocks WHERE block_start=? "
                    "AND meter_id IN (?,?)", (start, sc.EV, sc.BATT)).fetchall()]
        res = ps.attribute_devices(segs, devs)
        tot_k = sum(v["kwh"] for v in res["devices"].values()) + res["remainder"]["kwh"]
        tot_c = sum(v["cost"] for v in res["devices"].values()) + res["remainder"]["cost"]
        self.assertAlmostEqual(tot_k, ps.total_kwh(segs), places=5)
        self.assertAlmostEqual(tot_c, ps.total_cost(segs), places=5)
        # EV charger priced on the EV segment cost; battery on the house rate.
        self.assertAlmostEqual(res["devices"][sc.EV]["cost"],
                               ps.attribution_cost(segs, "ev"), places=5)
        self.assertAlmostEqual(res["devices"][sc.BATT]["rate"],
                               ps.attribution_rate(segs, "house"), places=5)

    def test_on_top_of_highgrove_breaches_cap(self):
        # the whole point: re-price REAL Highgrove under a 3h cap so its real 4.5–6h
        # dispatch days genuinely breach — real bulk, real dispatch, now capped.
        hg = "/sessions/sleepy-bold-tesla/mnt/uploads/blocks 180826.db"
        if not os.path.exists(hg):
            self.skipTest("Highgrove DB not present")
        import tempfile
        dst = os.path.join(tempfile.mkdtemp(), "capped.db")
        sc.cap_highgrove(hg, dst, cap_hours=3.0)
        st = BlockStore(dst)
        self.assertGreater(st._conn.execute("SELECT COUNT(*) n FROM blocks").fetchone()["n"],
                           100000)                                  # real bulk intact
        # the cap breached: real blocks now carry an EV-peak segment
        peak = st._conn.execute("SELECT COUNT(*) n FROM block_segments "
                                "WHERE attribution='ev' AND band='peak'").fetchone()["n"]
        self.assertGreater(peak, 0)
        # every re-priced block still reconciles to its stored figures
        for r in st._conn.execute("SELECT block_start, imp_kwh, imp_cost FROM blocks "
                                  "WHERE imp_kwh_ev IS NOT NULL AND meter_id='electricity_main'"
                                  ).fetchall():
            segs = [ps.Segment(**x) for x in st.get_block_segments(r["block_start"], sc.MAIN)]
            if segs:
                self.assertAlmostEqual(ps.total_cost(segs), r["imp_cost"] or 0, places=4)


if __name__ == "__main__":
    unittest.main()
