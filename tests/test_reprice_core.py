"""
Phase-1 Δ1: the pure reprice core (reprice.py) — the one pricing model.
Covers the account-type EV resolver, uncapped byte-identity, over-cap peak, the boundary
blend, settled-cost reconciliation (house = plug), the all-EV edge, and the reconciliation
invariants (Σ seg kWh == grid, Σ seg cost == settled).
"""
import os, sys, unittest
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import reprice
import pricing_segments as ps

OFF, PEAK = 0.05493, 0.323092
CAPPED_RATES = {"house_off": OFF, "house_day": PEAK, "ev_off": OFF, "ev_peak": PEAK}
FLAT = lambda r: {"house_off": r, "house_day": r, "ev_off": r, "ev_peak": r}
BS, BE = "2026-08-15T02:00:00", "2026-08-15T02:30:00"


def _sum_kwh(b): return round(sum(s.kwh for s in b["segments"]), 6)
def _sum_cost(b): return round(sum(s.kwh * s.inc_rate for s in b["segments"]), 6)


class TestResolver(unittest.TestCase):
    def test_iog_uses_dispatch_clipped(self):
        r = reprice.resolve_ev_energy(reprice.IOG_CAPPED, grid_kwh=3.0,
                                      dispatch_ev_kwh=5.0, physical_ev_kwh=9.9)
        self.assertEqual(r["source"], "dispatch")
        self.assertEqual(r["clipped"], 3.0)      # grid-clipped for billing
        self.assertEqual(r["unclipped"], 5.0)    # full draw for carbon

    def test_non_iog_uses_physical(self):
        r = reprice.resolve_ev_energy(reprice.NON_IOG, grid_kwh=3.0, physical_ev_kwh=2.0)
        self.assertEqual(r["source"], "physical")
        self.assertEqual((r["clipped"], r["unclipped"]), (2.0, 2.0))

    def test_no_ev(self):
        r = reprice.resolve_ev_energy(reprice.NON_IOG, grid_kwh=3.0)
        self.assertEqual((r["source"], r["clipped"], r["unclipped"]), (None, 0.0, 0.0))


class TestRepriceBlock(unittest.TestCase):
    def _ev(self, clip, unclip=None, src="dispatch"):
        return {"source": src, "clipped": clip, "unclipped": clip if unclip is None else unclip}

    def test_uncapped_single_rate_reconciles(self):
        b = reprice.reprice_block(block_start=BS, block_end=BE, grid_kwh=3.0,
                                  ev=self._ev(2.0), rates=FLAT(OFF), in_off_peak_window=True)
        self.assertAlmostEqual(b["rate"], OFF, places=6)
        self.assertAlmostEqual(_sum_kwh(b), 3.0, places=6)
        self.assertAlmostEqual(_sum_cost(b), round(3.0 * OFF, 6), places=6)
        self.assertTrue(all(abs(s.inc_rate - OFF) < 1e-9 for s in b["segments"]))

    def test_over_cap_ev_peak_house_day(self):
        # out of window, over cap (boundary before this slot → ev_frac 0)
        b = reprice.reprice_block(block_start="2026-08-15T18:00:00", block_end="2026-08-15T18:30:00",
                                  grid_kwh=3.0, ev=self._ev(2.0), rates=CAPPED_RATES,
                                  in_off_peak_window=False, boundary=("2026-08-15T06:00:00", 1.0))
        self.assertEqual(b["bands"]["ev"], "peak")
        self.assertEqual(b["bands"]["house"], "day")
        self.assertAlmostEqual(b["devices"]["ev"]["rate"], PEAK, places=6)
        self.assertAlmostEqual(b["devices"]["house"]["rate"], PEAK, places=6)

    def test_bump_inherited_is_peak_no_freebie_no_cap(self):
        # BUMP eligibility (finalised peak): EV peak, house day out of window, boundary ignored
        # (a bump never enters the cap). Inherited, not re-derived.
        b = reprice.reprice_block(block_start="2026-08-15T18:00:00", block_end="2026-08-15T18:30:00",
                                  grid_kwh=3.0, ev=self._ev(2.0), rates=CAPPED_RATES,
                                  in_off_peak_window=False, ev_eligibility=reprice.BUMP,
                                  boundary=("2026-08-15T20:00:00", 1.0))   # boundary AFTER → ignored
        self.assertEqual(b["bands"]["ev"], "peak")
        self.assertEqual(b["bands"]["house"], "day")
        self.assertAlmostEqual(b["devices"]["ev"]["rate"], PEAK, places=6)

    def test_smart_within_cap_freebie_offpeak(self):
        # SMART eligibility out of window, within cap → EV off-peak AND house freebie off-peak.
        b = reprice.reprice_block(block_start="2026-08-15T18:00:00", block_end="2026-08-15T18:30:00",
                                  grid_kwh=3.0, ev=self._ev(2.0), rates=CAPPED_RATES,
                                  in_off_peak_window=False, ev_eligibility=reprice.SMART,
                                  boundary=None)                            # no cap breach → within
        self.assertEqual(b["bands"]["ev"], "off_peak")
        self.assertEqual(b["bands"]["house"], "off_peak")                  # freebie

    def test_boundary_block_blends(self):
        b = reprice.reprice_block(block_start="2026-08-15T04:30:00", block_end="2026-08-15T05:00:00",
                                  grid_kwh=3.0, ev=self._ev(2.0), rates=CAPPED_RATES,
                                  in_off_peak_window=False, boundary=("2026-08-15T04:30:00", 0.5))
        self.assertEqual(b["bands"]["ev"], "mixed")                 # off + peak in one block
        evrate = b["devices"]["ev"]["rate"]
        self.assertTrue(OFF < evrate < PEAK)                        # blended EV rate
        self.assertAlmostEqual(_sum_kwh(b), 3.0, places=6)

    def test_settled_cost_is_truth_house_is_plug(self):
        # anchor a settled cost that differs from the rate-implied total; EV cost unchanged,
        # house absorbs, Σ cost == settled exactly.
        settled = 0.75
        b = reprice.reprice_block(block_start="2026-08-15T18:00:00", block_end="2026-08-15T18:30:00",
                                  grid_kwh=3.0, ev=self._ev(2.0), rates=CAPPED_RATES,
                                  in_off_peak_window=False, boundary=("2026-08-15T06:00:00", 1.0),
                                  settled_cost=settled)
        self.assertAlmostEqual(b["cost"], settled, places=6)
        self.assertAlmostEqual(_sum_cost(b), settled, places=6)     # invariant: Σ cost == settled
        self.assertAlmostEqual(b["devices"]["ev"]["cost"] + b["devices"]["house"]["cost"], settled, places=6)
        self.assertAlmostEqual(b["devices"]["ev"]["cost"], round(2.0 * PEAK, 6), places=6)  # EV authoritative

    def test_all_ev_block_ev_absorbs(self):
        b = reprice.reprice_block(block_start=BS, block_end=BE, grid_kwh=2.0,
                                  ev=self._ev(2.0), rates=FLAT(OFF), in_off_peak_window=True,
                                  settled_cost=0.20)
        self.assertAlmostEqual(_sum_cost(b), 0.20, places=6)
        self.assertAlmostEqual(b["devices"]["house"]["kwh"], 0.0, places=6)

    def test_carbon_keeps_unclipped(self):
        b = reprice.reprice_block(block_start=BS, block_end=BE, grid_kwh=3.0,
                                  ev=self._ev(3.0, unclip=6.0), rates=FLAT(OFF),
                                  in_off_peak_window=True, generation_kwh=2.5)
        self.assertAlmostEqual(b["carbon"]["ev_unclipped_kwh"], 6.0, places=6)   # unclipped for carbon
        self.assertAlmostEqual(b["devices"]["ev"]["kwh"], 3.0, places=6)         # clipped for billing
        self.assertAlmostEqual(b["carbon"]["generation_kwh"], 2.5, places=6)

    def test_invariants_hold_everywhere(self):
        for iow, bnd in [(True, None), (False, ("2026-08-15T06:00:00", 1.0)),
                         (False, ("2026-08-15T04:30:00", 0.4))]:
            b = reprice.reprice_block(block_start="2026-08-15T04:30:00", block_end="2026-08-15T05:00:00",
                                      grid_kwh=3.0, ev=self._ev(2.0), rates=CAPPED_RATES,
                                      in_off_peak_window=iow, boundary=bnd, settled_cost=0.6)
            self.assertAlmostEqual(_sum_kwh(b), 3.0, places=6)
            # segment Σcost reconciles to sub-penny (rate stored at 6dp → ≤~1e-6 jitter);
            # the bundle 'cost' and the device costs are exact by construction.
            self.assertLess(abs(_sum_cost(b) - 0.6), 1e-4)
            self.assertAlmostEqual(b["cost"], 0.6, places=6)
            self.assertAlmostEqual(b["devices"]["ev"]["cost"] + b["devices"]["house"]["cost"], 0.6, places=6)


if __name__ == "__main__":
    unittest.main()
