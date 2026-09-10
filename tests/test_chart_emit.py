"""
Phase-1 Δ3: chart_emit.day_rate_series is the ONE place "what rate applies each half-hour"
lives for presentation — the chart only plots it. Reads rates (never band labels): EV shows
the priced rate where it charged, off-peak when idle, holds peak to the noon reset once the
cap breaks; bump peaks show but don't latch; non-IOG days get no override (None series).
"""
import os, sys, unittest
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import chart_emit

OFF, PEAK, BLEND = 0.05493, 0.323092, 0.12


def _blk(rate, evsegs=None, rate_ev=None):
    imp = {"rate": rate}
    if evsegs is not None:
        imp["segments"] = evsegs
    if rate_ev is not None:
        imp["rate_ev"] = rate_ev
    return {"meters": {"electricity_main": {"channels": {"import": imp}}}}


def _ev(kwh, rate, band):
    return {"kwh": kwh, "inc_rate": rate, "band": band, "attribution": "ev"}


class TestChartEmit(unittest.TestCase):
    def test_non_iog_no_override(self):
        db = [(0, _blk(OFF)), (20, _blk(PEAK))]     # no EV anywhere
        s = chart_emit.day_rate_series(db, slots=48, block_minutes=30)
        self.assertTrue(all(v is None for v in s["ev"]))
        self.assertTrue(all(v is None for v in s["house"]))

    def test_capped_off_blend_peak_hold_reset(self):
        db = [(2, _blk(OFF, [_ev(2.0, OFF, "off_peak")])),                 # within cap
              (4, _blk(BLEND, [_ev(1.0, OFF, "off_peak"), _ev(1.0, PEAK, "peak")])),  # boundary
              (5, _blk(PEAK, [_ev(2.0, PEAK, "peak")]))]                   # over cap
        # over_cap is the authoritative held-peak signal: the cap is EXCEEDED from the
        # boundary slot (4) through to the noon reset (24). chart_emit latches on this
        # alone -- never on an inferred peak band.
        oc = [4 <= hh < 24 for hh in range(48)]
        s = chart_emit.day_rate_series(db, slots=48, block_minutes=30, over_cap=oc)
        self.assertAlmostEqual(s["ev"][2], OFF, places=4)
        self.assertTrue(OFF < s["ev"][4] < PEAK)          # blended boundary
        self.assertAlmostEqual(s["ev"][5], PEAK, places=4)
        self.assertAlmostEqual(s["ev"][10], PEAK, places=4)   # HELD across idle to noon
        self.assertAlmostEqual(s["ev"][23], PEAK, places=4)
        self.assertAlmostEqual(s["ev"][24], OFF, places=4)    # noon reset → off-peak
        self.assertAlmostEqual(s["house"][5], PEAK, places=4) # house line = main rate

    def test_house_line_uses_tou_over_artefact(self):
        # A mis-priced / 0-kWh idle block stores off-peak, but the schedule TOU says peak:
        # the house line shows the TOU (peak), not the stored artefact. A GENUINE transition
        # blend (stored strictly between the schedule off and peak) is preserved.
        db = [(2, _blk(OFF, [_ev(2.0, OFF, "off_peak")])),   # a charge (main loop runs)
              (20, _blk(OFF)),                                  # daytime idle, stored off (artefact)
              (21, _blk(BLEND))]                                # transition block
        tou = [None] * 48
        tou[2], tou[20], tou[21] = OFF, PEAK, PEAK
        s = chart_emit.day_rate_series(db, slots=48, block_minutes=30, capped=True, house_tou=tou)
        self.assertAlmostEqual(s["house"][2],  OFF,   places=4)   # TOU off-peak (window)
        self.assertAlmostEqual(s["house"][20], PEAK,  places=4)   # artefact corrected to TOU peak
        self.assertAlmostEqual(s["house"][21], BLEND, places=4)   # transition blend preserved

    def test_precap_ev_tracks_house(self):
        # Pre-cap (capped=False): no cap model. EV shows its charged rate where charging and
        # TRACKS the house TOU line when idle -- house and EV coincide except where an actual
        # charge prices EV lower (a daytime SMART charge). No held-peak, no noon reset.
        db = [(2,  _blk(OFF,  [_ev(2.0, OFF,  "off_peak")])),   # overnight off-peak charge
              (22, _blk(PEAK, [_ev(1.0, OFF,  "off_peak")])),   # DAYTIME smart charge: EV off / house peak
              (30, _blk(PEAK)),                                  # idle daytime  -> tracks house = peak
              (46, _blk(OFF))]                                   # idle off window -> tracks house = off
        s = chart_emit.day_rate_series(db, slots=48, block_minutes=30, capped=False)
        self.assertAlmostEqual(s["ev"][2],  OFF,  places=4)
        self.assertAlmostEqual(s["house"][22], PEAK, places=4)
        self.assertAlmostEqual(s["ev"][22], OFF,  places=4)     # daytime smart charge diverges to off-peak
        self.assertAlmostEqual(s["ev"][30], PEAK, places=4)     # idle tracks house TOU (not latched off)
        self.assertAlmostEqual(s["ev"][46], OFF,  places=4)     # idle tracks house TOU (not latched peak)

    def test_capped_idle_within_cap_diverges_from_house(self):
        # Capped day, idle daytime BEFORE any cap break: EV = off-peak (a smart charge here
        # would be within cap) while the house line sits at peak TOU -- the deliberate divergence.
        db = [(2, _blk(OFF, [_ev(2.0, OFF, "off_peak")])), (30, _blk(PEAK))]
        s = chart_emit.day_rate_series(db, slots=48, block_minutes=30, capped=True)
        self.assertAlmostEqual(s["house"][30], PEAK, places=4)
        self.assertAlmostEqual(s["ev"][30],    OFF,  places=4)

    def test_ev_fallback_flags_guessed_slots(self):
        # ev_fallback marks slots where the EV rate is the off/held GUESS (no EV segment),
        # NOT a priced EV rate. The day chart uses this to keep a real dispatch-derived rate
        # (a corrected / late-attributed slot) instead of the guess -- line follows the bar.
        db = [(2, _blk(OFF, [_ev(2.0, OFF, "off_peak")])),   # priced EV segment -> NOT fallback
              (5, _blk(PEAK))]                                # priced PEAK, NO ev segment -> fallback
        s = chart_emit.day_rate_series(db, slots=48, block_minutes=30)
        self.assertIn("ev_fallback", s)
        self.assertFalse(s["ev_fallback"][2])
        self.assertTrue(s["ev_fallback"][5])
        self.assertAlmostEqual(s["ev"][5], OFF, places=4)

    def test_bump_shows_peak_but_does_not_latch(self):
        db = [(2, _blk(OFF, [_ev(2.0, OFF, "off_peak")])),
              (30, _blk(PEAK, [_ev(1.0, PEAK, "off_peak")])),   # bump: peak rate, off_peak band
              (31, _blk(PEAK, []))]                             # idle after bump
        s = chart_emit.day_rate_series(db, slots=48, block_minutes=30)
        self.assertAlmostEqual(s["ev"][30], PEAK, places=4)    # bump → peak
        self.assertAlmostEqual(s["ev"][31], OFF, places=4)     # not latched (no real break)


    def test_peak_slot_without_cap_exceedance_does_not_latch(self):
        # Regression (4.5.7): a lone PEAK-priced early slot that is NOT a genuine 6-hour-cap
        # exceedance (a mis-priced schedule slot, or a settled ~0-kWh peak) must NOT hold the
        # EV line to noon. With over_cap all-False (no cap break), idle slots stay off-peak.
        # This is the 10/09 05:30 and 04/09 04:30 "peak held to noon" bug.
        db = [(2, _blk(OFF, [_ev(2.0, OFF, "off_peak")])),   # genuine overnight off-peak charge
              (9, _blk(PEAK, [_ev(0.001, PEAK, "peak")])),   # lone peak blip, no cap break
              (11, _blk(PEAK))]                               # idle daytime after the blip
        oc = [False] * 48                                    # cap never exceeded this day
        s = chart_emit.day_rate_series(db, slots=48, block_minutes=30, capped=True, over_cap=oc)
        self.assertAlmostEqual(s["ev"][9], PEAK, places=4)   # the blip shows its own peak tick
        self.assertAlmostEqual(s["ev"][10], OFF, places=4)   # NOT latched -> off-peak baseline
        self.assertAlmostEqual(s["ev"][20], OFF, places=4)   # still off-peak later in the day

    def test_synthetic_ev_overlay_and_house_tou_fill(self):
        # Synthetic EV (no priced EV segments): the dispatch overlay (ev_slot_rate) feeds the
        # EV line so it never drops to None on idle slots, and the house line follows the TOU
        # across idle / stale near-zero off-peak slots (a battery IOG-SMB day).
        db = [(4, _blk(OFF)), (12, _blk(OFF))]         # overnight + a stale off-peak DAYTIME block
        tou = [OFF if h <= 10 else PEAK for h in range(48)]
        ev_slot = [0.0] * 48
        for h in (4, 5, 6):
            ev_slot[h] = OFF                            # overnight dispatch, from the overlay
        s = chart_emit.day_rate_series(db, slots=48, block_minutes=30, capped=True,
                                       house_tou=tou, ev_slot_rate=ev_slot)
        self.assertAlmostEqual(s["ev"][4], OFF, places=4)       # charged (overlay) off-peak
        self.assertAlmostEqual(s["ev"][30], OFF, places=4)      # idle EV baseline off-peak
        self.assertTrue(all(v is not None for v in s["ev"]))    # never drops to None
        self.assertAlmostEqual(s["house"][12], PEAK, places=4)  # daytime → TOU, not the stale off
        self.assertAlmostEqual(s["house"][4],  OFF,  places=4)  # within-cap dispatch → freebee off


if __name__ == "__main__":
    unittest.main()
