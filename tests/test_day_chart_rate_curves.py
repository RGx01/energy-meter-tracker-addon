"""
BL-27 charts: the day chart draws the EV and house rate LINES from the RATES the four-rate
pricing already resolved (main import rate = house/grid rate; the EV-attributed segment /
imp_rate_ev = EV rate) — never from band labels, which are unreliable in live data (a bump
charge is priced peak but labelled off_peak). IOG has two rates, off-peak and peak, plus a
single blended block where the cap breaks mid-block.

  House — the main import rate each slot: off-peak in the guaranteed window and during the
          within-cap dispatch freebie, peak otherwise (incl. bump/boost). Every house line
          (Direct import + house devices) shares it, so they agree.
  EV    — the EV rate where it charged (off-peak dispatch, peak bump, blended boundary);
          off-peak when idle; once the cap genuinely BREAKS (an EV-peak band), peak is held
          to the noon reset.
Non-IOG tariffs (the EV never draws) are untouched — byte-identical.
"""

import json
import re
import unittest
import energy_charts as ec

MAIN = "electricity_main"
OFF, PEAK = 0.05493, 0.323092


def _seg(kwh, rate, band, attr):
    return {"kwh": kwh, "inc_rate": rate, "exc_rate": round(rate / 1.05, 6),
            "band": band, "attribution": attr}


def _block(hh, segs, ev_kwh=0.0, batt_kwh=0.0):
    tk = sum(s["kwh"] for s in segs)
    tc = sum(s["kwh"] * s["inc_rate"] for s in segs)
    rate = (tc / tk) if tk > 1e-9 else OFF                 # realistic blended block rate
    imp = {"kwh": tk, "cost": round(tc, 6), "rate": round(rate, 6),
           "rate_exc": round(rate / 1.05, 6), "segments": segs}
    ev_kwh = sum(s["kwh"] for s in segs if s["attribution"] == "ev") or ev_kwh
    meters = {MAIN: {"meta": {}, "standing_charge": 0.0, "channels": {"import": imp}}}
    if ev_kwh:
        meters["ev_charger"] = {"meta": {"sub_meter": True, "device": "EV charger"},
                                "channels": {"import": {"kwh": ev_kwh, "kwh_grid": ev_kwh,
                                                        "cost": 0.1, "rate": 0.2}}}
    if batt_kwh:
        meters["house_battery"] = {"meta": {"sub_meter": True, "device": "Battery"},
                                   "channels": {"import": {"kwh": batt_kwh, "kwh_grid": batt_kwh,
                                                           "cost": 0.1, "rate": 0.2}}}
    return {"start": f"2026-08-15T{hh // 2:02d}:{'30' if hh % 2 else '00'}:00", "meters": meters}


def _render(day_blocks):
    html = ec.build_day_chart_html(
        "2026-08-15", day_blocks,
        {MAIN: "#1f77b4", "ev_charger": "#e377c2", "house_battery": "#ff7f0e"},
        block_minutes=30, currency="£", bill_rounding=True)
    m = re.search(r'<script type="application/json" id="data_[^"]+">(.*?)</script>', html, re.S)
    return json.loads(m.group(1))["meters"]


class TestDayChartRateCurves(unittest.TestCase):

    def _capped_day(self):
        # over-cap day: off-peak dispatch (in window) → boundary block (EV mixed) → over-cap
        # peak held across idle slots to the noon reset.
        b = []
        b.append((0, _block(0, [_seg(2.0, OFF, "off_peak", "ev"), _seg(0.5, OFF, "off_peak", "house")])))
        b.append((1, _block(1, [_seg(2.0, OFF, "off_peak", "ev"), _seg(0.5, OFF, "off_peak", "house")])))
        b.append((2, _block(2, [_seg(1.0, OFF, "off_peak", "ev"), _seg(1.0, PEAK, "peak", "ev"),
                                _seg(0.5, PEAK, "day", "house")])))              # boundary (mixed)
        b.append((3, _block(3, [_seg(2.0, PEAK, "peak", "ev"), _seg(0.5, PEAK, "day", "house")])))
        for hh in (4, 5, 6, 20):
            b.append((hh, _block(hh, [_seg(0.5, PEAK, "day", "house")])))        # idle EV, house peak
        b.append((24, _block(24, [_seg(0.5, PEAK, "day", "house")])))            # noon: EV idle → reset
        return b

    def test_ev_hold_peak_to_noon_and_blend(self):
        ev = _render(self._capped_day())["ev_charger"]["rate"]
        self.assertAlmostEqual(ev[0], OFF, places=5)
        self.assertTrue(OFF < ev[2] < PEAK)              # blended boundary block
        self.assertAlmostEqual(ev[3], PEAK, places=5)    # over-cap peak == house peak
        self.assertAlmostEqual(ev[6], PEAK, places=5)    # HELD across idle slots
        self.assertAlmostEqual(ev[20], PEAK, places=5)   # held to noon
        self.assertAlmostEqual(ev[24], OFF, places=5)    # noon reset → off-peak

    def test_house_lines_follow_main_rate(self):
        m = _render(self._capped_day())
        hs = m[MAIN]["rate"]
        self.assertAlmostEqual(hs[0], OFF, places=5)     # in window
        self.assertAlmostEqual(hs[3], PEAK, places=5)    # over-cap day rate
        self.assertAlmostEqual(hs[20], PEAK, places=5)

    def test_bump_pushes_ev_to_peak_without_holding(self):
        # A bump/boost out of window: priced PEAK but labelled off_peak (no cap break). EV
        # shows peak for THAT block only; it must not latch peak across later idle slots.
        b = [(0, _block(0, [_seg(2.0, OFF, "off_peak", "ev"), _seg(0.5, OFF, "off_peak", "house")])),
             (30, _block(30, [_seg(1.0, PEAK, "off_peak", "ev"), _seg(0.5, PEAK, "off_peak", "house")])),
             (31, _block(31, [_seg(0.5, PEAK, "day", "house")]))]     # idle EV after the bump
        ev = _render(b)["ev_charger"]["rate"]
        self.assertAlmostEqual(ev[30], PEAK, places=5)   # bump → peak
        self.assertAlmostEqual(ev[31], OFF, places=5)    # not latched (no real cap break)

    def test_freebie_out_of_window_dispatch_is_offpeak(self):
        # EV dispatch out of window within cap → whole block off-peak (freebie) → house off-peak.
        b = [(0, _block(0, [_seg(2.0, OFF, "off_peak", "ev"), _seg(0.5, OFF, "off_peak", "house")])),
             (40, _block(40, [_seg(1.5, OFF, "off_peak", "ev"), _seg(0.5, OFF, "off_peak", "house")]))]
        m = _render(b)
        self.assertAlmostEqual(m[MAIN]["rate"][40], OFF, places=5)          # house freebie
        self.assertAlmostEqual(m["ev_charger"]["rate"][40], OFF, places=5)  # EV off-peak dispatch

    def test_house_lines_agree_and_ev_offpeak_uncapped(self):
        b = [(0, _block(0, [_seg(2.0, OFF, "off_peak", "ev"), _seg(0.4, OFF, "off_peak", "house")], batt_kwh=0.4)),
             (20, _block(20, [_seg(0.6, PEAK, "day", "house")], batt_kwh=0.6))]
        m = _render(b)
        di = m[MAIN]["rate"]; bt = m["house_battery"]["rate"]; ev = m["ev_charger"]["rate"]
        n = min(len(di), len(bt))
        self.assertEqual([round(x, 6) for x in di[:n]], [round(x, 6) for x in bt[:n]])  # agree
        self.assertTrue(all(abs(v - PEAK) > 1e-6 for v in ev))     # EV never peak (uncapped)
        self.assertAlmostEqual(di[0], OFF, places=5)
        self.assertAlmostEqual(di[20], PEAK, places=5)

    def test_non_iog_untouched(self):
        # No EV draw anywhere → gate off → line keeps the raw stored rate.
        b = [(0, _block(0, [_seg(1.0, 0.2145, "agile", "house")]))]
        self.assertNotIn(OFF, [round(v, 4) for v in _render(b)[MAIN]["rate"] if v])


if __name__ == "__main__":
    unittest.main()
