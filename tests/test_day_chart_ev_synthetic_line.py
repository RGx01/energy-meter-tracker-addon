"""4.5.7: on a capped (IOG-SMB) day the EV rate LINE must come from the synthetic curve
(off-peak baseline within cap, held-peak on a real over_cap break, noon reset) even when a
PHYSICAL ev_charger device is configured — matching the EV bars, Usage Stats, Insights and
the bill, which are all synthetic/hybrid. The physical device's own stored rate (the stale
plain-TOU value it carries on 0-kWh idle slots) must NOT reach the line on a capped day.
Charging slots (real EV kWh) still keep the bar's rate so line and bar agree.
"""
import json, os, re, sys, unittest
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import energy_charts as ec

MAIN = "electricity_main"
OFF, PEAK = 0.05493, 0.323092


def _seg(kwh, rate, band, attr):
    return {"kwh": kwh, "inc_rate": rate, "exc_rate": round(rate / 1.05, 6),
            "band": band, "attribution": attr}


def _main(hh, segs):
    tk = sum(s["kwh"] for s in segs)
    tc = sum(s["kwh"] * s["inc_rate"] for s in segs)
    rate = (tc / tk) if tk > 1e-9 else PEAK
    return {"meta": {}, "standing_charge": 0.0, "channels": {"import": {
        "kwh": tk, "cost": round(tc, 6), "rate": round(rate, 6),
        "rate_exc": round(rate / 1.05, 6), "segments": segs}}}


def _evdev(kwh, rate):
    return {"meta": {"sub_meter": True, "device": "EV charger"},
            "channels": {"import": {"kwh": kwh, "kwh_grid": kwh,
                                    "cost": round(kwh * rate, 6), "rate": rate}}}


def _blk(hh, segs, ev_dev):
    m = {MAIN: _main(hh, segs)}
    if ev_dev is not None:
        m["ev_charger"] = ev_dev
    return {"start": f"2026-08-15T{hh // 2:02d}:{'30' if hh % 2 else '00'}:00", "meters": m}


class _CapStub:
    """Supplies build_day_chart_html's over_cap read; only cap_day_boundary is called."""
    def __init__(self, by_capday): self._b = by_capday or {}
    def cap_day_boundary(self, ts, tz_name):
        import iog_cap
        return self._b.get(iog_cap.cap_day_key(ts, tz_name))


def _day():
    # slot 2 (01:00): a real overnight off-peak EV charge (main EV segment + device draw)
    # slot 20 (10:00): IDLE daytime — no EV segment; the physical device block sits at PEAK
    #                  (the stale plain-TOU rate it carries at 0 kWh) — the slot under test
    return [
        (2,  _blk(2,  [_seg(2.0, OFF, "off_peak", "ev"), _seg(0.3, OFF, "off_peak", "house")], _evdev(2.0, OFF))),
        (20, _blk(20, [_seg(0.4, PEAK, "day", "house")], _evdev(0.0, PEAK))),
        (47, _blk(47, [_seg(0.4, PEAK, "day", "house")], _evdev(0.0, PEAK))),  # full day of data
    ]


def _render(day_blocks, *, cap_from=None, cap_boundary=None):
    store = _CapStub(cap_boundary) if cap_boundary is not None else None
    html = ec.build_day_chart_html(
        "2026-08-15", day_blocks,
        {MAIN: "#1f77b4", "ev_charger": "#e377c2"},
        block_minutes=30, currency="£", bill_rounding=True,
        ev_fold_meter="ev_charger", ev_label="EV", cap_from=cap_from, store=store)
    m = re.search(r'<script type="application/json" id="data_[^"]+">(.*?)</script>', html, re.S)
    return json.loads(m.group(1))["meters"]


class TestEvSyntheticLine(unittest.TestCase):
    def test_capped_idle_uses_synthetic_baseline_not_device_rate(self):
        # Capped day, no cap break: the idle 10:00 slot must show the off-peak baseline,
        # NOT the physical device's stale PEAK rate.
        ev = _render(_day(), cap_from="2026-08-01")["ev_charger"]["rate"]
        self.assertAlmostEqual(ev[2],  OFF, places=4)   # real charge → its rate (bar agrees)
        self.assertAlmostEqual(ev[20], OFF, places=4)   # idle → synthetic off-peak baseline
        self.assertAlmostEqual(ev[30], OFF, places=4)   # still off-peak later (no break)

    def test_capped_over_cap_holds_peak_on_idle_slot(self):
        # A genuine cap break (boundary at slot 4) holds the idle EV line at peak to the noon
        # reset, then off-peak after — driven by over_cap, not the device rate.
        oc = {"2026-08-14": ("2026-08-15T02:00:00", 0.0)}   # cap exceeded from slot 4 (02:00 UTC)
        ev = _render(_day(), cap_from="2026-08-01", cap_boundary=oc)["ev_charger"]["rate"]
        self.assertAlmostEqual(ev[20], PEAK, places=4)  # held peak to noon
        self.assertAlmostEqual(ev[30], OFF,  places=4)  # after the noon reset → off-peak

    def test_non_capped_keeps_device_rate(self):
        # Pre-SMB (non-capped) behaviour is UNCHANGED: the physical device's own rate is
        # preserved on the idle slot (the guard only relaxes on capped days).
        ev = _render(_day(), cap_from=None)["ev_charger"]["rate"]
        self.assertAlmostEqual(ev[20], PEAK, places=4)


if __name__ == "__main__":
    unittest.main()
