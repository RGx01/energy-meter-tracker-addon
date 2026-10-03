"""
The day table's Total Import on a slot where the EV fold applies (3 Oct 2026).

With a physical EV meter on an Intelligent tariff, the EV line shows the dispatch kWh
wherever a dispatch segment exists (H6b). The difference between metered and dispatch is
pushed into Direct import, so the grid total is unchanged. But the slot's Total Import
started from the house remainder BEFORE the fold and added the EV AFTER it, so it read
remainder + dispatch EV, off by (metered − dispatch). Direct + EV + devices still added up
to the meter. The figures below have the shape of a production half-hour: meter 0.766
(settled), EV meter 0.670, dispatch 0.660; Total Import showed 0.756.

Display only: the stored data, the bill and the day's footer were right throughout. The
fold tests fail on the unpatched tree; the guard (no fold) passes on both.
"""
import json, os, re, sys, unittest
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import energy_charts as ec

MAIN = "electricity_main"
OFF = 0.05493


def _block(hh, *, meter, remainder, ev_segment, ev_metered, battery):
    segs = [{"kwh": ev_segment, "inc_rate": OFF, "exc_rate": round(OFF / 1.05, 6),
             "band": "off_peak", "attribution": "ev"},
            {"kwh": round(meter - ev_segment, 6), "inc_rate": OFF, "exc_rate": round(OFF / 1.05, 6),
             "band": "off_peak", "attribution": "house"}]
    return {"start": "2026-09-29T%02d:%s:00" % (hh // 2, "30" if hh % 2 else "00"), "meters": {
        MAIN: {"meta": {}, "standing_charge": 0.0, "channels": {"import": {
            "kwh": meter, "kwh_remainder": remainder, "cost": round(meter * OFF, 6),
            "rate": OFF, "rate_exc": round(OFF / 1.05, 6), "segments": segs}}},
        "ev_charger": {"meta": {"sub_meter": True, "device": "EV charger"},
                       "channels": {"import": {"kwh": ev_metered, "kwh_grid": ev_metered,
                                               "cost": round(ev_metered * OFF, 6), "rate": OFF}}},
        "house_battery": {"meta": {"sub_meter": True, "device": "Battery"},
                          "channels": {"import": {"kwh": battery, "kwh_grid": battery,
                                                  "cost": round(battery * OFF, 6), "rate": OFF}}}}}


def _render(blocks, *, fold):
    day = [(hh, b) for hh, b in blocks]
    html = ec.build_day_chart_html(
        "2026-09-29", day, {MAIN: "#1f77b4", "ev_charger": "#e377c2", "house_battery": "#2ca02c"},
        block_minutes=30, currency="£", bill_rounding=True,
        ev_fold_meter=("ev_charger" if fold else None), ev_label=("EV" if fold else None))
    m = re.search(r'<script type="application/json" id="data_[^"]+">(.*?)</script>', html, re.S)
    return json.loads(m.group(1))


class TheSlotTotal(unittest.TestCase):

    def _slot(self, d, hh):
        parts = {k: d["meters"][k]["y"][hh] for k in d["meters"] if not d["meters"][k].get("is_export")}
        return d["ti_kwh"][hh], d["ti_cost"][hh], parts

    def test_metered_above_dispatch(self):
        """The production half-hour: 0.766 on the meter, 0.670 metered EV, 0.660 dispatched."""
        d = _render([(27, _block(27, meter=0.766, remainder=0.0955, ev_segment=0.660,
                                 ev_metered=0.670, battery=0.0005))], fold=True)
        ti, tc, parts = self._slot(d, 27)
        self.assertAlmostEqual(ti, 0.766, places=6)
        self.assertAlmostEqual(sum(parts.values()), 0.766, places=6)   # it agrees with its parts
        self.assertAlmostEqual(tc, 0.766 * OFF, places=6)

    def test_metered_below_dispatch(self):
        """13:00 in production: 0.779 on the meter, 0.580 metered EV, 0.620 dispatched (showed 0.819)."""
        d = _render([(26, _block(26, meter=0.779, remainder=0.1982, ev_segment=0.620,
                                 ev_metered=0.580, battery=0.0008))], fold=True)
        ti, tc, parts = self._slot(d, 26)
        self.assertAlmostEqual(ti, 0.779, places=6)
        self.assertAlmostEqual(sum(parts.values()), 0.779, places=6)

    def test_without_the_fold_nothing_moves(self):
        """GUARD: no EV fold (no physical EV meter folded) — the total was already right."""
        d = _render([(27, _block(27, meter=0.766, remainder=0.0955, ev_segment=0.660,
                                 ev_metered=0.670, battery=0.0005))], fold=False)
        ti, _tc, _parts = self._slot(d, 27)
        self.assertAlmostEqual(ti, 0.766, places=6)


if __name__ == "__main__":
    unittest.main()
