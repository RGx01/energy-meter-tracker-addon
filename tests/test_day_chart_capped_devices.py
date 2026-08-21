"""
test_day_chart_capped_devices.py — BL-27: on a CAPPED block the day chart drives the
EV/house split from the block SEGMENTS — the physical EV device carries the dispatch EV
bands (off-peak within cap, peak beyond), house sub-devices sit at the house band rate, and
'Direct import' is the house-segment remainder (cleanly off-peak). So the over-cap peak
lands on the EV, the house stays off-peak (no metered-vs-dispatch residual), and slot totals
are untouched. A block with no EV-peak segment keeps the column path (byte-identical uncapped).
"""

import os
import re
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import energy_charts as ec

MAIN = "electricity_main"
OFF, DAY, PEAK = 0.05493, 0.323092, 0.30


def _main_import(ev_off, ev_peak, house_off, house_day):
    segs = []
    if ev_off:   segs.append({"kwh": ev_off,  "inc_rate": OFF,  "exc_rate": None, "band": "off_peak", "attribution": "ev"})
    if ev_peak:  segs.append({"kwh": ev_peak, "inc_rate": PEAK, "exc_rate": None, "band": "peak",     "attribution": "ev"})
    if house_off: segs.append({"kwh": house_off, "inc_rate": OFF, "exc_rate": None, "band": "off_peak", "attribution": "house"})
    if house_day: segs.append({"kwh": house_day, "inc_rate": DAY, "exc_rate": None, "band": "day",      "attribution": "house"})
    tot_k = ev_off + ev_peak + house_off + house_day
    tot_c = ev_off*OFF + ev_peak*PEAK + house_off*OFF + house_day*DAY
    return {"kwh": tot_k, "kwh_remainder": house_off + house_day,
            "cost": round(tot_c, 6), "rate": round(tot_c/tot_k, 6), "segments": segs}


def _meters(main_import, zappi_grid=None, batt_grid=None):
    m = {MAIN: {"meta": {}, "channels": {"import": main_import}}}
    if zappi_grid is not None:
        m["zappi"] = {"meta": {"sub_meter": True, "meter_type": "ev"},
                      "channels": {"import": {"kwh": zappi_grid, "kwh_grid": zappi_grid,
                                              "cost": round(zappi_grid*OFF, 6), "rate": OFF}}}
    if batt_grid is not None:
        m["batt"] = {"meta": {"sub_meter": True, "meter_type": "battery"},
                     "channels": {"import": {"kwh": batt_grid, "kwh_grid": batt_grid,
                                             "cost": round(batt_grid*OFF, 6), "rate": OFF}}}
    return m


class TestDayChartCappedDevices(unittest.TestCase):

    def test_ev_gets_bands_house_stays_offpeak(self):
        # over-cap block: EV all peak (3.5), house all off-peak (2.5); a Zappi + battery.
        imp = _main_import(ev_off=0, ev_peak=3.5, house_off=2.5, house_day=0)
        meters = _meters(imp, zappi_grid=3.4, batt_grid=1.0)
        out = ec._day_segment_split(imp, meters)
        self.assertIsNotNone(out)
        # EV BAR = the physical charger's METERED grid kWh (3.4), NOT the dispatch segment
        # (3.5); its COST/RATE come from the dispatch split (3.5 @ peak → cost != kWh×rate).
        self.assertAlmostEqual(out["zappi"]["kwh"], 3.4, places=5)          # metered bar
        self.assertAlmostEqual(out["zappi"]["rate"], PEAK, places=5)        # dispatch rate
        self.assertAlmostEqual(out["zappi"]["cost"], round(3.5 * PEAK, 6), places=5)  # dispatch cost
        # battery at the house band (off-peak); Direct import = grid − metered EV − battery
        self.assertAlmostEqual(out["batt"]["rate"], OFF, places=5)
        self.assertAlmostEqual(out[MAIN]["rate"], OFF, places=5)
        self.assertAlmostEqual(out[MAIN]["kwh"], 6.0 - 3.4 - 1.0, places=5)  # grid − metered EV − batt
        # reconciles to the grid: EV + battery + direct == total, cost too
        tot_k = out["zappi"]["kwh"] + out["batt"]["kwh"] + out[MAIN]["kwh"]
        tot_c = out["zappi"]["cost"] + out["batt"]["cost"] + out[MAIN]["cost"]
        self.assertAlmostEqual(tot_k, imp["kwh"], places=4)
        self.assertAlmostEqual(tot_c, imp["cost"], places=4)

    def test_dispatch_over_attribution_never_negatives_the_remainder(self):
        # regression (08-19 04:30): dispatch EV (2.31) + battery (0.263) > grid (2.461). With
        # the metered EV bar (2.18) the Direct-import remainder stays >= 0 — never a phantom
        # negative bar below the axis.
        imp = _main_import(ev_off=0, ev_peak=2.31, house_off=0.151, house_day=0)
        out = ec._day_segment_split(imp, _meters(imp, zappi_grid=2.18, batt_grid=0.2627))
        self.assertAlmostEqual(out["zappi"]["kwh"], 2.18, places=4)
        self.assertGreaterEqual(out[MAIN]["kwh"], 0.0)                       # not negative
        self.assertAlmostEqual(out[MAIN]["kwh"], 2.461 - 2.18 - 0.2627, places=3)

    def test_no_ev_peak_keeps_column_path(self):
        # uncapped-style block (EV off-peak == house) → helper returns None, column path kept.
        imp = _main_import(ev_off=2.0, ev_peak=0, house_off=1.0, house_day=0)
        self.assertIsNone(ec._day_segment_split(imp, _meters(imp, zappi_grid=2.0)))

    def test_no_physical_ev_meter_returns_none(self):
        # capped block but no physical EV sub-meter → leave to the synthetic dispatch path.
        imp = _main_import(ev_off=0, ev_peak=3.5, house_off=2.5, house_day=0)
        self.assertIsNone(ec._day_segment_split(imp, _meters(imp, batt_grid=1.0)))

    def test_rendered_direct_import_has_no_peak(self):
        # end-to-end: the rendered day chart's 'Direct import' shows off-peak, not peak.
        imp = _main_import(ev_off=0, ev_peak=3.5, house_off=2.5, house_day=0)
        html = ec.build_day_chart_html(
            "2026-08-15", [(4, {"start": "2026-08-15T02:00:00",
                                "meters": _meters(imp, zappi_grid=3.4)})],
            {MAIN: "#1f77b4", "zappi": "#e377c2"}, block_minutes=30, currency="£")
        t = re.sub("<[^>]+>", " ", html).split('{"x_labels"')[0]
        _d0 = t.find("Direct import")
        direct = t[_d0:t.find(chr(8627), _d0 + 1)]   # up to the next ↳ (device boundary)
        self.assertIn("0.0549", direct)          # house off-peak
        self.assertNotIn("0.3000", direct)        # no EV peak leaked into the house


if __name__ == "__main__":
    unittest.main()