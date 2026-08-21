"""
Synthetic EV is the SPINE (design decision). In Usage Stats (`_aggregate_usage`), for a
fully-segmented range the EV portion comes from the dispatch-derived 'ev' SEGMENT — not a
physical EV clamp's metered draw. The clamp is superseded (posterity, not counted); non-dispatch
(granny) charging stays in house; non-EV sub-meters (heat pump) keep counting. Reconciliation:
synthetic-EV + non-EV devices + house == grid import.

Note: the shipped fixture is under-segmented so it never enters BL-27; these build a FULLY
segmented block to exercise the new path directly.
"""
import os, sys, unittest
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import test_server as _ts          # applies import stubs + imports server
server     = _ts.server
BlockStore = _ts.BlockStore

T  = "2025-06-01T02:00:00"
T2 = "2025-06-01T02:30:00"


def _store(*, ev_metered, ev_dispatch, hp_metered, grid=10.0, rate=0.30,
           ev_band="off_peak", house_band="off_peak", ev_rate=None, house_rate=None):
    ev_rate = ev_rate if ev_rate is not None else rate
    house_rate = house_rate if house_rate is not None else rate
    st = BlockStore(":memory:")
    with st._conn:
        cp = st._conn.execute(
            "INSERT INTO config_periods (effective_from, billing_day, block_minutes, timezone, "
            "currency_symbol, currency_code) VALUES ('2024-01-01T00:00:00',1,30,'UTC','£','GBP')"
        ).lastrowid
        st._conn.execute("INSERT INTO meters (config_period_id, meter_id, is_sub_meter, meter_type) "
                         "VALUES (?, 'electricity_main', 0, '')", (cp,))
        if ev_metered is not None:
            st._conn.execute("INSERT INTO meters (config_period_id, meter_id, is_sub_meter, "
                             "meter_type, parent_meter_id) VALUES (?, 'ev_charger', 1, 'ev_charger', "
                             "'electricity_main')", (cp,))
        if hp_metered is not None:
            st._conn.execute("INSERT INTO meters (config_period_id, meter_id, is_sub_meter, "
                             "meter_type, parent_meter_id) VALUES (?, 'heat_pump', 1, 'heat_pump', "
                             "'electricity_main')", (cp,))
        subs = (ev_metered or 0.0) + (hp_metered or 0.0)
        # main block — fully segmented so BL-27 runs
        st._conn.execute(
            "INSERT INTO blocks (block_start, block_end, meter_id, config_period_id, imp_kwh, "
            "imp_kwh_grid, imp_kwh_remainder, imp_rate, imp_cost) VALUES (?,?, 'electricity_main', "
            "?, ?, ?, ?, ?, ?)", (T, T2, cp, grid, grid, grid - subs, rate, round(grid*rate, 6)))
        house_seg = round(grid - ev_dispatch, 6)
        for seq, (kwh, rt, band, attr) in enumerate([
                (ev_dispatch, ev_rate, ev_band, "ev"),
                (house_seg,   house_rate, house_band, "house")]):
            if kwh <= 0:
                continue
            st._conn.execute(
                "INSERT INTO block_segments (block_start, meter_id, channel, seq, kwh, inc_rate, "
                "exc_rate, band, attribution) VALUES (?, 'electricity_main', 'import', ?, ?, ?, ?, ?, ?)",
                (T, seq, kwh, rt, round(rt/1.05, 6), band, attr))
        if ev_metered is not None:
            st._conn.execute(
                "INSERT INTO blocks (block_start, block_end, meter_id, config_period_id, imp_kwh, "
                "imp_kwh_grid, imp_rate, imp_cost) VALUES (?,?, 'ev_charger', ?, ?, ?, ?, ?)",
                (T, T2, cp, ev_metered, ev_metered, ev_rate, round(ev_metered*ev_rate, 6)))
        if hp_metered is not None:
            st._conn.execute(
                "INSERT INTO blocks (block_start, block_end, meter_id, config_period_id, imp_kwh, "
                "imp_kwh_grid, imp_rate, imp_cost) VALUES (?,?, 'heat_pump', ?, ?, ?, ?, ?)",
                (T, T2, cp, hp_metered, hp_metered, house_rate, round(hp_metered*house_rate, 6)))
    st._conn.commit()
    return st


def _cfg(ev=True, hp=True):
    m = {"electricity_main": {"meta": {"timezone": "UTC"}}}
    if ev: m["ev_charger"] = {"meta": {"sub_meter": True, "meter_type": "ev_charger", "device": "EV"}}
    if hp: m["heat_pump"] = {"meta": {"sub_meter": True, "meter_type": "heat_pump", "device": "Heat pump"}}
    return {"meters": m}


def _agg(st, cfg):
    return server._aggregate_usage(st, cfg, "2025-06-01T00:00:00", "2025-06-02T00:00:00", "UTC")


class TestSyntheticEvAuthority(unittest.TestCase):
    def test_active_sensor_superseded_by_dispatch_granny_to_house(self):
        # physical clamp measured 4.0 (incl 0.5 granny); dispatch says 3.5; heat pump 2.0
        st = _store(ev_metered=4.0, ev_dispatch=3.5, hp_metered=2.0)
        r = _agg(st, _cfg())
        sm = r["sub_meters"]
        self.assertAlmostEqual(sm["ev_dispatch"]["imp_kwh"], 3.5, places=6)   # EV = segment
        self.assertTrue(sm["ev_dispatch"].get("derived"))
        self.assertAlmostEqual(sm["heat_pump"]["imp_kwh"], 2.0, places=6)      # non-EV counted
        # physical clamp superseded → dropped from the breakdown (no 0-kWh ghost); its block
        # data remains in the DB, just not surfaced.
        self.assertNotIn("ev_charger", sm)
        # house absorbs the 0.5 granny: grid 10 - ev 3.5 - hp 2.0 = 4.5
        self.assertAlmostEqual(r["house_imp_kwh"], 4.5, places=3)
        # reconciliation: synthetic EV + heat pump + house == grid
        self.assertAlmostEqual(sm["ev_dispatch"]["imp_kwh"] + sm["heat_pump"]["imp_kwh"]
                               + r["house_imp_kwh"], 10.0, places=3)

    def test_retired_ev_device_gap_closed(self):
        # no EV sub-meter block at all (retired) but dispatch charged 3.5 → must show as EV
        st = _store(ev_metered=None, ev_dispatch=3.5, hp_metered=None)
        r = _agg(st, _cfg(ev=False, hp=False))
        sm = r["sub_meters"]
        self.assertAlmostEqual(sm["ev_dispatch"]["imp_kwh"], 3.5, places=6)
        self.assertAlmostEqual(r["house_imp_kwh"], 6.5, places=3)             # 10 - 3.5
        self.assertAlmostEqual(sm["ev_dispatch"]["imp_kwh"] + r["house_imp_kwh"], 10.0, places=3)

    def test_two_band_ev_at_offpeak_rate(self):
        # capped-style: EV at off-peak 0.07, house at peak 0.30. dispatch EV 4 kWh.
        st = _store(ev_metered=4.0, ev_dispatch=4.0, hp_metered=None,
                    grid=6.0, ev_rate=0.07, house_rate=0.30, ev_band="off_peak", house_band="peak")
        r = _agg(st, _cfg(hp=False))
        sm = r["sub_meters"]
        self.assertAlmostEqual(sm["ev_dispatch"]["imp_kwh"], 4.0, places=6)
        self.assertAlmostEqual(sm["ev_dispatch"]["imp_cost"], round(4.0*0.07, 6), places=5)  # off-peak
        self.assertAlmostEqual(r["house_imp_kwh"], 2.0, places=3)             # 6 - 4
        self.assertAlmostEqual(r["house_imp_cost"], round(2.0*0.30, 4), places=4)            # peak


if __name__ == "__main__":
    unittest.main()
