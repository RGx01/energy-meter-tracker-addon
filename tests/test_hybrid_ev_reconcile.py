"""
H5: cross-seam reconciliation + byte-identity guard for the hybrid EV authority.

The hybrid EV (synthetic post-seam, recorded physical pre-seam) must never move the whole:
per period, EV + non-EV sub-meters + house == grid import. And an account with no dispatch
AND no physical EV meter (non-IOG) must be byte-identical — no EV row anywhere, nothing
leaked into or out of house.
"""
import os, sys, unittest
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import test_server as _ts
server     = _ts.server
BlockStore = _ts.BlockStore

# two main blocks in one local day: pre-seam (no dispatch) then post-seam (dispatch)
PRE, PREe  = "2025-06-01T01:00:00", "2025-06-01T01:30:00"   # imp_kwh_ev NULL  -> recorded EV
POST, POSTe = "2025-06-01T02:00:00", "2025-06-01T02:30:00"  # imp_kwh_ev set   -> synthetic EV


def _seg(st, cp, bs, kwh, rate, attr, seq):
    st._conn.execute(
        "INSERT INTO block_segments (block_start, meter_id, channel, seq, kwh, inc_rate, "
        "exc_rate, band, attribution) VALUES (?, 'electricity_main', 'import', ?, ?, ?, ?, 'flat', ?)",
        (bs, seq, kwh, rate, round(rate/1.05, 6), attr))


def _hybrid_store(rate=0.30):
    st = BlockStore(":memory:")
    with st._conn:
        cp = st._conn.execute(
            "INSERT INTO config_periods (effective_from, billing_day, block_minutes, timezone, "
            "currency_symbol, currency_code) VALUES ('2024-01-01T00:00:00',1,30,'UTC','£','GBP')"
        ).lastrowid
        for mid, sub, mt in [("electricity_main", 0, ""), ("ev_charger", 1, "ev_charger"),
                             ("heat_pump", 1, "heat_pump")]:
            st._conn.execute("INSERT INTO meters (config_period_id, meter_id, is_sub_meter, "
                             "meter_type) VALUES (?,?,?,?)", (cp, mid, sub, mt))
        # PRE-seam: grid 10, physical EV 4.0, heat pump 2.0, no dispatch, imp_kwh_ev NULL
        st._conn.execute("INSERT INTO blocks (block_start, block_end, meter_id, config_period_id, "
            "imp_kwh, imp_kwh_grid, imp_kwh_remainder, imp_rate, imp_cost) VALUES (?,?, "
            "'electricity_main', ?, 10.0, 10.0, 4.0, ?, ?)", (PRE, PREe, cp, rate, round(10*rate, 6)))
        _seg(st, cp, PRE, 10.0, rate, "house", 0)
        st._conn.execute("INSERT INTO blocks (block_start, block_end, meter_id, config_period_id, "
            "imp_kwh, imp_kwh_grid, imp_rate, imp_cost) VALUES (?,?, 'ev_charger', ?, 4.0, 4.0, ?, ?)",
            (PRE, PREe, cp, rate, round(4*rate, 6)))
        st._conn.execute("INSERT INTO blocks (block_start, block_end, meter_id, config_period_id, "
            "imp_kwh, imp_kwh_grid, imp_rate, imp_cost) VALUES (?,?, 'heat_pump', ?, 2.0, 2.0, ?, ?)",
            (PRE, PREe, cp, rate, round(2*rate, 6)))
        # POST-seam: grid 10, synthetic EV 3.5, heat pump 2.0, physical EV 3.8 (superseded)
        st._conn.execute("INSERT INTO blocks (block_start, block_end, meter_id, config_period_id, "
            "imp_kwh, imp_kwh_grid, imp_kwh_remainder, imp_rate, imp_cost, imp_kwh_ev, imp_cost_ev) "
            "VALUES (?,?, 'electricity_main', ?, 10.0, 10.0, 4.2, ?, ?, 3.5, ?)",
            (POST, POSTe, cp, rate, round(10*rate, 6), round(3.5*rate, 6)))
        _seg(st, cp, POST, 3.5, rate, "ev", 0)
        _seg(st, cp, POST, 6.5, rate, "house", 1)
        st._conn.execute("INSERT INTO blocks (block_start, block_end, meter_id, config_period_id, "
            "imp_kwh, imp_kwh_grid, imp_rate, imp_cost) VALUES (?,?, 'ev_charger', ?, 3.8, 3.8, ?, ?)",
            (POST, POSTe, cp, rate, round(3.8*rate, 6)))
        st._conn.execute("INSERT INTO blocks (block_start, block_end, meter_id, config_period_id, "
            "imp_kwh, imp_kwh_grid, imp_rate, imp_cost) VALUES (?,?, 'heat_pump', ?, 2.0, 2.0, ?, ?)",
            (POST, POSTe, cp, rate, round(2*rate, 6)))
    st._conn.commit()
    return st


def _flat_store(rate=0.25):
    """Non-IOG: main only, no EV meter, no imp_kwh_ev, fully segmented."""
    st = BlockStore(":memory:")
    with st._conn:
        cp = st._conn.execute(
            "INSERT INTO config_periods (effective_from, billing_day, block_minutes, timezone, "
            "currency_symbol, currency_code) VALUES ('2024-01-01T00:00:00',1,30,'UTC','£','GBP')"
        ).lastrowid
        st._conn.execute("INSERT INTO meters (config_period_id, meter_id, is_sub_meter, meter_type) "
                         "VALUES (?, 'electricity_main', 0, '')", (cp,))
        for bs, be in [(PRE, PREe), (POST, POSTe)]:
            st._conn.execute("INSERT INTO blocks (block_start, block_end, meter_id, config_period_id, "
                "imp_kwh, imp_kwh_grid, imp_kwh_remainder, imp_rate, imp_cost) VALUES (?,?, "
                "'electricity_main', ?, 8.0, 8.0, 8.0, ?, ?)", (bs, be, cp, rate, round(8*rate, 6)))
            _seg(st, cp, bs, 8.0, rate, "house", 0)
    st._conn.commit()
    return st


def _cfg(ev=True, hp=True):
    m = {"electricity_main": {"meta": {"timezone": "UTC"}}}
    if ev: m["ev_charger"] = {"meta": {"sub_meter": True, "meter_type": "ev_charger", "device": "EV"}}
    if hp: m["heat_pump"] = {"meta": {"sub_meter": True, "meter_type": "heat_pump", "device": "Heat pump"}}
    return {"meters": m}


DAY = ("2025-06-01T00:00:00", "2025-06-02T00:00:00")


class TestHybridEvReconcile(unittest.TestCase):

    def test_usage_reconciles_across_seam(self):
        r = server._aggregate_usage(_hybrid_store(), _cfg(), DAY[0], DAY[1], "UTC")
        sm = r["sub_meters"]
        # EV = recorded pre-seam 4.0 + synthetic post-seam 3.5
        self.assertAlmostEqual(sm["ev_dispatch"]["imp_kwh"], 7.5, places=5)
        self.assertEqual(sm["ev_dispatch"]["label"], "EV")
        self.assertNotIn("ev_charger", sm)                       # physical folded in, no ghost
        self.assertAlmostEqual(sm["heat_pump"]["imp_kwh"], 4.0, places=5)   # non-EV sub kept
        self.assertAlmostEqual(r["house_imp_kwh"], 8.5, places=3)
        # the whole: EV + non-EV subs + house == grid import (20.0)
        self.assertAlmostEqual(sm["ev_dispatch"]["imp_kwh"] + sm["heat_pump"]["imp_kwh"]
                               + r["house_imp_kwh"], 20.0, places=3)

    def test_non_iog_byte_identical(self):
        r = server._aggregate_usage(_flat_store(), _cfg(ev=False, hp=False), DAY[0], DAY[1], "UTC")
        sm = r["sub_meters"]
        self.assertNotIn("ev_dispatch", sm)                      # no EV row invented
        self.assertNotIn("ev_charger", sm)
        self.assertAlmostEqual(r["house_imp_kwh"], 16.0, places=3)   # all grid stays house (8+8)

    def test_resolver_matches_usage_split(self):
        # the shared resolver agrees with what Usage Stats surfaced (same per-block sources)
        H = server._hybrid_ev_by_block
        out = H({PRE: (None, None), POST: (3.5, round(3.5*0.30, 6))},
                {PRE: (4.0, 1.2), POST: (3.8, 1.14)})
        self.assertEqual(out[PRE]["source"], "recorded")
        self.assertEqual(out[POST]["source"], "synthetic")
        self.assertAlmostEqual(out[PRE]["kwh"] + out[POST]["kwh"], 7.5, places=5)


if __name__ == "__main__":
    unittest.main()
