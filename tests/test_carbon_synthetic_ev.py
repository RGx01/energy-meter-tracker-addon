"""
Watch #8 — the carbon twin of P4.15. On IOG the EV carbon axis is the SYNTHETIC dispatch figure,
so the house remainder must net the synthetic EV GRID carbon (not the physical charger's carbon),
and the physical EV device is superseded in the breakdown. Proven on a DIVERGENT account (physical
carbon >> dispatch truth — the case the old code floored the house to 0).
"""
import os, sys, unittest
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import test_server as _ts
server     = _ts.server
BlockStore = _ts.BlockStore

T  = "2025-07-01T02:00:00"
Te = "2025-07-01T02:30:00"


def _store():
    st = BlockStore(":memory:")
    with st._conn:
        cp = st._conn.execute(
            "INSERT INTO config_periods (effective_from, billing_day, block_minutes, timezone, "
            "currency_symbol, currency_code) VALUES ('2024-01-01T00:00:00',1,30,'UTC','£','GBP')"
        ).lastrowid
        st._conn.execute("INSERT INTO meters (config_period_id, meter_id, is_sub_meter, meter_type) "
                         "VALUES (?, 'electricity_main', 0, '')", (cp,))
        st._conn.execute("INSERT INTO meters (config_period_id, meter_id, is_sub_meter, meter_type, "
                         "parent_meter_id) VALUES (?, 'ev_charger', 1, 'ev_charger', 'electricity_main')", (cp,))
        # main block: 10 kWh grid @ 200 g/kWh = 2000 g; grid EV clipped = 4 kWh
        st._conn.execute(
            "INSERT INTO blocks (block_start, block_end, meter_id, config_period_id, imp_kwh, "
            "exp_kwh, carbon_g, carbon_intensity_g, imp_kwh_ev) VALUES (?,?, 'electricity_main', "
            "?, 10.0, 0.0, 2000.0, 200.0, 4.0)", (T, Te, cp))
        # physical EV clamp: DIVERGENT inflated carbon (3000 g) vs the 800 g the dispatch says
        st._conn.execute(
            "INSERT INTO blocks (block_start, block_end, meter_id, config_period_id, imp_kwh, "
            "carbon_g) VALUES (?,?, 'ev_charger', ?, 4.0, 3000.0)", (T, Te, cp))
        # completed dispatch: car drew 5 kWh (grid supplied 4 → 1 kWh behind-meter saving)
        st._conn.execute(
            "INSERT INTO dispatch_history (slot_start, kind, energy_kwh, first_seen, last_seen) "
            "VALUES (?, 'completed', 5.0, ?, ?)", (T, T, T))
    st._conn.commit()
    return st


def _cfg():
    return {"meters": {
        "electricity_main": {"meta": {"timezone": "UTC", "block_minutes": 30}},
        "ev_charger": {"meta": {"sub_meter": True, "meter_type": "ev_charger", "device": "EV"}},
    }}


class TestCarbonSyntheticEv(unittest.TestCase):
    def test_house_carbon_nets_synthetic_not_physical(self):
        r = server._aggregate_insights(_store(), _cfg(), "2025-07-01T00:00:00", "2025-07-02T00:00:00")
        ev = r["ev_carbon"]
        self.assertIsNotNone(ev)
        self.assertAlmostEqual(ev["grid_g"], 800.0, places=1)     # 4 kWh × 200
        self.assertAlmostEqual(ev["saving_g"], 200.0, places=1)   # 1 kWh behind-meter × 200
        # house carbon = main import 2000 − synthetic EV grid 800 = 1200 — NOT floored to 0 by the
        # inflated physical 3000 (the old bug)
        self.assertAlmostEqual(r["house_carbon_g"], 1200.0, places=1)
        # physical EV clamp superseded → dropped from the carbon breakdown
        self.assertNotIn("ev_charger", r["sub_meters"])

    def test_non_iog_unchanged_physical_authoritative(self):
        # no dispatch → no synthetic → physical device authoritative (old behaviour)
        st = _store()
        st._conn.execute("DELETE FROM dispatch_history")
        st._conn.commit()
        r = server._aggregate_insights(st, _cfg(), "2025-07-01T00:00:00", "2025-07-02T00:00:00")
        self.assertIsNone(r["ev_carbon"])
        self.assertIn("ev_charger", r["sub_meters"])              # physical kept
        self.assertEqual(r["house_carbon_g"], 0.0)                # 2000 − 3000 physical → floored (unchanged)


if __name__ == "__main__":
    unittest.main()
