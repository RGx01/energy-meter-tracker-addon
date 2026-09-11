"""Cost-alignment: Usage Stats' Direct/House cost must be derived the SAME way as
compute_period_net (Billing) — the sub-meter total subtracted from the main total ONCE per
day, clamped at zero at the DAY level. A per-rate clamp floored each band independently, so on
a multi-band SMB day where a sub-meter's grid cost exceeds the main in one band, Usage Stats
over-counted (the few-pence Billing↔Usage drift). This guards the per-day reducer."""
import os, sys, types, unittest

# Stub energy_engine_io (server imports it at module load), like the other server tests.
eio = types.ModuleType("energy_engine_io"); eio.load_json = lambda *a, **k: {}
sys.modules.setdefault("energy_engine_io", eio)
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import server  # tests/server.py -> ../web/server.py (run_tests.sh symlink)
from block_store import BlockStore

OFF, PK = 0.05493, 0.323092


def _row(meter_id, is_sub, rate, cost, kwh, *, grid=None):
    return {"block_start": "2026-09-04T02:30:00", "meter_id": meter_id,
            "is_sub_meter": 1 if is_sub else 0, "parent_meter_id": None,
            "imp_kwh": kwh, "imp_kwh_grid": grid, "imp_kwh_remainder": None,
            "imp_rate": rate, "imp_cost": cost, "imp_cost_remainder": None,
            "exp_kwh": 0.0, "exp_cost": 0.0, "imp_kwh_ev": None, "imp_cost_ev": None,
            "standing_charge": 0.0, "carbon_g": None, "interpolated": 0, "billing_day": 3}


class TestUsageBillingMultiband(unittest.TestCase):
    def _direct(self, rows):
        dd = server._aggregate_block_rows(rows, lambda bs: "2026-09-04",
                                          standing_scope="bucket", billing_day_fallback=3)
        vals = server._bucket_to_row_values(dd["2026-09-04"])
        return vals["meters"]["electricity_main"]["imp_cost"]

    def test_direct_matches_per_day_subtraction_not_per_rate(self):
        # Multi-band day: main off_peak £0.10 + main peak £0.30 (total £0.40).
        # A battery/EV sub-meter drew £0.15 of grid in the OFF-PEAK band — MORE than the main's
        # own off-peak remainder (£0.10). Per-day: max(0, 0.40 - 0.15) = £0.25. The old per-rate
        # clamp gave max(0, 0.10-0.15)=0 + 0.30 = £0.30, dropping the £0.05 excess.
        rows = [_row("electricity_main", False, OFF, 0.10, 1.82),
                _row("electricity_main", False, PK,  0.30, 0.93),
                _row("ev_charger",       True,  OFF, 0.15, 2.73, grid=2.73)]
        self.assertAlmostEqual(self._direct(rows), 0.25, places=4)   # per-day (== compute_period_net)
        self.assertNotAlmostEqual(self._direct(rows), 0.30, places=4)  # NOT the old per-rate value

    def test_single_rate_day_is_unchanged(self):
        # One band → per-rate and per-day are identical (flat / Economy-7 stay byte-identical).
        rows = [_row("electricity_main", False, OFF, 0.40, 7.28),
                _row("ev_charger",       True,  OFF, 0.15, 2.73, grid=2.73)]
        self.assertAlmostEqual(self._direct(rows), 0.25, places=4)   # max(0, 0.40 - 0.15)

    def test_negative_main_credit_survives(self):
        # Agile plunge credit: a genuinely negative main total is preserved, not clamped to 0.
        rows = [_row("electricity_main", False, -0.02, -0.05, 2.5)]
        self.assertAlmostEqual(self._direct(rows), -0.05, places=4)



class TestComputePeriodNetPlunge(unittest.TestCase):
    """compute_period_net (Billing) preserves a negative main total (Agile plunge CREDIT)
    rather than clamp it to 0 — matching the Usage-Stats per-day reducer on every tariff."""

    def _store(self):
        st = BlockStore(":memory:")
        st.insert_config_period({"meters": {"electricity_main": {"meta": {
            "billing_day": 1, "block_minutes": 30, "timezone": "UTC",
            "currency_symbol": "£", "currency_code": "GBP", "site": "Home"}}}})
        return st

    def _ins(self, st, slot, imp_cost, exp_cost=0.0, sc=0.0):
        st._conn.execute(
            "INSERT INTO blocks (block_start, block_end, meter_id, config_period_id, "
            "imp_kwh, imp_rate, imp_cost, exp_kwh, exp_cost, standing_charge) "
            "VALUES (?,?,?,?,?,?,?,?,?,?)",
            (slot, slot, "electricity_main", 1, 2.0, None, imp_cost, 0.0, exp_cost, sc))
        st._conn.commit()

    def test_negative_main_credit_survives(self):
        st = self._store()
        self._ins(st, "2026-01-15T02:00:00", imp_cost=-0.05, sc=0.30)   # plunge credit
        net = st.compute_period_net("2026-01-15T00:00:00", "2026-01-16T00:00:00", "UTC")
        self.assertAlmostEqual(net, 0.25, places=2)   # round(-0.05 + 0.30, 2), NOT clamped to 0.30

    def test_positive_unaffected(self):
        st = self._store()
        self._ins(st, "2026-01-15T02:00:00", imp_cost=0.10, sc=0.30)
        self.assertAlmostEqual(
            st.compute_period_net("2026-01-15T00:00:00", "2026-01-16T00:00:00", "UTC"), 0.40, places=2)


if __name__ == "__main__":
    unittest.main()
