"""
H2: _apply_hybrid_ev_to_summary_rows folds a physical EV sub-meter's chart series into the
one hybrid 'EV' series (synthetic post-seam, recorded pre-seam) and moves only the DIFFERENCE
to/from the Direct segment, so every bucket total — and the bill — is invariant. Pins the
invariant, the pre-seam no-op, and the one-identity relabel.
"""
import os, sys, unittest
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import test_server as _ts
server = _ts.server
A = server._apply_hybrid_ev_to_summary_rows


def _row(y, m, d, house_k, house_c, ev_k=None, ev_c=None):
    meters = {"electricity_main": {"imp_kwh": house_k, "imp_cost": house_c}}
    if ev_k is not None:
        meters["ev_charger"] = {"imp_kwh": ev_k, "imp_cost": ev_c}
    return {"year": y, "month": m, "day": d, "meters": meters}


def _total(row):
    return (round(sum(mm["imp_kwh"] for mm in row["meters"].values()), 6),
            round(sum(mm["imp_cost"] for mm in row["meters"].values()), 6))


class TestApplyHybridEvCharts(unittest.TestCase):

    def _mlist(self):
        return [{"id": "electricity_main", "label": "Direct", "color": "#111"},
                {"id": "ev_charger", "label": "Zappi EV Charger", "color": "#0a0"}]

    def test_total_invariant_post_seam(self):
        # physical 5.0/0.35 -> synthetic 4.0/0.28: EV drops, Direct absorbs the difference
        r = _row(2026, 8, 1, house_k=10.0, house_c=1.00, ev_k=5.0, ev_c=0.35)
        before = _total(r)
        ml = self._mlist()
        changed = A([r], ml, "ev_charger", {"2026-08-01": {"kwh": 4.0, "cost": 0.28}})
        self.assertTrue(changed)
        self.assertEqual(_total(r), before)                       # bucket total invariant
        self.assertAlmostEqual(r["meters"]["ev_charger"]["imp_kwh"], 4.0)
        self.assertAlmostEqual(r["meters"]["electricity_main"]["imp_kwh"], 11.0)  # +1.0 absorbed

    def test_pre_seam_no_move_only_relabel(self):
        # hybrid == physical (recorded) -> no total shift, series just relabelled
        r = _row(2026, 6, 1, house_k=10.0, house_c=1.00, ev_k=5.0, ev_c=0.35)
        before = _total(r)
        ml = self._mlist()
        A([r], ml, "ev_charger", {"2026-06-01": {"kwh": 5.0, "cost": 0.35}})
        self.assertEqual(_total(r), before)
        self.assertAlmostEqual(r["meters"]["electricity_main"]["imp_kwh"], 10.0)  # unchanged
        self.assertAlmostEqual(r["meters"]["ev_charger"]["imp_kwh"], 5.0)

    def test_one_ev_identity_relabel(self):
        r = _row(2026, 6, 1, 10.0, 1.0, ev_k=5.0, ev_c=0.35)
        ml = self._mlist()
        A([r], ml, "ev_charger", {"2026-06-01": {"kwh": 5.0, "cost": 0.35}})
        ev = [m for m in ml if m["id"] == "ev_charger"][0]
        self.assertEqual(ev["label"], "EV")
        # H6c: the EV series keeps its DEVICE colour (relabel only) — not forced to purple —
        # so it stays at the palette index that came before (accessibility: distinct from house).
        self.assertEqual(ev["color"], "#0a0")

    def test_synthetic_bucket_with_no_physical_draw(self):
        # dispatch EV in a bucket where the physical meter recorded nothing
        r = _row(2026, 8, 2, house_k=8.0, house_c=0.80, ev_k=None, ev_c=None)
        before = _total(r)
        A([r], self._mlist(), "ev_charger", {"2026-08-02": {"kwh": 3.0, "cost": 0.21}})
        self.assertEqual(_total(r), before)                       # still invariant
        self.assertAlmostEqual(r["meters"]["ev_charger"]["imp_kwh"], 3.0)
        self.assertAlmostEqual(r["meters"]["electricity_main"]["imp_kwh"], 5.0)  # 8-3

    def test_synthetic_over_grid_floors_direct_at_zero(self):
        # B4: battery-assist slot — synthetic EV (2.31) exceeds grid available (Direct 0 +
        # metered 2.18); the excess is battery-sourced, so Direct must floor at 0, EV caps to
        # the grid-available 2.18, and the total is preserved (no spurious negative bar).
        r = _row(2026, 8, 19, house_k=0.0, house_c=0.0, ev_k=2.18, ev_c=0.12)
        before = _total(r)
        A([r], self._mlist(), "ev_charger", {"2026-08-19": {"kwh": 2.31, "cost": 0.126}})
        self.assertGreaterEqual(r["meters"]["electricity_main"]["imp_kwh"], 0.0)   # Direct not negative
        self.assertAlmostEqual(r["meters"]["electricity_main"]["imp_kwh"], 0.0)
        self.assertAlmostEqual(r["meters"]["ev_charger"]["imp_kwh"], 2.18)          # capped to grid
        self.assertEqual(_total(r), before)                                        # total invariant

    def test_slot_key_fn(self):
        r = {"slot": "2026-08-01T23:00:00",
             "meters": {"electricity_main": {"imp_kwh": 6.0, "imp_cost": 0.6},
                        "ev_charger": {"imp_kwh": 2.0, "imp_cost": 0.14}}}
        before = _total(r)
        A([r], self._mlist(), "ev_charger",
          {"2026-08-01T23:00:00": {"kwh": 1.8, "cost": 0.12}},
          key_fn=lambda _r: _r.get("slot"))
        self.assertEqual(_total(r), before)
        self.assertAlmostEqual(r["meters"]["ev_charger"]["imp_kwh"], 1.8)


if __name__ == "__main__":
    unittest.main()