"""
H6: the billing-summary breakdown goes hybrid for a has-EV-meter account by folding the
physical EV sub-meter line back into the Direct remainder, then carving a single 'EV' line
(synthetic post-seam, recorded pre-seam). The fold-back only moves energy BETWEEN the EV
line and the remainder, so Total Import and total_cost stay byte-identical. Non-EV
sub-meters (battery) are untouched; a no-EV-meter account is unchanged.
"""
import os, sys, unittest
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import energy_charts as ec

R = 0.30
S1 = "2026-07-08T00:00:00"   # post-seam slot


def _summary():
    def chan(k, c): return {R: {"kwh": k, "cost": c, "read_start": None, "read_end": None}}
    def tot(k, c, sub): return {"kwh": k, "cost": c, "is_submeter": sub,
                                "read_start": None, "read_end": None}
    return {
        "total_cost": 4.80,
        "meters": {
            "electricity_main / Import": chan(10.0, 3.00),
            "Zappi EV Charger / Import": chan(4.0, 1.20),
            "Battery / Import":          chan(2.0, 0.60),
        },
        "totals": {
            "electricity_main / Import": tot(10.0, 3.00, False),
            "Zappi EV Charger / Import": tot(4.0, 1.20, True),
            "Battery / Import":          tot(2.0, 0.60, True),
        },
        "meter_meta": {
            "electricity_main / Import": {"device": "Home", "is_submeter": False},
            "Zappi EV Charger / Import": {"device": "Zappi EV Charger", "is_submeter": True},
            "Battery / Import":          {"device": "Battery", "is_submeter": True},
        },
    }


def _total_import(sm):
    return (round(sum(t["kwh"] for k, t in sm["totals"].items() if k.endswith("/ Import")), 3),
            round(sum(t["cost"] for k, t in sm["totals"].items() if k.endswith("/ Import")), 2))


class TestHybridEvBilling(unittest.TestCase):

    def test_fold_preserves_total(self):
        sm = _summary()
        before = _total_import(sm)
        ec._fold_ev_submeter_into_remainder(sm, ["Zappi EV Charger"])
        self.assertNotIn("Zappi EV Charger / Import", sm["totals"])   # folded away
        self.assertIn("Battery / Import", sm["totals"])               # non-EV sub kept
        self.assertEqual(_total_import(sm), before)                   # Total Import invariant
        # remainder absorbed the EV line
        self.assertAlmostEqual(sm["totals"]["electricity_main / Import"]["kwh"], 14.0, places=3)

    def test_hybrid_injection_byte_identical_total(self):
        sm = _summary()
        tc_before = sm["total_cost"]
        ti_before = _total_import(sm)
        ev_map = {S1: {"kwh": 4.0, "cost": 1.20, "rate": R}}   # hybrid EV = 4.0 (e.g. synthetic)
        ok = ec._inject_ev_breakdown_into_summary(
            sm, [{"start": S1}], ev_map, label="EV", fold_devices=["Zappi EV Charger"])
        self.assertTrue(ok)
        self.assertIn("EV / Import", sm["totals"])                    # one hybrid EV line
        self.assertNotIn("Zappi EV Charger / Import", sm["totals"])   # physical superseded
        self.assertIn("Battery / Import", sm["totals"])               # battery untouched
        self.assertEqual(sm["total_cost"], tc_before)                 # BILL total byte-identical
        self.assertEqual(_total_import(sm), ti_before)                # Total Import invariant
        self.assertAlmostEqual(sm["totals"]["EV / Import"]["kwh"], 4.0, places=3)

    def test_no_ev_meter_account_unchanged(self):
        # no fold_devices + a real sub-meter present -> original bail (byte-identical)
        sm = _summary()
        snap = {k: dict(v) for k, v in sm["totals"].items()}
        ok = ec._inject_ev_breakdown_into_summary(
            sm, [{"start": S1}], {S1: {"kwh": 4.0, "cost": 1.2, "rate": R}}, fold_devices=None)
        self.assertFalse(ok)
        self.assertEqual({k: dict(v) for k, v in sm["totals"].items()}, snap)


if __name__ == "__main__":
    unittest.main()
