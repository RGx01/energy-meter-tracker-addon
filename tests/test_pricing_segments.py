"""
test_pricing_segments.py — BL-27 pure segment core.

The validation backbone for the segment refactor: because we have no capped multi-device DB,
these synthetic scenarios (uncapped, capped within-cap freebie, capped over-cap, boundary
blend) plus the reconciliation invariant are what prove the model. Also pins the projections
that the legacy imp_* columns become views over.
"""

import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import pricing_segments as ps

OFF, DAY, PEAK = 0.05, 0.30, 0.32          # house off-peak, house day, EV peak (inc)
EV_OFF = 0.05                              # EV off-peak (== house off-peak on IOG)


class TestImportSegments(unittest.TestCase):

    def _reconciles(self, segs, exp_kwh, exp_cost):
        self.assertAlmostEqual(ps.total_kwh(segs), round(exp_kwh, 6), places=5)
        self.assertAlmostEqual(ps.total_cost(segs), round(exp_cost, 6), places=5)
        for s in segs:                      # exc can never exceed inc
            if s.exc_rate is not None:
                self.assertLessEqual(s.exc_rate, s.inc_rate + 1e-9)

    def test_uncapped_same_rate_both_attributions(self):
        # Uncapped: EV and house share the block rate → two segments at that rate.
        segs = ps.import_segments(
            ev_kwh=2.0, house_kwh=1.0,
            house_offpeak_rate=OFF, house_day_rate=OFF,
            ev_offpeak_rate=OFF, ev_peak_rate=OFF)     # all == block rate
        self.assertEqual(len(segs), 2)
        self._reconciles(segs, 3.0, 3.0 * OFF)
        self.assertAlmostEqual(ps.attribution_rate(segs, "ev"), OFF)
        self.assertAlmostEqual(ps.attribution_rate(segs, "house"), OFF)

    def test_capped_within_cap_freebie(self):
        # Within cap, out of window: EV off-peak AND house off-peak (rule-3 freebie).
        segs = ps.import_segments(
            ev_kwh=2.0, house_kwh=1.0,
            house_offpeak_rate=OFF, house_day_rate=DAY,
            ev_offpeak_rate=EV_OFF, ev_peak_rate=PEAK,
            ev_offpeak_frac=1.0, house_offpeak_frac=1.0)
        self._reconciles(segs, 3.0, 2 * EV_OFF + 1 * OFF)
        self.assertEqual({s.band for s in segs}, {"off_peak"})

    def test_capped_over_cap_ev_peak_house_day(self):
        # Over cap, out of window: EV → peak, house freebie withdrawn → day.
        segs = ps.import_segments(
            ev_kwh=2.0, house_kwh=1.0,
            house_offpeak_rate=OFF, house_day_rate=DAY,
            ev_offpeak_rate=EV_OFF, ev_peak_rate=PEAK,
            ev_offpeak_frac=0.0, house_offpeak_frac=0.0)
        self._reconciles(segs, 3.0, 2 * PEAK + 1 * DAY)
        self.assertAlmostEqual(ps.attribution_rate(segs, "ev"), PEAK)
        self.assertAlmostEqual(ps.attribution_rate(segs, "house"), DAY)

    def test_boundary_block_splits_each_portion(self):
        # Cap crosses mid-block: EV and house each split into off + on sub-segments.
        segs = ps.import_segments(
            ev_kwh=2.0, house_kwh=2.0,
            house_offpeak_rate=OFF, house_day_rate=DAY,
            ev_offpeak_rate=EV_OFF, ev_peak_rate=PEAK,
            ev_offpeak_frac=0.5, house_offpeak_frac=0.5)
        self.assertEqual(len(segs), 4)      # EV-off, EV-peak, house-off, house-day
        self._reconciles(segs, 4.0,
                         1 * EV_OFF + 1 * PEAK + 1 * OFF + 1 * DAY)

    def test_house_only_block(self):
        segs = ps.import_segments(ev_kwh=0.0, house_kwh=1.5,
                                  house_offpeak_rate=DAY, house_day_rate=DAY)
        self.assertEqual(len(segs), 1)
        self.assertEqual(segs[0].attribution, "house")
        self._reconciles(segs, 1.5, 1.5 * DAY)


class TestProjections(unittest.TestCase):

    def test_legacy_column_views(self):
        # The imp_* columns become projections: blended rate, EV kWh/cost, exc.
        segs = ps.import_segments(
            ev_kwh=2.0, house_kwh=1.0,
            house_offpeak_rate=DAY, house_day_rate=DAY,
            ev_offpeak_rate=PEAK, ev_peak_rate=PEAK,
            ev_offpeak_frac=0.0, house_offpeak_frac=0.0, vat=0.05)
        self.assertAlmostEqual(ps.attribution_kwh(segs, "ev"), 2.0)
        self.assertAlmostEqual(ps.attribution_cost(segs, "ev"), 2 * PEAK)     # imp_cost_ev
        self.assertAlmostEqual(ps.attribution_rate(segs, "ev"), PEAK)         # imp_rate_ev
        # imp_rate = blended; imp_cost = total
        self.assertAlmostEqual(ps.total_cost(segs), 2 * PEAK + 1 * DAY)
        self.assertAlmostEqual(ps.blended_rate(segs),
                               (2 * PEAK + 1 * DAY) / 3.0, places=5)
        # ex-VAT is per-segment and total exc < total inc
        self.assertLess(ps.total_cost_exc(segs), ps.total_cost(segs))
        self.assertAlmostEqual(ps.total_cost_exc(segs),
                               ps.total_cost(segs) / 1.05, places=4)


class TestSegmentsFromLegacy(unittest.TestCase):
    """The backfill's core: rebuild segments from the legacy imp_* columns so history
    projects to the SAME figures the columns held."""

    def test_split_block_reconstructs_ev_and_house(self):
        # capped: 6 kWh grid £1.88; EV 4 kWh @ 0.32 (£1.28), home 2 kWh @ 0.30 (£0.60).
        segs = ps.segments_from_legacy(
            imp_kwh=6.0, imp_cost=1.88, imp_rate=round(1.88 / 6, 6),
            kwh_ev=4.0, cost_ev=1.28, rate_ev=0.32,
            ev_band="peak", home_band="day", exc_ratio=1 / 1.05)
        self.assertEqual(len(segs), 2)
        self.assertAlmostEqual(ps.attribution_cost(segs, "ev"), 1.28)     # imp_cost_ev
        self.assertAlmostEqual(ps.attribution_rate(segs, "ev"), 0.32)     # imp_rate_ev
        self.assertAlmostEqual(ps.attribution_rate(segs, "house"), 0.30)  # derived home rate
        self.assertAlmostEqual(ps.total_cost(segs), 1.88)                 # imp_cost
        self.assertLess(ps.total_cost_exc(segs), ps.total_cost(segs))     # exc applied

    def test_non_split_block_single_house_segment(self):
        segs = ps.segments_from_legacy(imp_kwh=1.5, imp_cost=0.45, imp_rate=0.30)
        self.assertEqual(len(segs), 1)
        self.assertEqual(segs[0].attribution, "house")
        self.assertAlmostEqual(ps.total_cost(segs), 0.45)
        self.assertEqual(segs[0].band, "standard")     # no band known off-IOG

    def test_zero_kwh_yields_no_segments(self):
        self.assertEqual(ps.segments_from_legacy(imp_kwh=0.0, imp_cost=0.0, imp_rate=0.3), [])


class TestDeviceAttribution(unittest.TestCase):
    """The case we have no DB for: a CAPPED account with a physical EV charger + a battery.
    Every scenario asserts the reconciliation invariant — devices + ev_dispatch + remainder
    == the grid total, kWh and cost — which is what proves the model without real data."""

    def _capped_over_cap(self):
        # 6 kWh grid: EV 4 @ peak, house 2 @ day (over-cap, out of window).
        return ps.import_segments(
            ev_kwh=4.0, house_kwh=2.0,
            house_offpeak_rate=OFF, house_day_rate=DAY,
            ev_offpeak_rate=EV_OFF, ev_peak_rate=PEAK,
            ev_offpeak_frac=0.0, house_offpeak_frac=0.0)

    def _assert_reconciles(self, segs, result):
        tot_k = sum(v["kwh"] for v in result["devices"].values()) + result["remainder"]["kwh"]
        tot_c = sum(v["cost"] for v in result["devices"].values()) + result["remainder"]["cost"]
        if result["ev_dispatch"]:
            tot_k += result["ev_dispatch"]["kwh"]
            tot_c += result["ev_dispatch"]["cost"]
        self.assertAlmostEqual(tot_k, ps.total_kwh(segs), places=5)
        self.assertAlmostEqual(tot_c, ps.total_cost(segs), places=5)

    def test_physical_ev_and_battery_capped(self):
        segs = self._capped_over_cap()          # EV 4@0.32, house 2@0.30 → grid £2.88
        res = ps.attribute_devices(segs, [
            {"meter_id": "zappi", "attribution": "ev", "grid_kwh": 4.05},   # metered ≠ dispatch
            {"meter_id": "battery", "attribution": "house", "grid_kwh": 1.0}])
        self._assert_reconciles(segs, res)
        # EV shown at METERED kWh, priced from the EV segment cost (4 × 0.32 = 1.28).
        self.assertAlmostEqual(res["devices"]["zappi"]["kwh"], 4.05)
        self.assertAlmostEqual(res["devices"]["zappi"]["cost"], 4 * PEAK, places=5)
        # battery = house load → house day rate (0.30).
        self.assertAlmostEqual(res["devices"]["battery"]["rate"], DAY, places=5)
        self.assertIsNone(res["ev_dispatch"])

    def test_sensorless_ev_uses_synthetic_row(self):
        segs = self._capped_over_cap()
        res = ps.attribute_devices(segs, [
            {"meter_id": "battery", "attribution": "house", "grid_kwh": 1.0}])
        self._assert_reconciles(segs, res)
        self.assertIsNotNone(res["ev_dispatch"])                 # no physical EV → synthetic
        self.assertAlmostEqual(res["ev_dispatch"]["kwh"], 4.0)   # the dispatch EV kWh
        self.assertAlmostEqual(res["ev_dispatch"]["rate"], PEAK, places=5)

    def test_two_ev_devices_share_cost_prorata(self):
        segs = self._capped_over_cap()
        res = ps.attribute_devices(segs, [
            {"meter_id": "ev_a", "attribution": "ev", "grid_kwh": 3.0},
            {"meter_id": "ev_b", "attribution": "ev", "grid_kwh": 1.0}])
        self._assert_reconciles(segs, res)
        # EV segment cost 1.28 split 3:1 by metered kWh.
        self.assertAlmostEqual(res["devices"]["ev_a"]["cost"], 1.28 * 0.75, places=5)
        self.assertAlmostEqual(res["devices"]["ev_b"]["cost"], 1.28 * 0.25, places=5)

    def test_uncapped_no_devices_is_pure_remainder(self):
        segs = ps.import_segments(ev_kwh=0.0, house_kwh=3.0,
                                  house_offpeak_rate=OFF, house_day_rate=OFF)
        res = ps.attribute_devices(segs, [])
        self._assert_reconciles(segs, res)
        self.assertEqual(res["devices"], {})
        self.assertIsNone(res["ev_dispatch"])
        self.assertAlmostEqual(res["remainder"]["kwh"], 3.0)


class TestPriceDevicesHybrid(unittest.TestCase):
    """The hybrid physical-device model: a device is shown at its METERED kWh and valued
    at the block's band rate for its attribution. Single-rate blocks reproduce metered ×
    block_rate exactly (byte-identical uncapped); capped blocks apply the band rates. Every
    case asserts devices + remainder == the grid total (the reconciliation invariant)."""

    def _reconciles(self, segs, res):
        tot_k = sum(v["kwh"] for v in res["devices"].values()) + res["remainder"]["kwh"]
        tot_c = sum(v["cost"] for v in res["devices"].values()) + res["remainder"]["cost"]
        self.assertAlmostEqual(tot_k, ps.total_kwh(segs), places=5)
        self.assertAlmostEqual(tot_c, ps.total_cost(segs), places=5)

    def test_single_rate_block_is_metered_times_block_rate(self):
        # Uncapped: EV + house share one rate. A device costs metered × that rate — exactly
        # what its own imp_cost column holds, so the reader is byte-identical uncapped.
        segs = ps.import_segments(ev_kwh=2.0, house_kwh=1.0,
                                  house_offpeak_rate=OFF, house_day_rate=OFF,
                                  ev_offpeak_rate=OFF, ev_peak_rate=OFF)
        res = ps.price_devices_hybrid(segs, [
            {"meter_id": "zappi", "attribution": "ev", "grid_kwh": 2.05},
            {"meter_id": "battery", "attribution": "house", "grid_kwh": 0.5}])
        self._reconciles(segs, res)
        self.assertAlmostEqual(res["devices"]["zappi"]["cost"], 2.05 * OFF, places=5)
        self.assertAlmostEqual(res["devices"]["battery"]["cost"], 0.5 * OFF, places=5)

    def test_capped_block_values_metered_at_band_rate(self):
        # Over-cap: EV metered valued at the EV PEAK rate (not the parent blended), the
        # battery at the house DAY rate — the 4-rate fix, on the device's own metered kWh.
        segs = ps.import_segments(
            ev_kwh=4.0, house_kwh=2.0, house_offpeak_rate=OFF, house_day_rate=DAY,
            ev_offpeak_rate=EV_OFF, ev_peak_rate=PEAK,
            ev_offpeak_frac=0.0, house_offpeak_frac=0.0)
        res = ps.price_devices_hybrid(segs, [
            {"meter_id": "zappi", "attribution": "ev", "grid_kwh": 4.05},   # metered ≠ dispatch
            {"meter_id": "battery", "attribution": "house", "grid_kwh": 1.0}])
        self._reconciles(segs, res)
        self.assertAlmostEqual(res["devices"]["zappi"]["rate"], PEAK, places=5)
        self.assertAlmostEqual(res["devices"]["zappi"]["cost"], 4.05 * PEAK, places=5)
        self.assertAlmostEqual(res["devices"]["battery"]["rate"], DAY, places=5)

    def test_non_dispatch_ev_on_house_only_block_bills_at_house_rate(self):
        # A house-only (no dispatch) block: an EV device that grid-charged here drew at the
        # house/day rate — so it is valued there, never leaking into house.
        segs = ps.import_segments(ev_kwh=0.0, house_kwh=2.0,
                                  house_offpeak_rate=OFF, house_day_rate=DAY,
                                  house_offpeak_frac=0.0)
        res = ps.price_devices_hybrid(segs, [
            {"meter_id": "zappi", "attribution": "ev", "grid_kwh": 1.0}])
        self._reconciles(segs, res)
        self.assertAlmostEqual(res["devices"]["zappi"]["rate"], DAY, places=5)


if __name__ == "__main__":
    unittest.main()
