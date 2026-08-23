"""
test_iog_cap.py — the IOG 6-hour cap accumulator (pure math).
Cap = union of COMPLETED dispatch windows per noon→noon (local) cap-day, 6 h.
"""

import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import iog_cap as cap
from kraken_rates import RateSchedule


class TestCapHoursConfigurable(unittest.TestCase):
    """B3: the cap length is live-configurable — settable at startup via IOG_CAP_HOURS and
    respected when CAP_HOURS is changed at runtime (the old float-default bound it at import,
    silently pinning the cap to 6h so the value could never change)."""
    def setUp(self): self._orig = cap.CAP_HOURS
    def tearDown(self): cap.CAP_HOURS = self._orig

    def _slots(self):
        # ~5 charge-hours in one cap-day (10 half-hours, power ~ 2×fullest slot)
        return [(f"2026-08-10T0{h}:{m:02d}:00", f"2026-08-10T0{h}:{m+29:02d}:59", 2.0)
                for h in range(1, 6) for m in (0, 30)]

    def test_runtime_cap_hours_changes_boundary(self):
        slots = self._slots()
        cap.CAP_HOURS = 6.0                      # 5h < 6h → never reaches the cap
        self.assertFalse(any(v["over"] for v in cap.cap_usage(slots, "Europe/London").values()))
        cap.CAP_HOURS = 3.0                      # 5h > 3h → cap reached, boundary set
        u = cap.cap_usage(slots, "Europe/London")
        self.assertTrue(any(v["over"] for v in u.values()))
        b = cap.cap_day_boundaries(slots, "Europe/London")   # no cap_hours arg → live value
        self.assertTrue(any(bs is not None for (bs, bf) in b.values()))

    def test_explicit_cap_hours_still_overrides(self):
        slots = self._slots()
        cap.CAP_HOURS = 6.0
        self.assertTrue(any(v["over"] for v in cap.cap_usage(slots, "Europe/London", cap_hours=3.0).values()))

    def test_env_sets_default(self):
        os.environ["IOG_CAP_HOURS"] = "4"
        try:
            self.assertEqual(cap._env_cap_hours(), 4.0)
        finally:
            os.environ.pop("IOG_CAP_HOURS", None)
        self.assertEqual(cap._env_cap_hours(), 6.0)   # default when unset


class TestCapDayKey(unittest.TestCase):
    def test_before_noon_is_previous_day(self):
        # 10:00 local (UTC, so same) → belongs to the previous noon→noon window
        self.assertEqual(cap.cap_day_key("2026-01-02T10:00:00", "UTC"), "2026-01-01")

    def test_at_or_after_noon_is_same_day(self):
        self.assertEqual(cap.cap_day_key("2026-01-02T12:00:00", "UTC"), "2026-01-02")
        self.assertEqual(cap.cap_day_key("2026-01-02T23:30:00", "UTC"), "2026-01-02")

    def test_bst_local_boundary(self):
        # 10:30 UTC = 11:30 BST (before local noon) → previous cap-day
        self.assertEqual(cap.cap_day_key("2026-07-02T10:30:00", "Europe/London"),
                         "2026-07-01")
        # 11:30 UTC = 12:30 BST (after local noon) → same cap-day
        self.assertEqual(cap.cap_day_key("2026-07-02T11:30:00", "Europe/London"),
                         "2026-07-02")


class TestMergeIntervals(unittest.TestCase):
    def test_overlap_unioned_not_summed(self):
        m = cap.merge_intervals([("2026-01-01T01:00:00", "2026-01-01T02:00:00"),
                                 ("2026-01-01T01:30:00", "2026-01-01T03:00:00")])
        self.assertEqual(m, [("2026-01-01T01:00:00", "2026-01-01T03:00:00")])

    def test_adjacent_merged(self):
        m = cap.merge_intervals([("2026-01-01T01:00:00", "2026-01-01T01:30:00"),
                                 ("2026-01-01T01:30:00", "2026-01-01T02:00:00")])
        self.assertEqual(m, [("2026-01-01T01:00:00", "2026-01-01T02:00:00")])

    def test_disjoint_kept(self):
        m = cap.merge_intervals([("2026-01-01T01:00:00", "2026-01-01T01:30:00"),
                                 ("2026-01-01T05:00:00", "2026-01-01T05:30:00")])
        self.assertEqual(len(m), 2)


class TestCapUsage(unittest.TestCase):
    # Charged-time basis: a 7 kW car delivers 3.5 kWh in a full half-hour (0.5 h of
    # charging); charge power is inferred as 2x the cap-day's fullest delivered slot.
    def _slot(self, h, m, kwh):
        s = "2026-01-01T%02d:%02d:00" % (h, m)
        e = "2026-01-01T%02d:%02d:00" % (h + (m + 30) // 60, (m + 30) % 60)
        return (s, e, -kwh)

    def test_under_cap_no_boundary(self):
        u = cap.cap_usage([self._slot(13, 0, 3.5), self._slot(13, 30, 3.5)],
                          "UTC", cap_hours=3.0)
        self.assertAlmostEqual(u["2026-01-01"]["used_hours"], 1.0)   # 2 full slots = 1 h
        self.assertFalse(u["2026-01-01"]["over"])
        self.assertIsNone(u["2026-01-01"]["boundary_slot"])

    def test_boundary_mid_slot_with_partial(self):
        # a 1.75 kWh (1/4 h) slot shifts the cumulative so the 3 h cap lands MID-block.
        slots = [self._slot(13, 0, 3.5), self._slot(13, 30, 3.5),
                 self._slot(14, 0, 3.5), self._slot(14, 30, 3.5),
                 self._slot(15, 0, 1.75),                       # 1/4 h partial
                 self._slot(15, 30, 3.5), self._slot(16, 0, 3.5)]
        u = cap.cap_usage(slots, "UTC", cap_hours=3.0)
        self.assertTrue(u["2026-01-01"]["over"])
        self.assertEqual(u["2026-01-01"]["boundary_slot"], "2026-01-01T16:00:00")
        self.assertAlmostEqual(u["2026-01-01"]["boundary_frac"], 0.5)

    def test_all_full_slots_boundary_at_edge(self):
        # six full slots = exactly 3 h -> cap reached at the end of the 6th (frac 1.0)
        slots = [self._slot(13 + i // 2, (i % 2) * 30, 3.5) for i in range(7)]
        u = cap.cap_usage(slots, "UTC", cap_hours=3.0)
        self.assertTrue(u["2026-01-01"]["over"])
        self.assertAlmostEqual(u["2026-01-01"]["boundary_frac"], 1.0)

    def test_splits_into_two_cap_days_at_noon(self):
        u = cap.cap_usage([("2026-01-02T02:00:00", "2026-01-02T02:30:00", -3.5),   # -> 01-01
                           ("2026-01-02T14:00:00", "2026-01-02T14:30:00", -3.5)],  # -> 01-02
                          "UTC")
        self.assertIn("2026-01-01", u)
        self.assertIn("2026-01-02", u)

    def test_ignores_missing_energy(self):
        u = cap.cap_usage([("2026-01-01T13:00:00", "2026-01-01T13:30:00", None)], "UTC")
        self.assertEqual(u, {})


class TestClassifySlot(unittest.TestCase):
    # A daytime (out-of-window) slot and a night (in-window) slot.
    DAY = ("2026-01-01T13:00:00", "2026-01-01T13:30:00")
    NIGHT = ("2026-01-01T02:00:00", "2026-01-01T02:30:00")

    def c(self, slot, **kw):
        return cap.classify_slot(slot[0], slot[1], **kw)

    def test_no_dispatch_in_window_house_offpeak(self):
        r = self.c(self.NIGHT, in_off_peak_window=True, is_dispatch=False)
        self.assertIsNone(r["ev"])
        self.assertEqual(r["house"], "off_peak")

    def test_no_dispatch_out_of_window_house_day(self):
        r = self.c(self.DAY, in_off_peak_window=False, is_dispatch=False)
        self.assertIsNone(r["ev"])
        self.assertEqual(r["house"], "day")

    def test_within_cap_out_of_window_freebie(self):
        # dispatched daytime slot, within allowance (no boundary) → EV + house off-peak
        r = self.c(self.DAY, in_off_peak_window=False, is_dispatch=True,
                   boundary=None)
        self.assertEqual(r["ev"], "off_peak")
        self.assertEqual(r["house"], "off_peak")          # rule 3 freebie

    def test_over_cap_out_of_window_withdraws_freebie(self):
        # boundary already passed (before this slot) → EV peak, house back to day
        r = self.c(self.DAY, in_off_peak_window=False, is_dispatch=True,
                   boundary=("2026-01-01T13:00:00", 0.0))
        self.assertEqual(r["ev"], "peak")
        self.assertEqual(r["house"], "day")               # rule 4

    def test_over_cap_IN_window_house_stays_offpeak(self):
        # THE key rule: over-cap EV goes peak, but the guaranteed window keeps
        # the house off-peak ("even at 2 a.m. while the house is still off-peak").
        r = self.c(self.NIGHT, in_off_peak_window=True, is_dispatch=True,
                   boundary=("2026-01-01T02:00:00", 0.0))
        self.assertEqual(r["ev"], "peak")
        self.assertEqual(r["house"], "off_peak")
        self.assertEqual(r["house_offpeak_frac"], 1.0)

    def test_boundary_slot_out_of_window_blends_both(self):
        # 6-h mark at 13:15 splits the 13:00–13:30 slot 50/50
        r = self.c(self.DAY, in_off_peak_window=False, is_dispatch=True,
                   boundary=("2026-01-01T13:00:00", 0.5))
        self.assertTrue(r["boundary"])
        self.assertAlmostEqual(r["ev_offpeak_frac"], 0.5)
        self.assertEqual(r["ev"], "mixed")
        self.assertEqual(r["house"], "mixed")             # freebie half-applies
        self.assertAlmostEqual(r["house_offpeak_frac"], 0.5)

    def test_boundary_slot_IN_window_house_whole_offpeak(self):
        # boundary bisects a night slot → EV blends, but house stays fully off-peak
        r = self.c(self.NIGHT, in_off_peak_window=True, is_dispatch=True,
                   boundary=("2026-01-01T02:00:00", 0.5))
        self.assertTrue(r["boundary"])
        self.assertEqual(r["ev"], "mixed")
        self.assertEqual(r["house"], "off_peak")
        self.assertEqual(r["house_offpeak_frac"], 1.0)

    def test_boost_is_always_ev_peak(self):
        # a within-cap position, but Boost forces EV peak; out-of-window house → day
        r = self.c(self.DAY, in_off_peak_window=False, is_dispatch=True,
                   is_boost=True, boundary=None)
        self.assertEqual(r["ev"], "peak")
        self.assertEqual(r["house"], "day")


class TestPriceImportSplit(unittest.TestCase):
    # house: off-peak 0.07, day 0.30 ; EV: off-peak 0.05, peak 0.25 (£/kWh)
    RATES = dict(house_offpeak_rate=0.07, house_day_rate=0.30,
                 ev_offpeak_rate=0.05, ev_peak_rate=0.25)

    def price(self, cls, ev_kwh, house_kwh):
        return cap.price_import_split(cls, ev_kwh=ev_kwh, house_kwh=house_kwh,
                                      **self.RATES)

    def test_within_cap_out_of_window(self):
        # freebie: EV + house both off-peak (evf=1, hf=1)
        r = self.price({"ev_offpeak_frac": 1.0, "house_offpeak_frac": 1.0}, 2.0, 1.0)
        self.assertAlmostEqual(r["ev_cost"], 0.10)      # 2 * 0.05
        self.assertAlmostEqual(r["house_cost"], 0.07)   # 1 * 0.07
        self.assertAlmostEqual(r["total_cost"], 0.17)

    def test_over_cap_out_of_window(self):
        # EV peak, house day (evf=0, hf=0)
        r = self.price({"ev_offpeak_frac": 0.0, "house_offpeak_frac": 0.0}, 2.0, 1.0)
        self.assertAlmostEqual(r["ev_cost"], 0.50)      # 2 * 0.25
        self.assertAlmostEqual(r["house_cost"], 0.30)   # 1 * 0.30
        self.assertAlmostEqual(r["total_cost"], 0.80)

    def test_over_cap_in_window_house_stays_offpeak(self):
        # THE key money case: EV peak, but house off-peak (guaranteed window)
        r = self.price({"ev_offpeak_frac": 0.0, "house_offpeak_frac": 1.0}, 2.0, 1.0)
        self.assertAlmostEqual(r["ev_cost"], 0.50)      # 2 * 0.25
        self.assertAlmostEqual(r["house_cost"], 0.07)   # 1 * 0.07 (not day!)
        self.assertAlmostEqual(r["total_cost"], 0.57)

    def test_boundary_slot_blends_both(self):
        # evf=hf=0.5 → EV rate 0.15, house rate 0.185
        r = self.price({"ev_offpeak_frac": 0.5, "house_offpeak_frac": 0.5}, 2.0, 1.0)
        self.assertAlmostEqual(r["ev_cost"], 0.30)      # 2 * 0.15
        self.assertAlmostEqual(r["house_cost"], 0.185)  # 1 * 0.185
        self.assertAlmostEqual(r["effective_rate"], round(0.485 / 3.0, 6))

    def test_no_dispatch_prices_house_only(self):
        # ev_kwh 0, EV rates may be None; house on its band
        r = cap.price_import_split(
            {"ev_offpeak_frac": 0.0, "house_offpeak_frac": 0.0},
            ev_kwh=0.0, house_kwh=1.5,
            house_offpeak_rate=0.07, house_day_rate=0.30)
        self.assertEqual(r["ev_cost"], 0.0)
        self.assertAlmostEqual(r["house_cost"], 0.45)   # 1.5 * 0.30
        self.assertAlmostEqual(r["effective_rate"], 0.30)

    def test_composes_with_classify_slot(self):
        # boundary daytime slot 13:00–13:30, 6h mark at 13:15 → blended both
        cls = cap.classify_slot("2026-01-01T13:00:00", "2026-01-01T13:30:00",
                                in_off_peak_window=False, is_dispatch=True,
                                boundary=("2026-01-01T13:00:00", 0.5))
        r = self.price(cls, 2.0, 1.0)
        self.assertAlmostEqual(r["ev_cost"], 0.30)
        self.assertAlmostEqual(r["house_cost"], 0.185)


class TestCapDayBoundaries(unittest.TestCase):
    def test_projects_boundary_tuple_per_day(self):
        # a partial slot lands the boundary mid-block; the projection carries (slot, frac).
        slots = [("2026-01-01T13:00:00", "2026-01-01T13:30:00", -3.5),
                 ("2026-01-01T13:30:00", "2026-01-01T14:00:00", -1.75),
                 ("2026-01-01T14:00:00", "2026-01-01T14:30:00", -3.5)]
        b = cap.cap_day_boundaries(slots, "UTC", cap_hours=1.0)
        self.assertEqual(b["2026-01-01"], ("2026-01-01T14:00:00", 0.5))

    def test_absent_when_under_cap(self):
        b = cap.cap_day_boundaries(
            [("2026-01-01T13:00:00", "2026-01-01T13:30:00", -3.5)], "UTC")   # 0.5 h << 6 h
        self.assertEqual(b["2026-01-01"], (None, None))


class TestPriceSlot(unittest.TestCase):
    DAY = ("2026-01-01T13:00:00", "2026-01-01T13:30:00")
    NIGHT = ("2026-01-01T02:00:00", "2026-01-01T02:30:00")
    CAP_RATES = dict(house_offpeak_rate=0.07, house_day_rate=0.30,
                     ev_offpeak_rate=0.05, ev_peak_rate=0.25)

    def test_capped_within_cap_freebie(self):
        r = cap.price_slot(*self.DAY, 3.0, 2.0, in_off_peak_window=False,
                           is_boost=False, boundary=None, **self.CAP_RATES)
        self.assertAlmostEqual(r["imp_cost"], 0.17)     # 2*0.05 + 1*0.07
        self.assertEqual(r["imp_kwh_ev"], 2.0)
        self.assertAlmostEqual(r["imp_cost_ev"], 0.10)
        self.assertAlmostEqual(r["imp_rate_ev"], 0.05)

    def test_capped_over_cap_in_window_house_offpeak(self):
        r = cap.price_slot(*self.NIGHT, 3.0, 2.0, in_off_peak_window=True,
                           is_boost=False, boundary=("2026-01-01T02:00:00", 0.0),
                           **self.CAP_RATES)
        self.assertAlmostEqual(r["imp_cost"], 0.57)     # 2*0.25 + 1*0.07 (house off-peak)
        self.assertAlmostEqual(r["imp_cost_ev"], 0.50)
        self.assertAlmostEqual(r["imp_rate_ev"], 0.25)

    def test_no_ev_house_only(self):
        r = cap.price_slot(*self.DAY, 1.5, 0.0, in_off_peak_window=False,
                           is_boost=False, boundary=None,
                           house_offpeak_rate=0.07, house_day_rate=0.30)
        self.assertAlmostEqual(r["imp_cost"], 0.45)     # 1.5 * 0.30
        self.assertIsNone(r["imp_kwh_ev"])
        self.assertIsNone(r["imp_cost_ev"])

    def test_uncapped_whole_slot_offpeak_totals_unchanged(self):
        # Uncapped IOG emulation: boundary None, all four rates = the overlay
        # off-peak rate → total is byte-identical to chosen*rate, EV still carved.
        OP = 0.05
        r = cap.price_slot(*self.DAY, 3.0, 2.0, in_off_peak_window=False,
                           is_boost=False, boundary=None,
                           house_offpeak_rate=OP, house_day_rate=OP,
                           ev_offpeak_rate=OP, ev_peak_rate=OP)
        self.assertAlmostEqual(r["imp_cost"], round(3.0 * OP, 6))   # unchanged
        self.assertAlmostEqual(r["imp_rate"], OP)
        self.assertAlmostEqual(r["imp_cost_ev"], round(2.0 * OP, 6))

    # ── BL-27: the seam emits full-fidelity segments that reconcile to the columns ──

    def _reconciles(self, r):
        segs = r["segments"]
        self.assertAlmostEqual(sum(k * rate for (k, rate, b, a) in segs),
                               r["imp_cost"], places=6)
        self.assertAlmostEqual(sum(k * rate for (k, rate, b, a) in segs if a == "ev"),
                               r["imp_cost_ev"], places=6)

    def test_boundary_slot_emits_four_bands(self):
        # Cap boundary mid-slot, out of window → EV and house both straddle → four bands,
        # each at a REAL tariff rate (not the blended 0.15 the imp_rate_ev column collapses to).
        r = cap.price_slot(*self.DAY, 3.0, 2.0, in_off_peak_window=False, is_boost=False,
                           boundary=("2026-01-01T13:00:00", 0.5), **self.CAP_RATES)
        self._reconciles(r)
        bands = {(a, b) for (k, rate, b, a) in r["segments"]}
        self.assertEqual(bands, {("ev", "off_peak"), ("ev", "peak"),
                                 ("house", "off_peak"), ("house", "day")})
        self.assertEqual(len(r["segments"]), 4)

    def test_clean_over_cap_slot_two_bands(self):
        # Over-cap, out of window: EV all peak, house all day → two bands, no blend.
        r = cap.price_slot(*self.DAY, 3.0, 2.0, in_off_peak_window=False, is_boost=False,
                           boundary=("2026-01-01T02:00:00", 0.0), **self.CAP_RATES)
        self._reconciles(r)
        self.assertEqual({(a, b) for (k, rate, b, a) in r["segments"]},
                         {("ev", "peak"), ("house", "day")})

    def test_uncapped_segments_reconcile_at_single_rate(self):
        OP = 0.05
        r = cap.price_slot(*self.DAY, 3.0, 2.0, in_off_peak_window=False, is_boost=False,
                           boundary=None, house_offpeak_rate=OP, house_day_rate=OP,
                           ev_offpeak_rate=OP, ev_peak_rate=OP)
        self._reconciles(r)
        self.assertTrue(all(rate == OP for (k, rate, b, a) in r["segments"]))

    def test_no_ev_slot_has_no_segments(self):
        r = cap.price_slot(*self.DAY, 1.5, 0.0, in_off_peak_window=False, is_boost=False,
                           boundary=None, house_offpeak_rate=0.07, house_day_rate=0.30)
        self.assertIsNone(r["segments"])

    def test_columns_are_exact_segment_projections(self):
        # BL-27: with messy kWh the columns are now bit-identical projections of the
        # segments (single source of truth) — Σ segment cost EQUALS imp_cost exactly, and
        # the EV bands EQUAL imp_cost_ev, not merely to a tolerance.
        r = cap.price_slot(*self.DAY, 2.837, 1.913, in_off_peak_window=False, is_boost=False,
                           boundary=("2026-01-01T13:00:00", 0.5), **self.CAP_RATES)
        self.assertEqual(round(sum(k * rate for (k, rate, b, a) in r["segments"]), 6),
                         r["imp_cost"])
        self.assertEqual(round(sum(k * rate for (k, rate, b, a) in r["segments"]
                                   if a == "ev"), 6), r["imp_cost_ev"])


class TestComputeIogSplit(unittest.TestCase):
    DAY = ("2026-01-01T13:00:00", "2026-01-01T13:30:00")
    NIGHT = ("2026-01-01T02:00:00", "2026-01-01T02:30:00")

    def _house(self):
        # banded day/night in PENCE (what RateSchedule/from_api_records stores in production):
        # off-peak 7.0p (23:30–05:30), day 30.0p. compute_iog_split converts these to £/kWh.
        return RateSchedule([
            ("2026-01-01T00:00:00", "2026-01-01T05:30:00", 7.0),
            ("2026-01-01T05:30:00", "2026-01-01T23:30:00", 30.0),
            ("2026-01-01T23:30:00", "2026-01-02T05:30:00", 7.0),
        ])

    def _ev(self, rate_pence):
        return RateSchedule([("2026-01-01T00:00:00", None, rate_pence)])

    def test_no_house_schedule_returns_none(self):
        r = cap.compute_iog_split(*self.DAY, chosen_kwh=3.0, ev_kwh=2.0,
                                  overlay_rate=0.30, is_boost=False, capped=True,
                                  boundary=None, import_sched=RateSchedule([]))
        self.assertIsNone(r)

    def test_capped_within_cap_freebie(self):
        r = cap.compute_iog_split(*self.DAY, chosen_kwh=3.0, ev_kwh=2.0,
                                  overlay_rate=0.30, is_boost=False, capped=True,
                                  boundary=None, import_sched=self._house(),
                                  ev_off_sched=self._ev(5.0), ev_peak_sched=self._ev(25.0))
        self.assertAlmostEqual(r["imp_cost"], 0.17)     # 2*0.05 + 1*0.07 (house freebie)
        self.assertAlmostEqual(r["imp_cost_ev"], 0.10)

    def test_capped_over_cap_in_window_house_offpeak(self):
        r = cap.compute_iog_split(*self.NIGHT, chosen_kwh=3.0, ev_kwh=2.0,
                                  overlay_rate=0.07, is_boost=False, capped=True,
                                  boundary=("2026-01-01T02:00:00", 0.0),
                                  import_sched=self._house(),
                                  ev_off_sched=self._ev(5.0), ev_peak_sched=self._ev(25.0))
        self.assertAlmostEqual(r["imp_cost"], 0.57)     # 2*0.25 + 1*0.07 (house guaranteed)
        self.assertAlmostEqual(r["imp_cost_ev"], 0.50)

    def test_uncapped_totals_unchanged(self):
        # capped=False → whole slot on overlay_rate; total == chosen*overlay
        OP = 0.05
        r = cap.compute_iog_split(*self.DAY, chosen_kwh=3.0, ev_kwh=2.0,
                                  overlay_rate=OP, is_boost=False, capped=False,
                                  boundary=None, import_sched=self._house())
        self.assertAlmostEqual(r["imp_cost"], round(3.0 * OP, 6))
        self.assertAlmostEqual(r["imp_rate"], OP)
        self.assertAlmostEqual(r["imp_cost_ev"], round(2.0 * OP, 6))

    def test_capped_but_missing_ev_rate_falls_back(self):
        # capped flag set but ev_device schedules empty → behaves like uncapped
        OP = 0.05
        r = cap.compute_iog_split(*self.DAY, chosen_kwh=3.0, ev_kwh=2.0,
                                  overlay_rate=OP, is_boost=False, capped=True,
                                  boundary=None, import_sched=self._house(),
                                  ev_off_sched=RateSchedule([]),
                                  ev_peak_sched=RateSchedule([]))
        self.assertAlmostEqual(r["imp_cost"], round(3.0 * OP, 6))   # totals unchanged


if __name__ == "__main__":
    unittest.main()
