"""BL-52 — reconstruct the windowed day/night periods the new 4-rate IOG-SMB
tariff omits (Octopus dropped `standard-unit-rates` and returns two flat,
windowless rates). The reconstruction rebuilds the same period shape the old
`standard-unit-rates` feed produced, so `resolve`/`is_off_peak` — and therefore
the settlement reconcile and the cap classifier — work unchanged.

Regression anchor: on the degenerate (windowless) schedule, `resolve()` returned
the off-peak rate for every time of day, so `reconcile_dispatch_overlay`'s
`abs(base - off_peak) < 1e-6` guard skipped every block (prod: "scanned 583,
reverted 0"). These tests assert the window is back and that guard no longer
trips.
"""
import unittest
from datetime import datetime

from kraken_rates import _synthesize_iog_tou_windowed, RateSchedule, build_rate_schedule
import asyncio

DAY_INC, NIGHT_INC = 32.309235, 5.49297
DAY_EXC, NIGHT_EXC = 30.7707, 5.2314
# Flat, windowless day/night buckets exactly as the IOG-SMB API returns them.
DAY = [{"value_inc_vat": DAY_INC, "value_exc_vat": DAY_EXC,
        "valid_from": "2026-07-05T23:00:00Z", "valid_to": None}]
NIGHT = [{"value_inc_vat": NIGHT_INC, "value_exc_vat": NIGHT_EXC,
          "valid_from": "2026-07-05T23:00:00Z", "valid_to": None}]


def _sched(now):
    return RateSchedule.from_api_records(
        _synthesize_iog_tou_windowed(DAY, NIGHT, now=now))


def _band(r):
    if r is None:
        return None
    if abs(r - NIGHT_INC) < 1e-6:
        return "night"
    if abs(r - DAY_INC) < 1e-6:
        return "day"
    return "other"



def _run(coro):
    """Fresh event loop per call — robust when a prior test module closed the
    shared default loop (test-isolation, not shared-loop reuse)."""
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()

class TestIogTouWindow(unittest.TestCase):
    def test_summer_bst_window(self):
        # BST: off-peak window 23:30-05:30 local == 22:30-04:30Z
        s = _sched(datetime(2026, 8, 25))
        self.assertEqual(_band(s.resolve("2026-08-23T10:00:00")), "day")
        self.assertEqual(_band(s.resolve("2026-08-23T02:00:00")), "night")
        self.assertEqual(_band(s.resolve("2026-08-23T04:29:00")), "night")  # 05:29 BST
        self.assertEqual(_band(s.resolve("2026-08-23T04:31:00")), "day")    # 05:31 BST
        self.assertEqual(_band(s.resolve("2026-08-23T22:31:00")), "night")  # 23:31 BST

    def test_winter_gmt_window_dst_aware(self):
        # GMT: window shifts to 23:30-05:30Z
        s = _sched(datetime(2027, 1, 20))
        self.assertEqual(_band(s.resolve("2027-01-15T10:00:00")), "day")
        self.assertEqual(_band(s.resolve("2027-01-15T05:00:00")), "night")  # 05:00 GMT
        self.assertEqual(_band(s.resolve("2027-01-15T05:31:00")), "day")    # 05:31 GMT
        self.assertEqual(_band(s.resolve("2027-01-15T23:31:00")), "night")  # 23:31 GMT

    def test_dst_transition_day(self):
        # 2026-10-25 clocks go back (02:00 BST -> 01:00 GMT); boundaries are clear
        # of the 01:00-02:00 fold, so still resolve cleanly.
        s = _sched(datetime(2026, 10, 27))
        self.assertEqual(_band(s.resolve("2026-10-25T05:31:00")), "day")
        self.assertEqual(_band(s.resolve("2026-10-25T00:30:00")), "night")

    def test_is_off_peak_matches_resolve(self):
        s = _sched(datetime(2026, 8, 25))
        self.assertFalse(s.is_off_peak("2026-08-23T10:00:00"))
        self.assertTrue(s.is_off_peak("2026-08-23T02:00:00"))

    def test_reconcile_guard_no_longer_trips(self):
        # The bug: base == off_peak_near for every slot -> reconcile skips.
        s = _sched(datetime(2026, 8, 25))
        base = s.resolve("2026-08-23T10:00:00")
        offpeak = s.off_peak_rate_near("2026-08-23T10:00:00")
        self.assertGreater(abs(base - offpeak), 1e-6)  # daytime base != off-peak

    def test_exc_sibling_preserved(self):
        s = _sched(datetime(2026, 8, 25))
        self.assertIsNotNone(s.exc)
        self.assertAlmostEqual(s.exc.resolve("2026-08-23T10:00:00"), DAY_EXC, places=5)
        self.assertAlmostEqual(s.exc.resolve("2026-08-23T02:00:00"), NIGHT_EXC, places=5)

    def test_flat_schedule_untouched(self):
        flat = [{"value_inc_vat": 24.5, "value_exc_vat": 23.3,
                 "valid_from": "2025-01-01T00:00:00Z", "valid_to": None,
                 "payment_method": "DIRECT_DEBIT"}]
        s = RateSchedule.from_api_records(flat)
        self.assertEqual(s.resolve("2026-08-23T10:00:00"), 24.5)


class _MockClient:
    def __init__(self, standard, day, night):
        self._s, self._d, self._n = standard, day, night

    async def get_unit_rates(self, product, tariff, *, rate_type="standard-unit-rates",
                             period_from=None, period_to=None):
        return {"standard-unit-rates": self._s, "day-unit-rates": self._d,
                "night-unit-rates": self._n}.get(rate_type, [])


class TestBuildRateScheduleScoping(unittest.TestCase):
    def test_iog_smb_reconstructs(self):
        c = _MockClient([], DAY, NIGHT)
        s = _run(
            build_rate_schedule(c, "IOG-SMB-FIX-12M-26-03-17",
                                "E-1R-IOG-SMB-FIX-12M-26-03-17-B"))
        self.assertGreater(len(s), 2)  # windowed periods, not two flat ones
        self.assertEqual(_band(s.resolve("2026-08-23T10:00:00")), "day")
        self.assertEqual(_band(s.resolve("2026-08-23T02:00:00")), "night")

    def test_iog_with_standard_rates_not_reconstructed(self):
        std = [{"value_inc_vat": 28.557, "value_exc_vat": 27.19,
                "valid_from": "2026-08-23T04:30:00Z", "valid_to": "2026-08-23T22:30:00Z"},
               {"value_inc_vat": 6.9, "value_exc_vat": 6.57,
                "valid_from": "2026-08-23T22:30:00Z", "valid_to": "2026-08-24T04:30:00Z"}]
        c = _MockClient(std, DAY, NIGHT)
        s = _run(
            build_rate_schedule(c, "INTELLI-VAR-24-10-29", "E-1R-INTELLI-VAR-24-10-29-B"))
        self.assertAlmostEqual(s.resolve("2026-08-23T10:00:00"), 28.557, places=3)

    def test_non_iog_windowless_not_reconstructed(self):
        # A non-IOG tariff with windowless day/night must NOT get the IOG window.
        c = _MockClient([], DAY, NIGHT)
        s = _run(
            build_rate_schedule(c, "COSY-SOMETHING", "E-1R-COSY-SOMETHING-A"))
        self.assertEqual(len(s), 2)  # plain concat, not reconstructed


if __name__ == "__main__":
    unittest.main()
