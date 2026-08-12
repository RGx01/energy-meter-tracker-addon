"""Tests for kraken_rates.py (Chunk 5) — RateSchedule build + resolve."""

import asyncio
import unittest

from kraken_rates import RateSchedule, build_rate_schedule


def run(coro):
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


class TestRateSchedule(unittest.TestCase):
    def test_empty(self):
        s = RateSchedule([])
        self.assertTrue(s.is_empty())
        self.assertIsNone(s.resolve("2026-05-01T00:00:00"))

    def test_fixed_single_open_period(self):
        s = RateSchedule.from_api_records([
            {"value_inc_vat": 24.5, "valid_from": "2026-02-01T00:00:00Z",
             "valid_to": None}])
        self.assertEqual(len(s), 1)
        self.assertEqual(s.resolve("2026-05-01T12:00:00"), 24.5)
        self.assertIsNone(s.resolve("2026-01-01T00:00:00"))  # before start

    def test_multiple_periods_pick_correct(self):
        s = RateSchedule.from_api_records([
            {"value_inc_vat": 20.0, "valid_from": "2026-01-01T00:00:00Z",
             "valid_to": "2026-03-01T00:00:00Z"},
            {"value_inc_vat": 25.0, "valid_from": "2026-03-01T00:00:00Z",
             "valid_to": None},
        ])
        self.assertEqual(s.resolve("2026-02-15T00:00:00"), 20.0)
        self.assertEqual(s.resolve("2026-03-01T00:00:00"), 25.0)  # boundary = new
        self.assertEqual(s.resolve("2026-05-01T00:00:00"), 25.0)

    def test_prefers_direct_debit(self):
        s = RateSchedule.from_api_records([
            {"value_inc_vat": 24.5, "valid_from": "2026-02-01T00:00:00Z",
             "valid_to": None, "payment_method": "DIRECT_DEBIT"},
            {"value_inc_vat": 30.0, "valid_from": "2026-02-01T00:00:00Z",
             "valid_to": None, "payment_method": "NON_DIRECT_DEBIT"},
        ])
        self.assertEqual(len(s), 1)
        self.assertEqual(s.resolve("2026-03-01T00:00:00"), 24.5)

    def test_bst_offset_normalised(self):
        # A CET/offset valid_from must normalise to naive UTC.
        s = RateSchedule.from_api_records([
            {"value_inc_vat": 15.0, "valid_from": "2026-06-01T01:00:00+01:00",
             "valid_to": None}])
        # 01:00+01:00 == 00:00 UTC
        self.assertEqual(s.resolve("2026-06-01T00:00:00"), 15.0)
        self.assertIsNone(s.resolve("2026-05-31T23:00:00"))

    def test_skips_records_without_value(self):
        s = RateSchedule.from_api_records([
            {"valid_from": "2026-02-01T00:00:00Z", "valid_to": None},  # no value
            {"value_inc_vat": 12.0, "valid_from": "2026-02-01T00:00:00Z",
             "valid_to": None},
        ])
        self.assertEqual(len(s), 1)

    def test_exc_sibling_from_api_records(self):
        # BL-23 (4.2 Slice B): the parallel EX-VAT schedule is exposed as `.exc` and
        # resolves independently; the inc path is unchanged, and VAT is the pair's ratio.
        s = RateSchedule.from_api_records([
            {"value_inc_vat": 26.1537, "value_exc_vat": 24.9083,
             "valid_from": "2026-02-01T00:00:00Z", "valid_to": None}])
        self.assertEqual(s.resolve("2026-05-01T00:00:00"), 26.1537)        # inc unchanged
        self.assertIsNotNone(s.exc)
        self.assertEqual(s.exc.resolve("2026-05-01T00:00:00"), 24.9083)    # exc series
        self.assertIsNone(s.exc.exc)                                       # no recursion
        self.assertAlmostEqual(26.1537 / 24.9083 - 1, 0.05, places=3)      # VAT from pair

    def test_exc_sibling_absent_when_no_exc(self):
        # inc-only records → no exc sibling (nothing to resolve against).
        s = RateSchedule.from_api_records([
            {"value_inc_vat": 12.0, "valid_from": "2026-02-01T00:00:00Z", "valid_to": None}])
        self.assertIsNone(s.exc)

    def test_exc_sibling_banded_bounds(self):
        # IOG-style banded tariff: exc day_rate_bounds gives (off-peak-exc, peak-exc),
        # reusing the inc resolver logic for free.
        s = RateSchedule.from_api_records([
            {"value_inc_vat": 5.77, "value_exc_vat": 5.4952,
             "valid_from": "2026-02-01T00:00:00Z", "valid_to": "2026-02-01T05:00:00Z"},
            {"value_inc_vat": 33.92, "value_exc_vat": 32.3048,
             "valid_from": "2026-02-01T05:00:00Z", "valid_to": None},
        ])
        lo, hi = s.exc.day_rate_bounds("2026-02-01T12:00:00")
        self.assertAlmostEqual(lo, 5.4952, places=4)
        self.assertAlmostEqual(hi, 32.3048, places=4)


class _FakeClient:
    def __init__(self, records=None, raise_=False):
        self._records = records or []
        self._raise = raise_

    async def get_unit_rates(self, product, tariff, *, period_from=None,
                             period_to=None):
        if self._raise:
            raise RuntimeError("boom")
        return self._records


class TestBuildRateSchedule(unittest.TestCase):
    def test_builds(self):
        c = _FakeClient([{"value_inc_vat": 24.5,
                          "valid_from": "2026-02-01T00:00:00Z", "valid_to": None}])
        s = run(build_rate_schedule(c, "INTELLI-FIX-12M-26-03-17",
                                    "E-1R-INTELLI-FIX-12M-26-03-17-B"))
        self.assertEqual(len(s), 1)

    def test_missing_codes_returns_empty(self):
        c = _FakeClient([])
        self.assertTrue(run(build_rate_schedule(c, "", "")).is_empty())

    def test_fetch_failure_returns_empty(self):
        c = _FakeClient(raise_=True)
        s = run(build_rate_schedule(c, "P", "T"))
        self.assertTrue(s.is_empty())


class _FakeSCClient:
    def __init__(self, records=None):
        self._records = records or []
    async def get_standing_charges(self, product, tariff, *, period_from=None,
                                   period_to=None):
        return self._records


class TestBuildStandingChargeSchedule(unittest.TestCase):
    def test_builds(self):
        from kraken_rates import build_standing_charge_schedule
        c = _FakeSCClient([{"value_inc_vat": 47.85,
                            "valid_from": "2026-02-01T00:00:00Z", "valid_to": None}])
        s = run(build_standing_charge_schedule(c, "INTELLI-FIX-12M-26-03-17",
                                               "E-1R-INTELLI-FIX-12M-26-03-17-B"))
        self.assertEqual(len(s), 1)
        self.assertEqual(s.resolve("2026-05-01T00:00:00"), 47.85)



class TestNewIOGTariffFallback(unittest.TestCase):
    """#1708: the new IOG time-of-use / 6-hour-cap tariff drops standard-unit-rates
    for day/night/ev buckets. build_rate_schedule must fall back to merging
    day+night into the base TOU schedule instead of returning empty."""

    def _client(self, standard, day, night):
        class C:
            async def get_unit_rates(self, product, tariff, *,
                                     rate_type="standard-unit-rates",
                                     period_from=None, period_to=None):
                return {"standard-unit-rates": standard, "day-unit-rates": day,
                        "night-unit-rates": night}.get(rate_type, [])
        return C()

    def test_day_night_merge_when_no_standard(self):
        day = [{"value_inc_vat": 32.0, "valid_from": "2026-07-01T05:30:00Z",
                "valid_to": "2026-07-01T23:30:00Z"}]
        night = [{"value_inc_vat": 7.0, "valid_from": "2026-07-01T23:30:00Z",
                  "valid_to": "2026-07-02T05:30:00Z"}]
        sched = asyncio.get_event_loop().run_until_complete(
            build_rate_schedule(self._client([], day, night), "IOG-SMB-TOU", "E-1R-IOG-SMB-TOU-H"))
        self.assertFalse(sched.is_empty())
        self.assertEqual(sched.resolve("2026-07-01T12:00:00"), 32.0)   # day
        self.assertEqual(sched.resolve("2026-07-02T02:00:00"), 7.0)    # night
        self.assertEqual(sched.off_peak_rate_near("2026-07-01T23:45:00"), 7.0)

    def test_standard_still_preferred_when_present(self):
        std = [{"value_inc_vat": 24.5, "valid_from": "2026-07-01T00:00:00Z",
                "valid_to": None}]
        sched = asyncio.get_event_loop().run_until_complete(
            build_rate_schedule(self._client(std, [], []), "INTELLI-FIX", "E-1R-INTELLI-FIX-H"))
        self.assertEqual(sched.resolve("2026-07-01T12:00:00"), 24.5)

    def test_empty_when_no_buckets_at_all(self):
        sched = asyncio.get_event_loop().run_until_complete(
            build_rate_schedule(self._client([], [], []), "X", "E-1R-X-H"))
        self.assertTrue(sched.is_empty())   # guard territory


class _FakeRateClient:
    """Stand-in exposing get_unit_rates with controllable per-bucket behaviour."""

    def __init__(self, standard=None, day=None, night=None,
                 raise_std=False, raise_dn=False):
        self.standard, self.day, self.night = standard, day, night
        self.raise_std, self.raise_dn = raise_std, raise_dn

    async def get_unit_rates(self, product, tariff, *,
                             rate_type="standard-unit-rates",
                             period_from=None, period_to=None):
        if rate_type == "standard-unit-rates":
            if self.raise_std:
                raise RuntimeError("boom (standard)")
            return self.standard or []
        if self.raise_dn:
            raise RuntimeError("boom (day/night)")
        if rate_type == "day-unit-rates":
            return self.day or []
        if rate_type == "night-unit-rates":
            return self.night or []
        return []


_REC = [{"value_inc_vat": 5.0, "valid_from": "2026-07-01T00:00:00Z",
         "valid_to": None}]


class TestBuildRateScheduleFetchErrors(unittest.TestCase):
    """A FAILED rate fetch must be distinguishable from a genuinely-empty one, so
    the caller doesn't mislabel a transient API failure as 'tariff unsupported'."""

    def test_success_returns_schedule(self):
        s = run(build_rate_schedule(_FakeRateClient(standard=_REC), "P",
                                    "E-1R-P-A", raise_on_error=True))
        self.assertFalse(s.is_empty())

    def test_fetch_error_raises_when_requested(self):
        from kraken_rates import RateFetchError
        with self.assertRaises(RateFetchError):
            run(build_rate_schedule(_FakeRateClient(raise_std=True), "P",
                                    "E-1R-P-A", raise_on_error=True))

    def test_fetch_error_returns_empty_by_default(self):
        # Default (raise_on_error=False) keeps the graceful-empty behaviour that
        # pricing/drain callers rely on.
        s = run(build_rate_schedule(_FakeRateClient(raise_std=True), "P",
                                    "E-1R-P-A"))
        self.assertTrue(s.is_empty())

    def test_genuinely_empty_returns_empty_without_raising(self):
        # Standard empty AND day/night empty = a real unsupported tariff: empty
        # schedule, NOT an error.
        s = run(build_rate_schedule(_FakeRateClient(standard=[], day=[], night=[]),
                                    "P", "E-1R-P-A", raise_on_error=True))
        self.assertTrue(s.is_empty())

    def test_daynight_fallback_error_raises(self):
        from kraken_rates import RateFetchError
        with self.assertRaises(RateFetchError):
            run(build_rate_schedule(_FakeRateClient(standard=[], raise_dn=True),
                                    "P", "E-1R-P-A", raise_on_error=True))

    def test_daynight_fallback_success(self):
        s = run(build_rate_schedule(_FakeRateClient(standard=[], day=_REC, night=[]),
                                    "P", "E-1R-P-A", raise_on_error=True))
        self.assertFalse(s.is_empty())


if __name__ == "__main__":
    unittest.main()