"""
test_ev_device_rates.py — build_ev_device_schedules (IOG-SMB-TOU 6-hour-cap
EV-device rate buckets). Additive + non-fatal: a missing bucket or a fetch error
yields an empty schedule so the cap classifier falls back to the general overlay.
"""

import asyncio
import unittest

from kraken_rates import build_ev_device_schedules


def run(coro):
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


class _FakeEVClient:
    """Serves per-rate_type buckets; unknown buckets return []."""
    def __init__(self, buckets):
        self._b = buckets

    async def get_unit_rates(self, product_code, tariff_code, *,
                             rate_type="standard-unit-rates",
                             period_from=None, period_to=None):
        return self._b.get(rate_type, [])


class TestEVDeviceSchedules(unittest.TestCase):
    _OFF = [{"value_inc_vat": 7.0, "valid_from": "2026-01-01T00:00:00Z",
             "valid_to": None}]
    _PEAK = [{"value_inc_vat": 25.0, "valid_from": "2026-01-01T00:00:00Z",
              "valid_to": None}]

    def test_builds_both_buckets(self):
        client = _FakeEVClient({"ev-device-off-peak-unit-rates": self._OFF,
                                "ev-device-peak-unit-rates": self._PEAK})
        off, peak = run(build_ev_device_schedules(
            client, "P", "E-1R-IOG-SMB-TOU-25-12-12-H"))
        self.assertEqual(off.resolve("2026-06-01T12:00:00"), 7.0)
        self.assertEqual(peak.resolve("2026-06-01T12:00:00"), 25.0)

    def test_missing_buckets_yield_empty(self):
        off, peak = run(build_ev_device_schedules(
            _FakeEVClient({}), "P", "E-1R-IOG-SMB-TOU-25-12-12-H"))
        self.assertTrue(off.is_empty())
        self.assertTrue(peak.is_empty())

    def test_fetch_error_is_non_fatal(self):
        class _Boom:
            async def get_unit_rates(self, *a, **k):
                raise RuntimeError("boom")
        off, peak = run(build_ev_device_schedules(_Boom(), "P", "T"))
        self.assertTrue(off.is_empty() and peak.is_empty())


if __name__ == "__main__":
    unittest.main()
