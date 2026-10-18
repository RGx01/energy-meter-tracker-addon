"""
A running fixed-term agreement is rebuilt at every refresh, not cached (4.5.24, 18 Oct 2026).

The stitch cached an agreement's schedule for the life of the process whenever the agreement had a
`valid_to`. A fixed-term agreement still in force has one, and IOG-SMB's windows are synthesised
only up to now + 2 days (BL-52), so the copy cached when the process started stopped two days
later. v4's process ran from the 4.5.23 install on 12 Oct: its schedule ended at 14 Oct and every
bill from 15 Oct was refused for want of a rate. Now only an agreement that has ENDED is cached
(`_agreement_ended`).

Discriminating (fail on the parent): test_a_running_fixed_term_agreement_is_rebuilt,
test_ended_means_valid_to_has_passed. The rest are guards
(test_no_valid_to_is_not_ended calls the new helper, so fails on the parent by name).
"""
import asyncio
import os
import sys
import unittest
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import kraken_rates                              # noqa: E402


def _run(coro):
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


def _iso(days):
    return (datetime.now(timezone.utc).replace(tzinfo=None, microsecond=0)
            + timedelta(days=days)).isoformat() + "Z"


class _Client:
    def __init__(self):
        self.calls = {}

    @staticmethod
    def _tariff_to_product_code(tc):
        return "-".join(tc.split("-")[2:-1])

    async def get_unit_rates(self, product, tariff, *, rate_type="standard-unit-rates",
                             period_from=None, period_to=None):
        self.calls[(tariff, rate_type)] = self.calls.get((tariff, rate_type), 0) + 1
        if rate_type in ("standard-unit-rates", "ev-device-off-peak-unit-rates",
                         "ev-device-peak-unit-rates"):
            return [{"value_inc_vat": 10.0, "value_exc_vat": 10.0,
                     "valid_from": "2020-01-01T00:00:00Z", "valid_to": None}]
        return []


RUNNING = "E-1R-IOG-SMB-FIX-12M-26-03-17-B"
ENDED = "E-1R-INTELLI-FIX-12M-25-03-17-B"
OPEN = "E-1R-AGILE-24-10-01-B"


class TheStitch(unittest.TestCase):

    def setUp(self):
        kraken_rates._AGREEMENT_SCHED_CACHE.clear()

    def _twice(self, agreements):
        c = _Client()
        for _ in range(2):
            _run(kraken_rates.build_agreement_stitched_schedule(c, agreements, vat=kraken_rates.NO_VAT))
        return c

    def _n(self, c, tariff):
        return c.calls.get((tariff, "standard-unit-rates"), 0)

    def test_a_running_fixed_term_agreement_is_rebuilt(self):
        c = self._twice([{"tariff_code": RUNNING, "valid_from": _iso(-50), "valid_to": _iso(300)}])
        self.assertEqual(self._n(c, RUNNING), 2)

    def test_an_ended_agreement_is_cached(self):
        """GUARD: closed history cannot change."""
        c = self._twice([{"tariff_code": ENDED, "valid_from": _iso(-400), "valid_to": _iso(-50)}])
        self.assertEqual(self._n(c, ENDED), 1)

    def test_an_open_ended_agreement_is_rebuilt(self):
        """GUARD: as before."""
        c = self._twice([{"tariff_code": OPEN, "valid_from": _iso(-50), "valid_to": None}])
        self.assertEqual(self._n(c, OPEN), 2)

class TheRule(unittest.TestCase):

    def test_ended_means_valid_to_has_passed(self):
        self.assertTrue(kraken_rates._agreement_ended(_iso(-1)[:-1]))
        self.assertFalse(kraken_rates._agreement_ended(_iso(1)[:-1]))

    def test_no_valid_to_is_not_ended(self):
        """GUARD."""
        self.assertFalse(kraken_rates._agreement_ended(None))


if __name__ == "__main__":
    unittest.main()
