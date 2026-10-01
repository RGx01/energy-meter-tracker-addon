"""
Domestic-energy VAT went to 0% on 1 Oct 2026 — and must not reach back into history.

Octopus applied it by EDITING its open-ended rate records in place, not by closing the
old one: on 1 Oct 2026 IOG-SMB-FIX-12M-26-03-17-B's day and night records, both valid
from 2026-07-05T23:00:00Z with no end, read inc == exc (30.7707 / 5.2314). Read as
published, that priced 5 Jul - 30 Sep with no VAT (the Billing tab's house line at
30.7707p from the SMB agreement's start), and the VAT learner, dating the ratio at the
record's valid_from, stored "0% from 2026-07-05" — so the fallback VAT, and with it every
inc/exc split, went to 0% for the same months. The change is statutory and tariff-
agnostic: every import unit rate and standing charge carries it; export carries no VAT.

The fixtures below are that API response, as fetched on 1 Oct 2026.
"""
import asyncio
import os
import sys
import unittest
from unittest import mock

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import engine                                            # noqa: E402
import kraken_rates                                      # noqa: E402
import vat_calendar as vc                                # noqa: E402
from block_store import BlockStore                       # noqa: E402
from kraken_rates import (NO_VAT, RateSchedule, build_rate_schedule,   # noqa: E402
                          build_standing_charge_schedule)

PRODUCT, TARIFF = "IOG-SMB-FIX-12M-26-03-17", "E-1R-IOG-SMB-FIX-12M-26-03-17-B"
_OPEN = {"valid_from": "2026-07-05T23:00:00Z", "valid_to": None}
DAY = [dict(_OPEN, value_exc_vat=30.7707, value_inc_vat=30.7707)]
NIGHT = [dict(_OPEN, value_exc_vat=5.2314, value_inc_vat=5.2314)]


def run(coro):
    return asyncio.new_event_loop().run_until_complete(coro)


class _Client:
    def __init__(self, standard=None, day=None, night=None, standing=None):
        self.standard, self.day, self.night, self.standing = standard, day, night, standing

    async def get_unit_rates(self, product, tariff, *, rate_type="standard-unit-rates",
                             period_from=None, period_to=None):
        return {"standard-unit-rates": self.standard, "day-unit-rates": self.day,
                "night-unit-rates": self.night}.get(rate_type) or []

    async def get_standing_charges(self, product, tariff, *, period_from=None, period_to=None):
        return self.standing or []


class TheImportScheduleIsTrueAtEachDate(unittest.TestCase):
    """The rate a past half-hour resolves to carries the VAT of ITS date."""

    def setUp(self):
        self.s = run(build_rate_schedule(_Client([], DAY, NIGHT), PRODUCT, TARIFF, vat=[]))

    def test_september_peak_is_inc_5pc(self):
        self.assertAlmostEqual(self.s.resolve("2026-09-10T12:00:00"), 32.3092, places=4)

    def test_august_night_is_inc_5pc(self):
        self.assertAlmostEqual(self.s.resolve("2026-08-27T02:00:00"), 5.493, places=4)

    def test_from_1_october_the_published_figure_stands(self):
        """GUARD (passes on the unpatched tree too): the fix must not gross up today."""
        self.assertAlmostEqual(self.s.resolve("2026-10-01T12:00:00"), 30.7707, places=4)

    def test_the_change_is_at_local_midnight(self):
        # 23:30 BST on 30 Sep (night, 5%) vs 00:00 BST on 1 Oct (night, 0%).
        self.assertAlmostEqual(self.s.resolve("2026-09-30T22:30:00"), 5.493, places=4)
        self.assertAlmostEqual(self.s.resolve("2026-09-30T23:00:00"), 5.2314, places=4)

    def test_a_correctly_published_figure_is_kept(self):
        """GUARD: a record whose VAT already agrees with the calendar keeps Octopus's figure."""
        rec = [{"value_inc_vat": 24.4965, "value_exc_vat": 23.33,
                "valid_from": "2026-01-01T00:00:00Z", "valid_to": "2026-04-01T00:00:00Z"}]
        s = run(build_rate_schedule(_Client(rec), "P", "E-1R-P-A", vat=[]))
        self.assertEqual(s.resolve("2026-02-01T00:00:00"), 24.4965)

    def test_restating_twice_is_restating_once(self):
        """GUARD: with_vat restates from the published schedule, never compounds."""
        again = self.s.with_vat([])
        for ts in ("2026-09-10T12:00:00", "2026-10-01T12:00:00"):
            self.assertEqual(again.resolve(ts), self.s.resolve(ts))

    def test_export_is_never_grossed_up(self):
        """GUARD: export carries no VAT — inc == exc there is the truth."""
        exp = [{"value_inc_vat": 15.0, "value_exc_vat": 15.0,
                "valid_from": "2026-07-05T23:00:00Z", "valid_to": None}]
        s = run(build_rate_schedule(_Client(exp), "P", "E-1R-OUTGOING-A", vat=NO_VAT))
        self.assertEqual(s.resolve("2026-09-10T12:00:00"), 15.0)

    def test_the_standing_charge_carries_vat_too(self):
        sc = [{"value_inc_vat": 60.0, "value_exc_vat": 60.0,
               "valid_from": "2026-07-05T23:00:00Z", "valid_to": None}]
        s = run(build_standing_charge_schedule(_Client(standing=sc), PRODUCT, TARIFF, vat=[]))
        self.assertAlmostEqual(s.resolve("2026-09-10T12:00:00"), 63.0, places=4)
        self.assertAlmostEqual(s.resolve("2026-10-02T12:00:00"), 60.0, places=4)


class TheLearnerDoesNotBackdate(unittest.TestCase):

    def setUp(self):
        self._s, self._sched = engine._store, engine._kraken_rate_schedules
        self.store = BlockStore(":memory:")
        engine._store = self.store
        raw = RateSchedule.from_api_records(DAY)
        engine._kraken_rate_schedules = {"import": raw.with_vat([])}

    def tearDown(self):
        engine._store, engine._kraken_rate_schedules = self._s, self._sched
        self.store.close()

    def test_september_keeps_5pc(self):
        engine._learn_vat_from_import_schedule("2026-10-01T08:00:00")
        self.assertAlmostEqual(self.store.vat_rate_at("2026-09-10T12:00:00"), 0.05, places=6)
        self.assertAlmostEqual(self.store.vat_rate_at("2026-10-01T12:00:00"), 0.0, places=6)

    def test_an_unseeded_change_is_dated_today(self):
        """Without the seed entry the record still can't say since when — today."""
        with mock.patch.object(vc, "SEED", [("1997-09-01", 0.05)]):
            self.assertTrue(engine._learn_vat_from_import_schedule("2026-10-01T08:00:00"))
            self.assertEqual(self.store.get_vat_calendar(), [("2026-10-01", 0.0)])
            self.assertAlmostEqual(self.store.vat_rate_at("2026-09-10"), 0.05, places=6)

    def test_a_closed_record_is_dated_at_its_start(self):
        """GUARD: history Octopus versioned properly still says when it changed."""
        sched = RateSchedule(
            [("2026-01-01T00:00:00", "2026-03-01T00:00:00", 20.0),
             ("2026-03-01T00:00:00", "2026-05-01T00:00:00", 21.0)],
            exc_periods=[("2026-01-01T00:00:00", "2026-03-01T00:00:00", 20.0),
                         ("2026-03-01T00:00:00", "2026-05-01T00:00:00", 20.0)])
        engine._kraken_rate_schedules = {"import": sched}
        with mock.patch.object(vc, "SEED", [("1997-09-01", 0.05)]):
            engine._learn_vat_from_import_schedule("2026-10-01T08:00:00")
            self.assertAlmostEqual(self.store.vat_rate_at("2026-02-01"), 0.0, places=6)
            self.assertAlmostEqual(self.store.vat_rate_at("2026-03-15"), 0.05, places=6)


class VatResolvesOnTheLocalDate(unittest.TestCase):

    def test_the_first_half_hour_of_1_october_is_0pc(self):
        self.assertEqual(vc.resolve_vat("2026-09-30T23:00:00"), 0.0)
        self.assertEqual(vc.resolve_vat("2026-09-30T22:30:00"), 0.05)

    def test_a_bare_date_is_local(self):
        self.assertEqual(vc.resolve_vat("2026-09-30"), 0.05)
        self.assertEqual(vc.resolve_vat("2026-10-01"), 0.0)


class TheCapDateIsTheLocalAgreementDay(unittest.TestCase):
    """An agreement from 00:00 BST on 26 Aug is 2026-08-25T23:00:00 in naive UTC."""

    def test_26_august_not_25(self):
        with mock.patch.object(engine, "_kraken_current_agreement_from", "2026-08-25T23:00:00"), \
             mock.patch.object(engine, "_import_is_smb_capped", lambda: True), \
             mock.patch.object(engine, "_iog_site_tz", lambda *a, **k: "Europe/London"):
            self.assertEqual(engine._chart_cap_from(), "2026-08-26")


if __name__ == "__main__":
    unittest.main()
