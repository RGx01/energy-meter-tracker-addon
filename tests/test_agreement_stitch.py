"""BL-54 — the import RateSchedule is stitched across ALL agreements, so a block
is priced on the tariff that applied on its date (not the current one); plus the
guard that stops the overlay/reconcile pricing a pre-migration block from a
current-tariff-only schedule when the stitch is unavailable.
"""
import asyncio
import unittest

from kraken_rates import build_agreement_stitched_schedule, RateSchedule


def _win(day, night, days):
    """Windowed standard-unit-rates: night 22:30-04:30Z, day 04:30-22:30Z."""
    out = []
    for d in days:
        y, m, dd = d.split("-")
        prev = f"{y}-{m}-{int(dd) - 1:02d}"
        out += [{"value_inc_vat": night, "valid_from": f"{prev}T22:30:00Z",
                 "valid_to": f"{d}T04:30:00Z"},
                {"value_inc_vat": day, "valid_from": f"{d}T04:30:00Z",
                 "valid_to": f"{d}T22:30:00Z"}]
    return out



def _run(coro):
    """Fresh event loop per call — robust when a prior test module closed the
    shared default loop (test-isolation, not shared-loop reuse)."""
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()

class _Mock:
    """INTELLI-FIX = windowed standard-unit-rates (day 28 / night 6);
    IOG-SMB-FIX = flat day/night buckets (32.309 / 5.493) → reconstruction."""
    def __init__(self):
        self.calls = {}

    @staticmethod
    def _tariff_to_product_code(tc):
        return "-".join(tc.split("-")[2:-1])

    async def get_unit_rates(self, product, tariff, *, rate_type="standard-unit-rates",
                             period_from=None, period_to=None):
        self.calls[tariff] = self.calls.get(tariff, 0) + 1
        if "INTELLI-FIX" in tariff:
            if rate_type == "standard-unit-rates":
                return _win(28.0, 6.0, ["2026-08-22", "2026-08-23", "2026-08-24", "2026-08-25", "2026-08-26"])
            return []
        if "IOG-SMB-FIX" in tariff:
            if rate_type == "day-unit-rates":
                return [{"value_inc_vat": 32.309235, "valid_from": "2026-07-05T23:00:00Z",
                         "valid_to": None}]
            if rate_type == "night-unit-rates":
                return [{"value_inc_vat": 5.49297, "valid_from": "2026-07-05T23:00:00Z",
                         "valid_to": None}]
            return []
        return []


AGS = [
    {"tariff_code": "E-1R-INTELLI-FIX-12M-26-03-17-B",
     "valid_from": "2026-03-18T00:00:00Z", "valid_to": "2026-08-26T00:00:00+01:00"},
    {"tariff_code": "E-1R-IOG-SMB-FIX-12M-26-03-17-B",
     "valid_from": "2026-08-26T00:00:00+01:00", "valid_to": None},
]
# migration boundary = 2026-08-26T00:00+01:00 = 2026-08-25T23:00Z


def _stitch(client=None):
    client = client or _Mock()
    return _run(
        build_agreement_stitched_schedule(client, AGS)), client


class TestAgreementStitch(unittest.TestCase):
    def test_prices_each_block_on_its_own_agreement(self):
        s, _ = _stitch()
        # pre-migration → INTELLI-FIX (28 day / 6 night)
        self.assertAlmostEqual(s.resolve("2026-08-23T10:00:00"), 28.0, places=5)
        self.assertAlmostEqual(s.resolve("2026-08-23T02:00:00"), 6.0, places=5)
        # post-migration → SMB (32.309 day / 5.493 night)
        self.assertAlmostEqual(s.resolve("2026-08-27T10:00:00"), 32.309235, places=5)
        self.assertAlmostEqual(s.resolve("2026-08-27T02:00:00"), 5.49297, places=5)

    def test_boundary_handoff_at_valid_from(self):
        s, _ = _stitch()
        # night straddling the 23:00Z migration boundary
        self.assertAlmostEqual(s.resolve("2026-08-25T22:45:00"), 6.0, places=5)   # pre → INTELLI-FIX
        self.assertAlmostEqual(s.resolve("2026-08-25T23:30:00"), 5.49297, places=5)  # post → SMB

    def test_closed_agreement_cached(self):
        # two stitches on the same client → the CLOSED agreement is fetched once
        c = _Mock()
        _run(
            build_agreement_stitched_schedule(c, AGS))
        first = c.calls.get("E-1R-INTELLI-FIX-12M-26-03-17-B", 0)
        _run(
            build_agreement_stitched_schedule(c, AGS))
        second = c.calls.get("E-1R-INTELLI-FIX-12M-26-03-17-B", 0)
        self.assertEqual(second, first)  # no extra fetch — served from cache

    def test_empty_agreements(self):
        s = _run(
            build_agreement_stitched_schedule(_Mock(), []))
        self.assertTrue(s.is_empty())


class TestAgreementGuard(unittest.TestCase):
    """_agreement_priced_ok gates the overlay/reconcile when the schedule can't
    cover history (stitch failed → current-tariff-only)."""
    def setUp(self):
        import engine
        self.engine = engine
        self._save = (engine._import_schedule_covers_history,
                      engine._kraken_current_agreement_from)

    def tearDown(self):
        self.engine._import_schedule_covers_history = self._save[0]
        self.engine._kraken_current_agreement_from = self._save[1]

    def test_covers_history_allows_all(self):
        self.engine._import_schedule_covers_history = True
        self.engine._kraken_current_agreement_from = "2026-08-25T23:00:00"
        self.assertTrue(self.engine._agreement_priced_ok("2026-08-23T10:00:00"))

    def test_uncovered_pre_migration_blocked(self):
        self.engine._import_schedule_covers_history = False
        self.engine._kraken_current_agreement_from = "2026-08-25T23:00:00"
        self.assertFalse(self.engine._agreement_priced_ok("2026-08-23T10:00:00"))  # pre-migration
        self.assertTrue(self.engine._agreement_priced_ok("2026-08-27T10:00:00"))   # within current

    def test_unknown_boundary_allows(self):
        self.engine._import_schedule_covers_history = False
        self.engine._kraken_current_agreement_from = None
        self.assertTrue(self.engine._agreement_priced_ok("2026-08-23T10:00:00"))


if __name__ == "__main__":
    unittest.main()
