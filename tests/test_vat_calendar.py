"""VAT-rate calendar: seed, resolve, snap, learn (4.2 BL-23)."""
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import vat_calendar as vc


class TestVatCalendar(unittest.TestCase):

    def test_seed_default_is_five_percent(self):
        self.assertAlmostEqual(vc.resolve_vat("2026-08-11"), 0.05, places=6)
        self.assertAlmostEqual(vc.resolve_vat("1998-01-01"), 0.05, places=6)

    def test_before_first_entry_uses_default(self):
        # Pre-1997 falls back to DEFAULT_RATE (we don't model the old 8%).
        self.assertAlmostEqual(vc.resolve_vat("1990-01-01"), vc.DEFAULT_RATE, places=6)

    def test_snap_to_statutory(self):
        self.assertEqual(vc.snap_vat(0.0476), 0.05)   # inc/exc ≈ 1.05 → 5%
        self.assertEqual(vc.snap_vat(0.0009), 0.0)    # ≈ 0 → 0%
        self.assertEqual(vc.snap_vat(0.19), 0.20)     # ≈ 20%
        self.assertIsNone(vc.snap_vat(None))

    def test_learned_holiday_boundary(self):
        learned = [("2026-10-01", 0.0), ("2027-04-01", 0.05)]   # a 6-month 0% holiday
        self.assertAlmostEqual(vc.resolve_vat("2026-09-30", learned), 0.05, places=6)
        self.assertAlmostEqual(vc.resolve_vat("2026-11-15", learned), 0.0, places=6)
        self.assertAlmostEqual(vc.resolve_vat("2027-05-01", learned), 0.05, places=6)

    def test_collapse_keeps_change_points_only(self):
        series = [("2026-01-01", 0.05), ("2026-02-01", 0.0476),   # both → 5%
                  ("2026-10-01", 0.004), ("2026-11-01", 0.001),   # both → 0%
                  ("2027-04-01", 0.0501)]                          # → 5%
        self.assertEqual(vc.collapse(series),
                         [("2026-01-01", 0.05), ("2026-10-01", 0.0), ("2027-04-01", 0.05)])

    def test_merge_learned_idempotent(self):
        observed = [("2026-10-01", 0.0), ("2027-04-01", 0.05)]
        once = vc.merge_learned([], observed)
        twice = vc.merge_learned(once, observed)     # re-observing the same tariff
        self.assertEqual(once, twice)
        self.assertEqual(once, [("2026-10-01", 0.0), ("2027-04-01", 0.05)])


if __name__ == "__main__":
    unittest.main()
