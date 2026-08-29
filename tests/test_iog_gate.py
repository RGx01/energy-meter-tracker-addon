"""
test_iog_gate.py — 4.5.6 IOG stop-gap.

Covers the two guarantees added after Octopus removed the per-slot OFF_PEAK label
from the Measurements API:

  1. The measured-cost pass (which now fetches the gross STANDARD rate and would
     stamp settled off-peak blocks to peak — the 4.5.5 corruption) is OFF.
  2. API import / gap-fill and block DELETE are gated for any window that overlaps
     an Intelligent Octopus Go (IOG) agreement, while CSV import stays allowed.
"""
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import backfill
import engine


class TestMeasuredPassDisabled(unittest.TestCase):
    def test_measured_flags_off(self):
        # The active corruption engine must be held off in 4.5.6.
        self.assertFalse(engine._MEASURED_APPLY,
                         "_MEASURED_APPLY must be False (never write gross STANDARD over a block)")
        self.assertFalse(engine._MEASURED_FETCH_ENABLED,
                         "_MEASURED_FETCH_ENABLED must be False (measurements lost the off-peak band)")


class TestBackfillIogGate(unittest.TestCase):
    def test_api_refused_when_iog_locked(self):
        g = backfill.evaluate_gates("range", "api", api_available=True,
                                    has_blocks=True, gaps_present=False, iog_locked=True)
        self.assertFalse(g["allowed"])
        self.assertEqual(g["reason"], "iog_locked")

    def test_csv_allowed_even_when_iog_locked(self):
        # CSV (from a bill) is black-and-white — never gated by the IOG lock.
        g = backfill.evaluate_gates("range", "csv", api_available=True,
                                    has_blocks=True, gaps_present=False, iog_locked=True)
        self.assertTrue(g["allowed"])

    def test_api_allowed_when_not_locked(self):
        g = backfill.evaluate_gates("range", "api", api_available=True,
                                    has_blocks=True, gaps_present=False, iog_locked=False)
        self.assertTrue(g["allowed"])

    def test_plan_backfill_threads_lock(self):
        plan = backfill.plan_backfill(scope="whole_history", source="api",
                                      api_available=True, has_blocks=True,
                                      iog_locked=True)
        self.assertFalse(plan["ok"])
        self.assertEqual(plan["reason"], "iog_locked")


class TestEngineIogWindows(unittest.TestCase):
    """The shared truth: _iog_agreement_windows / _range_overlaps_iog /
    _block_start_in_iog read the discovered agreement history."""

    def setUp(self):
        self._save = engine._kraken_discovery

    def tearDown(self):
        engine._kraken_discovery = self._save

    def _set(self, agreements):
        engine._kraken_discovery = {"import": {"agreements": agreements}}

    def test_no_iog_agreement_never_locks(self):
        self._set([{"valid_from": "2024-01-01T00:00:00", "valid_to": None,
                    "tariff_code": "E-1R-VAR-22-11-01-A"}])
        self.assertEqual(engine._iog_agreement_windows(), [])
        self.assertFalse(engine._range_overlaps_iog("2026-07-01T00:00:00", "2026-07-31T00:00:00"))
        self.assertFalse(engine._block_start_in_iog("2026-07-21T15:00:00"))

    def test_iog_window_detected(self):
        self._set([{"valid_from": "2026-08-01T00:00:00", "valid_to": None,
                    "tariff_code": "E-1R-INTELLI-FLUX-IMPORT-SMB-A"}])
        wins = engine._iog_agreement_windows()
        self.assertEqual(len(wins), 1)

    def test_point_membership(self):
        self._set([{"valid_from": "2026-08-01T00:00:00", "valid_to": None,
                    "tariff_code": "SOME-IOG-TARIFF"}])
        self.assertTrue(engine._block_start_in_iog("2026-08-15T20:00:00"))   # inside
        self.assertFalse(engine._block_start_in_iog("2026-07-15T20:00:00"))  # before valid_from

    def test_range_overlap_open_ended(self):
        self._set([{"valid_from": "2026-08-01T00:00:00", "valid_to": None,
                    "tariff_code": "IOG-SMB"}])
        # A whole-history-style range that reaches into the IOG window overlaps it.
        self.assertTrue(engine._range_overlaps_iog("2026-06-01T00:00:00", "2026-08-15T00:00:00"))
        # A range entirely before valid_from does not.
        self.assertFalse(engine._range_overlaps_iog("2026-06-01T00:00:00", "2026-07-01T00:00:00"))

    def test_bounded_iog_window(self):
        # Old bounded IOG agreement that later switched away — only that window locks.
        self._set([{"valid_from": "2026-02-01T00:00:00", "valid_to": "2026-05-01T00:00:00",
                    "tariff_code": "INTELLI-IOG"},
                   {"valid_from": "2026-05-01T00:00:00", "valid_to": None,
                    "tariff_code": "E-1R-FLAT-A"}])
        self.assertTrue(engine._block_start_in_iog("2026-03-01T00:00:00"))    # in IOG window
        self.assertFalse(engine._block_start_in_iog("2026-06-01T00:00:00"))   # in the flat window


if __name__ == "__main__":
    unittest.main()
