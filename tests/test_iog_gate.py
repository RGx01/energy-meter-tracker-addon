"""
test_iog_gate.py — 4.5.7 walk-back invariants.

4.5.6 gated API import/gap-fill + block delete for IOG periods, on the (later
disproven) premise that Octopus had removed the per-slot off-peak label. 4.5.7 walks
that back: the label is present for settled data, and the real fix is settled-as-truth
in reconcile (see test_reconcile_settled_guard.py). These assert the gate is gone and
the measured pass never applies.
"""
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import backfill
import engine


class TestMeasuredCheckSafeguards(unittest.TestCase):
    def test_measured_settlement_enabled(self):
        # 4.5.7 (post-#115): settled-cost apply is enabled and reads the four device buckets.
        # No age-gate (settled = authority, both bands apply); the rate-settlement horizon
        # and its 'rates finalising' pill are retired.
        self.assertTrue(engine._MEASURED_APPLY)
        self.assertTrue(engine._MEASURED_FETCH_ENABLED)


class TestImportGateRemoved(unittest.TestCase):
    def test_evaluate_gates_has_no_iog_lock(self):
        # The iog_locked parameter is gone; API import is allowed on its own merits.
        import inspect
        self.assertNotIn("iog_locked", inspect.signature(backfill.evaluate_gates).parameters)
        self.assertNotIn("iog_locked", inspect.signature(backfill.plan_backfill).parameters)

    def test_api_import_allowed(self):
        g = backfill.evaluate_gates("range", "api", api_available=True,
                                    has_blocks=True, gaps_present=False)
        self.assertTrue(g["allowed"])
        self.assertIsNone(g["reason"])

    def test_iog_window_helpers_removed(self):
        # The gate helpers were purpose-built for 4.5.6 and are retired.
        for name in ("_iog_agreement_windows", "_range_overlaps_iog", "_block_start_in_iog"):
            self.assertFalse(hasattr(engine, name), f"engine.{name} should be removed")


if __name__ == "__main__":
    unittest.main()
