"""Unit tests for the unified backfill decision core (backfill.py).

Pure module — no engine/store, so these import it directly with no stubbing.
"""
import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
import backfill  # noqa: E402


class TestGates(unittest.TestCase):
    def test_api_blocked_without_api(self):
        g = backfill.evaluate_gates("whole_history", "api",
                                    api_available=False, has_blocks=False, gaps_present=False)
        self.assertFalse(g["allowed"])
        self.assertEqual(g["reason"], "no_api")

    def test_api_allowed_with_api(self):
        g = backfill.evaluate_gates("whole_history", "api",
                                    api_available=True, has_blocks=False, gaps_present=False)
        self.assertTrue(g["allowed"])

    def test_csv_allowed_without_api(self):
        # CSV never depends on the supplier API.
        g = backfill.evaluate_gates("gaps", "csv",
                                    api_available=False, has_blocks=True, gaps_present=True)
        self.assertTrue(g["allowed"])

    def test_gaps_scope_blocked_when_no_gaps(self):
        g = backfill.evaluate_gates("gaps", "csv",
                                    api_available=True, has_blocks=True, gaps_present=False)
        self.assertFalse(g["allowed"])
        self.assertEqual(g["reason"], "no_gaps")

    def test_whole_history_warns_when_blocks_exist(self):
        g = backfill.evaluate_gates("whole_history", "api",
                                    api_available=True, has_blocks=True, gaps_present=False)
        self.assertTrue(g["allowed"])
        self.assertTrue(g["warnings"])          # non-empty warning about existing data

    def test_whole_history_no_warn_when_fresh(self):
        g = backfill.evaluate_gates("whole_history", "api",
                                    api_available=True, has_blocks=False, gaps_present=False)
        self.assertEqual(g["warnings"], [])

    def test_bad_scope_and_source(self):
        self.assertEqual(backfill.evaluate_gates("nope", "api", api_available=True,
                         has_blocks=False, gaps_present=False)["reason"], "bad_scope")
        self.assertEqual(backfill.evaluate_gates("gaps", "nope", api_available=True,
                         has_blocks=False, gaps_present=True)["reason"], "bad_source")


class TestDispatch(unittest.TestCase):
    def test_whole_history_api_runs_import_job(self):
        d = backfill.dispatch_action("whole_history", "api")
        self.assertEqual(d["action"], "run_api_import_job")
        self.assertFalse(d["needs_csv"])
        self.assertFalse(d["emit_template"])

    def test_gaps_api_resolves_history_gaps(self):
        d = backfill.dispatch_action("gaps", "api")
        self.assertEqual(d["action"], "resolve_history_gaps")

    def test_range_api_runs_import_job_with_note(self):
        d = backfill.dispatch_action("range", "api")
        self.assertEqual(d["action"], "run_api_import_job")
        self.assertIn("end-bound", d["note"])

    def test_csv_any_scope_applies_csv(self):
        for scope in ("whole_history", "range"):
            d = backfill.dispatch_action(scope, "csv")
            self.assertEqual(d["action"], "apply_csv_import")
            self.assertTrue(d["needs_csv"])
            self.assertFalse(d["emit_template"])

    def test_gaps_csv_emits_template(self):
        d = backfill.dispatch_action("gaps", "csv")
        self.assertEqual(d["action"], "apply_csv_import")
        self.assertTrue(d["emit_template"])     # gap-scoped template pre-step


class TestClassifyWindows(unittest.TestCase):
    def test_split_fill_vs_occupied(self):
        target = ["2026-01-01T00:00:00", "2026-01-01T00:30:00", "2026-01-01T01:00:00"]
        occupied = ["2026-01-01T00:30:00"]
        r = backfill.classify_windows(target, occupied)
        self.assertEqual(r["fill"], ["2026-01-01T00:00:00", "2026-01-01T01:00:00"])
        self.assertEqual(r["occupied"], ["2026-01-01T00:30:00"])
        self.assertEqual(r["fill_count"], 2)
        self.assertEqual(r["occupied_count"], 1)

    def test_none_occupied(self):
        r = backfill.classify_windows(["a", "b"], None)
        self.assertEqual(r["fill"], ["a", "b"])
        self.assertEqual(r["occupied"], [])


class TestPlanBackfill(unittest.TestCase):
    def test_gaps_api_ok(self):
        gaps = [{"start": "2026-01-01T00:00:00", "end": "2026-01-01T00:30:00", "slots": 2}]
        plan = backfill.plan_backfill(
            scope="gaps", source="api", api_available=True, has_blocks=True,
            gaps=gaps, target_starts=["2026-01-01T00:00:00", "2026-01-01T00:30:00"],
            occupied_starts=[])
        self.assertTrue(plan["ok"])
        self.assertEqual(plan["action"], "resolve_history_gaps")
        self.assertEqual(plan["windows"]["fill_count"], 2)
        self.assertEqual(plan["gap_runs"], gaps)

    def test_gaps_blocked_no_api(self):
        plan = backfill.plan_backfill(scope="gaps", source="api",
                                      api_available=False, has_blocks=True,
                                      gaps=[{"start": "x", "end": "x", "slots": 1}])
        self.assertFalse(plan["ok"])
        self.assertEqual(plan["reason"], "no_api")

    def test_gaps_csv_ok_emits_template(self):
        plan = backfill.plan_backfill(scope="gaps", source="csv",
                                      api_available=False, has_blocks=True,
                                      gaps=[{"start": "x", "end": "x", "slots": 1}],
                                      target_starts=["x"])
        self.assertTrue(plan["ok"])
        self.assertTrue(plan["emit_template"])

    def test_occupied_windows_handed_off(self):
        plan = backfill.plan_backfill(
            scope="range", source="api", api_available=True, has_blocks=True,
            target_starts=["a", "b", "c"], occupied_starts=["b"])
        self.assertTrue(plan["ok"])
        self.assertEqual(plan["handoff_to_corrections"], ["b"])
        self.assertEqual(plan["windows"]["fill"], ["a", "c"])

    def test_whole_history_fresh_no_warnings(self):
        plan = backfill.plan_backfill(scope="whole_history", source="api",
                                      api_available=True, has_blocks=False)
        self.assertTrue(plan["ok"])
        self.assertEqual(plan["warnings"], [])
        self.assertEqual(plan["action"], "run_api_import_job")


if __name__ == "__main__":
    unittest.main()
