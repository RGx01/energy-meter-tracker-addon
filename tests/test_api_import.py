"""Tests for api_import.py — pure chunk planning for the API import route."""
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import api_import as ai


class TestPlanChunks(unittest.TestCase):
    def test_newest_first_contiguous_covers_window(self):
        chunks = ai.plan_chunks("2024-01-01T00:00:00", "2024-07-19T00:00:00",
                                chunk_days=60)
        self.assertEqual(len(chunks), 4)                       # 200d / 60 → 4
        self.assertEqual(chunks[0]["to"], "2024-07-19T00:00:00")   # newest first
        self.assertEqual(chunks[-1]["from"], "2024-01-01T00:00:00")  # reaches start
        for i in range(len(chunks) - 1):                       # abut, no gap/overlap
            self.assertEqual(chunks[i]["from"], chunks[i + 1]["to"])
        for c in chunks:
            self.assertLessEqual(c["days"], 60.0)

    def test_empty_window(self):
        self.assertEqual(ai.plan_chunks("2024-07-19", "2024-01-01"), [])
        self.assertEqual(ai.plan_chunks("2024-01-01", "2024-01-01"), [])

    def test_single_partial_chunk(self):
        chunks = ai.plan_chunks("2024-07-01T00:00:00", "2024-07-19T00:00:00",
                                chunk_days=60)
        self.assertEqual(len(chunks), 1)
        self.assertEqual(chunks[0]["from"], "2024-07-01T00:00:00")
        self.assertEqual(chunks[0]["to"], "2024-07-19T00:00:00")


class TestClampWindow(unittest.TestCase):
    def test_clamps_to_api_retention(self):
        w = ai.clamp_window("2023-01-01T00:00:00", "2026-07-01T00:00:00",
                            earliest_reachable="2024-07-20T00:00:00")
        self.assertTrue(w["ok"])
        self.assertEqual(w["from"], "2024-07-20T00:00:00")     # raised to the wall
        self.assertEqual(w["to"], "2026-07-01T00:00:00")       # go-live
        self.assertIn("api_retention", w["clamped_by"])

    def test_no_clamp_when_reachable(self):
        w = ai.clamp_window("2025-01-01T00:00:00", "2026-07-01T00:00:00",
                            earliest_reachable="2024-07-20T00:00:00")
        self.assertTrue(w["ok"])
        self.assertEqual(w["from"], "2025-01-01T00:00:00")
        self.assertEqual(w["clamped_by"], [])

    def test_empty_window_flagged(self):
        w = ai.clamp_window("2026-08-01T00:00:00", "2026-07-01T00:00:00")
        self.assertFalse(w["ok"])

    def test_offset_input_normalised(self):
        w = ai.clamp_window("2025-06-01T00:30:00+01:00", "2026-07-01T00:00:00")
        self.assertEqual(w["from"], "2025-05-31T23:30:00")     # → UTC


class TestPlanImport(unittest.TestCase):
    def test_end_to_end(self):
        p = ai.plan_import("2023-01-01T00:00:00", "2024-07-19T00:00:00",
                           earliest_reachable="2024-01-01T00:00:00", chunk_days=60)
        self.assertTrue(p["ok"])
        self.assertEqual(p["window"]["from"], "2024-01-01T00:00:00")
        self.assertEqual(p["window"]["to"], "2024-07-19T00:00:00")
        self.assertEqual(p["chunk_count"], 4)

    def test_empty_reports_not_ok(self):
        p = ai.plan_import("2026-08-01T00:00:00", "2026-07-01T00:00:00")
        self.assertFalse(p["ok"])
        self.assertEqual(p["chunk_count"], 0)


if __name__ == "__main__":
    unittest.main()
