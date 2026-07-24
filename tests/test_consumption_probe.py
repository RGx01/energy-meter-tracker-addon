"""Tests for the read-only export-retention probe (historical-import spike)."""
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import consumption_probe as cp


def _row(iso):
    return {"consumption": 0.123, "interval_start": iso,
            "interval_end": iso}


class TestBoundaryReport(unittest.TestCase):
    def test_available_span(self):
        r = cp.build_consumption_boundary(
            "import", _row("2024-06-12T13:00:00Z"), _row("2026-07-19T20:00:00Z"))
        self.assertTrue(r["available"])
        self.assertEqual(r["channel"], "import")
        self.assertEqual(r["earliest"], "2024-06-12T13:00:00+00:00")
        self.assertEqual(r["latest"], "2026-07-19T20:00:00+00:00")
        self.assertEqual(r["raw_time_kind"], "iso_string")
        # ~2 years; +30 min tail included, so strictly > raw diff.
        self.assertGreater(r["span_days"], 760)

    def test_offset_timestamp_normalised(self):
        # A BST +01:00 stamp normalises to UTC.
        r = cp.build_consumption_boundary(
            "import", _row("2025-06-01T00:30:00+01:00"),
            _row("2025-06-02T00:30:00+01:00"))
        self.assertTrue(r["earliest"].endswith("+00:00"))
        self.assertEqual(r["earliest"], "2025-05-31T23:30:00+00:00")

    def test_unavailable_when_no_first_row(self):
        r = cp.build_consumption_boundary("export", None, None)
        self.assertFalse(r["available"])
        self.assertIn("note", r)

    def test_unparseable_timestamp(self):
        r = cp.build_consumption_boundary("export", _row("not-a-date"), None)
        self.assertFalse(r["available"])

    def test_latest_missing_gives_no_span(self):
        r = cp.build_consumption_boundary("import", _row("2024-06-12T13:00:00Z"), None)
        self.assertTrue(r["available"])
        self.assertEqual(r["earliest"], "2024-06-12T13:00:00+00:00")
        self.assertNotIn("span_days", r)


class TestExportLag(unittest.TestCase):
    def _chan(self, imp_iso, exp_iso):
        return {
            "import": cp.build_consumption_boundary("import", _row(imp_iso), _row(imp_iso)),
            "export": (cp.build_consumption_boundary("export", _row(exp_iso), _row(exp_iso))
                       if exp_iso else {"channel": "export", "available": False}),
        }

    def test_export_starts_later(self):
        # import 2024-06-12 13:00, export 2024-06-29 03:00 → 16d14h → 16 whole days.
        ch = self._chan("2024-06-12T13:00:00Z", "2024-06-29T03:00:00Z")
        self.assertEqual(cp.export_lag_days(ch), 16)

    def test_export_reaches_as_far(self):
        ch = self._chan("2024-06-12T13:00:00Z", "2024-06-12T13:00:00Z")
        self.assertEqual(cp.export_lag_days(ch), 0)

    def test_export_earlier_clamps_to_zero(self):
        ch = self._chan("2024-06-29T03:00:00Z", "2024-06-12T13:00:00Z")
        self.assertEqual(cp.export_lag_days(ch), 0)

    def test_none_when_export_unavailable(self):
        ch = self._chan("2024-06-12T13:00:00Z", None)
        self.assertIsNone(cp.export_lag_days(ch))


if __name__ == "__main__":
    unittest.main()