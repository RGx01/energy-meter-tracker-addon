"""Tests for the read-only recorder-statistics probe (historical-import spike)."""
import os
import sys
import unittest
from datetime import datetime, timezone, timedelta

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import statistics_probe as sp


def _hourly_ms(start_iso, n, *, value_key="sum", skip=()):
    """n hourly rows from start_iso (UTC), timestamps as epoch-ms floats."""
    t0 = datetime.fromisoformat(start_iso).replace(tzinfo=timezone.utc)
    rows = []
    for i in range(n):
        if i in skip:
            continue
        t = t0 + timedelta(hours=i)
        rows.append({"start": t.timestamp() * 1000.0,
                     "end": (t + timedelta(hours=1)).timestamp() * 1000.0,
                     value_key: float(i)})
    return rows


class TestEnergySensor(unittest.TestCase):
    def test_energy_sum_hourly(self):
        rows = _hourly_ms("2026-01-01T00:00:00", 48)
        meta = {"unit_of_measurement": "kWh", "has_sum": True, "has_mean": False}
        r = sp.build_sensor_probe("sensor.ev_energy", rows, meta)
        self.assertTrue(r["found"])
        self.assertEqual(r["value_kind"], "energy_sum")
        self.assertEqual(r["count"], 48)
        self.assertEqual(r["cadence_seconds"], 3600)
        self.assertTrue(r["cadence_matches_hourly"])
        self.assertEqual(r["raw_start_kind"], "epoch_ms")
        self.assertEqual(r["earliest"], "2026-01-01T00:00:00+00:00")
        self.assertEqual(r["gap_count"], 0)
        self.assertAlmostEqual(r["coverage_pct"], 100.0, places=0)

    def test_power_sensor_is_mean(self):
        rows = _hourly_ms("2026-01-01T00:00:00", 12, value_key="mean")
        meta = {"unit_of_measurement": "W", "has_sum": False, "has_mean": True}
        r = sp.build_sensor_probe("sensor.ev_power", rows, meta)
        self.assertEqual(r["value_kind"], "power_mean")


class TestTimestampsAndGaps(unittest.TestCase):
    def test_iso_string_timestamps(self):
        t0 = datetime(2026, 1, 1, tzinfo=timezone.utc)
        rows = [{"start": (t0 + timedelta(hours=i)).isoformat().replace("+00:00", "Z"),
                 "sum": float(i)} for i in range(6)]
        r = sp.build_sensor_probe("sensor.x", rows, {"has_sum": True})
        self.assertTrue(r["parsed_timestamps"])
        self.assertEqual(r["raw_start_kind"], "iso_string")
        self.assertEqual(r["cadence_seconds"], 3600)

    def test_gap_detected(self):
        rows = _hourly_ms("2026-01-01T00:00:00", 10, skip=(4, 5, 6))
        r = sp.build_sensor_probe("sensor.x", rows, {"has_sum": True})
        self.assertGreaterEqual(r["gap_count"], 1)
        self.assertLess(r["coverage_pct"], 100.0)

    def test_empty(self):
        r = sp.build_sensor_probe("sensor.missing", [], {"has_sum": True})
        self.assertFalse(r["found"])
        self.assertIn("note", r)


class TestDST(unittest.TestCase):
    def test_autumn_transition_flagged(self):
        # UK clocks go back 2026-10-25 (02:00 BST → 01:00 GMT); the local offset
        # changes from +01:00 to +00:00 within this UTC span.
        rows = _hourly_ms("2026-10-25T00:00:00", 6)
        r = sp.build_sensor_probe("sensor.x", rows, {"has_sum": True})
        self.assertTrue(r["dst_samples"])
        self.assertEqual(r["dst_samples"][0]["local_offset_changed_to"], "0:00:00")


if __name__ == "__main__":
    unittest.main()
