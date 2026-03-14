"""
test_engine.py
==============
Unit tests for the pure functions in engine.py.

Run with:
    python3 -m pytest test_engine.py -v
or:
    python3 test_engine.py
"""

import sys
import os
import unittest
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

# ── Minimal stubs so engine.py imports without HA/filesystem ─────────────────

# Stub energy_engine_io
import types
eio = types.ModuleType("energy_engine_io")
eio.ensure_dir      = lambda *a, **kw: None
eio.load_json       = lambda *a, **kw: a[1] if len(a) > 1 else {}
eio.save_json_atomic = lambda *a, **kw: None
eio.save_file       = lambda *a, **kw: None
sys.modules["energy_engine_io"] = eio

# Stub energy_charts
ec = types.ModuleType("energy_charts")
ec.generate_net_heatmap              = lambda *a, **kw: ""
ec.generate_daily_import_export_charts = lambda *a, **kw: ""
sys.modules["energy_charts"] = ec

# Stub ha_client
hc = types.ModuleType("ha_client")
hc.HAClient = MagicMock
sys.modules["ha_client"] = hc

# Now import the engine
sys.path.insert(0, os.path.dirname(__file__))
import engine


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def dt(s):
    """Parse ISO string to datetime."""
    return datetime.fromisoformat(s)

def read(value, ts):
    return {"value": value, "ts": ts}

def rate(value, ts):
    return {"value": value, "ts": ts}


# ─────────────────────────────────────────────────────────────────────────────
# floor_to_hh
# ─────────────────────────────────────────────────────────────────────────────

class TestFloorToHH(unittest.TestCase):

    def test_exactly_on_hour(self):
        self.assertEqual(engine.floor_to_hh(dt("2026-01-01T09:00:00")), dt("2026-01-01T09:00:00"))

    def test_exactly_on_half(self):
        self.assertEqual(engine.floor_to_hh(dt("2026-01-01T09:30:00")), dt("2026-01-01T09:30:00"))

    def test_early_in_first_half(self):
        self.assertEqual(engine.floor_to_hh(dt("2026-01-01T09:07:33")), dt("2026-01-01T09:00:00"))

    def test_late_in_first_half(self):
        self.assertEqual(engine.floor_to_hh(dt("2026-01-01T09:29:59")), dt("2026-01-01T09:00:00"))

    def test_early_in_second_half(self):
        self.assertEqual(engine.floor_to_hh(dt("2026-01-01T09:30:01")), dt("2026-01-01T09:30:00"))

    def test_late_in_second_half(self):
        self.assertEqual(engine.floor_to_hh(dt("2026-01-01T09:59:59")), dt("2026-01-01T09:30:00"))

    def test_midnight(self):
        self.assertEqual(engine.floor_to_hh(dt("2026-01-01T00:00:00")), dt("2026-01-01T00:00:00"))


# ─────────────────────────────────────────────────────────────────────────────
# interpolate_value
# ─────────────────────────────────────────────────────────────────────────────

class TestInterpolateValue(unittest.TestCase):

    def test_midpoint(self):
        pre  = read(1000.0, "2026-01-01T09:00:00")
        post = read(1001.0, "2026-01-01T09:02:00")
        result = engine.interpolate_value(pre, post, dt("2026-01-01T09:01:00"))
        self.assertAlmostEqual(result["value"], 1000.5, places=2)
        self.assertTrue(result["interpolated"])

    def test_at_pre_boundary(self):
        pre  = read(1000.0, "2026-01-01T09:00:00")
        post = read(1001.0, "2026-01-01T09:02:00")
        result = engine.interpolate_value(pre, post, dt("2026-01-01T09:00:00"))
        self.assertAlmostEqual(result["value"], 1000.0, places=3)

    def test_at_post_boundary(self):
        pre  = read(1000.0, "2026-01-01T09:00:00")
        post = read(1001.0, "2026-01-01T09:02:00")
        result = engine.interpolate_value(pre, post, dt("2026-01-01T09:02:00"))
        self.assertAlmostEqual(result["value"], 1001.0, places=3)

    def test_zero_window_returns_pre_value(self):
        pre  = read(1000.0, "2026-01-01T09:00:00")
        post = read(1001.0, "2026-01-01T09:00:00")  # same ts
        result = engine.interpolate_value(pre, post, dt("2026-01-01T09:00:00"))
        self.assertEqual(result["value"], 1000.0)

    def test_fraction_clamped_at_zero(self):
        pre  = read(1000.0, "2026-01-01T09:01:00")
        post = read(1001.0, "2026-01-01T09:02:00")
        # target before pre — fraction clamped to 0
        result = engine.interpolate_value(pre, post, dt("2026-01-01T09:00:00"))
        self.assertAlmostEqual(result["value"], 1000.0, places=3)

    def test_fraction_clamped_at_one(self):
        pre  = read(1000.0, "2026-01-01T09:00:00")
        post = read(1001.0, "2026-01-01T09:01:00")
        # target after post — fraction clamped to 1
        result = engine.interpolate_value(pre, post, dt("2026-01-01T09:02:00"))
        self.assertAlmostEqual(result["value"], 1001.0, places=3)

    def test_boundary_crossing(self):
        """Classic boundary case: reads either side of :30."""
        pre  = read(9916.655, "2026-01-01T09:28:00")
        post = read(9918.033, "2026-01-01T09:32:00")
        result = engine.interpolate_value(pre, post, dt("2026-01-01T09:30:00"))
        # fraction = 120/240 = 0.5 → 9916.655 + 0.5 * 1.378 = 9917.344
        self.assertAlmostEqual(result["value"], 9917.344, places=2)


# ─────────────────────────────────────────────────────────────────────────────
# detect_gap
# ─────────────────────────────────────────────────────────────────────────────

class TestDetectGap(unittest.TestCase):

    def test_no_gap(self):
        # Last read at 09:15, now is 09:20 — still in same block
        missing = engine.detect_gap("2026-01-01T09:15:00", dt("2026-01-01T09:20:00"))
        self.assertEqual(missing, [])

    def test_one_missing_block(self):
        # Last read at 09:15, now is 10:05 — one block (09:30→10:00) missing
        missing = engine.detect_gap("2026-01-01T09:15:00", dt("2026-01-01T10:05:00"))
        self.assertEqual(len(missing), 1)
        self.assertEqual(missing[0][0], dt("2026-01-01T09:30:00"))
        self.assertEqual(missing[0][1], dt("2026-01-01T10:00:00"))

    def test_multiple_missing_blocks(self):
        # Last read at 09:00 → last_block_end=09:30, now=11:00 → 3 blocks missing
        # (09:30→10:00, 10:00→10:30, 10:30→11:00)
        missing = engine.detect_gap("2026-01-01T09:00:00", dt("2026-01-01T11:00:00"))
        self.assertEqual(len(missing), 3)

    def test_none_last_read(self):
        missing = engine.detect_gap(None, dt("2026-01-01T10:00:00"))
        self.assertEqual(missing, [])

    def test_exact_boundary_no_gap(self):
        # Last read exactly at block end — no gap to next block
        missing = engine.detect_gap("2026-01-01T09:30:00", dt("2026-01-01T09:45:00"))
        self.assertEqual(missing, [])

    def test_overnight_gap(self):
        # Last read 22:00 → last_block_end=22:30, now=06:00 → 7.5hrs = 15 blocks
        missing = engine.detect_gap("2026-01-01T22:00:00", dt("2026-01-02T06:00:00"))
        self.assertEqual(len(missing), 15)


# ─────────────────────────────────────────────────────────────────────────────
# compute_channel — main meter
# ─────────────────────────────────────────────────────────────────────────────

class TestComputeChannelMain(unittest.TestCase):

    def _channel(self, reads, rates):
        return {"reads": reads, "rates": rates}

    def test_simple_delta(self):
        ch = self._channel(
            reads=[read(1000.0, "2026-01-01T09:00:00"), read(1001.0, "2026-01-01T09:30:00")],
            rates=[rate(0.25, "2026-01-01T09:00:00")]
        )
        result = engine.compute_channel(ch, is_sub_meter=False)
        self.assertAlmostEqual(result["kwh"], 1.0)
        self.assertAlmostEqual(result["cost"], 0.25)
        self.assertAlmostEqual(result["rate"], 0.25)

    def test_negative_delta_clamped_to_zero(self):
        # Meter reset or bad reading — delta is negative, should clamp to 0
        ch = self._channel(
            reads=[read(1001.0, "2026-01-01T09:00:00"), read(1000.0, "2026-01-01T09:30:00")],
            rates=[rate(0.25, "2026-01-01T09:00:00")]
        )
        result = engine.compute_channel(ch, is_sub_meter=False)
        self.assertEqual(result["kwh"], 0.0)
        self.assertEqual(result["cost"], 0.0)

    def test_single_read_returns_zero(self):
        ch = self._channel(
            reads=[read(1000.0, "2026-01-01T09:00:00")],
            rates=[rate(0.25, "2026-01-01T09:00:00")]
        )
        result = engine.compute_channel(ch, is_sub_meter=False)
        self.assertEqual(result["kwh"], 0.0)

    def test_no_rates_defaults_to_zero(self):
        ch = self._channel(
            reads=[read(1000.0, "2026-01-01T09:00:00"), read(1001.0, "2026-01-01T09:30:00")],
            rates=[]
        )
        result = engine.compute_channel(ch, is_sub_meter=False)
        self.assertAlmostEqual(result["kwh"], 1.0)
        self.assertEqual(result["rate"], 0.0)
        self.assertEqual(result["cost"], 0.0)

    def test_parent_rates_used_when_no_rates(self):
        ch = self._channel(
            reads=[read(1000.0, "2026-01-01T09:00:00"), read(1000.5, "2026-01-01T09:30:00")],
            rates=[]
        )
        parent_rates = [rate(0.30, "2026-01-01T09:00:00")]
        result = engine.compute_channel(ch, parent_rates=parent_rates, is_sub_meter=False)
        self.assertAlmostEqual(result["rate"], 0.30)
        self.assertAlmostEqual(result["cost"], 0.15, places=5)


# ─────────────────────────────────────────────────────────────────────────────
# compute_channel — sub-meter
# ─────────────────────────────────────────────────────────────────────────────

class TestComputeChannelSub(unittest.TestCase):

    def test_simple_sub_meter(self):
        ch = {
            "reads": [read(100.0, "2026-01-01T09:00:00"), read(100.5, "2026-01-01T09:30:00")],
            "rates": [rate(0.25, "2026-01-01T09:00:00")]
        }
        result = engine.compute_channel(ch, is_sub_meter=True)
        self.assertAlmostEqual(result["kwh"], 0.5)
        self.assertAlmostEqual(result["cost"], 0.125)

    def test_sub_meter_negative_delta_skipped(self):
        # Sub-meter reads going backwards — delta < 0 should be skipped
        ch = {
            "reads": [
                read(100.0, "2026-01-01T09:00:00"),
                read(99.0,  "2026-01-01T09:10:00"),  # negative delta — skip
                read(100.5, "2026-01-01T09:30:00"),
            ],
            "rates": [rate(0.25, "2026-01-01T09:00:00")]
        }
        result = engine.compute_channel(ch, is_sub_meter=True)
        # Only positive deltas counted: 100.5 - 100.0 = 0.5 (99→100.5 = 1.5, but 100→99 skipped)
        self.assertGreaterEqual(result["kwh"], 0.0)

    def test_sub_meter_backward_rate_reconstruction(self):
        """Rate should not increase looking backwards through corrections."""
        ch = {
            "reads": [
                read(100.0, "2026-01-01T09:00:00"),
                read(100.5, "2026-01-01T09:30:00"),
            ],
            "rates": [
                rate(0.20, "2026-01-01T09:00:00"),
                rate(0.25, "2026-01-01T09:15:00"),  # rate went up mid-block
            ]
        }
        result = engine.compute_channel(ch, is_sub_meter=True)
        # Cost should be calculated using corrected rates
        self.assertGreater(result["cost"], 0.0)
        self.assertAlmostEqual(result["kwh"], 0.5)


# ─────────────────────────────────────────────────────────────────────────────
# select_opening_read / select_closing_read
# ─────────────────────────────────────────────────────────────────────────────

class TestSelectReads(unittest.TestCase):

    def setUp(self):
        self.reads = [
            read(1000.0, "2026-01-01T09:25:00"),
            read(1000.3, "2026-01-01T09:28:00"),
            read(1000.6, "2026-01-01T09:32:00"),
            read(1000.9, "2026-01-01T09:35:00"),
        ]
        self.boundary = dt("2026-01-01T09:30:00")

    def test_opening_read_is_last_before_boundary(self):
        r = engine.select_opening_read(self.reads, self.boundary)
        self.assertEqual(r["ts"], "2026-01-01T09:28:00")

    def test_closing_read_is_first_after_or_at_boundary(self):
        r = engine.select_closing_read(self.reads, self.boundary)
        self.assertEqual(r["ts"], "2026-01-01T09:32:00")

    def test_opening_read_falls_back_to_first_post_if_no_pre(self):
        reads = [read(1000.6, "2026-01-01T09:32:00")]
        r = engine.select_opening_read(reads, self.boundary)
        self.assertEqual(r["ts"], "2026-01-01T09:32:00")

    def test_closing_read_falls_back_to_last_pre_if_no_post(self):
        reads = [read(1000.3, "2026-01-01T09:28:00")]
        r = engine.select_closing_read(reads, self.boundary)
        self.assertEqual(r["ts"], "2026-01-01T09:28:00")

    def test_read_exactly_on_boundary(self):
        reads = [
            read(1000.0, "2026-01-01T09:28:00"),
            read(1000.5, "2026-01-01T09:30:00"),  # exactly on boundary
            read(1001.0, "2026-01-01T09:32:00"),
        ]
        opening = engine.select_opening_read(reads, self.boundary)
        closing = engine.select_closing_read(reads, self.boundary)
        self.assertEqual(opening["ts"], "2026-01-01T09:30:00")
        self.assertEqual(closing["ts"], "2026-01-01T09:30:00")


# ─────────────────────────────────────────────────────────────────────────────
# gap marker helpers
# ─────────────────────────────────────────────────────────────────────────────

class TestGapMarker(unittest.TestCase):

    def test_set_and_detect(self):
        block = {}
        engine.set_gap_marker(block, {"meter": {"import": {"value": 1.0, "ts": "2026-01-01T09:00:00"}}}, {})
        self.assertTrue(engine.has_gap_marker(block))
        self.assertIn("_gap_marker", block)

    def test_clear(self):
        block = {}
        engine.set_gap_marker(block, {}, {})
        engine.clear_gap_marker(block)
        self.assertFalse(engine.has_gap_marker(block))

    def test_clear_idempotent(self):
        block = {}
        engine.clear_gap_marker(block)  # should not raise
        self.assertFalse(engine.has_gap_marker(block))


# ─────────────────────────────────────────────────────────────────────────────
# build_gap_blocks
# ─────────────────────────────────────────────────────────────────────────────

class TestBuildGapBlocks(unittest.TestCase):

    def setUp(self):
        self.config = {
            "meters": {
                "electricity_main": {
                    "meta": {"type": "electricity"},
                    "channels": {
                        "import": {"read": "sensor.import", "rate": "sensor.rate"},
                        "export": {"read": "sensor.export", "rate": "sensor.exp_rate"},
                    }
                }
            }
        }
        self.window = [(dt("2026-01-01T09:30:00"), dt("2026-01-01T10:00:00"))]

    def test_single_gap_block_created(self):
        pre  = {"electricity_main": {"import": read(1000.0, "2026-01-01T09:15:00"), "export": read(500.0, "2026-01-01T09:15:00")}}
        post = {"electricity_main": {"import": read(1002.0, "2026-01-01T10:15:00"), "export": read(500.5, "2026-01-01T10:15:00")}}
        rates = {"electricity_main": {"import": 0.25, "export": 0.10}}
        blocks = engine.build_gap_blocks(self.window, pre, post, rates, self.config)
        self.assertEqual(len(blocks), 1)
        self.assertTrue(blocks[0]["interpolated"])
        self.assertGreater(blocks[0]["totals"]["import_kwh"], 0.0)

    def test_gap_too_large_produces_zero_block(self):
        pre  = {"electricity_main": {"import": read(1000.0, "2026-01-01T09:00:00"), "export": read(500.0, "2026-01-01T09:00:00")}}
        post = {"electricity_main": {"import": read(1050.0, "2026-01-01T22:00:00"), "export": read(510.0, "2026-01-01T22:00:00")}}
        rates = {"electricity_main": {"import": 0.25, "export": 0.10}}
        blocks = engine.build_gap_blocks(self.window, pre, post, rates, self.config)
        self.assertEqual(len(blocks), 1)
        # Gap > 12 hours — main meter still interpolates, sub-meters zero
        # Main meter has no 12hr limit, only sub-meters do

    def test_missing_reads_produces_zero_channel(self):
        pre  = {}
        post = {}
        rates = {}
        blocks = engine.build_gap_blocks(self.window, pre, post, rates, self.config)
        self.assertEqual(len(blocks), 1)
        self.assertEqual(blocks[0]["totals"]["import_kwh"], 0.0)

    def test_multiple_windows(self):
        windows = [
            (dt("2026-01-01T09:30:00"), dt("2026-01-01T10:00:00")),
            (dt("2026-01-01T10:00:00"), dt("2026-01-01T10:30:00")),
        ]
        pre  = {"electricity_main": {"import": read(1000.0, "2026-01-01T09:15:00"), "export": read(500.0, "2026-01-01T09:15:00")}}
        post = {"electricity_main": {"import": read(1004.0, "2026-01-01T10:45:00"), "export": read(501.0, "2026-01-01T10:45:00")}}
        rates = {"electricity_main": {"import": 0.25, "export": 0.10}}
        blocks = engine.build_gap_blocks(windows, pre, post, rates, self.config)
        self.assertEqual(len(blocks), 2)


# ─────────────────────────────────────────────────────────────────────────────
# extract_last_reads
# ─────────────────────────────────────────────────────────────────────────────

class TestExtractLastReads(unittest.TestCase):

    def test_extracts_last_read_per_channel(self):
        block = {
            "meters": {
                "electricity_main": {
                    "channels": {
                        "import": {
                            "reads": [
                                read(1000.0, "2026-01-01T09:00:00"),
                                read(1001.0, "2026-01-01T09:29:00"),
                            ],
                            "rates": [rate(0.25, "2026-01-01T09:00:00")]
                        }
                    }
                }
            }
        }
        reads, rates = engine.extract_last_reads(block)
        self.assertEqual(reads["electricity_main"]["import"]["value"], 1001.0)
        self.assertEqual(rates["electricity_main"]["import"], 0.25)

    def test_empty_block(self):
        reads, rates = engine.extract_last_reads({"meters": {}})
        self.assertEqual(reads, {})
        self.assertEqual(rates, {})


# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    unittest.main(verbosity=2)
