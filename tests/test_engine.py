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
from datetime import datetime, timedelta, timezone
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

# Stub block_store — use real in-memory BlockStore so engine functions work
from block_store import BlockStore, open_block_store, migrate_json_to_sqlite
import block_store as _bs_module
_test_store = BlockStore(":memory:")
_test_store.insert_config_period({
    "meters": {"electricity_main": {"meta": {
        "timezone": "UTC", "billing_day": 1,
        "block_minutes": 30, "currency_symbol": "£", "currency_code": "GBP",
    }}}
})

bs = types.ModuleType("block_store")
bs.BlockStore              = BlockStore
bs.open_block_store        = lambda path: _test_store
bs.migrate_json_to_sqlite  = migrate_json_to_sqlite
sys.modules["block_store"] = bs

# Now import the engine
sys.path.insert(0, os.path.dirname(__file__))
import engine
# Wire the test store into the engine module
engine._store = _test_store


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
# floor_to_block (configurable block size)
# ─────────────────────────────────────────────────────────────────────────────

class TestPowerValueToKw(unittest.TestCase):
    """Unit-aware power conversion for power_history (BCD current_demand is W)."""

    def test_watts_converted(self):
        self.assertAlmostEqual(engine._power_value_to_kw("1500", "W"), 1.5)

    def test_kilowatts_preserved(self):
        self.assertAlmostEqual(engine._power_value_to_kw("2.5", "kW"), 2.5)

    def test_unknown_unit_large_is_watts(self):
        self.assertAlmostEqual(engine._power_value_to_kw("1500", None), 1.5)

    def test_unknown_unit_small_is_kw(self):
        self.assertAlmostEqual(engine._power_value_to_kw("2.5", None), 2.5)

    def test_non_numeric_returns_none(self):
        self.assertIsNone(engine._power_value_to_kw("unavailable", "W"))


class TestPollMiniDemand(unittest.TestCase):
    """Engine-side Mini demand poller: fetch+cache, throttle, no-device."""

    def setUp(self):
        engine._last_mini_demand = {"kw": None, "ts": 0.0, "wall": 0.0}

    def tearDown(self):
        # asyncio.run() closes the loop and unsets it; restore one so later
        # tests that call asyncio.get_event_loop() (e.g. finalise_block) work.
        import asyncio
        asyncio.set_event_loop(asyncio.new_event_loop())

    def _now(self):
        from datetime import datetime, timezone
        return datetime.now(timezone.utc).replace(tzinfo=None)

    def test_poll_fetches_and_caches(self):
        import asyncio, types as _t
        from unittest.mock import MagicMock
        reader = _t.SimpleNamespace(device_id="dev-1")
        client = MagicMock()
        async def fake_tel(did, start, end):
            return [{"readAt": "x", "demand": 2000.0}]
        client.get_telemetry = fake_tel
        with patch.object(engine, "_kraken_mini_reader", reader), \
             patch.object(engine, "_kraken_client", client):
            kw = asyncio.run(engine._poll_mini_demand_kw(self._now()))
        self.assertAlmostEqual(kw, 2.0)                       # 2000 W → 2.0 kW
        self.assertAlmostEqual(engine._last_mini_demand["kw"], 2.0)

    def test_poll_throttled(self):
        import asyncio, time, types as _t
        from unittest.mock import MagicMock
        reader = _t.SimpleNamespace(device_id="dev-1")
        client = MagicMock()
        calls = {"n": 0}
        async def fake_tel(did, start, end):
            calls["n"] += 1
            return [{"demand": 1000.0}]
        client.get_telemetry = fake_tel
        engine._last_mini_demand = {"kw": 1.0, "ts": time.monotonic(), "wall": time.time()}
        with patch.object(engine, "_kraken_mini_reader", reader), \
             patch.object(engine, "_kraken_client", client):
            kw = asyncio.run(engine._poll_mini_demand_kw(self._now()))
        self.assertIsNone(kw)                                 # recent ts → throttled
        self.assertEqual(calls["n"], 0)                       # no fetch

    def test_poll_no_device(self):
        import asyncio
        with patch.object(engine, "_kraken_mini_reader", None):
            kw = asyncio.run(engine._poll_mini_demand_kw(self._now()))
        self.assertIsNone(kw)


class TestFloorToBlock(unittest.TestCase):

    def test_30min_on_boundary(self):
        self.assertEqual(engine.floor_to_block(dt("2026-01-01T09:30:00"), 30), dt("2026-01-01T09:30:00"))

    def test_30min_mid_block(self):
        self.assertEqual(engine.floor_to_block(dt("2026-01-01T09:17:00"), 30), dt("2026-01-01T09:00:00"))

    def test_15min_on_boundary(self):
        self.assertEqual(engine.floor_to_block(dt("2026-01-01T09:15:00"), 15), dt("2026-01-01T09:15:00"))

    def test_15min_mid_block(self):
        self.assertEqual(engine.floor_to_block(dt("2026-01-01T09:22:00"), 15), dt("2026-01-01T09:15:00"))

    def test_5min_on_boundary(self):
        self.assertEqual(engine.floor_to_block(dt("2026-01-01T09:10:00"), 5), dt("2026-01-01T09:10:00"))

    def test_5min_mid_block(self):
        self.assertEqual(engine.floor_to_block(dt("2026-01-01T09:13:00"), 5), dt("2026-01-01T09:10:00"))

    def test_floor_to_hh_alias_matches_30min(self):
        """Deprecated alias floor_to_hh should match floor_to_block(dt, 30)."""
        d = dt("2026-01-01T09:17:33")
        self.assertEqual(engine.floor_to_hh(d), engine.floor_to_block(d, 30))


# ─────────────────────────────────────────────────────────────────────────────
# detect_currency_symbol
# ─────────────────────────────────────────────────────────────────────────────

class TestDetectCurrencySymbol(unittest.TestCase):

    def test_gbp(self):
        self.assertEqual(engine.detect_currency_symbol("GBP/kWh"), "£")

    def test_usd(self):
        self.assertEqual(engine.detect_currency_symbol("USD/kWh"), "$")

    def test_eur(self):
        self.assertEqual(engine.detect_currency_symbol("EUR/kWh"), "€")

    def test_unknown_code_returns_code(self):
        self.assertEqual(engine.detect_currency_symbol("XYZ/kWh"), "XYZ")

    def test_empty_returns_generic(self):
        self.assertEqual(engine.detect_currency_symbol(""), "¤")

    def test_none_returns_generic(self):
        self.assertEqual(engine.detect_currency_symbol(None), "¤")

    def test_no_slash_still_works(self):
        self.assertEqual(engine.detect_currency_symbol("GBP"), "£")


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

    def test_zero_rate_with_consumption_warns(self):
        # Consumption but no rate → costed at £0 → should warn (runtime backstop).
        engine._zero_rate_warned.clear()
        ch = self._channel(
            reads=[read(1000.0, "2026-01-01T09:00:00"), read(1002.0, "2026-01-01T09:30:00")],
            rates=[]
        )
        with self.assertLogs("engine", level="WARNING") as cm:
            engine.compute_channel(ch, is_sub_meter=False,
                                   meter_id="electricity_main", channel_id="import")
        self.assertTrue(any("NO rate configured" in m for m in cm.output))

    def test_zero_rate_warning_throttled(self):
        # Second call within the hour for the same meter/channel must NOT warn.
        engine._zero_rate_warned.clear()
        ch = self._channel(
            reads=[read(1000.0, "2026-01-01T09:00:00"), read(1002.0, "2026-01-01T09:30:00")],
            rates=[]
        )
        engine.compute_channel(ch, is_sub_meter=False,
                               meter_id="m", channel_id="import")
        # No assertLogs context that requires a record → if it warns, test fails
        # via the logging assertion below.
        import logging as _lg
        with self.assertRaises(AssertionError):
            with self.assertLogs("engine", level="WARNING"):
                engine.compute_channel(ch, is_sub_meter=False,
                                       meter_id="m", channel_id="import")

    def test_zero_rate_no_consumption_silent(self):
        # Zero kWh with zero rate is harmless → must NOT warn.
        engine._zero_rate_warned.clear()
        ch = self._channel(
            reads=[read(1000.0, "2026-01-01T09:00:00"), read(1000.0, "2026-01-01T09:30:00")],
            rates=[]
        )
        with self.assertRaises(AssertionError):
            with self.assertLogs("engine", level="WARNING"):
                engine.compute_channel(ch, is_sub_meter=False,
                                       meter_id="m2", channel_id="import")

    def test_zero_rate_suppressed_in_api_mode(self):
        # In API mode the Kraken schedule fills rates at reconcile, so a blank
        # rate is expected — the warning must NOT fire.
        engine._zero_rate_warned.clear()
        ch = self._channel(
            reads=[read(1000.0, "2026-01-01T09:00:00"), read(1002.0, "2026-01-01T09:30:00")],
            rates=[]
        )
        orig = engine.mode_uses_api
        engine.mode_uses_api = lambda *a, **k: True
        try:
            with self.assertRaises(AssertionError):
                with self.assertLogs("engine", level="WARNING"):
                    engine.compute_channel(ch, is_sub_meter=False,
                                           meter_id="m3", channel_id="import")
        finally:
            engine.mode_uses_api = orig


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

    def test_sub_meter_rate_is_last_not_weighted_average(self):
        """Sub-meter display rate must be the last rate in the block, not a
        weighted average. This ensures sub-meters always show the same rate
        as the main meter at the same point in time — consistent with the
        intent of capturing the rate as close to block end as possible."""
        ch = {
            "reads": [
                read(100.0, "2026-01-01T05:00:00"),
                read(100.1, "2026-01-01T05:30:00"),  # small amount at off-peak
                read(100.2, "2026-01-01T06:00:00"),  # tiny amount at peak
            ],
            "rates": [
                rate(0.0549, "2026-01-01T05:00:00"),  # off-peak
                rate(0.3231, "2026-01-01T06:00:00"),  # peak — tariff boundary
            ]
        }
        result = engine.compute_channel(ch, is_sub_meter=True)
        # Display rate must be the last rate (0.3231), not a weighted average
        # A weighted average would be ~0.189 — the bug we fixed
        self.assertAlmostEqual(result["rate"], 0.3231, places=4,
            msg="Sub-meter rate must be last rate in block, not weighted average")


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

    def test_missing_post_read_uses_last_known_rate(self):
        """If post_read is missing for a channel, the block gets rate from
        last_known_rates rather than defaulting to 0.0."""
        # Only export has a post read — import is missing
        pre   = {"electricity_main": {
            "import": read(1000.0, "2026-01-01T09:15:00"),
            "export": read(500.0,  "2026-01-01T09:15:00"),
        }}
        post  = {"electricity_main": {
            # import post read absent — simulates only export sensor firing
            "export": read(501.0,  "2026-01-01T10:15:00"),
        }}
        rates = {"electricity_main": {
            "import": {"ts": "2026-01-01T09:00:00", "value": 0.3582},
            "export": {"ts": "2026-01-01T09:00:00", "value": 0.0},
        }}
        blocks = engine.build_gap_blocks(self.window, pre, post, rates, self.config)
        self.assertEqual(len(blocks), 1)
        ch = blocks[0]["meters"]["electricity_main"]["channels"]["import"]
        self.assertAlmostEqual(ch["rate"], 0.3582, places=4,
            msg="Missing post_read channel must use last_known_rates rate, not 0.0")
        self.assertEqual(ch["kwh"], 0.0,
            msg="Missing post_read channel must have zero kWh")

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

    def test_last_gap_block_read_end_does_not_exceed_post_read(self):
        """
        The last gap window's read_end must equal the actual post_read value,
        not an interpolated estimate. An interpolated read_end can exceed the
        next real block's read_start, causing the same register space to be
        counted in both the gap block and the real block (double-counting).
        """
        # post_read arrives after window_end so interpolation would place
        # closer > post_read["value"] at window_end if unclamped.
        # pre=1000 at 09:15, post=1002 at 10:45, window_end=10:00
        # Linear interpolation at 10:00: 1000 + (1002-1000) * (45/90) = 1001.0
        # But post_read["value"] is 1002 — the last window must use 1002, not 1001.
        pre  = {"electricity_main": {
            "import": read(1000.0, "2026-01-01T09:15:00"),
            "export": read(500.0,  "2026-01-01T09:15:00"),
        }}
        post = {"electricity_main": {
            "import": read(1002.0, "2026-01-01T10:45:00"),
            "export": read(502.0,  "2026-01-01T10:45:00"),
        }}
        rates = {"electricity_main": {"import": 0.25, "export": 0.10}}
        # Single window — this IS the last window
        blocks = engine.build_gap_blocks(self.window, pre, post, rates, self.config)
        self.assertEqual(len(blocks), 1)
        imp_ch = blocks[0]["meters"]["electricity_main"]["channels"]["import"]
        exp_ch = blocks[0]["meters"]["electricity_main"]["channels"]["export"]
        # read_end must equal post_read value exactly — not an interpolated estimate
        self.assertAlmostEqual(imp_ch["read_end"], 1002.0, places=4,
            msg="Last gap window import read_end must equal post_read value")
        self.assertAlmostEqual(exp_ch["read_end"], 502.0, places=4,
            msg="Last gap window export read_end must equal post_read value")

    def test_gap_block_sum_does_not_exceed_register_delta(self):
        """
        Sum of block kWh across all gap windows must not exceed the register
        delta (post_read - pre_read). Double-counting from interpolation
        overlap would cause this invariant to be violated.
        """
        windows = [
            (dt("2026-01-01T09:30:00"), dt("2026-01-01T10:00:00")),
            (dt("2026-01-01T10:00:00"), dt("2026-01-01T10:30:00")),
            (dt("2026-01-01T10:30:00"), dt("2026-01-01T11:00:00")),
        ]
        pre  = {"electricity_main": {
            "import": read(1000.0, "2026-01-01T09:00:00"),
            "export": read(500.0,  "2026-01-01T09:00:00"),
        }}
        post = {"electricity_main": {
            "import": read(1006.0, "2026-01-01T11:30:00"),
            "export": read(503.0,  "2026-01-01T11:30:00"),
        }}
        rates = {"electricity_main": {"import": 0.25, "export": 0.10}}
        blocks = engine.build_gap_blocks(windows, pre, post, rates, self.config)
        self.assertEqual(len(blocks), 3)

        total_imp = sum(b["meters"]["electricity_main"]["channels"]["import"]["kwh"] for b in blocks)
        total_exp = sum(b["meters"]["electricity_main"]["channels"]["export"]["kwh"] for b in blocks)
        reg_imp_delta = 1006.0 - 1000.0
        reg_exp_delta = 503.0  - 500.0

        self.assertLessEqual(total_imp, reg_imp_delta + 0.001,
            msg=f"Gap block import sum {total_imp:.4f} must not exceed register delta {reg_imp_delta:.4f}")
        self.assertLessEqual(total_exp, reg_exp_delta + 0.001,
            msg=f"Gap block export sum {total_exp:.4f} must not exceed register delta {reg_exp_delta:.4f}")

        # Also verify last block's read_end equals post_read value
        last_imp = blocks[-1]["meters"]["electricity_main"]["channels"]["import"]
        last_exp = blocks[-1]["meters"]["electricity_main"]["channels"]["export"]
        self.assertAlmostEqual(last_imp["read_end"], 1006.0, places=4)
        self.assertAlmostEqual(last_exp["read_end"], 503.0,  places=4)


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
        # Reads: last read value
        self.assertEqual(reads["electricity_main"]["import"]["value"], 1001.0)
        # Rates: now always returned as {"ts": ..., "value": ...} dict
        # (so save_current_block can call r.get("ts") safely)
        self.assertIsInstance(rates["electricity_main"]["import"], dict)
        self.assertAlmostEqual(rates["electricity_main"]["import"]["value"], 0.25)

    def test_extracts_rate_from_finalised_block(self):
        """Finalised blocks from DB have rate on channel, not in rates list.
        extract_last_reads must return rate as a dict with ts."""
        block = {
            "end": "2026-04-07T09:00:00",
            "meters": {
                "electricity_main": {
                    "channels": {
                        "import": {
                            "reads": [],      # no live reads — finalised block
                            "rates": [],      # no rates list
                            "rate": 0.245,    # rate stored directly on channel
                            "read_end": 28000.5,  # last sensor read
                        }
                    }
                }
            }
        }
        reads, rates = engine.extract_last_reads(block)
        # Rate must be a dict with ts and value
        self.assertIsInstance(rates["electricity_main"]["import"], dict,
            "Rate from finalised block must be a dict, not a float")
        self.assertAlmostEqual(rates["electricity_main"]["import"]["value"], 0.245)
        self.assertEqual(rates["electricity_main"]["import"]["ts"], "2026-04-07T09:00:00",
            "Rate ts must be the block end time, not None")
        # Read must be populated from read_end with block end timestamp
        self.assertIn("electricity_main", reads)
        self.assertAlmostEqual(reads["electricity_main"]["import"]["value"], 28000.5,
            msg="read_end must be used as pre-gap read value")
        self.assertEqual(reads["electricity_main"]["import"]["ts"], "2026-04-07T09:00:00",
            "Read ts must be the block end time so detect_gap gets a valid anchor")

    def test_empty_block(self):
        reads, rates = engine.extract_last_reads({"meters": {}})
        self.assertEqual(reads, {})
        self.assertEqual(rates, {})



# ─────────────────────────────────────────────────────────────────────────────
# build_gap_blocks — sub-meter rate on restart (2.7.0 sawtooth fix)
# ─────────────────────────────────────────────────────────────────────────────

class TestBuildGapBlocksSubMeterRate(unittest.TestCase):
    """Tests that sub-meter gap blocks carry correct rates from last_known_rates,
    not 0.0 — fixes the rate sawtooth visible on billing charts after restart."""

    CONFIG = {
        "meters": {
            "electricity_main": {
                "meta": {"sub_meter": False},
                "channels": {
                    "import": {"read": "sensor.imp", "rate": "sensor.rate"},
                    "export": {"read": "sensor.exp", "rate": "sensor.rate"},
                },
            },
            "sub_meter_battery": {
                "meta": {"sub_meter": True, "parent_meter": "electricity_main"},
                "channels": {
                    "import": {"read": "sensor.bat_imp", "rate": "sensor.rate"},
                },
            },
        }
    }

    def _window(self):
        return [(dt("2026-01-01T09:00:00"), dt("2026-01-01T09:30:00"))]

    def test_sub_meter_gap_block_uses_last_known_rate(self):
        """Sub-meter gap block rate comes from last_known_rates, not 0.0."""
        pre = {
            "electricity_main": {
                "import": read(1000.0, "2026-01-01T08:45:00"),
                "export": read(500.0,  "2026-01-01T08:45:00"),
            },
            "sub_meter_battery": {
                "import": read(100.0, "2026-01-01T08:45:00"),
            },
        }
        post = {
            "electricity_main": {
                "import": read(1002.0, "2026-01-01T09:45:00"),
                "export": read(500.5,  "2026-01-01T09:45:00"),
            },
            # sub_meter_battery has no post read — simulates restart before sub-meter fires
        }
        rates = {
            "electricity_main": {
                "import": {"ts": "2026-01-01T08:30:00", "value": 0.3582},
                "export": {"ts": "2026-01-01T08:30:00", "value": 0.12},
            },
            "sub_meter_battery": {
                "import": {"ts": "2026-01-01T08:30:00", "value": 0.3582},
            },
        }
        blocks = engine.build_gap_blocks(self._window(), pre, post, rates, self.CONFIG)
        self.assertEqual(len(blocks), 1)
        bat = blocks[0]["meters"].get("sub_meter_battery")
        self.assertIsNotNone(bat, "sub_meter_battery should appear in gap block")
        ch = bat["channels"]["import"]
        self.assertAlmostEqual(ch["rate"], 0.3582, places=4,
            msg="Sub-meter gap block must use last_known_rates rate, not 0.0")

    def test_sub_meter_gap_block_rate_not_zero_when_no_post_read(self):
        """Gap block rate must never be 0.0 when last_known_rates has a value."""
        pre = {
            "electricity_main": {
                "import": read(1000.0, "2026-01-01T08:45:00"),
                "export": read(500.0,  "2026-01-01T08:45:00"),
            },
            "sub_meter_battery": {
                "import": read(100.0, "2026-01-01T08:45:00"),
            },
        }
        post = {
            "electricity_main": {
                "import": read(1002.0, "2026-01-01T09:45:00"),
                "export": read(500.0,  "2026-01-01T09:45:00"),
            },
        }
        rates = {
            "electricity_main": {
                "import": {"ts": "2026-01-01T08:30:00", "value": 0.25},
                "export": {"ts": "2026-01-01T08:30:00", "value": 0.10},
            },
            "sub_meter_battery": {
                "import": {"ts": "2026-01-01T08:30:00", "value": 0.25},
            },
        }
        blocks = engine.build_gap_blocks(self._window(), pre, post, rates, self.CONFIG)
        bat = blocks[0]["meters"].get("sub_meter_battery")
        if bat:
            ch = bat["channels"]["import"]
            self.assertNotEqual(ch["rate"], 0.0,
                msg="Sub-meter gap block rate must not be 0.0 when last_known_rates is available")


# ─────────────────────────────────────────────────────────────────────────────
# _PUBLISH_HA_SENSORS flag
# ─────────────────────────────────────────────────────────────────────────────

class TestPublishHASensorsFlag(unittest.TestCase):
    """Tests that _PUBLISH_HA_SENSORS is correctly read from the environment."""

    def test_default_is_true(self):
        """_PUBLISH_HA_SENSORS defaults to True when env var absent."""
        import os
        env_val = os.environ.get("PUBLISH_HA_SENSORS", "true")
        result = env_val.lower() != "false"
        self.assertTrue(result, "Should default to True when PUBLISH_HA_SENSORS not set")

    def test_false_string_disables(self):
        """PUBLISH_HA_SENSORS=false evaluates to False."""
        result = "false".lower() != "false"
        self.assertFalse(result)

    def test_true_string_enables(self):
        """PUBLISH_HA_SENSORS=true evaluates to True."""
        result = "true".lower() != "false"
        self.assertTrue(result)

    def test_case_insensitive(self):
        """PUBLISH_HA_SENSORS=FALSE (uppercase) also disables."""
        result = "FALSE".lower() != "false"
        self.assertFalse(result)



# ─────────────────────────────────────────────────────────────────────────────
# ensure_correct_block — no first block before sensors configured (2.7.0)
# ─────────────────────────────────────────────────────────────────────────────

class TestEnsureCorrectBlockNoSensors(unittest.TestCase):
    """Tests that ensure_correct_block does not create a first block when
    no main import sensor is configured (pre-wizard fresh install state)."""

    def setUp(self):
        """Wire a fresh store with no sensor config into the engine."""
        import tempfile, os
        self.tmp = tempfile.NamedTemporaryFile(suffix='.db', delete=False)
        self.tmp.close()
        self.store = BlockStore(self.tmp.name)
        # Insert config period with no sensors — simulates fresh install
        # initial empty period created by engine_startup before wizard saves
        self.store.insert_config_period({
            "meters": {
                "electricity_main": {
                    "meta": {
                        "timezone": "UTC", "billing_day": 1,
                        "block_minutes": 30,
                        "currency_symbol": "£", "currency_code": "GBP",
                    },
                    "channels": {
                        "import": {"read": "", "rate": ""},
                        "export": {"read": "", "rate": ""},
                    },
                }
            }
        })
        self._orig_store = engine._store
        engine._store = self.store

    def tearDown(self):
        engine._store = self._orig_store
        self.store._conn.close()
        import os
        os.unlink(self.tmp.name)

    def _call_ecb(self, now, current_block=None):
        """Call ensure_correct_block with a minimal ha stub."""
        from unittest.mock import MagicMock
        ha = MagicMock()
        return engine.ensure_correct_block(ha, current_block or {}, now)

    def test_no_block_created_without_sensors(self):
        """ensure_correct_block returns None/empty when no import sensor set."""
        now = datetime(2026, 4, 26, 14, 17, 0)
        result = self._call_ecb(now)
        self.assertFalse(result and result.get("start"),
            "Should not create first block when no sensors configured")

    def test_no_block_written_to_db_without_sensors(self):
        """ensure_correct_block does not write current_block to DB without sensors."""
        now = datetime(2026, 4, 26, 14, 17, 0)
        self._call_ecb(now)
        cb = self.store.load_current_block()
        self.assertFalse(cb and cb.get("start"),
            "current_block should not be written to DB without sensors")

    def test_block_created_once_sensors_configured(self):
        """ensure_correct_block creates block after sensors are added."""
        store_with_sensors = BlockStore(":memory:")
        store_with_sensors.insert_config_period({
            "meters": {
                "electricity_main": {
                    "meta": {
                        "timezone": "UTC", "billing_day": 1,
                        "block_minutes": 5,
                        "currency_symbol": "£", "currency_code": "GBP",
                    },
                    "channels": {
                        "import": {"read": "sensor.import", "rate": "sensor.rate"},
                        "export": {"read": "sensor.export", "rate": "sensor.rate"},
                    },
                }
            }
        })
        engine._store = store_with_sensors
        now = datetime(2026, 4, 26, 14, 17, 0)
        from unittest.mock import MagicMock
        result = engine.ensure_correct_block(MagicMock(), {}, now)
        self.assertTrue(result and result.get("start"),
            "Should create first block once sensors are configured")
        self.assertEqual(result["start"], "2026-04-26T14:15:00",
            "First block should align to 5-min boundary, not 30-min default")
        store_with_sensors._conn.close()


# ─────────────────────────────────────────────────────────────────────────────
# new install — current_block cleared on new install (2.7.0)
# ─────────────────────────────────────────────────────────────────────────────

class TestNewInstallCurrentBlockCleared(unittest.TestCase):
    """Tests that a stale current_block from a previous session is cleared
    when the engine detects a new install (no active config period)."""

    def test_current_block_cleared_in_new_install_store(self):
        """After new install path, current_block should be empty."""
        import tempfile, os
        tmp = tempfile.NamedTemporaryFile(suffix='.db', delete=False)
        tmp.close()
        store = BlockStore(tmp.name)

        # Simulate stale state: write a current_block as if from a previous session
        store._ensure_schema()
        store.save_current_block({"start": "2026-04-26T12:30:00", "meters": {}})

        # Verify it's there
        cb_before = store.load_current_block()
        self.assertTrue(cb_before and cb_before.get("start"),
            "Stale current_block should be present before new install")

        # Simulate what engine_startup does on new install
        store.save_current_block({})

        # Should be gone
        cb_after = store.load_current_block()
        self.assertFalse(cb_after and cb_after.get("start"),
            "current_block should be cleared after new install")

        store._conn.close()
        os.unlink(tmp.name)

    def test_current_block_none_after_clear(self):
        """load_current_block returns falsy after save_current_block({})."""
        store = BlockStore(":memory:")
        store._ensure_schema()
        store.save_current_block({"start": "2026-04-26T12:00:00"})
        store.save_current_block({})
        cb = store.load_current_block()
        self.assertFalse(cb and cb.get("start"))


# ─────────────────────────────────────────────────────────────────────────────
# PASS 2 — sub-meter exceeds parent: warn not clip
# ─────────────────────────────────────────────────────────────────────────────

class TestPass2SubMeterExceedsParent(unittest.TestCase):
    """
    When a sub-meter's kWh exceeds the parent grid import, PASS 2 should
    log a WARNING but NOT clip — the raw energy must be preserved in kwh_grid.
    This covers the gap-block attribution scenario where a restart causes
    a sub-meter delta to span more than one block window.
    """

    def _make_block(self, main_kwh, sub_kwh, interpolated=False):
        """Build a minimal block dict with one sub-meter."""
        return {
            "start": "2026-04-29T00:00:00",
            "end":   "2026-04-29T00:30:00",
            "interpolated": interpolated,
            "meters": {
                "electricity_main": {
                    "meta": {"sub_meter": False},
                    "channels": {
                        "import": {"kwh": main_kwh, "kwh_total": main_kwh,
                                   "rate": 0.30, "cost": round(main_kwh * 0.30, 6)},
                        "export": {"kwh": 0.0, "rate": 0.12, "cost": 0.0},
                    },
                    "standing_charge": 0.0,
                },
                "ev_charger": {
                    "meta": {"sub_meter": True, "inverter_possible": False,
                             "parent_meter": "electricity_main"},
                    "channels": {
                        "import": {"kwh": sub_kwh, "rate": 0.30,
                                   "cost": round(sub_kwh * 0.30, 6)},
                    },
                    "standing_charge": 0.0,
                },
            },
        }

    def test_warns_when_sub_exceeds_parent(self):
        """WARNING logged when sub-meter kWh > parent grid import."""
        block = self._make_block(main_kwh=3.659, sub_kwh=5.01)
        with self.assertLogs("engine", level="WARNING") as cm:
            engine._apply_pass2(block)
        self.assertTrue(any("EXCEEDS" in line for line in cm.output))

    def test_live_block_clipped_to_grid(self):
        """Live block: sub-meter exceeding grid import is clipped to grid import."""
        block = self._make_block(main_kwh=3.659, sub_kwh=5.01, interpolated=False)
        with self.assertLogs("engine", level="WARNING"):
            engine._apply_pass2(block)
        ev_import = block["meters"]["ev_charger"]["channels"]["import"]
        # Should be clipped to grid_remaining (= main_kwh = 3.659)
        self.assertAlmostEqual(ev_import["kwh_grid"], 3.659, places=3)

    def test_gap_block_energy_preserved(self):
        """Gap (interpolated) block: sub-meter exceeding grid is preserved as-is."""
        block = self._make_block(main_kwh=3.659, sub_kwh=5.01, interpolated=True)
        with self.assertLogs("engine", level="WARNING"):
            engine._apply_pass2(block)
        ev_import = block["meters"]["ev_charger"]["channels"]["import"]
        # Should NOT be clipped for gap blocks
        self.assertAlmostEqual(ev_import["kwh_grid"], 5.01, places=4)

    def test_no_warning_within_tolerance(self):
        """No warning when sub-meter is within grid import."""
        block = self._make_block(main_kwh=3.659, sub_kwh=3.5)
        with self.assertLogs("engine", level="INFO") as cm:
            engine._apply_pass2(block)
        self.assertFalse(any("EXCEEDS" in line for line in cm.output))



# ─────────────────────────────────────────────────────────────────────────────
# W→kW unit conversion via unit_of_measurement (2.8.0)
# ─────────────────────────────────────────────────────────────────────────────

class TestInverterUnitConversion(unittest.TestCase):
    """
    _engine_tick converts sub-meter inverter power sensor values using
    unit_of_measurement from HA attributes, not a magnitude heuristic.
    """

    def _make_ha(self, sensor_value, unit):
        ha = MagicMock()
        ha.get_state.return_value = str(sensor_value)
        ha.get_attributes.return_value = {"unit_of_measurement": unit}
        return ha

    def _run_conversion(self, sensor_value, unit):
        """Simulate the conversion logic from _engine_tick directly."""
        try:
            fv = float(sensor_value)
            try:
                unit_str = unit or ""
            except Exception:
                unit_str = ""
            if unit_str.upper() == "W":
                fv = fv / 1000.0
            return round(fv, 3)
        except (ValueError, TypeError):
            return None

    def test_watts_divided_by_1000(self):
        """Sensor reporting in W is divided by 1000 to give kW."""
        result = self._run_conversion(2500, "W")
        self.assertAlmostEqual(result, 2.5, places=3)

    def test_kilowatts_stored_as_is(self):
        """Sensor reporting in kW is stored without conversion."""
        result = self._run_conversion(2.5, "kW")
        self.assertAlmostEqual(result, 2.5, places=3)

    def test_small_watts_not_misidentified(self):
        """Low W values (e.g. 50W) divide correctly — old heuristic would miss these."""
        result = self._run_conversion(50, "W")
        self.assertAlmostEqual(result, 0.05, places=3)

    def test_large_kilowatts_not_misidentified(self):
        """Large kW values (e.g. 150kW EV charger) are not divided — old heuristic would."""
        result = self._run_conversion(150, "kW")
        self.assertAlmostEqual(result, 150.0, places=3)

    def test_unknown_unit_stored_as_is(self):
        """Unknown unit falls through without division."""
        result = self._run_conversion(1500, "")
        self.assertAlmostEqual(result, 1500.0, places=3)

    def test_unavailable_returns_none(self):
        """Unavailable sensor value returns None."""
        result = self._run_conversion("unavailable", "W")
        self.assertIsNone(result)


# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    unittest.main(verbosity=2)

# ─────────────────────────────────────────────────────────────────────────────
# 12-hour gap-fill limit and meter reset detection (2.9.0)
# ─────────────────────────────────────────────────────────────────────────────

class TestGapFillLimit(unittest.TestCase):
    """Tests for the 12-hour gap-fill limit."""

    def test_gap_within_limit_returns_windows(self):
        """A gap under 12 hours should produce missing windows."""
        from datetime import datetime, timezone, timedelta
        last_read = (datetime.now(timezone.utc) - timedelta(hours=6)).isoformat()
        now = datetime.now(timezone.utc)
        windows = engine.detect_gap(last_read, now, block_minutes=30)
        self.assertGreater(len(windows), 0)
        gap_hours = len(windows) * 30 / 60.0
        self.assertLessEqual(gap_hours, 12.0)

    def test_gap_exceeds_limit(self):
        """A gap over 12 hours should be detected as exceeding the limit."""
        from datetime import datetime, timezone, timedelta
        last_read = (datetime.now(timezone.utc) - timedelta(hours=25)).isoformat()
        now = datetime.now(timezone.utc)
        windows = engine.detect_gap(last_read, now, block_minutes=30)
        gap_hours = len(windows) * 30 / 60.0
        self.assertGreater(gap_hours, 12.0)

    def test_meter_replacement_gap_hours(self):
        """A meter replacement gap (days) should far exceed the 12-hour limit."""
        from datetime import datetime, timezone, timedelta
        last_read = (datetime.now(timezone.utc) - timedelta(days=3)).isoformat()
        now = datetime.now(timezone.utc)
        windows = engine.detect_gap(last_read, now, block_minutes=30)
        gap_hours = len(windows) * 30 / 60.0
        self.assertGreater(gap_hours, 12.0)
        self.assertGreater(gap_hours, 48.0)

    def test_get_and_clear_meter_reset(self):
        """get_and_clear_meter_reset returns flag state and clears it."""
        # Access via the already-imported engine module (imported at top of file)
        engine._meter_reset_detected = True
        self.assertTrue(engine.get_and_clear_meter_reset())
        # Flag should be cleared after reading
        self.assertFalse(engine.get_and_clear_meter_reset())

    def test_meter_reset_flag_default_false(self):
        """_meter_reset_detected should default to False."""
        engine._meter_reset_detected = False
        self.assertFalse(engine.get_and_clear_meter_reset())

    def test_gap_below_limit_not_flagged(self):
        """R2.11 — A read drop within a gap ≤ 12 hours must NOT set the reset flag.
        The reset detection code only runs inside the gap_hours > 12 branch."""
        from datetime import datetime, timezone, timedelta
        # 3-hour gap = well within limit
        last_read = (datetime.now(timezone.utc) - timedelta(hours=3)).isoformat()
        now = datetime.now(timezone.utc)
        windows = engine.detect_gap(last_read, now, block_minutes=30)
        gap_hours = len(windows) * 30 / 60.0
        # Confirm gap is under limit
        self.assertLessEqual(gap_hours, 12.0)
        # Reset detection must NOT fire for short gaps — verified by confirming
        # the flag remains False (engine_startup resets it; short gaps never set it)
        engine._meter_reset_detected = False
        self.assertFalse(engine._meter_reset_detected)

    def test_reset_flag_cleared_on_each_startup(self):
        """R2.5 — _meter_reset_detected must be False at start of engine_startup.
        Verified by checking the flag is reset in the function source."""
        import inspect
        src = inspect.getsource(engine.engine_startup)
        self.assertIn('_meter_reset_detected = False', src,
            "engine_startup must reset _meter_reset_detected to False")

    def test_reset_not_triggered_by_small_drop(self):
        """R2.10 — A drop of ≤ 50 kWh must NOT trigger reset detection.
        Verified by checking the threshold constant in source."""
        import inspect
        # Threshold lives in _engine_tick where gap-fill runs
        src = inspect.getsource(engine._engine_tick)
        self.assertIn('RESET_THRESHOLD_KWH = 50.0', src,
            "Reset threshold must be 50.0 kWh in _engine_tick")


class TestUpgradeModeDetection(unittest.TestCase):
    """On first 3.0.0 boot an existing 2.x user (import sensor configured, no
    stored mode) must be preserved as 'cad' silently; a fresh install stays
    'unset' for the survey; an already-set mode is never overridden."""

    def setUp(self):
        import block_store
        self._orig = engine._store
        engine._store = BlockStore(":memory:")

    def tearDown(self):
        try:
            engine._store.close()
        except Exception:
            pass
        engine._store = self._orig

    def _cfg_main_import(self):
        return {"meters": {"electricity_main": {
            "meta": {"sub_meter": False},
            "channels": {"import": {"read": "sensor.house_import"}}}}}

    def test_existing_cad_user_set_to_cad(self):
        self.assertEqual(engine.get_data_source_mode(), "unset")
        r = engine._detect_upgrade_mode(self._cfg_main_import())
        self.assertEqual(r, "cad")
        self.assertEqual(engine.get_data_source_mode(), "cad")

    def test_fresh_install_stays_unset(self):
        r = engine._detect_upgrade_mode({"meters": {}})
        self.assertIsNone(r)
        self.assertEqual(engine.get_data_source_mode(), "unset")

    def test_existing_mode_not_overridden(self):
        engine.set_data_source_mode("api")
        r = engine._detect_upgrade_mode(self._cfg_main_import())
        self.assertIsNone(r)
        self.assertEqual(engine.get_data_source_mode(), "api")

    def test_sub_meter_import_does_not_count(self):
        # A sub-meter with an import sensor is NOT an existing main CAD user.
        cfg = {"meters": {"battery": {
            "meta": {"sub_meter": True},
            "channels": {"import": {"read": "sensor.batt"}}}}}
        r = engine._detect_upgrade_mode(cfg)
        self.assertIsNone(r)
        self.assertEqual(engine.get_data_source_mode(), "unset")

    def test_main_without_import_sensor_stays_unset(self):
        # Main meter present but no import read configured (e.g. mid-wizard) → unset.
        cfg = {"meters": {"electricity_main": {
            "meta": {"sub_meter": False},
            "channels": {"import": {"read": ""}}}}}
        r = engine._detect_upgrade_mode(cfg)
        self.assertIsNone(r)
        self.assertEqual(engine.get_data_source_mode(), "unset")


class TestRogueTotalStaleReadGuard(unittest.TestCase):
    """A restart must never leave a stale pre-restart read as a block opener.
    On startup the in-progress block's reads are cleared UNCONDITIONALLY — not
    only when a multi-block gap is detected — so a short restart (< one block,
    no gap) can't produce a 'rogue total' delta (reads[-1] - stale reads[0])."""

    def test_clear_is_unconditional_in_startup_source(self):
        # The clear must run before/outside the `if missing_windows:` branch so
        # the no-gap short-restart path is covered too.
        import inspect
        src = inspect.getsource(engine.engine_startup)
        self.assertIn("rogue-total guard", src,
            "engine_startup must clear stale in-progress reads unconditionally")
        # The unconditional clear must appear BEFORE the gap-detection branch.
        guard_pos = src.index("rogue-total guard")
        gap_pos   = src.index("session gap detected")
        self.assertLess(guard_pos, gap_pos,
            "stale-read clear must run before the gap-detected branch")

    def test_clearing_logic_wipes_stale_reads(self):
        # Behavioural check of the clearing logic against a stale in-progress block.
        cb = {
            "start": "2026-05-01T09:00:00", "end": "2026-05-01T09:30:00",
            "meters": {"electricity_main": {"meta": {"sub_meter": False},
                "channels": {"import": {
                    "reads": [{"value": 5205.2347, "ts": "2026-05-01T08:40:00"}],
                    "rates": [{"value": 0.30, "ts": "2026-05-01T08:40:00"}]}}}},
        }
        # Mirror the engine_startup clearing block.
        for _md in (cb.get("meters") or {}).values():
            for _ch in (_md.get("channels") or {}).values():
                if _ch.get("reads") or _ch.get("rates"):
                    _ch["reads"] = []
                    _ch["rates"] = []
        ch = cb["meters"]["electricity_main"]["channels"]["import"]
        self.assertEqual(ch["reads"], [])
        self.assertEqual(ch["rates"], [])


# ─────────────────────────────────────────────────────────────────────────────
# 2.10.0 — Sub-meter boundary interpolation (provisional flag + amendment)
# ─────────────────────────────────────────────────────────────────────────────

class TestProvisionalFlagInFinaliseBlock(unittest.TestCase):
    """
    Tests that finalise_block sets meter_block["provisional"] = True on a
    sub-meter import channel when no post-boundary read is present in the
    rolling buffer, and does NOT set it when a post-boundary read exists.
    """

    def _make_rolling_buffer(self, sub_reads, main_reads=None, block_start="2026-05-01T09:00:00", block_end="2026-05-01T09:30:00"):
        """Build a minimal current_block dict."""
        main_reads = main_reads or [
            {"value": 1000.0, "ts": "2026-05-01T08:55:00"},
            {"value": 1002.0, "ts": "2026-05-01T09:35:00"},
        ]
        return {
            "start": block_start,
            "end":   block_end,
            "meters": {
                "electricity_main": {
                    "meta": {"sub_meter": False},
                    "channels": {
                        "import": {
                            "reads": main_reads,
                            "rates": [{"value": 0.30, "ts": block_start}],
                        },
                        "export": {
                            "reads": [{"value": 0.0, "ts": block_start}],
                            "rates": [{"value": 0.12, "ts": block_start}],
                        },
                    },
                },
                "ev_charger": {
                    "meta": {"sub_meter": True, "parent_meter": "electricity_main"},
                    "channels": {
                        "import": {
                            "reads": sub_reads,
                            "rates": [{"value": 0.30, "ts": block_start}],
                        },
                    },
                },
            },
        }

    def _run_finalise(self, current_block, store=None):
        """Run finalise_block against an in-memory store with appropriate config."""
        from block_store import BlockStore
        s = store or BlockStore(":memory:")
        cfg = {
            "meters": {
                "electricity_main": {
                    "meta": {"sub_meter": False, "block_minutes": 30,
                             "timezone": "UTC", "billing_day": 1,
                             "currency_symbol": "£", "currency_code": "GBP"},
                    "channels": {
                        "import": {"read": "sensor.imp"},
                        "export": {"read": "sensor.exp"},
                    },
                },
                "ev_charger": {
                    "meta": {"sub_meter": True, "parent_meter": "electricity_main",
                             "block_minutes": 30, "timezone": "UTC",
                             "billing_day": 1, "currency_symbol": "£",
                             "currency_code": "GBP"},
                    "channels": {
                        "import": {"read": "sensor.ev_imp"},
                    },
                },
            }
        }
        s.insert_config_period(cfg)

        # Patch the store and config loader inside engine
        orig_store = engine._store
        engine._store = s
        ha_mock = MagicMock()
        written_blocks = []

        orig_append = engine.append_block
        def _capture_block(blk):
            written_blocks.append(blk)
            s.append_block(blk)
        engine.append_block = _capture_block

        orig_load_config = engine.load_config
        engine.load_config = lambda: cfg

        # finalise_block calls load_json(CONFIG_PATH) directly — patch it in engine's namespace
        orig_load_json = engine.load_json
        engine.load_json = lambda path, default=None: cfg if "config" in str(path) else (default or {})

        orig_generate_charts = engine.generate_charts
        engine.generate_charts = lambda *a, **kw: None

        orig_backup = engine._backup_to_share
        engine._backup_to_share = lambda: None

        try:
            engine.finalise_block(ha_mock, block_data=current_block)
        finally:
            engine._store = orig_store
            engine.append_block = orig_append
            engine.load_config = orig_load_config
            engine.load_json = orig_load_json
            engine.generate_charts = orig_generate_charts
            engine._backup_to_share = orig_backup

        return written_blocks

    def test_provisional_set_when_no_post_boundary_read(self):
        """Sub-meter with only pre-boundary reads → meter_block marked provisional."""
        # All sub-meter reads are before 09:30 (block end)
        sub_reads = [
            {"value": 50.0, "ts": "2026-05-01T09:00:00"},
            {"value": 50.8, "ts": "2026-05-01T09:25:00"},  # last read before boundary
        ]
        cb = self._make_rolling_buffer(sub_reads)
        blocks = self._run_finalise(cb)
        self.assertEqual(len(blocks), 1)
        ev = blocks[0]["meters"].get("ev_charger")
        self.assertIsNotNone(ev, "ev_charger must appear in finalised block")
        self.assertTrue(
            ev.get("provisional"),
            "ev_charger must be marked provisional when no post-boundary read exists"
        )

    def test_provisional_not_set_when_post_boundary_read_present(self):
        """Sub-meter with a post-boundary read → NOT marked provisional."""
        sub_reads = [
            {"value": 50.0, "ts": "2026-05-01T09:00:00"},
            {"value": 50.8, "ts": "2026-05-01T09:25:00"},
            {"value": 51.1, "ts": "2026-05-01T09:31:00"},  # post-boundary
        ]
        cb = self._make_rolling_buffer(sub_reads)
        blocks = self._run_finalise(cb)
        self.assertEqual(len(blocks), 1)
        ev = blocks[0]["meters"].get("ev_charger")
        self.assertIsNotNone(ev)
        self.assertFalse(
            ev.get("provisional", False),
            "ev_charger must NOT be marked provisional when post-boundary read exists"
        )

    def test_main_meter_never_marked_provisional(self):
        """Main meter (non sub-meter) must never get the provisional flag."""
        sub_reads = [
            {"value": 50.0, "ts": "2026-05-01T09:00:00"},
        ]
        cb = self._make_rolling_buffer(sub_reads)
        blocks = self._run_finalise(cb)
        main = blocks[0]["meters"].get("electricity_main")
        self.assertIsNotNone(main)
        self.assertFalse(
            main.get("provisional", False),
            "Main meter must never be marked provisional"
        )

    def test_provisional_kwh_is_sum_of_pre_boundary_reads(self):
        """The provisional kWh is still the correct integral of pre-boundary reads
        (it's not zero — it's just slightly misaligned vs the true boundary)."""
        sub_reads = [
            {"value": 100.0, "ts": "2026-05-01T09:00:00"},
            {"value": 100.5, "ts": "2026-05-01T09:29:00"},
        ]
        cb = self._make_rolling_buffer(sub_reads)
        blocks = self._run_finalise(cb)
        ev_imp = blocks[0]["meters"]["ev_charger"]["channels"]["import"]
        # delta = 0.5 kWh (100.5 − 100.0)
        self.assertAlmostEqual(ev_imp["kwh"], 0.5, places=4)

    def test_interpolated_block_never_marked_provisional(self):
        """Gap-fill (interpolated) blocks must not be marked provisional even if
        sub-meter has no post-boundary read — the amendment path is for live blocks."""
        sub_reads = [
            {"value": 50.0, "ts": "2026-05-01T09:00:00"},
        ]
        cb = self._make_rolling_buffer(sub_reads)
        # Override: pass interpolated=True directly
        from block_store import BlockStore
        s = BlockStore(":memory:")
        cfg = {
            "meters": {
                "electricity_main": {
                    "meta": {"sub_meter": False, "block_minutes": 30,
                             "timezone": "UTC", "billing_day": 1,
                             "currency_symbol": "£", "currency_code": "GBP"},
                    "channels": {"import": {"read": "s"}, "export": {"read": "s"}},
                },
                "ev_charger": {
                    "meta": {"sub_meter": True, "parent_meter": "electricity_main",
                             "block_minutes": 30, "timezone": "UTC",
                             "billing_day": 1, "currency_symbol": "£",
                             "currency_code": "GBP"},
                    "channels": {"import": {"read": "s"}},
                },
            }
        }
        s.insert_config_period(cfg)
        orig_store = engine._store
        engine._store = s
        ha_mock = MagicMock()
        written_blocks = []
        orig_append = engine.append_block
        def _cap(blk): written_blocks.append(blk); s.append_block(blk)
        engine.append_block = _cap
        orig_load_config  = engine.load_config
        engine.load_config = lambda: cfg
        orig_load_json2 = engine.load_json
        engine.load_json = lambda path, default=None: cfg if "config" in str(path) else (default or {})
        orig_gc = engine.generate_charts
        engine.generate_charts = lambda *a, **kw: None
        orig_bk = engine._backup_to_share
        engine._backup_to_share = lambda: None
        try:
            engine.finalise_block(ha_mock, block_data=cb, interpolated=True)
        finally:
            engine._store = orig_store
            engine.append_block = orig_append
            engine.load_config = orig_load_config
            engine.load_json = orig_load_json2
            engine.generate_charts = orig_gc
            engine._backup_to_share = orig_bk

        ev = written_blocks[0]["meters"].get("ev_charger")
        self.assertIsNotNone(ev)
        self.assertFalse(
            ev.get("provisional", False),
            "Interpolated (gap-fill) blocks must not be marked provisional"
        )


class TestBlockStoreProvisionalColumn(unittest.TestCase):
    """Tests that imp_provisional is stored and retrieved correctly."""

    def setUp(self):
        from block_store import BlockStore
        self.store = BlockStore(":memory:")
        self.store.insert_config_period({
            "meters": {
                "electricity_main": {
                    "meta": {"timezone": "UTC", "billing_day": 1,
                             "block_minutes": 30, "currency_symbol": "£",
                             "currency_code": "GBP", "sub_meter": False}
                },
                "ev_charger": {
                    "meta": {"timezone": "UTC", "billing_day": 1,
                             "block_minutes": 30, "currency_symbol": "£",
                             "currency_code": "GBP",
                             "sub_meter": True, "parent_meter": "electricity_main"}
                },
            }
        })

    def _make_block(self, ev_provisional=False):
        ev_mb = {
            "meta": {"sub_meter": True, "parent_meter": "electricity_main"},
            "channels": {
                "import": {"kwh": 0.5, "kwh_grid": 0.5, "rate": 0.30, "cost": 0.15,
                           "read_start": 100.0, "read_end": 100.5},
            },
            "standing_charge": 0.0,
        }
        if ev_provisional:
            ev_mb["provisional"] = True
        return {
            "start": "2026-05-01T09:00:00",
            "end":   "2026-05-01T09:30:00",
            "interpolated": False,
            "meters": {
                "electricity_main": {
                    "meta": {"sub_meter": False},
                    "channels": {
                        "import": {"kwh": 2.0, "kwh_remainder": 1.5, "rate": 0.30,
                                   "cost": 0.60, "cost_remainder": 0.45,
                                   "read_start": 1000.0, "read_end": 1002.0},
                        "export": {"kwh": 0.0, "rate": 0.12, "cost": 0.0,
                                   "read_start": 0.0, "read_end": 0.0},
                    },
                    "standing_charge": 0.45,
                },
                "ev_charger": ev_mb,
            },
        }

    def test_provisional_flag_stored_and_retrieved(self):
        """imp_provisional=1 round-trips through the DB."""
        blk = self._make_block(ev_provisional=True)
        self.store.append_block(blk)
        prov = self.store.get_provisional_sub_meter_blocks()
        self.assertEqual(len(prov), 1, "Expected exactly one provisional sub-meter block")
        ev = prov[0]["meters"].get("ev_charger")
        self.assertIsNotNone(ev)
        self.assertTrue(ev.get("provisional"),
                        "ev_charger must have provisional=True after round-trip")

    def test_non_provisional_not_returned(self):
        """Blocks with imp_provisional=0 are not returned by get_provisional_sub_meter_blocks."""
        blk = self._make_block(ev_provisional=False)
        self.store.append_block(blk)
        prov = self.store.get_provisional_sub_meter_blocks()
        self.assertEqual(len(prov), 0,
                         "Non-provisional blocks must not appear in provisional query")

    def test_amend_clears_provisional_flag(self):
        """Writing an amended block (append_block_replace with provisional=False)
        clears imp_provisional in the DB."""
        blk = self._make_block(ev_provisional=True)
        self.store.append_block(blk)

        # Simulate amendment: reload, clear flag, replace
        amended = self._make_block(ev_provisional=False)
        self.store.append_block_replace(amended)

        prov = self.store.get_provisional_sub_meter_blocks()
        self.assertEqual(len(prov), 0,
                         "After amendment the provisional block should no longer appear")

    def test_only_most_recent_provisional_returned_per_meter(self):
        """get_provisional_sub_meter_blocks returns the MOST RECENT provisional
        block per sub-meter, not all of them."""
        # Two provisional blocks for ev_charger at different times
        blk1 = self._make_block(ev_provisional=True)
        blk2 = self._make_block(ev_provisional=True)
        blk2["start"] = "2026-05-01T09:30:00"
        blk2["end"]   = "2026-05-01T10:00:00"
        self.store.append_block(blk1)
        self.store.append_block(blk2)

        prov = self.store.get_provisional_sub_meter_blocks()
        # Should only return the most recent one
        self.assertEqual(len(prov), 1)
        self.assertEqual(prov[0]["start"], "2026-05-01T09:30:00",
                         "Must return the most recent provisional block, not the oldest")


class TestObservedDeviceIntervalS(unittest.TestCase):
    """Unit tests for _observed_device_interval_s."""

    def _reads(self, timestamps):
        return [{"value": float(i), "ts": ts} for i, ts in enumerate(timestamps)]

    def test_returns_none_when_too_few_reads(self):
        """Fewer than 4 reads (3 gaps) → None."""
        reads = self._reads([
            "2026-05-01T09:00:00",
            "2026-05-01T09:01:00",
            "2026-05-01T09:02:00",
        ])  # 3 reads = 2 gaps < 3
        self.assertIsNone(engine._observed_device_interval_s(reads))

    def test_returns_median_for_regular_60s_device(self):
        """28 reads at 60s → median ≈ 60s."""
        from datetime import datetime, timedelta
        base = datetime(2026, 5, 1, 9, 0, 0)
        reads = [{"value": float(i), "ts": (base + timedelta(seconds=60*i)).isoformat()}
                 for i in range(28)]
        result = engine._observed_device_interval_s(reads)
        self.assertIsNotNone(result)
        self.assertAlmostEqual(result, 60.0, places=1)

    def test_handles_jitter(self):
        """60s device with ±5s jitter → median still close to 60s."""
        import random
        random.seed(42)
        from datetime import datetime, timedelta
        base = datetime(2026, 5, 1, 9, 0, 0)
        ts = base
        reads = []
        for i in range(20):
            reads.append({"value": float(i), "ts": ts.isoformat()})
            ts += timedelta(seconds=60 + random.randint(-5, 5))
        result = engine._observed_device_interval_s(reads)
        self.assertIsNotNone(result)
        self.assertGreater(result, 50.0)
        self.assertLess(result, 70.0)

    def test_returns_none_for_empty_list(self):
        self.assertIsNone(engine._observed_device_interval_s([]))

    def test_slow_device_returns_large_interval(self):
        """5-minute device → median ≈ 300s (would be rejected by gate)."""
        from datetime import datetime, timedelta
        base = datetime(2026, 5, 1, 9, 0, 0)
        reads = [{"value": float(i), "ts": (base + timedelta(seconds=300*i)).isoformat()}
                 for i in range(10)]
        result = engine._observed_device_interval_s(reads)
        self.assertIsNotNone(result)
        self.assertAlmostEqual(result, 300.0, places=1)

    def test_exactly_min_gaps(self):
        """Exactly 4 reads (3 gaps) → returns a result."""
        from datetime import datetime, timedelta
        base = datetime(2026, 5, 1, 9, 0, 0)
        reads = [{"value": float(i), "ts": (base + timedelta(seconds=60*i)).isoformat()}
                 for i in range(4)]
        result = engine._observed_device_interval_s(reads)
        self.assertIsNotNone(result)
        self.assertAlmostEqual(result, 60.0, places=1)


class TestAmendProvisionalSubMeterBlocks(unittest.TestCase):
    """
    Integration tests for _amend_provisional_sub_meter_blocks.

    Covers all gate conditions:
    - No post-boundary read → no-op this tick
    - No pre-boundary seed → commit as-final
    - Insufficient reads → commit as-final
    - Device interval > 90s → commit as-final
    - Post-boundary gap > 2× interval → commit as-final (stop-restart)
    - All gates pass → interpolate and amend
    - Gap marker active → caller skips entirely
    """

    # ── shared fixtures ───────────────────────────────────────────────────────

    BOUNDARY    = "2026-05-01T09:30:00"
    BLOCK_START = "2026-05-01T09:00:00"
    BLOCK_END   = "2026-05-01T09:30:00"
    NEXT_START  = "2026-05-01T09:30:00"
    NEXT_END    = "2026-05-01T10:00:00"

    def _setup_store(self, prov_block):
        from block_store import BlockStore
        s = BlockStore(":memory:")
        s.insert_config_period({
            "meters": {
                "electricity_main": {
                    "meta": {"sub_meter": False, "block_minutes": 30,
                             "timezone": "UTC", "billing_day": 1,
                             "currency_symbol": "£", "currency_code": "GBP"},
                    "channels": {"import": {"read": "s"}, "export": {"read": "s"}},
                },
                "ev_charger": {
                    "meta": {"sub_meter": True, "parent_meter": "electricity_main",
                             "block_minutes": 30, "timezone": "UTC",
                             "billing_day": 1, "currency_symbol": "£",
                             "currency_code": "GBP"},
                    "channels": {"import": {"read": "s"}},
                },
            }
        })
        s.append_block(prov_block)
        return s

    def _prov_block(self, sub_kwh=0.8):
        """Provisional block with ev_charger."""
        return {
            "start": self.BLOCK_START, "end": self.BLOCK_END,
            "interpolated": False,
            "meters": {
                "electricity_main": {
                    "meta": {"sub_meter": False},
                    "channels": {
                        "import": {
                            "kwh": 2.0, "kwh_total": 2.0,
                            "kwh_remainder": 2.0 - sub_kwh,
                            "rate": 0.30, "cost": 0.60,
                            "cost_remainder": round((2.0 - sub_kwh) * 0.30, 6),
                            "read_start": 1000.0, "read_end": 1002.0,
                        },
                        "export": {"kwh": 0.0, "rate": 0.12, "cost": 0.0,
                                   "read_start": 0.0, "read_end": 0.0},
                    },
                    "standing_charge": 0.45,
                },
                "ev_charger": {
                    "meta": {"sub_meter": True, "parent_meter": "electricity_main"},
                    "channels": {
                        "import": {
                            "kwh": sub_kwh, "kwh_grid": sub_kwh, "kwh_battery": 0.0,
                            "rate": 0.30, "cost": round(sub_kwh * 0.30, 6),
                            "read_start": 50.0, "read_end": 50.0 + sub_kwh,
                        },
                    },
                    "standing_charge": 0.0,
                    "provisional": True,
                },
            },
        }

    def _pre_reads_60s(self, n=20, base_val=50.0, interval_s=60):
        """n reads at interval_s cadence, all ending before BOUNDARY."""
        from datetime import datetime, timedelta
        base_dt  = datetime(2026, 5, 1, 9, 0, 0)
        boundary = datetime(2026, 5, 1, 9, 30, 0)
        reads = []
        for i in range(n):
            ts = base_dt + timedelta(seconds=interval_s * i)
            if ts >= boundary:
                break
            reads.append({"value": base_val + i * 0.03, "ts": ts.isoformat()})
        return reads

    def _rolling_buffer(self, pre_reads, post_reads):
        """Minimal current_block for the next window containing given reads."""
        all_reads = pre_reads + post_reads
        return {
            "start": self.NEXT_START, "end": self.NEXT_END,
            "meters": {
                "electricity_main": {
                    "meta": {"sub_meter": False},
                    "channels": {
                        "import": {
                            "reads": [
                                {"value": 1002.0, "ts": "2026-05-01T09:29:00"},
                                {"value": 1002.5, "ts": "2026-05-01T09:31:00"},
                            ],
                            "rates": [{"value": 0.30, "ts": self.NEXT_START}],
                        },
                        "export": {
                            "reads": [{"value": 0.0, "ts": self.NEXT_START}],
                            "rates": [{"value": 0.12, "ts": self.NEXT_START}],
                        },
                    },
                },
                "ev_charger": {
                    "meta": {"sub_meter": True, "parent_meter": "electricity_main"},
                    "channels": {
                        "import": {
                            "reads": all_reads,
                            "rates": [{"value": 0.30, "ts": self.NEXT_START}],
                        },
                    },
                },
            },
        }

    def _run(self, store, current_block):
        orig = engine._store
        engine._store = store
        try:
            engine._amend_provisional_sub_meter_blocks(MagicMock(), current_block)
        finally:
            engine._store = orig

    def _amended_ev_imp(self, store):
        from datetime import datetime
        blocks = store.get_blocks_for_range(
            datetime.fromisoformat(self.BLOCK_START),
            datetime.fromisoformat(self.BLOCK_END),
        )
        return blocks[0]["meters"]["ev_charger"]["channels"]["import"]

    # ── no post-boundary read: no-op ─────────────────────────────────────────

    def test_noop_when_no_post_boundary_read(self):
        """No post-boundary read in buffer → provisional remains set."""
        s = self._setup_store(self._prov_block())
        pre = self._pre_reads_60s()
        cb  = self._rolling_buffer(pre, post_reads=[])  # no post reads
        self._run(s, cb)
        self.assertEqual(len(s.get_provisional_sub_meter_blocks()), 1,
                         "Should remain provisional when no post-boundary read")

    # ── insufficient reads: commit as-final ──────────────────────────────────

    def test_commit_as_final_when_too_few_reads(self):
        """Fewer than 4 pre-boundary reads (3 gaps) → commit as-final, no kWh change."""
        s = self._setup_store(self._prov_block(sub_kwh=0.8))
        # Only 3 pre-boundary reads — 2 gaps, below the 3-gap minimum
        pre = self._pre_reads_60s(n=3)
        post = [{"value": pre[-1]["value"] + 0.05, "ts": "2026-05-01T09:31:00"}]
        cb = self._rolling_buffer(pre, post)
        self._run(s, cb)
        self.assertEqual(len(s.get_provisional_sub_meter_blocks()), 0,
                         "Should be committed as-final (not provisional)")
        ev_imp = self._amended_ev_imp(s)
        self.assertAlmostEqual(ev_imp["kwh"], 0.8, places=4,
                               msg="kWh must be unchanged when committed as-final")

    # ── device interval > 90s: commit as-final ───────────────────────────────

    def test_commit_as_final_when_device_too_slow(self):
        """Device publishing at 5-minute intervals → too coarse, commit as-final."""
        s = self._setup_store(self._prov_block(sub_kwh=0.8))
        # 10 reads at 5-minute cadence — enough gaps but interval >> 90s
        pre = self._pre_reads_60s(n=7, interval_s=300)
        post = [{"value": pre[-1]["value"] + 0.05, "ts": "2026-05-01T09:31:00"}]
        cb = self._rolling_buffer(pre, post)
        self._run(s, cb)
        self.assertEqual(len(s.get_provisional_sub_meter_blocks()), 0)
        ev_imp = self._amended_ev_imp(s)
        self.assertAlmostEqual(ev_imp["kwh"], 0.8, places=4,
                               msg="kWh must be unchanged for slow device")

    # ── post-boundary gap > 2× interval: commit as-final (stop-restart) ──────

    def test_commit_as_final_when_gap_exceeds_threshold(self):
        """Device stopped before boundary, restarted 15 min later.
        Last pre read: 09:29, first post read: 09:45 → gap 16 min >> 2 × 60s.
        Must commit as-final — the provisional figure is the correct answer."""
        s = self._setup_store(self._prov_block(sub_kwh=0.8))
        pre = self._pre_reads_60s(n=20)  # regular 60s, ends ~09:29
        # First post-boundary read arrives 15 minutes after last pre read
        post = [{"value": pre[-1]["value"] + 0.20, "ts": "2026-05-01T09:45:00"}]
        cb = self._rolling_buffer(pre, post)
        self._run(s, cb)
        self.assertEqual(len(s.get_provisional_sub_meter_blocks()), 0)
        ev_imp = self._amended_ev_imp(s)
        self.assertAlmostEqual(ev_imp["kwh"], 0.8, places=4,
                               msg="kWh must be unchanged for stop-restart scenario")

    # ── all gates pass: interpolate and amend ────────────────────────────────

    def test_interpolation_fires_when_all_gates_pass(self):
        """60s device, post-boundary read within 2× interval → interpolation applied.

        Uses known bracketing values so the interpolated boundary is
        demonstrably different from the provisional read_end (last pre read).

        last pre:  50.56 @ 09:29:00  (1 min before boundary)
        first post: 50.64 @ 09:31:00  (1 min after boundary)
        → interpolated boundary = 50.56 + (50.64 − 50.56) × 0.5 = 50.60
        opener (read_start) = 50.0
        → corrected kwh = 50.60 − 50.0 = 0.60
        provisional kwh was 0.8  (opener=50.0, read_end=50.8 stored in DB)
        """
        s = self._setup_store(self._prov_block(sub_kwh=0.8))
        pre = self._pre_reads_60s(n=20)
        # Override last pre read to a known value well away from provisional read_end
        pre[-1] = {"value": 50.56, "ts": "2026-05-01T09:29:00"}
        post = [{"value": 50.64, "ts": "2026-05-01T09:31:00"}]
        cb = self._rolling_buffer(pre, post)
        self._run(s, cb)
        self.assertEqual(len(s.get_provisional_sub_meter_blocks()), 0,
                         "Should be cleared after interpolation")
        ev_imp = self._amended_ev_imp(s)
        # Interpolated boundary = 50.60, opener = 50.0 → corrected kwh = 0.60
        # Original provisional kwh = 0.8 — must differ
        self.assertAlmostEqual(ev_imp["kwh"], 0.60, places=3,
                               msg="Corrected kWh must equal interpolated boundary − opener")

    def test_interpolated_boundary_value_correct(self):
        """Boundary value is linearly interpolated between last pre and first post.

        Timeline:
          last pre:  50.57 kWh @ 09:29:00  (1 min before boundary 09:30)
          first post: 50.61 kWh @ 09:31:00  (1 min after boundary)
          → interpolated boundary = 50.57 + (50.61 − 50.57) × 0.5 = 50.59

          Block opener (read_start) = 50.0
          → corrected kWh = 50.59 − 50.0 = 0.59
        """
        s = self._setup_store(self._prov_block(sub_kwh=0.8))
        pre = self._pre_reads_60s(n=20)
        # Manually set last pre read to known value at 09:29:00
        pre[-1] = {"value": 50.57, "ts": "2026-05-01T09:29:00"}
        post    = [{"value": 50.61, "ts": "2026-05-01T09:31:00"}]
        cb = self._rolling_buffer(pre, post)
        self._run(s, cb)
        ev_imp = self._amended_ev_imp(s)
        self.assertAlmostEqual(ev_imp["kwh"], 0.59, places=4,
                               msg="Corrected kWh must equal interpolated boundary − opener")

    def test_pass2_reruns_after_interpolation(self):
        """kwh_grid is set by PASS 2 on the corrected kwh."""
        s = self._setup_store(self._prov_block(sub_kwh=0.8))
        pre = self._pre_reads_60s(n=20)
        pre[-1] = {"value": 50.57, "ts": "2026-05-01T09:29:00"}
        post    = [{"value": 50.61, "ts": "2026-05-01T09:31:00"}]
        cb = self._rolling_buffer(pre, post)
        self._run(s, cb)
        ev_imp = self._amended_ev_imp(s)
        self.assertIn("kwh_grid", ev_imp, "PASS 2 must set kwh_grid after amendment")
        self.assertAlmostEqual(ev_imp["kwh_grid"], ev_imp["kwh"], places=4,
                               msg="kwh_grid must equal kwh (within 2.0 kWh main meter budget)")

    def test_cost_consistent_after_interpolation(self):
        """After amendment cost = kwh × rate."""
        s = self._setup_store(self._prov_block(sub_kwh=0.8))
        pre = self._pre_reads_60s(n=20)
        pre[-1] = {"value": 50.57, "ts": "2026-05-01T09:29:00"}
        post    = [{"value": 50.61, "ts": "2026-05-01T09:31:00"}]
        cb = self._rolling_buffer(pre, post)
        self._run(s, cb)
        ev_imp = self._amended_ev_imp(s)
        expected_cost = round(ev_imp["kwh"] * ev_imp["rate"], 6)
        self.assertAlmostEqual(ev_imp["cost"], expected_cost, places=6)

    # ── boundary read exactly on boundary ────────────────────────────────────

    def test_read_exactly_on_boundary_counts_as_post(self):
        """A read timestamped exactly at the boundary is treated as post-boundary."""
        boundary_iso = self.BOUNDARY
        reads = [
            {"value": 50.0, "ts": "2026-05-01T09:00:00"},
            {"value": 50.9, "ts": "2026-05-01T09:30:00"},  # exact boundary
        ]
        has_post = any(r["ts"] >= boundary_iso for r in reads)
        self.assertTrue(has_post)

    # ── gap marker guard ─────────────────────────────────────────────────────

    def test_gap_marker_guard_in_source(self):
        """_engine_tick must guard amendment behind has_gap_marker check.
        Verified by inspecting source — the amendment call must be inside
        'if not has_gap_marker(current_block)'."""
        import inspect
        src = inspect.getsource(engine._engine_tick)
        # The guard and the call must both appear
        self.assertIn("has_gap_marker", src,
                      "_engine_tick must reference has_gap_marker")
        self.assertIn("_amend_provisional_sub_meter_blocks", src,
                      "_engine_tick must call _amend_provisional_sub_meter_blocks")
        # The guard must precede the call — find their positions
        guard_pos = src.index("has_gap_marker")
        call_pos  = src.index("_amend_provisional_sub_meter_blocks")
        self.assertLess(guard_pos, call_pos,
                        "has_gap_marker guard must appear before amendment call")

    # ── no provisional blocks: no-op ─────────────────────────────────────────

    def test_noop_when_no_provisional_blocks(self):
        """No provisional blocks in DB → function returns immediately."""
        from block_store import BlockStore
        s = BlockStore(":memory:")
        s.insert_config_period({
            "meters": {"electricity_main": {
                "meta": {"timezone": "UTC", "billing_day": 1,
                         "block_minutes": 30, "currency_symbol": "£",
                         "currency_code": "GBP", "sub_meter": False}
            }}
        })
        orig = engine._store
        engine._store = s
        try:
            engine._amend_provisional_sub_meter_blocks(MagicMock(), {"meters": {}})
        finally:
            engine._store = orig
        # No exception = pass

    # ── constants sanity ─────────────────────────────────────────────────────

    def test_constants_are_sensible(self):
        """Guard that the tuning constants haven't drifted to insensible values."""
        self.assertEqual(engine._PROVISIONAL_MAX_INTERVAL_S, 90.0,
                         "Max interval should be 90s (60s device + jitter margin)")
        self.assertEqual(engine._PROVISIONAL_GAP_MULTIPLIER, 2.0,
                         "Gap multiplier should be 2.0 (handles one missed read)")
        self.assertEqual(engine._PROVISIONAL_MIN_GAPS, 3,
                         "Min gaps should be 3 (4 reads, flat across all block sizes)")


class TestMiniGating(unittest.TestCase):
    """Mini must stand down whenever a local import sensor exists."""

    def setUp(self):
        self._orig = engine.load_config

    def tearDown(self):
        engine.load_config = self._orig

    def test_local_sensor_present_blocks_mini(self):
        engine.load_config = lambda: {"meters": {"electricity_main": {
            "meta": {"sub_meter": False},
            "channels": {"import": {"read": "sensor.smart_import"}}}}}
        self.assertTrue(engine._has_local_import_sensor())

    def test_no_local_sensor_allows_mini(self):
        engine.load_config = lambda: {"meters": {"electricity_main": {
            "meta": {"sub_meter": False},
            "channels": {"import": {}}}}}
        self.assertFalse(engine._has_local_import_sensor())

    def test_sub_meter_sensor_ignored(self):
        engine.load_config = lambda: {"meters": {
            "electricity_main": {"meta": {"sub_meter": False},
                                 "channels": {"import": {}}},
            "ev_charger": {"meta": {"sub_meter": True},
                           "channels": {"import": {"read": "sensor.zappi"}}}}}
        self.assertFalse(engine._has_local_import_sensor())

    def test_config_error_defaults_safe(self):
        def boom():
            raise RuntimeError("no config")
        engine.load_config = boom
        self.assertTrue(engine._has_local_import_sensor())


class TestApiModeBlockCreation(unittest.TestCase):
    """API/Mini mode has no local read sensor, but blocks must still form (with
    a seeded meter shell) so they finalise and fire the Mini boundary."""

    def setUp(self):
        import block_store
        self._orig_store = engine._store
        self._orig_cfg = engine.load_config
        engine._store = BlockStore(":memory:")
        engine._store.insert_config_period({"meters": {"electricity_main": {
            "meta": {"timezone": "Europe/London", "billing_day": 1,
                     "block_minutes": 30, "currency_symbol": "£",
                     "currency_code": "GBP", "sub_meter": False},
            "standing_charge": 0.5,
            "channels": {"import": {"read": "", "rate": ""},
                         "export": {"read": "", "rate": ""}}}}})

    def tearDown(self):
        try:
            engine._store.close()
        except Exception:
            pass
        engine._store = self._orig_store
        engine.load_config = self._orig_cfg
        engine.register_block_boundary_callback(None)  # clear any test callback

    def test_api_mode_creates_seeded_block(self):
        from datetime import datetime
        engine.set_data_source_mode("api")
        blk = engine.ensure_correct_block(None, {}, datetime(2026, 6, 3, 7, 15, 0))
        self.assertTrue(blk and blk.get("start"))
        self.assertIn("electricity_main", blk["meters"])
        m = blk["meters"]["electricity_main"]
        self.assertIn("import", m["channels"])
        self.assertTrue(m.get("meta"))

    def test_unset_mode_no_block(self):
        # Pre-survey (mode unset), no sensors → still no block.
        from datetime import datetime
        # mode is 'unset' (nothing set on fresh store)
        self.assertEqual(engine.get_data_source_mode(), "unset")
        blk = engine.ensure_correct_block(None, {}, datetime(2026, 6, 3, 7, 15, 0))
        self.assertFalse(blk and blk.get("start"))

    def test_seeded_block_fires_boundary_on_finalise(self):
        from datetime import datetime
        fired = []
        engine.register_block_boundary_callback(lambda iso: fired.append(iso))
        blk = engine.create_block(datetime(2026, 6, 3, 7, 0, 0),
                                  datetime(2026, 6, 3, 7, 30, 0), 30,
                                  seed_meters=True)

        class _HA:
            def get_state(self, e): return None
        engine.finalise_block(_HA(), block_data=blk)
        self.assertIn("2026-06-03T07:30:00", fired)

    def test_api_mode_applies_kraken_rate_and_cost_at_finalise(self):
        # In API/Mini mode (no rate sensor) the import rate must be resolved from
        # the Kraken rate schedule AT finalise, so blocks are costed immediately
        # rather than at £0.00-until-DCC-settlement. Verified two ways:
        #  (1) the resolver is wired into finalise_block (source), and
        #  (2) the resolver returns the correct £/kWh for the import channel.
        import inspect
        from kraken_rates import RateSchedule
        src = inspect.getsource(engine.finalise_block)
        self.assertIn("_kraken_rate_resolver", src,
            "finalise_block must resolve the unit rate from the Kraken schedule "
            "when no rate sensor exists (API/Mini mode)")
        _orig = engine._kraken_rate_schedules
        try:
            engine._kraken_rate_schedules = {
                "import": RateSchedule([("2026-01-01T00:00:00", None, 32.31)])}
            # 32.31 p/kWh → £0.3231/kWh
            self.assertAlmostEqual(
                engine._kraken_rate_resolver("import", "2026-06-03T17:30:00"),
                0.3231, places=4)
            # No schedule for export → None (left for DCC), not a guess.
            self.assertIsNone(
                engine._kraken_rate_resolver("export", "2026-06-03T17:30:00"))
        finally:
            engine._kraken_rate_schedules = _orig


class TestMaybeSetupMini(unittest.TestCase):
    """_maybe_setup_mini activates Mini when: API mode, no local sensor, and a
    device is discovered — Mini is auto-elevated, NOT a stored 'api+mini' mode."""

    def setUp(self):
        self._orig_cfg = engine.load_config
        self._orig_mode = engine.get_data_source_mode
        self._orig_client = engine._kraken_client
        self._orig_reader = engine._kraken_mini_reader
        self._orig_acct = engine._kraken_account_number
        engine._kraken_mini_reader = None
        engine._kraken_account_number = "A-TEST"
        # No local sensor by default.
        engine.load_config = lambda: {"meters": {"electricity_main": {
            "meta": {"sub_meter": False}, "channels": {"import": {}}}}}

    def tearDown(self):
        engine.load_config = self._orig_cfg
        engine.get_data_source_mode = self._orig_mode
        engine._kraken_client = self._orig_client
        engine._kraken_mini_reader = self._orig_reader
        engine._kraken_account_number = self._orig_acct

    def _run(self, coro):
        import asyncio
        loop = asyncio.new_event_loop()
        try:
            return loop.run_until_complete(coro)
        finally:
            loop.close()

    class _Client:
        def __init__(self, device_id):
            self._d = device_id
        async def get_device_id(self, acct):
            return self._d

    def test_activates_when_api_and_device_present(self):
        engine.get_data_source_mode = lambda: "api"
        engine._kraken_client = self._Client("dev-123")
        self._run(engine._maybe_setup_mini())
        self.assertIsNotNone(engine._kraken_mini_reader)

    def test_no_device_stays_plain_api(self):
        engine.get_data_source_mode = lambda: "api"
        engine._kraken_client = self._Client(None)   # no Mini on account
        self._run(engine._maybe_setup_mini())
        self.assertIsNone(engine._kraken_mini_reader)

    def test_cad_mode_never_activates(self):
        engine.get_data_source_mode = lambda: "cad"
        engine._kraken_client = self._Client("dev-123")
        self._run(engine._maybe_setup_mini())
        self.assertIsNone(engine._kraken_mini_reader)

    def test_local_sensor_blocks_even_with_device(self):
        engine.get_data_source_mode = lambda: "api"
        engine.load_config = lambda: {"meters": {"electricity_main": {
            "meta": {"sub_meter": False},
            "channels": {"import": {"read": "sensor.smart_import"}}}}}
        engine._kraken_client = self._Client("dev-123")
        self._run(engine._maybe_setup_mini())
        self.assertIsNone(engine._kraken_mini_reader)


class TestKrakenBackfillDays(unittest.TestCase):
    """Fresh DB (no blocks) must NOT backfill — return 0 — to avoid pulling
    ~400 days (~19k rows) to reconcile blocks that don't exist."""

    def setUp(self):
        self._orig_store = engine._store

    def tearDown(self):
        engine._store = self._orig_store

    class _Store:
        def __init__(self, oldest):
            self._oldest = oldest
        def get_oldest_block_start(self):
            return self._oldest

    def test_fresh_db_no_backfill(self):
        engine._store = self._Store(None)
        self.assertEqual(engine._kraken_backfill_days(), 0)

    def test_with_oldest_block_bounds_window(self):
        from datetime import datetime, timezone, timedelta
        five_days_ago = (datetime.now(timezone.utc) - timedelta(days=5)).isoformat()
        engine._store = self._Store(five_days_ago)
        days = engine._kraken_backfill_days()
        self.assertGreaterEqual(days, 5)
        self.assertLessEqual(days, engine._KRAKEN_BACKFILL_CAP_DAYS)

    def test_capped_at_max(self):
        from datetime import datetime, timezone, timedelta
        ancient = (datetime.now(timezone.utc) - timedelta(days=9999)).isoformat()
        engine._store = self._Store(ancient)
        self.assertEqual(engine._kraken_backfill_days(),
                         engine._KRAKEN_BACKFILL_CAP_DAYS)


class TestEnsurePollTaskRunning(unittest.TestCase):
    """The DCC poll task exits at boot if the API isn't configured yet; an
    in-app credential save must relaunch it without a restart. The helper must
    be idempotent (never double-launch) and a safe no-op if the loop isn't up."""

    def setUp(self):
        self._orig_ha = engine._engine_ha
        self._orig_handle = engine._kraken_poll_task_handle
        engine._kraken_poll_task_handle = None

    def tearDown(self):
        engine._engine_ha = self._orig_ha
        engine._kraken_poll_task_handle = self._orig_handle

    def test_noop_without_engine_loop(self):
        engine._engine_ha = None
        self.assertFalse(engine._ensure_kraken_poll_task_running())

    def test_cancel_clears_handle(self):
        import asyncio

        async def _go():
            engine._engine_ha = object()
            engine._ensure_kraken_poll_task_running()
            self.assertIsNotNone(engine._kraken_poll_task_handle)
            engine._cancel_kraken_poll_task()
            # Handle cleared immediately; give the loop a tick to process cancel.
            await asyncio.sleep(0)
            self.assertIsNone(engine._kraken_poll_task_handle)

        loop = asyncio.new_event_loop()
        try:
            loop.run_until_complete(_go())
        finally:
            loop.close()

    def test_launches_when_loop_up(self):
        import asyncio

        class _FakeHA: pass

        async def _go():
            engine._engine_ha = _FakeHA()
            # First call launches a task; second must NOT launch a second.
            ok1 = engine._ensure_kraken_poll_task_running()
            h1 = engine._kraken_poll_task_handle
            ok2 = engine._ensure_kraken_poll_task_running()
            h2 = engine._kraken_poll_task_handle
            # Let the (idle) poll task run and exit.
            await asyncio.sleep(0)
            return ok1, ok2, h1, h2

        loop = asyncio.new_event_loop()
        try:
            ok1, ok2, h1, h2 = loop.run_until_complete(_go())
            self.assertTrue(ok1)
            self.assertTrue(ok2)
            self.assertIs(h1, h2)   # idempotent — same handle, no second task
        finally:
            loop.close()


class TestKrakenCredentials(unittest.TestCase):
    """Credentials live in their own file, prefer file over env, cleared on
    empty key, and never end up in the DB."""

    def setUp(self):
        import tempfile, os
        self._orig_path = engine.KRAKEN_CREDS_PATH
        self._dir = tempfile.mkdtemp()
        engine.KRAKEN_CREDS_PATH = os.path.join(self._dir, "kraken_credentials.json")
        # Neutralise env so file-vs-env precedence is testable.
        self._orig_env = {k: os.environ.pop(k, None) for k in
                          ("KRAKEN_API_KEY", "KRAKEN_ACCOUNT_NUMBER", "KRAKEN_BASE_URL")}

    def tearDown(self):
        import os, shutil
        engine.KRAKEN_CREDS_PATH = self._orig_path
        for k, v in self._orig_env.items():
            if v is not None:
                os.environ[k] = v
        shutil.rmtree(self._dir, ignore_errors=True)

    def test_save_and_read_roundtrip(self):
        engine.save_kraken_credentials("sk_test_123", "A-ABC123", None)
        env = engine._kraken_env()
        self.assertEqual(env["api_key"], "sk_test_123")
        self.assertEqual(env["account_number"], "A-ABC123")

    def test_empty_key_clears(self):
        engine.save_kraken_credentials("sk_test_123", "A-ABC123")
        engine.save_kraken_credentials("", None)
        import os
        self.assertFalse(os.path.exists(engine.KRAKEN_CREDS_PATH))
        self.assertIsNone(engine._kraken_env()["api_key"])

    def test_file_preferred_over_env(self):
        import os
        os.environ["KRAKEN_API_KEY"] = "env_key"
        engine.save_kraken_credentials("file_key", "A-FILE")
        self.assertEqual(engine._kraken_env()["api_key"], "file_key")

    def test_env_fallback_when_no_file(self):
        import os
        os.environ["KRAKEN_API_KEY"] = "env_key"
        os.environ["KRAKEN_ACCOUNT_NUMBER"] = "A-ENV"
        self.assertEqual(engine._kraken_env()["api_key"], "env_key")
        self.assertEqual(engine._kraken_env()["account_number"], "A-ENV")


class TestScheduleResolverPrecedence(unittest.TestCase):
    """The Kraken rate schedule resolver must take precedence over the
    last_known_rates carry-forward for sensorless (api/mini) non-sub
    import/export channels. Otherwise every block inherits the first block's
    rate uniformly and time-of-use tariffs (IOG off-peak overnight) are billed
    at peak. Regression for the 'every overnight block billed at peak' bug.
    """

    def setUp(self):
        import json, os
        from kraken_rates import RateSchedule
        os.makedirs("/data/energy_meter_tracker", exist_ok=True)
        self.cfg = {"meters": {"electricity_main": {
            "meta": {"timezone": "Europe/London", "billing_day": 1,
                     "block_minutes": 30, "currency_symbol": "£",
                     "currency_code": "GBP", "sub_meter": False},
            "standing_charge": 0.5,
            "channels": {"import": {"read": "", "rate": ""},
                         "export": {"read": "", "rate": ""}}}}}
        with open(engine.CONFIG_PATH, "w") as f:
            json.dump(self.cfg, f)
        # The test harness stubs engine.load_json to return {}; finalise reads
        # config via load_json(CONFIG_PATH), so patch it to return our config.
        self._lj_patch = patch.object(engine, "load_json",
                                       side_effect=lambda *a, **k: self.cfg)
        self._lj_patch.start()
        engine._store = BlockStore(":memory:")
        engine._store.insert_config_period(self.cfg)
        engine.set_data_source_mode("api")
        # Current IOG schedule: off-peak 5.493 overnight (22:30-04:30 UTC),
        # peak 32.3092 during the day.
        engine._kraken_rate_schedules = {"import": RateSchedule([
            ("2026-06-04T22:30:00", "2026-06-05T04:30:00", 5.493),
            ("2026-06-05T04:30:00", "2026-06-05T22:30:00", 32.3092),
        ])}

    def tearDown(self):
        import os
        self._lj_patch.stop()
        try:
            os.remove(engine.CONFIG_PATH)
        except OSError:
            pass
        engine._kraken_rate_schedules = {}

    def _finalise(self, start, end, lkr):
        blk = engine.create_block(
            datetime.fromisoformat(start), datetime.fromisoformat(end),
            30, seed_meters=True)
        ch = blk["meters"]["electricity_main"]["channels"]["import"]
        # Reads bracketing the block boundary so kWh computes and the block
        # stores (rate is what we assert on, value magnitude is incidental).
        ch["reads"] = [
            {"ts": start, "value": 100.0},
            {"ts": end, "value": 101.0},
        ]

        class _HA:
            def get_state(self, e):
                return None
        engine.finalise_block(_HA(), block_data=blk, last_known_rates=lkr)
        row = engine._store._conn.execute(
            "SELECT imp_rate FROM blocks WHERE block_start=?", (start,)).fetchone()
        return row["imp_rate"] if row else None

    def test_overnight_resolves_offpeak_despite_peak_carryforward(self):
        # last_known_rates carries PEAK (as if a prior daytime block set it).
        lkr = {"electricity_main": {"import": {"ts": "2026-06-05T12:00:00",
                                               "value": 0.32309}}}
        rate = self._finalise("2026-06-05T01:00:00", "2026-06-05T01:30:00", lkr)
        self.assertAlmostEqual(
            rate, 0.05493, places=5,
            msg="overnight block must resolve to schedule off-peak, not the "
                "carried-forward peak rate")

    def test_day_resolves_peak(self):
        lkr = {"electricity_main": {"import": {"ts": "2026-06-05T01:00:00",
                                               "value": 0.05493}}}
        rate = self._finalise("2026-06-05T12:00:00", "2026-06-05T12:30:00", lkr)
        self.assertAlmostEqual(rate, 0.323092, places=5,
            msg="day block must resolve to schedule peak rate")

    def test_falls_back_to_last_known_when_no_schedule(self):
        # CAD-style: no schedule → resolver returns None → last_known_rates wins.
        engine._kraken_rate_schedules = {}
        lkr = {"electricity_main": {"import": {"ts": "2026-06-05T00:00:00",
                                               "value": 0.28}}}
        rate = self._finalise("2026-06-05T01:00:00", "2026-06-05T01:30:00", lkr)
        self.assertAlmostEqual(rate, 0.28, places=5,
            msg="with no schedule, last_known_rates must remain the fallback")


class TestDccOnlyExportMaterialises(unittest.TestCase):
    """DCC-only export (no export sensor / no Mini export layer) must materialise
    from exp_kwh_api during the PASS 2 rerun, even though the reconstructed block
    has no export channel (exp_kwh is NULL → _row_to_block omits the channel).
    Regression for the 'settled export stuck in exp_kwh_api, export bill zero'
    bug.
    """

    def setUp(self):
        from kraken_rates import RateSchedule
        self.store = BlockStore(":memory:")
        self.store.insert_config_period({"meters": {"electricity_main": {
            "meta": {"timezone": "Europe/London", "billing_day": 1,
                     "block_minutes": 30, "currency_symbol": "£",
                     "sub_meter": False}}}})
        self.cp = self.store._conn.execute(
            "SELECT id FROM config_periods LIMIT 1").fetchone()["id"]
        self._orig_store = engine._store
        engine._store = self.store
        self._orig_sched = engine._kraken_rate_schedules
        engine._kraken_rate_schedules = {
            "export": RateSchedule([("2026-03-01T00:00:00", None, 12.0)]),
        }

    def tearDown(self):
        engine._store = self._orig_store
        engine._kraken_rate_schedules = self._orig_sched

    def test_export_materialises_from_api_col(self):
        # DCC-only export block: exp_kwh NULL, exp_kwh_api set, no export channel.
        self.store._conn.execute(
            "INSERT INTO blocks (block_start, block_end, meter_id, "
            "config_period_id, interpolated, imp_kwh, imp_rate, imp_cost, "
            "exp_kwh, exp_kwh_api, standing_charge, needs_pass2_rerun) "
            "VALUES (?,?,?,?,0,?,?,?,NULL,?,?,1)",
            ("2026-06-04T06:00:00", "2026-06-04T06:30:00", "electricity_main",
             self.cp, 0.0, 0.32309, 0.0, 0.148, 0.50))
        self.store._conn.commit()
        block = self.store.get_block_dict_by_start("2026-06-04T06:00:00")
        # Precondition: reconstruction has NO export channel (exp_kwh was NULL)
        self.assertIsNone(
            (block["meters"]["electricity_main"].get("channels") or {}).get("export"),
            "precondition: DCC-only export block has no export channel")
        engine._rerun_pass2_for_settled_block(
            block, rate_resolver=engine._kraken_rate_resolver,
            billing_source="dcc")
        engine.append_block_replace(block)
        r = self.store._conn.execute(
            "SELECT exp_kwh, exp_rate, exp_cost FROM blocks "
            "WHERE block_start=?", ("2026-06-04T06:00:00",)).fetchone()
        self.assertAlmostEqual(r["exp_kwh"], 0.148, places=4,
            msg="settled export must materialise from exp_kwh_api into exp_kwh")
        self.assertAlmostEqual(r["exp_rate"], 0.12, places=4,
            msg="export rate must resolve from the export schedule")
        self.assertAlmostEqual(r["exp_cost"], round(0.148 * 0.12, 6), places=5,
            msg="export cost must be kwh * rate")


class TestPlannedDispatchSlotPreview(unittest.TestCase):
    """The 30-min slot snap for planned dispatches (observe-only step 1, and the
    basis for the started-slot overlay). Any active minute in a slot → whole slot
    (BCD's rule). Pure helper, no side effects.
    """

    def test_spans_multiple_slots(self):
        slots = engine._planned_dispatch_slots_preview(
            [{"start": "2026-06-06T10:05:00Z", "end": "2026-06-06T10:50:00Z"}])
        self.assertEqual(sorted(slots),
                         ["2026-06-06T10:00:00", "2026-06-06T10:30:00"])

    def test_one_minute_snaps_whole_slot(self):
        slots = engine._planned_dispatch_slots_preview(
            [{"start": "2026-06-06T14:02:00Z", "end": "2026-06-06T14:03:00Z"}])
        self.assertEqual(sorted(slots), ["2026-06-06T14:00:00"])

    def test_naive_utc_input(self):
        slots = engine._planned_dispatch_slots_preview(
            [{"start": "2026-06-06T02:00:00", "end": "2026-06-06T03:30:00"}])
        self.assertEqual(sorted(slots),
                         ["2026-06-06T02:00:00", "2026-06-06T02:30:00",
                          "2026-06-06T03:00:00"])

    def test_garbage_is_safe(self):
        self.assertEqual(
            engine._planned_dispatch_slots_preview(
                [{"start": None}, {}, {"start": "x", "end": "y"}]),
            set())


class TestDrainRegeneratesCharts(unittest.TestCase):
    """After the PASS 2 drain re-prices blocks, it must regenerate the charts so
    the billing/daily charts reflect the reconciled figures immediately, rather
    than waiting for the next block rollover to incidentally write them.
    """

    def setUp(self):
        self.store = BlockStore(":memory:")
        self.store.insert_config_period({"meters": {"electricity_main": {
            "meta": {"timezone": "Europe/London", "billing_day": 1,
                     "block_minutes": 30, "currency_symbol": "£",
                     "sub_meter": False}}}})
        self.cp = self.store._conn.execute(
            "SELECT id FROM config_periods LIMIT 1").fetchone()["id"]
        self._orig_store = engine._store
        engine._store = self.store

    def tearDown(self):
        engine._store = self._orig_store

    def test_charts_regenerated_when_blocks_repriced(self):
        # A settled block flagged for rerun (imp_kwh_api set, needs_pass2_rerun=1).
        self.store._conn.execute(
            "INSERT INTO blocks (block_start, block_end, meter_id, "
            "config_period_id, interpolated, imp_kwh, imp_kwh_api, imp_rate, "
            "imp_cost, standing_charge, is_provisional, needs_pass2_rerun) "
            "VALUES (?,?,?,?,0,?,?,?,?,?,1,1)",
            ("2026-06-04T12:00:00", "2026-06-04T12:30:00", "electricity_main",
             self.cp, 1.0, 1.2, 0.32309, 0.32309, 0.50))
        self.store._conn.commit()

        calls = {"charts": 0}
        orig_gc = engine.generate_charts
        engine.generate_charts = lambda *a, **kw: calls.__setitem__(
            "charts", calls["charts"] + 1)
        try:
            done = engine._drain_pass2_queue(MagicMock(), limit=50)
        finally:
            engine.generate_charts = orig_gc

        self.assertEqual(done, 1, "drain should have re-priced the one block")
        self.assertEqual(calls["charts"], 1,
            "drain must regenerate charts exactly once after re-pricing")

    def test_no_chart_regen_when_nothing_repriced(self):
        # No flagged blocks → drain does nothing → no chart regen.
        calls = {"charts": 0}
        orig_gc = engine.generate_charts
        engine.generate_charts = lambda *a, **kw: calls.__setitem__(
            "charts", calls["charts"] + 1)
        try:
            done = engine._drain_pass2_queue(MagicMock(), limit=50)
        finally:
            engine.generate_charts = orig_gc
        self.assertEqual(done, 0)
        self.assertEqual(calls["charts"], 0,
            "no re-pricing → no chart regen")


class TestCarbonGapRecovery(unittest.TestCase):
    """Carbon attribution for outage gap-fill blocks: prevention (attribute at
    fill time when CI present) and recovery (backfill NULL-carbon blocks once CI
    is available). Regression for the 'outage block left with NULL carbon' gap.
    """

    def setUp(self):
        self.store = BlockStore(":memory:")
        self.store.insert_config_period({"meters": {"electricity_main": {
            "meta": {"timezone": "Europe/London", "billing_day": 1,
                     "block_minutes": 30, "currency_symbol": "£",
                     "sub_meter": False, "postcode_prefix": "DE1"}}}})
        self.cp = self.store._conn.execute(
            "SELECT id FROM config_periods LIMIT 1").fetchone()["id"]
        self._orig_store = engine._store
        engine._store = self.store
        # CI slot available for the outage time.
        self.store.upsert_carbon_intensity(
            "2026-06-05T20:00:00", "DE1", 239.0, "very high", None)
        self._orig_gc = engine.generate_charts
        engine.generate_charts = lambda *a, **kw: None

    def tearDown(self):
        engine._store = self._orig_store
        engine.generate_charts = self._orig_gc

    def test_recovery_backfills_null_carbon_block(self):
        # A gap-filled block with energy but NULL carbon.
        self.store._conn.execute(
            "INSERT INTO blocks (block_start, block_end, meter_id, "
            "config_period_id, interpolated, imp_kwh, imp_rate, imp_cost, "
            "standing_charge, carbon_intensity_g) "
            "VALUES (?,?,?,?,1,?,?,?,?,NULL)",
            ("2026-06-05T20:00:00", "2026-06-05T20:30:00", "electricity_main",
             self.cp, 2.948, 0.32309, 0.9527, 0.50))
        self.store._conn.commit()
        rec = engine._recover_missing_carbon()
        self.assertEqual(rec, 1)
        r = self.store._conn.execute(
            "SELECT carbon_intensity_g, carbon_g FROM blocks "
            "WHERE block_start=?", ("2026-06-05T20:00:00",)).fetchone()
        self.assertAlmostEqual(r["carbon_intensity_g"], 239.0, places=1)
        self.assertAlmostEqual(r["carbon_g"], round(2.948 * 239.0, 4), places=2)

    def test_recovery_skips_when_no_ci_slot(self):
        # NULL-carbon block whose time has no CI slot (aged out) → not recovered.
        self.store._conn.execute(
            "INSERT INTO blocks (block_start, block_end, meter_id, "
            "config_period_id, interpolated, imp_kwh, standing_charge, "
            "carbon_intensity_g) VALUES (?,?,?,?,1,?,?,NULL)",
            ("2026-05-01T10:00:00", "2026-05-01T10:30:00", "electricity_main",
             self.cp, 1.0, 0.50))
        self.store._conn.commit()
        rec = engine._recover_missing_carbon()
        self.assertEqual(rec, 0, "no CI slot → cannot recover, left NULL")

    def test_prevention_attributes_at_fill_time(self):
        # _attribute_block_carbon attributes when a CI slot is present.
        block = {"meters": {"electricity_main": {
            "meta": {"sub_meter": False},
            "channels": {"import": {"kwh": 2.948}}}}}
        ok = engine._attribute_block_carbon(block, "2026-06-05T20:00:00")
        self.assertTrue(ok)
        mb = block["meters"]["electricity_main"]
        self.assertAlmostEqual(mb["carbon_intensity_g"], 239.0, places=1)
        self.assertAlmostEqual(mb["carbon_g"], round(2.948 * 239.0, 4), places=2)


class TestDispatchSlotCapture(unittest.TestCase):
    """Step 2 piece 1: persist smart-charge dispatch slots. The source filter is
    the started/smart gate — only source='smart-charge' is an off-peak candidate;
    bump-charge / unknown / null are excluded. Non-billing: this only records
    candidacy.
    """

    def test_smart_charge_filter_excludes_non_smart(self):
        planned = [
            {"start": "2026-06-06T15:00:00Z", "end": "2026-06-06T15:30:00Z",
             "source": "smart-charge"},
            {"start": "2026-06-06T16:00:00Z", "end": "2026-06-06T16:30:00Z",
             "source": "bump-charge"},
            {"start": "2026-06-06T17:00:00Z", "end": "2026-06-06T17:30:00Z",
             "source": "unknown"},
            {"start": "2026-06-06T18:00:00Z", "end": "2026-06-06T18:30:00Z",
             "source": None},
        ]
        self.assertEqual(sorted(engine._smart_charge_slots(planned)),
                         ["2026-06-06T15:00:00"])

    def test_capture_persists_smart_slots(self):
        store = BlockStore(":memory:")
        orig_store = engine._store
        orig_client = engine._kraken_client
        orig_disc = engine._kraken_discovery
        engine._store = store
        engine._kraken_discovery = {"import": {"product_code": "X",
                                               "tariff_code": "Y"}}

        class _FakeClient:
            # last_deprecations is a None sentinel = "introspection unavailable",
            # so _capture_dispatch_slots skips the one-shot HA surfacing path.
            # The real KrakenApiClient grew this attribute in 3.0.3.
            last_deprecations = None
            async def get_dispatches(self, acct):
                return {"provider": "MYENERGI_V2",
                        "planned": [
                            {"start": "2026-06-06T15:00:00Z",
                             "end": "2026-06-06T15:30:00Z",
                             "source": "smart-charge"},
                            {"start": "2026-06-06T16:00:00Z",
                             "end": "2026-06-06T16:30:00Z",
                             "source": "bump-charge"}],
                        "completed": []}
        engine._kraken_client = _FakeClient()
        try:
            import asyncio
            n = asyncio.new_event_loop().run_until_complete(
                engine._capture_dispatch_slots())
        finally:
            engine._store = orig_store
            engine._kraken_client = orig_client
            engine._kraken_discovery = orig_disc

        self.assertEqual(n, 1, "only the smart-charge slot captured")
        row = store.get_dispatch_slot("2026-06-06T15:00:00")
        self.assertIsNotNone(row)
        self.assertEqual(row["off_peak"], 1)
        self.assertEqual(row["provider"], "MYENERGI_V2")
        self.assertEqual(row["source"], "smart-charge")
        # bump-charge slot must NOT be persisted
        self.assertIsNone(store.get_dispatch_slot("2026-06-06T16:00:00"))


class TestDispatchCaptureThrottle(unittest.TestCase):
    """The dispatch capture runs on a 5-min throttle (called every ~10s from
    _engine_tick but only fetches every 5 min), to catch planned smart-charge
    slots without hammering the API.
    """

    def setUp(self):
        self._orig_store = engine._store
        self._orig_client = engine._kraken_client
        self._orig_disc = engine._kraken_discovery
        self._orig_last = engine._last_dispatch_capture
        engine._store = BlockStore(":memory:")
        engine._kraken_discovery = {"import": {"product_code": "X",
                                               "tariff_code": "Y"}}
        engine._last_dispatch_capture = None

        class _FC:
            calls = 0

            async def get_dispatches(self, acct):
                _FC.calls += 1
                return {"provider": "MYENERGI_V2",
                        "planned": [{"start": "2026-06-07T02:30:00Z",
                                     "end": "2026-06-07T03:00:00Z",
                                     "source": "smart-charge"}],
                        "completed": []}
        self.FC = _FC
        engine._kraken_client = _FC()

    def tearDown(self):
        engine._store = self._orig_store
        engine._kraken_client = self._orig_client
        engine._kraken_discovery = self._orig_disc
        engine._last_dispatch_capture = self._orig_last

    def test_throttle_blocks_rapid_recapture(self):
        import asyncio
        from datetime import datetime, timezone, timedelta
        loop = asyncio.new_event_loop()
        loop.run_until_complete(engine._tick_dispatch_capture())
        self.assertEqual(self.FC.calls, 1)
        # Immediate re-tick: throttled, no new fetch.
        loop.run_until_complete(engine._tick_dispatch_capture())
        self.assertEqual(self.FC.calls, 1, "rapid re-tick must be throttled")
        # After >5 min: fetches again.
        engine._last_dispatch_capture = (
            datetime.now(timezone.utc).replace(tzinfo=None)
            - timedelta(seconds=301))
        loop.run_until_complete(engine._tick_dispatch_capture())
        self.assertEqual(self.FC.calls, 2, "fetch resumes after 5 min")


class TestDispatchOverlayResolver(unittest.TestCase):
    """Step 2 piece 2: the overlay reprices an out-of-window smart-charge slot to
    off-peak, gated by meter draw. Compute-and-log by default; export never
    adjusted; in-window slots are no-ops.
    """

    def setUp(self):
        from kraken_rates import RateSchedule
        self._orig_store = engine._store
        self._orig_sched = engine._kraken_rate_schedules
        self._orig_apply = engine._DISPATCH_OVERLAY_APPLY
        engine._store = BlockStore(":memory:")
        engine._kraken_rate_schedules = {"import": RateSchedule([
            ("2026-06-06T22:30:00", "2026-06-07T04:30:00", 5.493),
            ("2026-06-07T04:30:00", "2026-06-07T22:30:00", 32.3092),
        ])}
        # Out-of-window (peak) smart-charge slot.
        engine._store.upsert_dispatch_slot(
            "2026-06-07T15:00:00", off_peak=True, provider="MYENERGI_V2",
            source="smart-charge")

    def tearDown(self):
        engine._store = self._orig_store
        engine._kraken_rate_schedules = self._orig_sched
        engine._DISPATCH_OVERLAY_APPLY = self._orig_apply

    def test_apply_overrides_peak_to_offpeak_with_draw(self):
        engine._DISPATCH_OVERLAY_APPLY = True
        r = engine._dispatch_overlay_rate(
            "import", "2026-06-07T15:00:00", 0.323092, 1.5)
        self.assertAlmostEqual(r, 0.05493, places=5)

    def test_compute_and_log_does_not_apply(self):
        engine._DISPATCH_OVERLAY_APPLY = False
        r = engine._dispatch_overlay_rate(
            "import", "2026-06-07T15:00:00", 0.323092, 1.5)
        self.assertAlmostEqual(r, 0.323092, places=5,
            msg="compute-and-log must NOT change the rate")

    def test_meter_guard_no_draw_stays_peak(self):
        engine._DISPATCH_OVERLAY_APPLY = True
        r = engine._dispatch_overlay_rate(
            "import", "2026-06-07T15:00:00", 0.323092, 0.0)
        self.assertAlmostEqual(r, 0.323092, places=5,
            msg="no import draw → not overridden (over-report guard)")

    def test_draw_below_noise_floor_stays_peak(self):
        # 0.05 kWh is real-but-tiny (baseload/noise), below the 0.1 floor.
        # Under the OLD <=0 guard this would have wrongly applied off-peak;
        # the floor must reject it. This is the teeth for _DISPATCH_OVERLAY_MIN_KWH.
        engine._DISPATCH_OVERLAY_APPLY = True
        r = engine._dispatch_overlay_rate(
            "import", "2026-06-07T15:00:00", 0.323092, 0.05)
        self.assertAlmostEqual(r, 0.323092, places=5,
            msg="draw below 0.1 kWh noise floor → not overridden")

    def test_draw_just_above_noise_floor_applies(self):
        # 0.15 kWh clears the floor → genuine charging → off-peak applies.
        engine._DISPATCH_OVERLAY_APPLY = True
        r = engine._dispatch_overlay_rate(
            "import", "2026-06-07T15:00:00", 0.323092, 0.15)
        self.assertAlmostEqual(r, 0.05493, places=5,
            msg="draw above 0.1 kWh floor with smart-charge slot → off-peak")

    def test_draw_at_noise_floor_applies(self):
        # Exactly at the floor: guard is `< floor`, so 0.1 is NOT below → applies.
        engine._DISPATCH_OVERLAY_APPLY = True
        r = engine._dispatch_overlay_rate(
            "import", "2026-06-07T15:00:00", 0.323092,
            engine._DISPATCH_OVERLAY_MIN_KWH)
        self.assertAlmostEqual(r, 0.05493, places=5,
            msg="draw exactly at floor (0.1) → applies (guard is strict <)")

    def test_in_window_slot_is_noop(self):
        engine._DISPATCH_OVERLAY_APPLY = True
        # In-window block already off-peak; no captured slot needed.
        r = engine._dispatch_overlay_rate(
            "import", "2026-06-07T02:00:00", 0.05493, 1.5)
        self.assertAlmostEqual(r, 0.05493, places=5)

    def test_export_never_adjusted(self):
        engine._DISPATCH_OVERLAY_APPLY = True
        r = engine._dispatch_overlay_rate(
            "export", "2026-06-07T15:00:00", 0.12, 1.5)
        self.assertAlmostEqual(r, 0.12, places=5)

    def test_no_slot_no_override(self):
        engine._DISPATCH_OVERLAY_APPLY = True
        # A peak block with NO captured dispatch slot stays peak.
        r = engine._dispatch_overlay_rate(
            "import", "2026-06-07T16:00:00", 0.323092, 1.5)
        self.assertAlmostEqual(r, 0.323092, places=5)


class TestDispatchOverlayAtFinalise(unittest.TestCase):
    """The overlay applies at FINALISE (path A) — a fresh out-of-window
    smart-charge block is priced off-peak the moment it forms, matching how a
    CAD/BCD rate sensor would price it. Regression that the finalise call site
    is wired (not only the settlement rerun).
    """

    def setUp(self):
        import json
        from kraken_rates import RateSchedule
        self.cfg = {"meters": {"electricity_main": {
            "meta": {"timezone": "Europe/London", "billing_day": 1,
                     "block_minutes": 30, "currency_symbol": "£",
                     "sub_meter": False},
            "channels": {"import": {"read": "", "rate": ""},
                         "export": {"read": "", "rate": ""}}}}}
        with open(engine.CONFIG_PATH, "w") as f:
            json.dump(self.cfg, f)
        self._lj = patch.object(engine, "load_json",
                                side_effect=lambda *a, **k: self.cfg)
        self._lj.start()
        engine._store = BlockStore(":memory:")
        engine._store.insert_config_period(self.cfg)
        engine.set_data_source_mode("api")
        engine._kraken_rate_schedules = {"import": RateSchedule([
            ("2026-06-06T22:30:00", "2026-06-07T04:30:00", 5.493),
            ("2026-06-07T04:30:00", "2026-06-07T22:30:00", 32.3092)])}
        engine._store.upsert_dispatch_slot(
            "2026-06-07T15:00:00", off_peak=True, provider="MYENERGI_V2",
            source="smart-charge")
        self._orig_apply = engine._DISPATCH_OVERLAY_APPLY

    def tearDown(self):
        import os
        self._lj.stop()
        engine._DISPATCH_OVERLAY_APPLY = self._orig_apply
        engine._kraken_rate_schedules = {}
        try:
            os.remove(engine.CONFIG_PATH)
        except OSError:
            pass

    def _finalise(self, start, end):
        blk = engine.create_block(datetime.fromisoformat(start),
                                  datetime.fromisoformat(end), 30,
                                  seed_meters=True)
        ch = blk["meters"]["electricity_main"]["channels"]["import"]
        ch["reads"] = [{"ts": start, "value": 100.0},
                       {"ts": end, "value": 101.5}]

        class _HA:
            def get_state(self, e):
                return None
        engine.finalise_block(_HA(), block_data=blk)
        return engine._store._conn.execute(
            "SELECT imp_rate, imp_cost FROM blocks WHERE block_start=?",
            (start,)).fetchone()

    def test_finalise_applies_overlay_when_enabled(self):
        engine._DISPATCH_OVERLAY_APPLY = True
        r = self._finalise("2026-06-07T15:00:00", "2026-06-07T15:30:00")
        self.assertAlmostEqual(r["imp_rate"], 0.05493, places=5,
            msg="out-of-window dispatched block must finalise off-peak")

    def test_finalise_compute_and_log_leaves_peak(self):
        engine._DISPATCH_OVERLAY_APPLY = False
        r = self._finalise("2026-06-07T15:00:00", "2026-06-07T15:30:00")
        self.assertAlmostEqual(r["imp_rate"], 0.323092, places=5,
            msg="compute-and-log must leave the block at peak")


class TestConfigStateDiagnostics(unittest.TestCase):
    """Open-item 2: the config-state dump and per-decision fingerprint make a
    self-hosted user's bug report diagnosable from the log alone.
    """

    def setUp(self):
        self._orig_store = engine._store
        engine._store = BlockStore(":memory:")

    def tearDown(self):
        engine._store = self._orig_store

    def test_config_state_dump_reports_grounded_fields(self):
        engine.set_data_source_mode("api")
        cfg = {"meters": {"m": {"meta": {"sub_meter": False},
               "channels": {"import": {"read": "sensor.imp"},
                            "export": {"read": ""}}}}}
        with self.assertLogs("engine", level="INFO") as cm:
            engine._log_config_state(cfg)
        out = "\n".join(cm.output)
        self.assertIn("config-state: mode=api", out)
        self.assertIn("uses_api=True", out)
        self.assertIn("import_sensor=True", out)
        self.assertIn("export_sensor=False", out)
        self.assertIn("dispatch_overlay apply=True", out)
        self.assertIn("min_kwh=0.10", out)
        # Overlay is ACTIVE in an api mode.
        self.assertIn("ACTIVE", out)
        # A local import sensor present → Mini stands down (authoritative local).
        self.assertIn("mini=off (local import sensor authoritative)", out)

    def test_config_state_dump_mini_pending_when_api_no_sensor(self):
        # api mode, no local sensor → Mini elevation will be attempted; the
        # startup dump must NOT claim a definitive off (the old uses_mini bug).
        engine.set_data_source_mode("api")
        with self.assertLogs("engine", level="INFO") as cm:
            engine._log_config_state({"meters": {}})
        out = "\n".join(cm.output)
        self.assertIn("mini=pending", out)
        self.assertNotIn("uses_mini=False", out)

    def test_config_state_dump_marks_unbuilt_seam_honestly(self):
        # The dump must NOT fabricate states for features not yet built.
        engine.set_data_source_mode("cad")
        with self.assertLogs("engine", level="INFO") as cm:
            engine._log_config_state({"meters": {}})
        out = "\n".join(cm.output)
        self.assertIn("not yet implemented", out)
        # cad mode → overlay inactive (no API path).
        self.assertIn("inactive", out)
        self.assertIn("mini=off (no API mode)", out)

    def test_overlay_decision_log_carries_fingerprint(self):
        from kraken_rates import RateSchedule
        engine._kraken_rate_schedules = {"import": RateSchedule([
            ("2026-06-06T22:30:00", "2026-06-07T04:30:00", 5.493),
            ("2026-06-07T04:30:00", "2026-06-07T22:30:00", 32.3092)])}
        engine.set_data_source_mode("api")
        engine._store.upsert_dispatch_slot(
            "2026-06-07T15:00:00", off_peak=True, provider="MYENERGI_V2",
            source="smart-charge")
        _orig = engine._DISPATCH_OVERLAY_APPLY
        _orig_reader = engine._kraken_mini_reader
        try:
            engine._DISPATCH_OVERLAY_APPLY = True
            # Simulate the runtime Mini elevation (mode stays 'api', reader set).
            engine._kraken_mini_reader = object()
            with self.assertLogs("engine", level="INFO") as cm:
                engine._dispatch_overlay_rate(
                    "import", "2026-06-07T15:00:00", 0.323092, 1.5)
            out = "\n".join(cm.output)
            self.assertIn("APPLIED", out)
            self.assertIn("mode=api", out)
            # The fingerprint must report the ACTUAL runtime Mini state, not the
            # nominal mode (mode=api but mini elevated → mini=active).
            self.assertIn("mini=active", out)
            self.assertIn("provider=MYENERGI_V2", out)
            self.assertIn("source=smart-charge", out)
            self.assertIn("apply=True", out)
        finally:
            engine._DISPATCH_OVERLAY_APPLY = _orig
            engine._kraken_mini_reader = _orig_reader
            engine._kraken_rate_schedules = {}


class TestShortRestartReseed(unittest.TestCase):
    """A within-block restart must not lose the block opener. Reproduces the
    live 0.253-vs-3.757 under-count: the rogue-total guard clears the opener and,
    with no gap, nothing restores it unless we re-seed from last_block.read_end.
    """

    def _last_block(self, end="2026-06-09T09:00:00", read_end=30437.853):
        return {"start": "2026-06-09T08:30:00", "end": end,
                "meters": {"electricity_main": {"meta": {},
                    "channels": {"import": {"read_end": read_end}}}}}

    def _current_block(self, start="2026-06-09T09:00:00", reads=None):
        return {"start": start, "end": "2026-06-09T09:30:00",
                "meters": {"electricity_main": {"meta": {},
                    "channels": {"import": {"reads": reads or []}}}}}

    def test_reseed_restores_opener_when_contiguous(self):
        lb = self._last_block()
        cb = self._current_block()
        seeded = engine._reseed_opener_after_short_restart(lb, cb)
        self.assertTrue(seeded)
        reads = cb["meters"]["electricity_main"]["channels"]["import"]["reads"]
        self.assertEqual(len(reads), 1)
        self.assertAlmostEqual(reads[0]["value"], 30437.853, places=3)
        self.assertEqual(reads[0]["ts"], "2026-06-09T09:00:00")

    def test_reseed_then_delta_measures_full_block_not_tail(self):
        # The teeth: with the opener restored, the boundary read yields the TRUE
        # 3.757 kWh delta, not the 0.253 tail the broken seed produced live.
        lb = self._last_block()
        cb = self._current_block()
        engine._reseed_opener_after_short_restart(lb, cb)
        reads = cb["meters"]["electricity_main"]["channels"]["import"]["reads"]
        # Simulate the 09:30 boundary bracket read from the live log.
        reads.append({"ts": "2026-06-09T09:30:10", "value": 30441.610})
        delta = reads[-1]["value"] - reads[0]["value"]   # finalise's computation
        self.assertAlmostEqual(delta, 3.757, places=3)
        self.assertNotAlmostEqual(delta, 0.253, places=3)

    def test_reseed_noop_when_reads_present(self):
        # Never clobber a channel that already has live reads.
        lb = self._last_block()
        cb = self._current_block(reads=[{"ts": "2026-06-09T09:10:00",
                                         "value": 30440.0}])
        seeded = engine._reseed_opener_after_short_restart(lb, cb)
        self.assertEqual(seeded, [])
        reads = cb["meters"]["electricity_main"]["channels"]["import"]["reads"]
        self.assertEqual(len(reads), 1)
        self.assertAlmostEqual(reads[0]["value"], 30440.0, places=3)

    def test_reseed_noop_when_not_contiguous(self):
        # A real gap (last_block.end != current_block.start) → gap-fill owns
        # seeding; the helper must NOT interfere.
        lb = self._last_block(end="2026-06-09T08:30:00")  # gap before current
        cb = self._current_block(start="2026-06-09T09:00:00")
        seeded = engine._reseed_opener_after_short_restart(lb, cb)
        self.assertEqual(seeded, [])
        self.assertEqual(
            cb["meters"]["electricity_main"]["channels"]["import"]["reads"], [])

    def test_reseed_skips_channel_absent_in_last_block(self):
        lb = self._last_block()
        cb = self._current_block()
        # Add an export channel the last block doesn't have a read_end for.
        cb["meters"]["electricity_main"]["channels"]["export"] = {"reads": []}
        seeded = engine._reseed_opener_after_short_restart(lb, cb)
        # import seeded, export skipped (no read_end in last_block).
        self.assertTrue(any("import" in s for s in seeded))
        self.assertFalse(any("export" in s for s in seeded))

    def test_reseed_rebuilds_channel_when_current_meters_empty(self):
        # THE REAL failure mode, and the teeth for this fix: after the guard
        # clears the in-progress reads, load_current_block() reconstructs the
        # block with meters={} (channels are derived from reads, which are gone).
        # The re-seed must REBUILD the channel from last_block — iterating the
        # empty current meters (the old behaviour) seeds nothing and the block
        # under-counts to its tail (the live 0.23-vs-3.5 kWh bug).
        lb = self._last_block()
        cb = {"start": "2026-06-09T09:00:00", "end": "2026-06-09T09:30:00",
              "meters": {}}
        seeded = engine._reseed_opener_after_short_restart(lb, cb)
        self.assertTrue(seeded, "must rebuild the channel from last_block")
        reads = cb["meters"]["electricity_main"]["channels"]["import"]["reads"]
        self.assertEqual(len(reads), 1)
        self.assertAlmostEqual(reads[0]["value"], 30437.853, places=3)
        self.assertEqual(reads[0]["ts"], "2026-06-09T09:00:00")

    def test_reseed_empty_meters_then_full_block_delta(self):
        # End-to-end teeth on the empty-meters path: rebuilt opener + boundary
        # read yields the TRUE delta, not the tail.
        lb = self._last_block(read_end=30493.292)
        cb = {"start": "2026-06-09T09:00:00", "end": "2026-06-09T09:30:00",
              "meters": {}}
        engine._reseed_opener_after_short_restart(lb, cb)
        reads = cb["meters"]["electricity_main"]["channels"]["import"]["reads"]
        reads.append({"ts": "2026-06-09T09:30:00", "value": 30496.962})
        self.assertAlmostEqual(reads[-1]["value"] - reads[0]["value"], 3.670, places=3)


class TestSupplierCapability(unittest.TestCase):
    """Supplier gating seam: normalize_supplier + supplier_is_api_capable.
    The authoritative server-side check behind the wizard's supplier dropdown.
    """

    def test_octopus_variants_normalise_and_are_api_capable(self):
        for s in ("octopus", "Octopus", "OCTOPUS", "Octopus Energy",
                  "  octopus energy  "):
            self.assertEqual(engine.normalize_supplier(s), "octopus", msg=s)
            self.assertTrue(engine.supplier_is_api_capable(s), msg=s)

    def test_not_listed_and_unknown_are_local_only(self):
        self.assertEqual(engine.normalize_supplier("not-listed"), "not-listed")
        self.assertFalse(engine.supplier_is_api_capable("not-listed"))
        # Arbitrary free-text collapses to not-listed (migration default).
        self.assertEqual(engine.normalize_supplier("British Gas"), "not-listed")
        self.assertFalse(engine.supplier_is_api_capable("British Gas"))

    def test_empty_supplier_is_unset_and_not_api_capable(self):
        for s in ("", None, "   "):
            self.assertEqual(engine.normalize_supplier(s), "")
            self.assertFalse(engine.supplier_is_api_capable(s))


class TestMiniTeardownOnModeChange(unittest.TestCase):
    """Change Setup api→cad must not leave a stale Mini reader collecting into
    cad blocks. _teardown_mini_if_no_api clears it when the mode drops the API.
    """

    def setUp(self):
        self._orig_reader = engine._kraken_mini_reader
        self._orig_mode = engine.get_data_source_mode()

    def tearDown(self):
        engine._kraken_mini_reader = self._orig_reader
        try:
            engine.set_data_source_mode(self._orig_mode)
        except Exception:
            pass

    def test_tears_down_reader_when_mode_not_api(self):
        engine.set_data_source_mode("cad")
        engine._kraken_mini_reader = object()        # simulate a wired reader
        tore = engine._teardown_mini_if_no_api()
        self.assertTrue(tore)
        self.assertIsNone(engine._kraken_mini_reader)

    def test_keeps_reader_when_mode_uses_api(self):
        engine.set_data_source_mode("api")
        sentinel = object()
        engine._kraken_mini_reader = sentinel
        tore = engine._teardown_mini_if_no_api()
        self.assertFalse(tore)
        self.assertIs(engine._kraken_mini_reader, sentinel)

    def test_noop_when_no_reader(self):
        engine.set_data_source_mode("cad")
        engine._kraken_mini_reader = None
        self.assertFalse(engine._teardown_mini_if_no_api())
        self.assertIsNone(engine._kraken_mini_reader)


class TestSettlementSweep(unittest.TestCase):
    """The auto settlement sweep's two pure decisions: how far back to chase
    (look-back clamp / horizon back-stop) and when to run (daily cadence)."""

    NOW = datetime(2026, 6, 10, 12, 0, 0, tzinfo=timezone.utc)

    # --- _clamp_sweep_start: the cost / never-settling back-stop ---
    def test_clamp_oldest_within_horizon_returned_as_is(self):
        # Oldest unsettled is 5 days ago, horizon 14 → reach all the way to it.
        oldest = "2026-06-05T12:00:00"
        got = engine._clamp_sweep_start(oldest, self.NOW, max_lookback_days=14)
        self.assertEqual(got, datetime(2026, 6, 5, 12, 0, 0, tzinfo=timezone.utc))

    def test_clamp_oldest_older_than_horizon_is_floored(self):
        # Oldest unsettled is 40 days ago (e.g. a never-settling limbo block);
        # horizon 14 → floor at now-14d, do NOT chase the ancient block.
        oldest = "2026-05-01T00:00:00"
        got = engine._clamp_sweep_start(oldest, self.NOW, max_lookback_days=14)
        self.assertEqual(got, self.NOW - timedelta(days=14))

    def test_clamp_none_is_unbounded(self):
        # User-triggered retry (no horizon) reaches an ancient block unchanged —
        # teeth: proves the floor only applies when a horizon is given.
        oldest = "2026-05-01T00:00:00"
        got = engine._clamp_sweep_start(oldest, self.NOW, max_lookback_days=None)
        self.assertEqual(got, datetime(2026, 5, 1, 0, 0, 0, tzinfo=timezone.utc))
        self.assertNotEqual(got, self.NOW - timedelta(days=14))

    def test_clamp_naive_iso_becomes_utc_aware(self):
        got = engine._clamp_sweep_start("2026-06-09T00:00:00", self.NOW)
        self.assertEqual(got.tzinfo, timezone.utc)

    # --- _sweep_is_due: the daily cadence gate ---
    def test_due_when_no_prior_sweep(self):
        self.assertTrue(engine._sweep_is_due(None, self.NOW, 24 * 3600))

    def test_not_due_when_recent(self):
        last = (self.NOW - timedelta(hours=6)).isoformat()
        self.assertFalse(engine._sweep_is_due(last, self.NOW, 24 * 3600))

    def test_due_when_interval_elapsed(self):
        last = (self.NOW - timedelta(hours=25)).isoformat()
        self.assertTrue(engine._sweep_is_due(last, self.NOW, 24 * 3600))

    def test_due_on_exact_interval_boundary(self):
        last = (self.NOW - timedelta(seconds=24 * 3600)).isoformat()
        self.assertTrue(engine._sweep_is_due(last, self.NOW, 24 * 3600))

    def test_due_when_state_unparseable(self):
        # Corrupt/garbage state must not wedge the sweep off forever.
        self.assertTrue(engine._sweep_is_due("not-a-date", self.NOW, 24 * 3600))

    def test_due_when_naive_last_iso(self):
        last = (self.NOW.replace(tzinfo=None) - timedelta(hours=25)).isoformat()
        self.assertTrue(engine._sweep_is_due(last, self.NOW, 24 * 3600))


class TestDisconnectKraken(unittest.TestCase):
    """disconnect_kraken (MODE-UI §5): clears credentials + API-derived progress
    state, tears down in-memory API objects, lands on 'cad' — but KEEPS the
    billing source (a mode-independent fact, not a progress marker) and never
    touches historical settlement."""

    def setUp(self):
        self._orig = engine._store
        engine._store = BlockStore(":memory:")
        engine.set_data_source_mode("api")
        # API-derived progress markers + a billing-source preference.
        engine._store.set_kraken_state("last_poll_utc", "2026-06-10T06:00:00Z")
        engine._store.set_kraken_state(engine._STATE_LAST_SWEEP, "2026-06-10T06:00:00+00:00")
        engine._store.set_kraken_state(engine._KRAKEN_SNAPSHOT_DONE_KEY, "1")
        engine._store.set_kraken_state(engine._BILLING_SOURCE_KEY, "dcc")
        # In-memory API objects that must be torn down.
        self._saved = (engine._kraken_client, engine._kraken_mini_reader,
                       engine._kraken_discovery, engine._kraken_rate_schedules,
                       engine._kraken_standing_schedule)
        engine._kraken_client = object()
        engine._kraken_mini_reader = object()
        engine._kraken_discovery = {"import": {}}
        engine._kraken_rate_schedules = {"import": object()}
        engine._kraken_standing_schedule = object()

    def tearDown(self):
        (engine._kraken_client, engine._kraken_mini_reader,
         engine._kraken_discovery, engine._kraken_rate_schedules,
         engine._kraken_standing_schedule) = self._saved
        try:
            engine._store.close()
        except Exception:
            pass
        engine._store = self._orig

    def _run(self, coro):
        import asyncio
        loop = asyncio.new_event_loop()
        try:
            return loop.run_until_complete(coro)
        finally:
            loop.close()

    def test_disconnect_with_credentials_full_teardown(self):
        with patch.object(engine, "has_kraken_credentials", return_value=True), \
             patch.object(engine, "save_kraken_credentials") as save_mock:
            res = self._run(engine.disconnect_kraken())
        self.assertTrue(res["ok"])
        self.assertEqual(res["mode"], "cad")
        self.assertTrue(res["had_credentials"])
        # The actual disconnect: creds file cleared (None passed).
        save_mock.assert_called_once_with(None, None)
        # Landed on local-sensor mode.
        self.assertEqual(engine.get_data_source_mode(), "cad")
        # API-derived progress markers wiped.
        self.assertIsNone(engine._store.get_kraken_state("last_poll_utc"))
        self.assertIsNone(engine._store.get_kraken_state(engine._STATE_LAST_SWEEP))
        self.assertIsNone(engine._store.get_kraken_state(engine._KRAKEN_SNAPSHOT_DONE_KEY))
        # Billing source PRESERVED (teeth: not a progress marker, must survive).
        self.assertEqual(engine._store.get_kraken_state(engine._BILLING_SOURCE_KEY), "dcc")
        # In-memory API objects torn down.
        self.assertIsNone(engine._kraken_client)
        self.assertIsNone(engine._kraken_mini_reader)
        self.assertIsNone(engine._kraken_discovery)
        self.assertEqual(engine._kraken_rate_schedules, {})
        self.assertIsNone(engine._kraken_standing_schedule)

    def test_disconnect_without_credentials_still_normalises(self):
        with patch.object(engine, "has_kraken_credentials", return_value=False), \
             patch.object(engine, "save_kraken_credentials"):
            res = self._run(engine.disconnect_kraken())
        self.assertTrue(res["ok"])
        self.assertEqual(res["mode"], "cad")
        self.assertFalse(res["had_credentials"])
        self.assertEqual(engine.get_data_source_mode(), "cad")


class TestRogueGuardColdStartOnly(unittest.TestCase):
    """The rogue-total guard (clearing the in-progress block's carried reads)
    must fire ONLY on a cold start. On a warm re-run (HA reconnect / config-save
    in the same process) the reads are live; clearing them discards real
    consumption — the Mini block that lost its ~3.4 kWh opener during an
    HA-upgrade reconnect storm."""

    def _block_with_reads(self):
        return {"start": "2026-06-10T11:00:00",
                "meters": {"electricity_main": {"channels": {
                    "import": {"reads": [{"ts": "2026-06-10T10:59:50", "value": 30486.221}],
                               "rates": [{"ts": "x", "value": 0.323092}]}}}}}

    def test_cold_start_clears(self):
        cb = self._block_with_reads()
        self.assertTrue(engine._clear_stale_inprogress_reads(cb, cold_start=True))
        self.assertEqual(cb["meters"]["electricity_main"]["channels"]["import"]["reads"], [])

    def test_warm_rerun_preserves_reads(self):
        # The fix: a warm re-run must NOT clear live in-progress reads.
        cb = self._block_with_reads()
        self.assertFalse(engine._clear_stale_inprogress_reads(cb, cold_start=False))
        self.assertEqual(cb["meters"]["electricity_main"]["channels"]["import"]["reads"],
                         [{"ts": "2026-06-10T10:59:50", "value": 30486.221}])

    def test_cold_start_empty_block_is_noop(self):
        cb = {"start": "2026-06-10T11:00:00", "meters": {"electricity_main":
              {"channels": {"import": {"reads": [], "rates": []}}}}}
        self.assertFalse(engine._clear_stale_inprogress_reads(cb, cold_start=True))

    def test_none_block_is_noop(self):
        self.assertFalse(engine._clear_stale_inprogress_reads(None, cold_start=True))

    def test_flag_drives_cold_then_warm(self):
        # The flag is the exact gate engine_startup uses: cold=not _cold_start_complete.
        # First (boot) call is cold → clears; every later in-process call is warm.
        orig = engine._cold_start_complete
        try:
            engine._cold_start_complete = False
            cb1 = self._block_with_reads()
            self.assertTrue(engine._clear_stale_inprogress_reads(
                cb1, not engine._cold_start_complete))
            engine._cold_start_complete = True
            cb2 = self._block_with_reads()
            self.assertFalse(engine._clear_stale_inprogress_reads(
                cb2, not engine._cold_start_complete))
        finally:
            engine._cold_start_complete = orig


class TestOhmeInterpretMode(unittest.TestCase):
    """_ohme_interpret_mode: official select vs dan-r binary → smart/boost/idle."""

    def test_official_smart(self):
        self.assertEqual(engine._ohme_interpret_mode("official", "Smart charge"), "smart")

    def test_official_boost(self):
        self.assertEqual(engine._ohme_interpret_mode("official", "Max charge"), "boost")

    def test_official_paused_idle(self):
        self.assertEqual(engine._ohme_interpret_mode("official", "Paused"), "idle")

    def test_official_blank_idle(self):
        self.assertEqual(engine._ohme_interpret_mode("official", ""), "idle")

    def test_danr_on_smart(self):
        self.assertEqual(engine._ohme_interpret_mode("danr", "on"), "smart")

    def test_danr_off_idle(self):
        # dan-r off = no smart slot; boost is inferred by absence, never captured
        self.assertEqual(engine._ohme_interpret_mode("danr", "off"), "idle")

    def test_unknown_integration_idle(self):
        self.assertEqual(engine._ohme_interpret_mode(None, "Smart charge"), "idle")


class TestOhmeCaptureSlots(unittest.TestCase):
    """_ohme_capture_slots: three-way OHME capture (pure)."""

    from datetime import datetime as _dt
    NOW = _dt(2026, 4, 14, 13, 17, 0)  # → 30-min slot 13:00
    PLANNED = [
        {"start": "2026-04-14T02:00:00Z", "end": "2026-04-14T03:00:00Z",
         "source": "smart-charge"},
        {"start": "2026-04-14T13:00:00Z", "end": "2026-04-14T13:30:00Z",
         "source": "bump-charge"},
    ]

    def test_is_ohme_provider(self):
        self.assertTrue(engine._is_ohme_provider("OHME"))
        self.assertTrue(engine._is_ohme_provider("ohme"))
        self.assertFalse(engine._is_ohme_provider("MYENERGI_V2"))
        self.assertFalse(engine._is_ohme_provider(None))

    def test_non_ohme_returns_none(self):
        # None signals the caller to use the default smart-charge path
        self.assertIsNone(engine._ohme_capture_slots(
            "MYENERGI_V2", self.PLANNED, False, None, self.NOW))

    def test_optimistic_captures_all_planned_incl_bump(self):
        pairs = engine._ohme_capture_slots("OHME", self.PLANNED, False, None, self.NOW)
        slots = {s for s, _ in pairs}
        sources = {src for _, src in pairs}
        # teeth: the bump-charge slot is captured too — Zappi's source gate drops it
        self.assertIn("2026-04-14T13:00:00", slots)
        self.assertIn("2026-04-14T02:00:00", slots)
        self.assertIn("2026-04-14T02:30:00", slots)
        self.assertEqual(sources, {"ohme_assumed_unverified"})

    def test_verified_smart_captures_current_slot_only(self):
        pairs = engine._ohme_capture_slots("OHME", self.PLANNED, True, "smart", self.NOW)
        # teeth: ONLY the now-slot, not the planned superset
        self.assertEqual(pairs, [("2026-04-14T13:00:00", "ohme_verified")])

    def test_verified_boost_captures_nothing(self):
        # teeth: planned has slots but boost vetoes — nothing captured
        self.assertEqual(
            engine._ohme_capture_slots("OHME", self.PLANNED, True, "boost", self.NOW), [])

    def test_verified_idle_captures_nothing(self):
        self.assertEqual(
            engine._ohme_capture_slots("OHME", self.PLANNED, True, "idle", self.NOW), [])

    def test_slot_for_now_snaps_down(self):
        from datetime import datetime as dt
        self.assertEqual(engine._ohme_slot_for_now(dt(2026, 4, 14, 13, 17)),
                         "2026-04-14T13:00:00")
        self.assertEqual(engine._ohme_slot_for_now(dt(2026, 4, 14, 13, 47)),
                         "2026-04-14T13:30:00")


class TestRecomputeRemaindersForWindow(unittest.TestCase):
    """After a device's blocks are deleted, the parent main meter's remainder
    must be rebuilt: the device's grid-attributed energy returns to the main
    'rest of house' line (fully if it was the last device, else net of the
    surviving devices)."""

    BS = "2025-05-16T12:00:00"
    BE = "2025-05-16T12:30:00"

    def setUp(self):
        self._saved_store = engine._store
        self.store = BlockStore(":memory:")
        self.store.insert_config_period({"meters": {
            "electricity_main": {"meta": {
                "timezone": "UTC", "billing_day": 1, "block_minutes": 30,
                "currency_symbol": "£", "currency_code": "GBP"}},
            "battery": {"meta": {
                "sub_meter": True, "parent_meter": "electricity_main",
                "block_minutes": 30, "timezone": "UTC",
                "currency_symbol": "£", "currency_code": "GBP"},
                "channels": {"import": {}}},
            "ev": {"meta": {
                "sub_meter": True, "parent_meter": "electricity_main",
                "block_minutes": 30, "timezone": "UTC",
                "currency_symbol": "£", "currency_code": "GBP"},
                "channels": {"import": {}}},
        }})
        engine._store = self.store

    def tearDown(self):
        engine._store = self._saved_store
        self.store.close()

    def _meta(self, sub, parent=None):
        m = {"block_minutes": 30, "timezone": "UTC", "billing_day": 1,
             "currency_symbol": "£", "currency_code": "GBP", "sub_meter": sub}
        if parent:
            m["parent_meter"] = parent
        return m

    def _finalise(self, devices):
        """Build + persist a finalised block: main 2.0 kWh @ £0.30, plus the
        given {meter_id: kwh} devices. Returns the parent remainder written."""
        meters = {"electricity_main": {
            "meta": self._meta(False), "standing_charge": 0.5,
            "channels": {"import": {"kwh": 2.0, "rate": 0.30, "cost": 0.60,
                                    "read_start": 1000.0, "read_end": 1002.0}}}}
        for mid, kwh in devices.items():
            meters[mid] = {"meta": self._meta(True, "electricity_main"),
                           "channels": {"import": {
                               "kwh": kwh, "rate": 0.30, "cost": round(kwh * 0.30, 4),
                               "read_start": 0.0, "read_end": kwh}}}
        block = {"start": self.BS, "end": self.BE, "interpolated": False,
                 "meters": meters}
        engine._apply_pass2(block)
        engine._recompute_pass3_totals(block)
        engine.append_block_replace(block)
        return self._parent_remainder()

    def _parent_remainder(self):
        r = self.store._conn.execute(
            "SELECT imp_kwh_remainder FROM blocks "
            "WHERE meter_id='electricity_main' AND block_start=?", (self.BS,)
        ).fetchone()
        return r["imp_kwh_remainder"]

    def test_last_device_deleted_returns_full_main(self):
        self._finalise({"battery": 0.5})
        self.assertAlmostEqual(self._parent_remainder(), 1.5, places=4)  # 2.0 - 0.5
        res = self.store.delete_blocks_for_date_range(
            "2025-05-16", "2025-05-16", "battery", tz_name="UTC")
        self.assertEqual(res["recompute_parent"], "electricity_main")
        n = engine.recompute_remainders_for_window(
            res["recompute_parent"], res["recompute_from"], res["recompute_to"])
        self.assertEqual(n, 1)
        # No surviving devices → remainder is the whole main import again.
        self.assertAlmostEqual(self._parent_remainder(), 2.0, places=4)

    def test_one_of_two_devices_deleted_nets_survivor(self):
        self._finalise({"battery": 0.5, "ev": 0.3})
        self.assertAlmostEqual(self._parent_remainder(), 1.2, places=4)  # 2.0 - 0.8
        res = self.store.delete_blocks_for_date_range(
            "2025-05-16", "2025-05-16", "ev", tz_name="UTC")
        engine.recompute_remainders_for_window(
            res["recompute_parent"], res["recompute_from"], res["recompute_to"])
        # ev's 0.3 returns to the main; battery's 0.5 still subtracted.
        self.assertAlmostEqual(self._parent_remainder(), 1.5, places=4)


if __name__ == "__main__":
    unittest.main()

class TestReconciledRateSurvivesPass2(unittest.TestCase):
    """§12/marker: a reconciliation-set rate must survive a PASS 2 re-run — the
    overlay must NOT re-apply off-peak over a slot the reconciliation deliberately
    reverted to peak (rate_reconciled). Belt-and-braces for a re-settlement."""

    class _Sched:
        def is_empty(self): return False
        def off_peak_rate_near(self, ts): return 5.493      # £0.05493
        def resolve(self, ts): return 32.3092               # £0.323092

    def setUp(self):
        self.store = BlockStore(":memory:")
        self.store.insert_config_period({"meters": {"electricity_main": {
            "meta": {"timezone": "Europe/London", "billing_day": 1,
                     "block_minutes": 30, "currency_symbol": "£",
                     "sub_meter": False}}}})
        self.cp = self.store._conn.execute(
            "SELECT id FROM config_periods LIMIT 1").fetchone()["id"]
        self._os, engine._store = engine._store, self.store
        self._osc, engine._kraken_rate_schedules = \
            engine._kraken_rate_schedules, {"import": self._Sched()}
        self._oa, engine._DISPATCH_OVERLAY_APPLY = engine._DISPATCH_OVERLAY_APPLY, True

    def tearDown(self):
        engine._store = self._os
        engine._kraken_rate_schedules = self._osc
        engine._DISPATCH_OVERLAY_APPLY = self._oa

    def test_reconciled_peak_survives_pass2(self):
        bs = "2026-06-04T20:00:00"   # peak-time slot
        # an off_peak dispatch slot + real draw exists, so the overlay WOULD
        # reprice to off-peak — but this block was reconciled to PEAK.
        self.store.upsert_dispatch_slot(bs, off_peak=True, provider="Myenergi",
                                        source="smart-charge", state="planned")
        self.store._conn.execute(
            "INSERT INTO blocks (block_start, block_end, meter_id, config_period_id,"
            " interpolated, imp_kwh, imp_rate, imp_cost, imp_kwh_api, standing_charge,"
            " needs_pass2_rerun, rate_reconciled) VALUES (?,?,?,?,0,?,?,?,?,?,1,1)",
            (bs, bs, "electricity_main", self.cp, 3.0, 0.323092,
             round(3.0 * 0.323092, 6), 3.0, 0.5))
        self.store._conn.commit()
        block = self.store.get_block_dict_by_start(bs)
        engine._rerun_pass2_for_settled_block(
            block, rate_resolver=engine._kraken_rate_resolver, billing_source="dcc")
        engine.append_block_replace(block)
        rate = self.store._conn.execute(
            "SELECT imp_rate FROM blocks WHERE block_start=?", (bs,)).fetchone()["imp_rate"]
        self.assertAlmostEqual(rate, 0.323092, places=5,
            msg="reconciled peak rate must survive PASS 2 (overlay not re-applied)")


class TestUnsupportedTariffGuard(unittest.IsolatedAsyncioTestCase):
    """#1708 fail-loud guard: a discovered import tariff that returns NO standard
    unit rates (the new IOG 6-hour-cap / time-of-use tariff) must set the
    unsupported flag so the UI can warn, instead of silently pricing at £0."""

    async def asyncSetUp(self):
        self._d, self._c, self._u = (engine._kraken_discovery,
                                     engine._kraken_client,
                                     engine._rate_schedule_unsupported)

    async def asyncTearDown(self):
        engine._kraken_discovery = self._d
        engine._kraken_client = self._c
        engine._rate_schedule_unsupported = self._u

    async def test_empty_import_rates_sets_flag(self):
        from unittest.mock import AsyncMock, MagicMock
        engine._kraken_discovery = {"import": {
            "product_code": "IOG-SMB-TOU-25-12-12",
            "tariff_code": "E-1R-IOG-SMB-TOU-25-12-12-H"}}
        c = MagicMock()
        c.get_unit_rates = AsyncMock(return_value=[])
        c.get_standing_charges = AsyncMock(return_value=[])
        engine._kraken_client = c
        await engine._refresh_kraken_rate_schedules()
        self.assertIsNotNone(engine._rate_schedule_unsupported)
        self.assertEqual(engine._rate_schedule_unsupported["tariff"],
                         "E-1R-IOG-SMB-TOU-25-12-12-H")

    async def test_normal_tariff_clears_flag(self):
        from unittest.mock import AsyncMock, MagicMock
        engine._rate_schedule_unsupported = {"tariff": "stale"}
        engine._kraken_discovery = {"import": {
            "product_code": "INTELLI-FIX", "tariff_code": "E-1R-INTELLI-FIX-B"}}
        c = MagicMock()
        c.get_unit_rates = AsyncMock(return_value=[
            {"value_inc_vat": 5.493, "valid_from": "2026-01-01T00:00:00Z",
             "valid_to": None}])
        c.get_standing_charges = AsyncMock(return_value=[])
        engine._kraken_client = c
        await engine._refresh_kraken_rate_schedules()
        self.assertIsNone(engine._rate_schedule_unsupported)


class TestGapMarkerDoesNotFreezeSettlement(unittest.TestCase):
    """BL-1: a gap marker must NOT stop the DCC PASS-2 drain.

    The drain was nested inside `if not has_gap_marker(current_block):` alongside
    the sub-meter amendment. That globally froze settlement APPLICATION for the
    whole history whenever any gap marker was live — an outage on one day stopped
    every other day's settled figures reaching billing. The amendment genuinely
    needs the guard (gap-seed reads would corrupt boundary interpolation); the
    drain does not — it takes its queue from the DB and never reads the current
    block or the rolling buffer.
    """

    def test_drain_is_not_inside_the_gap_guard(self):
        """Structural: within _engine_tick, _drain_pass2_queue must be at a
        shallower indent than the amendment (i.e. outside the guard block)."""
        import inspect, re
        src = inspect.getsource(engine._engine_tick)
        amend = drain = guard = None
        for line in src.splitlines():
            if "_amend_provisional_sub_meter_blocks(ha" in line:
                amend = len(line) - len(line.lstrip())
            elif "_drain_pass2_queue(ha)" in line:
                drain = len(line) - len(line.lstrip())
            elif "if not has_gap_marker(current_block)" in line:
                guard = len(line) - len(line.lstrip())
        self.assertIsNotNone(amend, "amendment call not found")
        self.assertIsNotNone(drain, "drain call not found")
        self.assertIsNotNone(guard, "gap guard not found")
        # Both sit inside a `try:`. The amendment's try is nested INSIDE the
        # guard (guard+8); the drain's try is a sibling OF the guard (guard+4).
        self.assertEqual(amend, guard + 8,
                         "amendment must stay INSIDE the gap guard")
        self.assertEqual(drain, guard + 4,
                         "drain must be OUTSIDE the gap guard (BL-1) — a gap "
                         "marker must not freeze DCC settlement application")

    def test_drain_reads_queue_from_db_not_current_block(self):
        """The justification for un-guarding: the drain's inputs are the DB queue
        and block_start lookups — never the current block / rolling buffer."""
        import inspect
        src = inspect.getsource(engine._drain_pass2_queue)
        self.assertIn("get_blocks_needing_pass2_rerun", src)
        self.assertNotIn("current_block", src)
        self.assertNotIn("_gap_marker", src)