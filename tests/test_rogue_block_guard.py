"""
test_rogue_block_guard.py
=========================
Regression guard for the rogue full-register block.

Incident (2026-07): during device add/delete config churn the main meter's
opening register was momentarily lost/zeroed while the closing read still held
the true cumulative register (~30961), so a single 30-minute block booked the
ENTIRE lifetime register as one interval's import — inflating the month from
~676 kWh to ~31637 kWh and the bill to five figures.

compute_channel now clamps any main-meter block whose delta exceeds a
physically-impossible ceiling, collapsing read_start onto read_end so the total
isn't inflated and the next block still opens at the correct register.

Reuses test_engine.py's stub harness so engine imports without HA/filesystem.

Run with:  python3 -m unittest test_rogue_block_guard -v
"""

import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import test_engine  # noqa: F401  — installs stubs, imports engine
import engine


class TestRogueBlockGuard(unittest.TestCase):

    def _channel(self, opener, closer, rate=0.30):
        return {
            "reads": [
                {"value": opener, "ts": "2026-07-01T21:30:00"},
                {"value": closer, "ts": "2026-07-01T22:00:00"},
            ],
            "rates": [{"value": rate, "ts": "2026-07-01T21:30:00"}],
        }

    def test_zeroed_opener_full_register_is_clamped(self):
        """The exact incident: opener lost to 0, closer at the real register."""
        r = engine.compute_channel(
            self._channel(0.0, 30961.065),
            is_sub_meter=False, meter_id="electricity_main", channel_id="import")
        self.assertEqual(r["kwh"], 0.0)                 # not 30961
        self.assertEqual(r["cost"], 0.0)
        self.assertEqual(r["read_start"], r["read_end"])  # collapsed — can't re-inflate
        self.assertEqual(r["read_end"], 30961.065)        # continuity preserved
        self.assertTrue(r.get("needs_review"))

    def test_normal_block_unaffected(self):
        """A real ~6 kWh half-hour is booked exactly as before."""
        r = engine.compute_channel(
            self._channel(30955.0, 30961.065),
            is_sub_meter=False, meter_id="electricity_main", channel_id="import")
        self.assertAlmostEqual(r["kwh"], 6.065, places=3)
        self.assertAlmostEqual(r["read_start"], 30955.0)
        self.assertAlmostEqual(r["read_end"], 30961.065)
        self.assertFalse(r.get("needs_review", False))

    def test_just_under_ceiling_not_clamped(self):
        """Guard must not fire below the ceiling — no false positives on usage."""
        r = engine.compute_channel(
            self._channel(0.0, 499.0),
            is_sub_meter=False, meter_id="m", channel_id="import")
        self.assertAlmostEqual(r["kwh"], 499.0)
        self.assertFalse(r.get("needs_review", False))

    def test_just_over_ceiling_is_clamped(self):
        r = engine.compute_channel(
            self._channel(0.0, 501.0),
            is_sub_meter=False, meter_id="m", channel_id="import")
        self.assertEqual(r["kwh"], 0.0)
        self.assertTrue(r.get("needs_review"))

    def test_negative_delta_still_zero(self):
        """A register that reads lower than the opener (reset) stays 0 as before —
        the new guard doesn't disturb the existing max(delta,0) behaviour."""
        r = engine.compute_channel(
            self._channel(30961.0, 30900.0),
            is_sub_meter=False, meter_id="m", channel_id="import")
        self.assertEqual(r["kwh"], 0.0)
        self.assertFalse(r.get("needs_review", False))


class TestGuardIsMainMeterOnly(unittest.TestCase):
    """The rogue-total clamp is scoped to the main meter (is_sub_meter=False).
    Device sub-meters — including session-kWh sensors that reset to 0 after an
    EV charge — use the separate per-pair sub-meter path and must be unaffected
    by the guard."""

    _TS = ["2026-07-01T21:00:00", "2026-07-01T21:15:00",
           "2026-07-01T21:30:00", "2026-07-01T21:45:00"]

    def _channel(self, values, rate=0.30):
        reads = [{"value": v, "ts": self._TS[i]} for i, v in enumerate(values)]
        return {"reads": reads, "rates": [{"value": rate, "ts": reads[0]["ts"]}]}

    def test_device_session_counting_up_unaffected(self):
        """A session sensor counting up within a block sums normally."""
        r = engine.compute_channel(
            self._channel([0.0, 5.2, 11.4]),
            is_sub_meter=True, meter_id="zappi_ev", channel_id="import")
        self.assertAlmostEqual(r["kwh"], 11.4, places=3)
        self.assertNotIn("needs_review", r)

    def test_device_session_reset_to_zero_unaffected(self):
        """The case raised: a session sensor RESETS to 0 mid-block. The sub-meter
        path skips the negative step and counts only the post-reset rise — the
        guard must not touch this and must not clamp it."""
        r = engine.compute_channel(
            self._channel([11.4, 0.0, 2.1]),   # charge ends at 11.4, resets, +2.1
            is_sub_meter=True, meter_id="zappi_ev", channel_id="import")
        self.assertAlmostEqual(r["kwh"], 2.1, places=3)   # not 11.4, not 0
        self.assertNotIn("needs_review", r)

    def test_device_large_value_not_clamped(self):
        """Proof of scope: even a sub-meter delta above the main-meter ceiling is
        NOT clamped — the guard is inside the is_sub_meter=False branch only."""
        r = engine.compute_channel(
            self._channel([0.0, 600.0]),        # 600 > 500 ceiling
            is_sub_meter=True, meter_id="dev", channel_id="import")
        self.assertAlmostEqual(r["kwh"], 600.0)   # untouched, not 0
        self.assertNotIn("needs_review", r)


if __name__ == "__main__":
    unittest.main()