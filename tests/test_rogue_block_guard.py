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
    """Session-energy sub-meter sensors (which reset to 0 each EV charge and
    count up) must be booked normally — the sub-meter guard keys on physical
    plausibility (the 60 kWh device ceiling), NOT on a zero opener, so a genuine
    charge counting up from 0 is preserved while a lifetime-register dump above
    the ceiling is clamped (#260)."""

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

    def test_device_impossible_value_is_clamped(self):
        """#260: a sub-meter block above the device physical ceiling (60 kWh) IS
        now clamped — previously the guard was is_sub_meter=False only, which let
        a cumulative sensor book its whole lifetime register."""
        r = engine.compute_channel(
            self._channel([0.0, 600.0]),        # 600 kWh in a block — impossible
            is_sub_meter=True, meter_id="dev", channel_id="import")
        self.assertEqual(r["kwh"], 0.0)          # clamped, not 600
        self.assertEqual(r["read_start"], r["read_end"])
        self.assertTrue(r.get("needs_review"))


if __name__ == "__main__":
    unittest.main()

class TestSubMeterRogueBlockGuard(unittest.TestCase):
    """#260: the main-meter clamp was is_sub_meter-gated, leaving device channels
    unprotected — a cumulative battery/EV sensor with a lost/absent opener booked
    its whole lifetime register as one block. The sub-meter branch now clamps the
    lost-opener signature (magnitude-independent) and a physical ceiling."""

    def _sub(self, opener, closer, rate=0.30):
        return {
            "reads": [
                {"value": opener, "ts": "2026-07-01T21:30:00"},
                {"value": closer, "ts": "2026-07-01T22:00:00"},
            ],
            "rates": [{"value": rate, "ts": "2026-07-01T21:30:00"}],
        }

    def _kwh(self, opener, closer):
        return engine.compute_channel(
            self._sub(opener, closer), is_sub_meter=True,
            meter_id="battery", channel_id="import")

    def test_lifetime_dump_clamped(self):
        r = self._kwh(0.0, 9977.4)          # the reporter's ~10 MWh
        self.assertEqual(r["kwh"], 0.0)
        self.assertEqual(r["read_start"], r["read_end"])
        self.assertEqual(r["read_end"], 9977.4)
        self.assertTrue(r.get("needs_review"))

    def test_small_lifetime_dump_clamped(self):
        # a lifetime well above any real device block but under the main 500
        # clamp — caught by the sub-meter physical ceiling (60 kWh)
        self.assertEqual(self._kwh(0.0, 380.0)["kwh"], 0.0)

    def test_large_but_plausible_charge_preserved(self):
        # a session sensor counting a big-but-physical charge (e.g. 45 kWh over a
        # long block) starts at 0 and must NOT be clamped — it's under 60 kWh
        r = self._kwh(0.0, 45.0)
        self.assertAlmostEqual(r["kwh"], 45.0, places=3)
        self.assertFalse(r.get("needs_review", False))

    def test_normal_cumulative_block_unaffected(self):
        r = self._kwh(500.0, 503.2)         # opener not near-zero → real 3.2 kWh
        self.assertAlmostEqual(r["kwh"], 3.2, places=3)
        self.assertFalse(r.get("needs_review", False))

    def test_tiny_genuine_draw_from_zero_preserved(self):
        # a genuinely-new-at-0 sensor drawing < 1 kWh in its first block is real
        r = self._kwh(0.0, 0.5)
        self.assertAlmostEqual(r["kwh"], 0.5, places=3)

    def test_single_read_baselines_to_zero(self):
        ch = {"reads": [{"value": 9977.4, "ts": "2026-07-01T22:00:00"}], "rates": []}
        self.assertEqual(engine.compute_channel(
            ch, is_sub_meter=True, meter_id="battery", channel_id="import")["kwh"], 0.0)


if __name__ == "__main__":
    unittest.main()