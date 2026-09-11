"""
test_measurement_ladder.py — 4.5.7.

recover_measurement_costs reads the OFF_PEAK label, which Octopus only attaches once the
query window extends ~2h PAST the slot (forward-anchored; proven by window-sweep on
2026-07-21/-23 & 08-14). The recovery ladder must therefore widen the FORWARD edge, not
the look-back. These simulate that API behaviour and assert the ladder reaches it.
"""
import os
import sys
import asyncio
import unittest
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from kraken_api_client import KrakenAPIClient

SLOT = "2026-07-21T15:00:00"
SLOT_DT = datetime.fromisoformat(SLOT)


class _FwdClient(KrakenAPIClient):
    """Simulates the forward-anchored label: OFF_PEAK only when the window END reaches
    >= slot + `off_at` hours. Records the max look-ahead the ladder actually tried."""
    def __init__(self, off_at_hours):
        self.account_number = "A-TEST"
        self._off_at = off_at_hours
        self.max_lookahead = 0.0

    async def get_measurements(self, mpan, start, end, **kw):
        e = datetime.fromisoformat(end.replace("Z", ""))
        la = (e - SLOT_DT).total_seconds() / 3600.0
        self.max_lookahead = max(self.max_lookahead, la)
        off = la >= self._off_at - 1e-9
        return [{"start": SLOT, "cost_incl": 0.0111, "cost_excl": 0.0105,
                 "kwh": 0.203, "off_peak": off, "standing_incl": None,
                 "standing_excl": None, "buckets": ["OFF_PEAK" if off else "STANDARD"]}]

    async def close(self):
        pass


class TestForwardLadder(unittest.TestCase):
    def _run(self, off_at):
        c = _FwdClient(off_at)
        rec = asyncio.run(
            c.recover_measurement_costs("M", [SLOT], account_number="A-TEST", pace_s=0))
        return c, rec

    def test_flips_at_2h_forward(self):
        # label appears at +2h; the ladder's first rung (2h) must catch it.
        c, rec = self._run(off_at=2.0)
        self.assertIn(SLOT, rec)
        self.assertTrue(rec[SLOT]["off_peak"], "should recover OFF_PEAK via the 2h forward rung")
        self.assertGreaterEqual(c.max_lookahead, 2.0)

    def test_widens_forward_when_first_rung_standard(self):
        # label only at +3h → ladder must widen to the 3h rung and still get OFF_PEAK.
        c, rec = self._run(off_at=3.0)
        self.assertTrue(rec[SLOT]["off_peak"])
        self.assertGreaterEqual(c.max_lookahead, 3.0)

    def test_never_offpeak_keeps_standard_fallback(self):
        # a genuinely-peak slot (never flips) is returned as STANDARD, not dropped.
        c, rec = self._run(off_at=99.0)
        self.assertIn(SLOT, rec)
        self.assertFalse(rec[SLOT]["off_peak"])


if __name__ == "__main__":
    unittest.main()
