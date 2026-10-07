"""
4.5.21 — the day chart's house/device rate line draws a manually corrected rate.

A rate correction rewrites a block's segments to the new rate but not their band label. The house
line (`chart_emit.day_rate_series`, which every non-EV device rides) only trusted a stored rate
with an explicit off_peak/peak band, a strictly-between blend, or an EV rate in the slot — else it
took the schedule. So 21 Jul 2026 05:30, corrected to off-peak (one house segment, band 'day', no
EV), still drew peak, while 16:00 (an EV segment) dropped. Found on prod-dev, legacy Intelligent.

Discriminating: test_a_corrected_slot_draws_its_corrected_rate and
test_the_lightweight_block_carries_the_flag fail on 4.5.20. The uncorrected cases are guards.
"""
import os
import sys
import unittest

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _ROOT)
import chart_emit                                # noqa: E402
from block_store import BlockStore               # noqa: E402

OFF, PEAK = 0.0549297, 0.323092
HH = 11                                          # 05:30 local, the corrected half-hour


def _day(corrected):
    imp = {"rate": OFF, "segments": [
        {"kwh": 0.142, "inc_rate": OFF, "band": "day", "attribution": "house"}]}
    if corrected:
        imp["rate_corrected"] = True
    blk = {"meters": {"electricity_main": {"channels": {"import": imp}}}}
    # A legacy Intelligent day: night off-peak until 05:30, peak after (the schedule's prediction).
    tou = [OFF if i < HH else PEAK for i in range(48)]
    tou[47] = OFF
    return [(HH, blk), (47, {"meters": {"electricity_main": {"channels": {"import": {"rate": OFF}}}}})], tou


class TheRateLine(unittest.TestCase):

    def test_a_corrected_slot_draws_its_corrected_rate(self):
        db, tou = _day(corrected=True)
        s = chart_emit.day_rate_series(db, slots=48, block_minutes=30, capped=False, house_tou=tou)
        self.assertAlmostEqual(s["house"][HH], OFF, places=6)

    def test_an_uncorrected_slot_still_follows_the_schedule(self):
        """GUARD: without the user's correction the line keeps its rules (schedule here)."""
        db, tou = _day(corrected=False)
        s = chart_emit.day_rate_series(db, slots=48, block_minutes=30, capped=False, house_tou=tou)
        self.assertAlmostEqual(s["house"][HH], PEAK, places=6)


class TheLightweightBlock(unittest.TestCase):

    def _store(self):
        st = BlockStore(":memory:")
        st.insert_config_period({"meters": {"electricity_main": {"meta": {
            "billing_day": 1, "block_minutes": 30, "timezone": "UTC",
            "currency_symbol": "£", "currency_code": "GBP"}}}})
        cp = st.get_current_config_period_id()
        with st._conn:
            for bs, corrected in (("2026-07-21T04:30:00", 1), ("2026-07-21T05:00:00", 0)):
                st._conn.execute(
                    "INSERT INTO blocks (block_start, block_end, meter_id, config_period_id, "
                    "interpolated, imp_kwh, imp_rate, imp_cost, exp_kwh, exp_cost, rate_corrected) "
                    "VALUES (?, ?, 'electricity_main', ?, 0, 0.1, ?, 0.01, 0, 0, ?)",
                    (bs, bs, cp, OFF if corrected else PEAK, corrected))
        return st

    def _imp(self, blocks, bs):
        b = next(x for x in blocks if x["start"] == bs)
        return b["meters"]["electricity_main"]["channels"]["import"]

    def test_the_lightweight_block_carries_the_flag(self):
        blocks = self._store().get_blocks_lightweight()
        self.assertTrue(self._imp(blocks, "2026-07-21T04:30:00").get("rate_corrected"))

    def test_an_uncorrected_block_gains_no_key(self):
        """GUARD: only a corrected block changes shape."""
        blocks = self._store().get_blocks_lightweight()
        self.assertNotIn("rate_corrected", self._imp(blocks, "2026-07-21T05:00:00"))


if __name__ == "__main__":
    unittest.main()
