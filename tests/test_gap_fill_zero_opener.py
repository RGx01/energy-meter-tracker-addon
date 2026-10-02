"""
test_gap_fill_zero_opener.py — #493: a gap-fill that opened from a 0.0 register.

A power cut. The add-on's gap-fill stored the half-hours it had no readings for with
registers of 0.0 → 0.0. The next gap-fill took the last block's `read_end` (0.0) as its
opening register (`extract_last_reads`) and interpolated from there to the real
post-outage register (`build_gap_blocks`). That booked the main meter's whole lifetime
export register as an hour's export, and a device's register as phantom device energy.
The 500 kWh rogue-total ceiling didn't catch it, because it lives in `compute_channel`
and a gap block never passes through it.

Forward fixes, no heal, and no change to what is stored. A gap block keeps its 0.0 → 0.0
registers, because they keep every half-hour's shape for the charts. Only their readers change:
  * `_apply_pass2`, the write point every block passes, now applies the main meter's
    ceiling as well (it already held #307's device ceiling);
  * `extract_last_reads` (the next gap-fill's opening read) and
    `_reseed_opener_after_short_restart` (a live half-hour's opening read after a short
    restart) do not take a gap block's 0.0 → 0.0 register as a reading
    (`_is_gap_zero_register`).

The incident's figures are replaced by synthetic ones of the same shape. Every test here
except those labelled GUARD fails on the unpatched tree.

Run with:  python3 -m unittest test_gap_fill_zero_opener -v
"""
import os
import sys
import unittest
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import test_engine  # noqa: F401  — installs stubs, imports engine
import engine

REGISTER = 24000.25          # a lifetime export register, synthetic


def dt(s):
    return datetime.fromisoformat(s)


def read(v, ts):
    return {"value": v, "ts": ts}


def _main_block(channels, *, interpolated=True, start="2026-10-01T12:00:00"):
    return {"start": start, "end": start.replace("12:00", "12:30"), "interpolated": interpolated,
            "totals": {"import_kwh": sum(c.get("kwh", 0) for n, c in channels.items() if n == "import"),
                       "import_cost": 0.0,
                       "export_kwh": sum(c.get("kwh", 0) for n, c in channels.items() if n == "export"),
                       "export_cost": 0.0},
            "meters": {"electricity_main": {"meta": {"block_minutes": 30}, "channels": channels}}}


class TheWritePointCeiling(unittest.TestCase):
    """`_apply_pass2` clamps a main-meter channel over the rogue-total ceiling."""

    def test_a_lifetime_export_register_in_one_block_is_clamped(self):
        blk = _main_block({"export": {"kwh": REGISTER - 8000.0, "rate": 0.12,
                                      "cost": (REGISTER - 8000.0) * 0.12,
                                      "read_start": 8000.0, "read_end": REGISTER}})
        blk["totals"]["export_cost"] = (REGISTER - 8000.0) * 0.12
        engine._apply_pass2(blk)
        ch = blk["meters"]["electricity_main"]["channels"]["export"]
        self.assertEqual((ch["kwh"], ch["cost"]), (0.0, 0.0))
        self.assertEqual(ch["read_start"], REGISTER)               # opens the next block right
        self.assertTrue(blk["meters"]["electricity_main"].get("needs_review"))
        self.assertAlmostEqual(blk["totals"]["export_kwh"], 0.0)
        self.assertAlmostEqual(blk["totals"]["export_cost"], 0.0)

    def test_import_too(self):
        blk = _main_block({"import": {"kwh": 900.0, "rate": 0.25, "cost": 225.0,
                                      "read_start": 0.0, "read_end": 900.0}})
        engine._apply_pass2(blk)
        self.assertEqual(blk["meters"]["electricity_main"]["channels"]["import"]["kwh"], 0.0)

    def test_a_real_half_hour_is_untouched(self):
        """GUARD: passes on the unpatched tree too."""
        blk = _main_block({"export": {"kwh": 3.25, "rate": 0.12, "cost": 0.39,
                                      "read_start": REGISTER, "read_end": REGISTER + 3.25}})
        engine._apply_pass2(blk)
        ch = blk["meters"]["electricity_main"]["channels"]["export"]
        self.assertEqual(ch["kwh"], 3.25)
        self.assertEqual(ch["read_start"], REGISTER)
        self.assertFalse(blk["meters"]["electricity_main"].get("needs_review"))


class TheOpeningRead(unittest.TestCase):
    """`extract_last_reads` on the last stored block before a gap."""

    def _stored(self, rs, re, *, interpolated):
        return {"end": "2026-10-01T12:00:00", "interpolated": interpolated,
                "meters": {"electricity_main": {"channels": {
                    "export": {"read_start": rs, "read_end": re, "rate": 0.12}}}}}

    def test_a_gap_blocks_zero_register_is_not_a_reading(self):
        reads, _ = engine.extract_last_reads(self._stored(0.0, 0.0, interpolated=True))
        self.assertNotIn("export", reads["electricity_main"])

    def test_a_gap_blocks_real_register_is(self):
        """GUARD: a gap block whose registers were interpolated from real reads."""
        reads, _ = engine.extract_last_reads(self._stored(REGISTER - 1, REGISTER, interpolated=True))
        self.assertEqual(reads["electricity_main"]["export"]["value"], REGISTER)

    def test_a_live_blocks_zero_is_kept(self):
        """GUARD: a live block's 0.0 can be real (a daily-reset sensor at midnight)."""
        reads, _ = engine.extract_last_reads(self._stored(0.0, 0.0, interpolated=False))
        self.assertEqual(reads["electricity_main"]["export"]["value"], 0.0)


class TheGapFill(unittest.TestCase):

    def setUp(self):
        self.config = {"meters": {
            "electricity_main": {"meta": {"type": "electricity", "block_minutes": 30},
                                 "channels": {"import": {}, "export": {}}},
            "battery": {"meta": {"sub_meter": True, "parent_meter": "electricity_main",
                                 "block_minutes": 30, "meter_type": "battery"},
                        "channels": {"import": {}}}}}
        self.windows = [(dt("2026-10-01T12:00:00"), dt("2026-10-01T12:30:00")),
                        (dt("2026-10-01T12:30:00"), dt("2026-10-01T13:00:00"))]
        self.rates = {"electricity_main": {"import": 0.25, "export": 0.12},
                      "battery": {"import": 0.25}}

    def test_a_gap_block_still_has_every_channel(self):
        """GUARD: no reads → the half-hour is still built, every channel present at 0 kWh
        (the 48-a-day shape the charts rely on)."""
        blocks = engine.build_gap_blocks(self.windows[:1], {}, {}, self.rates, self.config)
        self.assertEqual(len(blocks), 1)
        self.assertEqual(blocks[0]["meters"]["electricity_main"]["channels"]["import"]["kwh"], 0.0)
        self.assertEqual(blocks[0]["meters"]["battery"]["channels"]["import"]["kwh"], 0.0)

    def test_the_incident_end_to_end(self):
        """The last stored block is a gap block with 0.0 → 0.0 registers; the post-outage
        reads are the real registers. Nothing phantom is booked on any channel."""
        last = {"end": "2026-10-01T12:00:00", "interpolated": True, "meters": {
            "electricity_main": {"channels": {
                "import": {"read_start": 0.0, "read_end": 0.0, "rate": 0.25},
                "export": {"read_start": 0.0, "read_end": 0.0, "rate": 0.12}}},
            "battery": {"channels": {"import": {"read_start": 0.0, "read_end": 0.0, "rate": 0.25}}}}}
        pre, _ = engine.extract_last_reads(last)
        post = {"electricity_main": {"import": read(36000.5, "2026-10-01T13:00:00"),
                                     "export": read(REGISTER, "2026-10-01T13:00:00")},
                "battery": {"import": read(40.0, "2026-10-01T13:00:00")}}
        blocks = engine.build_gap_blocks(self.windows, pre, post, self.rates, self.config)
        for b in blocks:
            for meter in b["meters"].values():
                for name, ch in meter["channels"].items():
                    self.assertLess(ch.get("kwh", 0.0), 1.0, (b["start"], name, ch))
            self.assertLess(b["totals"]["export_kwh"], 1.0)
            self.assertLess(b["totals"]["import_kwh"], 1.0)

    def test_an_ordinary_gap_still_interpolates(self):
        """GUARD: real registers either side — the gap is filled as before."""
        pre = {"electricity_main": {"import": read(36000.0, "2026-10-01T12:00:00"),
                                    "export": read(REGISTER - 1.0, "2026-10-01T12:00:00")}}
        post = {"electricity_main": {"import": read(36001.0, "2026-10-01T13:00:00"),
                                     "export": read(REGISTER, "2026-10-01T13:00:00")}}
        blocks = engine.build_gap_blocks(self.windows, pre, post, self.rates, self.config)
        self.assertAlmostEqual(sum(b["totals"]["export_kwh"] for b in blocks), 1.0, places=3)
        self.assertAlmostEqual(sum(b["totals"]["import_kwh"] for b in blocks), 1.0, places=3)



class TheShortRestartReseed(unittest.TestCase):
    """`_reseed_opener_after_short_restart` seeds the live half-hour's opening read from
    the block before it. Its docstring said a finalised `read_end` "cannot reintroduce a
    rogue total" — a gap block's planted 0.0 can."""

    def _last(self, rs, re, *, interpolated):
        return {"start": "2026-10-01T11:30:00", "end": "2026-10-01T12:00:00",
                "interpolated": interpolated,
                "meters": {"battery": {"channels": {"import": {"read_start": rs, "read_end": re}}}}}

    def _current(self):
        return {"start": "2026-10-01T12:00:00", "meters": {}}

    def test_a_gap_blocks_zero_register_is_not_seeded(self):
        cur = self._current()
        engine._reseed_opener_after_short_restart(self._last(0.0, 0.0, interpolated=True), cur)
        self.assertNotIn("reads", cur["meters"].get("battery", {}).get("channels", {}).get("import", {}))

    def test_a_real_register_is(self):
        """GUARD: passes on the unpatched tree too."""
        cur = self._current()
        engine._reseed_opener_after_short_restart(self._last(45.4, 45.5, interpolated=False), cur)
        self.assertEqual(cur["meters"]["battery"]["channels"]["import"]["reads"][0]["value"], 45.5)


if __name__ == "__main__":
    unittest.main()
