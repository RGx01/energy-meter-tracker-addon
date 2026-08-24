# -*- coding: utf-8 -*-
"""Unit tests for _parse_ohme_slots (Ohme own-plan parser, BL-31 OHME card upgrade).
The end-to-end OHME path is UNVALIDATED (no Ohme tester), but this pure parser is
fully covered here: official HH:MM local strings (incl. midnight-crossing + anchoring)
and the dan-r ISO attribute shape."""
import re, unittest
from datetime import datetime

_src = open("engine.py", encoding="utf-8").read()
_fn = re.search(r"(def _parse_ohme_slots\(.*?\n    return sorted\(set\(slots\)\)\n)", _src, re.S).group(1)
_ns = {}
exec(_fn, _ns)
parse = _ns["_parse_ohme_slots"]


class TestParseOhmeSlots(unittest.TestCase):
    def test_danr_iso_utc(self):
        raw = [{"start": "2026-08-23T01:00:00", "end": "2026-08-23T02:30:00"}]
        got = parse(raw, datetime(2026, 8, 23, 0, 0), "UTC")
        self.assertEqual(got, ["2026-08-23T01:00:00", "2026-08-23T01:30:00", "2026-08-23T02:00:00"])

    def test_danr_iso_with_offset(self):
        # +01:00 local → UTC minus an hour
        raw = [{"start": "2026-08-23T02:00:00+01:00", "end": "2026-08-23T03:00:00+01:00"}]
        got = parse(raw, datetime(2026, 8, 23, 0, 0), "UTC")
        self.assertEqual(got, ["2026-08-23T01:00:00", "2026-08-23T01:30:00"])

    def test_official_hhmm_utc(self):
        got = parse("01:00-02:30", datetime(2026, 8, 23, 0, 0), "UTC")
        self.assertEqual(got, ["2026-08-23T01:00:00", "2026-08-23T01:30:00", "2026-08-23T02:00:00"])

    def test_official_multiple(self):
        got = parse("01:00-02:00, 03:30-04:00", datetime(2026, 8, 23, 0, 0), "UTC")
        self.assertEqual(got, ["2026-08-23T01:00:00", "2026-08-23T01:30:00", "2026-08-23T03:30:00"])

    def test_official_midnight_cross(self):
        # 23:30-00:30 with now just before midnight → spans into next day
        got = parse("23:30-00:30", datetime(2026, 8, 23, 23, 0), "UTC")
        self.assertEqual(got, ["2026-08-23T23:30:00", "2026-08-24T00:00:00"])

    def test_official_london_bst(self):
        # BST = UTC+1 in August. local 01:00-02:00 → UTC 00:00-01:00
        got = parse("01:00-02:00", datetime(2026, 8, 23, 0, 0), "Europe/London")
        self.assertEqual(got, ["2026-08-23T00:00:00", "2026-08-23T00:30:00"])

    def test_official_anchor_forward(self):
        # a 01:00 slot seen at 22:00 UTC belongs to *tomorrow* morning (nearest ±12h)
        got = parse("01:00-01:30", datetime(2026, 8, 23, 22, 0), "UTC")
        self.assertEqual(got, ["2026-08-24T01:00:00"])

    def test_empty_and_unknown(self):
        for v in ("", "unknown", "unavailable", "none", None, "-"):
            self.assertEqual(parse(v, datetime(2026, 8, 23, 0, 0), "UTC"), [])

    def test_garbage_is_safe(self):
        self.assertEqual(parse("not-a-slot, 99:99-10:00", datetime(2026, 8, 23, 0, 0), "UTC"), [])


if __name__ == "__main__":
    unittest.main()
