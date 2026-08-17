"""
test_gap_template_local.py
==========================
The CSV gap/date-range template must start at LOCAL midnight, not UTC midnight.

A date-range picker sends the picked LOCAL day (e.g. 2026-08-01T00:00:00 meaning
local midnight). The template generator treats its input as UTC, so in BST that
rendered the first row as 2026-08-01T01:00:00+01:00 instead of 00:00+01:00 — the
whole day shifted forward an hour, dropping the first two slots and adding two from
the next day. In GMT (offset 0) the two coincide, which is why it only showed in
summer. The endpoint localises `from_local`/`to_local` boundaries before generating;
this reproduces that pipeline and pins the fix. Detected gaps pass real UTC
block_starts (no flag) and must be unaffected.
"""

import os
import sys
import unittest
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import csv_import as ci
from block_store import local_datetime_to_utc

TZ = "Europe/London"
BM = 30


def _endpoint_rows(frm, to, *, from_local=False, to_local=False, inclusive=False):
    """Mirror api_historical_gap_template's boundary handling, then generate."""
    if from_local:
        frm = local_datetime_to_utc(frm[:10], (frm[11:16] or "00:00"), TZ)
    if to_local:
        to = local_datetime_to_utc(to[:10], (to[11:16] or "00:00"), TZ)
    if inclusive and not to_local:
        _t = datetime.fromisoformat(to.replace("Z", "").split("+")[0])
        to = (_t + timedelta(minutes=BM)).isoformat()
    text = ci.gap_template_csv(frm, to, block_minutes=BM, tz_name=TZ)
    rows = text.strip().splitlines()[1:]          # drop header
    return [r.split(",")[0] for r in rows]        # Start column


class TestGapTemplateLocal(unittest.TestCase):

    def test_bst_single_day_starts_at_local_midnight(self):
        # The exact scenario from the bug report: pick Aug 1 (BST) as a date range.
        starts = _endpoint_rows("2026-08-01T00:00:00", "2026-08-01T23:59:59",
                                from_local=True, to_local=True, inclusive=True)
        self.assertEqual(starts[0], "2026-08-01T00:00:00+01:00")   # not 01:00
        self.assertEqual(starts[-1], "2026-08-01T23:30:00+01:00")
        self.assertEqual(len(starts), 48)                          # not 49

    def test_bst_two_day_range(self):
        starts = _endpoint_rows("2026-08-01T00:00:00", "2026-08-02T23:59:59",
                                from_local=True, to_local=True, inclusive=True)
        self.assertEqual(starts[0], "2026-08-01T00:00:00+01:00")
        self.assertEqual(starts[-1], "2026-08-02T23:30:00+01:00")
        self.assertEqual(len(starts), 96)

    def test_gmt_single_day_unchanged(self):
        # GMT already worked (offset 0); confirm the fix doesn't regress it.
        starts = _endpoint_rows("2026-03-01T00:00:00", "2026-03-01T23:59:59",
                                from_local=True, to_local=True, inclusive=True)
        self.assertEqual(starts[0], "2026-03-01T00:00:00+00:00")
        self.assertEqual(starts[-1], "2026-03-01T23:30:00+00:00")
        self.assertEqual(len(starts), 48)

    def test_gap_path_utc_starts_unaffected(self):
        # A detected BST whole-day gap passes real UTC block_starts (no local flag,
        # inclusive last-slot). Must still render local midnight -> 23:30, 48 slots.
        starts = _endpoint_rows("2026-07-31T23:00:00", "2026-08-01T22:30:00",
                                inclusive=True)
        self.assertEqual(starts[0], "2026-08-01T00:00:00+01:00")
        self.assertEqual(starts[-1], "2026-08-01T23:30:00+01:00")
        self.assertEqual(len(starts), 48)


if __name__ == "__main__":
    unittest.main()
