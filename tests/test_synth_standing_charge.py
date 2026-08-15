"""
test_synth_standing_charge.py
=============================
Issue #370: a synthesised (no-HH-pages) dual-rate import bill wrote the daily
standing charge TWICE per day — once on the first night-window slot and once on
the first day-window slot — because `_import_synth_rows._emit` runs once per tier
and each pass kept its own "first slot of the day" set. The daily standing charge
must appear exactly once per local day, on the day's first slot (chronologically
the 00:00 night slot). The single-rate (flat) path already emitted once and must
stay that way.
"""

import os
import sys
import unittest
from collections import Counter
from datetime import date

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import bill_parser as bp


def _sc_rows(rows):
    """{local_day: [Start times that carry an inc standing charge]}."""
    out = {}
    for r in rows:
        if r.get("Standing Charge Inc. Tax (p)") not in ("", None):
            out.setdefault(r["Start"][:10], []).append(r["Start"])
    return out


class TestSynthStandingCharge(unittest.TestCase):

    def _bill(self, tiers):
        b = bp.Bill(vat_import=0.05)
        b.import_periods = [bp.ImportPeriod(
            frm=date(2024, 8, 1), to=date(2024, 8, 2),
            tiers=tiers, total_kwh=sum(t[1] for t in tiers),
            standing_days=2, standing_pre_p=58.35)]
        return b

    def test_dual_rate_standing_once_per_day(self):
        rows = bp._import_synth_rows(self._bill(
            [(10.63, 0.9, 0.10), (31.46, 0.4, 0.13)]))
        sc = _sc_rows(rows)
        self.assertEqual({d: len(v) for d, v in sc.items()},
                         {"2024-08-01": 1, "2024-08-02": 1})
        # …and on the day's first (00:00) slot.
        self.assertEqual(sc["2024-08-01"][0], "2024-08-01T00:00:00+01:00")
        self.assertEqual(sc["2024-08-02"][0], "2024-08-02T00:00:00+01:00")

    def test_dual_rate_with_zero_day_tier(self):
        # The reported bill's shape: all kWh in the night tier, day tier = 0. Still
        # exactly one standing charge per day (the zero-kWh day slots still exist).
        rows = bp._import_synth_rows(self._bill(
            [(10.63, 0.9, 0.10), (31.46, 0.0, 0.0)]))
        self.assertEqual({d: len(v) for d, v in _sc_rows(rows).items()},
                         {"2024-08-01": 1, "2024-08-02": 1})

    def test_flat_rate_unchanged(self):
        rows = bp._import_synth_rows(self._bill([(24.5, 8.0, 1.96)]))
        self.assertEqual({d: len(v) for d, v in _sc_rows(rows).items()},
                         {"2024-08-01": 1, "2024-08-02": 1})

    def test_full_daily_standing_value_not_split(self):
        # The single emitted row carries the FULL daily standing (58.35 * 1.05),
        # not a halved/duplicated figure.
        rows = bp._import_synth_rows(self._bill(
            [(10.63, 0.9, 0.10), (31.46, 0.4, 0.13)]))
        vals = [r["Standing Charge Inc. Tax (p)"] for r in rows
                if r.get("Standing Charge Inc. Tax (p)") not in ("", None)]
        self.assertTrue(all(abs(v - round(58.35 * 1.05, 4)) < 1e-6 for v in vals))


if __name__ == "__main__":
    unittest.main()
