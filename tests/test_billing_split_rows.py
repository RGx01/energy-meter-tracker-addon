"""
P4.1: the billing summary's IOG EV/Home split rows (`_bill_split_rows`).

Segment blocks bill a cap-BOUNDARY block to its two REAL rate bands (off-peak + peak), so the
legacy blended "transition/mixed" row is retired for them — it only fires for pre-segment
(legacy-column) blocks. Validated end-to-end on the 3h-cap Highgrove fixture (July):
EV 361 kWh off-peak + ~2 kWh peak (over-cap/bump pushed to peak), Home 583 + ~1.7, transition
buckets 0, EV+Home reconciles to the grid import within rounding (947.53 vs 947.78 kWh).
"""
import os, sys, unittest, re
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import energy_charts as ec

OFF, PEAK = 0.0549, 0.3231


def _rows(html):
    return re.findall(r'<tr><td>(EV|Home)</td><td>([^<]+)</td><td>([^<]+)</td><td>([^<]+)</td>', html or "")


class TestBillSplitRows(unittest.TestCase):
    def _summary(self, ev, home, ev_tr=0.0, home_tr=0.0):
        return {
            "ev_by_rate":   {r: {"kwh": k, "cost": c, "cost_exc": c} for r, (k, c) in ev.items()},
            "home_by_rate": {r: {"kwh": k, "cost": c, "cost_exc": c} for r, (k, c) in home.items()},
            "ev_transition":   {"kwh": ev_tr,   "cost": ev_tr * PEAK,   "cost_exc": ev_tr * PEAK},
            "home_transition": {"kwh": home_tr, "cost": home_tr * PEAK, "cost_exc": home_tr * PEAK},
        }

    def test_segment_block_shows_two_real_bands_no_transition_row(self):
        # off-peak + peak bands for both EV and Home; NO transition bucket → 4 rows, none noted
        sm = self._summary(ev={OFF: (361.0, 19.83), PEAK: (2.0, 0.65)},
                           home={OFF: (583.0, 32.02), PEAK: (1.7, 0.55)})
        html = ec._bill_split_rows(sm, "£")
        rows = [(lbl, float(rate)) for (lbl, rate, kwh, cost) in _rows(html)]
        # displayed rate is cost/kwh; assert each label appears once off-peak (<0.1) and once peak (>0.3)
        ev_rates = sorted(r for (lbl, r) in rows if lbl == "EV")
        home_rates = sorted(r for (lbl, r) in rows if lbl == "Home")
        self.assertTrue(ev_rates[0] < 0.1 and ev_rates[-1] > 0.3, ev_rates)     # EV off-peak + over-cap PEAK
        self.assertTrue(home_rates[0] < 0.1 and home_rates[-1] > 0.3, home_rates)
        self.assertNotIn("(transition)", html)   # retired for segment blocks

    def test_legacy_mixed_block_still_shows_transition_row(self):
        # a legacy block with a blended boundary → the transition bucket → a noted row
        sm = self._summary(ev={OFF: (100.0, 5.49)}, home={OFF: (200.0, 10.98)},
                           ev_tr=3.0, home_tr=1.0)
        html = ec._bill_split_rows(sm, "£")
        self.assertIn("(transition)", html)      # legacy path keeps the blended row

    def test_none_when_no_ev(self):
        sm = self._summary(ev={}, home={OFF: (200.0, 10.98)})
        self.assertIsNone(ec._bill_split_rows(sm, "£"))  # no EV → caller shows plain rows


if __name__ == "__main__":
    unittest.main()
