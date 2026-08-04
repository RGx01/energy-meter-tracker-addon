"""test_bill_parser.py — Octopus PDF bill → EMT CSV parser.

Uses SYNTHETIC page text (mimicking the Octopus layout) rather than a real bill,
so there's no personal data in the repo and no pypdf/PDF dependency at test time
(bill_parser imports pypdf lazily, only inside _read_pages, which we monkeypatch).
"""
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import bill_parser as bp  # noqa: E402


def _hh_page(day_str, rows):
    """Build one HH day-page's text. `rows` = [(hhmm_start, hhmm_end, rate, kwh, cost)]."""
    body = [f"{day_str}", "For electricity meter 22L3610597",
            "Total cost", "£ 1.00",
            f"Total consumption {round(sum(r[3] for r in rows),2)} kWh",
            "Period", "Rate p / kWh", "Consumption kWh", "Cost p"]
    for (a, b, rate, kwh, cost) in rows:
        body.append(f"{a} - {b}\n{rate}\n{kwh}\n{cost}")
    return "\n".join(body)


def _full_day_rows(rate_night=6.67, rate_day=23.84, total=4.8):
    """48 half-hour rows summing to `total`, IOG-style: all consumption at NIGHT
    (before 05:30), zero during the day — so daylight is free for export (exclusivity)."""
    night_idx = [i for i in range(48) if f"{(i*30)//60:02d}:{(i*30)%60:02d}" < "05:30"]
    per = total / len(night_idx)
    out = []
    for i in range(48):
        mins = i * 30
        sh, sm = divmod(mins, 60)
        eh, em = divmod(mins + 30, 60)
        eh = eh % 24
        hhmm = f"{sh:02d}:{sm:02d}"
        rate = rate_night if hhmm < "05:30" else rate_day
        kwh = round(per, 3) if i in night_idx else 0.0
        out.append((hhmm, f"{eh:02d}:{em:02d}", rate, kwh, round(rate * kwh, 3)))
    return out


SUMMARY = """Your Charges In Detail
Electricity
Supply number
S 2 900 BBA 2600002170611
Intelligent Octopus Go (3rd December 2024 - 4th December 2024)
Energy Charges for Meter 22L3610597
6.67p/kWh 9.0 kWh £0.600
23.84p/kWh 0.6 kWh £0.143
Total consumption 9.6kWh @ 7.7p/kWh
Standing Charge 2 days @ 52.24p/day
VAT @ 5.00%
Electricity
Supply number
S 2 900 BBA 2600002170611
Intelligent Octopus Go (5th December 2024 - 5th December 2024)
Energy Charges for Meter 22L3610597
6.67p/kWh 4.8 kWh £0.320
Total consumption 4.8kWh @ 6.67p/kWh
Standing Charge 1 days @ 52.24p/day
Outgoing export
Supply number
S 8 867 BBE 2600004092930
Outgoing Octopus 12M Fixed (3rd December 2024 - 5th December 2024)
Energy Exported for Meter 22L3610597
Energy Exported 30.0 kWh @ 15.00p/kWh £4.50
Standing Charge 3 days @ 0.00p/day
VAT @ 0.00%
"""


def _pages():
    # 3 HH day pages (3rd, 4th, 5th Dec), each summing to 4.8 kWh (48 × 0.1).
    return [SUMMARY,
            _hh_page("3rd December 2024", _full_day_rows()),
            _hh_page("4th December 2024", _full_day_rows()),
            _hh_page("5th December 2024", _full_day_rows())]


class TestParse(unittest.TestCase):
    def setUp(self):
        self._orig = bp._read_pages
        bp._read_pages = lambda src: _pages()
        self.bill = bp.parse_bill("x.pdf")

    def tearDown(self):
        bp._read_pages = self._orig

    def test_mpans_import_and_export_distinct(self):
        self.assertEqual(self.bill.mpan_import, "2600002170611")
        self.assertEqual(self.bill.mpan_export, "2600004092930")

    def test_vat_rates(self):
        self.assertEqual(self.bill.vat_import, 0.05)

    def test_two_import_periods(self):
        self.assertEqual(len(self.bill.import_periods), 2)
        self.assertEqual(self.bill.import_periods[1].standing_pre_p, 52.24)

    def test_export_period_backs_out(self):
        self.assertEqual(len(self.bill.export_periods), 1)
        e = self.bill.export_periods[0]
        self.assertAlmostEqual(e.kwh, 30.0)
        self.assertAlmostEqual(e.rate_pre_p, 15.0)

    def test_hh_days_transcribed(self):
        self.assertEqual(len(self.bill.hh_days), 3)
        self.assertEqual(len(self.bill.hh_days[0]), 48)

    def test_reconciliation_ok(self):
        self.assertTrue(self.bill.reconciliation["ok"])


class TestCsv(unittest.TestCase):
    def setUp(self):
        self._orig = bp._read_pages
        bp._read_pages = lambda src: _pages()
        self.bill = bp.parse_bill("x.pdf")
        self.csv = bp.build_csv_rows(self.bill)

    def tearDown(self):
        bp._read_pages = self._orig

    def test_import_rows_are_transcription(self):
        self.assertEqual(len(self.csv["import"]), 3 * 48)   # 3 days × 48 HH

    def test_import_rate_grossed_up_inc_vat(self):
        r0 = self.csv["import"][0]
        self.assertAlmostEqual(r0["Unit Rate (p/kWh)"], round(6.67 * 1.05, 4))   # 7.0035
        self.assertEqual(r0["Estimated Cost Inc. Tax (p)"], "")                  # rate-first

    def test_start_is_tz_aware(self):
        self.assertTrue(self.csv["import"][0]["Start"].endswith("+00:00"))       # GMT in Dec

    def test_standing_once_per_day_full_daily(self):
        stand = [r for r in self.csv["import"] if r["Standing Charge Inc. Tax (p)"] not in ("", None)]
        self.assertEqual(len(stand), 3)                                          # one per day
        self.assertAlmostEqual(stand[0]["Standing Charge Inc. Tax (p)"], round(52.24 * 1.05, 4))

    def test_export_total_reconciles_to_credit(self):
        exp = self.csv["export"]
        total = sum(r["Consumption (kWh)"] for r in exp)
        self.assertAlmostEqual(total, 30.0, places=3)
        self.assertAlmostEqual(total * 0.15, 4.50, places=2)                     # credit

    def test_export_rate_and_no_standing(self):
        exp = self.csv["export"]
        self.assertAlmostEqual(exp[0]["Unit Rate (p/kWh)"], 15.0)                # 0% VAT
        self.assertTrue(all(r["Standing Charge Inc. Tax (p)"] == "" for r in exp))

    def test_export_lands_in_daylight(self):
        # Export is spread across daylight half-hours (08:00–16:00 local). No
        # import/export exclusivity is enforced — both are grid-boundary quantities
        # and device attribution only ever splits import — so a shared slot is fine.
        for r in self.csv["export"]:
            hhmm = r["Start"][11:16]
            self.assertTrue("08:00" <= hhmm < "16:00", r["Start"])

    def test_csv_serialises(self):
        text = bp.rows_to_csv(self.csv["import"])
        self.assertTrue(text.startswith("Start,End,Consumption"))
        self.assertEqual(len(text.strip().splitlines()), 3 * 48 + 1)             # + header


class TestSynthesis(unittest.TestCase):
    """A period with no HH pages → synthesise from tier totals (§2b)."""

    def test_flat_even_distribution(self):
        b = bp.Bill(vat_import=0.05)
        from datetime import date
        b.import_periods = [bp.ImportPeriod(date(2024, 6, 1), date(2024, 6, 1),
                                            tiers=[(30.0, 48.0, 14.4)], total_kwh=48.0,
                                            standing_days=1, standing_pre_p=50.0)]
        rows = bp._import_synth_rows(b)
        self.assertEqual(len(rows), 48)                       # one flat day
        self.assertAlmostEqual(sum(r["Consumption (kWh)"] for r in rows), 48.0, places=3)
        self.assertAlmostEqual(rows[0]["Unit Rate (p/kWh)"], round(30.0 * 1.05, 4))
        self.assertTrue(any(b.warnings))                      # flagged shape-approximated

    def test_dual_rate_splits_night_window(self):
        b = bp.Bill(vat_import=0.05)
        from datetime import date
        b.import_periods = [bp.ImportPeriod(date(2024, 6, 1), date(2024, 6, 1),
                                            tiers=[(6.0, 12.0, 0.72), (30.0, 36.0, 10.8)],
                                            total_kwh=48.0, standing_days=1, standing_pre_p=50.0)]
        rows = bp._import_synth_rows(b)
        # night window 23:30–05:30 = 12 half-hours; each gets 12kWh/12 = 1.0.
        night = [r for r in rows if r["Unit Rate (p/kWh)"] == round(6.0 * 1.05, 4)]
        self.assertEqual(len(night), 12)
        self.assertAlmostEqual(sum(r["Consumption (kWh)"] for r in night), 12.0, places=3)


class TestNoneDatedPeriodsDoNotCrash(unittest.TestCase):
    """A period header EMT can't fully parse leaves frm/to = None. build_csv_rows must
    skip it gracefully, not raise (the 'NoneType - NoneType' crash on a real bill)."""

    def test_none_dated_periods_skipped(self):
        from datetime import date
        b = bp.Bill(vat_import=0.05)
        b.export_periods = [bp.ExportPeriod(None, None, 10.0, 15.0, 1.5)]
        b.import_periods = [
            bp.ImportPeriod(None, None, [(6.0, 10.0, 0.6)], 10.0, 1, 50.0),   # unparseable
            bp.ImportPeriod(date(2024, 6, 1), date(2024, 6, 1),
                            [(6.0, 48.0, 2.88)], 48.0, 1, 50.0),             # valid → synth
        ]
        rows = bp.build_csv_rows(b)                 # must not raise
        self.assertEqual(len(rows["export"]), 0)    # None-dated export skipped
        self.assertEqual(len(rows["import"]), 48)   # only the valid period synthesised

    def test_period_days_helper(self):
        from datetime import date
        self.assertEqual(bp._period_days(None, date(2024, 1, 2)), [])
        self.assertEqual(bp._period_days(date(2024, 1, 2), None), [])
        self.assertEqual(len(bp._period_days(date(2024, 1, 1), date(2024, 1, 3))), 3)
        self.assertEqual(bp._period_days(date(2024, 1, 3), date(2024, 1, 1)), [])  # reversed


class TestExportFallbacks(unittest.TestCase):
    """A bill whose export section has meter reads + a tariff rate but NO
    'Energy Exported X kWh @ Yp/kWh £Z' line must still produce export: derive kWh
    from the two meter reads and the rate from the outgoing tariff summary."""

    CREDIT_ONLY = """Your Charges In Detail
Electricity
Supply number
S 8 867 BBE 2600004092930
Outgoing Octopus 12M Fixed (3rd December 2024 - 5th December 2024)
Energy exported for Meter 22L3610597
3rd Dec 2024
2393.4 Smart meter reading
6th Dec 2024
2423.4 Smart meter reading
Total Electricity Credits
£4.50
About Your Tariff
Electricity
Tariff Name
Outgoing Octopus 12M Fixed May 2019
Unit Rate
15.00p/kWh
"""

    def _bill(self):
        orig = bp._read_pages
        bp._read_pages = lambda src: [self.CREDIT_ONLY]
        try:
            return bp.parse_bill("x.pdf")
        finally:
            bp._read_pages = orig

    def test_export_mpan_and_period(self):
        b = self._bill()
        self.assertEqual(b.mpan_export, "2600004092930")
        self.assertEqual(len(b.export_periods), 1)

    def test_kwh_from_meter_reads_and_rate_from_tariff(self):
        e = self._bill().export_periods[0]
        self.assertAlmostEqual(e.kwh, 30.0)          # 2423.4 − 2393.4
        self.assertAlmostEqual(e.rate_pre_p, 15.0)   # from the outgoing tariff summary
        self.assertAlmostEqual(e.credit_gbp, 4.50)

    def test_builds_export_rows(self):
        rows = bp.build_csv_rows(self._bill())["export"]
        self.assertTrue(rows)
        self.assertAlmostEqual(sum(r["Consumption (kWh)"] for r in rows), 30.0, places=2)

    def test_no_warning_when_export_reads_fine(self):
        b = self._bill()
        self.assertFalse(any("couldn't be read" in w for w in b.warnings))


class TestAbbreviatedMonths(unittest.TestCase):
    """Octopus uses full month names on the HH day pages but ABBREVIATED ones in the
    Charges-In-Detail period headers ('(3rd Jul 2024 - 2nd Aug 2024)'). Both the
    import (standing charge) and export periods must parse from the abbreviations."""

    SUMMARY_ABBR = """Your Charges In Detail
Electricity Supply number S2 900 BBA
2600002170611
Intelligent Octopus Go (3rd Jul 2024 - 2nd Aug 2024)
Energy Charges for Meter 22L3610597
6.67p/kWh 745.6 kWh £49.706
21.70p/kWh 6.0 kWh £1.304
Total consumption 751.6kWh @ 6.79p/kWh
Standing Charge 31 days @ 51.37p/day
VAT @ 5.00%
Outgoing export Supply number S8 867 BBE
2600004092930
Outgoing Octopus 12M Fixed (14th Jul 2024 - 2nd Aug 2024)
Energy Exported for Meter 22L3610597
Energy Exported 478.5 kWh @ 15.00p/kWh £71.77
VAT @ 0.00%
"""

    def _bill(self):
        orig = bp._read_pages
        bp._read_pages = lambda src: [self.SUMMARY_ABBR]
        try:
            return bp.parse_bill("x.pdf")
        finally:
            bp._read_pages = orig

    def test_import_period_parses_with_standing(self):
        from datetime import date
        b = self._bill()
        self.assertEqual(len(b.import_periods), 1)
        p = b.import_periods[0]
        self.assertEqual((p.frm, p.to), (date(2024, 7, 3), date(2024, 8, 2)))
        self.assertAlmostEqual(p.standing_pre_p, 51.37)

    def test_export_period_parses(self):
        from datetime import date
        b = self._bill()
        self.assertEqual(len(b.export_periods), 1)
        e = b.export_periods[0]
        self.assertEqual((e.frm, e.to), (date(2024, 7, 14), date(2024, 8, 2)))
        self.assertAlmostEqual(e.kwh, 478.5)
        self.assertAlmostEqual(e.rate_pre_p, 15.0)


class TestReconMismatch(unittest.TestCase):
    def test_day_mismatch_flags(self):
        # A day page whose rows don't sum to its printed Total consumption.
        bad = _hh_page("3rd December 2024", _full_day_rows())
        bad = bad.replace("Total consumption 4.8 kWh", "Total consumption 99.0 kWh")
        orig = bp._read_pages
        bp._read_pages = lambda src: [SUMMARY, bad,
                                      _hh_page("4th December 2024", _full_day_rows()),
                                      _hh_page("5th December 2024", _full_day_rows())]
        try:
            b = bp.parse_bill("x.pdf")
            self.assertFalse(b.reconciliation["ok"])
            self.assertTrue(any("parsed" in w for w in b.warnings))
        finally:
            bp._read_pages = orig


class TestAutumnClockChange(unittest.TestCase):
    """On the day the clocks go back (29 Oct 2023) the 01:00 and 01:30 wall-clock
    slots occur twice. The HH page lists them in order (BST pair, then GMT pair); the
    parser must emit the first at +01:00 and the second at +00:00, or both collapse to
    the same UTC instant and a UTC half-hour is lost → EMT sees a phantom gap."""

    from datetime import date as _date

    def _fold_day(self):
        d = self._date(2023, 10, 29)
        raw = [(0, 30, 1, 0), (1, 0, 1, 30), (1, 30, 2, 0),   # BST
               (1, 0, 1, 30), (1, 30, 2, 0), (2, 0, 2, 30)]   # GMT (time steps back)
        slots = [bp.HHSlot(d, sh, sm, eh, em, 10.0, 0.5, 5.0)
                 for (sh, sm, eh, em) in raw]
        b = bp.Bill(source="t"); b.hh_days = [slots]; b.vat_import = 0.05
        return b

    def test_repeated_hour_maps_to_distinct_utc(self):
        import csv_import as ci
        rows = bp._import_hh_rows(self._fold_day())
        utc = [ci._to_naive_utc(r["Start"]) for r in rows]
        self.assertEqual(len(set(utc)), len(utc))          # no collision → no gap
        # The two 01:00 rows split into 00:00 UTC (BST) and 01:00 UTC (GMT).
        self.assertIn("2023-10-29T00:00:00", utc)
        self.assertIn("2023-10-29T01:00:00", utc)
        self.assertIn("2023-10-29T00:30:00", utc)          # 01:30 BST
        self.assertIn("2023-10-29T01:30:00", utc)          # 01:30 GMT

    def test_normal_day_unaffected(self):
        import csv_import as ci
        d = self._date(2024, 3, 10)   # a plain day, no fold
        raw = [(0, 0, 0, 30), (0, 30, 1, 0), (1, 0, 1, 30), (1, 30, 2, 0)]
        slots = [bp.HHSlot(d, sh, sm, eh, em, 10.0, 0.5, 5.0)
                 for (sh, sm, eh, em) in raw]
        b = bp.Bill(source="t"); b.hh_days = [slots]; b.vat_import = 0.05
        utc = [ci._to_naive_utc(r["Start"]) for r in bp._import_hh_rows(b)]
        self.assertEqual(len(set(utc)), 4)                 # all distinct, no fold applied


if __name__ == "__main__":
    unittest.main()
