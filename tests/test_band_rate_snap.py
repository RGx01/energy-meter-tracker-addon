"""
BL-69: give a banded-IOG block the tariff rate it was charged at, read from the schedule.

An imported half-hour's rate is divided back out of Octopus's billed cost, so it inherits
that cost's rounding. On one live account the 7.0004p off-peak band is stored as 0.070003
across ~9,500 blocks; an earlier 7.4999p era as 0.07497; a 9.0p era as 0.089985. Cost stays
correct — it is the billed figure — but every reader that compares a rate to the schedule
then disagrees with it.

The heal must never know a rate. Bands come from `day_rate_bounds` per day, so they follow
whatever that account's tariff charged in whatever era; the tolerance is a FRACTION of the
band, because the absolute error scales with the rate (1e-6 at 7.0004p, 2.9e-5 at 7.4999p)
and any constant tuned on one misses the other — and another user's IOG rates differ again.
"""

import ast
import os
import sqlite3
import sys
import unittest

_HERE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _HERE)


def _core():
    src = open(os.path.join(_HERE, "engine.py")).read()
    ns = {}
    for n in ast.parse(src).body:
        if isinstance(n, ast.FunctionDef) and n.name == "_band_rate_snap_core":
            exec(compile(ast.Module([n], []), "<s>", "exec"), ns)
        elif isinstance(n, ast.Assign) and getattr(
                n.targets[0], "id", "") == "_BAND_SNAP_REL_TOL":
            exec(compile(ast.Module([n], []), "<s>", "exec"), ns)
    return ns


class _Sched:
    """Bands in PENCE, as the real RateSchedule returns them."""
    def __init__(self, lo, hi): self.lo, self.hi = lo, hi
    def is_empty(self): return False
    def day_rate_bounds(self, ts): return (self.lo, self.hi)


class _Store:
    VAT = None          # None -> resolve_vat falls back to its own default
    def get_vat_calendar(self):
        return self.VAT

    def __init__(self):
        self._conn = sqlite3.connect(":memory:")
        self._conn.row_factory = sqlite3.Row
        self._conn.executescript("""
          CREATE TABLE blocks (block_start TEXT, meter_id TEXT, imp_kwh REAL,
                               imp_cost REAL, imp_rate REAL, imp_rate_exc REAL);
          CREATE TABLE block_segments (block_start TEXT, channel TEXT, seq INTEGER,
                               kwh REAL, inc_rate REAL, exc_rate REAL, band TEXT,
                               attribution TEXT);
        """)
    def add(self, slot, kwh, rate, cost, band="standard", exc=None, attribution="house",
            seq=0, block=True):
        if block:
            self._conn.execute("INSERT INTO blocks VALUES (?,'electricity_main',?,?,?,?)",
                               (slot, kwh, cost, rate, exc if exc is not None else rate / 1.05))
        self._conn.execute("INSERT INTO block_segments VALUES (?,'import',?,?,?,?,?,?)",
                           (slot, seq, kwh, rate, rate / 1.05, band, attribution))
        self._conn.commit()

    def segs(self, slot):
        return self._conn.execute(
            "SELECT * FROM block_segments WHERE block_start=? ORDER BY seq", (slot,)).fetchall()
    def block(self, slot):
        return self._conn.execute("SELECT * FROM blocks WHERE block_start=?", (slot,)).fetchone()
    def seg(self, slot):
        return self._conn.execute("SELECT * FROM block_segments WHERE block_start=?", (slot,)).fetchone()


SLOT = "2026-03-07T06:30:00"


class _Base(unittest.TestCase):
    def setUp(self):
        self.ns = _core()
        self.ns["_ai"] = None

    def run_core(self, store, lo, hi, tariff="E-1R-INTELLI-VAR-24-10-29-B"):
        self.ns["_kraken_rate_schedules"] = {"import": _Sched(lo, hi)}
        self.ns["_agreement_era"] = lambda ch, bs: (tariff, None, None)
        return self.ns["_band_rate_snap_core"](store)


class TestSnapsToWhateverTheScheduleSays(_Base):

    def test_snaps_the_7p_era(self):
        st = _Store(); st.add(SLOT, 4.018, 0.070003, 0.28127)
        r = self.run_core(st, 7.0004, 28.124)
        self.assertEqual(r["rate_snapped"], 1)
        self.assertAlmostEqual(st.block(SLOT)["imp_rate"], 0.070004, places=8)
        self.assertEqual(st.seg(SLOT)["band"], "off_peak")

    def test_snaps_a_completely_different_tariff(self):
        """The anti-hardcode test: same code, another account's rates, bigger error."""
        st = _Store(); st.add(SLOT, 2.0, 0.07497, 0.14994)
        r = self.run_core(st, 7.4999, 39.2713)
        self.assertEqual(r["rate_snapped"], 1)
        self.assertAlmostEqual(st.block(SLOT)["imp_rate"], 0.074999, places=8)

    def test_snaps_a_third_one(self):
        st = _Store(); st.add(SLOT, 1.0, 0.089985, 0.089985)
        r = self.run_core(st, 9.0, 29.7771)
        self.assertAlmostEqual(st.block(SLOT)["imp_rate"], 0.09, places=8)

    def test_snaps_toward_the_UPPER_band_too(self):
        """House speaks off_peak/day, NEVER peak. 'peak' is the EV vocabulary — the four
        buckets use different labels at the SAME rate so house and EV stay distinguishable
        (ECO7_DAY vs EV_DEVICE_PEAK), and collapsing them loses that."""
        st = _Store(); st.add(SLOT, 1.0, 0.281239, 0.281239)
        self.run_core(st, 7.0004, 28.124)
        self.assertAlmostEqual(st.block(SLOT)["imp_rate"], 0.28124, places=8)
        self.assertEqual(st.seg(SLOT)["band"], "day")
        self.assertNotEqual(st.seg(SLOT)["band"], "peak")


class TestNeverTouchesWhatItDoesNotOwn(_Base):
    """v1 wrote bands from the RATE alone, with no attribution filter and no guard on an
    already-explicit band. On a live account with an EV charger that relabelled 30 EV
    segments Octopus had banded off_peak at the peak RATE, and 169 house 'day' segments to
    'peak' — destroying the four-bucket distinction. The label is the supplier's statement;
    the rate is not a licence to overwrite it."""

    def test_an_ev_segment_is_never_touched(self):
        st = _Store()
        st.add(SLOT, 4.0, 0.070003, 0.28)                                  # house
        st.add(SLOT, 2.0, 0.28124, 0.0, band="off_peak",
               attribution="ev", seq=1, block=False)                       # EV at PEAK rate
        self.run_core(st, 7.0004, 28.124)
        ev = [r for r in st.segs(SLOT) if r["attribution"] == "ev"][0]
        self.assertEqual(ev["band"], "off_peak")                           # untouched
        self.assertAlmostEqual(ev["inc_rate"], 0.28124, places=8)

    def test_an_explicit_house_band_is_never_overwritten(self):
        st = _Store(); st.add(SLOT, 4.0, 0.28124, 1.12, band="off_peak")
        self.run_core(st, 7.0004, 28.124)
        self.assertEqual(st.seg(SLOT)["band"], "off_peak")

    def test_only_the_unknown_fallback_is_filled_in(self):
        st = _Store(); st.add(SLOT, 4.0, 0.28124, 1.12, band="standard")
        self.run_core(st, 7.0004, 28.124)
        self.assertEqual(st.seg(SLOT)["band"], "day")


class TestHouseUsesBothVocabularies(_Base):
    """A house segment banded 'peak' can be perfectly correct. iog_cap.classify_slot writes
    off_peak/day, but recover_device_breakdown — the settled four-bucket read — writes
    `"off_peak" if label_band(lab) else "peak"`. On an account with no home battery, house
    draw lands in a peak bucket routinely. So this pass must never "repair" one."""

    def test_a_settled_house_peak_is_left_alone(self):
        st = _Store(); st.add(SLOT, 4.0, 0.28124, 1.12, band="peak")
        r = self.run_core(st, 7.0004, 28.124)
        self.assertEqual(st.seg(SLOT)["band"], "peak")
        self.assertEqual(r["band_only"], 0)

    def test_nor_a_settled_house_off_peak_at_the_upper_rate(self):
        st = _Store(); st.add(SLOT, 4.0, 0.28124, 1.12, band="off_peak")
        self.run_core(st, 7.0004, 28.124)
        self.assertEqual(st.seg(SLOT)["band"], "off_peak")

    def test_only_standard_is_ever_written(self):
        for existing in ("peak", "day", "off_peak", "mixed"):
            st = _Store(); st.add(SLOT, 4.0, 0.28124, 1.12, band=existing)
            self.run_core(st, 7.0004, 28.124)
            self.assertEqual(st.seg(SLOT)["band"], existing, existing)


class TestExcVat(_Base):
    """exc must be DERIVED from the snapped inc and the statutory VAT, not scaled by the
    stored inc/exc ratio -- which, in the very era this heal exists for, is itself a
    rounding artefact and would carry the error straight through."""

    def test_exc_is_derived_not_inherited(self):
        # 0.070003 / 0.06667 = 1.049993 -- a bad ratio, from the same lost digit.
        st = _Store(); st.add(SLOT, 4.018, 0.070003, 0.28127, exc=0.06667)
        self.run_core(st, 7.0004, 28.124)
        inc = st.block(SLOT)["imp_rate"]
        exc = st.block(SLOT)["imp_rate_exc"]
        self.assertAlmostEqual(inc, 0.070004, places=8)
        self.assertAlmostEqual(exc, 0.070004 / 1.05, places=8)      # derived
        self.assertNotAlmostEqual(exc, 0.06667 * (0.070004 / 0.070003), places=8)  # not scaled

    def test_the_segment_exc_is_derived_too(self):
        st = _Store(); st.add(SLOT, 4.018, 0.070003, 0.28127, exc=0.06667)
        self.run_core(st, 7.0004, 28.124)
        sg = st.seg(SLOT)
        self.assertAlmostEqual(sg["exc_rate"], sg["inc_rate"] / 1.05, places=8)

    def test_a_clean_ratio_era_is_unharmed(self):
        """Where the stored ratio was already exactly 1.05, the answer is the same."""
        st = _Store(); st.add(SLOT, 2.0, 0.07497, 0.14994, exc=0.0714)
        self.run_core(st, 7.4999, 39.2713)
        self.assertAlmostEqual(st.block(SLOT)["imp_rate_exc"], 0.074999 / 1.05, places=8)

    def test_a_null_exc_stays_null(self):
        st = _Store(); st.add(SLOT, 4.0, 0.070003, 0.28, exc=None)
        st._conn.execute("UPDATE blocks SET imp_rate_exc=NULL"); st._conn.commit()
        self.run_core(st, 7.0004, 28.124)
        self.assertIsNone(st.block(SLOT)["imp_rate_exc"])


class TestGuards(_Base):

    def test_non_iog_tariff_is_never_touched(self):
        st = _Store(); st.add(SLOT, 4.0, 0.070003, 0.28)
        r = self.run_core(st, 7.0004, 28.124, tariff="E-1R-VAR-22-11-01-B")
        self.assertEqual(r["rate_snapped"], 0)
        self.assertEqual(r["skipped_non_iog"], 1)
        self.assertAlmostEqual(st.block(SLOT)["imp_rate"], 0.070003, places=8)

    def test_a_flat_tariff_has_no_band_to_snap_to(self):
        st = _Store(); st.add(SLOT, 4.0, 0.497983, 1.99)
        r = self.run_core(st, 49.7983, 49.7983)
        self.assertEqual(r["skipped_flat"], 1)
        self.assertEqual(r["rate_snapped"], 0)

    def test_a_genuine_transition_blend_is_left_alone(self):
        """A half-hour spanning the band boundary sits far from either band."""
        st = _Store(); st.add(SLOT, 4.0, 0.12, 0.48)
        r = self.run_core(st, 7.0004, 28.124)
        self.assertEqual(r["skipped_off_band"], 1)
        self.assertAlmostEqual(st.block(SLOT)["imp_rate"], 0.12, places=8)

    def test_a_rate_one_percent_out_is_not_snapped(self):
        st = _Store(); st.add(SLOT, 4.0, 0.070004 * 1.01, 0.28)
        r = self.run_core(st, 7.0004, 28.124)
        self.assertEqual(r["rate_snapped"], 0)
        self.assertEqual(r["skipped_off_band"], 1)

    def test_the_billed_cost_is_never_written(self):
        st = _Store(); st.add(SLOT, 4.018, 0.070003, 0.28127)
        self.run_core(st, 7.0004, 28.124)
        self.assertAlmostEqual(st.block(SLOT)["imp_cost"], 0.28127, places=8)

    def test_idempotent(self):
        st = _Store(); st.add(SLOT, 4.018, 0.070003, 0.28127)
        first = self.run_core(st, 7.0004, 28.124)
        second = self.run_core(st, 7.0004, 28.124)
        self.assertEqual(first["rate_snapped"], 1)
        self.assertEqual(second["rate_snapped"], 0)

    def test_an_already_exact_rate_still_gets_its_band(self):
        st = _Store(); st.add(SLOT, 4.0, 0.070004, 0.28)
        r = self.run_core(st, 7.0004, 28.124)
        self.assertEqual(r["rate_snapped"], 0)
        self.assertEqual(r["band_only"], 1)
        self.assertEqual(st.seg(SLOT)["band"], "off_peak")

    def test_no_schedule_is_a_refusal_not_a_guess(self):
        st = _Store(); st.add(SLOT, 4.0, 0.070003, 0.28)
        self.ns["_kraken_rate_schedules"] = {}
        self.ns["_agreement_era"] = lambda ch, bs: ("IOG", None, None)
        r = self.ns["_band_rate_snap_core"](st)
        self.assertFalse(r["ok"])
        self.assertAlmostEqual(st.block(SLOT)["imp_rate"], 0.070003, places=8)


class TestTheLineThenFollowsTheBand(unittest.TestCase):
    """Once the band is trustworthy, day_rate_series must prefer it to the schedule."""

    def setUp(self):
        import chart_emit
        self.ce = chart_emit

    def _blk(self, rate, band, kwh=4.0):
        return {"meters": {"electricity_main": {"channels": {"import": {
            "rate": rate, "kwh": kwh,
            "segments": [{"kwh": kwh, "inc_rate": rate, "attribution": "house",
                          "band": band}]}}}}}

    OFF, PK = 0.070004, 0.28124

    def _tou(self):
        """A real two-band day: off-peak 23:30-05:30, peak otherwise. A FLAT list is not
        a time-of-use schedule and day_rate_series rightly declines to override on one."""
        return [(self.OFF if (hh >= 47 or hh < 11) else self.PK) for hh in range(48)]

    def test_off_peak_banded_slot_inside_the_peak_window(self):
        """The reported case: a legacy dispatch discount at 06:30 (slot 13), which the
        tariff schedule puts squarely in the peak window."""
        db = [(13, self._blk(self.OFF, "off_peak")), (20, self._blk(self.PK, "peak"))]
        s = self.ce.day_rate_series(db, slots=48, block_minutes=30, capped=False,
                                    house_tou=self._tou())
        self.assertAlmostEqual(s["house"][13], self.OFF, places=5)   # bill wins
        self.assertAlmostEqual(s["house"][20], self.PK, places=5)    # and peak stays peak

    def test_an_unknown_band_still_defers_to_the_schedule(self):
        """'standard' is pricing_segments' fallback for unknown — it must NOT override
        the tariff, or every un-banded block would start dictating the line."""
        db = [(13, self._blk(self.OFF, "standard"))]
        s = self.ce.day_rate_series(db, slots=48, block_minutes=30, capped=False,
                                    house_tou=self._tou())
        self.assertAlmostEqual(s["house"][13], self.PK, places=5)

    def test_an_overnight_off_peak_slot_is_unaffected(self):
        """Byte-identity guard: a normally-banded off-peak slot plots off-peak either way."""
        db = [(2, self._blk(self.OFF, "off_peak"))]
        s = self.ce.day_rate_series(db, slots=48, block_minutes=30, capped=False,
                                    house_tou=self._tou())
        self.assertAlmostEqual(s["house"][2], self.OFF, places=5)


if __name__ == "__main__":
    unittest.main()
