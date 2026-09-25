"""
test_measured_apply.py — BL-53 step 3 helper apply_measured_to_block (still inert in prod).

Writes Octopus's billed cost as authoritative: rate keyed on cost/kWh (NOT the label),
EV+house split summing to the bill exactly, rate_source='measured'.
"""
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import engine
import pricing_segments as ps
from block_store import BlockStore


class _Sched:
    def is_empty(self):
        return False

    def day_rate_bounds(self, ts):
        return (5.493, 32.3092)   # pence (off, peak)


class TestMeasuredApply(unittest.TestCase):

    SLOT = "2026-08-23T10:00:00"

    def setUp(self):
        self._save = (engine._store, engine._kraken_rate_schedules)
        self.st = BlockStore(":memory:")
        engine._store = self.st
        engine._kraken_rate_schedules = {"import": _Sched()}
        self.st._conn.execute(
            "INSERT OR IGNORE INTO config_periods (id, effective_from, billing_day, "
            "block_minutes, timezone) VALUES (1,'2020-01-01T00:00:00',1,30,'UTC')")

    def tearDown(self):
        (engine._store, engine._kraken_rate_schedules) = self._save

    def _blk(self, kwh, evk, rate=0.05493):
        self.st._conn.execute(
            "INSERT INTO blocks (block_start, block_end, meter_id, config_period_id, "
            "imp_kwh, imp_kwh_api, imp_rate, imp_cost, imp_kwh_ev, rate_source) "
            "VALUES (?,?,?,?,?,?,?,?,?,?)",
            (self.SLOT, self.SLOT, "electricity_main", 1, kwh, kwh, rate,   # DCC-settled
             round(kwh*rate, 6), evk, "reconciled"))
        self.st._conn.commit()

    def _row(self):
        return self.st._conn.execute(
            "SELECT imp_rate, imp_cost, imp_rate_ev, imp_cost_ev, imp_ev_band, "
            "imp_home_band, rate_source, imp_rate_exc, imp_cost_exc, imp_cost_remainder FROM blocks "
            "WHERE block_start=?", (self.SLOT,)).fetchone()

    def test_orphaned_ev_attributed_from_completed_dispatch(self):
        # EV-split race: imp_kwh_ev NULL but a COMPLETED dispatch exists -> the measured pass
        # attributes the EV from the dispatch (grid-clipped), instead of writing house-only.
        self._blk(3.0, None, rate=0.05493)
        self.st.record_dispatch_history(self.SLOT, "completed", provider="Myenergi",
                                        source="unknown", energy_kwh=-2.0)
        res = engine.apply_measured_to_block(self.SLOT, cost_incl=round(3.0*0.05493, 6),
                                             label="OFF_PEAK")
        self.assertTrue(res)
        r = self._row()
        self.assertIsNotNone(r["imp_rate_ev"])                       # EV attributed, not house-only
        self.assertAlmostEqual(r["imp_cost_ev"], round(2.0*0.05493, 6), places=5)

    def test_no_dispatch_stays_house_only(self):
        # imp_kwh_ev NULL and NO dispatch -> house-only (never invents EV).
        self._blk(3.0, None, rate=0.05493)
        engine.apply_measured_to_block(self.SLOT, cost_incl=round(3.0*0.05493, 6),
                                       label="OFF_PEAK")
        self.assertIsNone(self._row()["imp_rate_ev"])

    def test_peak_bill_writes_measured_and_splits_exact(self):
        # 23rd bump: block currently off-peak; Octopus bills STANDARD £1.0294 / 3.186 kWh
        self._blk(3.186, 2.24, rate=0.05493)
        # 4.5.9: EV quantity is bounded by the car's completed dispatch (a boost lands in the
        # completed feed). Without a session the slot is house-only (test_no_dispatch_stays_house_only).
        self.st.record_dispatch_history(self.SLOT, "completed", source="unknown", energy_kwh=-2.24)
        ok = engine.apply_measured_to_block(
            self.SLOT, cost_incl=1.029372, cost_excl=0.980355, label="STANDARD_RATE")
        self.assertTrue(ok)
        r = self._row()
        self.assertEqual(r["rate_source"], "measured")
        self.assertAlmostEqual(r["imp_rate"], round(1.029372/3.186, 6), places=6)
        self.assertAlmostEqual(r["imp_cost"], 1.029372, places=6)
        self.assertEqual(r["imp_ev_band"], "peak")       # cost ~0.323 → peak band
        self.assertEqual(r["imp_home_band"], "day")
        self.assertAlmostEqual(r["imp_rate_exc"], round(0.980355/3.186, 6), places=6)
        segs = [ps.Segment(**x) for x in
                self.st.get_block_segments(self.SLOT, "electricity_main")]
        self.assertAlmostEqual(ps.total_cost(segs), 1.029372, places=5)   # sums to the bill
        self.assertAlmostEqual(ps.attribution_cost(segs, "ev"),
                               round(2.24 * (1.029372/3.186), 6), places=5)

    def test_cost_beats_label(self):
        # label says STANDARD but the COST is the off-peak amount (25/08 23:00 signature).
        # Band must follow the COST (off_peak), not the label.
        self._blk(1.0, 0.5, rate=0.05493)
        engine.apply_measured_to_block(
            self.SLOT, cost_incl=0.05493, cost_excl=0.052314, label="STANDARD_RATE")
        r = self._row()
        self.assertEqual(r["imp_ev_band"], "off_peak")
        self.assertEqual(r["imp_home_band"], "off_peak")
        self.assertAlmostEqual(r["imp_rate"], 0.05493, places=6)

    def test_mixed_uses_4band_clean_no_rescale(self):
        # A genuine over-cap boundary slot (label='mixed'): keep the true 4-band split at the
        # schedule's CLEAN per-band rates — NOT rescaled to the bill (clean-rate invariant).
        # imp_cost stays the bill; the split reconciles to it via imp_cost_remainder.
        import iog_cap
        self._blk(3.0, 2.0, rate=0.05493)          # 2 kWh EV, 1 kWh house
        self.st.record_dispatch_history(self.SLOT, "completed", source="unknown", energy_kwh=-2.0)
        engine._kraken_rate_schedules = {
            "import": _Sched(),
            "ev_device_off_peak": _Sched(), "ev_device_peak": _Sched()}
        # EV 1@off_peak(0.055) + 1@peak(0.323), house 1@day(0.323) — CLEAN rates
        _raw = [(1.0, 0.055, "off_peak", "ev"),
                (1.0, 0.323, "peak", "ev"),
                (1.0, 0.323, "day", "house")]
        _save = iog_cap.compute_iog_split
        iog_cap.compute_iog_split = lambda *a, **k: {
            "segments": _raw, "classification": {"ev": "mixed", "house": "day"}}
        try:
            bill_incl, bill_excl = 0.900, 0.857
            ok = engine.apply_measured_to_block(
                self.SLOT, cost_incl=bill_incl, cost_excl=bill_excl, label="mixed")
        finally:
            iog_cap.compute_iog_split = _save
        self.assertTrue(ok)
        segs = [ps.Segment(**x) for x in
                self.st.get_block_segments(self.SLOT, "electricity_main")]
        self.assertEqual(len(segs), 3)
        self.assertEqual({s.band for s in segs}, {"off_peak", "peak", "day"})
        # CLEAN rates preserved (NOT rescaled) — every segment rate is a stub band value
        for sg in segs:
            self.assertIn(round(sg.inc_rate, 3), (0.055, 0.323))
        r = self._row()
        self.assertEqual(r["rate_source"], "measured")
        self.assertAlmostEqual(r["imp_cost"], bill_incl, places=6)   # cost stays the bill
        self.assertEqual(r["imp_ev_band"], "mixed")
        # EV cost = clean sum (1x0.055 + 1x0.323); house remainder absorbs the bill delta
        self.assertAlmostEqual(r["imp_cost_ev"], round(0.055 + 0.323, 6), places=6)
        self.assertAlmostEqual(r["imp_cost_remainder"], round(bill_incl - (0.055 + 0.323), 6), places=6)

    def test_no_kwh_is_noop(self):
        self._blk(0.0, 0.0, rate=0.05493)
        self.assertFalse(engine.apply_measured_to_block(
            self.SLOT, cost_incl=0.5, cost_excl=0.48, label="STANDARD_RATE"))
        self.assertEqual(self._row()["rate_source"], "reconciled")  # untouched


if __name__ == "__main__":
    unittest.main()


class TestMeasuredApplyDeviceRecost(unittest.TestCase):
    """Settlement re-costs devices from the GRID-ATTRIBUTED kWh, not the raw draw (#473).

    `apply_measured_to_block` re-costs the slot's sub-meters to the settled rate, because
    PASS 2 costs every device at its parent's rate and settlement moves that rate. It priced
    from raw `imp_kwh`, which silently re-based the cost onto a different quantity than PASS 2
    had used: a device that ran off solar or battery was billed as though the grid supplied
    all of it.

    That reaches the BILL, not just the device line. `compute_period_net` builds each day as
    `max(0, main - SUM(devices)) + SUM(devices)` — algebraically `main`, except the clamp
    stops the device terms cancelling once they exceed the main. So an over-costed device
    becomes the period total.

    The kWh COLUMNS were always correct, which is why this hid: the comment above the
    statement promises "kWh is untouched; only the priced rate layer moves", and that is true
    of the columns while being false of the cost.
    """

    SLOT = "2026-08-23T02:00:00"
    OFF = 0.05493          # the off-peak band _Sched returns, in £

    def setUp(self):
        self._save = (engine._store, engine._kraken_rate_schedules)
        self.st = BlockStore(":memory:")
        engine._store = self.st
        engine._kraken_rate_schedules = {"import": _Sched()}
        self.st._conn.execute(
            "INSERT OR IGNORE INTO config_periods (id, effective_from, billing_day, "
            "block_minutes, timezone) VALUES (1,'2020-01-01T00:00:00',1,30,'UTC')")
        # Main: 3 kWh settled, priced into the off-peak band. Deliberately started on the
        # PEAK rate so settlement genuinely moves it and the device re-cost fires.
        self.st._conn.execute(
            "INSERT INTO blocks (block_start, block_end, meter_id, config_period_id, "
            "imp_kwh, imp_kwh_api, imp_rate, imp_cost, rate_source) "
            "VALUES (?,?,?,?,?,?,?,?,?)",
            (self.SLOT, self.SLOT, "electricity_main", 1, 3.0, 3.0, 0.323092,
             round(3.0*0.323092, 6), "reconciled"))
        self.st._conn.commit()

    def tearDown(self):
        (engine._store, engine._kraken_rate_schedules) = self._save

    def _device(self, mid, kwh, kwh_grid):
        self.st._conn.execute(
            "INSERT INTO blocks (block_start, block_end, meter_id, config_period_id, "
            "imp_kwh, imp_kwh_grid, imp_rate, imp_cost) VALUES (?,?,?,?,?,?,?,?)",
            (self.SLOT, self.SLOT, mid, 1, kwh, kwh_grid, 0.323092,
             round((kwh if kwh_grid is None else kwh_grid) * 0.323092, 6)))
        self.st._conn.commit()

    def _settle(self):
        ok = engine.apply_measured_to_block(
            self.SLOT, cost_incl=round(3.0 * self.OFF, 6), label="OFF_PEAK")
        self.assertTrue(ok, "settlement did not apply")
        return ok

    def _dev_row(self, mid):
        return self.st._conn.execute(
            "SELECT imp_kwh, imp_kwh_grid, imp_rate, imp_rate_exc, imp_cost, imp_cost_exc "
            "FROM blocks WHERE block_start=? AND meter_id=?", (self.SLOT, mid)).fetchone()

    def test_device_cost_uses_grid_attributed_kwh(self):
        """4 kWh drawn, only 0.01 of it from the grid — bill the 0.01."""
        self._device("battery", 4.0, 0.01)
        self._settle()
        r = self._dev_row("battery")
        self.assertAlmostEqual(r["imp_rate"], self.OFF, places=6)        # rate did move
        self.assertAlmostEqual(r["imp_cost"], round(0.01 * self.OFF, 6), places=6)
        # and emphatically NOT the raw-kWh figure the old expression produced
        self.assertNotAlmostEqual(r["imp_cost"], round(4.0 * self.OFF, 6), places=4)

    def test_device_kwh_columns_are_untouched(self):
        self._device("battery", 4.0, 0.01)
        self._settle()
        r = self._dev_row("battery")
        self.assertEqual(r["imp_kwh"], 4.0)
        self.assertEqual(r["imp_kwh_grid"], 0.01)

    def test_exc_follows_the_same_quantity(self):
        """inc and exc must derive from the same kWh or the ex-VAT view disagrees."""
        self._device("battery", 4.0, 0.01)
        self._settle()
        r = self._dev_row("battery")
        self.assertIsNotNone(r["imp_rate_exc"])
        # exact: cost_exc is the SAME 0.01 kWh priced at the row's own ex-VAT rate
        self.assertAlmostEqual(r["imp_cost_exc"], round(0.01 * r["imp_rate_exc"], 6), places=6)
        self.assertNotAlmostEqual(r["imp_cost_exc"], round(4.0 * r["imp_rate_exc"], 6), places=4)

    def test_unclipped_device_still_costs_from_raw_kwh(self):
        """imp_kwh_grid NULL means PASS 2 never clipped — the COALESCE fallback.

        A production database carries thousands of these; re-costing them to zero would be a
        worse bug than the one being fixed.
        """
        self._device("nogrid", 0.5, None)
        self._settle()
        r = self._dev_row("nogrid")
        self.assertIsNone(r["imp_kwh_grid"])
        self.assertAlmostEqual(r["imp_cost"], round(0.5 * self.OFF, 6), places=6)

    def test_devices_no_longer_exceed_the_main(self):
        """The property that makes the bill safe: with costs clipped, SUM(devices) <= main,
        so the per-day clamp in compute_period_net cannot fire."""
        self._device("battery", 4.0, 0.01)
        self._device("ev", 2.5, 0.40)
        self._settle()
        main = self.st._conn.execute(
            "SELECT imp_cost FROM blocks WHERE block_start=? AND meter_id='electricity_main'",
            (self.SLOT,)).fetchone()["imp_cost"]
        devs = self.st._conn.execute(
            "SELECT COALESCE(SUM(imp_cost),0) s FROM blocks WHERE block_start=? "
            "AND meter_id != 'electricity_main'", (self.SLOT,)).fetchone()["s"]
        self.assertLessEqual(devs, main + 1e-9)
        # the raw-kWh forms would NOT have fitted — that is the defect, stated as arithmetic
        self.assertGreater(round(4.0*self.OFF, 6) + round(2.5*self.OFF, 6), main)
