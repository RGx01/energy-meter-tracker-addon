"""
4.5.21 — on a DISPATCH-ONLY account the devices fit inside the house (grid − the car).

An account with Octopus's dispatch EV on the main (`kwh_ev`) and no EV meter: PASS 2 fitted the
devices against the WHOLE main, never reading `kwh_ev`, and nothing re-fitted them once the
completed dispatch arrived. A battery charging alongside the car kept grid the car used; the EV
then had nothing left to sit in (the bill capped it, Insights' house went negative).

Now the car's grid is reserved first and the devices fit in what is left; the stored remainder
keeps its shape (grid − devices, the EV inside it) and can no longer be smaller than the EV. Each
later writer of `kwh_ev` re-fits the devices (`_refit_devices_to_house`), keeping a measured
block's bill-set £ remainder. With an EV meter the meter is the EV and nothing changes.

No local database is a dispatch-only account with devices, so these fixtures are synthetic.
Discriminating: test_the_car_is_reserved_before_the_devices fails on 4.5.20; the re-fit tests
exercise new code. The EV-meter and no-dispatch cases are guards.
"""
import asyncio
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(__file__))
import test_rebuild_meter_type as H              # noqa: E402  (stubs + harness)
engine = H.engine

MAIN, BATT, EV = 8.0, 7.0, 2.86                  # the car drew 2.86; the battery 7.0; grid 8.0


def _block(ev_kwh=EV, ev_meter_type=None):
    """A block dict: the main carries the dispatch EV; a battery; optionally an EV meter."""
    meters = {
        "electricity_main": {
            "meta": {"sub_meter": False},
            "channels": {"import": {"kwh": MAIN, "kwh_total": MAIN, "rate": 0.245,
                                    "cost": round(MAIN * 0.245, 6), "kwh_ev": ev_kwh}}},
        "house_battery": {
            "meta": {"sub_meter": True, "meter_type": "battery", "parent_meter": "electricity_main"},
            "channels": {"import": {"kwh": BATT, "rate": 0.245, "cost": round(BATT * 0.245, 6)}}},
    }
    if ev_meter_type:
        meters["ev_charger"] = {
            "meta": {"sub_meter": True, "meter_type": ev_meter_type, "parent_meter": "electricity_main"},
            "channels": {"import": {"kwh": EV, "rate": 0.245, "cost": round(EV * 0.245, 6)}}}
    return {"start": "2026-07-12T15:30:00", "end": "2026-07-12T16:00:00", "interpolated": False,
            "meters": meters}


def _grid(blk, mid):
    return blk["meters"][mid]["channels"]["import"]["kwh_grid"]


def _rem(blk):
    return blk["meters"]["electricity_main"]["channels"]["import"]["kwh_remainder"]


class TheSplit(unittest.TestCase):

    def test_the_car_is_reserved_before_the_devices(self):
        blk = _block()
        engine._apply_pass2(blk)
        self.assertAlmostEqual(_grid(blk, "house_battery"), MAIN - EV, places=6)
        self.assertAlmostEqual(_rem(blk), EV, places=6)        # grid − devices, the car inside it

    def test_an_ev_meter_is_the_ev(self):
        """GUARD: with an EV meter the meter claims first and the dispatch figure is not reserved
        again — unchanged from 4.5.20 (for an `ev`- or `ev_charger`-typed meter alike)."""
        for t in ("ev", "ev_charger"):
            with self.subTest(meter_type=t):
                blk = _block(ev_meter_type=t)
                engine._apply_pass2(blk)
                self.assertAlmostEqual(_grid(blk, "house_battery") + _grid(blk, "ev_charger"),
                                       MAIN, places=6)
                self.assertAlmostEqual(_rem(blk), 0.0, places=6)

    def test_no_dispatch_is_unchanged(self):
        """GUARD: no dispatch EV, nothing reserved."""
        blk = _block(ev_kwh=None)
        engine._apply_pass2(blk)
        self.assertAlmostEqual(_grid(blk, "house_battery"), BATT, places=6)
        self.assertAlmostEqual(_rem(blk), MAIN - BATT, places=6)

    def test_a_dispatch_larger_than_the_main_is_capped_at_it(self):
        blk = _block(ev_kwh=MAIN + 5)
        engine._apply_pass2(blk)
        self.assertAlmostEqual(_grid(blk, "house_battery"), 0.0, places=6)
        self.assertAlmostEqual(_rem(blk), MAIN, places=6)

    def test_the_battery_keeps_its_ha_reading(self):
        blk = _block()
        engine._apply_pass2(blk)
        imp = blk["meters"]["house_battery"]["channels"]["import"]
        self.assertEqual(imp["kwh"], BATT)
        self.assertAlmostEqual(imp["kwh_grid"] + imp["kwh_battery"], BATT, places=6)


def _battery_only():
    cfg = H._cfg()
    cfg["meters"].pop("ev_charger")
    return cfg


class TheRefit(H._Harness):
    """A block closes before its completed dispatch is known; the EV then arrives on the main."""

    def _make_cfg(self):
        return _battery_only()

    def _close_then_arrive(self, measured=False):
        blk = engine.create_block(H.datetime.fromisoformat(H.START), H.datetime.fromisoformat(H.END),
                                  30, seed_meters=True)
        for meter, lo, d in (("electricity_main", 100.0, MAIN), ("house_battery", 10.0, BATT)):
            blk["meters"][meter]["channels"]["import"]["reads"] = [
                {"ts": H.START, "value": lo}, {"ts": H.END, "value": round(lo + d, 6)}]
        engine.finalise_block(H._HA(), block_data=blk)
        with engine._store._conn:
            engine._store._conn.execute(
                "UPDATE blocks SET imp_kwh_ev = ?, imp_cost_ev = ROUND(? * imp_rate, 6)"
                + (", rate_source = 'measured', imp_cost_remainder = 1.2345" if measured else "")
                + " WHERE block_start = ? AND meter_id = 'electricity_main'", (EV, EV, H.START))

    def test_at_close_the_battery_takes_the_grid(self):
        """GUARD: before the EV is known there is nothing to reserve."""
        self._close_then_arrive()
        self.assertAlmostEqual(self._row("house_battery")["imp_kwh_grid"], BATT, places=4)

    def test_the_arrival_refits_the_battery_inside_the_house(self):
        self._close_then_arrive()
        res = engine._refit_devices_to_house(engine._store, [H.START])
        self.assertEqual(res["refit_blocks"], 1)
        batt, main = self._row("house_battery"), self._row("electricity_main")
        self.assertAlmostEqual(batt["imp_kwh_grid"], MAIN - EV, places=4)
        self.assertAlmostEqual(batt["imp_cost"], round((MAIN - EV) * batt["imp_rate"], 6), places=6)
        self.assertAlmostEqual(main["imp_kwh_remainder"], EV, places=4)
        self.assertAlmostEqual(batt["imp_kwh"], BATT, places=4)          # the reading is kept

    def test_a_measured_block_keeps_its_bill_remainder(self):
        self._close_then_arrive(measured=True)
        engine._refit_devices_to_house(engine._store, [H.START])
        row = engine._store._conn.execute(
            "SELECT imp_cost_remainder, imp_kwh_remainder FROM blocks WHERE block_start=? "
            "AND meter_id='electricity_main'", (H.START,)).fetchone()
        self.assertAlmostEqual(row["imp_cost_remainder"], 1.2345, places=6)
        self.assertAlmostEqual(row["imp_kwh_remainder"], EV, places=4)

    def test_it_is_idempotent(self):
        self._close_then_arrive()
        engine._refit_devices_to_house(engine._store, [H.START])
        self.assertEqual(engine._refit_devices_to_house(engine._store, [H.START])["refit_blocks"], 0)

    def test_the_heal_refits_history_and_marks_itself_done(self):
        self._close_then_arrive()
        res = asyncio.run(engine.run_dispatch_house_refit_heal())
        self.assertEqual(res["refit_blocks"], 1)
        self.assertTrue(engine._store.get_kraken_state(engine._DISPATCH_HOUSE_REFIT_DONE_KEY))
        self.assertEqual(asyncio.run(engine.run_dispatch_house_refit_heal()).get("skipped"),
                         "already done")


class AnAccountWithAnEvMeter(H._Harness):
    """GUARD: the meter is the EV — no dispatch-only candidates, nothing re-fitted."""

    def test_no_candidates(self):
        self._finalise()
        with engine._store._conn:
            engine._store._conn.execute("UPDATE blocks SET imp_kwh_ev = 2.0 "
                                        "WHERE meter_id = 'electricity_main'")
        self.assertEqual(engine._dispatch_house_candidates(engine._store), [])


if __name__ == "__main__":
    unittest.main()
