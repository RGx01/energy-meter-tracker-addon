"""
4.5.21 heal — a device on a manually corrected block takes the main's corrected ex-VAT rate.

Before 4.5.21 the corrections route carried only the inc rate and cost onto devices, so a device
that already had an ex-VAT figure kept the pre-correction band (prod and prod-dev, 21 Jul 05:30
and 16:00). The route is fixed (tests/test_correction_device_exc.py); this heal repairs blocks
corrected before it. The fixture is that half-hour as 4.5.20 left it.

New code, so these exercise it rather than discriminate against 4.5.20.
"""
import asyncio
import os
import sys
import unittest
from unittest.mock import patch

sys.path.insert(0, os.path.dirname(__file__))
import test_rebuild_meter_type as H             # noqa: E402  (stubs; the real engine)
engine = H.engine

PEAK, PEAK_EXC, OFF = 0.323092, 0.307707, 0.0549297
OFF_EXC = round(OFF / 1.05, 6)


def _store():
    st = H.BlockStore(":memory:")
    st.insert_config_period({"meters": {
        "electricity_main": {"meta": {"billing_day": 1, "block_minutes": 30, "timezone": "UTC",
                                      "currency_symbol": "£", "currency_code": "GBP"}},
        "house_battery": {"meta": {"sub_meter": True, "parent_meter": "electricity_main",
                                   "meter_type": "battery"}},
        "ev_charger": {"meta": {"sub_meter": True, "parent_meter": "electricity_main",
                                "meter_type": "ev"}},
    }})
    cp = st.get_current_config_period_id()

    def row(bs, meter, kwh, grid, rate, exc, corrected=0):
        st._conn.execute(
            "INSERT INTO blocks (block_start, block_end, meter_id, config_period_id, interpolated, "
            "imp_kwh, imp_kwh_grid, imp_rate, imp_cost, imp_rate_exc, imp_cost_exc, exp_kwh, "
            "exp_cost, rate_corrected, rate_source) VALUES (?, ?, ?, ?, 0, ?, ?, ?, ?, ?, ?, 0, 0, ?, ?)",
            (bs, bs, meter, cp, kwh, grid, rate, round((grid if grid is not None else kwh) * rate, 6),
             exc, round((grid if grid is not None else kwh) * exc, 6) if exc is not None else None,
             corrected, "corrected" if corrected else "schedule"))
    with st._conn:
        # A corrected half-hour as 4.5.20 left it: main right, devices' exc still at peak.
        row("2026-07-21T04:30:00", "electricity_main", 0.142, None, OFF, OFF_EXC, corrected=1)
        row("2026-07-21T04:30:00", "house_battery", 0.002, 0.002, OFF, PEAK_EXC)
        row("2026-07-21T04:30:00", "ev_charger", 0.1, 0.1, OFF, PEAK_EXC)
        # A device whose exc is NULL (falls back to inc ÷ VAT): left alone.
        row("2026-07-21T15:00:00", "electricity_main", 0.203, None, OFF, OFF_EXC, corrected=1)
        row("2026-07-21T15:00:00", "house_battery", 0.001, 0.001, OFF, None)
        # An uncorrected peak half-hour: nothing to repair.
        row("2026-07-21T16:00:00", "electricity_main", 0.15, None, PEAK, PEAK_EXC)
        row("2026-07-21T16:00:00", "house_battery", 0.003, 0.003, PEAK, PEAK_EXC)
    return st


def _exc(st, bs, meter):
    return st._conn.execute("SELECT imp_rate_exc, imp_cost_exc FROM blocks WHERE block_start=? "
                            "AND meter_id=?", (bs, meter)).fetchone()


class TheHeal(unittest.TestCase):

    def test_corrected_devices_take_the_mains_exc(self):
        st = _store()
        res = engine._corrected_device_exc_core(st)
        self.assertEqual(res["repaired"], 2)
        for meter, grid in (("house_battery", 0.002), ("ev_charger", 0.1)):
            with self.subTest(meter=meter):
                e = _exc(st, "2026-07-21T04:30:00", meter)
                self.assertAlmostEqual(e["imp_rate_exc"], OFF_EXC, places=6)
                self.assertAlmostEqual(e["imp_cost_exc"], round(grid * OFF_EXC, 6), places=6)

    def test_a_null_exc_and_an_uncorrected_block_are_left_alone(self):
        st = _store()
        engine._corrected_device_exc_core(st)
        self.assertIsNone(_exc(st, "2026-07-21T15:00:00", "house_battery")["imp_rate_exc"])
        self.assertAlmostEqual(_exc(st, "2026-07-21T16:00:00", "house_battery")["imp_rate_exc"],
                               PEAK_EXC, places=6)

    def test_it_is_idempotent(self):
        st = _store()
        engine._corrected_device_exc_core(st)
        self.assertEqual(engine._corrected_device_exc_core(st)["repaired"], 0)

    def test_the_run_marks_itself_done(self):
        st = _store()
        with patch.object(engine, "_store", st):
            self.assertEqual(asyncio.run(engine.run_corrected_device_exc_heal())["repaired"], 2)
            self.assertTrue(st.get_kraken_state(engine._CORRECTED_DEVICE_EXC_DONE_KEY))
            self.assertEqual(asyncio.run(engine.run_corrected_device_exc_heal()).get("skipped"),
                             "already done")


if __name__ == "__main__":
    unittest.main()
