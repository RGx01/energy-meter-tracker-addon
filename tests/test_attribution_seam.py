"""
BL-49: device attribution must heal a deleted gap anywhere, not only before the
live-coverage seam. The seam filter was removed; correctness now rests on the
per-block guard in _write_device_into_block — fill an ABSENT row or a zero-hole,
never overwrite a real non-zero device reading. These pin that guard.
"""
import os, sys, unittest
from unittest import mock
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import engine


def _block(dev_kwh_existing=None):
    m = {"electricity_main": {"channels": {"import": {"kwh": 1.0, "rate": 0.30}}}}
    if dev_kwh_existing is not None:
        m["ev_charger"] = {"meta": {"sub_meter": True, "parent_meter": "electricity_main"},
                           "channels": {"import": {"kwh": dev_kwh_existing}}}
    return {"start": "2026-04-15T12:00:00", "meters": m}


class TestWriteDeviceGuard(unittest.TestCase):
    def setUp(self):
        st = mock.MagicMock()
        st.RECORDER_ATTRIBUTED_SOURCE = "recorder_attributed"
        self.p = [
            mock.patch.object(engine, "get_store", lambda: st),
            mock.patch.object(engine, "_apply_pass2", lambda b: None),
            mock.patch.object(engine, "_recompute_pass3_totals", lambda b: None),
            mock.patch.object(engine, "_recompute_block_carbon", lambda b: None),
            mock.patch.object(engine, "append_block_replace", lambda b: None),
        ]
        for x in self.p: x.start()

    def tearDown(self):
        for x in self.p: x.stop()

    def test_absent_device_is_written(self):        # a deleted gap → heal it
        b = _block(None)
        self.assertTrue(engine._write_device_into_block(b, "ev_charger", "electricity_main", 0.4))
        self.assertIn("ev_charger", b["meters"])

    def test_zero_hole_with_energy_is_overwritten(self):   # dropout → heal it
        b = _block(0.0)
        self.assertTrue(engine._write_device_into_block(b, "ev_charger", "electricity_main", 0.4))

    def test_real_reading_is_never_overwritten(self):      # live data → protected
        b = _block(0.5)
        self.assertFalse(engine._write_device_into_block(b, "ev_charger", "electricity_main", 0.4))
        self.assertEqual(b["meters"]["ev_charger"]["channels"]["import"]["kwh"], 0.5)

    def test_zero_hole_no_energy_left_alone(self):
        b = _block(0.0)
        self.assertFalse(engine._write_device_into_block(b, "ev_charger", "electricity_main", 0.0))


if __name__ == "__main__":
    unittest.main()
