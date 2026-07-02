"""
test_pv_solar_server.py
=======================
Server-side coverage for the 3.0.5 battery PV/solar dial feature.

Two independent code paths gained PV support and had no tests:

* the Overview route's ``has_sub_devices`` flag now counts ``pv_power_sensor``,
  so a battery whose only sensor is PV still reveals the sub-device cards even
  on an API-only setup with no main power gauge (the regression the flag fixed);
* ``_build_soc_response`` reads the PV entity into ``pv_kw`` with the PV contract
  — always positive (no invert), auto unit (no override).

Reuses the stub harness and Flask test client from test_server.py.

Run with:
    python3 -m pytest test_pv_solar_server.py -v
"""

import os
import sys
import unittest
from unittest.mock import patch, MagicMock

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import test_server as TS          # module-level: installs stubs, imports server
import server

BlockStore  = TS.BlockStore
make_client = TS.make_client


class TestHasSubDevicesGating(unittest.TestCase):
    """The sub-device cards container is gated on has_power_sensor OR
    has_sub_devices; pv_power_sensor must satisfy the latter."""

    def _cfg(self, *, main_power=False, sub_pv=False, sub_soc=False):
        base = {"billing_day": 1, "block_minutes": 30, "timezone": "Europe/London",
                "currency_symbol": "£"}
        main_meta = dict(base)
        if main_power:
            main_meta["power_sensor"] = "sensor.house_power"
        batt_meta = dict(base, **{
            "sub_meter": True, "parent_meter": "electricity_main",
            "meter_type": "battery", "device": "House Battery"})
        if sub_pv:
            batt_meta["pv_power_sensor"] = "sensor.solax_pv_power"
        if sub_soc:
            batt_meta["soc_sensor"] = "sensor.batt_soc"
        return {"meters": {
            "electricity_main": {"meta": main_meta, "channels": {}},
            "sub_meter_batt":   {"meta": batt_meta, "channels": {"import": {}}},
        }}

    def _get(self, cfg):
        client = make_client(store=BlockStore(":memory:"))
        with patch.object(server, "load_config", return_value=cfg):
            return client.get("/live-power")

    def test_pv_only_battery_reveals_cards_without_main_power(self):
        """API-only (no main power sensor); a battery whose ONLY sensor is
        pv_power_sensor must still render the sub-device cards container."""
        r = self._get(self._cfg(main_power=False, sub_pv=True))
        self.assertEqual(r.status_code, 200)
        self.assertIn(b'id="soc-cards-container"', r.data)   # cards revealed
        self.assertNotIn(b'class="power-card"', r.data)      # and no main gauge

    def test_sub_meter_without_any_sensor_gates_cards_out(self):
        """A sub-meter with no soc/inverter/device/pv sensor and no main power
        must NOT render the container."""
        r = self._get(self._cfg(main_power=False, sub_pv=False, sub_soc=False))
        self.assertEqual(r.status_code, 200)
        self.assertNotIn(b'id="soc-cards-container"', r.data)

    def test_soc_battery_still_reveals(self):
        """Sanity: the pre-existing soc_sensor path still reveals the container."""
        r = self._get(self._cfg(main_power=False, sub_soc=True))
        self.assertEqual(r.status_code, 200)
        self.assertIn(b'id="soc-cards-container"', r.data)


class TestBuildSocResponsePv(unittest.TestCase):
    """_build_soc_response reads the PV entity into pv_kw with no invert and no
    unit override, independent of the soc/inverter fields."""

    def setUp(self):
        # engine is stubbed by test_server as a bare module; give it the shared
        # converter with a faithful W→kW + invert behaviour, and record calls so
        # we can assert the PV-specific contract (invert=False, override=None).
        self.calls = []

        def fake_power_value_to_kw(value, unit, override, invert):
            self.calls.append({"value": value, "unit": unit,
                               "override": override, "invert": invert})
            val = float(value)
            if unit and str(unit).lower() in ("w", "watt", "watts"):
                val /= 1000.0
            return round(val * (-1.0 if invert else 1.0), 3)

        sys.modules["engine"]._power_value_to_kw = fake_power_value_to_kw

    def _ha(self, states, attrs=None):
        attrs = attrs or {}
        c = MagicMock()
        c.get_state.side_effect = lambda e: states.get(e)
        c.get_attributes.side_effect = lambda e: attrs.get(e, {})
        return c

    def test_pv_kw_positive_no_invert_no_override(self):
        ha = self._ha({"sensor.pv": "8000"},
                      {"sensor.pv": {"unit_of_measurement": "W"}})
        soc = {"batt": {"label": "House Battery", "type": "battery",
                        "pv_entity": "sensor.pv"}}
        out = server._build_soc_response(soc, ha)["batt"]
        self.assertAlmostEqual(out["pv_kw"], 8.0)
        self.assertEqual(out["pv_entity"], "sensor.pv")
        pv_call = next(c for c in self.calls if c["value"] == "8000")
        self.assertFalse(pv_call["invert"], "PV must never be inverted")
        self.assertIsNone(pv_call["override"], "PV uses the declared unit, no override")

    def test_pv_kw_none_when_no_pv_entity(self):
        out = server._build_soc_response(
            {"batt": {"label": "B", "type": "battery"}}, self._ha({}))["batt"]
        self.assertIsNone(out["pv_kw"])
        self.assertIsNone(out["pv_entity"])

    def test_pv_kw_none_when_unavailable(self):
        ha = self._ha({"sensor.pv": "unavailable"})
        out = server._build_soc_response(
            {"batt": {"pv_entity": "sensor.pv"}}, ha)["batt"]
        self.assertIsNone(out["pv_kw"])

    def test_pv_independent_of_soc_and_inverter(self):
        ha = self._ha(
            {"sensor.soc": "83", "sensor.inv": "6000", "sensor.pv": "8000"},
            {"sensor.inv": {"unit_of_measurement": "W"},
             "sensor.pv":  {"unit_of_measurement": "W"}})
        soc = {"batt": {"soc_entity": "sensor.soc", "power_entity": "sensor.inv",
                        "pv_entity": "sensor.pv", "power_invert": False}}
        out = server._build_soc_response(soc, ha)["batt"]
        self.assertAlmostEqual(out["soc"], 83.0)
        self.assertAlmostEqual(out["power_kw"], 6.0)
        self.assertAlmostEqual(out["pv_kw"], 8.0)


if __name__ == "__main__":
    unittest.main()