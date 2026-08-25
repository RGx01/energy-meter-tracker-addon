# -*- coding: utf-8 -*-
"""BL-40 (4.5.1): the physical-EV-meter identity must recognise a config-UI device typed
'ev' (not just 'ev_charger'), so the hybrid resolver folds it and it doesn't double against
the synthetic 'EV (from dispatch)'. Regression for the Indra case (meter_type='ev', hashed id)."""
import re, unittest
_src = open("energy_charts.py", encoding="utf-8").read()
_fn = re.search(r"(def _ev_meter_id\(cfg\):.*?\n    return None\n)", _src, re.S).group(1)
_ns = {}; exec(_fn, _ns)
ev_meter_id = _ns["_ev_meter_id"]

def _cfg(mid, mtype):
    return {"meters": {mid: {"meta": {"meter_type": mtype}}}}

class TestEvMeterId(unittest.TestCase):
    def test_indra_ev_type_hashed_id(self):   # the reported bug
        self.assertEqual(ev_meter_id(_cfg("sub_meter_1782651008694733", "ev")),
                         "sub_meter_1782651008694733")
    def test_ev_charger_type(self):
        self.assertEqual(ev_meter_id(_cfg("sub_meter_x", "ev_charger")), "sub_meter_x")
    def test_zappi_canonical_id(self):         # matched via substring even pre-fix
        self.assertEqual(ev_meter_id(_cfg("ev_charger", "ev")), "ev_charger")
    def test_battery_not_matched(self):
        self.assertIsNone(ev_meter_id(_cfg("sub_meter_1783243773736116", "battery")))
    def test_id_substring_fallback(self):
        self.assertEqual(ev_meter_id(_cfg("zappi_charger", "heat_pump")), "zappi_charger")
    def test_empty(self):
        self.assertIsNone(ev_meter_id({}))
        self.assertIsNone(ev_meter_id(None))

if __name__ == "__main__":
    unittest.main()
