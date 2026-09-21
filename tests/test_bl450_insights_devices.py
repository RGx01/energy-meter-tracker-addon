"""#450 — Insights shows every configured device, not one per type.

The Usage tab had three fixed cards (battery / EV / heat pump). The server has
always keyed `sub_meters` by meter id and sent every device, but the template
wrote all of them into the same three element ids, so the LAST device of each
type won and a second heat pump never appeared.

Two halves are pinned here: that the server really does return every device
(including two of one type, and the synthetic EV), and that the template no
longer carries the fixed per-type ids that caused the loss.
"""
import os
import re
import sys
import tempfile
import types
import unittest
from unittest.mock import MagicMock

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

_hc = types.ModuleType("ha_client"); _hc.HAClient = MagicMock
sys.modules.setdefault("ha_client", _hc)

import importlib.util as _ilu

_HERE = os.path.dirname(os.path.abspath(__file__))
_TPL = os.path.join(_HERE, "templates", "insights.html")


def _load_server():
    spec = _ilu.spec_from_file_location(
        "srv_450", os.path.join(_HERE, "web", "server.py"))
    mod = _ilu.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class TestServerSendsEveryDevice(unittest.TestCase):
    """The data half. If this breaks, no template change can help."""

    def setUp(self):
        from block_store import BlockStore
        self.path = tempfile.mktemp(suffix=".db")
        self.store = BlockStore(self.path)
        self.store._conn.execute("PRAGMA foreign_keys=OFF")
        self.store._conn.execute(
            "INSERT INTO config_periods (id,effective_from,billing_day,block_minutes,timezone) "
            "VALUES (1,'2026-01-01',1,30,'UTC')")
        # The reporting user's shape: battery, EV charger, and TWO heat pumps.
        self.devs = [("electricity_main", 0, None),
                     ("solax",    1, "battery"),
                     ("ohme",     1, "ev"),
                     ("stylish",  1, "heat_pump"),
                     ("altherma", 1, "heat_pump")]
        for mid, sub, mt in self.devs:
            self.store._conn.execute(
                "INSERT INTO meters (config_period_id,meter_id,is_sub_meter,meter_type,device_label) "
                "VALUES (1,?,?,?,?)", (mid, sub, mt, mid.title()))
        for i in range(4):
            slot = "2026-03-01T%02d:00:00" % i
            end = slot[:-5] + "30:00"
            self.store._conn.execute(
                "INSERT INTO blocks (block_start,block_end,config_period_id,meter_id,"
                "imp_kwh,imp_cost,imp_rate) VALUES (?,?,1,'electricity_main',10.0,3.0,0.30)",
                (slot, end))
            for mid, kwh in (("solax", 2.0), ("ohme", 3.0), ("stylish", 0.5), ("altherma", 1.5)):
                self.store._conn.execute(
                    "INSERT INTO blocks (block_start,block_end,config_period_id,meter_id,"
                    "imp_kwh,imp_cost,imp_rate) VALUES (?,?,1,?,?,?,0.30)",
                    (slot, end, mid, kwh, kwh * 0.30))
        self.store._conn.commit()
        self.cfg = {"meters": {mid: {"meta": {"sub_meter": bool(sub), "meter_type": mt,
                                              "device": mid.title()}}
                               for mid, sub, mt in self.devs}}

    def tearDown(self):
        try:
            self.store.close(); os.unlink(self.path)
        except Exception:
            pass

    def test_both_heat_pumps_are_returned(self):
        srv = _load_server()
        out = srv._aggregate_usage(self.store, self.cfg, "2026-03-01", "2026-03-02", "UTC")
        subs = out["sub_meters"]
        self.assertEqual(len(subs), 4, "all four devices must reach the client: %s" % sorted(subs))
        hps = [k for k, v in subs.items() if v.get("meter_type") == "heat_pump"]
        self.assertEqual(sorted(hps), ["altherma", "stylish"],
                         "the second heat pump is the reported bug")

    def test_devices_are_keyed_by_id_not_type(self):
        """Two devices of one type must not collide — the failure mode the
        template reproduced with fixed per-type element ids."""
        srv = _load_server()
        out = srv._aggregate_usage(self.store, self.cfg, "2026-03-01", "2026-03-02", "UTC")
        subs = out["sub_meters"]
        self.assertNotEqual(subs["stylish"]["imp_kwh"], subs["altherma"]["imp_kwh"],
                            "distinct devices must carry distinct figures")
        self.assertEqual(subs["stylish"]["label"], "Stylish")
        self.assertEqual(subs["altherma"]["label"], "Altherma")


class TestTemplateHasNoPerTypeCards(unittest.TestCase):
    """The display half, asserted structurally — the fixed ids are what lost the
    devices, so their absence is the regression guard."""

    def setUp(self):
        with open(_TPL, encoding="utf-8") as fh:
            self.html = fh.read()

    def test_fixed_per_type_card_ids_are_gone(self):
        for dead in ('id="ucard-battery"', 'id="ucard-ev"', 'id="ucard-hp"',
                     'u-bat-kwh', 'u-ev-kwh', 'u-hp-kwh',
                     'u-bat-cost', 'u-ev-cost', 'u-hp-cost'):
            self.assertNotIn(dead, self.html,
                             "%s is a per-TYPE id; a second device of that type "
                             "would overwrite it (#450)" % dead)

    def test_container_exists(self):
        self.assertIn('id="ucard-devices"', self.html)

    def test_cards_are_built_per_meter_id(self):
        self.assertIn("uDeviceElId(mid)", self.html)
        self.assertIn("'u-dev-' + eid + '-kwh'", self.html)

    def test_unknown_meter_type_still_gets_a_card(self):
        """The old code had three branches and silently dropped anything else."""
        self.assertIn("title: 'Device'", self.html)


class TestCarbonTabIsPerDevice(unittest.TestCase):
    """The carbon cards kept their own model (grid-intensity attribution, the gas
    and grid-average offsets) — only WHICH devices get one has changed. They are
    rich bespoke markup, so the static card is cloned per device with its ids
    suffixed, rather than rebuilt in JS."""

    def setUp(self):
        with open(_TPL, encoding="utf-8") as fh:
            self.html = fh.read()

    def test_no_first_device_wins_lookups_remain(self):
        """`.find()` over sub_meters is the exact construct that hid Altherma."""
        self.assertNotIn("sub_meters || {}).find(", self.html)
        self.assertNotIn("Object.values(d.sub_meters).find(", self.html)
        self.assertNotIn("Object.values(c.sub_meters).find(", self.html)

    def test_cards_are_cloned_per_device(self):
        self.assertIn("function carbonCardsFor(", self.html)
        for proto in ("'card-hp'", "'card-battery'", "'card-ev'"):
            self.assertIn("carbonCardsFor(" + proto, self.html)

    def test_prototypes_are_never_shown(self):
        self.assertIn("proto.style.display = 'none'", self.html)
        self.assertIn("data-clone-of", self.html,
                      "clones must be identifiable so a re-render clears them")

    def test_synthetic_ev_path_still_reachable(self):
        """A device-less IOG account has no meter id to clone against, so that
        path populates the prototype directly and must survive."""
        self.assertIn("if (!evCarbonDevs.length && d.ev_carbon) {", self.html)

    def test_ev_clones_cleared_even_with_no_ev_devices(self):
        """carbonCardsFor must be called unconditionally, or a card would linger
        after moving to a period with no EV."""
        i = self.html.index("var evCarbonDevs")
        j = self.html.index("if (!evCarbonDevs.length && d.ev_carbon) {")
        self.assertIn("carbonCardsFor('card-ev', evCarbonDevs", self.html[i:j])

    def test_carbon_model_is_unchanged(self):
        """Guard the offsetting comparisons the carbon tab exists for."""
        for kept in ("gas_co2_g_per_kwh", "gas_boiler_efficiency", "hp_cop",
                     "Grid crossover intensity", "avg_charge_intensity"):
            self.assertIn(kept, self.html)

    def test_battery_narrative_sums_across_batteries(self):
        self.assertIn("function _batAgg(src)", self.html)
        self.assertIn("iw / ik", self.html)   # kWh-weighted intensity, not a plain mean


class TestDeviceElementIds(unittest.TestCase):
    """uDeviceElId must not collide: element ids are a flat namespace and two
    meters differing only in a punctuation character would land on one card."""

    @staticmethod
    def _el_id(mid):
        safe = re.sub(r"[^A-Za-z0-9_-]", "-", mid)
        h = 0
        for ch in mid:
            h = ((h << 5) - h + ord(ch)) & 0xFFFFFFFF
            if h >= 0x80000000:
                h -= 0x100000000
        return "%s-%x" % (safe, h & 0xFFFFFFFF)

    def test_punctuation_variants_do_not_collide(self):
        self.assertNotEqual(self._el_id("a.b"), self._el_id("a-b"))

    def test_id_is_dom_safe(self):
        for mid in ("ev charger", "heat/pump", "solax.battery", "Ohme #1"):
            self.assertRegex(self._el_id(mid), r"^[A-Za-z0-9_-]+$")


if __name__ == "__main__":
    unittest.main()
