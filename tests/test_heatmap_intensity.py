"""
Heatmap carbon-intensity view: the gCO2/kWh cell is the grid's intensity (a
property of the grid at that time), not a usage-derived ratio. A zero-net block
must therefore still render its stored grid intensity instead of an empty cell.

These tests pin:
  * stored carbon_intensity_g is used directly when present (net != 0) — same
    number the old carbon_g/net path produced, so populated cells don't move;
  * a ZERO-NET block with stored intensity now renders a value (the fix);
  * pre-3.0.0 blocks without the stored column fall back to carbon_g/net;
  * a zero-net block with no intensity data at all stays empty (genuinely
    unknown — can't invent a grid intensity we never captured).
"""

import json
import os
import re
import sys
import unittest

# Add tests/ to the path so the in-test `from test_block_store import make_block`
# resolves (pytest runs with the repo root on the path, not tests/).
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from energy_charts import generate_net_heatmap

MID = "electricity_main"


def _block(start, import_kwh, export_kwh, carbon_g, carbon_intensity_g):
    meta = {"sub_meter": False, "block_minutes": 30}
    md = {"meta": meta, "carbon_g": carbon_g, "carbon_intensity_g": carbon_intensity_g}
    return {
        "start":  start,
        "totals": {"import_kwh": import_kwh, "export_kwh": export_kwh},
        "meters": {MID: md},
    }


def _intensity_z(html):
    m = re.search(r"var INTENSITY_Z\s*=\s*(\[.*?\]);", html, re.S)
    assert m, "INTENSITY_Z not found in heatmap output"
    return json.loads(m.group(1))


class TestHeatmapIntensity(unittest.TestCase):

    def setUp(self):
        # One day, 30-min slots → hh_index = (hour*60+min)//30
        self.blocks = [
            # A: net != 0, stored intensity present → use stored (==150); also
            #    equals carbon_g/net (750/5) so this proves "no visual change".
            _block("2025-05-17T02:00:00", 5.0, 0.0, 750.0, 150.0),   # hh 4
            # B: ZERO net, stored intensity present → THE FIX: renders 120, not None
            _block("2025-05-17T03:00:00", 2.0, 2.0,   0.0, 120.0),   # hh 6
            # C: pre-3.0.0 — no stored intensity, net != 0 → fall back to carbon_g/net (400/4=100)
            _block("2025-05-17T04:00:00", 4.0, 0.0, 400.0, None),    # hh 8
            # D: zero net AND no stored intensity → genuinely unknown → stays None
            _block("2025-05-17T05:00:00", 3.0, 3.0,   0.0, None),    # hh 10
        ]
        html = generate_net_heatmap(self.blocks, timezone_name="UTC", block_minutes=30)
        self.z = _intensity_z(html)
        self.assertEqual(len(self.z), 1, "expected exactly one day row")
        self.row = self.z[0]

    def test_stored_intensity_used_when_net_nonzero(self):
        self.assertEqual(self.row[4], 150.0)

    def test_zero_net_block_renders_stored_intensity(self):
        # The fix: previously None (0/0), now the grid intensity.
        self.assertEqual(self.row[6], 120.0)

    def test_pre_v3_block_falls_back_to_derived(self):
        self.assertEqual(self.row[8], 100.0)

    def test_zero_net_without_intensity_stays_empty(self):
        self.assertIsNone(self.row[10])

    def test_populated_cell_value_unchanged_vs_derived(self):
        # Block A: stored intensity (150) must equal the old carbon_g/net (750/5).
        self.assertEqual(self.row[4], round(750.0 / 5.0, 1))


class TestHeatmapIntensityIntegration(unittest.TestCase):
    """End-to-end: a net==0 block with a stored grid intensity must surface
    through the REAL fetch (get_blocks_lightweight) and fill its heatmap cell.
    The earlier unit test hand-built blocks and so missed that the lightweight
    fetch wasn't carrying carbon_intensity_g at all."""

    def setUp(self):
        from block_store import BlockStore
        self.store = BlockStore(":memory:")
        self.store.insert_config_period({"meters": {"electricity_main": {"meta": {
            "timezone": "UTC", "billing_day": 1, "block_minutes": 30,
            "currency_symbol": "£", "currency_code": "GBP"}}}})

    def tearDown(self):
        self.store.close()

    def test_zero_net_block_fills_via_real_fetch(self):
        from test_block_store import make_block
        blk = make_block("2025-05-17T12:00:00", imp_kwh=1.0, exp_kwh=1.0)  # net 0
        m = blk["meters"]["electricity_main"]
        m["carbon_g"] = 0.0
        m["carbon_intensity_g"] = 150.0
        self.store.append_block(blk)

        blocks = self.store.get_blocks_lightweight()
        self.assertEqual(len(blocks), 1)
        # 1) the fetch must carry the stored intensity (the part that was missing)
        self.assertEqual(
            blocks[0]["meters"]["electricity_main"]["carbon_intensity_g"], 150.0)

        # 2) and the heatmap cell for that net==0 block must be filled, not empty
        html = generate_net_heatmap(blocks, timezone_name="UTC", block_minutes=30)
        z = _intensity_z(html)
        self.assertEqual(len(z), 1)
        hh = (12 * 60) // 30  # 12:00 → slot 24
        self.assertEqual(z[0][hh], 150.0)


if __name__ == "__main__":
    unittest.main()