"""
BL-42: load_current_block must repopulate each meter's meta from config.

Regression: load_current_block reconstructed the in-progress block's meters from
current_reads with an EMPTY meta ({}). On the api/Mini boundary-finalise path,
finalise_block/_apply_pass2 then saw sub_meter=False / parent_meter=None for a
properly-parented sub-meter, so the grid-authoritative device split silently
no-op'd and imp_kwh_remainder was left NULL (the Usage-Stats double-count on
provisional days). The CAD tick path masked it because capture_samples re-stamps
meta live; the settled path masked it because get_block_dict maps the parent.
This guards that a parented sub-meter's meta survives the current-block round-trip.
"""
import os, sys, unittest
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from block_store import BlockStore


class TestCurrentBlockMeta(unittest.TestCase):
    def _store(self):
        st = BlockStore(":memory:")
        with st._conn:
            cp = st._conn.execute(
                "INSERT INTO config_periods (effective_from, billing_day, block_minutes, "
                "timezone, currency_symbol, currency_code) "
                "VALUES ('2024-01-01T00:00:00',1,30,'UTC','£','GBP')").lastrowid
            st._conn.execute(
                "INSERT INTO meters (config_period_id, meter_id, is_sub_meter, meter_type) "
                "VALUES (?, 'electricity_main', 0, '')", (cp,))
            st._conn.execute(
                "INSERT INTO meters (config_period_id, meter_id, is_sub_meter, "
                "parent_meter_id, meter_type) "
                "VALUES (?, 'sub_battery', 1, 'electricity_main', 'battery')", (cp,))
        st._conn.commit()
        return st

    def _save_block(self, st):
        block = {
            "start": "2026-08-25T17:30:00",
            "end":   "2026-08-25T18:00:00",
            "interpolated": False,
            "_last_checkpoint": None,
            "meters": {
                "electricity_main": {"meta": {}, "channels": {"import": {
                    "reads": [{"ts": "2026-08-25T17:30:00", "value": 100.0},
                              {"ts": "2026-08-25T18:00:00", "value": 101.0}], "rates": []}}},
                "sub_battery": {"meta": {}, "channels": {"import": {
                    "reads": [{"ts": "2026-08-25T17:30:00", "value": 10.0},
                              {"ts": "2026-08-25T18:00:00", "value": 10.5}], "rates": []}}},
            },
        }
        st.save_current_block(block)

    def test_parented_sub_meta_survives_reload(self):
        st = self._store()
        self._save_block(st)
        blk = st.load_current_block()
        self.assertIn("sub_battery", blk["meters"])
        meta = blk["meters"]["sub_battery"]["meta"]
        # The fix: parented sub-meter carries its config meta after reload (was {} before → split skipped).
        self.assertTrue(meta.get("sub_meter"), "sub_meter flag lost on current-block reload")
        self.assertEqual(meta.get("parent_meter"), "electricity_main",
                         "parent_meter lost on current-block reload")
        # main is not flagged as a sub-meter
        self.assertFalse(blk["meters"]["electricity_main"]["meta"].get("sub_meter"))


if __name__ == "__main__":
    unittest.main()
