"""BL: a NULL channel/meter_id row (legacy pre-NOT-NULL, or delete/re-add residue) became
a None dict key in config_from_db and crashed jsonify's key-sort on GET /api/config.
config_from_db must skip such rows; _write_meters must never persist one."""
import json, os, sys, unittest
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from block_store import BlockStore
_CH=["meter_id","channel","read_sensor","rate_sensor","standing_charge_sensor","rate_source","standing_charge_source","mpan","tariff"]
class T(unittest.TestCase):
    def _p(self, st):
        with st._conn:
            return st._conn.execute("INSERT INTO config_periods (effective_from, billing_day, block_minutes, timezone, currency_symbol, currency_code) VALUES ('2024-07-01T00:00:00',1,30,'Europe/London','£','GBP')").lastrowid
    def test_null_channel_skipped_and_serialisable(self):
        st=BlockStore(":memory:")
        with st._conn:
            st._conn.execute("DROP TABLE meter_channels")
            st._conn.execute("CREATE TABLE meter_channels ("+", ".join(c+" TEXT" for c in _CH)+")")
        cp=self._p(st)
        with st._conn:
            mid=st._conn.execute("INSERT INTO meters (config_period_id, meter_id, is_sub_meter) VALUES (?, 'electricity_main', 0)",(cp,)).lastrowid
            st._conn.execute("INSERT INTO meter_channels (meter_id, channel, read_sensor) VALUES (?, 'import','s')",(mid,))
            st._conn.execute("INSERT INTO meter_channels (meter_id, channel, read_sensor) VALUES (?, NULL,'j')",(mid,))
        cfg=st.config_from_db(cp)
        self.assertIn("import", cfg["meters"]["electricity_main"]["channels"])
        self.assertNotIn(None, cfg["meters"]["electricity_main"]["channels"].keys())
        json.dumps(cfg, sort_keys=True)
    def test_write_skips_falsy(self):
        st=BlockStore(":memory:"); cp=self._p(st)
        st._write_meters({"meters":{"electricity_main":{"meta":{},"channels":{"import":{"read":"s"},None:{"read":"j"},"":{"read":"j"}}},None:{"meta":{},"channels":{}},"":{"meta":{},"channels":{}}}}, cp)
        self.assertEqual([r["meter_id"] for r in st._conn.execute("SELECT meter_id FROM meters WHERE config_period_id=?",(cp,)).fetchall()], ["electricity_main"])
        self.assertEqual([r["channel"] for r in st._conn.execute("SELECT channel FROM meter_channels").fetchall()], ["import"])
if __name__=="__main__": unittest.main()
