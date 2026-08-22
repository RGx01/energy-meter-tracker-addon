"""
Phase-1 Δ2: the reprice-based capped fixture builder (tests/fixture_capped.py) turns a real
uncapped DB into an authoritative capped account THROUGH reprice_block. Hermetic: builds a
tiny in-memory IOG account with out-of-window dispatch crossing a 0.75 h cap, so it exercises
the within-cap freebie, the boundary blend, and over-cap peak — and asserts every rewritten
block reconciles (Σ segment kWh/cost == the block totals).
"""
import os, sys, unittest
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from block_store import BlockStore
import fixture_capped as fx

_BOOLS = ("interpolated standing_charge imp_provisional is_provisional needs_pass2_rerun "
          "needs_review rate_corrected rate_reconciled finalised_from_cad review_dismissed").split()


def _store():
    st = BlockStore(":memory:"); c = st._conn
    c.execute("INSERT OR IGNORE INTO config_periods (id, effective_from, billing_day, "
              "block_minutes, timezone, currency_symbol, currency_code) "
              "VALUES (1,'2020-01-01T00:00:00',1,30,'Europe/London','£','GBP')")
    c.execute("INSERT OR IGNORE INTO meters (meter_id, config_period_id, is_sub_meter) "
              "VALUES ('electricity_main',1,0)")
    # four out-of-window dispatched slots (14:00–16:00 BST = 13:00–15:00 UTC), 3.0 kWh EV each
    from datetime import datetime, timedelta
    slots = ["2026-08-15T13:00:00", "2026-08-15T13:30:00",
             "2026-08-15T14:00:00", "2026-08-15T14:30:00"]
    colnames = ["block_start", "block_end", "meter_id", "config_period_id",
                "imp_kwh", "imp_kwh_ev", "imp_rate_ev"] + _BOOLS
    cols = ", ".join(colnames)
    ph = ", ".join(["?"] * len(colnames))
    for s in slots:
        e = (datetime.fromisoformat(s) + timedelta(minutes=30)).isoformat()
        vals = [s, e, "electricity_main", 1, 3.5, 3.0, 0.05493] + [0] * len(_BOOLS)  # off-peak → SMART
        c.execute(f"INSERT INTO blocks ({cols}) VALUES ({ph})", vals)
        c.execute("INSERT INTO dispatch_history (slot_start, kind, raw_start, raw_end, "
                  "energy_kwh, first_seen, last_seen) VALUES (?,?,?,?,?,?,?)",
                  (s, "completed", s, e, -3.0, s, s))
    st._conn.commit()
    return st, slots


class TestFixtureCapped(unittest.TestCase):
    def test_builder_reconciles_and_engages_cap(self):
        st, slots = _store()
        n = fx.build_capped_via_reprice(st, cap_hours=0.75)   # 4 slots × 0.5h charged → breach
        self.assertEqual(n, 4)
        c = st._conn
        bands = {}
        for s in slots:
            r = c.execute("SELECT imp_kwh, imp_cost, imp_ev_band, imp_home_band FROM blocks "
                          "WHERE block_start=?", (s,)).fetchone()
            segs = c.execute("SELECT kwh, inc_rate FROM block_segments WHERE block_start=? "
                             "AND meter_id='electricity_main'", (s,)).fetchall()
            self.assertTrue(segs, f"no segments for {s}")
            sk = sum(x["kwh"] for x in segs); sc = sum(x["kwh"] * x["inc_rate"] for x in segs)
            self.assertAlmostEqual(sk, r["imp_kwh"], places=4)            # Σ kWh reconciles
            self.assertLess(abs(sc - r["imp_cost"]), 1e-3)               # Σ cost reconciles
            bands[s] = r["imp_ev_band"]
        # within-cap freebie → off_peak; the cap breaks mid-block → a mixed boundary; then peak
        self.assertEqual(bands[slots[0]], "off_peak")
        self.assertIn("mixed", bands.values())                          # boundary block present
        self.assertEqual(bands[slots[3]], "peak")                       # over-cap EV peak

    def test_bump_block_stays_peak(self):
        # a completed charge with finalised imp_rate_ev = PEAK (bump / no smart plan) must stay
        # peak — it never becomes a within-cap freebie, and never consumes the cap allowance.
        st, slots = _store()
        # flip the first slot to a bump: peak imp_rate_ev
        st._conn.execute("UPDATE blocks SET imp_rate_ev=0.323092 WHERE block_start=?", (slots[0],))
        st._conn.commit()
        fx.build_capped_via_reprice(st, cap_hours=0.75)
        r = st._conn.execute("SELECT imp_ev_band, imp_rate_ev FROM blocks WHERE block_start=?",
                             (slots[0],)).fetchone()
        self.assertEqual(r["imp_ev_band"], "peak")
        self.assertAlmostEqual(r["imp_rate_ev"], 0.323092, places=5)

    def test_house_is_peak_over_cap_out_of_window(self):
        st, slots = _store()
        fx.build_capped_via_reprice(st, cap_hours=0.75)
        # the last (over-cap, out-of-window) slot: house band withdrawn to day
        r = st._conn.execute("SELECT imp_home_band FROM blocks WHERE block_start=?",
                             (slots[3],)).fetchone()
        self.assertEqual(r["imp_home_band"], "day")


if __name__ == "__main__":
    unittest.main()
