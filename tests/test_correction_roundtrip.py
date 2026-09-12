"""4.5.11: a user Cost-Correction (rate_corrected=1 / rate_source='corrected') — and the
rate_reconciled flag — must SURVIVE a read -> modify -> append_block_replace round-trip.
They weren't carried onto the block dict nor written by the append path, so any round-trip
(gap-fill, carbon/remainder recompute, PASS 2 re-run) silently reset them to the column
defaults — un-protecting the correction, which the dispatch reconcile then reverted (the
negative-Home strand on SMB). Exposed when 4.5.7 removed the IOG import gate."""
import os, sys, unittest
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from block_store import BlockStore

OFF, PK = 0.05493, 0.323092


class TestCorrectionRoundTrip(unittest.TestCase):
    def setUp(self):
        self.st = BlockStore(":memory:")
        self.st.insert_config_period({"meters": {"electricity_main": {"meta": {
            "billing_day": 1, "block_minutes": 30, "timezone": "UTC",
            "currency_symbol": "£", "currency_code": "GBP", "site": "Home"}}}})
        self.cp = self.st._conn.execute("SELECT id FROM config_periods LIMIT 1").fetchone()[0]

    def _ins(self, slot, rate, *, rate_corrected=0, rate_source=None, rate_reconciled=0,
             kwh=3.0, ev_kwh=None, ev_rate=None):
        self.st._conn.execute(
            "INSERT INTO blocks (block_start, block_end, meter_id, config_period_id, imp_kwh, "
            "imp_rate, imp_cost, imp_kwh_ev, imp_rate_ev, imp_cost_ev, "
            "rate_corrected, rate_source, rate_reconciled) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (slot, slot, "electricity_main", self.cp, kwh, rate, round(kwh*rate, 6),
             ev_kwh, ev_rate, (round(ev_kwh*ev_rate, 6) if ev_kwh and ev_rate else None),
             rate_corrected, rate_source, rate_reconciled))
        self.st._conn.commit()

    def _flags(self, slot):
        return self.st._conn.execute(
            "SELECT rate_corrected, rate_source, rate_reconciled, imp_rate, imp_rate_ev "
            "FROM blocks WHERE block_start=? AND meter_id='electricity_main'", (slot,)).fetchone()

    def _roundtrip(self, slot):
        # read the block dict, then write it straight back (the exact round-trip shape)
        blocks = self.st.get_blocks_for_utc_range("2000-01-01T00:00:00", "2100-01-01T00:00:00")
        target = next(b for b in blocks if b["start"] == slot)
        self.st.append_block_replace(target, self.cp)

    def test_corrected_block_survives_roundtrip(self):
        slot = "2026-08-30T12:00:00"
        self._ins(slot, PK, rate_corrected=1, rate_source="corrected",
                  kwh=5.276, ev_kwh=3.3, ev_rate=PK)
        self._roundtrip(slot)
        r = self._flags(slot)
        self.assertEqual(r["rate_corrected"], 1)          # correction survives
        self.assertEqual(r["rate_source"], "corrected")   # authority survives
        self.assertAlmostEqual(r["imp_rate"], PK, places=5)
        self.assertAlmostEqual(r["imp_rate_ev"], PK, places=5)

    def test_reconciled_flag_survives_roundtrip(self):
        slot = "2026-08-30T13:00:00"
        self._ins(slot, OFF, rate_reconciled=1, rate_source="measured")
        self._roundtrip(slot)
        r = self._flags(slot)
        self.assertEqual(r["rate_reconciled"], 1)
        self.assertEqual(r["rate_source"], "measured")

    def test_fresh_block_defaults_unchanged(self):
        slot = "2026-08-30T14:00:00"
        self._ins(slot, OFF)                              # uncorrected, schedule
        self._roundtrip(slot)
        r = self._flags(slot)
        self.assertEqual(r["rate_corrected"], 0)
        self.assertIsNone(r["rate_source"])
        self.assertEqual(r["rate_reconciled"], 0)


if __name__ == "__main__":
    unittest.main()
