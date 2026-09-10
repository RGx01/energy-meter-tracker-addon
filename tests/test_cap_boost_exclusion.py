"""Fix C (4.5.7): BlockStore.cap_day_boundary must NOT let bump/boost energy advance the
6-hour cap. A peak-billed boost can't also consume the off-peak allowance (4-rate rules).
The boundary is the SINGLE source both settlement (_iog_cap_day_boundary) and the day
chart's held-peak read, so excluding boosts here fixes both at once."""
import os, sys, tempfile, unittest
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from block_store import BlockStore
import iog_cap

TZ = "Europe/London"


class TestCapBoostExclusion(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False); self.tmp.close()
        self.store = BlockStore(self.tmp.name)
        self._cap_saved = iog_cap.CAP_HOURS
        iog_cap.CAP_HOURS = 0.1        # tiny cap so a single completed slot reaches it

    def tearDown(self):
        iog_cap.CAP_HOURS = self._cap_saved
        try: os.unlink(self.tmp.name)
        except OSError: pass

    def _completed(self, slot, energy, *, boost=False):
        end = slot[:11] + ("%02d:%02d:00" % divmod((int(slot[11:13]) * 60 + int(slot[14:16]) + 30) % 1440, 60))
        with self.store._conn:
            self.store._conn.execute(
                "INSERT INTO dispatch_history (slot_start, kind, provider, source, energy_kwh, "
                "first_seen, last_seen, raw_start, raw_end) VALUES (?,'completed','T',NULL,?,?,?,?,?)",
                (slot, -energy, slot, slot, slot, end))
            if boost:
                self.store._conn.execute(
                    "INSERT INTO dispatch_history (slot_start, kind, provider, source, energy_kwh, "
                    "first_seen, last_seen, raw_start, raw_end) VALUES (?,'planned','T','boost',?,?,?,?,?)",
                    (slot, -energy, slot, slot, slot, end))

    def test_smart_charge_reaches_cap_boost_does_not(self):
        # A single smart-charge slot reaches the (tiny) cap -> boundary set.
        self._completed("2026-09-09T14:00:00", 1.0)
        b = self.store.cap_day_boundary("2026-09-09T14:00:00", TZ)
        self.assertIsNotNone(b)
        self.assertEqual(b[0], "2026-09-09T14:00:00")

    def test_boost_energy_excluded_from_cap(self):
        # The SAME energy delivered as a BOOST is excluded -> the cap is never reached.
        self._completed("2026-09-09T14:00:00", 1.0, boost=True)
        b = self.store.cap_day_boundary("2026-09-09T14:00:00", TZ)
        self.assertIsNone(b)          # boost billed peak, must not eat the off-peak allowance


if __name__ == "__main__":
    unittest.main()
