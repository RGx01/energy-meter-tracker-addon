"""One-off IOG-SMB rate-repair migration (4.5.7; retires v5.0.0).

Re-derives the priced rate layer for SMB-era blocks from the FIXED schedule (the day-feed-gap
bug priced recent daytime off-peak). Preserves kWh + EV split; never touches a `corrected`
block; idempotent.
"""
import os, sys, tempfile, unittest
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from block_store import BlockStore
from kraken_rates import RateSchedule
import engine

OFF, PK = 5.49297, 32.30924        # pence
OFF_G, PK_G = round(OFF / 100, 6), round(PK / 100, 6)


def _sched():
    periods = []
    from datetime import datetime as DT, timedelta as TD
    d, end = DT(2026, 8, 1), DT(2026, 9, 12)
    while d < end:
        ds = d.strftime("%Y-%m-%d"); nx = (d + TD(days=1)).strftime("%Y-%m-%d")
        periods += [(f"{ds}T00:00:00", f"{ds}T05:30:00", OFF),
                    (f"{ds}T05:30:00", f"{ds}T23:30:00", PK),
                    (f"{ds}T23:30:00", f"{nx}T00:00:00", OFF)]
        d += TD(days=1)
    return RateSchedule(periods)


class TestSmbRateRepair(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        self.tmp.close()
        self.store = BlockStore(self.tmp.name)
        # Blocks all stored OFF-PEAK (the bug), source='schedule', settled.
        rows = [
            ("2026-09-09T02:00:00", 3.0, OFF_G, None, "schedule"),   # night → stays off
            ("2026-09-09T12:00:00", 0.0, OFF_G, None, "schedule"),   # daytime idle → PEAK
            ("2026-09-09T13:00:00", 0.5, OFF_G, None, "schedule"),   # daytime draw → PEAK
            ("2026-09-09T14:00:00", 0.2, OFF_G, None, "corrected"),  # user → MUST NOT change
        ]
        with self.store._conn:
            self.store._conn.execute(
                "INSERT INTO config_periods (id, effective_from, block_minutes, timezone) "
                "VALUES (1, '2026-08-01T00:00:00', 30, 'Europe/London')")
            for bs, kwh, rate, evk, src in rows:
                be = bs[:11] + ("%02d:%02d:00" % divmod((int(bs[11:13]) * 60 + int(bs[14:16]) + 30) % 1440, 60))
                self.store._conn.execute(
                    "INSERT INTO blocks (block_start, block_end, meter_id, config_period_id, "
                    "imp_kwh, imp_rate, imp_cost, imp_kwh_ev, rate_source, is_provisional, interpolated) "
                    "VALUES (?,?,?,1,?,?,?,?,?,0,0)",
                    (bs, be, "electricity_main", kwh, rate, round(kwh * rate, 6), evk, src))

    def tearDown(self):
        try: os.unlink(self.tmp.name)
        except OSError: pass

    def _rate(self, bs):
        return self.store._conn.execute(
            "SELECT imp_rate, imp_home_band FROM blocks WHERE block_start=?", (bs,)).fetchone()

    def test_daytime_flips_peak_night_stays_corrected_untouched(self):
        res = engine._smb_rate_repair_core(self.store, _sched(), "2026-08-26",
                                           vat_at=lambda s: 0.05)
        self.assertTrue(res["ok"])
        # daytime off→peak
        self.assertAlmostEqual(self._rate("2026-09-09T12:00:00")[0], PK_G, places=5)
        self.assertEqual(self._rate("2026-09-09T12:00:00")[1], "peak")
        self.assertAlmostEqual(self._rate("2026-09-09T13:00:00")[0], PK_G, places=5)
        # night unchanged (already correct)
        self.assertAlmostEqual(self._rate("2026-09-09T02:00:00")[0], OFF_G, places=5)
        # corrected block untouched
        self.assertAlmostEqual(self._rate("2026-09-09T14:00:00")[0], OFF_G, places=5)
        self.assertEqual(res["re_resolved"], 2)
        self.assertGreaterEqual(res["skipped"], 1)   # the corrected block

    def _add_dispatch(self, slot, *, energy=0.3, source_planned=None):
        """A completed smart-charge dispatch on `slot` (feeds the cap tally) plus, when
        given, a planned row carrying a bump/boost source (completed rows carry none)."""
        with self.store._conn:
            end = slot[:11] + ("%02d:%02d:00" % divmod((int(slot[11:13]) * 60 + int(slot[14:16]) + 30) % 1440, 60))
            self.store._conn.execute(
                "INSERT INTO dispatch_slots (slot_start, off_peak, provider, source, state, "
                "captured_at, energy_completed, raw_start, raw_end) "
                "VALUES (?,1,'Test','smart-charge','completed',?,?,?,?)",
                (slot, slot, -energy, slot, end))
            self.store._conn.execute(
                "INSERT INTO dispatch_history (slot_start, kind, provider, source, energy_kwh, first_seen, last_seen, raw_start, raw_end) "
                "VALUES (?,'completed','Test',NULL,?,?,?,?,?)", (slot, -energy, slot, slot, slot, end))
            if source_planned:
                self.store._conn.execute(
                    "INSERT INTO dispatch_history (slot_start, kind, provider, source, energy_kwh, first_seen, last_seen, raw_start, raw_end) "
                    "VALUES (?,'planned','Test',?,?,?,?,?,?)", (slot, source_planned, -energy, slot, slot, slot, end))

    def _add_block(self, bs, kwh, rate, src):
        be = bs[:11] + ("%02d:%02d:00" % divmod((int(bs[11:13]) * 60 + int(bs[14:16]) + 30) % 1440, 60))
        with self.store._conn:
            self.store._conn.execute(
                "INSERT INTO blocks (block_start, block_end, meter_id, config_period_id, "
                "imp_kwh, imp_rate, imp_cost, imp_kwh_ev, rate_source, is_provisional, interpolated) "
                "VALUES (?,?,?,1,?,?,?,NULL,?,0,0)",
                (bs, be, "electricity_main", kwh, rate, round(kwh * rate, 6), src))

    def test_within_cap_dispatch_keeps_offpeak_boost_stays_peak(self):
        # A daytime (post-05:30) SMART-charge slot within the 6h cap rides the off-peak
        # freebee -- the migration must NOT strip it to peak (the 05:30 dispatch bug). A
        # BOOST slot bills peak. (No cap-day here reaches 6h -> all smart-charge within cap.)
        self._add_block("2026-09-09T06:00:00", 0.5, PK_G, "schedule")   # mis-stored PEAK
        self._add_dispatch("2026-09-09T06:00:00")                       # within-cap smart charge
        self._add_block("2026-09-09T07:00:00", 0.5, OFF_G, "schedule")  # mis-stored OFF
        self._add_dispatch("2026-09-09T07:00:00", source_planned="boost")
        engine._smb_rate_repair_core(self.store, _sched(), "2026-08-26", vat_at=lambda s: 0.05)
        self.assertAlmostEqual(self._rate("2026-09-09T06:00:00")[0], OFF_G, places=5)  # freebee kept
        self.assertEqual(self._rate("2026-09-09T06:00:00")[1], "off_peak")
        self.assertAlmostEqual(self._rate("2026-09-09T07:00:00")[0], PK_G, places=5)   # boost -> peak
        self.assertEqual(self._rate("2026-09-09T07:00:00")[1], "peak")

    def test_preserves_kwh_and_is_idempotent(self):
        kwh_before = dict(self.store._conn.execute(
            "SELECT block_start, imp_kwh FROM blocks").fetchall())
        engine._smb_rate_repair_core(self.store, _sched(), "2026-08-26", vat_at=lambda s: 0.05)
        kwh_after = dict(self.store._conn.execute(
            "SELECT block_start, imp_kwh FROM blocks").fetchall())
        self.assertEqual(kwh_before, kwh_after)             # kWh untouched
        res2 = engine._smb_rate_repair_core(self.store, _sched(), "2026-08-26",
                                            vat_at=lambda s: 0.05)
        self.assertEqual(res2["re_resolved"], 0)            # nothing left to change
        self.assertEqual(res2["re_snapped"], 0)


if __name__ == "__main__":
    unittest.main()
