"""
Migration validation across TARIFF types — proves the reprice sweep segments non-IOG tariffs
faithfully, with special attention to Agile (per-slot prices, incl. PLUNGE / negative rates).

The sweep runs in INHERIT mode (`_reprice_history_block`): it reads each block's STORED
imp_rate/imp_cost and never re-resolves from a schedule, so per-slot Agile prices survive and
`segments_from_legacy` derives each segment rate from cost÷kWh (sign-agnostic). These tests
assert that end to end: every block segmented, one house segment per non-IOG block, the two
core invariants (Σ kwh == grid, Σ kwh×inc_rate == imp_cost), exc = inc×ratio incl. negatives,
and that the §4a unit guard has enough headroom to never mistake a legit Agile price for pence.
"""
import os, sys, unittest, asyncio
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import engine
from block_store import BlockStore

_VAT = 1.05
_RATIO = round(1.0 / _VAT, 8)


class _Exc:
    def __init__(self, v): self.v = v
    def resolve(self, ts): return self.v
    def flat_rate(self): return self.v

class _Sched:
    """Flat schedule only supplies the exc/inc RATIO + 'ready' signal; the sweep inherits each
    block's stored per-slot rate, so a flat sched is fine even for Agile."""
    def __init__(self, inc): self._inc = inc; self.exc = _Exc(round(inc * _RATIO, 8))
    def resolve(self, ts): return self._inc
    def flat_rate(self): return self._inc
    def is_empty(self): return False

# full-history rate segs — constant VAT ratio (Agile/flat/E7 all just VAT on inc)
_RATE_SEGS = [("2000-01-01T00:00:00", None, _Sched(0.30))]


def _mk_store():
    st = BlockStore(":memory:")
    with st._conn:
        cp = st._conn.execute(
            "INSERT INTO config_periods (effective_from, billing_day, block_minutes, "
            "timezone, currency_symbol, currency_code) "
            "VALUES ('2024-01-01T00:00:00',1,30,'UTC','£','GBP')").lastrowid
        st._conn.execute("INSERT INTO meters (config_period_id, meter_id, is_sub_meter, "
                         "meter_type) VALUES (?, 'electricity_main', 0, '')", (cp,))
    st._conn.commit()
    return st, cp


class _Base(unittest.TestCase):
    def setUp(self):
        self._s = engine._kraken_rate_schedules
        self._st_saved = engine._store
        self._bcs = engine._build_channel_rate_segs
        engine._kraken_rate_schedules = {"import": _Sched(0.30)}
        async def _fake(ch): return _RATE_SEGS
        engine._build_channel_rate_segs = _fake
        self.st, self._cp = _mk_store()
        engine._store = self.st

    def tearDown(self):
        engine._kraken_rate_schedules = self._s
        engine._store = self._st_saved
        engine._build_channel_rate_segs = self._bcs
        self.st._conn.close()

    def _blk(self, start, kwh, rate):
        cost = round(kwh * rate, 6)
        self.st._conn.execute(
            "INSERT INTO blocks (block_start, block_end, meter_id, config_period_id, imp_kwh, "
            "imp_rate, imp_cost, source, imp_kwh_api) VALUES (?,?, 'electricity_main', ?, ?, ?, ?, "
            "'imported_api', ?)", (start, start, self._cp, kwh, rate, cost, kwh))

    def _run_sweep(self):
        for _ in range(30):
            asyncio.run(engine._run_historical_reprice_sweep(batch=100))
            if (self.st.get_meta(engine._REPRICE_HISTORY_MARKER, {}) or {}).get("done"):
                break

    def _segs(self, start):
        return self.st._conn.execute(
            "SELECT kwh, inc_rate, exc_rate, band, attribution FROM block_segments "
            "WHERE block_start=? AND channel='import' ORDER BY seq", (start,)).fetchall()

    def _assert_block_faithful(self, start, kwh, rate):
        segs = self._segs(start)
        self.assertEqual(len(segs), 1, f"{start}: expected 1 house segment, got {len(segs)}")
        s = segs[0]
        self.assertEqual(s["attribution"], "house")
        self.assertAlmostEqual(s["kwh"], kwh, places=6)                 # Σ kwh == grid
        self.assertAlmostEqual(s["inc_rate"], rate, places=6)           # per-slot rate preserved
        self.assertAlmostEqual(s["kwh"] * s["inc_rate"], round(kwh*rate,6), places=6)  # Σ cost
        # exc = inc × VAT ratio, sign preserved
        self.assertAlmostEqual(s["exc_rate"], round(rate * _RATIO, 6), places=6)


class TestAgileWithPlunge(_Base):
    # a realistic Agile day slice: normal, high-near-cap, cheap, ZERO, and two PLUNGE slots
    SLOTS = [
        ("2025-01-10T16:00:00", 0.80, 0.2841),    # peak-ish
        ("2025-01-10T16:30:00", 1.20, 0.9550),    # near the £1/kWh Agile cap
        ("2025-01-10T02:00:00", 2.00, 0.0712),    # overnight cheap
        ("2025-01-10T13:00:00", 1.50, 0.0000),    # free
        ("2025-01-10T12:30:00", 3.00, -0.0500),   # plunge (paid to consume)
        ("2025-01-10T12:00:00", 2.50, -0.1520),   # deep plunge
    ]

    def test_agile_every_block_segmented_and_faithful(self):
        for s, k, r in self.SLOTS:
            self._blk(s, k, r)
        self.st._conn.commit()
        self.assertEqual(self.st.count_blocks_needing_reprice(), len(self.SLOTS))
        self._run_sweep()
        self.assertEqual(self.st.count_blocks_needing_reprice(), 0, "sweep left Agile blocks unpriced")
        for s, k, r in self.SLOTS:
            self._assert_block_faithful(s, k, r)

    def test_plunge_reconciles_negative_cost(self):
        s, k, r = "2025-01-10T12:00:00", 2.50, -0.1520
        self._blk(s, k, r); self.st._conn.commit()
        self._run_sweep()
        seg = self._segs(s)[0]
        self.assertLess(seg["inc_rate"], 0)                       # negative rate stored
        self.assertLess(seg["kwh"] * seg["inc_rate"], 0)          # negative cost (a credit)
        self.assertLess(seg["exc_rate"], 0)                       # negative exc too
        self.assertAlmostEqual(seg["exc_rate"], round(-0.1520 * _RATIO, 6), places=6)


class TestGuardHeadroomVsAgile(_Base):
    def test_near_cap_and_plunge_do_not_trip_unit_guard(self):
        # a legit Agile price near the cap and a deep plunge must NOT be mistaken for pence
        for rate in (0.955, -0.152, 0.99):
            ch = {"kwh": 1.0, "rate": rate, "cost": round(rate, 6)}
            self.assertFalse(engine._sanitise_inc_units(ch, "2025-01-10T00:00:00"),
                             f"guard wrongly fired on legit Agile rate {rate}")
            self.assertEqual(ch["rate"], rate)   # untouched


class TestFlatAndEconomy7(_Base):
    def test_flat_single_rate(self):
        for i in range(4):
            self._blk("2025-02-01T0%d:00:00" % i, 1.0 + i, 0.2700)
        self.st._conn.commit()
        self._run_sweep()
        self.assertEqual(self.st.count_blocks_needing_reprice(), 0)
        for i in range(4):
            self._assert_block_faithful("2025-02-01T0%d:00:00" % i, 1.0 + i, 0.2700)

    def test_economy7_two_rates_by_time(self):
        night, day = 0.1000, 0.4200
        self._blk("2025-02-02T02:00:00", 3.0, night)   # night slot
        self._blk("2025-02-02T18:00:00", 1.2, day)     # day slot
        self.st._conn.commit()
        self._run_sweep()
        self.assertEqual(self.st.count_blocks_needing_reprice(), 0)
        self._assert_block_faithful("2025-02-02T02:00:00", 3.0, night)
        self._assert_block_faithful("2025-02-02T18:00:00", 1.2, day)



class TestCosy(_Base):
    """Cosy Octopus — 3-band time-of-use (cheap / day / peak). Each half-hour is ONE rate for its
    window, so each block is a single house segment; three distinct rates across the day."""
    SLOTS = [
        ("2025-03-01T04:30:00", 2.0, 0.1200),   # cheap window
        ("2025-03-01T13:30:00", 1.5, 0.1200),   # cheap window (afternoon)
        ("2025-03-01T10:00:00", 1.0, 0.2800),   # day
        ("2025-03-01T17:30:00", 1.2, 0.4200),   # peak
    ]
    def test_cosy_three_bands_single_segment_each(self):
        for s, k, r in self.SLOTS: self._blk(s, k, r)
        self.st._conn.commit()
        self._run_sweep()
        self.assertEqual(self.st.count_blocks_needing_reprice(), 0)
        for s, k, r in self.SLOTS: self._assert_block_faithful(s, k, r)


class TestOctopusGo(_Base):
    """Octopus Go — 2-band: very cheap fixed night window + day. Non-smart (no dispatch), so the
    main block is single-segment even for an EV charger (the EV meter is a physical sub-meter,
    priced in PASS 2, not a within-block carve)."""
    def test_go_cheap_night_and_day(self):
        self._blk("2025-03-02T01:00:00", 6.0, 0.0850)   # cheap night (EV charge)
        self._blk("2025-03-02T19:00:00", 1.0, 0.4000)   # day
        self.st._conn.commit()
        self._run_sweep()
        self.assertEqual(self.st.count_blocks_needing_reprice(), 0)
        self._assert_block_faithful("2025-03-02T01:00:00", 6.0, 0.0850)
        self._assert_block_faithful("2025-03-02T19:00:00", 1.0, 0.4000)


class TestTracker(_Base):
    """Octopus Tracker — one rate per DAY that changes daily (always positive, capped). Five
    consecutive days at different rates: every block single-segment, sweep completes (no stall),
    exc faithful across the varying rates."""
    RATES = [0.1400, 0.1600, 0.0900, 0.2200, 0.1100]
    def test_tracker_daily_varying_rates(self):
        for i, r in enumerate(self.RATES, start=1):
            self._blk("2025-04-0%dT12:00:00" % i, 2.0, r)
        self.st._conn.commit()
        self._run_sweep()
        self.assertEqual(self.st.count_blocks_needing_reprice(), 0)
        for i, r in enumerate(self.RATES, start=1):
            self._assert_block_faithful("2025-04-0%dT12:00:00" % i, 2.0, r)


class TestNoAccidentalSplitCrossTariff(_Base):
    """Migration invariant: NO non-IOG tariff produces an EV/within-block split. Seed a mix of all
    the single-rate shapes and assert every segment is a lone 'house' segment (no 'ev')."""
    def test_no_ev_segment_without_dispatch(self):
        rows = [("2025-05-01T00:00:00", 2.0, 0.30),      # flat
                ("2025-05-01T02:00:00", 3.0, 0.085),     # go night
                ("2025-05-01T12:00:00", 2.5, -0.05),     # agile plunge
                ("2025-05-01T17:30:00", 1.2, 0.42)]      # cosy peak
        for s, k, r in rows: self._blk(s, k, r)
        self.st._conn.commit()
        self._run_sweep()
        self.assertEqual(self.st.count_blocks_needing_reprice(), 0)
        allsegs = self.st._conn.execute(
            "SELECT attribution, COUNT(*) n FROM block_segments WHERE channel='import' "
            "GROUP BY attribution").fetchall()
        attrs = {r["attribution"]: r["n"] for r in allsegs}
        self.assertEqual(attrs, {"house": len(rows)}, f"unexpected split segments: {attrs}")


if __name__ == "__main__":
    unittest.main()
