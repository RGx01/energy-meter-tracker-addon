"""
P3.3a/b/d (Phase 3 / Option A): the unified historical re-price.

Derives split + exc + segments in one pass via the forward seam. Exc is derived the SAME way
as the exc backfill — the block's stored inc rate scaled by the tariff's published exc/inc ratio
over the FULL agreement history (`rate_segs` / `_exc_rate_for_block`) — and GATED to the backfill's
coverage (imported OR DCC-settled), so provisional blocks stay NULL until settlement.
"""
import os, sys, unittest, asyncio
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import engine
from block_store import BlockStore


class _Exc:
    def __init__(self, v): self.v = v
    def resolve(self, ts): return self.v
    def flat_rate(self): return self.v

class _Sched:
    """Import tariff: flat inc 0.30 (£), exc = inc/1.05. Mirrors a RateSchedule for the seam."""
    def __init__(self, inc, exc): self._inc = inc; self.exc = _Exc(exc)
    def resolve(self, ts): return self._inc
    def flat_rate(self): return self._inc
    def is_empty(self): return False

# full-history rate segments: one open-ended agreement covering everything
_RATE_SEGS = [("2000-01-01T00:00:00", None, _Sched(0.30, round(0.30 / 1.05, 8)))]
_EXC = round(0.30 / 1.05, 6)   # what _exc_rate_for_block yields for a 0.30 inc block


def _mk_store(cp_tz="UTC"):
    st = BlockStore(":memory:")
    with st._conn:
        cp = st._conn.execute(
            "INSERT INTO config_periods (effective_from, billing_day, block_minutes, "
            "timezone, currency_symbol, currency_code) "
            "VALUES ('2024-01-01T00:00:00',1,30,?,'£','GBP')", (cp_tz,)).lastrowid
        st._conn.execute("INSERT INTO meters (config_period_id, meter_id, is_sub_meter, "
                         "meter_type) VALUES (?, 'electricity_main', 0, '')", (cp,))
    st._conn.commit()
    return st, cp


class TestRepriceHistoryBlockCore(unittest.TestCase):
    def setUp(self):
        self._saved_sched = engine._kraken_rate_schedules
        self._saved_store = engine._store
        engine._kraken_rate_schedules = {"import": _Sched(0.30, _EXC)}
        self.st, self._cp = _mk_store()
        engine._store = self.st

    def tearDown(self):
        engine._kraken_rate_schedules = self._saved_sched
        engine._store = self._saved_store
        self.st._conn.close()

    def _blk(self, start, kwh, rate, cost, source="imported_api", imp_kwh_api=None, dispatch=None):
        self.st._conn.execute(
            "INSERT INTO blocks (block_start, block_end, meter_id, config_period_id, "
            "imp_kwh, imp_rate, imp_cost, source, imp_kwh_api) VALUES (?,?, 'electricity_main', "
            "?, ?, ?, ?, ?, ?)", (start, start, self._cp, kwh, rate, cost, source, imp_kwh_api))
        if dispatch is not None:
            self.st._conn.execute(
                "INSERT INTO dispatch_history (slot_start, kind, energy_kwh, first_seen, last_seen)"
                " VALUES (?, 'completed', ?, ?, ?)", (start, dispatch, start, start))

    def _row(self, start):
        return self.st._conn.execute(
            "SELECT block_start, meter_id, imp_kwh, imp_rate, imp_cost, source, imp_kwh_api "
            "FROM blocks WHERE block_start=?", (start,)).fetchone()

    def _segs(self, start):
        return self.st._conn.execute(
            "SELECT kwh, attribution FROM block_segments WHERE block_start=? "
            "AND meter_id='electricity_main' ORDER BY seq", (start,)).fetchall()

    def test_uncapped_imported_gets_exc_via_rate_segs(self):
        A = "2025-01-01T13:00:00"
        self._blk(A, 2.0, 0.30, 0.60, source="imported_api")
        self.st._conn.commit()
        engine._reprice_history_blocks([self._row(A)], "UTC", _RATE_SEGS)
        r = self.st._conn.execute("SELECT imp_kwh_ev, imp_rate_exc, imp_cost_exc FROM blocks "
                                  "WHERE block_start=?", (A,)).fetchone()
        self.assertIsNone(r["imp_kwh_ev"])
        self.assertAlmostEqual(r["imp_rate_exc"], _EXC, places=6)
        self.assertAlmostEqual(r["imp_cost_exc"], round(2.0 * _EXC, 6), places=6)
        segs = self._segs(A)
        self.assertEqual(len(segs), 1)
        self.assertEqual(segs[0]["attribution"], "house")

    def test_provisional_block_exc_gated_out(self):
        # live provisional (not imported, imp_kwh_api NULL) → exc must stay NULL (backfill coverage)
        A = "2025-01-02T13:00:00"
        self._blk(A, 2.0, 0.30, 0.60, source="live", imp_kwh_api=None)
        self.st._conn.commit()
        engine._reprice_history_blocks([self._row(A)], "UTC", _RATE_SEGS)
        r = self.st._conn.execute("SELECT imp_rate_exc FROM blocks WHERE block_start=?", (A,)).fetchone()
        self.assertIsNone(r["imp_rate_exc"])          # gated out — no stale exc on a movable rate
        self.assertEqual(len(self._segs(A)), 1)       # segments still derived

    def test_settled_live_block_gets_exc(self):
        A = "2025-01-03T13:00:00"
        self._blk(A, 2.0, 0.30, 0.60, source="live", imp_kwh_api=2.0)   # DCC-settled
        self.st._conn.commit()
        engine._reprice_history_blocks([self._row(A)], "UTC", _RATE_SEGS)
        r = self.st._conn.execute("SELECT imp_rate_exc FROM blocks WHERE block_start=?", (A,)).fetchone()
        self.assertAlmostEqual(r["imp_rate_exc"], _EXC, places=6)

    def test_dispatched_split_invariant(self):
        B = "2025-01-04T02:00:00"
        self._blk(B, 3.0, 0.30, 0.90, source="imported_api", dispatch=2.0)
        self.st._conn.commit()
        engine._reprice_history_blocks([self._row(B)], "UTC", _RATE_SEGS)
        r = self.st._conn.execute("SELECT imp_kwh_ev FROM blocks WHERE block_start=?", (B,)).fetchone()
        self.assertAlmostEqual(r["imp_kwh_ev"], 2.0, places=6)
        segs = self.st._conn.execute("SELECT kwh, attribution FROM block_segments WHERE "
                                     "block_start=?", (B,)).fetchall()
        self.assertAlmostEqual(sum(s["kwh"] for s in segs), 3.0, places=6)
        self.assertIn("ev", {s["attribution"] for s in segs})


class TestSweepAndConformance(unittest.TestCase):
    def setUp(self):
        self._saved_sched = engine._kraken_rate_schedules
        self._saved_store = engine._store
        self._saved_bcs = engine._build_channel_rate_segs
        engine._kraken_rate_schedules = {"import": _Sched(0.30, _EXC)}
        async def _fake_segs(ch): return _RATE_SEGS
        engine._build_channel_rate_segs = _fake_segs
        self.st, self._cp = _mk_store()
        engine._store = self.st

    def tearDown(self):
        engine._kraken_rate_schedules = self._saved_sched
        engine._store = self._saved_store
        engine._build_channel_rate_segs = self._saved_bcs
        self.st._conn.close()

    def _blk(self, start, kwh, rate, cost, source="imported_api", imp_kwh_api=None,
             dispatch=None, rate_exc=None):
        self.st._conn.execute(
            "INSERT INTO blocks (block_start, block_end, meter_id, config_period_id, imp_kwh, "
            "imp_rate, imp_cost, source, imp_kwh_api, imp_rate_exc) VALUES (?,?, 'electricity_main', "
            "?, ?, ?, ?, ?, ?, ?)", (start, start, self._cp, kwh, rate, cost, source, imp_kwh_api, rate_exc))
        if dispatch is not None:
            self.st._conn.execute(
                "INSERT INTO dispatch_history (slot_start, kind, energy_kwh, first_seen, last_seen)"
                " VALUES (?, 'completed', ?, ?, ?)", (start, dispatch, start, start))

    def test_sweep_derives_and_is_idempotent(self):
        self._blk("2025-03-01T13:00:00", 2.0, 0.30, 0.60, source="imported_api")
        self._blk("2025-03-02T02:00:00", 3.0, 0.30, 0.90, source="imported_api", dispatch=2.0)
        self.st._conn.commit()
        self.assertEqual(self.st.count_blocks_missing_segments(), 2)
        for _ in range(5):
            asyncio.run(engine._run_historical_reprice_sweep(batch=1))
            if (self.st.get_meta(engine._REPRICE_HISTORY_MARKER, {}) or {}).get("done"):
                break
        self.assertEqual(self.st.count_blocks_missing_segments(), 0)
        exc = self.st._conn.execute("SELECT imp_rate_exc FROM blocks WHERE "
                                    "block_start='2025-03-01T13:00:00'").fetchone()["imp_rate_exc"]
        self.assertAlmostEqual(exc, _EXC, places=6)
        self.assertEqual(asyncio.run(engine._run_historical_reprice_sweep()), 0)

    def test_sweep_superset_fills_exc_on_settled_block_with_segments(self):
        # P3.3c: a settled block that HAS segments but NULL exc is in the superset coverage.
        # The sweep must fill its exc (fill-null-only writer) WITHOUT rebuilding its segments.
        import pricing_segments as ps
        start = "2025-06-01T13:00:00"
        self._blk(start, 2.0, 0.30, 0.60, source="live", imp_kwh_api=2.0, rate_exc=None)
        self.st.set_block_segments(start, "electricity_main",
                                   [ps.Segment(2.0, 0.30, None, "day", "house")])
        self.st._conn.commit()
        self.assertEqual(self.st.count_blocks_missing_segments(), 0)      # already has segments
        self.assertGreaterEqual(self.st.count_blocks_needing_reprice(), 1)  # but needs exc
        for _ in range(5):
            asyncio.run(engine._run_historical_reprice_sweep(batch=50))
            if (self.st.get_meta(engine._REPRICE_HISTORY_MARKER, {}) or {}).get("done"):
                break
        r = self.st._conn.execute("SELECT imp_rate_exc FROM blocks WHERE block_start=?",
                                  (start,)).fetchone()
        self.assertAlmostEqual(r["imp_rate_exc"], _EXC, places=6)          # exc filled
        n = self.st._conn.execute("SELECT COUNT(*) n FROM block_segments WHERE block_start=?",
                                  (start,)).fetchone()["n"]
        self.assertEqual(n, 1)                                            # segments NOT rebuilt

    def test_conformance_clean_and_categorises(self):
        # exact: imported, stored exc correct → exact
        self._blk("2025-04-01T13:00:00", 2.0, 0.30, 0.60, source="imported_api", rate_exc=_EXC)
        # fill: settled but stored exc NULL → sweep adds it → FILL (not a regression)
        self._blk("2025-04-02T13:00:00", 2.0, 0.30, 0.60, source="live", imp_kwh_api=2.0, rate_exc=None)
        # regression: imported, stored exc WRONG → sweep computes different → REGRESSION
        self._blk("2025-04-03T13:00:00", 2.0, 0.30, 0.60, source="imported_api", rate_exc=0.99)
        self.st._conn.commit()
        rep = engine.reprice_history_conformance("2025-04-01T00:00:00", "2025-04-04T00:00:00", rate_segs=_RATE_SEGS)
        self.assertEqual(rep["checked"], 3)
        self.assertEqual(rep["exact"], 1)
        self.assertEqual(rep["fill_count"], 1)
        self.assertEqual(rep["regression_count"], 1)
        self.assertEqual(rep["regressions"][0]["start"], "2025-04-03T13:00:00")
        self.assertIn("REGRESSION", rep["verdict"])

    def test_conformance_clean_when_no_regressions(self):
        self._blk("2025-05-01T13:00:00", 2.0, 0.30, 0.60, source="imported_api", rate_exc=_EXC)
        self._blk("2025-05-02T13:00:00", 2.0, 0.30, 0.60, source="live", imp_kwh_api=2.0)  # fill
        self.st._conn.commit()
        rep = engine.reprice_history_conformance("2025-05-01T00:00:00", "2025-05-03T00:00:00", rate_segs=_RATE_SEGS)
        self.assertEqual(rep["regression_count"], 0)
        self.assertEqual(rep["verdict"], "clean")


class TestNetAlarm(unittest.TestCase):
    def setUp(self):
        self._saved_store = engine._store
        self.st, self._cp = _mk_store()
        engine._store = self.st

    def tearDown(self):
        engine._store = self._saved_store
        self.st._conn.close()

    def test_net_alarm_fires_only_after_sweep_done(self):
        import logging
        with self.assertLogs("engine", level="WARNING") as h1:
            logging.getLogger("engine").warning("keepalive")
            engine._net_alarm("exc", 5)                          # marker not done → no alarm
        self.assertFalse(any("NET-ALARM" in m for m in h1.output))
        self.st.set_meta(engine._REPRICE_HISTORY_MARKER, {"done": True})
        with self.assertLogs("engine", level="WARNING") as h2:
            engine._net_alarm("exc", 5)                          # done + filled → alarm
        self.assertTrue(any("NET-ALARM" in m and "exc" in m for m in h2.output))
        with self.assertLogs("engine", level="WARNING") as h3:
            logging.getLogger("engine").warning("keepalive")
            engine._net_alarm("segment", 0)                      # filled 0 → no alarm
        self.assertFalse(any("NET-ALARM" in m for m in h3.output))


if __name__ == "__main__":
    unittest.main()


class TestSweepHardening(unittest.TestCase):
    """Watch #10 ROOT CAUSE: a chunk that throws must NOT let the sweep mark 'done' with blocks
    still unpriced. Transient failure → re-arm and heal; persistent failure → stall loudly, no
    busy-loop."""
    def setUp(self):
        self._saved_sched = engine._kraken_rate_schedules
        self._saved_store = engine._store
        self._saved_bcs = engine._build_channel_rate_segs
        self._saved_rhb = engine._reprice_history_blocks
        engine._kraken_rate_schedules = {"import": _Sched(0.30, _EXC)}
        async def _fake_segs(ch): return _RATE_SEGS
        engine._build_channel_rate_segs = _fake_segs
        self.st, self._cp = _mk_store()
        engine._store = self.st

    def tearDown(self):
        engine._kraken_rate_schedules = self._saved_sched
        engine._store = self._saved_store
        engine._build_channel_rate_segs = self._saved_bcs
        engine._reprice_history_blocks = self._saved_rhb
        self.st._conn.close()

    def _blk(self, start, kwh=2.0, rate=0.30, cost=0.60):
        self.st._conn.execute(
            "INSERT INTO blocks (block_start, block_end, meter_id, config_period_id, imp_kwh, "
            "imp_rate, imp_cost, source, imp_kwh_api, imp_rate_exc) VALUES (?,?, 'electricity_main', "
            "?, ?, ?, ?, 'imported_api', NULL, NULL)", (start, start, self._cp, kwh, rate, cost))

    def _marker(self):
        return self.st.get_meta(engine._REPRICE_HISTORY_MARKER, {}) or {}

    def test_transient_chunk_failure_rearms_then_heals(self):
        for d in ("01", "02", "03"):
            self._blk("2025-03-%sT02:00:00" % d)
        self.st._conn.commit()
        self.assertEqual(self.st.count_blocks_needing_reprice(), 3)
        # fail ONLY the first block once, delegate the rest to the real pricer
        real = self._saved_rhb
        state = {"boom": True}
        def _flaky(rows, tz, segs):
            if state["boom"] and any(r["block_start"] == "2025-03-01T02:00:00" for r in rows):
                state["boom"] = False
                raise RuntimeError("transient")
            return real(rows, tz, segs)
        engine._reprice_history_blocks = _flaky
        asyncio.run(engine._run_historical_reprice_sweep(batch=1))
        m = self._marker()
        # must NOT be done (a block was skipped) and cursor re-armed to None
        self.assertFalse(m.get("done"), "sweep marked done despite a skipped block")
        self.assertIsNone(m.get("cursor"), "cursor not re-armed after a skip")
        self.assertGreater(self.st.count_blocks_needing_reprice(), 0)
        # next pass (no failure now) heals the remainder and completes
        for _ in range(5):
            asyncio.run(engine._run_historical_reprice_sweep(batch=1))
            if self._marker().get("done"):
                break
        self.assertTrue(self._marker().get("done"))
        self.assertEqual(self.st.count_blocks_needing_reprice(), 0)

    def test_persistent_failure_stalls_loudly_no_busyloop(self):
        self._blk("2025-03-01T02:00:00")
        self.st._conn.commit()
        engine._reprice_history_blocks = lambda rows, tz, segs: (_ for _ in ()).throw(RuntimeError("always"))
        # run several passes — must terminate (mark done+stalled), not loop forever
        for _ in range(4):
            asyncio.run(engine._run_historical_reprice_sweep(batch=1))
            if self._marker().get("done"):
                break
        m = self._marker()
        self.assertTrue(m.get("done"), "persistent failure never terminated")
        self.assertEqual(m.get("stalled"), 1)
        self.assertGreater(self.st.count_blocks_needing_reprice(), 0)   # the bad block remains, but visibly


class TestPenceSelfHeal(unittest.TestCase):
    """Q1: the sweep self-heals a legacy PENCE block (4.3.0 cap bug) — normalises inc to £ before
    building segments AND force-repairs the stored columns (main + sub-meters). Healthy blocks are
    byte-identical (guard fires only on inc > £3/kWh)."""
    def setUp(self):
        self._saved_sched = engine._kraken_rate_schedules
        self._saved_store = engine._store
        self._saved_bcs = engine._build_channel_rate_segs
        engine._kraken_rate_schedules = {"import": _Sched(0.30, _EXC)}
        async def _fake_segs(ch): return _RATE_SEGS
        engine._build_channel_rate_segs = _fake_segs
        self.st, self._cp = _mk_store()
        # add a sub-meter so the block-scoped repair reaches it
        self.st._conn.execute("INSERT INTO meters (config_period_id, meter_id, is_sub_meter, "
                              "meter_type) VALUES (?, 'ev_charger', 1, 'ev_charger')", (self._cp,))
        engine._store = self.st

    def tearDown(self):
        engine._kraken_rate_schedules = self._saved_sched
        engine._store = self._saved_store
        engine._build_channel_rate_segs = self._saved_bcs
        self.st._conn.close()

    def _blk(self, start, meter, kwh, rate, cost, is_grid=False):
        col = "imp_kwh_grid" if is_grid else "imp_kwh_api"
        self.st._conn.execute(
            f"INSERT INTO blocks (block_start, block_end, meter_id, config_period_id, imp_kwh, "
            f"imp_rate, imp_cost, source, {col}) VALUES (?,?,?,?,?,?,?, 'kraken_api', ?)",
            (start, start, meter, self._cp, kwh, rate, cost, kwh))

    def test_pence_block_healed_columns_and_segments(self):
        B = "2026-08-18T03:30:00"
        # the real bug shape: main + sub both in pence
        self._blk(B, "electricity_main", 3.929, 6.89997, 27.109982)
        self._blk(B, "ev_charger", 3.5, 6.89997, 24.14990, is_grid=True)
        # a healthy neighbour that must stay byte-identical
        H = "2026-08-18T03:00:00"
        self._blk(H, "electricity_main", 2.0, 0.069, 0.138)
        self.st._conn.commit()
        rows = self.st.get_blocks_needing_reprice()
        res = engine._reprice_history_blocks(rows, "UTC", _RATE_SEGS)
        self.assertGreaterEqual(res.get("healed", 0), 2)     # main + sub repaired
        # main + sub columns ÷100 to £
        m = self.st._conn.execute("SELECT imp_rate, imp_cost FROM blocks WHERE block_start=? AND "
                                  "meter_id='electricity_main'", (B,)).fetchone()
        self.assertAlmostEqual(m["imp_rate"], 0.069, places=4)
        self.assertAlmostEqual(m["imp_cost"], 0.2711, places=3)
        sub = self.st._conn.execute("SELECT imp_rate, imp_cost FROM blocks WHERE block_start=? AND "
                                    "meter_id='ev_charger'", (B,)).fetchone()
        self.assertAlmostEqual(sub["imp_rate"], 0.069, places=4)
        # segment built in £ (house segment ~0.069, NOT 6.9)
        seg = self.st._conn.execute("SELECT kwh, inc_rate FROM block_segments WHERE block_start=? "
                                    "AND meter_id='electricity_main'", (B,)).fetchone()
        self.assertLess(seg["inc_rate"], 1.0)
        self.assertAlmostEqual(seg["inc_rate"], 0.069, places=3)
        # healthy neighbour untouched
        h = self.st._conn.execute("SELECT imp_rate, imp_cost FROM blocks WHERE block_start=? AND "
                                  "meter_id='electricity_main'", (H,)).fetchone()
        self.assertEqual(h["imp_rate"], 0.069)
        self.assertEqual(h["imp_cost"], 0.138)

    def test_completion_report_written_with_heal_count(self):
        import tempfile, os, json
        B = "2026-08-18T03:30:00"
        self._blk(B, "electricity_main", 3.929, 6.89997, 27.109982)
        self.st._conn.commit()
        tmp = tempfile.mkdtemp()
        saved = engine.SHARE_BACKUP_DIR
        engine.SHARE_BACKUP_DIR = tmp
        try:
            for _ in range(10):
                asyncio.run(engine._run_historical_reprice_sweep(batch=50))
                if (self.st.get_meta(engine._REPRICE_HISTORY_MARKER, {}) or {}).get("done"):
                    break
            p = os.path.join(tmp, "reprice_history_report.json")
            self.assertTrue(os.path.exists(p), "sweep completion report not written")
            rpt = json.load(open(p))
            self.assertEqual(rpt["verdict"], "complete")
            self.assertGreaterEqual(rpt["blocks_self_healed_pence"], 1)
            self.assertIn("pence", rpt["summary"])
        finally:
            engine.SHARE_BACKUP_DIR = saved


if __name__ == "__main__":
    unittest.main()


class TestGapAwareMigration(unittest.TestCase):
    """G1/G2: a gap (imp_cost NULL = unknown price) is left honestly unsegmented and never stalls
    the sweep; a genuine free slot (imp_cost 0) is segmented; and filling or correcting a price
    RE-OPENS the block for segmentation — the fill-later door stays open, idempotently."""
    def setUp(self):
        self._s = engine._kraken_rate_schedules
        self._st_saved = engine._store
        self._bcs = engine._build_channel_rate_segs
        engine._kraken_rate_schedules = {"import": _Sched(0.30, _EXC)}
        async def _fake(ch): return _RATE_SEGS
        engine._build_channel_rate_segs = _fake
        self.st, self._cp = _mk_store()
        engine._store = self.st

    def tearDown(self):
        engine._kraken_rate_schedules = self._s
        engine._store = self._st_saved
        engine._build_channel_rate_segs = self._bcs
        self.st._conn.close()

    def _ins(self, start, kwh, rate, cost):
        self.st._conn.execute(
            "INSERT INTO blocks (block_start, block_end, meter_id, config_period_id, imp_kwh, "
            "imp_rate, imp_cost, source, imp_kwh_api) VALUES (?,?, 'electricity_main', ?, ?, ?, ?, "
            "'imported_api', ?)", (start, start, self._cp, kwh, rate, cost, kwh))

    def _has_seg(self, start):
        return self.st._conn.execute(
            "SELECT COUNT(*) c FROM block_segments WHERE block_start=? AND meter_id='electricity_main'",
            (start,)).fetchone()["c"] > 0

    def _seg_rate(self, start):
        r = self.st._conn.execute("SELECT inc_rate FROM block_segments WHERE block_start=? "
                                  "AND meter_id='electricity_main'", (start,)).fetchone()
        return r["inc_rate"] if r else None

    def _done(self):
        return (self.st.get_meta(engine._REPRICE_HISTORY_MARKER, {}) or {}).get("done")

    def _sweep(self):
        for _ in range(20):
            asyncio.run(engine._run_historical_reprice_sweep(batch=50))
            if self._done():
                break

    def test_gap_skipped_free_segmented_sweep_completes(self):
        self._ins("2026-01-01T00:00:00", 2.0, None, None)   # GAP — imp_cost NULL
        self._ins("2026-01-01T00:30:00", 1.0, 0.0, 0.0)     # FREE — imp_cost 0
        self._ins("2026-01-01T01:00:00", 3.0, 0.30, 0.90)   # priced
        self.st._conn.commit()
        self.assertEqual(self.st.count_blocks_needing_reprice(), 2)   # gap excluded; free+priced in
        self._sweep()
        self.assertTrue(self._done())                                 # no stall on the gap
        self.assertFalse(self._has_seg("2026-01-01T00:00:00"))        # gap NOT segmented (honest)
        self.assertTrue(self._has_seg("2026-01-01T00:30:00"))         # free segmented (£0, real)
        self.assertTrue(self._has_seg("2026-01-01T01:00:00"))         # priced segmented

    def test_filling_a_gap_reopens_segmentation(self):
        G = "2026-02-01T00:00:00"
        self._ins(G, 2.0, None, None)
        self.st._conn.commit()
        self._sweep()
        self.assertFalse(self._has_seg(G))                            # gap stays unsegmented
        # fill the price later (the Retry/CSV path uses reprice_imported_block)
        self.assertTrue(self.st.reprice_imported_block(G, "electricity_main", "import", 0.30, 0.60))
        self.assertEqual(self.st.count_blocks_needing_reprice(), 1)   # door re-opened
        asyncio.run(engine.run_reprice_history_sweep_to_done())       # G2: endpoints force this
        self.assertTrue(self._has_seg(G))                             # now segmented from the fill
        self.assertAlmostEqual(self._seg_rate(G), 0.30, places=6)

    def test_correcting_a_price_reinvalidates_and_resegments(self):
        B = "2026-03-01T00:00:00"
        self._ins(B, 2.0, 0.30, 0.60)
        self.st._conn.commit()
        self._sweep()
        self.assertAlmostEqual(self._seg_rate(B), 0.30, places=6)     # segmented at first price
        # correct the price → segment invalidated → re-admitted → rebuilt at the new price
        self.assertTrue(self.st.reprice_imported_block(B, "electricity_main", "import", 0.25, 0.50))
        self.assertFalse(self._has_seg(B))                            # stale segment invalidated
        self.assertEqual(self.st.count_blocks_needing_reprice(), 1)
        asyncio.run(engine.run_reprice_history_sweep_to_done())       # G2: endpoints force this
        self.assertAlmostEqual(self._seg_rate(B), 0.25, places=6)     # rebuilt at corrected price


class TestMigrationGuard(unittest.TestCase):
    """The guard that refuses manual reprice/fill while the first-upgrade sweep is still working."""
    def setUp(self):
        self._st_saved = engine._store
        self._run_saved = engine._reprice_history_running
        self.st, self._cp = _mk_store()
        engine._store = self.st

    def tearDown(self):
        engine._store = self._st_saved
        engine._reprice_history_running = self._run_saved
        self.st._conn.close()

    def _priced_block(self):
        self.st._conn.execute(
            "INSERT INTO blocks (block_start, block_end, meter_id, config_period_id, imp_kwh, "
            "imp_rate, imp_cost, source, imp_kwh_api) VALUES ('2026-01-01T00:00:00','2026-01-01T00:00:00',"
            "'electricity_main', ?, 2.0, 0.30, 0.60, 'imported_api', 2.0)", (self._cp,))
        self.st._conn.commit()

    def test_guard_signals(self):
        engine._reprice_history_running = False
        # not done + a priced block still needs a segment → migrating
        self._priced_block()
        self.assertTrue(engine.reprice_history_in_progress())
        # marked done → not migrating (manual recovery allowed)
        self.st.set_meta(engine._REPRICE_HISTORY_MARKER, {"done": True})
        self.assertFalse(engine.reprice_history_in_progress())
        # a pass executing right now → migrating, even past 'done'
        engine._reprice_history_running = True
        self.assertTrue(engine.reprice_history_in_progress())
        engine._reprice_history_running = False
        # done and nothing needs reprice → not migrating
        self.assertFalse(engine.reprice_history_in_progress())
