"""
Tests for the historical carbon backfill (v2 -> v3 migration).

Store layer: NULL-carbon date range, windowed NULL-block query, meta KV.
Engine worker: attributes historical blocks from a paged intensity map, run-once
done marker, resume cursor on fetch failure, and no-op without a postcode.
"""
import sys
import os
import types
import unittest
import asyncio
from unittest.mock import MagicMock

# ── Minimal stubs so engine.py imports without HA/filesystem ─────────────────
eio = types.ModuleType("energy_engine_io")
eio.ensure_dir = lambda *a, **kw: None
eio.load_json = lambda *a, **kw: a[1] if len(a) > 1 else {}
eio.save_json_atomic = lambda *a, **kw: None
eio.save_file = lambda *a, **kw: None
sys.modules["energy_engine_io"] = eio

ec = types.ModuleType("energy_charts")
ec.generate_net_heatmap = lambda *a, **kw: ""
ec.generate_daily_import_export_charts = lambda *a, **kw: ""
sys.modules["energy_charts"] = ec

hc = types.ModuleType("ha_client")
hc.HAClient = MagicMock
sys.modules["ha_client"] = hc

from block_store import BlockStore, open_block_store, migrate_json_to_sqlite
_boot_store = BlockStore(":memory:")
_boot_store.insert_config_period({"meters": {"electricity_main": {"meta": {
    "timezone": "UTC", "billing_day": 1, "block_minutes": 30,
    "currency_symbol": "£", "currency_code": "GBP"}}}})

bs = types.ModuleType("block_store")
bs.BlockStore = BlockStore
bs.open_block_store = lambda path: _boot_store
bs.migrate_json_to_sqlite = migrate_json_to_sqlite
sys.modules["block_store"] = bs

sys.path.insert(0, os.path.dirname(__file__))
import engine
engine._store = _boot_store


# ── Helpers ──────────────────────────────────────────────────────────────────
def _store_with_postcode(postcode="DE1"):
    st = BlockStore(":memory:")
    meta = {"timezone": "Europe/London", "billing_day": 1, "block_minutes": 30,
            "currency_symbol": "£", "sub_meter": False}
    if postcode:
        meta["postcode_prefix"] = postcode
    st.insert_config_period({"meters": {"electricity_main": {"meta": meta}}})
    return st


def _cp_id(st):
    return st._conn.execute("SELECT id FROM config_periods LIMIT 1").fetchone()["id"]


def _insert_null_block(st, start, imp_kwh, cp):
    st._conn.execute(
        "INSERT INTO blocks (block_start, block_end, meter_id, config_period_id, "
        "interpolated, imp_kwh, imp_rate, imp_cost, standing_charge, "
        "carbon_intensity_g) VALUES (?,?,?,?,0,?,?,?,?,NULL)",
        (start, start, "electricity_main", cp,
         imp_kwh, 0.30, round(imp_kwh * 0.30, 4), 0.50))
    st._conn.commit()


def _carbon_of(st, start):
    r = st._conn.execute(
        "SELECT carbon_intensity_g, carbon_g FROM blocks WHERE block_start=?",
        (start,)).fetchone()
    return (r["carbon_intensity_g"], r["carbon_g"]) if r else (None, None)


# ── Store helpers ─────────────────────────────────────────────────────────────
class TestBackfillStoreHelpers(unittest.TestCase):
    def setUp(self):
        self.st = _store_with_postcode()
        self.cp = _cp_id(self.st)

    def test_meta_kv_round_trip(self):
        self.assertEqual(self.st.get_meta("nope", {"d": 1}), {"d": 1})
        self.st.set_meta("carbon_backfill_state", {"cursor": "2026-05-15T10:00:00",
                                                   "done": False})
        got = self.st.get_meta("carbon_backfill_state")
        self.assertEqual(got["cursor"], "2026-05-15T10:00:00")
        self.assertFalse(got["done"])
        self.st.set_meta("carbon_backfill_state", {"done": True})
        self.assertTrue(self.st.get_meta("carbon_backfill_state")["done"])

    def test_missing_range_none_when_all_attributed(self):
        self.assertIsNone(self.st.get_missing_carbon_date_range())

    def test_missing_range_bounds(self):
        _insert_null_block(self.st, "2026-05-01T10:00:00", 1.0, self.cp)
        _insert_null_block(self.st, "2026-05-20T10:00:00", 2.0, self.cp)
        lo, hi = self.st.get_missing_carbon_date_range()
        self.assertEqual(lo, "2026-05-01T10:00:00")
        self.assertEqual(hi, "2026-05-20T10:00:00")

    def test_in_range_window(self):
        for d in ("2026-05-01T10:00:00", "2026-05-10T10:00:00",
                  "2026-05-20T10:00:00"):
            _insert_null_block(self.st, d, 1.0, self.cp)
        got = self.st.get_block_starts_missing_carbon_in_range(
            "2026-05-01T00:00:00", "2026-05-15T00:00:00")
        self.assertEqual(got, ["2026-05-01T10:00:00", "2026-05-10T10:00:00"])


# ── Engine worker ─────────────────────────────────────────────────────────────
class TestHistoricalCarbonBackfill(unittest.TestCase):
    MARKER = "carbon_backfill_state"

    def setUp(self):
        self.st = _store_with_postcode("DE1")
        self.cp = _cp_id(self.st)
        self._orig_store = engine._store
        self._orig_fetch = engine._fetch_carbon_intensity_range
        self._orig_gc = engine.generate_charts
        engine._store = self.st
        engine.generate_charts = lambda *a, **kw: None
        engine._carbon_backfill_running = False

    def tearDown(self):
        engine._store = self._orig_store
        engine._fetch_carbon_intensity_range = self._orig_fetch
        engine.generate_charts = self._orig_gc

    def test_attributes_historical_blocks(self):
        _insert_null_block(self.st, "2026-05-01T10:00:00", 2.0, self.cp)
        _insert_null_block(self.st, "2026-05-01T10:30:00", 1.0, self.cp)
        _insert_null_block(self.st, "2026-05-02T09:00:00", 3.0, self.cp)
        engine._fetch_carbon_intensity_range = lambda pc, f, t: {
            "2026-05-01T10:00": 200.0,
            "2026-05-01T10:30": 210.0,
            "2026-05-02T09:00": 150.0,
        }
        n = asyncio.run(engine._run_historical_carbon_backfill())
        self.assertEqual(n, 3)
        self.assertEqual(_carbon_of(self.st, "2026-05-01T10:00:00"),
                         (200.0, round(2.0 * 200.0, 4)))
        self.assertEqual(_carbon_of(self.st, "2026-05-01T10:30:00"),
                         (210.0, round(1.0 * 210.0, 4)))
        self.assertEqual(_carbon_of(self.st, "2026-05-02T09:00:00"),
                         (150.0, round(3.0 * 150.0, 4)))
        self.assertTrue(self.st.get_meta(self.MARKER)["done"])

    def test_completion_render_is_offloaded_not_synchronous(self):
        # Regression: rendering charts synchronously on the loop after a large
        # backfill stalled the HA heartbeat → reconnect → re-startup → re-render
        # storm. The completion render must go through the OFF-loop path, never a
        # direct on-loop generate_charts().
        _insert_null_block(self.st, "2026-05-01T10:00:00", 2.0, self.cp)
        engine._fetch_carbon_intensity_range = lambda pc, f, t: {
            "2026-05-01T10:00": 200.0}
        sync_calls = {"n": 0}
        engine.generate_charts = lambda *a, **kw: sync_calls.__setitem__("n", sync_calls["n"] + 1)
        offloaded = {"n": 0}
        orig_off = engine._generate_charts_offloaded

        async def _fake_offload():
            offloaded["n"] += 1
        engine._generate_charts_offloaded = _fake_offload
        try:
            n = asyncio.run(engine._run_historical_carbon_backfill())
        finally:
            engine._generate_charts_offloaded = orig_off
        self.assertEqual(n, 1)
        self.assertEqual(offloaded["n"], 1, "completion render must be offloaded")
        self.assertEqual(sync_calls["n"], 0, "must not render synchronously on the loop")

    def test_backfill_preserves_imported_source_tag(self):
        # THE root-cause regression: the carbon backfill used to round-trip the
        # whole row (append_block_replace) and silently wipe `source` to NULL,
        # untagging every imported block it filled. It must now write carbon IN
        # PLACE and leave the tag intact.
        self.st._conn.execute(
            "INSERT INTO blocks (block_start, block_end, meter_id, config_period_id, "
            "interpolated, imp_kwh, imp_rate, imp_cost, standing_charge, "
            "carbon_intensity_g, source) VALUES (?,?,?,?,0,?,?,?,?,NULL,'imported_api')",
            ("2026-05-01T10:00:00", "2026-05-01T10:00:00", "electricity_main",
             self.cp, 2.0, 0.30, 0.60, 0.50))
        self.st._conn.commit()
        engine._fetch_carbon_intensity_range = lambda pc, f, t: {"2026-05-01T10:00": 200.0}
        n = asyncio.run(engine._run_historical_carbon_backfill())
        self.assertEqual(n, 1)
        r = self.st._conn.execute(
            "SELECT source, carbon_intensity_g, carbon_g FROM blocks "
            "WHERE block_start='2026-05-01T10:00:00'").fetchone()
        self.assertEqual(r["source"], "imported_api")     # tag SURVIVES the carbon fill
        self.assertAlmostEqual(r["carbon_intensity_g"], 200.0)
        self.assertAlmostEqual(r["carbon_g"], round(2.0 * 200.0, 4))

    def test_paused_flag_stops_recovery(self):
        # The kill switch: while carbon_paused, the recovery sweep stands down.
        _insert_null_block(self.st, "2026-05-01T10:00:00", 2.0, self.cp)
        self.st.set_meta("carbon_paused", True)
        self.assertEqual(engine._recover_missing_carbon(), 0)
        self.assertEqual(_carbon_of(self.st, "2026-05-01T10:00:00"), (None, None))

    def test_run_once_marker_short_circuits(self):
        _insert_null_block(self.st, "2026-05-01T10:00:00", 2.0, self.cp)
        engine._fetch_carbon_intensity_range = lambda pc, f, t: {
            "2026-05-01T10:00": 200.0}
        self.assertEqual(asyncio.run(engine._run_historical_carbon_backfill()), 1)
        # New NULL block after migration is NOT re-touched (done marker is final;
        # live attribution/recovery handles post-migration gaps).
        _insert_null_block(self.st, "2026-05-03T10:00:00", 5.0, self.cp)
        self.assertEqual(asyncio.run(engine._run_historical_carbon_backfill()), 0)
        self.assertEqual(_carbon_of(self.st, "2026-05-03T10:00:00"), (None, None))

    def test_no_postcode_is_noop(self):
        st = _store_with_postcode(postcode=None)
        cp = _cp_id(st)
        engine._store = st
        _insert_null_block(st, "2026-05-01T10:00:00", 2.0, cp)
        called = {"n": 0}

        def _spy(*a, **kw):
            called["n"] += 1
            return {}
        engine._fetch_carbon_intensity_range = _spy
        self.assertEqual(asyncio.run(engine._run_historical_carbon_backfill()), 0)
        self.assertEqual(called["n"], 0, "must not hit the API without a postcode")
        self.assertEqual(_carbon_of(st, "2026-05-01T10:00:00"), (None, None))
        self.assertNotEqual(st.get_meta(self.MARKER, {}).get("done"), True)

    def test_resume_cursor_on_fetch_failure(self):
        # Span > 13 days -> two windows at window_days=13. Fail the 2nd window's
        # fetch, confirm the cursor is persisted, then resume and complete.
        _insert_null_block(self.st, "2026-05-01T10:00:00", 2.0, self.cp)
        _insert_null_block(self.st, "2026-05-20T10:00:00", 4.0, self.cp)

        calls = {"n": 0}

        def _fail_second(pc, f, t):
            calls["n"] += 1
            if calls["n"] >= 2:
                raise RuntimeError("simulated network error")
            return {"2026-05-01T10:00": 200.0}
        engine._fetch_carbon_intensity_range = _fail_second
        n1 = asyncio.run(engine._run_historical_carbon_backfill(window_days=13))
        self.assertEqual(n1, 1)
        self.assertEqual(_carbon_of(self.st, "2026-05-01T10:00:00"),
                         (200.0, round(2.0 * 200.0, 4)))
        self.assertEqual(_carbon_of(self.st, "2026-05-20T10:00:00"), (None, None))
        mk = self.st.get_meta(self.MARKER)
        self.assertFalse(mk.get("done"))
        # First 13-day window ends 2026-05-01T10:00 + 13d = 2026-05-14T10:00.
        self.assertEqual(mk.get("cursor"), "2026-05-14T10:00:00")

        # Resume: 2nd window now succeeds.
        engine._fetch_carbon_intensity_range = lambda pc, f, t: {
            "2026-05-20T10:00": 150.0}
        n2 = asyncio.run(engine._run_historical_carbon_backfill(window_days=13))
        self.assertEqual(n2, 1)
        self.assertEqual(_carbon_of(self.st, "2026-05-20T10:00:00"),
                         (150.0, round(4.0 * 150.0, 4)))
        self.assertTrue(self.st.get_meta(self.MARKER)["done"])

    def test_persist_failure_leaves_gap_not_marked_done_then_retries(self):
        # A per-block persist failure must NOT let the worker mark done over the
        # gap (the original bug: marker keyed on cursor, not on remaining NULLs).
        # The next pass must retry and complete.
        _insert_null_block(self.st, "2026-05-01T10:00:00", 2.0, self.cp)
        _insert_null_block(self.st, "2026-05-01T10:30:00", 1.0, self.cp)
        engine._fetch_carbon_intensity_range = lambda pc, f, t: {
            "2026-05-01T10:00": 200.0, "2026-05-01T10:30": 210.0}
        real = engine._persist_block_carbon    # carbon now writes IN PLACE (keeps source)
        flaky = {"calls": 0}

        def _flaky(block, start):
            flaky["calls"] += 1
            if flaky["calls"] == 1:        # fail the first block of the pass
                raise RuntimeError("simulated persist failure")
            return real(block, start)
        engine._persist_block_carbon = _flaky
        try:
            n1 = asyncio.run(engine._run_historical_carbon_backfill())
        finally:
            engine._persist_block_carbon = real
        self.assertEqual(n1, 1)                       # one persisted, one failed
        mk = self.st.get_meta(self.MARKER)
        self.assertFalse(mk.get("done"),
                         "must not mark done while a NULL block remains")
        self.assertIsNotNone(self.st.get_missing_carbon_date_range())

        # Clean retry fills the stranded block and completes.
        n2 = asyncio.run(engine._run_historical_carbon_backfill())
        self.assertEqual(n2, 1)
        self.assertTrue(self.st.get_meta(self.MARKER)["done"])
        self.assertIsNone(self.st.get_missing_carbon_date_range())

    def test_default_window_under_api_cap(self):
        # The Carbon Intensity API rejects >=14-day ranges (HTTP 400 — observed in
        # prod on an exactly-14-day window). The default must stay strictly under.
        import inspect
        wd = inspect.signature(
            engine._run_historical_carbon_backfill
        ).parameters["window_days"].default
        self.assertLessEqual(wd, 13,
                             "window_days must be < 14 or the range fetch 400s")


class TestEngineModuleSurface(unittest.TestCase):
    """Guards against an adjacent edit swallowing a function signature — the
    '_tick_carbon_intensity is not defined' prod regression where a str_replace
    consumed the `async def` line and merged the tick body into the function
    above it. Parsing still succeeded and no test called the async tick, so it
    shipped. These assertions fail loudly if that recurs."""

    def test_key_async_entrypoints_defined(self):
        import inspect
        for name in ("_tick_carbon_intensity", "_engine_tick", "engine_loop_task"):
            self.assertTrue(hasattr(engine, name), f"{name} is not defined")
            self.assertTrue(inspect.iscoroutinefunction(getattr(engine, name)),
                            f"{name} is not a coroutine function")

    def test_tick_and_backfill_are_separate(self):
        import inspect
        self.assertTrue(hasattr(engine, "_maybe_backfill_historical_carbon"))
        self.assertFalse(
            inspect.iscoroutinefunction(engine._maybe_backfill_historical_carbon))
        tick_src = inspect.getsource(engine._tick_carbon_intensity)
        maybe_src = inspect.getsource(engine._maybe_backfill_historical_carbon)
        # The tick owns the CI-store logic and the backfill trigger; the
        # scheduler must not have absorbed the tick body.
        self.assertIn("upsert_carbon_intensity", tick_src)
        self.assertIn("_maybe_backfill_historical_carbon()", tick_src)
        self.assertNotIn("upsert_carbon_intensity", maybe_src)
        self.assertNotIn("_last_ci_fetch", maybe_src)


    def test_backfill_runs_db_on_loop_thread_not_executor(self):
        # Regression guard for the SQLite SQLITE_MISUSE crash: the backfill shared
        # the engine's single connection across threads by running its DB writes on
        # a run_in_executor worker while the main loop drove _drain_pass2_queue on
        # the same connection. The fix: the worker is a coroutine (DB on the loop
        # thread) dispatched via create_task, with only the network fetch offloaded.
        import inspect
        self.assertTrue(
            inspect.iscoroutinefunction(engine._run_historical_carbon_backfill),
            "backfill worker must be a coroutine so DB access runs on the loop "
            "thread, not an executor thread sharing the engine's SQLite connection")
        src = inspect.getsource(engine._maybe_backfill_historical_carbon)
        self.assertIn("create_task", src,
                      "scheduler must dispatch the worker as a loop task")
        self.assertNotIn("run_in_executor", src,
                         "scheduler must NOT offload the worker to a thread")


if __name__ == "__main__":
    unittest.main()