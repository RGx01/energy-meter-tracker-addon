"""Tests for engine.probe_consumption_retention + diagnose_consumption_retention
(read-only historical-import spikes)."""
import os
import sys
import types
import asyncio
import unittest
from unittest.mock import MagicMock

# ── Stubs so engine imports without HA/filesystem (mirrors test_recorder_probe) ─
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

from block_store import BlockStore, migrate_json_to_sqlite
_boot = BlockStore(":memory:")
_boot.insert_config_period({"meters": {"electricity_main": {"meta": {
    "timezone": "UTC", "billing_day": 1, "block_minutes": 30,
    "currency_symbol": "£", "currency_code": "GBP"}}}})
bs = types.ModuleType("block_store")
bs.BlockStore = BlockStore
bs.open_block_store = lambda path: _boot
bs.migrate_json_to_sqlite = migrate_json_to_sqlite
bs.IMPORTED_SOURCE_API = "imported_api"
bs.IMPORTED_SOURCE_CSV = "imported_csv"
bs.IMPORTED_SOURCE_BLENDED = "imported_blended"
sys.modules["block_store"] = bs

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import engine


def _row(iso):
    return {"consumption": 0.1, "interval_start": iso, "interval_end": iso}


class _FakeClient:
    """Async stand-in for KrakenAPIClient covering the probe surface."""
    def __init__(self, *, account=None, boundaries=None, below_rows=0):
        self._account = account or {}
        self._b = boundaries or {}          # (serial, newest) -> row|None
        self._below = below_rows

    async def get_account(self, *a, **k):
        return self._account

    async def get_consumption_boundary(self, mpan, serial, *, newest=False,
                                       period_from=None):
        return self._b.get((serial, newest))

    async def get_consumption(self, mpan, serial, *, period_from=None,
                              period_to=None, order_by="period"):
        return [_row("x")] * self._below


class _EngineState:
    """Save/restore the engine module globals the probes read."""
    def setUp(self):
        self._client = engine._kraken_client
        self._disc = engine._kraken_discovery

    def tearDown(self):
        engine._kraken_client = self._client
        engine._kraken_discovery = self._disc


class TestRetentionProbe(_EngineState, unittest.TestCase):
    def _run(self):
        return asyncio.run(engine.probe_consumption_retention())

    def test_no_api(self):
        engine._kraken_client = None
        engine._kraken_discovery = None
        r = self._run()
        self.assertFalse(r["ok"])
        self.assertEqual(r["reason"], "no_api")

    def test_import_and_export_lag(self):
        engine._kraken_client = _FakeClient(boundaries={
            ("IMPS", False): _row("2024-06-12T13:00:00Z"),
            ("IMPS", True): _row("2026-07-18T20:00:00Z"),
            ("EXPS", False): _row("2024-06-29T03:00:00Z"),
            ("EXPS", True): _row("2026-07-18T20:00:00Z"),
        })
        engine._kraken_discovery = {
            "import": {"mpan": "IMPAN", "serial": "IMPS"},
            "export": {"mpan": "EXPAN", "serial": "EXPS"},
        }
        r = self._run()
        self.assertTrue(r["ok"])
        self.assertEqual(r["channels"]["import"]["earliest"], "2024-06-12T13:00:00+00:00")
        self.assertTrue(r["channels"]["export"]["available"])
        self.assertEqual(r["lag_days"], 16)   # 17d - time-of-day → 16 whole days

    def test_no_export_meter(self):
        engine._kraken_client = _FakeClient(boundaries={
            ("IMPS", False): _row("2024-06-12T13:00:00Z"),
            ("IMPS", True): _row("2026-07-18T20:00:00Z"),
        })
        engine._kraken_discovery = {"import": {"mpan": "IMPAN", "serial": "IMPS"}}
        r = self._run()
        self.assertFalse(r["channels"]["export"]["available"])
        self.assertIsNone(r["lag_days"])


def _account(meter_points):
    return {"number": "A-1", "properties": [
        {"electricity_meter_points": meter_points}]}


class TestConsumptionDiagnostic(_EngineState, unittest.TestCase):
    def _run(self):
        return asyncio.run(engine.diagnose_consumption_retention())

    def test_no_api(self):
        engine._kraken_client = None
        engine._kraken_discovery = None
        r = self._run()
        self.assertFalse(r["ok"])

    def test_meter_exchange_shows_both_serials(self):
        # One MPAN, two meters — an old serial reaching 2024-07-01 and a new one
        # from 2024-07-20 (exactly what the newest-serial probe would miss).
        acct = _account([{"mpan": "IMPAN", "is_export": False, "meters": [
            {"serial_number": "OLDSERIAL01"},
            {"serial_number": "NEWSERIAL20"},
        ]}])
        engine._kraken_client = _FakeClient(account=acct, below_rows=0, boundaries={
            ("OLDSERIAL01", False): _row("2024-07-01T00:00:00Z"),
            ("OLDSERIAL01", True): _row("2024-07-19T23:30:00Z"),
            ("NEWSERIAL20", False): _row("2024-07-20T00:00:00Z"),
            ("NEWSERIAL20", True): _row("2026-07-18T20:00:00Z"),
        })
        engine._kraken_discovery = {"import": {"mpan": "IMPAN", "serial": "NEWSERIAL20"}}
        r = self._run()
        self.assertTrue(r["ok"])
        mp = r["meter_points"][0]
        self.assertEqual(mp["meter_count"], 2)
        earliest = sorted(m["earliest"][:10] for m in mp["meters"])
        self.assertEqual(earliest, ["2024-07-01", "2024-07-20"])

    def test_below_boundary_flag(self):
        # Single serial, but rows DO exist below the reported earliest → the
        # boundary query is under-reaching (a query artifact, not a real limit).
        acct = _account([{"mpan": "IMPAN", "is_export": False, "meters": [
            {"serial_number": "S1"}]}])
        engine._kraken_client = _FakeClient(account=acct, below_rows=48, boundaries={
            ("S1", False): _row("2024-07-20T00:00:00Z"),
            ("S1", True): _row("2026-07-18T20:00:00Z"),
        })
        engine._kraken_discovery = {"import": {"mpan": "IMPAN", "serial": "S1"}}
        r = self._run()
        m = r["meter_points"][0]["meters"][0]
        self.assertEqual(m["rows_just_below_earliest"], 48)
        self.assertIsNotNone(m["earliest_below"])   # the below row's date is surfaced

    def test_agreement_history_surfaced(self):
        # Two agreements (a tariff change on 2024-11-01) while consumption starts
        # 2024-07-01 — proves the tariff change is NOT the data boundary.
        acct = _account([{"mpan": "IMPAN", "is_export": False,
                          "meters": [{"serial_number": "S1"}],
                          "agreements": [
                              {"valid_from": "2024-11-01T00:00:00Z", "valid_to": None,
                               "tariff_code": "E-1R-IOG-B"},
                              {"valid_from": "2024-07-01T00:00:00Z",
                               "valid_to": "2024-11-01T00:00:00Z",
                               "tariff_code": "E-1R-VAR-A"}]}])
        engine._kraken_client = _FakeClient(account=acct, boundaries={
            ("S1", False): _row("2024-07-01T00:00:00Z"),
            ("S1", True): _row("2026-07-18T20:00:00Z"),
        })
        engine._kraken_discovery = {"import": {"mpan": "IMPAN", "serial": "S1"}}
        mp = self._run()["meter_points"][0]
        self.assertEqual(mp["agreement_count"], 2)
        # Sorted earliest-first → supply start is the first agreement.
        self.assertEqual(mp["supply_start"], "2024-07-01T00:00:00Z")
        self.assertEqual(mp["agreements"][0]["tariff"], "E-1R-VAR-A")

    def test_serials_are_tail_masked(self):
        acct = _account([{"mpan": "IMPAN", "is_export": False, "meters": [
            {"serial_number": "ABCDEF123456"}]}])
        engine._kraken_client = _FakeClient(account=acct, boundaries={
            ("ABCDEF123456", False): _row("2024-07-20T00:00:00Z"),
            ("ABCDEF123456", True): _row("2026-07-18T20:00:00Z"),
        })
        engine._kraken_discovery = {"import": {"mpan": "IMPAN", "serial": "ABCDEF123456"}}
        r = self._run()
        tail = r["meter_points"][0]["meters"][0]["serial_tail"]
        self.assertEqual(tail, "…123456")
        self.assertNotIn("ABCDEF", tail)


class TestApiImportPlan(unittest.TestCase):
    def setUp(self):
        self._c, self._d, self._s = (engine._kraken_client,
                                     engine._kraken_discovery, engine._store)

    def tearDown(self):
        (engine._kraken_client, engine._kraken_discovery,
         engine._store) = self._c, self._d, self._s

    def _run(self, **kw):
        return asyncio.run(engine.plan_api_import(**kw))

    def test_no_api(self):
        engine._kraken_client = None
        engine._kraken_discovery = None
        self.assertFalse(self._run()["ok"])

    def test_no_go_live(self):
        engine._kraken_client = object()
        engine._kraken_discovery = {"import": {"mpan": "IM", "serial": "IMPS"}}
        engine._store = types.SimpleNamespace(get_oldest_block_start=lambda *a, **k: None)
        r = self._run()
        self.assertFalse(r["ok"])
        self.assertEqual(r["reason"], "no_go_live")

    def test_window_from_agreement_floor(self):
        engine._kraken_client = object()
        engine._kraken_discovery = {"import": {
            "mpan": "IM", "serial": "IMPS",
            "agreements": [{"valid_from": "2024-06-01T00:00:00Z"}]}}
        engine._store = types.SimpleNamespace(
            get_oldest_block_start=lambda *a, **k: "2026-07-01T00:00:00")
        r = self._run()
        imp = r["channels"]["import"]
        self.assertTrue(imp["ok"])
        self.assertEqual(imp["window"]["from"], "2024-06-01T00:00:00")   # ~join
        self.assertEqual(imp["window"]["to"], "2026-07-01T00:00:00")     # go-live
        self.assertGreater(imp["chunk_count"], 0)

    def test_requested_from_overrides_floor(self):
        engine._kraken_client = object()
        engine._kraken_discovery = {"import": {
            "mpan": "IM", "serial": "IMPS",
            "agreements": [{"valid_from": "2024-06-01T00:00:00Z"}]}}
        engine._store = types.SimpleNamespace(
            get_oldest_block_start=lambda *a, **k: "2026-07-01T00:00:00")
        r = self._run(requested_from="2025-01-01T00:00:00")
        self.assertEqual(r["channels"]["import"]["window"]["from"], "2025-01-01T00:00:00")


import api_import as _ai


def _nu(s):
    return _ai._iso(_ai._naive_utc(s)) if s else None


def _iv(start, kwh, cost_incl, *, off_peak=True, standing=0.0112):
    """A parsed measurement interval (get_measurements return shape)."""
    return {"start": start, "kwh": kwh, "cost_incl": cost_incl,
            "off_peak": off_peak, "standing_incl": standing}


class _FakeMeasClient:
    """Fake for the GraphQL Measurements apply path: returns parsed intervals."""
    def __init__(self, intervals, rate=None):
        self.intervals = intervals
        self.rate = rate or {"pointsUsed": 0, "pointsLimit": 1000, "remaining": 1000}

    async def get_rate_limit(self):
        return self.rate

    async def get_unit_rates(self, product, tariff, *, rate_type="standard-unit-rates",
                             period_from=None, period_to=None):
        # Flat 7p inc-VAT tariff so build_rate_schedule yields a usable schedule.
        if rate_type == "standard-unit-rates":
            return [{"value_inc_vat": 7.0, "valid_from": "2024-01-01T00:00:00Z",
                     "valid_to": None, "payment_method": "DIRECT_DEBIT"}]
        return []

    async def get_measurements(self, mpan, start, end, *, direction="CONSUMPTION",
                               **kw):
        pf, pt = _nu(start), _nu(end)
        return [dict(i) for i in self.intervals
                if (pf is None or i["start"] >= pf) and (pt is None or i["start"] < pt)]

    async def get_standing_charges(self, product, tariff, *, period_from=None,
                                   period_to=None):
        # Flat 40p/day standing charge → £0.40/day (pence in the API).
        return [{"value_inc_vat": 40.0, "value_exc_vat": 38.0,
                 "valid_from": "2024-01-01T00:00:00Z", "valid_to": None}]

    async def get_consumption(self, mpan, serial, *, period_from=None,
                              period_to=None, order_by="period"):
        # REST raw half-hours for the export fallback (set via .rest_rows).
        pf, pt = _nu(period_from), _nu(period_to)
        out = []
        for r in getattr(self, "rest_rows", []):
            st = _nu(r["interval_start"])
            if (pf is None or st >= pf) and (pt is None or st < pt):
                out.append(dict(r))
        return out


class TestApiImportApply(unittest.TestCase):
    def setUp(self):
        self._c, self._d, self._s = (engine._kraken_client,
                                     engine._kraken_discovery, engine._store)
        self.store = BlockStore(":memory:")
        self.store.insert_config_period({"meters": {"electricity_main": {"meta": {
            "billing_day": 1, "block_minutes": 30, "timezone": "UTC",
            "currency_symbol": "£", "currency_code": "GBP", "site": "Home"}}}})
        cp = self.store._conn.execute("SELECT id FROM config_periods LIMIT 1").fetchone()["id"]
        # Existing live block → go-live = 2024-07-01T02:00:00.
        self.store._conn.execute(
            "INSERT INTO blocks (block_start, block_end, meter_id, config_period_id,"
            " interpolated, imp_kwh) VALUES (?,?,?,?,0,1.0)",
            ("2024-07-01T02:00:00", "2024-07-01T02:30:00", "electricity_main", cp))
        self.store._conn.commit()
        engine._store = self.store
        engine._hist_rate_segs_cache.clear()
        engine._hist_standing_segs_cache.clear()
        engine._hist_floor_cache.clear()
        engine._kraken_discovery = {"import": {
            "mpan": "IM", "serial": "IS",
            "agreements": [{"tariff_code": "E-1R-VAR-22-11-01-A",
                            "valid_from": "2024-01-01T00:00:00Z", "valid_to": None}]}}

    def tearDown(self):
        (engine._kraken_client, engine._kraken_discovery,
         engine._store) = self._c, self._d, self._s
        engine._hist_rate_segs_cache.clear()
        engine._hist_standing_segs_cache.clear()
        engine._hist_floor_cache.clear()

    def _contig(self):
        # Four off-peak intervals just below go-live; cost = 7p/kWh (IOG off-peak).
        return [_iv("2024-07-01T00:00:00", 0.108, 0.007560378),
                _iv("2024-07-01T00:30:00", 0.127, 0.008890445),
                _iv("2024-07-01T01:00:00", 0.105, 0.007350368),
                _iv("2024-07-01T01:30:00", 0.124, 0.008680434)]

    def test_writes_exact_dispatch_aware_blocks(self):
        engine._kraken_client = _FakeMeasClient(self._contig())
        r = asyncio.run(engine.import_api_history())
        ch = r["channels"]["import"]
        self.assertEqual(ch["imported"], 4)
        self.assertEqual(ch["written"], 4)
        self.assertEqual(ch["priced"], 4)
        self.assertEqual(ch["off_peak_intervals"], 4)          # from the API label
        rows = self.store._conn.execute(
            "SELECT imp_kwh, imp_rate, imp_cost, source FROM blocks "
            "WHERE source='imported_api' ORDER BY block_start").fetchall()
        self.assertEqual(len(rows), 4)
        self.assertAlmostEqual(rows[0]["imp_cost"], 0.007560378, places=7)  # billed cost £
        self.assertAlmostEqual(rows[0]["imp_rate"], 0.07, places=3)         # cost÷kWh

    def test_standing_charge_from_tariff_schedule(self):
        # Standing charge must come from the tariff standing-charge schedule
        # (40p/day here), NOT the patchy Measurements STANDING_CHARGE_COST — so
        # imported days can't silently land at £0.00.
        engine._kraken_client = _FakeMeasClient(self._contig())
        asyncio.run(engine.import_api_history(
            restart=True, max_chunks=6, pace_s=0))
        row = self.store._conn.execute(
            "SELECT standing_charge FROM blocks WHERE source='imported_api' "
            "LIMIT 1").fetchone()
        self.assertAlmostEqual(row["standing_charge"], 0.40, places=3)

    def test_partial_day_standing_extrapolated(self):
        # A partial day (only 2 of 48 half-hours) with no schedule coverage must
        # still be billed the FULL daily standing charge, not a prorated fraction.
        class _NoStanding(_FakeMeasClient):
            async def get_standing_charges(self, *a, **k):
                return []          # empty schedule → force the Measurements fallback
        per = round(0.4752 / 48, 8)      # Measurements spreads daily/48 per interval
        engine._kraken_client = _NoStanding([
            _iv("2024-07-01T00:00:00", 0.1, 0.007, standing=per),
            _iv("2024-07-01T00:30:00", 0.1, 0.007, standing=per)])
        asyncio.run(engine.import_api_history(
            restart=True, max_chunks=6, pace_s=0))
        row = self.store._conn.execute(
            "SELECT standing_charge FROM blocks WHERE source='imported_api' "
            "LIMIT 1").fetchone()
        self.assertAlmostEqual(row["standing_charge"], 0.4752, places=4)

    def test_standing_carries_forward_past_closed_record(self):
        # The API can return a standing-charge record with an explicit valid_to
        # and no open successor; a standing charge persists until it next changes,
        # so a date AFTER that valid_to must still resolve (not drop to None and
        # fall back to the slightly-different Measurements figure).
        from kraken_rates import RateSchedule
        sched = RateSchedule.from_api_records([
            {"value_inc_vat": 50.4559, "value_exc_vat": 48.0,
             "valid_from": "2026-03-17T00:00:00Z",
             "valid_to": "2026-04-06T00:00:00Z"}])          # closed, no successor
        segs = [("2026-03-17T00:00:00", None, sched)]
        self.assertAlmostEqual(engine._standing_for(segs, "2026-03-20"),
                               0.504559, places=6)          # within the record
        self.assertAlmostEqual(engine._standing_for(segs, "2026-05-01"),
                               0.504559, places=6)          # AFTER — carried forward

    def test_cost_missing_priced_from_tariff(self):
        # A half-hour Measurements returns with kWh but NO cost (a settlement gap)
        # must still be priced — from the tariff schedule, not left at £0.
        engine._kraken_client = _FakeMeasClient(
            [_iv("2024-07-01T00:00:00", 1.0, None, off_peak=None)])   # cost=None
        asyncio.run(engine.import_api_history(restart=True, max_chunks=6, pace_s=0))
        row = self.store._conn.execute(
            "SELECT imp_kwh, imp_rate, imp_cost FROM blocks "
            "WHERE source='imported_api'").fetchone()
        self.assertIsNotNone(row)
        self.assertAlmostEqual(row["imp_rate"], 0.07, places=4)   # tariff schedule
        self.assertAlmostEqual(row["imp_cost"], 0.07, places=4)   # 1.0 kWh × 0.07

    def test_dry_run_writes_nothing(self):
        engine._kraken_client = _FakeMeasClient(self._contig())
        r = asyncio.run(engine.import_api_history(dry_run=True))
        self.assertEqual(r["channels"]["import"]["written"], 0)
        self.assertEqual(r["channels"]["import"]["priced"], 4)
        n = self.store._conn.execute(
            "SELECT COUNT(*) FROM blocks WHERE source='imported_api'").fetchone()[0]
        self.assertEqual(n, 0)

    def test_records_and_reports_gap(self):
        # 01:00 missing → a 1-slot gap must be persisted and surfaced via
        # api_import_gaps (so the import page can show it without re-querying).
        engine._kraken_client = _FakeMeasClient([
            _iv("2024-07-01T00:00:00", 0.1, 0.007),
            _iv("2024-07-01T00:30:00", 0.1, 0.007),
            _iv("2024-07-01T01:30:00", 0.1, 0.007)])       # 01:00 missing
        asyncio.run(engine.import_api_history(
            restart=True, max_chunks=6, pace_s=0))
        g = engine.api_import_gaps()["channels"]["import"]
        self.assertEqual(g["missing"], 1)
        self.assertEqual(g["gap_count"], 1)
        self.assertEqual(g["gaps"][0]["from"][:16], "2024-07-01T01:00")
        self.assertEqual(g["gaps"][0]["to"][:16], "2024-07-01T01:00")

    def test_no_gap_when_contiguous(self):
        engine._kraken_client = _FakeMeasClient(self._contig())
        asyncio.run(engine.import_api_history(
            restart=True, max_chunks=6, pace_s=0))
        self.assertEqual(engine.api_import_gaps()["channels"]["import"]["missing"], 0)

    def test_gap_does_not_halt_walk(self):
        # A missing half-hour must NOT permanently halt the backfill — real
        # supplier data has gaps (settlement thinning, DST, comms). Import every
        # present half-hour and keep going; the hole is simply left empty.
        engine._kraken_client = _FakeMeasClient([
            _iv("2024-07-01T00:00:00", 0.1, 0.007),
            _iv("2024-07-01T00:30:00", 0.1, 0.007),
            _iv("2024-07-01T01:30:00", 0.1, 0.007)])       # 01:00 missing
        r = asyncio.run(engine.import_api_history(
            restart=True, max_chunks=6, pace_s=0))
        ch = r["channels"]["import"]
        self.assertEqual(ch["imported"], 3)                 # all present; gap skipped
        self.assertIsNone(ch["halted_at"])

    def test_writes_export_channel(self):
        # Export MPAN → direction GENERATION → exp_* columns.
        engine._kraken_discovery = {"export": {
            "mpan": "EX", "serial": "ES",
            "agreements": [{"tariff_code": "E-1R-OUTGOING-FIX-A",
                            "valid_from": "2024-06-01T00:00:00Z", "valid_to": None}]}}
        engine._kraken_client = _FakeMeasClient(self._contig())
        r = asyncio.run(engine.import_api_history())
        ch = r["channels"]["export"]
        self.assertEqual(ch["written"], 4)
        rows = self.store._conn.execute(
            "SELECT exp_kwh, exp_cost, source FROM blocks "
            "WHERE source='imported_api' ORDER BY block_start").fetchall()
        self.assertEqual(len(rows), 4)
        self.assertIsNotNone(rows[0]["exp_kwh"])
        self.assertAlmostEqual(rows[0]["exp_cost"], 0.007560378, places=7)

    def test_no_go_live(self):
        engine._store = types.SimpleNamespace(get_oldest_block_start=lambda *a, **k: None)
        engine._kraken_client = _FakeMeasClient(self._contig())
        r = asyncio.run(engine.import_api_history())
        self.assertFalse(r["ok"])

    def test_no_store_guard(self):
        # If a restore has closed the store, the import must bail — never write to
        # a freed SQLite connection (that segfaults the process).
        engine._store = None
        engine._kraken_client = _FakeMeasClient(self._contig())
        r = asyncio.run(engine.import_api_history())
        self.assertFalse(r["ok"])
        self.assertEqual(r["reason"], "no_store")

    def test_resume_across_calls(self):
        # max_chunks=2 over a 6-month window → first call imports the recent data
        # (not done), second call resumes below the checkpoint and completes.
        engine._kraken_client = _FakeMeasClient(self._contig())
        r1 = asyncio.run(engine.import_api_history(restart=True, max_chunks=2, pace_s=0))
        self.assertFalse(r1["done"])
        self.assertEqual(r1["channels"]["import"]["written"], 4)
        r2 = asyncio.run(engine.import_api_history(restart=False, max_chunks=2, pace_s=0))
        self.assertTrue(r2["done"])
        self.assertEqual(r2["channels"]["import"]["written"], 0)   # nothing older
        total = self.store._conn.execute(
            "SELECT COUNT(*) FROM blocks WHERE source='imported_api'").fetchone()[0]
        self.assertEqual(total, 4)

    def test_rate_limit_pauses(self):
        # Remaining points below the 25% headroom → pause before fetching.
        engine._kraken_client = _FakeMeasClient(
            self._contig(), rate={"pointsUsed": 95, "pointsLimit": 100, "remaining": 5})
        r = asyncio.run(engine.import_api_history(restart=True, max_chunks=6,
                                                  pace_s=0, headroom_frac=0.25))
        ch = r["channels"]["import"]
        self.assertEqual(ch["reason"], "rate_limit")
        self.assertFalse(r["done"])
        self.assertEqual(ch["written"], 0)


class TestExportRestFallback(unittest.TestCase):
    """Export: where the half-hourly Measurements feed has no data, raw REST
    half-hours fill in, priced from the tariff schedule (flat / Agile Outgoing)."""

    def setUp(self):
        self._c, self._d, self._s = (engine._kraken_client,
                                     engine._kraken_discovery, engine._store)
        self.store = BlockStore(":memory:")
        self.store.insert_config_period({"meters": {"electricity_main": {"meta": {
            "billing_day": 1, "block_minutes": 30, "timezone": "UTC",
            "currency_symbol": "£", "currency_code": "GBP", "site": "Home"}}}})
        cp = self.store._conn.execute(
            "SELECT id FROM config_periods LIMIT 1").fetchone()["id"]
        self.store._conn.execute(
            "INSERT INTO blocks (block_start, block_end, meter_id, "
            "config_period_id, interpolated, imp_kwh) VALUES (?,?,?,?,0,1.0)",
            ("2024-07-01T02:00:00", "2024-07-01T02:30:00", "electricity_main", cp))
        self.store._conn.commit()
        engine._store = self.store
        engine._hist_rate_segs_cache.clear()
        engine._hist_standing_segs_cache.clear()
        engine._hist_floor_cache.clear()
        engine._kraken_discovery = {"export": {
            "mpan": "EX", "serial": "ES",
            "agreements": [{"tariff_code": "E-1R-OUTGOING-FIX-12M-A",
                            "valid_from": "2024-06-01T00:00:00Z",
                            "valid_to": None}]}}

    def tearDown(self):
        (engine._kraken_client, engine._kraken_discovery,
         engine._store) = self._c, self._d, self._s
        engine._hist_rate_segs_cache.clear()
        engine._hist_standing_segs_cache.clear()
        engine._hist_floor_cache.clear()

    def test_rest_fills_below_measurements(self):
        client = _FakeMeasClient([
            _iv("2024-07-01T01:00:00", 0.5, 0.075, off_peak=None),
            _iv("2024-07-01T01:30:00", 0.6, 0.090, off_peak=None)])
        # REST returns the contiguous half-hours (incl. two BELOW Measurements),
        # AND the chunk-top boundary interval (02:00 = go-live) which REST includes
        # but must be EXCLUDED — re-adding it breaks the contiguity check.
        client.rest_rows = [
            {"interval_start": "2024-07-01T00:00:00Z", "consumption": 0.4},
            {"interval_start": "2024-07-01T00:30:00Z", "consumption": 0.3},
            {"interval_start": "2024-07-01T01:00:00Z", "consumption": 0.5},  # dup
            {"interval_start": "2024-07-01T01:30:00Z", "consumption": 0.6},  # dup
            {"interval_start": "2024-07-01T02:00:00Z", "consumption": 0.9}]  # boundary
        engine._kraken_client = client
        r = asyncio.run(engine.import_api_history(
            restart=True, max_chunks=6, pace_s=0))
        ch = r["channels"]["export"]
        self.assertEqual(ch["written"], 4)   # 2 Measurements + 2 REST; NOT the boundary
        self.assertIsNone(self.store._conn.execute(
            "SELECT 1 FROM blocks WHERE block_start='2024-07-01T02:00:00' "
            "AND source='imported_api'").fetchone())   # boundary excluded
        rows = self.store._conn.execute(
            "SELECT block_start, exp_kwh, exp_cost, exp_rate FROM blocks "
            "WHERE source='imported_api' ORDER BY block_start").fetchall()
        self.assertEqual(len(rows), 4)
        # earliest block is REST-filled: kWh from REST, priced via tariff (7p flat)
        self.assertAlmostEqual(rows[0]["exp_kwh"], 0.4, places=6)
        self.assertAlmostEqual(rows[0]["exp_rate"], 0.07, places=4)
        self.assertAlmostEqual(rows[0]["exp_cost"], 0.4 * 0.07, places=5)
        # Measurements block keeps its exact billed cost
        self.assertAlmostEqual(rows[2]["exp_cost"], 0.075, places=5)


class TestApiImportJob(unittest.TestCase):
    """Background job: worker completion + pause/cancel control."""
    def setUp(self):
        self._c, self._d, self._s = (engine._kraken_client,
                                     engine._kraken_discovery, engine._store)
        self._job = engine._api_import_job
        self.store = BlockStore(":memory:")
        self.store.insert_config_period({"meters": {"electricity_main": {"meta": {
            "billing_day": 1, "block_minutes": 30, "timezone": "UTC",
            "currency_symbol": "£", "currency_code": "GBP", "site": "Home"}}}})
        cp = self.store._conn.execute("SELECT id FROM config_periods LIMIT 1").fetchone()["id"]
        self.store._conn.execute(
            "INSERT INTO blocks (block_start, block_end, meter_id, config_period_id,"
            " interpolated, imp_kwh) VALUES (?,?,?,?,0,1.0)",
            ("2024-07-01T02:00:00", "2024-07-01T02:30:00", "electricity_main", cp))
        self.store._conn.commit()
        engine._store = self.store
        engine._api_import_job = {"status": "idle"}
        engine._hist_rate_segs_cache.clear()
        engine._hist_standing_segs_cache.clear()
        engine._hist_floor_cache.clear()
        engine._kraken_discovery = {"import": {
            "mpan": "IM", "serial": "IS",
            "agreements": [{"tariff_code": "E-1R-VAR-22-11-01-A",
                            "valid_from": "2024-01-01T00:00:00Z", "valid_to": None}]}}

    def tearDown(self):
        (engine._kraken_client, engine._kraken_discovery,
         engine._store) = self._c, self._d, self._s
        engine._api_import_job = self._job
        engine._hist_rate_segs_cache.clear()
        engine._hist_standing_segs_cache.clear()
        engine._hist_floor_cache.clear()

    def _contig(self):
        return [_iv("2024-07-01T00:00:00", 0.108, 0.00756),
                _iv("2024-07-01T00:30:00", 0.127, 0.00889),
                _iv("2024-07-01T01:00:00", 0.105, 0.00735),
                _iv("2024-07-01T01:30:00", 0.124, 0.00868)]

    def test_status_hides_task_and_control_guard(self):
        engine._api_import_job = {"status": "idle", "task": object()}
        self.assertNotIn("task", engine.api_import_status())
        self.assertFalse(engine.api_import_control("pause")["ok"])   # nothing running

    def test_worker_completes(self):
        engine._kraken_client = _FakeMeasClient(self._contig())
        asyncio.run(engine.run_api_import_job(max_chunks=6, pace_s=0))
        self.assertEqual(engine._api_import_job["status"], "done")
        self.assertEqual(engine._api_import_job["written"]["import"], 4)

    def test_worker_cancel_between_slices(self):
        class _Cancelling(_FakeMeasClient):
            async def get_measurements(self, *a, **kw):
                engine._api_import_job["control"] = "cancel"   # cancel after fetching
                return await _FakeMeasClient.get_measurements(self, *a, **kw)
        engine._kraken_client = _Cancelling(self._contig())
        asyncio.run(engine.run_api_import_job(max_chunks=1, pace_s=0))
        self.assertEqual(engine._api_import_job["status"], "cancelled")

    def test_worker_regenerates_charts_on_completion(self):
        # Charts must refresh when the import finishes, not only on the next
        # live block finalise.
        calls = []
        orig = engine.generate_charts
        # Match the real signature generate_charts(store, config=None): the import
        # job now AWAITS the off-loop render, which resolves + passes config.
        engine.generate_charts = lambda store, config=None: calls.append(store)
        try:
            engine._kraken_client = _FakeMeasClient(self._contig())
            asyncio.run(engine.run_api_import_job(max_chunks=6, pace_s=0))
        finally:
            engine.generate_charts = orig
        self.assertEqual(engine._api_import_job["status"], "done")
        self.assertTrue(calls, "charts should regenerate after a completed import")

    def test_regen_helper_runs_when_written(self):
        calls = []
        orig = engine.generate_charts
        engine.generate_charts = lambda store: calls.append(store)
        try:
            engine._regen_charts_after_import({"written": {"import": 3, "export": 0}})
        finally:
            engine.generate_charts = orig
        self.assertTrue(calls)

    def test_regen_helper_skips_when_nothing_written(self):
        calls = []
        orig = engine.generate_charts
        engine.generate_charts = lambda store: calls.append(store)
        try:
            engine._regen_charts_after_import({"written": {"import": 0, "export": 0}})
        finally:
            engine.generate_charts = orig
        self.assertFalse(calls)


class TestDataFloor(unittest.TestCase):
    """The import floor follows the DATA, not the agreement: it extends earlier
    when Measurements returns readings before the earliest supplier agreement."""

    def setUp(self):
        self._c, self._a = (engine._kraken_client,
                            engine._kraken_account_number)
        engine._kraken_account_number = "A-7"
        engine._hist_floor_cache.clear()

    def tearDown(self):
        (engine._kraken_client,
         engine._kraken_account_number) = self._c, self._a
        engine._hist_floor_cache.clear()

    def _info(self):
        # export agreement starts 2025-03-08 (like the reported account)
        return {"mpan": "EX", "agreements": [
            {"tariff_code": "E-1R-OUTGOING", "valid_from": "2025-03-08T00:00:00Z",
             "valid_to": None}]}

    def test_extends_floor_when_data_predates_agreement(self):
        # a reading ~6 months before the agreement (in the first probe window)
        engine._kraken_client = _FakeMeasClient([
            _iv("2024-09-10T00:00:00", 0.4, 0.06)])
        floor = asyncio.run(engine._data_floor_for("export", self._info()))
        self.assertEqual(floor[:10], "2024-09-10")

    def test_keeps_agreement_when_no_earlier_data(self):
        engine._kraken_client = _FakeMeasClient([])
        floor = asyncio.run(engine._data_floor_for("export", self._info()))
        self.assertEqual(floor[:10], "2025-03-08")

    def test_no_client_returns_agreement(self):
        engine._kraken_client = None
        floor = asyncio.run(engine._data_floor_for("export", self._info()))
        self.assertEqual(floor[:10], "2025-03-08")


class TestDiagnoseImportRange(unittest.TestCase):
    """diagnose_import_range: reports the per-channel start floor and whether
    Measurements data exists before it."""

    def setUp(self):
        self._c, self._d, self._a = (engine._kraken_client,
                                     engine._kraken_discovery,
                                     engine._kraken_account_number)
        engine._kraken_account_number = "A-TEST-7"
        engine._hist_floor_cache.clear()
        engine._kraken_discovery = {"import": {
            "mpan": "IM", "serial": "IS",
            "agreements": [{"tariff_code": "E-1R-VAR",
                            "valid_from": "2024-07-01T00:00:00Z",
                            "valid_to": None}]}}

    def tearDown(self):
        (engine._kraken_client, engine._kraken_discovery,
         engine._kraken_account_number) = self._c, self._d, self._a
        engine._hist_floor_cache.clear()

    def test_no_api(self):
        engine._kraken_client = None
        res = asyncio.run(engine.diagnose_import_range())
        self.assertFalse(res["ok"])

    def test_no_data_before_floor(self):
        engine._kraken_client = _FakeMeasClient([])
        res = asyncio.run(engine.diagnose_import_range())
        self.assertTrue(res["ok"])
        imp = res["channels"]["import"]
        self.assertEqual(imp["import_starts_at"][:10], "2024-07-01")
        self.assertFalse(imp["data_before_floor"])

    def test_data_before_floor_detected(self):
        # a reading inside the first 30-day probe window (~6 months before the
        # 2024-07-01 agreement) → the floor extends and flags data earlier
        engine._kraken_client = _FakeMeasClient([
            _iv("2024-01-15T00:00:00", 0.1, 0.007)])
        res = asyncio.run(engine.diagnose_import_range())
        imp = res["channels"]["import"]
        self.assertTrue(imp["data_before_floor"])
        self.assertEqual(imp["import_starts_at"][:10], "2024-01-15")
        self.assertEqual(imp["earliest_seen_before_floor"][:10], "2024-01-15")


if __name__ == "__main__":
    unittest.main()