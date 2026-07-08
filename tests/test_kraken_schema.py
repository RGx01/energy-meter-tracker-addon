"""
Tests for 3.0.0 Chunk 1a — schema foundations and Kraken storage layer.

Covers:
  - The six new blocks columns exist (fresh install + simulated upgrade)
  - kraken_state table + get/set
  - Named unique index and partial sweep indexes present
  - Duplicate detection is a clean no-op on a constrained DB
  - get_block_by_start / get_block_by_id
  - upsert_kraken_block classification (positive / over-threshold / zero / cad)
  - PASS 2 re-run queue: get_blocks_needing_pass2_rerun / clear_pass2_rerun_flag
  - Provisional timeout: get_timed_out_provisionals / finalise_timed_out_provisionals
  - Drift alerts: get_drift_alerts / dismiss_drift_alerts
  - get_settled_to

These methods touch only 3.0.0 columns/tables; the pre-3.0.0 suites
(test_block_store, test_engine, test_server, test_usage_stats_vs_billing)
prove existing behaviour is unchanged.
"""

import sqlite3
import unittest
import os
import asyncio
from datetime import datetime, timedelta, timezone

from block_store import BlockStore

# ── Stubs so `import engine` (in TestBoundaryCallback) works without the HA /
# aiohttp runtime deps. Harmless for the block_store tests below, which never
# import these modules. Mirrors the stub block in test_engine.py.
import sys as _sys, types as _types
for _name, _attrs in (
    ("energy_engine_io", {"ensure_dir": lambda *a, **k: None,
                           "load_json": lambda *a, **k: (a[1] if len(a) > 1 else {}),
                           "save_json_atomic": lambda *a, **k: None,
                           "save_file": lambda *a, **k: None}),
    ("energy_charts", {"generate_net_heatmap": lambda *a, **k: "",
                       "generate_daily_import_export_charts": lambda *a, **k: ""}),
    ("ha_client", {"HAClient": type("HAClient", (), {})}),
):
    if _name not in _sys.modules:
        _m = _types.ModuleType(_name)
        for _k, _v in _attrs.items():
            setattr(_m, _k, _v)
        _sys.modules[_name] = _m


NEW_BLOCK_COLUMNS = {
    "source", "is_provisional", "needs_pass2_rerun",
    "imp_kwh_api", "needs_review", "carbon_intensity_g",
}


def _utc(s: str) -> str:
    """Naive UTC ISO string, matching engine/_utc_now_iso convention."""
    return s


class _StoreBase(unittest.TestCase):
    """Fresh in-memory store with one config period to satisfy the blocks FK."""

    def setUp(self):
        self.store = BlockStore(":memory:")
        self.store.insert_config_period({
            "meters": {
                "electricity_main": {
                    "meta": {
                        "timezone": "Europe/London",
                        "billing_day": 1,
                        "block_minutes": 30,
                        "currency_symbol": "£",
                        "currency_code": "GBP",
                        "sub_meter": False,
                    },
                    "standing_charge": 0.50,
                    "channels": {"import": {"sensor": "sensor.import"}},
                }
            }
        })
        self.pid = self.store.get_current_config_period_id()

    def tearDown(self):
        self.store.close()

    def _insert_block(self, block_start, meter_id="main", *,
                      imp_kwh=None, imp_kwh_api=None, imp_cost=None,
                      source="ha_sensor", is_provisional=0,
                      needs_pass2_rerun=0, needs_review=0,
                      carbon_intensity_g=None, block_end=None):
        if block_end is None:
            block_end = (datetime.fromisoformat(block_start)
                         + timedelta(minutes=30)).isoformat()
        self.store._conn.execute(
            """INSERT INTO blocks
                 (block_start, block_end, meter_id, config_period_id,
                  imp_kwh, imp_kwh_api, imp_cost, source, is_provisional,
                  needs_pass2_rerun, needs_review, carbon_intensity_g)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
            (block_start, block_end, meter_id, self.pid,
             imp_kwh, imp_kwh_api, imp_cost, source, is_provisional,
             needs_pass2_rerun, needs_review, carbon_intensity_g),
        )
        self.store._conn.commit()


class TestSchemaColumns(_StoreBase):

    def test_new_columns_present_fresh_install(self):
        cols = {r[1] for r in self.store._conn.execute(
            "PRAGMA table_info(blocks)").fetchall()}
        self.assertTrue(NEW_BLOCK_COLUMNS <= cols,
                        f"missing: {NEW_BLOCK_COLUMNS - cols}")

    def test_column_defaults(self):
        self._insert_block("2026-05-01T00:00:00")
        row = self.store.get_block_by_start("2026-05-01T00:00:00", "main")
        self.assertEqual(row["is_provisional"], 0)
        self.assertEqual(row["needs_pass2_rerun"], 0)
        self.assertEqual(row["needs_review"], 0)
        self.assertIsNone(row["imp_kwh_api"])
        self.assertIsNone(row["carbon_intensity_g"])

    def test_kraken_state_table_exists(self):
        names = {r[0] for r in self.store._conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
        self.assertIn("kraken_state", names)

    def test_indexes_present(self):
        idx = {r[0] for r in self.store._conn.execute(
            "SELECT name FROM sqlite_master WHERE type='index'").fetchall()}
        for name in ("idx_blocks_start_meter", "idx_blocks_provisional",
                     "idx_blocks_pass2_rerun", "idx_blocks_needs_review"):
            self.assertIn(name, idx)

    def test_unique_index_is_unique(self):
        info = self.store._conn.execute(
            "PRAGMA index_info(idx_blocks_start_meter)").fetchall()
        self.assertEqual(len(info), 2)  # (block_start, meter_id)

    def test_upgrade_path_adds_columns(self):
        """Simulate a pre-3.0.0 DB: build blocks without the new columns,
        then re-open via _ensure_schema and confirm the ALTERs add them."""
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        conn.executescript("""
            CREATE TABLE config_periods (id INTEGER PRIMARY KEY AUTOINCREMENT,
                effective_from TEXT, effective_to TEXT);
            CREATE TABLE blocks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                block_start TEXT NOT NULL, block_end TEXT NOT NULL,
                meter_id TEXT NOT NULL, config_period_id INTEGER NOT NULL,
                imp_kwh REAL, imp_cost REAL,
                UNIQUE (block_start, meter_id));
            INSERT INTO config_periods (effective_from) VALUES ('2026-01-01T00:00:00');
            INSERT INTO blocks (block_start, block_end, meter_id, config_period_id, imp_kwh)
                VALUES ('2026-05-01T00:00:00','2026-05-01T00:30:00','main',1,1.5);
        """)
        conn.commit()
        store = BlockStore.__new__(BlockStore)
        store._conn = conn
        store._ensure_schema()
        cols = {r[1] for r in conn.execute("PRAGMA table_info(blocks)").fetchall()}
        self.assertTrue(NEW_BLOCK_COLUMNS <= cols,
                        f"upgrade missing: {NEW_BLOCK_COLUMNS - cols}")
        # Existing data preserved
        row = conn.execute("SELECT imp_kwh, is_provisional FROM blocks").fetchone()
        self.assertEqual(row["imp_kwh"], 1.5)
        self.assertEqual(row["is_provisional"], 0)
        conn.close()

    def test_duplicate_detection_noop_on_clean_db(self):
        self._insert_block("2026-05-01T00:00:00")
        with self.assertLogs("block_store", level="WARNING") as cm:
            self.store._ensure_schema()
            # force at least one log record so assertLogs doesn't error
            import logging
            logging.getLogger("block_store").warning("sentinel")
        self.assertFalse(any("deduplicated" in m for m in cm.output))


class TestKrakenState(_StoreBase):

    def test_set_get(self):
        self.assertIsNone(self.store.get_kraken_state("last_poll_utc"))
        self.store.set_kraken_state("last_poll_utc", "2026-05-28T12:00:00")
        self.assertEqual(self.store.get_kraken_state("last_poll_utc"),
                         "2026-05-28T12:00:00")

    def test_overwrite(self):
        self.store.set_kraken_state("k", "a")
        self.store.set_kraken_state("k", "b")
        self.assertEqual(self.store.get_kraken_state("k"), "b")
        n = self.store._conn.execute(
            "SELECT COUNT(*) FROM kraken_state WHERE key='k'").fetchone()[0]
        self.assertEqual(n, 1)


class TestBlockAccessors(_StoreBase):

    def test_get_block_by_start(self):
        self._insert_block("2026-05-01T00:00:00", imp_kwh=2.0)
        row = self.store.get_block_by_start("2026-05-01T00:00:00", "main")
        self.assertEqual(row["imp_kwh"], 2.0)
        self.assertIsNone(self.store.get_block_by_start("2026-05-01T01:00:00", "main"))

    def test_get_block_by_id(self):
        self._insert_block("2026-05-01T00:00:00", imp_kwh=2.0)
        row = self.store.get_block_by_start("2026-05-01T00:00:00", "main")
        self.assertEqual(self.store.get_block_by_id(row["id"])["imp_kwh"], 2.0)
        self.assertIsNone(self.store.get_block_by_id(99999))


class TestUpsertKrakenBlock(_StoreBase):

    def test_unchanged_resettle_does_not_reflag_pass2(self):
        # The poll re-upserts a rolling backfill window every cycle. An already-
        # settled block must NOT be re-flagged for PASS 2 each poll (the
        # perpetual re-run churn). Only a NEW or CHANGED figure re-flags.
        self._insert_block("2026-05-01T00:00:00", imp_kwh=1.000)
        r1 = self.store.upsert_kraken_block(
            "2026-05-01T00:00:00", "main", 1.010, billing_source="api")
        self.assertEqual(r1["needs_pass2_rerun"], 1, "first settle flags rerun")
        self.store.clear_pass2_rerun_flag(r1["block_id"])
        # Re-poll, SAME figure → must not re-flag.
        r2 = self.store.upsert_kraken_block(
            "2026-05-01T00:00:00", "main", 1.010, billing_source="api")
        self.assertEqual(r2["needs_pass2_rerun"], 0,
                         "unchanged re-settle must NOT re-flag pass2")
        # CHANGED figure → re-flag.
        self.store.clear_pass2_rerun_flag(r2["block_id"])
        r3 = self.store.upsert_kraken_block(
            "2026-05-01T00:00:00", "main", 1.500, billing_source="api")
        self.assertEqual(r3["needs_pass2_rerun"], 1,
                         "changed figure must re-flag pass2")

    def test_missing_block(self):
        res = self.store.upsert_kraken_block("2026-05-01T00:00:00", "main", 1.0)
        self.assertEqual(res["status"], "missing_block")

    def test_positive_within_threshold_api(self):
        self._insert_block("2026-05-01T00:00:00", imp_kwh=1.000)
        res = self.store.upsert_kraken_block(
            "2026-05-01T00:00:00", "main", 1.010,  # +1.0%
            billing_source="api", drift_block_percent=2.0)
        self.assertEqual(res["status"], "stored")
        self.assertEqual(res["needs_review"], 0)
        self.assertEqual(res["needs_pass2_rerun"], 1)
        self.assertAlmostEqual(res["drift_pct"], 1.0, places=3)
        row = self.store.get_block_by_start("2026-05-01T00:00:00", "main")
        self.assertEqual(row["imp_kwh_api"], 1.010)
        self.assertEqual(row["imp_kwh"], 1.000)  # CAD figure untouched
        self.assertEqual(row["source"], "kraken_api")

    def test_positive_over_threshold_flags_review(self):
        self._insert_block("2026-05-01T00:00:00", imp_kwh=2.000)
        res = self.store.upsert_kraken_block(
            "2026-05-01T00:00:00", "main", 1.500,  # -25%
            billing_source="api", drift_block_percent=2.0)
        self.assertEqual(res["needs_review"], 1)
        self.assertAlmostEqual(res["drift_pct"], -25.0, places=3)

    def test_zero_record_flags_review(self):
        self._insert_block("2026-05-01T00:00:00", imp_kwh=3.089)
        res = self.store.upsert_kraken_block(
            "2026-05-01T00:00:00", "main", 0.0, billing_source="api")
        self.assertEqual(res["needs_review"], 1)
        row = self.store.get_block_by_start("2026-05-01T00:00:00", "main")
        self.assertEqual(row["imp_kwh_api"], 0.0)

    def test_billing_source_cad_no_rerun(self):
        self._insert_block("2026-05-01T00:00:00", imp_kwh=1.0)
        res = self.store.upsert_kraken_block(
            "2026-05-01T00:00:00", "main", 1.5,
            billing_source="cad", drift_block_percent=2.0)
        self.assertEqual(res["needs_pass2_rerun"], 0)  # cad: no re-run scheduled
        self.assertEqual(res["needs_review"], 1)        # drift still surfaced
        row = self.store.get_block_by_start("2026-05-01T00:00:00", "main")
        self.assertEqual(row["imp_kwh_api"], 1.5)       # still stored

    def test_null_cad_dcc_material_flags_discrepancy(self):
        """api mode: boundary block has imp_kwh NULL (treated as ~zero). DCC
        reports a material figure → genuine discrepancy, flagged for review.
        Drift % is not computable (CAD is zero/None), so drift_pct stays None."""
        self._insert_block("2026-05-01T00:00:00", imp_kwh=None, source="kraken_api")
        res = self.store.upsert_kraken_block(
            "2026-05-01T00:00:00", "main", 1.234, billing_source="api")
        self.assertIsNone(res["drift_pct"])         # no percentage vs zero
        self.assertEqual(res["needs_review"], 1)    # but discrepancy surfaced
        self.assertEqual(res["needs_pass2_rerun"], 1)

    def test_zero_vs_zero_is_agreement(self):
        """Solar profile: CAD zero (daytime import / night export) and DCC zero
        → agreement, NOT review. This is the fix for the 5957 inflation."""
        self._insert_block("2026-05-01T12:00:00", imp_kwh=0.0)
        res = self.store.upsert_kraken_block(
            "2026-05-01T12:00:00", "main", 0.0, billing_source="api")
        self.assertEqual(res["needs_review"], 0)    # zero-vs-zero = agreement
        self.assertIsNone(res["drift_pct"])

    def test_tiny_values_below_epsilon_agree(self):
        """Float dust / sub-watt noise below 5 Wh epsilon is treated as zero."""
        self._insert_block("2026-05-01T12:00:00", imp_kwh=0.0)
        res = self.store.upsert_kraken_block(
            "2026-05-01T12:00:00", "main", 0.002, billing_source="api")  # 2 Wh
        self.assertEqual(res["needs_review"], 0)

    def test_small_pct_large_delta_below_floor_no_flag(self):
        """131% of 16 Wh: big percentage, tiny absolute delta → NOT flagged
        (this is the solar-noise case from real data)."""
        self._insert_block("2026-05-01T12:00:00", imp_kwh=0.016)
        res = self.store.upsert_kraken_block(
            "2026-05-01T12:00:00", "main", 0.037,  # +131%, but only 21 Wh
            billing_source="api", drift_block_percent=2.0, drift_min_kwh=0.05)
        self.assertEqual(res["needs_review"], 0)   # below 50 Wh floor

    def test_large_block_real_drift_flags(self):
        """5% of 6 kWh: both thresholds exceeded → flagged (real divergence)."""
        self._insert_block("2026-05-01T03:00:00", imp_kwh=6.152)
        res = self.store.upsert_kraken_block(
            "2026-05-01T03:00:00", "main", 6.365,  # +3.5%, 213 Wh
            billing_source="api", drift_block_percent=2.0, drift_min_kwh=0.05)
        self.assertEqual(res["needs_review"], 1)
        self.assertAlmostEqual(res["drift_pct"], 3.46, places=1)

    def test_pct_over_but_delta_under_floor(self):
        """3% of 1 kWh = 30 Wh: over % but under 50 Wh floor → no flag."""
        self._insert_block("2026-05-01T03:00:00", imp_kwh=1.0)
        res = self.store.upsert_kraken_block(
            "2026-05-01T03:00:00", "main", 1.03,  # +3%, 30 Wh
            billing_source="api", drift_block_percent=2.0, drift_min_kwh=0.05)
        self.assertEqual(res["needs_review"], 0)

    def test_interpolated_surfaced_in_verdict(self):
        self._insert_block("2026-05-01T03:00:00", imp_kwh=6.0)
        self.store._conn.execute(
            "UPDATE blocks SET interpolated=1 WHERE block_start='2026-05-01T03:00:00'")
        self.store._conn.commit()
        v = self.store.classify_kraken_block(
            "2026-05-01T03:00:00", "main", 6.4, drift_min_kwh=0.05)
        self.assertTrue(v["interpolated"])
        self.assertEqual(v["needs_review"], 1)


class TestPass2RerunQueue(_StoreBase):

    def test_queue_and_clear(self):
        self._insert_block("2026-05-01T00:00:00", needs_pass2_rerun=1)
        self._insert_block("2026-05-01T00:30:00", needs_pass2_rerun=1)
        self._insert_block("2026-05-01T01:00:00", needs_pass2_rerun=0)
        q = self.store.get_blocks_needing_pass2_rerun()
        self.assertEqual(len(q), 2)
        self.store.clear_pass2_rerun_flag(q[0]["id"])
        self.assertEqual(len(self.store.get_blocks_needing_pass2_rerun()), 1)

    def test_limit(self):
        for i in range(3):
            self._insert_block(f"2026-05-01T0{i}:00:00", needs_pass2_rerun=1)
        self.assertEqual(len(self.store.get_blocks_needing_pass2_rerun(limit=2)), 2)


class TestProvisionalTimeout(_StoreBase):

    def test_get_and_finalise(self):
        old = "2026-04-01T00:00:00"
        recent = "2026-05-27T00:00:00"
        self._insert_block(old, is_provisional=1, source="kraken_api")
        self._insert_block(recent, is_provisional=1, source="kraken_mini")
        cutoff = "2026-05-01T00:00:00"
        timed_out = self.store.get_timed_out_provisionals(cutoff)
        self.assertEqual(len(timed_out), 1)
        self.assertEqual(timed_out[0]["block_start"], old)
        n = self.store.finalise_timed_out_provisionals(cutoff)
        self.assertEqual(n, 1)
        self.assertEqual(
            self.store.get_block_by_start(old, "main")["is_provisional"], 0)
        # recent provisional untouched
        self.assertEqual(
            self.store.get_block_by_start(recent, "main")["is_provisional"], 1)

    def test_source_filter_excludes_ha_sensor(self):
        self._insert_block("2026-04-01T00:00:00", is_provisional=1, source="ha_sensor")
        self.assertEqual(
            len(self.store.get_timed_out_provisionals("2026-05-01T00:00:00")), 0)


class TestDriftAlerts(_StoreBase):

    def test_get_and_dismiss(self):
        self._insert_block("2026-05-26T13:00:00", imp_kwh=3.089,
                           imp_kwh_api=0.0, needs_review=1)
        self._insert_block("2026-05-26T14:30:00", imp_kwh=2.847,
                           imp_kwh_api=2.104, needs_review=1)
        self._insert_block("2026-05-26T15:00:00", imp_kwh=1.0,
                           imp_kwh_api=1.0, needs_review=0)
        alerts = self.store.get_drift_alerts()
        self.assertEqual(len(alerts), 2)
        self.assertAlmostEqual(alerts[0]["delta_pct"], -100.0, places=1)
        self.assertAlmostEqual(alerts[1]["delta_pct"], -26.1, places=1)
        n = self.store.dismiss_drift_alerts([alerts[0]["block_id"]])
        self.assertEqual(n, 1)
        self.assertEqual(len(self.store.get_drift_alerts()), 1)
        self.store.dismiss_drift_alerts()  # dismiss all
        self.assertEqual(len(self.store.get_drift_alerts()), 0)

    def test_dismiss_empty_list_noop(self):
        self.assertEqual(self.store.dismiss_drift_alerts([]), 0)


class TestSettledTo(_StoreBase):

    def test_settled_to(self):
        # provisional block — should not count
        self._insert_block("2026-05-28T10:00:00", imp_cost=0.5, is_provisional=1)
        # settled with cost
        self._insert_block("2026-05-28T09:00:00", imp_cost=0.4, is_provisional=0)
        # settled, later, with cost
        self._insert_block("2026-05-28T09:30:00", imp_cost=0.45, is_provisional=0)
        # settled but no cost — should not count
        self._insert_block("2026-05-28T11:00:00", imp_cost=None, is_provisional=0)
        self.assertEqual(self.store.get_settled_to("main"), "2026-05-28T09:30:00")

    def test_settled_to_none_when_all_provisional(self):
        self._insert_block("2026-05-28T09:00:00", imp_cost=0.4, is_provisional=1)
        self.assertIsNone(self.store.get_settled_to("main"))


class TestFinalisedOnly(_StoreBase):
    """finalised_only excludes provisional blocks from Tier 1 aggregations.

    The fixture's main meter_id is 'electricity_main'; the JOIN-based billing
    methods require blocks against it.
    """

    MAIN = "electricity_main"

    def _main_block(self, block_start, **kw):
        kw.setdefault("meter_id", self.MAIN)
        self._insert_block(block_start, **kw)

    def test_compute_period_net_excludes_provisional(self):
        # Same local day; no sub-meters → direct = main import cost.
        self._main_block("2026-05-01T10:00:00", imp_cost=1.0, is_provisional=0,
                         imp_kwh=4.0)
        self._main_block("2026-05-01T10:30:00", imp_cost=2.0, is_provisional=1,
                         imp_kwh=8.0)
        self.store._conn.execute(
            "UPDATE blocks SET standing_charge = 0.5 WHERE meter_id = ?",
            (self.MAIN,))
        self.store._conn.commit()
        s, e = "2026-05-01T00:00:00", "2026-05-02T00:00:00"
        full = self.store.compute_period_net(s, e, "Europe/London",
                                             finalised_only=False)
        fin = self.store.compute_period_net(s, e, "Europe/London",
                                            finalised_only=True)
        self.assertAlmostEqual(full, round(3.0 + 0.5, 2))   # both blocks
        self.assertAlmostEqual(fin, round(1.0 + 0.5, 2))    # finalised only

    def test_billing_totals_excludes_provisional(self):
        self._main_block("2026-05-01T10:00:00", imp_cost=1.0, imp_kwh=4.0,
                         is_provisional=0)
        self._main_block("2026-05-01T10:30:00", imp_cost=2.0, imp_kwh=8.0,
                         is_provisional=1)
        s, e = "2026-05-01T00:00:00", "2026-05-02T00:00:00"
        full = self.store.get_billing_totals_for_utc_range(s, e, "Europe/London",
                                                           finalised_only=False)
        fin = self.store.get_billing_totals_for_utc_range(s, e, "Europe/London",
                                                          finalised_only=True)
        self.assertAlmostEqual(full["imp_cost"], 3.0)
        self.assertAlmostEqual(full["imp_kwh"], 12.0)
        self.assertAlmostEqual(fin["imp_cost"], 1.0)
        self.assertAlmostEqual(fin["imp_kwh"], 4.0)

    def test_selectors_exclude_provisional(self):
        self._main_block("2026-05-01T10:00:00", is_provisional=0)
        self._main_block("2026-05-01T10:30:00", is_provisional=1)
        s, e = "2026-05-01T00:00:00", "2026-05-02T00:00:00"
        self.assertEqual(len(self.store.get_blocks_for_utc_range(s, e)), 2)
        self.assertEqual(
            len(self.store.get_blocks_for_utc_range(s, e, finalised_only=True)), 1)
        self.assertEqual(len(self.store.get_blocks_lightweight(s, e)), 2)
        self.assertEqual(
            len(self.store.get_blocks_lightweight(s, e, finalised_only=True)), 1)
        from datetime import datetime
        st, en = datetime(2026, 5, 1), datetime(2026, 5, 2)
        self.assertEqual(len(self.store.get_blocks_for_range(st, en)), 2)
        self.assertEqual(
            len(self.store.get_blocks_for_range(st, en, finalised_only=True)), 1)

    def test_lightweight_no_range_with_finalised(self):
        """The empty-where + finalised_only branch must produce valid SQL."""
        self._main_block("2026-05-01T10:00:00", is_provisional=0)
        self._main_block("2026-05-01T10:30:00", is_provisional=1)
        self.assertEqual(len(self.store.get_blocks_lightweight()), 2)
        self.assertEqual(
            len(self.store.get_blocks_lightweight(finalised_only=True)), 1)

    def test_noop_when_no_provisional_blocks(self):
        """Regression guard: on cad-style data (no provisional blocks),
        finalised_only=True is identical to finalised_only=False."""
        self._main_block("2026-05-01T10:00:00", imp_cost=1.0, imp_kwh=4.0,
                         is_provisional=0)
        self._main_block("2026-05-01T10:30:00", imp_cost=2.0, imp_kwh=8.0,
                         is_provisional=0)
        self.store._conn.execute(
            "UPDATE blocks SET standing_charge = 0.5 WHERE meter_id = ?",
            (self.MAIN,))
        self.store._conn.commit()
        s, e = "2026-05-01T00:00:00", "2026-05-02T00:00:00"
        self.assertEqual(
            self.store.compute_period_net(s, e, "Europe/London", finalised_only=False),
            self.store.compute_period_net(s, e, "Europe/London", finalised_only=True))
        a = self.store.get_billing_totals_for_utc_range(s, e, "Europe/London", finalised_only=False)
        b = self.store.get_billing_totals_for_utc_range(s, e, "Europe/London", finalised_only=True)
        self.assertEqual(a["imp_cost"], b["imp_cost"])
        self.assertEqual(a["imp_kwh"], b["imp_kwh"])
        self.assertEqual(len(self.store.get_blocks_for_utc_range(s, e)),
                         len(self.store.get_blocks_for_utc_range(s, e, finalised_only=True)))


class TestCarbonIntensityPersistence(_StoreBase):
    """append_block persists carbon_intensity_g (3.0.0), the value PASS 3b
    re-run will reuse instead of re-querying the pruned intensity table."""

    def _block(self, start, *, carbon_g=None, carbon_intensity_g=None):
        end = (datetime.fromisoformat(start) + timedelta(minutes=30)).isoformat()
        return {
            "start": start, "end": end, "interpolated": False,
            "meters": {
                "electricity_main": {
                    "meta": {"sub_meter": False},
                    "standing_charge": 0.5,
                    "carbon_g": carbon_g,
                    "carbon_intensity_g": carbon_intensity_g,
                    "channels": {
                        "import": {"kwh": 2.0, "rate": 0.25, "cost": 0.5,
                                   "read_start": 100.0, "read_end": 102.0},
                    },
                }
            },
            "totals": {"import_kwh": 2.0, "import_cost": 0.5,
                       "export_kwh": 0.0, "export_cost": 0.0},
        }

    def test_persisted_when_present(self):
        self.store.append_block(self._block(
            "2026-05-01T00:00:00", carbon_g=375.0, carbon_intensity_g=187.5))
        row = self.store.get_block_by_start("2026-05-01T00:00:00", "electricity_main")
        self.assertEqual(row["carbon_intensity_g"], 187.5)
        self.assertEqual(row["carbon_g"], 375.0)  # existing column unaffected

    def test_null_when_absent(self):
        # No CI data (e.g. no postcode configured) → column stays NULL.
        self.store.append_block(self._block("2026-05-01T00:30:00"))
        row = self.store.get_block_by_start("2026-05-01T00:30:00", "electricity_main")
        self.assertIsNone(row["carbon_intensity_g"])

    def test_replace_path_persists(self):
        self.store.append_block_replace(self._block(
            "2026-05-01T01:00:00", carbon_g=100.0, carbon_intensity_g=50.0))
        row = self.store.get_block_by_start("2026-05-01T01:00:00", "electricity_main")
        self.assertEqual(row["carbon_intensity_g"], 50.0)


class TestBoundaryCallback(unittest.TestCase):
    """Engine block-boundary callback registration and guarded firing."""

    def setUp(self):
        import engine
        self.engine = engine
        engine.register_block_boundary_callback(None)  # clean slate

    def tearDown(self):
        self.engine.register_block_boundary_callback(None)

    def test_fire_calls_registered_callback(self):
        seen = []
        self.engine.register_block_boundary_callback(lambda t: seen.append(t))
        self.engine._fire_block_boundary("2026-05-01T00:30:00")
        self.assertEqual(seen, ["2026-05-01T00:30:00"])

    def test_fire_noop_when_unregistered(self):
        # Should not raise with no callback registered.
        self.engine._fire_block_boundary("2026-05-01T00:30:00")

    def test_callback_error_swallowed(self):
        def boom(_):
            raise RuntimeError("ingester down")
        self.engine.register_block_boundary_callback(boom)
        # Must not propagate — finalisation can never be broken by the ingester.
        self.engine._fire_block_boundary("2026-05-01T00:30:00")


class TestStateColumnRoundTrip(_StoreBase):
    """append_block / append_block_replace must preserve the 3.0.0 state
    columns so a PASS 2 re-run (INSERT OR REPLACE) doesn't reset imp_kwh_api
    or un-clear is_provisional."""

    def _block(self, start, **main_overrides):
        end = (datetime.fromisoformat(start) + timedelta(minutes=30)).isoformat()
        main = {
            "meta": {"sub_meter": False},
            "standing_charge": 0.5,
            "carbon_g": 100.0,
            "carbon_intensity_g": 50.0,
            "channels": {"import": {"kwh": 2.0, "rate": 0.25, "cost": 0.5,
                                    "read_start": 0.0, "read_end": 2.0}},
        }
        main.update(main_overrides)
        return {"start": start, "end": end, "interpolated": False,
                "meters": {"electricity_main": main},
                "totals": {"import_kwh": 2.0, "import_cost": 0.5,
                           "export_kwh": 0.0, "export_cost": 0.0}}

    def test_fresh_block_defaults(self):
        self.store.append_block(self._block("2026-05-01T00:00:00"))
        row = self.store.get_block_by_start("2026-05-01T00:00:00", "electricity_main")
        self.assertEqual(row["is_provisional"], 0)
        self.assertEqual(row["needs_pass2_rerun"], 0)
        self.assertEqual(row["needs_review"], 0)
        self.assertIsNone(row["imp_kwh_api"])

    def test_provisional_block_persists(self):
        self.store.append_block(self._block(
            "2026-05-01T00:30:00", is_provisional=True, source="kraken_api"))
        row = self.store.get_block_by_start("2026-05-01T00:30:00", "electricity_main")
        self.assertEqual(row["is_provisional"], 1)
        self.assertEqual(row["source"], "kraken_api")

    def test_replace_preserves_imp_kwh_api_and_clears_provisional(self):
        # Initial provisional block with a settled API figure present.
        self.store.append_block(self._block(
            "2026-05-01T01:00:00", is_provisional=True,
            imp_kwh_api=1.85, source="kraken_api"))
        # Simulate the re-run result: provisional cleared, api figure retained.
        b = self._block("2026-05-01T01:00:00", imp_kwh_api=1.85,
                        source="kraken_api")  # is_provisional absent → cleared
        self.store.append_block_replace(b)
        row = self.store.get_block_by_start("2026-05-01T01:00:00", "electricity_main")
        self.assertEqual(row["is_provisional"], 0)        # cleared survives
        self.assertEqual(row["imp_kwh_api"], 1.85)         # audit figure survives


class TestDccRerun(unittest.TestCase):
    """Engine PASS 2+3b DCC re-run (pure dict transform)."""

    def setUp(self):
        import engine
        self.engine = engine

    def _main_only_block(self, kwh=2.0, rate=0.30, api=1.80,
                         intensity=200.0):
        return {
            "start": "2026-05-01T00:00:00", "end": "2026-05-01T00:30:00",
            "interpolated": False,
            "meters": {
                "electricity_main": {
                    "meta": {"sub_meter": False},
                    "standing_charge": 0.5,
                    "carbon_g": round(kwh * intensity, 4),
                    "carbon_intensity_g": intensity,
                    "imp_kwh_api": api,
                    "is_provisional": True,
                    "channels": {"import": {"kwh": kwh, "kwh_total": kwh,
                                            "rate": rate,
                                            "cost": round(kwh * rate, 6),
                                            "read_start": 0.0, "read_end": kwh},
                                 "export": {"kwh": 0.0, "rate": 0.0, "cost": 0.0}},
                }
            },
            "totals": {"import_kwh": kwh, "import_cost": round(kwh * rate, 6),
                       "export_kwh": 0.0, "export_cost": 0.0},
        }

    def test_main_meter_kwh_and_cost_use_settled_figure(self):
        b = self._main_only_block(kwh=2.0, rate=0.30, api=1.80)
        out = self.engine._rerun_pass2_for_settled_block(b)
        imp = out["meters"]["electricity_main"]["channels"]["import"]
        self.assertEqual(imp["kwh"], 1.80)
        self.assertAlmostEqual(imp["cost"], round(1.80 * 0.30, 6))
        self.assertAlmostEqual(out["totals"]["import_kwh"], 1.80)
        self.assertAlmostEqual(out["totals"]["import_cost"], round(1.80 * 0.30, 6))

    def test_carbon_recomputed_from_stored_intensity(self):
        b = self._main_only_block(kwh=2.0, api=1.80, intensity=200.0)
        out = self.engine._rerun_pass2_for_settled_block(b)
        # carbon = (1.80 - 0) * 200.0
        self.assertAlmostEqual(
            out["meters"]["electricity_main"]["carbon_g"], round(1.80 * 200.0, 4))

    def test_provisional_cleared(self):
        b = self._main_only_block()
        out = self.engine._rerun_pass2_for_settled_block(b)
        self.assertNotIn("is_provisional", out["meters"]["electricity_main"])
        self.assertNotIn("provisional", out["meters"]["electricity_main"])

    def test_imp_kwh_api_retained_for_audit(self):
        b = self._main_only_block(api=1.80)
        out = self.engine._rerun_pass2_for_settled_block(b)
        self.assertEqual(out["meters"]["electricity_main"]["imp_kwh_api"], 1.80)

    def test_no_settled_figure_is_noop_on_kwh(self):
        b = self._main_only_block(kwh=2.0)
        b["meters"]["electricity_main"].pop("imp_kwh_api")
        out = self.engine._rerun_pass2_for_settled_block(b)
        self.assertEqual(
            out["meters"]["electricity_main"]["channels"]["import"]["kwh"], 2.0)

    def test_recompute_skips_when_no_intensity(self):
        b = self._main_only_block()
        b["meters"]["electricity_main"].pop("carbon_intensity_g")
        original = b["meters"]["electricity_main"]["carbon_g"]
        out = self.engine._rerun_pass2_for_settled_block(b)
        # carbon_g unchanged (cannot recompute without stored intensity)
        self.assertEqual(out["meters"]["electricity_main"]["carbon_g"], original)


class TestDrainPass2Queue(_StoreBase):
    """Full _drain_pass2_queue against a real in-memory store."""

    MAIN = "electricity_main"

    def setUp(self):
        super().setUp()
        import engine
        self.engine = engine
        # Point the engine module's _store at our in-memory store, and stub
        # cumulative totals / sensor update so the drain runs without HA.
        self._orig_store = getattr(engine, "_store", None)
        engine._store = self.store
        self.store.get_cumulative_totals = lambda: {}
        self._ha = type("HA", (), {})()

    def tearDown(self):
        self.engine._store = self._orig_store
        super().tearDown()

    def _settled_block(self, start, kwh=2.0, rate=0.30, api=1.80, intensity=200.0):
        end = (datetime.fromisoformat(start) + timedelta(minutes=30)).isoformat()
        self.store._conn.execute(
            """INSERT INTO blocks
                 (block_start, block_end, meter_id, config_period_id,
                  imp_kwh, imp_rate, imp_cost, imp_read_start, imp_read_end,
                  exp_kwh, standing_charge, carbon_g, carbon_intensity_g,
                  imp_kwh_api, is_provisional, needs_pass2_rerun, source)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (start, end, self.MAIN, self.pid,
             kwh, rate, round(kwh * rate, 6), 0.0, kwh,
             0.0, 0.5, round(kwh * intensity, 4), intensity,
             api, 1, 1, "kraken_api"))
        self.store._conn.commit()

    def test_drain_reruns_and_clears(self):
        self._settled_block("2026-05-01T00:00:00", kwh=2.0, rate=0.30, api=1.80)
        n = self.engine._drain_pass2_queue(self._ha)
        self.assertEqual(n, 1)
        row = self.store.get_block_by_start("2026-05-01T00:00:00", self.MAIN)
        self.assertEqual(row["needs_pass2_rerun"], 0)   # flag cleared
        self.assertEqual(row["is_provisional"], 0)       # block finalised
        self.assertAlmostEqual(row["imp_kwh"], 1.80)     # billing kwh = DCC
        self.assertAlmostEqual(row["imp_cost"], round(1.80 * 0.30, 6))
        self.assertEqual(row["imp_kwh_api"], 1.80)       # audit retained
        # Idempotent: nothing left to drain.
        self.assertEqual(self.engine._drain_pass2_queue(self._ha), 0)

    def test_drain_empty_queue(self):
        self.assertEqual(self.engine._drain_pass2_queue(self._ha), 0)

    def test_billing_reflects_dcc_after_drain(self):
        self._settled_block("2026-05-01T10:00:00", kwh=4.0, rate=0.25, api=3.0)
        # Before drain: billing sees the provisional CAD figure (4.0).
        s, e = "2026-05-01T00:00:00", "2026-05-02T00:00:00"
        before = self.store.get_billing_totals_for_utc_range(s, e, "Europe/London")
        self.assertAlmostEqual(before["imp_kwh"], 4.0)
        self.engine._drain_pass2_queue(self._ha)
        after = self.store.get_billing_totals_for_utc_range(s, e, "Europe/London")
        self.assertAlmostEqual(after["imp_kwh"], 3.0)   # now DCC-settled
        self.assertAlmostEqual(after["imp_cost"], round(3.0 * 0.25, 6))


class TestExportSettlement(_StoreBase):
    """exp_kwh_api column + export channel of upsert_kraken_block."""

    MAIN = "electricity_main"

    def _block(self, block_start, *, imp_kwh=None, exp_kwh=None):
        end = (datetime.fromisoformat(block_start) + timedelta(minutes=30)).isoformat()
        self.store._conn.execute(
            """INSERT INTO blocks (block_start, block_end, meter_id,
                 config_period_id, imp_kwh, exp_kwh)
               VALUES (?,?,?,?,?,?)""",
            (block_start, end, self.MAIN, self.pid, imp_kwh, exp_kwh))
        self.store._conn.commit()

    def test_exp_kwh_api_column_exists(self):
        cols = {r[1] for r in self.store._conn.execute(
            "PRAGMA table_info(blocks)").fetchall()}
        self.assertIn("exp_kwh_api", cols)

    def test_export_upsert_writes_exp_column(self):
        self._block("2026-05-01T12:00:00", exp_kwh=1.0)
        res = self.store.upsert_kraken_block(
            "2026-05-01T12:00:00", self.MAIN, 1.2, channel="export",
            billing_source="api", drift_block_percent=2.0)
        self.assertEqual(res["status"], "stored")
        self.assertEqual(res["channel"], "export")
        row = self.store.get_block_by_start("2026-05-01T12:00:00", self.MAIN)
        self.assertEqual(row["exp_kwh_api"], 1.2)
        self.assertIsNone(row["imp_kwh_api"])      # import untouched
        self.assertEqual(row["needs_pass2_rerun"], 1)

    def test_export_drift_vs_exp_kwh(self):
        self._block("2026-05-01T12:00:00", exp_kwh=2.0)
        res = self.store.upsert_kraken_block(
            "2026-05-01T12:00:00", self.MAIN, 1.0, channel="export",  # -50%
            drift_block_percent=2.0)
        self.assertEqual(res["needs_review"], 1)
        self.assertAlmostEqual(res["drift_pct"], -50.0, places=3)

    def test_both_channels_coexist_flags_or(self):
        # Import settlement first, then export; neither clobbers the other.
        self._block("2026-05-01T12:00:00", imp_kwh=3.0, exp_kwh=1.0)
        self.store.upsert_kraken_block(
            "2026-05-01T12:00:00", self.MAIN, 2.99, channel="import",
            billing_source="api")  # within threshold, no review
        self.store.upsert_kraken_block(
            "2026-05-01T12:00:00", self.MAIN, 0.0, channel="export",
            billing_source="api")  # zero export → review
        row = self.store.get_block_by_start("2026-05-01T12:00:00", self.MAIN)
        self.assertAlmostEqual(row["imp_kwh_api"], 2.99)
        self.assertEqual(row["exp_kwh_api"], 0.0)
        self.assertEqual(row["needs_review"], 1)        # from export zero
        self.assertEqual(row["needs_pass2_rerun"], 1)

    def test_invalid_channel_raises(self):
        self._block("2026-05-01T12:00:00")
        with self.assertRaises(ValueError):
            self.store.upsert_kraken_block(
                "2026-05-01T12:00:00", self.MAIN, 1.0, channel="gas")

    def test_classify_is_read_only(self):
        self._block("2026-05-01T12:00:00", imp_kwh=2.0)
        v = self.store.classify_kraken_block(
            "2026-05-01T12:00:00", self.MAIN, 1.5,  # -25%
            billing_source="api", drift_block_percent=2.0)
        self.assertEqual(v["status"], "stored")     # WOULD store
        self.assertEqual(v["needs_review"], 1)
        self.assertEqual(v["needs_pass2_rerun"], 1)
        self.assertAlmostEqual(v["drift_pct"], -25.0, places=3)
        # nothing written
        row = self.store.get_block_by_start("2026-05-01T12:00:00", self.MAIN)
        self.assertIsNone(row["imp_kwh_api"])
        self.assertEqual(row["needs_review"], 0)
        self.assertEqual(row["needs_pass2_rerun"], 0)

    def test_classify_missing_block(self):
        v = self.store.classify_kraken_block(
            "2099-01-01T00:00:00", self.MAIN, 1.0)
        self.assertEqual(v["status"], "missing_block")


class TestOldestBlockStart(_StoreBase):
    def test_none_when_empty(self):
        self.assertIsNone(self.store.get_oldest_block_start())

    def test_returns_min(self):
        for bs in ("2026-05-03T00:00:00", "2026-05-01T00:00:00",
                   "2026-05-02T00:00:00"):
            end = (datetime.fromisoformat(bs) + timedelta(minutes=30)).isoformat()
            self.store._conn.execute(
                """INSERT INTO blocks (block_start, block_end, meter_id,
                     config_period_id) VALUES (?,?,?,?)""",
                (bs, end, "electricity_main", self.pid))
        self.store._conn.commit()
        self.assertEqual(self.store.get_oldest_block_start(),
                         "2026-05-01T00:00:00")


class TestExportReRun(unittest.TestCase):
    """Engine re-run applies exp_kwh_api to the export channel."""

    def setUp(self):
        import engine
        self.engine = engine

    def test_export_settled_overwrites_and_costs(self):
        block = {
            "start": "2026-05-01T12:00:00", "end": "2026-05-01T12:30:00",
            "meters": {"electricity_main": {
                "meta": {"sub_meter": False},
                "carbon_intensity_g": 100.0,
                "imp_kwh_api": 2.0,
                "exp_kwh_api": 1.5,
                "channels": {
                    "import": {"kwh": 2.5, "kwh_total": 2.5, "rate": 0.30,
                               "cost": 0.75},
                    "export": {"kwh": 1.0, "rate": 0.12, "cost": 0.12},
                }}},
            "totals": {},
        }
        out = self.engine._rerun_pass2_for_settled_block(block)
        exp = out["meters"]["electricity_main"]["channels"]["export"]
        self.assertEqual(exp["kwh"], 1.5)                       # settled
        self.assertAlmostEqual(exp["cost"], round(1.5 * 0.12, 6))
        self.assertAlmostEqual(out["totals"]["export_kwh"], 1.5)
        # carbon = (imp_total 2.0 - exp 1.5) * 100
        self.assertAlmostEqual(
            out["meters"]["electricity_main"]["carbon_g"], round(0.5 * 100.0, 4))

    def test_rate_repair_fills_zero_rate(self):
        # Block has settled import kWh but rate 0.0 (gap-fill hole).
        block = {
            "start": "2026-05-01T03:00:00", "end": "2026-05-01T03:30:00",
            "meters": {"electricity_main": {
                "meta": {"sub_meter": False},
                "carbon_intensity_g": 100.0,
                "imp_kwh_api": 3.0,
                "channels": {"import": {"kwh": 3.0, "kwh_total": 3.0,
                                        "rate": 0.0, "cost": 0.0}}}},
            "totals": {},
        }
        # Resolver returns £/kWh for the import channel.
        def resolver(channel, ts):
            return 0.2450 if channel == "import" else None
        out = self.engine._rerun_pass2_for_settled_block(block, rate_resolver=resolver)
        imp = out["meters"]["electricity_main"]["channels"]["import"]
        self.assertEqual(imp["rate"], 0.2450)            # repaired
        self.assertAlmostEqual(imp["cost"], round(3.0 * 0.2450, 6))
        self.assertTrue(out["meters"]["electricity_main"].get("rate_repaired"))

    def test_rate_repair_skips_when_rate_present(self):
        block = {
            "start": "2026-05-01T03:00:00", "end": "2026-05-01T03:30:00",
            "meters": {"electricity_main": {
                "meta": {"sub_meter": False}, "carbon_intensity_g": 100.0,
                "imp_kwh_api": 3.0,
                "channels": {"import": {"kwh": 3.0, "kwh_total": 3.0,
                                        "rate": 0.30, "cost": 0.9}}}},
            "totals": {},
        }
        called = {"n": 0}
        def resolver(channel, ts):
            called["n"] += 1
            return 0.99
        out = self.engine._rerun_pass2_for_settled_block(block, rate_resolver=resolver)
        imp = out["meters"]["electricity_main"]["channels"]["import"]
        self.assertEqual(imp["rate"], 0.30)              # kept stored rate
        self.assertEqual(called["n"], 0)                 # resolver not consulted
        self.assertFalse(out["meters"]["electricity_main"].get("rate_repaired"))

    def test_rate_repair_rejects_implausible_units(self):
        # Resolver mistakenly returns pence (24.5) not £ — must be rejected.
        block = {
            "start": "2026-05-01T03:00:00", "end": "2026-05-01T03:30:00",
            "meters": {"electricity_main": {
                "meta": {"sub_meter": False}, "carbon_intensity_g": 100.0,
                "imp_kwh_api": 3.0,
                "channels": {"import": {"kwh": 3.0, "kwh_total": 3.0,
                                        "rate": 0.0, "cost": 0.0}}}},
            "totals": {},
        }
        def resolver(channel, ts):
            return 24.5   # implausible as £/kWh
        out = self.engine._rerun_pass2_for_settled_block(block, rate_resolver=resolver)
        imp = out["meters"]["electricity_main"]["channels"]["import"]
        self.assertEqual(imp["rate"], 0.0)               # rejected, not applied
        self.assertFalse(out["meters"]["electricity_main"].get("rate_repaired"))


class TestKrakenRateResolver(unittest.TestCase):
    """The sync resolver the drain passes in: pence→£ conversion, cache read."""

    def setUp(self):
        import engine
        from kraken_rates import RateSchedule
        self.engine = engine
        self._saved = engine._kraken_rate_schedules
        engine._kraken_rate_schedules = {
            "import": RateSchedule.from_api_records([
                {"value_inc_vat": 24.5, "valid_from": "2026-02-01T00:00:00Z",
                 "valid_to": None}])}

    def tearDown(self):
        self.engine._kraken_rate_schedules = self._saved

    def test_resolves_pence_to_pounds(self):
        r = self.engine._kraken_rate_resolver("import", "2026-05-01T00:00:00")
        self.assertAlmostEqual(r, 0.245, places=6)   # 24.5p → £0.245

    def test_none_when_no_schedule(self):
        self.assertIsNone(
            self.engine._kraken_rate_resolver("export", "2026-05-01T00:00:00"))

    def test_none_when_uncovered(self):
        self.assertIsNone(
            self.engine._kraken_rate_resolver("import", "2025-01-01T00:00:00"))


class TestKrakenEnv(unittest.TestCase):
    def setUp(self):
        import engine
        self.engine = engine
        self._saved = {k: os.environ.get(k) for k in
                       ("KRAKEN_API_KEY", "KRAKEN_ACCOUNT_NUMBER", "KRAKEN_BASE_URL")}

    def tearDown(self):
        for k, v in self._saved.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v

    def test_null_and_empty_become_none(self):
        os.environ["KRAKEN_API_KEY"] = "null"
        os.environ["KRAKEN_ACCOUNT_NUMBER"] = ""
        os.environ.pop("KRAKEN_BASE_URL", None)
        env = self.engine._kraken_env()
        self.assertIsNone(env["api_key"])
        self.assertIsNone(env["account_number"])
        self.assertIsNone(env["base_url"])

    def test_real_values_pass_through(self):
        os.environ["KRAKEN_API_KEY"] = "sk_test_123"
        os.environ["KRAKEN_ACCOUNT_NUMBER"] = "A-ABCD1234"
        env = self.engine._kraken_env()
        self.assertEqual(env["api_key"], "sk_test_123")
        self.assertEqual(env["account_number"], "A-ABCD1234")


class TestKrakenStartupDiscovery(unittest.TestCase):
    """4b-i gate: discovery logs and stores result but never polls/writes."""

    def setUp(self):
        import engine
        from block_store import BlockStore
        self.engine = engine
        self._saved = {k: os.environ.get(k) for k in
                       ("KRAKEN_API_KEY", "KRAKEN_ACCOUNT_NUMBER", "KRAKEN_BASE_URL")}
        self._saved_store = engine._store
        engine._store = BlockStore(":memory:")
        engine._kraken_client = None
        engine._kraken_discovery = None

    def tearDown(self):
        for k, v in self._saved.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v
        try:
            self.engine._store.close()
        except Exception:
            pass
        self.engine._store = self._saved_store
        self.engine._kraken_client = None
        self.engine._kraken_discovery = None

    def _run(self, coro):
        loop = asyncio.new_event_loop()
        try:
            return loop.run_until_complete(coro)
        finally:
            loop.close()

    def test_no_key_is_noop(self):
        os.environ.pop("KRAKEN_API_KEY", None)
        self._run(self.engine._kraken_startup_discovery())
        self.assertIsNone(self.engine._kraken_discovery)
        self.assertIsNone(self.engine._kraken_client)

    def test_unset_mode_does_not_activate_api(self):
        # Orphaned creds: key present but no mode chosen (flat DB) → discovery
        # must NOT run. This is the flatten-then-restart bug.
        os.environ["KRAKEN_API_KEY"] = "sk_test"
        os.environ["KRAKEN_ACCOUNT_NUMBER"] = "A-ABCD1234"
        # mode left unset (fresh in-memory store)
        self.assertEqual(self.engine.get_data_source_mode(), "unset")
        self._run(self.engine._kraken_startup_discovery())
        self.assertIsNone(self.engine._kraken_discovery)
        self.assertIsNone(self.engine._kraken_client)

    def test_force_bypasses_mode_gate(self):
        # The wizard's own connect uses force=True even pre-survey (mode unset).
        os.environ["KRAKEN_API_KEY"] = "sk_test"
        os.environ["KRAKEN_ACCOUNT_NUMBER"] = "A-ABCD1234"
        import kraken_api_client

        class FakeClient:
            def __init__(self, *a, **k): pass
            async def test_connection(self, *a, **k):
                return {"ok": True, "account_number": "A-ABCD1234"}
            async def auto_discover(self, *a, **k):
                return {"account_number": "A-ABCD1234", "properties": 1,
                        "import": {"mpan": "1900000000001", "serial": "21L9",
                                   "tariff_code": "T", "product_code": "P"},
                        "export": None, "warnings": []}
            async def get_consumption(self, *a, **k):
                return []

        orig = kraken_api_client.KrakenAPIClient
        kraken_api_client.KrakenAPIClient = FakeClient
        try:
            self._run(self.engine._kraken_startup_discovery(force=True))
        finally:
            kraken_api_client.KrakenAPIClient = orig
        self.assertIsNotNone(self.engine._kraken_discovery)

    def test_discovery_stores_result_without_polling(self):
        os.environ["KRAKEN_API_KEY"] = "sk_test"
        os.environ["KRAKEN_ACCOUNT_NUMBER"] = "A-ABCD1234"
        # API auto-activation now requires an explicitly-set API mode.
        self.engine.set_data_source_mode("api")

        # Patch the client class the function imports.
        import kraken_api_client
        calls = {"consumption": 0}

        class FakeClient:
            def __init__(self, *a, **k):
                pass
            async def test_connection(self, *a, **k):
                return {"ok": True, "account_number": "A-ABCD1234"}
            async def auto_discover(self, *a, **k):
                return {"account_number": "A-ABCD1234", "properties": 1,
                        "import": {"mpan": "1900000000001", "serial": "21L9",
                                   "tariff_code": "E-1R-AGILE-FLEX-22-11-25-A",
                                   "product_code": "AGILE-FLEX-22-11-25"},
                        "export": None, "warnings": []}
            async def get_consumption(self, *a, **k):
                calls["consumption"] += 1
                return []

        orig = kraken_api_client.KrakenAPIClient
        kraken_api_client.KrakenAPIClient = FakeClient
        try:
            self._run(self.engine._kraken_startup_discovery())
        finally:
            kraken_api_client.KrakenAPIClient = orig

        self.assertIsNotNone(self.engine._kraken_discovery)
        self.assertEqual(self.engine._kraken_discovery["import"]["mpan"],
                         "1900000000001")
        self.assertEqual(calls["consumption"], 0)   # NEVER polled

    def test_connection_failure_no_discovery(self):
        os.environ["KRAKEN_API_KEY"] = "sk_bad"
        import kraken_api_client

        class FailClient:
            def __init__(self, *a, **k): pass
            async def test_connection(self, *a, **k):
                return {"ok": False, "detail": "auth failed"}
            async def auto_discover(self, *a, **k):
                raise AssertionError("must not discover after failed connection")

        orig = kraken_api_client.KrakenAPIClient
        kraken_api_client.KrakenAPIClient = FailClient
        try:
            self._run(self.engine._kraken_startup_discovery())
        finally:
            kraken_api_client.KrakenAPIClient = orig
        self.assertIsNone(self.engine._kraken_discovery)


class TestKrakenPollTask(_StoreBase):
    """4b-ii: ingester builder gating, backfill-days, and idle no-op."""

    def setUp(self):
        super().setUp()
        import engine
        self.engine = engine
        self._saved_store = engine._store
        self._saved_client = engine._kraken_client
        self._saved_disc = engine._kraken_discovery
        engine._store = self.store
        engine._kraken_client = None
        engine._kraken_discovery = None
        engine._kraken_ingester = None

    def tearDown(self):
        self.engine._store = self._saved_store
        self.engine._kraken_client = self._saved_client
        self.engine._kraken_discovery = self._saved_disc
        self.engine._kraken_ingester = None
        super().tearDown()

    def _run(self, coro):
        loop = asyncio.new_event_loop()
        try:
            return loop.run_until_complete(coro)
        finally:
            loop.close()

    def _seed_blocks(self):
        for bs in ("2026-05-01T00:00:00", "2026-05-20T00:00:00"):
            end = (datetime.fromisoformat(bs) + timedelta(minutes=30)).isoformat()
            self.store._conn.execute(
                """INSERT INTO blocks (block_start, block_end, meter_id,
                     config_period_id) VALUES (?,?,?,?)""",
                (bs, end, "electricity_main", self.pid))
        self.store._conn.commit()

    def test_backfill_days_from_oldest_block(self):
        self._seed_blocks()
        days = self.engine._kraken_backfill_days()
        # oldest block 2026-05-01; today is well after — capped sanity
        self.assertGreaterEqual(days, 1)
        self.assertLessEqual(days, self.engine._KRAKEN_BACKFILL_CAP_DAYS)

    def test_no_backfill_when_no_blocks(self):
        # Fresh DB (no blocks) must NOT backfill — nothing to reconcile. A
        # deliberate historical-import action can be added later.
        self.assertEqual(self.engine._kraken_backfill_days(), 0)

    def test_builder_none_without_discovery(self):
        self.assertIsNone(self.engine._build_kraken_ingester())

    def test_builder_constructs_with_discovery(self):
        self.engine._kraken_client = object()  # presence is enough
        self.engine._kraken_discovery = {
            "import": {"mpan": "M", "serial": "S"},
            "export": {"mpan": "EM", "serial": "ES"}}
        ing = self.engine._build_kraken_ingester()
        self.assertIsNotNone(ing)
        self.assertEqual(ing.import_mpan, "M")
        self.assertEqual(ing.export_mpan, "EM")
        self.assertEqual(ing.billing_source, "api")

    def test_builder_none_without_import_meter(self):
        self.engine._kraken_client = object()
        self.engine._kraken_discovery = {"import": None, "export": None}
        self.assertIsNone(self.engine._build_kraken_ingester())

    def test_poll_task_idle_when_unconfigured(self):
        # No client/discovery → returns immediately, no exception.
        self.engine._kraken_client = None
        self.engine._kraken_discovery = None
        self._run(self.engine.kraken_poll_task(None))

    def test_live_mode_is_active(self):
        # 4b-iii: dry-run has been deliberately flipped off.
        self.assertFalse(self.engine._KRAKEN_DRY_RUN)

    def test_pre_live_snapshot_idempotent_and_gated(self):
        # No blocks → snapshot is a safe no-op that marks done and allows writes.
        ok = self.engine._kraken_pre_live_snapshot()
        self.assertTrue(ok)
        marker = self.store.get_kraken_state(self.engine._KRAKEN_SNAPSHOT_DONE_KEY)
        self.assertIsNotNone(marker)
        # Second call sees the marker and returns True without redoing work.
        ok2 = self.engine._kraken_pre_live_snapshot()
        self.assertTrue(ok2)


class TestBillingSourceToggle(unittest.TestCase):
    """Global cad/dcc toggle: re-run honours source both ways."""

    def setUp(self):
        import engine
        self.engine = engine

    def _block(self, cad_imp, dcc_imp):
        return {
            "start": "2026-05-01T03:00:00", "end": "2026-05-01T03:30:00",
            "meters": {"electricity_main": {
                "meta": {"sub_meter": False}, "carbon_intensity_g": 100.0,
                "imp_kwh_api": dcc_imp,
                "channels": {"import": {"kwh": cad_imp, "kwh_total": cad_imp,
                                        "rate": 0.30, "cost": round(cad_imp*0.30, 6)}}}},
            "totals": {},
        }

    def test_dcc_mode_uses_settlement(self):
        b = self._block(cad_imp=3.0, dcc_imp=3.2)
        out = self.engine._rerun_pass2_for_settled_block(b, billing_source="dcc")
        imp = out["meters"]["electricity_main"]["channels"]["import"]
        self.assertEqual(imp["kwh"], 3.2)
        self.assertEqual(imp["kwh_cad"], 3.0)
        self.assertAlmostEqual(imp["cost"], round(3.2*0.30, 6))

    def test_cad_mode_uses_cad_despite_dcc_present(self):
        b = self._block(cad_imp=3.0, dcc_imp=3.2)
        out = self.engine._rerun_pass2_for_settled_block(b, billing_source="cad")
        imp = out["meters"]["electricity_main"]["channels"]["import"]
        self.assertEqual(imp["kwh"], 3.0)
        self.assertAlmostEqual(imp["cost"], round(3.0*0.30, 6))

    def test_dcc_falls_back_to_cad_when_absent(self):
        b = self._block(cad_imp=3.0, dcc_imp=None)
        out = self.engine._rerun_pass2_for_settled_block(b, billing_source="dcc")
        imp = out["meters"]["electricity_main"]["channels"]["import"]
        self.assertEqual(imp["kwh"], 3.0)

    def test_switch_back_restores_cad(self):
        b = self._block(cad_imp=3.0, dcc_imp=3.2)
        b = self.engine._rerun_pass2_for_settled_block(b, billing_source="dcc")
        self.assertEqual(b["meters"]["electricity_main"]["channels"]["import"]["kwh"], 3.2)
        b = self.engine._rerun_pass2_for_settled_block(b, billing_source="cad")
        self.assertEqual(b["meters"]["electricity_main"]["channels"]["import"]["kwh"], 3.0)

    def test_standing_charge_repaired_when_zero(self):
        b = self._block(cad_imp=3.0, dcc_imp=3.2)
        b["meters"]["electricity_main"]["standing_charge"] = 0.0
        out = self.engine._rerun_pass2_for_settled_block(
            b, standing_resolver=lambda ts: 0.4785)
        self.assertAlmostEqual(out["meters"]["electricity_main"]["standing_charge"], 0.4785)
        self.assertTrue(out["meters"]["electricity_main"].get("standing_charge_repaired"))

    def test_standing_charge_reverified_when_nonzero(self):
        # Kraken wins: a non-zero SC differing from Kraken is corrected.
        b = self._block(cad_imp=3.0, dcc_imp=3.2)
        b["meters"]["electricity_main"]["standing_charge"] = 0.45
        out = self.engine._rerun_pass2_for_settled_block(
            b, standing_resolver=lambda ts: 0.4785)
        self.assertAlmostEqual(out["meters"]["electricity_main"]["standing_charge"], 0.4785)

    def test_standing_charge_implausible_rejected(self):
        # Resolver returns pence (47.85) not £ — rejected, stored value kept.
        b = self._block(cad_imp=3.0, dcc_imp=3.2)
        b["meters"]["electricity_main"]["standing_charge"] = 0.45
        out = self.engine._rerun_pass2_for_settled_block(
            b, standing_resolver=lambda ts: 47.85)
        self.assertEqual(out["meters"]["electricity_main"]["standing_charge"], 0.45)

    def test_standing_charge_no_churn_when_equal(self):
        b = self._block(cad_imp=3.0, dcc_imp=3.2)
        b["meters"]["electricity_main"]["standing_charge"] = 0.4785
        out = self.engine._rerun_pass2_for_settled_block(
            b, standing_resolver=lambda ts: 0.4785)
        # Equal (within epsilon) → not marked repaired.
        self.assertFalse(out["meters"]["electricity_main"].get("standing_charge_repaired"))


class TestBillingSourceStoreOps(_StoreBase):
    def _block(self, bs, imp_kwh=2.0, imp_kwh_api=None):
        end = (datetime.fromisoformat(bs) + timedelta(minutes=30)).isoformat()
        self.store._conn.execute(
            """INSERT INTO blocks (block_start, block_end, meter_id,
                 config_period_id, imp_kwh, imp_kwh_api)
               VALUES (?,?,?,?,?,?)""",
            (bs, end, "electricity_main", self.pid, imp_kwh, imp_kwh_api))
        self.store._conn.commit()

    def test_flag_all_for_pass2_rerun(self):
        self._block("2026-05-01T00:00:00")
        self._block("2026-05-01T00:30:00")
        n = self.store.flag_all_for_pass2_rerun("electricity_main")
        self.assertEqual(n, 2)
        self.assertEqual(len(self.store.get_blocks_needing_pass2_rerun()), 2)

    def test_unsettled_blocks_query(self):
        self._block("2026-05-01T00:00:00", imp_kwh_api=1.5)
        self._block("2026-05-01T00:30:00", imp_kwh_api=None)
        self._block("2026-05-01T01:00:00", imp_kwh_api=None)
        self.assertEqual(self.store.count_unsettled_blocks("electricity_main"), 2)
        rows = self.store.get_unsettled_blocks("electricity_main")
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["block_start"], "2026-05-01T01:00:00")


class TestDataSourceMode(_StoreBase):
    def setUp(self):
        super().setUp()
        import engine
        self.engine = engine
        self._saved = engine._store
        engine._store = self.store

    def tearDown(self):
        self.engine._store = self._saved
        super().tearDown()

    def test_default_is_unset(self):
        # No mode stored → 'unset' (NOT 'cad') so a flat DB is distinguishable
        # from a deliberate cad choice and the API won't auto-activate.
        self.assertEqual(self.engine.get_data_source_mode(), "unset")
        self.assertFalse(self.engine.is_mode_configured())
        self.assertFalse(self.engine.mode_uses_api())
        self.assertFalse(self.engine.mode_uses_mini())

    def test_set_and_get(self):
        self.engine.set_data_source_mode("api+mini")
        self.assertEqual(self.engine.get_data_source_mode(), "api+mini")

    def test_invalid_raises(self):
        with self.assertRaises(ValueError):
            self.engine.set_data_source_mode("teleport")

    def test_uses_api_flags(self):
        for m in ("cad+api", "api", "api+mini"):
            self.engine.set_data_source_mode(m)
            self.assertTrue(self.engine.mode_uses_api())
        self.engine.set_data_source_mode("cad")
        self.assertFalse(self.engine.mode_uses_api())

    def test_uses_mini_only_api_mini(self):
        self.engine.set_data_source_mode("api+mini")
        self.assertTrue(self.engine.mode_uses_mini())
        self.engine.set_data_source_mode("api")
        self.assertFalse(self.engine.mode_uses_mini())


class TestApplyBillingSourceChange(_StoreBase):
    def setUp(self):
        super().setUp()
        import engine
        self.engine = engine
        self._saved_store = engine._store
        engine._store = self.store

    def tearDown(self):
        self.engine._store = self._saved_store
        super().tearDown()

    def _block(self, bs):
        end = (datetime.fromisoformat(bs) + timedelta(minutes=30)).isoformat()
        self.store._conn.execute(
            """INSERT INTO blocks (block_start, block_end, meter_id,
                 config_period_id, imp_kwh) VALUES (?,?,?,?,?)""",
            (bs, end, "electricity_main", self.pid, 2.0))
        self.store._conn.commit()

    def test_default_is_dcc(self):
        self.assertEqual(self.engine._get_billing_source(), "dcc")

    def test_change_to_cad_flags_blocks(self):
        self._block("2026-05-01T00:00:00")
        self._block("2026-05-01T00:30:00")
        res = self.engine.apply_billing_source_change("cad")
        self.assertTrue(res["changed"])
        self.assertEqual(res["flagged"], 2)
        self.assertEqual(self.engine._get_billing_source(), "cad")

    def test_same_source_noop(self):
        self._block("2026-05-01T00:00:00")
        res = self.engine.apply_billing_source_change("dcc")
        self.assertFalse(res["changed"])
        self.assertEqual(res["flagged"], 0)

    def test_invalid_source_raises(self):
        with self.assertRaises(ValueError):
            self.engine.apply_billing_source_change("smart_meter")


class TestDispatchSlots(_StoreBase):

    def test_upsert_get_roundtrip(self):
        self.store.upsert_dispatch_slot(
            "2026-06-06T15:00:00", off_peak=True, provider="MYENERGI_V2",
            source="smart-charge")
        row = self.store.get_dispatch_slot("2026-06-06T15:00:00")
        self.assertEqual(row["off_peak"], 1)
        self.assertEqual(row["provider"], "MYENERGI_V2")
        self.assertEqual(row["source"], "smart-charge")
        self.assertIsNone(self.store.get_dispatch_slot("2026-06-06T16:00:00"))

    def test_upsert_last_write_wins(self):
        self.store.upsert_dispatch_slot("2026-06-06T15:00:00", source="smart-charge")
        self.store.upsert_dispatch_slot("2026-06-06T15:00:00", source="bump-charge")
        self.assertEqual(
            self.store.get_dispatch_slot("2026-06-06T15:00:00")["source"],
            "bump-charge")

    def test_range_query(self):
        for s in ("2026-06-06T14:00:00", "2026-06-06T15:00:00",
                  "2026-06-07T00:00:00"):
            self.store.upsert_dispatch_slot(s, source="smart-charge")
        got = self.store.get_dispatch_slots_in_range(
            "2026-06-06T00:00:00", "2026-06-07T00:00:00")
        self.assertEqual(len(got), 2)  # 14:00 and 15:00, not the 6-07 one


if __name__ == "__main__":
    unittest.main()

class TestDispatchLifecycleColumns(_StoreBase):
    """3.1.x observe-only dispatch lifecycle capture (dispatch_validation_design.md):
    state + planned/completed energy persisted per slot, no billing effect."""

    def test_lifecycle_columns_present(self):
        cols = {r[1] for r in self.store._conn.execute(
            "PRAGMA table_info(dispatch_slots)")}
        self.assertTrue({"state", "energy_planned", "energy_completed"} <= cols)

    def test_upsert_and_read_roundtrip(self):
        self.store.upsert_dispatch_slot(
            "2026-06-27T19:00:00", provider="MYENERGI_V2", source="smart-charge",
            state="planned", energy_planned=1.71)
        r = self.store.get_dispatch_slot("2026-06-27T19:00:00")
        self.assertEqual(r["state"], "planned")
        self.assertAlmostEqual(r["energy_planned"], 1.71)
        self.assertIsNone(r["energy_completed"])

    def test_completed_energy_preserved_across_replan(self):
        s = "2026-06-27T20:00:00"
        self.store.upsert_dispatch_slot(s, source="smart-charge",
                                        state="completed", energy_completed=0.02)
        # a later planned re-capture (no completed energy) must NOT wipe it
        self.store.upsert_dispatch_slot(s, source="smart-charge", state="planned",
                                        energy_planned=1.5)
        r = self.store.get_dispatch_slot(s)
        self.assertAlmostEqual(r["energy_completed"], 0.02)
        self.assertAlmostEqual(r["energy_planned"], 1.5)

    def test_legacy_row_reads_null_lifecycle(self):
        # a row written the old way (no lifecycle fields) reads back None, not error
        self.store._conn.execute(
            "INSERT INTO dispatch_slots (slot_start, off_peak, captured_at) "
            "VALUES ('2026-06-27T18:00:00', 1, '2026-06-27T18:00:00')")
        r = self.store.get_dispatch_slot("2026-06-27T18:00:00")
        self.assertIsNone(r["state"])
        self.assertIsNone(r["energy_planned"])


class TestPowerInvertPersistence(_StoreBase):
    """Issue #251: the main Power Sensor 'invert' flag (power_invert) must survive
    a config save/load — previously it had no column and was dropped on save, so
    the checkbox reverted."""

    def test_power_invert_column_present(self):
        cols = {r[1] for r in self.store._conn.execute("PRAGMA table_info(meters)")}
        self.assertIn("power_invert", cols)

    def test_power_invert_roundtrips(self):
        cfg = {"schema_version": "1.0", "meters": {"electricity_main": {
            "meta": {"timezone": "Europe/London", "billing_day": 1,
                     "sub_meter": False, "power_sensor": "sensor.pwr",
                     "power_invert": True},
            "channels": {"import": {"read": "s", "rate": ""},
                         "export": {"read": "", "rate": ""}}}}}
        self.store.insert_config_period(cfg)
        pid = self.store.get_current_config_period_id()
        meta = self.store.config_from_db(pid)["meters"]["electricity_main"]["meta"]
        self.assertTrue(meta.get("power_invert"))

    def test_device_power_invert_roundtrips(self):
        # Device power sensor invert (#251, device coverage) must also persist
        cfg = {"schema_version": "1.0", "meters": {
            "electricity_main": {"meta": {"timezone": "Europe/London", "billing_day": 1,
                     "sub_meter": False}, "channels": {"import": {"read": "m", "rate": ""},
                     "export": {"read": "", "rate": ""}}},
            "ev_charger": {"meta": {"sub_meter": True, "parent_meter": "electricity_main",
                     "meter_type": "ev", "device_power_sensor": "sensor.z",
                     "device_power_invert": True},
                     "channels": {"import": {"read": "e", "rate_source": "main"}}}}}
        self.store.insert_config_period(cfg)
        pid = self.store.get_current_config_period_id()
        meta = self.store.config_from_db(pid)["meters"]["ev_charger"]["meta"]
        self.assertTrue(meta.get("device_power_invert"))

    def test_power_invert_absent_stays_falsey(self):
        cfg = {"schema_version": "1.0", "meters": {"electricity_main": {
            "meta": {"timezone": "Europe/London", "billing_day": 1,
                     "sub_meter": False, "power_sensor": "sensor.pwr"},
            "channels": {"import": {"read": "s", "rate": ""},
                         "export": {"read": "", "rate": ""}}}}}
        self.store.insert_config_period(cfg)
        pid = self.store.get_current_config_period_id()
        meta = self.store.config_from_db(pid)["meters"]["electricity_main"]["meta"]
        self.assertFalse(meta.get("power_invert"))


class TestDispatchHistoryAccumulation(_StoreBase):
    """§11 observe-only accumulation: a persistent record of every dispatch seen,
    separate from the billing dispatch_slots, so 'absent' is trustworthy and tail
    dispatches aren't lost to the per-poll snapshot."""

    def test_table_present(self):
        cols = {r[1] for r in self.store._conn.execute(
            "PRAGMA table_info(dispatch_history)")}
        self.assertTrue(
            {"slot_start", "kind", "provider", "source", "energy_kwh",
             "first_seen", "last_seen"} <= cols)

    def test_accumulates_first_seen_preserved_last_seen_advances(self):
        self.store.record_dispatch_history(
            "2026-07-06T20:00:00", "completed", provider="Myenergi",
            source="unknown", energy_kwh=-0.27, seen_at="2026-07-07T07:50:00")
        # re-seen on a later poll
        self.store.record_dispatch_history(
            "2026-07-06T20:00:00", "completed", provider="Myenergi",
            source="unknown", energy_kwh=-0.27, seen_at="2026-07-07T08:33:00")
        r = self.store.get_dispatch_history(
            "2026-07-06T00:00:00", "2026-07-08T00:00:00", kind="completed")[0]
        self.assertEqual(r["first_seen"], "2026-07-07T07:50:00")  # preserved
        self.assertEqual(r["last_seen"], "2026-07-07T08:33:00")   # advanced
        self.assertAlmostEqual(r["energy_kwh"], -0.27, places=3)

    def test_records_tail_slot_dispatch_slots_would_skip(self):
        # a completed dispatch with no matching planned slot — dispatch_slots
        # skips it, but history must retain it
        self.store.record_dispatch_history(
            "2026-07-06T20:00:00", "completed", energy_kwh=-0.27)
        self.assertIsNone(self.store.get_dispatch_slot("2026-07-06T20:00:00"))
        hist = self.store.get_dispatch_history(
            "2026-07-06T00:00:00", "2026-07-08T00:00:00")
        self.assertEqual(len(hist), 1)

    def test_planned_and_completed_coexist_same_slot(self):
        self.store.record_dispatch_history(
            "2026-07-07T02:00:00", "planned", energy_kwh=-3.35)
        self.store.record_dispatch_history(
            "2026-07-07T02:00:00", "completed", energy_kwh=-3.18)
        both = self.store.get_dispatch_history(
            "2026-07-07T00:00:00", "2026-07-07T03:00:00")
        self.assertEqual({r["kind"] for r in both}, {"planned", "completed"})

    def test_prune(self):
        self.store.record_dispatch_history(
            "2020-01-01T00:00:00", "completed", energy_kwh=-1.0)
        deleted = self.store.prune_dispatch_history(days=90)
        self.assertEqual(deleted, 1)