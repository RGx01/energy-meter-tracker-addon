"""
test_block_store.py
===================
Unit tests for block_store.py.

All tests use an in-memory SQLite database — no files are written to disk.

Run with:
    python3 -B test_block_store.py
or:
    python3 -m pytest test_block_store.py -v
"""

import json
import sys
import os
import unittest
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(__file__))
from block_store import (BlockStore, open_block_store,
                          local_date_to_utc_bounds, local_date_range_to_utc_bounds)


# ─────────────────────────────────────────────────────────────────────────────
# Fixtures
# ─────────────────────────────────────────────────────────────────────────────

EXAMPLE_CONFIG = {
    "meters": {
        "electricity_main": {
            "meta": {
                "site": "Test Home",
                "timezone": "Europe/London",
                "billing_day": 1,
                "block_minutes": 30,
                "currency_symbol": "£",
                "currency_code": "GBP",
            },
            "channels": {
                "import": {"sensor": "sensor.import"},
                "export": {"sensor": "sensor.export"},
            },
        }
    }
}

EXAMPLE_CONFIG_WITH_SUB = {
    "meters": {
        "electricity_main": {
            "meta": {
                "site": "Test Home",
                "timezone": "Europe/London",
                "billing_day": 15,
                "block_minutes": 30,
                "currency_symbol": "£",
                "currency_code": "GBP",
            },
            "channels": {
                "import": {"sensor": "sensor.import"},
                "export": {"sensor": "sensor.export"},
            },
        },
        "zappi_ev": {
            "meta": {
                "sub_meter": True,
                "device": "Zappi EV Charger",
                "parent_meter": "electricity_main",
                "block_minutes": 30,
                "timezone": "Europe/London",
                "currency_symbol": "£",
                "currency_code": "GBP",
            },
            "channels": {
                "import": {"sensor": "sensor.zappi"},
            },
        },
    }
}


def make_block(start_iso: str,
               imp_kwh: float = 0.5,
               exp_kwh: float = 0.1,
               standing: float = 0.5,
               interpolated: bool = False,
               meter_id: str = "electricity_main") -> dict:
    """Build a minimal finalised block dict matching the engine output shape."""
    end_dt = datetime.fromisoformat(start_iso) + timedelta(minutes=30)
    end_iso = end_dt.isoformat()
    return {
        "start": start_iso,
        "end":   end_iso,
        "interpolated": interpolated,
        "meters": {
            meter_id: {
                "meta": {
                    "block_minutes":  30,
                    "timezone":       "Europe/London",
                    "billing_day":    1,
                    "currency_symbol":"£",
                    "currency_code":  "GBP",
                    "sub_meter":      False,
                },
                "standing_charge": standing,
                "interpolated":    interpolated,
                "channels": {
                    "import": {
                        "kwh":        imp_kwh,
                        "kwh_remainder": imp_kwh - 0.05,
                        "rate":       0.245,
                        "cost":       round(imp_kwh * 0.245, 4),
                        "cost_remainder": round((imp_kwh - 0.05) * 0.245, 4),
                        "read_start": 1000.0,
                        "read_end":   1000.0 + imp_kwh,
                    },
                    "export": {
                        "kwh":        exp_kwh,
                        "rate":       0.15,
                        "cost":       round(exp_kwh * 0.15, 4),
                        "read_start": 500.0,
                        "read_end":   500.0 + exp_kwh,
                    },
                },
            }
        },
        "totals": {
            "import_kwh":  imp_kwh,
            "import_cost": round(imp_kwh * 0.245, 4),
            "export_kwh":  exp_kwh,
            "export_cost": round(exp_kwh * 0.15, 4),
        },
    }


def make_block_with_sub(start_iso: str) -> dict:
    """Block with main meter + sub-meter."""
    end_iso = (datetime.fromisoformat(start_iso) + timedelta(minutes=30)).isoformat()
    return {
        "start": start_iso,
        "end":   end_iso,
        "interpolated": False,
        "meters": {
            "electricity_main": {
                "meta": {"block_minutes": 30, "timezone": "Europe/London",
                         "billing_day": 15, "currency_symbol": "£",
                         "currency_code": "GBP", "sub_meter": False},
                "standing_charge": 0.5,
                "interpolated": False,
                "channels": {
                    "import": {
                        "kwh": 1.0, "kwh_remainder": 0.7,
                        "rate": 0.245, "cost": 0.245,
                        "cost_remainder": round(0.7 * 0.245, 4),
                        "read_start": 1000.0, "read_end": 1001.0,
                    },
                    "export": {
                        "kwh": 0.1, "rate": 0.15, "cost": 0.015,
                        "read_start": 500.0, "read_end": 500.1,
                    },
                },
            },
            "zappi_ev": {
                "meta": {"block_minutes": 30, "timezone": "Europe/London",
                         "billing_day": 15, "currency_symbol": "£",
                         "currency_code": "GBP", "sub_meter": True},
                "standing_charge": 0.0,
                "interpolated": False,
                "channels": {
                    "import": {
                        "kwh": 0.3, "kwh_grid": 0.3,
                        "rate": 0.245, "cost": round(0.3 * 0.245, 4),
                        "read_start": 200.0, "read_end": 200.3,
                    },
                },
            },
        },
        "totals": {
            "import_kwh": 0.7, "import_cost": round(0.7 * 0.245, 4),
            "export_kwh": 0.1, "export_cost": 0.015,
        },
    }


def new_store() -> BlockStore:
    """Return a fresh in-memory BlockStore."""
    return BlockStore(":memory:")


class TestCarbonUntagRepair(unittest.TestCase):
    """Root-cause fix: carbon must be written IN PLACE (set_block_carbon), and the
    reconstruction blocks a prior carbon round-trip wiped to NULL must be
    re-taggable (retag_untagged_imports). This is the bug that silently untagged
    every imported block the carbon backfill touched."""

    def _store(self):
        store = new_store()
        with store._conn:
            cp = store._conn.execute(
                "INSERT INTO config_periods (effective_from, billing_day, block_minutes, "
                "timezone, currency_symbol, currency_code) "
                "VALUES ('2024-07-01T00:00:00',1,30,'Europe/London','£','GBP')").lastrowid
            store._conn.execute(
                "INSERT INTO meters (config_period_id, meter_id, is_sub_meter) "
                "VALUES (?, 'electricity_main', 0)", (cp,))
            def blk(bs, source, read_start=None):
                store._conn.execute(
                    "INSERT INTO blocks (block_start, block_end, meter_id, config_period_id, "
                    "imp_kwh, imp_read_start, source) VALUES (?,?,'electricity_main',?,1.0,?,?)",
                    (bs, bs, cp, read_start, source))
            # reconstruction wiped to NULL (pre-go-live, no meter read)
            blk("2024-07-01T00:00:00", None)
            blk("2025-01-01T00:00:00", None)
            # one import that survived tagged (carbon couldn't fill it)
            blk("2025-02-01T00:00:00", "imported_api")
            # live blocks: real meter reads, from go-live on
            blk("2026-06-04T23:00:00", "kraken_api", read_start=100.0)
            blk("2026-06-05T00:00:00", None, read_start=101.0)
        return store

    def test_set_block_carbon_preserves_source(self):
        s = self._store()
        self.assertTrue(s.set_block_carbon("2025-02-01T00:00:00", "electricity_main", 200.0, 0.2))
        r = s._conn.execute("SELECT source, carbon_intensity_g, carbon_g FROM blocks "
                            "WHERE block_start='2025-02-01T00:00:00'").fetchone()
        self.assertEqual(r["source"], "imported_api")       # tag intact
        self.assertAlmostEqual(r["carbon_intensity_g"], 200.0)
        self.assertAlmostEqual(r["carbon_g"], 0.2)

    def test_retag_restores_reconstruction_blocks(self):
        s = self._store()
        res = s.retag_untagged_imports()
        self.assertEqual(res["go_live"], "2026-06-04T23:00:00")   # earliest live-read block
        self.assertEqual(res["retagged"], 2)                      # the 2 wiped reconstruction blocks
        srcs = {r["block_start"]: r["source"] for r in s._conn.execute(
            "SELECT block_start, source FROM blocks").fetchall()}
        self.assertEqual(srcs["2024-07-01T00:00:00"], "imported_api")
        self.assertEqual(srcs["2025-01-01T00:00:00"], "imported_api")
        self.assertEqual(srcs["2025-02-01T00:00:00"], "imported_api")   # already tagged
        # live blocks NEVER re-tagged
        self.assertEqual(srcs["2026-06-04T23:00:00"], "kraken_api")
        self.assertIsNone(srcs["2026-06-05T00:00:00"])

    def test_retag_is_idempotent(self):
        s = self._store()
        s.retag_untagged_imports()
        self.assertEqual(s.retag_untagged_imports()["retagged"], 0)


class TestImportRepriceQueue(unittest.TestCase):
    """Reprice queue + in-place re-price of imported blocks (the calm repair pass
    that fixes half-hours the bulk import mispriced under load)."""

    def _store(self):
        store = new_store()
        with store._conn:
            cp = store._conn.execute(
                "INSERT INTO config_periods (effective_from, billing_day, block_minutes, "
                "timezone, currency_symbol, currency_code) "
                "VALUES ('2025-10-01T00:00:00',1,30,'Europe/London','£','GBP')").lastrowid
            store._conn.execute(
                "INSERT INTO meters (config_period_id, meter_id, is_sub_meter) "
                "VALUES (?, 'electricity_main', 0)", (cp,))
            # a mispriced imported block (schedule peak) + a live block (must be immune)
            store._conn.execute(
                "INSERT INTO blocks (block_start, block_end, meter_id, config_period_id, "
                "imp_kwh, imp_rate, imp_cost, source) VALUES "
                "('2025-10-21T18:30:00','2025-10-21T19:00:00','electricity_main',?,3.361,0.28124,0.945,'imported_api')", (cp,))
            store._conn.execute(
                "INSERT INTO blocks (block_start, block_end, meter_id, config_period_id, "
                "imp_kwh, imp_rate, imp_cost, source) VALUES "
                "('2026-06-05T18:30:00','2026-06-05T19:00:00','electricity_main',?,1.0,0.28,0.28,NULL)", (cp,))
        return store

    def test_queue_add_dedup_and_clear(self):
        s = self._store()
        s.add_reprice_queue("import", ["a", "b", "a"])
        s.add_reprice_queue("import", ["b", "c"])
        self.assertEqual(s.get_reprice_queue()["import"], ["a", "b", "c"])
        self.assertEqual(s.reprice_queue_count(), 3)
        s.clear_reprice_queue_slots("import", ["a", "c"])
        self.assertEqual(s.get_reprice_queue()["import"], ["b"])

    def test_reprice_updates_only_on_difference(self):
        s = self._store()
        # correct off-peak price now available → changes, returns True
        self.assertTrue(s.reprice_imported_block(
            "2025-10-21T18:30:00", "electricity_main", "import", 0.07, 0.23528))
        r = s._conn.execute("SELECT imp_rate, imp_cost FROM blocks "
                            "WHERE block_start='2025-10-21T18:30:00'").fetchone()
        self.assertAlmostEqual(r["imp_rate"], 0.07)
        self.assertAlmostEqual(r["imp_cost"], 0.23528)
        # same values again → no change
        self.assertFalse(s.reprice_imported_block(
            "2025-10-21T18:30:00", "electricity_main", "import", 0.07, 0.23528))

    def test_reprice_never_touches_live_block(self):
        s = self._store()
        self.assertFalse(s.reprice_imported_block(
            "2026-06-05T18:30:00", "electricity_main", "import", 0.07, 0.07))
        r = s._conn.execute("SELECT imp_rate FROM blocks "
                            "WHERE block_start='2026-06-05T18:30:00'").fetchone()
        self.assertAlmostEqual(r["imp_rate"], 0.28)   # untouched (source is NULL)

    def test_reprice_from_csv_sets_exact_billed_cost(self):
        # A dispatch slot the API mispriced at peak; the CSV (billing truth) says
        # off-peak. Repricing from the CSV sets the exact billed cost + rate, only
        # on imported rows, and leaves the live (NULL-source) row alone. The store
        # fixture already has an imported block at 2025-10-21T18:30 (peak 0.28124)
        # and a live block at 2026-06-05T18:30 (NULL source).
        s = self._store()
        # CSV row: local +01:00 (18:30 UTC = 19:30 local), 3.361 kWh, 23.528p off-peak.
        # Plus a row for the live block's time — must be ignored (source NULL).
        csv = ("Consumption (kwh), Estimated Cost Inc. Tax (p), Standing Charge Inc. Tax (p), Start, End\n"
               "3.361, 23.528, 0.99, 2025-10-21T19:30:00+01:00, 2025-10-21T20:00:00+01:00\n"
               "1.0, 7.0, 0.99, 2026-06-05T19:30:00+01:00, 2026-06-05T20:00:00+01:00\n")
        res = s.reprice_imported_blocks_from_csv(csv)
        self.assertEqual(res["changed"], 1)                        # only the imported row
        r = s._conn.execute("SELECT imp_rate, imp_cost FROM blocks "
                            "WHERE block_start='2025-10-21T18:30:00'").fetchone()
        self.assertAlmostEqual(r["imp_cost"], 0.23528)             # exact billed £ inc-VAT
        self.assertAlmostEqual(r["imp_rate"], round(0.23528 / 3.361, 6))  # cost/kWh ≈ 7p
        r2 = s._conn.execute("SELECT imp_rate FROM blocks "
                             "WHERE block_start='2026-06-05T18:30:00'").fetchone()
        self.assertAlmostEqual(r2["imp_rate"], 0.28)               # live row untouched

    def test_reprice_from_csv_rate_first_and_column_order_agnostic(self):
        # Shares the name-matched parser now, so (a) a Unit Rate column drives cost
        # = rate × kWh (no explicit cost needed), and (b) column ORDER doesn't
        # matter — the old positional reader would have misread this layout.
        s = self._store()
        csv = ("Start,Consumption (kWh),Unit Rate (p/kWh),End\n"
               "2025-10-21T19:30:00+01:00,4.0,7.5,2025-10-21T20:00:00+01:00\n")
        res = s.reprice_imported_blocks_from_csv(csv)
        self.assertEqual(res["changed"], 1)
        r = s._conn.execute("SELECT imp_rate, imp_cost FROM blocks "
                            "WHERE block_start='2025-10-21T18:30:00'").fetchone()
        self.assertAlmostEqual(r["imp_cost"], 0.30)                # 0.075 × 4.0
        self.assertAlmostEqual(r["imp_rate"], 0.075)               # cost / kWh

    def test_get_imported_block_starts_range(self):
        s = self._store()
        got = s.get_imported_block_starts("2025-10-01T00:00:00", "2025-11-01T00:00:00")
        self.assertEqual(got, ["2025-10-21T18:30:00"])
        # live block excluded even though in a wide range
        got2 = s.get_imported_block_starts("2025-01-01T00:00:00", "2027-01-01T00:00:00")
        self.assertEqual(got2, ["2025-10-21T18:30:00"])

    def test_get_imported_block_pricing_carries_rate_kwh_cost(self):
        # The cheap read the suspect prefilter uses: current rate/kwh/cost per
        # imported block, live (NULL-source) rows excluded.
        s = self._store()
        rows = s.get_imported_block_pricing("2025-01-01T00:00:00", "2027-01-01T00:00:00")
        self.assertEqual([r["start"] for r in rows], ["2025-10-21T18:30:00"])
        r = rows[0]
        self.assertAlmostEqual(r["imp_rate"], 0.28124)
        self.assertAlmostEqual(r["imp_kwh"], 3.361)
        self.assertAlmostEqual(r["imp_cost"], 0.945)

    def test_clear_reprice_queue_empties_all(self):
        s = self._store()
        s.add_reprice_queue("import", ["2025-10-21T18:30:00"])
        s.add_reprice_queue("export", ["2025-10-21T19:00:00"])
        self.assertEqual(s.reprice_queue_count(), 2)
        s.clear_reprice_queue()                       # new-import reset
        self.assertEqual(s.reprice_queue_count(), 0)


class TestGridInvariantSweepBL19(unittest.TestCase):
    """BL-19 — flag_grid_invariant_violations flags only settled blocks whose
    sub-meter grid attribution exceeds the parent's grid import."""

    def setUp(self):
        self.store = new_store()
        self.store.insert_config_period(EXAMPLE_CONFIG_WITH_SUB,
                                        effective_from="2026-07-01T00:00:00")

    def _seed(self, start, main_grid, sub_grid, settled=True):
        self.store.append_block(make_block_with_sub(start), config_period_id=1)
        self.store._conn.execute(
            "UPDATE blocks SET imp_kwh=?, imp_kwh_api=? "
            "WHERE block_start=? AND meter_id='electricity_main'",
            (main_grid, main_grid if settled else None, start))
        self.store._conn.execute(
            "UPDATE blocks SET imp_kwh_grid=? "
            "WHERE block_start=? AND meter_id='zappi_ev'",
            (sub_grid, start))
        self.store._conn.commit()

    def _flag(self, start):
        return self.store._conn.execute(
            "SELECT needs_pass2_rerun FROM blocks WHERE block_start=? "
            "AND meter_id='electricity_main'", (start,)).fetchone()["needs_pass2_rerun"]

    def test_flags_settled_violation_only(self):
        self._seed("2026-07-10T16:30:00", main_grid=0.157, sub_grid=2.053)  # violation
        self._seed("2026-07-10T16:00:00", main_grid=2.894, sub_grid=2.053)  # ok (main covers)
        self._seed("2026-07-10T12:00:00", main_grid=0.10,  sub_grid=2.0,
                   settled=False)                                            # unsettled — skip
        self.assertEqual(self.store.flag_grid_invariant_violations(), 1)
        self.assertEqual(self._flag("2026-07-10T16:30:00"), 1)   # flagged
        self.assertEqual(self._flag("2026-07-10T16:00:00"), 0)   # not flagged
        self.assertEqual(self._flag("2026-07-10T12:00:00"), 0)   # unsettled, not flagged

    def test_no_violations_flags_nothing(self):
        self._seed("2026-07-10T16:00:00", main_grid=2.894, sub_grid=2.053)
        self.assertEqual(self.store.flag_grid_invariant_violations(), 0)

    def test_tolerance_ignores_float_noise(self):
        # sub grid a hair over main (rounding noise) must NOT flag.
        self._seed("2026-07-10T16:00:00", main_grid=1.0, sub_grid=1.0 + 1e-6)
        self.assertEqual(self.store.flag_grid_invariant_violations(), 0)


class TestReviewFlagsBL18(unittest.TestCase):
    """BL-18 — per-block review flag with a stored reason, and its clear paths."""

    def setUp(self):
        self.store = new_store()
        self.store.insert_config_period(EXAMPLE_CONFIG)
        self.store.append_block(make_block("2026-07-10T16:30:00", imp_kwh=0.157))

    def test_flag_sets_reason_and_surfaces(self):
        reason = "dispatch ambiguous: completed 2.10 kWh without started"
        self.assertEqual(
            self.store.flag_block_for_review("2026-07-10T16:30:00", reason), 1)
        alerts = self.store.get_drift_alerts()
        self.assertEqual(len(alerts), 1)
        self.assertEqual(alerts[0]["reason"], reason)
        self.assertEqual(alerts[0]["block_start"], "2026-07-10T16:30:00")

    def test_clear_block_review(self):
        self.store.flag_block_for_review("2026-07-10T16:30:00", "x")
        self.assertEqual(self.store.clear_block_review("2026-07-10T16:30:00"), 1)
        self.assertEqual(self.store.get_drift_alerts(), [])

    def test_dismiss_nulls_reason(self):
        self.store.flag_block_for_review("2026-07-10T16:30:00", "x")
        bid = self.store.get_drift_alerts()[0]["block_id"]
        self.store.dismiss_drift_alerts([bid])
        row = self.store._conn.execute(
            "SELECT needs_review, review_reason FROM blocks WHERE id=?", (bid,)).fetchone()
        self.assertEqual(row["needs_review"], 0)
        self.assertIsNone(row["review_reason"])

    def test_legacy_drift_reason_synthesised(self):
        # A flag with no stored reason but a CAD/DCC delta gets a synthesised one.
        self.store._conn.execute(
            "UPDATE blocks SET needs_review=1, imp_kwh=1.0, imp_kwh_api=0.5 "
            "WHERE meter_id='electricity_main'")
        self.store._conn.commit()
        self.assertTrue(
            self.store.get_drift_alerts()[0]["reason"].startswith("CAD/DCC settlement drift"))

    def _add_drift_flag(self, start="2026-07-11T00:00:00"):
        # A drift-style flag: needs_review set, NO review_reason.
        self.store.append_block(make_block(start, imp_kwh=1.0))
        self.store._conn.execute(
            "UPDATE blocks SET needs_review=1, imp_kwh_api=0.5, review_reason=NULL "
            "WHERE block_start=? AND meter_id='electricity_main'", (start,))
        self.store._conn.commit()

    def test_get_review_blocks_excludes_drift(self):
        self.store.flag_block_for_review("2026-07-10T16:30:00", "dispatch ambiguous")
        self._add_drift_flag()
        rows = self.store.get_review_blocks()
        self.assertEqual(len(rows), 1)                       # only the dispatch flag
        self.assertEqual(rows[0]["block_start"], "2026-07-10T16:30:00")
        self.assertEqual(rows[0]["reason"], "dispatch ambiguous")

    def test_get_review_blocks_excludes_auto_corrections_and_null_rows(self):
        # The IOG pricing panel is for rate tasks only. Auto-CORRECTION reasons (the
        # integrity sweeps) and malformed/legacy rows (null-ish block_start) must NOT
        # appear — they're not rate-actionable.
        self.store.flag_block_for_review("2026-07-10T16:30:00", "dispatch ambiguous")
        self.store.append_block(make_block("2026-07-12T00:00:00", imp_kwh=1.0))
        for reason in BlockStore.AUTO_CORRECTION_REASONS:
            self.store._conn.execute(
                "UPDATE blocks SET needs_review=1, review_reason=? "
                "WHERE block_start='2026-07-12T00:00:00' AND meter_id='electricity_main'",
                (reason,))
        # a legacy/malformed flag with an empty block_start
        self.store._conn.execute(
            "INSERT INTO blocks (block_start, block_end, meter_id, config_period_id, "
            "needs_review, review_reason) VALUES ('', '', 'electricity_main', "
            "(SELECT id FROM config_periods LIMIT 1), 1, 'flagged')")
        self.store._conn.commit()
        rows = self.store.get_review_blocks()
        self.assertEqual([r["reason"] for r in rows], ["dispatch ambiguous"])  # only the rate task

    def test_dismiss_review_blocks_leaves_drift(self):
        self.store.flag_block_for_review("2026-07-10T16:30:00", "dispatch ambiguous")
        self._add_drift_flag("2026-07-11T00:00:00")
        cleared = self.store.dismiss_review_blocks(None)     # dismiss all (scoped)
        self.assertEqual(cleared, 1)                         # only the dispatch flag
        self.assertEqual(self.store.get_review_blocks(), [])
        drift = self.store._conn.execute(
            "SELECT needs_review FROM blocks WHERE block_start='2026-07-11T00:00:00' "
            "AND meter_id='electricity_main'").fetchone()
        self.assertEqual(drift["needs_review"], 1)           # dormant drift survives

    def test_dismissed_block_is_not_reflagged(self):
        # #322: once dismissed, the dispatch reconcile must not resurrect the same
        # ambiguous block on its next re-scan.
        bs = "2026-07-10T16:30:00"
        self.store.flag_block_for_review(bs, "dispatch ambiguous")
        self.store.dismiss_review_blocks(None)               # user dismisses
        self.assertEqual(self.store.get_review_blocks(), [])
        # A later reconcile pass tries to flag it again — must be a no-op.
        n = self.store.flag_block_for_review(bs, "completed (3.23 kWh) but not started")
        self.assertEqual(n, 0)                               # nothing re-flagged
        self.assertEqual(self.store.get_review_blocks(), []) # stays gone
        row = self.store._conn.execute(
            "SELECT needs_review, review_dismissed FROM blocks "
            "WHERE block_start=? AND meter_id='electricity_main'", (bs,)).fetchone()
        self.assertEqual(row["needs_review"], 0)
        self.assertEqual(row["review_dismissed"], 1)


# ─────────────────────────────────────────────────────────────────────────────
# Tests: schema and setup
# ─────────────────────────────────────────────────────────────────────────────

class TestSchema(unittest.TestCase):

    def test_schema_created(self):
        store = new_store()
        cur = store._conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        )
        tables = {r["name"] for r in cur.fetchall()}
        self.assertIn("blocks", tables)
        self.assertIn("config_periods", tables)
        self.assertIn("meters", tables)
        self.assertIn("reads", tables)
        self.assertIn("store_meta", tables)
        store.close()

    def test_schema_version_recorded(self):
        store = new_store()
        cur = store._conn.execute(
            "SELECT value FROM store_meta WHERE key = 'schema_version'"
        )
        row = cur.fetchone()
        self.assertIsNotNone(row)
        self.assertEqual(row["value"], "1")
        store.close()

    def test_wal_mode(self):
        store = new_store()
        cur = store._conn.execute("PRAGMA journal_mode")
        # in-memory DB may report 'memory' not 'wal' — just check it doesn't error
        self.assertIsNotNone(cur.fetchone())
        store.close()

    def test_busy_timeout_set(self):
        # Critical for the two-connection (engine + web) WAL setup — without it,
        # write contention raises "database is locked" instead of waiting.
        store = new_store()
        bt = store._conn.execute("PRAGMA busy_timeout").fetchone()[0]
        self.assertEqual(bt, 5000)
        store.close()

    def test_no_open_transaction_after_init(self):
        # Fresh-DB lock root cause: __init__ must leave the connection in a clean
        # autocommit state (no lingering transaction), or the engine's startup
        # wal_checkpoint(TRUNCATE) fails with "database table is locked".
        import tempfile, os
        p = tempfile.mktemp(suffix=".db")
        try:
            from block_store import BlockStore
            store = BlockStore(p)
            self.assertFalse(store._conn.in_transaction,
                             "BlockStore.__init__ left an open transaction")
            # The checkpoint that was failing on fresh DBs must now succeed.
            store._conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            store.close()
        finally:
            for ext in ("", "-wal", "-shm"):
                try: os.remove(p + ext)
                except OSError: pass

    def test_reimport_overwrites_imported_standing(self):
        # A re-import MUST re-price standing charge on blocks it owns — the old
        # guard only wrote standing when the existing value was NULL/0, so a
        # corrected figure could never land on a re-run.
        store = BlockStore(":memory:")
        store.insert_config_period(EXAMPLE_CONFIG)
        base = dict(kwh=0.1, rate=0.07, cost=0.007)
        store.upsert_imported_blocks(
            [dict(start="2026-02-04T00:00:00", standing=0.1188, **base)],
            "electricity_main", "import", source="imported_api")
        store.upsert_imported_blocks(     # corrected standing
            [dict(start="2026-02-04T00:00:00", standing=0.4752, **base)],
            "electricity_main", "import", source="imported_api")
        row = store._conn.execute(
            "SELECT standing_charge FROM blocks "
            "WHERE block_start='2026-02-04T00:00:00'").fetchone()
        self.assertAlmostEqual(row["standing_charge"], 0.4752, places=4)
        store.close()

    def test_export_channel_does_not_clobber_standing(self):
        # Standing charge lives on the shared block row. The export channel (no
        # standing charge → passes NULL) must NOT overwrite the import channel's
        # value with null/0 — the regression that zeroed every imported day.
        store = BlockStore(":memory:")
        store.insert_config_period(EXAMPLE_CONFIG)
        store.upsert_imported_blocks(
            [dict(start="2026-02-04T00:00:00", standing=0.4752, kwh=0.1,
                  rate=0.07, cost=0.007)],
            "electricity_main", "import", source="imported_api")
        store.upsert_imported_blocks(     # export for the SAME row, standing None
            [dict(start="2026-02-04T00:00:00", standing=None, kwh=0.5,
                  rate=0.15, cost=0.075)],
            "electricity_main", "export", source="imported_api")
        row = store._conn.execute(
            "SELECT standing_charge, exp_kwh FROM blocks "
            "WHERE block_start='2026-02-04T00:00:00'").fetchone()
        self.assertAlmostEqual(row["standing_charge"], 0.4752, places=4)
        self.assertAlmostEqual(row["exp_kwh"], 0.5, places=4)   # export still lands
        store.close()

    def test_import_preserves_live_standing(self):
        # An import must NOT clobber a genuine live/settled block's standing.
        store = BlockStore(":memory:")
        store.insert_config_period(EXAMPLE_CONFIG)
        pid = store._conn.execute(
            "SELECT id FROM config_periods LIMIT 1").fetchone()["id"]
        store._conn.execute(
            "INSERT INTO blocks (block_start, block_end, meter_id, "
            "config_period_id, standing_charge) VALUES (?,?,?,?,?)",
            ("2026-06-10T00:00:00", "2026-06-10T00:30:00", "electricity_main",
             pid, 0.504559))      # live block (source NULL)
        store._conn.commit()
        store.upsert_imported_blocks(
            [dict(start="2026-06-10T00:00:00", standing=0.4752, kwh=0.1,
                  rate=0.07, cost=0.007)],
            "electricity_main", "import", source="imported_api")
        row = store._conn.execute(
            "SELECT standing_charge FROM blocks "
            "WHERE block_start='2026-06-10T00:00:00'").fetchone()
        self.assertAlmostEqual(row["standing_charge"], 0.504559, places=6)
        store.close()

    def test_foreign_keys_on(self):
        store = new_store()
        cur = store._conn.execute("PRAGMA foreign_keys")
        self.assertEqual(cur.fetchone()[0], 1)
        store.close()

    def test_indexes_created(self):
        store = new_store()
        cur = store._conn.execute(
            "SELECT name FROM sqlite_master WHERE type='index'"
        )
        indexes = {r["name"] for r in cur.fetchall()}
        self.assertIn("idx_blocks_start", indexes)
        self.assertIn("idx_blocks_meter", indexes)
        self.assertIn("idx_reads_captured", indexes)
        self.assertNotIn("idx_blocks_date", indexes)
        self.assertNotIn("idx_blocks_ym", indexes)
        store.close()

    def test_read_only_mode_reads_but_rejects_writes(self):
        # The off-loop chart renderer opens a read-only companion: it must read
        # the writer's data but never be able to write (query_only), so it can't
        # contend with or corrupt the primary connection.
        import tempfile, os
        p = tempfile.mktemp(suffix=".db")
        try:
            w = BlockStore(p)
            w.insert_config_period(EXAMPLE_CONFIG)
            pid = w._conn.execute(
                "SELECT id FROM config_periods LIMIT 1").fetchone()["id"]
            w._conn.execute(
                "INSERT INTO blocks (block_start, block_end, meter_id, "
                "config_period_id) VALUES (?,?,?,?)",
                ("2026-05-01T00:00:00", "2026-05-01T00:30:00",
                 "electricity_main", pid))
            w._conn.commit()

            ro = BlockStore(p, read_only=True)
            self.assertEqual(ro.count_blocks(), 1)          # reads the writer's data
            with self.assertRaises(Exception):              # query_only blocks writes
                ro._conn.execute(
                    "INSERT INTO blocks (block_start, block_end, meter_id, "
                    "config_period_id) VALUES (?,?,?,?)",
                    ("2026-05-02T00:00:00", "2026-05-02T00:30:00",
                     "electricity_main", pid))
                ro._conn.commit()
            ro.close()
            w.close()
        finally:
            for ext in ("", "-wal", "-shm"):
                try: os.remove(p + ext)
                except OSError: pass


# ─────────────────────────────────────────────────────────────────────────────
# Tests: config periods
# ─────────────────────────────────────────────────────────────────────────────

class TestConfigPeriods(unittest.TestCase):

    def setUp(self):
        self.store = new_store()

    def tearDown(self):
        self.store.close()

    def test_insert_config_period(self):
        pid = self.store.insert_config_period(EXAMPLE_CONFIG)
        self.assertEqual(pid, 1)

    def test_config_period_fields_extracted(self):
        self.store.insert_config_period(EXAMPLE_CONFIG)
        cp = self.store.get_config_period(1)
        self.assertEqual(cp["billing_day"], 1)
        self.assertEqual(cp["block_minutes"], 30)
        self.assertEqual(cp["timezone"], "Europe/London")
        self.assertEqual(cp["currency_symbol"], "£")
        self.assertEqual(cp["currency_code"], "GBP")
        self.assertEqual(cp["site_name"], "Test Home")
        self.assertIsNone(cp["effective_to"])

    def test_config_from_db_roundtrip(self):
        """config_from_db() should reproduce the original config dict."""
        self.store.insert_config_period(EXAMPLE_CONFIG)
        period_id = self.store.get_current_config_period_id()
        restored = self.store.config_from_db(period_id)
        # Check top-level structure
        self.assertIn("meters", restored)
        self.assertIn("electricity_main", restored["meters"])
        # Check billing scalar fields round-trip
        main_meta = restored["meters"]["electricity_main"]["meta"]
        orig_meta  = EXAMPLE_CONFIG["meters"]["electricity_main"]["meta"]
        self.assertEqual(main_meta["billing_day"],     orig_meta["billing_day"])
        self.assertEqual(main_meta["timezone"],        orig_meta["timezone"])
        self.assertEqual(main_meta["currency_symbol"], orig_meta["currency_symbol"])
        # Check channel sensors
        imp = restored["meters"]["electricity_main"]["channels"]["import"]
        orig_imp = EXAMPLE_CONFIG["meters"]["electricity_main"]["channels"]["import"]
        self.assertEqual(imp.get("read"), orig_imp.get("read"))
        self.assertEqual(imp.get("rate"), orig_imp.get("rate"))

    def test_power_source_marker_round_trips(self):
        """The Octopus Mini opt-in marker (meta.power_source) must survive the
        DB write/read round-trip — it has a dedicated column, not a generic blob."""
        import copy
        cfg = copy.deepcopy(EXAMPLE_CONFIG)
        cfg["meters"]["electricity_main"]["meta"]["power_source"] = "mini"
        self.store.insert_config_period(cfg)
        period_id = self.store.get_current_config_period_id()
        restored = self.store.config_from_db(period_id)
        self.assertEqual(
            restored["meters"]["electricity_main"]["meta"].get("power_source"), "mini")
        # And clearing it round-trips to absent (not stale "mini")
        cfg2 = copy.deepcopy(EXAMPLE_CONFIG)
        cfg2["meters"]["electricity_main"]["meta"].pop("power_source", None)
        self.store.insert_config_period(cfg2)
        restored2 = self.store.config_from_db(self.store.get_current_config_period_id())
        self.assertNotIn(
            "power_source", restored2["meters"]["electricity_main"]["meta"])

    def test_rate_source_marker_round_trips(self):
        """The per-device 'use overlay' marker (meta.rate_source) must survive the
        DB write/read round-trip — dedicated column, like power_source."""
        import copy
        cfg = copy.deepcopy(EXAMPLE_CONFIG)
        cfg["meters"]["electricity_main"]["meta"]["rate_source"] = "base"
        self.store.insert_config_period(cfg)
        period_id = self.store.get_current_config_period_id()
        restored = self.store.config_from_db(period_id)
        self.assertEqual(
            restored["meters"]["electricity_main"]["meta"].get("rate_source"), "base")
        # Clearing it round-trips to absent (engine then applies its default).
        cfg2 = copy.deepcopy(EXAMPLE_CONFIG)
        cfg2["meters"]["electricity_main"]["meta"].pop("rate_source", None)
        self.store.insert_config_period(cfg2)
        restored2 = self.store.config_from_db(self.store.get_current_config_period_id())
        self.assertNotIn(
            "rate_source", restored2["meters"]["electricity_main"]["meta"])

    def test_second_config_period_closes_first(self):
        self.store.insert_config_period(EXAMPLE_CONFIG,
                                        effective_from="2026-01-01T00:00:00")
        config2 = json.loads(json.dumps(EXAMPLE_CONFIG))
        config2["meters"]["electricity_main"]["meta"]["billing_day"] = 15
        self.store.insert_config_period(config2,
                                        effective_from="2026-03-15T00:00:00",
                                        change_reason="Supplier change")
        cp1 = self.store.get_config_period(1)
        cp2 = self.store.get_config_period(2)
        self.assertEqual(cp1["effective_to"], "2026-03-15T00:00:00")
        self.assertIsNone(cp2["effective_to"])
        self.assertEqual(cp2["billing_day"], 15)
        self.assertEqual(cp2["change_reason"], "Supplier change")

    def test_get_current_config_period_id(self):
        self.store.insert_config_period(EXAMPLE_CONFIG)
        pid = self.store.get_current_config_period_id()
        self.assertEqual(pid, 1)

    def test_get_current_config_period_id_after_update(self):
        self.store.insert_config_period(EXAMPLE_CONFIG,
                                        effective_from="2026-01-01T00:00:00")
        config2 = json.loads(json.dumps(EXAMPLE_CONFIG))
        self.store.insert_config_period(config2,
                                        effective_from="2026-03-15T00:00:00")
        self.assertEqual(self.store.get_current_config_period_id(), 2)

    def test_get_config_period_for_date_current(self):
        self.store.insert_config_period(EXAMPLE_CONFIG,
                                        effective_from="2026-01-01T00:00:00")
        cp = self.store.get_config_period_for_date("2026-04-01")
        self.assertIsNotNone(cp)
        self.assertEqual(cp["id"], 1)

    def test_get_config_period_for_date_historical(self):
        self.store.insert_config_period(EXAMPLE_CONFIG,
                                        effective_from="2026-01-01T00:00:00")
        config2 = json.loads(json.dumps(EXAMPLE_CONFIG))
        config2["meters"]["electricity_main"]["meta"]["billing_day"] = 15
        self.store.insert_config_period(config2,
                                        effective_from="2026-03-15T00:00:00")
        # Date before the change
        cp = self.store.get_config_period_for_date("2026-02-01")
        self.assertEqual(cp["id"], 1)
        self.assertEqual(cp["billing_day"], 1)
        # Date after the change
        cp2 = self.store.get_config_period_for_date("2026-04-01")
        self.assertEqual(cp2["id"], 2)
        self.assertEqual(cp2["billing_day"], 15)

    def test_no_config_period_returns_none(self):
        self.assertIsNone(self.store.get_current_config_period_id())
        self.assertIsNone(self.store.get_config_period(999))


# ─────────────────────────────────────────────────────────────────────────────
# Tests: append and read blocks
# ─────────────────────────────────────────────────────────────────────────────

class TestFinaliseHorizon(unittest.TestCase):
    """Past-horizon limbo-block finalisation (finalised-from-CAD flag): drains the
    unsettled count without faking DCC settlement, and is reversible."""

    def setUp(self):
        self.store = BlockStore(":memory:")
        self.store.insert_config_period(EXAMPLE_CONFIG)

    def _add(self, start_iso, imp_kwh=0.5):
        self.store.append_block(make_block(start_iso, imp_kwh=imp_kwh))

    def test_finalise_marks_only_old_unsettled(self):
        old, recent = "2026-05-01T00:00:00", "2026-06-09T00:00:00"
        self._add(old); self._add(recent)
        self.assertEqual(self.store.count_unsettled_blocks(), 2)
        n = self.store.finalise_past_horizon_blocks("2026-05-28T00:00:00")
        self.assertEqual(n, 1)                                   # only the old one
        self.assertEqual(self.store.count_unsettled_blocks(), 1)  # recent still counts
        starts = [r["block_start"] for r in self.store.get_unsettled_blocks()]
        self.assertIn(recent, starts)
        self.assertNotIn(old, starts)                            # excluded from the list

    def test_finalise_idempotent(self):
        self._add("2026-05-01T00:00:00")
        self.assertEqual(
            self.store.finalise_past_horizon_blocks("2026-05-28T00:00:00"), 1)
        self.assertEqual(
            self.store.finalise_past_horizon_blocks("2026-05-28T00:00:00"), 0)  # re-run: none

    def test_finalise_skips_settled(self):
        old = "2026-05-01T00:00:00"
        self._add(old)   # make_block gives exp_kwh=0.1, so settle both channels
        self.store.upsert_kraken_block(old, "electricity_main", 0.5,
                                       channel="import", billing_source="api")
        self.store.upsert_kraken_block(old, "electricity_main", 0.1,
                                       channel="export", billing_source="api")
        self.assertEqual(
            self.store.finalise_past_horizon_blocks("2026-05-28T00:00:00"), 0)  # already DCC

    def test_settlement_clears_finalised_flag(self):
        # Reversibility: a real DCC settlement landing later supersedes the
        # CAD finalisation (imp_kwh_api set, flag cleared).
        old = "2026-05-01T00:00:00"
        self._add(old)
        self.store.finalise_past_horizon_blocks("2026-05-28T00:00:00")
        self.assertEqual(self.store.count_unsettled_blocks(), 0)   # finalised → not counted
        self.store.upsert_kraken_block(old, "electricity_main", 0.5, billing_source="api")
        row = self.store._conn.execute(
            "SELECT imp_kwh_api, finalised_from_cad FROM blocks WHERE block_start=?",
            (old,)).fetchone()
        self.assertIsNotNone(row["imp_kwh_api"])        # real settlement landed
        self.assertEqual(row["finalised_from_cad"], 0)  # flag cleared


class TestExportSettlementUnsettled303(unittest.TestCase):
    """#303: a block whose import is DCC-settled but whose export isn't (and did
    export) must still count as unsettled — otherwise it ages out of the sweep
    window and its export figure is never corrected from the estimate."""

    def setUp(self):
        self.store = BlockStore(":memory:")
        self.store.insert_config_period(EXAMPLE_CONFIG)

    def _settle(self, start, kwh, channel):
        self.store.upsert_kraken_block(start, "electricity_main", kwh,
                                       channel=channel, billing_source="api")

    def test_export_lagging_block_is_unsettled(self):
        s = "2026-07-09T12:00:00"
        self.store.append_block(make_block(s, imp_kwh=4.7, exp_kwh=16.0))
        self._settle(s, 4.7, "import")                    # import settled, export lags
        self.assertEqual(self.store.count_unsettled_blocks(), 1)
        self.assertEqual(self.store.get_oldest_unsettled_block_start(), s)
        self.assertIn(s, [r["block_start"] for r in self.store.get_unsettled_blocks()])

    def test_both_channels_settled_is_settled(self):
        s = "2026-07-09T12:00:00"
        self.store.append_block(make_block(s, imp_kwh=4.7, exp_kwh=16.0))
        self._settle(s, 4.7, "import")
        self._settle(s, 15.9, "export")
        self.assertEqual(self.store.count_unsettled_blocks(), 0)
        self.assertIsNone(self.store.get_oldest_unsettled_block_start())

    def test_no_export_activity_not_chased(self):
        # Import-only / nighttime block (exp_kwh 0): once import settles it's done.
        s = "2026-07-09T02:00:00"
        self.store.append_block(make_block(s, imp_kwh=0.3, exp_kwh=0.0))
        self._settle(s, 0.3, "import")
        self.assertEqual(self.store.count_unsettled_blocks(), 0)
        self.assertIsNone(self.store.get_oldest_unsettled_block_start())

    def test_oldest_reaches_back_past_recent_import_gap(self):
        # The core of #303: an older export-lagging block (import already settled)
        # must set the sweep window start, not the more-recent import gap.
        old, recent = "2026-07-09T12:00:00", "2026-07-18T00:30:00"
        self.store.append_block(make_block(old, imp_kwh=4.7, exp_kwh=16.0))
        self._settle(old, 4.7, "import")                  # import settled, export lags
        self.store.append_block(make_block(recent, imp_kwh=0.5, exp_kwh=0.0))  # import unsettled
        self.assertEqual(self.store.get_oldest_unsettled_block_start(), old)

    def test_export_settlement_drops_it_from_unsettled(self):
        # Self-heal: once the sweep settles the lagging export, it's done.
        s = "2026-07-09T12:00:00"
        self.store.append_block(make_block(s, imp_kwh=4.7, exp_kwh=16.0))
        self._settle(s, 4.7, "import")
        self.assertEqual(self.store.count_unsettled_blocks(), 1)
        self._settle(s, 15.9, "export")
        self.assertEqual(self.store.count_unsettled_blocks(), 0)

    def test_zero_live_export_on_exporting_meter_is_chased(self):
        # blocks-4.db regression: a DCC-export-only meter carries NO live/CAD
        # daytime export, so an un-settled daytime slot has exp_kwh = 0 (or NULL)
        # with exp_kwh_api NULL. The old `exp_kwh > 0` guard skipped it, stranding
        # real solar export (245 slots). It must now be chased because the meter
        # DEMONSTRABLY exports (another slot has settled export > 0) — the fix is
        # value-agnostic on the row, keyed on whether the meter exports at all.
        evening = "2026-07-25T18:00:00"     # export DID settle here (> 0)
        daytime = "2026-07-25T12:00:00"     # import settled, live export 0, DCC not yet in
        self.store.append_block(make_block(evening, imp_kwh=0.1, exp_kwh=0.5))
        self._settle(evening, 0.1, "import")
        self._settle(evening, 0.5, "export")            # meter has real settled export
        self.store.append_block(make_block(daytime, imp_kwh=0.2, exp_kwh=0.0))
        self._settle(daytime, 0.2, "import")            # import settled, export absent
        self.assertEqual(self.store.count_unsettled_blocks(), 1)   # was 0 under the old guard
        self.assertEqual(self.store.get_oldest_unsettled_block_start(), daytime)

    def test_imported_history_not_counted_unsettled(self):
        # 3.5.0: a reconstructed block (imp_kwh set, imp_kwh_api NULL, and it did
        # export) must NOT count as awaiting DCC settlement — DCC never settles
        # pre-EMT history, so it would otherwise be chased forever.
        self.store.upsert_imported_block(
            "2024-07-01T12:00:00", "electricity_main", "import",
            kwh=4.7, rate=0.07, cost=0.329, standing=0.53, source="imported_api")
        self.store.upsert_imported_block(
            "2024-07-01T12:00:00", "electricity_main", "export",
            kwh=16.0, rate=0.15, cost=2.4, source="imported_api")
        self.assertEqual(self.store.count_unsettled_blocks(), 0)
        self.assertIsNone(self.store.get_oldest_unsettled_block_start())
        # A genuine live block is still counted (guard against over-broad filter).
        self.store.append_block(make_block("2026-07-09T12:00:00", imp_kwh=1.0, exp_kwh=0.0))
        self.assertEqual(self.store.count_unsettled_blocks(), 1)


class TestAppendBlock(unittest.TestCase):

    def setUp(self):
        self.store = new_store()
        self.store.insert_config_period(EXAMPLE_CONFIG)

    def tearDown(self):
        self.store.close()

    def test_append_block_inserts_rows(self):
        block = make_block("2026-03-01T00:00:00")
        self.store.append_block(block)
        self.assertEqual(self.store.count_meter_rows(), 1)

    def test_append_block_count(self):
        for i in range(5):
            dt = datetime(2026, 3, 1) + timedelta(minutes=30 * i)
            self.store.append_block(make_block(dt.isoformat()))
        self.assertEqual(self.store.count_blocks(), 5)

    def test_append_block_idempotent(self):
        """INSERT OR IGNORE prevents duplicate meter rows."""
        block = make_block("2026-03-01T00:00:00")
        self.store.append_block(block)
        self.store.append_block(block)
        self.assertEqual(self.store.count_meter_rows(), 1)

    def test_append_block_with_sub_meter(self):
        self.store.insert_config_period(EXAMPLE_CONFIG_WITH_SUB,
                                        effective_from="2026-03-01T00:00:00")
        block = make_block_with_sub("2026-03-01T00:00:00")
        self.store.append_block(block, config_period_id=2)
        self.assertEqual(self.store.count_meter_rows(), 2)

    def test_append_block_no_config_raises(self):
        store2 = new_store()
        with self.assertRaises(RuntimeError):
            store2.append_block(make_block("2026-03-01T00:00:00"))
        store2.close()

    def test_append_block_replace_overwrites_existing(self):
        """append_block_replace must overwrite a zero block, unlike INSERT OR IGNORE."""
        zero_block = make_block("2026-03-01T00:00:00", imp_kwh=0.0, exp_kwh=0.0)
        self.store.append_block(zero_block)
        self.assertEqual(self.store.count_blocks(), 1)

        # Now replace with a block that has real data
        real_block = make_block("2026-03-01T00:00:00", imp_kwh=1.23, exp_kwh=0.45)
        self.store.append_block_replace(real_block)
        self.assertEqual(self.store.count_blocks(), 1, "Should still be 1 block")

        blocks = self.store.get_all_blocks()
        ch = blocks[0]["meters"]["electricity_main"]["channels"]["import"]
        self.assertAlmostEqual(
            ch["kwh"], 1.23, places=3,
            msg="append_block_replace must overwrite zero block with real data"
        )

    def test_append_block_replace_inserts_when_absent(self):
        """append_block_replace also inserts when no block exists at that start."""
        block = make_block("2026-03-01T00:00:00", imp_kwh=0.5)
        self.store.append_block_replace(block)
        self.assertEqual(self.store.count_blocks(), 1)

    def test_append_block_ignore_skips_existing(self):
        """Confirm original append_block still ignores duplicates (regression guard)."""
        first = make_block("2026-03-01T00:00:00", imp_kwh=1.23)
        self.store.append_block(first)
        second = make_block("2026-03-01T00:00:00", imp_kwh=9.99)
        self.store.append_block(second)
        blocks = self.store.get_all_blocks()
        ch = blocks[0]["meters"]["electricity_main"]["channels"]["import"]
        self.assertAlmostEqual(ch["kwh"], 1.23, places=3,
            msg="Original append_block must ignore duplicate, not overwrite")

    def test_get_last_block_before(self):
        """get_last_block_before returns the last finalised block before given start."""
        for i, start in enumerate(["2026-03-01T00:00:00", "2026-03-01T00:30:00",
                                   "2026-03-01T01:00:00"]):
            self.store.append_block(make_block(start, imp_kwh=float(i + 1)))

        result = self.store.get_last_block_before("2026-03-01T01:00:00")
        self.assertIsNotNone(result)
        self.assertEqual(result["start"], "2026-03-01T00:30:00")

    def test_get_last_block_before_none_when_no_earlier_blocks(self):
        """get_last_block_before returns None if all blocks are at or after anchor."""
        self.store.append_block(make_block("2026-03-01T01:00:00"))
        result = self.store.get_last_block_before("2026-03-01T01:00:00")
        self.assertIsNone(result)


        blocks = [
            make_block((datetime(2026, 3, 1) + timedelta(minutes=30 * i)).isoformat())
            for i in range(48)
        ]
        inserted = self.store.append_blocks(blocks)
        self.assertEqual(inserted, 48)
        self.assertEqual(self.store.count_blocks(), 48)

    def test_block_fields_stored_correctly(self):
        block = make_block("2026-03-01T06:00:00", imp_kwh=1.234, exp_kwh=0.567)
        self.store.append_block(block)
        blocks = self.store.get_all_blocks()
        self.assertEqual(len(blocks), 1)
        b = blocks[0]
        self.assertEqual(b["start"], "2026-03-01T06:00:00")
        imp = b["meters"]["electricity_main"]["channels"]["import"]
        self.assertAlmostEqual(imp["kwh"], 1.234, places=4)
        exp = b["meters"]["electricity_main"]["channels"]["export"]
        self.assertAlmostEqual(exp["kwh"], 0.567, places=4)

    def test_standing_charge_stored(self):
        block = make_block("2026-03-01T00:00:00", standing=0.9876)
        self.store.append_block(block)
        blocks = self.store.get_all_blocks()
        sc = blocks[0]["meters"]["electricity_main"]["standing_charge"]
        self.assertAlmostEqual(sc, 0.9876, places=4)

    def test_interpolated_flag_stored(self):
        block = make_block("2026-03-01T00:00:00", interpolated=True)
        self.store.append_block(block)
        blocks = self.store.get_all_blocks()
        self.assertTrue(blocks[0]["interpolated"])

    def test_kwh_remainder_stored(self):
        block = make_block("2026-03-01T00:00:00", imp_kwh=1.0)
        self.store.append_block(block)
        blocks = self.store.get_all_blocks()
        imp = blocks[0]["meters"]["electricity_main"]["channels"]["import"]
        self.assertIn("kwh_remainder", imp)
        self.assertAlmostEqual(imp["kwh_remainder"], 0.95, places=4)

    def test_config_fields_joined(self):
        block = make_block("2026-03-01T00:00:00")
        self.store.append_block(block)
        blocks = self.store.get_all_blocks()
        b = blocks[0]
        self.assertEqual(b["_billing_day"], 1)
        self.assertEqual(b["_timezone"], "Europe/London")
        self.assertEqual(b["_currency_symbol"], "£")


# ─────────────────────────────────────────────────────────────────────────────
# Tests: query methods
# ─────────────────────────────────────────────────────────────────────────────

class TestQueryMethods(unittest.TestCase):

    def setUp(self):
        self.store = new_store()
        self.store.insert_config_period(EXAMPLE_CONFIG)
        # Insert 3 days of blocks: 48 blocks per day
        self.base = datetime(2026, 3, 1)
        for i in range(48 * 3):
            dt = self.base + timedelta(minutes=30 * i)
            self.store.append_block(make_block(dt.isoformat()))

    def tearDown(self):
        self.store.close()

    def test_count_blocks(self):
        self.assertEqual(self.store.count_blocks(), 48 * 3)

    def test_get_all_blocks_count(self):
        blocks = self.store.get_all_blocks()
        self.assertEqual(len(blocks), 48 * 3)

    def test_get_all_blocks_ordered(self):
        blocks = self.store.get_all_blocks()
        starts = [b["start"] for b in blocks]
        self.assertEqual(starts, sorted(starts))

    def test_get_last_block(self):
        last = self.store.get_last_block()
        self.assertIsNotNone(last)
        expected = (self.base + timedelta(minutes=30 * (48 * 3 - 1))).isoformat()
        self.assertEqual(last["start"], expected)

    def test_get_last_block_empty(self):
        store2 = new_store()
        store2.insert_config_period(EXAMPLE_CONFIG)
        self.assertIsNone(store2.get_last_block())
        store2.close()

    def test_get_blocks_for_range(self):
        start = datetime(2026, 3, 1)
        end   = datetime(2026, 3, 1, 23, 59, 59)
        blocks = self.store.get_blocks_for_range(start, end)
        self.assertEqual(len(blocks), 48)

    def test_get_blocks_for_range_partial(self):
        start = datetime(2026, 3, 1, 6, 0, 0)
        end   = datetime(2026, 3, 1, 11, 59, 59)
        blocks = self.store.get_blocks_for_range(start, end)
        self.assertEqual(len(blocks), 12)  # 6 hours * 2 blocks/hour

    def test_get_blocks_for_range_meter_filter(self):
        blocks = self.store.get_blocks_for_range(
            datetime(2026, 3, 1), datetime(2026, 3, 3, 23, 59),
            meter_id="electricity_main"
        )
        self.assertEqual(len(blocks), 48 * 3)

    def test_get_blocks_for_range_meter_filter_no_match(self):
        blocks = self.store.get_blocks_for_range(
            datetime(2026, 3, 1), datetime(2026, 3, 3, 23, 59),
            meter_id="nonexistent"
        )
        self.assertEqual(len(blocks), 0)

    def test_get_blocks_for_date(self):
        s, e = local_date_to_utc_bounds('2026-03-02', 'UTC')
        blocks = self.store.get_blocks_for_utc_range(s, e)
        self.assertEqual(len(blocks), 48)

    def test_get_blocks_for_date_no_match(self):
        s, e = local_date_to_utc_bounds('2026-04-01', 'UTC')
        blocks = self.store.get_blocks_for_utc_range(s, e)
        self.assertEqual(len(blocks), 0)

    def test_get_blocks_for_month(self):
        s, e = local_date_range_to_utc_bounds('2026-03-01', '2026-03-31', 'UTC')
        blocks = self.store.get_blocks_for_utc_range(s, e)
        self.assertEqual(len(blocks), 48 * 3)

    def test_get_blocks_for_month_no_match(self):
        s, e = local_date_range_to_utc_bounds('2025-01-01', '2025-01-31', 'UTC')
        blocks = self.store.get_blocks_for_utc_range(s, e)
        self.assertEqual(len(blocks), 0)

    def test_get_dates_in_utc_range(self):
        dates = self.store.get_dates_in_utc_range(
            "2026-03-01T00:00:00", "2026-03-04T00:00:00", "Europe/London"
        )
        self.assertEqual(len(dates), 3)
        self.assertIn("2026-03-01", dates)
        self.assertIn("2026-03-02", dates)
        self.assertIn("2026-03-03", dates)

    def test_get_dates_in_utc_range_ordered(self):
        dates = self.store.get_dates_in_utc_range(
            "2026-03-01T00:00:00", "2026-03-04T00:00:00", "Europe/London"
        )
        self.assertEqual(dates, sorted(dates))


# ─────────────────────────────────────────────────────────────────────────────
# Tests: block reconstruction fidelity
# ─────────────────────────────────────────────────────────────────────────────

class TestBlockFidelity(unittest.TestCase):
    """Verify round-trip fidelity: block dict in == block dict out."""

    def setUp(self):
        self.store = new_store()
        self.store.insert_config_period(EXAMPLE_CONFIG)

    def tearDown(self):
        self.store.close()

    def test_start_end_preserved(self):
        block = make_block("2026-03-15T12:30:00")
        self.store.append_block(block)
        out = self.store.get_all_blocks()[0]
        self.assertEqual(out["start"], "2026-03-15T12:30:00")
        self.assertEqual(out["end"],   "2026-03-15T13:00:00")

    def test_import_kwh_preserved(self):
        block = make_block("2026-03-01T00:00:00", imp_kwh=3.14159)
        self.store.append_block(block)
        out = self.store.get_all_blocks()[0]
        imp = out["meters"]["electricity_main"]["channels"]["import"]
        self.assertAlmostEqual(imp["kwh"], 3.14159, places=4)

    def test_export_kwh_preserved(self):
        block = make_block("2026-03-01T00:00:00", exp_kwh=2.71828)
        self.store.append_block(block)
        out = self.store.get_all_blocks()[0]
        exp = out["meters"]["electricity_main"]["channels"]["export"]
        self.assertAlmostEqual(exp["kwh"], 2.71828, places=4)

    def test_rate_preserved(self):
        block = make_block("2026-03-01T00:00:00")
        self.store.append_block(block)
        out = self.store.get_all_blocks()[0]
        imp = out["meters"]["electricity_main"]["channels"]["import"]
        self.assertAlmostEqual(imp["rate"], 0.245, places=4)

    def test_read_start_end_preserved(self):
        block = make_block("2026-03-01T00:00:00", imp_kwh=0.5)
        self.store.append_block(block)
        out = self.store.get_all_blocks()[0]
        imp = out["meters"]["electricity_main"]["channels"]["import"]
        self.assertAlmostEqual(imp["read_start"], 1000.0, places=4)
        self.assertAlmostEqual(imp["read_end"],   1000.5, places=4)

    def test_sub_meter_round_trip(self):
        self.store.insert_config_period(EXAMPLE_CONFIG_WITH_SUB,
                                        effective_from="2026-03-01T00:00:00")
        block = make_block_with_sub("2026-03-01T00:00:00")
        self.store.append_block(block, config_period_id=2)
        out = self.store.get_all_blocks()
        # Should have 1 block with 2 meters
        self.assertEqual(len(out), 1)
        meters = out[0]["meters"]
        self.assertIn("electricity_main", meters)
        self.assertIn("zappi_ev", meters)

    def test_sub_meter_kwh_grid(self):
        self.store.insert_config_period(EXAMPLE_CONFIG_WITH_SUB,
                                        effective_from="2026-03-01T00:00:00")
        block = make_block_with_sub("2026-03-01T00:00:00")
        self.store.append_block(block, config_period_id=2)
        out = self.store.get_all_blocks()
        zappi_imp = out[0]["meters"]["zappi_ev"]["channels"]["import"]
        self.assertAlmostEqual(zappi_imp["kwh_grid"], 0.3, places=4)

    def test_supplier_and_v2x_round_trip(self):
        """
        supplier survives via config_periods (historical record per billing period).
        v2x_capable survives via meters table (per-meter property).
        """
        cfg = {"meters": {
            "electricity_main": {"meta": {
                "billing_day": 1, "block_minutes": 30, "timezone": "UTC",
                "currency_symbol": "£", "currency_code": "GBP",
                "supplier": "Octopus Energy",
                "v2x_capable": False,
            }, "channels": {"import": {"read": "s.imp", "rate": "s.rate"}}},
            "ev_charger": {"meta": {
                "sub_meter": True, "parent_meter": "electricity_main",
                "v2x_capable": True,
            }, "channels": {"import": {"read": "s.ev", "rate": "s.rate"}}},
        }}
        store2 = new_store()
        store2.insert_config_period(cfg)
        pid = store2.get_current_config_period_id()
        out = store2.config_from_db(pid)
        store2.close()

        # supplier comes from config_periods — available on main meter meta
        self.assertEqual(out["meters"]["electricity_main"]["meta"].get("supplier"),
                         "Octopus Energy",
                         "supplier must be stored on config_periods and returned in main meter meta")
        # v2x_capable comes from meters table
        self.assertFalse(out["meters"]["electricity_main"]["meta"].get("v2x_capable", False))
        self.assertTrue(out["meters"]["ev_charger"]["meta"].get("v2x_capable"),
                        "v2x_capable must be stored on meters table")

    def test_sub_meter_meta_flags_in_retrieved_blocks(self):
        """
        Blocks retrieved from DB must include sub_meter, parent_meter, device
        in meter.meta — the charts and billing rely on these to identify
        sub-meters. This requires _select_blocks to JOIN the meters table.
        """
        self.store.insert_config_period(EXAMPLE_CONFIG_WITH_SUB,
                                        effective_from="2026-03-01T00:00:00")
        block = make_block_with_sub("2026-03-01T00:00:00")
        self.store.append_block(block, config_period_id=2)
        out = self.store.get_all_blocks()
        meters = out[0]["meters"]

        # Main meter: sub_meter must be absent or False
        main_meta = meters["electricity_main"]["meta"]
        self.assertFalse(main_meta.get("sub_meter", False),
            "Main meter must not be flagged as sub_meter")

        # Sub-meter: must have sub_meter=True, parent_meter, device
        zappi_meta = meters["zappi_ev"]["meta"]
        self.assertTrue(zappi_meta.get("sub_meter"),
            "zappi_ev must be flagged as sub_meter in retrieved block meta")
        self.assertEqual(zappi_meta.get("parent_meter"), "electricity_main",
            "parent_meter must be populated from meters table")
        self.assertEqual(zappi_meta.get("device"), "Zappi EV Charger",
            "device label must be populated from meters table")

    def test_main_meter_no_sub_meter_flag(self):
        """Main meter without sub-meters must not have sub_meter in meta."""
        out = self.store.get_all_blocks()
        if out:
            main_meta = out[0]["meters"]["electricity_main"]["meta"]
            self.assertFalse(main_meta.get("sub_meter", False))


# ─────────────────────────────────────────────────────────────────────────────
# Tests: local date calculation
# ─────────────────────────────────────────────────────────────────────────────

class TestLocalDate(unittest.TestCase):
    """Verify UTC timestamps are converted to the correct local date."""

    def setUp(self):
        self.store = new_store()
        self.store.insert_config_period(EXAMPLE_CONFIG)

    def tearDown(self):
        self.store.close()

    def test_midnight_utc_is_correct_london_date(self):
        # 2026-03-15 00:00:00 UTC = 2026-03-15 00:00:00 GMT (no DST yet)
        block = make_block("2026-03-15T00:00:00")
        self.store.append_block(block)
        dates = self.store.get_dates_in_utc_range("2026-03-15T00:00:00", "2026-03-16T00:00:00", "Europe/London")
        self.assertIn("2026-03-15", dates)

    def test_late_utc_before_dst_is_same_date(self):
        # 2026-03-14 23:30:00 UTC = 2026-03-14 23:30:00 GMT
        block = make_block("2026-03-14T23:30:00")
        self.store.append_block(block)
        dates = self.store.get_dates_in_utc_range("2026-03-14T00:00:00", "2026-03-15T00:00:00", "Europe/London")
        self.assertIn("2026-03-14", dates)

    def test_blocks_for_date_uses_local_not_utc(self):
        # 2026-03-29 00:30 UTC = 01:30 BST (BST starts at 01:00 UTC on 29th)
        # So local London date is 2026-03-29
        # UTC bounds for Mar 29 BST: 2026-03-29T00:00:00 → 2026-03-29T23:00:00
        block = make_block("2026-03-29T00:30:00")
        self.store.append_block(block)
        s29, e29 = local_date_to_utc_bounds("2026-03-29", "Europe/London")
        s28, e28 = local_date_to_utc_bounds("2026-03-28", "Europe/London")
        blocks_29 = self.store.get_blocks_for_utc_range(s29, e29)
        blocks_28 = self.store.get_blocks_for_utc_range(s28, e28)
        self.assertEqual(len(blocks_29), 1)
        self.assertEqual(len(blocks_28), 0)


# ─────────────────────────────────────────────────────────────────────────────
# Tests: reads (Phase 2+ table, Phase 1 just verifies table exists and works)
# ─────────────────────────────────────────────────────────────────────────────

class TestReads(unittest.TestCase):

    def setUp(self):
        self.store = new_store()
        self.store.insert_config_period(EXAMPLE_CONFIG)

    def tearDown(self):
        self.store.close()

    def test_insert_read(self):
        self.store.insert_read(
            meter_id="electricity_main",
            channel="import",
            captured_at="2026-03-01T06:00:00",
            reading_kwh=1000.5,
            rate=0.245,
        )
        reads = self.store.get_reads_for_range(
            datetime(2026, 3, 1), datetime(2026, 3, 1, 23, 59)
        )
        self.assertEqual(len(reads), 1)
        self.assertAlmostEqual(reads[0]["reading_kwh"], 1000.5, places=4)

    def test_read_block_id_initially_null(self):
        self.store.insert_read(
            meter_id="electricity_main",
            channel="import",
            captured_at="2026-03-01T06:00:00",
            reading_kwh=1000.5,
        )
        reads = self.store.get_reads_for_range(
            datetime(2026, 3, 1), datetime(2026, 3, 2)
        )
        self.assertIsNone(reads[0]["block_id"])

    def test_link_reads_to_block(self):
        # Insert a block and some reads
        self.store.append_block(make_block("2026-03-01T06:00:00"))
        for i in range(6):
            ts = (datetime(2026, 3, 1, 6, 0) + timedelta(minutes=5 * i)).isoformat()
            self.store.insert_read("electricity_main", "import", ts, 1000.0 + i * 0.1)

        # Get the block's DB id
        cur = self.store._conn.execute(
            "SELECT id FROM blocks WHERE block_start = '2026-03-01T06:00:00'"
        )
        block_db_id = cur.fetchone()["id"]

        linked = self.store.link_reads_to_block(
            block_start="2026-03-01T06:00:00",
            block_end="2026-03-01T06:30:00",
            block_id=block_db_id,
        )
        self.assertEqual(linked, 6)

        reads = self.store.get_reads_for_block(block_db_id)
        self.assertEqual(len(reads), 6)
        for r in reads:
            self.assertEqual(r["block_id"], block_db_id)

    def test_get_reads_for_range_meter_filter(self):
        self.store.insert_read("electricity_main", "import",
                               "2026-03-01T06:00:00", 1000.0)
        self.store.insert_read("zappi_ev", "import",
                               "2026-03-01T06:00:00", 200.0)
        reads = self.store.get_reads_for_range(
            datetime(2026, 3, 1), datetime(2026, 3, 2),
            meter_id="electricity_main"
        )
        self.assertEqual(len(reads), 1)
        self.assertEqual(reads[0]["meter_id"], "electricity_main")

    def test_purge_reads_older_than(self):
        from datetime import timezone
        # Insert an old read
        old_ts = "2020-01-01T00:00:00"
        self.store.insert_read("electricity_main", "import", old_ts, 1000.0)
        # Insert a recent read
        recent_ts = datetime.now(timezone.utc).replace(tzinfo=None).isoformat()
        self.store.insert_read("electricity_main", "import", recent_ts, 1001.0)

        deleted = self.store.purge_reads_older_than(days=30)
        self.assertEqual(deleted, 1)

        reads = self.store.get_reads_for_range(
            datetime(2019, 1, 1), datetime(2021, 1, 1)
        )
        self.assertEqual(len(reads), 0)


# ─────────────────────────────────────────────────────────────────────────────
# Tests: migration
# ─────────────────────────────────────────────────────────────────────────────

# TestMigration removed in 4.0.0 — the JSON->SQLite migration shim it exercised
# (migrate_json_to_sqlite) has been retired.


# ─────────────────────────────────────────────────────────────────────────────
# Tests: backup
# ─────────────────────────────────────────────────────────────────────────────

class TestBackup(unittest.TestCase):

    def setUp(self):
        import tempfile
        self.tmp_db  = tempfile.mktemp(suffix=".db")
        self.tmp_bak = tempfile.mktemp(suffix=".db")
        self.store = open_block_store(self.tmp_db)
        self.store.insert_config_period(EXAMPLE_CONFIG)
        for i in range(5):
            dt = datetime(2026, 3, 1) + timedelta(minutes=30 * i)
            self.store.append_block(make_block(dt.isoformat()))

    def tearDown(self):
        self.store.close()
        for p in (self.tmp_db, self.tmp_bak):
            try:
                os.remove(p)
            except Exception:
                pass

    def test_backup_creates_file(self):
        self.store.backup(self.tmp_bak)
        self.assertTrue(os.path.exists(self.tmp_bak))

    def test_backup_has_same_block_count(self):
        self.store.backup(self.tmp_bak)
        bak = open_block_store(self.tmp_bak)
        self.assertEqual(bak.count_blocks(), 5)
        bak.close()

    def test_backup_has_config_periods(self):
        self.store.backup(self.tmp_bak)
        bak = open_block_store(self.tmp_bak)
        self.assertIsNotNone(bak.get_current_config_period_id())
        bak.close()


# ─────────────────────────────────────────────────────────────────────────────
# Runner
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    loader = unittest.TestLoader()
    suite  = unittest.TestSuite()
    for cls in [
        TestSchema,
        TestConfigPeriods,
        TestAppendBlock,
        TestQueryMethods,
        TestBlockFidelity,
        TestLocalDate,
        TestReads,
        TestMigration,
        TestBackup,
    ]:
        suite.addTests(loader.loadTestsFromTestCase(cls))

    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    sys.exit(0 if result.wasSuccessful() else 1)


class TestBillingTotalsVsBlockMethod(unittest.TestCase):
    """Verify get_billing_totals_for_range matches calculate_billing_summary_for_period."""

    def setUp(self):
        import sys, types
        eio = types.ModuleType("energy_engine_io")
        eio.load_json = lambda *a, **kw: {}
        sys.modules.setdefault("energy_engine_io", eio)
        import energy_charts as ec
        self.ec = ec

        self.store = BlockStore(":memory:")
        self.store.insert_config_period({"meters": {"electricity_main": {"meta": {
            "billing_day": 3, "block_minutes": 30, "timezone": "Europe/London",
            "currency_symbol": "£", "currency_code": "GBP", "site": "Home",
        }}}})
        self.cp_id = self.store._conn.execute(
            "SELECT id FROM config_periods LIMIT 1"
        ).fetchone()["id"]

    def _insert_block(self, block_start_iso, imp_kwh, imp_cost, exp_kwh, exp_cost, standing):
        self.store._conn.execute("""
            INSERT INTO blocks
            (block_start, block_end,
             meter_id, config_period_id, interpolated,
             imp_kwh, imp_rate, imp_cost,
             exp_kwh, exp_rate, exp_cost,
             standing_charge)
            VALUES (?,?,?,?,0, ?,NULL,?, ?,NULL,?, ?)
        """, (
            block_start_iso, block_start_iso,
            "electricity_main", self.cp_id,
            imp_kwh, imp_cost, exp_kwh, exp_cost, standing
        ))
        self.store._conn.commit()

    def test_totals_match_block_method(self):
        """SQL aggregation should match calculate_billing_summary_for_period."""
        from datetime import datetime
        # Insert 3 days of blocks (2 blocks per day, UTC times)
        # Jan 1: 00:00 UTC and 00:30 UTC (= Jan 1 BST since UTC=BST in Jan)
        blocks_data = [
            # (block_start UTC, imp_kwh, imp_cost, exp_kwh, exp_cost, standing)
            ("2026-03-01T00:00:00", 1.0, 0.245, 0.0, 0.0, 0.50),
            ("2026-03-01T00:30:00", 1.2, 0.294, 0.0, 0.0, 0.50),
            ("2026-03-02T00:00:00", 0.8, 0.196, 0.2, 0.03, 0.50),
            ("2026-03-02T00:30:00", 0.9, 0.220, 0.0, 0.0,  0.50),
            ("2026-03-03T00:00:00", 1.1, 0.270, 0.0, 0.0,  0.50),
            ("2026-03-03T00:30:00", 0.7, 0.172, 0.3, 0.045, 0.50),
        ]
        for bd in blocks_data:
            self._insert_block(*bd)

        # SQL method — UTC range covering Mar 1-3 GMT
        utc_s, utc_e = local_date_range_to_utc_bounds("2026-03-01", "2026-03-03", "Europe/London")
        sql_t = self.store.get_billing_totals_for_utc_range(utc_s, utc_e, "Europe/London")

        # Expected from manual calculation
        self.assertAlmostEqual(sql_t["imp_kwh"],  1.0+1.2+0.8+0.9+1.1+0.7, places=3)
        self.assertAlmostEqual(sql_t["imp_cost"], 0.245+0.294+0.196+0.220+0.270+0.172, places=3)
        self.assertAlmostEqual(sql_t["exp_kwh"],  0.0+0.0+0.2+0.0+0.0+0.3, places=3)
        self.assertAlmostEqual(sql_t["exp_cost"], 0.0+0.0+0.03+0.0+0.0+0.045, places=3)
        # Standing charge: 0.50 per day × 3 days = 1.50 (NOT 6 × 0.50 = 3.00)
        self.assertAlmostEqual(sql_t["standing"], 1.50, places=3,
                               msg="Standing charge should be summed once per local day")

    def test_standing_charge_decrease_uses_start_of_day(self):
        """If the standing charge DECREASES mid-day, billing must use the
        start-of-day value, not MAX (which would over-bill the old higher one)."""
        # Day with an early block at 0.60 then a later block at 0.40 (a decrease).
        self._insert_block("2026-03-01T00:00:00", 1.0, 0.245, 0.0, 0.0, 0.60)
        self._insert_block("2026-03-01T12:00:00", 1.0, 0.245, 0.0, 0.0, 0.40)
        utc_s, utc_e = local_date_range_to_utc_bounds("2026-03-01", "2026-03-01", "Europe/London")
        t = self.store.get_billing_totals_for_utc_range(utc_s, utc_e, "Europe/London")
        # Start-of-day = 0.60 (the earliest block). MAX would also give 0.60 here,
        # so make the discriminating case explicit below.
        self.assertAlmostEqual(t["standing"], 0.60, places=3)

    def test_standing_charge_increase_uses_start_of_day_not_max(self):
        """The discriminating case: charge INCREASES mid-day. MAX would pick the
        later higher value; start-of-day must pick the earlier lower one."""
        self._insert_block("2026-03-01T00:00:00", 1.0, 0.245, 0.0, 0.0, 0.40)
        self._insert_block("2026-03-01T12:00:00", 1.0, 0.245, 0.0, 0.0, 0.60)
        utc_s, utc_e = local_date_range_to_utc_bounds("2026-03-01", "2026-03-01", "Europe/London")
        t = self.store.get_billing_totals_for_utc_range(utc_s, utc_e, "Europe/London")
        # Start-of-day = 0.40 (earliest). Old MAX logic would have given 0.60.
        self.assertAlmostEqual(t["standing"], 0.40, places=3,
                               msg="Must use start-of-day (0.40), not MAX (0.60)")

    def test_standing_charge_leading_zero_not_shadowing(self):
        """A leading zero (e.g. early gap-filled block) must NOT shadow the real
        charge — first NON-zero of the day wins."""
        self._insert_block("2026-03-01T00:00:00", 0.0, 0.0, 0.0, 0.0, 0.0)   # gap-fill
        self._insert_block("2026-03-01T01:00:00", 1.0, 0.245, 0.0, 0.0, 0.50)
        utc_s, utc_e = local_date_range_to_utc_bounds("2026-03-01", "2026-03-01", "Europe/London")
        t = self.store.get_billing_totals_for_utc_range(utc_s, utc_e, "Europe/London")
        self.assertAlmostEqual(t["standing"], 0.50, places=3,
                               msg="Leading zero must not shadow the real charge")

    def test_standing_charge_bst_boundary(self):
        """Block at 23:00 UTC = 00:00 BST next day should count for the BST day."""
        from datetime import datetime
        # After BST starts (after March 29 2026 01:00 UTC), UTC+1 applies.
        # Block at 2026-04-01T23:00:00 UTC = 2026-04-02 00:00:00 BST (next local day)
        # Block at 2026-04-02T00:00:00 UTC = 2026-04-02 01:00:00 BST (same local day)
        # Both are on local date 2026-04-02 → standing should be counted once
        self._insert_block("2026-04-01T23:00:00", 1.0, 0.245, 0.0, 0.0, 0.60)
        self._insert_block("2026-04-02T00:00:00", 0.5, 0.122, 0.0, 0.0, 0.60)

        # Use UTC bounds for BST Apr 2 — correctly includes 23:00 UTC Apr 1 block
        utc_s, utc_e = local_date_range_to_utc_bounds("2026-04-02", "2026-04-02", "Europe/London")
        sql_t = self.store.get_billing_totals_for_utc_range(utc_s, utc_e, "Europe/London")
        self.assertAlmostEqual(sql_t["standing"], 0.60, places=3,
                               msg="Blocks crossing UTC midnight but same BST day should count once")
        self.assertAlmostEqual(sql_t["imp_kwh"], 1.5, places=3,
                               msg="Both BST blocks should be included in UTC range for Apr 2 BST")

    def test_bst_block_included_in_utc_range(self):
        """get_blocks_for_utc_range includes 23:xx UTC blocks for the next local day when using UTC bounds."""
        self._insert_block("2026-04-01T23:00:00", 2.0, 0.490, 0.0, 0.0, 0.50)  # BST Apr 2
        self._insert_block("2026-04-02T00:00:00", 1.0, 0.245, 0.0, 0.0, 0.50)  # BST Apr 2

        # UTC bounds for BST Apr 2 = 2026-04-01T23:00:00 → 2026-04-02T23:00:00
        utc_s, utc_e = local_date_range_to_utc_bounds("2026-04-02", "2026-04-02", "Europe/London")
        blocks = self.store.get_blocks_for_utc_range(utc_s, utc_e)
        self.assertEqual(len(blocks), 2,
                         "get_blocks_for_utc_range should include 23:00 UTC block for BST Apr 2")

        # block_start range Apr 2 00:00 → would miss the 23:00 UTC block
        from datetime import datetime
        blocks_utc = self.store.get_blocks_for_range(datetime(2026,4,2,0,0,0), datetime(2026,4,2,23,59,59))
        self.assertEqual(len(blocks_utc), 1,
                         "get_blocks_for_range misses the 23:00 UTC block (known limitation)")


class TestMixedSourceBilling(unittest.TestCase):
    """A billing period that spans a data-source-mode change (Change Setup, or
    DCC settlement catching up) contains a MIX of block origins:
      - cad blocks  (source='ha_sensor'): kWh in imp_kwh, grid/remainder NULL
      - DCC blocks  (source='kraken_api', settled): authoritative kWh in
        imp_kwh_grid, with a DIFFERENT provisional value left in imp_kwh
      - Mini blocks (source='kraken_mini', provisional): kWh in imp_kwh only

    The billing aggregation is source-agnostic — it must pick the right kWh per
    block via COALESCE(imp_kwh_remainder, imp_kwh_grid, imp_kwh) and sum the
    materialised imp_cost, with no double-count, no omission, and standing charge
    once per local day across the mode boundary. This pins that behaviour (the
    mixed case was never previously exercised — the reconciliation harness leaves
    imp_kwh_grid/imp_kwh_remainder NULL on every block).
    """

    def setUp(self):
        self.store = BlockStore(":memory:")
        self.store.insert_config_period({"meters": {"electricity_main": {"meta": {
            "billing_day": 1, "block_minutes": 30, "timezone": "Europe/London",
            "currency_symbol": "£", "currency_code": "GBP", "site": "Home",
        }}}})
        self.cp = self.store._conn.execute(
            "SELECT id FROM config_periods LIMIT 1").fetchone()["id"]

    def _insert(self, start, *, imp_kwh=None, imp_kwh_grid=None,
                imp_kwh_remainder=None, imp_cost=0.0, exp_kwh=0.0, exp_cost=0.0,
                standing=0.50, source=None, is_provisional=0, imp_kwh_api=None):
        from datetime import datetime, timedelta
        end = (datetime.fromisoformat(start) + timedelta(minutes=30)).isoformat()
        self.store._conn.execute(
            """INSERT INTO blocks
               (block_start, block_end, meter_id, config_period_id, interpolated,
                imp_kwh, imp_kwh_grid, imp_kwh_remainder, imp_rate, imp_cost,
                exp_kwh, exp_rate, exp_cost, standing_charge,
                source, is_provisional, imp_kwh_api)
               VALUES (?,?,?,?,0, ?,?,?,NULL,?, ?,NULL,?, ?, ?,?,?)""",
            (start, end, "electricity_main", self.cp,
             imp_kwh, imp_kwh_grid, imp_kwh_remainder, imp_cost,
             exp_kwh, exp_cost, standing, source, is_provisional, imp_kwh_api))
        self.store._conn.commit()

    def test_mixed_cad_api_mini_period_bills_correctly(self):
        # A period spanning a mode change contains blocks of different ORIGIN, but
        # data-source mode is invisible to billing: on a sub-less meter every
        # origin stores its billable kWh in imp_kwh (DCC settlement writes the
        # authoritative value into imp_kwh/kwh_total — engine.py reconstruct), so
        # the aggregation simply sums imp_kwh across origins.
        # Day 1 — cad (ha_sensor): kWh from local reads.
        self._insert("2026-03-04T00:00:00", imp_kwh=1.0, imp_cost=0.30,
                     source="ha_sensor")
        self._insert("2026-03-04T00:30:00", imp_kwh=1.5, imp_cost=0.45,
                     source="ha_sensor")
        # Day 2 — DCC-settled (kraken_api): authoritative kWh normalised into
        # imp_kwh by settlement (grid/remainder NULL on a sub-less meter).
        self._insert("2026-03-05T00:00:00", imp_kwh=2.0, imp_cost=0.60,
                     source="kraken_api", imp_kwh_api=2.0)
        self._insert("2026-03-05T00:30:00", imp_kwh=2.0, imp_cost=0.60,
                     source="kraken_api", imp_kwh_api=2.0)
        # Day 2 — recent Mini provisional block (kraken_mini, not yet settled).
        self._insert("2026-03-05T01:00:00", imp_kwh=0.5, imp_cost=0.16,
                     source="kraken_mini", is_provisional=1)

        utc_s, utc_e = local_date_range_to_utc_bounds(
            "2026-03-04", "2026-03-05", "Europe/London")
        t = self.store.get_billing_totals_for_utc_range(utc_s, utc_e, "Europe/London")

        self.assertAlmostEqual(t["imp_kwh"], 1.0+1.5+2.0+2.0+0.5, places=4,
            msg="Mixed-origin period must sum every block's imp_kwh (7.0) — "
                "data-source mode is invisible to billing")
        self.assertAlmostEqual(t["imp_cost"], 0.30+0.45+0.60+0.60+0.16, places=4)
        self.assertAlmostEqual(t["standing"], 1.00, places=4,
            msg="Standing charge once per local day across the mode boundary")

    def test_source_column_does_not_affect_billing_total(self):
        # Same kWh/cost, three different source tags → identical billing total.
        for src in ("ha_sensor", "kraken_api", "kraken_mini"):
            st = BlockStore(":memory:")
            st.insert_config_period({"meters": {"electricity_main": {"meta": {
                "billing_day": 1, "block_minutes": 30, "timezone": "Europe/London",
                "currency_symbol": "£", "currency_code": "GBP", "site": "Home"}}}})
            cp = st._conn.execute("SELECT id FROM config_periods LIMIT 1").fetchone()["id"]
            st._conn.execute(
                """INSERT INTO blocks (block_start, block_end, meter_id,
                   config_period_id, interpolated, imp_kwh, imp_cost,
                   standing_charge, source)
                   VALUES (?,?,?,?,0,?,?,?,?)""",
                ("2026-03-04T00:00:00", "2026-03-04T00:30:00", "electricity_main",
                 cp, 2.0, 0.60, 0.50, src))
            st._conn.commit()
            utc_s, utc_e = local_date_range_to_utc_bounds(
                "2026-03-04", "2026-03-04", "Europe/London")
            t = st.get_billing_totals_for_utc_range(utc_s, utc_e, "Europe/London")
            self.assertAlmostEqual(t["imp_kwh"], 2.0, places=4,
                msg=f"source={src} must bill identically (mode-invisible)")
            self.assertAlmostEqual(t["imp_cost"], 0.60, places=4)

    def test_billing_kwh_coalesce_priority(self):
        # Contract of the aggregation's kWh selection: remainder > grid > raw.
        # (On the main meter PASS 2 sets remainder+grid together for sub-meter
        # periods; this pins the priority so a future change can't silently flip
        # which column bills.)
        self._insert("2026-03-04T00:00:00", imp_kwh=3.0, imp_kwh_grid=2.5,
                     imp_kwh_remainder=2.0, imp_cost=0.55, source="kraken_api")
        utc_s, utc_e = local_date_range_to_utc_bounds(
            "2026-03-04", "2026-03-04", "Europe/London")
        t = self.store.get_billing_totals_for_utc_range(utc_s, utc_e, "Europe/London")
        self.assertAlmostEqual(t["imp_kwh"], 2.0, places=4,
            msg="remainder (2.0) must win over grid (2.5) and raw (3.0)")


if __name__ == "__main__":
    unittest.main()


class TestCurrentBlock(unittest.TestCase):
    """Tests for save_current_block / load_current_block / clear_current_block."""

    def setUp(self):
        import sys, types
        eio = types.ModuleType("energy_engine_io"); eio.load_json = lambda *a,**kw: {}
        sys.modules.setdefault("energy_engine_io", eio)
        self.store = BlockStore(":memory:")

    def _make_block(self, start="2026-04-05T00:00:00", end="2026-04-05T00:30:00"):
        return {
            "start": start, "end": end,
            "interpolated": False,
            "_last_checkpoint": "2026-04-05T00:10:00",
            "meters": {
                "electricity_main": {
                    "meta": {},
                    "standing_charge": 0.5046,
                    "channels": {
                        "import": {
                            "reads": [
                                {"ts": "2026-04-05T00:00:00", "value": 28000.0},
                                {"ts": "2026-04-05T00:10:00", "value": 28000.5},
                            ],
                            "rates": [
                                {"ts": "2026-04-05T00:00:00", "value": 0.245},
                            ],
                        },
                        "export": {
                            "reads": [{"ts": "2026-04-05T00:00:00", "value": 10000.0}],
                            "rates": [{"ts": "2026-04-05T00:00:00", "value": 0.0}],
                        },
                    },
                }
            },
        }

    def test_save_and_load_roundtrip(self):
        block = self._make_block()
        self.store.save_current_block(block)
        loaded = self.store.load_current_block()

        self.assertEqual(loaded["start"], block["start"])
        self.assertEqual(loaded["end"], block["end"])
        self.assertEqual(loaded["_last_checkpoint"], block["_last_checkpoint"])
        self.assertFalse(loaded["interpolated"])

    def test_reads_roundtrip(self):
        block = self._make_block()
        self.store.save_current_block(block)
        loaded = self.store.load_current_block()

        imp_reads = loaded["meters"]["electricity_main"]["channels"]["import"]["reads"]
        self.assertEqual(len(imp_reads), 2)
        self.assertAlmostEqual(imp_reads[0]["value"], 28000.0, places=3)
        self.assertAlmostEqual(imp_reads[1]["value"], 28000.5, places=3)

    def test_rates_roundtrip(self):
        block = self._make_block()
        self.store.save_current_block(block)
        loaded = self.store.load_current_block()

        imp_rates = loaded["meters"]["electricity_main"]["channels"]["import"]["rates"]
        self.assertEqual(len(imp_rates), 1)
        self.assertAlmostEqual(imp_rates[0]["value"], 0.245, places=4)

    def test_standing_charge_roundtrip(self):
        block = self._make_block()
        self.store.save_current_block(block)
        loaded = self.store.load_current_block()

        sc = loaded["meters"]["electricity_main"]["standing_charge"]
        self.assertAlmostEqual(sc, 0.5046, places=4)

    def test_gap_marker_roundtrip(self):
        """Gap marker stored as gap_detected_at + is_gap_seed rows, not a JSON blob."""
        block = self._make_block()
        block["_gap_marker"] = {
            "detected_at": "2026-04-05T00:05:00",
            "pre_reads": {
                "electricity_main": {"import": {"ts": "2026-04-04T23:55:00", "value": 27999.9}}
            },
            "last_known_rates": {
                "electricity_main": {"import": {"ts": "2026-04-04T23:55:00", "value": 0.245}}
            },
        }
        self.store.save_current_block(block)

        # Verify storage is relational — gap_detected_at column, not a blob
        row = self.store._conn.execute(
            "SELECT gap_detected_at FROM current_block WHERE id=1"
        ).fetchone()
        self.assertEqual(row["gap_detected_at"], "2026-04-05T00:05:00",
                         "gap_detected_at must be stored as a column, not a JSON blob")
        # Verify gap_marker blob column no longer exists — schema is fully relational
        cols = [r[1] for r in self.store._conn.execute(
            "PRAGMA table_info(current_block)"
        ).fetchall()]
        self.assertNotIn("gap_marker", cols,
            "gap_marker blob must not exist — gap state stored as gap_detected_at + is_gap_seed rows")

        # Verify gap seed rows exist
        seed_rows = self.store._conn.execute(
            "SELECT * FROM current_reads WHERE is_gap_seed > 0"
        ).fetchall()
        self.assertGreater(len(seed_rows), 0, "Gap seed rows must be stored in current_reads")

        # Verify full roundtrip
        loaded = self.store.load_current_block()
        self.assertIn("_gap_marker", loaded)
        self.assertEqual(loaded["_gap_marker"]["detected_at"], "2026-04-05T00:05:00")
        pre = loaded["_gap_marker"]["pre_reads"]
        self.assertAlmostEqual(
            pre["electricity_main"]["import"]["value"], 27999.9, places=3
        )

    def test_gap_marker_last_block_start_roundtrip(self):
        """gap_last_block_start is persisted and restored correctly."""
        block = self._make_block()
        block["_gap_marker"] = {
            "detected_at": "2026-04-05T00:05:00",
            "pre_reads": {
                "electricity_main": {"import": {"ts": "2026-04-04T23:55:00", "value": 27999.9}}
            },
            "last_known_rates": {
                "electricity_main": {"import": {"ts": "2026-04-04T23:55:00", "value": 0.245}}
            },
            "last_block_start": "2026-04-04T23:50:00",
        }
        self.store.save_current_block(block)

        # Verify stored in column
        row = self.store._conn.execute(
            "SELECT gap_last_block_start FROM current_block WHERE id=1"
        ).fetchone()
        self.assertEqual(row["gap_last_block_start"], "2026-04-04T23:50:00")

        # Verify full roundtrip
        loaded = self.store.load_current_block()
        self.assertEqual(
            loaded["_gap_marker"]["last_block_start"], "2026-04-04T23:50:00"
        )

    def test_gap_marker_last_block_start_none_when_absent(self):
        """If last_block_start not set, roundtrip returns None not KeyError."""
        block = self._make_block()
        block["_gap_marker"] = {
            "detected_at": "2026-04-05T00:05:00",
            "pre_reads": {
                "electricity_main": {"import": {"ts": "2026-04-04T23:55:00", "value": 27999.9}}
            },
            "last_known_rates": {},
        }
        self.store.save_current_block(block)
        loaded = self.store.load_current_block()
        self.assertIsNone(loaded["_gap_marker"]["last_block_start"])

    def test_no_gap_marker_absent(self):
        block = self._make_block()
        self.store.save_current_block(block)
        loaded = self.store.load_current_block()
        self.assertNotIn("_gap_marker", loaded)
        # Verify gap_detected_at is NULL
        row = self.store._conn.execute(
            "SELECT gap_detected_at FROM current_block WHERE id=1"
        ).fetchone()
        self.assertIsNone(row["gap_detected_at"])

    def test_save_overwrites_previous(self):
        block1 = self._make_block(start="2026-04-05T00:00:00")
        block2 = self._make_block(start="2026-04-05T00:30:00", end="2026-04-05T01:00:00")
        self.store.save_current_block(block1)
        self.store.save_current_block(block2)
        loaded = self.store.load_current_block()
        self.assertEqual(loaded["start"], "2026-04-05T00:30:00")

    def test_save_replaces_reads(self):
        """Each save replaces all reads — no accumulation across saves."""
        block1 = self._make_block()
        self.store.save_current_block(block1)
        block2 = self._make_block()
        block2["meters"]["electricity_main"]["channels"]["import"]["reads"] = [
            {"ts": "2026-04-05T00:25:00", "value": 28001.0}
        ]
        self.store.save_current_block(block2)
        loaded = self.store.load_current_block()
        imp_reads = loaded["meters"]["electricity_main"]["channels"]["import"]["reads"]
        self.assertEqual(len(imp_reads), 1)
        self.assertAlmostEqual(imp_reads[0]["value"], 28001.0, places=3)

    def test_load_empty_returns_empty_dict(self):
        loaded = self.store.load_current_block()
        self.assertEqual(loaded, {})

    def test_clear_removes_state(self):
        self.store.save_current_block(self._make_block())
        self.store.clear_current_block()
        loaded = self.store.load_current_block()
        self.assertEqual(loaded, {})

    def test_get_cumulative_totals_empty(self):
        totals = self.store.get_cumulative_totals()
        self.assertEqual(totals["import_kwh"], 0.0)
        self.assertEqual(totals["export_kwh"], 0.0)

    def test_get_cumulative_totals_no_sub_meters(self):
        """Without sub-meters, totals equal direct SUM of main meter blocks."""
        self.store.insert_config_period({"meters": {"electricity_main": {"meta": {
            "billing_day": 1, "block_minutes": 30, "timezone": "UTC",
            "currency_symbol": "£", "currency_code": "GBP",
        }}}})
        cp_id = self.store._conn.execute(
            "SELECT id FROM config_periods LIMIT 1"
        ).fetchone()["id"]
        self.store._conn.execute("""
            INSERT INTO blocks (block_start, block_end, meter_id, config_period_id, interpolated,
            imp_kwh, imp_cost, exp_kwh, exp_cost, standing_charge)
            VALUES ('2026-01-01T00:00:00','2026-01-01T00:30:00','electricity_main',?,0, 1.5,0.368, 0.3,0.024, 0.5)
        """, (cp_id,))
        self.store._conn.execute("""
            INSERT INTO blocks (block_start, block_end, meter_id, config_period_id, interpolated,
            imp_kwh, imp_cost, exp_kwh, exp_cost, standing_charge)
            VALUES ('2026-01-01T00:30:00','2026-01-01T01:00:00','electricity_main',?,0, 2.0,0.490, 0.0,0.0, 0.5)
        """, (cp_id,))
        self.store._conn.commit()

        totals = self.store.get_cumulative_totals()
        self.assertAlmostEqual(totals["import_kwh"],  3.5,   places=4)
        self.assertAlmostEqual(totals["import_cost"], 0.858, places=4)
        self.assertAlmostEqual(totals["export_kwh"],  0.3,   places=4)
        self.assertAlmostEqual(totals["export_cost"], 0.024, places=4)

    def test_billing_totals_no_double_counting(self):
        """
        get_billing_totals_for_utc_range must not double-count sub-meters.
        electricity_main.imp_kwh already includes sub-meter consumption.
        """
        cfg = {"meters": {
            "electricity_main": {"meta": {
                "billing_day": 1, "block_minutes": 30, "timezone": "UTC",
                "currency_symbol": "£", "currency_code": "GBP",
            }, "channels": {
                "import": {"read": "sensor.main", "rate": "sensor.rate"},
                "export": {"read": "sensor.exp",  "rate": "sensor.exprate"},
            }},
            "ev_charger": {"meta": {
                "sub_meter": True, "parent_meter": "electricity_main",
            }, "channels": {
                "import": {"read": "sensor.ev", "rate": "sensor.rate"},
            }},
        }}
        self.store.insert_config_period(cfg)
        cp_id = self.store._conn.execute(
            "SELECT id FROM config_periods LIMIT 1"
        ).fetchone()["id"]

        # main: 3.0 kWh total, remainder=1.0 (house), cost=0.735
        # ev:   2.0 kWh, all from grid, no independent cost
        self.store._conn.execute("""
            INSERT INTO blocks (block_start, block_end, meter_id, config_period_id, interpolated,
            imp_kwh, imp_kwh_remainder, imp_cost, exp_kwh, exp_cost, standing_charge)
            VALUES ('2026-01-01T00:00:00','2026-01-01T00:30:00','electricity_main',?,0, 3.0,1.0,0.735, 0.2,0.024, 0.5)
        """, (cp_id,))
        self.store._conn.execute("""
            INSERT INTO blocks (block_start, block_end, meter_id, config_period_id, interpolated,
            imp_kwh, imp_kwh_grid, imp_cost, exp_kwh, exp_cost, standing_charge)
            VALUES ('2026-01-01T00:00:00','2026-01-01T00:30:00','ev_charger',?,0, 2.0,2.0,0.0, 0.0,0.0, 0.0)
        """, (cp_id,))
        self.store._conn.commit()

        t = self.store.get_billing_totals_for_utc_range(*local_date_range_to_utc_bounds('2026-01-01', '2026-01-01', 'UTC'))

        # imp_kwh: remainder(1.0) + ev_grid(2.0) = 3.0, NOT 3.0+2.0=5.0
        self.assertAlmostEqual(t["imp_kwh"], 3.0, places=3,
            msg="Billing totals must not double-count sub-meter imp_kwh")
        self.assertNotAlmostEqual(t["imp_kwh"], 5.0, places=1,
            msg="5.0 kWh indicates double-counting bug")
        # cost from main meter only
        self.assertAlmostEqual(t["imp_cost"], 0.735, places=3)
        # export from main meter only
        self.assertAlmostEqual(t["exp_kwh"], 0.2, places=3)
        self.assertAlmostEqual(t["exp_cost"], 0.024, places=3)
        # standing from main meter only
        self.assertAlmostEqual(t["standing"], 0.5, places=3)

    def test_get_cumulative_totals_with_sub_meters(self):
        """
        With sub-meters, totals must NOT double-count.
        electricity_main.imp_kwh already includes sub-meter consumption.
        get_cumulative_totals should use:
          - main meter: imp_kwh_remainder (house-only grid load)
          - sub-meter:  imp_kwh_grid (sub-meter grid portion), or imp_kwh
          TOTAL = remainder + sub_grid ≈ main.imp_kwh
        """
        cfg = {"meters": {
            "electricity_main": {"meta": {
                "billing_day": 1, "block_minutes": 30, "timezone": "UTC",
                "currency_symbol": "£", "currency_code": "GBP",
            }, "channels": {
                "import": {"read": "sensor.main", "rate": "sensor.rate"},
                "export": {"read": "sensor.exp",  "rate": "sensor.exprate"},
            }},
            "ev_charger": {"meta": {
                "sub_meter": True, "parent_meter": "electricity_main",
            }, "channels": {
                "import": {"read": "sensor.ev", "rate": "sensor.rate"},
            }},
        }}
        self.store.insert_config_period(cfg)
        cp_id = self.store._conn.execute(
            "SELECT id FROM config_periods LIMIT 1"
        ).fetchone()["id"]

        # One block: main draws 3.0 kWh total, EV uses 2.0, house uses 1.0
        # main: imp_kwh=3.0, imp_kwh_remainder=1.0 (house only), imp_cost=0.735
        # ev:   imp_kwh=2.0, imp_kwh_grid=2.0 (all from grid), no independent cost
        self.store._conn.execute("""
            INSERT INTO blocks (block_start, block_end, meter_id, config_period_id, interpolated,
            imp_kwh, imp_kwh_remainder, imp_cost, exp_kwh, exp_cost, standing_charge)
            VALUES ('2026-01-01T00:00:00','2026-01-01T00:30:00','electricity_main',?,0, 3.0,1.0,0.735, 0.0,0.0, 0.5)
        """, (cp_id,))
        self.store._conn.execute("""
            INSERT INTO blocks (block_start, block_end, meter_id, config_period_id, interpolated,
            imp_kwh, imp_kwh_grid, imp_cost, exp_kwh, exp_cost, standing_charge)
            VALUES ('2026-01-01T00:00:00','2026-01-01T00:30:00','ev_charger',?,0, 2.0,2.0,0.0, 0.0,0.0, 0.0)
        """, (cp_id,))
        self.store._conn.commit()

        totals = self.store.get_cumulative_totals()

        # Correct: remainder(1.0) + ev_grid(2.0) = 3.0 kWh total grid import
        self.assertAlmostEqual(totals["import_kwh"], 3.0, places=4,
            msg="Sub-meter must not double-count: total grid = house(1) + ev_grid(2) = 3")
        # Cost only from main meter
        self.assertAlmostEqual(totals["import_cost"], 0.735, places=4,
            msg="Import cost must come from main meter only")
        # NOT 5.0 (3.0 + 2.0 double-counted)
        self.assertNotAlmostEqual(totals["import_kwh"], 5.0, places=1,
            msg="5.0 would indicate double-counting bug")


class TestUpgradePaths(unittest.TestCase):
    """
    Verify that the 1.x→2.1.0 and 2.0→2.1.0 upgrade paths work correctly:
    - New DB tables are created automatically (CREATE TABLE IF NOT EXISTS)
    - Config is loaded from file when DB has no periods (1.x path)
    - Config is loaded from DB when periods exist (2.0 path)
    - current_block.json migration seeds the DB correctly
    """

    def setUp(self):
        import sys, types
        eio = types.ModuleType("energy_engine_io"); eio.load_json = lambda *a,**kw: {}
        sys.modules.setdefault("energy_engine_io", eio)

    def test_new_tables_created_on_existing_db(self):
        """
        Simulates a 2.0.0 DB that lacks current_block and current_reads tables.
        Opening it with BlockStore should create them via CREATE TABLE IF NOT EXISTS.
        """
        import tempfile, os, sqlite3
        with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as f:
            db_path = f.name
        try:
            # Create a minimal 2.0.0-style DB with blocks and config_periods only
            conn = sqlite3.connect(db_path)
            conn.execute("""CREATE TABLE config_periods (
                id INTEGER PRIMARY KEY, effective_from TEXT, effective_to TEXT,
                billing_day INTEGER, block_minutes INTEGER, timezone TEXT,
                currency_symbol TEXT, currency_code TEXT, site_name TEXT,
                change_reason TEXT, full_config_json TEXT NOT NULL)""")
            conn.execute("""CREATE TABLE blocks (
                id INTEGER PRIMARY KEY, block_start TEXT, block_end TEXT, meter_id TEXT, config_period_id INTEGER,
                interpolated INTEGER, imp_kwh REAL, imp_cost REAL,
                exp_kwh REAL, exp_cost REAL, standing_charge REAL NOT NULL DEFAULT 0)""")
            conn.commit()
            conn.close()

            # Opening with BlockStore should add missing tables
            store = BlockStore(db_path)
            # Verify new tables exist
            tables = {r[0] for r in store._conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()}
            self.assertIn("current_block", tables,
                "current_block table must be created on 2.0→2.1 upgrade")
            self.assertIn("current_reads", tables,
                "current_reads table must be created on 2.0→2.1 upgrade")
        finally:
            os.unlink(db_path)

    def test_empty_db_has_no_current_block(self):
        """Fresh DB: load_current_block returns empty dict."""
        store = BlockStore(":memory:")
        self.assertEqual(store.load_current_block(), {})

    def test_config_period_none_when_empty(self):
        """Fresh DB (1.x path): get_current_config_period_id returns None."""
        store = BlockStore(":memory:")
        self.assertIsNone(store.get_current_config_period_id())

    def test_config_period_present_after_insert(self):
        """After insert_config_period (2.0 path): get_current_config_period_id returns id."""
        import json
        store = BlockStore(":memory:")
        cfg = {"meters": {"electricity_main": {"meta": {
            "billing_day": 1, "block_minutes": 30, "timezone": "UTC",
            "currency_symbol": "£", "currency_code": "GBP",
        }}}}
        store.insert_config_period(cfg)
        self.assertIsNotNone(store.get_current_config_period_id())

    def test_current_block_migration_from_file(self):
        """
        Simulates 2.0→2.1 current_block.json migration:
        load_current_block() returns empty, then file is loaded and saved to DB.
        """
        import json
        store = BlockStore(":memory:")
        # DB is empty (no current block)
        self.assertEqual(store.load_current_block(), {})

        # Simulate file content (as written by 2.0.0 engine)
        cb_from_file = {
            "start": "2026-04-05T00:00:00",
            "end":   "2026-04-05T00:30:00",
            "interpolated": False,
            "_last_checkpoint": "2026-04-05T00:15:00",
            "meters": {
                "electricity_main": {
                    "meta": {},
                    "standing_charge": 0.50,
                    "channels": {
                        "import": {
                            "reads": [{"ts": "2026-04-05T00:00:00", "value": 28000.0}],
                            "rates": [{"ts": "2026-04-05T00:00:00", "value": 0.245}],
                        }
                    }
                }
            }
        }

        # Migration step: save file content to DB
        store.save_current_block(cb_from_file)

        # Verify it round-trips correctly
        loaded = store.load_current_block()
        self.assertEqual(loaded["start"], "2026-04-05T00:00:00")
        self.assertEqual(loaded["_last_checkpoint"], "2026-04-05T00:15:00")
        reads = loaded["meters"]["electricity_main"]["channels"]["import"]["reads"]
        self.assertEqual(len(reads), 1)
        self.assertAlmostEqual(reads[0]["value"], 28000.0, places=3)

    def test_cumulative_totals_from_empty_db(self):
        """Fresh DB (or 2.1.0 after removing file): totals are all zero."""
        store = BlockStore(":memory:")
        totals = store.get_cumulative_totals()
        self.assertEqual(totals["import_kwh"], 0.0)
        self.assertEqual(totals["export_kwh"], 0.0)
        self.assertEqual(totals["import_cost"], 0.0)
        self.assertEqual(totals["export_cost"], 0.0)


class TestNormalisedMeters(unittest.TestCase):
    """Tests for the normalised meters/meter_channels tables."""

    def setUp(self):
        import sys, types
        eio = types.ModuleType("energy_engine_io"); eio.load_json = lambda *a, **kw: {}
        sys.modules.setdefault("energy_engine_io", eio)
        self.store = BlockStore(":memory:")

    def _cfg(self, billing_day=1, site="Home", sub_meters=None):
        cfg = {"meters": {"electricity_main": {"meta": {
            "billing_day": billing_day, "block_minutes": 30,
            "timezone": "Europe/London", "currency_symbol": "£",
            "currency_code": "GBP", "site": site,
        }, "channels": {
            "import": {"read": "sensor.import_kwh", "rate": "sensor.import_rate",
                       "standing_charge_sensor": "sensor.standing"},
            "export": {"read": "sensor.export_kwh", "rate": "sensor.export_rate"},
        }}}}
        if sub_meters:
            for mid, label in sub_meters.items():
                cfg["meters"][mid] = {"meta": {
                    "sub_meter": True, "parent_meter": "electricity_main",
                    "device": label, "protected": True,
                }, "channels": {
                    "import": {"read": f"sensor.{mid}_kwh", "rate": "sensor.import_rate"},
                }}
        return cfg

    def test_insert_creates_meter_rows(self):
        cfg = self._cfg()
        self.store.insert_config_period(cfg)
        rows = self.store._conn.execute("SELECT * FROM meters").fetchall()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["meter_id"], "electricity_main")
        self.assertEqual(rows[0]["is_sub_meter"], 0)

    def test_insert_creates_channel_rows(self):
        cfg = self._cfg()
        self.store.insert_config_period(cfg)
        rows = self.store._conn.execute("SELECT * FROM meter_channels ORDER BY channel").fetchall()
        self.assertEqual(len(rows), 2)
        channels = {r["channel"] for r in rows}
        self.assertEqual(channels, {"import", "export"})

    def test_import_sensors_stored(self):
        cfg = self._cfg()
        self.store.insert_config_period(cfg)
        ch = self.store._conn.execute(
            "SELECT * FROM meter_channels WHERE channel='import'"
        ).fetchone()
        self.assertEqual(ch["read_sensor"], "sensor.import_kwh")
        self.assertEqual(ch["rate_sensor"], "sensor.import_rate")
        self.assertEqual(ch["standing_charge_sensor"], "sensor.standing")

    def test_sub_meter_flags_stored(self):
        cfg = self._cfg(sub_meters={"ev_charger": "EV Charger"})
        self.store.insert_config_period(cfg)
        sub = self.store._conn.execute(
            "SELECT * FROM meters WHERE meter_id='ev_charger'"
        ).fetchone()
        self.assertEqual(sub["is_sub_meter"], 1)
        self.assertEqual(sub["parent_meter_id"], "electricity_main")
        self.assertEqual(sub["device_label"], "EV Charger")
        self.assertEqual(sub["protected"], 1)

    def test_config_from_db_roundtrip_simple(self):
        """config_from_db reproduces sensor entity IDs correctly."""
        cfg = self._cfg()
        self.store.insert_config_period(cfg)
        pid = self.store.get_current_config_period_id()
        out = self.store.config_from_db(pid)
        imp = out["meters"]["electricity_main"]["channels"]["import"]
        self.assertEqual(imp["read"], "sensor.import_kwh")
        self.assertEqual(imp["rate"], "sensor.import_rate")
        self.assertEqual(imp["standing_charge_sensor"], "sensor.standing")

    def test_config_from_db_roundtrip_sub_meter(self):
        """Sub-meter flags and parent_meter survive the roundtrip."""
        cfg = self._cfg(sub_meters={"ev_charger": "EV Charger"})
        self.store.insert_config_period(cfg)
        pid = self.store.get_current_config_period_id()
        out = self.store.config_from_db(pid)
        self.assertIn("ev_charger", out["meters"])
        meta = out["meters"]["ev_charger"]["meta"]
        self.assertTrue(meta.get("sub_meter"))
        self.assertEqual(meta.get("parent_meter"), "electricity_main")
        self.assertEqual(meta.get("device"), "EV Charger")
        self.assertTrue(meta.get("protected"))

    def test_config_from_db_billing_scalars(self):
        """Billing scalars from config_periods appear on every meter's meta."""
        cfg = self._cfg(billing_day=15, site="Test Home")
        self.store.insert_config_period(cfg)
        pid = self.store.get_current_config_period_id()
        out = self.store.config_from_db(pid)
        meta = out["meters"]["electricity_main"]["meta"]
        self.assertEqual(meta["billing_day"], 15)
        self.assertEqual(meta["site"], "Test Home")
        self.assertEqual(meta["timezone"], "Europe/London")

    def test_channel_meta_stored_and_retrieved(self):
        """mpan/tariff in channel meta round-trips through meter_channels columns."""
        cfg = self._cfg()
        cfg["meters"]["electricity_main"]["channels"]["import"]["meta"] = {
            "mpan": "1234567890123", "tariff": "Agile",
        }
        self.store.insert_config_period(cfg)
        pid = self.store.get_current_config_period_id()
        out = self.store.config_from_db(pid)
        ch_meta = out["meters"]["electricity_main"]["channels"]["import"].get("meta", {})
        self.assertEqual(ch_meta.get("mpan"), "1234567890123")
        self.assertEqual(ch_meta.get("tariff"), "Agile")

    def test_second_period_has_own_meter_rows(self):
        """Each config period gets its own set of meter rows."""
        cfg1 = self._cfg(billing_day=1, site="Period 1")
        cfg2 = self._cfg(billing_day=15, site="Period 2",
                         sub_meters={"ev_charger": "EV"})
        self.store.insert_config_period(cfg1)
        self.store.insert_config_period(cfg2)

        periods = self.store._conn.execute(
            "SELECT id FROM config_periods ORDER BY effective_from"
        ).fetchall()
        p1_id, p2_id = periods[0]["id"], periods[1]["id"]

        out1 = self.store.config_from_db(p1_id)
        out2 = self.store.config_from_db(p2_id)

        self.assertNotIn("ev_charger", out1["meters"])
        self.assertIn("ev_charger", out2["meters"])
        self.assertEqual(out1["meters"]["electricity_main"]["meta"]["billing_day"], 1)
        self.assertEqual(out2["meters"]["electricity_main"]["meta"]["billing_day"], 15)

    def test_delete_period_cascades_meters(self):
        """Deleting a config period removes its meter and channel rows."""
        cfg1 = self._cfg(site="First")
        cfg2 = self._cfg(site="Second")
        self.store.insert_config_period(cfg1)
        self.store.insert_config_period(cfg2)

        p1_id = self.store._conn.execute(
            "SELECT id FROM config_periods ORDER BY effective_from LIMIT 1"
        ).fetchone()["id"]

        self.store.delete_config_period(p1_id)

        remaining = self.store._conn.execute(
            "SELECT config_period_id FROM meters"
        ).fetchall()
        period_ids = {r["config_period_id"] for r in remaining}
        self.assertNotIn(p1_id, period_ids,
            "Meter rows for deleted period must be removed")

    def test_save_config_rewrites_meter_rows(self):
        """
        Saving a config that removes a sub-meter deletes the old meter row.
        """
        cfg_with_sub = self._cfg(sub_meters={"ev_charger": "EV Charger"})
        self.store.insert_config_period(cfg_with_sub)
        pid = self.store.get_current_config_period_id()

        # Verify ev_charger exists
        count_before = self.store._conn.execute(
            "SELECT COUNT(*) FROM meters WHERE config_period_id=?", (pid,)
        ).fetchone()[0]
        self.assertEqual(count_before, 2)  # main + ev_charger

        # Remove sub-meter from config (simulate save_config removing a meter)
        cfg_no_sub = self._cfg()
        with self.store._conn:
            self.store._conn.execute(
                """UPDATE config_periods
                   SET billing_day=?, block_minutes=?, timezone=?,
                       currency_symbol=?, currency_code=?, site_name=?
                   WHERE id=?""",
                (1, 30, "Europe/London", "£", "GBP", "Home", pid)
            )
            old_mids = [r["id"] for r in self.store._conn.execute(
                "SELECT id FROM meters WHERE config_period_id=?", (pid,)
            ).fetchall()]
            for mid in old_mids:
                self.store._conn.execute(
                    "DELETE FROM meter_channels WHERE meter_id=?", (mid,))
            self.store._conn.execute(
                "DELETE FROM meters WHERE config_period_id=?", (pid,))
            self.store._write_meters(cfg_no_sub, pid)

        count_after = self.store._conn.execute(
            "SELECT COUNT(*) FROM meters WHERE config_period_id=?", (pid,)
        ).fetchone()[0]
        self.assertEqual(count_after, 1,
            "ev_charger meter row must be removed when absent from new config")


class TestMigration(unittest.TestCase):
    """Tests for migrate_full_config_json — 2.0→2.1 upgrade path."""

    def setUp(self):
        import sys, types
        eio = types.ModuleType("energy_engine_io"); eio.load_json = lambda *a, **kw: {}
        sys.modules.setdefault("energy_engine_io", eio)

    def _make_v20_db(self, path):
        """Create a minimal 2.0.0-style DB with full_config_json and gap_marker blob."""
        import sqlite3, json
        conn = sqlite3.connect(path)
        conn.execute("""CREATE TABLE config_periods (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            effective_from TEXT NOT NULL, effective_to TEXT,
            billing_day INTEGER NOT NULL DEFAULT 1,
            block_minutes INTEGER NOT NULL DEFAULT 30,
            timezone TEXT NOT NULL DEFAULT 'UTC',
            currency_symbol TEXT NOT NULL DEFAULT '£',
            currency_code TEXT NOT NULL DEFAULT 'GBP',
            site_name TEXT, change_reason TEXT,
            full_config_json TEXT NOT NULL)""")
        conn.execute("""CREATE TABLE blocks (
            id INTEGER PRIMARY KEY, block_start TEXT, block_end TEXT, meter_id TEXT, config_period_id INTEGER,
            interpolated INTEGER, imp_kwh REAL, imp_cost REAL,
            exp_kwh REAL, exp_cost REAL, standing_charge REAL NOT NULL DEFAULT 0)""")
        conn.execute("""CREATE TABLE meters (
            id INTEGER PRIMARY KEY AUTOINCREMENT, meter_id TEXT NOT NULL,
            is_sub_meter INTEGER NOT NULL DEFAULT 0, device_label TEXT,
            parent_meter_id TEXT, config_period_id INTEGER NOT NULL,
            UNIQUE (config_period_id, meter_id))""")
        conn.execute("""CREATE TABLE current_block (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            block_start TEXT, block_end TEXT, last_checkpoint TEXT,
            gap_marker TEXT, interpolated INTEGER NOT NULL DEFAULT 0)""")
        conn.execute("""CREATE TABLE current_reads (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            captured_at TEXT NOT NULL, meter_id TEXT NOT NULL,
            channel TEXT NOT NULL, channel_type TEXT NOT NULL DEFAULT 'read',
            value REAL NOT NULL, standing_charge REAL)""")
        # Minimal config
        cfg = {"meters": {"electricity_main": {"meta": {
            "billing_day": 1, "block_minutes": 30, "timezone": "Europe/London",
            "currency_symbol": "£", "currency_code": "GBP", "site": "Home",
        }, "channels": {
            "import": {"read": "sensor.import", "rate": "sensor.rate",
                        "meta": {"mpan": "1234567890", "tariff": "Agile"}},
            "export": {"read": "sensor.export", "rate": "sensor.rate"},
        }}}}
        conn.execute(
            "INSERT INTO config_periods "
            "(effective_from, billing_day, block_minutes, timezone, "
            "currency_symbol, currency_code, full_config_json) "
            "VALUES ('2026-01-01T00:00:00', 1, 30, 'Europe/London', '£', 'GBP', ?)",
            (json.dumps(cfg),)
        )
        # Gap marker blob
        gap = {
            "detected_at": "2026-04-05T12:00:00",
            "pre_reads": {"electricity_main": {"import": {"ts": "2026-04-05T11:55:00", "value": 28000.0}}},
            "last_known_rates": {"electricity_main": {"import": {"ts": "2026-04-05T11:55:00", "value": 0.245}}},
        }
        conn.execute(
            "INSERT INTO current_block (id, block_start, block_end, gap_marker, interpolated) "
            "VALUES (1, '2026-04-05T12:00:00', '2026-04-05T12:30:00', ?, 0)",
            (json.dumps(gap),)
        )
        conn.commit()
        conn.close()

    def test_migrate_populates_meters_table(self):
        import tempfile, os
        with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as f:
            path = f.name
        try:
            self._make_v20_db(path)
            store = BlockStore(path)
            store.migrate_full_config_json()
            count = store._conn.execute("SELECT COUNT(*) FROM meters").fetchone()[0]
            self.assertGreater(count, 0, "meters table must be populated after migration")
        finally:
            os.unlink(path)

    def test_migrate_drops_full_config_json(self):
        import tempfile, os
        with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as f:
            path = f.name
        try:
            self._make_v20_db(path)
            store = BlockStore(path)
            store.migrate_full_config_json()
            cols = [r[1] for r in store._conn.execute(
                "PRAGMA table_info(config_periods)"
            ).fetchall()]
            self.assertNotIn("full_config_json", cols,
                "full_config_json column must be dropped after migration")
        finally:
            os.unlink(path)

    def test_migrate_drops_gap_marker_blob(self):
        import tempfile, os
        with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as f:
            path = f.name
        try:
            self._make_v20_db(path)
            store = BlockStore(path)
            store.migrate_full_config_json()
            cols = [r[1] for r in store._conn.execute(
                "PRAGMA table_info(current_block)"
            ).fetchall()]
            self.assertNotIn("gap_marker", cols,
                "gap_marker blob column must be dropped after migration")
            self.assertIn("gap_detected_at", cols,
                "gap_detected_at column must exist after migration")
        finally:
            os.unlink(path)

    def test_migrate_preserves_gap_detected_at(self):
        import tempfile, os
        with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as f:
            path = f.name
        try:
            self._make_v20_db(path)
            store = BlockStore(path)
            store.migrate_full_config_json()
            row = store._conn.execute(
                "SELECT gap_detected_at FROM current_block WHERE id=1"
            ).fetchone()
            self.assertEqual(row["gap_detected_at"], "2026-04-05T12:00:00",
                "gap_detected_at must be populated from migrated gap_marker blob")
        finally:
            os.unlink(path)

    def test_migrate_seeds_gap_seed_rows(self):
        import tempfile, os
        with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as f:
            path = f.name
        try:
            self._make_v20_db(path)
            store = BlockStore(path)
            store.migrate_full_config_json()
            seeds = store._conn.execute(
                "SELECT * FROM current_reads WHERE is_gap_seed > 0"
            ).fetchall()
            self.assertGreater(len(seeds), 0,
                "Gap seed rows must be written to current_reads during migration")
        finally:
            os.unlink(path)

    def test_migrate_adds_is_gap_seed_column(self):
        import tempfile, os
        with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as f:
            path = f.name
        try:
            self._make_v20_db(path)
            store = BlockStore(path)
            store.migrate_full_config_json()
            cols = [r[1] for r in store._conn.execute(
                "PRAGMA table_info(current_reads)"
            ).fetchall()]
            self.assertIn("is_gap_seed", cols,
                "is_gap_seed column must be added to current_reads during migration")
        finally:
            os.unlink(path)

    def test_migrate_adds_mpan_tariff_columns(self):
        import tempfile, os
        with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as f:
            path = f.name
        try:
            self._make_v20_db(path)
            store = BlockStore(path)
            store.migrate_full_config_json()
            cols = [r[1] for r in store._conn.execute(
                "PRAGMA table_info(meter_channels)"
            ).fetchall()]
            self.assertIn("mpan",   cols, "mpan column must be added to meter_channels")
            self.assertIn("tariff", cols, "tariff column must be added to meter_channels")
        finally:
            os.unlink(path)

    def test_migrate_populates_mpan_tariff(self):
        import tempfile, os
        with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as f:
            path = f.name
        try:
            self._make_v20_db(path)
            store = BlockStore(path)
            store.migrate_full_config_json()
            ch = store._conn.execute(
                "SELECT mpan, tariff FROM meter_channels WHERE channel='import'"
            ).fetchone()
            self.assertEqual(ch["mpan"],   "1234567890")
            self.assertEqual(ch["tariff"], "Agile")
        finally:
            os.unlink(path)

    def test_migrate_config_from_db_roundtrip(self):
        """After migration, config_from_db returns correct sensor IDs."""
        import tempfile, os
        with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as f:
            path = f.name
        try:
            self._make_v20_db(path)
            store = BlockStore(path)
            store.migrate_full_config_json()
            pid = store._conn.execute(
                "SELECT id FROM config_periods LIMIT 1"
            ).fetchone()["id"]
            cfg = store.config_from_db(pid)
            self.assertIn("electricity_main", cfg["meters"])
            imp = cfg["meters"]["electricity_main"]["channels"]["import"]
            self.assertEqual(imp["read"], "sensor.import")
            self.assertEqual(imp["rate"], "sensor.rate")
            self.assertEqual(imp.get("meta", {}).get("mpan"), "1234567890")
        finally:
            os.unlink(path)

    def test_migrate_idempotent(self):
        """Running migration twice has no ill effects."""
        import tempfile, os
        with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as f:
            path = f.name
        try:
            self._make_v20_db(path)
            store = BlockStore(path)
            result1 = store.migrate_full_config_json()
            result2 = store.migrate_full_config_json()
            self.assertEqual(result2, 0, "Second migration must return 0 (nothing to do)")
        finally:
            os.unlink(path)



class TestBillingTotalsSubMeterNullGrid(unittest.TestCase):
    """Regression test: sub-meter with NULL imp_kwh_grid must not double-count."""

    def setUp(self):
        self.store = BlockStore(":memory:")
        cfg = {"meters": {
            "electricity_main": {"meta": {
                "billing_day": 1, "block_minutes": 30, "timezone": "UTC",
                "currency_symbol": "£", "currency_code": "GBP",
            }, "channels": {
                "import": {"read": "s.imp", "rate": "s.rate"},
            }},
            "house_battery": {"meta": {
                "sub_meter": True, "parent_meter": "electricity_main",
            }, "channels": {
                "import": {"read": "s.bat", "rate": "s.rate"},
            }},
        }}
        self.store.insert_config_period(cfg)
        self.cp = self.store._conn.execute(
            "SELECT id FROM config_periods LIMIT 1"
        ).fetchone()["id"]

    def test_null_grid_no_fallback_to_raw_kwh(self):
        """
        Sub-meter with imp_kwh populated but imp_kwh_grid=NULL must NOT fall back
        to imp_kwh — that would double-count since main meter imp_kwh already
        includes sub-meter consumption. Only COALESCE(imp_kwh_grid, 0) is used.
        """
        # main: 13.0 kWh raw, 2.5 remainder
        self.store._conn.execute("""
            INSERT INTO blocks (block_start, block_end, meter_id, config_period_id, interpolated,
            imp_kwh, imp_kwh_remainder, imp_cost, standing_charge)
            VALUES ('2026-03-01T00:00:00','2026-03-01T00:30:00','electricity_main',?,0, 13.0,2.5,1.17,0.5)
        """, (self.cp,))
        # battery: 10.5 kWh raw but imp_kwh_grid=NULL (older block)
        self.store._conn.execute("""
            INSERT INTO blocks (block_start, block_end, meter_id, config_period_id, interpolated,
            imp_kwh, imp_kwh_grid, imp_cost, standing_charge)
            VALUES ('2026-03-01T00:00:00','2026-03-01T00:30:00','house_battery',?,0, 10.5,NULL,0.0,0.0)
        """, (self.cp,))
        self.store._conn.commit()

        t = self.store.get_billing_totals_for_utc_range(*local_date_range_to_utc_bounds('2026-03-01', '2026-03-01', 'UTC'))

        # 2.5 (main remainder) + 0 (battery NULL grid → 0) = 2.5, not 2.5+10.5=13.0
        self.assertAlmostEqual(t["imp_kwh"], 2.5, places=3,
            msg="Sub-meter NULL imp_kwh_grid must not fall back to imp_kwh")
        self.assertNotAlmostEqual(t["imp_kwh"], 13.0, places=1,
            msg="13.0 indicates double-counting bug")

    def test_set_grid_is_included(self):
        """Sub-meter with imp_kwh_grid set should be included in total."""
        self.store._conn.execute("""
            INSERT INTO blocks (block_start, block_end, meter_id, config_period_id, interpolated,
            imp_kwh, imp_kwh_remainder, imp_cost, standing_charge)
            VALUES ('2026-03-01T00:00:00','2026-03-01T00:30:00','electricity_main',?,0, 13.0,2.5,1.17,0.5)
        """, (self.cp,))
        self.store._conn.execute("""
            INSERT INTO blocks (block_start, block_end, meter_id, config_period_id, interpolated,
            imp_kwh, imp_kwh_grid, imp_cost, standing_charge)
            VALUES ('2026-03-01T00:00:00','2026-03-01T00:30:00','house_battery',?,0, 10.5,10.5,0.0,0.0)
        """, (self.cp,))
        self.store._conn.commit()

        t = self.store.get_billing_totals_for_utc_range(*local_date_range_to_utc_bounds('2026-03-01', '2026-03-01', 'UTC'))

        # 2.5 remainder + 10.5 grid = 13.0 total grid draw
        self.assertAlmostEqual(t["imp_kwh"], 13.0, places=3,
            msg="Sub-meter with imp_kwh_grid set must be included in total")

    def test_cost_from_main_meter_only(self):
        """imp_cost comes from main meter only — it already includes sub-meter costs."""
        self.store._conn.execute("""
            INSERT INTO blocks (block_start, block_end, meter_id, config_period_id, interpolated,
            imp_kwh, imp_kwh_remainder, imp_cost, standing_charge)
            VALUES ('2026-03-01T00:00:00','2026-03-01T00:30:00','electricity_main',?,0, 13.0,2.5,1.90,0.5)
        """, (self.cp,))
        # Battery: has its own imp_cost but main meter already includes it
        self.store._conn.execute("""
            INSERT INTO blocks (block_start, block_end, meter_id, config_period_id, interpolated,
            imp_kwh, imp_kwh_grid, imp_cost, standing_charge)
            VALUES ('2026-03-01T00:00:00','2026-03-01T00:30:00','house_battery',?,0, 10.5,10.5,0.73,0.0)
        """, (self.cp,))
        self.store._conn.commit()

        t = self.store.get_billing_totals_for_utc_range(*local_date_range_to_utc_bounds('2026-03-01', '2026-03-01', 'UTC'))

        # Total cost = main only (1.90) — sub-meter costs already included in main
        self.assertAlmostEqual(t["imp_cost"], 1.90, places=3,
            msg="imp_cost must come from main meter only — already includes sub-meters")
        self.assertNotAlmostEqual(t["imp_cost"], 1.90 + 0.73, places=2,
            msg="Adding sub-meter cost would double-count")


class TestBlockDeletion(unittest.TestCase):
    """Tests for delete_blocks_for_date_range and count_blocks_for_date_range."""

    def _make_store(self):
        store = BlockStore(":memory:")
        store.insert_config_period({"meters": {"electricity_main": {"meta": {
            "billing_day": 1, "block_minutes": 30, "timezone": "UTC",
            "currency_symbol": "£", "currency_code": "GBP",
        }, "channels": {"import": {"read": "s.imp", "rate": "s.rate"}}},
        "ev_charger": {"meta": {"sub_meter": True, "parent_meter": "electricity_main"},
                       "channels": {"import": {"read": "s.ev", "rate": "s.rate"}}},
        }})
        cp_id = store.get_current_config_period_id()
        rows = [
            ("2026-03-01T00:00:00", "electricity_main", 1.0),
            ("2026-03-01T00:30:00", "electricity_main", 1.0),
            ("2026-03-01T00:00:00", "ev_charger",       0.5),
            ("2026-03-02T00:00:00", "electricity_main", 2.0),
            ("2026-03-02T00:30:00", "electricity_main", 2.0),
            ("2026-03-03T00:00:00", "electricity_main", 3.0),
        ]
        for (bs, mid, kwh) in rows:
            store._conn.execute("""
                INSERT INTO blocks (block_start, block_end, meter_id, config_period_id, interpolated,
                  imp_kwh, imp_rate, imp_cost, standing_charge)
                VALUES (?,?,?,?,0,?,0.07,?,0.5)
            """, (bs, bs, mid, cp_id, kwh, kwh * 0.07))
        store._conn.commit()
        return store

    def test_count_preview_all_meters(self):
        store = self._make_store()
        r = store.count_blocks_for_date_range("2026-03-01", "2026-03-02")
        self.assertEqual(r["blocks"], 5)
        self.assertEqual(r["dates"], 2)

    def test_count_preview_single_meter(self):
        # Counting (or deleting) a MAIN meter now includes its sub-meters, so the
        # two 2026-03-01 main blocks plus its ev_charger block = 3.
        store = self._make_store()
        r = store.count_blocks_for_date_range("2026-03-01", "2026-03-01", "electricity_main")
        self.assertEqual(r["blocks"], 3)
        self.assertEqual(r["dates"], 1)
        # A device on its own counts only itself.
        rd = store.count_blocks_for_date_range("2026-03-01", "2026-03-01", "ev_charger")
        self.assertEqual(rd["blocks"], 1)

    def test_count_preview_no_match(self):
        store = self._make_store()
        r = store.count_blocks_for_date_range("2026-04-01", "2026-04-30")
        self.assertEqual(r["blocks"], 0)
        self.assertEqual(r["dates"], 0)

    def test_delete_all_meters(self):
        store = self._make_store()
        r = store.delete_blocks_for_date_range("2026-03-01", "2026-03-02")
        self.assertEqual(r["deleted"], 5)
        self.assertEqual(r["dates"], 2)
        remaining = store._conn.execute("SELECT COUNT(*) FROM blocks").fetchone()[0]
        self.assertEqual(remaining, 1, "Only Mar 3 block should remain")

    def test_delete_single_meter(self):
        store = self._make_store()
        r = store.delete_blocks_for_date_range("2026-03-01", "2026-03-01", "ev_charger")
        self.assertEqual(r["deleted"], 1)
        remaining = store._conn.execute(
            "SELECT COUNT(*) FROM blocks WHERE meter_id='ev_charger'"
        ).fetchone()[0]
        self.assertEqual(remaining, 0)
        # Other meters untouched
        main_count = store._conn.execute(
            "SELECT COUNT(*) FROM blocks WHERE meter_id='electricity_main'"
        ).fetchone()[0]
        self.assertEqual(main_count, 5)

    def test_delete_no_match_is_safe(self):
        store = self._make_store()
        r = store.delete_blocks_for_date_range("2026-05-01", "2026-05-31")
        self.assertEqual(r["deleted"], 0)
        total = store._conn.execute("SELECT COUNT(*) FROM blocks").fetchone()[0]
        self.assertEqual(total, 6, "No blocks should have been removed")

    def test_invalid_date_range_raises(self):
        store = self._make_store()
        with self.assertRaises(ValueError):
            store.delete_blocks_for_date_range("2026-03-10", "2026-03-01")

    def test_missing_dates_raises(self):
        store = self._make_store()
        with self.assertRaises(ValueError):
            store.delete_blocks_for_date_range("", "2026-03-01")


# ─────────────────────────────────────────────────────────────────────────────
# Carbon intensity (2.3.0+)
# ─────────────────────────────────────────────────────────────────────────────

class TestCarbonIntensity(unittest.TestCase):

    def setUp(self):
        self.store = BlockStore(":memory:")

    def test_upsert_and_retrieve(self):
        """Stored intensity can be retrieved as the nearest row."""
        self.store.upsert_carbon_intensity("2026-04-14T12:00:00", "DE1", 138.0, "moderate")
        row = self.store.get_nearest_carbon_intensity("2026-04-14T12:00:00", "DE1")
        self.assertIsNotNone(row)
        self.assertAlmostEqual(row["intensity"], 138.0)
        self.assertEqual(row["ci_index"], "moderate")

    def test_upsert_updates_existing(self):
        """Upserting same timestamp/postcode updates the value."""
        self.store.upsert_carbon_intensity("2026-04-14T12:00:00", "DE1", 138.0, "moderate")
        self.store.upsert_carbon_intensity("2026-04-14T12:00:00", "DE1", 95.0, "low")
        row = self.store.get_nearest_carbon_intensity("2026-04-14T12:00:00", "DE1")
        self.assertAlmostEqual(row["intensity"], 95.0)
        self.assertEqual(row["ci_index"], "low")

    def test_nearest_picks_closest_timestamp(self):
        """get_nearest picks the row closest in time."""
        self.store.upsert_carbon_intensity("2026-04-14T10:00:00", "DE1", 100.0, "low")
        self.store.upsert_carbon_intensity("2026-04-14T12:00:00", "DE1", 250.0, "high")
        # Query at 11:50 — closer to 12:00
        row = self.store.get_nearest_carbon_intensity("2026-04-14T11:50:00", "DE1")
        self.assertAlmostEqual(row["intensity"], 250.0)
        # Query at 10:10 — closer to 10:00
        row = self.store.get_nearest_carbon_intensity("2026-04-14T10:10:00", "DE1")
        self.assertAlmostEqual(row["intensity"], 100.0)

    def test_postcode_isolation(self):
        """Different postcodes don't interfere."""
        self.store.upsert_carbon_intensity("2026-04-14T12:00:00", "DE1", 138.0, "moderate")
        self.store.upsert_carbon_intensity("2026-04-14T12:00:00", "SW1", 220.0, "high")
        de1 = self.store.get_nearest_carbon_intensity("2026-04-14T12:00:00", "DE1")
        sw1 = self.store.get_nearest_carbon_intensity("2026-04-14T12:00:00", "SW1")
        self.assertAlmostEqual(de1["intensity"], 138.0)
        self.assertAlmostEqual(sw1["intensity"], 220.0)

    def test_returns_none_when_no_data(self):
        """No stored data → None."""
        row = self.store.get_nearest_carbon_intensity("2026-04-14T12:00:00", "DE1")
        self.assertIsNone(row)

    def test_prune_removes_old_rows(self):
        """Rows older than retention window are pruned."""
        from datetime import datetime, timezone, timedelta
        recent = (datetime.now(timezone.utc) - timedelta(hours=1)).replace(tzinfo=None).isoformat()
        self.store.upsert_carbon_intensity("2020-01-01T00:00:00", "DE1", 300.0, "very-high")
        self.store.upsert_carbon_intensity(recent, "DE1", 100.0, "low")
        deleted = self.store.prune_carbon_intensity(days=4)
        self.assertEqual(deleted, 1)
        remaining = self.store._conn.execute(
            "SELECT COUNT(*) FROM carbon_intensity"
        ).fetchone()[0]
        self.assertEqual(remaining, 1)

    def test_prune_preserves_recent_rows(self):
        """Recent rows survive pruning."""
        from datetime import datetime, timezone, timedelta
        recent = (datetime.now(timezone.utc) - timedelta(hours=1)).replace(tzinfo=None).isoformat()
        self.store.upsert_carbon_intensity(recent, "DE1", 138.0, "moderate")
        deleted = self.store.prune_carbon_intensity(days=4)
        self.assertEqual(deleted, 0)


# ─────────────────────────────────────────────────────────────────────────────
# Power history (2.3.0+)
# ─────────────────────────────────────────────────────────────────────────────

class TestPowerHistory(unittest.TestCase):

    def setUp(self):
        self.store = BlockStore(":memory:")

    @staticmethod
    def _ts(offset_hours=1):
        """Return a UTC ISO timestamp offset_hours ago — always within 48h window."""
        from datetime import datetime, timezone, timedelta
        return (datetime.now(timezone.utc).replace(tzinfo=None)
                - timedelta(hours=offset_hours)).strftime("%Y-%m-%dT%H:%M:%S")

    def test_append_and_retrieve(self):
        """Appended row is returned by get_power_history."""
        self.store.append_power_history(self._ts(3), 2.5, 180.0, 7.5)
        rows = self.store.get_power_history(hours=48)
        self.assertEqual(len(rows), 1)
        self.assertAlmostEqual(rows[0]["net_kw"], 2.5)
        self.assertAlmostEqual(rows[0]["intensity"], 180.0)
        self.assertAlmostEqual(rows[0]["carbon_gco2_min"], 7.5)

    def test_negative_net_kw(self):
        """Negative net_kw (exporting) is stored correctly."""
        self.store.append_power_history(self._ts(3), -1.5, 115.0, -2.875)
        rows = self.store.get_power_history(hours=48)
        self.assertAlmostEqual(rows[0]["net_kw"], -1.5)
        self.assertAlmostEqual(rows[0]["carbon_gco2_min"], -2.875)

    def test_null_intensity_stored(self):
        """None intensity stored and returned as None."""
        self.store.append_power_history(self._ts(3), 1.0, None, None)
        rows = self.store.get_power_history(hours=48)
        self.assertIsNone(rows[0]["intensity"])
        self.assertIsNone(rows[0]["carbon_gco2_min"])

    def test_upsert_on_conflict(self):
        """Duplicate captured_at updates the row."""
        self.store.append_power_history(self._ts(3), 2.5, 180.0, 7.5)
        self.store.append_power_history(self._ts(3), 3.0, 190.0, 9.5)
        rows = self.store.get_power_history(hours=48)
        self.assertEqual(len(rows), 1)
        self.assertAlmostEqual(rows[0]["net_kw"], 3.0)

    def test_rows_ordered_oldest_first(self):
        """get_power_history returns rows in ascending captured_at order."""
        self.store.append_power_history(self._ts(1), 3.0, None)
        self.store.append_power_history(self._ts(3), 1.0, None)
        self.store.append_power_history(self._ts(2), 2.0, None)
        rows = self.store.get_power_history(hours=48)
        self.assertEqual(len(rows), 3)
        self.assertLess(rows[0]["captured_at"], rows[1]["captured_at"])
        self.assertLess(rows[1]["captured_at"], rows[2]["captured_at"])

    def test_prune_removes_old_rows(self):
        """Rows older than retention window are pruned."""
        self.store.append_power_history("2020-01-01T00:00:00", 1.0, None)
        self.store.append_power_history(self._ts(3), 2.0, 180.0)
        deleted = self.store.prune_power_history(hours=48)
        self.assertEqual(deleted, 1)
        remaining = self.store._conn.execute(
            "SELECT COUNT(*) FROM power_history"
        ).fetchone()[0]
        self.assertEqual(remaining, 1)

    def test_prune_preserves_recent_rows(self):
        """Recent rows survive pruning."""
        from datetime import datetime, timezone, timedelta
        recent = (datetime.now(timezone.utc) - timedelta(minutes=10)).replace(tzinfo=None).isoformat()
        self.store.append_power_history(recent, 1.5, 120.0)
        deleted = self.store.prune_power_history(hours=48)
        self.assertEqual(deleted, 0)

    def test_hours_param_filters_results(self):
        """get_power_history with hours=1 excludes rows older than 1 hour."""
        self.store.append_power_history("2020-01-01T00:00:00", 1.0, None)
        from datetime import datetime, timezone, timedelta
        recent = (datetime.now(timezone.utc) - timedelta(minutes=30)).replace(tzinfo=None).isoformat()
        self.store.append_power_history(recent, 2.0, 150.0)
        rows = self.store.get_power_history(hours=1)
        self.assertEqual(len(rows), 1)
        self.assertAlmostEqual(rows[0]["net_kw"], 2.0)

    def test_carbon_gco2_min_formula(self):
        """Verify carbon_gco2_min = net_kw * intensity / 60 is consistent."""
        net_kw = -1.5
        intensity = 115.0
        expected = round(net_kw * intensity / 60.0, 4)
        self.store.append_power_history(self._ts(3), net_kw, intensity, expected)
        rows = self.store.get_power_history(hours=48)
        self.assertAlmostEqual(rows[0]["carbon_gco2_min"], expected, places=4)

class TestSubMeterHistory(unittest.TestCase):
    """Tests for sub_meter_history table — SoC and power history for Live Power gauges."""

    def setUp(self):
        self.store = open_block_store(":memory:")
        self.store.insert_config_period(EXAMPLE_CONFIG)

    def tearDown(self):
        self.store.close()

    def _ts(self, minutes_ago: int) -> str:
        from datetime import datetime, timezone, timedelta
        return (datetime.now(timezone.utc) - timedelta(minutes=minutes_ago)).replace(tzinfo=None).isoformat()

    def test_append_and_retrieve(self):
        """append_sub_meter_history stores a row retrievable by get_sub_meter_history."""
        ts = self._ts(10)
        self.store.append_sub_meter_history(ts, "house_battery", 83.5, -3.0)
        rows = self.store.get_sub_meter_history("house_battery", hours=48)
        self.assertEqual(len(rows), 1)
        self.assertAlmostEqual(rows[0]["soc_pct"], 83.5)
        self.assertAlmostEqual(rows[0]["inverter_kw"], -3.0)

    def test_null_soc_stored(self):
        """soc_pct can be None when only inverter power is available."""
        ts = self._ts(5)
        self.store.append_sub_meter_history(ts, "house_battery", None, 2.5)
        rows = self.store.get_sub_meter_history("house_battery", hours=48)
        self.assertEqual(len(rows), 1)
        self.assertIsNone(rows[0]["soc_pct"])
        self.assertAlmostEqual(rows[0]["inverter_kw"], 2.5)

    def test_null_power_stored(self):
        """inverter_kw can be None when only SoC is available."""
        ts = self._ts(5)
        self.store.append_sub_meter_history(ts, "house_battery", 47.0, None)
        rows = self.store.get_sub_meter_history("house_battery", hours=48)
        self.assertEqual(len(rows), 1)
        self.assertAlmostEqual(rows[0]["soc_pct"], 47.0)
        self.assertIsNone(rows[0]["inverter_kw"])

    def test_meter_id_isolation(self):
        """get_sub_meter_history only returns rows for the requested meter."""
        ts1 = self._ts(10)
        ts2 = self._ts(5)
        self.store.append_sub_meter_history(ts1, "house_battery", 80.0, -2.0)
        self.store.append_sub_meter_history(ts2, "ev_charger", None, 7.4)
        batt_rows = self.store.get_sub_meter_history("house_battery", hours=48)
        ev_rows   = self.store.get_sub_meter_history("ev_charger",    hours=48)
        self.assertEqual(len(batt_rows), 1)
        self.assertEqual(len(ev_rows),   1)
        self.assertAlmostEqual(batt_rows[0]["soc_pct"], 80.0)
        self.assertAlmostEqual(ev_rows[0]["inverter_kw"], 7.4)

    def test_rows_ordered_oldest_first(self):
        """get_sub_meter_history returns rows in ascending captured_at order."""
        # Insert newest first — i minutes ago, SoC = i*10 (older = lower SoC)
        for i in range(5, 0, -1):
            self.store.append_sub_meter_history(self._ts(i), "house_battery", float(i * 10), None)
        rows = self.store.get_sub_meter_history("house_battery", hours=48)
        # Oldest first means highest i first → highest SoC first (50, 40, 30, 20, 10)
        # Verify timestamps are ascending (captured_at ISO strings sort lexicographically)
        timestamps = [r["captured_at"] for r in rows]
        self.assertEqual(timestamps, sorted(timestamps))

    def test_prune_removes_old_rows(self):
        """prune_sub_meter_history deletes rows older than the specified hours."""
        old_ts = "2020-01-01T00:00:00"
        self.store.append_sub_meter_history(old_ts, "house_battery", 50.0, None)
        recent_ts = self._ts(30)
        self.store.append_sub_meter_history(recent_ts, "house_battery", 60.0, None)
        deleted = self.store.prune_sub_meter_history(hours=48)
        self.assertEqual(deleted, 1)
        rows = self.store.get_sub_meter_history("house_battery", hours=48)
        self.assertEqual(len(rows), 1)
        self.assertAlmostEqual(rows[0]["soc_pct"], 60.0)

    def test_prune_preserves_recent_rows(self):
        """prune_sub_meter_history does not delete rows within the window."""
        for i in [5, 10, 20]:
            self.store.append_sub_meter_history(self._ts(i), "house_battery", float(i), None)
        deleted = self.store.prune_sub_meter_history(hours=48)
        self.assertEqual(deleted, 0)
        rows = self.store.get_sub_meter_history("house_battery", hours=48)
        self.assertEqual(len(rows), 3)

    def test_hours_param_filters_results(self):
        """get_sub_meter_history with hours=1 excludes rows older than 1 hour."""
        old_ts = "2020-01-01T00:00:00"
        self.store.append_sub_meter_history(old_ts, "house_battery", 40.0, None)
        recent_ts = self._ts(30)
        self.store.append_sub_meter_history(recent_ts, "house_battery", 75.0, None)
        rows = self.store.get_sub_meter_history("house_battery", hours=1)
        self.assertEqual(len(rows), 1)
        self.assertAlmostEqual(rows[0]["soc_pct"], 75.0)


class TestMeterTypePersistence(unittest.TestCase):
    """Tests that meter_type and sensor fields persist through write/read roundtrip."""

    BATTERY_CONFIG = {
        "meters": {
            "electricity_main": {
                "meta": {
                    "site": "Test Home",
                    "timezone": "Europe/London",
                    "billing_day": 1,
                    "block_minutes": 30,
                    "currency_symbol": "£",
                    "currency_code": "GBP",
                },
                "channels": {
                    "import": {"sensor": "sensor.import"},
                    "export": {"sensor": "sensor.export"},
                },
            },
            "sub_meter_battery_001": {
                "meta": {
                    "sub_meter": True,
                    "meter_type": "battery",
                    "device": "House Battery",
                    "parent_meter": "electricity_main",
                    "soc_sensor": "sensor.battery_soc",
                    "inverter_power_sensor": "sensor.battery_power",
                    "device_power_sensor": None,
                    "v2x_capable": False,
                },
                "channels": {
                    "import": {"sensor": "sensor.battery_import"},
                },
            },
        }
    }

    EV_CONFIG = {
        "meters": {
            "electricity_main": {
                "meta": {
                    "site": "Test Home",
                    "timezone": "Europe/London",
                    "billing_day": 1,
                    "block_minutes": 30,
                    "currency_symbol": "£",
                    "currency_code": "GBP",
                },
                "channels": {
                    "import": {"sensor": "sensor.import"},
                    "export": {"sensor": "sensor.export"},
                },
            },
            "sub_meter_ev_001": {
                "meta": {
                    "sub_meter": True,
                    "meter_type": "ev",
                    "device": "Zappi EV Charger",
                    "parent_meter": "electricity_main",
                    "device_power_sensor": "sensor.zappi_power",
                    "v2x_capable": True,
                },
                "channels": {
                    "import": {"sensor": "sensor.zappi"},
                },
            },
        }
    }

    def setUp(self):
        self.store = open_block_store(":memory:")

    def tearDown(self):
        self.store.close()

    def test_meter_type_battery_roundtrip(self):
        """meter_type='battery' persists through insert and config_from_db."""
        self.store.insert_config_period(self.BATTERY_CONFIG)
        pid = self.store.get_current_config_period_id()
        restored = self.store.config_from_db(pid)
        sub = restored["meters"]["sub_meter_battery_001"]["meta"]
        self.assertEqual(sub.get("meter_type"), "battery")

    def test_soc_sensor_roundtrip(self):
        """soc_sensor persists through insert and config_from_db."""
        self.store.insert_config_period(self.BATTERY_CONFIG)
        pid = self.store.get_current_config_period_id()
        restored = self.store.config_from_db(pid)
        sub = restored["meters"]["sub_meter_battery_001"]["meta"]
        self.assertEqual(sub.get("soc_sensor"), "sensor.battery_soc")

    def test_inverter_power_sensor_roundtrip(self):
        """inverter_power_sensor persists through insert and config_from_db."""
        self.store.insert_config_period(self.BATTERY_CONFIG)
        pid = self.store.get_current_config_period_id()
        restored = self.store.config_from_db(pid)
        sub = restored["meters"]["sub_meter_battery_001"]["meta"]
        self.assertEqual(sub.get("inverter_power_sensor"), "sensor.battery_power")

    def test_inverter_possible_auto_set_for_battery(self):
        """inverter_possible is True in DB when meter_type='battery'."""
        self.store.insert_config_period(self.BATTERY_CONFIG)
        pid = self.store.get_current_config_period_id()
        row = self.store._conn.execute(
            "SELECT inverter_possible FROM meters WHERE meter_id = 'sub_meter_battery_001'"
        ).fetchone()
        self.assertEqual(row["inverter_possible"], 1)

    def test_meter_type_ev_roundtrip(self):
        """meter_type='ev' persists through insert and config_from_db."""
        self.store.insert_config_period(self.EV_CONFIG)
        pid = self.store.get_current_config_period_id()
        restored = self.store.config_from_db(pid)
        sub = restored["meters"]["sub_meter_ev_001"]["meta"]
        self.assertEqual(sub.get("meter_type"), "ev")

    def test_device_power_sensor_roundtrip(self):
        """device_power_sensor persists through insert and config_from_db."""
        self.store.insert_config_period(self.EV_CONFIG)
        pid = self.store.get_current_config_period_id()
        restored = self.store.config_from_db(pid)
        sub = restored["meters"]["sub_meter_ev_001"]["meta"]
        self.assertEqual(sub.get("device_power_sensor"), "sensor.zappi_power")

    def test_v2x_capable_roundtrip(self):
        """v2x_capable=True persists through insert and config_from_db."""
        self.store.insert_config_period(self.EV_CONFIG)
        pid = self.store.get_current_config_period_id()
        restored = self.store.config_from_db(pid)
        sub = restored["meters"]["sub_meter_ev_001"]["meta"]
        self.assertTrue(sub.get("v2x_capable"))

    def test_inverter_possible_not_auto_set_for_ev(self):
        """inverter_possible is NOT auto-set for EV type meters."""
        self.store.insert_config_period(self.EV_CONFIG)
        row = self.store._conn.execute(
            "SELECT inverter_possible FROM meters WHERE meter_id = 'sub_meter_ev_001'"
        ).fetchone()
        self.assertEqual(row["inverter_possible"], 0)



import tempfile
from block_store import BlockStore

class TestInverterPowerInvert(unittest.TestCase):
    """Tests for inverter_power_invert field persistence through write/read roundtrip."""

    def setUp(self):
        self.tmp = tempfile.NamedTemporaryFile(suffix='.db', delete=False)
        self.tmp.close()
        self.store = BlockStore(self.tmp.name)

    def tearDown(self):
        self.store._conn.close()
        os.unlink(self.tmp.name)

    BASE_CONFIG = {
        "meters": {
            "electricity_main": {
                "meta": {
                    "site": "Test Home", "timezone": "UTC",
                    "billing_day": 1, "block_minutes": 30,
                    "currency_symbol": "£", "currency_code": "GBP",
                },
                "channels": {
                    "import": {"sensor": "sensor.import"},
                    "export": {"sensor": "sensor.export"},
                },
            },
            "sub_meter_battery_001": {
                "meta": {
                    "sub_meter": True, "meter_type": "battery",
                    "device": "House Battery",
                    "parent_meter": "electricity_main",
                    "soc_sensor": "sensor.soc",
                    "inverter_power_sensor": "sensor.inv_power",
                    "inverter_power_invert": True,
                },
                "channels": { "import": {"sensor": "sensor.battery_import"} },
            },
        }
    }

    def test_inverter_power_invert_true_roundtrip(self):
        """inverter_power_invert=True persists through insert and config_from_db."""
        self.store.insert_config_period(self.BASE_CONFIG)
        pid = self.store.get_current_config_period_id()
        restored = self.store.config_from_db(pid)
        sub = restored["meters"]["sub_meter_battery_001"]["meta"]
        self.assertTrue(sub.get("inverter_power_invert"),
                        "inverter_power_invert should be True after roundtrip")

    def test_inverter_power_invert_false_not_in_meta(self):
        """inverter_power_invert=False is not included in meta (falsy values omitted)."""
        cfg = {
            "meters": {
                "electricity_main": self.BASE_CONFIG["meters"]["electricity_main"],
                "sub_meter_battery_002": {
                    "meta": {
                        "sub_meter": True, "meter_type": "battery",
                        "device": "Battery 2",
                        "parent_meter": "electricity_main",
                        "inverter_power_invert": False,
                    },
                    "channels": { "import": {"sensor": "sensor.bat2"} },
                }
            }
        }
        self.store.insert_config_period(cfg)
        pid = self.store.get_current_config_period_id()
        restored = self.store.config_from_db(pid)
        sub = restored["meters"]["sub_meter_battery_002"]["meta"]
        self.assertFalse(sub.get("inverter_power_invert"),
                         "inverter_power_invert=False should not appear in meta")

    def test_inverter_power_invert_db_column(self):
        """inverter_power_invert is stored as INTEGER 1 in the meters table."""
        self.store.insert_config_period(self.BASE_CONFIG)
        row = self.store._conn.execute(
            "SELECT inverter_power_invert FROM meters WHERE meter_id = 'sub_meter_battery_001'"
        ).fetchone()
        self.assertEqual(row["inverter_power_invert"], 1)

    def test_inverter_power_invert_stored_as_integer_one(self):
        """inverter_power_invert=True is stored as INTEGER 1 in meters table after insert."""
        self.store.insert_config_period(self.BASE_CONFIG)
        row = self.store._conn.execute(
            "SELECT inverter_power_invert FROM meters WHERE meter_id = 'sub_meter_battery_001'"
        ).fetchone()
        self.assertIsNotNone(row, "meter row should exist")
        self.assertEqual(row["inverter_power_invert"], 1,
                         "inverter_power_invert=True should be stored as INTEGER 1")


class TestConfigFromDbReconstruction(unittest.TestCase):
    """Tests that config_from_db correctly reconstructs all meter meta fields."""

    def setUp(self):
        self.tmp = tempfile.NamedTemporaryFile(suffix='.db', delete=False)
        self.tmp.close()
        self.store = BlockStore(self.tmp.name)
        self.FULL_CONFIG = {
            "meters": {
                "electricity_main": {
                    "meta": {
                        "site": "My Home", "timezone": "Europe/London",
                        "billing_day": 3, "block_minutes": 30,
                        "currency_symbol": "£", "currency_code": "GBP",
                        "power_sensor": "sensor.smart_meter_power",
                        "postcode_prefix": "DE1",
                        "supplier": "Octopus Energy",
                    },
                    "channels": {
                        "import": {"read": "sensor.import", "rate": "sensor.rate"},
                        "export": {"read": "sensor.export", "rate": "sensor.export_rate"},
                    },
                },
                "sub_meter_battery": {
                    "meta": {
                        "sub_meter": True, "meter_type": "battery",
                        "device": "Solax Battery",
                        "parent_meter": "electricity_main",
                        "soc_sensor": "sensor.solax_soc",
                        "inverter_power_sensor": "sensor.solax_power",
                        "inverter_power_invert": True,
                        "inverter_possible": True,
                    },
                    "channels": { "import": {"read": "sensor.solax_kwh", "rate": "sensor.rate"} },
                },
                "sub_meter_ev": {
                    "meta": {
                        "sub_meter": True, "meter_type": "ev",
                        "device": "Zappi Charger",
                        "parent_meter": "electricity_main",
                        "device_power_sensor": "sensor.zappi_power",
                        "v2x_capable": True,
                    },
                    "channels": { "import": {"read": "sensor.zappi_kwh", "rate": "sensor.rate"} },
                },
                "sub_meter_hp": {
                    "meta": {
                        "sub_meter": True, "meter_type": "heat_pump",
                        "device": "Heat Pump",
                        "parent_meter": "electricity_main",
                        "device_power_sensor": "sensor.hp_power",
                    },
                    "channels": { "import": {"read": "sensor.hp_kwh", "rate": "sensor.rate"} },
                },
            }
        }
        self.store.insert_config_period(self.FULL_CONFIG)
        self.pid = self.store.get_current_config_period_id()
        self.restored = self.store.config_from_db(self.pid)

    def tearDown(self):
        self.store._conn.close()
        os.unlink(self.tmp.name)

    def test_site_name_reconstructed(self):
        main = self.restored["meters"]["electricity_main"]["meta"]
        self.assertEqual(main.get("site"), "My Home")

    def test_postcode_reconstructed(self):
        main = self.restored["meters"]["electricity_main"]["meta"]
        self.assertEqual(main.get("postcode_prefix"), "DE1")

    def test_battery_soc_sensor_reconstructed(self):
        sub = self.restored["meters"]["sub_meter_battery"]["meta"]
        self.assertEqual(sub.get("soc_sensor"), "sensor.solax_soc")

    def test_battery_inverter_power_sensor_reconstructed(self):
        sub = self.restored["meters"]["sub_meter_battery"]["meta"]
        self.assertEqual(sub.get("inverter_power_sensor"), "sensor.solax_power")

    def test_battery_inverter_power_invert_reconstructed(self):
        sub = self.restored["meters"]["sub_meter_battery"]["meta"]
        self.assertTrue(sub.get("inverter_power_invert"))

    def test_battery_meter_type_reconstructed(self):
        sub = self.restored["meters"]["sub_meter_battery"]["meta"]
        self.assertEqual(sub.get("meter_type"), "battery")

    def test_ev_device_power_sensor_reconstructed(self):
        sub = self.restored["meters"]["sub_meter_ev"]["meta"]
        self.assertEqual(sub.get("device_power_sensor"), "sensor.zappi_power")

    def test_ev_v2x_capable_reconstructed(self):
        sub = self.restored["meters"]["sub_meter_ev"]["meta"]
        self.assertTrue(sub.get("v2x_capable"))

    def test_ev_meter_type_reconstructed(self):
        sub = self.restored["meters"]["sub_meter_ev"]["meta"]
        self.assertEqual(sub.get("meter_type"), "ev")

    def test_hp_device_power_sensor_reconstructed(self):
        sub = self.restored["meters"]["sub_meter_hp"]["meta"]
        self.assertEqual(sub.get("device_power_sensor"), "sensor.hp_power")

    def test_hp_meter_type_reconstructed(self):
        sub = self.restored["meters"]["sub_meter_hp"]["meta"]
        self.assertEqual(sub.get("meter_type"), "heat_pump")

    def test_all_meters_present(self):
        self.assertEqual(len(self.restored["meters"]), 4)


class TestCascadeDeleteConfig(unittest.TestCase):
    """Tests that cascade delete correctly removes meter from meters table."""

    def setUp(self):
        self.tmp = tempfile.NamedTemporaryFile(suffix='.db', delete=False)
        self.tmp.close()
        self.store = BlockStore(self.tmp.name)
        self.CONFIG = {
            "meters": {
                "electricity_main": {
                    "meta": {
                        "site": "Test", "timezone": "UTC",
                        "billing_day": 1, "block_minutes": 30,
                        "currency_symbol": "£", "currency_code": "GBP",
                    },
                    "channels": {
                        "import": {"read": "sensor.imp", "rate": "sensor.rate"},
                        "export": {"read": "sensor.exp", "rate": "sensor.rate"},
                    },
                },
                "sub_meter_battery": {
                    "meta": {
                        "sub_meter": True, "meter_type": "battery",
                        "device": "House Battery",
                        "parent_meter": "electricity_main",
                    },
                    "channels": { "import": {"read": "sensor.bat", "rate": "sensor.rate"} },
                },
            }
        }
        self.store.insert_config_period(self.CONFIG)
        self.pid = self.store.get_current_config_period_id()

    def tearDown(self):
        self.store._conn.close()
        os.unlink(self.tmp.name)

    def _write_meters_delete_then_rewrite(self, new_cfg):
        """Mirrors the delete-then-rewrite pattern in api_meter_delete_data."""
        with self.store._conn:
            old_ids = [r["id"] for r in self.store._conn.execute(
                "SELECT id FROM meters WHERE config_period_id=?", (self.pid,)
            ).fetchall()]
            for oid in old_ids:
                self.store._conn.execute(
                    "DELETE FROM meter_channels WHERE meter_id=?", (oid,)
                )
            self.store._conn.execute(
                "DELETE FROM meters WHERE config_period_id=?", (self.pid,)
            )
            self.store._write_meters(new_cfg, self.pid)

    def test_meter_present_before_delete(self):
        """Both meters exist before delete."""
        restored = self.store.config_from_db(self.pid)
        self.assertIn("sub_meter_battery", restored["meters"])
        self.assertIn("electricity_main", restored["meters"])

    def test_meter_absent_after_delete_rewrite(self):
        """Sub-meter is absent after delete-then-rewrite with config that excludes it."""
        new_cfg = {
            "meters": {
                "electricity_main": self.CONFIG["meters"]["electricity_main"]
            }
        }
        self._write_meters_delete_then_rewrite(new_cfg)
        restored = self.store.config_from_db(self.pid)
        self.assertNotIn("sub_meter_battery", restored["meters"],
                         "Deleted sub-meter should not appear in config_from_db")

    def test_main_meter_preserved_after_delete(self):
        """Main meter is preserved after sub-meter delete."""
        new_cfg = {
            "meters": {
                "electricity_main": self.CONFIG["meters"]["electricity_main"]
            }
        }
        self._write_meters_delete_then_rewrite(new_cfg)
        restored = self.store.config_from_db(self.pid)
        self.assertIn("electricity_main", restored["meters"])

    def test_both_meters_present_without_delete(self):
        """Both meters present in config_from_db after initial insert — no delete performed."""
        restored = self.store.config_from_db(self.pid)
        self.assertIn("sub_meter_battery", restored["meters"])
        self.assertIn("electricity_main", restored["meters"])
        self.assertEqual(len(restored["meters"]), 2)


class TestFreshInstallNoDuplicatePeriod(unittest.TestCase):
    """Tests that a fresh install doesn't create a duplicate billing period."""

    def setUp(self):
        self.tmp = tempfile.NamedTemporaryFile(suffix='.db', delete=False)
        self.tmp.close()
        self.store = BlockStore(self.tmp.name)

    def tearDown(self):
        self.store._conn.close()
        os.unlink(self.tmp.name)

    def test_no_blocks_period_updated_in_place(self):
        """When current period has 0 blocks, _write_meters update replaces rather than creates."""
        # Simulate engine creating initial empty period
        empty_cfg = {
            "meters": {
                "electricity_main": {
                    "meta": {
                        "site": "", "timezone": "UTC",
                        "billing_day": 1, "block_minutes": 30,
                        "currency_symbol": "£", "currency_code": "GBP",
                    },
                    "channels": {
                        "import": {"read": "", "rate": ""},
                        "export": {"read": "", "rate": ""},
                    },
                }
            }
        }
        self.store.insert_config_period(empty_cfg)
        pid1 = self.store.get_current_config_period_id()

        # Verify no blocks
        block_count = self.store._conn.execute(
            "SELECT COUNT(*) FROM blocks WHERE config_period_id=?", (pid1,)
        ).fetchone()[0]
        self.assertEqual(block_count, 0)

        # Simulate wizard saving full config — should update period 1, not create period 2
        full_cfg = {
            "meters": {
                "electricity_main": {
                    "meta": {
                        "site": "My Home", "timezone": "Europe/London",
                        "billing_day": 1, "block_minutes": 30,
                        "currency_symbol": "£", "currency_code": "GBP",
                        "supplier": "Octopus Energy",
                    },
                    "channels": {
                        "import": {"read": "sensor.import", "rate": "sensor.rate"},
                        "export": {"read": "sensor.export", "rate": "sensor.rate"},
                    },
                }
            }
        }
        # Rewrite meters for existing period (simulates api_save_config block_count==0 path)
        with self.store._conn:
            old_ids = [r["id"] for r in self.store._conn.execute(
                "SELECT id FROM meters WHERE config_period_id=?", (pid1,)
            ).fetchall()]
            for oid in old_ids:
                self.store._conn.execute("DELETE FROM meter_channels WHERE meter_id=?", (oid,))
            self.store._conn.execute("DELETE FROM meters WHERE config_period_id=?", (pid1,))
            self.store._write_meters(full_cfg, pid1)

        # Should still be only one config period
        periods = self.store._conn.execute(
            "SELECT COUNT(*) FROM config_periods"
        ).fetchone()[0]
        self.assertEqual(periods, 1, "Should be exactly 1 config period after wizard save on fresh install")

    def test_period_count_after_two_saves_with_no_blocks(self):
        """Multiple saves with 0 blocks should not accumulate periods."""
        cfg = {
            "meters": {
                "electricity_main": {
                    "meta": {
                        "site": "Home", "timezone": "UTC",
                        "billing_day": 1, "block_minutes": 30,
                        "currency_symbol": "£", "currency_code": "GBP",
                    },
                    "channels": {
                        "import": {"read": "sensor.import", "rate": "sensor.rate"},
                        "export": {"read": "", "rate": ""},
                    },
                }
            }
        }
        self.store.insert_config_period(cfg)
        pid = self.store.get_current_config_period_id()

        # Two updates on same zero-block period
        for _ in range(2):
            with self.store._conn:
                old_ids = [r["id"] for r in self.store._conn.execute(
                    "SELECT id FROM meters WHERE config_period_id=?", (pid,)
                ).fetchall()]
                for oid in old_ids:
                    self.store._conn.execute("DELETE FROM meter_channels WHERE meter_id=?", (oid,))
                self.store._conn.execute("DELETE FROM meters WHERE config_period_id=?", (pid,))
                self.store._write_meters(cfg, pid)

        periods = self.store._conn.execute("SELECT COUNT(*) FROM config_periods").fetchone()[0]
        self.assertEqual(periods, 1)


# ─────────────────────────────────────────────────────────────────────────────
# local_date_to_utc_bounds and local_date_range_to_utc_bounds
# ─────────────────────────────────────────────────────────────────────────────

class TestLocalDateToUtcBounds(unittest.TestCase):
    """
    Comprehensive tests for local_date_to_utc_bounds() covering:
    - GMT (winter) days
    - BST (summer) days
    - BST start day (23-hour day, clocks forward)
    - BST end day (25-hour day, clocks back)
    - UTC timezone (no offset)
    - Invalid timezone (falls back to UTC)
    - Multi-day ranges via local_date_range_to_utc_bounds()
    - Ranges straddling DST transitions
    - Billing period summation correctness
    """

    def test_gmt_day(self):
        """Winter GMT day: midnight-to-midnight UTC."""
        start, end = local_date_to_utc_bounds('2026-01-15', 'Europe/London')
        self.assertEqual(start, '2026-01-15T00:00:00')
        self.assertEqual(end,   '2026-01-16T00:00:00')

    def test_bst_day(self):
        """Summer BST day: offset -1hr — day starts at 23:00 UTC previous day."""
        start, end = local_date_to_utc_bounds('2026-07-15', 'Europe/London')
        self.assertEqual(start, '2026-07-14T23:00:00')
        self.assertEqual(end,   '2026-07-15T23:00:00')

    def test_bst_start_day(self):
        """BST starts 29 Mar 2026 — 23-hour day (clocks go forward at 01:00).
        Mar 29 starts at midnight GMT (00:00 UTC) and ends at 23:00 UTC (Mar 30 midnight BST)."""
        start, end = local_date_to_utc_bounds('2026-03-29', 'Europe/London')
        self.assertEqual(start, '2026-03-29T00:00:00')
        self.assertEqual(end,   '2026-03-29T23:00:00')
        # Verify it's exactly 23 hours
        from datetime import datetime
        s = datetime.fromisoformat(start)
        e = datetime.fromisoformat(end)
        self.assertEqual((e - s).total_seconds(), 23 * 3600)

    def test_bst_end_day(self):
        """BST ends 25 Oct 2026 — 25-hour day (clocks go back at 02:00).
        Oct 25 starts at 23:00 UTC Oct 24 (midnight BST) and ends at 00:00 UTC Oct 26 (midnight GMT)."""
        start, end = local_date_to_utc_bounds('2026-10-25', 'Europe/London')
        self.assertEqual(start, '2026-10-24T23:00:00')
        self.assertEqual(end,   '2026-10-26T00:00:00')
        # Verify it's exactly 25 hours
        from datetime import datetime
        s = datetime.fromisoformat(start)
        e = datetime.fromisoformat(end)
        self.assertEqual((e - s).total_seconds(), 25 * 3600)

    def test_day_before_bst_start(self):
        """28 Mar 2026 is still GMT — full 24 hours."""
        start, end = local_date_to_utc_bounds('2026-03-28', 'Europe/London')
        self.assertEqual(start, '2026-03-28T00:00:00')
        self.assertEqual(end,   '2026-03-29T00:00:00')
        from datetime import datetime
        self.assertEqual(
            (datetime.fromisoformat(end) - datetime.fromisoformat(start)).total_seconds(),
            24 * 3600
        )

    def test_day_after_bst_start(self):
        """30 Mar 2026 is BST — 24 hours in BST."""
        start, end = local_date_to_utc_bounds('2026-03-30', 'Europe/London')
        self.assertEqual(start, '2026-03-29T23:00:00')
        self.assertEqual(end,   '2026-03-30T23:00:00')
        from datetime import datetime
        self.assertEqual(
            (datetime.fromisoformat(end) - datetime.fromisoformat(start)).total_seconds(),
            24 * 3600
        )

    def test_utc_timezone(self):
        """UTC timezone: always midnight-to-midnight."""
        start, end = local_date_to_utc_bounds('2026-04-15', 'UTC')
        self.assertEqual(start, '2026-04-15T00:00:00')
        self.assertEqual(end,   '2026-04-16T00:00:00')

    def test_invalid_timezone_falls_back_to_utc(self):
        """Invalid timezone falls back to UTC gracefully."""
        start, end = local_date_to_utc_bounds('2026-04-15', 'Invalid/Zone')
        self.assertEqual(start, '2026-04-15T00:00:00')
        self.assertEqual(end,   '2026-04-16T00:00:00')

    def test_year_boundary(self):
        """New Year's Eve GMT."""
        start, end = local_date_to_utc_bounds('2025-12-31', 'Europe/London')
        self.assertEqual(start, '2025-12-31T00:00:00')
        self.assertEqual(end,   '2026-01-01T00:00:00')

    def test_leap_day(self):
        """Feb 29 leap day handled correctly."""
        start, end = local_date_to_utc_bounds('2028-02-29', 'Europe/London')
        self.assertEqual(start, '2028-02-29T00:00:00')
        self.assertEqual(end,   '2028-03-01T00:00:00')

    def test_no_tzinfo_in_output(self):
        """Output strings must be naive ISO — no Z, no +00:00."""
        start, end = local_date_to_utc_bounds('2026-06-15', 'Europe/London')
        self.assertNotIn('Z', start)
        self.assertNotIn('+', start)
        self.assertNotIn('Z', end)
        self.assertNotIn('+', end)

    def test_consecutive_days_are_contiguous(self):
        """End of day N must equal start of day N+1 — no gaps or overlaps."""
        _, end_14   = local_date_to_utc_bounds('2026-04-14', 'Europe/London')
        start_15, _ = local_date_to_utc_bounds('2026-04-15', 'Europe/London')
        self.assertEqual(end_14, start_15)

    def test_consecutive_days_across_bst_start(self):
        """No gap between 28 Mar (GMT) and 29 Mar (BST start).
        Mar 28 ends 2026-03-29T00:00:00, Mar 29 starts 2026-03-29T00:00:00."""
        _, end_28   = local_date_to_utc_bounds('2026-03-28', 'Europe/London')
        start_29, _ = local_date_to_utc_bounds('2026-03-29', 'Europe/London')
        self.assertEqual(end_28, start_29)

    def test_consecutive_days_across_bst_end(self):
        """No gap between 25 Oct (BST end) and 26 Oct (GMT)."""
        _, end_25   = local_date_to_utc_bounds('2026-10-25', 'Europe/London')
        start_26, _ = local_date_to_utc_bounds('2026-10-26', 'Europe/London')
        self.assertEqual(end_25, start_26)


class TestLocalDateRangeToUtcBounds(unittest.TestCase):
    """Tests for local_date_range_to_utc_bounds() — multi-day ranges."""

    def test_single_day_gmt(self):
        """Single day range in GMT."""
        start, end = local_date_range_to_utc_bounds(
            '2026-01-15', '2026-01-15', 'Europe/London'
        )
        self.assertEqual(start, '2026-01-15T00:00:00')
        self.assertEqual(end,   '2026-01-16T00:00:00')

    def test_single_day_bst(self):
        """Single day range in BST."""
        start, end = local_date_range_to_utc_bounds(
            '2026-06-15', '2026-06-15', 'Europe/London'
        )
        self.assertEqual(start, '2026-06-14T23:00:00')
        self.assertEqual(end,   '2026-06-15T23:00:00')

    def test_multi_day_gmt(self):
        """Three GMT days: covers exactly 72 hours."""
        start, end = local_date_range_to_utc_bounds(
            '2026-01-15', '2026-01-17', 'Europe/London'
        )
        self.assertEqual(start, '2026-01-15T00:00:00')
        self.assertEqual(end,   '2026-01-18T00:00:00')
        from datetime import datetime
        self.assertEqual(
            (datetime.fromisoformat(end) - datetime.fromisoformat(start)).total_seconds(),
            3 * 24 * 3600
        )

    def test_range_straddling_bst_start(self):
        """
        Billing period Mar 3 → Apr 2 straddles BST start (Mar 29).
        Start (Mar 3) is GMT, end (Apr 2) is BST.
        UTC start should be midnight GMT, UTC end should be 23:00 UTC (Apr 2 BST midnight).
        """
        start, end = local_date_range_to_utc_bounds(
            '2026-03-03', '2026-04-02', 'Europe/London'
        )
        self.assertEqual(start, '2026-03-03T00:00:00')  # GMT — no offset
        self.assertEqual(end,   '2026-04-02T23:00:00')  # BST — 1hr offset

    def test_range_straddling_bst_end(self):
        """
        Period straddling BST end (Oct 25).
        Start (Oct 3) is BST, end (Nov 2) is GMT.
        """
        start, end = local_date_range_to_utc_bounds(
            '2026-10-03', '2026-11-02', 'Europe/London'
        )
        self.assertEqual(start, '2026-10-02T23:00:00')  # BST start
        self.assertEqual(end,   '2026-11-03T00:00:00')  # GMT end

    def test_bst_start_day_included_in_range(self):
        """
        Range Mar 28 → Mar 30 includes the 23-hour BST start day.
        Total = 24 + 23 + 24 = 71 hours.
        """
        start, end = local_date_range_to_utc_bounds(
            '2026-03-28', '2026-03-30', 'Europe/London'
        )
        self.assertEqual(start, '2026-03-28T00:00:00')
        self.assertEqual(end,   '2026-03-30T23:00:00')
        from datetime import datetime
        hours = (datetime.fromisoformat(end) - datetime.fromisoformat(start)).total_seconds() / 3600
        self.assertAlmostEqual(hours, 71, places=1)

    def test_bst_end_day_included_in_range(self):
        """
        Range Oct 24 → Oct 26 includes the 25-hour BST end day.
        Oct 24 (BST, 24h) + Oct 25 (transition, 25h) + Oct 26 (GMT, 24h) = 73 hours.
        """
        start, end = local_date_range_to_utc_bounds(
            '2026-10-24', '2026-10-26', 'Europe/London'
        )
        self.assertEqual(start, '2026-10-23T23:00:00')
        self.assertEqual(end,   '2026-10-27T00:00:00')
        from datetime import datetime
        hours = (datetime.fromisoformat(end) - datetime.fromisoformat(start)).total_seconds() / 3600
        self.assertEqual(hours, 73)

    def test_blocks_in_bst_transition_hour_not_double_counted(self):
        """
        On BST end day, the 01:00 BST hour occurs twice (01:00 BST = 00:00 UTC,
        01:00 GMT = 01:00 UTC). Both UTC blocks should be within the UTC bounds
        for Oct 25 and not duplicated.
        """
        start, end = local_date_to_utc_bounds('2026-10-25', 'Europe/London')
        # 00:00 UTC (= 01:00 BST, first occurrence) should be >= start
        self.assertGreaterEqual('2026-10-25T00:00:00', start)
        self.assertLess('2026-10-25T00:00:00', end)
        # 01:00 UTC (= 01:00 GMT, second occurrence) should also be in range
        self.assertGreaterEqual('2026-10-25T01:00:00', start)
        self.assertLess('2026-10-25T01:00:00', end)

    def test_utc_range_same_as_single_day(self):
        """UTC timezone: range bounds identical to single day bounds."""
        start_r, end_r = local_date_range_to_utc_bounds(
            '2026-04-15', '2026-04-15', 'UTC'
        )
        start_s, end_s = local_date_to_utc_bounds('2026-04-15', 'UTC')
        self.assertEqual(start_r, start_s)
        self.assertEqual(end_r,   end_s)
# ─────────────────────────────────────────────────────────────────────────────
# Generation Mix (2.8.0)
# ─────────────────────────────────────────────────────────────────────────────

class TestGenerationMix(unittest.TestCase):
    """Tests for upsert_generation_mix and get_generation_mix_for_range."""

    def setUp(self):
        self.store = BlockStore(":memory:")
        with self.store._conn:
            self.store._conn.execute(
                "INSERT INTO config_periods (billing_day, block_minutes, timezone, "
                "currency_symbol, currency_code, effective_from) "
                "VALUES (1, 30, 'UTC', '£', 'GBP', '2026-01-01')"
            )
            self.cp_id = self.store._conn.execute(
                "SELECT id FROM config_periods LIMIT 1"
            ).fetchone()[0]

    def _insert_block(self, block_start, meter_id, imp_kwh, is_sub=False):
        with self.store._conn:
            if is_sub:
                self.store._conn.execute(
                    "INSERT OR IGNORE INTO meters (meter_id, config_period_id, is_sub_meter) "
                    "VALUES (?, ?, 1)", (meter_id, self.cp_id)
                )
            self.store._conn.execute(
                "INSERT INTO blocks (block_start, block_end, meter_id, config_period_id, "
                "imp_kwh, exp_kwh) VALUES (?, ?, ?, ?, ?, 0.0)",
                (block_start, block_start, meter_id, self.cp_id, imp_kwh)
            )
            return self.store._conn.execute(
                "SELECT id FROM blocks WHERE block_start=? AND meter_id=?",
                (block_start, meter_id)
            ).fetchone()[0]

    MIX_A = [{"fuel": "wind", "perc": 60.0}, {"fuel": "gas", "perc": 40.0}]
    MIX_B = [{"fuel": "wind", "perc": 20.0}, {"fuel": "gas", "perc": 80.0}]

    def test_upsert_and_retrieve(self):
        """Stored mix rows are retrievable via generation_mix table."""
        bid = self._insert_block("2026-04-01T00:00:00", "electricity_main", 10.0)
        self.store.upsert_generation_mix(bid, self.MIX_A)
        rows = self.store._conn.execute(
            "SELECT fuel, perc FROM generation_mix WHERE block_id=? ORDER BY fuel",
            (bid,)
        ).fetchall()
        self.assertEqual(len(rows), 2)
        fuels = {r["fuel"]: r["perc"] for r in rows}
        self.assertAlmostEqual(fuels["wind"], 60.0)
        self.assertAlmostEqual(fuels["gas"],  40.0)

    def test_upsert_replaces_existing(self):
        """Re-upserting same block_id replaces previous mix."""
        bid = self._insert_block("2026-04-01T00:00:00", "electricity_main", 10.0)
        self.store.upsert_generation_mix(bid, self.MIX_A)
        self.store.upsert_generation_mix(bid, self.MIX_B)
        rows = self.store._conn.execute(
            "SELECT fuel, perc FROM generation_mix WHERE block_id=? ORDER BY fuel",
            (bid,)
        ).fetchall()
        fuels = {r["fuel"]: r["perc"] for r in rows}
        self.assertAlmostEqual(fuels["wind"], 20.0)
        self.assertAlmostEqual(fuels["gas"],  80.0)

    def test_get_generation_mix_for_range_weighted_average(self):
        """Weighted average correctly weights by imp_kwh."""
        # Block 1: 10 kWh, mix A (60% wind, 40% gas)
        # Block 2: 30 kWh, mix B (20% wind, 80% gas)
        # Expected weighted wind: (10*60 + 30*20) / 40 = 1200/40 = 30%
        # Expected weighted gas:  (10*40 + 30*80) / 40 = 2800/40 = 70%
        bid1 = self._insert_block("2026-04-01T00:00:00", "electricity_main", 10.0)
        bid2 = self._insert_block("2026-04-01T00:30:00", "electricity_main", 30.0)
        self.store.upsert_generation_mix(bid1, self.MIX_A)
        self.store.upsert_generation_mix(bid2, self.MIX_B)
        result = self.store.get_generation_mix_for_range(
            "2026-04-01T00:00:00", "2026-04-02T00:00:00"
        )
        fuels = {r["fuel"]: r["perc"] for r in result}
        self.assertAlmostEqual(fuels["wind"], 30.0, places=1)
        self.assertAlmostEqual(fuels["gas"],  70.0, places=1)

    def test_get_generation_mix_empty_range(self):
        """Range with no blocks returns empty list."""
        result = self.store.get_generation_mix_for_range(
            "2025-01-01T00:00:00", "2025-01-02T00:00:00"
        )
        self.assertEqual(result, [])

    def test_get_generation_mix_main_meter_only(self):
        """Mix is only stored/retrieved for electricity_main, not sub-meters."""
        bid_main = self._insert_block("2026-04-01T00:00:00", "electricity_main", 10.0)
        bid_sub  = self._insert_block("2026-04-01T00:00:00", "ev_charger", 5.0, is_sub=True)
        self.store.upsert_generation_mix(bid_main, self.MIX_A)
        # Do NOT store mix for sub-meter (engine no longer does this)
        result = self.store.get_generation_mix_for_range(
            "2026-04-01T00:00:00", "2026-04-02T00:00:00",
            meter_id="electricity_main"
        )
        fuels = {r["fuel"]: r["perc"] for r in result}
        self.assertIn("wind", fuels)
        # Sub-meter query should return empty
        sub_result = self.store.get_generation_mix_for_range(
            "2026-04-01T00:00:00", "2026-04-02T00:00:00",
            meter_id="ev_charger"
        )
        self.assertEqual(sub_result, [])

    def test_migration_removes_sub_meter_mix_rows(self):
        """Opening a DB with sub-meter mix rows removes them on first open."""
        import tempfile, os
        tmp = tempfile.mktemp(suffix=".db")
        try:
            store = BlockStore(tmp)
            with store._conn:
                store._conn.execute(
                    "INSERT INTO config_periods (billing_day, block_minutes, timezone, "
                    "currency_symbol, currency_code, effective_from) "
                    "VALUES (1, 30, 'UTC', '£', 'GBP', '2026-01-01')"
                )
                cp_id = store._conn.execute(
                    "SELECT id FROM config_periods LIMIT 1"
                ).fetchone()[0]
                # Insert main and sub-meter blocks
                store._conn.execute(
                    "INSERT INTO blocks (block_start, block_end, meter_id, config_period_id, "
                    "imp_kwh, exp_kwh) VALUES ('2026-04-01T00:00:00', '2026-04-01T00:30:00', "
                    "'electricity_main', ?, 10.0, 0.0)", (cp_id,)
                )
                store._conn.execute(
                    "INSERT OR IGNORE INTO meters (meter_id, config_period_id, is_sub_meter) "
                    "VALUES ('ev_charger', ?, 1)", (cp_id,)
                )
                store._conn.execute(
                    "INSERT INTO blocks (block_start, block_end, meter_id, config_period_id, "
                    "imp_kwh, exp_kwh) VALUES ('2026-04-01T00:00:00', '2026-04-01T00:30:00', "
                    "'ev_charger', ?, 5.0, 0.0)", (cp_id,)
                )
                main_id = store._conn.execute(
                    "SELECT id FROM blocks WHERE meter_id='electricity_main'"
                ).fetchone()[0]
                sub_id  = store._conn.execute(
                    "SELECT id FROM blocks WHERE meter_id='ev_charger'"
                ).fetchone()[0]
                # Manually insert sub-meter mix rows (simulating pre-2.8.0 data)
                store._conn.execute(
                    "INSERT INTO generation_mix (block_id, fuel, perc) VALUES (?, 'wind', 55.0)",
                    (main_id,)
                )
                store._conn.execute(
                    "INSERT INTO generation_mix (block_id, fuel, perc) VALUES (?, 'wind', 55.0)",
                    (sub_id,)
                )
            store._conn.close()

            # Re-open — migration should delete sub-meter rows
            store2 = BlockStore(tmp)
            count = store2._conn.execute(
                "SELECT COUNT(*) FROM generation_mix"
            ).fetchone()[0]
            self.assertEqual(count, 1, "Only main meter mix row should remain after migration")
            store2._conn.close()
        finally:
            if os.path.exists(tmp):
                os.remove(tmp)


# ─────────────────────────────────────────────────────────────────────────────
# mix_history table (2.8.1)
# ─────────────────────────────────────────────────────────────────────────────

class TestMixHistory(unittest.TestCase):
    """Tests for mix_history CI-tick resolution storage."""

    def setUp(self):
        self.store = BlockStore(":memory:")

    def test_upsert_and_get(self):
        """Stored mix is retrievable via get_mix_history."""
        from datetime import datetime, timezone, timedelta
        recent = (datetime.now(timezone.utc) - timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M")
        self.store.upsert_mix_history(recent, [
            {"fuel": "wind", "perc": 60.0},
            {"fuel": "gas",  "perc": 40.0},
        ])
        slots = self.store.get_mix_history(hours=48)
        self.assertEqual(len(slots), 1)
        self.assertEqual(slots[0]["captured_at"], recent)
        self.assertAlmostEqual(slots[0]["fuels"]["wind"], 60.0)
        self.assertAlmostEqual(slots[0]["fuels"]["gas"],  40.0)

    def test_upsert_replaces_existing(self):
        """Re-upserting same captured_at replaces previous perc values."""
        ts = "2026-05-01T06:30"
        self.store.upsert_mix_history(ts, [{"fuel": "wind", "perc": 60.0}])
        self.store.upsert_mix_history(ts, [{"fuel": "wind", "perc": 75.0}])
        slots = self.store.get_mix_history(hours=48*365)
        fuels = {s["fuels"]["wind"] for s in slots if s["captured_at"] == ts}
        self.assertIn(75.0, fuels)
        self.assertNotIn(60.0, fuels)

    def test_multiple_slots_ordered(self):
        """get_mix_history returns slots in chronological order."""
        from datetime import datetime, timezone, timedelta
        now = datetime.now(timezone.utc)
        for h in [3, 1, 2]:
            ts = (now - timedelta(hours=h)).strftime("%Y-%m-%dT%H:%M")
            self.store.upsert_mix_history(ts, [{"fuel": "wind", "perc": float(h * 10)}])
        slots = self.store.get_mix_history(hours=48)
        times = [s["captured_at"] for s in slots]
        self.assertEqual(times, sorted(times))

    def test_prune_removes_old_rows(self):
        """prune_mix_history removes rows older than the specified window."""
        # Insert one old slot (3 days ago) and one recent slot (1 hour ago)
        old_ts    = "2020-01-01T00:00"
        from datetime import datetime, timezone, timedelta
        recent_ts = (datetime.now(timezone.utc) - timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M")
        self.store.upsert_mix_history(old_ts,    [{"fuel": "wind", "perc": 50.0}])
        self.store.upsert_mix_history(recent_ts, [{"fuel": "wind", "perc": 70.0}])
        self.store.prune_mix_history(hours=48)
        count = self.store._conn.execute("SELECT COUNT(*) FROM mix_history").fetchone()[0]
        self.assertEqual(count, 1)
        remaining = self.store._conn.execute(
            "SELECT captured_at FROM mix_history"
        ).fetchone()[0]
        self.assertEqual(remaining, recent_ts)

    def test_get_mix_history_empty(self):
        """get_mix_history returns empty list when no data exists."""
        slots = self.store.get_mix_history(hours=48)
        self.assertEqual(slots, [])

    def test_migration_backfills_from_generation_mix(self):
        """mix_history is backfilled from generation_mix on first open when empty."""
        import tempfile, os
        tmp = tempfile.mktemp(suffix=".db")
        try:
            store = BlockStore(tmp)
            with store._conn:
                store._conn.execute(
                    "INSERT INTO config_periods (billing_day, block_minutes, timezone, "
                    "currency_symbol, currency_code, effective_from) "
                    "VALUES (1, 30, 'UTC', '£', 'GBP', '2026-01-01')"
                )
                cp_id = store._conn.execute(
                    "SELECT id FROM config_periods LIMIT 1"
                ).fetchone()[0]
                from datetime import datetime, timezone, timedelta
                recent = (datetime.now(timezone.utc) - timedelta(hours=1)).strftime(
                    "%Y-%m-%dT%H:%M:%S"
                )
                store._conn.execute(
                    "INSERT INTO blocks (block_start, block_end, meter_id, config_period_id, "
                    "imp_kwh, exp_kwh) VALUES (?, ?, 'electricity_main', ?, 10.0, 0.0)",
                    (recent, recent, cp_id)
                )
                bid = store._conn.execute(
                    "SELECT id FROM blocks WHERE meter_id='electricity_main'"
                ).fetchone()[0]
                store._conn.execute(
                    "INSERT INTO generation_mix (block_id, fuel, perc) VALUES (?, 'wind', 65.0)",
                    (bid,)
                )
            store._conn.close()

            # Re-open — migration should backfill mix_history from generation_mix
            store2 = BlockStore(tmp)
            count = store2._conn.execute(
                "SELECT COUNT(*) FROM mix_history"
            ).fetchone()[0]
            self.assertGreater(count, 0, "mix_history should be backfilled from generation_mix")
            store2._conn.close()
        finally:
            if os.path.exists(tmp):
                os.remove(tmp)


# ─────────────────────────────────────────────────────────────────────────────
# Device retirement (2.9.0)
# ─────────────────────────────────────────────────────────────────────────────

class TestDeviceRetirement(unittest.TestCase):
    """Tests for sub-meter retirement functionality."""

    def setUp(self):
        self.store = BlockStore(":memory:")
        # Add a config period and a sub-meter
        with self.store._conn:
            self.store._conn.execute(
                "INSERT INTO config_periods (billing_day, block_minutes, timezone, "
                "currency_symbol, currency_code, effective_from) "
                "VALUES (1, 30, 'UTC', '£', 'GBP', '2026-01-01')"
            )
            cp_id = self.store._conn.execute(
                "SELECT id FROM config_periods LIMIT 1"
            ).fetchone()[0]
            self.store._conn.execute(
                "INSERT INTO meters (config_period_id, meter_id, is_sub_meter, "
                "device_label, meter_type) VALUES (?, 'ev_charger', 1, 'Zappi', 'ev')",
                (cp_id,)
            )

    def test_retire_meter_sets_retired_at(self):
        """retire_meter sets the retired_at date on the meter."""
        self.store.retire_meter('ev_charger', '2026-05-01', 'EV sold')
        row = self.store._conn.execute(
            "SELECT retired_at, retired_reason FROM meters WHERE meter_id='ev_charger'"
        ).fetchone()
        self.assertEqual(row['retired_at'], '2026-05-01')
        self.assertEqual(row['retired_reason'], 'EV sold')

    def test_unretire_meter_clears_retired_at(self):
        """unretire_meter clears retired_at and retired_reason."""
        self.store.retire_meter('ev_charger', '2026-05-01')
        self.store.unretire_meter('ev_charger')
        row = self.store._conn.execute(
            "SELECT retired_at FROM meters WHERE meter_id='ev_charger'"
        ).fetchone()
        self.assertIsNone(row['retired_at'])

    def test_is_meter_retired_before_date(self):
        """is_meter_retired returns False before the retirement date."""
        self.store.retire_meter('ev_charger', '2026-06-01')
        self.assertFalse(self.store.is_meter_retired('ev_charger', as_of='2026-05-31'))

    def test_is_meter_retired_on_date(self):
        """is_meter_retired returns True on and after the retirement date."""
        self.store.retire_meter('ev_charger', '2026-05-01')
        self.assertTrue(self.store.is_meter_retired('ev_charger', as_of='2026-05-01'))
        self.assertTrue(self.store.is_meter_retired('ev_charger', as_of='2026-12-31'))

    def test_is_meter_retired_not_retired(self):
        """is_meter_retired returns False for an active meter."""
        self.assertFalse(self.store.is_meter_retired('ev_charger'))

    def test_get_retired_meters(self):
        """get_retired_meters returns only meters with retirement dates."""
        self.store.retire_meter('ev_charger', '2026-05-01', 'EV sold')
        retired = self.store.get_retired_meters()
        self.assertEqual(len(retired), 1)
        self.assertEqual(retired[0]['meter_id'], 'ev_charger')
        self.assertEqual(retired[0]['retired_at'], '2026-05-01')

    def test_get_retired_meters_empty(self):
        """get_retired_meters returns empty list when no meters are retired."""
        retired = self.store.get_retired_meters()
        self.assertEqual(retired, [])

    def test_unretire_raises_on_sensor_conflict(self):
        """unretire_meter raises ValueError if the sensor is already in use by an active meter."""
        cp_id = self.store._conn.execute(
            "SELECT id FROM config_periods LIMIT 1"
        ).fetchone()[0]
        # Add a second active sub-meter using the same read_sensor
        self.store._conn.execute(
            "INSERT INTO meters (config_period_id, meter_id, is_sub_meter, device_label, meter_type) "
            "VALUES (?, 'ev_charger_2', 1, 'Zappi 2', 'ev')", (cp_id,)
        )
        # Give both meters the same read_sensor in meter_channels
        m1_id = self.store._conn.execute(
            "SELECT id FROM meters WHERE meter_id='ev_charger'"
        ).fetchone()[0]
        m2_id = self.store._conn.execute(
            "SELECT id FROM meters WHERE meter_id='ev_charger_2'"
        ).fetchone()[0]
        self.store._conn.execute(
            "INSERT OR REPLACE INTO meter_channels (meter_id, channel, read_sensor) "
            "VALUES (?, 'import', 'sensor.zappi_kwh')", (m1_id,)
        )
        self.store._conn.execute(
            "INSERT OR REPLACE INTO meter_channels (meter_id, channel, read_sensor) "
            "VALUES (?, 'import', 'sensor.zappi_kwh')", (m2_id,)
        )
        self.store._conn.commit()

        # Retire ev_charger
        self.store.retire_meter('ev_charger', '2026-05-01')
        # ev_charger_2 is active and using the same sensor
        # Attempting to unretire ev_charger should raise ValueError
        with self.assertRaises(ValueError) as ctx:
            self.store.unretire_meter('ev_charger')
        self.assertIn('sensor.zappi_kwh', str(ctx.exception))
        self.assertIn('ev_charger_2', str(ctx.exception))

    def test_retire_only_affects_sub_meters(self):
        """retire_meter only updates sub-meters (is_sub_meter=1)."""
        # Add a main meter
        cp_id = self.store._conn.execute(
            "SELECT id FROM config_periods LIMIT 1"
        ).fetchone()[0]
        self.store._conn.execute(
            "INSERT INTO meters (config_period_id, meter_id, is_sub_meter) "
            "VALUES (?, 'electricity_main', 0)", (cp_id,)
        )
        # Try to retire it
        self.store.retire_meter('electricity_main', '2026-05-01')
        row = self.store._conn.execute(
            "SELECT retired_at FROM meters WHERE meter_id='electricity_main'"
        ).fetchone()
        # Should NOT be retired (is_sub_meter = 0)
        self.assertIsNone(row['retired_at'])

    def test_retire_clears_current_reads(self):
        """retire_meter deletes current_reads entries for the retired meter."""
        # Seed some current_reads entries for the sub-meter
        self.store._conn.execute(
            """INSERT INTO current_reads
               (captured_at, meter_id, channel, channel_type, value, standing_charge, is_gap_seed)
               VALUES ('2026-05-01T12:00:00', 'ev_charger', 'import', 'read', 100.0, 0.0, 0)"""
        )
        self.store._conn.execute(
            """INSERT INTO current_reads
               (captured_at, meter_id, channel, channel_type, value, standing_charge, is_gap_seed)
               VALUES ('2026-05-01T12:00:00', 'ev_charger', 'import', 'rate', 0.30, NULL, 0)"""
        )
        self.store._conn.commit()

        # Verify they exist before retirement
        count_before = self.store._conn.execute(
            "SELECT COUNT(*) FROM current_reads WHERE meter_id='ev_charger'"
        ).fetchone()[0]
        self.assertEqual(count_before, 2)

        # Retire the meter
        self.store.retire_meter('ev_charger', '2026-05-01')

        # current_reads should be cleared for this meter
        count_after = self.store._conn.execute(
            "SELECT COUNT(*) FROM current_reads WHERE meter_id='ev_charger'"
        ).fetchone()[0]
        self.assertEqual(count_after, 0)

    def test_load_current_block_skips_retired_meter_reads(self):
        """load_current_block does not include reads for retired meters."""
        import json
        from datetime import datetime, timezone

        # Seed a current_block with a retirement date in the past
        self.store._conn.execute(
            """INSERT OR REPLACE INTO current_block
               (id, block_start, block_end, last_checkpoint, interpolated)
               VALUES (1, '2026-05-01T12:00:00', '2026-05-01T12:30:00',
                       '2026-05-01T12:00:00', 0)"""
        )
        # Write current_reads for both an active meter and a retired meter
        self.store._conn.execute(
            """INSERT INTO current_reads
               (captured_at, meter_id, channel, channel_type, value, standing_charge, is_gap_seed)
               VALUES ('2026-05-01T12:00:00', 'electricity_main', 'import', 'read', 1000.0, 0.5, 0)"""
        )
        self.store._conn.execute(
            """INSERT INTO current_reads
               (captured_at, meter_id, channel, channel_type, value, standing_charge, is_gap_seed)
               VALUES ('2026-05-01T12:00:00', 'ev_charger', 'import', 'rate', 0.30, NULL, 0)"""
        )
        self.store._conn.commit()

        # Retire the EV meter before the block date
        self.store.retire_meter('ev_charger', '2026-04-30')

        # Re-seed the rate entry (retire_meter clears current_reads, so add it back to test loading)
        self.store._conn.execute(
            """INSERT INTO current_reads
               (captured_at, meter_id, channel, channel_type, value, standing_charge, is_gap_seed)
               VALUES ('2026-05-01T12:00:00', 'ev_charger', 'import', 'rate', 0.30, NULL, 0)"""
        )
        self.store._conn.commit()

        cb = self.store.load_current_block()
        self.assertIn('electricity_main', cb.get('meters', {}))
        self.assertNotIn('ev_charger', cb.get('meters', {}))

class TestProvisionalBlocks(unittest.TestCase):
    """Tests for 2.10.0 imp_provisional column and get_provisional_sub_meter_blocks."""

    def setUp(self):
        self.store = BlockStore(":memory:")
        with self.store._conn:
            self.store._conn.execute(
                "INSERT INTO config_periods (billing_day, block_minutes, timezone, "
                "currency_symbol, currency_code, effective_from) "
                "VALUES (1, 30, 'UTC', '£', 'GBP', '2026-01-01')"
            )
            cp_id = self.store._conn.execute(
                "SELECT id FROM config_periods LIMIT 1"
            ).fetchone()[0]
            self.store._conn.execute(
                "INSERT INTO meters (config_period_id, meter_id, is_sub_meter, "
                "device_label, meter_type) "
                "VALUES (?, 'electricity_main', 0, 'Main', 'electricity')",
                (cp_id,)
            )
            self.store._conn.execute(
                "INSERT INTO meters (config_period_id, meter_id, is_sub_meter, "
                "parent_meter_id, device_label, meter_type) "
                "VALUES (?, 'ev_charger', 1, 'electricity_main', 'Zappi', 'ev')",
                (cp_id,)
            )

    def _make_block(self, provisional=False,
                    block_start="2026-05-01T09:00:00",
                    block_end="2026-05-01T09:30:00"):
        ev_mb = {
            "meta": {"sub_meter": True, "parent_meter": "electricity_main"},
            "channels": {
                "import": {
                    "kwh": 0.5, "kwh_grid": 0.5, "rate": 0.30,
                    "cost": 0.15, "read_start": 100.0, "read_end": 100.5,
                },
            },
            "standing_charge": 0.0,
        }
        if provisional:
            ev_mb["provisional"] = True
        return {
            "start": block_start, "end": block_end,
            "interpolated": False,
            "meters": {
                "electricity_main": {
                    "meta": {"sub_meter": False},
                    "channels": {
                        "import": {
                            "kwh": 2.0, "kwh_remainder": 1.5,
                            "rate": 0.30, "cost": 0.60,
                            "cost_remainder": 0.45,
                            "read_start": 1000.0, "read_end": 1002.0,
                        },
                        "export": {
                            "kwh": 0.0, "rate": 0.12, "cost": 0.0,
                            "read_start": 0.0, "read_end": 0.0,
                        },
                    },
                    "standing_charge": 0.45,
                },
                "ev_charger": ev_mb,
            },
        }

    # ── schema ────────────────────────────────────────────────────────────────

    def test_imp_provisional_column_exists(self):
        """imp_provisional column must exist on the blocks table."""
        cols = [row[1] for row in self.store._conn.execute(
            "PRAGMA table_info(blocks)"
        ).fetchall()]
        self.assertIn("imp_provisional", cols)

    def test_imp_provisional_default_zero(self):
        """imp_provisional defaults to 0 for a non-provisional block."""
        self.store.append_block(self._make_block(provisional=False))
        row = self.store._conn.execute(
            "SELECT imp_provisional FROM blocks WHERE meter_id='ev_charger'"
        ).fetchone()
        self.assertIsNotNone(row)
        self.assertEqual(row[0], 0)

    def test_imp_provisional_set_to_one(self):
        """imp_provisional is stored as 1 for a provisional block."""
        self.store.append_block(self._make_block(provisional=True))
        row = self.store._conn.execute(
            "SELECT imp_provisional FROM blocks WHERE meter_id='ev_charger'"
        ).fetchone()
        self.assertIsNotNone(row)
        self.assertEqual(row[0], 1)

    def test_main_meter_never_provisional(self):
        """Main meter rows always have imp_provisional=0 regardless of sub-meter flag."""
        self.store.append_block(self._make_block(provisional=True))
        row = self.store._conn.execute(
            "SELECT imp_provisional FROM blocks WHERE meter_id='electricity_main'"
        ).fetchone()
        self.assertIsNotNone(row)
        self.assertEqual(row[0], 0)

    # ── get_provisional_sub_meter_blocks ──────────────────────────────────────

    def test_returns_empty_when_no_provisional_blocks(self):
        """Returns empty list when no provisional sub-meter blocks exist."""
        self.store.append_block(self._make_block(provisional=False))
        result = self.store.get_provisional_sub_meter_blocks()
        self.assertEqual(result, [])

    def test_returns_provisional_block(self):
        """Returns the provisional block with provisional=True on the meter."""
        self.store.append_block(self._make_block(provisional=True))
        result = self.store.get_provisional_sub_meter_blocks()
        self.assertEqual(len(result), 1)
        ev = result[0]["meters"].get("ev_charger")
        self.assertIsNotNone(ev)
        self.assertTrue(ev.get("provisional"))

    def test_returns_only_most_recent_provisional_per_meter(self):
        """Returns only the most recent provisional block per sub-meter."""
        blk1 = self._make_block(provisional=True,
                                 block_start="2026-05-01T09:00:00",
                                 block_end="2026-05-01T09:30:00")
        blk2 = self._make_block(provisional=True,
                                 block_start="2026-05-01T09:30:00",
                                 block_end="2026-05-01T10:00:00")
        self.store.append_block(blk1)
        self.store.append_block(blk2)
        result = self.store.get_provisional_sub_meter_blocks()
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["start"], "2026-05-01T09:30:00")

    def test_amendment_clears_provisional_flag(self):
        """append_block_replace with provisional=False clears imp_provisional."""
        self.store.append_block(self._make_block(provisional=True))
        # Confirm it's provisional
        self.assertEqual(len(self.store.get_provisional_sub_meter_blocks()), 1)
        # Amend — same block, provisional cleared
        self.store.append_block_replace(self._make_block(provisional=False))
        self.assertEqual(len(self.store.get_provisional_sub_meter_blocks()), 0)

    def test_row_to_block_sets_provisional_true(self):
        """Loading a provisional block from DB sets meter_block['provisional']=True."""
        self.store.append_block(self._make_block(provisional=True))
        import datetime
        blocks = self.store.get_blocks_for_range(
            datetime.datetime(2026, 5, 1, 9, 0),
            datetime.datetime(2026, 5, 1, 9, 30),
        )
        self.assertTrue(blocks)
        ev = blocks[0]["meters"].get("ev_charger")
        self.assertIsNotNone(ev)
        self.assertTrue(ev.get("provisional"))

    def test_row_to_block_no_provisional_key_when_false(self):
        """Loading a non-provisional block does not set provisional key."""
        self.store.append_block(self._make_block(provisional=False))
        import datetime
        blocks = self.store.get_blocks_for_range(
            datetime.datetime(2026, 5, 1, 9, 0),
            datetime.datetime(2026, 5, 1, 9, 30),
        )
        self.assertTrue(blocks)
        ev = blocks[0]["meters"].get("ev_charger")
        self.assertIsNotNone(ev)
        self.assertFalse(ev.get("provisional", False))

    def test_returns_empty_when_no_provisional_blocks_exist_at_all(self):
        """get_provisional_sub_meter_blocks returns [] on a fresh store."""
        result = self.store.get_provisional_sub_meter_blocks()
        self.assertEqual(result, [])

class TestDeleteKrakenState(unittest.TestCase):
    """delete_kraken_state removes a marker entirely (used by disconnect to wipe
    API-derived progress state), without disturbing other keys."""

    def setUp(self):
        import sys, types
        eio = types.ModuleType("energy_engine_io"); eio.load_json = lambda *a, **kw: {}
        sys.modules.setdefault("energy_engine_io", eio)
        self.store = BlockStore(":memory:")

    def tearDown(self):
        try:
            self.store.close()
        except Exception:
            pass

    def test_delete_removes_key(self):
        self.store.set_kraken_state("last_poll_utc", "2026-06-10T06:00:00Z")
        self.assertEqual(self.store.get_kraken_state("last_poll_utc"), "2026-06-10T06:00:00Z")
        self.store.delete_kraken_state("last_poll_utc")
        self.assertIsNone(self.store.get_kraken_state("last_poll_utc"))

    def test_delete_absent_key_is_noop(self):
        # Must not raise on a key that was never set.
        self.store.delete_kraken_state("never_set")
        self.assertIsNone(self.store.get_kraken_state("never_set"))

    def test_delete_leaves_other_keys(self):
        self.store.set_kraken_state("billing_source", "dcc")
        self.store.set_kraken_state("last_poll_utc", "x")
        self.store.delete_kraken_state("last_poll_utc")
        self.assertIsNone(self.store.get_kraken_state("last_poll_utc"))
        self.assertEqual(self.store.get_kraken_state("billing_source"), "dcc")


from block_store import IMPORTED_SOURCE_API, IMPORTED_SOURCE_CSV


class TestHistoricalImportFoundation(unittest.TestCase):
    """3.5.0 block-store groundwork: derivation table + block link, imported
    source flags, and the reconstructed-history delete filter."""

    def setUp(self):
        import sys, types
        eio = types.ModuleType("energy_engine_io"); eio.load_json = lambda *a, **kw: {}
        sys.modules.setdefault("energy_engine_io", eio)
        self.store = BlockStore(":memory:")
        self.store.insert_config_period({"meters": {"electricity_main": {"meta": {
            "billing_day": 1, "block_minutes": 30, "timezone": "UTC",
            "currency_symbol": "£", "currency_code": "GBP", "site": "Home"}}}})
        self.cp_id = self.store._conn.execute(
            "SELECT id FROM config_periods LIMIT 1").fetchone()["id"]

    def _insert_block(self, start, *, meter_id="electricity_main", source=None):
        self.store._conn.execute(
            """INSERT INTO blocks (block_start, block_end, meter_id,
                 config_period_id, interpolated, imp_kwh, imp_cost, source)
               VALUES (?,?,?,?,0,1.0,0.07,?)""",
            (start, start, meter_id, self.cp_id, source))
        self.store._conn.commit()

    # ── schema ────────────────────────────────────────────────────────────
    def test_schema_has_link_and_table(self):
        cols = {r[1] for r in self.store._conn.execute(
            "PRAGMA table_info(blocks)").fetchall()}
        self.assertIn("derivation_id", cols)
        t = self.store._conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' "
            "AND name='historical_derivation'").fetchone()
        self.assertIsNotNone(t)

    # ── derivation CRUD ─────────────────────────────────────────────────────
    def test_insert_get_roundtrip_json(self):
        did = self.store.insert_historical_derivation(
            "device_attribution", "2024-07-01T00:00:00", "2025-01-01T00:00:00",
            subject="zappi_ev", sensor_ids=["sensor.a", "sensor.b"],
            sensor_kind="energy_total", params={"eps": 0.02},
            source=IMPORTED_SOURCE_CSV)
        d = self.store.get_historical_derivation(did)
        self.assertEqual(d["scope"], "device_attribution")
        self.assertEqual(d["sensor_ids"], ["sensor.a", "sensor.b"])   # JSON decoded
        self.assertEqual(d["params"], {"eps": 0.02})
        self.assertIsNone(d["superseded_by"])

    def test_invalid_scope_raises(self):
        with self.assertRaises(ValueError):
            self.store.insert_historical_derivation("nonsense", "a", "b")

    def test_supersede_hides_from_current(self):
        old = self.store.insert_historical_derivation(
            "rate", "2024-01-01", "2024-06-01", subject="import", derived_value=7.0)
        new = self.store.insert_historical_derivation(
            "rate", "2024-01-01", "2024-06-01", subject="import", derived_value=7.5)
        self.store.supersede_historical_derivation(old, new)
        current = self.store.list_historical_derivations(scope="rate", current_only=True)
        self.assertEqual([d["id"] for d in current], [new])
        allrows = self.store.list_historical_derivations(scope="rate", current_only=False)
        self.assertEqual({d["id"] for d in allrows}, {old, new})   # audit trail kept

    # ── block tagging ───────────────────────────────────────────────────────
    def test_tag_blocks_over_span(self):
        self._insert_block("2024-07-01T00:00:00", source=IMPORTED_SOURCE_CSV)
        self._insert_block("2024-07-01T00:30:00", source=IMPORTED_SOURCE_CSV)
        self._insert_block("2025-02-01T00:00:00", source=IMPORTED_SOURCE_CSV)   # outside span
        did = self.store.insert_historical_derivation(
            "rate", "2024-07-01T00:00:00", "2025-01-01T00:00:00", subject="import")
        n = self.store.tag_blocks_with_derivation(
            did, "electricity_main", "2024-07-01T00:00:00", "2025-01-01T00:00:00")
        self.assertEqual(n, 2)
        tagged = self.store._conn.execute(
            "SELECT COUNT(*) FROM blocks WHERE derivation_id = ?", (did,)).fetchone()[0]
        self.assertEqual(tagged, 2)

    def test_imported_blocks_skip_carbon_backfill(self):
        # A native NULL-carbon block IS a carbon-backfill candidate; a
        # reconstructed one is NOT (skips the expensive CI backfill).
        self._insert_block("2024-07-01T00:00:00", source=None)
        self._insert_block("2024-07-01T00:30:00", source=IMPORTED_SOURCE_CSV)
        starts = self.store.get_block_starts_missing_carbon()
        self.assertIn("2024-07-01T00:00:00", starts)
        self.assertNotIn("2024-07-01T00:30:00", starts)
        self.assertEqual(self.store.get_missing_carbon_date_range(),
                         ("2024-07-01T00:00:00", "2024-07-01T00:00:00"))

    def test_bulk_upsert_writes_and_merges(self):
        imp = [{"start": "2024-07-01T00:00:00", "kwh": 1.0, "rate": 0.07,
                "cost": 0.07, "standing": 0.53},
               {"start": "2024-07-01T00:30:00", "kwh": 1.0, "rate": 0.07,
                "cost": 0.07, "standing": 0.53}]
        n = self.store.upsert_imported_blocks(
            imp, "electricity_main", "import", source=IMPORTED_SOURCE_API)
        self.assertEqual(n, 2)
        # Export merges onto the same rows (no new rows).
        exp = [{"start": "2024-07-01T00:00:00", "kwh": 0.5, "rate": 0.15,
                "cost": 0.075, "standing": None}]
        self.store.upsert_imported_blocks(
            exp, "electricity_main", "export", source=IMPORTED_SOURCE_API)
        rows = self.store._conn.execute(
            "SELECT imp_cost, exp_cost FROM blocks WHERE source='imported_api' "
            "ORDER BY block_start").fetchall()
        self.assertEqual(len(rows), 2)                       # export merged, not added
        self.assertAlmostEqual(rows[0]["imp_cost"], 0.07, places=4)
        self.assertAlmostEqual(rows[0]["exp_cost"], 0.075, places=4)

    # ── reconstructed-history delete filter ─────────────────────────────────
    def test_reconstructed_only_delete_spares_native(self):
        self._insert_block("2025-01-15T10:00:00", source=None)                 # live
        self._insert_block("2025-01-15T11:00:00", source="kraken_api")         # native
        self._insert_block("2025-01-15T12:00:00", source=IMPORTED_SOURCE_API)  # imported
        self._insert_block("2025-01-15T13:00:00", source=IMPORTED_SOURCE_CSV)  # imported
        prev = self.store.count_blocks_for_date_range(
            "2025-01-15", "2025-01-15", tz_name="UTC", reconstructed_only=True)
        self.assertEqual(prev["blocks"], 2)
        res = self.store.delete_blocks_for_date_range(
            "2025-01-15", "2025-01-15", tz_name="UTC", reconstructed_only=True)
        self.assertEqual(res["deleted"], 2)
        srcs = {r[0] for r in self.store._conn.execute(
            "SELECT source FROM blocks").fetchall()}
        self.assertEqual(srcs, {None, "kraken_api"})   # native survive

    def test_default_delete_removes_all(self):
        self._insert_block("2025-02-10T10:00:00", source=None)
        self._insert_block("2025-02-10T11:00:00", source=IMPORTED_SOURCE_CSV)
        res = self.store.delete_blocks_for_date_range(
            "2025-02-10", "2025-02-10", tz_name="UTC")
        self.assertEqual(res["deleted"], 2)


class TestUnsettledHorizonFloor(unittest.TestCase):
    """The 'awaiting DCC settlement' count must floor at the settlement horizon so
    historic/reconstructed blocks (which never settle) don't inflate the badge."""

    def _store(self):
        store = BlockStore(":memory:")
        with store._conn:
            cp = store._conn.execute(
                "INSERT INTO config_periods (effective_from, billing_day, block_minutes, "
                "timezone, currency_symbol, currency_code) "
                "VALUES ('2024-01-01T00:00:00',1,30,'UTC','£','GBP')").lastrowid
            store._conn.execute(
                "INSERT INTO meters (config_period_id, meter_id, is_sub_meter) "
                "VALUES (?, 'electricity_main', 0)", (cp,))
            for bs in ("2024-07-01T00:00:00", "2026-07-20T00:00:00"):
                store._conn.execute(
                    "INSERT INTO blocks (block_start, block_end, meter_id, config_period_id, "
                    "imp_kwh, imp_kwh_api, source, finalised_from_cad) "
                    "VALUES (?, ?, 'electricity_main', ?, 1.0, NULL, NULL, 0)", (bs, bs, cp))
        return store

    def test_count_unfloored_counts_all(self):
        self.assertEqual(self._store().count_unsettled_blocks(), 2)

    def test_count_floored_excludes_old(self):
        self.assertEqual(
            self._store().count_unsettled_blocks(since_iso="2026-07-08T00:00:00"), 1)

    def test_list_floored_excludes_old(self):
        rows = self._store().get_unsettled_blocks(since_iso="2026-07-08T00:00:00")
        self.assertEqual([r["block_start"] for r in rows], ["2026-07-20T00:00:00"])


class TestSweepImplausibleSubBlocks(unittest.TestCase):
    """#307: a lost-opener sub-meter block booked its whole lifetime register as one
    interval (house_battery read_start=0 / read_end=6137 → 6137 kWh in a half-hour).
    The sweep clamps imp_kwh to the grid-bounded value, baselines the register,
    recomputes carbon, flags review — and leaves imp_kwh_grid / imp_cost alone."""

    def _store(self):
        s = BlockStore(":memory:")
        s.insert_config_period({"meters": {
            "electricity_main": {"meta": {
                "billing_day": 1, "block_minutes": 30, "timezone": "Europe/London",
                "currency_symbol": "£", "currency_code": "GBP", "site": "Home"}},
            "house_battery": {"meta": {
                "sub_meter": True, "parent_meter": "electricity_main",
                "device": "Solax Battery", "billing_day": 1, "block_minutes": 30,
                "timezone": "Europe/London", "currency_symbol": "£",
                "currency_code": "GBP"}},
        }})
        cp = s._conn.execute("SELECT id FROM config_periods LIMIT 1").fetchone()["id"]
        return s, cp

    def _ins(self, s, cp, start, imp_kwh, grid, rs, re, carbon,
             intensity=140.0, cost=0.0):
        s._conn.execute(
            "INSERT INTO blocks (block_start, block_end, meter_id, config_period_id, "
            "imp_kwh, imp_kwh_grid, imp_cost, imp_read_start, imp_read_end, carbon_g, "
            "carbon_intensity_g) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            (start, start, "house_battery", cp, imp_kwh, grid, cost, rs, re,
             carbon, intensity))
        s._conn.commit()

    def test_clamps_lost_opener_spike_and_fixes_carbon(self):
        s, cp = self._store()
        self._ins(s, cp, "2026-07-21T09:30:00", 6137.592, 0.0, 0.0, 6137.592, 859262.88)
        self._ins(s, cp, "2026-07-22T02:30:00", 2.844, 2.844, 6314.6, 6317.5, 423.8,
                  cost=0.156)                                   # normal — must not move
        prev = s.sweep_implausible_sub_blocks(dry_run=True)
        self.assertEqual(prev["count"], 1)
        self.assertEqual(prev["ceiling_kwh"], 60.0)            # 30 min × 120 kW
        self.assertAlmostEqual(s._conn.execute(
            "SELECT imp_kwh FROM blocks WHERE block_start='2026-07-21T09:30:00'"
        ).fetchone()[0], 6137.592)                             # dry run changed nothing

        res = s.sweep_implausible_sub_blocks(dry_run=False)
        self.assertTrue(res["applied"])
        r = s._conn.execute(
            "SELECT imp_kwh, imp_kwh_grid, imp_cost, imp_read_start, imp_read_end, "
            "carbon_g, needs_review FROM blocks "
            "WHERE block_start='2026-07-21T09:30:00'").fetchone()
        self.assertEqual(r["imp_kwh"], 0.0)                    # clamped to grid (0)
        self.assertEqual(r["imp_read_start"], 6137.592)        # baselined onto read_end
        self.assertEqual(r["carbon_g"], 0.0)                   # recomputed from 0 kWh
        self.assertEqual(r["needs_review"], 1)
        self.assertEqual(r["imp_kwh_grid"], 0.0)               # untouched
        self.assertEqual(r["imp_cost"], 0.0)                   # untouched
        n = s._conn.execute(
            "SELECT imp_kwh, needs_review FROM blocks "
            "WHERE block_start='2026-07-22T02:30:00'").fetchone()
        self.assertAlmostEqual(n["imp_kwh"], 2.844)            # normal block untouched
        self.assertEqual(n["needs_review"], 0)
        self.assertEqual(s.sweep_implausible_sub_blocks(dry_run=False)["count"], 0)  # idempotent

    def test_grid_bearing_spike_keeps_grid_cost_and_recomputes_carbon(self):
        # A lost-opener spike where the device DID draw some grid: the total is the
        # glitch but imp_kwh_grid (house-bounded) is real → keep grid + cost, clamp
        # the total to it, recompute carbon from the grid kWh.
        s, cp = self._store()
        self._ins(s, cp, "2026-05-01T00:30:00", 5000.0, 2.5, 0.0, 5000.0, 700000.0,
                  intensity=140.0, cost=0.20)
        s.sweep_implausible_sub_blocks(dry_run=False)
        r = s._conn.execute(
            "SELECT imp_kwh, imp_kwh_grid, imp_cost, carbon_g FROM blocks "
            "WHERE block_start='2026-05-01T00:30:00'").fetchone()
        self.assertAlmostEqual(r["imp_kwh"], 2.5)              # clamped to grid
        self.assertAlmostEqual(r["imp_kwh_grid"], 2.5)         # untouched
        self.assertAlmostEqual(r["imp_cost"], 0.20)            # untouched
        self.assertAlmostEqual(r["carbon_g"], round(2.5 * 140.0, 4))  # grid × intensity


class TestRecorderAttributionStore(unittest.TestCase):
    """The reversible device-attribution layer: recorder_attributed device rows are
    identifiable, scoped-deletable, and NEVER touch live/imported house totals."""

    def _store(self):
        s = BlockStore(":memory:")
        with s._conn:
            cp = s._conn.execute(
                "INSERT INTO config_periods (effective_from, billing_day, block_minutes, "
                "timezone, currency_symbol, currency_code) "
                "VALUES ('2025-01-01T00:00:00',1,30,'UTC','£','GBP')").lastrowid
            s._conn.execute(
                "INSERT INTO meters (config_period_id, meter_id, is_sub_meter) "
                "VALUES (?, 'electricity_main', 0)", (cp,))
            s._conn.execute(
                "INSERT INTO meters (config_period_id, meter_id, is_sub_meter, parent_meter_id) "
                "VALUES (?, 'ev_charger', 1, 'electricity_main')", (cp,))
            for bs, meter, kwh, src in (
                    ("2025-03-01T00:00:00", "electricity_main", 1.0, "imported_api"),
                    ("2025-03-01T00:30:00", "electricity_main", 1.0, "kraken_api"),
                    ("2025-03-01T00:00:00", "ev_charger", 0.5, "recorder_attributed"),
                    ("2025-03-01T00:30:00", "ev_charger", 0.5, "recorder_attributed")):
                s._conn.execute(
                    "INSERT INTO blocks (block_start, block_end, meter_id, config_period_id, "
                    "imp_kwh, source) VALUES (?,?,?,?,?,?)", (bs, bs, meter, cp, kwh, src))
        return s

    def test_count_and_scoped_delete_resolves_parent(self):
        s = self._store()
        c = s.count_recorder_attributed()
        self.assertEqual(c["total"], 2)
        self.assertEqual(c["meters"][0]["meter_id"], "ev_charger")
        res = s.delete_recorder_attributed(meter_id="ev_charger")
        self.assertEqual(res["deleted"], 2)
        self.assertIn("electricity_main", res["parents"])           # parent resolved
        self.assertEqual(res["from"], "2025-03-01T00:00:00")
        self.assertEqual(s.count_recorder_attributed()["total"], 0)

    def test_delete_never_touches_live_or_imported(self):
        s = self._store()
        s.delete_recorder_attributed()                              # all attributed
        srcs = [r["source"] for r in s._conn.execute(
            "SELECT DISTINCT source FROM blocks").fetchall()]
        self.assertNotIn("recorder_attributed", srcs)
        self.assertIn("imported_api", srcs)
        self.assertIn("kraken_api", srcs)                           # live untouched

    def test_run_ledger_add_and_remove(self):
        s = self._store()
        s.record_attribution_run({"run_id": "r1", "meter_id": "ev_charger",
                                  "sensor_ids": ["sensor.ev"]})
        s.record_attribution_run({"run_id": "r2", "meter_id": "house_battery"})
        self.assertEqual([r["run_id"] for r in s.get_attribution_runs()], ["r1", "r2"])
        s.remove_attribution_run("r1")
        self.assertEqual([r["run_id"] for r in s.get_attribution_runs()], ["r2"])

    def test_sum_meter_import_over_window(self):
        s = self._store()
        # Two electricity_main imported blocks, 1.0 kWh each, both on 2025-03-01.
        self.assertAlmostEqual(
            s.sum_meter_import_kwh("electricity_main",
                                   "2025-03-01T00:00:00", "2025-03-01T00:30:00"), 2.0)
        # A window that excludes the second block sums just the first.
        self.assertAlmostEqual(
            s.sum_meter_import_kwh("electricity_main",
                                   "2025-03-01T00:00:00", "2025-03-01T00:00:00"), 1.0)

    def test_live_coverage_seam_ignores_attributed(self):
        s = self._store()
        # ev_charger currently only has recorder_attributed rows → no live seam.
        self.assertIsNone(s.get_device_live_coverage_start("ev_charger"))
        # Add a real (live) ev_charger block AFTER the attributed ones.
        cp = s._conn.execute("SELECT id FROM config_periods LIMIT 1").fetchone()["id"]
        with s._conn:
            s._conn.execute(
                "INSERT INTO blocks (block_start, block_end, meter_id, config_period_id, "
                "imp_kwh, source) VALUES (?,?,?,?,?,?)",
                ("2025-03-02T00:00:00", "2025-03-02T00:00:00", "ev_charger", cp, 0.7, None))
        # Seam is the earliest NON-attributed block — the live one, not the 2025-03-01 attributed rows.
        self.assertEqual(s.get_device_live_coverage_start("ev_charger"), "2025-03-02T00:00:00")

    def test_live_coverage_seam_skips_leading_zero_holes(self):
        s = self._store()
        cp = s._conn.execute("SELECT id FROM config_periods LIMIT 1").fetchone()["id"]
        # A run of LIVE ev_charger blocks that recorded zero import (a sub-meter
        # dropout) followed by the first block with real energy. The seam must
        # advance past the zero holes to the first non-zero live block so
        # attribution can heal the holes rather than stop short at them.
        with s._conn:
            for ds, kwh in (("2026-02-12T00:00:00", 0.0),
                            ("2026-02-13T00:00:00", 0.0),
                            ("2026-02-14T00:00:00", 5.5)):
                s._conn.execute(
                    "INSERT INTO blocks (block_start, block_end, meter_id, "
                    "config_period_id, imp_kwh, source) VALUES (?,?,?,?,?,?)",
                    (ds, ds, "ev_charger", cp, kwh, None))
        self.assertEqual(s.get_device_live_coverage_start("ev_charger"),
                         "2026-02-14T00:00:00")

    def test_live_coverage_seam_none_when_only_zero_live(self):
        s = self._store()
        cp = s._conn.execute("SELECT id FROM config_periods LIMIT 1").fetchone()["id"]
        # Only zero-valued live blocks → no real coverage yet → seam is None so
        # attribution may fill the full available range.
        with s._conn:
            s._conn.execute(
                "INSERT INTO blocks (block_start, block_end, meter_id, "
                "config_period_id, imp_kwh, source) VALUES (?,?,?,?,?,?)",
                ("2026-02-12T00:00:00", "2026-02-12T00:00:00", "ev_charger", cp, 0.0, None))
        self.assertIsNone(s.get_device_live_coverage_start("ev_charger"))


class TestSweepRegisterGlitches(unittest.TestCase):
    """Stale-boundary phantom: a register that DIPS below its established level and
    RECOVERS (a dropout surfacing an old value, e.g. house_battery 6259.77 on
    2026-07-21) books the climb back as ~49 kWh. The sweep clamps only the
    provably-recovering dip; a non-recovering drop (possible reset) is left alone."""

    def _store(self):
        s = BlockStore(":memory:")
        s.insert_config_period({"meters": {
            "electricity_main": {"meta": {
                "billing_day": 1, "block_minutes": 30, "timezone": "Europe/London",
                "currency_symbol": "£", "currency_code": "GBP", "site": "Home"}},
            "house_battery": {"meta": {
                "sub_meter": True, "parent_meter": "electricity_main",
                "billing_day": 1, "block_minutes": 30, "timezone": "Europe/London",
                "currency_symbol": "£", "currency_code": "GBP"}},
        }})
        cp = s._conn.execute("SELECT id FROM config_periods LIMIT 1").fetchone()["id"]
        return s, cp

    def _ins(self, s, cp, start, imp_kwh, grid, re, carbon=0.0, intensity=142.0):
        s._conn.execute(
            "INSERT INTO blocks (block_start, block_end, meter_id, config_period_id, "
            "imp_kwh, imp_kwh_grid, imp_read_end, carbon_g, carbon_intensity_g) "
            "VALUES (?,?,?,?,?,?,?,?,?)",
            (start, start, "house_battery", cp, imp_kwh, grid, re, carbon, intensity))
        s._conn.commit()

    def test_clamps_dip_and_recover_phantom(self):
        s, cp = self._store()
        self._ins(s, cp, "2026-07-21T08:30:00", 0.0, 0.0, 6309.12)   # high-water established
        self._ins(s, cp, "2026-07-21T09:30:00", 0.0, 0.0, 6259.77)   # dip (already 0 → not flagged)
        self._ins(s, cp, "2026-07-21T10:00:00", 48.93, 0.0, 6308.70, carbon=6948.0)  # phantom climb-back
        self._ins(s, cp, "2026-07-21T10:30:00", 0.10, 0.10, 6309.12) # recovery to high-water

        prev = s.sweep_register_glitches(dry_run=True)
        self.assertEqual(prev["count"], 1)
        self.assertEqual(prev["blocks"][0]["block_start"], "2026-07-21T10:00:00")

        res = s.sweep_register_glitches(dry_run=False)
        self.assertTrue(res["applied"])
        r = s._conn.execute("SELECT imp_kwh, imp_kwh_grid, carbon_g, needs_review "
                            "FROM blocks WHERE block_start='2026-07-21T10:00:00'").fetchone()
        self.assertEqual(r["imp_kwh"], 0.0)          # clamped to grid
        self.assertEqual(r["carbon_g"], 0.0)         # recomputed from 0
        self.assertEqual(r["needs_review"], 1)
        self.assertEqual(r["imp_kwh_grid"], 0.0)     # bill side untouched
        # The reason must stay in the shared constant so the IOG pricing panel
        # filters it out (an auto-correction is not a rate task).
        rr = s._conn.execute("SELECT review_reason FROM blocks "
                             "WHERE block_start='2026-07-21T10:00:00'").fetchone()[0]
        self.assertIn(rr, BlockStore.AUTO_CORRECTION_REASONS)
        self.assertEqual(s.get_review_blocks(), [])  # not surfaced as a pricing task
        # recovery + normal blocks untouched
        self.assertAlmostEqual(s._conn.execute(
            "SELECT imp_kwh FROM blocks WHERE block_start='2026-07-21T10:30:00'").fetchone()[0], 0.10)
        self.assertEqual(s.sweep_register_glitches(dry_run=False)["count"], 0)   # idempotent

    def test_non_recovering_drop_left_alone(self):
        # A genuine reset: register drops and never returns to the prior high-water.
        # The sweep must NOT touch it (it isn't a dip-and-recover glitch).
        s, cp = self._store()
        self._ins(s, cp, "2026-06-01T00:00:00", 0.0, 0.0, 6300.0)    # high-water
        self._ins(s, cp, "2026-06-01T00:30:00", 3.0, 3.0, 3.0)       # reset → climbs fresh from ~0
        self._ins(s, cp, "2026-06-01T01:00:00", 2.0, 2.0, 5.0)
        self._ins(s, cp, "2026-06-01T01:30:00", 2.0, 2.0, 7.0)
        self.assertEqual(s.sweep_register_glitches(dry_run=True)["count"], 0)


class TestSourceTagRoundTrip(unittest.TestCase):
    """Regression: the 'source' provenance tag (imported_api etc.) must survive a
    read → modify → append_block_replace round-trip. Dropping it re-inserted
    source=NULL and silently untagged imported history (the carbon-round-trip bug)."""

    def _store(self):
        s = BlockStore(":memory:")
        with s._conn:
            cp = s._conn.execute(
                "INSERT INTO config_periods (effective_from, billing_day, block_minutes, "
                "timezone, currency_symbol, currency_code) "
                "VALUES ('2025-01-01T00:00:00',1,30,'UTC','£','GBP')").lastrowid
            s._conn.execute(
                "INSERT INTO meters (config_period_id, meter_id, is_sub_meter) "
                "VALUES (?, 'electricity_main', 0)", (cp,))
            s._conn.execute(
                "INSERT INTO blocks (block_start, block_end, meter_id, config_period_id, "
                "imp_kwh, source) VALUES ('2025-03-01T00:00:00','2025-03-01T00:30:00',"
                "'electricity_main',?,2.0,'imported_api')", (cp,))
        return s, cp

    def test_read_carries_source_onto_dict(self):
        s, _ = self._store()
        blk = s.get_block_dict_by_start("2025-03-01T00:00:00")
        self.assertEqual(blk["meters"]["electricity_main"].get("source"), "imported_api")

    def test_source_survives_read_modify_write(self):
        s, cp = self._store()
        blk = s.get_block_dict_by_start("2025-03-01T00:00:00")
        blk["meters"]["electricity_main"]["carbon_g"] = 123.0        # a carbon-style edit
        s.append_block_replace(blk, config_period_id=cp)
        src = s._conn.execute(
            "SELECT source FROM blocks WHERE meter_id='electricity_main'").fetchone()[0]
        self.assertEqual(src, "imported_api")                        # NOT wiped to NULL


if __name__ == "__main__":
    unittest.main()