"""Region timeline foundation — per-period postcode (outward code) for carbon.

Covers: outward_code normalisation, derive_region_periods parsing, the
get_postcode_prefix_at per-date resolver, and apply_region_periods auto-apply
(single-region backfill idempotency + no-clobber; multi-region split flag).
"""
import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from block_store import (  # noqa: E402
    BlockStore, outward_code, derive_region_periods,
)


def _new_store():
    return BlockStore(tempfile.mktemp(suffix=".db"))


def _add_period(store, *, eff_from, eff_to=None, postcode=None, source=None,
                billing_day=1):
    """Insert a config period with a main meter, return the period id."""
    with store._conn:
        cur = store._conn.execute(
            "INSERT INTO config_periods (effective_from, effective_to, billing_day, "
            "block_minutes, timezone, currency_symbol, currency_code) "
            "VALUES (?, ?, ?, 30, 'Europe/London', '£', 'GBP')",
            (eff_from, eff_to, billing_day),
        )
        pid = cur.lastrowid
        store._conn.execute(
            "INSERT INTO meters (config_period_id, meter_id, is_sub_meter, "
            "postcode_prefix, postcode_source) VALUES (?, 'electricity_main', 0, ?, ?)",
            (pid, postcode, source),
        )
    return pid


class TestOutwardCode(unittest.TestCase):
    def test_with_space(self):
        self.assertEqual(outward_code("SW1A 1AA"), "SW1A")
        self.assertEqual(outward_code("m1 1ae"), "M1")

    def test_no_space_full_postcode(self):
        self.assertEqual(outward_code("SW1A1AA"), "SW1A")
        self.assertEqual(outward_code("M11AE"), "M1")

    def test_already_outward(self):
        self.assertEqual(outward_code("EH8"), "EH8")
        self.assertEqual(outward_code("eh8"), "EH8")

    def test_empty(self):
        self.assertIsNone(outward_code(None))
        self.assertIsNone(outward_code(""))
        self.assertIsNone(outward_code("   "))


class TestDeriveRegionPeriods(unittest.TestCase):
    def test_single_property(self):
        acct = {"properties": [
            {"postcode": "SW1A 1AA", "moved_in_at": "2020-01-01", "moved_out_at": None},
        ]}
        spans = derive_region_periods(acct)
        self.assertEqual(len(spans), 1)
        self.assertEqual(spans[0]["outcode"], "SW1A")
        self.assertEqual(spans[0]["from"], "2020-01-01")
        self.assertIsNone(spans[0]["to"])

    def test_distinct_properties_same_outcode_not_merged(self):
        # Two DIFFERENT properties that share an outward code stay separate
        acct = {"properties": [
            {"id": 1, "postcode": "EH8 9YL", "moved_in_at": "2020-01-01", "moved_out_at": "2022-01-01"},
            {"id": 2, "postcode": "EH8 1RT", "moved_in_at": "2022-01-01", "moved_out_at": None},
        ]}
        spans = derive_region_periods(acct)
        self.assertEqual(len(spans), 2)
        self.assertEqual([s["outcode"] for s in spans], ["EH8", "EH8"])
        self.assertNotEqual(spans[0]["key"], spans[1]["key"])

    def test_same_property_id_records_merge(self):
        acct = {"properties": [
            {"id": 7, "postcode": "M1 1AE", "moved_in_at": "2020-01-01", "moved_out_at": "2021-01-01"},
            {"id": 7, "postcode": "M1 2CD", "moved_in_at": "2021-01-01", "moved_out_at": None},
        ]}
        spans = derive_region_periods(acct)
        self.assertEqual(len(spans), 1)
        self.assertIsNone(spans[0]["to"])

    def test_key_and_hint_present(self):
        acct = {"properties": [
            {"id": 42, "postcode": "DE65 6GG", "town": "Hatton",
             "moved_in_at": "2024-01-01", "moved_out_at": None},
        ]}
        spans = derive_region_periods(acct)
        self.assertTrue(spans[0]["key"])          # stable handle present
        self.assertEqual(spans[0]["hint"], "Hatton")

    def test_key_from_address_when_no_id(self):
        acct = {"properties": [
            {"postcode": "DE65 6GG", "address_line_1": "1 The Lane",
             "moved_in_at": "2024-01-01"},
        ]}
        spans = derive_region_periods(acct)
        self.assertTrue(spans[0]["key"])          # derived from address+postcode

    def test_move_two_regions_sorted(self):
        acct = {"properties": [
            {"postcode": "M1 1AE",  "moved_in_at": "2025-03-01", "moved_out_at": None},
            {"postcode": "EH8 9YL", "moved_in_at": "2020-01-01", "moved_out_at": "2025-03-01"},
        ]}
        spans = derive_region_periods(acct)
        self.assertEqual([s["outcode"] for s in spans], ["EH8", "M1"])
        self.assertEqual(spans[0]["to"], "2025-03-01")

    def test_merges_consecutive_records_of_same_property(self):
        # Same property (identical full postcode, no id) recorded twice → one span
        acct = {"properties": [
            {"postcode": "EH8 9YL", "moved_in_at": "2020-01-01", "moved_out_at": "2022-01-01"},
            {"postcode": "EH8 9YL", "moved_in_at": "2022-01-01", "moved_out_at": None},
        ]}
        spans = derive_region_periods(acct)
        self.assertEqual(len(spans), 1)
        self.assertEqual(spans[0]["outcode"], "EH8")
        self.assertIsNone(spans[0]["to"])

    def test_full_postcode_not_retained(self):
        acct = {"properties": [{"postcode": "SW1A 1AA", "moved_in_at": "2020-01-01"}]}
        spans = derive_region_periods(acct)
        # Only the outward code survives — never the full postcode
        self.assertEqual(spans[0]["outcode"], "SW1A")
        self.assertNotIn("1AA", str(spans))


class TestGetPostcodePrefixAt(unittest.TestCase):
    def test_resolves_per_period(self):
        store = _new_store()
        _add_period(store, eff_from="2024-01-01", eff_to="2025-06-01",
                    postcode="SW1A", source="user")
        _add_period(store, eff_from="2025-06-01", eff_to=None,
                    postcode="M1", source="octopus")
        self.assertEqual(store.get_postcode_prefix_at("2024-07-01"), ("SW1A", "user"))
        self.assertEqual(store.get_postcode_prefix_at("2025-07-01"), ("M1", "octopus"))

    def test_pre_history_falls_back_to_oldest(self):
        store = _new_store()
        _add_period(store, eff_from="2024-01-01", eff_to=None,
                    postcode="SW1A", source="user")
        # A date before any period (imported/pre-EMT) resolves to the oldest period
        self.assertEqual(store.get_postcode_prefix_at("2023-01-01"), ("SW1A", "user"))

    def test_no_postcode_returns_none(self):
        store = _new_store()
        _add_period(store, eff_from="2024-01-01", eff_to=None)
        self.assertEqual(store.get_postcode_prefix_at("2024-07-01"), (None, None))


class TestApplyRegionPeriods(unittest.TestCase):
    def test_single_region_backfills_all_periods(self):
        store = _new_store()
        p1 = _add_period(store, eff_from="2024-01-01", eff_to="2025-01-01")
        p2 = _add_period(store, eff_from="2025-01-01", eff_to=None)
        res = store.apply_region_periods([{"outcode": "EH8", "from": "2020-01-01"}])
        self.assertFalse(res["split_required"])
        self.assertEqual(res["applied"], 2)
        self.assertEqual(store.get_postcode_prefix_at("2024-06-01"), ("EH8", "octopus"))
        self.assertEqual(store.get_postcode_prefix_at("2025-06-01"), ("EH8", "octopus"))

    def test_idempotent(self):
        store = _new_store()
        _add_period(store, eff_from="2024-01-01", eff_to=None)
        store.apply_region_periods([{"outcode": "EH8"}])
        res2 = store.apply_region_periods([{"outcode": "EH8"}])
        self.assertEqual(res2["applied"], 0)   # already applied → no-op

    def test_never_clobbers_user_value(self):
        store = _new_store()
        _add_period(store, eff_from="2024-01-01", eff_to=None,
                    postcode="AB1", source="user")
        res = store.apply_region_periods([{"outcode": "EH8"}])
        self.assertEqual(res["applied"], 0)
        self.assertEqual(store.get_postcode_prefix_at("2024-06-01"), ("AB1", "user"))

    def test_full_postcode_is_reduced_before_storage(self):
        store = _new_store()
        _add_period(store, eff_from="2024-01-01", eff_to=None)
        store.apply_region_periods([{"outcode": "SW1A 1AA"}])   # full slips in
        oc, src = store.get_postcode_prefix_at("2024-06-01")
        self.assertEqual(oc, "SW1A")   # stored as outward code only
        self.assertEqual(src, "octopus")

    def test_overwrite_user_clobbers(self):
        store = _new_store()
        _add_period(store, eff_from="2024-01-01", eff_to=None,
                    postcode="DE1", source="user")
        res = store.apply_region_periods([{"outcode": "DE65"}], overwrite_user=True)
        self.assertEqual(res["applied"], 1)
        self.assertEqual(store.get_postcode_prefix_at("2024-06-01"), ("DE65", "octopus"))

    def test_migration_tags_existing_postcode_as_user(self):
        # A postcode present with NULL source (legacy) should be tagged 'user'
        # by the schema migration when the store is (re)opened.
        path = tempfile.mktemp(suffix=".db")
        store = BlockStore(path)
        _add_period(store, eff_from="2024-01-01", eff_to=None,
                    postcode="DE1", source=None)
        store._conn.close()
        reopened = BlockStore(path)   # _ensure_schema runs the backfill
        self.assertEqual(reopened.get_postcode_prefix_at("2024-06-01"), ("DE1", "user"))

    def test_multi_region_flags_split_and_applies_latest(self):
        store = _new_store()
        _add_period(store, eff_from="2024-01-01", eff_to=None)
        res = store.apply_region_periods([
            {"outcode": "EH8", "from": "2020-01-01", "to": "2025-03-01"},
            {"outcode": "M1",  "from": "2025-03-01", "to": None},
        ])
        self.assertTrue(res["split_required"])
        self.assertEqual(sorted(res["outcodes"]), ["EH8", "M1"])
        # latest region stamped onto existing period(s)
        self.assertEqual(store.get_postcode_prefix_at("2024-06-01"), ("M1", "octopus"))


def _moved_store():
    """A single active config period spanning a move, with a main+sub meter,
    channels on the main, and blocks either side of a 2023-06-01 boundary."""
    store = _new_store()
    with store._conn:
        cur = store._conn.execute(
            "INSERT INTO config_periods (effective_from, effective_to, billing_day, "
            "block_minutes, timezone, currency_symbol, currency_code, site_name, supplier) "
            "VALUES ('2020-01-01T00:00:00', NULL, 1, 30, 'UTC', '£', 'GBP', 'Home', 'Octopus')"
        )
        pid = cur.lastrowid
        mcur = store._conn.execute(
            "INSERT INTO meters (config_period_id, meter_id, is_sub_meter, postcode_prefix, postcode_source) "
            "VALUES (?, 'electricity_main', 0, 'M1', 'user')", (pid,))
        main_id = mcur.lastrowid
        store._conn.execute(
            "INSERT INTO meters (config_period_id, meter_id, is_sub_meter, parent_meter_id) "
            "VALUES (?, 'ev_charger', 1, 'electricity_main')", (pid,))
        store._conn.execute(
            "INSERT INTO meter_channels (meter_id, channel, mpan, tariff) "
            "VALUES (?, 'import', '1200000', 'AGILE')", (main_id,))
        store._conn.execute(
            "INSERT INTO meter_channels (meter_id, channel, mpan, tariff) "
            "VALUES (?, 'export', '1200001', 'OUTGOING')", (main_id,))
        for bs in ['2023-03-01T00:00:00', '2023-05-01T00:00:00',
                   '2023-06-01T00:00:00', '2023-09-01T00:00:00', '2024-01-01T00:00:00']:
            store._conn.execute(
                "INSERT INTO blocks (block_start, block_end, meter_id, config_period_id, imp_kwh) "
                "VALUES (?, ?, 'electricity_main', ?, 1.0)", (bs, bs, pid))
    return store, pid


class TestSplitConfigPeriod(unittest.TestCase):
    SPLIT = "2023-06-01"

    def test_creates_two_contiguous_periods(self):
        store, pid = _moved_store()
        new_id = store.split_config_period_at(pid, self.SPLIT)
        orig = store.get_config_period(pid)
        new  = store.get_config_period(new_id)
        self.assertEqual(orig["effective_from"], "2020-01-01T00:00:00")
        self.assertEqual(orig["effective_to"],   "2023-06-01T00:00:00")
        self.assertEqual(new["effective_from"],  "2023-06-01T00:00:00")
        self.assertIsNone(new["effective_to"])   # inherits the active (open) end

    def test_copies_meters_and_channels(self):
        store, pid = _moved_store()
        new_id = store.split_config_period_at(pid, self.SPLIT)
        meters = store._conn.execute(
            "SELECT meter_id, is_sub_meter, postcode_prefix FROM meters "
            "WHERE config_period_id=? ORDER BY meter_id", (new_id,)).fetchall()
        self.assertEqual([m["meter_id"] for m in meters], ["electricity_main", "ev_charger"])
        main = [m for m in meters if m["meter_id"] == "electricity_main"][0]
        self.assertEqual(main["postcode_prefix"], "M1")
        # Channels copied and re-pointed at the NEW meter row
        new_main_id = store._conn.execute(
            "SELECT id FROM meters WHERE config_period_id=? AND meter_id='electricity_main'",
            (new_id,)).fetchone()["id"]
        chans = store._conn.execute(
            "SELECT channel, mpan FROM meter_channels WHERE meter_id=? ORDER BY channel",
            (new_main_id,)).fetchall()
        self.assertEqual([(c["channel"], c["mpan"]) for c in chans],
                         [("export", "1200001"), ("import", "1200000")])

    def test_reassigns_later_blocks_only(self):
        store, pid = _moved_store()
        new_id = store.split_config_period_at(pid, self.SPLIT)
        orig_blocks = store._conn.execute(
            "SELECT block_start FROM blocks WHERE config_period_id=? ORDER BY block_start", (pid,)).fetchall()
        new_blocks = store._conn.execute(
            "SELECT block_start FROM blocks WHERE config_period_id=? ORDER BY block_start", (new_id,)).fetchall()
        self.assertEqual([b["block_start"] for b in orig_blocks],
                         ["2023-03-01T00:00:00", "2023-05-01T00:00:00"])
        self.assertEqual([b["block_start"] for b in new_blocks],
                         ["2023-06-01T00:00:00", "2023-09-01T00:00:00", "2024-01-01T00:00:00"])
        total = store._conn.execute("SELECT COUNT(*) c FROM blocks").fetchone()["c"]
        self.assertEqual(total, 5)   # nothing lost or duplicated

    def test_regions_resolve_per_half_after_edit(self):
        store, pid = _moved_store()
        new_id = store.split_config_period_at(pid, self.SPLIT)
        store.set_period_postcode(pid, "EH8", "user")     # earlier half = old address
        # new half keeps M1 (copied)
        self.assertEqual(store.get_postcode_prefix_at("2023-03-15"), ("EH8", "user"))
        self.assertEqual(store.get_postcode_prefix_at("2023-09-15"), ("M1", "user"))

    def test_resolver_maps_dates_to_correct_period(self):
        store, pid = _moved_store()
        new_id = store.split_config_period_at(pid, self.SPLIT)
        self.assertEqual(store.get_config_period_for_date("2023-03-15")["id"], pid)
        self.assertEqual(store.get_config_period_for_date("2023-09-15")["id"], new_id)

    def test_rejects_split_before_start(self):
        store, pid = _moved_store()
        with self.assertRaises(ValueError):
            store.split_config_period_at(pid, "2019-01-01")

    def test_rejects_split_outside_closed_period(self):
        store, pid = _moved_store()
        store.split_config_period_at(pid, self.SPLIT)   # pid now closed at 2023-06-01
        with self.assertRaises(ValueError):
            store.split_config_period_at(pid, "2023-09-01")   # past its new effective_to


class TestRegionReconciliation(unittest.TestCase):
    SPLIT = "2023-06-01"

    def _moved_spans(self):
        return [
            {"outcode": "EH8", "from": "2020-01-01", "to": self.SPLIT, "key": "a", "hint": "Edinburgh"},
            {"outcode": "M1",  "from": self.SPLIT,   "to": None,       "key": "b", "hint": "Manchester"},
        ]

    def test_plan_never_moved_no_confirmation(self):
        store, pid = _moved_store()   # site_name 'Home' already set, postcode M1
        plan = store.plan_region_reconciliation(
            [{"outcode": "M1", "from": "2020-01-01", "to": None, "key": "b", "hint": "Manchester"}])
        self.assertFalse(plan["needs_confirmation"])
        self.assertEqual(plan["split_dates"], [])

    def test_plan_move_flags_split_and_unnamed_site(self):
        store, pid = _moved_store()
        plan = store.plan_region_reconciliation(self._moved_spans())
        self.assertTrue(plan["needs_confirmation"])
        self.assertEqual(plan["split_dates"], [self.SPLIT])
        by_oc = {s["outcode"]: s for s in plan["sites"]}
        # EH8 covers the existing period start → inherits its 'Home' name
        self.assertEqual(by_oc["EH8"]["site_name"], "Home")
        self.assertFalse(by_oc["EH8"]["needs_name"])
        # M1 (later) has no existing named period yet → needs a name
        self.assertTrue(by_oc["M1"]["needs_name"])
        # block counts split across the boundary (2 before, 3 from the boundary)
        self.assertEqual(by_oc["EH8"]["block_count"], 2)
        self.assertEqual(by_oc["M1"]["block_count"], 3)

    def test_apply_splits_and_stamps_regions_and_names(self):
        store, pid = _moved_store()
        res = store.apply_region_reconciliation([
            {"outcode": "EH8", "from": "2020-01-01", "to": self.SPLIT, "site_name": "Old Flat"},
            {"outcode": "M1",  "from": self.SPLIT,   "to": None,       "site_name": "New House"},
        ])
        self.assertEqual(res["splits"], 1)
        self.assertEqual(res["stamped"], 2)
        # regions resolve per half
        self.assertEqual(store.get_postcode_prefix_at("2023-03-15"), ("EH8", "octopus"))
        self.assertEqual(store.get_postcode_prefix_at("2023-09-15"), ("M1", "octopus"))
        # site names land on the EARLIER (past-address) period; the ACTIVE period
        # keeps its existing name — it's the instance / backup-folder identity.
        early = store.get_config_period_for_date("2023-03-15")
        late  = store.get_config_period_for_date("2023-09-15")
        self.assertEqual(early["site_name"], "Old Flat")
        self.assertEqual(late["site_name"],  "Home")   # preserved (was active)

    def test_reconcile_never_renames_active_period(self):
        store, pid = _moved_store()   # single active period, site_name 'Home'
        store.apply_region_reconciliation([
            {"outcode": "M1", "from": "2020-01-01", "to": None, "site_name": "Renamed"}])
        active = store._conn.execute(
            "SELECT site_name FROM config_periods WHERE effective_to IS NULL").fetchone()
        self.assertEqual(active["site_name"], "Home")   # instance identity untouched
        # region still applied to it
        self.assertEqual(store.get_postcode_prefix_at("2023-01-01"), ("M1", "octopus"))

    def test_plan_csv_reconciliation_region_editable(self):
        store, pid = _moved_store()
        plan = store.plan_csv_reconciliation("2023-04-01", "2023-08-01")
        self.assertTrue(plan["needs_confirmation"])
        self.assertEqual(plan["source"], "csv")
        site = plan["sites"][0]
        self.assertTrue(site["region_editable"])
        self.assertIsNone(site["outcode"])
        self.assertEqual(site["block_count"], 2)   # 2023-05-01, 2023-06-01
        self.assertEqual(plan["split_dates"], ["2023-04-01", "2023-08-01"])

    def test_apply_subrange_splits_both_ends_stamps_middle_only(self):
        store, pid = _moved_store()
        res = store.apply_region_reconciliation([
            {"outcode": "ZZ1", "from": "2023-04-01", "to": "2023-08-01", "site_name": "Rental"}])
        self.assertEqual(res["splits"], 2)
        n = store._conn.execute("SELECT COUNT(*) c FROM config_periods").fetchone()["c"]
        self.assertEqual(n, 3)
        self.assertEqual(store.get_postcode_prefix_at("2023-05-15"), ("ZZ1", "octopus"))
        self.assertEqual(store.get_postcode_prefix_at("2023-03-15"), ("M1", "user"))
        self.assertEqual(store.get_postcode_prefix_at("2023-09-15"), ("M1", "user"))
        self.assertEqual(store.get_config_period_for_date("2023-05-15")["site_name"], "Rental")

    def test_apply_is_idempotent(self):
        store, pid = _moved_store()
        sites = [
            {"outcode": "EH8", "from": "2020-01-01", "to": self.SPLIT, "site_name": "Old Flat"},
            {"outcode": "M1",  "from": self.SPLIT,   "to": None,       "site_name": "New House"},
        ]
        store.apply_region_reconciliation(sites)
        res2 = store.apply_region_reconciliation(sites)   # boundary now exists
        self.assertEqual(res2["splits"], 0)               # no duplicate split
        n_periods = store._conn.execute("SELECT COUNT(*) c FROM config_periods").fetchone()["c"]
        self.assertEqual(n_periods, 2)


def _carbon_gating_store():
    """Two periods — A has a known region, B does not — with a NULL-carbon
    imported block in each, plus a NULL-carbon live block in B."""
    store = _new_store()
    with store._conn:
        a = store._conn.execute(
            "INSERT INTO config_periods (effective_from, effective_to, billing_day, "
            "block_minutes, timezone, currency_symbol, currency_code) "
            "VALUES ('2024-01-01T00:00:00','2024-06-01T00:00:00',1,30,'UTC','£','GBP')").lastrowid
        store._conn.execute(
            "INSERT INTO meters (config_period_id, meter_id, is_sub_meter, postcode_prefix) "
            "VALUES (?, 'electricity_main', 0, 'SW1A')", (a,))
        b = store._conn.execute(
            "INSERT INTO config_periods (effective_from, effective_to, billing_day, "
            "block_minutes, timezone, currency_symbol, currency_code) "
            "VALUES ('2024-06-01T00:00:00',NULL,1,30,'UTC','£','GBP')").lastrowid
        store._conn.execute(
            "INSERT INTO meters (config_period_id, meter_id, is_sub_meter) "
            "VALUES (?, 'electricity_main', 0)", (b,))
        def _blk(bs, cp, source):
            store._conn.execute(
                "INSERT INTO blocks (block_start, block_end, meter_id, config_period_id, "
                "imp_kwh, carbon_intensity_g, source) VALUES (?, ?, 'electricity_main', ?, 1.0, NULL, ?)",
                (bs, bs, cp, source))
        _blk("2024-03-01T00:00:00", a, "imported_api")   # imported, region known
        _blk("2024-07-01T00:00:00", b, "imported_api")   # imported, region UNKNOWN
        _blk("2024-08-01T00:00:00", b, None)             # live, always eligible
    return store, a, b


class TestCarbonRegionGating(unittest.TestCase):
    def test_missing_range_includes_region_known_imports_excludes_unknown(self):
        store, a, b = _carbon_gating_store()
        lo, hi = store.get_missing_carbon_date_range()
        self.assertEqual(lo, "2024-03-01T00:00:00")   # region-known import is eligible
        self.assertEqual(hi, "2024-08-01T00:00:00")   # live block eligible

    def test_block_starts_excludes_region_unknown_import(self):
        store, a, b = _carbon_gating_store()
        starts = store.get_block_starts_missing_carbon_in_range(
            "2024-01-01T00:00:00", "2024-12-01T00:00:00")
        self.assertIn("2024-03-01T00:00:00", starts)     # region known
        self.assertIn("2024-08-01T00:00:00", starts)     # live
        self.assertNotIn("2024-07-01T00:00:00", starts)  # region unknown → excluded

    def test_assigning_region_makes_import_eligible(self):
        store, a, b = _carbon_gating_store()
        store.set_period_postcode(b, "M1", "user")
        starts = store.get_block_starts_missing_carbon_in_range(
            "2024-01-01T00:00:00", "2024-12-01T00:00:00")
        self.assertIn("2024-07-01T00:00:00", starts)     # now eligible

    def test_rearm_clears_marker(self):
        store, a, b = _carbon_gating_store()
        store.set_meta("carbon_backfill_state", {"done": True, "unfilled_from": "x"})
        store.rearm_carbon_backfill()
        self.assertFalse((store.get_meta("carbon_backfill_state", {}) or {}).get("done"))


class TestExtendEarliestPeriod(unittest.TestCase):
    def _store(self):
        store = _new_store()
        with store._conn:
            store._conn.execute(
                "INSERT INTO config_periods (effective_from, effective_to, billing_day, "
                "block_minutes, timezone, currency_symbol, currency_code) "
                "VALUES ('2026-06-03T23:00:00', NULL, 1, 30, 'UTC', '£', 'GBP')")
        return store

    def test_extends_back_to_cover_import(self):
        store = self._store()
        self.assertTrue(store.extend_earliest_period_to("2024-07-01T00:00:00"))
        ef = store._conn.execute(
            "SELECT effective_from FROM config_periods ORDER BY effective_from LIMIT 1").fetchone()[0]
        self.assertEqual(ef, "2024-07-01T00:00:00")

    def test_never_shrinks(self):
        store = self._store()
        # a later date must NOT move the start forward
        self.assertFalse(store.extend_earliest_period_to("2026-12-01T00:00:00"))
        ef = store._conn.execute(
            "SELECT effective_from FROM config_periods ORDER BY effective_from LIMIT 1").fetchone()[0]
        self.assertEqual(ef, "2026-06-03T23:00:00")


class TestPurgeImportedHistory(unittest.TestCase):
    def _store(self):
        store = _new_store()
        with store._conn:
            cp = store._conn.execute(
                "INSERT INTO config_periods (effective_from, billing_day, block_minutes, "
                "timezone, currency_symbol, currency_code) "
                "VALUES ('2024-01-01T00:00:00',1,30,'UTC','£','GBP')").lastrowid
            store._conn.execute(
                "INSERT INTO meters (config_period_id, meter_id, is_sub_meter) "
                "VALUES (?, 'electricity_main', 0)", (cp,))
            store._conn.execute(
                "INSERT INTO blocks (block_start, block_end, meter_id, config_period_id, imp_kwh, source) "
                "VALUES ('2024-03-01T00:00:00','2024-03-01T00:30:00','electricity_main',?,1.0,'imported_api')", (cp,))
            store._conn.execute(
                "INSERT INTO blocks (block_start, block_end, meter_id, config_period_id, imp_kwh, source) "
                "VALUES ('2026-01-01T00:00:00','2026-01-01T00:30:00','electricity_main',?,1.0,NULL)", (cp,))
            store._conn.execute(
                "INSERT INTO historical_derivation (scope, subject, period_from, period_to, derived_at, source) "
                "VALUES ('rate','import','2024-01-01','2024-06-01','2024-06-01T00:00:00','imported_api')")
            store._conn.execute(
                "INSERT INTO historical_derivation (scope, subject, period_from, period_to, derived_at, source) "
                "VALUES ('device_attribution','ev','2024-01-01','2024-06-01','2024-06-01T00:00:00','recorder_probe')")
            for k, v in [("api_import_done_import", "1"), ("import_gaps_import", "[]")]:
                store._conn.execute("INSERT INTO kraken_state (key,value) VALUES (?,?)", (k, v))
        return store

    def test_count(self):
        self.assertEqual(self._store().count_imported_history()["blocks"], 1)

    def test_purge_removes_imports_keeps_live_and_probe(self):
        store = self._store()
        res = store.purge_imported_history()
        self.assertEqual(res["blocks"], 1)
        # imported block gone, live block remains
        rows = store._conn.execute("SELECT source FROM blocks").fetchall()
        self.assertEqual([r["source"] for r in rows], [None])
        # import derivation gone, recorder_probe kept
        srcs = [r["source"] for r in store._conn.execute("SELECT source FROM historical_derivation")]
        self.assertEqual(srcs, ["recorder_probe"])
        # import checkpoints cleared
        n = store._conn.execute(
            "SELECT COUNT(*) c FROM kraken_state "
            "WHERE key LIKE 'api_import%' OR key LIKE 'import_gaps%'").fetchone()["c"]
        self.assertEqual(n, 0)


def _preimport_store(postcode=None, source=None):
    """PRE-import state: one active period at go-live (2026-06-03, UTC), a
    main+sub meter, channels on the main, and NO blocks yet. Mirrors a fresh
    install about to run its first historical import."""
    store = _new_store()
    with store._conn:
        cur = store._conn.execute(
            "INSERT INTO config_periods (effective_from, effective_to, billing_day, "
            "block_minutes, timezone, currency_symbol, currency_code, site_name, supplier) "
            "VALUES ('2026-06-03T00:00:00', NULL, 1, 30, 'UTC', '£', 'GBP', 'Home', 'Octopus')")
        pid = cur.lastrowid
        mcur = store._conn.execute(
            "INSERT INTO meters (config_period_id, meter_id, is_sub_meter, postcode_prefix, postcode_source) "
            "VALUES (?, 'electricity_main', 0, ?, ?)", (pid, postcode, source))
        main_id = mcur.lastrowid
        store._conn.execute(
            "INSERT INTO meters (config_period_id, meter_id, is_sub_meter, parent_meter_id) "
            "VALUES (?, 'ev_charger', 1, 'electricity_main')", (pid,))
        store._conn.execute(
            "INSERT INTO meter_channels (meter_id, channel, mpan, tariff) "
            "VALUES (?, 'import', '1200000', 'AGILE')", (main_id,))
        store._conn.execute(
            "INSERT INTO meter_channels (meter_id, channel, mpan, tariff) "
            "VALUES (?, 'export', '1200001', 'OUTGOING')", (main_id,))
    return store, pid


class TestCreateCoveringPeriod(unittest.TestCase):
    def test_clones_meters_channels_and_stamps_region(self):
        store, pid = _preimport_store()
        new_id = store.create_covering_period(
            "2018-01-01", "2023-06-01", outcode="EH8", site_name="Old Flat")
        cp = store.get_config_period(new_id)
        self.assertEqual(cp["effective_from"], "2018-01-01T00:00:00")
        self.assertEqual(cp["effective_to"],   "2023-06-01T00:00:00")
        self.assertEqual(cp["site_name"], "Old Flat")
        meters = store._conn.execute(
            "SELECT id, meter_id, postcode_prefix, postcode_source FROM meters "
            "WHERE config_period_id=? ORDER BY meter_id", (new_id,)).fetchall()
        self.assertEqual([m["meter_id"] for m in meters],
                         ["electricity_main", "ev_charger"])
        main = [m for m in meters if m["meter_id"] == "electricity_main"][0]
        self.assertEqual(main["postcode_prefix"], "EH8")
        self.assertEqual(main["postcode_source"], "octopus")
        chans = store._conn.execute(
            "SELECT channel, mpan FROM meter_channels WHERE meter_id=? ORDER BY channel",
            (main["id"],)).fetchall()
        self.assertEqual([(c["channel"], c["mpan"]) for c in chans],
                         [("export", "1200001"), ("import", "1200000")])

    def test_reduces_full_postcode_before_storage(self):
        store, pid = _preimport_store()
        new_id = store.create_covering_period("2018-01-01", "2023-06-01", outcode="EH8 9YL")
        self.assertEqual(store.get_config_period_for_date("2019-01-01")["id"], new_id)
        self.assertEqual(store.get_postcode_prefix_at("2019-01-01"), ("EH8", "octopus"))

    def test_rejects_inverted_range(self):
        store, pid = _preimport_store()
        with self.assertRaises(ValueError):
            store.create_covering_period("2023-06-01", "2018-01-01")


class TestPlanPreImportSites(unittest.TestCase):
    def test_single_site_no_confirmation(self):
        store, pid = _preimport_store(postcode="DE65", source="user")
        plan = store.plan_pre_import_sites(
            [{"outcode": "DE65", "from": "2023-06-01", "to": None, "key": "a", "hint": "Derby"}])
        self.assertFalse(plan["needs_confirmation"])
        self.assertEqual(len(plan["sites"]), 1)
        s = plan["sites"][0]
        self.assertTrue(s["is_current"])
        self.assertEqual(s["site_name"], "Home")     # prefilled from active (instance)
        self.assertFalse(s["needs_name"])

    def test_move_flags_past_site_needs_name(self):
        store, pid = _preimport_store()
        plan = store.plan_pre_import_sites([
            {"outcode": "EH8", "from": "2018-01-01", "to": "2023-06-01", "key": "a", "hint": "Edinburgh"},
            {"outcode": "M1",  "from": "2023-06-01", "to": None,         "key": "b", "hint": "Manchester"}])
        self.assertTrue(plan["needs_confirmation"])
        by = {s["outcode"]: s for s in plan["sites"]}
        self.assertFalse(by["EH8"]["is_current"])
        self.assertTrue(by["EH8"]["needs_name"])
        self.assertIsNone(by["EH8"]["site_name"])
        self.assertEqual(by["EH8"]["hint"], "Edinburgh")
        self.assertTrue(by["M1"]["is_current"])
        self.assertEqual(by["M1"]["site_name"], "Home")   # read-only instance name
        self.assertFalse(by["M1"]["needs_name"])

    def test_empty_derived(self):
        store, pid = _preimport_store()
        self.assertEqual(store.plan_pre_import_sites([]),
                         {"needs_confirmation": False, "sites": []})


class TestApplyPreImportSites(unittest.TestCase):
    def _apply_move(self, store):
        return store.apply_pre_import_sites([
            {"outcode": "EH8", "from": "2018-01-01", "to": "2023-06-01", "site_name": "Old Flat"},
            {"outcode": "M1",  "from": "2023-06-01", "to": None}])

    def test_creates_past_extends_active_contiguous(self):
        store, pid = _preimport_store()   # active [2026-06-03, NULL), region unset
        res = self._apply_move(store)
        self.assertEqual(res["created"], 1)
        self.assertTrue(res["extended"])
        active = store.get_config_period(pid)
        self.assertEqual(active["effective_from"], "2023-06-01T00:00:00")  # extended back to move-in
        self.assertIsNone(active["effective_to"])
        self.assertEqual(active["site_name"], "Home")   # NEVER renamed (instance identity)
        past = store.get_config_period_for_date("2019-01-01")
        self.assertEqual(past["effective_from"], "2018-01-01T00:00:00")
        self.assertEqual(past["effective_to"],   "2023-06-01T00:00:00")   # abuts the active period
        self.assertEqual(past["site_name"], "Old Flat")

    def test_regions_resolve_per_period(self):
        store, pid = _preimport_store()
        self._apply_move(store)
        self.assertEqual(store.get_postcode_prefix_at("2019-01-01"), ("EH8", "octopus"))
        self.assertEqual(store.get_postcode_prefix_at("2024-01-01"), ("M1", "octopus"))

    def test_current_region_not_clobbered_when_user_set(self):
        store, pid = _preimport_store(postcode="DE65", source="user")
        self._apply_move(store)
        self.assertEqual(store.get_postcode_prefix_at("2024-01-01"), ("DE65", "user"))

    def test_single_site_extends_only(self):
        store, pid = _preimport_store()
        res = store.apply_pre_import_sites(
            [{"outcode": "DE65", "from": "2023-06-01", "to": None}])
        self.assertEqual(res["created"], 0)
        self.assertTrue(res["extended"])
        self.assertEqual(store.get_config_period(pid)["effective_from"], "2023-06-01T00:00:00")
        self.assertEqual(store.get_postcode_prefix_at("2024-01-01"), ("DE65", "octopus"))

    def test_idempotent(self):
        store, pid = _preimport_store()
        self._apply_move(store)
        n1 = store._conn.execute("SELECT COUNT(*) c FROM config_periods").fetchone()["c"]
        res2 = self._apply_move(store)
        self.assertEqual(res2["created"], 0)   # existing period updated, not duplicated
        n2 = store._conn.execute("SELECT COUNT(*) c FROM config_periods").fetchone()["c"]
        self.assertEqual(n1, n2)
        # names/regions unchanged on re-run
        self.assertEqual(store.get_config_period_for_date("2019-01-01")["site_name"], "Old Flat")


class TestPreImportPersistence(unittest.TestCase):
    """Durability: the pre-import writes run inside `with self._conn:` blocks, so
    they should COMMIT — prove it by writing to a real file, closing the store,
    reopening it, and confirming the created period + its cloned meters/channels
    survived (an uncommitted write would read back fine on the same connection but
    vanish on reopen)."""

    def test_created_periods_persist_across_reopen(self):
        path = tempfile.mktemp(suffix=".db")
        store = BlockStore(path)
        with store._conn:
            pid = store._conn.execute(
                "INSERT INTO config_periods (effective_from, effective_to, billing_day, "
                "block_minutes, timezone, currency_symbol, currency_code, site_name) "
                "VALUES ('2026-06-03T00:00:00', NULL, 1, 30, 'UTC', '£', 'GBP', 'Home')").lastrowid
            mid = store._conn.execute(
                "INSERT INTO meters (config_period_id, meter_id, is_sub_meter) "
                "VALUES (?, 'electricity_main', 0)", (pid,)).lastrowid
            store._conn.execute(
                "INSERT INTO meter_channels (meter_id, channel, mpan) "
                "VALUES (?, 'import', '1200000')", (mid,))
        store.apply_pre_import_sites([
            {"outcode": "EH8", "from": "2018-01-01", "to": "2023-06-01", "site_name": "Old Flat"},
            {"outcode": "M1",  "from": "2023-06-01", "to": None}])
        store.close()

        # Reopen the SAME file — only committed rows are here.
        store2 = BlockStore(path)
        try:
            past = store2.get_config_period_for_date("2019-01-01")
            self.assertIsNotNone(past)
            self.assertEqual(past["effective_from"], "2018-01-01T00:00:00")
            self.assertEqual(past["effective_to"],   "2023-06-01T00:00:00")
            self.assertEqual(past["site_name"], "Old Flat")
            self.assertEqual(store2.get_postcode_prefix_at("2019-01-01"), ("EH8", "octopus"))
            # cloned channel survived on the new period's main meter
            chans = store2._conn.execute(
                "SELECT mc.channel FROM meter_channels mc JOIN meters m ON m.id = mc.meter_id "
                "WHERE m.config_period_id = ?", (past["id"],)).fetchall()
            self.assertEqual([c["channel"] for c in chans], ["import"])
            # active period extended back to move-in, name untouched
            active = store2.get_config_period(pid)
            self.assertEqual(active["effective_from"], "2023-06-01T00:00:00")
            self.assertEqual(active["site_name"], "Home")
        finally:
            store2.close()


if __name__ == "__main__":
    unittest.main()
