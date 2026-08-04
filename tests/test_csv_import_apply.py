"""Tests for BlockStore.apply_csv_import — CSV core wired into the store."""
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from block_store import BlockStore, IMPORTED_SOURCE_CSV

_HEADER = ("Consumption (kwh),Estimated Cost Inc. Tax (p),"
           "Standing Charge Inc. Tax (p),Start,End\n")


def _csv(rows):
    """rows: list of (kwh, cost_p, standing_p, start_iso). End = +30 min omitted."""
    out = _HEADER
    for kwh, cost_p, sc_p, start in rows:
        out += f"{kwh},{cost_p},{sc_p},{start},{start}\n"
    return out


# Import: 4 off-peak (7p) + 2 peak (25p) on 2024-07-01.
IMPORT_CSV = _csv([
    (1.0, 7.0, 1.12, "2024-07-01T00:00:00+00:00"),
    (1.0, 7.0, 1.12, "2024-07-01T00:30:00+00:00"),
    (1.0, 7.0, 1.12, "2024-07-01T01:00:00+00:00"),
    (1.0, 7.0, 1.12, "2024-07-01T01:30:00+00:00"),
    (1.0, 25.0, 1.12, "2024-07-01T18:00:00+00:00"),
    (1.0, 25.0, 1.12, "2024-07-01T18:30:00+00:00"),
])
# Export: 2 blocks at 15p, timestamps overlapping import rows (merge test).
EXPORT_CSV = _csv([
    (0.5, 7.5, 1.12, "2024-07-01T00:00:00+00:00"),
    (0.5, 7.5, 1.12, "2024-07-01T00:30:00+00:00"),
])


class TestApplyCsvImport(unittest.TestCase):
    def setUp(self):
        import types
        eio = types.ModuleType("energy_engine_io"); eio.load_json = lambda *a, **kw: {}
        sys.modules.setdefault("energy_engine_io", eio)
        self.store = BlockStore(":memory:")
        self.store.insert_config_period({"meters": {"electricity_main": {"meta": {
            "billing_day": 1, "block_minutes": 30, "timezone": "UTC",
            "currency_symbol": "£", "currency_code": "GBP", "site": "Home"}}}})

    def _rows(self):
        return self.store._conn.execute(
            "SELECT block_start, imp_kwh, imp_rate, imp_cost, exp_kwh, exp_cost, "
            "standing_charge, source, derivation_id FROM blocks "
            "ORDER BY block_start").fetchall()

    def test_writes_import_blocks(self):
        r = self.store.apply_csv_import({"import": IMPORT_CSV})
        self.assertTrue(r["ok"])
        self.assertEqual(r["channels"]["import"]["blocks_written"], 6)
        rows = self._rows()
        self.assertEqual(len(rows), 6)
        for row in rows:
            self.assertEqual(row["source"], IMPORTED_SOURCE_CSV)
        # off-peak block: cost £0.07, rate ~0.07.
        first = rows[0]
        self.assertAlmostEqual(first["imp_cost"], 0.07, places=4)
        self.assertAlmostEqual(first["imp_rate"], 0.07, places=3)

    def test_offpeak_peak_rates(self):
        self.store.apply_csv_import({"import": IMPORT_CSV})
        rates = sorted({round(r["imp_rate"], 3) for r in self._rows()})
        self.assertEqual(rates, [0.07, 0.25])

    def test_rate_derivation_recorded(self):
        self.store.apply_csv_import({"import": IMPORT_CSV})
        derivs = self.store.list_historical_derivations(scope="rate", subject="import")
        self.assertEqual(len(derivs), 1)
        d = derivs[0]
        self.assertEqual(d["params"]["kind"], "banded")
        # blocks link to it
        linked = self.store._conn.execute(
            "SELECT COUNT(*) FROM blocks WHERE derivation_id = ?", (d["id"],)).fetchone()[0]
        self.assertEqual(linked, 6)

    def test_import_export_merge_one_row(self):
        self.store.apply_csv_import({"import": IMPORT_CSV, "export": EXPORT_CSV})
        rows = self._rows()
        self.assertEqual(len(rows), 6)   # export merged, no new rows
        merged = [r for r in rows if r["block_start"] == "2024-07-01T00:00:00"][0]
        self.assertAlmostEqual(merged["imp_cost"], 0.07, places=4)   # import kept
        self.assertAlmostEqual(merged["exp_cost"], 0.075, places=4)  # export added
        self.assertAlmostEqual(merged["exp_kwh"], 0.5, places=4)

    def test_reconciles(self):
        r = self.store.apply_csv_import({"import": IMPORT_CSV, "export": EXPORT_CSV})
        self.assertTrue(r["channels"]["import"]["reconcile"]["ok"])
        self.assertTrue(r["channels"]["export"]["reconcile"]["ok"])

    def test_standing_per_day_on_blocks(self):
        self.store.apply_csv_import({"import": IMPORT_CSV})
        # 6 import blocks that day × £0.0112 = £0.0672 daily, stored on each block.
        sc = {round(r["standing_charge"], 4) for r in self._rows()}
        self.assertEqual(sc, {0.0672})

    def test_override_changes_rate_and_confirmed(self):
        # Confirm the off-peak tier at 8p instead of the derived 7p.
        self.store.apply_csv_import(
            {"import": IMPORT_CSV},
            overrides={"import": {"0": {"off_peak": 0.08}}})
        rates = sorted({round(r["imp_rate"], 3) for r in self._rows()})
        self.assertEqual(rates, [0.08, 0.25])       # off-peak overridden, peak kept
        d = self.store.list_historical_derivations(scope="rate", subject="import")[0]
        self.assertAlmostEqual(d["confirmed_value"], 0.08, places=6)

    def test_reconstructed_only_delete_removes_them(self):
        self.store.apply_csv_import({"import": IMPORT_CSV})
        prev = self.store.count_blocks_for_date_range(
            "2024-07-01", "2024-07-01", tz_name="UTC", reconstructed_only=True)
        self.assertEqual(prev["blocks"], 6)
        res = self.store.delete_blocks_for_date_range(
            "2024-07-01", "2024-07-01", tz_name="UTC", reconstructed_only=True)
        self.assertEqual(res["deleted"], 6)


_RATEFIRST_HEADER = ("Start,End,Consumption (kWh),Unit Rate (p/kWh),"
                     "Estimated Cost Inc. Tax (p),Standing Charge Inc. Tax (p)\n")


def _ratefirst_csv(rows):
    """rows: (kwh, rate_p, standing_p_or_blank, start_iso). Rate-first (Cost blank)."""
    out = _RATEFIRST_HEADER
    for kwh, rate_p, sc, start in rows:
        out += f"{start},{start},{kwh},{rate_p},,{sc}\n"
    return out


class TestApplyCsvRateFirst(unittest.TestCase):
    """A rate-FIRST CSV (explicit Unit Rate per row — e.g. an Octopus HH bill table
    transcribed exactly) must store the EXACT per-slot rate, not the tier aggregate.
    Two near rates that the cost-model would cluster into one blended tier stay
    distinct, and cost stays consistent with rate × kWh."""

    def setUp(self):
        import types
        eio = types.ModuleType("energy_engine_io"); eio.load_json = lambda *a, **kw: {}
        sys.modules.setdefault("energy_engine_io", eio)
        self.store = BlockStore(":memory:")
        self.store.insert_config_period({"meters": {"electricity_main": {"meta": {
            "billing_day": 1, "block_minutes": 30, "timezone": "UTC",
            "currency_symbol": "£", "currency_code": "GBP", "site": "Home"}}}})

    def test_stores_exact_per_slot_rate(self):
        csv = _ratefirst_csv([
            (2.0, 7.50, "1.12", "2024-07-01T00:00:00+00:00"),
            (1.0, 8.00, "",     "2024-07-01T00:30:00+00:00"),
        ])
        self.store.apply_csv_import({"import": csv})
        rows = self.store._conn.execute(
            "SELECT block_start, imp_rate, imp_cost, imp_kwh FROM blocks "
            "ORDER BY block_start").fetchall()
        # Exact per-slot rates preserved — NOT a ~0.0767 blended aggregate.
        self.assertAlmostEqual(rows[0]["imp_rate"], 0.075, places=4)
        self.assertAlmostEqual(rows[1]["imp_rate"], 0.080, places=4)
        # Cost = rate × kWh, consistent with the stored rate.
        self.assertAlmostEqual(rows[0]["imp_cost"], 0.15, places=4)
        self.assertAlmostEqual(rows[1]["imp_cost"], 0.08, places=4)

    def test_cost_only_csv_still_aggregates(self):
        # No rate column → the old cost-model path: blocks in a tier share the
        # aggregate rate (unchanged behaviour).
        self.store.apply_csv_import({"import": IMPORT_CSV})
        rates = sorted({round(r["imp_rate"], 3) for r in self.store._conn.execute(
            "SELECT imp_rate FROM blocks").fetchall()})
        self.assertIn(0.07, rates)          # off-peak aggregate
        self.assertIn(0.25, rates)          # peak aggregate


class TestFirstManWins(unittest.TestCase):
    """First-man-wins: a CSV/bill import must NEVER overwrite a block that already
    holds data — not live/settled readings, not an earlier import. The user can't
    tell exactly where their CSV butts up against existing data, so whoever wrote a
    channel first keeps it; to change a block, delete it and re-fill. Only a channel
    that's currently empty is filled (so import+export of a fresh block both land)."""

    def setUp(self):
        import types
        eio = types.ModuleType("energy_engine_io"); eio.load_json = lambda *a, **kw: {}
        sys.modules.setdefault("energy_engine_io", eio)
        self.store = BlockStore(":memory:")
        self.store.insert_config_period({"meters": {"electricity_main": {"meta": {
            "billing_day": 1, "block_minutes": 30, "timezone": "UTC",
            "currency_symbol": "£", "currency_code": "GBP", "site": "Home"}}}})

    def _rows(self):
        return self.store._conn.execute(
            "SELECT block_start, imp_kwh, imp_rate, imp_cost, exp_kwh, exp_cost, "
            "source FROM blocks ORDER BY block_start").fetchall()

    def test_reimport_does_not_overwrite(self):
        first = self.store.apply_csv_import({"import": IMPORT_CSV})
        self.assertEqual(first["blocks_written"], 6)
        self.assertEqual(first["blocks_skipped"], 0)
        # Re-import the SAME span with different numbers — nothing must change.
        bigger = _csv([(9.9, 999.0, 9.99, s)
                       for (_k, _c, _s, s) in [
                           (0, 0, 0, "2024-07-01T00:00:00+00:00"),
                           (0, 0, 0, "2024-07-01T00:30:00+00:00"),
                           (0, 0, 0, "2024-07-01T01:00:00+00:00"),
                           (0, 0, 0, "2024-07-01T01:30:00+00:00"),
                           (0, 0, 0, "2024-07-01T18:00:00+00:00"),
                           (0, 0, 0, "2024-07-01T18:30:00+00:00")]])
        second = self.store.apply_csv_import({"import": bigger})
        self.assertEqual(second["blocks_written"], 0)
        self.assertEqual(second["blocks_skipped"], 6)
        self.assertEqual(second["channels"]["import"]["blocks_skipped"], 6)
        rows = self._rows()
        self.assertEqual(len(rows), 6)               # no new rows
        self.assertAlmostEqual(rows[0]["imp_kwh"], 1.0, places=4)     # original kept
        self.assertAlmostEqual(rows[0]["imp_cost"], 0.07, places=4)   # NOT 9.99

    def test_live_block_never_clobbered(self):
        cp = self.store.get_current_config_period_id()
        self.store._conn.execute(
            "INSERT INTO blocks (block_start, block_end, meter_id, config_period_id, "
            "interpolated, imp_kwh, imp_rate, imp_cost, standing_charge, source) "
            "VALUES (?,?,?,?,0,?,?,?,?,NULL)",
            ("2024-07-01T00:00:00", "2024-07-01T00:30:00", "electricity_main",
             cp, 4.4, 0.44, 4.4, 0.0))
        self.store._conn.commit()
        res = self.store.apply_csv_import({"import": IMPORT_CSV})
        self.assertEqual(res["blocks_skipped"], 1)   # the live slot
        self.assertEqual(res["blocks_written"], 5)   # the other five
        live = [r for r in self._rows() if r["block_start"] == "2024-07-01T00:00:00"][0]
        self.assertIsNone(live["source"])            # still LIVE, not re-tagged
        self.assertAlmostEqual(live["imp_kwh"], 4.4, places=4)   # untouched
        self.assertAlmostEqual(live["imp_cost"], 4.4, places=4)

    def test_empty_export_channel_still_fills(self):
        # Import first (creates rows with export empty), then a LATER export CSV
        # must fill the empty export column of those rows — that's not an overwrite.
        self.store.apply_csv_import({"import": IMPORT_CSV})
        res = self.store.apply_csv_import({"export": EXPORT_CSV})
        self.assertEqual(res["channels"]["export"]["blocks_written"], 2)
        self.assertEqual(res["channels"]["export"]["blocks_skipped"], 0)
        merged = [r for r in self._rows()
                  if r["block_start"] == "2024-07-01T00:00:00"][0]
        self.assertAlmostEqual(merged["imp_kwh"], 1.0, places=4)    # import kept
        self.assertAlmostEqual(merged["exp_kwh"], 0.5, places=4)    # export added


class TestApplyRearmsCarbonBackfill(unittest.TestCase):
    """Importing new history must clear the carbon-backfill 'done' marker, or the
    freshly-added (NULL-carbon) span sits at 0% forever because the backfill thinks
    it already finished. Same-MPAN history inherits its period's region, so no region
    change is needed — just a re-scan."""

    def setUp(self):
        import types
        eio = types.ModuleType("energy_engine_io"); eio.load_json = lambda *a, **kw: {}
        sys.modules.setdefault("energy_engine_io", eio)
        self.store = BlockStore(":memory:")
        self.store.insert_config_period({"meters": {"electricity_main": {"meta": {
            "billing_day": 1, "block_minutes": 30, "timezone": "UTC",
            "currency_symbol": "£", "currency_code": "GBP", "site": "Home"}}}})

    def test_import_clears_done_marker(self):
        self.store.set_meta("carbon_backfill_state", {"done": True})
        self.store.apply_csv_import({"import": IMPORT_CSV})
        state = self.store.get_meta("carbon_backfill_state", {}) or {}
        self.assertFalse(state.get("done"))     # re-armed → backfill will re-scan

    def test_no_rearm_when_nothing_written(self):
        # Re-import the same span → first-man-wins skips everything → marker untouched.
        self.store.apply_csv_import({"import": IMPORT_CSV})
        self.store.set_meta("carbon_backfill_state", {"done": True})
        r = self.store.apply_csv_import({"import": IMPORT_CSV})
        self.assertEqual(r["blocks_written"], 0)
        state = self.store.get_meta("carbon_backfill_state", {}) or {}
        self.assertTrue(state.get("done"))      # nothing new → left as-is


if __name__ == "__main__":
    unittest.main()
