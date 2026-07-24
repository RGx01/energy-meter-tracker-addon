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


if __name__ == "__main__":
    unittest.main()