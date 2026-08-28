"""BL-53 step 1 + BL-57 — rate_source authority column, measured_cost store,
the reconcile/rerun authority gate, the corrections exc restamp, and the
pre-migration upgrade snapshot.
"""
import glob
import os
import tempfile
import unittest
import zipfile

from block_store import BlockStore


def _mk_store():
    p = tempfile.mktemp(suffix=".db")
    st = BlockStore(p)
    st._conn.execute("PRAGMA foreign_keys=OFF")
    return st, p


def _ins(c, bs, *, rate=None, rc=0, rr=0, rs=None, kwh=None,
         rate_exc=None, cost_exc=None, cost=None):
    c.execute(
        "INSERT INTO blocks (block_start, block_end, config_period_id, meter_id, "
        "imp_kwh, imp_rate, imp_cost, imp_rate_exc, imp_cost_exc, rate_corrected, "
        "rate_reconciled, rate_source) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
        (bs, bs[:-2] + "30", 1, "electricity_main", kwh, rate, cost, rate_exc,
         cost_exc, rc, rr, rs))


_BACKFILL = ("UPDATE blocks SET rate_source = CASE "
             "WHEN rate_corrected = 1 THEN 'corrected' "
             "WHEN rate_reconciled = 1 THEN 'reconciled' "
             "WHEN imp_rate IS NOT NULL THEN 'schedule' END WHERE rate_source IS NULL")
_GATE = "(rate_source IS NULL OR rate_source NOT IN ('measured','corrected'))"


class TestSchemaAndBackfill(unittest.TestCase):
    def test_column_and_table_exist(self):
        st, p = _mk_store()
        cols = [r[1] for r in st._conn.execute("PRAGMA table_info(blocks)")]
        self.assertIn("rate_source", cols)
        self.assertTrue(st._conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='measured_cost'").fetchone())
        os.remove(p)

    def test_backfill_derivation_and_idempotent(self):
        st, p = _mk_store(); c = st._conn
        _ins(c, "2026-01-01T00:00:00", rate=0.30, rc=1)
        _ins(c, "2026-01-01T00:30:00", rate=0.05, rr=1)
        _ins(c, "2026-01-01T01:00:00", rate=0.30)
        _ins(c, "2026-01-01T01:30:00")               # no rate → NULL
        c.commit()
        c.execute(_BACKFILL); c.commit()
        got = {r[0]: r[1] for r in c.execute("SELECT block_start, rate_source FROM blocks")}
        self.assertEqual(got["2026-01-01T00:00:00"], "corrected")
        self.assertEqual(got["2026-01-01T00:30:00"], "reconciled")
        self.assertEqual(got["2026-01-01T01:00:00"], "schedule")
        self.assertIsNone(got["2026-01-01T01:30:00"])
        # a measured row is never downgraded by a re-run
        c.execute("UPDATE blocks SET rate_source='measured' WHERE block_start='2026-01-01T01:00:00'")
        c.commit(); c.execute(_BACKFILL); c.commit()
        self.assertEqual(c.execute(
            "SELECT rate_source FROM blocks WHERE block_start='2026-01-01T01:00:00'").fetchone()[0],
            "measured")
        os.remove(p)


class TestMeasuredCostStore(unittest.TestCase):
    def test_upsert_get_missing(self):
        st, p = _mk_store()
        st.upsert_measured_cost("2026-08-23T10:00:00", mpan="X", cost_incl=1.0294,
                                cost_excl=0.98, label="STANDARD_RATE", kwh=3.186)
        got = st.get_measured_cost("2026-08-23T10:00:00", mpan="X")
        self.assertEqual(got["cost_incl"], 1.0294)
        self.assertEqual(got["label"], "STANDARD_RATE")
        # upsert overwrites
        st.upsert_measured_cost("2026-08-23T10:00:00", mpan="X", cost_incl=2.0, label="OFF_PEAK")
        self.assertEqual(st.get_measured_cost("2026-08-23T10:00:00", mpan="X")["cost_incl"], 2.0)
        miss = st.measured_slots_missing(
            ["2026-08-23T10:00:00", "2026-08-23T10:30:00"], mpan="X")
        self.assertEqual(miss, ["2026-08-23T10:30:00"])
        os.remove(p)


class TestAuthorityGate(unittest.TestCase):
    def test_reconcile_query_skips_measured_and_corrected(self):
        st, p = _mk_store(); c = st._conn
        _ins(c, "2026-08-24T09:00:00", rate=0.05, rs="measured")
        _ins(c, "2026-08-24T09:30:00", rate=0.05, rc=1, rs="corrected")
        _ins(c, "2026-08-24T10:00:00", rate=0.05, rr=1, rs="reconciled")
        _ins(c, "2026-08-24T10:30:00", rate=0.32, rs="schedule")
        c.commit()
        sel = [r[0] for r in c.execute(
            f"SELECT block_start FROM blocks WHERE meter_id='electricity_main' "
            f"AND rate_corrected=0 AND {_GATE} ORDER BY block_start")]
        self.assertNotIn("2026-08-24T09:00:00", sel)   # measured
        self.assertNotIn("2026-08-24T09:30:00", sel)   # corrected (rate_corrected=1)
        self.assertIn("2026-08-24T10:00:00", sel)      # reconciled — still eligible
        self.assertIn("2026-08-24T10:30:00", sel)      # schedule — eligible
        os.remove(p)


class TestCorrectionsExc(unittest.TestCase):
    def test_exc_restamped_preserves_vat_ratio(self):
        st, p = _mk_store(); c = st._conn
        _ins(c, "2026-08-23T10:00:00", kwh=2.0, rate=0.30, cost=0.60,
             rate_exc=0.2857, cost_exc=0.5714)      # VAT ~1.05
        c.commit()
        value = 0.20
        c.execute(
            """UPDATE blocks SET imp_rate=?, imp_cost=ROUND(imp_kwh*?,6),
                 imp_rate_exc=CASE WHEN imp_rate_exc IS NOT NULL AND imp_rate!=0
                                   THEN ROUND(imp_rate_exc*?/imp_rate,6) ELSE imp_rate_exc END,
                 imp_cost_exc=CASE WHEN imp_cost_exc IS NOT NULL AND imp_rate!=0
                                   THEN ROUND(imp_cost_exc*?/imp_rate,6) ELSE imp_cost_exc END,
                 rate_corrected=1, rate_source='corrected'
               WHERE block_start='2026-08-23T10:00:00'""",
            [value, value, value, value])
        c.commit()
        r = dict(c.execute("SELECT imp_rate, imp_rate_exc, imp_cost_exc, rate_source "
                           "FROM blocks").fetchone())
        self.assertAlmostEqual(r["imp_rate_exc"], round(0.2857 * 0.20 / 0.30, 6), places=6)
        self.assertAlmostEqual(r["imp_cost_exc"], round(0.5714 * 0.20 / 0.30, 6), places=6)
        self.assertAlmostEqual(r["imp_rate"] / r["imp_rate_exc"], 0.30 / 0.2857, places=3)  # VAT preserved
        self.assertEqual(r["rate_source"], "corrected")
        os.remove(p)


class TestPreUpgradeBackup(unittest.TestCase):
    def test_snapshot_created_gated_idempotent(self):
        import engine
        d = tempfile.mkdtemp()
        saveD, saveB, saveS = engine.DATA_DIR, engine.BLOCKS_DB_PATH, engine.SHARE_BACKUP_DIR
        try:
            engine.DATA_DIR = d
            engine.BLOCKS_DB_PATH = os.path.join(d, "blocks.db")
            engine.SHARE_BACKUP_DIR = os.path.join(d, "share")
            open(engine.BLOCKS_DB_PATH, "wb").write(b"db")
            open(engine.BLOCKS_DB_PATH + "-wal", "wb").write(b"wal")
            engine._pre_upgrade_backup_if_needed()
            zips = glob.glob(os.path.join(d, "share", "backups", "*_pre_upgrade_*.zip"))
            self.assertEqual(len(zips), 1)
            self.assertIn("blocks.db-wal", zipfile.ZipFile(zips[0]).namelist())
            engine._pre_upgrade_backup_if_needed()   # same version → no new zip
            self.assertEqual(len(glob.glob(os.path.join(d, "share", "backups", "*.zip"))), 1)
        finally:
            engine.DATA_DIR, engine.BLOCKS_DB_PATH, engine.SHARE_BACKUP_DIR = saveD, saveB, saveS


if __name__ == "__main__":
    unittest.main()
