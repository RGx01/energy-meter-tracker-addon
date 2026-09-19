"""BL-72 — the software version recorded in the database.

The one thing a database could not previously say about itself. `schema_version`
reads 1 on a 4.3.1 and a 4.5.14 alike, and the upgrade-backup version file lives
in DATA_DIR, which backups do not contain — so a backup carried no record of what
produced it.

Heals are marker-gated and ungated markers run at startup, so "opened by version
X" means "healed to X's standard". A reader (EMT v5) gates on a floor version.
That is the whole mechanism: no computed level, no per-heal invariant.
"""
import os
import sqlite3
import tempfile
import unittest
from unittest import mock

import engine
from block_store import BlockStore


def _mk_store():
    p = tempfile.mktemp(suffix=".db")
    st = BlockStore(p)
    st._conn.execute("PRAGMA foreign_keys=OFF")
    return st, p


class TestStamp(unittest.TestCase):
    def test_stamps_the_running_version(self):
        st, p = _mk_store()
        with mock.patch.object(engine, "_read_config_version", return_value="4.5.14"):
            self.assertEqual(engine.stamp_version(st), "4.5.14")
        self.assertEqual(st.get_meta("written_by"), "4.5.14")

    def test_restamps_on_upgrade(self):
        """Stamped every startup, so it names the LAST version to open the DB —
        which is the version whose heals have run."""
        st, p = _mk_store()
        with mock.patch.object(engine, "_read_config_version", return_value="4.5.13"):
            engine.stamp_version(st)
        self.assertEqual(st.get_meta("written_by"), "4.5.13")
        with mock.patch.object(engine, "_read_config_version", return_value="4.5.14"):
            engine.stamp_version(st)
        self.assertEqual(st.get_meta("written_by"), "4.5.14")

    def test_idempotent(self):
        st, p = _mk_store()
        with mock.patch.object(engine, "_read_config_version", return_value="4.5.14"):
            self.assertEqual(engine.stamp_version(st), "4.5.14")
            self.assertEqual(engine.stamp_version(st), "4.5.14")
        self.assertEqual(st.get_meta("written_by"), "4.5.14")


class TestAbsence(unittest.TestCase):
    def test_unreadable_version_writes_nothing(self):
        st, p = _mk_store()
        for bad in ("unknown", "", None):
            with mock.patch.object(engine, "_read_config_version", return_value=bad):
                self.assertIsNone(engine.stamp_version(st))
        self.assertIsNone(st.get_meta("written_by"))

    def test_unreadable_version_does_not_clobber_a_good_stamp(self):
        """The previous stamp is the last thing known to be true — better than
        overwriting it with 'unknown'."""
        st, p = _mk_store()
        with mock.patch.object(engine, "_read_config_version", return_value="4.5.14"):
            engine.stamp_version(st)
        with mock.patch.object(engine, "_read_config_version", return_value="unknown"):
            engine.stamp_version(st)
        self.assertEqual(st.get_meta("written_by"), "4.5.14")

    def test_no_store_is_survivable(self):
        self.assertIsNone(engine.stamp_version(None))


class TestSurvivesBackup(unittest.TestCase):
    def test_stamp_reaches_a_backup_copy(self):
        """store_meta lives in blocks.db, which is what _backup_to_share copies —
        unlike the DATA_DIR version file, which is precisely the gap this closes."""
        st, p = _mk_store()
        with mock.patch.object(engine, "_read_config_version", return_value="4.5.14"):
            engine.stamp_version(st)
        dst = tempfile.mktemp(suffix=".db")
        a = sqlite3.connect(p); b = sqlite3.connect(dst)
        try:
            a.backup(b)
        finally:
            a.close(); b.close()
        self.assertEqual(BlockStore(dst).get_meta("written_by"), "4.5.14")


class TestReplacesSchemaVersion(unittest.TestCase):
    def test_schema_version_is_not_a_discriminator(self):
        """Why this key exists at all: the pre-existing candidate cannot tell two
        very different databases apart."""
        st, p = _mk_store()
        self.assertEqual(st.get_meta("schema_version") or
                         st._conn.execute(
                             "SELECT value FROM store_meta WHERE key='schema_version'"
                         ).fetchone()[0], 1)


if __name__ == "__main__":
    unittest.main()
