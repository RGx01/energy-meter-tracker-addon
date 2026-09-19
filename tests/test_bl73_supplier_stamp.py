"""BL-73 — the supplier registry-key stamp in store_meta.

v5's importer reads this to decide which supplier's credential prompt to show on
the restore path, so the contract these tests pin is: the stamp is a NORMALISED
key, it is absent rather than a placeholder when unknown, and 'not-listed' is a
real answer that must never be read as Octopus.
"""
import os
import sqlite3
import tempfile
import unittest

import engine
from block_store import BlockStore


def _mk_store(supplier=None, with_period=True):
    p = tempfile.mktemp(suffix=".db")
    st = BlockStore(p)
    st._conn.execute("PRAGMA foreign_keys=OFF")
    if with_period:
        st._conn.execute(
            "INSERT INTO config_periods (effective_from, billing_day, block_minutes, "
            "timezone, supplier) VALUES ('2026-01-01', 1, 30, 'Europe/London', ?)",
            (supplier,))
        st._conn.commit()
    return st, p


class TestStampValue(unittest.TestCase):
    def test_wizard_key_stamped_as_is(self):
        st, p = _mk_store("octopus")
        self.assertEqual(engine.stamp_supplier(st), "octopus")
        self.assertEqual(st.get_meta("supplier"), "octopus")

    def test_legacy_free_text_normalises(self):
        """Every real database predates the dropdown and holds display text."""
        for raw in ("Octopus Energy", "octopus energy", "  OCTOPUS  "):
            st, p = _mk_store(raw)
            self.assertEqual(engine.stamp_supplier(st), "octopus",
                             "%r should normalise to the registry key" % raw)
            self.assertEqual(st.get_meta("supplier"), "octopus")

    def test_non_octopus_is_not_listed_not_octopus(self):
        """The case a reader must not get wrong: a local-only install."""
        for raw in ("not-listed", "British Gas", "EDF"):
            st, p = _mk_store(raw)
            self.assertEqual(engine.stamp_supplier(st), "not-listed",
                             "%r must not be read as Octopus" % raw)
            self.assertEqual(st.get_meta("supplier"), "not-listed")

    def test_token_is_lowercase(self):
        """Guards against drift toward the display string ('Octopus Energy')."""
        st, p = _mk_store("Octopus Energy")
        key = engine.stamp_supplier(st)
        self.assertEqual(key, key.lower())
        self.assertNotIn(" ", key)


class TestAbsence(unittest.TestCase):
    """Absent must stay absent: v5 branches on 'never established'."""

    def test_unset_supplier_writes_nothing(self):
        for raw in (None, "", "   "):
            st, p = _mk_store(raw)
            self.assertIsNone(engine.stamp_supplier(st))
            self.assertIsNone(st.get_meta("supplier"),
                              "a placeholder would read as a real answer")

    def test_no_config_period_writes_nothing(self):
        st, p = _mk_store(with_period=False)
        self.assertIsNone(engine.stamp_supplier(st))
        self.assertIsNone(st.get_meta("supplier"))

    def test_no_store_is_survivable(self):
        self.assertIsNone(engine.stamp_supplier(None))


class TestRestamping(unittest.TestCase):
    def test_idempotent(self):
        st, p = _mk_store("Octopus Energy")
        self.assertEqual(engine.stamp_supplier(st), "octopus")
        self.assertEqual(engine.stamp_supplier(st), "octopus")
        self.assertEqual(st.get_meta("supplier"), "octopus")

    def test_follows_a_supplier_change(self):
        """A new config period is how a supplier change is recorded; the stamp
        answers 'whose credentials now', so it must move with it."""
        st, p = _mk_store("Octopus Energy")
        self.assertEqual(engine.stamp_supplier(st), "octopus")
        st._conn.execute(
            "INSERT INTO config_periods (effective_from, billing_day, block_minutes, "
            "timezone, supplier) VALUES ('2026-06-01', 1, 30, 'Europe/London', 'not-listed')")
        st._conn.commit()
        self.assertEqual(engine.stamp_supplier(st), "not-listed")
        self.assertEqual(st.get_meta("supplier"), "not-listed")


class TestSurvivesBackup(unittest.TestCase):
    def test_stamp_survives_a_backup_copy(self):
        """store_meta lives in blocks.db, which is what _backup_to_share copies —
        so the stamp reaches v5 in the backup zip. Asserted, not assumed."""
        st, p = _mk_store("Octopus Energy")
        engine.stamp_supplier(st)
        dst_path = tempfile.mktemp(suffix=".db")
        src = sqlite3.connect(p)
        dst = sqlite3.connect(dst_path)
        try:
            src.backup(dst)
        finally:
            src.close()
            dst.close()
        copy = BlockStore(dst_path)
        self.assertEqual(copy.get_meta("supplier"), "octopus")


class TestNormalisationContract(unittest.TestCase):
    """stamp_supplier is only ever as good as the mapping beneath it."""

    def test_api_capability_agrees_with_the_stamp(self):
        self.assertTrue(engine.supplier_is_api_capable("octopus"))
        self.assertFalse(engine.supplier_is_api_capable("not-listed"))
        self.assertFalse(engine.supplier_is_api_capable(""))


if __name__ == "__main__":
    unittest.main()
