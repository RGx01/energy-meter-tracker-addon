"""BL-5: instance identity + backup-directory isolation.

/data is per-instance however EMT was installed, so blocks.db is already safe.
/share is shared across add-ons in supervised mode — two instances would list and
restore each other's backups. These tests pin the namespacing and the three
silent rename cases.
"""

import os
import shutil
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import instance as I


class TestSlugify(unittest.TestCase):
    def test_basic(self):
        self.assertEqual(I.slugify_site("Highgrove"), "highgrove")

    def test_apostrophes_are_dropped_not_separated(self):
        self.assertEqual(I.slugify_site("Mum's House"), "mums_house")
        self.assertEqual(I.slugify_site("Mum\u2019s House"), "mums_house")

    def test_spaces_and_punctuation(self):
        self.assertEqual(I.slugify_site("Flat 2b"), "flat_2b")
        self.assertEqual(I.slugify_site("  A -- B  "), "a_b")

    def test_empty_falls_back(self):
        self.assertEqual(I.slugify_site(""), "site")
        self.assertEqual(I.slugify_site(None), "site")
        self.assertEqual(I.slugify_site("!!!"), "site")

    def test_length_capped(self):
        self.assertLessEqual(len(I.slugify_site("A" * 200)), 48)


class _SandboxBase(unittest.TestCase):
    """Redirect /share and /data into a temp dir; run as supervised."""

    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self._share, self._legacy = I._SHARE_ROOT, I.LEGACY_SHARE_BACKUP_DIR
        self._mode = os.environ.pop("EMT_MODE", None)
        I._SHARE_ROOT = os.path.join(self.tmp, "share")
        os.makedirs(I._SHARE_ROOT)
        I.LEGACY_SHARE_BACKUP_DIR = os.path.join(
            I._SHARE_ROOT, "energy_meter_tracker_backup")
        self.data = os.path.join(self.tmp, "data")
        os.makedirs(self.data)

    def tearDown(self):
        I._SHARE_ROOT, I.LEGACY_SHARE_BACKUP_DIR = self._share, self._legacy
        if self._mode is not None:
            os.environ["EMT_MODE"] = self._mode
        else:
            os.environ.pop("EMT_MODE", None)
        shutil.rmtree(self.tmp, ignore_errors=True)


class TestInstanceId(_SandboxBase):
    def test_minted_once_and_stable(self):
        a = I.get_instance_id(self.data)
        b = I.get_instance_id(self.data)
        self.assertEqual(a, b)
        self.assertTrue(os.path.exists(os.path.join(self.data, "instance_id")))

    def test_distinct_instances_differ(self):
        other = os.path.join(self.tmp, "data2")
        os.makedirs(other)
        self.assertNotEqual(I.get_instance_id(self.data),
                            I.get_instance_id(other))


class TestStandaloneIsUnaffected(_SandboxBase):
    def test_volume_scoped_and_site_independent(self):
        os.environ["EMT_MODE"] = "standalone"
        a = I.resolve_backup_dir("Highgrove", data_dir=self.data)
        b = I.resolve_backup_dir("Rental", data_dir=self.data)
        self.assertEqual(a, b)                       # rename changes nothing
        self.assertEqual(a, os.path.join(self.data, "backup"))
        self.assertNotIn("/share", a)


class TestBackupDirRename(_SandboxBase):
    """The three silent cases."""

    def test_case1_destination_free_creates_and_marks(self):
        d = I.resolve_backup_dir("Highgrove", data_dir=self.data)
        self.assertTrue(d.endswith("energy_meter_tracker_backup_highgrove"))
        self.assertTrue(I._owned_by_us(d, I.get_instance_id(self.data)))

    def test_case1b_rename_moves_the_directory(self):
        d1 = I.resolve_backup_dir("Highgrove", data_dir=self.data)
        with open(os.path.join(d1, "proof.txt"), "w") as f:
            f.write("backup")
        d2 = I.resolve_backup_dir("Rental", data_dir=self.data, current_dir=d1)
        self.assertTrue(d2.endswith("_rental"))
        self.assertFalse(os.path.exists(d1))                       # moved
        self.assertTrue(os.path.exists(os.path.join(d2, "proof.txt")))

    def test_case2_rename_back_adopts_our_own_directory(self):
        iid = I.get_instance_id(self.data)
        d1 = I.resolve_backup_dir("Highgrove", data_dir=self.data)
        d2 = I.resolve_backup_dir("Rental", data_dir=self.data, current_dir=d1)
        # our old highgrove dir exists again (e.g. restored) and is marked ours
        os.makedirs(d1, exist_ok=True)
        I._write_marker(d1, iid)
        with open(os.path.join(d1, "old.txt"), "w") as f:
            f.write("x")
        d3 = I.resolve_backup_dir("Highgrove", data_dir=self.data, current_dir=d2)
        self.assertEqual(d3, d1)                                   # adopted
        self.assertTrue(os.path.exists(os.path.join(d3, "old.txt")))  # reappear

    def test_case3_foreign_directory_is_never_touched(self):
        foreign = I.share_backup_dir("Shared")
        os.makedirs(foreign)
        I._write_marker(foreign, "another-instance-uuid")
        with open(os.path.join(foreign, "theirs.txt"), "w") as f:
            f.write("x")
        d = I.resolve_backup_dir("Shared", data_dir=self.data)
        self.assertNotEqual(d, foreign)                 # timestamp-suffixed
        self.assertTrue(d.startswith(foreign + "_"))
        self.assertTrue(os.path.exists(os.path.join(foreign, "theirs.txt")))
        self.assertTrue(I._owned_by_us(d, I.get_instance_id(self.data)))

    def test_unmarked_existing_directory_is_treated_as_foreign(self):
        target = I.share_backup_dir("Highgrove")
        os.makedirs(target)                              # no marker
        d = I.resolve_backup_dir("Highgrove", data_dir=self.data)
        self.assertNotEqual(d, target)

    def test_legacy_directory_is_migrated_on_upgrade(self):
        """EMT has only ever supported one instance, so an unmarked legacy dir
        belongs to this install: migrate it, carrying the backups across."""
        os.makedirs(I.LEGACY_SHARE_BACKUP_DIR)
        with open(os.path.join(I.LEGACY_SHARE_BACKUP_DIR, "old.zip"), "w") as f:
            f.write("x")
        d = I.resolve_backup_dir("Highgrove", data_dir=self.data)
        self.assertTrue(d.endswith("_highgrove"))
        self.assertTrue(os.path.exists(os.path.join(d, "old.zip")))  # carried over
        self.assertFalse(os.path.exists(I.LEGACY_SHARE_BACKUP_DIR))
        self.assertTrue(I._owned_by_us(d, I.get_instance_id(self.data)))

    def test_migration_is_idempotent_across_restarts(self):
        os.makedirs(I.LEGACY_SHARE_BACKUP_DIR)
        with open(os.path.join(I.LEGACY_SHARE_BACKUP_DIR, "old.zip"), "w") as f:
            f.write("x")
        d1 = I.resolve_backup_dir("Highgrove", data_dir=self.data)
        d2 = I.resolve_backup_dir("Highgrove", data_dir=self.data)
        self.assertEqual(d1, d2)
        self.assertEqual(len([x for x in os.listdir(d1) if x.endswith(".zip")]), 1)

    def test_migrated_dir_is_protected_from_a_later_second_instance(self):
        """After migration the marker guards the user's backups: a second
        instance with the same site name takes its own directory."""
        os.makedirs(I.LEGACY_SHARE_BACKUP_DIR)
        with open(os.path.join(I.LEGACY_SHARE_BACKUP_DIR, "user.zip"), "w") as f:
            f.write("x")
        mine = I.resolve_backup_dir("Highgrove", data_dir=self.data)
        other = os.path.join(self.tmp, "data2")
        os.makedirs(other)
        theirs = I.resolve_backup_dir("Highgrove", data_dir=other)
        self.assertNotEqual(theirs, mine)
        self.assertTrue(os.path.exists(os.path.join(mine, "user.zip")))

    def test_legacy_marked_by_another_instance_is_never_migrated(self):
        os.makedirs(I.LEGACY_SHARE_BACKUP_DIR)
        I._write_marker(I.LEGACY_SHARE_BACKUP_DIR, "another-instance-uuid")
        with open(os.path.join(I.LEGACY_SHARE_BACKUP_DIR, "theirs.zip"), "w") as f:
            f.write("x")
        d = I.resolve_backup_dir("Highgrove", data_dir=self.data)
        self.assertNotEqual(d, I.LEGACY_SHARE_BACKUP_DIR)
        self.assertTrue(os.path.exists(
            os.path.join(I.LEGACY_SHARE_BACKUP_DIR, "theirs.zip")))


class TestScanFirstOwnership(_SandboxBase):
    """Ownership is resolved by scanning /share for our marker, so an instance
    keeps its own directory even after a restore whose data reports a different
    site name (the prod -> prod dev workflow)."""

    def test_restore_with_colliding_site_name_keeps_own_dir(self):
        # prod owns ..._home; prod dev owns ..._home_dev.
        prod = os.path.join(self.tmp, "data_prod")
        proddev = os.path.join(self.tmp, "data_proddev")
        os.makedirs(prod); os.makedirs(proddev)
        prod_dir = I.resolve_backup_dir("Home", data_dir=prod)
        dev_dir = I.resolve_backup_dir("Home Dev", data_dir=proddev)
        self.assertNotEqual(prod_dir, dev_dir)
        # Restore prod -> prod dev: prod dev's site name now reads "Home".
        # It must keep its OWN dir, never adopt prod's, stable across repeats.
        for _ in range(3):
            got = I.resolve_backup_dir("Home", data_dir=proddev)
            self.assertEqual(got, dev_dir)
            self.assertNotEqual(got, prod_dir)
        self.assertTrue(I._owned_by_us(prod_dir, I.get_instance_id(prod)))
        self.assertTrue(I._owned_by_us(dev_dir, I.get_instance_id(proddev)))

    def test_owned_dir_found_wherever_it_sits(self):
        d = I.resolve_backup_dir("Home", data_dir=self.data)
        self.assertEqual(I._find_owned_dir(I.get_instance_id(self.data)), d)


class TestDataLineage(_SandboxBase):
    """db_uuid travels with the data; a mismatch against the install id means
    the DB was restored from another instance. Ownership never keys on it."""

    def test_native_db_uuid_equals_install_id(self):
        minted = I.ensure_db_uuid(None, data_dir=self.data)
        self.assertEqual(minted, I.get_instance_id(self.data))
        self.assertFalse(
            I.foreign_restore_notice(minted, data_dir=self.data)["foreign"])

    def test_existing_db_uuid_is_immutable(self):
        self.assertEqual(I.ensure_db_uuid("abc123", data_dir=self.data), "abc123")

    def test_foreign_restore_flagged_and_dismissal_sticks_per_lineage(self):
        # A DB carrying another install's id is foreign to this one.
        foreign_id = "f" * 32
        n = I.foreign_restore_notice(foreign_id, data_dir=self.data)
        self.assertTrue(n["foreign"])
        self.assertFalse(n["acknowledged"])
        # Dismiss once; sticks for that lineage (survives re-read / re-restore).
        I.acknowledge_db_uuid(foreign_id, data_dir=self.data)
        n2 = I.foreign_restore_notice(foreign_id, data_dir=self.data)
        self.assertTrue(n2["foreign"])
        self.assertTrue(n2["acknowledged"])
        # A different lineage still surfaces.
        other = I.foreign_restore_notice("e" * 32, data_dir=self.data)
        self.assertTrue(other["foreign"])
        self.assertFalse(other["acknowledged"])


if __name__ == "__main__":
    unittest.main()