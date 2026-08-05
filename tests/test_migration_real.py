"""
test_migration_real.py — retired in 4.0.0.

This was a manual CLI verification script for the legacy JSON->SQLite migration
(block_store.migrate_json_to_sqlite), which was removed in 4.0.0. The migration
shim no longer exists, so this script has nothing to verify. It is kept only as
an inert placeholder (the file cannot be deleted in this workspace) and defines
no active tests.
"""
import unittest


class TestMigrationRealRetired(unittest.TestCase):
    @unittest.skip("JSON->SQLite migration removed in 4.0.0")
    def test_retired(self):
        pass


if __name__ == "__main__":
    unittest.main()
