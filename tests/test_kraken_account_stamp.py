"""#357: a DB is stamped with its Octopus account on first successful discovery.

When the credentials present are for a DIFFERENT account than the loaded DB was
stamped with (operator swapped to another user's DB), the API must NOT auto-activate
— that would poll the wrong account's data into this DB. Credentials are KEPT either
way; an explicit in-app reconnect (force=True) re-associates the DB.
"""
import asyncio
import os
import tempfile
import unittest
from unittest.mock import patch

import engine
from block_store import BlockStore


class _FakeClient:
    """Minimal async KrakenAPIClient stand-in for discovery."""
    def __init__(self, api_key, account_number=None, base_url=None):
        self.account_number = account_number

    async def test_connection(self):
        return {"ok": True, "account_number": self.account_number}

    async def auto_discover(self, acct):
        return {"account_number": acct, "import": {"mpan": "1234"}, "export": {},
                "properties": [], "warnings": []}

    async def close(self):
        return None


class TestKrakenAccountStamp(unittest.TestCase):

    def setUp(self):
        self._orig_store = engine._store
        self._store = BlockStore(":memory:")
        engine._store = self._store
        engine.set_data_source_mode("api")          # mode_uses_api() → True
        # Credentials in a temp file (never touch the real /data path).
        self._orig_path = engine.KRAKEN_CREDS_PATH
        self._orig_data = engine.DATA_DIR
        self._dir = tempfile.mkdtemp()
        engine.DATA_DIR = self._dir
        engine.KRAKEN_CREDS_PATH = os.path.join(self._dir, "kraken_credentials.json")
        self._orig_env = {k: os.environ.pop(k, None) for k in
                          ("KRAKEN_API_KEY", "KRAKEN_ACCOUNT_NUMBER", "KRAKEN_BASE_URL")}
        engine._kraken_client = None
        engine._kraken_discovery = None
        engine._kraken_account_mismatch = None

    def tearDown(self):
        import shutil
        engine._store = self._orig_store
        engine.KRAKEN_CREDS_PATH = self._orig_path
        engine.DATA_DIR = self._orig_data
        for k, v in self._orig_env.items():
            if v is not None:
                os.environ[k] = v
        shutil.rmtree(self._dir, ignore_errors=True)
        engine._kraken_client = None
        engine._kraken_discovery = None
        engine._kraken_account_mismatch = None

    def _discover(self, force=False):
        async def _go():
            with patch("kraken_api_client.KrakenAPIClient", _FakeClient), \
                 patch.object(engine, "_maybe_setup_mini", side_effect=_noop):
                await engine._kraken_startup_discovery(force=force)
        loop = asyncio.new_event_loop()
        try:
            loop.run_until_complete(_go())
        finally:
            loop.close()

    def test_first_discovery_stamps_the_db(self):
        engine.save_kraken_credentials("k", "A-OWNER")
        self._discover()
        self.assertEqual(engine.get_db_account(), "A-OWNER")
        self.assertIsNone(engine.kraken_account_mismatch())
        self.assertIsNotNone(engine._kraken_discovery)

    def test_matching_account_connects(self):
        self._store.set_kraken_state(engine._ACCOUNT_KEY, "A-OWNER")
        engine.save_kraken_credentials("k", "A-OWNER")
        self._discover()
        self.assertIsNone(engine.kraken_account_mismatch())
        self.assertIsNotNone(engine._kraken_client)

    def test_mismatch_blocks_activation_and_keeps_credentials(self):
        # DB belongs to another user; my credentials are for a different account.
        self._store.set_kraken_state(engine._ACCOUNT_KEY, "A-OTHERUSER")
        engine.save_kraken_credentials("k", "A-OWNER")
        self._discover(force=False)
        mm = engine.kraken_account_mismatch()
        self.assertEqual(mm, ("A-OTHERUSER", "A-OWNER"))
        self.assertIsNone(engine._kraken_client)          # not activated
        self.assertFalse(engine._kraken_discovery)        # no MPANs → no polling
        # Credentials are KEPT — never silently dropped.
        self.assertTrue(os.path.exists(engine.KRAKEN_CREDS_PATH))
        self.assertEqual(engine._kraken_env()["api_key"], "k")
        # The stamp is untouched (still the other user's account).
        self.assertEqual(engine.get_db_account(), "A-OTHERUSER")

    def test_case_insensitive_match_is_not_a_mismatch(self):
        self._store.set_kraken_state(engine._ACCOUNT_KEY, "a-owner")
        engine.save_kraken_credentials("k", "A-OWNER")
        self._discover(force=False)
        self.assertIsNone(engine.kraken_account_mismatch())

    def test_explicit_reconnect_reassociates(self):
        # force=True is the in-app "connect" — the operator deliberately binds this
        # DB to the new account, so re-stamp instead of blocking.
        self._store.set_kraken_state(engine._ACCOUNT_KEY, "A-OTHERUSER")
        engine.save_kraken_credentials("k", "A-OWNER")
        self._discover(force=True)
        self.assertIsNone(engine.kraken_account_mismatch())
        self.assertEqual(engine.get_db_account(), "A-OWNER")
        self.assertIsNotNone(engine._kraken_client)


async def _noop(*a, **k):
    return None


if __name__ == "__main__":
    unittest.main()
