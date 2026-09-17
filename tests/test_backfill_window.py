"""Regression: the first-run settlement backfill is sized by what still needs settling.

`_kraken_backfill_days` sets how far back the DCC settlement poll reaches when it has
no cursor. It measured from the OLDEST BLOCK, so a fresh install that imported two
years of history immediately swept the full 400-day cap — ~19,000 half-hourly rows per
poll, of figures the importer had already fetched from the same API (measured on the
reporting DB: 19,129 of 19,129 identical, total delta 0.000000 kWh).

An imported block needs no settling. `_UNSETTLED_WHERE` already excludes `imported%`,
so the window is now measured from the oldest block that is genuinely awaiting
settlement — the same predicate the poll uses to chase lagging settlement.
"""
import os
import sys
import types
import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

eio = types.ModuleType("energy_engine_io")
eio.ensure_dir = lambda *a, **kw: None
eio.load_json = lambda *a, **kw: a[1] if len(a) > 1 else {}
eio.save_json_atomic = lambda *a, **kw: None
eio.save_file = lambda *a, **kw: None
sys.modules["energy_engine_io"] = eio
ec = types.ModuleType("energy_charts")
ec.generate_net_heatmap = lambda *a, **kw: ""
ec.generate_daily_import_export_charts = lambda *a, **kw: ""
sys.modules["energy_charts"] = ec
hc = types.ModuleType("ha_client"); hc.HAClient = MagicMock
sys.modules["ha_client"] = hc

from block_store import BlockStore
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import engine

CFG = {"meters": {"electricity_main": {"meta": {
    "timezone": "UTC", "billing_day": 1, "block_minutes": 30,
    "currency_symbol": "£", "currency_code": "GBP"}}}}


class TestBackfillWindow(unittest.TestCase):
    def setUp(self):
        self._s = engine._store
        engine._store = BlockStore(":memory:")
        engine._store.insert_config_period(CFG)

    def tearDown(self):
        engine._store = self._s

    def _block(self, days_ago, *, source, api=None):
        t = (datetime.now(timezone.utc).replace(tzinfo=None)
             - timedelta(days=days_ago)).replace(minute=0, second=0, microsecond=0)
        engine._store._conn.execute(
            "INSERT INTO blocks (block_start, block_end, meter_id, config_period_id, "
            "imp_kwh, source, imp_kwh_api) VALUES (?,?,?,?,?,?,?)",
            (t.isoformat(), (t + timedelta(minutes=30)).isoformat(),
             "electricity_main", 1, 1.0, source, api))
        engine._store._conn.commit()

    def test_fresh_db_does_not_backfill(self):
        self.assertEqual(engine._kraken_backfill_days(), 0)

    def test_imported_history_alone_does_not_backfill(self):
        """THE regression: two years of imported blocks used to force the 400-day cap."""
        for d in (730, 365, 30, 1):
            self._block(d, source="imported_api")
        self.assertEqual(engine._kraken_backfill_days(), 0,
                         "imported blocks already carry the supplier's figure")

    def test_settled_live_history_does_not_backfill(self):
        for d in (400, 200, 5):
            self._block(d, source="kraken_api", api=1.0)
        self.assertEqual(engine._kraken_backfill_days(), 0)

    def test_an_unsettled_live_block_sizes_the_window(self):
        self._block(730, source="imported_api")       # history — ignored
        self._block(3, source="kraken_api")           # genuinely awaiting settlement
        self.assertEqual(engine._kraken_backfill_days(), 4,
                         "window reaches the oldest block that still needs settling")

    def test_an_untagged_legacy_block_still_counts(self):
        self._block(6, source=None)
        self.assertEqual(engine._kraken_backfill_days(), 7)

    def test_the_cap_still_applies(self):
        self._block(900, source="kraken_api")
        self.assertEqual(engine._kraken_backfill_days(),
                         engine._KRAKEN_BACKFILL_CAP_DAYS)

    def test_the_window_covers_its_own_anchor(self):
        """The poll floors the unsettled anchor at `now - backfill_days`; if the window
        were shorter than the anchor's age the block could never be reached."""
        self._block(730, source="imported_api")
        self._block(11, source="kraken_api")
        days = engine._kraken_backfill_days()
        anchor = engine._store.get_oldest_unsettled_block_start()
        floor = (datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(days=days))
        self.assertLessEqual(floor.isoformat(), anchor,
                             "the horizon must reach the oldest unsettled block")


if __name__ == "__main__":
    unittest.main()


class TestProbeFailureIsLoud(unittest.TestCase):
    """A store that cannot answer must not look like a store with nothing to settle.

    Returning 0 is the safe direction — never hammer the API on an unknown state — but
    it LATCHES: the ingester keeps backfill_days for its lifetime, and the poll floors
    its own unsettled anchor at `now - backfill_days`, so a horizon of 0 clamps that
    anchor to now. Settlement then stops reaching back with no signal at all.
    """

    class _BrokenStore:
        def get_oldest_unsettled_block_start(self, *a, **kw):
            raise RuntimeError("database is locked")
        def get_oldest_block_start(self, *a, **kw):
            raise RuntimeError("database is locked")

    def setUp(self):
        self._s = engine._store
        engine._store = self._BrokenStore()

    def tearDown(self):
        engine._store = self._s

    def test_it_still_returns_zero(self):
        self.assertEqual(engine._kraken_backfill_days(), 0,
                         "safe direction: do not backfill on an unknown state")

    def test_it_warns(self):
        with self.assertLogs("engine", level="WARNING") as cm:
            engine._kraken_backfill_days()
        joined = "\n".join(cm.output)
        self.assertIn("oldest unsettled", joined)
        self.assertIn("database is locked", joined,
                      "the underlying cause must reach the log")
