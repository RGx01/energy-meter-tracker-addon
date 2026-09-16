"""Regression: a settlement that merely re-states an imported figure is not a change.

An IMPORTED block already holds the supplier's own half-hourly kWh — the historical
import fetched it from the same API. Its `*_kwh_api` column is NULL only because it
never went through DCC settlement locally, not because the number is an estimate.

`classify_kraken_block` treated that NULL as "the figure changed" (`prev_api is None`),
so the first settlement pass over imported history queued every block for a full
PASS 2 + 3b re-run. On a fresh install that imports two years, the 400-day backfill
poll re-fetched 19,129 slots and flagged all of them; measured on the reporting DB,
19,129 of 19,129 matched `imp_kwh` exactly (total delta 0.000000 kWh), so 13,779
blocks were re-materialised against a kWh that had not moved.

The figure is still STORED — provenance stays honest and the corrections tool's
settled-only gate opens. Only the re-run flag is withheld.
"""
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from block_store import BlockStore

CFG = {"meters": {"electricity_main": {"meta": {
    "timezone": "UTC", "billing_day": 1, "block_minutes": 30,
    "currency_symbol": "£", "currency_code": "GBP"}}}}
SLOT = "2026-05-01T00:00:00"


class _Base(unittest.TestCase):
    def setUp(self):
        self.st = BlockStore(":memory:")
        self.st.insert_config_period(CFG)

    def _block(self, *, imp=4.0, exp=0.0, source="imported_api", api=None):
        self.st._conn.execute(
            "INSERT INTO blocks (block_start, block_end, meter_id, config_period_id, "
            "imp_kwh, exp_kwh, source, imp_kwh_api) VALUES (?,?,?,?,?,?,?,?)",
            (SLOT, "2026-05-01T00:30:00", "electricity_main", 1, imp, exp, source, api))
        self.st._conn.commit()

    def _classify(self, settled, channel="import"):
        return self.st.classify_kraken_block(SLOT, "electricity_main", settled,
                                             channel=channel, billing_source="api")

    def _row(self):
        return self.st._conn.execute(
            "SELECT imp_kwh_api, needs_pass2_rerun FROM blocks "
            "WHERE block_start=? AND meter_id='electricity_main'", (SLOT,)).fetchone()


class TestImportedRestatement(_Base):
    def test_identical_figure_does_not_queue_a_rerun(self):
        """THE regression: the supplier confirming what the import already wrote."""
        self._block(imp=4.0)
        v = self._classify(4.0)
        self.assertEqual(v["status"], "stored", "the figure must still be stored")
        self.assertEqual(v["needs_pass2_rerun"], 0,
                         "an unchanged kWh must not queue a PASS 2 re-run")

    def test_the_figure_is_still_written(self):
        """Provenance stays honest; the corrections tool's settled gate opens."""
        self._block(imp=4.0)
        self.st.upsert_kraken_block(SLOT, "electricity_main", 4.0,
                                    channel="import", billing_source="api")
        r = self._row()
        self.assertAlmostEqual(r["imp_kwh_api"], 4.0, places=6)
        self.assertEqual(r["needs_pass2_rerun"], 0)

    def test_a_genuinely_different_figure_still_queues(self):
        """A real correction to imported history must still re-materialise."""
        self._block(imp=4.0)
        self.assertEqual(self._classify(4.25)["needs_pass2_rerun"], 1)

    def test_a_hair_over_the_tolerance_still_queues(self):
        self._block(imp=4.0)
        self.assertEqual(self._classify(4.0 + 2e-6)["needs_pass2_rerun"], 1)

    def test_within_tolerance_does_not_queue(self):
        self._block(imp=4.0)
        self.assertEqual(self._classify(4.0 + 1e-7)["needs_pass2_rerun"], 0)

    def test_export_channel_behaves_the_same(self):
        self._block(imp=0.0, exp=2.5)
        self.assertEqual(self._classify(2.5, channel="export")["needs_pass2_rerun"], 0)
        self.assertEqual(self._classify(2.9, channel="export")["needs_pass2_rerun"], 1)


class TestLiveBlocksUnchanged(_Base):
    """A LIVE block's kWh IS an estimate, so first settlement must still re-run —
    even in the rare case where the estimate happened to match exactly."""

    def test_live_block_first_settlement_still_queues(self):
        self._block(imp=4.0, source="kraken_api")
        self.assertEqual(self._classify(4.0)["needs_pass2_rerun"], 1)

    def test_legacy_null_source_still_queues(self):
        self._block(imp=4.0, source=None)
        self.assertEqual(self._classify(4.0)["needs_pass2_rerun"], 1)


class TestResettlementUnchanged(_Base):
    """Once *_kwh_api is populated the existing comparison governs, imported or not."""

    def test_second_settlement_same_figure_does_not_queue(self):
        self._block(imp=4.0, api=4.0)
        self.assertEqual(self._classify(4.0)["needs_pass2_rerun"], 0)

    def test_second_settlement_changed_figure_queues(self):
        self._block(imp=4.0, api=4.0)
        self.assertEqual(self._classify(4.5)["needs_pass2_rerun"], 1)

    def test_live_resettlement_changed_figure_queues(self):
        self._block(imp=4.0, api=4.0, source="kraken_api")
        self.assertEqual(self._classify(4.5)["needs_pass2_rerun"], 1)


if __name__ == "__main__":
    unittest.main()


class TestSettlementPreservesProvenance(_Base):
    """`source` records how a block came into existence; `*_kwh_api` records that the
    supplier has since confirmed the figure. Settlement writes the second, never the
    first — three import-scoped behaviours key off the tag, and a repair function
    anchors go-live to the earliest non-imported block."""

    def _settle(self, kwh=4.0):
        self.st.upsert_kraken_block(SLOT, "electricity_main", kwh,
                                    channel="import", billing_source="api")

    def _src(self):
        return self.st._conn.execute(
            "SELECT source FROM blocks WHERE block_start=? AND meter_id='electricity_main'",
            (SLOT,)).fetchone()["source"]

    def test_an_imported_tag_survives_settlement(self):
        self._block(imp=4.0, source="imported_api")
        self._settle()
        self.assertEqual(self._src(), "imported_api",
                         "settlement must not restate where the block came from")

    def test_a_csv_import_tag_survives_too(self):
        self._block(imp=4.0, source="imported_csv")
        self._settle()
        self.assertEqual(self._src(), "imported_csv")

    def test_an_untagged_block_still_gets_tagged(self):
        self._block(imp=4.0, source=None)
        self._settle()
        self.assertEqual(self._src(), "kraken_api",
                         "a block with no provenance must still be tagged")

    def test_a_live_tag_is_unchanged(self):
        self._block(imp=4.0, source="kraken_api")
        self._settle()
        self.assertEqual(self._src(), "kraken_api")

    def test_the_settled_figure_still_lands(self):
        self._block(imp=4.0, source="imported_api")
        self._settle()
        self.assertAlmostEqual(self._row()["imp_kwh_api"], 4.0, places=6)

    def test_the_rollback_filter_still_sees_the_block(self):
        """Delete Blocks -> reconstructed history is `source LIKE 'imported%'`."""
        self._block(imp=4.0, source="imported_api")
        self._settle()
        n = self.st._conn.execute(
            "SELECT COUNT(*) FROM blocks WHERE source LIKE 'imported%'").fetchone()[0]
        self.assertEqual(n, 1, "settling must not shrink the import rollback set")

    def test_go_live_anchor_is_not_dragged_backwards(self):
        """retag_untagged_imports anchors go-live to the earliest live-signature block."""
        self._block(imp=4.0, source="imported_api")          # 2026-05-01, history
        self.st._conn.execute(
            "INSERT INTO blocks (block_start, block_end, meter_id, config_period_id, "
            "imp_kwh, source) VALUES (?,?,?,?,?,?)",
            ("2026-06-01T00:00:00", "2026-06-01T00:30:00", "electricity_main", 1,
             1.0, "kraken_api"))
        self.st._conn.commit()
        self._settle()
        anchor = self.st._conn.execute(
            "SELECT MIN(block_start) FROM blocks "
            "WHERE imp_read_start IS NOT NULL OR source = 'kraken_api'").fetchone()[0]
        self.assertEqual(anchor, "2026-06-01T00:00:00",
                         "a settled import must not become the go-live anchor")

    def test_an_ha_sensor_block_is_still_relabelled(self):
        """Deliberate existing behaviour (test_kraken_schema): a live sensor block whose
        figure DCC-settles genuinely becomes API-sourced. Only IMPORT tags are held."""
        self._block(imp=4.0, source="ha_sensor")
        self._settle()
        self.assertEqual(self._src(), "kraken_api")
