"""A bill may replace an imported block ONLY inside a period the supplier never costed.

First-man-wins protects data a user cannot get back — live meter readings, and their own
manual corrections. It is relaxed in exactly one place: an agreement where the API
returned no costs at all, so what EMT holds is usage x published rate rather than what
was billed. These tests pin both halves: that the relaxation happens where it should,
and that nothing else moves.
"""
import unittest

from block_store import BlockStore, IMPORTED_SOURCE_API, _in_span

CFG = {"meters": {"electricity_main": {
    "meta": {"timezone": "Europe/London", "billing_day": 1, "block_minutes": 30,
             "currency_symbol": "£", "sub_meter": False},
    "channels": {"import": {"read": "sensor.imp", "rate": ""}, "export": {"read": "", "rate": ""}}}}}

BLANK = [{"from": "2025-01-01T00:00:00", "to": "2025-02-01T00:00:00"}]
INSIDE = "2025-01-15T12:00:00"
OUTSIDE = "2025-03-15T12:00:00"


class TestSpanMembership(unittest.TestCase):
    def test_half_open(self):
        self.assertFalse(_in_span(BLANK, "2024-12-31T23:30:00"))
        self.assertTrue(_in_span(BLANK, "2025-01-01T00:00:00"))
        self.assertFalse(_in_span(BLANK, "2025-02-01T00:00:00"))

    def test_open_ended_and_empty(self):
        self.assertTrue(_in_span([{"from": "2025-01-01T00:00:00", "to": None}], "2030-01-01T00:00:00"))
        self.assertFalse(_in_span([], INSIDE))
        self.assertFalse(_in_span(None, INSIDE))


class TestReplaceability(unittest.TestCase):
    def setUp(self):
        self.s = BlockStore(":memory:")
        self.s.insert_config_period(CFG)

    def _seed(self, start, *, source=IMPORTED_SOURCE_API, corrected=0, rate_source=None):
        self.s.upsert_imported_block(start, "electricity_main", "import",
                                     kwh=1.0, rate=0.30, cost=0.30, source=source)
        if corrected or rate_source:
            with self.s._conn:
                self.s._conn.execute(
                    "UPDATE blocks SET rate_corrected = ?, rate_source = ? "
                    "WHERE block_start = ? AND meter_id = 'electricity_main'",
                    (corrected, rate_source, start))

    def ok(self, start):
        return self.s.imported_block_replaceable(start, "electricity_main", "import")

    def test_empty_slot_is_an_insert_not_a_replace(self):
        self.assertEqual(self.ok(INSIDE), (False, "empty"))

    def test_imported_block_is_replaceable(self):
        self._seed(INSIDE)
        self.assertEqual(self.ok(INSIDE), (True, "replaceable"))

    def test_live_reading_is_never_replaceable(self):
        self._seed(INSIDE, source="kraken_api")
        self.assertEqual(self.ok(INSIDE), (False, "live"))

    def test_user_correction_is_never_replaceable(self):
        self._seed(INSIDE, corrected=1)
        self.assertEqual(self.ok(INSIDE), (False, "corrected"))
        self.s = BlockStore(":memory:"); self.s.insert_config_period(CFG)
        self._seed(INSIDE, rate_source="corrected")
        self.assertEqual(self.ok(INSIDE), (False, "corrected"))

    def test_writer_refuses_overwrite_of_protected_data(self):
        # Defence in depth: even a caller that asks for overwrite=True cannot clobber
        # a live reading or a correction.
        for kind, kw in (("live", {"source": "kraken_api"}), ("corrected", {"corrected": 1})):
            s = BlockStore(":memory:"); s.insert_config_period(CFG)
            self.s = s
            self._seed(INSIDE, **kw)
            _bid, wrote = s.upsert_imported_block(
                INSIDE, "electricity_main", "import", kwh=9.9, rate=0.05, cost=0.495,
                overwrite=True)
            self.assertFalse(wrote, kind)
            row = s._conn.execute(
                "SELECT imp_kwh, imp_cost FROM blocks WHERE block_start = ?",
                (INSIDE,)).fetchone()
            self.assertAlmostEqual(row["imp_kwh"], 1.0, places=6)     # untouched
            self.assertAlmostEqual(row["imp_cost"], 0.30, places=6)

    def test_writer_replaces_an_imported_block_on_overwrite(self):
        self._seed(INSIDE)
        _bid, wrote = self.s.upsert_imported_block(
            INSIDE, "electricity_main", "import", kwh=2.0, rate=0.04493, cost=0.08986,
            overwrite=True)
        self.assertTrue(wrote)
        row = self.s._conn.execute(
            "SELECT imp_kwh, imp_rate, imp_cost FROM blocks WHERE block_start = ?",
            (INSIDE,)).fetchone()
        # kwh/rate/cost move together — the bill's figures are internally consistent.
        self.assertAlmostEqual(row["imp_kwh"], 2.0, places=6)
        self.assertAlmostEqual(row["imp_rate"], 0.04493, places=6)
        self.assertAlmostEqual(row["imp_cost"], 0.08986, places=6)

    def test_first_man_wins_still_applies_without_overwrite(self):
        self._seed(INSIDE)
        _bid, wrote = self.s.upsert_imported_block(
            INSIDE, "electricity_main", "import", kwh=2.0, rate=0.04, cost=0.08)
        self.assertFalse(wrote)
        row = self.s._conn.execute(
            "SELECT imp_kwh FROM blocks WHERE block_start = ?", (INSIDE,)).fetchone()
        self.assertAlmostEqual(row["imp_kwh"], 1.0, places=6)


if __name__ == "__main__":
    unittest.main()
