"""
Device cost prices from the GRID-ATTRIBUTED kWh, never from the raw device draw.

PASS 2 costs a sub-meter at `imp_kwh_grid x parent_rate` — the portion of the device's
draw the GRID actually supplied. Two re-cost sites priced from raw `imp_kwh` instead:
the settlement device re-cost in `apply_measured`, and `_smb_device_recost_core`. A
third site (the reconcile/revert path) and the corrections tool in web/server.py
already price from `imp_kwh_grid`, and the corrections tool even states the rule:
*"Device cost = grid-attributed kWh x rate, exactly what PASS 2 computes at finalise."*
So the rule was written down correctly and two of four implementations ignored it.

WHY IT REACHED THE BILL, which is what made it serious rather than cosmetic.
`compute_period_net` builds each day as `max(0, main - SUM(devices)) + SUM(devices)`.
That is algebraically `main` — the device terms cancel — EXCEPT that the clamp stops
them cancelling once the devices exceed the main. So an over-costed device does not
merely mis-state a device line; it inflates the BILL.

MEASURED on two 4.5.14 databases. On a storage-heavy site the clamp converted the
over-cost into a bill error approaching the size of the bill itself, and after the heal
the bill matched the metered figure exactly with the clamp firing on no day at all. On a
site whose battery charges from the grid overnight the effect was negligible and the
bill did not move.

ONSET is the tariff migration, not a release. Migrating switches on settled costing,
which restates the parent rate, which makes the device rate drift, which fires the
re-cost. Two databases on the SAME version were affected from different
days, each its own migration date.

THE KWH COLUMNS WERE NEVER WRONG, which is why this hid for so long. Both re-cost
sites carry a comment promising "kWh is untouched; only the priced rate layer moves",
and that is true of the columns. The cost silently changed WHICH kWh it derived from.
"""
import os, sys, unittest, tempfile
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import engine
from block_store import BlockStore

BS = "2026-09-06T02:00:00"
RATE, RATE_EXC = 0.32720, 0.31162


class _Base(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mktemp(suffix=".db")
        self.store = BlockStore(self.tmp)
        with self.store._conn:
            self.store._conn.execute(
                "INSERT INTO config_periods (billing_day, block_minutes, timezone, "
                "currency_symbol, currency_code, effective_from) "
                "VALUES (1, 30, 'Europe/London', '£', 'GBP', '2026-01-01')")
            self.cp = self.store._conn.execute(
                "SELECT id FROM config_periods LIMIT 1").fetchone()[0]
            for mid, sub in (("electricity_main", 0), ("battery", 1), ("nogrid", 1)):
                self.store._conn.execute(
                    "INSERT OR IGNORE INTO meters (meter_id, config_period_id, is_sub_meter, "
                    "parent_meter_id) VALUES (?,?,?,?)",
                    (mid, self.cp, sub, "electricity_main" if sub else None))
            self.store._conn.execute(
                "INSERT INTO blocks (block_start, block_end, meter_id, config_period_id, "
                "imp_kwh, imp_rate, imp_rate_exc, imp_cost, imp_cost_exc, exp_kwh, exp_cost, "
                "standing_charge) VALUES (?, '2026-09-06T02:30:00', 'electricity_main', ?, "
                "1.0, ?, ?, ?, ?, 0.0, 0.0, 0.0)",
                (BS, self.cp, RATE, RATE_EXC, round(1.0 * RATE, 6), round(1.0 * RATE_EXC, 6)))
            # battery: drew 4 kWh, only 0.01 of it from the grid (rest was solar)
            self.store._conn.execute(
                "INSERT INTO blocks (block_start, block_end, meter_id, config_period_id, "
                "imp_kwh, imp_kwh_grid, imp_rate, imp_rate_exc, imp_cost, imp_cost_exc) "
                "VALUES (?, '2026-09-06T02:30:00', 'battery', ?, 4.0, 0.01, ?, ?, ?, ?)",
                (BS, self.cp, RATE, RATE_EXC, round(0.01 * RATE, 6), round(0.01 * RATE_EXC, 6)))
            # a device PASS 2 never clipped: imp_kwh_grid NULL -> must keep costing from raw
            self.store._conn.execute(
                "INSERT INTO blocks (block_start, block_end, meter_id, config_period_id, "
                "imp_kwh, imp_kwh_grid, imp_rate, imp_rate_exc, imp_cost, imp_cost_exc) "
                "VALUES (?, '2026-09-06T02:30:00', 'nogrid', ?, 0.5, NULL, ?, ?, ?, ?)",
                (BS, self.cp, RATE, RATE_EXC, round(0.5 * RATE, 6), round(0.5 * RATE_EXC, 6)))

    def tearDown(self):
        self.store._conn.close()
        if os.path.exists(self.tmp):
            os.remove(self.tmp)

    def _row(self, mid):
        return self.store._conn.execute(
            "SELECT imp_kwh, imp_kwh_grid, imp_rate, imp_cost, imp_cost_exc "
            "FROM blocks WHERE block_start=? AND meter_id=?", (BS, mid)).fetchone()


class TestHealRepairsRawCostedRows(_Base):

    def _corrupt(self):
        """Write what the OLD re-cost produced: cost from raw imp_kwh."""
        with self.store._conn:
            self.store._conn.execute(
                "UPDATE blocks SET imp_cost = ROUND(COALESCE(imp_kwh,0) * ?, 6), "
                "imp_cost_exc = ROUND(COALESCE(imp_kwh,0) * ?, 6), exc_source='tariff' "
                "WHERE block_start=? AND meter_id != 'electricity_main' AND imp_kwh IS NOT NULL",
                (RATE, RATE_EXC, BS))

    def test_heal_restores_the_clipped_cost(self):
        self._corrupt()
        self.assertAlmostEqual(self._row("battery")["imp_cost"], 4.0 * RATE, places=5)
        res = engine._smb_device_cost_clip_core(self.store)
        self.assertTrue(res["ok"])
        self.assertEqual(res["re_costed"], 1)          # battery only; nogrid is skipped
        r = self._row("battery")
        self.assertAlmostEqual(r["imp_cost"], 0.01 * RATE, places=6)
        self.assertAlmostEqual(r["imp_cost_exc"], 0.01 * RATE_EXC, places=6)

    def test_kwh_columns_are_never_touched(self):
        self._corrupt()
        engine._smb_device_cost_clip_core(self.store)
        r = self._row("battery")
        self.assertEqual(r["imp_kwh"], 4.0)
        self.assertEqual(r["imp_kwh_grid"], 0.01)

    def test_unclipped_device_keeps_costing_from_raw_kwh(self):
        """imp_kwh_grid IS NULL means PASS 2 never clipped — the COALESCE fallback.

        The reporter's database has none of these; a second database has 8,711, so
        this path is real and must not be re-costed to zero.
        """
        self._corrupt()
        engine._smb_device_cost_clip_core(self.store)
        r = self._row("nogrid")
        self.assertIsNone(r["imp_kwh_grid"])
        self.assertAlmostEqual(r["imp_cost"], 0.5 * RATE, places=6)

    def test_idempotent(self):
        self._corrupt()
        self.assertEqual(engine._smb_device_cost_clip_core(self.store)["re_costed"], 1)
        self.assertEqual(engine._smb_device_cost_clip_core(self.store)["re_costed"], 0)

    def test_clean_db_is_a_no_op(self):
        self.assertEqual(engine._smb_device_cost_clip_core(self.store)["re_costed"], 0)

    def test_heal_never_raises_a_cost(self):
        """Direction is DOWN only — measured across two production databases, zero
        rows sat below the clipped figure. The heal must not invent cost."""
        with self.store._conn:
            self.store._conn.execute(
                "UPDATE blocks SET imp_cost = 0.0001 WHERE block_start=? AND meter_id='battery'",
                (BS,))
        engine._smb_device_cost_clip_core(self.store)
        self.assertAlmostEqual(self._row("battery")["imp_cost"], 0.0001, places=6)


class TestTeeth(_Base):
    """The superseded expression, spelled out. If these ever agree the fixture has
    stopped discriminating and the tests above are no longer evidence."""

    def test_raw_and_clipped_forms_differ_on_this_fixture(self):
        raw = round(4.0 * RATE, 6)
        clipped = round(0.01 * RATE, 6)
        self.assertNotAlmostEqual(raw, clipped, places=4)
        self.assertGreater(raw, clipped * 100)

    def test_over_costed_device_would_exceed_the_main(self):
        """This is why it reaches the bill: SUM(devices) > main trips the clamp."""
        main = 1.0 * RATE
        self.assertGreater(4.0 * RATE, main)           # raw form: clamp fires
        self.assertLess(0.01 * RATE + 0.5 * RATE, main)  # clipped form: it cannot


if __name__ == "__main__":
    unittest.main()
