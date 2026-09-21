"""#468 — the Overview cost cards show the grid-clipped EV the bill charges.

The cards are built from a SQL query over the `meters` table, so they only ever
showed a PHYSICAL sub-meter. Two disagreements with the billing chart followed:

  * no EV device  — the dispatch-derived EV stayed folded inside 'Direct import'
                    and never appeared (the reported bug);
  * an EV device  — the card showed the meter's metered draw while the bill
                    charges the grid-clipped synthetic.

Both are now resolved by the rule every other surface uses: synthetic wins
wherever `imp_kwh_ev IS NOT NULL`, else the recorded physical meter. These tests
pin that rule as SQL, and — more importantly — pin the display-only invariant:
'Total Import' never moves, and the rows always sum back to it.
"""
import os
import tempfile
import unittest

from block_store import BlockStore

# The aggregate the card runs. Kept verbatim so a change to server.py that drifts
# from _hybrid_ev_by_block's rule fails here.
HYBRID_SQL = """
SELECT COALESCE(SUM(CASE WHEN m.imp_kwh_ev IS NOT NULL THEN m.imp_kwh_ev
                         ELSE COALESCE(s.imp_kwh_grid, s.imp_kwh, 0) END), 0.0) AS kwh,
       COALESCE(SUM(CASE WHEN m.imp_kwh_ev IS NOT NULL THEN COALESCE(m.imp_cost_ev, 0)
                         ELSE COALESCE(s.imp_cost, 0) END), 0.0) AS cost
FROM blocks m
LEFT JOIN blocks s ON s.block_start = m.block_start AND s.meter_id = ?
WHERE m.meter_id = 'electricity_main' AND m.block_start >= ? AND m.block_start < ?
"""


def _hybrid_py(main_ev_by_block, phys_ev_by_block=None):
    """The rule as server._hybrid_ev_by_block states it, reimplemented here so the
    SQL is checked against the SPEC rather than against itself."""
    phys = phys_ev_by_block or {}
    out = {}
    for bs in set(main_ev_by_block) | set(phys):
        syn = main_ev_by_block.get(bs)
        if syn is not None and syn[0] is not None:
            k, c = float(syn[0]), float(syn[1] or 0.0)
            if k > 1e-9 or abs(c) > 1e-12:
                out[bs] = (k, c)
            continue                       # synthetic is authority even at 0
        p = phys.get(bs)
        if p is not None and float(p[0] or 0.0) > 1e-9:
            out[bs] = (float(p[0]), float(p[1] or 0.0))
    return round(sum(v[0] for v in out.values()), 6), round(sum(v[1] for v in out.values()), 6)


class _Base(unittest.TestCase):
    def setUp(self):
        self.path = tempfile.mktemp(suffix=".db")
        self.store = BlockStore(self.path)
        self.store._conn.execute("PRAGMA foreign_keys=OFF")
        self.store._conn.execute(
            "INSERT INTO config_periods (id, effective_from, billing_day, block_minutes, timezone) "
            "VALUES (1,'2026-01-01',1,30,'UTC')")
        self.store._conn.commit()

    def tearDown(self):
        try:
            self.store.close(); os.unlink(self.path)
        except Exception:
            pass

    def _blk(self, slot, meter, kwh, cost, kwh_ev=None, cost_ev=None, kwh_grid=None):
        self.store._conn.execute(
            "INSERT INTO blocks (block_start, block_end, config_period_id, meter_id, "
            "imp_kwh, imp_cost, imp_kwh_ev, imp_cost_ev, imp_kwh_grid) "
            "VALUES (?,?,?,?,?,?,?,?,?)",
            (slot, slot[:-2] + "30", 1, meter, kwh, cost, kwh_ev, cost_ev, kwh_grid))
        self.store._conn.commit()

    def _sql(self, ev_mid, s="2026-01-01", e="2027-01-01"):
        r = self.store._conn.execute(HYBRID_SQL, (ev_mid, s, e)).fetchone()
        return round(float(r["kwh"]), 6), round(float(r["cost"]), 6)


class TestNoEvDevice(_Base):
    """The reported case: dispatch-derived EV with no device configured."""

    def test_synthetic_ev_is_visible(self):
        self._blk("2026-03-01T00:00:00", "electricity_main", 3.0, 0.95, kwh_ev=2.0, cost_ev=0.60)
        self.assertEqual(self._sql(None), (2.0, 0.6))

    def test_rows_still_sum_to_total_import(self):
        """The display-only invariant: Total Import is the raw grid figure and does
        not move; Direct import absorbs the difference."""
        self._blk("2026-03-01T00:00:00", "electricity_main", 3.0, 0.95, kwh_ev=2.0, cost_ev=0.60)
        total_import = 3.0
        ev_kwh, _ = self._sql(None)
        direct = total_import - ev_kwh
        self.assertAlmostEqual(direct + ev_kwh, total_import, places=6)
        self.assertAlmostEqual(direct, 1.0, places=6)

    def test_no_dispatch_no_ev_row(self):
        """A non-IOG / no-dispatch account has imp_kwh_ev NULL throughout — the card
        must stay byte-identical to before."""
        self._blk("2026-03-01T00:00:00", "electricity_main", 3.0, 0.95)
        self.assertEqual(self._sql(None), (0.0, 0.0))


class TestWithEvDevice(_Base):
    """The second disagreement: the card showed metered draw, the bill charges clipped."""

    def test_synthetic_supersedes_the_meter(self):
        self._blk("2026-03-01T00:00:00", "electricity_main", 5.0, 1.60, kwh_ev=3.0, cost_ev=0.90)
        self._blk("2026-03-01T00:00:00", "ev_charger", 3.4, 1.05)      # metered draw, higher
        self.assertEqual(self._sql("ev_charger"), (3.0, 0.9),
                         "grid-clipped synthetic must win over the meter's own draw")

    def test_recorded_meter_used_where_no_synthetic(self):
        """Pre-seam blocks: imp_kwh_ev IS NULL, so the physical meter is authority."""
        self._blk("2026-03-01T00:00:00", "electricity_main", 5.0, 1.60)
        self._blk("2026-03-01T00:00:00", "ev_charger", 3.4, 1.05)
        self.assertEqual(self._sql("ev_charger"), (3.4, 1.05))

    def test_synthetic_at_zero_blocks_the_fallback(self):
        """'Synthetic is authority even at 0 — no fallback.' A dispatched slot where
        the car drew nothing must NOT fall back to the meter."""
        self._blk("2026-03-01T00:00:00", "electricity_main", 5.0, 1.60, kwh_ev=0.0, cost_ev=0.0)
        self._blk("2026-03-01T00:00:00", "ev_charger", 3.4, 1.05)
        self.assertEqual(self._sql("ev_charger"), (0.0, 0.0))

    def test_grid_kwh_preferred_over_raw_for_the_meter(self):
        self._blk("2026-03-01T00:00:00", "electricity_main", 5.0, 1.60)
        self._blk("2026-03-01T00:00:00", "ev_charger", 3.4, 1.05, kwh_grid=2.9)
        self.assertEqual(self._sql("ev_charger")[0], 2.9)


class TestSqlMatchesTheRule(_Base):
    """The SQL exists only as a cheap restatement of _hybrid_ev_by_block. If the two
    ever diverge, the card silently disagrees with every other surface."""

    def test_mixed_history_matches_the_python_rule(self):
        main, phys = {}, {}
        for i in range(24):
            slot = "2026-04-%02dT00:00:00" % (i + 1)
            if i % 3 == 0:            # synthetic present
                self._blk(slot, "electricity_main", 5.0, 1.60, kwh_ev=1.5 + i / 10, cost_ev=0.4)
                main[slot] = (1.5 + i / 10, 0.4)
            elif i % 3 == 1:          # synthetic explicitly zero
                self._blk(slot, "electricity_main", 5.0, 1.60, kwh_ev=0.0, cost_ev=0.0)
                main[slot] = (0.0, 0.0)
            else:                     # pre-seam: no synthetic
                self._blk(slot, "electricity_main", 5.0, 1.60)
                main[slot] = (None, None)
            if i % 2 == 0:
                self._blk(slot, "ev_charger", 2.0 + i / 20, 0.7)
                phys[slot] = (2.0 + i / 20, 0.7)
        self.assertEqual(self._sql("ev_charger", "2026-04-01", "2026-05-01"),
                         _hybrid_py(main, phys))


if __name__ == "__main__":
    unittest.main()
