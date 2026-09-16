"""
BL-65: SETTLEMENT picks the EV-split authority, not the presence of a dispatch row.

  * SETTLED half-hour (rate_source measured/corrected, or imported history) — Octopus has
    priced it into its four buckets, so the BILL decides. Its stored/segmented split is the
    answer and needs no dispatch record: Octopus serves a short rolling dispatch window and
    keeps no history, so no imported slot has one. The absence of a split is equally an
    answer — the bill billed no EV there, and a stray dispatch row must not invent one.
  * UNSETTLED half-hour — still a prediction, so DISPATCH decides exactly as before: no
    completed dispatch, no EV. Where the live split already wrote a stored column, that IS
    this dispatch's split, priced cap-aware across the bands, so it stays preferred over
    re-deriving a pro-rata carve at the block's blended rate (BL-9, unchanged).

Both walks previously keyed off the dispatch map alone, so every bill-derived split was
dropped and the charts drew 100% house while the billing summary — which sources the
EV-attributed segments — showed the EV.
"""

import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from block_store import BlockStore
from energy_charts import _dispatch_ev_slot_map

SLOT_A = "2026-09-01T01:30:00"
SLOT_B = "2026-09-01T02:00:00"
CFG = {"meters": {"electricity_main": {"meta": {}}}}


def _server_ns():
    """Pull the pure functions out of web/server.py without importing Flask."""
    import ast
    _here = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    src = open(os.path.join(_here, "web", "server.py")).read()
    ns = {}
    for n in ast.parse(src).body:
        if isinstance(n, ast.FunctionDef) and n.name in (
                "_dispatch_ev_split_by_bucket", "_block_settled"):
            exec(compile(ast.Module([n], []), "<s>", "exec"), ns)
        elif isinstance(n, ast.Assign) and getattr(
                n.targets[0], "id", "") == "_SETTLED_RATE_SOURCES":
            exec(compile(ast.Module([n], []), "<s>", "exec"), ns)
    return ns


def _block(slot, kwh, cost, kwh_ev=None, cost_ev=None, segments=None,
           rate_source=None, source=None):
    imp = {"kwh": kwh, "kwh_total": kwh, "cost": cost, "rate": 0.20, "rate_used": 0.20}
    if kwh_ev is not None:
        imp["kwh_ev"] = kwh_ev
        imp["cost_ev"] = cost_ev
        imp["rate_ev"] = round(cost_ev / kwh_ev, 4) if kwh_ev else 0.0
    if segments is not None:
        imp["segments"] = segments
    mb = {"channels": {"import": imp}}
    if rate_source is not None:
        mb["price_rate_source"] = rate_source
    if source is not None:
        mb["source"] = source
    return {"start": slot, "meters": {"electricity_main": mb}}


def _dispatched(store, slot, kwh=2.0):
    store._conn.execute(
        "INSERT INTO dispatch_history (slot_start, kind, provider, source, energy_kwh, "
        "first_seen, last_seen) VALUES (?, 'completed', 'test', NULL, ?, ?, ?)",
        (slot, kwh, slot, slot))
    store._conn.commit()
    return store


class TestSlotMapAuthority(unittest.TestCase):
    """energy_charts._dispatch_ev_slot_map — billing breakdown / day-table source."""

    # ── SETTLED: the bill decides ───────────────────────────────────────────────
    def test_settled_split_survives_an_empty_dispatch_table(self):
        blocks = [_block(SLOT_A, 4.0, 0.80, kwh_ev=3.0, cost_ev=0.55,
                         rate_source="measured")]
        out = _dispatch_ev_slot_map(BlockStore(":memory:"), blocks, CFG)
        self.assertIn(SLOT_A, out, "bill-derived split dropped with no dispatch row")
        self.assertAlmostEqual(out[SLOT_A]["kwh"], 3.0, places=9)
        self.assertAlmostEqual(out[SLOT_A]["cost"], 0.55, places=9)

    def test_imported_history_counts_as_settled(self):
        blocks = [_block(SLOT_A, 4.0, 0.80, kwh_ev=3.0, cost_ev=0.55,
                         source="imported_api")]
        out = _dispatch_ev_slot_map(BlockStore(":memory:"), blocks, CFG)
        self.assertAlmostEqual(out[SLOT_A]["kwh"], 3.0, places=9)

    def test_ev_segment_alone_qualifies_a_settled_slot(self):
        segs = [{"kwh": 2.5, "inc_rate": 0.07, "attribution": "ev"},
                {"kwh": 1.5, "inc_rate": 0.28, "attribution": "house"}]
        out = _dispatch_ev_slot_map(
            BlockStore(":memory:"),
            [_block(SLOT_A, 4.0, 0.60, segments=segs, rate_source="measured")], CFG)
        self.assertAlmostEqual(out[SLOT_A]["kwh"], 2.5, places=9)
        self.assertAlmostEqual(out[SLOT_A]["cost"], 2.5 * 0.07, places=9)

    def test_settled_with_no_split_is_not_carved_from_a_dispatch_row(self):
        """The guard: the bill billed no EV here, so a stray dispatch row invents none."""
        st = _dispatched(BlockStore(":memory:"), SLOT_A)
        out = _dispatch_ev_slot_map(
            st, [_block(SLOT_A, 4.0, 0.80, rate_source="measured")], CFG)
        self.assertEqual(out, {})

    def test_settled_split_is_clipped_to_the_slot_grid_import(self):
        blocks = [_block(SLOT_A, 2.0, 0.40, kwh_ev=3.0, cost_ev=0.60,
                         rate_source="measured")]
        out = _dispatch_ev_slot_map(BlockStore(":memory:"), blocks, CFG)
        self.assertAlmostEqual(out[SLOT_A]["kwh"], 2.0, places=9)
        self.assertAlmostEqual(out[SLOT_A]["cost"], 0.40, places=9)

    # ── UNSETTLED: dispatch decides, as before ──────────────────────────────────
    def test_unsettled_without_dispatch_has_no_ev(self):
        """A prediction needs something to predict from."""
        out = _dispatch_ev_slot_map(
            BlockStore(":memory:"),
            [_block(SLOT_A, 4.0, 0.80, kwh_ev=3.0, cost_ev=0.55)], CFG)
        self.assertEqual(out, {})

    def test_unsettled_with_dispatch_carves_pro_rata(self):
        st = _dispatched(BlockStore(":memory:"), SLOT_B, kwh=2.0)
        out = _dispatch_ev_slot_map(st, [_block(SLOT_B, 4.0, 0.80)], CFG)
        self.assertAlmostEqual(out[SLOT_B]["kwh"], 2.0, places=9)
        self.assertAlmostEqual(out[SLOT_B]["cost"], 0.40, places=9)

    def test_unsettled_prefers_the_stored_cap_aware_split(self):
        """BL-9 unchanged: the stored column is this dispatch's split, better priced."""
        st = _dispatched(BlockStore(":memory:"), SLOT_B, kwh=2.0)
        out = _dispatch_ev_slot_map(
            st, [_block(SLOT_B, 4.0, 0.80, kwh_ev=2.0, cost_ev=0.11)], CFG)
        self.assertAlmostEqual(out[SLOT_B]["kwh"], 2.0, places=9)
        self.assertAlmostEqual(out[SLOT_B]["cost"], 0.11, places=9)   # not the 0.40 carve

    def test_no_authority_anywhere_is_empty(self):
        out = _dispatch_ev_slot_map(
            BlockStore(":memory:"), [_block(SLOT_A, 4.0, 0.80)], CFG)
        self.assertEqual(out, {})


class TestBucketSplitAuthority(unittest.TestCase):
    """web/server.py._dispatch_ev_split_by_bucket — Usage Stats / charts source."""

    def setUp(self):
        ns = _server_ns()
        self.fn = ns["_dispatch_ev_split_by_bucket"]
        self.settled = ns["_block_settled"]
        self.day = lambda s: s[:10]

    # ── SETTLED ─────────────────────────────────────────────────────────────────
    def test_settled_slots_need_no_dispatch(self):
        main = {SLOT_A: (4.0, 0.80), SLOT_B: (2.0, 0.40)}
        stored = {SLOT_A: (3.0, 0.55), SLOT_B: (1.0, 0.18)}
        out = self.fn(main, {}, self.day, stored_by_slot=stored,
                      settled_slots={SLOT_A, SLOT_B})
        self.assertAlmostEqual(out["2026-09-01"]["kwh"], 4.0, places=9)
        self.assertAlmostEqual(out["2026-09-01"]["cost"], 0.73, places=9)

    def test_settled_without_a_split_is_never_carved(self):
        out = self.fn({SLOT_A: (4.0, 0.80)}, {SLOT_A: 2.0}, self.day,
                      settled_slots={SLOT_A})
        self.assertEqual(out, {}, "a dispatch row invented EV the bill did not bill")

    def test_settled_split_clipped_to_grid(self):
        out = self.fn({SLOT_A: (2.0, 0.40)}, {}, self.day,
                      stored_by_slot={SLOT_A: (3.0, 0.60)}, settled_slots={SLOT_A})
        self.assertAlmostEqual(out["2026-09-01"]["kwh"], 2.0, places=9)
        self.assertAlmostEqual(out["2026-09-01"]["cost"], 0.40, places=9)

    # ── UNSETTLED ───────────────────────────────────────────────────────────────
    def test_unsettled_without_dispatch_has_no_ev(self):
        out = self.fn({SLOT_A: (4.0, 0.80)}, {}, self.day,
                      stored_by_slot={SLOT_A: (3.0, 0.55)}, settled_slots=set())
        self.assertEqual(out, {})

    def test_unsettled_prefers_stored_over_the_carve(self):
        out = self.fn({SLOT_A: (4.0, 0.80)}, {SLOT_A: 2.0}, self.day,
                      stored_by_slot={SLOT_A: (2.0, 0.11)}, settled_slots=set())
        self.assertAlmostEqual(out["2026-09-01"]["kwh"], 2.0, places=9)
        self.assertAlmostEqual(out["2026-09-01"]["cost"], 0.11, places=9)

    def test_unsettled_dispatch_only_is_byte_identical(self):
        """The standing guard: the pre-existing pro-rata result, unchanged."""
        main = {SLOT_A: (4.0, 0.80), SLOT_B: (2.0, 0.40)}
        out = self.fn(main, {SLOT_A: 1.0, SLOT_B: 0.5}, self.day, settled_slots=set())
        self.assertAlmostEqual(out["2026-09-01"]["kwh"], 1.5, places=9)
        self.assertAlmostEqual(out["2026-09-01"]["cost"], 0.20 + 0.10, places=9)

    # ── mixed, and the unknown-settlement fallback ──────────────────────────────
    def test_mixed_era_settled_and_unsettled_in_one_bucket(self):
        main = {SLOT_A: (4.0, 0.80), SLOT_B: (2.0, 0.40)}
        out = self.fn(main, {SLOT_B: 1.5}, self.day,
                      stored_by_slot={SLOT_A: (3.0, 0.55)}, settled_slots={SLOT_A})
        self.assertAlmostEqual(out["2026-09-01"]["kwh"], 4.5, places=9)
        self.assertAlmostEqual(out["2026-09-01"]["cost"], 0.55 + 0.30, places=9)

    def test_unknown_settlement_takes_a_stored_split_at_face_value(self):
        """settled_slots=None — the caller cannot say; pre-existing behaviour."""
        out = self.fn({SLOT_A: (4.0, 0.80)}, {}, self.day,
                      stored_by_slot={SLOT_A: (3.0, 0.55)})
        self.assertAlmostEqual(out["2026-09-01"]["kwh"], 3.0, places=9)

    def test_zero_grid_slot_contributes_nothing(self):
        out = self.fn({SLOT_A: (0.0, 0.0)}, {}, self.day,
                      stored_by_slot={SLOT_A: (3.0, 0.60)}, settled_slots={SLOT_A})
        self.assertEqual(out, {})

    def test_bucket_sums_are_order_stable(self):
        slots = ["2026-09-01T%02d:00:00" % h for h in range(12)]
        main = {s: (4.0, 0.8123456789) for s in slots}
        stored = {s: (3.0, 0.5512345678) for s in slots}
        a = self.fn(main, {}, self.day, stored_by_slot=stored, settled_slots=set(slots))
        b = self.fn(main, {}, self.day,
                    stored_by_slot={k: stored[k] for k in reversed(slots)},
                    settled_slots=set(slots))
        self.assertEqual(a["2026-09-01"]["cost"], b["2026-09-01"]["cost"])

    # ── the predicate ───────────────────────────────────────────────────────────
    def test_settled_predicate(self):
        for rs, src, want in [("measured", None, True), ("corrected", None, True),
                              ("schedule", None, False), (None, "imported_api", True),
                              ("schedule", "imported_bill", True),
                              (None, "ha_sensor", False), (None, None, False)]:
            self.assertEqual(self.settled({"rate_source": rs, "source": src}), want,
                             "rate_source=%r source=%r" % (rs, src))

    def test_settled_predicate_tolerates_a_row_without_the_columns(self):
        self.assertFalse(self.settled({}))


if __name__ == "__main__":
    unittest.main()
