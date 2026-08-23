"""
§4a SAFETY GUARD + P4.6 correction consistency — the 2026-08-18 capped-DB break.

Two protections against an INC-in-pence / exc-in-£ unit mix on a main-import block:
  (1) engine `_sanitise_inc_units` — the write-side guard at the single main-import chokepoint
      (`_reprice_main_import_block`): any inc rate above the £/kWh ceiling is pence-not-pounds,
      normalised /100 across ALL inc fields (rate/cost + EV split); exc (already £) untouched.
  (2) the Cost-Corrections recalc UPDATE — when a user corrects the import rate, the EV-split
      columns are recomputed in the SAME statement so imp_cost_ev + imp_cost_remainder == imp_cost
      (previously only imp_rate/imp_cost were updated → the split kept the stale figure).
"""
import os, sys, unittest
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import engine
from block_store import BlockStore


class TestIncUnitGuard(unittest.TestCase):
    def test_pence_inc_normalised_exc_untouched(self):
        # The real 03:30 bad block: inc in pence, exc correct £.
        ch = {"kwh": 3.929, "rate": 6.89997, "cost": 27.109982,
              "kwh_ev": 3.55, "rate_ev": 6.89997, "cost_ev": 24.494893,
              "cost_remainder": 2.822088,
              "rate_exc": 0.065714, "cost_exc": 0.258191}
        fixed = engine._sanitise_inc_units(ch, "2026-08-18T03:30:00")
        self.assertTrue(fixed)
        self.assertAlmostEqual(ch["rate"], 0.069, places=3)
        self.assertAlmostEqual(ch["cost"], 0.271, places=3)
        self.assertAlmostEqual(ch["rate_ev"], 0.069, places=3)
        self.assertAlmostEqual(ch["cost_ev"], 0.244949, places=5)
        self.assertAlmostEqual(ch["cost_remainder"], 0.028221, places=5)
        # exc must NOT be touched (already £)
        self.assertEqual(ch["rate_exc"], 0.065714)
        self.assertEqual(ch["cost_exc"], 0.258191)
        # corrected inc reconciles with exc × VAT (1.05)
        self.assertAlmostEqual(ch["cost"], ch["cost_exc"] * 1.05, places=3)

    def test_healthy_block_byte_identical(self):
        ch = {"kwh": 2.0, "rate": 0.069, "cost": 0.138,
              "rate_exc": 0.065714, "cost_exc": 0.131}
        snap = dict(ch)
        self.assertFalse(engine._sanitise_inc_units(ch, "2026-08-18T03:00:00"))
        self.assertEqual(ch, snap)   # untouched

    def test_idempotent(self):
        ch = {"kwh": 3.929, "rate": 6.89997, "cost": 27.109982}
        engine._sanitise_inc_units(ch, "x")
        again = dict(ch)
        self.assertFalse(engine._sanitise_inc_units(ch, "x"))  # already sane
        self.assertEqual(ch, again)


# ── P4.6: the correction-tool recalc UPDATE keeps the EV split consistent ────────
# This runs the SAME statement shipped in web/server.py api_corrections_apply
# (import + recalc branch) against an in-memory block, and asserts the invariant.
_CORR_RECALC_SQL = (
    "UPDATE blocks SET "
    "  imp_rate = ?, "
    "  imp_cost = ROUND(imp_kwh * ?, 6), "
    "  imp_rate_ev = CASE WHEN imp_kwh_ev IS NOT NULL THEN ? ELSE imp_rate_ev END, "
    "  imp_cost_ev = CASE WHEN imp_kwh_ev IS NOT NULL THEN ROUND(imp_kwh_ev * ?, 6) ELSE imp_cost_ev END, "
    "  imp_cost_remainder = CASE WHEN imp_kwh_ev IS NOT NULL "
    "       THEN ROUND(imp_kwh * ? - ROUND(imp_kwh_ev * ?, 6), 6) ELSE imp_cost_remainder END "
    "WHERE block_start = ? AND meter_id = 'electricity_main'")


def _store_with_block(**cols):
    st = BlockStore(":memory:")
    with st._conn:
        cp = st._conn.execute(
            "INSERT INTO config_periods (effective_from, billing_day, block_minutes, "
            "timezone, currency_symbol, currency_code) "
            "VALUES ('2024-01-01T00:00:00',1,30,'UTC','\u00a3','GBP')").lastrowid
        st._conn.execute("INSERT INTO meters (config_period_id, meter_id, is_sub_meter, "
                         "meter_type) VALUES (?, 'electricity_main', 0, '')", (cp,))
    keys = ",".join(cols)
    ph = ",".join("?" * len(cols))
    st._conn.execute(
        f"INSERT INTO blocks (block_start, block_end, meter_id, config_period_id, interpolated, {keys}) "
        f"VALUES ('2026-08-18T03:30:00','2026-08-18T04:00:00','electricity_main',?,0,{ph})",
        (cp, *cols.values()))
    st._conn.commit()
    return st


class TestCorrectionRecalcSplit(unittest.TestCase):
    def test_ev_block_split_stays_consistent(self):
        # a block priced 100x (pence) with an EV split, corrected to 0.069 £/kWh
        st = _store_with_block(imp_kwh=3.929, imp_rate=6.89997, imp_cost=27.109982,
                               imp_kwh_ev=3.55, imp_rate_ev=6.89997, imp_cost_ev=24.494893,
                               imp_cost_remainder=2.822088)
        v = 0.069
        st._conn.execute(_CORR_RECALC_SQL, [v, v, v, v, v, v, "2026-08-18T03:30:00"])
        st._conn.commit()
        r = st._conn.execute("SELECT imp_rate, imp_cost, imp_rate_ev, imp_cost_ev, imp_cost_remainder "
                             "FROM blocks WHERE meter_id='electricity_main'").fetchone()
        self.assertAlmostEqual(r["imp_rate"], 0.069, places=6)
        self.assertAlmostEqual(r["imp_cost"], round(3.929*v, 6), places=6)
        self.assertAlmostEqual(r["imp_rate_ev"], 0.069, places=6)
        self.assertAlmostEqual(r["imp_cost_ev"], round(3.55*v, 6), places=6)
        # the invariant: EV + house == total
        self.assertAlmostEqual(r["imp_cost_ev"] + r["imp_cost_remainder"], r["imp_cost"], places=6)

    def test_non_ev_block_split_columns_untouched(self):
        st = _store_with_block(imp_kwh=2.0, imp_rate=6.9, imp_cost=13.8)  # no EV split
        v = 0.069
        st._conn.execute(_CORR_RECALC_SQL, [v, v, v, v, v, v, "2026-08-18T03:30:00"])
        st._conn.commit()
        r = st._conn.execute("SELECT imp_rate, imp_cost, imp_cost_ev, imp_rate_ev, imp_cost_remainder "
                             "FROM blocks WHERE meter_id='electricity_main'").fetchone()
        self.assertAlmostEqual(r["imp_rate"], 0.069, places=6)
        self.assertAlmostEqual(r["imp_cost"], 0.138, places=6)
        self.assertIsNone(r["imp_cost_ev"])
        self.assertIsNone(r["imp_rate_ev"])
        self.assertIsNone(r["imp_cost_remainder"])



# ── P4.10 (watch #11): a rate correction rescales block_segments too ─────────────
# Mirrors the segment UPDATE shipped in api_corrections_apply (import + recalc):
# every import segment on the corrected blocks takes the one corrected rate, so the
# segment invariant Σ(kwh×inc_rate) == imp_cost holds against the recomputed cost.
_CORR_SEG_SQL = ("UPDATE block_segments SET "
                 "  exc_rate = CASE WHEN exc_rate IS NOT NULL AND inc_rate != 0 "
                 "                  THEN ROUND(exc_rate * ? / inc_rate, 6) ELSE exc_rate END, "
                 "  inc_rate = ? "
                 "WHERE channel = 'import' AND block_start IN (?)")


class TestCorrectionSegmentRescale(unittest.TestCase):
    def _seg_store(self):
        st = _store_with_block(imp_kwh=4.0, imp_rate=6.9, imp_cost=27.6)  # priced in pence
        # two import segments on the block, both at the pence rate (a cap-boundary block)
        for seq, (kwh, band, attr) in enumerate(
                [(3.0, "off_peak", "house"), (1.0, "peak", "house")]):
            st._conn.execute(
                "INSERT INTO block_segments (block_start, meter_id, channel, seq, kwh, "
                "inc_rate, exc_rate, band, attribution) VALUES "
                "('2026-08-18T03:30:00','electricity_main','import',?,?,6.9,6.571,?,?)",
                (seq, kwh, band, attr))
        st._conn.commit()
        return st

    def test_segments_follow_correction_and_reconcile(self):
        st = self._seg_store()
        v = 0.069
        # correct the block columns (P4.8 SQL) …
        st._conn.execute(_CORR_RECALC_SQL, [v, v, v, v, v, v, "2026-08-18T03:30:00"])
        # … and rescale its segments (P4.10 SQL)
        st._conn.execute(_CORR_SEG_SQL, [v, v, "2026-08-18T03:30:00"])
        st._conn.commit()
        imp_cost = st._conn.execute(
            "SELECT imp_cost FROM blocks WHERE meter_id='electricity_main'").fetchone()["imp_cost"]
        rows = st._conn.execute(
            "SELECT kwh, inc_rate, exc_rate FROM block_segments "
            "WHERE block_start='2026-08-18T03:30:00' AND channel='import'").fetchall()
        # every segment now on the corrected rate
        for r in rows:
            self.assertAlmostEqual(r["inc_rate"], v, places=6)
            # exc rescaled proportionally (P4.18): pence 6.571 → £, ratio preserved
            self.assertAlmostEqual(r["exc_rate"], round(6.571 * v / 6.9, 6), places=6)
            self.assertLess(r["exc_rate"], r["inc_rate"] * 1.001)   # exc <= inc (VAT), never pence-inflated
        # segment invariant: Σ(kwh×inc_rate) == imp_cost
        seg_cost = round(sum(r["kwh"] * r["inc_rate"] for r in rows), 6)
        self.assertAlmostEqual(seg_cost, imp_cost, places=6)
        # Σ kwh unchanged == imp_kwh
        self.assertAlmostEqual(sum(r["kwh"] for r in rows), 4.0, places=6)


if __name__ == "__main__":
    unittest.main()
