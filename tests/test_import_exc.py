"""
Phase-2 Δ4b: ex-VAT is derived in ONE place (engine._apply_import_exc), from the block's
POST-split inc rate x the tariff's exc/inc ratio — not from the tariff schedule rate. This
is what kills the latent capped mismatch (settlement used to scale the pre-split rate). The
key property: exc tracks whatever rate the split/overlay left on the block.
"""
import os, sys, unittest
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import engine


class _Exc:
    def __init__(self, v): self.v = v
    def resolve(self, ts): return self.v


class _Sched:
    """Tariff inc rate 0.30, exc = inc/1.05 (5% VAT). .exc mirrors a RateSchedule."""
    def __init__(self, inc, exc): self._inc = inc; self.exc = _Exc(exc)
    def resolve(self, ts): return self._inc
    def is_empty(self): return False


class TestImportExc(unittest.TestCase):
    def setUp(self):
        self._saved = engine._kraken_rate_schedules
        engine._kraken_rate_schedules = {"import": _Sched(0.30, round(0.30 / 1.05, 8))}

    def tearDown(self):
        engine._kraken_rate_schedules = self._saved

    def test_exc_scales_the_post_split_rate_not_the_tariff_rate(self):
        # the block's rate is 0.05493 (e.g. an overlay/freebie off-peak), NOT the tariff's
        # 0.30 — exc must scale 0.05493, i.e. 0.05493 / 1.05.
        imp = {"rate": 0.05493, "kwh": 2.0}
        applied = engine._apply_import_exc(imp, "2026-08-15T02:00:00")
        self.assertTrue(applied)
        self.assertAlmostEqual(imp["rate_exc"], round(0.05493 / 1.05, 6), places=6)
        self.assertAlmostEqual(imp["cost_exc"], round(2.0 * 0.05493 / 1.05, 6), places=6)

    def test_no_exc_tariff_is_noop(self):
        engine._kraken_rate_schedules = {"import": _Sched(0.30, 0.30)}
        engine._kraken_rate_schedules["import"].exc = None
        imp = {"rate": 0.30, "kwh": 1.0}
        self.assertFalse(engine._apply_import_exc(imp, "2026-08-15T02:00:00"))
        self.assertNotIn("rate_exc", imp)


if __name__ == "__main__":
    unittest.main()
