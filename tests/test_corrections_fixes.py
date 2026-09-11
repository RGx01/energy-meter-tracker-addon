"""
test_corrections_fixes.py — 4.5.7 Historical Corrections panel fixes.

Covers three independent fixes to the corrections/review surface:
  1. _active_period_timezone(): resolve the display tz straight from config_periods,
     so the review list's UTC->local conversion never silently degrades to UTC (which
     shifted every flagged block by the BST offset and mis-targeted Load-into-tool).
  2. _review_band_reason(): flag text speaks peak/off-peak (band from cost, not the
     OFF_PEAK label), not raw pounds.
  3. _cluster_nearby_rates(): fold float/rounding artifacts (e.g. 0.054929 vs the
     canonical 0.054930) into the dominant value, while keeping genuinely-distinct
     Agile rates (which step in 1e-4 increments) separate.
"""
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import engine
import web.server as server


class TestNearbyRateClustering(unittest.TestCase):
    def test_float_artifact_folds_into_dominant(self):
        rows = [{"rate": 0.323092, "count": 204},
                {"rate": 0.054930, "count": 131},
                {"rate": 0.054929, "count": 1}]       # 1e-6 off canonical off-peak
        out = server._cluster_nearby_rates(rows)
        self.assertEqual(len(out), 2)
        peak, off = out[0], out[1]
        self.assertAlmostEqual(peak["rate"], 0.323092)
        self.assertEqual(peak["count"], 204)
        self.assertAlmostEqual(off["rate"], 0.054930)   # dominant value kept
        self.assertEqual(off["count"], 132)             # 131 + 1

    def test_agile_rates_one_step_apart_not_merged(self):
        # Agile unit rates step in 0.01p = 1e-4; two distinct rates exactly 1e-4 apart
        # must survive (strict < epsilon).
        rows = [{"rate": 0.24530, "count": 3},
                {"rate": 0.24540, "count": 2}]
        out = server._cluster_nearby_rates(rows)
        self.assertEqual(len(out), 2)

    def test_empty(self):
        self.assertEqual(server._cluster_nearby_rates([]), [])


class TestActivePeriodTimezone(unittest.TestCase):
    def setUp(self):
        self._save = server._get_read_store

    def tearDown(self):
        server._get_read_store = self._save

    def _install(self, tz_value):
        class _Row(dict):
            pass
        class _Cur:
            def fetchone(self):
                return None if tz_value is _MISSING else _Row(timezone=tz_value)
        class _Conn:
            def execute(self, *a, **k):
                return _Cur()
        class _Store:
            _conn = _Conn()
        server._get_read_store = lambda: _Store()

    def test_resolves_period_tz(self):
        self._install("Europe/London")
        self.assertEqual(server._active_period_timezone(), "Europe/London")

    def test_none_when_no_period(self):
        self._install(_MISSING)
        self.assertIsNone(server._active_period_timezone(retries=1))

    def test_none_when_blank(self):
        self._install("")           # blank tz column
        self.assertIsNone(server._active_period_timezone(retries=1))


class TestReviewBandReason(unittest.TestCase):
    def test_band_flip_named(self):
        r = engine._review_band_reason("off_peak", "peak", 0.0089, 0.0523)
        self.assertIn("off-peak", r)
        self.assertIn("peak", r)
        self.assertIn("→", r)          # arrow
        self.assertIn("0.0523", r)
        self.assertNotIn("prior", r.lower())  # no raw "vs prior" phrasing

    def test_same_band_material_gap(self):
        r = engine._review_band_reason("peak", "peak", 0.30, 0.42)
        self.assertIn("same band", r)
        self.assertIn("peak", r)

    def test_unknown_prior_band_degrades_gracefully(self):
        r = engine._review_band_reason(None, "off_peak", 0.05, 0.02)
        self.assertIn("off-peak", r)        # still readable, no crash


_MISSING = object()

if __name__ == "__main__":
    unittest.main()


class TestMultibandGuard(unittest.TestCase):
    """_corrections_multiband_count flags a genuine cap-transition block (import segments
    spanning a real off-peak<->peak rate gap) so the single-rate corrections tool can refuse it,
    while ignoring same-rate EV/house label pairs and float/rounding noise."""

    def _store(self):
        from block_store import BlockStore
        st = BlockStore(":memory:")
        st._conn.execute(
            "INSERT OR IGNORE INTO config_periods (id, effective_from, billing_day, "
            "block_minutes, timezone) VALUES (1,'2020-01-01T00:00:00',1,30,'UTC')")
        return st

    def _seg(self, st, bs, seq, rate, band, attr):
        st._conn.execute(
            "INSERT INTO blocks (block_start, block_end, meter_id, config_period_id, imp_kwh, "
            "imp_rate) VALUES (?,?,?,1,1,?) ON CONFLICT DO NOTHING", (bs, bs, "electricity_main", rate))
        st._conn.execute(
            "INSERT INTO block_segments (block_start, meter_id, channel, seq, kwh, inc_rate, "
            "exc_rate, band, attribution) VALUES (?,?,?,?,?,?,?,?,?)",
            (bs, "electricity_main", "import", seq, 1, rate, rate*0.95, band, attr))
        st._conn.commit()

    def test_genuine_transition_flagged(self):
        st = self._store()
        self._seg(st, "2020-01-01T12:00:00", 1, 0.05493,  "off_peak", "ev")
        self._seg(st, "2020-01-01T12:00:00", 2, 0.323092, "peak",     "house")
        self.assertEqual(server._corrections_multiband_count(
            st, "meter_id='electricity_main'", []), 1)

    def test_same_rate_label_pair_not_flagged(self):
        st = self._store()
        self._seg(st, "2020-01-01T12:00:00", 1, 0.323092, "peak", "ev")
        self._seg(st, "2020-01-01T12:00:00", 2, 0.323092, "day",  "house")
        self.assertEqual(server._corrections_multiband_count(
            st, "meter_id='electricity_main'", []), 0)

    def test_float_noise_not_flagged(self):
        st = self._store()
        self._seg(st, "2020-01-01T12:00:00", 1, 0.05493,  "off_peak", "ev")
        self._seg(st, "2020-01-01T12:00:00", 2, 0.054929, "off_peak", "house")
        self.assertEqual(server._corrections_multiband_count(
            st, "meter_id='electricity_main'", []), 0)
