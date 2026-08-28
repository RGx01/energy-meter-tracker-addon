"""BL-56 — RateSchedule resolution is O(log n) via bisect (periods sorted by
valid_from), so a large stitched/Agile-history schedule doesn't stall the engine
loop. Results must be BYTE-IDENTICAL to the previous linear scan; an overlapping
(non-monotonic) schedule falls back to the exact linear scan.
"""
import random
import unittest
from datetime import datetime, timedelta

from kraken_rates import RateSchedule


def _iso(dt):
    return dt.strftime("%Y-%m-%dT%H:%M:%S")


# ---- reference LINEAR implementations (the pre-BL-56 behaviour) --------------
def _ref_resolve(periods, ts):
    match = None
    for vf, vt, r in periods:
        if ts < vf:
            break
        if vt is None or ts < vt:
            match = r
    return match


def _ref_dayrates(periods, ts):
    day = str(ts)[:10]
    ds, de = day + "T00:00:00", day + "T23:59:59"
    out = []
    for vf, vt, r in periods:
        if vf > de:
            break
        if vt is None or vt > ds:
            out.append(r)
    return out


def _windowed(day_rate, night_rate, start, ndays):
    p, d = [], start
    for _ in range(ndays):
        p.append((_iso(d.replace(hour=4, minute=30)), _iso(d.replace(hour=22, minute=30)), day_rate))
        p.append((_iso(d.replace(hour=22, minute=30)),
                  _iso((d + timedelta(days=1)).replace(hour=4, minute=30)), night_rate))
        d += timedelta(days=1)
    return sorted(p, key=lambda x: x[0])


def _agile(start, ndays):
    p, d = [], start.replace(hour=0, minute=0, second=0)
    for _ in range(ndays * 48):
        nxt = d + timedelta(minutes=30)
        p.append((_iso(d), _iso(nxt), round(random.uniform(-5, 40), 4)))
        d = nxt
    return p


class TestBisectByteIdentity(unittest.TestCase):
    def _fuzz(self, periods, n=1500):
        s = RateSchedule(periods)
        random.seed(7)
        tss = [_iso(datetime(2024, 1, 1) + timedelta(seconds=random.randint(0, 4 * 365 * 86400)))
               for _ in range(n)]
        for p in periods[:40]:
            tss += [p[0], p[1] or "2027-06-01T00:00:00"]
        for ts in tss:
            self.assertEqual(s.resolve(ts), _ref_resolve(periods, ts), f"resolve {ts}")
            ref = _ref_dayrates(periods, ts)
            self.assertEqual(s.off_peak_rate_near(ts), (min(ref) if ref else None), f"offpeak {ts}")
            self.assertEqual(s.day_rate_bounds(ts),
                             ((min(ref), max(ref)) if ref else (None, None)), f"bounds {ts}")

    def test_flat(self):
        self._fuzz([("2025-01-01T00:00:00", None, 24.5)])

    def test_windowed(self):
        self._fuzz(_windowed(32.3092, 5.493, datetime(2026, 7, 6), 40))

    def test_agile_large(self):
        random.seed(3)
        self._fuzz(_agile(datetime(2025, 1, 1), 200))  # ~9.6k periods

    def test_gapped(self):
        self._fuzz([("2026-01-01T00:00:00", "2026-01-01T12:00:00", 10.0),
                    ("2026-01-03T00:00:00", "2026-01-03T12:00:00", 20.0)])

    def test_degenerate_overlap_falls_back(self):
        # BL-52 pre-fix shape: two open-ended periods, same valid_from → non-monotonic
        periods = [("2026-07-05T23:00:00", None, 32.3092),
                   ("2026-07-05T23:00:00", None, 5.493)]
        s = RateSchedule(periods)
        self.assertFalse(s._monotonic)          # linear fallback engaged
        self._fuzz(periods)


class TestMonotonicFlag(unittest.TestCase):
    def test_contiguous_is_monotonic(self):
        self.assertTrue(RateSchedule(_windowed(30, 5, datetime(2026, 1, 1), 5))._monotonic)

    def test_flat_open_ended_is_monotonic(self):
        self.assertTrue(RateSchedule([("2025-01-01T00:00:00", None, 24.5)])._monotonic)

    def test_overlap_is_not_monotonic(self):
        self.assertFalse(RateSchedule(
            [("2026-01-01T00:00:00", "2026-01-01T10:00:00", 1.0),
             ("2026-01-01T05:00:00", "2026-01-01T20:00:00", 2.0)])._monotonic)

    def test_open_ended_not_last_is_not_monotonic(self):
        self.assertFalse(RateSchedule(
            [("2026-01-01T00:00:00", None, 1.0),
             ("2026-02-01T00:00:00", None, 2.0)])._monotonic)

    def test_empty(self):
        self.assertTrue(RateSchedule([])._monotonic)
        self.assertIsNone(RateSchedule([]).resolve("2026-01-01T00:00:00"))


if __name__ == "__main__":
    unittest.main()
