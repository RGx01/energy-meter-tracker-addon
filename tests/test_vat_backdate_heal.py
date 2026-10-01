"""One-off 4.5.17 heal: 1 Oct 2026's 0% VAT backdated to July (retires under BL-33).

Octopus applied the 0% by editing its open rate records in place (a record from
2026-07-05 reads inc == exc). 4.5.16 learned "0% from 2026-07-05", priced re-written
history from the schedule without VAT, and kept pricing 1 Oct's live blocks at 5%.
The heal drops the learned entry and re-derives whichever side of each block's
inc/exc pair disagrees with the VAT of its date, decided against the tariff's figures.
Fixtures are the tariff as Octopus publishes it on 1 Oct 2026.
"""
import asyncio
import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from block_store import BlockStore                     # noqa: E402
from kraken_rates import RateSchedule                  # noqa: E402
import engine                                          # noqa: E402

_OPEN = {"valid_from": "2026-07-05T23:00:00Z", "valid_to": None}
# Windowless day/night, as IOG-SMB publishes them (inc == exc after the edit). The
# heal's tests only need the figures, so a flat day record and a flat night record.
DAY = dict(_OPEN, value_exc_vat=30.7707, value_inc_vat=30.7707)
NIGHT = dict(_OPEN, value_exc_vat=5.2314, value_inc_vat=5.2314)
STANDING = [dict(_OPEN, value_exc_vat=48.0532, value_inc_vat=48.0532)]


def _import_sched():
    """Day 05:30-23:30 local, night otherwise, from 1 Aug to 3 Oct, restated per date."""
    from datetime import datetime as DT, timedelta as TD
    recs, d = [], DT(2026, 7, 31)
    while d < DT(2026, 10, 3):
        n = d + TD(days=1)
        # naive UTC, BST: 23:30 local = 22:30Z, 05:30 local = 04:30Z
        recs += [dict(NIGHT, valid_from=f"{d:%Y-%m-%d}T22:30:00Z", valid_to=f"{n:%Y-%m-%d}T04:30:00Z"),
                 dict(DAY, valid_from=f"{n:%Y-%m-%d}T04:30:00Z", valid_to=f"{n:%Y-%m-%d}T22:30:00Z")]
        d = n
    return RateSchedule.from_api_records(recs).with_vat([])


def _standing_sched():
    return RateSchedule.from_api_records(STANDING).with_vat([])


class TheHeal(unittest.TestCase):

    def setUp(self):
        self.tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        self.tmp.close()
        self.st = BlockStore(self.tmp.name)
        self.st.set_vat_calendar([("2023-06-02", 0.05), ("2026-07-05", 0.0)])   # 4.5.16 learned this
        rows = [
            # (start, kwh, inc, exc, standing, rate_source)
            ("2026-09-10T11:00:00", 1.0, 0.307707, 0.307707, 0.504559, "schedule"),  # inc written ex-VAT
            ("2026-09-10T11:30:00", 1.0, 0.323092, 0.323092, 0.504559, "schedule"),  # exc derived at 0%
            ("2026-09-10T12:00:00", 1.0, 0.323092, 0.307707, 0.504559, "schedule"),  # right all along
            ("2026-09-10T12:30:00", 1.0, 0.307707, 0.307707, 0.504559, "measured"),  # the bill: never
            ("2026-09-10T13:00:00", 1.0, 0.307707, 0.307707, 0.504559, "corrected"),  # the user: never
            ("2026-09-10T13:30:00", 1.0, 0.250000, 0.250000, 0.504559, "schedule"),  # no tariff figure
            ("2026-10-01T05:30:00", 2.0, 0.323092, 0.307707, 0.504559, "schedule"),  # 1 Oct at 5%
        ]
        with self.st._conn:
            self.st._conn.execute(
                "INSERT INTO config_periods (id, effective_from, block_minutes, timezone) "
                "VALUES (1, '2026-08-01T00:00:00', 30, 'Europe/London')")
            for bs, kwh, inc, exc, sc, src in rows:
                self.st._conn.execute(
                    "INSERT INTO blocks (block_start, block_end, meter_id, config_period_id, "
                    "imp_kwh, imp_rate, imp_cost, imp_rate_exc, imp_cost_exc, standing_charge, "
                    "rate_source, is_provisional, interpolated) VALUES (?,?,?,1,?,?,?,?,?,?,?,0,0)",
                    (bs, bs, "electricity_main", kwh, inc, round(kwh * inc, 6), exc,
                     round(kwh * exc, 6), sc, src))
        self.res = engine._vat_backdate_heal_core(
            self.st, _import_sched(), _standing_sched(), std_floor="2026-08-25T23:00:00")

    def tearDown(self):
        self.st._conn.close()
        os.unlink(self.tmp.name)

    def _row(self, bs):
        return self.st._conn.execute(
            "SELECT imp_rate, imp_cost, imp_rate_exc, imp_cost_exc, standing_charge "
            "FROM blocks WHERE block_start = ?", (bs,)).fetchone()

    def test_the_backdated_calendar_entry_is_dropped(self):
        self.assertEqual(self.st.get_vat_calendar(), [("2023-06-02", 0.05)])
        self.assertAlmostEqual(self.st.vat_rate_at("2026-09-10T11:00:00"), 0.05)

    def test_an_inc_written_ex_vat_is_grossed_up(self):
        r = self._row("2026-09-10T11:00:00")
        self.assertAlmostEqual(r["imp_rate"], 0.323092, places=6)
        self.assertAlmostEqual(r["imp_cost"], 0.323092, places=6)
        self.assertAlmostEqual(r["imp_rate_exc"], 0.307707, places=6)

    def test_an_exc_derived_at_0pc_is_rederived(self):
        r = self._row("2026-09-10T11:30:00")
        self.assertAlmostEqual(r["imp_rate"], 0.323092, places=6)
        self.assertAlmostEqual(r["imp_rate_exc"], 0.307707, places=6)
        self.assertAlmostEqual(r["imp_cost_exc"], 0.307707, places=6)

    def test_1_october_at_5pc_comes_down_to_0pc(self):
        r = self._row("2026-10-01T05:30:00")
        self.assertAlmostEqual(r["imp_rate"], 0.307707, places=6)
        self.assertAlmostEqual(r["imp_cost"], 0.615414, places=6)     # 2 kWh x 0.307707
        self.assertAlmostEqual(r["standing_charge"], 0.480532, places=6)

    def test_a_correct_row_the_bill_and_the_user_are_untouched(self):
        self.assertAlmostEqual(self._row("2026-09-10T12:00:00")["imp_rate"], 0.323092, places=6)
        self.assertAlmostEqual(self._row("2026-09-10T12:30:00")["imp_rate"], 0.307707, places=6)
        self.assertAlmostEqual(self._row("2026-09-10T13:00:00")["imp_rate"], 0.307707, places=6)

    def test_a_row_matching_no_tariff_figure_is_left_and_counted(self):
        self.assertAlmostEqual(self._row("2026-09-10T13:30:00")["imp_rate"], 0.25, places=6)
        self.assertEqual(self.res["unmatched"], 1)

    def test_september_standing_charge_is_kept(self):
        self.assertAlmostEqual(self._row("2026-09-10T11:00:00")["standing_charge"], 0.504559, places=6)

    def test_counts_and_idempotence(self):
        self.assertEqual((self.res["inc_rebuilt"], self.res["exc_rebuilt"],
                          self.res["standing_fixed"], self.res["calendar_entries_dropped"]),
                         (2, 1, 1, 1))
        again = engine._vat_backdate_heal_core(self.st, _import_sched(), _standing_sched(),
                                               std_floor="2026-08-25T23:00:00")
        self.assertEqual((again["inc_rebuilt"], again["exc_rebuilt"], again["standing_fixed"],
                          again["calendar_entries_dropped"]), (0, 0, 0, 0))


class TheGate(unittest.TestCase):
    """GUARD-style plumbing: marks itself done; waits for a schedule; never re-runs."""

    class _Store:
        def __init__(self):
            self.state = {}
        def get_kraken_state(self, k): return self.state.get(k)
        def set_kraken_state(self, k, v): self.state[k] = v

    def setUp(self):
        self._save = {k: getattr(engine, k) for k in
                      ("_store", "_kraken_rate_schedules", "_kraken_client",
                       "_vat_backdate_heal_core", "_schedule_chart_regen")}
        engine._store = self._Store()
        engine._schedule_chart_regen = lambda: None
        engine._vat_backdate_heal_core = lambda *a, **k: {
            "ok": True, "inc_rebuilt": 1, "exc_rebuilt": 0, "standing_fixed": 0}

    def tearDown(self):
        for k, v in self._save.items():
            setattr(engine, k, v)

    def test_waits_for_the_schedule(self):
        engine._kraken_client, engine._kraken_rate_schedules = object(), {}
        self.assertEqual(asyncio.run(engine.run_vat_backdate_heal())["reason"], "schedule not ready")
        self.assertFalse(engine._store.state)

    def test_marks_itself_done_once(self):
        engine._kraken_client, engine._kraken_rate_schedules = object(), {"import": _import_sched()}
        asyncio.run(engine.run_vat_backdate_heal())
        self.assertIn(engine._VAT_BACKDATE_HEAL_DONE_KEY, engine._store.state)
        self.assertEqual(asyncio.run(engine.run_vat_backdate_heal())["skipped"], "already done")


if __name__ == "__main__":
    unittest.main()
