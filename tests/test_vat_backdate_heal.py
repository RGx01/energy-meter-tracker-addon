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
            ("2026-09-30T00:00:00", 2.0, 0.052314, 0.049823, 0.504559, "schedule"),  # both a VAT short
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

    def test_a_pair_one_vat_short_is_scaled_up(self):
        """4.5.17 wrote 30 Sep's in-window EV slots with inc = the ex-VAT figure and exc
        derived from it at 5%: the ratio is right, so v2 passed them over."""
        r = self._row("2026-09-30T00:00:00")
        self.assertAlmostEqual(r["imp_rate"], 0.05493, places=5)
        self.assertAlmostEqual(r["imp_rate_exc"], 0.052314, places=5)
        self.assertAlmostEqual(r["imp_cost"], 0.10986, places=5)

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
                          self.res["both_short_vat"], self.res["standing_fixed"],
                          self.res["calendar_entries_dropped"]),
                         (2, 1, 1, 1, 1))
        again = engine._vat_backdate_heal_core(self.st, _import_sched(), _standing_sched(),
                                               std_floor="2026-08-25T23:00:00")
        self.assertEqual((again["inc_rebuilt"], again["exc_rebuilt"], again["both_short_vat"],
                          again["standing_fixed"], again["calendar_entries_dropped"]),
                         (0, 0, 0, 0, 0))


class SubMeterRowsFollowTheirMain(unittest.TestCase):
    """4.5.19 (heal v4). A device row is priced at its block's main rate when written; the
    heal re-priced main rows only (device rows carry no exc), so 1 Oct's EV-charger and
    battery rows written before 4.5.18 stayed at 5% beside a 0% main."""

    def setUp(self):
        self.tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        self.tmp.close()
        self.st = BlockStore(self.tmp.name)
        rows = [  # (start, meter, kwh, kwh_grid, rate, exc)
            ("2026-10-01T00:00:00", "electricity_main", 6.0, None, 0.052314, 0.052314),
            ("2026-10-01T00:00:00", "ev_charger", 3.44, 3.44, 0.05493, None),      # 5% beside 0%
            ("2026-10-01T00:00:00", "house_battery", 2.0, 1.5, 0.05493, None),     # grid-clipped
            ("2026-09-20T12:00:00", "electricity_main", 1.0, None, 0.05493, 0.052314),
            ("2026-09-20T12:00:00", "ev_charger", 1.0, 1.0, 0.323092, None),       # EV at peak: real
        ]
        with self.st._conn:
            self.st._conn.execute(
                "INSERT INTO config_periods (id, effective_from, block_minutes, timezone) "
                "VALUES (1, '2026-08-01T00:00:00', 30, 'Europe/London')")
            for bs, mid, kwh, gk, rate, exc in rows:
                self.st._conn.execute(
                    "INSERT INTO blocks (block_start, block_end, meter_id, config_period_id, imp_kwh, "
                    "imp_kwh_grid, imp_rate, imp_cost, imp_rate_exc, imp_cost_exc, rate_source, "
                    "is_provisional, interpolated) VALUES (?,?,?,1,?,?,?,?,?,?,'schedule',0,0)",
                    (bs, bs, mid, kwh, gk, rate, round((gk or kwh) * rate, 6), exc,
                     None if exc is None else round(kwh * exc, 6)))
        self.res = engine._vat_backdate_heal_core(self.st, _import_sched(), _standing_sched(),
                                                  std_floor="2026-08-25T23:00:00")

    def tearDown(self):
        self.st._conn.close()
        os.unlink(self.tmp.name)

    def _dev(self, bs, mid):
        return self.st._conn.execute("SELECT imp_rate, imp_cost FROM blocks WHERE block_start=? "
                                     "AND meter_id=?", (bs, mid)).fetchone()

    def test_device_rows_take_the_main_rate(self):
        r = self._dev("2026-10-01T00:00:00", "ev_charger")
        self.assertAlmostEqual(r["imp_rate"], 0.052314, places=6)
        self.assertAlmostEqual(r["imp_cost"], round(3.44 * 0.052314, 6), places=6)

    def test_device_cost_is_from_grid_kwh(self):
        r = self._dev("2026-10-01T00:00:00", "house_battery")
        self.assertAlmostEqual(r["imp_cost"], round(1.5 * 0.052314, 6), places=6)

    def test_a_device_at_a_different_band_is_left(self):
        """GUARD: an EV at peak beside an off-peak main is a real split, not a VAT factor."""
        self.assertAlmostEqual(self._dev("2026-09-20T12:00:00", "ev_charger")["imp_rate"], 0.323092)
        self.assertEqual(self.res["device_rows_aligned"], 2)


class TheGate(unittest.TestCase):
    """GUARD-style plumbing: marks itself done; waits for a schedule; never re-runs."""

    class _Store:
        def __init__(self):
            self.state = {}
        def get_kraken_state(self, k): return self.state.get(k)
        def set_kraken_state(self, k, v): self.state[k] = v
        def get_vat_calendar(self): return []
        def set_vat_calendar(self, e): pass

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


class TheReArmedHealCleansBeforeItMatches(unittest.TestCase):
    """The 4.5.17 miss, end to end through run_vat_backdate_heal: the calendar says 0%
    since 26 Aug and the cached schedule was restated on it, so 30.7707p looked like
    the tariff's inc figure for September. A block 4.5.17 left at inc == exc == the
    inc-VAT figure (exc derived at 0%) matched nothing and was skipped."""

    def setUp(self):
        self._save = {k: getattr(engine, k) for k in
                      ("_store", "_kraken_rate_schedules", "_kraken_client",
                       "_kraken_standing_schedule", "_kraken_current_agreement_from",
                       "_schedule_chart_regen")}
        self.tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        self.tmp.close()
        self.st = BlockStore(self.tmp.name)
        self.st.set_vat_calendar([("2023-06-02", 0.05), ("2026-08-26", 0.0)])   # 4.5.17 state
        with self.st._conn:
            self.st._conn.execute(
                "INSERT INTO config_periods (id, effective_from, block_minutes, timezone) "
                "VALUES (1, '2026-08-01T00:00:00', 30, 'Europe/London')")
            self.st._conn.execute(
                "INSERT INTO blocks (block_start, block_end, meter_id, config_period_id, imp_kwh, "
                "imp_rate, imp_cost, imp_rate_exc, imp_cost_exc, rate_source, is_provisional, "
                "interpolated) VALUES ('2026-09-10T11:30:00','2026-09-10T12:00:00',"
                "'electricity_main',1,1.0,0.323092,0.323092,0.323092,0.323092,'schedule',0,0)")
        poisoned = self.st.get_vat_calendar()
        engine._store = self.st
        engine._kraken_client = object()
        engine._kraken_rate_schedules = {"import": _import_sched().with_vat(poisoned)}
        engine._kraken_standing_schedule = _standing_sched().with_vat(poisoned)
        engine._kraken_current_agreement_from = "2026-08-25T23:00:00"
        engine._schedule_chart_regen = lambda: None

    def tearDown(self):
        for k, v in self._save.items():
            setattr(engine, k, v)
        self.st._conn.close()
        os.unlink(self.tmp.name)

    def test_the_block_is_healed_and_the_schedules_restated(self):
        res = asyncio.run(engine.run_vat_backdate_heal())
        self.assertEqual(res["calendar_entries_dropped"], 1)
        self.assertEqual(res["exc_rebuilt"], 1)
        r = self.st._conn.execute("SELECT imp_rate, imp_rate_exc FROM blocks").fetchone()
        self.assertAlmostEqual(r["imp_rate"], 0.323092, places=6)
        self.assertAlmostEqual(r["imp_rate_exc"], 0.307707, places=6)
        self.assertAlmostEqual(
            engine._kraken_rate_schedules["import"].resolve("2026-09-10T11:30:00"), 32.3092, places=4)
        self.assertAlmostEqual(
            engine._kraken_standing_schedule.resolve("2026-09-10T12:00:00"), 50.4559, places=4)

    def test_it_runs_again_where_an_earlier_heal_marked_itself_done(self):
        self.st.set_kraken_state("vat_backdate_heal_done", "2026-10-01T09:30:00+00:00")
        self.st.set_kraken_state("vat_backdate_heal_done_v2", "2026-10-01T10:20:00+00:00")
        self.assertNotIn("skipped", asyncio.run(engine.run_vat_backdate_heal()))


if __name__ == "__main__":
    unittest.main()
