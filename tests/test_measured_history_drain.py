"""4.5.11: first-time / bulk SMB history import — run_measured_history_drain fetches Octopus's
billed four-bucket breakdown for imported capped-tariff slots (oldest-first) and settles them,
so filled history reads the real cost/EV-House split/band instead of schedule-provisional. Reuses
recover_device_breakdown + apply_measured_settled; agreement-floored; skips not-yet-billed slots."""
import os, sys, asyncio, unittest
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import engine
from block_store import BlockStore

OFF = 0.05493


class _Sched:
    def is_empty(self): return False
    def day_rate_bounds(self, ts): return (5.493, 32.3092)   # pence (off, peak)


class _FakeClient:
    """Returns Octopus's device breakdown for requested slots (all off-peak here)."""
    def __init__(self, kwh_by_slot):
        self.kwh = kwh_by_slot
        self.calls = []

    async def recover_device_breakdown(self, mpan, starts, **kw):
        self.calls.append(list(starts))
        out = {}
        for s in starts:
            k = self.kwh.get(s)
            if k is None:
                continue                                  # not billed → absent
            home, ev = k
            out[s] = {"home_kwh": home, "ev_kwh": ev,
                      "home_cost": round(home * OFF, 6), "ev_cost": round(ev * OFF, 6),
                      "home_rate": OFF, "ev_rate": OFF,
                      "home_rate_exc": round(OFF / 1.05, 6), "ev_rate_exc": round(OFF / 1.05, 6),
                      "home_band": "off_peak", "ev_band": "off_peak"}
        return out


class TestMeasuredHistoryDrain(unittest.TestCase):
    def setUp(self):
        self._save = (engine._store, engine._kraken_client, engine._kraken_discovery,
                      engine._kraken_rate_schedules, engine._kraken_current_agreement_from,
                      engine._MEASURED_APPLY, engine._MEASURED_FETCH_ENABLED,
                      engine._MEASURED_HISTORY_DRAIN_PACE)
        self.st = BlockStore(":memory:"); engine._store = self.st
        engine._kraken_discovery = {"import": {"mpan": "m", "tariff_code": "E-1R-IOG-SMB-FIX"}}
        engine._kraken_rate_schedules = {"import": _Sched()}
        engine._kraken_current_agreement_from = "2026-07-05T00:00:00"
        engine._MEASURED_APPLY = True; engine._MEASURED_FETCH_ENABLED = True
        engine._MEASURED_HISTORY_DRAIN_PACE = 0
        self.st._conn.execute(
            "INSERT OR IGNORE INTO config_periods (id, effective_from, billing_day, "
            "block_minutes, timezone) VALUES (1,'2020-01-01T00:00:00',1,30,'UTC')")

    def tearDown(self):
        (engine._store, engine._kraken_client, engine._kraken_discovery,
         engine._kraken_rate_schedules, engine._kraken_current_agreement_from,
         engine._MEASURED_APPLY, engine._MEASURED_FETCH_ENABLED,
         engine._MEASURED_HISTORY_DRAIN_PACE) = self._save

    def _imported_block(self, slot, kwh, rate=0.323092):
        # an imported SMB slot: settled kWh (imp_kwh_api), schedule-priced provisional (peak here)
        self.st._conn.execute(
            "INSERT INTO blocks (block_start, block_end, meter_id, config_period_id, "
            "imp_kwh, imp_kwh_api, imp_rate, imp_cost, rate_source) "
            "VALUES (?,?,?,1,?,?,?,?,'schedule')",
            (slot, slot, "electricity_main", kwh, kwh, rate, round(kwh * rate, 6)))
        self.st._conn.commit()

    def _row(self, slot):
        return self.st._conn.execute(
            "SELECT imp_rate, imp_cost, rate_source FROM blocks WHERE block_start=? "
            "AND meter_id='electricity_main'", (slot,)).fetchone()

    def test_drain_settles_imported_history_from_the_bill(self):
        s1, s2 = "2026-08-01T02:00:00", "2026-08-01T02:30:00"
        self._imported_block(s1, 3.0)                     # provisional peak (schedule)
        self._imported_block(s2, 2.0)
        engine._kraken_client = _FakeClient({s1: (1.0, 2.0), s2: (1.5, 0.5)})

        res = asyncio.run(engine.run_measured_history_drain())
        self.assertTrue(res["ok"]); self.assertEqual(res["settled"], 2)

        for s, kwh in ((s1, 3.0), (s2, 2.0)):
            r = self._row(s)
            self.assertEqual(r["rate_source"], "measured")                # settled from the bill
            self.assertAlmostEqual(r["imp_rate"], OFF, places=5)          # snapped to off-peak
            self.assertAlmostEqual(r["imp_cost"], round(kwh * OFF, 6), places=6)
            mc = self.st.get_measured_breakdown(s)
            self.assertIsNotNone(mc)                                      # breakdown cached

    def test_drain_settles_when_imp_kwh_api_is_null(self):
        # prod-dev regression (v4.5.11): on a site with a live CAD/local import meter, the API
        # import/gap-fill path writes consumption into imp_kwh and leaves imp_kwh_api NULL, and
        # freshly-imported blocks land with rate_source NULL. The drain must still settle them
        # from the bill (grid measured draw is king) — it must NOT gate on imp_kwh_api.
        s = "2026-08-01T02:00:00"
        self.st._conn.execute(
            "INSERT INTO blocks (block_start, block_end, meter_id, config_period_id, "
            "imp_kwh, imp_kwh_api, imp_rate, imp_cost, rate_source, source) "
            "VALUES (?,?,?,1,?,NULL,?,?,NULL,'imported_api')",
            (s, s, "electricity_main", 3.0, 0.323092, round(3.0 * 0.323092, 6)))
        self.st._conn.commit()
        engine._kraken_client = _FakeClient({s: (1.0, 2.0)})

        res = asyncio.run(engine.run_measured_history_drain())
        self.assertEqual(res["settled"], 1)                              # settled despite NULL imp_kwh_api
        r = self._row(s)
        self.assertEqual(r["rate_source"], "measured")
        self.assertAlmostEqual(r["imp_rate"], OFF, places=5)
        self.assertIsNotNone(self.st.get_measured_breakdown(s))

    def _live_block(self, slot, kwh, *, api_settled, rate=0.323092):
        # a LIVE (CAD) block: source NULL. api_settled=True -> imp_kwh_api set (DCC-settled);
        # False -> imp_kwh_api NULL (meter not yet settled).
        api = kwh if api_settled else None
        self.st._conn.execute(
            "INSERT INTO blocks (block_start, block_end, meter_id, config_period_id, "
            "imp_kwh, imp_kwh_api, imp_rate, imp_cost, rate_source, source) "
            "VALUES (?,?,?,1,?,?,?,?,NULL,NULL)",
            (slot, slot, "electricity_main", kwh, api, rate, round(kwh * rate, 6)))
        self.st._conn.commit()

    def test_drain_skips_unsettled_live_block(self):
        # prod regression (07:00 mis-band): a LIVE block that has NOT DCC-settled
        # (imp_kwh_api NULL, source NULL) must NOT be drained. Settling it off a provisional
        # CAD kWh is what mis-banded the 07:00 slot; it stays schedule until the meter settles.
        s = "2026-08-01T02:00:00"
        self._live_block(s, 3.0, api_settled=False)
        engine._kraken_client = _FakeClient({s: (1.0, 2.0)})
        res = asyncio.run(engine.run_measured_history_drain())
        self.assertEqual(res["settled"], 0)
        self.assertIsNone(self._row(s)["rate_source"])     # untouched — not settled

    def test_drain_settles_dcc_settled_live_block(self):
        # a LIVE block that HAS DCC-settled (imp_kwh_api present) is a valid drain target.
        s = "2026-08-01T02:30:00"
        self._live_block(s, 3.0, api_settled=True)
        engine._kraken_client = _FakeClient({s: (1.0, 2.0)})
        res = asyncio.run(engine.run_measured_history_drain())
        self.assertEqual(res["settled"], 1)
        self.assertEqual(self._row(s)["rate_source"], "measured")

    def test_apply_measured_settled_skips_unsettled_live_with_cache(self):
        # even when a breakdown is ALREADY cached, apply_measured_settled must not stamp an
        # unsettled LIVE block 'measured' (the exact 07:00 split-brain: measured + in the 86).
        s = "2026-08-01T03:00:00"
        self._live_block(s, 0.004, api_settled=False)
        self.st.upsert_measured_cost(s, mpan="m", cost_incl=0.000323, cost_excl=0.000308,
                                     label="STANDARD_RATE", kwh=0.001)
        engine.apply_measured_settled()
        self.assertIsNone(self._row(s)["rate_source"])     # NOT stamped measured
        # but a DCC-settled live block with the same cache DOES apply
        s2 = "2026-08-01T03:30:00"
        self._live_block(s2, 3.0, api_settled=True)
        self.st.upsert_measured_cost(s2, mpan="m", cost_incl=0.1, cost_excl=0.095,
                                     label="OFF_PEAK", kwh=3.0)
        engine.apply_measured_settled()
        self.assertEqual(self._row(s2)["rate_source"], "measured")

    # ── 4.5.12 hardening: the three guards that stop a "settled-but-unsettled" block recurring ──

    def test_apply_measured_to_block_refuses_unsettled_live_directly(self):
        # H1 defence in depth: apply_measured_to_block is the ONLY stamper of rate_source=
        # 'measured'. It must refuse an unsettled LIVE block on its own, so a future/direct
        # caller (bypassing apply_measured_settled's gated query) cannot reintroduce the bug.
        s = "2026-08-01T04:00:00"
        self._live_block(s, 3.0, api_settled=False)
        ok = engine.apply_measured_to_block(s, cost_incl=round(3.0 * OFF, 6), label="OFF_PEAK")
        self.assertFalse(ok)
        self.assertIsNone(self._row(s)["rate_source"])     # untouched
        # …but a DCC-settled live block and an imported block are both accepted directly
        s2 = "2026-08-01T04:30:00"
        self._live_block(s2, 3.0, api_settled=True)
        self.assertTrue(engine.apply_measured_to_block(s2, cost_incl=round(3.0 * OFF, 6), label="OFF_PEAK"))
        self.assertEqual(self._row(s2)["rate_source"], "measured")
        s3 = "2026-08-01T05:00:00"
        self._imported_block(s3, 3.0)
        self.st._conn.execute("UPDATE blocks SET imp_kwh_api=NULL, source='imported_api' WHERE block_start=?", (s3,))
        self.st._conn.commit()
        self.assertTrue(engine.apply_measured_to_block(s3, cost_incl=round(3.0 * OFF, 6), label="OFF_PEAK"))
        self.assertEqual(self._row(s3)["rate_source"], "measured")

    def test_backlog_excludes_unsettled_live_blocks(self):
        # H2: the drain's backlog signal shares the single _SETTLEABLE_SQL predicate, so it
        # cannot schedule the drain for blocks the drain itself must never settle.
        self._live_block("2026-08-01T06:00:00", 3.0, api_settled=False)   # unsettled live
        self.assertEqual(engine._measured_history_backlog(), 0)
        self._live_block("2026-08-01T06:30:00", 3.0, api_settled=True)    # DCC-settled live
        self.assertEqual(engine._measured_history_backlog(), 1)

    def test_settled_slot_bands_on_settled_kwh_not_stale_cad_kwh(self):
        # H3 (the 07:00 shape, on a SETTLED block): Octopus settled 0.001 kWh @ peak
        # (cost 0.000323, label STANDARD_RATE) but the block still holds 0.004 CAD kWh
        # (pass-2 not yet reconciled). cost / CAD 0.004 = 0.0808 -> nearest is OFF-PEAK (wrong).
        # The band divisor must be the SETTLED kWh: cost / 0.001 = 0.323 -> PEAK. Cost still
        # beats the label (see test_cost_beats_label) — only the denominator is fixed.
        s = "2026-08-01T07:00:00"
        self.st._conn.execute(
            "INSERT INTO blocks (block_start, block_end, meter_id, config_period_id, "
            "imp_kwh, imp_kwh_api, imp_rate, imp_cost, rate_source, source) "
            "VALUES (?,?,?,1,?,?,?,?,NULL,NULL)",
            (s, s, "electricity_main", 0.004, 0.001, 0.323092, 0.000323))
        self.st._conn.commit()
        self.st.upsert_measured_cost(s, mpan="m", cost_incl=0.000323, cost_excl=0.000308,
                                     label="STANDARD_RATE", kwh=0.001)     # settled kWh = 0.001
        self.assertTrue(engine.apply_measured_to_block(s, cost_incl=0.000323, label="STANDARD_RATE"))
        r = self._row(s)
        self.assertEqual(r["rate_source"], "measured")
        self.assertAlmostEqual(r["imp_rate"], 0.323092, places=5)   # PEAK, not 0.05493
        # unchanged where CAD and settled agree: an off-peak cost bands off-peak as before
        s2 = "2026-08-01T07:30:00"
        self._live_block(s2, 3.0, api_settled=True)
        self.st.upsert_measured_cost(s2, mpan="m", cost_incl=round(3.0 * OFF, 6),
                                     cost_excl=0.15, label="OFF_PEAK", kwh=3.0)
        engine.apply_measured_to_block(s2, cost_incl=round(3.0 * OFF, 6), label="OFF_PEAK")
        self.assertAlmostEqual(self._row(s2)["imp_rate"], OFF, places=5)

    def test_recent_unbilled_slot_is_left_for_the_live_pass(self):
        from datetime import datetime, timezone
        recent = datetime.now(timezone.utc).replace(tzinfo=None).strftime("%Y-%m-%dT%H:00:00")
        self._imported_block(recent, 3.0)
        engine._kraken_client = _FakeClient({recent: (1.0, 2.0)})
        res = asyncio.run(engine.run_measured_history_drain())
        self.assertEqual(res["settled"], 0)                              # excluded by the recency cutoff
        self.assertEqual(self._row(recent)["rate_source"], "schedule")   # untouched

    def test_noop_when_not_capped(self):
        engine._kraken_discovery = {"import": {"mpan": "m", "tariff_code": "E-1R-FLAT"}}
        engine._kraken_rate_schedules = {"import": _Sched()}   # no ev_device schedule
        self._imported_block("2026-08-01T02:00:00", 3.0)
        engine._kraken_client = _FakeClient({"2026-08-01T02:00:00": (1.0, 2.0)})
        res = asyncio.run(engine.run_measured_history_drain())
        self.assertEqual(res.get("skipped"), "not applicable")


if __name__ == "__main__":
    unittest.main()
