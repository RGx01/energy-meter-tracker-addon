"""
4.5.3-A: build_rate_schedule must construct the RateSchedule + diagnostics OFF
the event loop. A long half-hourly history (Agile ~34k periods) built inline on
the engine loop stalled the HA WebSocket heartbeat during a rate refresh /
first-time connect (PONG timeout -> the reported setup "Could not connect"). This
guards that the offloaded build still produces a correct schedule, including the
large-schedule diag-cap path.
"""
import os, sys, unittest
from datetime import datetime, timedelta
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import kraken_rates
from kraken_rates import build_rate_schedule, RateSchedule, _build_schedule_and_diag


def _records(n):
    recs, t = [], datetime(2024, 1, 1)
    for i in range(n):
        vf = t + timedelta(minutes=30 * i)
        vt = vf + timedelta(minutes=30)
        inc = round(10 + (i % 48) * 0.5, 4)
        recs.append({"value_inc_vat": inc, "value_exc_vat": round(inc / 1.05, 4),
                     "valid_from": vf.isoformat() + "Z", "valid_to": vt.isoformat() + "Z",
                     "payment_method": "DIRECT_DEBIT"})
    return recs


class _FakeClient:
    def __init__(self, records): self._records = records
    async def get_unit_rates(self, product, tariff, *, rate_type="standard-unit-rates",
                             period_from=None, period_to=None):
        return self._records if rate_type == "standard-unit-rates" else []


class TestRateScheduleOffload(unittest.IsolatedAsyncioTestCase):
    async def test_large_agile_schedule_builds_via_executor(self):
        sched = await build_rate_schedule(_FakeClient(_records(3000)), "AGILE", "E-1R-AGILE")
        self.assertIsInstance(sched, RateSchedule)
        self.assertEqual(len(sched), 3000)

    async def test_small_schedule_builds_via_executor(self):
        sched = await build_rate_schedule(_FakeClient(_records(12)), "P", "T")
        self.assertEqual(len(sched), 12)

    def test_build_helper_sync_small_and_large(self):
        self.assertTrue(callable(_build_schedule_and_diag))
        self.assertEqual(len(_build_schedule_and_diag(_records(2500), "P", "T")), 2500)
        self.assertEqual(len(_build_schedule_and_diag(_records(5), "P", "T")), 5)

    def test_offload_wired_in_source(self):
        import inspect
        self.assertIn("run_in_executor", inspect.getsource(build_rate_schedule),
                      "build_rate_schedule must offload the build")


if __name__ == "__main__":
    unittest.main()
