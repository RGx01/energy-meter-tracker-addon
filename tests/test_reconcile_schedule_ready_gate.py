"""
test_reconcile_schedule_ready_gate.py — BL-59.

On boot the first _tick_dispatch_capture can fire before the import rate schedule
is built. The reconcile block must NOT consume its hourly slot (advance
_last_reconcile) when the schedule is unready — otherwise the real reconcile is
deferred a full hour and, on a frequently-restarted box, never runs.
"""
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import engine


class _ReadySched:
    def is_empty(self):
        return False


class _EmptySched:
    def is_empty(self):
        return True


class TestReconcileReadyGate(unittest.IsolatedAsyncioTestCase):

    def setUp(self):
        self._save = (engine._kraken_client, engine._kraken_discovery,
                      engine._last_dispatch_capture, engine._last_reconcile,
                      engine._kraken_rate_schedules,
                      engine._capture_dispatch_slots,
                      engine.reconcile_dispatch_overlay,
                      engine.measure_settled_dispatched_blocks)
        engine._kraken_client = object()
        engine._kraken_discovery = {"import": {"mpan": "x"}}
        engine._last_dispatch_capture = None
        engine._last_reconcile = None
        self.calls = {"reconcile": 0, "measure": 0}

        async def _cap():
            return None

        async def _rec():
            self.calls["reconcile"] += 1
            return {"reverted": 0}

        async def _meas():
            self.calls["measure"] += 1
            return {}

        engine._capture_dispatch_slots = _cap
        engine.reconcile_dispatch_overlay = _rec
        engine.measure_settled_dispatched_blocks = _meas

    def tearDown(self):
        (engine._kraken_client, engine._kraken_discovery,
         engine._last_dispatch_capture, engine._last_reconcile,
         engine._kraken_rate_schedules, engine._capture_dispatch_slots,
         engine.reconcile_dispatch_overlay,
         engine.measure_settled_dispatched_blocks) = self._save

    async def test_unready_schedule_does_not_consume_slot(self):
        engine._kraken_rate_schedules = {"import": _EmptySched()}
        engine._last_reconcile = None
        await engine._tick_dispatch_capture()
        self.assertEqual(self.calls["reconcile"], 0)     # did not run
        self.assertIsNone(engine._last_reconcile)         # slot NOT burned

    async def test_missing_schedule_does_not_consume_slot(self):
        engine._kraken_rate_schedules = {}
        engine._last_reconcile = None
        await engine._tick_dispatch_capture()
        self.assertEqual(self.calls["reconcile"], 0)
        self.assertIsNone(engine._last_reconcile)

    async def test_ready_schedule_runs_and_advances(self):
        engine._kraken_rate_schedules = {"import": _ReadySched()}
        engine._last_dispatch_capture = None
        engine._last_reconcile = None
        await engine._tick_dispatch_capture()
        self.assertEqual(self.calls["reconcile"], 1)
        self.assertEqual(self.calls["measure"], 1)
        self.assertIsNotNone(engine._last_reconcile)


if __name__ == "__main__":
    unittest.main()
