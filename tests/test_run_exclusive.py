"""
BL-48: run_exclusive must run the callable while holding the engine tick lock, so a
device-delete/recompute can't overlap an in-flight capture/finalise tick on the
shared SQLite connection. Also falls back to a direct call when no loop lock exists.
"""
import asyncio, os, sys, unittest
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import engine


class TestRunExclusive(unittest.TestCase):
    def test_runs_and_returns_under_lock(self):
        engine._engine_loop_lock = asyncio.Lock()
        seen = {}
        def op(a, b=0):
            seen["held"] = engine._engine_loop_lock.locked()   # lock held while we run
            return a + b
        out = asyncio.get_event_loop().run_until_complete(
            engine.run_exclusive(op, 40, b=2))
        self.assertEqual(out, 42)
        self.assertTrue(seen["held"])
        self.assertFalse(engine._engine_loop_lock.locked())    # released afterwards

    def test_serializes_against_a_holder(self):
        # While something else holds the lock, run_exclusive must wait for it.
        engine._engine_loop_lock = asyncio.Lock()
        order = []
        async def scenario():
            await engine._engine_loop_lock.acquire()
            async def run_op():
                await engine.run_exclusive(lambda: order.append("op"))
            task = asyncio.ensure_future(run_op())
            await asyncio.sleep(0.05)
            order.append("before-release")     # op must NOT have run yet
            engine._engine_loop_lock.release()
            await task
        asyncio.get_event_loop().run_until_complete(scenario())
        self.assertEqual(order, ["before-release", "op"])

    def test_fallback_without_lock(self):
        engine._engine_loop_lock = None
        out = asyncio.get_event_loop().run_until_complete(
            engine.run_exclusive(lambda x: x * 2, 21))
        self.assertEqual(out, 42)


if __name__ == "__main__":
    unittest.main()
