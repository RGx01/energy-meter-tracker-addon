"""Regression tests for main.py's task orchestration.

The DCC poll task is cancelled and relaunched by engine_startup on every config
save (to tear down an in-flight backfill before reopening the store). If that
task is awaited inside main()'s asyncio.gather, the cancellation propagates as
CancelledError to the gather and shuts the whole add-on down. This was observed
when completing the setup wizard on a running api-mode instance (where the poll
task is a live long-running loop, not an already-idle no-op): the add-on stopped
mid-startup. These tests pin the fix — the poll task must be launched and
tracked, but NOT awaited in the critical gather.
"""
import ast
import os
import unittest


def _main_src():
    here = os.path.dirname(os.path.abspath(__file__))
    with open(os.path.join(here, "main.py")) as f:
        return f.read()


class TestMainPollTaskNotInGather(unittest.TestCase):

    def test_poll_task_not_awaited_in_gather(self):
        tree = ast.parse(_main_src())
        gathers = [n for n in ast.walk(tree)
                   if isinstance(n, ast.Call)
                   and isinstance(n.func, ast.Attribute)
                   and n.func.attr == "gather"]
        self.assertTrue(gathers, "expected an asyncio.gather(...) call in main.py")
        for g in gathers:
            names = [a.id for a in g.args if isinstance(a, ast.Name)]
            self.assertNotIn(
                "poll_task", names,
                "poll_task must NOT be awaited in main()'s gather — engine_startup "
                "cancels it on every config save, which would propagate "
                "CancelledError and shut the add-on down.")

    def test_poll_task_still_launched_and_tracked(self):
        # The fix must not drop the poll task: it's still launched and its handle
        # tracked so engine_startup can cancel/relaunch it.
        compact = _main_src().replace(" ", "")
        self.assertIn("ensure_future(kraken_poll_task", compact)
        self.assertIn("_kraken_poll_task_handle=poll_task", compact)


if __name__ == "__main__":
    unittest.main()