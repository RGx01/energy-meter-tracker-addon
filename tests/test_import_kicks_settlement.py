"""
BL-67: a finished bulk import must KICK the settlement machinery, not wait for a clock.

`_maybe_drain_measured_history()` is what fetches Octopus's four-bucket device breakdown
for freshly imported capped history — and on a first import that bill is the ONLY source
of the EV/House split, because Octopus serves a short rolling dispatch window and keeps no
history, so there is no completed dispatch to derive one from either.

It used to be reachable ONLY from the hourly branch of `_tick_dispatch_capture`. On a fresh
install that branch runs once during startup — against an empty store, taking the hourly
slot with nothing to do — so an import that wrote ~35k blocks then sat silent for the best
part of an hour with the split visibly missing. Observed live: import finished 21:02:38,
drain finally scheduled 21:17:07, 438 slots settled from the bill 81 seconds later.

These are structural (AST) assertions rather than behavioural ones: driving a full verify
pass needs the network, a populated store and a running loop, and the regression being
guarded is precisely "the call site went away / went back to being clock-only". A test that
pins WHERE the call lives catches that; a mocked behavioural test would not.
"""

import ast
import os
import sys
import unittest

_HERE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _HERE)

KICK = "_maybe_drain_measured_history"


def _engine_tree():
    with open(os.path.join(_HERE, "engine.py")) as fh:
        return ast.parse(fh.read())


def _funcs_calling(tree, name):
    """{enclosing function name: [call nodes]} for every real call to `name`."""
    out = {}
    for node in ast.walk(tree):
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        calls = [n for n in ast.walk(node)
                 if isinstance(n, ast.Call)
                 and isinstance(n.func, ast.Name) and n.func.id == name]
        if calls:
            # attribute the call to its INNERMOST enclosing function
            inner = [c for c in ast.walk(node)
                     if isinstance(c, (ast.FunctionDef, ast.AsyncFunctionDef)) and c is not node]
            claimed = {id(n) for f in inner for n in ast.walk(f) if isinstance(n, ast.Call)}
            mine = [c for c in calls if id(c) not in claimed]
            if mine:
                out[node.name] = mine
    return out


class TestImportKicksSettlement(unittest.TestCase):

    def setUp(self):
        self.tree = _engine_tree()
        self.callers = _funcs_calling(self.tree, KICK)

    def test_the_helper_exists(self):
        names = {n.name for n in ast.walk(self.tree)
                 if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))}
        self.assertIn(KICK, names)

    def test_the_import_finish_kicks_the_drain(self):
        """The regression. Without a caller on the import-completion path, a fresh
        install waits up to an hour for the hourly tick before fetching the bill."""
        self.assertTrue(
            any(c != "_tick_dispatch_capture" for c in self.callers),
            "%s() is only reachable from %s — a finished import cannot start the "
            "four-bucket fetch, so the EV/House split stays missing until the clock "
            "comes round" % (KICK, sorted(self.callers)))

    def test_the_hourly_tick_still_kicks_it_too(self):
        """Belt and braces: the import hook is additive, not a replacement. A drain
        that fails or is rate-limited still gets retried on the hourly cadence."""
        self.assertIn("_tick_dispatch_capture", self.callers)

    def test_the_kick_is_guarded(self):
        """It must never take the import path down with it — the drain is a nicety,
        the import is the user's data."""
        for fname, calls in self.callers.items():
            if fname == "_tick_dispatch_capture":
                continue
            fn = next(n for n in ast.walk(self.tree)
                      if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))
                      and n.name == fname)
            guarded = any(
                any(c in ast.walk(handler_parent) for c in calls)
                for handler_parent in ast.walk(fn)
                if isinstance(handler_parent, ast.Try))
            self.assertTrue(guarded,
                            "the %s() call in %s() is not inside a try/except" % (KICK, fname))


class TestDrainGuardsAreIntact(unittest.TestCase):
    """The kick is only safe because the helper no-ops when there is nothing to do.
    If these guards were dropped, calling it on every import finish would be wrong."""

    def setUp(self):
        self.fn = next(n for n in ast.walk(_engine_tree())
                       if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))
                       and n.name == KICK)
        self.src = ast.dump(self.fn)

    def test_backlog_gated(self):
        self.assertIn("_measured_history_backlog", self.src)

    def test_reentrancy_guarded(self):
        self.assertIn("_measured_history_drain_running", self.src)

    def test_defers_to_a_running_import_or_delete(self):
        self.assertIn("api_import_running", self.src)
        self.assertIn("delete_in_progress", self.src)

    def test_smb_gated(self):
        self.assertIn("_import_is_smb_capped", self.src)


if __name__ == "__main__":
    unittest.main()
