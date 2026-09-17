"""
BL-68: the re-pricing banner must not call a fresh install's own import an upgrade.

`_run_historical_reprice_sweep` has TWO triggers — the first run after an upgrade, and the
forced pass the verify stage runs after every import / gap-fill (P3.3d) — but the banner
named only the first. A brand-new install that imported its history was therefore told
"Finishing your upgrade", on a box that had never run a previous version and had just been
asked to do exactly this work. The advice attached to it ("avoid restarting or rebuilding")
made a normal, expected wait read like a warning.
"""

import ast
import json
import os
import sys
import unittest
from datetime import datetime, timedelta

_HERE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _HERE)


def _cause_fn():
    """Lift _reprice_sweep_cause out of web/server.py without importing Flask."""
    src = open(os.path.join(_HERE, "web", "server.py")).read()
    ns = {}
    for n in ast.parse(src).body:
        if isinstance(n, ast.FunctionDef) and n.name == "_reprice_sweep_cause":
            exec(compile(ast.Module([n], []), "<s>", "exec"), ns)
        elif isinstance(n, ast.Assign) and getattr(
                n.targets[0], "id", "") == "_REPRICE_CAUSE_WINDOW_H":
            exec(compile(ast.Module([n], []), "<s>", "exec"), ns)
    return ns["_reprice_sweep_cause"], ns


class _Store:
    def __init__(self, finished_at=None):
        self._fin = finished_at

    def get_kraken_state(self, key):
        if self._fin is None:
            return None
        return json.dumps({"status": "done", "finished_at": self._fin})


class _Eng:
    _IMPORT_RUN_KEY = "import_run_status"

    def __init__(self, running=False):
        self._running = running

    def api_import_running(self):
        return self._running


class TestRepriceSweepCause(unittest.TestCase):

    def setUp(self):
        self.fn, self.ns = _cause_fn()
        self._real = sys.modules.get("engine")

    def tearDown(self):
        if self._real is not None:
            sys.modules["engine"] = self._real
        else:
            sys.modules.pop("engine", None)

    def _run(self, store, running=False):
        sys.modules["engine"] = _Eng(running)
        return self.fn(store)

    def test_a_live_import_owns_the_sweep(self):
        self.assertEqual(self._run(_Store(), running=True), "import")

    def test_a_just_finished_import_owns_the_sweep(self):
        """The reported case: fresh install, import finishes, sweep runs on behind it."""
        fin = (datetime.utcnow() - timedelta(minutes=3)).isoformat()
        self.assertEqual(self._run(_Store(fin)), "import")

    def test_an_old_import_does_not(self):
        fin = (datetime.utcnow() - timedelta(days=4)).isoformat()
        self.assertEqual(self._run(_Store(fin)), "upgrade")

    def test_no_import_history_reads_as_upgrade(self):
        self.assertEqual(self._run(_Store(None)), "upgrade")

    def test_window_boundary(self):
        w = self.ns["_REPRICE_CAUSE_WINDOW_H"]
        just_in = (datetime.utcnow() - timedelta(hours=w - 1)).isoformat()
        just_out = (datetime.utcnow() - timedelta(hours=w + 1)).isoformat()
        self.assertEqual(self._run(_Store(just_in)), "import")
        self.assertEqual(self._run(_Store(just_out)), "upgrade")

    def test_garbage_fails_safe_to_the_old_wording(self):
        for bad in ("not-a-date", "", "2026-13-45T99:99:99"):
            self.assertEqual(self._run(_Store(bad)), "upgrade", bad)

    def test_a_broken_store_fails_safe(self):
        class Boom:
            def get_kraken_state(self, key):
                raise RuntimeError("db gone")
        self.assertEqual(self._run(Boom()), "upgrade")


class TestBannerWording(unittest.TestCase):
    """The template must branch on the cause, and never hard-code 'upgrade' alone."""

    def setUp(self):
        self.html = open(os.path.join(_HERE, "web", "templates", "base.html")).read()

    def test_import_wording_exists(self):
        self.assertIn("Finishing your import", self.html)

    def test_upgrade_wording_survives(self):
        self.assertIn("Finishing your upgrade", self.html)

    def test_it_branches_on_the_cause(self):
        self.assertIn("d.cause", self.html)
