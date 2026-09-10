"""Fix D (4.5.7): run_smb_rate_repair kicks a chart regen when it actually moved the priced
rate layer, so the corrected rates render immediately (the migration commits mid-session,
after the schedule build -- without the kick the user keeps seeing pre-repair rate lines)."""
import os, sys, asyncio, unittest
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import engine


class _Sched:
    def is_empty(self): return False


class _Store:
    def __init__(self): self.marked = None
    def get_kraken_state(self, k): return None            # not yet done
    def set_kraken_state(self, k, v): self.marked = (k, v)
    def vat_rate_at(self, s): return 0.05


class TestSmbRepairRegen(unittest.TestCase):
    def setUp(self):
        self._save = {k: getattr(engine, k) for k in
                      ("_store", "_kraken_rate_schedules", "_import_is_smb_capped",
                       "_measured_floor", "_smb_rate_repair_core", "_write_smb_repair_report",
                       "_schedule_chart_regen")}
        self.regens = []
        engine._store = _Store()
        engine._kraken_rate_schedules = {"import": _Sched()}
        engine._import_is_smb_capped = lambda: True
        engine._measured_floor = lambda: "2026-08-26"
        engine._write_smb_repair_report = lambda res: None
        engine._schedule_chart_regen = lambda: self.regens.append(1)

    def tearDown(self):
        for k, v in self._save.items():
            setattr(engine, k, v)

    def test_regen_kicked_when_rates_changed(self):
        engine._smb_rate_repair_core = lambda *a, **k: {"ok": True, "re_resolved": 3,
                                                        "re_snapped": 0, "unchanged": 0}
        asyncio.run(engine.run_smb_rate_repair())
        self.assertEqual(len(self.regens), 1)      # data moved -> regen kicked

    def test_no_regen_when_nothing_changed(self):
        engine._smb_rate_repair_core = lambda *a, **k: {"ok": True, "re_resolved": 0,
                                                        "re_snapped": 0, "unchanged": 40}
        asyncio.run(engine.run_smb_rate_repair())
        self.assertEqual(len(self.regens), 0)      # idempotent no-op -> no wasted render


if __name__ == "__main__":
    unittest.main()
