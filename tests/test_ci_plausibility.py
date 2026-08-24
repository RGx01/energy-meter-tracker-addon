# -*- coding: utf-8 -*-
"""BL-38: unit tests for _ci_slot_plausible (regional generation-mix / intensity glitch guard)."""
import re, unittest
_src = open("engine.py", encoding="utf-8").read()
_fn = re.search(r"(def _ci_slot_plausible\(.*?\n    return True\n)", _src, re.S).group(1)
_ns = {}; exec(_fn, _ns)
plausible = _ns["_ci_slot_plausible"]

GOOD = [{"fuel": "gas", "perc": 40.0}, {"fuel": "solar", "perc": 30.0},
        {"fuel": "wind", "perc": 20.0}, {"fuel": "nuclear", "perc": 10.0}]


class TestCiPlausible(unittest.TestCase):
    def test_good_slot(self):
        self.assertTrue(plausible(GOOD, 169.0, "moderate"))

    def test_solar_97_glitch(self):
        glitch = [{"fuel": "solar", "perc": 97.0}, {"fuel": "gas", "perc": 0.0},
                  {"fuel": "wind", "perc": 0.1}]
        self.assertFalse(plausible(glitch, 0.0, "very low"))

    def test_zero_intensity_alone(self):
        self.assertFalse(plausible(GOOD, 0.0, "very low"))

    def test_negative_intensity(self):
        self.assertFalse(plausible(GOOD, -5.0, "very low"))

    def test_boundary_95_rejected_94_kept(self):
        self.assertFalse(plausible([{"fuel": "wind", "perc": 95.0}], 30.0, "very low"))
        self.assertTrue(plausible([{"fuel": "wind", "perc": 94.0}], 30.0, "low"))

    def test_low_but_nonzero_intensity_ok(self):
        # a genuinely very-green slot: low intensity but not zero, no degenerate fuel
        self.assertTrue(plausible([{"fuel": "wind", "perc": 70.0},
                                   {"fuel": "solar", "perc": 20.0}], 25.0, "very low"))

    def test_empty_mix_ok(self):
        self.assertTrue(plausible([], 200.0, "high"))
        self.assertTrue(plausible(None, 200.0, "high"))

    def test_parse_error_never_rejects(self):
        self.assertTrue(plausible([{"fuel": "gas"}], "notanumber", "x"))  # missing perc / bad intensity


if __name__ == "__main__":
    unittest.main()
