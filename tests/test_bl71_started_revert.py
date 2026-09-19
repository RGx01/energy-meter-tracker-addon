"""BL-71 — `started` is adjudicated by `completed` in both directions.

`started` is not returned by the Octopus API: EMT derives it from
SMART_CONTROL_IN_PROGRESS seen while a planned dispatch is active, sampled at the
poll cadence. That makes it a very good finalise-time predictor (measured 97.4%
precision against 61.3% for `planned` alone) but NOT a record — so it has a
false-positive rate, and before BL-71 a false positive was permanent.

These tests pin the four started/completed combinations, the settle-window gate
that preserves the fast restore, and the offline case where absence of `completed`
is not evidence.
"""
import unittest

import engine


def D(**kw):
    """_reconcile_decision with the defaults these cases share."""
    # currently_off_peak defaults True: the slot is priced off-peak today, which
    # is what makes a 'peak' target actionable rather than collapsing to 'ok'.
    args = dict(has_started=False, has_completed=False, completed_energy=None,
                currently_off_peak=True, has_planned=True, was_online=True,
                contemporaneous=True, past_settle=True)
    args.update(kw)
    return engine._reconcile_decision(
        args["has_started"], args["has_completed"], args["completed_energy"],
        args["currently_off_peak"], has_planned=args["has_planned"],
        was_online=args["was_online"], contemporaneous=args["contemporaneous"],
        past_settle=args["past_settle"])


class TestStartedAdjudicatedByCompleted(unittest.TestCase):
    def test_started_and_completed_is_off_peak(self):
        """The ordinary confirmed smart charge — unchanged by BL-71."""
        t, r = D(has_started=True, has_completed=True, completed_energy=-3.1, currently_off_peak=False)
        self.assertEqual(t, "off_peak")

    def test_started_without_completed_past_settle_reverts(self):
        """The BL-71 arm. Measured: 11 such slots in 3 years, all zero charger draw."""
        t, r = D(has_started=True, has_completed=False, past_settle=True)
        self.assertEqual(t, "peak")
        self.assertIn("never completed", r)

    def test_started_without_completed_inside_settle_window_stays_off_peak(self):
        """The fast RESTORE must survive: `started` is real-time, so a solar-supplied
        charge is put right within the 40-minute gate, not hours later."""
        t, r = D(has_started=True, has_completed=False, past_settle=False, currently_off_peak=False)
        self.assertEqual(t, "off_peak")

    def test_started_without_completed_while_offline_stays_off_peak(self):
        """Absence of `completed` is not evidence when we could not have seen it."""
        t, r = D(has_started=True, has_completed=False, past_settle=True,
                 was_online=False, currently_off_peak=False)
        self.assertEqual(t, "off_peak")


class TestUnchangedBehaviour(unittest.TestCase):
    """BL-71 must touch only the started-without-completed case."""

    def test_neither_started_nor_completed_still_peak(self):
        t, r = D(has_started=False, has_completed=False)
        self.assertEqual(t, "peak")

    def test_negligible_completion_still_peak(self):
        t, r = D(has_started=False, has_completed=True, completed_energy=-0.05)
        self.assertEqual(t, "peak")

    def test_completed_not_started_with_planned_still_review(self):
        """The 98 measured false negatives land here or in the branches below —
        none of them may move."""
        t, r = D(has_started=False, has_completed=True, completed_energy=-3.0,
                 has_planned=True)
        self.assertEqual(t, "review")

    def test_completed_only_online_still_bump(self):
        t, r = D(has_started=False, has_completed=True, completed_energy=-3.0,
                 has_planned=False, was_online=True, contemporaneous=True)
        self.assertEqual(t, "peak")

    def test_completed_only_offline_still_off_peak(self):
        t, r = D(has_started=False, has_completed=True, completed_energy=-3.0,
                 has_planned=False, was_online=False, currently_off_peak=False)
        self.assertEqual(t, "off_peak")


class TestDefaultIsSafe(unittest.TestCase):
    def test_past_settle_defaults_to_no_revert(self):
        """A caller that doesn't pass past_settle keeps the old behaviour, so the
        new arm can never fire by omission."""
        t, r = engine._reconcile_decision(True, False, None, False)
        self.assertEqual(t, "off_peak")


class TestTailSlotsAreNotCollateral(unittest.TestCase):
    """63% of missed-`started` slots are the LAST slot of a session, median 0.25 kWh
    against 3.10 for slots that captured it — the charge ended mid-slot. They are
    genuine smart charges and must not be routed through the negligible-energy or
    review branches by BL-71."""

    def test_small_energy_with_started_stays_off_peak(self):
        t, r = engine._reconcile_decision(
            True, True, -0.25, False, has_planned=True, was_online=True,
            contemporaneous=True, past_settle=True)
        self.assertEqual(t, "off_peak",
                         "a started tail slot must not fall into the <0.4 kWh rule")


if __name__ == "__main__":
    unittest.main()
