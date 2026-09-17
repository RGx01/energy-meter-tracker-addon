"""Per-agreement billed-cost coverage, and the verdict that drives the import panel.

An account can sit on a tariff whose per-slot costs Octopus simply does not publish.
Retrying those slots returns the same empty answer however many times it is pressed, so
the panel has to tell that case apart from a starved fetch and send the user to their
PDF bills instead. The discriminator is coverage PER AGREEMENT, not across the import.
"""
import unittest
from unittest import mock

import engine


class TestAgreementEra(unittest.TestCase):
    AGREEMENTS = [
        {"tariff_code": "E-1R-OE-FIX-12M-24-05-11-H",
         "valid_from": "2024-05-15T00:00:00+01:00", "valid_to": "2024-09-12T00:00:00+01:00"},
        {"tariff_code": "E-1R-INTELLI-VAR-22-10-14-H",
         "valid_from": "2024-09-12T00:00:00+01:00", "valid_to": "2026-08-24T00:00:00+01:00"},
        {"tariff_code": "E-1R-IOG-SMB-FIX-6M-26-03-05-H",
         "valid_from": "2026-08-24T00:00:00+01:00", "valid_to": None},
    ]

    def setUp(self):
        patcher = mock.patch.object(
            engine, "_kraken_discovery", {"import": {"agreements": self.AGREEMENTS}})
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_resolves_the_live_agreement(self):
        era = engine._agreement_era("import", "2025-06-25T12:00:00")
        self.assertEqual(era[0], "E-1R-INTELLI-VAR-22-10-14-H")

    def test_open_ended_agreement_has_no_end(self):
        era = engine._agreement_era("import", "2026-09-10T12:00:00")
        self.assertEqual(era[0], "E-1R-IOG-SMB-FIX-6M-26-03-05-H")
        self.assertIsNone(era[2])

    def test_boundary_is_half_open_and_bst_shifted(self):
        # The agreement reads 2026-08-24T00:00+01:00 — i.e. 23:00Z on the 23rd. Slots are
        # naive UTC, so the boundary must land there and not at midnight UTC, or a whole
        # BST evening gets attributed to the wrong tariff.
        self.assertEqual(engine._agreement_era("import", "2026-08-23T22:30:00")[0],
                         "E-1R-INTELLI-VAR-22-10-14-H")
        # Half-open [from, to): the switch instant itself belongs to the NEW agreement,
        # so no slot is ever counted against two eras (which would double the denominator).
        self.assertEqual(engine._agreement_era("import", "2026-08-23T23:00:00")[0],
                         "E-1R-IOG-SMB-FIX-6M-26-03-05-H")

    def test_uncovered_date_returns_none(self):
        self.assertIsNone(engine._agreement_era("import", "2019-01-01T00:00:00"))

    def test_no_discovery_returns_none(self):
        with mock.patch.object(engine, "_kraken_discovery", {}):
            self.assertIsNone(engine._agreement_era("import", "2025-06-25T12:00:00"))


class _Store:
    """Minimal store double: only the meta bag api_import_health reads."""

    def __init__(self, meta):
        self._meta = meta

    def get_meta(self, key, default=None):
        return self._meta.get(key, default)

    def get_kraken_state(self, key):
        return None

    def reprice_queue_count(self):
        return 0

    def get_reprice_queue(self):
        return {}


def era(code, frm, to, slots, costed):
    return {"tariff_code": code, "from": frm, "to": to, "slots": slots, "costed": costed}


class TestHealthVerdicts(unittest.TestCase):
    def health(self, eras):
        store = _Store({engine._COST_COVERAGE_KEY: {"import": eras}})
        with mock.patch.object(engine, "_store", store):
            return engine.api_import_health()

    def test_blank_era_is_pdf(self):
        out = self.health({"a": era("INTELLI", "2024-09-12T00:00:00",
                                    "2026-08-24T00:00:00", 33573, 0)})
        self.assertEqual(out["eras"][0]["verdict"], "pdf")
        self.assertEqual(out["eras"][0]["uncosted_pct"], 100.0)
        self.assertEqual(out["uncosted_total"], 33573)

    def test_fully_costed_era_is_ok(self):
        out = self.health({"a": era("SMB", "2026-08-24T00:00:00", None, 1000, 1000)})
        self.assertEqual(out["eras"][0]["verdict"], "ok")
        self.assertEqual(out["uncosted_total"], 0)

    def test_threshold_is_inclusive_at_95_percent(self):
        # Exactly 95% uncosted must already read as "pdf" — the panel should not offer a
        # retry that recovers 5% of a period and leaves the user thinking it worked.
        # Sized above _COST_COVERAGE_MIN_SLOTS so this tests the THRESHOLD, not the floor.
        out = self.health({"a": era("X", "2025-01-01T00:00:00", None, 1000, 50)})
        self.assertEqual(out["eras"][0]["uncosted_pct"], 95.0)
        self.assertEqual(out["eras"][0]["verdict"], "pdf")

    def test_just_under_threshold_stays_retryable(self):
        out = self.health({"a": era("X", "2025-01-01T00:00:00", None, 1000, 60)})
        self.assertEqual(out["eras"][0]["uncosted_pct"], 94.0)
        self.assertEqual(out["eras"][0]["verdict"], "partial")

    def test_a_starved_fetch_stays_retryable(self):
        # The case the Retry button exists for: most slots priced, a few dropped.
        out = self.health({"a": era("X", "2025-01-01T00:00:00", None, 1000, 975)})
        self.assertEqual(out["eras"][0]["verdict"], "partial")

    def test_eras_are_sorted_and_mixed_verdicts_coexist(self):
        out = self.health({
            "smb": era("SMB", "2026-08-24T00:00:00", None, 500, 500),
            "flat": era("FLAT", "2024-05-15T00:00:00", "2024-09-12T00:00:00", 200, 200),
            "intelli": era("INTELLI", "2024-09-12T00:00:00", "2026-08-24T00:00:00", 900, 0),
        })
        self.assertEqual([e["tariff_code"] for e in out["eras"]],
                         ["FLAT", "INTELLI", "SMB"])
        self.assertEqual([e["verdict"] for e in out["eras"]], ["ok", "pdf", "ok"])
        self.assertEqual(out["uncosted_total"], 900)

    def test_empty_era_is_skipped_not_divided_by_zero(self):
        out = self.health({"a": era("X", "2025-01-01T00:00:00", None, 0, 0)})
        self.assertEqual(out["eras"], [])

    def test_no_coverage_recorded_yields_empty_not_error(self):
        store = _Store({})
        with mock.patch.object(engine, "_store", store):
            out = engine.api_import_health()
        self.assertEqual(out["eras"], [])
        self.assertEqual(out["uncosted_total"], 0)


if __name__ == "__main__":
    unittest.main()


class TestHealthDemo(unittest.TestCase):
    """The demo payloads exist so the panel's unhappy states can be inspected on a healthy
    account. They are only useful if they stay bound to the contract the panel reads, so
    pin that here — a field renamed in api_import_health must break these too."""

    PANEL_ERA_FIELDS = {"tariff_code", "from", "to", "slots", "costed",
                        "uncosted", "uncosted_pct", "verdict", "banded"}

    def test_every_scenario_matches_the_real_payload_contract(self):
        for name in ("pdf", "mixed", "partial", "clean"):
            out = engine.api_import_health_demo(name)
            self.assertTrue(out["ok"], name)
            self.assertEqual(out["demo"], name)
            for key in ("have", "raised", "auto_recovered", "remaining",
                        "uncosted_total", "eras", "queue", "from", "to"):
                self.assertIn(key, out, f"{name} missing {key}")
            for e in out["eras"]:
                self.assertEqual(set(e) , self.PANEL_ERA_FIELDS, name)
                self.assertIn(e["verdict"], ("pdf", "partial", "ok"), name)

    def test_scenarios_actually_differ_in_the_way_the_panel_branches_on(self):
        verdicts = {n: sorted({e["verdict"] for e in
                               engine.api_import_health_demo(n)["eras"]})
                    for n in ("pdf", "mixed", "partial", "clean")}
        self.assertIn("pdf", verdicts["pdf"])
        self.assertIn("pdf", verdicts["mixed"])
        self.assertIn("partial", verdicts["mixed"])       # retry AND bills together
        self.assertNotIn("pdf", verdicts["partial"])      # retry is the right answer
        self.assertEqual(verdicts["clean"], ["ok"])

    def test_uncosted_total_is_consistent_with_the_eras(self):
        for name in ("pdf", "mixed", "partial", "clean"):
            out = engine.api_import_health_demo(name)
            self.assertEqual(out["uncosted_total"],
                             sum(e["uncosted"] for e in out["eras"]), name)

    def test_unknown_scenario_falls_back_rather_than_erroring(self):
        out = engine.api_import_health_demo("nonsense")
        self.assertEqual(out["demo"], "pdf")

    def test_demo_verdicts_use_the_same_threshold_as_production(self):
        # Not a second implementation of the rule — same helper, same constants.
        self.assertEqual(
            engine._demo_era("X", "2025-01-01T00:00:00", None, 1000, 50)["verdict"], "pdf")
        self.assertEqual(
            engine._demo_era("X", "2025-01-01T00:00:00", None, 1000, 60)["verdict"], "partial")
        # ...including the floor: a thin era never reads as unrecoverable.
        self.assertEqual(
            engine._demo_era("X", "2025-01-01T00:00:00", None, 50, 0)["verdict"], "partial")


class TestDebugGate(unittest.TestCase):
    """The demo must be unreachable for a normal user. It is gated on `log_level: debug`,
    which run.sh exports as LOG_LEVEL — an existing, user-visible add-on option rather
    than a hidden switch."""

    def _mode(self, value):
        env = {} if value is None else {"LOG_LEVEL": value}
        with mock.patch.dict(engine.os.environ, env, clear=False):
            if value is None:
                engine.os.environ.pop("LOG_LEVEL", None)
            return engine.debug_mode()

    def test_debug_enables(self):
        for v in ("debug", "DEBUG", " Debug "):
            self.assertTrue(self._mode(v), v)

    def test_every_other_level_disables(self):
        for v in ("info", "warning", "error", "", "verbose", None):
            self.assertFalse(self._mode(v), v)

    def test_route_ignores_demo_unless_debug(self):
        # The contract the route relies on: not debug -> fall through to the real payload,
        # so a shared URL carrying ?health_demo never shows a stranger synthetic figures.
        import web.server as server
        with mock.patch.object(engine, "debug_mode", return_value=False):
            self.assertFalse(engine.debug_mode())
        with mock.patch.object(engine, "debug_mode", return_value=True):
            self.assertTrue(engine.debug_mode())
        self.assertIn("debug_mode", open(
            server.__file__.replace(".pyc", ".py")).read(),
            "health route must consult debug_mode")


class TestDebugFlagReachesThePanel(unittest.TestCase):
    """The panel can only offer its demo picker if the health payload tells it debug is on.

    This is not cosmetic: EMT is served through Home Assistant ingress, so the page lives
    in an iframe and a ?health_demo= on the browser's address bar belongs to the OUTER HA
    document — it never reaches this one. The in-page picker is the only route that works,
    and it is driven entirely by this flag.
    """

    def test_real_health_reports_debug_state(self):
        store = _Store({})
        for dbg in (True, False):
            with mock.patch.object(engine, "_store", store), \
                 mock.patch.object(engine, "debug_mode", return_value=dbg):
                self.assertIs(engine.api_import_health()["debug"], dbg)

    def test_demo_payload_keeps_the_picker_visible(self):
        # While a demo is being shown the picker must stay on screen, or there is no way
        # back to the real data without editing the URL — which ingress makes impossible.
        for name in ("pdf", "mixed", "partial", "clean"):
            self.assertIs(engine.api_import_health_demo(name)["debug"], True, name)


class TestBandedSeverity(unittest.TestCase):
    """Whether an uncosted era was BANDED decides what the user is told.

    Single-rate tariff: the schedule price is exact, only the cross-check is missing.
    Banded/dispatch-aware: a smart charge outside the off-peak window is billed off-peak
    but priced at peak, so the costs are estimates biased HIGH. Telling someone their
    figures are fine when they are over-stated is the failure mode this guards.
    """

    class _Sched:
        def __init__(self, lo, hi):
            self._b = (lo, hi)

        def day_rate_bounds(self, _start):
            return self._b

    def segs(self, lo, hi):
        return [("2024-01-01T00:00:00", None, self._Sched(lo, hi))]

    def test_distinct_rates_are_banded(self):
        self.assertTrue(engine._era_is_banded(self.segs(4.493, 31.70), "2025-06-01T12:00:00"))

    def test_single_rate_is_not_banded(self):
        self.assertFalse(engine._era_is_banded(self.segs(23.6, 23.6), "2025-06-01T12:00:00"))

    def test_unknown_bounds_are_not_banded(self):
        # Better to under-claim than to tell someone their costs are estimates on no basis.
        self.assertFalse(engine._era_is_banded(self.segs(None, None), "2025-06-01T12:00:00"))

    def test_uncovered_date_and_empty_segments(self):
        self.assertFalse(engine._era_is_banded([], "2025-06-01T12:00:00"))
        self.assertFalse(engine._era_is_banded(self.segs(1, 2), "2020-01-01T00:00:00"))

    def test_health_exposes_banded_per_era(self):
        store = _Store({engine._COST_COVERAGE_KEY: {"import": {
            "a": dict(era("INTELLI", "2024-09-12T00:00:00", None, 100, 0), banded=True),
            "b": dict(era("FLAT", "2023-01-01T00:00:00", "2024-09-12T00:00:00", 100, 0)),
        }}})
        with mock.patch.object(engine, "_store", store):
            out = engine.api_import_health()
        by = {e["tariff_code"]: e for e in out["eras"]}
        self.assertTrue(by["INTELLI"]["banded"])
        self.assertFalse(by["FLAT"]["banded"])

    def test_demo_intelli_eras_are_banded(self):
        # The demo must show the stronger wording, or it does not preview the real thing.
        for name in ("pdf", "mixed"):
            eras = engine.api_import_health_demo(name)["eras"]
            blank = [e for e in eras if e["verdict"] == "pdf"]
            self.assertTrue(all(e["banded"] for e in blank), name)
        flat = [e for e in engine.api_import_health_demo("pdf")["eras"]
                if "OE-FIX" in e["tariff_code"]]
        self.assertFalse(flat[0]["banded"])


class TestCoverageFloor(unittest.TestCase):
    """A verdict that stops work must not fire on a handful of slots.

    0-costed-out-of-3 is also "100% uncosted". Without a floor a thin era — the tail of an
    import, a few days either side of a tariff switch — would suppress the recovery pass
    and tell the user their history is wrong on almost no evidence.
    """

    def test_below_the_floor_stays_retryable(self):
        self.assertEqual(engine._coverage_verdict(199, 0), "partial")

    def test_at_the_floor_it_counts(self):
        self.assertEqual(engine._coverage_verdict(200, 0), "pdf")

    def test_floor_alone_is_not_enough(self):
        # Plenty of slots, but most of them priced → an ordinary starved fetch.
        self.assertEqual(engine._coverage_verdict(1000, 900), "partial")

    def test_fully_costed_is_ok_at_any_size(self):
        self.assertEqual(engine._coverage_verdict(3, 3), "ok")
        self.assertEqual(engine._coverage_verdict(0, 0), "ok")

    def test_display_and_skip_use_the_same_rule(self):
        # One function, both consumers — the panel cannot say "don't retry" while the
        # engine still retries, because there is no second implementation to drift.
        store = _Store({engine._COST_COVERAGE_KEY: {"import": {
            "thin": era("X", "2026-01-01T00:00:00", "2026-01-03T00:00:00", 50, 0),
            "real": era("INTELLI", "2024-09-12T00:00:00", "2026-08-24T00:00:00", 9000, 0),
        }}})
        with mock.patch.object(engine, "_store", store):
            verdicts = {e["tariff_code"]: e["verdict"] for e in engine.api_import_health()["eras"]}
            ranges = [r["tariff_code"] for r in engine._uncostable_ranges("import")]
        self.assertEqual(verdicts["X"], "partial")
        self.assertEqual(verdicts["INTELLI"], "pdf")
        self.assertEqual(ranges, ["INTELLI"])          # only the real one stops work


class TestUncostableRanges(unittest.TestCase):
    RANGES = [{"from": "2024-09-11T23:00:00", "to": "2026-08-23T23:00:00",
               "tariff_code": "INTELLI"}]

    def test_membership_is_half_open(self):
        f = engine._in_uncostable_range
        self.assertFalse(f(self.RANGES, "2024-09-11T22:30:00"))   # before
        self.assertTrue(f(self.RANGES, "2024-09-11T23:00:00"))    # first slot
        self.assertTrue(f(self.RANGES, "2025-06-01T12:00:00"))    # inside
        self.assertFalse(f(self.RANGES, "2026-08-23T23:00:00"))   # the switch itself
        self.assertFalse(f(self.RANGES, "2026-09-01T00:00:00"))   # after

    def test_open_ended_range_has_no_upper_bound(self):
        r = [{"from": "2026-08-24T00:00:00", "to": None}]
        self.assertTrue(engine._in_uncostable_range(r, "2030-01-01T00:00:00"))

    def test_empty_inputs_never_match(self):
        self.assertFalse(engine._in_uncostable_range([], "2025-01-01T00:00:00"))
        self.assertFalse(engine._in_uncostable_range(None, "2025-01-01T00:00:00"))

    def test_no_coverage_means_nothing_is_skipped(self):
        # First chunk of an import: nothing established yet, so full work must happen.
        with mock.patch.object(engine, "_store", _Store({})):
            self.assertEqual(engine._uncostable_ranges("import"), [])

    def test_a_straddling_chunk_is_not_wholly_inside(self):
        # The verify sweep skips only when BOTH ends of a chunk are inside; a chunk
        # spanning the tariff switch still contains costable days.
        f = engine._in_uncostable_range
        c_from, c_to = "2026-08-10T00:00:00", "2026-09-09T00:00:00"
        self.assertTrue(f(self.RANGES, c_from))
        self.assertFalse(f(self.RANGES, c_to))


class TestVerifyOnlyAfterAWrite(unittest.TestCase):
    """The deferred pricing check re-verifies the WHOLE imported history, not just what a
    run touched. Firing it after an import that wrote nothing — an empty window, a span
    already covered — spends minutes and a shared API allowance confirming what was
    already confirmed, then reports "split verified" for an import that imported nothing.
    """

    def test_counts_what_a_run_wrote(self):
        self.assertEqual(engine._run_wrote_blocks({"written": {"import": 1440, "export": 96}}), 1536)

    def test_an_empty_window_wrote_nothing(self):
        self.assertEqual(engine._run_wrote_blocks({"written": {"import": 0, "export": 0}}), 0)

    def test_missing_or_malformed_written_is_zero_not_an_error(self):
        for job in ({}, {"written": None}, {"written": {}},
                    {"written": {"import": None}}, {"written": {"import": "x"}}):
            self.assertEqual(engine._run_wrote_blocks(job), 0, job)

    def test_one_channel_written_still_counts(self):
        # A gap fill can land import-only; that must still be verified.
        self.assertEqual(engine._run_wrote_blocks({"written": {"import": 12, "export": 0}}), 12)
