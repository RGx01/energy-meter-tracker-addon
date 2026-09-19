# Roadmap

*Active backlog only — priority ordered. Shipped, closed and superseded items live in
[ROADMAP_archive.md](ROADMAP_archive.md); release detail is in [CHANGELOG.md](../CHANGELOG.md).
Nothing in this file has shipped.*

**4.5.14 — the last v4 feature release.** BL-71, BL-72 and BL-73 landed and have moved to the
archive. After .14 this repository accepts **critical fixes only** — data-loss, crash-on-start or a
pricing error — each cherry-picked into the EMT repository the same day. A fix that adds a heal also
raises the floor version EMT will import from.

**Issue tracking.** Every open item should carry a GitHub issue reference. Items marked
⚠️ **needs issue** have none yet — see [Issues to create](#issues-to-create) at the foot.

| # | Item | Area | Target | Issue |
|---|------|------|--------|-------|
| 1 | BL-47 — Auto-run recorder device attribution across gap-filled windows | attribution | next | ⚠️ needs issue |
| 2 | BL-50 — Unify user-job mutual exclusion | architecture · correctness | v5.0.0 | ⚠️ needs issue |
| 3 | BL-33 — Remove the one-time legacy migrations | deprecation | v5.0.0 | ⚠️ needs issue |
| 4 | BL-61 — 4.5.7 settlement/chart cleanup follow-ups | tech-debt | v5.0.0 | ⚠️ needs issue |
| 5 | BL-63 — Two pricing models: collapse onto one (Principle 0) | architecture · first-principles | v5.0.0 | ⚠️ needs issue |
| 6 | BL-64 — Bill-parser fixtures + a pypdf bump gate | testing · tooling | next | ⚠️ needs issue |
| 7 | BL-60 — Usage Stats block inspector (pre/post-settlement) | diagnostics | proposed | ⚠️ needs issue |

---

## Open backlog

#### BL-47 — Auto-run recorder device attribution across freshly gap-filled windows  ·  *attribution · medium*  ·  ⚠️ **needs issue**
*Surfaced during the BL-46 (4.5.4) device-delete review.* When an outage gap-fills the house total from the supplier API, the **synthetic** dispatch-EV split self-heals automatically (BL-39, every reconcile pass) — but a **physical** sub-meter (Zappi/Ohme CT-clamp, Fox/Indra battery, any recorder-backed device) has **no** automatic reconstruction. The API only returns the *house* total for the gap, so the device's share of that window folds entirely into house/Direct until the user **manually** runs Device History (BL-12) for the range. This is also the un-automated step the BL-46 heal-via-reimport story leans on: "delete the data and re-import the gap" only refills a physical device if you then hand-run the recorder attribution. **Fix:** after a gap-fill (and after an import/CSV backfill) that covers a window a recorder-backed sub-meter was recording in, offer to (or, opt-in, automatically) replay that device's recorder history across exactly the filled blocks — reusing the existing `run_attribution_job` path (grid-clipped, house-capped, reversible via the attribution back-out ledger). Bounded to recorder-backed **physical** sub-meters (never the synthetic dispatch EV, which BL-39 already heals); opt-in and idempotent; re-derives the parent remainder on completion (shares the BL-46 EV-aware recompute). Turns "gap-filled, now go remember to re-attribute each device" into a one-click (or automatic) heal. Depends on: BL-12 (recorder attribution, shipped), BL-46 (EV-aware parent recompute, shipped 4.5.4).

#### BL-50 — Unify user-job mutual exclusion (deny + idempotent-retry)  ·  *architecture · correctness · target v5.0.0*  ·  ⚠️ **needs issue**
*Surfaced across the 4.5.4 device-delete / gap-fill / attribution work — a recurring race class we kept fixing pairwise.* EMT's bounded DB-mutating jobs (import / **gap fill**, **device delete** + recompute, device-history attribution, attribution backout, delete-blocks, manual/CSV reprice, DB restore) plus the automatic batch tails (post-import **recovery + verify**, the reprice-history migration sweep, historical carbon backfill) all write one shared SQLite store. Overlapping them races (the `float * NoneType` attribution-over-unverified-blocks crash; the delete-drains-import / attribution-waits-for-verify guards). Today we guard **pairwise** — O(N²), and we only find a missing pair when a user hits it. **Fix:** one coordinator enforcing a single invariant — *at most one bounded mutating job at a time; a new user job is refused (`409 + running`), not queued*. Because every such job is **idempotent**, a denied user just retries when the running one finishes and lands correctly (idempotency buys the retry, not concurrency — so no queue needed). The lock is held across a job's **whole pipeline including its verify tail**, which is the single change that closes the race class. The live engine tick is orthogonal (continuous, handled by the loop-lock / `pause_engine` layer, not this coordinator). Retires the interim pairwise guards (`_verify_running`, delete's import-drain, refuse-reprice-during-migration, scattered `delete_in_progress` checks) and lets the run-lock UI read one `current_job()`. Targeted at **v5.0.0** alongside the BL-27 aggregation-unify — both remove a recurring drift/race bug class at the root under one migration-gated release. Design: `docs/design/BL-50_user_job_mutual_exclusion_design.md`.

#### BL-33 — Remove the one-time legacy migrations  ·  *deprecation · target v5.0.0*  ·  ⚠️ **needs issue**
*Announced with 4.4.0; the removal itself is still outstanding.* 4.4.0 shipped one-time background
migrations that bring a pre-4.4.0 database up to the priced-segment model (segment backfill, the
capped-IOG pence→£ repair, canonical-rate re-stamp). From **v5.0.0** those migrations are removed, so
a database below 4.4.0 must be upgraded **through** 4.4.x first. Work: delete the migration paths and
their gates, drop the upgrade-sweep banner wiring that exists only to report them, state the minimum
supported upgrade-from version in the README, and fail clearly (rather than silently mispricing) if a
pre-4.4.0 schema is opened. Pairs with BL-61's `rate_source` rename and BL-50's job coordinator — all
three want the same migration-gated release.

#### BL-61 — 4.5.7 settlement/chart cleanup follow-ups  ·  *tech-debt · target v5.0.0*  ·  ⚠️ **needs issue**
*Transitional scaffolding from the 4.5.7 IOG-SMB settlement rework, to retire once the release has
settled.* (1) The **one-off SMB rate-repair migration** (re-derives feed-gap-mispriced blocks from the
fixed schedule on upgrade; gated + idempotent) is removed once users have upgraded — same pattern as
the 4.4.0 one-time migrations. (2) The **`chart_emit` house-TOU override** is now redundant — with the
day/night feed-gap fixed, the stored idle-slot rate is already the correct clean rate, so the override
only re-affirms it; retire it as part of (3) the **single charting API**: collapse the billing-chart /
usage-chart duo into one rate-line layer (today's `chart_emit` + `energy_charts` split), landing with
the BL-27 aggregation-unify. (4) **Simplify the import-recovery ladder** — the forward-extending window
exists only to coax an OFF_PEAK *label* out of the Measurements API, but the band comes from
cost/buckets, not the label, so it can collapse to a plain small-window cost fetch (retire
`recover_measurement_costs`'s `lookahead_ladder`). (5) **Rename `rate_source` `'measured'` →
`'settled'`** to match the design hierarchy — deferred from 4.5.7 because it forces a data migration of
every existing `'measured'` block row, best batched under one migration-gated release.

#### BL-63 — Two pricing models: decide which one is the model, and collapse onto it  ·  *architecture · first-principles · target v5.0.0*  ·  ⚠️ **needs issue**

*Surfaced during the BL-62 investigation (Sept 2026), where `reprice.py` reads as the canonical pricer, carries the exact arithmetic fingerprint of the rate being chased, and has no production caller at all — which cost most of a day's misdiagnosis.*

**Where it came from.** The 4.4.0 design opens (§0) with the bug class it exists to kill: *"an authoritative input changed (a dispatch completed, a block settled, a reconcile ran, the VAT calendar changed) and we updated **some** derived fields but not others, so the ex-VAT figures, or the EV split, or the segments, or the bands went stale."* Root cause 2 was named as *"derived fields maintained piecemeal — no single operation owned 'recompute everything this block derives'."* The remedy was **Principle 0** — one pure function turning a block's authoritative inputs into every derived value, with all other paths conforming — implemented as `reprice.reprice_block`, with `pricing_segments` as its representation layer and `carbon.py` as the adjacent pass.

**What actually happened.** §P3.3 chose option (A), *"to remain faithful to the one model"*, staged as route → prove conformance → collapse, with (B) rejected as *"Safer; two pricing models persist."* P3.3a/b/c then built `_reprice_history_block` **inside engine.py** and flipped the sweep onto that. The routing shipped; the collapse never did. The outcome is the rejected (B).

**Current state (measured on prod-dev, 15 Sept 2026).**
- **Live model:** engine's `_reprice_history_block` + the finalise/settlement seam. This is what runs.
- **Shelved model:** `reprice.reprice_block` (169 lines, all four functions unreachable); `carbon.carbon_from_reprice`, plus `ev_carbon` and `house_carbon`, which are reachable *only* through it; and **3 of 11** public functions in `pricing_segments` — `attribute_devices`, `import_segments`, `total_cost_exc`. Zero production callers; ~37 test references, so the suite is green and the code reads as load-bearing.

  > **Corrected 19 Sep 2026 — this bullet previously read "9 of 11 public functions in `pricing_segments` (the whole projection layer)", and acting on that would have deleted working code.** Traced by reachability from the two functions that *do* have external callers (`segments_from_legacy`, `price_devices_hybrid`), **8 of the 11 are live** — `total_kwh`, `total_cost`, `blended_rate`, `attribution_kwh`, `attribution_cost` and `attribution_rate` are all reached transitively. No *external* caller is not the same as no caller; for a helper inside its own module it usually means correctly encapsulated. Only three are genuinely unreachable.
  >
  > The carbon half of the original bullet was right, and is kept: `carbon_from_reprice` has no caller anywhere, and `ev_carbon`/`house_carbon` are called only from inside it. (Apparent hits elsewhere are local variable names — `ev_carbon_block`, `house_carbon_g` in `web/server.py` — not calls. `period_ev_saving` and `slot_intensity` are live and are **not** part of the shelved set.)
- **The requirement is already met by the live path.** Block-vs-segment rate agreement across every measured block since 1 Sept: **0 mismatches**. The atomic-recompute invariant was reached incrementally (BL-27 segments-as-truth, P3.3a's unified sweep, the settlement seam writing all derived fields together) rather than via Principle 0.

**The residue that is NOT solved.** Derived rates are back-computed from a 6-dp rounded cost rather than carrying the canonical band rate, so they scatter: 12 measured blocks hold `imp_rate != imp_rate_ev`, worst case 1.3e-5 (0.05493 vs 0.054917). Cost impact nil — but it is exactly what let a blended rate slip past reconcile's `abs(cur_rate - off_peak) < 1e-6` band test in BL-62, silently disarming the bump revert. Any exact-equality comparison against a canonical rate is defeatable this way.

**Decision required at v5.0.0** (do not act before — switching live pricing to an unexercised path is the larger risk, and the 4.4.0 doc itself rates (A) *"higher risk, perf-sensitive (a re-price per block over years)"*):
1. **Finish the collapse** — route everything through `reprice_block`, delete engine's copy. Faithful to Principle 0; highest risk; buys an invariant already held.
2. **Adopt the live model** — retire `reprice.py`, `carbon_from_reprice` with `ev_carbon`/`house_carbon`, and the three unreachable `pricing_segments` functions, with their tests; record in the 4.4.0 design that Principle 0 was satisfied incrementally. Lowest risk; removes the trap that a shelved-but-tested module reads as canonical. **Scope is the named functions, not "the projection layer"** — see the correction above; the other eight are live.
3. **Keep both** — rejected: it is the status quo, and it has already cost a misdiagnosis.

**Before acting on (2), re-run the trace.** These figures are from 19 Sep 2026 against merged 4.5.13, and the first version of them was wrong in the direction that deletes working code. Reachability from real entry points, over-approximating, is the only method that has given the right answer here — a grep for the function name counts local variables and comments, which is how "9 of 11" happened.

**Recommendation: (2), plus the rounding fix** — carry the canonical band rate instead of re-deriving it from rounded cost, or compare bands with a tolerance that reflects storage precision (1e-4, matching the existing near-identical-rate clustering). Pairs naturally with BL-33 (one-time-migration removal) and BL-61 (4.5.7 tech-debt), both already v5.0.0.

#### BL-64 — Bill-parser fixtures from real bills, and a pypdf bump gate  ·  *testing · tooling · target next*  ·  ⚠️ **needs issue**

*Surfaced 16 Sept 2026 while clearing `pypdf==6.18.0`. Verifying a pypdf bump needs real Octopus bills, which cannot go in the repo — so there is no CI gate and the check depends on remembering to ask for one.*

**Two risks are tangled here, and only one actually needs the PDFs.** `_read_pages` is the entire pypdf surface — one function, `list[str]` out. Everything above it is pure text → `Bill`.

- **Risk A — pypdf changes what it extracts.** Needs real bills; genuinely not CI-able. But it only matters on a bump, which is deliberate and infrequent.
- **Risk B — a parser change breaks real-world bills.** Needs realistic *text*, not PDFs — so it IS CI-able, and today it is not covered. `tests/test_bill_parser.py` hand-writes an idealised summary plus synthetic HH pages of 48 identical 0.1 kWh rows: no real-bill quirks, no mid-period standing-charge change, no export MPAN alongside import.

**Evidence the distinction matters.** 6.16.1 → 6.18.0 over five real bills: all five parsed to **byte-identical `Bill` objects** (7,344 HH readings, standing charges, VAT, reconciliation, zero warnings) — but the extracted **text** changed on four. Leading whitespace (`Supply number` → ` Supply number`, from 6.16.2's space-width leniency), and on the 2024-03-06 bill a day header lost its newline: `Monday\n5th February 2024` → `Monday5th February 2024`. The parser absorbed it; a slightly different layout might not. The earlier token-count comparison (13 Sept, 6.16.1 vs 6.16.2) would **not** have surfaced that — counting probe strings is not enough, and one of those probes was itself wrong (`Standing charge` vs the bills' `Standing Charge`), reporting a phantom gap.

**Proposed work.**
1. **Commit redacted text fixtures** — snapshot `_read_pages()` output from the real bills; redact MPAN / meter serial / account number / name / address / direct-debit amounts; keep kWh and rates, which are what the reconciliation check exercises. ~70 KB each, ~350 KB total, plain text so diffs stay reviewable. Re-point the parser tests at these: five genuinely-shaped bills spanning 2024–2026, including a mid-period standing-charge change and import+export MPANs.
2. **Commit the gate, not the bills** — a `skipUnless` harness keyed on an env var (e.g. `EMT_BILL_FIXTURES`) pointing at a private directory: absent in CI, one command locally on a bump. Compares extracted text AND the parsed `Bill` across old and new pins. A working prototype exists from this investigation.
3. **Fold it into the bump procedure** so regenerating fixtures is a planned step.

**Caveats to design for.**
- Redaction is one-way risk: a sloppy pass puts an MPAN in git history permanently. The generator needs a verification pass that greps the finished fixtures for every real identifier and fails loudly.
- A text fixture freezes one pypdf version's output, so a bump may require regenerating it. That must be deliberate, not a surprise CI failure that invites a blind refresh.

*Related: `pypdf==6.18.0` was verified against five real bills on 16 Sept 2026; `requirements.txt` still pins 6.16.1.*

#### BL-60 — Usage Stats block inspector: pre/post-settlement detail for a single block  ·  *diagnostics · proposed*  ·  ⚠️ **needs issue**
*Turns the manual forensic we keep repeating into a self-serve drill-down.* Diagnosing a single
half-hour today means hand-running SQL across `blocks`, `block_segments`, `measured_cost` and
`dispatch_history`, plus live Measurements probes — exactly the process used on the 2026-08-14 19:00
and 2026-09-11 07:00 investigations. **Proposal:** click a block in Usage Stats and see its full
provenance in one panel — what EMT priced it and from which authority (`rate_source`), what Octopus
billed it (settled cost, band label, the four-bucket split), the dispatch lifecycle behind it
(planned / started / completed), and how it changed from provisional → settled. Read-only; no pricing
impact. Would have made the 07:00 mis-band self-evident instead of a multi-hour trace. Design:
`docs/design/EMT-BL60_block_inspector_pre_post_settlement.md`.

---

## Issues to create

None of the open items above currently carry a GitHub issue. Each needs one raising against
[RGx01/energy-meter-tracker-addon](https://github.com/RGx01/energy-meter-tracker-addon/issues),
then the reference added to its heading and to the priority table:

- [ ] **BL-47** — Auto-run recorder device attribution across freshly gap-filled windows
- [ ] **BL-50** — Unify user-job mutual exclusion (deny + idempotent-retry)
- [ ] **BL-33** — Remove the one-time legacy migrations (v5.0.0 deprecation)
- [ ] **BL-61** — 4.5.7 settlement/chart cleanup follow-ups
- [ ] **BL-63** — Two pricing models: decide which one is the model, and collapse onto it
- [ ] **BL-64** — Bill-parser fixtures from real bills, and a pypdf bump gate
- [ ] **BL-60** — Usage Stats block inspector: pre/post-settlement detail for a single block

*Convention: reference issues as `[#nnn]` on the item heading, with the link definition at the foot
of the file (as the archive does).*
