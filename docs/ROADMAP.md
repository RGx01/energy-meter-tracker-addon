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
| 2 | BL-75 — Settlement visibility: both frontiers, retry from the pill, user-accepted CAD | diagnostics · correctness | next | ⚠️ needs issue |
| 3 | BL-50 — Unify user-job mutual exclusion | architecture · correctness | v5.0.0 | ⚠️ needs issue |
| 4 | BL-33 — Remove the one-time legacy migrations | deprecation | v5.0.0 | ⚠️ needs issue |
| 5 | BL-61 — 4.5.7 settlement/chart cleanup follow-ups | tech-debt | v5.0.0 | ⚠️ needs issue |
| 6 | BL-63 — Two pricing models: collapse onto one (Principle 0) | architecture · first-principles | v5.0.0 | ⚠️ needs issue |
| 7 | BL-64 — Bill-parser fixtures + a pypdf bump gate | testing · tooling | next | ⚠️ needs issue |
| 8 | BL-60 — Usage Stats block inspector (pre/post-settlement) | diagnostics | proposed | ⚠️ needs issue |
| 9 | BL-74 — Retire the `needs_review` writes nothing can display | tech-debt | v5.0.0 | ⚠️ needs issue |
| 10 | BL-76 — Device grid attribution: replace the ranked draw with a proportional split | attribution · correctness | proposed | ⚠️ needs issue |

---

## Open backlog

#### BL-47 — Auto-run recorder device attribution across freshly gap-filled windows  ·  *attribution · medium*  ·  ⚠️ **needs issue**
*Surfaced during the BL-46 (4.5.4) device-delete review.* When an outage gap-fills the house total from the supplier API, the **synthetic** dispatch-EV split self-heals automatically (BL-39, every reconcile pass) — but a **physical** sub-meter (Zappi/Ohme CT-clamp, Fox/Indra battery, any recorder-backed device) has **no** automatic reconstruction. The API only returns the *house* total for the gap, so the device's share of that window folds entirely into house/Direct until the user **manually** runs Device History (BL-12) for the range. This is also the un-automated step the BL-46 heal-via-reimport story leans on: "delete the data and re-import the gap" only refills a physical device if you then hand-run the recorder attribution. **Fix:** after a gap-fill (and after an import/CSV backfill) that covers a window a recorder-backed sub-meter was recording in, offer to (or, opt-in, automatically) replay that device's recorder history across exactly the filled blocks — reusing the existing `run_attribution_job` path (grid-clipped, house-capped, reversible via the attribution back-out ledger). Bounded to recorder-backed **physical** sub-meters (never the synthetic dispatch EV, which BL-39 already heals); opt-in and idempotent; re-derives the parent remainder on completion (shares the BL-46 EV-aware recompute). Turns "gap-filled, now go remember to re-attribute each device" into a one-click (or automatic) heal. Depends on: BL-12 (recorder attribution, shipped), BL-46 (EV-aware parent recompute, shipped 4.5.4).

#### BL-75 — Settlement visibility: report both frontiers, retry from the pill, and let the user accept CAD  ·  *diagnostics · correctness*  ·  ⚠️ **needs issue**
*Every claim below was traced to an assignment or measured on a live database; the earlier drafts of this item asserted several things the code did not say.*

**The model.** A settled **cost** always carries a settled **kWh** — measured on prod, zero counter-examples three ways (no `measured_cost` row on a `imp_kwh_api IS NULL` block, none orphaned, no `rate_source='measured'` without `imp_kwh_api`). The converse does not hold: the two frontiers move independently, and cost lags kWh routinely (measured: min 0.8 days, mean 5.9, max 20.6 over 680 rows). The reconcile runs six-hourly and usually lands both together, but nothing guarantees it. **The invariant is accidental, not structural** — `upsert_measured_cost` has no foreign key and no guard; it holds only because its single caller's query requires `imp_kwh_api IS NOT NULL`. A second caller would break it silently. Make it structural, with a test.

**(a) The pill reports one frontier and names the other.** `#dcc-settle-note` renders `'N block(s) awaiting cost settlement'` from `count_unsettled_blocks(since_iso=_settlement_horizon_floor())` — which tests `imp_kwh_api IS NULL`, i.e. **kWh**, floored at 14 days, and `AND finalised_from_cad = 0`. So it names cost, counts kWh, and hides two populations. **Target:** report the true unsettled-kWh count (no horizon floor), and report unsettled **cost** as a second figure, excluding manually-set blocks (`rate_corrected = 1`). Both tariff-agnostic.

**(b) The unsettled-cost count does not exist, and one narrow gate blocks it.** Nothing in the codebase counts blocks lacking a settled cost. Of the three stages, only the **fetch** is tariff-bound: `measure_settled_dispatched_blocks` returns early unless the tariff code contains `IOG` or `INTELLI`, and its candidate query further requires `EXISTS (dispatch_slots … off_peak = 1 OR source = 'smart-charge-completed')`. The other two stages are already tariff-agnostic, contrary to an earlier reading of this item: `_measured_floor()` resolves `_kraken_current_agreement_from`, set unconditionally from the current **import agreement**'s `valid_from` for any tariff (the SMB wording in its docstring describes the account it was written against, not a gate), and the apply path gates on `_MEASURED_APPLY` plus an mpan only — the `_import_is_smb_capped()` checks nearby belong to separate SMB-era self-heals. **Target:** drop the IOG/INTELLI test and the dispatch `EXISTS` from the fetch, keep the agreement floor. The count is meaningless until the fetch widens — on a clock-banded or Agile account today it would read 100%, because nothing writes `measured` there.

**(c) A stuck cost slot retries forever, silently.** `measured_slots_missing(starts, mpan)[:_MEASURED_MAX_PER_PASS]` re-requests every slot with no cached row, on every pass, with no attempt counter and nothing surfaced. Measured on prod: **12 blocks** aged 21–60 days are dispatched, kWh-settled, inside the agreement floor, and have no `measured_cost` row — so they have been re-fetched every pass for weeks against the Octopus points allowance, invisibly. **Target:** per-slot attempt count and last-attempted stamp, with backoff and a terminal state meaning *the supplier will not price this*. That terminal state is also what tells the corrections tool it may offer itself.

**(d) The 14-day bound is narrower than it appears — but the stamp it drives is not.** `retry_settlement_for_unsettled(max_lookback_days=None)` is day-unbounded by default; only the daily automatic sweep passes `_SETTLEMENT_SWEEP_HORIZON_DAYS`, and the user endpoint (`POST /api/retry-settlement`) passes nothing and counts with no `since_iso`. So the **user's** hard retry already reaches arbitrarily far back. What the horizon does drive is `finalise_past_horizon_blocks(cutoff)`, which stamps `finalised_from_cad = 1` **automatically and silently** on any block still unsettled past 14 days. That stamp removes the block from `count_unsettled_blocks()` — and therefore from the pill, and from the `if before == 0: return` guard that lets a retry start — while **not** removing it from `get_oldest_unsettled_block_start()`, so it is still re-fetched if a retry runs for another reason. Measured on prod: **zero** blocks carry the stamp, so this has never actually fired.

**Why the horizon itself is defensible, and what is missing.** DCC should always supply usage, so an unsettled kWh means a HAN fault or patchy coverage — and the recovery path is out of EMT's hands: the user reports it, the supplier asks DCC to re-read the meter (registers persist on the meter for long periods, so the data is usually recoverable), and cost settles later on those reads. That timeline is indeterminate, so refusing to chase forever is right, and pricing the block from CAD meanwhile is a reasonable stand-in. **What is wrong is that EMT makes the give-up decision for the user, silently, on a timer, and never tells them a block exists that a phone call could recover.** A CAD-priced block is billed on a non-authoritative usage figure — on Agile the rate is right but the kWh still is not — so this is a correctness statement the user should own. **Target:** stop stamping `finalised_from_cad` automatically. Surface the past-horizon unsettled population as an explicit list, offer a **hard retry** over a selected range, and stamp `finalised_from_cad` only on **explicit user acceptance** that the usage is unrecoverable. The pill should be honest that such blocks exist rather than counting them to zero.

**(e) `rate_corrected` forecloses cost settlement permanently.** Set only by the corrections apply (`web/server.py` ~9041) and **cleared nowhere**, it excludes the block from the cost fetch (engine 11885), the measured passes (12163, 12252) and apply (12547 — *"so a manual correction is never stomped"*). So a manual correction locks the supplier's bill out of that block for good. **Target:** the corrections tool is the last resort, not the first — offered once (c)'s terminal state is reached or the block is past acceptance, and warning at the point of use that it forecloses any future bill.

**(f) Retry is reachable from one page only.** `POST /api/retry-settlement` exists and regenerates charts on success, but its only caller in any template is `corrections.html` (~613). **Target:** a context menu on the pill offering **Retry DCC settlement** and **Retry cost settlement** against the same endpoint(s), since the pill is where the user learns there is something to retry.

**Out of scope.** The import reprice path (`repair_import_pricing`, `reprice_imported_block`, the reprice queue) is a different mechanism for a different problem — Octopus returns empty `statistics` under bulk-fetch load, so a targeted re-fetch recovers the real cost. It is scoped `source LIKE 'imported%'`, is rate-limit aware, and is not user-driven rate authority. Nothing here changes it.

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

#### BL-74 — Retire the `needs_review` writes nothing can display  ·  *tech-debt*  ·  ⚠️ **needs issue**
*Audit of what a review flag is for. Corrections-tool gating moved to BL-75, where it depends on a terminal retry state.*

**Two writers write nowhere.** `compute_channel`'s rogue-total (engine 1849) and rogue-sub (1926) clamps put `needs_review: True` in the **channel** dict; `append_block_replace` reads only `meter_block.get("needs_review")` (block_store 695) and nothing propagates channel → meter_block. Those flags reach no column. Delete them, or propagate them — but they currently decorate a backstop against a four-figure phantom bill and do nothing.

**Nothing can raise a review on a cost-settled block, by construction.** `_apply_pass2`'s #307 clamp (2456–2457) and both integrity sweeps are scoped to sub-meter rows, which carry no `rate_source`; the reconcile's candidate query excludes `rate_source IN ('measured','corrected')`; and `apply_measured_to_block` clears the flag outright. **The one exception is a defect:** `classify_kraken_block` has no `rate_source` guard, `_figure_changed` gates only `rerun`, and `new_review` ORs onto the stored value — so a cost-settled block inside the rolling backfill window whose CAD figure materially disagrees with DCC is re-flagged on **every poll** after settlement clears it, then keeps the last flag once the window moves on. It is also information-free: on an API-sourced block the DCC figure is authoritative by definition. **Fix:** guard on cost-settled, and stop re-ORing a recomputed flag.

**The integrity flags are already inert.** All five one-off repairs are marker-gated and self-marking (`run_smb_device_recost` 4.5.7, `run_smb_ev_resplit` 4.5.9, `run_smb_device_cost_clip` 4.5.15, and the two startup sweeps), so on any database that has started once the sweeps never flag again — their own comments say why: *"the write-point guard stops NEW ones"*. The only ongoing glitch flagger is that write-point guard, which writes `needs_review` on a **sub-meter** row with **no reason** — invisible to the corrections list (which filters `review_reason IS NOT NULL`) and to the drift list, which is dead code (below). **Fix:** drop the `needs_review` write from the sweeps and the clamp, with `AUTO_CORRECTION_REASONS` and the exclusion that exists only to hide them. Keep every clamp and every `logger.warning` — the repair is the value, and the log already carries the information. Nothing displayable is lost.

**The drift surface has no caller.** `get_drift_alerts` / `dismiss_drift_alerts` — the only readers that can see or clear a null-reason drift flag — are referenced nowhere outside `tests/test_block_store.py` and `tests/test_kraken_schema.py`. No route, no template. **Fix:** retire both with their tests.

**`dismiss_review_blocks` is wider than the list that feeds it.** Its docstring claims scoping to dispatch-origin flags; the predicate is only `review_reason IS NOT NULL`. That does protect the null-reason drift flags as claimed, but **not** the sweeps, which store reasons and which `get_review_blocks` deliberately excludes — so "Dismiss all" clears, and sets `review_dismissed = 1` on, flags that were never displayed. **Fix:** reuse the `get_review_blocks` predicate verbatim in both dismiss branches.

**Net effect.** `needs_review` ends with one writer, one reason and one action: a dispatch-reconcile ambiguity on a block whose band nothing else can decide. No flag has ever moved a billing figure, and the partial index `ON blocks (needs_review) WHERE needs_review = 1` keeps the dormant rows cheap — the cost is three unrelated meanings in one column, one of which cannot be reviewed.

#### BL-76 — Device grid attribution: replace the ranked draw with a proportional split  ·  *attribution · correctness*  ·  ⚠️ **needs issue**

*Surfaced from #473. The bill defect is fixed — device cost is now bounded to `imp_kwh_grid` and the
day total follows the meter again. What it exposed is the rule underneath: how a block's grid
import is apportioned **between** devices.*

**The rule today.** `_apply_pass2` orders sub-meters **EV first, then by descending recorded kWh**,
and walks them taking `claimed = min(entry["kwh"], grid_remaining)` from a pool that shrinks as it
goes. First in the queue takes everything it asked for; whatever is left trickles down; the tail can
get nothing.

**Why that matters more than it looks.** The ordering only decides anything when the devices
collectively recorded more than the main's grid import — but that is not a rare case. Measured on a
user database: **7,378 of 12,998 blocks (57%)** are contested, and on those each device received
roughly a sixth of what it claimed. So on more than half of all blocks the split is being decided by
the sort order, and for everything below the EV that order is **raw size** — which says nothing
about how much of that device's draw was actually grid-sourced. A big device is not a more likely
grid consumer than a small one; it is just bigger.

**The cliff.** Because the draw is greedy rather than shared, the outcome is discontinuous. Two
devices running together with a short pool do not each get a reduced share — the larger one is
satisfied first and the smaller one absorbs the entire shortfall. A device can read zero grid on a
block where it genuinely drew from the grid, purely because something bigger was running at the same
time.

**Fix: proportional allocation of the post-EV remainder.** Each remaining device gets
`grid_remaining x (its kwh / Σ kwh)`. That is:

* **bounded** — the ratio is ≤ 1 whenever `Σ kwh ≥ grid_remaining`, so a device can never be
  attributed more grid than it actually drew, which a flat even split would not guarantee;
* **order-independent** — the sort stops deciding outcomes, and the meaningless size tiebreak goes
  away with it;
* **smooth** — a shortfall is shared in proportion to draw instead of landing entirely on whoever
  sorted last.

The real per-device consumption is never lost (`imp_kwh` is always the recorded figure), so the
split is free to be reframed; only the derived `imp_kwh_grid` changes.

**Keep the EV pre-allocation.** EV-first is not an artefact of the size sort, it is a deliberate
correction: on IOG the supplier is deliberately pulling cheap grid for the car, so the car's import
must land on the grid rather than being squeezed out. The comment records what happened without it —
*"the old order (biggest draw first) handed the whole grid pool to a simultaneously-charging battery
and labelled the car's grid charge as battery-sourced, so the car vanished"*. Proportional applies to
what remains **after** the EV has claimed, not instead of that rule.

**Same code, delete while you are there: the `unprotected` branch is dead.** `protected` and
`unprotected` are both built, but the only `append` is to `protected` — *"all sub-meters are
protected (inverter_possible removed)"*. `unprotected` is sorted (2466) and iterated (2523–2550) and
is always empty. Leaving it implies two classes of device where the code has one.

**And `kwh_battery` is computed and discarded.** The allocation sets
`sub_import["kwh_battery"] = entry["kwh"] - claimed` on every block, and there is **no
`imp_kwh_battery` column** — `blocks` carries `imp_kwh`, `imp_kwh_grid`, `imp_kwh_remainder`,
`imp_kwh_ev` and nothing else. It is the non-grid share, which is the natural output of a
proportional split and the figure that would answer "how much of this device ran off the roof".
Persist it or stop deriving it.

**History can be healed, and the machinery already exists.** The re-split needs only what is
already stored — each sub-meter's recorded `imp_kwh` and the parent's grid import — so it is
computable from the database with no recorder fetch and no supplier call. Three things make it
tractable:

* **It is bill-neutral.** Measured across all 12,998 blocks of a user database, the greedy draw
  allocates *exactly* `min(Σ recorded, grid)` on **100%** of them — the pool is always fully
  consumed. Proportional allocates the same total by construction, so Σ device cost is unchanged
  and no day-level total moves. Energy and cost shift **between** devices only.
* **The reconstruct-and-re-run pattern is already built.** `recompute_remainders_for_window`
  reconstructs a parent block and calls `_apply_pass2` directly (deliberately *not*
  `_rerun_pass2_for_settled_block`, so it cannot disturb DCC/CAD materialisation, the dispatch
  overlay or `is_provisional`). A heal is that generalised from a window to the whole history.
* **The recorder attribution path picks the new rule up for free.** `_write_device_into_block`
  ends with `_apply_pass2(block)` → `_recompute_block_carbon(block)` → `append_block_replace(block)`,
  so any block that utility touches is re-allocated under whatever rule is current. Change the rule
  and that path conforms without modification.

Note the recorder utility solves a *different* split and is not affected by this item: it
distributes an hour of recorder energy across that hour's blocks in proportion to the **house import
shape** (`_split_hour_to_blocks`), which is temporal, not inter-device. Worth noting only as
precedent — that split is already proportional rather than ranked. It also never overwrites a live
reading, so a heal and the utility cannot fight over the same block.

Being a heal, it raises the v5 import floor (see BL-74 and the §6 floor note) even though no bill
changes.

**Scope.** Nothing here changes the bill. #473 already bounded device cost to what the grid
supplied, and the day-level total follows the meter. This changes only how that grid energy is
divided between devices, which affects Usage, Insights and per-device carbon.

**Not in scope, recorded so it is not re-litigated:** a device sensor reporting energy the grid never
supplied is a *sensor* problem, not an allocation one, and it cannot be detected reliably from the
data — the signal only appears when there is generation to expose it, so a quiet winter looks
identical to a compliant sensor. Proportional allocation does not fix a bad sensor; it stops one
device's error from silently consuming another device's share.

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
- [ ] **BL-75** — Settlement visibility: both frontiers, retry from the pill, user-accepted CAD
- [ ] **BL-74** — Retire the `needs_review` writes nothing can display
- [ ] **BL-76** — Device grid attribution: replace the ranked draw with a proportional split

*Convention: reference issues as `[#nnn]` on the item heading, with the link definition at the foot
of the file (as the archive does).*
