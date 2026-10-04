# Roadmap — Archive

*Ordered record of **shipped, closed, superseded and moved** work. Nothing here is planned or in progress —
the live backlog is [ROADMAP.md](ROADMAP.md). Backlog (BL) items are listed first, newest resolution
first; the release history follows, newest first.*

## Moved to EMT — 4 Oct 2026

*v4 accepts critical fixes only, so its open backlog is v5 work. Each item below moved to EMT's
roadmap ([RGx01/EMT `docs/ROADMAP.md` §F](https://github.com/RGx01/EMT/blob/dev/docs/ROADMAP.md)),
assessed against v5's code, keeping its BL number. BL-50 is EMT's workstream B. The entries are kept
here verbatim as the record of what each item said and why.*

### BL-47 — Auto-run recorder device attribution across freshly gap-filled windows  ·  *attribution · medium*  ·  ⚠️ **needs issue**
*Surfaced during the BL-46 (4.5.4) device-delete review.* When an outage gap-fills the house total from the supplier API, the **synthetic** dispatch-EV split self-heals automatically (BL-39, every reconcile pass) — but a **physical** sub-meter (Zappi/Ohme CT-clamp, Fox/Indra battery, any recorder-backed device) has **no** automatic reconstruction. The API only returns the *house* total for the gap, so the device's share of that window folds entirely into house/Direct until the user **manually** runs Device History (BL-12) for the range. This is also the un-automated step the BL-46 heal-via-reimport story leans on: "delete the data and re-import the gap" only refills a physical device if you then hand-run the recorder attribution. **Fix:** after a gap-fill (and after an import/CSV backfill) that covers a window a recorder-backed sub-meter was recording in, offer to (or, opt-in, automatically) replay that device's recorder history across exactly the filled blocks — reusing the existing `run_attribution_job` path (grid-clipped, house-capped, reversible via the attribution back-out ledger). Bounded to recorder-backed **physical** sub-meters (never the synthetic dispatch EV, which BL-39 already heals); opt-in and idempotent; re-derives the parent remainder on completion (shares the BL-46 EV-aware recompute). Turns "gap-filled, now go remember to re-attribute each device" into a one-click (or automatic) heal. Depends on: BL-12 (recorder attribution, shipped), BL-46 (EV-aware parent recompute, shipped 4.5.4).

### BL-75 — Settlement visibility: report both frontiers, retry from the pill, and let the user accept CAD  ·  *diagnostics · correctness*  ·  ⚠️ **needs issue**
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

### BL-50 — Unify user-job mutual exclusion (deny + idempotent-retry)  ·  *architecture · correctness · target v5.0.0*  ·  ⚠️ **needs issue**
*Surfaced across the 4.5.4 device-delete / gap-fill / attribution work — a recurring race class we kept fixing pairwise.* EMT's bounded DB-mutating jobs (import / **gap fill**, **device delete** + recompute, device-history attribution, attribution backout, delete-blocks, manual/CSV reprice, DB restore) plus the automatic batch tails (post-import **recovery + verify**, the reprice-history migration sweep, historical carbon backfill) all write one shared SQLite store. Overlapping them races (the `float * NoneType` attribution-over-unverified-blocks crash; the delete-drains-import / attribution-waits-for-verify guards). Today we guard **pairwise** — O(N²), and we only find a missing pair when a user hits it. **Fix:** one coordinator enforcing a single invariant — *at most one bounded mutating job at a time; a new user job is refused (`409 + running`), not queued*. Because every such job is **idempotent**, a denied user just retries when the running one finishes and lands correctly (idempotency buys the retry, not concurrency — so no queue needed). The lock is held across a job's **whole pipeline including its verify tail**, which is the single change that closes the race class. The live engine tick is orthogonal (continuous, handled by the loop-lock / `pause_engine` layer, not this coordinator). Retires the interim pairwise guards (`_verify_running`, delete's import-drain, refuse-reprice-during-migration, scattered `delete_in_progress` checks) and lets the run-lock UI read one `current_job()`. Targeted at **v5.0.0** alongside the BL-27 aggregation-unify — both remove a recurring drift/race bug class at the root under one migration-gated release. Design: `docs/design/BL-50_user_job_mutual_exclusion_design.md`.

### BL-33 — Remove the one-time legacy migrations  ·  *deprecation · target v5.0.0*  ·  ⚠️ **needs issue**
*Announced with 4.4.0; the removal itself is still outstanding.* 4.4.0 shipped one-time background
migrations that bring a pre-4.4.0 database up to the priced-segment model (segment backfill, the
capped-IOG pence→£ repair, canonical-rate re-stamp). From **v5.0.0** those migrations are removed, so
a database below 4.4.0 must be upgraded **through** 4.4.x first. Work: delete the migration paths and
their gates, drop the upgrade-sweep banner wiring that exists only to report them, state the minimum
supported upgrade-from version in the README, and fail clearly (rather than silently mispricing) if a
pre-4.4.0 schema is opened. Pairs with BL-61's `rate_source` rename and BL-50's job coordinator — all
three want the same migration-gated release.

### BL-61 — 4.5.7 settlement/chart cleanup follow-ups  ·  *tech-debt · target v5.0.0*  ·  ⚠️ **needs issue**
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

### BL-63 — Two pricing models: decide which one is the model, and collapse onto it  ·  *architecture · first-principles · target v5.0.0*  ·  ⚠️ **needs issue**

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

### BL-64 — Bill-parser fixtures from real bills, and a pypdf bump gate  ·  *testing · tooling · target next*  ·  ⚠️ **needs issue**

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

### BL-60 — Usage Stats block inspector: pre/post-settlement detail for a single block  ·  *diagnostics · proposed*  ·  ⚠️ **needs issue**
*Turns the manual forensic we keep repeating into a self-serve drill-down.* Diagnosing a single
half-hour today means hand-running SQL across `blocks`, `block_segments`, `measured_cost` and
`dispatch_history`, plus live Measurements probes — exactly the process used on the 2026-08-14 19:00
and 2026-09-11 07:00 investigations. **Proposal:** click a block in Usage Stats and see its full
provenance in one panel — what EMT priced it and from which authority (`rate_source`), what Octopus
billed it (settled cost, band label, the four-bucket split), the dispatch lifecycle behind it
(planned / started / completed), and how it changed from provisional → settled. Read-only; no pricing
impact. Would have made the 07:00 mis-band self-evident instead of a multi-hour trace. Design:
`docs/design/EMT-BL60_block_inspector_pre_post_settlement.md`.

### BL-74 — Retire the `needs_review` writes nothing can display  ·  *tech-debt*  ·  ⚠️ **needs issue**
*Audit of what a review flag is for. Corrections-tool gating moved to BL-75, where it depends on a terminal retry state.*

**Two writers write nowhere.** `compute_channel`'s rogue-total (engine 1849) and rogue-sub (1926) clamps put `needs_review: True` in the **channel** dict; `append_block_replace` reads only `meter_block.get("needs_review")` (block_store 695) and nothing propagates channel → meter_block. Those flags reach no column. Delete them, or propagate them — but they currently decorate a backstop against a four-figure phantom bill and do nothing.

**Nothing can raise a review on a cost-settled block, by construction.** `_apply_pass2`'s #307 clamp (2456–2457) and both integrity sweeps are scoped to sub-meter rows, which carry no `rate_source`; the reconcile's candidate query excludes `rate_source IN ('measured','corrected')`; and `apply_measured_to_block` clears the flag outright. **The one exception is a defect:** `classify_kraken_block` has no `rate_source` guard, `_figure_changed` gates only `rerun`, and `new_review` ORs onto the stored value — so a cost-settled block inside the rolling backfill window whose CAD figure materially disagrees with DCC is re-flagged on **every poll** after settlement clears it, then keeps the last flag once the window moves on. It is also information-free: on an API-sourced block the DCC figure is authoritative by definition. **Fix:** guard on cost-settled, and stop re-ORing a recomputed flag.

**The integrity flags are already inert.** All five one-off repairs are marker-gated and self-marking (`run_smb_device_recost` 4.5.7, `run_smb_ev_resplit` 4.5.9, `run_smb_device_cost_clip` 4.5.15, and the two startup sweeps), so on any database that has started once the sweeps never flag again — their own comments say why: *"the write-point guard stops NEW ones"*. The only ongoing glitch flagger is that write-point guard, which writes `needs_review` on a **sub-meter** row with **no reason** — invisible to the corrections list (which filters `review_reason IS NOT NULL`) and to the drift list, which is dead code (below). **Fix:** drop the `needs_review` write from the sweeps and the clamp, with `AUTO_CORRECTION_REASONS` and the exclusion that exists only to hide them. Keep every clamp and every `logger.warning` — the repair is the value, and the log already carries the information. Nothing displayable is lost.

**The drift surface has no caller.** `get_drift_alerts` / `dismiss_drift_alerts` — the only readers that can see or clear a null-reason drift flag — are referenced nowhere outside `tests/test_block_store.py` and `tests/test_kraken_schema.py`. No route, no template. **Fix:** retire both with their tests.

**`dismiss_review_blocks` is wider than the list that feeds it.** Its docstring claims scoping to dispatch-origin flags; the predicate is only `review_reason IS NOT NULL`. That does protect the null-reason drift flags as claimed, but **not** the sweeps, which store reasons and which `get_review_blocks` deliberately excludes — so "Dismiss all" clears, and sets `review_dismissed = 1` on, flags that were never displayed. **Fix:** reuse the `get_review_blocks` predicate verbatim in both dismiss branches.

**Net effect.** `needs_review` ends with one writer, one reason and one action: a dispatch-reconcile ambiguity on a block whose band nothing else can decide. No flag has ever moved a billing figure, and the partial index `ON blocks (needs_review) WHERE needs_review = 1` keeps the dormant rows cheap — the cost is three unrelated meanings in one column, one of which cannot be reviewed.

### BL-76 — Device grid attribution: replace the ranked draw with a proportional split  ·  *attribution · correctness*  ·  ⚠️ **needs issue**

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

## Shipped / closed backlog items

### BL-72 — 4.5.14 — a database records the version that last opened it

*Closed 19 Sep 2026. Scoped down drastically on the way: the item as written specified a computed
"heal level"; what shipped is one key.*

**The flaw.** Nothing in a database said what produced it. `store_meta.schema_version` reads `1` on
a 4.3.1 and a 4.5.14 alike, and the version recorded at upgrade goes to a file in `DATA_DIR` —
which `_backup_to_share` does not copy, it takes `blocks.db` and `meters_config.json` only. So a
backup, or a `blocks.db` handed to anything, arrived anonymous. EMT (v5) needs to know, because v5
carries none of v4's one-time heals and must refuse history they have not run against.

**The fix, as shipped.** `stamp_version()` writes the running version to `store_meta.written_by` at
startup, beside the existing `db_uuid` lineage stamp and BL-73's supplier stamp. It does not write
when the version cannot be read, leaving the previous stamp — the last thing known to be true —
rather than overwriting it with `unknown`. Covered by `tests/test_bl72_version_stamp.py`.

**Why a version is enough, and the mechanism that was dropped.** The item originally specified an
integer `heal_level`, computed by each heal re-verifying its own invariant at startup, plus a
migration and a downgrade guard. That was dropped as unjustified. **Every heal is marker-gated, and
an ungated marker runs at startup** — so upgrading any older database runs every heal the release
carries: a 4.4.0 database has no markers at all and runs all of them; a restored 4.5.12 backup
carries 4.5.12's markers, so a heal added in .13 is ungated and runs while the older ones correctly
do not. *"Last opened by version X" therefore means "healed to X's standard"*, and a reader gates on
a floor version. Heals are historical: if a later release adds one, the floor simply rises.

The specification also did not survive contact with the code. It described "eight one-off heals
marker-gated in `store_meta`"; there are more than eight, they live mostly in `kraken_state`, and
one of the eight (`pre_live_snapshot_done`) takes a **backup** — it has no data invariant to verify,
so the central mechanism could not have been implemented as written.

### BL-73 — 4.5.14 — a database records which supplier its history belongs to

*Raised and closed 19 Sep 2026, alongside BL-72 and through the same code path.*

**The flaw.** v4 has had a supplier registry since the setup wizard shipped —
`_API_CAPABLE_SUPPLIERS = frozenset({"octopus"})` with `normalize_supplier()`, and a dropdown
offering `octopus` and `not-listed` ("My supplier isn't listed / local metering only"). What it
never did is write the *answer* down in one place. `config_periods.supplier` is the display and
historical record and holds either kind of value: a registry key on an install set up through the
wizard, free text on one that predates it (`'Octopus Energy'` — which is what every real database
to hand contains). Any later reader would have to reimplement the mapping against a mixed column.

**The fix, as shipped.** `stamp_supplier()` writes the normalised key to `store_meta.supplier` at
startup, refreshed on every config save so a supplier change follows. Three states are kept
distinct: `octopus`, `not-listed`, and **absent** — a configuration predating the field, where
nothing is written rather than a placeholder that would read as an answer. Covered by
`tests/test_bl73_supplier_stamp.py`.

**Scoped down twice.** The first draft proposed two keys. `account_ref` was dropped: the account is
already stamped at `kraken_state.kraken_account_number`, with `get_db_account()` and
`kraken_account_mismatch()` already implementing the match guard. The supplier key was then argued
to be optional, on the grounds that "v4 is frozen, so every v4 database is Octopus" — **false**, and
the reason the item shipped: `not-listed` is a supported local-metering state with real CAD-read kWh
history and no Octopus account at any point, so it cannot be inferred from the version.

### BL-71 — 4.5.14 — a `started` dispatch is adjudicated by `completed`, in both directions

*Closed 19 Sep 2026. Surfaced by the 17 Sep case where prod and prod-dev priced the same half-hour
at 32.3p and 5.5p.*

**The flaw.** `_reconcile_decision` tested `has_started` first and returned `off_peak`
unconditionally, so a slot that looked started could never be re-priced — the settle window governed
only the other branches. But **`started` is not returned by the Octopus API**: it is derived from
`SMART_CONTROL_IN_PROGRESS` observed while a planned dispatch is active, at the poll cadence. (The
reference implementation says so on the field itself: *"Bit of a smell this being on the API client
object as it's never returned from the API"*.) It is therefore a sample, not a record, and had a
measurable false-positive rate being treated as permanently conclusive.

**Measured, three years of one account.** `started` is a far better finalise-time predictor than the
plan alone — 97.4% precision (406 of 417) against 61.3% for `planned` (446 of 727) — which is why it
stays as the gate. Its 11 false positives all had **zero charger draw**. Of the 98 completed slots
that never captured `started`, 63% are the **last** slot of a session at a median 0.25 kWh against
3.10 for slots that did capture it: the charge ended mid-slot, so the state was only briefly true.
Those are already handled — 91 of the 98 land off-peak, none in review — so the false negatives
needed no change.

**The fix, as shipped.** `started` keeps the fast restore, and gains the missing arm: past the
settle window, EMT online to have seen a confirmation, and no `completed` → fall through to the
existing revert, which re-derives from the tariff schedule rather than forcing peak. The caller
passes `past_settle` rather than deferring, because deferring would cost the 40-minute restore that
rescues a solar-supplied charge. Covered by `tests/test_bl71_started_revert.py`.

**Blast radius: 3 slots in three years, about 10p.** The other 8 started-without-completed slots sit
inside the off-peak window, where the base tariff is already the off-peak rate and the reconcile
skips outright — the price never depended on a dispatch. The value is consistency, not money: two
installs of one account had priced the same half-hour differently purely on which sampled the signal.

### BL-51 — 4.5.13 — the re-price banner names its own trigger

*Surfaced during 4.5.4 gap-fill testing; closed by the 4.5.13 fresh-install case, where a box that
had never run an earlier version was told EMT was "finishing your upgrade".*

**The flaw.** The unified re-price sweep has two triggers by design (P3.3d) — the first run after a
version change, and the forced pass following any import, gap-fill or delete-reimport — but the
backlog counter driving the banner recorded only that a sweep was running, never why. So every
non-upgrade sweep borrowed the upgrade's wording, and its advice to avoid restarting or rebuilding,
on an install doing precisely what it had just been asked to do.

**The fix, as specified.** `_reprice_sweep_cause` derives the trigger from the marker's own
timestamps, `/api/reprice-history-status` returns it as `cause`, and `base.html` branches on it:
`import` leads with "Finishing your import — EMT is pricing the history it just fetched", `upgrade`
keeps its existing wording, and anything unrecognised falls back to the upgrade text. Cosmetic only —
no pricing or data path was touched. Covered by `tests/test_reprice_banner_cause.py`.

### BL-62 — 4.5.13 — settlement re-run is scoped to the channel that settled

*Closed by the 13 Sep 2026 prod case: a historical slot repriced peak → off-peak roughly thirty
hours after it was written, on a restart, on a block whose import had never settled.*

**The flaw.** `needs_pass2_rerun` is a **block-level** flag. `upsert_kraken_block` picks
`imp_kwh_api` or `exp_kwh_api` to decide whether *that channel's* figure changed, then raises one
shared flag with no channel recorded; `_drain_pass2_queue` reloads the block and re-runs every
channel. An export-only settlement therefore re-ran the import channel against `chosen = cad_kwh`
— the kWh already stored. Nothing to re-cost, but the re-run still re-resolved the dispatch
overlay and re-ran the IOG split.

**Why it surfaced when it did.** The Kraken poll is six-hourly and a restart forces one
immediately, so the restart is what fetched the export settlement — not a code path that only
runs at boot. Export settled for all 48 slots of 13 Sep; import had not settled for any of them.

**Three quirks it exposed, and why none of them is work to schedule.** The investigation surfaced
three things that look like defects in isolation:

1. `_apply_iog_split` ignores `_DISPATCH_OVERLAY_MIN_KWH`. The over-report floor guards
   `_dispatch_overlay_rate` only, so the capped seam reprices a sub-floor dispatch slot even after
   the overlay has refused it and logged the refusal.
2. The capped seam writes a **blended** rate (£0.054917) rather than the canonical band rate
   (£0.05493). Reconcile's `abs(cur_rate - off_peak) < 1e-6` band test cannot see that as
   off-peak, so its out-of-app-bump revert silently never fires.
3. `_iog_slot_is_boost` matches `dispatch_history.source` against `{"bump-charge", "boost"}`, but
   Myenergi's completed rows carry `source='unknown'` — so a bump is invisible to the cap layer
   and "exclude boost energy from the 6 h cap tally" cannot fire for that provider.

All three only bite a **schedule-derived estimate that is never superseded by a bill**, and on the
evidence no such block exists:

* **The seam cannot reach an uncosted era.** `_apply_iog_split` returns at `ev_kwh <= 1e-9`, so it
  needs a completed dispatch row for that exact slot. Dispatch records are now kept
  **indefinitely** — the 90-day prune was retired in 4.5.0 (**BL-35**, below) so completed
  dispatches survive for later reconstruction, and `prune_dispatch_history` remains only as a dead
  method. But retention is not
  the constraint: coverage can only ever accumulate **forward** from the moment EMT began polling,
  because Octopus serves a short rolling dispatch window and no `planned`/`started` history at all,
  so a past era can never acquire records it did not have at the time. On prod-dev that leaves
  **54,143 of ~57,450 main blocks (94 %) predating every dispatch record**, including every
  imported and legacy-tariff era — a figure that only improves going forward, never regresses. The
  accounts where Octopus never publishes a cost (Brian's 2024–26 Intelligent stretch) are exactly
  those eras, so "cost never settles" and "the capped seam repriced it" cannot coexist on one
  block.
* **Where the seam can reach, the bill always lands.** Of settled blocks 1–12 Sep that drew
  anything, **257 of 257 (100 %)** carry `rate_source='measured'`; the remainder on `schedule` are
  zero-kWh slots with no cost to bill. `apply_measured_to_block` then overwrites rate and cost and
  stamps `measured`, which is in the preserve list permanently.
* **After this fix the window is a few minutes.** The re-price now requires `imp_kwh_api`, and
  import kWh and import cost share a settlement frontier (both `2026-09-12T23:30` in the
  snapshot). The drain runs a little ahead of the measured-cost pass within one tick (21:11:31 vs
  21:16:29 in the prod log), so an estimate can be written and corrected by the bill minutes
  later. Not worth code.

Recorded here so the reasoning survives, not as deferred work. Re-open only if a capped account
is ever observed holding dispatched slots that stay on `rate_source='schedule'` after settlement.

**Fix (shipped).** No settled figure for a channel ⇒ no re-resolve for that channel. The import
re-run keeps its finalised rate and re-costs only, joining the existing preserve list (user
correction, dispatch reconciliation, Octopus-billed). A missing or zero stored rate still falls
through to `_resolve_block_rate`, so gap-block rate repair is unaffected, and a genuine import
settlement re-resolves exactly as before. Guarded by `tests/test_export_settlement_scope.py`,
which reproduces the prod slot to six decimal places.

### 4.5.0 — IOG bump handling: don't promote completed-only dispatches to off-peak

*Closes the long-open bump-validation item (`dispatch_validation_design.md` §12/§13; `4.4.0_iog_pricing_and_reprice_design.md` §3c) with the **first real bump ever observed** — a manual Zappi **Fast** bump during a Free Electricity hour, 23 Aug 2026 prod. This is a **framing/correction of the completed-dispatch classification model**, not new provider support.*

**What the shipped model already does.** Smart-vs-bump is discriminated by **`started`** — a planned dispatch that went active under the account's `SMART_CONTROL_IN_PROGRESS` state — **not** by the meter and **not** by `completed`. A bump *charges and completes identically to a smart charge*, so `completed` can't tell them apart; but a **bump never enters `SMART_CONTROL_IN_PROGRESS`, so it never `starts`**. The validated rule (3.1.4, §12): *off-peak iff the slot started.* Started-capture is working — the prod bump has `planned=0, started=0, completed=1`; the same night's real smart charges have all three.

**The defect (confirmed in prod).** The settlement reconcile has a **"completed-only → promote off-peak"** branch (§3c) meant to rescue a genuine smart charge whose *plan* EMT missed during downtime (the accumulation-gap case). But a **bump is also completed-only** (no plan, no started), so the branch promoted the bump to off-peak (`rate_reconciled=1`) — overriding the not-started signal that already had it right. The two bump slots carry `rate_reconciled=1`; the same night's `started` smart charge and the earlier *settled* completed-only orphans do not. First real bump to exercise the branch — proves the optimistic promotion unsafe.

**Fix (shipped).** Gate the completed-only branch on whether EMT was **online** for the slot — read straight off the block's own `interpolated` flag, no new signal needed. A **live** block (`interpolated=0`, not `imported%`) means EMT was up and polling, so an unplanned completed dispatch is an **out-of-app bump → peak** (freebie withheld, cap allowance untouched); an **offline / gap / imported** block keeps the optimistic **off-peak** (a genuinely missed smart charge). Implemented as `_reconcile_decision(..., was_online = not interpolated)`. The live-bump revert is **confident, not `review`** (a bump is unplanned by construction), and an already-peak live bump is a no-op — so no spurious review flags. `started` stays the positive off-peak signal; this only tightens the completed-only fallback. Fixes the Zappi case **and** the pure car-side EV-integration case (no sensor exists there). Full design: `dispatch_validation_design.md` §14 / §14a. Your "no plan ⇒ bump" instinct, expressed on the already-captured signals (`no started` + `was_online`).

**Estimate-only, settlement-backed.** Confirmed in prod: every **settled** completed-only orphan (14–22 Aug) is already priced **peak** (API-authoritative, `rate_reconciled=0`); only the **provisional** 23rd was promoted. So this corrects the *provisional* estimate — settlement is and stays the final authority.

**Also in 4.5.0 (additive, secondary):**

- **Charger/car mode-sensor corroboration** — Zappi Fast / Ohme Max / Hypervolt Boost as optional ground-truth *on top of* `started`, catching the residual bump-overlaps-a-plan case. For a **pure car-side EV integration** no such sensor can ever exist, so `started` + no-promote + settlement is the whole model there.
- **Hypervolt provider support** (**BL-31**) — additive provider coverage, not the headline.
- **Retention invariant** (**BL-35**, shipped enabler) — the planned/started/completed dispatch lifecycle is no longer pruned at 90 days; permanent retention is what makes "a genuine smart charge would have been planned, and we'd have caught it" a safe inference for the online-gate.
- **Band-label fix** (**BL-34**, low priority) — settled peak-priced segments still carry a stale `band='off_peak'` label.
- **Online-gated bump detection** (**BL-36**, shipped) — the `interpolated` gate that tells an out-of-app bump from a missed smart charge; and **dispatch-poll heartbeat** (**BL-37**, unplanned) — the residual meter-up/poller-down refinement, parked because Kraken is dependable and settlement backstops it.

Settlement remains the final authority throughout.


#### BL-15 — Historical rate correction from bill
*Low.* Populate missing or incorrect rate data from a supplier PDF — useful when the rate sensor was misconfigured for a period and block costs are wrong even though kWh figures are correct.



> **Closed — superseded (post‑4.5.2 review).** Rates are now API‑authoritative (Kraken tariff schedule) and self‑heal via the reprice sweep, so the 'misconfigured rate sensor' failure mode is largely designed out; and the bill‑correction capability already exists (`bill_parser.py` + the Corrections tool + the CSV/bill reprice path).
#### BL-20 — Auto-resolve ambiguous dispatch slots from the API's billed cost (fewer review prompts)  ·  *follow-up to #322*
*Medium — accuracy + UX.* The dispatch reconciliation decides a smart-charge slot's off-peak/peak **rate** from the *local dispatch lifecycle* (planned / started / completed signals). When a slot is `completed but not started` with substantial energy — typical of an **Ohme replan that didn't actually charge** — the signals are genuinely ambiguous, so EMT leaves the price unchanged and flags it for the user to check against the bill. #322 made those flags **dismissible-for-good**; this item reduces how many are raised at all. The review flag is already gated on the block being **DCC-settled** (`imp_kwh_api` present), which means Octopus has an authoritative **billed cost** for that exact half-hour — and that cost already encodes whether IOG charged it off-peak. So: for a settled slot the heuristic can't decide, **fetch the real billed cost via the Measurements cost-recovery path** (`recover_measurement_costs`, already built) and set the rate from it automatically; only fall back to a human `review` flag when the API genuinely can't answer (cost still missing after retry). Turns "here are 9 blocks, check them against your bill" into "priced from your actual bill" for the common case. Keep the local heuristic as the fast pre-settlement path (dispatch `completed` lands hours before DCC settlement) — this only changes what happens at settlement time for the ambiguous ones. Validate that the billed-cost-derived rate matches settled billing (the reconcile must not change billing totals).



> **Closed — substantively delivered (post‑4.5.2 review).** The billed‑cost reconciliation is built and auto‑runs: the verify‑pricing sweep (`repair_import_pricing(suspect_only=True)` → `_reprice_suspect` → `_billed_rate`) re‑prices peak‑band material dispatch slots from Octopus Measurements billed cost (`kraken_state` shows it running). Narrow residual only: the dispatch `needs_review` flag isn't cleared when a slot is verify‑repriced, and the sweep re‑checks peak‑band (over‑charge) slots only — raise a small fresh item if wanted.
#### BL-32 — Charts/Bill freshness token must catch a cost-neutral re-price  ·  *UI · low risk*  ·  **SHIPPED (4.4.0)**
*Fixed: `_blocks_data_version` now appends a `block_segments` rate fingerprint (COUNT + ROUND(SUM(inc_rate),4) + ROUND(SUM(exc_rate),4), guarded for pre-4.4.0 DBs), so any rate-only re-price busts the Charts/Bill cache on the next poll/tab-focus. Verified the fingerprint moves between a scattered and a canonical (re-migrated) DB.*
*Surfaced during 4.4.0 re-migration testing (M1/B6).* The Charts UI gates its auto-refresh on `_blocks_data_version` — `COUNT + MAX(block_start) + SUM(imp_kwh/imp_cost/exp_kwh/exp_cost/carbon_g)` plus the mtimes of `daily_usage.html`/`net_heatmap.html`. A **rate-only re-price** (the first-upgrade migration, or the M1/B6 canonical-rate work) changes segment/displayed **rates** but leaves `imp_cost`/`imp_kwh` byte-identical (the reconciliation invariant), so the DB fingerprint does **not** move; only a chart regen advancing the two mtimes bumps the token, which is incidental and doesn't cover the bill-summary breakdown reliably. Result: after a migration the Bill Summary keeps its cached render until a manual browser refresh (a finalise, which moves cost, refreshes normally). **Fix:** make the token capture a rate/segment change too — cheapest is a `block_segments` rate fingerprint (e.g. `SUM(ROUND(inc_rate,6)*seq)` or a rowid/updated-at max), or have the reprice sweep bump a persistent `reprice_generation` counter the token reads; then any cost-neutral re-price busts the cache and the Charts + Usage-Stats surfaces refresh on the next poll/tab-focus without a hard refresh. Also audit whether the **billing-history** page (no `data-version` poll at all today) should adopt the same gate. Display/UX only — no figure changes.


#### BL-35 — Retain the dispatch lifecycle permanently (remove the 90-day prune)  ·  *data integrity · SHIPPED (4.5.0)*
*The planned/started/completed dispatch history is the canonical ingredient for any future re-price (smart-vs-bump, cap reconstruction) — and unlike Octopus's rolling ~90-day window, EMT must keep it for the life of the DB.* 4.4.0 shipped with `prune_dispatch_slots` / `prune_dispatch_history` deleting both tables at 90 days (Octopus's own amnesia, replicated). Removed the scheduled prune calls from the `engine.py` capture ticks so the lifecycle is retained forever; the `prune_*` defs remain unused (test-covered). Storage is negligible (~thousands of rows/yr on a 70 MB+ DB). Underpins BL-9's cap, BL-28's deep-history reconstruction, and the 4.5.0 online-bump gate. Design: `4.4.0_iog_pricing_and_reprice_design.md` §3d.


#### BL-36 — Online-gated bump detection: `interpolated` distinguishes an out-of-app bump from a missed smart charge  ·  **SHIPPED (4.5.0)**
*The 4.5.0 core.* The settlement reconcile could not tell an out-of-app **bump** (a completed dispatch with no plan/started, billed peak by Octopus) from a genuine smart charge whose plan EMT missed while offline — both are completed-only, and Octopus leaves `source` null. Resolved by reading the block's own **`interpolated`** flag as the "was EMT online?" signal: a **live** block (`interpolated=0`, not imported) means EMT was polling, so an unplanned completed dispatch is a **bump → peak** (freebie withheld, cap untouched); an offline/gap/imported block keeps the optimistic off-peak. Implemented as `_reconcile_decision(..., was_online = not interpolated)`. Fixes the observed Zappi Fast case and the pure car-side EV-integration case (no charge-mode sensor needed). Design: `dispatch_validation_design.md` §14 / §14a.


#### BL-38 — Generation-mix / carbon-intensity plausibility guard + donut↔chart alignment  ·  *display + carbon · SHIPPED (4.5.0)*
*Surfaced during the 4.5.0 soak.* National Grid's **regional** generation mix is a modelled estimate that occasionally emits a degenerate half-hour — one fuel ~100% with a 0 gCO₂/kWh intensity (e.g. "solar 97%" at 21:30) — which EMT stored verbatim, corrupting the **Current Grid Generation Mix** donut, the 48-hour chart, and that block's **carbon** (intensity → block `carbon_g`). Fixed: `_ci_slot_plausible` rejects a glitch slot on fetch (one fuel ≥95% or intensity ≤0) so the previous good slot stands, and the donut now reads the **same `mix_history` latest slot as the chart** (was the lagging block-stamped `generation_mix` with a fragile `LIMIT 9`), so the two can't disagree. Forward-only — no backfill (a re-fetch returns the same upstream glitch; existing bad slots age out of the rolling 48h/4-day stores). Parser unit-tested (`test_ci_plausibility.py`). Display / carbon-view only — no bill effect.


#### BL-39 — Gap-fill EV/house split heals from the late completed dispatch  ·  *attribution · SHIPPED (4.5.0)*
*Surfaced during the 4.5.0 soak — dev was offline over an evening charge while prod_dev (live) was correct on the same code.* An outage gap-fills the missed blocks from the API, and the EV share could be stamped from an incomplete/planned fragment (e.g. `imp_kwh_ev=0.146` when the completed dispatch shows 1.61 kWh) and then **never corrected**, because the back-attribution only healed a **NULL** split — so the charge showed as house/Direct, not EV. `_attribute_missing_ev_split` now also re-attributes a **gap-filled** (`interpolated=1`) block whose EV kWh **materially disagrees** (>0.1 kWh) with its completed dispatch, re-deriving the grid-clipped split + segments. Runs every reconcile pass, so an outage **self-heals** once the retained (BL-35) completed dispatch lands; idempotent; interpolated-only, `rate_corrected`-safe, uncapped. The interim EV-column re-stamp (BL-27/segments drift) is retired when the 5.0.0 aggregation-unify ships.


#### BL-40 — EV device typed 'ev' (config UI) not recognised by the hybrid EV gate → double EV line  ·  *attribution · SHIPPED (4.5.1)*
*Reported by an Indra Smart Pro user — the charger added as an "EV Charger" device **and** the Octopus dispatch provider, so it double-counted against the synthetic "EV (from dispatch)".* Root: the config UI writes `meter_type='ev'`, but the physical-EV identity gates (`_ev_meter_id` in `energy_charts.py`; the coverage gate + insights-carbon gate in `server.py`) hard-checked `== 'ev_charger'` with only a meter-id 'ev'/'charger' **substring** fallback. A UI-added device has a hashed `sub_meter_<id>` (no substring) + type `ev` → matched neither → never fed to `_hybrid_ev_by_block` → the synthetic couldn't supersede it → **two EV lines**. Latent since the synthetic-EV/hybrid coverage gate landed (~4.1.x); masked because every prior test/dev device had a canonical id (e.g. Zappi `ev_charger`). Fix: gates accept `meter_type in ('ev','ev_charger')` — matching what the device-list, Usage-Stats and Insights code already did. Display/attribution only, no stored-data change. Future-proofs any Octopus-controlled charger added as a device (Hypervolt, Pod Point, VCHRGD, …).

---


#### BL-42 — API/Mini sub-meter device split skipped on the boundary-finalise path (`load_current_block` drops config meta)  ·  *display · SHIPPED (4.5.2)*
*Indra + Fox-battery user (Octopus Home Mini, `data_source_mode=api`).* `load_current_block` rebuilt the in-progress block's meters with empty meta, so `_apply_pass2` saw `sub_meter=False`/`parent_meter=None` and the device split no-op'd → `imp_kwh_remainder` NULL → Usage-Stats double-count on provisional days. Masked on CAD (`capture_samples` re-stamps meta) and settled blocks (`get_block_dict`); byte-identical 3.2.0→4.4.0 (not a release regression; latent since ≥3.2.0, surfaced on Mini reconnect). Fix: repopulate meta from `config_from_db`. Forward fix; history self-heals at settlement. Test: `tests/test_current_block_meta.py`.


#### BL-43 — First-time connect stalls the HA WebSocket building a large (Agile) rate schedule  ·  *setup robustness · SHIPPED (4.5.3)*
Agile ~34k half-hourly periods: `build_rate_schedule` built the schedule + O(n) diagnostic walk inline on the engine loop during a rate refresh / first-time connect → starved the HA WebSocket heartbeat → supervisor "No PONG received after 15s" → the connect request timed out → the wizard showed the generic "Could not connect: check key/account" (the backend connect had actually succeeded). Fix: offload the CPU-bound build + diag to a worker thread (`run_in_executor`); cap the per-period distinct/date-span diag for large (>2000-period) schedules. Reported by a new Agile user; couldn't reproduce on a fixed-tariff account (328 vs 34,078 periods). Tests: `tests/test_rate_schedule_offload.py`.

#### BL-44 — API-only account with no live source spins forever on block formation  ·  *robustness · SHIPPED (4.5.3)*
No live source (no Mini reads AND no local sensor) → a block never gets a post-boundary read, finalises "nothing to finalise", and the empty block never advanced the opener → `ensure_correct_block` re-rolled the SAME boundary every ~10s indefinitely. Fix: when a finalise leaves the opener unchanged, roll it forward to the current window so blocks advance one per boundary and DCC settlement backfills. Keyed on "opener unchanged" so gap catch-up is never skipped. Tests: `tests/test_ensure_block_advance.py`.

#### BL-45 — Generation-mix donut shows a forecast slot (MAX(captured_at) over forecast-bearing mix_history)  ·  *display · SHIPPED (4.5.3)*  ·  [#408]
`mix_history` holds fw48h forecast rows; the donut's current-mix query took `MAX(captured_at)` → a slot up to ~48h ahead (gas-heavy forecast night ~56% vs ~30% now), while the 48h chart showed the real current slot. Fix: donut selects the newest slot `<= now`; `get_mix_history` gains the same upper bound (belt-and-braces; the frontend already clipped). CO2 value + Insights unaffected (`get_nearest_carbon_intensity` / block-stamped). Regional-vs-national ruled out (same source); confirmed on the prod DB (the 56.4% donut = the `2026-08-28T00:00` forecast row).

#### BL-46 — Device delete leaves the parent's house/EV split stale  ·  *correctness · SHIPPED (4.5.4)*
`/api/meter/<id>/delete-data` deleted the device but never recomputed the parent, leaving `imp_kwh_remainder` stale (dev: kwh/2), diverging from the correct segments. Fix: recompute the parent over the deleted window on delete, and make the recompute EV-aware (`grid − imp_kwh_ev − surviving subs`, not `grid − subs`). Delete button re-enabled (was disabled in 4.5.3). Already-corrupted history heals via delete+reimport. Test: `tests/test_recompute_ev_aware.py`.

#### BL-9 — IOG 6-hour charge cap (4-rate model)  ·  *pricing · SHIPPED (4.4.0, experimental) — VALIDATED (4.5.x)*
Shipped experimental in 4.4.0 and carried a standing validation debt: *"pending validation against a
real settled capped statement."* **Closed** — the 4.5.7–4.5.12 IOG-SMB settlement work reconciles every
capped slot against Octopus's own billed four-bucket breakdown, so the cap model has now been proven
against real settled bills rather than reconstructed rates. The "experimental" qualifier no longer applies.

#### BL-28 — Charger-derived IOG split / deep-history reconstruction  ·  *attribution*
> **Closed — superseded.** The dispatch-derived **synthetic EV** (grid-clipped, hybrid across the
> physical/synthetic seam) plus the **settled billed-breakdown** reconciliation together cover the
> ground this item was scoped for: the house/car split is now correct with or without a charger
> sensor, and settled history is priced from Octopus's own per-slot split. Design retained for
> reference: `docs/design/charger_derived_iog_split_design.md`.

#### BL-34 — Settled peak-priced segments keep a stale `band='off_peak'` label  ·  *UI · cosmetic*
> **Closed — resolved by the 4.5.7 settled-band rework.** The defect was a segment priced at the peak
> rate while still labelled `off_peak`. `apply_measured_to_block` now derives the **band and the rate
> from the same decision** (the bill's cost/kWh against the block's own-date agreement bounds) and
> writes both onto the segment together, so they cannot disagree. Verified on live data (prod and
> prod-dev, 12 Sep 2026): across all settled segments, every `off_peak` segment carries the off-peak
> rate and every `day` segment the peak rate — **0 mismatches**.

#### BL-37 — Dispatch-poll heartbeat (meter-up / poller-down refinement)  ·  *robustness*
> **Closed — not planned.** The residual case (EMT's meter ingest up but the dispatch poller down) is
> covered in practice: Kraken's dispatch feed is dependable, and DCC settlement is the final authority
> on any slot EMT mis-estimated in the interim. Parked deliberately at 4.5.0 and now formally closed
> rather than carried as perpetual backlog.

## v4

### 4.5.5 ✅ — IOG SMB / time-of-use pricing complete + measured-cost reconciliation

*Completes the SMB / 6-hour-cap tariff and adds a settled-bill reconciliation path.* The new tariff drops `standard-unit-rates`, returning day/night as two flat windowless rates that collapsed `resolve()` — EMT now **reconstructs the windowed periods** (BL-52), prices every block on the tariff that applied on **its own date** via an **agreement-stitched** schedule (BL-54), and resolves large half-hourly schedules in **O(log n)** (BL-56, Agile-scale). The settlement reconcile was made to actually run after a restart (schedule-ready gate, **BL-59**) and to re-price the **EV/house split on a capped block** in lock-step with a band change (**BL-58 / BL-58b**). For a settled dispatched block the heuristic can't price with confidence, EMT **defers to Octopus's billed cost** (**BL-53**, closes BL-20): the bill decides the band, `imp_rate` snaps to the clean tariff rate, `imp_cost` is the exact bill, and a material disagreement is applied **and review-flagged** as a possible billing discrepancy. The Cost-Corrections tool became a complete top authority (ex-VAT re-derived from the corrected inc via the VAT calendar, **BL-57 / BL-57b**), and the review list + web reads were hardened (per-thread read connection, no more `SQLITE_MISUSE`; retrying loader — **BL-18b / BL-18c**). Additive and off for non-IOG tariffs; inc-VAT totals unchanged. Full design: `docs/design/EMT-4.5.5_iog_smb_tou_pricing_design.md`.

### 4.4.0 ✅ — Priced-segment pricing model, hybrid EV & the IOG 6-hour cap (experimental)
Pricing moves to a single **priced-segment** model (**BL-27**): every half-hour stores its real rate bands — off-peak / peak, car / house — as one ordered `{kwh, inc_rate, exc_rate, band, attribution}` record that **is** the pricing and the single source of truth for **every** surface (Billing, Usage Stats, Usage Insights, the day & heatmap charts, the carbon view, and Cost Corrections), retiring the layered EV-split / ex-VAT columns whose drift caused a recurring bug class. History migrates **once** in the background on first run — a progress banner while it works, self-repair of any block a prior version mispriced (notably the **capped-IOG-priced-in-pence** break, now correct in £), and a single `reprice_history_report.json` written to the share folder — via a unified reprice sweep that is now the **sole** historical derivation (the three legacy backfills retired). The **EV device** becomes a **hybrid across the seam**: the recorded physical charger (CT/CAD) stands *before* dispatch coverage, Octopus's **synthetic completed-dispatch** EV supersedes it *after* — stitched into one continuous 'EV' identity, authoritative for cost **and** carbon, so the house/car split is correct with or without a charger sensor and reads the same on every surface. The **IOG 6-hour charge cap** lands as an **experimental** 4-rate model (**BL-9**, still pending validation against a real settled capped statement): a migrated capped meter is priced on the full noon→noon cap-day with the out-of-window off-peak 'freebie', and a cap-boundary half-hour bills to its **two real rate bands** rather than a blended average; cap length is live-configurable (`IOG_CAP_HOURS`). Real-data hardening fixes: the bill EV/Home split shows one **canonical per-band rate** (rate-change-safe) taken from the tariff rather than re-divided rounded costs; **Direct import floored at 0** (kWh *and* £) on battery-assist slots where the synthetic EV over-claims grid; the migration carries the **canonical tariff rate** for clean blocks (no 1/kWh scatter); Usage-Stats rate-tiers rebuilt from segments; the Charts/Bill freshness token catches a cost-neutral re-price (**BL-32**); and the supplier **reconnect** now persists the API mode and re-renders the panel in place, so a Disconnect or DB-swap no longer looks like lost credentials (**#381**). Additive and billing-neutral for uncapped / non-IOG accounts — inc-VAT totals and the Total Bill are byte-identical. **Deprecation:** from **v5.0.0** the one-time legacy migrations are removed — if you're below 4.4.0, upgrade *through* 4.4.x first (**BL-33**).

### 4.3.2 ✅ — IOG house-vs-car split consistency (pre-4.4.0)
Corrects an attribution error present since the split landed in 4.3.0 (**BL-9**): a completed dispatch Octopus confirms *after* a block was priced was never back-attributed (the settlement re-stamp only touched blocks that already had a split), so a late-confirmed charge sat silently in **Home** — and because the outcome depended on *when* each instance priced a slot, two copies of the same account could disagree permanently. Settlement now **back-attributes** the same grid-clipped split on any block that has a completed dispatch but no stored split, so history self-heals on first run and both instances converge on the same figure. Display attribution only — grid totals and the Total Bill are unchanged (it moves settled cost Home→EV, so a charge hiding in Home now shows against the car). Uncapped IOG only — the capped case belongs to the priced-segment model (**BL-27**).

### 4.3.1 ✅ — Phantom rate above the tariff peak (IOG split)
When settlement **reverted** a negligible smart-charge slot from off-peak back to peak it rewrote the block's inc rate but **not** the stored EV/house split, so the car slice stayed frozen off-peak while the block priced at peak and the home remainder absorbed the missing peak cost — surfacing an impossible "Home" row at a rate *above* the tariff peak on the grid-total breakdown. Reconciliation now **re-derives** the EV/house split alongside the rate on uncapped IOG (so it can't drift again), with a one-off startup repair for any block already showing it. Display attribution only — grid totals and the Total Bill were always correct.

### 4.3.0 ✅ — Charge Cap groundwork *(experimental)*
[Still experimental] Back-end for Intelligent Octopus Go's new **4-rate / 6-hour-cap** tariff (`IOG-SMB-TOU`): every IOG block now stores the house-vs-car split reconstructed from Octopus's own **completed-dispatch** record (grid-clipped, no charger sensor needed), a migrated (capped) meter is priced with the **full 4-rate model** (noon→noon 6-hour boundary + the out-of-window off-peak freebie), and the billing summary itemises the house-vs-car split per rate band; the day chart's car-rate line is wired to diverge the moment a cap engages. Additive and off for non-IOG tariffs; inc-VAT figures byte-identical. Plus fixes: CSV gap/date-range templates start at **local** (not UTC) midnight (#372); synthesised dual-rate bill CSVs no longer double the daily standing charge (#370); the Usage Insights rate breakdown matches the Billing view (#371); and a FIT / no-export-agreement account no longer shows a permanent "awaiting DCC settlement" backlog.

### 4.2.0 ✅ — Ex-VAT figures, VAT calendar & settlement/backfill fixes
Retain the real **pre-VAT** figures at source (`imp_cost_exc` / `imp_rate_exc` / `standing_charge_exc` / `exc_source`), captured at both import and DCC settlement and shown on an opt-in ex-VAT toggle, with a paced one-time backfill for existing history — inc-VAT figures byte-identical (**BL-23**). A **VAT calendar** (5% domestic seed since 1997 + rates learned per tariff, snapped to {0,5,20}%) replaces the four hardwired `÷1.05` spots, with summary VAT computed as inc − exc so a VAT-holiday boundary inside a period is exact; negative Agile prices handled via the inc/exc pair. Opt-in **bill-style rounding (ex-VAT method)** on-read at the totals layer, default `exact` (**BL-24**). New **overlay-a-single-channel** CSV backfill — a FIT owner can bring export from a third-party CSV (Glowmarkt/Bright) over a range that already holds import, via the store's per-channel first-in-wins merge. Fixes: the 6-hour poll window now anchors to the **oldest unsettled block on any channel** so lagging DCC export stops looking stuck behind live import; gap-fill no longer writes a **false-0 export block** for a DCC-only export channel. All additive, default-off, billing-neutral.

### 4.1.3 ✅ — Dispatch-derived EV split + Agile plunge-price display fix
**Dispatch-derived EV sub-meter (BL-22):** for an Intelligent Octopus account with no EV meter, reconstruct the EV-vs-house split from Octopus's own completed-dispatch data and show it as an "EV (from dispatch)" device across Insights, Usage Stats and Billing — grid-clipped and cost-apportioned so house + EV sum to grid import exactly (Total Bill byte-identical), validated ~99% vs a real CT-clamp meter, display-only and a no-op for anyone with a real sub-meter. Fixes: Agile plunge-price credits no longer dropped from the charts (negative import cost now survives aggregation/display in every path — a −£1.12 day had shown as +£0.22); smart-charging card colours a slot from its dispatch source so genuine off-peak charging no longer flashes red before settlement. Also lands the additive, default-off BL-23/BL-24 groundwork (nullable `imp_cost_exc` column + pure `octopus_bill_total()` rounding-ladder helper), wired to nothing.

### 4.1.2 ✅ — Smart-charging card polish
Experimental "≈ charging" time estimate shown beside the (renamed) "dispatched" window; per-slot chart labels now in local time rather than UTC (display only).

### 4.1.1 ✅ — Import-panel & carbon-backfill fixes
Import page no longer sticks on "A backfill is running" after a restart (durable summary now records a terminal status); historical carbon backfill no longer stalls on a full stored postcode (normalised to the outward code per request). Experimental smart-charging "time spent charging" + slots figures.

### 4.1.0 ✅ — Settlement/attribution refresh fixes
Usage Stats refreshes on an export-only settlement and after an in-place edit (value fingerprint in the change token); device grid-share fixes so the billing breakdown, Usage Stats and grid-import total reconcile exactly; completed-dispatch actual window retained (groundwork for BL-9). *(The Usage-Stats two-endpoint aggregation-share landed back in 4.0.0; the remaining Billing-chart unification is still upcoming — see above.)*

### 4.0.0 ✅ — Historical import, region-aware carbon & data-management overhaul
Backfill your full half-hourly history from Octopus (GraphQL Measurements API, ~2-year retention) or a CSV, with a background job (pause/resume/cancel), rate-limit-polite pacing, self-healing pricing verification and per-gap CSV fill. Region-timeline foundation → region-correct historical carbon for imported blocks. Recorder-history device attribution (reconstruct a device's past usage from HA's recorder, reversible) — this delivers **BL-12** (the recorder-based device split), generalised beyond outage blocks to any device added after it was already recording in HA. Usage Stats **HH** view + side-by-side Spiral rework. Data Management reorganised (Fill History & Gaps landing, background Delete/backup jobs). Physical-plausibility block guard (#307) + self-heal. Removed the legacy `migrate_json_to_sqlite` shim and the four per-block HA sensors. Many settlement/carbon/reconnect-storm fixes.

## v3

### 3.4.0 ✅ — Smart-charging insight + cleanup
IOG smart-charging card on the Overview (per-charge sessions from `dispatch_history`, off-peak/peak split + saving; **BL-10**), raw dispatch `startDt`/`endDt` precision retained (**BL-11**), and retirement of `publish_ha_sensors` + the deprecations sensor (**BL-17**).

### 3.3.0 ✅ *(+ hotfix 3.3.1)* — Review surface + correction usability
Flagged (`needs_review`) blocks surfaced in the UI (**BL-18**); nearby-rate picker when correcting ([#270]); sub-meter grid-import invariant enforced on the fill/re-settle paths (**BL-19**).

### 3.2.0 ✅ — Feature release with fixes
Outage-resilience (**BL-1** settlement-freeze fix, **BL-8** outage backfill), instance isolation (**BL-5**), and the global notification region + update-available banner (**BL-6**, fixes #219).

> **BL-8 phase 2 (4.2.x) — deliberate-deletion persistence.** BL-8's outage backfill couldn't tell a deliberately-deleted range from an outage hole, so a manual delete was silently re-created on the next poll (and the 4.2.3 gap-scan pulled the window back to refill it). Phase 2 records each delete in a `deleted_ranges` tombstone that the backfill, settlement sweep, and gap-scan skip, so deletes stay deleted; a **targeted** re-import / per-gap fill / CSV fill lifts the tombstone (sub-span split) and restores, while the **blanket** "recover all" respects it. Billing byte-identical (gates only block *creation*). See `docs/design/deleted_ranges_design.md`.

### 3.1.5 ✅ — New IOG 6-hour-cap tariff (general rates)
Fail-loud guard for the new Intelligent Octopus time-of-use tariff plus day/night general-usage rate support, so a migrated meter is billed correctly instead of silently priced at £0 ([#1708]). EV-device rates and the 6-hour cap deferred to BL-9 (#272).

### 3.1.4 ✅ — Intelligent Octopus dispatch reconciliation
Smart charges are priced from the dispatch **lifecycle** (`started` under `SMART_CONTROL_IN_PROGRESS`), not the meter — solar/battery-supplied charges are billed off-peak correctly ([#253]). Also: same-total-every-month billing-chart fix ([#271]), meter-exchange selection hardening ([#244]), billing-source indicator.

### 3.1.3 ✅ — Critical segfault hotfix
Fixed concurrent cross-thread use of one SQLite connection during delete-triggered chart regeneration.

### 3.1.2 ✅
Cumulative sub-meter lifetime-dump fix ([#260]); delete-device / delete-blocks chart regeneration ([#261]).

### 3.1.1 ✅ — #253 diagnostic groundwork
Observe-only dispatch-lifecycle capture (groundwork for #253), plus fixes to the corrections tool, Usage Stats, power-sensor config, and the Spiral chart on mobile.

### 3.1.0 ✅ — Spiral chart
Spiral chart for a year — or a lifetime — of energy at a glance; carbon-intensity averaging fix on near-balanced solar days; devices always follow the main meter's rate.

### 3.0.1 ✅
Post-release fixes: #217, #218, #221, #223.

### 3.0.0 ✅ — DCC Settlement, Carbon & Intelligent Octopus Go
**The largest release since EMT began.** Reconciles every block against settled half-hourly Kraken/DCC data (import/export, unit rates, standing charges on a schedule; automatic settlement sweep; Meter-vs-Supplier-API billing toggle). Adds `cad`/`cad+api` modes with supplier-first wizard setup; Octopus Home Mini live power; the **Intelligent Octopus Go dispatch overlay**; whole-app **grid carbon intensity**; per-channel rate source; power-sensor invert/unit override; Ohme EV charger support. Major-additive — everything new is opt-in.

## v2

### 2.10.0 ✅ — Sub-meter Boundary Interpolation
Provisional sub-meter blocks retrospectively boundary-corrected within ~10s of the post-boundary read; `imp_provisional` column; PASS-2 re-run after each amendment.

### 2.9.0 ✅ — Gap-fill Limit, Meter Reset Advisory & Device Retirement
12-hour gap-fill limit (extended outages no longer interpolated); meter-reset advisory banner; device retirement (archive without deleting history); Usage PDF fix.

### 2.8.3 — incorporated into 2.9.0
Usage PDF carbon-narrative fix.

### 2.8.2 ✅
PDF export fixes (carbon duplicate panel, usage hidden-card exclusion).

### 2.8.1 ✅
Favicon; wizard timezone auto-detect; Insights PDF; generation-mix history at CI-tick resolution; Usage Stats Net Import/Export columns; gauge light-theme fix; charts period recall.

### 2.8.0 ✅ — Timezone Refactor, Performance & Usage Insights
UTC throughout (local_date dropped); billing-chart JS 5.8 MB → 76 KB; Usage Insights tab; generation-mix donut + 48-hour mix chart; mix in Carbon Insights.

### 2.7.1 ✅
CI gap backfill; PASS-2 on gap-fill blocks; sub-meter spike detection; Insights calendar nav; narrative comparison; data-bounds gating; main-meter cascade delete; 464 tests.

### 2.7.0 ✅
Battery SoC / inverter / EV-HP gauges on Live Power; sub-meter card layout; meter type selector; Add Device redesign; config change reason in Billing History.

### 2.6.3 ✅
Usage Stats table scrollable with sticky headers/totals; sortable date; billing day-order toggle; heatmap viewport + CI ordering fixes.

### 2.6.2 ✅
Theme toggle on logo; Insights mobile; wiki link in Help.

### 2.6.1 ✅
Logo click toggles theme; sub-meter weighted-rate fix; session-gap block_minutes fix; 1.x Docker upgrade fix.

### 2.6.0 ✅ — Carbon Insights & Navigation Refactor
Insights page (Carbon tab, six adaptive cards); Settings page; navigation refactor; DB sole source of truth; restore reliability.

### 2.5.x ✅ — Stability & Mobile
Touch zoom; mobile topbar collapse; orientation/billing-landscape/chart-height fixes; storage monitoring card.

### 2.5.0 ✅ — Carbon Heatmaps & UI Polish
Heatmap metric toggle (kWh/gCO₂/gCO₂-per-kWh); effective-intensity column; power-history drag zoom; fixed topbar and floating toolbars.

### 2.4.1 ✅ — Carbon Accounting Fixes
House-remainder carbon; double-count removal; mixed-unit render fix; per-block carbon split.

### 2.4.0 ✅ — Carbon in Usage Stats
CO₂ metric in Usage Stats; reliable backup/restore (WAL flush, engine pause/reset); server-side zip restore; upgrade safety backup.

### 2.3.0 ✅ — Carbon Tracking
Carbon intensity from National Grid ESO; `carbon_g` per block; 48-hour power-history chart with kW/CO₂ toggle.

### 2.2.x ✅ — Chart & Billing Fixes
Auto-refresh, cache headers, regeneration optimisation, gap-fill rate/spike fixes.

### 2.2.0 ✅ — Data Management
Bill summary redesign; Delete Blocks; Corrections page; Compact Database; Lovelace chart endpoints.

### 2.1.x ✅ — Sub-meter & Billing Fixes
Sub-meter flags; double-count fixes; standing-charge to main meter only; chart colour sync.

### 2.1.0 ✅ — Full SQLite: Single Source of Truth
All state in `blocks.db`; normalised schema; JSON authoritative-state files eliminated; Corrections with time-of-day window + per-meter targeting.

### 2.0.0 ✅ — SQLite Foundation
SQLite replacing `blocks.json`; Billing History; config-period chain; fast SQL aggregation.

## v1

### 1.6.x ✅ — Polish & Fixes
Billing/Calendar toggle; table totals column; heatmap mobile fixes; light/dark fixes throughout.

### 1.6.0 ✅ — Usage Stats & Theme
Usage Stats chart with sub-meter breakdown; light/dark theme; remember last page; mobile improvements.

### 1.5.0 ✅ — Live Power Gauge
Live power gauge; carbon-intensity forecast; billing cards; billing auto-refresh.

### 1.4.0 ✅ — Global Readiness
Configurable reconciliation period (5/15/30 min); automatic currency detection; international sensor compatibility.

### 1.3.x ✅ — Stability & Timezone
Timezone-aware rendering; UTC timestamp fixes; sensor-timeout fix; standing-charge billing fix.

### 1.2.0 ✅ — Setup Wizard
Guided first-time configuration of main meter and sub-meters.

### 1.1.0 ✅ — Web UI
Flask web UI: Meter Config, Charts, Import & Backup, Logs, Help.

### 1.0.0 ✅ — Initial Release
Core half-hour metering engine, sub-meter support, gap filling, billing charts, HA sensor publishing.

[#272]: https://github.com/RGx01/energy-meter-tracker-addon/issues/272
[#270]: https://github.com/RGx01/energy-meter-tracker-addon/issues/270
[#253]: https://github.com/RGx01/energy-meter-tracker-addon/issues/253
[#271]: https://github.com/RGx01/energy-meter-tracker-addon/issues/271
[#244]: https://github.com/RGx01/energy-meter-tracker-addon/issues/244
[#260]: https://github.com/RGx01/energy-meter-tracker-addon/issues/260
[#261]: https://github.com/RGx01/energy-meter-tracker-addon/issues/261
[#1708]: https://github.com/BottlecapDave/HomeAssistant-OctopusEnergy/issues/1708
[#219]: https://github.com/RGx01/energy-meter-tracker-addon/issues/219
[#408]: https://github.com/RGx01/energy-meter-tracker-addon/issues/408
