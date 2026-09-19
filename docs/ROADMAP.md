# Roadmap

*Active backlog only — priority ordered. Shipped, closed and superseded items live in
[ROADMAP_archive.md](ROADMAP_archive.md); release detail is in [CHANGELOG.md](../CHANGELOG.md).
Nothing in this file has shipped.*

**Issue tracking.** Every open item should carry a GitHub issue reference. Items marked
⚠️ **needs issue** have none yet — see [Issues to create](#issues-to-create) at the foot.

| # | Item | Area | Target | Issue |
|---|------|------|--------|-------|
| 1 | BL-47 — Auto-run recorder device attribution across gap-filled windows | attribution | next | ⚠️ needs issue |
| 2 | BL-50 — Unify user-job mutual exclusion | architecture · correctness | v5.0.0 | ⚠️ needs issue |
| 3 | BL-33 — Remove the one-time legacy migrations | deprecation | v5.0.0 | ⚠️ needs issue |
| 4 | BL-61 — 4.5.7 settlement/chart cleanup follow-ups | tech-debt | v5.0.0 | ⚠️ needs issue |
| 5 | BL-72 — A database cannot say which software produced it | migration · correctness | **4.5.14** | ⚠️ needs issue |
| 6 | BL-71 — `started` alone decides off-peak forever (no revert path) | correctness · dispatch | 4.5.14 | ⚠️ needs issue |
| 7 | BL-63 — Two pricing models: collapse onto one (Principle 0) | architecture · first-principles | v5.0.0 | ⚠️ needs issue |
| 8 | BL-64 — Bill-parser fixtures + a pypdf bump gate | testing · tooling | next | ⚠️ needs issue |
| 9 | BL-60 — Usage Stats block inspector (pre/post-settlement) | diagnostics | proposed | ⚠️ needs issue |

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
- **Shelved model:** `reprice.reprice_block` (169 lines), `carbon.ev_carbon` / `house_carbon` / `carbon_from_reprice`, and 9 of 11 public functions in `pricing_segments` (the whole "legacy imp_* columns become views over the segments" projection layer). Zero production callers; ~37 test references, so the suite is green and the code reads as load-bearing.
- **The requirement is already met by the live path.** Block-vs-segment rate agreement across every measured block since 1 Sept: **0 mismatches**. The atomic-recompute invariant was reached incrementally (BL-27 segments-as-truth, P3.3a's unified sweep, the settlement seam writing all derived fields together) rather than via Principle 0.

**The residue that is NOT solved.** Derived rates are back-computed from a 6-dp rounded cost rather than carrying the canonical band rate, so they scatter: 12 measured blocks hold `imp_rate != imp_rate_ev`, worst case 1.3e-5 (0.05493 vs 0.054917). Cost impact nil — but it is exactly what let a blended rate slip past reconcile's `abs(cur_rate - off_peak) < 1e-6` band test in BL-62, silently disarming the bump revert. Any exact-equality comparison against a canonical rate is defeatable this way.

**Decision required at v5.0.0** (do not act before — switching live pricing to an unexercised path is the larger risk, and the 4.4.0 doc itself rates (A) *"higher risk, perf-sensitive (a re-price per block over years)"*):
1. **Finish the collapse** — route everything through `reprice_block`, delete engine's copy. Faithful to Principle 0; highest risk; buys an invariant already held.
2. **Adopt the live model** — retire `reprice.py`, `carbon_from_reprice`, `ev_carbon`/`house_carbon` and the unused projection layer with their tests; record in the 4.4.0 design that Principle 0 was satisfied incrementally. Lowest risk; removes the trap that a shelved-but-tested module reads as canonical.
3. **Keep both** — rejected: it is the status quo, and it has already cost a misdiagnosis.

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

# BL-73 — supplier token in `store_meta` (4.5.14)

*Rewritten 19 Sep 2026 after checking the 4.5.13 schema. **The first draft of this item was mostly
redundant** — it proposed adding an account marker that already exists, and justified a supplier
marker with an argument that does not survive v4 being frozen. What remains is one optional field
and a decision.*

---

## What the first draft got wrong

It proposed two new `store_meta` keys, `supplier` and `account_ref`. Checking 4.5.13:

| proposed | reality |
|---|---|
| `account_ref` — "supplier-neutral account identity" | **Already exists.** `kraken_state.kraken_account_number`, written at `engine.py:5507` / `:5509`, populated in all three real databases to hand. `get_db_account()` reads it; `kraken_account_mismatch()` already implements the match guard. |
| `supplier` — "which supplier this history belongs to" | **A supplier field already exists** — `config_periods.supplier`, holding `'Octopus Energy'`. It is display text, not a machine token, so it does not serve the purpose — but see below, because a token may not be needed in v4 at all. |

The `account_ref` rationale was that reading a Kraken-specific key would leak v4's internal layout
into v5. That is a weak argument: the importer reads v4's entire schema by definition — twenty
tables and forty-five columns — so one more key name costs nothing, and duplicating a live value to
avoid it is the worse trade. **Dropped.**

---

## What is actually open: does v4 need a supplier token?

### The case for "no"

**v4 is frozen after .14.** It will never gain a second supplier, so *every v4 database that will
ever exist is Octopus*. BL-72's `written_by` already identifies a source as v4-era. The supplier is
therefore derivable with certainty:

```
written_by says v4  ⇒  supplier is octopus
```

One line, in the importer — the module that already encodes everything about v4's shape and is
deleted when v4 support ends. Nothing leaks, nothing is added to a freeze release.

### The case for "yes"

The derivation is an *argument*, and arguments rot. It holds only while "v4 is Octopus-only" stays
true, and it lives in the reader rather than the data. A stamped token is a fact in the file:

```
store_meta.supplier = 'octopus'
```

One string, written where BL-72 already writes two, in the release whose whole job is stamping the
database so v5 can act on it. And the asymmetry is the usual one — if the derivation is ever wrong,
the backups are already on users' disks and no release can reach back to annotate them.

### Recommendation

**Ship it**, but for the honest reason rather than the one the first draft gave: not because the
account layout would leak, and not because v4 might become multi-supplier — it will not — but
because it converts an inference into a recorded fact for the cost of one string, in the one release
designed to record facts.

If it is skipped, the importer carries the `written_by ⇒ octopus` rule explicitly, with a comment
saying why it is safe.

---

## Where the requirement really lives: v5's schema

The multi-supplier future is **v5's**, not v4's. v5 will support suppliers with different
authentication — OAuth, username/password, API key — so v5's own databases and backups need a stable
machine token from the start, for exactly the reason above: a marker cannot be added to a backup
after the fact.

That is a **v5 schema item**, and it should be carried into the v5 schema work rather than treated
as satisfied by whatever .14 does or does not stamp.

---

## The change, if shipped

One key in `store_meta`, written wherever BL-72 writes `heal_level` and `written_by`:

| key | type | value |
|---|---|---|
| `supplier` | TEXT | `'octopus'` — a lowercase machine token, never the display name |

Explicitly **not** doing:

- Not adding an account marker. `kraken_account_number` already exists and is already guarded.
- Not touching `config_periods.supplier`. It is a display and historical-record field with its own
  meaning; overloading it with machine semantics would break both.
- Not adding a supplier abstraction, dispatch or interface to v4. It writes a constant string.

### Acceptance

1. A 4.5.14 database has `store_meta.supplier = 'octopus'`.
2. Written unconditionally, **not** gated on credentials being configured — unlike the account
   stamp, the supplier of an unconfigured install is still known.
3. Survives backup → restore within v4.
4. Lowercase token, asserted by a test, so it cannot drift toward the display string.

> Note the contrast with the account stamp, which is deliberately absent until an account is
> associated — `get_db_account()` returning None is meaningful and v5's import gate branches on it
> (import spec §3). The supplier has no such "not yet known" state.

#### BL-72 — A database cannot say which software produced it, so migration readiness is unknowable  ·  *migration · correctness · target 4.5.14*  ·  ⚠️ **needs issue**

*The gate for EMT (v5). Until this exists, v5 has no safe way to decide whether a database it has
been handed is fit to import.*

**Two findings, one cause.**

*1 — the version stamp does not live in the database.* The upgrade-backup path records the running
version to a **file** in `DATA_DIR`:

```python
with open(_ver_file, "w") as _vfw:
    _vfw.write(_cur_ver)
```

The backup zip contains `blocks.db` and `meters_config.json` — **not** that file. So a backup, a
copied `blocks.db`, or anything EMT is ever handed carries no record of what produced it. The one
in-database candidate, `store_meta.schema_version`, reads `1` on a 4.3.1 database and a 4.5.13 one
alike (§3 of the v5 import spec already flags it as useless for this).

*2 — the heal markers are claims about the past that later events falsify.* Eight one-off heals are
marker-gated in `store_meta`: `pre_live_snapshot_done`, `bl19_grid_invariant_sweep_done`,
`implausible_sub_block_sweep_done`, `register_glitch_sweep_done`, `smb_rate_repair_done_v2`,
`smb_device_recost_done`, `smb_ev_resplit_done`, `band_rate_snap_done_v2`. `run_band_rate_snap` has
**exactly one call site** — the hourly tick, no `force` — and the import/gap-fill paths force only
`_run_historical_reprice_sweep(force=True)`. So **history imported after a heal has marked itself
done is never healed**, while the marker goes on asserting completion. That is a live 4.5.13 defect
in its own right, not only a migration concern: import three years of history tomorrow and those
blocks are never band-snapped.

**Design — two keys in `store_meta`, and a computed level.**

| key | type | role |
|---|---|---|
| `heal_level` | INTEGER, monotonic | **The gate.** Highest heal generation whose invariants hold against the *current* contents. |
| `written_by` | TEXT | Diagnostics only. Never compared, never gates anything. |

An **integer, not a version string**. Comparison is total and cheap, the bar can rise without any
consumer learning v4's version numbering or the names of its eight heals — and a string comparison
walks straight into `"4.5.9" > "4.5.13"`, which is true lexically and false in every other sense.

**The level must be computed, not sticky.** Each heal, at startup, cheaply **verifies its own
invariant** before contributing to the level, rather than trusting its `*_done` marker. If an import
introduced unhealed history the verification fails, the level drops, and the heal re-runs — which
also fixes finding 2 for free.

This inverts the failure mode into the safe direction, and that is the point:

- *Sticky claim* fails **dangerously** — forget to reset it after an import and v5 ingests unhealed
  data believing it clean.
- *Computed level* fails **annoyingly** — a verification bug blocks migration until fixed.

It also makes restore self-correcting with no special handling: restore a 4.5.12-era backup into
4.5.14 and the restored database carries 4.5.12's level, because the level travelled *with the
data*. The heals re-earn it.

**The mechanism carries into v5.** v5 inherits `heal_level` and the verify-before-contribute
pattern, starting from an empty heal set. The `# removed in v5.0.0` comments on four of the markers
refer to *those specific v4 heals*, not to the mechanism — v5 will accumulate heals of its own and
needs the same gate for whatever follows it.

**Also in scope, cheaply, once the key exists:** a **downgrade guard**. Nothing today stops 4.5.13
being installed over a 4.5.14 database and writing to it under older assumptions. Refuse to open a
database whose `heal_level` exceeds what the running build understands.

**Build (4.5.14).**

1. `heal_level` + `written_by` in `store_meta`, with a migration that derives an initial level from
   the markers already present.
2. Each existing heal gains a cheap invariant check, run before it contributes to the level. The
   band-snap one is the most valuable — every IOG-era block's rate matching one of its day's two
   band rates — because it is the heal whose absence produces visible mispricing, and 4.5.13 already
   reports `skipped_off_band` as a countable quantity.
3. Downgrade guard at store open.
4. Surface the level on the health endpoint, so a user can see "ready to migrate" *before* switching
   rather than discovering it at import time.

**Tests.** Level derived correctly from a 4.5.13-era database. Level drops when unhealed history is
imported, and is re-earned after the heal re-runs. Restore of an older backup lowers the level.
Downgrade refused. A 4.4.x database yields the lowest level and is refused by a v5-era requirement.
Explicitly: a database whose marker says done but whose invariant fails must **not** contribute.

**Note for the v5 import spec.** §1 and §10 Q4 currently say "4.5.13 is the last v4 the importer
will ever see", and §3's gate is written around 4.5.13. Both change once this ships: the bar becomes
4.5.14, and §3 gains `heal_level >= N` as its primary check. The bar may rise again as v5 develops;
that is the point of a level.

#### BL-71 — A `started` dispatch decides off-peak permanently, with no revert path  ·  *correctness · dispatch · target next*  ·  ⚠️ **needs issue**

*Found 17 Sep 2026: the same half-hour priced 6× apart on two instances of identical code.*

**The defect.** `_reconcile_decision` tests `has_started` first and returns unconditionally:

```python
if has_started:
    target = "off_peak"      # returns here — the settle window is never consulted
elif not has_completed:
    target = "peak"          # the revert, and its 6h wait, live below
```

The settle window governs only the second branch. A slot that started and **never completed** takes
the first branch and stays off-peak forever — not waiting for a timer, but never entering the timed
path at all. Every hourly reconcile re-confirms it, returns `ok`, and writes nothing.

**The evidence.** `2026-09-17T04:30` (05:30 local — the first half-hour *after* the IOG window
closes, so the overlay alone decides its price):

| | `planned` | `started` | outcome |
|---|---|---|---|
| prod | ✓ | **absent** | neither → peak (revert). **£0.071403** |
| prod-dev | ✓ | ✓ 04:30:04, one poll | `has_started` → off-peak. **£0.011920** |

Same tariff, same plan (`-0.2471`, window `04:30:00–04:32:53`), same `dispatch_slots` row. Both
instances independently measured `ev_charger imp_kwh = 0.0` — **the car did not charge, so prod is
right.** Sixteen hours later prod-dev was still off-peak.

**Why the instances differ — and why `started` is too weak to be decisive.** `_derive_started_slots`
requires `SMART_CONTROL_IN_PROGRESS` *and* an active planned dispatch in the same poll, and returns
at most one slot (the one containing `now`); the full set is built by accumulation across polls. For
this slot the plan's own window was **2m53s** against a ~5-minute poller — structurally
unobservable more often than not. Measured over both instances since 1 Jul:

- **19% of demonstrably-real charges are never sampled as `started`** (98 of 504 slots that *did*
  get a `completed`) — identical on both instances, so the leak is systematic, not environmental.
- Only **~1% of `started` slots were caught by a single poll** (6/417, 5/416). Normal slots are
  sampled repeatedly across a 30-minute window; the fragile case is the **short tail plan** at the
  end of a charge.

So `started` carries a ~19% false-negative rate and is being treated as permanently conclusive. Note
this is *not* restart fragility: `dispatch_history` is never trimmed, writes commit immediately, and
a restart forces an immediate capture. What a restart does is **re-randomise the sampling phase**,
which is the only reason two identical instances disagree at all.

**Fix — a reordering, not new machinery.** Keep *restore early, revert late* (the existing intent:
`started` is real-time, `completed` takes hours). Add the late half: once a slot is past the settle
window with `started` and still no `completed`, fall through to the existing revert. Pass
`past_settle_window` into `_reconcile_decision`; the caller already computes `settle_cutoff`.
Behaviour becomes optimistic-then-corrected, matching the rest of the settlement design — and it
removes the dependence on whether one instance's poll happened to land in a three-minute window.

**Blast radius (prod-dev, all history).** 11 slots have `started` with no `completed`. **8 are
inside the IOG off-peak window**, where the schedule prices off-peak anyway — reverting them changes
nothing. Only **3 are load-bearing**, all at 05:30 local:

| slot | kWh | now | → peak | delta | EV drew |
|---|---|---|---|---|---|
| 2026-07-21T04:30 | 0.142 | £0.007800 | £0.045879 | +£0.0381 | 0.0 |
| 2026-08-04T04:30 | 0.001 | £0.000055 | £0.000323 | +£0.0003 | 0.000828 |
| 2026-09-17T04:30 | 0.217 | £0.011920 | £0.070111 | +£0.0582 | 0.0 |

**Net effect across three months: £0.0965.** The two cases where a revert would be "wrong" are
`2026-08-16T02:30` (drew 0.045 kWh but sits inside the window — no price change) and
`2026-08-04T04:30` (0.8 Wh, £0.0003).

**Do not narrow the rule to the window boundary**, even though that is the signature. The boundary
is tariff-specific; hard-coding 05:30 is the class of assumption that caused the 4.5.13 band work.
The general rule touches 11 slots, changes 3, and moves ten pence.

**The settle window itself is validated — keep 6h.** Completed-arrival latency measured over 504
records: **493 arrive within 2 hours, and nothing at all arrives between 2h and 6h.** The 11
apparent stragglers (6.9h–12.4h) share just two `first_seen` timestamps — `2026-07-08T10:51:51` and
`2026-07-14T13:17:40` — i.e. whole overnight charges caught in one poll after **EMT was down**, not
Octopus dribbling records out. So 6h sits well beyond the real distribution. *Follow-up worth
considering separately:* make the wait downtime-aware (revert only once EMT has been continuously up
for the window since the slot), because a fixed window of any length mis-serves a multi-day outage.
`_reconcile_decision` already takes `was_online` for this kind of reasoning.

**Caveat on the latency figures.** `first_seen` is when EMT saw the record, not when Octopus
published it, so these are an upper bound including our own poll cadence.

**Not urgent.** Settlement overwrites with the bill regardless, so the divergence window is "until
DCC settlement", not permanent. Files: `engine.py` (`_reconcile_decision` and its caller).

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
- [ ] **BL-72** — A database cannot say which software produced it (migration gate)
- [ ] **BL-63** — Two pricing models: decide which one is the model, and collapse onto it
- [ ] **BL-71** — A `started` dispatch decides off-peak permanently, with no revert path
- [ ] **BL-64** — Bill-parser fixtures from real bills, and a pypdf bump gate
- [ ] **BL-60** — Usage Stats block inspector: pre/post-settlement detail for a single block

*Convention: reference issues as `[#nnn]` on the item heading, with the link definition at the foot
of the file (as the archive does).*
