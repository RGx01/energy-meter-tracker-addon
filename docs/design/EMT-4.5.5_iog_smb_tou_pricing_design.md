# 4.5.5 — IOG SMB / time-of-use pricing & measured-cost reconciliation (as-built)

*Consolidated as-built design for the 4.5.5 pricing work. Supersedes the three interim build
plans (folded in here). The CHANGELOG 4.5.5 entry and the code are the source of truth; this doc
records the final architecture, and — the part worth keeping — the **design decisions and
deferrals** and why they were made, so future work doesn't re-open settled questions or repeat
rejected approaches.*

## 1. Problem

Intelligent Octopus Go's new **SMB / time-of-use ("6-hour cap")** tariff (`IOG-SMB-FIX…`) changed
how rates arrive from Kraken and how the EV portion is billed:

- It **drops `standard-unit-rates`** and returns day/night as two **flat, windowless** rates. A
  windowless schedule collapses `RateSchedule.resolve()` (last-period-wins) to a single all-day
  rate — so every slot read off-peak, and the settlement reconcile stopped reverting mis-flagged
  blocks account-wide.
- The EV portion of a dispatch is billed under a **6-hour daily cap** (noon→noon): within the cap
  it's off-peak; beyond it, peak. That accounting lives at Octopus and **cannot be re-derived
  locally** from a single block — EMT only sees the dispatch lifecycle, not the day's cap balance.

4.5.5 makes the tariff price correctly, prices every block on the tariff that applied on **its own
date**, and — where the local heuristic genuinely can't decide a settled dispatched block —
**defers to Octopus's billed cost**, without ever leaving EMT's canonical clean rates.

## 2. Pricing model (schedule side)

- **Windowed TOU reconstruction (BL-52).** `build_rate_schedule` detects the IOG "no standard
  rates" signature and reconstructs the time-windowed day/night periods the old feed produced —
  night inside 23:30–05:30 UK-local (DST-aware), day otherwise — so `resolve()` / `is_off_peak()`
  work again. Gated to the IOG signature; flat and other tariffs are byte-identical.
- **Agreement-stitched, own-date schedule (BL-54).** The import schedule is stitched across **all**
  of the account's agreements, each clipped to its `[valid_from, valid_to)` window, so a block from
  before a tariff migration prices on its own-date rate rather than the current one. A block whose
  date **no agreement covers** is guarded (`_agreement_priced_ok`) so the reconcile/overlay won't
  reprice it onto the current tariff. Closed agreements are cached in-process by
  `(tariff, from, to)`; only the open agreement rebuilds each refresh.
- **Bisection resolve (BL-56).** `resolve()` / `off_peak_rate_near` / `day_rate_bounds` bisect a
  monotonic schedule (`_vfroms` + `_monotonic`) instead of scanning. Byte-identical results;
  O(log n) so an Agile-scale stitched history (tens of thousands of periods) stays fast.

## 3. Settlement reconcile

- **Schedule-ready gate (BL-59).** The hourly reconcile only runs — and only advances its timer —
  once the import schedule is built. Previously the first dispatch tick after a restart could fire
  before the schedule was ready; the reconcile early-returned on the empty schedule but had already
  burned its hourly slot, so on a frequently-restarted box it silently never ran. Now an unready
  tick is a no-op.
- **Capped-tariff split heal (BL-58 / BL-58b).** On a single-rate revert the reconcile restamps the
  EV/house **segments** in lock-step (not just the block headline), on capped tariffs as well as
  uncapped — the band **labels** carry the capped distinction (EV `peak` / house `day`). A block
  the reconcile already reverted but left **stale-rated** (segment rate in a different band than the
  block) is re-priced in the `target=="ok"` path — but only on a genuine **band flip** (a real
  multi-band cap block, ≥2 distinct segment rates, is left for the measured path), never on
  rounding noise.

## 4. Measured-cost reconciliation (BL-53)

For a **settled, dispatched, IOG** import block the local heuristic can't price with confidence,
EMT defers to Octopus's own billed cost.

- **Authority hierarchy (`rate_source`).** Every block records provenance on one axis:
  `schedule < reconciled < measured < corrected`. A lower authority never overwrites a higher one;
  the heuristic sweeps (reconcile, PASS-2 re-cost) skip `measured`/`corrected`.
- **`measured_cost` store.** Octopus's per-slot billed `cost_incl`/`cost_excl` + TOU-bucket label +
  kWh, fetched via `recover_measurement_costs` (the look-back ladder that dodges the dense-run
  stat-strip), cached immutably with a `fetched_at` stamp. Import only; a floor bounds the reach to
  the tariff era.
- **Audit (read-only).** `audit_measured_costs` compares every cached row against its block across
  the whole set (not just freshly-fetched), classifying **agree / band-flip / label-only / material
  / absent** with a net £ delta — the "safe to apply?" gate.
- **Apply (`apply_measured_to_block`).** The bill is authoritative for `imp_cost` and for the
  **band** decision, but the stored `imp_rate` is the **clean tariff band rate for the block's own
  date** (nearest of `day_rate_bounds`), never a derived `cost_incl/kWh`. exc mirrors: rate ex-VAT
  from the clean rate via the VAT calendar, cost ex-VAT from the bill. Sets `rate_source='measured'`.
- **Apply-and-flag.** A **material** disagreement (£ delta over threshold) is applied **and**
  `needs_review`-flagged with a "possible billing error — verify" reason and the delta — so a
  genuine Octopus mis-bill (a smart charge billed peak; an over- or under-charge) is surfaced for
  dispute, not silently absorbed. Non-material (agree / label-only) apply with no flag.

## 5. Corrections & UI hardening

- **Cost-Corrections is a complete top authority (BL-57 / BL-57b).** A user rate correction stamps
  `rate_source='corrected'` and re-derives block **and segment** ex-VAT from the corrected inc-VAT
  rate via the VAT calendar (per-block), setting an explicit exc even where none existed — instead
  of a proportional rescale / the read-side `inc÷1.05` approximation.
- **Review list + web reads (BL-18b / BL-18c).** Web reads use a **per-thread read-only** SQLite
  connection (safe alongside the engine's writer under WAL), fixing the `SQLITE_MISUSE` from a
  shared connection under concurrent page-load reads; the "flagged for review" list retries with
  backoff and re-pulls on tab focus instead of failing silently.

## 6. Decisions & rationale (settled — do not re-open lightly)

- **Store the clean tariff rate, not the billed cost/kWh.** The measured path keys `imp_cost` on
  the bill but **snaps `imp_rate` to the clean band**. Cost-keying the rate was rejected: it broke
  the canonical clean-rate invariant (every rate must be the tariff rate, so bill totals reconcile
  without per-block rounding scatter), producing many near-identical derived rates
  (`0.323091/0.323093/…`) that cluttered the rate surfaces. The bill's exactness belongs in
  `imp_cost`; the rate stays canonical. **Do not re-introduce a derived rate.**
- **Review-flag material disagreements; never blindly conform to the bill.** A settled bill is *what
  Octopus charged*, which includes **Octopus's own errors** — a real case (14/08/2026) was a
  genuine smart charge Octopus billed at peak. And the measurement can **re-settle** over the
  reconciliation window, so it isn't fixed ground truth. So measured is applied where it agrees / is
  immaterial, and material band flips are **surfaced for the user to accept or dispute** — the audit
  is a billing-discrepancy detector, not just a corrector.
- **The measurement LABEL is only a hint; the COST decides the band.** Octopus reserves the
  `OFF_PEAK` bucket label for smart-charge credits, so a time-of-day night slot reads `STANDARD` at
  the low night value, and a mis-credited daytime charge reads `STANDARD` at peak. Deriving the band
  from the **cost vs the schedule bounds** (not the label) is correct in both cases.
- **The `_capped` gate keys off the block's own date, not the current tariff.** A pre-migration
  (uncapped-era) block reconciled while the current tariff is capped must price from its own-date
  bands. The stitched schedule already knows per-date bands.

## 7. Deferred (with reasoning — for future decisions)

- **Persistent closed-agreement schedule cache (BL-56 pt 2).** Closed agreements are cached
  in-process but re-fetched on each cold restart. A disk cache would trim the one-time startup
  fetch (notable for a multi-year Agile history). Deferred: it's a **startup-latency** nicety, not a
  runtime cost — lookups are O(log n) and the build is off the hot path — so there's no penalty to
  leaving it.
- **Re-settlement watchdog.** Measured cost is cached as immutable, but Octopus can re-settle a slot
  during reconciliation. A periodic re-verify of cached `measured_cost` (keeping history) would let
  EMT flag "Octopus re-billed this slot off-peak → standard on <date>". Natural surface: BL-60.
- **BL-60 block inspector.** A read-only Usage-Stats drill-down (pre/post-settlement, EMT vs bill,
  dispatch lifecycle, discrepancy badge) — productises the manual forensic. No schema; all inputs
  already captured. Proposed, not built.
- **Exact 4-band split on a genuine mixed slot.** `apply_measured_to_block` keeps the true 4-band
  split at the schedule's clean rates for a `mixed` label; a fully-exact per-device attribution on a
  cap-boundary slot is a refinement. Deferred: **zero mixed slots** have occurred on the reference
  account, and the block total is always exact.
- **`target=="ok"` band-label relabel on capped (`_capped_band`).** Left gated: a genuine multi-band
  cap block can have legitimately mixed labels, so a blanket relabel there could clobber a real
  4-band block. Needs true per-segment bands (the measured / `compute_iog_split` path). A handful of
  cosmetic label mismatches (rate/cost correct) wait for it.
- **Zero-kWh apply candidates.** A settled dispatched block with a `measured_cost` row but zero
  metered kWh is skipped by apply (nothing to price) and re-scanned each pass as a cheap no-op; an
  `imp_kwh > 0` clause on the apply query would retire it. Cosmetic.

## 8. Tests

`test_iog_tou_window`, `test_agreement_stitch`, `test_rate_schedule_bisect`,
`test_reconcile_schedule_ready_gate`, `test_reconcile_exc_restamp` (capped restamp + stale-rated
heal + same-band-no-fire), `test_bl53_step2`, `test_measured_audit`, `test_measured_apply`,
`test_measured_apply_pass`, `test_corrections_exc_vat`, `test_read_store_concurrency`.
