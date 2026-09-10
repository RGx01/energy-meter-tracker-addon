# IOG-SMB / time-of-use pricing & settlement (as-built, through 4.5.7)

*Consolidated as-built design for the IOG-SMB pricing work (4.5.5 schedule reconstruction →
4.5.7 settlement authority). The CHANGELOG and the code are the source of truth; this doc records
the **final architecture** and the **design decisions** behind it, so future work doesn't re-open
settled questions.*

> **Settlement authority (IOG-SMB).** A settled IOG-SMB block's band, cost and EV/home split are
> **read directly from Octopus's four per-slot rate buckets** (`getDeviceConsumptionBreakdown`) —
> not inferred from a single aggregate label. Octopus's settlement is authoritative and EMT mirrors
> it; the reconcile lifecycle is **prediction for unsettled blocks only**. See §4.

## 1. Problem

Intelligent Octopus Go's **SMB / time-of-use ("6-hour cap")** tariff (`IOG-SMB-FIX…`) changed how
rates arrive from Kraken and how the EV portion is billed:

- It **drops `standard-unit-rates`** and returns day/night as two **flat, windowless** rates. A
  windowless schedule collapses `RateSchedule.resolve()` (last-period-wins) to a single all-day
  rate — so every slot read off-peak, and the settlement reconcile stopped reverting mis-flagged
  blocks account-wide.
- The EV portion of a dispatch is billed under a **6-hour daily cap** (noon→noon): within the cap
  it's off-peak; beyond it, peak. That accounting lives at Octopus and **cannot be re-derived
  locally** from a single block — EMT only sees the dispatch lifecycle, not the day's cap balance.
- Octopus prices each settled half-hour into **one of four rate buckets** — home day / home night /
  EV off-peak / EV peak — exposed per-slot with a `pricePerUnit` and cost. This is the authoritative
  band + EV/home split (§4).

4.5.x makes the tariff price correctly, prices every block on the tariff that applied on **its own
date**, predicts the band live from the dispatch lifecycle, and **locks a settled block to
Octopus's four-bucket settlement** — without ever leaving EMT's canonical clean rates.

## 2. Pricing model (schedule side)

- **Windowed TOU reconstruction (BL-52).** `build_rate_schedule` detects the IOG "no standard
  rates" signature and reconstructs the time-windowed day/night periods the old feed produced —
  night inside 23:30–05:30 UK-local (DST-aware), day otherwise — so `resolve()` / `is_off_peak()`
  work again. Gated to the IOG signature; flat and other tariffs are byte-identical.
- **Day-feed-gap carry-forward (4.5.7).** Octopus's `day` and `night` unit-rate feeds can trail
  each other — the `day` feed often lacks the most recent dates. Where a band's own feed has a gap,
  `_synthesize_iog_tou_windowed` **carries that band's own last rate forward**
  (`RateSchedule.resolve_or_carry`) rather than borrowing the *other* band's. The earlier code
  cross-borrowed, silently pricing a peak (day) window at the off-peak (night) rate — so recent
  daytime slots settled off-peak. A TOU band rate is stable until a new agreement, so carry-forward
  is correct; cross-band borrow now happens only if a band's feed is entirely empty.
- **Agreement-stitched, own-date schedule (BL-54).** The import schedule is stitched across **all**
  of the account's agreements, each clipped to its `[valid_from, valid_to)` window, so a block from
  before a tariff migration prices on its own-date rate rather than the current one. A block whose
  date **no agreement covers** is guarded (`_agreement_priced_ok`) so the reconcile/overlay won't
  reprice it onto the current tariff. Closed agreements are cached in-process by `(tariff, from,
  to)`; only the open agreement rebuilds each refresh.
- **Bisection resolve (BL-56).** `resolve()` / `off_peak_rate_near` / `day_rate_bounds` bisect a
  monotonic schedule instead of scanning. Byte-identical results; O(log n) so an Agile-scale
  stitched history stays fast.

## 3. Reconcile — the live (unsettled) band prediction

The dispatch-lifecycle reconcile is EMT's **prediction** for blocks Octopus has **not yet
settled**. Once a block is settled, §4 is authority and the heuristic never runs against it.

- **Schedule-ready gate (BL-59).** The hourly reconcile only runs — and only advances its timer —
  once the import schedule is built, so a dispatch tick on a frequently-restarted box can't burn its
  slot on an empty schedule.
- **Capped-tariff split heal (BL-58 / BL-58b).** On a single-rate revert the reconcile restamps the
  EV/house **segments** in lock-step (not just the block headline), on capped as well as uncapped
  tariffs — the band labels carry the capped distinction (EV `peak` / house `day`). A block already
  reverted but left **stale-rated** is re-priced in the `target=="ok"` path only on a genuine band
  flip, never on rounding noise.
- **Prediction rules (unsettled only).** Off-peak is predicted only where a genuine planned dispatch
  covers the slot; a bump / out-of-app draw / non-dispatch draw predicts **peak**. This is a
  best-effort live view; when Octopus settles the slot, §4 replaces it (a live bump shown peak may
  settle off-peak — the value simply updates, with no per-block flag).

## 4. Settlement authority — the four-bucket device breakdown (IOG-SMB)

For a **settled IOG-SMB** block, EMT reads Octopus's own per-slot pricing directly and locks the
block to it. This retires the earlier single-label measured-cost machinery entirely.

- **The four buckets.** `getDeviceConsumptionBreakdown` (standard Kraken endpoint, existing JWT)
  returns, per half-hour, up to four `CONSUMPTION_COST` statistics, each with a `label`, a `value`
  (kWh in that bucket), an `estimatedAmount` (cost) and a `pricePerUnit`:

  | label | party | band |
  |---|---|---|
  | `CONSUMPTION_CHARGE_ECO7_DAY_B` | home | peak |
  | `CONSUMPTION_CHARGE_ECO7_NIGHT_B` | home | off-peak |
  | `CONSUMPTION_CHARGE_EV_DEVICE_OFF_PEAK_B` | EV | off-peak |
  | `CONSUMPTION_CHARGE_EV_DEVICE_PEAK_B` | EV | peak |

  The consumption lands in whichever bucket(s) carry a non-zero `value`. **Home = Σ `ECO7_*`,
  EV = Σ `EV_DEVICE_*`**; the band and applied rate are the active bucket's — read, never inferred.
  (`modelledSubMeterReadings` cross-checks the EV split and matches.)

- **Settled = authority; EMT mirrors it.** Octopus's settlement is the truth for a settled block's
  **cost, band and split**, *including Octopus's own errors*. A known IOG-SMB implementation defect
  buckets some **non-dispatch** draws (out-of-app bumps, free-electric sessions, solar micro-draws)
  into `EV_DEVICE_OFF_PEAK` when the rule would be peak; EMT reflects what was billed and does **not**
  second-guess or per-block-flag it. If Octopus later re-bills (clawback), the user re-fetches the
  range with the existing import/gap-fill tools and the corrected settlement locks in.

- **Authority hierarchy (`rate_source`).** `schedule < reconciled < measured < corrected` — the
  settled-bill tier's stored value is `'measured'` (a historical name from the BL-53 measured-cost
  work; deliberately **not** renamed to `'settled'`, which would force a data migration of every
  existing `'measured'` row — deferred to v5.0.0). A lower authority never overwrites a higher one;
  the reconcile/PASS-2 sweeps skip `measured`/`corrected`.
  The settlement lock is idempotent (**settle-once**): once a block is `rate_source='settled'` it is
  not re-touched except by a user correction or a user-initiated re-fetch of changed settlement.

- **Rate stays canonical.** `imp_cost` is the bucket cost; `imp_rate` is the bucket's `pricePerUnit`
  — which *is* the clean tariff band rate — so the canonical clean-rate invariant holds without a
  derived `cost/kWh`. exc mirrors via the VAT calendar.

- **SMB-gated.** This path is the settlement authority **only** for capped IOG-SMB (signal:
  `"ev_device_off_peak" in _kraken_rate_schedules`). Non-SMB tariffs (old IOG, flat, Agile,
  Economy-7, CSV) are untouched and keep their existing settlement path.

## 5. Corrections & UI hardening

- **Cost-Corrections is a complete top authority (BL-57 / BL-57b).** A user rate correction stamps
  `rate_source='corrected'` and re-derives block **and segment** ex-VAT from the corrected inc-VAT
  rate via the VAT calendar (per-block), setting an explicit exc even where none existed.
- **Guard against flattening a multi-band block.** A single-rate correction on a genuine multi-rate
  (cap-transition) IOG block would collapse its four-bucket split, so the tool refuses it on
  IOG/capped accounts (warn / confirm-to-override) and steers to settlement; single-rate tariffs
  (flat / Agile / Economy-7) and CSV accounts are unaffected. It is not a multi-band editor.
- **Review list + web reads (BL-18b / BL-18c).** Web reads use a per-thread read-only SQLite
  connection (safe alongside the engine's writer under WAL); the review list retries with backoff
  and re-pulls on tab focus.
- **Review-list local time.** `/api/review-blocks` resolves the display tz from `config_periods`
  directly; when it genuinely can't, local fields are null and Load-into-tool is disabled rather
  than offering a shifted block.
- **Nearby-rate clustering.** The rate picker folds float/rounding artefacts into the dominant value
  with a strict `< 1e-4` epsilon, so genuinely distinct Agile rates (1e-4 steps) are never merged.

## 6. Day-chart rate lines (display)

The rate-line model `chart_emit.day_rate_series` (rendered by `energy_charts`) is a pure plotter —
all "what rate applies each half-hour" lives there, once.

- **EV line follows the dispatch-derived rate.** `day_rate_series` returns `ev_fallback[]` flagging
  idle slots where the EV rate is a guess; the chart never lets that guess override a real
  dispatch-derived per-slot rate, so line and bar agree.
- **Cap model gated to capped days via the agreement seam.** Held-peak + the noon-wall-clock reset
  apply only on days within the capped IOG-SMB era (`cap_from` = the SMB agreement's `valid_from`,
  threaded engine → `build_day_chart_html` → `day_rate_series`). Pre-cap days follow uncapped TOU.
- **House line from the authoritative TOU schedule.** Sourced from `import_schedule.resolve(slot)`,
  so it never trusts a stored rate on a 0-kWh / seam-mispriced block; a genuine TOU-transition block
  keeps its blended average rate.
  *(4.5.7 note: with the day-feed-gap fixed (§2), the stored rate on an idle slot is already the
  correct clean rate, so this TOU override is now largely redundant — it re-affirms the same value.
  Retiring it folds into the v5.0.0 single-charting-API consolidation.)*

## 7. Decisions & rationale (settled — do not re-open lightly)

- **The settled band is READ from the four buckets, not inferred.** Earlier work tried to derive the
  band from a single aggregate `TOU_BUCKET_COST` label (unreliable — Octopus reserves `OFF_PEAK` for
  smart-charge credits, so a night slot reads `STANDARD` at the off-peak value) and then from the
  cost magnitude. Both are superseded: the four-bucket breakdown states the band unambiguously (the
  bucket the consumption sits in). This removes the need for label-coping heuristics entirely.
- **Settled = authority; no per-block anomaly flagging.** EMT mirrors Octopus's billed band/cost,
  including known Octopus mis-bills (the IOG-SMB off-peak bucketing defect). Highlighting every
  block that settled differently from the prediction was rejected: high code cost, high user burden,
  nothing the user can action per-block. A re-bill is handled by a **user-initiated re-fetch**, not
  a watchdog.
- **No settlement-lag age-gate.** The four-bucket read is unambiguous, so the asymmetric age-gate,
  the "OFF_PEAK-heals-any-age" rule, the review-flag and the forward-anchored recovery ladder are
  **retired from settlement** — settlement reads cost + split + band from the buckets in ONE
  small-window fetch. `recover_measurement_costs` + its ladder remain **only** for non-SMB import
  recovery (no device buckets there). The 'rates finalising' pill — built on the same disproven
  "settled rates firm up over days" belief — is retired too (the per-slot cost is stable from the
  first fetch).
- **Store the clean tariff rate, not a derived cost/kWh.** `imp_rate` is the bucket's `pricePerUnit`
  (the canonical band rate); `imp_cost` carries the bill's exactness. Do not re-introduce a derived
  rate — it breaks the clean-rate invariant and scatters near-identical rates across the surfaces.
- **`_capped` / settlement gate keys off the block's own date, not the current tariff.** A
  pre-migration block reconciled while the current tariff is capped prices from its own-date bands;
  the four-bucket path is SMB-gated and applies only to capped-era blocks.

## 8. Deferred (with reasoning)

- **Persistent closed-agreement schedule cache (BL-56 pt 2).** In-process only; a disk cache would
  trim cold-start fetch. Deferred — startup-latency nicety, not runtime cost.
- **BL-60 block inspector.** A read-only Usage-Stats drill-down (EMT vs bill, dispatch lifecycle,
  four-bucket breakdown). Proposed, not built; no schema needed.
- **Non-SMB device breakdown.** The four-bucket read is SMB-only; extending an authoritative
  per-device split to other tariffs is out of scope until a tariff needs it.

## 9. History (recorded so it is not repeated)

Kept deliberately short — the *why*, not a change log.

- **The 2026-07 mis-pricing was a structural dual-authority bug, not an API change.** The reconcile
  lifecycle heuristic was running against **already-settled** blocks and racing a competing measured
  pass, so a correctly-settled off-peak block could be reverted to peak. The fix is the principle now
  in §3/§4: **the heuristic is prediction for unsettled blocks; a settled block is locked to
  Octopus's settlement.** (4.5.6 shipped the seed of this — the settled guard — plus precautionary
  IOG import/delete gates that were later walked back once the cause was understood.)
- **The "off-peak label is gone" scare was a misdiagnosis.** It traced to a one-hour timezone bug in
  a standalone probe, not the API. The label was never gone; EMT had corrupted its own settled
  blocks. Don't blame the feed before ruling out local UTC/local key mismatches.
- **The single-bucket + age-gate + forward-ladder machinery (interim 4.5.7) was a workaround for the
  unreliable aggregate label.** It is superseded by reading the four device buckets directly (§4),
  which is authoritative and lets that machinery be deleted.
- **Recent daytime priced off-peak was the day/night feed gap, not a chart bug.** The windowed-TOU
  reconstruction borrowed the *other* band's rate across a gap in a band's own feed, so a peak (day)
  window with no fresh `day`-feed data settled off-peak. Fixed by same-band carry-forward (§2). The
  visible symptom (flat off-peak rate lines on recent days) was downstream — the chart faithfully
  plotted mis-priced blocks; time was lost chasing the display layer before the schedule.
- **One-off SMB rate-repair migration (removed in v5.0.0).** Blocks the feed-gap bug mis-priced are
  re-derived from the fixed schedule on upgrade — local, gated, idempotent; preserves kWh and the
  EV/house split (only the priced rate layer moves). A transient repair, tracked in ROADMAP for
  removal once users have upgraded.

## 10. Tests

Schedule/pricing: `test_iog_tou_window`, `test_agreement_stitch`, `test_rate_schedule_bisect`,
`test_reconcile_schedule_ready_gate`, `test_reconcile_exc_restamp`. Settlement: a four-bucket
tests — `test_measure_settled_buckets` (cost+split+band from `getDeviceConsumptionBreakdown`,
SMB-gated; non-SMB keeps single-cost recovery), `test_measured_settled_v2` (both bands apply, no
age-gate, no flag; skips when the agreement can't give a clean rate), `test_c2_settled_split`,
`test_device_breakdown` — **replace** the retired `test_measured_age_gate`, `test_measured_apply_pass`
and `test_bl53_step2`. `test_measurement_ladder` is **kept** (the ladder still serves non-SMB import).
Migration: `test_smb_rate_repair`. Corrections/UI:
`test_corrections_exc_vat`, `test_read_store_concurrency`, `test_corrections_fixes`. Display:
`test_chart_emit`.