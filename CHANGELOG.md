# Changelog
 
## [4.5.13] — 2026-09-14

### Intelligent Octopus Go (IOG-SMB): read the band from the bucket that carries the charge

Octopus returns **all four** IOG-SMB buckets on **every** half-hour
(`CONSUMPTION_CHARGE_ECO7_DAY_x`, `ECO7_NIGHT_x`, `EV_DEVICE_OFF_PEAK_x`,
`EV_DEVICE_PEAK_x`), zero-valued where unused. The import parser decided off-peak from the
label *set* — a test written for the legacy Intelligent Octopus vocabulary, where a slot
carried exactly one bare `OFF_PEAK` / `STANDARD_RATE` label — so on IOG-SMB it resolved
**every** slot to peak.

Costs were never wrong: `_billed_rate` takes Octopus's billed cost as truth on a banded
tariff, so a mis-banded slot still fell through to cost÷kWh and landed on the right number.
What it cost was the *snap* — an off-peak slot's rate fragmented into per-slot jitter
instead of the clean published value — and a post-import "off-peak slots" count that always
read zero.

The labels are **region-suffixed** with the GSP group letter (`…_H` in the South, `…_B` in
the East Midlands — fourteen groups, A–P without I or O). The new `label_band()` matches the
bucket NAME as a substring, so the suffix is never read and no region list exists anywhere
in the tree. Off-peak is tested before peak because `PEAK` is a substring of `OFF_PEAK`, and
home off-peak (`NIGHT`) and EV off-peak (`OFF_PEAK`) are the same band spelled two ways.

Two consequential details:

- A slot carrying BOTH home and EV consumption is genuinely band-ambiguous and now reports
  `off_peak=None` instead of a false "peak". `_billed_rate` gained a `labelled` flag so such
  a slot is still treated as banded (billed cost is truth) rather than being mistaken for a
  continuous/Agile slot where the schedule would win — which matters for an EV dispatch
  outside the fixed off-peak window.
- A flat tariff's bare `CONSUMPTION` label now classifies as "no band" rather than peak,
  letting the rate resolve by time of day. `STANDARD_RATE` still classifies as peak, which
  the export/published-rate path depends on.

The settled four-bucket reader (`_parse_breakdown_node`) was already correct — it matched
substrings and read the carrying bucket's `value`. It now shares the same classifier so the
two paths cannot drift apart again.

### Historical import: say when Octopus never published half-hourly costs, and point at the bills

The import counts half-hours that came back without a billed cost. It reported that count
as one number for the whole import and offered a Retry — which is right when a fetch was
starved, and useless when Octopus simply does not publish per-slot costs for that product.
On a real account the difference was stark: three consecutive Intelligent Octopus
agreements ran **100% uncosted for 23 months**, while the flat tariff before them and the
IOG-SMB tariff after were both fully priced. Three independent whole-day sweeps and an
11-variant request matrix (every granularity from five-minute to monthly, per register,
date-windowed) all came back empty. Pressing Retry returned the same empty answer.

Coverage is now tracked **per tariff agreement** — the unit Octopus configures product
rates against — and the pricing-health panel reads it:

- an era at or above **95%** uncosted is reported as a period Octopus never priced, naming
  the dates and tariff code, with a button through to the existing PDF-bill CSV builder;
- Retry is still offered, but only for the slots outside such an era — the ones a calm
  re-fetch can genuinely recover;
- the panel no longer claims "All prices recovered" when the only reason nothing is
  outstanding is that an entire period is unrecoverable.

Also fixes an **undercount** in the same panel. The "came back without a price" figure only
ever counted half-hours above 1.0 kWh — a diagnostic threshold that had leaked into
user-facing copy, showing "2,091" where the true figure was around 33,500. The health
endpoint now carries `uncosted_total` (unfiltered) alongside the material count.

The notice distinguishes two very different situations, because the severity is not the
same. On a **single-rate** tariff a schedule-priced half-hour is exactly right and only the
cross-check is missing. On a **banded, dispatch-aware** tariff it is not: Intelligent
Octopus bills a smart-charge session *outside* the fixed off-peak window at the off-peak
rate, and nothing in the consumption feed says when one happened — so those half-hours get
the peak rate and read **too high**. Bandedness is determined from the rate schedule EMT
already builds (distinct off-peak/peak rates that day), not by matching tariff-code names,
so an unfamiliar product classifies itself.

For a banded period the notice says plainly that **the period was not costed correctly**:
EMT could only multiply usage by the published rate, that is not what Octopus charged, and
**EMT has no way to detect or correct the difference** — the costs are absent from the API
at every granularity, so retrying cannot help. The only remedy is re-importing from the PDF
bills, and the notice names the exact date span of bills to gather. Usage is unaffected and
does not change; only the costs are replaced.

### Also

### Historical Import: stop the panel misreporting what is happening

Three fixes to a panel that could show a finished run's summary underneath a banner
saying work was in progress, next to a button that looked ready to press:

- **A disabled button now looks disabled.** There was no `:disabled` styling for `.btn`
  anywhere, so a control the code had correctly locked rendered identically to a live one
  — most visibly "Preview plan", which stayed solid while every button around it greyed
  out. Fixed app-wide rather than per-screen.
- **"Start import" is locked while anything is running**, including the *previous* run's
  pricing-verification pass. The run lock covered the source/task picker but not the start
  control, and the confirm checkbox flipped Start's `disabled` directly — so ticking it
  re-armed Start straight through a live lock. The server already refused these with a 409;
  the UI was offering an action that could only end in a refusal. It now explains itself
  ("waiting for the previous run's pricing check to finish") instead of failing on click.
- **The panel and the banner now read from one state.** The panel tracked only the import
  job while the banner above also tracks the post-import pricing check — which is how a
  terminal "✓ Import complete" came to sit underneath "A backfill is running". While that
  check runs the panel now says so: this session's run reads "Import complete — checking
  prices…" with a note that all blocks are written and prices may read high until the check
  reaches them; a replayed older summary says plainly that the check is current and the
  figures above are from the earlier run.
- **A replayed summary is labelled and dated.** With no job live, the status endpoint
  returns the last completed run so the panel isn't blank on load — but it was rendered
  bare, so a run from weeks ago read as current, complete with "Charts are rebuilt
  automatically". It now reads "Previous import complete · Last import, finished
  19 Aug 2026, 21:02" and puts the chart line in the past tense. A genuinely fresh run is
  unchanged.

- **Bills can now correct an uncostable period without deleting anything first.** A
  CSV/bill import is otherwise first-man-wins — it must never clobber data a user cannot
  get back — so re-importing over an existing span silently skipped every block and
  changed nothing. That rule is now relaxed in exactly one place: inside an agreement a
  previous API import proved the supplier does not cost, a bill's half-hour **replaces**
  what the import left behind (usage x published rate) with the billed kWh, rate and cost
  together. Everywhere else first-man-wins is unchanged, and even inside such a period two
  things are still never overwritten — a live/settled meter reading, and any block the
  user corrected by hand. This matters most on an account with several Intelligent Octopus
  agreements where only some lack costs: coverage is tracked per agreement, so only the
  uncostable ones are touched and the user does no date arithmetic. The apply result now
  reports `blocks_replaced` and `blocks_protected` alongside written/skipped.
- The engine now **acts** on that verdict instead of only reporting it. Once a chunk has
  established that an agreement is uncostable (and only above a ~200-slot floor, so a thin
  era cannot suppress work on noise), its half-hours are no longer queued for retry or
  re-fetched by the import's calm recovery pass, and the deferred verify sweep skips any
  chunk lying wholly inside it — a straddling chunk is still checked in full. Previously
  the panel said "retrying cannot help" while the sweep spent hours of a shared Octopus
  allowance confirming exactly that, reporting 0% progress throughout. The import itself is
  untouched: every half-hour is still fetched and written.
- `log_level` now actually works. `run.sh` has always resolved the add-on option into
  `LOG_LEVEL`, but nothing in Python read it, so logging was hardcoded to INFO and setting
  the option had no effect. An instance already configured with `log_level: debug` will
  start emitting debug logs after this upgrade.
- Maintainer diagnostics, gated behind `log_level: debug` and invisible otherwise: the
  pricing-health panel on Historical Import gains a scenario picker that renders it from
  canned coverage (`pdf`, `mixed`, `partial`, `clean`), so its unhappy states can be
  checked without an affected account. Read-only and banner-labelled. Chosen in the page
  rather than by URL because Home Assistant serves the add-on in an ingress iframe, where
  a query string on the address bar never reaches the document.

## [4.5.12] — 2026-09-12

*Intelligent Octopus Go (IOG-SMB) only. Fixes the billed-cost settlement wrongly claiming a
block that has not yet DCC-settled. On the capped IOG-SMB tariff, the measured-cost drain and
apply pass could stamp `rate_source='measured'` on a live block whose meter reading hadn't
settled (`imp_kwh_api` still NULL) — pricing it from Octopus's billed cost divided by the
still-provisional CAD kWh, which mis-banded tiny slots (e.g. a no-dispatch daytime half-hour
priced off-peak instead of peak) and made a block show as both settled and "awaiting cost
settlement". Costs/totals are within a sub-penny; the visible effect is a wrong band on the
rate line for such slots. Agile, flat, Economy-7 and other tariffs are not affected: the
billed-cost settlement path only runs on IOG/IOG-SMB, and Agile/export reconciliation is
untouched by this change.*

### Fixed

- **IOG-SMB settlement now only applies to blocks that are actually settled.** The history
  drain, its backlog signal, and `apply_measured_settled` now require a block to be
  **DCC-settled** (`imp_kwh_api` present) *or* an **imported** block (`source LIKE 'imported%'`)
  before taking Octopus's billed cost as authoritative — never an unsettled live block. This
  keeps first-time import settlement working while preventing a live block from being priced
  off a provisional CAD kWh before its meter reading lands. Forward-only: an affected slot
  re-settles correctly once its meter DCC-settles (or on the next apply pass).

- **Hardened so it can't come back the same way.** The settleability rule is now a single
  shared predicate used by every IOG settlement pass (no drift between them); the one function
  that stamps `rate_source='measured'` refuses an unsettled live block on its own, so a
  future or direct caller can't bypass the gate; and the band pick now divides the billed cost
  by the **settled** kWh Octopus computed it on, not the block's local CAD kWh — on a small slot
  whose local kWh hadn't yet been reconciled to the settled figure, that mismatch could sag
  cost ÷ kWh into the wrong band. Cost still decides the band over Octopus's label (unchanged
  — the label can be wrong); only the denominator is corrected.

## [4.5.11] — 2026-09-12

*Two Intelligent Octopus Go robustness fixes: a manual Cost-Correction now sticks (it could
silently lose its protection and be reverted, leaving a negative Home rate on the bill), and
importing/gap-filling history now settles the capped SMB tariff to Octopus's own billed split
instead of a schedule-provisional guess. The correction fix is forward-only — re-apply the
correction (or re-import) to repair an already-affected block.*

### Fixed

- **A manual rate correction now survives a block rewrite.** When EMT re-touched a block — a
  gap-fill, a carbon or remainder recompute, a device re-attribution, a settlement re-cost — it
  rebuilt the row and silently reset the block's pricing authority (`rate_corrected`, `rate_source`,
  `rate_reconciled`) to defaults, because those flags were never carried through the rewrite path.
  On IOG that unprotected a user's peak correction, so the dispatch reconcile reverted the block to
  the off-peak "freebee" and left the EV columns stranded at peak — showing a negative Home rate in
  the bill. The rewrite now preserves all three flags (the same fix the `source`/`imported` tag got
  earlier), so a Cost-Correction — and a settled or reconciled rate — holds. Latent since the flags
  shipped; surfaced when 4.5.7 removed the IOG re-import gate. *(Forward fix: existing mis-reverted
  blocks are unchanged — re-apply the correction or re-import the day to repair them.)*

- **Importing / gap-filling history now prices the capped SMB tariff correctly.** The consumption
  feed returns only kWh; on IOG-SMB the price depends on the dispatch EV/House split and off-peak
  freebee, which a first-time user has no local dispatch records for — so filled history read
  schedule-provisional (House-only, day/night, no freebee) and only trickled to the real figures via
  the live settlement pass (newest-first, 40/hour). EMT now drains imported capped-tariff history
  directly against Octopus's own billed four-bucket breakdown (`getDeviceConsumptionBreakdown`),
  oldest-first, so a bulk history fill lands the real cost / EV-House split / band. Agreement-aware
  (only the capped SMB window; flat and uncapped-IOG history already price correctly on the
  schedule); the not-yet-billed recent tail is left to the live settlement pass. Read-only fetch,
  additive apply — a settled or corrected block is never touched.

## [4.5.10] — 2026-09-11

*Aligns the cost totals between the Billing and Usage Stats tabs to the penny on the SMB /
time-of-use tariff, and on Agile. Display-only — no stored figure or bill total changes.*

### Fixed

- **Billing and Usage Stats now show the same cost, to the penny, on multi-rate days.** The two
  tabs derived the "Direct / House" cost differently: Billing subtracts the sub-meters from the
  main total once per day, but Usage Stats did it per rate band and floored each band at zero
  independently. On a single-rate day these are identical, but on an SMB day with several bands a
  battery- or EV-heavy off-peak band could floor to zero and drop a few pence the other bands
  would have absorbed — so Usage Stats read a few pence above Billing. Usage Stats now uses the
  same per-day reducer, so the tabs agree by construction. Flat and Economy-7 are byte-identical
  (one band means per-rate equals per-day), and both tabs already match Octopus's own settled
  per-slot cost exactly.

- **Agile plunge-price credits are no longer clamped away in the period total.** A genuinely
  negative main cost (a "plunge" credit) now survives the day-level house/direct calculation on
  both tabs instead of being floored to zero, so the credit reaches the total on both surfaces.

*(Display-only: recomputed on the next page load / chart refresh — no re-import, no data migration.)*

## [4.5.9] — 2026-09-11

*Fixes the EV vs House split reading too much as EV on days a home battery grid-charged at the
same time as the car, on Intelligent Octopus Go. Costs and totals are unaffected — only the split.*

### Fixed

- **EV is no longer over-counted when the battery grid-charges during a smart-charge slot.**
  Octopus's settled per-slot device breakdown splits the grid into just Home + EV — it has no
  bucket for a home battery, so on an overnight slot where the battery drew from the grid at the
  same time as the car, that battery energy landed in EV. EMT now bounds the EV quantity to the
  car's own completed-dispatch session (Octopus's per-slot smart-charge energy, which matches a
  physical charger meter to within a rounding error), so the battery's share stays with the House.
  The settled bill still sets the EV rate and band; only the kWh split is capped. The Total Bill,
  the grid total and every cost are byte-identical — Billing and Usage Stats simply attribute the
  right amount to EV vs House. Works with or without a physical charger meter.

- **Recent days no longer park EV charging in House until settlement.** When a smart-charge
  dispatch arrives after a half-hour has already been priced, EMT now carves the predicted
  EV/House split from the dispatch straight away (grid-clipped, same source as the settled
  cap) instead of leaving the car's charge in House for ~2 days until Octopus settles. The
  Billing "grid total" EV/House rows match the charger from the moment the dispatch lands;
  settlement still overwrites with the final split. Additive — no kWh or cost changes.

### On upgrade

- **Existing history is corrected automatically, once.** A one-off local pass re-splits any
  settled off-peak slot whose EV was inflated by a concurrent battery charge, moving the excess
  back to House at the same rate. No kWh or cost changes — only the EV/House split. No re-import
  needed.

## [4.5.8] — 2026-09-11

*Fixes the EV / battery cost reading too high on the Billing and Usage Stats tabs for accounts with
a physical charger or battery meter on Intelligent Octopus Go.*

### Fixed

- **A physical EV charger (or battery) is now costed at the settled rate, not the pre-settlement peak.**
  When Octopus settles a daytime smart-charge slot off-peak, EMT corrected the main import but left the
  physical sub-meter at the earlier (peak) rate — so Billing and Usage Stats showed the device more
  expensive than the bill, and the two tabs could disagree. The device now re-prices in lock-step with
  the settled main, so every surface matches the bill. Accounts with no physical device (the synthetic
  "EV (from dispatch)") were never affected.

### On upgrade

- **Existing history is corrected automatically, once.** A one-off local pass re-prices any device
  half-hour left at the stale rate to match its settled main rate — energy (kWh) is untouched, only the
  price moves. No re-import needed.

## [4.5.7] — 2026-09-11

*Fixes recent Intelligent Octopus Go days showing the wrong (off-peak) rate on the billing charts,
and simplifies how EMT reconciles against Octopus's settled bill. The root cause was a gap in
Octopus's day/night rate feeds — not the charts — and the settlement machinery built around an
assumed "rates firm up over days" behaviour is retired in favour of reading Octopus's own per-slot
device breakdown directly.*

*Underlying it all: before a period settles, EMT predicts each block's rate by applying Octopus's
Intelligent Octopus Go four-rate rules faithfully, as published by Octopus on 7&nbsp;May&nbsp;2026
([intelligent-octopus-go-smarter-charging-for-a-greener-grid](https://octopus.energy/blog/intelligent-octopus-go-smarter-charging-for-a-greener-grid/)):*

- Home usage at the **off-peak** rate in the guaranteed 23:30–05:30 window.
- Home usage at the **peak** rate in the 05:30–23:30 window.
- The smart-charge dispatch off-peak **"freebee"** for a metered draw within the 6-hour car allowance.
- **Peak** for bump/boost, for out-of-dispatch charging, and once the cap is exceeded (any EV usage outside of Smart Control, at any time of day).

*Once Octopus settles the period the settled bill is authoritative and EMT reconciles to it. The
published rules and the actual bill do not always agree — in either direction — so a pre-settlement
rate is a best-effort prediction, not a guarantee.*

### Fixed

- **Recent daytime charges no longer show an off-peak rate.** Octopus's separate `day` and `night`
  rate feeds can lag each other, and a missing recent `day` rate was being filled with the `night`
  (off-peak) rate — so daytime slots on recent days priced off-peak. The time-of-use reconstruction
  now carries each band's own last rate forward across a feed gap, so daytime is peak and night is
  off-peak as it should be.
- **The billing chart's house rate line follows the tariff on idle days.** A rounding mismatch
  between the stored rate and the schedule could leave a whole day's house line flat off-peak (and,
  on a day with no EV charging, blank the day). Both are fixed; the line shows the correct
  time-of-use rate across idle and no-charge days.
- **A late-arriving completed EV dispatch no longer strands the charge in Home.** Settlement now
  takes the EV/house split from Octopus's own device breakdown, so a dispatch that lands after a
  block was priced still attributes the EV correctly.
- **The EV rate line only holds peak when the 6-hour cap is genuinely exceeded.** Previously any
  peak-priced early slot — a bump charge, an out-of-dispatch draw, or a mis-priced ~0-kWh slot —
  could latch the EV line to peak all the way to the noon reset. The held-peak now follows only a
  real cap exceedance (a within-dispatch slot past the 6-hour boundary); a bump or out-of-dispatch
  charge shows its own peak tick without latching.
- **The one-off historical re-price keeps the smart-charge off-peak "freebee".** A daytime
  smart-charge slot within the 6-hour cap stays off-peak (as billed) instead of being re-derived to
  peak; a bump/boost or over-cap slot stays peak. Days the first repair pass mis-priced are corrected
  automatically on the next start.
- **The charts refresh automatically after the historical re-price.** The one-off repair now triggers
  a chart regeneration when it changes any rate, so corrected days appear without waiting for a restart.

### Changed

- **Settlement reads Octopus's four-bucket device breakdown.** For IOG-SMB, EMT now reads the
  settled cost, the Home/EV split, and the off-peak/peak band directly from Octopus's per-slot device
  buckets (`getDeviceConsumptionBreakdown`) in one small query. The settled bill is authoritative for
  cost and band, both bands apply as soon as the bill is available, and the settled rate is always the
  exact tariff-agreement rate — never a cost÷kWh derivation.
- **Retired the interim single-label settlement machinery.** The asymmetric age-gate (which deferred
  recent "standard" reads), the per-block review-flag on a band change, the forward-extending
  "recovery ladder" used to coax an off-peak label out of the API, and the "N rates finalising" pill
  are all removed from settlement — they were built to cope with a settlement-lag / unreliable-label
  problem the device breakdown makes moot. The recovery fetch is retained only for non-IOG-SMB import
  backfill, which has no device buckets.
- **Bump/boost charging no longer counts toward the 6-hour cap.** A bump/boost slot is billed at peak,
  so its energy no longer advances the off-peak cap allowance — a peak-billed slot can't also consume
  the off-peak window. This affects the pre-settlement rate prediction only (the settled bill remains
  authoritative).

### On upgrade

- **Historical IOG-SMB days are re-priced automatically, once.** A one-off, local repair on first run
  re-derives the rate for the days the feed-gap bug mis-priced, straight from the corrected schedule.
  Your raw meter data and the Home/EV split are untouched — only the priced rate moves. No re-import
  needed. This repair now also re-runs once to restore the smart-charge off-peak "freebee" on any
  daytime dispatch slots an earlier pass had over-corrected to peak.

## [4.5.6] — 2026-08-29

> ℹ️ **Note for Intelligent Octopus Go users.** IOG per-slot rate data from the API
> is **not reliable enough to safely re-import or re-price against right now**.
> Recently-settled slots come back at the standard (peak) rate until they finish
> settling with Octopus (a few days later they show as off-peak), and some out-of-core
> smart-charge slots don't carry an off-peak label at all — the off-peak comes from a
> dispatch credit the raw rate data doesn't reflect. Because EMT can't reliably tell
> those apart from a genuine peak slot at import time, **API imports and block deletion
> are disabled for IOG periods as a precaution** until the import/settlement timing is
> handled properly. Live tracking is unaffected — EMT keeps pricing your ongoing usage
> from its own dispatch capture — and **CSV import from a bill and manual block
> corrections both still work**, so you can fix or fill data by hand.

*A precautionary stop-gap while IOG API rate reliability is worked through. The
measured-cost pass (which could apply an unsettled/provisional rate over a block that
later settles off-peak) is turned off, a settled-block guard stops the dispatch
reconcile reverting genuine off-peak bumps, and API import + block delete are gated for
IOG periods.*

### Fixed

- **The measured-cost pass no longer over-writes a settled off-peak block with a provisional rate.** The settlement-time fetch could read a slot before it had finished settling with Octopus — getting the provisional **standard (peak)** rate — and then apply it, stamping a genuinely off-peak block to peak (the drift seen between prod and prod-dev on the July/August blocks). It also can't recover the off-peak on some out-of-core smart-charge slots, where the off-peak comes from a dispatch credit the rate data doesn't carry. Both switches (`_MEASURED_FETCH_ENABLED`, `_MEASURED_APPLY`) are held **off** until the pass can reliably wait for settlement. The dispatch overlay (captured live) remains the per-slot off-peak truth for pricing.
- **The dispatch reconcile no longer reverts a settled, dispatched off-peak block to peak.** A completed dispatch is Octopus's own signal that it ran the slot as a smart charge (→ off-peak on the bill); once such a block is **settled** (`imp_kwh_api` present) its billed band is authoritative. The lifecycle heuristic used to flip it to peak on a "we never saw it start" inference or a negligible-completed-energy revert — over-charging genuine off-peak bumps the bill priced off-peak (2026-07-21 16:00 BST, completed −0.09 kWh, under the negligible gate). A planned-only slot (never completed = never charged) is still correctly reverted. Tests: `tests/test_reconcile_settled_guard.py`, `test_dispatch_overlay_follow_main.py`.

### Changed

- **API import / gap-fill is disabled for Intelligent Octopus Go periods (precaution).** IOG rate data can be provisional for recently-settled slots and absent for some out-of-core smart-charge slots, so a re-import can't reliably reprice every slot and could quietly mis-price the bill. Rather than risk silent errors, API import is gated on every path (whole-history, range, per-gap fill, targeted re-import, background controller) plus the engine chokepoints as a safety net. **CSV import from a bill/statement stays allowed.** Import over pre-IOG (e.g. flat) history is unaffected. IOG here covers the whole Intelligent Octopus Go family — the `INTELLI` agreements and the SMB/TOU cap variant.
- **Deleting blocks is disabled for Intelligent Octopus Go periods (precaution).** The only way to restore a deleted block is to re-import it — and while IOG re-import isn't reliable, deleting risks losing data that can't be cleanly restored. The delete preview shows an explainer and the delete is refused; non-IOG history stays deletable, and manual Cost Corrections are unaffected. Tests: `tests/test_iog_gate.py`.
- **The Deleted-ranges list self-heals orphaned tombstones.** A range deleted in the past and later re-created by another path (live poll / an older refill) left a "deleted" tombstone behind even though the gap no longer exists — cluttering the Data Management list and making a present, correctly-priced block read as deleted (`is_deleted_range`). The list now prunes any tombstone whose range is **fully populated with blocks** when it loads; still-empty and partially-filled deletions are kept. This also sidesteps the fact that the normal clear path (re-import) is now IOG-gated. Test: `tests/test_stale_tombstones.py`.

### Known limitation

- **Some IOG slots can't be reliably priced from the API on import: recently-settled slots (until they settle, a few days) and out-of-core smart-charge bumps (where the off-peak is a dispatch credit the rate data doesn't reflect).** For everything else the API's off-peak data settles in correctly. This is why import and delete are gated for IOG rather than left on — a follow-up will let the import respect settlement timing and re-enable it for settled history.

## [4.5.5] — 2026-08-28

*Completes Intelligent Octopus Go **SMB / time-of-use ("6-hour cap")** pricing. EMT now reconstructs the windowed day/night schedule the new tariff drops, prices every block on the tariff that applied on **its own date**, and — for settled EV-dispatch blocks it can't price with confidence locally — **defers to Octopus's own billed cost** to settle the band, while keeping EMT's canonical clean tariff rates. Plus the settlement-reconcile fixes that make all of that actually run, and Cost-Corrections + review-list hardening.*

### Added

- **Measured-cost reconciliation — settled IOG dispatch blocks defer to Octopus's billed cost (BL-53).** The 6-hour-cap tariff prices the EV portion of a dispatch in a way EMT cannot always re-derive locally (it depends on Octopus's cap accounting across the whole day). For a **settled, dispatched** import block **on the current IOG-SMB agreement** — the pass is scoped to the agreement's `valid_from`, so it only ever touches blocks on the capped tariff and never reaches into pre-migration dates (which the prior tariff's schedule and the settlement reconcile already price correctly) — EMT now fetches Octopus's own per-slot billed cost + TOU-bucket from the Measurements API, caches it (immutably, in a new `measured_cost` store), and uses it to settle the block: the **bill decides the band** (off-peak vs peak), `imp_rate` is **snapped to the clean tariff band rate for the block's own date**, and `imp_cost` is set to the exact bill. The canonical clean-rate invariant is preserved — the rate is always the tariff rate, never a derived cost/kWh — while the block reconciles to the penny. Import only; non-dispatched, pre-cap, and non-IOG blocks are untouched. Tests: `tests/test_measured_apply.py`, `test_measured_apply_pass.py`, `test_measured_audit.py`, `test_bl53_step2.py`.
- **A billing-discrepancy detector.** When the settled bill materially disagrees with EMT's own price on a dispatched block, EMT applies the bill but also **flags the block for review** with a "possible billing error — verify" reason and the £ delta — so a genuine Octopus mis-bill (a smart charge billed at peak; an over-charge or an under-charge) is surfaced for you to check and dispute, not silently absorbed. A read-only pre-apply audit (`measure_audit`) reports agree / band-flip / label-only / material / net-Δ£ across the whole cached set.
- **Explicit rate provenance (`rate_source`).** Every block now records who set its price — `schedule` < `reconciled` < `measured` < `corrected` — a single authority axis. A lower authority can never overwrite a higher one, so the heuristic sweeps can't clobber a measured or user-corrected block.

### Fixed

- **The new IOG SMB / time-of-use tariff is now priced correctly (BL-52).** The tariff drops `standard-unit-rates` and returns day/night as two **flat, windowless** rates, which collapsed `RateSchedule.resolve()` to a single all-day rate — every slot read off-peak, and the settlement reconcile stopped reverting mis-flagged blocks account-wide. EMT now reconstructs the missing time-windowed periods (night 23:30–05:30 local, day otherwise; DST-aware) so the schedule has the same shape the old feed produced. Flat/other tariffs are byte-identical; gated to the IOG signature.
- **Pre-migration blocks now price on the tariff that applied then, not the current one (BL-54).** The import schedule is stitched across **all** of the account's agreements and clipped to each agreement's date window, so a block from before a tariff migration prices on its own-date rate. A block whose date no agreement covers is guarded from repricing rather than being forced onto the current tariff.
- **The settlement reconcile could silently stop running after a restart (BL-59).** On boot the first dispatch tick could fire before the rate schedule finished building; the reconcile then early-returned on the empty schedule but had already advanced its hourly timer — so the *real* reconcile was deferred a full hour, and on a frequently-restarted instance it effectively never ran (no heartbeat, no heals). The reconcile block now gates on the import schedule being ready, so an unready tick is a no-op that doesn't burn the slot. Test: `tests/test_reconcile_schedule_ready_gate.py`.
- **The EV/house cost split on a capped block now follows a reconciled band (BL-58 / BL-58b).** On the capped tariff the reconcile used to revert a block's headline rate but **skip** re-pricing its EV/house segments, leaving the split frozen at the old (off-peak) rate — so the billing split showed the EV cost understated and the "Direct/house" line absorbing the whole peak premium (a phantom ~£0.96/kWh house rate on the 23/08 bump). The reconcile now restamps the segments in lock-step on a single-rate revert, and separately re-prices a **stale-rated** split (segment rate in a different band than the block) it finds on an already-reconciled block. Fires only on a genuine band flip (not rounding noise); a real multi-band cap block is left for the measured path. Test: `tests/test_reconcile_exc_restamp.py`.
- **The Cost-Corrections tool is now a complete top authority (BL-57 / BL-57b).** A user rate correction now stamps `rate_source='corrected'` and re-derives the block **and segment** ex-VAT from the corrected inc-VAT rate via the VAT calendar (per-block), instead of a proportional rescale that left a stale figure — and it now sets an explicit ex-VAT even where none existed, rather than the read-side `inc÷1.05` approximation. So a corrected block is complete inc+exc and genuinely outranks the heuristic layers below it. Tests: `tests/test_reconcile_exc_restamp.py`, `test_corrections_exc_vat.py`.
- **The Corrections "flagged for review" list now loads reliably (BL-18b / BL-18c).** The list fired a single fetch and swallowed any error, so a transient hiccup left it blank ("hit and miss"). Root cause: the web server shared one SQLite connection across waitress threads, so concurrent page-load reads raised `SQLITE_MISUSE` ("bad parameter or other API misuse") and returned 500. Reads now use a **per-thread read-only connection** (safe alongside the engine's writer under WAL), and the list retries with backoff + shows a visible Retry rather than failing silently, and re-pulls when you return to the tab. Test: `tests/test_read_store_concurrency.py`.

### Changed

- **Large half-hourly schedules resolve in O(log n) (BL-56).** `RateSchedule.resolve()` (and the off-peak / day-rate lookups) bisect a monotonic schedule instead of scanning it, so pricing on an Agile-scale history (tens of thousands of periods) is fast; results are byte-identical. Closed agreements are cached in-process, so the agreement stitch only rebuilds the current agreement per refresh.
- **A pre-upgrade database backup is taken before the 4.5.5 migration.** Named with the target version so the pre-upgrade snapshot is identifiable; the schema additions (`rate_source`, `measured_cost`) are additive and backfilled silently.

## [4.5.4] — 2026-08-26

*Fix + re-enable: deleting a device (BL-46) now recomputes the parent meter's house/EV split and runs with exclusive DB access (BL-48), so the "Remove" button is safely back. Plus a Historical Import panel-staleness fix (#391).*

### Fixed

- **Deleting a device no longer corrupts the parent's house/EV split (BL-46); the "Remove" button is re-enabled.** The device-delete path removed the device's data but never recomputed the parent meter, leaving `imp_kwh_remainder` stale — so Usage Stats and Billing read a wrong house/EV split for the deleted device's window (dev showed `imp_kwh_remainder = imp_kwh/2`). Two fixes: the delete now recomputes the parent over the affected window, and that recompute is **EV-aware** — it re-derives the house remainder as `grid − dispatch EV − surviving sub-meters` (previously it reset to the full grid and subtracted only physical sub-meters, folding the EV back into house). Validated on a real capped IOG DB: every block collapses to the correct segment house. Already-corrupted history (from a delete on 4.5.2/earlier) can be healed by deleting + re-importing that range.
- **Device delete now runs with exclusive DB access, so it can't race a block finalise or a running import (BL-48).** The delete + parent recompute previously ran on the web thread after only a cooperative `pause_engine()` — which stops the *next* tick but not one already mid-finalise, and doesn't stop the API import writing on the engine loop. Both share the add-on's single SQLite connection, so a delete fired on a block boundary could overlap a finalise (stale/'/2' remainder churn in the logs at best; the two-threads-one-connection segfault the restore path already guards against at worst). Now the delete first drains any running import (aborting with a clear message if it won't stop), then drains the in-flight tick via a no-op under the tick lock (new `engine.run_exclusive`) — the engine stays paused so no new tick touches the DB — and runs the delete+recompute on the request thread, so nothing shares the SQLite connection while it writes. The recompute deliberately does **not** run on the engine loop: a whole-history device delete can touch tens of thousands of parent blocks and take minutes, and running it on the loop froze HA comms and blew the request timeout. The per-block PASS 2 log is silenced during a bulk recompute (a whole-history delete was emitting tens of thousands of INFO lines). Also fixes a latent leak where the engine could stay paused after a delete (`engine_startup` doesn't resume on its own). Tests: `tests/test_run_exclusive.py`.
- **Device History re-attribution is safe over freshly gap-filled ranges (`float * NoneType` crash fixed).** Re-attributing a device (e.g. an EV charger) right after a gap fill could run PASS 2 over house blocks whose legacy `imp_rate` was still `NULL` — the gap-fill's verify pass hadn't repriced them yet — so `claimed * rate` raised `unsupported operand type(s) for *: 'float' and 'NoneType'` and `run_attribution_job` failed. Two fixes: PASS 2 now resolves a null rate from the priced `block_segments` (source of truth), else cost/kWh, else 0 — so the device is costed at the real rate, never crashes, and a genuinely-unpriced block is the only thing that lands on 0; and the attribution job now **waits for a running import or verify** (not just a delete) before writing, since attributing over not-yet-verified blocks could otherwise cost a device at 0 permanently. Tests: `tests/test_pass2_rate_fallback.py`.
- **Device History now heals a deleted gap anywhere in a device's history, not just before its first live reading (BL-49).** Attribution filtered to hours *before* the live-coverage seam (the device's earliest real block), so a gap deleted in the live region — e.g. deleting Apr–May and re-attributing — was silently skipped (`wrote 0`) even though the recorder had the data. The per-block guard already protects live readings (it fills only an absent row or a zero-hole and never overwrites a real non-zero device reading), so the blanket seam cutoff was removed and attribution now walks the full recorded range. This makes the delete-data → re-attribute heal (and BL-47) actually work in the live region; it's also why the null-rate fallback above is required — healing now writes into recently gap-filled blocks. Test: `tests/test_attribution_seam.py`. The per-block PASS 2 log is also silenced during a bulk re-attribution (walking the full range writes thousands of blocks), matching the delete-recompute suppression.
- **Historical Import: the Pricing-health panel no longer shows a stale verify verdict until a manual refresh (#391).** After an import settled, the panel's poll only re-armed while a verify was *already* active, and the status loop pinged it just once at the import→verify handoff — so a verify that launched a moment later was never picked up, leaving "✓ Up to date" on screen while a pass was genuinely running (seen at ~82% behind the stale badge). Fix: a bounded ~20s watch window after an import settles keeps the health poll re-arming until the verify appears, then hands off to the normal verify poll. JS-only; no server, data, pricing, or bill-total change; respects the existing `_freshFlow` / persisted-snapshot staleness guards. Two related Historical-Import reactivity fixes found while testing this: after kicking off a gap fill the status panel no longer shows a stale plan until a manual refresh — the poll now keeps watching until the job is actually seen active (a gap fill takes a pre-import backup first, which outlasted the old 12s window); and the "Import these blocks" arm checkbox is now hidden while a job is running, instead of sitting there unchecked next to a live Pause/Cancel. And the status/health polls now keep each other alive across the job's phase gaps (import → price-recovery → whole-history verify) via a shared recent-activity heartbeat, so the panel tracks the verify tail live instead of freezing on a stale "Up to date" until you refresh.
- **`GET /api/config` is now read on the engine loop, so a concurrent DB access can't corrupt it (root cause of the `'<' not supported between 'str' and 'NoneType'` 500).** The config was read on the Flask request thread against the SQLite connection the engine also uses; under load (waitress queue backed up, engine draining reads + a settlement poll) that cross-thread single-connection use produced a garbled read — one column came back `None`, became a `None` dict key, and crashed jsonify's key-sort. No data corruption (the on-disk config was provably clean immediately after); unrelated to the same-day tariff migration. Fix: `api_get_config` marshals the read onto the engine loop (single-threaded owner of the store), matching the existing `_regen_charts_safely` pattern. The motivating case for BL-50.
- **`config_from_db` / `_write_meters` now skip a NULL/empty channel or meter_id row.** Belt-and-braces so the data path can't produce a `None` dict key even if a legacy pre-`NOT NULL` row exists. Test: `tests/test_config_null_meter_id.py`.

## [4.5.3] — 2026-08-26

*Hotfix: first-time setup on an Agile account (or any long half-hourly tariff history) could time out at "Connect your supplier" with "Could not connect: check key/account" even with a valid key; an API-only account with no live source could get stuck with blocks never forming; and the "Current grid generation mix" donut could show a forecast slot up to ~48h ahead.*

### Fixed

- **First-time connect no longer stalls building a large tariff schedule (BL-43).** Agile's rate history is ~34,000 half-hourly periods; EMT built that schedule + diagnostics inline on the engine event loop during connect, starving the Home Assistant WebSocket heartbeat (supervisor "No PONG received after 15s"), so the connect request timed out and the wizard showed the generic "check key/account" — though the backend connect had actually succeeded. The build now runs on a worker thread and the per-period diagnostic is capped for large schedules. Fixed-tariff accounts (≈328 periods) were unaffected, which is why it only hit Agile users.
- **API-only accounts with no live source no longer thrash block formation (BL-44).** With no Octopus Home Mini reads and no local sensor, a block gets no post-boundary read, finalises "nothing to finalise", and — because an empty block didn't advance the opener — every tick re-rolled the same boundary forever. The opener now rolls forward to the current window, so blocks advance and DCC settlement backfills. Gap catch-up is unaffected.
- **The "Current grid generation mix" donut no longer shows a forecast slot (#408, BL-45).** `mix_history` stores the fw48h forecast, and the donut selected `MAX(captured_at)` — the furthest-future slot (up to ~48h ahead), e.g. a gas-heavy forecast night reading ~56% gas while the current mix and the 48-hour chart showed ~30%. The donut now picks the most recent slot at or before now, and `get_mix_history` gained the same `<= now` bound so a forecast slot can't reach the chart either. Display-only; the CO₂ figure and Insights were unaffected (they read `get_nearest_carbon_intensity` / block-stamped carbon).

- **Deleting a device is temporarily disabled (pending BL-46).** The device-delete path (`/api/meter/<id>/delete-data`) removed a device's data but never recomputed the parent meter's house/EV split, leaving `imp_kwh_remainder` stale (out of sync with the still-correct segments) — so Usage Stats and Billing read wrong for the affected window. The "Remove" button and the endpoint are disabled with a notice until the recompute-on-delete fix lands; **Retire** is unaffected.

## [4.5.2] — 2026-08-25

*Hotfix: on Octopus **API / Home Mini** accounts **with sub-meters** (an EV charger, a house battery, a CT-clamped device), the most recent day or two of **Usage Stats** could read too high — a device's energy counted once inside "Direct/house" and again on its own line — until the half-hourly reads settled. Reported on an Indra + Fox battery setup whose Mini had just come back online.*

### Fixed

- **The live house/device split now runs for API/Mini sub-meter setups, not only on settled days.** When EMT rebuilds the in-progress block from its saved reads (`load_current_block`), it was dropping each meter's configuration — including which meters are sub-meters and what they hang off. On a local-sensor (CAD) setup this never showed, because the live sampling loop re-applies that config every few seconds before the block closes. But on an API/Home-Mini setup the block is closed by the Mini's boundary read, which uses the rebuilt (config-less) block — so the grid-authoritative sub-meter split silently did nothing, and the day's "Direct" import kept the devices inside it *and* listed them separately, inflating the Usage-Stats total. EMT now restores each meter's configuration when it rebuilds the block, so the split lands live (as it does on CAD). Display / aggregation only — grand totals and the bill were always correct, and already-affected recent days self-correct as they settle (settlement always carried the configuration). Long-standing behaviour that surfaces when a Mini drives block finalisation with sub-meters present — not a regression from a specific release.

## [4.5.1] — 2026-08-25

*Hotfix: a charger added as an **"EV Charger"** device could show **two** EV lines — the physical charger *and* the synthetic **"EV (from dispatch)"** — double-representing the same charge. Reported on an Indra Smart Pro that is also the Octopus dispatch source.*

### Fixed

- **A charger added as an "EV Charger" device no longer double-counts against the dispatch EV.** The config screen writes the device type as `ev`, but three of EMT's EV-recognition checks only accepted the internal `ev_charger` spelling (otherwise matching on the meter's *name* containing "ev"/"charger"). A charger added through the UI gets a random meter id, so on IOG accounts where that charger is **also** the Octopus dispatch source (Indra, Hypervolt, Pod Point, …), EMT failed to recognise it as the EV, didn't let the synthetic "EV (from dispatch)" supersede it, and drew **both** — the same charge on two device lines. The checks now accept both spellings, so the two fold into one EV line. Display / attribution only — grand totals were always correct; no reprice or migration (it re-renders correctly on the next load).

## [4.5.0] — 2026-08-25

*Theme: **Honest bump pricing.** When you force a charge outside Octopus's smart schedule — a Zappi **Fast** boost, an in-app **bump** — Octopus bills it at the **peak** rate (and credits any Free Electricity to your balance separately). EMT's live estimate was doing the opposite on those slots, showing them as cheap **off-peak** until settlement corrected them. 4.5.0 teaches the estimate to tell an out-of-app bump from a genuine smart charge, so your provisional numbers read right the first time — and it stops EMT ever discarding the dispatch history it needs to do so. Estimate/display only on API-connected IOG accounts; settlement was, and remains, the final authority, so no settled bill figure changes.*

### Fixed

- **A manual bump / boost is no longer shown as cheap off-peak before settlement.** On Intelligent Octopus Go, EMT reconstructs the car-vs-house split from Octopus's **completed-dispatch** records. A genuine smart charge is *planned* ahead and confirmed *in progress* (`started`); a **bump** is neither — Octopus never plans it, so it arrives only as a **completed** dispatch, with no plan and (crucially) **no `source`** flag to say it was a boost. EMT's settlement reconcile treated any such plan-less completed dispatch as "a smart charge we must have missed while offline" and priced it **off-peak** — correct when EMT really *was* offline, but wrong for a bump made while EMT was up and running. It now checks whether EMT was actually **online** for that half-hour — read straight off the block's own live-vs-gap-filled flag: if EMT was live and polling, an unplanned completed dispatch is an **out-of-app bump** and is priced **peak** (the house off-peak "freebie" withheld, the 6-hour cap allowance untouched); only a genuinely offline or imported slot keeps the optimistic off-peak. This fixes the mis-price for **Zappi Fast** boosts and for **car-side EV integrations**, where no charger sensor exists to tell smart from boost. It also guards against a **database rebuild**: the planned/started record accumulates locally (Octopus doesn't serve it historically), so a rebuild that re-fetched only the *completed* dispatch could otherwise look like a bump — EMT therefore only calls it a bump when that completed dispatch was recorded **live, close to the slot**. *(Provisional estimate only — a settled block always takes Octopus's authoritative cost, so no settled figure moves.)*
- **Rate-band labels now always match the rate on every screen.** A block whose rate was reverted to peak (a bump, or a tiny out-of-window top-up) could keep a stale `off-peak` band **label** on its segments even though the **rate** and cost were already correct — so the rate-band breakdown could colour a peak block as off-peak. Labels now follow the rate: corrected when a rate changes, and swept onto any already-correct block that still carried a stale label (a self-limiting pass that heals older blocks over the normal settlement run, then leaves them alone). Display only — no cost or bill figure changes.
- **The grid generation‑mix donut and 48‑hour chart no longer show impossible spikes, and now always agree.** National Grid's *regional* fuel‑mix figures are a modelled estimate, and for a small region they occasionally emit a physically‑impossible half‑hour — one fuel at ~100% with a 0 gCO₂/kWh intensity (e.g. "solar 97%" at 9pm). EMT stored those verbatim, so the **Current Grid Generation Mix** donut, the **48‑hour** chart, and that half‑hour's **carbon** could all read wrong. EMT now rejects a glitch slot on fetch — the previous good slot stands — and the donut reads the **same live source as the chart**, so the two can no longer disagree. Display and carbon‑view only (no bill effect); any already‑stored bad slots age out of the rolling 48‑hour / 4‑day stores on their own, so no backfill is needed.
- **The EV / house split now heals itself after an outage.** If EMT was offline while your car charged, those half-hours are backfilled from Octopus's data — but the car's share could be stamped from an incomplete early reading and then **never corrected** once the real *completed* charge record arrived hours later, leaving too much of the import shown as **house / Direct** instead of **EV**. The hourly reconciliation now re-checks any backfilled block whose EV figure disagrees with Octopus's completed charge record and re-derives the split (and the underlying segments) from it — so an outage **self-corrects on the next pass** once the record lands, and stays fixed. Gap-filled blocks only; live half-hours were always correct, and manual corrections are never overwritten.

### Changed

- **EMT now keeps your dispatch history permanently.** The planned/started/completed dispatch record is what lets EMT tell a smart charge from a bump and re-price history correctly — yet 4.4.0 quietly deleted it after 90 days, mirroring Octopus's own rolling window. That prune is removed: EMT retains the full dispatch lifecycle for the life of the database (a few thousand rows a year), so a re-price months later still has the original ingredients to work from. *(No visible change today — it protects future accuracy.)*
- **Ohme chargers: the smart-charging card can show Ohme's own charge plan (experimental — Ohme only).** When Intelligent Octopus Go is linked to the *charger* rather than the car, Octopus doesn't publish the plan — Ohme does. With the Ohme Home Assistant integration connected, EMT can now read Ohme's upcoming charge slots and its live smart/boost mode, so the smart-charging card shows the real forward plan and a verified smart charge is no longer flagged for review. **Experimental and unvalidated** — guarded, logged, and settlement stays authoritative, but not yet confirmed against a live Ohme account (Ohme owners' feedback welcome). No change for non-Ohme setups.

## [4.4.0] — 2026-08-23

*Theme: **Priced Segments**. The house-vs-car pricing that 4.3.0 stored as layered, drift-prone columns now lives in a single, extensible **priced-segment** model — each half-hour's price is stored as its real rate bands, and that one record is the source of truth for **every** surface and tool: Billing, Usage Stats, Usage Insights, the charts, the carbon view, and the Cost Corrections tool all read the same segments, so a figure can no longer disagree between screens. Your history is migrated automatically the first time you run 4.4.0 — with a progress banner while it works and a one-file report when it's done — and it self-repairs any block a previous version left mispriced. The headline fix: **capped Intelligent Octopus Go charging that a prior version could price in pence is now priced correctly in pounds.** Additive for existing tariffs — inc-VAT totals and the Total Bill are unchanged. (The 4-rate cap model remains **experimental** pending validation against a real settled capped statement.)*

### Added

- **Priced-segment pricing model — one record, every surface (BL-27).** Every half-hour block now stores its price as **priced segments** — the actual rate bands that make it up (off-peak / peak, car / house) — and that single record is what **all** surfaces and tools read: Billing, Usage Stats, Usage Insights, the day and heatmap charts, the carbon view, and the Cost Corrections tool. This retires the layered EV-split / ex-VAT columns that kept **drifting out of sync** between screens (the recurring class of bug behind several 4.2.x / 4.3.x fixes), so the car-vs-house cost, the ex-VAT breakdown, and the rate-band split now agree everywhere by construction. The model is tariff-agnostic — flat, Economy 7, Octopus Go, Cosy, Tracker and **Agile** (including **free 0p** slots and negative **"plunge"** prices) all migrate faithfully — and a capped IOG boundary half-hour is stored as its two **real** rate bands instead of one blended average. *(Additive — on an uncapped or non-IOG account the stored figures are byte-identical to before; the Total Bill never moves.)*

- **Your history migrates automatically on the first upgrade — and tells you how it went.** The first time you run 4.4.0, EMT re-prices your existing history into the new segment model **in the background**, while the app keeps running. A banner (*"Finishing your upgrade… please be patient and avoid restarting or rebuilding"*) shows while it works and clears itself when it's done, any block a previous version stored incorrectly is **repaired automatically** as part of the pass, and a single **`reprice_history_report.json`** is written to your share folder summarising what happened — the one file to send if anything ever looks off. *(No re-import needed; no supplier calls beyond the rates EMT already fetches.)*

- **The car-vs-house split is driven by Octopus's own charging data everywhere — energy *and* carbon.** 4.3.0 began reconstructing the IOG house-vs-car split from Octopus's **completed-dispatch** record (no charger sensor required). In 4.4.0 that dispatch figure is the **authority** on every surface, for both energy/cost **and** the carbon view — so your car-vs-house numbers are correct **whether or not** you have a physical charger sensor, and they keep working unchanged if you **retire** an old sensor. A physical charger is still kept for reference (and remains the source on non-API accounts), but it no longer overrides Octopus's own record — which also means the two can no longer disagree on screen. *(Non-IOG accounts are unaffected.)*

- **Cost Corrections now repairs the whole block.** Correcting a half-hour's **Import Rate** now also rewrites the car/house split, the ex-VAT figures, the device lines, **and** the stored segments — not just the headline rate and cost — so a corrected block reconciles end to end and stays consistent across every screen. *(Fixing a rate you know to be wrong now leaves nothing stale behind it.)*

### Fixed

- **Reconnecting the supplier API after a Disconnect (or a database swap) now sticks — it no longer silently reverts to local-only (#381).** Following the #357 credentials-lifecycle work, one gap remained: re-entering your API key and reconnecting built the **live** connection (discovery ran, polling resumed) but never **persisted the data-source mode**. So if a prior **Disconnect** — or a **database swap** that parked the mode on `cad` while the key survived in its own file — had left the mode local-only, a successful reconnect came up connected *for that session only*: on the **next restart** the startup activation gate saw a non-API mode and skipped the supplier API, dropping you straight back to local billing and surfacing as **“credentials missing”** even though the key was saved. The reconnect looked like it had **falsely failed and reverted to `cad`** (with the config screen's Disconnect/Reconnect controls out of step until a manual page refresh, hiding the way back). A successful in-app reconnect now **promotes the mode back to an API mode** — `cad+api` when a local import sensor is present (keeping the CAD feed), otherwise `api` — so activation **persists across restarts** and Reconnect can’t quietly undo itself. Modes that already use the API are left untouched, and because a reconnect only promotes on a **verified** live connection, it can never strand you on an API mode with nothing to poll. *(Mode/activation lifecycle only — credential values are never handled differently, and no billing figure or stored data changes.)*

- **Capped Intelligent Octopus Go charging is no longer priced in pence.** On a migrated **capped** IOG-SMB-TOU meter, the 4-rate cap pricing introduced in 4.3.0 multiplied the car/house rate by **100** on the specific half-hours the cap's EV-device rates applied — so a handful of smart-charge blocks could show a wildly inflated rate and cost (and, downstream, an inflated bill for those slots). Capped blocks are now priced correctly in **pounds**, and any block a prior version already stored in pence is **repaired automatically** on upgrade (or on demand via Cost Corrections). *(Only capped IOG accounts were affected; uncapped and non-IOG pricing was always correct.)*

- **Car and house figures agree on every screen — even when a charger sensor disagrees with Octopus.** When a physical charger's recorded energy or carbon diverged from Octopus's dispatch record, some surfaces (notably the carbon breakdown) could mis-attribute the difference — in the worst case flooring the "home" figure to zero. With the dispatch record now authoritative everywhere (see *Added*), the car and house splits — cost **and** carbon — reconcile consistently across Billing, Usage Stats, Usage Insights and the charts. *(Display attribution only — grid totals and the Total Bill were always correct.)*
 
### Deprecated

- **From v5.0.0, EMT will require a database already migrated to the 4.4.0 priced‑segment model.** 4.4.0 migrates your history once, in the background, on first run. In v5.0.0 the one‑time legacy migrations are **removed** — the pre‑segment history backfill, the 2.x→3.0 data‑source‑mode bridge, and the 4.3.0 pence‑repair self‑heal — so a database that has never been opened by 4.4.x cannot be upgraded straight to 5.0.0. **If you're on a version older than 4.4.0, upgrade through 4.4.x first** (let the one‑time migration finish), then move to 5.0.0. Nothing changes for anyone already on 4.4.0+; the ongoing import/CSV/gap‑fill re‑pricing is unaffected (only the *first‑upgrade* migration paths go). See the README upgrade notice and `docs/ROADMAP.md` (BL‑33).

---

Released versions (**4.3.x and earlier**) are in [CHANGELOG-ARCHIVE.md](CHANGELOG-ARCHIVE.md).