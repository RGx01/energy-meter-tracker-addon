# Roadmap — Archive

*Shipped and superseded items, moved out of the active [ROADMAP.md](ROADMAP.md).*

## Shipped / closed backlog items

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


#### BL-34 — Settled peak-priced segments keep a stale `band='off_peak'` label  ·  *UI · cosmetic · low risk · scheduled 4.5.0*
*Surfaced during the 23 Aug 2026 prod dispatch review.* On API-settled out-of-window blocks the EV/house segment is priced at the **peak** `inc_rate` (Octopus's settled cost — authoritative) but still carries `band='off_peak'`, so the label doesn't match the rate. Cost is unaffected (the segment rate wins), but the band breakdown / any band-keyed display can mislabel the energy. **Fix:** derive the segment `band` from the applied rate (or clear the stale label when a settled API cost overrides the estimate) so label and rate agree. Display-only; no figure changes.


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

## v4

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
