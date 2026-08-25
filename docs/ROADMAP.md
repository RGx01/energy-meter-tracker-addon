# Roadmap

Newest at the top. **Upcoming work (ranked by priority) first, then release history newest-first** (v3 → v2 → v1, oldest at the very bottom). Scope and timing subject to change.

---

## Upcoming — ranked by priority

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

### 5.0.0 — Unify the billing / usage-stats aggregation onto one endpoint

*Consolidation, deferred out of 4.0.x deliberately because it touches billing (and not yet picked up in 4.1.x / 4.2.0).* There are today **three** independent implementations that aggregate the same per-block data: the **Billing chart** (`calculate_billing_summary_for_period` in `energy_charts.py`, server-rendered into `daily_usage.html`), **Usage Stats daily/monthly/yearly** (`/api/charts/blocks-summary`), and **Usage Stats HH** (`/api/charts/blocks-day`). The 4.0.x HH work already collapsed the *two Usage Stats endpoints* onto shared helpers (`_aggregate_block_rows` + `_bucket_to_row_values` in `server.py`) — proven byte-identical by a golden-output capture and kept honest by `test_usage_stats_vs_billing.py`. **What remains is the harder unification: fold the Billing chart's aggregation and the Usage Stats endpoints onto a single source of truth**, so "billing" and "usage stats" can never drift apart by construction rather than by a cross-check test.

**Hard constraint (non-negotiable, carried from the 4.0.x work):** the results must not change. A great deal of effort has gone into making billing as accurate as possible and into making every other aggregation *align* with billing. Any refactor here MUST be pure code-motion — identical outputs — or it doesn't ship. Method to follow (the one used for the HH refactor): capture the current Billing-chart and endpoint JSON as a **golden baseline**, refactor, assert **byte-identical** output, run `test_usage_stats_vs_billing` + `test_server` + the billing-chart render smoke test; ship only if the diff is empty, otherwise revert.

**Shape of work (to be spec'd):** (1) decide the single home for the block→period aggregation — most likely `calculate_billing_summary_for_period` becomes the canonical primitive and the two endpoints call *it* (rather than the reverse), since billing is the authoritative one; (2) reconcile the row/column shapes the three consumers expect (the endpoints emit a per-bucket JSON shape with rate-keyed sub-meter subtraction, carbon split, avg-intensity; the billing chart consumes a summary object) behind one core with thin per-consumer adapters; (3) golden-proof each consumer independently before deleting any duplicated code. *Medium–large; gated on a design spike, not a quick win — the value is eliminating drift risk, so correctness proof is the whole job.*

**Concrete drift instance (23 Aug 2026 prod_dev) — evidence this is real, not theoretical.** The 4.5.0 bump work exposed exactly the drift this item removes. The reconcile reverting a bump to peak rewrote `block_segments` (which the **Billing chart** prices from), but the **Usage Stats** endpoints (`/blocks-summary`, `/blocks-day`) still aggregate the legacy `imp_kwh_ev` / `imp_cost_ev` columns — so for the *same day* Billing showed **EV £2.28** while Usage Stats showed **EV £0.74**, spilling the difference into Direct import. Two representations of one split, updated out of step. **Interim shims** now keep them consistent: the reconcile re-stamps the legacy EV columns in lock-step with the segments (rate-change path + an `ok`-branch self-heal, same pattern as the BL-34 band sweep). **Those shims exist only because the aggregation isn't unified — delete them when this item ships**: once Usage Stats reads the segments directly there is no second representation to drift. It's also the proof that the cross-check test (`test_usage_stats_vs_billing`) can *detect* drift but not *prevent* it — only a single source of truth does.

### Post-4.0.0 follow-ups (remaining threads from the 3.5.x / 4.0.0 import + guard work)

*Most of the original 3.5.1 list shipped in 4.0.0 — historical-import correctness, tariff-schedule standing charges, the HTTP-400 old-agreement fix, the #307 §1+§3 plausibility guard + self-heal, region-correct historical carbon, and the backup-creation / Delete-Blocks background jobs. These two are what's left.*

#### Deprecation-CI drift guard — remaining hardening  ·  *maintainer tooling*
Registered in 4.0.0: the Measurements query fields + filter enums, the rate-limit pacing query and IOG `currentState`, plus a completeness test that AST-parses every GraphQL query and fails if a selected field isn't covered. **Still to do:** (1) **type-scoped matching** (`_EMT_GRAPHQL_TYPED_FIELDS`) for the Relay-connection generics still in the test's allowlist (`edges`/`node`/`value`/`unit`/`pageInfo`/`hasNextPage`/`endCursor`/`limit`/`ttl`) so they're covered without bare-name false positives; (2) note in the doc that the check is **GraphQL-only** — the import's **REST** dependencies (`get_account` postcode/tenancy/address, `get_consumption`) and the external **Carbon Intensity API** are out of scope and would want their own light shape/contract guard.

#### #307-b — Sensor-semantic reset model + user-declared reset boundary  ·  *fix / enabler*  ·  *follow-up to #307*
*The accurate half of #307, split out because it needs a config surface.* The plausibility guard clamps an impossible delta to **0**; the *better* repair is register **continuity** — when a cumulative sensor's opener is lost, re-base it to the **previous block's `read_end`** and book the true (0-or-plausible) delta instead of discarding it. But that's only safe if EMT knows the channel is a **cumulative lifetime register** (opener must equal previous closer) versus a **session-energy sensor** (e.g. `zappi_charge_added_session`) that *legitimately* resets to 0 each plug-in — applying the continuity rule to the latter would "repair" a real reset. Two parts: **(a)** make the semantic **explicit per channel** — a `cumulative | session` flag on the meter/sensor config, derived where possible but overridable — and drive the continuity/reset logic off it instead of today's implicit time-separation-between-charges luck; **(b)** a **setup option to declare a reset**: "this sensor was reset / replaced at block/date XYZ", so a genuine meter swap or firmware reset is recorded as an intentional boundary (not a glitch) and the continuity check re-bases from there rather than flagging or mis-clamping. Reuses the effective-dated pattern (like region-periods / config-periods). Lets a user fix the rare real reset EMT can't infer, and unlocks the accurate previous-read recompute for #307. *Small–medium; not urgent — the #307 clamp already prevents the damage.*

---

### Backlog — unscheduled (ordered by BL-ID)

#### BL-2 — Phantom export channel from the gap-seed
*Low — cosmetic, never affects billing (kWh = 0).* In a no-export `api` setup the gap-seed carries a 0-kWh export channel forward, so the daily billing chart draws a 12p export-rate line despite zero actual export. Root fix: don't carry forward an export channel that has no configured source and never carried real export.

#### BL-3 — Carry billing-chart rate lines forward as a step
*Low — the agreed near-term fix for BL-2's visible symptom.* Build the import/export rate series by forward-filling the last-known rate (a tariff is in force all day) instead of zero-filling per slot; back-fill leading slots and use a None sentinel so a genuine 0 rate stays distinct from "no data". Exact for a flat tariff; the per-slot schedule-resolve variant is the upgrade path for a varying Agile Outgoing export tariff.

#### BL-4 — PDF export overhaul (pagination + orientation)
*Low — usability, no data impact.* The PDF export relies on the browser's print-to-PDF and doesn't paginate: every chart is forced A4 landscape as fixed-size images that can't reflow or break across pages. **Billing is the worst case** (multiple per-period plots + tables, effectively unprintable in portrait). Fix is a print-layout rethink: portrait-first per-view orientation via a single `@page` rule; real pagination (`break-inside: avoid`, `break-before: page`, `thead` repeated via `display: table-header-group`); reflowable capture sized to page width; optionally move generation server-side (WeasyPrint/reportlab) if the CSS-print route proves too limited.

#### BL-7 — Sensor-vs-device-type sanity check
*Low-medium — prevents a class of silent misattribution.* At device creation, warn when the assigned sensor looks inconsistent with the device type (e.g. an EV device pointed at a battery/inverter sensor). Surfaced by the forum device-usage-swap case, where an "Indra Smart Pro" EV device read the Fox battery's register for a week before it was noticed. A creation-time heuristic (entity-ID / device-class / magnitude sanity) that prompts the user to confirm would catch it before it corrupts history.

#### BL-9 — IOG 6-hour EV-cap billing  ·  [#272]  ·  **SHIPPED (4.4.0, experimental) — pending real-bill validation**
**SHIPPED as experimental in 4.4.0** — the 4-rate / 6-hour-cap billing mechanism is built and wired (noon→noon cap accumulator, 4-rate classifier, `ev_device_off_peak`/`ev_device_peak` rates, completed-dispatch window bounds) and reconciles to the penny on a capped fixture; what remains is validation against a real *settled* capped statement. *High impact.* Step 2 of the IOG time-of-use tariff (step 1 — day/night general rates — shipped in 3.1.5). The 4-rate / 6-hour-cap model is **confirmed against Octopus's published material** and is no longer blocked — full design in `docs/design/iog_6hr_cap_design.md`.

**Model:** bill on main-meter grid import; **EV = the `completed`-dispatch delta, grid-clipped**; **house = import − EV** (physical charger meters stay indicative — billing is always dispatch-derived, so it works for any charger). **Cap = union of `completed` dispatch windows per noon→noon (local) day**; the EV portion bills `ev_device_off_peak` within 6 h and `ev_device_peak` beyond (or on Boost). House gets the guaranteed off-peak window (from the rate schedule, not hard-coded) always, **plus** the off-peak *freebie* on out-of-window dispatch slots **while within** the cap — withdrawn once over-cap. Enabled by **tariff-code detection** (no manual toggle); the cap is a **live/provisional predictor** and **settled billed cost is authoritative**, so estimates self-correct at settlement. Build pieces: `off_peak_windows()` on `RateSchedule`; a noon→noon cap accumulator over the existing dispatch-split machinery; the 4-rate classifier; `ev_device_*` rate fetch; per-channel ex-VAT capture (BL-23 follow-on). Recording prerequisite (completed-window capture, `raw_start`/`raw_end`) is **done**. Ships on the **dispatch-derived EV sub-meter** (attribution only, billing-neutral), which can land first. Resolves **#272**.

**Sub-item — historical `completed`-dispatch fetch (90-day) — shared prerequisite.** The cap-day union and the completed-only reprice (§14 / `materialise_completed_only_slots`) both need the `completed` record present in `dispatch_history`, but it only lands while EMT is live inside the provider's **rolling** window (`get_dispatches` ≈ current day). After downtime, or a delete + re-import once the dispatch has aged out, it never arrives — so the slot stays mispriced at peak and the cap-day union is undercounted. Octopus exposes completed dispatches for ~90 days; add a bounded, gated, non-fatal historical fetch (paged over a date range) that backfills `completed` rows on demand (re-import / gap recovery) and/or as a settlement-sweep catch-up.

#### BL-13 — Sub-meter replacement auto-detection
*Medium — resilience.* When a device is replaced and reuses the same sensor entity ID, the cumulative read resets to zero. EMT currently relies on the user to retire the old device and add a new one. Auto-detect a significant mid-block read drop on a sub-meter and prompt the user — similar to the main-meter reset detection added in 2.9.0.

#### BL-14 — Per-device weighted generation mix in Usage Insights
*Low — insight accuracy.* The generation-mix bar in Carbon Insights shows the grid-wide period average. A device-specific weighted mix (e.g. a battery that charges overnight when the grid is cleaner) would be more accurate and informative.

#### BL-15 — Historical rate correction from bill
*Low.* Populate missing or incorrect rate data from a supplier PDF — useful when the rate sensor was misconfigured for a period and block costs are wrong even though kWh figures are correct.

#### BL-16 — Surface Kraken API deprecations in the UI  ·  *bus-factor insurance*
*Low urgency, high long-term value. Cheap now; impossible to add later when it's needed.*

EMT already detects deprecated Kraken fields each poll (`check_field_deprecations`), but the signal only reaches a log line and `sensor.energy_meter_tracker_api_deprecations` — **neither of which a normal user sees**. Today the only person who would notice Octopus changing the schema is the maintainer, reading logs. If the maintainer becomes less active, EMT drifts silently toward breaking and nobody knows until billing goes wrong.

**The point is not to inform users — it is to make a stranger open an issue.** Design accordingly:
- **Name the affected fields.** "2 deprecations detected" is unactionable; the field names are what gets pasted into a report.
- **Link directly to the issue tracker**, ideally a pre-filled new-issue URL. The gap between "I saw a warning" and "I filed a report" is where this dies.
- **Persistent, dismissed per-deprecation-set** (not per session). If it vanishes on reload, nobody reports it.
- **Must not depend on `publish_ha_sensors`.** Today the only entity EMT publishes is the deprecations sensor, gated behind that option — which is itself slated for removal (BL-17). Once this notification exists, the sensor and the option can both go.

Feed the existing detection into the BL-6 notification region (persistent, `warn` level).

**Done — the maintainer-facing half (CI early warning).** A scheduled GitHub Action (`.github/workflows/graphql-deprecations.yml` + `.build/check_graphql_deprecations.py`) now introspects the live Kraken schema weekly and opens/updates `graphql-deprecation` issues for any field EMT selects that Octopus has flagged deprecated — mirroring BCD's `checkGraphqlDeprecations`. It reuses the same field sets as `check_field_deprecations` (extracted statically via `ast`, so one source of truth), needs no secret (introspection is unauthenticated), and fails loudly if the schema can't be fetched. This closes the "make a stranger — or the maintainer — see it" gap for the *repo* side. **Still to do:** the *user-facing* half — surface the same detection in the app (BL-6 notification region) so a running instance warns its owner, per the design above.

**Known limitation, worth stating:** the check compares the live schema against a hardcoded list of the fields EMT uses. If Octopus *removes* a field outright rather than marking it deprecated, or changes how deprecation is signalled, the detector goes quiet — and a silent detector is worse than none, because it looks like everything is fine.

#### BL-17 — Stop the carbon backfill re-hitting the API for known-empty slots
*Low — efficiency, no data impact.* When the Carbon Intensity API has **no regional data** for a date/region (confirmed empty `data:[]`, not a transient outage), those NULL-carbon blocks stay eligible forever, so the historical backfill re-fetches them on every ~24h cadence permanently — a small but perpetual no-op (observed: a handful of whole-day gaps, e.g. 20–22 Oct 2023 and 13 Jan 2025 for `DE65`, where the source simply has nothing). Fix: record a per-`(date, region)` **confirmed-empty tombstone** after a fetch returns empty for a slot old enough that data won't appear (i.e. not "still settling"), and skip those in `get_missing_carbon_date_range` / the window fetch — while still clearing the tombstones if the region changes or on an explicit "re-scan carbon" action, so a genuine later backfill upstream isn't permanently foreclosed. Distinguish **confirmed-empty** (source has none) from **transient/thin** (retry as today). Optional companion: a national-intensity fallback (flagged approximate) for slots the regional endpoint never provides.

#### BL-20 — Auto-resolve ambiguous dispatch slots from the API's billed cost (fewer review prompts)  ·  *follow-up to #322*
*Medium — accuracy + UX.* The dispatch reconciliation decides a smart-charge slot's off-peak/peak **rate** from the *local dispatch lifecycle* (planned / started / completed signals). When a slot is `completed but not started` with substantial energy — typical of an **Ohme replan that didn't actually charge** — the signals are genuinely ambiguous, so EMT leaves the price unchanged and flags it for the user to check against the bill. #322 made those flags **dismissible-for-good**; this item reduces how many are raised at all. The review flag is already gated on the block being **DCC-settled** (`imp_kwh_api` present), which means Octopus has an authoritative **billed cost** for that exact half-hour — and that cost already encodes whether IOG charged it off-peak. So: for a settled slot the heuristic can't decide, **fetch the real billed cost via the Measurements cost-recovery path** (`recover_measurement_costs`, already built) and set the rate from it automatically; only fall back to a human `review` flag when the API genuinely can't answer (cost still missing after retry). Turns "here are 9 blocks, check them against your bill" into "priced from your actual bill" for the common case. Keep the local heuristic as the fast pre-settlement path (dispatch `completed` lands hours before DCC settlement) — this only changes what happens at settlement time for the ambiguous ones. Validate that the billed-cost-derived rate matches settled billing (the reconcile must not change billing totals).

#### BL-21 — Heal *interior* device zero-holes (dropout mid-live-period)
*Low–Medium — completeness of the zero-hole heal.* Attribution now heals a device sub-meter that flat-lined at zero at the **start** of its live period: the join seam skips leading zero blocks and the write path overwrites a zero-hole from the recorder (never a real reading). But a **true interior hole** — real live data on *both* sides of a zero run (e.g. the sub-meter sensor drops out for a week mid-history, then resumes) — is still not reached, because the seam stops at the *first* non-zero live block, before the hole. Fix: drive the fill from "blocks that are absent **or** a zero-hole with recorder energy behind them" across the recorder's covered range, rather than clipping strictly at the seam — the per-block invariants (never overwrite a non-zero row; don't churn a genuine zero) already make walking past the seam safe. Keep the seam as the fast-path bound for the common (no-hole) case so healthy devices don't re-scan their whole live history each run. Also consider a UI signal: a device sub-meter reading flat-0 while the parent house import is elevated is a **suspect gap** worth surfacing rather than silently recording 0. Validate: billing/house totals unchanged; only the device split moves.

#### BL-25 — Render charts in a separate process (stop GIL starvation)
*Medium-high — reliability on large histories.* Chart rendering is CPU/GIL-bound (matplotlib + Python loops over every block). It's already offloaded to a thread executor (`_generate_charts_offloaded`), but a thread doesn't help GIL-bound work: while a render runs over a large history (tens of thousands of blocks it can take minutes) it starves **both** the asyncio event loop that pings the HA WebSocket — heartbeat missed → HA drops the socket → reconnect → startup re-run → render again, a self-perpetuating **reconnect storm** — **and** the waitress request threads (UI polls / chart-PNG fetches back up → "Task queue depth" warnings). The 4.2.x reconnect-debounce (skip the startup re-run while a startup completed recently OR a render is in progress/just finished — `render_recently_active`) *mitigates* the storm but not the underlying starvation. **Fix:** move the render into a separate **process** (subprocess / `multiprocessing`), which has its own GIL, so neither the WebSocket heartbeat nor waitress is affected regardless of history size. The worker already uses a dedicated read-only DB connection, so the data boundary is clean; it mainly needs process spawn/lifecycle + writing the PNGs to `CHART_DIR`. Consider also coalescing the multiple renders a single delete/poll/startup currently triggers into one. *(Surfaced Aug 2026 on a ~56k-block dev DB doing frequent deletes; a normal user re-renders far less often and feels this much less.)*

#### BL-26 — Rate corrections on a capped IOG block: preserve / independently adjust the EV vs house rate  ·  *follow-up to BL-9*
*Medium — correctness on the capped IOG tariff.* The rate-correction tool sets **one rate per meter per block**, and a correction **gates the IOG house/EV split off entirely** — `_apply_iog_split` runs only when `_override_rate is None` (engine.py), and a corrected block sets that override from `rate_corrected`. So correcting a **capped** block flattens its 4-rate breakdown to a single rate: the stored EV/house split (`imp_kwh_ev`/`imp_cost_ev`/`imp_rate_ev`) is dropped for that block, and there is no way to adjust the **EV** rate and the **house** rate independently (the synthetic dispatch EV isn't a separate meter the tool can target). Fix, either: (a) on an IOG block, **re-apply the split after a house-rate correction** so only the house portion moves and the EV portion stays on its `ev_device_*` rate; or (b) let a correction carry a separate **EV-rate and house-rate override** that feed straight into `compute_iog_split`, persisting both. Keep the **settled-cost-is-authoritative** contract — corrections are a pre-settlement / wrong-data tool, so this only changes how a *corrected* capped block is priced. Validate: uncapped / non-IOG corrections byte-identical; a corrected capped block still reconciles EV + house to the grid import, and `rate_reconciled`/dispatch reconciliation still never touches a user-corrected block.

#### BL-27 — Priced-segment pricing model (retire the layered decompositions)  ·  *strategic follow-on to BL-9*
*Medium–High — removes a recurring bug class.* A block's grid import is priced as a **single rate** with the IOG 4-rate **EV/house split** and **ex-VAT** figures bolted on as separately-stored columns — so every reprice path (settlement, reconcile, overlay) must remember to update all of them, and repeatedly hasn't: ex-VAT drifted stale after a revert (fixed), then the EV-split drifted stale the same way (fixed), and device pricing under the cap uses the blended parent rate for all devices (open). One root cause — *two representations that must agree and sometimes don't*. Replace it with **one priced-segment decomposition per block**: a small ordered set of `{kwh, inc_rate, exc_rate, band, attribution}` segments (the 4-rate matrix) that **is** the pricing; `imp_rate`/`imp_cost`/the `imp_*_ev`/`imp_*_exc` columns become **views** over the segments, a device inherits the segment its energy lands in, and a reprice just recomputes the segments — so there is nothing left to keep in sync and the whole drift class cannot recur. Phased/additive migration (introduce segments alongside → backfill → migrate readers → retire the re-stamp/repair machinery), billing-neutral throughout (Σ segments = grid cost). **Validate against a real capped multi-device DB** (EV charger + battery on the cap). Full design: `docs/design/segment_pricing_refactor_design.md`.

#### BL-28 — Charger-derived IOG car/house + cap split for deep history  ·  *low priority · additive · bill-neutral*
*Fallback for history beyond the ~90-day dispatch window (or no Octopus dispatch integration).* The car/house split and the capped cap-boundary rate split come from completed-dispatch records; past ~90 days there are none, so imported history shows correct bills but no split / a blended capped breakdown. Where a physical EV charger (Zappi, Ohme, …) was already recording in HA, its charge profile is a proxy for the missing dispatch: recorder attribution (BL-12) already recovers the **energy** split; this item adds an **approximate 4-rate / cap reconstruction** — walk the noon→noon day accumulating the car's charging against the 6-hour off-peak allowance, price EV off-peak within cap / peak beyond, house freebie while within cap (per `iog_6hr_cap_design.md`). **Approximate by nature** — energy can't distinguish a smart dispatch from a boost (assume off-peak, as for Ohme), cap accounting is re-derived not recorded, recorder LTS is hourly; the **billed total stays authoritative**, so this only improves the breakdown, never the bill. Slots into the segment model's no-dispatch fallback, flagged reconstructed + reversible. Complements BL-9's 90-day dispatch backfill (precise for the recent window; BL-28 is the deep-history estimate). Full design: `docs/design/charger_derived_iog_split_design.md`.

#### BL-29 — User-selectable device legend colours (accessibility)  ·  *UI · additive · display-only · low risk*
*Colour-vision-deficiency accessibility.* Device legend colours are currently **positional** — assigned by config order from a fixed 9-entry `COLOR_PALETTE` (`build_meter_colors_from_config`), so Direct=blue `#1f77b4`, first sub-meter=pink `#e377c2`, second=orange, etc. Some palette neighbours are hard to distinguish for colour-blind users — notably the synthetic-EV purple `#8b5cf6` sits very close to the house blue, making house vs EV hard to tell apart. Add a **per-device colour picker in Meter Config**: store an optional `color` on each meter's meta (config_periods), have `build_meter_colors*` prefer it over the positional palette (fall back to the palette when unset), and honour it everywhere a series/legend colour is drawn (billing day chart, Usage Stats, insights, spiral). Offer a **colour-blind-safe default palette** as a one-click option too. Additive and display-only — cannot change any figure or bill. *Prompted by the 4.4.0 hybrid-EV work: the unified "EV" line now takes its device-index colour rather than a forced purple, which is the interim fix; this item lets the user choose.*

#### BL-30 — Historical Import: pricing-health panel goes stale until manual refresh  ·  *UI · bug · low risk*
*Reported against 4.4.0 delete+refill testing.* On the Historical Import page, the **Pricing health / verify** panel stops updating after a verify completes and is **not restarted when a new verify begins**, so it shows a stale "✓ Up to date / verified in Ns" verdict while a backfill's verify is actually running — only a manual page reload reveals the live "Verifying · N%" progress. Root cause: `loadPricingHealth()` only re-arms its own poll timer `if(vActive)` (historical_import.html ~L1259), and `pollStatus` — which polls the *import* status continuously — calls `loadPricingHealth` only **once** on the import→verify settle handoff (~L1033), never on a transition *into* an active verify. So a verify that starts while the panel shows a prior "done" state is never picked up until `DOMContentLoaded` re-runs the fetch on reload (~L1700). **Fix direction:** drive `loadPricingHealth` from the continuous `pollStatus` loop whenever a job is active/settling (or restart the health poll when `pollStatus` sees an active job and no `_verifyTimer` is armed), so a freshly-started verify surfaces without a manual refresh. Small, JS-only; the file is a delicate state machine (`_freshFlow`/`_pollExpectUntil`/`_settling`/persisted-snapshot guards all guard *other* staleness bugs), so the fix must be tested against the live-verify path before shipping. Totals/data unaffected — display only.

#### BL-31 — Level the IOG integration types: read the plan from wherever it lives (Octopus or the charger)  ·  *provider coverage · re-scoped*
**Ohme plan-from-charger: SHIPPED experimental in 4.5.0** (BL-31 first slice) — `_parse_ohme_slots` (official `slot_list` + dan-r shapes) feeds Ohme's own plan into the card + lifecycle, and a sensor-verified smart slot records `started` (no more OHME review-churn). UNVALIDATED pending an Ohme tester; parser unit-tested. The generic multi-provider adapter below remains backlog.
*Re-scoped after the 4.5.0 online-gate (BL-36) and the Ohme control-model analysis.* The original framing — "add an OHME-style boost sensor per provider" — is mostly obsolete. Two facts change it: (a) **settlement is the correctness floor everywhere**, so no provider path is ever needed for a correct *bill*; and (b) smart-vs-bump is **structural**, keyed on whether Octopus *authored* the charge plan. Octopus authors it only when it **dispatches** the charge (car-connected, or an Octopus-controlled charger) — signalled by `started` (`SMART_CONTROL_IN_PROGRESS`). When the **charger** schedules (so far only **Ohme**, via its deep/native integration), Octopus never enters that state: no `started`, an unreliable superset `planned`, `source=null` completeds — because the plan lives in the *charger's* cloud, not Octopus's. So the job isn't per-brand bump detection; it's **reading the plan from wherever it lives.**

**Classify by `started`, not by brand.** The same hardware is either shape depending on which device the user links to IOG — an Ohme owner who links the *car* is Octopus-dispatched (`started` fires); one who links the *charger* is charger-scheduled (no `started`). So EMT self-classifies per account from `started`-seen; there is no provider allow-list to maintain.

**The resolver — 4 rungs, picked automatically:**

1. **`started` seen** → Octopus authored and dispatched the plan → the standard lifecycle + online-gate (BL-36) handle smart-vs-bump. **No charger integration needed** (Zappi, car integrations, any Octopus-controlled charger).
2. **No `started` + the charger's HA integration exposes its plan/mode** → read the plan *from the charger*: its **planned slots** → `planned`, **slot-active** → `started`, **charge-mode** → the boost veto. Reconstructs the full lifecycle from the charger, so pricing **and** the smart-charging card work — parity with Octopus-dispatched.
3. **No `started` + no usable charger shape** → fall back to **Octopus**: optimistically off-peak the planned superset, `review`-flag the ambiguous, let **settlement** be authoritative. Bill correct; provisional optimistic; no reliable forward card. (Today's `_capture_ohme_slots(sensor_present=False)` behaviour, generalised.)
4. **No Octopus API** (local-sensor mode) → nothing to fall back to; rely on the charger's HA sensors or a physical CT.

**Generalise `_capture_ohme_slots` into a provider-agnostic adapter** parameterised by *(charge-mode entity, boost-value set, charging-status entity, optional planned-slots entity)*. Brands become **config mappings, not code**; a user with any charger points EMT at their entities. Ohme keeps auto-detection but runs on the same engine.

**Ohme gets better, not just fixed.** The `dan-r/HomeAssistant-Ohme` integration exposes **Next Charge Slot Start/End**, **Planned Slots**, and a **Charge Slot Active** binary sensor that *"mimics the Octopus planned/completed dispatches."* EMT currently reads only live mode+status, so the charge card falls back to Octopus's unreliable superset (the "pointless card"). Reading Ohme's own slots restores the forward card **and** feeds rung 2's lifecycle — for a charger-connected Ohme user this is *better* than Octopus-dispatched, because Ohme's plan is authoritative, not a superset. (Planned Slots populate once plugged in — "tonight's plan on plug-in," which is what the card needs.)

**Primary-source finding — OHME is the outlier, everything else is Octopus-controlled.** Checking each vendor's own IOG docs (not comparison guides) flips the earlier read: **Hypervolt** ("Octopus calculates a smart charging schedule… and sends it to your Hypervolt"), **Indra/VCHRGD** ("Octopus controls charging now; your Indra schedules will be disabled"), **Pod Point** ("leave it to Octopus… they will schedule your charge") and **Easee** (where IOG works at all — "disable the smart charging schedule once connected") all put **Octopus** in charge of the plan. So `started` fires and the 4.5.0 online-gate (BL-36) levels them at **rung 1 with no per-provider code**. **OHME is, so far, the only confirmed self-scheduling (delegated) IOG charger** — so rung 2 (read the charger's plan) exists essentially *for OHME*, where its main payoff is the charge-card upgrade, not correctness.

**Per-provider state (primary-source-checked; `started`-seen remains the runtime truth):**

| Provider | IOG control | HA integration | Smart-vs-bump signal | Rung |
|---|---|---|---|---|
| **Zappi / car integrations** | Octopus-controlled | CAD / n/a | Octopus lifecycle (`started`) | **1** |
| **Hypervolt** (Home 3 Pro) | Octopus-controlled — "sends it to your Hypervolt" | gndean | Octopus lifecycle. Its **Boost mode = smart *and* bump**, so **not a veto** | **1** |
| **Indra / VCHRGD** (Kaluza) | Octopus-controlled — "Indra schedules disabled" | guybw IndraSmartPro / KaluzaCharger | Octopus lifecycle | **1** |
| **Pod Point** (Solo 3S) | Octopus-controlled — "leave it to Octopus" | mattrayner | Octopus lifecycle | **1** |
| **Easee** (One) | Octopus-controlled where IOG works; **IOG "not fully supported"** | official `easee` | Octopus lifecycle if dispatching; else no dispatches (dumb ToU) → CT | **1** / n/a |
| **OHME** (Home Pro/ePod) | **self-scheduling (delegated) — the outlier** | dan-r / official `ohme` | **charge-mode (Max/Smart) + Ohme Planned Slots** (Octopus gives no reliable plan) | **2/3** (path built; card upgrade) |
| **Andersen** (A3/Quartz) | Octopus-controlled (likely) | none mature | Octopus lifecycle; no HA fallback if it isn't | **1** / else settlement |

**Zappi is not exempt (shipped 4.5.0, BL-36).** A manual Fast bump returns a completed dispatch with `source=null` and no plan; on a live block the online-gate now prices it peak. A **Zappi Fast** charge-mode sensor stays an *optional* rung-2 corroborator for the rare bump-overlaps-a-plan case — not required.

Complements BL-9 (cap split), BL-28 (deep-history reconstruction), BL-36 (the online-gate this builds on).

#### BL-32 — Charts/Bill freshness token must catch a cost-neutral re-price  ·  *UI · low risk*  ·  **SHIPPED (4.4.0)**
*Fixed: `_blocks_data_version` now appends a `block_segments` rate fingerprint (COUNT + ROUND(SUM(inc_rate),4) + ROUND(SUM(exc_rate),4), guarded for pre-4.4.0 DBs), so any rate-only re-price busts the Charts/Bill cache on the next poll/tab-focus. Verified the fingerprint moves between a scattered and a canonical (re-migrated) DB.*
*Surfaced during 4.4.0 re-migration testing (M1/B6).* The Charts UI gates its auto-refresh on `_blocks_data_version` — `COUNT + MAX(block_start) + SUM(imp_kwh/imp_cost/exp_kwh/exp_cost/carbon_g)` plus the mtimes of `daily_usage.html`/`net_heatmap.html`. A **rate-only re-price** (the first-upgrade migration, or the M1/B6 canonical-rate work) changes segment/displayed **rates** but leaves `imp_cost`/`imp_kwh` byte-identical (the reconciliation invariant), so the DB fingerprint does **not** move; only a chart regen advancing the two mtimes bumps the token, which is incidental and doesn't cover the bill-summary breakdown reliably. Result: after a migration the Bill Summary keeps its cached render until a manual browser refresh (a finalise, which moves cost, refreshes normally). **Fix:** make the token capture a rate/segment change too — cheapest is a `block_segments` rate fingerprint (e.g. `SUM(ROUND(inc_rate,6)*seq)` or a rowid/updated-at max), or have the reprice sweep bump a persistent `reprice_generation` counter the token reads; then any cost-neutral re-price busts the cache and the Charts + Usage-Stats surfaces refresh on the next poll/tab-focus without a hard refresh. Also audit whether the **billing-history** page (no `data-version` poll at all today) should adopt the same gate. Display/UX only — no figure changes.

#### BL-33 — v5.0.0: drop pre‑4.4.0 legacy migrations (tech‑debt removal)  ·  *breaking · scheduled for 5.0.0*
*Announced in 4.4.0 (README upgrade notice + CHANGELOG Deprecated).* 4.4.0 migrates a pre‑segment DB to the priced‑segment model **once** on first run. v5.0.0 assumes that migration has already happened, so the **one‑time** legacy paths are deleted and the code that carries them retired. **Removable at 5.0.0** (each marked `# DEPRECATED — remove in 5.0.0` in the tree): the three dormant backfills `_run_historical_{exc,iog_split,segment}_backfill` + their `_maybe_backfill_historical_*` schedulers; `_detect_upgrade_mode` (the 2.x→3.0 data‑source‑mode bridge) and the 2.x add‑on‑config compatibility in `_kraken_env`; `repair_pence_inc_blocks` + the sweep's pence self‑heal (the 4.3.0 cap‑bug repair); and — via a schema migration — the vestigial **band columns** and the legacy `imp_kwh_ev`/`imp_cost_ev`/`imp_rate_ev` EV‑split columns now that every reader is segment‑based. **Keep** (NOT migration): the reprice‑history sweep + `segments_from_legacy` themselves — CSV/PDF/gap‑fill imports use them ongoing to segment newly‑imported blocks. **Add at 5.0.0:** a version‑floor guard at store‑open — a DB with no `block_segments` table (or below a stored schema‑version stamp) is **refused** with "upgrade through 4.4.x first," rather than silently mis‑reading. Net: a smaller, single‑model codebase with no dead migration branches. Prereq: 4.4.x has been out long enough that direct pre‑4.4.0→5.0.0 jumps are rare (the README/CHANGELOG notice is the reach mechanism for un‑updated users).

#### BL-34 — Settled peak-priced segments keep a stale `band='off_peak'` label  ·  *UI · cosmetic · low risk · scheduled 4.5.0*
*Surfaced during the 23 Aug 2026 prod dispatch review.* On API-settled out-of-window blocks the EV/house segment is priced at the **peak** `inc_rate` (Octopus's settled cost — authoritative) but still carries `band='off_peak'`, so the label doesn't match the rate. Cost is unaffected (the segment rate wins), but the band breakdown / any band-keyed display can mislabel the energy. **Fix:** derive the segment `band` from the applied rate (or clear the stale label when a settled API cost overrides the estimate) so label and rate agree. Display-only; no figure changes.

#### BL-35 — Retain the dispatch lifecycle permanently (remove the 90-day prune)  ·  *data integrity · SHIPPED (4.5.0)*
*The planned/started/completed dispatch history is the canonical ingredient for any future re-price (smart-vs-bump, cap reconstruction) — and unlike Octopus's rolling ~90-day window, EMT must keep it for the life of the DB.* 4.4.0 shipped with `prune_dispatch_slots` / `prune_dispatch_history` deleting both tables at 90 days (Octopus's own amnesia, replicated). Removed the scheduled prune calls from the `engine.py` capture ticks so the lifecycle is retained forever; the `prune_*` defs remain unused (test-covered). Storage is negligible (~thousands of rows/yr on a 70 MB+ DB). Underpins BL-9's cap, BL-28's deep-history reconstruction, and the 4.5.0 online-bump gate. Design: `4.4.0_iog_pricing_and_reprice_design.md` §3d.

#### BL-36 — Online-gated bump detection: `interpolated` distinguishes an out-of-app bump from a missed smart charge  ·  **SHIPPED (4.5.0)**
*The 4.5.0 core.* The settlement reconcile could not tell an out-of-app **bump** (a completed dispatch with no plan/started, billed peak by Octopus) from a genuine smart charge whose plan EMT missed while offline — both are completed-only, and Octopus leaves `source` null. Resolved by reading the block's own **`interpolated`** flag as the "was EMT online?" signal: a **live** block (`interpolated=0`, not imported) means EMT was polling, so an unplanned completed dispatch is a **bump → peak** (freebie withheld, cap untouched); an offline/gap/imported block keeps the optimistic off-peak. Implemented as `_reconcile_decision(..., was_online = not interpolated)`. Fixes the observed Zappi Fast case and the pure car-side EV-integration case (no charge-mode sensor needed). Design: `dispatch_validation_design.md` §14 / §14a.

#### BL-37 — Dispatch-poll heartbeat log — close the meter-up/poller-down bump edge  ·  *follow-up to BL-36 · currently unplanned*
*Refinement to BL-36's online-gate.* A block can be `interpolated=0` (meter/HA live) yet the **dispatch poll specifically** missed heartbeats over that slot (Kraken poll down) — a genuine smart charge whose plan we missed on an otherwise-live block, which the gate then prices peak. **Do:** record a lightweight dispatch-poll heartbeat (poll timestamps / online intervals) so the reconcile can detect a poller gap over a slot and keep the optimistic off-peak only then. **Currently unplanned:** Kraken polling is dependable in practice and settlement is the final authority, so the current pessimistic-but-safe behaviour (peak, self-corrected at settlement) is acceptable.

#### BL-38 — Generation-mix / carbon-intensity plausibility guard + donut↔chart alignment  ·  *display + carbon · SHIPPED (4.5.0)*
*Surfaced during the 4.5.0 soak.* National Grid's **regional** generation mix is a modelled estimate that occasionally emits a degenerate half-hour — one fuel ~100% with a 0 gCO₂/kWh intensity (e.g. "solar 97%" at 21:30) — which EMT stored verbatim, corrupting the **Current Grid Generation Mix** donut, the 48-hour chart, and that block's **carbon** (intensity → block `carbon_g`). Fixed: `_ci_slot_plausible` rejects a glitch slot on fetch (one fuel ≥95% or intensity ≤0) so the previous good slot stands, and the donut now reads the **same `mix_history` latest slot as the chart** (was the lagging block-stamped `generation_mix` with a fragile `LIMIT 9`), so the two can't disagree. Forward-only — no backfill (a re-fetch returns the same upstream glitch; existing bad slots age out of the rolling 48h/4-day stores). Parser unit-tested (`test_ci_plausibility.py`). Display / carbon-view only — no bill effect.

#### BL-39 — Gap-fill EV/house split heals from the late completed dispatch  ·  *attribution · SHIPPED (4.5.0)*
*Surfaced during the 4.5.0 soak — dev was offline over an evening charge while prod_dev (live) was correct on the same code.* An outage gap-fills the missed blocks from the API, and the EV share could be stamped from an incomplete/planned fragment (e.g. `imp_kwh_ev=0.146` when the completed dispatch shows 1.61 kWh) and then **never corrected**, because the back-attribution only healed a **NULL** split — so the charge showed as house/Direct, not EV. `_attribute_missing_ev_split` now also re-attributes a **gap-filled** (`interpolated=1`) block whose EV kWh **materially disagrees** (>0.1 kWh) with its completed dispatch, re-deriving the grid-clipped split + segments. Runs every reconcile pass, so an outage **self-heals** once the retained (BL-35) completed dispatch lands; idempotent; interpolated-only, `rate_corrected`-safe, uncapped. The interim EV-column re-stamp (BL-27/segments drift) is retired when the 5.0.0 aggregation-unify ships.

#### BL-40 — EV device typed 'ev' (config UI) not recognised by the hybrid EV gate → double EV line  ·  *attribution · SHIPPED (4.5.1)*
*Reported by an Indra Smart Pro user — the charger added as an "EV Charger" device **and** the Octopus dispatch provider, so it double-counted against the synthetic "EV (from dispatch)".* Root: the config UI writes `meter_type='ev'`, but the physical-EV identity gates (`_ev_meter_id` in `energy_charts.py`; the coverage gate + insights-carbon gate in `server.py`) hard-checked `== 'ev_charger'` with only a meter-id 'ev'/'charger' **substring** fallback. A UI-added device has a hashed `sub_meter_<id>` (no substring) + type `ev` → matched neither → never fed to `_hybrid_ev_by_block` → the synthetic couldn't supersede it → **two EV lines**. Latent since the synthetic-EV/hybrid coverage gate landed (~4.1.x); masked because every prior test/dev device had a canonical id (e.g. Zappi `ev_charger`). Fix: gates accept `meter_type in ('ev','ev_charger')` — matching what the device-list, Usage-Stats and Insights code already did. Display/attribution only, no stored-data change. Future-proofs any Octopus-controlled charger added as a device (Hypervolt, Pod Point, VCHRGD, …).


#### BL-42 — API/Mini sub-meter device split skipped on the boundary-finalise path (`load_current_block` drops config meta) → Usage Stats double-count on provisional days  ·  *display · SHIPPED (4.5.2)*
*Reported by the Indra + Fox-battery user (Octopus Home Mini, `data_source_mode=api`); most recent 1-2 unsettled days showed EV + battery counted inside "Direct" **and** on their own lines (~15 kWh over), while his CAD prod was correct on identical code.* Root: `load_current_block` reconstructs the in-progress block's meters from `current_reads` with **empty meta**, so `finalise_block`/`_apply_pass2` see `sub_meter=False`/`parent_meter=None` and the grid-authoritative device split no-ops -> `imp_kwh_remainder` left NULL. Masked on CAD (`capture_samples` re-stamps meta each tick before the block closes) and on settled blocks (settlement rebuilds via the `get_block_dict` join, which maps `parent_meter_id -> meta.parent_meter`); bites only the API/Mini boundary-finalise path with sub-meters present. Traced 3.2.0->4.4.0: split/finalise/mini machinery byte-identical — **not a release regression**; latent since >=3.2.0, surfaced when the Mini reconnected and began driving finalisation. Fix: `load_current_block` repopulates meta from `config_from_db`. Forward fix; broken provisional blocks self-heal at DCC settlement. Regression test: `tests/test_current_block_meta.py`.

---

### To triage
- *(nothing outstanding)*

---

### Longer-term themes (unversioned)

**API polish** — per-meter colour customisation (bidirectional-aware), API versioning. *Carried forward from the pre-3.0 "clean break from legacy" theme. (The `migrate_json_to_sqlite` legacy migration shim was removed in 4.0.0.)*

**Bright (Hildebrand / Glow) API as a DCC consumption source** — use `api.glowmarkt.com` as an additional source of DCC half-hourly consumption, giving non-Octopus GB smart-meter owners the same block-resolution data. Pluggable alongside Kraken, feeding the same `imp_kwh_api`/`exp_kwh_api` settlement path (and BL-8 backfill). *Until this lands, a FIT owner can bridge the export gap manually: 4.2.0's overlay-a-single-channel CSV backfill imports a Glowmarkt/Bright export CSV onto a range that already holds import — this API source would automate exactly that.*

**Non-Octopus Kraken suppliers (EDF, etc.)** — EDF runs on Kraken, so an EDF account is reachable through nearly the same GraphQL API. Scoped, shovel-ready plan (auth strategy + consumption route, behind a `SupplierProfile` seam; Octopus unchanged) in **`docs/edf_supplier_support_design.md`**. Blocked only on a real EDF account for bill reconciliation — no EMT tester yet, so unvalidated. Endpoint + auth quirks (email/password → `regenerateSecretKey`, which invalidates existing keys) captured in the note.

**Gas meters** — extend the engine to gas alongside electricity. Needs a design spike: m³/ft³ with calorific-value conversion, different billing periods, slower sensor updates.

---

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