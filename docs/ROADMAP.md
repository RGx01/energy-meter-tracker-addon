# Roadmap

Newest at the top. **Upcoming work (ranked by priority) first, then release history newest-first** (v3 → v2 → v1, oldest at the very bottom). Scope and timing subject to change.

---

## Upcoming — ranked by priority

### 4.x — Unify the billing / usage-stats aggregation onto one endpoint

*Consolidation, deferred out of 4.0.x deliberately because it touches billing (and not yet picked up in 4.1.x / 4.2.0).* There are today **three** independent implementations that aggregate the same per-block data: the **Billing chart** (`calculate_billing_summary_for_period` in `energy_charts.py`, server-rendered into `daily_usage.html`), **Usage Stats daily/monthly/yearly** (`/api/charts/blocks-summary`), and **Usage Stats HH** (`/api/charts/blocks-day`). The 4.0.x HH work already collapsed the *two Usage Stats endpoints* onto shared helpers (`_aggregate_block_rows` + `_bucket_to_row_values` in `server.py`) — proven byte-identical by a golden-output capture and kept honest by `test_usage_stats_vs_billing.py`. **What remains is the harder unification: fold the Billing chart's aggregation and the Usage Stats endpoints onto a single source of truth**, so "billing" and "usage stats" can never drift apart by construction rather than by a cross-check test.

**Hard constraint (non-negotiable, carried from the 4.0.x work):** the results must not change. A great deal of effort has gone into making billing as accurate as possible and into making every other aggregation *align* with billing. Any refactor here MUST be pure code-motion — identical outputs — or it doesn't ship. Method to follow (the one used for the HH refactor): capture the current Billing-chart and endpoint JSON as a **golden baseline**, refactor, assert **byte-identical** output, run `test_usage_stats_vs_billing` + `test_server` + the billing-chart render smoke test; ship only if the diff is empty, otherwise revert.

**Shape of work (to be spec'd):** (1) decide the single home for the block→period aggregation — most likely `calculate_billing_summary_for_period` becomes the canonical primitive and the two endpoints call *it* (rather than the reverse), since billing is the authoritative one; (2) reconcile the row/column shapes the three consumers expect (the endpoints emit a per-bucket JSON shape with rate-keyed sub-meter subtraction, carbon split, avg-intensity; the billing chart consumes a summary object) behind one core with thin per-consumer adapters; (3) golden-proof each consumer independently before deleting any duplicated code. *Medium–large; gated on a design spike, not a quick win — the value is eliminating drift risk, so correctness proof is the whole job.*

### Post-4.0.0 follow-ups (remaining threads from the 3.5.x / 4.0.0 import + guard work)

*Most of the original 3.5.1 list shipped in 4.0.0 — historical-import correctness, tariff-schedule standing charges, the HTTP-400 old-agreement fix, the #307 §1+§3 plausibility guard + self-heal, region-correct historical carbon, and the backup-creation / Delete-Blocks background jobs. These two are what's left.*

#### Deprecation-CI drift guard — remaining hardening  ·  *maintainer tooling*
Registered in 4.0.0: the Measurements query fields + filter enums, the rate-limit pacing query and IOG `currentState`, plus a completeness test that AST-parses every GraphQL query and fails if a selected field isn't covered. **Still to do:** (1) **type-scoped matching** (`_EMT_GRAPHQL_TYPED_FIELDS`) for the Relay-connection generics still in the test's allowlist (`edges`/`node`/`value`/`unit`/`pageInfo`/`hasNextPage`/`endCursor`/`limit`/`ttl`) so they're covered without bare-name false positives; (2) note in the doc that the check is **GraphQL-only** — the import's **REST** dependencies (`get_account` postcode/tenancy/address, `get_consumption`) and the external **Carbon Intensity API** are out of scope and would want their own light shape/contract guard.

#### #307-b — Sensor-semantic reset model + user-declared reset boundary  ·  *fix / enabler*  ·  *follow-up to #307*
*The accurate half of #307, split out because it needs a config surface.* The plausibility guard clamps an impossible delta to **0**; the *better* repair is register **continuity** — when a cumulative sensor's opener is lost, re-base it to the **previous block's `read_end`** and book the true (0-or-plausible) delta instead of discarding it. But that's only safe if EMT knows the channel is a **cumulative lifetime register** (opener must equal previous closer) versus a **session-energy sensor** (e.g. `zappi_charge_added_session`) that *legitimately* resets to 0 each plug-in — applying the continuity rule to the latter would "repair" a real reset. Two parts: **(a)** make the semantic **explicit per channel** — a `cumulative | session` flag on the meter/sensor config, derived where possible but overridable — and drive the continuity/reset logic off it instead of today's implicit time-separation-between-charges luck; **(b)** a **setup option to declare a reset**: "this sensor was reset / replaced at block/date XYZ", so a genuine meter swap or firmware reset is recorded as an intentional boundary (not a glitch) and the continuity check re-bases from there rather than flagging or mis-clamping. Reuses the effective-dated pattern (like region-periods / config-periods). Lets a user fix the rare real reset EMT can't infer, and unlocks the accurate previous-read recompute for #307. *Small–medium; not urgent — the #307 clamp already prevents the damage.*

---

### Prioritised backlog — unscheduled (highest first)

#### BL-9 — IOG 6-hour EV-cap billing  ·  [#272]  ·  **model confirmed — ready to build**
*High impact.* Step 2 of the IOG time-of-use tariff (step 1 — day/night general rates — shipped in 3.1.5). The 4-rate / 6-hour-cap model is **confirmed against Octopus's published material** and is no longer blocked — full design in `docs/design/iog_6hr_cap_design.md`.

**Model:** bill on main-meter grid import; **EV = the `completed`-dispatch delta, grid-clipped**; **house = import − EV** (physical charger meters stay indicative — billing is always dispatch-derived, so it works for any charger). **Cap = union of `completed` dispatch windows per noon→noon (local) day**; the EV portion bills `ev_device_off_peak` within 6 h and `ev_device_peak` beyond (or on Boost). House gets the guaranteed off-peak window (from the rate schedule, not hard-coded) always, **plus** the off-peak *freebie* on out-of-window dispatch slots **while within** the cap — withdrawn once over-cap. Enabled by **tariff-code detection** (no manual toggle); the cap is a **live/provisional predictor** and **settled billed cost is authoritative**, so estimates self-correct at settlement. Build pieces: `off_peak_windows()` on `RateSchedule`; a noon→noon cap accumulator over the existing dispatch-split machinery; the 4-rate classifier; `ev_device_*` rate fetch; per-channel ex-VAT capture (BL-23 follow-on). Recording prerequisite (completed-window capture, `raw_start`/`raw_end`) is **done**. Ships on the **dispatch-derived EV sub-meter** (attribution only, billing-neutral), which can land first. Resolves **#272**.

**Sub-item — historical `completed`-dispatch fetch (90-day) — shared prerequisite.** The cap-day union and the completed-only reprice (§14 / `materialise_completed_only_slots`) both need the `completed` record present in `dispatch_history`, but it only lands while EMT is live inside the provider's **rolling** window (`get_dispatches` ≈ current day). After downtime, or a delete + re-import once the dispatch has aged out, it never arrives — so the slot stays mispriced at peak and the cap-day union is undercounted. Octopus exposes completed dispatches for ~90 days; add a bounded, gated, non-fatal historical fetch (paged over a date range) that backfills `completed` rows on demand (re-import / gap recovery) and/or as a settlement-sweep catch-up.

#### BL-28 — Charger-derived IOG car/house + cap split for deep history  ·  *low priority · additive · bill-neutral*
*Fallback for history beyond the ~90-day dispatch window (or no Octopus dispatch integration).* The car/house split and the capped cap-boundary rate split come from completed-dispatch records; past ~90 days there are none, so imported history shows correct bills but no split / a blended capped breakdown. Where a physical EV charger (Zappi, Ohme, …) was already recording in HA, its charge profile is a proxy for the missing dispatch: recorder attribution (BL-12) already recovers the **energy** split; this item adds an **approximate 4-rate / cap reconstruction** — walk the noon→noon day accumulating the car's charging against the 6-hour off-peak allowance, price EV off-peak within cap / peak beyond, house freebie while within cap (per `iog_6hr_cap_design.md`). **Approximate by nature** — energy can't distinguish a smart dispatch from a boost (assume off-peak, as for Ohme), cap accounting is re-derived not recorded, recorder LTS is hourly; the **billed total stays authoritative**, so this only improves the breakdown, never the bill. Slots into the segment model's no-dispatch fallback, flagged reconstructed + reversible. Complements BL-9's 90-day dispatch backfill (precise for the recent window; BL-28 is the deep-history estimate). Full design: `docs/design/charger_derived_iog_split_design.md`.

#### BL-31 — Extend synthetic-EV reconstruction to more IOG chargers (Hypervolt, Indra, Easee, …)  ·  *additive · provider coverage*
*Prompted by the 4.4.0 synthetic-EV work.* The dispatch-derived EV split is proven only where Octopus hands EMT a usable **completed-dispatch** record — Octopus-controlled chargers (**Myenergi Zappi**) and car-side **EV integrations** — plus **OHME**, which is charger-scheduled (Octopus doesn't control it, `source=null`, planned = a wiped superset) and so earns a bespoke capture path gated on its HA charge-mode sensor (`_capture_ohme_slots`, official `ohme` select or dan-r binary). Other IOG-eligible chargers that are likewise **not** Octopus-controlled — **Hypervolt, Indra, Easee, Andersen, Pod Point, VCHRGD** — currently reconstruct nothing: `get_dispatches` returns no usable completed dispatches, there is no provider-specific fallback, and EMT degrades silently to "no synthetic EV" (the split then relies on a physical CT/sub-meter, or the car's charging lumps into Direct import; an out-of-window smart charge may also read at the day rate rather than the off-peak Octopus applied). **Do:** go round the loop of the current IOG provider list, characterise how each reports dispatches (is `source` populated? Octopus-controlled? is there a usable HA integration exposing charge state / smart-vs-boost?), and add per-provider capture paths **mirroring the OHME model** — gate on a real HA charge-state signal where one exists, fall back to the bounded-optimistic OHME path where it doesn't. Additive and billing-authoritative-safe: capture only feeds the display split and the off-peak overlay, which stays meter-draw-validated. **Also verify** the out-of-window pricing question — whether an unrecognised smart charge is over-stated at the day rate or corrected at settlement — and fix if needed. Complements BL-28 (deep-history charger-derived reconstruction) and BL-9 (the dispatch-derived cap split).

#### BL-25 — Render charts in a separate process (stop GIL starvation)
*Medium-high — reliability on large histories.* Chart rendering is CPU/GIL-bound (matplotlib + Python loops over every block). It's already offloaded to a thread executor (`_generate_charts_offloaded`), but a thread doesn't help GIL-bound work: while a render runs over a large history (tens of thousands of blocks it can take minutes) it starves **both** the asyncio event loop that pings the HA WebSocket — heartbeat missed → HA drops the socket → reconnect → startup re-run → render again, a self-perpetuating **reconnect storm** — **and** the waitress request threads (UI polls / chart-PNG fetches back up → "Task queue depth" warnings). The 4.2.x reconnect-debounce (skip the startup re-run while a startup completed recently OR a render is in progress/just finished — `render_recently_active`) *mitigates* the storm but not the underlying starvation. **Fix:** move the render into a separate **process** (subprocess / `multiprocessing`), which has its own GIL, so neither the WebSocket heartbeat nor waitress is affected regardless of history size. The worker already uses a dedicated read-only DB connection, so the data boundary is clean; it mainly needs process spawn/lifecycle + writing the PNGs to `CHART_DIR`. Consider also coalescing the multiple renders a single delete/poll/startup currently triggers into one. *(Surfaced Aug 2026 on a ~56k-block dev DB doing frequent deletes; a normal user re-renders far less often and feels this much less.)*

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

#### BL-7 — Sensor-vs-device-type sanity check
*Low-medium — prevents a class of silent misattribution.* At device creation, warn when the assigned sensor looks inconsistent with the device type (e.g. an EV device pointed at a battery/inverter sensor). Surfaced by the forum device-usage-swap case, where an "Indra Smart Pro" EV device read the Fox battery's register for a week before it was noticed. A creation-time heuristic (entity-ID / device-class / magnitude sanity) that prompts the user to confirm would catch it before it corrupts history.

#### BL-13 — Sub-meter replacement auto-detection
*Medium — resilience.* When a device is replaced and reuses the same sensor entity ID, the cumulative read resets to zero. EMT currently relies on the user to retire the old device and add a new one. Auto-detect a significant mid-block read drop on a sub-meter and prompt the user — similar to the main-meter reset detection added in 2.9.0.

#### BL-2 — Phantom export channel from the gap-seed
*Low — cosmetic, never affects billing (kWh = 0).* In a no-export `api` setup the gap-seed carries a 0-kWh export channel forward, so the daily billing chart draws a 12p export-rate line despite zero actual export. Root fix: don't carry forward an export channel that has no configured source and never carried real export.

#### BL-3 — Carry billing-chart rate lines forward as a step
*Low — the agreed near-term fix for BL-2's visible symptom.* Build the import/export rate series by forward-filling the last-known rate (a tariff is in force all day) instead of zero-filling per slot; back-fill leading slots and use a None sentinel so a genuine 0 rate stays distinct from "no data". Exact for a flat tariff; the per-slot schedule-resolve variant is the upgrade path for a varying Agile Outgoing export tariff.

#### BL-30 — Historical Import: pricing-health panel goes stale until manual refresh  ·  *UI · bug · low risk*
*Reported against 4.4.0 delete+refill testing.* On the Historical Import page, the **Pricing health / verify** panel stops updating after a verify completes and is **not restarted when a new verify begins**, so it shows a stale "✓ Up to date / verified in Ns" verdict while a backfill's verify is actually running — only a manual page reload reveals the live "Verifying · N%" progress. Root cause: `loadPricingHealth()` only re-arms its own poll timer `if(vActive)` (historical_import.html ~L1259), and `pollStatus` — which polls the *import* status continuously — calls `loadPricingHealth` only **once** on the import→verify settle handoff (~L1033), never on a transition *into* an active verify. So a verify that starts while the panel shows a prior "done" state is never picked up until `DOMContentLoaded` re-runs the fetch on reload (~L1700). **Fix direction:** drive `loadPricingHealth` from the continuous `pollStatus` loop whenever a job is active/settling (or restart the health poll when `pollStatus` sees an active job and no `_verifyTimer` is armed), so a freshly-started verify surfaces without a manual refresh. Small, JS-only; the file is a delicate state machine (`_freshFlow`/`_pollExpectUntil`/`_settling`/persisted-snapshot guards all guard *other* staleness bugs), so the fix must be tested against the live-verify path before shipping. Totals/data unaffected — display only.

#### BL-29 — User-selectable device legend colours (accessibility)  ·  *UI · additive · display-only · low risk*
*Colour-vision-deficiency accessibility.* Device legend colours are currently **positional** — assigned by config order from a fixed 9-entry `COLOR_PALETTE` (`build_meter_colors_from_config`), so Direct=blue `#1f77b4`, first sub-meter=pink `#e377c2`, second=orange, etc. Some palette neighbours are hard to distinguish for colour-blind users — notably the synthetic-EV purple `#8b5cf6` sits very close to the house blue, making house vs EV hard to tell apart. Add a **per-device colour picker in Meter Config**: store an optional `color` on each meter's meta (config_periods), have `build_meter_colors*` prefer it over the positional palette (fall back to the palette when unset), and honour it everywhere a series/legend colour is drawn (billing day chart, Usage Stats, insights, spiral). Offer a **colour-blind-safe default palette** as a one-click option too. Additive and display-only — cannot change any figure or bill. *Prompted by the 4.4.0 hybrid-EV work: the unified "EV" line now takes its device-index colour rather than a forced purple, which is the interim fix; this item lets the user choose.*

#### BL-4 — PDF export overhaul (pagination + orientation)
*Low — usability, no data impact.* The PDF export relies on the browser's print-to-PDF and doesn't paginate: every chart is forced A4 landscape as fixed-size images that can't reflow or break across pages. **Billing is the worst case** (multiple per-period plots + tables, effectively unprintable in portrait). Fix is a print-layout rethink: portrait-first per-view orientation via a single `@page` rule; real pagination (`break-inside: avoid`, `break-before: page`, `thead` repeated via `display: table-header-group`); reflowable capture sized to page width; optionally move generation server-side (WeasyPrint/reportlab) if the CSS-print route proves too limited.

#### BL-14 — Per-device weighted generation mix in Usage Insights
*Low — insight accuracy.* The generation-mix bar in Carbon Insights shows the grid-wide period average. A device-specific weighted mix (e.g. a battery that charges overnight when the grid is cleaner) would be more accurate and informative.

#### BL-15 — Historical rate correction from bill
*Low.* Populate missing or incorrect rate data from a supplier PDF — useful when the rate sensor was misconfigured for a period and block costs are wrong even though kWh figures are correct.

#### BL-17 — Stop the carbon backfill re-hitting the API for known-empty slots
*Low — efficiency, no data impact.* When the Carbon Intensity API has **no regional data** for a date/region (confirmed empty `data:[]`, not a transient outage), those NULL-carbon blocks stay eligible forever, so the historical backfill re-fetches them on every ~24h cadence permanently — a small but perpetual no-op (observed: a handful of whole-day gaps, e.g. 20–22 Oct 2023 and 13 Jan 2025 for `DE65`, where the source simply has nothing). Fix: record a per-`(date, region)` **confirmed-empty tombstone** after a fetch returns empty for a slot old enough that data won't appear (i.e. not "still settling"), and skip those in `get_missing_carbon_date_range` / the window fetch — while still clearing the tombstones if the region changes or on an explicit "re-scan carbon" action, so a genuine later backfill upstream isn't permanently foreclosed. Distinguish **confirmed-empty** (source has none) from **transient/thin** (retry as today). Optional companion: a national-intensity fallback (flagged approximate) for slots the regional endpoint never provides.

#### BL-20 — Auto-resolve ambiguous dispatch slots from the API's billed cost (fewer review prompts)  ·  *follow-up to #322*
*Medium — accuracy + UX.* The dispatch reconciliation decides a smart-charge slot's off-peak/peak **rate** from the *local dispatch lifecycle* (planned / started / completed signals). When a slot is `completed but not started` with substantial energy — typical of an **Ohme replan that didn't actually charge** — the signals are genuinely ambiguous, so EMT leaves the price unchanged and flags it for the user to check against the bill. #322 made those flags **dismissible-for-good**; this item reduces how many are raised at all. The review flag is already gated on the block being **DCC-settled** (`imp_kwh_api` present), which means Octopus has an authoritative **billed cost** for that exact half-hour — and that cost already encodes whether IOG charged it off-peak. So: for a settled slot the heuristic can't decide, **fetch the real billed cost via the Measurements cost-recovery path** (`recover_measurement_costs`, already built) and set the rate from it automatically; only fall back to a human `review` flag when the API genuinely can't answer (cost still missing after retry). Turns "here are 9 blocks, check them against your bill" into "priced from your actual bill" for the common case. Keep the local heuristic as the fast pre-settlement path (dispatch `completed` lands hours before DCC settlement) — this only changes what happens at settlement time for the ambiguous ones. Validate that the billed-cost-derived rate matches settled billing (the reconcile must not change billing totals).

#### BL-21 — Heal *interior* device zero-holes (dropout mid-live-period)
*Low–Medium — completeness of the zero-hole heal.* Attribution now heals a device sub-meter that flat-lined at zero at the **start** of its live period: the join seam skips leading zero blocks and the write path overwrites a zero-hole from the recorder (never a real reading). But a **true interior hole** — real live data on *both* sides of a zero run (e.g. the sub-meter sensor drops out for a week mid-history, then resumes) — is still not reached, because the seam stops at the *first* non-zero live block, before the hole. Fix: drive the fill from "blocks that are absent **or** a zero-hole with recorder energy behind them" across the recorder's covered range, rather than clipping strictly at the seam — the per-block invariants (never overwrite a non-zero row; don't churn a genuine zero) already make walking past the seam safe. Keep the seam as the fast-path bound for the common (no-hole) case so healthy devices don't re-scan their whole live history each run. Also consider a UI signal: a device sub-meter reading flat-0 while the parent house import is elevated is a **suspect gap** worth surfacing rather than silently recording 0. Validate: billing/house totals unchanged; only the device split moves.

#### BL-26 — Rate corrections on a capped IOG block: preserve / independently adjust the EV vs house rate  ·  *follow-up to BL-9*
*Medium — correctness on the capped IOG tariff.* The rate-correction tool sets **one rate per meter per block**, and a correction **gates the IOG house/EV split off entirely** — `_apply_iog_split` runs only when `_override_rate is None` (engine.py), and a corrected block sets that override from `rate_corrected`. So correcting a **capped** block flattens its 4-rate breakdown to a single rate: the stored EV/house split (`imp_kwh_ev`/`imp_cost_ev`/`imp_rate_ev`) is dropped for that block, and there is no way to adjust the **EV** rate and the **house** rate independently (the synthetic dispatch EV isn't a separate meter the tool can target). Fix, either: (a) on an IOG block, **re-apply the split after a house-rate correction** so only the house portion moves and the EV portion stays on its `ev_device_*` rate; or (b) let a correction carry a separate **EV-rate and house-rate override** that feed straight into `compute_iog_split`, persisting both. Keep the **settled-cost-is-authoritative** contract — corrections are a pre-settlement / wrong-data tool, so this only changes how a *corrected* capped block is priced. Validate: uncapped / non-IOG corrections byte-identical; a corrected capped block still reconciles EV + house to the grid import, and `rate_reconciled`/dispatch reconciliation still never touches a user-corrected block.

#### BL-27 — Priced-segment pricing model (retire the layered decompositions)  ·  *strategic follow-on to BL-9*
*Medium–High — removes a recurring bug class.* A block's grid import is priced as a **single rate** with the IOG 4-rate **EV/house split** and **ex-VAT** figures bolted on as separately-stored columns — so every reprice path (settlement, reconcile, overlay) must remember to update all of them, and repeatedly hasn't: ex-VAT drifted stale after a revert (fixed), then the EV-split drifted stale the same way (fixed), and device pricing under the cap uses the blended parent rate for all devices (open). One root cause — *two representations that must agree and sometimes don't*. Replace it with **one priced-segment decomposition per block**: a small ordered set of `{kwh, inc_rate, exc_rate, band, attribution}` segments (the 4-rate matrix) that **is** the pricing; `imp_rate`/`imp_cost`/the `imp_*_ev`/`imp_*_exc` columns become **views** over the segments, a device inherits the segment its energy lands in, and a reprice just recomputes the segments — so there is nothing left to keep in sync and the whole drift class cannot recur. Phased/additive migration (introduce segments alongside → backfill → migrate readers → retire the re-stamp/repair machinery), billing-neutral throughout (Σ segments = grid cost). **Validate against a real capped multi-device DB** (EV charger + battery on the cap). Full design: `docs/design/segment_pricing_refactor_design.md`.

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