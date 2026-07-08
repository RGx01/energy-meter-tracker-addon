# Changelog

## [3.1.5] — 2026-07-08

### Fixed

- **New Intelligent Octopus 6-hour-cap tariff — general-usage rates now supported** ([#1708](https://github.com/BottlecapDave/HomeAssistant-OctopusEnergy/issues/1708)). Following the fail-loud guard below, EMT now reads the new tariff's split rate buckets: when `standard-unit-rates` is absent it fetches `day-unit-rates` + `night-unit-rates` and merges them into the day/night time-of-use schedule, so a migrated meter is **billed correctly for general usage** instead of flagged unsupported. The EV-device rates and the daily 6-hour off-peak cap are **not** applied yet — Octopus hasn't finalised those rules — so dispatched EV charging keeps using the general off-peak (night) rate in the interim (a bounded approximation, documented in `docs/iog_6hr_cap_design.md`). Full EV-cap support follows once OE confirms the mechanics.
- **Fail loud on the new Intelligent Octopus 6-hour-cap tariff instead of silently mispricing** ([#1708](https://github.com/BottlecapDave/HomeAssistant-OctopusEnergy/issues/1708)). Octopus is migrating IOG customers to a new time-of-use tariff (`IOG-SMB-TOU-…`) that **drops the `standard-unit-rates` link** in favour of separate `day` / `night` / `ev_device_peak` / `ev_device_off_peak` rates. EMT reads `standard-unit-rates`, so a migrated meter would get an empty rate schedule and be **silently priced at £0**. EMT now detects this — an active import tariff that returns no standard unit rates — logs a prominent error and surfaces a red **"⚠ Tariff not supported — rates unavailable"** indicator, so the gap is visible rather than producing a wrong-but-plausible bill. Full support for the new tariff (the four rate buckets and the daily 6-hour off-peak cap) is separate, upcoming work; this release makes the gap safe, not silent.

## [3.1.4] — 2026-07-08

### Fixed

- **Billing chart showed the same total for every month** ([#271](https://github.com/RGx01/energy-meter-tracker-addon/issues/271)). In Month (and Quarter/Year) view, every period's headline "Total Bill" displayed the identical figure — the whole-dataset net — even though each period's breakdown was correct and different. The per-period total was computed from the first/last of *all* blocks rather than the blocks in the selected period, so `compute_period_net` always ran over the entire history. It now bounds the total to the selected period. The per-meter/per-rate breakdown was already correct; only the headline total and the month-dropdown labels were affected. Present since the shared net computation was introduced (3.1.3).
- **Intelligent Octopus smart charges are now priced from the dispatch lifecycle, not the meter** ([#253](https://github.com/RGx01/energy-meter-tracker-addon/issues/253)). The old over-report guard decided off-peak from grid draw against a fixed floor, which is wrong in two directions: a planned slot that never charged but drew household baseload was credited off-peak (over-credit), and a genuine smart charge supplied by solar/battery drew ≈0 from the grid and was billed peak (under-credit). A settlement-time reconciliation pass now reprices each smart-charge slot from whether it actually **`started`** under `SMART_CONTROL_IN_PROGRESS` — the signal Octopus itself uses, immune to where the energy came from. Slots that started are off-peak (restoring solar-supplied charges); slots that were planned but never started or completed revert to peak; the ambiguous case (completed without started — a missed poll or a boost) is flagged for review and left unchanged. Only slots recorded under the new lifecycle accumulation are touched, user-corrected blocks are never overwritten, and devices follow the main rate. Validated against a live solar-supplemented charge where the grid meter read 0.002 kWh but the slot was correctly kept off-peak. OHME chargers are excluded — Octopus doesn't control an OHME charge, so its dispatch records can't tell a smart charge from a boost; they keep the existing behaviour.
- **Meter-exchange selection now uses the authoritative retirement signal** (hardening of [#244](https://github.com/RGx01/energy-meter-tracker-addon/issues/244)). When an MPAN lists several meters after an exchange, the active-meter picker relied on list order ("Kraken lists oldest-first, take the last") as its main heuristic. It now reads the meter's `active_to` field — a swapped-out meter has it set, a live one has it null (the same signal BottleCapDave's integration keys off) — to drop retired meters authoritatively, and among any still-live meters prefers the one still reporting / most recently activated (newest `latest_consumption` / `active_from`). The list-order fallback is kept only for payloads that carry none of those fields. Turns "the current meter warning" from a positional guess into a read of the actual retirement/reporting data.
- **Billing-source indicator wrongly showed "N blocks awaiting DCC settlement" with no API configured.** The header pill gated on the billing source being `dcc`, but that's the default even when no supplier API is set up — so a user with no API saw thousands of blocks "awaiting settlement" that would never settle (their blocks are billed on the local/CAD figure, permanently). The pill now also requires an API to be available before showing the DCC message, and otherwise reports the actual state: **"No API · local billing"** (no API), **"CAD source · API ready"** (API available but billing uses the local CAD figure), or **"⏳ N awaiting DCC settlement"** (DCC path with blocks pending). Info states are neutral-styled; only genuine pending settlement is amber.

## [3.1.3] — 2026-07-06

### Fixed

- **Critical — segfault when deleting a device** (regression introduced in 3.1.2). The chart regeneration added in 3.1.2 for [#261](https://github.com/RGx01/energy-meter-tracker-addon/issues/261) ran on the Flask request thread using the *engine's* SQLite connection, while `engine_startup` was simultaneously restarting on the asyncio event-loop thread. The connection is opened `check_same_thread=False`, so SQLite permitted the concurrent cross-thread use rather than raising — and it crashed the add-on with a segmentation fault. Chart regeneration after any mutating action (delete device / blocks, backup restore, zip import, rate corrections) is now scheduled onto the engine's event loop, serialised with all other engine store access, so it can never race. The delete/restore/correction itself always completed; only the follow-up regen crashed.

## [3.1.2] — 2026-07-06

### Fixed

- **Cumulative sub-meter sensors no longer book their whole lifetime on the first block** ([#260](https://github.com/RGx01/energy-meter-tracker-addon/issues/260)). Adding a cumulative battery/EV import sensor (or a read dropout that lost the opener) could book the sensor's entire lifetime register as one block's usage — one reporter saw ~10 MWh land in a single day. The rogue-block clamp that already protected the main meter was scoped to `is_sub_meter=False`, so device channels had no guard at all. The sub-meter path now applies a physical-plausibility ceiling (60 kWh — impossible for a single domestic device in a block, but well above any real charge), so a lost-opener dump is clamped to 0 and the register baselined, while genuine charges — including session-energy sensors that start each charge at 0 and count up — are booked normally. Recovery for an already-affected day: Data Management → Delete blocks.
- **Delete Device / Delete Blocks now regenerate the billing charts immediately** ([#261](https://github.com/RGx01/energy-meter-tracker-addon/issues/261)). Both delete paths removed the data but left the pre-built billing/heatmap charts stale until the next half-hourly block finalised — correct eventually, but confusing when it isn't instant. They now call `generate_charts` after the delete, matching the corrections (#254a) and restore (#257) paths. (This also makes Delete Blocks usable as the immediate recovery for #260.)

## [3.1.1] — 2026-07-04

*A bug-fix release. The headline is diagnostic groundwork for a critical Intelligent Octopus pricing bug ([#253](https://github.com/RGx01/energy-meter-tracker-addon/issues/253)); the rest are fixes to the corrections tool, Usage Stats, power-sensor config, and the Spiral chart on mobile.*

### Critical — Intelligent Octopus off-peak mispricing, groundwork laid ([#253](https://github.com/RGx01/energy-meter-tracker-addon/issues/253))

On Intelligent Octopus, a **peak** slot can be priced **off-peak** when a smart-charge dispatch was *planned* but the vehicle didn't actually charge — for example it paused mid-session. EMT applied the off-peak overlay on the planned slot, but the supplier billed peak, so the block is under-billed. The over-report floor doesn't catch it, because ordinary household baseload alone can clear the floor during the pause.

**3.1.1 does not fix the pricing yet — it lays the groundwork to.** Each captured smart-charge slot now records its dispatch **lifecycle state** and the dispatch's **planned / completed energy** (kWh), retained in the database. This is **observe-only, with no billing effect**; it exists because, without the dispatch energy on record, there was no way after the fact to tell a genuine off-peak charge from a planned slot that never charged. The pricing fix — validating a slot against the energy actually dispatched — is targeted for **3.1.2**.

### Fixed

- **Corrections tool now follows the devices-follow-main model** ([#254](https://github.com/RGx01/energy-meter-tracker-addon/issues/254)). Three bugs shared one root — the DCC-settled gate keys on `imp_kwh_api`, which only the main meter carries: (a) charts weren't regenerated after a correction until the next block; (b) corrections only touched the main meter, never the device rate lines; and (c) a spurious "N blocks awaiting settlement" warning counted device rows that never settle independently. All fixed: a rate correction now applies to the main meter and the device rate lines follow it (as they do in the 3.0.6 engine), the preview shows the devices following, and charts regenerate immediately. The now-redundant meter/device selector has been removed from the tool.
- **Charts not regenerated after a database restore/import** ([#257](https://github.com/RGx01/energy-meter-tracker-addon/issues/257)). Same shape as the corrections case above: restoring or importing a backup reopened the database but left the pre-built billing/heatmap charts showing the old data until the next block finalised. Both restore paths now regenerate the charts against the restored database immediately.
- **Usage Stats — "Inc. standing charge" toggle had no effect on Totals or Net** ([#255](https://github.com/RGx01/energy-meter-tracker-addon/issues/255)). The table's Total column and grand total used the server's `net_cost`, which bakes in the standing charge, so unchecking the box changed nothing. Unchecking now correctly removes the standing charge from both.
- **Power sensor "invert" setting not persisting** ([#251](https://github.com/RGx01/energy-meter-tracker-addon/issues/251)). The main power sensor's invert flag was applied at runtime but had no database column, so it was dropped on save and the checkbox reverted. It now persists like the battery inverter's flag. The device power sensor's invert flag (`device_power_invert`, used by the Live Power gauge) had the same missing-column problem and is fixed alongside it.
- **Spiral options unreachable on mobile** ([#248](https://github.com/RGx01/energy-meter-tracker-addon/issues/248)). The options panel stacked below the chart on mobile but was clipped by the chart container; it now scrolls into view.
- **Browser tab icon intermittent** ([#249](https://github.com/RGx01/energy-meter-tracker-addon/issues/249)). The favicon is now an inline data URI — no separate request or path to resolve — so it renders consistently across ingress/standalone and every environment, working around Safari's URL-keyed favicon cache.
- **Review-sample log mislabelled the provisional figure.** On the `api` + Mini path the drift-review log printed `CAD=…` even though there's no CAD; it now reflects the actual source (`Mini=…` when the Mini supplies the provisional figure).

### Also in this release

- **Spiral chart PDF export** ([#250](https://github.com/RGx01/energy-meter-tracker-addon/issues/250)) — a small feature addition: the Spiral view now exports to PDF, in portrait, instead of showing a "coming soon" banner.
- **Docs** ([#252](https://github.com/RGx01/energy-meter-tracker-addon/issues/252)) — documented how to update a standalone Docker build (`git pull` + `docker-compose up -d --build`).

## [3.1.0] — 2026-07-03

*Adds a Spiral chart for seeing a year — or a lifetime — of energy at a glance, corrects carbon-intensity averaging on near-balanced solar days, and simplifies device setup now that devices always follow the main meter's rate.*

### Added

- **Spiral chart.** A new chart (Charts → Spiral) that winds your running total — cost, usage, or carbon — outward as a single continuous coil, one loop per year, so a fatter gap between loops is a heavier year and the centre always shows the total to date. Switch between Cost / Usage / Carbon, pick any device or the whole meter, and toggle **Lifetime** (one endless coil) or **Year-aligned** (every year starts at January, at the top) winding. Axis rings land on round numbers, and the unit scales automatically as totals grow — kWh → MWh → GWh, and kg → t for carbon — so a 4,598 kWh year reads as "4.6 MWh" rather than "4.6k". Works in both light and dark themes.

- **Unsettled-blocks indicator in the Charts header.** When billing runs on the DCC settlement path, the Charts header shows a count of blocks still awaiting the supplier's settled figure (e.g. "174 blocks awaiting DCC settlement"), so a stalled settlement is visible at a glance rather than only in Data Management. Hidden on the local/CAD path, where it doesn't apply.

### Changed

- **Devices always follow the main meter's rate — device rate fields removed from setup.** A device's grid import is part of the main meter's metered supply, so it's billed at the main meter's rate; this became the engine's behaviour in 3.0.6. The now-redundant per-device "Rate Sensor" field and "Use the main meter's rate" toggle have been removed from the Add Device dialog, the device editor, and the first-run setup wizard. Existing device rate settings are left untouched in your configuration and simply ignored — nothing to migrate, and no change to any bill. (If per-device tariffs return in a future release — for example Octopus's mooted 6-hour EV-charging cap — the field comes back.)

### Fixed

- **DCC import settlement no longer stalls after a meter exchange ([#244](https://github.com/RGx01/energy-meter-tracker-addon/issues/244)).** On a supply whose meter has been swapped, the meter point lists both the old and the new meter. EMT was always taking the *first*-listed meter — the retired one — so DCC consumption queries for its serial returned nothing and import blocks never settled (while a single-meter export point settled normally, which is the tell). EMT now selects the *current* meter: it honours an active/removal flag when the account provides one, and otherwise takes the newest meter in the list. If your import settlement was stuck at a fixed number of unsettled blocks, it should clear on the next settlement pass. Single-meter supplies are unaffected.

- **Carbon average intensity no longer blows up on near-balanced days.** In the usage-stats table, the average carbon intensity was being re-derived from net figures (total carbon ÷ net kWh), which collapses toward a divide-by-near-zero on days where import and export almost cancel out — sending the figure to absurd values (~3,800 gCO₂/kWh against a grid that peaks around 500). It now uses the server's per-day intensity (computed from half-hourly *absolute* throughput, which doesn't collapse), averaged across the selected range weighted by each day's import + export. Purely a display fix; no stored data changes.

## [3.0.6] — 2026-07-02

*Safeguards the running import total against a rare lost-register glitch, and makes devices always bill at the main meter's effective rate — fixing an Intelligent Octopus dispatch overcharge that could hit device costs on reconciled smart-charge blocks.*

### Fixed

- **Guard against a rogue full-register import block.** If the main meter's *opening* register reading is momentarily lost — for example a read dropout during a restart, which can happen while adding or removing devices — a single half-hour block could book the entire cumulative meter register as one interval's import, massively inflating the running total (and bill) until settlement caught up. The engine now clamps any single block whose import exceeds a physically impossible ceiling, logs it, and keeps the register continuous so the next block opens correctly. On API / Octopus Mini / DCC setups the true half-hourly figure still replaces the placeholder when DCC settlement arrives (no action needed); on CAD-only setups with no reconciliation source the affected half-hour is treated as zero — a negligible loss versus a phantom total.

- **Devices are always priced at the main meter's effective rate — fixing an Intelligent Octopus dispatch overcharge at reconciliation.** A device's grid import (a home battery or EV) is a portion of the main meter's supply, so it's billed at the main meter's rate — base tariff plus any Intelligent Octopus Go off-peak dispatch. Two faults broke that during smart-charge slots: at recording time a device whose own draw fell below the 0.1 kWh over-report floor (typical when solar or another device took most of the grid import) was stranded on the standard peak rate; and at DCC reconciliation the main's off-peak rate was applied to its own cost but not carried onto its devices, so a device's grid import was re-billed at **peak** on every settled smart-charge block — a material overcharge (a 3 kWh grid charge billed at ~£0.97 instead of ~£0.16). Device pricing is now resolved in one place, after every meter is finalised, and always follows the main meter's effective rate for both cost and the displayed rate — at recording time and at settlement, and whether or not the device drew anything in the block (a device that drew nothing at an off-peak→peak boundary previously kept a reconstructed off-peak rate while the main was peak; it now follows the main). The per-device breakdown continues to sum exactly to the metered bill. Single-tariff installs (the common case) see identical bills; the only visible change is a device's shown rate now always matching what it was costed at.

## [3.0.5] — 2026-07-01

*Adds an optional solar/PV dial to the House Battery card, fixes device cards (battery / EV / heat pump) not appearing on the Overview for setups with no main live-power source, and tidies the Overview layout. No change to tracking or billing.*

### Added

- **Solar / PV dial on the House Battery card.** Set a new **Solar / PV Power Sensor** on a battery — on an existing battery in Meter Config, or when adding one via the device wizard — and the battery card shows a small live PV generation dial (kW) beside the inverter gauge. It's display-only — not recorded in blocks or used in any calculation — and the card is unchanged if you don't set one. This release adds a `pv_power_sensor` column to the meters table; existing databases are migrated automatically on upgrade.

### Fixed

- **Device cards now appear on the Overview without a main power sensor.** Battery, EV, and heat-pump cards were only shown when the main meter had a live-power source — your own power sensor, a BottlecapDave demand sensor, or an opted-in Octopus Mini. On API / DCC-only setups with none of those, the cards were never placed on the page even though the underlying data was correct, so configured devices looked missing. They now render whenever the devices exist, independently of the main power gauge.
- **Overview no longer stretches its cards when four gauges are shown.** A page with four gauges (for example live power plus a battery, an EV, and a heat pump) now keeps the compact, uniform card width instead of switching to a wider, stretched layout. A related miscount could also flip a page with no live-power gauge into the wide layout one card too early; both are corrected. Cards on a full top row now grow to share the width, so a four-card row fills tidily instead of leaving a trailing gap.
- **Info banners line up with the cards.** The Octopus Mini enable/disable notices and the "add a power sensor" hint are now constrained to the same width as the card grid instead of spanning the full window.
- **The gauge row fills the full width with four gauges.** When four gauges push the carbon card onto its own line, the top gauge row now stretches to the same width as the rows beneath it (rather than stopping short), and shrinks with the browser window.
- **Power-card heights stay consistent.** The gauge cards no longer shrink slightly when the carbon card moves onto its own row as the window narrows.
- **Cleaner power gauges.** Removed the small min/max scale numbers under each gauge arc — they were hard to read and duplicated the value already shown in the gauge.

## [3.0.4] — 2026-06-29

*Completes the Intelligent dispatch API migration begun in 3.0.3. No change to how charges are priced for current setups — the same smart-charge slots are detected, now via Octopus's current API.*

### Changed

- **Migrated Intelligent planned dispatches to `flexPlannedDispatches`** — Octopus deprecated the `plannedDispatches` field (scheduled for removal) in favour of `flexPlannedDispatches`, which is keyed by the charge-point device rather than the account and reports `type`/`energyAddedKwh` in place of `meta.source`/`delta`. EMT now discovers the charge-point device id and queries the new field, mapping it back to the same internal shape. Verified against live data (planned-slot and smart-slot counts matched the old field exactly through a real charging schedule) before the old field was removed.
- **Smart-charge detection now recognises both API vocabularies** — the dispatch source is matched against `smart-charge` (legacy) and `smart` (flex), so detection is correct regardless of which spelling Octopus returns. Boost/bump dispatches (`bump-charge`/`boost`) remain excluded from off-peak, as before.
- **Dispatch slot times moved to `start`/`end`** — off the deprecated `startDt`/`endDt` fields (same values, current field names).

### Fixed

- **Deprecation self-check no longer over-reports on generic field names** — fields like `id` exist on nearly every schema type, so name-only matching flagged unrelated deprecations (ledgers, payments) EMT never uses. Generic names are now matched only on the specific types EMT actually reads them from, so the count reflects fields EMT genuinely depends on. With this migration complete, EMT uses no deprecated API fields, so the deprecation notification clears itself.

## [3.0.3] — 2026-06-28

*Resilience to Octopus/Kraken API changes — no functional change for current setups.*

### Changed

- **Future-proofed Intelligent dispatch detection** — moved off Octopus's deprecated `registeredKrakenflexDevice` API (which Octopus has scheduled for removal) to the current `devices` query. Smart-charge / dispatch provider detection keeps working once Octopus withdraws the old field.

### Added

- **API drift self-detection** — if an Octopus/Kraken schema change ever rejects a field the add-on depends on, it now logs a distinct `kraken_schema_drift` error pointing to the Octopus announcements page, instead of a generic "unavailable" message — so a broken query is obvious in the logs and quick to pin down.
- **Upcoming-deprecation early warning** — on the first poll after startup the add-on introspects the live Octopus/Kraken schema and checks the specific fields and enum values it relies on against those Octopus has flagged *deprecated* (the grace window before a field is actually removed). If any are found it raises a Home Assistant **persistent notification** and publishes a `sensor.energy_meter_tracker_api_deprecations` entity (a count, with the affected fields and Octopus's stated reasons in its attributes), alongside a distinct `kraken_field_deprecated` log line — so the alert reaches you outside the logs and you can migrate *ahead* of the break rather than discovering it via `kraken_schema_drift` after it lands. The notification dismisses itself and the sensor returns to `0` once the schema comes back clean; if the endpoint has introspection disabled the check quietly skips and leaves drift-detection as the safety net.

---

## [3.0.2] — 2026-06-28

*Packaging and repository housekeeping — no functional changes. Faster to install and update.*

### Changed

- **Faster, more reliable builds** — the Docker base image and Python dependencies (`aiohttp`, `flask`, `waitress`) are now pinned to fixed versions. The dependency layer is cached across updates instead of being reinstalled each time, and builds no longer drift onto progressively heavier dependency trees.
- **Smaller build context** — screenshots, tests, and development docs are excluded from the image build, so each build copies far less.
- **Trimmed changelog** — older release notes (2.10.x and earlier) moved to `CHANGELOG-ARCHIVE.md`, so the in-app update dialog loads and renders much faster.

---

## [3.0.1] — 2026-06-27

*Post-release fixes for issues reported after 3.0.0.*

### Fixed

- **Confusing error after entering the Octopus API key (issue #217)** — the Setup Wizard's Account number field was labelled "(optional — auto-detected)", so users left it blank; when auto-detection couldn't resolve the account the result was an unhelpful error. The misleading hint is removed so the field reads plainly as the credential it is.
- **Device name appeared optional when adding an EV / battery / heat pump (issue #218)** — the wizard's Device Name field was labelled "(optional)" even though a name is required (and was already enforced on submit), producing a confusing "name is optional, apparently" experience. The misleading "(optional)" label is removed; the genuinely-optional Site Name is unchanged.
- **"This Bill" showed a stale historical date when the current period had no data yet (issue #221)** — on the Overview, when the most recent block predated the current billing period (so no generated period contained today), the "This Bill" card fell back to the last *historical* period and displayed an old date (e.g. an April period). It now synthesises the current billing period from your billing day and shows it — at £0.00 until data lands — instead of a past one. The billing-day arithmetic is also hardened so a billing day of 29–31 no longer breaks in shorter months.
- **Generation Mix card overlapped its neighbour on the Overview (issue #223)** — with a live-power card plus three or more devices, the Overview switches to a grid layout whose 220 px columns were narrower than the 280 px Generation Mix card, so at some window widths the card overflowed its cell and overlapped the Live Power card instead of wrapping. The grid columns are widened to 280 px so the cards always wrap cleanly. (Live Power with two devices was unaffected and still is.)

---

## [3.0.0] — 2026-06-21

*The largest release since EMT began. v3 turns a meter-reading recorder into a billing-grade energy ledger: it can reconcile every block against the settled half-hourly data your supplier actually bills from (Octopus / Kraken DCC), understands Intelligent Octopus Go smart-charge slots, and accounts for grid carbon intensity across the whole app. This is a **major-additive release — nothing breaks**. Existing 2.x installs upgrade in place, keep behaving exactly as before, and switch the new capabilities on when they're ready. Everything new is opt-in through the Setup Wizard. See **Upgrading** at the foot of this entry.*

### Added

- **Octopus / Kraken DCC settlement** — EMT can now reconcile each block against the settled half-hourly consumption your supplier bills from, fetched directly from the Kraken platform. It pulls settled import (and export), unit rates and standing charges on a schedule, runs an automatic settlement sweep with a horizon back-stop so older blocks get reconciled as the data lands, and retries transient failures. A **billing-source toggle** (Meter sensor vs Supplier API) chooses which figures drive your bill. Credentials are entered in-app with a keep-or-replace flow and a disconnect/clear action — no YAML. Rate corrections are gated until a block is DCC-settled, so a manual edit can't fight the authoritative figure.

- **Data-source modes & supplier-first setup** — the Setup Wizard now leads with your supplier and meter situation, then configures the right data path: **`cad`** (local CAD/sensor only — exactly how 2.x works) or **`cad+api`** (local readings plus supplier-API settlement and telemetry). A **Change Setup** launcher lets you move between modes later, behind a confirmation because a switch can trigger a full recalculation. Configuration is mode-aware — options that don't apply to your setup are hidden.

- **Octopus Home Mini live power** — if you have an Octopus Home Mini, EMT can use its real-time demand feed as a live-power source for the gauges and 48-hour history without a separate CAD sensor, with exponential backoff and rate-limit-aware polling. (Mini figures are treated as provisional; DCC wins once a block settles.)

- **Intelligent Octopus Go dispatch overlay** — for IOG users, EMT captures smart-charge dispatch slots and reprices the affected blocks at the off-peak rate they were actually charged at, applied at both finalise and DCC settlement. Per-device **"use dispatch overlay" rates** let an EV charger bill at the dispatch rate while the rest of the house stays on the standard tariff. A 0.1 kWh validation floor ignores meter jitter.

- **Grid carbon intensity** — EMT records gCO₂/kWh for every block (🇬🇧 UK, postcode-based) and surfaces it throughout: a **Carbon Insights** page (house / solar / EV / battery / heat-pump breakdown, gCO₂/mile, generation mix), CO₂ columns and charts in Usage Stats, and a gCO₂/kWh **heatmap** mode. A one-time historical backfill fills carbon data for your existing 2.x blocks in the background after first start.

- **Per-channel rate source (API rates vs sensor)** — each channel can independently take its unit rate from the supplier API or from a sensor, so mixed setups work (e.g. API import rates alongside a sensor-supplied export rate).

- **Power-sensor invert & unit override** — any power sensor (main, EV, heat pump, battery) can now have its sign inverted and its unit forced to W or kW. This fixes live-power and 48-hour charts that read reversed (sensor sign convention opposite EMT's) or 1000× wrong (a sensor that emits watt-scale numbers while declaring its unit as kW, which no auto-heuristic can catch).

- **Ohme EV charger support** — detection-aware handling for Ohme chargers, preferring the charger's own session sensor where available. Ships conservatively, with a feedback path in the config UI for Ohme users to report behaviour.

### Improved

*Lighter-touch refinements to things 2.x users already rely on:*

- **"Devices" replaces "sub-meters"** — your EV charger, battery and heat pump are now called **Devices** throughout the UI. No data or configuration change.
- **Overview page (formerly Live Power)** — the Live Power page is now **Overview** and renders even without a live-power sensor, so postcode-only installs still get generation mix and carbon context.
- **EV grid attribution (issue #212)** — when an EV and a battery draw at the same time and grid import only covers part of it (solar filling the rest), the EV now claims grid import **first** instead of the largest load winning. An EV on a cheap smart-charge slot no longer vanishes from the grid view behind a simultaneously-charging battery. Bill totals are unchanged — only the per-device attribution is corrected.
- **Block delete redesign** — deleting blocks now cascades device rows, reseeds the following block's opening reads and recomputes the remainder, so a delete no longer leaves orphaned device data or a gap.
- **BottlecapDave (BCD) awareness** — if you run the BCD Octopus integration, EMT detects it and adjusts so the two don't double up on live-power polling.
- **Billing consistency & restart robustness** — many reconciliation, gap-fill, restart-recovery and cost-rounding refinements across the api+mini path, so totals stay self-consistent across restarts and configuration changes.

### Upgrading

- **Nothing breaks.** Your blocks, configuration and history are preserved. Schema migrations run automatically on first start, and a one-time carbon-intensity backfill runs in the background (UK postcode installs).
- **You don't have to change anything.** Leaving the data-source mode at `cad` keeps EMT behaving exactly as 2.x did. DCC settlement, Mini live power and the dispatch overlay are all opt-in via the Setup Wizard / Change Setup.
- If you configured the Octopus API before the supplier field existed, EMT infers Octopus from your existing credentials. A restored v2-era backup with no supplier field defaults cleanly to local-metering-only.
- See **Upgrading from 2.x** in the README for the full walk-through.


---

Older releases (2.10.x and earlier) are in [CHANGELOG-ARCHIVE.md](CHANGELOG-ARCHIVE.md).