# Changelog
 
## [3.3.1] — 2026-07-14
 
### Fixed
 
- **The "Flagged for review" list could raise false positives** (hotfix for the 3.3.0 review list). Two kinds of half-hour block were flagged that had nothing to review: (1) blocks **inside the off-peak window**, where the price is already the off-peak rate by the tariff schedule regardless of any smart charge — so nothing a dispatch does could change it (this also covers a charge landing in the window under the 6-hour cap); and (2) blocks **not yet settled by the DCC**, which the correction tool can't act on anyway (dispatch data arrives within hours, but settlement lands a day or two later). The review list now only surfaces settled, out-of-window blocks — the ones where the off-peak price genuinely depended on a dispatch having happened. Any block flagged in error is cleared automatically on the next reconciliation, so no manual dismissal is needed.

## [3.3.0] — 2026-07-14
 
### Added
 
- **Pick a nearby rate when correcting** ([#270](https://github.com/RGx01/energy-meter-tracker-addon/issues/270)). The rate correction form now offers a dropdown of the exact rates already in force in blocks *around* the correction window — the everyday off-peak and peak values — ranked by how common they are. Pick one and the field fills at full precision, so there's no mistyping, wrong decimal place, or accidental rounding. You can still type a custom value to override. The list refreshes as you change the date range, and pre-fills when you load a flagged block from the review list.
 
- **"Flagged for review" list on the Corrections page** (BL-18). When a smart charge can't be priced with confidence — the supplier planned a slot but it's genuinely ambiguous whether the car actually charged — the block is now left at its current price *and surfaced for you to decide*, instead of being silently ignored. The Corrections page shows a list of these blocks with the reason and the exact half-hour window; each has a **Load into tool** button that pre-fills the correction form for that block, and a **Dismiss** if the current price is right. A dismissible notice appears across the app when new blocks are flagged (and stays quiet once acknowledged, until *more* are flagged). Flags clear themselves automatically when the block later becomes decidable or when you apply a manual correction to it.
 
### Fixed
 
- **A device could be credited more grid import than the whole house drew, on data rebuilt after an outage** (BL-19). When EMT misses live readings — for example the add-on was stopped or offline during a smart charge — it reconstructs the missing half-hours by spreading the accumulated energy across them. Those rebuilt blocks skipped the check that caps each device's grid share at what the house actually imported, so once the meter settled to its true figure (often small on a sunny, export-heavy day) a device such as an EV charger could still show far more grid energy than the house imported — over-attributing grid, and grid cost, to the device while understating its solar/battery self-supply. The whole-house bill was always correct; only the per-device split was wrong. The cap now always applies once the house's import figure is authoritative (live-metered or settled), and a one-time check on upgrade repairs any already-affected blocks (they re-price automatically over the following polls). An unsettled gap block is still left as-is until settlement lands, so a genuine overnight grid charge isn't prematurely reclassified.
 
## [3.2.1] — 2026-07-13
 
### Fixed
 
- **Intelligent Octopus smart charges on the official Ohme integration were billed at peak** ([#286](https://github.com/RGx01/energy-meter-tracker-addon/issues/286)). The verified Ohme path read the charge-mode `select` and compared its state against `"smart charge"` — but the official Home Assistant `ohme` integration reports the underscore **slug** as the state (`smart_charge`), and only *displays* "Smart charge". So every tick resolved to `idle`, no slot was ever captured, and the entire smart charge was priced at the day rate. The mode match is now slug/display-agnostic (`smart_charge` / `max_charge` / `paused`, space and underscore treated alike), so the off-peak overlay applies again. **This affected every user on the official Ohme integration since the Ohme path shipped** — dan-r-integration and non-Ohme users were unaffected.
- **A planned smart charge that barely ran is no longer over-credited at off-peak.** When Octopus planned a smart-charge slot but the car didn't materially charge (for example it was near-full or solar covered the house), the slot's small grid import — really household baseload — could stay priced at the off-peak rate. The settlement reconciliation previously treated *any* "completed-without-`started`" slot as ambiguous and left it unchanged. It now uses the supplier's **completed energy** (not the meter, so it stays solar-safe): a completion below **0.4 kWh** can be neither a boost (which draws hard) nor a real smart charge, so the slot reverts to peak. Completions above that remain genuinely ambiguous (a missed-poll smart charge or a boost) and are left untouched (a UI to surface these flagged blocks for review is planned for 3.3.0).
- **Ohme off-peak capture now follows the charger's real charging state.** In addition to the fix above, the verified path now gates on the Ohme **Status** sensor (`charging`) rather than only the charge-mode setting. This captures a slot precisely when the charger is actually drawing — so mid-session pauses and Octopus/Ohme replanning that adds *further* slots ("additional slots") are handled by construction, without depending on Octopus's planned-dispatch superset (which is unreliable for Ohme, since Octopus doesn't control the charge). Setups without the Status sensor fall back to the previous mode-only behaviour. The `_capture_ohme_slots` log line now includes the raw mode and status values, so any future mismatch is visible at a glance.

## [3.2.0] — 2026-07-11

### Added

- **"Update available" notice** (BL-6). Supervised installs get update badges from the Home Assistant Supervisor; Docker/standalone installs got nothing and only learned of a release by checking the repository. EMT now checks once a day for a newer release and shows a dismissible notice linking to the release notes. **Dismissal sticks per version** — dismiss it for a given release and you won't be asked again until something newer ships.

- **Backups are now isolated per site** (BL-5). In supervised installs backups are written to `/share`, which Home Assistant shares across **all** add-ons — so two EMT instances would list, and could restore, each other's backups. The backup directory is now namespaced by a slugified site name (taken from your existing configuration; nothing new to set). Renaming your site moves the directory across silently; renaming back re-adopts the original one so old backups reappear; and a directory belonging to another instance is never merged or touched. Ownership is decided by a persistent per-install id, resolved by **scanning `/share` for the directory this install already owns** — so an instance keeps its own backup directory even after a restore whose data reports a *different* site name, and repeated restores never spawn duplicate directories or let one instance adopt another's backups. Docker/standalone installs are unaffected — their backup directory lives inside the container's own volume and is already isolated. These protections key on site identity rather than how EMT was installed, so they hold however a second instance came to exist.

- **"Restored from another instance" notice** (BL-5). Each database now carries a lineage id that travels with it through backup and restore. When a database is restored from a *different* install (for example a production backup restored into a staging instance), EMT shows a dismissible notice so the swap is visible rather than silent. Dismissal is **per source lineage** — acknowledge a given source once and routine repeated restores of it stay quiet, while a genuinely new source still surfaces. Native databases carry their own install's id and never raise it.

- **Recover missing data after an outage** (BL-8). A new **Missing Data** panel in Data Management finds half-hour blocks that were never recorded — typically because the add-on was offline longer than the 12-hour gap-fill limit — and rebuilds them from your supplier's settled readings. The routine settlement poll only ever looks forward from its last run, so a gap it has already passed was previously permanent. Recovery is a **manual action on purpose**: EMT cannot tell an outage gap from blocks you deleted deliberately, so an automatic sweep would restore those too. Recovered blocks use the authoritative half-hourly figures and are priced at the rates in force at the time, but carry **no per-device breakdown** — EMT was not running, so no device readings exist for that period.

- **Outage recovery: gaps are now backfilled from settled supplier data as settlement arrives** (BL-8). An outage longer than the 12-hour gap-fill limit used to leave a permanent hole — reconciliation only reprices blocks that exist, and the DCC poll only *settled* existing blocks, dropping the settled figure for any period with no block. EMT now **creates** those blocks from the authoritative half-hourly figures as settlement arrives, and the normal PASS-2 pass prices them (fetching historical rates for the period, so any tariff — Agile included — prices correctly). Recovered blocks are marked as externally sourced, and carry **no sub-meter split** — EMT was down, so no device readings exist for that period. Their off-peak status is decided once, at creation, from whatever dispatch data the supplier still returns (~24–48h), so recovery is best-endeavours: a smart charge during a long-past outage may be billed at the standard rate.

### Removed

- **The four synthetic block sensors have been removed**: `sensor.energy_meter_import_kwh`, `sensor.energy_meter_export_kwh`, `sensor.energy_meter_import_cost`, `sensor.energy_meter_export_credit`. They published live per-block figures that DCC settlement retrospectively corrects, so their values disagreed with the billing EMT itself reports — misleading rather than merely unused. **Breaking change** if you referenced them in a dashboard, template or automation; use the Charts and Insights pages, which reflect settled figures. `sensor.energy_meter_tracker_api_deprecations` is unchanged.

### Fixed

- **Saved-configuration messages no longer overlap the page** ([#219](https://github.com/RGx01/energy-meter-tracker-addon/issues/219)). The "Configuration saved" and error messages on Meter Config were pinned to the top of the viewport, painting over whatever sat beneath them — usually the Meter Config / Carbon tabs, leaving both unreadable. Notifications now appear in a shared region between the topbar and the page content: they push the page down instead of covering it, stack rather than overlap, and stay visible while you scroll. Transient messages clear themselves; ones you need to act on stay until dismissed. The unsupported-tariff warning moved here too, since it affects the whole app rather than the Charts page.

- **A long outage no longer freezes DCC settlement across your whole history** (BL-1). When EMT detects a session gap (an outage longer than the 12-hour gap-fill limit) it sets a gap marker on the current block. That marker also — wrongly — gated the DCC PASS-2 drain, the step that applies settled half-hourly figures to billing. So while a gap marker was live, settled data for **every** block, not just the outage, was ingested but never applied: costs stayed on the pre-settlement estimates until the marker cleared. The marker still correctly guards sub-meter boundary amendment (gap-seed reads would corrupt its interpolation), but the drain now runs regardless — it takes its queue from the database and never reads the current block's rolling buffer, so gap-seed reads cannot contaminate it.

- **A supplier-API outage no longer shows a false "Tariff not supported" banner.** The rate-schedule refresh flagged a meter "unsupported — billing will be incorrect" whenever the unit-rate fetch came back empty — but a *failed* fetch (transport error, timeout, or an edge/WAF **HTTP 403** while the supplier's GraphQL is throttled) returned empty too, so a transient block was reported as a permanent tariff problem. The refresh now distinguishes a failed fetch from a successful-but-empty one: on a fetch failure it keeps the last-known schedule and leaves the banner untouched, and only raises the "unsupported" warning when a fetch genuinely succeeds and returns no standard **and** no day/night rates.

- **Footer now identifies the instance, not the port.** The sidebar footer showed `port 8099` — the internal container port, which is identical for every instance under ingress and so distinguished nothing. It now shows a per-instance label, resolved highest-first from: the new **`instance_name` option** (set it per instance in the add-on's Configuration tab — the only distinguisher when two installs share one add-on `name`, e.g. the repo-URL workaround); the **add-on name** via Supervisor (e.g. "Energy Meter Tracker (DEV)"); or the container hostname on standalone. It falls back to the port only if none resolves. Every source is an *install* identity — the manifest or `options.json`, never the database — so the label stays correct after restoring one instance's backup into another (the site name, which lives in the database, would travel with the restore).

- **An edge 403 no longer masquerades as "check API key".** The REST client mapped *every* 403 to "authentication failed — check API key", so an edge/WAF block (which returns an HTML error page, distinct from a genuine JSON auth error) told users to rotate a perfectly good key. A 403 with an HTML body is now reported as an edge block ("temporarily blocked by the supplier — not an API key problem"), while a real 401 — or a 403 with a JSON body — still says "check API key". The Settings "Test connection" button reflects the same distinction.

- **GraphQL edge-403 circuit breaker — stop hammering a blocked endpoint.** When the supplier's GraphQL edge returns a 403 (an intermediary blocking the endpoint, distinct from an app-level error), EMT used to keep calling it every poll — the Octopus Home Mini polls ~every 10s — which only prolongs the block and floods the log (hundreds of identical multi-line HTML dumps). A 403 now opens a circuit breaker that short-circuits GraphQL for a growing cooldown (1 min, doubling, capped at 15 min) and resets on the next success. Mini-telemetry and dispatch failures are logged once per episode instead of on every retry, with a clear "recovered" line when the block clears.

### Internal

- **`energy_charts.py` now parses on Python 3.10 as well as 3.12.** Three option-list f-strings used same-quote nesting (a 3.12-only syntax), which made the whole module — and the ~380 tests that import it — fail to even load on older interpreters. Rewritten to equivalent dual-compatible quoting; no behaviour change. Keeps the test suite runnable across interpreter versions.
- **Test suite runs file-isolated.** Several test modules install `sys.modules` stubs, so they must run one module per process (as the harness does); `docs/instance_isolation_design.md` documents the BL-5 test coverage.

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