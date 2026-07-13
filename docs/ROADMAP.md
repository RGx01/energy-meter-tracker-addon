# Roadmap

Newest at the top. **Upcoming work (ranked by priority) first, then release history newest-first** (v3 → v2 → v1, oldest at the very bottom). Scope and timing subject to change.

---

## Upcoming — ranked by priority

### 3.3.0

#### BL-18 — Surface flagged (`needs_review`) blocks in the UI
*The UI half of the dispatch-reconcile ambiguity handling (deferred from 3.2.1).* The store already flags blocks `needs_review = 1` in a few places (e.g. DCC zero-blocks) and provides `get_drift_alerts()` plus clear methods — but nothing is **wired to the UI**: `get_drift_alerts()` is called by no route or template, and there is no "review list" anywhere, so flagged blocks are invisible. 3.2.1 auto-reverts the *unambiguous* over-credits (completed-without-`started` below 0.4 kWh → peak) but deliberately does **not** flag the genuinely-ambiguous ones (substantial completed-without-`started`), because there is nowhere to show them. Build the surface: (a) a small route returning `get_drift_alerts()`; (b) a **"Flagged for review"** list on the Corrections page — each row showing the block's date + reason, with a button to load it into the correction tool and a dismiss/clear (the clear method already exists); (c) a count/badge (or a BL-6 notification) so it's discoverable. Then (d) have the dispatch-reconcile `review` case set `needs_review = 1`, and clear it when a block is manually corrected. Ships the flag-setting and the UI together so the feature is whole.

### 3.2.0 — Feature release with fixes (next)
Outage-resilience fixes (**BL-1**, **BL-8**) plus instance-isolation and notification work (**BL-5**, **BL-6** — which also fixes the overlapping-banner bug [#219]).

#### BL-1 — Gap marker globally freezes DCC settlement application  ·  *fix*
*The correctness item.* An outage longer than the 12-hour gap-fill limit leaves the gap marker set, which globally gates the PASS-2 drain — so DCC-settled figures are ingested but never **applied** to billing (across the whole history) until the source returns or the add-on restarts. Fix: make the drain gap-window-aware so blocks outside the gap window still settle, rather than gating globally.

#### BL-8 — DCC outage backfill / gap recovery
*Resilience; recovers data an outage would otherwise lose forever.* An outage longer than the 12h gap-fill limit leaves a permanent hole: reconciliation only reprices existing blocks, and the DCC poll's `upsert_kraken_block` only settles **existing** blocks (returns `missing_block` and skips when none exists). The settled half-hourly data is still at the supplier. Fix: on `missing_block`, **create** a DCC-sourced block from the settled kWh, flagged externally-sourced (`interpolated=2`). Two flavours — **automatic** (self-heals as settlement lands, T+1/T+2) and **on-demand** (extend "retry settlement" to create-not-just-settle, to fill a known gap now). *Diagnosed against the 6-Jul segfault, which left a real 24h hole.* Absorbs the old "Extended outage backfill" note.

**Rate sourcing — solved.** DCC returns consumption only, no rates, and the in-memory schedule isn't guaranteed to cover the gap. But `get_unit_rates` already accepts `period_from`/`period_to` against the public REST products API, which serves the **full historical rate series** — including Agile import *and* export (published day-ahead). So a backfilled block is priced at the correct rate for its actual time, on any tariff. **Exception:** a channel whose rate source is a *sensor* has no history — those blocks must be created kWh-only and flagged for manual rate correction (BL-15).

**Off-peak / dispatch status — best endeavours only.** *Measured:* the API's `completedDispatches` window is ~24–48h (28 accumulated slots spanning ~36h under continuous 5-min polling; the 90-day figure is our own `prune_dispatch_history` retention, not the API's). This corroborates BCD's "the OE API doesn't provide historic intelligent dispatch information." Nothing was captured during the outage, so:
- If the gap is still **inside** the dispatch window when we backfill, mark off-peak slots from `completedDispatches`. Here `completed` is the *only* signal available (no `started` was captured), so we use it despite it not discriminating smart-vs-bump — best available, not ideal.
- If the window has passed, the block is priced at the **standard rate**. An unrecovered overnight smart charge is then billed at peak → **cost overstated** (errs expensive, not cheap).

**Therefore:** backfilled blocks MUST be visibly flagged "recovered — approximate", never presented as authoritative; an overstated-but-authoritative-looking block is worse than a visible gap. And since settlement (T+1/T+2) barely overlaps the dispatch window (~24–48h), the **automatic** flavour is the primary one — it has a real chance of catching dispatch data; a manual backfill days later does not. Whole-house-accurate regardless; no sub-meter split (we were down) — BL-12 is the device-split upgrade. **Pairs naturally with BL-1** — BL-1 unfreezes settlement, BL-8 fills the hole.

#### BL-5 — Instance isolation (safe under multiple instances)
*Not a multi-property feature — a correctness fix. Closes a live silent-data-loss footgun and removes misleading sensors.* One instance stays single-account/single-property throughout; making the engine multi-tenant would touch the config-period chain, block store, billing periods, Kraken account and dispatch overlay, for a small audience. What BL-5 delivers is that **EMT is correct when a second instance exists**, however it came to exist — and that is needed today, since a user can already create one without asking us.

**DECISION: we do NOT ship `energy_meter_tracker_2`.** We neither document nor endorse the repo-URL workaround. **But if a user does it, nothing breaks** — that is the design requirement, and everything below delivers it. Consequence, stated plainly rather than discovered later: **multi-property on HAOS is not a supported feature.** A HAOS user who wants two properties must either use the unendorsed workaround (at their own risk, and it will work) or run HA in Docker. We do not carry a second store entry for every single-property user to serve a small audience, and we do not take on a second manifest to maintain.

This decision costs nothing in scope: the `/share` collision, the misleading sensors, and the `instance_id` work below are **all still needed**, because they apply to *any* second instance however it was installed — including ones users create without asking us.

**Ports — a non-issue.** There is only one port (8099). `ingress: true` + `ingress_port: 8099` route the Supervisor's authenticated proxy to it; the separate `ports:` entry merely *also* publishes it on the host for optional direct access. The Supervisor gives each add-on its own ingress route automatically, so no collision is possible there. The only clash is the optional host publish — two instances both binding host 8099 — and the Supervisor **refuses to start the second**, a loud visible failure the user fixes in the add-on's Network panel. Fail-fast, nothing for us to do. *Separately:* remove the `options.port` / `schema.port` user option — it can't work as advertised (the Supervisor's `ports`/`ingress_port` are static and don't follow it, so setting it breaks ingress and direct access), and it exists only for the dev instance, which declares its port in its own local manifest. `EMT_PORT` env stays for standalone/Docker.

**Background — why a second supervised instance can't be user-installed.** The Supervisor keys installed add-ons by repository + slug and permits a given add-on from a given repository exactly once (a long-standing feature request). The only *supported* route would be for us to ship a second add-on (distinct slug, same image) — declined above. A user-side workaround exists: add the repo twice under slightly-varying URLs, which the Supervisor hashes into distinct repositories, each offering its own installable copy. Under it the Supervisor creates genuinely separate containers, each with its own `/data` and its own ingress route.

**HAOS cannot fall back to Docker.** HAOS is an appliance: no user-accessible Docker daemon. "Use Docker for multi-property" is not advice for the largest install base — it is a refusal with extra steps. Hence the decision above is a real refusal of the feature on HAOS, not a redirection.

**Sensor collisions — resolved by removal, not namespacing.**
- **Remove** the four per-block sensors: `sensor.energy_meter_import_kwh`, `..._export_kwh`, `..._import_cost`, `..._export_credit`. Unused, and **actively misleading** — they publish live per-block figures that DCC retrospectively corrects at settlement. Breaking change; call out in release notes.
- **Leave `sensor.energy_meter_tracker_api_deprecations` alone** — no site name. It counts deprecated *Kraken API* fields, a property of the API, not of the site. Two instances compute the same value, so a collision is idempotent: both write the same number, nothing is lost or misleading. Namespacing it would buy churn and nothing else.

Net effect: **no sensor-naming option, no entity churn, no opt-in flag.** The only thing that ever gets namespaced is a filesystem path.

**Backup directory — the one real collision.** `/data` is per-add-on (so `blocks.db` is already isolated — two engines can never share a database, whatever the install route), but supervised mode writes backups to a hardcoded `/share/energy_meter_tracker_backup`, and **`/share` is shared across all add-ons**. Two instances would list and restore each other's backups — silent data loss. Fix: suffix the path with a **slugified site name** (already in the config-period chain as `site_name`; no new user config). Slug: lowercase, `[a-z0-9_]`, collapse the rest, cap length, fall back to a default if empty (`"Mum's House"` → `mums_house`).

**Instance identity.** Nothing in the environment identifies an instance (no `ADDON_SLUG`, no usable hostname). So **mint a persistent `instance_id` (UUID) on first run and store it in `/data`** — per-instance and persistent by construction. The backup-directory ownership marker keys on this id. It survives restarts and upgrades and needs nothing from the Supervisor.

**These defences are install-method-agnostic — by design.** They key on *site identity*, not on slug or repository, so EMT behaves correctly whether the second instance came from our blessed second slug, from the unendorsed repo-URL workaround, or from Docker. We neither detect nor block the workaround; EMT is simply correct under it. (Residual, accepted: two hack-installed instances sharing a site name land in the adopt-or-suffix path and each keeps a stable directory via its own marker. A user could still deliberately restore from the wrong directory — the UI lists only their own, so it takes effort.)

**Site rename — fully silent, three cases.** An instance writes an ownership marker (its `instance_id`) into its backup dir. On a site rename:
1. **Destination free** → `os.rename` the directory. Atomic (same filesystem, both under `/share`), instant, invisible. The common case.
2. **Destination exists, marker says it's *ours*** (e.g. renamed away and back) → **adopt it**. The old backups reappear; nothing moves.
3. **Destination exists, marker says it's someone else's, or is absent** → a sibling instance may own it. **Timestamp-suffix** the new dir (`..._flat_2b_20260708T120000`) and start fresh. Never merge, never touch their data.

Ownership is decided by **instance identity, not liveness** — a heartbeat would risk adopting a merely-stopped sibling's directory. All three paths are silent: **no user warning anywhere.** (Without case 2, a rename-back would leave the user's backups split across two directories with the UI listing only the empty one — "my backups vanished". That's why identity matters.)

**Unsupervised (Docker / HA Container) — works today, docs-only.** In standalone mode the backup dir is `/data/energy_meter_tracker/backup`, i.e. **volume-scoped and site-name-independent** — there is no `/share`, so no collision and nothing happens on a site rename. The user already controls the two things that matter: a separate data volume per container and the host port mapping (`-p 8100:8099`). Work is a second compose service, documented. This is *not* a fallback for HAOS users (see above) — it is only relevant to those already running HA in Docker.

**Multiple accounts vs multiple addresses (for whoever runs two instances anyway).** *Multiple accounts* (home + rental, two Octopus accounts) needs nothing further — each instance authenticates with its own API key. *Multiple addresses on one account* (one account, two MPANs) would additionally need a small property/MPAN selector in `auto_discover`, which today picks the account's property. Build only if requested.

**Implementation notes.** (a) Serialise the directory rename against in-flight backup writes — the engine takes an upgrade backup on first start per version, which could race a rename-at-startup. (b) "Destination exists" is a **filesystem check**, not a string comparison against the previous name — the used-before case includes names this instance never had.

**Shape of work, smallest first:** (1) mint a persistent `instance_id` (UUID) in `/data`; (2) slugify helper + site-slug suffix on the supervised backup dir, with the ownership-marker rename handling; (3) remove the four block sensors; (4) remove the `options.port` / `schema.port` option (after the dev instance has its own local manifest, not before — ordering matters or the dev instance loses its port). No packaging work: no second slug is shipped. *Optional, on demand only:* property/MPAN selector for multiple addresses on one account.

#### BL-6 — Global notification region + "update available" banner  ·  fixes [#219]
*Usability, primarily for unsupervised users; also fixes a live UI bug.*

**Why it's more than a banner.** The config-saved message in `meter_config.html` is `position:fixed; top:16px; left:50%` — pinned to the **viewport, out of flow**, so it paints over whatever sits at the top of the page (currently the Meter Config / Carbon tab row). That is **#219**. Adding an update banner as a second free-floating overlay would put two elements in the same 40 pixels and reproduce the bug one layer up. So BL-6 builds the region first, then uses it.

**The region.** A **global** notification area living in the shared layout (not per-page), **docked to the existing fixed topbar** (2.5.0) rather than floating independently:
- It renders **inside/immediately below the topbar**, as part of that fixed header block — so it stays visible however far the page is scrolled.
- The page content offset becomes **topbar height + region height**, computed when messages are present, so content is **pushed down, never covered**. This is what makes it a real fix for #219 rather than relocating the overlap — nothing ever renders beneath it.
- Messages **stack** rather than superimpose.

**Two message classes.**
- **Transient** — auto-dismiss (~10s), no user action. *Consumer:* config-saved.
- **Persistent** — dismissible, sticky until dismissed or resolved. *Consumers:* update-available; the 3.1.5 **unsupported-tariff** warning (currently a one-off pill in the charts topbar — migrate it here).

**The update check.** Supervised / HA OS users get update notifications free (the Supervisor watches the repo and badges on a `version:` bump). **Unsupervised (Docker / HA Container) users get nothing** and only learn of a release by manually checking the repo. Fix: a small self-contained check benefiting *all* install types — on a **cached daily** schedule, fetch the latest release tag from the GitHub Releases API (`GET /repos/RGx01/energy-meter-tracker-addon/releases/latest`); if newer, show a **dismissible banner** linking to the release / the standalone "Updating" docs.

**Dismissal is per-version and sticks — no nagging.** Dismiss the banner for 3.3.0 and it stays dismissed; it reappears only when a *newer* version than the dismissed one is published. Persist the dismissed version string client-side and compare against the fetched tag.

**Mobile:** behave as desktop for now. The topbar already collapses on mobile and a stacked region eats vertical space, but don't pre-solve it — revisit only if testing shows a problem.

**Implementation note.** The content offset must be recomputed when messages appear/disappear (content shifts — acceptable and standard), and the region must be empty-height when there are no messages, so no layout change for the common case.

---

### Prioritised backlog — unscheduled (highest first)

#### BL-9 — IOG 6-hour EV-cap billing  ·  [#272]  ·  **BLOCKED on Octopus**
*High impact, cannot start.* Step 2 of the new IOG time-of-use tariff (step 1 — day/night general rates — shipped in 3.1.5). Fetch `ev-device-off-peak` / `ev-device-peak` rates, track dispatched EV energy per **cap-day** (midday→midday) against the 6-hour cap, and in the reconciliation map a `started` slot to `ev_device_off_peak` **within** the cap and `ev_device_peak` **beyond** it — replacing the interim night-rate approximation. **Do not build until OE publishes the cap definitions** — the community (BCD included) is still waiting on them. Confirmed structure and open questions in `docs/iog_6hr_cap_design.md`. Resolves **#272**.

#### BL-16 — Surface Kraken API deprecations in the UI  ·  *bus-factor insurance*
*Low urgency, high long-term value. Cheap now; impossible to add later when it's needed.*

EMT already detects deprecated Kraken fields each poll (`check_field_deprecations`), but the signal only reaches a log line and `sensor.energy_meter_tracker_api_deprecations` — **neither of which a normal user sees**. Today the only person who would notice Octopus changing the schema is the maintainer, reading logs. If the maintainer becomes less active, EMT drifts silently toward breaking and nobody knows until billing goes wrong.

**The point is not to inform users — it is to make a stranger open an issue.** Design accordingly:
- **Name the affected fields.** "2 deprecations detected" is unactionable; the field names are what gets pasted into a report.
- **Link directly to the issue tracker**, ideally a pre-filled new-issue URL. The gap between "I saw a warning" and "I filed a report" is where this dies.
- **Persistent, dismissed per-deprecation-set** (not per session). If it vanishes on reload, nobody reports it.
- **Must not depend on `publish_ha_sensors`.** Today the only entity EMT publishes is the deprecations sensor, gated behind that option — which is itself slated for removal (BL-17). Once this notification exists, the sensor and the option can both go.

Feed the existing detection into the BL-6 notification region (persistent, `warn` level).

**Known limitation, worth stating:** the check compares the live schema against a hardcoded list of the fields EMT uses. If Octopus *removes* a field outright rather than marking it deprecated, or changes how deprecation is signalled, the detector goes quiet — and a silent detector is worse than none, because it looks like everything is fine.

#### BL-17 — Retire `publish_ha_sensors` and the deprecations sensor
*Small; blocked on BL-16.* Since 3.2.0 removed the four per-block sensors, `publish_ha_sensors` gates exactly one entity: `sensor.energy_meter_tracker_api_deprecations`. Two instances writing it collide harmlessly (both compute the same API-derived value), so it needs no isolation. Once BL-16 surfaces deprecations in the UI, the sensor carries no unique signal and both it and the option can be removed — EMT would then publish no HA entities at all, and `PUBLISH_HA_SENSORS` / the `run.sh` plumbing goes with it. Do **not** remove the sensor before BL-16 lands, or the deprecation signal disappears entirely.

#### BL-7 — Sensor-vs-device-type sanity check
*Low-medium — prevents a class of silent misattribution.* At device creation, warn when the assigned sensor looks inconsistent with the device type (e.g. an EV device pointed at a battery/inverter sensor). Surfaced by the forum device-usage-swap case, where an "Indra Smart Pro" EV device read the Fox battery's register for a week before it was noticed. A creation-time heuristic (entity-ID / device-class / magnitude sanity) that prompts the user to confirm would catch it before it corrupts history.

#### BL-13 — Sub-meter replacement auto-detection
*Medium — resilience.* When a device is replaced and reuses the same sensor entity ID, the cumulative read resets to zero. EMT currently relies on the user to retire the old device and add a new one. Auto-detect a significant mid-block read drop on a sub-meter and prompt the user — similar to the main-meter reset detection added in 2.9.0.

#### BL-10 — Charge-session insight
*Low-medium — a feature, buildable now off existing data.* A per-charge-session view (time on-charge, full-rate window, total kWh, fill curve) derived from the accumulated `dispatch_history` (planned/started/completed). Started-slot count gives the smart-charge window; completed-energy per slot gives the fill/taper curve. 30-minute resolution; minute-accurate actual draw needs a charger power sensor (not universal). Design discussion in `docs/dispatch_validation_design.md`.

#### BL-11 — Keep raw dispatch `startDt`/`endDt` precision
*Tiny — do it next time we touch dispatch capture.* The Octopus API exposes dispatch `startDt`/`endDt` to the **second**; EMT snaps to 30-minute slots at capture and discards it. Retain the raw window boundaries alongside the slotted energy so BL-10 can show exact scheduled-window bounds. (This is the scheduled window, not the car's actual draw.)

#### BL-2 — Phantom export channel from the gap-seed
*Low — cosmetic, never affects billing (kWh = 0).* In a no-export `api` setup the gap-seed carries a 0-kWh export channel forward, so the daily billing chart draws a 12p export-rate line despite zero actual export. Root fix: don't carry forward an export channel that has no configured source and never carried real export.

#### BL-3 — Carry billing-chart rate lines forward as a step
*Low — the agreed near-term fix for BL-2's visible symptom.* Build the import/export rate series by forward-filling the last-known rate (a tariff is in force all day) instead of zero-filling per slot; back-fill leading slots and use a None sentinel so a genuine 0 rate stays distinct from "no data". Exact for a flat tariff; the per-slot schedule-resolve variant is the upgrade path for a varying Agile Outgoing export tariff.

#### BL-4 — PDF export overhaul (pagination + orientation)
*Low — usability, no data impact.* The PDF export relies on the browser's print-to-PDF and doesn't paginate: every chart is forced A4 landscape as fixed-size images that can't reflow or break across pages. **Billing is the worst case** (multiple per-period plots + tables, effectively unprintable in portrait). Fix is a print-layout rethink: portrait-first per-view orientation via a single `@page` rule; real pagination (`break-inside: avoid`, `break-before: page`, `thead` repeated via `display: table-header-group`); reflowable capture sized to page width; optionally move generation server-side (WeasyPrint/reportlab) if the CSS-print route proves too limited.

#### BL-12 — Device apportionment for backfilled blocks (HA recorder)
*Long-term — needs HA recorder-DB research.* The accuracy upgrade to BL-8: reconstruct the sub-meter (EV/battery) split for an outage-backfilled block from **Home Assistant recorder history** — the device sensors kept recording while EMT was down. Requires research into what's available/queryable in the HA recorder DB (schema, retention, access from the add-on). Turns a whole-house-only backfilled block into a proper per-device one.

#### BL-14 — Per-device weighted generation mix in Usage Insights
*Low — insight accuracy.* The generation-mix bar in Carbon Insights shows the grid-wide period average. A device-specific weighted mix (e.g. a battery that charges overnight when the grid is cleaner) would be more accurate and informative.

#### BL-15 — Historical rate correction from bill
*Low.* Populate missing or incorrect rate data from a supplier PDF — useful when the rate sensor was misconfigured for a period and block costs are wrong even though kWh figures are correct.

---

### To triage
- **#270** — new item raised, **needs investigation** (scope/detail TBD — pull up the issue and slot it into the ranking once understood).

---

### Longer-term themes (unversioned)

**Migration shim removal & API polish** — migration shim removal (`migrate_json_to_sqlite` and related), per-meter colour customisation (bidirectional-aware), API versioning. *Carried forward from the pre-3.0 "clean break from legacy" theme.*

**Bright (Hildebrand / Glow) API as a DCC consumption source** — use `api.glowmarkt.com` as an additional source of DCC half-hourly consumption, giving non-Octopus GB smart-meter owners the same block-resolution data. Pluggable alongside Kraken, feeding the same `imp_kwh_api`/`exp_kwh_api` settlement path (and BL-8 backfill).

**Gas meters** — extend the engine to gas alongside electricity. Needs a design spike: m³/ft³ with calorific-value conversion, different billing periods, slower sensor updates.

---

## v3

### 3.2.0 — Feature release with fixes 🔜
Planned — see **Upcoming** above. BL-1 (settlement-freeze fix), BL-8 (outage backfill), BL-5 (instance isolation), BL-6 (notification region + update banner, fixes #219).

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