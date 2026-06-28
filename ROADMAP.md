# Roadmap

This document outlines the planned release trajectory for Energy Meter Tracker. Scope and timing are subject to change.

---

## Released

### 1.0.0 — Initial Release
Core half-hour metering engine, sub-meter support, gap filling, billing charts, HA sensor publishing.

### 1.1.0 — Web UI
Flask-based web UI with Meter Config, Charts, Import & Backup, Logs and Help pages.

### 1.2.0 — Setup Wizard
Guided setup wizard for first-time configuration of main meter and sub-meters.

### 1.3.x — Stability & Timezone
Timezone-aware chart rendering, UTC timestamp fixes, silent sensor timeout fix, standing charge billing fix.

### 1.4.0 — Global Readiness
Configurable reconciliation period (5/15/30 min), automatic currency detection, international sensor compatibility.

### 1.5.0 — Live Power Gauge
Live power gauge, carbon intensity forecast, billing cards (Today / This Bill / This Year), billing auto-refresh.

### 1.6.0 — Usage Stats & Theme
Usage Stats chart (daily/monthly/yearly with sub-meter breakdown), global light/dark theme toggle, remember last page, mobile improvements.

### 1.6.x — Polish & Fixes
Billing/Calendar period toggle, data table totals column, heatmap mobile fixes, light/dark mode fixes throughout.

### 2.0.0 — SQLite Foundation
SQLite storage replacing `blocks.json`, Billing History page, config period chain, billing period transition logic, fast SQL aggregation, gauge scale cache.

### 2.1.0 — Full SQLite: Single Source of Truth
All state in `blocks.db`. Fully normalised schema — no JSON blobs. `cumulative_totals.json`, `current_block.json` and `meters_config.json` eliminated as authoritative state. Historical Corrections enhanced with time-of-day window and per-meter targeting.

### 2.1.x — Sub-meter & Billing Fixes
Sub-meter flags in reconstructed block dicts, double-counting fixes in cumulative totals and billing, standing charge corrections restricted to main meter, colour sync between Billing chart and Usage Stats.

### 2.2.0 — Data Management
Bill summary redesigned. Delete Blocks page. Historical Corrections promoted to own page. Compact Database. Lovelace chart endpoints.

### 2.2.x — Chart & Billing Fixes
Auto-refresh improvements, cache headers, chart regeneration optimisation, gap-fill rate and spike fixes.

### 2.3.0 — Carbon Tracking
Carbon intensity recording from National Grid ESO API (UK, postcode required). `carbon_g` per block. 48-hour power history chart with kW/CO₂ toggle. Hover tooltip.

### 2.4.0 — Carbon in Usage Stats
CO₂ metric in Usage Stats (net + totals views). `carbon_g_imp`/`carbon_g_exp` split. Reliable backup and restore (WAL flush, engine pause/reset). Server-side zip restore. Upgrade safety backup on first start per version.

### 2.4.1 — Carbon Accounting Fixes
Blue bar shows house remainder not full main meter net carbon. `carbon_g_total` double-count removed. Mixed CO₂ units within a chart render fixed. Per-block `carbon_g_imp`/`carbon_g_exp` to handle days with mixed NULL/non-NULL carbon data.

### 2.5.0 — Carbon Heatmaps & UI Polish
Heatmap metric toggle (kWh / gCO₂ / gCO₂/kWh). Effective intensity column in Usage Stats CO₂ mode. Power history drag zoom. Usage Stats auto-refresh TTL. Tab buttons in topbar. Floating toolbars. Fixed topbar across all pages.

### 2.5.x — Stability & Mobile
Touch zoom, mobile topbar collapse, orientation fixes, billing landscape fix, chart height fix, Y axis duplicate labels, stale test timestamp, storage monitoring card.

### 2.6.0 — Carbon Insights & Navigation Refactor ✅
Insights page with Carbon tab (six adaptive cards). Settings page with Meter Config and Carbon tabs. Navigation refactor. DB as sole source of truth. Restore reliability fixes. 

### 2.6.1 ✅
Logo click toggles theme. Sub-meter rate weighted average fix. Session gap detection block_minutes fix. 1.x Docker upgrade fix.

### 2.6.2 ✅
Theme toggle consolidated to logo. Logo size increased. Insights mobile improvements. GitHub wiki link added to Help.

### 2.6.3 ✅
Usage Stats data table scrollable with sticky headers and totals. Sortable date column. Zebra shading. Billing chart day order toggle. Heatmap viewport fix. CI fetch ordering fix.

### 2.7.0 ✅
Battery SoC dial, inverter power gauge, EV/Heat Pump power gauge on Live Power. Sub-meter card layout. Meter type selector. Add Device modal redesign. Config change reason in Billing History.

### 2.7.1 ✅
CI gap backfill. PASS 2 applied to gap-fill blocks. Sub-meter spike detection. Insights calendar navigation. Narrative comparison panel. Data-bounds gating. Main meter cascade delete. Postcode normalisation. Direct import terminology. 464 tests.

### 2.8.0 — Timezone Refactor, Performance & Usage Insights ✅
Timezone refactor (UTC throughout, local_date columns dropped). Billing chart performance (5.8 MB → 76 KB JS). Usage Insights tab (cost breakdown, rate period distribution, grid position, peak demand window, device costs, comparison narrative). Generation mix donut on Live Power and 48-hour mix chart. Generation mix in Carbon Insights.

### 2.8.1 ✅
Favicon. Timezone auto-detect in wizard. PDF export on Insights. Generation mix history at CI-tick resolution. Usage Stats Net view Import/Export columns. Gauge arc light theme fix. Charts period recall fix.

### 2.8.2 ✅
PDF export fixes — carbon report duplicate panel, usage report hidden card exclusion.

### 2.8.3 — incorporated into 2.9.0
Usage PDF showing carbon comparison narrative fix.

### 2.9.0 — Gap-fill Limit, Meter Reset Advisory & Device Retirement ✅
- **12-hour gap-fill limit** — gaps > 12 hours are not gap-filled. Handles extended CAD/HA outages, meter replacement, moving property. Short gaps (power cuts, brief restarts) still gap-fill as before.
- **Meter reset advisory** — when a gap > 12 hours is followed by a significantly lower import read, an advisory banner suggests creating a new billing period. Covers meter replacement and moving property (both produce a multi-day HAN re-pairing gap).
- **Device retirement** — sub-meters can be archived from a specific date without deleting historical data. Sensor entity IDs freed for reuse. Retirement reversible. Main meter cannot be retired.
- **PDF fix** — Usage Insights PDF no longer shows carbon comparison narrative.
- 556 tests passing.

### 2.10.0 — Sub-meter Boundary Interpolation ✅
- **Provisional block amendment** — sub-meter blocks written without a post-boundary read are retrospectively corrected (boundary-interpolated) within ~10 seconds of the post-boundary read arriving. PASS 2 re-run after each amendment. Non-cascading.
- **`imp_provisional` DB column** — flags provisional sub-meter rows; cleared on amendment. Auto-migrated on first start.
- 122 tests passing.

### 3.0.0 — DCC Settlement, Carbon & Intelligent Octopus Go ✅
**The largest release since EMT began.** Reconciles every block against settled half-hourly data from the Kraken platform (Octopus / DCC): settled import/export, unit rates and standing charges on a schedule, an automatic settlement sweep with a horizon back-stop, and a Meter-sensor-vs-Supplier-API billing toggle. Adds data-source modes (`cad`, `cad+api`) with supplier-first wizard setup; Octopus Home Mini live power; the **Intelligent Octopus Go dispatch overlay** (captures smart-charge slots and reprices affected blocks off-peak, with per-device overlay rates and a 0.1 kWh validation floor); whole-app **grid carbon intensity** (Carbon Insights, CO₂ columns and charts, gCO₂/kWh heatmap, one-time backfill); per-channel rate source (API vs sensor); power-sensor invert & unit override; and Ohme EV charger support. Major-additive — nothing breaks; everything new is opt-in.

### 3.0.1 ✅
Post-release fixes: #217 (account-number hint that led to an unhelpful API-key error), #218 (misleading device-name "(optional)" label), #221 ("This Bill" defaulting to a stale historical date when the current period has no data yet), #223 (Generation Mix card overlapping a neighbour with three or more devices on the Overview).

---

## Planned

### 3.x — Migration shim removal & API polish
**Theme: Clean break from legacy** — *carried forward; these were pencilled in as "3.0.0" before 3.0.0 shipped as the DCC / carbon release.*

- Migration shim removal (`migrate_json_to_sqlite` and related code)
- Meter colour customisation per meter (bidirectional aware)
- API versioning

---

### 3.x — Bright (Hildebrand / Glow) API as a DCC consumption source
**Theme: DCC data beyond Octopus**

Use the Hildebrand **Bright / Glow** API (`api.glowmarkt.com`) as an additional source of DCC half-hourly smart-meter consumption data. EMT currently obtains settled DCC figures only via Kraken (Octopus); Bright would give owners of GB smart meters on other suppliers access to the same block-resolution usage data, broadening settlement and gap-backfill coverage beyond Octopus. Pluggable alongside the existing Kraken source and feeding the same `imp_kwh_api` / `exp_kwh_api` settlement path.

---

### 3.x — Gas meters
**Theme: Whole-home energy tracking** — *deferred from 2.11.0.*

Extend the engine to support gas meter recording alongside electricity. Requires a design spike — gas uses m³/ft³ with calorific value conversion, different billing periods, and slower sensor updates.

---

## Engineering backlog — post-3.0 fixes

*Surfaced while diagnosing the dev system on the `api` + Mini path. Full forensic detail in `POST-3.0-BACKLOG.md`; all are candidates for a 3.0.x point release and none block anything.*

### BL-1 — Gap marker globally freezes DCC settlement application
*Medium — the only correctness item.* An outage longer than the 12-hour gap-fill limit leaves the gap marker set, which globally gates the PASS-2 drain — so DCC-settled figures are ingested but never **applied** to billing (across the whole history) until the source returns or the add-on restarts. Fix: make the drain gap-window-aware so blocks outside the gap window still settle, rather than gating globally.

### BL-2 — Phantom export channel from the gap-seed
*Low — cosmetic, never affects billing (kWh = 0).* In a no-export `api` setup the gap-seed carries a 0-kWh export channel forward, so the daily billing chart draws a 12p export-rate line despite zero actual export. Root fix: don't carry forward an export channel that has no configured source and never carried real export.

### BL-3 — Carry billing-chart rate lines forward as a step
*Low — the agreed near-term fix for BL-2's visible symptom.* Build the import/export rate series by forward-filling the last-known rate (a tariff is in force all day) instead of zero-filling per slot; back-fill leading slots and use a None sentinel so a genuine 0 rate stays distinct from "no data". Exact for a flat tariff; the per-slot schedule-resolve variant is the upgrade path for a varying Agile Outgoing export tariff.

---

## Backlog — Unscheduled

### Extended outage backfill

When a CAD or HA device fails for weeks, EMT has no data for that window. Several backfill sources are possible, in order of accuracy:

**Supplier API (preferred)** — suppliers with open APIs (e.g. Octopus Energy) can return actual half-hourly consumption data. EMT already stores MPAN in `meter_channels`. This would fill gaps with real data at block resolution, no distribution algorithm needed, and rate data is available from the same API. Pluggable per supplier.

**Supplier PDF bill** — parse the bill to extract total kWh and rate period breakdown, then distribute the missing kWh across the gap window using the user's historical consumption pattern (same time-of-day, same day-of-week from surrounding periods). Less accurate than API data. Needs a pluggable parser per supplier (Octopus, British Gas, EDF etc.) — PDF formats vary significantly.

**Constraints for both approaches:**
- All backfilled blocks flagged as `interpolated=2` (externally sourced) distinct from live and gap-fill blocks
- User reviews and confirms the proposed fill before blocks are written
- No user-typed kWh values — must come from a verified external source to prevent data corruption
- Sub-meter attribution uses historical device proportions for the gap window

### Per-device weighted generation mix in Usage Insights

The generation mix bar in Carbon Insights shows the grid-wide average for the period. For devices that charge predominantly during specific conditions (e.g. battery charges overnight when the grid is cleaner), a device-specific weighted mix would be more accurate and informative.

### Sub-meter replacement auto-detection

When a device is replaced and reuses the same sensor entity ID, the cumulative read resets to zero. EMT currently relies on the user to retire the old device and add a new one. Auto-detection of a significant mid-block read drop on a sub-meter could prompt the user automatically — similar to the main meter reset detection added in 2.9.0.

### Historical rate correction from bill

Populate missing or incorrect rate data from a supplier PDF — useful when the rate sensor was misconfigured for a period and block costs are wrong even though kWh figures are correct.