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

---

## Planned

### 2.10.0 — Sub-meter Boundary Interpolation

#### Sub-meter boundary interpolation

**Problem:** Sub-meters currently use raw reads bracketing the block window rather than interpolated boundary values. The last pre-boundary read is carried as a seed into the next block so no kWh is lost — but it appears in a slightly different block than it should. For a 60-second update device the maximum misalignment per boundary is ~0.12 kWh at 7.4 kW.

**Key constraint:** This only affects the **distribution** of kWh between adjacent blocks, never the period total. Sub-meter calibration drift (CT clamps, inverter sensors) has the same property — drift shifts costs between device cards and the Direct Import remainder but never changes the billing total. PASS 2 enforces `sum(sub-meters) + remainder = main meter import` at every block.

**Design:**
- Block finalisation writes a **provisional** sub-meter figure when there is no post-boundary read yet
- The first post-boundary sub-meter read triggers a **retrospective amendment** of the previous block:
  - Interpolate the sub-meter to the exact boundary using the bracketing pre/post reads
  - Re-run PASS 2 with the corrected sub-meter figure
  - Re-write the amended block to the DB
  - Republish HA sensors with corrected cumulative totals
- Amendment must not cascade — subsequent blocks already have their own reads and are unaffected

**Affected code:** `finalise_block` PASS 1 sub-meter path, `_apply_pass2`, `block_store.py` (provisional flag), HA sensor publish.

---

### 2.11.0 — Gas Meters
**Theme: Whole-home energy tracking**

Extend the engine to support gas meter recording alongside electricity. Requires a design spike — gas uses m³/ft³ with calorific value conversion, different billing periods, and slower sensor updates.

---

### 3.0.0 — Migration Shim Removal & Polish
**Theme: Clean break from legacy**

- Migration shim removal (`migrate_json_to_sqlite` and related code)
- Meter colour customisation per meter (bidirectional aware)
- API versioning

---

## Backlog — Unscheduled

### Bill PDF reader for extended outage backfill

When a CAD or HA device fails for weeks, EMT has no data for that window. The only authoritative source is the supplier bill. A PDF reader could parse the bill, extract total kWh and rate period breakdown, and distribute the missing kWh across the gap window using the user's historical consumption pattern. All backfilled blocks would be flagged as bill-derived. Needs a pluggable parser per supplier and careful design to prevent data corruption — no user-typed kWh values.

### Per-device weighted generation mix in Usage Insights

The generation mix bar in Carbon Insights shows the grid-wide average for the period. For devices that charge predominantly during specific conditions (e.g. battery charges overnight when the grid is cleaner), a device-specific weighted mix would be more accurate and informative.

### Sub-meter replacement auto-detection

When a device is replaced and reuses the same sensor entity ID, the cumulative read resets to zero. EMT currently relies on the user to retire the old device and add a new one. Auto-detection of a significant mid-block read drop on a sub-meter could prompt the user automatically — similar to the main meter reset detection added in 2.9.0.

### Historical rate correction from bill

Populate missing or incorrect rate data from a supplier PDF — useful when the rate sensor was misconfigured for a period and block costs are wrong even though kWh figures are correct.