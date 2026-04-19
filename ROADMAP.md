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

---

## Planned

### 2.6.0 — Carbon Insights
**Theme: Make per-block carbon data meaningful — something most energy monitors can't do**

EMT records actual grid carbon intensity at the time of every half-hour block. This enables genuine per-period carbon accounting rather than monthly-total × national-average estimation. 2.6.0 surfaces this as a dedicated **Insights** page, backed by a new **Settings** page for assumption management.

#### New pages

**Insights** — sidebar between Charts and Live Power. Narrative cards with headlines, supporting numbers and equivalences. Adapts based on configured meters — sections irrelevant to the installation are silently omitted. Each card cites its data sources and links to Settings for assumption overrides.

**Settings** — sidebar above Help. Initially contains carbon methodology assumptions with cited defaults and user overrides. Extensible — future releases will add display preferences and other application-level settings. Assumptions stored in a new `settings` key-value table in `blocks.db`.

#### Settings — initial assumptions

| Assumption | Default | Source |
|---|---|---|
| Petrol car gCO₂/mile | 180 g | BEIS/DESNZ 2023 GHG conversion factors |
| Diesel car gCO₂/mile | 168 g | BEIS/DESNZ 2023 |
| Electric kettle rating | 3.0 kW | UK standard |
| Tree CO₂ absorption/year | 21 kg | Woodland Trust |
| Flight LHR→NYC gCO₂/passenger | 670 kg | BEIS 2023 (economy, radiative forcing) |
| Export displacement | Grid average intensity | National Grid ESO — conservative |

All assumptions show their citation in the Insights page footnotes. Overridden values note the custom figure alongside the original cited default.

#### Insights cards

**Billing period carbon summary (all users)**
- Carbon imported (kgCO₂), carbon offset by export (kgCO₂), net position
- Effective intensity (gCO₂/kWh) vs grid average for the same period
- Verdict: "You beat the grid average" / "You were X% above the grid average"
- Bar chart trend across billing periods as history accumulates

**Solar offset story (solar users)**
- Export displaced X kgCO₂ — with equivalences (miles not driven, kettle hours, tree-days)
- Self-consumption ratio: % of generation consumed directly vs exported
- Best and worst carbon days of the period

**EV charging carbon (EV sub-meter)**
- Total EV charging carbon, % of sessions in below-average intensity windows
- Average charging intensity vs grid average
- Estimated saving vs unmanaged flat-rate charging

**Battery carbon efficiency (battery sub-meter)**
- Average charge intensity vs average discharge intensity
- Net carbon cost/benefit of cycling this period
- Days battery was carbon-positive (charged clean, discharged dirty)

#### Cadence
- **Closed periods** — computed on demand, cached. Definitive.
- **Current period** — live provisional estimate, refreshed on block finalise. Clearly marked provisional.

#### Data layer
- `api/insights/billing-period` — aggregates carbon figures for a period from existing block data
- `api/settings` GET/POST — reads and writes the `settings` table
- `settings` table in `blocks.db` — simple key-value, JSON values

#### Build order
1. Settings page + `settings` table + `api/settings`
2. `api/insights/billing-period` endpoint
3. Insights page — billing period summary card + trend chart
4. Solar offset card
5. EV and battery cards

---

### 2.7.0 — Meter Replacement
**Theme: Handle real-world meter changes gracefully**

When a physical meter is replaced, cumulative reads reset to zero. The engine clips negative deltas to zero but affected blocks are wrong. Explicit user-triggered correction flow via Billing History with preview and confirmation.

---

### 2.8.0 — Gas Meters
**Theme: Whole-home energy tracking**

Extend the engine to support gas meter recording alongside electricity. Requires a design spike — gas uses m³/ft³ with calorific value conversion, different billing periods, and slower sensor updates.

---

### 2.9.0 — Charting Insights
**Theme: Understand your energy patterns**

- Cost forecasting — projected bill based on current period rate
- Peak demand analysis — highest consumption periods and times of day
- Solar self-consumption ratio (requires solar generation sub-meter)
- Tariff optimisation hints (Agile, Go charging windows)
- Day-of-week and seasonal consumption patterns


## Longer Term / Unscheduled

- **Solar generation tracking** — export sub-metering and self-consumption breakdown
- **V2G / V2X export** — breakdown of EV-to-grid export by device
- **Multiple batteries / inverters** — better support for complex hybrid systems
- **Multi-dwelling / multi-site** — support for properties with more than one grid connection
- **HACS / community distribution** — evaluate distribution channels beyond the add-on store

---

## Release Principles

- Each release has a clear theme and a testable scope
- Billing accuracy is never compromised by new features
- Breaking changes (data format, config schema) require a migration path and deprecation notice
- The reconciliation block is the authoritative unit — higher-resolution features are additive, not replacements
- User data is never deleted without explicit confirmation
- Migration tools are maintained for at least one full minor release after the migration they support
