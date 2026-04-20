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

---

## Planned

### 2.6.0 — Carbon Insights & Navigation Refactor *(in development)*
**Theme: Make per-block carbon data meaningful and surface it in a dedicated Insights page**

EMT records actual grid carbon intensity at the time of every block. 2.6.0 surfaces this as a dedicated **Insights** page backed by a **Settings** page for assumption management, and refactors the navigation to reflect the evolved UI.

#### Navigation refactor
- Sidebar renamed and restructured: ⚙️ Settings | 📊 Charts | 🌿 Insights | ⚡ Live Power | 🗄️ Data Management | 📋 Logs | 📖 Help
- **Settings** absorbs Meter Config as its default tab, with a 🌿 Carbon tab for assumptions
- Meter Config sub-bar: Wizard | Refresh | Billing History actions
- All routes renamed for clarity: `/config` → `/settings`, `/import` → `/data-management`, `/summary` → `/live-power`, `/config-history` → `/billing-history`
- Templates renamed to match: `meter_config.html`, `live_power.html`, `data_management.html`, `billing_history.html`
- `blocks.db` confirmed as sole source of truth — `load_config()` in both `server.py` and `engine.py` never falls back to `meters_config.json` when `blocks.db` exists

#### Settings page
- ⚙️ Meter Config tab — existing Meter Config content, unchanged
- 🌿 Carbon tab — carbon equivalence assumptions with citations, export displacement methodology, EV/battery/heat pump assumptions, distance unit (miles/km)
- Postcode prompt inline if not configured
- Assumptions stored in `settings` key in `store_meta` table (no schema change)

#### Settings — assumptions
| Assumption | Default | Source |
|---|---|---|
| Petrol car gCO₂/mile | 180 g | BEIS/DESNZ 2023 |
| Diesel car gCO₂/mile | 168 g | BEIS/DESNZ 2023 |
| Tree CO₂/year | 21 kg | Woodland Trust |
| Flight LHR→NYC | 670 kgCO₂ | BEIS 2023 (economy, RF) |
| Distance unit | miles | — |
| EV efficiency | 3.2 miles/kWh | UK average |
| EV charge efficiency | 88% | IEA 2022, Type 2 AC |
| Battery round-trip | 90% | Li-ion home battery |
| Heat pump SCOP | 3.0 | Typical air-source |
| Gas boiler efficiency | 90% | Modern condensing |
| Gas gCO₂/kWh | 203 g | BEIS/DESNZ 2023 |
| Export displacement | Grid average | National Grid ESO |

#### Insights page — Period view
Cards adapt based on configured meters. Irrelevant cards are silently omitted.

**Carbon Summary** — net kgCO₂, effective intensity vs grid average, import/export split, equivalences (car miles, tree days, flight %), billing period trend chart (inline SVG, click-to-navigate).

**House Consumption** — grid import minus EV and battery charging. Baseline load — lighting and appliances. % of total import.

**Solar Export Offset** — export displaced X kgCO₂. Equivalences. Note that export may include battery discharge.

**EV Charging** — total carbon, average charge intensity vs grid average, estimated distance (using efficiency + charge efficiency assumptions), gCO₂/mile vs petrol/diesel comparison.

**Battery Charging Behaviour** — average charge intensity vs grid average, estimated carbon saving if charged cleaner than average. Honest note: cannot split export between solar and battery without additional metering.

**Heat Pump** — electricity used, heat delivered (kWh × COP), carbon vs equivalent gas boiler, % cleaner, grid crossover intensity.

#### Insights navigation architecture
- 🌿 Carbon tab (active) | Usage tab (hidden, placeholder for 2.7.0)
- Period selector: prev/next arrows in topbar, label shows billing period dates
- Trend chart: inline SVG bar chart, click any bar to navigate to that period
- Three states: no postcode → collecting data → full insights

#### Data layer
- `GET /api/insights/periods` — list all billing periods with carbon summary
- `GET /api/insights/billing-period?period_start=YYYY-MM-DD` — full carbon breakdown including sub-meter avg charge intensity and house remainder
- `GET/POST /api/settings` — reads/writes `settings` table
- Meter type detection: `meta.meter_type` key first, falls back to meter ID keywords (ev, charger, battery, heat, pump)

#### Help page additions
- Cost sensor setup guide — standard variable, Economy 7, Octopus Go, IOG template, Agile (API), no-cost-data scenario

---

### 2.7.0 — Carbon Insights: Comparisons
**Theme: Longitudinal carbon analysis — understand trends over time**

With 12+ months of per-block carbon data, meaningful comparisons become possible. This release adds comparison views to the Insights page.

#### Insights navigation expansion
The Insights topbar Carbon tab gains sub-navigation:

| View | Description |
|---|---|
| **Period** | Existing detail view for one billing period (2.6.0) |
| **Month** | Current billing period vs previous period + same period last year |
| **Year** | Annual summary — all billing periods aggregated |

#### Month comparison view
- Side-by-side: selected period vs previous period vs same period last year (where available)
- Delta cards: carbon ↑↓, intensity ↑↓, EV carbon ↑↓, house consumption ↑↓
- "Your carbon was X% lower than this time last year"
- Grid intensity delta: "The grid was X gCO₂/kWh cleaner than last year" — separates your behaviour from grid improvement

#### Year summary view
- Total carbon for the year, breakdown by category (house, EV, battery, solar offset)
- Best and worst billing period for carbon intensity
- Trend chart showing all periods in the year
- Year-on-year comparison card (requires 2 years of data)
- "Annual carbon report" exportable summary

#### Year-on-year
- Year selector: [2025] [2026▼]
- Same metrics as month comparison but at annual scale
- Highlights grid decarbonisation contribution vs behaviour change contribution

#### Data layer additions
- `GET /api/insights/year-summary?year=YYYY` — aggregate all periods in a year
- `GET /api/insights/compare?period_a=YYYY-MM-DD&period_b=YYYY-MM-DD` — delta between two periods

---

### 2.8.0 — Meter Replacement
**Theme: Handle real-world meter changes gracefully**

When a physical meter is replaced, cumulative reads reset to zero. The engine clips negative deltas to zero but affected blocks are wrong. Explicit user-triggered correction flow via Billing History with preview and confirmation.

---

### 2.9.0 — Gas Meters
**Theme: Whole-home energy tracking**

Extend the engine to support gas meter recording alongside electricity. Requires a design spike — gas uses m³/ft³ with calorific value conversion, different billing periods, and slower sensor updates.

---

### 2.10.0 — Cost Sensor Guidance & Tariff Templates
**Theme: Lower the barrier for users without API cost sensors**

- Help page: step-by-step cost sensor setup guide for standard variable, Economy 7, Octopus Go, IOG, Agile
- IOG template: normalised HA template handling 6-hour rolling cheap window resetting at noon
- Tariff template library in UI: select your tariff type, get a copy-pasteable HA template

---

### 3.0.0 — Charting Insights
**Theme: Understand your energy patterns**

- Peak demand analysis — highest consumption periods and times of day
- Solar self-consumption ratio (requires solar generation sub-meter)
- Tariff optimisation hints (Agile, Go charging windows)
- Day-of-week and seasonal consumption patterns
- Cost forecasting — projected bill based on current period rate

---

## Longer Term / Unscheduled

- **Solar generation tracking** — export sub-metering and self-consumption breakdown
- **V2G / V2X export** — breakdown of EV-to-grid export by device
- **Multiple batteries / inverters** — better support for complex hybrid systems
- **Multi-dwelling / multi-site** — support for properties with more than one grid connection
- **Usage Insights** — non-carbon insights: peak times, highest consumption days, standing charge as % of bill
- **HACS / community distribution** — evaluate distribution channels beyond the add-on store

---

## Release Principles

- Each release has a clear theme and a testable scope
- Billing accuracy is never compromised by new features
- Breaking changes (data format, config schema) require a migration path and deprecation notice
- The reconciliation block is the authoritative unit — higher-resolution features are additive, not replacements
- `blocks.db` is the single source of truth — no other file takes precedence when the DB exists
- User data is never deleted without explicit confirmation
- Migration tools are maintained for at least one full minor release after the migration they support