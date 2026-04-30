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

### 2.6.0 — Carbon Insights & Navigation Refactor ✅ shipped
**Theme: Make per-block carbon data meaningful and surface it in a dedicated Insights page**

#### What shipped
- **Insights page** — Carbon tab with six adaptive cards: Carbon Summary, House Consumption, Solar Export Offset, EV Charging, Battery Charging Behaviour, Heat Pump. Billing period trend chart with click-to-navigate. Period prev/next navigation.
- **Settings page** — Meter Config tab + Carbon tab (equivalence assumptions with citations, EV/battery/heat pump efficiency, export displacement methodology). Postcode inline prompt.
- **Navigation refactor** — sidebar restructured: ⚙️ Settings | 📊 Charts | 🌿 Insights | ⚡ Live Power | 🗄️ Data Management | 📋 Logs | 📖 Help. Routes renamed for clarity.
- **DB as sole source of truth** — `load_config()` never falls back to `meters_config.json` when `blocks.db` exists.
- **Restore reliability** — six fixes to gap detection, gap fill, config persistence after restore, and catch-up rollover contamination. Full diagnostics logging added.
- **Sub-meter rate wording** — clarified as one-time copy at setup, not a live link.

### 2.6.1 ✅ shipped
- Logo click toggles light/dark theme
- Sub-meter rate using weighted average instead of last rate (tariff boundary bug)
- Session gap detection using wrong `block_minutes` on non-30-minute setups
- 1.x.x / 2.0.x Docker upgrade showing wizard and losing sensor config

### 2.6.2 ✅ shipped
- Theme toggles consolidated to logo only (sidebar footer and charts toolbar buttons removed)
- Logo size increased
- Insights mobile topbar compacted
- Insights metric labels simplified
- GitHub wiki link added to Help page

### 2.6.3 ✅ shipped
- Usage Stats data table scrollable on desktop with chart fixed above
- Sticky column headers and totals row pinned in table header
- Sortable date column (↑/↓, newest first default, localStorage)
- Alternate row zebra shading (light and dark mode)
- Colour dots in column headers matching chart legend
- Billing chart day order toggle (↓ Newest first / ↑ Oldest first)
- Heatmap fills available desktop viewport (was hardcoded 31 rows)
- Heatmap metric buttons larger on mobile
- Carbon intensity fetched before catch-up block finalisation (CI ordering fix)
- Deprecated `armhf`/`armv7` arch values removed from config.yaml


### 2.7.0 ✅ shipped
- Battery SoC dial, inverter power gauge, EV/Heat Pump power gauge on Live Power
- Sub-meter card layout with V2X bidirectional gauge
- Sub-meter history endpoint (95th percentile max_kw)
- Meter type selector in Meter Config (locked after data recorded)
- Device power sensor field for EV/HP sub-meters
- Add Device modal redesign
- Config change reason in Billing History

### 2.7.1 ✅ shipped
- CI gap backfill (first run 60 days, subsequent 48hr, postcode-keyed)
- PASS 2 applied to gap-fill blocks (`_apply_pass2` extracted and shared)
- Sub-meter spike detection in gap fill — clips impossible values, logs ERROR
- Insights calendar month/year navigation with floating toolbar
- Narrative comparison panel (vs last month / same month last year / last year)
- Data-bounds gating — compare buttons disabled when no data exists
- Main meter cascade delete — wipes DB, creates backup, restarts engine
- Postcode normalisation — strips to outward code everywhere
- Direct import terminology throughout (billing cards, charts, insights, help)
- Grid import double-count fix in Live Power billing card
- Billing sub-meter breakdown rows restored
- Billing chart sort order toggle
- Regenerate Charts hidden on Usage Stats tab
- Live Power gauge improvements — alignment, pill position, layout A/B, heat pump orange arc
- `_aggregate_insights` 7× performance improvement (direct SQL + covering index)
- New API endpoints: `/api/insights/calendar-month`, `/api/insights/calendar-year`, `/api/insights/data-bounds`, `/api/meter/main/reset`
- 464 tests passing (16 new tests)

---

## Planned

### Known Engine Limitation — Sub-meter Late Finalise

When the addon restarts during active charging, blocks that finalise late (after a restart) use the raw cumulative delta for sub-meters. If no intermediate reads arrived during the gap, this delta can span multiple block windows — e.g. a Zappi charging at 7kW for 72 minutes with no reads may attribute all 8.4 kWh to a single 30-minute block rather than distributing it across the two or three affected blocks.

The main meter avoids this by using boundary interpolation — it finds the nearest reads on either side of each block boundary and computes an exact energy value for each window. Sub-meters currently don't have this applied at late finalise time.

**Symptom:** `PASS 2: ev_charger sub-meter X.XX kWh EXCEEDS parent grid import Y.YY kWh` WARNING in logs. The energy is preserved (not clipped) but is attributed to the wrong block.

**Correct fix:** Apply boundary interpolation to sub-meters at block finalise time using the same pre/post read pair logic as the main meter. The seed value from the previous block's `read_end` serves as the pre-boundary read; the first post-restart read serves as the post-boundary read. This would correctly distribute the energy across each block window proportionally.

**Why not fixed yet:** Requires careful handling of the seed/carry-forward mechanism for sub-meters and interaction with the gap fill path. Likely coupled with the timezone refactor in 2.8.0 since both require changes to how block boundaries are computed.

---

### 2.8.0 — Timezone Refactor, Performance & Usage Insights
**Theme: Correctness, speed and deeper usage analysis**

#### Timezone refactor (coupled with performance)
Drop `local_date`, `local_year`, `local_month`, `local_day` from `blocks` table. Engine fully UTC. `local_date_to_utc_bounds()` helper computes correct UTC window at query time using the configured timezone. Retroactively corrects wrong-timezone users without data migration.

#### Billing/Usage Stats performance (coupled with timezone)
Rewrite `calculate_billing_summary_for_period` and `build_day_chart_html` to work on lightweight SQL rows rather than full block dicts reconstructed via `_rows_to_blocks`. Currently ~470ms per chart regeneration. Direct SQL target ~40ms. Both require changes to the data access layer.

#### Carbon accuracy — dropped
Regional carbon intensity actuals are not available from the NESO API — the `actual`
field only exists on the national endpoint. The regional endpoint (used by the engine)
only ever returns `forecast`. Backfilling regional actuals is therefore not possible.
The forecast values are directionally correct and sufficient for the Insights Carbon tab.
The `intensity_actual` column remains in the schema but will not be populated.

#### Sub-meter boundary interpolation
Apply same pre/post boundary read logic to sub-meters as main meter at block finalise time. Fixes gap block attribution issue (documented in Known Engine Limitation). Requires careful interaction with seed/carry-forward mechanism.

#### Generation mix UI
- Live Power — stacked bar or donut showing current grid fuel split, updates on CI tick
- Insights Carbon Summary — period-weighted average generation mix breakdown using `get_generation_mix_for_range()`

#### Usage Insights tab
New "Usage" tab on the Insights page alongside "Carbon":
- **Cost breakdown** — total spend split by meter, standing charge as £ and %, effective rate (£/kWh)
- **Usage patterns** — peak consumption window, highest consumption day, average daily import, self-sufficiency ratio (% of consumption from battery/solar vs grid)
- **Tariff efficiency** — % of EV/battery charging during cheap rate periods, estimated saving vs peak rate (requires rate sensor history)

---

### 2.9.0 — Meter Replacement
**Theme: Handle real-world meter changes gracefully**

When a physical meter is replaced, cumulative reads reset to zero. The engine clips negative deltas to zero but affected blocks are wrong. Explicit user-triggered correction flow via Billing History with preview and confirmation.

---

### 2.10.0 — Gas Meters
**Theme: Whole-home energy tracking**

Extend the engine to support gas meter recording alongside electricity. Requires a design spike — gas uses m³/ft³ with calorific value conversion, different billing periods, and slower sensor updates.

---

### 3.0.0 — Migration Shim Removal & Polish
**Theme: Clean break from legacy**

- Migration shim removal (`migrate_json_to_sqlite` and related code)
- Meter colour customisation per meter (bidirectional aware)
- API versioning

---
---

## Backlog — Unscheduled Items

### Device Decommissioning
When a physical device (battery, EV charger, heat pump) is replaced or retired, the current
workflow requires deleting the sub-meter entirely — which destroys all historical data.

**Required:** A "decommission" option on sub-meter devices that:
- Stops the engine polling for that meter's entities
- Retains all historical block data for reporting
- Marks the meter as inactive in config so it is excluded from live charts but visible in
  historical billing/usage/carbon views with a clear "decommissioned" label
- Allows the meter to be reactivated if the same device returns

Workaround in the interim: remove the HA entity sensors from the meter config fields
(leave the meter in place) — the engine will stop recording new data but history is preserved.

---