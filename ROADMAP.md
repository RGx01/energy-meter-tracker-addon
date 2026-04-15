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

### 2.0.0 — SQLite & Billing History
SQLite storage replacing blocks.json, config period history, billing-accurate charts and Live Power cards, fast SQL aggregation.

### 2.1.0 — Full SQLite: Single Source of Truth
All state in one DB file. Fully normalised schema — no JSON blobs. cumulative_totals.json, current_block.json and meters_config.json eliminated as live state. Enhanced Historical Corrections.

### 2.1.x — Stability
Sub-meter billing accuracy, gap-fill reliability, startup crash fixes, colour consistency across charts.

### 2.2.0 — Data Management
Bill summary redesign, Delete Blocks page, Historical Corrections promoted to own page, Compact Database, Lovelace-friendly chart endpoints.

### 2.2.x — Fixes
Billing totals double-counting, Usage Stats billing/calendar toggle, blocks.json import removed from UI.

### 2.3.0 — Carbon Intensity & Power History
48-hour power history chart with kW/CO₂ toggle, hover tooltip, carbon intensity recording, `carbon_g` on blocks, `/api/power/history` and `/api/carbon/current` endpoints.

---

## Planned

### 2.4.0 — Carbon Footprint Tracking
**Theme: Understand and communicate your environmental impact**

The `carbon_g` column has been recorded on every block since 2.3.0. This release surfaces that data across the UI, and improves the power history chart for high-resolution exploration.

**Power history chart improvements:**
- **Incremental polling** — `/api/power/history` gains a `?since=` parameter; the browser sends its last `captured_at` and only receives new rows, merging them into the existing dataset rather than replacing it on every 30-second refresh. Reduces payload from ~1.7 MB to a few KB per poll once the initial load is complete.
- **Drag-to-zoom** — click and drag on the chart canvas to zoom into a time window; double-click or a reset button to return to the full 48-hour view. Particularly useful given the ~10-second resolution of the underlying data.

- **Daily / monthly / all-time carbon footprint** — `SUM(carbon_g)` aggregations in the same pattern as kWh and cost; grouped by `local_date`, `local_year`/`local_month`
- **Usage Stats** — carbon tab or additional row alongside kWh and cost, switchable between daily/monthly/yearly views
- **Bill summary** — total net carbon for the billing period shown alongside import cost
- **Live Power cards** — Today / This Bill / This Year carbon totals
- **NULL handling** — graceful display when `carbon_g` is NULL for pre-2.3.0 blocks; a note explains when recording began
- **Back-fill tool** (optional) — fetch historical CI from National Grid API and apply `(imp_kwh - exp_kwh) × intensity` retroactively to older blocks; API does provide historical data so this is feasible

> Carbon figures are informational only and reflect grid carbon intensity at the time of consumption, not a certified emissions measurement.

---

---

### 2.5.0 — Meter Replacement
**Theme: Handle real-world meter changes gracefully**

When a physical meter is replaced, cumulative reads reset to zero, creating a discontinuity that produces garbled blocks. The engine already clips negative deltas to zero, so the damage is contained but the affected blocks are wrong.

**Design decisions:**
- Triggered explicitly by the user via Billing History — no automated MPAN-change detection. Automated detection creates risk of typo-triggered recalculations that are difficult to undo.
- `meter_replaced INTEGER DEFAULT 0` audit flag on `config_periods` — replacement periods are visually distinguished (🔄 icon) in the Billing History page.

**Flow — "Meter Replaced" button on active config period:**
1. User selects which meter was replaced and the replacement date
2. Preview shows affected blocks: interpolated gap-fill blocks spanning old→new meter reads, plus the one straddling block where `read_end` is from the new meter and `read_start` from the old. Total kWh being zeroed shown for sanity check.
3. On confirm:
   - New config period created from replacement date (`change_reason = "Meter replaced — {meter_id}"`, `meter_replaced = 1`)
   - Interpolated blocks in window zeroed (`imp_kwh`, `imp_cost`, `exp_kwh`, `exp_cost` → 0, `interpolated` flag preserved)
   - Straddling block: `read_start` / `read_end` nulled (values are meaningless old→new meter crossings; `imp_kwh` is already 0 from engine clipping)

**Scope of damage:**
- Typical case (app online throughout): one block wrong — the 30-min window straddling the replacement. Loss is ~0.1–0.3 kWh, unrecoverable.
- App offline during replacement: gap-filled blocks spanning old read → new meter baseline are interpolated nonsense. Zeroing these is correct and the user should accept the data loss for that window.

**What is NOT in scope:**
- Automatic MPAN change detection — too fragile, typos cause cascading recalcs
- Retroactive recovery of lost kWh — the reads from the old meter's final moments are gone
- Sub-meter replacement — sub-meters use delta reads from session-based sensors which reset naturally; the problem only exists for cumulative main meter reads

---

---

### 2.6.0 — Gas Meters
**Theme: Whole-home energy tracking**

Extend the engine to support gas meter recording alongside electricity.

- Gas uses m³ or ft³ — requires calorific value and correction factor conversion to kWh
- Billing periods and standing charges may differ from electricity
- Gas meters update less frequently than smart electricity meters
- Separate chart views and a combined electricity + gas billing summary

> Requires a design spike before development begins.

---

### 2.7.0 — Charting Insights
**Theme: Understand your energy patterns**

New analytical views. Planned after Gas Meters so insights can reflect whole-home consumption.

- Cost forecasting — projected bill based on current period consumption rate
- Peak demand analysis — highest consumption periods and times of day
- Solar self-consumption ratio (requires solar generation sub-meter)
- Tariff optimisation hints (e.g. best EV charging windows for Agile tariff users)
- Day-of-week consumption patterns

---

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