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

---

- **Live Power performance** — instant page load with async billing cards; SQL aggregation replaces full block scans
- **Historical Corrections** — bulk-update standing charge or import/export rates across a date range via the Import & Backup page
- **Billing alignment** — kWh, cost and standing charge now agree between Billing chart, Usage Stats and Live Power including BST period boundaries
- **Config history fixes** — deleting the active period restores `meters_config.json` from the predecessor; restoring `meters_config.json` from backup syncs the active config period in the DB

---

### 2.1.0 — Full SQLite: Single Source of Truth
**Theme: One database file, fully relational, no JSON blobs**

All state is now in `energy_meter.db`. Backup and restore is a single file copy. The schema is fully normalised — no JSON blobs anywhere.

- `cumulative_totals.json`, `current_block.json`, `meters_config.json` eliminated as live state
- `full_config_json`, `gap_marker`, `meter_channel_meta` blobs/tables replaced by normalised columns
- `migrate_full_config_json()` — automatic upgrade from 2.0.x, idempotent
- Historical Corrections enhanced — time-of-day window, per-meter targeting, per-block preview

### 2.1.x — Gap Fill & Stability
- Gap fill overhaul (4 bugs) — startup crash, silent skip, false sub-meter spike, wrong post-gap read
- Chart auto-refresh tab detection fixed (`data-chart` attributes)
- Standing charge corrections restricted to main meter rows only
- Supplier field added to config periods and Billing History

### 2.2.0 — Data Management
**Theme: Give users control over their data**

- **Bill summary redesign** — total grid draw shown at top matching supplier bill, sub-meter breakdown indented beneath
- **Delete Blocks** — sub-page to permanently remove blocks for a date range with preview and confirmation
- **Historical Corrections** — promoted to own sub-page under Data Management
- **Compact Database** — VACUUM on demand with engine pause, reports size before/after
- **Lovelace chart endpoints** — `/lovelace/billing` and `/lovelace/heatmap` with auto-refresh and no-cache headers
- **Chart flicker fixed** — meta refresh removed from generated HTML; crossfade swap prevents blank flash on 2-minute refresh
- `migrate_json_to_sqlite()` retained — removal deferred to 3.0.0

### 2.2.1 — Bug Fix
- **Billing totals double-counting** — sub-meter blocks with NULL `imp_kwh_grid` no longer fall back to raw `imp_kwh`, preventing double-counting in Live Power Today/Bill/Year cards

---

## Planned

### 2.3.0 — Meter Replacement
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
- Sub-meter replacement — sub-meters use delta reads from session-based sensors (e.g. Zappi charge added) which reset naturally; the problem only exists for cumulative main meter reads

---

### 2.4.0 — High-Resolution Charting
**Theme: See what's really happening within each block**

Capture sensor data at full resolution (e.g. every 10 seconds) for charting, while keeping reconciliation blocks for billing accuracy. The `reads` table (populated since 2.1.0) is the data source.

- High-res data already stored per sensor capture in the `reads` table
- Configurable retention (default 7 days — storage is significant at 10s resolution)
- Daily charts rendered from high-res data when available, falling back to block data for older periods
- No impact on billing calculations — reconciliation blocks remain authoritative

---

### 2.5.0 — Gas Meters
**Theme: Whole-home energy tracking**

Extend the engine to support gas meter recording alongside electricity.

- Gas uses m³ or ft³ — requires calorific value and correction factor conversion to kWh
- Billing periods and standing charges may differ from electricity
- Gas meters update less frequently than smart electricity meters
- Separate chart views and a combined electricity + gas billing summary

> Requires a design spike before development begins.

---

### 2.6.0 — Charting Insights
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