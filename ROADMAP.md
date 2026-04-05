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

## In Development (unreleased — basis for 2.1.0)

### SQLite & Billing History
- **SQLite storage** — all blocks in an indexed database; automatic migration from `blocks.json`
- **Billing History** — config periods tracked; billing charts use historically correct billing day and rates
- **Billing period transitions** — truncation-only model; correct period boundaries in usage stats and live power
- **Live Power performance** — instant page load with async billing cards; SQL aggregation replaces full block scans
- **Historical Corrections** — bulk-update standing charge or import/export rates across a date range via the Import & Backup page
- **Billing alignment** — kWh, cost and standing charge now agree between Billing chart, Usage Stats and Live Power including BST period boundaries
- **Config history fixes** — deleting the active period restores `meters_config.json` from the predecessor; restoring `meters_config.json` from backup syncs the active config period in the DB

---

## Planned

### 2.1.0 — Full SQLite: Eliminate JSON Files
**Theme: Single source of truth — one database, no JSON state files**

The unreleased SQLite work moved blocks to the DB but left three JSON files as live state. This release completes the transition so the database is the only thing that needs to be backed up or restored, and the codebase is significantly simpler.

**`cumulative_totals.json` → derived from `blocks` table** (trivial)

`SUM(imp_kwh)`, `SUM(exp_kwh)`, `SUM(imp_cost)`, `SUM(exp_cost)` from the blocks table gives the same numbers. On startup the engine runs this query instead of loading the file. The file is no longer written after each finalise.

**`meters_config.json` → `config_periods.full_config_json`** (low risk)

The active config period's `full_config_json` already contains the complete meters config. The engine and server query the active config period directly. The file becomes a convenience export only — written on config save for human readability, never read back as live state.

Migration: on startup, if `config_periods` is empty and `meters_config.json` exists, use the file to seed the first period (existing behaviour). If `config_periods` has rows, ignore the file entirely.

**`current_block.json` → `current_block` table + `reads` table** (most complex)

The in-progress block contains a rolling reads buffer and gap marker — the engine writes it every tick. The `reads` table schema and `insert_read()` already exist but the engine never calls them.
- Add a `current_block` table (one row: block_start, block_end, gap_marker, serialised state)
- Engine writes each sensor capture to the `reads` table rather than accumulating in JSON
- On startup, reconstruct in-progress state from `current_block` + `reads` instead of `current_block.json`

**Backup and restore simplification**

Once all state is in the DB, backup = SQLite online backup API. Restore = copy the file. The Import & Backup page gains:
- Download the live DB directly
- Restore by uploading a DB file
- Selective table restore (e.g. restore `blocks` from an older DB without touching `config_periods`)

**Deprecations removed in this release**

| Artefact | Removed in |
|----------|-----------|
| `migrate_json_to_sqlite()` in `block_store.py` | 2.1.0 |
| `blocks.json` preservation after migration | 2.1.0 |
| `cumulative_totals.json` as live state | 2.1.0 |
| `current_block.json` as live state | 2.1.0 |
| `meters_config.json` as live state | 2.1.0 |
| `SQLITE_MIGRATION_PLAN.md` | 2.1.0 |

> The engine refactor for `current_block.json` is the riskiest part — recommend a design spike to ensure tick-loop latency is not affected before development begins.

---

### 2.2.0 — Data Management
**Theme: Give users control over their data**

With the DB as the single source of truth, data management operations are safe and atomic.

- Stop / Start engine controls (pause recording without restarting the add-on)
- Reset data wizard — guided flow: stop engine → backup → clear blocks → reconfigure → restart
- Selective date range deletion (e.g. remove a period of bad data)
- DB-to-DB migration tool (copy blocks between installs or from older DB files)
- Confirmation dialogs and safety checks throughout

---

### 2.3.0 — High-Resolution Charting
**Theme: See what's really happening within each block**

Capture sensor data at full resolution (e.g. every 10 seconds) for charting, while keeping reconciliation blocks for billing accuracy. The `reads` table (populated since 2.1.0) is the data source.

- High-res data already stored per sensor capture in the `reads` table
- Configurable retention (default 7 days — storage is significant at 10s resolution)
- Daily charts rendered from high-res data when available, falling back to block data for older periods
- No impact on billing calculations — reconciliation blocks remain authoritative

---

### 2.4.0 — Gas Meters
**Theme: Whole-home energy tracking**

Extend the engine to support gas meter recording alongside electricity.

- Gas uses m³ or ft³ — requires calorific value and correction factor conversion to kWh
- Billing periods and standing charges may differ from electricity
- Gas meters update less frequently than smart electricity meters
- Separate chart views and a combined electricity + gas billing summary

> Requires a design spike before development begins.

---

### 2.5.0 — Charting Insights
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