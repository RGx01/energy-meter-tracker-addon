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
- **SQLite storage** — all blocks stored in an indexed database; automatic migration from `blocks.json`
- **Billing History** — config periods tracked in the database; billing charts use historically correct billing day and rates
- **Billing period transitions** — truncation-only model; correct period boundaries in usage stats and live power
- **Live Power performance** — instant page load with async billing cards; SQL aggregation replaces full block scans
- **Billing History UI** — New Period button, remove period for all periods (when 2+ exist), block reassignment on edit/delete

---

## Planned

### 2.1.0 — Data Management
**Theme: Give users control over their data**

Now that blocks are in SQLite, data management operations are safe and atomic.

- Stop / Start engine controls (pause recording without restarting the add-on)
- Reset data wizard — guided flow: stop engine → backup → clear blocks → reconfigure → restart
- Selective date range deletion (e.g. remove a period of bad data)
- Confirmation dialogs and safety checks throughout
- Help page documentation for the reset procedure

> Prerequisite for users who need to change their reconciliation period or start fresh after a misconfiguration.

---

### 2.2.0 — Charting Insights
**Theme: Understand your energy patterns**

New analytical views beyond raw consumption tracking.

Candidate features (final scope TBD):
- Cost forecasting — projected bill based on current period consumption rate
- Peak demand analysis — identify highest consumption periods and times of day
- Solar self-consumption ratio (requires solar generation sub-meter)
- Tariff optimisation hints (e.g. best EV charging windows for Agile tariff users)
- Day-of-week consumption patterns

---

### 2.3.0 — High-Resolution Charting
**Theme: See what's really happening within each block**

Capture sensor data at full resolution (e.g. every 10 seconds) for charting, while keeping reconciliation blocks for billing accuracy.

Key design considerations:
- High-res data stored in a separate SQLite table from reconciliation blocks
- Configurable retention (default 7 days — storage is significant at 10s resolution)
- Daily charts rendered from high-res data when available, falling back to block data for older periods
- No impact on billing calculations — reconciliation blocks remain authoritative

---

### 2.x — Gas Meters
**Theme: Whole-home energy tracking**

Extend the engine to support gas meter recording alongside electricity.

Key design considerations:
- Gas uses m³ or ft³ — requires calorific value and correction factor conversion to kWh
- Billing periods and standing charges may differ from electricity
- Gas meters update less frequently than smart electricity meters
- Separate chart views and a combined electricity + gas billing summary

> Requires a design spike before development begins.

---

## Longer Term / Unscheduled

- **Solar generation tracking** — export sub-metering and self-consumption breakdown
- **V2G / V2X export** — breakdown of EV-to-grid export by device
- **Multiple batteries / inverters** — better support for complex hybrid systems
- **Multi-dwelling / multi-site** — support for properties with more than one grid connection
- **HACS / community distribution** — evaluate distribution channels beyond the add-on store

---

## Planned Deprecations

### Migration tools — deprecation target: 2.2.0

The following tools were introduced to support the 1.x → 2.0.0 SQLite migration and will be removed once the migration window has passed:

| Tool | Purpose | Planned removal |
|------|---------|-----------------|
| `migrate_json_to_sqlite()` in `block_store.py` | Migrates `blocks.json` to `energy_meter.db` on first start | 2.2.0 |
| `SQLITE_MIGRATION_PLAN.md` | Internal migration design document | 2.2.0 |
| `blocks.json` preservation after migration | Original JSON file is kept as a fallback during the migration window | 2.2.0 |

Users upgrading from 1.x should ensure they have confirmed their data migrated correctly before 2.2.0 is released. After removal, `blocks.json` will no longer be read or preserved — only `energy_meter.db` will be used.

> If you are still on 1.x and plan to upgrade, do so before 2.2.0. The migration runs automatically on first start and requires no manual steps.

---

## Release Principles

- Each release has a clear theme and a testable scope
- Billing accuracy is never compromised by new features
- Breaking changes (data format, config schema) require a migration path and deprecation notice
- The reconciliation block is the authoritative unit — higher-resolution features are additive, not replacements
- User data is never deleted without explicit confirmation
- Migration tools are maintained for at least two minor releases after the migration they support