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
- Timezone-aware chart rendering
- UTC timestamp bug fixes
- Silent sensor timeout fix (block stuck at boundary)
- Standing charge billing display fix

### 1.4.0 — Global Readiness
- Configurable meter reconciliation period (5, 15 or 30 minutes)
- Automatic currency detection from rate sensor unit of measurement
- International sensor compatibility (currency-agnostic sensor filters)
- Export-only daily chart fix (rate axis alignment)
- Billing day always read from live config (takes effect immediately)
- Help page internationalised; reconciliation period terminology throughout

---

## Planned

### 1.4.x — Data Management
**Theme: Give users control over their data**

The reconciliation period is locked once data collection begins, but currently the only way to reset data is via terminal. This release adds proper data management tools to the UI.

- Stop / Start engine controls (pause recording without restarting the add-on)
- Reset data wizard — guided flow: stop engine → backup → clear data → reconfigure → restart
- Confirmation dialogs and safety checks throughout
- Help page documentation for the reset procedure

> This is a prerequisite for users who need to change their reconciliation period or start fresh after a misconfiguration.

---

### 1.5.0 — Live Power Gauge
**Theme: Real-time visibility**

A live power gauge in the add-on sidebar showing current import and export power from your smart meter sensors.

- Sidebar gauge component showing live import/export kW
- Auto-refreshing without page reload (WebSocket or SSE)
- Visual indicator of net flow direction (importing vs exporting)
- Configurable sensor mapping (reads from existing meter config)

---

### 1.6.0 — Gas Meters
**Theme: Whole-home energy tracking**

Extend the engine to support gas meter recording alongside electricity.

Key design considerations:
- Gas uses m³ or ft³ — requires calorific value and correction factor conversion to kWh
- Billing periods and standing charges may differ from electricity
- Gas meters update less frequently than smart electricity meters
- Separate chart views for gas consumption
- Potential for combined electricity + gas billing summary

> Requires a design spike before development begins. Scope may split across sub-releases.

---

### 1.7.0 — Charting Insights
**Theme: Understand your energy patterns**

New analytical views beyond raw consumption tracking.

Candidate features (final scope TBD):
- Peak demand analysis — identify highest consumption periods
- Solar self-consumption ratio (requires solar generation sub-meter)
- Cost forecasting — projected bill based on current period consumption rate
- Day-of-week and time-of-day consumption heatmaps
- Tariff optimisation hints (e.g. best EV charging windows on Agile)

---

### 1.8.0 — High-Resolution Charting
**Theme: See what's really happening within each block**

Capture sensor data at full sensor resolution (e.g. every 10 seconds) for charting purposes, while keeping the reconciliation block size for billing accuracy.

Key design considerations:
- High-res data stored in a separate buffer from reconciliation blocks
- Configurable retention period (default 7 days suggested — storage is significant at 10s resolution)
- Daily charts rendered from high-res data when available, falling back to block data for older periods
- No impact on billing calculations — reconciliation blocks remain authoritative
- Migration path for users upgrading from lower-resolution historical data

---

## Longer Term / Unscheduled

- **Solar generation tracking** — export sub-metering and self-consumption breakdown
- **V2G / V2X export** — breakdown of EV-to-grid export by device
- **Multiple batteries / inverters** — better support for complex hybrid systems
- **Historical data migration tool** — convert 30-minute blocks to finer resolution when supplier resolution changes
- **HACS / community distribution** — evaluate distribution channels beyond the add-on store
- **Multi-dwelling / multi-site** — support for properties with more than one grid connection

---

## Release Principles

- Each release should have a clear theme and a testable scope
- Billing accuracy is never compromised by new features
- Breaking changes (data format, config schema) require a migration path
- The reconciliation block is the authoritative unit — higher-resolution features are additive, not replacements
- User data is never deleted without explicit confirmation