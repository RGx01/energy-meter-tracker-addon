# Changelog

## [2.0.0] — 2026-04-05

### Added
- **SQLite database** — all blocks now stored in a SQLite database (`energy_meter.db`) replacing the `blocks.json` flat file; queries are indexed and fast regardless of history length; backward-compatible migration from `blocks.json` runs automatically on first start
- **Config history (Billing History)** — every billing-significant config change is recorded as a new config period in the `config_periods` table; historical billing charts use the billing day and rates that were active when each block was recorded, not today's values
- **Billing History page** — accessible via the 🕓 Billing History button on the Meter Config page; shows all config periods with edit and remove controls; **New Period** button creates a new period inheriting the current config (use when changing address, supplier or billing day)
- **Billing period transition logic** — when the billing day changes, the old config's last billing period is truncated at the transition date; subsequent periods use the new billing day; bills can only be truncated, never extended
- **Usage Stats — billing period navigator** — the navigator label now shows the correct inclusive end date for each billing period, including truncated transition periods
- **Live Power — billing-accurate "This Bill"** — the This Bill card on the Live Power page now uses config history to find the correct billing period start rather than assuming the current billing day has always been in effect
- **Fast SQL billing aggregation** — `api/billing` now uses `SUM()` aggregation queries instead of loading all blocks into Python; standing charge is summed once per local calendar day using the pre-computed `local_date` column; year-to-date totals load in milliseconds regardless of history length
- **Gauge scale cache** — the 7-day percentile used for gauge scaling is cached for 30 minutes, eliminating a large block scan on every Live Power page load and every SSE tick

### Changed
- **Live Power page loads instantly** — billing card data (Today, This Bill, This Year) is now fetched asynchronously after the page renders; the page itself requires no block queries on initial load, showing "Loading…" briefly then populating via `api/billing`
- **Billing History removed from sidebar nav** — accessible via button on Meter Config page; Meter Config nav item highlights for both pages
- **Config period removal** — any period can be removed when 2+ periods exist; blocks are always reassigned to the previous (older) period; the chain is rebuilt and blocks are re-assigned by date range after any insert, edit or delete
- **`_rebuild_config_period_chain`** — now also reassigns blocks by date range after rebuilding the effective_from/effective_to chain, ensuring blocks always belong to the correct config period

### Fixed
- **Usage Stats billing period start** — the `api/blocks_summary` endpoint now fetches all blocks (not just the display range) when computing billing period boundaries, so `first_block_date` is always the true start of history rather than the start of the current view
- **Effective To field in dark mode** — the read-only Effective To input in the Edit Config Period modal now uses `readonly` instead of `disabled`, so browser dark-mode styling doesn't make the text unreadable; a dashed border indicates it is not editable
- **Active period Effective To hidden** — when editing the active (current) config period, the Effective To field is hidden entirely since it is always null
- **Effective From date validation** — the modal now enforces `effective_from < effective_to` both via a `max` attribute on the date input and a JS check in `saveEdit()`

---

## [1.6.3] — 2026-04-01

### Fixed
- **Heatmap mobile portrait — chart fills full viewport**
- **Heatmap mobile pinch zoom**
- **Heatmap scroll-guard strip overlapping totals bar**
- **Usage Stats width unconstrained on mobile portrait**

---

## [1.6.2] — 2026-04-01

### Added
- **Usage Stats — Billing/Calendar period toggle**
- **Usage Stats data table — totals column**
- **Usage Stats data table — period labels**
- **Global light/dark theme toggle**

### Fixed
- **Usage Stats export cost positive in data table**
- **Usage Stats meter labels include site name**
- **Light/dark theme toggle not working on Billing and Usage Stats**
- **Heatmap totals bar white bars in dark mode**
- **Heatmap weekend shading in dark mode**
- **Heatmap toggle button rendering as artefact over chart**
- **Heatmap mobile portrait gap**
- **Billing chart daily sections always collapsed after re-expanding**
- **Heatmap mobile pinch zoom re-enabled**
- **Usage Stats width unconstrained on mobile portrait**

---

## [1.6.0] — 2026-04-01

### Added
- **Usage Stats chart** — daily, monthly and yearly import/export with sub-meter breakdown
- **Data table** — tabular view with copy-to-clipboard export
- **Light/dark theme toggle** — all chart types

### Changed
- **Summary page renamed to Live Power**
- **Remember last visited page**
- **Chart tabs renamed** — Daily Usage → Billing, Import / Export → Usage Stats

### Fixed
- `config_page` route missing
- `/api/charts/heatmap` route missing
- Heatmap Safari blob URL error
- Copy to clipboard over HTTP

---

## [1.5.1] — 2026-03-26

### Added
- **Live Power page** — gauge, billing cards, carbon intensity forecast
- **Power sensor and postcode prefix fields** in Meter Config

---

## [1.4.0] — 2026-02-10

### Added
- Configurable meter reconciliation period (5, 15 or 30 minutes)
- Automatic currency detection

### Fixed
- Export-only daily chart rate axis alignment
- Billing day always read from live config

---

## [1.3.x] — 2025-12-01

### Fixed
- Timezone-aware chart rendering; UTC timestamp bugs; silent sensor timeout; standing charge billing display

---

## [1.2.0] — 2025-10-15

### Added
- Guided Setup Wizard

---

## [1.1.0] — 2025-09-01

### Added
- Flask-based web UI

---

## [1.0.0] — 2025-08-01

Initial release. Core metering engine, sub-meter support, gap filling, billing charts, HA sensor publishing.