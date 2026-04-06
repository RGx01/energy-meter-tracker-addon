# Changelog

## [2.1.2] — 2026-04-06

### Fixed
- Re-release of 2.1.1 fixes under a new version number. 2.1.1 was briefly
  live before being rolled back, leaving some users on 2.1.1 with the broken
  build. 2.1.2 ensures all users receive the corrected version.
  See 2.1.1 release notes for the full list of fixes.

---

## [2.1.1] — 2026-04-06

### Fixed
- **Sub-meter flags missing from reconstructed block dicts** — `_row_to_block` built
  `meter.meta` from `config_periods` columns only, never setting `sub_meter`,
  `parent_meter`, or `device`. Charts and billing relied on `meta.sub_meter` to
  identify sub-meters; without it all meters appeared as main meters, sub-meters were
  not plotted separately, and billing calculations were wrong. Fixed by joining the
  `meters` table in `_select_blocks` and `get_last_block` and populating the full meta
  from the joined columns.

- **`get_cumulative_totals()` double-counting sub-meter consumption** — the four HA
  sensors (import kWh, export kWh, import cost, export cost) were incorrectly inflated
  for installations with sub-meters (EV charger, battery etc).

  `electricity_main.imp_kwh` already includes sub-meter consumption. The previous
  implementation did `SELECT SUM(imp_kwh) FROM blocks` across all meters, which added
  sub-meter `imp_kwh` a second time. On a system with an EV charger and battery this
  produced import sensor readings roughly 67% higher than actual grid import.

  The fix mirrors the engine's PASS 3 finalise logic:
  - Main meter: uses `imp_kwh_remainder` (house-only grid load after sub-meters),
    falling back to `imp_kwh` when no sub-meters are configured
  - Sub-meters: uses `imp_kwh_grid` (the portion drawn from the grid rather than
    from solar/battery), falling back to `imp_kwh`
  - Cost and export figures: main meter only

  **Historical block data is unaffected** — the blocks table, billing charts, and
  per-block calculations were correct throughout. Only the HA sensor values
  published after each block finalise were wrong.

  Users without sub-meters are unaffected.

---

## [2.1.0] — 2026-04-06

### Changed (breaking — upgrade path is fully automatic)
- **`energy_meter.db` is now the only file that matters** — it is the single source of truth for all state; backup and restore requires only this one file
- **`cumulative_totals.json` eliminated** — lifetime totals derived via `SELECT SUM(...)` on the blocks table; file silently ignored on startup
- **`current_block.json` eliminated** — in-progress block state now stored in the new `current_block` and `current_reads` tables; migrated automatically on first 2.1.0 startup and renamed `.migrated`
- **`meters_config.json` is a convenience export only** — written on every config save for human readability, never read back as live state
- **Config is fully normalised** — `full_config_json` blob removed from `config_periods`; meter definitions live in the `meters` and `meter_channels` tables; `gap_marker` blob removed from `current_block` and replaced with `gap_detected_at` column and `is_gap_seed` rows in `current_reads`; `mpan` and `tariff` promoted to proper columns on `meter_channels`

### Added
- `meters` table — fully populated: one row per meter per config period, with all sensor entity IDs, sub-meter flags, and optional fields
- `meter_channels` table — per-channel sensor config (`read_sensor`, `rate_sensor`, `standing_charge_sensor`, `mpan`, `tariff`)
- `current_block` table — single-row in-progress block state (`block_start`, `block_end`, `last_checkpoint`, `gap_detected_at`)
- `current_reads` table — rolling reads/rates buffer with `is_gap_seed` column (0=live, 1=gap seed kWh, 2=gap seed rate)
- `BlockStore.config_from_db(period_id)` — reconstructs full config dict by joining normalised tables; no JSON parsing
- `BlockStore._write_meters(config, period_id)` — upserts meter and channel rows from a config dict
- `BlockStore.save_current_block()` / `load_current_block()` / `clear_current_block()` — DB persistence for in-progress block state
- `BlockStore.get_cumulative_totals()` — single SQL aggregation replacing `cumulative_totals.json`
- `BlockStore.migrate_full_config_json()` — automatic 2.0→2.1 upgrade: populates normalised tables from `full_config_json` blobs, migrates `gap_marker` blob, adds missing columns; safe to call on every startup; idempotent
- **Historical Corrections enhanced** — rate corrections now support:
  - Time-of-day window (`from_time` / `to_time` in local time, DST-aware — e.g. "from 15:00" for a mid-day tariff change)
  - Per-meter targeting (`meter_id` selector populated from blocks table — correct `ev_charger` independently of main meter)
  - Per-block preview table showing block time, current rate, new rate, kWh, current cost, new cost, and cost delta before committing
  - `/api/corrections/meters` endpoint returning distinct meter IDs for the UI selector
- Import & Backup page — file reference table updated; restore modal reflects single-file model; deprecated file entries removed

### Removed
- `full_config_json TEXT` column from `config_periods`
- `gap_marker TEXT` blob column from `current_block`
- `meter_channel_meta` key/value table (replaced by proper columns on `meter_channels`)
- `cumulative_totals.json`, `current_block.json`, `meters_config.json` as authoritative state files

### Migration
On first startup after upgrading from 1.x:
- `blocks.json` is migrated to `energy_meter.db` and renamed `.migrated` (existing 2.0.x behaviour)
- `current_block.json` is migrated to the DB and renamed `.migrated`
- `cumulative_totals.json` is ignored

On first startup after upgrading from 2.0.x:
- `energy_meter.db` is opened; new tables (`current_block`, `current_reads`, `meter_channels`) are created automatically
- `migrate_full_config_json()` populates the normalised meter tables from existing `full_config_json` blobs and drops the column
- `gap_marker` blob is migrated to `gap_detected_at` + `is_gap_seed` rows
- `current_block.json` is migrated to the DB and renamed `.migrated`

---

## [2.0.1] — 2026-04-05

### Added
- **Historical Corrections** — new section on the Import & Backup page; bulk-update standing charge or import/export rates across a local date range in the live database (`/data/energy_meter_tracker/energy_meter.db`); Preview shows affected block and day counts plus current value range before committing; rate corrections optionally recalculate cost from corrected rate × kWh

### Fixed
- **kWh and cost alignment between Billing chart and Usage Stats** — `calculate_billing_summary_for_period` was comparing UTC block_start strings against local naive period boundaries; BST blocks at `23:xx UTC` (local midnight) were excluded from billing periods and daily summaries they belonged to; block_start is now converted to local time before filtering, fixing both kWh totals and standing charge grouping in the Billing chart
- **Standing charge double-counted in Usage Stats (BST days)** — standing charge was read from `s["total_standing"]` which used UTC date grouping, counting the `23:xx UTC` block as a separate day; now read directly from `block["meters"][meter_id]["standing_charge"]` (same value on all blocks for a local day)
- **Standing charge not shown in Usage Stats** — `standing_charge` is at `block["meters"][meter_id]["standing_charge"]`, not the top-level block dict; previous code did `blocks[0].get("standing_charge")` which always returned `None`
- **Usage Stats blocks fetched by local_date** — all block queries in `api_blocks_summary` now use `get_blocks_for_local_date_range` (local_date column) rather than UTC block_start boundaries, ensuring BST blocks are never missed
- **Billing period computation in Usage Stats** — replaced `get_all_blocks()` + `get_billing_periods_from_config_history` with the fast `get_config_periods()` + `get_billing_periods_from_config_periods`; eliminates a full block scan on every Usage Stats page load

---

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
