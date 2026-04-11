# Changelog

## [2.2.0] — 2026-04-10

### Added
- **Bill summary redesign** — the Import section now shows total grid draw at the top,
  matching what your supplier bills, with sub-meter breakdown (House remainder, EV charger,
  Battery) indented beneath. Previously only the remainder was shown as "Import", making
  supplier reconciliation require manual summing across sections.

- **Delete Blocks** — new sub-page under Data Management (`/delete-blocks`) to permanently
  delete blocks for a date range, optionally filtered to a single meter. Shows a block and
  day count preview before requiring explicit confirmation. Cannot be undone.

- **Historical Corrections** promoted to own page — moved from the Data Management page to
  its own sub-page (`/corrections`) following the same pattern as Billing History under
  Meter Config. Data Management topbar now has direct links to both sub-pages.

- **Compact Database** — "Compact Now" button on the Data Management page runs `VACUUM`
  on the blocks database. The engine is paused briefly during the operation to ensure
  exclusive access. Reports size before and after so you can see how much space was
  reclaimed. Most useful after bulk deletions; at ~40 KB/day growth it is rarely urgent.

### Fixed
- Date inputs in Delete Blocks and Historical Corrections now show `dd/mm/yyyy` format
  hint in labels and use `lang="en-GB"` to encourage day-first display in supporting
  browsers.

- **Billing and heatmap chart flicker removed** — `<meta http-equiv="refresh">` has been
  removed from generated chart HTML. The EMT charts page handles refresh cleanly via its
  own 2-minute `setInterval` without reloading the iframe. Lovelace users should switch
  to the dedicated `/lovelace/billing` and `/lovelace/heatmap` endpoints (see below).

- **Lovelace-friendly chart endpoints** — `/lovelace/billing` and `/lovelace/heatmap`
  serve the chart HTML with a 130-second meta refresh and aggressive no-cache headers
  baked in at serve time. Use these URLs in Lovelace webpage cards instead of the raw
  `/charts/*.html` URLs — they refresh reliably and never get stuck in the browser cache.
  Documented in the Help page.

---

## [2.1.9] — 2026-04-07

### Fixed
- **Billing chart not auto-refreshing** — the 2-minute auto-refresh timer used
  `textContent.indexOf('Daily')` to identify the active tab, which stopped working
  when the billing tab was renamed from "Daily Usage" to "Billing" in 1.6.0. With
  a third "Usage Stats" tab also present, the fallback logic refreshed the heatmap
  instead of the billing chart when Usage Stats was active, leaving the billing chart
  stale until a hard refresh. Fixed by adding `data-chart` attributes to tab buttons
  and clearing all `chartsLoaded` state on every timer tick so any tab switch always
  fetches fresh data.

- **`api/charts/daily` and `api/charts/heatmap` missing cache headers** — the JSON
  endpoints serving chart HTML had no `Cache-Control` headers, allowing some browsers
  and the HA ingress proxy to cache responses. Added `no-cache, no-store,
  must-revalidate` headers to both endpoints.

---

## [2.1.8] — 2026-04-07

### Fixed
- **Chart regeneration on every gap-fill block** — when the engine restarts after a
  long offline period, charts were regenerated once per missing block. A 6-hour gap
  with 5-minute blocks triggers 72 chart regeneration cycles, each taking ~7 seconds,
  causing minutes of CPU load before the engine catches up. Fixed by skipping chart
  regeneration for interpolated (gap-fill) blocks — charts are regenerated once at
  startup and again on the first live block.

- **Gap-fill blocks showing zero rate and cost** — after the `extract_last_reads`
  fix in this release, `last_known_rates` entries became `{"ts":..., "value":...}`
  dicts instead of raw floats. The gap-fill rate lookup expected a float, so rate
  and cost were silently zeroed for all interpolated blocks during catch-up.
  Fixed by adding a `_rate_value()` helper that unwraps either format.

- **Crash on startup with session gap: `AttributeError: 'float' object has no attribute 'get'`** —
  `extract_last_reads()` stored the last known rate as a raw float in `last_known_rates`.
  `save_current_block()` expects `{"ts": ..., "value": ...}` dicts and calls `r.get("ts")`
  on each entry — crashing when it encounters a float. This path is triggered when the
  engine restarts after a session gap and the last known rates come from a finalised block
  (which stores rate directly on the channel, not in a rates list). Fixed by always
  returning rates as `{"ts": ..., "value": ...}` dicts from `extract_last_reads()`.

- **Gap fill using first post-outage read instead of latest** — when the engine
  restarts after a gap, `post_reads` was populated with `reads[0]` (the first
  sensor capture after restart). If the sensor updated multiple times before gap
  fill triggered, the interpolation endpoint was stale — later sensor values were
  ignored. Fixed by using `reads[-1]` (the most recent read) as the post-gap
  anchor, giving the most accurate interpolation endpoint available.

- **Gap fill not running after session outage** — `extract_last_reads()` was
  called on the last finalised block (from DB via `_row_to_block`) which stores
  sensor values as `read_end` floats with no timestamps, not as `{"ts", "value"}`
  dicts. The gap fill anchor `pre_ts` was therefore always `None`, causing
  `detect_gap()` to return no missing windows and silently skip gap fill entirely.
  Fixed by using `read_end` and the block's `end` timestamp when extracting reads
  from finalised DB blocks, giving `detect_gap` a valid anchor.

- **False large kWh spike on sub-meters after add-on restart** — when the engine
  restarts after a session gap (e.g. upgrade, HA restart), the current in-progress
  block retains stale channel reads from before the restart. Sub-meters that use
  cumulative sensors produce a delta spanning the entire offline period, resulting
  in a false import spike on the first post-restart block.

  The main meter is unaffected — it uses boundary interpolation from a precise
  pair of reads around the block start/end. Sub-meters accumulate all reads
  directly, so the stale pre-restart read was included in the delta calculation.

  Fixed by clearing the current block's channel reads on startup when a session
  gap is detected. The gap marker's `pre_reads` correctly captures the pre-gap
  values for gap interpolation; live reads accumulate fresh from the first
  post-restart sensor capture.

---

## [2.1.7] — 2026-04-06

### Fixed
- **Standing charge corrections updating sub-meter rows** — the correction
  query filtered only by `local_date`, so it updated all meter rows including
  `ev_charger` and `house_battery` which should always have `standing_charge = 0`.
  This caused the preview to show `current_min = 0.0` (from sub-meter rows) instead
  of the real standing charge, and the apply wrote incorrect values to sub-meter rows.
  Fixed by restricting standing charge corrections to main meter rows only via a
  subquery on the `meters` table (`is_sub_meter = 0`).

- **Billing chart and Usage Stats colours out of sync** — both charts now use
  `build_meter_colors_from_config(cfg)` so sub-meters always get the same colour
  in both views. Previously the billing chart built its colour map from the first
  day of block data, which could assign different indices to sub-meters added after
  data collection began, causing colour mismatches between charts.

- **Startup crash on pre-2.1.6 databases: `no such column: m.v2x_capable`** —
  `get_last_block()` selects `m.v2x_capable` explicitly, but on older databases
  this column doesn't exist until `migrate_full_config_json()` runs — which is
  too late. Fixed by moving all incremental column additions into `_ensure_schema()`
  so they run at `open_block_store()` time, before any query.

- **`supplier` and `v2x_capable` meta fields silently dropped on config save** —
  `_write_meters` only persisted a subset of meter meta fields; both fields were
  lost on every config save.

  `supplier` is now a column on `config_periods` (not `meters`) giving it a full
  historical record — if you change supplier you create a new config period, just
  like changing billing day, and historical blocks retain a reference to the supplier
  that was active when they were recorded.

  `v2x_capable` is a column on `meters` (correct — it is a per-meter property, not
  billing-period-specific).

  Existing databases are upgraded automatically on startup. Users who had configured
  a V2G-capable meter will need to re-save their meter config once to restore the
  `v2x_capable` flag. The `supplier` field can be set via Edit on the Billing History
  page for any config period where it matters.

---

## [2.1.5] — 2026-04-06

### Fixed
- **Live Power Today / This Bill / This Year cards showing inflated values for
  sub-meter installations** — `get_billing_totals_for_local_date_range()` did
  `SUM(imp_kwh)` across all meters with no sub-meter filter, double-counting
  sub-meter consumption already included in `electricity_main.imp_kwh`. On a
  system with an EV charger and battery, Today showed ~79% more kWh and cost
  than actual grid import. Fixed by applying the same PASS 3 logic as
  `get_cumulative_totals`: main meter uses `imp_kwh_remainder`, sub-meters use
  `imp_kwh_grid`, cost/export/standing from main meter only. Standing charge
  query also restricted to main meter rows to prevent duplication.

---

## [2.1.4] — 2026-04-06

### Fixed
- **Sub-meters added after the first block date missing from Usage Stats** —
  `build_meter_colors` sampled only the first day of blocks to determine which
  meters to plot. Sub-meters that were added to the config after data collection
  began (e.g. a battery added weeks after the EV charger) had no blocks on the
  first day and were silently excluded from Usage Stats charts and data table.
  Fixed by replacing the block-sample approach with `build_meter_colors_from_config`
  which builds the colour map directly from the config dict, guaranteeing all
  configured meters are represented regardless of when they first recorded data.

---

## [2.1.3] — 2026-04-06

### Fixed
- **Sub-meter flags missing from reconstructed block dicts** — `_row_to_block` built
  `meter.meta` from `config_periods` columns only, never setting `sub_meter`,
  `parent_meter`, or `device`. Charts, Live Power and Usage Stats relied on
  `meta.sub_meter` to identify sub-meters; without it all meters appeared as main
  meters, sub-meters were not plotted separately, and billing calculations were wrong.
  Fixed by joining the `meters` table in `_select_blocks` and `get_last_block` and
  populating the full meta from the joined columns.

- **`get_cumulative_totals()` double-counting sub-meter consumption** — the four HA
  sensors (import kWh, export kWh, import cost, export cost) were incorrectly inflated
  for installations with sub-meters. `electricity_main.imp_kwh` already includes
  sub-meter consumption; the previous implementation added sub-meter `imp_kwh` a
  second time. Fix mirrors engine PASS 3 logic — main meter uses `imp_kwh_remainder`,
  sub-meters use `imp_kwh_grid`, cost and export from main meter only.
  Historical block data unaffected. Users without sub-meters unaffected.

### Also includes
- All 2.1.0 changes (fully relational DB, JSON file elimination, normalised schema)
- Data Management page (renamed from Import & Backup)
- Enhanced Historical Corrections (time-of-day window, per-meter targeting, per-block preview)

> 2.1.1 and 2.1.2 were briefly live during a difficult release cycle and have been
> superseded by this release. If you are on 2.1.1 or 2.1.2 please update immediately.

---

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