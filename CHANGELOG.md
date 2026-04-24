# Changelog

## [2.6.3] — 2026-04-23

### Added

- **Usage Stats data table — scrollable on desktop** — `#chart-bar` converted to a viewport-filling flex column. Chart is `flex-shrink:0` and stays pinned above the table. Table wrapper is `flex:1; min-height:0` and scrolls independently. On mobile natural page scroll is preserved.

- **Usage Stats data table — sticky column headers and totals row** — column headers stick at the top while scrolling. Totals row moved from the bottom of `<tbody>` to a second `<thead>` row, pinned just below the column headers so it is always visible.

- **Usage Stats data table — sortable date column** — clicking the Period column header toggles ascending/descending sort with a ↑/↓ indicator. Defaults to newest first. Preference persisted in localStorage.

- **Usage Stats data table — alternate row shading** — odd/even rows use `var(--surface)` / `var(--bg)` for zebra striping that works in both light and dark mode.

- **Usage Stats data table — colour dots in column headers** — each column that maps to a specific meter or direction (import, export, standing charge) shows a coloured dot matching the chart legend. Columns with no single colour (Period, Total, Net, Avg intensity) show no dot.

- **Billing chart — day chart order toggle** — "↓ Newest first / ↑ Oldest first" button added to the billing chart toolbar. Reverses the order of day charts within the displayed billing period. Useful for comparing against a PDF utility bill. Preference persisted in localStorage and restored on page load. The period dropdown navigator is unaffected.

- **Heatmap — fills available desktop viewport** — scroll height was hardcoded to 31 rows. Now calculates maximum height from `window.innerHeight` so the heatmap uses all available vertical space on desktop.

- **Heatmap — larger metric toggle buttons on mobile** — kWh/CO₂/gCO₂/kWh toggle buttons were very small touch targets on mobile. Added `@media (max-width: 600px)` override: `min-height: 40px`, larger padding, `min-width: 64px`.

### Fixed

- **Carbon intensity fetched after catch-up block finalisation** — on the first engine tick after a restart or rebuild, `ensure_correct_block` finalised all catch-up blocks before `_tick_carbon_intensity` had run. Those blocks were assigned carbon values from pre-restart CI data which could be stale. Fixed by moving the CI fetch to run before the block lifecycle in every tick.


- **Deprecated architecture values in config.yaml** — `armhf` and `armv7` removed from `arch` list. Supported architectures are now `aarch64` and `amd64` only.

---

## [2.6.2] — 2026-04-22

### Added

- **GitHub Wiki link in Help page** — new "Further Reading & Support" section at the bottom of the Help page linking to the GitHub wiki (sensor requirements, integration guides), issues, and repository.

### Changed

- **Theme toggle consolidated to logo** — the `☾` toggle buttons in the sidebar footer and charts toolbar have been removed. The logo in the top-left of the sidebar is now the sole theme toggle, consistent across all pages. The heatmap iframe theme sync is preserved so theme changes propagate correctly into generated charts.

- **Logo size increased** — logo width increased from 160px to 180px with sidebar brand padding adjusted to fit cleanly within the sidebar without changing its width.

- **Insights mobile topbar** — subtitle hidden on mobile, Carbon tab collapses to emoji-only, period navigation compacts to fit a single row. Consistent with the charts page mobile pattern.

- **Insights metric labels simplified** — "Grid import (CI blocks)" and "Grid export (CI blocks)" shortened to "Grid import" and "Grid export". The footnote already explains the CI-only basis.

---

## [2.6.1] — 2026-04-21

### Added

- **Logo theme toggle** — clicking the Energy Meter Tracker logo in the top-left sidebar now toggles between light and dark mode, identical to the existing button in the bottom right. The logo adapts visually in light mode via CSS filter inversion.

### Fixed

- **Sub-meter display rate used weighted average instead of last rate** — at tariff boundaries (e.g. Octopus off-peak → peak transition) a sub-meter block that recorded a tiny amount of kWh spanning both sides of the boundary would compute a weighted average rate (e.g. `£0.189`) rather than using the last rate in the block (e.g. `£0.323`). This caused the sub-meter rate line on charts to show an anomalous mid-point value while the main meter showed the correct end-of-block rate. All meters now consistently use the last captured rate in the block, which is the intent — capturing the rate as close to block end as possible to account for any API lag from remote rate providers.

- **Net bar colour wrong when standing charge tips a day positive** — on the Net billing chart with "Inc. Standing Charge" enabled, days where the standing charge pushed a net-credit day into net-cost showed the bar above zero in the credit colour (grey) rather than the cost colour (blue). The bar height calculation correctly included the standing charge but the colour check did not. Fixed by including `barStandingVal(agg)` in the colour check to match the bar height calculation exactly.

- **Session gap detection silently failing on non-30-minute block setups** — `detect_gap` was always called with the default `block_minutes=30` during startup gap detection, regardless of the actual configured block size. On 5-minute or 15-minute block setups, window boundaries were computed in 30-minute increments, causing the gap to evaluate to zero or far fewer missing windows than actually existed — `no session gap detected` was logged when up to 90 minutes of data was missing. Fixed by reading `block_minutes` from the last block's own meta before calling `detect_gap`. This particularly affects users outside the UK where 5-minute smart meter data is common.

---

## [2.6.0] — 2026-04-20

### Added

- **Insights page** — new sidebar entry 🌿 between Charts and Live Power. Billing period carbon analysis with narrative cards. Period selector (prev/next) in topbar. Click any trend chart bar to navigate directly to that period.

- **Carbon Summary card** — net kgCO₂ for the billing period, effective intensity vs grid average, import/export split, car/tree/flight equivalences, inline SVG trend chart across all periods, coverage warning when CI data is below 95%.

- **House Consumption card** — grid import attributable directly to the house (total minus EV and battery sub-meters). Shows kWh and carbon from CI-covered blocks only. Notes that battery discharge into the house cannot be separated without generation-side metering.

- **Solar Export Offset card** — export displaced X kgCO₂ from the grid. Equivalences. Shown only when export > 0.1 kWh.

- **EV Charging card** — AC from grid, usable DC stored (after charge efficiency loss), average charge intensity vs grid average, estimated mileage, gCO₂/mile vs petrol comparison. Carbon and kWh figures use CI-covered blocks only to avoid mixing partial carbon with full-period kWh. Coverage warning when below 80%.

- **Battery Charging Behaviour card** — average charge intensity vs grid average, estimated carbon saving if charged during cleaner periods. Honest note that export cannot be split between solar and battery without generation-side metering.

- **Heat Pump card** — electricity used, estimated heat delivered (× SCOP), carbon vs equivalent gas boiler, % cleaner, crossover grid intensity. Uses CI-covered kWh only for the gas comparison.

- **inverter_possible caveat** — EV, battery and heat pump cards show a warning if the sub-meter is marked inverter-capable, noting that carbon shown is a maximum and may be lower if solar or battery contributed.

- **Settings page** — new sidebar entry ⚙️ (replaces Meter Config entry). Two tabs: Meter Config (existing config content) and Carbon (assumptions). Carbon tab has postcode prompt if not configured, linked to the same config save API as Meter Config.

- **Carbon assumptions** — petrol/diesel car gCO₂/mile, tree kgCO₂/year, flight LHR→NYC kgCO₂, distance unit (miles/km), export displacement methodology (grid average or custom), EV efficiency (miles/kWh battery), EV charge efficiency (AC→DC), battery round-trip efficiency, heat pump SCOP, gas boiler efficiency, gas gCO₂/kWh. All with citations. Stored in `store_meta` table — no schema change.

- **`GET/POST /api/settings`** — reads and writes carbon assumptions. Missing keys fall back to SETTINGS_DEFAULTS at read time.

- **`GET /api/insights/periods`** — lists all billing periods with quick carbon summary (SQL aggregate, no block loading).

- **`GET /api/insights/billing-period`** — full carbon breakdown for one billing period. Returns CI-only kWh (`ci_imp_kwh`, `ci_exp_kwh`) separately from total kWh for all meters. Sub-meters include `avg_charge_intensity`, `ci_imp_kwh`, and `inverter_possible` flag.

- **Meter type detection** — resolves sub-meter type from `meta.meter_type` first, then falls back to meter ID keywords (ev/charger → ev_charger, battery/batt → battery, heat/pump → heat_pump, solar/pv/inv → inverter).

- **`subbar` block** — new Jinja2 block in `base.html` between topbar and content. Used by Settings/Meter Config for secondary actions (Wizard, Refresh, Billing History).

### Changed

- **Navigation restructured** — sidebar now: ⚙️ Settings | 📊 Charts | 🌿 Insights | ⚡ Live Power | 🗄️ Data Management | 📋 Logs | 📖 Help. Separate Settings entry removed.

- **Routes renamed** — `/config` → `/settings`, `/import` → `/data-management`, `/summary` → `/live-power`, `/config-history` → `/billing-history`. Old cookie values mapped forward for backwards compatibility.

- **Templates renamed** — `config.html` → `meter_config.html`, `import.html` → `data_management.html`, `summary.html` → `live_power.html`, `config_history.html` → `billing_history.html`.

- **Settings page** renders `meter_config.html` by default (`?tab=carbon` for the Carbon tab). Meter Config sub-bar (Wizard, Refresh, Billing History) moved below the topbar using the new subbar block.

- **Sub-meter rate sensor wording** — label changed from "pre-filled from main meter" to "default: one-time copy from main meter at setup — update here if your sub-meter has a different rate or you retrospectively change rates". Clarifies this is not a live link.

### Fixed

- **Insights carbon/kWh consistency** — all carbon figures and their accompanying kWh use CI-covered blocks only (`ci_imp_kwh`). Previously total-period kWh was mixed with partial-period carbon, producing misleadingly low intensity figures and incorrect mileage estimates.

- **Upgrade from 1.x.x / 2.0.x (Docker) showed wizard and lost sensor config** — when upgrading from any version that stored data in `blocks.json` and config in `meters_config.json` (all 1.x.x and 2.0.x releases), the auto-migration to SQLite correctly migrated all block data but created an empty `config_periods` row because `load_config()` returns `{}` when `config_periods` is empty — a chicken-and-egg problem. The wizard then appeared as if it were a fresh install, and users with no prior DB thought their data was gone. Fixed in two places: (1) `migrate_json_to_sqlite` now explicitly reads `meters_config.json` directly before creating the config period, bypassing `load_config()`; (2) a post-startup repair step detects an empty `config_periods` row and populates it from `meters_config.json` if present — so existing installs that already migrated blocks but got an empty config period will self-heal on next restart without any user action.

- **Config not persisting after restore** — `engine_startup` previously called `load_config()` before opening the block store. With `_store=None` and `blocks.db` already present, `load_config()` returned `{}`, causing the startup config sync to reset scalar fields (billing day, timezone, etc.) to defaults and wipe sensor subscriptions. Fixed by opening the store before calling `load_config()` and removing the dangerous startup config sync entirely — the DB is authoritative.

- **`load_config()` — DB always authoritative** — both `server.py` and `engine.py` no longer fall back to `meters_config.json` when `blocks.db` exists. Previously a stale JSON file could override the DB config when swapping databases between environments.

- **Import rate zero across all gap fill blocks** — when only the export sensor fires before the first tick after a restore (the common case), `post_reads` has no import channel entry. `build_gap_blocks` hit the "missing reads" branch and hardcoded `rate=0.0`. Fixed: the missing-reads branch now uses `last_known_rates` as a fallback so the rate is preserved even when no post-read is available.

- **Unfinalised current_block window excluded from gap fill** — the gap anchor was derived from `pre_ts` (the last read timestamp), which put `detect_gap`'s `last_block_end` one block *after* the unfinalised current_block window, silently skipping it. Fixed by storing `last_block_start` in the gap marker (persisted to a new `gap_last_block_start` column on `current_block`) and using it as the anchor so the unfinalised window is always included in `missing_windows`.

- **Zero-rate/zero-kWh blocks from catch-up rollovers not overwritten** — after restore, `ensure_correct_block` fires during `engine_startup` and writes zero-read catch-up blocks via `INSERT OR IGNORE`. Gap fill then silently skipped them. Fixed: gap fill now uses `INSERT OR REPLACE` (`append_block_replace`) so catch-up zero blocks are overwritten with interpolated data.

- **Zero-rate blocks from catch-up rollovers contaminating last_known_rates** — `engine_startup` gap detection called `get_last_block()` which uses `MAX(block_start)`. After restore, catch-up rollovers fire during startup and write zero-rate blocks; by the time gap detection ran, `get_last_block()` returned one of those zero-rate blocks, infecting `last_known_rates` with `rate=0.0`. Fixed: gap detection now uses `get_last_block_before(current_block.start)` to always find the real last block from the restored DB.

- **`meters_config.json` appearing in restore UI and backup zips** — removed from all known sets in zip extraction endpoints, from backup creation, from restore sync logic, and marked "No longer needed" in the data management UI.

- **Stale `url_for('import_page')` references** — fixed in `charts.html`, `corrections.html`, `delete_blocks.html`, and `help.html`. Route was renamed to `data_management_page` in 2.6.0 but template references were not updated, causing 500 errors on those pages.

### Templates removed (old names no longer served)

- `config.html` — replaced by `meter_config.html`
- `summary.html` — replaced by `live_power.html`
- `import.html` — replaced by `data_management.html`
- `config_history.html` — replaced by `billing_history.html`

---

## [2.5.4] — 2026-04-19

### Added
- **Storage monitoring** — new Storage card on the Data Management page showing
  database size, disk free, growth rate (MB/day) and estimated runway. Runway is
  colour-coded green (>2 years), amber (6 months–2 years), red (<6 months). A disk
  usage bar shows used vs total with percentage free. Estimate is based on actual
  days of data recorded and assumes current growth rate continues.

### Fixed
- **Usage Stats floating toolbar** — `position: sticky` was removed in 2.5.2 when
  fixing billing chart landscape space (the two were unrelated). Restored with the
  correct `top: 0` offset now that `.content` has `padding: 0` on the Charts page.
  Works on both desktop and mobile.

---

## [2.5.3] — 2026-04-18

### Fixed
- **Usage Stats Y axis duplicate labels** — cost mode was using `.toFixed(0)` for axis
  labels so Chart.js ticks at 0.5 intervals (e.g. -1.0 and -1.5) both rendered as
  `-£1`, producing duplicate labels. Fixed to use 1 decimal place for cost labels.
  `maxTicksLimit: 8` and `grace: 2%` added to prevent over-generation of ticks and
  reduce excess headroom above/below the data.

- **Stale test timestamp** — `test_prune_removes_old_rows` in `test_block_store.py`
  used a hardcoded `2026-04-14` date which has now passed the 4-day pruning window,
  causing the test to fail. Fixed to use a relative timestamp (1 hour ago).

---

## [2.5.2] — 2026-04-17

### Fixed
- **Power history zoom rescales axes** — when zoomed, both the Y axis and X axis now
  rescale to the visible window. Y scale is computed only from data points within the
  zoom window. X axis uses a smart tick interval (1 min to 12 hr) chosen based on the
  zoom span, always anchors labels at the start and end of the window, and skips
  overlapping interior labels.

---

## [2.5.1] — 2026-04-17

### Added
- **Power history touch zoom** — the drag-to-zoom on the 48-hour power history chart
  now works on touchscreen devices. Drag horizontally to zoom, double-tap to reset.
  Uses `{ passive: false }` to prevent page scroll during chart interaction.

- **Mobile topbar collapse** — a drawer handle strip sits between the topbar and chart
  content on mobile and landscape. Shows the active tab name with chevrons. Tap to
  collapse the topbar entirely for maximum chart space, tap again to restore. State
  persists across sessions via localStorage.

### Fixed
- **Usage Stats toolbar over hamburger menu** — the floating toolbar `z-index` reduced
  to `9`, below the HA ingress chrome.

- **Orientation change leaves chart inconsistent** — rotating between portrait and
  landscape now forces a full reload of the active iframe chart after layout settles
  (600ms), eliminating the stale dimensions that required switching pages to recover.

- **Billing chart landscape real estate** — on mobile landscape the `period-nav` toolbar
  is now `position: static` so it scrolls off, giving the chart full viewport height.
  Portrait behaviour is unchanged.

- **Billing/heatmap chart fills available height** — charts now measure `.content`
  element height directly rather than using a hardcoded `window.innerHeight - 220px`
  offset, correctly filling the content area now that the topbar is sticky.

---

## [2.5.0] — 2026-04-16

### Added
- **Heatmap metric toggle** — the Net Energy Heatmap now has a floating toolbar with
  three metric modes: kWh (existing), gCO₂ (carbon flow per slot — red when emitting,
  green when offsetting), and gCO₂/kWh (grid carbon intensity — green→yellow→red).
  The toggle only appears when carbon data exists. Selected mode persists across
  sessions. The intensity colour scale is anchored to the 95th percentile of your
  recorded intensities so contrast is always meaningful as the grid decarbonises.

- **Usage Stats effective intensity column** — when in CO₂ mode, the data table gains
  an "Avg intensity" column showing the weighted average grid carbon intensity
  (gCO₂/kWh) for each period. Calculated as SUM(|carbon_g|) / SUM(|net_kwh|) across
  blocks with carbon data, matching the heatmap's per-slot intensity exactly when
  aggregated. The totals row shows the weighted average across the full period.

- **`avg_intensity` in `api/charts/blocks-summary`** — each day row now includes a
  weighted average grid intensity (gCO₂/kWh), enabling the consistency cross-check
  between the heatmap and Usage Stats.

- **Usage Stats auto-refresh** — the Usage Stats chart now re-fetches data if it is
  older than 5 minutes, rather than relying on a one-shot `barInited` flag. Switching
  tabs or navigating away and back within the TTL window still uses the cached data
  (no unnecessary fetches), but data will refresh at the same cadence as block finalise.

- **Power history drag zoom** — drag horizontally on the 48-hour power history chart
  to zoom into a specific time window. Double-click to reset to the full 48-hour view.
  Works alongside the existing hover tooltip.

- **Tab buttons in topbar** — the Billing / Heatmaps / Usage Stats tab buttons have
  moved from the content area into the topbar, freeing vertical space and following
  the same pattern as other pages (Meter Config, Data Management). The tab is now
  always visible without scrolling.

- **Floating toolbars** — Usage Stats and Heatmaps now have a sticky floating toolbar
  matching the billing chart's period-nav style (surface background, drop shadow,
  border-radius on bottom). The Usage Stats toolbar sticks to the top of the content
  area as you scroll through the data table. The Heatmap toolbar sticks within the
  heatmap scroll container. Both use CSS classes rather than inline styles for active
  state, matching the billing chart pattern exactly.

- **Fixed page scroll** — all pages (Meter Config, Charts, Live Power, Data Management,
  Help, Logs) now have a fixed topbar with only the content area below it scrolling.
  Previously the entire page including the topbar scrolled. The topbar is now always
  visible. This required `body: overflow: hidden`, `.main: height: 100vh`, and
  `.content: overflow-y: auto` in the base template.

### Fixed
- **Power history tests using stale timestamps** — `TestPowerHistory` and
  `TestApiPowerHistory` used hardcoded April 14 timestamps which fell outside the
  48-hour retention window as time passed, causing silent test failures. Fixed by
  using relative timestamps (now − N hours) that always stay within the window.

---

## [2.4.1] — 2026-04-16

### Fixed
- **Blue (house) bar negative on solar export days** — the house remainder carbon
  was computed as `main_carbon_g - sub_carbon` where `main_carbon_g` is the net
  figure (import − export) × intensity. On a net export day `main_carbon_g` is
  negative, making the remainder more negative still when sub-meter carbon is
  subtracted. Fixed: the remainder is now computed from `carbon_g_imp` (import
  carbon only, always positive) minus sub-meter carbon. The export offset is
  shown separately as a grey bar below zero.

- **`carbon_g_imp`/`carbon_g_exp` using wrong kWh values for intensity** — the
  back-calculated intensity used `main_imp_kwh`/`main_exp_kwh` from the billing
  summary which uses `imp_kwh_remainder` (house only, sub-meters stripped). But
  `carbon_g` in the engine is computed from the raw full-meter `imp_kwh`. This
  mismatch produced incorrect intensities. Fixed: raw block channel kWh are now
  accumulated directly from day_blocks for the main meter and used in the intensity
  calculation, matching what the engine used when computing `carbon_g`.

- **CO₂ totals chart showing incorrect bar heights** — the blue (house/grid) bar in
  Usage Stats CO₂ totals view was showing the full main meter net carbon rather than
  the house remainder (main minus sub-meters). This made the chart appear to
  triple-count consumption — the house bar included the EV and battery carbon which
  were then also shown separately as their own bars. Fixed: the blue bar now shows
  only the house remainder, matching exactly how kWh totals handles the main meter.

- **CO₂ totals stacked total double-counting sub-meters** — `carbon_g_total` in the
  API response was computed as `main_carbon_g + sub_carbon` rather than just
  `main_carbon_g`. Since `main_carbon_g` already includes sub-meter consumption
  (it is the full grid net carbon), adding sub-meter carbon again produced a total
  nearly double the correct figure. Fixed: `carbon_g_total = main_carbon_g`.

- **Mixed CO₂ units within a single chart render** — the CO₂ formatter was scaling
  each value independently (gCO₂ or kgCO₂ based on its own magnitude), so different
  bars, the y-axis and the data table could show different units for the same render.
  A day with large EV charging would show the EV bar as "1.710 kgCO₂" while the
  battery showed "875 gCO₂" making them appear inconsistent. Fixed: the unit is now
  determined once per render from the largest value in the dataset and applied
  consistently to all labels, axis ticks and table values.

---

## [2.4.0] — 2026-04-16

### Added
- **Carbon footprint in Usage Stats** — a third CO₂ metric alongside kWh and Cost in
  the Usage Stats chart. Shows net carbon (gCO₂) in net view and import/export carbon
  split in totals view, mirroring the kWh breakdown. Auto-scales between gCO₂ and kgCO₂.
  Only visible when a postcode prefix is configured. Pre-2.3.0 blocks show `—` in the
  data table (no CI data available). The CO₂ toggle is hidden when no postcode is set.

- **`carbon_g` in block dicts** — `_row_to_block` now includes `carbon_g` when
  reconstructing blocks from the database, making it available to the API layer.
  Previously `carbon_g` was stored in the DB but never surfaced in block dicts.

- **`has_postcode` in `/api/charts/blocks-summary`** — the response now includes a
  `has_postcode` boolean so the UI can conditionally show the CO₂ metric toggle.

- **`carbon_g_imp` / `carbon_g_exp` split on main meter** — Usage Stats totals mode
  shows import carbon above the zero line and export carbon offset below, matching the
  kWh totals view. Back-calculated from average intensity implied by the net `carbon_g`.

### Fixed
- **DB restore silently ignoring `blocks.db`** — the `/api/import` endpoint accepted
  `blocks.db` uploads but never wrote the file to disk — only `meters_config.json` was
  processed. The field was missing from `file_map`. Restoring from a zip therefore always
  left the old database in place with no error reported.

- **Restore failing with "malformed" error** — when restoring `blocks.db` via either
  `/api/backup/restore` or `/api/import/from-zip`, the engine was never paused before
  the file was overwritten. The engine's open WAL connection remained active, causing
  SQLite to declare the newly written file "malformed" on next open. The corruption
  recovery handler then renamed the restored DB to `.corrupt` and created a fresh
  empty one — silently destroying the restore. Fixed by pausing the engine, flushing
  the WAL, closing all store connections, and removing WAL/SHM files before writing
  the restored file.

- **`meters_config.json` overwriting restored DB config** — when a backup zip (which
  contains both `blocks.db` and `meters_config.json`) was restored, the sync logic
  pushed the flat JSON file back into the DB, overwriting the restored DB's full config
  period history with a single-period snapshot. Fixed: when `blocks.db` is restored,
  the DB is always authoritative — `meters_config.json` is written from the DB, never
  the reverse.

- **Engine store not reset after restore** — the web server's `_store` was set to `None`
  after restore but the engine's own `_store` was not. On the next engine tick it continued
  using the old connection, seeing the old data. Fixed by calling `engine.reset_store()`
  which closes and reopens the engine's connection from the new DB file.

### Changed
- **Zip restore flow** — dropping a backup zip on the Data Management page now shows a
  confirmation modal with the filename, size, and an optional "back up current data first"
  checkbox (checked by default) before proceeding. The restore is handled entirely
  server-side via `/api/import/from-zip` — no base64 round-trip through the browser,
  which previously corrupted binary DB files.

- **No restart required after restore** — the engine is paused, the DB replaced, and
  the engine store reset and resumed without requiring an add-on restart. The page
  reloads automatically after 2.5 seconds.

### Hardening
- **WAL checkpoint before backup** — `_create_backup_zip` now uses the engine's store
  connection and runs `PRAGMA wal_checkpoint(TRUNCATE)` before copying. This ensures
  the backup captures a fully consistent snapshot. In practice SQLite's auto-checkpoint
  means recent data is almost always already in the main file, but this makes it
  explicit and guaranteed.

- **WAL checkpoint on every startup** — `PRAGMA wal_checkpoint(TRUNCATE)` runs at the
  start of `engine_startup` before any new blocks are written. Cheap and ensures the
  DB is always fully flushed after any unclean shutdown.

- **Upgrade safety backup** — on the first startup after a version change, the engine
  creates a `*_upgrade_2.4.0.zip` in the backup list before recording any new data.
  Runs once per version by comparing against a `.last_startup_backup_version` marker
  file. Provides a complete snapshot at the point of upgrade as a precautionary
  safety net.

---

## [2.3.0] — 2026-04-14

### Added
- **Carbon intensity recording** — the engine now fetches carbon intensity data
  from the National Grid API every 15 minutes (UK only, requires postcode prefix
  in Meter Config). Data is stored in a new `carbon_intensity` table with a 4-day
  rolling retention window. Fails silently if no postcode is configured; logs a
  warning if postcode is configured but the fetch fails.

- **`carbon_g` on blocks** — net carbon footprint (gCO₂) is now computed at block
  finalise time for every meter using the nearest stored carbon intensity value.
  Formula: main meter `(imp_kwh - exp_kwh) × intensity`, sub-meters
  `imp_kwh × intensity`. NULL if no carbon intensity data is available. This is the
  foundation for carbon footprint reporting in future releases.

- **48-hour power history chart** — a new rolling chart on the Live Power page
  shows net grid power (kW) at ~10-second resolution for the past 48 hours.
  Import is shown in red, export in green, with fill regions and a per-segment
  colour line. The dashed "now" marker sits at the right edge.

- **kW / CO₂ toggle** — the power history chart can be switched between kW and
  carbon rate (gCO₂/min) views. The selected mode is remembered across page visits
  via `localStorage`. The toggle only appears when a postcode prefix is configured.
  In CO₂ mode, red indicates emitting (net import) and green indicates offsetting
  (net export), using the same colour language as the kW view.

- **Hover tooltip on power history chart** — hovering over the chart snaps to the
  nearest data point and shows local time, net kW or gCO₂/min (depending on mode),
  and the current grid carbon intensity (gCO₂/kWh). The tooltip follows the mouse
  and flips to the left side when near the right edge.

- **`/api/power/history`** — returns the rolling 48-hour power and carbon history.
  Accepts optional `?hours=N` parameter (max 48).

- **`/api/carbon/current`** — returns the current carbon intensity from the stored
  `carbon_intensity` table (faster than a live API call). Falls back to 404 if no
  data is available.

### Technical
- `BlockStore` gains `upsert_carbon_intensity`, `get_nearest_carbon_intensity`,
  `prune_carbon_intensity`, `append_power_history`, `get_power_history`, and
  `prune_power_history` methods.
- `_ensure_schema` migration adds `carbon_g` column to existing `blocks` tables and
  `carbon_gco2_min` column to `power_history`.
- Carbon rate (`carbon_gco2_min`) is derived from the power sensor reading directly
  (`net_kw × intensity ÷ 60`) rather than from meter read deltas, which would
  produce zeros and spikes due to the sensor's ~60-second update cadence vs the
  engine's ~10-second tick rate.
- `power_history` has 48-hour rolling retention pruned on every engine tick.
- `carbon_intensity` has 4-day rolling retention pruned on every fetch cycle.

---

## [2.2.3] — 2026-04-12

### Changed
- **blocks.json import removed from UI** — dragging a `blocks.json` file into the
  Import page is no longer supported. The manual upgrade path (placing `blocks.json`
  on disk and letting the engine migrate it on startup) still works. Full removal
  of `migrate_json_to_sqlite()` is scheduled for 3.0.0.

---

## [2.2.2] — 2026-04-11

### Fixed
- **Billing totals kWh double-counting** — `get_billing_totals_for_local_date_range`
  and `get_cumulative_totals` both fell back to raw `imp_kwh` for sub-meter blocks
  where `imp_kwh_grid` was NULL, double-counting consumption already included in the
  main meter total. Fixed to use `COALESCE(imp_kwh_grid, 0)` in both methods.

- **Usage Stats calendar/billing toggle** — switching between billing and calendar
  modes in daily view left the table stale and caused an empty chart on switching
  back. Fixed by correctly handling `barDailyMonth` as either a string (billing mode)
  or object (calendar mode) in `barGetDataForPeriod` and `barBuildSubBar`.

---

## [2.2.1] — 2026-04-11

### Fixed
- **Billing totals incorrect kWh** — `get_billing_totals_for_local_date_range`
  was falling back to raw `imp_kwh` for sub-meter blocks where `imp_kwh_grid` was
  NULL, double-counting consumption already included in the main meter total. Fixed
  to use `COALESCE(imp_kwh_grid, 0)` — NULL means no recorded grid import, not
  missing data. Affected Today / This Bill / This Year totals on the Live Power cards.

- **Standing charge incorrect on Live Power cards** — `get_billing_totals_for_local_date_range`
  used `MIN(standing_charge)` per day, which picked 0 for days where the standing
  charge sensor hadn't updated for the first block. Fixed to use `MAX(standing_charge)`
  per day, matching the billing chart which takes the highest (correct) value recorded
  for that day.

---

## [2.2.0] — 2026-04-11

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

- **Lovelace-friendly chart endpoints** — `/lovelace/billing` and `/lovelace/heatmap`
  serve the chart HTML with a 130-second meta refresh and aggressive no-cache headers
  baked in at serve time. Use these URLs in Lovelace webpage cards instead of the raw
  `/charts/*.html` URLs — they refresh reliably and never get stuck in the browser cache.
  Documented in the Help page.

### Fixed
- Date inputs in Delete Blocks and Historical Corrections now show `dd/mm/yyyy` format
  hint in labels and use `lang="en-GB"` to encourage day-first display in supporting
  browsers.

- **Chart flicker removed** — `<meta http-equiv="refresh">` removed from generated chart
  HTML. The EMT charts page handles refresh cleanly via `setInterval` without reloading
  the iframe. Lovelace users should use the dedicated `/lovelace/*` endpoints which have
  the meta refresh injected at serve time.

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
- **Historical Corrections enhanced** — rate corrections now support time-of-day window, per-meter targeting, and per-block preview table
- Import & Backup page — file reference table and restore UI reflect single-file model

### Removed
- `full_config_json TEXT` column from `config_periods`
- `gap_marker TEXT` blob column from `current_block`
- `meter_channel_meta` key/value table
- `cumulative_totals.json`, `current_block.json`, `meters_config.json` as authoritative state files

---

## [2.0.1] — 2026-04-05

### Added
- **Historical Corrections** — new section on the Import & Backup page; bulk-update standing charge or import/export rates across a local date range in the live database; Preview shows affected block and day counts plus current value range before committing; rate corrections optionally recalculate cost from corrected rate × kWh

### Fixed
- **kWh and cost alignment between Billing chart and Usage Stats**
- **Standing charge double-counted in Usage Stats (BST days)**
- **Standing charge not shown in Usage Stats**
- **Usage Stats blocks fetched by local_date**
- **Billing period computation in Usage Stats**

---

## [2.0.0] — 2026-04-05

### Added
- **SQLite database** — all blocks now stored in a SQLite database (`energy_meter.db`) replacing the `blocks.json` flat file
- **Config history (Billing History)** — every billing-significant config change is recorded as a new config period
- **Billing History page** — accessible via the 🕓 Billing History button on the Meter Config page
- **Billing period transition logic** — truncation-only model; bills can only be truncated, never extended
- **Usage Stats — billing period navigator**
- **Live Power — billing-accurate "This Bill"**
- **Fast SQL billing aggregation**
- **Gauge scale cache** — 7-day percentile cached for 30 minutes

### Changed
- **Live Power page loads instantly** — billing card data fetched asynchronously
- **Billing History removed from sidebar nav**
- **Config period removal** — any period can be removed when 2+ periods exist

---

## [1.6.3] — 2026-04-01

### Fixed
- Heatmap mobile portrait — chart fills full viewport
- Heatmap mobile pinch zoom
- Heatmap scroll-guard strip overlapping totals bar
- Usage Stats width unconstrained on mobile portrait

---

## [1.6.2] — 2026-04-01

### Added
- Usage Stats — Billing/Calendar period toggle
- Usage Stats data table — totals column
- Usage Stats data table — period labels
- Global light/dark theme toggle

### Fixed
- Usage Stats export cost positive in data table
- Usage Stats meter labels include site name
- Various light/dark mode and heatmap fixes

---

## [1.6.0] — 2026-04-01

### Added
- **Usage Stats chart** — daily, monthly and yearly import/export with sub-meter breakdown
- **Data table** — tabular view with copy-to-clipboard export
- **Light/dark theme toggle**

### Changed
- Summary page renamed to Live Power
- Remember last visited page
- Chart tabs renamed — Daily Usage → Billing, Import / Export → Usage Stats

---

## [1.5.1] — 2026-03-26

### Added
- Live Power page — gauge, billing cards, carbon intensity forecast
- Power sensor and postcode prefix fields in Meter Config

---

## [1.4.0] — 2026-02-10

### Added
- Configurable meter reconciliation period (5, 15 or 30 minutes)
- Automatic currency detection

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