# Changelog

## [1.3.1] — 2026-03-22

### Fixed
- **UTC time bug** — replaced deprecated `datetime.utcnow()` with `datetime.now(timezone.utc)` throughout the engine; on systems where the OS clock is not set to UTC this was causing incorrect block timestamps and rapid catch-up finalisation on restart
- **Engine crash on data reset** — `capture_samples` now guards against a missing `meters` key in `current_block.json`; previously clearing the file to `{}` caused repeated `KeyError: 'meters'` crashes
- **Charts always using UTC timezone** — `generate_charts` was reading timezone from the wrong location in config (top-level key that doesn't exist) instead of the meter meta; charts now correctly use the configured timezone (e.g. `Europe/London`)
- **Standing charge averaging** — billing summary now shows separate rows per rate when a tariff change occurs mid-period rather than showing a misleading average across all days
- **Heatmap hover offset** — replaced CSS `zoom` scaling with `transform: scale()` so hover tooltips register on the correct cell when the heatmap is scaled to fit smaller screens

---

## [1.3.0] — 2026-03-21

### Added
- **Timezone support** — charts now assign blocks to the correct local day based on the user's timezone; configurable per meter in Meter Config and the setup wizard; defaults to UTC for backward compatibility (credit: KShips for the original implementation)
- **Standing charge rate split** — billing summary now shows separate rows for each standing charge rate when a tariff change occurs mid-period, rather than averaging across all days

### Changed
- Billing summary standing charge display groups days by rate, showing count and subtotal per rate
- Meter Config and wizard timezone field uses a curated dropdown of 30 common IANA timezones
- UK users should set `Europe/London` to correctly handle BST/GMT transitions

---

## [1.2.0] — 2026-03-20

### Added
- **Standalone Docker support** — run without HA Supervisor by providing `HA_URL` and `HA_TOKEN` environment variables; `run.sh` auto-detects mode based on `SUPERVISOR_TOKEN` presence; `Dockerfile.standalone` uses `python:3.12-slim` and works on any platform including Apple Silicon
- **Logs in standalone mode** — Python logging writes to `/data/energy_meter_tracker/addon.log` in standalone mode so the Logs page works without the Supervisor API
- **`Dockerfile.standalone`** — separate Dockerfile for standalone Docker users; original `Dockerfile` unchanged for supervised HA
- **Zip import** — drag a backup zip directly onto the import page; JSON files are extracted server-side and presented for preview before importing
- **Selective restore modal** — clicking Restore on a backup opens a modal showing all files with checkboxes; user selects which files to restore with mismatch warning if `blocks.json` and `meters_config.json` are not restored together; auto-backup created before restoring
- **Last-finalise backup restore** — the flat file backup copied to `/share` after every block finalise is now visible and restorable from the Import & Backup page
- **Wizard auto-save** — pressing Finish in the setup wizard now saves config automatically without needing to find the Save button
- **Mobile hamburger menu** — sidebar collapses to a hamburger button on mobile portrait and landscape; sidebar slides in as an overlay
- **Chart auto-refresh** — charts reload automatically every 2 minutes without manual page refresh
- **Minimum chart height** — daily charts enforce a 320px minimum height to prevent collapse when secondary axis is absent
- **DEVELOPMENT.md** — architecture guide covering block lifecycle, interpolation, gap filling, file structure, running tests and local dev setup
- **CONTRIBUTING.md** — contribution guidelines covering bug reporting, feature requests, branch naming, PR workflow and code style

### Fixed
- Rate line no longer drops to zero on the current in-progress day — truncated at last known reading
- Chart height instability (runaway height) resolved with fixed minimum and improved sizing logic
- Heatmap scroll now uses `100vh` so all rows are reachable regardless of screen height
- Heatmap right-edge scroll conflict on mobile — touch guard overlay allows page scroll without triggering chart zoom
- Weekend shading in heatmap now correctly fills to the right edge of the totals chart
- Charts not updating after HA session timeout — blob URL cache cleared on auto-refresh cycle
- Period mode switching (Bill/Month/Quarter/Year) no longer triggers a chart reload delay — resize suppressed during DOM changes via `postMessage`
- Billing table right-justification fixed — channel title and site header rows now correctly left-aligned
- Daily chart summary panel font sizes now consistent across all meter types at all zoom levels

### Changed
- Import & Backup page restructured — backups section moved to top, Create Backup button prominent, PyScript path column removed, Restore risk column added to file reference
- Backup list shows top 5 with scroll for more, restore button per entry
- Period nav bar tightened — consistent `11px` font and reduced padding across all buttons, labels and selectors so bar fits on one row
- Billing table row padding and font sizes reduced — more bill visible without scrolling
- Daily chart summary panel uses `clamp(10px, 1vw, 13px)` font scaling and `white-space: nowrap` to prevent wrapping at any zoom level
- Chart page `max-width` constraint removed — charts fill full iframe width at any zoom level
- Sub-meter card and wizard device hints now explicitly state sensors must be **cumulative kWh consumed (import only)** — not net, not export, not watts
- Sub-meter info box now states that if no rate sensor is provided the main meter import rate is used automatically
- Help page sensor requirements updated to match
- Mobile chart height uses more available vertical space in both portrait and landscape
- Panel icon changed to `mdi:speedometer`
- README data & backup section expanded with survival table, pre-upgrade advice and standalone Docker volume mount guidance

### Known Issues
- Plotly legend occasionally renders with a spurious scrollbar on mobile portrait — self-corrects on first user interaction with the chart

---

## [1.1.0] — 2026-03-14

### Added
- **Setup Wizard** — guided configuration flow for main meter, EV charger, battery and heat pump with sensor pre-population from existing config
- **Live Log Viewer** — dedicated logs page with auto-refresh, colour-coded levels and line count selector
- **Help & Reference page** — full engine documentation including disclaimer, interpolation explanation, sub-meter logic, sensor requirements, gap filling and data storage
- **Ingress support** — "Show in sidebar" toggle now works in HA; all pages accessible embedded in the HA UI
- **Logo and icon** — branding shown in sidebar and add-on store, sidebar icon changed to `mdi:meter-electric`
- **Backup on config save** — zip snapshot created automatically before every config change with 20-zip rolling retention
- **Manual backup button** — on the Import & Data page with list of 10 most recent backups
- **Entity picker filtering** — dropdowns filtered by sensor type (kWh for reads, £/kWh for rates, £/day for standing charge) with unit shown inline
- **Entity picker width** — dropdown expands to full entity ID width, no more truncation on long names
- **Meter ID lock** — existing meter IDs are read-only after first save to prevent orphaning historical data
- **Zero blocks warning** — amber banner on Charts page when no data has been recorded yet
- **Charts as default page** — app opens directly to Charts once meters are configured
- **Config reload on save** — engine re-registers sensor subscriptions immediately after a config change without requiring a restart
- **HA restart resilience** — WebSocket reconnects indefinitely after HA restart and re-runs full engine startup on reconnect
- **Unit tests** — 42 tests covering interpolation, gap detection, block computation, boundary reads and gap filling

### Fixed
- Channel meta (MPAN, tariff, source) no longer stripped on config save
- Double sensor subscription after HA restart reconnect
- Unclosed aiohttp session on failed reconnect attempts
- All fetch() calls updated for Ingress path compatibility using `apiUrl()` helper
- Plotly charts now render correctly in sidebar via blob URL iframe
- Backup list and import fetch calls had mismatched parentheses — fixed
- Backup to `/share` re-enabled after startup ordering issue resolved

### Changed
- Replaced Werkzeug development server with Waitress for production use
- Removed diagnostic boot.log lines from run.sh
- Solar removed from setup wizard — export sub-metering not yet supported
- Charts loaded via API and blob URL rather than direct iframe to support Ingress
- Backup location standardised to `/share/energy_meter_tracker_backup/` with dated zip archives

---

## [1.0.0] — 2026-03-10

### Added
- Initial port from PyScript to Home Assistant add-on
- Half-hour block engine with boundary interpolation
- Gap detection and interpolated gap filling (up to 12 hours)
- Sub-meter support with PASS 2 grid-authoritative consumption distribution
- Battery/inverter logic (`inverter_possible`, `v2x_capable` flags)
- Standing charge capture per block
- Four synthetic HA sensors — import/export kWh and cost (`total_increasing`)
- Net heatmap and daily import/export charts
- Flask web UI on port 8099
- Meter configuration page with entity search
- Import page for migrating PyScript data files
- Backup to `/share` after every block finalise
- Auto-start on boot