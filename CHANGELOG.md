# Changelog

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