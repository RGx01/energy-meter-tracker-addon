# Changelog

## [1.1.0] — Unreleased

### Added
- Setup wizard for first-time configuration (EV, Battery, Heat Pump)
- Live log viewer page (auto-refreshes every 5 seconds)
- Help & Reference page with full engine documentation
- Backup zip created automatically before every config save
- Manual backup button on Import & Backup page
- Backup list showing 10 most recent zips
- Entity pickers filtered by sensor type (kWh, £/kWh, £/day) with unit hints
- Warning banner on Charts page when no blocks are loaded
- Meter ID locked after first save to prevent orphaning historical data
- Config save triggers engine_startup to re-register sensor subscriptions

### Fixed
- HA restart reconnect — WebSocket now retries indefinitely until HA is back
- Double subscription on reconnect after HA restart
- Unclosed aiohttp session on reconnect attempts
- Channel meta (MPAN, tariff) no longer stripped on config save
- Backup to /share re-enabled after startup issue resolved

### Changed
- Replaced Werkzeug development server with Waitress for production use
- Removed diagnostic boot.log lines from run.sh
- Solar removed from setup wizard (not yet supported as sub-meter type)
- Entity picker now returns unit_of_measurement and device_class for filtering

## [1.0.4] — 2026-03-12

### Added
- Flask web UI on port 8099
- Meter configuration page with entity picker
- Charts page (net heatmap, daily import/export)
- Import page for migrating PyScript data
- Auto-start on boot
- HA WebSocket reconnect with engine re-startup
- Backup to /share after every block finalise
- pause_engine / resume_engine during data import

### Fixed
- Auto-start on full host reboot confirmed working
- Block finalisation and rolling buffer pruning

## [1.0.0] — 2026-03-10

### Added
- Initial port from PyScript to HA add-on
- Half-hour block engine with boundary interpolation
- Gap detection and interpolated gap filling
- Sub-meter support with PASS 2 grid-authoritative distribution
- Battery/inverter logic (inverter_possible, v2x_capable)
- Four synthetic HA sensors (import/export kWh and cost)
- Chart generation (net heatmap, daily usage)