# Energy Meter Tracker

A Home Assistant add-on that records your electricity usage in precise half-hour blocks — the same intervals used by your energy supplier for billing.

## What it does

- Records import and export meter readings at every :00 and :30 boundary
- Interpolates precisely to the boundary timestamp so block deltas are billing-accurate
- Tracks sub-meters (EV charger, home battery, heat pump) and distributes grid consumption across them
- Fills gaps automatically if the add-on restarts mid-session
- Publishes four cumulative sensors back to Home Assistant
- Serves a local web UI on port 8099 for configuration, charts, logs and data management

## Requirements

- A smart meter with a Consumer Access Device (CAD) publishing readings via MQTT to Home Assistant, updating at least every 60 seconds (10 seconds recommended)
- Cumulative kWh sensors for import and export
- Live rate sensors (£/kWh) for import and export tariffs
- Home Assistant OS or Supervised installation

## Installation

1. Add this repository to your Home Assistant add-on store
2. Install **Energy Meter Tracker**
3. Start the add-on and open the Web UI
4. Use the **Setup Wizard** to configure your main meter and sub-meters
5. Save — the engine will begin recording immediately

## Web UI

Access the UI at `http://<your-ha-ip>:8099`

| Page | Description |
|------|-------------|
| Meter Config | Configure main meter, sub-meters, sensors and rates |
| Charts | Half-hour heatmap and daily import/export chart |
| Import Data | Migrate data from a previous installation |
| Logs | Live add-on log viewer |
| Help | Full reference documentation |

## Home Assistant Sensors

After each block finalises, four synthetic sensors are updated:

| Sensor | Description |
|--------|-------------|
| `sensor.energy_meter_import_kwh` | Cumulative grid import (kWh) |
| `sensor.energy_meter_export_kwh` | Cumulative grid export (kWh) |
| `sensor.energy_meter_import_cost` | Cumulative import cost (£) |
| `sensor.energy_meter_export_credit` | Cumulative export credit (£) |

These are compatible with the HA Energy dashboard and Utility Meter integrations.

## Data & Backup

All data is stored in the add-on's private `/data/` directory. After every block finalise, files are copied to `/share/energy_meter_tracker_backup/`. Zip snapshots are created automatically before every config save.

> ⚠️ **Uninstalling the add-on will wipe `/data/`**. Always ensure a recent backup exists in `/share/` before uninstalling.

## Disclaimer

Energy Meter Tracker is for informational use only. It cannot replicate your supplier's authoritative Half-Hourly reconciliation. Do not use this data for billing disputes or formal energy accounting.

## Supported Hardware

| Architecture | Supported |
|-------------|-----------|
| amd64 | ✅ |
| aarch64 | ✅ |
| armhf | ✅ |
| armv7 | ✅ |