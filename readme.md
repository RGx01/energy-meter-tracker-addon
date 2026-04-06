# Energy Meter Tracker

[![GitHub Release][releases-shield]][releases]
![Project Stage][project-stage-shield]
[![License][license-shield]](LICENSE)
[![Community Forum][forum-shield]][forum]
[![GitHub Activity][commits-shield]][commits]
![Project Maintenance][maintenance-shield]

![Supports aarch64 Architecture][aarch64-shield]
![Supports amd64 Architecture][amd64-shield]
![Supports armhf Architecture][armhf-shield]
![Supports armv7 Architecture][armv7-shield]

[releases-shield]: https://img.shields.io/github/release/RGx01/energy-meter-tracker-addon.svg
[releases]: https://github.com/RGx01/energy-meter-tracker-addon/releases
[project-stage-shield]: https://img.shields.io/badge/project%20stage-production%20ready-brightgreen.svg
[license-shield]: https://img.shields.io/badge/license-BUSL--1.1-blue.svg
[forum-shield]: https://img.shields.io/badge/community-forum-informational.svg
[forum]: https://community.home-assistant.io/t/energy-meter-tracker/995674
[commits-shield]: https://img.shields.io/github/commit-activity/y/RGx01/energy-meter-tracker-addon.svg
[commits]: https://github.com/RGx01/energy-meter-tracker-addon/commits/main
[maintenance-shield]: https://img.shields.io/maintenance/yes/2026.svg
[aarch64-shield]: https://img.shields.io/badge/aarch64-yes-green.svg
[amd64-shield]: https://img.shields.io/badge/amd64-yes-green.svg
[armhf-shield]: https://img.shields.io/badge/armhf-yes-green.svg
[armv7-shield]: https://img.shields.io/badge/armv7-yes-green.svg

A Home Assistant add-on that records your electricity usage in precise configurable intervals — matching your energy supplier's meter reconciliation period for accurate billing.

![Usage Stats chart showing daily import/export breakdown by sub-meter](screenshots/usage_stats0.png)
![Usage Stats chart showing daily import/export breakdown by sub-meter](screenshots/usage_stats1.png)

## What it does

- Records import and export meter readings at configurable reconciliation period boundaries — 5, 15 or 30 minutes — matching your supplier's billing resolution
- Interpolates precisely to the boundary timestamp so block deltas are billing-accurate
- Tracks sub-meters (EV charger, home battery, heat pump) and distributes grid consumption across them
- Fills gaps automatically if the add-on restarts mid-session
- Publishes four cumulative sensors back to Home Assistant
- Serves a local web UI on port 8099 for configuration, charts, live power and data management

## What's new in 2.0.1

- **🛠️ Historical Corrections** — bulk-update standing charge or import/export rates across a date range directly in the live database; always edits `/data/` not the `/share` backup copy
- **📊 Billing alignment fixed** — kWh, cost and standing charge now agree exactly between Billing chart, Usage Stats and Live Power for all dates including BST period boundaries
- **🗄️ SQLite storage** — blocks are now stored in a SQLite database rather than a JSON file; queries are indexed and fast regardless of how much history you have; migration from `blocks.json` is automatic on first start
- **🕓 Billing History** — config changes are now recorded as history; billing charts always use the billing day and rates that were active when each block was recorded; access via the **Billing History** button on Meter Config
- **📅 Billing period transitions** — when you change your billing day the old period is correctly truncated at the transition date; usage stats and live power always show the right period boundaries
- **⚡ Live Power loads instantly** — billing card data (Today, This Bill, This Year) now loads asynchronously after page render using fast SQL aggregation queries that complete in milliseconds regardless of history length

## Requirements

- A smart meter with a Consumer Access Device (CAD) publishing readings via MQTT to Home Assistant, updating at least every 60 seconds (10 seconds recommended)
- Cumulative kWh sensors for import and export
- Live rate sensors (£/kWh or local currency equivalent) for import and export tariffs
- Home Assistant OS, Supervised, or standalone Docker
- For correct local day assignment, configure your timezone in Meter Config (e.g. `Europe/London` for UK users)

## Installation

### HA OS / Supervised (recommended)

1. Add this repository to your Home Assistant add-on store
2. Install **Energy Meter Tracker**
3. Start the add-on and open the Web UI
4. Use the **Setup Wizard** to configure your main meter and sub-meters
5. Save — the engine will begin recording immediately

### Standalone Docker

If you run Home Assistant Container (plain Docker) without the Supervisor, clone the repo and build locally using the provided `Dockerfile.standalone`:

**Step 1 — Clone the repo**
```bash
git clone https://github.com/RGx01/energy-meter-tracker-addon.git
cd energy-meter-tracker-addon
```

**Step 2 — Create a data directory**
```bash
mkdir -p ~/emt-data
```

**Step 3 — Create a Long-Lived Access Token**

In your HA instance go to your profile → **Security → Long-Lived Access Tokens → Create Token**.

**Step 4 — Add to your docker-compose.yml**
```yaml
  energy-meter-tracker:
    build:
      context: ./energy-meter-tracker-addon
      dockerfile: Dockerfile.standalone
    container_name: energy-meter-tracker
    restart: unless-stopped
    ports:
      - "8099:8099"
    environment:
      - EMT_MODE=standalone
      - HA_URL=http://homeassistant:8123
      - LOG_LEVEL=info
      - HA_TOKEN=your_long_lived_access_token
    volumes:
      - ~/emt-data:/data/energy_meter_tracker
```

Replace `homeassistant` in `HA_URL` with your HA container service name, or use the host IP address if they are on different networks.

**Step 5 — Build and start**
```bash
docker-compose up -d --build energy-meter-tracker
```

Access the UI at `http://<host>:8099`.

> ⚠️ Ingress (sidebar embedding) is only available in HA OS/Supervised. In standalone mode access the UI directly at `http://<host>:8099`.

> ℹ️ Logs are written to `/data/energy_meter_tracker/addon.log` in standalone mode and are viewable from the **Logs** page in the UI.

**Optional — add to HA sidebar**

You can embed the UI in your HA sidebar using `panel_iframe` in your `configuration.yaml`:

```yaml
panel_iframe:
  energy_meter:
    title: "Energy Meter"
    icon: mdi:speedometer
    url: "http://192.168.1.x:8099"
```

Replace `192.168.1.x` with your Docker host IP. Restart HA after adding this.

## Web UI

Access the UI at `http://<your-ha-ip>:8099`

| Page | Description |
|------|-------------|
| Meter Config | Configure main meter, sub-meters, sensors, power sensor and postcode |
| Billing History | View, edit and add config periods; record billing day / address / supplier changes |
| Charts | Billing chart, net energy heatmap and usage stats |
| ⚡ Live Power | Live power gauge, billing cards and carbon intensity forecast |
| Import & Backup | Migrate data from a previous installation or restore a backup |
| Logs | Live add-on log viewer |
| Help | Full reference documentation |

## Charts

### Billing

The daily billing chart shows import, export and sub-meter consumption for each day, with accurate cost calculations matching the engine's billing logic. Billing periods, standing charges and rate changes are all handled correctly. If your billing day has changed, each period uses the billing day that was active at the time.

### Net Energy Heatmap

A half-hour heatmap showing net grid flow (import − export) for every reconciliation period. Colour-coded from red (import) through white to blue (export), making it easy to spot patterns — overnight EV charging, solar export windows, evening peaks.

![Net energy heatmap](screenshots/heatmap.png)

### Usage Stats

Import and export broken down by day, month or year with sub-meter stacking. Switch between kWh and cost, and between Totals and Net views. A data table below the chart mirrors exactly what the chart shows, with a copy-to-clipboard button for exporting to Excel or Google Sheets.

Billing mode groups data by your billing periods (respecting billing day changes). Calendar mode groups by calendar month.

![Usage Stats chart](screenshots/usage_stats.png)

## Live Power

The Live Power page appears in the sidebar once a **power sensor** is configured in Meter Config.

![Live Power page showing gauge, carbon intensity and billing cards](screenshots/live_power.png)

It provides:

- **Live power gauge** — shows net grid flow with asymmetric import/export scales derived from your usage history; colour reflects carbon intensity (UK) or import magnitude (global)
- **Billing cards** — Today, This Bill and This Year with full sub-meter breakdown; figures match the Billing chart exactly; This Bill uses your billing history to show the correct period even if your billing day has changed
- **Carbon intensity** (🇬🇧 UK only) — add your outward postcode prefix (e.g. `DE1`) in Meter Config to enable a 48-hour forecast strip from the National Grid API

### Configuring Live Power

In Meter Config → main meter card:

| Field | Description |
|-------|-------------|
| Power Sensor | Live power in kW — e.g. `sensor.smart_meter_electricity_power` |
| Postcode Prefix | 🇬🇧 UK only — outward postcode with district, e.g. `DE1`, `SW1A`, `M1` |

## Billing History

The Billing History page records when your billing configuration changed. Access it via the **🕓 Billing History** button on the Meter Config page.

Use **New Period** when you:
- Move address
- Change energy supplier
- Change your billing day
- Add or change meters

Each period stores the billing day, timezone, currency, site name and a freetext change reason. Billing charts always use the config that was active when each block was recorded, so historical figures remain accurate after any change.

### Period transitions

When you add a new period, the previous period's final billing cycle is **truncated** at the transition date — it cannot be extended. If your billing day changes from the 3rd to the 15th and you set the effective date to the 10th, your last bill under the old config runs from the 3rd to the 14th, and your first bill under the new config starts on the 15th.

### Removing periods

When 2 or more periods exist, any period can be removed. Blocks from the removed period are reassigned to the previous (older) period. If you remove the active (most recent) period, the previous period becomes active again.

## Home Assistant Sensors

After each block finalises, four synthetic sensors are updated:

| Sensor | Description |
|--------|-------------|
| `sensor.energy_meter_import_kwh` | Cumulative grid import (kWh) |
| `sensor.energy_meter_export_kwh` | Cumulative grid export (kWh) |
| `sensor.energy_meter_import_cost` | Cumulative import cost |
| `sensor.energy_meter_export_credit` | Cumulative export credit |

These are compatible with the HA Energy dashboard and Utility Meter integrations.

## Data & Backup

### Storage

All blocks are stored in a SQLite database (`energy_meter.db`) in the add-on's data directory. After every block finalise, the database and config are also copied to `/share/energy_meter_tracker_backup/`. Zip snapshots are created automatically before every config save and are accessible from the Import & Backup page.

| Event | `/data/` | `/share/energy_meter_tracker_backup/` |
|-------|----------|---------------------------------------|
| Add-on update | ✅ Preserved | ✅ Preserved |
| HA restart | ✅ Preserved | ✅ Preserved |
| Add-on uninstall | ❌ **Wiped** | ✅ Preserved |

> ⚠️ **Uninstalling wipes `/data/`**. Always ensure a recent backup exists in `/share/` before uninstalling.

> ℹ️ There is no automatic pre-upgrade backup in supervised mode. Your most recent `/share` backup and the automatic zip before the last config save are your safety net. Create a manual backup before upgrading if you want extra assurance.

### Migrating from 1.x

If you are upgrading from a version that used `blocks.json`, the add-on will automatically migrate your data to SQLite on first start. The original `blocks.json` is preserved. Migration typically takes a few seconds for a year of 5-minute blocks.

### Standalone Docker

The volume mount is **essential** — without it all data is lost when the container is recreated:

```bash
-v /path/to/data:/data/energy_meter_tracker
```

> ⚠️ **Before upgrading**, always create a manual backup from the Import & Backup page and copy it off the host.

## Disclaimer

Energy Meter Tracker is for informational use only. It cannot replicate your supplier's authoritative Half-Hourly reconciliation. Do not use this data for billing disputes or formal energy accounting.