# Energy Meter Tracker

[![GitHub Release][releases-shield]][releases]
![Project Stage][project-stage-shield]
[![License][license-shield]](LICENSE)
[![Community Forum][forum-shield]][forum]
[![GitHub Activity][commits-shield]][commits]
![Project Maintenance][maintenance-shield]

![Supports aarch64 Architecture][aarch64-shield]
![Supports amd64 Architecture][amd64-shield]

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

A Home Assistant add-on that records your electricity usage in precise configurable intervals — matching your energy supplier's meter reconciliation period for accurate billing.

<table>
  <tr>
    <td align="center" width="33%">
      <a href="screenshots/live-power.png">
        <img src="screenshots/live-power.png" width="240" alt="Live Power"><br>
        <sub><b>Live Power</b></sub>
      </a>
    </td>
    <td align="center" width="33%">
      <a href="screenshots/carbon-insights.png">
        <img src="screenshots/carbon-insights.png" width="240" alt="Carbon Insights"><br>
        <sub><b>Carbon Insights</b></sub>
      </a>
    </td>
    <td align="center" width="33%">
      <a href="screenshots/usage-insights.png">
        <img src="screenshots/usage-insights.png" width="240" alt="Usage Insights"><br>
        <sub><b>Usage Insights</b></sub>
      </a>
    </td>
  </tr>
  <tr>
    <td align="center" width="33%">
      <a href="screenshots/billing-chart.png">
        <img src="screenshots/billing-chart.png" width="240" alt="Billing Chart"><br>
        <sub><b>Billing Chart</b></sub>
      </a>
    </td>
    <td align="center" width="33%">
      <a href="screenshots/heatmap.png">
        <img src="screenshots/heatmap.png" width="240" alt="Net Energy Heatmap"><br>
        <sub><b>Net Energy Heatmap</b></sub>
      </a>
    </td>
    <td align="center" width="33%">
      <a href="screenshots/usage-stats-totals.png">
        <img src="screenshots/usage-stats-totals.png" width="240" alt="Usage Stats"><br>
        <sub><b>Usage Stats</b></sub>
      </a>
    </td>
  </tr>
</table>

---

## What it does

- Records import and export meter readings at configurable reconciliation period boundaries — 5, 15 or 30 minutes — matching your supplier's billing resolution
- Interpolates precisely to the boundary timestamp so block deltas are billing-accurate
- Tracks Devices — EV charger, home battery, heat pump — and distributes grid consumption across them
- Optionally reconciles every block against your supplier's settled half-hourly data (Octopus / Kraken DCC) for billing-grade accuracy
- Understands Intelligent Octopus Go smart-charge slots and prices the affected blocks at the off-peak rate you were actually charged
- Fills gaps automatically if the add-on restarts mid-session
- Publishes four cumulative sensors back to Home Assistant, compatible with the HA Energy dashboard
- Serves a local web UI on port 8099 for configuration, charts, insights, Overview and data management
- Records grid carbon intensity at every block (🇬🇧 UK, postcode required) for per-period carbon accounting
- **Carbon Insights** — house consumption, solar offset, EV mileage and gCO₂/mile, battery charging behaviour, heat pump vs gas comparison, grid generation mix breakdown
- **Usage Insights** — cost breakdown by meter, rate period distribution, grid position, peak demand window, per-device costs with period comparison narrative in plain English
- Exports any chart tab as a PDF report — Billing (with per-day data tables and billing summary), Heatmaps, and Usage Stats

---

## 📖 Documentation

Full documentation is on the **[GitHub Wiki](https://github.com/RGx01/energy-meter-tracker-addon/wiki)**:

| | |
|---|---|
| [Installation and Setup](https://github.com/RGx01/energy-meter-tracker-addon/wiki/Installation-and-Setup) | Installing, first run, Setup Wizard |
| [Configuring Meters](https://github.com/RGx01/energy-meter-tracker-addon/wiki/Configuring-Meters) | Main meter, sub-meters, sensors |
| [Managing Billing Periods](https://github.com/RGx01/energy-meter-tracker-addon/wiki/Managing-Billing-Periods) | Config history, tariff changes |
| [Live Power](https://github.com/RGx01/energy-meter-tracker-addon/wiki/Live-Power) | Gauges, billing cards, generation mix |
| [Charts](https://github.com/RGx01/energy-meter-tracker-addon/wiki/Charts) | Billing chart, heatmap, usage stats |
| [Insights](https://github.com/RGx01/energy-meter-tracker-addon/wiki/Insights) | Carbon and Usage analysis, PDF export |
| [Data Management](https://github.com/RGx01/energy-meter-tracker-addon/wiki/Data-Management) | Backups, restore, corrections |
| [Sensor Requirements](https://github.com/RGx01/energy-meter-tracker-addon/wiki/Sensor-Requirements-and-Known-Limitations) | Sensor types, units, known limitations |
| [Carbon Intensity](https://github.com/RGx01/energy-meter-tracker-addon/wiki/Carbon-Intensity) | How carbon data is fetched and used |

---

## What's new in 3.2

- **🩹 Outage recovery** — if the add-on was offline longer than the 12-hour gap-fill limit, those blocks were lost for good: the settlement poll only ever looks forward. **Data Management → Missing Data** now finds the holes and rebuilds them from your supplier's settled readings. Manual on purpose — EMT can't tell an outage from blocks you deleted deliberately.
- **⚡ Settlement no longer freezes after an outage** — a gap marker was wrongly gating the step that applies settled figures to billing, so *every* block's costs stayed on pre-settlement estimates until the marker cleared.
- **🔔 Global notification region** — messages now appear between the topbar and the page instead of floating over it, so they no longer overlap the page content ([#219](https://github.com/RGx01/energy-meter-tracker-addon/issues/219)). Includes an **update-available** notice for standalone users, who get no Supervisor badge. Dismissal sticks per version.
- **🗂️ Backups isolated per site** — supervised backups live in `/share`, which HA shares across *all* add-ons, so two EMT instances could list and restore each other's backups. The directory is now namespaced by site name.
- **🔌 Four unused sensors removed** — `sensor.energy_meter_import_kwh`, `_export_kwh`, `_import_cost`, `_export_credit`. They published live per-block figures that settlement later corrects, so they disagreed with EMT's own billing. **Breaking** if you referenced them.

Earlier 3.x releases added Intelligent Octopus dispatch-lifecycle pricing (smart charges billed from the dispatch data rather than the meter, so solar-supplied charges price correctly), support for the new IOG 6-hour-cap tariff, and the Spiral chart.

See the [full changelog](CHANGELOG.md) for the complete list.

---

## What's new in 3.0

EMT 3.0 is the largest release so far — and a **major-additive one: nothing breaks**. Existing installs upgrade in place, keep working exactly as before, and switch the new things on when ready. Everything new is opt-in through the Setup Wizard.

- **🔌 Supplier settlement (Octopus / Kraken DCC)** — reconcile every block against the settled half-hourly data your supplier actually bills from. A billing-source toggle chooses whether your bill is driven by your meter sensor or the supplier API; credentials are entered in-app (no YAML), and corrections are gated until a block is settled. Opt-in.
- **🧙 Supplier-first Setup Wizard & data-source modes** — setup now starts from your supplier and meter situation and configures the right path: **`cad`** (local sensor only, as in 2.x) or **`cad+api`** (local readings plus supplier settlement). A **Change Setup** launcher lets you switch later.
- **🚗 Intelligent Octopus Go awareness** — EMT captures IOG smart-charge dispatch slots and prices the affected blocks at the off-peak rate you were actually charged, at both finalise and settlement. Per-device "use dispatch overlay" rates let your EV bill at the dispatch rate while the house stays on the standard tariff.
- **📡 Octopus Home Mini live power** — use a Home Mini's real-time demand feed for the gauges and 48-hour history without a separate CAD sensor.
- **🌍 Grid carbon intensity everywhere** — gCO₂/kWh recorded on every block, with a Carbon Insights page, CO₂ columns and charts in Usage Stats, and a gCO₂/kWh heatmap mode. A one-time backfill fills carbon data for your existing 2.x history.
- **🔁 Per-channel rate source** — each channel can take its unit rate from the API or a sensor independently (mix as needed).
- **🔧 Power-sensor invert & unit override** — fix sensors that read reversed or 1000× wrong (sign convention or a mis-declared W/kW unit) on the main, EV, heat pump or battery.
- **🔋 Ohme EV charger support** — detection-aware, preferring the charger's own session sensor. Ships conservatively, with an in-app feedback path.
- **Renamed for clarity** — "sub-meters" are now **Devices**; the **Live Power** page is now **Overview** and works even without a live-power sensor.
- **EV grid attribution fix (#212)** — when an EV and a battery charge at once and solar covers part of it, the EV now claims grid import first, so a cheap smart-charge no longer disappears behind a charging battery. Bill totals unchanged.

See the [full changelog](CHANGELOG.md) for the complete list.

---

## Upgrading from 2.x

**3.0 is a drop-in upgrade. Nothing you have changes, and nothing new is forced on you.**

- **Your data is preserved.** Blocks, configuration, billing periods and history all carry over. Schema migrations run automatically on first start.
- **Carbon backfill runs once, in the background.** UK postcode installs get a one-time pass that fills gCO₂/kWh for your existing 2.x blocks. It self-heals if interrupted by a restart — no action needed.
- **Your setup behaves exactly as before.** The data-source mode defaults to **`cad`** (local metering only), which is precisely how 2.x worked. You'll see the new terminology (Devices, Overview) and the new pages, but your recording and billing are unchanged.
- **The supplier features are opt-in.** When you're ready, open the **Setup Wizard** (or **Change Setup**) to add Octopus/Kraken DCC settlement, Home Mini live power, or Intelligent Octopus Go dispatch pricing. Switching modes is gated behind a confirmation because it can trigger a full recalculation.
- **Already had the Octopus API configured?** EMT infers your supplier from the existing credentials — no re-entry needed.
- **Restoring an old backup?** A v2-era backup with no supplier field restores cleanly as local-metering-only.

If anything looks off after upgrading, your previous data is untouched and you can review it in **Data Management** → backups.

---

## Requirements

- A smart meter with a Consumer Access Device (CAD) or integration providing cumulative kWh readings, updating at least every 60 seconds (10 seconds recommended)
- Cumulative kWh sensors for import and export
- Live rate sensors (£/kWh or local currency equivalent) for import and export tariffs
- Home Assistant OS, Supervised, or standalone Docker

See [Sensor Requirements and Known Limitations](https://github.com/RGx01/energy-meter-tracker-addon/wiki/Sensor-Requirements-and-Known-Limitations) for full details.

---

## Installation

### HA OS / Supervised (recommended)

[![Open your Home Assistant instance and show the add app repository dialog with a specific repository URL pre-filled.](https://my.home-assistant.io/badges/supervisor_add_addon_repository.svg)](https://my.home-assistant.io/redirect/supervisor_add_addon_repository/?repository_url=https%3A%2F%2Fgithub.com%2FRGx01%2Fenergy-meter-tracker-addon)

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

**Updating to a new version**

Standalone builds don't auto-update. To pull a new release, from your clone of the repo:

```bash
git pull
docker-compose up -d --build energy-meter-tracker
```

`--build` rebuilds the image from the updated source; `-d` keeps it detached. Your data (`/data/energy_meter_tracker`) is preserved across rebuilds as long as the volume mapping in your `docker-compose.yml` is unchanged.

> ⚠️ Ingress (sidebar embedding) is only available in HA OS/Supervised. In standalone mode access the UI directly at `http://<host>:8099`.

> ℹ️ Logs are written to `/data/energy_meter_tracker/addon.log` in standalone mode and are viewable from the **Logs** page in the UI.

**Optional — add to the HA sidebar**

You can add the standalone UI to your Home Assistant sidebar as a **Webpage dashboard** (via the UI):

1. Go to **Settings → Dashboards** (or [open dashboard settings](https://my.home-assistant.io/redirect/lovelace_dashboards/)).
2. Click **Add dashboard → Webpage**.
3. Set the **URL** to `http://<host>:8099` (your Docker host IP and port), give it a **Title** (e.g. "Energy Meter") and an **Icon** (e.g. `mdi:speedometer`), and choose whether to show it in the sidebar.

**Multiple properties / accounts**

One EMT instance tracks one property on one supplier account. To track a second — a rental, a second home, or a separate Octopus account — **run a second container**. Docker is the only install type where this works today.

Give each instance its own **data volume** and its own **host port**, and each authenticates with its own supplier credentials:

```yaml
  energy-meter-tracker-rental:
    build:
      context: ./energy-meter-tracker-addon
      dockerfile: Dockerfile.standalone
    container_name: energy-meter-tracker-rental
    restart: unless-stopped
    ports:
      - "8100:8099"            # different HOST port; container port stays 8099
    environment:
      - EMT_MODE=standalone
      - HA_URL=http://homeassistant:8123
      - LOG_LEVEL=info
      - HA_TOKEN=your_long_lived_access_token
    volumes:
      - ~/emt-data-rental:/data/energy_meter_tracker   # SEPARATE volume
```

The two instances are fully isolated: the database, configuration and backups all live inside each container's own volume. Give each a distinct **site name** in the Setup Wizard so you can tell their pages apart.

> ⚠️ **Do not share a data volume between instances.** Each expects exclusive use of its own database.

> ℹ️ **HA OS / Supervised cannot run a second instance.** Home Assistant permits a given add-on from a given repository to be installed only once, and we don't ship a second add-on for this. HAOS also has no user-accessible Docker daemon, so there is no fallback — multi-property support is Docker-only.

---

## Support

- 📖 [GitHub Wiki](https://github.com/RGx01/energy-meter-tracker-addon/wiki) — full documentation
- 🐛 [GitHub Issues](https://github.com/RGx01/energy-meter-tracker-addon/issues) — bug reports and feature requests
- 💬 [Community Forum](https://community.home-assistant.io/t/energy-meter-tracker/995674) — discussion and help

---

## Disclaimer

Energy Meter Tracker is for informational use only. It cannot replicate your supplier's authoritative Half-Hourly reconciliation. Do not use this data for billing disputes or formal energy accounting.