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

> **Known limitation — historical car/home split on Intelligent Octopus Go.**
>
> It helps to separate two things a half-hour block carries. The **total** is what that half-hour cost you; your bill is just these totals stacked up. On Intelligent Octopus Go that total is Octopus's own billed figure, and it already has the car/home (and, on the capped tariff, 4-rate) split *baked into it* — Octopus computes the split behind the scenes and hands EMT the finished number. **Your bill is therefore always correct**, whether the data was captured live or imported later.
>
> The **split** — how much of that total was the *car* versus the *home* — is something EMT shows by decomposing each block's total, and that decomposition needs Octopus's live smart-charge **dispatch** records (which slots the car charged, and how much). Octopus only exposes those in real time, so they can't be fetched for the past. That's the entire limitation: for a **backfilled or imported history** EMT has each block's correct total but not the dispatch behind it, so it can't pull the total back apart into car versus home — *like trying to un-bake a cake to get the eggs back out*. The split, and the capped cap-boundary rate breakdown, are only precise from the day EMT started capturing dispatch.
>
> If your EV was on a **physical sub-meter already recording in Home Assistant**, the historical car/home *energy* split can be rebuilt from HA's recorder (Device History) — a second recording to lean on; the cap-boundary *rate* split still can't be recovered. Nothing here affects your bill — only the car/home split and rate-band breakdown detail.

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
| [Historical Import](https://github.com/RGx01/energy-meter-tracker-addon/wiki/Historical-Import) | Backfill from API/CSV, fill gaps *(new in 4.0)* |
| [Device History](https://github.com/RGx01/energy-meter-tracker-addon/wiki/Device-History) | Reconstruct a device's past usage from HA recorder *(new in 4.0)* |
| [Sensor Requirements](https://github.com/RGx01/energy-meter-tracker-addon/wiki/Sensor-Requirements-and-Known-Limitations) | Sensor types, units, known limitations |
| [Carbon Intensity](https://github.com/RGx01/energy-meter-tracker-addon/wiki/Carbon-Intensity) | How carbon data is fetched and used |

---

## What's new in 4.3.0

EMT 4.3.0 — theme **Charge Cap** — is a drop-in upgrade: additive and **off for non-IOG tariffs**, with inc-VAT figures for every existing tariff byte-identical.

- **🚗 Intelligent Octopus Go house-vs-car split _(experimental)_.** Octopus's new IOG tariff gives your car up to **6 hours** of cheap charging a day (measured midday-to-midday) and puts anything beyond it on the peak rate. EMT now reconstructs the **house-vs-car split** for every IOG block from Octopus's own dispatch record — **no charger sensor needed**, it works for any charger or vehicle integration — and itemises it on the bill under *Import — total grid*, the way the statement lays it out. On a **migrated (capped)** meter it prices the full **4-rate** model, and the day chart's car rate line lifts away from the house line when a cap bites. The **same** split now drives **Usage Stats, Usage Insights and the charts**, so every screen shows the same house-vs-car figure as the bill. On an **uncapped** IOG meter it's shown for reference and your billing is unchanged. *(The cap model is a live estimate; settled DCC cost stays authoritative and any boundary estimate self-corrects at settlement.)*

See the [full changelog](CHANGELOG.md) for everything in 4.3.0.

---

## What's new in 4.0

EMT 4.0 is a **major-additive** release — like 3.0 it's a drop-in upgrade: nothing breaks, existing installs keep working, and the new tools are opt-in.

- **📥 Historical Import — backfill your full history.** Pull your past half-hourly data straight from Octopus (GraphQL Measurements API, a rolling ~2 years) or import a consumption **CSV** — kWh, exact billed cost, and the off-peak/standard label, so Intelligent Octopus smart charges price correctly rather than being guessed. Runs as a background job (**pause / resume / cancel**), rate-limit-polite so it won't starve live polling, with a self-healing pricing check afterwards. Fully reversible.
- **🩹 Fill History & Gaps.** When a settlement outage leaves holes, each gap offers a **pre-filled CSV to complete from your bill**, or an API fill — all through one guided flow. Delete and backup now run as background jobs with live progress.
- **🌍 Region-aware historical carbon.** The carbon backfill now extends across your imported history, resolving the correct DNO region **per billing period** (only the outward part of your postcode is ever stored), so Carbon/Insights and the heatmaps reach back years instead of stopping where carbon recording began. A house move is handled as a region boundary.
- **📊 Device History (recorder attribution).** Added a device — EV charger, battery, heat pump — to EMT *after* it was already recording in Home Assistant? Reconstruct its earlier usage from HA's recorder, split across the half-hours and clipped so it never exceeds what the house drew. One-click reversible.
- **🌀 Usage Stats HH view + reworked Spiral.** A new **HH** breakdown shows a single day as its 48 half-hour blocks; the **Spiral** now shows **Cost, Usage and Carbon side by side** with one shared grouping (Net / Import / Export / device) and a separate standing-charge spiral, each expandable.
- **🧹 Data Management reorganised** into clear sections, with a **Fill History & Gaps** landing page for the backfill/repair tools.
- **🛡️ Reliability** — a physical-plausibility guard stops impossible device spikes from a sensor glitch, plus fixes for post-outage settlement freezes, carbon-backfill retries, and the reconnect storms a large history could trigger.

See the **[Historical Import](https://github.com/RGx01/energy-meter-tracker-addon/wiki/Historical-Import)** and **[Device History](https://github.com/RGx01/energy-meter-tracker-addon/wiki/Device-History)** wiki guides for how to use the new tools, or the [full changelog](CHANGELOG.md) for everything in 4.0.

---

## Upgrading

**Every major version has been a drop-in upgrade — including 4.0. Nothing you have changes, and nothing new is forced on you.** The notes below cover the jump from 2.x (the biggest step); a 3.x → 4.0 upgrade is smaller still and needs nothing from you.

- **Your data is preserved.** Blocks, configuration, billing periods and history all carry over. Schema migrations run automatically on first start.
- **Carbon backfill runs once, in the background.** UK postcode installs get a one-time pass that fills gCO₂/kWh for your existing 2.x blocks. It self-heals if interrupted by a restart — no action needed.
- **Your setup behaves exactly as before.** The data-source mode defaults to **`cad`** (local metering only), which is precisely how 2.x worked. You'll see the new terminology (Devices, Overview) and the new pages, but your recording and billing are unchanged.
- **The supplier features are opt-in.** When you're ready, open the **Setup Wizard** (or **Change Setup**) to add Octopus/Kraken DCC settlement, Home Mini live power, or Intelligent Octopus Go dispatch pricing. Switching modes is gated behind a confirmation because it can trigger a full recalculation.
- **Already had the Octopus API configured?** EMT infers your supplier from the existing credentials — no re-entry needed.
- **Restoring an old backup?** A v2-era backup with no supplier field restores cleanly as local-metering-only.
- **Coming from 3.x?** 4.0 adds Historical Import, Device History and the reworked charts — all **opt-in**, changing nothing until you use them. Historical import and device attribution each take an automatic backup first and are one-click reversible, so you can try them without risk. (The only removal in 4.0 is an internal pre-2.0 JSON migration shim that hasn't been needed for years.)

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

> ℹ️ **On HA OS / Supervised**, a second instance isn't an officially supported path — Home Assistant installs a given add-on from a given repository only once, and we don't ship a second add-on. You can still run one as a self-maintained **local add-on**; see **Multiple instances on HA OS / Supervised** below.

### Multiple instances on HA OS / Supervised (advanced)

> ⚠️ **Advanced and unofficial.** Home Assistant is designed to install a given add-on once, so a second supervised instance is a workaround — treat it as at your own risk. If you'd rather not, the Docker approach above is the supported route.

Reassuringly, everything that must stay separate between instances is isolated automatically — nothing is shared by accident:

- **Database & charts** — each add-on has its own private `/data`, so `blocks.db` and the generated charts can never clash.
- **Ingress** — each add-on gets its own authenticated ingress route automatically.
- **Backups** — the `/share` backup directory is namespaced per instance (by site name and a persistent per-install id), so two instances never list or restore each other's backups.

The only things you set per instance are a **host port** and a **label**.

#### Option 1 — install a second copy from the store (no file access needed)

Home Assistant keys add-on repositories by the exact URL string, so adding this repository a **second time under a different-but-equivalent URL** makes HA treat it as a separate repository offering its own installable copy:

1. Settings → Add-ons → Add-on store → ⋮ → **Repositories**.
2. Add the repo again under a different-but-equivalent URL. Two confirmed variations:
   - `http://github.com/RGx01/energy-meter-tracker-addon` (`http://` instead of `https://`), or
   - `https://github.com/RGx01/energy-meter-tracker-addon.git` (append `.git`).
   HA stores either as a distinct repository, and GitHub resolves both to the same repo, so the clone works normally.
3. The store now lists **Energy Meter Tracker** twice. Install the second one — but **don't start it yet**.
4. Open the new instance → **Configuration → Network** and change its **host port** from 8099 to a free one such as **8100** (or clear it to use ingress only). Both copies declare host port 8099 in the shared manifest, so without this the second one can't start.
5. Start it. Set an **Instance name** in the Configuration tab so the EMT footer labels it, then run the **Setup Wizard** for the second property or account.

This copy **updates from the store** like any other add-on, and its icon works automatically (it's in the repo). The catch: both copies read the *same* `config.yaml`, so they share the add-on **name and icon** — in HA's add-on list they look identical, and you tell them apart by the footer label, the port, or the repository shown on each add-on's page.

#### Option 2 — a local add-on (if you want them visually distinct in HA)

Install the second copy as a **local add-on** if you want each instance to have its own **name, icon and sidebar entry** in Home Assistant, at the cost of a manual, self-maintained setup:

1. Get access to `/addons` via the [Samba](https://my.home-assistant.io/redirect/supervisor_addon/?addon=core_samba), [SSH / Terminal](https://my.home-assistant.io/redirect/supervisor_addon/?addon=core_ssh), or Studio Code Server add-on.
2. Copy the **entire** add-on into a new folder, e.g. `/addons/energy_meter_tracker_2/`, **including `icon.png` and `logo.png`** (the tile icon is read from this folder).
3. Edit that folder's `config.yaml`:
   ```yaml
   name: "Energy Meter Tracker (2)"    # add-on list + EMT footer label
   slug: "energy_meter_tracker_2"      # must be unique
   panel_title: "Energy Meter (2)"     # sidebar label
   ports:
     8099/tcp: 8100                     # a free HOST port (or null for ingress-only)
   ```
   Leave `ingress_port: 8099` unchanged — the internal port is the same for every instance and never collides (each container has its own network namespace).
4. Add-on store → ⋮ → **Check for updates** → it appears under **Local add-ons** → install, start, and run the **Setup Wizard**.

A local add-on doesn't auto-update: on a new release, copy the files in again (keeping your `config.yaml`) and **Rebuild** from the ⋮ menu — your `/data` is preserved.

> ⚠️ Each instance authenticates to your supplier independently. If two point at the same Octopus account, each opens its own API session — mind the supplier's rate limits if one polls heavily (for example via an Octopus Home Mini).

---

## Support

- 📖 [GitHub Wiki](https://github.com/RGx01/energy-meter-tracker-addon/wiki) — full documentation
- 🐛 [GitHub Issues](https://github.com/RGx01/energy-meter-tracker-addon/issues) — bug reports and feature requests
- 💬 [Community Forum](https://community.home-assistant.io/t/energy-meter-tracker/995674) — discussion and help

---

## Disclaimer

Energy Meter Tracker is for informational use only. It cannot replicate your supplier's authoritative Half-Hourly reconciliation. Do not use this data for billing disputes or formal energy accounting.