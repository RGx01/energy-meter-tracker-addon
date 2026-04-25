# Roadmap

## Released

### 2.7.0
- Live Power battery SoC card — battery icon with fill level (green/amber/red) and percentage
- Live Power inverter gauge — bidirectional semicircle (teal = discharging, purple = charging)
- Live Power EV charger card — unidirectional charge gauge, V2X aware
- Live Power heat pump card — unidirectional power gauge
- Live Power status pill — replaces import/export rows with Importing/Exporting/Net Zero
- Meter Type field — Battery / EV Charger / Heat Pump in sub-meter config
- SoC sensor, inverter power sensor, charger power sensor, heat pump power sensor fields
- `sub_meter_history` table — 48hr rolling history for gauge scaling, pruned each tick
- `meter_type` column — persisted in `meters` table, takes priority over keyword detection
- Cascade delete on meter removal — confirmation + permanent data wipe
- Device name validation — unique, 40 chars, safe characters, client + server
- Change reason modal — only shown on billing-significant changes
- Meter IDs hidden from UI and wizard — auto-generated, not user-facing
- `inverter_possible` auto-set from meter_type = battery
- Parent meter auto-assigned — not shown in UI
- Keyword detection retained as fallback for existing meters without meter_type set

### 2.6.3
- Usage Stats data table scrollable on desktop, sticky headers, sortable date column, zebra shading, colour dots
- Billing chart day order toggle
- Heatmap fills available viewport, larger mobile metric buttons
- CI fetch moved before ensure_correct_block

### 2.6.2
- Theme toggle consolidated to logo, logo size increase
- Insights mobile topbar and metric label improvements
- GitHub wiki link in Help

### 2.6.1
- Logo theme toggle
- Sub-meter weighted average rate bug, session gap detection block_minutes fix
- Upgrade wizard and startup crash fixes

### 2.6.0
- Insights page with carbon analysis and Carbon assumptions tab
- Settings page restructure

### 2.5.x
- Power history drag zoom and touch zoom
- Storage monitoring card on Data Management
- Mobile topbar collapse

### 2.4.x
- Carbon footprint in Usage Stats
- DB restore hardening (WAL checkpoint, engine pause, no-restart restore)
- Upgrade safety backup on version change

### 2.3.0
- Carbon intensity recording (National Grid API, UK only)
- 48-hour power history chart with kW / CO₂ toggle and hover tooltip
- `carbon_g` on blocks

### 2.2.x
- Delete Blocks sub-page
- Historical Corrections promoted to own page
- Compact Database
- Lovelace-friendly chart endpoints

### 2.1.x
- Fully relational SQLite schema — `meters`, `meter_channels`, `current_block`, `current_reads` tables
- `meters_config.json` demoted to convenience export only
- Config history (Billing History page)
- Gap fill hardening — multiple fixes across 2.1.5–2.1.9

### 2.0.0
- SQLite database replacing blocks.json
- Config history / Billing History
- Billing period transition logic
- Fast SQL billing aggregation

### 1.6.x
- Usage Stats chart — daily, monthly, yearly with sub-meter breakdown
- Data table with copy-to-clipboard export
- Light/dark theme toggle
- Summary page renamed to Live Power

### 1.5.1
- Live Power page — gauge, billing cards, carbon intensity forecast
- Power sensor and postcode prefix fields in Meter Config

### 1.4.0
- Configurable meter reconciliation period (5, 15 or 30 minutes)
- Automatic currency detection

### 1.2.0
- Guided Setup Wizard

### 1.1.0
- Flask-based web UI

### 1.0.0
- Initial release — core metering engine, sub-meter support, gap filling, billing charts, HA sensor publishing

---

## Planned

### 2.7.1
- **CI gap backfill** — scan blocks with NULL carbon_g and backfill from stored carbon_intensity data; fetch historical CI from National Grid ESO range endpoint for any gaps. Was planned for 2.6.4 but slipped.
- **Test suite updates** — engine and server tests for 2.7.0 changes (sub_meter_history write, meter_type detection, cascade delete endpoint, _build_soc_response)
- **Toolbar unification** — billing chart, heatmap and Insights toolbars refactored to match the Usage Stats pattern (segmented tabs in topbar, period nav in floating sub-bar, consistent mobile collapse)

### 2.8.0 — Usage Insights Tab

The hidden Usage tab on the Insights page becomes active. Placeholder tab already present in `insights.html`.

**Cost breakdown**
- Total spend for the billing period split by meter (house remainder, EV, battery, heat pump)
- Standing charge as £ and % of total bill
- Effective rate (total spend ÷ total import kWh)
- Comparison to previous period

**Usage patterns**
- Peak consumption window (which 3-hour block has the highest average import)
- Highest consumption day in the period
- Average daily import kWh
- Self-sufficiency ratio (export ÷ (import + export)) where export data exists
- Comparison to previous period for all metrics

**Tariff efficiency** *(if rate sensor history is reliable)*
- % of EV charging kWh recorded during cheap-rate blocks
- % of battery charging kWh recorded during cheap-rate blocks
- Estimated saving vs charging entirely at peak rate

**Open questions to resolve before build**
- Does cost breakdown duplicate the Charts billing view enough to be redundant, or does the period-over-period comparison add sufficient value?
- Tariff efficiency requires identifying which rate is "cheap" — is this derivable from the rate sensor history alone (e.g. lowest N% of recorded rates), or does it need explicit tariff band configuration?
- Should the Usage tab require carbon data, or is it fully independent of the CI pipeline?

### 3.0.0
- Remove `migrate_json_to_sqlite()` — blocks.json migration path deprecated since 2.2.3
- Remove all 1.x.x / 2.0.x upgrade shims
- API versioning

---

## Open Questions

- **CI backfill staleness threshold** — how old does a block's CI data need to be before backfilling is worthwhile? Needs defining before 2.7.1 build.
- **Usage Insights scope** — does cost breakdown duplicate Charts billing view? Does tariff efficiency require explicit tariff configuration or is it derivable from rate sensor history? To be resolved before 2.8.0 build begins.

## Decisions Made

- **Meter type locking** — `meter_type` is locked for life once set. Users who set the wrong type must delete and recreate the meter (cascade delete handles data cleanup cleanly).
- **Multiple battery gauge scaling** — each battery has its own inverter, so each card fetches its own 48hr history independently. Scale is self-sorting per card. `INV_MAX_KW` variable to be renamed per-meter for clarity in a future cleanup.