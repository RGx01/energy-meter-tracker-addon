# Development Guide

## Architecture Overview

Energy Meter Tracker is a Home Assistant add-on built around a Python asyncio engine. The key components are:

```
main.py              — Entry point. Wires together HAClient, engine and Flask server.
engine.py            — Core half-hour block engine. All metering logic lives here.
ha_client.py         — WebSocket + REST client. Replaces PyScript primitives.
energy_engine_io.py  — Atomic file I/O helpers.
energy_charts.py     — Chart generation (billing periods, billing history, heatmap).
block_store.py       — SQLite persistence layer. All block and config period storage.
web/server.py        — Flask web UI and API endpoints.
web/templates/       — Jinja2 HTML templates.
```

### Runtime modes

| Mode | Detection | HA connection |
|------|-----------|---------------|
| Supervised | `SUPERVISOR_TOKEN` env var present | `ws://supervisor/core/websocket` |
| Standalone Docker | No `SUPERVISOR_TOKEN` | `ws://<HA_URL>/api/websocket` |

`run.sh` detects the mode and sets `EMT_MODE` before starting Python.

---

## Data Storage — SQLite

Since 2.0.0, all blocks are stored in a SQLite database (`energy_meter.db`). The schema has two main tables:

### `config_periods`

Tracks billing configuration history. Every billing-significant change creates a new row.

| Column | Type | Description |
|--------|------|-------------|
| `id` | INTEGER PK | Auto-increment |
| `effective_from` | TEXT | UTC ISO datetime — when this config became active |
| `effective_to` | TEXT | UTC ISO datetime — when the next config took over (NULL = current) |
| `billing_day` | INTEGER | Day of month billing period starts (1–28) |
| `block_minutes` | INTEGER | Reconciliation period (5, 15 or 30) |
| `timezone` | TEXT | IANA timezone name |
| `currency_symbol` | TEXT | e.g. `£` |
| `currency_code` | TEXT | e.g. `GBP` |
| `site_name` | TEXT | Display name |
| `change_reason` | TEXT | Freetext note |
| `full_config_json` | TEXT | Full `meters_config.json` snapshot at save time |

`effective_from` is always snapped to **midnight in the configured timezone** (converted to UTC) when a config is saved.

### `blocks`

One row per meter per reconciliation period.

| Column | Type | Description |
|--------|------|-------------|
| `block_start` | TEXT | UTC ISO datetime |
| `block_end` | TEXT | UTC ISO datetime |
| `local_date` | TEXT | Local calendar date (YYYY-MM-DD) — pre-computed at insert |
| `local_year/month/day` | INTEGER | Derived from local_date |
| `meter_id` | TEXT | e.g. `electricity_main` |
| `config_period_id` | INTEGER FK | Which config was active when this block was recorded |
| `imp_kwh` | REAL | Grid import kWh |
| `imp_cost` | REAL | Import cost |
| `exp_kwh` | REAL | Export kWh |
| `exp_cost` | REAL | Export value |
| `standing_charge` | REAL | Daily standing charge (same value on all blocks for a given day) |

Key indexes: `block_start`, `local_date`, `config_period_id`.

### Standing charge

Standing charge is a daily charge stored on **every block** (same value repeated). When summing standing charges for a range, always aggregate once per `local_date` — not once per block:

```sql
SELECT SUM(daily_sc) FROM (
    SELECT MIN(standing_charge) as daily_sc
    FROM blocks
    WHERE block_start >= ? AND block_start < ?
    GROUP BY local_date
)
```

`local_date` is pre-computed at block insert time using the configured timezone, so BST/GMT transitions are handled correctly (a block at `23:00 UTC` in BST correctly gets `local_date = next day`).

---

## Billing Period Logic

### Config period chain

Config periods form a contiguous chain: `period_A.effective_to = period_B.effective_from`. The last period has `effective_to = NULL`. `_rebuild_config_period_chain()` in `server.py` sorts all periods by `effective_from` and rebuilds this chain after any insert, edit or delete. It also reassigns all blocks to the correct period based on `block_start` ranges.

### Billing period transitions

When the billing day changes, the old config's last billing period is **truncated** at the transition date. Bills can only be truncated, never extended.

**Transition date rule** (given `effective_from` date and new `billing_day`):
- If `effective_from.day < new_bd`: transition = `new_bd` of `effective_from.month`
- If `effective_from.day >= new_bd`: transition = `new_bd` of `effective_from.month + 1`

Example: `effective_from = 4 Apr`, `new_bd = 15`:
- Old config: `Mar 3 → Apr 3` (complete), `Apr 3 → Apr 15` (truncated)
- New config: `Apr 15 → May 15`, `May 15 → Jun 15`, ...

### `get_billing_periods_from_config_history(blocks, tz)`

Takes the full block list (with `_effective_from` on each block). Used by the chart generators which already have blocks loaded. Segments blocks by `_effective_from`, computes transitions, generates the period list.

### `get_billing_periods_from_config_periods(config_periods, tz)`

Fast alternative that takes `store.get_config_periods()` rows instead of all blocks. Used by the Live Power page and `api/billing` to avoid loading all blocks. Produces identical output.

---

## Block Lifecycle

The engine runs on a 10-second tick loop. Each tick:

1. **Drain read queue** — sensor state change callbacks push timestamps onto `_read_queue`. The tick drains all queued reads and calls `capture_samples()` for each.
2. **Periodic checkpoint** — if 60 seconds have elapsed since last checkpoint, capture a sample regardless of sensor updates.
3. **Near-boundary capture** — within 15 seconds of a block boundary, capture on every tick.
4. **Gap fill** — if a `_gap_marker` is present on the current block, attempt to fill missing blocks using interpolation.
5. **Block rollover** — if the current time has passed the block's end boundary and a post-boundary read is available, `finalise_block()` is called.

### Block finalisation

Finalisation runs four passes:

- **PASS 1** — compute kWh and cost for all meters using boundary-interpolated opening and closing reads
- **PASS 2** — grid-authoritative sub-meter distribution
- **PASS 3** — compute block totals
- **PASS 4** — update cumulative totals and push to HA sensors

After finalisation: charts are regenerated, data files are backed up to `/share/`.

### Interpolation

`interpolate_value(pre_read, post_read, target_dt)` performs linear interpolation between two timestamped meter readings. Fraction is clamped to [0, 1].

### Gap detection and filling

If the engine restarts after an outage, `detect_gap()` counts missing windows between the last known block end and now. `build_gap_blocks()` interpolates all missing windows. Gaps longer than 12 hours produce zero blocks.

---

## File Structure

```
/data/energy_meter_tracker/
    blocks.db                      — SQLite database (all state since 2.1.0)
    meters_config.json             — convenience export only (not authoritative)
    .last_startup_backup_version   — tracks last version that created an upgrade backup

/share/energy_meter_tracker_backup/
    blocks.db                      — copied after every block finalise
    meters_config.json             — copied after every block finalise
    backups/
        YYYYMMDDTHHMMSS_label.zip  — zip snapshots (20 max, pruned automatically)
        YYYYMMDDTHHMMSS_upgrade_x.x.x.zip — created once on first startup per version
```

`blocks.db` is the single source of truth for all state including meter config, config periods, blocks, current in-progress block state, carbon intensity, and power history. `meters_config.json` is written for human readability but never read back as authoritative state.

---

## Running Unit Tests

Tests use Python's built-in `unittest` — no external dependencies needed.

```bash
cd /addons/energy_meter_tracker
python3 -m unittest discover -v   # ~330 tests across all files
```
# Development Guide

## Architecture Overview

Energy Meter Tracker is a Home Assistant add-on built around a Python asyncio engine. The key components are:

```
main.py              — Entry point. Wires together HAClient, engine and Flask server.
engine.py            — Core half-hour block engine. All metering logic lives here.
ha_client.py         — WebSocket + REST client. Replaces PyScript primitives.
energy_engine_io.py  — Atomic file I/O helpers.
energy_charts.py     — Chart generation (billing periods, billing history, heatmap).
block_store.py       — SQLite persistence layer. All block and config period storage.
web/server.py        — Flask web UI and API endpoints.
web/templates/       — Jinja2 HTML templates.
```

### Runtime modes

| Mode | Detection | HA connection |
|------|-----------|---------------|
| Supervised | `SUPERVISOR_TOKEN` env var present | `ws://supervisor/core/websocket` |
| Standalone Docker | No `SUPERVISOR_TOKEN` | `ws://<HA_URL>/api/websocket` |

`run.sh` detects the mode and sets `EMT_MODE` before starting Python.

---

## Data Storage — SQLite

Since 2.0.0, all state is stored in `blocks.db`. This is the **single source of truth** — no other file takes precedence when `blocks.db` exists. `meters_config.json` is written as a human-readable export after each config save but is never read back if the DB is present.

### `config_periods`

Tracks billing configuration history. Every billing-significant change creates a new row.

| Column | Type | Description |
|--------|------|-------------|
| `id` | INTEGER PK | Auto-increment |
| `effective_from` | TEXT | UTC ISO datetime — when this config became active |
| `effective_to` | TEXT | UTC ISO datetime — when the next config took over (NULL = current) |
| `billing_day` | INTEGER | Day of month billing period starts (1–28) |
| `block_minutes` | INTEGER | Reconciliation period (5, 15 or 30) |
| `timezone` | TEXT | IANA timezone name |
| `currency_symbol` | TEXT | e.g. `£` |
| `currency_code` | TEXT | e.g. `GBP` |
| `site_name` | TEXT | Display name |
| `change_reason` | TEXT | Freetext note |
| `supplier` | TEXT | Optional supplier name |

`effective_from` is always snapped to **midnight in the configured timezone** (converted to UTC) when a config is saved.

### `blocks`

One row per meter per reconciliation period.

| Column | Type | Description |
|--------|------|-------------|
| `block_start` | TEXT | UTC ISO datetime |
| `block_end` | TEXT | UTC ISO datetime |
| `local_date` | TEXT | Local calendar date (YYYY-MM-DD) — pre-computed at insert |
| `local_year/month/day` | INTEGER | Derived from local_date |
| `meter_id` | TEXT | e.g. `electricity_main` |
| `config_period_id` | INTEGER FK | Which config was active when this block was recorded |
| `imp_kwh` | REAL | Grid import kWh |
| `imp_cost` | REAL | Import cost |
| `exp_kwh` | REAL | Export kWh |
| `exp_cost` | REAL | Export value |
| `standing_charge` | REAL | Daily standing charge (same value on all blocks for a given day) |
| `carbon_g` | REAL | Net gCO₂ for this block (NULL if no postcode or pre-2.3.0) |

Key indexes: `block_start`, `local_date`, `config_period_id`, `meter_id`.

### `store_meta`

Simple key-value table for application state.

| Key | Value | Description |
|-----|-------|-------------|
| `schema_version` | `"1"` | DB schema version |
| `settings` | JSON blob | User-configurable carbon assumptions (see Settings) |

### `meters` and `meter_channels`

Normalised meter definitions joined to each config period. Replaces the embedded JSON blobs from earlier versions.

### Standing charge

Standing charge is stored on **every block** (same value repeated). When summing for a range, always aggregate once per `local_date`:

```sql
SELECT SUM(daily_sc) FROM (
    SELECT MIN(standing_charge) as daily_sc
    FROM blocks
    WHERE block_start >= ? AND block_start < ?
    GROUP BY local_date
)
```

---

## load_config() — Single Source of Truth Rule

Both `server.py` and `engine.py` have a `load_config()` function. The rule is:

**If `blocks.db` exists → always read from the DB. Never fall back to `meters_config.json`.**

`meters_config.json` is only read when there is no `blocks.db` at all (true fresh install). This ensures that swapping a `blocks.db` between environments always uses that DB's own config — correct site name, block_minutes, timezone, everything self-contained.

```python
db_exists = os.path.exists(os.path.join(DATA_DIR, "blocks.db"))

try:
    cp = store._conn.execute(
        "SELECT id FROM config_periods WHERE effective_to IS NULL ..."
    ).fetchone()
    if cp:
        return store.config_from_db(cp["id"])
    if db_exists:
        return {"schema_version": "1.0", "meters": {}}  # DB exists, no periods yet
except Exception as e:
    if db_exists:
        logger.error("load_config failed: %s", e)
        return {"schema_version": "1.0", "meters": {}}  # DB exists, don't use JSON

# True fresh install only
return load_from_json()
```

---

## Billing Period Logic

### Config period chain

Config periods form a contiguous chain: `period_A.effective_to = period_B.effective_from`. The last period has `effective_to = NULL`. `_rebuild_config_period_chain()` in `server.py` sorts all periods by `effective_from` and rebuilds this chain after any insert, edit or delete. It also reassigns all blocks to the correct period based on `block_start` ranges.

### Billing period transitions

When the billing day changes, the old config's last billing period is **truncated** at the transition date. Bills can only be truncated, never extended.

**Transition date rule** (given `effective_from` date and new `billing_day`):
- If `effective_from.day < new_bd`: transition = `new_bd` of `effective_from.month`
- If `effective_from.day >= new_bd`: transition = `new_bd` of `effective_from.month + 1`

### `get_billing_periods_from_config_periods(config_periods, tz)`

Fast alternative to `get_billing_periods_from_config_history` — takes `store.get_config_periods()` rows instead of all blocks. Used by Insights, Live Power page, and `api/billing`. Produces identical output.

---

## Block Lifecycle

The engine runs on a 10-second tick loop. Each tick:

1. **Drain read queue** — sensor state change callbacks push timestamps onto `_read_queue`. The tick drains all queued reads and calls `capture_samples()` for each.
2. **Periodic checkpoint** — if 60 seconds have elapsed since last checkpoint, capture a sample regardless of sensor updates.
3. **Near-boundary capture** — within 15 seconds of a block boundary, capture on every tick.
4. **Gap fill** — if a `_gap_marker` is present on the current block, attempt to fill missing blocks using interpolation.
5. **Block rollover** — if the current time has passed the block's end boundary and a post-boundary read is available, `finalise_block()` is called.

### Block finalisation

Finalisation runs four passes:

- **PASS 1** — compute kWh and cost for all meters using boundary-interpolated opening and closing reads
- **PASS 2** — grid-authoritative sub-meter distribution
- **PASS 3** — compute block totals
- **PASS 4** — update cumulative totals and push to HA sensors

Carbon intensity (`carbon_g`) is applied after PASS 1 using the nearest `carbon_intensity` row for the configured postcode. Both main meter and sub-meters get `carbon_g` — main meter uses `(imp_kwh_total - exp_kwh) × intensity`, sub-meters use `imp_kwh × intensity`.

---

## Carbon Intensity

Carbon data comes from the National Grid ESO regional API (UK only, requires postcode prefix).

- Fetched every 30 minutes, stored in `carbon_intensity` table (4-day retention)
- Applied at block finalisation: `carbon_g = net_kwh × intensity_g_per_kwh`
- Grid average intensity derivable from `carbon_g / net_kwh` per block — this is the true grid intensity at that moment, independent of consumption level
- `blocks.db` contains the full intensity history since 2.3.0 for any block with non-zero net flow

---

## Insights Data Layer

### `/api/insights/periods`

Returns all billing periods with a quick carbon summary per period (SQL aggregate, no block loading).

### `/api/insights/billing-period?period_start=YYYY-MM-DD`

Full carbon breakdown for one billing period. Key computed fields:

- `carbon_g_imp` / `carbon_g_exp` — derived from `carbon_g / net_kwh × imp_kwh` per block
- `effective_intensity` — consumption-weighted avg gCO₂/kWh (what you experienced)
- `grid_avg_intensity` — time-weighted avg gCO₂/kWh (what the grid was doing)
- `house_imp_kwh` / `house_carbon_g` — main import minus all sub-meter imports
- Per sub-meter: `avg_charge_intensity` — weighted avg `carbon_g / imp_kwh` across all blocks with CI data

### Meter type detection

Sub-meter type is resolved in order:
1. `meta.meter_type` key in config (explicit)
2. Meter ID keyword fallback: `ev`/`charger` → `ev_charger`, `battery`/`batt` → `battery`, `heat`/`pump` → `heat_pump`, `solar`/`pv`/`inv` → `inverter`

---

## Settings

User-configurable carbon assumptions are stored as a JSON blob in `store_meta` under the key `settings`. Missing keys fall back to `SETTINGS_DEFAULTS` in `server.py` at read time — no migration needed when new defaults are added.

Current defaults with citations:

| Key | Default | Source |
|-----|---------|--------|
| `co2_car_petrol_g_per_mile` | 180.0 | BEIS/DESNZ 2023 |
| `co2_car_diesel_g_per_mile` | 168.0 | BEIS/DESNZ 2023 |
| `co2_tree_kg_per_year` | 21.0 | Woodland Trust |
| `co2_flight_lhr_nyc_kg` | 670.0 | BEIS 2023 |
| `distance_unit` | `"miles"` | — |
| `co2_export_method` | `"grid_average"` | — |
| `co2_export_custom_intensity` | 200.0 | — |
| `ev_efficiency` | 3.2 | UK average |
| `ev_charge_efficiency` | 0.88 | IEA 2022, Type 2 AC |
| `battery_round_trip_efficiency` | 0.90 | Li-ion home battery |
| `hp_cop` | 3.0 | Typical air-source SCOP |
| `gas_co2_g_per_kwh` | 203.0 | BEIS/DESNZ 2023 |
| `gas_boiler_efficiency` | 0.90 | Modern condensing |

---

## Navigation & Templates

### Routes (as of 2.6.0)

| Route | Function | Template |
|-------|----------|----------|
| `/settings` | `settings_page()` | `meter_config.html` (default) or `settings.html` (`?tab=carbon`) |
| `/settings?tab=carbon` | `settings_page()` | `settings.html` |
| `/charts` | `charts_page()` | `charts.html` |
| `/insights` | `insights_page()` | `insights.html` |
| `/live-power` | `live_power_page()` | `live_power.html` |
| `/data-management` | `data_management_page()` | `data_management.html` |
| `/billing-history` | `billing_history_page()` | `billing_history.html` |
| `/logs` | `logs_page()` | `logs.html` |
| `/help` | `help_page()` | `help.html` |

### Sidebar active flags

| Page | `active` value |
|------|---------------|
| Settings (both tabs) | `"settings"` |
| Charts | `"charts"` |
| Insights | `"insights"` |
| Live Power | `"live_power"` |
| Data Management | `"data_management"` |
| Logs | `"logs"` |
| Help | `"help"` |

### `base.html` blocks

| Block | Purpose |
|-------|---------|
| `topbar` | Page title, tab buttons, primary action button |
| `subbar` | Secondary actions (Wizard, Refresh, Billing History) — Settings/Meter Config only |
| `content` | Main page body |

---

## File Structure

```
/data/energy_meter_tracker/
    blocks.db                — SQLite database (single source of truth)
    meters_config.json       — Human-readable export only, never read if blocks.db exists

/share/energy_meter_tracker_backup/
    blocks.db                — copied after every finalise
    meters_config.json       — copied after every finalise
    backups/
        YYYYMMDDTHHMMSS_label.zip   — zip snapshots (20 max)

web/templates/
    base.html                — sidebar, topbar/subbar blocks, theme
    meter_config.html        — Settings > Meter Config tab
    settings.html            — Settings > Carbon tab
    insights.html            — Insights page (all carbon cards)
    charts.html              — Charts page
    live_power.html          — Live Power page
    data_management.html     — Data Management page
    billing_history.html     — Billing History page
    delete_blocks.html       — Delete Blocks sub-page
    corrections.html         — Historical Corrections sub-page
    logs.html                — Logs page
    help.html                — Help page
```

---

## Running Unit Tests

Tests use Python's built-in `unittest` — no external dependencies needed.

```bash
cd /addons/energy_meter_tracker
python3 -m unittest test_engine -v       # engine tests
python3 -m unittest test_block_store -v  # block store + billing tests
python3 -m unittest test_server -v       # server/API tests (359 tests as of 2.6.0)
```

Or run all at once:
```bash
python3 -m unittest discover -v
```

### Test coverage (2.6.0)

- `test_engine.py` — `floor_to_hh`, `interpolate_value`, `detect_gap`, `compute_channel`, `select_opening_read`, `select_closing_read`, gap marker helpers, `build_gap_blocks`, `extract_last_reads`
- `test_block_store.py` — SQLite schema, block insertion, `get_blocks_for_range`, config period CRUD, `delete_config_period` (block reassignment), `get_billing_totals_for_range`, settings get/save
- `test_server.py` — all API endpoints, billing accuracy, config history CRUD, insights periods, insights billing-period, settings GET/POST, all page routes including renamed routes

When adding new logic, add corresponding tests. All test files use module stubs so they run without HA, Flask or filesystem access.

---

## Local Development

### Supervised (HA OS)

The add-on is loaded from `/addons/energy_meter_tracker/`. Changes to Python files require a rebuild via the HA add-on UI. Template changes (`web/templates/`) also require a rebuild.

After rebuilding, if `config.yaml` changed run:
```bash
ha supervisor restart
```

### Standalone Docker

```bash
docker build -t emt-dev .
docker run -d \
  --name emt-dev \
  -p 8099:8099 \
  -e EMT_MODE=standalone \
  -e HA_URL=http://192.168.1.10:8123 \
  -e HA_TOKEN=your_token \
  -e LOG_LEVEL=debug \
  -v /tmp/emt-data:/data/energy_meter_tracker \
  emt-dev
docker logs -f emt-dev
```

### Swapping databases between environments

Because `blocks.db` is the single source of truth, you can copy it directly between environments:

```bash
# Copy prod DB to dev
scp ha:/data/energy_meter_tracker/blocks.db /tmp/emt-data/blocks.db
```

The dev instance will use the prod DB's own config periods (site name, block_minutes, timezone etc) correctly. No need to copy `meters_config.json` — it is not authoritative.

---

## Key Design Decisions

**Why SQLite?**
The flat `blocks.json` file became a bottleneck as history grew — loading a year of 5-minute blocks (100k+ rows) into Python memory on every billing calculation was slow. SQLite provides indexed queries, SQL aggregation (SUM, GROUP BY), and atomic writes without requiring a separate database server.

**Why is blocks.db the single source of truth?**
`meters_config.json` was originally the config file but became a derived export once config periods were introduced in 2.0.0. Having two authoritative sources caused subtle bugs when copying a DB between environments — the JSON from the old environment would override the DB's config. Since 2.6.0, `load_config()` never reads the JSON when the DB exists.

**Why asyncio?**
The engine needs to handle WebSocket events, a 10-second tick loop, and Flask serving concurrently. Flask runs in a background thread via `threading.Thread`; everything else runs in the main asyncio event loop.

**Why is the main meter authoritative?**
Sub-meter sensors (CT clamps, device integrations) are less accurate than the DCC smart meter CAD feed. Treating the main meter as authoritative and distributing its reading across sub-meters ensures billing totals are always grounded in the actual grid reading.

**Why interpolation at boundaries?**
Without it, a sensor update that arrives at 09:28 would be assigned entirely to one block. Interpolation splits the delta proportionally so each block gets the fraction that actually occurred within it.

**Why `local_date` pre-computed at insert?**
SQLite's `DATE()` function operates in UTC. Standing charge must be summed once per local calendar day. Pre-computing `local_date` at insert time (using the configured timezone) avoids needing to pass timezone offsets into every query and handles BST/GMT transitions correctly.

**Why truncation-only billing transitions?**
Allowing periods to be extended would make it possible to create billing periods longer than one month. Truncation-only means the user always gets a partial final period under the old config, and a clean start under the new config — matching real billing behaviour.

**Why store carbon_g per block rather than deriving it?**
The grid intensity at the time of a block is only available for 4 days from the National Grid ESO API. Storing `carbon_g = intensity × net_kwh` at finalisation time preserves the full intensity history indefinitely. The raw intensity can be recovered as `carbon_g / net_kwh` for any block with non-zero net flow.

**Why is export carbon offset calculated at grid average?**
Marginal intensity (what the next generator to switch off would have produced) is not publicly available in real time. Grid average is conservative, standard, and used by the Carbon Trust. Users can override to a fixed custom intensity in Settings if they have a specific figure.

---

## Known Limitations & Future Work

- **Solar generation** — not supported as a sub-meter type. Export sub-metering requires design work around the export channel. Cannot split export between solar and battery discharge without additional metering.
- **Gas meters** — not designed. Would require a separate meter type with different unit handling (m³/ft³ with calorific value conversion).
- **Multiple batteries/inverters** — only one inverter-possible sub-meter per parent is well tested.
- **V2G export** — V2X-capable EV export to grid is flagged but not broken down by sub-meter.
- **Carbon insights comparisons** — month-on-month and year-on-year comparisons planned for 2.7.0.
- **Usage Insights** — non-carbon insights (peak times, highest consumption days) planned for a future release.
- **Carbon intensity outside UK** — National Grid ESO API is UK-only. International carbon data sources are not yet integrated.
Or run individual suites:
```bash
python3 -m unittest test_engine -v
python3 -m unittest test_block_store -v
python3 -m unittest test_server -v
python3 -m unittest test_usage_stats_vs_billing -v
```

### Test coverage

- `test_engine.py` — block engine logic: `floor_to_hh`, `interpolate_value`, `detect_gap`, `compute_channel`, `select_opening_read`, `select_closing_read`, gap marker helpers, `build_gap_blocks`, `extract_last_reads`
- `test_block_store.py` — SQLite schema, block insertion, config period CRUD, billing totals, carbon intensity, power history
- `test_server.py` — all API endpoints, billing accuracy, config history CRUD, carbon API, power history API, blocks.db import regression
- `test_usage_stats_vs_billing.py` — cross-checks that Usage Stats aggregations match direct DB queries: kWh, cost, carbon, sub-meter accounting, carbon identity (`carbon_g_imp - carbon_g_exp = carbon_g_net`), avg intensity

All test files use in-memory SQLite (`:memory:`) and module stubs so they run without HA, Flask or filesystem access. No test modifies any real data file.

### Visual / UI tests (test harness)

The test harness generates realistic synthetic data and uploads it to a running add-on instance for visual verification:

```bash
cd /addons/energy_meter_tracker
pip install requests
python3 test_harness.py --url http://192.168.x.x:8099
```

The harness cycles through 15 scenarios (various block sizes, scenarios, sub-meter configs and CO₂ scenarios) and opens the Charts page for manual pass/fail judgement. Results are saved to `test_results.json`.

> ⚠️ The harness **replaces all data** on the target instance. Always back up first and only run against a development instance.

---

## Local Development

### Supervised (HA OS)

The add-on is loaded from `/addons/energy_meter_tracker/`. Changes to Python files require a rebuild via the HA add-on UI. Template changes (`web/templates/`) also require a rebuild.

After rebuilding, if `config.yaml` changed run:
```bash
ha supervisor restart
```

### Standalone Docker

```bash
docker build -t emt-dev .
docker run -d \
  --name emt-dev \
  -p 8099:8099 \
  -e EMT_MODE=standalone \
  -e HA_URL=http://192.168.1.10:8123 \
  -e HA_TOKEN=your_token \
  -e LOG_LEVEL=debug \
  -v /tmp/emt-data:/data/energy_meter_tracker \
  emt-dev
docker logs -f emt-dev
```

---

## Key Design Decisions

**Why SQLite?**
The flat `blocks.json` file became a bottleneck as history grew — loading a year of 5-minute blocks (100k+ rows) into Python memory on every billing calculation was slow. SQLite provides indexed queries, SQL aggregation (SUM, GROUP BY), and atomic writes without requiring a separate database server.

**Why asyncio?**
The engine needs to handle WebSocket events, a 10-second tick loop, and Flask serving concurrently. Flask runs in a background thread via `threading.Thread`; everything else runs in the main asyncio event loop.

**Why not HACS?**
HACS is for integrations, Lovelace cards and themes — not add-ons. Add-ons run as Docker containers alongside HA and are distributed via add-on repositories.

**Why is the main meter authoritative?**
Sub-meter sensors (CT clamps, device integrations) are less accurate than the DCC smart meter CAD feed. Treating the main meter as authoritative and distributing its reading across sub-meters ensures billing totals are always grounded in the actual grid reading.

**Why interpolation at boundaries?**
Without it, a sensor update that arrives at 09:28 would be assigned entirely to one block. Interpolation splits the delta proportionally so each block gets the fraction that actually occurred within it.

**Why `local_date` pre-computed at insert?**
SQLite's `DATE()` function operates in UTC. Standing charge must be summed once per local calendar day. Pre-computing `local_date` at insert time (using the configured timezone) avoids needing to pass timezone offsets into every query and handles BST/GMT transitions correctly.

**Why truncation-only billing transitions?**
Allowing periods to be extended (e.g. moving the transition date later) would make it possible to create billing periods longer than one month, which doesn't match how suppliers work. Truncation-only means the user always gets a partial final period under the old config, and a clean start under the new config — matching real billing behaviour.

---

## Known Limitations & Future Work

- **Solar generation** — not supported as a sub-meter type. Export sub-metering requires design work around the export channel.
- **Gas meters** — not designed. Would require a separate meter type with different unit handling.
- **Multiple batteries/inverters** — only one inverter-possible sub-meter per parent is well tested.
- **V2G export** — V2X-capable EV export to grid is flagged but not broken down by sub-meter.
- **Ingress** — currently supported via a WSGI middleware. Full Ingress with sidebar toggle works.
- **Config reload** — sensor subscriptions re-register on config save but the engine does not watch the config file for changes.
- **Carbon — UK only** — carbon intensity data comes from the National Grid ESO API which is UK-specific. Non-UK installations will not see carbon data.
- **Carbon — average not marginal intensity** — we use grid average intensity (what the National Grid API provides) not marginal intensity. This is standard for household carbon tracking but not suitable for formal carbon accounting.
- **Usage Stats iframe sizing** — the heatmap chart occasionally renders at incorrect width on first load due to a race between iframe layout and Plotly initialisation. It self-corrects on the next resize event or block finalise. A full fix requires eliminating the iframe pattern for the heatmap.