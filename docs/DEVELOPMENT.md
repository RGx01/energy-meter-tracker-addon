# Development Guide

> **The one hard rule:** changes must not alter existing billing results — the
> kWh / cost / carbon already stored for past history. `run_tests.sh` is the
> guard, and accuracy-sensitive changes should add a test that pins the expected
> output. See [`README.md`](README.md) for the full doc map.

## Architecture Overview

Energy Meter Tracker is a Home Assistant add-on built around a Python asyncio engine.

```
main.py              — Entry point. Wires together HAClient, engine and Flask server.
engine.py            — Core half-hour block engine. All metering logic (incl. device attribution).
ha_client.py         — WebSocket + REST client to Home Assistant.
energy_engine_io.py  — Atomic file I/O helpers.
energy_charts.py     — Chart generation (billing periods, billing history, heatmap, spiral).
block_store.py       — SQLite persistence layer. All block and config-period storage.
kraken_api_client.py — Supplier (Kraken) REST + GraphQL client: auth, consumption, rates, dispatch.
kraken_ingester.py   — DCC settlement poll/sweep: writes settled kWh onto blocks.
kraken_rates.py      — Tariff / rate-schedule resolution.
kraken_mini.py       — Octopus Home Mini live-power source.
csv_import.py        — CSV backfill parser → block store.
bill_parser.py       — Octopus PDF bill → import/export CSV (optional; lazy `pypdf`).
web/server.py        — Flask web UI and API endpoints.
web/templates/       — Jinja2 HTML templates.
```

EMT is Octopus-first but the supplier layer is not fully hardcoded — the API
endpoint is configurable and supplier capability is gated in one place. See the
"Adding support for another supplier" section in [`CONTRIBUTING.md`](CONTRIBUTING.md).

### Runtime modes

| Mode | Detection | HA connection |
|------|-----------|---------------|
| Supervised | `SUPERVISOR_TOKEN` env var present | `ws://supervisor/core/websocket` |
| Standalone Docker | No `SUPERVISOR_TOKEN` | `ws://<HA_URL>/api/websocket` |

`run.sh` detects the mode and sets `EMT_MODE` before starting Python.

---

## Data Storage — SQLite

Since 2.0.0 all state lives in a single SQLite database, `blocks.db` (at
`{DATA_DIR}/blocks.db`). It is the **single source of truth** — meter config,
config periods, blocks, the in-progress block, carbon intensity and power
history. The legacy `blocks.json` / `current_block.json` JSON stores and their
auto-migration shim were **removed in 4.0.0**. `meters_config.json` is still
written as a human-readable export but is never read back when `blocks.db`
exists.

> The tables below list the **key columns** for orientation, not the full
> schema. The authoritative schema (including the DCC-settlement columns
> `imp_kwh_api` / `exp_kwh_api` / `is_provisional` / `finalised_from_cad`, the
> device-attribution columns `imp_kwh_grid` / `imp_kwh_remainder` / `source`,
> and `carbon_intensity_g`) lives in `block_store.py`.

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
| `currency_symbol` / `currency_code` | TEXT | e.g. `£` / `GBP` |
| `site_name` | TEXT | Display name |
| `supplier` | TEXT | Optional supplier name (drives API capability gating) |
| `change_reason` | TEXT | Freetext note |

`effective_from` is always snapped to **midnight in the configured timezone**
(converted to UTC) when a config is saved.

### `blocks`

One row per meter per reconciliation period.

| Column | Type | Description |
|--------|------|-------------|
| `block_start` / `block_end` | TEXT | UTC ISO datetime |
| `local_date` | TEXT | Local calendar date (pre-computed at insert, from the configured tz) |
| `meter_id` | TEXT | e.g. `electricity_main` |
| `config_period_id` | INTEGER FK | Which config was active when this block was recorded |
| `imp_kwh` / `imp_cost` | REAL | Grid import kWh / cost |
| `exp_kwh` / `exp_cost` | REAL | Export kWh / value |
| `standing_charge` | REAL | Daily standing charge (same value on every block for a given day) |
| `carbon_g` | REAL | Net gCO₂ for this block (NULL if no region or pre-carbon) |

Key indexes: `block_start`, `local_date`, `config_period_id`, `meter_id`.

### `meters` / `meter_channels`

Normalised meter definitions joined to each config period (replaced the embedded
JSON blobs used before 2.0.0).

### `store_meta` / `kraken_state`

Key-value tables for application state — schema version, user carbon settings,
and the Kraken cursors (`last_poll_utc`, `last_settlement_sweep_utc`, import
run status, verify cursor, etc.).

### Standing charge

Standing charge is stored on **every block** (same value repeated). When summing
for a range, always aggregate once per `local_date` — never once per block:

```sql
SELECT SUM(daily_sc) FROM (
    SELECT MIN(standing_charge) AS daily_sc
    FROM blocks
    WHERE block_start >= ? AND block_start < ?
    GROUP BY local_date
)
```

`local_date` is pre-computed at insert using the configured timezone, so BST/GMT
transitions are handled correctly (a block at `23:00 UTC` in BST correctly gets
`local_date = next day`).

---

## `load_config()` — single-source-of-truth rule

Both `server.py` and `engine.py` have a `load_config()`. The rule:

**If `blocks.db` exists → always read config from the DB. Never fall back to
`meters_config.json`.** The JSON is read only when there is no `blocks.db` at all
(a true fresh install). This ensures that swapping a `blocks.db` between
environments always uses that DB's own config — site name, `block_minutes`,
timezone, everything self-contained.

---

## Billing Period Logic

### Config-period chain

Config periods form a contiguous chain: `period_A.effective_to =
period_B.effective_from`; the last period has `effective_to = NULL`.
`_rebuild_config_period_chain()` in `server.py` sorts periods by `effective_from`
and rebuilds the chain after any insert/edit/delete, reassigning every block to
the correct period by its `block_start`.

### Billing-period transitions

When the billing day changes, the old config's last period is **truncated** at
the transition date — bills can only be truncated, never extended.

Transition rule (given `effective_from` date and new `billing_day`):
- `effective_from.day < new_bd` → transition = `new_bd` of `effective_from.month`
- `effective_from.day >= new_bd` → transition = `new_bd` of the following month

### Period generators

- `get_billing_periods_from_config_history(blocks, tz)` — segments a loaded block list by `_effective_from`. Used by chart generators that already hold blocks.
- `get_billing_periods_from_config_periods(config_periods, tz)` — the fast path; takes `store.get_config_periods()` rows and avoids loading blocks. Used by Insights, Live Power and `api/billing`. Produces identical output.

---

## Block Lifecycle

The engine runs a **10-second tick loop**. Each tick:

1. **Drain read queue** — sensor state-change callbacks push timestamps onto `_read_queue`; the tick drains them and calls `capture_samples()`.
2. **Periodic checkpoint** — capture a sample every 60 s regardless of sensor updates.
3. **Near-boundary capture** — within 15 s of a block boundary, capture every tick.
4. **Gap fill** — if a `_gap_marker` is present, fill missing blocks by interpolation.
5. **Block rollover** — once past the block's end boundary with a post-boundary read available, `finalise_block()` runs.

### Finalisation passes

- **PASS 1** — kWh and cost per meter from boundary-interpolated opening/closing reads.
- **PASS 2** — grid-authoritative sub-meter distribution (`imp_kwh_grid` / `imp_kwh_remainder`).
- **PASS 3** — block totals.
- **PASS 4** — cumulative totals + push to HA sensors.

Carbon (`carbon_g`) is applied after PASS 1 from the nearest `carbon_intensity`
row: the main meter uses `(imp_kwh − exp_kwh) × intensity`, sub-meters use
`imp_kwh × intensity`. After finalisation, charts regenerate (off-loop) and
`blocks.db` is backed up to `/share/`.

### Interpolation & gaps

`interpolate_value(pre, post, target_dt)` linearly interpolates between two
timestamped reads (fraction clamped to [0,1]). On restart after an outage,
`detect_gap()` counts missing windows and `build_gap_blocks()` interpolates them;
gaps longer than 12 hours produce zero blocks.

### DCC settlement (API modes)

For supplier-API billing, `kraken_ingester.py` polls settled half-hourly
consumption and writes it onto blocks (`*_api` columns), replacing the
provisional live/CAD figure. A once-per-day **settlement sweep** re-chases blocks
still awaiting settlement (import or export) over `oldest-unsettled → now`,
bounded by a 14-day horizon. Export commonly settles a day or two behind import.

---

## Carbon Intensity

Carbon data comes from the National Grid / Carbon Intensity **regional** API
(**UK only**, keyed on the outward postcode → DNO region).

- Fetched every ~30 min into the `carbon_intensity` table (4-day retention).
- Applied at finalisation: `carbon_g = net_kwh × intensity_g_per_kwh`.
- Storing `carbon_g` per block preserves the intensity history indefinitely (the raw value is only available from the API for ~4 days). Recover it as `carbon_g / net_kwh` for any block with non-zero net flow.
- A historical backfill fills imported history per **region-per-period** (a house move is a region boundary); only the **outward** postcode is ever stored.

---

## Insights & Settings

**Insights** (`/api/insights/*`) return per-billing-period carbon summaries and a
full breakdown (effective vs grid-average intensity, per-sub-meter charge
intensity, house-vs-device split). Sub-meter type is resolved from
`meta.meter_type`, falling back to keyword matching on the meter id
(`ev`/`charger`, `battery`, `heat`/`pump`, `solar`/`pv`/`inv`).

**Settings** — user-configurable carbon assumptions are a JSON blob in
`store_meta` under `settings`; missing keys fall back to `SETTINGS_DEFAULTS` in
`server.py` at read time (no migration needed when defaults are added). Defaults
carry citations (BEIS/DESNZ, Woodland Trust, IEA, etc.).

---

## Navigation & Templates

Page routes (indicative — see `@app.route` decorators in `server.py` for the
authoritative list):

```
/                 /charts            /insights          /live-power
/settings         /billing-history   /data-management   /fill-history
/historical-import /historical-probe /delete-blocks     /corrections
/logs             /help
```

`base.html` provides the shared shell; each page sets an `active` flag for the
sidebar and fills the `topbar` / `subbar` / `content` blocks.

---

## File Structure

```
/data/energy_meter_tracker/
    blocks.db                — SQLite database (single source of truth)
    meters_config.json       — human-readable export only; never read if blocks.db exists

/share/energy_meter_tracker_backup/
    blocks.db                — copied after every finalise
    backups/
        YYYYMMDDTHHMMSS_label.zip           — zip snapshots (pruned, ~20 kept)
        YYYYMMDDTHHMMSS_upgrade_x.y.z.zip   — one per version on first startup
```

---

## Running Tests

The suite uses `pytest` over the `tests/` folder. Everything runs against
in-memory SQLite (`:memory:`) with module stubs — no HA, Flask or real data files
required; no test touches real data.

```bash
bash run_tests.sh                      # the whole suite (1,850+ tests) — the release gate
python3 -m pytest tests/test_engine.py -q          # one file while iterating
python3 -m pytest tests/ -q -k unsettled           # filter by name
```

Key coverage: `test_engine.py` (block engine, gap fill, attribution, settlement
sweep, verify pass), `test_block_store.py` (schema, billing totals, unsettled
predicate, carbon), `test_server.py` (endpoints, billing accuracy, config CRUD),
`test_usage_stats_vs_billing.py` (Usage Stats aggregations reconcile with direct
DB queries), plus per-feature files (kraken client/ingester, region timeline,
CSV/bill import, dispatch overlay, etc.).

> A separate `test_harness.py` uploads synthetic data to a **running** instance
> for visual checks and **replaces all data** on the target — only run it against
> a development instance, and back up first.

---

## Local Development

### Supervised (HA OS)

The add-on loads from `/addons/energy_meter_tracker/`. Python and template
changes require a rebuild via the HA add-on UI; if `config.yaml` changed, run
`ha supervisor restart` afterwards.

### Standalone Docker

```bash
docker build -t emt-dev .
docker run -d --name emt-dev -p 8099:8099 \
  -e EMT_MODE=standalone \
  -e HA_URL=http://192.168.1.10:8123 \
  -e HA_TOKEN=your_token \
  -e LOG_LEVEL=debug \
  -v /tmp/emt-data:/data/energy_meter_tracker \
  emt-dev
docker logs -f emt-dev
```

### Swapping databases between environments

Because `blocks.db` is the single source of truth, copy it directly between
environments; the target uses that DB's own config periods. No need to copy
`meters_config.json`.

```bash
scp ha:/data/energy_meter_tracker/blocks.db /tmp/emt-data/blocks.db
```

---

## Key Design Decisions

**Why SQLite?** The flat `blocks.json` became a bottleneck as history grew —
loading 100k+ rows into memory for every billing calculation was slow. SQLite
gives indexed queries, SQL aggregation and atomic writes with no server.

**Why is `blocks.db` the single source of truth?** Two authoritative sources
(DB + JSON) caused subtle bugs when copying a DB between environments — the old
environment's JSON would override the DB's config. `load_config()` never reads the
JSON when the DB exists.

**Why asyncio?** The engine juggles WebSocket events, a 10-second tick and Flask
concurrently. Flask runs in a background thread; everything else is on the main
event loop. This is also why DB writes stay on the loop thread — the engine has a
single SQLite connection, and offloading writes to executor threads corrupts it.

**Why is the main meter authoritative?** Sub-meter sensors (CT clamps, device
integrations) are less accurate than the DCC / CAD grid reading. Treating the
main meter as authoritative and distributing it across sub-meters keeps billing
totals grounded in the actual grid reading.

**Why interpolate at boundaries?** A read arriving at 09:28 would otherwise land
entirely in one block. Interpolation splits the delta proportionally so each
block gets the fraction that occurred within it.

**Why pre-compute `local_date`?** SQLite's `DATE()` is UTC; standing charge must
sum once per local day. Pre-computing `local_date` at insert (in the configured
tz) avoids passing offsets into every query and handles BST/GMT correctly.

**Why truncation-only transitions?** Extending a period could create bills longer
than a month. Truncation-only gives a partial final period under the old config
and a clean start under the new — matching real supplier behaviour.

**Why store `carbon_g` per block?** The grid intensity for a block is only
available from the API for ~4 days. Storing `carbon_g` at finalisation preserves
the full history; the raw intensity is recoverable as `carbon_g / net_kwh`.

**Why grid-average (not marginal) export carbon?** Marginal intensity isn't
published in real time. Grid average is conservative, standard, and used by the
Carbon Trust; users can override to a fixed custom intensity in Settings.

---

## Known Limitations & Future Work

- **Gas meters** — not designed; would need a separate meter type with unit conversion (m³/ft³ + calorific value).
- **Export sub-metering** — export can't yet be split between solar and battery discharge without additional metering.
- **Multiple batteries/inverters** — only one inverter-type sub-meter per parent is well tested.
- **V2G export** — V2X EV export is flagged but not broken down by sub-meter.
- **Carbon — UK only** — the regional intensity API is UK-specific; non-UK installs see no carbon data.
- **Carbon — average not marginal** — standard for household tracking, not formal carbon accounting.
- **Non-Octopus suppliers** — other Kraken suppliers (EDF, etc.) aren't supported yet; the seam and the intended approach are documented in `CONTRIBUTING.md`.
- **Config reload** — sensor subscriptions re-register on config save, but the engine doesn't watch the config file for external changes.