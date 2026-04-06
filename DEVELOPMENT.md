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

`effective_from` is always snapped to **midnight in the configured timezone** (converted to UTC) when a config is saved. `full_config_json` was removed in 2.1.0 — meter definitions live in the `meters` and `meter_channels` tables.

### `meters`

One row per meter per config period. `meter_id` is a stable string key (e.g. `electricity_main`). `blocks.meter_id` references this by value (no FK) so adding, removing, or re-adding a meter never causes constraint issues on historical blocks.

| Column | Type | Description |
|--------|------|-------------|
| `config_period_id` | INTEGER FK | Which config period this meter belongs to |
| `meter_id` | TEXT | Stable key e.g. `electricity_main`, `ev_charger` |
| `is_sub_meter` | INTEGER | 1 if this is a sub-meter |
| `parent_meter_id` | TEXT | `meter_id` of parent (sub-meters only) |
| `device_label` | TEXT | Display name e.g. `EV Charger` |
| `protected` | INTEGER | 1 if protected load (EV, heat pump) |
| `inverter_possible` | INTEGER | 1 if battery/inverter capable |
| `power_sensor` | TEXT | HA entity_id for live power (main meter only) |
| `postcode_prefix` | TEXT | UK postcode prefix for carbon intensity (main meter only) |

### `meter_channels`

Per-channel sensor configuration. One row per meter per channel (`import`/`export`).

| Column | Type | Description |
|--------|------|-------------|
| `meter_id` | INTEGER FK | → `meters.id` |
| `channel` | TEXT | `import` or `export` |
| `read_sensor` | TEXT | HA entity_id for kWh cumulative sensor |
| `rate_sensor` | TEXT | HA entity_id for rate (£/kWh) sensor |
| `standing_charge_sensor` | TEXT | HA entity_id (optional) |
| `mpan` | TEXT | Meter Point Administration Number (optional) |
| `tariff` | TEXT | Tariff name/code (optional) |

### `current_block`

Single-row table (id always = 1) holding the in-progress block state. Replaces `current_block.json`. Gap state is fully relational — no JSON blobs.

| Column | Type | Description |
|--------|------|-------------|
| `id` | INTEGER | Always 1 — enforces single row |
| `block_start` | TEXT | UTC ISO — current block window start |
| `block_end` | TEXT | UTC ISO — current block window end |
| `last_checkpoint` | TEXT | UTC ISO — last sensor capture timestamp |
| `gap_detected_at` | TEXT | UTC ISO — when gap was detected; NULL if no active gap |
| `interpolated` | INTEGER | 1 if this is a gap-filled block |

### `current_reads`

Rolling reads/rates buffer for the in-progress block. Replaces the `reads[]` and `rates[]` arrays in `current_block.json`. Fully replaced on every `save_current_block()` call. Gap seed rows (pre-gap meter readings used to interpolate missing blocks) are stored here with `is_gap_seed > 0` rather than in a separate JSON blob.

| Column | Type | Description |
|--------|------|-------------|
| `captured_at` | TEXT | UTC ISO — when reading was taken |
| `meter_id` | TEXT | e.g. `electricity_main` |
| `channel` | TEXT | `import` or `export` |
| `channel_type` | TEXT | `read` (kWh) or `rate` (£/kWh) |
| `value` | REAL | kWh for reads, £/kWh for rates |
| `standing_charge` | REAL | Standing charge at capture time (reads only) |
| `is_gap_seed` | INTEGER | 0=live read, 1=gap seed kWh, 2=gap seed rate |

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
    energy_meter.db          — SQLite database (all state — blocks, config, current block)
    meters_config.json       — convenience export only; not read back as live state

/share/energy_meter_tracker_backup/
    energy_meter.db          — copied after every finalise (SQLite online backup API)
    meters_config.json       — copied after every finalise (human-readable export)
    backups/
        YYYYMMDDTHHMMSS_label.zip   — zip snapshots (20 max)
```

### Deprecated files (still present for migration window)
```
current_block.json.migrated  — renamed from current_block.json on first 2.1.0 startup
blocks.json.migrated         — renamed from blocks.json on first 2.0.0 startup
cumulative_totals.json       — ignored since 2.1.0; can be deleted
```

---

## Running Unit Tests

Tests use Python's built-in `unittest` — no external dependencies needed.

```bash
cd /addons/energy_meter_tracker
python3 -m unittest test_engine -v      # 56 engine tests
python3 -m unittest test_block_store -v # 68 block store + billing tests
python3 -m unittest test_server -v      # 50 server/API tests
```

Or run all at once:
```bash
python3 -m unittest discover -v
```

### Test coverage

- `test_engine.py` — `floor_to_hh`, `interpolate_value`, `detect_gap`, `compute_channel`, `select_opening_read`, `select_closing_read`, gap marker helpers, `build_gap_blocks`, `extract_last_reads`
- `test_block_store.py` — SQLite schema, block insertion, `get_blocks_for_range`, `get_blocks_for_local_date_range`, config period CRUD, `delete_config_period` (cascade to meters/channels, block reassignment), `get_billing_totals_for_local_date_range` (SQL vs block-method comparison, BST boundary), `save_current_block`/`load_current_block` roundtrip (reads, rates, gap marker via `is_gap_seed`, standing charge), `get_cumulative_totals`, normalised meter schema (`_write_meters`, `config_from_db`, sub-meters, channel meta, mpan/tariff, multi-period isolation), migration tests (`migrate_full_config_json` — full 2.0→2.1 path against real SQLite file: meters populated, blobs dropped, columns added, idempotency)
- `test_server.py` — all API endpoints, billing accuracy, config history CRUD, historical corrections (preview + apply, all types, recalc, validation)
- `test_usage_stats_vs_billing.py` — daily/monthly/yearly Usage Stats rows vs SQL ground truth; BST boundary days; standing charge once per local day; billing chart vs usage stats cross-method agreement

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

**Why is `energy_meter.db` the single source of truth?**
Having three JSON files (`current_block.json`, `cumulative_totals.json`, `meters_config.json`) alongside the database created split-brain risks — manual DB edits were silently overwritten by file reads, backup/restore had to handle multiple files consistently, and the sync logic between file and DB was a recurring source of bugs. A single DB file eliminates all of these: backup is a SQLite online backup, restore is a file copy, and there is no ambiguity about which store is authoritative.

**Why is the config fully normalised rather than stored as a JSON blob?**
`full_config_json` was the fastest path to getting config into the DB, but it meant that individual config fields (billing_day, timezone, sensor entity IDs) could not be queried directly. The normalised `meters` and `meter_channels` tables allow the billing and billing history logic to join directly on config fields, make adding/removing meters a simple row operation, and eliminate a class of bugs where the blob and the scalar columns could disagree.

**Why `is_gap_seed` rather than a separate gap_seeds table?**
The gap seed rows (pre-gap meter readings used to interpolate missing blocks) have exactly the same shape as live `current_reads` rows. Storing them in the same table with a discriminator column (`is_gap_seed`) avoids a join and keeps `load_current_block()` simple — one query returns all rows, live and seed alike, and the caller reconstructs the `_gap_marker` dict from the flag values.

**Why `load_config()` falls back to `meters_config.json`?**
The fallback exists only for fresh installs and the migration window. If `config_periods` has rows, the file is never consulted. This means the file can drift from the DB without causing problems — it is written for human readability only. The fallback will be removed once the migration window closes.

**Why `local_date` pre-computed at insert?**
SQLite's `DATE()` function operates in UTC. Standing charge must be summed once per local calendar day. Pre-computing `local_date` at insert time (using the configured timezone) avoids needing to pass timezone offsets into every query and handles BST/GMT transitions correctly.

**Why corrections operate on `/data/` not `/share/`?**
`/share/energy_meter_tracker_backup/` is overwritten after every block finalise by the engine's backup routine. Any manual edits there are ephemeral. The correction tools always write to the live database at `/data/energy_meter_tracker/energy_meter.db`. The next backup cycle will then propagate the corrected values to `/share/`.

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