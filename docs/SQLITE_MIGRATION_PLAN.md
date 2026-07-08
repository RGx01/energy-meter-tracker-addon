# SQLite Migration Plan

## Overview

Replace `blocks.json` with a SQLite database (`blocks.db`) to address performance
limits at high resolution and multi-meter configurations. The change is transparent
to users — the DB file lives in the same `/data/energy_meter_tracker/` directory,
the backup mechanism carries it to `/share/`, and a one-time automatic migration
handles existing `blocks.json` files.

---

## Schema

The schema is designed around a core principle: **blocks contain measurements,
not configuration**. Currently `blocks.json` repeats the full meter meta on every
block -- timezone, billing day, block minutes, currency, device labels -- none of
which is measurement data and all of which changes meaning if config is ever
updated. The relational schema separates these cleanly.

### `config_periods` -- configuration history

```sql
CREATE TABLE IF NOT EXISTS config_periods (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    effective_from   TEXT    NOT NULL,   -- UTC ISO 8601, when this config became active
    effective_to     TEXT,               -- NULL = currently active
    billing_day      INTEGER NOT NULL DEFAULT 1,
    block_minutes    INTEGER NOT NULL DEFAULT 30,
    timezone         TEXT    NOT NULL DEFAULT 'UTC',
    currency_symbol  TEXT    NOT NULL DEFAULT '£',
    currency_code    TEXT    NOT NULL DEFAULT 'GBP',
    site_name        TEXT,
    change_reason    TEXT,               -- optional user note e.g. "Switched to Octopus"
    full_config_json TEXT    NOT NULL    -- complete meters_config.json snapshot
);
```

Every time the user saves Meter Config, the current config is snapshotted here
before the new one takes effect. `effective_to` on the previous row is set to
the same timestamp as `effective_from` on the new row.

This means `billing_day` is no longer a single global value -- it is a value
that was true for a specific period. Historical billing calculations use the
`billing_day` that was active *when each block was recorded*, not today's value.
Changing supplier no longer retroactively alters historical bills.

### `meters` -- meter registry

```sql
CREATE TABLE IF NOT EXISTS meters (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    meter_id         TEXT    NOT NULL,   -- "electricity_main", "zappi_ev", "gas_main"
    is_sub_meter     INTEGER NOT NULL DEFAULT 0,
    device_label     TEXT,               -- display name, may change over time
    parent_meter_id  TEXT,               -- for sub-meters
    config_period_id INTEGER NOT NULL,
    FOREIGN KEY (config_period_id) REFERENCES config_periods(id)
);

CREATE INDEX IF NOT EXISTS idx_meters_meter_id ON meters (meter_id);
```

### `blocks` -- measurement data only

```sql
CREATE TABLE IF NOT EXISTS blocks (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    block_start      TEXT    NOT NULL,
    block_end        TEXT    NOT NULL,
    local_date       TEXT    NOT NULL,   -- YYYY-MM-DD in configured timezone
    local_year       INTEGER NOT NULL,
    local_month      INTEGER NOT NULL,
    local_day        INTEGER NOT NULL,
    meter_id         TEXT    NOT NULL,
    config_period_id INTEGER NOT NULL,   -- which config was active for this block
    interpolated     INTEGER NOT NULL DEFAULT 0,
    imp_kwh          REAL,
    imp_kwh_grid     REAL,
    imp_rate         REAL,
    imp_cost         REAL,
    imp_read_start   REAL,
    imp_read_end     REAL,
    exp_kwh          REAL,
    exp_rate         REAL,
    exp_cost         REAL,
    exp_read_start   REAL,
    exp_read_end     REAL,
    standing_charge  REAL DEFAULT 0,
    FOREIGN KEY (config_period_id) REFERENCES config_periods(id)
);

CREATE INDEX IF NOT EXISTS idx_blocks_start     ON blocks (block_start);
CREATE INDEX IF NOT EXISTS idx_blocks_date      ON blocks (local_date);
CREATE INDEX IF NOT EXISTS idx_blocks_ym        ON blocks (local_year, local_month);
CREATE INDEX IF NOT EXISTS idx_blocks_meter     ON blocks (meter_id);
CREATE INDEX IF NOT EXISTS idx_blocks_meter_dt  ON blocks (meter_id, block_start);
CREATE INDEX IF NOT EXISTS idx_blocks_period    ON blocks (config_period_id);
```

Compared to storing meta in every block, this eliminates ~15 fields of repeated
config data per block row. At 5-minute resolution with two meters over a year
that is roughly 1 million redundant field values removed.

### `reads` -- raw sensor reads (Phase 2+)

```sql
CREATE TABLE IF NOT EXISTS reads (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    captured_at  TEXT    NOT NULL,
    meter_id     TEXT    NOT NULL,
    channel      TEXT    NOT NULL,   -- "import" or "export"
    reading_kwh  REAL    NOT NULL,
    rate         REAL,
    block_id     INTEGER,            -- NULL until containing block is finalised
    FOREIGN KEY (block_id) REFERENCES blocks(id)
);

CREATE INDEX IF NOT EXISTS idx_reads_captured   ON reads (captured_at);
CREATE INDEX IF NOT EXISTS idx_reads_block      ON reads (block_id);
CREATE INDEX IF NOT EXISTS idx_reads_meter_time ON reads (meter_id, captured_at);
```

### Schema versioning

```sql
CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT
);
INSERT OR IGNORE INTO meta VALUES ('schema_version', '1');
```

### Design decisions

**Config periods as first-class entities** -- the relationship between blocks
and config_periods is the most important design decision. It makes the schema
self-describing: any block can be fully understood by joining to its
config_period, without needing to consult the current live config. This enables
historically accurate billing and makes the data portable and auditable.

**Billing mode vs Calendar mode** -- the billing chart can offer both views:

- *Billing mode*: groups blocks by billing period using `billing_day` from
  `config_period_id` -- historically accurate, matches actual invoices
- *Calendar mode*: ignores `billing_day`, groups by calendar month -- comparable
  across periods even when config changed

The Usage Stats chart already has this toggle; the billing chart gains it as
part of this migration.

**`full_config_json`** -- storing the complete config snapshot alongside the
normalised columns means nothing is lost even if the schema doesn't capture
every field. It also means the config at any point in history can be fully
reconstructed without guesswork.

**Text timestamps** -- ISO 8601 strings sort lexicographically correctly and
are human-readable in DB Browser for SQLite. The performance difference vs
integer Unix timestamps is negligible for this workload.

**`local_date` pre-computed** -- timezone conversion at write time rather than
query time avoids repeated conversion on every chart query and makes date-range
indexes effective.


## Migration phases

### Phase 1 — Drop-in replacement (target: 1.7.0)

Replace `blocks.json` read/write with `BlockStore` calls. No other logic changes.
Engine and charting continue to receive blocks as Python dicts. This is the
lowest-risk phase and delivers the core performance improvement.

**Changes required:**

| File | Change |
|------|--------|
| `engine.py` | Replace `load_json(BLOCKS_PATH)` / `append_block()` with `BlockStore` calls |
| `engine.py` | On config save, insert new `config_periods` row and set `effective_to` on previous |
| `engine.py` | `generate_charts(blocks)` receives `store.get_all_blocks()` with config fields joined |
| `web/server.py` | Replace `_load_json("blocks.json")` with `store.get_blocks_for_range()` |
| `web/server.py` | Billing calculations join blocks to `config_periods` for historically correct `billing_day` |
| `energy_charts.py` | No changes — still receives list of dicts with config fields joined in |
| `energy_engine_io.py` | Add `open_block_store(path)` factory |
| `engine.py` | Auto-migrate `blocks.json` → DB on startup if DB absent |
| `engine.py` | Update `_backup_to_share()` to copy `blocks.db` using SQLite backup API |
| `test_engine.py` | Use in-memory `BlockStore` fixture |
| `test_server.py` | Same |

**SQLite settings applied at open time:**
```sql
PRAGMA journal_mode = WAL;    -- concurrent reads during write
PRAGMA synchronous  = NORMAL; -- safe but faster than FULL
PRAGMA cache_size   = -8000;  -- 8MB page cache
PRAGMA temp_store   = MEMORY;
```

### Phase 2 — Query optimisation (target: 1.8.0)

Update charting and billing functions to use date-range queries rather than
`get_all_blocks()`. This is where the biggest performance gain comes for large
datasets — chart generation goes from O(n) full scan to O(k) where k is blocks
in the requested period.

| Function | Change |
|----------|--------|
| `generate_daily_import_export_charts` | Query by date range, not full list |
| `generate_net_heatmap` | Query by date range |
| `calculate_billing_summary_for_period` | Direct date-range query |
| `api_blocks_summary` | Direct date-range query |

### Phase 3 — Gas meter (target: 1.9.0+)

The schema already accommodates gas via `meter_id`. Gas blocks would be inserted
with `meter_id = "gas_main"` alongside electricity blocks. No schema change needed
— just a new meter type in `meters_config.json` and corresponding engine logic.

### Phase 4 — Higher resolution (future)

Sub-5-minute resolution is viable with SQLite. A year of 1-minute electricity
data is ~525,000 rows — trivial for SQLite with proper indexing. The main
constraint shifts from storage to chart rendering (too many bars to display
meaningfully), which is a UI problem not a DB problem.

---

## Migration script (auto-run on startup)

```python
def migrate_json_to_sqlite(json_path: str, store: BlockStore) -> int:
    """
    One-time migration of blocks.json → blocks.db.
    Returns number of blocks migrated.
    Renames blocks.json → blocks.json.migrated on success.
    Safe to run multiple times (idempotent via INSERT OR IGNORE).
    """
```

**Migration strategy:**
1. On engine startup, check if `blocks.db` exists
2. If not, and `blocks.json` exists, run migration
3. Create a single `config_periods` row from the current `meters_config.json`,
   with `effective_from` set to the timestamp of the oldest block in the data
   (or a safe epoch if no blocks exist), and `effective_to` = NULL
4. All migrated blocks receive `config_period_id = 1` -- the single historical
   config; this preserves exactly the current behaviour where one config applies
   to all history
5. Log progress every 1,000 blocks
6. On success, rename `blocks.json` to `blocks.json.migrated`
7. Keep `blocks.json.migrated` for two releases then delete
8. If migration fails partway, `blocks.db` is deleted and engine falls back
   to `blocks.json` -- no data loss possible

From the migration date onwards, every Meter Config save creates a new
`config_periods` row. Historical data remains consistent with current behaviour
while new data benefits from accurate config period tracking immediately.

---

## Backup changes

The backup destination already differs by deployment mode (set at startup):

- **HA OS / Supervised**: `/share/energy_meter_tracker_backup/`
- **Standalone Docker**: `{DATA_DIR}/backup/` (inside the mounted volume)

This is handled by the existing `SHARE_BACKUP_DIR` constant — no change needed
for standalone support. Both modes back up to the same logical location they
always have; the SQLite migration just changes what gets copied there.

Current: copy `blocks.json` to `SHARE_BACKUP_DIR/blocks.json`

New: use SQLite online backup API to safely hot-copy the live DB to
`SHARE_BACKUP_DIR/blocks.db`:

```python
import sqlite3

def backup_db(src_conn, dst_path: str) -> None:
    dst = sqlite3.connect(dst_path)
    src_conn.backup(dst, pages=100, sleep=0.1)
    dst.close()
```

This is atomic and safe while the engine is writing — no risk of a corrupt
backup from copying a live SQLite file directly. The file-copy approach used
for JSON is unsafe for SQLite because a mid-write copy can produce a corrupt
WAL state; the backup API handles this correctly regardless of write activity.

**Standalone users** have no `/share/` directory, which is already accounted
for — their backups land at `~/emt-data/backup/blocks.db` (or wherever their
volume is mounted). The Import & Backup page restore path works the same way
in both modes.

---


---

## Raw reads table

### Motivation

The engine currently captures raw sensor reads at every sample interval
(10-60 seconds depending on the CAD) and discards them after block finalise.
Only the interpolated boundary deltas are kept in blocks. Storing the raw reads
alongside blocks enables:

- **Within-block resolution charting** - the 1.9.0 High-Res Charting roadmap
  item requires this; 30-minute blocks currently show as a single bar, but with
  reads you can render 30 one-minute bars within each block
- **Auditability** - any block's kWh delta and cost can be reconstructed from
  first principles from the raw reads
- **Mid-block rate change detection** - if a rate changes mid-block (e.g. Agile
  tariff), individual reads capture the rate at each point rather than a blended
  average
- **Gas meter support** - gas CADs often update at 30-minute intervals; storing
  each update as a read allows the same charting infrastructure to handle gas
  at whatever resolution the hardware provides
- **Configurable retention** - keep full-resolution reads for a rolling window
  (e.g. 90 days), retain only block summaries beyond that

### Schema addition

```sql
-- Every raw sensor read captured by the engine sample loop
CREATE TABLE IF NOT EXISTS reads (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    captured_at  TEXT    NOT NULL,   -- UTC ISO 8601 timestamp of the HA state read
    meter_id     TEXT    NOT NULL,   -- "electricity_main", "zappi_ev", "gas_main" etc
    channel      TEXT    NOT NULL,   -- "import" or "export"
    reading_kwh  REAL    NOT NULL,   -- cumulative kWh from the sensor at this moment
    rate         REAL,               -- currency/kWh at time of read
    block_id     INTEGER,            -- NULL until the containing block is finalised
    FOREIGN KEY (block_id) REFERENCES blocks(id)
);

CREATE INDEX IF NOT EXISTS idx_reads_captured   ON reads (captured_at);
CREATE INDEX IF NOT EXISTS idx_reads_block      ON reads (block_id);
CREATE INDEX IF NOT EXISTS idx_reads_meter_time ON reads (meter_id, captured_at);
```

### Relationship to blocks

Every read belongs to exactly one block. The `block_id` foreign key is NULL
while the block is open, and is set to the new block's `id` at finalise time:

```
reads (captured_at 00:00:04) --+
reads (captured_at 00:00:14) --+
reads (captured_at 00:00:24) --+--> blocks (block_start 00:00:00, block_end 00:30:00)
...                             |
reads (captured_at 00:29:54) --+
```

The block's `imp_kwh` / `exp_kwh` values are the interpolated boundary deltas -
the authoritative billing figures. Reads are supplementary, providing
within-block detail.

### Data volumes

| Capture interval | Meters | Reads/day | Reads/year | Approx size/year |
|-----------------|--------|-----------|------------|-----------------|
| 60 sec | 1 | 1,440 | 525,600 | ~53 MB |
| 60 sec | 2 | 2,880 | 1,051,200 | ~105 MB |
| 10 sec | 1 | 8,640 | 3,153,600 | ~315 MB |
| 10 sec | 2 | 17,280 | 6,307,200 | ~630 MB |

At 60-second capture a two-meter setup produces roughly 100 MB/year - comfortable
for SQLite with proper indexing. At 10-second capture this grows to 630 MB/year;
the configurable retention policy keeps this bounded.

### Engine changes

**Phase 1** - create the `reads` table in the schema but do not write to it.
Establishes the relationship and indexes without changing any engine logic.
Schema is stable from day one.

**Phase 2** - populate `reads` during `capture_samples()`. Each sample is
persisted immediately alongside the existing in-memory accumulation:

```python
# In capture_samples(), after reading each sensor:
store.insert_read(
    meter_id    = 'electricity_main',
    channel     = 'import',
    captured_at = now_utc_iso,
    reading_kwh = cumulative_kwh,
    rate        = current_rate,
    block_id    = None   # linked at finalise
)
```

At block finalise, link all open reads to the new block:

```python
store.link_reads_to_block(
    block_start = block['start'],
    block_end   = block['end'],
    block_id    = new_block_id
)
```

The in-memory `current_block` accumulation continues to work exactly as before.
Reads are written to the DB in addition to, not instead of, the existing logic.
No engine logic changes in Phase 2 - only additions.

**Phase 4 (High-Res Charting)** - `BlockStore` gains read-query methods:

```python
def get_reads_for_block(self, block_id: int) -> list[dict]:
    # All reads for a single block - for within-block drill-down charts

def get_reads_for_range(self, start, end, meter_id=None) -> list[dict]:
    # Reads within a time range - for high-res chart generation

def purge_reads_older_than(self, days: int) -> int:
    # Delete reads beyond the retention window. Returns count deleted.
```

### Read retention policy

A configurable retention window in Meter Config (default: 90 days, options:
30 / 90 / 180 / 365 / unlimited) allows users to balance storage against
historical detail. Reads outside the window are purged periodically after block
finalise. Block summaries are never purged - they are the permanent billing record.

### Migration of existing data

The `reads` table will be empty after migration from `blocks.json` - raw reads
were never stored previously. This is handled gracefully: high-res charts show
no data for periods before the migration date, while billing and standard charts
are entirely unaffected (they use the `blocks` table only).

## Testing strategy

All tests use an in-memory BlockStore:

```python
@pytest.fixture
def store():
    s = BlockStore(':memory:')
    yield s
    s.close()
```

The existing 56 engine tests and 50 server tests continue to work by injecting
the in-memory store. No test reads from disk.

---

## What users will notice

- **Nothing** on upgrade — migration is automatic and silent (one log line)
- Slightly faster startup (no large JSON parse)
- Faster chart regeneration as data grows
- `blocks.json` replaced by `blocks.db` in the data directory
- DB Browser for SQLite can inspect `blocks.db` directly
- Backup/restore via Import & Backup page works the same way

---

## Risks and mitigations

| Risk | Mitigation |
|------|------------|
| Migration corrupts data | Keep `blocks.json.migrated`; delete DB and retry is safe |
| Concurrent write/read corruption | WAL mode eliminates this |
| Schema change in future version | `schema_version` in `meta` table; migration scripts per version |
| Users manually copying `blocks.db` while running | Document this risk; backup page uses SQLite backup API |
| armhf SQLite version too old for WAL | WAL available since SQLite 3.7.0 (2010) — safe on all supported platforms |
| Test suite complexity | In-memory DB fixture is simpler than JSON file fixtures |
