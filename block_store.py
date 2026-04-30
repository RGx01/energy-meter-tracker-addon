"""
block_store.py
==============
SQLite-backed storage for energy meter blocks and configuration history.

Replaces the flat blocks.json file with a relational schema that:
- Separates measurement data (blocks) from configuration (config_periods)
- Records configuration history so historical bills use the billing_day
  that was active when each block was recorded, not today's value
- Supports raw sensor reads for future high-resolution charting (Phase 2+)

Usage::

    store = BlockStore("/data/energy_meter_tracker/blocks.db")
    store.append_block(block_dict)
    blocks = store.get_blocks_for_range(start_dt, end_dt)
    store.close()

The :func:`open_block_store` factory is the preferred entry point — it applies
all required PRAGMAs and ensures the schema exists before returning.
"""

import json
import logging
import sqlite3
from datetime import datetime, timedelta, timezone
from typing import Optional
from zoneinfo import ZoneInfo

logger = logging.getLogger("block_store")

# ─────────────────────────────────────────────────────────────────────────────
# Schema
# ─────────────────────────────────────────────────────────────────────────────

SCHEMA_VERSION = 1

_DDL = """
-- Schema version tracking
CREATE TABLE IF NOT EXISTS store_meta (
    key   TEXT PRIMARY KEY,
    value TEXT
);

-- Configuration history: one row per config period.
-- Every time meters_config.json is saved a new row is inserted and
-- effective_to on the previous row is set to the same timestamp.
CREATE TABLE IF NOT EXISTS config_periods (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    effective_from   TEXT    NOT NULL,
    effective_to     TEXT,
    billing_day      INTEGER NOT NULL DEFAULT 1,
    block_minutes    INTEGER NOT NULL DEFAULT 30,
    timezone         TEXT    NOT NULL DEFAULT 'UTC',
    currency_symbol  TEXT    NOT NULL DEFAULT '£',
    currency_code    TEXT    NOT NULL DEFAULT 'GBP',
    site_name        TEXT,
    supplier         TEXT,               -- energy supplier name (display + historical record)
    change_reason    TEXT
);  -- full_config_json removed in 2.1.0; meters/channels in normalised tables

-- Meter definitions: one row per meter per config period.
-- meter_id is a stable string key (e.g. "electricity_main", "ev_charger").
-- blocks.meter_id references this by value (no FK) so adding/removing/
-- re-adding a meter never causes constraint issues on historical blocks.
CREATE TABLE IF NOT EXISTS meters (
    id                 INTEGER PRIMARY KEY AUTOINCREMENT,
    config_period_id   INTEGER NOT NULL,
    meter_id           TEXT    NOT NULL,
    is_sub_meter       INTEGER NOT NULL DEFAULT 0,
    parent_meter_id    TEXT,               -- meter_id of parent (sub-meters only)
    device_label       TEXT,               -- display name e.g. "EV Charger"
    meter_type         TEXT,               -- battery, ev, heat_pump, or NULL
    protected          INTEGER DEFAULT 0,  -- protected load (EV, heat pump)
    inverter_possible  INTEGER DEFAULT 0,  -- battery / inverter capable
    power_sensor       TEXT,               -- HA entity_id (main meter only)
    postcode_prefix    TEXT,               -- UK carbon intensity (main meter only)
    v2x_capable             INTEGER DEFAULT 0,  -- V2G / bidirectional charging capable
    inverter_power_invert   INTEGER DEFAULT 0,  -- negate inverter power sensor value
    soc_sensor         TEXT,               -- HA entity_id for battery SoC % (informational)
    inverter_power_sensor TEXT,            -- HA entity_id for inverter power W/kW (informational)
    device_power_sensor TEXT,              -- HA entity_id for EV/heat pump power W/kW (informational)
    FOREIGN KEY (config_period_id) REFERENCES config_periods(id),
    UNIQUE (config_period_id, meter_id)
);

CREATE INDEX IF NOT EXISTS idx_meters_period    ON meters (config_period_id);
CREATE INDEX IF NOT EXISTS idx_meters_meter_id  ON meters (meter_id);

-- Per-channel sensor configuration for each meter.
CREATE TABLE IF NOT EXISTS meter_channels (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
    meter_id                INTEGER NOT NULL,  -- FK → meters.id
    channel                 TEXT    NOT NULL,  -- 'import' or 'export'
    read_sensor             TEXT,              -- HA entity_id for kWh sensor
    rate_sensor             TEXT,              -- HA entity_id for rate sensor
    standing_charge_sensor  TEXT,              -- HA entity_id (optional)
    mpan                    TEXT,              -- meter point reference number
    tariff                  TEXT,              -- tariff name / code
    FOREIGN KEY (meter_id) REFERENCES meters(id),
    UNIQUE (meter_id, channel)
);

CREATE INDEX IF NOT EXISTS idx_meter_channels_meter ON meter_channels (meter_id);

-- Blocks: pure measurement data, no repeated config fields.
-- config_period_id links each block to the config that was active when
-- it was recorded.
CREATE TABLE IF NOT EXISTS blocks (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    block_start      TEXT    NOT NULL,
    block_end        TEXT    NOT NULL,

    meter_id         TEXT    NOT NULL,
    config_period_id INTEGER NOT NULL,
    interpolated     INTEGER NOT NULL DEFAULT 0,
    imp_kwh          REAL,
    imp_kwh_grid     REAL,
    imp_kwh_remainder REAL,
    imp_rate         REAL,
    imp_cost         REAL,
    imp_cost_remainder REAL,
    imp_read_start   REAL,
    imp_read_end     REAL,
    exp_kwh          REAL,
    exp_rate         REAL,
    exp_cost         REAL,
    exp_read_start   REAL,
    exp_read_end     REAL,
    standing_charge  REAL    NOT NULL DEFAULT 0,
    carbon_g         REAL,               -- net gCO2 for this block (NULL if no CI data)
    FOREIGN KEY (config_period_id) REFERENCES config_periods(id),
    UNIQUE (block_start, meter_id)
);

CREATE INDEX IF NOT EXISTS idx_blocks_start     ON blocks (block_start);

CREATE INDEX IF NOT EXISTS idx_blocks_meter     ON blocks (meter_id);
CREATE INDEX IF NOT EXISTS idx_blocks_meter_dt  ON blocks (meter_id, block_start);
CREATE INDEX IF NOT EXISTS idx_blocks_period    ON blocks (config_period_id);

-- Raw sensor reads (Phase 2+): populated by capture_samples().
-- block_id is NULL until the containing block is finalised.
CREATE TABLE IF NOT EXISTS reads (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    captured_at  TEXT    NOT NULL,
    meter_id     TEXT    NOT NULL,
    channel      TEXT    NOT NULL,
    reading_kwh  REAL    NOT NULL,
    rate         REAL,
    block_id     INTEGER,
    FOREIGN KEY (block_id) REFERENCES blocks(id)
);

CREATE INDEX IF NOT EXISTS idx_reads_captured   ON reads (captured_at);
CREATE INDEX IF NOT EXISTS idx_reads_block      ON reads (block_id);
CREATE INDEX IF NOT EXISTS idx_reads_meter_time ON reads (meter_id, captured_at);

-- current_block: single-row table holding the in-progress block state.
-- Replaces current_block.json as the engine's live state store.
-- gap_detected_at IS NOT NULL means a gap marker is active.
CREATE TABLE IF NOT EXISTS current_block (
    id              INTEGER PRIMARY KEY CHECK (id = 1),  -- enforce single row
    block_start     TEXT,       -- UTC ISO — current block window start
    block_end       TEXT,       -- UTC ISO — current block window end
    last_checkpoint TEXT,       -- UTC ISO — last capture timestamp
    gap_detected_at TEXT,       -- UTC ISO — when gap was detected, NULL if no gap
    gap_last_block_start TEXT,  -- UTC ISO — start of last finalised block before gap
    interpolated    INTEGER NOT NULL DEFAULT 0
);

-- Rolling reads/rates buffer for the in-progress block.
-- is_gap_seed: 0=live read, 1=gap seed kWh read, 2=gap seed rate reading.
-- Gap seed rows are the pre-gap meter readings used to interpolate missing blocks.
CREATE TABLE IF NOT EXISTS current_reads (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    captured_at     TEXT    NOT NULL,
    meter_id        TEXT    NOT NULL,
    channel         TEXT    NOT NULL,   -- 'import' or 'export'
    channel_type    TEXT    NOT NULL DEFAULT 'read',  -- 'read' or 'rate'
    value           REAL    NOT NULL,   -- kWh for reads, £/kWh for rates
    standing_charge REAL,
    is_gap_seed     INTEGER NOT NULL DEFAULT 0  -- 0=live, 1=gap seed read, 2=gap seed rate
);

CREATE INDEX IF NOT EXISTS idx_current_reads_meter ON current_reads (meter_id, channel, channel_type);
CREATE INDEX IF NOT EXISTS idx_current_reads_time  ON current_reads (captured_at);

-- Carbon intensity samples from National Grid API (15-min cadence, ~4-day retention).
-- postcode stored per row so config changes don't invalidate historical data.
CREATE TABLE IF NOT EXISTS carbon_intensity (
    captured_at          TEXT NOT NULL,      -- UTC ISO, 30-min API slot boundary
    postcode             TEXT NOT NULL,
    intensity_forecast   REAL,               -- gCO₂/kWh forecast (always populated)
    intensity_actual     REAL,               -- gCO₂/kWh actual (populated ~24hr later)
    ci_index             TEXT,               -- very-low/low/moderate/high/very-high
    PRIMARY KEY (captured_at, postcode)
);
-- Computed alias: intensity = COALESCE(actual, forecast) for backward compat
-- All consumers should use get_nearest_carbon_intensity which applies this.

CREATE INDEX IF NOT EXISTS idx_carbon_intensity_time ON carbon_intensity (captured_at);

-- Generation mix per block — fuel type percentages at time of each block.
-- Populated at block finalise time and backfilled alongside carbon_g.
-- One row per fuel per block. Lives as long as the block.
CREATE TABLE IF NOT EXISTS generation_mix (
    block_id    INTEGER NOT NULL REFERENCES blocks(id) ON DELETE CASCADE,
    fuel        TEXT    NOT NULL,   -- 'wind','solar','gas','nuclear','biomass','hydro','imports','other'
    perc        REAL    NOT NULL,   -- percentage of generation mix (0.0–100.0)
    PRIMARY KEY (block_id, fuel)
);

-- High-resolution net power history (engine-tick cadence ~10s, 48hr retention).
-- Only written when a power sensor is configured. intensity is denormalised from
-- the nearest carbon_intensity row at capture time.
CREATE TABLE IF NOT EXISTS power_history (
    captured_at      TEXT PRIMARY KEY,  -- UTC ISO
    net_kw           REAL NOT NULL,     -- positive = importing, negative = exporting
    intensity        REAL,              -- gCO2/kWh at capture time, NULL if no postcode
    carbon_gco2_min  REAL               -- net carbon rate gCO2/min from meter reads, NULL if unavailable
);

CREATE INDEX IF NOT EXISTS idx_power_history_time ON power_history (captured_at);

CREATE TABLE IF NOT EXISTS sub_meter_history (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    captured_at TEXT    NOT NULL,  -- UTC ISO
    meter_id    TEXT    NOT NULL,  -- battery sub-meter ID
    soc_pct     REAL,              -- state of charge 0-100, NULL if no SoC sensor
    inverter_kw REAL               -- inverter power kW, positive=charging, negative=discharging, NULL if no sensor
);

CREATE INDEX IF NOT EXISTS idx_sub_meter_history_time ON sub_meter_history (captured_at);
CREATE INDEX IF NOT EXISTS idx_sub_meter_history_meter ON sub_meter_history (meter_id, captured_at);
"""


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(tzinfo=None).isoformat()


def local_date_to_utc_bounds(local_date: str, tz_name: str) -> tuple[str, str]:
    """
    Convert a local date string (YYYY-MM-DD) to a pair of UTC ISO strings
    (utc_start_inclusive, utc_end_exclusive) representing the full local day
    in the given timezone.

    Handles DST correctly:
      - BST start day (23 hours): returns 23hr window
      - BST end day (25 hours): returns 25hr window
      - GMT days: returns exact 24hr window aligned to midnight UTC

    Returns naive ISO strings (no Z suffix) suitable for direct comparison
    with block_start values stored in the database.

    Example (Europe/London, BST):
      local_date_to_utc_bounds('2026-04-15', 'Europe/London')
      → ('2026-04-14T23:00:00', '2026-04-15T23:00:00')

    Example (Europe/London, GMT):
      local_date_to_utc_bounds('2026-01-15', 'Europe/London')
      → ('2026-01-15T00:00:00', '2026-01-16T00:00:00')

    Example (BST start day — 23 hours):
      local_date_to_utc_bounds('2026-03-29', 'Europe/London')
      → ('2026-03-28T23:00:00', '2026-03-29T23:00:00')

    Example (BST end day — 25 hours):
      local_date_to_utc_bounds('2026-10-25', 'Europe/London')
      → ('2026-10-24T23:00:00', '2026-10-25T23:00:00')
    """
    try:
        tz = ZoneInfo(tz_name)
    except Exception:
        tz = ZoneInfo("UTC")

    year, month, day = int(local_date[:4]), int(local_date[5:7]), int(local_date[8:10])

    # Midnight at start of local day
    local_start = datetime(year, month, day, 0, 0, 0, tzinfo=tz)
    # Midnight at start of next local day — computed by advancing date then re-localising
    next_day    = (datetime(year, month, day) + timedelta(days=1))
    local_end   = datetime(next_day.year, next_day.month, next_day.day, 0, 0, 0, tzinfo=tz)

    # Convert to UTC and strip tzinfo for DB comparison
    utc_start = local_start.astimezone(ZoneInfo("UTC")).replace(tzinfo=None)
    utc_end   = local_end.astimezone(ZoneInfo("UTC")).replace(tzinfo=None)

    return utc_start.isoformat(), utc_end.isoformat()


def local_date_range_to_utc_bounds(first_local_date: str, last_local_date: str,
                                    tz_name: str) -> tuple[str, str]:
    """
    Convert an inclusive local date range to UTC bounds for DB filtering.

    Returns (utc_start_inclusive, utc_end_exclusive) covering all blocks
    whose local date falls within [first_local_date, last_local_date].

    Handles DST correctly across the full range — the UTC start uses the
    offset at first_local_date, the UTC end uses the offset at last_local_date.
    Both are computed independently so a range straddling a DST transition
    (e.g. March 3 GMT → April 2 BST) is handled correctly.
    """
    utc_start, _ = local_date_to_utc_bounds(first_local_date, tz_name)
    _, utc_end   = local_date_to_utc_bounds(last_local_date,  tz_name)
    return utc_start, utc_end


def _channel(meter_block: dict, channel_name: str) -> dict:
    return (meter_block.get("channels") or {}).get(channel_name) or {}


def _block_rows(block: dict, config_period_id: int, tz_name: str) -> list[dict]:
    """
    Decompose a finalised block dict (as written to blocks.json) into a list
    of row dicts, one per meter, ready for INSERT into the blocks table.
    """
    block_start = block.get("start", "")
    block_end   = block.get("end", "")
    interpolated = 1 if block.get("interpolated") else 0


    rows = []
    for meter_id, meter_block in (block.get("meters") or {}).items():
        meta    = meter_block.get("meta") or {}
        imp     = _channel(meter_block, "import")
        exp     = _channel(meter_block, "export")

        rows.append({
            "block_start":       block_start,
            "block_end":         block_end,

            "meter_id":          meter_id,
            "config_period_id":  config_period_id,
            "interpolated":      interpolated,
            # import channel
            "imp_kwh":           imp.get("kwh"),
            "imp_kwh_grid":      imp.get("kwh_grid"),
            "imp_kwh_remainder": imp.get("kwh_remainder"),
            "imp_rate":          imp.get("rate"),
            "imp_cost":          imp.get("cost"),
            "imp_cost_remainder":imp.get("cost_remainder"),
            "imp_read_start":    imp.get("read_start"),
            "imp_read_end":      imp.get("read_end"),
            # export channel
            "exp_kwh":           exp.get("kwh"),
            "exp_rate":          exp.get("rate"),
            "exp_cost":          exp.get("cost"),
            "exp_read_start":    exp.get("read_start"),
            "exp_read_end":      exp.get("read_end"),
            # standing charge on main meter import
            "standing_charge":   float(meter_block.get("standing_charge") or 0),
            # carbon footprint (NULL if no CI data available)
            "carbon_g":          meter_block.get("carbon_g"),
        })
    return rows


def _row_to_block(rows: list[sqlite3.Row]) -> dict:
    """
    Reconstruct a block dict (matching the old blocks.json shape) from one or
    more DB rows that all share the same block_start.  The config fields
    (timezone, billing_day etc) are joined in from config_periods so callers
    don't need a separate lookup.
    """
    if not rows:
        return {}

    first = rows[0]
    block = {
        "start":       first["block_start"],
        "end":         first["block_end"],
        "interpolated": bool(first["interpolated"]),
        "meters":      {},
        "totals": {
            "import_kwh":  0.0,
            "import_cost": 0.0,
            "export_kwh":  0.0,
            "export_cost": 0.0,
        },
        # config fields joined from config_periods
        "_config_period_id": first["config_period_id"],
        "_effective_from":   first["effective_from"],
        "_billing_day":      first["billing_day"],
        "_block_minutes":    first["block_minutes"],
        "_timezone":         first["timezone"],
        "_currency_symbol":  first["currency_symbol"],
        "_currency_code":    first["currency_code"],
    }

    for row in rows:
        mid = row["meter_id"]
        imp_kwh  = row["imp_kwh"]  or 0.0
        imp_cost = row["imp_cost"] or 0.0
        exp_kwh  = row["exp_kwh"]  or 0.0
        exp_cost = row["exp_cost"] or 0.0

        # Build meta — include sub-meter flags from meters table if joined
        meta = {
            "block_minutes":  row["block_minutes"],
            "timezone":       row["timezone"],
            "billing_day":    row["billing_day"],
            "currency_symbol":row["currency_symbol"],
            "currency_code":  row["currency_code"],
        }
        try:
            if row["is_sub_meter"]:
                meta["sub_meter"] = True
            if row["parent_meter_id"]:
                meta["parent_meter"] = row["parent_meter_id"]
            if row["device_label"]:
                meta["device"] = row["device_label"]
            if row["inverter_possible"]:
                meta["inverter_possible"] = True
            if row["power_sensor"]:
                meta["power_sensor"] = row["power_sensor"]
            if row["postcode_prefix"]:
                meta["postcode_prefix"] = row["postcode_prefix"]
            if row["v2x_capable"]:
                meta["v2x_capable"] = True
            try:
                if row["soc_sensor"]:
                    meta["soc_sensor"] = row["soc_sensor"]
                if row["meter_type"]:
                    meta["meter_type"] = row["meter_type"]
                if row["inverter_power_sensor"]:
                    meta["inverter_power_sensor"] = row["inverter_power_sensor"]
                if row["inverter_power_invert"]:
                    meta["inverter_power_invert"] = True
                if row["device_power_sensor"]:
                    meta["device_power_sensor"] = row["device_power_sensor"]
            except IndexError:
                pass  # older DB without these columns
        except IndexError:
            pass  # meters columns not present (e.g. get_last_block pre-join)

        meter_block = {
            "meta": meta,
            "interpolated":   bool(row["interpolated"]),
            "standing_charge": row["standing_charge"] or 0.0,
            "carbon_g":        row["carbon_g"],  # None when no CI data (pre-2.3.0)
            "channels":       {},
        }

        if row["imp_kwh"] is not None:
            imp_ch = {
                "kwh":        row["imp_kwh"],
                "rate":       row["imp_rate"],
                "cost":       row["imp_cost"],
                "read_start": row["imp_read_start"],
                "read_end":   row["imp_read_end"],
            }
            if row["imp_kwh_grid"] is not None:
                imp_ch["kwh_grid"] = row["imp_kwh_grid"]
            if row["imp_kwh_remainder"] is not None:
                imp_ch["kwh_remainder"] = row["imp_kwh_remainder"]
            if row["imp_cost_remainder"] is not None:
                imp_ch["cost_remainder"] = row["imp_cost_remainder"]
            meter_block["channels"]["import"] = imp_ch

        if row["exp_kwh"] is not None:
            meter_block["channels"]["export"] = {
                "kwh":        row["exp_kwh"],
                "rate":       row["exp_rate"],
                "cost":       row["exp_cost"],
                "read_start": row["exp_read_start"],
                "read_end":   row["exp_read_end"],
            }

        block["meters"][mid] = meter_block

        # accumulate totals (match engine.py PASS 3 logic)
        if row["imp_kwh_remainder"] is not None:
            block["totals"]["import_kwh"]  += row["imp_kwh_remainder"] or 0.0
            block["totals"]["import_cost"] += row["imp_cost_remainder"] or imp_cost
        elif row["imp_kwh_grid"] is not None:
            block["totals"]["import_kwh"]  += row["imp_kwh_grid"] or 0.0
            block["totals"]["import_cost"] += imp_cost
        else:
            block["totals"]["import_kwh"]  += imp_kwh
            block["totals"]["import_cost"] += imp_cost

        block["totals"]["export_kwh"]  += exp_kwh
        block["totals"]["export_cost"] += exp_cost

    return block


def _rows_to_blocks(rows: list[sqlite3.Row]) -> list[dict]:
    """Group DB rows by block_start and reconstruct block dicts."""
    grouped: dict[str, list] = {}
    for row in rows:
        key = row["block_start"]
        grouped.setdefault(key, []).append(row)
    return [_row_to_block(group) for group in grouped.values()]


# ─────────────────────────────────────────────────────────────────────────────
# BlockStore
# ─────────────────────────────────────────────────────────────────────────────


def config_meta_significant(old_config: dict, new_config: dict) -> bool:
    """
    Return True if the billing-significant meta fields have changed between
    two config snapshots. Sensor entity IDs, power sensor, postcode etc are
    NOT significant — only fields that affect billing calculations are.
    """
    SIGNIFICANT = ("billing_day", "block_minutes", "timezone",
                   "currency_symbol", "currency_code")

    def _main_meta(cfg):
        for m in cfg.get("meters", {}).values():
            if not (m.get("meta") or {}).get("sub_meter"):
                return m.get("meta") or {}
        return {}

    old_meta = _main_meta(old_config)
    new_meta = _main_meta(new_config)

    for key in SIGNIFICANT:
        if old_meta.get(key) != new_meta.get(key):
            return True
    return False

class BlockStore:
    """
    SQLite-backed block store.

    Thread safety: each instance holds one connection.  The engine runs on a
    single thread; the web server should open its own instance (SQLite WAL
    mode allows concurrent readers alongside one writer).
    """

    def __init__(self, db_path: str):
        self._path = db_path
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._apply_pragmas()
        self._ensure_schema()
        # Covering index for insights aggregation — created after migrations
        # so carbon_g and other late-added columns are guaranteed to exist
        # Covering index for all insights/summary queries — avoids row fetches.
        # Includes all columns used by _aggregate_insights, _aggregate_usage,
        # and api_blocks_summary. Rebuilt if schema changes (DROP + CREATE).
        _insights_idx_sql = (
            "CREATE INDEX IF NOT EXISTS idx_blocks_insights "
            "ON blocks (block_start, meter_id, config_period_id, "
            "imp_kwh, imp_kwh_grid, imp_kwh_remainder, "
            "imp_rate, imp_cost, imp_cost_remainder, "
            "exp_kwh, exp_cost, "
            "standing_charge, carbon_g)"
        )
        try:
            existing = self._conn.execute(
                "SELECT sql FROM sqlite_master WHERE type='index' AND name='idx_blocks_insights'"
            ).fetchone()
            if existing and existing[0] and "standing_charge" not in existing[0]:
                # Old narrow index — drop and recreate with full covering set
                with self._conn:
                    self._conn.execute("DROP INDEX IF EXISTS idx_blocks_insights")
            with self._conn:
                self._conn.execute(_insights_idx_sql)
        except Exception:
            pass
        # 2.8.0 — drop timezone-baked local date columns (now computed at query time)
        # Must drop indexes first — SQLite won't drop a column used in an index
        for _drop_idx in ("idx_blocks_date", "idx_blocks_ym"):
            try:
                with self._conn:
                    self._conn.execute(f"DROP INDEX IF EXISTS {_drop_idx}")
            except Exception:
                pass
        for _drop_col in ("local_date", "local_year", "local_month", "local_day"):
            try:
                cols = [r["name"] for r in self._conn.execute(
                    "PRAGMA table_info(blocks)"
                ).fetchall()]
                if _drop_col in cols:
                    with self._conn:
                        self._conn.execute(f"ALTER TABLE blocks DROP COLUMN {_drop_col}")
            except Exception as _e:
                pass  # SQLite < 3.35 or column already dropped
        # Migrate carbon_intensity: rename intensity → intensity_forecast, add intensity_actual
        try:
            cols = [r["name"] for r in self._conn.execute(
                "PRAGMA table_info(carbon_intensity)"
            ).fetchall()]
            if "intensity" in cols and "intensity_forecast" not in cols:
                with self._conn:
                    self._conn.execute(
                        "ALTER TABLE carbon_intensity RENAME COLUMN intensity TO intensity_forecast"
                    )
            if "intensity_actual" not in cols and "intensity_forecast" in cols + ["intensity_forecast"]:
                with self._conn:
                    self._conn.execute(
                        "ALTER TABLE carbon_intensity ADD COLUMN intensity_actual REAL"
                    )
        except Exception:
            pass

        # 2.8.0 — remove redundant generation_mix rows stored against sub-meter blocks.
        # Mix is a grid property; only the main meter block needs it.
        try:
            with self._conn:
                self._conn.execute("""
                    DELETE FROM generation_mix
                    WHERE block_id IN (
                        SELECT id FROM blocks WHERE meter_id != 'electricity_main'
                    )
                """)
        except Exception:
            pass

        try:
            self._conn.execute("""
                CREATE TABLE IF NOT EXISTS generation_mix (
                    block_id INTEGER NOT NULL REFERENCES blocks(id) ON DELETE CASCADE,
                    fuel     TEXT    NOT NULL,
                    perc     REAL    NOT NULL,
                    PRIMARY KEY (block_id, fuel)
                )
            """)
        except Exception:
            pass
        logger.debug("BlockStore opened: %s", db_path)

    # ── Connection management ─────────────────────────────────────────────

    def _apply_pragmas(self) -> None:
        self._conn.executescript("""
            PRAGMA journal_mode = WAL;
            PRAGMA synchronous  = NORMAL;
            PRAGMA cache_size   = -8000;
            PRAGMA temp_store   = MEMORY;
            PRAGMA foreign_keys = ON;
        """)

    def _ensure_schema(self) -> None:
        self._conn.executescript(_DDL)

        # ── Incremental column additions ──────────────────────────────────────
        # These run on every open so new columns are available immediately,
        # even before migrate_full_config_json() runs. ALTER TABLE IF NOT EXISTS
        # is not supported in SQLite < 3.37 so we check PRAGMA first.
        _m_cols  = {r[1] for r in self._conn.execute("PRAGMA table_info(meters)").fetchall()}
        _cp_cols = {r[1] for r in self._conn.execute("PRAGMA table_info(config_periods)").fetchall()}
        _mc_cols = {r[1] for r in self._conn.execute("PRAGMA table_info(meter_channels)").fetchall()}

        _b_cols  = {r[1] for r in self._conn.execute("PRAGMA table_info(blocks)").fetchall()}
        _ph_cols = {r[1] for r in self._conn.execute("PRAGMA table_info(power_history)").fetchall()} if self._conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='power_history'").fetchone() else set()

        for _col, _tbl, _defn, _col_set in [
            ("protected",            "meters",         "INTEGER DEFAULT 0",  _m_cols),
            ("inverter_possible",    "meters",         "INTEGER DEFAULT 0",  _m_cols),
            ("power_sensor",         "meters",         "TEXT",               _m_cols),
            ("postcode_prefix",      "meters",         "TEXT",               _m_cols),
            ("v2x_capable",          "meters",         "INTEGER DEFAULT 0",  _m_cols),
            ("inverter_power_invert","meters",         "INTEGER DEFAULT 0",  _m_cols),
            ("meter_type",           "meters",         "TEXT",               _m_cols),
            ("soc_sensor",           "meters",         "TEXT",               _m_cols),
            ("inverter_power_sensor","meters",         "TEXT",               _m_cols),
            ("device_power_sensor",  "meters",         "TEXT",               _m_cols),
            ("supplier",              "config_periods",  "TEXT",              _cp_cols),
            ("mpan",             "meter_channels",  "TEXT",              _mc_cols),
            ("tariff",           "meter_channels",  "TEXT",              _mc_cols),
            ("carbon_g",         "blocks",          "REAL",              _b_cols),
            ("carbon_gco2_min",  "power_history",   "REAL",              _ph_cols),
        ]:
            if _col not in _col_set:
                try:
                    self._conn.execute(f"ALTER TABLE {_tbl} ADD COLUMN {_col} {_defn}")
                    self._conn.commit()
                except Exception:
                    pass  # already exists or table missing — migrate will handle it

        # Create is_gap_seed index only if column exists (deferred for upgrade compat)
        cr_cols = [r[1] for r in self._conn.execute(
            "PRAGMA table_info(current_reads)"
        ).fetchall()]
        if "is_gap_seed" in cr_cols:
            self._conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_current_reads_gap "
                "ON current_reads (is_gap_seed)"
            )
        cur = self._conn.execute(
            "SELECT value FROM store_meta WHERE key = 'schema_version'"
        )
        row = cur.fetchone()
        if row is None:
            self._conn.execute(
                "INSERT INTO store_meta (key, value) VALUES ('schema_version', ?)",
                (str(SCHEMA_VERSION),)
            )
            self._conn.commit()

    # ── Carbon intensity ─────────────────────────────────────────────────────

    def upsert_carbon_intensity(self, captured_at: str, postcode: str,
                                 intensity_forecast: float | None,
                                 ci_index: str | None,
                                 intensity_actual: float | None = None) -> None:
        """
        Store a carbon intensity sample. captured_at is the 30-min slot boundary.
        intensity_forecast is always provided. intensity_actual is populated ~24hr
        later when NESO publishes actuals — only updates if provided (not None).
        """
        with self._conn:
            if intensity_actual is not None:
                self._conn.execute(
                    """INSERT INTO carbon_intensity
                           (captured_at, postcode, intensity_forecast, intensity_actual, ci_index)
                       VALUES (?, ?, ?, ?, ?)
                       ON CONFLICT(captured_at, postcode) DO UPDATE SET
                           intensity_forecast = COALESCE(excluded.intensity_forecast, intensity_forecast),
                           intensity_actual   = excluded.intensity_actual,
                           ci_index           = excluded.ci_index""",
                    (captured_at, postcode, intensity_forecast, intensity_actual, ci_index)
                )
            else:
                self._conn.execute(
                    """INSERT INTO carbon_intensity
                           (captured_at, postcode, intensity_forecast, ci_index)
                       VALUES (?, ?, ?, ?)
                       ON CONFLICT(captured_at, postcode) DO UPDATE SET
                           intensity_forecast = excluded.intensity_forecast,
                           ci_index           = excluded.ci_index""",
                    (captured_at, postcode, intensity_forecast, ci_index)
                )

    def get_nearest_carbon_intensity(self, block_start: str, postcode: str) -> dict | None:
        """
        Return the nearest carbon_intensity row to block_start for the given postcode.
        intensity = COALESCE(actual, forecast) — actual preferred when available.
        """
        row = self._conn.execute(
            """SELECT captured_at, ci_index,
                      COALESCE(intensity_actual, intensity_forecast) as intensity,
                      intensity_forecast,
                      intensity_actual
               FROM carbon_intensity
               WHERE postcode = ?
               ORDER BY ABS(strftime('%s', captured_at) - strftime('%s', ?))
               LIMIT 1""",
            (postcode, block_start)
        ).fetchone()
        return dict(row) if row else None

    def prune_carbon_intensity(self, days: int = 4) -> int:
        """Delete carbon_intensity rows older than `days` days. Returns rows deleted."""
        cutoff = (datetime.now(timezone.utc).replace(tzinfo=None)
                  - timedelta(days=days)).isoformat()
        with self._conn:
            cur = self._conn.execute(
                "DELETE FROM carbon_intensity WHERE captured_at < ?", (cutoff,)
            )
        return cur.rowcount

    # ── Generation mix ────────────────────────────────────────────────────────

    def upsert_generation_mix(self, block_id: int, mix: list[dict]) -> None:
        """
        Store generation mix for a block. mix is a list of {fuel, perc} dicts
        from the National Grid ESO API generationmix array.
        Replaces any existing mix for this block.
        """
        if not mix or block_id is None:
            return
        with self._conn:
            self._conn.execute(
                "DELETE FROM generation_mix WHERE block_id = ?", (block_id,)
            )
            self._conn.executemany(
                "INSERT INTO generation_mix (block_id, fuel, perc) VALUES (?, ?, ?)",
                [(block_id, str(row.get("fuel", "")).lower(), float(row.get("perc", 0)))
                 for row in mix if row.get("fuel") is not None]
            )

    def get_generation_mix(self, block_id: int) -> list[dict]:
        """Return generation mix for a block as [{fuel, perc}, ...] sorted by perc desc."""
        rows = self._conn.execute(
            "SELECT fuel, perc FROM generation_mix WHERE block_id = ? ORDER BY perc DESC",
            (block_id,)
        ).fetchall()
        return [{"fuel": r["fuel"], "perc": r["perc"]} for r in rows]

    def get_generation_mix_for_range(self, utc_start: str, utc_end: str,
                                      meter_id: str = "electricity_main") -> list[dict]:
        """
        Return imp_kwh-weighted average generation mix across all blocks in the
        given UTC range for the specified meter. Returns [{fuel, perc}, ...].
        Useful for period-level Insights aggregations.
        """
        rows = self._conn.execute(
            """SELECT gm.fuel,
                      SUM(gm.perc * b.imp_kwh) / NULLIF(SUM(b.imp_kwh), 0) as weighted_perc
               FROM generation_mix gm
               JOIN blocks b ON b.id = gm.block_id
               WHERE b.block_start >= ? AND b.block_start < ?
                 AND b.meter_id = ?
                 AND b.imp_kwh > 0
               GROUP BY gm.fuel
               ORDER BY weighted_perc DESC""",
            (utc_start, utc_end, meter_id)
        ).fetchall()
        return [{"fuel": r["fuel"], "perc": round(r["weighted_perc"], 2)} for r in rows]

    # ── Power history ─────────────────────────────────────────────────────────

    def append_power_history(self, captured_at: str, net_kw: float,
                              intensity: float | None,
                              carbon_gco2_min: float | None = None) -> None:
        """Append a power history row. Only call when power sensor is available."""
        with self._conn:
            self._conn.execute(
                """INSERT INTO power_history (captured_at, net_kw, intensity, carbon_gco2_min)
                   VALUES (?, ?, ?, ?)
                   ON CONFLICT(captured_at) DO UPDATE SET
                       net_kw          = excluded.net_kw,
                       intensity       = excluded.intensity,
                       carbon_gco2_min = excluded.carbon_gco2_min""",
                (captured_at, net_kw, intensity, carbon_gco2_min)
            )

    # ── Settings ──────────────────────────────────────────────────────────────

    def get_settings(self) -> dict:
        """Return all settings as a dict. Missing keys return defaults."""
        import json as _json
        row = self._conn.execute(
            "SELECT value FROM store_meta WHERE key = 'settings'"
        ).fetchone()
        if row and row['value']:
            try:
                return _json.loads(row['value'])
            except Exception:
                pass
        return {}

    def save_settings(self, settings: dict) -> None:
        """Persist settings dict to store_meta."""
        import json as _json
        self._conn.execute(
            "INSERT INTO store_meta (key, value) VALUES ('settings', ?) "
            "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            (_json.dumps(settings),)
        )
        self._conn.commit()

    def get_setting(self, key: str, default=None):
        """Return a single setting value by key."""
        return self.get_settings().get(key, default)

    def prune_power_history(self, hours: int = 48) -> int:
        """Delete power_history rows older than `hours` hours. Returns rows deleted."""
        cutoff = (datetime.now(timezone.utc).replace(tzinfo=None)
                  - timedelta(hours=hours)).isoformat()
        with self._conn:
            cur = self._conn.execute(
                "DELETE FROM power_history WHERE captured_at < ?", (cutoff,)
            )
        return cur.rowcount

    def get_power_history(self, hours: int = 48) -> list:
        """Return power_history rows for the last `hours` hours, oldest first."""
        cutoff = (datetime.now(timezone.utc).replace(tzinfo=None)
                  - timedelta(hours=hours)).isoformat()
        rows = self._conn.execute(
            """SELECT captured_at, net_kw, intensity, carbon_gco2_min
               FROM power_history
               WHERE captured_at >= ?
               ORDER BY captured_at ASC""",
            (cutoff,)
        ).fetchall()
        return [dict(r) for r in rows]

    def append_sub_meter_history(self, captured_at: str, meter_id: str,
                                soc_pct: float | None,
                                inverter_kw: float | None) -> None:
        """Append a battery history row for one battery meter."""
        with self._conn:
            self._conn.execute(
                """INSERT INTO sub_meter_history (captured_at, meter_id, soc_pct, inverter_kw)
                   VALUES (?, ?, ?, ?)""",
                (captured_at, meter_id, soc_pct, inverter_kw)
            )

    def prune_sub_meter_history(self, hours: int = 48) -> int:
        """Delete sub_meter_history rows older than `hours` hours. Returns rows deleted."""
        cutoff = (datetime.now(timezone.utc).replace(tzinfo=None)
                  - timedelta(hours=hours)).isoformat()
        with self._conn:
            cur = self._conn.execute(
                "DELETE FROM sub_meter_history WHERE captured_at < ?", (cutoff,)
            )
        return cur.rowcount

    def get_sub_meter_history(self, meter_id: str, hours: int = 48) -> list:
        """Return sub_meter_history rows for one meter for the last `hours` hours, oldest first."""
        cutoff = (datetime.now(timezone.utc).replace(tzinfo=None)
                  - timedelta(hours=hours)).isoformat()
        rows = self._conn.execute(
            """SELECT captured_at, soc_pct, inverter_kw
               FROM sub_meter_history
               WHERE meter_id = ? AND captured_at >= ?
               ORDER BY captured_at ASC""",
            (meter_id, cutoff)
        ).fetchall()
        return [dict(r) for r in rows]

    def close(self) -> None:
        try:
            self._conn.close()
        except Exception:
            pass

    def delete_config_period(self, period_id: int) -> dict:
        """
        Delete a config period.

        Blocks are always reassigned to the PREVIOUS period (older effective_from).
        If no previous period exists, they fall to the next period.

        Returns {"deleted": True, "blocks_reassigned": N}.
        Raises ValueError if the period does not exist or is the only period.
        """
        cp = self.get_config_period(period_id)
        if not cp:
            raise ValueError(f"Config period {period_id} not found")

        # Must not delete the only period
        cur = self._conn.execute("SELECT COUNT(*) FROM config_periods")
        if cur.fetchone()[0] <= 1:
            raise ValueError("Cannot remove the only config period")

        # Count blocks in this period
        cur = self._conn.execute(
            "SELECT COUNT(*) FROM blocks WHERE config_period_id = ?", (period_id,)
        )
        block_rows = cur.fetchone()[0]

        # Find the PREVIOUS period (strictly older effective_from)
        absorb_id = None
        cur = self._conn.execute(
            """SELECT id FROM config_periods
               WHERE effective_from < ? AND id != ?
               ORDER BY effective_from DESC LIMIT 1""",
            (cp["effective_from"], period_id)
        )
        row = cur.fetchone()
        if row:
            absorb_id = row["id"]

        # Fall back to next period if no previous exists
        if absorb_id is None:
            cur = self._conn.execute(
                """SELECT id FROM config_periods
                   WHERE id != ?
                   ORDER BY effective_from ASC LIMIT 1""",
                (period_id,)
            )
            row = cur.fetchone()
            if row:
                absorb_id = row["id"]

        with self._conn:
            # Reassign blocks to absorbing period
            if block_rows > 0 and absorb_id:
                self._conn.execute(
                    "UPDATE blocks SET config_period_id = ? WHERE config_period_id = ?",
                    (absorb_id, period_id)
                )

            # Fix the chain:
            # - Middle period: predecessor's effective_to becomes this period's effective_to
            # - Active (last) period: predecessor becomes active (effective_to = NULL)
            if cp["effective_to"] is not None:
                # Middle period — bridge predecessor to successor
                self._conn.execute(
                    "UPDATE config_periods SET effective_to = ? WHERE effective_to = ?",
                    (cp["effective_to"], cp["effective_from"])
                )
            else:
                # Active period — make predecessor active
                self._conn.execute(
                    "UPDATE config_periods SET effective_to = NULL WHERE effective_to = ?",
                    (cp["effective_from"],)
                )

            # Delete normalised meter rows before removing the period
            # (FK constraint: meters.config_period_id → config_periods.id)
            meter_ids = [r["id"] for r in self._conn.execute(
                "SELECT id FROM meters WHERE config_period_id = ?", (period_id,)
            ).fetchall()]
            for mid in meter_ids:
                self._conn.execute(
                    "DELETE FROM meter_channels WHERE meter_id = ?", (mid,)
                )
            self._conn.execute(
                "DELETE FROM meters WHERE config_period_id = ?", (period_id,)
            )
            self._conn.execute(
                "DELETE FROM config_periods WHERE id = ?", (period_id,)
            )

        logger.info(
            "delete_config_period: id=%d deleted, %d blocks reassigned to id=%s",
            period_id, block_rows, absorb_id
        )
        return {"deleted": True, "blocks_reassigned": block_rows}


    def delete_blocks_for_date_range(
        self, from_date: str, to_date: str, meter_id: str | None = None,
        from_time: str = "00:00", to_time: str = "23:59", tz_name: str = "UTC"
    ) -> dict:
        """
        Delete all blocks within [from_date, to_date] local date range (inclusive),
        optionally restricted by time-of-day (UTC HH:MM) within those dates.
        Uses UTC bounds derived from local dates via local_date_range_to_utc_bounds().
        """
        if not from_date or not to_date:
            raise ValueError("from_date and to_date are required")
        if from_date > to_date:
            raise ValueError("from_date must not be after to_date")

        from_time = from_time or "00:00"
        to_time   = to_time   or "23:59"

        utc_start, utc_end = local_date_range_to_utc_bounds(from_date, to_date, tz_name)

        clauses = ["block_start >= ?", "block_start < ?"]
        params  = [utc_start, utc_end]

        if from_time != "00:00" and to_time != "23:59":
            if from_time <= to_time:
                clauses.append("TIME(block_start) >= ?")
                clauses.append("TIME(block_start) <= ?")
                params.extend([from_time, to_time])
            else:
                clauses.append("(TIME(block_start) >= ? OR TIME(block_start) <= ?)")
                params.extend([from_time, to_time])
        elif from_time != "00:00":
            clauses.append("TIME(block_start) >= ?")
            params.append(from_time)
        elif to_time != "23:59":
            clauses.append("TIME(block_start) <= ?")
            params.append(to_time)

        if meter_id:
            clauses.append("meter_id = ?")
            params.append(meter_id)

        where = " AND ".join(clauses)

        cur = self._conn.execute(
            f"SELECT COUNT(*) as n, COUNT(DISTINCT date(block_start)) as d FROM blocks WHERE {where}",
            params
        )
        row = cur.fetchone()
        n_blocks = row["n"]
        n_dates  = row["d"]

        with self._conn:
            self._conn.execute(f"DELETE FROM blocks WHERE {where}", params)

        return {"deleted": n_blocks, "dates": n_dates}

    def count_blocks_for_date_range(
        self, from_date: str, to_date: str, meter_id: str | None = None,
        from_time: str = "00:00", to_time: str = "23:59", tz_name: str = "UTC"
    ) -> dict:
        """
        Preview how many blocks would be affected by delete_blocks_for_date_range.
        Returns {"blocks": N, "dates": N_distinct_dates}.
        """
        from_time = from_time or "00:00"
        to_time   = to_time   or "23:59"

        utc_start, utc_end = local_date_range_to_utc_bounds(from_date, to_date, tz_name)

        clauses = ["block_start >= ?", "block_start < ?"]
        params  = [utc_start, utc_end]

        if from_time != "00:00" and to_time != "23:59":
            if from_time <= to_time:
                clauses.append("TIME(block_start) >= ?")
                clauses.append("TIME(block_start) <= ?")
                params.extend([from_time, to_time])
            else:
                clauses.append("(TIME(block_start) >= ? OR TIME(block_start) <= ?)")
                params.extend([from_time, to_time])
        elif from_time != "00:00":
            clauses.append("TIME(block_start) >= ?")
            params.append(from_time)
        elif to_time != "23:59":
            clauses.append("TIME(block_start) <= ?")
            params.append(to_time)

        if meter_id:
            clauses.append("meter_id = ?")
            params.append(meter_id)

        where = " AND ".join(clauses)

        cur = self._conn.execute(
            f"SELECT COUNT(*) as n, COUNT(DISTINCT date(block_start)) as d FROM blocks WHERE {where}",
            params
        )
        row = cur.fetchone()
        return {"blocks": row["n"], "dates": row["d"]}


    def backup(self, dst_path: str) -> None:
        """Hot backup to dst_path using SQLite's online backup API."""
        dst = sqlite3.connect(dst_path)
        try:
            self._conn.backup(dst, pages=100, sleep=0.05)
        finally:
            dst.close()

    # ── Config periods ────────────────────────────────────────────────────

    def get_current_config_period_id(self) -> Optional[int]:
        """Return the id of the currently active config period, or None."""
        cur = self._conn.execute(
            "SELECT id FROM config_periods WHERE effective_to IS NULL "
            "ORDER BY effective_from DESC LIMIT 1"
        )
        row = cur.fetchone()
        return row["id"] if row else None

    def get_config_period(self, period_id: int) -> Optional[dict]:
        """Return a config period row as a dict."""
        cur = self._conn.execute(
            "SELECT * FROM config_periods WHERE id = ?", (period_id,)
        )
        row = cur.fetchone()
        return dict(row) if row else None

    def get_config_periods(self) -> list:
        """Return all config periods ordered by effective_from, with first/last block dates."""
        cur = self._conn.execute(
            """SELECT id, effective_from, effective_to, billing_day, block_minutes,
                      timezone, currency_symbol, currency_code, site_name
               FROM config_periods ORDER BY effective_from ASC"""
        )
        periods = [dict(row) for row in cur.fetchall()]

        # Attach first and last block_start for each period (used by billing period calc)
        for p in periods:
            c = self._conn.execute(
                """SELECT MIN(block_start) as first_bs, MAX(block_start) as last_bs
                   FROM blocks WHERE config_period_id = ?""",
                (p["id"],)
            )
            row = c.fetchone()
            p["first_block_start"] = row["first_bs"] if row else None
            p["last_block_start"]  = row["last_bs"]  if row else None
        return periods



    def get_billing_totals_for_utc_range(self, utc_start: str, utc_end: str,
                                          tz_name: str = "UTC") -> dict:
        """
        UTC-based replacement for get_billing_totals_for_local_date_range.
        Filters on block_start >= utc_start AND block_start < utc_end.

        Use local_date_range_to_utc_bounds() to compute utc_start/utc_end
        from local date strings. Handles DST correctly since bounds are
        computed per-date using the configured timezone.

        Standing charge is summed per distinct local day — computed at
        query time using strftime with the configured timezone offset.
        """
        active_period_sq = (
            "SELECT id FROM config_periods "
            "WHERE effective_to IS NULL ORDER BY effective_from DESC LIMIT 1"
        )
        cur = self._conn.execute(
            f"""SELECT
                 COALESCE(SUM(
                   CASE
                     WHEN m.is_sub_meter = 0 THEN
                       CASE
                         WHEN b.imp_kwh_remainder IS NOT NULL THEN b.imp_kwh_remainder
                         WHEN b.imp_kwh_grid      IS NOT NULL THEN b.imp_kwh_grid
                         ELSE b.imp_kwh
                       END
                     ELSE COALESCE(b.imp_kwh_grid, 0)
                   END
                 ), 0.0) as imp_kwh,
                 COALESCE(SUM(CASE WHEN m.is_sub_meter = 0 THEN b.imp_cost ELSE 0 END), 0.0) as imp_cost,
                 COALESCE(SUM(CASE WHEN m.is_sub_meter = 0 THEN b.exp_kwh  ELSE 0 END), 0.0) as exp_kwh,
                 COALESCE(SUM(CASE WHEN m.is_sub_meter = 0 THEN b.exp_cost ELSE 0 END), 0.0) as exp_cost
               FROM blocks b
               JOIN meters m
                 ON m.meter_id = b.meter_id
                AND m.config_period_id = ({active_period_sq})
               WHERE b.block_start >= ? AND b.block_start < ?""",
            (utc_start, utc_end)
        )
        row = cur.fetchone()

        # Standing charge: once per distinct local day.
        # Convert block_start to local date using the timezone offset at query time.
        try:
            tz = ZoneInfo(tz_name)
        except Exception:
            tz = ZoneInfo("UTC")

        # Fetch all main-meter blocks in range and group by local date in Python
        # (SQLite cannot do timezone-aware date conversion natively)
        sc_rows = self._conn.execute(
            f"""SELECT b.block_start, b.standing_charge
               FROM blocks b
               JOIN meters m ON m.meter_id = b.meter_id
                 AND m.config_period_id = ({active_period_sq})
               WHERE b.block_start >= ? AND b.block_start < ?
                 AND m.is_sub_meter = 0
               ORDER BY b.block_start""",
            (utc_start, utc_end)
        ).fetchall()

        # Group by local date, take MAX standing_charge per day
        daily_sc: dict = {}
        for sc_row in sc_rows:
            local_d = (datetime.fromisoformat(sc_row["block_start"])
                       .replace(tzinfo=ZoneInfo("UTC"))
                       .astimezone(tz)
                       .strftime("%Y-%m-%d"))
            sc_val = float(sc_row["standing_charge"] or 0)
            if local_d not in daily_sc or sc_val > daily_sc[local_d]:
                daily_sc[local_d] = sc_val
        standing = round(sum(daily_sc.values()), 4)

        return {
            "imp_kwh":  round(float(row["imp_kwh"]  or 0), 4),
            "imp_cost": round(float(row["imp_cost"] or 0), 4),
            "exp_kwh":  round(float(row["exp_kwh"]  or 0), 4),
            "exp_cost": round(float(row["exp_cost"] or 0), 4),
            "standing": standing,
        }

    # ── Current block (replaces current_block.json) ──────────────────────────

    def save_current_block(self, block: dict) -> None:
        """
        Persist the in-progress block state to the current_block table.
        Replaces current_block.json.

        Gap marker state (previously a JSON blob) is now stored relationally:
          - current_block.gap_detected_at: timestamp when gap was detected
          - current_reads rows with is_gap_seed=1 (kWh) or 2 (rate): the
            pre-gap readings used to interpolate missing blocks

        block: the engine's current_block dict containing:
          start, end, interpolated, _last_checkpoint,
          _gap_marker (optional: {detected_at, pre_reads, last_known_rates}),
          meters[meter_id].channels[channel].reads[], .rates[]
        """
        block_start     = block.get("start")
        block_end       = block.get("end")
        last_checkpoint      = block.get("_last_checkpoint")
        gap_marker           = block.get("_gap_marker")
        gap_detected_at      = (gap_marker or {}).get("detected_at") if gap_marker else None
        gap_last_block_start = (gap_marker or {}).get("last_block_start") if gap_marker else None
        interpolated         = 1 if block.get("interpolated") else 0

        with self._conn:
            self._conn.execute(
                """INSERT INTO current_block
                       (id, block_start, block_end, last_checkpoint, gap_detected_at,
                        gap_last_block_start, interpolated)
                   VALUES (1, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT(id) DO UPDATE SET
                       block_start          = excluded.block_start,
                       block_end            = excluded.block_end,
                       last_checkpoint      = excluded.last_checkpoint,
                       gap_detected_at      = excluded.gap_detected_at,
                       gap_last_block_start = excluded.gap_last_block_start,
                       interpolated         = excluded.interpolated""",
                (block_start, block_end, last_checkpoint, gap_detected_at,
                 gap_last_block_start, interpolated)
            )

            # Replace all current_reads with the rolling buffer + gap seed rows
            self._conn.execute("DELETE FROM current_reads")
            rows = []

            # Live reads and rates from the rolling buffer (is_gap_seed=0)
            for meter_id, meter_data in (block.get("meters") or {}).items():
                sc = float((meter_data or {}).get("standing_charge") or 0.0)
                for channel, ch_data in ((meter_data or {}).get("channels") or {}).items():
                    for r in (ch_data.get("reads") or []):
                        rows.append((r.get("ts"), meter_id, channel, "read",
                                     float(r.get("value", 0)), sc, 0))
                    for r in (ch_data.get("rates") or []):
                        rows.append((r.get("ts"), meter_id, channel, "rate",
                                     float(r.get("value", 0)), None, 0))

            # Gap seed rows from _gap_marker (is_gap_seed=1 for reads, 2 for rates)
            if gap_marker:
                for meter_id, channels in (gap_marker.get("pre_reads") or {}).items():
                    for channel, r in (channels or {}).items():
                        if r:
                            rows.append((r.get("ts"), meter_id, channel, "read",
                                         float(r.get("value", 0)), None, 1))
                for meter_id, channels in (gap_marker.get("last_known_rates") or {}).items():
                    for channel, r in (channels or {}).items():
                        if r:
                            rows.append((r.get("ts"), meter_id, channel, "rate",
                                         float(r.get("value", 0)), None, 2))

            if rows:
                self._conn.executemany(
                    """INSERT INTO current_reads
                           (captured_at, meter_id, channel, channel_type,
                            value, standing_charge, is_gap_seed)
                       VALUES (?, ?, ?, ?, ?, ?, ?)""",
                    rows
                )

    def load_current_block(self) -> dict:
        """
        Load the in-progress block state from the DB.
        Returns a block dict in the same shape the engine expects,
        or {} if no current block exists.

        Gap marker is reconstructed from gap_detected_at and is_gap_seed rows.
        """
        from collections import defaultdict

        row = self._conn.execute(
            "SELECT * FROM current_block WHERE id = 1"
        ).fetchone()

        if not row or not row["block_start"]:
            return {}

        block = {
            "start":            row["block_start"],
            "end":              row["block_end"],
            "interpolated":     bool(row["interpolated"]),
            "_last_checkpoint": row["last_checkpoint"],
            "meters":           {},
        }

        # Load all current_reads rows — live and gap seed
        reads_cur = self._conn.execute(
            """SELECT meter_id, channel, channel_type, value,
                      standing_charge, captured_at, is_gap_seed
               FROM current_reads ORDER BY captured_at ASC"""
        )
        all_rows = reads_cur.fetchall()

        # Live reads (is_gap_seed=0) → reconstruct meters/channels
        meter_reads = defaultdict(lambda: defaultdict(list))
        meter_rates = defaultdict(lambda: defaultdict(list))
        meter_sc    = {}
        for r in all_rows:
            if r["is_gap_seed"] != 0:
                continue
            mid = r["meter_id"]; ch = r["channel"]
            entry = {"ts": r["captured_at"], "value": r["value"]}
            if r["channel_type"] == "read":
                meter_reads[mid][ch].append(entry)
                if r["standing_charge"] is not None:
                    meter_sc[mid] = r["standing_charge"]
            else:
                meter_rates[mid][ch].append(entry)

        all_meter_ids = set(list(meter_reads.keys()) + list(meter_rates.keys()))
        for mid in all_meter_ids:
            channels = {}
            for ch in set(list(meter_reads[mid].keys()) + list(meter_rates[mid].keys())):
                channels[ch] = {
                    "reads": meter_reads[mid].get(ch, []),
                    "rates": meter_rates[mid].get(ch, []),
                }
            block["meters"][mid] = {
                "channels": channels,
                "standing_charge": meter_sc.get(mid, 0.0),
                "meta": {},
            }

        # Reconstruct _gap_marker from gap_detected_at + gap seed rows
        if row["gap_detected_at"]:
            pre_reads       = defaultdict(dict)
            last_known_rates = defaultdict(dict)
            for r in all_rows:
                if r["is_gap_seed"] == 1:  # gap seed kWh read
                    pre_reads[r["meter_id"]][r["channel"]] = {
                        "ts": r["captured_at"], "value": r["value"]
                    }
                elif r["is_gap_seed"] == 2:  # gap seed rate
                    last_known_rates[r["meter_id"]][r["channel"]] = {
                        "ts": r["captured_at"], "value": r["value"]
                    }
            block["_gap_marker"] = {
                "detected_at":      row["gap_detected_at"],
                "pre_reads":        dict(pre_reads),
                "last_known_rates": dict(last_known_rates),
                "last_block_start": row["gap_last_block_start"],
            }

        return block

    def clear_current_block(self) -> None:
        """Clear the in-progress block state (e.g. after a reset)."""
        with self._conn:
            self._conn.execute("DELETE FROM current_block")
            self._conn.execute("DELETE FROM current_reads")

    def get_cumulative_totals(self) -> dict:
        """
        Return lifetime cumulative totals for HA sensor publishing.

        Mirrors engine PASS 3 logic exactly:
          - Main meter (is_sub_meter=0): use imp_kwh_remainder if available
            (house-only grid load after sub-meters claimed their share),
            falling back to imp_kwh (correct when no sub-meters configured).
          - Sub-meters (is_sub_meter=1): use imp_kwh_grid if available
            (the portion the sub-meter drew from the grid rather than from
            solar/battery), falling back to imp_kwh.

        This avoids double-counting: electricity_main.imp_kwh already
        includes EV charger and battery consumption; adding sub-meter
        imp_kwh on top inflates the total by the sub-meter kWh.

        export_kwh/imp_cost/exp_cost: main meter only (sub-meters don't
        have independent costs or export).
        """
        active_period_sq = (
            "SELECT id FROM config_periods "
            "WHERE effective_to IS NULL ORDER BY effective_from DESC LIMIT 1"
        )
        cur = self._conn.execute(
            f"""SELECT
                 COALESCE(SUM(
                   CASE
                     WHEN m.is_sub_meter = 0 THEN
                       CASE
                         WHEN b.imp_kwh_remainder IS NOT NULL THEN b.imp_kwh_remainder
                         WHEN b.imp_kwh_grid      IS NOT NULL THEN b.imp_kwh_grid
                         ELSE b.imp_kwh
                       END
                     ELSE COALESCE(b.imp_kwh_grid, 0)  -- sub-meter: grid only, no raw fallback
                   END
                 ), 0.0) as import_kwh,
                 COALESCE(SUM(CASE WHEN m.is_sub_meter = 0 THEN b.exp_kwh  ELSE 0 END), 0.0) as export_kwh,
                 COALESCE(SUM(CASE WHEN m.is_sub_meter = 0 THEN b.imp_cost ELSE 0 END), 0.0) as import_cost,
                 COALESCE(SUM(CASE WHEN m.is_sub_meter = 0 THEN b.exp_cost ELSE 0 END), 0.0) as export_cost
               FROM blocks b
               JOIN meters m
                 ON m.meter_id = b.meter_id
                AND m.config_period_id = ({active_period_sq})"""
        )
        row = cur.fetchone()
        return {
            "import_kwh":  round(float(row["import_kwh"]),  6),
            "export_kwh":  round(float(row["export_kwh"]),  6),
            "import_cost": round(float(row["import_cost"]), 6),
            "export_cost": round(float(row["export_cost"]), 6),
        }


    def get_config_period_for_date(self, date_iso: str) -> Optional[dict]:
        """
        Return the config period that was active on a given date (YYYY-MM-DD).
        Used by billing calculations to get the historically correct billing_day.
        """
        cur = self._conn.execute(
            """
            SELECT * FROM config_periods
            WHERE effective_from <= ?
              AND (effective_to IS NULL OR effective_to > ?)
            ORDER BY effective_from DESC
            LIMIT 1
            """,
            (date_iso, date_iso)
        )
        row = cur.fetchone()
        return dict(row) if row else None

    def _snap_to_midnight_utc(self, raw_from: str, tz_name: str) -> str:
        """Snap a UTC ISO timestamp to local midnight, returned as UTC ISO."""
        try:
            from zoneinfo import ZoneInfo as _ZI
            from datetime import datetime as _dt2, timezone as _tz2
            raw_dt = _dt2.fromisoformat(raw_from.replace(" ", "T").split(".")[0])
            raw_dt_utc = raw_dt.replace(tzinfo=_tz2.utc)
            local_dt = raw_dt_utc.astimezone(_ZI(tz_name))
            midnight_local = local_dt.replace(hour=0, minute=0, second=0, microsecond=0)
            return midnight_local.astimezone(_tz2.utc).replace(tzinfo=None).strftime(
                "%Y-%m-%dT%H:%M:%S"
            )
        except Exception:
            return raw_from.replace(" ", "T").split(".")[0]

    def _write_meters(self, config_json: dict, period_id: int) -> None:
        """
        Upsert meters and meter_channels rows
        for a config period from a config dict.
        """
        for meter_id, meter_cfg in (config_json.get("meters") or {}).items():
            meta = meter_cfg.get("meta") or {}
            is_sub      = 1 if meta.get("sub_meter") else 0
            parent      = meta.get("parent_meter")
            device      = meta.get("device")
            protected   = 1 if meta.get("protected") else 0
            inv_poss    = 1 if (meta.get("meter_type") == "battery" or meta.get("inverter_possible")) else 0
            power_s     = meta.get("power_sensor")
            postcode    = meta.get("postcode_prefix")
            v2x         = 1 if meta.get("v2x_capable") else 0
            meter_type  = meta.get("meter_type")
            soc_s       = meta.get("soc_sensor")
            inv_pwr_s    = meta.get("inverter_power_sensor")
            inv_invert   = 1 if meta.get("inverter_power_invert") else 0
            dev_pwr_s   = meta.get("device_power_sensor")

            cur = self._conn.execute(
                """INSERT INTO meters
                       (config_period_id, meter_id, is_sub_meter, parent_meter_id,
                        device_label, protected, inverter_possible,
                        power_sensor, postcode_prefix, v2x_capable,
                        meter_type, soc_sensor, inverter_power_sensor, inverter_power_invert,
                        device_power_sensor)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT(config_period_id, meter_id) DO UPDATE SET
                       is_sub_meter            = excluded.is_sub_meter,
                       parent_meter_id         = excluded.parent_meter_id,
                       device_label            = excluded.device_label,
                       protected               = excluded.protected,
                       inverter_possible       = excluded.inverter_possible,
                       power_sensor            = excluded.power_sensor,
                       postcode_prefix         = excluded.postcode_prefix,
                       v2x_capable             = excluded.v2x_capable,
                       meter_type              = excluded.meter_type,
                       soc_sensor              = excluded.soc_sensor,
                       inverter_power_sensor   = excluded.inverter_power_sensor,
                       inverter_power_invert   = excluded.inverter_power_invert,
                       device_power_sensor     = excluded.device_power_sensor""",
                (period_id, meter_id, is_sub, parent, device,
                 protected, inv_poss, power_s, postcode, v2x,
                 meter_type, soc_s, inv_pwr_s, inv_invert, dev_pwr_s)
            )
            meter_row_id = cur.lastrowid or self._conn.execute(
                "SELECT id FROM meters WHERE config_period_id=? AND meter_id=?",
                (period_id, meter_id)
            ).fetchone()["id"]

            for channel, ch_cfg in (meter_cfg.get("channels") or {}).items():
                ch_meta = ch_cfg.get("meta") or {}
                self._conn.execute(
                    """INSERT INTO meter_channels
                           (meter_id, channel, read_sensor, rate_sensor,
                            standing_charge_sensor, mpan, tariff)
                       VALUES (?, ?, ?, ?, ?, ?, ?)
                       ON CONFLICT(meter_id, channel) DO UPDATE SET
                           read_sensor            = excluded.read_sensor,
                           rate_sensor            = excluded.rate_sensor,
                           standing_charge_sensor = excluded.standing_charge_sensor,
                           mpan                   = excluded.mpan,
                           tariff                 = excluded.tariff""",
                    (
                        meter_row_id, channel,
                        ch_cfg.get("read"),
                        ch_cfg.get("rate"),
                        ch_cfg.get("standing_charge_sensor"),
                        ch_meta.get("mpan"),
                        ch_meta.get("tariff"),
                    )
                )

    def config_from_db(self, period_id: int) -> dict:
        """
        Reconstruct a config dict (matching the old meters_config.json shape)
        from the normalised meters/meter_channels tables.
        """
        cp = self.get_config_period(period_id)
        if not cp:
            return {"schema_version": "1.0", "meters": {}}

        meters_out = {}
        m_rows = self._conn.execute(
            "SELECT * FROM meters WHERE config_period_id=? ORDER BY id",
            (period_id,)
        ).fetchall()

        for m in m_rows:
            mid = m["meter_id"]
            meta = {
                "billing_day":    cp["billing_day"],
                "block_minutes":  cp["block_minutes"],
                "timezone":       cp["timezone"],
                "currency_symbol": cp["currency_symbol"],
                "currency_code":  cp["currency_code"],
                "site":           cp["site_name"],
            }
            if cp["supplier"]:
                meta["supplier"] = cp["supplier"]
            if m["is_sub_meter"]:
                meta["sub_meter"] = True
            if m["parent_meter_id"]:
                meta["parent_meter"] = m["parent_meter_id"]
            if m["device_label"]:
                meta["device"] = m["device_label"]
            if m["protected"]:
                meta["protected"] = True
            if m["inverter_possible"]:
                meta["inverter_possible"] = True
            if m["power_sensor"]:
                meta["power_sensor"] = m["power_sensor"]
            if m["postcode_prefix"]:
                meta["postcode_prefix"] = m["postcode_prefix"]
            if m["v2x_capable"]:
                meta["v2x_capable"] = True
            try:
                if m["meter_type"]:
                    meta["meter_type"] = m["meter_type"]
                if m["soc_sensor"]:
                    meta["soc_sensor"] = m["soc_sensor"]
                if m["inverter_power_sensor"]:
                    meta["inverter_power_sensor"] = m["inverter_power_sensor"]
                if m["inverter_power_invert"]:
                    meta["inverter_power_invert"] = True
                if m["device_power_sensor"]:
                    meta["device_power_sensor"] = m["device_power_sensor"]
            except IndexError:
                pass

            channels = {}
            ch_rows = self._conn.execute(
                "SELECT * FROM meter_channels WHERE meter_id=?", (m["id"],)
            ).fetchall()
            for ch in ch_rows:
                ch_dict = {}
                if ch["read_sensor"]:
                    ch_dict["read"] = ch["read_sensor"]
                if ch["rate_sensor"]:
                    ch_dict["rate"] = ch["rate_sensor"]
                if ch["standing_charge_sensor"]:
                    ch_dict["standing_charge_sensor"] = ch["standing_charge_sensor"]
                # mpan / tariff as channel meta dict (preserves engine-expected shape)
                ch_meta = {}
                if ch["mpan"]:
                    ch_meta["mpan"] = ch["mpan"]
                if ch["tariff"]:
                    ch_meta["tariff"] = ch["tariff"]
                if ch_meta:
                    ch_dict["meta"] = ch_meta
                channels[ch["channel"]] = ch_dict

            meters_out[mid] = {"meta": meta, "channels": channels}

        return {"schema_version": "1.0", "meters": meters_out}

    def insert_config_period(self,
                             config_json: dict,
                             effective_from: Optional[str] = None,
                             change_reason: Optional[str] = None) -> int:
        """
        Snapshot the current config as a new config period.
        Closes the previous period's effective_to.
        Writes meter definitions to the normalised meters/meter_channels tables.
        Returns the new period's id.
        """
        main_meta = {}
        for m in config_json.get("meters", {}).values():
            if not (m.get("meta") or {}).get("sub_meter"):
                main_meta = m.get("meta") or {}
                break

        tz_name = main_meta.get("timezone", "UTC")
        now = self._snap_to_midnight_utc(effective_from or _utc_now_iso(), tz_name)

        with self._conn:
            self._conn.execute(
                "UPDATE config_periods SET effective_to = ? WHERE effective_to IS NULL",
                (now,)
            )
            cur = self._conn.execute(
                """INSERT INTO config_periods
                       (effective_from, effective_to, billing_day, block_minutes,
                        timezone, currency_symbol, currency_code, site_name,
                        supplier, change_reason)
                   VALUES (?, NULL, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    now,
                    int(main_meta.get("billing_day") or 1),
                    int(main_meta.get("block_minutes") or 30),
                    main_meta.get("timezone", "UTC"),
                    main_meta.get("currency_symbol", "£"),
                    main_meta.get("currency_code", "GBP"),
                    main_meta.get("site", main_meta.get("site_name")),
                    main_meta.get("supplier"),
                    change_reason,
                )
            )
            period_id = cur.lastrowid
            self._write_meters(config_json, period_id)

        logger.info("insert_config_period: new period id=%d effective_from=%s", period_id, now)
        return period_id

    def migrate_full_config_json(self) -> int:
        """
        One-time migration for 2.0→2.1 upgrade. Safe to call on every startup.

        Steps (each guarded independently so partial upgrades resume correctly):
          1. full_config_json → normalised meters/meter_channels + column drop
          2. gap_marker blob  → gap_detected_at column + is_gap_seed rows
          3. is_gap_seed column added to current_reads if missing
          4. mpan/tariff columns added to meter_channels if missing

        Returns the number of config periods whose meters were migrated (step 1).
        """
        migrated = 0

        # ── Step 0: upgrade meters table schema if it has the old 2.0.x shape ──
        # The 2.0.x meters table lacked protected, inverter_possible,
        # power_sensor, postcode_prefix. Recreate with the full 2.1.0 schema.
        m_cols = {r[1] for r in self._conn.execute(
            "PRAGMA table_info(meters)"
        ).fetchall()}
        if m_cols and "protected" not in m_cols:
            try:
                with self._conn:
                    self._conn.execute("""
                        CREATE TABLE IF NOT EXISTS meters_new (
                            id                 INTEGER PRIMARY KEY AUTOINCREMENT,
                            config_period_id   INTEGER NOT NULL,
                            meter_id           TEXT    NOT NULL,
                            is_sub_meter       INTEGER NOT NULL DEFAULT 0,
                            parent_meter_id    TEXT,
                            device_label       TEXT,
                            protected          INTEGER DEFAULT 0,
                            inverter_possible  INTEGER DEFAULT 0,
                            power_sensor       TEXT,
                            postcode_prefix    TEXT,
                            supplier           TEXT,
                            v2x_capable        INTEGER DEFAULT 0,
                            meter_type         TEXT,
                            soc_sensor         TEXT,
                            inverter_power_sensor TEXT,
                            device_power_sensor TEXT,
                            FOREIGN KEY (config_period_id) REFERENCES config_periods(id),
                            UNIQUE (config_period_id, meter_id)
                        )
                    """)
                    # Copy compatible columns from old meters table
                    old_m_cols = {r[1] for r in self._conn.execute(
                        "PRAGMA table_info(meters)"
                    ).fetchall()}
                    common = {"id", "config_period_id", "meter_id", "is_sub_meter",
                              "parent_meter_id", "device_label"} & old_m_cols
                    col_list = ", ".join(sorted(common))
                    self._conn.execute(f"""
                        INSERT INTO meters_new ({col_list})
                        SELECT {col_list} FROM meters
                    """)
                    self._conn.execute("DROP TABLE meters")
                    self._conn.execute("ALTER TABLE meters_new RENAME TO meters")
                logger.info("migrate_full_config_json: upgraded meters table schema")
            except Exception as _e:
                logger.warning(
                    "migrate_full_config_json: meters schema upgrade failed: %s", _e
                )

        # ── Step 0b: add v2x_capable to meters if missing ───────────────────
        # Must run before Step 1 (_write_meters) which INSERTs this column.
        m_cols_now = {r[1] for r in self._conn.execute(
            "PRAGMA table_info(meters)"
        ).fetchall()}
        for _col, _defn in [("v2x_capable", "INTEGER DEFAULT 0")]:
            if _col not in m_cols_now:
                try:
                    with self._conn:
                        self._conn.execute(
                            f"ALTER TABLE meters ADD COLUMN {_col} {_defn}"
                        )
                    logger.info(
                        "migrate_full_config_json: added %s to meters", _col
                    )
                except Exception as _e:
                    logger.warning(
                        "migrate_full_config_json: %s column add failed: %s", _col, _e
                    )

        # ── Step 1: full_config_json → normalised meter tables ───────────────
        cp_cols = [r[1] for r in self._conn.execute(
            "PRAGMA table_info(config_periods)"
        ).fetchall()]
        if "full_config_json" in cp_cols:
            rows = self._conn.execute(
                "SELECT id, full_config_json FROM config_periods "
                "WHERE full_config_json IS NOT NULL AND full_config_json != ''"
            ).fetchall()
            for row in rows:
                period_id = row["id"]
                existing = self._conn.execute(
                    "SELECT COUNT(*) FROM meters WHERE config_period_id=?", (period_id,)
                ).fetchone()[0]
                if existing:
                    continue
                try:
                    cfg = json.loads(row["full_config_json"])
                    with self._conn:
                        self._write_meters(cfg, period_id)
                    migrated += 1
                except Exception as e:
                    logger.warning(
                        "migrate_full_config_json: period %d failed: %s", period_id, e
                    )
            try:
                # Temporarily disable FK enforcement for table recreation
                self._conn.execute("PRAGMA foreign_keys = OFF")
                with self._conn:
                    self._conn.execute("""
                        CREATE TABLE IF NOT EXISTS config_periods_new (
                            id               INTEGER PRIMARY KEY AUTOINCREMENT,
                            effective_from   TEXT    NOT NULL,
                            effective_to     TEXT,
                            billing_day      INTEGER NOT NULL DEFAULT 1,
                            block_minutes    INTEGER NOT NULL DEFAULT 30,
                            timezone         TEXT    NOT NULL DEFAULT 'UTC',
                            currency_symbol  TEXT    NOT NULL DEFAULT '£',
                            currency_code    TEXT    NOT NULL DEFAULT 'GBP',
                            site_name        TEXT,
                            change_reason    TEXT
                        )
                    """)
                    self._conn.execute("""
                        INSERT INTO config_periods_new
                            (id, effective_from, effective_to, billing_day, block_minutes,
                             timezone, currency_symbol, currency_code, site_name, change_reason)
                        SELECT id, effective_from, effective_to, billing_day, block_minutes,
                               timezone, currency_symbol, currency_code, site_name, change_reason
                        FROM config_periods
                    """)
                    self._conn.execute("DROP TABLE config_periods")
                    self._conn.execute(
                        "ALTER TABLE config_periods_new RENAME TO config_periods"
                    )
                logger.info(
                    "migrate_full_config_json: dropped full_config_json, %d periods migrated",
                    migrated
                )
            except Exception as e:
                logger.warning("migrate_full_config_json: column drop failed: %s", e)
            finally:
                self._conn.execute("PRAGMA foreign_keys = ON")

        # ── Step 2a: is_gap_seed column on current_reads (must precede gap_marker migration) ───────────────────────
        cr_cols = [r[1] for r in self._conn.execute(
            "PRAGMA table_info(current_reads)"
        ).fetchall()]
        if "is_gap_seed" not in cr_cols:
            try:
                with self._conn:
                    self._conn.execute(
                        "ALTER TABLE current_reads "
                        "ADD COLUMN is_gap_seed INTEGER NOT NULL DEFAULT 0"
                    )
                    self._conn.execute(
                        "CREATE INDEX IF NOT EXISTS idx_current_reads_gap "
                        "ON current_reads (is_gap_seed)"
                    )
                logger.info(
                    "migrate_full_config_json: added is_gap_seed to current_reads"
                )
            except Exception as _e:
                logger.warning(
                    "migrate_full_config_json: is_gap_seed add failed: %s", _e
                )

        # ── Step 2b: gap_last_block_start column on current_block ────────────
        cb_cols_check = [r[1] for r in self._conn.execute(
            "PRAGMA table_info(current_block)"
        ).fetchall()]
        if "gap_last_block_start" not in cb_cols_check:
            try:
                with self._conn:
                    self._conn.execute(
                        "ALTER TABLE current_block "
                        "ADD COLUMN gap_last_block_start TEXT"
                    )
                logger.info("migrate: added gap_last_block_start to current_block")
            except Exception as _e:
                logger.warning("migrate: gap_last_block_start add failed: %s", _e)

        # ── Step 2c: gap_marker blob → gap_detected_at + is_gap_seed rows ────
        cb_cols = [r[1] for r in self._conn.execute(
            "PRAGMA table_info(current_block)"
        ).fetchall()]
        if "gap_marker" in cb_cols:
            try:
                with self._conn:
                    cb_row = self._conn.execute(
                        "SELECT gap_marker FROM current_block WHERE id=1"
                    ).fetchone()
                    gap_detected_at = None
                    if cb_row and cb_row["gap_marker"]:
                        try:
                            gm = json.loads(cb_row["gap_marker"])
                            gap_detected_at = gm.get("detected_at")
                            for meter_id, channels in (gm.get("pre_reads") or {}).items():
                                for ch, r in (channels or {}).items():
                                    if r:
                                        self._conn.execute(
                                            """INSERT OR IGNORE INTO current_reads
                                               (captured_at, meter_id, channel,
                                                channel_type, value, is_gap_seed)
                                               VALUES (?, ?, ?, 'read', ?, 1)""",
                                            (r.get("ts"), meter_id, ch, r.get("value", 0))
                                        )
                            for meter_id, channels in (gm.get("last_known_rates") or {}).items():
                                for ch, r in (channels or {}).items():
                                    if r:
                                        self._conn.execute(
                                            """INSERT OR IGNORE INTO current_reads
                                               (captured_at, meter_id, channel,
                                                channel_type, value, is_gap_seed)
                                               VALUES (?, ?, ?, 'rate', ?, 2)""",
                                            (r.get("ts"), meter_id, ch, r.get("value", 0))
                                        )
                        except Exception as _ge:
                            logger.warning(
                                "migrate_full_config_json: gap_marker parse failed: %s", _ge
                            )
                    self._conn.execute("""
                        CREATE TABLE IF NOT EXISTS current_block_new (
                            id              INTEGER PRIMARY KEY CHECK (id = 1),
                            block_start     TEXT,
                            block_end       TEXT,
                            last_checkpoint TEXT,
                            gap_detected_at TEXT,
                            interpolated    INTEGER NOT NULL DEFAULT 0
                        )
                    """)
                    self._conn.execute("""
                        INSERT INTO current_block_new
                            (id, block_start, block_end, last_checkpoint,
                             gap_detected_at, interpolated)
                        SELECT id, block_start, block_end, last_checkpoint, ?, interpolated
                        FROM current_block
                    """, (gap_detected_at,))
                    self._conn.execute("DROP TABLE current_block")
                    self._conn.execute(
                        "ALTER TABLE current_block_new RENAME TO current_block"
                    )
                logger.info(
                    "migrate_full_config_json: dropped gap_marker, gap_detected_at set"
                )
            except Exception as _e:
                logger.warning(
                    "migrate_full_config_json: gap_marker migration failed: %s", _e
                )

        # ── Step 4: mpan/tariff columns on meter_channels ─────────────────────
        mc_cols = [r[1] for r in self._conn.execute(
            "PRAGMA table_info(meter_channels)"
        ).fetchall()]
        for _col, _defn in [("mpan", "TEXT"), ("tariff", "TEXT")]:
            if _col not in mc_cols:
                try:
                    with self._conn:
                        self._conn.execute(
                            f"ALTER TABLE meter_channels ADD COLUMN {_col} {_defn}"
                        )
                    logger.info(
                        "migrate_full_config_json: added %s to meter_channels", _col
                    )
                except Exception as _e:
                    logger.warning(
                        "migrate_full_config_json: %s column add failed: %s", _col, _e
                    )

        # ── Step 5: supplier column on config_periods ────────────────────────
        # Supplier belongs on config_periods (not meters) so it has a historical
        # record — each config period records which supplier was active.
        # Attempt to migrate supplier from the full_config_json if still present
        # in any period (2.0.x databases that haven't been fully migrated yet).
        cp_cols = [r[1] for r in self._conn.execute(
            "PRAGMA table_info(config_periods)"
        ).fetchall()]
        if "supplier" not in cp_cols:
            try:
                with self._conn:
                    self._conn.execute(
                        "ALTER TABLE config_periods ADD COLUMN supplier TEXT"
                    )
                logger.info("migrate_full_config_json: added supplier to config_periods")
            except Exception as _e:
                logger.warning(
                    "migrate_full_config_json: supplier column add failed: %s", _e
                )

        return migrated

    # ── Write ─────────────────────────────────────────────────────────────

    def append_block(self, block: dict,
                     config_period_id: Optional[int] = None) -> None:
        """
        Insert a finalised block (all meters) as one transaction.
        If config_period_id is None, uses the current active period.
        """
        period_id = config_period_id or self.get_current_config_period_id()
        if period_id is None:
            raise RuntimeError(
                "BlockStore.append_block: no config period exists. "
                "Call insert_config_period() before appending blocks."
            )
        cp = self.get_config_period(period_id)
        tz_name = cp["timezone"] if cp else "UTC"
        rows = _block_rows(block, period_id, tz_name)
        self._insert_block_rows(rows)

    def append_blocks(self, blocks: list[dict],
                      config_period_id: Optional[int] = None) -> int:
        """
        Bulk insert a list of blocks. Used by migration and gap fill.
        Returns the number of meter-rows inserted.
        """
        period_id = config_period_id or self.get_current_config_period_id()
        if period_id is None:
            raise RuntimeError(
                "BlockStore.append_blocks: no config period exists."
            )
        cp = self.get_config_period(period_id)
        tz_name = cp["timezone"] if cp else "UTC"
        all_rows = []
        for block in blocks:
            all_rows.extend(_block_rows(block, period_id, tz_name))
        self._insert_block_rows(all_rows)
        return len(all_rows)

    def _insert_block_rows(self, rows: list[dict]) -> None:
        if not rows:
            return
        sql = """
            INSERT OR IGNORE INTO blocks (
                block_start, block_end,
                meter_id, config_period_id, interpolated,
                imp_kwh, imp_kwh_grid, imp_kwh_remainder,
                imp_rate, imp_cost, imp_cost_remainder,
                imp_read_start, imp_read_end,
                exp_kwh, exp_rate, exp_cost,
                exp_read_start, exp_read_end,
                standing_charge, carbon_g
            ) VALUES (
                :block_start, :block_end,
                :meter_id, :config_period_id, :interpolated,
                :imp_kwh, :imp_kwh_grid, :imp_kwh_remainder,
                :imp_rate, :imp_cost, :imp_cost_remainder,
                :imp_read_start, :imp_read_end,
                :exp_kwh, :exp_rate, :exp_cost,
                :exp_read_start, :exp_read_end,
                :standing_charge, :carbon_g
            )
        """
        with self._conn:
            self._conn.executemany(sql, rows)

    def _insert_block_rows_replace(self, rows: list[dict]) -> None:
        """Like _insert_block_rows but uses INSERT OR REPLACE to overwrite
        existing blocks. Used by gap fill to correct zero/bad blocks in the DB."""
        if not rows:
            return
        sql = """
            INSERT OR REPLACE INTO blocks (
                block_start, block_end,
                meter_id, config_period_id, interpolated,
                imp_kwh, imp_kwh_grid, imp_kwh_remainder,
                imp_rate, imp_cost, imp_cost_remainder,
                imp_read_start, imp_read_end,
                exp_kwh, exp_rate, exp_cost,
                exp_read_start, exp_read_end,
                standing_charge, carbon_g
            ) VALUES (
                :block_start, :block_end,
                :meter_id, :config_period_id, :interpolated,
                :imp_kwh, :imp_kwh_grid, :imp_kwh_remainder,
                :imp_rate, :imp_cost, :imp_cost_remainder,
                :imp_read_start, :imp_read_end,
                :exp_kwh, :exp_rate, :exp_cost,
                :exp_read_start, :exp_read_end,
                :standing_charge, :carbon_g
            )
        """
        with self._conn:
            self._conn.executemany(sql, rows)

    def append_block_replace(self, block: dict,
                             config_period_id: Optional[int] = None) -> None:
        """Insert a finalised block, replacing any existing block at the same
        (block_start, meter_id, config_period_id). Used by gap fill."""
        period_id = config_period_id or self.get_current_config_period_id()
        if period_id is None:
            raise RuntimeError(
                "BlockStore.append_block_replace: no config period exists."
            )
        cp = self.get_config_period(period_id)
        tz_name = cp["timezone"] if cp else "UTC"
        rows = _block_rows(block, period_id, tz_name)
        self._insert_block_rows_replace(rows)

    def _select_blocks(self, where: str, params: tuple) -> list[dict]:
        sql = f"""
            SELECT b.*, cp.billing_day, cp.block_minutes, cp.timezone,
                   cp.currency_symbol, cp.currency_code, cp.effective_from,
                   m.is_sub_meter, m.parent_meter_id, m.device_label,
                   m.inverter_possible, m.power_sensor, m.postcode_prefix,
                   m.v2x_capable, m.meter_type, m.soc_sensor, m.inverter_power_sensor, m.inverter_power_invert, m.device_power_sensor
            FROM blocks b
            JOIN config_periods cp ON b.config_period_id = cp.id
            LEFT JOIN meters m ON m.meter_id = b.meter_id
                               AND m.config_period_id = b.config_period_id
            {where}
            ORDER BY b.block_start, b.meter_id
        """
        cur = self._conn.execute(sql, params)
        return _rows_to_blocks(cur.fetchall())

    def get_all_blocks(self) -> list[dict]:
        """Full export — used by chart generation during transition phase."""
        return self._select_blocks("", ())

    def get_blocks_lightweight(self, utc_start: str | None = None,
                                utc_end: str | None = None) -> list[dict]:
        """
        Lightweight block fetch for chart generation — returns minimal dicts
        without full _rows_to_blocks reconstruction. Significantly faster than
        get_all_blocks() for large datasets.

        Returns dicts compatible with generate_net_heatmap and
        generate_daily_import_export_charts:
          - start, totals.import_kwh, totals.export_kwh
          - meters[meter_id].carbon_g, .imp_kwh, .exp_kwh, .imp_cost, .exp_cost
          - meters[meter_id].meta, .standing_charge, .channels

        Filters to the given UTC range if provided.
        """
        where = ""
        params: list = []
        if utc_start and utc_end:
            where = "WHERE b.block_start >= ? AND b.block_start < ?"
            params = [utc_start, utc_end]
        elif utc_start:
            where = "WHERE b.block_start >= ?"
            params = [utc_start]

        rows = self._conn.execute(
            f"""SELECT b.block_start, b.meter_id,
                       b.imp_kwh, b.imp_kwh_grid, b.imp_kwh_remainder,
                       b.imp_rate, b.imp_cost, b.imp_read_start, b.imp_read_end,
                       b.exp_kwh, b.exp_rate, b.exp_cost,
                       b.standing_charge, b.carbon_g, b.interpolated,
                       m.is_sub_meter, m.parent_meter_id, m.device_label,
                       m.inverter_possible, m.meter_type,
                       cp.billing_day, cp.block_minutes, cp.timezone,
                       cp.currency_symbol, cp.currency_code, cp.effective_from
               FROM blocks b
               JOIN config_periods cp ON b.config_period_id = cp.id
               LEFT JOIN meters m ON m.meter_id = b.meter_id
                 AND m.config_period_id = b.config_period_id
               {where}
               ORDER BY b.block_start, b.meter_id""",
            params
        ).fetchall()

        # Group by block_start and reconstruct minimal block dicts
        from collections import defaultdict as _dd
        block_map: dict = {}
        block_order: list = []

        for row in rows:
            bs = row["block_start"]
            if bs not in block_map:
                block_map[bs] = {
                    "start":   bs,
                    "end":     bs,  # approximate — not stored separately here
                    "meters":  {},
                    "totals":  {"import_kwh": 0.0, "export_kwh": 0.0,
                                "import_cost": 0.0, "export_cost": 0.0},
                    "_billing_day":    row["billing_day"],
                    "_block_minutes":  row["block_minutes"],
                    "_timezone":       row["timezone"],
                    "_currency_symbol": row["currency_symbol"],
                    "_currency_code":  row["currency_code"],
                    "_effective_from": row["effective_from"],
                }
                block_order.append(bs)

            b = block_map[bs]
            mid = row["meter_id"]
            is_sub = bool(row["is_sub_meter"])
            imp = float(row["imp_kwh"] or 0)
            exp = float(row["exp_kwh"] or 0)

            b["meters"][mid] = {
                "meta": {
                    "sub_meter":        is_sub,
                    "parent_meter":     row["parent_meter_id"],
                    "device":           row["device_label"],
                    "inverter_possible": bool(row["inverter_possible"]),
                    "meter_type":       row["meter_type"],
                    "block_minutes":    row["block_minutes"],
                    "timezone":         row["timezone"],
                    "currency_symbol":  row["currency_symbol"],
                },
                "carbon_g":      row["carbon_g"],
                "imp_kwh":       imp,
                "exp_kwh":       exp,
                "imp_cost":      float(row["imp_cost"] or 0),
                "exp_cost":      float(row["exp_cost"] or 0),
                "standing_charge": float(row["standing_charge"] or 0),
                "interpolated":  bool(row["interpolated"]),
                "channels": {
                    "import": {
                        "kwh":           imp,
                        "kwh_grid":      float(row["imp_kwh_grid"] or 0) if row["imp_kwh_grid"] is not None else None,
                        "kwh_remainder": float(row["imp_kwh_remainder"] or 0) if row["imp_kwh_remainder"] is not None else None,
                        "rate":          float(row["imp_rate"]) if row["imp_rate"] is not None else None,
                        "cost":          float(row["imp_cost"] or 0),
                        "read_start":    row["imp_read_start"],
                        "read_end":      row["imp_read_end"],
                    },
                    "export": {
                        "kwh":  exp,
                        "rate": float(row["exp_rate"]) if row["exp_rate"] is not None else None,
                        "cost": float(row["exp_cost"] or 0),
                    },
                },
            }

            if not is_sub:
                b["totals"]["import_kwh"]  += imp
                b["totals"]["export_kwh"]  += exp
                b["totals"]["import_cost"] += float(row["imp_cost"] or 0)
                b["totals"]["export_cost"] += float(row["exp_cost"] or 0)

        return [block_map[bs] for bs in block_order]

    def get_last_block_before(self, block_start: str) -> Optional[dict]:
        """Return the most recently finalised block before the given block_start.
        Used by engine_startup to avoid catching up zero-rate blocks written
        by ensure_correct_block before startup completes."""
        cur = self._conn.execute(
            """
            SELECT b.*, cp.billing_day, cp.block_minutes, cp.timezone,
                   cp.currency_symbol, cp.currency_code, cp.effective_from,
                   m.is_sub_meter, m.parent_meter_id, m.device_label,
                   m.inverter_possible, m.power_sensor, m.postcode_prefix,
                   m.v2x_capable, m.meter_type, m.soc_sensor, m.inverter_power_sensor, m.inverter_power_invert, m.device_power_sensor
            FROM blocks b
            JOIN config_periods cp ON b.config_period_id = cp.id
            LEFT JOIN meters m ON m.meter_id = b.meter_id
                               AND m.config_period_id = b.config_period_id
            WHERE b.block_start = (
                SELECT MAX(block_start) FROM blocks WHERE block_start < ?
            )
            ORDER BY b.meter_id
            """,
            (block_start,)
        )
        rows = cur.fetchall()
        if not rows:
            return None
        return _row_to_block(rows)

    def get_last_block(self) -> Optional[dict]:
        """Return the most recently finalised block (by block_start)."""
        cur = self._conn.execute(
            """
            SELECT b.*, cp.billing_day, cp.block_minutes, cp.timezone,
                   cp.currency_symbol, cp.currency_code, cp.effective_from,
                   m.is_sub_meter, m.parent_meter_id, m.device_label,
                   m.inverter_possible, m.power_sensor, m.postcode_prefix,
                   m.v2x_capable, m.meter_type, m.soc_sensor, m.inverter_power_sensor, m.inverter_power_invert, m.device_power_sensor
            FROM blocks b
            JOIN config_periods cp ON b.config_period_id = cp.id
            LEFT JOIN meters m ON m.meter_id = b.meter_id
                               AND m.config_period_id = b.config_period_id
            WHERE b.block_start = (SELECT MAX(block_start) FROM blocks)
            ORDER BY b.meter_id
            """
        )
        rows = cur.fetchall()
        if not rows:
            return None
        return _row_to_block(rows)

    def get_blocks_for_range(self,
                             start: datetime,
                             end: datetime,
                             meter_id: Optional[str] = None) -> list[dict]:
        """Return blocks within [start, end], optionally filtered by meter."""
        start_iso = start.isoformat()
        end_iso   = end.isoformat()
        if meter_id:
            return self._select_blocks(
                "WHERE b.block_start >= ? AND b.block_start <= ? AND b.meter_id = ?",
                (start_iso, end_iso, meter_id)
            )
        return self._select_blocks(
            "WHERE b.block_start >= ? AND b.block_start <= ?",
            (start_iso, end_iso)
        )

    # ── UTC-based query methods ───────────────────────────────────────────────

    def get_blocks_for_utc_range(self, utc_start: str, utc_end: str,
                                  meter_id: str | None = None) -> list[dict]:
        """
        Return all blocks where block_start >= utc_start AND block_start < utc_end.
        utc_start / utc_end are naive ISO strings (no tzinfo) matching DB format.
        Replaces get_blocks_for_local_date_range — use local_date_range_to_utc_bounds()
        to compute the UTC bounds from local date strings.
        """
        if meter_id:
            return self._select_blocks(
                "WHERE b.block_start >= ? AND b.block_start < ? AND b.meter_id = ?",
                (utc_start, utc_end, meter_id)
            )
        return self._select_blocks(
            "WHERE b.block_start >= ? AND b.block_start < ?",
            (utc_start, utc_end)
        )

    def get_dates_in_utc_range(self, utc_start: str, utc_end: str,
                                tz_name: str) -> list[str]:
        """
        Return distinct local dates (YYYY-MM-DD) present within a UTC range,
        computed at query time from block_start using the given timezone.
        Replaces get_local_dates() — does not use the local_date column.
        """
        try:
            tz = ZoneInfo(tz_name)
        except Exception:
            tz = ZoneInfo("UTC")
        rows = self._conn.execute(
            """SELECT DISTINCT block_start FROM blocks
               WHERE block_start >= ? AND block_start < ?
               ORDER BY block_start""",
            (utc_start, utc_end)
        ).fetchall()
        seen = set()
        dates = []
        for row in rows:
            dt = datetime.fromisoformat(row[0]).replace(
                tzinfo=ZoneInfo("UTC")
            ).astimezone(tz)
            d = dt.strftime("%Y-%m-%d")
            if d not in seen:
                seen.add(d)
                dates.append(d)
        return dates

    def count_blocks(self) -> int:
        """Total distinct block count (by block_start)."""
        cur = self._conn.execute(
            "SELECT COUNT(DISTINCT block_start) FROM blocks"
        )
        return cur.fetchone()[0]

    def count_meter_rows(self) -> int:
        """Total meter-row count — useful for diagnostics."""
        cur = self._conn.execute("SELECT COUNT(*) FROM blocks")
        return cur.fetchone()[0]

    # ── Reads (Phase 2+) ──────────────────────────────────────────────────

    def insert_read(self, meter_id: str, channel: str,
                    captured_at: str, reading_kwh: float,
                    rate: Optional[float] = None,
                    block_id: Optional[int] = None) -> None:
        """Insert a raw sensor read. block_id is None until block is finalised."""
        with self._conn:
            self._conn.execute(
                """
                INSERT INTO reads (captured_at, meter_id, channel,
                                   reading_kwh, rate, block_id)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (captured_at, meter_id, channel, reading_kwh, rate, block_id)
            )

    def link_reads_to_block(self, block_start: str, block_end: str,
                            block_id: int) -> int:
        """
        Set block_id on all reads that fall within [block_start, block_end].
        Returns the count of reads linked.
        """
        with self._conn:
            cur = self._conn.execute(
                """
                UPDATE reads SET block_id = ?
                WHERE block_id IS NULL
                  AND captured_at >= ? AND captured_at <= ?
                """,
                (block_id, block_start, block_end)
            )
        return cur.rowcount

    def get_reads_for_block(self, block_id: int) -> list[dict]:
        """All reads linked to a specific block."""
        cur = self._conn.execute(
            "SELECT * FROM reads WHERE block_id = ? ORDER BY captured_at",
            (block_id,)
        )
        return [dict(r) for r in cur.fetchall()]

    def get_reads_for_range(self, start: datetime, end: datetime,
                            meter_id: Optional[str] = None) -> list[dict]:
        """Reads within a datetime range, optionally filtered by meter."""
        start_iso = start.isoformat()
        end_iso   = end.isoformat()
        if meter_id:
            cur = self._conn.execute(
                """
                SELECT * FROM reads
                WHERE captured_at >= ? AND captured_at <= ?
                  AND meter_id = ?
                ORDER BY captured_at
                """,
                (start_iso, end_iso, meter_id)
            )
        else:
            cur = self._conn.execute(
                """
                SELECT * FROM reads
                WHERE captured_at >= ? AND captured_at <= ?
                ORDER BY captured_at
                """,
                (start_iso, end_iso)
            )
        return [dict(r) for r in cur.fetchall()]

    def purge_reads_older_than(self, days: int) -> int:
        """
        Delete reads older than `days` days. Returns count deleted.
        Block summaries (the blocks table) are never affected.
        """
        cutoff = datetime.now(timezone.utc).replace(tzinfo=None)
        from datetime import timedelta
        cutoff -= timedelta(days=days)
        cutoff_iso = cutoff.isoformat()
        with self._conn:
            cur = self._conn.execute(
                "DELETE FROM reads WHERE captured_at < ?", (cutoff_iso,)
            )
        deleted = cur.rowcount
        if deleted:
            logger.info("purge_reads_older_than: deleted %d reads older than %s", deleted, cutoff_iso)
        return deleted


# ─────────────────────────────────────────────────────────────────────────────
# Migration
# ─────────────────────────────────────────────────────────────────────────────

def migrate_json_to_sqlite(json_path: str,
                           store: "BlockStore",
                           config_json: dict,
                           effective_from: Optional[str] = None) -> int:
    """
    One-time migration of blocks.json -> BlockStore.

    Creates a single config_periods row from config_json covering all history,
    then bulk-inserts all blocks. Idempotent via INSERT OR IGNORE.

    Returns the number of blocks migrated.
    """
    import json as _json
    try:
        with open(json_path, "r") as f:
            blocks = _json.load(f)
    except Exception as e:
        logger.error("migrate_json_to_sqlite: failed to read %s: %s", json_path, e)
        return 0

    if not isinstance(blocks, list):
        logger.error("migrate_json_to_sqlite: blocks.json is not a list")
        return 0

    # Determine effective_from: oldest block start, or now if no blocks
    if effective_from is None:
        starts = [b.get("start") for b in blocks if b.get("start")]
        effective_from = min(starts) if starts else _utc_now_iso()

    # Create the single historical config period
    period_id = store.insert_config_period(
        config_json=config_json,
        effective_from=effective_from,
        change_reason="Migrated from blocks.json",
    )

    # Bulk insert all blocks
    total = 0
    batch_size = 500
    for i in range(0, len(blocks), batch_size):
        batch = blocks[i:i + batch_size]
        inserted = store.append_blocks(batch, config_period_id=period_id)
        total += inserted
        logger.info(
            "migrate_json_to_sqlite: %d/%d blocks processed",
            min(i + batch_size, len(blocks)), len(blocks)
        )

    logger.info(
        "migrate_json_to_sqlite: complete — %d blocks, %d meter-rows inserted",
        len(blocks), total
    )
    return len(blocks)


# ─────────────────────────────────────────────────────────────────────────────
# Factory
# ─────────────────────────────────────────────────────────────────────────────

def open_block_store(db_path: str) -> "BlockStore":
    """
    Preferred entry point. Opens (or creates) a BlockStore at db_path,
    applies all PRAGMAs, and ensures the schema exists.

    If the DB file is corrupt, renames it to .corrupt and starts fresh
    so the engine can still start (migration will re-run if blocks.json exists).
    """
    import os
    os.makedirs(os.path.dirname(db_path) or ".", exist_ok=True)
    try:
        return BlockStore(db_path)
    except Exception as e:
        logger.error("open_block_store: failed to open %s: %s — attempting recovery", db_path, e)
        if os.path.exists(db_path):
            corrupt_path = db_path + ".corrupt"
            try:
                os.rename(db_path, corrupt_path)
                logger.warning("open_block_store: renamed corrupt DB to %s", corrupt_path)
            except Exception:
                pass
        # Start with a fresh DB — migration will re-run if blocks.json.migrated exists
        return BlockStore(db_path)