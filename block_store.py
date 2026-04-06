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
from datetime import datetime, timezone
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
    change_reason    TEXT,
    full_config_json TEXT    NOT NULL
);

-- Meter registry: one row per meter per config period.
CREATE TABLE IF NOT EXISTS meters (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    meter_id         TEXT    NOT NULL,
    is_sub_meter     INTEGER NOT NULL DEFAULT 0,
    device_label     TEXT,
    parent_meter_id  TEXT,
    config_period_id INTEGER NOT NULL,
    FOREIGN KEY (config_period_id) REFERENCES config_periods(id)
);

CREATE INDEX IF NOT EXISTS idx_meters_meter_id ON meters (meter_id);

-- Blocks: pure measurement data, no repeated config fields.
-- config_period_id links each block to the config that was active when
-- it was recorded.
CREATE TABLE IF NOT EXISTS blocks (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    block_start      TEXT    NOT NULL,
    block_end        TEXT    NOT NULL,
    local_date       TEXT    NOT NULL,
    local_year       INTEGER NOT NULL,
    local_month      INTEGER NOT NULL,
    local_day        INTEGER NOT NULL,
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
    FOREIGN KEY (config_period_id) REFERENCES config_periods(id),
    UNIQUE (block_start, meter_id)
);

CREATE INDEX IF NOT EXISTS idx_blocks_start     ON blocks (block_start);
CREATE INDEX IF NOT EXISTS idx_blocks_date      ON blocks (local_date);
CREATE INDEX IF NOT EXISTS idx_blocks_ym        ON blocks (local_year, local_month);
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
"""


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(tzinfo=None).isoformat()


def _local_date_parts(block_start_iso: str, tz_name: str) -> tuple[str, int, int, int]:
    """Return (local_date, year, month, day) for a block start timestamp."""
    try:
        tz = ZoneInfo(tz_name)
    except Exception:
        tz = ZoneInfo("UTC")
    dt = datetime.fromisoformat(block_start_iso).replace(tzinfo=ZoneInfo("UTC")).astimezone(tz)
    return dt.strftime("%Y-%m-%d"), dt.year, dt.month, dt.day


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
    local_date, local_year, local_month, local_day = _local_date_parts(block_start, tz_name)

    rows = []
    for meter_id, meter_block in (block.get("meters") or {}).items():
        meta    = meter_block.get("meta") or {}
        imp     = _channel(meter_block, "import")
        exp     = _channel(meter_block, "export")

        rows.append({
            "block_start":       block_start,
            "block_end":         block_end,
            "local_date":        local_date,
            "local_year":        local_year,
            "local_month":       local_month,
            "local_day":         local_day,
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

        meter_block = {
            "meta": {
                "block_minutes":  row["block_minutes"],
                "timezone":       row["timezone"],
                "billing_day":    row["billing_day"],
                "currency_symbol":row["currency_symbol"],
                "currency_code":  row["currency_code"],
            },
            "interpolated":   bool(row["interpolated"]),
            "standing_charge": row["standing_charge"] or 0.0,
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
                   "currency_symbol", "currency_code", "site", "site_name")

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

            self._conn.execute(
                "DELETE FROM config_periods WHERE id = ?", (period_id,)
            )

        logger.info(
            "delete_config_period: id=%d deleted, %d blocks reassigned to id=%s",
            period_id, block_rows, absorb_id
        )
        return {"deleted": True, "blocks_reassigned": block_rows}


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


    def get_billing_totals_for_local_date_range(self,
                                                first_local_date: str,
                                                last_local_date: str) -> dict:
        """
        Fast SQL aggregation of billing totals for a local calendar date range.
        Uses local_date column (timezone-corrected at insert time) for both
        kWh/cost and standing charge — correctly handles BST/GMT blocks at
        23:xx UTC that belong to the next local calendar day.

        first_local_date / last_local_date: YYYY-MM-DD strings (inclusive).
        """
        cur = self._conn.execute(
            """SELECT
                 SUM(imp_kwh)  as imp_kwh,
                 SUM(imp_cost) as imp_cost,
                 SUM(exp_kwh)  as exp_kwh,
                 SUM(exp_cost) as exp_cost
               FROM blocks
               WHERE local_date >= ? AND local_date <= ?""",
            (first_local_date, last_local_date)
        )
        row = cur.fetchone()

        # Standing charge: once per local calendar day using local_date column
        cur2 = self._conn.execute(
            """SELECT SUM(daily_sc) as standing FROM (
                 SELECT MIN(standing_charge) as daily_sc
                 FROM blocks
                 WHERE local_date >= ? AND local_date <= ?
                 GROUP BY local_date
               )""",
            (first_local_date, last_local_date)
        )
        row2 = cur2.fetchone()

        return {
            "imp_kwh":  round(float(row["imp_kwh"]  or 0), 4),
            "imp_cost": round(float(row["imp_cost"] or 0), 4),
            "exp_kwh":  round(float(row["exp_kwh"]  or 0), 4),
            "exp_cost": round(float(row["exp_cost"] or 0), 4),
            "standing": round(float(row2["standing"] or 0), 4),
        }

    def get_billing_totals_for_range(self, start: datetime, end: datetime) -> dict:
        """Wrapper: converts naive datetime boundaries to local_date strings."""
        from datetime import timezone
        # The caller passes local naive datetimes — derive the local date range
        start_date = start.date().isoformat()
        end_date   = end.date().isoformat()
        return self.get_billing_totals_for_local_date_range(start_date, end_date)


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

    def insert_config_period(self,
                             config_json: dict,
                             effective_from: Optional[str] = None,
                             change_reason: Optional[str] = None) -> int:
        """
        Snapshot the current config as a new config period.
        Closes the previous period's effective_to.
        Returns the new period's id.
        """
        main_meta = {}
        for m in config_json.get("meters", {}).values():
            if not (m.get("meta") or {}).get("sub_meter"):
                main_meta = m.get("meta") or {}
                break

        # Snap effective_from to midnight (start of day) in the configured timezone.
        # Config changes recorded mid-day still apply from the start of that day,
        # keeping billing periods aligned on whole days.
        tz_name = main_meta.get("timezone", "UTC")
        raw_from = effective_from or _utc_now_iso()
        try:
            from zoneinfo import ZoneInfo as _ZI
            from datetime import datetime as _dt2, timezone as _tz2
            # Parse the raw timestamp as UTC
            raw_dt = _dt2.fromisoformat(raw_from.replace(" ", "T").split(".")[0])
            raw_dt_utc = raw_dt.replace(tzinfo=_tz2.utc)
            # Convert to configured timezone and snap to midnight
            local_dt = raw_dt_utc.astimezone(_ZI(tz_name))
            midnight_local = local_dt.replace(hour=0, minute=0, second=0, microsecond=0)
            # Convert midnight back to UTC
            midnight_utc = midnight_local.astimezone(_tz2.utc).replace(tzinfo=None)
            now = midnight_utc.strftime("%Y-%m-%dT%H:%M:%S")
        except Exception:
            now = raw_from.replace(" ", "T").split(".")[0]

        with self._conn:
            # Close previous period — snap its effective_to to our effective_from
            self._conn.execute(
                "UPDATE config_periods SET effective_to = ? WHERE effective_to IS NULL",
                (now,)
            )
            cur = self._conn.execute(
                """
                INSERT INTO config_periods
                    (effective_from, effective_to, billing_day, block_minutes,
                     timezone, currency_symbol, currency_code, site_name,
                     change_reason, full_config_json)
                VALUES (?, NULL, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    now,
                    int(main_meta.get("billing_day") or 1),
                    int(main_meta.get("block_minutes") or 30),
                    main_meta.get("timezone", "UTC"),
                    main_meta.get("currency_symbol", "£"),
                    main_meta.get("currency_code", "GBP"),
                    main_meta.get("site", main_meta.get("site_name")),
                    change_reason,
                    json.dumps(config_json),
                )
            )
            period_id = cur.lastrowid

        logger.info("insert_config_period: new period id=%d effective_from=%s", period_id, now)
        return period_id

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
                local_date, local_year, local_month, local_day,
                meter_id, config_period_id, interpolated,
                imp_kwh, imp_kwh_grid, imp_kwh_remainder,
                imp_rate, imp_cost, imp_cost_remainder,
                imp_read_start, imp_read_end,
                exp_kwh, exp_rate, exp_cost,
                exp_read_start, exp_read_end,
                standing_charge
            ) VALUES (
                :block_start, :block_end,
                :local_date, :local_year, :local_month, :local_day,
                :meter_id, :config_period_id, :interpolated,
                :imp_kwh, :imp_kwh_grid, :imp_kwh_remainder,
                :imp_rate, :imp_cost, :imp_cost_remainder,
                :imp_read_start, :imp_read_end,
                :exp_kwh, :exp_rate, :exp_cost,
                :exp_read_start, :exp_read_end,
                :standing_charge
            )
        """
        with self._conn:
            self._conn.executemany(sql, rows)

    # ── Read ──────────────────────────────────────────────────────────────

    def _select_blocks(self, where: str, params: tuple) -> list[dict]:
        sql = f"""
            SELECT b.*, cp.billing_day, cp.block_minutes, cp.timezone,
                   cp.currency_symbol, cp.currency_code, cp.effective_from
            FROM blocks b
            JOIN config_periods cp ON b.config_period_id = cp.id
            {where}
            ORDER BY b.block_start, b.meter_id
        """
        cur = self._conn.execute(sql, params)
        return _rows_to_blocks(cur.fetchall())

    def get_all_blocks(self) -> list[dict]:
        """Full export — used by chart generation during transition phase."""
        return self._select_blocks("", ())

    def get_last_block(self) -> Optional[dict]:
        """Return the most recently finalised block (by block_start)."""
        cur = self._conn.execute(
            """
            SELECT b.*, cp.billing_day, cp.block_minutes, cp.timezone,
                   cp.currency_symbol, cp.currency_code, cp.effective_from
            FROM blocks b
            JOIN config_periods cp ON b.config_period_id = cp.id
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

    def get_blocks_for_local_date_range(self, first_local_date: str, last_local_date: str) -> list[dict]:
        """Return all blocks whose local_date falls within [first, last] inclusive.
        Uses the pre-computed local_date column so BST blocks at 23:xx UTC are
        correctly included in the next local day rather than missed by a UTC boundary."""
        return self._select_blocks(
            "WHERE b.local_date >= ? AND b.local_date <= ?",
            (first_local_date, last_local_date)
        )

    def get_blocks_for_date(self, local_date: str) -> list[dict]:
        """All blocks for a given YYYY-MM-DD local date."""
        return self._select_blocks(
            "WHERE b.local_date = ?", (local_date,)
        )

    def get_blocks_for_month(self, year: int, month: int) -> list[dict]:
        """All blocks for a given local year/month."""
        return self._select_blocks(
            "WHERE b.local_year = ? AND b.local_month = ?", (year, month)
        )

    def get_local_dates(self) -> list[str]:
        """Distinct local dates present — used by heatmap generation."""
        cur = self._conn.execute(
            "SELECT DISTINCT local_date FROM blocks ORDER BY local_date"
        )
        return [row["local_date"] for row in cur.fetchall()]

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