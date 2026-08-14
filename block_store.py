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

# ── 3.5.0 Historical-import provenance source values ─────────────────────────
# Reconstructed blocks carry one of these in blocks.source. The reconstructed-
# history delete filter matches the shared 'imported' prefix, so a bad import can
# be wiped without touching native (live/kraken) data.
IMPORTED_SOURCE_API     = "imported_api"       # from the Octopus consumption API
IMPORTED_SOURCE_CSV     = "imported_csv"        # from a supplied CSV (rate-from-cost)
IMPORTED_SOURCE_BLENDED = "imported_blended"    # go-live period straddling live capture
IMPORTED_SOURCES        = (IMPORTED_SOURCE_API, IMPORTED_SOURCE_CSV, IMPORTED_SOURCE_BLENDED)
IMPORTED_SOURCE_PREFIX  = "imported"            # blocks.source LIKE 'imported%'

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
    postcode_prefix    TEXT,               -- UK carbon intensity, OUTWARD CODE ONLY e.g. "SW1A" (main meter only)
    postcode_source    TEXT,               -- 'user' | 'octopus' | 'unknown' — provenance of postcode_prefix (main meter only)
    v2x_capable             INTEGER DEFAULT 0,  -- V2G / bidirectional charging capable
    inverter_power_invert   INTEGER DEFAULT 0,  -- negate inverter power sensor value
    power_invert            INTEGER DEFAULT 0,  -- negate MAIN power sensor value (Live Power)
    device_power_invert     INTEGER DEFAULT 0,  -- negate device power sensor value (Live Power)
    soc_sensor         TEXT,               -- HA entity_id for battery SoC % (informational)
    inverter_power_sensor TEXT,            -- HA entity_id for inverter power W/kW (informational)
    device_power_sensor TEXT,              -- HA entity_id for EV/heat pump power W/kW (informational)
    pv_power_sensor     TEXT,              -- HA entity_id for battery PV/solar power W/kW (informational)
    retired_at          TEXT,              -- ISO date from which this meter is retired (NULL = active)
    retired_reason      TEXT,              -- optional note e.g. "Device replaced", "Moved property"
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
    imp_cost_exc     REAL,               -- 4.1.x BL-23: exc-VAT import cost (NULL until captured; inc figures unchanged)
    imp_rate_exc     REAL,               -- 4.2 BL-23: exc-VAT import unit rate, full precision (NULL until captured)
    imp_read_start   REAL,
    imp_read_end     REAL,
    exp_kwh          REAL,
    exp_rate         REAL,
    exp_cost         REAL,
    exp_read_start   REAL,
    exp_read_end     REAL,
    standing_charge  REAL    NOT NULL DEFAULT 0,
    standing_charge_exc REAL,            -- 4.2 BL-23: exc-VAT daily standing charge (NULL until captured)
    carbon_g         REAL,               -- net gCO2 for this block (NULL if no CI data)
    imp_provisional  INTEGER NOT NULL DEFAULT 0,  -- 1 = sub-meter kWh written without post-boundary read; 0 = final
    -- ── 3.0.0 Kraken API integration columns ──────────────────────────────
    source              TEXT,                       -- 'ha_sensor' | 'kraken_api' | 'kraken_mini' (NULL = legacy/ha_sensor)
    exc_source          TEXT,                       -- 4.2 BL-23: provenance of exc figures: 'measurement'|'tariff'|'bill'|'csv'|NULL
    is_provisional      INTEGER NOT NULL DEFAULT 0, -- 1 = main meter not yet DCC-settled (api / api+mini modes)
    needs_pass2_rerun   INTEGER NOT NULL DEFAULT 0, -- 1 = DCC arrived, PASS 2+3b re-run pending
    imp_kwh_api         REAL,                       -- DCC-settled import kWh from Kraken REST (NULL until settlement)
    exp_kwh_api         REAL,                       -- DCC-settled export kWh from Kraken REST (NULL until settlement)
    needs_review        INTEGER NOT NULL DEFAULT 0, -- 1 = CAD/Mini vs DCC drift exceeded threshold
    carbon_intensity_g  REAL,                       -- gCO2/kWh at block_start, stored at write time (survives CI table pruning)
    rate_corrected      INTEGER NOT NULL DEFAULT 0, -- 1 = user manually corrected the rate; dispatch reconciliation must not touch it
    rate_reconciled     INTEGER NOT NULL DEFAULT 0, -- 1 = dispatch reconciliation set the rate; a later PASS 2 re-run must not stomp it
    -- ── 3.5.0 Historical import ───────────────────────────────────────────────
    derivation_id       INTEGER,                    -- FK → historical_derivation for a reconstructed block (NULL = live)
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

-- High-resolution generation mix (CI-tick cadence ~15min, 48hr retention).
-- Independent of block size — updated every CI tick so chart stays current.
CREATE TABLE IF NOT EXISTS mix_history (
    captured_at TEXT NOT NULL,  -- CI slot time (UTC, e.g. '2026-05-01T06:30')
    fuel        TEXT NOT NULL,
    perc        REAL NOT NULL,
    PRIMARY KEY (captured_at, fuel)
);
CREATE INDEX IF NOT EXISTS idx_mix_history_time ON mix_history (captured_at);

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

-- ── 3.0.0 Kraken API integration ─────────────────────────────────────────────
-- Persists ingester progress so a restart resumes from the last successful poll
-- rather than re-running a full backfill (quota protection). Single-row-per-key.
-- The blocks unique index idx_blocks_start_meter and the partial sweep indexes
-- are created in _ensure_schema AFTER the column ALTERs and duplicate detection,
-- because on an upgrade DB those columns do not exist when this DDL first runs.
CREATE TABLE IF NOT EXISTS kraken_state (
    key        TEXT PRIMARY KEY,   -- e.g. 'last_poll_utc', 'last_backfill_utc'
    value      TEXT,               -- ISO UTC timestamp or opaque string
    updated_at TEXT                -- ISO UTC of last write
);

-- ── 3.1.0 Intelligent dispatch overlay ───────────────────────────────────────
-- Durable record of which 30-min slots had an Intelligent smart-charge dispatch.
-- Dispatches are ephemeral in the Kraken API (planned dispatches churn and
-- disappear), so we capture them forward each poll and persist here. The overlay
-- resolver reads THIS table (not a live re-fetch) to decide whether a block's
-- slot should get the off-peak dispatch rate — gated by actual meter draw at
-- pricing time. One row per slot; re-capture upserts (last-write-wins).
CREATE TABLE IF NOT EXISTS dispatch_slots (
    slot_start  TEXT NOT NULL,      -- naive-UTC ISO, 30-min slot boundary
    off_peak    INTEGER NOT NULL DEFAULT 1,  -- 1 = smart-charge (off-peak candidate)
    provider    TEXT,               -- e.g. 'MYENERGI_V2', 'OHME', 'TESLA'
    source      TEXT,               -- meta.source: smart-charge/bump-charge/unknown
    captured_at TEXT NOT NULL,      -- UTC ISO when we recorded this slot
    -- Dispatch lifecycle capture (see dispatch_validation_design.md). OBSERVE-ONLY:
    -- recorded but NOT yet used for billing. Lets us validate a planned slot
    -- against what actually charged, instead of the meter-draw floor alone.
    state            TEXT,          -- 'planned' | 'started' | 'completed'
    energy_planned   REAL,          -- forecast energyAddedKwh for this slot (kWh)
    energy_completed REAL,          -- delivered energy for this slot at settlement (kWh)
    raw_start   TEXT,               -- BL-11: exact dispatch start (naive-UTC ISO, second precision)
    raw_end     TEXT,               -- BL-11: exact dispatch end (naive-UTC ISO, second precision)
    PRIMARY KEY (slot_start)
);
CREATE INDEX IF NOT EXISTS idx_dispatch_slots_start ON dispatch_slots (slot_start);

-- Local dispatch ACCUMULATION (observe-only; see dispatch_validation_design.md §11).
-- Separate from dispatch_slots (the billing surface) on purpose: this records
-- EVERY planned/completed dispatch we ever see, accumulated across polls, so
-- "absent" becomes meaningful and transient/tail dispatches aren't lost to the
-- per-poll snapshot. NOTHING reads this for billing. Mirrors BCD's local
-- intelligent_dispatches_history.
CREATE TABLE IF NOT EXISTS dispatch_history (
    slot_start  TEXT NOT NULL,      -- naive-UTC ISO, 30-min slot boundary
    kind        TEXT NOT NULL,      -- 'planned' | 'completed' | 'started'
    provider    TEXT,
    source      TEXT,               -- meta.source (smart-charge/bump/unknown)
    energy_kwh  REAL,               -- signed kWh for this slot (negative = charge)
    first_seen  TEXT NOT NULL,      -- UTC ISO — first poll that saw this slot/kind
    last_seen   TEXT NOT NULL,      -- UTC ISO — most recent poll that saw it
    raw_start   TEXT,               -- BL-11: exact dispatch start (naive-UTC ISO, second precision)
    raw_end     TEXT,               -- BL-11: exact dispatch end (naive-UTC ISO, second precision)
    PRIMARY KEY (slot_start, kind)
);
CREATE INDEX IF NOT EXISTS idx_dispatch_history_start ON dispatch_history (slot_start);

-- ── 3.5.0 Historical import: derivation provenance ────────────────────────────
-- One row per reconstruction (a device-attribution over a span, or a rate for a
-- tariff-period×tier). Reconstructed blocks point here via blocks.derivation_id,
-- so any imported figure can answer "which sensor / rate produced this, when",
-- and be rebuilt with new/extended data. Supersede (don't delete) on rebuild to
-- keep the audit trail; superseded_by IS NULL = the current derivation.
CREATE TABLE IF NOT EXISTS historical_derivation (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    scope           TEXT NOT NULL,              -- 'device_attribution' | 'rate'
    subject         TEXT,                       -- device meter_id (attribution) | channel 'import'/'export' (rate)
    period_from     TEXT NOT NULL,              -- UTC span this derivation covers
    period_to       TEXT NOT NULL,
    sensor_ids      TEXT,                       -- JSON list of sensor entity_ids (device scope)
    sensor_kind     TEXT,                       -- energy_total | power | session
    params          TEXT,                       -- JSON: split weights/ε, or rate tier + confidence
    derived_value   REAL,                       -- derived rate (rate scope), etc.
    confirmed_value REAL,                       -- user-confirmed rate, if overridden
    method_version  INTEGER NOT NULL DEFAULT 1, -- bump when the algorithm changes → stale-derivation detection
    derived_at      TEXT NOT NULL,              -- UTC ISO
    source          TEXT,                       -- imported_api | imported_csv | recorder_probe
    superseded_by   INTEGER,                    -- id that replaced this (NULL = current)
    notes           TEXT,
    FOREIGN KEY (superseded_by) REFERENCES historical_derivation(id)
);
CREATE INDEX IF NOT EXISTS idx_hderiv_subject ON historical_derivation (scope, subject);
CREATE INDEX IF NOT EXISTS idx_hderiv_current ON historical_derivation (superseded_by);
"""


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(tzinfo=None).isoformat()


def outward_code(postcode) -> Optional[str]:
    """Outward (first) part of a UK postcode, uppercased and space-stripped.

    'sw1a 1aa' → 'SW1A', 'M1 1AE' → 'M1', 'EH8' → 'EH8'. This is the ONLY part
    EMT persists (privacy: it resolves to a coarse DNO region, nothing more).
    A full postcode may be *read* transiently to detect a change, but callers
    must pass it through here before storage.
    """
    if not postcode:
        return None
    p = str(postcode).strip().upper()
    if not p:
        return None
    if " " in p:                      # normal case: outward code is before the space
        return p.split()[0]
    # No space present. A UK inward code is always 3 chars (digit + 2 letters);
    # strip it only when the tail actually looks like one, else assume already-outward.
    if len(p) > 3:
        tail = p[-3:]
        if tail[0].isdigit() and tail[1:].isalpha():
            return p[:-3]
    return p


def _property_key(p: dict) -> Optional[str]:
    """Stable, NON-identifying handle for a property — a short hash of the Octopus
    property id (or address+postcode as a fallback). Used only to group a
    property's records and to give the reconcile/confirmation UI a stable key;
    never stored, and not reversible to an address."""
    import hashlib
    pid = p.get("id")
    if pid is not None and str(pid).strip():
        base = "id:" + str(pid)
    else:
        addr = (p.get("address_line_1") or p.get("address_line1") or "").strip()
        pc   = (p.get("postcode") or p.get("post_code") or "").strip()
        if not (addr or pc):
            return None
        base = "addr:" + addr + "|" + pc
    return hashlib.sha1(base.encode("utf-8")).hexdigest()[:12]


def derive_region_periods(account: dict) -> list:
    """From a REST account payload, per-property tenancy spans for reconciliation.

    Returns [{"outcode","from","to","key","hint"}] sorted by move-in date, with
    consecutive records of the SAME property merged. Fields:
      - outcode : OUTWARD CODE ONLY (the only part persisted) — full postcodes
                  are read here and immediately reduced.
      - from/to : move-in / move-out (tolerant of moved_in_at/moved_in etc.).
      - key     : non-reversible property hash (grouping / UI handle; NOT stored).
      - hint    : the town, for the "name this site" UI — DISPLAY-ONLY, must NOT
                  be persisted.

    Different properties that happen to share an outward code are kept SEPARATE
    (they're different sites the user may name differently). Only when no stable
    key is derivable do we fall back to region-level merging so behaviour never
    regresses below the outward code.
    """
    spans = []
    for p in (account.get("properties") or []):
        oc = outward_code(p.get("postcode") or p.get("post_code"))
        if not oc:
            continue
        spans.append({
            "outcode": oc,
            "from": p.get("moved_in_at") or p.get("moved_in") or None,
            "to":   p.get("moved_out_at") or p.get("moved_out") or None,
            "key":  _property_key(p),
            "hint": (p.get("town") or None),
        })
    spans.sort(key=lambda s: s.get("from") or "")
    merged: list = []
    for s in spans:
        same = False
        if merged:
            prev = merged[-1]
            if s["key"] and prev["key"]:
                same = (s["key"] == prev["key"])          # same physical property
            else:
                same = (s["outcode"] == prev["outcode"])  # fallback: same region
        if same:
            merged[-1]["to"] = s["to"]                     # extend the open end
        else:
            merged.append(dict(s))
    return merged


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
            # BL-23: exc-VAT import cost + rate, when the import channel carries them
            # (NULL otherwise — inc-VAT figures are unaffected).
            "imp_cost_exc":      imp.get("cost_exc"),
            "imp_rate_exc":      imp.get("rate_exc"),
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
            # BL-23 (4.2): exc-VAT standing + exc provenance (NULL until captured)
            "standing_charge_exc": meter_block.get("standing_charge_exc"),
            "exc_source":        meter_block.get("exc_source"),
            # carbon footprint (NULL if no CI data available)
            "carbon_g":          meter_block.get("carbon_g"),
            # gCO2/kWh at block_start, stored at write time (3.0.0) — survives
            # carbon_intensity table pruning so PASS 3b re-run never re-queries it
            "carbon_intensity_g": meter_block.get("carbon_intensity_g"),
            # 3.0.0 Kraken state — preserved across append_block_replace so a
            # PASS 2 re-run (INSERT OR REPLACE) does not reset them. Defaults
            # keep pre-3.0.0 / CAD-mode blocks at 0/NULL.
            "imp_kwh_api":       meter_block.get("imp_kwh_api"),
            "exp_kwh_api":       meter_block.get("exp_kwh_api"),
            "is_provisional":    1 if meter_block.get("is_provisional") else 0,
            "needs_pass2_rerun": 1 if meter_block.get("needs_pass2_rerun") else 0,
            "needs_review":      1 if meter_block.get("needs_review") else 0,
            "source":            meter_block.get("source"),
            # provisional: 1 if sub-meter was written without a post-boundary read
            "imp_provisional":   1 if meter_block.get("provisional") else 0,
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
                if row["power_source"]:
                    meta["power_source"] = row["power_source"]
                if row["rate_source"]:
                    meta["rate_source"] = row["rate_source"]
                if row["soc_sensor"]:
                    meta["soc_sensor"] = row["soc_sensor"]
                if row["meter_type"]:
                    meta["meter_type"] = row["meter_type"]
                if row["inverter_power_sensor"]:
                    meta["inverter_power_sensor"] = row["inverter_power_sensor"]
                if row["inverter_power_invert"]:
                    meta["inverter_power_invert"] = True
                if "power_invert" in row.keys() and row["power_invert"]:
                    meta["power_invert"] = True
                if "device_power_invert" in row.keys() and row["device_power_invert"]:
                    meta["device_power_invert"] = True
                if row["device_power_sensor"]:
                    meta["device_power_sensor"] = row["device_power_sensor"]
                if row["retired_at"]:
                    meta["retired_at"] = row["retired_at"]
                if row["retired_reason"]:
                    meta["retired_reason"] = row["retired_reason"]
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
        # Provenance tag (imported_api / imported_csv / recorder_attributed / …).
        # MUST be carried onto the dict: any read → modify → append_block_replace
        # round-trip (remainder recompute, carbon recompute, gap fill, device
        # attribution) writes meter_block.get("source") back — so if we drop it
        # here, the write silently re-inserts source = NULL and the block loses its
        # tag. This was the "carbon round-trip wiped imported_api to NULL" bug.
        try:
            if row["source"] is not None:
                meter_block["source"] = row["source"]
        except (IndexError, KeyError):
            pass
        # 3.0.0 columns — surfaced so the DCC re-run can read them. Guarded
        # with try/except for older rows / pre-join fetches lacking the columns.
        try:
            meter_block["carbon_intensity_g"] = row["carbon_intensity_g"]
        except (IndexError, KeyError):
            pass
        try:
            if row["imp_kwh_api"] is not None:
                meter_block["imp_kwh_api"] = row["imp_kwh_api"]
        except (IndexError, KeyError):
            pass
        try:
            if row["exp_kwh_api"] is not None:
                meter_block["exp_kwh_api"] = row["exp_kwh_api"]
        except (IndexError, KeyError):
            pass
        try:
            if row["is_provisional"]:
                meter_block["is_provisional"] = True
        except (IndexError, KeyError):
            pass
        # BL-23 (4.2): exc-VAT standing + provenance — surfaced so a read→modify→rewrite
        # round-trip preserves them. Guarded for older DBs / pre-join fetches.
        try:
            if row["standing_charge_exc"] is not None:
                meter_block["standing_charge_exc"] = row["standing_charge_exc"]
        except (IndexError, KeyError):
            pass
        try:
            if row["exc_source"] is not None:
                meter_block["exc_source"] = row["exc_source"]
        except (IndexError, KeyError):
            pass

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
            # BL-23 exc-VAT cost + rate — surfaced (and thus preserved on round-trip
            # rewrite). Guarded for older DBs / pre-join fetches lacking the column.
            try:
                if row["imp_cost_exc"] is not None:
                    imp_ch["cost_exc"] = row["imp_cost_exc"]
            except (IndexError, KeyError):
                pass
            try:
                if row["imp_rate_exc"] is not None:
                    imp_ch["rate_exc"] = row["imp_rate_exc"]
            except (IndexError, KeyError):
                pass
            # Expose provisional flag so the amendment path can identify blocks
            # written without a post-boundary sub-meter read.
            try:
                if row["imp_provisional"]:
                    meter_block["provisional"] = True
            except (IndexError, KeyError):
                pass
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

    def __init__(self, db_path: str, *, read_only: bool = False):
        self._path = db_path
        self._read_only = read_only
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        if read_only:
            # Lightweight read-only companion (e.g. off-loop chart rendering):
            # NO schema/migration DDL, guarded by query_only so it can never
            # contend with the primary writer. Safe to use from a worker thread
            # while the engine writes on its own WAL connection (the db is
            # already WAL, so this connection reads the shared WAL snapshot).
            try:
                self._conn.execute("PRAGMA busy_timeout=5000")
                self._conn.execute("PRAGMA query_only=ON")
            except Exception:
                pass
            return
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
            # 2.8.0 — create mix_history for CI-tick resolution mix (independent of block size)
            self._conn.execute("""
                CREATE TABLE IF NOT EXISTS mix_history (
                    captured_at TEXT NOT NULL,
                    fuel        TEXT NOT NULL,
                    perc        REAL NOT NULL,
                    PRIMARY KEY (captured_at, fuel)
                )
            """)
            self._conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_mix_history_time ON mix_history (captured_at)"
            )
            # Backfill mix_history from generation_mix for existing users upgrading
            filled = self._conn.execute("SELECT COUNT(*) FROM mix_history").fetchone()[0]
            if filled == 0:
                self._conn.execute("""
                    INSERT OR IGNORE INTO mix_history (captured_at, fuel, perc)
                    SELECT b.block_start, gm.fuel, gm.perc
                    FROM generation_mix gm
                    JOIN blocks b ON b.id = gm.block_id
                    WHERE b.meter_id = 'electricity_main'
                      AND b.block_start >= datetime('now', '-48 hours')
                """)
        except Exception:
            pass
        # CRITICAL (fresh-DB lock): the blocks above (schema executescript, the
        # mix_history CREATE/INSERT) can leave an open transaction on the
        # connection. A lingering transaction makes the engine's startup
        # wal_checkpoint(TRUNCATE) fail with "database table is locked" (same
        # root cause as the 2.x 'fresh install hang' — executescript leaving an
        # open read txn on a fresh WAL DB). Commit now so the connection is in a
        # clean autocommit state before any checkpoint/backup runs.
        try:
            self._conn.commit()
        except Exception:
            pass
        logger.debug("BlockStore opened: %s", db_path)

    # ── Connection management ─────────────────────────────────────────────

    def _apply_pragmas(self) -> None:
        # busy_timeout is critical: EMT runs TWO connections (engine loop + web
        # server) against one WAL database. Without it, any momentary write
        # contention (a config save, a checkpoint, a block finalise) makes the
        # other connection fail INSTANTLY with "database is locked" instead of
        # waiting. 5s lets brief contention resolve transparently. This was the
        # cause of the every-tick "database is locked" loop errors on fresh DBs.
        self._conn.executescript("""
            PRAGMA busy_timeout = 5000;
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
        _ds_cols = {r[1] for r in self._conn.execute("PRAGMA table_info(dispatch_slots)").fetchall()} if self._conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='dispatch_slots'").fetchone() else set()
        _dh_cols = {r[1] for r in self._conn.execute("PRAGMA table_info(dispatch_history)").fetchall()} if self._conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='dispatch_history'").fetchone() else set()
        _ph_cols = {r[1] for r in self._conn.execute("PRAGMA table_info(power_history)").fetchall()} if self._conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='power_history'").fetchone() else set()

        for _col, _tbl, _defn, _col_set in [
            ("protected",            "meters",         "INTEGER DEFAULT 0",  _m_cols),
            ("inverter_possible",    "meters",         "INTEGER DEFAULT 0",  _m_cols),
            ("power_sensor",         "meters",         "TEXT",               _m_cols),
            ("power_source",         "meters",         "TEXT",               _m_cols),
            ("rate_source",          "meters",         "TEXT",               _m_cols),
            ("postcode_prefix",      "meters",         "TEXT",               _m_cols),
            ("postcode_source",      "meters",         "TEXT",               _m_cols),
            ("v2x_capable",          "meters",         "INTEGER DEFAULT 0",  _m_cols),
            ("inverter_power_invert","meters",         "INTEGER DEFAULT 0",  _m_cols),
            ("power_invert",          "meters",         "INTEGER DEFAULT 0",  _m_cols),
            ("device_power_invert",    "meters",         "INTEGER DEFAULT 0",  _m_cols),
            ("meter_type",           "meters",         "TEXT",               _m_cols),
            ("soc_sensor",           "meters",         "TEXT",               _m_cols),
            ("inverter_power_sensor","meters",         "TEXT",               _m_cols),
            ("device_power_sensor",  "meters",         "TEXT",               _m_cols),
            ("pv_power_sensor",      "meters",         "TEXT",               _m_cols),
            ("retired_at",            "meters",         "TEXT",               _m_cols),
            ("retired_reason",        "meters",         "TEXT",               _m_cols),
            ("supplier",              "config_periods",  "TEXT",              _cp_cols),
            ("mpan",             "meter_channels",  "TEXT",              _mc_cols),
            ("tariff",           "meter_channels",  "TEXT",              _mc_cols),
            # ── 3.0.0 per-channel rate/standing-charge source (API vs sensor) ─
            ("rate_source",            "meter_channels",  "TEXT",        _mc_cols),
            ("standing_charge_source", "meter_channels",  "TEXT",        _mc_cols),
            ("carbon_g",         "blocks",          "REAL",              _b_cols),
            ("imp_provisional",  "blocks",          "INTEGER NOT NULL DEFAULT 0", _b_cols),
            # ── 3.0.0 Kraken API integration columns (upgrade path) ──────────
            ("source",             "blocks",        "TEXT",                       _b_cols),
            ("is_provisional",     "blocks",        "INTEGER NOT NULL DEFAULT 0", _b_cols),
            ("needs_pass2_rerun",  "blocks",        "INTEGER NOT NULL DEFAULT 0", _b_cols),
            ("imp_kwh_api",        "blocks",        "REAL",                       _b_cols),
            ("exp_kwh_api",        "blocks",        "REAL",                       _b_cols),
            ("finalised_from_cad", "blocks",        "INTEGER NOT NULL DEFAULT 0", _b_cols),
            ("needs_review",       "blocks",        "INTEGER NOT NULL DEFAULT 0", _b_cols),
            # ── 3.3.0 BL-18: why a block was flagged for review (drift vs dispatch)
            ("review_reason",      "blocks",        "TEXT",                       _b_cols),
            # ── #322: sticky "user dismissed this review" marker. Cleared review flags
            # were re-set every reconcile run (no memory of the dismissal), so the same
            # ambiguous dispatch blocks kept reappearing. Once dismissed, don't re-flag.
            ("review_dismissed",   "blocks",        "INTEGER NOT NULL DEFAULT 0", _b_cols),
            ("carbon_intensity_g", "blocks",        "REAL",                       _b_cols),
            ("rate_corrected",     "blocks",        "INTEGER NOT NULL DEFAULT 0", _b_cols),
            ("rate_reconciled",    "blocks",        "INTEGER NOT NULL DEFAULT 0", _b_cols),
            # ── 3.5.0 historical-import: link a reconstructed block to its derivation
            ("derivation_id",      "blocks",        "INTEGER",                    _b_cols),
            # ── 4.1.x BL-23: exc-VAT import cost (additive; NULL until captured)
            ("imp_cost_exc",       "blocks",        "REAL",                       _b_cols),
            # ── 4.2 BL-23: exc-VAT rate + standing + provenance (additive; NULL until captured)
            ("imp_rate_exc",       "blocks",        "REAL",                       _b_cols),
            ("standing_charge_exc","blocks",        "REAL",                       _b_cols),
            ("exc_source",         "blocks",        "TEXT",                       _b_cols),
            ("carbon_gco2_min",  "power_history",   "REAL",              _ph_cols),
            # ── 3.1.x dispatch lifecycle capture (observe-only, no billing effect)
            ("state",            "dispatch_slots", "TEXT",  _ds_cols),
            ("energy_planned",   "dispatch_slots", "REAL",  _ds_cols),
            ("energy_completed", "dispatch_slots", "REAL",  _ds_cols),
            # ── 3.4.0 BL-11: retain the exact (second-precision) dispatch window
            ("raw_start",        "dispatch_slots",   "TEXT",  _ds_cols),
            ("raw_end",          "dispatch_slots",   "TEXT",  _ds_cols),
            ("raw_start",        "dispatch_history", "TEXT",  _dh_cols),
            ("raw_end",          "dispatch_history", "TEXT",  _dh_cols),
        ]:
            if _col not in _col_set:
                try:
                    self._conn.execute(f"ALTER TABLE {_tbl} ADD COLUMN {_col} {_defn}")
                    self._conn.commit()
                except Exception:
                    pass  # already exists or table missing — migrate will handle it

        # ── 3.5.x: tag existing (hand-entered) postcode prefixes as user-sourced ──
        # A pre-existing postcode_prefix was typed in Settings, so record its
        # provenance as 'user'. A one-time upgrade probe (engine) may then
        # overwrite it from the authoritative Octopus supply address — outward
        # code only — so a legacy wrong-region entry self-corrects.
        try:
            self._conn.execute(
                "UPDATE meters SET postcode_source = 'user' "
                "WHERE postcode_prefix IS NOT NULL AND postcode_source IS NULL")
            self._conn.commit()
        except Exception:
            pass  # column not present yet on a very old shape — migrate handles it

        # ── 3.0.0: backfill per-channel rate / standing-charge source ─────────
        # Pre-3.0.0 (v2) configs predate the explicit "API vs sensor" toggles.
        # Default every existing channel to the source it was ALREADY using, so an
        # upgrade does not change a single price until the user opts in:
        #   • sub-meter (device) channel → derived from the meter's legacy
        #     rate_source AND its own sensor: an explicit own-sensor choice
        #     ('own'/'base'/'sensor') OR a device that has its own rate sensor
        #     mapped becomes 'sensor'; anything else (incl. 'overlay'/'inherit'/
        #     NULL with no own sensor) becomes 'main' — i.e. inherit the main
        #     meter's effective rate, as v2 sub-meters did via parent_rates.
        #   • main-meter channel → 'sensor' when a rate sensor is actually mapped,
        #     otherwise 'api' (the rate must have been coming from the supplier
        #     API). We never silently flip a sensor-fed meter onto the API.
        #   • standing charge → 'sensor' when a standing-charge sensor is mapped,
        #     otherwise 'api'.
        # Only NULL rows are touched, so explicit UI/wizard choices are preserved.
        try:
            _mc_now = [r[1] for r in self._conn.execute("PRAGMA table_info(meter_channels)").fetchall()]
            if "rate_source" in _mc_now:
                n1 = self._conn.execute(
                    """UPDATE meter_channels SET rate_source = (
                           SELECT CASE
                               WHEN m.is_sub_meter = 1 THEN
                                   CASE WHEN m.rate_source IN ('own','base','sensor')
                                        THEN 'sensor'
                                        WHEN meter_channels.rate_sensor IS NOT NULL
                                             AND TRIM(meter_channels.rate_sensor) <> ''
                                        THEN 'sensor'
                                        ELSE 'main' END
                               ELSE
                                   CASE WHEN meter_channels.rate_sensor IS NOT NULL
                                             AND TRIM(meter_channels.rate_sensor) <> ''
                                        THEN 'sensor' ELSE 'api' END
                           END
                           FROM meters m WHERE m.id = meter_channels.meter_id)
                       WHERE rate_source IS NULL"""
                ).rowcount
                n2 = self._conn.execute(
                    """UPDATE meter_channels SET standing_charge_source =
                           CASE WHEN standing_charge_sensor IS NOT NULL
                                     AND TRIM(standing_charge_sensor) <> ''
                                THEN 'sensor' ELSE 'api' END
                       WHERE standing_charge_source IS NULL"""
                ).rowcount
                if n1 or n2:
                    self._conn.commit()
                    logger.info(
                        "_ensure_schema: backfilled channel sources "
                        "(rate_source=%d, standing_charge_source=%d) for v2 upgrade",
                        n1, n2,
                    )
        except Exception:
            logger.debug("_ensure_schema: rate-source backfill skipped", exc_info=True)

        # Clear inverter_possible for all existing meters — feature removed in 2.9.0
        # All sub-meters now use the protected queue in PASS 2 regardless of this flag
        try:
            if "inverter_possible" in [r[1] for r in self._conn.execute(
                "PRAGMA table_info(meters)"
            ).fetchall()]:
                n = self._conn.execute(
                    "UPDATE meters SET inverter_possible=0 WHERE inverter_possible=1"
                ).rowcount
                if n:
                    self._conn.commit()
                    logger.info("_ensure_schema: cleared %d inverter_possible flag(s) (deprecated)", n)
        except Exception:
            pass

        # Create is_gap_seed index only if column exists (deferred for upgrade compat)
        cr_cols = [r[1] for r in self._conn.execute(
            "PRAGMA table_info(current_reads)"
        ).fetchall()]
        if "is_gap_seed" in cr_cols:
            self._conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_current_reads_gap "
                "ON current_reads (is_gap_seed)"
            )

        # ── 3.0.0 Kraken integration: duplicate detection + partial indexes ───
        # The blocks table has carried a table-level UNIQUE(block_start, meter_id)
        # constraint since its inception, so genuine duplicates should not exist.
        # We still check defensively before relying on the named unique index,
        # and deduplicate (keeping the highest id) if any are somehow present —
        # e.g. a DB hand-restored from a pre-constraint export.
        try:
            dupes = self._conn.execute(
                """SELECT block_start, meter_id, COUNT(*) AS n
                   FROM blocks
                   GROUP BY block_start, meter_id
                   HAVING n > 1"""
            ).fetchall()
            if dupes:
                for d in dupes:
                    self._conn.execute(
                        """DELETE FROM blocks
                           WHERE block_start = ? AND meter_id = ?
                             AND id < (SELECT MAX(id) FROM blocks
                                       WHERE block_start = ? AND meter_id = ?)""",
                        (d["block_start"], d["meter_id"],
                         d["block_start"], d["meter_id"]),
                    )
                    logger.warning(
                        "_ensure_schema: deduplicated block_start=%s meter_id=%s "
                        "(%d duplicate row(s) removed, kept highest id)",
                        d["block_start"], d["meter_id"], d["n"] - 1,
                    )
                self._conn.commit()
        except Exception:
            logger.exception("_ensure_schema: duplicate detection failed (non-fatal)")

        # Partial indexes for the Kraken sweeps. Created here (not in _DDL) because
        # they reference columns that only exist after the ALTER loop above on an
        # upgrade DB. IF NOT EXISTS makes this idempotent on fresh installs too.
        _b_cols_now = {r[1] for r in self._conn.execute("PRAGMA table_info(blocks)").fetchall()}
        if {"is_provisional", "needs_pass2_rerun", "needs_review"} <= _b_cols_now:
            self._conn.executescript(
                "CREATE UNIQUE INDEX IF NOT EXISTS idx_blocks_start_meter "
                "    ON blocks (block_start, meter_id);"
                "CREATE INDEX IF NOT EXISTS idx_blocks_provisional "
                "    ON blocks (is_provisional)    WHERE is_provisional = 1;"
                "CREATE INDEX IF NOT EXISTS idx_blocks_pass2_rerun "
                "    ON blocks (needs_pass2_rerun) WHERE needs_pass2_rerun = 1;"
                "CREATE INDEX IF NOT EXISTS idx_blocks_needs_review "
                "    ON blocks (needs_review)      WHERE needs_review = 1;"
            )
            self._conn.commit()

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

    def get_block_starts_missing_carbon(self, limit: Optional[int] = None) -> list:
        """Distinct block_start values for main-meter blocks whose carbon was
        never attributed (carbon_intensity_g IS NULL but the block has energy).

        Used by the carbon recovery sweep: blocks left NULL by an outage gap-fill
        that ran before a CI slot was available. Ordered oldest-first so the most
        recently-recoverable (still within the 4-day CI window) are handled.
        """
        sql = (
            "SELECT DISTINCT block_start FROM blocks "
            "WHERE carbon_intensity_g IS NULL "
            "  AND (imp_kwh IS NOT NULL OR exp_kwh IS NOT NULL) "
            # 3.5.0: skip reconstructed history — carbon for years-old imported
            # blocks is low value and its bulk backfill stalls the engine loop.
            "  AND (source IS NULL OR source NOT LIKE 'imported%') "
            "ORDER BY block_start"
        )
        if limit is not None:
            sql += " LIMIT ?"
            return [r["block_start"]
                    for r in self._conn.execute(sql, (limit,)).fetchall()]
        return [r["block_start"]
                for r in self._conn.execute(sql).fetchall()]

    # Region-eligibility for carbon backfill: a live/reconstructed block is always
    # eligible; an IMPORTED block is eligible only once its config period has a
    # known region (postcode_prefix). Imported blocks without a region stay NULL
    # (excluded) rather than being attributed to a guessed region.
    _CARBON_ELIGIBLE = (
        "((blocks.source IS NULL OR blocks.source NOT LIKE 'imported%') "
        " OR EXISTS (SELECT 1 FROM meters m "
        "            WHERE m.config_period_id = blocks.config_period_id "
        "              AND m.is_sub_meter = 0 "
        "              AND m.postcode_prefix IS NOT NULL))"
    )

    # Reconstructed device attribution from the HA recorder. A distinct source so the
    # reconstructed device layer is identifiable and removable WITHOUT touching live
    # or imported house totals — the reversibility contract (see attribution design).
    RECORDER_ATTRIBUTED_SOURCE = "recorder_attributed"
    _ATTRIBUTION_RUNS_KEY = "recorder_attribution_runs"   # store_meta ledger

    def get_missing_carbon_date_range(self) -> tuple | None:
        """(min_block_start, max_block_start) over energy-bearing blocks whose
        carbon_intensity_g IS NULL AND that are region-eligible (see
        _CARBON_ELIGIBLE). None when there are no such blocks.

        Bounds the historical carbon backfill: every block before CI first became
        available carries a NULL intensity; imported history joins the span once
        its period's region is known."""
        row = self._conn.execute(
            "SELECT MIN(block_start) AS lo, MAX(block_start) AS hi FROM blocks "
            "WHERE carbon_intensity_g IS NULL "
            "  AND (imp_kwh IS NOT NULL OR exp_kwh IS NOT NULL) "
            "  AND " + self._CARBON_ELIGIBLE
        ).fetchone()
        if row and row["lo"] and row["hi"]:
            return (row["lo"], row["hi"])
        return None

    def get_block_starts_missing_carbon_in_range(
        self, from_iso: str, to_iso: str, limit: Optional[int] = None
    ) -> list:
        """Distinct NULL-carbon, region-eligible block_start values within
        [from_iso, to_iso), oldest-first. Used per fetched window by the
        historical backfill."""
        sql = (
            "SELECT DISTINCT block_start FROM blocks "
            "WHERE carbon_intensity_g IS NULL "
            "  AND (imp_kwh IS NOT NULL OR exp_kwh IS NOT NULL) "
            "  AND " + self._CARBON_ELIGIBLE + " "
            "  AND block_start >= ? AND block_start < ? "
            "ORDER BY block_start"
        )
        params: list = [from_iso, to_iso]
        if limit is not None:
            sql += " LIMIT ?"
            params.append(limit)
        return [r["block_start"]
                for r in self._conn.execute(sql, params).fetchall()]

    def extend_earliest_period_to(self, block_start_iso: str) -> bool:
        """Move the earliest config period's `effective_from` back to cover
        imported history that predates it (snapped to local midnight). ONLY ever
        extends backward — never shrinks. Keeps imported blocks inside a period
        that temporally covers them and makes Billing history show the real data
        start rather than the go-live date. Returns True if it changed anything."""
        if not block_start_iso:
            return False
        row = self._conn.execute(
            "SELECT id, effective_from, timezone FROM config_periods "
            "ORDER BY effective_from ASC LIMIT 1"
        ).fetchone()
        if not row:
            return False
        snapped = self._snap_to_midnight_utc(block_start_iso, row["timezone"] or "UTC")
        if snapped >= row["effective_from"]:
            return False   # already covers it (or would shrink) — no-op
        with self._conn:
            self._conn.execute(
                "UPDATE config_periods SET effective_from = ? WHERE id = ?",
                (snapped, row["id"]))
        logger.info("extend_earliest_period_to: period %d effective_from %s -> %s",
                    row["id"], row["effective_from"], snapped)
        return True

    def rearm_carbon_backfill(self) -> None:
        """Clear the historical-carbon backfill marker so the next scheduler tick
        re-scans from scratch. Called when a region is newly assigned to a period
        (reconcile / manual edit / one-time probe), which makes previously
        region-unknown imported blocks eligible for carbon."""
        self.set_meta("carbon_backfill_state", {})

    def prune_carbon_intensity(self, days: int = 4) -> int:
        """Delete carbon_intensity rows older than `days` days. Returns rows deleted."""
        cutoff = (datetime.now(timezone.utc).replace(tzinfo=None)
                  - timedelta(days=days)).isoformat()
        with self._conn:
            cur = self._conn.execute(
                "DELETE FROM carbon_intensity WHERE captured_at < ?", (cutoff,)
            )
        return cur.rowcount

    # ── 3.1.0 Intelligent dispatch slots ────────────────────────────────────────

    def upsert_dispatch_slot(self, slot_start: str, *, off_peak: bool = True,
                             provider: Optional[str] = None,
                             source: Optional[str] = None,
                             state: Optional[str] = "planned",
                             energy_planned: Optional[float] = None,
                             energy_completed: Optional[float] = None,
                             raw_start: Optional[str] = None,
                             raw_end: Optional[str] = None,
                             captured_at: Optional[str] = None) -> None:
        """Record (or refresh) a 30-min slot that had an Intelligent smart-charge
        dispatch. Last-write-wins on slot_start. Captured forward each poll; the
        overlay reads these to decide off-peak candidacy (gated by meter draw at
        pricing time, NOT here).

        state / energy_planned / energy_completed are OBSERVE-ONLY lifecycle
        capture (see dispatch_validation_design.md) — recorded but not yet used
        for billing. energy_completed is preserved across refreshes if a later
        upsert doesn't supply it (a planned re-capture shouldn't wipe a settled
        figure), and likewise energy_planned once known.
        """
        if captured_at is None:
            captured_at = (datetime.now(timezone.utc)
                           .replace(tzinfo=None).isoformat())
        with self._conn:
            self._conn.execute(
                """INSERT INTO dispatch_slots
                       (slot_start, off_peak, provider, source, captured_at,
                        state, energy_planned, energy_completed, raw_start, raw_end)
                   VALUES (?,?,?,?,?,?,?,?,?,?)
                   ON CONFLICT(slot_start) DO UPDATE SET
                       off_peak    = excluded.off_peak,
                       provider    = excluded.provider,
                       source      = excluded.source,
                       captured_at = excluded.captured_at,
                       state       = COALESCE(excluded.state, dispatch_slots.state),
                       energy_planned =
                           COALESCE(excluded.energy_planned, dispatch_slots.energy_planned),
                       energy_completed =
                           COALESCE(excluded.energy_completed, dispatch_slots.energy_completed),
                       raw_start   = COALESCE(excluded.raw_start, dispatch_slots.raw_start),
                       raw_end     = COALESCE(excluded.raw_end,   dispatch_slots.raw_end)""",
                (slot_start, 1 if off_peak else 0, provider, source, captured_at,
                 state, energy_planned, energy_completed, raw_start, raw_end),
            )

    def get_dispatch_slot(self, slot_start: str) -> dict | None:
        """Return the dispatch_slots row for a slot, or None. Used by the overlay
        resolver to check whether a block's slot is an off-peak candidate.
        """
        row = self._conn.execute(
            "SELECT slot_start, off_peak, provider, source, captured_at, "
            "state, energy_planned, energy_completed, raw_start, raw_end "
            "FROM dispatch_slots WHERE slot_start = ?", (slot_start,)
        ).fetchone()
        return dict(row) if row else None

    def get_dispatch_slots_in_range(self, start_iso: str, end_iso: str) -> list:
        """All dispatch_slots with slot_start in [start_iso, end_iso). Ordered."""
        return [dict(r) for r in self._conn.execute(
            "SELECT slot_start, off_peak, provider, source, captured_at, "
            "state, energy_planned, energy_completed, raw_start, raw_end "
            "FROM dispatch_slots WHERE slot_start >= ? AND slot_start < ? "
            "ORDER BY slot_start", (start_iso, end_iso)
        ).fetchall()]

    def prune_dispatch_slots(self, days: int = 90) -> int:
        """Delete dispatch_slots older than `days` days. Generous retention (90d)
        — these are small and the overlay may reprice historical blocks during a
        billing-period review. Returns rows deleted.
        """
        cutoff = (datetime.now(timezone.utc).replace(tzinfo=None)
                  - timedelta(days=days)).isoformat()
        with self._conn:
            cur = self._conn.execute(
                "DELETE FROM dispatch_slots WHERE slot_start < ?", (cutoff,)
            )
        return cur.rowcount

    # ── Dispatch history (observe-only accumulation, design §11) ──────────────

    def record_dispatch_history(self, slot_start: str, kind: str, *,
                                provider: str | None = None,
                                source: str | None = None,
                                energy_kwh: float | None = None,
                                raw_start: str | None = None,
                                raw_end: str | None = None,
                                seen_at: str | None = None) -> None:
        """Accumulate one (slot_start, kind) dispatch observation. OBSERVE-ONLY —
        never read for billing. first_seen is set once and preserved; last_seen
        and the latest energy/provider/source refresh on every poll that sees it.
        Keeping a persistent record (vs the per-poll snapshot) is what makes a
        later 'never seen this slot' judgement trustworthy."""
        seen = seen_at or datetime.now(timezone.utc).replace(tzinfo=None).isoformat()
        with self._conn:
            self._conn.execute(
                """INSERT INTO dispatch_history
                       (slot_start, kind, provider, source, energy_kwh,
                        first_seen, last_seen, raw_start, raw_end)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT(slot_start, kind) DO UPDATE SET
                       provider   = COALESCE(excluded.provider, dispatch_history.provider),
                       source     = COALESCE(excluded.source,   dispatch_history.source),
                       energy_kwh = COALESCE(excluded.energy_kwh, dispatch_history.energy_kwh),
                       last_seen  = excluded.last_seen,
                       raw_start  = COALESCE(excluded.raw_start, dispatch_history.raw_start),
                       raw_end    = COALESCE(excluded.raw_end,   dispatch_history.raw_end)""",
                (slot_start, kind, provider, source, energy_kwh, seen, seen,
                 raw_start, raw_end))

    def get_dispatch_history(self, start_iso: str, end_iso: str,
                             kind: str | None = None) -> list:
        """Accumulated dispatch observations with slot_start in [start, end).
        Optionally filter by kind. Ordered by slot_start."""
        if kind is not None:
            rows = self._conn.execute(
                "SELECT slot_start, kind, provider, source, energy_kwh, "
                "first_seen, last_seen, raw_start, raw_end FROM dispatch_history "
                "WHERE slot_start >= ? AND slot_start < ? AND kind = ? "
                "ORDER BY slot_start", (start_iso, end_iso, kind)).fetchall()
        else:
            rows = self._conn.execute(
                "SELECT slot_start, kind, provider, source, energy_kwh, "
                "first_seen, last_seen, raw_start, raw_end FROM dispatch_history "
                "WHERE slot_start >= ? AND slot_start < ? "
                "ORDER BY slot_start, kind", (start_iso, end_iso)).fetchall()
        return [dict(r) for r in rows]

    def prune_dispatch_history(self, days: int = 90) -> int:
        """Delete dispatch_history older than `days`. Returns rows deleted."""
        cutoff = (datetime.now(timezone.utc).replace(tzinfo=None)
                  - timedelta(days=days)).isoformat()
        with self._conn:
            cur = self._conn.execute(
                "DELETE FROM dispatch_history WHERE slot_start < ?", (cutoff,))
        return cur.rowcount

    # ── Generation mix ────────────────────────────────────────────────────────

    def upsert_mix_history(self, captured_at: str, mix: list[dict]) -> None:
        """Write CI-tick generation mix to mix_history (48hr rolling store)."""
        with self._conn:
            for entry in mix:
                self._conn.execute(
                    """INSERT INTO mix_history (captured_at, fuel, perc)
                       VALUES (?, ?, ?)
                       ON CONFLICT(captured_at, fuel) DO UPDATE SET perc=excluded.perc""",
                    (captured_at, entry.get("fuel", ""), entry.get("perc", 0.0))
                )

    def prune_mix_history(self, hours: int = 48) -> None:
        """Delete mix_history rows older than `hours` hours."""
        from datetime import datetime, timezone, timedelta
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours)).strftime("%Y-%m-%dT%H:%M")
        with self._conn:
            self._conn.execute(
                "DELETE FROM mix_history WHERE captured_at < ?", (cutoff,)
            )

    def get_mix_history(self, hours: int = 48) -> list:
        """Return mix_history slots for the last `hours` hours."""
        from datetime import datetime, timezone, timedelta
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours)).strftime("%Y-%m-%dT%H:%M")
        rows = self._conn.execute(
            """SELECT captured_at, fuel, perc
               FROM mix_history
               WHERE captured_at >= ?
               ORDER BY captured_at ASC, fuel ASC""",
            (cutoff,)
        ).fetchall()
        slots: dict = {}
        order: list = []
        for r in rows:
            ca = r["captured_at"]
            if ca not in slots:
                slots[ca] = {}
                order.append(ca)
            slots[ca][r["fuel"]] = r["perc"]
        return [{"captured_at": ca, "fuels": slots[ca]} for ca in order]

    def retire_meter(self, meter_id: str, retired_at: str, retired_reason: str = "") -> None:
        """Mark a sub-meter as retired from the given date. Historical data is preserved.
        Also clears any stale current_reads entries so the engine does not record
        cost/rate data for the retired meter after this point."""
        with self._conn:
            self._conn.execute(
                """UPDATE meters SET retired_at = ?, retired_reason = ?
                   WHERE meter_id = ? AND is_sub_meter = 1""",
                (retired_at, retired_reason or None, meter_id)
            )
            # Clear live read/rate accumulation for this meter
            self._conn.execute(
                "DELETE FROM current_reads WHERE meter_id = ?",
                (meter_id,)
            )

    def unretire_meter(self, meter_id: str) -> None:
        """Clear retirement status from a sub-meter.
        Raises ValueError if any active meter already uses the same read sensor."""
        # Check for sensor conflicts with active (non-retired) meters
        conflicts = self._conn.execute(
            """SELECT m.meter_id, mc.read_sensor
               FROM meters m
               JOIN meter_channels mc ON mc.meter_id = m.id
               WHERE m.retired_at IS NULL
                 AND m.meter_id != ?
                 AND mc.read_sensor IN (
                     SELECT mc2.read_sensor FROM meter_channels mc2
                     JOIN meters m2 ON m2.id = mc2.meter_id
                     WHERE m2.meter_id = ?
                       AND mc2.read_sensor IS NOT NULL
                 )""",
            (meter_id, meter_id)
        ).fetchall()
        if conflicts:
            sensors = ", ".join(set(r["read_sensor"] for r in conflicts))
            meters  = ", ".join(set(r["meter_id"]   for r in conflicts))
            raise ValueError(
                f"Cannot unretire — sensor(s) {sensors} already in use by active meter(s): {meters}. "
                f"Retire or reconfigure the conflicting meter first."
            )
        with self._conn:
            self._conn.execute(
                "UPDATE meters SET retired_at = NULL, retired_reason = NULL WHERE meter_id = ?",
                (meter_id,)
            )

    def get_retired_meters(self) -> list:
        """Return all meters with a retirement date set."""
        rows = self._conn.execute(
            """SELECT meter_id, device_label, meter_type, retired_at, retired_reason
               FROM meters WHERE retired_at IS NOT NULL ORDER BY retired_at DESC"""
        ).fetchall()
        return [dict(r) for r in rows]

    def is_meter_retired(self, meter_id: str, as_of: str | None = None) -> bool:
        """Return True if the meter is retired as of the given date (default: today)."""
        from datetime import datetime, timezone
        check_date = as_of or datetime.now(timezone.utc).strftime("%Y-%m-%d")
        row = self._conn.execute(
            "SELECT retired_at FROM meters WHERE meter_id = ?", (meter_id,)
        ).fetchone()
        if not row or row["retired_at"] is None:
            return False
        return row["retired_at"] <= check_date

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

    def get_meta(self, key: str, default=None):
        """Generic KV read from store_meta. Values are JSON-encoded; the raw
        string is returned if it isn't valid JSON (e.g. legacy schema_version)."""
        import json as _json
        row = self._conn.execute(
            "SELECT value FROM store_meta WHERE key = ?", (key,)
        ).fetchone()
        if row and row["value"] is not None:
            try:
                return _json.loads(row["value"])
            except Exception:
                return row["value"]
        return default

    def set_meta(self, key: str, value) -> None:
        """Generic KV write to store_meta (value JSON-encoded). Used for the
        run-once historical-carbon-backfill marker / resume cursor."""
        import json as _json
        self._conn.execute(
            "INSERT INTO store_meta (key, value) VALUES (?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            (key, _json.dumps(value))
        )
        self._conn.commit()

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


    def _resolve_delete_meters(self, meter_id):
        """Normalize the meter sentinel for a block delete/preview.

        Returns None when *all* meters should be affected (meter_id is None,
        empty, or the UI sentinel "all" — previously this string was passed
        through as a literal filter, matching no row and silently deleting
        nothing). Otherwise returns a de-duplicated list of meter_ids: the
        requested meter plus, when it is a parent (main) meter, all of its
        sub-meters — so deleting a day for the main meter takes its device
        blocks with it and cannot leave an orphaned, internally-inconsistent
        bill line behind.
        """
        if meter_id in (None, "", "all"):
            return None
        ids = [meter_id]
        cur = self._conn.execute(
            "SELECT DISTINCT meter_id FROM meters WHERE parent_meter_id = ?",
            (meter_id,),
        )
        ids.extend(r["meter_id"] for r in cur.fetchall())
        return list(dict.fromkeys(ids))

    def _block_range_where(self, utc_start, utc_end, from_time, to_time, meter_ids,
                           reconstructed_only=False):
        """Build the shared WHERE clause + params for block date-range
        delete/preview. `meter_ids` is None (all meters) or a list of ids.

        `reconstructed_only` restricts to historical-import blocks
        (`source LIKE 'imported%'`), so a bad import can be rolled back without
        touching native live/kraken data."""
        clauses = ["block_start >= ?", "block_start < ?"]
        params  = [utc_start, utc_end]

        if reconstructed_only:
            clauses.append("source LIKE 'imported%'")

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

        if meter_ids is not None:
            placeholders = ",".join("?" * len(meter_ids))
            clauses.append(f"meter_id IN ({placeholders})")
            params.extend(meter_ids)

        return " AND ".join(clauses), params

    def _count_blocks_and_local_dates(self, where, params, tz_name):
        """Return (block_count, distinct_local_date_count) for a delete/preview
        WHERE clause. The day count is in the *local* timezone, not UTC: a BST
        local day starts at 23:00 UTC the previous day, so counting distinct
        date(block_start) on the stored UTC timestamps would report two calendar
        days for a single local day. Convert each block_start to local first."""
        n = self._conn.execute(
            f"SELECT COUNT(*) FROM blocks WHERE {where}", params).fetchone()[0]
        if not n:
            return 0, 0
        try:
            tz = ZoneInfo(tz_name)
        except Exception:
            tz = ZoneInfo("UTC")
        starts = self._conn.execute(
            f"SELECT DISTINCT block_start FROM blocks WHERE {where}", params).fetchall()
        dates = {
            datetime.fromisoformat(r[0]).replace(tzinfo=timezone.utc).astimezone(tz).date()
            for r in starts
        }
        return n, len(dates)

    def delete_blocks_for_date_range(
        self, from_date: str, to_date: str, meter_id: str | None = None,
        from_time: str = "00:00", to_time: str = "23:59", tz_name: str = "UTC",
        reconstructed_only: bool = False
    ) -> dict:
        """
        Delete all blocks within [from_date, to_date] local date range (inclusive),
        optionally restricted by time-of-day (UTC HH:MM) and/or to a single meter
        (which also pulls in that meter's sub-meters — see _resolve_delete_meters).

        `reconstructed_only=True` limits the delete to historical-import blocks
        (`source LIKE 'imported%'`) — the reconstructed-history rollback filter.

        Cascades to the rows that reference the deleted blocks: `reads` (FK
        block_id, deleted first so foreign_keys=ON can't error) and
        `generation_mix` (block_id). When the delete removes the most recent
        finalised block for the targeted meters (a "tail" delete), the engine's
        live read-state (current_block + current_reads) is cleared in the same
        transaction so the next block re-anchors from the next live read instead
        of spanning the deleted window.
        """
        if not from_date or not to_date:
            raise ValueError("from_date and to_date are required")
        if from_date > to_date:
            raise ValueError("from_date must not be after to_date")

        from_time = from_time or "00:00"
        to_time   = to_time   or "23:59"

        utc_start, utc_end = local_date_range_to_utc_bounds(from_date, to_date, tz_name)

        meter_ids = self._resolve_delete_meters(meter_id)
        where, params = self._block_range_where(
            utc_start, utc_end, from_time, to_time, meter_ids, reconstructed_only
        )

        n_blocks, n_dates = self._count_blocks_and_local_dates(where, params, tz_name)

        # Tail detection: is the newest finalised block within the targeted
        # meters inside the deleted range? If so the engine anchor must reset.
        if meter_ids is not None:
            ph = ",".join("?" * len(meter_ids))
            latest = self._conn.execute(
                f"SELECT MAX(block_start) AS m FROM blocks WHERE meter_id IN ({ph})",
                meter_ids,
            ).fetchone()["m"]
        else:
            latest = self._conn.execute(
                "SELECT MAX(block_start) AS m FROM blocks"
            ).fetchone()["m"]
        is_tail = bool(latest is not None and utc_start <= latest < utc_end)

        # Device-only delete? If the resolved target is a single sub-meter (its
        # parent's blocks are untouched and survive), the parent's stored
        # remainder for these windows is now stale — hand the parent + window
        # back so the caller can trigger a PASS-2 remainder recompute. A main
        # or all-meters delete takes the devices with it, so nothing to recompute.
        recompute_parent = None
        if meter_ids is not None and len(meter_ids) == 1:
            pr = self._conn.execute(
                "SELECT parent_meter_id FROM meters "
                "WHERE meter_id = ? AND is_sub_meter = 1 AND parent_meter_id IS NOT NULL "
                "LIMIT 1",
                (meter_ids[0],),
            ).fetchone()
            if pr and pr["parent_meter_id"]:
                recompute_parent = pr["parent_meter_id"]

        reads_deleted = 0
        mix_deleted   = 0
        with self._conn:
            if n_blocks:
                # Children first (reads FK -> blocks(id) with foreign_keys=ON).
                # Match by block id via subquery: no huge IN-list, no param cap.
                cur_r = self._conn.execute(
                    f"DELETE FROM reads WHERE block_id IN "
                    f"(SELECT id FROM blocks WHERE {where})",
                    params,
                )
                reads_deleted = cur_r.rowcount
                cur_m = self._conn.execute(
                    f"DELETE FROM generation_mix WHERE block_id IN "
                    f"(SELECT id FROM blocks WHERE {where})",
                    params,
                )
                mix_deleted = cur_m.rowcount
                self._conn.execute(f"DELETE FROM blocks WHERE {where}", params)
            if is_tail and n_blocks:
                # Reseed live engine state so the next block starts cleanly.
                self._conn.execute("DELETE FROM current_block")
                self._conn.execute("DELETE FROM current_reads")

        return {
            "deleted":                n_blocks,
            "dates":                  n_dates,
            "reads_deleted":          reads_deleted,
            "generation_mix_deleted": mix_deleted,
            "reseeded":               bool(is_tail and n_blocks),
            # set only for a device-only delete with surviving parent blocks
            "recompute_parent":       recompute_parent if n_blocks else None,
            "recompute_from":         utc_start if recompute_parent and n_blocks else None,
            "recompute_to":           utc_end if recompute_parent and n_blocks else None,
        }

    def count_blocks_for_date_range(
        self, from_date: str, to_date: str, meter_id: str | None = None,
        from_time: str = "00:00", to_time: str = "23:59", tz_name: str = "UTC",
        reconstructed_only: bool = False
    ) -> dict:
        """
        Preview how many blocks delete_blocks_for_date_range would remove, using
        the identical meter resolution (sentinel + sub-meter inclusion) and WHERE
        clause so the preview can never disagree with the delete. Honours
        `reconstructed_only` identically to the delete.
        Returns {"blocks": N, "dates": N_distinct_dates}.
        """
        from_time = from_time or "00:00"
        to_time   = to_time   or "23:59"

        utc_start, utc_end = local_date_range_to_utc_bounds(from_date, to_date, tz_name)
        meter_ids = self._resolve_delete_meters(meter_id)
        where, params = self._block_range_where(
            utc_start, utc_end, from_time, to_time, meter_ids, reconstructed_only
        )

        n_blocks, n_dates = self._count_blocks_and_local_dates(where, params, tz_name)
        return {"blocks": n_blocks, "dates": n_dates}

    def count_imported_history(self) -> dict:
        """How much historical-import data exists: block count + date span."""
        row = self._conn.execute(
            "SELECT COUNT(*) AS n, MIN(block_start) AS lo, MAX(block_start) AS hi "
            "FROM blocks WHERE source LIKE 'imported%'"
        ).fetchone()
        return {"blocks": (row["n"] or 0) if row else 0,
                "from": row["lo"] if row else None,
                "to":   row["hi"] if row else None}

    def purge_imported_history(self) -> dict:
        """The get-out-clause: remove ALL historical-import data and everything
        derived from it, returning the account to its live-only state so a fresh
        import can start clean.

        Removes: imported blocks (``source LIKE 'imported%'`` — WHOLE blocks, all
        channels, so no per-meter apportionment is left stale) and their `reads` /
        `generation_mix`; the CSV/API **rate derivations**; and the import
        **checkpoints + gap records** in `kraken_state`. Also clears any pending
        region-reconcile prompt and re-arms the carbon backfill. Live/CAD/settled
        blocks are untouched — imports never overlap them (go-live ceiling).
        """
        info = self.count_imported_history()
        with self._conn:
            reads = self._conn.execute(
                "DELETE FROM reads WHERE block_id IN "
                "(SELECT id FROM blocks WHERE source LIKE 'imported%')").rowcount
            mix = self._conn.execute(
                "DELETE FROM generation_mix WHERE block_id IN "
                "(SELECT id FROM blocks WHERE source LIKE 'imported%')").rowcount
            self._conn.execute("DELETE FROM blocks WHERE source LIKE 'imported%'")
            derivs = self._conn.execute(
                "DELETE FROM historical_derivation WHERE source LIKE 'imported%'").rowcount
            try:
                self._conn.execute(
                    "DELETE FROM kraken_state WHERE key IN "
                    "('api_import_oldest_import','api_import_done_import',"
                    " 'api_import_oldest_export','api_import_done_export',"
                    " 'import_gaps_import','import_gaps_export')")
            except Exception:
                pass   # table/keys absent → nothing to reset
        # Clear the post-import region prompt and let carbon re-scan the survivors.
        self.set_meta("region_reconcile_pending", None)
        self.rearm_carbon_backfill()
        logger.info("purge_imported_history: removed %d block(s), %d derivation(s)",
                    info["blocks"], derivs)
        return {"blocks": info["blocks"], "reads": reads, "generation_mix": mix,
                "derivations": derivs, "from": info["from"], "to": info["to"]}

    # ── Recorder device attribution — reversible layer ────────────────────────

    def delete_recorder_attributed(self, meter_id=None, from_iso=None,
                                   to_iso=None) -> dict:
        """Delete recorder-attributed device blocks, optionally scoped to a meter
        and/or a [from, to) window. The undo primitive: removes ONLY the
        reconstructed device rows (source='recorder_attributed'), never live or
        imported house totals. Returns {deleted, meters, parents, from, to} — the
        caller recomputes each affected parent's remainder over [from, to)."""
        where = ["source = ?"]
        params = [self.RECORDER_ATTRIBUTED_SOURCE]
        if meter_id:
            where.append("meter_id = ?"); params.append(meter_id)
        if from_iso:
            where.append("block_start >= ?"); params.append(from_iso)
        if to_iso:
            where.append("block_start < ?"); params.append(to_iso)
        clause = " AND ".join(where)
        meters = [r["meter_id"] for r in self._conn.execute(
            f"SELECT DISTINCT meter_id FROM blocks WHERE {clause}", params).fetchall()]
        parents = []
        for mid in meters:
            pr = self._conn.execute(
                "SELECT parent_meter_id FROM meters WHERE meter_id = ? "
                "AND parent_meter_id IS NOT NULL LIMIT 1", (mid,)).fetchone()
            p = pr["parent_meter_id"] if pr else None
            if p and p not in parents:
                parents.append(p)
        span = self._conn.execute(
            f"SELECT MIN(block_start) lo, MAX(block_start) hi FROM blocks WHERE {clause}",
            params).fetchone()
        with self._conn:
            self._conn.execute(
                "DELETE FROM reads WHERE block_id IN "
                f"(SELECT id FROM blocks WHERE {clause})", params)
            cur = self._conn.execute(f"DELETE FROM blocks WHERE {clause}", params)
        return {"deleted": cur.rowcount, "meters": meters, "parents": parents,
                "from": span["lo"] if span else None,
                "to": span["hi"] if span else None}

    def get_parent_meter_id(self, meter_id: str):
        """The parent meter_id of a sub-meter (or None). Falls back to the main
        (non-sub) meter when the sub-meter row has no explicit parent."""
        r = self._conn.execute(
            "SELECT parent_meter_id FROM meters WHERE meter_id = ? "
            "AND parent_meter_id IS NOT NULL LIMIT 1", (meter_id,)).fetchone()
        if r and r["parent_meter_id"]:
            return r["parent_meter_id"]
        main = self._conn.execute(
            "SELECT meter_id FROM meters WHERE is_sub_meter = 0 LIMIT 1").fetchone()
        return main["meter_id"] if main else None

    def get_device_live_coverage_start(self, meter_id: str):
        """The earliest block_start where *meter_id* has REAL (non-reconstructed)
        import data — i.e. any block whose source is not 'recorder_attributed' AND
        whose import is actually non-zero. This is the seam where a device's
        live/imported history begins; recorder attribution fills up to (but not
        into) it so the reconstructed layer butts cleanly against the real one.

        Crucially we require imp_kwh > 0: a *live* block that recorded zero import
        is a hole, not coverage (e.g. a sub-meter sensor that flat-lined at 0 for a
        stretch while the device was really drawing — the load then sits unsplit in
        the house remainder). Treating those leading zeros as 'not yet covered'
        advances the seam past them so attribution can heal them from the recorder
        instead of stopping short. Returns None when the device has no non-zero live
        history at all, in which case attribution may fill the full available range."""
        r = self._conn.execute(
            "SELECT MIN(block_start) AS lo FROM blocks "
            "WHERE meter_id = ? AND COALESCE(source, '') <> ? "
            "AND COALESCE(imp_kwh, 0) > 0",
            (meter_id, self.RECORDER_ATTRIBUTED_SOURCE)).fetchone()
        return r["lo"] if r and r["lo"] else None

    def sum_meter_import_kwh(self, meter_id: str, from_iso: str, to_iso: str) -> float:
        """Sum of a meter's raw import kWh over [from_iso, to_iso] (inclusive end).
        Used to sanity-check an attribution: a device can't consume more than the
        house drew from the grid over the same window."""
        r = self._conn.execute(
            "SELECT COALESCE(SUM(imp_kwh), 0.0) FROM blocks "
            "WHERE meter_id = ? AND block_start >= ? AND block_start <= ?",
            (meter_id, from_iso, to_iso)).fetchone()
        return float(r[0] or 0.0)

    def count_recorder_attributed(self) -> dict:
        """Summary of the reconstructed device layer: total rows + per-meter span,
        for the attribution page / back-out UI."""
        rows = self._conn.execute(
            "SELECT meter_id, COUNT(*) n, MIN(block_start) lo, MAX(block_start) hi "
            "FROM blocks WHERE source = ? GROUP BY meter_id ORDER BY meter_id",
            (self.RECORDER_ATTRIBUTED_SOURCE,)).fetchall()
        return {"total": sum(r["n"] for r in rows),
                "meters": [{"meter_id": r["meter_id"], "blocks": r["n"],
                            "from": r["lo"], "to": r["hi"]} for r in rows]}

    def record_attribution_run(self, run: dict) -> None:
        """Append a run to the attribution ledger (run_id, meter_id, span, sensors,
        created_at, blocks_written) so a user can see and undo each run."""
        runs = self.get_meta(self._ATTRIBUTION_RUNS_KEY, None) or []
        runs.append(dict(run))
        self.set_meta(self._ATTRIBUTION_RUNS_KEY, runs[-200:])

    def get_attribution_runs(self) -> list:
        return self.get_meta(self._ATTRIBUTION_RUNS_KEY, None) or []

    def remove_attribution_run(self, run_id: str) -> None:
        runs = self.get_meta(self._ATTRIBUTION_RUNS_KEY, None) or []
        self.set_meta(self._ATTRIBUTION_RUNS_KEY,
                      [r for r in runs if r.get("run_id") != run_id])

    # ── 3.5.0 Historical-import derivation provenance ─────────────────────────

    @staticmethod
    def _row_to_derivation(row) -> dict:
        """sqlite Row → dict, decoding the JSON columns (sensor_ids, params)."""
        d = dict(row)
        for k in ("sensor_ids", "params"):
            v = d.get(k)
            if v:
                try:
                    d[k] = json.loads(v)
                except (ValueError, TypeError):
                    pass  # leave raw string if it isn't valid JSON
        return d

    def insert_historical_derivation(
        self, scope: str, period_from: str, period_to: str, *,
        subject: str | None = None, sensor_ids=None, sensor_kind: str | None = None,
        params=None, derived_value: float | None = None,
        confirmed_value: float | None = None, method_version: int = 1,
        derived_at: str | None = None, source: str | None = None,
        notes: str | None = None,
    ) -> int:
        """Record one reconstruction (a device-attribution span, or a rate for a
        tariff-period×tier) and return its id. `sensor_ids`/`params` may be lists
        or dicts (stored as JSON). Pure insert — linking blocks is a separate
        step (tag_blocks_with_derivation)."""
        if scope not in ("device_attribution", "rate"):
            raise ValueError(f"invalid derivation scope: {scope!r}")
        cur = self._conn.execute(
            """INSERT INTO historical_derivation
                 (scope, subject, period_from, period_to, sensor_ids, sensor_kind,
                  params, derived_value, confirmed_value, method_version,
                  derived_at, source, superseded_by, notes)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,NULL,?)""",
            (scope, subject, period_from, period_to,
             json.dumps(sensor_ids) if sensor_ids is not None else None,
             sensor_kind,
             json.dumps(params) if params is not None else None,
             derived_value, confirmed_value, int(method_version),
             derived_at or _utc_now_iso(), source, notes),
        )
        self._conn.commit()
        return cur.lastrowid

    def get_historical_derivation(self, derivation_id: int) -> Optional[dict]:
        row = self._conn.execute(
            "SELECT * FROM historical_derivation WHERE id = ?",
            (derivation_id,)).fetchone()
        return self._row_to_derivation(row) if row else None

    def list_historical_derivations(
        self, *, scope: str | None = None, subject: str | None = None,
        current_only: bool = True,
    ) -> list[dict]:
        """List derivations, newest first. `current_only` hides superseded rows
        (the audit trail is kept, not deleted)."""
        clauses, params = [], []
        if scope:
            clauses.append("scope = ?"); params.append(scope)
        if subject is not None:
            clauses.append("subject = ?"); params.append(subject)
        if current_only:
            clauses.append("superseded_by IS NULL")
        where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
        rows = self._conn.execute(
            f"SELECT * FROM historical_derivation{where} "
            "ORDER BY derived_at DESC, id DESC", params).fetchall()
        return [self._row_to_derivation(r) for r in rows]

    def supersede_historical_derivation(self, old_id: int, new_id: int) -> None:
        """Mark `old_id` replaced by `new_id` (rebuild). Keeps the old row for
        audit; list_historical_derivations(current_only=True) then hides it."""
        self._conn.execute(
            "UPDATE historical_derivation SET superseded_by = ? WHERE id = ?",
            (new_id, old_id))
        self._conn.commit()

    def tag_blocks_with_derivation(
        self, derivation_id: int, meter_id: str, period_from: str, period_to: str,
    ) -> int:
        """Point a meter's blocks in [period_from, period_to) at a derivation.
        Returns the number of blocks tagged. Used after writing reconstructed
        blocks so each can answer 'which sensor/rate produced this'."""
        cur = self._conn.execute(
            """UPDATE blocks SET derivation_id = ?
               WHERE meter_id = ? AND block_start >= ? AND block_start < ?""",
            (derivation_id, meter_id, period_from, period_to))
        self._conn.commit()
        return cur.rowcount

    def _imported_config_period_id(self, block_start: str):
        """Config period id for a reconstructed block: the covering period, or —
        for pre-EMT dates no live period covers — the OLDEST existing period (for
        its timezone/billing-day/block_minutes). Returns (id, block_minutes) or
        (None, 30)."""
        cp = self.get_config_period_for_date(block_start)
        if not cp:
            row = self._conn.execute(
                "SELECT id, block_minutes FROM config_periods "
                "ORDER BY effective_from ASC LIMIT 1").fetchone()
            if not row:
                return None, 30
            return row["id"], int(row["block_minutes"] or 30)
        return cp["id"], int(cp.get("block_minutes") or 30)

    def upsert_imported_block(
        self, block_start: str, meter_id: str, channel: str, *,
        kwh: float, rate: float | None, cost: float | None,
        standing: float | None = None, source: str = IMPORTED_SOURCE_CSV,
        derivation_id: int | None = None, overwrite: bool = False,
        cost_exc=None, rate_exc=None, standing_exc=None, exc_source=None,
    ) -> tuple[Optional[int], bool]:
        """Insert one reconstructed block for a single channel — FIRST-MAN-WINS.

        A CSV/bill import must never clobber a block that already exists: the user
        can't tell exactly where their CSV butts up against data EMT already holds,
        so the rule is that whoever wrote a channel first keeps it. If this
        (block_start, meter_id) already carries data for `channel`, it is LEFT
        UNTOUCHED (this protects live/settled readings AND a previous import). Only a
        channel that's currently empty is filled — so import + export of a genuinely
        new block both land, and a later export CSV can still fill the export column
        of an import-only row. To change an existing block, delete it first (Delete
        Blocks) and re-fill from any source. Pass `overwrite=True` to force replace.

        Import and export share ONE row; `cost` is inc-VAT £ (billing sums it), `rate`
        is £/kWh, `standing` is the day's charge. Returns (block_id, wrote) — wrote is
        False when the slot already held data and was left as-is."""
        kcol, rcol, ccol = (("imp_kwh", "imp_rate", "imp_cost") if channel == "import"
                            else ("exp_kwh", "exp_rate", "exp_cost"))
        cp_id, bm = self._imported_config_period_id(block_start)
        if cp_id is None:
            return None, False
        try:
            block_end = (datetime.fromisoformat(block_start)
                         + timedelta(minutes=bm)).isoformat()
        except ValueError:
            return None, False
        with self._conn:
            if not overwrite:
                ex = self._conn.execute(
                    f"SELECT id, {kcol} AS v FROM blocks "
                    "WHERE block_start = ? AND meter_id = ?",
                    (block_start, meter_id)).fetchone()
                if ex is not None and ex["v"] is not None:
                    return ex["id"], False        # first-man wins: leave it as-is
            is_import = channel == "import"
            # BL-23 (4.2 Slice D): persist ex-VAT for the import channel (NULL otherwise).
            exc_cols = (", imp_cost_exc, imp_rate_exc, standing_charge_exc, exc_source"
                        if is_import else "")
            exc_ph = ", ?, ?, ?, ?" if is_import else ""
            exc_upd = (""",
                       imp_cost_exc = excluded.imp_cost_exc,
                       imp_rate_exc = excluded.imp_rate_exc,
                       standing_charge_exc = CASE
                           WHEN excluded.standing_charge_exc IS NULL
                                OR excluded.standing_charge_exc = 0
                                THEN blocks.standing_charge_exc
                           ELSE excluded.standing_charge_exc END,
                       exc_source = excluded.exc_source""" if is_import else "")
            _p = [block_start, block_end, meter_id, cp_id,
                  kwh, rate, cost, (standing or 0.0), source, derivation_id]
            if is_import:
                _p += [cost_exc, rate_exc, standing_exc, exc_source]
            cur = self._conn.execute(
                f"""INSERT INTO blocks
                      (block_start, block_end, meter_id, config_period_id,
                       interpolated, {kcol}, {rcol}, {ccol}, standing_charge,
                       source, derivation_id{exc_cols})
                    VALUES (?,?,?,?,0,?,?,?,?,?,?{exc_ph})
                    ON CONFLICT(block_start, meter_id) DO UPDATE SET
                       {kcol} = excluded.{kcol},
                       {rcol} = excluded.{rcol},
                       {ccol} = excluded.{ccol},
                       standing_charge = CASE
                           WHEN excluded.standing_charge IS NULL
                                OR excluded.standing_charge = 0
                                THEN blocks.standing_charge   -- export (0) never clobbers
                           WHEN blocks.standing_charge IS NULL
                                OR blocks.standing_charge = 0
                           THEN excluded.standing_charge ELSE blocks.standing_charge END,
                       derivation_id = COALESCE(blocks.derivation_id, excluded.derivation_id){exc_upd}""",
                _p)
            row = self._conn.execute(
                "SELECT id FROM blocks WHERE block_start = ? AND meter_id = ?",
                (block_start, meter_id)).fetchone()
        return (row["id"] if row else cur.lastrowid), True

    def upsert_imported_blocks(
        self, rows: list, meter_id: str, channel: str, *,
        source: str = IMPORTED_SOURCE_CSV, derivation_id: int | None = None,
    ) -> int:
        """Bulk insert-or-merge reconstructed blocks for one channel in a SINGLE
        transaction (one commit for the whole batch), returning the count written.

        This is the hot path for the API import: committing per block (thousands
        of fsyncs) blocks the async event loop long enough to starve the HA
        WebSocket heartbeat → reconnect storm. One transaction per chunk fixes it.
        `rows`: list of {start, kwh, rate, cost, standing}. This is the API hot path
        and OVERWRITES on conflict (re-fetch corrects prices) — unlike the singular
        upsert_imported_block, which is first-man-wins for CSV/bill imports.
        Config-period id is cached per day."""
        is_import = channel == "import"
        kcol, rcol, ccol = (("imp_kwh", "imp_rate", "imp_cost") if is_import
                            else ("exp_kwh", "exp_rate", "exp_cost"))
        # BL-23 (4.2): persist ex-VAT figures for the IMPORT channel when the row
        # carries them (NULL otherwise — inc figures unaffected). Export exc is out of
        # scope (phase D+). Additive: rows without these keys write NULL.
        exc_cols = (", imp_cost_exc, imp_rate_exc, standing_charge_exc, exc_source"
                    if is_import else "")
        exc_ph = ", ?, ?, ?, ?" if is_import else ""
        exc_upd = (""",
                      imp_cost_exc = excluded.imp_cost_exc,
                      imp_rate_exc = excluded.imp_rate_exc,
                      standing_charge_exc = CASE
                          WHEN excluded.standing_charge_exc IS NULL
                               OR excluded.standing_charge_exc = 0
                               THEN blocks.standing_charge_exc  -- export (0) never clobbers
                          ELSE excluded.standing_charge_exc END,
                      exc_source = excluded.exc_source""" if is_import else "")
        sql = (f"""INSERT INTO blocks
                     (block_start, block_end, meter_id, config_period_id,
                      interpolated, {kcol}, {rcol}, {ccol}, standing_charge,
                      source, derivation_id{exc_cols})
                   VALUES (?,?,?,?,0,?,?,?,?,?,?{exc_ph})
                   ON CONFLICT(block_start, meter_id) DO UPDATE SET
                      {kcol} = excluded.{kcol},
                      {rcol} = excluded.{rcol},
                      {ccol} = excluded.{ccol},
                      standing_charge = CASE
                          WHEN excluded.standing_charge IS NULL
                               OR excluded.standing_charge = 0
                               THEN blocks.standing_charge   -- export (0) never clobbers
                          WHEN blocks.source LIKE 'imported%'
                               OR blocks.standing_charge IS NULL
                               OR blocks.standing_charge = 0
                          THEN excluded.standing_charge ELSE blocks.standing_charge END,
                      source = excluded.source,
                      derivation_id = COALESCE(blocks.derivation_id, excluded.derivation_id){exc_upd}""")
        cp_cache: dict = {}
        written = 0
        with self._conn:      # ONE transaction / one commit for the whole batch
            for r in rows:
                start = r.get("start")
                if not start:
                    continue
                day = start[:10]
                if day not in cp_cache:
                    cp_cache[day] = self._imported_config_period_id(start)
                cp_id, bm = cp_cache[day]
                if cp_id is None:
                    continue
                try:
                    block_end = (datetime.fromisoformat(start)
                                 + timedelta(minutes=bm)).isoformat()
                except ValueError:
                    continue
                params = [
                    start, block_end, meter_id, cp_id,
                    r.get("kwh") or 0.0, r.get("rate"), r.get("cost"),
                    (r.get("standing") or 0.0), source, derivation_id]
                if is_import:      # BL-23 exc fields (NULL when the row omits them)
                    params += [r.get("cost_exc"), r.get("rate_exc"),
                               r.get("standing_exc"), r.get("exc_source")]
                self._conn.execute(sql, params)
                written += 1
        return written

    # ── Import re-price repair queue ─────────────────────────────────────────
    # During the bulk import, some Measurements responses come back with the kWh
    # but no billed cost / TOU bucket (a degraded/partial response under load),
    # so those half-hours are priced from the tariff *schedule* (peak if they fall
    # outside the fixed off-peak window) instead of Octopus's real dispatch-aware
    # OFF_PEAK cost. We record each such slot here; a calm, targeted re-fetch
    # (engine.repair_import_pricing) later re-queries just these and re-prices in
    # place. The Measurements API DOES have the cost — proven by the cost probe —
    # so a small quiet query reliably returns it.
    def add_reprice_queue(self, channel: str, starts) -> None:
        """Queue imported block_starts (for `channel`) that were schedule-priced
        because Measurements returned no billed cost. Deduped, persisted in meta."""
        starts = [s for s in (starts or []) if s]
        if not starts:
            return
        q = self.get_meta("import_reprice_queue", None) or {}
        merged = set(q.get(channel) or []) | set(starts)
        q[channel] = sorted(merged)
        self.set_meta("import_reprice_queue", q)

    def get_reprice_queue(self) -> dict:
        """{'import': [start,...], 'export': [...]} of slots awaiting re-price."""
        return self.get_meta("import_reprice_queue", None) or {}

    def reprice_queue_count(self) -> int:
        q = self.get_reprice_queue()
        return sum(len(v or []) for v in q.values())

    def clear_reprice_queue(self) -> None:
        """Empty the whole reprice queue — used when a NEW import starts, so the
        previous run's flagged slots don't carry over into the fresh run's health."""
        self.set_meta("import_reprice_queue", {})

    def clear_reprice_queue_slots(self, channel: str, starts) -> None:
        """Remove `starts` from the channel's queue (they've been re-priced)."""
        if not starts:
            return
        q = self.get_meta("import_reprice_queue", None) or {}
        if channel not in q:
            return
        remaining = sorted(set(q[channel]) - set(starts))
        if remaining:
            q[channel] = remaining
        else:
            q.pop(channel, None)
        self.set_meta("import_reprice_queue", q)

    def reprice_imported_block(self, start: str, meter_id: str, channel: str,
                               rate, cost, cost_exc=None, exc_source=None) -> bool:
        """Update an IMPORTED block's rate + cost in place (source LIKE
        'imported%' only — never touches live/settled rows), ONLY when the value
        actually differs (so the returned rowcount = a genuine correction, which
        the repair pass uses as its 'recovered' signal). Returns True if changed.

        BL-23 (4.2 C3): when a re-fetch supplies the ex-VAT cost too (import channel),
        correct imp_cost_exc + exc_source in the same UPDATE, so the repair/settlement
        reprice keeps the exc figure in step with the billed inc cost."""
        rcol, ccol = (("imp_rate", "imp_cost") if channel == "import"
                      else ("exp_rate", "exp_cost"))
        set_exc, extra = "", []
        if channel == "import" and cost_exc is not None:
            set_exc = ", imp_cost_exc = ?, exc_source = ?"
            extra = [cost_exc, exc_source or "measurement"]
        with self._conn:
            cur = self._conn.execute(
                f"UPDATE blocks SET {rcol} = ?, {ccol} = ?{set_exc} "
                f"WHERE block_start = ? AND meter_id = ? AND source LIKE 'imported%' "
                f"AND (COALESCE({rcol}, -1) != ? OR COALESCE({ccol}, -1) != ?)",
                (rate, cost, *extra, start, meter_id, rate, cost))
        return cur.rowcount > 0

    def set_block_carbon(self, block_start: str, meter_id: str,
                         intensity_g, carbon_g) -> bool:
        """Write carbon IN PLACE for one block+meter — a targeted UPDATE of only
        the carbon columns. Critically it does NOT round-trip the whole row: the
        read→mutate→append_block_replace path drops `source` (and imp_kwh_api /
        is_provisional / derivation_id, and re-homes config_period_id), which
        silently untagged every imported block the carbon backfill touched. This
        preserves the tag and everything else. Returns True if a row changed."""
        with self._conn:
            cur = self._conn.execute(
                "UPDATE blocks SET carbon_intensity_g = ?, carbon_g = ? "
                "WHERE block_start = ? AND meter_id = ?",
                (intensity_g, carbon_g, block_start, meter_id))
        return cur.rowcount > 0

    def get_import_blocks_missing_exc(self, limit: int = 2000,
                                      after_start: str | None = None) -> list:
        """4.2 exc historical backfill: page IMPORT blocks that carry energy but no
        ex-VAT cost yet (imp_cost_exc IS NULL, imp_kwh > 0). Covers both historically
        IMPORTED blocks (source LIKE 'imported%') AND SETTLED live blocks
        (imp_kwh_api IS NOT NULL) — the latter were captured live before ex-VAT
        existed and have already settled via DCC, so settlement capture will never
        re-stamp them; without this they stay on the inc÷VAT approximation forever.
        Truly-unsettled live blocks are excluded — they get real exc at settlement.
        Ordered by block_start; pass the last start back as `after_start` to continue.
        Returns [{start, meter_id, kwh, rate}]."""
        sql = ("SELECT block_start, meter_id, imp_kwh, imp_rate FROM blocks "
               "WHERE imp_cost_exc IS NULL AND imp_kwh IS NOT NULL AND imp_kwh > 0 "
               "AND (source LIKE 'imported%' OR imp_kwh_api IS NOT NULL)")
        params: list = []
        if after_start is not None:
            sql += " AND block_start > ?"
            params.append(after_start)
        sql += " ORDER BY block_start LIMIT ?"
        params.append(int(limit))
        rows = self._conn.execute(sql, params).fetchall()
        return [{"start": r["block_start"], "meter_id": r["meter_id"],
                 "kwh": r["imp_kwh"], "rate": r["imp_rate"]} for r in rows]

    def count_import_blocks_missing_exc(self) -> int:
        """Count of import blocks still lacking ex-VAT cost — the backfill's
        remaining-work signal (0 ⇒ mark the pass done). Same coverage as
        get_import_blocks_missing_exc: imported OR settled live blocks."""
        row = self._conn.execute(
            "SELECT COUNT(*) AS n FROM blocks WHERE imp_cost_exc IS NULL "
            "AND imp_kwh IS NOT NULL AND imp_kwh > 0 "
            "AND (source LIKE 'imported%' OR imp_kwh_api IS NOT NULL)"
        ).fetchone()
        return int(row["n"]) if row else 0

    def set_block_exc(self, block_start: str, meter_id: str, cost_exc, rate_exc,
                      exc_source: str = "tariff") -> bool:
        """Write ex-VAT import figures IN PLACE for one block+meter — a targeted
        UPDATE of only the exc columns (mirrors set_block_carbon; no row round-trip,
        so source / imp_kwh_api / derivation_id are preserved). Only fills a NULL
        imp_cost_exc (idempotent — never overwrites captured exc) and NEVER touches
        the inc columns, so billing figures are unchanged. Returns True if a row
        changed."""
        with self._conn:
            cur = self._conn.execute(
                "UPDATE blocks SET imp_cost_exc = ?, imp_rate_exc = ?, exc_source = ? "
                "WHERE block_start = ? AND meter_id = ? AND imp_cost_exc IS NULL "
                "AND (source LIKE 'imported%' OR imp_kwh_api IS NOT NULL)",
                (cost_exc, rate_exc, exc_source, block_start, meter_id))
        return cur.rowcount > 0

    def set_blocks_exc(self, updates) -> int:
        """Batch ex-VAT writer: apply many `set_block_exc` UPDATEs inside ONE
        transaction (a single commit/fsync for the whole chunk, not one per block —
        the per-block commit made the historical backfill a minutes-long fsync storm
        that blocked the event loop). `updates` = iterable of
        (block_start, meter_id, cost_exc, rate_exc, exc_source). Same NULL-only
        guard and coverage (imported OR settled live) as set_block_exc. Returns the
        number of rows changed."""
        changed = 0
        with self._conn:
            for bs, mid, ce, rx, src in updates:
                cur = self._conn.execute(
                    "UPDATE blocks SET imp_cost_exc = ?, imp_rate_exc = ?, exc_source = ? "
                    "WHERE block_start = ? AND meter_id = ? AND imp_cost_exc IS NULL "
                    "AND (source LIKE 'imported%' OR imp_kwh_api IS NOT NULL)",
                    (ce, rx, src, bs, mid))
                changed += cur.rowcount
        return changed

    def get_vat_calendar(self) -> list:
        """Learned VAT-rate boundaries [(effective_from, rate), …] from store_meta.
        Merged with the statutory seed by vat_calendar.resolve_vat; empty until the
        engine observes a VAT change in the tariff."""
        raw = self.get_meta("vat_calendar", []) or []
        out = []
        for e in raw:
            try:
                d, r = e
                out.append((str(d)[:10], float(r)))
            except Exception:
                continue
        return out

    def set_vat_calendar(self, entries) -> None:
        """Persist the learned VAT boundaries (list of (date, rate))."""
        self.set_meta("vat_calendar",
                      [[str(d)[:10], float(r)] for d, r in (entries or [])])

    def vat_rate_at(self, date_iso) -> float:
        """The VAT rate effective at `date_iso` from the seed + learned calendar —
        the fallback rate used where no per-slot inc/exc pair is available."""
        import vat_calendar as _vc
        return _vc.resolve_vat(date_iso, self.get_vat_calendar())

    def retag_untagged_imports(self) -> dict:
        """Repair blocks the carbon round-trip bug wiped from 'imported_api' to
        NULL. Anchor go-live to the earliest LIVE-signature block (a real meter
        read, or a kraken_api source); anything BEFORE it that is NULL-source with
        no meter read is reconstruction, so re-tag it 'imported_api'. Never touches
        live/settled rows. Idempotent. Returns {retagged, go_live}."""
        row = self._conn.execute(
            "SELECT MIN(block_start) AS gl FROM blocks "
            "WHERE imp_read_start IS NOT NULL OR source = 'kraken_api'").fetchone()
        go_live = row["gl"] if row else None
        if not go_live:
            return {"retagged": 0, "go_live": None}
        with self._conn:
            cur = self._conn.execute(
                "UPDATE blocks SET source = 'imported_api' "
                "WHERE source IS NULL AND block_start < ? AND imp_read_start IS NULL",
                (go_live,))
        logger.info("retag_untagged_imports: re-tagged %d block(s) before go-live %s",
                    cur.rowcount, go_live)
        return {"retagged": cur.rowcount, "go_live": go_live}

    def sweep_implausible_sub_blocks(self, *, max_kw: float = 120.0,
                                     dry_run: bool = True) -> dict:
        """One-off repair for the #307 lost-opener spike: a device sub-meter whose
        OPENING register read was lost/zeroed booked its whole lifetime register as
        one interval (prod: house_battery read_start=0 / read_end=6137 → 6137 kWh in
        a half-hour, whose carbon = 6137×intensity dwarfed the real total).

        Physically impossible: no domestic device moves > `max_kw`. Any sub-meter
        block whose imp_kwh exceeds the block's ceiling (`block_minutes/60 × max_kw`;
        120 kW → 60 kWh for a half-hour) is a lost-opener glitch. For each:
          • set imp_kwh = imp_kwh_grid (the PASS-2 grid-clipped, house-bounded value
            — the only trustworthy part; the glitch lives in the remainder), and
            BASELINE imp_read_start onto imp_read_end so the register stays continuous
            (the next block still opens at the true register);
          • zero imp_kwh_remainder; recompute carbon_g from the corrected kWh;
          • flag needs_review = 1 with a reason.
        LEAVES imp_kwh_grid and imp_cost UNTOUCHED — the grid clip already bounded them
        to the correct house import, so billing/cost is unaffected.

        dry_run=True previews only. Returns {count, ceiling_kwh, applied, blocks:[...]}.
        """
        row = self._conn.execute(
            "SELECT block_minutes FROM config_periods "
            "ORDER BY effective_from DESC LIMIT 1").fetchone()
        block_minutes = (row["block_minutes"] if row and row["block_minutes"] else 30)
        ceiling = round(block_minutes / 60.0 * float(max_kw), 3)
        subs = [r["meter_id"] for r in self._conn.execute(
            "SELECT DISTINCT meter_id FROM meters WHERE parent_meter_id IS NOT NULL")]
        if not subs:
            return {"count": 0, "ceiling_kwh": ceiling, "applied": False, "blocks": []}
        ph = ",".join("?" * len(subs))
        rows = self._conn.execute(
            f"SELECT block_start, meter_id, imp_kwh, imp_kwh_grid, imp_read_start, "
            f"       imp_read_end, carbon_g, carbon_intensity_g "
            f"FROM blocks WHERE meter_id IN ({ph}) AND imp_kwh > ? "
            f"ORDER BY imp_kwh DESC", (*subs, ceiling)).fetchall()
        preview = [{"block_start": r["block_start"], "meter_id": r["meter_id"],
                    "imp_kwh": r["imp_kwh"],
                    "corrected_kwh": r["imp_kwh_grid"] or 0.0,
                    "read_start": r["imp_read_start"], "read_end": r["imp_read_end"],
                    "carbon_g": r["carbon_g"]} for r in rows]
        if not dry_run and rows:
            with self._conn:
                self._conn.execute(
                    f"UPDATE blocks SET "
                    f"  imp_kwh = COALESCE(imp_kwh_grid, 0), "
                    f"  imp_read_start = COALESCE(imp_read_end, imp_read_start), "
                    f"  imp_kwh_remainder = 0, "
                    f"  carbon_g = ROUND(COALESCE(imp_kwh_grid, 0) "
                    f"                   * COALESCE(carbon_intensity_g, 0), 4), "
                    f"  needs_review = 1, "
                    f"  review_reason = 'implausible sub-meter kWh clamped "
                    f"(#307 lost opener)' "
                    f"WHERE meter_id IN ({ph}) AND imp_kwh > ?", (*subs, ceiling))
        logger.info("sweep_implausible_sub_blocks: %s %d block(s) over %.1f kWh "
                    "ceiling (%d kW × %d min)",
                    "clamped" if (not dry_run and rows) else "previewed",
                    len(rows), ceiling, int(max_kw), block_minutes)
        return {"count": len(rows), "ceiling_kwh": ceiling,
                "applied": bool(not dry_run and rows), "blocks": preview}

    def sweep_register_glitches(self, *, recover_within_blocks: int = 48,
                                tol: float = 0.02, dry_run: bool = True) -> dict:
        """Repair phantom sub-meter deltas from a register that DIPPED below its
        established level and later RECOVERED — a stale/dropout reading (e.g. the
        2026-07-21 house_battery briefly surfacing a 4-day-old 6259.77 while the true
        register was ~6309), whose climb back toward the real value was booked as
        ~49 kWh of consumption.

        A cumulative register can only fall via a genuine reset (to ~0, staying low).
        A dip that RECOVERS to the prior high-water is therefore a glitch, and any
        'consumption' recorded while below that high-water is phantom. Conservative:
        only a dip that provably recovers within `recover_within_blocks` is touched;
        a drop that never recovers is left alone (it may be a real counter reset).

        For each phantom block: set imp_kwh = imp_kwh_grid (the trustworthy,
        house-clipped value — usually 0), recompute carbon from it, flag needs_review.
        LEAVES imp_kwh_grid / imp_cost untouched, so the bill is unaffected. Mirrors
        the #307 sweep's corrective action; only the detection differs.

        dry_run=True previews only. Returns {count, applied, blocks:[...]}.
        """
        subs = [r["meter_id"] for r in self._conn.execute(
            "SELECT DISTINCT meter_id FROM meters WHERE parent_meter_id IS NOT NULL")]
        to_fix = []
        for mid in subs:
            rows = self._conn.execute(
                "SELECT block_start, imp_kwh, imp_kwh_grid, imp_read_end, "
                "carbon_intensity_g FROM blocks WHERE meter_id = ? "
                "AND imp_read_end IS NOT NULL ORDER BY block_start", (mid,)).fetchall()
            reads = [r["imp_read_end"] for r in rows]
            n = len(rows)
            prior_max = None
            for i in range(n):
                re = reads[i]
                if re is None:
                    continue
                if prior_max is None:
                    prior_max = re
                    continue
                if re < prior_max - tol:
                    # Dip below the established register level. Recovers?
                    recovered = any(
                        reads[k] is not None and reads[k] >= prior_max - tol
                        for k in range(i + 1, min(n, i + 1 + recover_within_blocks)))
                    if recovered:
                        r = rows[i]
                        corr = r["imp_kwh_grid"] or 0.0
                        if round((r["imp_kwh"] or 0.0) - corr, 6) != 0.0:
                            to_fix.append({
                                "meter_id": mid, "block_start": r["block_start"],
                                "old_kwh": r["imp_kwh"], "corrected_kwh": corr,
                                "ci": r["carbon_intensity_g"] or 0.0})
                    # do NOT raise prior_max — re is below it
                else:
                    prior_max = max(prior_max, re)
        if not dry_run and to_fix:
            with self._conn:
                for f in to_fix:
                    self._conn.execute(
                        "UPDATE blocks SET imp_kwh = ?, imp_kwh_remainder = 0, "
                        "carbon_g = ROUND(? * ?, 4), needs_review = 1, "
                        "review_reason = 'register dip-and-recover phantom clamped' "
                        "WHERE meter_id = ? AND block_start = ?",
                        (f["corrected_kwh"], f["corrected_kwh"], f["ci"],
                         f["meter_id"], f["block_start"]))
        logger.info("sweep_register_glitches: %s %d phantom dip-and-recover block(s)",
                    "clamped" if (not dry_run and to_fix) else "previewed", len(to_fix))
        return {"count": len(to_fix), "applied": bool(not dry_run and to_fix),
                "blocks": to_fix}

    def reprice_imported_blocks_from_csv(self, csv_text: str,
                                         meter_id: str = "electricity_main",
                                         channel: str = "import") -> dict:
        """Overlay EXACT billed cost + rate onto imported blocks from a filled CSV —
        the billing source of truth. For dispatch slots Octopus's Measurements API
        returns with NO cost/label (so they can't be priced via the API, only via
        the bill), this is the only way to match the bill.

        Shares the SAME name-matched parser as the main CSV import
        (csv_import.parse_octopus_csv), so this path accepts the exact same
        template — including the rate-first cost derivation (a Unit Rate column
        wins over an explicit Cost column). Columns are matched by header name, not
        position, so column order doesn't matter. The block's rate is then stored as
        cost ÷ kWh. Only imported blocks are touched, only where the value differs.
        Returns {checked, changed, from, to}.
        """
        import csv_import as _ci
        rcol, ccol = (("imp_rate", "imp_cost") if channel == "import"
                      else ("exp_rate", "exp_cost"))
        parsed = _ci.parse_octopus_csv(csv_text, channel)
        checked = changed = 0
        lo = hi = None
        changed_starts = []
        with self._conn:
            for b in (parsed.get("blocks") or []):
                start = b.get("block_start")
                kwh = b.get("kwh")
                cost = b.get("cost")
                if not start or cost is None:     # a row with neither rate nor cost
                    continue
                cost = round(float(cost), 6)
                rate = round(cost / kwh, 6) if (kwh and kwh > 0) else None
                checked += 1
                lo = start if (lo is None or start < lo) else lo
                hi = start if (hi is None or start > hi) else hi
                cur = self._conn.execute(
                    f"UPDATE blocks SET {rcol} = ?, {ccol} = ? "
                    f"WHERE block_start = ? AND meter_id = ? AND source LIKE 'imported%' "
                    f"AND (COALESCE({rcol}, -1) != ? OR COALESCE({ccol}, -1) != ?)",
                    (rate, cost, start, meter_id, rate, cost))
                if cur.rowcount > 0:
                    changed += 1
                    changed_starts.append(start)
        if changed_starts:
            self.clear_reprice_queue_slots(channel, changed_starts)
        logger.info("reprice_imported_blocks_from_csv[%s]: %d changed of %d (%s..%s)",
                    channel, changed, checked, lo, hi)
        return {"checked": checked, "changed": changed, "from": lo, "to": hi}

    def get_imported_block_starts(self, from_iso: str, to_iso: str,
                                  meter_id: str = "electricity_main") -> list:
        """block_starts of IMPORTED blocks in [from, to) for a meter — the target
        set for a range-based re-price reconcile that runs WITHOUT a re-import."""
        rows = self._conn.execute(
            "SELECT block_start FROM blocks WHERE source LIKE 'imported%' "
            "AND meter_id = ? AND block_start >= ? AND block_start < ? "
            "ORDER BY block_start", (meter_id, from_iso, to_iso)).fetchall()
        return [r["block_start"] for r in rows]

    def get_imported_block_pricing(self, from_iso: str, to_iso: str,
                                   meter_id: str = "electricity_main") -> list:
        """Per-block CURRENT pricing for IMPORTED blocks in [from, to) — the cheap
        local read a suspect-only reprice uses to skip already-correct slots without
        touching the API. Returns dicts: block_start + imp/exp rate, kwh, cost."""
        rows = self._conn.execute(
            "SELECT block_start, imp_rate, imp_kwh, imp_cost, exp_rate, exp_kwh, exp_cost "
            "FROM blocks WHERE source LIKE 'imported%' AND meter_id = ? "
            "AND block_start >= ? AND block_start < ? ORDER BY block_start",
            (meter_id, from_iso, to_iso)).fetchall()
        return [{"start": r["block_start"],
                 "imp_rate": r["imp_rate"], "imp_kwh": r["imp_kwh"], "imp_cost": r["imp_cost"],
                 "exp_rate": r["exp_rate"], "exp_kwh": r["exp_kwh"], "exp_cost": r["exp_cost"]}
                for r in rows]

    def apply_csv_import(
        self, channel_csvs: dict, meter_id: str = "electricity_main", *,
        periods_by_channel: dict | None = None, overrides: dict | None = None,
    ) -> dict:
        """Parse Octopus per-channel CSV(s), derive rates (rate-from-cost), and
        write reconstructed `imported_csv` blocks + one `rate` derivation per
        (channel, tariff period). `channel_csvs` = {'import': csv_text, 'export':
        csv_text}. `periods_by_channel` (optional) = {channel: [(from,to),...]}
        agreement bounds; None ⇒ one period per channel.

        `overrides` (optional) carries the wizard's user-confirmed rates:
        {channel: {period_index(str): {tier_label: rate}}}. A tier's override
        replaces the derived aggregate for its blocks and is stored as the
        derivation's confirmed_value.

        Import is written before export so both settle onto the shared row.
        Returns a per-channel summary with blocks written, period rate derivations,
        and the re-price-vs-CSV-cost reconciliation."""
        import csv_import as _ci
        periods_by_channel = periods_by_channel or {}
        overrides = overrides or {}
        out: dict = {"ok": True, "meter_id": meter_id, "channels": {},
                     "blocks_written": 0, "blocks_skipped": 0,
                     "span": {"from": None, "to": None}}
        # Import first (house figure) so export merges onto existing rows.
        for channel in ("import", "export"):
            text = channel_csvs.get(channel)
            if not text:
                continue
            parsed = _ci.parse_octopus_csv(text, channel)
            if not parsed["ok"]:
                out["channels"][channel] = {"ok": False, "errors": parsed["errors"]}
                continue
            blocks = parsed["blocks"]
            if blocks:
                cf = blocks[0]["block_start"]
                ct = blocks[-1].get("block_end") or blocks[-1]["block_start"]
                if out["span"]["from"] is None or cf < out["span"]["from"]:
                    out["span"]["from"] = cf
                if out["span"]["to"] is None or ct > out["span"]["to"]:
                    out["span"]["to"] = ct
            deriv = _ci.derive_rates(blocks, periods=periods_by_channel.get(channel))
            ch_ov = overrides.get(channel, {}) or {}

            # One rate derivation per tariff period; blocks link to theirs.
            period_meta = []            # (from, to, derivation_id, tier_override_map)
            for i, p in enumerate(deriv["periods"]):
                ov_i = {k: float(v) for k, v in (ch_ov.get(str(i)) or {}).items()}
                tiers = [dict(t) for t in (p.get("tiers") or [])]
                for t in tiers:                       # fold confirmations into params
                    if t.get("label") in ov_i:
                        t["confirmed_rate"] = ov_i[t["label"]]
                derived_primary = min((t["rate"] for t in tiers if t.get("rate") is not None),
                                      default=None)
                confirmed_primary = min(ov_i.values()) if ov_i else None
                did = self.insert_historical_derivation(
                    "rate", p["from"], p["to"], subject=channel,
                    params={"kind": p["kind"], "tiers": tiers,
                            "standing_daily": p.get("standing_daily")},
                    derived_value=derived_primary, confirmed_value=confirmed_primary,
                    source=IMPORTED_SOURCE_CSV, notes="rate-from-cost (CSV import)")
                period_meta.append((p["from"], p["to"], did, ov_i))

            def _period_for(start):
                for f, t, d, ov in period_meta:
                    if f <= start < t:
                        return d, ov
                return (period_meta[-1][2], period_meta[-1][3]) if period_meta else (None, {})

            # Standing charge is per-day (sum the per-interval apportioned column).
            day_standing: dict = {}
            for b in blocks:
                day_standing[b["block_start"][:10]] = (
                    day_standing.get(b["block_start"][:10], 0.0) + (b.get("standing") or 0.0))

            written = 0
            skipped = 0                 # first-man-wins: slot already held data
            eff_flags: dict = {}        # for reconciliation with confirmed rates
            for b in blocks:
                flag = deriv["flags"].get(b["block_start"]) or {}
                did, ov = _period_for(b["block_start"])
                tier = flag.get("tier")
                # Rate precedence — RATE-FIRST. A user override for this tier wins;
                # otherwise, if the CSV row carried an explicit unit rate (an Octopus
                # HH bill table transcribed exactly, or any rate-first CSV) store THAT
                # exact per-slot rate — do NOT flatten it to the period's tier average.
                # Only a cost-only CSV (no rate column) falls back to the derived
                # aggregate. Cost is already per-slot (rate×kWh), so this makes the
                # stored rate consistent with cost and preserves the off-peak/peak
                # split instead of blending a whole tariff period into one rate.
                if ov and tier in ov:
                    rate = ov[tier]
                elif b.get("rate") is not None:
                    rate = b["rate"]
                else:
                    rate = flag.get("rate")
                eff_flags[b["block_start"]] = {"tier": tier, "rate": rate}
                bid, wrote = self.upsert_imported_block(
                    b["block_start"], meter_id, channel,
                    kwh=b["kwh"], rate=rate, cost=b.get("cost"),
                    standing=day_standing.get(b["block_start"][:10]),
                    derivation_id=did,
                    cost_exc=b.get("cost_exc"), rate_exc=b.get("rate_exc"),
                    standing_exc=b.get("standing_exc"), exc_source=b.get("exc_source"))
                if wrote:
                    written += 1
                elif bid is not None:
                    skipped += 1        # a block already existed here — left as-is

            out["channels"][channel] = {
                "ok": True, "blocks_written": written, "blocks_skipped": skipped,
                "period_derivations": [d for _f, _t, d, _o in period_meta],
                "off_peak_kwh": deriv["off_peak_kwh"], "peak_kwh": deriv["peak_kwh"],
                "reconcile": _ci.reconcile(blocks, eff_flags),
                "parse_errors": parsed["errors"],
            }
            out["blocks_written"] += written
            out["blocks_skipped"] += skipped
        if out["blocks_written"]:
            # Freshly-imported history carries NULL carbon_intensity. The historical
            # carbon backfill sets a "done" marker once it has swept the then-known
            # span and won't revisit on its own — so a later import would sit at 0%
            # carbon forever. Clear the marker so the next scheduler tick re-scans and
            # fills the added span (region-eligible blocks inherit their period's
            # postcode; same-MPAN history needs no region change).
            self.rearm_carbon_backfill()
        return out

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



    @staticmethod
    def _finalised_clause(finalised_only: bool, alias: str = "b") -> str:
        """SQL fragment to exclude provisional (not-yet-DCC-settled) blocks.

        Returns ' AND <alias>.is_provisional = 0' when finalised_only is True,
        else ''. Tier 1 (accuracy-critical) surfaces in 'api'/'api+mini' modes
        pass finalised_only=True. The default False preserves all pre-3.0.0
        behaviour: 'cad'/'cad+api' blocks are finalised at write time
        (is_provisional always 0), so the clause is a no-op for them anyway.
        """
        return f" AND {alias}.is_provisional = 0" if finalised_only else ""

    def compute_period_net(self, utc_s: str, utc_e: str, tz_name: str,
                           finalised_only: bool = False) -> float:
        """Compute net cost for a UTC period — single source of truth used by
        both Live Power (_fmt_total in server.py) and the billing chart
        (calculate_billing_summary_for_period in energy_charts.py).

        net_cost per day = round(
            round(direct + sub_4s, 4) + round(sc, 4) - round(exp, 4), 2)
        where direct = round(max(0, main_imp - sum(raw_subs)), 4)
        and sub_4s = sum(round(sub, 4) for each sub-meter).
        Summed across all days in the period.
        """
        from collections import defaultdict
        from datetime import datetime, timezone
        from zoneinfo import ZoneInfo

        tz_obj = ZoneInfo(tz_name)
        rows = self._conn.execute(
            """SELECT b.block_start, b.imp_cost, b.exp_cost, b.standing_charge,
                      m.is_sub_meter, m.meter_id
               FROM blocks b JOIN meters m ON m.meter_id = b.meter_id
                                          AND m.config_period_id = b.config_period_id
               WHERE b.block_start >= ? AND b.block_start < ?"""
            + self._finalised_clause(finalised_only),
            (utc_s, utc_e)
        ).fetchall()

        by_d     = defaultdict(lambda: {"main_imp": 0.0, "exp": 0.0, "sc": 0.0})
        sub_by_d = defaultdict(lambda: defaultdict(float))
        # Track the earliest block_start seen per day so the standing charge is
        # the start-of-day value (supplier convention), not MAX (which
        # over-bills on a decrease).
        _sc_earliest: dict = {}

        for r in rows:
            d = datetime.fromisoformat(r["block_start"]).replace(
                tzinfo=timezone.utc).astimezone(tz_obj).strftime("%Y-%m-%d")
            cost = float(r["imp_cost"] or 0)
            if not bool(r["is_sub_meter"]):
                by_d[d]["main_imp"] += cost
                by_d[d]["exp"] += float(r["exp_cost"] or 0)
                bs = r["block_start"]
                _sc = float(r["standing_charge"] or 0)
                # Start-of-day standing charge: take the earliest block that has
                # a non-zero charge, so a leading zero (early gap-fill) doesn't
                # shadow the real value. Not MAX (over-bills on a decrease).
                if _sc > 0 and (d not in _sc_earliest or bs < _sc_earliest[d]):
                    _sc_earliest[d] = bs
                    by_d[d]["sc"] = _sc
            else:
                sub_by_d[d][r["meter_id"]] += cost

        daily_nets = []
        for d in sorted(by_d):
            raw_subs = list(sub_by_d[d].values())
            direct   = round(max(0.0, by_d[d]["main_imp"] - sum(raw_subs)), 4)
            sub_4s   = sum(round(v, 4) for v in raw_subs)
            imp_4    = round(direct + sub_4s, 4)
            sc_4     = round(by_d[d]["sc"], 4)
            exp_4    = round(by_d[d]["exp"], 4)
            daily_nets.append(round(imp_4 + sc_4 - exp_4, 2))

        return round(sum(daily_nets), 2)

    # ── 3.0.0 Kraken API integration — storage layer ─────────────────────────
    # All methods below operate only on the 3.0.0 columns/tables. They never
    # modify imp_kwh, imp_kwh_grid, imp_kwh_remainder or any pre-3.0.0 column,
    # so existing billing behaviour is unaffected.

    def set_kraken_state(self, key: str, value: Optional[str]) -> None:
        """Persist an ingester progress marker (e.g. 'last_poll_utc')."""
        self._conn.execute(
            """INSERT INTO kraken_state (key, value, updated_at)
               VALUES (?, ?, ?)
               ON CONFLICT(key) DO UPDATE SET
                   value = excluded.value,
                   updated_at = excluded.updated_at""",
            (key, value, _utc_now_iso()),
        )
        self._conn.commit()

    def get_kraken_state(self, key: str) -> Optional[str]:
        """Read an ingester progress marker. None if never set."""
        row = self._conn.execute(
            "SELECT value FROM kraken_state WHERE key = ?", (key,)
        ).fetchone()
        return row["value"] if row else None

    def delete_kraken_state(self, key: str) -> None:
        """Remove a kraken_state marker entirely (used by the disconnect action to
        wipe API-derived progress state — poll watermark, sweep cadence, pre-live
        snapshot flag — so a later reconnect starts clean). No-op if absent."""
        self._conn.execute("DELETE FROM kraken_state WHERE key = ?", (key,))
        self._conn.commit()

    def get_block_by_start(self, block_start: str, meter_id: str):
        """Return the block row for (block_start, meter_id), or None."""
        return self._conn.execute(
            "SELECT * FROM blocks WHERE block_start = ? AND meter_id = ?",
            (block_start, meter_id),
        ).fetchone()

    def get_block_by_id(self, block_id: int):
        """Return the block row for a given id, or None."""
        return self._conn.execute(
            "SELECT * FROM blocks WHERE id = ?", (block_id,)
        ).fetchone()

    def get_oldest_block_start(self, meter_id: Optional[str] = None,
                               *, exclude_imported: bool = False) -> Optional[str]:
        """Return the earliest block_start in the store, or None if empty.

        Drives the data-driven backfill window: the ingester fetches DCC from
        this point to now (capped). Backfilling earlier than the oldest block
        is pointless — upsert_kraken_block would only return missing_block.

        `exclude_imported=True` ignores reconstructed/imported blocks
        (source LIKE 'imported%'), so the historical-import "go-live" ceiling is
        where LIVE recording actually began — not wherever a previous partial
        import happened to write. Without this a partial import lowers the
        ceiling below its own gap, and the gap can never be topped up.
        """
        where = []
        params: list = []
        if meter_id:
            where.append("meter_id = ?")
            params.append(meter_id)
        if exclude_imported:
            where.append("(source IS NULL OR source NOT LIKE 'imported%')")
        clause = (" WHERE " + " AND ".join(where)) if where else ""
        row = self._conn.execute(
            "SELECT MIN(block_start) AS m FROM blocks" + clause, params).fetchone()
        return row["m"] if row and row["m"] else None

    def classify_kraken_block(
        self,
        block_start: str,
        meter_id: str,
        settled_kwh: Optional[float],
        *,
        channel: str = "import",
        billing_source: str = "api",
        drift_block_percent: float = 2.0,
        drift_min_kwh: float = 0.05,
    ) -> dict:
        """Read-only: determine what upsert_kraken_block WOULD do, without
        writing. Returns the same verdict dict (status / drift_pct /
        needs_review / needs_pass2_rerun / cad_kwh / settled_kwh / channel /
        block_id) so a dry-run preview is identical to a live run minus the
        UPDATE. Performs one SELECT (the block lookup) and no writes.

        Drift flags review only when BOTH thresholds are exceeded: the absolute
        delta > drift_min_kwh (default 50 Wh) AND |drift| > drift_block_percent.
        The absolute floor stops percentage-on-tiny-numbers noise (e.g. 131% of
        16 Wh) dominating the count on a solar profile, while keeping real
        divergence on substantial blocks.
        """
        if channel not in ("import", "export"):
            raise ValueError(f"channel must be 'import' or 'export', got {channel!r}")
        cad_col = "imp_kwh" if channel == "import" else "exp_kwh"

        existing = self.get_block_by_start(block_start, meter_id)
        if existing is None:
            return {"status": "missing_block", "block_start": block_start,
                    "meter_id": meter_id, "channel": channel}
        if settled_kwh is None:
            return {"status": "no_value", "block_id": existing["id"],
                    "channel": channel}

        cad_kwh = existing[cad_col]
        drift_pct: Optional[float] = None
        review = 0
        # Materiality epsilon: kWh below this is treated as zero, so float dust
        # and sub-watt noise don't count as a real figure. 0.005 kWh = 5 Wh.
        _EPS = 0.005
        cad_material = cad_kwh is not None and abs(cad_kwh) >= _EPS
        dcc_material = settled_kwh is not None and abs(settled_kwh) >= _EPS

        if not cad_material and not dcc_material:
            # zero-vs-zero → agreement (e.g. night export, solar-covered import).
            review = 0
        elif cad_material and dcc_material:
            # both substantive → flag only if BOTH thresholds exceeded.
            abs_delta = abs(settled_kwh - cad_kwh)
            drift_pct = (settled_kwh - cad_kwh) / cad_kwh * 100.0
            if abs_delta > drift_min_kwh and abs(drift_pct) > drift_block_percent:
                review = 1
        else:
            # one side material, the other ~zero → genuine discrepancy, but only
            # worth flagging if the material side exceeds the absolute floor
            # (a lone 20 Wh blip is not actionable).
            material_val = settled_kwh if dcc_material else cad_kwh
            if abs(material_val) > drift_min_kwh:
                review = 1

        # Only queue a PASS 2 re-run when the DCC figure is NEW or CHANGED.
        # The poll re-upserts a rolling backfill window every cycle, so without
        # this guard an already-settled block would be re-flagged and re-run on
        # EVERY poll forever (rerun=1 unconditionally when billing_source=='api').
        # Compare the incoming settled_kwh against what's already materialised in
        # the api column: if unchanged, there's nothing new to re-run.
        api_col = "imp_kwh_api" if channel == "import" else "exp_kwh_api"
        try:
            prev_api = existing[api_col]
        except (IndexError, KeyError):
            prev_api = None
        _figure_changed = (prev_api is None) or (
            abs((settled_kwh or 0.0) - (prev_api or 0.0)) > 1e-6)
        rerun = 1 if (billing_source == "api" and _figure_changed) else 0
        new_review = 1 if (review or (existing["needs_review"] or 0)) else 0
        new_rerun = 1 if (rerun or (existing["needs_pass2_rerun"] or 0)) else 0
        try:
            interpolated = bool(existing["interpolated"])
        except (IndexError, KeyError):
            interpolated = False
        return {
            "status": "stored",   # i.e. WOULD store
            "block_id": existing["id"],
            "channel": channel,
            "settled_kwh": settled_kwh,
            "cad_kwh": cad_kwh,
            "drift_pct": drift_pct,
            "interpolated": interpolated,
            "needs_review": new_review,
            "needs_pass2_rerun": new_rerun,
        }

    def store_mini_import(self, block_start: str, meter_id: str,
                          imp_kwh: float) -> dict:
        """Store a provisional Mini-derived import kWh on an existing block.

        api+mini near-real-time layer: writes imp_kwh, marks the block
        provisional + sourced 'kraken_mini', and queues it for PASS 2 re-run so
        DCC settlement (when it arrives) overwrites it. Returns
        {"status": "stored"|"missing_block"}.

        Mini is treated like CAD: imp_kwh is the working import figure until DCC
        reconciles. Export is never Mini-sourced (no Mini export register).
        """
        row = self.get_block_by_start(block_start, meter_id)
        if row is None:
            return {"status": "missing_block", "block_start": block_start}
        self._conn.execute(
            """UPDATE blocks
               SET imp_kwh = ?, is_provisional = 1, source = 'kraken_mini',
                   needs_pass2_rerun = 1
               WHERE block_start = ? AND meter_id = ?""",
            (imp_kwh, block_start, meter_id))
        self._conn.commit()
        return {"status": "stored", "imp_kwh": imp_kwh}

    def upsert_kraken_block(
        self,
        block_start: str,
        meter_id: str,
        settled_kwh: Optional[float],
        *,
        channel: str = "import",
        source: str = "kraken_api",
        billing_source: str = "api",
        drift_block_percent: float = 2.0,
        drift_min_kwh: float = 0.05,
    ) -> dict:
        """Store a DCC-settled figure on an existing block, for one channel.

        channel='import' writes imp_kwh_api and compares against imp_kwh;
        channel='export' writes exp_kwh_api and compares against exp_kwh. The
        two channels are symmetric and independent: a block may receive both
        (separate ingester calls), and the second call must not clear the
        first's flags, so needs_review and needs_pass2_rerun are OR-ed with the
        existing row values rather than overwritten.

        Classification is delegated to classify_kraken_block (shared with the
        dry-run preview); this method adds the UPDATE for a 'stored' verdict.

        Returns a dict describing what happened; status == "missing_block"
        when no matching block exists, "no_value" when settled_kwh is None.
        """
        verdict = self.classify_kraken_block(
            block_start, meter_id, settled_kwh, channel=channel,
            billing_source=billing_source, drift_block_percent=drift_block_percent,
            drift_min_kwh=drift_min_kwh)
        if verdict["status"] != "stored":
            return verdict

        api_col = "imp_kwh_api" if channel == "import" else "exp_kwh_api"
        self._conn.execute(
            f"""UPDATE blocks
                SET {api_col} = ?,
                    source = COALESCE(?, source),
                    needs_review = ?,
                    needs_pass2_rerun = ?,
                    finalised_from_cad = 0
                WHERE id = ?""",
            (settled_kwh, source, verdict["needs_review"],
             verdict["needs_pass2_rerun"], verdict["block_id"]),
        )
        self._conn.commit()
        return verdict

    # ── BL-8: outage backfill ────────────────────────────────────────────────

    BACKFILL_INTERPOLATED = 2   # blocks materialised from settled supplier data

    def create_backfill_block(self, block_start: str, meter_id: str,
                              settled_kwh: float, *,
                              channel: str = "import",
                              source: str = "kraken_api") -> Optional[int]:
        """BL-8: materialise a block that does not exist, from DCC-settled data.

        An outage longer than the gap-fill limit leaves no block for the period,
        so `upsert_kraken_block` returns `missing_block` and the settled figure is
        dropped — a permanent hole. This creates the row with the authoritative
        kWh and marks it `needs_pass2_rerun=1`, letting the EXISTING PASS-2 drain
        resolve the rate and standing charge for that timestamp (it fetches
        historical rates, so any tariff — Agile included — prices correctly). We
        deliberately do not duplicate pricing logic here.

        `interpolated = BACKFILL_INTERPOLATED (2)` marks it externally sourced —
        distinct from 1 (locally interpolated gap-fill) — so the UI can show it as
        recovered/approximate. It carries NO sub-meter split (we were down) and,
        having no dispatch_history, will never be picked up by the dispatch
        reconciliation — its off-peak status is decided once, at creation.

        `block_end` is derived from the covering config period's `block_minutes`.
        Returns the new block id, or None if the block already exists or no
        config period covers the timestamp.
        """
        kwh_col = "imp_kwh" if channel == "import" else "exp_kwh"
        api_col = "imp_kwh_api" if channel == "import" else "exp_kwh_api"
        existing = self._conn.execute(
            f"SELECT id, {api_col}, interpolated FROM blocks "
            "WHERE block_start = ? AND meter_id = ?",
            (block_start, meter_id)).fetchone()
        if existing:
            # A backfill for the OTHER channel already created this block (import
            # is ingested before export). Don't lose this channel's kWh — fill it.
            # Guarded hard: only a block WE backfilled (interpolated=2) and whose
            # settled figure for THIS channel is unset. A live metered block is
            # never touched here (and never reaches this method anyway — we are
            # only called on a `missing_block` verdict).
            if existing[1] is None and existing[2] == self.BACKFILL_INTERPOLATED:
                with self._conn:
                    self._conn.execute(
                        f"UPDATE blocks SET {kwh_col} = ?, {api_col} = ?, "
                        "needs_pass2_rerun = 1 WHERE id = ?",
                        (settled_kwh, settled_kwh, existing[0]))
                return existing[0]
            return None
        cp = self.get_config_period_for_date(block_start)
        if not cp:
            return None
        try:
            bm = int(cp.get("block_minutes") or 30)
            block_end = (datetime.fromisoformat(block_start)
                         + timedelta(minutes=bm)).isoformat()
        except Exception:
            return None
        with self._conn:
            cur = self._conn.execute(
                f"""INSERT INTO blocks
                    (block_start, block_end, meter_id, config_period_id,
                     interpolated, {kwh_col}, {api_col}, source,
                     needs_pass2_rerun, is_provisional, finalised_from_cad)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1, 0, 0)""",
                (block_start, block_end, meter_id, cp["id"],
                 self.BACKFILL_INTERPOLATED, settled_kwh, settled_kwh, source))
        return cur.lastrowid

    def count_backfilled_blocks(self) -> int:
        """How many blocks were materialised from settled supplier data."""
        return self._conn.execute(
            "SELECT COUNT(*) FROM blocks WHERE interpolated = ?",
            (self.BACKFILL_INTERPOLATED,)).fetchone()[0]

    def channel_slots_present(self, channel: str, from_iso: str, to_iso: str,
                              meter_id: str = "electricity_main") -> int:
        """Count blocks in [from_iso, to_iso] inclusive that HAVE this channel's
        kWh. Used to reconcile a persisted import-gap against reality: a gap whose
        slots now carry data has been filled and should drop from the display."""
        col = "imp_kwh" if channel == "import" else "exp_kwh"
        row = self._conn.execute(
            f"SELECT COUNT(*) FROM blocks WHERE meter_id = ? "
            f"AND block_start >= ? AND block_start <= ? AND {col} IS NOT NULL",
            (meter_id, from_iso, to_iso)).fetchone()
        return int(row[0]) if row else 0

    def find_block_gaps(self, meter_id: str = "electricity_main",
                        *, min_age_hours: float = 48.0,
                        now: Optional[datetime] = None) -> list:
        """Find missing half-hour slots in the block history (BL-8 gap sweep).

        The DCC poll's window is a forward-moving cursor (`last_poll_utc` → now),
        so once it has advanced past an outage that hole is never revisited. This
        asks the *database* where the holes are instead — exact, cheap, and it
        finds gaps however old and however they arose.

        Returns contiguous runs: [{"start", "end", "slots"}], where `end` is the
        start of the last missing slot. Skips anything newer than `min_age_hours`
        (DCC has not settled it yet, so a fetch would return nothing).
        """
        rows = [r[0] for r in self._conn.execute(
            "SELECT block_start FROM blocks WHERE meter_id = ? ORDER BY block_start",
            (meter_id,))]
        if len(rows) < 2:
            return []
        cp = self.get_config_period_for_date(rows[0]) or {}
        step = timedelta(minutes=int(cp.get("block_minutes") or 30))
        cutoff = ((now or datetime.now(timezone.utc)).replace(tzinfo=None)
                  - timedelta(hours=min_age_hours))
        have = set(rows)
        runs: list = []
        t = datetime.fromisoformat(rows[0])
        last = datetime.fromisoformat(rows[-1])
        while t <= last:
            if t.isoformat() not in have and t < cutoff:
                if runs and runs[-1]["_end_dt"] + step == t:
                    runs[-1]["_end_dt"] = t
                    runs[-1]["slots"] += 1
                else:
                    runs.append({"start": t.isoformat(), "_end_dt": t, "slots": 1})
            t += step
        for r in runs:
            r["end"] = r.pop("_end_dt").isoformat()
        return runs

    def get_blocks_needing_pass2_rerun(self, limit: Optional[int] = None) -> list:
        """Blocks where DCC has arrived and PASS 2+3b re-run is pending.

        The engine drains this queue: re-runs attribution + carbon using the
        stored carbon_intensity_g, then calls clear_pass2_rerun_flag().
        """
        sql = ("SELECT * FROM blocks WHERE needs_pass2_rerun = 1 "
               "ORDER BY block_start")
        if limit is not None:
            sql += " LIMIT ?"
            return self._conn.execute(sql, (limit,)).fetchall()
        return self._conn.execute(sql).fetchall()

    def clear_pass2_rerun_flag(self, block_id: int) -> None:
        """Mark a block's PASS 2+3b re-run as complete."""
        self._conn.execute(
            "UPDATE blocks SET needs_pass2_rerun = 0 WHERE id = ?", (block_id,)
        )
        self._conn.commit()

    def flag_all_for_pass2_rerun(self, main_meter_id: Optional[str] = None) -> int:
        """Flag every (main-meter) block for PASS 2 re-run.

        Used when the global billing_source toggle changes (cad<->dcc): the
        drain then reprocesses each block, rewriting billing figures to the
        chosen source. Returns the number of blocks flagged. Bounded only by
        the table size; the drain spreads the actual work across ticks.

        Only main-meter blocks carry the settlement columns, so sub-meter
        blocks don't need flagging — but flagging all is harmless (their re-run
        is a no-op) and simpler, so we scope to main meter when given.
        """
        if main_meter_id:
            cur = self._conn.execute(
                "UPDATE blocks SET needs_pass2_rerun = 1 WHERE meter_id = ?",
                (main_meter_id,))
        else:
            cur = self._conn.execute("UPDATE blocks SET needs_pass2_rerun = 1")
        self._conn.commit()
        return cur.rowcount

    def flag_grid_invariant_violations(self, tolerance: float = 1e-4) -> int:
        """BL-19 one-time sweep: flag blocks whose sub-meter grid attribution
        exceeds the parent's settled grid import for PASS 2 re-run.

        Finds every settled main block (imp_kwh_api present, so its grid import
        is authoritative) where the sum of its sub-meters' imp_kwh_grid exceeds
        the main's grid import (imp_kwh) by more than `tolerance`, and sets
        needs_pass2_rerun = 1 on that main block. The existing drain then
        re-materialises it through the (BL-19-fixed) PASS 2, which clamps each
        sub-meter's grid share to the available grid import.

        This repairs historical violations written before the clamp fix — e.g.
        blocks reconstructed by gap-fill during an instance outage, where the
        main later settled small (heavy solar) but the sub-meter grid was left
        at the full interpolated draw. Returns the number of main blocks flagged.
        """
        cur = self._conn.execute(
            """
            UPDATE blocks SET needs_pass2_rerun = 1
            WHERE id IN (
                SELECT b_main.id
                FROM blocks b_main
                JOIN meters m
                  ON m.parent_meter_id = b_main.meter_id AND m.is_sub_meter = 1
                JOIN blocks b_sub
                  ON b_sub.meter_id = m.meter_id
                 AND b_sub.block_start = b_main.block_start
                WHERE b_main.imp_kwh_api IS NOT NULL
                  AND b_sub.imp_kwh_grid IS NOT NULL
                GROUP BY b_main.id, b_main.imp_kwh
                HAVING SUM(b_sub.imp_kwh_grid) > COALESCE(b_main.imp_kwh, 0) + ?
            )
            """,
            (tolerance,))
        self._conn.commit()
        return cur.rowcount

    # A main-meter block still awaiting DCC settlement on a channel it should
    # have: import not yet settled, OR export not yet settled on a block that
    # actually exported. Issue #303: export commonly settles days after import,
    # so keying "unsettled" on import alone let export-lagging blocks age out of
    # the re-fetch window and never settle — freezing the export figure on its
    # pre-settlement estimate. (Import-only accounts export 0, so the export
    # clause never fires for them.)
    # 3.5.0: reconstructed history (`imported_*`) is authoritative and pre-dates
    # EMT — DCC will NEVER settle it, so it must be excluded or every imported
    # block (imp_kwh_api NULL) is counted "awaiting DCC settlement" and chased by
    # the settlement sweep forever.
    # A block is "awaiting DCC settlement" if its import is unsettled, OR its export
    # is unsettled AND the meter demonstrably EXPORTS. The export test used to be
    # `exp_kwh IS NOT NULL AND exp_kwh > 0` — i.e. only chase export when a POSITIVE
    # live/CAD figure is already present. That silently strands DCC-export-only
    # meters, whose daytime export lands only via settlement: those slots carry
    # exp_kwh = 0 or NULL (no live figure), fail the guard, and are never re-fetched
    # (blocks-4.db: 245 stuck daytime slots). Instead: chase whenever exp_kwh_api is
    # NULL and the meter has EVER shown export (a live or settled positive anywhere)
    # — value-agnostic on the row, so 0/NULL placeholders are still corrected, while
    # a true import-only meter (never any export) is left alone. Bounded by the
    # settlement horizon + finalise-from-CAD so it can't chase forever.
    _UNSETTLED_WHERE = (
        "(imp_kwh_api IS NULL "
        " OR (exp_kwh_api IS NULL AND EXISTS ("
        "        SELECT 1 FROM blocks b2 WHERE b2.meter_id = blocks.meter_id "
        "        AND (b2.exp_kwh > 0 OR b2.exp_kwh_api > 0)))) "
        "AND (source IS NULL OR source NOT LIKE 'imported%')")

    def get_unsettled_blocks(self, main_meter_id: str = "electricity_main",
                             limit: Optional[int] = None,
                             since_iso: Optional[str] = None) -> list:
        """Main-meter blocks still awaiting DCC settlement (import or export).

        For the view-only 'review unsettled blocks' report (relevant only when
        billing_source='dcc'): blocks running on the CAD/estimate fallback
        because DCC hasn't settled a channel yet. Ordered newest first so the
        most recent gaps surface at the top.

        `since_iso` floors the result at a settlement-horizon boundary — a block
        older than the horizon is not "awaiting DCC" (settlement lands within
        days; anything older either settled or never will), so the caller passes
        `now − horizon` to keep the count honest even before the settlement sweep
        has finalised past-horizon blocks.
        """
        sql = ("SELECT block_start, block_end, imp_kwh, imp_kwh_api, "
               "       exp_kwh, exp_kwh_api, is_provisional "
               f"FROM blocks WHERE meter_id = ? AND {self._UNSETTLED_WHERE} "
               "AND finalised_from_cad = 0")
        params: list = [main_meter_id]
        if since_iso:
            sql += " AND block_start >= ?"
            params.append(since_iso)
        sql += " ORDER BY block_start DESC"
        if limit is not None:
            sql += " LIMIT ?"
            params.append(limit)
        return self._conn.execute(sql, params).fetchall()

    def count_unsettled_blocks(self, main_meter_id: str = "electricity_main",
                               since_iso: Optional[str] = None) -> int:
        """Count of main-meter blocks still awaiting DCC settlement on a channel
        (import or export), excluding those finalised-from-CAD. `since_iso` floors
        the count at the settlement horizon (see get_unsettled_blocks) so historic
        / never-settling blocks don't inflate the 'awaiting DCC settlement' badge."""
        sql = ("SELECT COUNT(*) AS n FROM blocks "
               f"WHERE meter_id = ? AND {self._UNSETTLED_WHERE} "
               "AND finalised_from_cad = 0")
        params: list = [main_meter_id]
        if since_iso:
            sql += " AND block_start >= ?"
            params.append(since_iso)
        row = self._conn.execute(sql, params).fetchone()
        return int(row["n"]) if row else 0

    def finalise_past_horizon_blocks(self, cutoff_block_start: str,
                                     main_meter_id: str = "electricity_main") -> int:
        """Mark main-meter blocks older than cutoff_block_start that are still
        awaiting DCC settlement (import or export, #303) as finalised-from-CAD,
        so the unsettled count can reach zero once DCC is no longer expected.
        This is DISTINCT from a real settlement — the *_api columns stay NULL —
        and reversible: a later DCC settlement clears the flag (see
        upsert_kraken_block). Returns the number of blocks newly flagged."""
        cur = self._conn.execute(
            "UPDATE blocks SET finalised_from_cad = 1 "
            f"WHERE meter_id = ? AND {self._UNSETTLED_WHERE} "
            "AND finalised_from_cad = 0 AND block_start < ?",
            (main_meter_id, cutoff_block_start))
        self._conn.commit()
        return cur.rowcount

    def get_oldest_unsettled_block_start(
            self, main_meter_id: str = "electricity_main") -> Optional[str]:
        """Earliest block_start still awaiting DCC settlement on a channel
        (import or export, #303), or None.

        Bounds the sweep / user-triggered retry re-fetch window
        (oldest-unsettled → now). Including export here is what lets an
        export-lagging block stay in the window until its export settles."""
        row = self._conn.execute(
            "SELECT MIN(block_start) AS m FROM blocks "
            f"WHERE meter_id = ? AND {self._UNSETTLED_WHERE}",
            (main_meter_id,)).fetchone()
        return row["m"] if row and row["m"] else None

    def get_timed_out_provisionals(
        self, cutoff_utc: str,
        sources: tuple = ("kraken_api", "kraken_mini"),
    ) -> list:
        """Provisional blocks older than cutoff whose DCC never arrived."""
        placeholders = ",".join("?" for _ in sources)
        return self._conn.execute(
            f"""SELECT * FROM blocks
                WHERE is_provisional = 1
                  AND block_start < ?
                  AND source IN ({placeholders})
                ORDER BY block_start""",
            (cutoff_utc, *sources),
        ).fetchall()

    def finalise_timed_out_provisionals(
        self, cutoff_utc: str,
        sources: tuple = ("kraken_api", "kraken_mini"),
    ) -> int:
        """Clear is_provisional on timed-out blocks. Returns rows affected.

        In 'api+mini' the Mini imp_kwh becomes the permanent billing value via
        COALESCE. In 'api' a block whose imp_kwh is still NULL finalises with
        NULL and is excluded from billing aggregations permanently — safer than
        writing a zero. The caller logs a WARNING per block (it has the list
        from get_timed_out_provisionals); this method only flips the flag.
        """
        placeholders = ",".join("?" for _ in sources)
        cur = self._conn.execute(
            f"""UPDATE blocks SET is_provisional = 0
                WHERE is_provisional = 1
                  AND block_start < ?
                  AND source IN ({placeholders})""",
            (cutoff_utc, *sources),
        )
        self._conn.commit()
        return cur.rowcount

    def get_drift_alerts(self) -> list:
        """Blocks flagged needs_review = 1, for the review list (BL-18).

        Returns dicts with the CAD/Mini figure, the DCC figure, the delta % and
        a stored review_reason so the UI table can render directly. Informational
        only — billing_source already determines which figure drives the numbers.
        A flag can come from CAD/DCC settlement drift or from an ambiguous
        dispatch-reconcile decision; review_reason (when set) names which.
        """
        rows = self._conn.execute(
            """SELECT id, block_start, meter_id, imp_kwh, imp_kwh_api, review_reason
               FROM blocks
               WHERE needs_review = 1
               ORDER BY block_start"""
        ).fetchall()
        alerts = []
        for r in rows:
            cad = r["imp_kwh"]
            dcc = r["imp_kwh_api"]
            if cad is not None and cad != 0 and dcc is not None:
                delta_pct = (dcc - cad) / cad * 100.0
            elif dcc == 0.0:
                delta_pct = -100.0 if (cad or 0) > 0 else 0.0
            else:
                delta_pct = None
            reason = r["review_reason"]
            if not reason:
                # Legacy drift flag with no stored reason — synthesise one.
                if delta_pct is not None:
                    reason = f"CAD/DCC settlement drift {delta_pct:+.0f}%"
                else:
                    reason = "flagged for review"
            alerts.append({
                "block_id": r["id"],
                "block_start": r["block_start"],
                "meter_id": r["meter_id"],
                "cad_kwh": cad,
                "dcc_kwh": dcc,
                "delta_pct": delta_pct,
                "reason": reason,
            })
        return alerts

    # Auto-CORRECTION review reasons: the integrity sweeps clamp a physically
    # impossible figure to the trustworthy grid-bounded value deterministically —
    # there's no second opinion for a user to give and no tool to give it with, so
    # these are NOT rate-actionable. They stay flagged in the DB as a dormant record
    # (and in the startup log), but never appear in the IOG pricing correction panel.
    AUTO_CORRECTION_REASONS = (
        "implausible sub-meter kWh clamped (#307 lost opener)",
        "register dip-and-recover phantom clamped",
    )

    def get_review_blocks(self) -> list:
        """BL-18: blocks flagged for review with a stored reason — i.e. the
        rate-actionable dispatch-reconcile ambiguities that the Corrections
        review list surfaces.

        Deliberately EXCLUDES:
          • bare needs_review flags with no review_reason (CAD/DCC settlement
            drift): drift is a kWh disagreement, and the correction tool only edits
            rates, so there is no action to take on it here;
          • auto-CORRECTION reasons (the integrity sweeps): deterministic repairs,
            not rate tasks — they'd only clutter the IOG pricing panel;
          • rows with a null/empty block_start (legacy/malformed flags), which can't
            be located or acted on anyway.
        Excluded flags stay set as a dormant diagnostic, just not shown as a task.
        """
        _ph = ",".join("?" * len(self.AUTO_CORRECTION_REASONS))
        rows = self._conn.execute(
            f"""SELECT id, block_start, meter_id, review_reason
               FROM blocks
               WHERE needs_review = 1 AND review_reason IS NOT NULL
                 AND block_start IS NOT NULL AND TRIM(block_start) <> ''
                 AND review_reason NOT IN ({_ph})
               ORDER BY block_start""",
            self.AUTO_CORRECTION_REASONS
        ).fetchall()
        return [{
            "block_id": r["id"],
            "block_start": r["block_start"],
            "meter_id": r["meter_id"],
            "reason": r["review_reason"],
        } for r in rows]

    def dismiss_review_blocks(self, block_ids: Optional[list] = None) -> int:
        """BL-18: clear review flags shown in the Corrections list. Scoped to
        dispatch-origin flags (review_reason present) so a 'dismiss all' never
        silently touches the dormant drift diagnostics. Returns rows affected.
        """
        # #322: mark the block DISMISSED (sticky) as well as clearing the live flag,
        # so the dispatch reconcile can't resurrect it on the next re-scan.
        if block_ids is None:
            cur = self._conn.execute(
                "UPDATE blocks SET needs_review = 0, review_reason = NULL, review_dismissed = 1 "
                "WHERE needs_review = 1 AND review_reason IS NOT NULL")
        elif not block_ids:
            return 0
        else:
            placeholders = ",".join("?" for _ in block_ids)
            cur = self._conn.execute(
                "UPDATE blocks SET needs_review = 0, review_reason = NULL, review_dismissed = 1 "
                f"WHERE id IN ({placeholders}) AND review_reason IS NOT NULL",
                tuple(block_ids))
        self._conn.commit()
        return cur.rowcount

    def flag_block_for_review(self, block_start: str, reason: str,
                              meter_id: str = "electricity_main") -> int:
        """BL-18: flag a single block (main meter row) for review with a reason.

        Used by the dispatch reconciliation when a block is genuinely ambiguous
        (substantial completed energy without a `started` signal). Idempotent —
        re-flagging refreshes the reason. Returns rows affected.

        #322: NEVER re-flags a block the user has already dismissed
        (`review_dismissed = 1`) — otherwise every reconcile re-scan resurrected the
        same ambiguous blocks (typically Ohme replan-without-charge slots), asking the
        user to review them again and again. Repricing is unaffected — only the review
        prompt is suppressed. To surface it again, the user re-flags via a correction.
        """
        cur = self._conn.execute(
            "UPDATE blocks SET needs_review = 1, review_reason = ? "
            "WHERE block_start = ? AND meter_id = ? AND review_dismissed = 0",
            (reason, block_start, meter_id))
        self._conn.commit()
        return cur.rowcount

    def clear_block_review(self, block_start: str,
                           meter_id: str = "electricity_main") -> int:
        """BL-18: clear the review flag + reason for a single block.

        Called when a previously-ambiguous block is later resolved — either the
        reconciliation itself reaches a definite verdict, or the user applies a
        manual correction. Returns rows affected.
        """
        cur = self._conn.execute(
            "UPDATE blocks SET needs_review = 0, review_reason = NULL "
            "WHERE block_start = ? AND meter_id = ? AND needs_review = 1",
            (block_start, meter_id))
        self._conn.commit()
        return cur.rowcount

    def dismiss_drift_alerts(self, block_ids: Optional[list] = None) -> int:
        """Clear needs_review. All flagged blocks, or a specific subset.

        Returns rows affected. Dismissing does not change any billing figure —
        it only removes the investigate flag.
        """
        if block_ids is None:
            cur = self._conn.execute(
                "UPDATE blocks SET needs_review = 0, review_reason = NULL "
                "WHERE needs_review = 1"
            )
        elif not block_ids:
            return 0
        else:
            placeholders = ",".join("?" for _ in block_ids)
            cur = self._conn.execute(
                "UPDATE blocks SET needs_review = 0, review_reason = NULL "
                f"WHERE id IN ({placeholders})",
                tuple(block_ids),
            )
        self._conn.commit()
        return cur.rowcount

    def get_settled_to(self, main_meter_id: str) -> Optional[str]:
        """Latest block_start that is fully DCC-settled for the main meter.

        Powers the Overview "As of HH:MM" timestamp in 'api'/'api+mini' modes.
        Returns None when nothing is settled yet.
        """
        row = self._conn.execute(
            """SELECT MAX(block_start) AS settled_to FROM blocks
               WHERE is_provisional = 0
                 AND imp_cost IS NOT NULL
                 AND meter_id = ?""",
            (main_meter_id,),
        ).fetchone()
        return row["settled_to"] if row and row["settled_to"] else None

    def get_billing_totals_for_utc_range(self, utc_start: str, utc_end: str,
                                          tz_name: str = "UTC",
                                          finalised_only: bool = False) -> dict:
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
               WHERE b.block_start >= ? AND b.block_start < ?"""
            + self._finalised_clause(finalised_only),
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
                 AND m.is_sub_meter = 0"""
            + self._finalised_clause(finalised_only)
            + " ORDER BY b.block_start",
            (utc_start, utc_end)
        ).fetchall()

        # Standing charge is a fixed DAILY fee. Bill the charge effective at the
        # START of each local day (the supplier convention), not the max — MAX
        # over-bills on any day the charge decreases. Rows are ORDER BY
        # block_start ascending, so the first row seen for a local date is that
        # day's earliest block = the start-of-day value. After DCC settlement
        # every block in a day carries the same schedule value anyway, so this
        # only differs from the old MAX on a genuine mid-day change boundary,
        # where start-of-day is the correct billing choice.
        daily_sc: dict = {}
        for sc_row in sc_rows:
            local_d = (datetime.fromisoformat(sc_row["block_start"])
                       .replace(tzinfo=ZoneInfo("UTC"))
                       .astimezone(tz)
                       .strftime("%Y-%m-%d"))
            sc_val = float(sc_row["standing_charge"] or 0)
            if sc_val > 0:
                if not daily_sc.get(local_d):       # unset or still 0
                    daily_sc[local_d] = sc_val
            elif local_d not in daily_sc:
                daily_sc[local_d] = 0.0
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

        # Get retirement dates for all meters so we can filter stale reads
        _retired = {}
        try:
            for _r in self._conn.execute(
                "SELECT meter_id, retired_at FROM meters WHERE retired_at IS NOT NULL"
            ).fetchall():
                _retired[_r["meter_id"]] = _r["retired_at"]
        except Exception:
            pass

        _block_date = (row["block_start"] or "")[:10]  # YYYY-MM-DD

        all_meter_ids = set(list(meter_reads.keys()) + list(meter_rates.keys()))
        for mid in all_meter_ids:
            # Skip retired sub-meters — don't load their stale reads back into the block
            if mid in _retired and _retired[mid] <= _block_date:
                continue
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

    # ── Region timeline (per-period postcode → DNO region for carbon) ──────────
    def get_postcode_prefix_at(self, date_iso: str) -> tuple:
        """(outward_code, source) for the config period effective at date_iso.

        Resolves via get_config_period_for_date → that period's MAIN meter.
        Pre-EMT/imported dates (no covering period) fall back to the OLDEST
        period, matching where imported blocks are attached. Returns
        (None, None) when there is no period or no postcode is configured.

        This is the per-date region lookup carbon backfill must use instead of a
        single global postcode, so a house move (a new period with a different
        outward code) attributes the correct region either side of the move.
        """
        cp = self.get_config_period_for_date(date_iso)
        if cp is None:
            row = self._conn.execute(
                "SELECT id FROM config_periods ORDER BY effective_from ASC LIMIT 1"
            ).fetchone()
            if not row:
                return (None, None)
            cp_id = row["id"]
        else:
            cp_id = cp["id"]
        r = self._conn.execute(
            "SELECT postcode_prefix, postcode_source FROM meters "
            "WHERE config_period_id = ? AND is_sub_meter = 0 "
            "ORDER BY id ASC LIMIT 1",
            (cp_id,)
        ).fetchone()
        if not r:
            return (None, None)
        try:
            src = r["postcode_source"]
        except (KeyError, IndexError):
            src = None
        return (r["postcode_prefix"], src)

    def set_period_postcode(self, period_id: int, outcode, source: str) -> None:
        """Set the outward code + provenance on a config period's MAIN meter."""
        with self._conn:
            self._conn.execute(
                "UPDATE meters SET postcode_prefix = ?, postcode_source = ? "
                "WHERE config_period_id = ? AND is_sub_meter = 0",
                (outward_code(outcode), source, period_id)
            )

    def apply_region_periods(self, spans: list, *, overwrite_user: bool = False) -> dict:
        """Auto-apply probed region spans (see derive_region_periods) onto the
        config-period timeline. Idempotent and privacy-preserving (outward code
        only). NEVER overwrites a user-set postcode unless overwrite_user=True.

        Single region (the common case, incl. never-moved accounts): stamp that
        outward code onto every period's main meter that lacks a user value —
        this fully "auto-applies" and lets carbon resolve historically.

        Multiple regions (a move): this needs a *historical* config-period split,
        for which there is no validated primitive yet (insert_config_period only
        appends a new current period). To avoid unvalidated surgery on
        billing-affecting history, we stamp the latest region where safe and
        return split_required=True with the spans, so the split can be done
        deliberately (and reviewed) rather than silently. See ROADMAP 3.5.1 /
        docs/region_timeline_design.md.
        """
        clean = []
        for s in (spans or []):
            oc = outward_code(s.get("outcode") or s.get("postcode"))
            if oc:
                clean.append({**s, "outcode": oc})
        if not clean:
            return {"applied": 0, "outcodes": [], "split_required": False,
                    "reason": "no region data"}

        outs = sorted({s["outcode"] for s in clean})
        periods = self._conn.execute(
            "SELECT id FROM config_periods ORDER BY effective_from ASC"
        ).fetchall()

        def _stamp(period_id: int, oc: str) -> bool:
            m = self._conn.execute(
                "SELECT postcode_prefix, postcode_source FROM meters "
                "WHERE config_period_id = ? AND is_sub_meter = 0 "
                "ORDER BY id ASC LIMIT 1", (period_id,)
            ).fetchone()
            if not m:
                return False
            try:
                src = m["postcode_source"]
            except (KeyError, IndexError):
                src = None
            if m["postcode_prefix"] and src == "user" and not overwrite_user:
                return False   # never clobber a user-set value
            if m["postcode_prefix"] == oc and src == "octopus":
                return False   # already applied — idempotent no-op
            self.set_period_postcode(period_id, oc, "octopus")
            return True

        applied = 0
        if len(outs) == 1:
            for p in periods:
                if _stamp(p["id"], outs[0]):
                    applied += 1
            return {"applied": applied, "outcodes": outs, "split_required": False}

        # Multiple regions → a move. Stamp the most-recent region where safe and flag.
        latest = sorted(clean, key=lambda s: (s.get("from") or ""))[-1]["outcode"]
        for p in periods:
            if _stamp(p["id"], latest):
                applied += 1
        logger.warning(
            "apply_region_periods: %d distinct regions %s imply a historical "
            "config-period split (a property move); applied latest region %r to "
            "existing periods only. Historical split NOT performed automatically.",
            len(outs), outs, latest)
        return {"applied": applied, "outcodes": outs, "split_required": True,
                "spans": clean}

    # ── Region reconciliation (post-import confirmation flow) ──────────────────
    @staticmethod
    def _site_for_date(sites: list, date_iso: str) -> Optional[dict]:
        """The tenancy whose [from, to) span contains date_iso (to=None → open).
        On overlap, the one with the latest `from` wins (most specific)."""
        best = None
        for s in sites:
            frm = s.get("from") or ""
            to  = s.get("to")
            if date_iso >= frm and (to is None or date_iso < to):
                if best is None or frm > (best.get("from") or ""):
                    best = s
        return best

    def _needs_split_at(self, from_iso: str):
        """(period_dict, snapped_iso) if a boundary at from_iso would genuinely
        divide an existing period; else (None, None)."""
        if not from_iso:
            return (None, None)
        cp = self.get_config_period_for_date(from_iso)
        if not cp:
            return (None, None)
        snapped = self._snap_to_midnight_utc(from_iso, cp.get("timezone") or "UTC")
        if snapped <= cp["effective_from"]:
            return (None, None)                       # boundary already present
        if cp.get("effective_to") is not None and snapped >= cp["effective_to"]:
            return (None, None)
        return (cp, snapped)

    def plan_region_reconciliation(self, derived: list) -> dict:
        """Read-only. Given derived tenancy spans (derive_region_periods), compute
        the plan to align the config-period timeline to the property history:

            {"needs_confirmation": bool,
             "sites": [{outcode, from, to, key, hint, block_count,
                        site_name(prefill|None), needs_name}],
             "split_dates": [iso, ...]}   # boundaries Apply will create

        `needs_confirmation` is True when any site lacks a name or any split is
        needed. Nothing is mutated."""
        sites = []
        for s in (derived or []):
            oc = outward_code(s.get("outcode"))
            if not oc:
                continue
            sites.append({"outcode": oc, "from": s.get("from"), "to": s.get("to"),
                          "key": s.get("key"), "hint": s.get("hint")})
        periods = self._conn.execute(
            "SELECT effective_from, effective_to, site_name FROM config_periods "
            "ORDER BY effective_from ASC").fetchall()
        if not sites or not periods:
            return {"needs_confirmation": False, "sites": [], "split_dates": []}

        split_dates = []
        for s in sites:
            for d in (s.get("from"), s.get("to")):   # a bounded span may need both ends cut
                cp, _snap = self._needs_split_at(d)
                if cp is not None:
                    split_dates.append(d)

        out_sites = []
        for s in sites:
            frm = s.get("from") or ""
            to  = s.get("to")
            if to is None:
                row = self._conn.execute(
                    "SELECT COUNT(*) c FROM blocks WHERE block_start >= ?", (frm,)).fetchone()
            else:
                row = self._conn.execute(
                    "SELECT COUNT(*) c FROM blocks WHERE block_start >= ? AND block_start < ?",
                    (frm, to)).fetchone()
            block_count = row["c"] if row else 0
            prefill = None
            for p in periods:
                pf = p["effective_from"]
                if pf >= frm and (to is None or pf < to) and p["site_name"]:
                    prefill = p["site_name"]
                    break
            out_sites.append({**s, "block_count": block_count,
                              "site_name": prefill, "needs_name": prefill is None})

        needs = any(x["needs_name"] for x in out_sites) or bool(split_dates)
        return {"needs_confirmation": needs, "sites": out_sites,
                "split_dates": sorted(set(split_dates))}

    def apply_region_reconciliation(self, sites: list) -> dict:
        """Apply a confirmed reconciliation: split config periods at each move
        boundary, then stamp every period with its tenancy's outward code
        (source 'octopus') and, where supplied, its user-confirmed site name.
        `sites` = [{outcode, from, to, site_name?}]. Returns counts."""
        splits = 0
        boundaries = []
        for s in sites:
            if s.get("from"):
                boundaries.append(s["from"])
            if s.get("to"):
                boundaries.append(s["to"])      # cut the far end of a bounded span too
        for d in sorted(set(boundaries)):
            cp, _snap = self._needs_split_at(d)
            if cp is not None:
                self.split_config_period_at(cp["id"], d)
                splits += 1
        stamped = 0
        periods = self._conn.execute(
            "SELECT id, effective_from, effective_to FROM config_periods ORDER BY effective_from ASC"
        ).fetchall()
        for p in periods:
            site = self._site_for_date(sites, p["effective_from"])
            if not site:
                continue
            oc = outward_code(site.get("outcode"))
            is_active = p["effective_to"] is None
            with self._conn:
                self._conn.execute(
                    "UPDATE meters SET postcode_prefix = ?, postcode_source = 'octopus' "
                    "WHERE config_period_id = ? AND is_sub_meter = 0", (oc, p["id"]))
                # NEVER rename the ACTIVE period — its site_name is the instance /
                # backup-folder identity (see instance.py). A historic import only
                # labels EARLIER (past-address) periods; the current site name is
                # managed via normal config, and a real future move creates a new
                # current period. Region (postcode) is always safe to stamp.
                if site.get("site_name") and not is_active:
                    self._conn.execute(
                        "UPDATE config_periods SET site_name = ? WHERE id = ?",
                        (site["site_name"], p["id"]))
            stamped += 1
        logger.info("apply_region_reconciliation: %d split(s), %d period(s) stamped",
                    splits, stamped)
        return {"splits": splits, "stamped": stamped}

    def plan_csv_reconciliation(self, from_iso: str, to_iso) -> dict:
        """Reconcile plan for a CSV import span. Unlike the API path, CSV carries
        NO provenance, so the single site is **region-editable**: the user names
        it AND supplies the outward code. If they skip, the blocks stay on their
        existing period(s) with region unknown (carbon excluded) — i.e. the data
        blends into whatever period already covers it. Returns the same shape as
        plan_region_reconciliation with one site + region_editable=True."""
        if not from_iso:
            return {"needs_confirmation": False, "sites": [], "split_dates": [], "source": "csv"}
        if to_iso is None:
            row = self._conn.execute(
                "SELECT COUNT(*) c FROM blocks WHERE block_start >= ?", (from_iso,)).fetchone()
        else:
            row = self._conn.execute(
                "SELECT COUNT(*) c FROM blocks WHERE block_start >= ? AND block_start < ?",
                (from_iso, to_iso)).fetchone()
        block_count = row["c"] if row else 0
        prefill = None
        for p in self._conn.execute(
                "SELECT effective_from, site_name FROM config_periods "
                "ORDER BY effective_from ASC").fetchall():
            pf = p["effective_from"]
            if pf >= from_iso and (to_iso is None or pf < to_iso) and p["site_name"]:
                prefill = p["site_name"]
                break
        split_dates = []
        for d in (from_iso, to_iso):
            cp, _snap = self._needs_split_at(d)
            if cp is not None:
                split_dates.append(d)
        site = {"outcode": None, "from": from_iso, "to": to_iso, "key": None,
                "hint": None, "region_editable": True, "block_count": block_count,
                "site_name": prefill, "needs_name": prefill is None}
        return {"needs_confirmation": True, "sites": [site],
                "split_dates": sorted(set(split_dates)), "source": "csv"}

    # ── Pre-import site confirmation (create covering periods BEFORE import) ────
    # The reconcile flow above SPLITS periods AFTER an import — which cannot
    # create a period earlier than the oldest existing one, so a past address is
    # only labelled from the earliest existing period forward. The canonical flow
    # is instead to CONFIRM the property history FIRST and create the covering
    # periods up front, so every imported block lands in its correct period from
    # the first write (correctly regioned + carbon-eligible), with no post-hoc
    # split or block reassignment. See docs/region_timeline_design.md.

    def create_covering_period(self, from_iso: str, to_iso,
                               *, outcode=None, site_name: Optional[str] = None,
                               based_on_period_id: Optional[int] = None) -> int:
        """Create a config period covering ``[from, to)`` by CLONING an existing
        period's config (meters + channels) and stamping its region (outward code)
        and site name. Intended for the PRE-import flow: the range is empty (no
        blocks exist yet), so nothing is reassigned and the chain of *other*
        periods is untouched — the caller (apply_pre_import_sites) guarantees
        contiguity. ``to`` may be None only for an open-ended (current) period,
        but in practice covering periods are bounded. Returns the new period id."""
        ref_id = based_on_period_id or self.get_current_config_period_id()
        if ref_id is None:
            row = self._conn.execute(
                "SELECT id FROM config_periods ORDER BY effective_from ASC LIMIT 1"
            ).fetchone()
            ref_id = row["id"] if row else None
        if ref_id is None:
            raise ValueError("create_covering_period: no reference period to clone")
        ref = self.get_config_period(ref_id)
        tz = ref.get("timezone") or "UTC"
        frm = self._snap_to_midnight_utc(from_iso, tz)
        to = self._snap_to_midnight_utc(to_iso, tz) if to_iso else None
        if to is not None and to <= frm:
            raise ValueError("create_covering_period: to must be after from")
        with self._conn:
            cur = self._conn.execute(
                """INSERT INTO config_periods
                       (effective_from, effective_to, billing_day, block_minutes,
                        timezone, currency_symbol, currency_code, site_name,
                        supplier, change_reason)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (frm, to, ref["billing_day"], ref["block_minutes"], ref["timezone"],
                 ref["currency_symbol"], ref["currency_code"], site_name,
                 ref.get("supplier"), "Historical site (pre-import)"))
            new_id = cur.lastrowid
            self._copy_meters_to_period(ref_id, new_id)
        if outcode:
            self.set_period_postcode(new_id, outcode, "octopus")
        logger.info("create_covering_period: [%s,%s) region=%s site=%r → period %d",
                    frm, to, outward_code(outcode), site_name, new_id)
        return new_id

    def _period_starting_at(self, from_iso: str) -> Optional[int]:
        """Id of a config period whose effective_from equals from_iso (snapped in
        the timeline's timezone), or None. Gives apply_pre_import_sites its
        idempotency: a re-run updates the existing period instead of duplicating."""
        if not from_iso:
            return None
        row = self._conn.execute(
            "SELECT timezone FROM config_periods ORDER BY effective_from ASC LIMIT 1"
        ).fetchone()
        tz = (row["timezone"] if row else None) or "UTC"
        snapped = self._snap_to_midnight_utc(from_iso, tz)
        hit = self._conn.execute(
            "SELECT id FROM config_periods WHERE effective_from = ? LIMIT 1",
            (snapped,)).fetchone()
        return hit["id"] if hit else None

    def _stamp_region_if_free(self, period_id: int, oc: str) -> bool:
        """Stamp outward code (source 'octopus') onto a period's main meter unless
        it already carries a USER value. Returns True if it changed anything."""
        m = self._conn.execute(
            "SELECT postcode_prefix, postcode_source FROM meters "
            "WHERE config_period_id = ? AND is_sub_meter = 0 ORDER BY id ASC LIMIT 1",
            (period_id,)).fetchone()
        if not m:
            return False
        try:
            src = m["postcode_source"]
        except (KeyError, IndexError):
            src = None
        if m["postcode_prefix"] and src == "user":
            return False
        if m["postcode_prefix"] == oc and src == "octopus":
            return False
        self.set_period_postcode(period_id, oc, "octopus")
        return True

    def plan_pre_import_sites(self, derived: list) -> dict:
        """Read-only. Classify derived tenancy spans (derive_region_periods) for
        the PRE-import confirmation UI:

            {"needs_confirmation": bool,
             "sites": [{outcode, from, to, key, hint, is_current,
                        site_name(prefill|None), needs_name}]}

        The latest span (to=None) is the CURRENT site — shown read-only and
        prefilled from the active period's site_name, because that name is the
        instance / backup-folder identity (see instance.py) and must not change on
        a historic import. Earlier spans are PAST sites the user names; each will
        become a new covering period. needs_confirmation is True only when there
        is at least one past site to name (a single-site account confirms nothing
        and goes straight to import). Nothing is mutated."""
        spans = []
        for s in (derived or []):
            oc = outward_code(s.get("outcode"))
            if not oc:
                continue
            spans.append({"outcode": oc, "from": s.get("from"), "to": s.get("to"),
                          "key": s.get("key"), "hint": s.get("hint")})
        spans.sort(key=lambda s: s.get("from") or "")
        if not spans:
            return {"needs_confirmation": False, "sites": []}
        arow = self._conn.execute(
            "SELECT site_name FROM config_periods WHERE effective_to IS NULL "
            "ORDER BY effective_from DESC LIMIT 1").fetchone()
        active_name = arow["site_name"] if arow else None
        out = []
        for s in spans:
            is_current = s.get("to") is None
            out.append({**s, "is_current": is_current,
                        "site_name": active_name if is_current else None,
                        "needs_name": (not is_current)})
        return {"needs_confirmation": any(x["needs_name"] for x in out),
                "sites": out}

    def apply_pre_import_sites(self, sites: list) -> dict:
        """Create/extend config periods to match confirmed tenancy sites BEFORE an
        import runs, so imported blocks land in the right period from the first
        write. ``sites`` = [{outcode, from, to, site_name?}] (as confirmed in the
        UI). Order of operations matters for contiguity:

          1. CURRENT tenancy (to=None): extend the ACTIVE period's effective_from
             back to its move-in (backward only — never shrinks, never renames:
             instance identity) and stamp its region. Done FIRST, while the active
             period is still the earliest, so the extend targets it.
          2. PAST tenancies: create a covering period for each (or, on a re-run,
             update the existing one). Each abuts the next via the tenancy spans.

        Idempotent. Returns {"created", "extended", "stamped"}."""
        clean = sorted([s for s in (sites or []) if s.get("from")],
                       key=lambda s: s["from"])
        if not clean:
            return {"created": 0, "extended": False, "stamped": 0}
        current = next((s for s in clean if s.get("to") is None), None)
        past = [s for s in clean if s.get("to") is not None]
        extended = False
        stamped = 0
        # 1. Current tenancy → extend + region-stamp the active period (no rename).
        if current:
            extended = self.extend_earliest_period_to(current["from"])
            oc = outward_code(current.get("outcode"))
            aid = self.get_current_config_period_id()
            if oc and aid and self._stamp_region_if_free(aid, oc):
                stamped += 1
        # 2. Past tenancies → create (or update) covering periods.
        created = 0
        for s in past:
            oc = outward_code(s.get("outcode"))
            existing = self._period_starting_at(s["from"])
            if existing is not None:
                if oc:
                    self.set_period_postcode(existing, oc, "octopus")
                if s.get("site_name"):
                    with self._conn:
                        self._conn.execute(
                            "UPDATE config_periods SET site_name = ? "
                            "WHERE id = ? AND effective_to IS NOT NULL",
                            (s["site_name"], existing))
            else:
                self.create_covering_period(
                    s["from"], s["to"], outcode=oc, site_name=s.get("site_name"))
                created += 1
        logger.info("apply_pre_import_sites: created=%d extended=%s stamped=%d",
                    created, extended, stamped)
        return {"created": created, "extended": extended, "stamped": stamped}

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
            power_src   = meta.get("power_source")
            rate_src    = meta.get("rate_source")
            postcode    = meta.get("postcode_prefix")
            v2x         = 1 if meta.get("v2x_capable") else 0
            meter_type  = meta.get("meter_type")
            soc_s       = meta.get("soc_sensor")
            inv_pwr_s    = meta.get("inverter_power_sensor")
            inv_invert   = 1 if meta.get("inverter_power_invert") else 0
            pwr_invert   = 1 if meta.get("power_invert") else 0
            dev_invert   = 1 if meta.get("device_power_invert") else 0
            dev_pwr_s   = meta.get("device_power_sensor")
            pv_pwr_s    = meta.get("pv_power_sensor")

            postcode_src = meta.get("postcode_source")
            cur = self._conn.execute(
                """INSERT INTO meters
                       (config_period_id, meter_id, is_sub_meter, parent_meter_id,
                        device_label, protected, inverter_possible,
                        power_sensor, power_source, postcode_prefix, postcode_source, v2x_capable,
                        meter_type, soc_sensor, inverter_power_sensor, inverter_power_invert,
                        device_power_sensor, pv_power_sensor, rate_source, power_invert,
                        device_power_invert)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT(config_period_id, meter_id) DO UPDATE SET
                       is_sub_meter            = excluded.is_sub_meter,
                       parent_meter_id         = excluded.parent_meter_id,
                       device_label            = excluded.device_label,
                       protected               = excluded.protected,
                       inverter_possible       = excluded.inverter_possible,
                       power_sensor            = excluded.power_sensor,
                       power_source            = excluded.power_source,
                       rate_source             = excluded.rate_source,
                       postcode_prefix         = excluded.postcode_prefix,
                       postcode_source         = excluded.postcode_source,
                       v2x_capable             = excluded.v2x_capable,
                       meter_type              = excluded.meter_type,
                       soc_sensor              = excluded.soc_sensor,
                       inverter_power_sensor   = excluded.inverter_power_sensor,
                       inverter_power_invert   = excluded.inverter_power_invert,
                       power_invert            = excluded.power_invert,
                       device_power_invert     = excluded.device_power_invert,
                       device_power_sensor     = excluded.device_power_sensor,
                       pv_power_sensor         = excluded.pv_power_sensor,
                       retired_at              = COALESCE(meters.retired_at, excluded.retired_at),
                       retired_reason          = COALESCE(meters.retired_reason, excluded.retired_reason)""",
                (period_id, meter_id, is_sub, parent, device,
                 protected, inv_poss, power_s, power_src, postcode, postcode_src, v2x,
                 meter_type, soc_s, inv_pwr_s, inv_invert, dev_pwr_s, pv_pwr_s, rate_src, pwr_invert,
                 dev_invert)
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
                            standing_charge_sensor, mpan, tariff,
                            rate_source, standing_charge_source)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                       ON CONFLICT(meter_id, channel) DO UPDATE SET
                           read_sensor            = excluded.read_sensor,
                           rate_sensor            = excluded.rate_sensor,
                           standing_charge_sensor = excluded.standing_charge_sensor,
                           mpan                   = excluded.mpan,
                           tariff                 = excluded.tariff,
                           rate_source            = excluded.rate_source,
                           standing_charge_source = excluded.standing_charge_source""",
                    (
                        meter_row_id, channel,
                        ch_cfg.get("read"),
                        ch_cfg.get("rate"),
                        ch_cfg.get("standing_charge_sensor"),
                        ch_meta.get("mpan"),
                        ch_meta.get("tariff"),
                        ch_cfg.get("rate_source"),
                        ch_cfg.get("standing_charge_source"),
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
            try:
                if m["power_source"]:
                    meta["power_source"] = m["power_source"]
                if m["rate_source"]:
                    meta["rate_source"] = m["rate_source"]
            except (KeyError, IndexError):
                pass
            if m["postcode_prefix"]:
                meta["postcode_prefix"] = m["postcode_prefix"]
            try:
                if m["postcode_source"]:
                    meta["postcode_source"] = m["postcode_source"]
            except (KeyError, IndexError):
                pass
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
                if "power_invert" in m.keys() and m["power_invert"]:
                    meta["power_invert"] = True
                if "device_power_invert" in m.keys() and m["device_power_invert"]:
                    meta["device_power_invert"] = True
                if m["device_power_sensor"]:
                    meta["device_power_sensor"] = m["device_power_sensor"]
                if m["pv_power_sensor"]:
                    meta["pv_power_sensor"] = m["pv_power_sensor"]
                if m["retired_at"]:
                    meta["retired_at"] = m["retired_at"]
                if m["retired_reason"]:
                    meta["retired_reason"] = m["retired_reason"]
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
                # 3.0.0 per-channel source toggles (API vs sensor; 'main' = device
                # inherits the main meter's effective rate). Guarded for older DBs.
                try:
                    if ch["rate_source"]:
                        ch_dict["rate_source"] = ch["rate_source"]
                    if ch["standing_charge_source"]:
                        ch_dict["standing_charge_source"] = ch["standing_charge_source"]
                except (KeyError, IndexError):
                    pass
                # mpan / tariff as channel meta dict (preserves engine-expected shape)
                ch_meta = {}
                if ch["mpan"]:
                    ch_meta["mpan"] = ch["mpan"]
                if ch["tariff"]:
                    ch_meta["tariff"] = ch["tariff"]
                if ch_meta:
                    ch_dict["meta"] = ch_meta
                channels[ch["channel"]] = ch_dict

            # Engine compatibility: the overlay path reads meta["rate_source"].
            # For a device, derive it from the (authoritative) import-channel
            # source so the existing engine honours the explicit choice:
            #   'main'   → 'overlay' (price on the main meter's effective rate)
            #   'sensor' → 'own'     (the device's own rate sensor wins)
            if meta.get("sub_meter"):
                _imp_src = (channels.get("import") or {}).get("rate_source")
                if _imp_src == "main":
                    meta["rate_source"] = "overlay"
                elif _imp_src == "sensor":
                    meta["rate_source"] = "own"

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

    # ── Region timeline: historical config-period split ───────────────────────
    def _copy_meters_to_period(self, src_period_id: int, dst_period_id: int) -> None:
        """Deep-copy every meter (and its channels) from one config period to
        another, preserving all columns except the surrogate keys. Used by
        split_config_period_at so the new half is a faithful config clone."""
        m_cols = [r[1] for r in self._conn.execute("PRAGMA table_info(meters)").fetchall()]
        copy_cols = [c for c in m_cols if c not in ("id", "config_period_id")]
        col_list = ", ".join(copy_cols)
        placeholders = ", ".join("?" for _ in copy_cols)
        src_rows = self._conn.execute(
            f"SELECT id, {col_list} FROM meters WHERE config_period_id = ?",
            (src_period_id,)
        ).fetchall()
        mc_cols = [r[1] for r in self._conn.execute("PRAGMA table_info(meter_channels)").fetchall()]
        ch_cols = [c for c in mc_cols if c not in ("id", "meter_id")]
        ch_list = ", ".join(ch_cols)
        ch_ph = ", ".join("?" for _ in ch_cols)
        for row in src_rows:
            old_meter_id = row["id"]
            vals = [row[c] for c in copy_cols]
            cur = self._conn.execute(
                f"INSERT INTO meters (config_period_id, {col_list}) VALUES (?, {placeholders})",
                [dst_period_id] + vals
            )
            new_meter_id = cur.lastrowid
            if ch_cols:
                for ch in self._conn.execute(
                    f"SELECT {ch_list} FROM meter_channels WHERE meter_id = ?",
                    (old_meter_id,)
                ).fetchall():
                    self._conn.execute(
                        f"INSERT INTO meter_channels (meter_id, {ch_list}) VALUES (?, {ch_ph})",
                        [new_meter_id] + [ch[c] for c in ch_cols]
                    )

    def split_config_period_at(self, period_id: int, split_date_iso: str,
                               *, change_reason: Optional[str] = None) -> int:
        """Divide an existing config period at a past date into two contiguous
        periods, preserving billing history.

        The ORIGINAL period keeps the earlier span ``[effective_from, split)``.
        A NEW period covers the later span ``[split, effective_to)`` with a full
        COPY of the original's meters + channels. Every block whose ``block_start``
        is >= split is reassigned to the new period; earlier blocks stay.

        This is the primitive the region timeline needs to record a mid-history
        property move (``insert_config_period`` only ever appends a *new current*
        period). The caller sets the region/site on whichever half applies.

        ``split_date_iso`` is snapped to local midnight (the period's timezone),
        matching ``insert_config_period``. Returns the NEW period's id.

        Raises ``ValueError`` if the split is not strictly inside the period.
        """
        cp = self.get_config_period(period_id)
        if not cp:
            raise ValueError(f"config period {period_id} not found")
        tz_name = cp.get("timezone") or "UTC"
        split = self._snap_to_midnight_utc(split_date_iso, tz_name)
        ef_from = cp["effective_from"]
        ef_to   = cp.get("effective_to")   # None for the active period
        if split <= ef_from:
            raise ValueError("split date must be after the period's effective_from")
        if ef_to is not None and split >= ef_to:
            raise ValueError("split date must be before the period's effective_to")

        with self._conn:
            cur = self._conn.execute(
                """INSERT INTO config_periods
                       (effective_from, effective_to, billing_day, block_minutes,
                        timezone, currency_symbol, currency_code, site_name,
                        supplier, change_reason)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    split, ef_to,
                    cp["billing_day"], cp["block_minutes"], cp["timezone"],
                    cp["currency_symbol"], cp["currency_code"],
                    cp.get("site_name"), cp.get("supplier"),
                    change_reason or "Region timeline split",
                )
            )
            new_id = cur.lastrowid
            self._copy_meters_to_period(period_id, new_id)
            # Shrink the original to the earlier half.
            self._conn.execute(
                "UPDATE config_periods SET effective_to = ? WHERE id = ?",
                (split, period_id)
            )
            # Reassign the later blocks to the new period.
            self._conn.execute(
                "UPDATE blocks SET config_period_id = ? "
                "WHERE config_period_id = ? AND block_start >= ?",
                (new_id, period_id, split)
            )
        logger.info("split_config_period_at: period %d split at %s → new period %d",
                    period_id, split, new_id)
        return new_id

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
                imp_rate, imp_cost, imp_cost_remainder, imp_cost_exc, imp_rate_exc,
                imp_read_start, imp_read_end,
                exp_kwh, exp_rate, exp_cost,
                exp_read_start, exp_read_end,
                standing_charge, standing_charge_exc, carbon_g, carbon_intensity_g, imp_provisional,
                source, exc_source, is_provisional, needs_pass2_rerun, imp_kwh_api, needs_review,
                exp_kwh_api
            ) VALUES (
                :block_start, :block_end,
                :meter_id, :config_period_id, :interpolated,
                :imp_kwh, :imp_kwh_grid, :imp_kwh_remainder,
                :imp_rate, :imp_cost, :imp_cost_remainder, :imp_cost_exc, :imp_rate_exc,
                :imp_read_start, :imp_read_end,
                :exp_kwh, :exp_rate, :exp_cost,
                :exp_read_start, :exp_read_end,
                :standing_charge, :standing_charge_exc, :carbon_g, :carbon_intensity_g, :imp_provisional,
                :source, :exc_source, :is_provisional, :needs_pass2_rerun, :imp_kwh_api, :needs_review,
                :exp_kwh_api
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
                imp_rate, imp_cost, imp_cost_remainder, imp_cost_exc, imp_rate_exc,
                imp_read_start, imp_read_end,
                exp_kwh, exp_rate, exp_cost,
                exp_read_start, exp_read_end,
                standing_charge, standing_charge_exc, carbon_g, carbon_intensity_g, imp_provisional,
                source, exc_source, is_provisional, needs_pass2_rerun, imp_kwh_api, needs_review,
                exp_kwh_api
            ) VALUES (
                :block_start, :block_end,
                :meter_id, :config_period_id, :interpolated,
                :imp_kwh, :imp_kwh_grid, :imp_kwh_remainder,
                :imp_rate, :imp_cost, :imp_cost_remainder, :imp_cost_exc, :imp_rate_exc,
                :imp_read_start, :imp_read_end,
                :exp_kwh, :exp_rate, :exp_cost,
                :exp_read_start, :exp_read_end,
                :standing_charge, :standing_charge_exc, :carbon_g, :carbon_intensity_g, :imp_provisional,
                :source, :exc_source, :is_provisional, :needs_pass2_rerun, :imp_kwh_api, :needs_review,
                :exp_kwh_api
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

    def get_provisional_sub_meter_blocks(self) -> list[dict]:
        """Return the most-recent block for each sub-meter that has imp_provisional=1.

        Used by the 2.10.0 boundary interpolation amendment path: after each
        engine tick we check whether a post-boundary read has arrived for any
        sub-meter whose previous block was written as provisional (no post-boundary
        read at block-close time).  Returns one single-meter block dict per
        provisional sub-meter row.
        """
        try:
            cur = self._conn.execute(
                """
                SELECT b.*, cp.billing_day, cp.block_minutes, cp.timezone,
                       cp.currency_symbol, cp.currency_code, cp.effective_from,
                       m.is_sub_meter, m.parent_meter_id, m.device_label,
                       m.inverter_possible, m.power_sensor, m.postcode_prefix,
                       m.v2x_capable, m.meter_type, m.soc_sensor,
                       m.inverter_power_sensor, m.inverter_power_invert,
                       m.device_power_sensor, m.retired_at, m.retired_reason
                FROM blocks b
                JOIN config_periods cp ON b.config_period_id = cp.id
                LEFT JOIN meters m ON m.meter_id = b.meter_id
                                   AND m.config_period_id = b.config_period_id
                WHERE b.imp_provisional = 1
                  AND m.is_sub_meter = 1
                  AND b.block_start = (
                      SELECT MAX(b2.block_start) FROM blocks b2
                      WHERE b2.meter_id = b.meter_id AND b2.imp_provisional = 1
                  )
                ORDER BY b.block_start, b.meter_id
                """
            )
            rows = cur.fetchall()
        except Exception:
            return []
        result = []
        for row in rows:
            blk = _row_to_block([row])
            if blk:
                result.append(blk)
        return result

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
                                utc_end: str | None = None,
                                finalised_only: bool = False) -> list[dict]:
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

        if finalised_only:
            where = (where + " AND b.is_provisional = 0") if where \
                    else "WHERE b.is_provisional = 0"

        rows = self._conn.execute(
            f"""SELECT b.block_start, b.meter_id,
                       b.imp_kwh, b.imp_kwh_grid, b.imp_kwh_remainder,
                       b.imp_rate, b.imp_cost, b.imp_read_start, b.imp_read_end,
                       b.imp_cost_exc, b.imp_rate_exc, b.standing_charge_exc, b.exc_source,
                       b.exp_kwh, b.exp_rate, b.exp_cost,
                       b.exp_read_start, b.exp_read_end,
                       b.standing_charge, b.carbon_g, b.carbon_intensity_g, b.interpolated,
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

            imp_channel = {
                "kwh":           imp,
                "cost":          float(row["imp_cost"] or 0),
                "read_start":    row["imp_read_start"],
                "read_end":      row["imp_read_end"],
            }
            # Only include kwh_grid / kwh_remainder / rate when actually set in
            # the DB. _row_to_block omits these when NULL; get_blocks_lightweight
            # must do the same, otherwise a present-but-None key makes downstream
            # membership tests (`if "kwh_remainder" in channel`) read None as 0,
            # zeroing a real import on no-sub-meter setups (the "cost but no kWh"
            # billing bug). kwh_grid/kwh_remainder are only set by PASS 2 when
            # sub-meters exist; on a sub-less main meter they are legitimately NULL.
            if row["imp_kwh_grid"] is not None:
                imp_channel["kwh_grid"] = float(row["imp_kwh_grid"])
            if row["imp_kwh_remainder"] is not None:
                imp_channel["kwh_remainder"] = float(row["imp_kwh_remainder"])
            if row["imp_rate"] is not None:
                imp_channel["rate"] = float(row["imp_rate"])
            # BL-23/BL-24: surface captured ex-VAT so the billing summary's bill method
            # and the data table's ex-VAT columns use the REAL figure (not inc÷1.05).
            # _row_to_block does this too; the lightweight chart fetch must match or the
            # whole ex-VAT view silently falls back to the approximation.
            if row["imp_cost_exc"] is not None:
                imp_channel["cost_exc"] = float(row["imp_cost_exc"])
            if row["imp_rate_exc"] is not None:
                imp_channel["rate_exc"] = float(row["imp_rate_exc"])

            exp_channel = {
                "kwh":        exp,
                "cost":       float(row["exp_cost"] or 0),
                "read_start": row["exp_read_start"],
                "read_end":   row["exp_read_end"],
            }
            if row["exp_rate"] is not None:
                exp_channel["rate"] = float(row["exp_rate"])

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
                "carbon_intensity_g": row["carbon_intensity_g"],
                "imp_kwh":       imp,
                "exp_kwh":       exp,
                "imp_cost":      float(row["imp_cost"] or 0),
                "exp_cost":      float(row["exp_cost"] or 0),
                "standing_charge": float(row["standing_charge"] or 0),
                "interpolated":  bool(row["interpolated"]),
                "channels": {
                    "import": imp_channel,
                    "export": exp_channel,
                },
            }
            if row["standing_charge_exc"] is not None:
                b["meters"][mid]["standing_charge_exc"] = float(row["standing_charge_exc"])
            if row["exc_source"] is not None:
                b["meters"][mid]["exc_source"] = row["exc_source"]

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
                             meter_id: Optional[str] = None,
                             finalised_only: bool = False) -> list[dict]:
        """Return blocks within [start, end], optionally filtered by meter."""
        start_iso = start.isoformat()
        end_iso   = end.isoformat()
        fin = self._finalised_clause(finalised_only)
        if meter_id:
            return self._select_blocks(
                "WHERE b.block_start >= ? AND b.block_start <= ? AND b.meter_id = ?" + fin,
                (start_iso, end_iso, meter_id)
            )
        return self._select_blocks(
            "WHERE b.block_start >= ? AND b.block_start <= ?" + fin,
            (start_iso, end_iso)
        )

    # ── UTC-based query methods ───────────────────────────────────────────────

    def get_block_dict_by_start(self, block_start: str) -> Optional[dict]:
        """Return the fully-reconstructed (joined) block dict for a single
        block_start — all meters, with meta/channels and the 3.0.0 columns
        (carbon_intensity_g, imp_kwh_api, is_provisional) surfaced.

        Used by the DCC PASS 2+3b re-run, which needs the same dict shape that
        _apply_pass2 / PASS 3b operate on in finalise_block. Returns None if no
        block exists at that start.
        """
        blocks = self._select_blocks(
            "WHERE b.block_start = ?", (block_start,)
        )
        return blocks[0] if blocks else None

    def get_blocks_for_utc_range(self, utc_start: str, utc_end: str,
                                  meter_id: str | None = None,
                                  finalised_only: bool = False) -> list[dict]:
        """
        Return all blocks where block_start >= utc_start AND block_start < utc_end.
        utc_start / utc_end are naive ISO strings (no tzinfo) matching DB format.
        Replaces get_blocks_for_local_date_range — use local_date_range_to_utc_bounds()
        to compute the UTC bounds from local date strings.
        """
        fin = self._finalised_clause(finalised_only)
        if meter_id:
            return self._select_blocks(
                "WHERE b.block_start >= ? AND b.block_start < ? AND b.meter_id = ?" + fin,
                (utc_start, utc_end, meter_id)
            )
        return self._select_blocks(
            "WHERE b.block_start >= ? AND b.block_start < ?" + fin,
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
# Factory
# ─────────────────────────────────────────────────────────────────────────────

def open_block_store(db_path: str) -> "BlockStore":
    """
    Preferred entry point. Opens (or creates) a BlockStore at db_path,
    applies all PRAGMAs, and ensures the schema exists.

    If the DB file is corrupt, renames it to .corrupt and starts fresh
    so the engine can still start.
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
        # Start with a fresh DB.
        return BlockStore(db_path)