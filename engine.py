"""
engine.py
=========
Energy Meter Tracker — ported from PyScript to HA add-on.

PyScript → add-on mapping:
  log.info/error          →  logging module
  state.get(entity_id)    →  read_sensor(ha, entity_id)
  state.set(...)          →  await ha.set_state(...)
  task.executor(fn, ...)  →  fn(...)  (direct call — we own the thread model)
  @state_trigger(eid)     →  ha.subscribe_state(eid, callback)
  @time_trigger startup   →  engine_startup() called from main()
  @time_trigger period    →  asyncio.sleep(10) loop in engine_loop_task()
  @task_unique(...)       →  asyncio.Lock()
"""

import asyncio
import logging
import os
import shutil
from datetime import datetime, timedelta, timezone

from energy_engine_io import (
    ensure_dir,
    load_json,
    save_json_atomic,
    save_file,
)
import energy_charts
from ha_client import HAClient
from block_store import BlockStore, open_block_store, migrate_json_to_sqlite

logger = logging.getLogger("engine")

# ─────────────────────────────────────────────────────────────────────────────
# Paths & constants
# ─────────────────────────────────────────────────────────────────────────────

DATA_DIR           = "/data/energy_meter_tracker"
CONFIG_PATH        = f"{DATA_DIR}/meters_config.json"
# current_block.json removed in 2.1.0 — state stored in DB current_block table
BLOCKS_PATH        = f"{DATA_DIR}/blocks.json"    # read-only: used only for one-time migration on startup
BLOCKS_DB_PATH     = f"{DATA_DIR}/blocks.db"
# cumulative_totals.json removed in 2.1.0 — totals derived from blocks table

import os as _os_engine
SHARE_BACKUP_DIR   = (
    _os_engine.path.join(DATA_DIR, "backup")
    if _os_engine.environ.get("EMT_MODE") == "standalone"
    else "/share/energy_meter_tracker_backup"
)

# Module-level BlockStore instance — opened in engine_startup()
_store: BlockStore | None = None


def get_store() -> BlockStore:
    """Return the active BlockStore. Raises if engine_startup has not run."""
    if _store is None:
        raise RuntimeError("BlockStore not initialised — engine_startup() has not run")
    return _store


def get_and_clear_meter_reset() -> bool:
    """Return True if a meter read reset was detected since last call, then clear the flag.
    Used by the web server to surface an advisory notification to the user."""
    global _meter_reset_detected
    val = _meter_reset_detected
    _meter_reset_detected = False
    return val

CHART_DIR          = "/data/energy_meter_tracker"   # accessible from HA /local/
BLOCK_MINUTES      = 30  # default — overridden at runtime from config
GAP_FILL_LIMIT_HOURS = 12  # gaps longer than this are not gap-filled

# ─────────────────────────────────────────────────────────────────────────────
# Module-level state
# ─────────────────────────────────────────────────────────────────────────────

_read_queue:               list         = []
_last_known_sensor_values: dict         = {}
_PUBLISH_HA_SENSORS: bool = os.environ.get("PUBLISH_HA_SENSORS", "true").lower() != "false"
_engine_loop_lock:         asyncio.Lock = None   # initialised in setup()
_engine_paused:            bool         = False
_last_ci_fetch:            datetime | None = None   # UTC — last carbon intensity fetch
_current_slot_mix:         dict            = {}      # captured_at → generationmix list
_meter_reset_detected:     bool            = False   # set when post-gap read < pre-gap read (possible meter replacement)


def setup():
    """Call once from main() before starting any tasks."""
    global _engine_loop_lock
    _engine_loop_lock = asyncio.Lock()
    ensure_dir(DATA_DIR)
    ensure_dir(CHART_DIR)


def pause_engine():
    """Pause the engine loop — called by the import page before writing files."""
    global _engine_paused
    _engine_paused = True
    logger.info("engine: paused")


def resume_engine():
    """Resume the engine loop — called by the import page after files are written."""
    global _engine_paused
    _engine_paused = False
    logger.info("engine: resumed")


def reset_store():
    """
    Close the current BlockStore and reopen it from BLOCKS_DB_PATH.
    Called after a DB restore/import so the engine picks up the new file.
    Must be called while the engine is paused.
    """
    global _store
    try:
        if _store is not None:
            try:
                _store.close()
            except Exception:
                pass
            _store = None
        _store = open_block_store(BLOCKS_DB_PATH)
        logger.info("engine: store reset — reopened %s", BLOCKS_DB_PATH)
    except Exception as e:
        logger.error("engine: reset_store failed: %s", e)
        raise


# ─────────────────────────────────────────────────────────────────────────────
# IO helpers
# ─────────────────────────────────────────────────────────────────────────────

def io_save(path: str, data):
    save_json_atomic(path, data)


def append_block(block: dict):
    get_store().append_block(block)


def append_block_replace(block: dict):
    get_store().append_block_replace(block)


def io_save_file(path: str, content: str):
    save_file(path, content)


# ─────────────────────────────────────────────────────────────────────────────
# Backup to /share
# ─────────────────────────────────────────────────────────────────────────────

def _backup_to_share():
    """Backup data files to SHARE_BACKUP_DIR after each block finalise."""
    try:
        ensure_dir(SHARE_BACKUP_DIR)
        # Backup SQLite DB using online backup API (safe while engine is writing)
        store = get_store()
        store.backup(f"{SHARE_BACKUP_DIR}/blocks.db")
        # Copy remaining JSON files
        for filename in ("meters_config.json",):  # current_block state is in DB
            src_path = f"{DATA_DIR}/{filename}"
            dst_path = f"{SHARE_BACKUP_DIR}/{filename}"
            if os.path.exists(src_path):
                shutil.copy2(src_path, dst_path)
        logger.info("_backup_to_share: backup written to %s", SHARE_BACKUP_DIR)
    except Exception as e:
        logger.warning("_backup_to_share: failed: %s", e)


# ─────────────────────────────────────────────────────────────────────────────
# Time helpers
# ─────────────────────────────────────────────────────────────────────────────

def floor_to_block(dt: datetime, block_minutes: int = BLOCK_MINUTES) -> datetime:
    minute = (dt.minute // block_minutes) * block_minutes
    return dt.replace(minute=minute, second=0, microsecond=0)


def floor_to_hh(dt: datetime) -> datetime:
    """Deprecated alias — use floor_to_block."""
    return floor_to_block(dt, BLOCK_MINUTES)


def iso(dt: datetime) -> str:
    return dt.isoformat()


# ─────────────────────────────────────────────────────────────────────────────
# Currency detection
# ─────────────────────────────────────────────────────────────────────────────

# Map ISO 4217 currency codes to symbols
_CURRENCY_SYMBOLS = {
    "GBP": "£", "USD": "$", "EUR": "€", "AUD": "A$", "CAD": "C$",
    "NZD": "NZ$", "SGD": "S$", "HKD": "HK$", "JPY": "¥", "CNY": "¥",
    "SEK": "kr", "NOK": "kr", "DKK": "kr", "CHF": "Fr", "INR": "₹",
    "ZAR": "R", "BRL": "R$", "MXN": "$", "TRY": "₺", "KRW": "₩",
}


def detect_currency_symbol(unit_of_measurement: str) -> str:
    """
    Extract a currency symbol from a HA sensor unit string.
    Examples: "GBP/kWh" → "£", "USD/kWh" → "$", "EUR/day" → "€"
    Falls back to the raw currency code if not in the lookup table,
    or "¤" (generic currency sign) if nothing can be parsed.
    """
    if not unit_of_measurement:
        return "¤"
    # Strip trailing unit suffix e.g. "/kWh", "/MWh", "/day"
    code = unit_of_measurement.split("/")[0].strip().upper()
    return _CURRENCY_SYMBOLS.get(code, code) if code else "¤"


# ─────────────────────────────────────────────────────────────────────────────
# Config
# ─────────────────────────────────────────────────────────────────────────────

def load_config() -> dict:
    """
    Load meter configuration from the normalised DB (blocks.db) config_periods table.
    The DB is the single source of truth. meters_config.json is only used before
    the DB is open at startup on a truly fresh install with no DB file.
    """
    global _store
    db_exists = os.path.exists(os.path.join(DATA_DIR, "blocks.db"))

    if _store is not None:
        try:
            cp = _store._conn.execute(
                "SELECT id FROM config_periods "
                "WHERE effective_to IS NULL ORDER BY effective_from DESC LIMIT 1"
            ).fetchone()
            if cp:
                return _store.config_from_db(cp["id"])
            # DB open but no config period — fresh DB
            if db_exists:
                logger.warning("load_config: DB open but no active config period")
                return {"meters": {}}
        except Exception as _e:
            logger.error("load_config: DB read failed: %s", _e)
            if db_exists:
                logger.error("load_config: DB exists — not falling back to JSON")
                return {"meters": {}}

    # Pre-startup: DB not open yet
    if db_exists:
        # DB exists but store not open yet — don't use stale JSON
        logger.warning("load_config: store not open, DB exists — returning empty config")
        return {"meters": {}}

    # True fresh install: no DB, use meters_config.json
    logger.info("load_config: no DB found, loading from meters_config.json")
    return load_json(CONFIG_PATH, {"meters": {}})


def get_block_minutes() -> int:
    """Read block_minutes from the main meter meta — defaults to 30."""
    cfg = load_config()
    for meter_id, meter in (cfg.get("meters") or {}).items():
        bm = (meter.get("meta") or {}).get("block_minutes")
        if bm:
            return int(bm)
    return BLOCK_MINUTES


# ─────────────────────────────────────────────────────────────────────────────
# Block lifecycle helpers
# ─────────────────────────────────────────────────────────────────────────────

def get_block_window(now: datetime, block_minutes: int = BLOCK_MINUTES):
    start = floor_to_block(now, block_minutes)
    return start, start + timedelta(minutes=block_minutes)


def create_block(start: datetime, end: datetime, block_minutes: int = BLOCK_MINUTES) -> dict:
    return {
        "start":         iso(start),
        "end":           iso(end),
        "block_minutes": block_minutes,
        "meters":        {},
        "interpolated":  False,
    }


def interpolate_value(pre_read: dict, post_read: dict, target_dt: datetime) -> dict:
    pre_ts         = datetime.fromisoformat(pre_read["ts"])
    post_ts        = datetime.fromisoformat(post_read["ts"])
    window_seconds = (post_ts - pre_ts).total_seconds()

    if window_seconds <= 0:
        logger.warning("interpolate_value: zero/negative window, returning pre_read value")
        return {"value": pre_read["value"], "ts": target_dt.isoformat(), "interpolated": True}

    fraction           = max(0.0, min(1.0, (target_dt - pre_ts).total_seconds() / window_seconds))
    interpolated_value = pre_read["value"] + fraction * (post_read["value"] - pre_read["value"])
    result             = round(interpolated_value, 3)

    logger.info(
        "interpolate_value: %.3f → %.3f at %s fraction=%.4f result=%.3f",
        pre_read["value"], post_read["value"], target_dt.isoformat(), fraction, result,
    )
    return {"value": result, "ts": target_dt.isoformat(), "interpolated": True}


def detect_gap(last_read_ts: str | None, now: datetime, block_minutes: int = BLOCK_MINUTES) -> list:
    if not last_read_ts:
        return []

    last_dt        = datetime.fromisoformat(last_read_ts)
    last_block_end = floor_to_block(last_dt, block_minutes) + timedelta(minutes=block_minutes)
    current_start  = floor_to_block(now, block_minutes)

    missing      = []
    window_start = last_block_end
    while window_start < current_start:
        window_end = window_start + timedelta(minutes=block_minutes)
        missing.append((window_start, window_end))
        window_start = window_end

    if missing:
        logger.warning(
            "detect_gap: %d missing blocks from %s to %s",
            len(missing), iso(last_block_end), iso(current_start),
        )
    return missing


def extract_last_reads(block: dict):
    """
    Extract the last known read and rate from each channel of a block dict.
    Returns (reads, rates) where both are {meter: {channel: {"ts": ..., "value": ...}}}.
    Both must be dicts-of-dicts so save_current_block can call r.get("ts")/r.get("value"),
    and so detect_gap gets a valid timestamp for pre_ts (gap fill anchor).

    Handles two block shapes:
    - Live current_block: channels have "reads" list of {"ts", "value"} dicts
    - Finalised DB block (_row_to_block): channels have "read_end" float + block "end" timestamp
    """
    from datetime import datetime, timezone as _tz
    _now_iso  = datetime.now(_tz.utc).isoformat()
    # Use block end time as timestamp anchor for finalised DB blocks
    block_end_iso = block.get("end") or _now_iso
    reads = {}
    rates = {}
    for meter_name, meter_data in block.get("meters", {}).items():
        reads[meter_name] = {}
        rates[meter_name] = {}
        for channel_name, channel in meter_data.get("channels", {}).items():
            channel_reads = channel.get("reads", [])
            channel_rates = channel.get("rates", [])

            if channel_reads:
                last_read = channel_reads[-1]
                if isinstance(last_read, dict):
                    reads[meter_name][channel_name] = last_read
                else:
                    reads[meter_name][channel_name] = {"ts": block_end_iso, "value": float(last_read)}
            elif channel.get("read_end") is not None:
                # Finalised DB block — read_end is the last sensor value, block end is the timestamp
                reads[meter_name][channel_name] = {
                    "ts":    block_end_iso,
                    "value": float(channel["read_end"]),
                }

            if channel_rates:
                last_rate = channel_rates[-1]
                if isinstance(last_rate, dict):
                    rates[meter_name][channel_name] = {
                        "ts":    last_rate.get("ts", block_end_iso),
                        "value": float(last_rate.get("value", 0)),
                    }
                else:
                    rates[meter_name][channel_name] = {"ts": block_end_iso, "value": float(last_rate)}
            else:
                # Fallback: rate stored directly on channel (finalised DB block)
                rate = channel.get("rate") or channel.get("rate_used")
                if rate:
                    rates[meter_name][channel_name] = {"ts": block_end_iso, "value": float(rate)}
    return reads, rates


# ─────────────────────────────────────────────────────────────────────────────
# Gap marker helpers
# ─────────────────────────────────────────────────────────────────────────────

def _rate_value(rate_entry) -> float:
    """Unwrap a rate entry that may be a float or a {"ts":..., "value":...} dict."""
    if isinstance(rate_entry, dict):
        return float(rate_entry.get("value", 0.0))
    return float(rate_entry or 0.0)


def set_gap_marker(block: dict, pre_reads: dict, last_known_rates: dict,
                   last_block_start: str | None = None):
    block["_gap_marker"] = {
        "detected_at":      datetime.now(timezone.utc).replace(tzinfo=None).isoformat(),
        "pre_reads":        pre_reads,
        "last_known_rates": last_known_rates,
        "last_block_start": last_block_start,
    }
    logger.info("set_gap_marker: stored at %s last_block_start=%s",
                block["_gap_marker"]["detected_at"], last_block_start)


def clear_gap_marker(block: dict):
    if "_gap_marker" in block:
        block.pop("_gap_marker")
        logger.info("clear_gap_marker: cleared")


def has_gap_marker(block: dict) -> bool:
    return "_gap_marker" in block


# ─────────────────────────────────────────────────────────────────────────────
# Sensor reading  (replaces PyScript state.get())
# ─────────────────────────────────────────────────────────────────────────────

def read_sensor(ha: HAClient, entity_id: str, use_cache: bool = True) -> float | None:
    try:
        val = ha.get_state(entity_id)
        if val in ("unknown", "unavailable", None):
            if use_cache and entity_id in _last_known_sensor_values:
                cached = _last_known_sensor_values[entity_id]
                logger.warning("read_sensor: %s='%s', using cached %s", entity_id, val, cached)
                return cached
            logger.warning("read_sensor: %s='%s', no cache", entity_id, val)
            return None
        val = float(val)
        _last_known_sensor_values[entity_id] = val
        return val
    except (ValueError, TypeError) as e:
        logger.warning("read_sensor: cannot cast %s to float — %s", entity_id, e)
        return None


# ─────────────────────────────────────────────────────────────────────────────
# Block rollover
# ─────────────────────────────────────────────────────────────────────────────

def ensure_correct_block(ha: HAClient, current_block: dict, now: datetime,
                         last_known_rates: dict | None = None) -> dict:
    start, end = get_block_window(now, block_minutes=int(get_block_minutes()))

    # Reload from store — a concurrent tick (e.g. gap fill) may have saved
    # a more recent current_block since the caller loaded theirs.
    fresh = _store.load_current_block()
    if fresh and fresh.get("start"):
        fresh_start = datetime.fromisoformat(fresh["start"])
        if fresh_start != datetime.fromisoformat(current_block.get("start", "")):
            logger.info("ensure_correct_block: current_block updated by concurrent tick (%s → %s), using fresh",
                        current_block.get("start"), fresh["start"])
            current_block = fresh

    if not current_block or not current_block.get("start"):
        # Don't create the first block until sensors are configured.
        # If no import sensor is set we're in the pre-wizard state —
        # the block_minutes value is the default (30) which may not match
        # what the wizard will set. Creating a block now would cause a
        # block-size mismatch and racing after wizard saves.
        cfg = load_config()
        has_sensors = any(
            (meter.get("channels") or {}).get("import", {}).get("read")
            for meter in (cfg.get("meters") or {}).values()
            if not (meter.get("meta") or {}).get("sub_meter")
        )
        if not has_sensors:
            return current_block  # return None/empty — no block yet
        logger.info("Creating first block %s", iso(start))
        return create_block(start, end, block_minutes=int(get_block_minutes()))

    existing_start = datetime.fromisoformat(current_block["start"])
    if existing_start == start:
        return current_block

    logger.info("Block rollover: %s → %s", current_block["start"], iso(start))

    # Wait for at least one post-boundary read before finalising
    boundary_iso           = iso(start)
    has_post_boundary_read = False
    for meter_data in current_block.get("meters", {}).values():
        for channel in meter_data.get("channels", {}).values():
            for read in channel.get("reads", []):
                if read["ts"] >= boundary_iso:
                    has_post_boundary_read = True
                    break

    if not has_post_boundary_read:
        seconds_since_boundary = (now - start).total_seconds()
        if seconds_since_boundary < 120:
            logger.info("ensure_correct_block: waiting for post-boundary read")
            return current_block
        logger.warning(
            "ensure_correct_block: no post-boundary read after %.0fs, finalising anyway",
            seconds_since_boundary
        )

    # Gap detection before finalise
    last_read_ts = None
    for meter_data in current_block.get("meters", {}).values():
        for channel in meter_data.get("channels", {}).values():
            reads = channel.get("reads", [])
            if reads:
                ts = reads[-1]["ts"]
                if not last_read_ts or ts > last_read_ts:
                    last_read_ts = ts

    _ecb_bm = int(get_block_minutes())
    missing_windows = detect_gap(last_read_ts, now, block_minutes=_ecb_bm)
    if missing_windows:
        logger.warning(
            "ensure_correct_block: %d missing blocks, setting gap marker", len(missing_windows)
        )
        pre_reads, last_rates = extract_last_reads(current_block)
        set_gap_marker(current_block, pre_reads, last_rates)

    # Skip chart regeneration during catch-up rollovers (more than one block
    # behind now). Charts are expensive; we only need them current, not for
    # every historical block we roll through after a gap or restore.
    catching_up = (start - existing_start).total_seconds() > int(get_block_minutes()) * 60
    finalise_block(ha, block_data=current_block, interpolated=catching_up,
                   last_known_rates=last_known_rates)

    new_block = _store.load_current_block()
    if not new_block or not new_block.get("start"):
        logger.warning("ensure_correct_block: pruned buffer missing, creating fresh")
        return create_block(start, end, block_minutes=int(get_block_minutes()))
    return new_block


# ─────────────────────────────────────────────────────────────────────────────
# Sample capture
# ─────────────────────────────────────────────────────────────────────────────

def capture_samples(ha: HAClient, block: dict, now: datetime):
    now_iso = iso(now)
    config  = load_config()

    if not config:
        logger.error("capture_samples: meters_config missing")
        return

    # Ensure block has meters key — may be missing if block was reset to {}
    if "meters" not in block:
        block["meters"] = {}

    today_iso = now.strftime("%Y-%m-%d")
    for meter_id, meter_cfg in config.get("meters", {}).items():
        # Skip retired sub-meters — don't record any reads after retirement date
        meta = meter_cfg.get("meta", {})
        if meta.get("sub_meter") and meta.get("retired_at"):
            if meta["retired_at"] <= today_iso:
                continue

        meter_block = block["meters"].setdefault(
            meter_id, {"meta": {}, "channels": {}, "interpolated": False}
        )
        meter_block["meta"] = meta

        for channel_id, channel_cfg in meter_cfg.get("channels", {}).items():
            channel_block = meter_block["channels"].setdefault(
                channel_id, {"reads": [], "rates": []}
            )

            read_id = channel_cfg.get("read")
            if read_id:
                read_val = read_sensor(ha, read_id)
                if read_val is not None:
                    channel_block["reads"].append({"value": read_val, "ts": now_iso})

            rate_id = channel_cfg.get("rate")
            if rate_id:
                rate_val = read_sensor(ha, rate_id)
                if rate_val is not None:
                    channel_block["rates"].append({"value": rate_val, "ts": now_iso})


# ─────────────────────────────────────────────────────────────────────────────
# Compute kWh / cost
# ─────────────────────────────────────────────────────────────────────────────

def compute_channel(channel: dict, parent_rates=None, is_sub_meter: bool = False) -> dict:
    reads     = channel.get("reads", [])
    rates     = channel.get("rates", [])
    if not rates and parent_rates:
        rates = parent_rates
    if not rates:
        logger.warning("compute_channel: no rate data, defaulting to 0.0")
    last_rate = rates[-1]["value"] if rates else 0.0

    if len(reads) < 2:
        return {
            "kwh":        0.0,
            "rate":       last_rate,
            "cost":       0.0,
            "read_start": reads[0]["value"] if reads else 0.0,
            "read_end":   reads[-1]["value"] if reads else 0.0,
        }

    # ── Main meter ────────────────────────────────────────────────────────
    if not is_sub_meter:
        raw_delta = reads[-1]["value"] - reads[0]["value"]
        total_kwh = max(raw_delta, 0.0)
        return {
            "kwh":        total_kwh,
            "rate":       last_rate,
            "cost":       total_kwh * last_rate,
            "read_start": reads[0]["value"],
            "read_end":   reads[-1]["value"],
        }

    # ── Sub meter — backward tariff reconstruction ────────────────────────
    corrected_rates = []
    if rates:
        current_rate = rates[-1]["value"]
        corrected_rates.append({"ts": rates[-1]["ts"], "value": current_rate})
        for r in reversed(rates[:-1]):
            if r["value"] < current_rate:
                current_rate = r["value"]
            corrected_rates.append({"ts": r["ts"], "value": current_rate})
        corrected_rates.reverse()
    else:
        corrected_rates = [{"ts": reads[0]["ts"], "value": last_rate}]

    total_kwh  = 0.0
    total_cost = 0.0
    rate_index = 0
    current_rate = corrected_rates[0]["value"]

    for i in range(1, len(reads)):
        prev_read = reads[i - 1]
        curr_read = reads[i]
        delta     = curr_read["value"] - prev_read["value"]
        if delta < 0:
            continue
        while (
            rate_index + 1 < len(corrected_rates)
            and corrected_rates[rate_index + 1]["ts"] <= curr_read["ts"]
        ):
            rate_index  += 1
            current_rate = corrected_rates[rate_index]["value"]
        total_kwh  += delta
        total_cost += delta * current_rate

    # Use the last rate in the block — consistent with main meter behaviour.
    # This captures the rate as close to the end of the block as possible,
    # accounting for any API lag from remote rate providers.
    display_rate = corrected_rates[-1]["value"]
    return {
        "kwh":        total_kwh,
        "rate":       display_rate,
        "cost":       total_cost,
        "read_start": reads[0]["value"],
        "read_end":   reads[-1]["value"],
    }


def select_opening_read(reads: list, boundary_dt: datetime) -> dict | None:
    boundary_iso = boundary_dt.isoformat()
    pre = [r for r in reads if r["ts"] <= boundary_iso]
    if pre:
        return pre[-1]
    post = [r for r in reads if r["ts"] > boundary_iso]
    return post[0] if post else None


def select_closing_read(reads: list, boundary_dt: datetime) -> dict | None:
    boundary_iso = boundary_dt.isoformat()
    post = [r for r in reads if r["ts"] >= boundary_iso]
    if post:
        return post[0]
    pre = [r for r in reads if r["ts"] < boundary_iso]
    return pre[-1] if pre else None


# ─────────────────────────────────────────────────────────────────────────────
# Gap block builder
# ─────────────────────────────────────────────────────────────────────────────

def build_gap_blocks(
    missing_windows:       list,
    pre_reads_by_channel:  dict,
    post_reads_by_channel: dict,
    last_known_rates:      dict,
    config:                dict,
    last_standing_charge:  float = 0.0,
) -> list:
    gap_blocks = []

    cfg_bm = int((next(iter(config.get("meters", {}).values()), {}).get("meta") or {}).get("block_minutes") or BLOCK_MINUTES)
    for window_start, window_end in missing_windows:
        block = {
            "start":         iso(window_start),
            "end":           iso(window_end),
            "block_minutes": cfg_bm,
            "meters": {},
            "totals": {
                "import_kwh": 0.0, "import_cost": 0.0,
                "export_kwh": 0.0, "export_cost": 0.0,
            },
            "interpolated": True,
        }

        for meter_name, meter_cfg in config.get("meters", {}).items():
            meter_meta  = meter_cfg.get("meta", {})
            is_sub      = meter_meta.get("sub_meter", False)

            # Skip retired sub-meters in gap-fill blocks
            if is_sub and meter_meta.get("retired_at"):
                window_date = window_start.strftime("%Y-%m-%d")
                if meter_meta["retired_at"] <= window_date:
                    continue

            meter_block = {
                "channels": {}, "meta": meter_meta,
                "interpolated": True, "standing_charge": last_standing_charge,
            }

            for channel_name in meter_cfg.get("channels", {}).keys():

                if is_sub:
                    pre_read  = pre_reads_by_channel.get(meter_name, {}).get(channel_name)
                    post_read = post_reads_by_channel.get(meter_name, {}).get(channel_name)
                    sub_kwh = sub_rate = sub_cost = sub_start = sub_end = 0.0
                    skip_reason = None

                    if not pre_read or not post_read:
                        skip_reason = "missing reads"
                    else:
                        pre_ts    = datetime.fromisoformat(pre_read["ts"])
                        post_ts   = datetime.fromisoformat(post_read["ts"])
                        gap_hours = (post_ts - pre_ts).total_seconds() / 3600
                        if gap_hours > 12:
                            skip_reason = f"gap too large ({gap_hours:.1f}hrs)"
                        elif post_read["value"] <= pre_read["value"]:
                            # Reset detected — use post_read value directly as kWh
                            # accumulated since the reset (handles daily-reset sensors
                            # and any other cumulative sensor that resets mid-block).
                            sub_kwh = max(round(post_read["value"], 6), 0.0)
                            logger.info(
                                "build_gap_blocks: %s/%s reset detected (%.4f → %.4f) "
                                "using post-reset value %.4f kWh",
                                meter_name, channel_name,
                                pre_read["value"], post_read["value"], sub_kwh
                            )
                        else:
                            opener   = interpolate_value(pre_read, post_read, window_start)
                            closer   = interpolate_value(pre_read, post_read, window_end)
                            sub_kwh  = max(round(closer["value"] - opener["value"], 6), 0.0)
                            _sr = last_known_rates.get(meter_name, {}).get(channel_name)
                            if _sr is None:
                                parent_name = meter_meta.get("parent_meter")
                                _sr = last_known_rates.get(parent_name, {}).get(channel_name)
                            sub_rate = _rate_value(_sr)
                            if not sub_rate:
                                logger.info("build_gap_blocks: %s/%s using parent rate %.4f", meter_name, channel_name, sub_rate)
                            sub_cost  = round(sub_kwh * sub_rate, 6)
                            sub_start = opener["value"]
                            sub_end   = closer["value"]

                    if skip_reason:
                        logger.warning("build_gap_blocks: %s/%s zero — %s", meter_name, channel_name, skip_reason)
                        # Still look up the last known rate so the chart rate line
                        # doesn't spike to zero on restarts
                        _sr = last_known_rates.get(meter_name, {}).get(channel_name)
                        if _sr is None:
                            parent_name = meter_meta.get("parent_meter")
                            _sr = last_known_rates.get(parent_name, {}).get(channel_name)
                        sub_rate = _rate_value(_sr)
                    else:
                        # Spike detection — sub-meter kwh cannot exceed parent grid import
                        # for this window. Get parent import kwh for sanity check.
                        parent_name   = meter_meta.get("parent_meter", "electricity_main")
                        parent_pre    = pre_reads_by_channel.get(parent_name, {}).get(channel_name)
                        parent_post   = post_reads_by_channel.get(parent_name, {}).get(channel_name)
                        if parent_pre and parent_post and parent_post["value"] > parent_pre["value"]:
                            p_opener    = interpolate_value(parent_pre, parent_post, window_start)
                            p_closer    = interpolate_value(parent_pre, parent_post, window_end)
                            parent_kwh  = max(round(p_closer["value"] - p_opener["value"], 6), 0.0)
                            if sub_kwh > parent_kwh * 1.05:  # 5% tolerance for rounding
                                logger.warning(
                                    "build_gap_blocks: %s/%s %.4f kWh EXCEEDS parent grid %.4f kWh "
                                    "— may be gap attribution issue or misconfigured sensor. "
                                    "Recording as-is.",
                                    meter_name, channel_name, sub_kwh, parent_kwh,
                                )

                    meter_block["channels"][channel_name] = {
                        "kwh": sub_kwh, "rate": sub_rate, "cost": sub_cost,
                        "read_start": sub_start, "read_end": sub_end, "interpolated": True,
                    }
                    continue

                # ── Main meter ────────────────────────────────────────────
                pre_read  = pre_reads_by_channel.get(meter_name, {}).get(channel_name)
                post_read = post_reads_by_channel.get(meter_name, {}).get(channel_name)

                if not pre_read or not post_read:
                    logger.warning("build_gap_blocks: missing reads for %s/%s", meter_name, channel_name)
                    _fallback_rate = _rate_value(last_known_rates.get(meter_name, {}).get(channel_name, 0.0))
                    meter_block["channels"][channel_name] = {
                        "kwh": 0.0, "rate": _fallback_rate, "cost": 0.0,
                        "read_start": 0.0, "read_end": 0.0, "interpolated": True,
                    }
                    continue

                opener = interpolate_value(pre_read, post_read, window_start)
                # For the last gap window, anchor read_end to the actual post_read
                # value rather than an interpolated value. An interpolated closer can
                # exceed the next real block's read_start, causing the same register
                # space to be counted in both the gap block and the real block.
                _is_last_window = ((window_start, window_end) == missing_windows[-1])
                if _is_last_window:
                    closer = {"value": post_read["value"], "ts": post_read["ts"]}
                else:
                    closer = interpolate_value(pre_read, post_read, window_end)
                kwh    = max(round(closer["value"] - opener["value"], 6), 0.0)
                rate   = _rate_value(last_known_rates.get(meter_name, {}).get(channel_name, 0.0))
                cost   = round(kwh * rate, 6)

                meter_block["channels"][channel_name] = {
                    "kwh": kwh, "rate": rate, "cost": cost,
                    "read_start": opener["value"], "read_end": closer["value"],
                    "interpolated": True,
                }

                if channel_name == "import":
                    block["totals"]["import_kwh"]  += kwh
                    block["totals"]["import_cost"] += cost
                elif channel_name == "export":
                    block["totals"]["export_kwh"]  += kwh
                    block["totals"]["export_cost"] += cost

            block["meters"][meter_name] = meter_block

        # Apply grid-authoritative clipping to gap blocks (same as finalise_block PASS 2)
        _apply_pass2(block)

        gap_blocks.append(block)
        logger.info(
            "build_gap_blocks: %s → %s  import=%.4f kWh  export=%.4f kWh",
            iso(window_start), iso(window_end),
            block["totals"]["import_kwh"], block["totals"]["export_kwh"],
        )

    return gap_blocks


# ─────────────────────────────────────────────────────────────────────────────
# HA sensor update helper  (replaces repeated state.set() blocks)
# ─────────────────────────────────────────────────────────────────────────────

async def update_ha_sensors(ha: HAClient, engine_totals: dict):
    """Push cumulative totals to four synthetic HA sensors."""
    await ha.set_state(
        "sensor.energy_meter_import_kwh",
        round(engine_totals["import_kwh"], 6),
        {
            "unit_of_measurement": "kWh",
            "device_class":        "energy",
            "state_class":         "total_increasing",
            "friendly_name":       "Energy Engine Import",
        },
    )
    await ha.set_state(
        "sensor.energy_meter_export_kwh",
        round(engine_totals["export_kwh"], 6),
        {
            "unit_of_measurement": "kWh",
            "device_class":        "energy",
            "state_class":         "total_increasing",
            "friendly_name":       "Energy Engine Export",
        },
    )
    config          = load_config()
    main_meta       = {}
    for meter_data in config.get("meters", {}).values():
        if not (meter_data.get("meta") or {}).get("sub_meter"):
            main_meta = meter_data.get("meta") or {}
            break
    currency_code = main_meta.get("currency_code", "GBP")

    await ha.set_state(
        "sensor.energy_meter_import_cost",
        round(engine_totals["import_cost"], 6),
        {
            "unit_of_measurement": currency_code,
            "device_class":        "monetary",
            "state_class":         "total_increasing",
            "friendly_name":       "Energy Engine Import Cost",
        },
    )
    await ha.set_state(
        "sensor.energy_meter_export_credit",
        round(engine_totals["export_cost"], 6),
        {
            "unit_of_measurement": currency_code,
            "device_class":        "monetary",
            "state_class":         "total_increasing",
            "friendly_name":       "Energy Engine Export Credit",
        },
    )


# ─────────────────────────────────────────────────────────────────────────────
# Chart generation helper
# ─────────────────────────────────────────────────────────────────────────────

def generate_charts(store: "BlockStore"):
    """
    Generate all charts from the BlockStore.
    Queries only the data each chart needs rather than loading all blocks.
    """
    if store.count_blocks() == 0:
        logger.info("generate_charts: no blocks, skipping")
        return
    config        = load_config()
    main_meta     = {}
    for meter_data in config.get("meters", {}).values():
        if not (meter_data.get("meta") or {}).get("sub_meter"):
            main_meta = meter_data.get("meta") or {}
            break
    timezone_name   = main_meta.get("timezone", "UTC")
    block_minutes   = int(main_meta.get("block_minutes") or 30)
    currency_symbol = main_meta.get("currency_symbol", "£")

    # Use lightweight block fetch — significantly faster than get_all_blocks()
    # for large datasets as it skips full _rows_to_blocks reconstruction.
    blocks = store.get_blocks_lightweight()

    try:
        html = energy_charts.generate_net_heatmap(blocks, timezone_name=timezone_name, block_minutes=block_minutes, currency=currency_symbol)
        io_save_file(f"{CHART_DIR}/net_heatmap.html", html)
        logger.info("generate_charts: net heatmap written (tz=%s, bm=%s, currency=%s)", timezone_name, block_minutes, currency_symbol)
    except Exception as e:
        logger.error("generate_charts: heatmap error: %s", e)
    try:
        html = energy_charts.generate_daily_import_export_charts(blocks, timezone_name=timezone_name, block_minutes=block_minutes, currency=currency_symbol, cfg=config)
        io_save_file(f"{CHART_DIR}/daily_usage.html", html)
        logger.info("generate_charts: daily usage chart written (tz=%s, bm=%s, currency=%s)", timezone_name, block_minutes, currency_symbol)
    except Exception as e:
        logger.error("generate_charts: daily chart error: %s", e)


# ─────────────────────────────────────────────────────────────────────────────
# Block finalise
# ─────────────────────────────────────────────────────────────────────────────

def _apply_pass2(block: dict) -> None:
    """
    PASS 2 — grid-authoritative sub-meter distribution.
    Clips each sub-meter's import to what the parent grid import can supply,
    sets kwh_grid and kwh_battery on each sub-meter channel.
    Called from both finalise_block and build_gap_blocks.
    """
    # Build parent → sub_kwh_total map
    parent_sub_kwh: dict = {}
    for meter_name, meter_block in block["meters"].items():
        meta = meter_block.get("meta", {})
        if not meta.get("sub_meter"):
            continue
        parent_name = meta.get("parent_meter")
        if not parent_name:
            continue
        sub_import = meter_block["channels"].get("import")
        if sub_import:
            parent_sub_kwh[parent_name] = parent_sub_kwh.get(parent_name, 0.0) + sub_import.get("kwh", 0.0)

    for parent_meter_name in parent_sub_kwh:
        parent_block  = block["meters"].get(parent_meter_name)
        if not parent_block:
            continue
        parent_import = parent_block["channels"].get("import")
        if not parent_import:
            continue

        grid_kwh       = parent_import.get("kwh", 0.0)
        parent_rate    = parent_import.get("rate", 0.0)
        grid_remaining = grid_kwh

        protected   = []
        unprotected = []

        for meter_name, meter_block in block["meters"].items():
            meta = meter_block.get("meta", {})
            if not meta.get("sub_meter") or meta.get("parent_meter") != parent_meter_name:
                continue
            sub_import = meter_block["channels"].get("import")
            if not sub_import:
                continue
            delta = sub_import.get("kwh", 0.0)
            if delta < 0:
                # Reset detected mid-block — read_end is the kWh accumulated since
                # the reset. Use it directly rather than skipping the block.
                delta = sub_import.get("read_end", 0.0)
                logger.info(
                    "PASS 2: %s reset detected, using post-reset read_end %.4f kWh",
                    meter_name, delta
                )
                if delta <= 0.0:
                    continue
                sub_import["kwh"] = delta
            if delta == 0.0:
                continue
            entry = {
                "meter_name": meter_name, "meter_block": meter_block,
                "sub_import": sub_import, "kwh": delta,
            }
            protected.append(entry)  # all sub-meters are protected (inverter_possible removed)

        protected.sort(key=lambda x: x["kwh"], reverse=True)
        unprotected.sort(key=lambda x: x["kwh"], reverse=True)

        is_interpolated = block.get("interpolated", False)
        for entry in protected:
            claimed = min(entry["kwh"], grid_remaining)
            if entry["kwh"] > grid_kwh:
                if is_interpolated:
                    # Gap-fill block — preserve energy attribution even if it exceeds
                    # grid import (gap attribution issues are expected)
                    logger.warning(
                        "PASS 2: %s sub-meter %.4f kWh EXCEEDS parent grid import %.4f kWh — "
                        "gap block attribution issue, recording as-is.",
                        entry["meter_name"], entry["kwh"], grid_kwh,
                    )
                    claimed = entry["kwh"]
                else:
                    # Live block — clip to grid import (sub-meter cannot exceed grid)
                    logger.warning(
                        "PASS 2: %s sub-meter %.4f kWh EXCEEDS parent grid import %.4f kWh — "
                        "clipping to grid import (live block).",
                        entry["meter_name"], entry["kwh"], grid_kwh,
                    )
                    claimed = grid_remaining  # already set to min above
            elif claimed < entry["kwh"]:
                logger.warning(
                    "PASS 2: %s protected load %.4f kWh clipped to %.4f kWh",
                    entry["meter_name"], entry["kwh"], claimed,
                )
            grid_remaining = max(grid_remaining - claimed, 0.0)
            entry["sub_import"]["kwh_grid"]    = claimed
            entry["sub_import"]["kwh_battery"] = entry["kwh"] - claimed
            entry["sub_import"]["cost"]        = round(claimed * parent_rate, 6)
            logger.info(
                "PASS 2: %s protected  grid=%.4f  battery=%.4f",
                entry["meter_name"], claimed, entry["sub_import"]["kwh_battery"],
            )

        for entry in unprotected:
            claimed    = min(entry["kwh"], grid_remaining)
            battery    = entry["kwh"] - claimed
            if entry["kwh"] > grid_kwh:
                if is_interpolated:
                    logger.warning(
                        "PASS 2: %s sub-meter %.4f kWh EXCEEDS parent grid import %.4f kWh — "
                        "gap block attribution issue, recording as-is.",
                        entry["meter_name"], entry["kwh"], grid_kwh,
                    )
                    claimed = entry["kwh"]
                    battery = 0.0
                else:
                    logger.warning(
                        "PASS 2: %s sub-meter %.4f kWh EXCEEDS parent grid import %.4f kWh — "
                        "clipping to grid import (live block).",
                        entry["meter_name"], entry["kwh"], grid_kwh,
                    )
                    # claimed already = min(kwh, grid_remaining) from above
                    battery = entry["kwh"] - claimed
            grid_remaining = max(grid_remaining - claimed, 0.0)
            entry["sub_import"]["kwh_grid"]    = claimed
            entry["sub_import"]["kwh_battery"] = battery
            entry["sub_import"]["cost"]        = round(claimed * parent_rate, 6)
            logger.info(
                "PASS 2: %s unprotected  grid=%.4f  battery=%.4f",
                entry["meter_name"], claimed, battery,
            )

        remainder_kwh  = max(grid_remaining, 0.0)

        # cost_remainder is computed as remainder_kwh × rate, NOT as main_cost − sub_costs.
        #
        # Why: both sub_import["cost"] (line above) and cost_remainder are derived from
        # kwh × rate. This is intentionally consistent — both sides of the identity use
        # the same arithmetic path.
        #
        # Why not main_cost − sub_costs: main_cost comes from actual meter register reads
        # (a cumulative kWh delta interpolated to block boundaries), while sub-meter costs
        # come from power sensor integration (kW polled at ~10s intervals). These are two
        # independent measurement systems with different sensor resolutions and timing.
        # Subtracting one from the other would silently absorb all sensor disagreement
        # into cost_remainder, potentially making it negative on blocks where the sub-meter
        # sensor over-reported relative to the main meter.
        #
        # The resulting ~0.01p/block discrepancy between cost_remainder + sub_costs and
        # main_cost is an inherent measurement artefact of combining two sensor systems.
        # Presentation layers (api_blocks_summary, billing charts) use rate-based
        # subtraction (main_cost[rate] − sub_cost[rate]) for display, which avoids
        # accumulating this artefact across periods. See block_store._aggregate_usage
        # and web/server.py api_blocks_summary for the display-layer approach.
        remainder_cost = round(remainder_kwh * parent_rate, 6)

        parent_import["kwh_total"]      = grid_kwh
        parent_import["kwh_remainder"]  = remainder_kwh
        parent_import["cost_remainder"] = remainder_cost
        parent_import["rate_used"]      = parent_rate
        logger.info(
            "PASS 2: %s  grid=%.4f kWh  remainder=%.4f kWh",
            parent_meter_name, grid_kwh, remainder_kwh,
        )


# ─────────────────────────────────────────────────────────────────────────────
# 2.10.0 — Sub-meter boundary interpolation amendment
# ─────────────────────────────────────────────────────────────────────────────

# Maximum median inter-read interval (seconds) for a sub-meter to be eligible
# for boundary interpolation.  Devices publishing slower than this are too
# coarse — the linear interpolation assumption breaks down and the provisional
# figure (last pre-boundary read) is the best available estimate.
# 90 s gives 60 s devices a comfortable jitter margin.
_PROVISIONAL_MAX_INTERVAL_S: float = 90.0

# A post-boundary read is only used for interpolation if it arrives within
# this multiple of the observed device interval after the boundary.  Catches
# the stop-then-restart case: if the device genuinely stopped before the
# boundary and restarted 15 min later, the gap is >> 2× interval and we
# correctly leave the provisional figure unchanged.
_PROVISIONAL_GAP_MULTIPLIER: float = 2.0

# Minimum number of inter-read gaps required to compute a reliable median
# device interval.  3 gaps (4 reads) gives a median robust against one
# outlier on a stable periodic signal — sufficient regardless of block size.
# Larger blocks naturally provide more gaps but the minimum stays flat.
_PROVISIONAL_MIN_GAPS: int = 3


def _observed_device_interval_s(reads: list) -> float | None:
    """Return the median inter-read gap in seconds from a list of read dicts.

    Returns None if there are fewer than (_PROVISIONAL_MIN_GAPS + 1) reads
    (i.e. fewer than 4 reads / 3 gaps).  3 gaps is the minimum needed for a
    median robust against one outlier on a stable periodic signal, and holds
    across all block sizes — a 5-minute block with a 60s device still yields
    ~4 reads, which is sufficient.
    Each read dict must have a 'ts' key containing an ISO timestamp string.
    """
    if len(reads) < _PROVISIONAL_MIN_GAPS + 1:
        return None
    sorted_reads = sorted(reads, key=lambda r: r["ts"])
    gaps = []
    for i in range(1, len(sorted_reads)):
        try:
            t0 = datetime.fromisoformat(sorted_reads[i - 1]["ts"])
            t1 = datetime.fromisoformat(sorted_reads[i]["ts"])
            gap_s = (t1 - t0).total_seconds()
            if gap_s > 0:
                gaps.append(gap_s)
        except (ValueError, KeyError):
            continue
    if len(gaps) < _PROVISIONAL_MIN_GAPS:
        return None
    gaps.sort()
    mid = len(gaps) // 2
    return gaps[mid] if len(gaps) % 2 else (gaps[mid - 1] + gaps[mid]) / 2.0


def _commit_provisional_as_final(ha: HAClient, full_block: dict,
                                  meter_name: str, block_start_iso: str,
                                  block_end_iso: str) -> None:
    """Clear the provisional flag on *meter_name* in *full_block*, re-run PASS 2,
    write back to DB, and republish HA sensors.

    The kWh figure is left exactly as originally computed — this path is taken
    when interpolation is not appropriate (device too slow, gap too large,
    gap-fill in progress, insufficient reads).  PASS 2 is re-run for
    correctness but produces the same result as the original write.
    """
    full_block["meters"][meter_name].pop("provisional", None)
    _apply_pass2(full_block)
    try:
        append_block_replace(full_block)
        logger.info(
            "_amend_provisional: block %s → %s committed as-final for %s "
            "(no interpolation applied)",
            block_start_iso, block_end_iso, meter_name,
        )
    except Exception as _we:
        logger.error(
            "_amend_provisional: DB write failed committing as-final for %s: %s",
            meter_name, _we,
        )
        return
    try:
        engine_totals = _store.get_cumulative_totals()
        loop = asyncio.get_event_loop()
        if loop.is_running():
            loop.create_task(_deferred_sensor_update(ha, engine_totals))
    except Exception as _se:
        logger.warning("_amend_provisional: sensor republish failed: %s", _se)


def _amend_provisional_sub_meter_blocks(ha: HAClient, current_block: dict) -> None:
    """Retrospectively correct provisional sub-meter blocks.

    Called each engine tick after reads are captured, but only when no gap
    marker is active (gap-seed reads must not be used as interpolation anchors).

    For each sub-meter whose previous block was written as provisional (no
    post-boundary read was available at close time), we:

      1. Check the device's observed publishing interval from the provisional
         block's own reads (≥ 6 reads required; median ≤ 90 s required).
      2. Check whether the first post-boundary read in the current rolling
         buffer arrived within 2 × that interval of the boundary.
      3. If both checks pass → interpolate to the boundary and amend the block.
      4. If either check fails → commit the provisional figure as-final without
         changing the kWh (the last pre-boundary read is the best estimate).

    Amendment is strictly non-cascading — only the immediately-previous
    block is touched.  Subsequent blocks already have their own reads and
    are unaffected.

    Gap-fill safety: the caller (_engine_tick) must not call this function
    when a gap marker is active.  Gap-seed reads look identical to real reads
    in the rolling buffer dict and must never be used as interpolation anchors.
    """
    provisional_blocks = _store.get_provisional_sub_meter_blocks()
    if not provisional_blocks:
        return

    for prov_block in provisional_blocks:
        block_start_iso = prov_block.get("start", "")
        block_end_iso   = prov_block.get("end", "")
        if not block_start_iso or not block_end_iso:
            continue

        block_end_dt = datetime.fromisoformat(block_end_iso)
        boundary_iso = block_end_iso  # boundary == block end

        for meter_name, meter_block in prov_block.get("meters", {}).items():
            if not meter_block.get("provisional"):
                continue
            meta = meter_block.get("meta", {})
            if not meta.get("sub_meter"):
                continue

            old_imp_ch = meter_block.get("channels", {}).get("import", {})
            old_rate   = old_imp_ch.get("rate", 0.0) or 0.0
            opener_val = old_imp_ch.get("read_start", 0.0)

            # ── Load full block from DB for PASS 2 ────────────────────────
            full_block = _store.get_last_block_before(block_end_iso)
            if not full_block or full_block.get("start") != block_start_iso:
                try:
                    blocks_here = _store.get_blocks_for_range(
                        datetime.fromisoformat(block_start_iso),
                        datetime.fromisoformat(block_end_iso),
                    )
                    full_block = blocks_here[-1] if blocks_here else None
                except Exception as _e:
                    logger.warning(
                        "_amend_provisional: block reload failed for %s: %s",
                        meter_name, _e,
                    )
                    full_block = None

            if not full_block or full_block.get("start") != block_start_iso:
                logger.warning(
                    "_amend_provisional: could not reload block %s from DB; "
                    "skipping %s",
                    block_start_iso, meter_name,
                )
                continue

            # ── Gate 1: device interval from provisional block's own reads ─
            # The provisional block's reads are in the prov_block meter_block,
            # but those reads are not stored to the DB — they live in the
            # current_reads table while the block is live and are discarded
            # after finalise.  We infer the interval from the rolling buffer's
            # pre-boundary reads, which include the full set of reads carried
            # into the current block as seeds plus any new reads.
            cb_meter = (current_block.get("meters") or {}).get(meter_name, {})
            cb_reads  = (cb_meter.get("channels") or {}).get("import", {}).get("reads", [])
            pre_reads  = [r for r in cb_reads if r.get("ts", "") <  boundary_iso]
            post_reads = [r for r in cb_reads if r.get("ts", "") >= boundary_iso]

            if not post_reads:
                # No post-boundary read yet — nothing to do this tick.
                logger.debug(
                    "_amend_provisional: %s — no post-boundary read yet",
                    meter_name,
                )
                continue

            if not pre_reads:
                # Post-boundary read exists but no pre-boundary seed in buffer.
                # Commit as-final — cannot interpolate without an opener.
                logger.warning(
                    "_amend_provisional: %s — post-boundary read present but no "
                    "pre-boundary seed; committing provisional as-final",
                    meter_name,
                )
                _commit_provisional_as_final(
                    ha, full_block, meter_name, block_start_iso, block_end_iso,
                )
                continue

            last_pre   = sorted(pre_reads,  key=lambda r: r["ts"])[-1]
            first_post = sorted(post_reads, key=lambda r: r["ts"])[0]

            # Observed interval: use all pre-boundary reads available in the
            # buffer, which covers the provisional block window.
            median_interval = _observed_device_interval_s(pre_reads)

            if median_interval is None:
                logger.info(
                    "_amend_provisional: %s — insufficient reads (%d) to "
                    "characterise device interval; committing provisional as-final",
                    meter_name, len(pre_reads),
                )
                _commit_provisional_as_final(
                    ha, full_block, meter_name, block_start_iso, block_end_iso,
                )
                continue

            if median_interval > _PROVISIONAL_MAX_INTERVAL_S:
                logger.warning(
                    "_amend_provisional: %s — device interval %.1fs exceeds "
                    "%.0fs limit; device too coarse for boundary interpolation. "
                    "Committing provisional as-final",
                    meter_name, median_interval, _PROVISIONAL_MAX_INTERVAL_S,
                )
                _commit_provisional_as_final(
                    ha, full_block, meter_name, block_start_iso, block_end_iso,
                )
                continue

            # ── Gate 2: post-boundary read must be within 2 × interval ────
            try:
                gap_s = (
                    datetime.fromisoformat(first_post["ts"]) -
                    datetime.fromisoformat(last_pre["ts"])
                ).total_seconds()
            except (ValueError, KeyError):
                gap_s = float("inf")

            threshold_s = _PROVISIONAL_GAP_MULTIPLIER * median_interval
            if gap_s > threshold_s:
                logger.info(
                    "_amend_provisional: %s — post-boundary gap %.1fs exceeds "
                    "%.1fs (%.0f × %.1fs interval); device likely stopped before "
                    "boundary. Committing provisional as-final",
                    meter_name, gap_s, threshold_s,
                    _PROVISIONAL_GAP_MULTIPLIER, median_interval,
                )
                _commit_provisional_as_final(
                    ha, full_block, meter_name, block_start_iso, block_end_iso,
                )
                continue

            # ── Both gates passed — interpolate to boundary ────────────────
            if last_pre["ts"] == first_post["ts"]:
                boundary_val = last_pre["value"]
            else:
                interp = interpolate_value(last_pre, first_post, block_end_dt)
                boundary_val = interp["value"] if interp else last_pre["value"]

            corrected_kwh = max(boundary_val - opener_val, 0.0)
            amended_imp = {
                "kwh":        corrected_kwh,
                "rate":       old_rate,
                "cost":       round(corrected_kwh * old_rate, 6),
                "read_start": opener_val,
                "read_end":   boundary_val,
            }
            if "meta" in old_imp_ch:
                amended_imp["meta"] = old_imp_ch["meta"]

            logger.info(
                "_amend_provisional: %s  interpolated — "
                "old_kwh=%.4f  new_kwh=%.4f  boundary=%.4f  "
                "gap=%.1fs  interval=%.1fs",
                meter_name, old_imp_ch.get("kwh", 0.0), corrected_kwh,
                boundary_val, gap_s, median_interval,
            )

            full_block["meters"][meter_name]["channels"]["import"] = amended_imp
            full_block["meters"][meter_name].pop("provisional", None)
            _apply_pass2(full_block)

            try:
                append_block_replace(full_block)
                logger.info(
                    "_amend_provisional: block %s → %s amended for %s",
                    block_start_iso, block_end_iso, meter_name,
                )
            except Exception as _we:
                logger.error(
                    "_amend_provisional: DB write failed for %s: %s",
                    meter_name, _we,
                )
                continue

            try:
                engine_totals = _store.get_cumulative_totals()
                loop = asyncio.get_event_loop()
                if loop.is_running():
                    loop.create_task(_deferred_sensor_update(ha, engine_totals))
            except Exception as _se:
                logger.warning(
                    "_amend_provisional: sensor republish failed: %s", _se,
                )


def finalise_block(ha: HAClient, block_data: dict | None = None, interpolated: bool = False,
                   last_known_rates: dict | None = None):
    cb = block_data if block_data is not None else _store.load_current_block()

    if not cb or not cb.get("meters"):
        logger.warning("finalise_block: nothing to finalise")
        return

    start           = cb.get("start")
    end             = cb.get("end")
    block_start_dt  = datetime.fromisoformat(start)
    block_end_dt    = datetime.fromisoformat(end)
    boundary_iso    = block_end_dt.isoformat()

    block = {
        "start":  start,
        "end":    end,
        "meters": {},
        "totals": {
            "import_kwh": 0.0, "import_cost": 0.0,
            "export_kwh": 0.0, "export_cost": 0.0,
        },
        "interpolated": interpolated,
    }

    config           = load_json(CONFIG_PATH, {"meters": {}})
    parent_sub_kwh   = {}
    parent_sub_cost  = {}

    # ── PASS 1 — compute all meters with boundary interpolation ───────────
    for meter_name, meter_data in cb.get("meters", {}).items():
        meter_meta  = meter_data.get("meta", {})
        meter_block = {"channels": {}, "meta": meter_meta, "interpolated": interpolated}

        meter_cfg = config.get("meters", {}).get(meter_name)
        if not meter_cfg:
            logger.warning("finalise_block: no config for '%s', skipping", meter_name)
            continue

        import_channel_cfg     = meter_cfg.get("channels", {}).get("import", {})
        standing_charge_sensor = import_channel_cfg.get("standing_charge_sensor")
        raw_sc                 = read_sensor(ha, standing_charge_sensor) if standing_charge_sensor else 0.0
        if (raw_sc is None or raw_sc == 0.0) and standing_charge_sensor:
            # Sensor unavailable — fall back to last known value from DB
            try:
                _sc_row = _store._conn.execute(
                    """SELECT standing_charge FROM blocks
                       WHERE meter_id = ? AND standing_charge > 0
                       ORDER BY block_start DESC LIMIT 1""",
                    (meter_name,)
                ).fetchone()
                if _sc_row:
                    raw_sc = float(_sc_row["standing_charge"])
                    logger.debug("finalise_block: standing charge sensor unavailable, using last known value %.4f", raw_sc)
            except Exception:
                pass
        meter_block["standing_charge"] = raw_sc if raw_sc is not None else 0.0

        parent_name  = meter_meta.get("parent_meter")
        parent_meter = cb.get("meters", {}).get(parent_name, {})
        parent_rates = parent_meter.get("channels", {}).get("import", {}).get("rates", [])

        for channel_name, channel in meter_data.get("channels", {}).items():
            is_sub      = meter_meta.get("sub_meter", False)
            rates       = channel.get("rates", [])
            valid_rates = [r for r in rates if r["ts"] < boundary_iso]
            if not valid_rates and rates:
                valid_rates = [rates[0]]
            # Fallback to last_known_rates when no live rates are available
            # (e.g. after a restore where stale reads were cleared)
            if not valid_rates and last_known_rates:
                _fallback = last_known_rates.get(meter_name, {}).get(channel_name)
                if _fallback:
                    _rate_val = _rate_value(_fallback)
                    valid_rates = [{"ts": start, "value": _rate_val}]

            reads = channel.get("reads", [])

            if reads and not is_sub:
                pre_open  = select_opening_read(reads, block_start_dt)
                pre_open  = select_opening_read(reads, block_start_dt)
                post_open = select_closing_read(reads, block_start_dt)
                if pre_open and post_open and pre_open["ts"] != post_open["ts"]:
                    interpolated_opener = interpolate_value(pre_open, post_open, block_start_dt)
                else:
                    interpolated_opener = pre_open or post_open

                pre_close  = select_opening_read(reads, block_end_dt)
                post_close = select_closing_read(reads, block_end_dt)
                if pre_close and post_close and pre_close["ts"] != post_close["ts"]:
                    interpolated_closer = interpolate_value(pre_close, post_close, block_end_dt)
                else:
                    interpolated_closer = post_close or pre_close

                if interpolated_opener and interpolated_closer:
                    channel_for_compute              = dict(channel)
                    channel_for_compute["reads"]     = [interpolated_opener, interpolated_closer]
                    channel_for_compute["rates"]     = valid_rates
                    logger.info(
                        "finalise_block: %s/%s boundary delta=%.4f",
                        meter_name, channel_name,
                        interpolated_closer["value"] - interpolated_opener["value"],
                    )
                else:
                    logger.warning(
                        "finalise_block: %s/%s could not select boundary reads", meter_name, channel_name
                    )
                    channel_for_compute          = dict(channel)
                    channel_for_compute["rates"] = valid_rates
            else:
                channel_for_compute          = dict(channel)
                channel_for_compute["rates"] = valid_rates

            result = compute_channel(channel_for_compute, parent_rates, is_sub_meter=is_sub)

            channel_cfg_meta = meter_cfg.get("channels", {}).get(channel_name, {}).get("meta")
            if channel_cfg_meta:
                result["meta"] = channel_cfg_meta

            meter_block["channels"][channel_name] = result

            # ── 2.10.0: provisional detection for sub-meter import ──────────
            # A sub-meter block is provisional when it was closed without a
            # post-boundary read.  The first post-boundary read in the NEXT
            # block's window will trigger a retrospective amendment.
            if is_sub and channel_name == "import" and not interpolated:
                has_post = any(r["ts"] >= boundary_iso for r in reads)
                if not has_post:
                    meter_block["provisional"] = True
                    logger.info(
                        "finalise_block: %s import marked provisional "
                        "(no post-boundary read — will amend when read arrives)",
                        meter_name,
                    )

        block["meters"][meter_name] = meter_block

        if meter_meta.get("sub_meter") and parent_name:
            sub_import = meter_block["channels"].get("import")
            if sub_import:
                parent_sub_kwh[parent_name]  = parent_sub_kwh.get(parent_name, 0.0)  + sub_import["kwh"]
                parent_sub_cost[parent_name] = parent_sub_cost.get(parent_name, 0.0) + sub_import["cost"]

    # ── PASS 2 — grid-authoritative sub-meter distribution ────────────────
    _apply_pass2(block)
    # ── PASS 3 — compute block totals ─────────────────────────────────────
    for meter_name, meter_block in block["meters"].items():
        meta = meter_block["meta"]
        for channel_name, channel in meter_block["channels"].items():
            if channel_name == "import":
                if meta.get("sub_meter"):
                    block["totals"]["import_kwh"]  += channel.get("kwh_grid", channel["kwh"])
                    block["totals"]["import_cost"] += channel["cost"]
                else:
                    block["totals"]["import_kwh"]  += channel.get("kwh_remainder", channel["kwh"])
                    block["totals"]["import_cost"] += channel.get("cost_remainder", channel["cost"])
            elif channel_name == "export":
                block["totals"]["export_kwh"]  += channel["kwh"]
                block["totals"]["export_cost"] += channel["cost"]

    # ── PASS 3b — carbon footprint ───────────────────────────────────────
    # Join to nearest carbon_intensity row and compute carbon_g for every meter.
    try:
        postcode = _get_postcode()
        if postcode:
            ci_row = _store.get_nearest_carbon_intensity(start, postcode)
            if ci_row:
                intensity = ci_row["intensity"]
                for meter_name, meter_block in block["meters"].items():
                    meta       = meter_block.get("meta", {}) or {}
                    imp_ch     = meter_block.get("channels", {}).get("import")
                    exp_ch     = meter_block.get("channels", {}).get("export")
                    is_sub     = meta.get("sub_meter", False)
                    if is_sub:
                        # Sub-meter: gross import consumption only
                        imp_kwh  = float((imp_ch or {}).get("kwh", 0.0) or 0.0)
                        carbon_g = round(imp_kwh * intensity, 4)
                    else:
                        # Main meter: use kwh_total (full grid draw before sub-meter
                        # subtraction) minus export. This is the actual carbon produced
                        # by grid generation on our behalf regardless of how it was
                        # distributed to sub-meters.
                        imp_kwh  = float((imp_ch or {}).get("kwh_total",
                                   (imp_ch or {}).get("kwh", 0.0)) or 0.0)
                        exp_kwh  = float((exp_ch or {}).get("kwh", 0.0) or 0.0)
                        carbon_g = round((imp_kwh - exp_kwh) * intensity, 4)
                    meter_block["carbon_g"] = carbon_g
                    logger.debug(
                        "finalise_block: %s carbon_g=%.2f gCO2 (intensity=%.1f gCO2/kWh)",
                        meter_name, carbon_g, intensity
                    )
    except Exception as e:
        logger.warning("finalise_block: carbon_g computation failed: %s", e)

    append_block(block)

    # ── PASS 3c — store generation mix for each meter block ─────────────
    try:
        if postcode and _current_slot_mix:
            start_iso = block.get("start", "")
            # Find nearest slot mix by timestamp
            nearest_key = min(_current_slot_mix.keys(),
                               key=lambda k: abs(
                                   datetime.fromisoformat(k).timestamp() -
                                   datetime.fromisoformat(start_iso).timestamp()
                               )) if start_iso else None
            if nearest_key:
                mix = _current_slot_mix[nearest_key]
                # Store mix against main meter block only — mix is a grid property,
                # not per-meter. Storing against all meters wastes ~3x space.
                main_row = _store._conn.execute(
                    "SELECT id FROM blocks WHERE block_start = ? AND meter_id = 'electricity_main' LIMIT 1",
                    (start_iso,)
                ).fetchone()
                if main_row:
                    _store.upsert_generation_mix(main_row["id"], mix)
    except Exception as _gme:
        logger.debug("finalise_block: generation mix storage failed: %s", _gme)

    # ── PASS 4 — update cumulative totals (derived from DB, no JSON file) ───
    engine_totals = _store.get_cumulative_totals()

    # ── Update HA sensors (schedule on the event loop — finalise_block is sync) ──
    loop = asyncio.get_event_loop()
    if loop.is_running():
        loop.create_task(_deferred_sensor_update(ha, engine_totals))
    else:
        logger.warning("finalise_block: no running event loop for sensor update")

    logger.info("finalise_block: %s → %s complete", start, end)

    # ── Prune rolling buffer ───────────────────────────────────────────────
    pruned_block = {
        "start":  iso(block_end_dt),
        "end":    iso(block_end_dt + timedelta(minutes=int(get_block_minutes()))),
        "meters": {},
        "interpolated": False,
    }

    for meter_name, meter_data in cb.get("meters", {}).items():
        pruned_block["meters"][meter_name] = {
            "meta":     meter_data.get("meta", {}),
            "channels": {},
            "interpolated": False,
        }
        for channel_name, channel in meter_data.get("channels", {}).items():
            reads = channel.get("reads", [])
            rates = channel.get("rates", [])

            pruned_reads = [r for r in reads if r["ts"] >= iso(block_end_dt)]
            pruned_rates = [r for r in rates if r["ts"] >= iso(block_end_dt)]

            if not pruned_reads and reads:
                pruned_reads = [reads[-1]]
            if not pruned_rates and rates:
                pruned_rates = [rates[-1]]

            # Carry last pre-boundary read as opener seed for next block
            pre_boundary = [r for r in reads if r["ts"] < iso(block_end_dt)]
            if pre_boundary:
                last_pre = pre_boundary[-1]
                if not any(r["ts"] == last_pre["ts"] for r in pruned_reads):
                    pruned_reads.insert(0, last_pre)
                    logger.info(
                        "finalise_block: carrying seed %s into next block for %s/%s",
                        last_pre["ts"], meter_name, channel_name,
                    )

            pruned_block["meters"][meter_name]["channels"][channel_name] = {
                "reads": pruned_reads,
                "rates": pruned_rates,
            }

    if "_gap_marker" in cb:
        pruned_block["_gap_marker"] = cb["_gap_marker"]
        logger.info("finalise_block: gap marker carried forward")

    _store.save_current_block(pruned_block)
    logger.info("finalise_block: rolling buffer pruned, new block starts %s", iso(block_end_dt))

    # ── Generate charts ────────────────────────────────────────────────────
    # Skip chart regeneration for interpolated (gap-fill) blocks — during a
    # long offline gap this would regenerate charts once per missing block,
    # causing minutes of CPU load. Charts are regenerated once at startup
    # (generate_charts in engine_startup) and again on the first live block.
    if not interpolated:
        generate_charts(get_store())

    # ── Backup to /share ───────────────────────────────────────────────────
    _backup_to_share()


async def _deferred_sensor_update(ha: HAClient, engine_totals: dict):
    """Awaitable wrapper so finalise_block (sync) can schedule an async sensor push."""
    try:
        if _PUBLISH_HA_SENSORS:
            await update_ha_sensors(ha, engine_totals)
    except Exception as e:
        logger.error("_deferred_sensor_update: %s", e)


# ─────────────────────────────────────────────────────────────────────────────
# State trigger callbacks  (replaces @state_trigger decorators)
# ─────────────────────────────────────────────────────────────────────────────

async def on_import_meter_update(entity_id: str, new_val: str, full_state: dict):
    """Fired by ha_client when the main import sensor changes state."""
    try:
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        _read_queue.append(now)
        logger.info("on_import_meter_update: read queued at %s", now.isoformat())
    except Exception as e:
        logger.error("on_import_meter_update: %s", e)


async def on_export_meter_update(entity_id: str, new_val: str, full_state: dict):
    """Fired by ha_client when the main export sensor changes state."""
    try:
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        _read_queue.append(now)
        logger.info("on_export_meter_update: read queued at %s", now.isoformat())
    except Exception as e:
        logger.error("on_export_meter_update: %s", e)


# ─────────────────────────────────────────────────────────────────────────────
# Engine loop  (replaces @time_trigger("period(now, 10s)"))
# ─────────────────────────────────────────────────────────────────────────────

def _get_postcode() -> str | None:
    """Return the postcode_prefix from the main meter config, or None."""
    try:
        cfg = load_config()
        for m_data in cfg.get("meters", {}).values():
            meta = (m_data or {}).get("meta", {}) or {}
            if not meta.get("sub_meter"):
                pc = _normalise_postcode(meta.get("postcode_prefix", ""))
                return pc if pc else None
    except Exception:
        pass
    return None


def _normalise_postcode(raw: str) -> str:
    """Strip full postcode to outward code only. 'DE1 3AT' → 'DE1', 'SW1A 2AA' → 'SW1A'."""
    return raw.strip().upper().split()[0] if raw and raw.strip() else ""


def _fetch_carbon_intensity(postcode: str) -> list:
    """
    Fetch current + 48hr forecast from National Grid API for the given postcode.
    Returns list of {captured_at, intensity, ci_index} dicts.
    Raises on HTTP/network error so caller can log appropriately.
    """
    import urllib.request
    import urllib.error
    import json as _json

    now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%MZ")
    url = (f"https://api.carbonintensity.org.uk/regional/intensity"
           f"/{now_iso}/fw48h/postcode/{postcode}")
    req = urllib.request.Request(url, headers={"Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=8) as resp:
        data = _json.loads(resp.read())

    slots = []
    raw = data.get("data", [])
    # Handle dict shape: {data: {regionid, postcode, data: [...]}}
    if isinstance(raw, dict):
        entries = raw.get("data", [])
    elif isinstance(raw, list) and raw:
        first = raw[0]
        entries = first.get("data", []) if "data" in first else raw
    else:
        entries = []

    for slot in entries:
        intensity_obj = slot.get("intensity", {})
        forecast      = intensity_obj.get("forecast")
        actual        = intensity_obj.get("actual")    # None until ~24hr after slot
        ci_index      = intensity_obj.get("index")
        slot_from     = slot.get("from")
        if slot_from and (forecast is not None or actual is not None):
            captured_at = slot_from.replace("Z", "").replace("+00:00", "")
            slots.append({
                "captured_at":       captured_at,
                "intensity":         float(actual if actual is not None else forecast),
                "intensity_forecast": float(forecast) if forecast is not None else None,
                "intensity_actual":   float(actual)   if actual   is not None else None,
                "ci_index":          ci_index,
                "generationmix":     slot.get("generationmix", []),
            })

    return slots


async def _tick_carbon_intensity() -> float | None:
    """
    Fetch and store carbon intensity if 15 minutes have elapsed since last fetch.
    Returns the current intensity (gCO2/kWh) or None.
    """
    global _last_ci_fetch
    import urllib.error

    postcode = _get_postcode()
    if not postcode:
        return None  # No postcode configured — fail silently

    now = datetime.now(timezone.utc).replace(tzinfo=None)
    elapsed = (now - _last_ci_fetch).total_seconds() if _last_ci_fetch else None
    if elapsed is not None and elapsed < 900:  # 15 minutes
        # Not time yet — return current intensity from DB without fetching
        row = _store.get_nearest_carbon_intensity(now.isoformat(), postcode)
        return row["intensity"] if row else None

    try:
        slots = _fetch_carbon_intensity(postcode)
        for slot in slots:
            _store.upsert_carbon_intensity(
                slot["captured_at"], postcode,
                slot["intensity_forecast"], slot["ci_index"],
                slot["intensity_actual"]
            )
            if slot.get("generationmix"):
                mix = slot["generationmix"]
                _current_slot_mix[slot["captured_at"]] = mix
                # Write to mix_history at CI-tick resolution (independent of block size)
                _store.upsert_mix_history(slot["captured_at"], mix)
        _store.prune_carbon_intensity(days=4)
        _store.prune_mix_history(hours=48)
        _last_ci_fetch = now
        logger.info("_tick_carbon_intensity: stored %d slots for %s", len(slots), postcode)
    except urllib.error.HTTPError as e:
        logger.warning("_tick_carbon_intensity: HTTP %s for postcode %s", e.code, postcode)
    except urllib.error.URLError as e:
        logger.warning("_tick_carbon_intensity: network error for postcode %s: %s", postcode, e)
    except Exception as e:
        logger.warning("_tick_carbon_intensity: unexpected error: %s", e)

    # Return current intensity from DB regardless of fetch success/failure
    row = _store.get_nearest_carbon_intensity(now.isoformat(), postcode)
    return row["intensity"] if row else None


async def engine_loop_task(ha: HAClient):
    """
    Runs forever, ticking every 10 seconds.
    Replaces @time_trigger("period(now, 10s)") + @task_unique("energy_engine_loop").
    The asyncio.Lock prevents overlapping executions.
    """
    logger.info("engine_loop_task: started")

    while True:
        try:
            async with _engine_loop_lock:
                await _engine_tick(ha)
        except Exception as e:
            logger.error("engine_loop_task: unhandled error: %s", e)

        await asyncio.sleep(10)


async def _engine_tick(ha: HAClient):
    if _engine_paused:
        return
    now = datetime.now(timezone.utc).replace(tzinfo=None)

    current_block = _store.load_current_block()

    # Load block size from config (may have changed since startup)
    block_minutes = get_block_minutes()

    # Periodic checkpoint
    last_checkpoint = current_block.get("_last_checkpoint")
    if last_checkpoint:
        since_checkpoint   = (now - datetime.fromisoformat(last_checkpoint)).total_seconds()
        periodic_checkpoint = since_checkpoint >= 60
    else:
        periodic_checkpoint = True

    seconds_into_block = (now.minute % block_minutes) * 60 + now.second
    near_boundary      = (block_minutes * 60 - seconds_into_block) <= 15

    # Drain read queue
    if _read_queue:
        drained = 0
        while _read_queue:
            queued_ts = _read_queue.pop(0)
            capture_samples(ha, current_block, queued_ts)
            drained += 1
        logger.info("_engine_tick: drained %d queued reads", drained)

    if not _read_queue and (periodic_checkpoint or near_boundary):
        capture_samples(ha, current_block, now)

    # ── 2.10.0: amend provisional sub-meter blocks ────────────────────────
    # Skip entirely when a gap marker is active — gap-seed reads in the
    # rolling buffer look identical to real reads and must never be used
    # as interpolation anchors for boundary correction.
    if not has_gap_marker(current_block):
        try:
            _amend_provisional_sub_meter_blocks(ha, current_block)
        except Exception as _amp_e:
            logger.warning("_engine_tick: provisional amendment failed: %s", _amp_e)

    # Deferred gap filling
    # Pre-populate with rates from the last finalised block so finalise_block
    # always has a rate fallback even when no gap fill has run this tick.
    _post_gap_rates = None
    try:
        _last_blk = _store.get_last_block()
        if _last_blk:
            _, _post_gap_rates = extract_last_reads(_last_blk)
    except Exception:
        pass
    if has_gap_marker(current_block):
        has_real_read = False
        post_reads    = {}

        for meter_name, meter_data in current_block.get("meters", {}).items():
            post_reads[meter_name] = {}
            for channel_name, channel in meter_data.get("channels", {}).items():
                reads = channel.get("reads", [])
                if reads:
                    # Use the LAST (most recent) read as post-gap anchor, not the first.
                    # If the sensor updated multiple times before gap fill triggered,
                    # reads[-1] gives the best interpolation endpoint.
                    post_reads[meter_name][channel_name] = reads[-1]
                    has_real_read = True

        if not has_real_read:
            logger.info("gap fill: waiting for first post-outage read")
        else:
            logger.info("gap fill: post-outage read available, filling gap now")
            marker     = current_block["_gap_marker"]
            pre_reads  = marker["pre_reads"]
            last_rates = marker["last_known_rates"]
            logger.info("gap fill: pre_reads=%s", pre_reads)
            logger.info("gap fill: last_rates=%s", last_rates)
            logger.info("gap fill: post_reads=%s", post_reads)

            pre_ts = None
            for meter_reads in pre_reads.values():
                for read in meter_reads.values():
                    if isinstance(read, dict) and read.get("ts"):
                        if not pre_ts or read["ts"] > pre_ts:
                            pre_ts = read["ts"]

            # Use last_block_start stored in the gap marker as the anchor.
            # This is the start of the last finalised block, so detect_gap
            # computes last_block_end = last_block_start + block_minutes,
            # which equals last_block.end — correctly including the unfinalised
            # current_block window in missing_windows.
            gap_anchor_ts = marker.get("last_block_start") or pre_ts

            logger.info("gap fill: pre_ts=%s gap_anchor_ts=%s now=%s", pre_ts, gap_anchor_ts, now)
            missing_windows = detect_gap(gap_anchor_ts, now, block_minutes=block_minutes)
            logger.info("gap fill: missing_windows count=%s first=%s last=%s",
                        len(missing_windows) if missing_windows else 0,
                        missing_windows[0] if missing_windows else None,
                        missing_windows[-1] if missing_windows else None)

            # GAP_FILL_LIMIT_HOURS defined at module level
            gap_hours = len(missing_windows) * block_minutes / 60.0 if missing_windows else 0.0

            if missing_windows and gap_hours > GAP_FILL_LIMIT_HOURS:
                # Gap exceeds 12-hour limit — skip gap-fill entirely.
                # The first resumed block will calculate its delta from post-gap
                # reads correctly. Handles: extended outages, meter replacement,
                # moving property — all produce gaps > 12 hours naturally.
                logger.warning(
                    "gap fill: gap of %.1f hours exceeds %d-hour limit — "
                    "gap-fill skipped. Data absent for this window.",
                    gap_hours, GAP_FILL_LIMIT_HOURS
                )
                # ── Detect possible meter replacement ────────────────────────
                # If post-gap read is significantly lower than pre-gap read,
                # flag it so the UI can suggest creating a new billing period.
                global _meter_reset_detected
                try:
                    main_pre  = pre_reads.get("electricity_main", {}).get("import", {})
                    main_post = post_reads.get("electricity_main", {}).get("import", {})
                    pre_val  = float(main_pre.get("value",  0)) if isinstance(main_pre,  dict) else None
                    post_val = float(main_post.get("value", 0)) if isinstance(main_post, dict) else None
                    RESET_THRESHOLD_KWH = 50.0
                    if pre_val is not None and post_val is not None:
                        if post_val < pre_val - RESET_THRESHOLD_KWH:
                            logger.warning(
                                "gap fill: meter read reset detected — "
                                "pre-gap=%.3f kWh post-gap=%.3f kWh (drop=%.1f kWh). "
                                "Possible meter replacement or property move.",
                                pre_val, post_val, pre_val - post_val
                            )
                            _meter_reset_detected = True
                except Exception as _re:
                    logger.debug("gap fill: meter reset check failed: %s", _re)

            elif missing_windows:
                config     = load_config()
                # Get last known standing charge from most recent finalised main-meter block
                last_sc = 0.0
                try:
                    row = _store._conn.execute(
                        """SELECT standing_charge FROM blocks
                           WHERE meter_id = 'electricity_main'
                             AND standing_charge > 0
                           ORDER BY block_start DESC LIMIT 1"""
                    ).fetchone()
                    if row:
                        last_sc = float(row["standing_charge"])
                except Exception:
                    pass
                gap_blocks = build_gap_blocks(
                    missing_windows, pre_reads, post_reads, last_rates, config,
                    last_standing_charge=last_sc
                )
                for gb in gap_blocks:
                    append_block_replace(gb)

                engine_totals = _store.get_cumulative_totals()
                if _PUBLISH_HA_SENSORS:
                    await update_ha_sensors(ha, engine_totals)
                logger.info("gap fill: %d interpolated blocks inserted", len(gap_blocks))
            else:
                logger.warning("gap fill: no missing windows found, clearing marker")

            clear_gap_marker(current_block)
            # Preserve last_known_rates for catch-up rollover rate fallback
            _post_gap_rates = last_rates
            # Reset current_block to the current window so the next restart
            # doesn't try to catch up through blocks already written by gap fill.
            block_minutes_now = int(get_block_minutes())
            gap_start, gap_end = get_block_window(now, block_minutes=block_minutes_now)
            current_block = create_block(gap_start, gap_end, block_minutes=block_minutes_now)
            _store.save_current_block(current_block)

    # ── Carbon intensity — fetch BEFORE block lifecycle so catch-up blocks ──
    # ── have fresh CI data when finalise_block computes carbon_g           ──
    try:
        current_intensity = await _tick_carbon_intensity()
    except Exception as _cie:
        logger.warning("_engine_tick: CI fetch raised: %s", _cie)
        current_intensity = None

    # Block lifecycle
    updated_block = ensure_correct_block(ha, current_block, now, last_known_rates=_post_gap_rates)
    block_changed = updated_block.get("start") != current_block.get("start")

    if block_changed or periodic_checkpoint or near_boundary:
        updated_block["_last_checkpoint"] = now.isoformat()
        _store.save_current_block(updated_block)

    # Write power history row every tick if a power sensor is configured
    try:
        cfg = load_config()
        power_sensor = None
        for m_data in cfg.get("meters", {}).values():
            meta = (m_data or {}).get("meta", {}) or {}
            if not meta.get("sub_meter"):
                power_sensor = meta.get("power_sensor")
                break
        if power_sensor and ha:
            raw_kw = ha.get_state(power_sensor)
            if raw_kw not in (None, "unknown", "unavailable"):
                net_kw = round(float(raw_kw), 3)

                # Carbon rate: net_kw (kW) × intensity (gCO2/kWh) ÷ 60 = gCO2/min.
                # Derived directly from the power sensor — the kWh sensor only updates
                # every ~60s while the engine ticks every ~10s, so the read-delta approach
                # produced zeros (sensor unchanged) and spikes (full 60s of kWh over a
                # ~10s dt_min). The power sensor is already the correct instantaneous value.
                carbon_gco2_min = None
                if current_intensity is not None:
                    try:
                        carbon_gco2_min = round(net_kw * current_intensity / 60.0, 4)
                    except Exception as _ce:
                        logger.debug("_engine_tick: carbon_gco2_min skipped: %s", _ce)

                _store.append_power_history(now.isoformat(), net_kw,
                                            current_intensity, carbon_gco2_min)
                _store.prune_power_history(hours=48)
    except Exception as e:
        logger.warning("_engine_tick: power_history write skipped: %s", e)

    # Write sub_meter_history rows every tick for battery/EV sub-meters
    try:
        cfg = load_config()
        _batt_kws = ['battery', 'batt', 'inverter', 'solax', 'givenergy', 'powerwall', 'solar']
        _pruned_sub = False
        for mid, m_data in cfg.get("meters", {}).items():
            meta = (m_data or {}).get("meta", {}) or {}
            if not meta.get("sub_meter"):
                continue
            soc_s = meta.get("soc_sensor")
            inv_s = meta.get("inverter_power_sensor")
            dev_s = meta.get("device_power_sensor")
            if not soc_s and not inv_s and not dev_s:
                continue  # no sensors configured for this sub-meter
            soc_val = inv_val = None
            if soc_s and ha:
                v = ha.get_state(soc_s)
                if v not in (None, "unknown", "unavailable"):
                    try:
                        soc_val = round(float(v), 1)
                    except (ValueError, TypeError):
                        pass
            # Use inverter_power_sensor for batteries, device_power_sensor for EV/heat pump
            power_s = inv_s or dev_s
            if power_s and ha:
                v = ha.get_state(power_s)
                if v not in (None, "unknown", "unavailable"):
                    try:
                        fv = float(v)
                        # Convert to kW using unit_of_measurement from HA attributes.
                        # The meter config sensor picker only allows power sensors (W or kW),
                        # so the unit is always known — no magnitude guessing needed.
                        try:
                            unit = (ha.get_attributes(power_s) or {}).get("unit_of_measurement", "")
                        except Exception:
                            unit = ""
                        if unit.upper() == "W":
                            fv = fv / 1000.0
                        inv_val = round(fv, 3)
                    except (ValueError, TypeError):
                        pass
            if soc_val is not None or inv_val is not None:
                _store.append_sub_meter_history(now.isoformat(), mid, soc_val, inv_val)
                if not _pruned_sub:
                    _store.prune_sub_meter_history(hours=48)
                    _pruned_sub = True
    except Exception as e:
        logger.warning("_engine_tick: sub_meter_history write skipped: %s", e)


# ─────────────────────────────────────────────────────────────────────────────
# Startup  (replaces @time_trigger("startup"))
# ─────────────────────────────────────────────────────────────────────────────


async def engine_startup(ha: HAClient):
    """
    Run once when the add-on starts.
    Registers state triggers, detects session gaps, generates startup charts.
    Replaces @time_trigger("startup").
    """
    # ── Open BlockStore FIRST so load_config() reads from the live DB ────
    # This must happen before load_config() — if _store is None and blocks.db
    # exists, load_config() deliberately returns {} to avoid stale JSON reads.
    # Opening the store here ensures config is always read from the DB on startup,
    # including after a restore where the DB file has been replaced.
    global _store
    _store = open_block_store(BLOCKS_DB_PATH)

    # ── Checkpoint WAL immediately after opening ──────────────────────────
    # Ensures all committed blocks are flushed from WAL into the main DB file.
    # Cheap and always safe — runs on every startup.
    global _meter_reset_detected
    _meter_reset_detected = False  # Reset on every startup so reconnects don't carry stale state

    try:
        _store._conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        logger.info("engine_startup: WAL checkpoint complete")
    except Exception as _wce:
        logger.warning("engine_startup: WAL checkpoint failed: %s", _wce)

    if not _PUBLISH_HA_SENSORS:
        logger.warning("engine_startup: HA sensor publishing DISABLED (publish_ha_sensors=false)")

    config = load_config()  # now reads from the open store ✓

    # ── Repair empty config period from meters_config.json (post-JSON migration) ─
    # If config_periods exists but has no meters (e.g. migration ran with empty config),
    # try to load meters_config.json and update the config period with it.
    if not config.get("meters") and os.path.exists(CONFIG_PATH):
        _json_config = load_json(CONFIG_PATH, {})
        if _json_config.get("meters"):
            logger.info(
                "engine_startup: config_periods has no meters — repairing from meters_config.json"
            )
            try:
                cp_id = _store.get_current_config_period_id()
                if cp_id:
                    _store._write_meters(_json_config, cp_id)
                    logger.info(
                        "engine_startup: config repaired — %d meter(s) written to period %d",
                        len(_json_config.get("meters", {})), cp_id
                    )
                else:
                    _store.insert_config_period(_json_config)
                    logger.info(
                        "engine_startup: config repaired — new period created from meters_config.json"
                    )
                config = load_config()
            except Exception as _cre:
                logger.warning("engine_startup: config repair failed: %s", _cre)

    # ── Register state triggers from config ─────────────────────────────
    main_import_sensor = None
    main_export_sensor = None

    for mid, mcfg in config.get("meters", {}).items():
        if not mcfg.get("meta", {}).get("sub_meter", False):
            main_import_sensor = mcfg.get("channels", {}).get("import", {}).get("read")
            main_export_sensor = mcfg.get("channels", {}).get("export", {}).get("read")
            break

    if main_import_sensor:
        ha.subscribe_state(main_import_sensor, on_import_meter_update)
        logger.info("engine_startup: import trigger active on %s", main_import_sensor)
    else:
        logger.warning("engine_startup: no main import sensor found in config")

    if main_export_sensor:
        ha.subscribe_state(main_export_sensor, on_export_meter_update)
        logger.info("engine_startup: export trigger active on %s", main_export_sensor)
    else:
        logger.warning("engine_startup: no main export sensor found in config")

    # Subscribe power sensor to state changes so the cache stays current
    # between restarts. No callback needed — ha_client updates the cache automatically.
    for mid, mcfg in config.get("meters", {}).items():
        if not mcfg.get("meta", {}).get("sub_meter", False):
            ps = mcfg.get("meta", {}).get("power_sensor")
            if ps:
                ha.subscribe_state(ps, lambda entity_id, new_val, full_state: None)
                logger.info("engine_startup: power sensor subscribed for cache: %s", ps)
            break

    # Subscribe to SoC sensors for all battery sub-meters (skip retired meters)
    for mid, mcfg in config.get("meters", {}).items():
        if mcfg.get("meta", {}).get("sub_meter", False):
            # Skip retired meters — don't subscribe to their sensors
            if _store.is_meter_retired(mid):
                logger.info("engine_startup: skipping retired meter %s", mid)
                continue
            soc_s = mcfg.get("meta", {}).get("soc_sensor")
            if soc_s:
                ha.subscribe_state(soc_s, lambda entity_id, new_val, full_state: None)
                logger.info("engine_startup: SoC sensor subscribed for cache: %s (%s)", soc_s, mid)
            inv_s = mcfg.get("meta", {}).get("inverter_power_sensor")
            if inv_s:
                ha.subscribe_state(inv_s, lambda entity_id, new_val, full_state: None)
                logger.info("engine_startup: inverter power sensor subscribed for cache: %s (%s)", inv_s, mid)
            dev_s = mcfg.get("meta", {}).get("device_power_sensor")
            if dev_s:
                ha.subscribe_state(dev_s, lambda entity_id, new_val, full_state: None)
                logger.info("engine_startup: device power sensor subscribed for cache: %s (%s)", dev_s, mid)

    # Pre-load sensor states into ha_client cache
    sensors_to_preload = []
    for mcfg in config.get("meters", {}).values():
        # Include power_sensor, soc_sensor and inverter_power_sensor from meta (not in channels)
        ps = mcfg.get("meta", {}).get("power_sensor")
        if ps:
            sensors_to_preload.append(ps)
        soc_s = mcfg.get("meta", {}).get("soc_sensor")
        if soc_s:
            sensors_to_preload.append(soc_s)
        inv_s = mcfg.get("meta", {}).get("inverter_power_sensor")
        if inv_s:
            sensors_to_preload.append(inv_s)
        dev_s = mcfg.get("meta", {}).get("device_power_sensor")
        if dev_s:
            sensors_to_preload.append(dev_s)
        for ccfg in mcfg.get("channels", {}).values():
            for key in ("read", "rate", "standing_charge_sensor"):
                eid = ccfg.get(key)
                if eid:
                    sensors_to_preload.append(eid)
    # ── Wait for sensors + CI (concurrent, with timeouts) ──────────────
    # Preload immediately — this gets the current REST-cached state for all sensors.
    if sensors_to_preload:
        await ha.preload_states(sensors_to_preload)

    # Identify read sensors that are still unavailable after preload.
    # These are the sensors we need to wait for — rate/SC sensors are less
    # critical since we fall back to last_known_rates from DB.
    _read_sensors = set()
    for mcfg in config.get("meters", {}).values():
        for ccfg in mcfg.get("channels", {}).values():
            eid = ccfg.get("read")
            if eid:
                _read_sensors.add(eid)

    _SENSOR_TIMEOUT = 60   # seconds to wait for read sensors
    _CI_TIMEOUT     = 30   # seconds to wait for CI API

    # Run sensor wait and CI fetch concurrently
    async def _wait_for_sensors():
        """Wait until all read sensors have a valid state or timeout.
        Only waits for sensors that exist in HA but are currently unavailable.
        Sensors that don't exist in HA at all (None) are skipped immediately —
        waiting won't help and would cause unnecessary 60s delays on dev systems
        using a prod DB with sensors not present in the dev HA instance.
        """
        import asyncio as _aio
        deadline = _aio.get_running_loop().time() + _SENSOR_TIMEOUT
        pending = {e for e in _read_sensors
                   if ha.get_state(e) in ("unknown", "unavailable")}
        missing = {e for e in _read_sensors if ha.get_state(e) is None}
        if missing:
            logger.warning("engine_startup: %d sensor(s) not found in HA — skipping wait: %s",
                           len(missing), missing)
        if not pending:
            logger.info("engine_startup: all read sensors available after preload")
            return
        logger.info("engine_startup: waiting for %d sensor(s): %s", len(pending), pending)
        while pending:
            remaining = deadline - _aio.get_running_loop().time()
            if remaining <= 0:
                logger.warning("engine_startup: sensor wait timeout — %d sensor(s) still unavailable: %s",
                               len(pending), pending)
                return
            await _aio.sleep(min(1.0, remaining))
            pending = {e for e in pending
                       if ha.get_state(e) in ("unknown", "unavailable")}
        logger.info("engine_startup: all read sensors now available")

    async def _startup_ci_fetch():
        """Fetch CI data at startup with timeout — runs in executor to avoid blocking event loop."""
        import asyncio as _aio
        import urllib.error
        # Extract postcode on the main thread BEFORE entering the executor.
        # _get_postcode() calls load_config() which accesses the SQLite connection.
        # Calling it from inside run_in_executor causes a cross-thread SQLite access
        # that can deadlock on a fresh install where the DB was just created.
        postcode = _get_postcode()
        if not postcode:
                return
        loop = _aio.get_running_loop()
        def _do_fetch():
            try:
                slots = _fetch_carbon_intensity(postcode)
                for slot in slots:
                    _store.upsert_carbon_intensity(
                        slot["captured_at"], postcode,
                        slot["intensity_forecast"], slot["ci_index"],
                        slot["intensity_actual"]
                    )
                _store.prune_carbon_intensity(days=4)
                return len(slots)
            except urllib.error.URLError as e:
                logger.warning("engine_startup: CI fetch failed: %s — will retry on first tick", e)
                return 0
            except Exception as e:
                logger.warning("engine_startup: CI fetch error: %s", e)
                return 0
        try:
            n = await _aio.wait_for(loop.run_in_executor(None, _do_fetch), timeout=_CI_TIMEOUT)
            if n:
                global _last_ci_fetch
                _last_ci_fetch = datetime.now(timezone.utc).replace(tzinfo=None)
                logger.info("engine_startup: CI fetch complete — %d slots for %s", n, postcode)
        except _aio.TimeoutError:
            logger.warning("engine_startup: CI fetch timed out after %ds — will retry on first tick", _CI_TIMEOUT)

    import asyncio as _asyncio
    await _asyncio.gather(
        _wait_for_sensors(),
        _startup_ci_fetch(),
        return_exceptions=True
    )

    # ── Detect and store currency symbol ────────────────────────────────
    for mid, mcfg in config.get("meters", {}).items():
        if not mcfg.get("meta", {}).get("sub_meter", False):
            rate_sensor = mcfg.get("channels", {}).get("import", {}).get("rate")
            if rate_sensor:
                try:
                    attrs = await ha.get_entity_attributes(rate_sensor)
                    unit  = attrs.get("unit_of_measurement", "")
                    symbol = detect_currency_symbol(unit)
                    # Derive ISO code — strip "/kWh" etc
                    code = unit.split("/")[0].strip().upper() if unit else "GBP"
                    mcfg.setdefault("meta", {})["currency_symbol"] = symbol
                    mcfg["meta"]["currency_code"]   = code
                    logger.info(
                        "engine_startup: currency detected from %s unit='%s' symbol='%s' code='%s'",
                        rate_sensor, unit, symbol, code,
                    )
                except Exception as e:
                    logger.warning("engine_startup: currency detection failed: %s", e)
            break
    # Persist currency to meters_config so charts can read it
    # Write meters_config.json as a convenience export (human-readable).
    # The DB config_period is the authoritative source — this file is not read back.
    try:
        save_json_atomic(CONFIG_PATH, config)
    except Exception as _e:
        logger.warning("engine_startup: could not write meters_config.json export: %s", _e)

    # ── Upgrade backup (once per version) ────────────────────────────────────
    # In versions prior to 2.4.0, backups could miss recent blocks due to the
    # WAL not being checkpointed. On first startup of a new version we create a
    # safety backup so users have a complete snapshot before any new data is
    # written. Runs once per version — not on every restart.
    try:
        import zipfile as _zf, glob as _gl
        from datetime import datetime as _dt2

        # Read current app version from config.yaml
        _ver_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.yaml")
        _cur_ver  = "unknown"
        if os.path.exists(_ver_path):
            with open(_ver_path) as _vf:
                for _vl in _vf:
                    if _vl.strip().startswith("version:"):
                        _cur_ver = _vl.split(":", 1)[1].strip().strip('"')
                        break

        # Check what version last created a startup backup
        _ver_file = os.path.join(DATA_DIR, ".last_startup_backup_version")
        _last_ver = ""
        if os.path.exists(_ver_file):
            with open(_ver_file) as _vff:
                _last_ver = _vff.read().strip()

        if _cur_ver != _last_ver:
            _bk_dir = f"{SHARE_BACKUP_DIR}/backups"
            ensure_dir(_bk_dir)
            _bk_ts   = _dt2.utcnow().strftime("%Y%m%dT%H%M%S")
            _bk_path = f"{_bk_dir}/{_bk_ts}_upgrade_{_cur_ver}.zip"
            # Skip backup on a fresh install with no blocks — nothing to back up,
            # and the SQLite backup API can hang on a newly-created WAL database.
            if _store.count_blocks() == 0:
                logger.info("engine_startup: skipping upgrade backup — no blocks (fresh install)")
                with open(_ver_file, "w") as _vfw:
                    _vfw.write(_cur_ver)
            else:
              with _zf.ZipFile(_bk_path, "w", _zf.ZIP_DEFLATED) as _bkz:
                # Commit any pending transactions before backup to avoid WAL lock
                try:
                    _store._conn.execute("PRAGMA wal_checkpoint(PASSIVE)")
                except Exception:
                    pass
                _store.backup(BLOCKS_DB_PATH + ".upgrade_bak")
                _bkz.write(BLOCKS_DB_PATH + ".upgrade_bak", "blocks.db")
                os.remove(BLOCKS_DB_PATH + ".upgrade_bak")
                _cfg_src = os.path.join(DATA_DIR, "meters_config.json")
                if os.path.exists(_cfg_src):
                    _bkz.write(_cfg_src, "meters_config.json")
              # Keep only the 20 most recent zips
              _all_zips = sorted(_gl.glob(f"{_bk_dir}/*.zip"))
              for _old_zip in _all_zips[:-20]:
                  try: os.remove(_old_zip)
                  except Exception: pass
              # Record this version so we don't backup again until next upgrade
              with open(_ver_file, "w") as _vfw:
                  _vfw.write(_cur_ver)
              logger.info(
                  "engine_startup: upgrade backup created for v%s: %s",
                  _cur_ver, os.path.basename(_bk_path)
              )
        else:
            logger.info("engine_startup: no upgrade detected (v%s), skipping upgrade backup", _cur_ver)
    except Exception as _sbe:
        logger.warning("engine_startup: upgrade backup failed: %s", _sbe)

    # Migrate full_config_json → normalised tables (2.0→2.1 one-time upgrade)
    try:
        migrated = _store.migrate_full_config_json()
        if migrated:
            logger.info(
                "engine_startup: migrated %d config periods to normalised schema", migrated
            )
    except Exception as _me:
        logger.warning("engine_startup: full_config_json migration failed: %s", _me)

    if _store.get_current_config_period_id() is None:
        # Fresh DB — check if blocks.json exists to migrate
        # Always try to load config from meters_config.json for migration,
        # since load_config() returns {} when config_periods is empty.
        _migration_config = load_json(CONFIG_PATH, {})
        if not _migration_config.get("meters"):
            _migration_config = config  # fall back to whatever load_config returned
        else:
            logger.info(
                "engine_startup: loaded meter config from meters_config.json for migration"
            )
        if os.path.exists(BLOCKS_PATH):
            logger.info("engine_startup: blocks.json found — running auto-migration to SQLite")
            migrated = migrate_json_to_sqlite(BLOCKS_PATH, _store, _migration_config)
            logger.info("engine_startup: migration complete — %d blocks migrated", migrated)
            # Rename blocks.json so it's preserved but no longer used
            migrated_path = BLOCKS_PATH + ".migrated"
            try:
                os.rename(BLOCKS_PATH, migrated_path)
                logger.info("engine_startup: blocks.json renamed to %s", migrated_path)
            except Exception as e:
                logger.warning("engine_startup: could not rename blocks.json: %s", e)
        elif os.path.exists(BLOCKS_PATH + ".migrated"):
            # DB was deleted but migrated source still exists — re-migrate from it
            logger.info("engine_startup: blocks.json.migrated found — re-migrating to fresh DB")
            migrated = migrate_json_to_sqlite(BLOCKS_PATH + ".migrated", _store, _migration_config)
            logger.info("engine_startup: re-migration complete — %d blocks migrated", migrated)
        else:
            # Brand new install — create initial config period starting NOW
            # Always use now regardless of any date in the config JSON —
            # using a historical date would trigger gap fill for all missing blocks.
            _store.insert_config_period(config, effective_from=datetime.now(timezone.utc).replace(tzinfo=None).isoformat())
            # Clear any stale current_block from a previous session —
            # if there's no config period, the current_block is invalid.
            _store.save_current_block({})
            logger.info("engine_startup: new install — initial config period created")

    logger.info("engine_startup: %d existing blocks in store", _store.count_blocks())

    # ── Migrate current_block.json → DB (one-time, 2.1.0 upgrade) ───────
    current_block_json_path = os.path.join(DATA_DIR, "current_block.json")
    if os.path.exists(current_block_json_path):
        cb_in_db = _store.load_current_block()
        if not cb_in_db or not cb_in_db.get("start"):
            try:
                cb_from_file = load_json(current_block_json_path, {})
                if cb_from_file and cb_from_file.get("start"):
                    _store.save_current_block(cb_from_file)
                    logger.info(
                        "engine_startup: current_block.json migrated to DB (start=%s)",
                        cb_from_file.get("start")
                    )
            except Exception as _cbe:
                logger.warning("engine_startup: current_block.json migration failed: %s", _cbe)
        # Rename so it's no longer read on subsequent startups
        try:
            os.rename(current_block_json_path, current_block_json_path + ".migrated")
            logger.info("engine_startup: current_block.json renamed to .migrated")
        except Exception as _cbe:
            logger.warning("engine_startup: could not rename current_block.json: %s", _cbe)

    # ── Session gap detection ────────────────────────────────────────────
    # Use the last block BEFORE current_block.start to avoid zero-rate catch-up
    # blocks (written by ensure_correct_block before this startup completes)
    # from contaminating the gap detection and last_known_rates.
    _cb_for_gap = _store.load_current_block()
    _cb_start   = _cb_for_gap.get("start") if _cb_for_gap else None
    last_block  = _store.get_last_block_before(_cb_start) if _cb_start else _store.get_last_block()
    logger.info("engine_startup: gap detection — current_block.start=%s", _cb_start)
    if last_block:
        logger.info("engine_startup: gap detection — last_block start=%s end=%s",
                    last_block.get("start"), last_block.get("end"))
        for _m, _md in last_block.get("meters", {}).items():
            for _ch, _chd in _md.get("channels", {}).items():
                logger.info("engine_startup: gap detection — last_block %s/%s rate=%s read_end=%s",
                            _m, _ch, _chd.get("rate"), _chd.get("read_end"))
    else:
        logger.warning("engine_startup: gap detection — no last_block found")
    if last_block:
        # Use the START of the last finalised block as the gap anchor so that
        # the unfinalised current_block window is included in gap fill.
        # detect_gap computes last_block_end = floor(anchor) + block_minutes,
        # so passing block start gives last_block_end = last_block.end,
        # covering the current_block window and everything after it.
        last_block_start = last_block.get("start")
        if last_block_start:
            # Derive block_minutes from the last block meta, falling back to config
            # then global default. Critical for non-30min setups.
            _lb_bm = int(
                (last_block.get("meters") or {})
                .get("electricity_main", {})
                .get("meta", {})
                .get("block_minutes") or get_block_minutes()
            )
            missing_windows = detect_gap(last_block_start, datetime.now(timezone.utc).replace(tzinfo=None), block_minutes=_lb_bm)
            if missing_windows:
                logger.warning(
                    "engine_startup: session gap detected — %d missing blocks", len(missing_windows)
                )
                logger.info("engine_startup: gap windows first=%s last=%s",
                            missing_windows[0], missing_windows[-1])
                current_block        = _store.load_current_block()
                pre_reads, last_rates = extract_last_reads(last_block)
                logger.info("engine_startup: pre_reads=%s", pre_reads)
                logger.info("engine_startup: last_rates=%s", last_rates)

                # ── Detect just-unretired sub-meters ─────────────────────
                # A sub-meter unretired after a long absence has a last block
                # from days/weeks ago. Including it in pre_reads causes a
                # massive delta spike on the first resumed block — the engine
                # tries to reconcile the entire retirement gap in one block.
                # Fix: remove any sub-meter whose last block is > 12 hours
                # old from pre_reads so it starts fresh from live reads.
                for _mid in list(pre_reads.keys()):
                    if _mid == "electricity_main":
                        continue
                    _last_sub = _store._conn.execute(
                        "SELECT MAX(block_end) as last_end FROM blocks WHERE meter_id=?",
                        (_mid,)
                    ).fetchone()
                    if not _last_sub or not _last_sub["last_end"]:
                        continue
                    _last_dt  = datetime.fromisoformat(_last_sub["last_end"])
                    _now_utc  = datetime.now(timezone.utc).replace(tzinfo=None)
                    _gap_hrs  = (_now_utc - _last_dt).total_seconds() / 3600
                    if _gap_hrs > GAP_FILL_LIMIT_HOURS:
                        logger.warning(
                            "engine_startup: sub-meter %s last block %.1f hours ago — "
                            "removing from pre_reads to prevent delta spike on resume "
                            "(possible unretire or extended absence).",
                            _mid, _gap_hrs
                        )
                        del pre_reads[_mid]
                        last_rates.pop(_mid, None)
                # Clear stale reads from before the restart — if we leave them in,
                # the sub-meter's channel reads will span the restart gap, producing
                # a false large delta on the first post-restart block (e.g. 10 kWh
                # from a battery sensor whose cumulative read advanced while offline).
                # The gap marker's pre_reads captures the correct pre-gap values;
                # live reads will accumulate fresh from the first post-restart capture.
                for meter_data in (current_block.get("meters") or {}).values():
                    for channel in (meter_data.get("channels") or {}).values():
                        channel["reads"] = []
                        channel["rates"] = []
                # ── Attempt immediate gap fill using preloaded sensor states ──
                # Build post_reads from the preloaded HA state cache.
                # This avoids the race condition where gap fill was previously
                # deferred until the first live sensor fire, which could happen
                # before sub-meter sensors had reported — producing rate=0 gap blocks.
                _preload_post_reads = {}
                _has_preload_read = False
                for _m_name, _m_cfg in config.get("meters", {}).items():
                    _preload_post_reads[_m_name] = {}
                    for _ch_name, _ch_cfg in _m_cfg.get("channels", {}).items():
                        _eid = _ch_cfg.get("read")
                        if not _eid:
                            continue
                        _val = ha.get_state(_eid)
                        if _val not in (None, "unknown", "unavailable"):
                            try:
                                _preload_post_reads[_m_name][_ch_name] = {
                                    "ts":    datetime.now(timezone.utc).replace(tzinfo=None).isoformat(),
                                    "value": float(_val),
                                }
                                _has_preload_read = True
                            except (ValueError, TypeError):
                                pass

                # ── 12-hour gap-fill limit ────────────────────────────────
                _startup_gap_hours = len(missing_windows) * _lb_bm / 60.0
                if _startup_gap_hours > 12:
                    logger.warning(
                        "engine_startup: gap of %.1f hours exceeds 12-hour limit — "
                        "gap-fill skipped. Data will be absent for this window.",
                        _startup_gap_hours
                    )
                    # ── Detect possible meter replacement ─────────────────────
                    try:
                        _su_pre  = pre_reads.get("electricity_main", {}).get("import", {})
                        _su_post = _preload_post_reads.get("electricity_main", {}).get("import", {})
                        _su_pre_val  = float(_su_pre.get("value",  0)) if isinstance(_su_pre,  dict) else None
                        _su_post_val = float(_su_post.get("value", 0)) if isinstance(_su_post, dict) else None
                        if _su_pre_val is not None and _su_post_val is not None:
                            if _su_post_val < _su_pre_val - 50.0:
                                logger.warning(
                                    "engine_startup: meter read reset detected — "
                                    "pre-gap=%.3f kWh post-gap=%.3f kWh (drop=%.1f kWh). "
                                    "Possible meter replacement or property move.",
                                    _su_pre_val, _su_post_val, _su_pre_val - _su_post_val
                                )
                                _meter_reset_detected = True
                    except Exception as _su_re:
                        logger.debug("engine_startup: meter reset check failed: %s", _su_re)
                    # Reset current_block to current window — don't leave stale start
                    _cb_bm = int(get_block_minutes())
                    _cb_start2, _cb_end2 = get_block_window(
                        datetime.now(timezone.utc).replace(tzinfo=None), block_minutes=_cb_bm
                    )
                    _store.save_current_block(create_block(_cb_start2, _cb_end2, block_minutes=_cb_bm))

                elif _has_preload_read:
                    # We have sensor data and gap is within limit — fill immediately
                    logger.info("engine_startup: filling gap immediately using preloaded sensor states")
                    _startup_sc = 0.0
                    try:
                        _sc_row = _store._conn.execute(
                            """SELECT standing_charge FROM blocks
                               WHERE meter_id = 'electricity_main'
                                 AND standing_charge > 0
                               ORDER BY block_start DESC LIMIT 1"""
                        ).fetchone()
                        if _sc_row:
                            _startup_sc = float(_sc_row["standing_charge"])
                    except Exception:
                        pass
                    _startup_gap_blocks = build_gap_blocks(
                        missing_windows, pre_reads, _preload_post_reads,
                        last_rates, config,
                        last_standing_charge=_startup_sc
                    )
                    # Add CI to gap blocks from the carbon_intensity table
                    _postcode = _get_postcode()
                    for _gb in _startup_gap_blocks:
                        try:
                            if _postcode:
                                _ci = _store.get_nearest_carbon_intensity(
                                    _gb["start"], _postcode
                                )
                                if _ci:
                                    _intensity = _ci["intensity"]
                                    for _mn, _mb in (_gb.get("meters") or {}).items():
                                        _imp_ch = (_mb.get("channels") or {}).get("import")
                                        _exp_ch = (_mb.get("channels") or {}).get("export")
                                        _is_sub = (_mb.get("meta") or {}).get("sub_meter", False)
                                        if _is_sub:
                                            _imp_kwh = float((_imp_ch or {}).get("kwh", 0.0) or 0.0)
                                            _mb["carbon_g"] = round(_imp_kwh * _intensity, 4)
                                        else:
                                            _imp_kwh = float((_imp_ch or {}).get("kwh_total",
                                                       (_imp_ch or {}).get("kwh", 0.0)) or 0.0)
                                            _exp_kwh = float((_exp_ch or {}).get("kwh", 0.0) or 0.0)
                                            _mb["carbon_g"] = round((_imp_kwh - _exp_kwh) * _intensity, 4)
                        except Exception:
                            pass
                        append_block_replace(_gb)
                    logger.info("engine_startup: %d gap blocks filled at startup", len(_startup_gap_blocks))
                    # No gap marker needed — gap already filled
                else:
                    # Sensors still unavailable after wait — fall back to deferred fill
                    logger.warning("engine_startup: no preloaded reads available — deferring gap fill")
                    set_gap_marker(current_block, pre_reads, last_rates,
                                   last_block_start=last_block_start)
                    _store.save_current_block(current_block)
                    logger.info("engine_startup: gap marker set, will fill on first sensor capture")
            else:
                logger.info("engine_startup: no session gap detected")

    # ── Seed _current_slot_mix from recent DB data ───────────────────────
    # _current_slot_mix is in-memory and lost on restart. Re-seed from
    # generation_mix rows for the last 2 hours so that blocks finalising
    # immediately after startup get mix data without waiting for the next
    # CI tick (which fires every 30 min and might race with block finalise).
    global _current_slot_mix
    try:
        _seed_rows = _store._conn.execute(
            """SELECT b.block_start, gm.fuel, gm.perc
               FROM blocks b
               JOIN generation_mix gm ON gm.block_id = b.id
               WHERE b.meter_id = 'electricity_main'
                 AND b.block_start >= datetime('now', '-4 hours')
               ORDER BY b.block_start ASC, gm.fuel ASC"""
        ).fetchall()
        _seed_slots = {}
        for row in _seed_rows:
            bs = row["block_start"]
            if bs not in _seed_slots:
                _seed_slots[bs] = []
            _seed_slots[bs].append({"fuel": row["fuel"], "perc": row["perc"]})
        for bs, mix in _seed_slots.items():
            _current_slot_mix[bs] = mix
        if _seed_slots:
            logger.info(
                "engine_startup: seeded _current_slot_mix with %d slots from DB",
                len(_seed_slots)
            )
            # Also backfill mix_history from generation_mix if mix_history is sparse
            try:
                mh_count = _store._conn.execute("SELECT COUNT(*) FROM mix_history").fetchone()[0]
                if mh_count == 0:
                    logger.info("engine_startup: backfilling mix_history from generation_mix")
                    for bs, mix in _seed_slots.items():
                        _store.upsert_mix_history(bs, mix)
            except Exception as _mh_err:
                logger.warning("engine_startup: mix_history backfill failed: %s", _mh_err)
    except Exception as _seed_err:
        logger.warning("engine_startup: could not seed slot mix from DB: %s", _seed_err)

    # ── Fire CI tick immediately so mix data is available for next finalise ─
    # If seed was empty (fresh install or long downtime), fetch mix now rather
    # than waiting up to 30 minutes for the scheduled CI tick.
    if not _current_slot_mix:
        try:
            await _tick_carbon_intensity()
            logger.info("engine_startup: immediate CI tick fired (no seed data)")
        except Exception as _ci_err:
            logger.warning("engine_startup: immediate CI tick failed: %s", _ci_err)

    # ── Startup charts ───────────────────────────────────────────────────
    generate_charts(_store)
    logger.info("engine_startup: complete")