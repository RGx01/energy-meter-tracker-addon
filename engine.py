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

CHART_DIR          = "/data/energy_meter_tracker"   # accessible from HA /local/
BLOCK_MINUTES      = 30  # default — overridden at runtime from config

# ─────────────────────────────────────────────────────────────────────────────
# Module-level state
# ─────────────────────────────────────────────────────────────────────────────

_read_queue:               list         = []
_last_known_sensor_values: dict         = {}
_engine_loop_lock:         asyncio.Lock = None   # initialised in setup()
_engine_paused:            bool         = False


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


# ─────────────────────────────────────────────────────────────────────────────
# IO helpers
# ─────────────────────────────────────────────────────────────────────────────

def io_save(path: str, data):
    save_json_atomic(path, data)


def append_block(block: dict):
    get_store().append_block(block)


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
    Load meter configuration from the normalised DB tables.
    Falls back to meters_config.json only before the DB is open (startup)
    or if no config period exists yet (fresh install).
    """
    global _store
    if _store is not None:
        try:
            cp = _store._conn.execute(
                "SELECT id FROM config_periods "
                "WHERE effective_to IS NULL ORDER BY effective_from DESC LIMIT 1"
            ).fetchone()
            if cp:
                return _store.config_from_db(cp["id"])
        except Exception as _e:
            logger.warning("load_config: DB read failed, falling back to file: %s", _e)
    # Pre-startup fallback — DB not open yet or fresh install
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


def set_gap_marker(block: dict, pre_reads: dict, last_known_rates: dict):
    block["_gap_marker"] = {
        "detected_at":      datetime.now(timezone.utc).replace(tzinfo=None).isoformat(),
        "pre_reads":        pre_reads,
        "last_known_rates": last_known_rates,
    }
    logger.info("set_gap_marker: stored at %s", block["_gap_marker"]["detected_at"])


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

def ensure_correct_block(ha: HAClient, current_block: dict, now: datetime) -> dict:
    start, end = get_block_window(now, block_minutes=int(get_block_minutes()))

    if not current_block or not current_block.get("start"):
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

    finalise_block(ha, block_data=current_block)

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

    for meter_id, meter_cfg in config.get("meters", {}).items():
        meter_block = block["meters"].setdefault(
            meter_id, {"meta": {}, "channels": {}, "interpolated": False}
        )
        meter_block["meta"] = meter_cfg.get("meta", {})

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

    return {
        "kwh":        total_kwh,
        "rate":       corrected_rates[-1]["value"],
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
            meter_block = {
                "channels": {}, "meta": meter_meta,
                "interpolated": True, "standing_charge": 0.0,
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
                            skip_reason = f"possible reset ({pre_read['value']} → {post_read['value']})"
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
                    meter_block["channels"][channel_name] = {
                        "kwh": 0.0, "rate": 0.0, "cost": 0.0,
                        "read_start": 0.0, "read_end": 0.0, "interpolated": True,
                    }
                    continue

                opener = interpolate_value(pre_read, post_read, window_start)
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

    # Both charts need all blocks for now — phase 2 of optimisation will
    # push date-range queries into the charting functions themselves.
    # This is still a significant win: the store is queried once and both
    # charts share the same list, vs the old approach of loading the full
    # JSON file twice (once per chart call site).
    blocks = store.get_all_blocks()

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

def finalise_block(ha: HAClient, block_data: dict | None = None, interpolated: bool = False):
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

            reads = channel.get("reads", [])

            if reads and not is_sub:
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

        block["meters"][meter_name] = meter_block

        if meter_meta.get("sub_meter") and parent_name:
            sub_import = meter_block["channels"].get("import")
            if sub_import:
                parent_sub_kwh[parent_name]  = parent_sub_kwh.get(parent_name, 0.0)  + sub_import["kwh"]
                parent_sub_cost[parent_name] = parent_sub_cost.get(parent_name, 0.0) + sub_import["cost"]

    # ── PASS 2 — grid-authoritative sub-meter distribution ────────────────
    for parent_meter_name, sub_kwh_total in parent_sub_kwh.items():
        parent_block  = block["meters"].get(parent_meter_name)
        if not parent_block:
            continue
        parent_import = parent_block["channels"].get("import")
        if not parent_import:
            continue

        grid_kwh      = parent_import.get("kwh", 0.0)
        parent_rate   = parent_import.get("rate", 0.0)
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
            if meta.get("v2x_capable") and delta < 0:
                logger.info("PASS 2: %s discharging %.4f kWh (V2X), excluded", meter_name, abs(delta))
                continue
            if delta == 0.0:
                continue
            entry = {
                "meter_name": meter_name, "meter_block": meter_block,
                "sub_import": sub_import, "kwh": delta,
            }
            (protected if not meta.get("inverter_possible", False) else unprotected).append(entry)

        protected.sort(key=lambda x: x["kwh"], reverse=True)
        unprotected.sort(key=lambda x: x["kwh"], reverse=True)

        for entry in protected:
            claimed = min(entry["kwh"], grid_remaining)
            if claimed < entry["kwh"]:
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
            claimed        = min(entry["kwh"], grid_remaining)
            battery        = entry["kwh"] - claimed
            grid_remaining = max(grid_remaining - claimed, 0.0)
            entry["sub_import"]["kwh_grid"]    = claimed
            entry["sub_import"]["kwh_battery"] = battery
            entry["sub_import"]["cost"]        = round(claimed * parent_rate, 6)
            logger.info(
                "PASS 2: %s unprotected  grid=%.4f  battery=%.4f",
                entry["meter_name"], claimed, battery,
            )

        remainder_kwh  = max(grid_remaining, 0.0)
        remainder_cost = round(remainder_kwh * parent_rate, 6)
        parent_import["kwh_total"]     = grid_kwh
        parent_import["kwh_remainder"] = remainder_kwh
        parent_import["cost_remainder"] = remainder_cost
        parent_import["rate_used"]     = parent_rate
        logger.info(
            "PASS 2: %s  grid=%.4f kWh  remainder=%.4f kWh",
            parent_meter_name, grid_kwh, remainder_kwh,
        )

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

    append_block(block)

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

    # Deferred gap filling
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

            pre_ts = None
            for meter_reads in pre_reads.values():
                for read in meter_reads.values():
                    if isinstance(read, dict) and read.get("ts"):
                        if not pre_ts or read["ts"] > pre_ts:
                            pre_ts = read["ts"]

            missing_windows = detect_gap(pre_ts, now, block_minutes=block_minutes)

            if missing_windows:
                config     = load_config()
                gap_blocks = build_gap_blocks(
                    missing_windows, pre_reads, post_reads, last_rates, config
                )
                for gb in gap_blocks:
                    append_block(gb)

                engine_totals = _store.get_cumulative_totals()
                await update_ha_sensors(ha, engine_totals)
                logger.info("gap fill: %d interpolated blocks inserted", len(gap_blocks))
            else:
                logger.warning("gap fill: no missing windows found, clearing marker")

            clear_gap_marker(current_block)
            _store.save_current_block(current_block)

    # Block lifecycle
    updated_block = ensure_correct_block(ha, current_block, now)
    block_changed = updated_block.get("start") != current_block.get("start")

    if block_changed or periodic_checkpoint or near_boundary:
        updated_block["_last_checkpoint"] = now.isoformat()
        _store.save_current_block(updated_block)


# ─────────────────────────────────────────────────────────────────────────────
# Startup  (replaces @time_trigger("startup"))
# ─────────────────────────────────────────────────────────────────────────────

async def engine_startup(ha: HAClient):
    """
    Run once when the add-on starts.
    Registers state triggers, detects session gaps, generates startup charts.
    Replaces @time_trigger("startup").
    """
    config = load_config()

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

    # Pre-load sensor states into ha_client cache
    sensors_to_preload = []
    for mcfg in config.get("meters", {}).values():
        for ccfg in mcfg.get("channels", {}).values():
            for key in ("read", "rate", "standing_charge_sensor"):
                eid = ccfg.get(key)
                if eid:
                    sensors_to_preload.append(eid)
    if sensors_to_preload:
        await ha.preload_states(sensors_to_preload)

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

    # ── Open BlockStore (auto-migrate from blocks.json if needed) ────────
    global _store
    _store = open_block_store(BLOCKS_DB_PATH)

    # Migrate full_config_json → normalised tables (2.0→2.1 one-time upgrade)
    try:
        migrated = _store.migrate_full_config_json()
        if migrated:
            logger.info(
                "engine_startup: migrated %d config periods to normalised schema", migrated
            )
    except Exception as _me:
        logger.warning("engine_startup: full_config_json migration failed: %s", _me)

    # Sync startup config (possibly with freshly detected currency) to DB.
    # Rewrites the normalised meter rows for the active period — handles
    # both 2.0→2.1 upgrades (meters table was empty) and currency detection.
    try:
        active_id = _store.get_current_config_period_id()
        if active_id is not None:
            main_meta_sync = {}
            for _m in config.get("meters", {}).values():
                if not (_m.get("meta") or {}).get("sub_meter"):
                    main_meta_sync = _m.get("meta") or {}
                    break
            with _store._conn:
                _store._conn.execute(
                    """UPDATE config_periods
                       SET billing_day     = ?,
                           block_minutes   = ?,
                           timezone        = ?,
                           currency_symbol = ?,
                           currency_code   = ?,
                           site_name       = ?,
                           supplier        = ?
                       WHERE id = ?""",
                    (
                        int(main_meta_sync.get("billing_day") or 1),
                        int(main_meta_sync.get("block_minutes") or 30),
                        main_meta_sync.get("timezone", "UTC"),
                        main_meta_sync.get("currency_symbol", "£"),
                        main_meta_sync.get("currency_code", "GBP"),
                        main_meta_sync.get("site"),
                        main_meta_sync.get("supplier"),
                        active_id,
                    )
                )
                _store._write_meters(config, active_id)
            logger.info("engine_startup: active config period synced to normalised tables")
    except Exception as _sync_e:
        logger.warning("engine_startup: config sync to DB failed: %s", _sync_e)

    if _store.get_current_config_period_id() is None:
        # Fresh DB — check if blocks.json exists to migrate
        if os.path.exists(BLOCKS_PATH):
            logger.info("engine_startup: blocks.json found — running auto-migration to SQLite")
            migrated = migrate_json_to_sqlite(BLOCKS_PATH, _store, config)
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
            migrated = migrate_json_to_sqlite(BLOCKS_PATH + ".migrated", _store, config)
            logger.info("engine_startup: re-migration complete — %d blocks migrated", migrated)
        else:
            # Brand new install — create initial config period
            _store.insert_config_period(config)
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
    last_block = _store.get_last_block()
    if last_block:
        last_block_end = last_block.get("end")
        if last_block_end:
            missing_windows = detect_gap(last_block_end, datetime.now(timezone.utc).replace(tzinfo=None))
            if missing_windows:
                logger.warning(
                    "engine_startup: session gap detected — %d missing blocks", len(missing_windows)
                )
                current_block        = _store.load_current_block()
                pre_reads, last_rates = extract_last_reads(last_block)
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
                set_gap_marker(current_block, pre_reads, last_rates)
                _store.save_current_block(current_block)
                logger.info("engine_startup: gap marker set, stale reads cleared, will fill on first capture")
            else:
                logger.info("engine_startup: no session gap detected")

    # ── Startup charts ───────────────────────────────────────────────────
    generate_charts(_store)
    logger.info("engine_startup: complete")