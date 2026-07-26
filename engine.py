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
# API credentials are stored in their OWN file (not the DB, not config.yaml) so
# they are never swept into the DB backups. Entered in-app, read at startup.
KRAKEN_CREDS_PATH  = f"{DATA_DIR}/kraken_credentials.json"
# current_block.json removed in 2.1.0 — state stored in DB current_block table
BLOCKS_PATH        = f"{DATA_DIR}/blocks.json"    # read-only: used only for one-time migration on startup
BLOCKS_DB_PATH     = f"{DATA_DIR}/blocks.db"
# cumulative_totals.json removed in 2.1.0 — totals derived from blocks table

import os as _os_engine
# BL-5: the supervised backup dir is namespaced by site slug once the DB is open
# (see instance.py). Until then this holds the legacy/standalone default so any
# pre-startup use is still valid. `resolve_backup_dir_for_site()` rebinds it.
SHARE_BACKUP_DIR   = (
    _os_engine.path.join(DATA_DIR, "backup")
    if _os_engine.environ.get("EMT_MODE") == "standalone"
    else "/share/energy_meter_tracker_backup"
)

# BL-5: set at startup from the data lineage check. Read by the server to surface
# a dismissible "restored from another instance" notice. Never gates anything.
FOREIGN_RESTORE_NOTICE = {"foreign": False, "db_uuid": None, "acknowledged": False}


def resolve_backup_dir_for_site(site_name: str | None) -> str:
    """BL-5: rebind SHARE_BACKUP_DIR for this site, migrating/adopting as needed.

    Called at startup (and after a site rename) once the config period — and
    therefore the site name — is known. Standalone is unaffected: its backup dir
    is volume-scoped and site-independent. Silent in all cases.
    """
    global SHARE_BACKUP_DIR
    try:
        import instance as _inst
        # Only pass `current_dir` when it is a directory THIS instance owns — it
        # is used to move our backups on a site rename. At startup the constant
        # still holds the legacy path, which we do not own until adopted; passing
        # it here made the rename path try to claim another instance's backups.
        current = None
        if _os_engine.path.isdir(SHARE_BACKUP_DIR):
            iid = _inst.get_instance_id()
            if _inst._owned_by_us(SHARE_BACKUP_DIR, iid):
                current = SHARE_BACKUP_DIR
        SHARE_BACKUP_DIR = _inst.resolve_backup_dir(site_name, current_dir=current)
    except Exception as e:
        logger.warning("resolve_backup_dir_for_site: failed (%s); keeping %s",
                       e, SHARE_BACKUP_DIR)
    return SHARE_BACKUP_DIR

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
_engine_loop_lock:         asyncio.Lock = None   # initialised in setup()
_engine_paused:            bool         = False
_last_ci_fetch:            datetime | None = None   # UTC — last carbon intensity fetch
_last_dispatch_capture:    datetime | None = None   # UTC — last dispatch-slot capture (5-min cadence)
_last_reconcile:           datetime | None = None   # UTC — last dispatch reconciliation (hourly cadence)
_current_slot_mix:         dict            = {}      # captured_at → generationmix list
_meter_reset_detected:     bool            = False   # set when post-gap read < pre-gap read (possible meter replacement)

# 3.0.0: optional block-boundary callback. The Kraken Mini ingester (Chunk 7)
# registers here so it can take a Mini register reading at each boundary. None
# until registered; firing is a guarded no-op otherwise. The full Mini read
# logic lands in Chunk 7 — this is the hook it plugs into.
_on_block_boundary_cb = None   # Optional[Callable[[str], None]]

# 3.0.0 Kraken integration runtime state. Populated at startup by
# _kraken_startup_discovery() when an API key is configured. The ingester
# task (4b-ii) reads _kraken_discovery for the verified meter identifiers.
# None/empty until then; the presence of _kraken_client gates all API work.
_kraken_client = None          # KrakenAPIClient | None
_kraken_discovery: dict | None = None
_kraken_account_number: str | None = None
# Reference to the running HAClient + poll-task handle, captured when the engine
# loop starts. Lets connect_kraken_now() (the in-app credential-save path)
# launch the DCC poll task without an addon restart — the task is otherwise
# started once at boot and exits permanently if the API wasn't yet configured.
_engine_ha = None              # HAClient | None
_kraken_poll_task_handle = None  # asyncio.Task | None
# True once the first (cold) engine_startup has completed in this process. The
# rogue-total guard (clearing the in-progress block's carried reads) is valid
# ONLY on a cold start, where those reads are stale leftovers from a previous
# container. Every later engine_startup in the SAME process — HA reconnect or
# config-save — is "warm": the process never stopped, the in-progress reads are
# LIVE, and clearing them would discard real consumption (observed: a Mini block
# lost ~3.4 kWh during an HA-upgrade reconnect storm). A new process resets this
# to False, so a genuine container restart is correctly treated as cold.
_cold_start_complete = False
# Result of the last third-party HA integration detection (BCD live-power
# offload source, OHME charge-mode smart-vs-boost signal). Populated by
# _detect_and_log_integrations() each startup; consulted by the overlay (OHME
# verified path) and the live-power tile (BCD offload). None until first run.
_detected_integrations: dict | None = None
# 4b-ii: ingester + scheduling. dry-run is HARDCODED here (4b choice); the
# flip to live writes is a deliberate later step. Poll every 6h (DCC settles
# ~daily, so frequent polling adds nothing). backfill cap is generous; the real
# window is MIN(block_start)→now via get_oldest_block_start().
_kraken_ingester = None         # KrakenIngester | None
_KRAKEN_POLL_INTERVAL_S = 6 * 3600
# 4b-iii: LIVE. The poll now writes DCC settlement into blocks (imp_kwh_api /
# exp_kwh_api + needs_pass2_rerun), which the engine drain re-runs into billing.
# A one-time blocks.db snapshot is taken before the first live write as a
# rollback point (see _kraken_pre_live_snapshot).
_KRAKEN_DRY_RUN = False
_KRAKEN_BACKFILL_CAP_DAYS = 400
_KRAKEN_SNAPSHOT_DONE_KEY = "pre_live_snapshot_done"
# Auto settlement sweep: the 6h incremental poll only re-checks a sliding window
# (last-seen − 6h → now). When DCC settles intervals out of order or later than
# that overlap (export commonly lags import by days), an earlier interval can age
# out behind the cursor and never be re-requested by the incremental poll. A
# daily sweep re-fetches oldest-unsettled → now (advance_cursor=False) so those
# gaps self-heal. Bounded to a horizon: beyond it, DCC settlement is unlikely and
# the provisional value stands (also caps the sweep window cost).
_SETTLEMENT_SWEEP_HORIZON_DAYS = 14
_SETTLEMENT_SWEEP_MIN_INTERVAL_S = 24 * 3600   # run at most once per day
_STATE_LAST_SWEEP = "last_settlement_sweep_utc"
# BL-19: run-once marker for the historical grid-invariant repair sweep.
_BL19_GRID_SWEEP_DONE_KEY = "bl19_grid_invariant_sweep_done"
# #307: run-once marker for the lost-opener device-spike self-heal. The write-point
# guard prevents NEW spikes, so this only needs to clean pre-guard history once.
_SPIKE_SWEEP_DONE_KEY = "implausible_sub_block_sweep_done"
# Chunk 5: cached rate schedules for reconcile-time rate repair. Built once per
# poll cycle by the (async) poll task; read by the (sync) drain via the resolver
# below. Rates stored as £/kWh (converted from the API's pence at build time).
_kraken_rate_schedules: dict = {}   # {"import": RateSchedule, "export": RateSchedule}
# Standing-charge schedule (single, import-side — the daily standing charge).
# Stored as £/day after conversion at resolve time, like rates.
_kraken_standing_schedule = None    # RateSchedule | None
_rate_schedule_unsupported: dict | None = None  # set when an active tariff returns
#   NO standard unit rates — the new IOG time-of-use / 6-hour-cap tariff signature.
#   Surfaced so a migrated user sees a clear "not yet supported" state, not £0 bills.
_kraken_mini_reader = None          # MiniBoundaryReader | None — Chunk 7
# Latest Mini live demand, polled server-side by the engine tick (page-independent)
# so the 48h history fills continuously and the gauge reads it without its own
# GraphQL call. {"kw": float|None, "ts": monotonic seconds, "wall": epoch seconds}
_last_mini_demand = {"kw": None, "ts": 0.0, "wall": 0.0}
_MINI_POLL_GAP = 55.0               # seconds between Mini demand polls (~1/min)
# Start collecting Mini telemetry this many seconds BEFORE the block boundary
# (per spec: 20s lead), continuing until the post-boundary read is bracketed.
_MINI_COLLECT_LEAD_S = 20


_BILLING_SOURCE_KEY = "billing_source"
_VALID_BILLING_SOURCES = ("dcc", "cad")

_MODE_KEY = "data_source_mode"
_VALID_MODES = ("cad", "cad+api", "api", "api+mini")


def get_data_source_mode() -> str:
    """Current data-source mode, stored in kraken_state. One of:
    'unset'    — no mode chosen yet (fresh/flattened DB, pre-survey). NOTHING
                 should auto-activate (esp. the API) in this state.
    'cad'      — local meter reads only, no supplier API
    'cad+api'  — local reads + DCC settlement/rates
    'api'      — no local reads; DCC is the import source
    'api+mini' — no local reads; Mini provisional + DCC settlement
    Returns 'unset' when nothing is stored — deliberately NOT defaulting to
    'cad', so a flat DB (which has chosen nothing) is distinguishable from a
    user who actually chose cad. The survey sets the mode explicitly."""
    try:
        val = _store.get_kraken_state(_MODE_KEY)
    except Exception:
        val = None
    return val if val in _VALID_MODES else "unset"


def is_mode_configured() -> bool:
    """True once the survey has explicitly set a data-source mode."""
    return get_data_source_mode() in _VALID_MODES


def set_data_source_mode(mode: str) -> str:
    """Persist the data-source mode. Returns the stored value. Raises ValueError
    on an unknown mode. The mode is derived by the setup survey and drives
    sensor collection, API requirement, and Mini activation."""
    mode = (mode or "").lower()
    if mode not in _VALID_MODES:
        raise ValueError(f"mode must be one of {_VALID_MODES}, got {mode!r}")
    _store.set_kraken_state(_MODE_KEY, mode)
    logger.info("set_data_source_mode: %s", mode)
    return mode


def _detect_upgrade_mode(config: dict) -> str | None:
    """Upgrade bridge: decide the mode for an install that has no stored mode.

    2.x installs predate the data-source-mode concept, so on first 3.0.0 boot
    get_data_source_mode() returns 'unset'. We must NOT leave an existing,
    working user in 'unset' (which disables everything pending the survey), and
    we must NOT infer or auto-activate the API. The conservative rule:

      no stored mode AND a non-sub (main) meter has a configured import read
      sensor (channels.import.read set)  →  this is an existing CAD user;
      silently set mode 'cad' and carry on exactly as before.

    A genuinely fresh/flattened install (no import sensor configured) is left
    'unset' so the setup survey runs. Returns the mode it set, or None if it
    made no change (already had a mode, or fresh install).

    Because 'cad' makes mode_uses_api() False, this also neutralises any stale
    credentials file left in /data on upgrade — nothing reads it under cad.
    """
    if get_data_source_mode() != "unset":
        return None  # mode already chosen (survey, or a prior 3.0.0 boot)
    has_import_sensor = any(
        ((meter.get("channels") or {}).get("import") or {}).get("read")
        for meter in (config.get("meters") or {}).values()
        if not (meter.get("meta") or {}).get("sub_meter")
    )
    if not has_import_sensor:
        return None  # fresh install — leave 'unset' for the survey
    set_data_source_mode("cad")
    logger.info(
        "engine_startup: upgrade detected — existing import sensor present and "
        "no stored mode; preserving as 'cad' (survey not required, API path off)"
    )
    return "cad"


def mode_uses_api(mode: str = None) -> bool:
    """True only if an explicitly-set mode involves the supplier API
    (cad+api / api / api+mini). 'unset' → False (don't auto-activate API)."""
    m = mode or get_data_source_mode()
    return m in _VALID_MODES and "api" in m


def mode_uses_mini(mode: str = None) -> bool:
    """True only for api+mini."""
    return (mode or get_data_source_mode()) == "api+mini"


# ── Supplier capability (server-side gating seam) ────────────────────────────
# Mirrors the client WIZ_SUPPLIERS registry. The supplier gate decides whether
# an API-backed mode is permitted at all. Adding a future Kraken-platform
# supplier = add its key here (+ eventually a profile). This is the authoritative
# server-side check: the UI gating is convenience, this is enforcement.
_API_CAPABLE_SUPPLIERS = frozenset({"octopus"})


def normalize_supplier(supplier: str) -> str:
    """Normalise a stored/submitted supplier (possibly legacy free-text like
    'Octopus Energy', or empty for a v2 user) to a registry key. Empty → ''.
    Any non-Octopus free-text collapses to 'not-listed' (the local-only
    migration default), so legacy configs map cleanly onto the gating model."""
    v = (supplier or "").strip().lower()
    if not v:
        return ""
    if "octopus" in v:
        return "octopus"
    if v in _API_CAPABLE_SUPPLIERS or v == "not-listed":
        return v
    return "not-listed"


def supplier_is_api_capable(supplier: str) -> bool:
    """True if the (normalised) supplier supports an API-backed mode."""
    return normalize_supplier(supplier) in _API_CAPABLE_SUPPLIERS


def _get_billing_source() -> str:
    """Current global billing source ('dcc' default, or 'cad').

    Stored in kraken_state so it persists and is readable by the sync drain.
    Defaults to 'dcc' (the validated default — settle to DCC except where
    missing).
    """
    try:
        val = _store.get_kraken_state(_BILLING_SOURCE_KEY)
    except Exception:
        val = None
    return val if val in _VALID_BILLING_SOURCES else "dcc"


def apply_billing_source_change(new_source: str,
                                main_meter_id: str = "electricity_main") -> dict:
    """Set the global billing source and, if it changed, flag all main-meter
    blocks for PASS 2 re-run so the drain re-materialises billing to the new
    source.

    Returns {"changed": bool, "source": str, "flagged": int}. The caller (config
    UI) shows the "recalculation takes time" warning when changed and flagged>0.
    Idempotent: setting the same source is a no-op (no re-flag, no churn).
    """
    new_source = (new_source or "").lower()
    if new_source not in _VALID_BILLING_SOURCES:
        raise ValueError(f"billing_source must be one of {_VALID_BILLING_SOURCES}, "
                         f"got {new_source!r}")
    current = _get_billing_source()
    if new_source == current:
        return {"changed": False, "source": current, "flagged": 0}
    _store.set_kraken_state(_BILLING_SOURCE_KEY, new_source)
    flagged = _store.flag_all_for_pass2_rerun(main_meter_id)
    logger.info("apply_billing_source_change: %s → %s, flagged %d blocks for "
                "re-run (drain will re-materialise billing)",
                current, new_source, flagged)
    return {"changed": True, "source": new_source, "flagged": flagged}


_DISPATCH_OVERLAY_APPLY = True    # live: overlay applies off-peak to validated slots

# Meter-validation noise floor (kWh). A smart-charge slot is credited off-peak
# only if the block drew at least this much import — a PRESENCE check (did real
# charging happen?), NOT a comparison to the dispatch delta. Block draw is
# car + household baseload; dispatch delta is car-only, so comparing them is
# wrong. 0.1 kWh rejects sensor noise / tiny baseload trickle while any genuine
# EV charge clears it with large margin (live data: charging slots 2–6 kWh,
# idle slots ~0.0). Confirmed against real slot data.
_DISPATCH_OVERLAY_MIN_KWH = 0.1


def _dispatch_overlay_rate(channel_name: str, block_start: str,
                           base_rate: float, imp_kwh: float | None):
    """Dispatch overlay (step 2 piece 2). Given a block's base-resolved import
    rate, decide whether an Intelligent smart-charge dispatch should override it
    to the off-peak rate. Returns the rate to use (£/kWh).

    Rules (design §1c):
      - Import channel only (export is never dispatch-adjusted).
      - The block's 30-min slot must be a persisted smart-charge dispatch_slot.
      - METER VALIDATION: the block must have drawn at least _DISPATCH_OVERLAY_MIN_KWH
        of import (0.1 kWh noise floor). A PRESENCE check — did real charging
        happen? — NOT a comparison to the dispatch delta. A slot with no real
        draw is NOT overridden (the over-reporting guard — Zappi/IOG dispatch
        slots stay 'on' after charging stops).
      - Only overrides when base_rate is ABOVE the off-peak rate (i.e. the slot
        is out-of-window / peak). In-window slots are already off-peak — no-op.

    COMPUTE-AND-LOG: when _DISPATCH_OVERLAY_APPLY is False, the override is
    computed and LOGGED but NOT applied (returns base_rate unchanged), so it can
    be validated against real data before affecting any bill. Idempotent: the
    decision is a pure function of (base schedule, dispatch_slots, meter draw).
    """
    if channel_name != "import" or _store is None or not block_start:
        return base_rate
    try:
        slot = _store.get_dispatch_slot(_snap_to_slot(block_start))
    except Exception:
        return base_rate
    if not slot or not slot.get("off_peak"):
        return base_rate
    # Per-decision config fingerprint (design open-item 2): makes every overlay
    # decision self-describing in the log, so a user's bug report carries the
    # config context (mode, mini, provider, source, apply-state) without a
    # round-trip. NOTE mini=active reports the ACTUAL runtime reader state
    # (_kraken_mini_reader), not the nominal mode: in plain `api` mode a Mini
    # device elevates to provisional collection at runtime, so mode=api can
    # legitimately run with mini=active — and the Mini is what supplies the
    # provisional kWh this guard validates against.
    _fp = ("mode=%s mini=%s provider=%s source=%s apply=%s"
           % (get_data_source_mode(),
              "active" if _kraken_mini_reader is not None else "off",
              slot.get("provider"), slot.get("source"),
              _DISPATCH_OVERLAY_APPLY))
    # Meter validation: only credit slots that actually drew power. PRESENCE
    # check against the block's CURRENT kWh (provisional at finalise,
    # authoritative at settlement) — a 0.1 kWh noise floor, NOT a dispatch-delta
    # comparison. Below the floor → treat as no real charging (over-report guard).
    if not imp_kwh or imp_kwh < _DISPATCH_OVERLAY_MIN_KWH:
        logger.info("dispatch-overlay: slot %s active but draw below floor "
                    "(%.4f kWh < %.2f) — NOT overriding (over-report guard) [%s]",
                    block_start, imp_kwh or 0.0, _DISPATCH_OVERLAY_MIN_KWH, _fp)
        return base_rate
    sched = _kraken_rate_schedules.get("import")
    if sched is None or sched.is_empty():
        return base_rate
    off_peak_pence = sched.off_peak_rate_near(block_start)
    if off_peak_pence is None:
        return base_rate
    off_peak_rate = round(off_peak_pence / 100.0, 6)
    # Only override if base is meaningfully above off-peak (out-of-window slot).
    if base_rate <= off_peak_rate + 1e-9:
        return base_rate  # already off-peak (in-window) — no-op
    if _DISPATCH_OVERLAY_APPLY:
        logger.info("dispatch-overlay: APPLIED %s peak→off-peak %.5f→%.5f "
                    "(%.4f kWh) [%s]", block_start, base_rate,
                    off_peak_rate, imp_kwh, _fp)
        return off_peak_rate
    logger.info("dispatch-overlay: WOULD apply %s peak→off-peak %.5f→%.5f "
                "(%.4f kWh) [compute-and-log; not applied] [%s]",
                block_start, base_rate, off_peak_rate, imp_kwh, _fp)
    return base_rate


def _snap_to_slot(block_start: str) -> str:
    """Snap a block_start to its 30-min slot boundary ISO (matches capture)."""
    try:
        dt = datetime.fromisoformat(str(block_start).replace("Z", "").split("+")[0])
        dt = dt.replace(minute=(0 if dt.minute < 30 else 30),
                        second=0, microsecond=0)
        return dt.isoformat()
    except Exception:
        return block_start


def _log_config_state(config: dict | None = None) -> None:
    """Emit a one-shot config-state dump at startup (design open-item 2).

    On a self-hosted add-on the LOG is the only window into a user's setup, so
    "rely on user reporting" only works if a report carries the config context.
    This logs the OVERLAY-relevant config that is known synchronously at startup.
    Tariff/MPAN/account are logged separately by the Kraken discovery step when
    it completes (async), so they are intentionally not duplicated here.

    Reports only GROUNDED values. Detection features not yet built (BCD-detected
    live-tile offload, explicit rate-sensor override, OHME charge-mode sensor
    path) are marked as a seam rather than printed as fabricated values — the log
    must never claim a state the code can't actually determine.
    """
    cfg = config if config is not None else {}
    mode = get_data_source_mode()
    # Is a main import/export read sensor configured? (cad-side presence)
    has_import = has_export = False
    for mcfg in (cfg.get("meters") or {}).values():
        if (mcfg.get("meta") or {}).get("sub_meter"):
            continue
        ch = mcfg.get("channels") or {}
        if (ch.get("import") or {}).get("read"):
            has_import = True
        if (ch.get("export") or {}).get("read"):
            has_export = True
    try:
        billing_source = _get_billing_source()
    except Exception:
        billing_source = "?"
    # Mini at startup-dump time: the reader is wired LATER in startup (after
    # device discovery), so we cannot yet report a definitive active/off here.
    # Report ELIGIBILITY honestly instead — a definitive `mini_provisional=...`
    # line is emitted once _maybe_setup_mini() resolves. Reporting the nominal
    # mode (== api+mini) would be WRONG: a plain `api` mode elevates to Mini at
    # runtime when a device is present, so mode alone can't tell the truth.
    if not mode_uses_api(mode):
        mini_note = "off (no API mode)"
    elif has_import:
        mini_note = "off (local import sensor authoritative)"
    else:
        mini_note = "pending (API mode, no local sensor — will probe for device)"
    logger.info(
        "config-state: mode=%s uses_api=%s mini=%s billing_source=%s "
        "import_sensor=%s export_sensor=%s",
        mode, mode_uses_api(mode), mini_note, billing_source,
        has_import, has_export)
    logger.info(
        "config-state: dispatch_overlay apply=%s min_kwh=%.2f (overlay %s)",
        _DISPATCH_OVERLAY_APPLY, _DISPATCH_OVERLAY_MIN_KWH,
        "ACTIVE — dispatch capture + repricing runs" if mode_uses_api(mode)
        else "inactive — no API mode, dispatch path off")
    # BCD + OHME detection are reported by _detect_and_log_integrations() once
    # HA states have been fetched (a `config-state: detection ...` line, emitted
    # later in startup like mini_provisional). The remaining unbuilt feature is
    # the explicit rate-sensor override; mark only that as a seam here.
    logger.info(
        "config-state: rate-sensor-override pending — not yet implemented "
        "(bcd + ohme detection logged after HA state fetch)")


async def _detect_and_log_integrations(ha) -> dict:
    """Detect third-party HA integrations EMT can use and log the result.

    BCD (BottlecapDave Octopus): its `current_demand` sensor lets the live-power
    tile read off BCD instead of EMT polling the Mini (~60/hr saved). OHME: a
    charge-mode signal (official `ohme` select or dan-r binary) upgrades the
    optimistic off-peak default to VERIFIED smart-vs-boost. One REST round-trip,
    non-fatal; stored in _detected_integrations for the overlay + tile to read.
    Re-run each startup so a mid-life install is picked up.
    """
    global _detected_integrations
    from kraken_api_client import detect_bottlecapdave, detect_ohme_charge_mode
    bcd = {"found": False}
    ohme = {"found": False, "integration": None}
    try:
        states = await ha.get_all_states()
        bcd = detect_bottlecapdave(states or [])
        ohme = detect_ohme_charge_mode(states or [])
    except Exception as e:
        logger.warning("config-state: integration detection failed: %s", e)
    _detected_integrations = {"bcd": bcd, "ohme": ohme}

    # Preload the OHME charge-mode entity so the capture tick's get_state() has a
    # value before the entity's first state_changed event. After that, the WS
    # listener keeps it fresh automatically (it caches every state_changed).
    if ohme.get("found") and ohme.get("charge_mode_entity"):
        try:
            await ha.preload_states([ohme["charge_mode_entity"]])
        except Exception as e:
            logger.warning("config-state: ohme entity preload failed: %s", e)
    logger.info(
        "config-state: detection bcd=%s bcd_live_power=%s ohme_charge_mode=%s "
        "(integration=%s) ohme_path=%s",
        bool(bcd.get("found")),
        "yes" if bcd.get("demand_sensor") else "no",
        bool(ohme.get("found")),
        ohme.get("integration") or "none",
        "verified" if ohme.get("found") else "optimistic")
    return _detected_integrations


def _reseed_opener_after_short_restart(last_block: dict, current_block: dict) -> list:
    """Restore the in-progress block's opener after a within-block restart.

    On restart the rogue-total guard clears the in-progress block's reads. The
    block's channel structure is reconstructed FROM those reads, so once they're
    gone load_current_block() returns the block with meters={} — no channels at
    all. We therefore rebuild the opener by iterating LAST_block's channels (each
    carries the durable read_end = this block's true opening register) and writing
    a single seed read into current_block, CREATING the meter/channel as needed.

    Without this the block re-seeds from a late post-restart read and UNDER-COUNTS,
    measuring only the tail (observed live: Mini blocks finalising at ~0.23 kWh
    when ~3.5 kWh really flowed, because each restart dropped the opener).

    Safe because:
      - acts only when last_block.end == current_block.start (contiguous — the
        no-gap case; gap-fill owns seeding when there's a gap, so we return []);
      - seeds a channel only when current_block has no live reads for it (never
        clobbers reads that survived a warm restart);
      - read_end is the durable finalised register, not a volatile in-block read,
        so it cannot reintroduce a rogue total.

    Mutates current_block in place; returns human-readable seeded descriptors
    (empty list if nothing seeded). Caller persists + logs.
    """
    if not last_block or not current_block:
        return []
    # Contiguity gate: only when the current block immediately follows last_block.
    if (last_block.get("end") or None) != (current_block.get("start") or None):
        return []
    seed_ts = current_block.get("start")
    if not seed_ts:
        return []
    reseeded = []
    cur_meters = current_block.setdefault("meters", {})
    for mn, lb_m in (last_block.get("meters") or {}).items():
        for cn, lb_ch in (lb_m.get("channels") or {}).items():
            read_end = lb_ch.get("read_end")
            if read_end is None:
                continue
            cur_ch = (cur_meters.setdefault(mn, {})
                                 .setdefault("channels", {})
                                 .setdefault(cn, {}))
            if cur_ch.get("reads"):
                continue  # live reads survived — never clobber
            cur_ch["reads"] = [{"ts": seed_ts, "value": float(read_end)}]
            reseeded.append("%s/%s=%.3f" % (mn, cn, float(read_end)))
    return reseeded


def _kraken_rate_resolver(channel_name: str, block_start: str):
    """Sync resolver the drain passes to the re-run. Returns £/kWh or None.

    Reads the cached schedule for the channel and resolves the rate at
    block_start, converting the API's pence/kWh to £/kWh. Pure/in-memory — no
    network. Returns None when no schedule or no covering period exists, so the
    re-run leaves the block flagged for the tooling rather than guessing.
    """
    sched = _kraken_rate_schedules.get(channel_name)
    if sched is None or sched.is_empty():
        return None
    pence = sched.resolve(block_start)
    if pence is None:
        return None
    return round(pence / 100.0, 6)   # pence/kWh → £/kWh


def _kraken_standing_resolver(block_start: str):
    """Resolve the standing charge (£/day) at block_start from the cached
    schedule, or None. pence/day → £/day. Pure/in-memory."""
    sched = _kraken_standing_schedule
    if sched is None or sched.is_empty():
        return None
    pence = sched.resolve(block_start)
    if pence is None:
        return None
    return round(pence / 100.0, 6)


def register_block_boundary_callback(cb) -> None:
    """Register a callback fired once per finalised block boundary.

    cb receives the boundary timestamp (the block_end ISO string). Pass None
    to clear. Exceptions raised by the callback are caught and logged so a
    failing ingester can never break finalisation.
    """
    global _on_block_boundary_cb
    _on_block_boundary_cb = cb


def _fire_block_boundary(boundary_time: str) -> None:
    """Invoke the registered boundary callback, swallowing any error."""
    cb = _on_block_boundary_cb
    if cb is None:
        return
    try:
        cb(boundary_time)
    except Exception as e:
        logger.warning("_fire_block_boundary: callback failed: %s", e)


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

_backup_in_progress = False


def _backup_to_share():
    """Backup data files to SHARE_BACKUP_DIR after each block finalise — OFF the
    event loop.

    Root cause of the ~every-30-min "restart" (diagnosed 2026-07): this ran
    SYNCHRONOUSLY on the finalise path and `store.backup()` copies the whole DB to
    /share (frequently a slow network mount). At tens of MB it took ~40s, stalling
    the asyncio loop — so the HA WebSocket heartbeat timed out, HA dropped the
    connection, and the reconnect re-ran the full engine_startup. It only LOOKED
    like a restart.

    Now it runs in a worker thread with its OWN read connection: the engine's
    `self._conn` is bound to the loop thread and SQLite forbids cross-thread use,
    while WAL lets a separate reader snapshot the DB consistently while the engine
    keeps writing. An overlap guard skips a block if the previous (slow) backup is
    still going, so a slow /share can't pile up backups."""
    global _backup_in_progress
    if _backup_in_progress:
        logger.info("_backup_to_share: previous backup still running — skipping")
        return
    try:
        db_path = get_store()._path
    except Exception:
        return

    def _work():
        global _backup_in_progress
        import sqlite3 as _sqlite
        try:
            ensure_dir(SHARE_BACKUP_DIR)
            src = _sqlite.connect(db_path)          # fresh connection in THIS thread
            try:
                dst = _sqlite.connect(f"{SHARE_BACKUP_DIR}/blocks.db")
                try:
                    src.backup(dst, pages=256, sleep=0.02)
                finally:
                    dst.close()
            finally:
                src.close()
            for filename in ("meters_config.json",):  # current_block state is in DB
                src_path = f"{DATA_DIR}/{filename}"
                if os.path.exists(src_path):
                    shutil.copy2(src_path, f"{SHARE_BACKUP_DIR}/{filename}")
            logger.info("_backup_to_share: backup written to %s", SHARE_BACKUP_DIR)
        except Exception as e:
            logger.warning("_backup_to_share: failed: %s", e)
        finally:
            _backup_in_progress = False

    _backup_in_progress = True
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None
    if loop is not None:
        loop.run_in_executor(None, _work)           # off the loop — never stalls the tick
    else:
        _work()                                     # no loop (sync/test) — inline


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


def create_block(start: datetime, end: datetime, block_minutes: int = BLOCK_MINUTES,
                 seed_meters: bool = False) -> dict:
    block = {
        "start":         iso(start),
        "end":           iso(end),
        "block_minutes": block_minutes,
        "meters":        {},
        "interpolated":  False,
    }
    # In API/Mini mode there are no local read sensors, so capture_samples adds
    # no reads — but the block still needs a meter SHELL (meter present, meta
    # set, empty channels) so finalise_block doesn't bail with "nothing to
    # finalise" and the Mini boundary read / DCC settlement have a block to
    # attach to. Seed it from config.
    if seed_meters:
        try:
            cfg = load_config()
            for meter_id, meter_cfg in (cfg.get("meters") or {}).items():
                meta = meter_cfg.get("meta", {}) or {}
                # Skip retired sub-meters.
                if meta.get("sub_meter") and meta.get("retired_at"):
                    continue
                channels = {}
                for channel_id in (meter_cfg.get("channels") or {}):
                    channels[channel_id] = {"reads": [], "rates": []}
                block["meters"][meter_id] = {
                    "meta": meta, "channels": channels, "interpolated": False,
                }
        except Exception as e:
            logger.warning("create_block: seed_meters failed: %s", e)
    return block


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
        # API/Mini mode has no local read sensor by design — blocks are
        # populated by the Mini boundary reader (provisional) and DCC
        # settlement. Block formation must still proceed once an API mode is
        # configured, with a seeded meter shell so the block can finalise and
        # fire the boundary. (CAD without sensors stays pre-wizard: no block.)
        api_mode = False
        try:
            api_mode = mode_uses_api()
        except Exception:
            api_mode = False
        if not has_sensors and not api_mode:
            return current_block  # pre-wizard / CAD-without-sensors: no block yet
        logger.info("Creating first block %s%s", iso(start),
                    " (API mode — seeded meter shell)" if (api_mode and not has_sensors) else "")
        return create_block(start, end, block_minutes=int(get_block_minutes()),
                            seed_meters=(api_mode and not has_sensors))

    existing_start = datetime.fromisoformat(current_block["start"])
    if existing_start == start:
        return current_block

    logger.info("Block rollover: %s → %s", current_block["start"], iso(start))

    # Wait for at least one post-boundary read before finalising. This brackets
    # the boundary so the block's end-register can be interpolated at the exact
    # boundary instant. It applies to BOTH local-sensor (CAD) reads AND Mini
    # telemetry reads — the Mini reader feeds its (timestamped, possibly delayed)
    # readings into the same per-channel reads buffer, so we wait for a read
    # timestamped at/after the boundary before finalising.
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

_zero_rate_warned: dict = {}   # (meter_id, channel_id) -> last warn monotonic ts


def _warn_zero_rate(meter_id: str, channel_id: str) -> None:
    """Warn (throttled to once/hour per meter+channel) that a block with real
    consumption is being costed at a zero rate — i.e. no rate is configured for
    this channel. This is the 'rate field left blank' misconfiguration; usage
    is being billed as free. Throttled so it doesn't spam every 30-min block.
    """
    import time as _t
    key = (meter_id, channel_id)
    now = _t.monotonic()
    last = _zero_rate_warned.get(key)
    if last is not None and (now - last) < 3600:
        return
    # In API modes the Kraken rate schedule fills missing rates at settlement,
    # so a transiently-zero rate is expected, not a misconfiguration. Only warn
    # in CAD-only mode where a blank rate genuinely means zero-cost blocks.
    try:
        if mode_uses_api():
            return
    except Exception:
        pass
    _zero_rate_warned[key] = now
    logger.warning(
        "compute_channel: %s/%s has consumption but NO rate configured — "
        "blocks are being costed at £0. Set a rate sensor for this channel "
        "(Configuration → meter), or configure the supplier API.",
        meter_id, channel_id)


# A single block's import/export delta can never plausibly reach this. Domestic
# supply tops out well under 100 kW, so a per-block delta in the hundreds of kWh
# is always a lost-opener artifact — the whole cumulative register booked as one
# interval — not real usage. Used as the rogue-total backstop in compute_channel;
# 15×+ margin over the most extreme domestic block so it never trips on real use.
_ROGUE_BLOCK_KWH_CEILING = 500.0

# Same idea for a SINGLE sub-meter (device) channel — but the main-meter clamp
# above is is_sub_meter-gated, which left device channels unprotected (#260: a
# newly-added cumulative battery/EV sensor booked its whole lifetime register as
# one block). Physical ceiling for ONE device: even a 48 kW 3-phase charger tops
# out ~24 kWh in a 30-min block / ~48 kWh in an hour, so 60 kWh is impossible and
# means a lost opener. Kept well above any real charge so session-energy sensors
# (which count a genuine 10–50 kWh charge) are never clamped.
_ROGUE_SUB_BLOCK_KWH_CEILING = 60.0
# #307: generalise the flat 60 kWh ceiling to any block length — no domestic device
# sustains 120 kW, so block_minutes/60 × 120 kW is the physical impossibility line
# (60 kWh for a 30-min block, matching the flat value above). Used by the write-point
# guard in _apply_pass2 so gap-fill / reconstruction / recovery can't bypass it.
_MAX_PLAUSIBLE_DEVICE_KW = 120.0


def _max_plausible_block_kwh(block_minutes=None) -> float:
    try:
        bm = int(block_minutes or get_block_minutes() or 30)
    except Exception:
        bm = 30
    return bm / 60.0 * _MAX_PLAUSIBLE_DEVICE_KW


def compute_channel(channel: dict, parent_rates=None, is_sub_meter: bool = False,
                    meter_id: str = "?", channel_id: str = "?") -> dict:
    reads     = channel.get("reads", [])
    rates     = channel.get("rates", [])
    if not rates and parent_rates:
        rates = parent_rates
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
        # Rogue-total backstop (source-agnostic — sensor, Mini, or DCC reads).
        # A single block cannot import a physically impossible amount. This
        # occurs when the opening register is lost or zeroed — e.g. a read
        # dropout during a restart leaves the opener at 0 while the closer still
        # holds the true cumulative register, so the block books the ENTIRE
        # lifetime register as one interval's delta. Clamp to zero and collapse
        # read_start onto read_end so (a) the total is not inflated and (b) the
        # next block still opens at the correct register. Logged (not silent) and
        # flagged for review. (Incident 2026-07: 30961 kWh booked in one 30-min
        # block from a zeroed opener during device config churn — a £10k bill.)
        if total_kwh > _ROGUE_BLOCK_KWH_CEILING:
            logger.warning(
                "compute_channel: %s/%s rogue block delta %.1f kWh "
                "(opener=%.3f closer=%.3f) exceeds %.0f kWh ceiling — clamping to "
                "0 (lost/zeroed opener); register continuity preserved via read_end.",
                meter_id, channel_id, total_kwh, reads[0]["value"],
                reads[-1]["value"], _ROGUE_BLOCK_KWH_CEILING)
            return {
                "kwh":          0.0,
                "rate":         last_rate,
                "cost":         0.0,
                "read_start":   reads[-1]["value"],
                "read_end":     reads[-1]["value"],
                "needs_review": True,
            }
        # Runtime backstop: a block that consumed/exported energy but has no
        # rate is being costed at ZERO silently — i.e. real usage billed as
        # free. This is the "rate field left blank" hole. Surface it (throttled)
        # so it's visible; billing isn't changed here, only flagged. (Zero-kwh
        # blocks with zero rate are harmless and don't warn.)
        if total_kwh > 0.0001 and (last_rate is None or abs(last_rate) < 1e-9):
            _warn_zero_rate(meter_id, channel_id)
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

    # Rogue-total backstop for sub-meters (#260). The main-meter clamp above is
    # is_sub_meter-gated, so device channels had none — a cumulative battery/EV
    # sensor with a lost/absent opener booked its whole lifetime register as one
    # block (the reporter's ~10 MWh). Guard on PHYSICAL PLAUSIBILITY only: no
    # single domestic device (even a 48 kW 3-phase charger) moves 60 kWh in a
    # block, so a delta this large is a lost opener / lifetime dump. Crucially we
    # do NOT key on a near-zero opener — a session-energy sensor legitimately
    # starts each charge at 0 and counts up, and must be booked normally. Clamp
    # to 0 and baseline read_start onto read_end (register continuity preserved).
    if total_kwh > _ROGUE_SUB_BLOCK_KWH_CEILING:
        logger.warning(
            "compute_channel: %s/%s rogue sub-meter block %.1f kWh "
            "(opener=%.3f closer=%.3f) exceeds %.0f kWh ceiling — clamping to 0 "
            "(lost opener / lifetime dump); baseline preserved via read_end.",
            meter_id, channel_id, total_kwh, reads[0]["value"], reads[-1]["value"],
            _ROGUE_SUB_BLOCK_KWH_CEILING)
        return {
            "kwh":          0.0,
            "rate":         display_rate,
            "cost":         0.0,
            "read_start":   reads[-1]["value"],
            "read_end":     reads[-1]["value"],
            "needs_review": True,
        }

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
                        elif post_read["value"] == pre_read["value"]:
                            # Identical reads — the sub-meter sensor did not change
                            # during the gap (e.g. a brief HA outage with no actual
                            # consumption). The delta is ZERO; do NOT mistake this
                            # for a reset and bill the full cumulative register.
                            # (Regression: 2.10.9 — rogue full-register kWh, e.g.
                            # 5798 kWh on a battery, from a momentary restart.)
                            sub_kwh = 0.0
                            logger.info(
                                "build_gap_blocks: %s/%s unchanged (%.4f → %.4f) "
                                "— zero delta for gap block",
                                meter_name, channel_name,
                                pre_read["value"], post_read["value"]
                            )
                        elif post_read["value"] < pre_read["value"]:
                            # Genuine reset — the register dropped. Use post_read
                            # value directly as kWh accumulated since the reset
                            # (handles daily-reset sensors and any other cumulative
                            # sensor that resets mid-block).
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

        # PASS 3b for gap blocks: attribute carbon if a CI slot is available for
        # this window. Gap blocks previously skipped this, leaving
        # carbon_intensity_g NULL (the outage-block carbon gap). If CI isn't
        # available now, the recovery sweep will backfill it later.
        if not _attribute_block_carbon(block, iso(window_start)):
            logger.info("build_gap_blocks: %s carbon not attributed "
                        "(no CI slot yet; recovery sweep will retry)",
                        iso(window_start))

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

# BL-5 (3.2.0): the four synthetic block sensors were REMOVED —
#   sensor.energy_meter_import_kwh / _export_kwh / _import_cost / _export_credit
# They were unused and actively misleading: they published live per-block figures
# that DCC settlement retrospectively corrects, so their values disagreed with the
# billing EMT itself reports. The last remaining entity,
# sensor.energy_meter_tracker_api_deprecations, was removed in 3.4.0 (BL-17) — its
# deprecation signal is now handled by the CI GraphQL-deprecation check — so EMT
# now publishes NO HA entities at all.


# ─────────────────────────────────────────────────────────────────────────────
# Chart generation helper
# ─────────────────────────────────────────────────────────────────────────────

def generate_charts(store: "BlockStore", config: dict = None):
    """
    Generate all charts from the BlockStore.
    Queries only the data each chart needs rather than loading all blocks.

    `config` may be passed in so this can run on a WORKER THREAD with a dedicated
    read-only `store` (see _generate_charts_offloaded): callers on a background
    thread MUST pass config, because load_config() reads the engine's primary
    connection and must never be touched cross-thread (the 3.1.3 segfault). When
    omitted (on-loop callers) it's loaded here as before.
    """
    if store.count_blocks() == 0:
        logger.info("generate_charts: no blocks, skipping")
        return
    if config is None:
        config = load_config()
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
        html = energy_charts.generate_daily_import_export_charts(blocks, timezone_name=timezone_name, block_minutes=block_minutes, currency=currency_symbol, cfg=config, store=store)
        io_save_file(f"{CHART_DIR}/daily_usage.html", html)
        logger.info("generate_charts: daily usage chart written (tz=%s, bm=%s, currency=%s)", timezone_name, block_minutes, currency_symbol)
    except Exception as e:
        logger.error("generate_charts: daily chart error: %s", e)


_charts_rendering = False        # guard: at most one off-loop render at a time
_charts_task = None              # held reference so a fire-and-forget task isn't GC'd


async def _generate_charts_offloaded():
    """Render charts OFF the event loop, in a thread executor, using a DEDICATED
    read-only DB connection.

    Chart generation over a large history can take tens of seconds. Run
    synchronously on the engine loop it stalls the HA WebSocket heartbeat →
    HA drops the connection → reconnect re-runs engine_startup → renders again →
    a self-perpetuating reconnect storm. Running it in a worker thread keeps the
    loop responsive.

    Thread-safety: the worker uses its OWN read-only BlockStore (never the
    engine's writer connection — the 3.1.3 cross-thread SQLite segfault), and we
    resolve `config` HERE on the loop and pass it in, since load_config() reads
    the primary connection. Falls back to an inline render on any error.
    """
    import asyncio as _asyncio
    from block_store import BlockStore
    global _charts_rendering
    if _store is None:
        return
    if _charts_rendering:
        logger.info("generate_charts: a render is already in progress — skipping")
        return
    _charts_rendering = True
    try:
        try:
            cfg = load_config()      # on the loop (reads the primary connection)
        except Exception:
            cfg = None

        def _work():
            ro = None
            try:
                ro = BlockStore(BLOCKS_DB_PATH, read_only=True)
                generate_charts(ro, config=cfg)
            finally:
                if ro is not None:
                    try:
                        ro.close()
                    except Exception:
                        pass

        loop = _asyncio.get_event_loop()
        try:
            await loop.run_in_executor(None, _work)
        except Exception as e:
            logger.warning("generate_charts: offload failed (%s) — rendering inline", e)
            try:
                generate_charts(_store, config=cfg)
            except Exception as _e2:
                logger.error("generate_charts: inline fallback failed: %s", _e2)
    finally:
        _charts_rendering = False


def _schedule_chart_regen() -> None:
    """Regenerate charts OFF the event loop when possible — via
    _generate_charts_offloaded (its own read-only connection + in-progress guard).
    A synchronous render over 2+ years of blocks takes long enough to stall the HA
    WebSocket heartbeat, which drops the connection and re-runs startup: a
    reconnect→re-finalise→re-render storm. Firing it as a loop task keeps the tick
    responsive. Falls back to an inline render only when there is no running loop
    (e.g. a non-async caller)."""
    import asyncio as _aio
    try:
        loop = _aio.get_running_loop()
    except RuntimeError:
        loop = None
    if loop is not None:
        loop.create_task(_generate_charts_offloaded())
        return
    try:
        generate_charts(get_store())
    except Exception as e:
        logger.warning("_schedule_chart_regen: inline fallback failed: %s", e)


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
        # BL-19: is the main's grid import authoritative? A DCC-settled main
        # (imp_kwh_api present) is the true grid boundary for the block, so the
        # sub-meter invariant (Σ sub kwh_grid ≤ main grid import) MUST be
        # enforced — even when the block is flagged interpolated (a gap-fill
        # origin whose main has since settled per-half-hour). See clamp_overflow.
        parent_settled = parent_block.get("imp_kwh_api") is not None

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
            # #307: physical-plausibility backstop at the shared write point. A lost
            # or zeroed OPENING register books the whole lifetime register as one
            # interval (prod: house_battery 0 → 6137 = 6137 kWh in a half-hour). That
            # is impossible for any device — clamp IMMEDIATELY, never defer to
            # settlement (which must only refine a sane figure, never pass an
            # impossibility). Sensor-agnostic: a session-energy sensor's real charge
            # never reaches this ceiling, so this can't mis-fire on a Zappi/session
            # reset. Baseline read_start onto read_end so the register stays
            # continuous, zero the block, and flag for review.
            _ceiling = _max_plausible_block_kwh((meta or {}).get("block_minutes"))
            if delta > _ceiling:
                logger.warning(
                    "PASS 2: %s implausible %.1f kWh in one block (> %.1f kWh "
                    "ceiling) — lost-opener/glitch; clamping to 0 and baselining "
                    "register (#307), flagged for review.",
                    meter_name, delta, _ceiling)
                _re = sub_import.get("read_end")
                if _re is not None:
                    sub_import["read_start"] = _re
                sub_import["kwh"] = 0.0
                sub_import["kwh_grid"] = 0.0
                sub_import["kwh_battery"] = 0.0
                sub_import["cost"] = 0.0
                sub_import["needs_review"] = True
                meter_block["needs_review"] = True
                continue
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
                # A device that drew nothing this block still FOLLOWS the main
                # meter's effective rate for its displayed rate (cost is 0). Skip
                # was leaving the sub-meter on whatever compute_channel produced,
                # and a sub-meter's running-minimum reconstruction collapses to the
                # adjacent off-peak rate at an off-peak→peak boundary block — so a
                # zero-draw device diverged from a peak main. Pin it to the parent.
                sub_import["rate"]        = parent_rate
                sub_import["cost"]        = 0.0
                sub_import["kwh_grid"]    = 0.0
                sub_import["kwh_battery"] = 0.0
                continue
            entry = {
                "meter_name": meter_name, "meter_block": meter_block,
                "sub_import": sub_import, "kwh": delta,
            }
            protected.append(entry)  # all sub-meters are protected (inverter_possible removed)

        # Grid-attribution priority: the EV claims grid import FIRST, then every
        # other device (batteries, heat pump) clips to whatever grid remains.
        # EV charging is grid-charging by design — on IOG/smart-charge the supplier
        # is deliberately pulling cheap grid for the car — so the car's import must
        # land on the grid, not get squeezed out. The old order (biggest draw first)
        # handed the whole grid pool to a simultaneously-charging battery and
        # labelled the car's grid charge as battery-sourced, so the car vanished
        # from the grid view until the battery filled (issue #212). Whatever the
        # battery then can't source from grid is its own non-grid (solar) charge.
        def _grid_priority(e):
            is_ev = ((e["meter_block"].get("meta", {}) or {}).get("meter_type")
                     == "ev")
            return (0 if is_ev else 1, -e["kwh"])   # EVs first, then desc by kWh
        protected.sort(key=_grid_priority)
        unprotected.sort(key=lambda x: x["kwh"], reverse=True)

        is_interpolated = block.get("interpolated", False)
        # BL-19: enforce the sub-meter grid invariant unless this is an UNSETTLED
        # gap block. When the main is live-metered OR DCC-settled its grid import
        # is authoritative, so a sub-meter can never claim more grid than the
        # house drew — clip the overflow to grid and spill the remainder to
        # self-consumption. The only case we still record as-is is an interpolated
        # block whose main has NOT yet settled: there the main is itself a
        # gap-fill estimate that may under-shoot, and clamping could push a real
        # (e.g. overnight) grid draw into free self-consumption and under-bill.
        # Settlement re-runs PASS 2 (imp_kwh_api set), so those self-correct then.
        clamp_overflow = parent_settled or not is_interpolated
        for entry in protected:
            claimed = min(entry["kwh"], grid_remaining)
            if entry["kwh"] > grid_kwh:
                if not clamp_overflow:
                    # Unsettled gap block — preserve energy attribution even if it
                    # exceeds grid import (the main is also an estimate here).
                    logger.warning(
                        "PASS 2: %s sub-meter %.4f kWh EXCEEDS parent grid import %.4f kWh — "
                        "unsettled gap block, recording as-is (settlement will re-clamp).",
                        entry["meter_name"], entry["kwh"], grid_kwh,
                    )
                    claimed = entry["kwh"]
                else:
                    # Authoritative main (live or DCC-settled) — clip to grid
                    # import; the sub-meter cannot exceed what the house imported.
                    logger.warning(
                        "PASS 2: %s sub-meter %.4f kWh EXCEEDS parent grid import %.4f kWh — "
                        "clipping to grid import (%s).",
                        entry["meter_name"], entry["kwh"], grid_kwh,
                        "settled" if parent_settled else "live",
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
            # Devices always follow the main meter's EFFECTIVE import rate (base
            # tariff + dispatch overlay): grid import is a portion of the main's
            # metered supply, billed at the main's rate, so per-device costs sum
            # to the metered bill. Set BOTH rate and cost here — this is the single
            # costing point (runs at finalise and settlement), so rate always
            # equals cost/kWh with no dependency on meter order or a per-device
            # rate source.
            entry["sub_import"]["rate"]        = parent_rate
            entry["sub_import"]["cost"]        = round(claimed * parent_rate, 6)
            logger.info(
                "PASS 2: %s protected  grid=%.4f  battery=%.4f",
                entry["meter_name"], claimed, entry["sub_import"]["kwh_battery"],
            )

        for entry in unprotected:
            claimed    = min(entry["kwh"], grid_remaining)
            battery    = entry["kwh"] - claimed
            if entry["kwh"] > grid_kwh:
                if not clamp_overflow:
                    logger.warning(
                        "PASS 2: %s sub-meter %.4f kWh EXCEEDS parent grid import %.4f kWh — "
                        "unsettled gap block, recording as-is (settlement will re-clamp).",
                        entry["meter_name"], entry["kwh"], grid_kwh,
                    )
                    claimed = entry["kwh"]
                    battery = 0.0
                else:
                    logger.warning(
                        "PASS 2: %s sub-meter %.4f kWh EXCEEDS parent grid import %.4f kWh — "
                        "clipping to grid import (%s).",
                        entry["meter_name"], entry["kwh"], grid_kwh,
                        "settled" if parent_settled else "live",
                    )
                    # claimed already = min(kwh, grid_remaining) from above
                    battery = entry["kwh"] - claimed
            grid_remaining = max(grid_remaining - claimed, 0.0)
            entry["sub_import"]["kwh_grid"]    = claimed
            entry["sub_import"]["kwh_battery"] = battery
            entry["sub_import"]["rate"]        = parent_rate
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
    and write back to DB.

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


def _apply_intensity_to_block(block: dict, intensity: float) -> None:
    """Attribute carbon_g + carbon_intensity_g to every meter in *block* using a
    known grid intensity (gCO2/kWh).

    Shared by the live CI-table attribution (_attribute_block_carbon) and the
    historical backfill (_run_historical_carbon_backfill), so both produce
    identical per-meter carbon: sub-meters bill their gross import; the main
    meter nets export against import."""
    for meter_name, meter_block in block.get("meters", {}).items():
        meta   = meter_block.get("meta", {}) or {}
        imp_ch = (meter_block.get("channels") or {}).get("import")
        exp_ch = (meter_block.get("channels") or {}).get("export")
        if meta.get("sub_meter", False):
            imp_kwh  = float((imp_ch or {}).get("kwh", 0.0) or 0.0)
            carbon_g = round(imp_kwh * intensity, 4)
        else:
            imp_kwh  = float((imp_ch or {}).get("kwh_total",
                       (imp_ch or {}).get("kwh", 0.0)) or 0.0)
            exp_kwh  = float((exp_ch or {}).get("kwh", 0.0) or 0.0)
            carbon_g = round((imp_kwh - exp_kwh) * intensity, 4)
        meter_block["carbon_g"] = carbon_g
        meter_block["carbon_intensity_g"] = intensity


def _persist_block_carbon(block: dict, start: str) -> int:
    """Write the per-meter carbon set by _apply_intensity_to_block IN PLACE
    (targeted carbon-column UPDATE per meter). Replaces append_block_replace on
    the carbon paths, which round-tripped the whole row and silently wiped the
    `source` tag (untagging every imported block carbon touched). Returns rows
    updated."""
    if _store is None:
        return 0
    n = 0
    for meter_name, mb in (block.get("meters") or {}).items():
        if _store.set_block_carbon(start, meter_name,
                                   mb.get("carbon_intensity_g"), mb.get("carbon_g")):
            n += 1
    return n


def _attribute_block_carbon(block: dict, start: str) -> bool:
    """Look up the nearest carbon-intensity slot for *start* and attribute
    carbon_g (+ persist carbon_intensity_g) to every meter in *block*.

    This is the PASS 3b logic, extracted so it can run from finalise_block,
    the gap-fill path (build_gap_blocks — prevention), and the NULL-carbon
    recovery sweep. Returns True if a CI slot was found and carbon attributed,
    False otherwise (no postcode, or no CI slot available for that time —
    e.g. the slot has aged out of the 4-day CI table).
    """
    try:
        postcode = _get_postcode()
        if not postcode:
            return False
        ci_row = _store.get_nearest_carbon_intensity(start, postcode)
        if not ci_row:
            return False
        # Bound the slot distance: get_nearest_carbon_intensity returns the
        # nearest row with NO distance limit, so a block whose true slot has aged
        # out of the 4-day table would otherwise be attributed a wrong, distant
        # CI value. Only attribute when the slot is genuinely near (≤60 min; CI
        # slots are 30 min, so a correct match is ≤30 min away).
        try:
            from datetime import datetime as _dt
            _bs = _dt.fromisoformat(str(start).replace("Z", "").split("+")[0])
            _cs = _dt.fromisoformat(str(ci_row["captured_at"]).replace("Z", "").split("+")[0])
            if abs((_bs - _cs).total_seconds()) > 3600:
                return False
        except Exception:
            pass
        _apply_intensity_to_block(block, ci_row["intensity"])
        return True
    except Exception as e:
        logger.warning("_attribute_block_carbon: failed for %s: %s", start, e)
        return False


def _recover_missing_carbon(limit: int = 200) -> int:
    """Backfill carbon for blocks whose carbon_intensity_g is NULL.

    These are typically outage gap-fill blocks that were built before a CI slot
    was available (the energy was recovered by interpolation, but carbon wasn't
    attributed). Now that the CI tick has populated the table, re-attribute from
    the nearest CI slot and persist. Blocks whose slot has aged out of the 4-day
    CI table can't be recovered and are left for a future manual tool.

    Returns the number of blocks recovered. Safe to call on startup and after a
    CI tick; a no-op when nothing is missing.
    """
    if _store is None:
        return 0
    if _store.get_meta("carbon_paused", None):
        return 0
    try:
        starts = _store.get_block_starts_missing_carbon(limit=limit)
    except Exception as e:
        logger.warning("_recover_missing_carbon: query failed: %s", e)
        return 0
    if not starts:
        return 0
    recovered = 0
    for start in starts:
        block = _store.get_block_dict_by_start(start)
        if not block:
            continue
        if _attribute_block_carbon(block, start):
            try:
                _persist_block_carbon(block, start)   # in place — keeps source tag
                recovered += 1
                logger.info("_recover_missing_carbon: backfilled carbon for %s",
                            start)
            except Exception as e:
                logger.warning("_recover_missing_carbon: persist failed for "
                               "%s: %s", start, e)
        # If not attributed, the CI slot isn't available (aged out) — skip.
    if recovered:
        # OFF the loop (see _schedule_chart_regen): a synchronous full-history
        # render here stalls the HA WebSocket heartbeat, and after a big import
        # (2+ years of blocks, ~90s/render) that kicks off a
        # reconnect → re-startup → re-render storm. The in-progress guard coalesces
        # overlapping renders across successive carbon ticks.
        _schedule_chart_regen()
        logger.info("_recover_missing_carbon: recovered %d block(s)", recovered)
    return recovered


def _recompute_block_carbon(block: dict) -> None:
    """Recompute carbon_g for every meter in *block* using the intensity
    persisted on the block (carbon_intensity_g), NOT a fresh CI-table lookup.

    Mirrors finalise_block PASS 3b exactly, but sources intensity from the
    stored value so a DCC re-run weeks later (after the 4-day CI prune) still
    produces the correct figure. If a meter has no stored intensity, carbon_g
    is left unchanged (we cannot recompute without it, and reverse-deriving
    from the old carbon_g would be circular).
    """
    for meter_name, meter_block in block.get("meters", {}).items():
        intensity = meter_block.get("carbon_intensity_g")
        if intensity is None:
            continue
        meta   = meter_block.get("meta", {}) or {}
        imp_ch = (meter_block.get("channels") or {}).get("import")
        exp_ch = (meter_block.get("channels") or {}).get("export")
        if meta.get("sub_meter"):
            imp_kwh  = float((imp_ch or {}).get("kwh", 0.0) or 0.0)
            meter_block["carbon_g"] = round(imp_kwh * intensity, 4)
        else:
            imp_kwh = float((imp_ch or {}).get("kwh_total",
                       (imp_ch or {}).get("kwh", 0.0)) or 0.0)
            exp_kwh = float((exp_ch or {}).get("kwh", 0.0) or 0.0)
            meter_block["carbon_g"] = round((imp_kwh - exp_kwh) * intensity, 4)


def _resolve_block_rate(channel: dict, block_start: str, channel_name: str,
                        rate_resolver=None) -> tuple[float, bool]:
    """Return (rate, repaired) for a channel at reconcile time.

    Uses the channel's stored rate when it is a sane non-zero value. When the
    stored rate is zero/missing AND a rate_resolver is supplied, asks the
    resolver for the historical rate at block_start (the Kraken fallback,
    Chunk 5). The resolver returns rate in £/kWh already (caller converts from
    the API's pence). A resolved rate outside a plausible band is rejected
    (logged) rather than applied, guarding against a units surprise.

    repaired=True when the resolver supplied the rate, so the caller can flag
    the block. Never returns a silent zero when a resolver could help: if the
    resolver yields nothing, the original (possibly zero) rate is returned and
    the block stays flagged for the tooling.
    """
    stored = channel.get("rate", 0.0) or 0.0
    if stored and abs(stored) >= 1e-6:
        return stored, False
    if rate_resolver is None:
        return stored, False
    try:
        resolved = rate_resolver(channel_name, block_start)
    except Exception as e:
        logger.warning("_resolve_block_rate: resolver error %s/%s: %s",
                       channel_name, block_start, e)
        return stored, False
    if resolved is None:
        return stored, False
    # Sanity band for a UK £/kWh rate (import or export). Reject wild values
    # (e.g. pence not converted → 24.5) rather than corrupt a block.
    if not (-2.0 <= resolved <= 2.0):
        logger.warning("_resolve_block_rate: rejected implausible rate %.4f "
                       "for %s/%s (units?)", resolved, channel_name, block_start)
        return stored, False
    logger.info("_resolve_block_rate: repaired %s rate at %s → %.4f £/kWh",
                channel_name, block_start, resolved)
    return resolved, True


def _recompute_pass3_totals(block: dict) -> None:
    """PASS 3 — re-derive a block's import/export totals from its (already
    PASS-2'd) per-meter channels: sub-meters contribute kwh_grid, the main
    contributes its kwh_remainder. Mirrors finalise_block PASS 3 and is shared
    by the settlement rerun and the post-delete remainder recompute so the two
    can never drift."""
    block.setdefault("totals", {})
    block["totals"].update({"import_kwh": 0.0, "import_cost": 0.0,
                            "export_kwh": 0.0, "export_cost": 0.0})
    for meter_name, meter_block in block["meters"].items():
        meta = meter_block.get("meta", {}) or {}
        for channel_name, channel in (meter_block.get("channels") or {}).items():
            if channel_name == "import":
                if meta.get("sub_meter"):
                    ik = channel.get("kwh_grid")
                    if ik is None:
                        ik = channel.get("kwh") or 0.0
                    block["totals"]["import_kwh"]  += ik or 0.0
                    block["totals"]["import_cost"] += channel.get("cost") or 0.0
                else:
                    ik = channel.get("kwh_remainder")
                    if ik is None:
                        ik = channel.get("kwh") or 0.0
                    ic = channel.get("cost_remainder")
                    if ic is None:
                        ic = channel.get("cost") or 0.0
                    block["totals"]["import_kwh"]  += ik or 0.0
                    block["totals"]["import_cost"] += ic or 0.0
            elif channel_name == "export":
                block["totals"]["export_kwh"]  += channel.get("kwh") or 0.0
                block["totals"]["export_cost"] += channel.get("cost") or 0.0


def recompute_remainders_for_window(parent_meter_id: str, utc_start: str,
                                    utc_end: str) -> int:
    """Re-derive a main meter's kwh_remainder/cost_remainder after one of its
    sub-meters' blocks were deleted.

    Deleting a device in isolation leaves the parent's stored remainder having
    subtracted a device that no longer exists, so the parent line reads low.
    For each affected parent block in [utc_start, utc_end) this reconstructs the
    block (the deleted device is already absent), re-runs PASS 2 against the
    surviving sub-meters — reattributing the deleted device's grid-attributed
    energy back into the remainder — then re-derives PASS 3 totals and carbon
    and writes it back. Returns the number of parent blocks recomputed.

    Uses _apply_pass2 directly, NOT _rerun_pass2_for_settled_block: the
    remainder is a PASS-2 concern only. The settlement rerun would also
    materialise DCC/CAD kWh, capture kwh_cad, apply the dispatch overlay and
    clear is_provisional — all of which would be wrong to trigger from a delete.
    Recomputing a window that was not actually affected is a no-op (the same
    surviving subs yield the same remainder), so the bounds can be generous.
    """
    if not parent_meter_id:
        return 0
    store = get_store()
    starts = [r["block_start"] for r in store._conn.execute(
        "SELECT DISTINCT block_start FROM blocks "
        "WHERE meter_id = ? AND block_start >= ? AND block_start < ? "
        "ORDER BY block_start",
        (parent_meter_id, utc_start, utc_end),
    ).fetchall()]
    done = 0
    for bs in starts:
        try:
            block = store.get_block_dict_by_start(bs)
            if not block:
                continue
            # Reset the parent's remainder to the full main import first, so a
            # window left with NO surviving sub-meters yields remainder == main
            # (no phantom subtraction). _apply_pass2 only writes the remainder
            # for parents that still have sub-meters, so without this a
            # fully-emptied window would keep its stale remainder.
            pm = (block.get("meters") or {}).get(parent_meter_id)
            if pm:
                pic = (pm.get("channels") or {}).get("import")
                if pic is not None:
                    pic["kwh_remainder"]  = pic.get("kwh")
                    pic["cost_remainder"] = pic.get("cost")
            _apply_pass2(block)
            _recompute_pass3_totals(block)
            _recompute_block_carbon(block)
            append_block_replace(block)
            done += 1
        except Exception as e:
            logger.error(
                "recompute_remainders_for_window: block %s failed: %s", bs, e)
    if done:
        try:
            generate_charts(store)
        except Exception as _ce:
            logger.warning(
                "recompute_remainders_for_window: chart regen failed: %s", _ce)
    logger.info(
        "recompute_remainders_for_window: recomputed %d block(s) for parent %s",
        done, parent_meter_id)
    return done


def backout_recorder_attribution(*, run_id=None, meter_id=None,
                                  from_date=None, to_date=None) -> dict:
    """Undo a recorder-attribution run (or an explicit device + range): delete the
    reconstructed 'recorder_attributed' device blocks, re-derive each affected parent
    meter's remainder over the span, and drop the run from the ledger. The reversible
    safety net — fast, surgical, and it never touches live or imported house totals
    (only the additive device layer is removed; the remainder is re-derived from the
    surviving sub-meters). Returns {ok, deleted, parents, recomputed, from, to}."""
    from datetime import datetime as _dt, timedelta as _td
    store = get_store()
    from_iso = from_date
    to_iso = to_date
    if run_id:
        for r in store.get_attribution_runs():
            if r.get("run_id") == run_id:
                meter_id = meter_id or r.get("meter_id")
                from_iso = from_iso or r.get("from")
                to_iso = to_iso or r.get("to")
                break
    res = store.delete_recorder_attributed(
        meter_id=meter_id, from_iso=from_iso, to_iso=to_iso)
    lo, hi = res.get("from"), res.get("to")
    recomputed = 0
    if lo and hi:
        try:
            hi_excl = (_dt.fromisoformat(hi)
                       + _td(minutes=int(get_block_minutes() or 30))).isoformat()
        except Exception:
            hi_excl = hi
        for parent in (res.get("parents") or []):
            recomputed += recompute_remainders_for_window(parent, lo, hi_excl)
    if run_id:
        store.remove_attribution_run(run_id)
    logger.info("backout_recorder_attribution: deleted %d device block(s), "
                "recomputed %d parent block(s) (run=%s meter=%s)",
                res.get("deleted"), recomputed, run_id, meter_id)
    return {"ok": True, "deleted": res.get("deleted", 0),
            "parents": res.get("parents") or [], "recomputed": recomputed,
            "from": lo, "to": hi}


def _stitch_hourly_energy(series_by_sensor: dict, priority: list) -> dict:
    """Combine one or more cumulative-kWh statistic series into a single hourly
    ENERGY map {naive-UTC hour ISO: kWh}. Pure.

    Per sensor, the hour energy is the HA `change` if present, else the delta of
    consecutive `sum` values; a DECREASE in `sum` is a counter reset (device swap /
    integration reset) → that step is dropped and the series continues. Sensors are
    tried in `priority` order and the FIRST to supply an hour wins, so listing
    [new_sensor, old_sensor] stitches a rename: the new sensor owns recent hours,
    the old fills the earlier ones. Each hour's energy is keyed to the hour it
    belongs to; alignment to the two half-hour blocks happens in _split_hour_to_blocks."""
    import statistics_probe as _sp
    out: dict = {}
    for sensor in (priority or []):
        parsed = []
        for r in (series_by_sensor.get(sensor) or []):
            t = _sp._to_utc(r.get("start"))
            if t is None:
                continue
            hour = t.replace(minute=0, second=0, microsecond=0, tzinfo=None)
            parsed.append((hour, r))
        parsed.sort(key=lambda x: x[0])
        prev_sum = None
        for hour, r in parsed:
            s = r.get("sum")
            ch = r.get("change")
            e = None
            if ch is not None:
                try:
                    e = float(ch)
                except (TypeError, ValueError):
                    e = None
            elif s is not None and prev_sum is not None:
                d = s - prev_sum
                if d >= 0:
                    e = d
            if s is not None:
                prev_sum = s
            key = hour.isoformat()
            if e is not None and e >= 0 and key not in out:
                out[key] = round(float(e), 6)
    return out


def _split_hour_to_blocks(hour_kwh, house_imports: list) -> list:
    """Distribute an hour's measured device energy across its constituent blocks in
    proportion to the house IMPORT shape for those blocks — so a charge burst lands
    in the half-hour it actually happened, not smeared. `house_imports` = per-block
    house import kWh for the hour, in order; equal split when the house imported
    nothing that hour. Returns per-block device kWh, same length/order. Pure."""
    n = len(house_imports)
    if n == 0:
        return []
    e = float(hour_kwh or 0.0)
    tot = sum((x or 0.0) for x in house_imports)
    if tot <= 0:
        return [round(e / n, 6)] * n
    return [round(e * ((x or 0.0) / tot), 6) for x in house_imports]


def _write_device_into_block(block: dict, device_meter_id: str,
                             parent_meter_id: str, dev_kwh) -> bool:
    """Add a reconstructed device sub-meter to an existing house block and re-run
    PASS 2/3 so the device's grid share is clipped to the parent import and the
    parent remainder is re-derived. Tags ONLY the device row 'recorder_attributed'.
    Returns False (no write) when the device is already present (never overwrite a
    live/attributed row) or the parent isn't in the block."""
    meters = block.get("meters") or {}
    if device_meter_id in meters or parent_meter_id not in meters:
        return False
    meters[device_meter_id] = {
        "meta": {"sub_meter": True, "parent_meter": parent_meter_id},
        "source": get_store().RECORDER_ATTRIBUTED_SOURCE,
        "channels": {"import": {"kwh": round(float(dev_kwh or 0.0), 6)}},
    }
    block["meters"] = meters
    _apply_pass2(block)
    _recompute_pass3_totals(block)
    _recompute_block_carbon(block)
    append_block_replace(block)
    return True


def _attribute_hour(hour_iso: str, device_meter_id: str, parent_meter_id: str,
                    hour_kwh, *, block_minutes: int = 30) -> tuple:
    """Attribute one hour's measured device energy across its half-hour blocks,
    weighted by the house import shape, writing a recorder_attributed device block
    wherever one doesn't already exist. Returns (written, skipped, no_house)."""
    from datetime import datetime as _dt, timedelta as _td
    store = get_store()
    try:
        h0 = _dt.fromisoformat(str(hour_iso))
    except ValueError:
        return (0, 0, 0)
    step = int(block_minutes or 30)
    nblocks = max(1, 60 // step)
    starts = [(h0 + _td(minutes=step * i)).isoformat() for i in range(nblocks)]
    blocks = [store.get_block_dict_by_start(bs) for bs in starts]
    house = []
    for b in blocks:
        pi = 0.0
        if b:
            pm = (b.get("meters") or {}).get(parent_meter_id)
            if pm:
                pi = ((pm.get("channels") or {}).get("import") or {}).get("kwh") or 0.0
        house.append(pi)
    split = _split_hour_to_blocks(hour_kwh, house)
    written = skipped = no_house = 0
    for b, dev_kwh in zip(blocks, split):
        if not b:                       # no house block → no control total
            no_house += 1
            continue
        if _write_device_into_block(b, device_meter_id, parent_meter_id, dev_kwh):
            written += 1
        else:
            skipped += 1                # already attributed / live
    return (written, skipped, no_house)


_attribution_job: dict = {"status": "idle"}


def api_attribution_status() -> dict:
    """JSON-safe snapshot of the recorder-attribution background job."""
    return {k: v for k, v in _attribution_job.items() if k != "task"}


def attribution_running() -> bool:
    return _attribution_job.get("status") in ("running", "paused")


def attribution_control(action: str) -> dict:
    """pause / resume / cancel the attribution job (mirrors the import controls)."""
    if action == "pause":
        _attribution_job["control"] = "pause"
    elif action == "resume":
        _attribution_job["control"] = "run"
    elif action == "cancel":
        _attribution_job["control"] = "cancel"
    return api_attribution_status()


async def run_attribution_job(device_meter_id: str, sensor_ids: list, *,
                              pace_s: float = 0.0) -> dict:
    """Background: attribute a device's HA recorder history onto the house blocks it
    isn't on yet. Fetch stats (on the loop's WS listener), stitch to hourly energy,
    walk oldest-first writing recorder_attributed device blocks — skipping any block
    where the device already has a row (never overwrites live). Cooperative: yields
    between hours and PAUSES while a delete/purge runs, so it coexists with the live
    tick and the pass-2 settlement drain. Records a ledger run. Never raises."""
    import asyncio as _aio
    j = _attribution_job
    store = get_store()
    j.clear()
    j.update({"status": "running", "phase": "fetching", "control": "run",
              "device": device_meter_id, "sensors": list(sensor_ids or []),
              "written": 0, "skipped": 0, "gaps": 0, "done_hours": 0,
              "total_hours": 0, "started_at": _dt_now_iso_safe()})
    run_id = _dt_now_iso_safe() + "|" + device_meter_id
    try:
        if _engine_ha is None:
            j.update({"status": "error", "error": "no HA connection"})
            return {"ok": False, "error": "no HA connection"}
        parent = store.get_parent_meter_id(device_meter_id) or "electricity_main"
        now = datetime.now(timezone.utc)
        start = (now - timedelta(days=3660)).isoformat()
        series = {}
        for sid in (sensor_ids or []):
            try:
                res = await _engine_ha.get_statistics([sid], start, now.isoformat(),
                                                      period="hour")
                series[sid] = res.get(sid) or []
            except Exception as e:
                logger.warning("run_attribution_job: stats fetch %s failed: %s", sid, e)
                series[sid] = []
        hourly = _stitch_hourly_energy(series, list(sensor_ids or []))
        hours = sorted(hourly.keys())          # oldest first
        bm = int(get_block_minutes() or 30)
        j.update({"phase": "attributing", "total_hours": len(hours)})
        written = skipped = gaps = 0
        for i, hour in enumerate(hours):
            if j.get("control") == "cancel":
                j["status"] = "cancelled"
                break
            while j.get("control") == "pause" or delete_in_progress():
                j["status"] = "paused"
                await _aio.sleep(1.0)
                if j.get("control") == "cancel":
                    break
            if j.get("control") == "cancel":
                j["status"] = "cancelled"
                break
            j["status"] = "running"
            w, s, nh = _attribute_hour(hour, device_meter_id, parent,
                                       hourly[hour], block_minutes=bm)
            written += w; skipped += s; gaps += nh
            if (i % 200) == 0:
                j.update({"written": written, "skipped": skipped, "gaps": gaps,
                          "done_hours": i})
                await _aio.sleep(pace_s)        # yield to the live tick / drain
        j.update({"written": written, "skipped": skipped, "gaps": gaps,
                  "done_hours": len(hours)})
        if written:
            store.record_attribution_run({
                "run_id": run_id, "meter_id": device_meter_id,
                "sensor_ids": list(sensor_ids or []),
                "from": hours[0] if hours else None,
                "to": hours[-1] if hours else None,
                "blocks_written": written, "created_at": _dt_now_iso_safe()})
        if j.get("status") != "cancelled":
            j["status"] = "done"
        j["run_id"] = run_id
        logger.info("run_attribution_job: device=%s wrote %d, skipped %d, gaps %d "
                    "(status=%s)", device_meter_id, written, skipped, gaps, j["status"])
        return {"ok": True, "written": written, "skipped": skipped, "gaps": gaps,
                "run_id": run_id, "status": j["status"]}
    except Exception as e:
        logger.warning("run_attribution_job failed: %s", e)
        j.update({"status": "error", "error": str(e)})
        return {"ok": False, "error": str(e)}


def _rerun_pass2_for_settled_block(block: dict, main_meter_id: str = "electricity_main",
                                   rate_resolver=None, billing_source: str = "dcc",
                                   standing_resolver=None) -> dict:
    """Re-run PASS 2 + PASS 3b for a single block, materialising billing
    figures from the chosen source.

    billing_source (global toggle):
      - 'dcc' (default): billing kWh is the DCC settlement (imp_kwh_api /
        exp_kwh_api) where present, falling back to the CAD figure where DCC is
        absent — "settle to DCC except where missing".
      - 'cad': billing kWh is always the CAD figure, ignoring DCC. DCC values
        stay stored for drift/diagnostics, just unused for billing.

    Switching the toggle flags every block for re-run, so this re-materialises
    each block to the new source. The mechanism is identical both ways; only
    the kWh selection differs. The original CAD figure is preserved in kwh_cad
    on first settlement so a switch back to CAD can restore it exactly.

    rate_resolver (Chunk 5): consulted only when the chosen kWh has no rate.
    Returns the mutated block dict.
    """
    main = block.get("meters", {}).get(main_meter_id)
    if not main:
        return block
    _rate_repaired = False
    use_dcc = (billing_source != "cad")

    imp_ch = (main.get("channels") or {}).get("import")
    if imp_ch is not None:
        cad_kwh = imp_ch.get("kwh_cad")
        if cad_kwh is None:
            # First settlement: current kwh IS the CAD figure; preserve it so a
            # later switch back to CAD restores the original exactly.
            cad_kwh = imp_ch.get("kwh")
            imp_ch["kwh_cad"] = cad_kwh
        dcc_kwh = main.get("imp_kwh_api")
        chosen = dcc_kwh if (use_dcc and dcc_kwh is not None) else cad_kwh
        if chosen is not None:
            # Preserve a deliberate rate override — a user correction
            # (rate_corrected) or a dispatch reconciliation (rate_reconciled). If
            # PASS 2 re-runs after one of those (e.g. a re-settlement), it must NOT
            # re-resolve the overlay over the override; keep the stored rate and
            # only re-cost it against the settled kWh. (Ordering normally puts
            # reconcile after settlement, so this is belt-and-braces.)
            _override_rate = None
            try:
                if _store is not None:
                    _mrow = _store._conn.execute(
                        "SELECT rate_corrected, rate_reconciled, imp_rate "
                        "FROM blocks WHERE block_start = ? AND meter_id = ?",
                        (block.get("start", ""), main_meter_id)).fetchone()
                    if _mrow and (_mrow[0] or _mrow[1]) and _mrow[2] is not None:
                        _override_rate = _mrow[2]
            except Exception:
                _override_rate = None
            if _override_rate is not None:
                rate = _override_rate
                logger.info("_rerun_pass2: %s rate override preserved (%.5f) — "
                            "re-costed to settled kWh (not re-resolved)",
                            block.get("start", ""), rate)
                imp_ch["rate"] = rate
            else:
                rate, rep = _resolve_block_rate(imp_ch, block.get("start", ""),
                                                "import", rate_resolver)
                _rate_repaired = _rate_repaired or rep
                if rep:
                    imp_ch["rate"] = rate
                # Dispatch overlay: an out-of-window smart-charge slot with real
                # draw is repriced to off-peak. Gated by the materialised import
                # kWh (meter validation).
                rate = _dispatch_overlay_rate("import", block.get("start", ""),
                                              rate, chosen)
                # Persist the EFFECTIVE (post-overlay) rate, not just the cost.
                # PASS 2 prices every device's grid import at the parent's
                # imp_ch["rate"]; leaving the pre-overlay base here would bill the
                # main off-peak but devices at PEAK — a real overcharge. Mirrors
                # finalise_block, where the main overlay writes result["rate"].
                imp_ch["rate"] = rate
            imp_ch["kwh"] = chosen
            imp_ch["kwh_total"] = chosen
            imp_ch["cost"] = round(chosen * rate, 6)

    exp_ch = (main.get("channels") or {}).get("export")
    if exp_ch is None and main.get("exp_kwh_api") is not None:
        # DCC-only export (no export sensor, no Mini export layer): exp_kwh was
        # never materialised at finalise, so the reconstructed block has no
        # export channel — but the settled figure is present in exp_kwh_api.
        # Create the channel so the settlement below can materialise it.
        # Without this, settled export stays stuck in exp_kwh_api forever and
        # the export bill reads zero (the DCC-only-export persistence bug).
        exp_ch = {}
        main.setdefault("channels", {})["export"] = exp_ch
    if exp_ch is not None:
        cad_exp = exp_ch.get("kwh_cad")
        if cad_exp is None:
            cad_exp = exp_ch.get("kwh")
            exp_ch["kwh_cad"] = cad_exp
        dcc_exp = main.get("exp_kwh_api")
        chosen_exp = dcc_exp if (use_dcc and dcc_exp is not None) else cad_exp
        if chosen_exp is not None:
            exp_rate, rep = _resolve_block_rate(exp_ch, block.get("start", ""),
                                                "export", rate_resolver)
            _rate_repaired = _rate_repaired or rep
            if rep:
                exp_ch["rate"] = exp_rate
            exp_ch["kwh"] = chosen_exp
            exp_ch["cost"] = round(chosen_exp * exp_rate, 6)

    if _rate_repaired:
        main["rate_repaired"] = True

    # Standing charge: repair zeros AND re-verify non-zero against Kraken
    # (Kraken wins for now). Resolved value is £/day; reject implausible
    # (units guard) and only correct when materially different to avoid churn.
    if standing_resolver is not None:
        try:
            sc_new = standing_resolver(block.get("start", ""))
        except Exception as e:
            logger.warning("_rerun: standing resolver error at %s: %s",
                           block.get("start", ""), e)
            sc_new = None
        if sc_new is not None and (0.0 <= sc_new <= 2.0):
            sc_old = main.get("standing_charge", 0.0) or 0.0
            if abs(sc_new - sc_old) > 1e-6:
                main["standing_charge"] = sc_new
                main["standing_charge_repaired"] = True
                logger.info("_rerun: standing_charge %s %.4f → %.4f £/day",
                            block.get("start", ""), sc_old, sc_new)
        elif sc_new is not None:
            logger.warning("_rerun: rejected implausible standing charge %.4f "
                           "at %s (units?)", sc_new, block.get("start", ""))

    _apply_pass2(block)

    # PASS 3 totals re-derivation (mirror finalise_block PASS 3)
    _recompute_pass3_totals(block)

    _recompute_block_carbon(block)

    # Clear provisional markers — block is DCC-final now.
    for meter_block in block["meters"].values():
        meter_block.pop("is_provisional", None)
        meter_block.pop("provisional", None)
    return block


def _drain_pass2_queue(ha: HAClient, limit: int = 50, rate_resolver=None) -> int:
    """Process blocks flagged needs_pass2_rerun=1 (DCC settlement arrived).

    For each: reload the full block, re-run PASS 2+3b against imp_kwh_api,
    write back (clearing is_provisional), clear the re-run flag. Returns the
    number of blocks successfully re-run.

    Bounded per call (limit) so a large settlement batch is spread across
    ticks rather than blocking one. Each block is independent — a failure on
    one is logged and skipped, leaving its flag set for a later retry.

    rate_resolver (Chunk 5, optional): callable(channel_name, block_start) ->
    £/kWh or None, used to repair zero/missing rates from Kraken history at
    reconcile time. Built once per poll cycle and cached; the drain only reads
    it, never fetches.
    """
    queued = _store.get_blocks_needing_pass2_rerun(limit=limit)
    if not queued:
        return 0

    resolver = rate_resolver if rate_resolver is not None else _kraken_rate_resolver
    source = _get_billing_source()

    done = 0
    for row in queued:
        block_start = row["block_start"]
        block_id    = row["id"]
        try:
            block = _store.get_block_dict_by_start(block_start)
            if not block:
                logger.warning(
                    "_drain_pass2_queue: block %s vanished; clearing flag",
                    block_start)
                _store.clear_pass2_rerun_flag(block_id)
                continue

            _rerun_pass2_for_settled_block(block, rate_resolver=resolver,
                                           billing_source=source,
                                           standing_resolver=_kraken_standing_resolver)
            append_block_replace(block)
            _store.clear_pass2_rerun_flag(block_id)
            done += 1
            logger.info(
                "_drain_pass2_queue: re-ran PASS 2+3b for %s against DCC figure",
                block_start)
        except Exception as e:
            logger.error(
                "_drain_pass2_queue: re-run failed for %s (flag left set "
                "for retry): %s", block_start, e)

    if done:
        # Re-priced blocks change the billing/daily charts — regenerate them now
        # so the UI reflects the reconciled figures immediately. Without this the
        # charts stay stale until the next block rollover happens to trigger a
        # chart write, so a user viewing right after a reconcile sees old numbers.
        try:
            generate_charts(_store)
        except Exception as _ce:
            logger.warning("_drain_pass2_queue: chart regen failed: %s", _ce)

    return done


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
        # Honour an explicit "use supplier API standing charge" choice on the
        # main meter: take the schedule value even when a SC sensor is mapped.
        _sc_force_api          = (import_channel_cfg.get("standing_charge_source") == "api") \
                                 and not meter_meta.get("sub_meter")
        raw_sc                 = 0.0 if _sc_force_api else \
                                 (read_sensor(ha, standing_charge_sensor) if standing_charge_sensor else 0.0)
        if (raw_sc is None or raw_sc == 0.0) and standing_charge_sensor and not _sc_force_api:
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
        # API mode (no SC sensor) OR an explicit "use API standing charge" choice:
        # the standing charge comes from the Kraken schedule. Apply it at finalise
        # (main import only; sub-meters inherit) so every block carries its
        # standing charge immediately, rather than 0-until-DCC-settlement. The
        # settlement rerun later refines it via the same resolver.
        if (raw_sc is None or raw_sc == 0.0) and (not standing_charge_sensor or _sc_force_api) \
                and not meter_meta.get("sub_meter"):
            try:
                _sc_sched = _kraken_standing_resolver(start)
                if _sc_sched:
                    raw_sc = float(_sc_sched)
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
            # API/Mini mode: no rate SENSOR exists — the unit rate comes from the
            # Kraken rate schedule, which knows the correct PER-BLOCK time-of-use
            # rate (e.g. IOG off-peak overnight vs peak by day). Resolve it at the
            # block start (main meter import/export only; sub-meters inherit the
            # parent rate via compute_channel).
            #
            # This MUST be tried BEFORE the last_known_rates fallback below.
            # last_known_rates carries the PREVIOUS block's rate forward as a
            # single value; if it ran first it would mask the schedule resolver
            # and every block would inherit the first block's rate uniformly
            # (the IOG "every block billed at peak, incl. overnight off-peak"
            # bug). The schedule is time-of-use aware and per-block correct, so
            # it wins; last_known_rates is only the gap/restore fallback when the
            # resolver yields nothing (no schedule, or timestamp uncovered).
            # Honour an explicit "use supplier API rate" choice on the MAIN
            # meter: resolve the schedule for this block even when a rate sensor
            # is mapped (the config-screen / wizard toggle wins over the sensor).
            # If the schedule can't resolve, the sensor value already in
            # valid_rates is left as a graceful fallback.
            _ch_cfg = (meter_cfg or {}).get("channels", {}).get(channel_name, {})
            if (_ch_cfg.get("rate_source") == "api") and not is_sub \
                    and channel_name in ("import", "export"):
                try:
                    _kr = _kraken_rate_resolver(channel_name, start)
                    if _kr is not None:
                        valid_rates = [{"ts": start, "value": _kr}]
                except Exception:
                    pass
            if not valid_rates and not is_sub and channel_name in ("import", "export"):
                try:
                    _kr = _kraken_rate_resolver(channel_name, start)
                    if _kr is not None:
                        valid_rates = [{"ts": start, "value": _kr}]
                except Exception:
                    pass
            # Fallback to last_known_rates when no live rates AND the schedule
            # resolver could not resolve (e.g. CAD sensor gap after a restore
            # where stale reads were cleared, or an uncovered schedule window).
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

            result = compute_channel(channel_for_compute, parent_rates, is_sub_meter=is_sub,
                                     meter_id=meter_name, channel_id=channel_name)

            channel_cfg_meta = meter_cfg.get("channels", {}).get(channel_name, {}).get("meta")
            if channel_cfg_meta:
                result["meta"] = channel_cfg_meta

            meter_block["channels"][channel_name] = result

            # Dispatch overlay (A) — at FINALISE, for the main import channel.
            # This is the finalise-time rate adjustment that BCD's rate sensor
            # would supply if installed; EMT computes the equivalent itself for
            # users without BCD. An out-of-window smart-charge slot with real
            # draw is repriced to off-peak here, the moment the block forms —
            # consistent with how CAD/BCD rates are applied at finalise. Gated by
            # the computed import kWh (meter validation). Settlement re-applies it
            # when the DCC kWh re-materialises (so it isn't clobbered).
            if not is_sub and channel_name == "import":
                _base = result.get("rate", 0.0) or 0.0
                if _base:
                    _ov = _dispatch_overlay_rate("import", start, _base,
                                                 result.get("kwh"))
                    if _ov != _base:
                        result["rate"] = _ov
                        result["cost"] = round((result.get("kwh") or 0.0) * _ov, 6)

            # NOTE: sub-meter (device) import is NOT priced here. Devices always
            # follow the main meter's effective import rate, applied uniformly in
            # PASS 2 (_apply_pass2), which runs after every meter is finalised and
            # sets both the device's rate and cost from the parent's rate. This is
            # the single device-costing point; keeping it out of PASS 1 removes any
            # dependency on meter order and the old per-device over-report floor.

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
                    # 3.0.0: persist the intensity used, so a later PASS 2+3b
                    # re-run (DCC settlement) can recompute carbon_g without
                    # re-querying the carbon_intensity table (pruned to 4 days).
                    meter_block["carbon_intensity_g"] = intensity
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

    logger.info("finalise_block: %s → %s complete", start, end)

    # 3.0.0: notify any registered boundary listener (Kraken Mini ingester).
    # No-op until Chunk 7 registers a callback.
    _fire_block_boundary(end)

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
    # Render OFF the loop (offloaded, read-only connection) so a large-history
    # render can't stall the tick / HA heartbeat (→ reconnect storm).
    if not interpolated:
        _schedule_chart_regen()

    # ── Backup to /share ───────────────────────────────────────────────────
    _backup_to_share()




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

async def _poll_mini_demand_kw(now) -> float | None:
    """Fetch the Mini's latest smartMeterTelemetry demand (kW), throttled to one
    call per _MINI_POLL_GAP. Updates _last_mini_demand (read by the gauge) and
    returns the kW for this tick's power_history row, or None when throttled /
    unavailable. Runs on the engine loop, so the GraphQL call is awaited directly."""
    global _last_mini_demand
    import time as _time
    t = _time.monotonic()
    if (t - _last_mini_demand["ts"]) < _MINI_POLL_GAP:
        return None  # throttled — no new row until the next poll window
    reader = _kraken_mini_reader
    device_id = getattr(reader, "device_id", None) if reader else None
    if not device_id or not _kraken_client:
        return None
    try:
        from datetime import timedelta
        start = now - timedelta(minutes=3)
        pts = await _kraken_client.get_telemetry(
            device_id, start.isoformat(), now.isoformat()) or []
        demand_w = None
        for p in reversed(pts):
            if p.get("demand") is not None:
                demand_w = float(p["demand"])
                break
        if demand_w is None:
            return None
        kw = round(demand_w / 1000.0, 3)  # telemetry demand is in watts
        _last_mini_demand = {"kw": kw, "ts": t, "wall": _time.time()}
        return kw
    except Exception as e:
        logger.warning("_poll_mini_demand_kw: %s", e)
        return None


def _power_value_to_kw(value, unit, unit_override=None, invert=False) -> float | None:
    """Convert a power sensor reading to kW.

    `unit_override` ('W'/'kW', case-insensitive) forces the unit when HA's
    unit_of_measurement is wrong or absent — e.g. a CT integration that declares
    'kW' but emits W-scale numbers (1400 for 1.4 kW), which no magnitude heuristic
    can catch because the (wrong) unit IS present. With no override, the sensor's
    declared unit drives it; an absent/unknown unit falls back to the magnitude
    heuristic (>100 ⇒ watts). `invert` negates the result for sensors whose sign
    convention is opposite EMT's (import positive / export negative; or, for a
    battery inverter, positive = charging). Returns None for non-numeric values.
    Mirrors the server's sensor_kw so BCD's current_demand is recorded correctly."""
    try:
        fv = float(value)
    except (ValueError, TypeError):
        return None
    ov = (unit_override or "").strip().upper()
    u = ov if ov in ("W", "KW") else (unit or "").upper()
    if u == "W":
        fv = fv / 1000.0
    elif u != "KW":
        fv = fv / 1000.0 if abs(fv) > 100 else fv
    if invert:
        fv = -fv
    return round(fv, 3)


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


# ── Historical carbon backfill (v2 -> v3 migration) ────────────────────────────
# The heatmap and Usage Stats now treat the stored carbon_intensity_g as the
# source of truth. Blocks created before CI first became available (the whole
# pre-postcode history, including everything carried over from v2) have a NULL
# intensity. The live recovery sweep (_recover_missing_carbon) can only reach
# blocks still inside the 4-day CI table, so this one-shot backfill pages the
# Carbon Intensity API's historical endpoint over the NULL span and writes the
# intensity straight onto the blocks. Gated on "postcode exists"; runs once
# (persistent marker); resumable (cursor); throttled to sub-14-day windows.
_CARBON_BACKFILL_MARKER = "carbon_backfill_state"   # store_meta key
_CARBON_RETRY_HOURS = 24                             # cool-down before re-attempting gaps
_carbon_backfill_running = False                    # in-process re-entry guard
_delete_active = False                              # a delete/purge job is mutating blocks


def set_delete_active(active: bool) -> None:
    """Flagged by the delete/purge workers around a block-removal job so the carbon
    backfill won't KICK OFF new work mid-delete (both share the single SQLite
    connection). An already-running backfill stays safe on its own — it re-reads each
    block before writing and skips any the delete removed."""
    global _delete_active
    _delete_active = bool(active)


def delete_in_progress() -> bool:
    return _delete_active


def _fetch_carbon_intensity_range(postcode: str, from_iso: str, to_iso: str) -> dict:
    """Fetch settled historical regional intensity for [from_iso, to_iso] and
    return {slot_from -> intensity_gco2}. The Carbon Intensity API serves at most
    a 14-day window per request, so callers must page. 'actual' is preferred over
    'forecast' (actuals settle ~24h after each slot; every backfill target is far
    older than that, so a real actual is always returned). Raises on HTTP/network
    error so the caller can persist a resume cursor and stop."""
    import urllib.request
    import json as _json

    url = (f"https://api.carbonintensity.org.uk/regional/intensity"
           f"/{from_iso}/{to_iso}/postcode/{postcode}")
    req = urllib.request.Request(url, headers={"Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=15) as resp:
        data = _json.loads(resp.read())

    raw = data.get("data", [])
    if isinstance(raw, dict):
        entries = raw.get("data", [])
    elif isinstance(raw, list) and raw:
        entries = raw[0].get("data", []) if "data" in raw[0] else raw
    else:
        entries = []

    out: dict = {}
    for slot in entries:
        io = slot.get("intensity", {}) or {}
        val = io.get("actual")
        if val is None:
            val = io.get("forecast")
        sf = slot.get("from")
        if sf and val is not None:
            out[sf.replace("Z", "").replace("+00:00", "")] = float(val)
    return out


def _nearest_intensity_from_map(ci_map: dict, block_start: str):
    """Nearest slot intensity (<=60 min) for *block_start* from a
    {slot_iso -> intensity} map. Tries the containing 30-min slot first (a direct
    hit for 30-min-aligned blocks), then falls back to the nearest within 60 min."""
    from datetime import datetime as _dt
    if not ci_map:
        return None
    try:
        bs = _dt.fromisoformat(str(block_start).replace("Z", "").split("+")[0])
    except Exception:
        return None
    floored = bs.replace(minute=(0 if bs.minute < 30 else 30),
                         second=0, microsecond=0)
    hit = ci_map.get(floored.strftime("%Y-%m-%dT%H:%M"))
    if hit is not None:
        return hit
    best = best_d = None
    for k, v in ci_map.items():
        try:
            kd = _dt.fromisoformat(k)
        except Exception:
            continue
        d = abs((bs - kd).total_seconds())
        if d <= 3600 and (best_d is None or d < best_d):
            best_d, best = d, v
    return best


async def _run_historical_carbon_backfill(window_days: int = 13,
                                          max_windows: int = 60) -> int:
    """One-shot historical carbon backfill. Pages the Carbon Intensity API over
    the span of NULL-intensity blocks in sub-14-day windows, writes carbon_g +
    carbon_intensity_g straight onto those blocks (NOT via the 4-day-pruned CI
    table), and records a resume cursor after each window so a restart continues.
    Idempotent: only touches NULL blocks; sets a done marker when no NULL blocks
    remain. Returns blocks backfilled this invocation.

    CONCURRENCY (this is why it is a coroutine, not a threaded worker): every
    BlockStore read/write here runs on the event-loop thread — the same thread
    that owns the engine's single SQLite connection (opened check_same_thread=
    False for a *single* thread). Only the blocking CI fetch is offloaded, via
    run_in_executor. The first cut ran the whole worker on an executor thread; its
    DB writes then raced the main-loop _drain_pass2_queue on that one shared
    connection and corrupted both (SQLITE_MISUSE: "bad parameter or other API
    misuse", "cannot commit - no transaction is active"). Running the DB work on
    the loop thread serialises it cooperatively with the drain — no shared-
    connection race, and no second connection / last-write-wins race either.

    window_days defaults to 13, NOT 14: the API's /regional/intensity/{from}/{to}
    range cap is effectively exclusive — a [00:00..00:00] 14-day request lands one
    half-hour period past the limit and returns HTTP 400. 13-day windows sit
    safely under it (one extra request over the whole history; negligible).

    `max_windows` bounds a single invocation; a larger history finishes across
    successive triggers via the cursor."""
    from datetime import datetime as _dt, timedelta as _td, timezone as _tz
    import asyncio as _aio
    _now = lambda: _dt.now(_tz.utc).replace(tzinfo=None).isoformat()
    # Cool-down before re-attempting gaps that a pass couldn't fill — so we self-heal
    # transient CI outages without hammering the API for genuinely-absent recent data.
    _retry_after = lambda: (_dt.now(_tz.utc).replace(tzinfo=None)
                            + _td(hours=_CARBON_RETRY_HOURS)).isoformat()
    if _store is None:
        return 0
    postcode = _get_postcode()
    if not postcode:
        return 0

    state = _store.get_meta(_CARBON_BACKFILL_MARKER, {}) or {}
    if state.get("done"):
        return 0

    rng = _store.get_missing_carbon_date_range()
    if not rng:
        _store.set_meta(_CARBON_BACKFILL_MARKER,
                        {"done": True, "completed_at": _now()})
        return 0

    lo_iso, hi_iso = rng
    cursor = state.get("cursor") or lo_iso
    try:
        cur_dt = _dt.fromisoformat(str(cursor).replace("Z", "").split("+")[0])
    except Exception:
        cur_dt = _dt.fromisoformat(str(lo_iso).replace("Z", "").split("+")[0])
    try:
        hi_dt = (_dt.fromisoformat(str(hi_iso).replace("Z", "").split("+")[0])
                 + _td(minutes=30))   # inclusive of the last slot
    except Exception:
        return 0

    try:
        loop = _aio.get_running_loop()
    except RuntimeError:
        loop = None

    def _apply_from_map(start, cmap):
        """Match a block to the nearest CI slot in `cmap` and persist carbon in
        place. Shared by the wide-window and the narrow per-day straggler pass.
        Returns True if written, None if no CI slot matched (a FETCH gap the
        straggler pass should refetch), False on a block/persist error (leave it for
        the next pass — a narrow refetch wouldn't help)."""
        intensity = _nearest_intensity_from_map(cmap, start)
        if intensity is None:
            return None
        block = _store.get_block_dict_by_start(start)
        if not block:
            return False
        _apply_intensity_to_block(block, intensity)
        try:
            _persist_block_carbon(block, start)
            return True
        except Exception as e:
            logger.warning("_run_historical_carbon_backfill: persist failed %s: %s",
                           start, e)
            return False

    backfilled = 0
    windows = 0
    while cur_dt < hi_dt and windows < max_windows:
        win_end = min(cur_dt + _td(days=window_days), hi_dt)
        f_iso = cur_dt.strftime("%Y-%m-%dT%H:%MZ")
        t_iso = win_end.strftime("%Y-%m-%dT%H:%MZ")
        # Resolve each block's region from its config period and fetch CI PER
        # region — so a mid-history property move attributes the correct region to
        # old blocks, not the current postcode. Blocks with no resolvable region
        # were already excluded by get_block_starts_missing_carbon_in_range.
        starts = _store.get_block_starts_missing_carbon_in_range(
            cur_dt.isoformat(), win_end.isoformat())
        by_oc: dict = {}
        for start in starts:
            oc = _store.get_postcode_prefix_at(start)[0] or postcode
            if oc:
                by_oc.setdefault(oc, []).append(start)
        for oc, oc_starts in by_oc.items():
            try:
                # Offload ONLY the blocking network fetch; DB stays on this thread.
                if loop is not None:
                    ci_map = await loop.run_in_executor(
                        None, _fetch_carbon_intensity_range, oc, f_iso, t_iso)
                else:
                    ci_map = _fetch_carbon_intensity_range(oc, f_iso, t_iso)
            except Exception as e:
                logger.warning("_run_historical_carbon_backfill: fetch failed "
                               "%s..%s (%s): %s — pausing, will resume",
                               f_iso, t_iso, oc, e)
                _store.set_meta(_CARBON_BACKFILL_MARKER,
                                {"cursor": cur_dt.isoformat(), "done": False})
                return backfilled
            missing = []
            for start in oc_starts:
                r = _apply_from_map(start, ci_map)
                if r is True:
                    backfilled += 1
                elif r is None:
                    missing.append(start)     # fetch gap → narrow refetch below
                # False (block/persist error) → leave for the next pass
            # Straggler pass: a thin wide-window CI response (a transient regional
            # outage, or a slow/partial return) leaves gaps the forward-only cursor
            # would otherwise strand. Retry the still-NULL slots with a NARROW
            # per-day fetch — the calmest request — which reliably returns what the
            # 13-day window missed. Mirrors the pricing single-slot recovery.
            for _day in sorted({s[:10] for s in missing}):
                try:
                    _d0 = _dt.fromisoformat(_day)
                    df = _d0.strftime("%Y-%m-%dT00:00Z")
                    dto = (_d0 + _td(days=1)).strftime("%Y-%m-%dT00:00Z")
                    if loop is not None:
                        day_map = await loop.run_in_executor(
                            None, _fetch_carbon_intensity_range, oc, df, dto)
                    else:
                        day_map = _fetch_carbon_intensity_range(oc, df, dto)
                except Exception as e:
                    logger.info("_run_historical_carbon_backfill: straggler refetch "
                                "%s (%s) failed: %s", _day, oc, e)
                    continue
                for start in [m for m in missing if m[:10] == _day]:
                    if _apply_from_map(start, day_map) is True:
                        backfilled += 1

        cur_dt = win_end
        windows += 1
        _store.set_meta(_CARBON_BACKFILL_MARKER,
                        {"cursor": cur_dt.isoformat(), "done": False})
        if loop is not None:
            await _aio.sleep(0)   # yield so ticks/reads interleave between windows

    if cur_dt < hi_dt:
        # Hit the per-invocation window cap; resume next trigger from the cursor.
        logger.info("_run_historical_carbon_backfill: paused after %d window(s), "
                    "%d block(s); will resume from cursor", windows, backfilled)
        return backfilled

    # Full span swept. Decide the marker by what NULL blocks ACTUALLY remain — not
    # by the cursor. A per-block persist failure leaves a gap the forward cursor
    # has already moved past; marking done on cursor alone (the original bug) would
    # strand it. Re-query the truth instead.
    remaining = _store.get_missing_carbon_date_range()
    if remaining is None:
        _store.set_meta(_CARBON_BACKFILL_MARKER,
                        {"done": True, "completed_at": _now()})
        if backfilled:
            # OFF the loop: a synchronous render over 2+ years of freshly-carbonned
            # blocks takes ~90s and stalls the HA heartbeat → reconnect →
            # re-startup → re-render storm (the exact failure a large historical
            # backfill produced). This runs once, only when the span is fully swept.
            await _generate_charts_offloaded()
        logger.info("_run_historical_carbon_backfill: complete — %d block(s) "
                    "backfilled", backfilled)
    elif backfilled > 0:
        # Progress made but gaps remain (transient persist failures). Retry next
        # trigger from the earliest still-NULL block — bounded so a permanently
        # stuck slot can't loop forever re-hitting the API.
        attempts = int(state.get("retry_attempts", 0)) + 1
        if attempts >= 6:
            _store.set_meta(_CARBON_BACKFILL_MARKER,
                            {"done": True, "completed_at": _now(),
                             "unfilled_from": remaining[0],
                             "retry_after": _retry_after()})
            logger.warning("_run_historical_carbon_backfill: gaps remain from %s "
                           "after %d rapid retries — cooling down, will re-attempt "
                           "later", remaining[0], attempts)
        else:
            _store.set_meta(_CARBON_BACKFILL_MARKER,
                            {"cursor": remaining[0], "done": False,
                             "retry_attempts": attempts})
            logger.info("_run_historical_carbon_backfill: %d block(s) backfilled; "
                        "gaps remain from %s — will retry (%d)",
                        backfilled, remaining[0], attempts)
    else:
        # A whole pass filled nothing yet NULL blocks remain. Likely a transient CI
        # outage (recent slots may not have settled; a regional window came back
        # thin). Cool down rather than freeze forever: record retry_after so a later
        # trigger re-attempts. Old slots almost always settle, so this self-heals.
        _store.set_meta(_CARBON_BACKFILL_MARKER,
                        {"done": True, "completed_at": _now(),
                         "unfilled_from": remaining[0],
                         "retry_after": _retry_after()})
        logger.warning("_run_historical_carbon_backfill: span swept but NULL "
                       "blocks remain from %s and none filled this pass — cooling "
                       "down, will re-attempt after %s", remaining[0], _retry_after())
    return backfilled


def _maybe_backfill_historical_carbon() -> None:
    """Schedule the one-shot historical backfill if a postcode exists and it
    hasn't completed. Safe to call repeatedly (engine startup + every CI tick) —
    guarded by an in-process flag and the persistent done marker. The CI-tick
    trigger is what catches a postcode added *after* first boot.

    Dispatches the async worker as a loop TASK (create_task), not an executor
    thread: all BlockStore access must run on the event-loop thread that owns the
    engine's single SQLite connection. Self-heals a stale done marker — an earlier
    (concurrency-broken) run could mark done while transient failures left NULL
    blocks behind; if gaps remain and we didn't already conclude they're
    unattributable, re-arm and let the worker fill them."""
    global _carbon_backfill_running
    try:
        if _store is None or not _get_postcode() or _carbon_backfill_running:
            return
        if _store.get_meta("carbon_paused", None):     # manual kill switch
            return
        # Don't compete with an active historical import — both do heavy work on
        # the engine's single DB connection. Defer; the next CI tick re-checks
        # (and the import's completion re-arms the marker anyway).
        if api_import_running():
            logger.info("_maybe_backfill_historical_carbon: import job active — deferring")
            return
        if delete_in_progress():
            logger.info("_maybe_backfill_historical_carbon: delete job active — deferring")
            return
        state = _store.get_meta(_CARBON_BACKFILL_MARKER, {}) or {}
        if state.get("done"):
            remaining = _store.get_missing_carbon_date_range()
            if remaining is None:
                return   # genuinely complete — no eligible NULL carbon blocks left
            # Gaps remain. A prior pass may have tombstoned them (unfilled_from) after
            # a transient CI outage — but old slots settle and are almost always
            # fillable later, so we must NOT freeze forever. Re-attempt on a slow
            # cadence: honour retry_after so we don't hammer the API for genuinely
            # recent/absent data, but always re-arm once the cool-down has passed.
            ra = state.get("retry_after")
            if state.get("unfilled_from") and ra:
                from datetime import datetime as _dt3, timezone as _tz3
                try:
                    if _dt3.now(_tz3.utc).replace(tzinfo=None).isoformat() < ra:
                        return   # still cooling down; re-check after retry_after
                except Exception:
                    pass
            logger.info("_maybe_backfill_historical_carbon: NULL carbon blocks remain "
                        "from %s — re-arming (retry)", remaining[0])
            _store.set_meta(_CARBON_BACKFILL_MARKER,
                            {"cursor": remaining[0], "done": False})
        import asyncio as _aio
        try:
            loop = _aio.get_running_loop()
        except RuntimeError:
            loop = None
        if loop is None:
            return   # no running loop to host the task (non-async caller)

        async def _task():
            global _carbon_backfill_running
            try:
                await _run_historical_carbon_backfill()
            except Exception as e:
                logger.warning("_maybe_backfill_historical_carbon: worker "
                               "failed: %s", e)
            finally:
                _carbon_backfill_running = False

        _carbon_backfill_running = True
        loop.create_task(_task())
    except Exception as e:
        logger.warning("_maybe_backfill_historical_carbon: schedule failed: %s", e)
        _carbon_backfill_running = False


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
        # CI is freshly populated — backfill any blocks left NULL by an outage
        # gap-fill (recovers without waiting for a restart).
        try:
            _recover_missing_carbon()
        except Exception as e:
            logger.warning("_tick_carbon_intensity: carbon recovery failed: %s", e)
        # Historical backfill (v2->v3 migration). Run-once; this trigger is what
        # picks it up when a postcode is added after first boot. No-op once done.
        try:
            _maybe_backfill_historical_carbon()
        except Exception as e:
            logger.warning("_tick_carbon_intensity: historical backfill "
                           "schedule failed: %s", e)
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
    global _engine_ha
    _engine_ha = ha

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

    # ── 3.0.0: Mini telemetry collection (CAD-like) ───────────────────────
    # When the Mini provisional layer is active (api+mini, no local sensor),
    # collect smart-meter telemetry into the CURRENT block's import reads buffer
    # around each boundary — starting ~20s before and continuing until a read
    # timestamped past the boundary lands. The existing post-boundary-read wait
    # + finalise interpolation then bracket the boundary and compute the closing
    # register uniformly with CAD. Mini reads carry their OWN readAt timestamps.
    if _kraken_mini_reader is not None and current_block.get("start"):
        try:
            await _collect_mini_into_block(current_block, now, block_minutes)
        except Exception as _mc_e:
            logger.warning("_engine_tick: mini collection failed: %s", _mc_e)

    # ── 2.10.0: amend provisional sub-meter blocks ────────────────────────
    # Skip entirely when a gap marker is active — gap-seed reads in the
    # rolling buffer look identical to real reads and must never be used
    # as interpolation anchors for boundary correction.
    if not has_gap_marker(current_block):
        try:
            _amend_provisional_sub_meter_blocks(ha, current_block)
        except Exception as _amp_e:
            logger.warning("_engine_tick: provisional amendment failed: %s", _amp_e)

    # ── 3.0.0: drain the DCC PASS 2 re-run queue ──────────────────────────
    # Blocks whose Kraken DCC settlement has arrived (needs_pass2_rerun=1) are
    # re-run against imp_kwh_api here. No-op until the ingester sets the flag.
    #
    # BL-1: NOT gap-guarded. This was previously inside the `not
    # has_gap_marker(...)` block above, which globally froze settlement
    # application for the WHOLE history while any gap marker was live — an
    # outage on one day stopped every other day's settled figures from being
    # applied to billing until the marker cleared. Unlike the amendment above,
    # the drain takes its work queue from the DB and re-costs historical blocks
    # by block_start; it never reads the current block or the rolling buffer, so
    # gap-seed reads cannot contaminate it. Settling a gap-filled block is in
    # fact desirable — the authoritative DCC figure replaces the interpolated
    # guess (the ingester already counts these as `flagged_interpolated`).
    try:
        _drain_pass2_queue(ha)
    except Exception as _dpq_e:
        logger.warning("_engine_tick: PASS 2 re-run drain failed: %s", _dpq_e)

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

    # Dispatch-slot capture (5-min throttled) — catch smart-charge slots while
    # still 'planned', before they convert to completed/unknown.
    try:
        await _tick_dispatch_capture()
    except Exception as _dce:
        logger.warning("_engine_tick: dispatch capture raised: %s", _dce)

    # Block lifecycle
    updated_block = ensure_correct_block(ha, current_block, now, last_known_rates=_post_gap_rates)
    block_changed = updated_block.get("start") != current_block.get("start")

    if block_changed or periodic_checkpoint or near_boundary:
        updated_block["_last_checkpoint"] = now.isoformat()
        _store.save_current_block(updated_block)

    # Write power history row every tick if a live power source is available
    try:
        cfg = load_config()
        power_sensor = None
        power_source = None
        _power_invert = False
        _power_unit_ov = None
        for m_data in cfg.get("meters", {}).values():
            meta = (m_data or {}).get("meta", {}) or {}
            if not meta.get("sub_meter"):
                power_sensor = meta.get("power_sensor")
                power_source = meta.get("power_source")
                # invert / unit-override apply to the user's OWN sensor only,
                # never to the BCD fallback (whose convention we don't control).
                if power_sensor:
                    _power_invert = bool(meta.get("power_invert", False))
                    _power_unit_ov = meta.get("power_unit") or None
                break
        # Auto-adopt BottlecapDave's current_demand when no sensor is configured —
        # it's a free local HA entity, so we can sample it every tick.
        if not power_sensor:
            bcd = (_detected_integrations or {}).get("bcd") or {}
            if bcd.get("found"):
                power_sensor = bcd.get("demand_sensor")

        net_kw = None
        if power_sensor and ha:
            raw_kw = ha.get_state(power_sensor)
            if raw_kw not in (None, "unknown", "unavailable"):
                # Respect the sensor's unit (BCD current_demand is in watts).
                _unit = None
                try:
                    _unit = (ha.get_attributes(power_sensor) or {}).get(
                        "unit_of_measurement")
                except Exception:
                    pass
                net_kw = _power_value_to_kw(raw_kw, _unit,
                                            _power_unit_ov, _power_invert)
        elif (not power_sensor and power_source == "mini"
              and _kraken_mini_reader
              and getattr(_kraken_mini_reader, "device_id", None)):
            # Octopus Mini source — poll it here (throttled) so the 48h history
            # fills continuously, independent of whether the page is open.
            net_kw = await _poll_mini_demand_kw(now)

        if net_kw is not None:
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
            # Use inverter_power_sensor for batteries, device_power_sensor for EV/heat pump.
            # Pick the matching invert/unit-override flags for whichever drives it.
            if inv_s:
                power_s = inv_s
                _sub_invert = bool(meta.get("inverter_power_invert", False))
                _sub_unit_ov = meta.get("inverter_power_unit") or None
            else:
                power_s = dev_s
                _sub_invert = bool(meta.get("device_power_invert", False))
                _sub_unit_ov = meta.get("device_power_unit") or None
            if power_s and ha:
                v = ha.get_state(power_s)
                if v not in (None, "unknown", "unavailable"):
                    try:
                        unit = (ha.get_attributes(power_s) or {}).get(
                            "unit_of_measurement", "")
                    except Exception:
                        unit = ""
                    # Shared converter: unit attr → kW, override when declared
                    # unit is wrong/absent, then invert. Falls back to the
                    # magnitude heuristic when the unit is missing (previously this
                    # path assumed kW on a missing unit — a W sensor read 1000× high).
                    inv_val = _power_value_to_kw(v, unit, _sub_unit_ov, _sub_invert)
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


def _read_kraken_creds_file() -> dict:
    """Read credentials from KRAKEN_CREDS_PATH, or {} if absent/unreadable."""
    try:
        with open(KRAKEN_CREDS_PATH, "r") as f:
            import json as _json
            data = _json.load(f)
        return data if isinstance(data, dict) else {}
    except (FileNotFoundError, ValueError, OSError):
        return {}


def save_kraken_credentials(api_key, account_number, base_url=None) -> None:
    """Persist API credentials to their own file (0600), outside the DB so they
    are never included in backups. Passing None/'' for api_key clears the file
    (disconnect). Account/base_url are stored alongside."""
    import json as _json
    ensure_dir(DATA_DIR)
    key = (api_key or "").strip()
    if not key:
        # Disconnect: remove the file entirely.
        try:
            os.remove(KRAKEN_CREDS_PATH)
        except FileNotFoundError:
            pass
        return
    payload = {"api_key": key,
               "account_number": (account_number or "").strip() or None,
               "base_url": (base_url or "").strip() or None}
    tmp = KRAKEN_CREDS_PATH + ".tmp"
    with open(tmp, "w") as f:
        _json.dump(payload, f)
    try:
        os.chmod(tmp, 0o600)
    except OSError:
        pass
    os.replace(tmp, KRAKEN_CREDS_PATH)


def _kraken_env() -> dict:
    """Resolve Kraken credentials. Prefers the in-app credentials file
    (KRAKEN_CREDS_PATH); falls back to environment variables for backward
    compatibility (the 2.x add-on-config path). Normalises empties to None.
    Returns {api_key, account_number, base_url}."""
    def _clean(v):
        v = (v or "").strip()
        return None if v in ("", "null", "None") else v
    creds = _read_kraken_creds_file()
    if creds.get("api_key"):
        return {
            "api_key": _clean(creds.get("api_key")),
            "account_number": _clean(creds.get("account_number")),
            "base_url": _clean(creds.get("base_url")),
        }
    return {
        "api_key": _clean(os.environ.get("KRAKEN_API_KEY")),
        "account_number": _clean(os.environ.get("KRAKEN_ACCOUNT_NUMBER")),
        "base_url": _clean(os.environ.get("KRAKEN_BASE_URL")),
    }


def has_kraken_credentials() -> bool:
    """True if a Kraken API key is configured (creds file or env). Used to guard
    against activating an API mode with no credentials (Change Setup cad→api)."""
    try:
        return bool(_kraken_env().get("api_key"))
    except Exception:
        return False


async def connect_kraken_now() -> dict:
    """(Re)build the Kraken client from current credentials and run discovery
    immediately — used after the user enters credentials in-app, so they get a
    live connection result without restarting. Returns a status dict:
      {ok, connected, account_number?, import_mpan?, export_mpan?, mini?, detail?}
    Never raises."""
    env = _kraken_env()
    if not env["api_key"]:
        return {"ok": False, "connected": False, "detail": "no_api_key"}
    try:
        await _kraken_startup_discovery(force=True)
    except Exception as e:
        return {"ok": False, "connected": False, "detail": str(e)}
    if _kraken_client is None or not _kraken_discovery:
        return {"ok": False, "connected": False, "detail": "discovery_failed"}
    imp = _kraken_discovery.get("import") or {}
    exp = _kraken_discovery.get("export") or {}
    # Also (re)build rate schedules so reconcile has them without waiting a poll.
    try:
        await _refresh_kraken_rate_schedules()
    except Exception:
        pass
    # NB: the DCC poll task is launched at the END of engine_startup (after the
    # store + client are rebuilt), NOT here — launching it mid-connect raced the
    # config-save's engine_startup teardown of those shared resources.
    return {
        "ok": True, "connected": True,
        "account_number": _kraken_discovery.get("account_number"),
        "import_mpan": imp.get("mpan"),
        "export_mpan": exp.get("mpan") or None,
        "mini": _kraken_mini_reader is not None,
    }


async def disconnect_kraken() -> dict:
    """Destructive 'disconnect Octopus' action (MODE-UI §5): clear stored API
    credentials and API-derived runtime state, stop polling immediately, tear
    down the live client + Mini reader, and land the install on local-sensor
    mode ('cad').

    Kept on purpose: historical DCC-settled billing data (per-block imp_kwh_api /
    exp_kwh_api + materialised cost) — that's billing history, not a credential —
    and the billing source, so existing settled blocks keep their materialisation
    and future local blocks bill off local figures exactly as a cad-only install
    does. NOT re-priced.

    Lands on 'cad' because api / api+mini with no credentials is non-functional;
    'cad' makes mode_uses_api() False and surfaces the local meter-sensor fields
    in the config UI, so the user can add a sensor (or reconnect) to resume. Runs
    on the engine loop (touches the asyncio poll-task handle). Returns
    {ok, mode, had_credentials}. Never raises.
    """
    global _kraken_client, _kraken_mini_reader, _kraken_discovery
    global _kraken_rate_schedules, _kraken_standing_schedule
    had = has_kraken_credentials()
    try:
        # 1. Stop the live poll loop and drop all in-memory API state, so polling
        #    stops NOW (not just on the next restart) — the existing 'send empty
        #    api_key' path only deleted the file and left the in-memory client
        #    polling.
        _cancel_kraken_poll_task()
        _kraken_client = None
        _kraken_mini_reader = None
        _kraken_discovery = None
        _kraken_rate_schedules = {}
        _kraken_standing_schedule = None
        # 2. Remove the credentials file (the actual disconnect).
        save_kraken_credentials(None, None)
        # 3. Wipe API-derived progress markers so a future reconnect starts clean
        #    (fresh backfill, re-takes the pre-live rollback snapshot). Mode and
        #    billing source also live in kraken_state but are NOT progress markers
        #    — they are set / preserved explicitly, never blanket-deleted.
        try:
            from kraken_ingester import _STATE_LAST_POLL as _LAST_POLL_KEY
        except Exception:
            _LAST_POLL_KEY = "last_poll_utc"
        for k in (_LAST_POLL_KEY, _STATE_LAST_SWEEP, _KRAKEN_SNAPSHOT_DONE_KEY):
            try:
                _store.delete_kraken_state(k)
            except Exception:
                pass
        # 4. Land on local-sensor mode (api-without-creds is invalid).
        set_data_source_mode("cad")
        logger.info("disconnect_kraken: credentials cleared (had=%s), API client/"
                    "Mini/discovery torn down, progress state wiped, mode→cad", had)
        return {"ok": True, "mode": "cad", "had_credentials": had}
    except Exception as e:
        logger.error("disconnect_kraken: failed: %s", e)
        return {"ok": False, "detail": str(e), "had_credentials": had}


def _cancel_kraken_poll_task() -> None:
    """Cancel the running DCC poll task, if any. Called before engine_startup
    tears down the store/client so an in-flight backfill can't operate on closed
    resources. Best-effort; the task is relaunched at the end of startup."""
    global _kraken_poll_task_handle
    h = _kraken_poll_task_handle
    if h is not None and not h.done():
        h.cancel()
        logger.info("_cancel_kraken_poll_task: cancelled in-flight poll")
    _kraken_poll_task_handle = None


def _ensure_kraken_poll_task_running() -> bool:
    """Launch kraken_poll_task on the engine loop if it isn't already running.
    Idempotent and safe to call repeatedly (e.g. on every credential save).
    Returns True if a task is running afterwards.

    If the engine loop isn't up yet (_engine_ha is None), we're still in boot —
    main() launches the poll task via its gather() a moment later, so this is
    expected and NOT a problem. We log it at INFO (not WARNING) to avoid a
    misleading 'engine loop not up' alarm on a normal pre-configured startup."""
    global _kraken_poll_task_handle
    if _engine_ha is None:
        logger.info("_ensure_kraken_poll_task_running: engine loop not up yet "
                    "— poll task will be launched by startup")
        return False
    h = _kraken_poll_task_handle
    if h is not None and not h.done():
        # Already running — nothing to do.
        return True
    try:
        loop = asyncio.get_event_loop()
        _kraken_poll_task_handle = loop.create_task(kraken_poll_task(_engine_ha))
        logger.info("_ensure_kraken_poll_task_running: poll task launched")
        return True
    except Exception as e:
        logger.warning("_ensure_kraken_poll_task_running: could not launch: %s", e)
        return False


async def _kraken_startup_discovery(force: bool = False) -> None:
    """If a Kraken API key is configured, construct the client and run
    auto_discover ONCE at startup, logging what was found.

    force=True bypasses the fresh-setup guard — used by connect_kraken_now()
    when the user explicitly enters credentials in the wizard (the DB may still
    be empty mid-setup, but the connect is intentional).

    This is the 4b-i gate: it performs only the single read-only account fetch
    that discovery needs, and does NOT poll consumption or write anything. The
    operator verifies the logged MPANs/serials/account/tariff before the
    scheduled (dry-run) polling is enabled in a later step. Any failure is
    logged and swallowed — API misconfiguration must never break engine
    startup or the CAD pipeline.
    """
    global _kraken_client, _kraken_discovery, _kraken_account_number
    env = _kraken_env()
    if not env["api_key"]:
        logger.info("kraken_discovery: no API key configured — API integration off")
        return
    # API-activation gate: only auto-activate the supplier API when a mode has
    # been EXPLICITLY chosen (by the survey) AND that mode uses the API. A flat/
    # pre-survey DB reports mode 'unset' → no auto-activation, even though an
    # orphaned credentials file may still exist (it lives outside the DB, so a
    # flatten doesn't clear it). The wizard re-establishes the mode + creds.
    # force=True (the wizard's own connect) bypasses this.
    if not force and not mode_uses_api():
        logger.info("kraken_discovery: data-source mode is '%s' — API not "
                    "auto-activated (awaiting survey / not an API mode)",
                    get_data_source_mode())
        return
    try:
        from kraken_api_client import KrakenAPIClient
    except Exception as e:
        logger.warning("kraken_discovery: client import failed: %s", e)
        return
    try:
        kwargs = {"account_number": env["account_number"]}
        if env["base_url"]:
            kwargs["base_url"] = env["base_url"]
        # Close any previous client first — connect_kraken_now() re-runs this on
        # every in-app credential save, and leaking the old aiohttp session
        # ("Unclosed client session") accumulates connections.
        if _kraken_client is not None:
            try:
                await _kraken_client.close()
            except Exception:
                pass
            _kraken_client = None
        _kraken_client = KrakenAPIClient(env["api_key"], **kwargs)
        # test_connection first — clean credential check, never raises.
        conn = await _kraken_client.test_connection()
        if not conn.get("ok"):
            logger.warning("kraken_discovery: connection check failed: %s",
                           conn.get("detail"))
            return
        acct = env["account_number"] or conn.get("account_number")
        _kraken_account_number = acct
        disc = await _kraken_client.auto_discover(acct)
        _kraken_discovery = disc

        imp = disc.get("import") or {}
        exp = disc.get("export") or {}
        from kraken_api_client import _mask as _m
        logger.info("=" * 60)
        logger.info("kraken_discovery: account verified — REVIEW BEFORE ENABLING POLLING")
        logger.info("  account_number : %s", _m(disc.get("account_number")))
        logger.info("  properties     : %s", disc.get("properties"))
        logger.info("  IMPORT mpan=%s serial=%s tariff=%s product=%s",
                    _m(imp.get("mpan")), _m(imp.get("serial")),
                    imp.get("tariff_code"), imp.get("product_code"))
        if exp:
            logger.info("  EXPORT mpan=%s serial=%s tariff=%s product=%s",
                        _m(exp.get("mpan")), _m(exp.get("serial")),
                        exp.get("tariff_code"), exp.get("product_code"))
        else:
            logger.info("  EXPORT : none detected")
        for w in disc.get("warnings", []):
            logger.warning("  ⚠ %s", w)
        logger.info("kraken_discovery: NO polling scheduled yet (4b-i). Verify the "
                    "above, then enable scheduled dry-run in the next step.")
        logger.info("=" * 60)
        # Chunk 7: discover an Octopus Mini and, if present, wire the
        # near-real-time provisional import layer (api+mini). Best-effort:
        # any failure leaves the Mini layer off and DCC still reconciles.
        await _maybe_setup_mini()
        # Point-of-truth config-state line: now the reader is resolved, report
        # the ACTUAL Mini provisional state (the startup dump could only report
        # eligibility, as this runs later in startup).
        logger.info("config-state: mini_provisional=%s",
                    "active" if _kraken_mini_reader is not None else "off")
    except Exception as e:
        logger.warning("kraken_discovery: discovery failed (non-fatal): %s", e)


def _teardown_mini_if_no_api() -> bool:
    """Tear down a stale Mini reader when the mode no longer uses the API.

    On a Change Setup api→cad transition the engine restarts (engine_startup),
    but _kraken_startup_discovery returns early in a non-API mode and never calls
    _maybe_setup_mini — so a Mini reader wired in a prior api+mini session would
    PERSIST (same process) and keep collecting into cad blocks. This clears it.
    Idempotent; returns True if it tore a reader down. Safe to call any time.
    """
    global _kraken_mini_reader
    if not mode_uses_api() and _kraken_mini_reader is not None:
        _kraken_mini_reader = None
        logger.info("teardown_mini: mode=%s — tore down stale Mini reader "
                    "(API path off)", get_data_source_mode())
        return True
    return False


async def _maybe_setup_mini() -> None:
    """If a Mini smart device exists AND there is no local meter read source,
    build the boundary reader and register it on the block-boundary hook.

    Gating rule (per design): Mini is a FALLBACK for when EMT has no local
    import feed. If a CAD / main-meter import read sensor is configured, that is
    the authoritative source and Mini must NEVER be used — even if a Mini device
    exists and even if mode were set to api+mini. Local sensors always win, so
    Mini and CAD can never contend for the same block's import. Best-effort;
    never raises."""
    global _kraken_mini_reader
    if _kraken_client is None:
        return
    if _has_local_import_sensor():
        logger.info("mini_setup: local meter import sensor present — Mini not "
                    "used (CAD/local is authoritative)")
        return
    # Mini is AUTOMATIC, not a survey choice: the survey derives cad/cad+api/api
    # only. In a no-local API mode, if a Mini device is actually present it
    # elevates behaviour to api+mini at runtime. So the gate is "mode uses API
    # and there's no local feed" — then we try to discover a device. (A cad/
    # cad+api setup has a local feed and is already excluded above.)
    if not mode_uses_api():
        logger.info("mini_setup: mode is %s (no API) — Mini layer off",
                    get_data_source_mode())
        return
    try:
        device_id = await _kraken_client.get_device_id(_kraken_account_number)
    except Exception as e:
        logger.warning("mini_setup: device discovery failed: %s", e)
        return
    if not device_id:
        logger.info("mini_setup: no Octopus Mini on account — staying plain API "
                    "(DCC settlement only)")
        return
    try:
        from kraken_mini import MiniBoundaryReader
        _kraken_mini_reader = MiniBoundaryReader(_kraken_client, device_id)
        # 3.0.0: Mini reads are now collected per-tick into the current block's
        # import buffer (CAD-like) and bracketed/interpolated by finalise — see
        # _collect_mini_into_block. We deliberately do NOT register the old
        # post-finalise boundary callback here: that path is kept defined as a
        # dormant fallback, but registering it too would double-write the block.
        logger.info("mini_setup: Mini provisional import layer ACTIVE "
                    "(no local sensor; per-tick boundary collection → "
                    "provisional, DCC reconciles)")
    except Exception as e:
        logger.warning("mini_setup: failed to wire Mini reader: %s", e)
        _kraken_mini_reader = None


def _has_local_import_sensor() -> bool:
    """True if any non-sub main meter has a local import 'read' sensor
    configured — i.e. a CAD/local feed exists and Mini must stand down."""
    try:
        cfg = load_config()
        return any(
            (meter.get("channels") or {}).get("import", {}).get("read")
            for meter in (cfg.get("meters") or {}).values()
            if not (meter.get("meta") or {}).get("sub_meter")
        )
    except Exception:
        # If config can't be read, assume a local sensor MIGHT exist and keep
        # Mini off — the safe default (never contend with a possible CAD feed).
        return True


def _main_meter_id() -> "str | None":
    """Return the id of the main (non-sub) meter from config, or None."""
    try:
        cfg = load_config()
        for mid, meter in (cfg.get("meters") or {}).items():
            if not (meter.get("meta") or {}).get("sub_meter"):
                return mid
    except Exception:
        pass
    return None


def _mini_collection_window_open(current_block: dict, now: datetime,
                                 block_minutes: int) -> bool:
    """True when we should be collecting Mini telemetry for the CURRENT block's
    upcoming boundary: from ~20s BEFORE the block end until a read timestamped
    at/after the boundary has landed (bracketing complete). The reader itself
    enforces the per-boundary call cap and drift pacing."""
    try:
        start = datetime.fromisoformat(current_block["start"])
    except (ValueError, KeyError, TypeError):
        return False
    boundary = start + timedelta(minutes=block_minutes)
    seconds_to_boundary = (boundary - now).total_seconds()
    # Open from 20s before the boundary onward (and remain open past it — the
    # reader stops itself once the post-boundary point is collected).
    return seconds_to_boundary <= _MINI_COLLECT_LEAD_S


async def _collect_mini_into_block(current_block: dict, now: datetime,
                                   block_minutes: int) -> None:
    """Drive Mini telemetry collection into the current block's main-import
    reads buffer around its boundary. No-op outside the collection window or if
    a local import sensor exists (CAD authoritative)."""
    reader = _kraken_mini_reader
    if reader is None:
        return
    if _has_local_import_sensor():
        return
    if not _mini_collection_window_open(current_block, now, block_minutes):
        return

    try:
        start = datetime.fromisoformat(current_block["start"])
    except (ValueError, KeyError, TypeError):
        return
    boundary_iso = iso(start + timedelta(minutes=block_minutes))

    main_id = _main_meter_id()
    if not main_id:
        return
    meters = current_block.setdefault("meters", {})
    meter_block = meters.setdefault(
        main_id, {"meta": {}, "channels": {}, "interpolated": False})
    channel = meter_block["channels"].setdefault(
        "import", {"reads": [], "rates": []})

    await reader.collect_into(channel["reads"], boundary_iso, now)
    # Persist the in-progress reads so a restart mid-collection isn't lost.
    try:
        _store.save_current_block(current_block)
    except Exception:
        pass


def _mini_boundary_callback(boundary_iso: str) -> None:
    """Fired (sync) at each block boundary. Schedules the async Mini read so the
    engine isn't blocked on GraphQL. Best-effort; never raises into the engine."""
    reader = _kraken_mini_reader
    if reader is None:
        return
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            loop.create_task(_mini_read_and_apply(reader, boundary_iso))
    except Exception as e:
        logger.warning("mini: could not schedule boundary read: %s", e)


async def _mini_read_and_apply(reader, boundary_iso: str) -> None:
    """Acquire the interpolated boundary import register read and apply it to
    the just-closed block as its closing import read, then re-finalise. All
    best-effort; on any miss, the block stays as-is and DCC reconciles later."""
    try:
        read = await reader.read_at_boundary(boundary_iso)
    except Exception as e:
        logger.warning("mini: boundary read failed for %s: %s", boundary_iso, e)
        return
    if not read:
        return
    try:
        _apply_mini_boundary_read(boundary_iso, read)
    except Exception as e:
        logger.warning("mini: apply failed for %s: %s", boundary_iso, e)


def _apply_mini_boundary_read(boundary_iso: str, read: dict) -> None:
    """Apply an interpolated Mini import-register reading at a block boundary.

    The boundary is the END of the block that just finalised; `read` is the
    interpolated cumulative import register at that instant. The block's import
    kWh = this boundary's register − the PREVIOUS boundary's register (cumulative
    register deltas, exactly as CAD computes from boundary reads).

    We persist the boundary register as the block's Mini reading and store the
    derived provisional import kWh via the store's Mini path, flagged
    provisional + sourced 'kraken_mini'. DCC settlement later overwrites it.

    Conservative + best-effort: if we don't yet have a previous boundary
    register (cold start, or a prior gap), we record THIS boundary's register as
    the anchor for the next block and store no kWh this time — the next boundary
    will have its pair. Never raises.
    """
    global _kraken_mini_last_register
    reg = read.get("value")
    if reg is None:
        return
    # Defence in depth: never let Mini write onto a block if a local import
    # sensor exists (CAD is authoritative). Mirrors the setup-time gate.
    if _has_local_import_sensor():
        return
    prev = _kraken_mini_last_register
    # Always advance the anchor so the next boundary can delta against this one.
    _kraken_mini_last_register = {"ts": boundary_iso, "value": reg}

    if prev is None:
        logger.info("mini: anchored first boundary register at %s (%.3f); kWh "
                    "from next boundary", boundary_iso, reg)
        return

    # The just-closed block starts at the previous boundary and ends at this one.
    block_start = prev["ts"]
    imp_kwh = round(reg - prev["value"], 6)
    if imp_kwh < 0:
        # Register went backwards (meter reset / bad point) — don't store a
        # negative; re-anchor and defer to DCC for this block.
        logger.warning("mini: register decreased %.3f→%.3f at %s — skipping, "
                       "DCC will reconcile", prev["value"], reg, boundary_iso)
        return
    try:
        res = _store.store_mini_import(block_start, "electricity_main", imp_kwh)
        if res.get("status") == "stored":
            logger.info("mini: provisional import %.3f kWh for block %s "
                        "(boundary %s)", imp_kwh, block_start, boundary_iso)
        elif res.get("status") == "missing_block":
            logger.info("mini: no block at %s for provisional import — skipped",
                        block_start)
    except Exception as e:
        logger.warning("mini: store_mini_import failed for %s: %s",
                       block_start, e)


_kraken_mini_last_register = None   # {"ts": boundary_iso, "value": register}


def _kraken_backfill_days() -> int:
    """Compute the first-run backfill window: from the oldest block to now,
    capped. Beyond the oldest block, DCC rows have no block to attach to.

    Fresh DB (no blocks): return 0 — do NOT backfill. There are no blocks to
    reconcile, so pulling ~400 days (~19k half-hourly rows) is wasted work and
    needless API load. The first poll then starts the cursor at 'now' and only
    tracks forward. A deliberate historical-import action can be added later."""
    oldest = None
    try:
        oldest = _store.get_oldest_block_start()
    except Exception:
        pass
    if not oldest:
        return 0
    try:
        oldest_dt = datetime.fromisoformat(oldest)
        if oldest_dt.tzinfo is None:
            oldest_dt = oldest_dt.replace(tzinfo=timezone.utc)
        days = (datetime.now(timezone.utc) - oldest_dt).days + 1
        return max(1, min(days, _KRAKEN_BACKFILL_CAP_DAYS))
    except Exception:
        return _KRAKEN_BACKFILL_CAP_DAYS


def _build_kraken_ingester():
    """Construct the KrakenIngester from verified discovery, or return None.

    Requires _kraken_client and a discovery result with an import meter. Export
    is wired only if discovery found an export MPAN. Returns None (logging why)
    if prerequisites are absent, so the poll task can no-op cleanly.
    """
    global _kraken_ingester
    if _kraken_client is None or not _kraken_discovery:
        return None
    imp = _kraken_discovery.get("import")
    if not imp or not imp.get("mpan") or not imp.get("serial"):
        logger.warning("kraken_ingester: no import meter in discovery — not built")
        return None
    try:
        from kraken_ingester import KrakenIngester
    except Exception as e:
        logger.warning("kraken_ingester: import failed: %s", e)
        return None
    exp = _kraken_discovery.get("export") or {}
    _kraken_ingester = KrakenIngester(
        _kraken_client, _store,
        import_mpan=imp["mpan"], import_serial=imp["serial"],
        export_mpan=exp.get("mpan"), export_serial=exp.get("serial"),
        main_meter_id="electricity_main",
        billing_source="api",
        backfill_days=_kraken_backfill_days(),
    )
    return _kraken_ingester


def kraken_available() -> bool:
    """True when a Kraken API client and verified discovery exist — i.e. there
    IS an API to settle/retry against. The UI uses this to gate the unsettled
    'retry settlement' action (no API ⇒ nothing to retry)."""
    return _kraken_client is not None and bool(_kraken_discovery)


def _clamp_sweep_start(oldest_iso: str, now: datetime,
                       max_lookback_days=None) -> datetime:
    """Earliest sweep start = oldest-unsettled block, floored at the horizon.

    Pure. Bounds how far back settlement is chased: an unsettled block older than
    `max_lookback_days` keeps its provisional value (DCC won't settle it now), and
    the floor caps the sweep window cost. None ⇒ unbounded (user-triggered retry).
    """
    odt = datetime.fromisoformat(oldest_iso)
    if odt.tzinfo is None:
        odt = odt.replace(tzinfo=timezone.utc)
    if max_lookback_days is not None:
        floor_dt = now - timedelta(days=max_lookback_days)
        if odt < floor_dt:
            odt = floor_dt
    return odt


def _sweep_is_due(last_iso, now: datetime, min_interval_s: int) -> bool:
    """Pure cadence gate: True if at least `min_interval_s` has elapsed since the
    last sweep (or there was none). Unparseable/missing state ⇒ due."""
    if not last_iso:
        return True
    try:
        last_dt = datetime.fromisoformat(last_iso)
    except (ValueError, TypeError):
        return True
    if last_dt.tzinfo is None:
        last_dt = last_dt.replace(tzinfo=timezone.utc)
    return (now - last_dt).total_seconds() >= min_interval_s


async def retry_settlement_for_unsettled(now=None, max_lookback_days=None) -> dict:
    """User-triggered: re-fetch DCC over the span oldest-unsettled → now and
    settle whatever is now available, WITHOUT advancing the incremental cursor.

    A cheaper, on-demand alternative to having every poll chase old gaps: the
    Data Management report shows unsettled blocks and the user clicks retry.
    Returns {ok, reason?, settled_import, settled_export, unsettled_before,
    unsettled_after, window}.
    """
    if not kraken_available():
        return {"ok": False, "reason": "no_api"}
    ingester = _build_kraken_ingester()
    if ingester is None:
        return {"ok": False, "reason": "no_ingester"}

    before = _store.count_unsettled_blocks()
    if before == 0:
        return {"ok": True, "settled_import": 0, "settled_export": 0,
                "unsettled_before": 0, "unsettled_after": 0, "window": None}

    oldest = _store.get_oldest_unsettled_block_start()
    if not oldest:
        return {"ok": True, "settled_import": 0, "settled_export": 0,
                "unsettled_before": before, "unsettled_after": before,
                "window": None}

    _now = now or datetime.now(timezone.utc)
    # Build a Z-suffixed UTC window from oldest unsettled block → now, floored at
    # the look-back horizon when one is given (the daily auto-sweep passes one;
    # the user-triggered retry leaves it None = reach as far back as needed).
    try:
        odt = _clamp_sweep_start(oldest, _now, max_lookback_days)
    except ValueError:
        return {"ok": False, "reason": "bad_oldest"}
    period_from = odt.astimezone(timezone.utc).replace(tzinfo=None).isoformat(timespec="seconds") + "Z"
    period_to = _now.astimezone(timezone.utc).replace(tzinfo=None).isoformat(timespec="seconds") + "Z"

    summary = await ingester.poll(window=(period_from, period_to),
                                  advance_cursor=False)
    after = _store.count_unsettled_blocks()
    logger.info("retry_settlement: window=%s..%s settled_import=%d "
                "settled_export=%d unsettled %d→%d",
                period_from, period_to, summary.get("stored", 0),
                summary.get("export_stored", 0), before, after)
    return {"ok": True,
            "settled_import": summary.get("stored", 0),
            "settled_export": summary.get("export_stored", 0),
            "unsettled_before": before, "unsettled_after": after,
            "window": [period_from, period_to],
            "errors": summary.get("errors", [])}


async def probe_recorder_statistics(entity_ids: list, days: int = 800) -> dict:
    """READ-ONLY historical-import spike: fetch HA long-term statistics for the
    chosen sensors and return per-sensor probe reports characterising retention,
    cadence, gaps, state_class/energy-vs-power and DST/timestamp behaviour.

    No writes, no block creation — purely diagnostic. Runs on the engine loop
    (the WS listener that resolves the command futures lives there)."""
    import statistics_probe as _sp
    if _engine_ha is None:
        return {"ok": False, "reason": "no_ha", "reports": []}
    ids = [e for e in (entity_ids or []) if e]
    if not ids:
        return {"ok": True, "reports": [], "window": None}
    days = max(1, min(int(days), 2000))
    now = datetime.now(timezone.utc)
    start = (now - timedelta(days=days)).isoformat()
    end = now.isoformat()
    try:
        meta_list = await _engine_ha.get_statistic_ids()
    except Exception as e:
        logger.warning("probe_recorder_statistics: list_statistic_ids failed: %s", e)
        meta_list = []
    meta_by_id = {m.get("statistic_id"): m for m in (meta_list or [])}
    try:
        stats = await _engine_ha.get_statistics(ids, start, end, period="hour")
    except Exception as e:
        logger.warning("probe_recorder_statistics: statistics_during_period failed: %s", e)
        return {"ok": False, "reason": "stats_failed", "error": str(e),
                "window": [start, end], "reports": []}
    reports = [_sp.build_sensor_probe(eid, stats.get(eid) or [], meta_by_id.get(eid))
               for eid in ids]
    return {"ok": True, "window": [start, end], "reports": reports}


async def probe_consumption_retention() -> dict:
    """READ-ONLY historical-import spike: measure how far back the Octopus API
    reaches per channel (import vs export), using two cheap single-row boundary
    fetches each. Reports earliest/latest per channel and the export-vs-import
    lag — the number that decides what a future import can promise (export is
    simply unavailable before its own start). No writes, no block creation.

    Returns {ok, channels:{import:{...}, export:{...}}, lag_days, reason?}."""
    import consumption_probe as _cp
    if not kraken_available():
        return {"ok": False, "reason": "no_api", "channels": {}}
    # Far-past floor: without a period_from the consumption endpoint defaults to
    # only the last ~week, so the "earliest" query would return a recent date,
    # not the meter's true start. This floor predates smart-meter rollout, so the
    # ascending scan starts from the real earliest interval.
    floor = "2015-01-01T00:00:00Z"
    disc = _kraken_discovery or {}
    out: dict = {"ok": True, "channels": {}}
    for name in ("import", "export"):
        ch = disc.get(name) or {}
        mpan, serial = ch.get("mpan"), ch.get("serial")
        if not mpan or not serial:
            out["channels"][name] = {"channel": name, "available": False,
                                     "note": "no meter of this kind discovered"}
            continue
        try:
            first = await _kraken_client.get_consumption_boundary(
                mpan, serial, period_from=floor)
            last = await _kraken_client.get_consumption_boundary(
                mpan, serial, newest=True, period_from=floor)
        except Exception as e:
            logger.warning("probe_consumption_retention: %s fetch failed: %s", name, e)
            out["channels"][name] = {"channel": name, "available": False,
                                     "note": f"fetch failed: {e}"}
            continue
        out["channels"][name] = _cp.build_consumption_boundary(name, first, last)
    out["lag_days"] = _cp.export_lag_days(out["channels"])
    return out


async def diagnose_consumption_retention() -> dict:
    """READ-ONLY deep diagnostic: enumerate EVERY meter point + serial on the
    account and report each serial's reachable consumption span. Distinguishes
    (a) API endpoint retention shorter than the website, (b) a meter exchange —
    an older serial the website aggregates but EMT's newest-serial probe misses,
    or (c) a boundary/query artifact. For each serial it also counts rows just
    BELOW the reported earliest, so a boundary that isn't a true data start is
    exposed. Writes nothing.

    Returns {ok, meter_points:[{mpan_tail, is_export, meter_count, meters:[...]}]}.
    Serials/MPANs are tail-masked so shared screenshots stay safe while still
    distinguishing meters."""
    import consumption_probe as _cp
    from datetime import datetime as _dt
    if not kraken_available():
        return {"ok": False, "reason": "no_api", "meter_points": []}
    floor = "2015-01-01T00:00:00Z"
    try:
        account = await _kraken_client.get_account()
    except Exception as e:
        logger.warning("diagnose_consumption_retention: get_account failed: %s", e)
        return {"ok": False, "reason": "account_failed", "error": str(e),
                "meter_points": []}

    def _tail(s):
        return ("…" + s[-6:]) if s and len(s) > 6 else (s or "?")

    def _z(dt):
        return (dt.astimezone(timezone.utc).replace(tzinfo=None)
                .isoformat(timespec="seconds") + "Z")

    out: dict = {"ok": True, "meter_points": []}
    for prop in account.get("properties", []) or []:
        for emp in prop.get("electricity_meter_points", []) or []:
            mpan = emp.get("mpan")
            is_export = bool(emp.get("is_export"))
            chan = "export" if is_export else "import"
            meters = emp.get("meters", []) or []
            # Agreement (tariff) history — already on the account payload, no
            # extra calls. Surfaces whether the consumption boundary lines up
            # with a tariff change (bundled meter exchange) or predates it (so a
            # tariff change is NOT the cause). Earliest valid_from ≈ supply start.
            ags = sorted((emp.get("agreements", []) or []),
                         key=lambda a: a.get("valid_from") or "")
            agreements = [{"valid_from": a.get("valid_from"),
                           "valid_to": a.get("valid_to"),
                           "tariff": a.get("tariff_code")} for a in ags]
            supply_start = (agreements[0]["valid_from"] if agreements else None)
            mreps: list = []
            for m in meters:
                serial = m.get("serial_number")
                if not serial:
                    continue
                try:
                    first = await _kraken_client.get_consumption_boundary(
                        mpan, serial, period_from=floor)
                    last = await _kraken_client.get_consumption_boundary(
                        mpan, serial, newest=True, period_from=floor)
                except Exception as e:
                    mreps.append({"serial_tail": _tail(serial), "channel": chan,
                                  "available": False, "note": f"fetch failed: {e}"})
                    continue
                rep = _cp.build_consumption_boundary(chan, first, last)
                rep["serial_tail"] = _tail(serial)
                # Below-boundary check: any rows in the ~45 days before the
                # reported earliest? >0 ⇒ 'earliest' is NOT a true data start
                # (a query artifact); 0 ⇒ it's a real wall for this serial.
                rep["rows_just_below_earliest"] = None
                rep["earliest_below"] = None
                if rep.get("earliest"):
                    try:
                        e0 = _dt.fromisoformat(rep["earliest"])
                        below = await _kraken_client.get_consumption(
                            mpan, serial,
                            period_from=_z(e0 - timedelta(days=45)),
                            period_to=_z(e0 - timedelta(seconds=1)))
                        rep["rows_just_below_earliest"] = len(below)
                        # Surface WHERE the below-rows sit: a handful of isolated
                        # rows at the boundary (DST/half-open-interval edge) is not
                        # recoverable history; a full run of them is real under-reach.
                        starts = [b.get("interval_start") for b in below
                                  if b.get("interval_start")]
                        rep["earliest_below"] = min(starts) if starts else None
                    except Exception as e:
                        logger.info("diagnose: below-boundary probe failed: %s", e)
                mreps.append(rep)
            out["meter_points"].append({
                "mpan_tail": _tail(mpan), "is_export": is_export,
                "meter_count": len(meters), "meters": mreps,
                "supply_start": supply_start,
                "agreement_count": len(agreements),
                "agreements": agreements,
            })
    return out


async def plan_api_import(requested_from=None, *, chunk_days: int = 60) -> dict:
    """READ-ONLY preview for the API import route: compute, per channel, the window
    it would import — from the earliest agreement (~join) up to go-live (EMT's
    oldest block) — and the newest→oldest chunk count. Matches the apply, which
    fetches GraphQL Measurements (reaching the account's full history, not the
    REST ~2-year wall). Writes nothing.

    Returns {ok, go_live, channels:{import:{available,window,chunk_count}, ...}}."""
    import api_import as _ai
    if not kraken_available():
        return {"ok": False, "reason": "no_api", "channels": {}}
    try:
        # Ceiling = oldest LIVE block, ignoring reconstructed/imported blocks — so
        # a previous partial import doesn't lower the ceiling below its own gap.
        go_live = _store.get_oldest_block_start(exclude_imported=True)
    except Exception:
        go_live = None
    if not go_live:
        return {"ok": False, "reason": "no_go_live", "channels": {},
                "note": "no existing blocks to tile up to yet"}
    disc = _kraken_discovery or {}
    out: dict = {"ok": True, "go_live": go_live, "channels": {}}
    for name in ("import", "export"):
        info = disc.get(name) or {}
        if not info.get("mpan"):
            out["channels"][name] = {"ok": False, "available": False}
            continue
        # Fast, PURE preview — no network. Reuse the data-floor if a prior import
        # or "Check start dates" already resolved it; otherwise fall back to the
        # agreement floor. The import itself runs the (network) boundary probe to
        # extend the floor to the true data start, so the preview must not block
        # or time out on API calls (it has the shortest route timeout).
        floor = (requested_from or _hist_floor_cache.get(name)
                 or _earliest_agreement_from(info))
        if not floor:
            gl_dt = _ai._naive_utc(go_live)
            floor = _ai._iso(gl_dt - timedelta(days=1100)) if gl_dt else None
        window = _ai.clamp_window(floor, go_live)
        if not window.get("ok"):
            out["channels"][name] = {"ok": False, "available": True,
                                     "note": window.get("note")}
            continue
        chunks = _ai.plan_chunks(window["from"], window["to"], chunk_days=chunk_days)
        out["channels"][name] = {
            "ok": True, "available": True, "chunk_count": len(chunks),
            "window": {"from": window["from"], "to": window["to"]}}
    return out


_API_IMPORT_CHECKPOINT = "api_import_oldest"   # kraken_state key (per channel suffix)


def _earliest_agreement_from(info: dict):
    """Earliest agreement valid_from (naive-UTC iso) for a channel, or None — the
    import floor (roughly account join)."""
    import api_import as _ai
    froms = [a.get("valid_from") for a in (info.get("agreements") or [])
             if a.get("valid_from")]
    naive = sorted(f for f in (_ai._iso(_ai._naive_utc(x)) for x in froms) if f)
    return naive[0] if naive else None


async def diagnose_import_range(*, probe_days: int = 1) -> dict:
    """READ-ONLY: for each channel report the earliest supplier *agreement*, the
    resolved import floor (the earliest half-hour Measurements actually returns —
    which is what the import USES), and whether real data was found before the
    agreement.

    Uses the same robust 30-day-window probing as the import floor
    (`_data_floor_for`), NOT a single day — export is intermittent (solar only),
    so a 1-day probe can land on a zero-export day and falsely report "no data".
    Writes nothing.
    """
    def _mask(v):
        v = str(v or "")
        return (v[:2] + "…" + v[-1]) if len(v) > 3 else ("…" if v else None)
    if _kraken_client is None or not _kraken_discovery:
        return {"ok": False, "reason": "no_api"}
    acct = _kraken_account_number
    out: dict = {"ok": True, "account_masked": _mask(acct) if acct else None,
                 "channels": {}}
    for channel in ("import", "export"):
        info = (_kraken_discovery or {}).get(channel) or {}
        mpan = info.get("mpan")
        if not mpan:
            continue
        agreement = _earliest_agreement_from(info)
        data_floor = await _data_floor_for(channel, info)   # robust 30-day probe
        _hist_floor_cache[channel] = data_floor   # so the (pure) Preview plan matches
        extended = bool(agreement and data_floor and data_floor < agreement)
        out["channels"][channel] = {
            "mpan_masked": _mask(mpan),
            "agreement_floor": agreement,
            "import_starts_at": data_floor,           # what the import will use
            "data_before_floor": extended,            # earlier than the agreement?
            "earliest_seen_before_floor": data_floor if extended else None,
        }
        logger.info("diagnose_import_range: %s agreement=%s import_starts_at=%s "
                    "extended=%s", channel, agreement, data_floor, extended)
    return out


# Per-run cache of the resolved import floor per channel, so the (few) boundary
# probes run once per import/preview, not per slice. Cleared on restart.
_hist_floor_cache: dict = {}


async def _data_floor_for(channel: str, info: dict):
    """The import floor for a channel: the earliest half-hour the Measurements API
    actually returns — which can be EARLIER than the earliest supplier *agreement*
    (e.g. solar export that ran before a formal export agreement was signed).

    The agreement floor (`_earliest_agreement_from`) was clamping the import there,
    silently dropping real earlier history. Because Measurements carries the billed
    cost per interval, pre-agreement half-hours still price correctly (cost from the
    API; rate falls back to cost÷kWh where no tariff schedule covers them).

    Cheap presence-probe: 30-day windows stepping ~6 months back from the agreement
    floor, one small page each (we only need to know IF data exists). A single empty
    window is tolerated (a mid-history outage); two in a row ends the scan. Returns
    the earlier of the agreement floor and the oldest window with data. Never floors
    HIGHER than the agreement (worst case = prior behaviour); agreement floor on any
    error."""
    import api_import as _ai
    agree = _earliest_agreement_from(info)
    mpan = info.get("mpan")
    if not mpan or _kraken_client is None:
        return agree
    anchor = _ai._naive_utc(agree) if agree else None
    if anchor is None:
        return agree
    direction = "GENERATION" if channel == "export" else "CONSUMPTION"
    step, win = 183, 30
    oldest_data = None
    empties = 0
    for i in range(1, 15):        # up to ~7 years of 6-month steps
        ps = anchor - timedelta(days=step * i)
        pe = ps + timedelta(days=win)
        try:
            rows = await _kraken_client.get_measurements(
                mpan, _ai._iso(ps) + "Z", _ai._iso(pe) + "Z",
                account_number=_kraken_account_number, direction=direction,
                page_size=100, max_pages=1)
        except Exception:
            break
        if rows:
            hits = [r["start"] for r in rows if r.get("start")]
            if hits:
                oldest_data = min(hits) if oldest_data is None else min(oldest_data, min(hits))
            empties = 0
        else:
            empties += 1
            if empties >= 2:
                break
    if oldest_data and (agree is None or oldest_data < agree):
        logger.info("_data_floor_for: %s floor extended %s → %s "
                    "(data predates agreement)", channel, agree, oldest_data)
        return oldest_data
    return agree


async def _cached_data_floor(channel: str, info: dict, *, restart: bool = False):
    """`_data_floor_for` memoised per run (cleared on restart, like rate segs)."""
    if restart:
        _hist_floor_cache.pop(channel, None)
    if channel not in _hist_floor_cache:
        _hist_floor_cache[channel] = await _data_floor_for(channel, info)
    return _hist_floor_cache[channel]


# Per-run cache of (valid_from, valid_to, RateSchedule) segments per channel, so
# the tariff rates are fetched once per import (not per slice). Cleared on restart.
_hist_rate_segs_cache: dict = {}


async def _build_channel_rate_segs(channel: str):
    """Per-agreement rate schedules for a channel, from the discovered agreement
    history. Used to give imported blocks a CLEAN tariff rate (and to price the
    half-hours where Measurements returned kWh but no cost)."""
    import api_import as _ai
    from kraken_api_client import KrakenAPIClient
    from kraken_rates import build_rate_schedule
    info = (_kraken_discovery or {}).get(channel) or {}
    segs = []
    for a in (info.get("agreements") or []):
        tariff = a.get("tariff_code")
        vf, vt = a.get("valid_from"), a.get("valid_to")
        if not tariff:
            continue
        product = KrakenAPIClient._tariff_to_product_code(tariff)
        if not product:
            continue
        nvf = _ai._iso(_ai._naive_utc(vf)) if vf else "0000-01-01T00:00:00"
        nvt = _ai._iso(_ai._naive_utc(vt)) if vt else None
        # Skip a degenerate agreement window (valid_from >= valid_to): the REST
        # rate API rejects it with HTTP 400 "period_from must not be greater than
        # period_to", and a zero/negative span can price nothing anyway. Octopus
        # emits these for superseded/back-to-back old agreements.
        if nvt is not None and nvf >= nvt:
            logger.info("_build_channel_rate_segs: skipping %s (%s) — empty "
                        "agreement window %s..%s", tariff, channel, nvf, nvt)
            continue
        try:
            rs = await build_rate_schedule(_kraken_client, product, tariff,
                                           period_from=vf, period_to=vt)
        except Exception as e:
            logger.warning("_build_channel_rate_segs: %s %s failed: %s",
                           channel, tariff, e)
            continue
        if not rs.is_empty():
            segs.append((nvf, nvt, rs))
    return segs


# Per-run cache of per-agreement standing-charge schedules per channel.
_hist_standing_segs_cache: dict = {}


async def _build_channel_standing_segs(channel: str):
    """Per-agreement STANDING-CHARGE schedules for a channel — the authoritative,
    complete source for the daily standing charge. Measurements' STANDING_CHARGE_COST
    is patchy (whole stretches come back with none), so imported days are priced
    from this schedule and only fall back to the Measurements figure where the
    schedule doesn't cover the date."""
    import api_import as _ai
    from kraken_api_client import KrakenAPIClient
    from kraken_rates import build_standing_charge_schedule
    info = (_kraken_discovery or {}).get(channel) or {}
    segs = []
    for a in (info.get("agreements") or []):
        tariff = a.get("tariff_code")
        vf, vt = a.get("valid_from"), a.get("valid_to")
        if not tariff:
            continue
        product = KrakenAPIClient._tariff_to_product_code(tariff)
        if not product:
            continue
        nvf = _ai._iso(_ai._naive_utc(vf)) if vf else "0000-01-01T00:00:00"
        nvt = _ai._iso(_ai._naive_utc(vt)) if vt else None
        # Skip a degenerate agreement window (valid_from >= valid_to) — same HTTP
        # 400 guard as the rate builder (see _build_channel_rate_segs).
        if nvt is not None and nvf >= nvt:
            logger.info("_build_channel_standing_segs: skipping %s (%s) — empty "
                        "agreement window %s..%s", tariff, channel, nvf, nvt)
            continue
        try:
            sc = await build_standing_charge_schedule(
                _kraken_client, product, tariff, period_from=vf, period_to=vt)
        except Exception as e:
            logger.warning("_build_channel_standing_segs: %s %s failed: %s",
                           channel, tariff, e)
            continue
        if not sc.is_empty():
            segs.append((nvf, nvt, sc))
    return segs


def _standing_for(standing_segs, day_iso):
    """Daily standing charge £/day for a date (YYYY-MM-DD) from the schedule, or
    None if uncovered. Resolve at midday to avoid midnight/DST boundary edges.
    Native pence/day → £/day."""
    ts = day_iso + "T12:00:00"
    for vf, vt, sched in standing_segs:
        if ts >= vf and (vt is None or ts < vt):
            v = sched.resolve(ts)
            if v is None:
                # A standing charge persists until it next changes. The API can
                # return records with an explicit valid_to and no open successor,
                # so resolve() stops carrying the rate forward past that date and
                # returns None — dropping us to the patchier Measurements figure
                # (the 0.5040 vs 0.504559 boundary mismatch). Carry the LAST known
                # rate at/before this date forward instead.
                periods = getattr(sched, "_periods", None) or []
                earlier = [r for (pf, _pt, r) in periods if pf <= ts]
                v = earlier[-1] if earlier else None
            if v is not None:
                return round(v / 100.0, 6)
            # Segment matched but its schedule has nothing at/before this date —
            # keep looking (overlapping agreements) before giving up.
    return None


def _tariff_rate_for(rate_segs, start, off_peak):
    """Clean £/kWh for a block from the tariff schedule, keyed by its bucket:
    OFF_PEAK→day's low rate, STANDARD→day's high rate, unknown→time-of-day
    resolve. None if uncovered. Native pence → £."""
    for vf, vt, sched in rate_segs:
        if start >= vf and (vt is None or start < vt):
            if off_peak is True:
                lo, _hi = sched.day_rate_bounds(start)
                v = lo
            elif off_peak is False:
                _lo, hi = sched.day_rate_bounds(start)
                v = hi
            else:
                v = sched.resolve(start)
            return round(v / 100.0, 6) if v is not None else None
    return None


def _billed_rate(rate_segs, start, off_peak, meas_cost, kwh,
                 tol: float = 0.001):
    """£/kWh for an imported slot, choosing the right source of truth by tariff type.

    The rate-source axis is whether the slot carries a time-of-use (OFF_PEAK/
    STANDARD) label:

    * NO label (off_peak is None) — a CONTINUOUS / published-rate tariff: flat
      export, Agile (either direction). Here the SCHEDULE is authoritative — it is
      the exact per-slot published rate. Billed cost÷kWh is only penny-rounding
      jitter that shatters one flat/Agile rate into dozens of near-identical
      buckets (the export-fragmentation bug). So we PREFER the schedule, and use
      billed only as a fallback when the schedule can't cover the slot.

    * WITH a label (off_peak True/False) — a BANDED, dispatch-aware tariff (IOG):
      Octopus's billed cost is truth. It captures dispatched charging outside the
      fixed off-peak window, and it avoids the price-cap transition-DAY hazard
      where day_rate_bounds sees both the old and new rate and would pick the
      wrong band. We still prefer the clean scheduled rate, but ONLY when it
      agrees with billed to within `tol` (0.1p); otherwise trust cost÷kWh.

    With no billed cost at all we fall back to the schedule (time/label based).
    Returns £/kWh or None.

    NOTE the discriminator is NOT the label alone: export slots come back labelled
    STANDARD_RATE (→ off_peak False, not None), so "no label" would miss them. What
    actually separates a published-rate slot from a dispatch one is whether the
    tariff is BANDED that day — a distinct off-peak vs peak rate (IOG) vs a single
    rate all day (flat export, fixed import). Agile carries no label at all
    (off_peak None) and is resolved per-slot."""
    sched = _tariff_rate_for(rate_segs, start, off_peak)
    # Continuous / unlabelled tariff (Agile): _tariff_rate_for used resolve() for the
    # exact per-slot published rate — always prefer it over billed jitter.
    if off_peak is None and sched is not None:
        return sched
    # Labelled slot (IOG import, or flat export tagged STANDARD_RATE). Decide by
    # whether the tariff is BANDED on this day. A SINGLE rate for the day (flat
    # export / fixed import) ⇒ the schedule is the published truth and billed
    # cost÷kWh is only penny-rounding jitter that fragments it — prefer the
    # schedule. Distinct off-peak/peak rates ⇒ dispatch-aware IOG ⇒ billed stays
    # truth (out-of-window dispatch + price-cap transition-day safety).
    if sched is not None:
        lo = hi = None
        for vf, vt, s in rate_segs:
            if s is not None and start >= vf and (vt is None or start < vt):
                lo, hi = s.day_rate_bounds(start)
                break
        if lo is not None and lo == hi:      # flat tariff that day → schedule is truth
            return sched
    if meas_cost is not None and (kwh or 0) > 0:
        billed = round(meas_cost / kwh, 6)
        if sched is not None and abs(sched - billed) <= tol:
            return sched
        return billed
    return sched


_PEAK_BAND_FACTOR = 1.5      # a slot priced >1.5x the off-peak floor is "peak band"


def _reprice_suspect(channel, row, rate_segs, min_kwh: float,
                     off_peak_floor=None) -> bool:
    """Cheap LOCAL test the suspect-only reprice uses to decide whether a re-fetch
    could actually improve a block — so it skips slots that are already correct
    instead of re-querying the whole range (the reason the pass ran as long as the
    import). A block is a SUSPECT when:
      - it has no billed cost stored (imp/exp_cost is None) — always re-check; or
      - it's priced at the PEAK band with material energy (>= min_kwh) — the
        signature of an out-of-window smart-charge dispatch the bulk import couldn't
        label off-peak.
    'Peak band' is decided from the schedule's day bounds when they're banded, else
    from `off_peak_floor` — the off-peak rate OBSERVED in the imported data itself.
    That fallback matters because IOG's off-peak is dispatch/label-driven and may not
    be in the tariff schedule, which would otherwise read flat and blind the filter.
    Everything else — already-off-peak slots and immaterial (<min_kwh) slots — a
    re-check can't make more correct, so skip. Missing row ⇒ re-check (be safe)."""
    if row is None:
        return True
    imp = (channel == "import")
    rate = row.get("imp_rate") if imp else row.get("exp_rate")
    kwh  = (row.get("imp_kwh") if imp else row.get("exp_kwh")) or 0.0
    cost = row.get("imp_cost") if imp else row.get("exp_cost")
    if cost is None:
        return True
    if kwh < min_kwh:
        return False
    if rate is None:
        return True
    start = row.get("start")
    lo = hi = None
    for vf, vt, s in rate_segs:
        if s is not None and start >= vf and (vt is None or start < vt):
            try:
                lo, hi = s.day_rate_bounds(start)     # PENCE (schedule native units)
            except Exception:
                lo = hi = None
            break
    if lo is not None and hi is not None and lo != hi:
        # Banded schedule day. day_rate_bounds is in PENCE; imp_rate is in £ — so
        # convert the midpoint to £ before comparing (the missing /100 was why the
        # peak-band branch never fired).
        return rate > (lo + hi) / 200.0
    # Flat / absent schedule (e.g. IOG whose off-peak isn't in standard-unit-rates):
    # fall back to the off-peak floor observed in the imported data (already £).
    if off_peak_floor is not None and off_peak_floor > 0:
        return rate > off_peak_floor * _PEAK_BAND_FACTOR
    return False


def _import_pricing_flag(channel: str, kwh, meas_cost, buckets,
                         threshold: float = 1.0):
    """Diagnostic classifier for the IOG-dispatch pricing gap.

    A material-kWh IMPORT slot that Measurements returned WITHOUT a billed cost is
    priced from the time-of-day tariff schedule — which cannot know an IOG dispatch
    charged the car OUTSIDE the fixed off-peak window, so it is peak-priced (an
    over-charge). Returns (is_fallback_material, is_no_bucket):
      - is_fallback_material: material kWh, no Measurements cost (schedule-priced)
      - is_no_bucket        : AND Octopus sent NO TOU bucket at all (label list
                              empty) — i.e. a data omission, not a STANDARD label.
    Distinguishes 'Octopus omitted the bucket' from 'genuinely labelled peak', to
    tell a settlement-timing gap from a permanent omission before we change pricing.
    """
    if channel != "import" or meas_cost is not None or (kwh or 0) < threshold:
        return (False, False)
    return (True, not bool(buckets))


def _record_import_gaps(channel: str, gaps_key: str, checkpoint, imported) -> None:
    """Detect runs of MISSING half-hours this slice reveals — within the fetched
    set and at the checkpoint boundary — and APPEND them to the persisted
    per-channel gap list (kraken_state JSON). `imported` is newest→oldest.
    Non-fatal."""
    import json as _json
    import api_import as _ai
    try:
        seq = ([checkpoint] if checkpoint else []) + list(imported)   # descending
        new_gaps = []
        for i in range(len(seq) - 1):
            try:
                a = datetime.fromisoformat(seq[i])          # newer
                b = datetime.fromisoformat(seq[i + 1])      # older
            except Exception:
                continue
            missing = int((a - b).total_seconds() // 1800) - 1        # 30-min slots
            if missing > 0:
                new_gaps.append({
                    "from": _ai._iso(b + timedelta(minutes=30)),
                    "to": _ai._iso(a - timedelta(minutes=30)),
                    "count": missing,
                })
        if not new_gaps:
            return
        try:
            existing = _json.loads(_store.get_kraken_state(gaps_key) or "[]")
        except Exception:
            existing = []
        existing.extend(new_gaps)
        _store.set_kraken_state(gaps_key, _json.dumps(existing))
    except Exception as e:
        logger.info("import: gap recording skipped (%s): %s", channel, e)


def api_import_gaps() -> dict:
    """Persisted reconstruction gaps per channel (missing half-hour runs found
    during the historical import), for the import page to display without any
    re-query. Returns {channels:{import:{gaps:[...], missing:N}, export:{...}}}.

    Self-clearing: each persisted gap is reconciled against the live blocks table —
    once its slots carry data (a later import or a backfill filled them), the gap is
    pruned from the stored list, so the display reflects what's ACTUALLY still
    missing rather than a stale snapshot from import time."""
    import json as _json
    out = {"channels": {}}
    if _store is None:
        return out
    for channel in ("import", "export"):
        try:
            gaps = _json.loads(_store.get_kraken_state("import_gaps_" + channel) or "[]")
        except Exception:
            gaps = []
        # Reconcile against reality — drop gaps whose slots are now all filled.
        kept = []
        for g in gaps:
            gf = g.get("from")
            gt = g.get("to") or gf
            need = int(g.get("count") or 0)
            try:
                have = _store.channel_slots_present(channel, gf, gt) if gf else 0
            except Exception:
                have = 0
            if need <= 0 or have < need:
                kept.append(g)
        if len(kept) != len(gaps):
            try:
                _store.set_kraken_state("import_gaps_" + channel, _json.dumps(kept))
            except Exception:
                pass
            gaps = kept
        missing = sum(int(g.get("count") or 0) for g in gaps)
        gaps_sorted = sorted(gaps, key=lambda g: g.get("from") or "", reverse=True)
        out["channels"][channel] = {"gaps": gaps_sorted, "missing": missing,
                                    "gap_count": len(gaps)}
    return out


async def import_api_history(requested_from=None, *, chunk_days: int = 60,
                             max_chunks: int = 6, pace_s: float = 0.0,
                             headroom_frac: float = 0.25, dry_run: bool = False,
                             restart: bool = False, recover_costs: bool = True,
                             recover_pace_s: float = 0.4,
                             meter_id: str = "electricity_main") -> dict:
    """API import route apply — INCREMENTAL and rate-limit-aware.

    Fetches half-hourly GraphQL Measurements (kWh + billed cost + Octopus's own
    OFF_PEAK/STANDARD_RATE label) newest→oldest and writes `imported_api` blocks.
    Pricing is exact and dispatch-aware (no tariff reconstruction / clustering).

    Bounded per call: processes at most `max_chunks` chunks, resuming from the
    per-channel checkpoint, so a full-history import spans several short calls (the
    UI loops until `done`). Between chunks it paces (`pace_s`, which also yields the
    engine loop) and checks `rateLimitInfo` — if remaining points fall below
    `headroom_frac × limit` it PAUSES (leaving budget for BCD / EMT's live poll)
    and returns `reason: 'rate_limit'` so the caller resumes later. `restart` clears
    the checkpoint to begin again from go-live. Reversible via the reconstructed-
    history delete filter."""
    import asyncio as _asyncio
    import api_import as _ai
    from block_store import IMPORTED_SOURCE_API
    if not kraken_available():
        return {"ok": False, "reason": "no_api", "channels": {}}
    if _store is None:      # e.g. a restore closed the store — never write to it
        return {"ok": False, "reason": "no_store", "channels": {}}
    try:
        # Oldest LIVE block (ignore reconstructed/imported) so a prior partial
        # import doesn't wall off its own gap — see plan_api_import.
        go_live = _store.get_oldest_block_start(exclude_imported=True)
    except Exception:
        go_live = None
    if not go_live:
        return {"ok": False, "reason": "no_go_live", "channels": {}}
    disc = _kraken_discovery or {}
    out: dict = {"ok": True, "go_live": go_live, "dry_run": dry_run, "channels": {}}
    all_done = True
    for channel in ("import", "export"):
        info = disc.get(channel) or {}
        mpan = info.get("mpan")
        if not mpan:
            out["channels"][channel] = {"ok": False, "available": False, "done": True}
            continue
        ck_key = _API_IMPORT_CHECKPOINT + "_" + channel
        done_key = "api_import_done_" + channel
        gaps_key = "import_gaps_" + channel
        if restart and not dry_run:
            _store.set_kraken_state(ck_key, "")
            _store.set_kraken_state(done_key, "")
            _store.set_kraken_state(gaps_key, "")
            # Fresh run → drop this channel's stale reprice-queue entries so the
            # queue reflects only THIS import's no-cost slots (it's cumulative
            # otherwise, carrying flags from earlier/broken runs).
            try:
                _store.clear_reprice_queue_slots(
                    channel, _store.get_reprice_queue().get(channel) or [])
            except Exception:
                pass
        if not dry_run and _store.get_kraken_state(done_key) == "1":
            out["channels"][channel] = {"ok": True, "done": True,
                                        "reason": "already_done", "written": 0}
            continue
        direction = "GENERATION" if channel == "export" else "CONSUMPTION"

        # Tariff rate schedules (fetched once per run, cached; cleared on restart)
        # → clean display rate + a price for half-hours Measurements returns
        # without cost.
        if restart and not dry_run:
            _hist_rate_segs_cache.pop(channel, None)
            _hist_standing_segs_cache.pop(channel, None)
        if channel not in _hist_rate_segs_cache:
            _hist_rate_segs_cache[channel] = await _build_channel_rate_segs(channel)
        rate_segs = _hist_rate_segs_cache[channel]
        # Standing-charge schedule (authoritative, complete) — see _standing_for.
        if channel not in _hist_standing_segs_cache:
            _hist_standing_segs_cache[channel] = \
                await _build_channel_standing_segs(channel)
        standing_segs = _hist_standing_segs_cache[channel]

        floor = requested_from or await _cached_data_floor(channel, info,
                                                            restart=restart)
        if not floor:
            gl_dt = _ai._naive_utc(go_live)
            floor = _ai._iso(gl_dt - timedelta(days=1100)) if gl_dt else None
        checkpoint = (_store.get_kraken_state(ck_key) or None) if not dry_run else None
        hi = checkpoint or go_live
        window = _ai.clamp_window(floor, hi)
        if not window.get("ok"):
            if not dry_run:
                _store.set_kraken_state(done_key, "1")
            out["channels"][channel] = {"ok": True, "done": True,
                                        "reason": "completed", "written": 0}
            continue
        chunks = _ai.plan_chunks(window["from"], window["to"], chunk_days=chunk_days)
        slice_ = chunks[:max_chunks]

        # Fetch the slice, paced + rate-limit-aware.
        by_start: dict = {}
        fetch_error = None
        paused = False
        for c in slice_:
            if headroom_frac > 0:
                try:
                    rl = await _kraken_client.get_rate_limit()
                except Exception:
                    rl = None
                if rl:
                    low = (rl.get("remaining") is not None and rl.get("pointsLimit")
                           and rl["remaining"] < headroom_frac * rl["pointsLimit"])
                    if rl.get("isBlocked") or low:
                        paused = True
                        break
            try:
                rows = await _kraken_client.get_measurements(
                    mpan, c["from"] + "Z", c["to"] + "Z", direction=direction)
            except Exception as e:
                fetch_error = str(e)
                break
            for r in (rows or []):
                if r.get("start"):
                    by_start[r["start"]] = r
            # ── EXPORT REST fallback ─────────────────────────────────────────
            # The half-hourly *Measurements* export feed can begin LATER than the
            # raw meter readings — Octopus bills export monthly from meter install
            # but its settled half-hourly export series may only start months
            # later. Export has no off-peak/dispatch, so raw REST kWh priced from
            # the tariff schedule (a flat OUTGOING rate OR Agile Outgoing's
            # half-hourly rate, both via resolve()) reproduces the bill exactly.
            # Fill any half-hour REST returns that Measurements didn't.
            if channel == "export" and info.get("serial"):
                # Match Measurements' EXCLUSIVE upper bound: the REST endpoint
                # includes the period_to boundary interval, which is the NEXT
                # (newer) chunk's oldest half-hour. Re-adding it here makes this
                # slice's newest timestamp one step higher than the checkpoint
                # expects, so the newest→oldest contiguity check rejects the whole
                # slice and halts the import at the slice boundary.
                _chunk_top = _ai._iso(_ai._naive_utc(c["to"] + "Z"))
                try:
                    rest_rows = await _kraken_client.get_consumption(
                        mpan, info["serial"],
                        period_from=c["from"] + "Z", period_to=c["to"] + "Z")
                except Exception as _rest_e:
                    rest_rows = []
                    logger.info("import: export REST fallback fetch failed "
                                "(%s..%s): %s", c["from"], c["to"], _rest_e)
                for rr in (rest_rows or []):
                    _isv = rr.get("interval_start")
                    st = _ai._iso(_ai._naive_utc(_isv)) if _isv else None
                    if st and (_chunk_top is None or st < _chunk_top) \
                            and st not in by_start:
                        by_start[st] = {
                            "start": st,
                            "kwh": float(rr.get("consumption") or 0.0),
                            "cost_incl": None,      # priced from the tariff below
                            "off_peak": None,       # export: no off-peak bucket
                            "standing_incl": 0.0,
                        }
            if pace_s:
                await _asyncio.sleep(pace_s)

        # Import EVERY fetched half-hour — do NOT halt at internal discontinuities.
        # Real supplier data has gaps: settlement thinning near where the settled
        # half-hourly series begins, the DST spring-forward's missing hour, meter
        # comms dropouts. Halting at the first gap permanently stopped the backfill
        # short (it kept tripping on chunk boundaries, then the March DST change).
        # Reconstructed history is allowed to have holes; termination is by reaching
        # the floor, or by a fully-empty chunk (no_more_data) once we descend below
        # where the data actually begins.
        starts = sorted(by_start.keys(), reverse=True)
        imported = list(starts)
        halted_at = None

        # Record any gaps (runs of missing half-hours) this slice reveals, so the
        # reconstruction's holes are surfaced to the user instead of silently
        # under-reporting a bill period. Zero-export nights come back as zero-value
        # rows (not omitted), so a gap here is a genuine hole — a DCC/settlement
        # outage. Persisted per channel; survives reload without re-querying.
        if not dry_run:
            _record_import_gaps(channel, gaps_key, checkpoint, imported)

        # Standing charge per day. PREFER the tariff standing-charge schedule
        # (authoritative + complete); only fall back to the summed Measurements
        # figure where the schedule doesn't cover the date. Measurements'
        # STANDING_CHARGE_COST is patchy (whole stretches return none → £0/day),
        # which was silently under-billing the reconstructed range.
        meas_standing: dict = {}
        meas_count: dict = {}
        for st in imported:
            d = st[:10]
            meas_standing[d] = (meas_standing.get(d, 0.0)
                                + (by_start[st].get("standing_incl") or 0.0))
            meas_count[d] = meas_count.get(d, 0) + 1
        try:
            _slots_per_day = int(round(24 * 60 / max(1, int(get_block_minutes()))))
        except Exception:
            _slots_per_day = 48
        day_standing: dict = {}
        for d in meas_standing:
            sc = _standing_for(standing_segs, d)
            if sc is not None:
                day_standing[d] = sc
            else:
                # Measurements spreads the daily standing charge across the day's
                # intervals, so a PARTIAL day (a gap or the go-live boundary) sums
                # to only a FRACTION of the charge (e.g. ¾ × 0.4512 = 0.3384). You
                # pay the full standing charge regardless of how many half-hours
                # have data — extrapolate the present intervals back to a whole day.
                n = meas_count.get(d, 0)
                day_standing[d] = (round(meas_standing[d] / n * _slots_per_day, 6)
                                   if n else meas_standing[d])

        priced = off_peak = 0
        fb_material = fb_no_bucket = 0     # IOG-dispatch pricing diagnostic
        fb_samples = []
        fb_starts = []                     # ALL no-cost material slots → reprice queue
        rows_to_write = []
        for st in imported:
            r = by_start[st]
            kwh = r.get("kwh") or 0.0
            meas_cost = r.get("cost_incl")
            ofp = r.get("off_peak")
            # Rate: Octopus's billed cost is truth — use cost÷kWh, keeping the clean
            # scheduled rate only when it agrees (so a mis-dated price-cap change in
            # the local schedule can't stamp a rate that contradicts the bill).
            rate = _billed_rate(rate_segs, st, ofp, meas_cost, kwh)
            # Cost: exact billed figure if Measurements gave one; otherwise
            # reconstruct kWh × tariff-rate (fills the cost-missing half-hours).
            if meas_cost is not None:
                cost = meas_cost
            elif rate is not None and kwh > 0:
                cost = round(kwh * rate, 6)
            else:
                cost = None
            if cost is not None:
                priced += 1
            if ofp:
                off_peak += 1
            # Diagnostic: a material-kWh slot Measurements gave no cost for is
            # schedule-priced and can't reflect an out-of-window IOG dispatch.
            _fb, _nb = _import_pricing_flag(channel, kwh, meas_cost, r.get("buckets"))
            if _fb:
                fb_material += 1
                fb_starts.append(st)       # queue for the calm re-price repair pass
                if _nb:
                    fb_no_bucket += 1
                if len(fb_samples) < 25:
                    fb_samples.append({"start": st, "kwh": round(kwh, 3),
                                       "off_peak": ofp, "buckets": r.get("buckets") or []})
            # Standing charge is an IMPORT-meter charge on the shared block row —
            # the export channel must not write it (it would clobber the import's
            # value with 0, since OUTGOING tariffs have no standing charge).
            _standing = day_standing.get(st[:10]) if channel == "import" else None
            rows_to_write.append({"start": st, "kwh": kwh, "rate": rate,
                                  "cost": cost, "standing": _standing})
        # Total flagged this chunk BEFORE in-pass recovery reduces it — the "raised"
        # figure the post-import health summary reports.
        flags_raised_ch = fb_material
        healed_in_pass = 0
        # ── In-pass cost recovery ────────────────────────────────────────────
        # The material no-cost slots above are the empty-`statistics` signature
        # Octopus emits under the bulk fetch's load — NOT a genuine gap (proven:
        # an isolated single-slot re-fetch returns the real OFF_PEAK/STANDARD cost
        # deterministically). So before writing, re-fetch each one calmly, in its
        # own single-slot query, and price it from the REAL cost. This makes the
        # import self-heal in one pass instead of leaving it to the repair pass.
        if (recover_costs and not dry_run and fb_starts
                and channel == "import" and _kraken_client is not None):
            try:
                _rec = await _kraken_client.recover_measurement_costs(
                    mpan, fb_starts, direction=direction, pace_s=recover_pace_s)
            except Exception as _re:
                _rec = {}
                logger.info("import cost recovery: skipped (%s)", _re)
            if _rec:
                _idx = {row["start"]: row for row in rows_to_write}
                _healed = []
                for st, node in _rec.items():
                    row = _idx.get(st)
                    mc = node.get("cost_incl")
                    if row is None or mc is None:
                        continue
                    ofp = node.get("off_peak")
                    kwh = row.get("kwh") or 0.0
                    # Billed cost is truth — cost÷kWh, keeping the clean scheduled
                    # rate only when it agrees (price-cap-safe).
                    rate = _billed_rate(rate_segs, st, ofp, mc, kwh)
                    if row.get("cost") is None:
                        priced += 1
                    if ofp and not row.get("_was_off_peak"):
                        off_peak += 1
                    row["cost"] = mc
                    row["rate"] = rate
                    row["_was_off_peak"] = bool(ofp)
                    _healed.append(st)
                if _healed:
                    _healset = set(_healed)
                    fb_starts = [s for s in fb_starts if s not in _healset]
                    fb_material = len(fb_starts)
                    fb_no_bucket = min(fb_no_bucket, fb_material)
                    healed_in_pass = len(_healed)
                    logger.info(
                        "import cost recovery [%s]: healed %d/%d no-cost slot(s) "
                        "in-pass via isolated re-fetch; %d still queued for repair",
                        channel, len(_healed), len(_rec) + 0, fb_material)
            # Strip the private helper key before persisting rows.
            for row in rows_to_write:
                row.pop("_was_off_peak", None)

        # IOG-dispatch pricing diagnostic: surface (and persist) how many material
        # slots Octopus gave no cost for, and how many had NO TOU bucket at all —
        # the signal that tells a settlement-timing gap from a permanent omission.
        if fb_material:
            logger.warning(
                "import pricing diag [%s]: %d material slot(s) (kWh>=1.0) had NO "
                "Measurements cost → schedule-priced (peak if out-of-window); %d of "
                "those had NO TOU bucket at all. Samples: %s",
                channel, fb_material, fb_no_bucket, fb_samples[:8])
            try:
                _diag = _store.get_meta("import_pricing_diag", None) or {}
                cur = _diag.get(channel) or {"material": 0, "no_bucket": 0, "samples": []}
                cur["material"] += fb_material
                cur["no_bucket"] += fb_no_bucket
                cur["samples"] = (cur["samples"] + fb_samples)[:50]
                _diag[channel] = cur
                _diag["updated_at"] = _dt_now_iso_safe()
                _store.set_meta("import_pricing_diag", _diag)
            except Exception as _de:
                logger.warning("import pricing diag: persist failed: %s", _de)
            # Queue the no-cost slots for the calm re-price repair pass.
            try:
                _store.add_reprice_queue(channel, fb_starts)
            except Exception as _qe:
                logger.warning("import reprice queue: persist failed: %s", _qe)

        written = 0
        if not dry_run and rows_to_write:
            # One transaction for the whole chunk (not a commit per block) so the
            # write doesn't stall the async loop and starve the HA WebSocket.
            written = _store.upsert_imported_blocks(
                rows_to_write, meter_id, channel, source=IMPORTED_SOURCE_API)
            await _asyncio.sleep(0)      # yield the loop after the batch write
        if not dry_run and imported:
            _store.set_kraken_state(ck_key, imported[-1])

        # Done when: gap-halt, or this slice reached the floor (all chunks fit),
        # or no data left below (past the account's start) — but never when paused
        # or a fetch failed (those must resume).
        reached_floor = len(chunks) <= max_chunks
        no_more_data = (not by_start) and (not paused) and (not fetch_error)
        done = bool(halted_at) or ((reached_floor or no_more_data)
                                   and not paused and not fetch_error)
        reason = ("halted" if halted_at else "rate_limit" if paused
                  else "fetch_error" if fetch_error
                  else "completed" if done else "more")
        if done and not dry_run:
            _store.set_kraken_state(done_key, "1")
        if not done:
            all_done = False

        out["channels"][channel] = {
            "ok": True, "done": done, "reason": reason,
            "fetched": len(by_start), "imported": len(imported),
            "written": written, "priced": priced, "off_peak_intervals": off_peak,
            "fallback_material": fb_material, "fallback_no_bucket": fb_no_bucket,
            "flags_raised": flags_raised_ch, "healed_in_pass": healed_in_pass,
            "halted_at": halted_at, "fetch_error": fetch_error, "paused": paused,
            "oldest": (imported[-1] if imported else checkpoint),
            "window": {"from": window["from"], "to": window["to"]},
        }
    out["done"] = all_done
    return out


async def repair_import_pricing(from_date=None, to_date=None, *,
                                window_days: int = 7, pace_s: float = 1.0,
                                max_windows: int = 400, headroom_frac: float = 0.15,
                                channels=("import", "export"), count_only: bool = False,
                                suspect_only: bool = False, suspect_min_kwh: float = 1.0,
                                meter_id: str = "electricity_main") -> dict:
    """Calm, targeted re-price of imported half-hours the bulk import mispriced
    because a Measurements response came back with the kWh but an unresolved
    (empty) cost `metaData` — a partial success under the import's back-to-back
    load, which we then priced from the tariff schedule (peak if out-of-window).

    This re-queries the SAME slots one small, well-spaced window at a time and
    re-prices each block in place from the real OFF_PEAK/STANDARD cost. Because a
    calm query reliably returns the cost (proven by the cost probe), this is BOTH
    the fix AND the root-cause test: the returned split says how many were
    recovered by a calm re-fetch (⇒ the misses were load-induced partial
    responses, not a genuine Octopus gap) vs how many are still cost-less
    (⇒ genuinely unavailable).

    Targets:
      - from_date/to_date given → ALL imported blocks in that local range
        (runs on the CURRENT DB — no re-import needed; pick a narrow range first).
      - otherwise → the persisted reprice queue (auto-populated by the last
        import's no-cost slots).

    Returns {ok, recovered, still_missing, checked, windows, remaining}.
    """
    global _kraken_client, _store
    if _store is None or _kraken_client is None or not kraken_available():
        return {"ok": False, "reason": "no api / store", "recovered": 0}
    import asyncio as _asyncio
    import api_import as _ai
    from datetime import datetime as _dt2, timedelta as _td2

    def _d(s):
        return _dt2.fromisoformat(str(s).replace(" ", "T").split(".")[0])

    # Resolve the per-channel target start lists.
    targets: dict = {"import": [], "export": []}
    suspect_rows: dict = {}          # start → current pricing, for the suspect prefilter
    if from_date or to_date:
        f_iso = _ai._iso(_ai._naive_utc((from_date or "2000-01-01") + "T00:00:00Z"))
        # `to_date` is INCLUSIVE of the whole picked day → advance one day for the
        # exclusive upper bound (+ a little slack for the BST local/UTC offset), so
        # "18 → 25" actually covers the 25th, not up to its 00:00.
        if to_date:
            t_iso = _ai._iso(_dt2.fromisoformat(to_date) + _td2(days=1, hours=1))
        else:
            t_iso = "2100-01-01T00:00:00"
        starts = _store.get_imported_block_starts(f_iso, t_iso, meter_id)
        # Only fill the channels the caller asked for — the range tool is IMPORT
        # pricing, and re-pricing export from cost÷kWh fragments its (usually flat)
        # rate into many buckets. `channels` scopes it; default keeps both.
        targets["import"] = list(starts) if "import" in channels else []
        targets["export"] = list(starts) if "export" in channels else []
        # Suspect-only (the auto verify pass): read each block's CURRENT pricing once
        # so we can locally skip slots a re-fetch can't improve — turning a whole-range
        # re-query into a targeted one. Falls back to full range if the read fails.
        if suspect_only:
            try:
                for _r in _store.get_imported_block_pricing(f_iso, t_iso, meter_id):
                    suspect_rows[_r["start"]] = _r
            except Exception as _se:
                logger.warning("repair_import_pricing: suspect prefilter read failed "
                               "(%s) — running full range", _se)
                suspect_only = False
    else:
        q = _store.get_reprice_queue()
        targets["import"] = sorted(q.get("import") or []) if "import" in channels else []
        targets["export"] = sorted(q.get("export") or []) if "export" in channels else []

    # Count-only preview: how many blocks a run would touch, no API calls, no writes.
    if count_only:
        return {"ok": True, "count_only": True,
                "import": len(targets["import"]), "export": len(targets["export"]),
                "count": len(targets["import"]) + len(targets["export"]),
                "mode": ("range" if (from_date or to_date) else "queue")}

    recovered = still_missing = checked = windows_done = skipped = 0
    opportunistic = 0                 # non-target neighbours re-priced from fetched data
    still_missing_starts: list = []   # the actual slots left cost-less (inspectable)
    throttled = False                 # stopped early because the rate-limit ran low

    async def _headroom_low() -> bool:
        """True if the Octopus points allowance is blocked or below headroom_frac
        of the limit — so we STOP re-fetching (rate limits are cumulative/hourly:
        pushing on only makes later windows come back cost-less, which is the very
        thing this repair exists to avoid). The caller re-runs once it recovers."""
        if headroom_frac <= 0:
            return False
        try:
            rl = await _kraken_client.get_rate_limit()
        except Exception:
            return False
        if not rl:
            return False
        if rl.get("isBlocked"):
            return True
        rem, lim = rl.get("remaining"), rl.get("pointsLimit")
        return bool(rem is not None and lim and rem < headroom_frac * lim)

    for channel in channels:
        if throttled:
            break
        starts = sorted(targets.get(channel) or [])
        if not starts:
            continue
        info = (_kraken_discovery or {}).get(channel) or {}
        mpan = info.get("mpan")
        if not mpan:
            continue
        direction = "GENERATION" if channel == "export" else "CONSUMPTION"
        try:
            if channel not in _hist_rate_segs_cache:
                _hist_rate_segs_cache[channel] = await _build_channel_rate_segs(channel)
            rate_segs = _hist_rate_segs_cache[channel]
        except Exception as e:
            logger.warning("repair_import_pricing: rate segs failed (%s): %s", channel, e)
            continue

        # Suspect prefilter (auto verify pass): drop already-correct slots locally so
        # the API only sees the ones a re-fetch could actually fix.
        if suspect_only and (from_date or to_date):
            _before = len(starts)
            # Off-peak floor OBSERVED in the imported data (the min stored rate for
            # this channel) — the fallback band discriminator when the tariff schedule
            # can't tell us the bands (IOG). Robust: computed over the whole repair
            # range, so a single fully-mislabelled day can't skew it.
            _rk = "imp_rate" if channel == "import" else "exp_rate"
            _rvals = [r.get(_rk) for r in suspect_rows.values() if (r.get(_rk) or 0) > 0]
            _floor = min(_rvals) if _rvals else None
            starts = [st for st in starts
                      if _reprice_suspect(channel, suspect_rows.get(st), rate_segs,
                                          suspect_min_kwh, _floor)]
            skipped += (_before - len(starts))
            logger.info("repair_import_pricing[%s]: suspect prefilter %d → %d slot(s)",
                        channel, _before, len(starts))
            if not starts:
                continue

        # Slots the window re-fetch STILL returns no cost for — retried one at a
        # time (single-slot, the calmest request) after the window pass, since the
        # probe showed an isolated single-slot query recovers what a wider window
        # under load does not.
        sm_starts: list = []
        # Group targets into <=window_days spans so each re-fetch is small + calm.
        groups, cur_group, anchor = [], [], None
        for st in starts:
            d = _d(st)
            if anchor is None or (d - anchor) < _td2(days=window_days):
                anchor = anchor or d
                cur_group.append(st)
            else:
                groups.append(cur_group)
                cur_group, anchor = [st], d
        if cur_group:
            groups.append(cur_group)

        for grp in groups:
            if windows_done >= max_windows:
                break
            if await _headroom_low():
                throttled = True
                logger.info("repair_import_pricing: rate-limit headroom low — "
                            "pausing after %d window(s); re-run to continue",
                            windows_done)
                break
            # Pad the window a day on BOTH sides of the target span: an IOG
            # dispatch's OFF_PEAK label is only returned when the query window spans
            # the WHOLE charge run, not just the target slot(s). The back-pad reaches
            # the run's start; the FORWARD pad reaches its tail. The forward pad is
            # essential once the suspect prefilter is in play: a run's later slots may
            # already be off-peak (so they're skipped), leaving a lone suspect at the
            # run's LEADING edge — e.g. the 05:30 start of a 05:30–07:00 charge. Ending
            # the window at that slot cuts off the run's continuation, and Octopus then
            # returns STANDARD (peak) for it. Padding forward keeps the run in context.
            # (The label is monotonic, so over-reaching is always safe; extra fetched
            #  rows are context only — we still write just the target slots.)
            win_from = _ai._iso(_d(grp[0]) - _td2(days=1))
            win_to = _ai._iso(_d(grp[-1]) + _td2(days=1))
            try:
                rows = await _kraken_client.get_measurements(
                    mpan, win_from + "Z", win_to + "Z", direction=direction)
            except Exception as e:
                logger.info("repair_import_pricing: refetch failed %s..%s: %s",
                            win_from, win_to, e)
                continue
            by_start = {r["start"]: r for r in (rows or []) if r.get("start")}
            fixed = []
            for st in grp:
                r = by_start.get(st)
                if not r:
                    continue
                checked += 1
                mc = r.get("cost_incl")
                if mc is None:            # window still no cost → try single-slot below
                    sm_starts.append(st)
                    continue
                ofp = r.get("off_peak")
                kwh = r.get("kwh") or 0
                rate = _billed_rate(rate_segs, st, ofp, mc, kwh)
                if _store.reprice_imported_block(st, meter_id, channel, rate, mc):
                    recovered += 1
                    fixed.append(st)
            if fixed and not (from_date or to_date):
                _store.clear_reprice_queue_slots(channel, fixed)

            # Opportunistic neighbours (suspect-only): the window we just fetched
            # already carries Octopus's label for EVERY half-hour in it, not only the
            # suspects. Re-price any OTHER imported block in the window that the fresh
            # data shows is mispriced — but ONLY downward (cheaper) or to fill a
            # missing cost, never off-peak → peak. This mops up the small charge-run
            # EDGE slots (partial half-hours below the suspect kWh threshold) sitting
            # next to a material dispatch slot, at ZERO extra API cost. Downward-only
            # means a bad-weather STANDARD re-fetch can't do collateral damage.
            if suspect_only:
                _imp = (channel == "import")
                grp_set = set(grp)
                for st, r in by_start.items():
                    if st in grp_set:
                        continue
                    cur = suspect_rows.get(st)
                    if cur is None:           # not an imported block in range → skip
                        continue
                    mc = r.get("cost_incl")
                    if mc is None:
                        continue
                    new_rate = _billed_rate(rate_segs, st, r.get("off_peak"),
                                            mc, r.get("kwh") or 0)
                    if new_rate is None:
                        continue
                    cur_rate = cur.get("imp_rate") if _imp else cur.get("exp_rate")
                    cur_cost = cur.get("imp_cost") if _imp else cur.get("exp_cost")
                    improves = (cur_cost is None) or (
                        cur_rate is not None and new_rate < cur_rate - 1e-6)
                    if not improves:
                        continue
                    if _store.reprice_imported_block(st, meter_id, channel, new_rate, mc):
                        recovered += 1
                        opportunistic += 1

            windows_done += 1
            if pace_s:
                await _asyncio.sleep(pace_s)

        # Context-anchored straggler pass: the window re-fetch left these cost-less;
        # recover_measurement_costs re-fetches each in a wide day-anchored window
        # (calm + retried). Skip it when throttled — adding load would only produce
        # more empties. Only slots still empty afterwards are truly left for a re-run.
        if sm_starts and not throttled:
            try:
                _rec = await _kraken_client.recover_measurement_costs(
                    mpan, sm_starts, direction=direction, pace_s=pace_s or 0.4)
            except Exception as e:
                _rec = {}
                logger.info("repair_import_pricing: single-slot pass failed: %s", e)
            fixed_ss = []
            for st in sm_starts:
                node = _rec.get(st)
                mc = node.get("cost_incl") if node else None
                if mc is None:
                    still_missing += 1
                    still_missing_starts.append(st)
                    continue
                ofp = node.get("off_peak")
                rate = _billed_rate(rate_segs, st, ofp, mc, node.get("kwh") or 0)
                if _store.reprice_imported_block(st, meter_id, channel, rate, mc):
                    recovered += 1
                    fixed_ss.append(st)
            if fixed_ss and not (from_date or to_date):
                _store.clear_reprice_queue_slots(channel, fixed_ss)

    if recovered:
        await _regen_charts_after_import_async({"written": {"import": recovered}})
    remaining = _store.reprice_queue_count()
    # Self-diagnostic: how much imported data exists at all + how many targets we
    # resolved — so a "0 checked" result says WHY (no imports vs none in range vs
    # empty queue) instead of leaving us blind.
    info = {}
    try:
        info = _store.count_imported_history()
    except Exception:
        pass
    n_targets = len(targets.get("import") or []) + (0 if (from_date or to_date)
                                                    else len(targets.get("export") or []))
    logger.info("repair_import_pricing: recovered %d, still-missing %d (of %d "
                "checked) in %d window(s); targets=%d imported_total=%s (%s..%s)",
                recovered, still_missing, checked, windows_done, n_targets,
                info.get("blocks"), info.get("from"), info.get("to"))
    return {"ok": True, "recovered": recovered, "still_missing": still_missing,
            "checked": checked, "windows": windows_done, "remaining": remaining,
            "skipped": skipped,       # already-correct slots the suspect prefilter dropped
            "opportunistic": opportunistic,   # neighbours re-priced from already-fetched data
            "suspect_only": bool(suspect_only),
            "targets": n_targets, "mode": ("range" if (from_date or to_date) else "queue"),
            "throttled": throttled,   # stopped early on low rate-limit headroom
            "missing": sorted(still_missing_starts)[:200],   # inspectable leftovers
            "imported_blocks": info.get("blocks"),
            "imported_from": info.get("from"), "imported_to": info.get("to")}


# ── API import as a background job (pause / resume / cancel) ──────────────────
# The import runs as a task on the engine loop, not driven by the browser, so it
# survives navigating away and can be paused/cancelled. State is in-memory (single
# add-on process); the per-channel checkpoint in kraken_state means a restart
# mid-import loses only the job UI, not the progress (re-Start resumes idempotently).
_api_import_job: dict = {"status": "idle"}


def api_import_status() -> dict:
    """JSON-safe snapshot of the background import job."""
    return {k: v for k, v in _api_import_job.items() if k != "task"}


def api_import_running() -> bool:
    return _api_import_job.get("status") in ("running", "paused", "rate_limited")


def api_import_control(action: str) -> dict:
    """pause / resume / cancel the running import (sets a flag the worker polls)."""
    j = _api_import_job
    if not api_import_running():
        return {"ok": False, "status": j.get("status"), "note": "no active import"}
    if action == "pause":
        j["control"] = "pause"
    elif action == "resume":
        j["control"] = "run"
    elif action == "cancel":
        j["control"] = "cancel"
    else:
        return {"ok": False, "error": f"unknown action: {action}"}
    return {"ok": True, "status": j.get("status"), "control": j["control"]}


def _regen_charts_after_import(j) -> None:
    """Rebuild the pre-built billing/heatmap charts after an import so the views
    reflect the imported blocks immediately — not only after the next live block
    finalises. Only runs if the import actually wrote something. Non-fatal."""
    try:
        written = j.get("written") or {}
        if not any((written.get(ch) or 0) for ch in written):
            return
        if _store is not None and _store.count_blocks():
            # Off the loop (read-only connection) — a synchronous render over the
            # full imported history stalls the tick / HA heartbeat (→ restart).
            _schedule_chart_regen()
            logger.info("api_import: charts regenerated after import "
                        "(import=%d export=%d)",
                        written.get("import", 0), written.get("export", 0))
    except Exception as e:
        logger.warning("api_import: chart regen after import failed: %s", e)


async def _regen_charts_after_import_async(j) -> None:
    """Terminal-step chart regen for the background import job — AWAITS the render.

    _regen_charts_after_import fires the render as a fire-and-forget loop task
    (right for the live tick, which must not stall the HA heartbeat). At the END
    of the import job, though, we want the job to only report 'done' once the
    charts actually reflect the imported history — and awaiting here is safe
    because the import job IS the background work (it is not the live tick, so
    the heartbeat is unaffected). Same write-gate + non-fatal contract."""
    try:
        written = j.get("written") or {}
        if not any((written.get(ch) or 0) for ch in written):
            return
        if _store is not None and _store.count_blocks():
            await _generate_charts_offloaded()
            logger.info("api_import: charts regenerated after import "
                        "(import=%d export=%d)",
                        written.get("import", 0), written.get("export", 0))
    except Exception as e:
        logger.warning("api_import: chart regen after import failed: %s", e)


def _import_extend_floor(earliest_import, plan) -> "str | None":
    """The earliest date it is SAFE to pull the current (oldest existing) config
    period's start back to after an import.

    Normally the earliest imported block. BUT when a **move** is detected (the
    region probe stashed a multi-site reconcile plan awaiting confirmation), the
    oldest existing period is the CURRENT site — so we stop at that site's move-in
    (the latest tenancy's `from`) and never cross into an earlier site's span.
    That prevents pre-move data (and its carbon region) being claimed by the
    current site before the user confirms the split. Pure/testable."""
    if plan and plan.get("needs_confirmation"):
        froms = [s.get("from") for s in (plan.get("sites") or []) if s.get("from")]
        if froms:
            return max(froms)
    return earliest_import


def _finalise_after_import() -> None:
    """Post-import housekeeping:

    1. **Extend the earliest config period** back to cover the imported history,
       so Billing history reflects the real data start (not the go-live date) and
       every imported block falls inside a period that temporally covers it —
       clamped by `_import_extend_floor` so a pending move never swallows an
       earlier site.
    2. **Re-arm the carbon backfill** so a (re-)import's NULL-carbon blocks fill
       automatically (a stale `unfilled_from` marker would otherwise block the
       self-heal re-arm). DEFERRED while a move awaits confirmation — pre-move
       blocks would resolve to the current region until the split is applied; the
       reconcile-apply re-arms once the sites (and regions) are set.
    """
    if _store is None:
        return
    move_pending = False
    try:
        plan = _store.get_meta("region_reconcile_pending", None)
        move_pending = bool(plan and plan.get("needs_confirmation"))
        info = _store.count_imported_history()
        floor = _import_extend_floor(info.get("from"), plan)
        if floor:
            _store.extend_earliest_period_to(floor)
    except Exception as e:
        logger.warning("_finalise_after_import: period extend failed: %s", e)
    if move_pending:
        logger.info("_finalise_after_import: move awaiting confirmation — "
                    "deferring carbon re-arm to the reconcile apply")
        return
    try:
        _store.rearm_carbon_backfill()
    except Exception as e:
        logger.warning("_finalise_after_import: carbon re-arm failed: %s", e)


async def _drain_reprice_queue_after_import(j: dict | None = None,
                                            *, pace_s: float = 0.4) -> dict:
    """Auto FINAL calm recovery pass — run once when the bulk import completes.

    The in-pass recovery (recover_measurement_costs during the import loop) runs
    UNDER the import's own load, so a few IOG dispatch slots — charged off-peak
    OUTSIDE the fixed off-peak window (e.g. a morning top-up) — can still come back
    with empty `statistics`, get schedule-priced at PEAK, and land in the reprice
    queue. Left there, they inflate the PEAK side of the off-peak/peak split (the
    classic wrong-split symptom). Nothing drained that queue except a manual
    'reprice queue' / 'Fix a date range' click — which is why the split only came
    right when repaired by hand.

    Now that the import is finished the connection is calm, so the look-back ladder
    finally has the window context it needs: draining the queue here relabels those
    slots OFF_PEAK and reprices them from the REAL cost, correcting the split
    automatically. Non-fatal; a rate-limit pause or a genuine gap simply leaves the
    remainder queued for the next import or a manual re-run."""
    out = {"recovered": 0, "still_missing": 0, "throttled": False, "ran": False}
    try:
        if _store is None or not kraken_available():
            return out
        if _store.reprice_queue_count() <= 0:
            return out
        res = await repair_import_pricing(pace_s=pace_s)     # queue mode (no dates)
        out.update({"recovered": res.get("recovered", 0),
                    "still_missing": res.get("still_missing", 0),
                    "throttled": bool(res.get("throttled")), "ran": True})
        logger.info("final recovery pass: recovered %d, still-missing %d%s "
                    "(auto-drained the reprice queue post-import)",
                    out["recovered"], out["still_missing"],
                    " — throttled, remainder still queued" if out["throttled"] else "")
    except Exception as e:
        logger.warning("final recovery pass failed (queue left intact): %s", e)
    if j is not None:
        j["final_recovery"] = out
    return out


# ── Deferred pricing verification ─────────────────────────────────────────────
# The bulk import mislabels the leading/trailing/one-off half-hours of an IOG
# smart charge (an out-of-window dispatch): they come back bucket-less, get the
# peak default, and are too small to be 'flagged' for the in-pass recovery — so a
# fresh import leaves the off-peak/peak split slightly wrong and the only reliable
# cure is re-checking every slot with a CALM, look-back-wide window (exactly what
# 'Fix a date range' does). That needs a rested rate-limit allowance, which the
# import itself has just spent — so we run it DEFERRED: after the import, in calm,
# resumable, newest-first chunks, gated on the API allowance, so the split self-
# heals with no user action and no manual tool. Progress is reported live.
_VERIFY_CURSOR_KEY = "verify_pricing_cursor"     # kraken_state: oldest date still to verify
_VERIFY_SUMMARY_KEY = "verify_pricing_summary"   # kraken_state: last completed run summary
_verify_job: dict = {"status": "idle"}


def _persist_verify_summary(j: dict) -> None:
    """Persist a compact summary of the last completed verify pass so the import
    page can show 'last verified: corrected N over <span>, took <t>' on any load —
    including after an add-on restart clears the in-memory job."""
    import json as _json
    if _store is None:
        return
    try:
        _store.set_kraken_state(_VERIFY_SUMMARY_KEY, _json.dumps({
            "status": "done", "stale": True,
            "repriced": int(j.get("repriced") or 0),
            "checked": int(j.get("checked") or 0),
            "skipped": int(j.get("skipped") or 0),
            "elapsed_s": int(j.get("elapsed_s") or 0),
            "corrections": (j.get("corrections") or [])[-60:],
            "finished_at": j.get("finished_at"),
        }))
    except Exception as e:
        logger.warning("verify pricing: persist summary failed: %s", e)


def api_verify_pricing_status() -> dict:
    """JSON-safe snapshot of the deferred pricing-verification pass. When no pass is
    live (idle — e.g. after a restart), fall back to the last persisted summary so
    the UI can still show what the previous run corrected and how long it took."""
    import json as _json
    if _verify_job.get("status") in (None, "idle") and _store is not None:
        try:
            raw = _store.get_kraken_state(_VERIFY_SUMMARY_KEY)
            if raw:
                return _json.loads(raw)
        except Exception:
            pass
    return dict(_verify_job)


async def _verify_allowance_low(headroom_frac: float) -> bool:
    """True if the Octopus rate-limit allowance is too low to re-check calmly
    (blocked, or remaining < headroom_frac × limit) — so the verify pass should
    wait. Best-effort: a failed/absent rate-limit read is treated as 'ok to go'."""
    try:
        rl = await _kraken_client.get_rate_limit()
    except Exception:
        return False
    if not rl:
        return False
    if rl.get("isBlocked"):
        return True
    rem, lim = rl.get("remaining"), rl.get("pointsLimit")
    return bool(rem is not None and lim and rem < headroom_frac * lim)


async def run_deferred_verify_pricing(*, chunk_days: int = 30, headroom_frac: float = 0.5,
                                      wait_s: float = 60.0, max_wait_cycles: int = 120,
                                      restart: bool = True) -> dict:
    """Re-check the imported period's PRICING calmly, in resumable newest-first
    chunks gated on the rate-limit allowance. Idempotent (re-pricing a correct slot
    is a no-op). Runs on the engine loop; every await yields, so the heartbeat is
    unaffected. `restart` resets the cursor to the newest edge (fresh pass); omit it
    to RESUME an interrupted pass from its persisted cursor."""
    global _verify_job
    _asyncio = __import__("asyncio")
    _dt = datetime
    _td = timedelta
    if _store is None or not kraken_available():
        return {"ran": False}
    try:
        info = _store.count_imported_history() or {}
    except Exception:
        info = {}
    sfrom, sto = info.get("from"), info.get("to")
    if not sfrom or not sto:
        return {"ran": False}
    try:
        lo = _dt.fromisoformat(str(sfrom)[:19])
        hi = _dt.fromisoformat(str(sto)[:19])
    except ValueError:
        return {"ran": False}
    if restart:
        cursor = hi
        try:
            _store.set_kraken_state(_VERIFY_CURSOR_KEY, cursor.isoformat())
        except Exception:
            pass
    else:
        cur_raw = _store.get_kraken_state(_VERIFY_CURSOR_KEY)
        try:
            cursor = _dt.fromisoformat(cur_raw[:19]) if cur_raw else hi
        except (ValueError, TypeError):
            cursor = hi
    total_chunks = max(1, int((hi - lo).days // chunk_days) + 1)
    done_chunks = max(0, total_chunks - (max(1, int((cursor - lo).days // chunk_days) + 1)))
    _t0 = __import__("time").monotonic()
    corrections: list = []        # per-chunk {from,to,repriced} for days actually changed
    _verify_job.clear()
    _verify_job.update({"status": "running", "phase": "verifying",
                        "chunks_total": total_chunks, "chunks_done": done_chunks,
                        "repriced": 0, "checked": 0, "skipped": 0, "elapsed_s": 0,
                        "from": None, "to": None, "corrections": [],
                        "started_at": _dt_now_iso_safe()})
    repriced = checked = skipped = 0
    logger.info("verify pricing: starting over %s..%s in ~%d chunk(s)",
                lo.date(), hi.date(), total_chunks)
    try:
        while cursor > lo:
            c_to = cursor
            c_from = max(lo, cursor - _td(days=chunk_days))
            # Gate on the allowance — wait (yielding) until the API is calm enough.
            waited = 0
            while waited < max_wait_cycles and await _verify_allowance_low(headroom_frac):
                _verify_job["status"] = "waiting"
                _verify_job["phase"] = "waiting for API allowance to recover"
                await _asyncio.sleep(wait_s)
                waited += 1
            if waited >= max_wait_cycles:
                _verify_job["status"] = "paused"
                _verify_job["phase"] = "paused — API allowance still low, will resume"
                logger.info("verify pricing: paused (allowance low); cursor left at %s",
                            cursor.isoformat())
                return {"ran": True, "paused": True, "repriced": repriced}
            _verify_job["status"] = "running"
            _verify_job["phase"] = "verifying"
            _verify_job["from"] = c_from.date().isoformat()
            _verify_job["to"] = c_to.date().isoformat()
            try:
                res = await repair_import_pricing(
                    c_from.date().isoformat(), c_to.date().isoformat(),
                    channels=("import",), pace_s=0.4, suspect_only=True)
            except Exception as e:
                logger.warning("verify pricing: chunk %s..%s failed: %s",
                               c_from.date(), c_to.date(), e)
                res = {}
            _cr = int(res.get("recovered") or 0)
            _ck = int(res.get("checked") or 0)
            _sk = int(res.get("skipped") or 0)
            repriced += _cr; checked += _ck; skipped += _sk
            _elapsed = int(__import__("time").monotonic() - _t0)
            logger.info("verify pricing: %s..%s — corrected %d of %d re-checked "
                        "(%d already-correct skipped) · %ds elapsed",
                        c_from.date(), c_to.date(), _cr, _ck, _sk, _elapsed)
            if res.get("throttled"):
                # Paused mid-chunk on low headroom — do NOT advance the cursor; wait
                # and retry the same chunk (idempotent) once the allowance recovers.
                _verify_job.update({"status": "waiting",
                                    "phase": "waiting for API allowance to recover",
                                    "repriced": repriced, "checked": checked,
                                    "skipped": skipped, "elapsed_s": _elapsed})
                await _asyncio.sleep(wait_s)
                continue
            if _cr > 0:               # record only the days that actually changed
                corrections.append({"from": c_from.date().isoformat(),
                                    "to": c_to.date().isoformat(), "repriced": _cr})
                corrections[:] = corrections[-60:]
            cursor = c_from
            try:
                _store.set_kraken_state(_VERIFY_CURSOR_KEY, cursor.isoformat())
            except Exception:
                pass
            _verify_job.update({"chunks_done": _verify_job.get("chunks_done", 0) + 1,
                                "repriced": repriced, "checked": checked,
                                "skipped": skipped, "elapsed_s": _elapsed,
                                "corrections": list(corrections)})
        try:
            _store.set_kraken_state(_VERIFY_CURSOR_KEY, "")   # completed → clear cursor
        except Exception:
            pass
        _elapsed = int(__import__("time").monotonic() - _t0)
        _verify_job.update({"status": "done", "phase": "done",
                            "repriced": repriced, "checked": checked, "skipped": skipped,
                            "elapsed_s": _elapsed, "corrections": list(corrections),
                            "finished_at": _dt_now_iso_safe()})
        _persist_verify_summary(dict(_verify_job))
        logger.info("verify pricing: complete — corrected %d slot(s) across %d day-range(s) "
                    "(re-checked %d, skipped %d already-correct) in %ds",
                    repriced, len(corrections), checked, skipped, _elapsed)
        return {"ran": True, "repriced": repriced, "checked": checked,
                "skipped": skipped, "elapsed_s": _elapsed}
    except Exception as e:
        logger.warning("verify pricing pass failed: %s", e)
        _verify_job.update({"status": "error", "error": str(e)})
        return {"ran": True, "error": str(e)}


_IMPORT_HEALTH_KEY = "import_health_summary"


def _reset_pricing_health() -> None:
    """Clear the PREVIOUS import's pricing-health + verify state so the panel resets
    the moment a new import starts, instead of showing stale 'raised/recovered' or a
    'Last verified' summary from the run being replaced. The fresh run repopulates
    everything as it goes; the reprice queue is cleared so 'still need a retry' can't
    carry over slots the new import will re-flag itself."""
    global _verify_job
    if _store is None:
        return
    for k in (_IMPORT_HEALTH_KEY, _VERIFY_SUMMARY_KEY, _VERIFY_CURSOR_KEY):
        try:
            _store.set_kraken_state(k, "")
        except Exception:
            pass
    try:
        _store.clear_reprice_queue()
    except Exception:
        pass
    _verify_job.clear()
    _verify_job.update({"status": "idle"})


def _persist_import_health(j: dict) -> None:
    """Persist a compact post-import health summary to kraken_state so the import
    page's health panel renders on ANY load, not only right after a run. 'remaining'
    is always re-read LIVE from the reprice queue; raised/auto_recovered are this
    run's tallies (in-pass + final calm drain)."""
    import json as _json
    if _store is None:
        return
    info = {}
    try:
        info = _store.count_imported_history() or {}
    except Exception:
        pass
    remaining = 0
    try:
        remaining = int(_store.reprice_queue_count())
    except Exception:
        pass
    summary = {
        "raised": int(j.get("flags_raised") or 0),
        "auto_recovered": int(j.get("auto_recovered") or 0),
        "remaining": remaining,
        "blocks": int(info.get("blocks") or 0),
        "from": info.get("from"),
        "to": info.get("to"),
        "finished_at": _dt_now_iso_safe(),
    }
    _store.set_kraken_state(_IMPORT_HEALTH_KEY, _json.dumps(summary))


def api_import_health() -> dict:
    """Post-import health summary for the import page's persistent panel. Reads the
    persisted run summary and overlays the LIVE remaining-queue count, so 'still
    need a retry' drops the instant a repair clears slots."""
    import json as _json
    out = {"ok": True, "have": False, "raised": 0, "auto_recovered": 0,
           "remaining": 0, "blocks": 0, "from": None, "to": None}
    if _store is None:
        return out
    try:
        raw = _store.get_kraken_state(_IMPORT_HEALTH_KEY)
        if raw:
            out.update(_json.loads(raw))
            out["have"] = True
    except Exception:
        pass
    try:
        out["remaining"] = int(_store.reprice_queue_count())
    except Exception:
        pass
    return out


_REGION_CLOBBER_MARKER   = "region_probe_clobber_done"   # store_meta: one-time upgrade overwrite
_REGION_RECONCILE_MARKER = "region_reconcile_pending"    # store_meta: plan awaiting user confirmation
_PREIMPORT_SITES_MARKER  = "preimport_sites_applied"     # store_meta: sites confirmed+created BEFORE this import


async def discover_pre_import_sites() -> dict:
    """READ-ONLY. Fetch the account's property history and classify it for the
    PRE-import 'confirm your sites' wizard step (BlockStore.plan_pre_import_sites).

    Returns {"ok": True, "needs_confirmation": bool, "sites": [...]} — the latest
    tenancy is the current site (read-only instance name), earlier ones are past
    addresses the user names so a covering period can be created for each BEFORE
    the import runs, letting every imported block land in the right (correctly
    regioned, carbon-eligible) period from the first write. Writes nothing. A
    never-moved account returns needs_confirmation False so the wizard skips
    straight to the import."""
    global _kraken_client, _kraken_account_number, _store
    try:
        if _kraken_client is None or _store is None:
            return {"ok": False, "error": "no API / store"}
        from block_store import derive_region_periods
        account = await _kraken_client.get_account(_kraken_account_number)
        spans = derive_region_periods(account or {})
        plan = _store.plan_pre_import_sites(spans)
        return {"ok": True, **plan}
    except Exception as e:
        logger.warning("discover_pre_import_sites: %s", e)
        return {"ok": False, "error": str(e) or type(e).__name__}


async def _probe_and_apply_region_periods() -> dict | None:
    """If the API is configured, read the account's property address history,
    reduce it to OUTWARD CODES only, and stamp the DNO region onto the config
    period timeline (BlockStore.apply_region_periods). Best-effort; never raises.

    This is the region foundation for historical carbon: it records which region
    applied when, so carbon can later be attributed per-period rather than from a
    single global postcode. Full postcodes are never stored — only the outward
    code (see block_store.outward_code / derive_region_periods).

    ONE-TIME upgrade clobber: the first time this runs on a given DB, it
    overwrites even a user-entered postcode with the authoritative Octopus supply
    address (so a legacy wrong-region entry — e.g. DE1 where the supply is DE65 —
    self-corrects). After that it respects manual edits (overwrite_user=False),
    so CSV-only users and deliberate overrides stand."""
    global _kraken_client, _kraken_account_number, _store
    try:
        if _kraken_client is None or _store is None:
            return None
        from block_store import derive_region_periods
        account = await _kraken_client.get_account(_kraken_account_number)
        spans = derive_region_periods(account or {})
        if not spans:
            return None
        # A move (or an unnamed site) needs the user to name sites and confirm the
        # config-period split — stash the plan for the confirmation UI rather than
        # silently rewriting billing history.
        plan = _store.plan_region_reconciliation(spans)
        if plan.get("needs_confirmation"):
            _store.set_meta(_REGION_RECONCILE_MARKER, plan)
            logger.info("_probe_and_apply_region_periods: %d site(s) need "
                        "confirmation, %d split(s) proposed — stashed for UI",
                        len(plan.get("sites") or []), len(plan.get("split_dates") or []))
            return plan
        # Single site, nothing to confirm → silent apply (with one-time clobber).
        first_time = not (_store.get_meta(_REGION_CLOBBER_MARKER, None))
        res = _store.apply_region_periods(spans, overwrite_user=first_time)
        if first_time:
            _store.set_meta(_REGION_CLOBBER_MARKER, _dt_now_iso_safe())
        if res.get("applied"):
            try:
                _store.rearm_carbon_backfill()   # region now known → re-scan carbon
            except Exception:
                pass
        logger.info("_probe_and_apply_region_periods: regions=%s applied=%d "
                    "split_required=%s one_time_clobber=%s",
                    res.get("outcodes"), res.get("applied"),
                    res.get("split_required"), first_time)
        return res
    except Exception as e:
        logger.warning("_probe_and_apply_region_periods: %s", e)
        return None


async def run_api_import_job(requested_from=None, *, chunk_days: int = 60,
                             max_chunks: int = 1, pace_s: float = 1.5,
                             headroom_frac: float = 0.25) -> None:
    """Background worker: loop `import_api_history` slices until done, honouring
    the pause/cancel control flag between slices and during rate-limit cooldown.
    Updates `_api_import_job` for the status endpoint. Never raises."""
    j = _api_import_job
    j.clear()
    j.update({"status": "running", "phase": "importing", "control": "run",
              "started_at": _dt_now_iso_safe(),
              "written": {"import": 0, "export": 0}, "oldest": {},
              "flags_raised": 0, "auto_recovered": 0,
              "reason": None, "error": None, "done": False, "go_live": None})
    _reset_pricing_health()          # clear the PREVIOUS run's health/verify panel
    first = True
    try:
        while True:
            ctl = j.get("control")
            if ctl == "cancel":
                j["status"] = "cancelled"
                await _regen_charts_after_import_async(j)   # show what did import
                return
            if ctl == "pause":
                j["status"] = "paused"
                await asyncio.sleep(1.0)
                continue
            j["status"] = "running"
            j["phase"] = "importing"
            res = await import_api_history(
                requested_from, chunk_days=chunk_days, max_chunks=max_chunks,
                pace_s=pace_s, headroom_frac=headroom_frac, restart=first)
            first = False
            if not res.get("ok"):
                j["status"] = "error"
                j["error"] = res.get("reason") or "failed"
                return
            j["go_live"] = res.get("go_live")
            rate_limited = False
            for ch, c in (res.get("channels") or {}).items():
                if not isinstance(c, dict):
                    continue
                j["written"][ch] = j["written"].get(ch, 0) + (c.get("written") or 0)
                # Post-import health tallies (see _persist_import_health).
                j["flags_raised"] = j.get("flags_raised", 0) + (c.get("flags_raised") or 0)
                j["auto_recovered"] = j.get("auto_recovered", 0) + (c.get("healed_in_pass") or 0)
                if c.get("oldest"):
                    j["oldest"][ch] = c["oldest"]
                if c.get("reason") == "rate_limit":
                    rate_limited = True
            if res.get("done"):
                # Block-writing is finished, but a calm tail (region probe →
                # price recovery → chart rebuild) still runs. Surface it as phases
                # so the UI shows progress instead of a frozen block count.
                j["done"] = True
                j["status"] = "finalising"
                j["phase"] = "finalising"
                # Foundation for historical carbon: record which DNO region applied
                # when, from the account's property history (outward code only).
                # If the user already confirmed + created the site periods BEFORE
                # this import (the pre-import wizard step), the timeline is already
                # correct per period — running the post-import probe again would
                # re-stamp the latest region across the past periods, so skip it
                # (consume the marker) and let the pre-import layout stand.
                try:
                    if _store and _store.get_meta(_PREIMPORT_SITES_MARKER, None):
                        _store.set_meta(_PREIMPORT_SITES_MARKER, None)
                        logger.info("run_api_import_job: pre-import sites already "
                                    "applied — skipping post-import region probe")
                    else:
                        await _probe_and_apply_region_periods()
                except Exception as _e:
                    logger.warning("run_api_import_job: region probe failed: %s", _e)
                try:
                    _finalise_after_import()
                except Exception as _e:
                    logger.warning("run_api_import_job: post-import finalise failed: %s", _e)
                # Auto final calm recovery: drain the reprice queue now the connection
                # is calm, so the few off-peak-outside-window dispatch slots the bulk
                # load left peak-priced get relabelled OFF_PEAK — fixing the split
                # automatically instead of needing a manual 'reprice queue' click.
                j["phase"] = "recovering_prices"
                try:
                    _fin = await _drain_reprice_queue_after_import(j)
                    j["auto_recovered"] = j.get("auto_recovered", 0) + (_fin.get("recovered") or 0)
                except Exception as _e:
                    logger.warning("run_api_import_job: final recovery pass failed: %s", _e)
                j["phase"] = "rebuilding_charts"
                await _regen_charts_after_import_async(j)
                try:
                    _persist_import_health(j)
                except Exception as _e:
                    logger.warning("run_api_import_job: health persist failed: %s", _e)
                # Deferred, rate-limit-gated pricing verification: re-check the whole
                # imported span calmly to relabel the out-of-window dispatch slots the
                # bulk import couldn't (the small off-peak/peak-split error). Runs
                # DETACHED on the loop so the import job reports 'done' immediately —
                # the verify has its own live status the UI polls.
                try:
                    asyncio.create_task(run_deferred_verify_pricing())
                except Exception as _e:
                    logger.warning("run_api_import_job: verify launch failed: %s", _e)
                j["status"] = "done"
                j["phase"] = "done"
                return
            if rate_limited:
                j["status"] = "rate_limited"
                for _ in range(60):     # ~60s cooldown, but stay responsive
                    if j.get("control") in ("cancel", "pause"):
                        break
                    await asyncio.sleep(1.0)
    except Exception as e:
        logger.warning("run_api_import_job: failed: %s", e)
        j["status"] = "error"
        j["error"] = str(e)


async def _maybe_run_settlement_sweep() -> None:
    """Daily-cadenced sweep of unsettled gaps that aged out of the incremental
    poll's sliding window (late/out-of-order DCC settlement, e.g. export lagging
    import by days). Re-fetches oldest-unsettled → now bounded to the horizon, so
    such gaps self-heal without the user clicking 'Retry settlement'. Gated to run
    at most once per _SETTLEMENT_SWEEP_MIN_INTERVAL_S and non-fatal — a failure
    must never break the poll loop."""
    try:
        now = datetime.now(timezone.utc)
        last = _store.get_kraken_state(_STATE_LAST_SWEEP)
        if not _sweep_is_due(last, now, _SETTLEMENT_SWEEP_MIN_INTERVAL_S):
            return
        if _store.count_unsettled_blocks() == 0:
            _store.set_kraken_state(_STATE_LAST_SWEEP, now.isoformat())
            return
        res = await retry_settlement_for_unsettled(
            now=now, max_lookback_days=_SETTLEMENT_SWEEP_HORIZON_DAYS)
        _store.set_kraken_state(_STATE_LAST_SWEEP, now.isoformat())
        if res.get("ok"):
            logger.info("settlement_sweep: settled_import=%d settled_export=%d "
                        "unsettled %s→%s (horizon=%dd)",
                        res.get("settled_import", 0), res.get("settled_export", 0),
                        res.get("unsettled_before"), res.get("unsettled_after"),
                        _SETTLEMENT_SWEEP_HORIZON_DAYS)
        else:
            logger.info("settlement_sweep: skipped (%s)", res.get("reason"))
        # Finalise blocks that aged past the horizon still unsettled — DCC isn't
        # coming, so mark them finalised-from-CAD (distinct from a real
        # settlement; imp_kwh_api stays NULL) so the unsettled count can reach
        # zero. Reversible: a later settlement clears the flag in the store.
        cutoff = (now.replace(tzinfo=None)
                  - timedelta(days=_SETTLEMENT_SWEEP_HORIZON_DAYS)).isoformat()
        finalised = _store.finalise_past_horizon_blocks(cutoff)
        if finalised:
            logger.info("settlement_sweep: finalised %d past-horizon block(s) "
                        "from CAD (>%dd unsettled, DCC not expected)",
                        finalised, _SETTLEMENT_SWEEP_HORIZON_DAYS)
    except Exception as e:
        logger.warning("settlement_sweep: failed (non-fatal): %s", e)


def _kraken_pre_live_snapshot() -> bool:
    """Take a one-time blocks.db snapshot before the first live DCC write.

    Idempotent: gated on the kraken_state marker _KRAKEN_SNAPSHOT_DONE_KEY, so
    it fires once ever (across restarts) and is a no-op thereafter. Mirrors the
    upgrade-backup mechanism: an online SQLite backup zipped into the share
    backups dir. Returns True if it is safe to proceed with live writes (either
    the snapshot was taken now, already taken before, or there are no blocks to
    risk); returns False only if the snapshot was attempted and failed, in
    which case the caller must NOT write.
    """
    try:
        if _store.get_kraken_state(_KRAKEN_SNAPSHOT_DONE_KEY):
            return True  # already snapshotted on a prior run
    except Exception:
        pass
    try:
        if _store.count_blocks() == 0:
            # Nothing to protect; mark done so we don't re-check forever.
            _store.set_kraken_state(_KRAKEN_SNAPSHOT_DONE_KEY,
                                    _dt_now_iso_safe())
            return True
        import zipfile as _zf, glob as _gl
        bk_dir = f"{SHARE_BACKUP_DIR}/backups"
        ensure_dir(bk_dir)
        ts = datetime.utcnow().strftime("%Y%m%dT%H%M%S")
        bk_path = f"{bk_dir}/{ts}_pre_kraken_live.zip"
        try:
            _store._conn.execute("PRAGMA wal_checkpoint(PASSIVE)")
        except Exception:
            pass
        with _zf.ZipFile(bk_path, "w", _zf.ZIP_DEFLATED) as bkz:
            _store.backup(BLOCKS_DB_PATH + ".kraken_bak")
            bkz.write(BLOCKS_DB_PATH + ".kraken_bak", "blocks.db")
            os.remove(BLOCKS_DB_PATH + ".kraken_bak")
        _store.set_kraken_state(_KRAKEN_SNAPSHOT_DONE_KEY, ts)
        logger.info("=" * 60)
        logger.info("kraken_pre_live_snapshot: blocks.db snapshotted before "
                    "first live DCC write → %s", os.path.basename(bk_path))
        logger.info("  Rollback: stop add-on, unzip this over "
                    "%s, restart.", BLOCKS_DB_PATH)
        logger.info("=" * 60)
        return True
    except Exception as e:
        logger.error("kraken_pre_live_snapshot: FAILED (%s) — refusing to write "
                     "live until a snapshot succeeds", e)
        return False


def _dt_now_iso_safe() -> str:
    try:
        return datetime.now(timezone.utc).replace(tzinfo=None).isoformat()
    except Exception:
        return ""


async def _refresh_kraken_rate_schedules() -> None:
    """Build/refresh the cached rate schedules for import and export from the
    discovered tariffs. Cheap (fixed tariffs ≈ 1 record). Failures leave the
    previous cache intact; an empty cache simply means the resolver returns
    None and zero-rate blocks stay flagged for the tooling.
    """
    global _kraken_rate_schedules, _kraken_standing_schedule, _rate_schedule_unsupported
    if _kraken_client is None or not _kraken_discovery:
        return
    try:
        from kraken_rates import (build_rate_schedule,
                                  build_standing_charge_schedule, RateFetchError)
    except Exception as e:
        logger.warning("_refresh_kraken_rate_schedules: import failed: %s", e)
        return
    new_cache: dict = {}
    unsupported: dict | None = None
    # Did we get a DEFINITIVE (non-error) result for the import channel this
    # cycle? A transient fetch failure must not raise — or clear — the banner.
    import_determined = False
    for ch in ("import", "export"):
        info = _kraken_discovery.get(ch) or {}
        product = info.get("product_code")
        tariff = info.get("tariff_code")
        if not product or not tariff:
            continue
        try:
            sched = await build_rate_schedule(
                _kraken_client, product, tariff, raise_on_error=True)
        except RateFetchError as e:
            # The fetch FAILED (transport / HTTP / edge 403) — this is NOT an
            # unsupported tariff. Keep the last-known schedule and the previous
            # unsupported state untouched, so an API blip can't flip the
            # "tariff unsupported" banner (or a stale banner can't clear wrongly).
            logger.warning("_refresh_kraken_rate_schedules: %s rate fetch failed "
                           "transiently (%s); keeping last-known schedule", ch, e)
            continue
        except Exception as e:
            logger.warning("_refresh_kraken_rate_schedules: %s build failed: %s",
                           ch, e)
            continue
        if ch == "import":
            import_determined = True
        if not sched.is_empty():
            new_cache[ch] = sched
        elif ch == "import":
            # Fetch SUCCEEDED but the tariff genuinely returns no standard AND no
            # day/night rates — the signature of the new time-of-use Intelligent
            # Octopus tariff (IOG-SMB-TOU — the 6-hour charge cap), which drops
            # `standard-unit-rates` for `day/night/ev_device_*` links EMT does not
            # yet read. FAIL LOUD — do not silently price at £0.
            unsupported = {"tariff": tariff, "product": product}
            logger.error(
                "_refresh_kraken_rate_schedules: import tariff %s returned NO "
                "standard unit rates. This is the new IOG time-of-use / 6-hour-"
                "cap tariff (day/night/ev_device rates), which EMT does NOT yet "
                "support. Rates are unavailable and billing for this meter will "
                "be incorrect until support lands. See issue #1708.", tariff)
    # Only update the banner when we actually determined the import status this
    # cycle. A transient failure leaves the previous flag exactly as it was.
    if import_determined:
        _rate_schedule_unsupported = unsupported
    if new_cache:
        _kraken_rate_schedules = new_cache
        logger.info("_refresh_kraken_rate_schedules: import=%d export=%d periods",
                    len(_kraken_rate_schedules.get("import", []) or []),
                    len(_kraken_rate_schedules.get("export", []) or []))

    # Standing charge comes from the import tariff.
    imp = _kraken_discovery.get("import") or {}
    if imp.get("product_code") and imp.get("tariff_code"):
        try:
            sc = await build_standing_charge_schedule(
                _kraken_client, imp["product_code"], imp["tariff_code"])
            if not sc.is_empty():
                _kraken_standing_schedule = sc
                logger.info("_refresh_kraken_rate_schedules: standing_charge=%d periods",
                            len(sc))
        except Exception as e:
            logger.warning("_refresh_kraken_rate_schedules: standing build failed: %s", e)


# Smart-charge source vocabulary across API versions: legacy meta.source =
# 'smart-charge', flex type = 'smart'. (Bump/boost are excluded — see filter.)
_SMART_CHARGE_SOURCES = {"smart-charge", "smart"}


def _smart_charge_slots(planned: list) -> set:
    """30-min slots (naive-UTC ISO) covered by planned dispatches whose
    source is a smart-charge dispatch. This is the started/smart gate: bump-charge
    (boost) and unknown/null sources are EXCLUDED — only genuine smart-charge
    dispatches are off-peak candidates. 'Any active minute → whole slot.'

    Vocabulary: the legacy `plannedDispatches.meta.source` says 'smart-charge';
    the newer `flexPlannedDispatches.type` says 'smart'. Match BOTH so detection
    is correct whichever the API returns (mirrors BottlecapDave's
    INTELLIGENT_SOURCE_SMART_CHARGE_OPTIONS).
    """
    smart = [d for d in (planned or [])
             if str(d.get("source") or "").lower() in _SMART_CHARGE_SOURCES]
    return _planned_dispatch_slots_preview(smart)


# ── OHME charge-mode interpretation + capture (verified-gating path) ──────────
# OHME is structurally unlike Zappi: Octopus does not control the charge, so its
# planned-dispatch `source` labels are unreliable (the planned set is a superset
# wiped ~17:00, present even when unplugged; completed dispatches report
# source=null). The Zappi gate `source=='smart-charge'` would therefore capture
# almost nothing for OHME and badly under-bill. OHME gets its OWN capture-feeding
# rule — the overlay, meter-draw validation and storage stay the shared path.
# Three ways:
#   • no charge-mode sensor → OPTIMISTIC: treat every planned-superset slot as an
#     off-peak candidate (source ohme_assumed_unverified); the overlay's meter
#     validation narrows it to slots that actually drew. Bounded to
#     under-application (a boost in a drawn slot is the accepted residual).
#   • charge-mode sensor    → VERIFIED: the sensor is real-time charge STATE,
#     independent of Octopus's planned data, so it both vetoes boosts (Max charge
#     stays peak) AND catches smart charges the superset missed. Sensor-driven,
#     per-tick: snap NOW to its 30-min slot and capture it iff the sensor reports
#     a smart slot active. 5-min ticks sample each 30-min slot ~6× so a running
#     smart charge is caught.
_OHME_PROVIDER_TOKEN = "OHME"


def _is_ohme_provider(provider) -> bool:
    return _OHME_PROVIDER_TOKEN in str(provider or "").upper()


def _ohme_interpret_mode(integration, state) -> str:
    """Map a live OHME charge-mode entity state to 'smart' | 'boost' | 'idle'.

    official select: state is the underscore slug — 'smart_charge'→smart,
                     'max_charge'→boost, else idle. Space/underscore are
                     normalised so the display form 'Smart charge' also resolves
                     (#286: the raw state is 'smart_charge', so matching only
                     'smart charge' left every tick idle → nothing off-peak).
    dan-r binary:    'on'→smart, else idle. dan-r cannot report boost directly,
                     so boost is left to inference-by-absence (an out-of-window
                     drawn slot with the binary off is never captured → stays
                     peak), per the agreed dan-r asymmetry.
    """
    sl = str(state or "").strip().lower().replace(" ", "_")
    if integration == "official":
        if sl == "max_charge":
            return "boost"
        if sl == "smart_charge":
            return "smart"
        return "idle"
    if integration == "danr":
        return "smart" if sl in ("on", "true", "active") else "idle"
    return "idle"


def _ohme_status_charging(status) -> bool:
    """True if the official OHME Status sensor reports the charger actively
    charging (ChargerStatus 'charging'). Any other state — paused, plugged_in,
    finished, unplugged — is not an active draw."""
    return str(status or "").strip().lower() == "charging"


def _ohme_slot_for_now(now) -> str:
    """Snap a naive-UTC datetime to its 30-min slot start (naive-UTC ISO),
    matching _planned_dispatch_slots_preview's slot keys."""
    slot = now.replace(minute=(0 if now.minute < 30 else 30),
                       second=0, microsecond=0)
    return slot.isoformat()


def _ohme_capture_slots(provider, planned, sensor_present, mode, status, now):
    """Decide which dispatch slots OHME should persist this tick. PURE — all I/O
    (sensor read, persistence, logging) stays in the caller.

    `status` is the raw OHME Status-sensor state (or None when no Status sensor is
    available). Returns a list of (slot_start_iso, source) pairs, or None when the
    provider is NOT OHME (signalling the caller to use the default path).
    """
    if not _is_ohme_provider(provider):
        return None
    if not sensor_present:
        # Optimistic: every planned-superset slot is an off-peak candidate.
        return [(s, "ohme_assumed_unverified")
                for s in sorted(_planned_dispatch_slots_preview(planned))]
    # Verified. Boost (Max charge) is never off-peak → veto; idle → nothing.
    if mode != "smart":
        return []
    # When a real Status sensor is present, require the charger to be ACTIVELY
    # charging this tick. This is precise (no capturing plugged-in-but-idle slots)
    # AND catches replanned/"additional" slots by construction — a drawn slot is
    # captured whenever status=charging, regardless of Octopus's planned data
    # (#286). With no Status sensor, fall back to mode-only; the overlay's
    # meter-draw validation then narrows to slots that actually drew.
    if status is not None and not _ohme_status_charging(status):
        return []
    return [(_ohme_slot_for_now(now), "ohme_verified")]


def _capture_ohme_slots(provider, planned) -> int:
    """OHME branch of _capture_dispatch_slots (the I/O side). Reads the detected
    charge-mode sensor (if any), decides slots via the pure _ohme_capture_slots
    helper, persists them, and logs richly — including the planned-source
    distribution — so an OHME user's log fully explains every decision. That log
    is the diagnostic backstop for a path we cannot validate on a Zappi account.
    """
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    ohme = (_detected_integrations or {}).get("ohme") or {}
    entity = ohme.get("charge_mode_entity") if ohme.get("found") else None
    sensor_present = bool(entity)
    mode = None
    raw = None
    status_raw = None
    if sensor_present and _engine_ha is not None:
        try:
            raw = _engine_ha.get_state(entity)
        except Exception as e:
            logger.warning("_capture_ohme_slots: sensor read failed (%s): %s",
                           entity, e)
            raw = None
        mode = _ohme_interpret_mode(ohme.get("integration"), raw)
        # Official Status sensor: the real charging-state gate (#286). Absent on
        # dan-r / older setups → status_raw stays None → mode-only fallback.
        status_entity = ohme.get("status_entity")
        if status_entity:
            try:
                status_raw = _engine_ha.get_state(status_entity)
            except Exception as e:
                logger.warning("_capture_ohme_slots: status read failed (%s): %s",
                               status_entity, e)
                status_raw = None

    pairs = _ohme_capture_slots(provider, planned, sensor_present, mode,
                                status_raw, now)
    if pairs is None:        # not OHME — caller already gated; defensive
        return 0

    # Diagnostic backstop: the ACTUAL planned-source distribution OHME reports,
    # logged every capture so the first OHME log reveals what their dispatches
    # really carry (smart-charge / bump-charge / unknown / null).
    try:
        from collections import Counter
        dist = dict(Counter(str(d.get("source")).lower() for d in (planned or [])))
    except Exception:
        dist = {}

    if not sensor_present:
        decision = "optimistic (no charge-mode sensor; all planned slots)"
    elif mode == "boost":
        decision = "verified-boost (veto — stays peak)"
    elif mode != "smart":
        decision = "verified-idle (mode not smart)"
    elif status_raw is not None and not _ohme_status_charging(status_raw):
        decision = "verified-smart but status not charging (skip)"
    else:
        decision = "verified-smart charging (capture current slot)"

    captured = 0
    for slot_start, source in pairs:
        try:
            _store.upsert_dispatch_slot(
                slot_start, off_peak=True, provider=provider, source=source,
                state="planned")
            captured += 1
        except Exception as e:
            logger.warning("_capture_ohme_slots: persist %s failed: %s",
                           slot_start, e)
    logger.info(
        "_capture_ohme_slots: provider=%s integration=%s mode=%s raw_mode=%r "
        "status=%r %s — captured %d slot(s) planned=%d source_dist=%s",
        provider, ohme.get("integration") or "none", mode or "n/a", raw,
        status_raw, decision, captured, len(planned or []), dist)
    try:
        _store.prune_dispatch_slots(days=90)
    except Exception:
        pass
    return captured


def _record_ohme_dispatch_history(provider, planned, completed) -> int:
    """BL-10: accumulate OHME planned + completed dispatches into dispatch_history.

    The main _capture_dispatch_slots path records this for Kraken-controlled
    providers, but OHME returns early (its dispatch_slots billing capture is
    sensor-gated). The smart-charging card reads dispatch_history and colours each
    slot by its BILLED RATE — not by the smart-vs-boost signal — so OHME's
    unreliable source labels don't matter here; we retain every planned slot
    (`include_all=True`) and every completed dispatch. OBSERVE-ONLY: this never
    touches dispatch_slots or billing. Returns rows written.
    """
    if _store is None:
        return 0
    n = 0
    slot_energy = _planned_dispatch_slot_energy(planned, include_all=True)
    slot_bounds = _planned_dispatch_slot_bounds(planned, include_all=True)
    for slot_start in sorted(slot_energy):
        rb = slot_bounds.get(slot_start) or (None, None)
        _store.record_dispatch_history(
            slot_start, "planned", provider=provider, source="ohme",
            energy_kwh=slot_energy.get(slot_start), raw_start=rb[0], raw_end=rb[1])
        n += 1
    for slot_start, e_comp in _completed_dispatch_slot_energy(completed).items():
        _store.record_dispatch_history(
            slot_start, "completed", provider=provider, source="unknown",
            energy_kwh=e_comp)
        n += 1
    return n


async def _capture_dispatch_slots() -> int:
    """STEP 2 (piece 1): persist smart-charge dispatch slots to dispatch_slots.

    Captures forward each poll. NON-BILLING: this only records which slots had a
    smart-charge dispatch; it does NOT change any rate. The overlay resolver +
    meter-draw validation (next piece) decides whether a recorded slot actually
    gets the off-peak rate. Returns the number of slots captured this poll.

    Provider handling: the DEFAULT path is capability-based (design §1c) — we
    persist smart-charge slots for ANY provider (incl. MYENERGI_V2) gated by the
    source filter, not a provider allowlist. OHME is the one exception: its
    source labels are unreliable, so it branches to _capture_ohme_slots (which
    captures optimistically, or sensor-verified when a charge-mode signal is
    present). See that function for the rationale.
    """
    if _kraken_client is None or not _kraken_discovery or _store is None:
        return 0
    try:
        disp = await _kraken_client.get_dispatches(_kraken_account_number)
    except Exception as e:
        logger.warning("_capture_dispatch_slots: fetch failed: %s", e)
        return 0
    if not disp:
        return 0
    provider = disp.get("provider")
    planned = disp.get("planned") or []

    # OHME has its own capture-feeding rule (see _capture_ohme_slots): the Zappi
    # source=='smart-charge' gate would under-capture OHME badly.
    if _is_ohme_provider(provider):
        captured = _capture_ohme_slots(provider, planned)
        # BL-10: also accumulate observe-only dispatch_history so the smart-
        # charging card covers OHME (the billing dispatch_slots path is untouched).
        try:
            _record_ohme_dispatch_history(provider, planned, disp.get("completed") or [])
        except Exception as e:
            logger.warning("_capture_dispatch_slots: ohme history accumulation "
                           "failed: %s", e)
        return captured

    slots = _smart_charge_slots(planned)
    # NB: do NOT early-return when planned is empty. Completed dispatches land
    # AFTER the charge, by which point flexPlannedDispatches is already empty
    # (planned=0) — an early return here skipped the completed capture entirely.
    slot_energy = _planned_dispatch_slot_energy(planned) if slots else {}
    slot_bounds = _planned_dispatch_slot_bounds(planned) if slots else {}  # BL-11
    captured = 0
    for slot_start in sorted(slots):
        _rb = slot_bounds.get(slot_start) or (None, None)
        try:
            _store.upsert_dispatch_slot(
                slot_start, off_peak=True, provider=provider,
                source="smart-charge", state="planned",
                energy_planned=slot_energy.get(slot_start),
                raw_start=_rb[0], raw_end=_rb[1])
            captured += 1
        except Exception as e:
            logger.warning("_capture_dispatch_slots: persist %s failed: %s",
                           slot_start, e)
    if captured:
        logger.info("_capture_dispatch_slots: persisted %d smart-charge slot(s) "
                    "(provider=%s)", captured, provider)

    # Record COMPLETED dispatch energy per slot (observe-only). Completed
    # dispatches land hours after the charge, so this is the settlement-time
    # signal the #253 validation will veto against — NOT used for billing yet.
    # OHME already returned above (its completed dispatches carry no reliable
    # smart-vs-boost signal), so this covers only Octopus-controlled providers.
    # We only ANNOTATE slots that already exist (were captured as planned) — we
    # never create a new off_peak slot from a completed dispatch, so this can't
    # change what the overlay prices. off_peak / provider / source are preserved.
    completed = disp.get("completed") or []
    if completed:
        comp = _completed_dispatch_slot_energy(completed)
        n_comp = 0
        for slot_start, e_comp in comp.items():
            if e_comp is None:
                continue
            existing = _store.get_dispatch_slot(slot_start)
            if not existing:
                continue
            try:
                _store.upsert_dispatch_slot(
                    slot_start,
                    off_peak=bool(existing.get("off_peak")),
                    provider=existing.get("provider") or provider,
                    source=existing.get("source") or "smart-charge",
                    state="completed", energy_completed=e_comp)
                n_comp += 1
            except Exception as e:
                logger.warning("_capture_dispatch_slots: completed persist %s "
                               "failed: %s", slot_start, e)
        if n_comp:
            logger.info("_capture_dispatch_slots: recorded completed energy for "
                        "%d slot(s) (observe-only)", n_comp)

    # Accumulate ALL planned + completed dispatches into dispatch_history
    # (observe-only, design §11) — a persistent record across polls, separate
    # from the billing dispatch_slots. This retains completed dispatches for
    # slots we never planned (the tail slots the per-poll snapshot loses) and is
    # what makes a future "never saw this slot" judgement trustworthy. Never read
    # for billing.
    try:
        for slot_start in sorted(slots):
            _rb = slot_bounds.get(slot_start) or (None, None)
            _store.record_dispatch_history(
                slot_start, "planned", provider=provider,
                source="smart-charge", energy_kwh=slot_energy.get(slot_start),
                raw_start=_rb[0], raw_end=_rb[1])
        for slot_start, e_comp in _completed_dispatch_slot_energy(completed).items():
            _store.record_dispatch_history(
                slot_start, "completed", provider=provider,
                source="unknown", energy_kwh=e_comp)
    except Exception as e:
        logger.warning("_capture_dispatch_slots: history accumulation failed: %s", e)

    # Derive `started` for the current slot (design §11.2) — the smart-vs-bump
    # discriminator. Only fires when the intelligent state is
    # SMART_CONTROL_IN_PROGRESS AND a planned dispatch is active now; a bump never
    # enters that state, so it never starts. Best-effort + observe-only: the
    # state fetch is isolated (None on any error → no started slot), and started
    # is recorded only into dispatch_history, never the billing dispatch_slots.
    try:
        state = await _kraken_client.get_intelligent_state(_kraken_account_number)
        if state:
            now = datetime.now(timezone.utc).replace(tzinfo=None)
            for slot_start in _derive_started_slots(now, planned, state):
                _store.record_dispatch_history(
                    slot_start, "started", provider=provider,
                    source="smart-charge", energy_kwh=None)
                logger.info("_capture_dispatch_slots: slot %s STARTED "
                            "(intelligent_state=%s)", slot_start, state)
    except Exception as e:
        logger.warning("_capture_dispatch_slots: started derivation failed: %s", e)

    try:
        _store.prune_dispatch_slots(days=90)
        _store.prune_dispatch_history(days=90)
    except Exception:
        pass
    return captured


async def _tick_dispatch_capture() -> None:
    """5-minute-throttled dispatch-slot capture. Called from _engine_tick (every
    ~10s) but only fetches every 5 minutes.

    Rationale (confirmed against live data + BCD): a smart-charge dispatch only
    carries source='smart-charge' while it is PLANNED/upcoming; once actioned it
    moves to completed with source='unknown', losing the smart signal. The
    6-hour DCC poll is far too coarse to catch a short daytime dispatch while
    still planned. BCD refreshes ~every 60s; we use 5 min — frequent enough to
    catch the planned state with margin (Octopus schedules ahead), light enough
    for an add-on also doing DCC polls + CI ticks. Captured slots persist in
    dispatch_slots for the overlay to price later (gated by meter draw).
    """
    global _last_dispatch_capture
    if _kraken_client is None or not _kraken_discovery:
        return
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    elapsed = ((now - _last_dispatch_capture).total_seconds()
               if _last_dispatch_capture else None)
    if elapsed is not None and elapsed < 300:  # 5 minutes
        return
    _last_dispatch_capture = now
    try:
        await _capture_dispatch_slots()
    except Exception as e:
        logger.warning("_tick_dispatch_capture: failed: %s", e)

    # Settlement-time reconciliation (design §12) — hourly, not every 5 min. By
    # now `started` (real-time) and `completed` (~hours) have settled for older
    # slots, so we can reprice off-peak/peak from the lifecycle instead of the
    # meter. Runs on this loop thread (single store connection).
    global _last_reconcile
    r_elapsed = ((now - _last_reconcile).total_seconds()
                 if _last_reconcile else None)
    if r_elapsed is None or r_elapsed >= 3600:
        _last_reconcile = now
        try:
            await reconcile_dispatch_overlay()
        except Exception as e:
            logger.warning("_tick_dispatch_capture: reconcile failed: %s", e)


async def _log_dispatches_observe_only() -> None:
    """STEP 1 of the dispatch overlay: fetch dispatches and LOG them only.

    No rate changes, no persistence — pure observation. Lets us confirm the
    live dispatch shape (provider, planned/completed slots, meta.source) against
    the real account before building the started-dispatch overlay (step 2+).

    Logs: provider; counts; the first few planned and completed slots; and which
    30-min slots the planned dispatches would map to (the started-slot preview).
    """
    if _kraken_client is None or not _kraken_discovery:
        return
    disp = await _kraken_client.get_dispatches(_kraken_account_number)
    if disp is None:
        logger.info("dispatch-observe: no dispatch data available")
        return
    provider = disp.get("provider")
    planned = disp.get("planned") or []
    completed = disp.get("completed") or []
    logger.info("dispatch-observe: provider=%s planned=%d completed=%d",
                provider, len(planned), len(completed))
    for d in planned[:5]:
        logger.info("dispatch-observe:   planned   start=%s end=%s source=%s",
                    d.get("start"), d.get("end"), d.get("source"))
    for d in completed[:5]:
        logger.info("dispatch-observe:   completed start=%s end=%s delta=%s source=%s",
                    d.get("start"), d.get("end"), d.get("delta"), d.get("source"))
    # Preview the 30-min slots the PLANNED dispatches would map to (started-slot
    # snap). This is observation only — nothing is persisted or priced.
    try:
        slots = _planned_dispatch_slots_preview(planned)
        if slots:
            logger.info("dispatch-observe:   would-snap to %d off-peak 30-min "
                        "slot(s): %s", len(slots), sorted(slots)[:8])
    except Exception as e:
        logger.warning("dispatch-observe: slot preview failed: %s", e)


def _planned_dispatch_slots_preview(planned: list) -> set:
    """Map planned dispatches to the 30-min slots they cover (naive-UTC ISO).

    Pure helper, no side effects. A dispatch covering any part of a 30-min slot
    maps the whole slot (matches BCD's 'any active minute counts' rule). Used by
    the observe-only step now; the real overlay (step 2+) will gate these on the
    intelligent state being active and validate against meter draw.
    """
    slots: set = set()
    for d in planned or []:
        start = d.get("start")
        end = d.get("end")
        if not start or not end:
            continue
        try:
            s = datetime.fromisoformat(str(start).replace("Z", "+00:00"))
            e = datetime.fromisoformat(str(end).replace("Z", "+00:00"))
        except Exception:
            continue
        # Normalise to naive UTC
        if s.tzinfo is not None:
            s = s.astimezone(timezone.utc).replace(tzinfo=None)
        if e.tzinfo is not None:
            e = e.astimezone(timezone.utc).replace(tzinfo=None)
        # Snap start down to the 30-min slot, then step in 30-min increments
        slot = s.replace(minute=(0 if s.minute < 30 else 30),
                         second=0, microsecond=0)
        while slot < e:
            slots.add(slot.isoformat())
            slot = slot + timedelta(minutes=30)
    return slots


def _planned_dispatch_slot_energy(planned: list, include_all: bool = False) -> dict:
    """Map smart-charge planned dispatches to per-slot FORECAST energy (kWh).

    Companion to _planned_dispatch_slots_preview: distributes each dispatch's
    energyAddedKwh (normalised to `delta`) evenly across the 30-min slots it
    covers, so we can persist energy_planned per slot. OBSERVE-ONLY — recorded
    for diagnostics / future validation, NOT used for billing (see
    dispatch_validation_design.md). Slots whose dispatch has no delta map to None.

    `include_all` skips the smart-charge source filter — used by the OHME history
    path, whose source labels are unreliable so every planned slot is retained.
    """
    out: dict = {}
    for d in planned or []:
        if (not include_all
                and str(d.get("source") or "").lower() not in _SMART_CHARGE_SOURCES):
            continue
        start = d.get("start"); end = d.get("end")
        if not start or not end:
            continue
        try:
            s = datetime.fromisoformat(str(start).replace("Z", "+00:00"))
            e = datetime.fromisoformat(str(end).replace("Z", "+00:00"))
        except Exception:
            continue
        if s.tzinfo is not None:
            s = s.astimezone(timezone.utc).replace(tzinfo=None)
        if e.tzinfo is not None:
            e = e.astimezone(timezone.utc).replace(tzinfo=None)
        covered = []
        slot = s.replace(minute=(0 if s.minute < 30 else 30),
                         second=0, microsecond=0)
        while slot < e:
            covered.append(slot.isoformat())
            slot = slot + timedelta(minutes=30)
        if not covered:
            continue
        try:
            per = (float(d.get("delta")) / len(covered)) \
                if d.get("delta") is not None else None
        except (TypeError, ValueError):
            per = None
        for k in covered:
            if per is None:
                out.setdefault(k, None)
            else:
                out[k] = round((out.get(k) or 0.0) + per, 4)
    return out


def _planned_dispatch_slot_bounds(planned: list, include_all: bool = False) -> dict:
    """Map smart-charge planned dispatches to the EXACT (second-precision) raw
    window each slot belongs to (BL-11).

    The API's start/end are precise to the second, but capture snaps energy to
    30-min slots and discards the exact bounds. Retain them so a charge-session
    view (BL-10) can show the true scheduled window: every 30-min slot a dispatch
    covers carries that dispatch's (start, end) as naive-UTC ISO, so a session's
    exact window is min(raw_start) / max(raw_end) over its slots. Mirrors the
    slotting of _planned_dispatch_slot_energy exactly. OBSERVE-ONLY.
    Returns {slot_start_iso: (raw_start_iso, raw_end_iso)}.

    `include_all` skips the smart-charge source filter (OHME history path).
    """
    out: dict = {}
    for d in planned or []:
        if (not include_all
                and str(d.get("source") or "").lower() not in _SMART_CHARGE_SOURCES):
            continue
        start = d.get("start"); end = d.get("end")
        if not start or not end:
            continue
        try:
            s = datetime.fromisoformat(str(start).replace("Z", "+00:00"))
            e = datetime.fromisoformat(str(end).replace("Z", "+00:00"))
        except Exception:
            continue
        if s.tzinfo is not None:
            s = s.astimezone(timezone.utc).replace(tzinfo=None)
        if e.tzinfo is not None:
            e = e.astimezone(timezone.utc).replace(tzinfo=None)
        raw = (s.isoformat(), e.isoformat())
        slot = s.replace(minute=(0 if s.minute < 30 else 30),
                         second=0, microsecond=0)
        while slot < e:
            out[slot.isoformat()] = raw   # last dispatch wins on the rare overlap
            slot = slot + timedelta(minutes=30)
    return out


def _completed_dispatch_slot_energy(completed: list) -> dict:
    """Map COMPLETED dispatches to per-slot delivered energy (kWh).

    Like _planned_dispatch_slot_energy, but for completedDispatches — what
    Octopus ACTUALLY dispatched, which land hours after the charge. No source
    filter: completed dispatches come back source='unknown' (the smart-vs-bump
    label is lost on completion), so we key on the dispatch itself, not a label.
    Signed like the planned figure (a charge is negative kWh). OBSERVE-ONLY —
    recorded to energy_completed for the future settlement-time validation
    (see dispatch_validation_design.md); NOT used for billing yet.
    """
    out: dict = {}
    for d in completed or []:
        start = d.get("start"); end = d.get("end")
        if not start or not end:
            continue
        try:
            s = datetime.fromisoformat(str(start).replace("Z", "+00:00"))
            e = datetime.fromisoformat(str(end).replace("Z", "+00:00"))
        except Exception:
            continue
        if s.tzinfo is not None:
            s = s.astimezone(timezone.utc).replace(tzinfo=None)
        if e.tzinfo is not None:
            e = e.astimezone(timezone.utc).replace(tzinfo=None)
        covered = []
        slot = s.replace(minute=(0 if s.minute < 30 else 30),
                         second=0, microsecond=0)
        while slot < e:
            covered.append(slot.isoformat())
            slot = slot + timedelta(minutes=30)
        if not covered:
            continue
        try:
            per = (float(d.get("delta")) / len(covered)) \
                if d.get("delta") is not None else None
        except (TypeError, ValueError):
            per = None
        for k in covered:
            if per is None:
                out.setdefault(k, None)
            else:
                out[k] = round((out.get(k) or 0.0) + per, 4)
    return out


def _derive_started_slots(now_utc: datetime, planned: list,
                          intelligent_state: str | None) -> list:
    """Derive `started` 30-minute slots (design §11.2 — BCD's mechanism).

    The CURRENT slot becomes 'started' ONLY when the intelligent state is
    `SMART_CONTROL_IN_PROGRESS` AND a planned dispatch is active right now (and
    therefore still present in the list — planned can be pulled last-minute).
    Done one 30-min slot at a time; accumulation across polls builds the full
    set. A bump never enters `SMART_CONTROL_IN_PROGRESS`, so it never becomes
    started — which is exactly why `started` discriminates smart from bump where
    `completed` can't. Returns 0 or 1 slot_start ISO strings (naive UTC).
    """
    if intelligent_state != "SMART_CONTROL_IN_PROGRESS":
        return []
    now = now_utc
    if now.tzinfo is not None:
        now = now.astimezone(timezone.utc).replace(tzinfo=None)
    active = False
    for d in planned or []:
        s, e = d.get("start"), d.get("end")
        if not s or not e:
            continue
        try:
            sd = datetime.fromisoformat(str(s).replace("Z", "+00:00"))
            ed = datetime.fromisoformat(str(e).replace("Z", "+00:00"))
        except Exception:
            continue
        if sd.tzinfo is not None:
            sd = sd.astimezone(timezone.utc).replace(tzinfo=None)
        if ed.tzinfo is not None:
            ed = ed.astimezone(timezone.utc).replace(tzinfo=None)
        if sd <= now < ed:
            active = True
            break
    if not active:
        return []
    slot = now.replace(minute=(0 if now.minute < 30 else 30),
                       second=0, microsecond=0)
    return [slot.isoformat()]


# A completed-without-started dispatch below this magnitude didn't materially run:
# too small to be a bump (which draws hard) OR a real smart charge, so it's peak.
# Keyed on Octopus's completed ENERGY, not the meter, so it stays solar-safe.
_RECONCILE_SMALL_COMPLETED_KWH = 0.4


def _reconcile_decision(has_started: bool, has_completed: bool,
                        completed_energy, currently_off_peak: bool,
                        small_kwh: float = _RECONCILE_SMALL_COMPLETED_KWH) -> tuple:
    """Settlement-time reconciliation of the dispatch overlay against the
    accumulated lifecycle (design §12). Keys on whether the slot STARTED
    (SMART_CONTROL_IN_PROGRESS + planned active) — the validated smart-vs-bump,
    solar-immune signal — and, for the completed-without-started case, on the
    completed ENERGY magnitude (also solar-safe; never the meter).

    Returns (target, reason) where target is one of:
      - 'off_peak' : started present → genuine smart charge. Restores off-peak,
                     including solar slots the meter floor wrongly rejected.
      - 'peak'     : (a) neither started nor completed → planned but never charged;
                     or (b) completed-without-started with NEGLIGIBLE energy
                     (|completed| < small_kwh) — the dispatch didn't materially
                     run, so it can be neither a bump nor a real charge, hence
                     peak (fixes the 10-Jul over-credit).
      - 'review'   : completed-without-started with SUBSTANTIAL energy — still
                     genuinely ambiguous (a missed-poll smart charge OR a bump).
                     Price left unchanged and the block flagged for the user to
                     check against the real bill.
      - 'ok'       : the correct target already matches the current rate.
    Only 'off_peak' and 'peak' are actionable; 'review' flags, 'ok' writes nothing.
    """
    if has_started:
        target = "off_peak"
        reason = "started → off-peak (restore)"
    elif not has_completed:
        target = "peak"
        reason = "never started or completed → peak (revert)"
    else:
        mag = abs(completed_energy) if completed_energy is not None else 0.0
        if mag < small_kwh:
            target = "peak"
            reason = (f"completed but not started, negligible energy "
                      f"({mag:.2f} kWh < {small_kwh} kWh) → peak (did not run)")
        else:
            return ("review",
                    f"completed ({mag:.2f} kWh) but not started — ambiguous "
                    "(missed-poll smart or bump)")
    if (target == "off_peak" and currently_off_peak) or \
       (target == "peak" and not currently_off_peak):
        return ("ok", "already correct")
    return (target, reason)


_RECONCILE_SETTLE_HOURS = 6.0     # revert/review: wait for completed (~hrs) to settle
_RECONCILE_STARTED_GATE_MIN = 40  # restore: slot ended + a poll margin (started is real-time)
_DISPATCH_RECONCILE_APPLY = True  # write (risk accepted; soaked on prod_dev first)


async def reconcile_dispatch_overlay() -> dict:
    """Settlement-time reconciliation of the dispatch overlay against the
    accumulated lifecycle (design §12). For each SETTLED, non-user-corrected main
    block whose slot was a planned smart-charge candidate, decide off-peak/peak
    from `started` / `completed` — never the meter — and reprice if it disagrees
    with the current rate. Bidirectional: RESTORES solar slots the meter floor
    wrongly peaked, REVERTS planned-but-never-charged slots (and completed-without-
    started slots whose energy is too small to have materially run). A completed-
    without-started slot with SUBSTANTIAL energy stays genuinely ambiguous — its
    price is left unchanged and logged (a UI to surface these for review is planned
    for 3.3.0). Devices follow the main rate; blocks the user manually corrected
    are skipped. Runs on the engine loop (single store connection); regenerates
    charts if anything changed.
    """
    store = _store
    if store is None:
        return {}
    sched = _kraken_rate_schedules.get("import")
    if sched is None or sched.is_empty():
        return {}
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    # Two gates: a RESTORE can happen as soon as the slot has ended (`started` is
    # captured in real-time, so it's already known); a REVERT/REVIEW must wait for
    # `completed` (~hrs) to settle, else we can't tell "neither → peak" from
    # "completed-but-not-started → review".
    started_cutoff = (now - timedelta(minutes=_RECONCILE_STARTED_GATE_MIN)).isoformat()
    settle_cutoff = (now - timedelta(hours=_RECONCILE_SETTLE_HOURS)).isoformat()
    try:
        rows = store._conn.execute(
            """SELECT b.block_start, b.imp_kwh, b.imp_rate, b.needs_review,
                      b.imp_kwh_api
               FROM blocks b
               WHERE b.meter_id = 'electricity_main' AND b.rate_corrected = 0
                 -- NEVER reconcile IMPORTED history. Imported blocks carry Octopus's
                 -- ACTUAL BILLED, dispatch-aware rate (the ground truth) — the local
                 -- lifecycle heuristic ("we never saw it start → it didn't charge")
                 -- is invalid for history EMT wasn't running for, and would revert a
                 -- genuinely-off-peak charge to peak overnight, silently un-fixing an
                 -- import (the 'October regressed on its own' bug).
                 AND (b.source IS NULL OR b.source NOT LIKE 'imported%')
                 AND b.block_start < ?
                 AND EXISTS (SELECT 1 FROM dispatch_slots s
                             WHERE s.slot_start = b.block_start AND s.off_peak = 1)
                 AND EXISTS (SELECT 1 FROM dispatch_history h
                             WHERE h.slot_start = b.block_start AND h.kind = 'planned')
               ORDER BY b.block_start""", (started_cutoff,)).fetchall()
    except Exception as e:
        logger.warning("reconcile_dispatch_overlay: query failed: %s", e)
        return {}
    n_restore = n_revert = n_review = n_deferred = 0
    changed = False
    n_candidates = len(rows)
    for r in rows:
        bs = r["block_start"]
        drows = store._conn.execute(
            "SELECT kind, energy_kwh FROM dispatch_history WHERE slot_start = ?",
            (bs,)).fetchall()
        kinds = {x[0] for x in drows}
        # Octopus's completed energy for the slot (negative = charged); drives the
        # negligible-completion → peak decision. None if no completed record yet.
        completed_energy = next(
            (x[1] for x in drows if x[0] == "completed" and x[1] is not None), None)
        has_started = "started" in kinds
        # A no-started slot's fate (revert vs review) hinges on completed, which
        # lands late — defer it until it clears the settle gate. A started slot is
        # decidable now.
        if not has_started and bs >= settle_cutoff:
            n_deferred += 1
            continue
        op_pence = sched.off_peak_rate_near(bs)
        base_pence = sched.resolve(bs)
        if op_pence is None or base_pence is None:
            continue
        off_peak = round(op_pence / 100.0, 6)
        base = round(base_pence / 100.0, 6)
        # Inside the off-peak WINDOW the base tariff is already the off-peak rate,
        # so the block's price does not depend on any dispatch — there is nothing
        # to reconcile (a smart charge here, e.g. under the 6-hour cap, is priced
        # off-peak by the schedule regardless of whether it 'started'). Reverting
        # would be a no-op and flagging it for review is a false positive. Skip,
        # clearing any stale review flag a prior run may have set.
        if abs(base - off_peak) < 1e-6:
            if r["needs_review"]:
                store.clear_block_review(bs)
            continue
        cur_rate = r["imp_rate"] or 0.0
        currently_off_peak = abs(cur_rate - off_peak) < 1e-6
        target, reason = _reconcile_decision(
            has_started, "completed" in kinds, completed_energy, currently_off_peak)
        if target == "review":
            # A review flag invites a manual correction, but the correction tool
            # can only touch DCC-settled blocks. Dispatch `completed` arrives at
            # T+hours while DCC settlement is T+1/T+2, so a block can be decidable
            # here yet not correctable. Defer the flag until the block is actually
            # settled (imp_kwh_api present), so the review list only ever shows
            # blocks the user can act on. (The automatic revert path needs no user
            # action, so it is not gated this way.)
            if r["imp_kwh_api"] is None:
                n_deferred += 1
                # Retract any flag a prior run set before this gate existed — the
                # block isn't ready for review until it settles.
                if r["needs_review"]:
                    store.clear_block_review(bs)
                continue
            n_review += 1
            # Genuinely ambiguous — price left unchanged, but flag it so the UI
            # review list (BL-18) can surface it for a human decision. Idempotent:
            # re-flagging just refreshes the reason.
            store.flag_block_for_review(bs, reason)
            logger.info("reconcile: %s REVIEW — %s (flagged, price unchanged)", bs, reason)
            continue
        if target == "ok":
            # Already correct. If it was previously flagged ambiguous and has since
            # become decidable, clear the stale review flag.
            if r["needs_review"]:
                store.clear_block_review(bs)
            continue
        new_rate = off_peak if target == "off_peak" else base
        if not _DISPATCH_RECONCILE_APPLY:
            logger.info("reconcile: WOULD %s %s %.5f→%.5f (%s)",
                        target, bs, cur_rate, new_rate, reason)
            continue
        with store._conn:
            # A definite verdict also clears any prior ambiguous-review flag on
            # the main row (needs_review / review_reason).
            store._conn.execute(
                "UPDATE blocks SET imp_rate = ?, rate_reconciled = 1, "
                "needs_review = 0, review_reason = NULL, "
                "imp_cost = ROUND(COALESCE(imp_kwh, 0) * ?, 6) "
                "WHERE block_start = ? AND meter_id = 'electricity_main'",
                (new_rate, new_rate, bs))
            store._conn.execute(
                "UPDATE blocks SET imp_rate = ?, rate_reconciled = 1, "
                "imp_cost = ROUND(COALESCE(imp_kwh_grid, imp_kwh, 0) * ?, 6) "
                "WHERE block_start = ? AND rate_corrected = 0 AND meter_id IN "
                "(SELECT meter_id FROM meters WHERE is_sub_meter = 1)",
                (new_rate, new_rate, bs))
        changed = True
        if target == "off_peak":
            n_restore += 1
        else:
            n_revert += 1
        logger.info("reconcile: %s %s %.5f→%.5f (%s)",
                    target, bs, cur_rate, new_rate, reason)
    if changed:
        try:
            generate_charts(store)
        except Exception as e:
            logger.warning("reconcile_dispatch_overlay: chart regen failed: %s", e)
    # Heartbeat every run (even with no changes) so the pass is observable in the
    # log — "0 candidates" vs "N candidates, 0 changed" tells very different
    # stories when a block isn't being corrected.
    logger.info("reconcile: scanned %d candidate(s) — %d restored, %d reverted, "
                "%d review, %d deferred (settle=%dh)",
                n_candidates, n_restore, n_revert, n_review, n_deferred,
                int(_RECONCILE_SETTLE_HOURS))
    return {"restored": n_restore, "reverted": n_revert, "review": n_review,
            "deferred": n_deferred}


async def resolve_history_gaps(now=None, meter_id: str = "electricity_main",
                               max_runs: int = 60) -> dict:
    """User-triggered sweep: find gaps in the block history and backfill them.

    The DCC poll's window is a forward-moving cursor, so an outage the cursor has
    already passed is never revisited — the hole is permanent. This asks the DB
    where the holes are, then re-polls each gap window explicitly
    (`advance_cursor=False`, so the normal cursor is untouched). BL-8's
    `create_backfill_block` materialises the missing blocks from the settled
    figures; PASS 2 then prices them.

    Deliberately **explicit, not automatic**: we cannot distinguish an outage gap
    from blocks the user deleted on purpose (Data Management → Delete Blocks), and
    an automatic sweep would silently resurrect them.
    """
    if not kraken_available():
        return {"ok": False, "reason": "no_api"}
    ingester = _build_kraken_ingester()
    if ingester is None:
        return {"ok": False, "reason": "no_ingester"}
    if _store is None:
        return {"ok": False, "reason": "no_store"}

    gaps = _store.find_block_gaps(meter_id, now=now)
    if not gaps:
        return {"ok": True, "gaps": 0, "backfilled": 0, "runs": []}

    total_backfilled = 0
    runs_done = []
    step = timedelta(minutes=30)
    for g in gaps[:max_runs]:
        # widen by one slot each side so boundary intervals are included
        pf = (datetime.fromisoformat(g["start"]) - step)
        pt = (datetime.fromisoformat(g["end"]) + step * 2)
        window = (pf.isoformat(timespec="seconds") + "Z",
                  pt.isoformat(timespec="seconds") + "Z")
        try:
            summary = await ingester.poll(window=window, advance_cursor=False)
        except Exception as e:
            logger.warning("resolve_history_gaps: window %s failed: %s", window, e)
            continue
        made = summary.get("backfilled", 0)
        total_backfilled += made
        runs_done.append({"start": g["start"], "end": g["end"],
                          "slots": g["slots"], "backfilled": made})
        logger.info("resolve_history_gaps: %s..%s (%d slots) -> backfilled %d",
                    g["start"], g["end"], g["slots"], made)

    if total_backfilled:
        # We are already on the engine loop (called via _run_on_engine_loop), so
        # regen inline on this thread — never from the Flask thread, which would
        # use the engine's SQLite connection cross-thread (the 3.1.3 segfault).
        try:
            generate_charts(_store)
        except Exception as e:
            logger.warning("resolve_history_gaps: chart regen failed: %s", e)
    logger.info("resolve_history_gaps: %d gap run(s), %d block(s) backfilled",
                len(gaps), total_backfilled)
    return {"ok": True, "gaps": len(gaps), "backfilled": total_backfilled,
            "runs": runs_done}


async def kraken_poll_task(ha: HAClient):
    """Periodic DCC ingest loop. Runs poll() every 6h.

    4b-iii: LIVE — poll() now writes settlement. Before the FIRST live write
    (once, ever, gated on a kraken_state marker), a full blocks.db snapshot is
    taken as a rollback point. dry_run is False; the flip was a deliberate step.

    No-op (returns immediately) when no API key / discovery / ingester — so it
    is always safe to schedule.
    """
    if _kraken_client is None or not _kraken_discovery:
        logger.info("kraken_poll_task: API not configured — task idle")
        return
    ingester = _build_kraken_ingester()
    if ingester is None:
        logger.info("kraken_poll_task: ingester unavailable — task idle")
        return

    logger.info("kraken_poll_task: started (interval=%ds, dry_run=%s, "
                "backfill_days=%d)", _KRAKEN_POLL_INTERVAL_S, _KRAKEN_DRY_RUN,
                ingester.backfill_days)
    while True:
        try:
            # Chunk 5: refresh rate schedules once per cycle (cheap — fixed
            # tariffs return ~1 record). The sync drain reads these via
            # _kraken_rate_resolver to repair zero/missing rates at reconcile.
            await _refresh_kraken_rate_schedules()
            # Before the first LIVE write, ensure the rollback snapshot exists.
            # If it can't be taken, downgrade THIS cycle to dry-run rather than
            # mutate blocks without a backup.
            effective_dry_run = _KRAKEN_DRY_RUN
            if not _KRAKEN_DRY_RUN:
                if not _kraken_pre_live_snapshot():
                    logger.warning("kraken_poll_task: snapshot unavailable — "
                                   "running this cycle as dry-run (no writes)")
                    effective_dry_run = True
            summary = await ingester.poll(dry_run=effective_dry_run)
            logger.info(
                "kraken_poll_task: %s window=%s..%s import_rows=%d "
                "store_import=%d export_rows=%d store_export=%d "
                "skip=%d flag_review=%d (interpolated=%d) errors=%d",
                "DRY-RUN" if effective_dry_run else "LIVE",
                summary["window"][0], summary["window"][1],
                summary["import_rows"], summary["stored"],
                summary["export_rows"], summary["export_stored"],
                summary["skipped_no_block"], summary["flagged_review"],
                summary.get("flagged_interpolated", 0),
                len(summary["errors"]))
            for err in summary["errors"][:5]:
                logger.warning("kraken_poll_task: %s", err)
            # Label the provisional figure by its actual source: the Mini
            # supplies it whenever the runtime Mini reader is active (mode=api /
            # api+mini with no local sensor); otherwise it's the local CAD read.
            _prov_label = "Mini" if _kraken_mini_reader is not None else "CAD"
            for smp in summary.get("review_samples", []):
                logger.info(
                    "kraken_poll_task:   review-sample [%s]%s %s %s=%s DCC=%s drift=%s%%",
                    smp["channel"], " INTERP" if smp.get("interpolated") else "",
                    smp["block_start"], _prov_label, smp["cad_kwh"], smp["dcc_kwh"],
                    ("%.1f" % smp["drift_pct"]) if smp["drift_pct"] is not None
                    else "n/a")
            # Dispatch overlay — STEP 1: OBSERVE-ONLY. Fetch and log Intelligent
            # dispatches; make NO rate changes. This surfaces the real dispatch
            # shape (provider, planned vs completed, meta) against the live
            # account so the overlay (step 2+) can be built on confirmed data.
            # Wrapped independently so a dispatch-fetch failure never breaks the
            # DCC poll.
            try:
                await _log_dispatches_observe_only()
            except Exception as de:
                logger.warning("kraken_poll_task: dispatch observe failed: %s", de)
            # Daily sweep for unsettled gaps that aged out of the sliding window
            # (late/out-of-order DCC settlement). Runs on the first poll after
            # startup too, so downtime gaps catch up. Self-gated + non-fatal.
            await _maybe_run_settlement_sweep()
        except Exception as e:
            logger.error("kraken_poll_task: poll failed: %s", e)
        await asyncio.sleep(_KRAKEN_POLL_INTERVAL_S)


def _clear_stale_inprogress_reads(cb_for_gap, cold_start: bool) -> bool:
    """Rogue-total guard: on a COLD start, clear the in-progress block's carried
    reads/rates so a stale prior-process register isn't taken as the block opener
    (reads[0] becomes the opener and the block's kWh = reads[-1] - reads[0] off a
    previous-session value — a "rogue total").

    Cold-start ONLY. On a warm re-run (HA reconnect / config-save within the same
    process) the in-progress reads are LIVE, not stale, so clearing them discards
    real consumption — e.g. a Mini block losing its ~3.4 kWh opener when an HA
    upgrade triggered repeated engine_startup re-runs. Returns True if it cleared
    anything (caller persists + logs). Mutates cb_for_gap in place.
    """
    if not cold_start:
        return False
    if not (cb_for_gap and cb_for_gap.get("start")):
        return False
    cleared = False
    for _md in (cb_for_gap.get("meters") or {}).values():
        for _ch in (_md.get("channels") or {}).values():
            if _ch.get("reads") or _ch.get("rates"):
                _ch["reads"] = []
                _ch["rates"] = []
                cleared = True
    return cleared


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
    global _store, _cold_start_complete
    # Cancel any in-flight DCC poll BEFORE we tear down the store and Kraken
    # client below. A long backfill reading the DB / HTTP session while we close
    # them throws "Cannot operate on a closed database" / "Connector is closed".
    # The poll is relaunched at the end of startup once resources are rebuilt.
    _cancel_kraken_poll_task()
    # CRITICAL: engine_startup runs again on every config save. Reopening the
    # store without closing the previous connection leaves an orphaned WAL
    # connection that holds locks indefinitely — the engine loop's writes then
    # fail every tick with "database is locked" (busy_timeout can't help: the
    # orphaned connection never releases). Close it first.
    if _store is not None:
        try:
            _store.close()
        except Exception as _se:
            logger.warning("engine_startup: closing prior store failed: %s", _se)
        _store = None
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

    # BL-5: resolve this instance's backup directory from the site name. In
    # supervised mode /share is shared across add-ons, so the path is namespaced
    # by site slug; standalone is volume-scoped and unaffected. engine_startup
    # re-runs on every config save, so a site rename is picked up here and the
    # directory is migrated/adopted silently (see instance.py).
    try:
        _site = None
        if _store is not None:
            _pid = _store.get_current_config_period_id()
            if _pid is not None:
                _site = (_store.get_config_period(_pid) or {}).get("site_name")
        _bd = resolve_backup_dir_for_site(_site)
        logger.info("engine_startup: backup dir = %s (site=%r)", _bd, _site)
    except Exception as _bde:
        logger.warning("engine_startup: backup dir resolution failed: %s", _bde)

    # BL-5: data lineage (db_uuid) and foreign-restore detection. db_uuid lives
    # in the DB (store_meta) and travels with the data; a native DB carries
    # db_uuid == install_id. A mismatch means this DB was restored from another
    # instance — surface a dismissible notice (dismissed per lineage). This never
    # affects backup-directory ownership, which keys on the install id alone.
    try:
        global FOREIGN_RESTORE_NOTICE
        import instance as _inst
        _dbid = _store.get_meta("db_uuid") if _store is not None else None
        _dbid2 = _inst.ensure_db_uuid(_dbid)
        if _store is not None and _dbid2 != _dbid:
            _store.set_meta("db_uuid", _dbid2)
        FOREIGN_RESTORE_NOTICE = _inst.foreign_restore_notice(_dbid2)
        if FOREIGN_RESTORE_NOTICE.get("foreign") \
                and not FOREIGN_RESTORE_NOTICE.get("acknowledged"):
            logger.warning("engine_startup: DB appears restored from another "
                           "instance (db_uuid=%s… != this install)",
                           str(_dbid2)[:8])
    except Exception as _fre:
        logger.warning("engine_startup: lineage check failed: %s", _fre)
        FOREIGN_RESTORE_NOTICE = {"foreign": False, "db_uuid": None,
                                  "acknowledged": False}

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

    # ── Upgrade bridge: preserve existing CAD users on first 3.0.0 boot ───
    # If no mode is stored but a main import sensor is configured, this is an
    # existing 2.x user — set 'cad' silently so nothing is disrupted and the
    # API path stays off (also neutralises any stale creds file in /data).
    # A fresh install (no import sensor) is left 'unset' for the setup survey.
    try:
        _detect_upgrade_mode(config)
    except Exception as _ude:
        logger.warning("engine_startup: upgrade mode detection failed: %s", _ude)

    # ── Config-state diagnostic dump (open-item 2) ───────────────────────
    # One-shot, after mode is settled. The log is the only window into a
    # self-hosted user's config, so a bug report can be diagnosed from it.
    try:
        _log_config_state(config)
    except Exception as _lce:
        logger.warning("engine_startup: config-state dump failed: %s", _lce)

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
        # Offload ONLY the blocking NETWORK fetch to the executor. All DB writes
        # stay on the loop thread — the engine owns a single SQLite connection
        # (check_same_thread=False), so a DB write from the executor thread races
        # the loop-thread writers (poll/finalise/historical-carbon) on that one
        # connection and corrupts it: "cannot commit - no transaction is active" /
        # "error return without exception set". (Same rule the historical carbon
        # backfill already follows.)
        try:
            slots = await _aio.wait_for(
                loop.run_in_executor(None, _fetch_carbon_intensity, postcode),
                timeout=_CI_TIMEOUT)
        except _aio.TimeoutError:
            logger.warning("engine_startup: CI fetch timed out after %ds — will retry on first tick", _CI_TIMEOUT)
            return
        except urllib.error.URLError as e:
            logger.warning("engine_startup: CI fetch failed: %s — will retry on first tick", e)
            return
        except Exception as e:
            logger.warning("engine_startup: CI fetch error: %s", e)
            return
        # DB writes on the loop thread.
        n = 0
        try:
            for slot in (slots or []):
                _store.upsert_carbon_intensity(
                    slot["captured_at"], postcode,
                    slot["intensity_forecast"], slot["ci_index"],
                    slot["intensity_actual"])
                n += 1
            _store.prune_carbon_intensity(days=4)
        except Exception as e:
            logger.warning("engine_startup: CI persist error: %s", e)
        if n:
            global _last_ci_fetch
            _last_ci_fetch = datetime.now(timezone.utc).replace(tzinfo=None)
            logger.info("engine_startup: CI fetch complete — %d slots for %s", n, postcode)
            # Carbon-gap recovery — DB work, so run it ON THE LOOP THREAD (not an
            # executor) for the same single-connection reason.
            try:
                rec = _recover_missing_carbon()
                if rec:
                    logger.info("engine_startup: carbon recovery backfilled "
                                "%d block(s)", rec)
            except Exception as e:
                logger.warning("engine_startup: carbon recovery failed: %s", e)
            # Historical backfill (run-once, resumable, paged) — dispatched as a
            # loop task; keeps its own DB work on the loop thread.
            try:
                _maybe_backfill_historical_carbon()
            except Exception as e:
                logger.warning("engine_startup: historical backfill schedule "
                               "failed: %s", e)

    import asyncio as _asyncio
    await _asyncio.gather(
        _wait_for_sensors(),
        _startup_ci_fetch(),
        return_exceptions=True
    )

    # ── Detect third-party HA integrations (BCD live-power, OHME charge-mode) ──
    # Non-fatal; logs the config-state detection line and stores the result for
    # the overlay (OHME verified path) and live-power tile (BCD offload).
    try:
        await _detect_and_log_integrations(ha)
    except Exception as _de:
        logger.warning("engine_startup: integration detection step failed: %s", _de)

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

    # BL-19 one-time sweep: repair historical sub-meter grid-invariant violations
    # (sub-meter grid > settled parent grid import), e.g. gap-fill blocks written
    # during an outage whose main later settled small. Flag them for PASS 2
    # re-run; the drain re-clamps them via the BL-19-fixed apportionment. Gated
    # by a marker so it runs at most once per DB.
    try:
        if not _store.get_kraken_state(_BL19_GRID_SWEEP_DONE_KEY):
            _flagged = _store.flag_grid_invariant_violations()
            _store.set_kraken_state(_BL19_GRID_SWEEP_DONE_KEY,
                                    datetime.now(timezone.utc).isoformat())
            if _flagged:
                logger.info(
                    "engine_startup: BL-19 grid-invariant sweep flagged %d block(s) "
                    "for re-clamp (drain will re-materialise)", _flagged)
    except Exception as _b19e:
        logger.warning("engine_startup: BL-19 grid-invariant sweep failed: %s", _b19e)

    # #307 one-time self-heal: a lost/zeroed opening register books a device's whole
    # lifetime register as one interval (prod: house_battery 0→6137 kWh in a
    # half-hour, wrecking its carbon). Physically impossible, so we detect + repair
    # it automatically — no user action. The write-point guard stops NEW ones, so
    # this runs at most once per DB (marker-gated); repaired blocks are flagged
    # needs_review so they surface in the review list. Cost/billing is untouched.
    try:
        if not _store.get_kraken_state(_SPIKE_SWEEP_DONE_KEY):
            _sw = _store.sweep_implausible_sub_blocks(dry_run=False)
            _store.set_kraken_state(_SPIKE_SWEEP_DONE_KEY,
                                    datetime.now(timezone.utc).isoformat())
            if _sw.get("count"):
                logger.warning(
                    "engine_startup: #307 self-heal repaired %d implausible device "
                    "block(s) (> %.0f kWh in one block) — flagged for review",
                    _sw["count"], _sw.get("ceiling_kwh", 0))
    except Exception as _spe:
        logger.warning("engine_startup: #307 device-spike self-heal failed: %s", _spe)

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

    # ── Clear stale pre-restart reads from the in-progress block ──────────
    # The process has only just started and has captured nothing yet, so any
    # reads already in current_block are from BEFORE the restart. If left in,
    # the first one becomes reads[0] (the block opener) and the block's kWh is
    # computed as reads[-1] - reads[0] using a stale prior-session register —
    # a "rogue total" taken as a block value (e.g. read_start carried from the
    # previous session). The gap-detected path below also clears these, but a
    # SHORT restart (< one block) yields no missing windows and would otherwise
    # skip the clear entirely. Clearing unconditionally here covers both cases;
    # the correct opener is then supplied by boundary interpolation against
    # fresh post-restart reads (and the carry-seed written at finalise).
    # ── Clear stale pre-restart reads from the in-progress block ──────────
    # COLD START ONLY (see _clear_stale_inprogress_reads). On a warm re-run (HA
    # reconnect / config-save) the in-progress reads are live and must be kept;
    # an HA upgrade on the dev box was triggering reconnect-storm re-runs that
    # otherwise wiped the live Mini block opener.
    if _clear_stale_inprogress_reads(_cb_for_gap, not _cold_start_complete):
        _store.save_current_block(_cb_for_gap)
        logger.info("engine_startup: cleared stale pre-restart reads from "
                    "in-progress block %s (rogue-total guard, cold start)",
                    _cb_start)

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
                        # Skip if an interpolated block already exists for this
                        # window — prevents a double gap-fill when engine_startup
                        # runs twice in rapid succession (consecutive HA
                        # disconnects), where the second fill would overwrite the
                        # first with slightly different interpolated values.
                        # (Regression: 2.10.9.)
                        _gb_start = _gb.get("start")
                        if _gb_start:
                            _existing = _store._conn.execute(
                                """SELECT block_start FROM blocks
                                   WHERE meter_id = 'electricity_main'
                                     AND block_start = ?
                                     AND interpolated = 1""",
                                (_gb_start,)
                            ).fetchone()
                            if _existing:
                                logger.info(
                                    "engine_startup: gap block %s already exists "
                                    "(interpolated) — skipping", _gb_start
                                )
                                continue
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
                                        _mb["carbon_intensity_g"] = _intensity
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
                # ── Re-seed opener after a within-block (no-gap) restart ──────
                # The rogue-total guard above cleared the in-progress block's
                # reads, including the carry-seed opener. With no gap, gap-fill
                # does not run to restore it, so the block would under-count
                # (measure only the tail). Restore the opener from last_block's
                # read_end. Self-heals at DCC settlement anyway, but this fixes
                # the provisional kWh (and is the only fix for mini-only setups).
                try:
                    _rs_cb = _store.load_current_block()
                    if _rs_cb and last_block:
                        _seeded = _reseed_opener_after_short_restart(last_block, _rs_cb)
                        if _seeded:
                            _store.save_current_block(_rs_cb)
                            logger.info(
                                "engine_startup: re-seeded in-progress block opener "
                                "from last_block read_end after within-block restart "
                                "(%s)", ", ".join(_seeded))
                except Exception as _rse:
                    logger.warning("engine_startup: opener re-seed failed: %s", _rse)

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

    # ── 3.0.0 Kraken discovery (read-only; no polling yet) ────────────────
    # Runs BEFORE the (potentially slow) startup chart render so the supplier API
    # is activated first — chart generation on a large history can take tens of
    # seconds and must not gate API discovery / polling behind it.
    try:
        await _kraken_startup_discovery()
    except Exception as _kd_e:
        logger.warning("engine_startup: kraken discovery skipped: %s", _kd_e)

    # ── One-time region-timeline reconciliation (historical-carbon foundation) ──
    # With the API just discovered, stamp the DNO region (OUTWARD CODE ONLY) onto
    # the config-period timeline from the account's supply address. The first run
    # on a DB overwrites a legacy hand-entered postcode (e.g. a wrong region) with
    # the authoritative one; later runs respect manual edits. Best-effort.
    try:
        await _probe_and_apply_region_periods()
    except Exception as _rp_e:
        logger.warning("engine_startup: region probe skipped: %s", _rp_e)

    # Teardown guard for a Change Setup api→cad transition. When the mode no
    # longer uses the API, _kraken_startup_discovery returns early and never
    # calls _maybe_setup_mini, so a Mini reader wired in a prior api+mini session
    # would otherwise PERSIST (same process) and keep collecting into cad blocks.
    # Clear it explicitly. (The poll task is already cancelled at startup top and
    # not relaunched below — gated on mode_uses_api — so no poll leak.)
    _teardown_mini_if_no_api()

    # If an API mode is configured and discovery succeeded, ensure the DCC poll
    # task is running. Only relevant when the engine loop is ALREADY up — i.e. a
    # mid-session engine_startup re-run (config save). At first boot _engine_ha
    # is still None and main()'s gather() launches the poll task itself, so we
    # skip here to avoid a redundant (and previously misleading) call. Launching
    # HERE rather than from connect_kraken_now ensures the store + client are
    # rebuilt first (avoids the "Connector is closed" teardown race).
    if (_engine_ha is not None and _kraken_client is not None
            and _kraken_discovery and mode_uses_api()):
        _ensure_kraken_poll_task_running()

    # ── Startup charts (LAST, OFF the loop, FIRE-AND-FORGET) ──────────────
    # Rendered in a worker thread (dedicated read-only connection) AND not
    # awaited, so: (1) engine_startup — and the web server that waits on it —
    # completes immediately instead of sitting behind a tens-of-seconds render;
    # (2) the render never stalls the HA WebSocket heartbeat, which was dropping
    # the connection and triggering a reconnect→re-startup→re-render storm. The
    # in-flight guard means a reconnect that re-runs startup won't stack renders.
    global _charts_task
    try:
        _charts_task = asyncio.create_task(_generate_charts_offloaded())
    except Exception as _ct_e:
        logger.warning("engine_startup: could not schedule chart render: %s", _ct_e)

    # Mark cold start done: any later engine_startup in this process (HA
    # reconnect / config-save) is warm and must not run the rogue-total guard.
    _cold_start_complete = True
    logger.info("engine_startup: complete")