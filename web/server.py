"""
server.py
=========
Flask web server serving:
  - Config UI  (meter editor with live HA entity picker)
  - Chart viewer (net heatmap + daily usage)
  - Data import
  - REST API for the UI

Runs on port 8099 inside the add-on container.
Started as a background thread from main.py alongside the engine.
"""

import gzip as _gzip
import json
import logging
import os
import threading
from pathlib import Path

from flask import Flask, Response, jsonify, make_response, redirect, render_template, request, send_file, url_for

logger = logging.getLogger("server")

app = Flask(__name__, template_folder="templates")


# ── Response compression ──────────────────────────────────────────────────────
# HA's ingress proxy gzips responses, but the add-on's own web server (direct
# :8099 access, and Lovelace iframe/webpage cards) does not — so large text
# responses like the multi-MB daily chart HTML transferred uncompressed and took
# 15-20s to download on the direct port. Compress compressible text bodies when
# the client advertises gzip. Safe under ingress: a body already carrying
# Content-Encoding is passed through untouched, so nginx never double-compresses.
_GZIP_MIN_BYTES = 1024
_GZIP_TYPES = ("text/html", "text/css", "text/plain", "text/javascript",
               "application/javascript", "application/json", "application/xml",
               "image/svg+xml")


@app.after_request
def _compress_response(response):
    try:
        if "gzip" not in (request.headers.get("Accept-Encoding") or "").lower():
            return response
        if not (200 <= response.status_code < 300):
            return response
        if response.headers.get("Content-Encoding"):
            return response
        ctype = (response.headers.get("Content-Type") or "").split(";")[0].strip().lower()
        if ctype not in _GZIP_TYPES:
            return response
        # Materialise the body — send_file responses use direct_passthrough.
        response.direct_passthrough = False
        data = response.get_data()
        if len(data) < _GZIP_MIN_BYTES:
            return response
        compressed = _gzip.compress(data, compresslevel=6)
        if len(compressed) >= len(data):
            return response
        response.set_data(compressed)
        response.headers["Content-Encoding"] = "gzip"
        response.headers["Content-Length"] = str(len(compressed))
        vary = response.headers.get("Vary")
        if not vary:
            response.headers["Vary"] = "Accept-Encoding"
        elif "accept-encoding" not in vary.lower():
            response.headers["Vary"] = vary + ", Accept-Encoding"
    except Exception as e:
        logger.warning("response compression failed: %s", e)
    return response


def _read_version() -> str:
    """Read version from config.yaml — works in both supervised and standalone modes."""
    # config.yaml sits one directory above server.py (web/../config.yaml)
    candidates = [
        os.path.join(os.path.dirname(__file__), "..", "config.yaml"),
        "/addons/energy_meter_tracker/config.yaml",
    ]
    for path in candidates:
        try:
            with open(os.path.normpath(path)) as f:
                for line in f:
                    if line.startswith("version:"):
                        return line.split(":", 1)[1].strip().strip('"').strip("'")
        except Exception:
            pass
    return ""

APP_VERSION = _read_version()

# Per-instance display label for the footer. This is deliberately an INSTALL
# identity, not a data one: it must NOT come from the database (site name lives
# in the config-period chain and travels with a restore, so prod-dev restored
# from prod would show prod's name). Resolution order, highest first:
#   1. the `instance_name` option (exported as EMT_INSTANCE_NAME by run.sh, or
#      set directly in a standalone container) — the explicit per-instance
#      override, and the ONLY distinguisher when two installs share one manifest
#      name (the repo-URL workaround);
#   2. the add-on's manifest `name` via Supervisor self-info ("… (DEV)");
#   3. the container hostname (standalone last resort).
# Memoised: resolved once per process (never changes at runtime).
_INSTANCE_LABEL: "str | None" = None


def _instance_label() -> str:
    global _INSTANCE_LABEL
    if _INSTANCE_LABEL is not None:
        return _INSTANCE_LABEL
    # 1. Explicit per-instance override (the `instance_name` option).
    label = (os.environ.get("EMT_INSTANCE_NAME") or "").strip()
    # 2. Supervised: the add-on's manifest name (distinct per install).
    if not label:
        token = os.environ.get("SUPERVISOR_TOKEN")
        if token:
            try:
                import urllib.request
                req = urllib.request.Request(
                    "http://supervisor/addons/self/info",
                    headers={"Authorization": f"Bearer {token}"})
                with urllib.request.urlopen(req, timeout=3) as resp:
                    data = json.loads(resp.read().decode("utf-8"))
                label = ((data.get("data") or {}).get("name") or "").strip()
            except Exception as e:
                logger.info("_instance_label: supervisor self-info failed (%s)", e)
    # 3. Standalone last resort: the container hostname.
    if not label:
        import socket
        label = (socket.gethostname() or "").strip()
    _INSTANCE_LABEL = label
    return label


@app.context_processor
def inject_globals():
    has_power_sensor = False
    has_postcode = False
    try:
        from energy_engine_io import load_json as _lj
        cfg = load_config()
        for m_data in cfg.get("meters", {}).values():
            if not (m_data.get("meta") or {}).get("sub_meter"):
                _meta = (m_data.get("meta") or {})
                # A live-power source is a configured power_sensor entity (device
                # sensor or auto-adopted BCD) OR the Octopus Mini opted in as the
                # source (power_source=="mini" — not an HA entity, polled via API).
                has_power_sensor = (bool(_effective_power_sensor(_meta))
                                    or _meta.get("power_source") == "mini")
                has_postcode     = bool(_meta.get("postcode_prefix", "").strip())
                break
    except Exception:
        pass
    return {"app_version": APP_VERSION, "has_power_sensor": has_power_sensor, "has_postcode": has_postcode,
            "server_port": int(os.environ.get("EMT_PORT") or "8099"),
            "instance_label": _instance_label()}


class IngressMiddleware:
    """
    WSGI middleware that rewrites the PATH_INFO to strip the ingress prefix
    and sets SCRIPT_NAME so Flask url_for() generates correct URLs.
    """
    def __init__(self, app):
        self.app = app

    def __call__(self, environ, start_response):
        ingress_path = environ.get("HTTP_X_INGRESS_PATH", "")
        if ingress_path:
            environ["SCRIPT_NAME"] = ingress_path
            path = environ.get("PATH_INFO", "")
            if path.startswith(ingress_path):
                environ["PATH_INFO"] = path[len(ingress_path):] or "/"
        return self.app(environ, start_response)

app.wsgi_app = IngressMiddleware(app.wsgi_app)
app.secret_key = os.urandom(24)

# ── Paths (injected from main.py before server starts) ────────────────────────
DATA_DIR         = None
CHART_DIR        = None
import os as _os


def _share_backup_dir() -> str:
    """BL-5: the backup dir is resolved at startup from the site slug (see
    instance.py) and lives on `engine`. Read it there rather than keeping a
    second, stale copy — this module used to duplicate the derivation, which
    would silently disagree after a site rename."""
    try:
        import engine as _eng
        return _eng.SHARE_BACKUP_DIR
    except Exception:
        return (_os.path.join("/data/energy_meter_tracker", "backup")
                if _os.environ.get("EMT_MODE") == "standalone"
                else "/share/energy_meter_tracker_backup")

_ha_client = None   # reference to the running HAClient instance
_event_loop = None  # asyncio event loop — captured at init time

# BlockStore — opened lazily on first use, shared across all server requests
import sys as _sys
_sys.path.insert(0, _os.path.dirname(_os.path.dirname(__file__)))
from block_store import BlockStore, open_block_store
_store: BlockStore | None = None


def _get_store() -> BlockStore:
    """Return the server's BlockStore, opening it lazily on first call."""
    global _store
    if _store is None:
        if DATA_DIR is None:
            raise RuntimeError("server.init() has not been called")
        db_path = _os.path.join(DATA_DIR, "blocks.db")
        _store = open_block_store(db_path)
    return _store


def _config_timezone() -> str:
    """The main meter's configured timezone (for wall-clock display of UTC
    block_start values in the gap / deleted-ranges lists). Falls back to UTC."""
    try:
        _cfg = load_config()
        for _md in (_cfg.get("meters") or {}).values():
            if not (_md.get("meta") or {}).get("sub_meter"):
                return (_md.get("meta") or {}).get("timezone", "UTC")
    except Exception:
        pass
    return "UTC"


def _regen_charts_safely():
    """Regenerate the pre-built charts on the engine's event-loop thread — never
    the Flask request thread.

    The engine's BlockStore (engine.get_store()) is opened check_same_thread=False
    and is driven by the engine on the asyncio loop thread. Calling
    generate_charts against it from a request thread is concurrent cross-thread
    use of one SQLite connection → segfault (observed on delete-device, where
    engine_startup is running on the loop at the same instant). Scheduling the
    regen onto the loop serialises it with all other engine store access, exactly
    as the engine's own finalise-time regen already runs. Fire-and-forget: the
    chart is ready by the user's next page load. Falls back to inline only when no
    loop is running (tests / pre-init), where there is no concurrent engine."""
    try:
        import asyncio as _a
        import engine as _eng_r
        if _event_loop is not None and _event_loop.is_running():
            async def _run():
                try:
                    _eng_r.generate_charts(_eng_r.get_store())
                except Exception as _e:
                    logger.warning("_regen_charts_safely: loop regen failed: %s", _e)
            _a.run_coroutine_threadsafe(_run(), _event_loop)
        else:
            _eng_r.generate_charts(_eng_r.get_store())
    except Exception as e:
        logger.warning("_regen_charts_safely: %s", e)


def init(data_dir: str, chart_dir: str, ha_client):
    global DATA_DIR, CHART_DIR, _ha_client, _event_loop
    import asyncio as _asyncio
    _event_loop = _asyncio.get_event_loop()
    DATA_DIR   = data_dir
    CHART_DIR  = chart_dir
    _ha_client = ha_client


def _run_on_engine_loop(coro, timeout: float = 30.0):
    """Run an async coroutine on the ENGINE's running event loop and block this
    (Flask) thread on the result.

    This is the ONLY correct way for a sync route to await something that uses
    the engine's aiohttp session (HA client, Kraken client): those objects bind
    their connector/timeout to the loop they were created on. Spinning up a
    fresh loop with run_until_complete() raises "Timeout context manager should
    be used inside a task". Raises RuntimeError if the loop isn't available.
    """
    import asyncio as _asyncio
    if not (_event_loop and _event_loop.is_running()):
        raise RuntimeError("engine event loop not running")
    fut = _asyncio.run_coroutine_threadsafe(coro, _event_loop)
    return fut.result(timeout=timeout)


class _ConfigError(Exception):
    """A config-period mutation validation error → surfaced as HTTP 400."""


class _ConfigBusy(Exception):
    """The mutation was submitted to the engine loop but the loop was busy (typically a
    long post-restore engine_startup, made slower by an HA WebSocket drop), so we
    stopped waiting. run_coroutine_threadsafe is NOT cancelled by a result-timeout —
    the coroutine stays queued and WILL apply once the loop frees. The endpoint reports
    an honest 'queued' instead of a misleading raw TimeoutError."""


_CONFIG_BUSY_MSG = ("The engine is busy finishing a restore or reconnecting — your "
                    "change has been queued and should apply within a minute. Refresh "
                    "to confirm.")


def _config_mutation_on_loop(mutate, timeout: float = 30.0):
    """Run a config_periods read+write on the ENGINE's loop and connection, then
    schedule an OFF-loop chart regen (#361).

    Config edits used to write on the shared web BlockStore from the Flask request
    thread, racing the engine's writes on its own connection — producing SQLite
    "database is locked" / "another row available", and (when a write half-applied)
    a Total Bill that no longer summed until the next finalise regenerated the
    charts. Running the mutation on the engine loop serialises it with every other
    engine write (one connection, one thread); `_schedule_chart_regen()` — fired on
    the loop — offloads the chart rebuild to a read-only worker so the change shows
    immediately without blocking the loop. `mutate(store)` receives the engine's
    store and returns the JSON-able result. No engine loop (tests / pre-init) →
    there is no concurrent writer, so run inline on the web store."""
    if not (_event_loop and _event_loop.is_running()):
        store = _get_store()
        res = mutate(store)
        _regen_charts_safely()
        return res
    import engine as _eng
    async def _coro():
        store = _eng.get_store()
        res = mutate(store)
        try:
            _eng._schedule_chart_regen()      # on-loop → offloaded read-only render
        except Exception as _ce:
            logger.warning("_config_mutation_on_loop: chart regen schedule failed: %s", _ce)
        return res
    import concurrent.futures as _cf
    try:
        return _run_on_engine_loop(_coro(), timeout=timeout)
    except _cf.TimeoutError:
        # The loop was busy (e.g. a slow post-restore engine_startup). The mutation is
        # still queued and will apply — signal 'queued' so the endpoint doesn't report
        # a raw TimeoutError for a change that actually succeeds.
        logger.warning("_config_mutation_on_loop: engine loop busy after %.0fs — "
                       "mutation left queued (will apply once the loop frees)", timeout)
        raise _ConfigBusy()


def _schedule_offloaded_regen():
    """Rebuild the pre-built charts OFF the loop, from a Flask request thread.

    Schedules engine._schedule_chart_regen() ON the loop, which offloads the render
    to a read-only worker (never the request thread, never blocking the loop) and is
    coalesced so a change is never dropped. Used by settings toggles that change the
    displayed billing view (e.g. the ex-VAT / bill-rounding option) so the change
    shows immediately instead of waiting for the next block finalise. Inline fallback
    when no engine loop is running (tests / pre-init)."""
    try:
        import engine as _eng
        if _event_loop and _event_loop.is_running():
            import asyncio as _a
            async def _c():
                _eng._schedule_chart_regen()
            _a.run_coroutine_threadsafe(_c(), _event_loop)
        else:
            _eng.generate_charts(_eng.get_store())
    except Exception as e:
        logger.warning("_schedule_offloaded_regen: %s", e)



def start():
    """Start Flask in a background daemon thread."""
    t = threading.Thread(target=_run, daemon=True, name="flask")
    t.start()
    _port = int(os.environ.get("EMT_PORT") or "8099")
    logger.info("server: Flask started on port %d", _port)


def _run():
    import logging as _logging
    _log = _logging.getLogger("server")
    try:
        from waitress import serve
        _port = int(os.environ.get("EMT_PORT") or "8099")
        _log.info("server: waitress binding on port %d", _port)
        serve(app, host="0.0.0.0", port=_port, threads=4)
    except Exception as _e:
        _log.critical("server: waitress failed: %s", _e, exc_info=True)
        raise


# ── Settings defaults ─────────────────────────────────────────────────────────

# Carbon assumption defaults with citations.
# Stored in store_meta['settings'] as JSON; missing keys fall back to these.
SETTINGS_DEFAULTS = {
    # ── Equivalence factors ────────────────────────────────────────────────
    "co2_car_petrol_g_per_mile":   180.0,  # BEIS/DESNZ 2023 GHG conversion factors
    "co2_car_diesel_g_per_mile":   168.0,  # BEIS/DESNZ 2023 GHG conversion factors
    "co2_tree_kg_per_year":          21.0, # Woodland Trust estimate
    "co2_flight_lhr_nyc_kg":        670.0, # BEIS 2023 (economy, radiative forcing)
    # ── Display ────────────────────────────────────────────────────────────
    "distance_unit":              "miles", # miles | km
    # ── Export methodology ─────────────────────────────────────────────────
    "co2_export_method":       "grid_average",  # grid_average | custom
    "co2_export_custom_intensity":  200.0, # gCO₂/kWh — only if method=custom
    # ── EV ─────────────────────────────────────────────────────────────────
    "ev_efficiency":                  3.2, # miles/kWh or km/kWh (distance_unit)
    "ev_charge_efficiency":          0.88, # AC→DC charge efficiency (IEA 2022, typical Type 2)
    # ── Battery ────────────────────────────────────────────────────────────
    "battery_round_trip_efficiency": 0.90, # round-trip AC→DC→AC (typical Li-ion home battery)
    # ── Heat pump ──────────────────────────────────────────────────────────
    "hp_cop":                         3.0, # seasonal COP (SCOP) — override with manufacturer spec
    "gas_co2_g_per_kwh":            203.0, # BEIS/DESNZ 2023 GHG conversion factors
    "gas_boiler_efficiency":         0.90, # typical modern condensing boiler
    # ── Bill rounding (BL-24) ──────────────────────────────────────────────
    # Opt-in: show the Billing-tab bill summary with Octopus's bill-rounding
    # method (Guy Lipman ladder) on the stored ex-VAT figures. Default off ⇒ the
    # summary is byte-identical to today.
    "bill_rounding_summary":        False,  # bool
}

SETTINGS_NUMERIC = {
    "co2_car_petrol_g_per_mile",
    "co2_car_diesel_g_per_mile",
    "co2_tree_kg_per_year",
    "co2_flight_lhr_nyc_kg",
    "co2_export_custom_intensity",
    "ev_efficiency",
    "ev_charge_efficiency",
    "battery_round_trip_efficiency",
    "hp_cop",
    "gas_co2_g_per_kwh",
    "gas_boiler_efficiency",
}

# ── Helpers ───────────────────────────────────────────────────────────────────

def config_path():
    return os.path.join(DATA_DIR, "meters_config.json")


def _rebuild_config_period_chain(store):
    """
    Re-sort all config periods by effective_from and rebuild the contiguous
    chain: each period's effective_to is set to the next period's effective_from,
    with the last (most recent) period getting effective_to = NULL.

    Also reassigns blocks so every block's config_period_id matches the period
    whose [effective_from, effective_to) range contains the block's block_start.

    Called after any insert, edit or delete of a config period.
    """
    cur = store._conn.execute(
        "SELECT id, effective_from FROM config_periods ORDER BY effective_from ASC"
    )
    periods = cur.fetchall()
    if not periods:
        return

    # Build effective_to for each period
    updates = []
    for i, row in enumerate(periods):
        if i + 1 < len(periods):
            updates.append((periods[i + 1]["effective_from"], row["id"]))
        else:
            updates.append((None, row["id"]))

    store._conn.execute("BEGIN")
    for effective_to, period_id in updates:
        store._conn.execute(
            "UPDATE config_periods SET effective_to = ? WHERE id = ?",
            (effective_to, period_id)
        )

    # Reassign blocks: each block goes to the period containing its block_start
    # Fetch updated periods
    cur2 = store._conn.execute(
        "SELECT id, effective_from, effective_to FROM config_periods ORDER BY effective_from ASC"
    )
    chain = cur2.fetchall()
    for i, period in enumerate(chain):
        pid          = period["id"]
        ef_from      = period["effective_from"]
        ef_to        = period["effective_to"]
        if ef_to is not None:
            store._conn.execute(
                """UPDATE blocks SET config_period_id = ?
                   WHERE block_start >= ? AND block_start < ?""",
                (pid, ef_from, ef_to)
            )
        else:
            # Last period: all blocks from effective_from onwards
            store._conn.execute(
                """UPDATE blocks SET config_period_id = ?
                   WHERE block_start >= ?""",
                (pid, ef_from)
            )

    store._conn.execute("COMMIT")


def load_config():
    """
    Load meter configuration from the normalised DB (blocks.db) config_periods table.
    The DB is the single source of truth — meters_config.json is only used as a
    last resort for a truly fresh install where blocks.db does not yet exist.
    """
    db_path = os.path.join(DATA_DIR, "blocks.db") if DATA_DIR else None
    db_exists = db_path and os.path.exists(db_path)

    _load_attempts = 3
    for _attempt in range(_load_attempts):
        try:
            store = _get_store()
            cp = store._conn.execute(
                "SELECT id FROM config_periods "
                "WHERE effective_to IS NULL ORDER BY effective_from DESC LIMIT 1"
            ).fetchone()
            if cp:
                return store.config_from_db(cp["id"])
            # DB exists but no config period — may be a transient WAL lock.
            # Retry before concluding there is genuinely no config period.
            if db_exists and _attempt < _load_attempts - 1:
                import time as _time
                _time.sleep(0.1)
                continue
            if db_exists:
                logger.warning("load_config: blocks.db exists but no active config period found")
                return {"schema_version": "1.0", "meters": {}}
        except Exception as e:
            if _attempt < _load_attempts - 1:
                import time as _time
                _time.sleep(0.1)
                continue
            logger.error("load_config: failed to read config from DB: %s", e)
            # Only fall back to JSON if no DB exists at all
            if db_exists:
                logger.error("load_config: DB exists but config read failed — returning empty config")
                return {"schema_version": "1.0", "meters": {}}
        break

    # True fresh install: no DB yet, try meters_config.json
    p = config_path()
    if not os.path.exists(p):
        return {"schema_version": "1.0", "meters": {}}
    logger.info("load_config: no DB found, loading from meters_config.json")
    with open(p) as f:
        return json.load(f)


def save_config(data: dict):
    """
    Save meter configuration. The DB (normalised meters/meter_channels tables)
    is the authoritative store. meters_config.json is written as a
    human-readable export only and is not read back as live state.
    """
    # Preserve channel meta from existing config that the UI doesn't manage.
    # Read from DB (authoritative) rather than the JSON file.
    try:
        existing = load_config()
        for meter_id, meter in existing.get("meters", {}).items():
            for ch_id, ch in meter.get("channels", {}).items():
                if "meta" in ch:
                    try:
                        data["meters"][meter_id]["channels"][ch_id].setdefault("meta", ch["meta"])
                    except (KeyError, TypeError):
                        pass
    except Exception:
        pass

    # Write to the active config period in the DB (normalised tables)
    try:
        store = _get_store()
        main_meta = {}
        for md in data.get("meters", {}).values():
            if not (md.get("meta") or {}).get("sub_meter"):
                main_meta = md.get("meta") or {}
                break
        active = store._conn.execute(
            "SELECT id FROM config_periods WHERE effective_to IS NULL "
            "ORDER BY effective_from DESC LIMIT 1"
        ).fetchone()
        if active:
            period_id = active["id"]
            with store._conn:
                # Update billing scalar fields on config_periods
                store._conn.execute(
                    """UPDATE config_periods
                       SET billing_day     = ?,
                           block_minutes   = ?,
                           timezone        = ?,
                           currency_symbol = ?,
                           currency_code   = ?,
                           site_name       = ?
                       WHERE id = ?""",
                    (
                        int(main_meta.get("billing_day") or 1),
                        int(main_meta.get("block_minutes") or 30),
                        main_meta.get("timezone", "UTC"),
                        main_meta.get("currency_symbol", "£"),
                        main_meta.get("currency_code", "GBP"),
                        main_meta.get("site"),
                        period_id,
                    )
                )
                # Rewrite normalised meter rows for this period
                # Delete existing rows first (upsert handles adds/updates;
                # deletion handles meters removed from config)
                old_meter_ids = [r["id"] for r in store._conn.execute(
                    "SELECT id FROM meters WHERE config_period_id=?", (period_id,)
                ).fetchall()]
                for mid in old_meter_ids:
                    store._conn.execute(
                        "DELETE FROM meter_channels WHERE meter_id=?", (mid,)
                    )
                store._conn.execute(
                    "DELETE FROM meters WHERE config_period_id=?", (period_id,)
                )
                store._write_meters(data, period_id)
    except Exception as _e:
        logger.error("save_config: DB write failed: %s", _e)
        raise

    # Write meters_config.json as a convenience export (not read back as state)
    try:
        p = config_path()
        os.makedirs(os.path.dirname(p), exist_ok=True)
        tmp = p + ".tmp"
        with open(tmp, "w") as f:
            json.dump(data, f, indent=2)
        os.replace(tmp, p)
    except Exception as _e:
        logger.warning("save_config: could not write meters_config.json export: %s", _e)


# ── Pages ─────────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    cfg = load_config()
    if cfg.get("meters"):
        last = request.cookies.get("emt_last_page", "charts")
        # Map old cookie values to new page names for backwards compatibility
        page_map = {
            "charts":   "charts",
            "summary":  "live_power",
            "import":   "data_management",
            "logs":     "logs",
            "help":     "help",
            "config":   "settings",
            # new names
            "live_power":     "live_power",
            "data_management": "data_management",
            "settings":       "settings",
            "insights":       "insights",
        }
        last = page_map.get(last, "charts")
        return redirect(url_for(last + "_page"))
    return redirect(url_for("settings_page"))


@app.route("/api/last-page", methods=["POST"])
def api_set_last_page():
    page = request.get_json(force=True).get("page", "charts")
    valid = {"charts", "live_power", "data_management", "logs", "help", "settings", "insights"}
    if page not in valid:
        page = "charts"
    resp = jsonify({"ok": True})
    resp.set_cookie("emt_last_page", page, max_age=60*60*24*365, samesite="Lax")
    return resp





@app.route("/static/logo.png")
def serve_logo():
    import os
    p = "/app/logo.png"
    if os.path.exists(p):
        return send_file(p, mimetype="image/png")
    return "", 404


@app.route("/static/icon.png")
def serve_icon():
    import os
    p = "/app/icon.png"
    if os.path.exists(p):
        return send_file(p, mimetype="image/png")
    return "", 404


@app.route("/favicon.ico")
def serve_favicon():
    # Belt-and-braces for the browser's default /favicon.ico request (helps
    # standalone; under ingress the <link> in base.html carries the correct path).
    import os
    p = "/app/icon.png"
    if os.path.exists(p):
        return send_file(p, mimetype="image/png")
    return "", 404


@app.route("/help")
def help_page():
    return render_template("help.html", active="help")


@app.route("/logs")
def logs_page():
    return render_template("logs.html", active="logs")


@app.route("/api/logs")
def api_logs():
    """Fetch add-on logs — via Supervisor API in supervised mode, log file in standalone."""
    import urllib.request
    lines = min(int(request.args.get("lines", 100)), 1000)
    emt_mode = os.environ.get("EMT_MODE", "supervised")

    if emt_mode == "standalone":
        # In standalone mode read from log file if available, otherwise return empty
        log_path = "/data/energy_meter_tracker/addon.log"
        try:
            if os.path.exists(log_path):
                with open(log_path, "r", errors="replace") as f:
                    all_lines = f.read().splitlines()
                return jsonify({"lines": all_lines[-lines:]})
            else:
                return jsonify({"lines": ["[Logs not available in standalone Docker mode]",
                                          "Run with -v /path/to/logs:/data/energy_meter_tracker",
                                          "or check docker logs <container_name>"]})
        except Exception as e:
            return jsonify({"error": str(e), "lines": []})
    else:
        token = os.environ.get("SUPERVISOR_TOKEN", "")
        try:
            req = urllib.request.Request(
                "http://supervisor/addons/self/logs",
                headers={"Authorization": "Bearer " + token}
            )
            with urllib.request.urlopen(req, timeout=10) as resp:
                raw = resp.read().decode("utf-8", errors="replace")
            all_lines = raw.splitlines()
            return jsonify({"lines": all_lines[-lines:]})
        except Exception as e:
            logger.error("api_logs: %s", e)
            return jsonify({"error": str(e), "lines": []})


@app.route("/charts")
def charts_page():
    heatmap_exists = os.path.exists(os.path.join(CHART_DIR, "net_heatmap.html"))
    daily_exists   = os.path.exists(os.path.join(CHART_DIR, "daily_usage.html"))
    try:
        block_count = _get_store().count_blocks()
    except Exception:
        block_count = 0
    return render_template(
        "charts.html",
        heatmap_exists=heatmap_exists,
        daily_exists=daily_exists,
        block_count=block_count,
        active="charts",
    )


def _format_billing(summary, cfg, currency):
    """Convert energy_charts billing summary dict into (total, rows) for the summary template."""
    if not summary:
        return None, []
    totals     = summary.get("totals", {})
    meter_meta = summary.get("meter_meta", {})
    total_cost = summary.get("total_cost")
    if total_cost is None:
        return None, []

    main_imp_kwh = main_imp_cost = 0.0
    main_exp_kwh = main_exp_cost = 0.0
    sub_rows = []
    sub_imp_kwh_total = 0.0

    for key, t in totals.items():
        meta      = meter_meta.get(key, {})
        is_sub    = t.get("is_submeter") or meta.get("is_submeter", False)
        is_export = "export" in key.lower()
        cost      = float(t.get("cost") or 0)
        kwh       = float(t.get("kwh") or 0)

        if is_export:
            main_exp_kwh  += kwh
            main_exp_cost += abs(cost)
        elif is_sub:
            if abs(cost) > 0.0001 or kwh > 0.0001:
                device = meta.get("device") or key.split("/")[0].strip()
                sub_rows.append({"label": f"↳ {device} ({kwh:.3f} kWh)", "cost": cost})
                sub_imp_kwh_total += kwh
        else:
            main_imp_kwh  += kwh
            main_imp_cost += cost

    rows = []
    # Total import row (grid remainder + all sub-meters)
    total_imp_kwh = main_imp_kwh + sub_imp_kwh_total
    total_imp_cost = main_imp_cost + sum(float(r["cost"]) for r in sub_rows)
    if total_imp_kwh > 0.0001 or total_imp_cost > 0.0001:
        rows.append({"label": f"Total Import ({total_imp_kwh:.3f} kWh)", "cost": total_imp_cost, "bold": True})
    # Direct import (remainder not attributed to sub-meters)
    if main_imp_kwh > 0.0001 or main_imp_cost > 0.0001:
        rows.append({"label": f"Direct import ({main_imp_kwh:.3f} kWh)", "cost": main_imp_cost})
    # Sub-meters
    for r in sub_rows:
        rows.append(r)
    # Export
    if main_exp_kwh > 0.0001:
        rows.append({"label": f"Grid Export ({main_exp_kwh:.3f} kWh)", "cost": -main_exp_cost})
    # Standing charge
    sc = summary.get("total_standing", 0.0)
    if sc > 0.0001:
        rows.append({"label": "Standing Charge", "cost": sc})

    return total_cost, rows


@app.route("/live-power")
def live_power_page():
    from datetime import datetime
    from zoneinfo import ZoneInfo
    from energy_engine_io import load_json as _load_json

    try:
        cfg      = load_config()
        store    = _get_store()
        main_meta = {}
        for md in cfg.get("meters", {}).values():
            if not (md.get("meta") or {}).get("sub_meter"):
                main_meta = md.get("meta") or {}
                break
        currency    = main_meta.get("currency_symbol", "£")
        tz_name     = main_meta.get("timezone", "UTC")
        billing_day = int(main_meta.get("billing_day") or 1)
        _tz         = ZoneInfo(tz_name)
        now_local   = datetime.now(_tz)
        today_str   = now_local.date().isoformat()

        # ── Billing cards are populated by SSE on first tick — skip expensive
        # block queries on initial page load so the page renders instantly ──
        today_total = today_rows = today_date = None
        month_total = month_rows = month_period = None
        year_total  = year_rows  = year_period  = None
        today_date  = now_local.strftime("%d %b %Y")

        # ── Gauge scale — derived from 48hr power_history (real kW readings) ──
        # Falls back to sensible defaults when no power sensor is configured.
        block_minutes = int(main_meta.get("block_minutes") or 30)
        gauge_max_imp = 10
        gauge_max_exp = 5
        try:
            ph_rows = store.get_power_history(hours=48)
            if ph_rows:
                def _pct(vals, p):
                    s = sorted(vals)
                    return s[min(int(len(s) * p / 100), len(s) - 1)]
                def _nc(kw):
                    for s in [1, 2, 3, 5, 7, 10, 15, 20, 30, 50]:
                        if s >= kw: return s
                    return round(kw * 1.2)
                imp_vals = [r["net_kw"] for r in ph_rows if r["net_kw"] > 0]
                exp_vals = [-r["net_kw"] for r in ph_rows if r["net_kw"] < 0]
                if imp_vals:
                    gauge_max_imp = _nc(_pct(imp_vals, 95))
                if exp_vals:
                    gauge_max_exp = _nc(_pct(exp_vals, 95))
        except Exception as _ge:
            logger.warning("live_power_page: gauge scale from power_history failed: %s", _ge)

    except Exception as e:
        logger.error("summary_page: %s", e)
        currency = "£"
        month_total = today_total = year_total = None
        month_rows = today_rows = year_rows = []
        month_period = year_period = today_date = ""
        gauge_max_imp = 10; gauge_max_exp = 5

    # Check if a live-power source is configured: a power_sensor entity OR the
    # Octopus Mini opted in as the source (power_source=="mini").
    mini_chosen = main_meta.get("power_source") == "mini"
    has_power_sensor = bool(_effective_power_sensor(main_meta)) or mini_chosen

    # Sub-meter device cards (battery SoC / EV / heat-pump power) render
    # independently of the main gauge — each needs only its own sensor. Without
    # this flag the cards were gated on has_power_sensor and vanished on
    # API-only / DCC setups that have no main live-power source.
    has_sub_devices = False
    try:
        from datetime import datetime as _dt_sd
        _today_sd = _dt_sd.now().strftime("%Y-%m-%d")
        for _m in (cfg.get("meters") or {}).values():
            _mm = (_m or {}).get("meta") or {}
            if not _mm.get("sub_meter"):
                continue
            if _mm.get("retired_at") and _mm["retired_at"] <= _today_sd:
                continue
            if (_mm.get("soc_sensor") or _mm.get("inverter_power_sensor")
                    or _mm.get("device_power_sensor") or _mm.get("pv_power_sensor")):
                has_sub_devices = True
                break
    except Exception:
        has_sub_devices = False

    # Mini opt-in offer: a Mini is present AND there's no cheaper live source
    # (no power_sensor entity, no BCD demand sensor). Cheap — no GraphQL here.
    mini_device = bool(_mini_device_id())
    bcd_found = False
    try:
        import engine as _eng_mini
        bcd_found = bool(((_eng_mini._detected_integrations or {}).get("bcd") or {}).get("found"))
    except Exception:
        pass
    mini_available = mini_device and not main_meta.get("power_sensor") and not bcd_found

    # BL-10: gate the smart-charging card on there being IOG dispatch data. No gap
    # when absent — the card element isn't emitted at all.
    has_dispatch_data = False
    try:
        has_dispatch_data = bool(_get_store()._conn.execute(
            "SELECT 1 FROM dispatch_history LIMIT 1").fetchone())
    except Exception:
        has_dispatch_data = False

    return render_template(
        "live_power.html",
        has_dispatch_data=has_dispatch_data,
        active="live_power",
        currency=currency,
        today_total=today_total,
        today_rows=today_rows,
        today_date=today_date,
        month_total=month_total,
        month_rows=month_rows,
        month_period=month_period,
        year_total=year_total,
        year_rows=year_rows,
        year_period=year_period,
        has_power_sensor=has_power_sensor,
        has_sub_devices=has_sub_devices,
        mini_available=mini_available,
        mini_chosen=mini_chosen,
        gauge_max_imp=gauge_max_imp,
        gauge_max_exp=gauge_max_exp,
        block_minutes=int(main_meta.get("block_minutes") or 30),
    )


def _is_meter_type(meta: dict, meter_id: str, expected_type: str) -> bool:
    """Check if a meter is of the given type using only the explicit meter_type field.
    No keyword fallback — meter_type must be explicitly set via the config UI (2.7.0+).
    """
    return (meta.get('meter_type') or '').lower() == expected_type


def _build_soc_response(soc_sensors, ha_client):
    """Safely build SoC sensor readings dict for the live power API response."""
    import engine as _eng_pc
    result = {}
    for m_id, info in soc_sensors.items():
        soc_val = None
        power_kw = None
        pv_kw = None
        try:
            if ha_client and info.get("soc_entity"):
                v = ha_client.get_state(info["soc_entity"])
                if v not in (None, "unknown", "unavailable"):
                    soc_val = round(float(v), 1)
        except (ValueError, TypeError):
            pass
        try:
            if ha_client and info.get("power_entity"):
                v = ha_client.get_state(info["power_entity"])
                if v not in (None, "unknown", "unavailable"):
                    unit = ""
                    try:
                        unit = (ha_client.get_attributes(info["power_entity"])
                                or {}).get("unit_of_measurement", "")
                    except Exception:
                        pass
                    # Shared converter: unit attr → kW (override when the declared
                    # unit is wrong/absent), then invert (positive = charging).
                    power_kw = _eng_pc._power_value_to_kw(
                        v, unit, info.get("power_unit"), info.get("power_invert"))
        except (ValueError, TypeError):
            pass
        # PV / solar generation — always positive, auto unit, no invert.
        try:
            if ha_client and info.get("pv_entity"):
                v = ha_client.get_state(info["pv_entity"])
                if v not in (None, "unknown", "unavailable"):
                    unit = ""
                    try:
                        unit = (ha_client.get_attributes(info["pv_entity"])
                                or {}).get("unit_of_measurement", "")
                    except Exception:
                        pass
                    pv_kw = _eng_pc._power_value_to_kw(v, unit, None, False)
        except (ValueError, TypeError):
            pass
        result[m_id] = {
            "label":        info.get("label", m_id),
            "type":         info.get("type", "battery"),
            "v2x":          info.get("v2x", False),
            "soc_entity":   info.get("soc_entity"),
            "power_entity": info.get("power_entity"),
            "pv_entity":    info.get("pv_entity"),
            "soc":          soc_val,
            "power_kw":     power_kw,
            "pv_kw":        pv_kw,
        }
    return result


def _mini_device_id():
    """The discovered Octopus Mini deviceId (from the engine's boundary reader),
    or None when no Mini is present / not yet probed."""
    try:
        import engine as _eng
        reader = getattr(_eng, "_kraken_mini_reader", None)
        return getattr(reader, "device_id", None) if reader else None
    except Exception:
        return None


def _mini_live_demand_kw():
    """Latest Mini demand in kW, read from the engine's cache (the engine polls
    the Mini server-side, page-independently). Returns None when there's no recent
    reading. No GraphQL call here — the engine is the single poller."""
    try:
        import engine as _eng
        import time as _time
        d = getattr(_eng, "_last_mini_demand", None) or {}
        kw = d.get("kw")
        wall = d.get("wall", 0.0)
        if kw is None:
            return None
        # Treat readings older than a few poll gaps as stale (Mini stopped / gone).
        if (_time.time() - wall) > 180.0:
            return None
        return kw
    except Exception:
        return None


def _bcd_demand_sensor():
    """BottlecapDave's current_demand entity (live-power sensor) when BCD is
    detected, else None. Used to auto-adopt BCD as the power sensor when the user
    hasn't configured one of their own — it's a free local feed, no API quota."""
    try:
        import engine as _eng
        bcd = (_eng._detected_integrations or {}).get("bcd") or {}
        return bcd.get("demand_sensor") if bcd.get("found") else None
    except Exception:
        return None


def _ohme_detection():
    """OHME charge-mode detection result, surfaced to the EV card so the hint can
    say whether EMT found an Ohme charge-mode entity (verified off-peak path) or
    fell back to the optimistic path. None-safe; always returns a plain dict."""
    try:
        import engine as _eng
        ohme = (_eng._detected_integrations or {}).get("ohme") or {}
        if ohme.get("found"):
            return {
                "found": True,
                "integration": ohme.get("integration"),
                "charge_mode_entity": ohme.get("charge_mode_entity"),
            }
    except Exception:
        pass
    return {"found": False, "integration": None, "charge_mode_entity": None}


def _effective_power_sensor(main_meta):
    """Resolve the live-power sensor entity: the user's configured power_sensor,
    or — when none is set — BottlecapDave's current_demand (auto-adopted)."""
    return (main_meta or {}).get("power_sensor") or _bcd_demand_sensor()


@app.route("/api/power")
def api_power():
    """Returns live power (kW) from configured power sensor or derived from reads."""
    try:
        from energy_engine_io import load_json as _lj
        from datetime import datetime
        cfg        = load_config()
        block      = _get_store().load_current_block()
        meters_cfg = cfg.get("meters", {}) or {}
        meters_blk = block.get("meters", {}) or {}

        # Find power sensor and sub-meter sensors from config
        power_sensor = bat_sensor = ev_sensor = None
        power_source = None  # e.g. "mini" — Octopus Mini opted in as the live source
        postcode_prefix = None
        _main_power_invert = False
        _main_power_unit = None
        soc_sensors = {}  # {meter_id: {soc, inverter_power}}
        from datetime import datetime as _dt2
        _today = _dt2.now().strftime("%Y-%m-%d")
        for m_id, m_data in meters_cfg.items():
            meta = (m_data or {}).get("meta", {}) or {}
            # Skip retired sub-meters — don't show their cards on Live Power
            if meta.get("sub_meter") and meta.get("retired_at") and meta["retired_at"] <= _today:
                continue
            if not meta.get("sub_meter"):
                power_sensor = meta.get("power_sensor")
                power_source = meta.get("power_source")
                postcode_prefix = meta.get("postcode_prefix")
                # invert / unit-override apply to the user's OWN sensor only.
                if power_sensor:
                    _main_power_invert = bool(meta.get("power_invert", False))
                    _main_power_unit = meta.get("power_unit") or None
            elif _is_meter_type(meta, m_id, "battery"):
                bat_sensor = ((m_data.get("channels") or {}).get("import") or {}).get("read")
                if meta.get("soc_sensor") or meta.get("inverter_power_sensor") or meta.get("pv_power_sensor"):
                    soc_sensors[m_id] = {
                        "soc_entity":    meta.get("soc_sensor"),
                        "power_entity":  meta.get("inverter_power_sensor"),
                        "power_invert":  bool(meta.get("inverter_power_invert", False)),
                        "power_unit":    meta.get("inverter_power_unit") or None,
                        "pv_entity":     meta.get("pv_power_sensor"),
                        "label":         meta.get("device") or m_id,
                        "type":          "battery",
                    }
            elif _is_meter_type(meta, m_id, "ev"):
                ev_sensor = ((m_data.get("channels") or {}).get("import") or {}).get("read")
                if meta.get("device_power_sensor"):
                    soc_sensors[m_id] = {
                        "soc_entity":   None,
                        "power_entity": meta.get("device_power_sensor"),
                        "power_invert": bool(meta.get("device_power_invert", False)),
                        "power_unit":   meta.get("device_power_unit") or None,
                        "label":        meta.get("device") or m_id,
                        "type":         "ev",
                        "v2x":          bool(meta.get("v2x_capable")),
                    }
            elif _is_meter_type(meta, m_id, "heat_pump"):
                if meta.get("device_power_sensor"):
                    soc_sensors[m_id] = {
                        "soc_entity":   None,
                        "power_entity": meta.get("device_power_sensor"),
                        "power_invert": bool(meta.get("device_power_invert", False)),
                        "power_unit":   meta.get("device_power_unit") or None,
                        "label":        meta.get("device") or m_id,
                        "type":         "heat_pump",
                    }

        def sensor_kw(entity_id, invert=False, unit_override=None):
            if not entity_id or not _ha_client:
                return None
            val = _ha_client.get_state(entity_id)
            if val in (None, "unknown", "unavailable"):
                return None
            unit = ""
            try:
                unit = ((_ha_client.get_attributes(entity_id) or {})
                        .get("unit_of_measurement") or "")
            except Exception:
                pass
            # Shared converter: unit attr → kW, override when the declared unit is
            # wrong/absent (a CT sensor labelled kW but emitting W-scale numbers),
            # then invert for sensors wired import-negative. Falls back to the
            # magnitude heuristic when the unit is missing.
            import engine as _eng_pc
            return _eng_pc._power_value_to_kw(val, unit, unit_override, invert)

        def derive_kw(reads):
            if not reads or len(reads) < 2:
                return None
            try:
                r2 = reads[-1]
                r1 = None
                for r in reversed(reads[:-1]):
                    if r["ts"] != r2["ts"] and float(r["value"]) != float(r2["value"]):
                        r1 = r
                        break
                if r1 is None:
                    return None
                t1 = datetime.fromisoformat(r1["ts"])
                t2 = datetime.fromisoformat(r2["ts"])
                dt_hours = (t2 - t1).total_seconds() / 3600.0
                if dt_hours <= 0 or dt_hours > 0.5:
                    return None
                delta_kwh = float(r2["value"]) - float(r1["value"])
                return round(delta_kwh / dt_hours, 3)
            except Exception:
                return None

        # Always derive battery/EV from cumulative reads — they don't have power sensors
        bat_kw = ev_kw = None
        for m_id, m_data in meters_blk.items():
            if not m_data:
                continue
            # Use config meta for type detection — block meta may not have meter_type
            cfg_meta = (meters_cfg.get(m_id) or {}).get("meta", {}) or {}
            ch       = m_data.get("channels", {}) or {}
            if not cfg_meta.get("sub_meter", False):
                continue
            if _is_meter_type(cfg_meta, m_id, "battery"):
                bat_kw = derive_kw(ch.get("import", {}).get("reads", []))
            elif _is_meter_type(cfg_meta, m_id, "ev"):
                ev_kw = derive_kw(ch.get("import", {}).get("reads", []))

        effective_ps = power_sensor or _bcd_demand_sensor()
        mini_active = (not effective_ps) and power_source == "mini" and bool(_mini_device_id())
        if effective_ps:
            # Main meter — direct power sensor: the user's own, or BCD's
            # current_demand auto-adopted (kW after sensor_kw's unit handling).
            # invert / unit-override apply to the user's OWN sensor only.
            _own = (effective_ps == power_sensor)
            net_kw = sensor_kw(effective_ps,
                               invert=(_main_power_invert if _own else False),
                               unit_override=(_main_power_unit if _own else None))
            imp_kw = max(0.0, net_kw)  if net_kw is not None else None
            exp_kw = max(0.0, -net_kw) if net_kw is not None else None
        elif mini_active:
            # Main meter — Octopus Mini live demand (kW), polled by the engine.
            net_kw = _mini_live_demand_kw()
            imp_kw = max(0.0, net_kw)  if net_kw is not None else None
            exp_kw = max(0.0, -net_kw) if net_kw is not None else None
        else:
            # Main meter — derive from cumulative reads
            imp_kw = exp_kw = None
            for m_id, m_data in meters_blk.items():
                if not m_data:
                    continue
                meta   = m_data.get("meta", {}) or {}
                ch     = m_data.get("channels", {}) or {}
                if not meta.get("sub_meter", False):
                    imp_kw = derive_kw(ch.get("import", {}).get("reads", []))
                    exp_kw = derive_kw(ch.get("export", {}).get("reads", []))
                    break
            if imp_kw is not None: imp_kw = max(0.0, imp_kw)
            if exp_kw is not None: exp_kw = max(0.0, exp_kw)

        # Get current import rate from HA state cache
        rate_sensor = None
        try:
            from energy_engine_io import load_json as _lj2
            _cfg2 = load_config()
            for _m in _cfg2.get("meters", {}).values():
                if not (_m.get("meta") or {}).get("sub_meter"):
                    rate_sensor = ((_m.get("channels") or {}).get("import") or {}).get("rate")
                    break
        except Exception:
            pass
        current_rate = None
        if rate_sensor and _ha_client:
            try:
                rv = _ha_client.get_state(rate_sensor)
                if rv not in (None, "unknown", "unavailable"):
                    current_rate = round(float(rv), 6)
            except Exception:
                pass

        # Current generation mix from latest block
        current_mix = []
        try:
            _mix_store = _get_store()
            _mix_rows = _mix_store._conn.execute(
                """SELECT gm.fuel, gm.perc
                   FROM generation_mix gm
                   JOIN blocks b ON b.id = gm.block_id
                   WHERE b.meter_id = 'electricity_main'
                   ORDER BY b.block_start DESC
                   LIMIT 9"""
            ).fetchall()
            current_mix = [{"fuel": r["fuel"], "perc": r["perc"]} for r in _mix_rows]
        except Exception:
            pass

        # Mini availability for the Overview opt-in toggle: a Mini is present AND
        # there's no cheaper live source (no power_sensor entity, no BCD demand).
        mini_present = bool(_mini_device_id())
        bcd_present = False
        try:
            import engine as _eng
            bcd_present = bool(((_eng._detected_integrations or {}).get("bcd") or {}).get("found"))
        except Exception:
            pass
        mini_available = mini_present and not power_sensor and not bcd_present

        return jsonify({
            "import_kw":        imp_kw,
            "export_kw":        exp_kw,
            "battery_kw":       bat_kw,
            "ev_kw":            ev_kw,
            "max_kw":           10,
            "has_power_sensor": bool(effective_ps) or bool(mini_active),
            "mini_active":      bool(mini_active),
            "mini_available":   bool(mini_available),
            "rate":             current_rate,
            "soc_sensors":      _build_soc_response(soc_sensors, _ha_client),
            "generation_mix":   current_mix,
        })
    except Exception as e:
        logger.error("api_power: %s", e)
        return jsonify({"error": str(e)}), 500


@app.route("/api/power-source/mini", methods=["POST"])
def api_power_source_mini():
    """Opt the Octopus Mini in/out as the live-power source on the main meter
    (sets/clears meta.power_source == 'mini'). When on, the Overview gauge is
    driven by polling smartMeterTelemetry — which consumes the GraphQL query
    allowance, so this is strictly opt-in."""
    try:
        data = request.get_json(force=True) or {}
        enabled = bool(data.get("enabled"))
        cfg = load_config()
        main_id = None
        for m_id, m_data in (cfg.get("meters") or {}).items():
            if not ((m_data or {}).get("meta") or {}).get("sub_meter"):
                main_id = m_id
                break
        if not main_id:
            return jsonify({"error": "no main meter configured"}), 404
        meta = cfg["meters"][main_id].setdefault("meta", {})
        if enabled:
            if meta.get("power_sensor"):
                # never override a real device sensor
                return jsonify({"error": "a power sensor is already configured"}), 409
            meta["power_source"] = "mini"
        else:
            meta.pop("power_source", None)
        save_config(cfg)
        logger.info("api_power_source_mini: Mini live source %s",
                    "enabled" if enabled else "disabled")
        return jsonify({"ok": True, "enabled": enabled})
    except Exception as e:
        logger.error("api_power_source_mini: %s", e)
        return jsonify({"error": str(e)}), 500


@app.route("/api/billing")
def api_billing():
    """Return billing totals for Today, This Bill and This Year using fast SQL aggregation."""
    from datetime import datetime, timedelta
    from zoneinfo import ZoneInfo
    import energy_charts as _ec
    try:
        from energy_engine_io import load_json as _lj
        cfg      = load_config()
        store    = _get_store()
        main_meta = {}
        for md in cfg.get("meters", {}).values():
            if not (md.get("meta") or {}).get("sub_meter"):
                main_meta = md.get("meta") or {}
                break
        currency = main_meta.get("currency_symbol", "£")
        tz_name  = main_meta.get("timezone", "UTC")
        billing_day = int(main_meta.get("billing_day") or 1)
        _tz      = ZoneInfo(tz_name)
        now_local = datetime.now(_tz)
        now_naive = now_local.replace(tzinfo=None)

        from block_store import local_date_range_to_utc_bounds, local_date_to_utc_bounds

        def _fmt_total(totals, label_imp, label_exp, start_date=None, end_date=None,
                       utc_s=None, utc_e=None):
            """Format SQL totals into billing card total + rows."""
            imp_cost = round(float(totals["imp_cost"]), 2)
            exp_cost = round(float(totals["exp_cost"]), 2)
            standing = round(float(totals["standing"]), 2)
            # Compute total via BlockStore.compute_period_net —
            # single implementation shared by Live Power, Usage Stats and billing chart.
            if utc_s and utc_e:
                total = store.compute_period_net(utc_s, utc_e, tz_name)
            else:
                total = round(imp_cost + standing - exp_cost, 2)
            rows = []

            # Sub-meter breakdown rows
            sub_rows = []
            sub_kwh_total = 0.0
            sub_cost_total = 0.0
            if start_date and end_date:
                active_period_sq = (
                    "SELECT id FROM config_periods "
                    "WHERE effective_to IS NULL ORDER BY effective_from DESC LIMIT 1"
                )
                _sub_utc_s, _sub_utc_e = local_date_range_to_utc_bounds(start_date, end_date, tz_name)
                sub_cur = store._conn.execute(
                    f"""SELECT m.meter_id,
                               COALESCE(SUM(COALESCE(b.imp_kwh_grid, b.imp_kwh)), 0.0) as kwh,
                               COALESCE(SUM(b.imp_cost), 0.0) as cost
                        FROM blocks b
                        JOIN meters m ON m.meter_id = b.meter_id
                          AND m.config_period_id = ({active_period_sq})
                        WHERE m.is_sub_meter = 1
                          AND b.block_start >= ? AND b.block_start < ?
                        GROUP BY m.meter_id
                        HAVING kwh > 0.001 OR cost > 0.001""",
                    (_sub_utc_s, _sub_utc_e)
                )
                for sr in sub_cur.fetchall():
                    mid   = sr["meter_id"]
                    kwh   = float(sr["kwh"])
                    cost  = round(float(sr["cost"]), 2)
                    meta  = (cfg.get("meters", {}).get(mid, {}).get("meta") or {})
                    device = meta.get("device") or mid
                    sub_rows.append({"label": f"↳ {device} ({kwh:.3f} kWh)",
                                     "cost": cost, "bold": False})
                    sub_kwh_total  += kwh
                    sub_cost_total += cost

            # Get raw grid import (before sub-meter deduction) for correct total
            raw_cur = store._conn.execute(
                f"""SELECT COALESCE(SUM(COALESCE(b.imp_kwh_grid, b.imp_kwh)), 0.0) as raw_kwh,
                           COALESCE(SUM(b.imp_cost), 0.0) as raw_cost
                    FROM blocks b
                    JOIN meters m ON m.meter_id = b.meter_id
                      AND m.config_period_id = ({active_period_sq})
                    WHERE m.is_sub_meter = 0
                      AND b.block_start >= ? AND b.block_start < ?""",
                (_sub_utc_s, _sub_utc_e)
            ).fetchone()
            raw_grid_kwh  = float(raw_cur["raw_kwh"])  if raw_cur else totals["imp_kwh"]
            raw_grid_cost = float(raw_cur["raw_cost"]) if raw_cur else imp_cost

            # Total Import = raw grid import (already includes all consumption)
            total_imp_kwh  = raw_grid_kwh
            total_imp_cost = raw_grid_cost
            # Grid Import = raw grid minus sub-meter attributed portion
            grid_imp_kwh   = max(0.0, raw_grid_kwh  - sub_kwh_total)
            # Preserve a genuinely negative grid cost (Agile plunge-price credit);
            # only floor the sub-over-subtraction artifact (raw cost >= 0).
            _gc = raw_grid_cost - sub_cost_total
            grid_imp_cost  = _gc if raw_grid_cost < 0 else max(0.0, _gc)

            total_imp_cost = round(total_imp_cost, 2)
            grid_imp_cost  = round(grid_imp_cost,  2)
            if total_imp_kwh > 0.001 or total_imp_cost > 0.001:
                rows.append({"label": f"{label_imp} ({total_imp_kwh:.3f} kWh)",
                             "cost": total_imp_cost, "bold": True})
            if grid_imp_kwh > 0.001 or grid_imp_cost > 0.001:
                rows.append({"label": f"Direct import ({grid_imp_kwh:.3f} kWh)",
                             "cost": grid_imp_cost, "bold": False})
            # Sub-meter rows (already rounded when built)
            rows.extend(sub_rows)
            # Export and standing charge
            if totals["exp_kwh"] > 0.001:
                rows.append({"label": f"Grid Export ({totals['exp_kwh']:.3f} kWh)",
                             "cost": -exp_cost, "bold": False})
            if standing > 0.001:
                rows.append({"label": "Standing Charge", "cost": standing, "bold": False})
            # Keep total as raw-sum-round-once (computed above from totals["imp_cost"] etc.)
            # This matches billing chart and Octopus billing methodology.
            # The displayed row values are rounded to 2dp independently, so in edge cases
            # the headline may differ from the sum of visible line items by £0.01 —
            # this is preferable to disagreeing with the billing chart.
            return total, rows

        today_local_date = now_local.date().isoformat()

        # Today — UTC bounds for today's local date
        _today_utc_s, _today_utc_e = local_date_to_utc_bounds(today_local_date, tz_name)
        today_t = store.get_billing_totals_for_utc_range(_today_utc_s, _today_utc_e, tz_name)
        today_total, today_rows = _fmt_total(today_t, "Total Import", "Total Import",
                                              today_local_date, today_local_date,
                                              utc_s=_today_utc_s, utc_e=_today_utc_e)

        # Billing period — find current period from config history
        _bp_periods = _ec.get_billing_periods_from_config_periods(
            store.get_config_periods(), tz=_tz
        )
        _today_date = now_local.date()
        period_start = period_end_excl = None
        for (_bps, _bpe) in _bp_periods:
            if _bps.date() <= _today_date < _bpe.date():
                period_start, period_end_excl = _bps, _bpe
                break
        if period_start is None:
            # No generated period contains today. This happens when there are no
            # periods at all, OR — the reported bug — when the most recent data
            # predates the current billing period ("no current bill" yet, because
            # the generator stops at the last block). In both cases "This Bill"
            # must show the CURRENT period, even with £0 of data so far, not the
            # last historical period (which surfaced as a stale date).
            import calendar as _cal
            bd = billing_day
            _dim = _cal.monthrange(now_local.year, now_local.month)[1]
            if now_local.day >= min(bd, _dim):
                _sy, _sm = now_local.year, now_local.month
            else:
                _sm = now_local.month - 1 or 12
                _sy = now_local.year if now_local.month > 1 else now_local.year - 1
            _sdim = _cal.monthrange(_sy, _sm)[1]
            period_start = now_local.replace(
                year=_sy, month=_sm, day=min(bd, _sdim),
                hour=0, minute=0, second=0, microsecond=0)
            _ey, _em = (_sy + 1, 1) if _sm == 12 else (_sy, _sm + 1)
            _edim = _cal.monthrange(_ey, _em)[1]
            period_end_excl = period_start.replace(
                year=_ey, month=_em, day=min(bd, _edim))

        # Derive local date strings for period boundaries
        if hasattr(period_start, 'tzinfo') and period_start.tzinfo is not None:
            period_start_date = period_start.astimezone(_tz).date()
        else:
            period_start_date = period_start.date()

        if period_end_excl is not None:
            _end_incl = period_end_excl - timedelta(days=1)
            if hasattr(_end_incl, 'tzinfo') and _end_incl.tzinfo is not None:
                _end_incl_date = _end_incl.astimezone(_tz).date()
            else:
                _end_incl_date = _end_incl.date()
            month_period_end_str = _end_incl_date.strftime('%d %b %Y')
        else:
            month_period_end_str = now_local.strftime('%d %b %Y')

        _month_utc_s, _month_utc_e = local_date_range_to_utc_bounds(
            period_start_date.isoformat(), today_local_date, tz_name
        )
        month_t = store.get_billing_totals_for_utc_range(_month_utc_s, _month_utc_e, tz_name)

        month_total, month_rows = _fmt_total(month_t, "Total Import", "Total Import",
                                              period_start_date.isoformat(), today_local_date,
                                              utc_s=_month_utc_s, utc_e=_month_utc_e)

        # Calendar year — from Jan 1 local to today local
        year_start_date = now_local.date().replace(month=1, day=1).isoformat()
        _year_utc_s, _year_utc_e = local_date_range_to_utc_bounds(
            year_start_date, today_local_date, tz_name
        )
        year_t = store.get_billing_totals_for_utc_range(_year_utc_s, _year_utc_e, tz_name)
        year_total, year_rows = _fmt_total(year_t, "Total Import", "Total Import",
                                            year_start_date, today_local_date,
                                            utc_s=_year_utc_s, utc_e=_year_utc_e)

        def fmt_rows(rows):
            return [{"label": r["label"], "cost": r["cost"], "bold": r.get("bold", False)}
                    for r in rows]

        resp = jsonify({
            "currency":     currency,
            "today_total":  today_total,
            "today_rows":   fmt_rows(today_rows),
            "today_date":   now_local.strftime("%d %b %Y"),
            "month_total":  month_total,
            "month_rows":   fmt_rows(month_rows),
            "month_period": f"{period_start.strftime('%d %b')} → {month_period_end_str}",
            "year_total":   year_total,
            "year_rows":    fmt_rows(year_rows),
            "year_period":  f"1 Jan → {now_local.strftime('%d %b %Y')}",
        })
        resp.headers["Cache-Control"] = "no-store, no-cache, must-revalidate"
        resp.headers["Pragma"]        = "no-cache"
        resp.headers["Expires"]       = "0"
        return resp
    except Exception as e:
        logger.error("api_billing: %s", e)
        return jsonify({"error": str(e)}), 500


@app.route("/api/carbon")
def api_carbon():
    """Fetch 24-hour carbon intensity forecast from National Grid API."""
    import urllib.request
    import urllib.error
    try:
        from energy_engine_io import load_json as _lj
        cfg = load_config()
        postcode = None
        for m_data in cfg.get("meters", {}).values():
            meta = (m_data or {}).get("meta", {}) or {}
            if not meta.get("sub_meter"):
                postcode = meta.get("postcode_prefix", "").strip().upper().split()[0] if meta.get("postcode_prefix", "").strip() else ""
                break
        if not postcode:
            return jsonify({"error": "no_postcode"}), 404

        from datetime import datetime, timezone
        now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%MZ")
        url = f"https://api.carbonintensity.org.uk/regional/intensity/{now_iso}/fw48h/postcode/{postcode}"
        req = urllib.request.Request(url, headers={"Accept": "application/json"})
        with urllib.request.urlopen(req, timeout=8) as resp:
            data = json.loads(resp.read())

        slots = []
        raw = data.get("data", [])
        # Handle dict shape: {data: {regionid, postcode, data: [{from, to, intensity}]}}
        if isinstance(raw, dict):
            for slot in raw.get("data", []):
                slots.append({
                    "from":      slot.get("from"),
                    "to":        slot.get("to"),
                    "intensity": slot.get("intensity", {}).get("forecast"),
                    "index":     slot.get("intensity", {}).get("index"),
                })
        # Handle list shape
        elif isinstance(raw, list) and raw:
            first = raw[0]
            if "data" in first:
                # [{regionid, postcode, data: [{from, to, intensity}]}]
                for slot in first.get("data", []):
                    slots.append({
                        "from":      slot.get("from"),
                        "to":        slot.get("to"),
                        "intensity": slot.get("intensity", {}).get("forecast"),
                        "index":     slot.get("intensity", {}).get("index"),
                    })
            elif "from" in first:
                # flat [{from, to, intensity}]
                for slot in raw:
                    slots.append({
                        "from":      slot.get("from"),
                        "to":        slot.get("to"),
                        "intensity": slot.get("intensity", {}).get("forecast"),
                        "index":     slot.get("intensity", {}).get("index"),
                    })
        slots = slots[:96]  # cap at 48 hours

        return jsonify({"postcode": postcode, "slots": slots})
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="replace")[:500]
        logger.warning("api_carbon: HTTP %s — %s", e.code, body)
        return jsonify({"error": f"http_{e.code}", "detail": body}), 503
    except urllib.error.URLError as e:
        logger.warning("api_carbon: network error: %s", e)
        return jsonify({"error": "network_error"}), 503
    except Exception as e:
        logger.error("api_carbon: type=%s repr=%r str=%s", type(e).__name__, e, e)
        return jsonify({"error": str(e), "type": type(e).__name__, "repr": repr(e)}), 500


# ── Power history + carbon intensity ─────────────────────────────────────────

@app.route("/api/power/history")
def api_power_history():
    """
    Return rolling 48-hour net power and carbon intensity history.
    Used by the Live Power rolling chart.
    Returns: { rows: [{captured_at, net_kw, intensity}] }
    """
    try:
        hours = min(int(request.args.get("hours", 48)), 48)
        store = _get_store()
        rows  = store.get_power_history(hours=hours)
        return jsonify({"rows": rows})
    except Exception as e:
        logger.error("api_power_history: %s", e)
        return jsonify({"error": str(e)}), 500


@app.route("/api/power/mix-history")
def api_power_mix_history():
    """
    Return generation mix for the last 48 hours at CI-tick resolution (~15 min).
    Uses mix_history table (written per CI tick, independent of block size).
    Returns: { slots: [{captured_at, fuels: {wind, solar, gas, ...}}] }
    """
    try:
        store = _get_store()
        slots = store.get_mix_history(hours=48)
        return jsonify({"slots": slots})
    except Exception as e:
        logger.error("api_power_mix_history: %s", e)
        return jsonify({"error": str(e)}), 500


@app.route("/api/engine/meter-reset-detected")
def api_meter_reset_detected():
    """Returns whether a meter read reset was detected (possible replacement/move).
    Clears the flag on read — call once per page load."""
    try:
        detected = engine.get_and_clear_meter_reset()
        if detected:
            logger.warning("api_meter_reset_detected: meter reset flag was set — serving advisory to UI")
        return jsonify({"detected": detected})
    except Exception as e:
        return jsonify({"detected": False, "error": str(e)})


@app.route("/api/meter/<meter_id>/retire", methods=["POST"])
def api_retire_meter(meter_id: str):
    """Retire a sub-meter from a given date. Preserves all historical data."""
    try:
        data = request.get_json(force=True) or {}
        retired_at = data.get("retired_at")
        retired_reason = data.get("retired_reason", "")
        if not retired_at:
            return jsonify({"error": "retired_at is required (YYYY-MM-DD)"}), 400
        store = _get_store()
        # Verify it's a sub-meter
        row = store._conn.execute(
            "SELECT is_sub_meter FROM meters WHERE meter_id = ?", (meter_id,)
        ).fetchone()
        if not row:
            return jsonify({"error": f"Meter '{meter_id}' not found"}), 404
        if not row["is_sub_meter"]:
            return jsonify({"error": "Cannot retire the main meter"}), 400
        store.retire_meter(meter_id, retired_at, retired_reason)
        try:
            import engine as _eng_retire
            if hasattr(_eng_retire, "reset_store"):
                _eng_retire.reset_store()
        except Exception:
            pass
        logger.info("api_retire_meter: %s retired at %s (%s)", meter_id, retired_at, retired_reason)
        return jsonify({"ok": True, "meter_id": meter_id, "retired_at": retired_at})
    except Exception as e:
        logger.error("api_retire_meter: %s", e)
        return jsonify({"error": str(e)}), 500


@app.route("/api/meter/<meter_id>/unretire", methods=["POST"])
def api_unretire_meter(meter_id: str):
    """Clear retirement status from a sub-meter.
    Returns 409 Conflict if the sensor is already in use by another active meter."""
    try:
        store = _get_store()
        store.unretire_meter(meter_id)
        try:
            import engine as _eng_unretire
            if hasattr(_eng_unretire, "reset_store"):
                _eng_unretire.reset_store()
        except Exception:
            pass
        logger.info("api_unretire_meter: %s unretired", meter_id)
        return jsonify({"ok": True, "meter_id": meter_id})
    except ValueError as e:
        # Sensor conflict — return 409 so the UI can show a meaningful message
        logger.warning("api_unretire_meter: conflict — %s", e)
        return jsonify({"error": str(e), "conflict": True}), 409
    except Exception as e:
        logger.error("api_unretire_meter: %s", e)
        return jsonify({"error": str(e)}), 500


@app.route("/api/meter/main/reset", methods=["POST"])
def api_meter_main_reset():
    """
    Nuclear option — backup the DB then wipe it entirely.
    Used when user deletes the main meter (electricity_main).
    Triggers engine restart via engine_startup on next connection.
    """
    try:
        import shutil, time
        store = _get_store()

        # Create backup first
        try:
            _backup_to_share(store)
            logger.info("api_meter_main_reset: backup created")
        except Exception as be:
            logger.warning("api_meter_main_reset: backup failed: %s", be)

        # Close store connection
        db_path = store._path
        store._conn.close()

        # Delete the DB files
        for suffix in ["", "-wal", "-shm"]:
            p = db_path + suffix
            if os.path.exists(p):
                os.remove(p)
                logger.info("api_meter_main_reset: removed %s", p)

        # Also clear meters_config.json
        cfg_p = config_path()
        if os.path.exists(cfg_p):
            import json as _j
            with open(cfg_p, "w") as f:
                _j.dump({"meters": {}}, f, indent=2)

        # Remove generated chart HTML — otherwise the old charts linger and the
        # Billing/Charts pages show stale data from the wiped setup.
        _chart_dir = CHART_DIR or DATA_DIR
        if _chart_dir:
            for _cf in ("net_heatmap.html", "daily_usage.html"):
                _cp = os.path.join(_chart_dir, _cf)
                try:
                    if os.path.exists(_cp):
                        os.remove(_cp)
                        logger.info("api_meter_main_reset: removed stale chart %s", _cf)
                except OSError as _ce:
                    logger.warning("api_meter_main_reset: could not remove %s: %s",
                                   _cf, _ce)

        # Clear stored API credentials — they live outside the DB (so they're
        # not in backups), which means a DB wipe alone leaves them orphaned and
        # the API wrongly re-activates on next start. A reset means start over.
        try:
            import engine as _eng
            _eng.save_kraken_credentials("", None)   # empty key clears the file
            logger.info("api_meter_main_reset: cleared stored API credentials")
        except Exception as _ke:
            logger.warning("api_meter_main_reset: could not clear credentials: %s", _ke)

        logger.info("api_meter_main_reset: database wiped — restarting addon")

        # Trigger addon restart via supervisor API — runs in background thread
        # so the HTTP response is returned before the process exits
        import threading, urllib.request as _ur, json as _j2
        def _restart():
            import time; time.sleep(1)
            try:
                token = os.environ.get("SUPERVISOR_TOKEN", "")
                req = _ur.Request(
                    "http://supervisor/addons/self/restart",
                    data=b"{}",
                    headers={"Authorization": "Bearer " + token,
                             "Content-Type": "application/json"},
                    method="POST"
                )
                _ur.urlopen(req, timeout=10)
            except Exception as re:
                logger.warning("api_meter_main_reset: supervisor restart failed: %s — using sys.exit", re)
                import sys; sys.exit(0)
        threading.Thread(target=_restart, daemon=True).start()

        return jsonify({"ok": True})
    except Exception as e:
        logger.error("api_meter_main_reset: %s", e)
        return jsonify({"error": str(e)}), 500


@app.route("/api/meter/<meter_id>/delete-data", methods=["POST"])
def api_meter_delete_data(meter_id: str):
    """
    Atomically remove a meter from config AND delete all its data.
    Body: { config: <full config object> }
    Both operations run in a single DB transaction — all-or-nothing.
    Returns: { deleted: {blocks, sub_meter_history, current_reads}, ok: true }
    """
    try:
        store    = _get_store()
        payload  = request.get_json(force=True) or {}
        new_cfg  = payload.get("config")
        deleted  = {}

        # Pause engine before modifying DB — prevents mid-tick inconsistency
        try:
            import engine as _eng_d
            _eng_d.pause_engine()
        except Exception:
            pass

        with store._conn:
            # 1. Delete all data for this meter
            cur = store._conn.execute(
                "DELETE FROM blocks WHERE meter_id = ?", (meter_id,)
            )
            deleted["blocks"] = cur.rowcount

            cur = store._conn.execute(
                "DELETE FROM sub_meter_history WHERE meter_id = ?", (meter_id,)
            )
            deleted["sub_meter_history"] = cur.rowcount

            cur = store._conn.execute(
                "DELETE FROM current_reads WHERE meter_id = ?", (meter_id,)
            )
            deleted["current_reads"] = cur.rowcount

            # Remove from current_block JSON if present
            try:
                cb_row = store._conn.execute(
                    "SELECT id, last_checkpoint FROM current_block WHERE id = 1"
                ).fetchone()
                if cb_row and cb_row["last_checkpoint"]:
                    import json as _json
                    cb = _json.loads(cb_row["last_checkpoint"])
                    if meter_id in (cb.get("meters") or {}):
                        del cb["meters"][meter_id]
                        store._conn.execute(
                            "UPDATE current_block SET last_checkpoint = ? WHERE id = 1",
                            (_json.dumps(cb),)
                        )
                        deleted["current_block"] = 1
            except Exception:
                pass

            # 2. Save updated config (meter removed) in same transaction
            # Must delete existing meter rows first — _write_meters only upserts,
            # it won't remove meters that are no longer in the config.
            if new_cfg:
                period_id = store.get_current_config_period_id()
                old_meter_ids = [r["id"] for r in store._conn.execute(
                    "SELECT id FROM meters WHERE config_period_id=?", (period_id,)
                ).fetchall()]
                for old_mid in old_meter_ids:
                    store._conn.execute(
                        "DELETE FROM meter_channels WHERE meter_id=?", (old_mid,)
                    )
                store._conn.execute(
                    "DELETE FROM meters WHERE config_period_id=?", (period_id,)
                )
                store._write_meters(new_cfg, period_id)

        logger.info("api_meter_delete_data: deleted %s for meter %s", deleted, meter_id)

        # Restart engine to pick up config change
        try:
            import asyncio as _asyncio
            from engine import engine_startup as _engine_startup_d
            if _event_loop and _event_loop.is_running() and _ha_client:
                _asyncio.run_coroutine_threadsafe(_engine_startup_d(_ha_client), _event_loop)
        except Exception:
            pass

        # Regenerate the pre-built charts so the removed meter's data disappears
        # immediately, rather than after the next block finalises (same regen gap
        # as the corrections and restore paths).
        _regen_charts_safely()

        return jsonify({"ok": True, "deleted": deleted, "meter_id": meter_id})
    except Exception as e:
        logger.error("api_meter_delete_data: %s", e)
        return jsonify({"error": str(e)}), 500


@app.route("/api/sub-meter/history")
def api_sub_meter_history():
    """
    Return rolling 48-hour SoC and inverter power history for a battery sub-meter.
    Used by the Live Power page to auto-scale the inverter gauge.
    Returns: { rows: [{captured_at, soc_pct, inverter_kw}], max_kw: float }
    """
    try:
        meter_id = request.args.get("meter_id", "")
        hours    = min(int(request.args.get("hours", 48)), 48)
        if not meter_id:
            return jsonify({"error": "meter_id required"}), 400
        store = _get_store()
        rows  = store.get_sub_meter_history(meter_id, hours=hours)
        # Compute max abs inverter kW for gauge scaling.
        # Use 90th percentile — spurious sensor spikes (e.g. Solax reporting
        # impossible values) can make up > 5% of readings, causing p95 to land
        # on a spike. p90 is robust against up to 10% bad readings.
        # Also clamp to 20 kW — no residential inverter exceeds this; values
        # above it are always sensor errors and should not inflate the scale.
        kw_vals = [abs(r["inverter_kw"]) for r in rows
                   if r.get("inverter_kw") is not None and abs(r["inverter_kw"]) <= 20.0]
        max_kw = 5.0  # minimum scale
        if kw_vals:
            kw_vals.sort()
            p90_idx = min(int(len(kw_vals) * 0.90), len(kw_vals) - 1)
            max_kw = max(max_kw, kw_vals[p90_idx])
        return jsonify({"rows": rows, "max_kw": round(max_kw, 1)})
    except Exception as e:
        logger.error("api_sub_meter_history: %s", e)
        return jsonify({"error": str(e)}), 500


@app.route("/api/carbon/current")
def api_carbon_current():
    """
    Return the current carbon intensity from the stored carbon_intensity table.
    Falls back to the live National Grid API call if no stored data exists.
    Returns: { intensity, ci_index, postcode, source } or { error }
    """
    try:
        store    = _get_store()
        cfg      = load_config()
        postcode = None
        for m_data in cfg.get("meters", {}).values():
            meta = (m_data or {}).get("meta", {}) or {}
            if not meta.get("sub_meter"):
                postcode = meta.get("postcode_prefix", "").strip().upper().split()[0] if meta.get("postcode_prefix", "").strip() else ""
                break

        if not postcode:
            return jsonify({"error": "no_postcode"}), 404

        from datetime import datetime, timezone
        now = datetime.now(timezone.utc).replace(tzinfo=None).isoformat()
        row = store.get_nearest_carbon_intensity(now, postcode)
        if row:
            return jsonify({
                "intensity": row["intensity"],
                "ci_index":  row["ci_index"],
                "postcode":  postcode,
                "source":    "stored",
            })
        return jsonify({"error": "no_data", "postcode": postcode}), 404
    except Exception as e:
        logger.error("api_carbon_current: %s", e)
        return jsonify({"error": str(e)}), 500


@app.route("/data-management")
def data_management_page():
    return render_template("data_management.html", active="data_management")


@app.route("/historical-probe")
def historical_probe_page():
    """Beta, read-only: recorder long-term-statistics probe (historical-import
    spike). Lets the user pick a sensor per device and characterise what history
    is available. Creates nothing."""
    return render_template("historical_probe.html", active="data_management")


@app.route("/billing-history")
def billing_history_page():
    return render_template("billing_history.html", active="settings")


@app.route("/bill-rounding")
def bill_rounding_page():
    # BL-24: opt-in bill-style rounding for the Billing summary.
    return render_template("bill_rounding.html", active="settings")


@app.route("/delete-blocks")
def delete_blocks_page():
    return render_template("delete_blocks.html", active="data_management")


@app.route("/corrections")
def corrections_page():
    return render_template("corrections.html", active="data_management")


@app.route("/fill-history")
def fill_history_page():
    """Landing hub for the two 'bring data in / fill gaps' tools — Historical
    Import (supplier API / CSV backfill + gap fill) and Device History (recorder
    attribution). Reached from the Data Management topbar."""
    return render_template("fill_history.html", active="data_management")


# ── Chart file serving ────────────────────────────────────────────────────────

@app.route("/charts/net_heatmap.html")
def serve_heatmap():
    p = os.path.join(CHART_DIR, "net_heatmap.html")
    if not os.path.exists(p):
        return "Chart not yet generated", 404
    resp = send_file(p)
    resp.headers["Cache-Control"] = "no-cache, must-revalidate"
    resp.headers["Pragma"] = "no-cache"
    return resp


@app.route("/charts/daily_usage.html")
def serve_daily():
    p = os.path.join(CHART_DIR, "daily_usage.html")
    if not os.path.exists(p):
        return "Chart not yet generated", 404
    resp = send_file(p)
    resp.headers["Cache-Control"] = "no-cache, must-revalidate"
    resp.headers["Pragma"] = "no-cache"
    return resp


# ── Lovelace-friendly chart endpoints ─────────────────────────────────────────
# These endpoints serve the chart HTML with:
#   - Aggressive no-cache headers so HA never fixes the URL in time
#   - A meta refresh so the Lovelace web card auto-updates (the card's own
#     refresh setting is unreliable in HA)
# Use these URLs in Lovelace panel_iframe or webpage cards instead of the
# plain /charts/*.html URLs.

_LOVELACE_REFRESH = 130  # seconds

def _serve_lovelace_chart(filename):
    """Read chart HTML from disk, inject meta refresh + no-cache, return response."""
    p = os.path.join(CHART_DIR, filename)
    if not os.path.exists(p):
        return "Chart not yet generated — visit the EMT Charts page first.", 404
    with open(p, "r", encoding="utf-8") as f:
        html = f.read()
    # Inject meta refresh immediately after <head> (or <html> if no <head>)
    refresh_tag = (
        f'<meta http-equiv="refresh" content="{_LOVELACE_REFRESH}">\n'
        f'<meta http-equiv="Cache-Control" content="no-cache, no-store, must-revalidate">\n'
        f'<meta http-equiv="Pragma" content="no-cache">\n'
        f'<meta http-equiv="Expires" content="0">\n'
    )
    if "<head>" in html:
        html = html.replace("<head>", "<head>\n" + refresh_tag, 1)
    else:
        html = refresh_tag + html
    resp = make_response(html)
    resp.headers["Content-Type"]  = "text/html; charset=utf-8"
    resp.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
    resp.headers["Pragma"]        = "no-cache"
    resp.headers["Expires"]       = "0"
    return resp


@app.route("/lovelace/billing")
def lovelace_billing():
    """Billing chart — Lovelace-friendly URL with auto-refresh."""
    return _serve_lovelace_chart("daily_usage.html")


@app.route("/lovelace/heatmap")
def lovelace_heatmap():
    """Net energy heatmap — Lovelace-friendly URL with auto-refresh."""
    return _serve_lovelace_chart("net_heatmap.html")


def _aggregate_block_rows(raw_rows, bucket_of, standing_scope="bucket",
                          billing_day_fallback=1):
    """Billing-accurate per-block aggregation shared by the Usage Stats endpoints
    (/blocks-summary buckets by local day; /blocks-day by half-hour slot). Buckets
    by ``bucket_of(block_start)`` and applies the SAME rate-keyed direct-cost
    subtraction, sub-meter split and carbon apportioning as before — this is a pure
    extraction, so the day path is byte-identical (guarded by a golden-output check
    + test_usage_stats_vs_billing).

    ``standing_scope='bucket'`` books the standing charge as the first non-zero
    within EACH bucket (the daily rule). ``'global'`` books it once across ALL rows
    (used by the HH view, whose buckets are slots of one day, so the day's single
    standing charge lands on one slot and the slots still sum to it).

    Returns ``{bucket_key: agg}`` with main ``imp_cost`` already resolved. Row rows
    must carry: block_start, meter_id, is_sub_meter, imp_kwh, imp_kwh_grid,
    imp_kwh_remainder, imp_rate, imp_cost, exp_kwh, exp_cost, standing_charge,
    carbon_g (and optionally billing_day)."""
    _day_data = {}
    _global_sc_done = False
    for _r in raw_rows:
        _bk = bucket_of(_r["block_start"])
        if _bk not in _day_data:
            try:
                _bd_val = _r["billing_day"] if "billing_day" in _r.keys() else None
            except Exception:
                _bd_val = None
            _day_data[_bk] = {
                "main":        {"imp_kwh": 0.0, "imp_cost": 0.0, "exp_kwh": 0.0, "exp_cost": 0.0,
                                "carbon_g": None, "cg_imp": None, "cg_exp": None,
                                "ci_abs_carbon": 0.0, "ci_abs_net_kwh": 0.0,
                                "_main_by_rate": {}, "_sub_by_rate": {}},
                "subs":        {},
                "standing":    0.0,
                "billing_day": int(_bd_val or billing_day_fallback),
            }
        _dd = _day_data[_bk]
        _is_sub = bool(_r["is_sub_meter"])
        _imp = float(_r["imp_kwh"] or 0)
        _exp = float(_r["exp_kwh"] or 0)
        _cost_imp = float(_r["imp_cost"] or 0)
        _cost_exp = float(_r["exp_cost"] or 0)
        _sc = float(_r["standing_charge"] or 0)
        _cg = float(_r["carbon_g"]) if _r["carbon_g"] is not None else None
        if not _is_sub:
            _m = _dd["main"]
            _rate = round(float(_r["imp_rate"] or 0), 6)
            if _r["imp_kwh_remainder"] is not None:
                _m_imp = float(_r["imp_kwh_remainder"])
            elif _r["imp_kwh_grid"] is not None:
                _m_imp = float(_r["imp_kwh_grid"])
            else:
                _m_imp = _imp
            if _rate not in _dd["main"]["_main_by_rate"]:
                _dd["main"]["_main_by_rate"][_rate] = {"kwh": 0.0, "cost": 0.0}
            _dd["main"]["_main_by_rate"][_rate]["kwh"]  += _m_imp
            _dd["main"]["_main_by_rate"][_rate]["cost"] += _cost_imp
            _m["imp_kwh"] += _m_imp
            _m["exp_kwh"]  += _exp
            _m["exp_cost"] += _cost_exp
            if _sc > 0:
                if standing_scope == "global":
                    if not _global_sc_done:
                        _dd["standing"] = _sc
                        _global_sc_done = True
                elif not _dd["standing"]:
                    _dd["standing"] = _sc
            if _cg is not None:
                _m["carbon_g"] = (_m["carbon_g"] or 0.0) + _cg
                _b_net = _imp - _exp
                if _b_net != 0:
                    _b_int = abs(_cg / _b_net)
                    _m["cg_imp"] = (_m["cg_imp"] or 0.0) + _imp * _b_int
                    _m["cg_exp"] = (_m["cg_exp"] or 0.0) + _exp * _b_int
                    _m["ci_abs_carbon"]  += abs(_cg)
                    _m["ci_abs_net_kwh"] += abs(_b_net)
                elif _exp == 0:
                    _m["cg_imp"] = (_m["cg_imp"] or 0.0) + abs(_cg)
                    _m["ci_abs_carbon"]  += abs(_cg)
                    _m["ci_abs_net_kwh"] += _imp
                else:
                    _m["cg_exp"] = (_m["cg_exp"] or 0.0) + abs(_cg)
                    _m["ci_abs_carbon"]  += abs(_cg)
                    _m["ci_abs_net_kwh"] += _exp
        else:
            _mid = _r["meter_id"]
            if _mid not in _dd["subs"]:
                _dd["subs"][_mid] = {"imp_kwh": 0.0, "imp_cost": 0.0, "exp_kwh": 0.0, "exp_cost": 0.0, "carbon_g": None}
            _s = _dd["subs"][_mid]
            # Grid portion only; a real 0 (fully solar/battery-charged block) must
            # stay 0, not fall back to total consumption (see _aggregate_usage).
            _grid = _r["imp_kwh_grid"]
            _sub_imp = float(_grid) if _grid is not None else _imp
            _s["imp_kwh"]  += _sub_imp
            _s["imp_cost"] += _cost_imp
            _s["exp_kwh"]  += _exp
            _s["exp_cost"] += _cost_exp
            if _cg is not None:
                _s["carbon_g"] = (_s["carbon_g"] or 0.0) + _cg
            _rate = round(float(_r["imp_rate"] or 0), 6)
            if _rate not in _dd["main"]["_sub_by_rate"]:
                _dd["main"]["_sub_by_rate"][_rate] = {"kwh": 0.0, "cost": 0.0}
            _dd["main"]["_sub_by_rate"][_rate]["kwh"]  += _sub_imp
            _dd["main"]["_sub_by_rate"][_rate]["cost"] += _cost_imp

    for _ld, _dd in _day_data.items():
        _m = _dd["main"]
        _direct_cost = 0.0
        for _rate, _mv in (_m.get("_main_by_rate") or {}).items():
            _sv = (_m.get("_sub_by_rate") or {}).get(_rate, {"kwh": 0.0, "cost": 0.0})
            # max(0) guards a sub-meter's power-integration cost OVER-subtracting a
            # NON-negative main into a spurious negative remainder. A genuinely
            # negative main cost (Agile plunge-price CREDIT) must survive, so only
            # clamp when the main slot's cost is >= 0.
            _d = _mv["cost"] - _sv["cost"]
            _direct_cost += _d if _mv["cost"] < 0 else max(0.0, _d)
        _m["imp_cost"] = _direct_cost
    return _day_data


def _bucket_to_row_values(_dd):
    """The value fields of one aggregated bucket (from _aggregate_block_rows), in the
    shape barAggRows consumes. Pure extraction of the /blocks-summary row build, so
    it's byte-identical; each endpoint merges these with its own identity fields
    (year/month/day for the day view, slot/label for HH)."""
    _m = _dd["main"]
    _avg_intensity = (round(_m["ci_abs_carbon"] / _m["ci_abs_net_kwh"], 1)
                      if _m["ci_abs_net_kwh"] > 0 else None)
    _sub_carbon_for_remainder = sum(
        float(st["carbon_g"]) for st in _dd["subs"].values()
        if st["carbon_g"] is not None and st["carbon_g"] != 0.0)
    _cg_imp = _m["cg_imp"]
    _main_carbon_remainder = (round(_cg_imp - _sub_carbon_for_remainder, 4)
                              if _cg_imp is not None else None)
    meters_out = {"electricity_main": {
        "imp_kwh":      round(_m["imp_kwh"],  4),
        "imp_cost":     round(_m["imp_cost"], 4),
        "exp_kwh":      round(_m["exp_kwh"],  4),
        "exp_cost":     round(_m["exp_cost"], 4),
        "carbon_g":     _main_carbon_remainder,
        "carbon_g_imp":  round(_cg_imp, 4) if _cg_imp is not None else None,
        "carbon_g_exp":  round(_m["cg_exp"], 4) if _m["cg_exp"] is not None else None,
    }}
    for _mid, _st in _dd["subs"].items():
        meters_out[_mid] = {
            "imp_kwh":  round(_st["imp_kwh"],  3),
            "imp_cost": round(_st["imp_cost"], 4),
            "exp_kwh":  round(_st["exp_kwh"],  3),
            "exp_cost": round(_st["exp_cost"], 4),
            "carbon_g": round(_st["carbon_g"], 4) if _st["carbon_g"] is not None and _st["carbon_g"] != 0.0 else None,
        }
    _main_imp_kwh  = round(_m["imp_kwh"],  4)
    _main_imp_cost = round(_m["imp_cost"], 4)
    _main_exp_kwh  = round(_m["exp_kwh"],  4)
    _main_exp_cost = round(_m["exp_cost"], 4)
    meters_out["electricity_main"]["imp_kwh"]  = _main_imp_kwh
    meters_out["electricity_main"]["imp_cost"] = _main_imp_cost
    meters_out["electricity_main"]["exp_kwh"]  = _main_exp_kwh
    meters_out["electricity_main"]["exp_cost"] = _main_exp_cost
    return {
        "standing":  round(_dd["standing"], 4),
        "meters":    meters_out,
        "imp_kwh":  round(_main_imp_kwh  + sum(m["imp_kwh"]  for mid, m in meters_out.items() if mid != "electricity_main"), 4),
        "exp_kwh":  round(_main_exp_kwh,  4),
        "imp_cost": round(_main_imp_cost + sum(m["imp_cost"] for mid, m in meters_out.items() if mid != "electricity_main"), 4),
        "exp_cost": round(_main_exp_cost, 4),
        "net_cost": round(
            round(_main_imp_cost + sum(m["imp_cost"] for mid, m in meters_out.items() if mid != "electricity_main"), 4)
            + round(_dd["standing"], 4)
            - round(_main_exp_cost, 4),
            2),
        "carbon_g_net":   round(_m["carbon_g"], 4) if _m["carbon_g"] is not None else None,
        "carbon_g_total": round(_m["carbon_g"], 4) if _m["carbon_g"] is not None else None,
        "avg_intensity":  _avg_intensity,
    }


def _blocks_data_version(store) -> str:
    """A cheap token that changes whenever the block data changes — row COUNT +
    newest block_start + a value fingerprint (summed kWh / cost / carbon). Lets the
    Charts and Usage-Stats UIs skip a rebuild on tab-focus/poll when nothing has
    changed (no needless blank) and refresh promptly when it has (e.g. a new live
    half-hour finalised).

    The value fingerprint is what catches an **in-place** edit that leaves the row
    count and newest block untouched — a reprice/settlement, a carbon backfill, or a
    device attribution that rewrites historical blocks (e.g. healing a sub-meter
    zero-hole moves energy off the house remainder onto the device). Without it,
    those edits were invisible to the token and the client kept serving its stale
    cache until the TTL expired. It sums **both import and export** kWh + cost (plus
    carbon) — export settles later than import, so an export-only DCC settlement
    moves exp_kwh/exp_cost but not the import figures, and must still bust the cache.
    Every summed column lives in idx_blocks_insights, so this stays a single
    index-only scan (~20ms over ~130k rows)."""
    row = store._conn.execute(
        "SELECT COUNT(*) AS c, MAX(block_start) AS m, "
        "ROUND(COALESCE(SUM(imp_kwh),  0), 3) AS ik, "
        "ROUND(COALESCE(SUM(imp_cost), 0), 2) AS ic, "
        "ROUND(COALESCE(SUM(exp_kwh),  0), 3) AS ek, "
        "ROUND(COALESCE(SUM(exp_cost), 0), 2) AS ec, "
        "ROUND(COALESCE(SUM(carbon_g), 0), 0) AS cg "
        "FROM blocks").fetchone()
    # Rendered-chart freshness: the Billing (daily) and Heatmap tabs serve the
    # PRE-RENDERED files, and a config-period change (or settlement) regenerates them
    # OFF the loop a moment AFTER the DB write. Keying the token on the DB alone moved
    # it at write time — before the file was rewritten — so the Charts page reloaded
    # the STALE file, latched the new token, and then didn't refresh again until the
    # next finalise moved the token. Fingerprinting the rendered files' mtimes advances
    # the token exactly when the new HTML actually lands, so the reload picks up fresh
    # content — and this also covers a config change (billing-day / add / remove), since
    # every config mutation schedules a regen that rewrites these files.
    _mt = []
    for _cf in ("daily_usage.html", "net_heatmap.html"):
        try:
            _mt.append(f"{os.path.getmtime(os.path.join(CHART_DIR, _cf)):.3f}")
        except (OSError, TypeError):   # missing file, or CHART_DIR unset (tests)
            _mt.append("0")
    return (f"{row['c']}:{row['m'] or ''}:"
            f"{row['ik']}:{row['ic']}:{row['ek']}:{row['ec']}:{row['cg']}:"
            f"{':'.join(_mt)}")


@app.route("/api/charts/data-version")
def api_charts_data_version():
    """Lightweight change token for the Charts UI (see _blocks_data_version)."""
    try:
        return jsonify({"version": _blocks_data_version(_get_store())})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/charts/blocks-summary")
def api_blocks_summary():
    """Return billing-accurate per-period data for the Usage Stats bar chart.

    Strategy:
    - Main meter grid remainder: from calculate_billing_summary_for_period
      (billing-accurate, handles sub-meter subtraction correctly)
    - Sub-meters: aggregated directly from blocks by meter_id (avoids fragile
      display-key reverse-mapping)
    - Standing charge: from billing summary (once per day, correct)
    - Export: from blocks directly by meter_id
    """
    try:
        from energy_engine_io import load_json as _lj
        from zoneinfo import ZoneInfo
        from datetime import datetime
        import energy_charts as _ec
        from collections import defaultdict

        cfg   = load_config()
        store = _get_store()

        main_meta = {}
        for md in cfg.get("meters", {}).values():
            if not (md.get("meta") or {}).get("sub_meter"):
                main_meta = md.get("meta") or {}
                break

        tz_name  = main_meta.get("timezone", "UTC")
        currency = main_meta.get("currency_symbol", "£")
        _tz      = ZoneInfo(tz_name)

        billing_day = int(main_meta.get("billing_day") or 1)

        # Get all distinct local dates using UTC bounds — no local_date column needed
        from block_store import local_date_range_to_utc_bounds as _utc_bounds
        _all_utc_s = store._conn.execute("SELECT MIN(block_start) FROM blocks").fetchone()[0]
        _all_utc_e = store._conn.execute("SELECT MAX(block_start) FROM blocks").fetchone()[0]
        if not _all_utc_s:
            meter_colors = _ec.build_meter_colors_from_config(cfg)
            return jsonify({"currency": currency, "rows": [], "meters": [], "export_color": "#ff7f0e"})
        # Extend end by one block period to make it exclusive
        from datetime import timedelta as _td0
        _all_utc_e_excl = (datetime.fromisoformat(_all_utc_e) + _td0(minutes=int(main_meta.get("block_minutes") or 30))).isoformat()
        all_date_strs = store.get_dates_in_utc_range(_all_utc_s, _all_utc_e_excl, tz_name)
        if not all_date_strs:
            meter_colors = _ec.build_meter_colors_from_config(cfg)
            return jsonify({"currency": currency, "rows": [], "meters": [], "export_color": "#ff7f0e"})

        # Build meter colors from config — not from a block sample, which would
        # miss sub-meters that were added after the first block date.
        meter_colors = _ec.build_meter_colors_from_config(cfg)

        meter_labels = {}
        for meter_id, meter_cfg in cfg.get("meters", {}).items():
            meta  = (meter_cfg.get("meta") or {})
            is_sub = bool(meta.get("sub_meter"))
            if is_sub:
                label = meta.get("device") or meta.get("site") or meter_id
            else:
                label = "Direct"
            meter_labels[meter_id] = label

        all_dates = [datetime.strptime(d, "%Y-%m-%d").date() for d in all_date_strs]

        from block_store import local_date_to_utc_bounds as _ldtub
        from zoneinfo import ZoneInfo as _ZI

        # ── SQL aggregation per local day — no block dict reconstruction ──────
        # Fetch per-block data in a single SQL query, group into local days in Python.
        # This avoids loading full block dicts and is ~10x faster for large datasets.
        _all_utc_s2, _ = _ldtub(all_date_strs[0],  tz_name)
        _, _all_utc_e2 = _ldtub(all_date_strs[-1], tz_name)
        active_pid_sq = ("SELECT id FROM config_periods "
                         "WHERE effective_to IS NULL ORDER BY effective_from DESC LIMIT 1")

        _raw_rows = store._conn.execute(f"""
            SELECT b.block_start, b.meter_id, m.is_sub_meter, m.parent_meter_id,
                   b.imp_kwh, b.imp_kwh_grid, b.imp_kwh_remainder,
                   b.imp_rate, b.imp_cost, b.imp_cost_remainder, b.exp_kwh, b.exp_cost,
                   b.standing_charge, b.carbon_g,
                   b.interpolated, cp.billing_day
            FROM blocks b
            JOIN config_periods cp ON b.config_period_id = cp.id
            LEFT JOIN meters m ON m.meter_id = b.meter_id
              AND m.config_period_id = ({active_pid_sq})
            WHERE b.block_start >= ? AND b.block_start < ?
            ORDER BY b.block_start
        """, (_all_utc_s2, _all_utc_e2)).fetchall()

        # Group rows by local date and aggregate (shared billing-accurate helper —
        # bucket = local day, standing = first non-zero within each day).
        _tz_obj = _ZI(tz_name)
        _utc_zi = _ZI("UTC")

        def _bucket_local_day(_bs):
            return (datetime.fromisoformat(_bs)
                    .replace(tzinfo=_utc_zi)
                    .astimezone(_tz_obj)
                    .strftime("%Y-%m-%d"))

        _day_data = _aggregate_block_rows(
            _raw_rows, _bucket_local_day,
            standing_scope="bucket", billing_day_fallback=billing_day)

        # Pre-compute billing periods
        _bp_start_by_date = {}
        try:
            _bp_periods = _ec.get_billing_periods_from_config_periods(
                store.get_config_periods(), tz=_ZI(tz_name)
            )
            from datetime import timedelta as _td2
            for (_bps, _bpe) in _bp_periods:
                _d = _bps.date()
                while _d < _bpe.date():
                    _bp_start_by_date[_d.isoformat()] = (
                        _bps.date().isoformat(),
                        _bpe.date().isoformat(),
                    )
                    _d += _td2(days=1)
        except Exception as _bpe_err:
            logger.warning("api_blocks_summary: billing period pre-compute failed: %s", _bpe_err)

        # Build rows from aggregated day data
        rows = []
        for d in all_dates:
            _ds = d.isoformat()
            if _ds not in _day_data:
                continue
            _dd = _day_data[_ds]

            _bp_entry = _bp_start_by_date.get(_ds)
            # Keep all values at 4dp so the JS can sum across days without
            # accumulating per-day rounding error. Display rounding (2dp)
            # happens in the JS via .toFixed(2) at render time.
            _row = {
                "year":                  d.year,
                "month":                 d.month,
                "day":                   d.day,
                "billing_day":           _dd["billing_day"],
                "billing_period_start":  _bp_entry[0] if _bp_entry else _ds,
                "billing_period_end":    _bp_entry[1] if _bp_entry else None,
            }
            _row.update(_bucket_to_row_values(_dd))
            rows.append(_row)

        all_meter_ids = [m for m in meter_colors if m != "electricity_main_export"]
        meters_list = [{
            "id":    mid,
            "label": meter_labels.get(mid, mid),
            "color": meter_colors[mid],
            "is_sub": mid != "electricity_main",
        } for mid in all_meter_ids]

        export_color = meter_colors.get("electricity_main_export", "#ff7f0e")

        _postcode = ""
        for _md in cfg.get("meters", {}).values():
            _postcode = (_md.get("meta") or {}).get("postcode_prefix", "").strip()
            if _postcode:
                break

        # ── Synthetic dispatch-derived EV segment (display-only; BL-22) ──────────
        # For a no-sub-meter, no-EV account, split the "Direct" import segment into a
        # derived EV slice + reduced house slice, using completed dispatches (same
        # logic as the Usage-Insights card). Purely a DISPLAY split: every per-row
        # total (imp_kwh/imp_cost/net_cost) is left untouched, so the chart totals,
        # the data-table Totals, and the bill stay byte-identical. Guaranteed no-op
        # when an EV meter — or any sub-meter — exists.
        if (_configured_ev_meter_id(cfg) is None
                and len(meters_list) == 1
                and meters_list[0]["id"] == "electricity_main"):
            try:
                _drows = store._conn.execute(
                    "SELECT slot_start, kind, energy_kwh FROM dispatch_history "
                    "WHERE kind='completed' AND slot_start >= ? AND slot_start < ?",
                    (_all_utc_s2, _all_utc_e2)).fetchall()
                _ev_slot = _dispatch_derived_ev_kwh(
                    [{"slot_start": r["slot_start"], "kind": r["kind"],
                      "energy_kwh": r["energy_kwh"]} for r in _drows])
                _main_slot = {}
                for _rr in _raw_rows:
                    if _rr["is_sub_meter"]:
                        continue
                    _main_slot[_rr["block_start"]] = (
                        float(_rr["imp_kwh"] or 0), float(_rr["imp_cost"] or 0))
                _ev_day = _dispatch_ev_split_by_bucket(_main_slot, _ev_slot, _bucket_local_day)
            except Exception:
                _ev_day = {}
            _apply_ev_split_to_summary_rows(rows, meters_list, _ev_day)

        return jsonify({
            "currency":      currency,
            "billing_day":   billing_day,
            "block_minutes": int(main_meta.get("block_minutes") or 30),
            "rows":          rows,
            "meters":        meters_list,
            "export_color":  export_color,
            "has_postcode":  bool(_postcode),
            "data_version":  _blocks_data_version(store),
        })
    except Exception as e:
        logger.error("api_blocks_summary: %s", e)
        return jsonify({"error": str(e)}), 500


@app.route("/api/charts/blocks-day")
def api_blocks_day():
    """Per-half-hour breakdown for ONE local day — the Usage Stats 'HH' granularity.

    Same billing-accurate aggregation as /blocks-summary (rate-keyed direct-cost
    subtraction, sub-meters, carbon split, export), but bucketed by half-hour SLOT
    instead of by day, and ALWAYS returning a full day of slots (zero-filled), so
    the chart shows 48 bars even for slots that aren't populated yet.
    Query: date=YYYY-MM-DD (local; default today). Rows match barAggRows' shape."""
    try:
        from zoneinfo import ZoneInfo as _ZI
        from datetime import datetime as _dt, timedelta as _td
        import energy_charts as _ec
        from block_store import local_date_to_utc_bounds as _ldtub

        cfg = load_config()
        store = _get_store()
        main_meta = {}
        for md in cfg.get("meters", {}).values():
            if not (md.get("meta") or {}).get("sub_meter"):
                main_meta = md.get("meta") or {}
                break
        tz_name = main_meta.get("timezone", "UTC")
        currency = main_meta.get("currency_symbol", "£")
        bm = int(main_meta.get("block_minutes") or 30)
        billing_day = int(main_meta.get("billing_day") or 1)
        _tz = _ZI(tz_name)
        _utc = _ZI("UTC")

        date_str = (request.args.get("date") or "").strip()
        if not date_str:
            date_str = _dt.now(_tz).strftime("%Y-%m-%d")
        try:
            _day = _dt.strptime(date_str, "%Y-%m-%d").date()
        except ValueError:
            return jsonify({"error": "bad date (want YYYY-MM-DD)"}), 400

        _u_s, _u_e = _ldtub(date_str, tz_name)          # UTC bounds for the local day
        active_pid_sq = ("SELECT id FROM config_periods "
                         "WHERE effective_to IS NULL ORDER BY effective_from DESC LIMIT 1")
        _raw = store._conn.execute(f"""
            SELECT b.block_start, b.meter_id, m.is_sub_meter,
                   b.imp_kwh, b.imp_kwh_grid, b.imp_kwh_remainder,
                   b.imp_rate, b.imp_cost, b.exp_kwh, b.exp_cost,
                   b.standing_charge, b.carbon_g
            FROM blocks b
            JOIN config_periods cp ON b.config_period_id = cp.id
            LEFT JOIN meters m ON m.meter_id = b.meter_id
              AND m.config_period_id = ({active_pid_sq})
            WHERE b.block_start >= ? AND b.block_start < ?
            ORDER BY b.block_start
        """, (_u_s, _u_e)).fetchall()

        def _slot_of(bs):
            return (_dt.fromisoformat(bs).replace(tzinfo=_utc)
                    .astimezone(_tz).strftime("%H:%M"))

        # Same billing-accurate aggregation as /blocks-summary, bucketed by slot.
        # standing_scope='global' books the day's single standing charge on the
        # first slot that carries one, so the 48 bars sum to exactly one day.
        slots = _aggregate_block_rows(
            _raw, _slot_of, standing_scope="global", billing_day_fallback=billing_day)

        n_slots = int(round(24 * 60 / max(1, bm)))
        _base = _dt(_day.year, _day.month, _day.day)
        rows = []
        for i in range(n_slots):
            sk = (_base + _td(minutes=bm * i)).strftime("%H:%M")
            _s = slots.get(sk)
            if not _s:
                rows.append({"slot": sk, "label": sk, "standing": 0,
                             "imp_kwh": 0, "exp_kwh": 0, "imp_cost": 0, "exp_cost": 0,
                             "net_cost": 0, "carbon_g_net": None, "carbon_g_total": None,
                             "avg_intensity": None,
                             "meters": {"electricity_main": {"imp_kwh": 0, "imp_cost": 0,
                                        "exp_kwh": 0, "exp_cost": 0, "carbon_g": None,
                                        "carbon_g_imp": None, "carbon_g_exp": None}}})
                continue
            _row = {"slot": sk, "label": sk}
            _row.update(_bucket_to_row_values(_s))
            rows.append(_row)

        meter_colors = _ec.build_meter_colors_from_config(cfg)
        meter_labels = {}
        for mid, mc in cfg.get("meters", {}).items():
            meta = (mc.get("meta") or {})
            meter_labels[mid] = ((meta.get("device") or meta.get("site") or mid)
                                 if meta.get("sub_meter") else "Direct")
        all_meter_ids = [m for m in meter_colors if m != "electricity_main_export"]
        meters_list = [{"id": mid, "label": meter_labels.get(mid, mid),
                        "color": meter_colors[mid], "is_sub": mid != "electricity_main"}
                       for mid in all_meter_ids]
        _postcode = ""
        for _md in cfg.get("meters", {}).values():
            _postcode = (_md.get("meta") or {}).get("postcode_prefix", "").strip()
            if _postcode:
                break
        return jsonify({"currency": currency, "billing_day": billing_day,
                        "block_minutes": bm, "date": date_str, "rows": rows,
                        "meters": meters_list,
                        "export_color": meter_colors.get("electricity_main_export", "#ff7f0e"),
                        "has_postcode": bool(_postcode)})
    except Exception as e:
        logger.error("api_blocks_day: %s", e)
        return jsonify({"error": str(e)}), 500


@app.route("/api/charts/heatmap")
def api_chart_heatmap():
    """Return heatmap chart HTML as JSON for inline embedding."""
    p = os.path.join(CHART_DIR, "net_heatmap.html")
    if not os.path.exists(p):
        return jsonify({"html": None})
    with open(p) as f:
        resp = jsonify({"html": f.read()})
    resp.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
    resp.headers["Pragma"] = "no-cache"
    resp.headers["Expires"] = "0"
    return resp


@app.route("/api/charts/daily")
def api_chart_daily():
    """Return daily chart HTML as JSON for inline embedding."""
    p = os.path.join(CHART_DIR, "daily_usage.html")
    if not os.path.exists(p):
        return jsonify({"html": None})
    with open(p) as f:
        return jsonify({"html": f.read()})


# ── API ───────────────────────────────────────────────────────────────────────

@app.route("/api/entities")
def api_entities():
    """Return all HA entity IDs with unit_of_measurement and device_class for UI filtering."""
    import urllib.request
    token    = os.environ.get("HA_TOKEN") or os.environ.get("SUPERVISOR_TOKEN", "")
    ha_url   = os.environ.get("HA_URL", "").rstrip("/")
    base_url = (ha_url + "/api") if ha_url else "http://supervisor/core/api"
    try:
        req = urllib.request.Request(
            base_url + "/states",
            headers={"Authorization": "Bearer " + token, "Content-Type": "application/json"}
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            states = json.loads(resp.read().decode())
        entities = sorted([
            {
                "id":           s["entity_id"],
                "unit":         s.get("attributes", {}).get("unit_of_measurement", ""),
                "device_class": s.get("attributes", {}).get("device_class", ""),
            }
            for s in states
        ], key=lambda x: x["id"])
        return jsonify(entities)
    except Exception as e:
        logger.error("api_entities: %s", e)
        # Fall back to state cache
        if _ha_client:
            return jsonify(sorted(_ha_client._state_cache.keys()))
        return jsonify([])


@app.route("/api/config", methods=["GET"])
def api_get_config():
    return jsonify(load_config())


@app.route("/api/config/history")
def api_config_history():
    """Return config period history with block counts per period."""
    try:
        store = _get_store()
        cur = store._conn.execute(
            """
            SELECT cp.id, cp.effective_from, cp.effective_to, cp.billing_day,
                   cp.block_minutes, cp.timezone, cp.currency_symbol, cp.currency_code,
                   cp.site_name, cp.supplier, cp.change_reason,
                   (SELECT postcode_prefix FROM meters
                      WHERE config_period_id = cp.id AND is_sub_meter = 0
                      ORDER BY id ASC LIMIT 1) as postcode_prefix,
                   (SELECT postcode_source FROM meters
                      WHERE config_period_id = cp.id AND is_sub_meter = 0
                      ORDER BY id ASC LIMIT 1) as postcode_source,
                   COUNT(DISTINCT b.block_start) as block_count
            FROM config_periods cp
            LEFT JOIN blocks b ON b.config_period_id = cp.id
            GROUP BY cp.id
            ORDER BY cp.effective_from DESC
            """
        )
        rows = []
        for r in cur.fetchall():
            rows.append({
                "id":             r["id"],
                "effective_from": r["effective_from"],
                "effective_to":   r["effective_to"],
                "billing_day":    r["billing_day"],
                "block_minutes":  r["block_minutes"],
                "timezone":       r["timezone"],
                "currency_symbol":r["currency_symbol"],
                "currency_code":  r["currency_code"],
                "site_name":      r["site_name"],
                "supplier":       r["supplier"],
                "change_reason":  r["change_reason"],
                "postcode_prefix":r["postcode_prefix"],
                "postcode_source":r["postcode_source"],
                "block_count":    r["block_count"],
            })
        # Include the configured timezone for client-side date formatting
        cfg_tz = "UTC"
        try:
            from energy_engine_io import load_json as _lj_tz
            _cfg_tz = load_config()
            for _m in _cfg_tz.get("meters", {}).values():
                if not (_m.get("meta") or {}).get("sub_meter"):
                    cfg_tz = (_m.get("meta") or {}).get("timezone", "UTC")
                    break
        except Exception:
            pass
        return jsonify({"periods": rows, "timezone": cfg_tz})
    except Exception as e:
        logger.error("api_config_history: %s", e)
        return jsonify({"error": str(e)}), 500


@app.route("/api/region/reconcile")
def api_region_reconcile_get():
    """Pending region-reconciliation plan (the post-import 'name your sites'
    step). Returns {pending: False} when there's nothing to confirm."""
    try:
        store = _get_store()
        plan = store.get_meta("region_reconcile_pending", None)
        if not plan or not plan.get("needs_confirmation"):
            return jsonify({"pending": False})
        return jsonify({"pending": True, "plan": plan})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/region/reconcile/apply", methods=["POST"])
def api_region_reconcile_apply():
    """Apply a confirmed reconciliation: split at each move boundary and stamp
    every period's region (outward code) + user-supplied site name.
    Body: {"sites": [{outcode, from, to, site_name}]}."""
    try:
        store = _get_store()
        data = request.get_json(force=True) or {}
        sites = data.get("sites") or []
        if not sites:
            return jsonify({"error": "no sites supplied"}), 400
        res = store.apply_region_reconciliation(sites)
        store.set_meta("region_reconcile_pending", None)   # clear pending
        store.rearm_carbon_backfill()   # new regions → re-scan carbon
        try:
            _rebuild_config_period_chain(store)
        except Exception as _e:
            logger.warning("api_region_reconcile_apply: chain rebuild failed: %s", _e)
        logger.info("api_region_reconcile_apply: %s", res)
        return jsonify({"ok": True, **res})
    except Exception as e:
        logger.error("api_region_reconcile_apply: %s", e)
        return jsonify({"error": str(e)}), 500


@app.route("/api/region/reconcile/dismiss", methods=["POST"])
def api_region_reconcile_dismiss():
    """Dismiss the pending reconciliation without applying it."""
    try:
        store = _get_store()
        store.set_meta("region_reconcile_pending", None)
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/config/history", methods=["POST"])
def api_config_history_create():
    """Create a new config period, inheriting meter definitions from the current active period."""
    try:
        data  = request.get_json(force=True)

        from datetime import datetime as _dt
        ef_from_raw = data.get("effective_from")
        if not ef_from_raw:
            return jsonify({"error": "effective_from is required"}), 400
        ef_from = str(ef_from_raw).replace(" ", "T").split(".")[0]
        try:
            _dt.fromisoformat(ef_from)
        except ValueError:
            return jsonify({"error": "Invalid effective_from date"}), 400

        # #361: read + write on the ENGINE loop/connection (serialised with the
        # engine's writes) instead of the shared web connection on the Flask thread.
        def _mutate(store):
            active_cp = store._conn.execute(
                "SELECT id, billing_day, block_minutes, timezone, "
                "currency_symbol, currency_code, site_name, supplier "
                "FROM config_periods WHERE effective_to IS NULL "
                "ORDER BY effective_from DESC LIMIT 1"
            ).fetchone()
            if not active_cp:
                raise _ConfigError("No active config period found to inherit from")

            billing_day     = data.get("billing_day",     active_cp["billing_day"])
            timezone        = data.get("timezone",        active_cp["timezone"])
            currency_symbol = data.get("currency_symbol", active_cp["currency_symbol"])
            currency_code   = data.get("currency_code",   active_cp["currency_code"])
            site_name       = data.get("site_name",       active_cp["site_name"])
            supplier        = data.get("supplier",        active_cp["supplier"])
            change_reason   = data.get("change_reason") or None

            # Reconstruct config from DB, apply overrides, then insert new period
            full_cfg = store.config_from_db(active_cp["id"])
            for md in full_cfg.get("meters", {}).values():
                meta = md.get("meta") or {}
                if "billing_day"     in data: meta["billing_day"]     = int(billing_day)
                if "timezone"        in data: meta["timezone"]        = timezone
                if "currency_symbol" in data: meta["currency_symbol"] = currency_symbol
                if "currency_code"   in data: meta["currency_code"]   = currency_code
                if "site_name"       in data: meta["site_name"]       = site_name
                if "supplier"        in data: meta["supplier"]        = supplier
                # Postcode is main-meter-only (outward code); a manual entry is 'user'.
                if "postcode" in data and not meta.get("sub_meter"):
                    from block_store import outward_code as _oc
                    meta["postcode_prefix"] = _oc(data.get("postcode"))
                    meta["postcode_source"] = "user"
                md["meta"] = meta

            with store._conn:
                cur = store._conn.execute(
                    """INSERT INTO config_periods
                       (effective_from, effective_to, billing_day, block_minutes, timezone,
                        currency_symbol, currency_code, site_name, supplier, change_reason)
                       VALUES (?, NULL, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        ef_from,
                        int(billing_day) if billing_day else 1,
                        int(active_cp["block_minutes"] or 30),
                        timezone or "UTC",
                        currency_symbol or "£",
                        currency_code or "GBP",
                        site_name,
                        supplier,
                        change_reason,
                    )
                )
                new_period_id = cur.lastrowid
                store._write_meters(full_cfg, new_period_id)
            _rebuild_config_period_chain(store)
            return {"ok": True}

        result = _config_mutation_on_loop(_mutate)
        logger.info("api_config_history_create: new period from %s", ef_from)
        return jsonify(result)
    except _ConfigError as ce:
        return jsonify({"error": str(ce)}), 400
    except _ConfigBusy:
        return jsonify({"queued": True, "error": _CONFIG_BUSY_MSG}), 503
    except Exception as e:
        logger.error("api_config_history_create: %s", e)
        return jsonify({"error": str(e)}), 500


@app.route("/api/config/history/<int:period_id>", methods=["PUT"])
def api_config_history_update(period_id):
    """Update effective_from, effective_to, or change_reason for a config period."""
    try:
        store = _get_store()
        data  = request.get_json(force=True)

        # Validate period exists
        cp = store.get_config_period(period_id)
        if not cp:
            return jsonify({"error": "Config period not found"}), 404

        allowed = {
            "effective_from", "effective_to", "change_reason",
            "billing_day", "timezone",
            "currency_symbol", "currency_code", "site_name", "supplier",
        }
        updates = {k: v for k, v in data.items() if k in allowed}

        # `postcode` is not a config_periods column — it lives on the period's MAIN
        # meter (outward code only). Handle it separately: a manual edit here is a
        # user value, so mark provenance 'user'. Empty string clears it.
        postcode_edit = "postcode" in data
        postcode_outcode = None
        if postcode_edit:
            from block_store import outward_code as _outcode
            postcode_outcode = _outcode(data.get("postcode"))

        if not updates and not postcode_edit:
            return jsonify({"error": "No valid fields to update"}), 400

        # Type coerce numeric fields
        from datetime import datetime as _dt
        for field in ("billing_day", "block_minutes"):
            if field in updates and updates[field] is not None:
                try:
                    updates[field] = int(updates[field])
                except (ValueError, TypeError):
                    return jsonify({"error": f"Invalid value for {field}"}), 400

        # Normalise and validate date fields — ensure T separator, no microseconds
        for field in ("effective_from", "effective_to"):
            if field in updates and updates[field]:
                val = str(updates[field]).replace(" ", "T").split(".")[0]
                try:
                    _dt.fromisoformat(val)
                    updates[field] = val  # store normalised form
                except ValueError:
                    return jsonify({"error": f"Invalid date format for {field}"}), 400

        # Validate effective_from < effective_to if both present
        from_val = updates.get("effective_from") or cp.get("effective_from")
        to_val   = updates.get("effective_to")   or cp.get("effective_to")
        if from_val and to_val:
            if _dt.fromisoformat(from_val) >= _dt.fromisoformat(to_val):
                return jsonify({"error": "Effective From must be before Effective To"}), 400

        # #361: run the write + chain rebuild on the ENGINE loop/connection (serialised
        # with the engine's writes — no shared-web-connection race), then regen charts
        # OFF the loop so a billing-period change reflects immediately. Same path as
        # the create endpoint via _config_mutation_on_loop.
        def _mutate(estore):
            with estore._conn:
                if updates:
                    set_clause = ", ".join(f"{k} = ?" for k in updates)
                    estore._conn.execute(
                        f"UPDATE config_periods SET {set_clause} WHERE id = ?",
                        list(updates.values()) + [period_id]
                    )
                if postcode_edit:
                    # Outward code only; a manual edit is provenance 'user'.
                    estore._conn.execute(
                        "UPDATE meters SET postcode_prefix = ?, postcode_source = 'user' "
                        "WHERE config_period_id = ? AND is_sub_meter = 0",
                        (postcode_outcode, period_id)
                    )
            if postcode_edit:
                estore.rearm_carbon_backfill()   # a region change may free carbon backfill
            # Rebuild the contiguous chain: sort all periods by effective_from, then
            # set each period's effective_to = next period's effective_from. Robust
            # regardless of how far the date was moved.
            try:
                _rebuild_config_period_chain(estore)
            except Exception as _snap_e:
                logger.warning("api_config_history_update: chain rebuild failed: %s", _snap_e)
            return {"ok": True}

        result = _config_mutation_on_loop(_mutate)
        logger.info("api_config_history_update: period %d updated %s%s",
                    period_id, list(updates.keys()),
                    " +postcode" if postcode_edit else "")
        return jsonify(result)
    except _ConfigBusy:
        return jsonify({"queued": True, "error": _CONFIG_BUSY_MSG}), 503
    except Exception as e:
        logger.error("api_config_history_update: %s", e)
        return jsonify({"error": str(e)}), 500


@app.route("/api/config/history/<int:period_id>", methods=["DELETE"])
def api_config_history_delete(period_id):
    """Delete a config period, re-assigning its blocks to an adjacent period."""
    try:
        # #361: run the delete + chain rebuild on the ENGINE loop/connection so it's
        # serialised with the engine's own writes — a delete on the shared web store
        # from the Flask thread raced the engine and surfaced as SQLite "another row
        # available" / "database is locked". Charts regen OFF the loop afterwards, the
        # same path as create/update via _config_mutation_on_loop.
        def _mutate(estore):
            # Whether this is the active period must be read on the same connection,
            # before the delete.
            cp = estore.get_config_period(period_id)
            is_active = bool(cp and cp.get("effective_to") is None)
            result = estore.delete_config_period(period_id)
            # Rebuild the contiguous chain so effective_to values stay consistent.
            try:
                _rebuild_config_period_chain(estore)
            except Exception as _e:
                logger.warning("api_config_history_delete: chain rebuild failed: %s", _e)
            # If we deleted the active period, the predecessor is now active — write
            # its config back to meters_config.json (convenience export).
            if is_active:
                try:
                    new_active = estore._conn.execute(
                        "SELECT id FROM config_periods "
                        "WHERE effective_to IS NULL ORDER BY effective_from DESC LIMIT 1"
                    ).fetchone()
                    if new_active:
                        from energy_engine_io import save_json_atomic as _sja
                        restored_cfg = estore.config_from_db(new_active["id"])
                        cfg_path = os.path.join(DATA_DIR, "meters_config.json")
                        _sja(cfg_path, restored_cfg)
                        logger.info(
                            "api_config_history_delete: meters_config.json restored "
                            "from newly-active config period"
                        )
                except Exception as _e:
                    logger.warning(
                        "api_config_history_delete: could not restore meters_config.json: %s", _e
                    )
            return {"ok": True,
                    "blocks_reassigned": result["blocks_reassigned"],
                    "config_restored": is_active}

        result = _config_mutation_on_loop(_mutate)
        return jsonify(result)
    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    except _ConfigBusy:
        return jsonify({"queued": True, "error": _CONFIG_BUSY_MSG}), 503
    except Exception as e:
        # repr(): a loop timeout (TimeoutError) str()s to '' — repr keeps the type.
        logger.error("api_config_history_delete: %r", e)
        return jsonify({"error": str(e) or e.__class__.__name__}), 500


@app.route("/api/config", methods=["POST"])
def api_save_config():
    try:
        # Don't let a config save race an in-flight DB restore: it writes to the same
        # SQLite file the restore is swapping out (→ "database is locked"), takes a
        # pointless pre_config_save backup, and schedules an extra engine_startup that
        # piles onto the restore's own — the concurrency storm seen in the logs. Refuse
        # clearly; the user can save again once the restore's progress banner clears.
        if _restore_job.get("status") == "running":
            return jsonify({"error": "A database restore is in progress — wait for it "
                                     "to finish, then save your config again."}), 409

        payload = request.get_json(force=True)
        if not isinstance(payload, dict) or "meters" not in payload:
            return jsonify({"error": "Invalid config structure"}), 400

        # change_reason is an optional UI field, not part of the config schema
        change_reason = payload.pop("change_reason", None) or None
        data = payload

        # Validate device names — unique, max 40 chars, safe characters
        import re as _re
        _NAME_REGEX = _re.compile(r"^[a-zA-Z0-9 '\-&().]+$")
        _NAME_MAX   = 40
        seen_names  = {}
        name_errors = []
        for mid, mdata in (data.get("meters") or {}).items():
            meta     = (mdata or {}).get("meta") or {}
            is_sub   = meta.get("sub_meter")
            raw_name = (meta.get("device") if is_sub else meta.get("site") or "").strip()
            if not raw_name:
                if is_sub:
                    name_errors.append(f'Meter "{mid}": device name is required.')
                continue
            if len(raw_name) > _NAME_MAX:
                name_errors.append(f'"{raw_name}": name must be {_NAME_MAX} characters or fewer.')
            if not _NAME_REGEX.match(raw_name):
                name_errors.append(f'"{raw_name}": name contains invalid characters.')
            lower = raw_name.lower()
            if lower in seen_names:
                name_errors.append(f'"{raw_name}": name is already used by another meter.')
            else:
                seen_names[lower] = mid
        if name_errors:
            return jsonify({"error": " | ".join(name_errors)}), 400

        # Zip current data before committing config change
        _create_backup_zip(label="pre_config_save")

        # Determine if billing-significant meta changed before writing,
        # so we know whether to create a new period or update in place.
        try:
            from block_store import config_meta_significant
            import json as _json
            store = _get_store()
            cur_id = store.get_current_config_period_id()
            should_create = True
            if cur_id is not None:
                old_cfg = store.config_from_db(cur_id)
                # If current period has no blocks yet, update in place regardless
                # of how significant the change is — no data to protect
                block_count = store._conn.execute(
                    "SELECT COUNT(*) FROM blocks WHERE config_period_id = ?", (cur_id,)
                ).fetchone()[0]
                if block_count == 0:
                    should_create = False
                elif old_cfg.get("meters"):
                    if not config_meta_significant(old_cfg, data):
                        should_create = False
            if should_create:
                # New billing period — insert_config_period writes to normalised tables
                store.insert_config_period(data, change_reason=change_reason)
                logger.info("server: config period created (reason=%s)", change_reason or "none")
                # Write file export (not authoritative — convenience only)
                try:
                    p = config_path()
                    os.makedirs(os.path.dirname(p), exist_ok=True)
                    tmp = p + ".tmp"
                    with open(tmp, "w") as _f:
                        _json.dump(data, _f, indent=2)
                    os.replace(tmp, p)
                except Exception as _fe:
                    logger.warning("server: could not write meters_config.json export: %s", _fe)
            else:
                # No billing change — save_config updates the active period in-place
                save_config(data)
                logger.info("server: config saved — no billing meta change, period unchanged")
        except Exception as _e:
            logger.warning("server: config save failed: %s", _e)
            # Fallback: always attempt file export so UI isn't left stale
            try:
                p = config_path()
                os.makedirs(os.path.dirname(p), exist_ok=True)
                import json as _j2
                tmp = p + ".tmp"
                with open(tmp, "w") as _f:
                    _j2.dump(data, _f, indent=2)
                os.replace(tmp, p)
            except Exception:
                pass

        logger.info("server: config saved (%d meters)", len(data["meters"]))

        # Re-run engine_startup to pick up new sensor subscriptions
        import asyncio
        from engine import engine_startup
        if _event_loop and _event_loop.is_running() and _ha_client:
            asyncio.run_coroutine_threadsafe(engine_startup(_ha_client), _event_loop)
            logger.info("server: engine_startup scheduled after config save")

        return jsonify({"ok": True})
    except Exception as e:
        logger.error("api_save_config: %s", e)
        return jsonify({"error": str(e)}), 500


@app.route("/api/backup/create", methods=["POST"])
def api_backup_create():
    """Create a manual backup zip with an optional label."""
    try:
        data  = request.get_json(force=True) or {}
        label = data.get("label", "manual")
        path  = _create_backup_zip(label=label)
        return jsonify({"ok": True, "path": os.path.basename(path)})
    except Exception as e:
        logger.error("api_backup_create: %s", e)
        return jsonify({"error": str(e)}), 500


@app.route("/api/backup", methods=["POST"])
def api_backup():
    """Create a manual backup zip of all data files. Returns the filename and its
    size so the UI can confirm 'Backup ready: <name> (<size>)'."""
    try:
        path = _create_backup_zip(label="manual")
        try:
            size = os.path.getsize(path)
        except Exception:
            size = None
        return jsonify({"ok": True, "path": os.path.basename(path), "size": size})
    except Exception as e:
        logger.error("api_backup: %s", e)
        return jsonify({"error": str(e)}), 500


@app.route("/api/backup/info", methods=["GET"])
def api_backup_info():
    """Return backup configuration info."""
    return jsonify({
        "backup_dir": _share_backup_dir(),
        "mode": os.environ.get("EMT_MODE", "supervised")
    })


@app.route("/api/backup/list", methods=["GET"])
def api_backup_list():
    """List available backup zips and last-finalise flat files."""
    import glob
    try:
        zips = sorted(glob.glob(f"{_share_backup_dir()}/backups/*.zip"), reverse=True)
        # Check for flat files from last finalise
        primary = ["blocks.db"]
        legacy  = ["blocks.json"]   # written by 1.x/2.0.x, no longer updated
        flat_files = []
        for fname in primary + legacy:
            fpath = f"{_share_backup_dir()}/{fname}"
            if os.path.exists(fpath):
                mtime = os.path.getmtime(fpath)
                from datetime import datetime as _dt
                entry = {
                    "name": fname,
                    "modified": _dt.utcfromtimestamp(mtime).strftime("%Y-%m-%dT%H:%M:%S"),
                }
                if fname in legacy:
                    entry["legacy"] = True
                flat_files.append(entry)
        return jsonify({
            "zips": [os.path.basename(z) for z in zips],
            "flat": flat_files
        })
    except Exception as e:
        return jsonify({"zips": [], "flat": []})


_restore_job = {"status": "idle"}


def _atomic_restore_write(dest, fill):
    """Write `dest` atomically: fill a temp file, fsync it, then os.replace into
    place. An interrupted restore (e.g. the add-on being rebuilt/restarted
    mid-write) then leaves EITHER the old file OR the complete new one — never a
    truncated/zero-length blocks.db that would open as a fresh EMPTY database
    (the failure that silently wiped a user's data after a restore→rebuild).
    `fill(fh)` writes the file body to the open temp handle."""
    tmp = dest + ".restore-tmp"
    try:
        with open(tmp, "wb") as _t:
            fill(_t)
            _t.flush()
            os.fsync(_t.fileno())
        os.replace(tmp, dest)          # atomic on POSIX
        # fsync the directory so the rename itself is durable
        try:
            _dfd = os.open(os.path.dirname(dest) or ".", os.O_RDONLY)
            try: os.fsync(_dfd)
            finally: os.close(_dfd)
        except Exception:
            pass
    finally:
        if os.path.exists(tmp):
            try: os.remove(tmp)
            except Exception: pass


@app.route("/api/backup/restore", methods=["POST"])
def api_backup_restore():
    """Start a restore as a BACKGROUND job and return at once — so it completes
    even if the user navigates away. Poll /api/backup/restore/status for progress."""
    try:
        data      = request.get_json(force=True) or {}
        zipname   = data.get("zip", "")
        selected  = data.get("files", None)
        from_flat = data.get("from_flat", False)
        if not from_flat:
            if not zipname or "/" in zipname or "\\" in zipname:
                return jsonify({"error": "Invalid zip name"}), 400
            if not os.path.exists(f"{_share_backup_dir()}/backups/{zipname}"):
                return jsonify({"error": "Backup not found"}), 404
        if _restore_job.get("status") == "running":
            return jsonify({"error": "a restore is already running",
                            "status": _restore_job}), 409
        _restore_job.clear()
        _restore_job.update({"status": "running", "step": "starting",
                             "restored": None, "error": None})
        threading.Thread(target=_restore_worker,
                         args=(zipname, selected, from_flat),
                         daemon=True, name="restore").start()
        return jsonify({"ok": True, "status": "running"})
    except Exception as e:
        logger.error("api_backup_restore(launch): %s", e)
        return jsonify({"error": str(e)}), 500


@app.route("/api/backup/restore/status", methods=["GET"])
def api_backup_restore_status():
    """Poll the background restore job's progress/status."""
    return jsonify(_restore_job)


def _resolve_backup_zip(zipname: str):
    """Validate a user-supplied backup zip name and resolve it to an absolute path
    strictly inside the backups dir. Returns (path, None) or (None, (error, code))."""
    zipname = os.path.basename(zipname or "")          # strip any path components
    if not zipname or "/" in zipname or "\\" in zipname or not zipname.endswith(".zip"):
        return None, ("Invalid backup name", 400)
    path = os.path.join(_share_backup_dir(), "backups", zipname)
    if not os.path.exists(path):
        return None, ("Backup not found", 404)
    return path, None


@app.route("/api/backup/download/<zipname>", methods=["GET"])
def api_backup_download(zipname):
    """Download a named backup zip. Path-guarded to the backups dir."""
    path, err = _resolve_backup_zip(zipname)
    if err:
        return jsonify({"error": err[0]}), err[1]
    # octet-stream (not application/zip) so browsers don't treat it as a "safe"
    # archive and auto-extract it on download (e.g. Safari's "open safe files"),
    # which would leave a bare blocks.db instead of the .zip.
    return send_file(path, as_attachment=True,
                     download_name=os.path.basename(path),
                     mimetype="application/octet-stream")


@app.route("/api/backup/delete", methods=["POST"])
def api_backup_delete():
    """Delete a named backup zip (user-initiated from the backup list). Path-guarded
    to the backups dir; only removes the one requested .zip, nothing else."""
    try:
        data = request.get_json(force=True) or {}
        path, err = _resolve_backup_zip(data.get("zip", ""))
        if err:
            return jsonify({"error": err[0]}), err[1]
        os.remove(path)
        logger.info("api_backup_delete: removed backup %s", os.path.basename(path))
        return jsonify({"ok": True, "deleted": os.path.basename(path)})
    except Exception as e:
        logger.error("api_backup_delete: %s", e)
        return jsonify({"error": str(e)}), 500


def _restore_worker(zipname, selected, from_flat, staged=None):
    """Background restore worker (runs in its own thread; survives navigation).
    Updates _restore_job at each phase. Restores selected data files from a named
    backup zip, from last-finalise flat files, or — when `staged` is an absolute
    path to an already-validated uploaded blocks.db (#356) — from that single
    upload. The upload path gets the SAME safety backup, progress, atomic swap and
    gap-detection as the backup-zip restore."""
    global _store
    import zipfile, shutil, sys as _sys_r
    _j = _restore_job
    _atomic_write = _atomic_restore_write
    def _step(s):
        _j["step"] = s
        logger.info("restore: %s", s)
    try:
        known = {"blocks.db", "blocks.json"}

        # ── Step 1: pause engine ─────────────────────────────────────────────
        _eng_r = _sys_r.modules.get("engine")
        if _eng_r and hasattr(_eng_r, "pause_engine"):
            _eng_r.pause_engine()

        # ── Step 1b: STOP any running API import before touching the DB ───────
        # The background import writes via the SQLite store on the engine loop.
        # Closing that store under it (Step 3) uses a freed connection from
        # another thread → segfault. Cancel it and wait for it to actually stop;
        # if it won't, abort the restore rather than risk crashing the process.
        if getattr(_eng_r, "api_import_running", None) and _eng_r.api_import_running():
            _step("stopping the running import")
            import time as _t_r
            try:
                _eng_r.api_import_control("cancel")
            except Exception:
                pass
            stopped = False
            for _ in range(120):        # up to ~60s
                if not _eng_r.api_import_running():
                    stopped = True
                    break
                _t_r.sleep(0.5)
            if not stopped:
                if hasattr(_eng_r, "resume_engine"):
                    _eng_r.resume_engine()
                if staged and os.path.exists(staged):
                    try: os.remove(staged)
                    except Exception: pass
                _j["status"] = "error"
                _j["error"] = ("An import was still finishing — restore aborted. "
                               "Try again in a moment.")
                return

        # ── Step 2: pre-restore backup (stores still open — backup needs them) ─
        _step("taking a safety backup")
        _create_backup_zip(label="pre_restore")

        # ── Step 3: checkpoint WAL then close ALL store connections ───────────
        # Must happen after backup but before writing restored file.
        # If any connection remains open, SQLite WAL will be inconsistent with
        # the newly written DB file, causing "malformed" on next open.
        try:
            # Checkpoint + close the ENGINE store ON THE ENGINE LOOP — never from this
            # restore-worker thread. The engine's connection is opened
            # check_same_thread=False, so a PRAGMA wal_checkpoint / close() issued here
            # while the loop is mid-query on the SAME connection is a concurrent use of
            # one SQLite connection from two threads → Segmentation fault. pause_engine()
            # only sets a cooperative flag; it does NOT stop work already in flight on the
            # loop (a chart regen, discovery, a scheduled task), so the collision is real
            # whenever the loop is busy. Running this as a loop task serialises it against
            # every other loop DB access (the event loop is single-threaded).
            def _teardown_engine_store():
                import engine as _eng_mod2
                if getattr(_eng_mod2, "_store", None) is None:
                    return
                try:
                    _eng_mod2._store._conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
                    logger.info("api_backup_restore: WAL checkpoint complete")
                except Exception as _ce:
                    logger.warning("api_backup_restore: WAL checkpoint failed: %s", _ce)
                try:
                    _eng_mod2._store.close()
                except Exception:
                    pass
                _eng_mod2._store = None
            try:
                if _event_loop and _event_loop.is_running():
                    async def _co_teardown():
                        _teardown_engine_store()
                    _run_on_engine_loop(_co_teardown(), timeout=30.0)
                else:
                    _teardown_engine_store()
            except Exception as _te:
                # repr(): a loop timeout (TimeoutError) has an empty str(), which
                # logged a blank reason — repr keeps the type visible.
                logger.warning("api_backup_restore: engine store teardown failed: %r", _te)
            # Close web server store connection (this thread's own store handle)
            if _store:
                try: _store.close()
                except Exception: pass
            _store = None
            # Remove WAL/SHM files so restored DB opens cleanly
            _db_path_pre = os.path.join(DATA_DIR, "blocks.db")
            for _ext in ("-wal", "-shm"):
                _wf = _db_path_pre + _ext
                if os.path.exists(_wf):
                    try: os.remove(_wf)
                    except Exception: pass
            logger.info("api_backup_restore: all store connections closed, WAL files removed")
        except Exception as _pre_e:
            logger.warning("api_backup_restore: pre-restore cleanup failed: %s", _pre_e)

        _step("restoring files")
        restored = []

        if staged:
            # #356: swap in the uploaded (already SQLite-validated) blocks.db with
            # the same atomic write the zip/flat paths use, then drop the staging
            # file. Everything downstream (reset, migrate, gap-detect) is shared.
            dest = os.path.join(DATA_DIR, "blocks.db")
            with open(staged, "rb") as _src:
                _atomic_write(dest, lambda d, _s=_src: shutil.copyfileobj(_s, d))
            restored.append("blocks.db")
            try: os.remove(staged)
            except Exception: pass
            logger.info("api_backup_restore: restored blocks.db from upload")
        elif from_flat:
            for fname in (selected or list(known)):
                if fname not in known:
                    continue
                src_path = f"{_share_backup_dir()}/{fname}"
                dst_path = os.path.join(DATA_DIR, fname)
                if os.path.exists(src_path):
                    with open(src_path, "rb") as _src:
                        _atomic_write(dst_path,
                                      lambda d, _s=_src: shutil.copyfileobj(_s, d))
                    restored.append(fname)
            logger.info("api_backup_restore: restored flat files %s", restored)
        else:
            zip_path = f"{_share_backup_dir()}/backups/{zipname}"
            if not os.path.exists(zip_path):
                _j["status"] = "error"; _j["error"] = "Backup not found"
                return
            with zipfile.ZipFile(zip_path, "r") as zf:
                for name in zf.namelist():
                    basename = os.path.basename(name)
                    if basename not in known:
                        continue
                    if selected is not None and basename not in selected:
                        continue
                    dest = os.path.join(DATA_DIR, basename)
                    with zf.open(name) as zf_src:
                        _atomic_write(dest,
                                      lambda d, _s=zf_src: shutil.copyfileobj(_s, d))
                    restored.append(basename)
            logger.info("api_backup_restore: restored %s from %s", restored, zipname)

        # A legacy pre-SQLite blocks.json restored from an old backup is no
        # longer auto-migrated (JSON->SQLite migration was removed in 4.0.0).
        # Warn loudly and leave the file untouched rather than silently importing.
        legacy_json = os.path.join(DATA_DIR, "blocks.json")
        if "blocks.json" in restored and os.path.exists(legacy_json):
            logger.error(
                "api_backup_restore: restored a legacy blocks.json, but automatic "
                "JSON->SQLite migration was REMOVED in 4.0.0. It is left untouched "
                "and NOT imported. To recover it, migrate with an earlier release "
                "(3.x or older) before upgrading."
            )

        # ── Reset stores and fix WAL before any further DB access ────────────
        _step("reinitialising")
        if "blocks.db" in restored or "blocks.json" in restored:
            # Close web server store
            if _store:
                try: _store.close()
                except Exception: pass
            _store = None
            # Remove WAL/SHM files left by the running engine — these cause
            # "malformed" errors when BlockStore reopens with PRAGMA journal_mode=WAL
            _db_path = os.path.join(DATA_DIR, "blocks.db")
            for _ext in ("-wal", "-shm"):
                _wf = _db_path + _ext
                if os.path.exists(_wf):
                    try: os.remove(_wf)
                    except Exception: pass
            # Switch to DELETE journal mode via raw sqlite3 so BlockStore can
            # open cleanly without triggering the corruption recovery handler
            try:
                import sqlite3 as _sq3r
                _rc = _sq3r.connect(_db_path)
                _rc.execute("PRAGMA journal_mode=DELETE")
                _rc.close()
            except Exception as _je:
                logger.warning("api_backup_restore: journal_mode switch failed: %s", _je)
            # Reset engine store so it reopens from the new file — ON THE ENGINE LOOP,
            # so the fresh connection is created and owned by the loop thread and never
            # races a concurrent loop query (same check_same_thread=False hazard as the
            # checkpoint/close above).
            import sys as _sys_r
            _eng_r = _sys_r.modules.get("engine")
            if _eng_r and hasattr(_eng_r, "reset_store"):
                try:
                    if _event_loop and _event_loop.is_running():
                        async def _co_reset():
                            _eng_r.reset_store()
                        _run_on_engine_loop(_co_reset(), timeout=30.0)
                    else:
                        _eng_r.reset_store()
                    logger.info("api_backup_restore: engine store reset OK")
                except Exception as _re:
                    logger.warning("api_backup_restore: engine reset_store failed: %s", _re)

        # Run schema migration (2.0.x legacy full_config_json → normalised tables)
        if "blocks.db" in restored or "blocks.json" in restored:
            try:
                store_mig = _get_store()
                n = store_mig.migrate_full_config_json()
                if n:
                    logger.info(
                        "api_backup_restore: ran migrate_full_config_json — "
                        "%d periods migrated from full_config_json", n
                    )
            except Exception as _mig_e:
                logger.warning("api_backup_restore: schema migration failed: %s", _mig_e)

        # Re-run engine_startup so gap detection runs against the restored DB.
        # resume_engine() alone just unpauses the tick loop — it doesn't detect
        # the session gap between the backup timestamp and now. engine_startup()
        # reads the last finalised block, detects the gap, sets a gap marker and
        # fills missing blocks as soon as the first live sensor read arrives.
        import asyncio as _asyncio_r
        from engine import engine_startup as _engine_startup_r
        if _event_loop and _event_loop.is_running() and _ha_client:
            _asyncio_r.run_coroutine_threadsafe(_engine_startup_r(_ha_client), _event_loop)
            logger.info("api_backup_restore: engine_startup scheduled for gap detection")

        # Regenerate the pre-built charts against the restored DB now (issue #257),
        # before engine_startup's async gap detection — so the UI reflects the
        # restore immediately rather than after the next block finalises.
        if "blocks.db" in restored or "blocks.json" in restored:
            _regen_charts_safely()

        _j["restored"] = restored
        _j["status"] = "done"
        _step("done")
    except Exception as e:
        logger.error("api_backup_restore: %s", e)
        _j["status"] = "error"
        _j["error"] = str(e)
        if staged and os.path.exists(staged):
            try: os.remove(staged)
            except Exception: pass
    finally:
        # Always resume engine — reset_store already called above on success,
        # but we must resume even if an exception occurred mid-restore
        import sys as _sys_fin
        _eng_fin = _sys_fin.modules.get("engine")
        if _eng_fin and hasattr(_eng_fin, "resume_engine"):
            try: _eng_fin.resume_engine()
            except Exception: pass


@app.route("/insights")
def insights_page():
    return render_template("insights.html", active="insights")


@app.route("/settings")
def settings_page():
    tab = request.args.get("tab", "meter-config")
    if tab == "carbon":
        return render_template("settings.html", active="settings")
    # Default: Meter Config tab
    store = _get_store()
    cfg   = load_config()
    tz_select_html = '<select class="js-meta" data-key="timezone"><option value="UTC">UTC</option><option value="Europe/London">Europe/London (UK)</option><option value="Europe/Dublin">Europe/Dublin (Ireland)</option><option value="Europe/Lisbon">Europe/Lisbon (Portugal)</option><option value="Europe/Paris">Europe/Paris (France, Belgium, Netherlands)</option><option value="Europe/Berlin">Europe/Berlin (Germany, Austria)</option><option value="Europe/Amsterdam">Europe/Amsterdam</option><option value="Europe/Rome">Europe/Rome (Italy)</option><option value="Europe/Madrid">Europe/Madrid (Spain)</option><option value="Europe/Stockholm">Europe/Stockholm (Sweden, Norway, Denmark)</option><option value="Europe/Helsinki">Europe/Helsinki (Finland)</option><option value="Europe/Warsaw">Europe/Warsaw (Poland)</option><option value="Europe/Athens">Europe/Athens (Greece)</option><option value="Europe/Istanbul">Europe/Istanbul (Turkey)</option><option value="Europe/Moscow">Europe/Moscow (Russia)</option><option value="America/New_York">America/New_York (US Eastern)</option><option value="America/Chicago">America/Chicago (US Central)</option><option value="America/Denver">America/Denver (US Mountain)</option><option value="America/Los_Angeles">America/Los_Angeles (US Pacific)</option><option value="America/Toronto">America/Toronto (Canada Eastern)</option><option value="America/Vancouver">America/Vancouver (Canada Pacific)</option><option value="America/Sao_Paulo">America/Sao_Paulo (Brazil)</option><option value="Asia/Dubai">Asia/Dubai (UAE)</option><option value="Asia/Kolkata">Asia/Kolkata (India)</option><option value="Asia/Singapore">Asia/Singapore</option><option value="Asia/Tokyo">Asia/Tokyo (Japan)</option><option value="Asia/Shanghai">Asia/Shanghai (China)</option><option value="Australia/Sydney">Australia/Sydney</option><option value="Australia/Perth">Australia/Perth</option><option value="Pacific/Auckland">Pacific/Auckland (New Zealand)</option></select>'
    has_data = bool(store.count_blocks())
    try:
        import engine as _eng
        _ds_mode = _eng.get_data_source_mode()
    except Exception:
        _ds_mode = "cad"
    try:
        import engine as _eng2
        _has_creds = _eng2.has_kraken_credentials()
    except Exception:
        _has_creds = False
    return render_template(
        "meter_config.html",
        config=cfg,
        active="settings",
        tz_select_html=tz_select_html,
        has_data=has_data,
        data_source_mode=_ds_mode,
        has_credentials=_has_creds,
        ohme_detection=_ohme_detection()
    )


@app.route("/api/settings", methods=["GET"])
def api_settings_get():
    """Return current settings merged with defaults."""
    store = _get_store()
    saved = store.get_settings()
    # Merge saved over defaults — any missing key falls back to default
    result = dict(SETTINGS_DEFAULTS)
    result.update(saved)
    return jsonify(result)


@app.route("/api/settings", methods=["POST"])
def api_settings_post():
    """Save settings. Only known keys accepted."""
    store = _get_store()
    data = request.get_json(force=True) or {}
    saved = store.get_settings()
    for key in SETTINGS_DEFAULTS:
        if key in data:
            val = data[key]
            # Numeric fields
            if key in SETTINGS_NUMERIC:
                try:
                    val = float(val)
                    if val <= 0:
                        return jsonify({"error": f"{key} must be positive"}), 400
                except (TypeError, ValueError):
                    return jsonify({"error": f"{key} must be a number"}), 400
            saved[key] = val
    store.save_settings(saved)
    # The ex-VAT / bill-rounding option changes how the billing summary is rendered
    # (bill-method vs inc-VAT), so rebuild the pre-built charts now — otherwise the
    # toggle wouldn't show until the next block finalise (same regen gap as #361).
    if "bill_rounding_summary" in data:
        _schedule_offloaded_regen()
    return jsonify({"ok": True})


@app.route("/api/detect-bcd", methods=["GET"])
def api_detect_bcd():
    """Scan HA states for a BottlecapDave Octopus integration and report what
    EMT can pre-fill (account, MPANs, rate/standing-charge sensors, Mini
    presence). BCD is OPTIONAL — when absent this cleanly returns {found:false}
    and the wizard proceeds on the pure-API path. Never fails hard."""
    try:
        import engine as _eng
        from kraken_api_client import detect_bottlecapdave
        if _ha_client is None:
            return jsonify({"found": False, "detail": "no_ha_client"})
        states = _run_on_engine_loop(_ha_client.get_all_states(), timeout=20.0)
        result = detect_bottlecapdave(states or [])
        return jsonify(result)
    except Exception as e:
        # Detection failure must never block setup — fall back to pure-API path.
        return jsonify({"found": False, "detail": str(e)})


@app.route("/api/data-source-mode", methods=["GET"])
def api_data_source_mode_get():
    """Return the current data-source mode and derived capability flags."""
    try:
        import engine as _eng
        mode = _eng.get_data_source_mode()
        uses_api = _eng.mode_uses_api(mode)
        has_creds = _eng.has_kraken_credentials()
        return jsonify({"mode": mode,
                        "uses_api": uses_api,
                        "uses_mini": _eng.mode_uses_mini(mode),
                        "has_credentials": has_creds,
                        # API mode restored/selected but no stored key — e.g. after
                        # restoring a backup onto a fresh /data (creds live in a
                        # file that backups don't include).
                        "credentials_missing": bool(uses_api and not has_creds)})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/data-source-mode", methods=["POST"])
def api_data_source_mode_post():
    """Set the data-source mode (derived by the setup survey).

    Enforces supplier gating: an API-backed mode (cad+api / api / api+mini) is
    rejected unless the supplier is API-capable. The supplier is taken from the
    request if present, else from the saved config's main meter. This is the
    authoritative gate — the wizard UI hides the API path for non-API suppliers,
    but we never trust the UI alone.
    """
    try:
        import engine as _eng
        data = request.get_json(force=True) or {}
        requested_mode = data.get("mode")
        if _eng.mode_uses_api(requested_mode):
            supplier = data.get("supplier")
            if supplier is None:
                # Fall back to the saved config's supplier.
                try:
                    _cfg = load_config()
                    for _m in (_cfg.get("meters") or {}).values():
                        if (_m.get("meta") or {}).get("sub_meter"):
                            continue
                        supplier = (_m.get("meta") or {}).get("supplier")
                        break
                except Exception:
                    supplier = None
            if not _eng.supplier_is_api_capable(supplier):
                return jsonify({
                    "error": "supplier_not_api_capable",
                    "detail": ("mode %r requires an API-capable supplier; "
                               "supplier %r is local-only"
                               % (requested_mode, supplier or ""))
                }), 400
            # Guard: an API mode needs credentials present (Change Setup cad→api
            # must not strand the user on an API mode with nothing to poll).
            if not _eng.has_kraken_credentials():
                return jsonify({
                    "error": "no_credentials",
                    "detail": ("mode %r requires Kraken API credentials, but none "
                               "are configured" % (requested_mode,))
                }), 400
        mode = _eng.set_data_source_mode(requested_mode)
        return jsonify({"ok": True, "mode": mode})
    except ValueError as ve:
        return jsonify({"error": str(ve)}), 400
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/kraken-config", methods=["GET"])
def api_kraken_config_get():
    """Report whether API credentials are configured and the current connection
    status. NEVER returns the API key itself."""
    try:
        import engine as _eng
        env = _eng._kraken_env()
        configured = bool(env.get("api_key"))
        out = {
            "configured": configured,
            "account_number": env.get("account_number"),
            "connected": _eng.kraken_available(),
            "mini": _eng._kraken_mini_reader is not None,
            # #357: the account this DB was previously connected to (stamped on first
            # discovery). Lets the UI pre-fill the account on a lightweight reconnect
            # after a restore, so re-adding a key doesn't need the full setup wizard.
            "db_account": _eng.get_db_account(),
        }
        # #357: if this DB is stamped to a different account than the credentials,
        # the API was NOT auto-activated — surface it so the UI can prompt an
        # explicit reconnect (credentials are kept, not dropped).
        mm = _eng.kraken_account_mismatch()
        if mm:
            out["account_mismatch"] = True
            out["db_account"] = mm[0]
            out["credentials_account"] = mm[1]
        return jsonify(out)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/kraken-config", methods=["POST"])
def api_kraken_config_post():
    """Save API credentials to the credentials file (outside the DB/backups),
    then run discovery immediately and return the live result. Sending an empty
    api_key disconnects (clears the file)."""
    try:
        import engine as _eng
        data = request.get_json(force=True) or {}
        api_key = data.get("api_key")
        account = data.get("account_number")
        base_url = data.get("base_url")
        _eng.save_kraken_credentials(api_key, account, base_url)
        if not (api_key or "").strip():
            # Full disconnect, not just a file delete: stop polling, tear down the
            # live client + Mini, wipe API-derived state, land on cad. (Deleting
            # the file alone left the in-memory client polling until restart.)
            result = _run_on_engine_loop(_eng.disconnect_kraken(), timeout=30.0)
            return jsonify({"ok": result.get("ok", True), "connected": False,
                            "disconnected": True, "mode": result.get("mode")})
        result = _run_on_engine_loop(_eng.connect_kraken_now(), timeout=45.0)
        if not result.get("ok"):
            # The key was already persisted (save happens above, before the live
            # check). A failed check is usually TRANSIENT — rate limit, a network
            # blip, or a DB mid-swap — not a bad key. Make the save visible so a
            # failed verify never looks like the credentials were dropped, and
            # never auto-delete them here: only Disconnect removes credentials.
            result["saved"] = True
            result["message"] = (
                "Credentials saved, but the connection could not be verified right "
                "now (%s). They have been kept — EMT will connect automatically when "
                "the API is reachable. If the key is wrong, use Disconnect to remove "
                "it." % (result.get("detail") or "unknown"))
        return jsonify(result), (200 if result.get("ok") else 400)
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/api/disconnect-kraken", methods=["POST"])
def api_disconnect_kraken():
    """Destructive 'Disconnect Octopus' action (MODE-UI §5): clear API
    credentials + API-derived state, stop polling immediately, tear down the live
    client/Mini, and land on local-sensor mode ('cad'). Historical DCC-settled
    billing data and the billing source are KEPT. The UI confirms before calling
    this. See engine.disconnect_kraken."""
    try:
        import engine as _eng
        result = _run_on_engine_loop(_eng.disconnect_kraken(), timeout=30.0)
        return jsonify(result), (200 if result.get("ok") else 500)
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


def _parse_version(v: str):
    """'3.2.0' -> (3, 2, 0). Non-numeric parts sort as 0. Tolerates a 'v' prefix."""
    v = (v or "").strip().lstrip("vV")
    parts = []
    for chunk in v.split(".")[:3]:
        digits = "".join(c for c in chunk if c.isdigit())
        parts.append(int(digits) if digits else 0)
    while len(parts) < 3:
        parts.append(0)
    return tuple(parts)


_update_check_cache = {"checked_at": 0.0, "payload": None}
_UPDATE_CHECK_TTL = 86400.0   # once a day — the release cadence is far slower


@app.route("/api/update-check", methods=["GET"])
def api_update_check():
    """BL-6: is a newer release available?

    Supervised users get update badges from the Supervisor; unsupervised (Docker
    / HA Container) users get nothing and only learn of a release by checking the
    repo manually. Cached for a day — a release lookup is not worth a request per
    page load, and GitHub rate-limits unauthenticated calls.

    Never fatal: any failure returns update_available=false, so the UI is silent
    rather than showing an error the user can do nothing about.
    """
    import time
    now = time.time()
    if (_update_check_cache["payload"] is not None
            and now - _update_check_cache["checked_at"] < _UPDATE_CHECK_TTL):
        return jsonify(_update_check_cache["payload"])

    payload = {"current": APP_VERSION, "latest": None, "url": None,
               "update_available": False}
    try:
        import urllib.request
        req = urllib.request.Request(
            "https://api.github.com/repos/RGx01/energy-meter-tracker-addon/releases/latest",
            headers={"Accept": "application/vnd.github+json",
                     "User-Agent": "energy-meter-tracker"})
        with urllib.request.urlopen(req, timeout=8) as resp:
            data = json.loads(resp.read().decode("utf-8"))
        latest = (data.get("tag_name") or "").strip()
        if latest:
            payload["latest"] = latest.lstrip("vV")
            payload["url"] = data.get("html_url")
            # If we cannot determine our OWN version, never claim an update —
            # (0,0,0) < anything would prompt on every release, wrongly.
            payload["update_available"] = bool(APP_VERSION) and (
                _parse_version(latest) > _parse_version(APP_VERSION))
    except Exception as e:
        logger.info("api_update_check: lookup failed (non-fatal): %s", e)

    _update_check_cache.update({"checked_at": now, "payload": payload})
    return jsonify(payload)


@app.route("/api/instance/notice", methods=["GET"])
def api_instance_notice():
    """BL-5: is the open database restored from another instance?

    A native DB carries db_uuid == install_id; a restored one carries the source
    install's id. Computed once at startup (engine.FOREIGN_RESTORE_NOTICE). The
    UI shows a dismissible notice when foreign and not yet acknowledged. Never
    fatal — any error reports "not foreign" so the UI stays silent.
    """
    try:
        import engine as _eng
        notice = getattr(_eng, "FOREIGN_RESTORE_NOTICE", None) or {}
        return jsonify({"foreign": bool(notice.get("foreign")),
                        "db_uuid": notice.get("db_uuid"),
                        "acknowledged": bool(notice.get("acknowledged"))})
    except Exception as e:
        logger.info("api_instance_notice: %s", e)
        return jsonify({"foreign": False, "db_uuid": None, "acknowledged": False})


@app.route("/api/instance/notice/dismiss", methods=["POST"])
def api_instance_notice_dismiss():
    """BL-5: dismiss the foreign-restore notice for a data lineage. Persisted per
    db_uuid in /data (install-scoped), so it survives the next restore — a routine
    repeated restore of the same source is acknowledged once and stays quiet."""
    try:
        import engine as _eng
        import instance as _inst
        notice = getattr(_eng, "FOREIGN_RESTORE_NOTICE", None) or {}
        body = request.get_json(silent=True) or {}
        db_uuid = body.get("db_uuid") or notice.get("db_uuid")
        if not db_uuid:
            return jsonify({"ok": False, "error": "no db_uuid"}), 400
        _inst.acknowledge_db_uuid(db_uuid)
        if isinstance(getattr(_eng, "FOREIGN_RESTORE_NOTICE", None), dict) \
                and _eng.FOREIGN_RESTORE_NOTICE.get("db_uuid") == db_uuid:
            _eng.FOREIGN_RESTORE_NOTICE["acknowledged"] = True
        return jsonify({"ok": True})
    except Exception as e:
        logger.warning("api_instance_notice_dismiss: %s", e)
        return jsonify({"ok": False, "error": str(e)}), 500


def _settlement_horizon_floor() -> str:
    """UTC-ISO cutoff = now − the DCC settlement horizon. Blocks older than this
    can't still be 'awaiting DCC settlement' (settlement lands within days), so
    the unsettled badge/list floors here — keeping historic/imported reconstructed
    blocks (which never settle) out of the count regardless of whether the
    settlement sweep has run to finalise them yet."""
    from datetime import datetime, timezone, timedelta
    try:
        import engine as _eng
        days = int(getattr(_eng, "_SETTLEMENT_SWEEP_HORIZON_DAYS", 14))
    except Exception:
        days = 14
    return (datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(days=days)).isoformat()


@app.route("/api/billing-source", methods=["GET"])
def api_billing_source_get():
    """Return the current global billing source and unsettled-block count."""
    try:
        import engine as _eng
        store = _get_store()
        source = _eng._get_billing_source()
        unsettled = store.count_unsettled_blocks(since_iso=_settlement_horizon_floor())
        _uses_api = _eng.mode_uses_api()
        _has_creds = _eng.has_kraken_credentials()
        return jsonify({"source": source, "unsettled": unsettled,
                        "api_available": _eng.kraken_available(),
                        # The DCC/CAD settlement choice only makes sense with BOTH a
                        # local sensor (CAD) AND a supplier API — i.e. mode 'cad+api'.
                        # Pure-API has no CAD to fall back to; pure-CAD has no DCC.
                        "mode": _eng.get_data_source_mode(),
                        # API/DCC mode is selected but the stored API key is gone
                        # (typically after restoring a backup onto a fresh install
                        # — creds aren't in the backup). Distinct from "no API".
                        "credentials_missing": bool(_uses_api and not _has_creds),
                        "unsupported_tariff": _eng._rate_schedule_unsupported})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/billing-source", methods=["POST"])
def api_billing_source_post():
    """Set the global billing source (cad/dcc). When it changes, all blocks are
    flagged for PASS 2 re-run (the drain re-materialises billing); the response
    carries the flagged count so the UI can warn that recalculation takes time.
    """
    try:
        import engine as _eng
        data = request.get_json(force=True) or {}
        new_source = (data.get("source") or "").lower()
        result = _eng.apply_billing_source_change(new_source)
        return jsonify({"ok": True, **result})
    except ValueError as ve:
        return jsonify({"error": str(ve)}), 400
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/unsettled-blocks")
def api_unsettled_blocks():
    """View-only report of main-meter blocks with no DCC import settlement
    (running on CAD fallback). Relevant when billing_source='dcc'. Returns a
    capped list plus the full count."""
    try:
        store = _get_store()
        limit = int(request.args.get("limit", 500))
        _floor = _settlement_horizon_floor()
        count = store.count_unsettled_blocks(since_iso=_floor)
        rows = store.get_unsettled_blocks(limit=limit, since_iso=_floor)
        out = [{"block_start": r["block_start"],
                "imp_kwh": r["imp_kwh"],
                "exp_kwh": r["exp_kwh"],
                "is_provisional": bool(r["is_provisional"])}
               for r in rows]
        return jsonify({"count": count, "limit": limit, "blocks": out})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── Unified backfill (Heal Gaps + Historical Import) ────────────────────────
def _bf_step_minutes(store) -> int:
    """Block length in minutes from the earliest config period (fallback 30)."""
    try:
        row = store._conn.execute(
            "SELECT block_start FROM blocks ORDER BY block_start LIMIT 1").fetchone()
        if row:
            cp = store.get_config_period_for_date(row[0]) or {}
            return int(cp.get("block_minutes") or 30)
    except Exception:
        pass
    return 30


def _bf_flatten_gap_starts(gaps, step_min, cap=20000):
    """Expand [{start,end,slots}] runs into individual half-hour start ISO strings."""
    from datetime import datetime as _dt, timedelta as _td
    out, step = [], _td(minutes=step_min)
    for g in gaps:
        try:
            t = _dt.fromisoformat(g["start"]); end = _dt.fromisoformat(g["end"])
        except Exception:
            continue
        while t <= end and len(out) < cap:
            out.append(t.isoformat()); t += step
        if len(out) >= cap:
            break
    return out


def _bf_range_starts(from_iso, to_iso, step_min, cap=20000):
    """Every block start in [from, to) as ISO strings (capped)."""
    from datetime import datetime as _dt, timedelta as _td
    out = []
    try:
        t = _dt.fromisoformat(from_iso); end = _dt.fromisoformat(to_iso)
    except Exception:
        return out
    step = _td(minutes=step_min)
    while t < end and len(out) < cap:
        out.append(t.isoformat()); t += step
    return out


@app.route("/api/backfill/plan", methods=["POST"])
def api_backfill_plan():
    """Preview a unified backfill: gate + dispatch + window classification.

    Body: {scope: whole_history|gaps|range, source: api|csv, from?, to?}.
    Read-only — computes what WOULD happen, performs no writes. Delegates the
    decision logic to backfill.plan_backfill; this endpoint only gathers the live
    state (API availability, gaps, occupied windows) it needs.
    """
    try:
        import engine as _eng
        import backfill as _bf
        body   = request.get_json(silent=True) or {}
        scope  = body.get("scope")
        source = body.get("source")
        store  = _get_store()
        api_available = _eng.kraken_available()
        has_blocks    = bool(store.count_blocks())
        gaps          = store.find_block_gaps()

        target_starts = occupied_starts = None
        if scope == "gaps":
            target_starts = _bf_flatten_gap_starts(gaps, _bf_step_minutes(store))
            occupied_starts = []                     # gaps are empty by definition
        elif scope == "range":
            _from, _to = body.get("from"), body.get("to")
            if not _from or not _to:
                return jsonify({"ok": False, "reason": "bad_range",
                                "message": "A start and end date are required."}), 400
            occupied_starts = [r[0] for r in store._conn.execute(
                "SELECT block_start FROM blocks WHERE meter_id='electricity_main' "
                "AND block_start >= ? AND block_start < ? ORDER BY block_start",
                (_from, _to))]
            target_starts = _bf_range_starts(_from, _to, _bf_step_minutes(store))

        plan = _bf.plan_backfill(
            scope=scope, source=source, api_available=api_available,
            has_blocks=has_blocks, gaps=gaps,
            target_starts=target_starts, occupied_starts=occupied_starts)
        plan.update({"api_available": api_available, "has_blocks": has_blocks,
                     "gaps_present": bool(gaps),
                     "gap_slots_total": sum(g.get("slots", 0) for g in gaps)})
        return jsonify(plan), (200 if plan.get("ok") else 400)
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/api/backfill/run", methods=["POST"])
def api_backfill_run():
    """Execute a unified backfill after a preview.

    Re-checks the gate server-side (never trusts the client's earlier preview),
    requires `confirmed`, then dispatches on the plan's action:

      resolve_history_gaps -> run inline on the engine loop  (gaps + API)
      run_api_import_job   -> DEFER to /api/historical/api-import/start, the
                              dedicated background controller (owns backup +
                              pause/resume/cancel).
      apply_csv_import     -> apply here if a full `channel_csvs` dict is supplied;
                              otherwise DEFER to the CSV import wizard (which owns
                              rate/period confirmation).

    A 'deferred' response tells the caller which existing controller to drive.
    """
    try:
        import engine as _eng
        import backfill as _bf
        body   = request.get_json(silent=True) or {}
        scope  = body.get("scope")
        source = body.get("source")
        store  = _get_store()
        api_available = _eng.kraken_available()
        has_blocks    = bool(store.count_blocks())
        gaps          = store.find_block_gaps()

        gate = _bf.evaluate_gates(scope, source, api_available=api_available,
                                  has_blocks=has_blocks, gaps_present=bool(gaps))
        if not gate["allowed"]:
            return jsonify({"ok": False, "reason": gate["reason"],
                            "message": gate["message"]}), 400
        if not body.get("confirmed"):
            return jsonify({"ok": False, "reason": "unconfirmed",
                            "message": "Confirmation required before writing."}), 400

        action = _bf.dispatch_action(scope, source)["action"]

        if action == "resolve_history_gaps":
            result = _run_on_engine_loop(_eng.resolve_history_gaps(), timeout=300.0) or {}
            return jsonify({"ok": bool(result.get("ok", True)),
                            "action": action, **result}), 200

        if action == "apply_csv_import":
            channel_csvs = body.get("channel_csvs")
            if not channel_csvs and body.get("csv"):
                channel_csvs = {"import": body["csv"]}
            if not channel_csvs:
                return jsonify({"ok": True, "action": action, "deferred": True,
                                "use_endpoint": "/api/historical/import-csv",
                                "message": "Upload the CSV via the import wizard "
                                           "(rate/period confirmation happens there)."}), 200
            res = store.apply_csv_import(channel_csvs)
            if isinstance(res, dict) and res.get("blocks_written"):
                try:
                    _regen_charts_safely()   # imported span shows in billing/usage now
                except Exception as _ce:
                    logger.warning("apply_csv_import dispatch: chart regen failed: %s", _ce)
            return jsonify({"ok": True, "action": action, "result": res}), 200

        if action == "run_api_import_job":
            return jsonify({"ok": True, "action": action, "deferred": True,
                            "use_endpoint": "/api/historical/api-import/start",
                            "message": "Start the background API import via its "
                                       "controller (backup + pause/resume/cancel)."}), 200

        return jsonify({"ok": False, "message": "no runner for action '%s'" % action}), 400
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/api/backfill/reach")
def api_backfill_reach():
    """How far back the supplier API can provide data, and the point it fills up to
    (your oldest existing block), for the Import-history 'how far back' display.
    Read-only; flattens plan_api_import. Returns ok:False (not an error) when no API."""
    try:
        import engine as _eng
        if not _eng.kraken_available():
            return jsonify({"ok": False, "reason": "no_api", "available": False}), 200
        plan = _run_on_engine_loop(_eng.plan_api_import(), timeout=60.0) or {}
        if not plan.get("ok"):
            return jsonify({"ok": False, "reason": plan.get("reason") or "unavailable",
                            "note": plan.get("note")}), 200
        chans = plan.get("channels") or {}
        froms = [c["window"]["from"] for c in chans.values()
                 if c.get("ok") and (c.get("window") or {}).get("from")]
        chunks = [c.get("chunk_count", 0) for c in chans.values() if c.get("ok")]
        return jsonify({"ok": True, "available": True,
                        "reach_from": (min(froms) if froms else None),
                        "up_to": plan.get("go_live"),
                        "chunk_estimate": (max(chunks) if chunks else 0)}), 200
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


def _gap_end_inclusive(store, frm, to):
    """A gap's `to` is the LAST missing slot's START (inclusive), but the fill window
    (run_gap_fill_job → import_api_history's until_ts) is HALF-OPEN, so passing `to`
    as-is fetches [from, to) and leaves the final missing half-hour unfilled — the
    "1 block still missing after a gap fill" off-by-one. Extend `to` by one block so
    the last slot is included, mirroring the CSV gap-template's own inclusive step.
    Returns the extended ISO string (or the original `to` unchanged on any failure)."""
    try:
        bm, _tz = _period_block_minutes_tz(store, frm)
        from datetime import datetime as _dt, timedelta as _td
        t = _dt.fromisoformat(str(to).replace("Z", "").split("+")[0])
        return (t + _td(minutes=bm)).isoformat()
    except Exception:
        return to


def _backfill_verify_active(_eng) -> bool:
    """True while the deferred pricing-verification pass is running/waiting/paused.
    A backfill RUN isn't finished until this post-import pass completes, so the
    start endpoints treat it as 'still busy' — no second run can begin until the
    current one (import THROUGH verify) is done. Server-enforced so a page refresh
    or a direct API call can't get round the UI lock."""
    try:
        return (_eng.api_verify_pricing_status() or {}).get("status") in (
            "running", "waiting", "paused")
    except Exception:
        return False


@app.route("/api/backfill/fill-gap", methods=["POST"])
def api_backfill_fill_gap():
    """Fill ONE gap window via the supplier API — as a BACKGROUND job, identical in
    behaviour to the whole/date import but bounded to [from → to] on the gap's
    channel: in-pass price recovery, the verify pass, carbon re-arm, chart rebuild,
    pause/resume — all tracked in the shared import status + pricing-health panel.
    Channel-scoped so a missing-export gap doesn't overwrite present live import
    data. Body: {from, to, channel?, confirmed}. Requires API + confirmation."""
    try:
        import engine as _eng
        import asyncio as _asyncio
        if not _eng.kraken_available():
            return jsonify({"ok": False, "reason": "no_api",
                            "message": "No supplier API configured."}), 400
        body = request.get_json(silent=True) or {}
        frm, to = body.get("from"), body.get("to")
        if not frm or not to:
            return jsonify({"ok": False, "reason": "bad_range",
                            "message": "from and to are required."}), 400
        if not body.get("confirmed"):
            return jsonify({"ok": False, "reason": "unconfirmed",
                            "message": "Confirmation required before writing."}), 400
        if _eng.api_import_running():
            return jsonify({"ok": False, "reason": "busy",
                            "message": "An import is already running.",
                            "status": _eng.api_import_status()}), 409
        if _backfill_verify_active(_eng):
            return jsonify({"ok": False, "reason": "verify_active",
                            "message": "The previous backfill's pricing-verification pass is still "
                                       "finishing. Wait for it to complete before starting another."}), 409
        if not (_event_loop and _event_loop.is_running()):
            return jsonify({"ok": False, "reason": "no_loop",
                            "message": "engine loop not running"}), 500
        ch = body.get("channel")
        channels = (ch,) if ch in ("import", "export") else ("import", "export")
        # Include the gap's final half-hour: `to` is the last missing slot's start
        # (inclusive) but the fill window is half-open — extend by one block.
        to = _gap_end_inclusive(_get_store(), frm, to)
        _asyncio.run_coroutine_threadsafe(
            _eng.run_gap_fill_job(frm, to, channels=channels), _event_loop)
        return jsonify({"ok": True, "status": "running"}), 200
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/api/blocks/reimport", methods=["POST"])
def api_blocks_reimport():
    """BL-8 phase 2: TARGETED restore of a deliberately-deleted range — the explicit
    inverse of Delete. Lifts the tombstone over [from_date, to_date] (sub-span split)
    and IMMEDIATELY re-fills the window from the supplier API. The tombstone lift
    happens inside `run_gap_fill_job` (the shared targeted-fill worker), so this is
    the same 'clear + fill' primitive the per-gap button uses, scoped to a date range.
    Body: {from_date, to_date, meter_id?, channel?, confirmed}."""
    try:
        import engine as _eng
        import asyncio as _asyncio
        from block_store import local_date_range_to_utc_bounds
        if not _eng.kraken_available():
            return jsonify({"ok": False, "reason": "no_api",
                            "message": "No supplier API configured."}), 400
        body = request.get_json(silent=True) or {}
        # Accept EITHER an explicit UTC window (from the deleted-ranges list, exact —
        # no lossy local-date round-trip across a DST boundary) OR local dates.
        from_utc = (body.get("from_utc") or "").strip()
        to_utc   = (body.get("to_utc") or "").strip()
        if not body.get("confirmed"):
            return jsonify({"ok": False, "reason": "unconfirmed",
                            "message": "Confirmation required before writing."}), 400
        if _eng.api_import_running():
            return jsonify({"ok": False, "reason": "busy",
                            "message": "An import is already running.",
                            "status": _eng.api_import_status()}), 409
        if not (_event_loop and _event_loop.is_running()):
            return jsonify({"ok": False, "reason": "no_loop",
                            "message": "engine loop not running"}), 500
        if from_utc and to_utc:
            if from_utc >= to_utc:
                return jsonify({"ok": False, "reason": "bad_range",
                                "message": "from_utc must be before to_utc."}), 400
            utc_start, utc_end = from_utc, to_utc
        else:
            from_date = (body.get("from_date") or "").strip()
            to_date   = (body.get("to_date") or "").strip()
            if not from_date or not to_date:
                return jsonify({"ok": False, "reason": "bad_range",
                                "message": "from_date and to_date (or from_utc and "
                                           "to_utc) are required."}), 400
            if from_date > to_date:
                return jsonify({"ok": False, "reason": "bad_range",
                                "message": "from_date must not be after to_date."}), 400
            _cfg = load_config()
            _tz = "UTC"
            for _md in (_cfg.get("meters") or {}).values():
                if not (_md.get("meta") or {}).get("sub_meter"):
                    _tz = (_md.get("meta") or {}).get("timezone", "UTC")
                    break
            utc_start, utc_end = local_date_range_to_utc_bounds(from_date, to_date, _tz)
        meter_id = body.get("meter_id") or "electricity_main"
        ch = body.get("channel")
        channels = (ch,) if ch in ("import", "export") else ("import", "export")
        _asyncio.run_coroutine_threadsafe(
            _eng.run_gap_fill_job(utc_start, utc_end, channels=channels,
                                  meter_id=meter_id), _event_loop)
        return jsonify({"ok": True, "status": "running",
                        "window": {"from": utc_start, "to": utc_end}}), 200
    except ValueError as e:
        return jsonify({"ok": False, "error": str(e)}), 400
    except Exception as e:
        logger.error("api_blocks_reimport: %s", e)
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/api/blocks/deleted-ranges")
def api_blocks_deleted_ranges():
    """Active deletion tombstones for the Data Management list. Each is annotated
    with `fillable` — whether the supplier API can re-fetch it (advisory only; the
    re-import button uses it to warn/disable, it NEVER auto-clears a tombstone)."""
    try:
        import engine as _eng
        from datetime import datetime, timezone, timedelta
        store = _get_store()
        api_ok = _eng.kraken_available()
        horizon_days = getattr(_eng, "_KRAKEN_BACKFILL_DAYS", 400) or 400
        floor = (datetime.now(timezone.utc).replace(tzinfo=None)
                 - timedelta(days=horizon_days)).isoformat()
        from block_store import utc_to_local_label as _lbl
        _tz = _config_timezone()
        ranges = [{**r, "fillable": bool(api_ok and r["end_utc"] > floor),
                   "start_local": _lbl(r["start_utc"], _tz),
                   "end_local": _lbl(r["end_utc"], _tz)}
                  for r in store.get_deleted_ranges()]
        return jsonify({"ok": True, "ranges": ranges, "api_available": api_ok,
                        "timezone": _tz})
    except Exception as e:
        logger.error("api_blocks_deleted_ranges: %s", e)
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/api/backfill/gaps")
def api_backfill_gaps():
    """Unified "what's missing" for the heal UI. The user doesn't distinguish, so
    we merge two kinds into one labelled list:

      * missing BLOCK rows  — find_block_gaps() (import channel, interior holes).
      * missing per-channel DATA — the import's own self-clearing record for the
        import AND export channels (engine.api_import_gaps): windows where blocks
        exist but that channel's data is absent (e.g. a settlement/DCC outage).

    Each entry carries channel + kind so the fill can pick the right template.
    `fetchable` marks entries an API can (re)fetch. Read-only."""
    try:
        import engine as _eng
        store = _get_store()
        api_available = _eng.kraken_available()
        # Does the main meter export? A whole missing BLOCK then needs BOTH channels
        # filled (import AND export), not just import — otherwise the CSV template and
        # the API fill silently omit export for that window.
        try:
            _cfg = load_config()
            _main = next((m for m in _cfg.get("meters", {}).values()
                          if not (m.get("meta") or {}).get("sub_meter")), {})
            has_export = "export" in (_main.get("channels") or {})
        except Exception:
            has_export = False
        block_channels = ["import", "export"] if has_export else ["import"]
        gaps = []
        # (1) Missing block rows — the whole block is absent, so it needs every
        # configured channel (import, and export where the meter exports).
        for g in store.find_block_gaps():
            slots = int(g.get("slots") or 0)
            # BL-8 phase 2: flag a hole that is a deliberate deletion (tombstoned) so
            # the UI can label it "deleted — fill to restore" rather than as a fault.
            # It still appears here so the user can TARGET it for a restore; only the
            # blanket "recover all" and the automatic poll step over it.
            _deleted = store.is_slot_tombstoned("electricity_main", g.get("start"))
            gaps.append({"start": g.get("start"), "end": g.get("end"), "slots": slots,
                         "hours": round(slots / 2.0, 1), "channel": "import",
                         "channels": list(block_channels),
                         "kind": "missing_blocks", "fetchable": True,
                         "deleted": _deleted})
        # (2) Missing per-channel data recorded by the import (import + export).
        imp = _eng.api_import_gaps() or {}
        for ch, cd in (imp.get("channels") or {}).items():
            for g in (cd.get("gaps") or []):
                cnt = int(g.get("count") or 0)
                if not g.get("from"):
                    continue
                gaps.append({"start": g.get("from"), "end": g.get("to") or g.get("from"),
                             "slots": cnt, "hours": round(cnt / 2.0, 1), "channel": ch,
                             "channels": [ch],
                             "kind": "missing_data", "fetchable": api_available})
        gaps.sort(key=lambda x: (x.get("start") or ""), reverse=True)
        # Gap start/end are naive-UTC block_start; annotate wall-clock (local) so the
        # UI shows the times the user thinks in, not UTC. start/end stay UTC (the
        # fill/re-import endpoints need them).
        from block_store import utc_to_local_label as _lbl
        _tz = _config_timezone()
        for g in gaps:
            g["start_local"] = _lbl(g.get("start"), _tz)
            g["end_local"] = _lbl(g.get("end"), _tz)
        return jsonify({
            "ok": True, "gaps": gaps, "timezone": _tz,
            "total_slots": sum(g["slots"] for g in gaps),
            "fetchable_slots": sum(g["slots"] for g in gaps if g["fetchable"]),
            "api_available": api_available,
        })
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/api/history-gaps")
def api_history_gaps():
    """Review: missing half-hour slots in the block history (outage holes).

    Read-only, and deliberately NOT gated on the supplier API: finding gaps is a
    question about our own blocks table, which a CAD-only user can ask and get a
    true answer to. `api_available` is returned so the UI can explain that
    *recovering* them does need a supplier (the readings can only come from there)
    rather than silently hiding the action.
    """
    try:
        import engine as _eng
        store = _get_store()
        gaps = store.find_block_gaps()
        return jsonify({"ok": True, "gaps": gaps,
                        "total_slots": sum(g["slots"] for g in gaps),
                        "api_available": _eng.kraken_available()})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/api/resolve-gaps", methods=["POST"])
def api_resolve_gaps():
    """Act: backfill history gaps from settled supplier data.

    User-triggered on purpose — an automatic sweep could not tell an outage gap
    from blocks the user deleted deliberately, and would resurrect them.
    """
    try:
        import engine as _eng
        if not _eng.kraken_available():
            return jsonify({"ok": False, "reason": "no_api",
                            "error": "No supplier API configured"}), 400
        result = _run_on_engine_loop(_eng.resolve_history_gaps(), timeout=300.0)
        return jsonify(result), (200 if result.get("ok") else 400)
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/api/retry-settlement", methods=["POST"])
def api_retry_settlement():
    """User-triggered re-fetch of DCC over oldest-unsettled → now, to settle
    late/old gaps on demand. Requires a configured Kraken API. Runs the async
    retry on the engine's event loop (server handlers are sync)."""
    try:
        import engine as _eng
        if not _eng.kraken_available():
            return jsonify({"ok": False, "reason": "no_api",
                            "error": "No supplier API configured"}), 400
        result = _run_on_engine_loop(_eng.retry_settlement_for_unsettled(),
                                     timeout=120.0)
        # Settlement rewrote block figures (e.g. late export finally landing) — the
        # pre-built charts still show the pre-settlement values until regenerated.
        # Rebuild off the loop, but only when something actually changed.
        if result.get("ok") and (result.get("settled_import")
                                 or result.get("settled_export")):
            try:
                _run_on_engine_loop(_eng._generate_charts_offloaded(), timeout=300.0)
            except Exception as _e:
                logger.warning("retry-settlement: chart regen failed: %s", _e)
        status = 200 if result.get("ok") else 400
        return jsonify(result), status
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/api/historical/probe", methods=["POST"])
def api_historical_probe():
    """READ-ONLY beta diagnostic (historical-import spike): probe HA long-term
    statistics for the chosen per-device sensors, to characterise retention,
    cadence, gaps, energy-vs-power and DST/timestamp behaviour before building
    the historical import. Creates no blocks and writes nothing.

    Body: {"entity_ids": [...] } or {"sensors": [{"device","entity_id"},...]},
    optional "days" (lookback, default 800, max 2000)."""
    try:
        import engine as _eng
        body = request.get_json(force=True, silent=True) or {}
        entity_ids = body.get("entity_ids") or [
            (s or {}).get("entity_id") for s in (body.get("sensors") or [])]
        entity_ids = [e for e in entity_ids if e]
        if not entity_ids:
            return jsonify({"ok": False, "error": "no sensors selected"}), 400
        # No look-back control any more — probe the full retained history. HA only
        # keeps so much, so a generous window == "everything there is". A caller can
        # still pass days to narrow it.
        try:
            days = max(1, min(int(body.get("days", 3660)), 3660))
        except (TypeError, ValueError):
            days = 3660
        result = _run_on_engine_loop(
            _eng.probe_recorder_statistics(entity_ids, days), timeout=300.0)
        return jsonify(result), (200 if result.get("ok") else 400)
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/api/historical/probe-devices", methods=["GET"])
def api_probe_devices():
    """List configured meters/devices with the sensor entities currently assigned to
    each, so the recorder-history probe can pre-populate itself. Read-only."""
    try:
        from datetime import datetime as _dt
        cfg = load_config()
        today = _dt.now().strftime("%Y-%m-%d")
        # Sensor ledger: if a device still has a reconstructed layer, remember the
        # sensor(s) that built it (from the run ledger) and pre-populate them until
        # that layer is removed — so revisiting the page shows what was used.
        store = _get_store()
        _latest_run_sensors = {}
        for _r in (store.get_attribution_runs() or []):
            _mid = _r.get("meter_id")
            if _mid and _r.get("sensor_ids"):
                _latest_run_sensors[_mid] = list(_r["sensor_ids"])   # appended in order → last wins
        _attributed_meters = {m["meter_id"] for m in
                              (store.count_recorder_attributed().get("meters") or [])}
        devices = []
        for m_id, m_data in (cfg.get("meters") or {}).items():
            meta = (m_data or {}).get("meta", {}) or {}
            if not meta.get("sub_meter"):
                continue                       # main meter = the house control total,
                                               # not an attributable device — omit it
            if meta.get("retired_at") and meta["retired_at"] <= today:
                continue                       # skip retired sub-meters
            is_sub = True
            label = meta.get("device") or m_id
            sensors = []
            for ch in (m_data.get("channels") or {}).values():
                rd = (ch or {}).get("read")    # per-channel energy read sensor
                if rd:
                    sensors.append(rd)
            for k in ("power_sensor", "device_power_sensor", "soc_sensor",
                      "inverter_power_sensor", "pv_power_sensor"):
                v = meta.get(k)
                if v:
                    sensors.append(v)
            seen = set()
            sensors = [s for s in sensors if not (s in seen or seen.add(s))]
            # Kept sensors from the last reconstruction, while its layer still exists.
            history_sensors = (_latest_run_sensors.get(m_id, [])
                               if m_id in _attributed_meters else [])
            devices.append({
                "meter_id": m_id, "label": label, "is_sub_meter": is_sub,
                "meter_type": meta.get("meter_type") or ("sub" if is_sub else "main"),
                "sensors": sensors,
                "history_sensors": history_sensors,
            })
        devices.sort(key=lambda d: (d["is_sub_meter"], d["label"].lower()))
        return jsonify({"ok": True, "devices": devices})
    except Exception as e:
        logger.error("api_probe_devices: %s", e)
        return jsonify({"ok": False, "error": str(e), "devices": []}), 500


@app.route("/api/historical/attribute/start", methods=["POST"])
def api_attribute_start():
    """Start a recorder device-attribution run as a BACKGROUND job. Body:
    {meter_id, sensor_ids:[...]}. Takes a backup, schedules the worker on the engine
    loop, returns at once. Poll /status, drive /control, undo via /backout."""
    try:
        import engine as _eng
        import asyncio as _asyncio
        body = request.get_json(force=True, silent=True) or {}
        meter_id = (body.get("meter_id") or "").strip()
        sensor_ids = [s for s in (body.get("sensor_ids") or []) if s]
        if not meter_id or not sensor_ids:
            return jsonify({"ok": False, "error": "meter_id and at least one sensor required"}), 400
        # Attribution is for sub-meter DEVICES only. The main meter is the house
        # control total — its usage is the remainder, never a reconstructed device.
        _meta = ((load_config().get("meters") or {}).get(meter_id) or {}).get("meta") or {}
        if not _meta.get("sub_meter"):
            return jsonify({"ok": False, "error": "attribution applies to sub-meter "
                            "devices only; the house total is the control total"}), 400
        if _eng.attribution_running():
            return jsonify({"ok": False, "error": "an attribution run is already active",
                            "status": _eng.api_attribution_status()}), 409
        if not (_event_loop and _event_loop.is_running()):
            return jsonify({"ok": False, "error": "engine loop not running"}), 500
        # Sanity-check the chosen sensor(s) against the house import before writing:
        # a device that totals MORE than the house drew (or is absurdly small) is
        # almost certainly the wrong sensor or a Wh/kWh mix-up. The user can still
        # proceed by re-submitting with confirm_override.
        if not body.get("confirm_override"):
            try:
                pf = _asyncio.run_coroutine_threadsafe(
                    _eng.attribution_preflight(meter_id, sensor_ids), _event_loop
                ).result(timeout=180)
            except Exception as _pe:
                logger.warning("attribution preflight failed (continuing): %s", _pe)
                pf = {"verdict": "error"}
            _v = pf.get("verdict")
            if _v in ("device_exceeds_house", "suspiciously_small"):
                _dk, _hk = pf.get("device_kwh"), pf.get("house_kwh")
                _sp = ((pf.get("from") or "")[:10] + " → " + (pf.get("to") or "")[:10])
                _msg = (("This sensor totals about %s kWh over %s, but the house only "
                         "imported %s kWh in that time. A device can't exceed the house it "
                         "draws from — this looks like the wrong sensor, or a Wh/kWh mismatch."
                         % (_dk, _sp, _hk)) if _v == "device_exceeds_house" else
                        ("This sensor totals only about %s kWh over %s vs the house's %s kWh — "
                         "unusually small, which can mean a unit mismatch or the wrong sensor."
                         % (_dk, _sp, _hk)))
                return jsonify({"ok": False, "sanity": _v, "warn": _msg,
                                "device_kwh": _dk, "house_kwh": _hk}), 409
        backup = None
        try:
            backup = os.path.basename(_create_backup_zip(label="pre-attribution"))
        except Exception as be:
            logger.warning("attribution: backup failed (continuing): %s", be)
        _asyncio.run_coroutine_threadsafe(
            _eng.run_attribution_job(meter_id, sensor_ids, pace_s=0.05), _event_loop)
        return jsonify({"ok": True, "status": "running", "backup": backup})
    except Exception as e:
        logger.error("api_attribute_start: %s", e)
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/api/historical/attribute/status", methods=["GET"])
def api_attribute_status():
    """Poll the background attribution job."""
    import engine as _eng
    return jsonify(_eng.api_attribution_status())


@app.route("/api/historical/attribute/control", methods=["POST"])
def api_attribute_control():
    """pause / resume / cancel the running attribution job."""
    import engine as _eng
    action = ((request.get_json(force=True, silent=True) or {}).get("action") or "").lower()
    if action not in ("pause", "resume", "cancel"):
        return jsonify({"ok": False, "error": "action must be pause/resume/cancel"}), 400
    return jsonify({"ok": True, **_eng.attribution_control(action)})


@app.route("/api/historical/attribute/runs", methods=["GET"])
def api_attribute_runs():
    """The attribution run ledger + a summary of the reconstructed device layer,
    for the runs/undo list."""
    store = _get_store()
    return jsonify({"ok": True, "runs": store.get_attribution_runs(),
                    "summary": store.count_recorder_attributed()})


@app.route("/api/historical/attribute/backout", methods=["POST"])
def api_attribute_backout():
    """Undo an attribution run (by run_id) or an explicit device+range: deletes the
    reconstructed device blocks and re-derives the parent remainder. Refused while a
    run is active, while another back-out is already in flight, or for a run that is
    no longer in the ledger (already undone) — so a double-click can't misfire."""
    try:
        import engine as _eng
        if _eng.attribution_running():
            return jsonify({"ok": False, "error": "stop the running attribution first"}), 409
        if _eng.backout_running():
            return jsonify({"ok": False, "error": "an undo is already in progress",
                            "status": _eng.api_backout_status()}), 409
        body = request.get_json(force=True, silent=True) or {}
        res = _eng.backout_recorder_attribution(
            run_id=body.get("run_id"), meter_id=body.get("meter_id"),
            from_date=body.get("from"), to_date=body.get("to"))
        if not res.get("ok"):
            err = res.get("error")
            if err == "backout_in_progress":
                return jsonify({"ok": False, "error": "an undo is already in progress",
                                "status": _eng.api_backout_status()}), 409
            if err == "run_not_found":
                return jsonify({"ok": False, "error": "that run was already undone "
                                "(or no longer exists)"}), 409
            return jsonify(res), 500
        return jsonify(res)
    except Exception as e:
        logger.error("api_attribute_backout: %s", e)
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/api/historical/attribute/backout/status", methods=["GET"])
def api_attribute_backout_status():
    """Poll whether an undo is in flight (UI disables the undo buttons)."""
    import engine as _eng
    return jsonify(_eng.api_backout_status())


@app.route("/api/historical/export-probe", methods=["POST"])
def api_export_probe():
    """READ-ONLY beta diagnostic (historical-import spike): measure how far back
    the Octopus API reaches for import vs export, via cheap single-row boundary
    fetches. No body — uses the discovered meters. Creates nothing, writes
    nothing. Gated on an available API (nothing to probe otherwise)."""
    try:
        import engine as _eng
        if not _eng.kraken_available():
            return jsonify({"ok": False, "error": "no API connected"}), 400
        result = _run_on_engine_loop(
            _eng.probe_consumption_retention(), timeout=60.0)
        return jsonify(result), (200 if result.get("ok") else 400)
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/api/historical/consumption-diagnostic", methods=["POST"])
def api_consumption_diagnostic():
    """READ-ONLY deep diagnostic: enumerate every meter point + serial on the
    account with each serial's reachable consumption span, to tell apart an API
    retention limit, a meter exchange, or a query artifact. No body. Writes
    nothing. Gated on an available API."""
    try:
        import engine as _eng
        if not _eng.kraken_available():
            return jsonify({"ok": False, "error": "no API connected"}), 400
        result = _run_on_engine_loop(
            _eng.diagnose_consumption_retention(), timeout=180.0)
        return jsonify(result), (200 if result.get("ok") else 400)
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/api/historical/import-range-diagnostic", methods=["POST"])
def api_import_range_diagnostic():
    """READ-ONLY: report where each channel's import starts (its earliest supplier
    agreement) and probe the Measurements API for data BEFORE that floor — so we
    can tell whether real history exists earlier than where the import began
    (e.g. import from 01/07/24, export from a later export-agreement date). Writes
    nothing. Gated on an available API."""
    try:
        import engine as _eng
        if not _eng.kraken_available():
            return jsonify({"ok": False, "error": "no API connected"}), 400
        result = _run_on_engine_loop(
            _eng.diagnose_import_range(), timeout=120.0)
        return jsonify(result), (200 if result.get("ok") else 400)
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/api/historical/site-plan", methods=["GET"])
def api_historical_site_plan():
    """READ-ONLY. Discover the account's property history for the PRE-import
    'confirm your sites' step. Returns {ok, needs_confirmation, sites[]}. When
    needs_confirmation is False (never-moved account), the wizard skips straight
    to the import. Writes nothing."""
    try:
        import engine as _eng
        if not _eng.kraken_available():
            return jsonify({"ok": False, "error": "no API connected"}), 400
        result = _run_on_engine_loop(_eng.discover_pre_import_sites(), timeout=60.0)
        return jsonify(result), (200 if result.get("ok") else 400)
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/api/historical/site-plan/apply", methods=["POST"])
def api_historical_site_plan_apply():
    """Create/extend config periods to match the user-confirmed tenancy sites
    BEFORE the import runs, so imported blocks land in the right period from the
    first write. Body: {"sites": [{outcode, from, to, site_name?}]}. Sets a marker
    so the post-import region probe doesn't re-stamp the layout. Backs up first."""
    try:
        store = _get_store()
        data = request.get_json(force=True, silent=True) or {}
        sites = data.get("sites") or []
        if not sites:
            return jsonify({"ok": False, "error": "no sites supplied"}), 400
        try:
            backup = os.path.basename(_create_backup_zip(label="pre-import-sites"))
        except Exception as be:
            backup = None
            logger.warning("site-plan apply: backup failed (continuing): %s", be)
        from datetime import datetime as _dt_now
        res = store.apply_pre_import_sites(sites)
        store.set_meta("preimport_sites_applied", _dt_now.utcnow().isoformat())
        store.rearm_carbon_backfill()   # new regions → re-scan carbon after import
        try:
            _rebuild_config_period_chain(store)
        except Exception as _e:
            logger.warning("site-plan apply: chain rebuild failed: %s", _e)
        logger.info("api_historical_site_plan_apply: %s", res)
        return jsonify({"ok": True, "backup": backup, **res})
    except Exception as e:
        logger.error("api_historical_site_plan_apply: %s", e)
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/api/historical/api-import/plan", methods=["POST"])
def api_historical_api_plan():
    """READ-ONLY preview for the API import route: the per-channel window and
    newest→oldest chunk plan (clamped to the ~2-year wall + go-live). No writes."""
    try:
        import engine as _eng
        if not _eng.kraken_available():
            return jsonify({"ok": False, "error": "no API connected"}), 400
        body = request.get_json(force=True, silent=True) or {}
        try:
            chunk_days = max(1, min(int(body.get("chunk_days", 60)), 120))
        except (TypeError, ValueError):
            chunk_days = 60
        result = _run_on_engine_loop(
            _eng.plan_api_import(body.get("from"), chunk_days=chunk_days), timeout=60.0)
        return jsonify(result), (200 if result.get("ok") else 400)
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/api/historical/api-import/apply", methods=["POST"])
def api_historical_api_apply():
    """API import route apply: fetch consumption newest→oldest, price via
    per-agreement historical schedules, write imported_api blocks (backup first).
    `dry_run` previews counts without writing; a real write requires confirmed=true.
    Reversible via the reconstructed-history delete filter."""
    try:
        import engine as _eng
        if not _eng.kraken_available():
            return jsonify({"ok": False, "error": "no API connected"}), 400
        body = request.get_json(force=True, silent=True) or {}
        dry_run = bool(body.get("dry_run"))
        if not dry_run and not body.get("confirmed"):
            return jsonify({"ok": False, "error": "confirmation required"}), 400
        restart = bool(body.get("restart"))
        try:
            chunk_days = max(1, min(int(body.get("chunk_days", 60)), 120))
        except (TypeError, ValueError):
            chunk_days = 60
        try:
            max_chunks = max(1, min(int(body.get("max_chunks", 6)), 60))
        except (TypeError, ValueError):
            max_chunks = 6
        try:
            pace_s = min(max(float(body.get("pace_s", 1.5)), 0.0), 10.0)
        except (TypeError, ValueError):
            pace_s = 1.5
        # Backup once, at the start of a run (the first / restart call), not on
        # every incremental continuation.
        backup = None
        if not dry_run and restart:
            try:
                backup = os.path.basename(_create_backup_zip(label="pre-api-import"))
            except Exception as be:
                logger.warning("api import: backup failed (continuing): %s", be)
        result = _run_on_engine_loop(
            _eng.import_api_history(body.get("from"), chunk_days=chunk_days,
                                    max_chunks=max_chunks, pace_s=pace_s,
                                    dry_run=dry_run, restart=restart), timeout=300.0)
        result["backup"] = backup
        return jsonify(result), (200 if result.get("ok") else 400)
    except Exception as e:
        logger.error("api_historical_api_apply: %s", e)
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/api/historical/api-import/start", methods=["POST"])
def api_historical_api_start():
    """Start the API import as a BACKGROUND job (survives navigating away; can be
    paused/resumed/cancelled). Takes a backup, then schedules the worker on the
    engine loop and returns immediately. Poll /status; drive /control."""
    try:
        import engine as _eng
        import asyncio as _asyncio
        if not _eng.kraken_available():
            return jsonify({"ok": False, "error": "no API connected"}), 400
        body = request.get_json(force=True, silent=True) or {}
        if not body.get("confirmed"):
            return jsonify({"ok": False, "error": "confirmation required"}), 400
        if _eng.api_import_running():
            return jsonify({"ok": False, "error": "an import is already running",
                            "status": _eng.api_import_status()}), 409
        if _backfill_verify_active(_eng):
            return jsonify({"ok": False, "reason": "verify_active",
                            "error": "The previous backfill's pricing-verification pass is still "
                                     "finishing. Wait for it to complete before starting another."}), 409
        if not (_event_loop and _event_loop.is_running()):
            return jsonify({"ok": False, "error": "engine loop not running"}), 500
        try:
            chunk_days = max(1, min(int(body.get("chunk_days", 60)), 120))
        except (TypeError, ValueError):
            chunk_days = 60
        try:
            max_chunks = max(1, min(int(body.get("max_chunks", 1)), 60))
        except (TypeError, ValueError):
            max_chunks = 1
        try:
            pace_s = min(max(float(body.get("pace_s", 1.5)), 0.0), 10.0)
        except (TypeError, ValueError):
            pace_s = 1.5
        backup = None
        try:
            backup = os.path.basename(_create_backup_zip(label="pre-api-import"))
        except Exception as be:
            logger.warning("api import: backup failed (continuing): %s", be)
        _asyncio.run_coroutine_threadsafe(
            _eng.run_api_import_job(body.get("from"), chunk_days=chunk_days,
                                    max_chunks=max_chunks, pace_s=pace_s),
            _event_loop)      # fire-and-forget; poll /status
        return jsonify({"ok": True, "status": "running", "backup": backup})
    except Exception as e:
        logger.error("api_historical_api_start: %s", e)
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/api/historical/api-import/control", methods=["POST"])
def api_historical_api_control():
    """pause / resume / cancel the running background import."""
    try:
        import engine as _eng
        body = request.get_json(force=True, silent=True) or {}
        return jsonify(_eng.api_import_control(body.get("action", "")))
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/api/historical/api-import/status", methods=["GET"])
def api_historical_api_status():
    """Poll the background import job's progress/status."""
    try:
        import engine as _eng
        return jsonify(_eng.api_import_status())
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/api/historical/api-import/gaps", methods=["GET"])
def api_historical_api_gaps():
    """Persisted reconstruction gaps (missing half-hour runs) per channel from the
    last import — so the import page can show holes without re-querying. Read-only."""
    try:
        import engine as _eng
        return jsonify(_eng.api_import_gaps())
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/api/historical/api-import/health", methods=["GET"])
def api_historical_api_health():
    """Post-import health summary (flags raised / auto-recovered / still remaining,
    plus imported range) for the import page's persistent panel. 'remaining' is read
    live from the reprice queue. Read-only."""
    try:
        import engine as _eng
        return jsonify(_eng.api_import_health())
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/api/historical/api-import/verify-status", methods=["GET"])
def api_historical_verify_status():
    """Live status of the deferred, rate-limit-gated pricing-verification pass that
    runs after an import (chunk progress, slots corrected, waiting-for-allowance).
    Read-only."""
    try:
        import engine as _eng
        return jsonify(_eng.api_verify_pricing_status())
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/api/historical/reprice-repair", methods=["POST"])
def api_historical_reprice_repair():
    """Calm, targeted re-price of imported half-hours the bulk import mispriced
    (Measurements returned kWh but empty cost metaData under load). Re-queries the
    slots one small window at a time and re-prices in place. Also the root-cause
    test: the response's recovered/still_missing split says whether a calm re-fetch
    recovers what the loaded import missed (⇒ load-induced) or not (⇒ real gap).

    Body: {from_date?, to_date?, preview?, include_export?}. YYYY-MM-DD; omit dates
    to use the reprice queue. `preview` counts the affected blocks WITHOUT running
    (for a confirm step). Range mode defaults to IMPORT-only; set `include_export`
    to also reprice export — now that _billed_rate prefers the flat/Agile schedule
    for a published-rate slot, this COLLAPSES a fragmented export rate back to its
    clean scheduled value (it no longer fragments it from cost÷kWh)."""
    try:
        import engine as _eng
        if not _eng.kraken_available():
            return jsonify({"ok": False, "error": "no API connected"}), 400
        # Guard against a concurrent API pass: the deferred verify-pricing pass
        # fetches from Octopus on the shared rate-limit allowance. Running a reprice
        # at the same time would compete for the same budget (and re-check the same
        # slots), so refuse while verify is live — the UI greys the Retry button to
        # match, and the verify pass covers these slots anyway.
        try:
            _vs = _eng.api_verify_pricing_status() or {}
        except Exception:
            _vs = {}
        if _vs.get("status") in ("running", "waiting", "paused"):
            return jsonify({"ok": False, "reason": "verify_active",
                            "error": "A pricing verification pass is running. It shares the "
                                     "Octopus rate-limit allowance and already re-checks these "
                                     "slots — please wait for it to finish, then retry if any "
                                     "still need it."}), 409
        body = request.get_json(force=True, silent=True) or {}
        from_date = (body.get("from_date") or "").strip() or None
        to_date = (body.get("to_date") or "").strip() or None
        preview = bool(body.get("preview"))
        # Date validation: end must not precede start.
        if from_date and to_date and to_date < from_date:
            return jsonify({"ok": False,
                            "error": "The 'to' date can't be before the 'from' date."}), 400
        # Range mode defaults to import-only (the tool's purpose); queue mode keeps
        # whatever was flagged. include_export opts back into export re-pricing.
        if from_date or to_date:
            channels = ("import", "export") if body.get("include_export") else ("import",)
        else:
            channels = ("import", "export")
        result = _run_on_engine_loop(
            _eng.repair_import_pricing(from_date, to_date, channels=channels,
                                       count_only=preview),
            timeout=(60.0 if preview else 1800.0))
        return jsonify(result), (200 if result.get("ok") else 400)
    except Exception as e:
        logger.error("api_historical_reprice_repair: %s", e)
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/api/historical/retag-imports", methods=["POST"])
def api_historical_retag_imports():
    """Repair blocks the carbon round-trip bug wiped from 'imported_api' to NULL:
    re-tag reconstruction blocks (pre-go-live, no meter read) as imported_api.
    No re-import, no API cost — pure local repair. Returns {retagged, go_live}."""
    try:
        store = _get_store()
        res = store.retag_untagged_imports()
        try:
            _rebuild_config_period_chain(store)
        except Exception as _e:
            logger.warning("api_historical_retag_imports: chain rebuild failed: %s", _e)
        logger.info("api_historical_retag_imports: %s", res)
        return jsonify({"ok": True, **res})
    except Exception as e:
        logger.error("api_historical_retag_imports: %s", e)
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/api/blocks/sweep-implausible", methods=["GET", "POST"])
def api_blocks_sweep_implausible():
    """#307 one-off repair for lost-opener device spikes (e.g. house_battery
    read_start=0 / read_end=6137 → 6137 kWh in a half-hour, wrecking carbon). GET
    previews the offending blocks (dry-run); POST applies: clamp imp_kwh to the
    grid-bounded value, baseline the register, recompute carbon, flag needs_review
    — leaving imp_kwh_grid / imp_cost (billing) untouched. Regenerates charts on
    apply. Returns {ok, count, ceiling_kwh, applied, blocks:[...]}."""
    try:
        store = _get_store()
        apply = (request.method == "POST")
        res = store.sweep_implausible_sub_blocks(dry_run=not apply)
        if apply and res.get("applied"):
            try:
                _regen_charts_safely()
            except Exception as _e:
                logger.warning("api_blocks_sweep_implausible: chart regen failed: %s", _e)
        logger.info("api_blocks_sweep_implausible: %s (applied=%s)",
                    {k: res[k] for k in ("count", "ceiling_kwh", "applied")}, apply)
        return jsonify({"ok": True, **res})
    except Exception as e:
        logger.error("api_blocks_sweep_implausible: %s", e)
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/api/carbon/pause", methods=["GET", "POST"])
def api_carbon_pause():
    """Get or set the carbon-backfill kill switch. POST {"paused": true|false}.
    While paused, the historical backfill and live recovery both stand down."""
    try:
        store = _get_store()
        if request.method == "POST":
            data = request.get_json(force=True, silent=True) or {}
            paused = bool(data.get("paused"))
            store.set_meta("carbon_paused", True if paused else None)
            logger.info("api_carbon_pause: carbon backfill %s",
                        "PAUSED" if paused else "resumed")
            return jsonify({"ok": True, "paused": paused})
        return jsonify({"ok": True, "paused": bool(store.get_meta("carbon_paused", None))})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/api/historical/reprice-from-csv", methods=["POST"])
def api_historical_reprice_from_csv():
    """Overlay exact billed cost/rate onto imported blocks from an Octopus CSV
    export (the billing source of truth) — the only way to price dispatch slots
    Octopus's Measurements API returns with no cost. Body: {csv, channel?}."""
    try:
        store = _get_store()
        data = request.get_json(force=True, silent=True) or {}
        csv_text = (data.get("csv") or data.get("import_csv") or "").strip()
        channel = data.get("channel") or "import"
        if not csv_text:
            return jsonify({"ok": False, "error": "no CSV supplied"}), 400
        res = store.reprice_imported_blocks_from_csv(csv_text, channel=channel)
        if res.get("changed"):
            try:
                import engine as _eng
                _run_on_engine_loop(_eng._generate_charts_offloaded(), timeout=300.0)
            except Exception as _e:
                logger.warning("reprice-from-csv: chart regen failed: %s", _e)
        return jsonify({"ok": True, **res})
    except Exception as e:
        logger.error("api_historical_reprice_from_csv: %s", e)
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/historical-import")
def historical_import_page():
    """Historical import wizard (3.5.0). Currently the CSV route: upload the
    Octopus per-channel export, review the cost-derived rates, confirm, import."""
    return render_template("historical_import.html", active="data_management")


def _csv_channel_texts(body):
    texts = {}
    for channel in ("import", "export"):
        t = body.get(channel + "_csv") or body.get(channel)
        if t:
            texts[channel] = t
    return texts


@app.route("/api/historical/bills/build", methods=["POST"])
def api_historical_bills_build():
    """Parse uploaded Octopus PDF bills → per-(MPAN, channel) CSV TEXT, ready to feed
    the normal CSV preview/apply. Multipart form: `files` (one or more PDFs, the
    user's local folder uploaded by the browser). Groups by import MPAN — a folder
    can span a house move — and each group yields import + export CSV plus a
    reconciliation report and any shape-approximation warnings.

    Strictly upstream of block-writing: nothing is imported here. The generated CSV
    is returned to the client for review, then flows through the existing CSV path."""
    try:
        import bill_parser as _bp
        if not _bp.pdf_support():
            return jsonify({"ok": False,
                            "error": "PDF support (pypdf) isn't available in this build."}), 400
        files = request.files.getlist("files") or []
        # Only consider PDFs (a folder upload may include other files).
        pdfs = [f for f in files if (f.filename or "").lower().endswith(".pdf")]
        if not pdfs:
            return jsonify({"ok": False, "error": "No PDF files were uploaded."}), 400

        parsed, failed = [], []
        for f in pdfs:
            try:
                b = _bp.parse_bill(f.read(), source_name=f.filename or "bill.pdf")
                parsed.append(b)
            except Exception as e:
                failed.append({"file": f.filename, "error": str(e)})

        groups = {}
        for b in parsed:
            groups.setdefault(b.mpan_import or "unknown", []).append(b)

        def _dedupe(rows):        # later bill wins on overlap / re-issue (§6)
            by = {}
            for r in rows:
                by[r["Start"]] = r
            return [by[k] for k in sorted(by)]

        out_groups = []
        for mpan, blist in sorted(groups.items()):
            imp_rows, exp_rows, days, periods, gwarn = [], [], [], [], []
            mpan_export = None
            for b in blist:
                mpan_export = mpan_export or b.mpan_export
                try:
                    rows = _bp.build_csv_rows(b)
                except Exception as _be:      # one odd bill mustn't 500 the batch
                    gwarn.append(f"{b.source}: could not build CSV rows ({_be}).")
                    logger.warning("bills_build: %s CSV build failed: %s", b.source, _be)
                    rows = {"import": [], "export": []}
                imp_rows += rows["import"]
                exp_rows += rows["export"]
                days += b.reconciliation.get("days", [])
                periods += b.reconciliation.get("import_periods", [])
                gwarn += b.warnings
            imp_rows, exp_rows = _dedupe(imp_rows), _dedupe(exp_rows)
            recon_ok = (all(d.get("ok") for d in days) and all(p.get("ok") for p in periods))
            out_groups.append({
                "mpan_import": mpan, "mpan_export": mpan_export,
                "sources": [b.source for b in blist],
                "import_csv": _bp.rows_to_csv(imp_rows) if imp_rows else "",
                "export_csv": _bp.rows_to_csv(exp_rows) if exp_rows else "",
                "import_rows": len(imp_rows), "export_rows": len(exp_rows),
                "reconciliation": {"ok": recon_ok, "days": days, "import_periods": periods},
                "warnings": gwarn,
            })

        warnings = []
        if len([g for g in groups if g != "unknown"]) > 1:
            warnings.append(
                "More than one electricity import MPAN was found across these bills "
                "(a house move). Each is a separate site — import them one MPAN at a time.")
        if failed:
            warnings.append(f"{len(failed)} file(s) could not be parsed: "
                            + ", ".join(x['file'] or '?' for x in failed))
        return jsonify({"ok": True, "files": len(pdfs), "groups": out_groups,
                        "failed": failed, "warnings": warnings})
    except Exception as e:
        logger.error("api_historical_bills_build: %s", e)
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/api/historical/csv/preview", methods=["POST"])
def api_historical_csv_preview():
    """Parse + derive rates from the Octopus CSV(s) WITHOUT writing — the wizard's
    review/confirm step. Pure (no backup, no blocks): shows per-period tiers, the
    cost-derived rates + confidence, off/peak split, and the reconciliation."""
    try:
        import csv_import as _ci
        body = request.get_json(force=True, silent=True) or {}
        texts = _csv_channel_texts(body)
        if not texts:
            return jsonify({"ok": False, "error": "no CSV provided"}), 400
        out = {"ok": True, "channels": {}}
        for channel, text in texts.items():
            parsed = _ci.parse_octopus_csv(text, channel)
            if not parsed["ok"]:
                out["channels"][channel] = {"ok": False, "errors": parsed["errors"]}
                continue
            deriv = _ci.derive_rates(parsed["blocks"])
            out["channels"][channel] = {
                "ok": True, "block_count": len(parsed["blocks"]),
                "parse_errors": parsed["errors"], "periods": deriv["periods"],
                "off_peak_kwh": deriv["off_peak_kwh"], "peak_kwh": deriv["peak_kwh"],
                "reconcile": _ci.reconcile(parsed["blocks"], deriv["flags"]),
            }
        return jsonify(out)
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/api/historical/csv/apply", methods=["POST"])
def api_historical_csv_apply():
    """Back up, then write reconstructed imported_csv blocks from the CSV(s) with
    the user's confirmed rate overrides. Requires confirmed=true. Reversible via
    the reconstructed-history delete filter."""
    try:
        body = request.get_json(force=True, silent=True) or {}
        if not body.get("confirmed"):
            return jsonify({"ok": False, "error": "confirmation required"}), 400
        texts = _csv_channel_texts(body)
        if not texts:
            return jsonify({"ok": False, "error": "no CSV provided"}), 400
        meter_id = body.get("meter_id") or "electricity_main"
        overrides = body.get("overrides") or {}
        backup = None
        try:
            backup = os.path.basename(_create_backup_zip(label="pre-csv-import"))
        except Exception as be:
            logger.warning("csv apply: backup failed (continuing): %s", be)
        store = _get_store()
        result = store.apply_csv_import(
            texts, meter_id=meter_id, overrides=overrides)
        result["backup"] = backup
        # Offer a region/site confirmation for the imported span. CSV carries no
        # provenance, so the user names the site AND sets the region; skipping
        # leaves the data blended into whatever period already covers it (carbon
        # excluded). The panel lives on the Billing history page.
        try:
            span = result.get("span") or {}
            if span.get("from"):
                plan = store.plan_csv_reconciliation(span["from"], span.get("to"))
                if plan.get("needs_confirmation"):
                    store.set_meta("region_reconcile_pending", plan)
                    result["region_reconcile"] = True
        except Exception as _re:
            logger.warning("csv apply: region plan failed: %s", _re)
        # Rebuild the pre-rendered billing/usage charts so the imported span shows
        # up immediately — the API import path already does this; the CSV/bill path
        # previously left the charts stale until the next startup/live-block regen.
        if result.get("blocks_written"):
            try:
                _regen_charts_safely()
                result["charts_regenerated"] = True
            except Exception as _ce:
                logger.warning("csv apply: chart regen failed: %s", _ce)
        return jsonify(result)
    except Exception as e:
        logger.error("api_historical_csv_apply: %s", e)
        return jsonify({"ok": False, "error": str(e)}), 500


def _period_block_minutes_tz(store, from_iso):
    """(block_minutes, timezone) for the config period covering from_iso, with
    sane fallbacks — used to render template timestamps on the right grid/tz."""
    bm, tz = 30, "Europe/London"
    try:
        cp = store.get_config_period_for_date(from_iso) if from_iso else None
        if not cp:
            cp = store._conn.execute(
                "SELECT block_minutes, timezone FROM config_periods "
                "ORDER BY effective_from DESC LIMIT 1").fetchone()
        if cp:
            bm = int(cp["block_minutes"] or 30)
            tz = cp["timezone"] or "Europe/London"
    except Exception:
        pass
    return bm, tz


@app.route("/api/historical/gap-template")
def api_historical_gap_template():
    """Download a per-half-hour CSV pre-filled with Start/End for a gap, ready for
    the user to fill Consumption + Cost from their bill. Query: from, to (UTC ISO),
    channel (import|export, for the filename)."""
    try:
        from flask import Response
        import csv_import as _ci
        frm = request.args.get("from")
        to  = request.args.get("to")
        channel = (request.args.get("channel") or "import").strip().lower()
        inclusive = (request.args.get("inclusive") or "").lower() in ("1", "true", "yes")
        from_local = (request.args.get("from_local") or "").lower() in ("1", "true", "yes")
        to_local = (request.args.get("to_local") or "").lower() in ("1", "true", "yes")
        if not frm or not to:
            return jsonify({"error": "from and to are required"}), 400
        bm, tz = _period_block_minutes_tz(_get_store(), frm)
        # A date-range picker sends LOCAL wall-clock (e.g. the picked day
        # 2026-08-01T00:00:00 meaning local midnight); the generator treats its
        # input as UTC, so in BST that renders the first row as 01:00+01:00 not
        # 00:00+01:00. Localise those boundaries to naive-UTC first so the grid
        # lands on local midnight. (Gap paths pass real UTC block_starts and set
        # neither flag, so they are unaffected.)
        if from_local or to_local:
            import block_store as _bs
            def _loc(s):
                s = str(s)
                return _bs.local_datetime_to_utc(s[:10], (s[11:16] or "00:00"), tz)
            try:
                if from_local:
                    frm = _loc(frm)
                if to_local:
                    to = _loc(to)
            except Exception:
                pass
        # A persisted gap's `to` is the LAST missing slot's start (inclusive); the
        # generator is half-open, so extend the end by one block to include it. A
        # localised end is already an exclusive next-midnight/end-of-day boundary,
        # so it needs no bump (bumping would spill one slot into the next day).
        if inclusive and not to_local:
            from datetime import datetime as _dt, timedelta as _td
            try:
                _t = _dt.fromisoformat(str(to).replace("Z", "").split("+")[0])
                to = (_t + _td(minutes=bm)).isoformat()
            except Exception:
                pass
        csv_text = _ci.gap_template_csv(frm, to, block_minutes=bm, tz_name=tz)
        fname = f"emt-gap-{channel}-{frm[:10]}_to_{to[:10]}.csv"
        return Response(csv_text, mimetype="text/csv",
                        headers={"Content-Disposition": f'attachment; filename="{fname}"'})
    except Exception as e:
        logger.error("api_historical_gap_template: %s", e)
        return jsonify({"error": str(e)}), 500


@app.route("/api/historical/slots-template", methods=["POST"])
def api_historical_slots_template():
    """Pre-filled CSV for an ARBITRARY set of slots — the pricing-health escape
    hatch: hand the user exactly the half-hours the API couldn't price so they can
    fill them from their bill and run them through the CSV reprice path.
    Body: {starts: [naive-UTC iso...], channel}."""
    try:
        from flask import Response
        import csv_import as _ci
        body = request.get_json(silent=True) or {}
        starts = body.get("starts") or []
        channel = (body.get("channel") or "import").strip().lower()
        if not starts:
            return jsonify({"error": "no slots"}), 400
        bm, tz = _period_block_minutes_tz(_get_store(), starts[0])
        csv_text = _ci.slots_template_csv(starts, block_minutes=bm, tz_name=tz)
        fname = f"emt-slots-{channel}-{len(starts)}.csv"
        return Response(csv_text, mimetype="text/csv",
                        headers={"Content-Disposition": f'attachment; filename="{fname}"'})
    except Exception as e:
        logger.error("api_historical_slots_template: %s", e)
        return jsonify({"error": str(e)}), 500


@app.route("/api/historical/csv-template")
def api_historical_csv_template():
    """Download a small illustrative CSV template (headers + example rows in the
    exact time format) for users supplying their own consumption CSV."""
    try:
        from flask import Response
        import csv_import as _ci
        bm, _tz = _period_block_minutes_tz(_get_store(), None)
        csv_text = _ci.blank_template_csv(block_minutes=bm)
        return Response(csv_text, mimetype="text/csv",
                        headers={"Content-Disposition": 'attachment; filename="emt-consumption-template.csv"'})
    except Exception as e:
        logger.error("api_historical_csv_template: %s", e)
        return jsonify({"error": str(e)}), 500


@app.route("/api/insights/periods")
def api_insights_periods():
    """Return list of all billing periods with basic carbon summary for each."""
    try:
        import energy_charts as _ec
        from zoneinfo import ZoneInfo as _ZI
        from datetime import datetime, timezone
        store   = _get_store()
        cfg     = load_config()
        tz_name = "Europe/London"
        for mid, md in (cfg.get("meters") or {}).items():
            tz_name = (md.get("meta") or {}).get("timezone", "Europe/London")
            break
        periods = _ec.get_billing_periods_from_config_periods(
            store.get_config_periods(), tz=_ZI(tz_name)
        )
        now_local = datetime.now(_ZI(tz_name)).replace(tzinfo=None)  # periods are naive local datetimes
        result = []
        for (ps, pe) in periods:
            is_current = ps <= now_local < pe
            ps_iso = ps.astimezone(timezone.utc).replace(tzinfo=None).isoformat()
            pe_iso = pe.astimezone(timezone.utc).replace(tzinfo=None).isoformat()
            rows = store._conn.execute("""
                SELECT
                    SUM(CASE WHEN carbon_g IS NOT NULL THEN imp_kwh ELSE 0 END) AS imp_kwh_ci,
                    SUM(CASE WHEN carbon_g IS NOT NULL THEN exp_kwh ELSE 0 END) AS exp_kwh_ci,
                    SUM(CASE WHEN carbon_g IS NOT NULL THEN carbon_g ELSE 0 END) AS carbon_g_net,
                    COUNT(CASE WHEN carbon_g IS NOT NULL THEN 1 END) AS ci_blocks,
                    COUNT(*) AS total_blocks
                FROM blocks
                WHERE meter_id NOT IN (
                    SELECT meter_id FROM meters WHERE is_sub_meter = 1
                )
                AND block_start >= ? AND block_start < ?
            """, (ps_iso, pe_iso)).fetchone()
            r = dict(rows) if rows else {}
            result.append({
                "period_start":  ps.date().isoformat(),
                "period_end":    pe.date().isoformat(),
                "is_current":    is_current,
                "has_carbon":    (r.get("ci_blocks") or 0) > 0,
                "carbon_g_net":  r.get("carbon_g_net"),
            })
        return jsonify({"periods": result})
    except Exception as e:
        logger.exception("api_insights_periods failed")
        return jsonify({"error": str(e)}), 500


def _collapse_rate_tiers(tiers_list):
    """Fold a serialised rate_tiers list to ONE cost-weighted-average entry when it
    exceeds the Billing view's _MAX_RATE_ROWS threshold — the same collapse
    energy_charts._bill_rate_rows / _collapse_rate_kwh apply to Agile's ~48 rates a
    day, so Usage Insights and Billing fold identically (#371). The average rate is
    Σcost / Σkwh, so a net plunge-credit period reads NEGATIVE (sign preserved).
    `collapsed` + `n_rates` let the renderer label it 'avg of N rates'."""
    import energy_charts as _ec
    if len(tiers_list) <= _ec._MAX_RATE_ROWS:
        return tiers_list
    tot_kwh  = round(sum(t["kwh"]  for t in tiers_list), 3)
    tot_cost = round(sum(t["cost"] for t in tiers_list), 4)
    tot_blk  = sum(t.get("blocks", 0) for t in tiers_list)
    avg = round(tot_cost / tot_kwh, 6) if tot_kwh else 0.0
    return [{"rate": avg, "kwh": tot_kwh, "cost": tot_cost, "blocks": tot_blk,
             "collapsed": True, "n_rates": len(tiers_list)}]


def _aggregate_insights(store, cfg, utc_start: str, utc_end: str) -> dict:
    """
    Core aggregation for Insights — works for any UTC time range.
    Uses a direct lightweight SQL query rather than get_blocks_for_range
    to avoid fetching unnecessary columns and joining reads.
    """
    from datetime import datetime as _dt

    meters_cfg = cfg.get("meters") or {}
    sub_meter_ids = {
        mid for mid, md in meters_cfg.items()
        if (md.get("meta") or {}).get("sub_meter")
    }

    def _meter_type(mid, md):
        explicit = (md.get("meta") or {}).get("meter_type", "")
        if explicit:
            return explicit
        mid_l = mid.lower()
        if any(k in mid_l for k in ("ev", "charger")):      return "ev_charger"
        if any(k in mid_l for k in ("battery", "batt")):    return "battery"
        if any(k in mid_l for k in ("heat", "pump")):       return "heat_pump"
        if any(k in mid_l for k in ("solar", "pv", "inv")): return "inverter"
        return "unknown"

    # Lightweight direct query — only columns needed for carbon insights
    rows = store._conn.execute(
        """SELECT b.meter_id, b.imp_kwh, b.exp_kwh, b.carbon_g,
                  COALESCE(m.is_sub_meter, 0) as is_sub_meter
           FROM blocks b
           LEFT JOIN meters m ON m.meter_id = b.meter_id
                              AND m.config_period_id = b.config_period_id
           WHERE b.block_start >= ? AND b.block_start < ?
           ORDER BY b.block_start""",
        (utc_start, utc_end)
    ).fetchall()

    block_minutes = 30
    if meters_cfg:
        first_mid = next(iter(meters_cfg))
        block_minutes = int((meters_cfg[first_mid].get("meta") or {}).get("block_minutes", 30) or 30)

    main_imp_kwh = main_exp_kwh = 0.0
    main_ci_imp_kwh = main_ci_exp_kwh = 0.0
    cg_imp = cg_exp = cg_net = 0.0
    ci_kwh_weighted = ci_duration = 0.0
    eff_carbon = eff_kwh = 0.0
    has_carbon = False
    total_blocks = 0
    ci_blocks = 0

    sub_totals = {}

    for row in rows:
        mid        = row["meter_id"]
        b_imp_raw  = float(row["imp_kwh"] or 0)
        is_sub     = bool(row["is_sub_meter"]) or mid in sub_meter_ids
        b_imp      = b_imp_raw
        b_exp      = float(row["exp_kwh"] or 0)
        b_net      = b_imp - b_exp
        cg         = row["carbon_g"]

        if is_sub:
            if mid not in sub_totals:
                _m_meta = (meters_cfg.get(mid, {}).get("meta") or {})
                sub_totals[mid] = {
                    "imp_kwh": 0.0, "carbon_g": 0.0, "ci_blocks": 0,
                    "ci_imp_kwh": 0.0, "total_blocks": 0,
                    "meter_type": _meter_type(mid, meters_cfg.get(mid, {})),
                    # inverter_possible removed — deprecated in 2.9.0
                    "ci_kwh_weighted": 0.0, "ci_kwh_duration": 0.0,
                }
            sub_totals[mid]["imp_kwh"]      += b_imp
            sub_totals[mid]["total_blocks"] += 1
            if cg is not None:
                cg_f = float(cg)
                sub_totals[mid]["carbon_g"]   += cg_f
                sub_totals[mid]["ci_blocks"]  += 1
                sub_totals[mid]["ci_imp_kwh"] += b_imp
                if b_imp > 0.001:
                    b_intensity = abs(cg_f / b_imp)
                    sub_totals[mid]["ci_kwh_weighted"] += b_intensity * b_imp
                    sub_totals[mid]["ci_kwh_duration"] += b_imp
            continue

        # Main meter
        total_blocks += 1
        main_imp_kwh += b_imp
        main_exp_kwh += b_exp

        if cg is None:
            continue

        cg_f = float(cg)
        has_carbon = True
        ci_blocks += 1
        cg_net += cg_f
        main_ci_imp_kwh += b_imp
        main_ci_exp_kwh += b_exp

        if b_net != 0:
            b_intensity = abs(cg_f / b_net)
            cg_imp += b_imp * b_intensity
            cg_exp += b_exp * b_intensity
            ci_kwh_weighted += b_intensity * block_minutes
            ci_duration     += block_minutes
            eff_carbon += abs(cg_f)
            eff_kwh    += abs(b_net)
        elif b_imp > 0:
            cg_imp += abs(cg_f)
        else:
            cg_exp += abs(cg_f)

    effective_intensity  = round(eff_carbon / eff_kwh, 1) if eff_kwh > 0 else None
    grid_avg_intensity   = round(ci_kwh_weighted / ci_duration, 1) if ci_duration > 0 else None
    avg_export_intensity = round(cg_exp / main_ci_exp_kwh, 1) if main_ci_exp_kwh > 0 else None

    for mid, st in sub_totals.items():
        if st["ci_kwh_duration"] > 0:
            st["avg_charge_intensity"] = round(st["ci_kwh_weighted"] / st["ci_kwh_duration"], 1)
        else:
            st["avg_charge_intensity"] = None
        del st["ci_kwh_weighted"]
        del st["ci_kwh_duration"]

    total_sub_imp_kwh  = sum(st["imp_kwh"]   for st in sub_totals.values())
    total_sub_carbon_g = sum(st["carbon_g"]  for st in sub_totals.values())
    house_imp_kwh    = max(0.0, main_imp_kwh - total_sub_imp_kwh)
    house_ci_imp_kwh = max(0.0, main_ci_imp_kwh - sum(
        st.get("ci_imp_kwh", 0.0) for st in sub_totals.values()
    ))
    house_carbon_g   = max(0.0, cg_imp - total_sub_carbon_g) if has_carbon else None
    house_avg_intensity = round(house_carbon_g / house_ci_imp_kwh, 1) \
        if house_carbon_g and house_ci_imp_kwh > 0 else None

    settings = store.get_settings()
    merged   = dict(SETTINGS_DEFAULTS)
    merged.update(settings)

    coverage_pct = round(ci_blocks / total_blocks * 100, 1) if total_blocks else 0

    # Actual first block date within range — main meter and per sub-meter
    first_block_row = store._conn.execute(
        "SELECT MIN(block_start) as first_block FROM blocks "
        "WHERE meter_id = 'electricity_main' AND block_start >= ? AND block_start < ?",
        (utc_start, utc_end)
    ).fetchone()
    data_start = first_block_row["first_block"][:10] if first_block_row and first_block_row["first_block"] else None

    for mid in sub_totals:
        row = store._conn.execute(
            "SELECT MIN(block_start) as first_block FROM blocks "
            "WHERE meter_id = ? AND block_start >= ? AND block_start < ?",
            (mid, utc_start, utc_end)
        ).fetchone()
        sub_totals[mid]["data_start"] = row["first_block"][:10] if row and row["first_block"] else data_start

    # Generation mix — imp_kwh-weighted average for this period
    generation_mix = []
    try:
        generation_mix = store.get_generation_mix_for_range(utc_start, utc_end)
    except Exception:
        pass

    return {
        "has_carbon":           has_carbon,
        "carbon_coverage_pct":  coverage_pct,
        "data_start":           data_start,
        "generation_mix":       generation_mix,
        "imp_kwh":              round(main_imp_kwh, 3),
        "exp_kwh":              round(main_exp_kwh, 3),
        "ci_imp_kwh":           round(main_ci_imp_kwh, 3),
        "ci_exp_kwh":           round(main_ci_exp_kwh, 3),
        "carbon_g_imp":         round(cg_imp, 1) if has_carbon else None,
        "carbon_g_exp":         round(cg_exp, 1) if has_carbon else None,
        "carbon_g_net":         round(cg_net, 1) if has_carbon else None,
        "effective_intensity":  effective_intensity,
        "avg_export_intensity": avg_export_intensity,
        "grid_avg_intensity":   grid_avg_intensity,
        "sub_meters":           sub_totals,
        "house_imp_kwh":        round(house_imp_kwh, 3),
        "house_ci_imp_kwh":     round(house_ci_imp_kwh, 3),
        "house_carbon_g":       round(house_carbon_g, 1) if house_carbon_g is not None else None,
        "house_avg_intensity":  house_avg_intensity,
        "assumptions":          merged,
    }


@app.route("/api/insights/billing-period")
def api_insights_billing_period():
    """
    Full carbon insights for a billing period.
    Query params: period_start=YYYY-MM-DD (local date of period start).
    If omitted, returns the most recent period.
    """
    try:
        import energy_charts as _ec
        from zoneinfo import ZoneInfo as _ZI
        from datetime import datetime, timezone
        store   = _get_store()
        cfg     = load_config()
        tz_name = "Europe/London"
        for mid, md in (cfg.get("meters") or {}).items():
            tz_name = (md.get("meta") or {}).get("timezone", "Europe/London")
            break
        tz = _ZI(tz_name)

        periods = _ec.get_billing_periods_from_config_periods(
            store.get_config_periods(), tz=tz
        )
        if not periods:
            return jsonify({"error": "No billing periods found"}), 404

        requested = request.args.get("period_start")
        now_local = datetime.now(tz).replace(tzinfo=None)
        target_ps = target_pe = None
        for (ps, pe) in periods:
            if requested:
                if ps.date().isoformat() == requested:
                    target_ps, target_pe = ps, pe
                    break
            else:
                target_ps, target_pe = ps, pe

        if not target_ps:
            return jsonify({"error": "Billing period not found"}), 404

        is_current = target_ps <= now_local < target_pe
        ps_iso = target_ps.astimezone(timezone.utc).replace(tzinfo=None).isoformat()
        pe_iso = target_pe.astimezone(timezone.utc).replace(tzinfo=None).isoformat()

        d = _aggregate_insights(store, cfg, ps_iso, pe_iso)
        d["period_start"] = target_ps.date().isoformat()
        d["period_end"]   = target_pe.date().isoformat()
        d["is_current"]   = is_current
        return jsonify(d)
    except Exception as e:
        logger.exception("api_insights_billing_period failed")
        return jsonify({"error": str(e)}), 500

@app.route("/api/insights/data-bounds")
def api_insights_data_bounds():
    """Date bounds for the Insights page nav/compare gating.

    Returns TWO ranges, because the Carbon and Usage tabs have different data:
      - earliest/latest             → blocks that carry CARBON data (carbon_g),
                                       which only exists from v2.3.0 onward. The
                                       Carbon tab gates on this.
      - usage_earliest/usage_latest → ALL blocks for the meter, including
                                       historical imports that have kWh/cost but
                                       no carbon. The Usage tab gates on this, so
                                       "vs last year"/"vs last month" comparisons
                                       into backfilled months aren't wrongly
                                       disabled.
    """
    try:
        store = _get_store()
        row = store._conn.execute(
            """SELECT
                 MIN(block_start) AS usage_earliest,
                 MAX(block_start) AS usage_latest,
                 MIN(CASE WHEN carbon_g IS NOT NULL THEN block_start END) AS c_earliest,
                 MAX(CASE WHEN carbon_g IS NOT NULL THEN block_start END) AS c_latest
               FROM blocks
               WHERE meter_id = 'electricity_main'"""
        ).fetchone()
        return jsonify({
            # Carbon range (unchanged meaning — Carbon tab uses these)
            "earliest": row["c_earliest"][:10] if row and row["c_earliest"] else None,
            "latest":   row["c_latest"][:10]   if row and row["c_latest"]   else None,
            # Full energy range (Usage tab uses these)
            "usage_earliest": row["usage_earliest"][:10] if row and row["usage_earliest"] else None,
            "usage_latest":   row["usage_latest"][:10]   if row and row["usage_latest"]   else None,
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/insights/calendar-month")
def api_insights_calendar_month():
    """
    Carbon insights for a calendar month.
    Query params: year=YYYY, month=MM (1-12)
    Returns same shape as /api/insights/billing-period.
    """
    try:
        from zoneinfo import ZoneInfo as _ZI
        from datetime import datetime, timezone, timedelta
        store   = _get_store()
        cfg     = load_config()
        tz_name = "Europe/London"
        for mid, md in (cfg.get("meters") or {}).items():
            tz_name = (md.get("meta") or {}).get("timezone", "Europe/London")
            break
        tz = _ZI(tz_name)

        year  = int(request.args.get("year",  datetime.now(tz).year))
        month = int(request.args.get("month", datetime.now(tz).month))

        # Calendar month boundaries in local time → UTC
        local_start = datetime(year, month, 1, tzinfo=tz)
        if month == 12:
            local_end = datetime(year + 1, 1, 1, tzinfo=tz)
        else:
            local_end = datetime(year, month + 1, 1, tzinfo=tz)

        utc_start = local_start.astimezone(timezone.utc).replace(tzinfo=None).isoformat()
        utc_end   = local_end.astimezone(timezone.utc).replace(tzinfo=None).isoformat()

        # Reuse same data aggregation as billing-period endpoint
        d = _aggregate_insights(store, cfg, utc_start, utc_end)
        d["period_start"] = local_start.date().isoformat()
        d["period_end"]   = (local_end - timedelta(days=1)).date().isoformat()
        d["period_label"] = local_start.strftime("%B %Y")
        return jsonify(d)
    except Exception as e:
        logger.exception("api_insights_calendar_month failed")
        return jsonify({"error": str(e)}), 500


@app.route("/api/insights/calendar-year")
def api_insights_calendar_year():
    """
    Carbon insights for a calendar year.
    Query params: year=YYYY
    Returns same shape as /api/insights/billing-period.
    """
    try:
        from zoneinfo import ZoneInfo as _ZI
        from datetime import datetime, timezone, timedelta
        store   = _get_store()
        cfg     = load_config()
        tz_name = "Europe/London"
        for mid, md in (cfg.get("meters") or {}).items():
            tz_name = (md.get("meta") or {}).get("timezone", "Europe/London")
            break
        tz = _ZI(tz_name)

        year = int(request.args.get("year", datetime.now(tz).year))

        local_start = datetime(year, 1, 1, tzinfo=tz)
        local_end   = datetime(year + 1, 1, 1, tzinfo=tz)

        utc_start = local_start.astimezone(timezone.utc).replace(tzinfo=None).isoformat()
        utc_end   = local_end.astimezone(timezone.utc).replace(tzinfo=None).isoformat()

        d = _aggregate_insights(store, cfg, utc_start, utc_end)
        d["period_start"] = local_start.date().isoformat()
        d["period_end"]   = (local_end - timedelta(days=1)).date().isoformat()
        d["period_label"] = str(year)
        return jsonify(d)
    except Exception as e:
        logger.exception("api_insights_calendar_year failed")
        return jsonify({"error": str(e)}), 500


# ─────────────────────────────────────────────────────────────────────────────
# Usage Insights — aggregation + endpoints
# ─────────────────────────────────────────────────────────────────────────────

def _configured_ev_meter_id(cfg):
    """The configured EV sub-meter id, or None. The dispatch-derived EV device only
    applies when there's no real EV meter measuring it — so this gates the whole
    feature: any account WITH an EV sub-meter is unaffected (byte-identical)."""
    for mid, md in (cfg.get("meters") or {}).items():
        meta = md.get("meta") or {}
        if not meta.get("sub_meter"):
            continue
        if (meta.get("meter_type") == "ev_charger"
                or any(kw in mid.lower() for kw in ("ev", "charger"))):
            return mid
    return None


def _dispatch_derived_ev_kwh(dispatch_rows) -> dict:
    """EV kWh per half-hour slot reconstructed from COMPLETED dispatch deltas —
    Octopus's own per-slot EV energy. Excludes planned/started phantoms (a scheduled
    charge the car never took) and slots with no delivery. Validated ~99% against a
    real CT-clamp EV meter over 19 days (residual is clamp scatter, not missed
    energy). Returns {slot_start: kwh}. Pure — takes dispatch_history-shaped rows."""
    out: dict = {}
    for r in dispatch_rows:
        if r.get("kind") != "completed":
            continue
        e = r.get("energy_kwh")
        if e is None:
            continue
        v = abs(float(e))
        if v > 1e-9:
            out[r["slot_start"]] = out.get(r["slot_start"], 0.0) + v
    return out


def _dispatch_ev_split_by_bucket(main_by_slot, ev_by_slot, bucket_fn) -> dict:
    """Grid-clip the per-slot dispatch EV to that slot's main import and apportion its
    cost from the slot's import cost, then bucket to {bucket: {"kwh","cost"}}. Pure —
    `main_by_slot` is {slot: (imp_kwh, imp_cost)}, `ev_by_slot` is {slot: kwh}. The
    clip + apportionment guarantee house + EV == the slot's grid import exactly, so a
    caller can split a chart segment without moving any total."""
    out: dict = {}
    for _slot, _e in ev_by_slot.items():
        _mk, _mc = main_by_slot.get(_slot, (0.0, 0.0))
        _ek = min(_e, _mk) if _mk > 0 else 0.0        # can't exceed the grid that slot
        if _ek <= 1e-9:
            continue
        _ec = _mc * (_ek / _mk) if _mk > 0 else 0.0   # cost follows the same fraction
        _b = bucket_fn(_slot)
        _acc = out.setdefault(_b, {"kwh": 0.0, "cost": 0.0})
        _acc["kwh"]  += _ek
        _acc["cost"] += _ec
    return out


def _apply_ev_split_to_summary_rows(rows, meters_list, ev_by_day,
                                    color="#8b5cf6", label="EV (from dispatch)") -> bool:
    """Split each row's electricity_main import segment into a derived EV slice + a
    reduced house slice, per {local_date: {"kwh","cost"}} from _dispatch_ev_split_by_bucket.
    DISPLAY-ONLY: the row's top-level imp_kwh/imp_cost/net_cost are never touched, so the
    chart totals, the data-table Totals row, and the bill stay byte-identical — only the
    'Direct' stack segment is subdivided. Appends an 'ev_dispatch' meter to meters_list
    when any row splits. Mutates in place; returns True if applied. Pure/testable."""
    applied = False
    for _row in rows:
        _ds = "%04d-%02d-%02d" % (_row["year"], _row["month"], _row["day"])
        _ev = ev_by_day.get(_ds)
        _mm = (_row.get("meters") or {}).get("electricity_main")
        if not _ev or not _mm:
            continue
        _evk, _evc = _ev["kwh"], _ev["cost"]
        if _mm["imp_kwh"] >= 0:            # don't over-split a normal day
            _evk = min(_evk, _mm["imp_kwh"])
        if _mm["imp_cost"] >= 0:           # a plunge-price CREDIT keeps its sign
            _evc = min(_evc, _mm["imp_cost"])
        if _evk <= 1e-9:
            continue
        _mm["imp_kwh"]  = round(_mm["imp_kwh"]  - _evk, 4)
        _mm["imp_cost"] = round(_mm["imp_cost"] - _evc, 4)
        _row["meters"]["ev_dispatch"] = {
            "imp_kwh": round(_evk, 4), "imp_cost": round(_evc, 4),
            "exp_kwh": 0.0, "exp_cost": 0.0, "carbon_g": None,
        }
        applied = True
    if applied:
        meters_list.append({"id": "ev_dispatch", "label": label,
                            "color": color, "is_sub": True})
    return applied


def _aggregate_usage(store, cfg, utc_start: str, utc_end: str,
                     tz_name: str = "UTC") -> dict:
    """
    Core aggregation for Usage Insights — works for any UTC time range.
    Returns cost, earnings, rate-tier distribution, self-sufficiency,
    peak demand window, and per-device breakdowns.
    All monetary values in the configured currency.
    """
    from datetime import datetime as _dt
    from zoneinfo import ZoneInfo as _ZI
    from collections import defaultdict

    try:
        tz = _ZI(tz_name)
    except Exception:
        tz = _ZI("UTC")

    meters_cfg  = cfg.get("meters") or {}
    sub_meter_ids = {
        mid for mid, md in meters_cfg.items()
        if (md.get("meta") or {}).get("sub_meter")
    }

    # Fetch all columns needed for usage insights in one query
    rows = store._conn.execute(
        """SELECT b.meter_id, b.block_start,
                  b.imp_kwh, b.imp_kwh_grid, b.imp_kwh_remainder,
                  b.imp_rate, b.imp_cost,
                  b.exp_kwh, b.exp_rate, b.exp_cost,
                  b.standing_charge,
                  COALESCE(m.is_sub_meter, 0) as is_sub_meter,
                  m.meter_type
           FROM blocks b
           LEFT JOIN meters m ON m.meter_id = b.meter_id
                              AND m.config_period_id = b.config_period_id
           WHERE b.block_start >= ? AND b.block_start < ?
           ORDER BY b.block_start""",
        (utc_start, utc_end)
    ).fetchall()

    # ── Accumulators ──────────────────────────────────────────────────────────
    main_imp_kwh   = 0.0   # total grid import
    main_exp_kwh   = 0.0   # total export
    main_imp_cost  = 0.0   # total import cost
    main_exp_cost  = 0.0   # total export earnings
    house_imp_kwh  = 0.0   # import remainder (house only, excl sub-meters)
    house_imp_cost = 0.0   # cost of house-only import

    # Standing charge — sum once per local day
    daily_sc: dict = {}    # date_str → max sc value

    # Rate tier distribution {rate → {kwh, cost, blocks}}
    rate_tiers: dict = defaultdict(lambda: {"kwh": 0.0, "cost": 0.0, "blocks": 0})

    # Peak demand window — 2hr buckets (0–23) by hour, accumulate house imp_kwh
    hour_kwh: dict = defaultdict(float)   # hour (0-23) → house import kWh

    total_blocks = 0

    # Sub-meter accumulators
    sub_totals: dict = {}

    for row in rows:
        mid     = row["meter_id"]
        bs      = row["block_start"]
        is_sub  = bool(row["is_sub_meter"]) or mid in sub_meter_ids
        imp     = float(row["imp_kwh"] or 0)
        exp     = float(row["exp_kwh"] or 0)
        imp_cost = float(row["imp_cost"] or 0)
        exp_cost = float(row["exp_cost"] or 0)
        rate     = float(row["imp_rate"] or 0)
        sc       = float(row["standing_charge"] or 0)

        if is_sub:
            if mid not in sub_totals:
                _meta = (meters_cfg.get(mid, {}).get("meta") or {})
                sub_totals[mid] = {
                    "imp_kwh":   0.0,
                    "imp_cost":  0.0,
                    "exp_kwh":   0.0,
                    "label":     _meta.get("device") or mid,
                    "meter_type": (row["meter_type"] or "").lower(),
                    # rate tier breakdown per sub-meter
                    "rate_tiers": defaultdict(lambda: {"kwh": 0.0, "cost": 0.0, "blocks": 0}),
                }
            # Use imp_kwh_grid for sub-meters (grid portion only). Distinguish a
            # real 0 (device charged entirely from battery/solar this block — no
            # grid) from NULL (not computed): `or imp` would treat 0 as missing and
            # fall back to TOTAL consumption, over-stating the device (bites the
            # battery hardest). Mirrors the remainder handling below (`is not None`).
            _grid = row["imp_kwh_grid"]
            sub_grid_kwh = float(_grid) if _grid is not None else imp
            sub_totals[mid]["imp_kwh"]  += sub_grid_kwh
            sub_totals[mid]["imp_cost"] += imp_cost
            sub_totals[mid]["exp_kwh"]  += exp
            if rate != 0:      # keep plunge-price (negative-rate) slots too (#371)
                sub_totals[mid]["rate_tiers"][round(rate, 6)]["kwh"]    += sub_grid_kwh
                sub_totals[mid]["rate_tiers"][round(rate, 6)]["cost"]   += imp_cost
                sub_totals[mid]["rate_tiers"][round(rate, 6)]["blocks"] += 1
            continue

        # ── Main meter ────────────────────────────────────────────────────────
        total_blocks += 1
        main_imp_kwh  += imp
        main_exp_kwh  += exp
        main_imp_cost += imp_cost
        main_exp_cost += exp_cost

        # House-only import (main minus sub-meters, already computed by engine)
        rem = row["imp_kwh_remainder"]
        h_imp = float(rem) if rem is not None else imp
        h_cost = imp_cost * (h_imp / imp) if imp > 0 else 0.0
        house_imp_kwh  += h_imp
        house_imp_cost += h_cost

        # Standing charge — start-of-day value. Take the earliest block's
        # charge, but don't let a leading zero (e.g. an early gap-filled block)
        # shadow the real value: only set once with the first NON-zero seen,
        # falling back to 0 only if the whole day is zero. Not MAX.
        try:
            local_d = (_dt.fromisoformat(bs)
                       .replace(tzinfo=_ZI("UTC"))
                       .astimezone(tz)
                       .strftime("%Y-%m-%d"))
        except Exception:
            local_d = bs[:10]
        if sc > 0:
            if not daily_sc.get(local_d):      # unset or still 0
                daily_sc[local_d] = sc
        elif local_d not in daily_sc:
            daily_sc[local_d] = 0.0

        # Rate tier accumulation (main meter total) — only when actually importing.
        # `rate != 0` (not `> 0`) so Agile plunge-price slots (negative rate — a
        # CREDIT) stay in the distribution instead of being silently dropped (#371);
        # a rate of exactly 0 still means unpriced/gap-filled and is excluded.
        if imp > 0 and rate != 0:
            rate_key = round(rate, 6)
            rate_tiers[rate_key]["kwh"]    += imp
            rate_tiers[rate_key]["cost"]   += imp_cost
            rate_tiers[rate_key]["blocks"] += 1

        # Peak demand window — local hour bucket
        try:
            local_hour = (_dt.fromisoformat(bs)
                          .replace(tzinfo=_ZI("UTC"))
                          .astimezone(tz)
                          .hour)
            hour_kwh[local_hour] += h_imp
        except Exception:
            pass

    # ── Synthetic dispatch-derived EV device (display-only; BL-22) ───────────────
    # For an account with NO EV sub-meter AND no other sub-meters (a pure-API /
    # no-HA-sensor setup), reconstruct the EV/house split from the completed-dispatch
    # delta — Octopus's own per-slot EV energy — so a sensor-less user still sees
    # car-vs-house. Guaranteed no-op when an EV meter (or any sub-meter) exists.
    # Grid-clipped per slot (EV can't exceed that slot's grid import) with its cost
    # apportioned from the slot's import cost, so house + EV == grid import exactly.
    if _configured_ev_meter_id(cfg) is None and not sub_totals:
        try:
            _drows = store._conn.execute(
                "SELECT slot_start, kind, energy_kwh FROM dispatch_history "
                "WHERE kind='completed' AND slot_start >= ? AND slot_start < ?",
                (utc_start, utc_end)).fetchall()
            _ev_by_slot = _dispatch_derived_ev_kwh(
                [{"slot_start": r["slot_start"], "kind": r["kind"],
                  "energy_kwh": r["energy_kwh"]} for r in _drows])
        except Exception:
            _ev_by_slot = {}
        if _ev_by_slot:
            _main_by_slot = {}
            for row in rows:                     # no sub-meters here, so all main
                _main_by_slot[row["block_start"]] = (
                    float(row["imp_kwh"] or 0), float(row["imp_cost"] or 0),
                    float(row["imp_rate"] or 0))
            _ev_kwh = _ev_cost = 0.0
            _ev_tiers = defaultdict(lambda: {"kwh": 0.0, "cost": 0.0, "blocks": 0})
            for _slot, _e in _ev_by_slot.items():
                _mk, _mc, _mr = _main_by_slot.get(_slot, (0.0, 0.0, 0.0))
                _ek = min(_e, _mk) if _mk > 0 else 0.0     # grid-clip to the slot
                if _ek <= 1e-9:
                    continue
                _ec = _mc * (_ek / _mk) if _mk > 0 else 0.0   # apportion cost
                _ev_kwh += _ek
                _ev_cost += _ec
                if _mr != 0:                     # keep plunge-price slots too (#371)
                    _t = _ev_tiers[round(_mr, 6)]
                    _t["kwh"] += _ek; _t["cost"] += _ec; _t["blocks"] += 1
            if _ev_kwh > 1e-9:
                house_imp_kwh  = max(0.0, house_imp_kwh  - _ev_kwh)
                house_imp_cost = max(0.0, house_imp_cost - _ev_cost)
                sub_totals["ev_dispatch"] = {
                    "imp_kwh": _ev_kwh, "imp_cost": _ev_cost, "exp_kwh": 0.0,
                    "label": "EV (from dispatch)", "meter_type": "ev_charger",
                    "derived": True, "rate_tiers": _ev_tiers,
                }

    # ── Derived totals ────────────────────────────────────────────────────────
    total_sc = round(sum(daily_sc.values()), 4)
    total_days = len(daily_sc)
    net_cost = round(main_imp_cost + total_sc - main_exp_cost, 4)

    # Weighted average import rate
    weighted_rate = round(main_imp_cost / main_imp_kwh, 6) if main_imp_kwh > 0 else None

    # Net grid position — import minus export (positive = net consumer, negative = net exporter)
    # Self-sufficiency cannot be computed without a generation meter (we don't know
    # how much solar was self-consumed vs exported). We report net position instead.
    net_grid_kwh = round(main_imp_kwh - main_exp_kwh, 3)

    # Net exporter days — days where exp_kwh > imp_kwh
    # Requires a per-day query (lightweight)
    net_exporter_days = 0
    try:
        day_rows = store._conn.execute(
            """SELECT SUM(imp_kwh) as di, SUM(exp_kwh) as de
               FROM blocks
               WHERE meter_id='electricity_main'
                 AND block_start >= ? AND block_start < ?
               GROUP BY date(block_start)""",
            (utc_start, utc_end)
        ).fetchall()
        net_exporter_days = sum(1 for r in day_rows
                                if (r["de"] or 0) > (r["di"] or 0))
    except Exception:
        pass

    # Peak demand window — find the 2-hour window with highest average house import
    peak_window_start = peak_window_kwh = None
    if hour_kwh:
        hours = sorted(hour_kwh.keys())
        best = -1.0
        best_h = hours[0]
        for h in hours:
            h2 = (h + 1) % 24
            combined = hour_kwh.get(h, 0) + hour_kwh.get(h2, 0)
            if combined > best:
                best = combined
                best_h = h
        peak_window_start = best_h
        peak_window_kwh   = round(best, 3)

    # Serialise rate_tiers (keys are floats, not JSON-safe as dict keys), then fold
    # to a single average row above _MAX_RATE_ROWS so Agile matches the Billing view.
    rate_tiers_list = _collapse_rate_tiers(sorted(
        [{"rate": k, "kwh": round(v["kwh"], 3),
          "cost": round(v["cost"], 4), "blocks": v["blocks"]}
         for k, v in rate_tiers.items()],
        key=lambda x: x["rate"]
    ))

    # Serialise sub_totals rate_tiers similarly (same per-device collapse)
    for mid, st in sub_totals.items():
        st["rate_tiers"] = _collapse_rate_tiers(sorted(
            [{"rate": k, "kwh": round(v["kwh"], 3),
              "cost": round(v["cost"], 4), "blocks": v["blocks"]}
             for k, v in st["rate_tiers"].items()],
            key=lambda x: x["rate"]
        ))

    return {
        # Cost & earnings
        "imp_kwh":          round(main_imp_kwh,  3),
        "exp_kwh":          round(main_exp_kwh,  3),
        "imp_cost":         round(main_imp_cost, 4),
        "exp_cost":         round(main_exp_cost, 4),
        "standing_charge":  total_sc,
        "net_cost":         net_cost,
        "weighted_rate":    weighted_rate,
        "total_days":       total_days,
        # Rate tier distribution (main meter)
        "rate_tiers":       rate_tiers_list,
        # Net grid position
        "net_grid_kwh":      net_grid_kwh,    # positive = net importer, negative = net exporter
        "net_exporter_days": net_exporter_days,
        # House consumption (excl sub-meters)
        "house_imp_kwh":    round(house_imp_kwh,  3),
        "house_imp_cost":   round(house_imp_cost, 4),
        # Peak demand
        "peak_window_start": peak_window_start,   # local hour 0-23
        "peak_window_kwh":   peak_window_kwh,
        # Sub-meters
        "sub_meters":        sub_totals,
    }


@app.route("/api/usage/billing-period")
def api_usage_billing_period():
    """
    Usage insights for a billing period.
    Query params: period_start=YYYY-MM-DD (local date). Omit for current period.
    """
    try:
        import energy_charts as _ec
        from zoneinfo import ZoneInfo as _ZI
        from datetime import datetime, timezone
        store   = _get_store()
        cfg     = load_config()
        tz_name = "UTC"
        for md in (cfg.get("meters") or {}).values():
            tz_name = (md.get("meta") or {}).get("timezone", "UTC")
            break
        tz = _ZI(tz_name)

        periods = _ec.get_billing_periods_from_config_periods(
            store.get_config_periods(), tz=tz
        )
        if not periods:
            return jsonify({"error": "No billing periods found"}), 404

        requested = request.args.get("period_start")
        now_local = datetime.now(tz).replace(tzinfo=None)
        target_ps = target_pe = None
        for (ps, pe) in periods:
            if requested:
                if ps.date().isoformat() == requested:
                    target_ps, target_pe = ps, pe
                    break
            else:
                target_ps, target_pe = ps, pe

        if not target_ps:
            return jsonify({"error": "Billing period not found"}), 404

        is_current = target_ps <= now_local < target_pe
        ps_iso = target_ps.astimezone(timezone.utc).replace(tzinfo=None).isoformat()
        pe_iso = target_pe.astimezone(timezone.utc).replace(tzinfo=None).isoformat()

        d = _aggregate_usage(store, cfg, ps_iso, pe_iso, tz_name)
        d["period_start"] = target_ps.date().isoformat()
        d["period_end"]   = target_pe.date().isoformat()
        d["is_current"]   = is_current
        return jsonify(d)
    except Exception as e:
        logger.exception("api_usage_billing_period failed")
        return jsonify({"error": str(e)}), 500


@app.route("/api/usage/calendar-month")
def api_usage_calendar_month():
    """
    Usage insights for a calendar month.
    Query params: year=YYYY, month=MM (1-12)
    """
    try:
        from zoneinfo import ZoneInfo as _ZI
        from datetime import datetime, timezone, timedelta
        store   = _get_store()
        cfg     = load_config()
        tz_name = "UTC"
        for md in (cfg.get("meters") or {}).values():
            tz_name = (md.get("meta") or {}).get("timezone", "UTC")
            break
        tz = _ZI(tz_name)

        year  = int(request.args.get("year",  datetime.now(tz).year))
        month = int(request.args.get("month", datetime.now(tz).month))

        local_start = datetime(year, month, 1, tzinfo=tz)
        if month == 12:
            local_end = datetime(year + 1, 1, 1, tzinfo=tz)
        else:
            local_end = datetime(year, month + 1, 1, tzinfo=tz)

        utc_start = local_start.astimezone(timezone.utc).replace(tzinfo=None).isoformat()
        utc_end   = local_end.astimezone(timezone.utc).replace(tzinfo=None).isoformat()

        d = _aggregate_usage(store, cfg, utc_start, utc_end, tz_name)
        d["period_start"] = local_start.date().isoformat()
        d["period_end"]   = (local_end - timedelta(days=1)).date().isoformat()
        d["period_label"] = local_start.strftime("%B %Y")
        return jsonify(d)
    except Exception as e:
        logger.exception("api_usage_calendar_month failed")
        return jsonify({"error": str(e)}), 500


@app.route("/api/usage/calendar-year")
def api_usage_calendar_year():
    """
    Usage insights for a calendar year.
    Query params: year=YYYY
    """
    try:
        from zoneinfo import ZoneInfo as _ZI
        from datetime import datetime, timezone, timedelta
        store   = _get_store()
        cfg     = load_config()
        tz_name = "UTC"
        for md in (cfg.get("meters") or {}).values():
            tz_name = (md.get("meta") or {}).get("timezone", "UTC")
            break
        tz = _ZI(tz_name)

        year = int(request.args.get("year", datetime.now(tz).year))

        local_start = datetime(year, 1, 1, tzinfo=tz)
        local_end   = datetime(year + 1, 1, 1, tzinfo=tz)

        utc_start = local_start.astimezone(timezone.utc).replace(tzinfo=None).isoformat()
        utc_end   = local_end.astimezone(timezone.utc).replace(tzinfo=None).isoformat()

        d = _aggregate_usage(store, cfg, utc_start, utc_end, tz_name)
        d["period_start"] = local_start.date().isoformat()
        d["period_end"]   = (local_end - timedelta(days=1)).date().isoformat()
        d["period_label"] = str(year)
        return jsonify(d)
    except Exception as e:
        logger.exception("api_usage_calendar_year failed")
        return jsonify({"error": str(e)}), 500


@app.route("/api/storage-info")
def api_storage_info():
    """Return DB size, disk usage, growth rate and runway prediction."""
    import shutil as _shutil
    store = _get_store()
    db_path = os.path.join(DATA_DIR, "blocks.db")

    # DB and disk sizes
    db_bytes  = os.path.getsize(db_path) if os.path.exists(db_path) else 0
    try:
        du = _shutil.disk_usage(DATA_DIR)
        disk_total = du.total
        disk_free  = du.free
        disk_used  = du.used
    except Exception:
        disk_total = disk_free = disk_used = 0

    # Growth rate — oldest block to now
    growth_mb_per_day  = None
    runway_days        = None
    days_of_data       = None
    try:
        row = store._conn.execute(
            "SELECT MIN(block_start) FROM blocks"
        ).fetchone()
        if row and row[0]:
            from datetime import datetime, timezone
            oldest = datetime.fromisoformat(row[0]).replace(tzinfo=timezone.utc)
            now    = datetime.now(timezone.utc)
            days_of_data = max((now - oldest).total_seconds() / 86400, 1)
            growth_mb_per_day = (db_bytes / 1024 / 1024) / days_of_data
            if growth_mb_per_day > 0:
                runway_days = int((disk_free / 1024 / 1024) / growth_mb_per_day)
    except Exception:
        pass

    return jsonify({
        "db_bytes":         db_bytes,
        "db_mb":            round(db_bytes / 1024 / 1024, 2),
        "disk_total_mb":    round(disk_total / 1024 / 1024, 1),
        "disk_free_mb":     round(disk_free / 1024 / 1024, 1),
        "disk_used_mb":     round(disk_used / 1024 / 1024, 1),
        "disk_free_pct":    round(disk_free / disk_total * 100, 1) if disk_total else 0,
        "days_of_data":     round(days_of_data, 1) if days_of_data else None,
        "growth_mb_per_day": round(growth_mb_per_day, 4) if growth_mb_per_day else None,
        "runway_days":      runway_days,
    })


@app.route("/api/import/extract-zip", methods=["POST"])
def api_import_extract_zip():
    """Extract JSON files from an uploaded zip and return them as base64."""
    import zipfile, base64
    try:
        zf_file = request.files.get("zipfile")
        if not zf_file:
            return jsonify({"error": "No zip file provided"}), 400
        known = {"blocks.db", "blocks.json"}
        files = {}
        with zipfile.ZipFile(zf_file.stream, "r") as zf:
            for name in zf.namelist():
                basename = os.path.basename(name)
                if basename in known:
                    files[basename] = base64.b64encode(zf.read(name)).decode("utf-8")
        if not files:
            return jsonify({"error": "No recognised files found in zip"}), 400
        logger.info("api_import_extract_zip: extracted %s", list(files.keys()))
        return jsonify({"files": files})
    except Exception as e:
        logger.error("api_import_extract_zip: %s", e)
        return jsonify({"error": str(e)}), 500


@app.route("/api/backup/flat-info", methods=["GET"])
def api_backup_flat_info():
    """Return metadata about the last-finalise flat backup files.

    Primary files (written by 2.1.0+):
      blocks.db       — the only file needed for a full restore
      meters_config.json — convenience export

    Legacy files (written by older versions, no longer updated):
      blocks.json     — flat JSON format from 1.x/2.0.x
    """
    from datetime import datetime as _dt
    # Primary files — written by current engine
    primary = ["blocks.db"]
    # Legacy files — may be present from older versions, no longer written
    legacy  = ["blocks.json"]

    files = {}
    for fname in primary + legacy:
        fpath = f"{_share_backup_dir()}/{fname}"
        if fname == "meters_config.json":
            continue  # no longer a restore target — config lives inside blocks.db
        if os.path.exists(fpath):
            mtime = os.path.getmtime(fpath)
            size  = os.path.getsize(fpath)
            entry = {
                "modified": _dt.utcfromtimestamp(mtime).strftime("%Y-%m-%dT%H:%M:%S UTC"),
                "size_kb":  round(size / 1024, 1),
            }
            if fname in legacy:
                entry["legacy"] = True
                entry["note"]   = "Legacy format from v1.x/v2.0.x — no longer updated"
            files[fname] = entry
    return jsonify(files)


@app.route("/api/import/extract-zip-by-name", methods=["POST"])
def api_import_extract_zip_by_name():
    """Extract JSON files from a named backup zip (server-side) and return as base64."""
    import zipfile, base64
    try:
        data    = request.get_json(force=True)
        zipname = data.get("zip", "")
        if not zipname or "/" in zipname or "\\" in zipname:
            return jsonify({"error": "Invalid zip name"}), 400
        zip_path = f"{_share_backup_dir()}/backups/{zipname}"
        if not os.path.exists(zip_path):
            return jsonify({"error": "Backup not found"}), 404
        known = {"blocks.db", "blocks.json"}
        files = {}
        with zipfile.ZipFile(zip_path, "r") as zf:
            for name in zf.namelist():
                basename = os.path.basename(name)
                if basename in known:
                    files[basename] = base64.b64encode(zf.read(name)).decode("utf-8")
        if not files:
            return jsonify({"error": "No recognised JSON files found in backup"}), 400
        return jsonify({"files": files})
    except Exception as e:
        logger.error("api_import_extract_zip_by_name: %s", e)
        return jsonify({"error": str(e)}), 500


@app.route("/api/backup/zip-entries", methods=["POST"])
def api_backup_zip_entries():
    """List the recognised files inside a named backup zip — names + sizes ONLY, no
    decompression of the bodies. This is what the Restore dialog needs to build its
    file picker; the old path base64-encoded the whole blocks.db (tens of MB) just to
    read the filenames, which stalled the dialog. The actual restore re-extracts the
    zip server-side, so nothing else needs the contents here."""
    import zipfile
    try:
        data = request.get_json(force=True) or {}
        zipname = data.get("zip", "")
        if not zipname or "/" in zipname or "\\" in zipname:
            return jsonify({"error": "Invalid zip name"}), 400
        zip_path = f"{_share_backup_dir()}/backups/{zipname}"
        if not os.path.exists(zip_path):
            return jsonify({"error": "Backup not found"}), 404
        known = {"blocks.db", "blocks.json"}
        entries = {}
        with zipfile.ZipFile(zip_path, "r") as zf:
            for info in zf.infolist():
                base = os.path.basename(info.filename)
                if base in known:
                    entries[base] = {"size_kb": round(info.file_size / 1024.0, 1)}
        if not entries:
            return jsonify({"error": "No recognised files found in backup"}), 400
        return jsonify({"files": entries})
    except Exception as e:
        logger.error("api_backup_zip_entries: %s", e)
        return jsonify({"error": str(e)}), 500


def _create_backup_zip(label="backup"):
    """Zip all data files into /share/energy_meter_tracker_backup/backups/."""
    import zipfile
    import glob
    from datetime import datetime as _dt
    backup_dir = f"{_share_backup_dir()}/backups"
    os.makedirs(backup_dir, exist_ok=True)
    timestamp = _dt.utcnow().strftime("%Y%m%dT%H%M%S")
    zip_path  = f"{backup_dir}/{timestamp}_{label}.zip"
    files = []  # blocks.db is the only file needed — config is inside it
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
        # Backup blocks DB using SQLite online backup API into a temp file
        import tempfile
        db_src = os.path.join(DATA_DIR, "blocks.db")
        if os.path.exists(db_src):
            try:
                # Use the engine's store for backup — it holds the primary write
                # connection. Force a WAL checkpoint first so all committed pages
                # are in the main DB file and the backup is complete.
                import sys as _sys_bk
                _eng_bk = _sys_bk.modules.get("engine")
                _bk_store = None
                if _eng_bk and hasattr(_eng_bk, "get_store"):
                    try:
                        _bk_store = _eng_bk.get_store()
                        _bk_store._conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
                    except Exception:
                        _bk_store = None
                if _bk_store is None:
                    _bk_store = _get_store()
                # Unique temp name per call: two backups can run at once (e.g. a
                # config-save's pre_config_save and a restore's pre_restore), and a
                # shared 'blocks.db.bak' let one delete the other's file mid-write →
                # "No such file: blocks.db.bak". Scope it to this pid+thread.
                _bak = f"{db_src}.{os.getpid()}.{threading.get_ident()}.bak"
                try:
                    _bk_store.backup(_bak)
                    zf.write(_bak, "blocks.db")
                finally:
                    if os.path.exists(_bak):
                        try: os.remove(_bak)
                        except OSError: pass
            except Exception as _e:
                logger.warning("_create_backup_zip: blocks.db backup failed: %s", _e)
        for fname in files:
            src_f = f"{DATA_DIR}/{fname}"
            if os.path.exists(src_f):
                zf.write(src_f, fname)
    # Keep only the 20 most recent zips
    all_zips = sorted(glob.glob(f"{backup_dir}/*.zip"))
    for old_zip in all_zips[:-20]:
        try: os.remove(old_zip)
        except: pass
    logger.info("_create_backup_zip: %s written", os.path.basename(zip_path))
    return zip_path


@app.route("/api/charts/regenerate", methods=["POST"])
def api_regenerate_charts():
    """Trigger chart regeneration from current blocks data."""
    try:
        import energy_charts
        from energy_engine_io import load_json as _lj_regen
        store = _get_store()
        if store.count_blocks() == 0:
            return jsonify({"error": "No blocks data available"}), 400
        cfg       = load_config()
        main_meta = {}
        for md in cfg.get("meters", {}).values():
            if not (md.get("meta") or {}).get("sub_meter"):
                main_meta = md.get("meta") or {}
                break
        tz_name  = main_meta.get("timezone", "UTC")
        bm       = int(main_meta.get("block_minutes") or 30)
        currency = main_meta.get("currency_symbol", "£")
        os.makedirs(CHART_DIR, exist_ok=True)
        blocks = store.get_blocks_lightweight()
        html = energy_charts.generate_net_heatmap(blocks, timezone_name=tz_name, block_minutes=bm, currency=currency)
        with open(os.path.join(CHART_DIR, "net_heatmap.html"), "w") as f:
            f.write(html)
        html = energy_charts.generate_daily_import_export_charts(blocks, timezone_name=tz_name, block_minutes=bm, currency=currency, cfg=cfg, store=store)
        with open(os.path.join(CHART_DIR, "daily_usage.html"), "w") as f:
            f.write(html)
        logger.info("server: charts regenerated on demand")
        return jsonify({"ok": True})
    except Exception as e:
        logger.error("api_regenerate_charts: %s", e)
        return jsonify({"error": str(e)}), 500



@app.route("/api/import/from-zip", methods=["POST"])
def api_import_from_zip():
    """
    Accept an uploaded backup zip and restore blocks.db (and optionally
    meters_config.json) directly server-side — no base64 round-trip through
    the browser, which can corrupt binary DB files.
    The DB is always authoritative; meters_config.json is written FROM the DB.
    """
    import zipfile, sys
    global _store
    try:
        zf_file = request.files.get("zipfile")
        if not zf_file:
            return jsonify({"error": "No zip file provided"}), 400

        engine = sys.modules.get("engine")
        if engine and hasattr(engine, "pause_engine"):
            engine.pause_engine()

        known    = {"blocks.db", "blocks.json"}
        imported = []

        # Auto-backup before overwriting
        try:
            _create_backup_zip(label="pre_import")
        except Exception as _bke:
            logger.warning("api_import_from_zip: pre-import backup failed: %s", _bke)

        with zipfile.ZipFile(zf_file.stream, "r") as zf:
            names = {os.path.basename(n): n for n in zf.namelist() if os.path.basename(n) in known}
            logger.info("api_import_from_zip: zip contains %s", list(names.keys()))

            # ── blocks.db — extract directly, no base64 ───────────────────────
            if "blocks.db" in names:
                dest = os.path.join(DATA_DIR, "blocks.db")
                tmp  = dest + ".import_tmp"
                try:
                    zip_info = zf.getinfo(names["blocks.db"])
                    logger.info("api_import_from_zip: blocks.db in zip is %d bytes", zip_info.file_size)
                    with zf.open(names["blocks.db"]) as src, open(tmp, "wb") as dst:
                        dst.write(src.read())
                    tmp_size = os.path.getsize(tmp)
                    logger.info("api_import_from_zip: wrote tmp file %d bytes", tmp_size)
                    # Validate
                    import sqlite3 as _sq3
                    _tc = _sq3.connect(tmp)
                    count = _tc.execute("SELECT COUNT(*) FROM blocks").fetchone()[0]
                    _tc.close()
                    logger.info("api_import_from_zip: validated %d blocks in tmp file", count)
                    # Close web server store
                    if _store:
                        try: _store.close()
                        except Exception: pass
                        _store = None
                    os.replace(tmp, dest)
                    imported.append("blocks.db")
                    logger.info("api_import_from_zip: restored blocks.db (%d blocks)", count)

                    # Reset engine store so it picks up the new file immediately
                    _eng_early = sys.modules.get("engine")
                    if _eng_early and hasattr(_eng_early, "reset_store"):
                        try:
                            _eng_early.reset_store()
                            logger.info("api_import_from_zip: engine store reset OK")
                        except Exception as _re:
                            logger.warning("api_import_from_zip: engine reset_store failed: %s", _re)

                except Exception as _dbe:
                    if os.path.exists(tmp):
                        try: os.remove(tmp)
                        except Exception: pass
                    raise RuntimeError(f"blocks.db restore failed: {_dbe}") from _dbe

        if not imported:
            return jsonify({"error": "No blocks.db found in zip"}), 400

        # ── Post-import: remove WAL/SHM and reset engine store ───────────────
        # No need to write meters_config.json — engine_startup reads config
        # directly from the restored DB on next start.
        if "blocks.db" in imported:
            _dest = os.path.join(DATA_DIR, "blocks.db")
            for _ext in ("-wal", "-shm"):
                _f = _dest + _ext
                if os.path.exists(_f):
                    try: os.remove(_f)
                    except Exception: pass

        # Reset engine store so it reopens from the new DB file
        import sys as _sys
        _eng = _sys.modules.get("engine")
        if _eng and hasattr(_eng, "reset_store"):
            try:
                _eng.reset_store()
                logger.info("api_import_from_zip: engine store reset OK")
            except Exception as _re:
                logger.warning("api_import_from_zip: engine reset_store failed: %s", _re)

        # Regenerate the pre-built charts against the restored DB now, rather than
        # waiting for the next block to finalise (issue #257).
        _regen_charts_safely()

        return jsonify({"ok": True, "imported": imported})

    except Exception as e:
        logger.error("api_import_from_zip: %s", e)
        return jsonify({"error": str(e)}), 500
    finally:
        import sys as _sys
        _eng = _sys.modules.get("engine")
        if _eng and hasattr(_eng, "resume_engine"):
            try: _eng.resume_engine()
            except Exception: pass


@app.route("/api/import", methods=["POST"])
def api_import():
    """Accept an uploaded blocks.db and restore it through the SAME background job
    as the backup-zip restore (#356): a pre-restore safety backup, live progress
    via /api/backup/restore/status, an atomic swap, store reset and gap detection.

    The upload is staged and SQLite-validated synchronously (a bad file fails here,
    with the live DB untouched); the swap itself runs in the shared _restore_worker
    so the client sees the same progress banner instead of a silent, blocking wait.
    Returns {ok, status:"running"}; the client polls the restore status.
    (blocks.json import removed in 2.2.2; meters_config.json is ignored — the DB is
    authoritative.)"""
    global _restore_job
    try:
        blocks_file = request.files.get("blocks")
        if not blocks_file:
            return jsonify({"error": "No files received"}), 400

        # Stage + validate BEFORE touching the live DB. A non-SQLite / wrong-schema
        # upload is rejected synchronously here and the live database is never
        # disturbed.
        staged = os.path.join(DATA_DIR, "uploaded_restore.db")
        try:
            blocks_file.save(staged)
            import sqlite3 as _sq3
            _tc = _sq3.connect(staged)
            try:
                _tc.execute("SELECT COUNT(*) FROM blocks")
            finally:
                _tc.close()
        except Exception as _ve:
            if os.path.exists(staged):
                try: os.remove(staged)
                except Exception: pass
            logger.error("api_import: uploaded file is not a valid blocks.db: %s", _ve)
            return jsonify({"error": "That file is not a valid EMT database "
                                     "(blocks.db)."}), 400

        # Don't collide with an in-flight restore.
        if _restore_job.get("status") == "running":
            if os.path.exists(staged):
                try: os.remove(staged)
                except Exception: pass
            return jsonify({"error": "a restore is already running",
                            "status": _restore_job}), 409

        _restore_job.clear()
        _restore_job.update({"status": "running", "step": "starting",
                             "restored": None, "error": None})
        threading.Thread(target=_restore_worker,
                         args=("", ["blocks.db"], False),
                         kwargs={"staged": staged},
                         daemon=True, name="restore-upload").start()
        return jsonify({"ok": True, "status": "running"})
    except Exception as e:
        logger.error("api_import: %s", e)
        return jsonify({"error": str(e)}), 500

# ── Database maintenance ───────────────────────────────────────────────────────────────────

@app.route("/api/db/vacuum", methods=["POST"])
def api_db_vacuum():
    """
    Run VACUUM on the blocks database.
    Pauses the engine during the operation to ensure exclusive DB access.
    Returns { ok, size_before_kb, size_after_kb, saved_kb }.
    """
    import os
    try:
        import engine as _eng
        store = _get_store()

        # Measure before
        db_path = store._conn.execute("PRAGMA database_list").fetchone()["file"]
        size_before = os.path.getsize(db_path) if db_path and os.path.exists(db_path) else 0

        # Check free pages — gives user an honest picture before we run
        page_size  = store._conn.execute("PRAGMA page_size").fetchone()[0]
        free_pages = store._conn.execute("PRAGMA freelist_count").fetchone()[0]
        free_bytes = free_pages * page_size

        # Pause engine, run VACUUM, resume
        _eng.pause_engine()
        try:
            store._conn.execute("VACUUM")
            store._conn.commit()
        finally:
            _eng.resume_engine()

        size_after = os.path.getsize(db_path) if db_path and os.path.exists(db_path) else 0
        saved = max(0, size_before - size_after)

        logger.info(
            "api_db_vacuum: complete — before=%d B after=%d B saved=%d B (free_pages=%d)",
            size_before, size_after, saved, free_pages
        )
        return jsonify({
            "ok":           True,
            "size_before":  size_before,
            "size_after":   size_after,
            "saved":        saved,
            "free_pages":   free_pages,
            "free_bytes":   free_bytes,
        })
    except Exception as e:
        try:
            import engine as _eng
            _eng.resume_engine()
        except Exception:
            pass
        logger.error("api_db_vacuum: %s", e)
        return jsonify({"error": str(e)}), 500


# ── Block deletion ────────────────────────────────────────────────────────────────────────────────

def _delete_window_to_utc_times(from_time, to_time, tz_name, from_date, to_date):
    """Resolve the delete's time-of-day window to the UTC HH:MM the store filters on.

    A WHOLE-DAY window (00:00–23:59) must NOT become a UTC time-of-day filter: the
    local-date→UTC bounds (local_date_range_to_utc_bounds) already cover complete
    local days. Converting the day boundaries to UTC under BST turns 23:59 local
    into 22:59 UTC → `TIME(block_start) <= '22:59'`, which excludes the 23:00–23:59
    UTC band that IS local midnight — so 2 blocks/day (00:00 & 00:30 local) survive
    every delete. Only an EXPLICIT partial window is converted for the filter.
    """
    if from_time == "00:00" and to_time == "23:59":
        return "00:00", "23:59"            # whole day → rely on the date bounds only
    return (_corrections_time_to_utc(from_time, tz_name, from_date),
            _corrections_time_to_utc(to_time, tz_name, to_date))


@app.route("/api/blocks/delete/preview", methods=["POST"])
def api_blocks_delete_preview():
    """
    Preview how many blocks would be deleted for a date range.
    Body: { from_date, to_date, meter_id? }
    Returns: { blocks, dates }
    """
    try:
        data      = request.get_json(force=True) or {}
        from_date = data.get("from_date", "").strip()
        from_time = data.get("from_time", "00:00").strip() or "00:00"
        to_date   = data.get("to_date", "").strip()
        to_time   = data.get("to_time", "23:59").strip() or "23:59"
        meter_id  = data.get("meter_id") or None
        if not from_date or not to_date:
            return jsonify({"error": "from_date and to_date required"}), 400
        if from_date > to_date:
            return jsonify({"error": "from_date must not be after to_date"}), 400
        # Convert local times to UTC using the configured timezone
        _cfg = load_config()
        _tz_name = "UTC"
        for _md in (_cfg.get("meters") or {}).values():
            if not (_md.get("meta") or {}).get("sub_meter"):
                _tz_name = (_md.get("meta") or {}).get("timezone", "UTC")
                break
        reconstructed_only = bool(data.get("reconstructed_only"))
        store = _get_store()
        # Pass LOCAL times: the store treats from/to as the ends of ONE contiguous
        # span (from_date+from_time .. to_date+to_time), not a per-day window.
        result = store.count_blocks_for_date_range(from_date, to_date, meter_id, from_time, to_time, _tz_name, reconstructed_only)
        return jsonify(result)
    except Exception as e:
        logger.error("api_blocks_delete_preview: %s", e)
        return jsonify({"error": str(e)}), 500


# Background delete/purge job — shared status so the UI can show a persistent
# "Deleting... don't rebuild/restart" banner that survives navigation, exactly
# like the restore job. Only one destructive op runs at a time (both use this).
_delete_job = {"status": "idle"}


def _delete_step(s):
    _delete_job["step"] = s
    logger.info("delete: %s", s)


@app.route("/api/blocks/delete/status", methods=["GET"])
def api_blocks_delete_status():
    """Poll the background delete/purge job's progress/status."""
    return jsonify(_delete_job)


def _delete_worker(from_date, from_time_utc, to_date, to_time_utc,
                   meter_id, tz_name, reconstructed_only):
    """Background date-range delete worker (own thread; survives navigation).
    Same store access as the old synchronous route (off the engine loop thread),
    just with progress reported into _delete_job."""
    _j = _delete_job
    import engine as _eng
    _eng.set_delete_active(True)   # pause the carbon backfill from starting mid-delete
    try:
        _delete_step("deleting blocks")
        store = _get_store()
        result = store.delete_blocks_for_date_range(
            from_date, to_date, meter_id, from_time_utc, to_time_utc,
            tz_name, reconstructed_only)
        # A device-only delete leaves the parent main meter's remainder stale for
        # those windows; recompute it (PASS 2) against the surviving sub-meters.
        if result.get("recompute_parent"):
            try:
                import engine as _eng
                _delete_step("recomputing meter remainders")
                result["remainders_recomputed"] = _eng.recompute_remainders_for_window(
                    result["recompute_parent"], result["recompute_from"],
                    result["recompute_to"])
            except Exception as _re:
                logger.error("_delete_worker: remainder recompute failed: %s", _re)
                result["remainders_recomputed"] = 0
        _delete_step("rebuilding charts")
        _regen_charts_safely()
        logger.info("_delete_worker: deleted %d blocks across %d dates "
                    "(meter=%s)", result.get("deleted"), result.get("dates"),
                    meter_id or "all")
        _j.update({"status": "done", "kind": "delete", "result": result, "error": None})
    except Exception as e:
        logger.error("_delete_worker: %s", e)
        _j.update({"status": "error", "error": str(e)})
    finally:
        _eng.set_delete_active(False)


def _purge_worker():
    """Background 'delete all imported data' worker (own thread). Backs up first,
    purges every imported block + derivations + checkpoints, rebuilds charts."""
    _j = _delete_job
    import engine as _eng
    _eng.set_delete_active(True)   # pause the carbon backfill from starting mid-purge
    try:
        store = _get_store()
        backup = None
        try:
            _delete_step("taking a safety backup")
            backup = os.path.basename(_create_backup_zip(label="pre-purge-import"))
        except Exception as be:
            logger.warning("purge: backup failed (continuing): %s", be)
        _delete_step("deleting all imported data")
        result = store.purge_imported_history()
        result["backup"] = backup
        _delete_step("rebuilding charts")
        _regen_charts_safely()
        logger.info("_purge_worker: removed %d imported block(s) (backup=%s)",
                    result.get("blocks"), backup)
        _j.update({"status": "done", "kind": "purge",
                   "result": {"ok": True, **result}, "error": None})
    except Exception as e:
        logger.error("_purge_worker: %s", e)
        _j.update({"status": "error", "error": str(e)})
    finally:
        _eng.set_delete_active(False)


@app.route("/api/blocks/delete", methods=["POST"])
def api_blocks_delete():
    """
    Delete blocks for a date range as a BACKGROUND job — returns at once so it
    finishes even if the user navigates away. Poll /api/blocks/delete/status.
    Body: { from_date, to_date, meter_id?, confirmed: true }
    """
    try:
        data      = request.get_json(force=True) or {}
        from_date = data.get("from_date", "").strip()
        from_time = data.get("from_time", "00:00").strip() or "00:00"
        to_date   = data.get("to_date", "").strip()
        to_time   = data.get("to_time", "23:59").strip() or "23:59"
        meter_id  = data.get("meter_id") or None
        confirmed = data.get("confirmed", False)
        if not from_date or not to_date:
            return jsonify({"error": "from_date and to_date required"}), 400
        if from_date > to_date:
            return jsonify({"error": "from_date must not be after to_date"}), 400
        if not confirmed:
            return jsonify({"error": "confirmed must be true"}), 400
        if _delete_job.get("status") == "running":
            return jsonify({"error": "a delete is already running",
                            "status": _delete_job}), 409
        # Refuse while a kraken poll / BL-8 backfill is mutating blocks — the two
        # share the single SQLite connection and a delete racing an in-flight
        # backfill leaves a half-deleted day (the 'deleted the 12th mid-backfill,
        # it came back partial' incident). The flag is cleared when the poll cycle
        # finishes, so a retry succeeds once the log shows poll completion.
        import engine as _eng
        if _eng.poll_in_progress():
            return jsonify({"error": "Unable to delete — a backfill is in progress. "
                            "Please try again once it completes (watch the logs for "
                            "poll completion), then retry.",
                            "retry": True}), 409
        # Resolve the configured timezone; the store converts the LOCAL from/to
        # times into one CONTIGUOUS UTC span (not a per-day time-of-day window).
        _cfg = load_config()
        _tz_name = "UTC"
        for _md in (_cfg.get("meters") or {}).values():
            if not (_md.get("meta") or {}).get("sub_meter"):
                _tz_name = (_md.get("meta") or {}).get("timezone", "UTC")
                break
        reconstructed_only = bool(data.get("reconstructed_only"))
        _delete_job.clear()
        _delete_job.update({"status": "running", "kind": "delete",
                            "step": "starting", "result": None, "error": None})
        threading.Thread(
            target=_delete_worker,
            args=(from_date, from_time, to_date, to_time,
                  meter_id, _tz_name, reconstructed_only),
            daemon=True, name="delete").start()
        return jsonify({"ok": True, "status": "running"})
    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    except Exception as e:
        logger.error("api_blocks_delete: %s", e)
        return jsonify({"error": str(e)}), 500


@app.route("/api/historical/purge-preview")
def api_historical_purge_preview():
    """How much historical-import data would a purge remove (block count + span)."""
    try:
        return jsonify(_get_store().count_imported_history())
    except Exception as e:
        logger.error("api_historical_purge_preview: %s", e)
        return jsonify({"error": str(e)}), 500


@app.route("/api/historical/purge", methods=["POST"])
def api_historical_purge():
    """The get-out-clause: back up, then delete ALL historical-import data and
    everything derived from it (imported blocks, rate derivations, import
    checkpoints/gaps). Requires confirmed=true. Live data is untouched."""
    try:
        body = request.get_json(force=True, silent=True) or {}
        if not body.get("confirmed"):
            return jsonify({"error": "confirmation required"}), 400
        if _delete_job.get("status") == "running":
            return jsonify({"error": "a delete is already running",
                            "status": _delete_job}), 409
        _delete_job.clear()
        _delete_job.update({"status": "running", "kind": "purge",
                            "step": "starting", "result": None, "error": None})
        threading.Thread(target=_purge_worker, daemon=True, name="purge").start()
        return jsonify({"ok": True, "status": "running"})
    except Exception as e:
        logger.error("api_historical_purge: %s", e)
        return jsonify({"error": str(e)}), 500


# ── Historical corrections ────────────────────────────────────────────────────

def _corrections_time_to_utc(time_str: str, tz_name: str,
                              anchor_date: str = "") -> str:
    """
    Convert a local HH:MM time string to UTC HH:MM for use in block_start TIME() filter.
    anchor_date (YYYY-MM-DD) should be the correction date so the correct DST offset is
    applied — e.g. a March date uses GMT, an April date uses BST.
    Returns the original string if conversion fails.
    """
    if not time_str:
        return time_str
    try:
        from zoneinfo import ZoneInfo
        from datetime import datetime, timezone
        h, m = int(time_str[:2]), int(time_str[3:5])
        if anchor_date:
            y, mo, d = int(anchor_date[:4]), int(anchor_date[5:7]), int(anchor_date[8:10])
            naive = datetime(y, mo, d, h, m, 0)
        else:
            naive = datetime.now().replace(hour=h, minute=m, second=0, microsecond=0)
        local_dt = naive.replace(tzinfo=ZoneInfo(tz_name))
        utc_dt   = local_dt.astimezone(timezone.utc)
        return utc_dt.strftime("%H:%M")
    except Exception:
        return time_str


def _corrections_build_where(corr_type, from_date, to_date, channel,
                              from_time_utc, to_time_utc, meter_id, tz_name="UTC",
                              api_settled_only=False):
    """
    Build WHERE clause and params for corrections queries.

    Date filtering uses UTC block_start bounds derived from local dates via
    local_date_range_to_utc_bounds() so BST blocks are handled correctly.

    Time filtering uses TIME(block_start) in UTC, with the times already converted
    from local to UTC by _corrections_time_to_utc. When the converted window crosses
    UTC midnight (e.g. Economy 7 00:30-07:30 BST = 23:30-06:30 UTC), the clause uses
    OR instead of AND so blocks on both sides of midnight are included.

    Standing charge corrections are always restricted to the main meter
    (meter_id in the meters table with is_sub_meter=0) — sub-meter rows
    carry sc=0.0 and must not be overwritten.
    """
    from block_store import local_date_range_to_utc_bounds as _corr_utc_bounds
    col = None
    if corr_type == "rate":
        col = "imp_rate" if channel == "import" else "exp_rate"

    # Use UTC bounds derived from local dates — handles BST correctly without local_date column
    _utc_s, _utc_e = _corr_utc_bounds(from_date, to_date, tz_name)
    clauses = ["block_start >= ?", "block_start < ?"]
    params  = [_utc_s, _utc_e]

    if from_time_utc and to_time_utc:
        if from_time_utc <= to_time_utc:
            # Normal window — entirely within one UTC day
            clauses.append("TIME(block_start) >= ?")
            params.append(from_time_utc)
            clauses.append("TIME(block_start) < ?")
            params.append(to_time_utc)
        else:
            # Window crosses UTC midnight (e.g. BST night rate)
            # Select blocks OUTSIDE the gap (i.e. after from OR before to)
            clauses.append(
                "(TIME(block_start) >= ? OR TIME(block_start) < ?)"
            )
            params.extend([from_time_utc, to_time_utc])
    elif from_time_utc:
        clauses.append("TIME(block_start) >= ?")
        params.append(from_time_utc)
    elif to_time_utc:
        clauses.append("TIME(block_start) < ?")
        params.append(to_time_utc)

    if corr_type == "standing":
        # Standing charge only lives on main meter rows — always restrict
        # regardless of the meter_id selector (which is hidden for standing)
        clauses.append(
            "meter_id IN (SELECT meter_id FROM meters WHERE is_sub_meter = 0)"
        )
    elif meter_id and meter_id != "all":
        clauses.append("meter_id = ?")
        params.append(meter_id)

    if col:
        clauses.append(f"{col} IS NOT NULL")

    if api_settled_only and corr_type == "rate":
        # API+ gate: only DCC-settled blocks (imp_kwh_api/exp_kwh_api populated)
        # may have their rate corrected. An unsettled block's kWh — and its
        # overlay-derived rate — are still going to be rewritten at settlement,
        # which would clobber the correction.
        settled_col = "imp_kwh_api" if channel == "import" else "exp_kwh_api"
        clauses.append(f"{settled_col} IS NOT NULL")

    return " AND ".join(clauses), params, col


def _corrections_api_gate_active() -> bool:
    """True when the data-source mode uses the DCC API, so rate corrections must
    be gated to settled blocks. Pure CAD (no API) has no DCC reconciliation to
    clobber a correction, so it returns False and corrections apply immediately.
    """
    try:
        import engine as _eng
        return bool(_eng.mode_uses_api(_eng.get_data_source_mode()))
    except Exception:
        return False


def _corrections_unreconciled_count(store, from_date, to_date, channel,
                                    from_time_utc, to_time_utc, meter_id,
                                    tz_name) -> int:
    """Count in-window rate blocks NOT yet DCC-settled — the ones a gated rate
    correction will skip — so the UI can tell the user to retry after settlement.
    """
    settled_col = "imp_kwh_api" if channel == "import" else "exp_kwh_api"
    where, params, _ = _corrections_build_where(
        "rate", from_date, to_date, channel,
        from_time_utc, to_time_utc, meter_id, tz_name, api_settled_only=False)
    # Only MAIN-meter blocks settle via DCC (imp_kwh_api). Device rows never get
    # imp_kwh_api — they follow the main — so counting them here produced a
    # spurious "N blocks await settlement" warning (issue #254c).
    row = store._conn.execute(
        f"SELECT COUNT(*) AS n FROM blocks WHERE {where} AND {settled_col} IS NULL "
        f"AND meter_id IN (SELECT meter_id FROM meters WHERE is_sub_meter = 0)",
        params).fetchone()
    return int((row["n"] if row else 0) or 0)


_CHARGE_SESSION_BRIDGE_MIN = 180  # minutes; merge bursts within one plug-in period
# Dispatch source vocabulary (mirrors engine._SMART_CHARGE_SOURCES). A smart-charge
# dispatch bills off-peak; a bump/boost bills at peak; unknown/null can't be confirmed
# and shows provisional. Used to colour the card by dispatch semantics rather than the
# settlement-laggy block rate.
_SMART_SOURCES = {"smart-charge", "smart"}
_BUMP_SOURCES = {"bump-charge", "boost"}
# The "upcoming" plan is a live forecast that IOG revises. dispatch_history keeps
# every planned slot it ever saw (so absent stays meaningful for *completed*), but
# a slot dropped from a revised plan must not keep inflating the upcoming total.
# So an upcoming planned slot only counts if it was re-seen in (or very near) the
# most-recent planned observation — dispatches are polled ≥5 min apart, so a
# superseded slot's last_seen falls a full poll behind the current plan's.
_UPCOMING_BATCH_SEC = 150   # slots within this of the freshest plan = the current plan
_UPCOMING_STALE_MIN = 20    # if the freshest plan itself is older than this, no live plan


def _slot_is_off_peak(rate, peak_r):
    """A slot billed below the day's peak rate is off-peak. Unknown rate → treat
    as off-peak (no billing data to contradict it, avoids false peak flags)."""
    if rate is None or peak_r is None:
        return True
    return rate < peak_r - 1e-6


def _build_charge_sessions(history, rates, day_peak, slot_kwh=None,
                           bridge_min=_CHARGE_SESSION_BRIDGE_MIN):
    """BL-10: fold *delivered* IOG dispatch slots into charging sessions.

    Pure — no I/O, so it is unit-testable. Dispatch data identifies *which* slots
    were smart-charged (a ``completed`` row); planned-but-never-charged forecasts
    are dropped so the card reflects real charging. Each slot's **energy** is the
    metered grid import for that half-hour (``slot_kwh``) so the card's kWh,
    off-peak/peak split and savings reconcile with the billing charts; when a
    block isn't present yet (unsettled), it falls back to the dispatch figure.
    Each slot is coloured by its **actual billed rate** (``rates`` / ``day_peak``),
    so a slot priced at the day's peak reads peak, not off-peak.

    `history` is a list of dispatch_history rows (slot_start, kind, energy_kwh,
    raw_start?, raw_end?, provider). `rates` maps slot_start → the main meter's
    billed imp_rate for that half-hour. `slot_kwh` maps slot_start → that half-
    hour's metered import (kWh). `day_peak` maps a date (YYYY-MM-DD) → that day's
    highest main rate. The saving is the off-peak benefit vs the day's peak:
    ``max(0, day_peak - slot_rate) * kWh`` per slot (peak-billed slots contribute
    nothing). Consecutive delivered slots no more than ``bridge_min`` minutes
    apart form one session, bridging IOG's inter-burst gaps within a single
    plug-in. Windows use BL-11's second-precision raw_start/raw_end where present,
    else the 30-min slot boundaries. Returns sessions newest-first.
    """
    slot_kwh = slot_kwh or {}
    from datetime import datetime as _dt, timedelta as _td
    slots = {}
    for r in history:
        s = slots.setdefault(r["slot_start"], {
            "started": False, "completed": None,
            "raw_start": None, "raw_end": None, "source": None,
            "pstart": None, "pend": None, "provider": r.get("provider")})
        k = r.get("kind")
        if k == "completed" and r.get("energy_kwh") is not None:
            s["completed"] = r.get("energy_kwh")
        elif k == "started":
            s["started"] = True
        # The smart-charge vs bump/boost signal lives on the PLANNED/started record;
        # completed dispatches report source=null, so capture the source here (used
        # to colour the slot by dispatch semantics, not the settlement-laggy rate).
        if k in ("planned", "started") and r.get("source"):
            s["source"] = r.get("source")
        if r.get("raw_start"):
            s["raw_start"] = r["raw_start"]
        if r.get("raw_end"):
            s["raw_end"] = r["raw_end"]
        # The PLANNED window carries Octopus's real, sub-slot-precise dispatch bounds
        # (e.g. a dispatch that finished early reads 04:30–04:48, and a short one
        # 15:27–15:30) — whereas completedDispatches are padded to the 30-min slot.
        # Octopus re-plans continuously, but once a slot completes its planned row
        # stops updating, so this is the FROZEN last plan for that slot. Tracked
        # separately and used (clipped per slot) for dispatch_minutes below.
        if k == "planned":
            if r.get("raw_start"):
                s["pstart"] = r["raw_start"]
            if r.get("raw_end"):
                s["pend"] = r["raw_end"]

    # Delivered slots only — energy that actually flowed.
    delivered = sorted(k for k, v in slots.items() if v["completed"] is not None)
    if not delivered:
        return []

    groups, cur, prev = [], [], None
    for k in delivered:
        dt = _dt.fromisoformat(k)
        if prev is not None and (dt - prev) > _td(minutes=bridge_min):
            groups.append(cur); cur = []
        cur.append(k); prev = dt
    if cur:
        groups.append(cur)

    out = []
    for grp in groups:
        first, last = grp[0], grp[-1]
        end = (_dt.fromisoformat(last) + _td(minutes=30)).isoformat()
        exact_start = min((slots[k]["raw_start"] or k) for k in grp)
        exact_end = max(
            (slots[k]["raw_end"] or (_dt.fromisoformat(k) + _td(minutes=30)).isoformat())
            for k in grp)
        # Time actually spent charging = the merged UNION of each delivered slot's
        # PLANNED dispatch window, CLIPPED to that slot. Planned windows carry
        # Octopus's real sub-slot bounds (a dispatch that finished early reads
        # 04:30–04:48, not a padded 04:30–05:00; a 3-minute top-up reads 15:27–15:30)
        # where completedDispatches are always slot-aligned. Clipping to the slot
        # keeps a multi-hour plan from bleeding across a gap into an un-delivered
        # half-hour, and only DELIVERED slots are counted, so idle gaps between
        # bursts are excluded. Rate-limit-proof: never divides by energy. Falls back
        # to the completed/slot bounds when no planned window was captured.
        _ivs = []
        for k in grp:
            _ks = _dt.fromisoformat(k)
            _ke = _ks + _td(minutes=30)
            _a = _dt.fromisoformat(slots[k]["pstart"] or slots[k]["raw_start"] or k)
            _b = _dt.fromisoformat(
                slots[k]["pend"] or slots[k]["raw_end"] or _ke.isoformat())
            _a = max(_a, _ks)   # clip to this half-hour so a long plan can't bleed
            _b = min(_b, _ke)
            if _b > _a:
                _ivs.append((_a, _b))
        _ivs.sort()
        _merged = []
        for _a, _b in _ivs:
            if _merged and _a <= _merged[-1][1]:
                _merged[-1] = (_merged[-1][0], max(_merged[-1][1], _b))
            else:
                _merged.append((_a, _b))
        dispatch_minutes = round(
            sum((_b - _a).total_seconds() for _a, _b in _merged) / 60.0)
        kwh = op = pk = saving = 0.0
        started_slots = 0
        fill = []
        for k in grp:
            # Metered grid import for the half-hour (reconciles with the bill);
            # fall back to the dispatch figure when the block isn't settled yet.
            mv = slot_kwh.get(k)
            v = abs(mv) if mv is not None else abs(slots[k]["completed"])
            kwh += v
            peak_r = day_peak.get(k[:10])
            rate = rates.get(k)
            # Colour/label from the DISPATCH SEMANTICS where they're known, so a
            # just-completed smart charge reads off-peak IMMEDIATELY instead of
            # flashing peak until the overlay reprices it (the "red flash"). On IOG a
            # smart-charge dispatch is off-peak; a bump/boost bills at peak. When the
            # source is unknown (e.g. OHME, or a completed-only slot), fall back to the
            # settled block rate — which is authoritative once it lands.
            _src = (slots[k].get("source") or "").lower()
            if _src in _BUMP_SOURCES:            # TODO: or over-6h cap on the cap tariff
                is_op = False
            elif _src in _SMART_SOURCES:
                is_op = True
            else:
                is_op = _slot_is_off_peak(rate, peak_r)
            if is_op:
                op += v
                # Off-peak benefit vs the day's peak rate (firms up once settled).
                if peak_r is not None and rate is not None and peak_r > rate:
                    saving += (peak_r - rate) * v
            else:
                pk += v
            if slots[k]["started"]:
                started_slots += 1
            fill.append({"slot": k, "kwh": round(v, 3), "off_peak": is_op})
        # ≈charging ESTIMATE: infer the car's active power from the FULLEST
        # delivered half-hour (a fully-saturated slot ≈ true charge power) and
        # divide total delivered energy by it — the peak-slot form of energy÷power
        # (algebraically 30 × kWh ÷ max_slot_kWh). Validated 2026-08-07: 5 slots,
        # 11.41 kWh, peak 6.36 kW → 1.79 h vs a measured 1.77 h. This is a tighter
        # "how long did it actually charge" than the dispatched window; the two are
        # shown side by side (est ≤ dispatched always). Guards: need a real anchor
        # slot to infer power (else leave None — card shows only the window); clamp
        # inferred power to a plausible ceiling so a glitch slot can't crash the
        # time; never exceed the dispatched window.
        max_e = max((f["kwh"] for f in fill), default=0.0)
        charge_min = round(30.0 * kwh / max_e) if max_e > 1e-9 else 0
        _ANCHOR_KWH = 0.4    # ~0.8 kW half-hour — below this we can't infer power
        _MAX_KW = 25.0       # plausibility ceiling for a domestic charger
        active_kw = min(round(2.0 * max_e, 2), _MAX_KW)
        if max_e >= _ANCHOR_KWH and active_kw > 1e-9:
            est_charge_minutes = min(round(60.0 * kwh / active_kw), dispatch_minutes)
        else:
            active_kw = est_charge_minutes = None
        out.append({
            "start": first, "end": end,
            "exact_start": exact_start, "exact_end": exact_end,
            "kwh": round(kwh, 3), "n_slots": len(grp),
            "started_slots": started_slots, "status": "completed",
            "off_peak_kwh": round(op, 3), "peak_kwh": round(pk, 3),
            "charge_minutes": charge_min,
            "dispatch_minutes": dispatch_minutes,
            "est_charge_minutes": est_charge_minutes,
            "active_kw": active_kw,
            "saving": round(saving, 4) if saving > 1e-9 else None,
            "provider": slots[first]["provider"],
            "fill": fill,
        })
    out.reverse()   # newest-first
    return out


def _build_upcoming_dispatches(history, rates, day_peak, now_iso,
                               bridge_min=_CHARGE_SESSION_BRIDGE_MIN):
    """BL-10: fold *future planned* IOG dispatch slots into upcoming sessions.

    Complements _build_charge_sessions. A slot qualifies if it carries a planned
    forecast, has not yet completed, and has not fully elapsed (its 30-min end is
    at/after now_iso). Each slot is coloured by the tariff rate for that half-hour
    (`rates` / `day_peak`), so a plan landing in a peak half-hour reads peak.
    Consecutive slots ≤ bridge_min apart form one session. Returns sessions
    soonest-first, each with status 'scheduled' and saving None (a forecast).
    """
    from datetime import datetime as _dt, timedelta as _td
    now = _dt.fromisoformat(now_iso)
    slots = {}
    for r in history:
        s = slots.setdefault(r["slot_start"], {
            "planned": None, "completed": None, "planned_seen": None,
            "raw_start": None, "raw_end": None, "provider": r.get("provider")})
        k = r.get("kind")
        if k == "planned" and r.get("energy_kwh") is not None:
            s["planned"] = r.get("energy_kwh")
            if r.get("last_seen"):
                s["planned_seen"] = r["last_seen"]
        elif k == "completed" and r.get("energy_kwh") is not None:
            s["completed"] = r.get("energy_kwh")
        if r.get("raw_start"):
            s["raw_start"] = r["raw_start"]
        if r.get("raw_end"):
            s["raw_end"] = r["raw_end"]

    # Only the *current* plan is upcoming: keep planned slots re-seen in (or within
    # _UPCOMING_BATCH_SEC of) the freshest planned observation, so slots a revised
    # plan dropped don't linger and inflate the total. If the freshest plan itself
    # is older than _UPCOMING_STALE_MIN, there's no live plan → nothing upcoming.
    # When last_seen is absent (older data / unit tests), skip this filter.
    _seens = [v["planned_seen"] for v in slots.values()
              if v["planned"] is not None and v["planned_seen"]]
    _batch_cutoff = None
    if _seens:
        _newest = max(_dt.fromisoformat(s) for s in _seens)
        if (now - _newest) > _td(minutes=_UPCOMING_STALE_MIN):
            return []
        _batch_cutoff = _newest - _td(seconds=_UPCOMING_BATCH_SEC)

    def _is_current(v):
        if _batch_cutoff is None or not v["planned_seen"]:
            return True
        return _dt.fromisoformat(v["planned_seen"]) >= _batch_cutoff

    upcoming = sorted(
        k for k, v in slots.items()
        if v["planned"] is not None and v["completed"] is None
        and _dt.fromisoformat(k) + _td(minutes=30) > now
        and _is_current(v))
    if not upcoming:
        return []

    groups, cur, prev = [], [], None
    for k in upcoming:
        dt = _dt.fromisoformat(k)
        if prev is not None and (dt - prev) > _td(minutes=bridge_min):
            groups.append(cur); cur = []
        cur.append(k); prev = dt
    if cur:
        groups.append(cur)

    out = []
    for grp in groups:
        first, last = grp[0], grp[-1]
        end = (_dt.fromisoformat(last) + _td(minutes=30)).isoformat()
        exact_start = min((slots[k]["raw_start"] or k) for k in grp)
        exact_end = max(
            (slots[k]["raw_end"] or (_dt.fromisoformat(k) + _td(minutes=30)).isoformat())
            for k in grp)
        kwh = op = pk = 0.0
        fill = []
        for k in grp:
            v = abs(slots[k]["planned"])
            kwh += v
            is_op = _slot_is_off_peak(rates.get(k), day_peak.get(k[:10]))
            if is_op:
                op += v
            else:
                pk += v
            fill.append({"slot": k, "kwh": round(v, 3), "off_peak": is_op})
        max_e = max((f["kwh"] for f in fill), default=0.0)
        charge_min = round(30.0 * kwh / max_e) if max_e > 1e-9 else 0
        out.append({
            "start": first, "end": end,
            "exact_start": exact_start, "exact_end": exact_end,
            "kwh": round(kwh, 3), "n_slots": len(grp),
            "started_slots": 0, "status": "scheduled",
            "off_peak_kwh": round(op, 3), "peak_kwh": round(pk, 3),
            "charge_minutes": charge_min,
            "saving": None, "provider": slots[first]["provider"],
            "fill": fill,
        })
    return out   # soonest-first


@app.route("/api/charge-sessions", methods=["GET"])
def api_charge_sessions():
    """BL-10: smart-charge sessions from dispatch_history for the Overview card.

    Gated on there being dispatch data (IOG). Query `days` (default 14) sets the
    lookback. Returns {sessions, upcoming, has_data, currency}. `sessions` are
    delivered charges (newest-first); `upcoming` are future planned dispatches
    (soonest-first). Each carries kWh, local windows, and a per-slot fill curve.
    """
    try:
        from zoneinfo import ZoneInfo
        from datetime import datetime as _d2, timedelta as _t2, timezone as _tzc
        store = _get_store()
        try:
            days = max(1, min(int(request.args.get("days", 14)), 90))
        except (TypeError, ValueError):
            days = 14

        cfg = load_config()
        main_meta, main_id = {}, None
        for mid, md in cfg.get("meters", {}).items():
            if not (md.get("meta") or {}).get("sub_meter"):
                main_meta, main_id = md.get("meta") or {}, mid
                break
        main_id = main_id or "electricity_main"
        tz_name = main_meta.get("timezone", "UTC")
        currency = main_meta.get("currency_symbol", "£")

        now = _d2.now(_tzc.utc).replace(tzinfo=None)
        win_start = (now - _t2(days=days)).isoformat()
        win_end = (now + _t2(days=2)).isoformat()   # +2d captures tonight's plan

        history = store.get_dispatch_history(win_start, win_end)
        if not history:
            return jsonify({"sessions": [], "upcoming": [],
                            "has_data": False, "currency": currency})

        # Energy source = the EV's charge, from the supplier's dispatch figure —
        # what the car actually charged, without the house baseload the meter's
        # own import carries. (A future EV sub-meter could supply a bill-
        # reconciled per-slot figure via `slot_kwh`; left None here.) `rates` /
        # `day_peak` still come from the billing blocks so pricing/colouring
        # match the bill.
        rates = {r["block_start"]: r["imp_rate"] for r in store._conn.execute(
            "SELECT block_start, imp_rate FROM blocks WHERE meter_id = ? "
            "AND block_start >= ? AND block_start < ? AND imp_rate IS NOT NULL",
            (main_id, win_start, win_end))}
        day_peak = {r["d"]: r["p"] for r in store._conn.execute(
            "SELECT substr(block_start,1,10) d, MAX(imp_rate) p FROM blocks "
            "WHERE meter_id = ? AND block_start >= ? AND block_start < ? "
            "AND imp_rate IS NOT NULL GROUP BY d", (main_id, win_start, win_end))}

        sessions = _build_charge_sessions(history, rates, day_peak)

        # Attach local-time display strings (UTC→site tz), like the review list.
        try:
            _tz = ZoneInfo(tz_name)
        except Exception:
            _tz = ZoneInfo("UTC")

        def _loc(iso):
            try:
                d = _d2.fromisoformat(iso).replace(tzinfo=ZoneInfo("UTC")).astimezone(_tz)
                return {"date": d.strftime("%Y-%m-%d"), "time": d.strftime("%H:%M")}
            except Exception:
                return {"date": None, "time": None}

        upcoming = _build_upcoming_dispatches(history, rates, day_peak, now.isoformat())

        for s in sessions + upcoming:
            s["local"] = {
                "start": _loc(s["exact_start"]), "end": _loc(s["exact_end"])}
            # Localise each per-slot label too (UTC->site tz), so the fill-chart bar
            # times match the summary window instead of reading an hour off under BST.
            for f in s.get("fill", []):
                f["local_time"] = _loc(f["slot"])["time"]

        return jsonify({"sessions": sessions, "upcoming": upcoming,
                        "has_data": True, "currency": currency})
    except Exception as e:
        logger.error("api_charge_sessions: %s", e)
        return jsonify({"error": str(e)}), 500


@app.route("/api/corrections/meters", methods=["GET"])
def api_corrections_meters():
    """Return distinct meter_ids present in the blocks table for the corrections UI."""
    try:
        store = _get_store()
        rows = store._conn.execute(
            "SELECT DISTINCT meter_id FROM blocks ORDER BY meter_id"
        ).fetchall()
        return jsonify({"meters": [r["meter_id"] for r in rows]})
    except Exception as e:
        logger.error("api_corrections_meters: %s", e)
        return jsonify({"error": str(e)}), 500


# Days of blocks either side of the correction window to sample rates from.
_NEARBY_RATES_PAD_DAYS = 3


@app.route("/api/corrections/nearby-rates", methods=["GET"])
def api_corrections_nearby_rates():
    """#270: the distinct main-meter rates in force in blocks SURROUNDING a
    correction window, so the user can pick the exact stored value instead of
    re-typing it (avoiding fat-finger / decimal-place / rounding errors).

    Query: channel=import|export, from_date=YYYY-MM-DD, to_date=YYYY-MM-DD.
    Samples blocks in [from_date - N days, to_date + N days] (N = 3), grouped by
    the exact stored rate, ranked by how many blocks carry it (most common first)
    so the everyday off-peak/peak values surface at the top. Full precision — the
    UI fills the value verbatim.
    """
    try:
        channel   = request.args.get("channel", "import")
        from_date = request.args.get("from_date", "")
        to_date   = request.args.get("to_date", "")
        if channel not in ("import", "export"):
            return jsonify({"error": "channel must be 'import' or 'export'"}), 400
        if not from_date or not to_date:
            return jsonify({"error": "from_date and to_date required"}), 400

        from datetime import date as _date, timedelta as _td
        from block_store import local_date_range_to_utc_bounds as _utc_bounds

        store = _get_store()
        cfg = load_config()
        main_meta, main_id = {}, None
        for mid, md in cfg.get("meters", {}).items():
            if not (md.get("meta") or {}).get("sub_meter"):
                main_meta, main_id = md.get("meta") or {}, mid
                break
        if main_id is None:
            row = store._conn.execute(
                "SELECT meter_id FROM meters WHERE is_sub_meter = 0 "
                "ORDER BY meter_id LIMIT 1").fetchone()
            main_id = row["meter_id"] if row else "electricity_main"
        tz_name = main_meta.get("timezone", "UTC")

        # Pad the window by a few days each side to sample surrounding rates.
        pad = _td(days=_NEARBY_RATES_PAD_DAYS)
        win_from = (_date.fromisoformat(from_date) - pad).isoformat()
        win_to   = (_date.fromisoformat(to_date)   + pad).isoformat()
        utc_start, utc_end = _utc_bounds(win_from, win_to, tz_name)

        rate_col = "imp_rate" if channel == "import" else "exp_rate"
        rows = store._conn.execute(
            f"""SELECT {rate_col} AS rate, COUNT(*) AS n
                FROM blocks
                WHERE meter_id = ? AND block_start >= ? AND block_start < ?
                  AND {rate_col} IS NOT NULL AND {rate_col} > 0
                GROUP BY {rate_col}
                ORDER BY n DESC, {rate_col}""",
            (main_id, utc_start, utc_end)).fetchall()
        rates = [{"rate": r["rate"], "count": r["n"]} for r in rows]
        return jsonify({"rates": rates, "channel": channel,
                        "window": [win_from, win_to]})
    except Exception as e:
        logger.error("api_corrections_nearby_rates: %s", e)
        return jsonify({"error": str(e)}), 500


@app.route("/api/review-blocks", methods=["GET"])
def api_review_blocks():
    """BL-18: blocks flagged needs_review = 1 (ambiguous dispatch reconcile, or
    CAD/DCC settlement drift), for the 'Flagged for review' list on Corrections.
    Each entry carries block_start, meter_id, a human reason, and the kWh figures.
    """
    try:
        store = _get_store()
        alerts = store.get_review_blocks()
        # Enrich each block with the LOCAL date + 30-min window so the UI can
        # pre-fill the correction tool exactly (block_start is naive UTC). Doing
        # the UTC→local conversion server-side with the configured timezone
        # guarantees it round-trips through the local→UTC apply path.
        from zoneinfo import ZoneInfo
        from datetime import datetime as _dt, timedelta as _td
        cfg = load_config()
        main_meta = {}
        for md in cfg.get("meters", {}).values():
            if not (md.get("meta") or {}).get("sub_meter"):
                main_meta = md.get("meta") or {}
                break
        tz_name = main_meta.get("timezone", "UTC")
        block_minutes = int(main_meta.get("block_minutes", 30) or 30)
        try:
            _tz = ZoneInfo(tz_name)
        except Exception:
            _tz = ZoneInfo("UTC")
        for a in alerts:
            try:
                start_utc = _dt.fromisoformat(a["block_start"]).replace(tzinfo=ZoneInfo("UTC"))
                start_loc = start_utc.astimezone(_tz)
                end_loc = start_loc + _td(minutes=block_minutes)
                a["local_date"] = start_loc.strftime("%Y-%m-%d")
                a["from_time"] = start_loc.strftime("%H:%M")
                a["to_time"] = end_loc.strftime("%H:%M")
            except Exception:
                a["local_date"] = a["from_time"] = a["to_time"] = None
        return jsonify({"blocks": alerts, "count": len(alerts)})
    except Exception as e:
        logger.error("api_review_blocks: %s", e)
        return jsonify({"error": str(e)}), 500


@app.route("/api/review-blocks/dismiss", methods=["POST"])
def api_review_blocks_dismiss():
    """BL-18: dismiss review flags. Body {block_ids: [...]} clears those blocks;
    omit block_ids (or send null) to clear all. Dismissing only removes the flag
    — it never changes a billing figure.
    """
    try:
        data = request.get_json(force=True) or {}
        block_ids = data.get("block_ids")
        if block_ids is not None and not isinstance(block_ids, list):
            return jsonify({"error": "block_ids must be a list or omitted"}), 400
        store = _get_store()
        cleared = store.dismiss_review_blocks(block_ids)
        logger.info("api_review_blocks_dismiss: cleared %d review flag(s)", cleared)
        return jsonify({"cleared": cleared})
    except Exception as e:
        logger.error("api_review_blocks_dismiss: %s", e)
        return jsonify({"error": str(e)}), 500


@app.route("/api/corrections/preview", methods=["POST"])
def api_corrections_preview():
    """
    Preview what a correction would affect — returns per-block detail for rate
    corrections so the user can verify exactly which blocks will change.

    Body: {
      type: "standing"|"rate",
      from_date: "YYYY-MM-DD", to_date: "YYYY-MM-DD",
      value: float,
      channel: "import"|"export"  (rate only),
      from_time: "HH:MM"          (optional local time — start of window),
      to_time:   "HH:MM"          (optional local time — end of window, exclusive),
      meter_id:  "all"|"<meter>"  (optional — default "all")
    }

    Returns for standing: { days, blocks, current_min, current_max }
    Returns for rate:     { blocks: [{block_start, meter_id, current_rate,
                                       new_rate, kwh, current_cost, new_cost}] }
    """
    try:
        data       = request.get_json(force=True) or {}
        corr_type  = data.get("type")
        from_date  = data.get("from_date", "")
        to_date    = data.get("to_date", "")
        channel    = data.get("channel", "import")
        from_time  = data.get("from_time", "")   # local HH:MM
        to_time    = data.get("to_time", "")     # local HH:MM
        meter_id   = data.get("meter_id", "all")
        value      = data.get("value")

        if corr_type not in ("standing", "rate"):
            return jsonify({"error": "type must be 'standing' or 'rate'"}), 400
        if not from_date or not to_date:
            return jsonify({"error": "from_date and to_date required"}), 400

        store = _get_store()
        cfg   = load_config()
        main_meta = {}
        for md in cfg.get("meters", {}).values():
            if not (md.get("meta") or {}).get("sub_meter"):
                main_meta = md.get("meta") or {}
                break
        tz_name = main_meta.get("timezone", "UTC")

        # Use from_date as DST anchor so March/BST offsets are correct
        from_time_utc = _corrections_time_to_utc(from_time, tz_name, from_date) if from_time else ""
        to_time_utc   = _corrections_time_to_utc(to_time,   tz_name, from_date) if to_time   else ""

        where, params, col = _corrections_build_where(
            corr_type, from_date, to_date, channel,
            from_time_utc, to_time_utc, meter_id, tz_name
        )

        # API+ gate: rate corrections only touch DCC-settled blocks (see
        # _corrections_build_where). Pure CAD applies immediately.
        gate = corr_type == "rate" and _corrections_api_gate_active()
        skipped = 0
        if gate:
            where, params, col = _corrections_build_where(
                corr_type, from_date, to_date, channel,
                from_time_utc, to_time_utc, meter_id, tz_name,
                api_settled_only=True
            )
            skipped = _corrections_unreconciled_count(
                store, from_date, to_date, channel,
                from_time_utc, to_time_utc, meter_id, tz_name)

        if corr_type == "standing":
            cur = store._conn.execute(
                f"""SELECT COUNT(DISTINCT date(block_start)) as days,
                          COUNT(*) as blocks,
                          MIN(standing_charge) as cur_min,
                          MAX(standing_charge) as cur_max
                   FROM blocks WHERE {where}""",
                params
            )
            row = cur.fetchone()
            return jsonify({
                "days":        row["days"]    or 0,
                "blocks":      row["blocks"]  or 0,
                "current_min": round(float(row["cur_min"] or 0), 6),
                "current_max": round(float(row["cur_max"] or 0), 6),
            })

        else:  # rate — per-block detail; device rows follow the main (issue #254b)
            from zoneinfo import ZoneInfo
            from datetime import datetime, timezone as _tz
            tz = ZoneInfo(tz_name)
            kwh_col  = "imp_kwh"  if channel == "import" else "exp_kwh"
            cost_col = "imp_cost" if channel == "import" else "exp_cost"

            def _display(bs):
                try:
                    return (datetime.fromisoformat(bs).replace(tzinfo=_tz.utc)
                            .astimezone(tz).strftime("%d/%m %H:%M"))
                except Exception:
                    return bs

            def _row(bs, mid, cur_rate, kwh, cur_cost):
                nc = round(float(kwh or 0) * float(value), 6) if value is not None else None
                return {"block_start": bs, "display": _display(bs), "meter_id": mid,
                        "current_rate": round(float(cur_rate or 0), 6),
                        "new_rate": float(value) if value is not None else None,
                        "kwh": round(float(kwh or 0), 6),
                        "current_cost": round(float(cur_cost or 0), 6), "new_cost": nc}

            blocks_out = []
            # MAIN blocks (settled gate applied in `where`) — cost basis = imp_kwh
            main_rows = store._conn.execute(
                f"""SELECT block_start, meter_id, {col} as current_rate,
                           {kwh_col} as kwh, {cost_col} as current_cost
                    FROM blocks WHERE {where} ORDER BY block_start, meter_id""",
                params).fetchall()
            for r in main_rows:
                blocks_out.append(_row(r["block_start"], r["meter_id"],
                                       r["current_rate"], r["kwh"], r["current_cost"]))

            # DEVICE rows that will follow the correction (import only). Cost basis
            # is the grid-attributed kWh (imp_kwh_grid), exactly what PASS 2 uses,
            # so the preview matches what apply writes.
            if channel == "import" and main_rows:
                bs_list = [r["block_start"] for r in main_rows]
                ph = ",".join("?" * len(bs_list))
                dev_rows = store._conn.execute(
                    f"""SELECT block_start, meter_id, imp_rate as current_rate,
                               COALESCE(imp_kwh_grid, imp_kwh) as kwh, imp_cost as current_cost
                        FROM blocks
                        WHERE block_start IN ({ph}) AND imp_rate IS NOT NULL
                          AND meter_id IN (SELECT meter_id FROM meters WHERE is_sub_meter = 1)
                        ORDER BY block_start, meter_id""", bs_list).fetchall()
                for r in dev_rows:
                    blocks_out.append(_row(r["block_start"], r["meter_id"],
                                           r["current_rate"], r["kwh"], r["current_cost"]))

            blocks_out.sort(key=lambda b: (b["block_start"], b["meter_id"]))
            return jsonify({"blocks": blocks_out,
                            "skipped_unreconciled": skipped,
                            "api_gated": gate})

    except Exception as e:
        logger.error("api_corrections_preview: %s", e)
        return jsonify({"error": str(e)}), 500


@app.route("/api/corrections/apply", methods=["POST"])
def api_corrections_apply():
    """
    Apply a standing-charge or rate correction to the live database.
    Body: {
      type: "standing"|"rate",
      from_date: "YYYY-MM-DD", to_date: "YYYY-MM-DD",
      value: float,
      channel: "import"|"export"  (rate only),
      recalc_cost: bool           (rate only — recalculate cost from rate × kwh),
      from_time: "HH:MM"          (optional local time — start of window),
      to_time:   "HH:MM"          (optional local time — end of window, exclusive),
      meter_id:  "all"|"<meter>"  (optional — default "all")
    }
    Returns: { updated_blocks: int }
    """
    try:
        data        = request.get_json(force=True) or {}
        corr_type   = data.get("type")
        from_date   = data.get("from_date", "")
        to_date     = data.get("to_date", "")
        value       = data.get("value")
        channel     = data.get("channel", "import")
        recalc_cost = bool(data.get("recalc_cost", True))
        from_time   = data.get("from_time", "")
        to_time     = data.get("to_time", "")
        meter_id    = data.get("meter_id", "all")

        if corr_type not in ("standing", "rate"):
            return jsonify({"error": "type must be 'standing' or 'rate'"}), 400
        if not from_date or not to_date:
            return jsonify({"error": "from_date and to_date required"}), 400
        if value is None:
            return jsonify({"error": "value required"}), 400

        value = float(value)
        if value < 0:
            return jsonify({"error": "value must be >= 0"}), 400

        store = _get_store()
        cfg   = load_config()
        main_meta = {}
        for md in cfg.get("meters", {}).values():
            if not (md.get("meta") or {}).get("sub_meter"):
                main_meta = md.get("meta") or {}
                break
        tz_name = main_meta.get("timezone", "UTC")

        # Use from_date as DST anchor so March/BST offsets are correct
        from_time_utc = _corrections_time_to_utc(from_time, tz_name, from_date) if from_time else ""
        to_time_utc   = _corrections_time_to_utc(to_time,   tz_name, from_date) if to_time   else ""

        where, params, col = _corrections_build_where(
            corr_type, from_date, to_date, channel,
            from_time_utc, to_time_utc, meter_id, tz_name
        )

        # API+ gate: a rate correction only touches DCC-settled blocks; unsettled
        # ones would be clobbered at settlement, so skip + report them. Pure CAD
        # applies immediately.
        gate = corr_type == "rate" and _corrections_api_gate_active()
        skipped = 0
        if gate:
            where, params, col = _corrections_build_where(
                corr_type, from_date, to_date, channel,
                from_time_utc, to_time_utc, meter_id, tz_name,
                api_settled_only=True
            )
            skipped = _corrections_unreconciled_count(
                store, from_date, to_date, channel,
                from_time_utc, to_time_utc, meter_id, tz_name)

        if corr_type == "standing":
            cur = store._conn.execute(
                f"UPDATE blocks SET standing_charge = ? WHERE {where}",
                [value] + params
            )
            store._conn.commit()
            updated = cur.rowcount
            logger.info(
                "api_corrections_apply: standing_charge=%.4f %d blocks (%s %s→%s %s→%s)",
                value, updated, meter_id, from_date, to_date,
                from_time or "00:00", to_time or "24:00"
            )

        else:  # rate correction
            kwh_col = "imp_kwh" if channel == "import" else "exp_kwh"
            # Stamp a durable marker on manual RATE corrections so the dispatch
            # reconciliation pass never overwrites a user's override. A manual
            # correction is also a definite human decision, so it clears any
            # ambiguous-review flag on the block (BL-18).
            _mark = (", rate_corrected = 1, needs_review = 0, review_reason = NULL"
                     if col in ("imp_rate", "exp_rate") else "")
            if recalc_cost:
                cost_col = "imp_cost" if channel == "import" else "exp_cost"
                cur = store._conn.execute(
                    f"""UPDATE blocks
                        SET {col} = ?,
                            {cost_col} = ROUND({kwh_col} * ?, 6){_mark}
                        WHERE {where}""",
                    [value, value] + params
                )
            else:
                cur = store._conn.execute(
                    f"UPDATE blocks SET {col} = ?{_mark} WHERE {where}",
                    [value] + params
                )
            store._conn.commit()
            updated = cur.rowcount

            # Devices follow the main meter's rate, so propagate the corrected
            # IMPORT rate onto device rows for the same blocks (issue #254b). The
            # settled gate above only matches the main meter — device rows never
            # carry imp_kwh_api — so without this the correction silently skipped
            # them. Device cost = grid-attributed kWh × rate, exactly what PASS 2
            # computes at finalise.
            if channel == "import" and updated:
                bs_list = [r["block_start"] for r in store._conn.execute(
                    f"SELECT DISTINCT block_start FROM blocks WHERE {where}", params)]
                if bs_list:
                    ph = ",".join("?" * len(bs_list))
                    dev = (f"block_start IN ({ph}) AND meter_id IN "
                           "(SELECT meter_id FROM meters WHERE is_sub_meter = 1)")
                    if recalc_cost:
                        store._conn.execute(
                            f"UPDATE blocks SET imp_rate = ?, "
                            f"imp_cost = ROUND(COALESCE(imp_kwh_grid, imp_kwh, 0) * ?, 6) "
                            f"WHERE {dev}", [value, value] + bs_list)
                    else:
                        store._conn.execute(
                            f"UPDATE blocks SET imp_rate = ? WHERE {dev}",
                            [value] + bs_list)
                    store._conn.commit()

            logger.info(
                "api_corrections_apply: %s %s_rate=%.6f recalc=%s %d main block(s) "
                "(%s→%s %s→%s); devices follow",
                meter_id, channel, value, recalc_cost, updated,
                from_date, to_date, from_time or "00:00", to_time or "24:00"
            )

        # Regenerate the pre-built billing/heatmap charts so the correction is
        # visible immediately, rather than only after the next block finalises
        # (issue #254a). Client-rendered charts (usage stats, spiral) re-fetch.
        if updated:
            _regen_charts_safely()

        return jsonify({"ok": True, "updated_blocks": updated,
                        "skipped_unreconciled": skipped, "api_gated": gate})
    except Exception as e:
        logger.error("api_corrections_apply: %s", e)
        return jsonify({"error": str(e)}), 500