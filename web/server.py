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

import json
import logging
import os
import threading
from pathlib import Path

from flask import Flask, Response, jsonify, redirect, render_template, request, send_file, url_for

logger = logging.getLogger("server")

app = Flask(__name__, template_folder="templates")


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


@app.context_processor
def inject_globals():
    has_power_sensor = False
    has_postcode = False
    try:
        from energy_engine_io import load_json as _lj
        cfg = _lj(os.path.join(DATA_DIR, "meters_config.json"), {})
        for m_data in cfg.get("meters", {}).values():
            if not (m_data.get("meta") or {}).get("sub_meter"):
                has_power_sensor = bool((m_data.get("meta") or {}).get("power_sensor"))
                has_postcode     = bool((m_data.get("meta") or {}).get("postcode_prefix", "").strip())
                break
    except Exception:
        pass
    return {"app_version": APP_VERSION, "has_power_sensor": has_power_sensor, "has_postcode": has_postcode}


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
SHARE_BACKUP_DIR = (
    _os.path.join("/data/energy_meter_tracker", "backup")
    if _os.environ.get("EMT_MODE") == "standalone"
    else "/share/energy_meter_tracker_backup"
)
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


def init(data_dir: str, chart_dir: str, ha_client):
    global DATA_DIR, CHART_DIR, _ha_client, _event_loop
    import asyncio as _asyncio
    _event_loop = _asyncio.get_event_loop()
    DATA_DIR   = data_dir
    CHART_DIR  = chart_dir
    _ha_client = ha_client


def start():
    """Start Flask in a background daemon thread."""
    t = threading.Thread(target=_run, daemon=True, name="flask")
    t.start()
    logger.info("server: Flask started on port 8099")


def _run():
    from waitress import serve
    serve(app, host="0.0.0.0", port=8099, threads=4)


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
    p = config_path()
    if not os.path.exists(p):
        return {"schema_version": "1.0", "meters": {}}
    with open(p) as f:
        return json.load(f)


def save_config(data: dict):
    p = config_path()
    os.makedirs(os.path.dirname(p), exist_ok=True)

    # Preserve channel meta from existing config that the UI doesn't manage
    if os.path.exists(p):
        try:
            with open(p) as f:
                existing = json.load(f)
            for meter_id, meter in existing.get("meters", {}).items():
                for ch_id, ch in meter.get("channels", {}).items():
                    if "meta" in ch:
                        try:
                            data["meters"][meter_id]["channels"][ch_id].setdefault("meta", ch["meta"])
                        except (KeyError, TypeError):
                            pass
        except Exception:
            pass

    tmp = p + ".tmp"
    with open(tmp, "w") as f:
        json.dump(data, f, indent=2)
    os.replace(tmp, p)


# ── Pages ─────────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    cfg = load_config()
    if cfg.get("meters"):
        last = request.cookies.get("emt_last_page", "charts")
        valid = {"charts", "summary", "import", "logs", "help", "config"}
        if last not in valid:
            last = "charts"
        return redirect(url_for(last + "_page"))
    return redirect(url_for("config_page"))


@app.route("/api/last-page", methods=["POST"])
def api_set_last_page():
    page = request.get_json(force=True).get("page", "charts")
    valid = {"charts", "summary", "import", "logs", "help", "config"}
    if page not in valid:
        page = "charts"
    resp = jsonify({"ok": True})
    resp.set_cookie("emt_last_page", page, max_age=60*60*24*365, samesite="Lax")
    return resp


@app.route("/config")
def config_page():
    cfg = load_config()
    try:
        has_data = _get_store().count_blocks() > 0
    except Exception:
        has_data = False
    tz_select_html = '<select class="js-meta" data-key="timezone"><option value="UTC">UTC</option><option value="Europe/London">Europe/London (UK)</option><option value="Europe/Dublin">Europe/Dublin (Ireland)</option><option value="Europe/Lisbon">Europe/Lisbon (Portugal)</option><option value="Europe/Paris">Europe/Paris (France, Belgium, Netherlands)</option><option value="Europe/Berlin">Europe/Berlin (Germany, Austria)</option><option value="Europe/Amsterdam">Europe/Amsterdam</option><option value="Europe/Rome">Europe/Rome (Italy)</option><option value="Europe/Madrid">Europe/Madrid (Spain)</option><option value="Europe/Stockholm">Europe/Stockholm (Sweden, Norway, Denmark)</option><option value="Europe/Helsinki">Europe/Helsinki (Finland)</option><option value="Europe/Warsaw">Europe/Warsaw (Poland)</option><option value="Europe/Athens">Europe/Athens (Greece)</option><option value="Europe/Istanbul">Europe/Istanbul (Turkey)</option><option value="Europe/Moscow">Europe/Moscow (Russia)</option><option value="America/New_York">America/New_York (US Eastern)</option><option value="America/Chicago">America/Chicago (US Central)</option><option value="America/Denver">America/Denver (US Mountain)</option><option value="America/Los_Angeles">America/Los_Angeles (US Pacific)</option><option value="America/Toronto">America/Toronto (Canada Eastern)</option><option value="America/Vancouver">America/Vancouver (Canada Pacific)</option><option value="America/Sao_Paulo">America/Sao_Paulo (Brazil)</option><option value="Asia/Dubai">Asia/Dubai (UAE)</option><option value="Asia/Kolkata">Asia/Kolkata (India)</option><option value="Asia/Singapore">Asia/Singapore</option><option value="Asia/Tokyo">Asia/Tokyo (Japan)</option><option value="Asia/Shanghai">Asia/Shanghai (China)</option><option value="Australia/Sydney">Australia/Sydney</option><option value="Australia/Perth">Australia/Perth</option><option value="Pacific/Auckland">Pacific/Auckland (New Zealand)</option></select>'
    return render_template("config.html", config=cfg, active="config", tz_select_html=tz_select_html, has_data=has_data)


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
    # Grid remainder
    if main_imp_kwh > 0.0001 or main_imp_cost > 0.0001:
        rows.append({"label": f"Grid Import ({main_imp_kwh:.3f} kWh)", "cost": main_imp_cost})
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


# Cache for gauge scale — recomputed at most every 30 minutes
_gauge_cache = {"ts": None, "max_imp": 10, "max_exp": 5, "rate_low": 0.10, "rate_high": 0.25}

@app.route("/summary")
def summary_page():
    from datetime import datetime
    from zoneinfo import ZoneInfo
    from energy_engine_io import load_json as _load_json

    try:
        cfg      = _load_json(os.path.join(DATA_DIR, "meters_config.json"), {})
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

        # ── Gauge scale — use cache, recompute every 30 minutes ──
        from datetime import timedelta
        import time as _time
        block_minutes = int(main_meta.get("block_minutes") or 30)
        now_ts = _time.time()
        if _gauge_cache["ts"] is None or now_ts - _gauge_cache["ts"] > 1800:
            try:
                hours_per_block = block_minutes / 60.0
                cutoff_dt = now_local.replace(tzinfo=None) - timedelta(days=7)
                now_naive2 = now_local.replace(tzinfo=None)
                imp_kw_vals, exp_kw_vals, rate_vals = [], [], []
                for b in store.get_blocks_for_range(cutoff_dt, now_naive2):
                    if not b or not b.get("start"): continue
                    try:
                        for m_id, m_data in (b.get("meters") or {}).items():
                            meta = (m_data or {}).get("meta", {}) or {}
                            if meta.get("sub_meter"): continue
                            ch_imp = (m_data.get("channels", {}).get("import") or {})
                            ch_exp = (m_data.get("channels", {}).get("export") or {})
                            imp_kwh = float(ch_imp.get("kwh_remainder") or ch_imp.get("kwh") or 0)
                            exp_kwh = float(ch_exp.get("kwh") or 0)
                            if imp_kwh > 0: imp_kw_vals.append(imp_kwh / hours_per_block)
                            if exp_kwh > 0: exp_kw_vals.append(exp_kwh / hours_per_block)
                            rate = float(ch_imp.get("rate_used") or ch_imp.get("rate") or 0)
                            if rate > 0: rate_vals.append(rate)
                    except Exception: continue
                def _pct(vals, p):
                    if not vals: return None
                    s = sorted(vals); return s[min(int(len(s)*p/100), len(s)-1)]
                def _nc(kw):
                    for s in [1,2,3,5,7,10,15,20,30,50]:
                        if s >= kw: return s
                    return round(kw*1.2)
                _gauge_cache.update({
                    "ts": now_ts,
                    "max_imp": _nc(_pct(imp_kw_vals, 95) or 3.0),
                    "max_exp": _nc(_pct(exp_kw_vals, 95) or 3.0),
                    "rate_low":  _pct(rate_vals, 33) or 0.10,
                    "rate_high": _pct(rate_vals, 67) or 0.25,
                })
            except Exception as _ge:
                logger.warning("summary_page: gauge cache update failed: %s", _ge)

        gauge_max_imp = _gauge_cache["max_imp"]
        gauge_max_exp = _gauge_cache["max_exp"]
        gauge_max     = gauge_max_imp
        rate_low      = _gauge_cache["rate_low"]
        rate_high     = _gauge_cache["rate_high"]

    except Exception as e:
        logger.error("summary_page: %s", e)
        currency = "£"
        month_total = today_total = year_total = None
        month_rows = today_rows = year_rows = []
        month_period = year_period = today_date = ""
        gauge_max = gauge_max_imp = 10; gauge_max_exp = 5; rate_low = 0.10; rate_high = 0.25

    # Check if power sensor is configured
    has_power_sensor = bool(main_meta.get("power_sensor"))

    return render_template(
        "summary.html",
        active="summary",
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
        gauge_max_imp=gauge_max_imp,
        gauge_max_exp=gauge_max_exp,
        rate_low=rate_low,
        rate_high=rate_high,
        block_minutes=int(main_meta.get("block_minutes") or 30),
    )


@app.route("/api/power")
def api_power():
    """Returns live power (kW) from configured power sensor or derived from reads."""
    try:
        from energy_engine_io import load_json as _lj
        from datetime import datetime
        cfg        = _lj(os.path.join(DATA_DIR, "meters_config.json"), {})
        block      = _lj(os.path.join(DATA_DIR, "current_block.json"), {})
        meters_cfg = cfg.get("meters", {}) or {}
        meters_blk = block.get("meters", {}) or {}

        # Find power sensor and sub-meter sensors from config
        power_sensor = bat_sensor = ev_sensor = None
        for m_id, m_data in meters_cfg.items():
            meta = (m_data or {}).get("meta", {}) or {}
            if not meta.get("sub_meter"):
                power_sensor = meta.get("power_sensor")
            elif "battery" in m_id.lower() or "solax" in m_id.lower():
                bat_sensor = ((m_data.get("channels") or {}).get("import") or {}).get("read")
            elif "ev" in m_id.lower() or "zappi" in m_id.lower():
                ev_sensor = ((m_data.get("channels") or {}).get("import") or {}).get("read")

        def sensor_kw(entity_id):
            if not entity_id or not _ha_client:
                return None
            val = _ha_client.get_state(entity_id)
            if val in (None, "unknown", "unavailable"):
                return None
            try:
                return round(float(val), 3)  # already in kW
            except (ValueError, TypeError):
                return None

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
            meta   = m_data.get("meta", {}) or {}
            ch     = m_data.get("channels", {}) or {}
            if not meta.get("sub_meter", False):
                continue
            if "battery" in m_id.lower() or "solax" in m_id.lower() or "inverter" in m_id.lower():
                bat_kw = derive_kw(ch.get("import", {}).get("reads", []))
            elif "ev" in m_id.lower() or "zappi" in m_id.lower() or "charger" in m_id.lower():
                ev_kw = derive_kw(ch.get("import", {}).get("reads", []))

        if power_sensor:
            # Main meter — use direct power sensor (already in kW, +ve import, -ve export)
            net_kw = sensor_kw(power_sensor)
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
            _cfg2 = _lj2(os.path.join(DATA_DIR, "meters_config.json"), {})
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

        return jsonify({
            "import_kw":        imp_kw,
            "export_kw":        exp_kw,
            "battery_kw":       bat_kw,
            "ev_kw":            ev_kw,
            "max_kw":           10,
            "has_power_sensor": bool(power_sensor),
            "rate":             current_rate,
        })
    except Exception as e:
        logger.error("api_power: %s", e)
        return jsonify({"error": str(e)}), 500


@app.route("/api/billing")
def api_billing():
    """Return billing totals for Today, This Bill and This Year using fast SQL aggregation."""
    from datetime import datetime, timedelta
    from zoneinfo import ZoneInfo
    import energy_charts as _ec
    try:
        from energy_engine_io import load_json as _lj
        cfg      = _lj(os.path.join(DATA_DIR, "meters_config.json"), {})
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

        def _fmt_total(totals, label_imp, label_exp):
            """Format SQL totals into billing card total + rows."""
            imp_cost = totals["imp_cost"]
            exp_cost = totals["exp_cost"]
            standing = totals["standing"]
            total    = round(imp_cost + standing - exp_cost, 2)
            rows = []
            if totals["imp_kwh"] > 0.001 or imp_cost > 0.001:
                rows.append({"label": f"{label_imp} ({totals['imp_kwh']:.3f} kWh)",
                             "cost": imp_cost, "bold": True})
                rows.append({"label": f"Grid Import ({totals['imp_kwh']:.3f} kWh)",
                             "cost": imp_cost, "bold": False})
            if totals["exp_kwh"] > 0.001:
                rows.append({"label": f"Grid Export ({totals['exp_kwh']:.3f} kWh)",
                             "cost": -exp_cost, "bold": False})
            if standing > 0.001:
                rows.append({"label": "Standing Charge", "cost": standing, "bold": False})
            return total, rows

        # Use local_date-based queries throughout — correctly handles BST blocks
        # at 23:xx UTC that belong to the next local calendar day.
        today_local_date = now_local.date().isoformat()

        # Today
        today_t = store.get_billing_totals_for_local_date_range(
            today_local_date, today_local_date
        )
        today_total, today_rows = _fmt_total(today_t, "Total Import", "Total Import")

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
            if _bp_periods:
                period_start, period_end_excl = _bp_periods[-1]
            else:
                bd = billing_day
                if now_local.day >= bd:
                    period_start = now_local.replace(day=bd, hour=0, minute=0, second=0, microsecond=0)
                else:
                    m = now_local.month - 1 or 12
                    y = now_local.year if now_local.month > 1 else now_local.year - 1
                    period_start = now_local.replace(year=y, month=m, day=bd, hour=0, minute=0, second=0, microsecond=0)

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

        month_t = store.get_billing_totals_for_local_date_range(
            period_start_date.isoformat(), today_local_date
        )
        month_total, month_rows = _fmt_total(month_t, "Total Import", "Total Import")

        # Calendar year — from Jan 1 local to today local
        year_start_date = now_local.date().replace(month=1, day=1).isoformat()
        year_t = store.get_billing_totals_for_local_date_range(
            year_start_date, today_local_date
        )
        year_total, year_rows = _fmt_total(year_t, "Total Import", "Total Import")

        def fmt_rows(rows):
            return [{"label": r["label"], "cost": r["cost"], "bold": r.get("bold", False)}
                    for r in rows]

        return jsonify({
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
        cfg = _lj(os.path.join(DATA_DIR, "meters_config.json"), {})
        postcode = None
        for m_data in cfg.get("meters", {}).values():
            meta = (m_data or {}).get("meta", {}) or {}
            if not meta.get("sub_meter"):
                postcode = meta.get("postcode_prefix", "").strip().upper()
                break
        if not postcode:
            return jsonify({"error": "no_postcode"}), 404

        from datetime import datetime, timezone
        now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%MZ")
        url = f"https://api.carbonintensity.org.uk/regional/intensity/{now_iso}/fw48h/postcode/{postcode}"
        req = urllib.request.Request(url, headers={"Accept": "application/json"})
        with urllib.request.urlopen(req, timeout=8) as resp:
            data = json.loads(resp.read())

        # Log raw structure for debugging
        logger.info("api_carbon raw keys: %s", list(data.keys()) if isinstance(data, dict) else type(data).__name__)
        slots = []
        raw = data.get("data", [])
        logger.info("api_carbon data type: %s", type(raw).__name__)
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


@app.route("/import")
def import_page():
    return render_template("import.html", active="import")


@app.route("/config-history")
def config_history_page():
    return render_template("config_history.html", active="config_history")


# ── Chart file serving ────────────────────────────────────────────────────────

@app.route("/charts/net_heatmap.html")
def serve_heatmap():
    p = os.path.join(CHART_DIR, "net_heatmap.html")
    if not os.path.exists(p):
        return "Chart not yet generated", 404
    return send_file(p)


@app.route("/charts/daily_usage.html")
def serve_daily():
    p = os.path.join(CHART_DIR, "daily_usage.html")
    if not os.path.exists(p):
        return "Chart not yet generated", 404
    return send_file(p)


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

        cfg   = _lj(os.path.join(DATA_DIR, "meters_config.json"), {})
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

        # Use local dates from the store index (pre-computed, timezone-correct)
        all_date_strs = store.get_local_dates()
        if not all_date_strs:
            # Fallback: get meter colors from a small sample
            sample = store.get_blocks_for_range(
                datetime(2000,1,1), datetime(2100,1,1)
            )
            meter_colors = _ec.build_meter_colors(sample)
            return jsonify({"currency": currency, "rows": [], "meters": [], "export_color": "#ff7f0e"})

        # Build meter colors from a representative sample (first 200 blocks)
        from datetime import date as _date
        first_date = datetime.strptime(all_date_strs[0], "%Y-%m-%d")
        sample_end = first_date.replace(hour=23, minute=59, second=59)
        sample_blocks = store.get_blocks_for_range(first_date, sample_end)
        meter_colors = _ec.build_meter_colors(sample_blocks)

        meter_labels = {}
        for meter_id, meter_cfg in cfg.get("meters", {}).items():
            meta  = (meter_cfg.get("meta") or {})
            is_sub = bool(meta.get("sub_meter"))
            if is_sub:
                label = meta.get("device") or meta.get("site") or meter_id
            else:
                label = "Grid"
            meter_labels[meter_id] = label

        all_dates = [datetime.strptime(d, "%Y-%m-%d").date() for d in all_date_strs]

        # Fetch all blocks in one query using local_date boundaries
        # This correctly captures BST blocks at 23:xx UTC that belong to the next local day
        from datetime import timedelta as _td
        if all_dates:
            all_blocks_flat = store.get_blocks_for_local_date_range(
                all_date_strs[0], all_date_strs[-1]
            )
        else:
            all_blocks_flat = []

        # Index by local_date
        from collections import defaultdict as _dd
        # Index blocks by local_date using the pre-computed field on each block
        # (set by _row_to_block from the local_date column — timezone-correct).
        _blocks_by_local_date = _dd(list)
        from zoneinfo import ZoneInfo as _ZI
        for _blk in all_blocks_flat:
            # _row_to_block sets "start" as UTC ISO; derive local_date correctly
            _blk_dt = _blk.get("start", "")
            if _blk_dt:
                try:
                    _local_d = datetime.fromisoformat(_blk_dt).replace(
                        tzinfo=_ZI("UTC")).astimezone(_ZI(tz_name)).date().isoformat()
                    _blocks_by_local_date[_local_d].append(_blk)
                except Exception:
                    pass

        # Pre-compute billing_period_start for each date using config periods
        # (fast — no full block scan needed).
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
                        _bpe.date().isoformat(),  # exclusive end
                    )
                    _d += _td2(days=1)
            logger.info("api_blocks_summary: billing periods computed: %s",
                        [(s.strftime('%Y-%m-%d'), e.strftime('%Y-%m-%d')) for s,e in _bp_periods])
        except Exception as _bpe_err:
            logger.warning("api_blocks_summary: billing period pre-compute failed: %s", _bpe_err)

        rows = []
        for d in all_dates:
            day_blocks = _blocks_by_local_date.get(d.isoformat(), [])

            # ── Main meter remainder via billing summary (billing-accurate) ──
            # Use a very wide UTC window so all blocks in day_blocks pass the
            # internal block_start filter. day_blocks is pre-filtered by local_date
            # so only the correct day's blocks are present.
            ps_utc = datetime(d.year, d.month, d.day, 0, 0, 0) - _td(hours=14)
            pe_utc = datetime(d.year, d.month, d.day, 23, 59, 59) + _td(hours=14)
            s        = _ec.calculate_billing_summary_for_period(day_blocks, ps_utc, pe_utc)

            # Standing charge: take directly from any block — it is the same value
            # on all blocks for a given local day. Do NOT use s["total_standing"]
            # because calculate_billing_summary groups by block_start.date() (UTC)
            # which double-counts on BST days (23:xx UTC block = next local day).
            # standing_charge lives at block["meters"][meter_id]["standing_charge"]
            standing = 0.0
            if day_blocks:
                _first_meter = next(iter((day_blocks[0].get("meters") or {}).values()), {})
                standing = float(_first_meter.get("standing_charge") or 0.0)

            main_imp_kwh  = 0.0
            main_imp_cost = 0.0
            main_exp_kwh  = 0.0
            main_exp_cost = 0.0
            for key, t in (s.get("totals") or {}).items():
                if t.get("is_submeter"):
                    continue
                if "export" in key.lower():
                    main_exp_kwh  += float(t.get("kwh")  or 0)
                    main_exp_cost += abs(float(t.get("cost") or 0))
                else:
                    main_imp_kwh  += float(t.get("kwh")  or 0)
                    main_imp_cost += float(t.get("cost") or 0)

            # ── Sub-meters aggregated directly from day_blocks ──
            sub_totals = defaultdict(lambda: {"imp_kwh":0.0,"imp_cost":0.0,"exp_kwh":0.0,"exp_cost":0.0})
            for b in day_blocks:
                if not b or not b.get("start"):
                    continue
                try:
                    for mid, md in (b.get("meters") or {}).items():
                        if not (md or {}).get("meta", {}).get("sub_meter"):
                            continue
                        ch_imp = (md.get("channels") or {}).get("import") or {}
                        ch_exp = (md.get("channels") or {}).get("export") or {}
                        sub_totals[mid]["imp_kwh"]  += float(ch_imp.get("kwh_grid", ch_imp.get("kwh", 0)) or 0)
                        sub_totals[mid]["imp_cost"] += float(ch_imp.get("cost", 0) or 0)
                        sub_totals[mid]["exp_kwh"]  += float(ch_exp.get("kwh", 0) or 0)
                        sub_totals[mid]["exp_cost"] += float(ch_exp.get("cost", 0) or 0)
                except Exception:
                    continue

            # ── Assemble meters_out ──
            meters_out = {"electricity_main": {
                "imp_kwh":  round(main_imp_kwh,  4),
                "imp_cost": round(main_imp_cost, 4),
                "exp_kwh":  round(main_exp_kwh,  4),
                "exp_cost": round(main_exp_cost, 4),
            }}
            for mid, st in sub_totals.items():
                meters_out[mid] = {f: round(v, 4) for f, v in st.items()}

            # Get the historically correct billing_day for this date from blocks
            row_billing_day = billing_day  # default to current
            if day_blocks:
                bd = day_blocks[0].get("_billing_day")
                if bd is not None:
                    row_billing_day = int(bd)

            # Look up pre-computed billing period start/end for this date
            _bp_entry = _bp_start_by_date.get(d.isoformat())
            _bp_start = _bp_entry[0] if _bp_entry else d.isoformat()
            _bp_end   = _bp_entry[1] if _bp_entry else None  # exclusive end

            rows.append({
                "year":                 d.year,
                "month":               d.month,
                "day":                 d.day,
                "billing_day":         row_billing_day,
                "billing_period_start": _bp_start,
                "billing_period_end":   _bp_end,
                "standing":    round(standing, 4),
                "meters":      meters_out,
                "imp_kwh":  round(main_imp_kwh  + sum(m["imp_kwh"]  for mid,m in meters_out.items() if mid != "electricity_main"), 4),
                "exp_kwh":  round(main_exp_kwh, 4),
                "imp_cost": round(main_imp_cost + sum(m["imp_cost"] for mid,m in meters_out.items() if mid != "electricity_main"), 4),
                "exp_cost": round(main_exp_cost, 4),
            })

        all_meter_ids = [m for m in meter_colors if m != "electricity_main_export"]
        meters_list = [{
            "id":    mid,
            "label": meter_labels.get(mid, mid),
            "color": meter_colors[mid],
            "is_sub": mid != "electricity_main",
        } for mid in all_meter_ids]

        export_color = meter_colors.get("electricity_main_export",
                       meter_colors.get("electricity_main", "#ff7f0e"))

        return jsonify({
            "currency":     currency,
            "billing_day":  billing_day,
            "rows":         rows,
            "meters":       meters_list,
            "export_color": export_color,
        })
    except Exception as e:
        logger.error("api_blocks_summary: %s", e)
        return jsonify({"error": str(e)}), 500

@app.route("/api/charts/heatmap")
def api_chart_heatmap():
    """Return heatmap chart HTML as JSON for inline embedding."""
    p = os.path.join(CHART_DIR, "net_heatmap.html")
    if not os.path.exists(p):
        return jsonify({"html": None})
    with open(p) as f:
        return jsonify({"html": f.read()})


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
                   cp.site_name, cp.change_reason,
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
                "change_reason":  r["change_reason"],
                "block_count":    r["block_count"],
            })
        # Include the configured timezone for client-side date formatting
        cfg_tz = "UTC"
        try:
            from energy_engine_io import load_json as _lj_tz
            _cfg_tz = _lj_tz(os.path.join(DATA_DIR, "meters_config.json"), {})
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


@app.route("/api/config/history", methods=["POST"])
def api_config_history_create():
    """Create a new config period, inheriting full_config_json from the current active period."""
    try:
        store = _get_store()
        data  = request.get_json(force=True)

        from datetime import datetime as _dt
        # Required: effective_from
        ef_from_raw = data.get("effective_from")
        if not ef_from_raw:
            return jsonify({"error": "effective_from is required"}), 400
        ef_from = str(ef_from_raw).replace(" ", "T").split(".")[0]
        try:
            _dt.fromisoformat(ef_from)
        except ValueError:
            return jsonify({"error": "Invalid effective_from date"}), 400

        # Get the current active period to inherit its full_config_json
        cur = store._conn.execute(
            "SELECT * FROM config_periods WHERE effective_to IS NULL ORDER BY effective_from DESC LIMIT 1"
        )
        active = cur.fetchone()
        if not active:
            return jsonify({"error": "No active config period found to inherit from"}), 400

        import json as _json
        full_cfg = _json.loads(active["full_config_json"]) if active["full_config_json"] else {}

        # Apply any overrides from the request
        billing_day     = data.get("billing_day", active["billing_day"])
        timezone        = data.get("timezone", active["timezone"])
        currency_symbol = data.get("currency_symbol", active["currency_symbol"])
        currency_code   = data.get("currency_code", active["currency_code"])
        site_name       = data.get("site_name", active["site_name"])
        change_reason   = data.get("change_reason") or None

        # Update full_config_json with overrides
        for mid, md in full_cfg.get("meters", {}).items():
            meta = md.get("meta") or {}
            if "billing_day"      in data: meta["billing_day"]      = int(billing_day)
            if "timezone"         in data: meta["timezone"]         = timezone
            if "currency_symbol"  in data: meta["currency_symbol"]  = currency_symbol
            if "currency_code"    in data: meta["currency_code"]    = currency_code
            if "site_name"        in data: meta["site_name"]        = site_name
            md["meta"] = meta

        with store._conn:
            store._conn.execute(
                """INSERT INTO config_periods
                   (effective_from, effective_to, billing_day, block_minutes, timezone,
                    currency_symbol, currency_code, site_name, change_reason, full_config_json)
                   VALUES (?, NULL, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    ef_from,
                    int(billing_day) if billing_day else 1,
                    int(active["block_minutes"] or 30),
                    timezone or "UTC",
                    currency_symbol or "£",
                    currency_code or "GBP",
                    site_name,
                    change_reason,
                    _json.dumps(full_cfg),
                )
            )

        # Rebuild chain — sorts by effective_from and reassigns blocks
        _rebuild_config_period_chain(store)

        logger.info("api_config_history_create: new period from %s", ef_from)
        return jsonify({"ok": True})
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
            "currency_symbol", "currency_code", "site_name",
        }
        updates = {k: v for k, v in data.items() if k in allowed}
        if not updates:
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

        # Build UPDATE statement
        set_clause = ", ".join(f"{k} = ?" for k in updates)
        values     = list(updates.values()) + [period_id]
        with store._conn:
            store._conn.execute(
                f"UPDATE config_periods SET {set_clause} WHERE id = ?",
                values
            )

        logger.info("api_config_history_update: period %d updated %s", period_id, list(updates.keys()))

        # Rebuild the contiguous chain: sort all periods by effective_from,
        # then set each period's effective_to = next period's effective_from.
        # Robust regardless of how far the date was moved.
        try:
            _rebuild_config_period_chain(store)
        except Exception as _snap_e:
            logger.warning("api_config_history_update: chain rebuild failed: %s", _snap_e)

                # Regenerate charts since billing periods may have changed
        try:
            import energy_charts as _ec
            import os as _os
            blocks = store.get_all_blocks()
            if blocks:
                from energy_engine_io import load_json as _lj_ec
                cfg       = _lj_ec(os.path.join(DATA_DIR, "meters_config.json"), {})
                main_meta = {}
                for md in cfg.get("meters", {}).values():
                    if not (md.get("meta") or {}).get("sub_meter"):
                        main_meta = md.get("meta") or {}
                        break
                tz_name   = main_meta.get("timezone", "UTC")
                bm        = int(main_meta.get("block_minutes") or 30)
                currency  = main_meta.get("currency_symbol", "£")
                os.makedirs(CHART_DIR, exist_ok=True)
                html = _ec.generate_daily_import_export_charts(blocks, timezone_name=tz_name, block_minutes=bm, currency=currency)
                with open(os.path.join(CHART_DIR, "daily_usage.html"), "w") as f:
                    f.write(html)
                logger.info("api_config_history_update: charts regenerated")
        except Exception as _e:
            logger.warning("api_config_history_update: chart regen failed: %s", _e)

        return jsonify({"ok": True})
    except Exception as e:
        logger.error("api_config_history_update: %s", e)
        return jsonify({"error": str(e)}), 500


@app.route("/api/config/history/<int:period_id>", methods=["DELETE"])
def api_config_history_delete(period_id):
    """Delete a config period, re-assigning its blocks to an adjacent period."""
    import json as _json
    try:
        store = _get_store()

        # Check before deleting whether this is the active period
        cp = store.get_config_period(period_id)
        is_active = cp and cp.get("effective_to") is None

        result = store.delete_config_period(period_id)

        # Rebuild chain first so effective_to values are consistent
        try:
            _rebuild_config_period_chain(store)
        except Exception as _e:
            logger.warning("api_config_history_delete: chain rebuild failed: %s", _e)

        # If we deleted the active period, the predecessor is now active.
        # Write its full_config_json back to meters_config.json so the engine,
        # charts and billing all read the correct (now-current) config.
        if is_active:
            try:
                new_active = store._conn.execute(
                    "SELECT full_config_json FROM config_periods "
                    "WHERE effective_to IS NULL ORDER BY effective_from DESC LIMIT 1"
                ).fetchone()
                if new_active and new_active["full_config_json"]:
                    from energy_engine_io import save_json_atomic as _sja
                    restored_cfg = _json.loads(new_active["full_config_json"])
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

        # Regenerate charts
        try:
            import energy_charts as _ec
            blocks = store.get_all_blocks()
            if blocks:
                from energy_engine_io import load_json as _lj_del
                cfg       = _lj_del(os.path.join(DATA_DIR, "meters_config.json"), {})
                main_meta = {}
                for md in cfg.get("meters", {}).values():
                    if not (md.get("meta") or {}).get("sub_meter"):
                        main_meta = md.get("meta") or {}
                        break
                tz_name  = main_meta.get("timezone", "UTC")
                bm       = int(main_meta.get("block_minutes") or 30)
                currency = main_meta.get("currency_symbol", "£")
                os.makedirs(CHART_DIR, exist_ok=True)
                html = _ec.generate_daily_import_export_charts(
                    blocks, timezone_name=tz_name, block_minutes=bm, currency=currency
                )
                with open(os.path.join(CHART_DIR, "daily_usage.html"), "w") as f:
                    f.write(html)
        except Exception as _e:
            logger.warning("api_config_history_delete: chart regen failed: %s", _e)

        return jsonify({
            "ok": True,
            "blocks_reassigned": result["blocks_reassigned"],
            "config_restored": is_active,
        })
    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    except Exception as e:
        logger.error("api_config_history_delete: %s", e)
        return jsonify({"error": str(e)}), 500


@app.route("/api/config", methods=["POST"])
def api_save_config():
    try:
        payload = request.get_json(force=True)
        if not isinstance(payload, dict) or "meters" not in payload:
            return jsonify({"error": "Invalid config structure"}), 400

        # change_reason is an optional UI field, not part of the config schema
        change_reason = payload.pop("change_reason", None) or None
        data = payload

        # Zip current data before committing config change
        _create_backup_zip(label="pre_config_save")

        save_config(data)
        logger.info("server: meters_config.json saved (%d meters)", len(data["meters"]))

        # Snapshot the new config as a config period only if billing-significant
        # meta has changed (sensor changes, postcode etc don't need a new period)
        try:
            from block_store import config_meta_significant
            store = _get_store()
            cur_id = store.get_current_config_period_id()
            should_create = True
            if cur_id is not None:
                cur_cp = store.get_config_period(cur_id)
                if cur_cp:
                    import json as _json
                    old_cfg = _json.loads(cur_cp["full_config_json"])
                    if not config_meta_significant(old_cfg, data):
                        should_create = False
                        # Still update the full_config_json snapshot in place
                        with store._conn:
                            store._conn.execute(
                                "UPDATE config_periods SET full_config_json = ? WHERE id = ?",
                                (_json.dumps(data), cur_id)
                            )
                        logger.info("server: config saved — no billing meta change, period unchanged")
            if should_create:
                store.insert_config_period(data, change_reason=change_reason)
                logger.info("server: config period snapshot created (reason=%s)", change_reason or "none")
        except Exception as _e:
            logger.warning("server: config period snapshot failed: %s", _e)

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


@app.route("/api/backup", methods=["POST"])
def api_backup():
    """Create a manual backup zip of all data files."""
    try:
        path = _create_backup_zip(label="manual")
        return jsonify({"ok": True, "path": os.path.basename(path)})
    except Exception as e:
        logger.error("api_backup: %s", e)
        return jsonify({"error": str(e)}), 500


@app.route("/api/backup/info", methods=["GET"])
def api_backup_info():
    """Return backup configuration info."""
    return jsonify({
        "backup_dir": SHARE_BACKUP_DIR,
        "mode": os.environ.get("EMT_MODE", "supervised")
    })


@app.route("/api/backup/list", methods=["GET"])
def api_backup_list():
    """List available backup zips and last-finalise flat files."""
    import glob
    try:
        zips = sorted(glob.glob(f"{SHARE_BACKUP_DIR}/backups/*.zip"), reverse=True)
        # Check for flat files from last finalise
        known = ["blocks.db", "blocks.json", "current_block.json", "cumulative_totals.json", "meters_config.json"]
        flat_files = []
        for fname in known:
            fpath = f"{SHARE_BACKUP_DIR}/{fname}"
            if os.path.exists(fpath):
                mtime = os.path.getmtime(fpath)
                from datetime import datetime as _dt
                flat_files.append({
                    "name": fname,
                    "modified": _dt.utcfromtimestamp(mtime).strftime("%Y-%m-%dT%H:%M:%S")
                })
        return jsonify({
            "zips": [os.path.basename(z) for z in zips],
            "flat": flat_files
        })
    except Exception as e:
        return jsonify({"zips": [], "flat": []})


@app.route("/api/backup/restore", methods=["POST"])
def api_backup_restore():
    """Restore selected data files from a named backup zip or from last-finalise flat files."""
    global _store
    import zipfile, shutil
    try:
        data      = request.get_json(force=True)
        zipname   = data.get("zip", "")
        selected  = data.get("files", None)
        from_flat = data.get("from_flat", False)
        known     = {"blocks.db", "blocks.json", "current_block.json", "cumulative_totals.json", "meters_config.json"}

        if not from_flat:
            if not zipname or "/" in zipname or "\\" in zipname:
                return jsonify({"error": "Invalid zip name"}), 400

        _create_backup_zip(label="pre_restore")

        restored = []

        if from_flat:
            for fname in (selected or list(known)):
                if fname not in known:
                    continue
                src_path = f"{SHARE_BACKUP_DIR}/{fname}"
                dst_path = os.path.join(DATA_DIR, fname)
                if os.path.exists(src_path):
                    shutil.copy2(src_path, dst_path)
                    restored.append(fname)
            logger.info("api_backup_restore: restored flat files %s", restored)
        else:
            if not zipname or "/" in zipname or "\\" in zipname:
                return jsonify({"error": "Invalid zip name"}), 400
            zip_path = f"{SHARE_BACKUP_DIR}/backups/{zipname}"
            if not os.path.exists(zip_path):
                return jsonify({"error": "Backup not found"}), 404
            with zipfile.ZipFile(zip_path, "r") as zf:
                for name in zf.namelist():
                    basename = os.path.basename(name)
                    if basename not in known:
                        continue
                    if selected is not None and basename not in selected:
                        continue
                    dest = os.path.join(DATA_DIR, basename)
                    with zf.open(name) as zf_src:
                        with open(dest, "wb") as dst:
                            dst.write(zf_src.read())
                    restored.append(basename)
            logger.info("api_backup_restore: restored %s from %s", restored, zipname)

        # If a legacy blocks.json was restored from an old backup, auto-migrate it
        legacy_json = os.path.join(DATA_DIR, "blocks.json")
        if "blocks.json" in restored and os.path.exists(legacy_json):
            try:
                from block_store import migrate_json_to_sqlite
                from energy_engine_io import load_json as _lj3
                cfg = _lj3(os.path.join(DATA_DIR, "meters_config.json"), {})
                db_path = os.path.join(DATA_DIR, "blocks.db")
                if os.path.exists(db_path):
                    os.remove(db_path)
                _store = None
                store = _get_store()
                migrated = migrate_json_to_sqlite(legacy_json, store, cfg)
                os.rename(legacy_json, legacy_json + ".migrated")
                restored.append("blocks.db (migrated from legacy blocks.json)")
                logger.info("api_backup_restore: migrated legacy blocks.json -> blocks.db (%d blocks)", migrated)
            except Exception as _e:
                logger.warning("api_backup_restore: legacy migration failed: %s", _e)

        # Reset store connection so next request gets fresh handle to restored DB
        if "blocks.db" in restored or "blocks.json" in restored:
            if _store:
                try:
                    _store.close()
                except Exception:
                    pass
            _store = None

        return jsonify({"ok": True, "restored": restored})
    except Exception as e:
        logger.error("api_backup_restore: %s", e)
        return jsonify({"error": str(e)}), 500
@app.route("/api/import/extract-zip", methods=["POST"])
def api_import_extract_zip():
    """Extract JSON files from an uploaded zip and return them as base64."""
    import zipfile, base64
    try:
        zf_file = request.files.get("zipfile")
        if not zf_file:
            return jsonify({"error": "No zip file provided"}), 400
        known = {"blocks.db", "blocks.json", "current_block.json", "cumulative_totals.json", "meters_config.json"}
        files = {}
        with zipfile.ZipFile(zf_file.stream, "r") as zf:
            for name in zf.namelist():
                basename = os.path.basename(name)
                if basename in known:
                    files[basename] = base64.b64encode(zf.read(name)).decode("utf-8")
        if not files:
            return jsonify({"error": "No recognised JSON files found in zip"}), 400
        logger.info("api_import_extract_zip: extracted %s", list(files.keys()))
        return jsonify({"files": files})
    except Exception as e:
        logger.error("api_import_extract_zip: %s", e)
        return jsonify({"error": str(e)}), 500


@app.route("/api/backup/flat-info", methods=["GET"])
def api_backup_flat_info():
    """Return metadata about the last-finalise flat backup files."""
    known = ["blocks.db", "blocks.json", "current_block.json", "cumulative_totals.json", "meters_config.json"]
    from datetime import datetime as _dt
    files = {}
    for fname in known:
        fpath = f"{SHARE_BACKUP_DIR}/{fname}"
        if os.path.exists(fpath):
            mtime = os.path.getmtime(fpath)
            size  = os.path.getsize(fpath)
            files[fname] = {
                "modified": _dt.utcfromtimestamp(mtime).strftime("%Y-%m-%dT%H:%M:%S UTC"),
                "size_kb":  round(size / 1024, 1)
            }
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
        zip_path = f"{SHARE_BACKUP_DIR}/backups/{zipname}"
        if not os.path.exists(zip_path):
            return jsonify({"error": "Backup not found"}), 404
        known = {"blocks.db", "blocks.json", "current_block.json", "cumulative_totals.json", "meters_config.json"}
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


def _create_backup_zip(label="backup"):
    """Zip all data files into /share/energy_meter_tracker_backup/backups/."""
    import zipfile
    import glob
    from datetime import datetime as _dt
    backup_dir = f"{SHARE_BACKUP_DIR}/backups"
    os.makedirs(backup_dir, exist_ok=True)
    timestamp = _dt.utcnow().strftime("%Y%m%dT%H%M%S")
    zip_path  = f"{backup_dir}/{timestamp}_{label}.zip"
    files = ["cumulative_totals.json", "meters_config.json", "current_block.json"]
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
        # Backup blocks DB using SQLite online backup API into a temp file
        import tempfile
        db_src = os.path.join(DATA_DIR, "blocks.db")
        if os.path.exists(db_src):
            try:
                _get_store().backup(db_src + ".bak")
                zf.write(db_src + ".bak", "blocks.db")
                os.remove(db_src + ".bak")
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
        cfg       = _lj_regen(os.path.join(DATA_DIR, "meters_config.json"), {})
        main_meta = {}
        for md in cfg.get("meters", {}).values():
            if not (md.get("meta") or {}).get("sub_meter"):
                main_meta = md.get("meta") or {}
                break
        tz_name  = main_meta.get("timezone", "UTC")
        bm       = int(main_meta.get("block_minutes") or 30)
        currency = main_meta.get("currency_symbol", "£")
        os.makedirs(CHART_DIR, exist_ok=True)
        blocks = store.get_all_blocks()
        html = energy_charts.generate_net_heatmap(blocks, timezone_name=tz_name, block_minutes=bm, currency=currency)
        with open(os.path.join(CHART_DIR, "net_heatmap.html"), "w") as f:
            f.write(html)
        html = energy_charts.generate_daily_import_export_charts(blocks, timezone_name=tz_name, block_minutes=bm, currency=currency)
        with open(os.path.join(CHART_DIR, "daily_usage.html"), "w") as f:
            f.write(html)
        logger.info("server: charts regenerated on demand")
        return jsonify({"ok": True})
    except Exception as e:
        logger.error("api_regenerate_charts: %s", e)
        return jsonify({"error": str(e)}), 500


@app.route("/api/import", methods=["POST"])
def api_import():
    """
    Accept uploaded JSON data files.
    Expects multipart form with one or more of:
      blocks, current_block, cumulative_totals, meters_config
    Pauses the engine during import to prevent file conflicts.
    """
    import sys
    import importlib
    try:
        engine = sys.modules.get("engine")
        if engine and hasattr(engine, 'pause_engine'):
            engine.pause_engine()

        imported = []
        # Handle blocks import: write temp JSON then migrate into SQLite
        blocks_file = request.files.get("blocks")
        if blocks_file:
            import tempfile
            blocks_data = json.loads(blocks_file.read().decode("utf-8"))
            # Write to a temp JSON file for migrate_json_to_sqlite
            tmp_json = os.path.join(DATA_DIR, "blocks_import.json.tmp")
            with open(tmp_json, "w") as out:
                json.dump(blocks_data, out)
            try:
                from block_store import migrate_json_to_sqlite
                cfg_path = os.path.join(DATA_DIR, "meters_config.json")
                from energy_engine_io import load_json as _lj2
                cfg = _lj2(cfg_path, {})
                # Reset the store and migrate
                db_path = os.path.join(DATA_DIR, "blocks.db")
                if os.path.exists(db_path):
                    os.remove(db_path)
                global _store
                _store = None  # force re-open
                store = _get_store()
                migrate_json_to_sqlite(tmp_json, store, cfg)
                imported.append("blocks.db")
                logger.info("server: imported blocks.json -> blocks.db (%d blocks)", len(blocks_data))
            finally:
                if os.path.exists(tmp_json):
                    os.remove(tmp_json)

        # Handle remaining JSON files
        file_map = {
            "current_block":     "current_block.json",
            "cumulative_totals": "cumulative_totals.json",
            "meters_config":     "meters_config.json",
        }
        for field, filename in file_map.items():
            f = request.files.get(field)
            if f:
                data = json.loads(f.read().decode("utf-8"))
                dest = os.path.join(DATA_DIR, filename)
                tmp  = dest + ".tmp"
                with open(tmp, "w") as out:
                    json.dump(data, out, indent=2)
                os.replace(tmp, dest)
                imported.append(filename)
                logger.info("server: imported %s", filename)

        if not imported:
            if engine and hasattr(engine, 'resume_engine'):
                engine.resume_engine()
            return jsonify({"error": "No files received"}), 400

        return jsonify({"ok": True, "imported": imported})
    except Exception as e:
        logger.error("api_import: %s", e)
        return jsonify({"error": str(e)}), 500
    finally:
        # Always resume engine after import, wait one tick for files to settle
        import threading
        def delayed_resume():
            import time
            time.sleep(12)  # wait > one engine tick
            if engine and hasattr(engine, 'resume_engine'):
                engine.resume_engine()
        threading.Thread(target=delayed_resume, daemon=True).start()

# ── Historical corrections ────────────────────────────────────────────────────

@app.route("/api/corrections/preview", methods=["POST"])
def api_corrections_preview():
    """
    Preview what a standing-charge or rate correction would affect.
    Body: { type: "standing"|"rate", from_date: "YYYY-MM-DD", to_date: "YYYY-MM-DD",
            value: float, channel: "import"|"export" (rate only) }
    Returns: { days: int, blocks: int, current_min: float, current_max: float }
    """
    try:
        data       = request.get_json(force=True) or {}
        corr_type  = data.get("type")          # "standing" or "rate"
        from_date  = data.get("from_date", "")
        to_date    = data.get("to_date", "")
        channel    = data.get("channel", "import")  # for rate corrections

        if corr_type not in ("standing", "rate"):
            return jsonify({"error": "type must be 'standing' or 'rate'"}), 400
        if not from_date or not to_date:
            return jsonify({"error": "from_date and to_date required"}), 400

        store = _get_store()

        if corr_type == "standing":
            cur = store._conn.execute(
                """SELECT COUNT(DISTINCT local_date) as days,
                          COUNT(*) as blocks,
                          MIN(standing_charge) as cur_min,
                          MAX(standing_charge) as cur_max
                   FROM blocks
                   WHERE local_date >= ? AND local_date <= ?""",
                (from_date, to_date)
            )
        else:
            col = "imp_rate" if channel == "import" else "exp_rate"
            cur = store._conn.execute(
                f"""SELECT COUNT(DISTINCT local_date) as days,
                           COUNT(*) as blocks,
                           MIN({col}) as cur_min,
                           MAX({col}) as cur_max
                    FROM blocks
                    WHERE local_date >= ? AND local_date <= ?
                      AND {col} IS NOT NULL""",
                (from_date, to_date)
            )

        row = cur.fetchone()
        return jsonify({
            "days":        row["days"]    or 0,
            "blocks":      row["blocks"]  or 0,
            "current_min": row["cur_min"] or 0,
            "current_max": row["cur_max"] or 0,
        })
    except Exception as e:
        logger.error("api_corrections_preview: %s", e)
        return jsonify({"error": str(e)}), 500


@app.route("/api/corrections/apply", methods=["POST"])
def api_corrections_apply():
    """
    Apply a standing-charge or rate correction to the live database.
    Body: { type: "standing"|"rate", from_date: "YYYY-MM-DD", to_date: "YYYY-MM-DD",
            value: float, channel: "import"|"export" (rate only),
            recalc_cost: bool (rate only — recalculate imp_cost from rate × kwh) }
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

        if corr_type == "standing":
            cur = store._conn.execute(
                """UPDATE blocks SET standing_charge = ?
                   WHERE local_date >= ? AND local_date <= ?""",
                (value, from_date, to_date)
            )
            store._conn.commit()
            updated = cur.rowcount
            logger.info(
                "api_corrections_apply: standing_charge set to %.4f "
                "for %d blocks (%s → %s)", value, updated, from_date, to_date
            )

        else:  # rate correction
            if channel == "import":
                if recalc_cost:
                    cur = store._conn.execute(
                        """UPDATE blocks
                           SET imp_rate = ?,
                               imp_cost = ROUND(imp_kwh * ?, 6)
                           WHERE local_date >= ? AND local_date <= ?
                             AND imp_rate IS NOT NULL""",
                        (value, value, from_date, to_date)
                    )
                else:
                    cur = store._conn.execute(
                        """UPDATE blocks SET imp_rate = ?
                           WHERE local_date >= ? AND local_date <= ?
                             AND imp_rate IS NOT NULL""",
                        (value, from_date, to_date)
                    )
            else:  # export
                if recalc_cost:
                    cur = store._conn.execute(
                        """UPDATE blocks
                           SET exp_rate = ?,
                               exp_cost = ROUND(exp_kwh * ?, 6)
                           WHERE local_date >= ? AND local_date <= ?
                             AND exp_rate IS NOT NULL""",
                        (value, value, from_date, to_date)
                    )
                else:
                    cur = store._conn.execute(
                        """UPDATE blocks SET exp_rate = ?
                           WHERE local_date >= ? AND local_date <= ?
                             AND exp_rate IS NOT NULL""",
                        (value, from_date, to_date)
                    )
            store._conn.commit()
            updated = cur.rowcount
            logger.info(
                "api_corrections_apply: %s rate set to %.6f "
                "(recalc_cost=%s) for %d blocks (%s → %s)",
                channel, value, recalc_cost, updated, from_date, to_date
            )

        return jsonify({"ok": True, "updated_blocks": updated})
    except Exception as e:
        logger.error("api_corrections_apply: %s", e)
        return jsonify({"error": str(e)}), 500