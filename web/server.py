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

from flask import Flask, jsonify, redirect, render_template, request, send_file, url_for

logger = logging.getLogger("server")

app = Flask(__name__, template_folder="templates")

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
SHARE_BACKUP_DIR = "/share/energy_meter_tracker_backup"
_ha_client = None   # reference to the running HAClient instance
_event_loop = None  # asyncio event loop — captured at init time


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
        return redirect(url_for("charts_page"))
    return redirect(url_for("config_page"))


@app.route("/config")
def config_page():
    cfg = load_config()
    return render_template("config.html", config=cfg, active="config")


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
    """Fetch add-on logs via Supervisor API."""
    import urllib.request
    token = os.environ.get("SUPERVISOR_TOKEN", "")
    lines = min(int(request.args.get("lines", 100)), 1000)
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
        from energy_engine_io import load_json as _load_json
        blocks = _load_json(os.path.join(DATA_DIR, "blocks.json"), [])
        block_count = len(blocks)
    except Exception:
        block_count = 0
    return render_template(
        "charts.html",
        heatmap_exists=heatmap_exists,
        daily_exists=daily_exists,
        block_count=block_count,
        active="charts",
    )


@app.route("/import")
def import_page():
    return render_template("import.html", active="import")


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
    token = os.environ.get("SUPERVISOR_TOKEN", "")
    try:
        req = urllib.request.Request(
            "http://supervisor/core/api/states",
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


@app.route("/api/config", methods=["POST"])
def api_save_config():
    try:
        data = request.get_json(force=True)
        if not isinstance(data, dict) or "meters" not in data:
            return jsonify({"error": "Invalid config structure"}), 400

        # Zip current data before committing config change
        _create_backup_zip(label="pre_config_save")

        save_config(data)
        logger.info("server: meters_config.json saved (%d meters)", len(data["meters"]))

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


@app.route("/api/backup/list", methods=["GET"])
def api_backup_list():
    """List available backup zips."""
    import glob
    try:
        zips = sorted(glob.glob(f"{SHARE_BACKUP_DIR}/backups/*.zip"), reverse=True)
        return jsonify([os.path.basename(z) for z in zips])
    except Exception as e:
        return jsonify([])


def _create_backup_zip(label="backup"):
    """Zip all data files into /share/energy_meter_tracker_backup/backups/."""
    import zipfile
    import glob
    from datetime import datetime as _dt
    backup_dir = f"{SHARE_BACKUP_DIR}/backups"
    os.makedirs(backup_dir, exist_ok=True)
    timestamp = _dt.utcnow().strftime("%Y%m%dT%H%M%S")
    zip_path  = f"{backup_dir}/{timestamp}_{label}.zip"
    files = ["blocks.json", "cumulative_totals.json", "meters_config.json", "current_block.json"]
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
        for fname in files:
            src = f"{DATA_DIR}/{fname}"
            if os.path.exists(src):
                zf.write(src, fname)
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
        from energy_engine_io import load_json
        import energy_charts
        blocks = load_json(os.path.join(DATA_DIR, "blocks.json"), [])
        if not blocks:
            return jsonify({"error": "No blocks data available"}), 400
        os.makedirs(CHART_DIR, exist_ok=True)
        html = energy_charts.generate_net_heatmap(blocks)
        with open(os.path.join(CHART_DIR, "net_heatmap.html"), "w") as f:
            f.write(html)
        html = energy_charts.generate_daily_import_export_charts(blocks)
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
        file_map = {
            "blocks":            "blocks.json",
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
