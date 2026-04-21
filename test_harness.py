#!/usr/bin/env python3
"""
Energy Meter Tracker — Chart Test Harness
Cycles through test scenarios, uploading each dataset and opening
the charts page for visual inspection.

Usage:
    python3 test_harness.py --url http://192.168.x.x:8099

Requirements:
    pip install requests
"""

import argparse
import json
import os
import subprocess
import sys
import tempfile
import time
import traceback
import requests
from test_data_generator import generate

LOG_PATH = "test_harness.log"
_log_file = None

def log(msg=""):
    """Print to stdout and write to log file."""
    print(msg)
    if _log_file:
        _log_file.write(str(msg) + "\n")
        _log_file.flush()

# ─────────────────────────────────────────────────────────────
# Test scenarios
# ─────────────────────────────────────────────────────────────

SCENARIOS = [
    {
        "name":           "30min / 90 days / solar / sub-meters",
        "days":           90,
        "block_minutes":  30,
        "scenario":       "solar",
        "sub_meters":     True,
        "gap_day":        None,
    },
    {
        "name":           "30min / 7 days / import only",
        "days":           7,
        "block_minutes":  30,
        "scenario":       "import_only",
        "sub_meters":     False,
        "gap_day":        None,
    },
    {
        "name":           "30min / 1 day / solar",
        "days":           1,
        "block_minutes":  30,
        "scenario":       "solar",
        "sub_meters":     False,
        "gap_day":        None,
    },
    {
        "name":           "30min / 31 days / export only",
        "days":           31,
        "block_minutes":  30,
        "scenario":       "export_only",
        "sub_meters":     False,
        "gap_day":        None,
    },
    {
        "name":           "30min / 14 days / mixed / with gap",
        "days":           14,
        "block_minutes":  30,
        "scenario":       "mixed",
        "sub_meters":     False,
        "gap_day":        5,
    },
    {
        "name":           "15min / 7 days / solar",
        "days":           7,
        "block_minutes":  15,
        "scenario":       "solar",
        "sub_meters":     False,
        "gap_day":        None,
    },
    {
        "name":           "15min / 31 days / mixed / sub-meters",
        "days":           31,
        "block_minutes":  15,
        "scenario":       "mixed",
        "sub_meters":     True,
        "gap_day":        None,
    },
    {
        "name":           "5min / 2 days / solar",
        "days":           2,
        "block_minutes":  5,
        "scenario":       "solar",
        "sub_meters":     False,
        "gap_day":        None,
    },
    {
        "name":           "5min / 7 days / import only",
        "days":           7,
        "block_minutes":  5,
        "scenario":       "import_only",
        "sub_meters":     False,
        "gap_day":        None,
    },
    {
        "name":           "5min / 7 days / mixed / sub-meters",
        "days":           7,
        "block_minutes":  5,
        "scenario":       "mixed",
        "sub_meters":     True,
        "gap_day":        None,
    },
    {
        "name":           "5min / 2 days / export only",
        "days":           2,
        "block_minutes":  5,
        "scenario":       "export_only",
        "sub_meters":     False,
        "gap_day":        None,
    },
    # ── Net bar colour regression (2.6.1) ────────────────────
    # Deterministic marginal_solar scenario — daily export credit just exceeds
    # import cost but not import cost + standing charge. Verifies bars are blue
    # (not grey) above zero when inc. standing charge is enabled.
    {
        "name":           "Net bar colour — 30min / 14 days / marginal solar / standing charge",
        "days":           14,
        "block_minutes":  30,
        "scenario":       "marginal_solar",
        "sub_meters":     False,
        "gap_day":        None,
    },
    # ── CO₂ chart scenarios (2.4.0) ──────────────────────────
    {
        "name":           "CO₂ — 30min / 30 days / solar / sub-meters",
        "days":           30,
        "block_minutes":  30,
        "scenario":       "solar",
        "sub_meters":     True,
        "gap_day":        None,
        "co2":            True,
    },
    {
        "name":           "CO₂ — 30min / 7 days / import only / no sub-meters",
        "days":           7,
        "block_minutes":  30,
        "scenario":       "import_only",
        "sub_meters":     False,
        "gap_day":        None,
        "co2":            True,
    },
    {
        "name":           "CO₂ — 30min / 14 days / export only (all negative net)",
        "days":           14,
        "block_minutes":  30,
        "scenario":       "export_only",
        "sub_meters":     False,
        "gap_day":        None,
        "co2":            True,
    },
    {
        "name":           "CO₂ — 30min / 30 days / mixed / sub-meters / with gap",
        "days":           30,
        "block_minutes":  30,
        "scenario":       "mixed",
        "sub_meters":     True,
        "gap_day":        10,
        "co2":            True,
    },
]

# ─────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────

def upload_blocks(base_url, blocks_path):
    """
    Upload a blocks.db file to the add-on via /api/import.
    blocks_path must be a SQLite DB file (not blocks.json).
    """
    url = f"{base_url}/api/import"
    with open(blocks_path, "rb") as f:
        resp = requests.post(url, files={"blocks": ("blocks.db", f, "application/octet-stream")}, timeout=60)
    resp.raise_for_status()
    return resp.json()


def generate_db(scenario_args, db_path):
    """
    Generate test data and write directly to a SQLite blocks.db file
    via block_store, avoiding the blocks.json round-trip.
    """
    import sys, os
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    from block_store import BlockStore
    from test_data_generator import generate_blocks

    store = BlockStore(db_path)
    has_export = scenario_args.get("scenario") in ("solar", "mixed", "export_only", "marginal_solar")
    store.insert_config_period({"meters": {
        "electricity_main": {
            "meta": {
                "billing_day":    scenario_args["billing_day"],
                "block_minutes":  scenario_args["block_minutes"],
                "timezone":       "Europe/London",
                "currency_symbol": "£",
                "currency_code":  "GBP",
                "site":           "Test Site",
                "postcode_prefix": "DE1",
            },
            "channels": {
                "import": {"read": "sensor.test_import", "rate": "sensor.test_rate"},
                **({"export": {"read": "sensor.test_export", "rate": "sensor.test_export_rate"}} if has_export else {}),
            },
        },
        **({"ev_charger": {"meta": {
            "sub_meter": True, "parent_meter": "electricity_main",
            "device": "Zappi EV Charger",
            "billing_day": scenario_args["billing_day"],
            "block_minutes": scenario_args["block_minutes"],
            "timezone": "Europe/London",
            "currency_symbol": "£", "currency_code": "GBP",
        }}, "house_battery": {"meta": {
            "sub_meter": True, "parent_meter": "electricity_main",
            "device": "Solax Battery",
            "billing_day": scenario_args["billing_day"],
            "block_minutes": scenario_args["block_minutes"],
            "timezone": "Europe/London",
            "currency_symbol": "£", "currency_code": "GBP",
        }}} if scenario_args.get("include_sub_meters") else {})
    }})
    cp_id = store._conn.execute(
        "SELECT id FROM config_periods LIMIT 1").fetchone()["id"]

    blocks = generate_blocks(**scenario_args)
    count = store.append_blocks(blocks)
    store._conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    store.close()
    print(f"  Generated {len(blocks)} blocks ({count} meter-rows) → {db_path}")


def regenerate_charts(base_url):
    url = f"{base_url}/api/charts/regenerate"
    resp = requests.post(url, timeout=60)
    resp.raise_for_status()
    return resp.json()


def open_browser(url):
    """Open URL in default browser if possible, otherwise just print it."""
    try:
        if sys.platform == "darwin":
            subprocess.run(["open", url], check=True)
        elif sys.platform.startswith("linux"):
            subprocess.run(["xdg-open", url], check=True)
        elif sys.platform == "win32":
            subprocess.run(["start", url], shell=True, check=True)
    except (FileNotFoundError, subprocess.CalledProcessError):
        log(f"  → Open in browser: {url}")


def print_banner(text, char="─"):
    width = 60
    log(f"\n{char * width}")
    log(f"  {text}")
    log(f"{char * width}")


# ─────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────

def run(base_url, start_from, heatmap_only, daily_only):
    base_url = base_url.rstrip("/")

    global _log_file
    _log_file = open(LOG_PATH, "w")
    log(f"Test harness started — target: {base_url}")
    log(f"Log: {os.path.abspath(LOG_PATH)}")

    # Safety warning
    log(f"""
⚠️  WARNING
This harness will OVERWRITE blocks.json on the target instance.
ALL EXISTING DATA WILL BE LOST.

Target: {base_url}

Only run this against a development or test instance.
NEVER run against a production installation.
""")
    confirm = input("Type YES to continue: ").strip()
    if confirm != "YES":
        log("Aborted.")
        sys.exit(0)

    # Check connectivity
    try:
        requests.get(f"{base_url}/", timeout=5)
    except Exception as e:
        log(f"❌ Cannot reach {base_url}: {e}")
        sys.exit(1)

    results = []
    total   = len(SCENARIOS)

    for i, scenario in enumerate(SCENARIOS):
        if i < start_from:
            continue

        print_banner(f"Test {i+1}/{total}: {scenario['name']}", "═")

        # Generate dataset directly as blocks.db
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
            tmp_path = tmp.name
        os.unlink(tmp_path)  # generate_db creates the file itself

        log(f"  Generating {scenario['days']} days × {scenario['block_minutes']}min blocks...")
        try:
            generate_db(
                scenario_args={
                    "days":             scenario["days"],
                    "block_minutes":    scenario["block_minutes"],
                    "scenario":         scenario["scenario"],
                    "include_sub_meters": scenario["sub_meters"],
                    "billing_day":      1,
                    "rate":             0.2450,
                    "export_rate":      0.1500,
                    "standing_charge":  scenario.get("standing_charge", 0.5046),
                    "gap_day":          scenario["gap_day"],
                },
                db_path=tmp_path,
            )
        except Exception as e:
            log(f"  ❌ Generation failed: {e}")
            log(traceback.format_exc())
            results.append({"name": scenario["name"], "result": "ERROR", "note": str(e)})
            continue

        # Upload blocks.db
        log("  Uploading blocks.db...")
        try:
            upload_blocks(base_url, tmp_path)
            log("  ✅ Upload OK")
        except Exception as e:
            log(f"  ❌ Upload failed: {e}")
            log(traceback.format_exc())
            results.append({"name": scenario["name"], "result": "ERROR", "note": str(e)})
            continue
        finally:
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)

        # Regenerate charts
        log("  Regenerating charts...")
        try:
            regenerate_charts(base_url)
            log("  ✅ Charts regenerated")
        except Exception as e:
            log(f"  ❌ Regeneration failed: {e}")
            log(traceback.format_exc())
            results.append({"name": scenario["name"], "result": "ERROR", "note": str(e)})
            continue

        # Open browser — CO₂ scenarios go to Usage Stats tab
        if scenario.get("co2"):
            url = f"{base_url}/charts?tab=usage_stats"
            log("  ℹ️  CO₂ scenario: check Usage Stats → CO₂ toggle (kWh / Cost / CO₂ buttons)")
            log("     Verify: net view (single bar), totals view (import above / export below)")
            log("     Verify: data table columns match chart, gCO₂ / kgCO₂ auto-scaling")
        elif heatmap_only:
            url = f"{base_url}/charts/net_heatmap.html"
        elif daily_only:
            url = f"{base_url}/charts/daily_usage.html"
        else:
            url = f"{base_url}/charts"

        log(f"  Opening: {url}")
        open_browser(url)
        time.sleep(1)

        # Wait for human judgement
        log()
        while True:
            choice = input("  Result? [p]ass / [f]ail / [s]kip / [q]uit: ").strip().lower()
            if choice in ("p", "f", "s", "q"):
                break

        if choice == "q":
            log("\nAborted.")
            break
        elif choice == "p":
            note = input("  Note (optional, press Enter to skip): ").strip()
            results.append({"name": scenario["name"], "result": "PASS", "note": note})
        elif choice == "f":
            note = input("  Describe the issue: ").strip()
            results.append({"name": scenario["name"], "result": "FAIL", "note": note})
        elif choice == "s":
            results.append({"name": scenario["name"], "result": "SKIP", "note": ""})

    # Summary
    print_banner("Test Summary", "═")
    passed  = sum(1 for r in results if r["result"] == "PASS")
    failed  = sum(1 for r in results if r["result"] == "FAIL")
    skipped = sum(1 for r in results if r["result"] == "SKIP")
    errors  = sum(1 for r in results if r["result"] == "ERROR")

    for r in results:
        icon = {"PASS": "✅", "FAIL": "❌", "SKIP": "⏭", "ERROR": "💥"}.get(r["result"], "?")
        note = f" — {r['note']}" if r["note"] else ""
        log(f"  {icon} {r['result']:5}  {r['name']}{note}")

    log(f"\n  Passed: {passed}  Failed: {failed}  Skipped: {skipped}  Errors: {errors}")

    # Save results
    results_path = "test_results.json"
    with open(results_path, "w") as f:
        json.dump(results, f, indent=2)
    log(f"\n  Results saved to {results_path}")
    log(f"  Log saved to {os.path.abspath(LOG_PATH)}")
    if _log_file:
        _log_file.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Energy Meter Tracker chart test harness")
    parser.add_argument("--url",         type=str, default="http://localhost:8099",
                        help="Base URL of the add-on (default: http://localhost:8099)")
    parser.add_argument("--start-from",  type=int, default=0,
                        help="Start from scenario index (default: 0)")
    parser.add_argument("--heatmap-only", action="store_true",
                        help="Open heatmap chart directly instead of charts page")
    parser.add_argument("--daily-only",   action="store_true",
                        help="Open daily usage chart directly instead of charts page")
    args = parser.parse_args()

    run(args.url, args.start_from, args.heatmap_only, args.daily_only)