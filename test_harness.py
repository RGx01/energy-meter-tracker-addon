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
]

# ─────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────

def upload_blocks(base_url, blocks_path):
    url = f"{base_url}/api/import"
    with open(blocks_path, "rb") as f:
        resp = requests.post(url, files={"blocks": ("blocks.json", f, "application/json")}, timeout=30)
    resp.raise_for_status()
    return resp.json()


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

        # Generate dataset
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as tmp:
            tmp_path = tmp.name

        log(f"  Generating {scenario['days']} days × {scenario['block_minutes']}min blocks...")
        generate(
            days=scenario["days"],
            block_minutes=scenario["block_minutes"],
            scenario=scenario["scenario"],
            include_sub_meters=scenario["sub_meters"],
            billing_day=1,
            output_path=tmp_path,
            rate=0.2450,
            export_rate=0.1500,
            standing_charge=0.5046,
            gap_day=scenario["gap_day"],
        )

        # Upload
        log("  Uploading blocks.json...")
        try:
            upload_blocks(base_url, tmp_path)
            log("  ✅ Upload OK")
        except Exception as e:
            log(f"  ❌ Upload failed: {e}")
            log(traceback.format_exc())
            results.append({"name": scenario["name"], "result": "ERROR", "note": str(e)})
            continue
        finally:
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

        # Open browser
        if heatmap_only:
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