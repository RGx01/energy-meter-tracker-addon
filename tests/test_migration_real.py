"""
test_migration_real.py
======================
Migration verification script for real blocks.json data.

This is NOT part of the automated test suite — it runs against actual
data files on disk to verify the migration produces correct results
before the engine switch-over.

Usage:
    python3 test_migration_real.py [--blocks PATH] [--config PATH] [--db PATH]

Defaults:
    --blocks  /data/energy_meter_tracker/blocks.json
    --config  /data/energy_meter_tracker/meters_config.json
    --db      /tmp/blocks_migration_test.db   (deleted after run unless --keep)

Exits 0 if all checks pass, 1 if any check fails.
"""

import argparse
import json
import os
import sys
import random
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(__file__))
from block_store import BlockStore, migrate_json_to_sqlite

# ─────────────────────────────────────────────────────────────────────────────
# ANSI colours for terminal output
# ─────────────────────────────────────────────────────────────────────────────

GREEN  = "\033[92m"
RED    = "\033[91m"
YELLOW = "\033[93m"
CYAN   = "\033[96m"
RESET  = "\033[0m"
BOLD   = "\033[1m"

def ok(msg):    print(f"  {GREEN}✓{RESET} {msg}")
def fail(msg):  print(f"  {RED}✗{RESET} {msg}"); return False
def warn(msg):  print(f"  {YELLOW}!{RESET} {msg}")
def info(msg):  print(f"  {CYAN}·{RESET} {msg}")
def header(msg):print(f"\n{BOLD}{msg}{RESET}")


# ─────────────────────────────────────────────────────────────────────────────
# Checks
# ─────────────────────────────────────────────────────────────────────────────

def check_block_counts(blocks_json: list, store: BlockStore) -> bool:
    header("Block count verification")
    json_count = len(blocks_json)
    db_count   = store.count_blocks()
    info(f"blocks.json: {json_count} blocks")
    info(f"blocks.db:   {db_count} blocks")
    if db_count == json_count:
        ok(f"Block count matches: {db_count}")
        return True
    else:
        return fail(f"Count mismatch: JSON={json_count}, DB={db_count}")


def check_meter_row_counts(blocks_json: list, store: BlockStore) -> bool:
    header("Meter-row count verification")
    # Count expected meter rows from JSON
    json_meter_rows = sum(
        len(b.get("meters", {})) for b in blocks_json
    )
    db_meter_rows = store.count_meter_rows()
    info(f"Expected meter rows (from JSON): {json_meter_rows}")
    info(f"Actual meter rows (in DB):       {db_meter_rows}")
    if db_meter_rows == json_meter_rows:
        ok(f"Meter row count matches: {db_meter_rows}")
        return True
    else:
        return fail(
            f"Meter row count mismatch: expected={json_meter_rows}, got={db_meter_rows}"
        )


def check_date_range(blocks_json: list, store: BlockStore) -> bool:
    header("Date range verification")
    json_starts = sorted(b["start"] for b in blocks_json if b.get("start"))
    if not json_starts:
        warn("No blocks with start timestamps in JSON")
        return True

    json_first = json_starts[0]
    json_last  = json_starts[-1]

    last_block = store.get_last_block()
    db_last    = last_block["start"] if last_block else None

    info(f"JSON first block: {json_first}")
    info(f"JSON last block:  {json_last}")
    info(f"DB last block:    {db_last}")

    passed = True
    if db_last == json_last:
        ok(f"Last block matches: {db_last}")
    else:
        fail(f"Last block mismatch: JSON={json_last}, DB={db_last}")
        passed = False

    # Check first block is in DB
    first_blocks = store.get_blocks_for_range(
        datetime.fromisoformat(json_first),
        datetime.fromisoformat(json_first) + timedelta(seconds=1)
    )
    if first_blocks:
        ok(f"First block present in DB: {json_first}")
    else:
        fail(f"First block missing from DB: {json_first}")
        passed = False

    return passed


def check_config_period(config_json: dict, store: BlockStore,
                        blocks_json: list) -> bool:
    header("Config period verification")
    passed = True

    cp_id = store.get_current_config_period_id()
    if cp_id is None:
        return fail("No config period found in DB")
    ok(f"Config period created: id={cp_id}")

    cp = store.get_config_period(cp_id)

    # Check billing_day
    main_meta = {}
    for m in config_json.get("meters", {}).values():
        if not (m.get("meta") or {}).get("sub_meter"):
            main_meta = m.get("meta") or {}
            break

    expected_bd = int(main_meta.get("billing_day") or 1)
    if cp["billing_day"] == expected_bd:
        ok(f"billing_day correct: {expected_bd}")
    else:
        fail(f"billing_day mismatch: expected={expected_bd}, got={cp['billing_day']}")
        passed = False

    expected_tz = main_meta.get("timezone", "UTC")
    if cp["timezone"] == expected_tz:
        ok(f"timezone correct: {expected_tz}")
    else:
        fail(f"timezone mismatch: expected={expected_tz}, got={cp['timezone']}")
        passed = False

    expected_bm = int(main_meta.get("block_minutes") or 30)
    if cp["block_minutes"] == expected_bm:
        ok(f"block_minutes correct: {expected_bm}")
    else:
        fail(f"block_minutes mismatch: expected={expected_bm}, got={cp['block_minutes']}")
        passed = False

    # Check effective_from is the oldest block
    json_starts = sorted(b["start"] for b in blocks_json if b.get("start"))
    if json_starts:
        expected_from = json_starts[0]
        if cp["effective_from"] == expected_from:
            ok(f"effective_from correct: {expected_from}")
        else:
            fail(f"effective_from mismatch: expected={expected_from}, got={cp['effective_from']}")
            passed = False

    # Check full_config_json round-trips
    stored_config = json.loads(cp["full_config_json"])
    if stored_config == config_json:
        ok("full_config_json matches original")
    else:
        fail("full_config_json does not match original config")
        passed = False

    return passed


def check_spot_sample(blocks_json: list, store: BlockStore,
                      n: int = 20) -> bool:
    header(f"Spot-check {n} random blocks for data fidelity")
    passed = True

    # Filter to blocks that have at least one meter with channels
    valid_blocks = [
        b for b in blocks_json
        if b.get("start") and b.get("meters")
        and any(m.get("channels") for m in b["meters"].values())
    ]

    if not valid_blocks:
        warn("No valid blocks with channel data to spot-check")
        return True

    sample = random.sample(valid_blocks, min(n, len(valid_blocks)))
    failures = []

    for json_block in sample:
        start = json_block["start"]

        # Fetch from DB
        db_blocks = store.get_blocks_for_range(
            datetime.fromisoformat(start),
            datetime.fromisoformat(start) + timedelta(seconds=1)
        )

        if not db_blocks:
            failures.append(f"Block missing in DB: {start}")
            continue

        db_block = db_blocks[0]

        # Check each meter
        for meter_id, json_meter in json_block["meters"].items():
            if meter_id not in db_block["meters"]:
                failures.append(f"{start}: meter {meter_id} missing from DB")
                continue

            db_meter = db_block["meters"][meter_id]

            # Check import channel
            json_imp = (json_meter.get("channels") or {}).get("import") or {}
            db_imp   = (db_meter.get("channels") or {}).get("import") or {}

            for field in ("kwh", "rate", "cost", "read_start", "read_end",
                          "kwh_grid", "kwh_remainder", "cost_remainder"):
                jv = json_imp.get(field)
                dv = db_imp.get(field)
                if jv is None and dv is None:
                    continue
                if jv is None or dv is None:
                    # One is present, other absent — check if it matters
                    if field in ("kwh_grid", "kwh_remainder", "cost_remainder"):
                        continue  # Optional fields
                    failures.append(
                        f"{start}/{meter_id}/import/{field}: "
                        f"JSON={jv} DB={dv}"
                    )
                    continue
                if abs(float(jv) - float(dv)) > 1e-6:
                    failures.append(
                        f"{start}/{meter_id}/import/{field}: "
                        f"JSON={jv:.6f} DB={dv:.6f} diff={abs(jv-dv):.2e}"
                    )

            # Check export channel
            json_exp = (json_meter.get("channels") or {}).get("export") or {}
            db_exp   = (db_meter.get("channels") or {}).get("export") or {}

            for field in ("kwh", "rate", "cost"):
                jv = json_exp.get(field)
                dv = db_exp.get(field)
                if jv is None and dv is None:
                    continue
                if jv is None or dv is None:
                    continue
                if abs(float(jv) - float(dv)) > 1e-6:
                    failures.append(
                        f"{start}/{meter_id}/export/{field}: "
                        f"JSON={jv:.6f} DB={dv:.6f}"
                    )

            # Check standing charge
            json_sc = float(json_meter.get("standing_charge") or 0)
            db_sc   = float(db_meter.get("standing_charge") or 0)
            if abs(json_sc - db_sc) > 1e-6:
                failures.append(
                    f"{start}/{meter_id}/standing_charge: "
                    f"JSON={json_sc:.6f} DB={db_sc:.6f}"
                )

            # Check interpolated flag
            json_interp = bool(json_block.get("interpolated"))
            db_interp   = bool(db_block.get("interpolated"))
            if json_interp != db_interp:
                failures.append(
                    f"{start}: interpolated JSON={json_interp} DB={db_interp}"
                )

    if failures:
        for f in failures[:10]:
            fail(f)
        if len(failures) > 10:
            warn(f"  ... and {len(failures) - 10} more failures")
        passed = False
    else:
        ok(f"All {len(sample)} spot-checked blocks match exactly")

    return passed


def check_local_dates(blocks_json: list, store: BlockStore,
                      config_json: dict) -> bool:
    header("Local date verification")

    main_meta = {}
    for m in config_json.get("meters", {}).values():
        if not (m.get("meta") or {}).get("sub_meter"):
            main_meta = m.get("meta") or {}
            break
    tz_name = main_meta.get("timezone", "UTC")

    from zoneinfo import ZoneInfo
    try:
        tz = ZoneInfo(tz_name)
    except Exception:
        tz = ZoneInfo("UTC")

    # Sample 10 blocks and verify local_date
    valid = [b for b in blocks_json if b.get("start")]
    sample = random.sample(valid, min(10, len(valid)))
    failures = []

    for json_block in sample:
        start = json_block["start"]
        expected_date = (
            datetime.fromisoformat(start)
            .replace(tzinfo=ZoneInfo("UTC"))
            .astimezone(tz)
            .strftime("%Y-%m-%d")
        )
        db_blocks = store.get_blocks_for_date(expected_date)
        found = any(b["start"] == start for b in db_blocks)
        if not found:
            failures.append(
                f"{start} -> expected local_date={expected_date} but not found"
            )

    if failures:
        for f in failures:
            fail(f)
        return False
    else:
        ok(f"Local date correct for all {len(sample)} sampled blocks")
        return True


def check_meter_inventory(blocks_json: list, store: BlockStore) -> bool:
    header("Meter inventory verification")

    # Collect all meter IDs from JSON
    json_meters = set()
    for b in blocks_json:
        for mid in (b.get("meters") or {}).keys():
            json_meters.add(mid)

    info(f"Meters in JSON: {sorted(json_meters)}")

    # Check each meter has blocks in DB
    passed = True
    all_blocks = store.get_all_blocks()
    db_meters = set()
    for b in all_blocks:
        for mid in (b.get("meters") or {}).keys():
            db_meters.add(mid)

    info(f"Meters in DB:   {sorted(db_meters)}")

    for mid in json_meters:
        if mid in db_meters:
            ok(f"Meter present: {mid}")
        else:
            fail(f"Meter missing from DB: {mid}")
            passed = False

    return passed


def check_totals_consistency(blocks_json: list, store: BlockStore) -> bool:
    """
    Verify that summed block totals from JSON and DB are within tolerance.
    This catches systematic rounding or calculation errors.
    """
    header("Totals consistency check")

    json_imp_kwh  = sum(b.get("totals", {}).get("import_kwh", 0) for b in blocks_json)
    json_exp_kwh  = sum(b.get("totals", {}).get("export_kwh", 0) for b in blocks_json)
    json_imp_cost = sum(b.get("totals", {}).get("import_cost", 0) for b in blocks_json)

    # Sum from DB blocks
    db_blocks = store.get_all_blocks()
    db_imp_kwh  = sum(b.get("totals", {}).get("import_kwh", 0) for b in db_blocks)
    db_exp_kwh  = sum(b.get("totals", {}).get("export_kwh", 0) for b in db_blocks)
    db_imp_cost = sum(b.get("totals", {}).get("import_cost", 0) for b in db_blocks)

    info(f"JSON total import:  {json_imp_kwh:.3f} kWh  £{json_imp_cost:.2f}")
    info(f"DB   total import:  {db_imp_kwh:.3f} kWh  £{db_imp_cost:.2f}")
    info(f"JSON total export:  {json_exp_kwh:.3f} kWh")
    info(f"DB   total export:  {db_exp_kwh:.3f} kWh")

    passed = True
    tolerance = 0.001  # 1 Wh tolerance for floating point accumulation

    if abs(json_imp_kwh - db_imp_kwh) <= tolerance:
        ok(f"Import kWh matches: {db_imp_kwh:.3f}")
    else:
        fail(f"Import kWh mismatch: JSON={json_imp_kwh:.6f} DB={db_imp_kwh:.6f}")
        passed = False

    if abs(json_exp_kwh - db_exp_kwh) <= tolerance:
        ok(f"Export kWh matches: {db_exp_kwh:.3f}")
    else:
        fail(f"Export kWh mismatch: JSON={json_exp_kwh:.6f} DB={db_exp_kwh:.6f}")
        passed = False

    if abs(json_imp_cost - db_imp_cost) <= 0.01:  # 1p tolerance
        ok(f"Import cost matches: £{db_imp_cost:.2f}")
    else:
        fail(f"Import cost mismatch: JSON=£{json_imp_cost:.4f} DB=£{db_imp_cost:.4f}")
        passed = False

    return passed


def print_summary(results: dict) -> bool:
    header("Summary")
    all_passed = True
    for name, passed in results.items():
        if passed:
            ok(name)
        else:
            fail(name)
            all_passed = False

    if all_passed:
        print(f"\n{GREEN}{BOLD}All checks passed. Migration is safe to proceed.{RESET}\n")
    else:
        print(f"\n{RED}{BOLD}Some checks failed. Do not proceed with migration.{RESET}\n")

    return all_passed


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Verify blocks.json -> SQLite migration")
    parser.add_argument("--blocks", default="/data/energy_meter_tracker/blocks.json")
    parser.add_argument("--config", default="/data/energy_meter_tracker/meters_config.json")
    parser.add_argument("--db",     default="/tmp/blocks_migration_test.db")
    parser.add_argument("--keep",   action="store_true",
                        help="Keep the DB file after the run")
    parser.add_argument("--sample", type=int, default=20,
                        help="Number of blocks to spot-check (default 20)")
    parser.add_argument("--seed",   type=int, default=None,
                        help="Random seed for reproducible spot-checks")
    args = parser.parse_args()

    if args.seed is not None:
        random.seed(args.seed)

    print(f"\n{BOLD}EMT Migration Verification{RESET}")
    print(f"blocks.json : {args.blocks}")
    print(f"config.json : {args.config}")
    print(f"output DB   : {args.db}")

    # ── Load source files ────────────────────────────────────────────────
    print()
    if not os.path.exists(args.blocks):
        print(f"{RED}Error: blocks.json not found at {args.blocks}{RESET}")
        sys.exit(1)

    if not os.path.exists(args.config):
        print(f"{RED}Error: meters_config.json not found at {args.config}{RESET}")
        sys.exit(1)

    print(f"Loading {args.blocks} ...", end=" ", flush=True)
    with open(args.blocks) as f:
        blocks_json = json.load(f)
    print(f"{GREEN}{len(blocks_json)} blocks{RESET}")

    print(f"Loading {args.config} ...", end=" ", flush=True)
    with open(args.config) as f:
        config_json = json.load(f)
    print(f"{GREEN}OK{RESET}")

    # ── Remove any existing test DB ───────────────────────────────────────
    if os.path.exists(args.db):
        os.remove(args.db)
        print(f"Removed existing test DB: {args.db}")

    # ── Run migration ────────────────────────────────────────────────────
    print(f"\nRunning migration into {args.db} ...")
    store = BlockStore(args.db)
    migrated = migrate_json_to_sqlite(args.blocks, store, config_json)
    print(f"Migrated {migrated} blocks -> {store.count_meter_rows()} meter-rows\n")

    # ── Run verification checks ───────────────────────────────────────────
    results = {}

    results["Block count"]        = check_block_counts(blocks_json, store)
    results["Meter row count"]     = check_meter_row_counts(blocks_json, store)
    results["Date range"]          = check_date_range(blocks_json, store)
    results["Config period"]       = check_config_period(config_json, store, blocks_json)
    results["Meter inventory"]     = check_meter_inventory(blocks_json, store)
    results["Local date accuracy"] = check_local_dates(blocks_json, store, config_json)
    results["Spot sample fidelity"]= check_spot_sample(blocks_json, store, args.sample)
    results["Totals consistency"]  = check_totals_consistency(blocks_json, store)

    store.close()

    # ── Cleanup ───────────────────────────────────────────────────────────
    if not args.keep:
        try:
            os.remove(args.db)
            # Also remove WAL and SHM files if present
            for ext in ("-wal", "-shm"):
                p = args.db + ext
                if os.path.exists(p):
                    os.remove(p)
        except Exception as e:
            warn(f"Could not remove test DB: {e}")
    else:
        info(f"DB kept at: {args.db}")

    # ── Print summary and exit ────────────────────────────────────────────
    all_passed = print_summary(results)
    sys.exit(0 if all_passed else 1)


if __name__ == "__main__":
    main()