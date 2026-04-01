#!/usr/bin/env python3
"""
Energy Meter Tracker — Test Data Generator
Produces blocks.json files for testing charts with various configurations.

Usage:
    python3 test_data_generator.py [options]

Examples:
    python3 test_data_generator.py --days 7 --block-minutes 5 --output /tmp/blocks_5min_7days.json
    python3 test_data_generator.py --days 31 --block-minutes 30 --scenario solar --output /tmp/blocks_solar.json
    python3 test_data_generator.py --days 90 --block-minutes 15 --scenario mixed --sub-meters
"""

import json
import argparse
import math
import random
from datetime import datetime, timedelta, timezone

# ─────────────────────────────────────────────────────────────
# Scenario profiles — (hour, kwh_per_block) patterns
# ─────────────────────────────────────────────────────────────

def import_kwh(hour, block_minutes, scenario):
    """Return typical import kWh for a given hour and scenario."""
    t = hour + random.uniform(-0.1, 0.1)  # slight jitter

    if scenario == "import_only":
        # Typical household: low overnight, peak morning/evening
        base = 0.05
        if 6 <= t < 9:
            base = 0.35  # morning peak
        elif 17 <= t < 22:
            base = 0.45  # evening peak
        elif 0 <= t < 6:
            base = 0.08  # overnight low
        return max(0.0, base * block_minutes / 30 + random.uniform(-0.01, 0.01))

    elif scenario == "solar":
        # Solar household: import overnight/morning, export midday
        base = 0.05
        if 6 <= t < 9:
            base = 0.25
        elif 17 <= t < 22:
            base = 0.35
        elif 0 <= t < 6:
            base = 0.08
        elif 10 <= t < 15:
            base = 0.0  # solar covers demand midday
        return max(0.0, base * block_minutes / 30 + random.uniform(-0.005, 0.005))

    elif scenario == "mixed":
        # Mix of import and export throughout day
        base = 0.1
        if 17 <= t < 22:
            base = 0.4
        elif 0 <= t < 6:
            base = 0.06
        return max(0.0, base * block_minutes / 30 + random.uniform(-0.01, 0.01))

    elif scenario == "export_only":
        return 0.0

    return 0.1 * block_minutes / 30


def export_kwh(hour, block_minutes, scenario):
    """Return typical export kWh for a given hour and scenario."""
    t = hour + random.uniform(-0.1, 0.1)

    if scenario == "solar":
        if 9 <= t < 16:
            # Solar export during daylight
            peak = 0.5 * math.sin(math.pi * (t - 9) / 7)
            return max(0.0, peak * block_minutes / 30 + random.uniform(-0.02, 0.02))
        return 0.0

    elif scenario == "export_only":
        if 8 <= t < 17:
            peak = 0.4 * math.sin(math.pi * (t - 8) / 9)
            return max(0.0, peak * block_minutes / 30 + random.uniform(-0.01, 0.01))
        return 0.0

    elif scenario == "mixed":
        if 10 <= t < 15:
            return max(0.0, 0.2 * block_minutes / 30 + random.uniform(-0.01, 0.01))
        return 0.0

    return 0.0


def sub_meter_kwh(hour, block_minutes, device):
    """Return sub-meter kWh for EV charger or battery."""
    t = hour
    if device == "ev_charger":
        # EV charges overnight
        if 1 <= t < 5:
            return max(0.0, 0.8 * block_minutes / 30 + random.uniform(-0.05, 0.05))
        return 0.0
    elif device == "house_battery":
        # Battery charges midday, discharges evening
        if 11 <= t < 14:
            return max(0.0, 0.3 * block_minutes / 30 + random.uniform(-0.02, 0.02))
        return 0.0
    return 0.0


# ─────────────────────────────────────────────────────────────
# Block builder
# ─────────────────────────────────────────────────────────────

def make_block(start_dt, block_minutes, scenario, include_sub_meters,
               import_read, export_read, billing_day, rate, export_rate,
               standing_charge):
    end_dt = start_dt + timedelta(minutes=block_minutes)
    hour = start_dt.hour + start_dt.minute / 60.0

    imp_kwh  = import_kwh(hour, block_minutes, scenario)
    exp_kwh  = export_kwh(hour, block_minutes, scenario)
    imp_cost = imp_kwh * rate
    exp_cost = exp_kwh * export_rate

    new_import_read = import_read + imp_kwh
    new_export_read = export_read + exp_kwh

    meters = {
        "electricity_main": {
            "channels": {
                "import": {
                    "kwh":        imp_kwh,
                    "rate":       rate,
                    "cost":       imp_cost,
                    "read_start": round(import_read, 3),
                    "read_end":   round(new_import_read, 3),
                    "meta": {}
                },
                "export": {
                    "kwh":        exp_kwh,
                    "rate":       export_rate,
                    "cost":       exp_cost,
                    "read_start": round(export_read, 3),
                    "read_end":   round(new_export_read, 3),
                    "meta": {}
                }
            },
            "meta": {
                "billing_day":   billing_day,
                "block_minutes": block_minutes,
                "site":          "Test Site",
                "supplier":      "Test Supplier",
                "timezone":      "Europe/London",
                "type":          "electricity"
            },
            "interpolated":    False,
            "standing_charge": standing_charge if start_dt.hour == 0 and start_dt.minute == 0 else 0.0
        }
    }

    if include_sub_meters:
        ev_kwh = sub_meter_kwh(hour, block_minutes, "ev_charger")
        bat_kwh = sub_meter_kwh(hour, block_minutes, "house_battery")
        sub_total_kwh  = ev_kwh + bat_kwh
        sub_total_cost = (ev_kwh + bat_kwh) * rate

        # kwh_remainder = grid import minus sub-meter consumption (grid-authoritative)
        imp_remainder     = max(0.0, imp_kwh - sub_total_kwh)
        imp_cost_remainder = max(0.0, imp_cost - sub_total_cost)

        meters["electricity_main"]["channels"]["import"]["kwh_total"]      = round(imp_kwh, 6)
        meters["electricity_main"]["channels"]["import"]["kwh_remainder"]  = round(imp_remainder, 6)
        meters["electricity_main"]["channels"]["import"]["cost_remainder"] = round(imp_cost_remainder, 6)

        meters["ev_charger"] = {
            "channels": {
                "import": {
                    "kwh":      ev_kwh,
                    "kwh_grid": ev_kwh,
                    "rate":     rate,
                    "cost":     ev_kwh * rate
                }
            },
            "meta": {
                "device":       "Zappi EV Charger",
                "sub_meter":    True,
                "parent_meter": "electricity_main"
            },
            "interpolated": False
        }
        meters["house_battery"] = {
            "channels": {
                "import": {
                    "kwh":      bat_kwh,
                    "kwh_grid": bat_kwh,
                    "rate":     rate,
                    "cost":     bat_kwh * rate
                }
            },
            "meta": {
                "device":       "Solax Battery",
                "sub_meter":    True,
                "parent_meter": "electricity_main"
            },
            "interpolated": False
        }
    else:
        # No sub-meters — kwh_total equals kwh (no sub-meter subtraction)
        meters["electricity_main"]["channels"]["import"]["kwh_total"]     = round(imp_kwh, 6)
        meters["electricity_main"]["channels"]["import"]["kwh_remainder"] = round(imp_kwh, 6)

    return {
        "start":  start_dt.strftime("%Y-%m-%dT%H:%M:%S"),
        "end":    end_dt.strftime("%Y-%m-%dT%H:%M:%S"),
        "meters": meters,
        "totals": {
            "import_kwh":  imp_kwh,
            "import_cost": imp_cost,
            "export_kwh":  exp_kwh,
            "export_cost": exp_cost
        },
        "interpolated": False
    }, new_import_read, new_export_read


# ─────────────────────────────────────────────────────────────
# Main generator
# ─────────────────────────────────────────────────────────────

def generate(days, block_minutes, scenario, include_sub_meters,
             billing_day, output_path, rate, export_rate,
             standing_charge, gap_day):

    random.seed(42)  # reproducible output

    # Start at midnight UTC N days ago
    now   = datetime.now(timezone.utc).replace(tzinfo=None)
    start = now.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=days - 1)

    blocks        = []
    import_read   = 10000.0
    export_read   = 5000.0
    current       = start
    end_time      = now

    slots_per_day = 1440 // block_minutes
    total_slots   = days * slots_per_day
    done          = 0

    # Optional mid-period rate change
    rate_change_day = days // 2
    rate2 = round(rate * 1.1, 4)  # 10% rate increase mid-period

    while current < end_time:
        day_offset = (current.date() - start.date()).days

        # Skip a day to simulate a gap
        if gap_day is not None and day_offset == gap_day:
            current += timedelta(minutes=block_minutes)
            done += 1
            continue

        # Mid-period rate change
        current_rate = rate2 if day_offset >= rate_change_day else rate

        block, import_read, export_read = make_block(
            current, block_minutes, scenario, include_sub_meters,
            import_read, export_read, billing_day,
            current_rate, export_rate, standing_charge
        )
        blocks.append(block)
        current += timedelta(minutes=block_minutes)
        done += 1

        if done % 1000 == 0:
            pct = done * 100 // total_slots
            print(f"  {pct}% ({done}/{total_slots} blocks)...")

    with open(output_path, 'w') as f:
        json.dump(blocks, f, indent=2)

    print(f"\nGenerated {len(blocks)} blocks → {output_path}")
    print(f"  Scenario:    {scenario}")
    print(f"  Block size:  {block_minutes} min")
    print(f"  Days:        {days}")
    print(f"  Sub-meters:  {include_sub_meters}")
    print(f"  Rate change: day {rate_change_day} ({rate} → {rate2} £/kWh)")
    if gap_day is not None:
        print(f"  Gap day:     day {gap_day}")


# ─────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate test blocks.json for Energy Meter Tracker")
    parser.add_argument("--days",            type=int,   default=7,
                        help="Number of days of data (default: 7)")
    parser.add_argument("--block-minutes",   type=int,   default=30, choices=[5, 15, 30],
                        help="Block size in minutes (default: 30)")
    parser.add_argument("--scenario",        type=str,   default="import_only",
                        choices=["import_only", "solar", "mixed", "export_only"],
                        help="Usage scenario (default: import_only)")
    parser.add_argument("--sub-meters",      action="store_true",
                        help="Include EV charger and battery sub-meters")
    parser.add_argument("--billing-day",     type=int,   default=1,
                        help="Billing period start day of month (default: 1)")
    parser.add_argument("--rate",            type=float, default=0.2450,
                        help="Import rate £/kWh (default: 0.2450)")
    parser.add_argument("--export-rate",     type=float, default=0.1500,
                        help="Export rate £/kWh (default: 0.1500)")
    parser.add_argument("--standing-charge", type=float, default=0.5046,
                        help="Daily standing charge £/day (default: 0.5046)")
    parser.add_argument("--gap-day",         type=int,   default=None,
                        help="Skip this day index to simulate a data gap (optional)")
    parser.add_argument("--output",          type=str,   default="blocks_test.json",
                        help="Output file path (default: blocks_test.json)")
    args = parser.parse_args()

    print(f"\nGenerating test dataset...")
    generate(
        days=args.days,
        block_minutes=args.block_minutes,
        scenario=args.scenario,
        include_sub_meters=args.sub_meters,
        billing_day=args.billing_day,
        output_path=args.output,
        rate=args.rate,
        export_rate=args.export_rate,
        standing_charge=args.standing_charge,
        gap_day=args.gap_day,
    )