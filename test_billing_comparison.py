"""
Comparison test: verifies that api_billing (SQL aggregation) and
calculate_billing_summary_for_period (block method used by Billing charts)
produce identical results for today, current billing period, and year-to-date.

Run against a real database:
  python3 test_billing_comparison.py /data/energy_meter_tracker/energy_meter.db
"""

import sys
import os
import types
import unittest
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

# Stub energy_engine_io so imports work outside HA
eio = types.ModuleType("energy_engine_io")
eio.load_json = lambda *a, **kw: {}
sys.modules["energy_engine_io"] = eio

# Add repo root to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from block_store import BlockStore
import energy_charts as ec


def get_billing_summary_block_method(store, start, end, cfg=None):
    """
    Reference method: load blocks and use calculate_billing_summary_for_period.
    This is what the Billing chart uses — treat as ground truth.
    """
    blocks = store.get_blocks_for_range(start, end)
    summary = ec.calculate_billing_summary_for_period(blocks, start, end)
    imp_kwh  = sum(v.get("kwh", 0) for v in summary.get("totals", {}).values()
                   if not summary.get("meter_meta", {}).get(
                       next(iter(summary.get("totals", {})), ""), {}).get("is_submeter"))
    # Simpler: use the raw totals from the summary
    total_imp_kwh  = 0.0
    total_imp_cost = 0.0
    total_exp_kwh  = 0.0
    total_exp_cost = 0.0
    for key, t in (summary.get("totals") or {}).items():
        meta = (summary.get("meter_meta") or {}).get(key, {})
        if meta.get("is_submeter"):
            continue
        if "export" in key.lower():
            total_exp_kwh  += t.get("kwh", 0)
            total_exp_cost += abs(t.get("cost", 0))
        else:
            total_imp_kwh  += t.get("kwh", 0)
            total_imp_cost += t.get("cost", 0)
    standing = summary.get("total_standing", 0.0)
    total_cost = round(total_imp_cost + standing - total_exp_cost, 4)
    return {
        "imp_kwh":   round(total_imp_kwh, 4),
        "imp_cost":  round(total_imp_cost, 4),
        "exp_kwh":   round(total_exp_kwh, 4),
        "exp_cost":  round(total_exp_cost, 4),
        "standing":  round(standing, 4),
        "total":     total_cost,
    }


def get_billing_summary_sql_method(store, start, end, tz_name="Europe/London"):
    """
    SQL aggregation method: used by api_billing (Live Power page).
    """
    t = store.get_billing_totals_for_range(start, end)
    total = round(t["imp_cost"] + t["standing"] - t["exp_cost"], 4)
    return {
        "imp_kwh":  t["imp_kwh"],
        "imp_cost": t["imp_cost"],
        "exp_kwh":  t["exp_kwh"],
        "exp_cost": t["exp_cost"],
        "standing": t["standing"],
        "total":    total,
    }


def compare(label, block_result, sql_result, tolerance=0.01):
    """Print comparison between two methods, flag discrepancies."""
    fields = ["imp_kwh", "imp_cost", "exp_kwh", "exp_cost", "standing", "total"]
    ok = True
    lines = [f"\n{'='*60}", f"  {label}", f"{'='*60}"]
    lines.append(f"  {'Field':<12} {'Block method':>14} {'SQL method':>14} {'Diff':>10} {'OK?':>6}")
    lines.append(f"  {'-'*56}")
    for f in fields:
        bv = block_result.get(f, 0)
        sv = sql_result.get(f, 0)
        diff = abs(bv - sv)
        flag = "✓" if diff <= tolerance else "✗ MISMATCH"
        if diff > tolerance:
            ok = False
        lines.append(f"  {f:<12} {bv:>14.4f} {sv:>14.4f} {diff:>10.4f} {flag:>6}")
    lines.append(f"{'='*60}")
    print("\n".join(lines))
    return ok


def run_comparison(db_path, tz_name="Europe/London"):
    store = BlockStore(db_path)
    _tz = ZoneInfo(tz_name)
    now_local = datetime.now(_tz)
    now_naive = now_local.replace(tzinfo=None)

    print(f"\nBilling comparison — {now_local.strftime('%Y-%m-%d %H:%M %Z')}")
    print(f"Database: {db_path}")

    all_ok = True

    # ── Today ──────────────────────────────────────────────────────────
    today_start = now_naive.replace(hour=0, minute=0, second=0, microsecond=0)
    b = get_billing_summary_block_method(store, today_start, now_naive)
    s = get_billing_summary_sql_method(store, today_start, now_naive, tz_name)
    all_ok &= compare("TODAY", b, s)

    # ── Current billing period ─────────────────────────────────────────
    bp_periods = ec.get_billing_periods_from_config_periods(
        store.get_config_periods(), tz=_tz
    )
    today_date = now_local.date()
    period_start = period_end = None
    for bps, bpe in bp_periods:
        if bps.date() <= today_date < bpe.date():
            period_start, period_end = bps, bpe
            break
    if period_start is None and bp_periods:
        period_start, period_end = bp_periods[-1]

    if period_start is not None:
        ps = period_start.replace(tzinfo=None) if period_start.tzinfo else period_start
        label = f"THIS BILL  ({ps.strftime('%d %b')} → {now_local.strftime('%d %b %Y')})"
        b = get_billing_summary_block_method(store, ps, now_naive)
        s = get_billing_summary_sql_method(store, ps, now_naive, tz_name)
        all_ok &= compare(label, b, s)

    # ── Year to date ───────────────────────────────────────────────────
    year_start = now_naive.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
    b = get_billing_summary_block_method(store, year_start, now_naive)
    s = get_billing_summary_sql_method(store, year_start, now_naive, tz_name)
    all_ok &= compare(f"THIS YEAR  (1 Jan → {now_local.strftime('%d %b %Y')})", b, s)

    # ── Standing charge day count audit ────────────────────────────────
    print(f"\n{'='*60}")
    print("  STANDING CHARGE AUDIT — days counted per method")
    print(f"{'='*60}")
    for label, start, end in [
        ("Today",     today_start, now_naive),
        ("This Bill", ps if period_start else year_start, now_naive),
        ("This Year", year_start, now_naive),
    ]:
        # Count distinct local_dates in range (what SQL sees)
        cur = store._conn.execute(
            "SELECT COUNT(DISTINCT local_date) as n FROM blocks WHERE block_start >= ? AND block_start < ?",
            (start.isoformat(), end.isoformat())
        )
        sql_days = cur.fetchone()["n"]

        # Count distinct local days via blocks (what block method sees)
        blocks = store.get_blocks_for_range(start, end)
        block_days = len({
            datetime.fromisoformat(b["start"]).replace(tzinfo=ZoneInfo("UTC"))
            .astimezone(_tz).date()
            for b in blocks if b and b.get("start")
        })

        match = "✓" if sql_days == block_days else "✗ MISMATCH"
        print(f"  {label:<12} SQL local_date days: {sql_days:>4}   Block method days: {block_days:>4}   {match}")

    print(f"\n{'='*60}")
    print(f"  Overall: {'ALL MATCH ✓' if all_ok else 'DISCREPANCIES FOUND ✗'}")
    print(f"{'='*60}\n")
    return all_ok


if __name__ == "__main__":
    db_path = sys.argv[1] if len(sys.argv) > 1 else "/data/energy_meter_tracker/energy_meter.db"
    if not os.path.exists(db_path):
        print(f"Database not found: {db_path}")
        print("Usage: python3 test_billing_comparison.py /path/to/energy_meter.db [timezone]")
        sys.exit(1)
    tz_name = sys.argv[2] if len(sys.argv) > 2 else "Europe/London"
    ok = run_comparison(db_path, tz_name)
    sys.exit(0 if ok else 1)