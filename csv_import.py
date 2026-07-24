"""Historical-import CSV core (pure, no I/O).

Parses the Octopus website consumption export (per channel) and derives, from the
*cost* column, the per-block unit rate and off-peak/peak split — the model in
`docs/historical_import_design.md` §B:

  - A single block's `cost ÷ kwh` is only a COARSE signal (Octopus rounds it), so
    it's used to *bucket* blocks, never as the stored rate.
  - The true rate is the AGGREGATE `Σcost ÷ Σkwh` over all blocks in a tier within
    a tariff period — per-block rounding cancels.
  - Banded TOU (IOG/Go) → two rate clusters; the lower is off-peak. A dispatched
    peak half-hour was billed at the cheap rate, so its cost lands it in the
    off-peak cluster automatically — dispatch-aware without time windows.

All money is inc-VAT and in £ (Octopus CSV gives pence → divided by 100 here).
No DB writes, no HTTP — this turns CSV text into a derivation the caller can
confirm and persist. Timestamps normalise to naive UTC (offset mandatory).
"""
from __future__ import annotations

import csv as _csv
import io
from datetime import datetime, timezone
from statistics import mean, pstdev


# ── parsing ──────────────────────────────────────────────────────────────────

def _to_naive_utc(s: str):
    """Offset-aware ISO string → naive-UTC ISO. Returns None if naive/unparseable
    (naive local timestamps are rejected: we can't place them across DST)."""
    if not s:
        return None
    try:
        dt = datetime.fromisoformat(s.strip().replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        return None
    if dt.tzinfo is None:
        return None
    return dt.astimezone(timezone.utc).replace(tzinfo=None).isoformat()


def _find_col(fieldnames, *needles):
    for f in fieldnames or []:
        low = f.strip().lower()
        if all(n in low for n in needles):
            return f
    return None


def parse_octopus_csv(text: str, channel: str) -> dict:
    """Parse an Octopus per-channel consumption export.

    Columns (matched case/space-insensitively): Consumption (kWh),
    Estimated Cost Inc. Tax (p), Standing Charge Inc. Tax (p), Start, End.

    Returns {ok, channel, blocks, errors, row_count}. Each block:
    {channel, block_start, block_end (naive-UTC ISO), kwh, cost, standing}
    with cost/standing in £ inc-VAT. Per-row problems collect into `errors`
    (row number + reason) rather than aborting."""
    reader = _csv.DictReader(io.StringIO(text))
    fn = reader.fieldnames
    col_kwh   = _find_col(fn, "consumption")
    col_cost  = _find_col(fn, "cost")
    col_stand = _find_col(fn, "standing")
    col_start = _find_col(fn, "start")
    col_end   = _find_col(fn, "end")
    if not (col_kwh and col_start):
        return {"ok": False, "channel": channel, "blocks": [], "row_count": 0,
                "errors": [{"row": 0, "reason":
                            "missing required columns (need at least Consumption + Start)"}]}

    blocks, errors, n = [], [], 0
    for i, row in enumerate(reader, start=2):   # row 1 = header
        n += 1
        start = _to_naive_utc(row.get(col_start, ""))
        if start is None:
            errors.append({"row": i, "reason": "Start missing/naive (needs a timezone offset)"})
            continue
        end = _to_naive_utc(row.get(col_end, "")) if col_end else None
        try:
            kwh = float(row.get(col_kwh) or 0)
        except (TypeError, ValueError):
            errors.append({"row": i, "reason": "unparseable consumption"})
            continue
        cost_p = _num(row.get(col_cost)) if col_cost else None
        stand_p = _num(row.get(col_stand)) if col_stand else None
        blocks.append({
            "channel": channel,
            "block_start": start,
            "block_end": end,
            "kwh": kwh,
            "cost": (cost_p / 100.0) if cost_p is not None else None,      # pence → £
            "standing": (stand_p / 100.0) if stand_p is not None else None,
        })
    blocks.sort(key=lambda b: b["block_start"])
    return {"ok": True, "channel": channel, "blocks": blocks,
            "errors": errors, "row_count": n}


def _num(v):
    try:
        return float(str(v).strip())
    except (TypeError, ValueError):
        return None


# ── template / gap-fill CSV generation ───────────────────────────────────────

# Headers match parse_octopus_csv's case/space-insensitive column detection
# (substrings: start, end, consumption, cost, standing) and read like a real
# Octopus website export, so a filled template imports with no mapping.
TEMPLATE_HEADERS = [
    "Start", "End", "Consumption (kWh)",
    "Estimated Cost Inc. Tax (p)", "Standing Charge Inc. Tax (p)",
]


def _iter_slots(from_iso, to_iso, block_minutes, tz_name):
    """Yield (local_start, local_end) datetimes for every block-length slot in the
    half-open UTC range [from, to), rendered in tz_name (offsets DST-correct)."""
    from datetime import datetime, timedelta, timezone
    from zoneinfo import ZoneInfo
    tz = ZoneInfo(tz_name or "UTC")

    def _p(s):
        s = str(s).replace("Z", "").split("+")[0].replace(" ", "T").split(".")[0]
        return datetime.fromisoformat(s).replace(tzinfo=timezone.utc)

    start, end = _p(from_iso), _p(to_iso)
    step = timedelta(minutes=int(block_minutes or 30))
    cur = start
    while cur < end:
        nxt = cur + step
        yield (cur.astimezone(tz), nxt.astimezone(tz))
        cur = nxt


def gap_template_csv(from_iso, to_iso, *, block_minutes=30, tz_name="Europe/London") -> str:
    """Octopus-format CSV pre-filled with Start/End for every half-hour in the gap
    [from, to); the data columns are blank for the user to fill from their bill.
    Timestamps are LOCAL with DST-correct offsets, matching the Octopus export the
    user reads values from."""
    buf = io.StringIO()
    w = _csv.writer(buf)
    w.writerow(TEMPLATE_HEADERS)
    for (a, b) in _iter_slots(from_iso, to_iso, block_minutes, tz_name):
        w.writerow([a.isoformat(), b.isoformat(), "", "", ""])
    return buf.getvalue()


def blank_template_csv(*, block_minutes=30, rows=4,
                       start_iso="2024-07-01T00:00:00+01:00") -> str:
    """A short illustrative template: headers plus a few example rows in the exact
    offset-aware format the importer expects, with sample values so the user can
    see how to fill it. Not tied to any real data."""
    from datetime import datetime, timedelta
    buf = io.StringIO()
    w = _csv.writer(buf)
    w.writerow(TEMPLATE_HEADERS)
    base = datetime.fromisoformat(start_iso)          # keeps the example offset
    step = timedelta(minutes=int(block_minutes or 30))
    samples = [(0.42, 10.5), (0.38, 9.6), (0.51, 12.8), (0.29, 7.3)]
    for i in range(max(1, int(rows))):
        a = base + step * i
        b = a + step
        kwh, cost = samples[i % len(samples)]
        stand = "42.0" if i == 0 else ""            # standing is once-per-day
        w.writerow([a.isoformat(), b.isoformat(), kwh, cost, stand])
    return buf.getvalue()


# ── rate derivation ──────────────────────────────────────────────────────────

def _cluster(values, rel_gap):
    """1-D clustering of sorted rates: start a new cluster when the jump from the
    previous value exceeds rel_gap × previous. Returns list of clusters (lists).
    IOG's two tiers (~7p vs ~25p) split cleanly; Agile's spread makes many."""
    vals = sorted(v for v in values if v is not None and v > 0)
    if not vals:
        return []
    clusters = [[vals[0]]]
    for v in vals[1:]:
        prev = clusters[-1][-1]
        if prev > 0 and (v - prev) > rel_gap * prev:
            clusters.append([v])
        else:
            clusters[-1].append(v)
    return clusters


def _confidence(rates) -> float:
    """0..1 from block count (more = better) and spread (tighter = better)."""
    n = len(rates)
    if n == 0:
        return 0.0
    m = mean(rates)
    cv = (pstdev(rates) / m) if (n > 1 and m) else 0.0
    spread_score = 1.0 / (1.0 + cv * 20.0)     # cv 0 → 1.0, cv 5% → ~0.5
    count_score = min(1.0, n / 48.0)           # a full day of HH → 1.0
    return round(spread_score * count_score, 2)


def _derive_period(blocks, flags, rel_gap, flat_rel_spread):
    """Derive tiers for one tariff period's energy blocks (kwh>0)."""
    coarse = {b["block_start"]: (b["cost"] / b["kwh"])
              for b in blocks if b.get("cost") is not None and b["kwh"] > 0}
    if not coarse:
        return {"kind": "empty", "tiers": [], "standing_daily": _typical_standing(blocks)}

    vals = list(coarse.values())
    clusters = _cluster(vals, rel_gap)
    lo, hi = min(vals), max(vals)
    flat = (len(clusters) == 1) or (lo > 0 and (hi - lo) / lo <= flat_rel_spread)

    if flat:
        labels = [("flat", clusters_all(clusters))]
        kind = "flat"
    elif len(clusters) == 2:
        labels = [("off_peak", clusters[0]), ("peak", clusters[1])]
        kind = "banded"
    else:
        # Agile / non-banded: every block its own price; no off/peak split.
        for b in blocks:
            r = coarse.get(b["block_start"])
            flags[b["block_start"]] = {"tier": "varies", "rate": r}
        agg = _agg_rate(blocks)
        return {"kind": "non_banded",
                "tiers": [{"label": "varies", "rate": agg["rate"], "kwh": agg["kwh"],
                           "cost": agg["cost"], "block_count": agg["n"],
                           "confidence": _confidence(vals)}],
                "standing_daily": _typical_standing(blocks)}

    centers = [mean(c) for (_lbl, c) in labels]
    tiers = []
    for (label, _c), center in zip(labels, centers):
        members = [b for b in blocks
                   if coarse.get(b["block_start"]) is not None
                   and _nearest(coarse[b["block_start"]], centers) == center]
        agg = _agg_rate(members)
        for b in members:
            flags[b["block_start"]] = {"tier": label, "rate": agg["rate"]}
        tiers.append({"label": label, "rate": agg["rate"], "kwh": agg["kwh"],
                      "cost": agg["cost"], "block_count": agg["n"],
                      "confidence": _confidence([coarse[b["block_start"]] for b in members])})
    return {"kind": kind, "tiers": tiers, "standing_daily": _typical_standing(blocks)}


def clusters_all(clusters):
    return [v for c in clusters for v in c]


def _nearest(v, centers):
    return min(centers, key=lambda c: abs(c - v))


def _agg_rate(blocks) -> dict:
    kwh = sum(b["kwh"] for b in blocks if b.get("cost") is not None and b["kwh"] > 0)
    cost = sum(b["cost"] for b in blocks if b.get("cost") is not None and b["kwh"] > 0)
    n = sum(1 for b in blocks if b.get("cost") is not None and b["kwh"] > 0)
    return {"kwh": round(kwh, 6), "cost": round(cost, 6), "n": n,
            "rate": round(cost / kwh, 6) if kwh > 0 else None}


def _typical_standing(blocks):
    """Sum the per-interval apportioned standing charge per UTC day → £/day, and
    return the median across days (they may vary by tariff period)."""
    per_day: dict = {}
    for b in blocks:
        sc = b.get("standing")
        if sc is None:
            continue
        per_day.setdefault(b["block_start"][:10], 0.0)
        per_day[b["block_start"][:10]] += sc
    if not per_day:
        return None
    dailies = sorted(per_day.values())
    return round(dailies[len(dailies) // 2], 6)   # median day


def derive_rates(blocks, periods=None, *, rel_gap=0.10, flat_rel_spread=0.02) -> dict:
    """Derive per-(tariff-period × tier) rates from parsed blocks.

    `periods` — optional list of (from_iso, to_iso) tariff-period bounds (naive
    UTC, half-open). None ⇒ the whole span is one period. The wizard supplies
    agreement boundaries here so a rate change starts a fresh derivation.

    Returns {periods:[{from,to,kind,tiers,standing_daily}], flags, off_peak_kwh,
    peak_kwh, warnings}. `flags[block_start] = {tier, rate}` — the off/peak flag +
    the aggregate rate to bill that block by (flag, not clock)."""
    energy = [b for b in blocks if (b.get("kwh") or 0) > 0]
    flags: dict = {}
    warnings: list = []

    if periods:
        segs = [{"from": f, "to": t, "blocks": []} for (f, t) in periods]
        for b in energy:
            for s in segs:
                if s["from"] <= b["block_start"] < s["to"]:
                    s["blocks"].append(b)
                    break
            else:
                warnings.append(f"block {b['block_start']} fell outside all tariff periods")
    elif energy:
        segs = [{"from": energy[0]["block_start"], "to": energy[-1]["block_start"],
                 "blocks": energy}]
    else:
        segs = []

    out_periods = []
    for s in segs:
        pr = _derive_period(s["blocks"], flags, rel_gap, flat_rel_spread)
        pr["from"], pr["to"] = s["from"], s["to"]
        out_periods.append(pr)

    off = sum(b["kwh"] for b in energy
              if flags.get(b["block_start"], {}).get("tier") == "off_peak")
    peak = sum(b["kwh"] for b in energy
               if flags.get(b["block_start"], {}).get("tier") == "peak")
    return {"periods": out_periods, "flags": flags,
            "off_peak_kwh": round(off, 6), "peak_kwh": round(peak, 6),
            "warnings": warnings}


def reconcile(blocks, flags, *, tol_pct=1.0, tol_abs=0.05) -> dict:
    """Re-price blocks by their derived (flag, rate) and compare to the CSV cost —
    the sanity check that a period's rates need user input if it diverges.

    Returns {repriced, csv_cost, abs_diff, pct_diff, ok}."""
    repriced = 0.0
    csv_cost = 0.0
    for b in blocks:
        if b.get("cost") is None or (b.get("kwh") or 0) <= 0:
            continue
        csv_cost += b["cost"]
        r = (flags.get(b["block_start"]) or {}).get("rate")
        if r is not None:
            repriced += b["kwh"] * r
    diff = abs(repriced - csv_cost)
    pct = (100.0 * diff / csv_cost) if csv_cost else 0.0
    return {"repriced": round(repriced, 4), "csv_cost": round(csv_cost, 4),
            "abs_diff": round(diff, 4), "pct_diff": round(pct, 3),
            "ok": (diff <= tol_abs or pct <= tol_pct)}