"""Historical-import recorder probe (read-only, beta).

Pure report builder over HA long-term statistics, so we can characterise what is
actually available per sensor before building the historical-import feature:
retention (earliest stat), cadence, gaps, state_class / energy-vs-power, and the
raw timestamp/bucket format (including DST behaviour). No I/O here — ha_client
fetches the rows + metadata; this turns them into a JSON-able report dict.
"""
from __future__ import annotations

from datetime import datetime, timezone
from statistics import median


def _to_utc(v):
    """Parse a statistics 'start'/'end' value → aware UTC datetime, or None.

    HA has returned these as epoch-milliseconds floats (newer cores) or ISO
    strings (older). Handle both so the probe works across versions."""
    if isinstance(v, bool) or v is None:
        return None
    if isinstance(v, (int, float)):
        secs = v / 1000.0 if v > 1e12 else float(v)   # ms vs s
        try:
            return datetime.fromtimestamp(secs, tz=timezone.utc)
        except (OverflowError, OSError, ValueError):
            return None
    if isinstance(v, str):
        try:
            dt = datetime.fromisoformat(v.replace("Z", "+00:00"))
        except ValueError:
            return None
        return dt.astimezone(timezone.utc) if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    return None


def _raw_time_kind(v) -> str:
    """Label the raw timestamp representation (one of the probe's key answers)."""
    if isinstance(v, bool):
        return "bool?"
    if isinstance(v, (int, float)):
        return "epoch_ms" if v > 1e12 else "epoch_s"
    if isinstance(v, str):
        return "iso_string"
    return type(v).__name__


def build_sensor_probe(statistic_id, rows, meta=None, *,
                       expected_period_s: int = 3600) -> dict:
    """Characterise one sensor's long-term statistics.

    `rows`  : list of stat dicts (each with 'start'/'end' + value fields), as
              returned by recorder/statistics_during_period for this id.
    `meta`  : the matching recorder/list_statistic_ids entry (unit_of_measurement,
              has_sum, has_mean, source, name) or None.
    Returns a JSON-able report dict. Pure.
    """
    meta = meta or {}
    rows = rows or []
    report = {
        "statistic_id": statistic_id,
        "found": bool(rows),
        "count": len(rows),
        # list_statistic_ids exposes the unit as statistics_/display_unit_of_measurement;
        # plain unit_of_measurement is the fallback for older cores.
        "unit": (meta.get("statistics_unit_of_measurement")
                 or meta.get("display_unit_of_measurement")
                 or meta.get("unit_of_measurement")),
        "has_sum": bool(meta.get("has_sum")),
        "has_mean": bool(meta.get("has_mean")),
        "source": meta.get("source"),
        "name": meta.get("name"),
    }
    # Energy vs power — decides whether hourly figures are true energy.
    if report["has_sum"]:
        report["value_kind"] = "energy_sum"     # cumulative kWh → hourly sum = real energy
    elif report["has_mean"]:
        report["value_kind"] = "power_mean"      # W mean → energy ≈ mean×period (smears bursts)
    else:
        report["value_kind"] = "unknown"

    if not rows:
        report["note"] = "No long-term statistics returned for this sensor."
        return report

    report["raw_start_kind"] = _raw_time_kind(rows[0].get("start"))
    report["sample_row"] = {k: rows[0].get(k) for k in
                            ("start", "end", "sum", "state", "mean") if k in rows[0]}

    parsed = [(_to_utc(r.get("start")), r) for r in rows]
    parsed = [(t, r) for t, r in parsed if t is not None]
    parsed.sort(key=lambda x: x[0])
    if not parsed:
        report["parsed_timestamps"] = False
        report["note"] = "Statistics returned but timestamps could not be parsed."
        return report
    report["parsed_timestamps"] = True

    earliest, latest = parsed[0][0], parsed[-1][0]
    report["earliest"] = earliest.isoformat()
    report["latest"] = latest.isoformat()
    report["span_days"] = round((latest - earliest).total_seconds() / 86400.0, 1)

    deltas = [(parsed[i + 1][0] - parsed[i][0]).total_seconds()
              for i in range(len(parsed) - 1)]
    if deltas:
        med = median(deltas)
        report["cadence_seconds"] = int(med)
        report["cadence_matches_hourly"] = abs(med - expected_period_s) < 60
        gap_thresh = expected_period_s * 1.5
        gaps = [{"after": parsed[i][0].isoformat(),
                 "before": parsed[i + 1][0].isoformat(),
                 "hours": round(deltas[i] / 3600.0, 1)}
                for i in range(len(deltas)) if deltas[i] > gap_thresh]
        report["gap_count"] = len(gaps)
        report["gaps"] = gaps[:10]

    span_buckets = int(round(
        ((latest - earliest).total_seconds() + expected_period_s) / expected_period_s))
    report["expected_count"] = span_buckets
    report["coverage_pct"] = (round(100.0 * len(parsed) / span_buckets, 1)
                              if span_buckets else None)

    report["dst_samples"] = _dst_samples(parsed)
    return report


def _dst_samples(parsed) -> list:
    """Sample buckets around points where the UK local-time offset changes, to
    expose how the statistics buckets behave across DST (the transition-day
    question). Returns [] when no offset change falls inside the span."""
    try:
        from zoneinfo import ZoneInfo
        tz = ZoneInfo("Europe/London")
    except Exception:
        return []
    out, prev_off = [], None
    for i, (t, _r) in enumerate(parsed):
        off = t.astimezone(tz).utcoffset()
        if prev_off is not None and off != prev_off:
            delta = ((parsed[i][0] - parsed[i - 1][0]).total_seconds() / 3600.0
                     if i > 0 else None)
            out.append({
                "at_utc": t.isoformat(),
                "local_offset_changed_to": str(off),
                "delta_from_prev_hours": round(delta, 2) if delta is not None else None,
            })
        prev_off = off
    return out[:6]