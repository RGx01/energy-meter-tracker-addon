"""Export-retention probe (read-only, beta).

Pure report builder over Octopus consumption *boundaries*, so we can measure how
far back each channel (import vs export) actually reaches before building the
historical-import feature. Export commonly reaches less far than import; the gap
decides what the import route can honestly promise.

No I/O here — kraken_api_client.get_consumption_boundary fetches the single
earliest/latest row per channel; this turns those two rows into a JSON-able
report dict. Timestamp parsing is format-agnostic (ISO-with-offset today, but
epoch-ms / epoch-s are handled too, matching statistics_probe).
"""
from __future__ import annotations

from datetime import datetime, timezone


def _to_utc(v):
    """Parse a consumption 'interval_start' value → aware UTC datetime, or None.

    Octopus returns ISO-8601 with offset (e.g. '2024-06-12T13:00:00Z' or
    '...+01:00'); epoch-ms / epoch-s are handled too for robustness."""
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
    if isinstance(v, bool):
        return "bool?"
    if isinstance(v, (int, float)):
        return "epoch_ms" if v > 1e12 else "epoch_s"
    if isinstance(v, str):
        return "iso_string"
    return "none" if v is None else type(v).__name__


def build_consumption_boundary(channel: str, first_row, last_row) -> dict:
    """Characterise one channel's reachable consumption span from its two
    boundary rows (earliest + latest). Pure.

    `channel`   : 'import' | 'export' (label only).
    `first_row` : earliest interval row (order_by='period', page_size=1) or None.
    `last_row`  : latest interval row (order_by='-period', page_size=1) or None.
    Returns {channel, available, earliest?, latest?, span_days?, raw_time_kind?}.
    """
    report = {"channel": channel, "available": bool(first_row)}
    if not first_row:
        report["note"] = "No consumption returned for this meter."
        return report

    raw_start = first_row.get("interval_start")
    report["raw_time_kind"] = _raw_time_kind(raw_start)

    earliest = _to_utc(raw_start)
    latest = _to_utc((last_row or {}).get("interval_start"))
    if earliest is None:
        report["available"] = False
        report["note"] = "Consumption returned but the timestamp could not be parsed."
        return report

    report["earliest"] = earliest.isoformat()
    if latest is not None:
        report["latest"] = latest.isoformat()
        # latest is the START of the last half-hour; the covered span runs to its
        # end, so add one interval (30 min) for a truthful span.
        span_s = (latest - earliest).total_seconds() + 1800.0
        report["span_days"] = round(span_s / 86400.0, 1)
    return report


def export_lag_days(channels: dict):
    """How many days LESS export history reaches than import (earliest→earliest).

    Positive ⇒ export starts later than import (the common case) ⇒ before that
    date only import can be reconstructed. Returns:
      int   — the lag in whole days (0 if export reaches as far or further),
      None  — if either channel is unavailable / unparseable (can't compare).
    """
    imp = channels.get("import") or {}
    exp = channels.get("export") or {}
    if not (imp.get("available") and exp.get("available")):
        return None
    i0 = _to_utc(imp.get("earliest"))
    e0 = _to_utc(exp.get("earliest"))
    if i0 is None or e0 is None:
        return None
    return max(0, int((e0 - i0).total_seconds() // 86400))