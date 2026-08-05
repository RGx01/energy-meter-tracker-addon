"""Historical API-import planning (pure, no I/O).

The API route fetches Octopus consumption over the pre-EMT span in bounded
sub-windows, newest→oldest, so a tenure-scale import is resumable and never a
single giant request (see docs/historical_import_build_spec.md Part 2). This
module is the pure planning half: clamp the requested window to what's reachable
(the ~2-year API wall) and to go-live (EMT's oldest block), then split it into
chunks. No HTTP, no DB — the engine feeds it dates and executes the plan.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone


def _naive_utc(s):
    """ISO string (offset-aware or naive) → naive-UTC datetime, or None."""
    if not s:
        return None
    try:
        dt = datetime.fromisoformat(str(s).strip().replace("Z", "+00:00"))
    except ValueError:
        return None
    if dt.tzinfo is not None:
        dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
    return dt


def _iso(dt) -> str:
    return dt.replace(microsecond=0).isoformat()


def clamp_window(requested_from, go_live, *, earliest_reachable=None) -> dict:
    """Clamp the import window to [max(requested_from, earliest_reachable), go_live).

    `go_live` = EMT's oldest existing block (import tiles up to but not into live
    capture). `earliest_reachable` = the API's earliest interval for the channel
    (the ~2-year wall); requests before it are raised to it. Returns
    {ok, from, to, clamped_by, note}. ok=False when the window is empty
    (from >= to) — nothing to import."""
    rf = _naive_utc(requested_from)
    gl = _naive_utc(go_live)
    er = _naive_utc(earliest_reachable)
    if rf is None or gl is None:
        return {"ok": False, "note": "from and go_live are required"}

    start = rf
    clamped_by = []
    if er is not None and er > start:
        start = er
        clamped_by.append("api_retention")   # can't reach before the ~2-year wall
    to = gl                                    # tile up to go-live, exclusive

    if start >= to:
        return {"ok": False, "from": _iso(start), "to": _iso(to),
                "clamped_by": clamped_by,
                "note": "empty window — requested span is already covered by live data "
                        "or predates what the API retains"}
    return {"ok": True, "from": _iso(start), "to": _iso(to),
            "clamped_by": clamped_by,
            "note": "clamped to API retention" if "api_retention" in clamped_by else ""}


def plan_chunks(window_from, window_to, *, chunk_days: int = 60) -> list:
    """Split [window_from, window_to) into contiguous sub-windows, NEWEST→OLDEST.

    Each chunk is at most `chunk_days` long; they abut with no overlap or gap and
    together cover the whole window. Newest-first means the most useful recent
    history lands first and a mid-run failure still leaves a contiguous recent
    block. Returns [{from, to, days}, ...]; [] for an empty/invalid window."""
    lo_end = _naive_utc(window_from)
    hi = _naive_utc(window_to)
    if lo_end is None or hi is None or lo_end >= hi:
        return []
    step = timedelta(days=max(1, int(chunk_days)))
    chunks = []
    cursor = hi
    while cursor > lo_end:
        lo = cursor - step
        if lo < lo_end:
            lo = lo_end
        chunks.append({"from": _iso(lo), "to": _iso(cursor),
                       "days": round((cursor - lo).total_seconds() / 86400.0, 2)})
        cursor = lo
    return chunks


def plan_import(requested_from, go_live, *, earliest_reachable=None,
                chunk_days: int = 60) -> dict:
    """Clamp + chunk in one step. Returns {ok, window, chunks, chunk_count, note}."""
    w = clamp_window(requested_from, go_live, earliest_reachable=earliest_reachable)
    if not w.get("ok"):
        return {"ok": False, "window": w, "chunks": [], "chunk_count": 0,
                "note": w.get("note")}
    chunks = plan_chunks(w["from"], w["to"], chunk_days=chunk_days)
    return {"ok": True, "window": {"from": w["from"], "to": w["to"],
                                   "clamped_by": w["clamped_by"]},
            "chunks": chunks, "chunk_count": len(chunks), "note": w.get("note")}
