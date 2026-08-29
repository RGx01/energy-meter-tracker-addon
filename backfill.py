"""Unified backfill planning & dispatch  ("Heal Gaps" + Historical Import).

Historical Import and Missing Data are one engine wearing two hats: *fill a time
window from a source.* This module is the shared decision core for that flow —
**pure logic, with no engine/store imports**, so it can be unit-tested in
isolation and reused by both the web preview endpoint and the engine dispatcher.

The engine / web layer injects the live state (whether a supplier API is
available, the detected gap list, which windows already hold data) and owns the
concrete runners. This module decides three things:

  1. **Gates**   — is this scope+source combination allowed given the setup?
  2. **Dispatch**— which existing runner should carry it out?
  3. **Windows** — of the target windows, which are empty (fill) vs already
                   occupied (overwrite-protection / hand-off to Cost Corrections)?

Design boundaries (see docs/data_management_vision.md):
  * Backfill = fill EMPTIES only. Occupied windows are never silently overwritten;
    they are surfaced as a hand-off to Cost Corrections.
  * The "gaps" scope uses resolve_history_gaps (settled/Measurements fetch for the
    holes). Settlement-retry is for *existing unsettled blocks* — a different flow.
"""

SCOPES = ("whole_history", "gaps", "range")
SOURCES = ("api", "csv")


def evaluate_gates(scope, source, *, api_available, has_blocks, gaps_present,
                   iog_locked=False):
    """Is this scope+source allowed for the user's setup?

    Returns {'allowed': bool, 'reason': str|None, 'message': str, 'warnings': [str]}.
    `reason` is a stable machine code ('no_api', 'no_gaps', 'bad_scope', ...) for
    the UI to branch on; `message` is human-facing.
    """
    if scope not in SCOPES:
        return {"allowed": False, "reason": "bad_scope",
                "message": "Unknown scope '%s'." % scope, "warnings": []}
    if source not in SOURCES:
        return {"allowed": False, "reason": "bad_source",
                "message": "Unknown source '%s'." % source, "warnings": []}

    if source == "api" and not api_available:
        # Finding gaps is a question about our own blocks (always answerable), but
        # the readings can only come from the supplier — so the API source is gated.
        return {"allowed": False, "reason": "no_api",
                "message": ("No supplier API is configured, so readings can't be "
                            "fetched. Use a CSV instead, or add an API key in Settings."),
                "warnings": []}

    # 4.5.6: API import/gap-fill is disabled for any window that overlaps an
    # Intelligent Octopus Go agreement. Octopus removed the per-slot OFF_PEAK
    # label from the Measurements API, so re-fetching an IOG period re-prices
    # out-of-core smart-charge slots at PEAK with no way to tell them from a
    # genuine peak slot — a permanent, silent corruption of the bill. CSV import
    # (from a bill/statement, which is black-and-white) stays allowed; the caller
    # computes the overlap from the agreement history (engine._range_overlaps_iog).
    if source == "api" and iog_locked:
        return {"allowed": False, "reason": "iog_locked",
                "message": ("This period includes time on Intelligent Octopus Go. "
                            "Octopus no longer exposes the per-slot off-peak label, "
                            "so an API import would re-price smart charges at peak and "
                            "permanently corrupt the bill. API import is disabled for "
                            "IOG periods — a CSV import from your bill is still fine."),
                "warnings": []}

    if scope == "gaps" and not gaps_present:
        return {"allowed": False, "reason": "no_gaps",
                "message": "No gaps found — your block history is continuous.",
                "warnings": []}

    warnings = []
    if scope == "whole_history" and has_blocks:
        warnings.append(
            "You already have data. Whole-history backfill only fills windows with "
            "no block yet (before your earliest block, and any interior holes); "
            "existing blocks are left untouched.")
    return {"allowed": True, "reason": None, "message": "", "warnings": warnings}


def dispatch_action(scope, source):
    """Which existing runner carries out an (already-allowed) scope+source combo.

    Returns {'action', 'needs_csv', 'emit_template', 'note'}. The caller maps the
    action name onto the concrete coroutine/function and supplies runtime args:

      whole_history + api -> run_api_import_job      (Measurements bulk, from=None)
      range         + api -> run_api_import_job      (from=range.start; end bound TODO)
      gaps          + api -> resolve_history_gaps    (settled fetch for the holes)
      *             + csv -> apply_csv_import         (gaps+csv emits a gap-scoped
                                                       template to fill first)
    """
    if source == "csv":
        is_gaps = (scope == "gaps")
        return {"action": "apply_csv_import", "needs_csv": True,
                "emit_template": is_gaps,
                "note": ("Download the pre-filled template for these gaps, add "
                         "consumption + cost, then re-upload.") if is_gaps else ""}
    # source == "api"
    if scope == "gaps":
        return {"action": "resolve_history_gaps", "needs_csv": False,
                "emit_template": False, "note": ""}
    # whole_history or range
    return {"action": "run_api_import_job", "needs_csv": False, "emit_template": False,
            "note": ("Custom-range end-bound is not yet supported; the import runs "
                     "from the range start to now.") if scope == "range" else ""}


def classify_windows(target_starts, occupied_starts):
    """Split target block-start ISO strings into fillable (empty) vs occupied.

    Backfill writes only the fillable set. Occupied windows are the
    overwrite-protection set — surfaced as a hand-off to Cost Corrections rather
    than overwritten. Order of `target_starts` is preserved.
    """
    occ = set(occupied_starts or ())
    fill = [s for s in target_starts if s not in occ]
    occupied = [s for s in target_starts if s in occ]
    return {"fill": fill, "occupied": occupied,
            "fill_count": len(fill), "occupied_count": len(occupied)}


def plan_backfill(*, scope, source, api_available, has_blocks,
                  gaps=None, target_starts=None, occupied_starts=None,
                  iog_locked=False):
    """Compose gates + dispatch + window classification into a preview dict.

    Pure — performs no writes. Inputs:
      gaps           list of {start,end,slots} from find_block_gaps (gaps scope).
      target_starts  block-start ISO strings the caller computed for this scope
                     (e.g. the flattened gap slots, or whole-history/range slots).
      occupied_starts block starts that already hold data (overwrite set).

    Returns {'ok': False, 'reason', 'message'} when gated off, else a full plan:
      {'ok': True, scope, source, action, needs_csv, emit_template, note,
       windows:{fill,occupied,...}, gap_runs, warnings, handoff_to_corrections}.
    """
    gaps = gaps or []
    gates = evaluate_gates(scope, source, api_available=api_available,
                           has_blocks=has_blocks, gaps_present=bool(gaps),
                           iog_locked=iog_locked)
    if not gates["allowed"]:
        return {"ok": False, "scope": scope, "source": source,
                "reason": gates["reason"], "message": gates["message"]}

    action = dispatch_action(scope, source)
    windows = classify_windows(target_starts or [], occupied_starts)
    return {
        "ok": True,
        "scope": scope,
        "source": source,
        "action": action["action"],
        "needs_csv": action["needs_csv"],
        "emit_template": action["emit_template"],
        "note": action["note"],
        "windows": windows,
        "gap_runs": gaps,
        "warnings": gates["warnings"],
        # Windows that already hold data are not backfill's job — they belong to
        # Cost Corrections. Never silently overwritten.
        "handoff_to_corrections": windows["occupied"],
    }
