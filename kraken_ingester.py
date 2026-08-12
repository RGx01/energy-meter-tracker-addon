"""
kraken_ingester.py — DCC consumption ingester (Chunk 4a, core only).

Responsibility
--------------
On each poll, fetch DCC-settled half-hourly consumption from the Kraken REST
API for the configured import (and export) meter, and write the settled kWh
into each matching block's `imp_kwh_api` column via
BlockStore.upsert_kraken_block(). That sets `needs_pass2_rerun=1` (when
billing_source='api'), which the engine's _drain_pass2_queue() (Chunk 2b)
later acts on. The ingester itself never re-runs PASS 2 — it only delivers the
settled figure.

This module (4a) is the pure core: it takes an already-constructed API client
and BlockStore, computes the fetch window from persisted state, maps rows to
upserts, and persists progress. It performs NO engine wiring and is never
scheduled here — that is Chunk 4b. All I/O is via the injected client/store,
so the whole thing is unit-tested against mocks with no network.

Dry-run
-------
dry_run=True makes a poll fetch and log a summary but perform NO database
writes (no upsert, no state advance). This is the mode 4b uses for the very
first live poll against a real account, so we can eyeball what the API returns
before trusting it with writes.

Window logic
------------
- First run (no last_poll_utc in kraken_state): fetch back `backfill_days`.
- Subsequent runs: fetch from last_poll_utc minus a re-check overlap (settled
  figures can arrive/adjust a little late), up to now.
- After a successful (non-dry-run) poll, last_poll_utc advances to the latest
  interval actually seen (not "now"), so a quiet API never skips a gap.

Timestamp normalisation
-----------------------
block_start is stored as NAIVE UTC ISO (e.g. '2026-05-01T00:30:00'); the
consumption API returns interval_start with an offset ('...Z' or '+01:00').
normalise_to_naive_utc() converts the latter to the former so rows match
blocks. This is the single most correctness-critical mapping in the ingester.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

logger = logging.getLogger("kraken_ingester")

_STATE_LAST_POLL = "last_poll_utc"
# Re-fetch this far before last_poll on incremental runs, to catch late or
# adjusted DCC settlements that landed just behind the previous cursor.
_RECHECK_OVERLAP = timedelta(hours=6)


def normalise_to_naive_utc(ts: str) -> str:
    """Convert an API interval timestamp to naive-UTC ISO matching block_start.

    Accepts 'Z' suffix or numeric offset; returns 'YYYY-MM-DDTHH:MM:SS' in UTC
    with tzinfo stripped. Raises ValueError on an unparseable string (caller
    skips that row).
    """
    s = ts.strip()
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is not None:
        dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
    return dt.isoformat()


class KrakenIngester:
    """DCC consumption ingester core.

    Parameters
    ----------
    client : KrakenAPIClient   (or any object exposing get_consumption)
    store  : BlockStore
    import_mpan, import_serial : identifiers for the import meter
    export_mpan, export_serial : optional export meter
    main_meter_id : EMT meter_id the import figure belongs to
    billing_source : 'api' | 'cad' — passed through to upsert_kraken_block
    backfill_days : how far back the first poll reaches
    drift_block_percent : per-block drift threshold for needs_review
    """

    def __init__(
        self,
        client: Any,
        store: Any,
        *,
        import_mpan: str,
        import_serial: str,
        export_mpan: Optional[str] = None,
        export_serial: Optional[str] = None,
        main_meter_id: str = "electricity_main",
        billing_source: str = "api",
        backfill_days: int = 7,
        drift_block_percent: float = 2.0,
        drift_min_kwh: float = 0.05,
        backfill_missing: bool = True,
    ):
        self.client = client
        self.store = store
        self.import_mpan = import_mpan
        self.import_serial = import_serial
        self.export_mpan = export_mpan
        self.export_serial = export_serial
        self.main_meter_id = main_meter_id
        self.billing_source = billing_source
        # BL-8: materialise blocks that don't exist (outage backfill).
        self.backfill_missing = backfill_missing
        self.backfill_days = backfill_days
        self.drift_block_percent = drift_block_percent
        self.drift_min_kwh = drift_min_kwh

    # ── window ────────────────────────────────────────────────────────────
    def _compute_window(self, now: Optional[datetime] = None) -> tuple[str, str]:
        """Return (period_from, period_to) as 'Z'-suffixed UTC ISO strings.

        The window normally slides forward from the last poll, but it is also
        extended BACK to the oldest block still awaiting DCC settlement on ANY
        channel (import or export). Export has no live source on a Mini / no-export-
        sensor setup, so its DCC figure lags import by days; without this the lagging
        export would only be chased by the once-a-day settlement sweep, leaving a
        recent day showing import-but-no-export until then. Anchoring here makes every
        poll re-cover the lag so it fills within a cycle. Self-bounding: the window
        only widens when something is genuinely unsettled, and is floored at the
        backfill horizon so a stuck-old block can't blow it up (blocks past the
        horizon are finalised-from-CAD and drop out of "unsettled")."""
        now = now or datetime.now(timezone.utc)
        if now.tzinfo is None:
            now = now.replace(tzinfo=timezone.utc)
        last = self.store.get_kraken_state(_STATE_LAST_POLL)
        if last:
            try:
                last_dt = datetime.fromisoformat(last)
                if last_dt.tzinfo is None:
                    last_dt = last_dt.replace(tzinfo=timezone.utc)
                start_dt = last_dt - _RECHECK_OVERLAP
            except ValueError:
                start_dt = now - timedelta(days=self.backfill_days)
        else:
            start_dt = now - timedelta(days=self.backfill_days)
        # Chase lagging settlement (esp. export): pull the start back to the oldest
        # unsettled block, floored at the backfill horizon.
        try:
            oldest = self.store.get_oldest_unsettled_block_start()
            if oldest:
                oldest_dt = datetime.fromisoformat(str(oldest))
                if oldest_dt.tzinfo is None:
                    oldest_dt = oldest_dt.replace(tzinfo=timezone.utc)
                floor_dt = now - timedelta(days=self.backfill_days)
                start_dt = min(start_dt, max(oldest_dt, floor_dt))
        except Exception:
            pass          # never let the unsettled probe break the poll window
        return (self._z(start_dt), self._z(now))

    @staticmethod
    def _z(dt: datetime) -> str:
        """Format as '...Z' UTC ISO (what the API expects for period_from/to)."""
        if dt.tzinfo is not None:
            dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
        return dt.isoformat(timespec="seconds") + "Z"

    # ── poll ──────────────────────────────────────────────────────────────
    async def poll(self, *, dry_run: bool = False,
                   now: Optional[datetime] = None,
                   window: Optional[tuple[str, str]] = None,
                   advance_cursor: bool = True) -> dict:
        """Run one ingest cycle.

        window: if given, an explicit (period_from, period_to) to fetch instead
        of the last_poll-derived sliding window. Used by the user-triggered
        "retry settlement" action to re-fetch an arbitrary historical span
        (oldest-unsettled → now) so late/old DCC gaps can settle on demand.

        advance_cursor: when False, last_poll_utc is NOT updated even on a real
        poll. The retry path sets this False so re-fetching an old span doesn't
        rewind the normal incremental cursor (which would make the next regular
        poll redundantly re-fetch everything since the old date).

        Returns a summary dict:
            {
              "window": (from, to),
              "import_rows": int, "export_rows": int,
              "stored": int, "skipped_no_block": int, "flagged_review": int,
              "latest_interval": str | None,
              "dry_run": bool,
              "errors": [str, ...],
            }
        Never raises for per-row issues; a fetch-level failure is captured in
        errors and the cycle returns what it managed.
        """
        period_from, period_to = window if window else self._compute_window(now)
        summary: dict[str, Any] = {
            "window": (period_from, period_to),
            "import_rows": 0, "export_rows": 0,
            "stored": 0, "export_stored": 0,
            "skipped_no_block": 0, "flagged_review": 0,
            "flagged_interpolated": 0,
            "backfilled": 0,
            "latest_interval": None, "dry_run": dry_run, "errors": [],
            # Diagnostic: first few flagged blocks per channel, with both
            # figures, so a dry-run reveals WHY review fired (units / zeros /
            # real drift) without persisting anything. Capped to keep logs sane.
            "review_samples": [],
        }
        _SAMPLE_CAP = 10

        # Empty/degenerate window (period_from >= period_to) — e.g. a fresh DB
        # with no backfill, where there's nothing to fetch yet. Octopus rejects
        # period_from == period_to with HTTP 400 ("Must not be greater than
        # period_to"), so skip the API call entirely and return an empty
        # summary. Polling resumes normally once there's a real window (a
        # backfill span, or an advanced cursor once data/blocks exist).
        if period_from >= period_to:
            summary["skipped_empty_window"] = True
            logger.info("poll%s: empty window (%s) — nothing to fetch, skipping",
                        " (dry-run)" if dry_run else "", period_from)
            return summary

        # Import meter — the figure that drives billing / re-run.
        try:
            rows = await self.client.get_consumption(
                self.import_mpan, self.import_serial,
                period_from=period_from, period_to=period_to)
        except Exception as e:
            _msg = str(e) or type(e).__name__
            summary["errors"].append(f"import fetch failed: {_msg}")
            logger.warning("poll: import fetch failed: %s", _msg)
            return summary

        summary["import_rows"] = len(rows)
        latest_seen: Optional[str] = None

        for row in rows:
            try:
                iso = row.get("interval_start")
                if iso is None:
                    continue
                block_start = normalise_to_naive_utc(iso)
                kwh = row.get("consumption")
                if latest_seen is None or block_start > latest_seen:
                    latest_seen = block_start
                if dry_run:
                    # Read-only preview: classify (block match + drift) without
                    # writing, so would_store / would_skip / would_review are real.
                    res = self.store.classify_kraken_block(
                        block_start, self.main_meter_id, kwh,
                        billing_source=self.billing_source,
                        drift_block_percent=self.drift_block_percent,
                        drift_min_kwh=self.drift_min_kwh)
                else:
                    res = self.store.upsert_kraken_block(
                        block_start, self.main_meter_id, kwh,
                        source="kraken_api",
                        billing_source=self.billing_source,
                        drift_block_percent=self.drift_block_percent,
                        drift_min_kwh=self.drift_min_kwh)
                status = res.get("status")
                if status == "stored":
                    summary["stored"] += 1
                    if res.get("needs_review"):
                        summary["flagged_review"] += 1
                        if res.get("interpolated"):
                            summary["flagged_interpolated"] += 1
                        if len([s for s in summary["review_samples"]
                                if s["channel"] == "import"]) < _SAMPLE_CAP:
                            summary["review_samples"].append({
                                "channel": "import",
                                "block_start": block_start,
                                "cad_kwh": res.get("cad_kwh"),
                                "dcc_kwh": res.get("settled_kwh"),
                                "drift_pct": res.get("drift_pct"),
                                "interpolated": res.get("interpolated"),
                            })
                elif status == "missing_block":
                    # BL-8: no block exists for this settled interval — an outage
                    # longer than the gap-fill limit. Materialise it from the
                    # authoritative DCC figure; PASS 2 then prices it.
                    if self.backfill_missing and not dry_run and kwh is not None:
                        new_id = self.store.create_backfill_block(
                            block_start, self.main_meter_id, kwh,
                            channel="import", source="kraken_api")
                        if new_id:
                            summary["backfilled"] += 1
                        else:
                            summary["skipped_no_block"] += 1
                    else:
                        summary["skipped_no_block"] += 1
            except ValueError as ve:
                summary["errors"].append(f"row parse: {ve}")
            except Exception as e:
                summary["errors"].append(f"row store: {e}")

        summary["latest_interval"] = latest_seen

        # Export meter — settled figure stored for the export side. Export has
        # no live source in any mode except 'cad', so DCC is the authoritative
        # (or in cad+api, the correcting) export figure. Symmetric to import.
        if self.export_mpan and self.export_serial:
            try:
                exp_rows = await self.client.get_consumption(
                    self.export_mpan, self.export_serial,
                    period_from=period_from, period_to=period_to)
                summary["export_rows"] = len(exp_rows)
                for row in exp_rows:
                    try:
                        iso = row.get("interval_start")
                        if iso is None:
                            continue
                        block_start = normalise_to_naive_utc(iso)
                        kwh = row.get("consumption")
                        if dry_run:
                            res = self.store.classify_kraken_block(
                                block_start, self.main_meter_id, kwh,
                                channel="export",
                                billing_source=self.billing_source,
                                drift_block_percent=self.drift_block_percent,
                                drift_min_kwh=self.drift_min_kwh)
                        else:
                            res = self.store.upsert_kraken_block(
                                block_start, self.main_meter_id, kwh,
                                channel="export",
                                source="kraken_api",
                                billing_source=self.billing_source,
                                drift_block_percent=self.drift_block_percent,
                                drift_min_kwh=self.drift_min_kwh)
                        status = res.get("status")
                        if status == "stored":
                            summary["export_stored"] += 1
                            if res.get("needs_review"):
                                summary["flagged_review"] += 1
                                if res.get("interpolated"):
                                    summary["flagged_interpolated"] += 1
                                if len([s for s in summary["review_samples"]
                                        if s["channel"] == "export"]) < _SAMPLE_CAP:
                                    summary["review_samples"].append({
                                        "channel": "export",
                                        "block_start": block_start,
                                        "cad_kwh": res.get("cad_kwh"),
                                        "dcc_kwh": res.get("settled_kwh"),
                                        "drift_pct": res.get("drift_pct"),
                                        "interpolated": res.get("interpolated"),
                                    })
                        elif status == "missing_block":
                            # BL-8: backfill the export channel likewise.
                            if (self.backfill_missing and not dry_run
                                    and kwh is not None):
                                new_id = self.store.create_backfill_block(
                                    block_start, self.main_meter_id, kwh,
                                    channel="export", source="kraken_api")
                                if new_id:
                                    summary["backfilled"] += 1
                                else:
                                    summary["skipped_no_block"] += 1
                            else:
                                summary["skipped_no_block"] += 1
                    except ValueError as ve:
                        summary["errors"].append(f"export row parse: {ve}")
                    except Exception as e:
                        summary["errors"].append(f"export row store: {e}")
            except Exception as e:
                _msg = str(e) or type(e).__name__   # some timeouts stringify empty
                summary["errors"].append(f"export fetch failed: {_msg}")
                logger.warning("poll: export fetch failed: %s", _msg)

        # Advance the cursor only on a real (non-dry) poll that saw data —
        # and only when advance_cursor is set (the retry path opts out so it
        # doesn't rewind the normal incremental cursor).
        if not dry_run and latest_seen and advance_cursor:
            self.store.set_kraken_state(_STATE_LAST_POLL, latest_seen)

        logger.info(
            "poll%s: window=%s..%s import_rows=%d stored=%d export_rows=%d "
            "export_stored=%d skipped=%d backfilled=%d review=%d errors=%d",
            " (dry-run)" if dry_run else "",
            period_from, period_to, summary["import_rows"], summary["stored"],
            summary["export_rows"], summary["export_stored"],
            summary["skipped_no_block"], summary["backfilled"],
            summary["flagged_review"], len(summary["errors"]))
        if summary["backfilled"]:
            logger.info(
                "poll: BL-8 backfill — created %d block(s) from settled data for "
                "period(s) with no local block (outage recovery). PASS 2 will "
                "price them; they carry no sub-meter split.",
                summary["backfilled"])
        return summary