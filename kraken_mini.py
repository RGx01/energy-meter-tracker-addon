"""
kraken_mini.py — Octopus Mini near-real-time provisional import layer (Chunk 7).

`api+mini` mode. The Mini exposes the smart meter's live telemetry via GraphQL
(smartMeterTelemetry: readAt / demand / consumption / consumptionDelta /
costDelta). EMT uses the cumulative IMPORT register (`consumption`) read AT each
block boundary to give a provisional import figure immediately — long before
DCC settlement arrives (~a day later). DCC always wins: a Mini-sourced block is
provisional and gets overwritten when settlement lands.

Design (matches the CAD boundary path exactly):
  - At each block boundary, poll telemetry repeatedly (~10–15s cadence) from
    just before the boundary until a point with readAt AFTER the boundary
    arrives, bounded by a timeout (~2 min). This yields a pre-boundary and a
    post-boundary point bracketing the boundary instant.
  - Interpolate the register value AT the boundary with the SAME
    interpolate_value() the CAD path uses → a single boundary read, flagged
    interpolated.
  - Hand that boundary read back to the engine, which applies it to the
    just-closed block as its closing import read and re-finalises (the existing
    block-update path). kWh = this boundary − previous boundary, computed by the
    existing read-delta logic. Mini is therefore "just another import register
    source", not a parallel settlement system.

Robustness: Mini is best-effort. If the API is down, a bracketing pair never
arrives, or telemetry is empty, the boundary read is simply absent — the block
stays on whatever it had and DCC reconciles it later. This module NEVER raises
into the engine; every failure path returns None and logs.

Timestamps: readAt is authoritative (the meter's real record time), offset-aware
from the API; normalised to naive-UTC with the same helper the DCC ingester
uses, so it aligns with block_start strings.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone, timedelta
from typing import Optional, Callable

from kraken_ingester import normalise_to_naive_utc

logger = logging.getLogger("kraken_mini")

# Poll cadence and bound for acquiring the post-boundary point.
_POLL_INTERVAL_S = 12
_POLL_TIMEOUT_S = 120
# Check the shared GraphQL rate-limit budget every Nth boundary (not every one).
_RATE_LIMIT_CHECK_EVERY = 8
_RATE_LIMIT_MIN_REMAINING = 20

# --- CAD-like per-boundary collection (the primary model) -------------------
# Max telemetry calls per boundary before giving up (DCC reconciles later).
_MAX_CALLS_PER_BOUNDARY = 10
# If the latest telemetry readAt lags 'now' by more than this, the feed is
# drifted: defer the next call instead of polling blindly each tick.
_DRIFT_THRESHOLD_S = 60
# Cap how far ahead a drifted call is deferred (don't sleep past the next tick
# burst unreasonably).
_MAX_DEFER_S = 90


def _parse_readat(raw: str) -> Optional[datetime]:
    """Parse a telemetry readAt into a naive-UTC datetime, or None."""
    try:
        naive = normalise_to_naive_utc(raw)           # 'YYYY-MM-DDTHH:MM:SS'
        return datetime.fromisoformat(naive)
    except (ValueError, TypeError, AttributeError):
        return None


class MiniBoundaryReader:
    """Acquires an interpolated import-register reading at a block boundary.

    Stateless across boundaries except for the rate-limit-check counter and the
    device id (resolved once). Injected with the Kraken client and the resolved
    device_id; the engine wires the boundary callback to .read_at_boundary().
    """

    def __init__(self, client, device_id: str):
        self.client = client
        self.device_id = device_id
        self._boundary_count = 0
        # Dedupe repeated telemetry-fetch failures: log the first, suppress the
        # rest (the boundary loop polls ~every 10s), log again only on recovery.
        self._fetch_failing = False

    async def _maybe_check_rate_limit(self) -> bool:
        """Every Nth boundary, check the shared GraphQL budget. Returns False
        (skip this burst) if remaining is known and below the floor."""
        self._boundary_count += 1
        if self._boundary_count % _RATE_LIMIT_CHECK_EVERY != 0:
            return True
        try:
            info = await self.client.get_rate_limit()
        except Exception:
            return True   # unknown → proceed (best-effort)
        if not info:
            return True
        remaining = info.get("remaining")
        if remaining is not None and remaining < _RATE_LIMIT_MIN_REMAINING:
            logger.warning("mini: GraphQL budget low (remaining=%s) — skipping "
                           "this boundary burst", remaining)
            return False
        return True

    def reset_boundary_collection(self, boundary_iso: str) -> None:
        """Begin a fresh collection burst for a new boundary. Resets the
        per-boundary call counter and pacing state."""
        self._cur_boundary = boundary_iso
        self._boundary_calls = 0
        self._next_call_after = None   # datetime | None — pacing gate
        self._got_post = False

    async def collect_into(self, reads: list, boundary_iso: str,
                           now_dt: datetime) -> int:
        """Collect Mini telemetry into a channel `reads` buffer around a boundary,
        each point carrying its OWN readAt timestamp (telemetry is delayed/async).

        Cadence (per spec):
          - Called each engine tick once we're within ~20s before the boundary.
          - Keeps collecting on subsequent ticks until a read TIMESTAMPED at/after
            the boundary lands (the post-boundary point needed to bracket +
            interpolate), or the per-boundary call cap is hit.
          - Adaptive pacing: if telemetry is drifted (latest readAt is >60s behind
            now), the next call is deferred to roughly when the post-boundary
            point should appear, instead of polling blindly every tick.
          - Hard cap of MAX calls per boundary; after that, give up (DCC reconciles).

        Returns the number of new points appended this call. Never raises.
        """
        # (Re)initialise per-boundary state on a new boundary.
        if getattr(self, "_cur_boundary", None) != boundary_iso:
            self.reset_boundary_collection(boundary_iso)

        if self._got_post:
            return 0  # already bracketed this boundary
        if self._boundary_calls >= _MAX_CALLS_PER_BOUNDARY:
            return 0  # per-boundary budget spent
        if self._next_call_after is not None and now_dt < self._next_call_after:
            return 0  # paced: waiting for the deferred call time

        try:
            boundary_dt = datetime.fromisoformat(boundary_iso)
        except (ValueError, TypeError):
            return 0

        win_start = boundary_dt - timedelta(minutes=2)
        self._boundary_calls += 1
        points = await self._fetch(win_start, now_dt + timedelta(seconds=1))
        if not points:
            return 0

        existing_ts = {r.get("ts") for r in reads}
        added = 0
        for p in points:
            if p["ts"] in existing_ts:
                continue
            reads.append({"value": p["value"], "ts": p["ts"]})
            existing_ts.add(p["ts"])
            added += 1
        if added:
            reads.sort(key=lambda r: r["ts"])

        latest_ts = points[-1]["ts"]
        if latest_ts >= boundary_iso:
            self._got_post = True
            logger.info("mini: boundary %s bracketed after %d call(s) "
                        "(latest readAt %s, register %.3f)",
                        boundary_iso, self._boundary_calls, latest_ts,
                        points[-1]["value"])
            return added

        # No post-boundary point yet — measure drift and pace the next call.
        try:
            latest_dt = datetime.fromisoformat(latest_ts)
            drift_s = (now_dt - latest_dt).total_seconds()
        except (ValueError, TypeError):
            drift_s = 0.0
        if drift_s > _DRIFT_THRESHOLD_S:
            self._next_call_after = now_dt + timedelta(
                seconds=min(drift_s, _MAX_DEFER_S))
            logger.info("mini: telemetry drifted %.0fs at boundary %s — "
                        "deferring next call ~%.0fs (call %d/%d)",
                        drift_s, boundary_iso, min(drift_s, _MAX_DEFER_S),
                        self._boundary_calls, _MAX_CALLS_PER_BOUNDARY)
        else:
            self._next_call_after = None  # poll again next tick
        return added

    async def read_at_boundary(self, boundary_iso: str,
                               now_fn: Callable[[], datetime] = None
                               ) -> Optional[dict]:
        """Acquire an interpolated import register read at the boundary.

        boundary_iso: the block end time (naive-UTC ISO, as fired by the engine).
        Returns {value, ts, interpolated} ready to apply as the closing import
        read, or None if a bracketing pair couldn't be obtained (→ DCC will
        reconcile). Never raises.
        """
        try:
            boundary_dt = datetime.fromisoformat(boundary_iso)
        except (ValueError, TypeError):
            logger.warning("mini: bad boundary %r", boundary_iso)
            return None

        if not await self._maybe_check_rate_limit():
            return None

        # Poll until we have a point at/after the boundary, or timeout.
        now_fn = now_fn or (lambda: datetime.now(timezone.utc).replace(tzinfo=None))
        deadline = now_fn() + timedelta(seconds=_POLL_TIMEOUT_S)
        # Fetch window starts a little before the boundary so we always capture
        # a pre-boundary point too.
        win_start = (boundary_dt - timedelta(minutes=2))

        pre: Optional[dict] = None
        post: Optional[dict] = None
        while True:
            points = await self._fetch(win_start, boundary_dt + timedelta(minutes=2))
            pre, post = self._bracket(points, boundary_dt)
            if pre and post:
                break
            if now_fn() >= deadline:
                logger.info("mini: no post-boundary telemetry for %s within "
                            "%ds — deferring to DCC", boundary_iso, _POLL_TIMEOUT_S)
                return None
            await asyncio.sleep(_POLL_INTERVAL_S)

        # Reuse the engine's interpolation (imported lazily to avoid a cycle).
        from engine import interpolate_value
        read = interpolate_value(pre, post, boundary_dt)
        logger.info("mini: boundary %s import register → %.3f (interpolated)",
                    boundary_iso, read["value"])
        return read

    async def _fetch(self, start_dt: datetime, end_dt: datetime) -> list[dict]:
        """Fetch telemetry points as [{value, ts}] sorted by ts, or [] on any
        failure. value = cumulative import register (`consumption`)."""
        try:
            raw = await self.client.get_telemetry(
                self.device_id,
                start_dt.isoformat() + "Z",
                end_dt.isoformat() + "Z",
            )
        except Exception as e:
            # Quiet during a GraphQL 403 cooldown (the client logged it once);
            # otherwise log the FIRST failure and suppress the repeats until
            # telemetry recovers, so a persistent block can't flood the log.
            if type(e).__name__ == "KrakenCooldownError":
                logger.debug("mini: telemetry skipped (GraphQL cooldown)")
            elif not self._fetch_failing:
                logger.warning("mini: telemetry fetch failed: %s "
                               "(suppressing repeats until it recovers)", e)
            self._fetch_failing = True
            return []
        if self._fetch_failing:
            logger.info("mini: telemetry recovered")
            self._fetch_failing = False
        out: list[dict] = []
        for p in raw or []:
            ts = _parse_readat(p.get("readAt"))
            val = p.get("consumption")
            if ts is None or val is None:
                continue
            try:
                # smartMeterTelemetry `consumption` is the cumulative import
                # register in WATT-HOURS (e.g. 30248274 Wh). EMT works in kWh
                # throughout (rates are £/kWh, deltas are kWh), so convert here
                # at the single parse boundary — every downstream consumer
                # (interpolation, boundary delta, storage) is then kWh-native.
                # Without this, a 1 Wh tick was stored as 1 kWh (1000× too big).
                out.append({"value": float(val) / 1000.0,
                            "ts": ts.isoformat()})
            except (TypeError, ValueError):
                continue
        out.sort(key=lambda r: r["ts"])
        return out

    @staticmethod
    def _bracket(points: list[dict], boundary_dt: datetime
                 ) -> tuple[Optional[dict], Optional[dict]]:
        """From sorted points, return (last point at/before boundary, first
        point after boundary), or (None, None)/partial if not bracketed."""
        pre = post = None
        for p in points:
            try:
                pts = datetime.fromisoformat(p["ts"])
            except (ValueError, KeyError):
                continue
            if pts <= boundary_dt:
                pre = p          # keep the latest pre-boundary point
            elif post is None:
                post = p         # first post-boundary point
                break
        return pre, post