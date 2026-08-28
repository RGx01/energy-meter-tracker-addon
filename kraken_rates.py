"""
kraken_rates.py — historical rate schedule for DCC reconcile-time repair.

Chunk 5. When the DCC re-run processes a settled block whose stored rate is
zero/missing (the gap-fill hole, or a cold-start), it needs the *actual*
tariff rate that applied at that block's timestamp. This module fetches a
tariff's rate history from the Kraken REST API ONCE per drain cycle and builds
an in-memory RateSchedule that resolves a rate for any naive-UTC timestamp via
a range search — so the synchronous re-run never makes a network call per block.

Design notes
------------
- Fixed tariffs (e.g. INTELLI-FIX) return ~1 record spanning the term, so the
  schedule is tiny and one fetch covers the whole backfill cheaply.
- Variable tariffs (e.g. OUTGOING-VAR) return sparse change-point records, not
  one per half-hour, so still cheap.
- Rate record valid_from/valid_to are offset-aware (the API publishes in CET /
  with offsets); we normalise to naive UTC with the SAME helper the ingester
  uses, so rate periods align with block_start strings.
- payment_method: the endpoint can return both Direct-Debit and non-DD variants
  for the same period. We prefer DIRECT_DEBIT (the common case) but fall back to
  whatever is present, and never mix — picking one method consistently.
- This module only READS rates. It never writes; the re-run applies them.
"""

from __future__ import annotations

import logging
import asyncio
import bisect
from typing import Optional

from kraken_ingester import normalise_to_naive_utc
from datetime import datetime, timedelta, timezone
try:
    from zoneinfo import ZoneInfo
except Exception:  # pragma: no cover - zoneinfo is stdlib on 3.9+
    ZoneInfo = None

logger = logging.getLogger("kraken_rates")

_PREFERRED_PAYMENT = "DIRECT_DEBIT"


class RateFetchError(Exception):
    """A rate fetch FAILED (transport error, HTTP 4xx/5xx, edge 403) — as opposed
    to succeeding and returning no records. The two must not be conflated: a
    failed fetch is transient and must NOT be reported as an unsupported tariff.
    Only raised when `build_rate_schedule(..., raise_on_error=True)`.
    """


class RateSchedule:
    """An ordered set of (valid_from, valid_to, value) periods in naive-UTC.

    Holds either unit rates OR standing charges — both are "a value (in the
    API's native pence units) that applies over a time period", so one class
    serves both. resolve(ts) returns the value active at naive-UTC ts, or None.
    valid_to is exclusive; an open-ended final period (valid_to None) covers
    everything from its valid_from onward.
    """

    def __init__(self, periods: list[tuple[str, Optional[str], float]],
                 exc_periods: Optional[list[tuple[str, Optional[str], float]]] = None):
        # Sorted by valid_from ascending; each entry (from, to|None, rate).
        self._periods = sorted(periods, key=lambda p: p[0])
        # BL-56: bisect index + a "safe to bisect" flag. Resolution is O(log n)
        # for a non-overlapping/contiguous schedule (every real schedule — Agile,
        # standard-unit-rates, the BL-52 reconstruction, the BL-54 stitch), which
        # matters now the stitch can reach 100k+ periods. An OVERLAPPING schedule
        # (a period's vto extends past the next period's vfrom, or an open-ended
        # period that isn't last) keeps the exact linear "last-overlap-wins" scan.
        self._vfroms = [p[0] for p in self._periods]
        _mono = True
        for _i in range(len(self._periods) - 1):
            _vt = self._periods[_i][1]
            if _vt is None or _vt > self._periods[_i + 1][0]:
                _mono = False
                break
        self._monotonic = _mono
        # BL-23 (4.2): optional parallel EX-VAT schedule, exposed as `.exc`. It's a
        # sibling RateSchedule, so every resolver (resolve / day_rate_bounds /
        # off_peak_rate_near / flat_rate) works on exc for free — no duplicated logic
        # and the inc path is untouched. None when exc rates weren't supplied (standing
        # charges, older callers). The sibling is built with exc_periods=None, so its
        # own `.exc` is None — no recursion.
        self.exc = RateSchedule(exc_periods) if exc_periods else None

    def __len__(self) -> int:
        return len(self._periods)

    def is_empty(self) -> bool:
        return not self._periods

    def resolve(self, ts: str) -> Optional[float]:
        """Return the rate active at naive-UTC ts, or None if uncovered."""
        p = self._periods
        if not p:
            return None
        if self._monotonic:
            # rightmost period with vfrom <= ts; it covers ts unless ts is in a gap
            k = bisect.bisect_right(self._vfroms, ts) - 1
            if k < 0:
                return None
            vfrom, vto, rate = p[k]
            return rate if (vto is None or ts < vto) else None
        # overlapping/degenerate schedule — exact last-overlap-wins linear scan
        match: Optional[float] = None
        for vfrom, vto, rate in p:
            if ts < vfrom:
                break
            if vto is None or ts < vto:
                match = rate
        return match

    def _day_rates(self, ts: str) -> list:
        """Rates of every period overlapping ts's calendar day. Bisects to the day
        for a monotonic schedule (O(log n + periods-in-day)); exact linear scan
        otherwise. Shared by off_peak_rate_near / day_rate_bounds."""
        p = self._periods
        if not p:
            return []
        day = str(ts)[:10]
        day_start, day_end = day + "T00:00:00", day + "T23:59:59"
        rates = []
        if self._monotonic:
            i = bisect.bisect_right(self._vfroms, day_start) - 1
            if i < 0:
                i = 0
            n = len(p)
            while i < n:
                vfrom, vto, rate = p[i]
                if vfrom > day_end:
                    break
                if vto is None or vto > day_start:
                    rates.append(rate)
                i += 1
            return rates
        for vfrom, vto, rate in p:
            if vfrom > day_end:
                break
            if vto is None or vto > day_start:
                rates.append(rate)
        return rates

    def off_peak_rate_near(self, ts: str) -> Optional[float]:
        """The minimum rate among periods active on the same calendar day as ts.

        For a time-of-use tariff (e.g. IOG: 5.493 off-peak / 32.3092 peak) this is
        the off-peak rate — the dispatch overlay uses it to know what rate to
        override an out-of-window dispatched slot to. Tariff-agnostic: off-peak is
        always the cheaper rate. Returns None if the day isn't covered.

        We scan periods overlapping [day_start, day_end) rather than a single
        instant, so we capture both the peak and off-peak rates of that day and
        pick the minimum. Agile (which can go negative) would return its lowest
        rate of the day, which is acceptable — the overlay only applies to
        genuine dispatch slots, and a dispatched Agile slot getting the day's
        cheapest rate is defensible.
        """
        rates = self._day_rates(ts)
        return min(rates) if rates else None

    def day_rate_bounds(self, ts: str):
        """(min_rate, max_rate) over the calendar day of ts — i.e. (off-peak,
        peak) for a banded tariff, or (r, r) for a flat one. (None, None) if the
        day isn't covered. Used to give imported blocks a CLEAN tariff rate keyed
        by their OFF_PEAK/STANDARD label instead of a jittery cost÷kWh."""
        rates = self._day_rates(ts)
        return (min(rates), max(rates)) if rates else (None, None)

    def is_off_peak(self, ts: str, tol: float = 1e-9) -> Optional[bool]:
        """Whether naive-UTC ts falls in the tariff's OFF-PEAK band — the rate
        active at ts equals the day's minimum (off-peak) rate.

        This is what makes the guaranteed home off-peak window (IOG's 23:30–05:30)
        AGREEMENT-DRIVEN for the 6-hour-cap classifier: read from the published
        schedule, never hard-coded. True in the off-peak band, False in a higher
        (peak/day) band, None if the day/ts isn't covered. On a FLAT tariff every
        covered ts reads True (min == max) — a caller that only means banded IOG
        should gate with `day_rate_bounds` (min < max) first. Uses the same
        day-scan as off_peak_rate_near, so a dispatch overlay applied on TOP of the
        base schedule doesn't affect it — this reflects the base tariff window."""
        now = self.resolve(ts)
        op = self.off_peak_rate_near(ts)
        if now is None or op is None:
            return None
        return now <= op + tol

    def flat_rate(self, tol: float = 1e-6):
        """The single rate if this schedule is FLAT — every period carries the
        same value (a fixed-price tariff, e.g. a flat OUTGOING export). None if
        the schedule is empty OR carries more than one distinct rate (a banded
        tariff like IOG, or a rate that changed mid-agreement).

        Used to keep a flat tariff's schedule authoritative even at a tariff-
        TRANSITION seam: a new agreement's published unit rates can begin AFTER
        its valid_from, leaving the early days uncovered, so resolve() /
        day_rate_bounds() read None for them and pricing would drop to a jittery
        cost÷kWh that fragments the one flat rate. A flat schedule has the same
        rate everywhere, so those uncovered days take it too. Returning None for
        any non-flat schedule keeps banded (IOG) pricing on its exact per-slot
        path — this can never stamp a wrong band."""
        if not self._periods:
            return None
        rates = [r for (_vf, _vt, r) in self._periods]
        lo, hi = min(rates), max(rates)
        return lo if (hi - lo) <= tol else None

    def vat_series(self):
        """[(valid_from, vat_rate)] at each period boundary — vat = inc/exc − 1, from
        this (inc) schedule vs its `.exc` sibling. The VAT-calendar learner collapses
        this to change-points, so a VAT holiday (inc/exc stepping 1.05→1.00 at a
        `valid_from`) is picked up automatically. Empty when no exc sibling."""
        if self.exc is None:
            return []
        out = []
        for vfrom, _vto, inc in self._periods:
            exc = self.exc.resolve(vfrom)
            if exc is not None and abs(exc) > 1e-9 and inc is not None:
                out.append((vfrom, float(inc) / float(exc) - 1.0))
        return out

    @classmethod
    def from_api_records(cls, records: list[dict]) -> "RateSchedule":
        """Build from raw standard-unit-rates results, choosing one payment
        method and normalising timestamps to naive UTC.

        Each record: {value_inc_vat, value_exc_vat, valid_from, valid_to,
        payment_method?}. value_inc_vat is used (what the customer pays).
        """
        if not records:
            return cls([])

        # Choose a single payment method to avoid mixing DD / non-DD prices.
        methods = {r.get("payment_method") for r in records
                   if r.get("payment_method")}
        chosen_method = None
        if methods:
            chosen_method = (_PREFERRED_PAYMENT if _PREFERRED_PAYMENT in methods
                             else sorted(m for m in methods if m)[0])

        periods: list[tuple[str, Optional[str], float]] = []
        exc_periods: list[tuple[str, Optional[str], float]] = []
        for r in records:
            if chosen_method and r.get("payment_method") not in (chosen_method, None):
                continue
            val = r.get("value_inc_vat")
            if val is None:
                continue
            vfrom_raw = r.get("valid_from")
            if not vfrom_raw:
                continue
            try:
                vfrom = normalise_to_naive_utc(vfrom_raw)
            except ValueError:
                continue
            vto_raw = r.get("valid_to")
            vto: Optional[str] = None
            if vto_raw:
                try:
                    vto = normalise_to_naive_utc(vto_raw)
                except ValueError:
                    vto = None
            periods.append((vfrom, vto, float(val)))
            _exc = r.get("value_exc_vat")   # BL-23: parallel ex-VAT series (same window)
            if _exc is not None:
                exc_periods.append((vfrom, vto, float(_exc)))
        return cls(periods, exc_periods or None)


def _build_schedule_and_diag(records, product_code, tariff_code):
    """CPU-bound: build the RateSchedule from API records and log diagnostics.

    Runs in a worker thread (via run_in_executor) so a long half-hourly tariff
    history — Agile is ~34k periods over a couple of years — can't run inline on
    the engine event loop during a rate refresh / first-time connect and stall
    the HA WebSocket heartbeat (the observed PONG timeout → setup "Could not
    connect" timeout). Pure CPU, no awaits, safe off-loop. For a large schedule
    the per-period distinct/date-span walk (O(n) + a multi-KB log line) is capped
    to a count, keeping both the CPU and the log cheap.
    """
    sched = RateSchedule.from_api_records(records)
    try:
        _p = sched._periods
        n = len(_p)
        _first = [(x[0], x[1], round(x[2], 4)) for x in _p[:3]]
        _last = [(x[0], x[1], round(x[2], 4)) for x in _p[-3:]]
        if n > 2000:
            logger.info("build_rate_schedule: %s/%s \u2192 %d periods (large; diag capped)",
                        product_code, tariff_code, n)
            logger.info("build_rate_schedule: FIRST=%s LAST=%s", _first, _last)
        else:
            _vals = sorted({round(x[2], 4) for x in _p})
            _span = {}
            for vf, vt, rate in _p:
                k = round(rate, 4)
                if k not in _span:
                    _span[k] = [vf, vf]
                else:
                    _span[k][1] = vf
            _span_s = {k: (v[0][:10], v[1][:10]) for k, v in _span.items()}
            logger.info("build_rate_schedule: %s/%s \u2192 %d periods, distinct=%s",
                        product_code, tariff_code, n, _vals)
            logger.info("build_rate_schedule: FIRST=%s LAST=%s", _first, _last)
            logger.info("build_rate_schedule: rate date-span (earliest..latest "
                        "valid_from per rate)=%s", _span_s)
    except Exception as _de:
        logger.info("build_rate_schedule: %s/%s \u2192 %d periods (diag failed: %s)",
                    product_code, tariff_code, len(sched), _de)
    return sched




# ── BL-54: agreement-stitched import schedule ────────────────────────────────
# The overlay and the reconcile price a HISTORICAL block with the CURRENT import
# schedule (RateSchedule for the current tariff only). For any account whose
# current tariff differs in rate from the tariff that actually applied on a
# block's date, that misprices pre-migration blocks. Stitch ONE schedule across
# ALL agreements — each agreement's rates (via build_rate_schedule, incl. the
# BL-52 window reconstruction) clipped to its [valid_from, valid_to) — so
# resolve(ts) returns the tariff that applied on ts. Historical (closed)
# agreements are immutable and cached by (tariff, from, to).
_AGREEMENT_SCHED_CACHE: dict = {}


def _clip_periods(periods, ag_vf, ag_vt):
    """Clip (vfrom, vto, rate) periods to the agreement window [ag_vf, ag_vt)."""
    out = []
    for pvf, pvt, rate in periods:
        cvf = max(pvf, ag_vf) if ag_vf else pvf
        ends = [x for x in (pvt, ag_vt) if x is not None]
        cvt = min(ends) if ends else None
        if cvt is not None and cvf >= cvt:
            continue
        out.append((cvf, cvt, rate))
    return out


async def build_agreement_stitched_schedule(client, agreements) -> "RateSchedule":
    """Stitch a single import RateSchedule across ALL agreements so resolve(ts)
    prices ts on the tariff that applied then. Reuses build_rate_schedule per
    agreement; closed agreements cached by (tariff, from, to). On total failure
    returns an empty schedule (caller keeps its last-known)."""
    if not agreements:
        return RateSchedule([])
    ttp = getattr(client, "_tariff_to_product_code", None)
    ags = sorted(agreements, key=lambda a: a.get("valid_from") or "")
    inc, exc = [], []
    n_ok = 0
    for ag in ags:
        tc = ag.get("tariff_code")
        if not tc:
            continue
        vf_raw, vt_raw = ag.get("valid_from"), ag.get("valid_to")
        try:
            ag_vf = normalise_to_naive_utc(vf_raw) if vf_raw else None
        except ValueError:
            ag_vf = None
        ag_vt = None
        if vt_raw:
            try:
                ag_vt = normalise_to_naive_utc(vt_raw)
            except ValueError:
                ag_vt = None
        is_open = ag_vt is None
        ckey = (tc, vf_raw, vt_raw)
        ag_sched = None if is_open else _AGREEMENT_SCHED_CACHE.get(ckey)
        if ag_sched is None:
            pc = ttp(tc) if ttp else None
            if not pc:
                logger.warning("stitch: no product for tariff %s — skipped", tc)
                continue
            try:
                ag_sched = await build_rate_schedule(
                    client, pc, tc, period_from=vf_raw, period_to=vt_raw)
            except Exception as e:
                logger.warning("stitch: %s fetch failed (%s) — span uncovered", tc, e)
                continue
            if not is_open and not ag_sched.is_empty():
                _AGREEMENT_SCHED_CACHE[ckey] = ag_sched
        if ag_sched.is_empty():
            continue
        inc.extend(_clip_periods(ag_sched._periods, ag_vf, ag_vt))
        if ag_sched.exc is not None:
            exc.extend(_clip_periods(ag_sched.exc._periods, ag_vf, ag_vt))
        n_ok += 1
    if not inc:
        return RateSchedule([])
    logger.info("build_agreement_stitched_schedule: stitched %d/%d agreement(s) "
                "→ %d periods", n_ok, len(ags), len(inc))
    return RateSchedule(inc, exc or None)


# ── BL-52: reconstruct the windowed periods IOG's old standard-unit-rates gave ──
# The uncapped IOG tariff delivered its day/night split via `standard-unit-rates`
# as time-boundaried periods (night 23:30-05:30 local, day otherwise), so
# RateSchedule.resolve()/is_off_peak() worked for free. The new 4-rate
# IOG-SMB tariff drops standard-unit-rates and returns day/night as two FLAT,
# windowless rates — collapsing resolve() to a single all-day rate. We rebuild the
# missing windowed periods here; nothing downstream changes.
_IOG_OFFPEAK_START = (23, 30)   # local (UK) time the off-peak (night) band begins
_IOG_OFFPEAK_END   = (5, 30)    # local (UK) time it ends (day band begins)
_IOG_TZ_NAME       = "Europe/London"   # IOG is UK-only; the window is UK local time
_IOG_SYNTH_HORIZON_DAYS = 2


def _synthesize_iog_tou_windowed(day_records, night_records,
                                 *, tz_name: str = _IOG_TZ_NAME,
                                 horizon_days: int = _IOG_SYNTH_HORIZON_DAYS,
                                 now: "datetime | None" = None) -> list:
    """Rebuild time-windowed day/night unit-rate records from IOG's flat 4-rate
    day/night buckets, so the schedule has the SAME shape the old
    standard-unit-rates feed produced. Night rate inside 23:30-05:30 local, day
    rate outside; DST-aware; across the span the buckets cover. Returns records in
    the shape RateSchedule.from_api_records consumes. On ANY problem returns the
    plain concatenation (windowless) so pricing degrades to the rates, never to
    nothing."""
    day_records = day_records or []
    night_records = night_records or []
    try:
        if ZoneInfo is None:
            return day_records + night_records
        tz = ZoneInfo(tz_name)
        day_sched = RateSchedule.from_api_records(day_records)
        night_sched = RateSchedule.from_api_records(night_records)
        if day_sched.is_empty() and night_sched.is_empty():
            return []
        froms = []
        for r in day_records + night_records:
            vf = r.get("valid_from")
            if not vf:
                continue
            try:
                froms.append(normalise_to_naive_utc(vf))
            except ValueError:
                pass
        if not froms:
            return day_records + night_records
        start_dt = datetime.fromisoformat(min(froms))
        _now = now or datetime.utcnow()
        horizon = _now.replace(microsecond=0) + timedelta(days=max(1, horizon_days))
        # 05:30 (day begins) + 23:30 (night begins) LOCAL boundaries, day by day.
        # Both are clear of the 01:00-02:00 DST fold/gap, so no ambiguous times.
        first_local = ((start_dt.replace(tzinfo=timezone.utc)).astimezone(tz).date()
                       - timedelta(days=1))
        bounds = []            # (naive-UTC datetime, band)
        d, guard = first_local, 0
        while guard < 4000:    # safety cap (~5.5 yr of day/night boundaries)
            guard += 1
            day_begin = datetime(d.year, d.month, d.day,
                                 _IOG_OFFPEAK_END[0], _IOG_OFFPEAK_END[1], tzinfo=tz)
            night_begin = datetime(d.year, d.month, d.day,
                                   _IOG_OFFPEAK_START[0], _IOG_OFFPEAK_START[1], tzinfo=tz)
            bounds.append((day_begin.astimezone(timezone.utc).replace(tzinfo=None), "day"))
            bounds.append((night_begin.astimezone(timezone.utc).replace(tzinfo=None), "night"))
            d = d + timedelta(days=1)
            probe = (datetime(d.year, d.month, d.day, tzinfo=tz)
                     .astimezone(timezone.utc).replace(tzinfo=None))
            if probe > horizon:
                break
        bounds.sort(key=lambda b: b[0])
        out = []
        for i in range(len(bounds) - 1):
            vf, band = bounds[i]
            vt = bounds[i + 1][0]
            if vt <= start_dt:          # wholly before the agreement start
                continue
            if vf >= horizon:
                break
            src = day_sched if band == "day" else night_sched
            inc = src.resolve(vf.isoformat())
            if inc is None:             # one bucket empty → use the other
                src = night_sched if band == "day" else day_sched
                inc = src.resolve(vf.isoformat())
            if inc is None:
                continue
            rec = {"value_inc_vat": inc,
                   "valid_from": vf.isoformat(),
                   "valid_to": vt.isoformat(),
                   "payment_method": None}
            exc = src.exc.resolve(vf.isoformat()) if src.exc is not None else None
            if exc is not None:
                rec["value_exc_vat"] = exc
            out.append(rec)
        return out or (day_records + night_records)
    except Exception as e:
        logger.warning("_synthesize_iog_tou_windowed: failed (%s) — falling back "
                       "to windowless day/night rates", e)
        return day_records + night_records


async def build_rate_schedule(
    client, product_code: str, tariff_code: str,
    *, period_from: Optional[str] = None, period_to: Optional[str] = None,
    raise_on_error: bool = False,
) -> RateSchedule:
    """Fetch a tariff's unit-rate history and build a RateSchedule.

    One network call (paginated internally) per tariff.

    By default returns an empty schedule on ANY failure, so pricing/drain callers
    degrade to 'no rate found' rather than raising. Callers that must tell a
    *failed fetch* from a *tariff genuinely lacking rates* (e.g. the schedule
    refresh, which drives the "tariff unsupported" banner) pass
    `raise_on_error=True`: a fetch failure then raises RateFetchError, while a
    successful-but-empty result still returns an empty schedule.
    """
    if not product_code or not tariff_code:
        return RateSchedule([])
    try:
        records = await client.get_unit_rates(
            product_code, tariff_code,
            period_from=period_from, period_to=period_to)
    except Exception as e:
        _msg = str(e) or type(e).__name__   # timeouts can stringify empty
        logger.warning("build_rate_schedule: fetch failed for %s/%s: %s",
                       product_code, tariff_code, _msg)
        if raise_on_error:
            raise RateFetchError(
                f"unit-rate fetch failed for {product_code}/{tariff_code}: {_msg}"
            ) from e
        return RateSchedule([])
    if not records:
        # New IOG time-of-use / 6-hour-cap tariff (IOG-SMB-TOU) drops
        # `standard-unit-rates` for split `day` + `night` (general usage) and
        # `ev-device-*` (EV charging) buckets. Reconstruct the base TOU schedule
        # by merging day + night — each carries its own half-hour periods, so
        # concatenating them rebuilds the full day/night schedule. The EV-device
        # rates and the daily 6-hour off-peak cap are deliberately NOT applied
        # yet (OE hasn't confirmed the cap rules — see docs/iog_6hr_cap_design.md);
        # dispatched EV charging keeps using the general off-peak (night) rate via
        # the dispatch overlay in the interim.
        try:
            day = await client.get_unit_rates(
                product_code, tariff_code, rate_type="day-unit-rates",
                period_from=period_from, period_to=period_to)
            night = await client.get_unit_rates(
                product_code, tariff_code, rate_type="night-unit-rates",
                period_from=period_from, period_to=period_to)
            _is_iog = "IOG" in (tariff_code or "").upper()
            if (day or night) and _is_iog:
                # BL-52: rebuild the windowed periods the new 4-rate feed omits.
                records = _synthesize_iog_tou_windowed(day, night)
                logger.info("build_rate_schedule: %s — no standard rates; "
                            "reconstructed %d windowed period(s) from day(%d)+"
                            "night(%d) TOU buckets (IOG 23:30-05:30 local)",
                            tariff_code, len(records), len(day or []), len(night or []))
            else:
                records = (day or []) + (night or [])
                if records:
                    logger.info("build_rate_schedule: %s — no standard rates; built "
                                "from day(%d)+night(%d) TOU buckets (new IOG structure)",
                                tariff_code, len(day or []), len(night or []))
        except Exception as e:
            logger.warning("build_rate_schedule: day/night fallback failed for "
                           "%s/%s: %s", product_code, tariff_code, e)
            if raise_on_error:
                raise RateFetchError(
                    f"day/night rate fetch failed for {product_code}/"
                    f"{tariff_code}: {e}"
                ) from e
    # 4.5.3-A: build the schedule + diagnostics OFF the event loop. Both are pure
    # CPU and O(number of periods); a long half-hourly history (Agile ~34k periods)
    # would otherwise stall the engine loop / HA WebSocket heartbeat here during a
    # rate refresh or first-time connect (the reported setup timeout).
    sched = await asyncio.get_event_loop().run_in_executor(
        None, _build_schedule_and_diag, records, product_code, tariff_code)
    return sched


async def build_ev_device_schedules(
    client, product_code: str, tariff_code: str,
    *, period_from: Optional[str] = None, period_to: Optional[str] = None,
) -> tuple:
    """Fetch the IOG-SMB-TOU EV-device rate buckets — returns
    (off_peak_schedule, peak_schedule) from `ev-device-off-peak-unit-rates` /
    `ev-device-peak-unit-rates`.

    Only the new 6-hour-cap tariff exposes these; any other tariff simply returns
    empty schedules. Additive and non-fatal by design — a fetch failure or a
    missing bucket yields an empty RateSchedule, so the cap classifier just has no
    EV-device rate for that slot and the caller falls back to the general overlay.
    Used to price the EV portion of a dispatched slot (off-peak within the 6-hour
    cap, peak beyond). See docs/iog_6hr_cap_design.md."""
    async def _one(rate_type: str) -> RateSchedule:
        if not product_code or not tariff_code:
            return RateSchedule([])
        try:
            recs = await client.get_unit_rates(
                product_code, tariff_code, rate_type=rate_type,
                period_from=period_from, period_to=period_to)
        except Exception as e:
            logger.warning("build_ev_device_schedules: %s fetch failed for %s/%s: %s",
                           rate_type, product_code, tariff_code, e)
            return RateSchedule([])
        return RateSchedule.from_api_records(recs or [])

    off_peak = await _one("ev-device-off-peak-unit-rates")
    peak = await _one("ev-device-peak-unit-rates")
    return off_peak, peak


async def build_standing_charge_schedule(
    client, product_code: str, tariff_code: str,
    *, period_from: Optional[str] = None, period_to: Optional[str] = None,
) -> RateSchedule:
    """Fetch a tariff's standing-charge history and build a RateSchedule.

    Same shape and caching story as build_rate_schedule, but hits the
    standing-charges endpoint. Values are pence/day (the consumer converts to
    £/day). Returns an empty schedule on any failure.
    """
    if not product_code or not tariff_code:
        return RateSchedule([])
    try:
        records = await client.get_standing_charges(
            product_code, tariff_code,
            period_from=period_from, period_to=period_to)
    except Exception as e:
        logger.warning("build_standing_charge_schedule: fetch failed for %s/%s: %s",
                       product_code, tariff_code, str(e) or type(e).__name__)
        return RateSchedule([])
    sched = RateSchedule.from_api_records(records)
    logger.info("build_standing_charge_schedule: %s/%s → %d periods",
                product_code, tariff_code, len(sched))
    return sched