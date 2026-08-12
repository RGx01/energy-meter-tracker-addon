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
from typing import Optional

from kraken_ingester import normalise_to_naive_utc

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
        match: Optional[float] = None
        for vfrom, vto, rate in self._periods:
            if ts < vfrom:
                break  # periods are sorted; no earlier-starting match remains
            if vto is None or ts < vto:
                match = rate
                # don't break: a later period may also start <= ts (overlap);
                # last one wins, which matches "most recent change takes effect".
        return match

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
        if not self._periods:
            return None
        day = str(ts)[:10]  # YYYY-MM-DD
        day_start = day + "T00:00:00"
        day_end = day + "T23:59:59"
        rates = []
        for vfrom, vto, rate in self._periods:
            # period overlaps the day if it starts before day_end and ends after
            # day_start (or is open-ended)
            if vfrom > day_end:
                break
            if vto is None or vto > day_start:
                rates.append(rate)
        return min(rates) if rates else None

    def day_rate_bounds(self, ts: str):
        """(min_rate, max_rate) over the calendar day of ts — i.e. (off-peak,
        peak) for a banded tariff, or (r, r) for a flat one. (None, None) if the
        day isn't covered. Used to give imported blocks a CLEAN tariff rate keyed
        by their OFF_PEAK/STANDARD label instead of a jittery cost÷kWh."""
        if not self._periods:
            return (None, None)
        day = str(ts)[:10]
        day_start, day_end = day + "T00:00:00", day + "T23:59:59"
        rates = []
        for vfrom, vto, rate in self._periods:
            if vfrom > day_end:
                break
            if vto is None or vto > day_start:
                rates.append(rate)
        return (min(rates), max(rates)) if rates else (None, None)

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
    sched = RateSchedule.from_api_records(records)
    # Diagnostic: surface DISTINCT rate values, plus the FIRST and LAST few
    # periods and the date-span each distinct rate covers. This reveals whether
    # the CURRENT off-peak rate has daily windows reaching "now" or is orphaned
    # in stale early periods (the IOG flat-overnight bug).
    try:
        _p = sched._periods
        _vals = sorted({round(x[2], 4) for x in _p})
        _first = [(x[0], x[1], round(x[2], 4)) for x in _p[:3]]
        _last = [(x[0], x[1], round(x[2], 4)) for x in _p[-3:]]
        # For each distinct rate, the earliest valid_from and latest valid_from
        _span = {}
        for vf, vt, rate in _p:
            k = round(rate, 4)
            if k not in _span:
                _span[k] = [vf, vf]
            else:
                _span[k][1] = vf
        _span_s = {k: (v[0][:10], v[1][:10]) for k, v in _span.items()}
        logger.info("build_rate_schedule: %s/%s → %d periods, distinct=%s",
                    product_code, tariff_code, len(sched), _vals)
        logger.info("build_rate_schedule: FIRST=%s LAST=%s", _first, _last)
        logger.info("build_rate_schedule: rate date-span (earliest..latest "
                    "valid_from per rate)=%s", _span_s)
    except Exception as _de:
        logger.info("build_rate_schedule: %s/%s → %d periods (diag failed: %s)",
                    product_code, tariff_code, len(sched), _de)
    return sched


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