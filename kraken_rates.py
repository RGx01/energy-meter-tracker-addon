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


class RateSchedule:
    """An ordered set of (valid_from, valid_to, value) periods in naive-UTC.

    Holds either unit rates OR standing charges — both are "a value (in the
    API's native pence units) that applies over a time period", so one class
    serves both. resolve(ts) returns the value active at naive-UTC ts, or None.
    valid_to is exclusive; an open-ended final period (valid_to None) covers
    everything from its valid_from onward.
    """

    def __init__(self, periods: list[tuple[str, Optional[str], float]]):
        # Sorted by valid_from ascending; each entry (from, to|None, rate).
        self._periods = sorted(periods, key=lambda p: p[0])

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
        return cls(periods)


async def build_rate_schedule(
    client, product_code: str, tariff_code: str,
    *, period_from: Optional[str] = None, period_to: Optional[str] = None,
) -> RateSchedule:
    """Fetch a tariff's unit-rate history and build a RateSchedule.

    One network call (paginated internally) per tariff. Returns an empty
    schedule on any failure, so the caller degrades to 'no rate found' rather
    than raising into the drain.
    """
    if not product_code or not tariff_code:
        return RateSchedule([])
    try:
        records = await client.get_unit_rates(
            product_code, tariff_code,
            period_from=period_from, period_to=period_to)
    except Exception as e:
        logger.warning("build_rate_schedule: fetch failed for %s/%s: %s",
                       product_code, tariff_code, e)
        return RateSchedule([])
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
                       product_code, tariff_code, e)
        return RateSchedule([])
    sched = RateSchedule.from_api_records(records)
    logger.info("build_standing_charge_schedule: %s/%s → %d periods",
                product_code, tariff_code, len(sched))
    return sched