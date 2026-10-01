"""UK VAT-rate calendar for domestic-energy ex-VAT derivation (4.2 BL-23).

Why this exists
---------------
Ex-VAT figures need the VAT *rate* in two narrow places: the fallback for a slot
with no captured ex-VAT (a live/unsettled slot, or an inc-only CSV), and the VAT
row/labels on the bill summary. Hardcoding ``1 / 1.05`` is wrong the moment VAT
isn't 5% (a VAT holiday), so this module resolves the rate from a tiny calendar
instead.

The rate is *statutory* — the domestic fuel & power reduced rate has been 5% since
1997-09-01 (8% before) — set by government with publicly-known effective dates.
There is no suitable HMRC rates API (their platform is transactional MTD, not a
rates reference), so the calendar is:

  * SEEDED with the known statutory history (one entry, stable for ~30 years), and
  * self-maintained by LEARNING boundaries from the tariff's own inc/exc rates
    (Octopus versions ``valid_from``/``valid_to`` on the same tariff, so a VAT
    change shows up as the inc/exc ratio stepping at a date).

It is only ever a FALLBACK/guard. The primary, per-slot source stays the inc/exc
pair itself — Measurements ``cost_excl`` for settled data, tariff ``value_exc_vat``
for live — which is boundary-robust by construction. This module backstops the
inc-only cases and cross-checks that a data-derived rate looks statutory.
"""

# Statutory domestic-energy reduced-rate history: (effective_from YYYY-MM-DD, rate).
# Each date is a UK-local midnight. The 0% from 1 Oct 2026 is seeded rather than left
# to the learner: Octopus applied it by EDITING its open-ended rate records in place
# (a record from 5 Jul 2026 now reads inc == exc), so the tariff no longer says when
# 5% ended — dated from the records, it would be backdated to July. See
# learn_from_records.
SEED = [("1997-09-01", 0.05), ("2026-10-01", 0.0)]
# VAT dates are UK-local; block starts and tariff periods are naive UTC.
TZ_NAME = "Europe/London"
# Domestic supply is only ever one of these; used to snap a noisy derived ratio.
STATUTORY_RATES = (0.0, 0.05, 0.20)
DEFAULT_RATE = 0.05          # sane default for a date before any known entry


def snap_vat(raw):
    """Snap a derived VAT rate (e.g. inc/exc − 1) to the nearest statutory value.

    Returns None for None so callers can distinguish "no signal" from a real 0%.
    """
    if raw is None:
        return None
    return min(STATUTORY_RATES, key=lambda r: abs(r - float(raw)))


def _merged(learned):
    """SEED merged with `learned` [(date, rate), …] — de-duplicated by date, sorted.
    Learned entries win over the seed on a shared date."""
    m = {d: r for d, r in SEED}
    for d, r in (learned or []):
        if d:
            m[str(d)[:10]] = float(r)
    return sorted(m.items())


def local_day(when):
    """The UK-local date (YYYY-MM-DD) of `when`. A bare date is already local; a
    timestamp is naive UTC (or aware) — 2026-09-30T23:00:00 is 1 Oct in BST, the
    first half-hour of a change that starts at local midnight."""
    s = str(when)
    if len(s) <= 10:
        return s[:10]
    from datetime import datetime, timezone
    from zoneinfo import ZoneInfo
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
    except ValueError:
        return s[:10]
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(ZoneInfo(TZ_NAME)).date().isoformat()


def change_points_utc(learned=None):
    """Every boundary of the seed + learned calendar as a naive-UTC timestamp (the
    local midnight it starts at), ascending — where a tariff period must be split."""
    from datetime import datetime, timezone
    from zoneinfo import ZoneInfo
    out = []
    for d, _r in _merged(learned):
        try:
            dt = datetime.fromisoformat(d).replace(tzinfo=ZoneInfo(TZ_NAME))
        except ValueError:
            continue
        out.append(dt.astimezone(timezone.utc).replace(tzinfo=None).isoformat())
    return out


def resolve_vat(date_iso, learned=None):
    """The VAT rate effective at `date_iso`, from the seed + learned boundaries.
    A bare date is taken as UK-local; a timestamp as naive UTC, resolved on its
    UK-local date (`local_day`). DEFAULT_RATE before the first entry."""
    if not date_iso:
        return DEFAULT_RATE
    day = local_day(date_iso)
    rate = DEFAULT_RATE
    for d, r in _merged(learned):
        if d <= day:
            rate = r
        else:
            break
    return rate


def collapse(dated_rates):
    """Collapse a chronological list of (date, rate) observations into change-points:
    keep only the entries where the (snapped) rate differs from the running value.

    Used by the engine to turn a walk of the tariff's inc/exc periods into a minimal
    set of learned boundaries. Unsnappable (None) rates are skipped.
    """
    out = []
    prev = None
    for d, r in sorted((dated_rates or []), key=lambda x: str(x[0])[:10]):
        sr = snap_vat(r)
        if sr is None or not d:
            continue
        if prev is None or sr != prev:
            out.append((str(d)[:10], sr))
            prev = sr
    return out


def merge_learned(existing, observed):
    """Merge freshly-`observed` boundaries into the `existing` learned calendar and
    re-collapse, so re-observing the same tariff is idempotent and only genuine
    change-points survive."""
    merged = {str(d)[:10]: float(r) for d, r in (existing or [])}
    for d, r in (observed or []):
        sr = snap_vat(r)
        if sr is not None and d:
            merged[str(d)[:10]] = sr
    return collapse(sorted(merged.items()))


def learn_from_records(records, now_utc, learned=None):
    """VAT change-points observed in a tariff's (start, end, ratio) periods — about
    TODAY AND LATER only.

    `records` are (valid_from, valid_to|None, vat) in naive UTC, vat = inc/exc - 1.
    A tariff's published VAT says what VAT is now, never since when: 1 Oct 2026's 0%
    arrived as an open record from 5 Jul edited in place to read inc == exc. So a
    period still running, or yet to start, that disagrees with the calendar is dated
    no earlier than today (UK-local). A period that has ENDED teaches nothing — not
    even as "versioned history": EMT itself splits IOG's flat day/night buckets into
    one closed window per half-day (kraken_rates._synthesize_iog_tou_windowed), and
    every past window carries today's edited figures. Learning from those put
    "0% from 5 Jul" straight back after 4.5.17's heal removed it. Past VAT comes from
    SEED; a past change missing from it is a SEED edit, not an inference.
    Returns [(date, rate)] for merge_learned; observations the calendar already
    agrees with change nothing there."""
    today = local_day(now_utc)
    out = []
    for vf, vt, vat in records or []:
        r = snap_vat(vat)
        if r is None or not vf:
            continue
        if vt is not None and str(vt) <= str(now_utc):
            continue
        day = max(local_day(vf), today)
        if r != resolve_vat(day, learned):
            out.append((day, r))
    return out
