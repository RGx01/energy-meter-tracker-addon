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
SEED = [("1997-09-01", 0.05)]
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


def resolve_vat(date_iso, learned=None):
    """The VAT rate effective at `date_iso` (any ISO string; only the date is used),
    from the seed + learned boundaries. DEFAULT_RATE before the first entry."""
    if not date_iso:
        return DEFAULT_RATE
    day = str(date_iso)[:10]
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
