"""
pricing_segments.py — BL-27: a block's grid import as an ordered list of priced SEGMENTS.

A segment is one homogeneously-priced slice of the block's grid import:

    Segment(kwh, inc_rate, exc_rate, band, attribution)
        band, attribution : OPEN labels (strings). Storage and surfaces NEVER switch on
                            them — only the classifier assigns meaning. This is what makes
                            a new rate dimension (a battery band, a heat-pump attribution)
                            a one-place change. See docs/design/segment_pricing_refactor_design.md.
        exc_rate          : per-segment ex-VAT rate — VAT is a property of the slice, so a
                            VAT holiday / per-band quirk is absorbed with no special-casing.

This module is PURE (no I/O, no engine imports). The *classifier* that decides the
off-peak fractions/bands lives in `iog_cap`; here we turn (a kWh split + per-band rates)
into segments and expose the PROJECTIONS that the legacy `imp_*` columns become views over
(blended rate, EV/house split, ex-VAT). One decomposition; everything else reads it.

Invariant (the acceptance gate everywhere downstream):
    Σ segment.kwh   == the block's grid kWh
    Σ segment.kwh × inc_rate == the block's grid cost (to rounding)
so house + devices + remainder always reconcile to the grid import, capped or uncapped.
"""

from typing import NamedTuple, Optional, List

_EPS = 1e-9


class Segment(NamedTuple):
    kwh: float
    inc_rate: float
    exc_rate: Optional[float]
    band: str          # open label: off_peak | day | peak | …
    attribution: str   # open label: house | ev | …


def _seg(kwh: float, inc_rate: float, band: str, attribution: str,
         vat: float) -> Segment:
    exc = round(inc_rate / (1.0 + vat), 6) if inc_rate is not None else None
    return Segment(round(kwh, 6), inc_rate, exc, band, attribution)


def import_segments(*, ev_kwh: float, house_kwh: float,
                    house_offpeak_rate: float, house_day_rate: float,
                    ev_offpeak_rate: Optional[float] = None,
                    ev_peak_rate: Optional[float] = None,
                    ev_offpeak_frac: float = 1.0,
                    house_offpeak_frac: float = 1.0,
                    vat: float = 0.05) -> List[Segment]:
    """Decompose a block's grid import into priced segments.

    `*_offpeak_frac` (0..1) is the off-peak share of that portion — 1.0 = wholly off-peak,
    0.0 = wholly peak/day, and a value strictly between is the boundary slot (the cap
    crossing mid-block), which yields TWO sub-segments for that portion. `ev_kwh` is the
    dispatch-derived EV energy (grid-clipped upstream); `house_kwh` is the remainder.

    UNCAPPED usage: pass every rate equal to the block's rate (EV and house share it), and
    the fractions per the tariff window — the result is the same block rate on every
    segment, so it's byte-identical to today's single-rate pricing. CAPPED usage: pass the
    four distinct 4-rate values and the cap fractions from `iog_cap.classify_slot`.

    Zero-kWh slices are dropped. Segments are ordered EV-off, EV-peak, house-off, house-day.
    """
    out: List[Segment] = []
    if ev_kwh > _EPS and ev_offpeak_rate is not None and ev_peak_rate is not None:
        k_off = ev_kwh * ev_offpeak_frac
        k_pk = ev_kwh - k_off
        if k_off > _EPS:
            out.append(_seg(k_off, ev_offpeak_rate, "off_peak", "ev", vat))
        if k_pk > _EPS:
            out.append(_seg(k_pk, ev_peak_rate, "peak", "ev", vat))
    if house_kwh > _EPS:
        k_off = house_kwh * house_offpeak_frac
        k_day = house_kwh - k_off
        if k_off > _EPS:
            out.append(_seg(k_off, house_offpeak_rate, "off_peak", "house", vat))
        if k_day > _EPS:
            out.append(_seg(k_day, house_day_rate, "day", "house", vat))
    return out


def segments_from_legacy(*, imp_kwh: float, imp_cost: float, imp_rate: float,
                         kwh_ev: Optional[float] = None,
                         cost_ev: Optional[float] = None,
                         rate_ev: Optional[float] = None,
                         ev_band: Optional[str] = None,
                         home_band: Optional[str] = None,
                         exc_ratio: Optional[float] = None) -> List[Segment]:
    """Reconstruct a block's segments from the LEGACY stored columns — the backfill's core.

    Produces the same figures the `imp_*` columns held, so migrated readers show identical
    values: an EV segment (`imp_kwh_ev` @ `imp_rate_ev`) plus the house remainder at its
    derived rate when a split is present, else a single house segment at `imp_rate`. The
    columns already collapsed any 4-rate boundary detail, so this yields 1–2 segments; the
    live seam writes the full 1–4 going forward. `exc_ratio` = the block's exc/inc ratio
    (`imp_rate_exc`/`imp_rate`, or 1/(1+VAT)); None leaves exc NULL (view falls back).
    """
    def _exc(inc):
        return round(inc * exc_ratio, 6) if (exc_ratio is not None and inc is not None) else None

    # Rates are derived from the stored COSTS (not the stored tariff rate), so the segments
    # reproduce imp_cost/imp_cost_ev EXACTLY even on a block where cost ≠ kWh × rate (a
    # settlement-adjusted or stale-split block). On a consistent block the derived rate
    # equals the tariff rate; the small settlement jitter is collapsed at display.
    segs: List[Segment] = []
    if kwh_ev is not None and kwh_ev > _EPS:
        hk = imp_kwh - kwh_ev
        # The house remainder absorbs the difference so Σ cost == imp_cost exactly. When
        # there's NO house remainder (all-EV block), the EV segment carries the full cost —
        # so a stale/settlement-adjusted cost_ev can never leave an unreconciled residual.
        ev_c = imp_cost if hk <= _EPS else (
            cost_ev if cost_ev is not None else kwh_ev * (rate_ev or 0.0))
        er = round(ev_c / kwh_ev, 6)
        segs.append(Segment(round(kwh_ev, 6), er, _exc(er), ev_band or "standard", "ev"))
        if hk > _EPS:
            hr = round((imp_cost - ev_c) / hk, 6)
            segs.append(Segment(round(hk, 6), hr, _exc(hr), home_band or "standard", "house"))
    elif imp_kwh > _EPS:
        r = round(imp_cost / imp_kwh, 6) if imp_kwh > _EPS else imp_rate
        segs.append(Segment(round(imp_kwh, 6), r, _exc(r), home_band or "standard", "house"))
    return segs


# ── Projections — the legacy imp_* columns become views over the segments ────────────

def total_kwh(segs: List[Segment]) -> float:
    return round(sum(s.kwh for s in segs), 6)


def total_cost(segs: List[Segment]) -> float:
    return round(sum(s.kwh * s.inc_rate for s in segs), 6)


def total_cost_exc(segs: List[Segment]) -> Optional[float]:
    if any(s.exc_rate is None for s in segs):
        return None
    return round(sum(s.kwh * s.exc_rate for s in segs), 6)


def blended_rate(segs: List[Segment]) -> float:
    k = total_kwh(segs)
    return round(total_cost(segs) / k, 6) if k > _EPS else 0.0


def attribution_kwh(segs: List[Segment], attribution: str) -> float:
    return round(sum(s.kwh for s in segs if s.attribution == attribution), 6)


def attribution_cost(segs: List[Segment], attribution: str) -> float:
    return round(sum(s.kwh * s.inc_rate for s in segs if s.attribution == attribution), 6)


def attribution_rate(segs: List[Segment], attribution: str) -> float:
    k = attribution_kwh(segs, attribution)
    return round(attribution_cost(segs, attribution) / k, 6) if k > _EPS else 0.0


# ── Device attribution — a device's metered kWh priced on its segments ───────────────

def _row(kwh: float, cost: float) -> dict:
    return {"kwh": round(kwh, 6), "cost": round(cost, 6),
            "rate": round(cost / kwh, 6) if kwh > _EPS else 0.0}


def attribute_devices(segments: List[Segment], devices) -> dict:
    """Attribute physical sub-devices onto the segments they belong to.

    `devices` = iterable of {"meter_id", "attribution", "grid_kwh"} (grid-clipped upstream;
    `attribution` maps the device's type to a segment attribution — "ev" for a charger,
    anything else is house load). Each device is SHOWN at its metered grid kWh; an **ev**
    device is priced from the EV-attributed segment cost (dispatch is the EV billing basis —
    kWh from the device, cost from the segments, split pro-rata across multiple EV devices);
    a **house** device is priced at the house-band rate. The house **remainder** is the plug
    on BOTH axes, so device costs + remainder == the grid total exactly (the reconciliation
    invariant), capped or uncapped.

    Returns {"devices": {meter_id: {kwh,cost,rate}}, "ev_dispatch": {kwh,cost,rate}|None,
             "remainder": {kwh,cost,rate}}. `ev_dispatch` is the synthetic EV row — present
    only when EV segments exist but no physical EV device claims them (the sensor-less case).
    """
    grid_k = total_kwh(segments)
    grid_c = total_cost(segments)
    ev_k = attribution_kwh(segments, "ev")
    ev_c = attribution_cost(segments, "ev")
    house_rate = attribution_rate(segments, "house")

    devs = list(devices)
    ev_devs = [d for d in devs if d.get("attribution") == "ev"]
    house_devs = [d for d in devs if d.get("attribution") != "ev"]

    out: dict = {}
    claimed_k = claimed_c = 0.0
    ev_met = sum(d.get("grid_kwh", 0.0) for d in ev_devs)
    for d in ev_devs:
        k = d.get("grid_kwh", 0.0)
        c = round(ev_c * (k / ev_met), 6) if ev_met > _EPS else 0.0
        out[d["meter_id"]] = _row(k, c)
        claimed_k += k
        claimed_c += c
    for d in house_devs:
        k = d.get("grid_kwh", 0.0)
        c = round(k * house_rate, 6)
        out[d["meter_id"]] = _row(k, c)
        claimed_k += k
        claimed_c += c

    ev_dispatch = None
    if not ev_devs and ev_k > _EPS:                 # sensor-less EV → synthetic row
        ev_dispatch = _row(ev_k, ev_c)
        claimed_k += ev_k
        claimed_c += ev_c

    remainder = _row(grid_k - claimed_k, grid_c - claimed_c)
    return {"devices": out, "ev_dispatch": ev_dispatch, "remainder": remainder}


def price_devices_hybrid(segments: List[Segment], devices) -> dict:
    """Price PHYSICAL sub-devices by valuing each device's METERED grid kWh at the block's
    band rate for its attribution — the model for accounts that meter a device directly.

    Contrast with `attribute_devices`, which prices an EV device from the EV *dispatch*
    segment cost: on an account with a real EV meter the metered draw is the physical truth
    (a car also grid-charges outside smart-dispatch windows, at the house/day rate), so we
    keep the device's own kWh and value it at the correct rate:

      * a **single-rate** (uncapped-equivalent) block → every band rate equals the block's
        blended rate, so a device costs `metered × block_rate` — BYTE-IDENTICAL to its
        stored `imp_cost` column, so uncapped accounts are unchanged (no dispatch-maturity
        divergence, no leakage into house);
      * a **multi-rate** (capped / boundary) block → an EV device's metered kWh is valued at
        the EV band rate (off-peak within cap, peak beyond), a house device at the house band
        rate — the 4-rate fix the parent blended rate got wrong.

    `devices` = iterable of {"meter_id", "attribution" ('ev'|else house), "grid_kwh"}.
    The house **remainder** is the plug on both axes, so devices + remainder == the grid
    total exactly (the reconciliation invariant). Returns
    {"devices": {meter_id: {kwh,cost,rate}}, "remainder": {kwh,cost,rate}}.
    """
    grid_k = total_kwh(segments)
    grid_c = total_cost(segments)
    blended = blended_rate(segments)
    single = len({round(s.inc_rate, 6) for s in segments}) <= 1
    ev_rate = blended if single else (attribution_rate(segments, "ev") or blended)
    house_rate = blended if single else (attribution_rate(segments, "house") or blended)

    out: dict = {}
    claimed_k = claimed_c = 0.0
    for d in devices:
        k = d.get("grid_kwh", 0.0)
        r = ev_rate if d.get("attribution") == "ev" else house_rate
        c = round(k * r, 6)
        out[d["meter_id"]] = _row(k, c)
        claimed_k += k
        claimed_c += c

    remainder = _row(grid_k - claimed_k, grid_c - claimed_c)
    return {"devices": out, "remainder": remainder}
