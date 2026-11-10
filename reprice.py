"""
reprice.py — the single, pure per-block pricing model for 4.4.0.
See docs/design/4.4.0_iog_pricing_and_reprice_design.md.

One deterministic function turns a block's AUTHORITATIVE inputs (settled kWh + cost, the
resolved device energies, the effective tariff rates + VAT, the cap boundary) into EVERY
derived value the rest of EMT reads: blended rate, ex-VAT, the per-device split
(kWh/cost/rate), priced segments, DERIVED band labels, cap state, and the carbon energy
substrate (clipped for billing, unclipped for carbon). It never reads band labels and never
writes a subset — presentation just plots what it emits.

Principle 0: this is the one model every code path conforms to. Pure — depends only on
iog_cap + pricing_segments; the engine adapter supplies resolved rates from its live
schedule cache, fixtures/tests supply them as constants.
"""
from __future__ import annotations

from typing import Optional

import iog_cap
import pricing_segments as ps

_EPS = 1e-9

# Account types — decided by the config period's tariff code upstream.
IOG_CAPPED = "iog_capped"
IOG_UNCAPPED = "iog_uncapped"
NON_IOG = "non_iog"

# EV eligibility — the FINALISED decision (base schedule + started-gate smart overlay),
# INHERITED by reprice, never re-derived here (see design §3b). SMART = off-peak base, cap
# applies; BUMP = peak by finalise, never consumes the 6 h allowance.
SMART = "smart"
BUMP = "bump"


# ── account-type device resolver ─────────────────────────────────────────────
def resolve_ev_energy(account_type: str, *, grid_kwh: float,
                      dispatch_ev_kwh: Optional[float] = None,
                      physical_ev_kwh: Optional[float] = None) -> dict:
    """ONE EV source, chosen by account type — never both blended.

    IOG (capped or uncapped): the synthetic **dispatch** EV — what Octopus bills and what
    counts toward the cap. Non-IOG with an EV: the **physical** charger (grid import to the
    car). Else: no EV. Returns the grid-CLIPPED energy (billing basis) and the UNCLIPPED
    energy (carbon basis, so house generation can offset it).
    """
    if account_type in (IOG_CAPPED, IOG_UNCAPPED) and dispatch_ev_kwh is not None:
        source, raw = "dispatch", max(0.0, float(dispatch_ev_kwh))
    elif physical_ev_kwh is not None:
        source, raw = "physical", max(0.0, float(physical_ev_kwh))
    else:
        return {"source": None, "clipped": 0.0, "unclipped": 0.0}
    return {"source": source,
            "clipped": min(raw, max(0.0, float(grid_kwh))),
            "unclipped": raw}


def _band_label(segs, attribution):
    bands = {b for (_k, _r, b, a) in segs if a == attribution}
    if not bands:
        return None
    return "mixed" if len(bands) > 1 else next(iter(bands))


def _reconcile_house(house_segs, house_kwh, house_cost):
    """House is the reconciling plug: keep kWh fixed, absorb the residual into the house
    'day' band (else the last house band) so Σ house cost == house_cost exactly."""
    if not house_segs:
        if house_kwh > _EPS:
            return [(round(house_kwh, 6), round(house_cost / house_kwh, 6), "day", "house")]
        return []
    cur = sum(k * r for (k, r, b, a) in house_segs)
    resid = house_cost - cur
    if abs(resid) < _EPS:
        return list(house_segs)
    idx = next((i for i, (k, r, b, a) in enumerate(house_segs) if b == "day"),
               len(house_segs) - 1)
    out = list(house_segs)
    k, r, b, a = out[idx]
    out[idx] = (k, round((k * r + resid) / k, 6) if k > _EPS else r, b, a)
    return out


def reprice_block(*, block_start: str, block_end: str, grid_kwh: float,
                  ev: dict, rates: dict, settled_cost: Optional[float] = None,
                  vat: float = 0.05, in_off_peak_window: bool = True,
                  ev_eligibility: Optional[str] = None, boundary=None,
                  generation_kwh: float = 0.0) -> dict:
    """The one function. Emits the per-block bundle every surface reads.

    `ev`             : a resolve_ev_energy() dict (clipped/unclipped/source).
    `rates`          : {house_off, house_day, ev_off, ev_peak}. UNCAPPED → pass all four equal
                       to the block's resolved rate → byte-identical single-rate pricing.
    `ev_eligibility` : the INHERITED finalised decision — SMART (off-peak base; the cap may
                       flip its over-cap part to peak) or BUMP (peak by finalise; no freebie,
                       no cap allowance). reprice does NOT re-derive this (design §3b); the
                       caller passes the finalise-time / stamped value. None ≡ SMART.
    `settled_cost`   : the authoritative anchor when known — truth flows down and the HOUSE
                       slice is the plug that closes Σ cost == settled_cost. When None the
                       block self-costs from the rates (live/provisional).
    """
    is_boost = (ev_eligibility == BUMP)      # bump → EV peak, no freebie, outside the cap
    grid_kwh = max(0.0, float(grid_kwh))
    ev_clip = min(max(0.0, float(ev.get("clipped", 0.0))), grid_kwh)
    house_kwh = max(0.0, grid_kwh - ev_clip)

    priced = iog_cap.price_slot(
        block_start, block_end, grid_kwh, ev_clip,
        in_off_peak_window=in_off_peak_window, is_boost=is_boost, boundary=boundary,
        house_offpeak_rate=rates["house_off"], house_day_rate=rates["house_day"],
        ev_offpeak_rate=rates.get("ev_off"), ev_peak_rate=rates.get("ev_peak"))

    raw_segs = priced.get("segments")
    if raw_segs is None:                                  # house-only slot
        band = "off_peak" if in_off_peak_window else "day"
        raw_segs = [(round(house_kwh, 6), priced["imp_rate"], band, "house")] if house_kwh > _EPS else []

    ev_segs = [(k, r, b, a) for (k, r, b, a) in raw_segs if a == "ev"]
    house_segs_raw = [(k, r, b, a) for (k, r, b, a) in raw_segs if a == "house"]

    ev_cost = round(sum(k * r for (k, r, b, a) in ev_segs), 6)
    raw_total = round(sum(k * r for (k, r, b, a) in raw_segs), 6)
    total_cost = round(float(settled_cost), 6) if settled_cost is not None else raw_total

    if house_kwh > _EPS:
        house_cost = round(total_cost - ev_cost, 6)
        house_segs = _reconcile_house(house_segs_raw, house_kwh, house_cost)
    else:                                                 # all-EV block: EV absorbs
        house_cost = 0.0
        house_segs = []
        if ev_segs and abs(total_cost - ev_cost) > _EPS:
            k0, r0, b0, a0 = ev_segs[-1]
            resid = total_cost - ev_cost
            ev_segs[-1] = (k0, round((k0 * r0 + resid) / k0, 6) if k0 > _EPS else r0, b0, a0)
            ev_cost = total_cost

    segs_tuples = ev_segs + house_segs
    ratio = (1.0 / (1.0 + vat)) if vat is not None else None
    segments = [ps.Segment(round(k, 6), round(r, 6),
                           (round(r * ratio, 6) if ratio is not None else None), b, a)
                for (k, r, b, a) in segs_tuples]

    ev_kwh_billed = round(sum(k for (k, r, b, a) in ev_segs), 6)
    house_kwh_out = round(sum(k for (k, r, b, a) in house_segs), 6)
    rate = round(total_cost / grid_kwh, 6) if grid_kwh > _EPS else rates["house_day"]
    unclipped = round(float(ev.get("unclipped", 0.0)), 6)

    return {
        "kwh": round(grid_kwh, 6),
        "cost": round(total_cost, 6),
        "rate": rate,
        "rate_exc": round(rate * ratio, 6) if ratio is not None else None,
        "cost_exc": round(total_cost * ratio, 6) if ratio is not None else None,
        "segments": segments,
        "devices": {
            "ev": {"kwh": ev_kwh_billed, "kwh_unclipped": unclipped, "cost": ev_cost,
                   "rate": round(ev_cost / ev_kwh_billed, 6) if ev_kwh_billed > _EPS else None,
                   "source": ev.get("source")},
            "house": {"kwh": house_kwh_out, "cost": round(house_cost, 6),
                      "rate": round(house_cost / house_kwh_out, 6) if house_kwh_out > _EPS else None},
        },
        "bands": {"ev": _band_label(segs_tuples, "ev"),
                  "house": _band_label(segs_tuples, "house")},
        "cap": {"boundary": boundary,
                "over": bool(boundary and _band_label(segs_tuples, "ev") in ("peak", "mixed"))},
        "carbon": {"ev_unclipped_kwh": unclipped, "house_kwh": house_kwh_out,
                   "generation_kwh": round(float(generation_kwh), 6)},
    }
