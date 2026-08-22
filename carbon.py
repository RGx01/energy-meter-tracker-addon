"""
carbon.py — the pure carbon axis for 4.4.0 (Δ5).
See docs/design/4.4.0_iog_pricing_and_reprice_design.md §3a.

Carbon is a SEPARATE derived axis: kWh × grid CO2 intensity, price NEVER enters. This module
is the adjacent pass that consumes the energy substrate `reprice.reprice_block` already emits
(`carbon.ev_unclipped_kwh`, `carbon.house_kwh`) plus the grid-clipped billing EV
(`devices.ev.kwh`) and multiplies by the grid intensity (gCO2/kWh). It computes NOTHING from
rate or cost — the two models never touch, so carbon stays price-free by construction.

The EV carbon credit (the 4.3.2 methodology, re-sourced from synthetic dispatch on IOG):
a smart-charged car may draw MORE than the grid supplied in a slot — the difference came from
behind the meter (house battery / solar-direct / self-consumption) and never crossed the grid
meter. Valued at the CURRENT grid intensity, that behind-meter energy is avoided grid carbon:

    unclipped  = the car's FULL draw for the slot (from the completed dispatch)
    clipped    = min(unclipped, grid import)  — the portion the grid actually supplied (billing)
    behind     = unclipped − clipped          — sourced behind the meter, zero grid carbon

    grid_g   = clipped   × intensity   — the EV's ACTUAL grid carbon
    saving_g = behind    × intensity   — avoided grid carbon (the credit)
    gross_g  = unclipped × intensity   — grid_g + saving_g (the 4.3.2 sub-meter gross figure)

Export/outgoing figures do NOT enter this: the credit rides the DRAW gap, not generation
export. Single-source rule (mirrors the cost side): on IOG the synthetic dispatch device IS
the EV carbon source; any configured physical device is ignored for the EV carbon axis.
"""
from __future__ import annotations

from typing import Optional


def ev_carbon(*, unclipped_kwh: float, clipped_kwh: float,
              intensity_g: float) -> dict:
    """Per-EV carbon from the dispatch draw gap at a grid intensity (gCO2/kWh).

    Returns grid_g (actual grid carbon), saving_g (the avoided-carbon credit), and gross_g
    (= grid_g + saving_g), plus the kWh the figures derive from. Pure; no rate/cost input.
    `clipped` is clamped to `[0, unclipped]` so a stale/over-clipped billing figure can never
    manufacture a negative saving.
    """
    unclipped = max(0.0, float(unclipped_kwh or 0.0))
    clipped = min(max(0.0, float(clipped_kwh or 0.0)), unclipped)
    behind = unclipped - clipped
    grid_g = round(clipped * intensity_g, 4)
    saving_g = round(behind * intensity_g, 4)
    return {
        "grid_g": grid_g,
        "saving_g": saving_g,
        "gross_g": round(grid_g + saving_g, 4),
        "clipped_kwh": round(clipped, 6),
        "unclipped_kwh": round(unclipped, 6),
        "behind_meter_kwh": round(behind, 6),
    }


def house_carbon(*, house_kwh: float, intensity_g: float) -> dict:
    """House-axis carbon: the grid-supplied house energy × intensity. House has no behind-
    meter credit — the credit axis is the EV's alone (its draw can exceed grid; the house
    remainder is only ever what the grid supplied)."""
    kwh = max(0.0, float(house_kwh or 0.0))
    return {"carbon_g": round(kwh * intensity_g, 4), "kwh": round(kwh, 6)}


def carbon_from_reprice(result: dict, intensity_g: float) -> dict:
    """Adjacent carbon pass over a `reprice.reprice_block` result.

    Consumes ONLY the emitted energy substrate — the clipped billing EV (`devices.ev.kwh`)
    and the unclipped carbon EV (`carbon.ev_unclipped_kwh`) plus the house grid energy
    (`carbon.house_kwh`). Returns the per-axis carbon bundle. Never reads rate/cost/segments.
    """
    dev = (result.get("devices") or {}).get("ev") or {}
    sub = result.get("carbon") or {}
    ev = ev_carbon(
        unclipped_kwh=sub.get("ev_unclipped_kwh", dev.get("kwh_unclipped", 0.0)),
        clipped_kwh=dev.get("kwh", 0.0),
        intensity_g=intensity_g,
    )
    house = house_carbon(house_kwh=sub.get("house_kwh", 0.0),
                         intensity_g=intensity_g)
    return {
        "ev": ev,
        "house": house,
        "saving_g": ev["saving_g"],
        "intensity_g": intensity_g,
    }


def period_ev_saving(rows, unclipped_by_slot: dict) -> dict:
    """Aggregate the EV carbon axis over a period, single-source from synthetic dispatch.

    `rows`             : iterable of (slot_start, clipped_ev_kwh, intensity_g) — the stored
                         billing EV split (imp_kwh_ev) + block intensity, main meter only.
    `unclipped_by_slot`: {slot_start: raw dispatch kWh} — the car's FULL draw (dispatch_history
                         completed). Empty on a non-IOG account ⇒ zero saving ⇒ the caller
                         renders nothing ⇒ byte-identical.

    Shape-agnostic and pure so both carbon surfaces (heatmap, Insights) can consume it without
    re-implementing the methodology. Single-source: a slot is counted only when dispatch covers
    it, so the physical EV device is ignored on IOG (mirrors the cost side). Returns the summed
    grid_g / saving_g / gross_g and the count of blocks that carried a saving.
    """
    grid = save = gross = 0.0
    clipped_kwh = 0.0
    credit_blocks = 0
    for slot, clip, intensity in rows:
        try:
            clip = float(clip or 0.0)
        except (TypeError, ValueError):
            continue
        if clip <= 0:
            continue
        u = unclipped_by_slot.get(slot)
        if u is None:
            continue
        e = ev_carbon(unclipped_kwh=u, clipped_kwh=clip,
                      intensity_g=float(intensity or 0.0))
        grid += e["grid_g"]
        save += e["saving_g"]
        gross += e["gross_g"]
        clipped_kwh += e["clipped_kwh"]
        if e["saving_g"] > 0:
            credit_blocks += 1
    return {
        "grid_g": round(grid, 4),
        "saving_g": round(save, 4),
        "gross_g": round(gross, 4),
        "clipped_kwh": round(clipped_kwh, 6),
        "credit_blocks": credit_blocks,
    }


def slot_intensity(carbon_g, stored_intensity_g, net_kwh):
    """Per-slot grid intensity (gCO2/kWh, 1 dp) for the carbon heatmap — the emit the plotter
    reads instead of deriving inline. Intensity is a property of the grid, independent of how
    much we drew: prefer the value stored at write time (3.0.0+, defined even for a zero-net
    block); else derive from carbon_g / net for pre-3.0.0 blocks that predate the column; else
    None (a blank cell). Pure; mirrors the exact rounding of the retired inline branch."""
    if stored_intensity_g is not None:
        return round(abs(float(stored_intensity_g)), 1)
    if net_kwh:
        try:
            return round(abs(float(carbon_g) / float(net_kwh)), 1)
        except (TypeError, ValueError, ZeroDivisionError):
            return None
    return None
