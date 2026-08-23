"""
chart_emit.py — the day-level projection of the per-block reprice emit, for PRESENTATION.

Given a day's blocks (each carrying the derived values reprice wrote: the main import rate
and the EV-attributed segments), it returns the per-slot rate SERIES the charts plot: one
house series (every house line shares it) and one EV series. ALL of "what rate applies each
half-hour" lives here, once — so the chart is a pure plotter. If a rate line looks wrong the
fix is here, never in energy_charts. See the presentation read-contract in
docs/design/4.4.0_iog_pricing_and_reprice_design.md.

Reads RATES, never band labels (labels are unreliable live — a bump is priced peak but
labelled off_peak). The EV series shows the priced EV rate where the car charged, off-peak
when idle, and holds peak to the noon cap-day reset once the cap genuinely breaks.
"""
from __future__ import annotations


def _f(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return 0.0


def day_rate_series(day_blocks, *, slots: int, block_minutes: int,
                    main_meter: str = "electricity_main") -> dict:
    """`day_blocks`: iterable of (hh, block). Returns
    {'house': [rate|None]*slots, 'ev': [rate|None]*slots}; None = leave the base rate
    (non-IOG / no data → byte-identical to pre-emit rendering)."""
    house_rate: dict = {}    # slot -> HOUSE rate (house-attributed segment; NOT the block
                             # blend, which mixes in EV-peak and would draw a staircase)
    ev_rate: dict = {}       # slot -> EV rate where the car drew
    ev_break: dict = {}      # slot -> 'peak'/'mixed' where the cap genuinely broke
    rates: set = set()       # the tariff rate values, from SEGMENTS (clean off/peak extremes)
    for hh, block in day_blocks:
        imp = (((block.get("meters") or {}).get(main_meter) or {})
               .get("channels", {}) or {}).get("import", {}) or {}
        mr = _f(imp.get("rate_used", imp.get("rate")))
        segs = imp.get("segments") or []
        for x in segs:
            r = _f(x.get("inc_rate"))
            if r:
                rates.add(round(r, 6))
        # HOUSE line = house-attributed segment rate (off-peak through the guaranteed window,
        # one transition); fall back to the block rate only when there are no house segments
        # (uncapped / non-IOG, where block rate == house rate → byte-identical).
        hsegs = [x for x in segs if x.get("attribution") == "house"]
        hk = sum(_f(x.get("kwh")) for x in hsegs)
        if hk > 1e-9:
            house_rate[hh] = sum(_f(x.get("kwh")) * _f(x.get("inc_rate")) for x in hsegs) / hk
        elif mr:
            house_rate[hh] = mr
            rates.add(round(mr, 6))
        evsegs = [x for x in segs if x.get("attribution") == "ev"]
        evk = sum(_f(x.get("kwh")) for x in evsegs)
        if evk > 1e-9:                                     # EV rate from its priced segments
            ev_rate[hh] = sum(_f(x.get("kwh")) * _f(x.get("inc_rate")) for x in evsegs) / evk
        elif imp.get("rate_ev") is not None:              # no segments (live cols) → column
            ev_rate[hh] = _f(imp.get("rate_ev"))
        bands = set(x.get("band") for x in evsegs)
        if len(bands) > 1:
            ev_break[hh] = "mixed"
        elif "peak" in bands:
            ev_break[hh] = "peak"

    house = [None] * slots
    ev = [None] * slots
    if not ev_rate:                    # non-IOG / no dispatch → no override, byte-identical
        return {"house": house, "ev": ev}

    off = min(rates) if rates else None
    pk = max(rates) if rates else None
    noon = int((12 * 60) / max(1, block_minutes))
    held = None                        # cap-break hold; resets each cap-day at noon
    for hh in range(slots):
        if hh == noon:
            held = None
        if ev_break.get(hh) in ("peak", "mixed"):
            held = "peak"
        if hh in house_rate:
            house[hh] = round(house_rate[hh], 6)
        if hh in ev_rate:
            ev[hh] = round(ev_rate[hh], 6)
        elif held == "peak" and pk is not None:
            ev[hh] = round(pk, 6)
        elif off is not None:
            ev[hh] = round(off, 6)
    return {"house": house, "ev": ev}
