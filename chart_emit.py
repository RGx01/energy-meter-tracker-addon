"""
chart_emit.py — the day-level projection of the per-block reprice emit, for PRESENTATION.

Given a day's blocks (each carrying the derived values reprice wrote: the main import rate
and the EV-attributed segments), it returns the per-slot rate SERIES the charts plot: one
house series (every house line shares it) and one EV series. ALL of "what rate applies each
half-hour" lives here, once — so the chart is a pure plotter. If a rate line looks wrong the
fix is here, never in energy_charts. See the presentation read-contract in
docs/design/4.4.0_iog_pricing_and_reprice_design.md.

Rate lines are DATA where the block has usage/split data, TARIFF-FILL where it doesn't:
  HOUSE — where the house genuinely drew, its priced rate (which already carries the
    daytime-dispatch "freebee", i.e. the house riding a within-cap dispatch to off-peak);
    where it did NOT draw, the authoritative TOU rate for the slot (peak day / off-peak
    night), so the line follows the tariff across idle/no-data slots instead of clinging to
    a stale rate.
  EV — where the car drew (segment OR the synthetic dispatch overlay), its priced rate;
    where it did not, the off-peak baseline, held to PEAK only while the 6-hour cap is
    EXCEEDED (the `over_cap` signal, from the authoritative cap machinery), until the noon
    wall-clock reset. A bump or an out-of-dispatch peak charge shows its own peak tick but
    does NOT latch. A synthetic EV has no priced EV segments, so `ev_slot_rate` (the
    dispatch overlay's per-slot rate) feeds the line where the car drew.
Reads RATES for the line values (labels are unreliable live — a bump is priced peak but
labelled off_peak); the held-peak latch reads `over_cap`, never an inferred band.
"""
from __future__ import annotations


def _f(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return 0.0


def day_rate_series(day_blocks, *, slots: int, block_minutes: int,
                    main_meter: str = "electricity_main", capped: bool = True,
                    house_tou=None, ev_slot_rate=None, over_cap=None) -> dict:
    """`day_blocks`: iterable of (hh, block). Returns
    {'house': [rate|None]*slots, 'ev': [rate|None]*slots, 'ev_fallback': [bool]*slots};
    None = leave the base rate (non-IOG / no data → byte-identical to pre-emit rendering).

    `house_tou`: per-slot authoritative TOU rate (£) for EVERY slot (built from the schedule
      in the caller), used to FILL house slots with no usage.
    `ev_slot_rate`: per-slot EV rate (£) from the dispatch overlay, for a SYNTHETIC EV whose
      block carries no priced EV segments; None for a physical-EV meter (uses its segments).
    `over_cap`: per-slot bool — True where the 6-hour cap is EXCEEDED (a within-dispatch slot
      at/after the cap-day boundary, bump/boost excluded). This is the SOLE trigger for the
      held-peak-to-noon: a peak-priced slot that is NOT a cap-exceedance (a bump, an
      out-of-dispatch charge) plots its own peak tick but does NOT latch. Built in the caller
      from the authoritative cap machinery; None (or omitted) → no hold.
    """
    house_seg: dict = {}   # slot -> HOUSE rate where the house genuinely DREW (priced segment)
    house_band: dict = {}  # slot -> the BAND the bill put it in ('off_peak'/'peak'), when known
    house_mr: dict = {}    # slot -> block-rate fallback (non-IOG / no house segment)
    ev_rate: dict = {}     # slot -> EV rate where the car DREW (segment or dispatch overlay)
    rates: set = set()     # the tariff rate values (clean off/peak extremes) for the EV baseline
    # 4.5.7: the rate line must STOP at the last real block, never PROJECT the tariff to
    # midnight — so a today-chart ends at the current block and a stale/lagging render is
    # obvious (see the caller's last_nonzero truncation, which then trims the trace here).
    day_blocks = list(day_blocks)
    _last_hh = max((hh for hh, _ in day_blocks), default=-1)
    for hh, block in day_blocks:
        imp = (((block.get("meters") or {}).get(main_meter) or {})
               .get("channels", {}) or {}).get("import", {}) or {}
        mr = _f(imp.get("rate_used", imp.get("rate")))
        segs = imp.get("segments") or []
        for x in segs:
            r = _f(x.get("inc_rate"))
            if r:
                rates.add(round(r, 6))
        # HOUSE where the house genuinely drew: its priced (house-attributed) rate — carries
        # the within-cap-dispatch freebee. Fall back to the block rate only for `rates`.
        hsegs = [x for x in segs if x.get("attribution") == "house"]
        hk = sum(_f(x.get("kwh")) for x in hsegs)
        if hk > 1e-9:
            house_seg[hh] = sum(_f(x.get("kwh")) * _f(x.get("inc_rate")) for x in hsegs) / hk
            # The BAND the bill put this half-hour in, where the segments carry one they
            # were actually given. 'standard' is pricing_segments' fallback for "unknown",
            # so only an explicit off_peak/peak counts as the bill having spoken.
            _hb = {x.get("band") for x in hsegs if x.get("band") in ("off_peak", "peak")}
            if len(_hb) == 1:
                house_band[hh] = _hb.pop()
        elif mr:
            house_mr[hh] = mr
            rates.add(round(mr, 6))
        evsegs = [x for x in segs if x.get("attribution") == "ev"]
        evk = sum(_f(x.get("kwh")) for x in evsegs)
        if evk > 1e-9:                                     # EV rate from its priced segments
            ev_rate[hh] = sum(_f(x.get("kwh")) * _f(x.get("inc_rate")) for x in evsegs) / evk
        elif imp.get("rate_ev") is not None:              # no segments (live cols) → column
            ev_rate[hh] = _f(imp.get("rate_ev"))

    # SYNTHETIC EV: no priced EV segments in the block, but the dispatch overlay supplies the
    # per-slot EV rate. Fold it in so the EV line draws where the car drew AND the baseline
    # fill below runs. A physical-EV block already has segments and passes ev_slot_rate=None.
    if ev_slot_rate is not None:
        for hh, r in enumerate(ev_slot_rate):
            r = _f(r)
            if r > 1e-9:
                rates.add(round(r, 6))
                ev_rate.setdefault(hh, r)   # never override a real segment rate

    house = [None] * slots
    ev = [None] * slots
    # TOU bounds. Two jobs: (1) detect a GENUINE transition blend (a stored rate strictly
    # between the day's off & peak) vs a stale clean-band artefact; (2) let the house line —
    # and the EV baseline — be driven by the authoritative tariff even on a day with NO EV
    # draw and NO dispatch overlay (an idle capped day), which is where the old
    # `if not ev_rate` early-return wrongly reverted the WHOLE day to stale stored rates.
    _tou_vals = [v for v in house_tou if v is not None] if house_tou is not None else []
    _tlo = min(_tou_vals) if _tou_vals else None
    _thi = max(_tou_vals) if _tou_vals else None
    _is_tou = _tlo is not None and _thi is not None and (_thi - _tlo) > 1e-9
    for _v in _tou_vals:               # so off/pk exist for the EV baseline on a no-EV day
        rates.add(round(_v, 6))
    # Override only where we have something authoritative to say: an EV signal (priced
    # segments or the dispatch overlay) OR a genuine time-of-use house schedule. A non-IOG /
    # flat / no-schedule day has neither → leave the base rate untouched (byte-identical to
    # pre-emit rendering). This no longer conflates "no EV today" with "not an IOG day".
    if not ev_rate and not _is_tou:
        return {"house": house, "ev": ev, "ev_fallback": [False] * slots}

    off = min(rates) if rates else None
    pk = max(rates) if rates else None

    noon = int((12 * 60) / max(1, block_minutes))
    held = None                        # cap-break hold (CAPPED days only); resets at noon
    ev_fallback = [False] * slots      # True where ev[hh] is a FILL guess, not a priced draw
    for hh in range(slots):
        if hh > _last_hh:
            break                          # no real block here or later → don't project
        if capped:
            if hh == noon:
                held = None
            # HELD peak is driven SOLELY by a genuine 6-hour-cap exceedance (over_cap),
            # never by "this slot is peak-priced": a bump or an out-of-dispatch peak charge
            # shows its own peak tick but must NOT latch the line to noon (4-rate rules).
            if over_cap is not None and hh < len(over_cap) and over_cap[hh]:
                held = "peak"
        # HOUSE line, driven by the tariff (not stale segment rates on ~0-kWh slots):
        #   CAPPED dispatch slot → the house rides the dispatch band (within-cap freebee →
        #     off-peak; over-cap → peak). Pre-cap (uncapped IOG) has no freebee: house stays TOU.
        #   genuine TOU-transition blend (stored strictly between the day's off & peak) → preserved.
        #   otherwise → the authoritative TOU (peak day / off-peak night).
        #   non-IOG fallback (no TOU passed) → the priced/stored house rate, byte-identical.
        _stored_h = house_seg.get(hh, house_mr.get(hh))
        if capped and hh in ev_rate:
            house[hh] = round(ev_rate[hh], 6)
        elif hh in ev_rate and _stored_h is not None:
            # PRE-CAP (legacy Intelligent), slot with a dispatch: what the half-hour was
            # actually CHARGED wins over the TOU schedule. Legacy has no four-bucket split
            # — a smart-charge dispatch commonly discounts the whole half-hour, house
            # included — but not always (a part-slot dispatch stays at peak), so the line
            # must follow the priced figure rather than assume either. Falling through to
            # house_tou plotted the schedule's peak on slots the bill charged at off-peak,
            # stranding the house line above an EV line that had correctly dropped.
            # `_stored_h` is the house-attributed segment rate where the house DREW, else
            # the block's own rate; absent both, the TOU branch below still applies.
            house[hh] = round(_stored_h, 6)
        elif house_band.get(hh) in ("off_peak", "peak") and _stored_h is not None:
            # The bill EXPLICITLY banded this half-hour, so its own priced rate wins over
            # the schedule's prediction. This is what a legacy Intelligent dispatch looks
            # like once the rate has been snapped to its band: an off-peak-banded slot in
            # the middle of the peak window, sitting exactly ON the off-peak bound — which
            # the strictly-between blend test below rejects, sending the line to the
            # schedule's peak over a half-hour the bill charged at off-peak.
            house[hh] = round(_stored_h, 6)
        elif (_stored_h is not None and _tlo is not None and _thi is not None
              and _tlo + 1e-9 < _stored_h < _thi - 1e-9):
            house[hh] = round(_stored_h, 6)
        elif house_tou is not None and hh < len(house_tou) and house_tou[hh] is not None:
            house[hh] = round(house_tou[hh], 6)
        elif hh in house_seg:
            house[hh] = round(house_seg[hh], 6)
        elif hh in house_mr:
            house[hh] = round(house_mr[hh], 6)
        # EV line: charged → its priced rate; idle → off-peak baseline (capped), held PEAK
        # after a cap-break to the noon reset; pre-cap → tracks the house TOU line.
        if hh in ev_rate:
            ev[hh] = round(ev_rate[hh], 6)
        else:
            ev_fallback[hh] = True
            if capped and held == "peak" and pk is not None:
                ev[hh] = round(pk, 6)                      # over-cap: held peak to noon reset
            elif capped and off is not None:
                ev[hh] = round(off, 6)                     # within cap: off-peak baseline
            elif house[hh] is not None:
                ev[hh] = house[hh]                         # PRE-CAP: EV tracks house TOU line
    return {"house": house, "ev": ev, "ev_fallback": ev_fallback}
