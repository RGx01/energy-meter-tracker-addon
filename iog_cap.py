"""
iog_cap.py — IOG 6-hour charge-cap math (pure, no I/O)
======================================================
The cap is **6 hours of actual EV CHARGING per noon→noon (local) cap-day**, measured
from `completed` dispatch: the EV portion of a slot bills off-peak until cumulative
charging reaches the boundary and EV-peak beyond. Charged time is `|energy| ÷ power`,
with power inferred the way the charge card does (`2 ×` the cap-day's fullest delivered
half-hour ≈ true charge power), so a part-filled slot counts as the fraction it charged
and the boundary can fall MID-slot (a blended block). See docs/iog_6hr_cap_design.md.

Everything here is a pure function of (completed dispatch slots + delivered energy,
timezone), so it is unit-testable and gives the *same* answer live and at settlement. It
does NOT decide slot rates — it only measures cap usage and locates the boundary slot +
its within-cap energy fraction; the 4-rate classifier consumes that.
"""

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from typing import Iterable, Optional

CAP_HOURS = 6.0
CAP_DAY_ANCHOR_HOUR = 12          # noon→noon cap-day
_UTC = ZoneInfo("UTC")


def _tz(tz_name: Optional[str]) -> ZoneInfo:
    try:
        return ZoneInfo(tz_name) if tz_name else _UTC
    except Exception:
        return _UTC


def cap_day_key(ts_utc: str, tz_name: str) -> str:
    """The noon→noon cap-day a naive-UTC instant belongs to, as the LOCAL date of
    the day whose noon starts the window. A slot before local noon belongs to the
    *previous* day's noon→noon window. (Bucketing is by the interval's start; a
    dispatch is short enough that the noon boundary bisecting one is negligible.)"""
    tz = _tz(tz_name)
    local = datetime.fromisoformat(str(ts_utc)).replace(tzinfo=_UTC).astimezone(tz)
    anchor = local if local.hour >= CAP_DAY_ANCHOR_HOUR else local - timedelta(days=1)
    return anchor.date().isoformat()


def merge_intervals(intervals: Iterable[tuple]) -> list:
    """Merge overlapping/adjacent [start, end) intervals (naive-UTC ISO strings)
    into a sorted, disjoint list of (start, end) tuples. Overlapping durations are
    unioned, never summed — this is what keeps the cap honest when two dispatch
    rows cover the same wall-clock time."""
    ivs = sorted((s, e) for s, e in intervals if s and e and s < e)
    out: list = []
    for s, e in ivs:
        if out and s <= out[-1][1]:
            if e > out[-1][1]:
                out[-1] = (out[-1][0], e)
        else:
            out.append((s, e))
    return out


def _hours(s: str, e: str) -> float:
    return (datetime.fromisoformat(e) - datetime.fromisoformat(s)).total_seconds() / 3600.0


def cap_usage(completed_slots: Iterable[tuple], tz_name: str,
              cap_hours: float = CAP_HOURS) -> dict:
    """Cap usage per noon→noon cap-day, measured in ACTUAL CHARGING TIME.

    The IOG cap is `cap_hours` of *charging*, NOT of dispatch wall-clock — a half-hour
    slot that only part-filled counts as the time it actually charged. Charge power is
    inferred the way the charge card does (`_build_charge_sessions`): 2× the cap-day's
    fullest delivered half-hour (a saturated slot ≈ true power). Each slot then
    contributes `|energy| / power` hours, so the cap boundary can fall MID-slot.

    `completed_slots` — iterable of (raw_start, raw_end, energy_kwh); `completed` rows
    only. Returns `{cap_day: {"used_hours", "over", "boundary_slot", "boundary_frac"}}`:
      - `used_hours` — total charged hours in the cap-day,
      - `over` — whether it exceeds `cap_hours`,
      - `boundary_slot` — the slot_start whose running charge-time first reaches the cap
        (None if never), with `boundary_frac` = the fraction of THAT slot's energy still
        within the cap (so it bills as a blend). Slots before it are wholly within-cap,
        slots after wholly over-cap.
    """
    by_day: dict = {}
    for row in completed_slots:
        s0 = row[0]; e0 = row[1] if len(row) > 1 else None
        energy = row[2] if len(row) > 2 else None
        if not s0 or energy is None:
            continue
        d = by_day.setdefault(cap_day_key(s0, tz_name), {})
        d[str(s0)] = d.get(str(s0), 0.0) + abs(float(energy))   # one row/slot; sum defensively

    out: dict = {}
    for day, slots in by_day.items():
        items = sorted(slots.items())                 # (slot_start, |energy|) in time order
        max_e = max((e for _, e in items), default=0.0)
        power = 2.0 * max_e                            # kW — charge-card basis (saturated slot)
        cum = 0.0
        b_slot = b_frac = None
        for slot_start, e in items:
            ct = (e / power) if power > 1e-9 else 0.0  # charge-hours this slot
            if b_slot is None and cum + ct >= cap_hours - 1e-12:
                need = max(0.0, cap_hours - cum)
                b_slot = slot_start
                b_frac = max(0.0, min(1.0, (need / ct) if ct > 1e-12 else 0.0))
            cum += ct
        out[day] = {"used_hours": round(cum, 4),
                    "over": cum > cap_hours + 1e-9,
                    "boundary_slot": b_slot, "boundary_frac": b_frac}
    return out


def cap_day_boundaries(completed_slots: Iterable[tuple], tz_name: str,
                       cap_hours: float = CAP_HOURS) -> dict:
    """`{cap_day: (boundary_slot, boundary_frac)}` — the per-cap-day charge-time boundary
    (the slot the cap is reached in, and the within-cap energy fraction of that slot), or
    absent where the day never reaches the cap. Projection of `cap_usage` for the pricing
    seam: look a slot up with `boundaries.get(cap_day_key(ts))`."""
    return {d: (v["boundary_slot"], v["boundary_frac"])
            for d, v in cap_usage(completed_slots, tz_name, cap_hours).items()}


def _within_cap_frac(block_start: str, boundary) -> float:
    """Fraction of this slot's EV energy WITHIN the cap. `boundary` is
    (boundary_slot, boundary_frac) from `cap_usage`, or None / (None, ...) when the cap was
    never reached. A slot before the boundary slot is wholly within-cap (1.0); the boundary
    slot takes its energy fraction; a slot at/after is wholly over-cap (0.0)."""
    if not boundary:
        return 1.0
    b_slot, b_frac = boundary
    if b_slot is None:
        return 1.0
    if block_start < b_slot:
        return 1.0
    if block_start == b_slot:
        return float(b_frac if b_frac is not None else 0.0)
    return 0.0


def _band(frac: float, off: str = "off_peak", on: str = "peak") -> str:
    if frac >= 1.0 - 1e-9:
        return off
    if frac <= 1e-9:
        return on
    return "mixed"


def classify_slot(block_start: str, block_end: str, *, in_off_peak_window: bool,
                  is_dispatch: bool, is_boost: bool = False,
                  boundary=None) -> dict:
    """4-rate classification for one half-hour (docs/iog_6hr_cap_design.md).

    Pure — the caller resolves the inputs:
      in_off_peak_window : the slot is in the tariff's guaranteed off-peak (night)
                           window (RateSchedule.is_off_peak) — cap-independent.
      is_dispatch        : a `completed` dispatch overlaps this slot (EV charging).
      is_boost           : a bump/boost dispatch → EV always peak.
      boundary           : (boundary_slot, boundary_frac) from cap_usage, or None.

    Returns bands + off-peak fractions for the EV and HOUSE portions:
      ev    : "off_peak" | "peak" | "mixed" | None (None = no EV this slot)
      house : "off_peak" | "day" | "mixed"
      *_offpeak_frac : 0..1 (1 = whole slot off-peak; a value strictly between marks
                       the BOUNDARY slot, priced as a blend and confirmed at
                       settlement); boundary=True flags that straddle.

    Rules: (1) house always off-peak in the guaranteed window, regardless of cap;
    (2) EV off-peak within the 6 h allowance, EV-peak beyond (or on Boost);
    (3) out-of-window, a within-cap dispatch also gives the *house* the freebie;
    (4) over-cap out-of-window withdraws that freebie (house → day)."""
    if not is_dispatch:
        return {"ev": None, "ev_offpeak_frac": 0.0,
                "house": "off_peak" if in_off_peak_window else "day",
                "house_offpeak_frac": 1.0 if in_off_peak_window else 0.0,
                "boundary": False}
    ev_frac = 0.0 if is_boost else _within_cap_frac(block_start, boundary)
    # Guaranteed window → house off-peak whatever the cap; else the freebie tracks
    # the within-cap fraction (whole slot when within cap, withdrawn when over).
    house_frac = 1.0 if in_off_peak_window else ev_frac
    return {"ev": _band(ev_frac),
            "ev_offpeak_frac": round(ev_frac, 6),
            "house": _band(house_frac, off="off_peak", on="day"),
            "house_offpeak_frac": round(house_frac, 6),
            "boundary": 0.0 < ev_frac < 1.0}


def price_import_split(classification: dict, *, ev_kwh: float, house_kwh: float,
                       house_offpeak_rate: float, house_day_rate: float,
                       ev_offpeak_rate: Optional[float] = None,
                       ev_peak_rate: Optional[float] = None) -> dict:
    """Cost a slot's EV and HOUSE portions at their distinct 4-rate bands, blending
    the boundary slot by its off-peak fraction. Pure — the caller resolves the
    rates (£/kWh) from the schedules and the kWh from the grid-clipped dispatch
    split; this only does the arithmetic.

    `classification` is the dict from `classify_slot` (uses its `ev_offpeak_frac`
    and `house_offpeak_frac`, each 0..1; a value strictly between is the boundary
    blend). When there's no EV this slot (`ev_kwh == 0`) the EV rates may be None.

    Returns {ev_cost, house_cost, total_cost, effective_rate}, where
    `effective_rate = total_cost / (ev_kwh + house_kwh)` — the single blended rate
    to store on the block's import channel (per-portion detail is kept for display).
    All costs rounded to 6 dp, matching the existing pricing path."""
    evf = float(classification.get("ev_offpeak_frac", 0.0))
    hf = float(classification.get("house_offpeak_frac", 0.0))

    house_rate = hf * house_offpeak_rate + (1.0 - hf) * house_day_rate
    house_cost = house_kwh * house_rate

    if ev_kwh > 1e-12 and ev_offpeak_rate is not None and ev_peak_rate is not None:
        ev_rate = evf * ev_offpeak_rate + (1.0 - evf) * ev_peak_rate
        ev_cost = ev_kwh * ev_rate
    else:
        ev_cost = 0.0

    total = house_cost + ev_cost
    kwh = ev_kwh + house_kwh
    eff = (total / kwh) if kwh > 1e-12 else house_day_rate
    return {"ev_cost": round(ev_cost, 6),
            "house_cost": round(house_cost, 6),
            "total_cost": round(total, 6),
            "effective_rate": round(eff, 6)}


def price_slot(block_start: str, block_end: str, chosen_kwh: float, ev_kwh: float, *,
               in_off_peak_window: bool, is_boost: bool, boundary=None,
               house_offpeak_rate: float, house_day_rate: float,
               ev_offpeak_rate: Optional[float] = None,
               ev_peak_rate: Optional[float] = None) -> dict:
    """One-call IOG per-slot pricing: `classify_slot` + `price_import_split`. The
    single entry point the reconcile seam uses. Pure.

    `chosen_kwh` is the slot's grid import; `ev_kwh` the grid-clipped dispatch EV
    (0 when none, so `house = chosen − ev`). Returns the fields to write on the
    main block: `imp_cost` / `imp_rate` (blended) and the stored EV split
    `imp_kwh_ev` / `imp_cost_ev` / `imp_rate_ev` (all None when there's no EV).

    Capped tariff: pass the `ev_device_*` rates and the cap-day `boundary_utc`.
    Uncapped IOG: pass `boundary_utc=None` and all four rates equal to the slot's
    resolved overlay rate — the whole dispatched slot then reads off-peak and the
    totals are byte-identical to today, while the EV slice is still carved for the
    billing-summary display."""
    house_kwh = max(0.0, chosen_kwh - ev_kwh)
    has_ev = ev_kwh > 1e-12
    cls = classify_slot(block_start, block_end,
                        in_off_peak_window=in_off_peak_window,
                        is_dispatch=has_ev, is_boost=is_boost,
                        boundary=boundary)
    # BL-27: the slot's band SEGMENTS are the single source of truth — a boundary slot
    # carries its true four bands (EV off/peak, house off/day), and the imp_* columns below
    # are PROJECTIONS of them (one decomposition, one rounding). This is sub-penny vs the
    # old independent arithmetic (≤3e-6, settlement-jitter scale); the bill (2dp) is
    # unchanged. exc is left off the segments here; the persist seam applies the exc ratio.
    segs = _slot_segments(cls, ev_kwh=ev_kwh, house_kwh=house_kwh,
                          house_offpeak_rate=house_offpeak_rate,
                          house_day_rate=house_day_rate,
                          ev_offpeak_rate=ev_offpeak_rate,
                          ev_peak_rate=ev_peak_rate) if has_ev else None
    if segs:
        _kwh = ev_kwh + house_kwh
        _tot = round(sum(k * r for (k, r, b, a) in segs), 6)
        _evc = round(sum(k * r for (k, r, b, a) in segs if a == "ev"), 6)
        return {"imp_cost": _tot,
                "imp_rate": round(_tot / _kwh, 6) if _kwh > 1e-12 else house_day_rate,
                "imp_kwh_ev": round(ev_kwh, 6),
                "imp_cost_ev": _evc,
                "imp_rate_ev": round(_evc / ev_kwh, 6) if ev_kwh > 1e-12 else None,
                "segments": segs,
                "classification": cls}
    # House-only slot (no EV): a single house band — price_import_split is exact here.
    priced = price_import_split(cls, ev_kwh=ev_kwh, house_kwh=house_kwh,
                                house_offpeak_rate=house_offpeak_rate,
                                house_day_rate=house_day_rate,
                                ev_offpeak_rate=ev_offpeak_rate,
                                ev_peak_rate=ev_peak_rate)
    return {"imp_cost": priced["total_cost"],
            "imp_rate": priced["effective_rate"],
            "imp_kwh_ev": None,
            "imp_cost_ev": None,
            "imp_rate_ev": None,
            "segments": None,
            "classification": cls}


def _slot_segments(classification: dict, *, ev_kwh: float, house_kwh: float,
                   house_offpeak_rate: float, house_day_rate: float,
                   ev_offpeak_rate: Optional[float] = None,
                   ev_peak_rate: Optional[float] = None) -> list:
    """Decompose a priced slot into its band segments as (kwh, inc_rate, band,
    attribution) tuples — the same split `price_import_split` costs, but kept per-band
    so a boundary slot yields all four. Pure; mirrors `pricing_segments.import_segments`
    (kept inline so `iog_cap` stays dependency-free). Zero-kWh bands are dropped."""
    evf = float(classification.get("ev_offpeak_frac", 0.0))
    hf = float(classification.get("house_offpeak_frac", 0.0))
    out: list = []
    if ev_kwh > 1e-9 and ev_offpeak_rate is not None and ev_peak_rate is not None:
        k_off = ev_kwh * evf
        k_pk = ev_kwh - k_off
        if k_off > 1e-9:
            out.append((round(k_off, 6), ev_offpeak_rate, "off_peak", "ev"))
        if k_pk > 1e-9:
            out.append((round(k_pk, 6), ev_peak_rate, "peak", "ev"))
    if house_kwh > 1e-9:
        k_off = house_kwh * hf
        k_day = house_kwh - k_off
        if k_off > 1e-9:
            out.append((round(k_off, 6), house_offpeak_rate, "off_peak", "house"))
        if k_day > 1e-9:
            out.append((round(k_day, 6), house_day_rate, "day", "house"))
    return out


def compute_iog_split(block_start: str, block_end: str, *, chosen_kwh: float,
                      ev_kwh: float, overlay_rate: float, is_boost: bool,
                      capped: bool, boundary=None,
                      import_sched, ev_off_sched=None, ev_peak_sched=None):
    """Reconcile-seam entry: resolve the four rate bands and price one slot's
    house/EV split. Takes the RateSchedule objects (duck-typed — no import of
    RateSchedule here), so it stays testable and the engine just passes its cached
    schedules. Returns the `price_slot` dict, or None when there's no usable house
    schedule (not a priced IOG tariff → leave existing pricing alone).

    - **Capped** (ev_device schedules present + covering this slot): house on
      day/night (`day_rate_bounds`), EV on `ev_device_off_peak`/`ev_device_peak`,
      cap-day `boundary_utc`, guaranteed-window from `is_off_peak`.
    - **Uncapped** (or capped but an ev_device rate is missing this slot): the whole
      dispatched slot stays on the already-resolved `overlay_rate`, so the total is
      byte-identical to today; the EV slice is still carved for the display."""
    if import_sched is None or import_sched.is_empty():
        return None
    if capped and ev_off_sched is not None and ev_peak_sched is not None:
        op, day = import_sched.day_rate_bounds(block_start)
        ev_off = ev_off_sched.resolve(block_start)
        ev_peak = ev_peak_sched.resolve(block_start)
        if None not in (op, day, ev_off, ev_peak):
            # UNIT FIX (watch #12 origin): these come from the pence-valued RateSchedules
            # (Octopus API native pence; RateSchedule/from_api_records keep it raw), but
            # price_slot wants £/kWh. Convert here, exactly as every other consumer does
            # (_kraken_rate_resolver, _dispatch_overlay_rate → pence/100). Without it the capped
            # 4-rate split priced in PENCE — the 2026-08-18 imp_rate_ev=6.9 break, otherwise only
            # caught downstream by the §4a unit guard. Specified-rate/IOG only; Agile never caps
            # (no ev_device schedules → capped=False → the £ overlay fall-through below).
            op = round(op / 100.0, 6); day = round(day / 100.0, 6)
            ev_off = round(ev_off / 100.0, 6); ev_peak = round(ev_peak / 100.0, 6)
            return price_slot(
                block_start, block_end, chosen_kwh, ev_kwh,
                in_off_peak_window=(import_sched.is_off_peak(block_start) is True),
                is_boost=is_boost, boundary=boundary,
                house_offpeak_rate=op, house_day_rate=day,
                ev_offpeak_rate=ev_off, ev_peak_rate=ev_peak)
        # fall through: an ev_device rate is missing for this slot
    # Uncapped IOG: whole slot on the resolved overlay rate → totals unchanged.
    return price_slot(block_start, block_end, chosen_kwh, ev_kwh,
                      in_off_peak_window=True, is_boost=False, boundary=None,
                      house_offpeak_rate=overlay_rate, house_day_rate=overlay_rate,
                      ev_offpeak_rate=overlay_rate, ev_peak_rate=overlay_rate)
