"""
tests/fixture_capped.py — regenerate a CAPPED IOG account from a real (uncapped) DB by
re-pricing its dispatched blocks under a lower cap THROUGH the one model
(reprice.reprice_block). Authoritative by construction: settled-anchored segments,
synthetic-EV-only, clipped+unclipped energies, bands derived from rate.

Real Highgrove dispatch never exceeds the 6 h cap, so a 3 h cap turns the same real data
into a genuine capped account (over-cap days, a boundary block, blends). Replaces the
ad-hoc reprice_under_cap — every derived value now comes from reprice_block.
"""
from datetime import datetime, time
from zoneinfo import ZoneInfo

import iog_cap
import reprice

# Highgrove IOG-SMB-TOU inc rates — two rates only (EV peak == house day).
HOFF, HDAY = 0.05493, 0.323092
VAT = 0.05


def _in_window(bs: str, tz: str) -> bool:
    lt = (datetime.fromisoformat(bs[:19]).replace(tzinfo=ZoneInfo("UTC"))
          .astimezone(ZoneInfo(tz)).time())
    return lt >= time(23, 30) or lt < time(5, 30)      # IOG guaranteed off-peak window


def build_capped_via_reprice(store, *, cap_hours: float = 3.0, tz: str = "Europe/London",
                             house_off: float = HOFF, house_day: float = HDAY,
                             vat: float = VAT) -> int:
    """Re-price every dispatched main block through reprice_block under `cap_hours`, writing
    the emitted segments + derived columns. Returns the number of blocks re-priced.

    Design §3b: eligibility is INHERITED from the block's finalised `imp_rate_ev` (off-peak →
    SMART, peak → BUMP), never re-derived. The synthetic cap accumulates SMART charged-time
    ONLY and flips over-cap SMART segments to peak; BUMP blocks keep their finalised peak."""
    c = store._conn
    mid = (house_off + house_day) / 2.0
    # finalised eligibility per dispatched block, captured BEFORE we overwrite imp_rate_ev
    elig = {}
    for r in c.execute("SELECT block_start, imp_rate_ev FROM blocks "
                       "WHERE imp_kwh_ev > 0 AND meter_id='electricity_main'"):
        elig[r["block_start"]] = (reprice.SMART if (r["imp_rate_ev"] is not None
                                  and r["imp_rate_ev"] < mid) else reprice.BUMP)
    # charged-time cap over SMART dispatch ONLY (a bump never consumes the 6 h allowance)
    rows = c.execute("SELECT raw_start, raw_end, energy_kwh FROM dispatch_history "
                     "WHERE kind='completed' AND raw_start IS NOT NULL AND raw_end IS NOT NULL"
                     ).fetchall()
    smart_intervals = [(r["raw_start"], r["raw_end"], abs(r["energy_kwh"] or 0.0))
                       for r in rows if elig.get(r["raw_start"]) == reprice.SMART]
    boundaries = iog_cap.cap_day_boundaries(smart_intervals, tz, cap_hours=cap_hours)
    rates = {"house_off": house_off, "house_day": house_day,
             "ev_off": house_off, "ev_peak": house_day}          # two rates only
    n = 0
    blocks = c.execute("SELECT block_start, block_end, imp_kwh, imp_kwh_ev FROM blocks "
                       "WHERE imp_kwh_ev IS NOT NULL AND imp_kwh_ev > 0 "
                       "AND meter_id='electricity_main'").fetchall()
    for r in blocks:
        bs = r["block_start"]; be = r["block_end"] or bs
        grid = r["imp_kwh"] or 0.0
        ev = reprice.resolve_ev_energy(reprice.IOG_CAPPED, grid_kwh=grid,
                                       dispatch_ev_kwh=r["imp_kwh_ev"])
        ee = elig.get(bs, reprice.SMART)
        b = reprice.reprice_block(
            block_start=bs, block_end=be, grid_kwh=grid, ev=ev, rates=rates, vat=vat,
            in_off_peak_window=_in_window(bs, tz), ev_eligibility=ee,
            boundary=(boundaries.get(iog_cap.cap_day_key(bs, tz)) if ee == reprice.SMART else None))
        store.set_block_segments(bs, "electricity_main", b["segments"])
        dev = b["devices"]["ev"]
        c.execute(
            "UPDATE blocks SET imp_rate=?, imp_cost=?, imp_rate_exc=?, imp_cost_exc=?, "
            "imp_kwh_ev=?, imp_cost_ev=?, imp_rate_ev=?, imp_ev_band=?, imp_home_band=? "
            "WHERE block_start=? AND meter_id='electricity_main'",
            (b["rate"], b["cost"], b["rate_exc"], b["cost_exc"],
             dev["kwh"] or None, dev["cost"] or None, dev["rate"],
             b["bands"]["ev"], b["bands"]["house"], bs))
        n += 1
    store._conn.commit()
    return n


def cap_highgrove_via_reprice(src_path: str, dst_path: str, *, cap_hours: float = 3.0) -> str:
    import os, shutil
    from block_store import BlockStore
    shutil.copy(src_path, dst_path)
    os.chmod(dst_path, 0o644)
    build_capped_via_reprice(BlockStore(dst_path), cap_hours=cap_hours)
    return dst_path
