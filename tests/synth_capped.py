"""
synth_capped.py — a SYNTHETIC capped IOG multi-device scenario (BL-27 validation).

We can't get a real capped DB (and Highgrove's real dispatch never exceeds the 6-hour cap),
so this crafts one controlled noon→noon cap-day — within-cap, a boundary block, over-cap,
and EV+battery simultaneous — with a physical EV charger and battery whose METERED kWh
differs slightly from the dispatch (the realistic case). Every block is priced with
`pricing_segments`, so the stored imp_* columns AND block_segments are mutually consistent
and the segments are the ground truth reader tests assert against.

`build_capped_day(store)` injects the day into any store (a :memory: one, or a copy of a
real DB like Highgrove — "on top of Highgrove"). `CFG` describes the meters for the readers.
"""

import pricing_segments as ps

HOFF, HDAY = 0.05493, 0.323092        # house off-peak / day (inc)
EVOFF, EVPEAK = 0.05493, 0.323092     # EV off-peak / peak (inc). IOG-SMB-TOU has only TWO
VAT = 0.05                            #   rates: EV peak IS the house peak (day) rate.

MAIN, EV, BATT = "electricity_main", "ev_charger", "house_battery"

CFG = {"meters": {
    MAIN:  {"meta": {"timezone": "Europe/London"}},
    EV:    {"meta": {"sub_meter": True, "meter_type": "ev", "parent_meter": MAIN}},
    BATT:  {"meta": {"sub_meter": True, "meter_type": "battery", "parent_meter": MAIN}},
}}

# (start, ev_kwh, house_kwh, ev_off_frac, house_off_frac, zappi_metered, batt_metered)
_DAY = [
    ("2026-09-02T02:00:00", 2.5, 0.5, 1.0, 1.0, 2.525, 0.0),   # night, within cap → all off-peak
    ("2026-09-01T14:00:00", 1.5, 0.5, 1.0, 1.0, 1.515, 0.0),   # out-of-window, within cap → freebie
    ("2026-09-01T16:00:00", 1.0, 1.0, 0.5, 0.5, 1.010, 0.0),   # BOUNDARY — cap crosses mid-block
    ("2026-09-01T18:00:00", 2.0, 1.0, 0.0, 0.0, 2.020, 0.8),   # over cap → EV peak, house day; + battery
    ("2026-09-01T20:00:00", 2.5, 1.5, 0.0, 0.0, 2.525, 1.0),   # over cap, EV + battery simultaneous
    ("2026-09-01T13:00:00", 0.0, 2.0, 1.0, 0.0, 0.0,   1.2),   # house + battery, no EV
]


def _band(frac):
    return "off_peak" if frac >= 1 - 1e-9 else ("peak" if frac <= 1e-9 else "mixed")


def _house_band(frac):
    return "off_peak" if frac >= 1 - 1e-9 else ("day" if frac <= 1e-9 else "mixed")


def build_capped_day(store):
    """Inject the crafted capped multi-device day. Ensures config period 1 + the three
    meters exist (no-op if they already do, e.g. on a Highgrove copy). Returns the list of
    main-block starts written, so tests can iterate them."""
    c = store._conn
    c.execute("INSERT OR IGNORE INTO config_periods (id, effective_from, billing_day, "
              "block_minutes, timezone, currency_symbol, currency_code) "
              "VALUES (1,'2020-01-01T00:00:00',1,30,'Europe/London','£','GBP')")
    for mid, sub, mt in ((MAIN, 0, None), (EV, 1, "ev"), (BATT, 1, "battery")):
        c.execute("INSERT OR IGNORE INTO meters (meter_id, config_period_id, is_sub_meter, "
                  "meter_type) VALUES (?,1,?,?)", (mid, sub, mt))

    starts = []
    for (start, ek, hk, eof, hof, zappi, batt) in _DAY:
        segs = ps.import_segments(
            ev_kwh=ek, house_kwh=hk, house_offpeak_rate=HOFF, house_day_rate=HDAY,
            ev_offpeak_rate=EVOFF, ev_peak_rate=EVPEAK,
            ev_offpeak_frac=eof, house_offpeak_frac=hof, vat=VAT)
        imp_kwh = ps.total_kwh(segs)
        imp_cost = ps.total_cost(segs)
        imp_rate = ps.blended_rate(segs)
        rate_exc = round(imp_rate / (1 + VAT), 6)
        ev_c = ps.attribution_cost(segs, "ev")
        ev_k = ps.attribution_kwh(segs, "ev")
        c.execute(
            "INSERT INTO blocks (block_start, block_end, meter_id, config_period_id, "
            "imp_kwh, imp_rate, imp_cost, imp_rate_exc, imp_cost_exc, "
            "imp_kwh_ev, imp_cost_ev, imp_rate_ev, imp_ev_band, imp_home_band, imp_kwh_api) "
            "VALUES (?,?,?,1,?,?,?,?,?,?,?,?,?,?,?)",
            (start, start, MAIN, imp_kwh, imp_rate, imp_cost, rate_exc,
             round(imp_cost / (1 + VAT), 6),
             (ev_k or None), (ev_c or None),
             (ps.attribution_rate(segs, "ev") or None) if ev_k else None,
             _band(eof) if ek > 0 else None, _house_band(hof) if hk > 0 else None,
             imp_kwh))
        store.set_block_segments(start, MAIN, segs)
        # physical device blocks — priced the OLD way (parent blended rate); the reader
        # migration must re-price EV on the EV band and the battery on the house rate.
        for mid, metered in ((EV, zappi), (BATT, batt)):
            if metered <= 0:
                continue
            c.execute(
                "INSERT INTO blocks (block_start, block_end, meter_id, config_period_id, "
                "imp_kwh, imp_kwh_grid, imp_rate, imp_cost) VALUES (?,?,?,1,?,?,?,?)",
                (start, start, mid, metered, metered, imp_rate,
                 round(metered * imp_rate, 6)))
        starts.append(start)
    store._conn.commit()
    return starts


def reprice_under_cap(store, *, cap_hours: float = 3.0, tz: str = "Europe/London",
                      house_off: float = HOFF, house_day: float = HDAY,
                      ev_off: float = EVOFF, ev_peak: float = EVPEAK,
                      vat: float = VAT) -> int:
    """Re-price a real DB's DISPATCHED blocks under a LOWER cap so the cap actually
    breaches (Highgrove's real dispatch reaches 6h but never exceeds it — a 3h cap turns
    the same real data into a genuine capped account across many days). Uses the real
    `imp_kwh_ev` (the grid-clipped dispatch EV per block) and the real dispatch windows,
    runs `iog_cap` at `cap_hours`, and rewrites the 4-rate columns + block_segments. Returns
    the number of blocks re-priced."""
    from datetime import datetime, time
    from zoneinfo import ZoneInfo
    import iog_cap
    c = store._conn
    rows = c.execute("SELECT raw_start, raw_end, energy_kwh FROM dispatch_history "
                     "WHERE kind='completed' AND raw_start IS NOT NULL AND raw_end IS NOT NULL"
                     ).fetchall()
    boundaries = iog_cap.cap_day_boundaries(
        [(r["raw_start"], r["raw_end"], r["energy_kwh"]) for r in rows], tz,
        cap_hours=cap_hours)

    def _in_window(bs):
        lt = (datetime.fromisoformat(bs[:19]).replace(tzinfo=ZoneInfo("UTC"))
              .astimezone(ZoneInfo(tz)).time())
        return lt >= time(23, 30) or lt < time(5, 30)      # IOG guaranteed off-peak window

    n = 0
    blocks = c.execute(
        "SELECT block_start, block_end, imp_kwh, imp_kwh_ev FROM blocks "
        "WHERE imp_kwh_ev IS NOT NULL AND imp_kwh_ev > 0 "
        "AND meter_id = 'electricity_main'").fetchall()
    for r in blocks:
        bs, be = r["block_start"], r["block_end"] or r["block_start"]
        cls = iog_cap.classify_slot(
            bs, be, in_off_peak_window=_in_window(bs), is_dispatch=True,
            boundary=boundaries.get(iog_cap.cap_day_key(bs, tz)))
        ek = r["imp_kwh_ev"]
        hk = (r["imp_kwh"] or 0.0) - ek
        segs = ps.import_segments(
            ev_kwh=ek, house_kwh=hk, house_offpeak_rate=house_off, house_day_rate=house_day,
            ev_offpeak_rate=ev_off, ev_peak_rate=ev_peak,
            ev_offpeak_frac=cls["ev_offpeak_frac"], house_offpeak_frac=cls["house_offpeak_frac"],
            vat=vat)
        ic, ik = ps.total_cost(segs), ps.total_kwh(segs)
        c.execute(
            "UPDATE blocks SET imp_rate=?, imp_cost=?, imp_rate_exc=?, imp_cost_exc=?, "
            "imp_kwh_ev=?, imp_cost_ev=?, imp_rate_ev=?, imp_ev_band=?, imp_home_band=? "
            "WHERE block_start=? AND meter_id='electricity_main'",
            (ps.blended_rate(segs), ic, round(ps.blended_rate(segs) / (1 + vat), 6),
             round(ic / (1 + vat), 6), ps.attribution_kwh(segs, "ev") or None,
             ps.attribution_cost(segs, "ev") or None, ps.attribution_rate(segs, "ev") or None,
             cls["ev"], cls["house"], bs))
        store.set_block_segments(bs, "electricity_main", segs)
        n += 1
    store._conn.commit()
    return n


def cap_highgrove(src_path: str, dst_path: str, *, cap_hours: float = 3.0) -> str:
    """Copy a real DB (Highgrove) and turn it into a CAPPED account by re-pricing its real
    dispatched blocks under a `cap_hours` cap (default 3h, so the real 4.5–6h dispatch days
    breach it). Real bulk + real dispatch, genuinely capped. Returns dst_path."""
    import os
    import shutil
    from block_store import BlockStore
    shutil.copy(src_path, dst_path)
    os.chmod(dst_path, 0o644)          # the source may be read-only (mounted); make the copy writable
    st = BlockStore(dst_path)
    reprice_under_cap(st, cap_hours=cap_hours)
    return dst_path
