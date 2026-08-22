from datetime import datetime, timedelta, timezone
try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo
from collections import defaultdict
import json
import carbon as _carbon
import os
import re


# ─────────────────────────────────────────────────────────────
# Shared period rolodex/spinner (single source of truth)
# ─────────────────────────────────────────────────────────────
# The Billing charts render inside a sandboxed iframe as a self-contained
# document, so they cannot Jinja-{% include %} the shared partial the way
# Usage Stats / Insights do. Instead we read that same partial at generate
# time and inline its <style>+<script> into the page. Reading the one file
# keeps the spinner behaviour identical across all three pages.

def _load_period_spinner_html() -> str:
    """Return the shared spinner partial's <style>+<script>, ready to inline.

    Strips the leading Jinja {# ... #} comment (which would otherwise show as
    literal text in the non-Jinja iframe document). Returns '' if the partial
    can't be found, in which case the Billing page simply keeps its native
    <select> dropdowns.
    """
    path = os.path.join(os.path.dirname(__file__),
                        "web", "templates", "_period_spinner.html")
    try:
        with open(path, encoding="utf-8") as fh:
            raw = fh.read()
    except OSError:
        return ""
    return re.sub(r"\{#.*?#\}", "", raw, flags=re.S).strip()


# ─────────────────────────────────────────────────────────────
# Timezone helpers (credit: KShips)
# ─────────────────────────────────────────────────────────────

def _utc_to_local(dt_naive: datetime, tz: "ZoneInfo") -> datetime:
    """Attach UTC, then convert to local timezone."""
    return dt_naive.replace(tzinfo=timezone.utc).astimezone(tz)


def _parse_block_start(iso_str: str, tz: "ZoneInfo") -> datetime:
    """Parse a UTC ISO block-start string and return it in local time."""
    return _utc_to_local(datetime.fromisoformat(iso_str), tz)

# ─────────────────────────────────────────────────────────────
# Colour helpers
# ─────────────────────────────────────────────────────────────

def adjust_color(hex_color, factor=0.85):
    try:
        h = hex_color.lstrip('#')
        if len(h) == 3:
            h = ''.join(c*2 for c in h)
        r = max(0, min(255, int(int(h[0:2], 16) * factor)))
        g = max(0, min(255, int(int(h[2:4], 16) * factor)))
        b = max(0, min(255, int(int(h[4:6], 16) * factor)))
        return f'#{r:02x}{g:02x}{b:02x}'
    except Exception:
        return '#333333'


# ─────────────────────────────────────────────────────────────
# Meter colour palette
# ─────────────────────────────────────────────────────────────

COLOR_PALETTE = [
    "#1f77b4",  # blue       → main electricity meter
    "#e377c2",  # pink       → first sub-meter
    "#ff7f0e",  # orange     → second sub-meter
    "#7f7f7f",  # grey
    "#8c564b",  # brown
    "#bcbd22",  # lime
    "#d62728",  # red
    "#9467bd",  # purple
    "#17becf",  # cyan
]


def build_meter_colors(blocks):
    """Build meter colour map from a list of blocks (legacy — misses meters not active on sampled day)."""
    all_meters = []
    for block in blocks:
        meters = block.get("meters", {}) or {}
        if "electricity_main" not in all_meters:
            all_meters.append("electricity_main")
        for meter_name, meter in meters.items():
            if (meter or {}).get("meta", {}).get("sub_meter") and meter_name not in all_meters:
                all_meters.append(meter_name)
        main = meters.get("electricity_main", {}) or {}
        export = (main.get("channels", {}) or {}).get("export", {}) or {}
        if (export.get("kwh") or 0.0) > 0 and "electricity_main_export" not in all_meters:
            all_meters.append("electricity_main_export")
    return {m: COLOR_PALETTE[i % len(COLOR_PALETTE)] for i, m in enumerate(all_meters)}


def build_meter_colors_from_config(cfg: dict) -> dict:
    """
    Build meter colour map from the config dict rather than from blocks.
    This ensures all configured sub-meters get a colour even if they were
    added after the first block date and would be absent from a sampled day.
    Order: electricity_main, sub-meters (in config order),
           electricity_main_export (always last).
    """
    all_meters = ["electricity_main"]
    for meter_id, meter_cfg in (cfg.get("meters") or {}).items():
        meta = (meter_cfg.get("meta") or {})
        if meta.get("sub_meter") and meter_id not in all_meters:
            all_meters.append(meter_id)
    # Export gets a slot whether or not there is export data
    for meter_id, meter_cfg in (cfg.get("meters") or {}).items():
        if not (meter_cfg.get("meta") or {}).get("sub_meter"):
            if "export" in (meter_cfg.get("channels") or {}):
                if "electricity_main_export" not in all_meters:
                    all_meters.append("electricity_main_export")
    return {m: COLOR_PALETTE[i % len(COLOR_PALETTE)] for i, m in enumerate(all_meters)}


# ─────────────────────────────────────────────────────────────
# Billing period helpers
# ─────────────────────────────────────────────────────────────

def get_all_billing_periods(blocks, billing_day, tz=None):
    if not blocks:
        return []
    _tz = tz or ZoneInfo("UTC")
    sorted_blocks = sorted([b for b in blocks if b and b.get("start")], key=lambda b: b["start"])
    first = _parse_block_start(sorted_blocks[0]["start"], _tz).replace(tzinfo=None)
    last  = _parse_block_start(sorted_blocks[-1]["start"], _tz).replace(tzinfo=None)

    year, month = first.year, first.month
    if first.day < billing_day:
        month -= 1
        if month == 0:
            month = 12
            year -= 1

    periods = []
    while True:
        period_start = first.replace(year=year, month=month, day=billing_day,
                                     hour=0, minute=0, second=0, microsecond=0, tzinfo=None)
        n_year, n_month = year, month + 1
        if n_month == 13:
            n_month = 1
            n_year += 1
        period_end = period_start.replace(year=n_year, month=n_month)
        periods.append((period_start, period_end))
        if period_end > last.replace(tzinfo=None):
            break
        year, month = n_year, n_month

    return periods


def get_all_calmonth_periods(blocks, tz=None):
    """Calendar months: Jan 1 to Feb 1, Feb 1 to Mar 1, etc."""
    if not blocks:
        return []
    _tz = tz or ZoneInfo("UTC")
    sorted_blocks = sorted([b for b in blocks if b and b.get("start")], key=lambda b: b["start"])
    first = _parse_block_start(sorted_blocks[0]["start"], _tz).replace(tzinfo=None)
    last  = _parse_block_start(sorted_blocks[-1]["start"], _tz).replace(tzinfo=None)
    periods = []
    cur = first.replace(day=1, hour=0, minute=0, second=0, microsecond=0, tzinfo=None)
    last_naive = last.replace(tzinfo=None)
    while True:
        nm = cur.month + 1
        ny = cur.year + (1 if nm > 12 else 0)
        nm = nm - 12 if nm > 12 else nm
        nxt = cur.replace(year=ny, month=nm, day=1)
        periods.append((cur, nxt))
        if nxt > last_naive:
            break
        cur = nxt
    return periods


def get_billing_periods_from_config_periods(config_periods, tz=None):
    """
    Fast alternative to get_billing_periods_from_config_history that takes
    config_periods rows (from store.get_config_periods()) instead of all blocks.

    Avoids loading every block from the database — only needs the config period
    metadata and the first/last block_start per period.

    Returns the same list of (start_datetime, end_datetime) tuples as
    get_billing_periods_from_config_history.
    """
    if not config_periods:
        return []

    _tz = tz or ZoneInfo("UTC")

    # Build synthetic block-like segments from config_period rows
    # Each segment: (billing_day, ef_date, seg_last_naive)
    segments = []
    for p in config_periods:
        if not p.get("effective_from"):
            continue
        billing_day = int(p.get("billing_day") or 1)

        # Parse effective_from UTC → local date
        from datetime import datetime as _dt2, timezone as _tz2
        try:
            ef_iso = p["effective_from"].replace(" ", "T").split(".")[0]
            ef_utc = _dt2.fromisoformat(ef_iso).replace(tzinfo=_tz2.utc)
            ef_local_date = ef_utc.astimezone(_tz).date()
        except Exception:
            continue

        # seg_last: last block date in this period (local), or ef_date if no blocks
        last_bs = p.get("last_block_start")
        if last_bs:
            try:
                last_utc = _dt2.fromisoformat(
                    str(last_bs).replace(" ", "T").split(".")[0]
                ).replace(tzinfo=_tz2.utc)
                last_local = last_utc.astimezone(_tz).date()
            except Exception:
                last_local = ef_local_date
        else:
            last_local = ef_local_date

        segments.append((billing_day, ef_local_date, last_local))

    if not segments:
        return []

    # Use the same period-generation logic as get_billing_periods_from_config_history
    # Build synthetic blocks with _effective_from/_billing_day/_timezone set,
    # then delegate to the existing function.
    # Simpler: reconstruct the period list directly using the same algorithm.

    import calendar as _cal
    from datetime import datetime as _dt3, timedelta as _td

    def _date_to_naive(d):
        return _dt3(d.year, d.month, d.day, 0, 0, 0)

    def _period_end_from_start(p_start, billing_day):
        m = p_start.month + 1
        y = p_start.year + (1 if m > 12 else 0)
        m = m - 12 if m > 12 else m
        try:
            return p_start.replace(year=y, month=m, day=billing_day,
                                   hour=0, minute=0, second=0, microsecond=0, tzinfo=None)
        except ValueError:
            last_day = _cal.monthrange(y, m)[1]
            return p_start.replace(year=y, month=m, day=last_day,
                                   hour=0, minute=0, second=0, microsecond=0, tzinfo=None)

    def _transition_date(ef_local_date, new_bd):
        if ef_local_date.day < new_bd:
            try:
                return ef_local_date.replace(day=new_bd)
            except ValueError:
                import calendar
                last = calendar.monthrange(ef_local_date.year, ef_local_date.month)[1]
                return ef_local_date.replace(day=last)
        else:
            m = ef_local_date.month + 1
            y = ef_local_date.year + (1 if m > 12 else 0)
            m = m - 12 if m > 12 else m
            try:
                return ef_local_date.replace(year=y, month=m, day=new_bd)
            except ValueError:
                import calendar
                last = calendar.monthrange(y, m)[1]
                return ef_local_date.replace(year=y, month=m, day=last)

    # Compute transitions between segments
    transitions = []
    for i in range(len(segments) - 1):
        next_bd, next_ef, _ = segments[i + 1]
        transitions.append(_transition_date(next_ef, next_bd))

    # first_block_date: first block across all periods
    first_block_date = None
    for p in config_periods:
        fb = p.get("first_block_start")
        if fb:
            try:
                from datetime import datetime as _dt4, timezone as _tz3
                fb_utc = _dt4.fromisoformat(
                    str(fb).replace(" ", "T").split(".")[0]
                ).replace(tzinfo=_tz3.utc)
                fb_local = fb_utc.astimezone(_tz).date()
                if first_block_date is None or fb_local < first_block_date:
                    first_block_date = fb_local
            except Exception:
                pass

    if first_block_date is None:
        return []

    periods = []

    for seg_idx, (billing_day, ef_date, seg_last) in enumerate(segments):
        seg_end = _date_to_naive(transitions[seg_idx]) if seg_idx < len(transitions) else None
        seg_last_dt = _date_to_naive(seg_last) + _td(days=1)

        if seg_idx == 0:
            ref = first_block_date
            m, y = ref.month, ref.year
            if ref.day < billing_day:
                m -= 1
                if m == 0: m = 12; y -= 1
            try:
                period_start = _date_to_naive(ref.replace(year=y, month=m, day=billing_day))
            except ValueError:
                period_start = _date_to_naive(ref.replace(
                    year=y, month=m, day=_cal.monthrange(y, m)[1]))
        else:
            period_start = _date_to_naive(transitions[seg_idx - 1])

        truncate_from = None
        if seg_end:
            p0 = period_start
            while True:
                pe0 = _period_end_from_start(p0, billing_day)
                if pe0 > seg_end - _td(days=1):
                    truncate_from = p0
                    break
                p0 = pe0

        while True:
            period_end = _period_end_from_start(period_start, billing_day)

            if seg_end and truncate_from is not None and period_start == truncate_from:
                period_end = seg_end
            elif seg_end and truncate_from is not None and period_end == truncate_from:
                next_ef_naive = _date_to_naive(segments[seg_idx + 1][1])
                if period_start < next_ef_naive <= truncate_from:
                    period_end = seg_end
            elif seg_end and period_end > seg_end:
                period_end = seg_end

            if period_end > period_start:
                entry = (period_start, period_end)
                if not periods or periods[-1] != entry:
                    periods.append(entry)

            if seg_end and period_end >= seg_end:
                break
            if period_end >= seg_last_dt:
                if seg_end and truncate_from is not None and period_end <= truncate_from:
                    truncated = (truncate_from, seg_end)
                    if not periods or periods[-1] != truncated:
                        periods.append(truncated)
                break

            period_start = period_end

    return periods


def get_billing_periods_from_config_history(blocks, tz=None):
    """
    Build billing periods using the historically correct billing_day for each
    config period, using effective_from as the authoritative segment boundary.

    Transition rule: given effective_from date and new billing_day,
      - if effective_from.day < new_bd: transition = new_bd of effective_from.month
      - if effective_from.day >= new_bd: transition = new_bd of effective_from.month + 1
    The old config's last period ends at transition, the new config starts at transition.

    Falls back to get_all_billing_periods() if blocks lack config period info.
    """
    if not blocks:
        return []

    _tz = tz or ZoneInfo("UTC")
    sorted_blocks = sorted(
        [b for b in blocks if b and b.get("start")],
        key=lambda b: b["start"]
    )

    # Check whether blocks carry config period info
    has_config_info = any(b.get("_billing_day") is not None for b in sorted_blocks)
    if not has_config_info:
        billing_day = int(
            (sorted_blocks[0].get("meters", {}) or {})
            .get("electricity_main", {}).get("meta", {}).get("billing_day") or 1
        )
        return get_all_billing_periods(sorted_blocks, billing_day, tz=_tz)

    import calendar as _cal
    from datetime import datetime as _dt2, timezone as _tz2, timedelta as _td

    def _ef_to_local_date(ef_iso):
        """Parse effective_from UTC ISO → local date in configured timezone."""
        ef = _dt2.fromisoformat(ef_iso.replace(" ", "T").split(".")[0])
        ef_utc = ef.replace(tzinfo=_tz2.utc)
        return ef_utc.astimezone(_tz).date()

    def _transition_date(ef_local_date, new_bd):
        """
        Compute the billing period transition date given effective_from (local)
        and new billing_day.
        """
        if ef_local_date.day < new_bd:
            # new billing day hasn't arrived yet this month — transition this month
            try:
                return ef_local_date.replace(day=new_bd)
            except ValueError:
                import calendar
                last = calendar.monthrange(ef_local_date.year, ef_local_date.month)[1]
                return ef_local_date.replace(day=last)
        else:
            # new billing day already passed this month — transition next month
            m = ef_local_date.month + 1
            y = ef_local_date.year + (1 if m > 12 else 0)
            m = m - 12 if m > 12 else m
            try:
                return ef_local_date.replace(year=y, month=m, day=new_bd)
            except ValueError:
                import calendar
                last = calendar.monthrange(y, m)[1]
                return ef_local_date.replace(year=y, month=m, day=last)

    def _date_to_naive(d):
        """Convert date to midnight naive datetime."""
        return _dt2(d.year, d.month, d.day, 0, 0, 0)

    def _period_end_from_start(p_start, billing_day):
        """One billing month from p_start — next occurrence of billing_day."""
        m = p_start.month + 1
        y = p_start.year + (1 if m > 12 else 0)
        m = m - 12 if m > 12 else m
        try:
            return p_start.replace(year=y, month=m, day=billing_day,
                                   hour=0, minute=0, second=0, microsecond=0, tzinfo=None)
        except ValueError:
            last_day = _cal.monthrange(y, m)[1]
            return p_start.replace(year=y, month=m, day=last_day,
                                   hour=0, minute=0, second=0, microsecond=0, tzinfo=None)

    # Build segments: one per unique _effective_from value
    segments = []   # list of (billing_day, effective_from_local_date, seg_last_naive)
    cur_ef = None
    cur_bd = None
    cur_ef_date = None
    seg_last = None

    for b in sorted_blocks:
        ef = b.get("_effective_from") or ""
        bd = b.get("_billing_day") or 1
        dt = _parse_block_start(b["start"], _tz).replace(tzinfo=None)
        dt_date = dt.replace(hour=0, minute=0, second=0, microsecond=0)
        if ef != cur_ef:
            if cur_ef is not None:
                segments.append((cur_bd, cur_ef_date, seg_last))
            cur_ef = ef
            cur_bd = bd
            try:
                cur_ef_date = _ef_to_local_date(ef)
            except Exception:
                cur_ef_date = dt.date()
        seg_last = dt_date

    if cur_ef is not None:
        segments.append((cur_bd, cur_ef_date, seg_last))

    import logging as _logging
    _logging.getLogger(__name__).info(
        "get_billing_periods_from_config_history: %d segments: %s",
        len(segments),
        [(bd, str(ef), sl.strftime('%Y-%m-%d')) for bd, ef, sl in segments]
    )

    if not segments:
        return []

    # transitions[i] = the date where segment i ends and segment i+1 begins.
    # Computed from effective_from and new billing_day:
    #   if ef_date.day < new_bd: transition = new_bd of ef_date.month
    #   if ef_date.day >= new_bd: transition = new_bd of ef_date.month + 1
    transitions = []
    for i in range(len(segments) - 1):
        next_bd, next_ef, _ = segments[i + 1]
        t = _transition_date(next_ef, next_bd)
        transitions.append(t)

    # Generate billing periods for each segment.
    # Each segment runs from the previous transition to the next transition.
    # Segment 0 starts from the billing period containing the first block.
    # Subsequent segments start exactly at their transition date.
    periods = []

    # first_block_date: the local date of the very first block
    first_block_date = _parse_block_start(
        sorted_blocks[0]["start"], _tz
    ).date()

    for seg_idx, (billing_day, ef_date, seg_last_naive) in enumerate(segments):
        # Each segment's periods end at the next segment's effective_from (truncation).
        seg_end     = _date_to_naive(transitions[seg_idx]) if seg_idx < len(transitions) else None
        seg_last_dt = seg_last_naive + _td(days=1)  # exclusive upper bound

        if seg_idx == 0:
            # First segment: billing periods start at the billing_day boundary
            # on or before the first block, and run until truncated at seg_end.
            ref = first_block_date
            m, y = ref.month, ref.year
            if ref.day < billing_day:
                m -= 1
                if m == 0: m = 12; y -= 1
            try:
                period_start = _date_to_naive(ref.replace(year=y, month=m, day=billing_day))
            except ValueError:
                period_start = _date_to_naive(ref.replace(
                    year=y, month=m, day=_cal.monthrange(y, m)[1]))
        else:
            # Subsequent segments: start at the transition date (new billing_day
            # boundary). Bills can only be truncated — the transition date is
            # always a billing_day boundary of the new config.
            period_start = _date_to_naive(transitions[seg_idx - 1])

        # Pre-compute truncate_from: the start of the billing period that
        # contains (seg_end - 1 day). This is the LAST period under the old config.
        # All periods before truncate_from are complete. truncate_from→seg_end is final.
        truncate_from = None
        if seg_end:
            from datetime import timedelta as _td2
            target = seg_end - _td2(days=1)
            p0 = period_start
            while True:
                pe0 = _period_end_from_start(p0, billing_day)
                if pe0 > target:
                    truncate_from = p0
                    break
                p0 = pe0

        while True:
            period_end = _period_end_from_start(period_start, billing_day)

            if seg_end and truncate_from is not None and period_start == truncate_from:
                # This is the final period — clamp to seg_end
                period_end = seg_end
            elif seg_end and truncate_from is not None and period_end == truncate_from:
                # This period ends exactly at truncate_from. If ef_date falls
                # strictly inside (period_start, truncate_from), this period
                # straddles the config change and should extend to seg_end.
                next_ef_naive = _date_to_naive(segments[seg_idx + 1][1])
                if period_start < next_ef_naive <= truncate_from:
                    period_end = seg_end
            elif seg_end and period_end > seg_end:
                period_end = seg_end

            if period_end > period_start:
                entry = (period_start, period_end)
                if not periods or periods[-1] != entry:
                    periods.append(entry)

            if seg_end and period_end >= seg_end:
                break
            if period_end >= seg_last_dt:
                # Data ends — generate truncated final period if not yet done
                if seg_end and truncate_from is not None:
                    if period_end <= truncate_from:
                        # Haven't reached truncate_from yet — append clamped period
                        truncated = (truncate_from, seg_end)
                        if not periods or periods[-1] != truncated:
                            periods.append(truncated)
                break

            period_start = period_end
    return periods


def get_all_quarter_periods(blocks, tz=None):
    """Calendar quarters: Q1=Jan-Apr, Q2=Apr-Jul, Q3=Jul-Oct, Q4=Oct-Jan."""
    if not blocks:
        return []
    _tz = tz or ZoneInfo("UTC")
    sorted_blocks = sorted([b for b in blocks if b and b.get("start")], key=lambda b: b["start"])
    first = _parse_block_start(sorted_blocks[0]["start"], _tz).replace(tzinfo=None)
    last  = _parse_block_start(sorted_blocks[-1]["start"], _tz).replace(tzinfo=None)
    def quarter_start(dt):
        qm = ((dt.month - 1) // 3) * 3 + 1
        return dt.replace(month=qm, day=1, hour=0, minute=0, second=0, microsecond=0, tzinfo=None)
    periods = []
    cur = quarter_start(first)
    while True:
        nm = cur.month + 3
        ny = cur.year + (1 if nm > 12 else 0)
        nm = nm - 12 if nm > 12 else nm
        nxt = cur.replace(year=ny, month=nm, day=1)
        periods.append((cur, nxt))
        if nxt > last:
            break
        cur = nxt
    return periods


def get_all_year_periods(blocks, tz=None):
    """Calendar years: Jan 1 to Jan 1."""
    if not blocks:
        return []
    _tz = tz or ZoneInfo("UTC")
    sorted_blocks = sorted([b for b in blocks if b and b.get("start")], key=lambda b: b["start"])
    first = _parse_block_start(sorted_blocks[0]["start"], _tz).replace(tzinfo=None)
    last  = _parse_block_start(sorted_blocks[-1]["start"], _tz).replace(tzinfo=None)
    periods = []
    cur = first.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0, tzinfo=None)
    while True:
        nxt = cur.replace(year=cur.year + 1)
        periods.append((cur, nxt))
        if nxt > last:
            break
        cur = nxt
    return periods


def calculate_billing_summary_for_period(blocks, period_start, period_end, store=None, tz_name=None):
    meter_summary  = defaultdict(lambda: defaultdict(lambda: {"kwh": 0.0, "cost": 0.0, "read_start": None, "read_end": None}))
    meter_totals   = defaultdict(lambda: {"kwh": 0.0, "cost": 0.0, "read_start": None, "read_end": None})
    main_import_raw = defaultdict(lambda: {"kwh": 0.0, "cost": 0.0})
    # BL-9: IOG house/EV split by rate band (inc + derived exc). ev_by_rate is only
    # populated on dispatched IOG blocks; home_by_rate carries the remainder of every
    # main-import block, so together they reconstruct the grid total. exc is derived
    # from each block's own exc/inc ratio (flat VAT → exact, even across a VAT change).
    ev_by_rate   = defaultdict(lambda: {"kwh": 0.0, "cost": 0.0, "cost_exc": 0.0})
    home_by_rate = defaultdict(lambda: {"kwh": 0.0, "cost": 0.0, "cost_exc": 0.0})
    # Boundary (cap-transition) blocks carry a mixed band and a per-day blended
    # rate; collapse them into ONE transition row each rather than a row per blend.
    ev_transition   = {"kwh": 0.0, "cost": 0.0, "cost_exc": 0.0}
    home_transition = {"kwh": 0.0, "cost": 0.0, "cost_exc": 0.0}
    main_export_raw = {"kwh": 0.0, "cost": 0.0}  # raw main meter export totals
    # Main-meter-only daily accumulators for total_cost.
    # Matches _fmt_total in server.py: raw main meter imp/exp/sc per day,
    # rounded to 4dp, net_cost = round(imp + sc - exp, 2) per day, summed.
    # Sub-meter per-block subtraction is used for the detailed rate breakdown
    # display only — not for total_cost.
    _main_day_imp = defaultdict(float)
    _main_day_exp = defaultdict(float)
    _main_day_sc  = defaultdict(float)
    standing_by_day = defaultdict(float)
    charged_days   = set()
    meter_meta     = {}

    sorted_blocks = sorted([b for b in blocks if b and b.get("start")], key=lambda b: b["start"])

    _period_utc_starts = []  # UTC starts of blocks that fall IN this period (for total_cost)

    for block in sorted_blocks:
        # block["start"] is a UTC ISO string; parse it and convert to local naive
        # so BST blocks at 23:xx UTC compare correctly against local period boundaries.
        _block_utc = datetime.fromisoformat(block["start"])
        _tz_name = ((block.get("meters") or {})
                    .get("electricity_main", {}) or {})
        _tz_name = (_tz_name.get("meta") or {}).get("timezone") or block.get("_timezone")
        try:
            from zoneinfo import ZoneInfo as _ZI
            block_start = _block_utc.replace(tzinfo=_ZI("UTC")).astimezone(
                _ZI(_tz_name or "UTC")).replace(tzinfo=None)
        except Exception:
            block_start = _block_utc
        if not (period_start <= block_start < period_end):
            continue
        _period_utc_starts.append(block["start"])

        day_key = block_start.date()
        meters = block.get("meters", {}) or {}

        # ── Pass 1: accumulate sub-meter grid-attributed kwh/cost per rate ──
        sub_by_rate = defaultdict(lambda: {"kwh": 0.0, "cost": 0.0})
        for meter_name, meter in meters.items():
            if not (meter or {}).get("meta", {}).get("sub_meter"):
                continue
            for channel_name, channel in (meter.get("channels", {}) or {}).items():
                if channel_name.lower().endswith("export"):
                    continue
                try:
                    channel = channel or {}
                    rate = round(float(channel.get("rate_used", channel.get("rate")) or 0.0), 4)
                    # Use kwh_grid (grid-attributed portion) not kwh (total device consumption)
                    sub_by_rate[rate]["kwh"]  += float(channel.get("kwh_grid", channel.get("kwh")) or 0.0)
                    sub_by_rate[rate]["cost"] += float(channel.get("cost") or 0.0)
                except Exception:
                    pass

        # ── Pass 2: accumulate all meters, subtracting sub-meter totals from main import ──
        for meter_name, meter in meters.items():
            meter_m = (meter or {}).get("meta", {}) or {}
            is_sub  = meter_m.get("sub_meter", False)
            if is_sub:
                display_name = meter_m.get("device") or meter_name.replace("_", " ").title()
            else:
                display_name = meter_m.get("site") or meter_m.get("site_name") or meter_name.replace("_", " ").title()
            if not display_name:
                display_name = meter_name.replace("_", " ").title()
            is_main_import = (meter_name == "electricity_main")

            for channel_name, channel in (meter.get("channels", {}) or {}).items():
                # Sub-meter export channels are already captured in the main meter's
                # export total — skip them to prevent phantom export sections in the bill.
                if is_sub and channel_name.lower().endswith("export"):
                    continue
                try:
                    channel = channel or {}
                    channel_m = (channel.get("meta", {}) or {})

                    # Sub-meter rows show the device's GRID-attributed share
                    # (kwh_grid), NOT its total consumption (kwh_total). Total would
                    # include energy the device drew from battery/solar, which never
                    # touched the grid — so the breakdown would overshoot the grid
                    # import total and the kWh would no longer correspond to the
                    # grid-based cost. This mirrors the sub_by_rate reconciliation
                    # above and the Usage-Stats aggregation. The main meter keeps its
                    # total (grid) figure. (Total_cost is unaffected — see note at top.)
                    if is_sub:
                        kwh = float(channel.get("kwh_grid",
                                    channel.get("kwh_total", channel.get("kwh"))) or 0.0)
                    else:
                        kwh = float(channel.get("kwh_total", channel.get("kwh")) or 0.0)
                    cost = float(channel.get("cost") or 0.0)
                    rate = round(float(channel.get("rate_used", channel.get("rate")) or 0.0), 4)
                    is_export = channel_name.lower().endswith("export")

                    if is_export:
                        if is_main_import:
                            main_export_raw["kwh"]  += float(channel.get("kwh") or 0.0)
                            main_export_raw["cost"] += float(channel.get("cost") or 0.0)
                        cost = -abs(cost)
                    elif is_main_import:
                        # Store raw total before sub-meter subtraction
                        main_import_raw[rate]["kwh"]  += kwh
                        main_import_raw[rate]["cost"] += cost
                        # BL-9: split this block's import into EV (dispatch-derived,
                        # stored) and Home (remainder), grouped by each portion's own
                        # rate. exc derived from the block's exc/inc ratio (flat VAT →
                        # exact). ev_by_rate only on dispatched IOG blocks; home_by_rate
                        # carries every main block's remainder → they sum to the total.
                        _cx = channel.get("cost_exc")
                        if _cx is not None and cost:
                            _ratio = float(_cx) / cost
                        else:
                            # Not-yet-settled block has no stored ex-VAT cost. Counting
                            # its kWh at £0 exc would DILUTE the displayed ex-VAT rate
                            # (EV/Home read below the true off-peak rate until every
                            # block settles). Fall back to the same inc ÷ (1+VAT) basis
                            # the rest of the bill uses for uncovered slots, so the rate
                            # is correct immediately and self-corrects to the exact
                            # figure when the block settles and stamps cost_exc.
                            _vat = store.vat_rate_at(block.get("start")) if store is not None else 0.05
                            _ratio = 1.0 / (1.0 + _vat)
                        # BL-27: price the split from the block's priced SEGMENTS when they
                        # travel with it (block_store attaches them to the import channel).
                        # Each segment already carries its own inc/exc rate + attribution, so
                        # a boundary block simply presents its constituent rate rows — the
                        # mixed-band "transition" special-case is retired for segment blocks.
                        # Legacy blocks (pre-backfill / no segments) keep the column path.
                        _segs = channel.get("segments")
                        if _segs:
                            for _s in _segs:
                                _sk = float(_s.get("kwh") or 0.0)
                                if _sk <= 1e-9:
                                    continue
                                _sr = round(float(_s.get("inc_rate") or 0.0), 4)
                                _sc = _sk * float(_s.get("inc_rate") or 0.0)
                                _sxr = _s.get("exc_rate")
                                _sx = (_sk * float(_sxr)) if _sxr is not None \
                                    else _sc * _ratio
                                _grp = ev_by_rate if _s.get("attribution") == "ev" \
                                    else home_by_rate
                                _grp[_sr]["kwh"]      += _sk
                                _grp[_sr]["cost"]     += _sc
                                _grp[_sr]["cost_exc"] += _sx
                        else:
                            _evk = float(channel.get("kwh_ev") or 0.0)
                            _evc = float(channel.get("cost_ev") or 0.0)
                            if _evk > 1e-9:
                                _evx = _evc * _ratio
                                if channel.get("ev_band") == "mixed":
                                    ev_transition["kwh"]      += _evk
                                    ev_transition["cost"]     += _evc
                                    ev_transition["cost_exc"] += _evx
                                else:                          # clean band → exact rate row
                                    _evr = round(float(channel.get("rate_ev") or 0.0), 4)
                                    ev_by_rate[_evr]["kwh"]      += _evk
                                    ev_by_rate[_evr]["cost"]     += _evc
                                    ev_by_rate[_evr]["cost_exc"] += _evx
                            _hk = kwh - _evk
                            _hc = cost - _evc
                            if _hk > 1e-9:
                                _hx = _hc * _ratio
                                if channel.get("home_band") == "mixed":
                                    home_transition["kwh"]      += _hk
                                    home_transition["cost"]     += _hc
                                    home_transition["cost_exc"] += _hx
                                else:
                                    _hr = round(_hc / _hk, 4)
                                    home_by_rate[_hr]["kwh"]      += _hk
                                    home_by_rate[_hr]["cost"]     += _hc
                                    home_by_rate[_hr]["cost_exc"] += _hx
                        # Use kwh_remainder directly when available. With no
                        # sub-meters this key is absent (NULL in DB, omitted by
                        # reconstruction), so the raw kwh is kept unchanged —
                        # api+mini blocks are identical to CAD here.
                        # Floor the sub-meter subtraction only when the main cost is
                        # >= 0 (kills a power-integration over-subtraction artifact);
                        # a genuinely negative main cost (Agile plunge-price credit)
                        # must pass through, not clamp to 0.
                        if "kwh_remainder" in channel:
                            kwh  = float(channel["kwh_remainder"])
                            _c = cost - sub_by_rate[rate]["cost"]
                            cost = _c if cost < 0 else max(0.0, _c)
                        else:
                            kwh  = max(0.0, kwh  - sub_by_rate[rate]["kwh"])
                            _c = cost - sub_by_rate[rate]["cost"]
                            cost = _c if cost < 0 else max(0.0, _c)

                    key = f"{display_name} / {channel_name.replace('_', ' ').title()}"
                    if key not in meter_meta:
                        meter_meta[key] = {
                            "site":        meter_m.get("site"),
                            "device":      meter_m.get("device"),
                            "mpan":        channel_m.get("mpan"),
                            "tariff":      channel_m.get("tariff"),
                            "is_submeter": bool(meter_m.get("sub_meter")),
                        }
                    meter_summary[key][rate]["kwh"]  += kwh
                    meter_summary[key][rate]["cost"] += cost
                    meter_totals[key]["kwh"]         += kwh
                    meter_totals[key]["cost"]        += cost
                    meter_totals[key]["is_submeter"]  = bool(meter_m.get("sub_meter"))

                    if not meter_m.get("sub_meter"):
                        rs = channel.get("read_start")
                        re = channel.get("read_end")
                        # A cumulative meter register only increases, so the
                        # period's opening read is the SMALLEST real read and the
                        # closing is the LARGEST. Ignore 0/None reads: gap /
                        # interpolated / reset blocks and register-less sources
                        # (CAD power sensors, API Measurements) carry 0, and the old
                        # MIN-start / last-wins-end aggregation let a single such
                        # block drag the displayed Start/End to 0.000 even when real
                        # reads exist for the period.
                        for _tgt in (meter_summary[key][rate], meter_totals[key]):
                            if rs is not None and rs > 0 and (
                                    _tgt["read_start"] is None or rs < _tgt["read_start"]):
                                _tgt["read_start"] = rs
                            if re is not None and re > 0 and (
                                    _tgt["read_end"] is None or re > _tgt["read_end"]):
                                _tgt["read_end"] = re
                except Exception:
                    pass

        # Standing charge
        for meter in (meters or {}).values():
            if (meter or {}).get("meta", {}).get("sub_meter"):
                continue
            sc = float((meter or {}).get("standing_charge") or 0.0)
            if sc > 0:
                # Start-of-day: keep the first non-zero charge seen (blocks are
                # processed in ascending order). Not MAX (over-bills on a drop).
                if not standing_by_day.get(day_key):
                    standing_by_day[day_key] = sc
            elif day_key not in charged_days:
                standing_by_day.setdefault(day_key, 0.0)
        charged_days.add(day_key)

    # ── Round accumulated raw values once at the end ──
    # This matches how Octopus bills: sum raw block costs, round once to 2dp.
    # No intermediate per-day rounding — that would introduce cumulative error.
    for key in meter_summary:
        for rate in meter_summary[key]:
            meter_summary[key][rate]["kwh"]  = round(meter_summary[key][rate]["kwh"],  3)
            meter_summary[key][rate]["cost"] = round(meter_summary[key][rate]["cost"], 2)
        meter_totals[key]["kwh"]  = round(meter_totals[key]["kwh"],  3)
        meter_totals[key]["cost"] = round(meter_totals[key]["cost"], 2)
    total_standing = round(sum(standing_by_day.values()), 2)
    # Round main_import_raw for display
    for rate in main_import_raw:
        main_import_raw[rate]["kwh"]  = round(main_import_raw[rate]["kwh"],  3)
        main_import_raw[rate]["cost"] = round(main_import_raw[rate]["cost"], 2)
    # BL-9: round the house/EV split — costs to 3dp so the per-rate breakdown shows
    # like the statement (e.g. £8.001), while the section Total stays 2dp.
    for _grp in (ev_by_rate, home_by_rate):
        for _r in _grp:
            _grp[_r]["kwh"]      = round(_grp[_r]["kwh"], 3)
            _grp[_r]["cost"]     = round(_grp[_r]["cost"], 3)
            _grp[_r]["cost_exc"] = round(_grp[_r]["cost_exc"], 3)
    for _t in (ev_transition, home_transition):
        _t["kwh"]      = round(_t["kwh"], 3)
        _t["cost"]     = round(_t["cost"], 3)
        _t["cost_exc"] = round(_t["cost_exc"], 3)
    # total_cost = sum of daily net_cost values.
    # Same method as api_blocks_summary: round each day's imp/sc/exp to 4dp,
    # compute net = round(imp + sc - exp, 2) per day, sum across days.
    # This ensures billing chart total agrees with Usage Stats at every level.
    # Populate main-meter-only daily accumulators from blocks.
    # Raw main meter imp/exp/sc per day — no sub-meter adjustment.
    # Derive timezone from first block metadata (same as main loop does per-block).
    _first_tz_name = None
    for _fb in sorted_blocks:
        _fmeta = ((_fb.get("meters") or {}).get("electricity_main") or {})
        _first_tz_name = (_fmeta.get("meta") or {}).get("timezone") or _fb.get("_timezone")
        if _first_tz_name:
            break
    try:
        from zoneinfo import ZoneInfo as _ZI2
        _main_tz = _ZI2(_first_tz_name or "UTC")
    except Exception:
        from zoneinfo import ZoneInfo as _ZI2
        _main_tz = _ZI2("UTC")
    for _blk in sorted_blocks:
        _blk_dt = datetime.fromisoformat(_blk["start"])
        _blk_local = _blk_dt.replace(tzinfo=timezone.utc).astimezone(_main_tz).replace(tzinfo=None)
        if not (period_start <= _blk_local < period_end):
            continue
        _blk_day = _blk_local.date()
        _blk_meters = _blk.get("meters", {}) or {}
        for _blk_mid, _blk_meter in _blk_meters.items():
            _blk_meta = (_blk_meter or {}).get("meta", {}) or {}
            if _blk_meta.get("sub_meter"):
                continue  # main meter only
            _blk_sc = float((_blk_meter or {}).get("standing_charge") or 0.0)
            if _blk_sc > 0 and not _main_day_sc.get(_blk_day):
                # Start-of-day (first non-zero, ascending order); not MAX.
                _main_day_sc[_blk_day] = _blk_sc
            for _blk_ch_name, _blk_ch in ((_blk_meter or {}).get("channels", {}) or {}).items():
                if not _blk_ch:
                    continue
                _blk_cost = float((_blk_ch or {}).get("cost") or 0.0)
                if _blk_ch_name.lower().endswith("export"):
                    _main_day_exp[_blk_day] += abs(_blk_cost)
                else:
                    _main_day_imp[_blk_day] += _blk_cost

    # Compute total_cost via BlockStore.compute_period_net — the single shared
    # implementation used by Live Power, Usage Stats and billing chart.
    # Fall back to local accumulation if store not provided.
    if store is not None and tz_name and _period_utc_starts:
        # Bounds from the blocks IN THIS PERIOD only — using min/max of ALL
        # sorted_blocks spanned the whole dataset, so every period returned the
        # same (all-time) net. Now compute_period_net runs over just this period.
        _utc_s = min(_period_utc_starts)
        _utc_e_dt = max(datetime.fromisoformat(s) for s in _period_utc_starts)
        # utc_e must be past the last block — add 30 minutes
        from datetime import timedelta as _timedelta
        _utc_e = (_utc_e_dt + _timedelta(minutes=30)).strftime("%Y-%m-%dT%H:%M:%S")
        total_cost = store.compute_period_net(_utc_s, _utc_e, tz_name)
    elif store is not None and tz_name:
        total_cost = 0.0   # no blocks fall in this period
    else:
        _daily_nets = [
            round(
                round(_main_day_imp[_d], 4)
                + round(_main_day_sc.get(_d, 0.0), 4)
                - round(_main_day_exp.get(_d, 0.0), 4),
                2
            )
            for _d in sorted(_main_day_imp)
        ]
        total_cost = round(sum(_daily_nets), 2)

    # Collect main meter read_start / read_end for the raw import header
    _main_key = next((k for k in meter_totals if "Electricity Main / Import" in k
                      and not meter_totals[k].get("is_submeter")), None)
    main_import_reads = {
        "read_start": meter_totals[_main_key]["read_start"] if _main_key else None,
        "read_end":   meter_totals[_main_key]["read_end"]   if _main_key else None,
    }

    return {
        "start":             period_start,
        "end":               period_end,
        "meters":            meter_summary,
        "totals":            meter_totals,
        "main_import_raw":   dict(main_import_raw),
        "ev_by_rate":        dict(ev_by_rate),
        "home_by_rate":      dict(home_by_rate),
        "ev_transition":     ev_transition,
        "home_transition":   home_transition,
        "main_import_reads": main_import_reads,
        "standing":          standing_by_day,
        "total_standing":    total_standing,
        "total_cost":        total_cost,
        "meter_meta":        meter_meta,
    }


# ─────────────────────────────────────────────────────────────
# Billing summary renderer
# ─────────────────────────────────────────────────────────────

# Above this many distinct rates in one channel's period breakdown, collapse the
# per-rate rows into a single kWh-weighted average row. Agile has ~48 rates a day →
# hundreds across a bill period, which makes the billing summary unreadable; the
# Total row is unchanged, so only the (unusably long) per-rate detail is folded.
_MAX_RATE_ROWS = 5


def _bill_rate_rows(channels, currency):
    """Render rate breakdown rows for a channel dict {rate: {kwh, cost}}.
    Returns (html, total_kwh, total_cost) where totals are sum of rounded per-rate
    values. When a channel has more than _MAX_RATE_ROWS distinct rates (Agile), the
    rows collapse to ONE weighted-average row — the totals are computed identically
    (sum of the same rounded per-rate values), so the Total row never moves."""
    rates = sorted(channels)
    total_kwh = 0.0
    total_cost = 0.0
    if len(rates) > _MAX_RATE_ROWS:
        for rate in rates:
            total_kwh  += round(channels[rate]["kwh"],  3)
            total_cost += round(channels[rate]["cost"], 2)
        avg = (total_cost / total_kwh) if total_kwh else 0.0   # kWh-weighted £/kWh
        cost_str = f"({-total_cost:.2f})" if total_cost < 0 else f"{total_cost:.2f}"
        rows = f"""
            <tr>
              <td></td><td>{avg:.4f} <span style="opacity:0.6;">(avg of {len(rates)} rates)</span></td>
              <td>{total_kwh:.3f}</td><td>{cost_str}</td>
            </tr>"""
        return rows, round(total_kwh, 3), round(total_cost, 2)
    rows = ""
    for rate in rates:
        d = channels[rate]
        kwh      = round(d["kwh"],  3)
        cost_val = round(d["cost"], 2)
        total_kwh  += kwh
        total_cost += cost_val
        cost_str = f"({-cost_val:.2f})" if cost_val < 0 else f"{cost_val:.2f}"
        rows += f"""
            <tr>
              <td></td><td>{rate:.4f}</td>
              <td>{kwh:.3f}</td><td>{cost_str}</td>
            </tr>"""
    return rows, round(total_kwh, 3), round(total_cost, 2)


def _bill_total_row(kwh, cost):
    cost_str = f"({-cost:.2f})" if cost < 0 else f"{cost:.2f}"
    return f"""
        <tr class="channel-total">
          <td>Total</td><td></td><td>{kwh:.3f}</td><td>{cost_str}</td>
        </tr>"""


# Rate keys within this gap (£/kWh) are the SAME tariff band jittered by Octopus's
# per-half-hour settlement rounding (e.g. 0.323091/0.323092/0.323097 → 0.3230/0.3231/
# 0.3232). Fold them into one displayed band. Sub-penny, so it never merges two real
# IOG bands (off-peak ≈5p vs peak ≈32p vs any day rate are pence apart) — and the
# cap-transition block is a separate 'mixed' bucket, never grouped by rate at all.
_SPLIT_BAND_EPS = 0.0015


def _bill_split_rows(summary, currency, exc=False):
    """BL-9: EV/Home breakdown rows for the IOG 'Import — total grid' section — the
    house-vs-car split Octopus itemises on the statement. EV and Home are interleaved
    per rate band (matching the bill); near-identical rates are collapsed into one band
    (see _SPLIT_BAND_EPS) so per-half-hour settlement jitter doesn't shatter a band into
    look-alike rows, exactly as the bill-method view already does. The boundary
    (cap-transition) blocks are a separate 'mixed' bucket → one EV and one Home row at
    their blended average, untouched by the collapse. The rate shown is cost ÷ kWh, so
    it's correct in both inc and (derived) ex-VAT bases.

    Returns an HTML string, or None when there's no EV in the period (the caller
    then shows the plain per-rate rows)."""
    ev_by_rate   = summary.get("ev_by_rate") or {}
    home_by_rate = summary.get("home_by_rate") or {}
    ev_tr        = summary.get("ev_transition") or {}
    home_tr      = summary.get("home_transition") or {}
    _ck = "cost_exc" if exc else "cost"
    if (sum(v["kwh"] for v in ev_by_rate.values()) + (ev_tr.get("kwh") or 0.0)) <= 1e-9:
        return None

    def _row(label, kwh, cost, note=""):
        rate = (cost / kwh) if kwh else 0.0
        cs = f"({-cost:.3f})" if cost < 0 else f"{cost:.3f}"   # 3dp, as the bill
        return (f'\n        <tr><td>{label}</td><td>{rate:.4f}{note}</td>'
                f'<td>{kwh:.3f}</td><td>{cs}</td></tr>')

    # Cluster the rate keys of BOTH series together into bands (adjacent rates within
    # _SPLIT_BAND_EPS merge), so EV and Home stay aligned on the same displayed band.
    _rates = sorted(set(ev_by_rate) | set(home_by_rate))
    _bands: list[list[float]] = []
    for r in _rates:
        if _bands and (r - _bands[-1][-1]) <= _SPLIT_BAND_EPS:
            _bands[-1].append(r)
        else:
            _bands.append([r])

    html = ""
    for band in _bands:
        ek = ec = hk = hc = 0.0
        for r in band:
            e = ev_by_rate.get(r)
            if e:
                ek += e["kwh"]; ec += e.get(_ck, 0.0)
            h = home_by_rate.get(r)
            if h:
                hk += h["kwh"]; hc += h.get(_ck, 0.0)
        if ek > 1e-9:
            html += _row("EV", ek, ec)
        if hk > 1e-9:
            html += _row("Home", hk, hc)
    _note = ' <span style="opacity:0.6;">(transition)</span>'
    if (ev_tr.get("kwh") or 0.0) > 1e-9:
        html += _row("EV", ev_tr["kwh"], ev_tr.get(_ck, 0.0), _note)
    if (home_tr.get("kwh") or 0.0) > 1e-9:
        html += _row("Home", home_tr["kwh"], home_tr.get(_ck, 0.0), _note)
    return html


def _collapse_rate_kwh(rate_kwh):
    """Side-panel rate breakdown collapse. `rate_kwh` is a {rate: kwh} mapping (the
    day chart's `summary_rates[meter]`). Returns a list of (kwh, rate, n) tuples to
    render: one per rate normally, or a SINGLE kWh-weighted-average tuple (with n =
    the rate count) when there are more than _MAX_RATE_ROWS non-trivial rates — the
    same threshold as the billing summary's _bill_rate_rows, so the side panel's
    Direct-import and per-device lines fold the same way on Agile. Weighting is by
    kWh (Σ rate·kwh ÷ Σ kwh) since only kWh is available here; that equals cost ÷ kWh
    when cost = rate·kwh, so it agrees with the summary's cost-weighted average. Only
    rates with kwh > 0.0001 are counted (matching the renderers' own filter)."""
    items = [(k, r) for r, k in sorted(rate_kwh.items()) if k > 0.0001]
    if len(items) > _MAX_RATE_ROWS:
        tot_kwh = sum(k for k, _ in items)
        avg = (sum(r * k for k, r in items) / tot_kwh) if tot_kwh else 0.0
        return [(tot_kwh, avg, len(items))]
    return [(k, r, None) for k, r in items]


from decimal import Decimal as _Decimal, ROUND_HALF_EVEN as _ROUND_HALF_EVEN


def bankers_round(x, ndigits=0):
    """Round half-to-even (banker's rounding) — how Octopus rounds a bill (0.015→0.02,
    0.025→0.02). Uses Decimal(str(x)) so exact decimal halves round correctly, which
    the float built-in round() cannot (0.015 is 0.01499… in binary). BL-24 groundwork."""
    q = _Decimal(1).scaleb(-int(ndigits))
    return float(_Decimal(str(x)).quantize(q, rounding=_ROUND_HALF_EVEN))


def octopus_bill_total(slots, vat_rate=0.05):
    """Ex-VAT energy total for a set of (kwh, exc_rate) slots (exc_rate £/kWh), rounding
    only at the TOTAL (whole penny); VAT on top. Empirically — verified against a real
    Octopus bill — the per-half-hour figures are summed RAW; Octopus does NOT round each
    half-hour's kWh/cost (that would zero the many sub-0.01 kWh slots). See
    _bill_method_breakdown, which is what the Billing summary uses. Returns pence + £."""
    exc_pence = bankers_round(sum(kwh * float(exc_rate) * 100.0 for kwh, exc_rate in slots), 0)
    inc_pence = bankers_round(exc_pence * (1.0 + vat_rate), 0)
    return {"exc_pence": exc_pence, "inc_pence": inc_pence,
            "exc_gbp": round(exc_pence / 100.0, 2),
            "inc_gbp": round(inc_pence / 100.0, 2)}


def bill_slots_from_blocks(blocks, vat_rate=0.05):
    """Extract (kwh, exc_rate) main-import slots from block dicts for octopus_bill_total.
    Uses the stored exc-VAT cost (BL-23 imp_cost_exc → channel 'cost_exc') to derive the
    exc rate when present; otherwise falls back to the inc rate ÷ (1+VAT) — an
    approximation until a re-import captures the real exc figure."""
    slots = []
    for b in blocks or []:
        imp = (((b or {}).get("meters") or {}).get("electricity_main") or {})
        imp = (imp.get("channels") or {}).get("import") or {}
        kwh = imp.get("kwh")
        if not kwh:
            continue
        # Prefer the stored published ex-VAT rate, then the ex-VAT cost, then the
        # inc rate ÷ (1+VAT) approximation (until a re-import captures real exc).
        if imp.get("rate_exc") is not None:
            exc_rate = float(imp["rate_exc"])
        elif imp.get("cost_exc") is not None:
            exc_rate = float(imp["cost_exc"]) / float(kwh)
        else:
            rate = imp.get("rate")
            if rate is None:
                continue
            exc_rate = float(rate) / (1.0 + vat_rate)
        slots.append((float(kwh), exc_rate))
    return slots


def _bill_method_breakdown(blocks, period_vat=None, standing_inc_by_day=None):
    """BL-24: ex-VAT bill-method breakdown for a period — import energy by rate band, plus
    ex-VAT standing, a VAT row and the inc total. Matches how Octopus actually reconciles
    (verified against a real bill): the per-half-hour figures are summed RAW and rounding
    happens only at the SUBTOTAL — NOT per half-hour (that destroys the many sub-0.01 kWh
    peak slots) and NOT the 1dp kWh the bill shows for display. VAT is derived from a real
    inc/exc pair (snapped 0/5/20%; 0% once VAT is removed), else the default. Ex-VAT cost is
    the stored exc where captured, else the billed inc ÷ (1+VAT) — same for standing, so the
    standing charge still lands before ex-VAT capture. Returns a dict or None."""
    from collections import defaultdict as _dd
    blocks = list(blocks or [])
    # ── Pass 1: VAT rate from a real inc/exc pair (else default) ────────────────
    raw_vat = None
    for b in blocks:
        imp = (((b or {}).get("meters") or {}).get("electricity_main") or {})
        imp = (imp.get("channels") or {}).get("import") or {}
        c, ce = imp.get("cost"), imp.get("cost_exc")
        if c and ce and ce != 0:
            raw_vat = float(c) / float(ce) - 1.0
            break
    # VAT rate: derived from a real inc/exc pair where present; else the period's
    # statutory rate from the VAT calendar (passed in), NOT a hardcoded 5% — so a 0%/20%
    # period labels + fills correctly. The *amount* is computed as inc − exc below, which
    # is right regardless of the rate; this `vat` only sets the label and the fallbacks.
    _default_vat = 0.05 if period_vat is None else float(period_vat)
    vat = _default_vat if raw_vat is None else min((0.0, 0.05, 0.20),
                                                   key=lambda x: abs(x - raw_vat))
    # ── Pass 2: raw ex-VAT sums per band + ex-VAT standing per day ──────────────
    bands = _dd(lambda: {"kwh": 0.0, "exc": 0.0, "inc": 0.0})
    tot_kwh = with_exc = 0.0
    stand_exc_day = {}   # local day → ex-VAT standing (first non-zero; stored else inc/(1+VAT))
    for b in blocks:
        m = ((b or {}).get("meters") or {}).get("electricity_main") or {}
        imp = (m.get("channels") or {}).get("import") or {}
        d = (b.get("start") or "")[:10]
        if d and d not in stand_exc_day:
            se = m.get("standing_charge_exc")
            if se:
                stand_exc_day[d] = float(se)
            else:
                sc = m.get("standing_charge") or 0
                if sc:
                    stand_exc_day[d] = float(sc) / (1.0 + vat)
        kwh = imp.get("kwh") or 0
        if not kwh:
            continue
        tot_kwh += kwh
        inc_rate = imp.get("rate")
        if imp.get("cost_exc") is not None or imp.get("rate_exc") is not None:
            with_exc += kwh
        # Raw ex-VAT cost for this half-hour: the billed cost, ex-VAT.
        if imp.get("cost_exc") is not None:
            xc = float(imp["cost_exc"])
        elif imp.get("cost") is not None:
            xc = float(imp["cost"]) / (1.0 + vat)
        elif inc_rate is not None:
            xc = kwh * (float(inc_rate) / (1.0 + vat))
        else:
            continue
        # Inc cost for this slot — paired with xc so the VAT amount is inc − exc (exact
        # for any mix of rates across a period, e.g. a VAT-holiday boundary).
        if imp.get("cost") is not None:
            ic = float(imp["cost"])
        elif inc_rate is not None:
            ic = kwh * float(inc_rate)
        else:
            ic = xc * (1.0 + vat)
        # Band by the CLEAN inc-rate tariff band, NOT the per-slot derived exc rate
        # (cost_exc ÷ kWh). The derived rate jitters with Octopus's per-slot rounding and
        # shatters the two real bands into many near-duplicates (0.3075/0.3077/0.3078/…);
        # the stored inc rate is a clean tariff value, so grouping on it reproduces the
        # bill's band structure. We group on inc (not the exc rate) because Octopus stores
        # only the exc COST on measurement slots — imp_rate_exc is NULL there — so there's
        # no reliable exc-rate column to group on; inc↔exc are 1:1 (÷ (1+VAT)) so it's
        # equivalent. Fall back to an inc-scale key from the exc figures only when a slot
        # has no inc rate.
        if inc_rate is not None:
            band_key = round(float(inc_rate), 6)
        elif imp.get("rate_exc") is not None:
            band_key = round(float(imp["rate_exc"]) * (1.0 + vat), 6)
        else:
            # No inc rate / stored exc rate — derive an inc-scale key from this slot's
            # ex-VAT cost so it still bands consistently (xc is always set by here).
            band_key = round(xc / float(kwh) * (1.0 + vat), 6)
        band = bands[band_key]
        band["kwh"] += kwh
        band["exc"] += xc
        band["inc"] += ic
    if not bands:
        return None
    rows, energy_raw, energy_inc = [], 0.0, 0.0
    for key in sorted(bands):
        bk, be = bands[key]["kwh"], bands[key]["exc"]
        energy_raw += be
        energy_inc += bands[key]["inc"]
        # Displayed exc rate = the band's clean inc-scale key ÷ (1+VAT) — a deterministic
        # ex-VAT label that matches the bill, NOT a derived effective cost ÷ kWh (which can
        # land a rounding unit off). Cost to 3dp (mills), like the bill — the 4dp rate × kWh
        # only reconciles to the cost at finer precision; the summed raw cost is authoritative.
        rows.append({"rate_exc": round(key / (1.0 + vat), 4),
                     "kwh": round(bk, 3), "cost_exc": round(be, 3)})
    # Collapse a long rate list (Agile: hundreds of distinct half-hourly rates across a
    # bill period) into ONE kWh-weighted average row so the ex-VAT summary stays
    # readable. Uses the raw sums (energy_raw = Σ exc), so Total (exc) is unchanged.
    if len(rows) > _MAX_RATE_ROWS:
        _tot_kwh = sum(bands[k]["kwh"] for k in bands)
        _avg_exc = (energy_raw / _tot_kwh) if _tot_kwh else 0.0
        rows = [{"rate_exc": round(_avg_exc, 4), "kwh": round(_tot_kwh, 3),
                 "cost_exc": round(energy_raw, 3),
                 "collapsed": True, "n_rates": len(bands)}]
    # Standing: prefer the summary's per-LOCAL-day inc figure (correct day count across
    # the BST midnight boundary), ex-VAT via ÷ (1+VAT) — exact-to-the-mill for a flat
    # charge. Fall back to the block-derived exc standing when no summary is supplied.
    def _group_standing(day_amounts, to_exc):
        # {day: amount} → one row per DISTINCT daily rate, so a mid-period standing
        # change (price-cap / tariff switch) shows as separate lines instead of a
        # single averaged rate that matches neither. `to_exc(amount)` converts a
        # daily amount to its ex-VAT value.
        by_rate: dict = {}
        for _d, _amt in day_amounts.items():
            k = round(_amt, 6)
            g = by_rate.setdefault(k, [0, 0.0])
            g[0] += 1
            g[1] += _amt
        return [{"days": g[0], "rate_exc": round(to_exc(k), 4),
                 "cost_exc": round(to_exc(g[1]), 2)}
                for k, g in sorted(by_rate.items())]
    if standing_inc_by_day:
        standing_days = len(standing_inc_by_day)
        standing_inc_raw = sum(standing_inc_by_day.values())
        standing_raw  = standing_inc_raw / (1.0 + vat)
        standing_rows = _group_standing(standing_inc_by_day, lambda a: a / (1.0 + vat))
    else:
        standing_days = len(stand_exc_day)
        standing_raw  = sum(stand_exc_day.values())
        standing_inc_raw = standing_raw * (1.0 + vat)
        standing_rows = _group_standing(stand_exc_day, lambda a: a)   # already ex-VAT
    standing_rate = round(standing_raw / standing_days, 4) if standing_days else 0.0
    subtotal_raw  = energy_raw + standing_raw            # ex-VAT subtotal
    inc_raw       = energy_inc + standing_inc_raw        # inc-VAT subtotal
    subtotal = round(subtotal_raw, 2)
    # VAT amount as inc − exc (a subtraction) — exact for any mix of rates across the
    # period, so a VAT-holiday boundary inside a bill period is handled correctly.
    vat_amount = round(inc_raw - subtotal_raw, 2)
    return {
        "rows": rows,
        "energy_exc":        round(energy_raw, 2),
        "standing_days":     standing_days,
        "standing_rate_exc": standing_rate,
        "standing_rows":     standing_rows,
        "standing_exc":      round(standing_raw, 2),
        "subtotal_exc":      subtotal,
        "vat_rate":          vat,
        "vat_amount":        vat_amount,
        "inc_total":         round(inc_raw, 2),
        "coverage":          round(with_exc / tot_kwh, 3) if tot_kwh else 0.0,
    }


def _has_sub_meters(cfg):
    """True if any configured meter is a sub-meter (EV, battery, heat-pump, …). The
    derived-EV billing breakdown only applies to accounts with NONE, so a real EV meter
    always wins."""
    for md in ((cfg or {}).get("meters") or {}).values():
        if (md.get("meta") or {}).get("sub_meter"):
            return True
    return False


def _ev_meter_id(cfg):
    """The configured physical EV-charger meter_id (meter_type 'ev_charger', or an
    'ev'/'charger' id), or None. Used for the coverage-based synthetic-EV gate."""
    for mid, md in ((cfg or {}).get("meters") or {}).items():
        meta = md.get("meta") or {}
        if (meta.get("meter_type") == "ev_charger"
                or any(kw in str(mid).lower() for kw in ("ev", "charger"))):
            return mid
    return None


def _dispatch_ev_slot_map(store, blocks, cfg, gated=True):
    """{slot_start(UTC ISO): {"kwh","cost","rate"}} — per-slot EV reconstructed from
    COMPLETED dispatches (Octopus's own per-slot EV energy), grid-clipped to that slot's
    main import and cost-apportioned from the slot's import cost. Gated to no-sub-meter
    accounts. Empty when gated off / no store / no dispatch. This is the display-only
    source for the derived-EV billing breakdown; it never affects a bill total."""
    if store is None or not blocks:
        return {}
    # Coverage-based gate: synthesise EV only for slots the PHYSICAL EV device doesn't
    # already have a block for. An active EV meter covers every slot (synthetic stays
    # off, as before); a retired/decommissioned one stops writing blocks, so the
    # synthetic takes over from the cutover automatically. A battery or other sub-meter
    # never blocks the EV synthetic — only the EV device's OWN coverage does.
    _evm = _ev_meter_id(cfg)
    covered = ({b["start"] for b in blocks
                if b and b.get("start") and _evm in (b.get("meters") or {})}
               if (_evm and gated) else set())
    starts = [b["start"] for b in blocks if b and b.get("start")]
    if not starts:
        return {}
    try:
        drows = store._conn.execute(
            "SELECT slot_start, energy_kwh FROM dispatch_history "
            "WHERE kind='completed' AND slot_start >= ? AND slot_start <= ?",
            (min(starts), max(starts))).fetchall()
    except Exception:
        return {}
    ev_raw = {}
    for r in drows:
        e = r["energy_kwh"]
        if e is None:
            continue
        v = abs(float(e))
        if v > 1e-9:
            ev_raw[r["slot_start"]] = ev_raw.get(r["slot_start"], 0.0) + v
    if not ev_raw:
        return {}
    out = {}
    for b in blocks:
        slot = (b or {}).get("start")
        if not slot or slot not in ev_raw or slot in covered:
            continue
        imp = (((b.get("meters") or {}).get("electricity_main") or {})
               .get("channels", {}) or {}).get("import", {}) or {}
        try:
            mk = float(imp.get("kwh_total", imp.get("kwh")) or 0.0)
            mc = float(imp.get("cost") or 0.0)
            rate = round(float(imp.get("rate_used", imp.get("rate")) or 0.0), 4)
        except Exception:
            continue
        if mk <= 0:
            continue
        # BL-9: prefer the STORED, bill-authoritative split (imp_kwh_ev/imp_cost_ev/
        # imp_rate_ev — the 4-rate figures the billing summary's grid-total section
        # uses) so every EV/house surface agrees with the bill on a capped account.
        # Fall back to the dispatch-derived pro-rata carve (EV at the block's own rate)
        # only where a block has no stored split — un-backfilled / older history. On an
        # uncapped account the two are identical (EV and house share the rate), so this
        # is byte-identical there.
        # BL-27: prefer the EV-attributed SEGMENTS (the single source of truth) — imp_cost_ev
        # is just their summed cost, so this is byte-identical to the stored split while
        # letting the reconcile re-stamp of the EV columns retire. Fall back to the stored
        # columns, then the dispatch pro-rata carve, for any block without segments.
        _segs = imp.get("segments")
        _seg_evk = (sum(float(x.get("kwh") or 0.0) for x in _segs
                        if x.get("attribution") == "ev") if _segs else 0.0)
        _sk = imp.get("kwh_ev")
        if _seg_evk > 1e-9:
            _seg_evc = sum(float(x.get("kwh") or 0.0) * float(x.get("inc_rate") or 0.0)
                           for x in _segs if x.get("attribution") == "ev")
            ek = min(_seg_evk, mk)
            ec = _seg_evc * (ek / _seg_evk) if _seg_evk > 0 else 0.0
            rate = round(ec / ek, 4) if ek else rate
        elif _sk is not None and float(_sk) > 1e-9:
            ek = min(float(_sk), mk)
            _sc = float(imp.get("cost_ev") or 0.0)
            ec = _sc * (ek / float(_sk)) if float(_sk) > 0 else 0.0
            _sr = imp.get("rate_ev")
            rate = round(float(_sr), 4) if _sr else (round(ec / ek, 4) if ek else rate)
        else:
            ek = min(ev_raw[slot], mk)
            if ek <= 1e-9:
                continue
            ec = mc * (ek / mk)
        if ek <= 1e-9:
            continue
        out[slot] = {"kwh": ek, "cost": ec, "rate": rate}
    return out


def _hybrid_ev_slot_map(store, blocks, cfg):
    """Per-slot HYBRID EV for the billing breakdown on a has-EV-meter account: SYNTHETIC
    (dispatch / stored imp_kwh_ev / segments — bill-authoritative) wherever it exists,
    else the RECORDED physical EV sub-meter's own metered slot. One continuous EV. The
    synthetic map is taken UNGATED (the coverage gate is what kept it off for an active
    meter); the recorded fallback fills the pre-seam slots. Total-safety is the caller's
    (fold the physical EV line into the remainder before carving this out)."""
    out = dict(_dispatch_ev_slot_map(store, blocks, cfg, gated=False))   # synthetic, all slots
    evm = _ev_meter_id(cfg)
    if evm:
        for b in blocks:
            slot = (b or {}).get("start")
            if not slot or slot in out:                    # synthetic already owns this slot
                continue
            ch = (((b.get("meters") or {}).get(evm) or {}).get("channels", {}) or {}).get("import", {}) or {}
            try:
                k = float(ch.get("kwh_total", ch.get("kwh")) or 0.0)
                c = float(ch.get("cost") or 0.0)
                r = round(float(ch.get("rate_used", ch.get("rate")) or 0.0), 4)
            except Exception:
                continue
            if k > 1e-9:
                out[slot] = {"kwh": k, "cost": c, "rate": r}   # recorded physical (pre-seam)
    return out


def _fold_ev_submeter_into_remainder(summary, ev_devices):
    """Move the physical EV sub-meter line(s) back into the main-import remainder so the
    hybrid EV can be carved from it. TOTAL-SAFE: energy only moves between the EV line and
    the remainder, so Total Import / total_cost are untouched. Matches sub-meter lines by
    device name (or an ev/charger keyword). Returns True if anything folded."""
    totals = summary.get("totals") or {}
    meters = summary.get("meters") or {}
    meta   = summary.get("meter_meta") or {}
    rem_key = next((k for k in totals
                    if k.endswith("/ Import") and not totals[k].get("is_submeter")), None)
    if not rem_key:
        return False
    rem = meters.setdefault(rem_key, {})
    def _is_ev_line(k):
        d = str((meta.get(k, {}) or {}).get("device") or "")
        return (d in ev_devices) or ("charger" in d.lower()) or ("charger" in k.lower())
    folded = False
    for key in [k for k in list(totals)
                if totals[k].get("is_submeter") and k.endswith("/ Import") and _is_ev_line(k)]:
        for rate, v in (meters.get(key) or {}).items():
            _r = rem.setdefault(rate, {"kwh": 0.0, "cost": 0.0, "read_start": None, "read_end": None})
            _r["kwh"]  = round(_r.get("kwh", 0.0)  + (v.get("kwh")  or 0.0), 3)
            _r["cost"] = round(_r.get("cost", 0.0) + (v.get("cost") or 0.0), 2)
        rt, rtr = totals[key], totals[rem_key]
        rtr["kwh"]  = round(rtr["kwh"]  + rt["kwh"],  3)
        rtr["cost"] = round(rtr["cost"] + rt["cost"], 2)
        meters.pop(key, None); totals.pop(key, None); meta.pop(key, None)
        folded = True
    return folded


def _inject_ev_breakdown_into_summary(summary, period_blocks, ev_slot_map,
                                      label="EV (from dispatch)", fold_devices=None):
    """Split the 'Direct import' remainder in a billing summary into a reduced house
    slice + a synthetic '<label> / Import' sub-meter, from ev_slot_map. DISPLAY-ONLY:
    main_import_raw ('Import — total grid'), standing, and total_cost are untouched, so
    the bill stays byte-identical — only the 'Breakdown by meter' section subdivides.
    Whatever EV is added is subtracted from the remainder, so the two always sum back to
    the original Direct import. No-op if a real sub-meter already exists, there's no
    remainder key, or there's no EV. Mutates summary in place; returns True if applied."""
    if not summary or not ev_slot_map:
        return False
    totals = summary.get("totals") or {}
    if fold_devices:                              # hybrid: fold the physical EV line into remainder,
        _fold_ev_submeter_into_remainder(summary, fold_devices)   # then carve the hybrid EV from it
    elif any(t.get("is_submeter") for t in totals.values()):
        return False                              # no-meter path: unchanged (byte-identical)
    remainder_key = next((k for k in totals
                          if k.endswith("/ Import") and not totals[k].get("is_submeter")), None)
    if not remainder_key:
        return False
    ev_by_rate = defaultdict(lambda: {"kwh": 0.0, "cost": 0.0})
    for b in period_blocks:
        slot = (b or {}).get("start")
        ev = ev_slot_map.get(slot) if slot else None
        if not ev:
            continue
        ev_by_rate[ev["rate"]]["kwh"]  += ev["kwh"]
        ev_by_rate[ev["rate"]]["cost"] += ev["cost"]
    if not ev_by_rate:
        return False
    rem_channels = summary["meters"][remainder_key]
    ev_channels = {}
    ev_tot_k = ev_tot_c = 0.0
    for rate, v in ev_by_rate.items():
        ek = round(v["kwh"], 3)
        ec = round(v["cost"], 2)
        if ek <= 0:
            continue
        ev_channels[rate] = {"kwh": ek, "cost": ec, "read_start": None, "read_end": None}
        ev_tot_k += ek
        ev_tot_c += ec
        if rate in rem_channels:
            _rk = rem_channels[rate]["kwh"] - ek
            _rc = rem_channels[rate]["cost"] - ec
            rem_channels[rate]["kwh"]  = round(_rk if rem_channels[rate]["kwh"] < 0 else max(0.0, _rk), 3)
            rem_channels[rate]["cost"] = round(_rc if rem_channels[rate]["cost"] < 0 else max(0.0, _rc), 2)
    if not ev_channels:
        return False
    ev_key = f"{label} / Import"
    summary["meters"][ev_key] = ev_channels
    summary["totals"][ev_key] = {"kwh": round(ev_tot_k, 3), "cost": round(ev_tot_c, 2),
                                 "is_submeter": True, "read_start": None, "read_end": None}
    rt = summary["totals"][remainder_key]
    _rtk = rt["kwh"] - ev_tot_k
    _rtc = rt["cost"] - ev_tot_c
    rt["kwh"]  = round(_rtk if rt["kwh"] < 0 else max(0.0, _rtk), 3)
    rt["cost"] = round(_rtc if rt["cost"] < 0 else max(0.0, _rtc), 2)
    summary.setdefault("meter_meta", {})[ev_key] = {
        "site": None, "device": label, "mpan": None, "tariff": None, "is_submeter": True}
    return True


def render_billing_summary(summary, currency='£', site_name=None):
    if not summary:
        return ""

    meter_meta       = summary.get("meter_meta", {})
    main_import_raw  = summary.get("main_import_raw", {})
    main_reads       = summary.get("main_import_reads", {})

    # ── Site header ──
    if not site_name:
        site_name = next(
            (m.get("site") for m in meter_meta.values() if m.get("site") and not m.get("is_submeter")),
            None
        )
    site_header = f'''
        <tr class="bill-site-header">
          <td colspan="4"><span class="censored">{site_name}</span></td>
        </tr>''' if site_name else ""

    html = f'''
    <div class="billing-summary">
      <table class="billing-table">
        {site_header}
    '''

    # ── Separate export, main import remainder, and sub-meter imports ──
    export_keys    = sorted(k for k in summary["meters"] if k.endswith("/ Export"))
    remainder_keys = sorted(k for k in summary["meters"]
                            if k.endswith("/ Import")
                            and not (summary["totals"].get(k) or {}).get("is_submeter"))
    submeter_keys  = sorted(k for k in summary["meters"]
                            if k.endswith("/ Import")
                            and (summary["totals"].get(k) or {}).get("is_submeter"))

    # ── Export section(s) ──
    for meter_name in export_keys:
        channels   = summary["meters"][meter_name]
        totals     = summary["totals"].get(meter_name, {})
        meta       = meter_meta.get(meter_name, {})
        read_start = totals.get("read_start")
        read_end   = totals.get("read_end")
        mpan_html  = (f'&nbsp;&nbsp;|&nbsp;&nbsp;MPAN: <span class="censored">{meta["mpan"]}</span>'
                      if meta.get("mpan") else "")
        reads_html = ""
        if read_start is not None:
            read_total = (read_end or 0.0) - (read_start or 0.0)
            reads_html = (f'<br><span class="reads">'
                          f'Start: <span class="censored">{read_start:.3f}</span>'
                          f'&nbsp;&nbsp;End: <span class="censored">{read_end:.3f}</span>'
                          f'&nbsp;&nbsp;Total: {read_total:.3f} kWh</span>')
        html += f"""
        <tr class="channel-title">
          <td colspan="4">Export{mpan_html}{reads_html}</td>
        </tr>
        <tr class="channel-header">
          <td></td><td>Rate ({currency}/kWh)</td><td>kWh</td><td>Cost ({currency})</td>
        </tr>"""
        html += _bill_rate_rows(channels, currency)[0]
        html += _bill_total_row(totals["kwh"], totals["cost"])
    # BL-24: when "Bill Rounding" is on, the import charge is rendered EX-VAT (the bill
    # method) IN PLACE OF the inc-VAT display; otherwise the usual inc-VAT rows.
    _bm = summary.get("bill_method")
    _exc_lbl = ", exc" if _bm else ""
    if remainder_keys or submeter_keys:
        # Build raw totals from main_import_raw
        raw_kwh  = sum(d["kwh"]  for d in main_import_raw.values())
        raw_cost = sum(d["cost"] for d in main_import_raw.values())

        # Reads from main meter
        read_start = main_reads.get("read_start")
        read_end   = main_reads.get("read_end")
        reads_html = ""
        if read_start is not None:
            read_total = (read_end or 0.0) - (read_start or 0.0)
            reads_html = (f'<br><span class="reads">'
                          f'Start: <span class="censored">{read_start:.3f}</span>'
                          f'&nbsp;&nbsp;End: <span class="censored">{read_end:.3f}</span>'
                          f'&nbsp;&nbsp;Total: {read_total:.3f} kWh</span>')

        # MPAN from first remainder key meta
        mpan_html = ""
        if remainder_keys:
            _meta = meter_meta.get(remainder_keys[0], {})
            if _meta.get("mpan"):
                mpan_html = f'&nbsp;&nbsp;|&nbsp;&nbsp;MPAN: <span class="censored">{_meta["mpan"]}</span>'

        html += f"""
        <tr class="channel-title">
          <td colspan="4">Import — total grid{mpan_html}{reads_html}</td>
        </tr>
        <tr class="channel-header">
          <td></td><td>Rate ({currency}/kWh{_exc_lbl})</td><td>kWh</td><td>Cost ({currency}{_exc_lbl})</td>
        </tr>"""
        if _bm:
            # BL-24: the ex-VAT bill method REPLACES the inc-VAT import display when Bill
            # Rounding is on — per-rate ex-VAT (3dp, as the bill), Total (exc), the ex-VAT
            # standing charge, a VAT row, then the inc-VAT total. Octopus builds a bill this
            # way: sum the raw ex-VAT half-hours, round at the subtotal, then add VAT. The
            # Total Bill below is UNCHANGED — this only changes how the import is presented.
            # BL-9: on an IOG tariff show the EV/Home split (ex-VAT, derived from each
            # block's own exc/inc ratio) in place of the plain per-rate rows; the
            # Total (exc) below is the authoritative bill-method figure, unchanged.
            _split_html = _bill_split_rows(summary, currency, exc=True)
            if _split_html is not None:
                html += _split_html
            else:
                for _r in _bm["rows"]:
                    _rate_cell = (f"{_r['rate_exc']:.4f} <span style=\"opacity:0.6;\">(avg of {_r['n_rates']} rates)</span>"
                                  if _r.get("collapsed") else f"{_r['rate_exc']:.4f}")
                    html += f"""
        <tr><td></td><td>{_rate_cell}</td><td>{_r['kwh']:.3f}</td><td>{_r['cost_exc']:.3f}</td></tr>"""
            html += f"""
        <tr class="channel-total"><td>Total (exc)</td><td></td><td></td><td>{_bm['energy_exc']:.2f}</td></tr>"""
            if _bm["standing_days"]:
                _srows = _bm.get("standing_rows") or []
                if len(_srows) > 1:
                    # Standing-charge rate changed mid-period (price cap / tariff
                    # switch) — one line per rate, not a single averaged rate.
                    for _s in _srows:
                        html += f"""
        <tr class="standing"><td colspan="3">Standing charge (exc): {_s['days']} days @ {currency}{_s['rate_exc']:.4f}/day</td><td>{_s['cost_exc']:.2f}</td></tr>"""
                else:
                    html += f"""
        <tr class="standing"><td colspan="3">Standing charge (exc): {_bm['standing_days']} days @ {currency}{_bm['standing_rate_exc']:.4f}/day</td><td>{_bm['standing_exc']:.2f}</td></tr>"""
            html += f"""
        <tr class="bill-method"><td colspan="3">VAT @ {_bm['vat_rate'] * 100:.0f}%</td><td>{currency}{_bm['vat_amount']:.2f}</td></tr>
        <tr class="channel-total"><td colspan="3">Total incl. VAT</td><td>{currency}{_bm['inc_total']:.2f}</td></tr>"""
        else:
            _rate_html, raw_kwh_r, raw_cost_r = _bill_rate_rows(main_import_raw, currency)
            # BL-9: on an IOG tariff show the EV/Home split (as Octopus does) in place
            # of the plain per-rate rows; the Total is unchanged (the split sums to it).
            _split_html = _bill_split_rows(summary, currency)
            html += _split_html if _split_html is not None else _rate_html
            html += _bill_total_row(raw_kwh_r, raw_cost_r)

            # ── Standing charge folded into the import charge (display only) —
            # mirrors a real bill, where energy + standing make up the import total.
            # The kWh Total above is unchanged, and summary['total_cost'] (the Total
            # Bill) plus Usage Stats are computed elsewhere, so nothing here moves a
            # number — this only adds a cost-inclusive subtotal to the display.
            if summary["standing"]:
                _sc_groups = {}
                for _d, _amt in sorted(summary["standing"].items()):
                    _r = round(_amt, 4)
                    _sc_groups[_r] = _sc_groups.get(_r, 0) + 1
                for _r, _cnt in sorted(_sc_groups.items()):
                    html += f"""
        <tr class="standing">
          <td colspan="3">Standing charge: {_cnt} days @ {currency}{_r:.4f}/day</td>
          <td>{_r * _cnt:.2f}</td>
        </tr>"""
                _incl = raw_cost_r + round(sum(summary["standing"].values()), 2)
                _incl_str = f"({-_incl:.2f})" if _incl < 0 else f"{_incl:.2f}"
                html += f"""
        <tr class="channel-total">
          <td colspan="3">Total incl. standing charge</td><td>{_incl_str}</td>
        </tr>"""

        # ── Sub-meter breakdown (indented) ──
        if submeter_keys or remainder_keys:
            html += '''
        <tr class="submeter-breakdown-header">
          <td colspan="4">Breakdown by meter</td>
        </tr>'''

            # House remainder first
            for meter_name in remainder_keys:
                channels = summary["meters"][meter_name]
                totals   = summary["totals"].get(meter_name, {})
                html += f"""
        <tr class="channel-title submeter-indent">
          <td colspan="4">Direct import</td>
        </tr>
        <tr class="channel-header submeter-indent">
          <td></td><td>Rate ({currency}/kWh)</td><td>kWh</td><td>Cost ({currency})</td>
        </tr>"""
                _rate_html, tot_kwh_r, tot_cost_r = _bill_rate_rows(channels, currency)
                html += _rate_html
                html += _bill_total_row(tot_kwh_r, tot_cost_r)
            for meter_name in submeter_keys:
                channels = summary["meters"][meter_name]
                totals   = summary["totals"].get(meter_name, {})
                meta     = meter_meta.get(meter_name, {})
                label    = meta.get("device") or meter_name.split(" / ")[0].replace("(Sub-meter)", "").replace("(sub-meter)", "").strip()
                html += f"""
        <tr class="channel-title submeter-indent">
          <td colspan="4">{label}</td>
        </tr>
        <tr class="channel-header submeter-indent">
          <td></td><td>Rate ({currency}/kWh)</td><td>kWh</td><td>Cost ({currency})</td>
        </tr>"""
                _rate_html, tot_kwh_r, tot_cost_r = _bill_rate_rows(channels, currency)
                html += _rate_html
                html += _bill_total_row(tot_kwh_r, tot_cost_r)
    # Standing charge for export-only accounts (no import section above, so it
    # would otherwise not appear); the import case renders it in-section.
    if summary["standing"] and not (remainder_keys or submeter_keys):
        rate_groups = {}
        for day_date, amount in sorted(summary["standing"].items()):
            rate = round(amount, 4)
            rate_groups[rate] = rate_groups.get(rate, 0) + 1
        for rate, count in sorted(rate_groups.items()):
            html += f"""
        <tr class="standing">
          <td colspan="3">Standing Charge: {count} days @ {currency}{rate:.4f}/day</td>
          <td>{rate * count:.2f}</td>
        </tr>"""

    html += f"""
        <tr class="grand-total">
          <td colspan="3">Total Bill</td>
          <td>{currency}{summary['total_cost']:.2f}</td>
        </tr>"""

    html += """
      </table>
    </div>"""

    return html


# ─────────────────────────────────────────────────────────────
# Daily chart builder (returns HTML string for one day)
# ─────────────────────────────────────────────────────────────

def _day_segment_split(main_import, meters):
    """BL-27: on a CAPPED block (EV pushed to PEAK, or straddling the cap boundary) return
    the per-meter slot values driven by the block's dispatch SEGMENTS — the physical EV
    device on the EV bands (grid-clipped dispatch: off-peak within cap, peak beyond), house
    sub-devices at the house band rate, and 'Direct import' the house-segment remainder. So
    the house stays cleanly off-peak and the EV carries the peak, with no metered-vs-dispatch
    residual. Returns {meter_id: {"kwh","cost","rate"}} (incl. 'electricity_main' for the
    house remainder), or None — uncapped blocks (EV off-peak == house) and sensor-less
    accounts keep the column / synthetic paths, so they're byte-identical. Requires a
    physical EV sub-meter to attribute the EV bands onto."""
    segs = main_import.get("segments")
    if not segs:
        return None
    if not any(s.get("attribution") == "ev" and s.get("band") in ("peak", "mixed")
               for s in segs):
        return None
    _v = lambda x: float(x or 0.0)
    ev_k = sum(_v(s.get("kwh")) for s in segs if s.get("attribution") == "ev")
    ev_c = sum(_v(s.get("kwh")) * _v(s.get("inc_rate")) for s in segs if s.get("attribution") == "ev")
    ho_k = sum(_v(s.get("kwh")) for s in segs if s.get("attribution") != "ev")
    ho_c = sum(_v(s.get("kwh")) * _v(s.get("inc_rate")) for s in segs if s.get("attribution") != "ev")
    house_rate = (ho_c / ho_k) if ho_k > 1e-9 else 0.0
    ev_dev = None
    house_devs = []
    for _mn, _m in (meters or {}).items():
        if not (((_m or {}).get("meta", {}) or {}).get("sub_meter")):
            continue
        _mt = (((_m.get("meta") or {}).get("meter_type")) or "").lower()
        if _mt in ("ev", "ev_charger") or any(k in _mn.lower() for k in ("ev", "charger")):
            ev_dev = _mn
        else:
            house_devs.append((_mn, _m))
    if ev_dev is None or ev_k <= 1e-9:
        return None                      # no physical EV to carry the bands → leave as-is
    # The EV BAR is the physical charger's METERED grid kWh (the bar the user recognises);
    # its COST/RATE come from the dispatch split (ev_c / dispatch rate). The house remainder
    # absorbs the metered-vs-dispatch kWh difference, so Σ device kWh == grid import and the
    # remainder can never go negative from dispatch over-attributing the grid (design:
    # device-pricing "physical-EV-device question").
    _evsub = ((meters.get(ev_dev, {}).get("channels", {}) or {}).get("import", {}) or {})
    dev_ev = _v(_evsub.get("kwh_grid", _evsub.get("kwh"))) or ev_k   # metered; else dispatch
    grid_k = ev_k + ho_k                                             # block grid import (Σ seg)
    out = {ev_dev: {"kwh": round(dev_ev, 6), "cost": round(ev_c, 6),
                    "rate": round(ev_c / ev_k, 6)}}
    hk_used = hc_used = 0.0
    for _mn, _m in house_devs:
        _sub = ((_m.get("channels", {}) or {}).get("import", {}) or {})
        _gk = _v(_sub.get("kwh_grid", _sub.get("kwh")))
        _c = round(_gk * house_rate, 6)
        out[_mn] = {"kwh": round(_gk, 6), "cost": _c, "rate": round(house_rate, 6)}
        hk_used += _gk
        hc_used += _c
    out["electricity_main"] = {"kwh": round(grid_k - dev_ev - hk_used, 6),
                               "cost": round(ho_c - hc_used, 6),
                               "rate": round(house_rate, 6)}
    return out


def build_day_chart_html(day, day_blocks, meter_colors, chart_prefix='', block_minutes=30, currency='£', site_name=None, ev_slot_map=None, bill_rounding=False, fallback_vat=0.05, ev_relabel_meter=None, ev_label=None):
    slots = 1440 // block_minutes
    meter_kwh    = defaultdict(lambda: [0.0] * slots)
    meter_rate   = defaultdict(lambda: [0.0] * slots)
    meter_cost   = defaultdict(lambda: [0.0] * slots)
    slot_ti_kwh  = [0.0] * slots
    slot_ti_cost = [0.0] * slots
    slot_ti_rate = [0.0] * slots
    # BL-24 data table (opt-in): a per-slot ex-VAT/inc ratio so every IMPORT column
    # can show Cost (exc) alongside Cost (inc). Sourced from the block's stored ex-VAT
    # rate (authoritative for the main import after the 4.2 backfill); a slot with no
    # captured exc falls back to inc ÷ (1+VAT) and is flagged approximate. Only emitted
    # when the setting is on, so the default data table JSON is byte-identical.
    slot_exc_ratio = [None] * slots
    slot_exc_approx = [False] * slots
    summary_kwh  = defaultdict(float)
    summary_cost = defaultdict(float)
    summary_rates = defaultdict(lambda: defaultdict(float))
    meter_display_name = {}

    def _f(v, default=0.0):
        try:
            return float(v) if v is not None else default
        except (TypeError, ValueError):
            return default

    for hh, block in day_blocks:
        try:
            meters = block.get("meters", {}) or {}
            main   = meters.get("electricity_main", {}) or {}
            main_import = (main.get("channels", {}) or {}).get("import", {}) or {}
            main_export = (main.get("channels", {}) or {}).get("export", {}) or {}

            # Use kwh_remainder when available (engine PASS 2 remainder — same field
            # usage stats uses). With no sub-meters this key is absent (NULL in DB,
            # omitted by both block reconstruction paths), so this falls through to
            # the main meter's kwh — api+mini blocks are structurally identical to
            # CAD here, so no mode-specific handling is needed.
            if "kwh_remainder" in main_import:
                main_kwh  = _f(main_import["kwh_remainder"])
                main_cost = _f(main_import.get("cost"))
                _raw_main_cost = main_cost   # pre-subtraction, to keep an Agile credit's sign
                # Cost: subtract sub-meter costs (no cost_remainder stored per-block)
                for meter_name, meter in meters.items():
                    if (meter or {}).get("meta", {}).get("sub_meter"):
                        sub = ((meter.get("channels", {}) or {}).get("import", {}) or {})
                        main_cost -= _f(sub.get("cost"))
                # Floor only the sub-subtraction artifact (raw >= 0); a genuinely negative
                # Agile plunge-price cost (a credit) must survive, not clamp to 0.
                main_cost = main_cost if _raw_main_cost < 0 else max(main_cost, 0.0)
            else:
                main_kwh  = _f(main_import.get("kwh_total", main_import.get("kwh")))
                main_cost = _f(main_import.get("cost"))
                _raw_main_cost = main_cost   # pre-subtraction, to keep an Agile credit's sign
                for meter_name, meter in meters.items():
                    if (meter or {}).get("meta", {}).get("sub_meter"):
                        sub = ((meter.get("channels", {}) or {}).get("import", {}) or {})
                        main_kwh  -= _f(sub.get("kwh_grid", sub.get("kwh")))
                        main_cost -= _f(sub.get("cost"))
                main_kwh  = max(main_kwh, 0.0)
                # Floor only the sub-subtraction artifact (raw >= 0); a genuine Agile
                # plunge-price credit (negative cost) must survive, not clamp to 0.
                main_cost = main_cost if _raw_main_cost < 0 else max(main_cost, 0.0)
            main_rate = _f(main_import.get("rate_used", main_import.get("rate")))
            _seg_split = _day_segment_split(main_import, meters)  # BL-27: applied after exc

            # Per-slot ex-VAT ratio for the opt-in data-table Cost (exc) column.
            if bill_rounding:
                _exc_r = main_import.get("rate_exc")
                _ratio = None
                if _exc_r is not None and main_rate:
                    _ratio = _f(_exc_r) / main_rate
                else:
                    _ce, _ci = main_import.get("cost_exc"), main_import.get("cost")
                    if _ce is not None and _ci not in (None, 0):
                        _ratio = _f(_ce) / _f(_ci)
                if _ratio is None:
                    # No captured exc → fall back to the period's statutory VAT rate
                    # (from the calendar), NOT a hardcoded 1.05, so a 0%/20% period is right.
                    slot_exc_ratio[hh]  = round(1.0 / (1.0 + fallback_vat), 8)
                    slot_exc_approx[hh] = True
                else:
                    slot_exc_ratio[hh]  = round(_ratio, 8)

            if _seg_split is not None:                      # BL-27: house from house segments
                _hh = _seg_split["electricity_main"]
                main_kwh, main_cost = _hh["kwh"], _hh["cost"]
                main_rate = _hh["rate"] or main_rate

            meter_kwh["electricity_main"][hh]  = main_kwh
            meter_rate["electricity_main"][hh] = main_rate
            meter_cost["electricity_main"][hh] = main_cost
            summary_kwh["electricity_main"]   += main_kwh
            summary_cost["electricity_main"]  += main_cost
            summary_rates["electricity_main"][round(main_rate, 4)] += main_kwh
            if "electricity_main" not in meter_display_name:
                meter_display_name["electricity_main"] = "Direct import"

            # Total import: use kwh_total from engine if available (authoritative),
            # otherwise sum remainder + sub-meter grid portions only.
            ti_kwh_has_total = "kwh_total" in main_import
            ti_kwh  = _f(main_import.get("kwh_total")) if ti_kwh_has_total else main_kwh
            ti_cost = main_cost  # remainder cost; sub-meter costs added below

            for meter_name, meter in meters.items():
                if (meter or {}).get("meta", {}).get("sub_meter"):
                    sub      = ((meter.get("channels", {}) or {}).get("import", {}) or {})
                    sub_kwh      = _f(sub.get("kwh"))
                    sub_kwh_grid = _f(sub.get("kwh_grid", sub_kwh))  # grid-attributed portion
                    sub_cost = _f(sub.get("cost"))
                    sub_rate = _f(sub.get("rate"))
                    if _seg_split is not None and meter_name in _seg_split:
                        _sv = _seg_split[meter_name]     # BL-27: EV on dispatch bands / house dev on house band
                        sub_kwh_grid = _sv["kwh"]
                        sub_cost = _sv["cost"]
                        sub_rate = _sv["rate"]
                    meter_kwh[meter_name][hh]  = sub_kwh_grid  # grid-attributed, matches usage stats
                    meter_rate[meter_name][hh] = sub_rate
                    meter_cost[meter_name][hh] = sub_cost
                    summary_kwh[meter_name]   += sub_kwh_grid
                    summary_cost[meter_name]  += sub_cost
                    summary_rates[meter_name][round(sub_rate, 4)] += sub_kwh_grid
                    if meter_name not in meter_display_name:
                        meta = (meter or {}).get("meta", {}) or {}
                        label = meta.get("device") or meter_name.replace("_", " ").title()
                        meter_display_name[meter_name] = label
                    if not ti_kwh_has_total:
                        ti_kwh += sub_kwh_grid
                    ti_cost += sub_cost

            slot_ti_kwh[hh]  = ti_kwh
            slot_ti_cost[hh] = ti_cost
            slot_ti_rate[hh] = main_rate

            # ── Derived-EV split (display-only; see _dispatch_ev_slot_map) ──────
            # Re-partition the house slot into house + EV AFTER the total-import (ti)
            # figures are set, so the grid total is untouched — this only subdivides
            # the 'Direct import' segment. No-op unless an ev_slot_map is supplied
            # (gated to no-sub-meter accounts upstream).
            if ev_slot_map:
                _ev = ev_slot_map.get(block.get("start"))
                _mk = meter_kwh["electricity_main"][hh]
                if _ev and _mk > 0:
                    _ek = min(_ev["kwh"], _mk)
                    if _ek > 1e-9:
                        _mc = meter_cost["electricity_main"][hh]
                        # BL-9: EV cost from the (bill-authoritative) ev_slot_map cost,
                        # clipped consistently with _ek, so the EV bar, rate line and cost
                        # all match the bill on a capped account; house = main − EV keeps
                        # the grid total untouched. Uncapped → ev_slot_map cost == the
                        # pro-rata carve, so byte-identical. Fall back to pro-rata if the
                        # map somehow lacks kWh.
                        _evk_map = _f(_ev.get("kwh"))
                        _ec = (_f(_ev.get("cost")) * (_ek / _evk_map)
                               if _evk_map > 1e-9 else _mc * (_ek / _mk))
                        _r4 = round(main_rate, 4)
                        # The EV rate LINE follows the stored dispatch EV rate
                        # (imp_rate_ev) so it DIVERGES from the house line once the 6-h
                        # cap pushes EV charging to peak (the 4-rate rule). On an uncapped
                        # IOG account imp_rate_ev == the house rate, so the line sits
                        # exactly on the house line (no visible change). Energy/cost stay
                        # on the validated synthetic split (grid total untouched); only
                        # the rate the EV line is plotted at, and its summary rate bucket,
                        # change. Absent (non-IOG / no split) → falls back to main_rate.
                        # BL-27: the EV rate LINE follows the EV-attributed SEGMENT rate
                        # (full precision, == the old imp_rate_ev); the column is a dead
                        # fallback for any un-backfilled block, so the re-stamp can retire.
                        _segsr = main_import.get("segments")
                        _segr_k = (sum(_f(x.get("kwh")) for x in _segsr
                                       if x.get("attribution") == "ev") if _segsr else 0.0)
                        if _segr_k > 1e-9:
                            _evr = (sum(_f(x.get("kwh")) * _f(x.get("inc_rate")) for x in _segsr
                                        if x.get("attribution") == "ev") / _segr_k) or main_rate
                        else:
                            _evr = _f(main_import.get("rate_ev")) or main_rate
                        _r4e = round(_evr, 4)
                        meter_kwh["electricity_main"][hh]  = _mk - _ek
                        meter_cost["electricity_main"][hh] = _mc - _ec
                        meter_kwh["ev_dispatch"][hh]  = _ek
                        meter_cost["ev_dispatch"][hh] = _ec
                        meter_rate["ev_dispatch"][hh] = _evr
                        summary_kwh["electricity_main"]  -= _ek
                        summary_cost["electricity_main"] -= _ec
                        summary_kwh["ev_dispatch"]  += _ek
                        summary_cost["ev_dispatch"] += _ec
                        summary_rates["electricity_main"][_r4] -= _ek
                        summary_rates["ev_dispatch"][_r4e]     += _ek
                        meter_display_name.setdefault("ev_dispatch", "EV (from dispatch)")

            if main_export:
                exp_kwh  = abs(_f(main_export.get("kwh")))
                exp_cost = abs(_f(main_export.get("cost")))
                exp_rate = abs(_f(main_export.get("rate")))
                exp_name = "electricity_main_export"
                meter_kwh[exp_name][hh]  = -exp_kwh
                meter_rate[exp_name][hh] = exp_rate
                meter_cost[exp_name][hh] = exp_cost
                summary_kwh[exp_name]   += exp_kwh
                summary_cost[exp_name]  += exp_cost
                summary_rates[exp_name][round(exp_rate, 4)] += exp_kwh

        except Exception:
            pass

    # ── BL-27: the rate LINES come from the day-level emit (chart_emit.day_rate_series) —
    # ONE tested place for "what rate applies each half-hour"; the chart only plots it. See
    # the presentation read-contract in docs/design/4.4.0_iog_pricing_and_reprice_design.md.
    import chart_emit
    _series = chart_emit.day_rate_series(day_blocks, slots=slots, block_minutes=block_minutes)
    for _mid in list(meter_rate.keys()):
        if _mid.endswith("_export"):
            continue
        _is_ev = (_mid == "ev_dispatch" or "charger" in _mid.lower()
                  or _mid.lower().startswith("ev_") or _mid.lower().startswith("ev "))
        _curve = _series["ev"] if _is_ev else _series["house"]
        for hh in range(slots):
            if _curve[hh] is not None:
                meter_rate[_mid][hh] = _curve[hh]

    # ── x axis labels — outside the loop ──
    total_hh_kwh = [sum(meter_kwh[m][i] for m in meter_kwh if not m.endswith('_export')) for i in range(slots)]
    x_labels = []
    x_ranges = []
    for i in range(slots):
        minutes_start = i * block_minutes
        h_s, m_s = divmod(minutes_start, 60)
        minutes_end = minutes_start + block_minutes
        h_e, m_e = divmod(minutes_end % 1440, 60)
        x_labels.append(f"{h_s:02d}:{m_s:02d}")
        x_ranges.append(f"{h_s:02d}:{m_s:02d} - {h_e:02d}:{m_e:02d}")

    # ── Summary panel ──
    # H6b: on a has-EV-meter account, present the physical EV sub-meter as one continuous
    # 'EV' identity (matching Usage Stats / the period bill). Display-only relabel — the
    # metered kWh/cost are untouched, so the day total and the bill are byte-identical.
    if ev_relabel_meter and ev_label and ev_relabel_meter in meter_display_name:
        meter_display_name[ev_relabel_meter] = ev_label
    sub_meter_names = [k for k in summary_kwh if k not in ("electricity_main", "electricity_main_export")]

    total_import      = sum(v for k, v in summary_kwh.items()  if not k.endswith("_export") and v > 0)
    total_import_cost = sum(v for k, v in summary_cost.items() if not k.endswith("_export") and summary_kwh.get(k, 0) > 0)

    def rate_breakdown_html(meter_key, css_extra=""):
        out = ""
        for kwh, rate, n in _collapse_rate_kwh(summary_rates[meter_key]):
            suffix = f' (avg of {n})' if n else ''
            out += (f'<span class="rate-row {css_extra}">'
                    f'{kwh:.3f} kWh @ {currency}{rate:.4f}{suffix}</span>')
        return out

    house_kwh  = summary_kwh.get("electricity_main", 0.0)
    exp_kwh    = summary_kwh.get("electricity_main_export", 0.0)
    exp_cost   = summary_cost.get("electricity_main_export", 0.0)

    main_color   = meter_colors.get("electricity_main", "#1f77b4")
    export_color = meter_colors.get("electricity_main_export", "#ff7f0e")

    def cs(text, color, size="0.9em", bold=False):
        w = "font-weight:600;" if bold else ""
        return f'<span style="color:{color};font-size:{size};line-height:1.6;white-space:nowrap;{w}">{text}</span>'

    def rate_rows_colored(meter_key, color):
        out = ""
        for kwh, rate, n in _collapse_rate_kwh(summary_rates[meter_key]):
            suffix = f' (avg of {n})' if n else ''
            out += cs(f'{kwh:.3f} kWh @ {currency}{rate:.4f}{suffix}', color, size="0.8em")
        return out

    # ── Per-meter totals rounded to display precision ──────────────────────
    # Sum per-slot values at display precision (3dp kWh, 4dp cost) so the
    # sidebar, data table totals row, and any future views all use the same
    # single rounding path.
    meter_totals = {}
    for meter in meter_kwh:
        kwh_sum  = sum(abs(v) for v in meter_kwh[meter])
        # Import cost is summed SIGNED so an Agile plunge-price credit (negative
        # cost) reduces the total instead of being flipped positive; export is kept
        # as a magnitude (its sign is applied at display). No-op on positive days.
        _is_exp  = str(meter).lower().endswith("export")
        cost_sum = (sum(abs(v) for v in meter_cost[meter]) if _is_exp
                    else sum(meter_cost[meter]))
        meter_totals[meter] = {
            "kwh":  round(kwh_sum, 3),
            "cost": round(cost_sum, 4),
        }

    house_cost = meter_totals.get("electricity_main", {}).get("cost", 0.0)
    ti_total = {
        "kwh":  round(sum(meter_totals[m]["kwh"]  for m in meter_totals if not m.endswith("_export")), 3),
        "cost": round(sum(meter_totals[m]["cost"] for m in meter_totals if not m.endswith("_export")), 4),
    }
    # Use rounded totals for the import summary line too
    total_import_cost_display = ti_total["cost"]
    # BL-24 (opt-in): the ex-VAT import total for the side panel, from the same per-slot
    # exc ratio the data table uses (so the two agree). Only when the setting is on.
    _import_exc_display = None
    if bill_rounding:
        _inv = 1.0 / (1.0 + fallback_vat)
        _import_exc_display = round(sum(
            slot_ti_cost[_h] * (slot_exc_ratio[_h] if slot_exc_ratio[_h] is not None else _inv)
            for _h in range(slots)), 2)

    totals_html = ''
    if total_import > 0:
        totals_html += cs(f'Total import: {total_import:.3f} kWh', main_color)
        totals_html += cs(f'Import cost: {currency}{total_import_cost_display:.2f}', main_color)
        if _import_exc_display is not None:
            totals_html += cs(f'Import cost (exc VAT): {currency}{_import_exc_display:.2f}',
                              main_color)
    if exp_kwh > 0:
        totals_html += cs(f'Total export: {exp_kwh:.3f} kWh', export_color)
        totals_html += cs(f'Export credit: {currency}{exp_cost:.2f}', export_color)
        totals_html += rate_rows_colored("electricity_main_export", export_color)

    breakdown_cols = []

    if sub_meter_names:
        if house_kwh > 0.0001:
            label = meter_display_name.get("electricity_main", "Direct import")
            col   = '<div class="scol">'
            col  += cs(f'↳ {label}', main_color, size="1em", bold=True)
            col  += cs(f'{house_kwh:.3f} kWh', main_color, size="1em")
            col  += cs(f'{currency}{house_cost:.2f}', main_color, size="1em")
            col  += rate_rows_colored("electricity_main", adjust_color(main_color, 0.75))
            col  += '</div>'
            breakdown_cols.append(col)

        for meter_name in sub_meter_names:
            sub_kwh  = summary_kwh.get(meter_name, 0.0)
            sub_cost = meter_totals.get(meter_name, {}).get("cost", 0.0)
            if sub_kwh > 0.0001:
                sub_color = meter_colors.get(meter_name, "#e377c2")
                label     = meter_display_name.get(meter_name, meter_name.replace("_", " ").title())
                col  = '<div class="scol">'
                col += cs(f'↳ {label}', sub_color, size="1em", bold=True)
                col += cs(f'{sub_kwh:.3f} kWh', sub_color, size="1em")
                col += cs(f'{currency}{sub_cost:.2f}', sub_color, size="1em")
                col += rate_rows_colored(meter_name, adjust_color(sub_color, 0.75))
                col += '</div>'
                breakdown_cols.append(col)

    breakdown_html = f'<div class="scols">{"".join(breakdown_cols)}</div>' if breakdown_cols else ''

    summary_html = (
        f'<div class="chart-summary">'
        f'<div class="day-label">{day}</div>'
        f'<div class="stotals">{totals_html}</div>'
        + (f'<div class="sdivider"></div>' if breakdown_cols else '')
        + breakdown_html
        + '</div>'
    )

    # ── Plotly traces ──
    traces = []
    for meter in sorted(meter_kwh.keys()):
        bar_color  = meter_colors.get(meter, "#333333")
        line_color = adjust_color(bar_color, 0.8)
        dash_style = "dash" if meter.endswith("_export") else "solid"
        _ys = meter_kwh[meter]
        customdata = [[x_ranges[i], total_hh_kwh[i], abs(_ys[i])] for i in range(slots)]

        if meter == "electricity_main_export":
            nice_name = meter_display_name.get(meter, "Grid Export")
        else:
            nice_name = meter_display_name.get(
                            meter,
                            meter.replace("_", " ")
                                 .replace("electricity main", "Direct import")
                                 .replace("export", "Grid Export")
                                 .title()
                        )
        raw_rates = meter_rate[meter]
        last_nonzero = max((i for i, v in enumerate(raw_rates) if v != 0.0), default=None)
        if last_nonzero is not None:
            truncated_rates = raw_rates[:last_nonzero + 1] + [raw_rates[last_nonzero]]
            trunc_x_line = [i - 0.5 for i in range(last_nonzero + 2)]
        else:
            truncated_rates = raw_rates + [raw_rates[-1]]
            trunc_x_line = [i - 0.5 for i in range(slots + 1)]

        hover_total = "" if meter.endswith("_export") else " (%{customdata[1]:.3f} total)"
        use_area   = block_minutes < 15
        chart_type = 'scatter' if use_area else 'bar'

        if use_area:
            extra_props = (", mode: 'lines', fill: 'tozeroy', line: "
                           + "{" + f"shape:'hv', color:'{bar_color}'" + "}")
        else:
            extra_props = (", width: 0.7, marker: "
                           + "{" + f"color: '{bar_color}'" + "}")

        has_rate_data = any(v != 0.0 for v in raw_rates)
        rate_trace = (
            ",{"
            + f"\n  x: {json.dumps(trunc_x_line)},"
            + f"\n  y: {json.dumps(truncated_rates)},"
            + "\n  type: 'scatter', mode: 'lines',"
            + "\n  line: " + "{" + f"shape:'hv', width:2, color:'{line_color}', dash:'{dash_style}'" + "},"
            + f"\n  name: '{nice_name} rate',"
            + "\n  yaxis: 'y2',"
            + "\n  customdata: xRanges.concat([xRanges[xRanges.length-1]]),"
            + f"\n  hovertemplate: '{nice_name} rate<br>%{{customdata}}<br>{currency}%{{y:.4f}}<extra></extra>'"
            + "\n}"
        ) if has_rate_data else ""

        traces.append(
            "{"
            + f"\n  x: xBar,"
            + f"\n  y: {json.dumps(meter_kwh[meter])},"
            + f"\n  type: '{chart_type}'"
            + extra_props + ","
            + f"\n  name: '{nice_name}',"
            + f"\n  customdata: {json.dumps(customdata)},"
            + f"\n  hovertemplate: '{nice_name}<br>%{{customdata[0]}}<br>%{{customdata[2]:.3f}} kWh{hover_total}<extra></extra>'"
            + "\n}"
            + rate_trace
        )

    chart_id      = f"{chart_prefix}chart_{day.replace('-', '_')}"
    chart_id_safe = chart_id.replace('-', '_')

    # ── Serialise chart data as JSON — browser won't parse until needed ──
    meters_data = {}
    for meter in sorted(meter_kwh.keys()):
        bar_color  = meter_colors.get(meter, "#333333")
        line_color = adjust_color(bar_color, 0.8)
        if meter == "electricity_main_export":
            nice_name = meter_display_name.get(meter, "Grid Export")
        else:
            nice_name = meter_display_name.get(
                meter,
                meter.replace("_", " ")
                     .replace("electricity main", "Direct import")
                     .replace("export", "Grid Export")
                     .title()
            )
        raw_rates = meter_rate[meter]
        last_nonzero = max((i for i, v in enumerate(raw_rates) if v != 0.0), default=None)
        if last_nonzero is not None:
            truncated_rates = raw_rates[:last_nonzero + 1] + [raw_rates[last_nonzero]]
            trunc_x_line    = [i - 0.5 for i in range(last_nonzero + 2)]
        else:
            truncated_rates = raw_rates + [raw_rates[-1]]
            trunc_x_line    = [i - 0.5 for i in range(slots + 1)]
        meters_data[meter] = {
            "y":            meter_kwh[meter],
            "cost":         meter_cost[meter],
            "rate":         truncated_rates,
            "rate_x":       trunc_x_line,
            "has_rate":     any(v != 0.0 for v in raw_rates),
            "bar_color":    bar_color,
            "line_color":   line_color,
            "nice_name":    nice_name,
            "is_export":    meter.endswith("_export"),
        }

    chart_data = {
        "x_labels":      x_labels,
        "x_ranges":      x_ranges,
        "slots":         slots,
        "block_minutes": block_minutes,
        "currency":      currency,
        "meters":        meters_data,
        "ti_kwh":        slot_ti_kwh,
        "ti_cost":       slot_ti_cost,
        "ti_rate":       slot_ti_rate,
        "meter_totals":  meter_totals,
        "ti_total":      ti_total,
    }
    # Opt-in only — keep the default JSON byte-identical for users who haven't
    # enabled Bill Rounding.
    if bill_rounding:
        chart_data["bill_rounding"]  = True
        chart_data["exc_ratio"]      = slot_exc_ratio
        chart_data["exc_approx"]     = slot_exc_approx
        chart_data["fallback_invat"] = round(1.0 / (1.0 + fallback_vat), 8)
    chart_data_json = json.dumps(chart_data, separators=(',', ':'))

    chart_id      = f"{chart_prefix}chart_{day.replace('-', '_')}"
    chart_id_safe = chart_id.replace('-', '_')
    table_id      = f"tbl_{chart_id_safe}"

    return f"""
<div class="day-chart-wrap">
  {summary_html}
  <div id="{chart_id}" class="chart-container"></div>
  <div id="{table_id}" class="day-data-tables" style="display:none;"></div>
  <div class="day-tbl-toolbar">
    <button class="day-tbl-toggle" onclick="toggleDayTables('{table_id}',this)">&#9776; Data</button>
  </div>
  <script type="application/json" id="data_{chart_id}">{chart_data_json}</script>
  <script>
  (function() {{
    if (!window._pendingCharts) window._pendingCharts = {{}};
    window._pendingCharts['{chart_id}'] = '{chart_id}';
  }})();
  </script>
</div>
"""




# ─────────────────────────────────────────────────────────────
# Main entry point
# ─────────────────────────────────────────────────────────────

def generate_daily_import_export_charts(blocks, timezone_name="UTC", block_minutes=None, currency='£', cfg=None, store=None):

    if not blocks:
        return "<html><body><p>No data available.</p></body></html>"

    try:
        _tz = ZoneInfo(timezone_name)
    except Exception:
        _tz = ZoneInfo("UTC")
    
# Use passed block_minutes, or derive from block meter meta, or default to 30
    if block_minutes is None:
        block_minutes = 30
        for b in blocks:
            bm = (((b or {}).get("meters") or {})
                  .get("electricity_main") or {})
            bm = (bm.get("meta") or {}).get("block_minutes")
            if bm:
                block_minutes = int(bm)
                break
    slots = 1440 // block_minutes
    today = datetime.now(tz=_tz).date()

    # ── Group blocks by day ──
    days_map = defaultdict(list)
    for block in blocks:
        try:
            if not block or not block.get("start"):
                continue
            start = _parse_block_start(block["start"], _tz)
            day   = start.date().isoformat()
            hh    = (start.hour * 60 + start.minute) // block_minutes
            days_map[day].append((hh, block))
        except Exception:
            pass

    meter_colors = build_meter_colors_from_config(cfg) if cfg else build_meter_colors(blocks)

    # Derived-EV split (display-only; BL-22): no-op unless this is a no-sub-meter
    # account with completed dispatches. Gives the Billing tab an "EV (from dispatch)"
    # breakdown line + per-day trace without moving any bill total.
    # Hybrid EV for the billing breakdown (H6): a has-EV-meter account gets one continuous
    # 'EV' line (synthetic post-seam, recorded physical pre-seam) via the fold-back; a no-EV-
    # meter account keeps the display-only dispatch split labelled 'EV (from dispatch)'.
    _ev_phys_id = _ev_meter_id(cfg)
    if _ev_phys_id:
        _ev_slot_map = _hybrid_ev_slot_map(store, blocks, cfg)
        _ev_fold_devices = [((cfg.get("meters", {}).get(_ev_phys_id, {}).get("meta") or {}).get("device")
                             or _ev_phys_id)]
        _ev_label = "EV"
    else:
        _ev_slot_map = _dispatch_ev_slot_map(store, blocks, cfg)
        _ev_fold_devices = None
        _ev_label = "EV (from dispatch)"
    # Day tables use a GATED dispatch map so an ACTIVE EV meter shows its own metered EV
    # (no phantom 'EV (from dispatch)' carve, no pre-seam bleed); the physical series is
    # relabelled 'EV'. A no-EV-meter account keeps the dispatch split. The period bill
    # summary still uses the hybrid fold-back above.
    _ev_day_map = None if _ev_phys_id else _ev_slot_map
    if _ev_slot_map:
        meter_colors["ev_dispatch"] = "#8b5cf6"
    if _ev_phys_id:
        meter_colors[_ev_phys_id] = "#8b5cf6"

    # ── Billing periods ──
    # Use historically correct billing_day per block from config_periods join.
    # Falls back to reading from meters_config.json / block meta for legacy data.
    try:
        from energy_engine_io import load_json as _load_json
        import os as _os
        _cfg = _load_json("/data/energy_meter_tracker/meters_config.json", {})
        _main_meta = {}
        for _md in _cfg.get("meters", {}).values():
            if not (_md.get("meta") or {}).get("sub_meter"):
                _main_meta = _md.get("meta") or {}
                break
        site_name = _main_meta.get("site") or None
    except Exception:
        site_name = None

    periods = get_billing_periods_from_config_history(blocks, tz=_tz)
    # billing_day for display: use current config value (most recent period)
    try:
        billing_day = int(_main_meta.get("billing_day") or 1)
    except Exception:
        billing_day = 1

    # Pre-index blocks by date string for fast per-period lookup
    _blocks_by_date = defaultdict(list)
    for _b in blocks:
        if _b and _b.get("start"):
            try:
                _bd = datetime.fromisoformat(_b["start"]).replace(tzinfo=ZoneInfo("UTC")).astimezone(_tz)
                _blocks_by_date[_bd.date().isoformat()].append(_b)
            except Exception:
                pass

    def _blocks_for_period(p_start, p_end):
        """Return only the blocks that fall within [p_start, p_end)."""
        result = []
        d = p_start.date()
        while d < p_end.date():
            result.extend(_blocks_by_date.get(d.isoformat(), []))
            from datetime import timedelta as _td
            d += _td(days=1)
        return result

    # BL-24 (opt-in): show the bill summary with Octopus's rounding method. Read once;
    # default off ⇒ nothing injected ⇒ the summary is byte-identical to today.
    _bill_rounding = False
    _vat_learned = []
    try:
        if store is not None:
            _bill_rounding = bool((store.get_settings() or {}).get("bill_rounding_summary"))
            _vat_learned = store.get_vat_calendar()
    except Exception:
        _bill_rounding = False
    # Gate on capability: ex-VAT is only meaningful where EMT can capture a real exc
    # figure (API/tariff). A CAD/cost-sensor-only user has no exc source at all, so we
    # never show it — the fallback would only ever be inc ÷ (1+VAT), a bare assumption.
    # Proxy "has API" by "any captured exc present in the data".
    if _bill_rounding and not any(
            ((((b or {}).get("meters") or {}).get("electricity_main") or {})
             .get("channels", {}).get("import", {}) or {}).get("cost_exc") is not None
            for b in (blocks or [])):
        _bill_rounding = False

    import vat_calendar as _vc
    def _vat_at(_when):
        """Statutory VAT rate at a date from the seed + learned calendar — the fallback
        rate used where no per-slot inc/exc pair exists (replaces the hardcoded 1.05)."""
        return _vc.resolve_vat(_when, _vat_learned)

    def _summ(_blks_for_calc, _s, _e):
        """calculate_billing_summary_for_period + the display-only derived-EV split, and
        (opt-in) the bill-method figure. Both are additive: the bill anchors / Total Bill
        stay byte-identical (see _inject_ev_breakdown_into_summary / _bill_method_summary)."""
        _sm = calculate_billing_summary_for_period(
            _blks_for_calc, _s, _e, store=store, tz_name=timezone_name)
        _period_blocks = _blocks_for_period(_s, _e)
        if _ev_slot_map:
            _inject_ev_breakdown_into_summary(_sm, _period_blocks, _ev_slot_map,
                                              label=_ev_label, fold_devices=_ev_fold_devices)
        if _bill_rounding:
            _bm = _bill_method_breakdown(_period_blocks, period_vat=_vat_at(_s),
                                         standing_inc_by_day=_sm.get("standing"))
            if _bm:
                _sm["bill_method"] = _bm
        return _sm

    # ── Build per-period data ──
    period_sections = []   # list of dicts

    for i, (p_start, p_end) in enumerate(periods):
        period_blocks = _blocks_for_period(p_start, p_end)
        summary   = _summ(period_blocks, p_start, p_end)
        is_current = p_start.date() <= today < p_end.date()
        is_prev    = (len(periods) > 1) and (i == len(periods) - 2) and not is_current

        # days that belong to this period
        period_days = sorted(
            [d for d in days_map if p_start.date().isoformat() <= d < p_end.date().isoformat()],
            reverse=True
        )

        # Detect config-change boundary — period starts on a non-billing-day
        # meaning it was created by a config change mid-cycle, not a natural boundary
        _is_config_change = (
            i > 0 and
            p_start.day != billing_day
        )
        period_sections.append({
            "index":          i,
            "start":          p_start,
            "end":            p_end,
            "summary":        summary,
            "is_current":     is_current,
            "is_prev":        is_prev,
            "is_config_change": _is_config_change,
            "days":           period_days,
        })

    # Filter out periods that have no blocks at all (e.g. tiny config-change slivers
    # that fall between billing day and the config change date)
    period_sections = [ps for ps in period_sections if ps["days"] or ps["is_current"]]

    # Re-assign is_prev after filtering (second-to-last non-current period)
    non_current = [ps for ps in period_sections if not ps["is_current"]]
    if non_current:
        non_current[-1]["is_prev"] = True

    # Sort periods newest-first for display
    period_sections_display = list(reversed(period_sections))

    # ── Quarter periods ──
    quarter_sections = []
    for i, (q_start, q_end) in enumerate(get_all_quarter_periods(blocks, tz=_tz)):
        quarter_blocks = _blocks_for_period(q_start, q_end)
        summary    = _summ(quarter_blocks, q_start, q_end)
        is_current = q_start.date() <= today < q_end.date()
        q_num      = (q_start.month - 1) // 3 + 1
        quarter_sections.append({
            "index":      i,
            "start":      q_start,
            "end":        q_end,
            "summary":    summary,
            "is_current": is_current,
            "label":      f"Q{q_num} {q_start.year}",
            "days":       sorted([d for d in days_map
                                  if q_start.date().isoformat() <= d < q_end.date().isoformat()],
                                 reverse=True),
        })
    quarter_sections_display = list(reversed(quarter_sections))
    quarter_by_index = {qs["index"]: qs for qs in quarter_sections}

    # ── Year periods ──
    year_sections = []
    for i, (y_start, y_end) in enumerate(get_all_year_periods(blocks, tz=_tz)):
        summary    = _summ(blocks, y_start, y_end)
        is_current = y_start.date() <= today < y_end.date()
        year_sections.append({
            "index":      i,
            "start":      y_start,
            "end":        y_end,
            "summary":    summary,
            "is_current": is_current,
            "label":      str(y_start.year),
            "days":       sorted([d for d in days_map
                                  if y_start.date().isoformat() <= d < y_end.date().isoformat()],
                                 reverse=True),
        })
    year_sections_display = list(reversed(year_sections))
    year_by_index = {ys["index"]: ys for ys in year_sections}

    # ── Calendar month periods ──
    calmonth_sections = []
    for i, (cm_start, cm_end) in enumerate(get_all_calmonth_periods(blocks, tz=_tz)):
        summary    = _summ(blocks, cm_start, cm_end)
        is_current = cm_start.date() <= today < cm_end.date()
        calmonth_sections.append({
            "index":      i,
            "start":      cm_start,
            "end":        cm_end,
            "summary":    summary,
            "is_current": is_current,
            "label":      cm_start.strftime("%b %Y"),
            "days":       sorted([d for d in days_map
                                  if cm_start.date().isoformat() <= d < cm_end.date().isoformat()],
                                 reverse=True),
        })
    calmonth_sections_display = list(reversed(calmonth_sections))
    calmonth_by_index = {cs["index"]: cs for cs in calmonth_sections}

    # ── Dropdown options (month) ──
    dropdown_options = []
    for ps in period_sections_display:
        s_str = ps["start"].strftime("%d %b %Y")
        e_str = (ps["end"] - timedelta(seconds=1)).strftime("%d %b %Y")
        cost  = ps["summary"]["total_cost"]
        label = f"{s_str} → {e_str}  |  {currency}{cost:.2f}"
        if ps["is_current"]:
            label = "★ Current  " + label
        dropdown_options.append(f'<option value="period_{ps["index"]}">{label}</option>')

    # ── Dropdown options (calmonth) ──
    calmonth_options = []
    for cs in calmonth_sections_display:
        cost  = cs["summary"]["total_cost"]
        label = f"{cs['label']}  |  {currency}{cost:.2f}"
        if cs["is_current"]:
            label = "★ Current  " + label
        calmonth_options.append(f'<option value="calmonth_{cs["index"]}">{label}</option>')

    # ── Dropdown options (quarter) ──
    quarter_options = []
    for qs in quarter_sections_display:
        cost  = qs["summary"]["total_cost"]
        label = f"{qs['label']}  |  {currency}{cost:.2f}"
        if qs["is_current"]:
            label = "★ Current  " + label
        quarter_options.append(f'<option value="quarter_{qs["index"]}">{label}</option>')

    # ── Dropdown options (year) ──
    year_options = []
    for ys in year_sections_display:
        cost  = ys["summary"]["total_cost"]
        label = f"{ys['label']}  |  {currency}{cost:.2f}"
        if ys["is_current"]:
            label = "★ Current  " + label
        year_options.append(f'<option value="year_{ys["index"]}">{label}</option>')

    dropdown_html = f"""
<div class="period-nav">
  <div class="nav-left">
    <div class="period-mode-toggle">
      <span class="period-mode-label">Period:</span>
      <button class="pmode-btn active" data-mode="month"    onclick="setPeriodMode('month')">Bill</button>
      <button class="pmode-btn"        data-mode="calmonth" onclick="setPeriodMode('calmonth')">Month</button>
      <button class="pmode-btn"        data-mode="quarter"  onclick="setPeriodMode('quarter')">Quarter</button>
      <button class="pmode-btn"        data-mode="year"     onclick="setPeriodMode('year')">Year</button>
    </div>
    <span class="period-stepper">
      <button class="pstep-btn" title="Older period" onclick="_billStep(1)">&#8249;</button>
      <span id="period-rolodex-label" title="Scroll or drag to spin, or click to pick a period"></span>
      <button class="pstep-btn" title="Newer period" onclick="_billStep(-1)">&#8250;</button>
    </span>
    <div class="period-select-wrap" id="select-month">
      <label for="period-select-month" style="font-size:11px;">Billing Period:</label>
      <select id="period-select-month" onchange="showPeriod(this.value, 'month')">
        {chr(39)+chr(39).join(dropdown_options)}
      </select>
    </div>
    <div class="period-select-wrap" id="select-calmonth" style="display:none;">
      <label for="period-select-calmonth" style="font-size:11px;">Month:</label>
      <select id="period-select-calmonth" onchange="showPeriod(this.value, 'calmonth')">
        {chr(39)+chr(39).join(calmonth_options)}
      </select>
    </div>
    <div class="period-select-wrap" id="select-quarter" style="display:none;">
      <label for="period-select-quarter" style="font-size:11px;">Quarter:</label>
      <select id="period-select-quarter" onchange="showPeriod(this.value, 'quarter')">
        {chr(39)+chr(39).join(quarter_options)}
      </select>
    </div>
    <div class="period-select-wrap" id="select-year" style="display:none;">
      <label for="period-select-year" style="font-size:11px;">Year:</label>
      <select id="period-select-year" onchange="showPeriod(this.value, 'year')">
        {chr(39)+chr(39).join(year_options)}
      </select>
    </div>
  </div>
  <div id="sticky-bill-strip">
    <span id="sticky-bill-label"></span>
    <button id="sticky-bill-btn" onclick="stickyBillExpand()">&#8593; Show Bill</button>
  </div>
  <div class="nav-right view-toggle">
    <button class="view-btn active" data-view="vanilla" onclick="showView('vanilla')">Bill</button>
    <button class="view-btn" data-view="vs-prev"        onclick="showView('vs-prev')">vs Prev</button>
    <button class="view-btn vs-year-btn" data-view="vs-year" onclick="showView('vs-year')">vs Last Year</button>
    <button class="view-btn censor-btn" id="censor-toggle" onclick="toggleCensor()" title="Blur sensitive info">&#128065; Censor</button>
    <button class="view-btn" id="sort-toggle" onclick="toggleSortOrder()" title="Toggle period order">↓ Latest first</button>
    <button class="view-btn" id="expand-all-btn" onclick="toggleAllTables()" title="Show or hide all data tables">Show Data</button>
  </div>
</div>"""

    # ── Build period lookup for comparison ──
    period_by_index = {ps["index"]: ps for ps in period_sections}

    def find_prev_period(ps):
        return period_by_index.get(ps["index"] - 1)

    def find_year_period(ps):
        target = ps["start"].replace(year=ps["start"].year - 1)
        best, best_delta = None, timedelta(days=36)
        for other in period_by_index.values():
            if other["index"] == ps["index"]:
                continue
            delta = abs(other["start"] - target)
            if delta < best_delta:
                best_delta, best = delta, other
        return best

    # ── Period section HTML ──
    sections_html_parts = []

    for ps in period_sections_display:
        pid         = f"period_{ps['index']}"
        is_current  = ps["is_current"]
        is_prev     = ps["is_prev"]
        charts_open = is_current or is_prev

        s_str      = ps["start"].strftime("%d %b %Y")
        e_str      = (ps["end"] - timedelta(seconds=1)).strftime("%d %b %Y")
        bill_total = ps["summary"]["total_cost"]
        ph         = f"{s_str} &rarr; {e_str}"   # period heading shorthand

        # Current period bill HTML (with optional current-period highlight class)
        cur_bill = render_billing_summary(ps["summary"], currency=currency, site_name=site_name)
        extra    = " current-period" if is_current else ""
        cur_bill = cur_bill.replace('<div class="billing-summary">',
                                    f'<div class="billing-summary{extra}">', 1)

        def col(heading, bill_html, is_compare=False):
            cls = "bill-view-heading compare" if is_compare else "bill-view-heading"
            return f'<div class="bill-compare-col"><div class="{cls}"><h2>{heading}</h2></div>{bill_html}</div>'

        def empty_col(msg):
            return f'<div class="bill-compare-col bill-compare-empty"><p>{msg}</p></div>'

        # Vanilla
        vanilla_html = f'<div class="bill-compare-wrap">{col(ph, cur_bill)}</div>'

        # vs Previous
        prev_ps = find_prev_period(ps)
        if prev_ps:
            prev_h    = f"{prev_ps['start'].strftime('%d %b %Y')} &rarr; {(prev_ps['end'] - timedelta(seconds=1)).strftime('%d %b %Y')}"
            prev_bill = render_billing_summary(prev_ps["summary"], currency=currency, site_name=site_name)
            vs_prev_html = f'<div class="bill-compare-wrap">{col(ph, cur_bill)}{col(prev_h, prev_bill, True)}</div>'
        else:
            vs_prev_html = f'<div class="bill-compare-wrap">{col(ph, cur_bill)}{empty_col("No previous period available.")}</div>'

        # vs Last Year
        year_ps = find_year_period(ps)
        if year_ps:
            year_h    = f"{year_ps['start'].strftime('%d %b %Y')} &rarr; {(year_ps['end'] - timedelta(seconds=1)).strftime('%d %b %Y')}"
            year_bill = render_billing_summary(year_ps["summary"], currency=currency, site_name=site_name)
            vs_year_html = f'<div class="bill-compare-wrap">{col(ph, cur_bill)}{col(year_h, year_bill, True)}</div>'
        else:
            vs_year_html = f'<div class="bill-compare-wrap">{col(ph, cur_bill)}{empty_col("No data for same period last year.")}</div>'

        # Daily charts
        day_charts_html = ""
        for day in ps["days"]:
            day_charts_html += build_day_chart_html(day, days_map[day], meter_colors, block_minutes=block_minutes, currency=currency, site_name=site_name, ev_slot_map=_ev_day_map, bill_rounding=_bill_rounding, fallback_vat=_vat_at(day), ev_relabel_meter=_ev_phys_id, ev_label=("EV" if _ev_phys_id else None))

        open_attr    = "open" if charts_open else ""
        toggle_label = f"Daily Charts &mdash; {ph} &nbsp;|&nbsp; {currency}{bill_total:.2f}"

        config_change_banner = ""
        if ps.get("is_config_change"):
            config_change_banner = f"""
<div style="display:flex;align-items:center;gap:10px;padding:8px 14px;margin-bottom:12px;
            background:rgba(124,106,247,0.08);border:1px solid rgba(124,106,247,0.3);
            border-radius:6px;font-size:12px;color:#7c6af7;">
  ⚙️ <strong>Configuration changed</strong> &mdash; new billing period started {ps['start'].strftime('%-d %b %Y')}
</div>"""

        sections_html_parts.append(f"""
<div class="period-section month-section" id="{pid}" style="display:none;">
  {config_change_banner}
  <details class="bill-toggle" open>
    <summary class="bill-toggle-summary">Bill Summary &mdash; {ph} &nbsp;|&nbsp; {currency}{bill_total:.2f}</summary>
    <div class="bill-toggle-body">
      <div class="bill-view" data-view="vanilla">{vanilla_html}</div>
      <div class="bill-view" data-view="vs-prev" style="display:none;">{vs_prev_html}</div>
      <div class="bill-view" data-view="vs-year" style="display:none;">{vs_year_html}</div>
    </div>
  </details>
  <details class="day-charts-toggle" {open_attr}>
    <summary class="day-charts-summary">{toggle_label}</summary>
    <div class="day-charts-body">
      {day_charts_html}
    </div>
  </details>
</div>
""")

    # ── Grouped section builder (quarters + years) ──
    def build_grouped_section(gs, pid_prefix, find_prev_fn, find_year_fn, show_year_btn=True):
        pid        = f"{pid_prefix}_{gs['index']}"
        ph         = gs["label"]
        bill_total = gs["summary"]["total_cost"]

        cur_bill = render_billing_summary(gs["summary"], currency=currency, site_name=site_name)
        extra    = " current-period" if gs["is_current"] else ""
        cur_bill = cur_bill.replace('<div class="billing-summary">',
                                    f'<div class="billing-summary{extra}">', 1)

        def col(heading, bill_html, is_compare=False):
            cls = "bill-view-heading compare" if is_compare else "bill-view-heading"
            return f'<div class="bill-compare-col"><div class="{cls}"><h2>{heading}</h2></div>{bill_html}</div>'

        def empty_col(msg):
            return f'<div class="bill-compare-col bill-compare-empty"><p>{msg}</p></div>'

        vanilla_html = f'<div class="bill-compare-wrap">{col(ph, cur_bill)}</div>'

        prev_gs = find_prev_fn(gs)
        if prev_gs:
            prev_bill    = render_billing_summary(prev_gs["summary"], currency=currency, site_name=site_name)
            vs_prev_html = f'<div class="bill-compare-wrap">{col(ph, cur_bill)}{col(prev_gs["label"], prev_bill, True)}</div>'
        else:
            vs_prev_html = f'<div class="bill-compare-wrap">{col(ph, cur_bill)}{empty_col("No previous period available.")}</div>'

        if show_year_btn:
            year_gs = find_year_fn(gs)
            if year_gs:
                year_bill    = render_billing_summary(year_gs["summary"], currency=currency, site_name=site_name)
                vs_year_html = f'<div class="bill-compare-wrap">{col(ph, cur_bill)}{col(year_gs["label"], year_bill, True)}</div>'
            else:
                vs_year_html = f'<div class="bill-compare-wrap">{col(ph, cur_bill)}{empty_col("No data for same period last year.")}</div>'
        else:
            vs_year_html = f'<div class="bill-compare-wrap">{col(ph, cur_bill)}{empty_col("Not available in year view.")}</div>'

        day_charts_html = ""
        for day in gs["days"]:
            day_charts_html += build_day_chart_html(day, days_map[day], meter_colors, chart_prefix=f"{pid_prefix}_", block_minutes=block_minutes, currency=currency, site_name=site_name, ev_slot_map=_ev_day_map, bill_rounding=_bill_rounding, fallback_vat=_vat_at(day), ev_relabel_meter=_ev_phys_id, ev_label=("EV" if _ev_phys_id else None))

        toggle_label = f"Daily Charts &mdash; {ph} &nbsp;|&nbsp; {currency}{bill_total:.2f}"
        # Quarter/year sections can hold 90–365 day charts; default the panel
        # collapsed (summary-first) so switching to those modes is instant. The
        # charts build lazily when the panel is expanded (see _renderSection).
        open_attr    = ""
        return (
            f'<div class="period-section {pid_prefix}-section" id="{pid}" style="display:none;">'
            f'<details class="bill-toggle" open>'
            f'<summary class="bill-toggle-summary">Bill Summary &mdash; {ph} &nbsp;|&nbsp; {currency}{bill_total:.2f}</summary>'
            f'<div class="bill-toggle-body">'
            f'<div class="bill-view" data-view="vanilla">{vanilla_html}</div>'
            f'<div class="bill-view" data-view="vs-prev" style="display:none;">{vs_prev_html}</div>'
            f'<div class="bill-view" data-view="vs-year" style="display:none;">{vs_year_html}</div>'
            f'</div></details>'
            f'<details class="day-charts-toggle" {open_attr}>'
            f'<summary class="day-charts-summary">{toggle_label}</summary>'
            f'<div class="day-charts-body">{day_charts_html}</div>'
            f'</details></div>'
        )

    calmonth_html_parts = [
        build_grouped_section(
            cs, "calmonth",
            find_prev_fn=lambda c: calmonth_by_index.get(c["index"] - 1),
            find_year_fn=lambda c: next(
                (o for o in calmonth_by_index.values()
                 if o["start"].month == c["start"].month and o["start"].year == c["start"].year - 1),
                None),
        )
        for cs in calmonth_sections_display
    ]

    quarter_html_parts = [
        build_grouped_section(
            qs, "quarter",
            find_prev_fn=lambda q: quarter_by_index.get(q["index"] - 1),
            find_year_fn=lambda q: next(
                (o for o in quarter_by_index.values()
                 if o["start"].month == q["start"].month and o["start"].year == q["start"].year - 1),
                None),
        )
        for qs in quarter_sections_display
    ]

    year_html_parts = [
        build_grouped_section(
            ys, "year",
            find_prev_fn=lambda y: year_by_index.get(y["index"] - 1),
            find_year_fn=lambda y: None,
            show_year_btn=False,
        )
        for ys in year_sections_display
    ]

    # ── Determine first period to show ──
    default_period = next(
        (f"period_{ps['index']}" for ps in period_sections_display if ps["is_current"]),
        f"period_{period_sections_display[0]['index']}" if period_sections_display else ""
    )
    default_calmonth = next(
        (f"calmonth_{cs['index']}" for cs in calmonth_sections_display if cs["is_current"]),
        f"calmonth_{calmonth_sections_display[0]['index']}" if calmonth_sections_display else ""
    )
    default_quarter = next(
        (f"quarter_{qs['index']}" for qs in quarter_sections_display if qs["is_current"]),
        f"quarter_{quarter_sections_display[0]['index']}" if quarter_sections_display else ""
    )
    default_year = next(
        (f"year_{ys['index']}" for ys in year_sections_display if ys["is_current"]),
        f"year_{year_sections_display[0]['index']}" if year_sections_display else ""
    )


    # ── Full HTML ──
    spinner_html = _load_period_spinner_html()
    html = f"""<!DOCTYPE html>
<html data-theme="light">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<meta http-equiv="Cache-Control" content="no-cache, no-store, must-revalidate"/>
<meta http-equiv="Pragma" content="no-cache"/>
<meta http-equiv="Expires" content="0"/>
<script>
(function(){{
  var stored = localStorage.getItem('emt_chart_theme');
  var sys = window.matchMedia('(prefers-color-scheme: light)').matches ? 'light' : 'dark';
  document.documentElement.setAttribute('data-theme', stored || sys);
}})();
function _getThemeColours() {{
  var dark = document.documentElement.getAttribute('data-theme') !== 'light';
  return {{
    plotBg:  dark ? '#1a1d27' : '#f8f9fa',
    paperBg: dark ? '#1a1d27' : '#ffffff',
    axisC:   dark ? '#6b7080' : '#555566',
    gridC:   dark ? '#2a2d3a' : '#e5e5e5',
  }};
}}
</script>
<script src="https://cdn.plot.ly/plotly-3.0.1.min.js"></script>
{spinner_html}
<style>

/* ── Theme variables ──────────────────────────── */
:root {{
  --bg:      #f0f2f5; --surface: #ffffff; --border: #d0d5dd;
  --text:    #1a1d27; --muted:   #555970; --accent: #0a8c6a;
  --card:    #ffffff; --input-bg:#fafafa; --radius: 8px;
}}
[data-theme="dark"] {{
  --bg:      #0f1117; --surface: #1a1d27; --border: #2a2d3a;
  --text:    #e8eaf0; --muted:   #6b7080; --accent: #00d4aa;
  --card:    #1a1d27; --input-bg:#0f1117;
}}

/* ── Period rolodex ───────────────────────────── */
/* The native <select> per mode stays in the DOM as the data source and for
   programmatic value-setting, but is hidden -- the rolodex label replaces it
   visually, matching Usage Stats / Insights. */
.period-select-wrap {{ display: none !important; }}
.period-stepper {{ display: inline-flex; align-items: center; gap: 2px; }}
#period-rolodex-label {{
  font-size: 13px; font-weight: 600; color: var(--text);
  /* Fixed width (not min-width) so the ‹ › arrows stay put while stepping:
     proportional-font date labels differ in pixel width between periods even
     at equal character counts, which would otherwise nudge the right arrow. */
  width: 210px; display: inline-block; text-align: center;
  white-space: nowrap; overflow: hidden; text-overflow: ellipsis;
}}
.pstep-btn {{
  border: 1px solid var(--border); background: var(--surface); color: var(--text);
  border-radius: var(--radius); cursor: pointer; font-size: 15px; line-height: 1;
  padding: 3px 9px;
}}
.pstep-btn:hover {{ background: var(--border); }}

/* ── Base ─────────────────────────────────────── */
*, *::before, *::after {{ box-sizing: border-box; }}

/* ── Censor mode ───────────────────────────────── */
.censored {{
  filter: none;
  transition: filter 0.2s;
}}
body.censor-on .censored {{
  filter: blur(6px);
  user-select: none;
}}
.censor-btn.active {{
  background: #c0392b !important;
  color: white !important;
  border-color: #a93226 !important;
}}

html {{
  scroll-padding-top: 80px;
}}
body {{
  margin: 0;
  padding: 0 16px 16px 16px;
  background: var(--bg);
  font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
  font-size: 14px;
  color: var(--text);
}}

.page-wrap {{
  max-width: 100%;
  margin: 0;
  padding: 0 8px;
}}

/* ── Period nav ───────────────────────────────── */
.period-nav {{
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 8px;
  background: var(--surface);
  padding: 8px 12px;
  border-radius: 0 0 8px 8px;
  box-shadow: 0 2px 8px rgba(0,0,0,0.4);
  margin-bottom: 12px;
  flex-wrap: wrap;
  position: sticky;
  top: 0;
  z-index: 100;
  transition: box-shadow 0.2s;
}}
.nav-left {{
  display: flex;
  align-items: center;
  gap: 6px;
}}
.nav-right {{
  display: flex;
  align-items: center;
  gap: 6px;
}}
.period-nav select {{
  font-size: 11px;
  padding: 2px 7px;
  border: 1px solid var(--border);
  border-radius: 5px;
  background: var(--bg);
  color: var(--text);
  cursor: pointer;
  min-width: 280px;
}}
.view-btn {{
  font-size: 11px;
  padding: 4px 10px;
  border: 1px solid var(--border);
  border-radius: 5px;
  background: var(--surface);
  color: var(--muted);
  cursor: pointer;
  transition: background 0.15s, border-color 0.15s;
}}
.view-btn:hover {{
  background: var(--border);
}}
.view-btn.active {{
  background: var(--accent);
  color: var(--bg);
  border-color: var(--accent);
}}

/* ── Bill comparison layout ───────────────────── */
.bill-compare-wrap {{
  display: flex;
  gap: 20px;
  margin-bottom: 16px;
  flex-wrap: wrap;
}}
.bill-compare-col {{
  flex: 1;
  min-width: 300px;
}}
.bill-compare-empty {{
  display: flex;
  align-items: center;
  justify-content: center;
  background: var(--surface);
  border: 1px dashed var(--border);
  border-radius: 8px;
  color: var(--muted);
  font-style: italic;
  padding: 20px;
}}
.bill-view-heading h2 {{
  font-size: 15px;
  font-weight: 600;
  margin: 0 0 8px 0;
  color: var(--text);
}}
.bill-view-heading.compare h2 {{
  color: var(--muted);
}}

/* ── Billing card ─────────────────────────────── */
.billing-summary {{
  background: var(--surface);
  padding: 16px 20px;
  margin-bottom: 12px;
  border-radius: 8px;
  border: 1px solid var(--border);
  position: relative;
  overflow: hidden;
}}
.billing-summary::after,
.day-chart-wrap::after {{
  content: "Informational — not authoritative";
  position: absolute;
  top: 50%;
  left: 50%;
  transform: translate(-50%, -50%) rotate(-30deg);
  font-size: 22px;
  font-weight: 700;
  color: rgba(255,255,255,0.06);
  pointer-events: none;
  user-select: none;
  letter-spacing: 0.05em;
  z-index: 9999;
}}

.billing-summary.current-period {{
  border: 2px solid var(--accent);
  background: rgba(0,212,170,0.05);
}}
.bill-site-header td {{
  text-align: left;
  font-size: 15px;
  font-weight: 700;
  color: var(--text);
  padding: 0 0 12px 0;
  border-bottom: 2px solid var(--border);
}}
.reads {{
  font-size: 11px;
  font-weight: 400;
  color: var(--muted);
}}

.billing-summary h2 {{
  margin: 0 0 18px 0;
  font-size: 18px;
  font-weight: 600;
  color: var(--accent);
}}

/* ── Billing table ────────────────────────────── */
.billing-table {{
  border-collapse: collapse;
  width: 100%;
  font-size: 12px;
}}
.billing-table td {{
  padding: 3px 0;
  text-align: right;
}}
.channel-title td {{
  text-align: left;
  padding-top: 10px;
  padding-bottom: 2px;
  font-weight: 600;
  font-size: 13px;
  color: var(--text);
  border-top: 1px solid var(--border);
}}
.channel-header td {{ font-size: 11px; color: var(--muted); padding-bottom: 2px; }}
.channel-total td  {{ padding-top: 2px; font-weight: 600; }}
.standing td       {{ padding-top: 8px; }}
.submeter-breakdown-header td {{
  text-align: left;
  padding-top: 10px;
  padding-bottom: 3px;
  font-size: 11px;
  font-weight: 600;
  color: var(--muted);
  text-transform: uppercase;
  letter-spacing: 0.05em;
  border-top: 1px solid var(--border);
}}
.submeter-indent td:first-child {{ padding-left: 12px; }}
.channel-title.submeter-indent td {{
  border-top: none;
  padding-top: 6px;
  font-size: 12px;
  font-weight: 600;
}}
.channel-header.submeter-indent td {{ padding-left: 12px; }}
.grand-total td    {{
  padding-top: 8px;
  font-size: 14px;
  font-weight: 700;
  color: var(--accent);
  border-top: 2px solid var(--accent);
}}

/* ── Details / toggle ─────────────────────────── */
.day-charts-toggle {{
  background: var(--surface);
  border-radius: 8px;
  border: 1px solid var(--border);
  margin-bottom: 24px;
  overflow: hidden;
}}

.day-charts-summary {{
  cursor: pointer;
  padding: 12px 20px;
  font-weight: 600;
  font-size: 14px;
  color: var(--accent);
  background: rgba(0,212,170,0.08);
  user-select: none;
  list-style: none;
  display: flex;
  align-items: center;
  gap: 8px;
}}
.day-charts-summary::-webkit-details-marker {{ display: none; }}
.day-charts-summary::before {{
  content: "▶";
  font-size: 10px;
  transition: transform 0.2s;
  display: inline-block;
}}
.day-charts-toggle[open] .day-charts-summary::before {{
  transform: rotate(90deg);
}}

.day-charts-body {{
  padding: 12px 0 0 0;
}}

/* ── Day chart row ────────────────────────────── */
.day-chart-wrap {{
  display: flex;
  background: var(--surface);
  border-bottom: 1px solid var(--border);
  position: relative;
  overflow: hidden;
}}
.day-chart-wrap:last-child {{ border-bottom: none; }}

/* ── Summary panel ────────────────────────────── */
.chart-summary {{
  flex: 0 0 auto;
  width: auto;
  min-width: 120px;
  max-width: 220px;
  padding: 12px 10px;
  border-right: 1px solid var(--border);
  display: flex;
  flex-direction: column;
  gap: 0;
  overflow: hidden;
  font-size: clamp(10px, 1vw, 13px);
}}

.day-label {{
  font-weight: 700;
  font-size: 1em;
  color: var(--accent);
  margin-bottom: 8px;
}}

/* Meter sections inside the panel */
.scol {{
  display: flex;
  flex-direction: column;
  margin-bottom: 7px;
}}
.scol.sub    {{ color: var(--muted); }}
.scol.export {{ color: #ff9944; }}

.slabel {{
  font-weight: 600;
  font-size: 10px;
  text-transform: uppercase;
  letter-spacing: 0.04em;
  color: var(--muted);
  margin-bottom: 1px;
}}
.scol.export .slabel {{ color: #ffaa55; }}

.stotals {{
  display: flex;
  flex-direction: column;
  gap: 1px;
  margin-bottom: 4px;
}}

.sval {{
  font-size: 12px;
  line-height: 1.6;
  color: var(--text);
}}
.sval.export-val {{ color: #ff9944; }}
.sval.sub-val    {{ color: var(--muted); font-size: 11px; }}
.scol.export .sval {{ color: #ff9944; }}

.sdivider {{
  border-top: 1px solid var(--border);
  margin: 6px 0;
}}

.rate-row {{
  font-size: 0.8em;
  color: #4b5563;
  line-height: 1.4;
  white-space: nowrap;
}}
.rate-row.export {{ color: #ffaa55; }}
.rate-row.sub    {{ color: #4b5563; }}

/* ── Period mode toggle ───────────────────────── */
.period-mode-toggle {{
  display: flex;
  align-items: center;
  gap: 4px;
  margin-bottom: 0px;
}}
.period-mode-label {{
  font-size: 12px;
  color: var(--muted);
  margin-right: 4px;
}}
.pmode-btn {{
  padding: 2px 7px;
  font-size: 11px;
  border: 1px solid var(--border);
  border-radius: 4px;
  background: var(--surface);
  cursor: pointer;
  color: var(--muted);
}}
.pmode-btn.active {{
  background: var(--accent);
  color: var(--bg);
  border-color: var(--accent);
}}
.period-select-wrap {{
  display: flex;
  align-items: center;
  gap: 8px;
}}


/* ── Sticky bill strip ────────────────────────── */
#sticky-bill-strip {{
  width: 100%;
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: 7px 4px 2px 4px;
  border-top: 1px solid var(--border);
  font-size: 13px;
  color: var(--text);
  order: 3;
  visibility: hidden;  /* always takes up space — never changes nav height */
}}
#sticky-bill-label {{
  font-weight: 600;
  color: var(--accent);
}}
#sticky-bill-btn {{
  font-size: 12px;
  padding: 4px 12px;
  border: 1px solid var(--accent);
  border-radius: 4px;
  background: rgba(0,212,170,0.08);
  color: var(--accent);
  cursor: pointer;
  white-space: nowrap;
}}
#sticky-bill-btn:hover {{
  background: var(--accent);
  color: var(--bg);
}}

/* ── Bill toggle ──────────────────────────────── */
.bill-toggle {{
  background: var(--surface);
  border-radius: 8px;
  border: 1px solid var(--border);
  margin-bottom: 12px;
  overflow: hidden;
}}
.bill-toggle-summary {{
  display: flex;
  align-items: center;
  gap: 8px;
  padding: 12px 20px;
  font-size: 14px;
  font-weight: 600;
  color: var(--accent);
  cursor: pointer;
  user-select: none;
  list-style: none;
  background: var(--surface);
  border-radius: 8px;
}}
.bill-toggle-summary::-webkit-details-marker {{ display: none; }}
.bill-toggle-summary::before {{
  content: '▶';
  font-size: 11px;
  color: var(--muted);
  transition: transform 0.2s;
  display: inline-block;
  width: 14px;
}}
.bill-toggle[open] .bill-toggle-summary::before {{
  transform: rotate(90deg);
}}
.bill-toggle-body {{
  padding: 0 0 4px 0;
}}

/* ── Chart container ──────────────────────────── */
.chart-container {{
  flex: 1 1 0;
  min-width: 0;
  /* Reserve the day-chart height BEFORE it's lazily built, so a chart that
     materialises above the viewport (scrolling up) doesn't grow from ~0 and shove
     the scroll position. Matches the built layout height floor below. */
  min-height: 320px;
}}
/* ── Day data table ───────────────────────────── */
.day-chart-wrap {{ flex-wrap: wrap; }}
.day-data-tables {{
  flex: 0 0 100%;
  overflow-x: auto;
  border-top: 1px solid var(--border);
}}
.day-meter-table {{ width: 100%; }}
.day-meter-table table {{
  border-collapse: collapse;
  width: 100%;
  font-size: 11px;
  white-space: nowrap;
}}
.day-meter-table th {{
  padding: 3px 8px;
  text-align: right;
  font-weight: 600;
  font-size: 10px;
  text-transform: uppercase;
  color: var(--muted);
  border-bottom: 1px solid var(--border);
  border-right: 1px solid var(--border);
  background: var(--bg);
}}
.day-meter-table th:first-child {{ text-align: left; position: sticky; left: 0; z-index: 2; }}
.day-meter-table th[colspan] {{ text-align: center; }}
.day-meter-table td {{
  padding: 2px 8px;
  text-align: right;
  border-bottom: 1px solid var(--border);
  border-right: 1px solid var(--border);
  color: var(--text);
}}
.day-meter-table td:first-child {{
  text-align: left;
  color: var(--muted);
  font-variant-numeric: tabular-nums;
  position: sticky;
  left: 0;
  background: var(--surface);
  z-index: 1;
}}
.day-meter-table tr:hover td {{ background: rgba(0,212,170,0.06); }}
.day-meter-table tr:hover td:first-child {{ background: var(--surface); }}
.day-meter-table tfoot td {{
  font-weight: 700;
  color: var(--accent);
  border-top: 1px solid var(--border);
  background: var(--surface);
}}
.day-tbl-toolbar {{
  flex: 0 0 100%;
  padding: 3px 8px;
  border-top: 1px solid var(--border);
  background: var(--surface);
}}
.day-tbl-toggle {{
  font-size: 11px;
  padding: 2px 8px;
  border: 1px solid var(--border);
  border-radius: 4px;
  background: var(--surface);
  color: var(--muted);
  cursor: pointer;
  transition: background 0.15s;
}}
.day-tbl-toggle:hover, .day-tbl-toggle.active {{
  background: var(--accent);
  color: var(--bg);
  border-color: var(--accent);
}}
/* ── Landscape mobile — let period-nav scroll off for more chart space ── */
@media (max-height: 500px) and (orientation: landscape) {{
  .period-nav {{
    position: static;
  }}
}}
/* ── Mobile responsive ────────────────────────── */
@media (max-width: 600px) {{
  .period-nav {{
    padding: 6px 10px;
    gap: 4px;
  }}
  .nav-left {{
    flex-wrap: wrap;
    gap: 4px;
  }}
  .period-mode-toggle {{
    margin-bottom: 0;
  }}
  .period-nav select {{
    font-size: 11px;
    padding: 2px 6px;
    border: 1px solid var(--border);
    border-radius: 5px;
    background: var(--bg);
    cursor: pointer;
    min-width: 0;
    width: 100%;
  }}
  .pmode-btn {{
    padding: 2px 6px;
    font-size: 11px;
  }}
  .view-btn {{
    padding: 2px 6px;
    font-size: 11px;
  }}
  .period-select-wrap {{
    flex-direction: column;
    align-items: flex-start;
    gap: 3px;
    width: 100%;
  }}
  .day-chart-wrap {{
    flex-direction: column;
  }}
  .chart-summary {{
    flex: none;
    width: 100%;
    border-right: none;
    border-bottom: 1px solid var(--border);
    padding: 8px 12px;
    flex-direction: row;
    flex-wrap: wrap;
    gap: 6px 16px;
  }}
  .day-label {{
    width: 100%;
    margin-bottom: 2px;
  }}
  .scol {{
    margin-bottom: 2px;
  }}
  .bill-compare-wrap {{
    flex-direction: column;
  }}
  .bill-compare-col {{
    min-width: 0;
  }}
}}

</style>
</head>
<body>
<div class="page-wrap">

{dropdown_html}

<div id="period-sections-wrap" data-asc="0">
{''.join(sections_html_parts)}
</div>
{''.join(calmonth_html_parts)}
{''.join(quarter_html_parts)}
{''.join(year_html_parts)}

</div>

<script>
// ── Mobile chart scaling ──────────────────────────────────
var _MIN_CHART_W = 380; // px — below this we scale rather than reflow
var _relayoutTimer = null;

function _scaleChartEl(el, deferRelayout) {{
  var wrap = el.closest('.day-chart-wrap');
  if (!wrap) return;
  // Use el.offsetWidth — the actual flex-computed chart container width
  var avail = el.offsetWidth;
  if (avail < 1) return;
  var isMobile = window.getComputedStyle(wrap).flexDirection === 'column';
  if (isMobile) {{
    el.style.transform = '';
    el.style.transformOrigin = '';
    el.style.width = '';
    el.style.height = '';
    if (!deferRelayout && el._fullData && avail < 480) {{
      var skipEvery = avail < 320 ? 5 : 3;
      var vals = [], texts = [];
      el._fullLayout.xaxis.tickvals.forEach(function(v, i) {{
        if (i % skipEvery === 0) {{ vals.push(v); texts.push(el._fullLayout.xaxis.ticktext[i]); }}
      }});
      Plotly.relayout(el, {{'xaxis.tickvals': vals, 'xaxis.ticktext': texts}});
    }}
  }} else if (avail < _MIN_CHART_W) {{
    var scale = avail / _MIN_CHART_W;
    el.style.transformOrigin = 'top left';
    el.style.transform = 'scale(' + scale + ')';
    el.style.width = _MIN_CHART_W + 'px';
    el.style.height = (el.offsetHeight / scale) + 'px';
  }} else {{
    el.style.transform = '';
    el.style.transformOrigin = '';
    el.style.width = '';
    el.style.height = '';
    // Plotly relayout deferred — batched after resize settles
    if (!deferRelayout && window.Plotly && window._energyCharts && window._energyCharts[el.id]) {{
      Plotly.relayout(el, {{autosize: true, width: avail}});
    }}
  }}
}}

function _scaleDayCharts() {{
  // Pass 1: CSS transforms only — instant, no Plotly calls
  document.querySelectorAll('.chart-container').forEach(function(el) {{
    if (window._energyCharts && window._energyCharts[el.id]) _scaleChartEl(el, true);
  }});
  // Pass 2: Plotly relayouts — debounced, runs after resize animation settles
  clearTimeout(_relayoutTimer);
  _relayoutTimer = setTimeout(function() {{
    document.querySelectorAll('.chart-container').forEach(function(el) {{
      if (window._energyCharts && window._energyCharts[el.id]) _scaleChartEl(el, false);
    }});
  }}, 400);
}}

window.addEventListener('resize', _scaleDayCharts, {{passive: true}});

// ── Deferred chart renderer ──────────────────────────────────
// Charts store data in <script type="application/json"> blocks.
// _renderSection reads the JSON and builds Plotly traces only when
// the section becomes visible — no JS parsing at page load time.
function _buildDayChart(chartId) {{
  var el = document.getElementById(chartId);
  if (!el) return;
  var dataEl = document.getElementById('data_' + chartId);
  if (!dataEl) return;
  var d;
  try {{ d = JSON.parse(dataEl.textContent); }} catch(e) {{ return; }}

  var slots = d.slots;
  var bm    = d.block_minutes;
  var cur   = d.currency;
  var xBar  = Array.from({{length: slots}}, function(_, i) {{ return i; }});
  var xRanges = d.x_ranges;
  var xLabels = d.x_labels;

  var totalHhKwh = Array(slots).fill(0);
  Object.values(d.meters).forEach(function(m) {{
    if (!m.is_export) m.y.forEach(function(v, i) {{ totalHhKwh[i] += v; }});
  }});

  var traces = [];
  Object.entries(d.meters).forEach(function(entry) {{
    var mid = entry[0]; var m = entry[1];
    var useArea = bm < 15;
    var customdata = m.y.map(function(v, i) {{
      return [xRanges[i], totalHhKwh[i], Math.abs(v)];
    }});
    var hoverTotal = m.is_export ? '' : ' (%{{customdata[1]:.3f}} total)';
    var trace = {{
      x: xBar, y: m.y,
      type: useArea ? 'scatter' : 'bar',
      name: m.nice_name,
      customdata: customdata,
      hovertemplate: m.nice_name + '<br>%{{customdata[0]}}<br>%{{customdata[2]:.3f}} kWh' + hoverTotal + '<extra></extra>'
    }};
    if (useArea) {{
      trace.mode = 'lines'; trace.fill = 'tozeroy';
      trace.line = {{shape: 'hv', color: m.bar_color}};
    }} else {{
      trace.width = 0.7; trace.marker = {{color: m.bar_color}};
    }}
    traces.push(trace);
    if (m.has_rate) {{
      traces.push({{
        x: m.rate_x, y: m.rate,
        type: 'scatter', mode: 'lines',
        line: {{shape: 'hv', width: 2, color: m.line_color, dash: m.is_export ? 'dash' : 'solid'}},
        name: m.nice_name + ' rate', yaxis: 'y2',
        customdata: xRanges.concat([xRanges[xRanges.length - 1]]),
        hovertemplate: m.nice_name + ' rate<br>%{{customdata}}<br>' + cur + '%{{y:.4f}}<extra></extra>'
      }});
    }}
  }});

  var tickStep = Math.max(1, Math.round(30 / bm));
  var tickVals  = xBar.filter(function(_, i) {{ return i % tickStep === 0; }});
  var tickTexts = xLabels.filter(function(_, i) {{ return i % tickStep === 0; }});
  var tc = _getThemeColours();
  var layout = {{
    autosize: true, barmode: 'relative',
    margin: {{l: 46, r: 52, t: 16, b: 80}},
    plot_bgcolor: tc.plotBg, paper_bgcolor: tc.paperBg,
    xaxis: {{tickmode:'array', tickvals:tickVals, ticktext:tickTexts, tickangle:-45, showgrid:false}},
    yaxis:  {{title:'kWh', showgrid:true, gridcolor:tc.gridC, titlefont:{{size:11,color:tc.axisC}}, tickfont:{{color:tc.axisC}}}},
    yaxis2: {{title:cur+'/kWh', overlaying:'y', side:'right', showgrid:false, titlefont:{{size:11,color:tc.axisC}}, tickfont:{{color:tc.axisC}}}},
    legend: {{orientation:'h', x:0.5, xanchor:'center', y:-0.28, yanchor:'top', font:{{size:11,color:tc.axisC}}}}
  }};
  // Build to the CONTAINER's own (reserved) height — not the whole wrap. The wrap
  // also includes the full-width data tables below the chart, so measuring it made
  // the built chart overshoot AND, because the empty container had no reserved
  // height, the wrap grew when the chart appeared — the scroll-jump on lazy build.
  // The container carries min-height:320, so this is a stable 320 before and after.
  var elH = el.offsetHeight;
  layout.height = Math.max(elH > 0 ? elH : 0, 320);

  function _alignY2() {{
    var y1range = el._fullLayout.yaxis.range;
    var y2max = el._fullLayout.yaxis2._range ? el._fullLayout.yaxis2._range[1] : el._fullLayout.yaxis2.range[1];
    if (y2max <= 0) return;
    var hasImport = traces.some(function(t) {{ return t.yaxis !== 'y2' && t.y && t.y.some(function(v) {{ return v > 0.001; }}); }});
    var hasExport = traces.some(function(t) {{ return t.yaxis !== 'y2' && t.y && t.y.some(function(v) {{ return v < -0.001; }}); }});
    var y1min = y1range[0]; var y1top, y2min;
    var rawStep = y2max / 4;
    var mag  = Math.pow(10, Math.floor(Math.log10(rawStep)));
    var step = [1, 2, 2.5, 5, 10].map(function(f) {{ return f * mag; }}).find(function(s) {{ return s >= rawStep; }}) || mag;
    var ticks = [];
    for (var t = 0; t <= y2max + step * 0.01; t += step) ticks.push(parseFloat(t.toFixed(10)));
    if (hasExport && !hasImport) {{
      var exportDepth = -y1min; y1top = exportDepth * 0.5;
      var frac = exportDepth / (exportDepth + y1top);
      y2min = -frac * (y2max / (1 - frac));
    }} else if (hasImport && hasExport) {{
      y1top = y1range[1];
      var negFrac = -y1min / (y1range[1] - y1min);
      y2min = -negFrac * (y2max / (1 - negFrac));
    }} else {{
      // Import-only day (no export to pull the axes below zero). Normally the rate
      // axis floors at 0 — but an Agile plunge-price slot has a NEGATIVE rate, which
      // then clips flat at the baseline. Give the rate axis room below zero for the
      // lowest negative rate, and give the energy axis a matching zero-fraction so
      // the two zero-lines stay aligned (same trick as the export branches).
      y1top = y1range[1];
      var rateMin = 0;
      traces.forEach(function(t) {{
        if (t.yaxis === 'y2' && t.y) t.y.forEach(function(v) {{ if (v < rateMin) rateMin = v; }});
      }});
      if (rateMin < 0) {{
        y2min = rateMin - (-rateMin) * 0.08;          // small headroom below the trough
        var f = -y2min / (y2max - y2min);             // zero fraction from the bottom
        y1min = -(f / (1 - f)) * y1top;               // matching negative headroom on y1
      }} else {{
        y2min = 0;
      }}
    }}
    // Label the below-zero region of the rate axis when it dips negative (plunge
    // price / export), so the line has readable ticks rather than an unlabelled tail.
    if (y2min < 0) {{
      for (var tn = -step; tn >= y2min - step * 0.01; tn -= step) ticks.unshift(parseFloat(tn.toFixed(10)));
    }}
    Plotly.relayout(el, {{
      'yaxis.range': [y1min, y1top], 'yaxis2.range': [y2min, y2max],
      'yaxis2.tickmode': 'array', 'yaxis2.tickvals': ticks,
      'yaxis2.ticktext': ticks.map(function(v) {{ return v.toFixed(2); }})
    }});
  }}

  Plotly.newPlot(el, traces, layout, {{responsive:true, displayModeBar:false}}).then(function() {{
    _alignY2();
    el.on('plotly_restyle', function() {{ setTimeout(_alignY2, 50); }});
  }});
  if (!window._energyCharts) window._energyCharts = {{}};
  window._energyCharts[chartId] = el;
  _scaleChartEl(el);
}}

// One shared observer builds each day chart only as it scrolls near the
// viewport, so revealing a year (~365 charts) never renders more than the few
// on screen. The callback ignores anything in a section that's now hidden, so a
// stray event that fires just after a mode switch is a no-op. There are no
// background workers: each chart is one synchronous Plotly build on its own
// intersection callback, and switching section disconnects the observer (below),
// dropping everything still queued for the section you left.
var _chartObserver = null;
function _ensureChartObserver() {{
  if (_chartObserver || typeof IntersectionObserver === 'undefined') return _chartObserver;
  _chartObserver = new IntersectionObserver(function(entries) {{
    entries.forEach(function(entry) {{
      if (!entry.isIntersecting) return;
      var el = entry.target;
      _chartObserver.unobserve(el);
      var sec = el.closest('.period-section');
      if (!sec || sec.style.display === 'none' || sec.style.visibility === 'hidden') return;   // section left the screen
      var chartId = window._pendingCharts && window._pendingCharts[el.id];
      if (chartId) {{
        delete window._pendingCharts[el.id];
        _buildDayChart(chartId);
      }}
    }});
  }}, {{ rootMargin: '600px 0px' }});   // build a little before they scroll in
  return _chartObserver;
}}

function _renderSection(section) {{
  if (!section || !window._pendingCharts) return;
  var obs = _ensureChartObserver();
  // Stop watching the previous section's charts — anything queued there is
  // dropped, so switching mode mid-render leaves nothing rendering off-screen.
  if (obs) obs.disconnect();
  section.querySelectorAll('.chart-container').forEach(function(el) {{
    // Skip day charts inside a collapsed panel — they get observed when the
    // panel is expanded (see the toggle listener in _revealSection).
    var det = el.closest('details.day-charts-toggle');
    if (det && !det.open) return;
    if (!window._pendingCharts[el.id]) return;   // already built
    if (obs) {{
      obs.observe(el);                            // build when it nears the viewport
    }} else {{
      var chartId = window._pendingCharts[el.id]; // no IntersectionObserver — build now
      delete window._pendingCharts[el.id];
      _buildDayChart(chartId);
    }}
  }});
}}

var _currentMode = 'month';

function setPeriodMode(mode) {{
  if (window.parent) window.parent.postMessage({{ type: 'suppressResize' }}, '*');
  _currentMode = mode;
  sessionStorage.setItem('energyPeriodMode', mode);
  // Toggle dropdowns
  document.getElementById('select-month').style.display    = (mode==='month')    ? '' : 'none';
  document.getElementById('select-calmonth').style.display = (mode==='calmonth') ? '' : 'none';
  document.getElementById('select-quarter').style.display  = (mode==='quarter')  ? '' : 'none';
  document.getElementById('select-year').style.display     = (mode==='year')     ? '' : 'none';
  // Toggle mode buttons
  document.querySelectorAll('.pmode-btn').forEach(function(b) {{
    b.classList.toggle('active', b.dataset.mode === mode);
  }});
  // Hide vs Last Year in year mode
  var vyb = document.querySelector('.vs-year-btn');
  if (vyb) vyb.style.display = (mode==='year') ? 'none' : '';
  // Drop back to vanilla if vs-year was active in year mode
  if (mode==='year') {{
    var sv = sessionStorage.getItem('energyView') || 'vanilla';
    if (sv==='vs-year') showView('vanilla');
  }}
  // Hide all sections, show correct one for mode. Use display:none (NOT
  // visibility:hidden+absolute): an absolutely-positioned hidden section is out
  // of flow but still EXTENDS the scroll height, so a tall built-out year view
  // left a huge dead scroll void under a short month. display:none removes it.
  document.querySelectorAll('.period-section').forEach(function(el) {{
      el.style.display = 'none';
  }});
  var defaults = {{ month: '{default_period}', quarter: '{default_quarter}', year: '{default_year}' }};
  var savedId  = sessionStorage.getItem('energyPeriod_' + mode);
  // Validate saved ID still exists in DOM (blocks update may have changed period count)
  var initial  = (savedId && document.getElementById(savedId)) ? savedId : defaults[mode];
  // If default also missing (shouldn't happen) fall back to first visible section
  if (!document.getElementById(initial)) {{
    var first = document.querySelector('.' + mode + '-section');
    if (first) initial = first.id;
  }}
  sessionStorage.setItem('energyPeriod_' + mode, initial);
  _revealSection(initial);
  var selMap = {{ month:'period-select-month', calmonth:'period-select-calmonth', quarter:'period-select-quarter', year:'period-select-year' }};
  var sel = document.getElementById(selMap[mode]);
  if (sel) sel.value = initial;
  _syncRolodexLabel();
}}

// ── Period rolodex adapter (shared PeriodSpinner) ──────────
// The native <select> per mode is the data source; the rolodex label is the
// visible control. getPeriods reads the active mode's options (already
// newest-first); onSelect drives the existing showPeriod() path.
var _ROLODEX_SEL = {{ month:'period-select-month', calmonth:'period-select-calmonth', quarter:'period-select-quarter', year:'period-select-year' }};
function _rolodexSelect() {{ return document.getElementById(_ROLODEX_SEL[_currentMode]); }}
function _rolodexClean(text) {{ return String(text || '').replace(/^★ Current\\s+/, ''); }}

function _billGetPeriods() {{
  var sel = _rolodexSelect();
  if (!sel) return {{ list: [], curIdx: -1 }};
  var list = [];
  for (var i = 0; i < sel.options.length; i++) {{
    list.push({{ label: _rolodexClean(sel.options[i].text), value: sel.options[i].value }});
  }}
  return {{ list: list, curIdx: sel.selectedIndex }};
}}
function _billSelectPeriod(value) {{
  var sel = _rolodexSelect();
  if (sel) sel.value = value;
  showPeriod(value, _currentMode);   // showPeriod() re-syncs the label
}}
// ‹ / › arrows: step one period within the current mode. Options are
// newest-first, so delta +1 = older, -1 = newer.
function _billStep(delta) {{
  var sel = _rolodexSelect();
  if (!sel || !sel.options.length) return;
  var ni = Math.min(sel.options.length - 1, Math.max(0, sel.selectedIndex + delta));
  if (ni === sel.selectedIndex) return;
  _billSelectPeriod(sel.options[ni].value);
}}
function _syncRolodexLabel() {{
  var lbl = document.getElementById('period-rolodex-label');
  if (!lbl) return;
  var sel = _rolodexSelect();
  if (!sel || sel.selectedIndex < 0) {{ lbl.textContent = ''; return; }}
  var t = _rolodexClean(sel.options[sel.selectedIndex].text);
  var bar = t.indexOf('  |  ');            // drop the cost tail for the collapsed label
  lbl.textContent = (bar >= 0 ? t.slice(0, bar) : t).trim();
}}
function _initRolodex() {{
  var lbl = document.getElementById('period-rolodex-label');
  if (lbl && window.PeriodSpinner) {{
    PeriodSpinner.attach(lbl, {{ getPeriods: _billGetPeriods, onSelect: _billSelectPeriod }});
  }}
  _syncRolodexLabel();
}}

function _revealSection(id) {{
    var section = document.getElementById(id);
    if (section) {{
      section.style.display = '';
      section.style.visibility = 'visible';
      section.style.position = 'relative';
    // Restore day-charts-toggle open/closed state
    section.querySelectorAll('.day-charts-toggle').forEach(function(det) {{
      var key = 'energyChartsOpen_' + det.id;
      var saved = sessionStorage.getItem(key);
      if (saved !== null) {{ det.open = saved === '1'; }}
      if (!det._listenerAdded) {{
        det.addEventListener('toggle', function() {{
          sessionStorage.setItem('energyChartsOpen_' + det.id, det.open ? '1' : '0');
          // Build the (previously skipped) day charts now the panel is open.
          if (det.open) _renderSection(section);
        }});
        det._listenerAdded = true;
      }}
    }});
    // Re-apply current view so bill-view divs inside this section are correct
    var currentView = sessionStorage.getItem('energyView') || 'vanilla';
    section.querySelectorAll('.bill-view').forEach(function(el) {{
      el.style.display = (el.dataset.view === currentView) ? 'block' : 'none';
    }});
    if (window._energyCharts) {{
      section.querySelectorAll('.chart-container').forEach(function(c) {{
        if (window._energyCharts[c.id]) Plotly.relayout(c, {{autosize: true}});
      }});
    }}
    _renderSection(section);
    _attachStickyObserver(section);
  }}
}}

function showPeriod(id, mode) {{
  if (window.parent) window.parent.postMessage({{ type: 'suppressResize' }}, '*');
  if (!mode) mode = _currentMode;
  sessionStorage.setItem('energyPeriod_' + mode, id);
  // Hide all sections belonging to this mode, show only the selected one
  var clsMap = {{ month:'month-section', calmonth:'calmonth-section', quarter:'quarter-section', year:'year-section' }};
  document.querySelectorAll('.' + clsMap[mode]).forEach(function(el) {{
    el.style.display = 'none';
  }});
  document.getElementById('sticky-bill-strip').style.visibility = 'hidden';
  _revealSection(id);
  // A deliberate period change swaps in a section of a completely different
  // length. Reset the scroll to the top so the new charts are visible at once --
  // otherwise, coming from a long year view scrolled to the bottom, the shorter
  // month view shows only blank space until you scroll back up (and the
  // "Show Bill" strip only appears once a chart re-enters view). Also clear the
  // saved scroll so a later reload / tab-return doesn't restore the stale deep
  // position for this shorter section.
  window.scrollTo(0, 0);
  try {{ sessionStorage.setItem('energyScroll', '0'); }} catch (e) {{}}
  _syncRolodexLabel();
}}

// ── Sticky bill strip ─────────────────────────────────────
var _stickyObserver = null;
var _stickySection  = null;

function _getSectionBillLabel(section) {{
  // Try to extract period label + cost from the bill-toggle-summary context
  // and the h2 inside the visible bill-view
  var h2 = section.querySelector('.bill-view-heading h2');
  return h2 ? h2.textContent.trim() : '';
}}

function _getSectionBillCost(section) {{
  // Grab the total cost line from the billing table
  var el = section.querySelector('.bill-total td:last-child');
  return el ? el.textContent.trim() : '';
}}

function _attachStickyObserver(section) {{
  if (_stickyObserver) {{ _stickyObserver.disconnect(); _stickyObserver = null; }}
  _stickySection = section;
  var billToggle = section ? section.querySelector('.bill-toggle') : null;
  if (!billToggle) return;
  var strip = document.getElementById('sticky-bill-strip');
  var label = document.getElementById('sticky-bill-label');
  // Populate label directly from the bill-toggle summary text
  var summary = billToggle.querySelector('.bill-toggle-summary');
  label.textContent = summary ? summary.textContent.trim() : '';
  _stickyObserver = new IntersectionObserver(function(entries) {{
    var entry = entries[0];
    if (!entry.isIntersecting && entry.boundingClientRect.top < 0) {{
      // Bill toggle has scrolled above viewport — show strip and stop observing
      // to prevent oscillation from the nav growing taller
      strip.style.visibility = 'visible';
      _stickyObserver.disconnect();
      _stickyObserver = null;
      // Re-attach a one-shot observer to hide strip when user scrolls back up
      var hideObserver = new IntersectionObserver(function(e2) {{
        if (e2[0].isIntersecting) {{
          strip.style.visibility = 'hidden';
          hideObserver.disconnect();
          // Re-arm the main observer now that bill toggle is visible again
          _attachStickyObserver(_stickySection);
        }}
      }}, {{ threshold: 0 }});
      hideObserver.observe(billToggle);
    }}
  }}, {{ threshold: 0, rootMargin: '0px 0px 0px 0px' }});
  _stickyObserver.observe(billToggle);
}}

function stickyBillExpand() {{
  if (!_stickySection) return;
  // Open the bill toggle if collapsed
  var billToggle = _stickySection.querySelector('.bill-toggle');
  if (billToggle && !billToggle.open) billToggle.open = true;
  // Scroll section into view (below sticky nav)
  var navH = document.querySelector('.period-nav').offsetHeight;
  var top  = _stickySection.getBoundingClientRect().top + window.scrollY - navH - 8;
  window.scrollTo({{ top: top, behavior: 'smooth' }});
}}

function toggleTheme() {{
  var current = document.documentElement.getAttribute('data-theme');
  var next = current === 'light' ? 'dark' : 'light';
  document.documentElement.setAttribute('data-theme', next);
  localStorage.setItem('emt_chart_theme', next);
  var btn = document.getElementById('theme-toggle');
  if (btn) btn.textContent = next === 'dark' ? '\u263e' : '\u2600';
  // Notify parent shell so it syncs all other iframes and shell UI
  if (window.parent && window.parent !== window) {{
    window.parent.postMessage({{type:'emt-theme-change', theme:next}}, '*');
  }}
  // Relayout Plotly charts in this iframe
  var tc = _getThemeColours();
  if (window._energyCharts) {{
    Object.keys(window._energyCharts).forEach(function(id) {{
      var el = document.getElementById(id);
      if (el) {{
        Plotly.relayout(el, {{
          plot_bgcolor: tc.plotBg,
          paper_bgcolor: tc.paperBg,
          'xaxis.tickfont.color': tc.axisC,
          'yaxis.gridcolor': tc.gridC,
          'yaxis.titlefont.color': tc.axisC,
          'yaxis.tickfont.color': tc.axisC,
          'yaxis2.titlefont.color': tc.axisC,
          'yaxis2.tickfont.color': tc.axisC,
          'legend.font.color': tc.axisC,
        }});
      }}
    }});
  }}
}}

function toggleAllTables() {{
  var tables = document.querySelectorAll('.day-data-tables');
  var anyOpen = Array.from(tables).some(function(t) {{ return t.style.display === 'block' || t.style.display === 'flex'; }});
  var btn = document.getElementById('expand-all-btn');
  tables.forEach(function(t) {{
    if (anyOpen) {{
      t.style.display = 'none';
    }} else {{
      if (!t._built) {{
        var chartId = t.id.replace(/^tbl_/, '');
        var dataEl  = document.getElementById('data_' + chartId);
        if (dataEl) _buildTableContent(t, dataEl.textContent);
      }}
      t.style.display = 'flex';
    }}
  }});
  var nowOpen = !anyOpen;
  if (btn) {{
    btn.classList.toggle('active', nowOpen);
    btn.textContent = nowOpen ? 'Hide Data' : 'Show Data';
  }}
}}

function _buildTableContent(wrap, jsonText) {{
  var d; try {{ d = JSON.parse(jsonText); }} catch(e) {{ return; }}
  var cur = d.currency || '\xa3';
  var exportKey = 'electricity_main_export';
  var directKey = 'electricity_main';
  var deviceKeys = Object.keys(d.meters).filter(function(k) {{
    return k !== exportKey && k !== directKey;
  }}).sort();
  var orderedKeys = [exportKey, directKey].concat(deviceKeys).filter(function(k) {{ return d.meters[k]; }});
  var colNames = ['Total Import'].concat(orderedKeys.map(function(k) {{ return d.meters[k].nice_name; }}));
  var numMeters = colNames.length;

  // BL-24 opt-in: ex-VAT columns on IMPORT meters (export is zero-rated → unchanged).
  var br = !!d.bill_rounding;
  var ratio  = d.exc_ratio  || [];
  var approx = d.exc_approx || [];
  var invat  = d.fallback_invat || (1.0 / 1.05); // period statutory 1/(1+VAT) when a slot has no ratio
  // is a given column an import meter? Total Import (index 0) always is.
  function isImp(idx) {{ return idx === 0 ? true : !d.meters[orderedKeys[idx-1]].is_export; }}
  function money(v) {{ return (Math.abs(v) >= 0.000005) ? (cur + v.toFixed(5)) : '—'; }}

  var html = '<div class="day-meter-table"><table><thead>';
  html += '<tr><th rowspan="2">Period</th>';
  colNames.forEach(function(n, idx) {{
    var span = (br && isImp(idx)) ? 4 : 3;
    html += '<th colspan="' + span + '">' + n + '</th>';
  }});
  html += '</tr><tr>';
  for (var c = 0; c < numMeters; c++) {{
    if (br && isImp(c)) {{
      html += '<th>Rate (exc)</th><th>kWh</th><th>' + cur + ' exc</th><th>' + cur + ' inc</th>';
    }} else {{
      html += '<th>Rate</th><th>kWh</th><th>' + cur + '</th>';
    }}
  }}
  html += '</tr></thead><tbody>';

  // Per-column ex-VAT cost accumulators for the footer (import columns only).
  var excTot = new Array(numMeters); for (var e = 0; e < numMeters; e++) excTot[e] = 0;

  for (var i = 0; i < d.slots; i++) {{
    var rt = (ratio[i] == null) ? invat : ratio[i];
    var ap = !!approx[i];
    html += '<tr><td>' + d.x_ranges[i].split(' - ')[0] + '</td>';
    var ti_kwh  = parseFloat(Math.abs(d.ti_kwh  ? (d.ti_kwh[i]  || 0) : 0).toFixed(3));
    var ti_cost = parseFloat(Math.abs(d.ti_cost ? (d.ti_cost[i] || 0) : 0).toFixed(5));
    var ti_rate = d.ti_rate ? (d.ti_rate[i] || 0) : 0;
    if (br) {{
      var tiExc = ti_cost * rt; excTot[0] += tiExc;
      html += '<td>' + (ti_rate ? (cur + (ti_rate * rt).toFixed(4)) : '—') + '</td>';
      html += '<td>' + ti_kwh.toFixed(3) + '</td>';
      html += '<td>' + (ti_cost >= 0.000005 ? ((ap ? '≈' : '') + money(tiExc)) : '—') + '</td>';
      html += '<td>' + (ti_cost >= 0.000005 ? (cur + ti_cost.toFixed(5)) : '—') + '</td>';
    }} else {{
      html += '<td>' + (ti_rate ? (cur + ti_rate.toFixed(4)) : '—') + '</td>';
      html += '<td>' + ti_kwh.toFixed(3) + '</td>';
      html += '<td>' + (ti_cost >= 0.000005 ? (cur + ti_cost.toFixed(5)) : '—') + '</td>';
    }}
    orderedKeys.forEach(function(k, ki) {{
      var m    = d.meters[k];
      var imp  = !m.is_export;
      var kwh  = parseFloat(Math.abs(m.y[i] || 0).toFixed(3));
      var cost = parseFloat(Math.abs((m.cost && m.cost[i]) || 0).toFixed(5));
      var rr   = m.rate;
      var rate = rr[i] !== undefined ? rr[i] : (rr[rr.length-1] || 0);
      if (br && imp) {{
        var exc = cost * rt; excTot[ki + 1] += exc;
        html += '<td>' + (rate ? (cur + (rate * rt).toFixed(4)) : '—') + '</td>';
        html += '<td>' + kwh.toFixed(3) + '</td>';
        html += '<td>' + (cost >= 0.000005 ? ((ap ? '≈' : '') + money(exc)) : '—') + '</td>';
        html += '<td>' + (cost >= 0.000005 ? (cur + cost.toFixed(5)) : '—') + '</td>';
      }} else {{
        html += '<td>' + (rate ? (cur + rate.toFixed(4)) : '—') + '</td>';
        html += '<td>' + kwh.toFixed(3) + '</td>';
        html += '<td>' + (cost >= 0.000005 ? (cur + cost.toFixed(5)) : '—') + '</td>';
      }}
    }});
    html += '</tr>';
  }}

  html += '</tbody><tfoot><tr><td>Total</td>';
  var tot_ti_kwh = 0, tot_ti_cost = 0;
  orderedKeys.forEach(function(k) {{
    var t = (d.meter_totals && d.meter_totals[k]) || {{kwh:0,cost:0}};
    if (!d.meters[k].is_export) {{ tot_ti_kwh += t.kwh; tot_ti_cost += t.cost; }}
  }});
  if (br) {{
    html += '<td></td><td>' + tot_ti_kwh.toFixed(3) + '</td><td>' + cur + excTot[0].toFixed(2) + '</td><td>' + cur + tot_ti_cost.toFixed(2) + '</td>';
  }} else {{
    html += '<td></td><td>' + tot_ti_kwh.toFixed(3) + '</td><td>' + cur + tot_ti_cost.toFixed(2) + '</td>';
  }}
  orderedKeys.forEach(function(k, ki) {{
    var t = (d.meter_totals && d.meter_totals[k]) || {{kwh:0,cost:0}};
    if (br && !d.meters[k].is_export) {{
      html += '<td></td><td>' + t.kwh.toFixed(3) + '</td><td>' + cur + excTot[ki + 1].toFixed(2) + '</td><td>' + cur + t.cost.toFixed(2) + '</td>';
    }} else {{
      html += '<td></td><td>' + t.kwh.toFixed(3) + '</td><td>' + cur + t.cost.toFixed(2) + '</td>';
    }}
  }});
  html += '</tr></tfoot></table></div>';
  wrap.innerHTML = html;
  wrap._built = true;
}}

function toggleDayTables(tableId, btn) {{
  var wrap = document.getElementById(tableId);
  if (!wrap) return;
  var open = wrap.style.display === 'none' || wrap.style.display === '';
  if (open) {{
    if (!wrap._built) {{
      var dataEl = document.getElementById('data_' + tableId.replace(/^tbl_/, ''));
      if (!dataEl) return;
      _buildTableContent(wrap, dataEl.textContent);
    }}
    wrap.style.display = 'flex';
    if (btn) btn.classList.add('active');
  }} else {{
    wrap.style.display = 'none';
    if (btn) btn.classList.remove('active');
  }}
  // Sync the global Show/Hide Data button label
  var allTables = document.querySelectorAll('.day-data-tables');
  var anyOpen = Array.from(allTables).some(function(t) {{ return t.style.display === 'flex' || t.style.display === 'block'; }});
  var globalBtn = document.getElementById('expand-all-btn');
  if (globalBtn) {{
    globalBtn.classList.toggle('active', anyOpen);
    globalBtn.textContent = anyOpen ? 'Hide Data' : 'Show Data';
  }}
}}

function toggleCensor() {{
  var on = document.body.classList.toggle('censor-on');
  var btn = document.getElementById('censor-toggle');
  if (btn) btn.classList.toggle('active', on);
  sessionStorage.setItem('energyCensor', on ? '1' : '0');
}}

function toggleSortOrder() {{
  var asc = localStorage.getItem('emt_bill_sort_asc') === '1';
  var nowAsc = !asc;
  localStorage.setItem('emt_bill_sort_asc', nowAsc ? '1' : '0');

  // Reverse day charts within each period section
  document.querySelectorAll('.day-charts-body').forEach(function(body) {{
    var children = Array.from(body.children);
    children.reverse().forEach(function(c) {{ body.appendChild(c); }});
  }});

  // Update button
  var btn = document.getElementById('sort-toggle');
  if (btn) {{
    btn.classList.toggle('active', nowAsc);
    btn.textContent = nowAsc ? '↑ Oldest first' : '↓ Latest first';
  }}
}}

function showView(view) {{
  sessionStorage.setItem('energyView', view);
  document.querySelectorAll('.view-btn').forEach(function(b) {{
    b.classList.toggle('active', b.dataset.view === view);
  }});
  document.querySelectorAll('.bill-view').forEach(function(el) {{
    el.style.display = (el.dataset.view === view) ? 'block' : 'none';
  }});
}}


(function() {{
  var savedMode = sessionStorage.getItem('energyPeriodMode') || 'month';
  // Restore view
  var savedView = sessionStorage.getItem('energyView') || 'vanilla';
  if (savedMode==='year' && savedView==='vs-year') savedView = 'vanilla';
  showView(savedView);
  // Restore censor
  if (sessionStorage.getItem('energyCensor')==='1') toggleCensor();
  // Restore sort order button state and day chart order
  if (localStorage.getItem('emt_bill_sort_asc') === '1') {{
    document.querySelectorAll('.day-charts-body').forEach(function(body) {{
      var children = Array.from(body.children);
      children.reverse().forEach(function(c) {{ body.appendChild(c); }});
    }});
    var btn = document.getElementById('sort-toggle');
    if (btn) {{ btn.classList.add('active'); btn.textContent = '↑ Oldest first'; }}
  }}
  // Listen for theme changes from parent shell
  window.addEventListener('message', function(e) {{
    if (e.data && e.data.type === 'emt-theme') {{
      document.documentElement.setAttribute('data-theme', e.data.theme);
      var btn = document.getElementById('theme-toggle');
      if (btn) btn.textContent = e.data.theme === 'dark' ? '\u263e' : '\u2600';
      var tc = _getThemeColours();
      if (window._energyCharts) {{
        Object.keys(window._energyCharts).forEach(function(id) {{
          var el = document.getElementById(id);
          if (el) {{
            Plotly.relayout(el, {{
              plot_bgcolor: tc.plotBg, paper_bgcolor: tc.paperBg,
              'xaxis.tickfont.color': tc.axisC, 'yaxis.gridcolor': tc.gridC,
              'yaxis.titlefont.color': tc.axisC, 'yaxis.tickfont.color': tc.axisC,
              'yaxis2.titlefont.color': tc.axisC, 'yaxis2.tickfont.color': tc.axisC,
              'legend.font.color': tc.axisC,
            }});
          }}
        }});
      }}
    }}
  }});
  // Sync toggle button icon to current theme
  var _themeBtn = document.getElementById('theme-toggle');
  if (_themeBtn) _themeBtn.textContent = document.documentElement.getAttribute('data-theme') === 'light' ? '\u2600' : '\u263e';
  // Activate mode
  setPeriodMode(savedMode);
  // Attach the shared rolodex spinner to the period label
  _initRolodex();
  // Render charts in the initially visible section
  var _modeDefaults = {{ month: '{default_period}', calmonth: '{default_calmonth}', quarter: '{default_quarter}', year: '{default_year}' }};
  var _initId = sessionStorage.getItem('energyPeriod_' + savedMode);
  if (!_initId || !document.getElementById(_initId)) _initId = _modeDefaults[savedMode];
  _renderSection(document.getElementById(_initId));
  _attachStickyObserver(document.getElementById(_initId));
  // Restore scroll
  var savedScroll = sessionStorage.getItem('energyScroll');
  if (savedScroll) window.scrollTo(0, parseInt(savedScroll, 10));
  window.addEventListener('scroll', function() {{
    sessionStorage.setItem('energyScroll', window.scrollY.toString());
  }}, {{passive:true}});
}})();
</script>

<script>
(function() {{
  // Listen for resize notifications from the EMT parent page (charts.html).
  // The parent's ResizeObserver detects the sidebar toggle and posts the
  // new available width — we use it to relayout charts without reloading.
  window.addEventListener('message', function(e) {{
    if (!e.data) return;
    if (e.data.type === 'emt-resize') {{
      var w = e.data.width || window.innerWidth;
      if (typeof _scaleDayCharts === 'function') _scaleDayCharts();
      if (typeof scaleChart === 'function') scaleChart(w);
    }}
    if (e.data.type === 'emt-restore-tables' && Array.isArray(e.data.open)) {{
      e.data.open.forEach(function(id) {{
        var wrap = document.getElementById(id);
        if (!wrap) return;
        var btn = document.querySelector('.day-tbl-toggle[onclick*="' + id + '"]');
        var chartId = id.replace(/^tbl_/, '');
        function _tryOpen() {{
          if (wrap.style.display === 'none' || !wrap.style.display) {{
            toggleDayTables(id, btn);
          }}
        }}
        if (!window._pendingCharts || !window._pendingCharts[chartId]) {{
          _tryOpen();
        }} else {{
          var tries = 0;
          var poll = setInterval(function() {{
            if (!window._pendingCharts || !window._pendingCharts[chartId] || ++tries > 40) {{
              clearInterval(poll);
              _tryOpen();
            }}
          }}, 75);
        }}
      }});
    }}
  }});
}})();
</script>
</body>
</html>
"""

    return html





# ─────────────────────────────────────────────────────────────
# Net heatmap
# ─────────────────────────────────────────────────────────────

def generate_net_heatmap(blocks, timezone_name="UTC", block_minutes=None, currency='£'):
    if not blocks:
        return "<html><body><p>No data available</p></body></html>"

    def _f(v):
        try:
            return float(v) if v is not None else 0.0
        except (TypeError, ValueError):
            return 0.0

    try:
        _tz = ZoneInfo(timezone_name)
    except Exception:
        _tz = ZoneInfo("UTC")

    # Use passed block_minutes, or derive from block meter meta, or default to 30
    if block_minutes is None:
        block_minutes = 30
        for b in blocks:
            bm = (((b or {}).get("meters") or {})
                  .get("electricity_main") or {})
            bm = (bm.get("meta") or {}).get("block_minutes")
            if bm:
                block_minutes = int(bm)
                break
    slots = 1440 // block_minutes

    # ───── Build day → slots ─────
    days          = defaultdict(lambda: [0.0] * slots)    # net kWh
    days_carbon   = defaultdict(lambda: [None] * slots)   # gCO₂ per slot (None = no CI data)
    days_intensity = defaultdict(lambda: [None] * slots)  # gCO₂/kWh per slot (None = no CI data or net=0)

    for block in sorted([b for b in blocks if b and b.get("start")], key=lambda b: b["start"]):
        try:
            start    = _parse_block_start(block["start"], _tz)
            day      = start.date().isoformat()
            hh_index = (start.hour * 60 + start.minute) // block_minutes
            totals   = block.get("totals", {}) or {}
            net      = _f(totals.get("import_kwh")) - _f(totals.get("export_kwh"))
            days[day][hh_index] = net

            # Carbon data from main meter block
            for mid, md in (block.get("meters") or {}).items():
                if (md or {}).get("meta", {}).get("sub_meter"):
                    continue
                cg = md.get("carbon_g")
                if cg is None:
                    break
                cg = float(cg)
                days_carbon[day][hh_index] = cg
                # Carbon intensity is the grid's gCO2/kWh at this time — a property
                # of the grid, independent of how much we drew. Prefer the value
                # stored at write time (3.0.0+), which is defined even for a
                # zero-net block (so it no longer leaves an empty cell); fall back
                # to carbon_g/net for pre-3.0.0 blocks that predate the column.
                # Δ5c: intensity resolution lives in the carbon emit — the heatmap plots
                # what carbon.slot_intensity returns rather than deriving it inline.
                _it = _carbon.slot_intensity(cg, md.get("carbon_intensity_g"), net)
                if _it is not None:
                    days_intensity[day][hh_index] = _it
                break
        except Exception:
            continue

    sorted_days    = sorted(days.keys())
    heatmap_data   = [days[d] for d in sorted_days]
    daily_totals   = [sum(row) for row in heatmap_data]

    # Carbon datasets
    carbon_data    = [days_carbon[d]    for d in sorted_days]
    intensity_data = [days_intensity[d] for d in sorted_days]

    # Daily carbon totals (sum of non-None slots)
    daily_carbon_totals = [
        sum(v for v in row if v is not None) if any(v is not None for v in row) else None
        for row in carbon_data
    ]

    # 95th percentile of non-null intensities for scale anchoring
    all_intensities = [v for row in intensity_data for v in row if v is not None]
    if all_intensities:
        all_intensities_sorted = sorted(all_intensities)
        p95_idx = max(0, int(len(all_intensities_sorted) * 0.95) - 1)
        intensity_max = all_intensities_sorted[p95_idx]
        intensity_max = max(intensity_max, 50)  # floor at 50 gCO₂/kWh
    else:
        intensity_max = 300  # fallback if no CI data

    # ───── X axis labels & ranges ─────
    x_labels = []
    x_ranges = []
    for i in range(slots):
        minutes_start = i * block_minutes
        h_s, m_s = divmod(minutes_start, 60)
        minutes_end = minutes_start + block_minutes
        h_e, m_e = divmod(minutes_end % 1440, 60)
        x_labels.append(f"{h_s:02d}:{m_s:02d}")
        x_ranges.append(f"{h_s:02d}:{m_s:02d}–{h_e:02d}:{m_e:02d}")

    # Only show tick labels every 30 minutes
    tick_step  = max(1, 30 // block_minutes)
    x_tickvals = [x_labels[i] for i in range(0, slots, tick_step)]

    # ───── Y axis & customdata ─────
    y_labels     = sorted_days
    y_ticktext   = [str(int(d[8:10])) for d in sorted_days]
    customdata_2d = [[{"date": sorted_days[i], "time": x_ranges[j]}
                      for j in range(slots)] for i in range(len(sorted_days))]

    # ───── Heatmap colour scale ─────
    flat   = [v for row in heatmap_data for v in row]
    minVal = min(flat) if flat else 0
    maxVal = max(flat) if flat else 1
    if maxVal == minVal:
        maxVal += 1

    def make_colorscale(mn, mx):
        """Return a valid 7-stop colorscale with white at zero, handling all-positive or all-negative ranges."""
        wp = max(0.0, min(1.0, (0 - mn) / (mx - mn)))
        if wp <= 0.01:
            return [[0.0, "white"], [0.33, "#ffcc99"], [0.66, "#ff6600"], [1.0, "#cc0000"]]
        elif wp >= 0.99:
            return [[0.0, "#003366"], [0.33, "#0066cc"], [0.66, "#00aa66"], [1.0, "white"]]
        else:
            c1 = round(wp * 0.33, 4)
            c2 = round(wp * 0.66, 4)
            c3 = round(wp, 4)
            c4 = round(wp + (1 - wp) * 0.33, 4)
            c5 = round(wp + (1 - wp) * 0.66, 4)
            return [
                [0.0, "#003366"], [c1, "#0066cc"], [c2, "#00aa66"],
                [c3, "white"],
                [c4, "#ffcc99"], [c5, "#ff6600"], [1.0, "#cc0000"]
            ]

    heatmap_colorscale = make_colorscale(minVal, maxVal)

    # ───── Daily totals colour scale ─────
    tot_min = min(daily_totals) if daily_totals else 0
    tot_max = max(daily_totals) if daily_totals else 1
    if tot_max == tot_min:
        tot_max += 1
    totals_colorscale      = make_colorscale(tot_min, tot_max)
    heatmap_colorscale_dark = make_colorscale(minVal, maxVal)
    totals_colorscale_dark  = make_colorscale(tot_min, tot_max)
    # Replace 'white' with the dark surface colour in dark variants
    def _dark_cs(cs):
        return [[s, c.replace('white', '#1a1d27')] for s, c in cs]
    heatmap_colorscale_dark = _dark_cs(heatmap_colorscale_dark)
    totals_colorscale_dark  = _dark_cs(totals_colorscale_dark)

    # ───── Weekend overlay data ─────
    shapes = []
    weekend_z = []
    for day_str in sorted_days:
        dow = datetime.fromisoformat(day_str).weekday()
        weekend_z.append([1.0] * slots if dow >= 5 else [None] * slots)

    for idx, day_str in enumerate(sorted_days):
        dow = datetime.fromisoformat(day_str).weekday()
        if dow >= 5:
            shapes.append({
                "type": "rect",
                "xref": "paper", "yref": "y",
                "x0": 0.86, "x1": 1.0,
                "y0": day_str, "y1": day_str,
                "y0shift": -0.5, "y1shift": 0.5,
                "fillcolor": "__WEEKEND_FILL__",
                "line": {"width": 0},
                "layer": "below"
            })

    # ───── Month separators & labels ─────
    month_starts, month_ends, month_labels = [], [], []
    prev_month, start_idx = None, 0
    for idx, day_str in enumerate(sorted_days):
        month = day_str[:7]
        if prev_month is None:
            prev_month, start_idx = month, idx
        elif month != prev_month:
            shapes.append({"type":"line","x0":0,"x1":1,"xref":"paper",
                           "y0":idx-0.5,"y1":idx-0.5,"yref":"y",
                           "line":{"color":"#444","width":1,"dash":"dot"}})
            month_starts.append(start_idx)
            month_ends.append(idx-1)
            y, m = prev_month.split("-")
            month_labels.append(datetime(int(y), int(m), 1).strftime("%b %Y"))
            prev_month, start_idx = month, idx
    if prev_month is not None:
        shapes.append({"type":"line","x0":0,"x1":1,"xref":"paper",
                       "y0":len(sorted_days)-0.5,"y1":len(sorted_days)-0.5,"yref":"y",
                       "line":{"color":"#444","width":1,"dash":"dot"}})
        month_starts.append(start_idx)
        month_ends.append(len(sorted_days)-1)
        y, m = prev_month.split("-")
        month_labels.append(datetime(int(y), int(m), 1).strftime("%b %Y"))

    annotations = [
        {"x":-0.03,"y":(month_starts[i]+month_ends[i])/2,
         "xref":"paper","yref":"y","text":month_labels[i],
         "showarrow":False,"xanchor":"right","yanchor":"middle",
         "textangle":270,"font":{"size":12,"color":"#6b7080"}}
        for i in range(len(month_labels))
    ]

    # ───── Sizing ─────
    visible_rows   = 366  # effectively unlimited — JS caps to viewport height dynamically
    row_height     = 20
    col_width      = 20 * block_minutes // 30   # 20px@30min, 10px@15min, 3px@5min
    n_cols         = slots
    n_rows         = len(sorted_days)
    margin_l, margin_r, margin_t, margin_b = 80, 60, 120, 50
    plot_area_w    = int(n_cols * col_width / 0.85)
    heatmap_width  = margin_l + plot_area_w + margin_r
    heatmap_height = n_rows * row_height + margin_t + margin_b
    div_height     = min(n_rows, visible_rows) * row_height + margin_t + margin_b

    # ───── JSON ─────
    z_json           = json.dumps(heatmap_data)
    x_json           = json.dumps(x_labels)
    y_json           = json.dumps(y_labels)
    y_ticktext_json  = json.dumps(y_ticktext)
    totals_json      = json.dumps(daily_totals)
    shapes_json      = json.dumps(shapes)
    annotations_json = json.dumps(annotations)
    customdata_json  = json.dumps(customdata_2d)
    heatmap_cs_json  = json.dumps(heatmap_colorscale)
    totals_cs_json       = json.dumps(totals_colorscale)
    heatmap_cs_dark_json = json.dumps(heatmap_colorscale_dark)
    totals_cs_dark_json  = json.dumps(totals_colorscale_dark)
    weekend_z_json   = json.dumps(weekend_z)
    x_tickvals_json  = json.dumps(x_tickvals)
    # Carbon datasets
    carbon_z_json        = json.dumps(carbon_data)
    intensity_z_json     = json.dumps(intensity_data)
    daily_carbon_json    = json.dumps(daily_carbon_totals)
    intensity_max_json   = json.dumps(intensity_max)
    # Totals min/max for each metric (needed for per-metric colorscale anchoring)
    kwh_tot_min_json     = json.dumps(tot_min)
    kwh_tot_max_json     = json.dumps(tot_max)
    # Carbon totals min/max
    _dc_flat = [v for v in daily_carbon_totals if v is not None]
    co2_tot_min = min(_dc_flat) if _dc_flat else -1
    co2_tot_max = max(_dc_flat) if _dc_flat else 1
    if co2_tot_min == co2_tot_max: co2_tot_max += 1
    co2_tot_min_json = json.dumps(co2_tot_min)
    co2_tot_max_json = json.dumps(co2_tot_max)

    return f"""<html data-theme="light">
<head>
<meta name="viewport" content="width=device-width, initial-scale=1, maximum-scale=1, user-scalable=no">
<script>
(function(){{
  var stored = localStorage.getItem('emt_chart_theme');
  var sys = window.matchMedia('(prefers-color-scheme: light)').matches ? 'light' : 'dark';
  document.documentElement.setAttribute('data-theme', stored || sys);
}})();
function _getThemeColours() {{
  var dark = document.documentElement.getAttribute('data-theme') !== 'light';
  return {{
    plotBg:  dark ? '#1a1d27' : '#f8f9fa',
    paperBg: dark ? '#1a1d27' : '#ffffff',
    axisC:   dark ? '#6b7080' : '#555566',
    gridC:   dark ? '#2a2d3a' : '#e5e5e5',
  }};
}}
</script>
<script src="https://cdn.plot.ly/plotly-3.0.1.min.js"></script>
</head>
<style>
  :root {{
    --bg: #f0f0f0; --surface: #ffffff; --border: #dddddd;
    --text: #1a1a2e; --muted: #555566;
    --scroll-guard-bg: rgba(0,0,0,0.03);
    --scroll-guard-pill: rgba(0,0,0,0.15);
  }}
  [data-theme="dark"] {{
    --bg: #0f1117; --surface: #1a1d27; --border: #2a2d3a;
    --text: #e8eaf0; --muted: #6b7080;
    --scroll-guard-bg: rgba(255,255,255,0.03);
    --scroll-guard-pill: rgba(255,255,255,0.15);
  }}
  html {{ scroll-padding-top: 80px; }}
html, body {{ margin:0; padding:0; overflow:hidden; touch-action: none; background:var(--bg); color:var(--text); height:100%;
  font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
  font-size: 13px; }}
  #hm-nav {{
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 8px;
    background: var(--surface);
    padding: 8px 12px;
    border-radius: 0 0 8px 8px;
    box-shadow: 0 2px 8px rgba(0,0,0,0.4);
    margin-bottom: 8px;
    position: sticky;
    top: 0;
    z-index: 200;
    box-sizing: border-box;
  }}
  #hm-nav-left {{ display:flex; align-items:center; gap:4px; }}
  #hm-nav-right {{ display:flex; align-items:center; gap:4px; }}
  .hm-metric-btn {{
    font-size: 12px;
    padding: 4px 10px;
    border: 1px solid var(--border);
    border-radius: 5px;
    background: transparent;
    color: var(--muted);
    cursor: pointer;
    transition: all 0.15s;
  }}
  @media (max-width: 600px) {{
    .hm-metric-btn {{
      font-size: 14px;
      padding: 8px 14px;
      min-width: 64px;
      min-height: 40px;
    }}
  }}
  .hm-metric-btn.active {{
    background: rgba(0,212,170,0.15);
    color: var(--accent, #00d4aa);
    border-color: var(--accent, #00d4aa);
  }}
  .hm-theme-btn {{
    font-size: 13px;
    padding: 3px 8px;
    border: 1px solid var(--border);
    border-radius: 5px;
    background: transparent;
    color: var(--muted);
    cursor: pointer;
    opacity: 0.85;
  }}
  #outer {{ width:{heatmap_width}px; transform-origin: top left; position: relative; min-height: 100vh; }}
  #scroll {{
    width:{heatmap_width}px;
    height:auto;
    overflow-y:scroll;
    overflow-x:hidden;
    border:1px solid var(--border);
    position:relative;
    scrollbar-width:thin;
    touch-action: pan-y;
    -webkit-overflow-scrolling: touch;
  }}
  #scroll-guard {{
    position: fixed;
    top: 0; left: 0;
    width: 40px;
    height: 100%;
    z-index: 100;
    touch-action: pan-y;
    background: transparent;
    display: none;
    align-items: center;
    justify-content: center;
  }}
  #scroll-guard.visible {{
    display: flex;
  }}
  #scroll-guard::before {{
    content: '';
    display: block;
    width: 3px;
    height: 48px;
    background: var(--scroll-guard-pill);
    border-radius: 2px;
    opacity: 0.5;
  }}
</style>
<body>
<div id="outer">
  <div id="scroll">
    <div id="hm-nav">
      <div id="hm-nav-left"><!-- metric buttons injected by JS --></div>
      <div id="hm-nav-right"></div>
    </div>
    <div id="heatmap" style="width:{heatmap_width}px;height:{heatmap_height}px;"></div>
  </div>
  <div id="scroll-guard"></div>
</div>
<script>
function scaleChart(overrideW) {{
  var vw = overrideW || window.innerWidth;
  var vh = window.innerHeight;
  var cw = {heatmap_width};
  var isMobile = vw <= 768 || (vh <= 500 && vw > vh);
  var outer  = document.getElementById('outer');
  var scroll = document.getElementById('scroll');
  var guard  = document.getElementById('scroll-guard');
  // Show scroll-grab strip only on mobile
  if (guard) guard.classList.toggle('visible', isMobile);
  var guardW = 0; // guard now on left over y-axis labels, doesn't reduce chart width
  var availW = vw - guardW;
  var nav = document.getElementById('hm-nav');
  if (availW < cw) {{
    var scale = availW / cw;
    outer.style.transform = 'scale(' + scale + ')';
    outer.style.transformOrigin = 'top left';
    outer.style.width = cw + 'px';
    // nav is inside #scroll, scaled via #outer transform — no separate scaling needed
    if (isMobile) {{
      // After scaling, visual height = layout height * scale.
      // To make the chart fill vh visually, set layout height = vh / scale.
      var targetH = Math.ceil(vh / scale);
      outer.style.height = targetH + 'px';
      scroll.style.height = targetH + 'px';
    }} else {{
      // Desktop scaling (browser zoom). #outer is scaled down by `scale` so its
      // visual height = layout height * scale. Set scroll to fill the iframe by
      // using the same targetH approach as mobile — layout height = vh / scale,
      // capped at actual content height to avoid overscroll.
      var scrollH = {n_rows} * {row_height} + {margin_t} + {margin_b};
      var targetH = Math.ceil(vh / scale);
      var clampedH = Math.min(targetH, scrollH);
      outer.style.height = clampedH + 'px';
      scroll.style.height = clampedH + 'px';
    }}
  }} else {{
    outer.style.transform = '';
    outer.style.transformOrigin = '';
    outer.style.width = '';
    outer.style.height = '';
    var scrollH = {n_rows} * {row_height} + {margin_t} + {margin_b};
    var maxH = Math.floor((window.innerHeight - 120) / {row_height}) * {row_height} + {margin_t} + {margin_b};
    scroll.style.height = Math.min(scrollH, maxH) + 'px';
  }}
  // Never relayout Plotly height — cells must stay square at their natural row_height
}}
// Prevent pinch-zoom on the chart — Plotly intercepts touches and can trigger browser zoom
document.addEventListener('touchstart', function(e) {{
  if (e.touches.length > 1) {{ e.preventDefault(); }}
}}, {{ passive: false }});
document.addEventListener('touchmove', function(e) {{
  if (e.touches.length > 1) {{ e.preventDefault(); }}
}}, {{ passive: false }});
window.addEventListener('resize', scaleChart);
// Don't call scaleChart() synchronously — window.innerWidth may be 0 or stale
// if the iframe hasn't been assigned its final dimensions yet. Instead poll
// with rAF until we get a stable non-zero width, then scale.
(function _waitForWidth(attempts) {{
  var vw = window.innerWidth;
  if (vw > 0) {{
    scaleChart();
  }} else if (attempts < 20) {{
    requestAnimationFrame(function() {{ _waitForWidth(attempts + 1); }});
  }}
}})(0);
</script>
<script>
function _hmGetTheme() {{
  var dark = document.documentElement.getAttribute('data-theme') !== 'light';
  return {{
    plotBg:  dark ? '#1a1d27' : '#f8f9fa',
    paperBg: dark ? '#0f1117' : '#ffffff',
    textC:   dark ? '#e8eaf0' : '#1a1a2e',
    axisC:   dark ? '#6b7080' : '#555566',
    monthC:  dark ? '#6b7080' : '#555566',
  }};
}}
var _hmTc = _hmGetTheme();
var _hmCsLight = {heatmap_cs_json};
var _hmCsDark  = {heatmap_cs_dark_json};
var _hmTotCsLight = {totals_cs_json};
var _hmTotCsDark  = {totals_cs_dark_json};
function _hmGetCs()    {{ return document.documentElement.getAttribute('data-theme') !== 'light' ? _hmCsDark    : _hmCsLight; }}
function _hmGetTotCs() {{ return document.documentElement.getAttribute('data-theme') !== 'light' ? _hmTotCsDark : _hmTotCsLight; }}
var _hmShapesRaw = {shapes_json};
function _hmThemedShapes() {{
  var dark = document.documentElement.getAttribute('data-theme') !== 'light';
  var fill = dark ? 'rgba(0,0,0,0.15)' : 'rgba(0,0,0,0.10)';
  return _hmShapesRaw.map(function(s) {{
    return s.fillcolor === '__WEEKEND_FILL__' ? Object.assign({{}}, s, {{fillcolor: fill}}) : s;
  }});
}}
var _hmShapes = _hmThemedShapes();
function _hmWeekendCs() {{
  return document.documentElement.getAttribute('data-theme') !== 'light'
    ? [[0,'rgba(0,0,0,0)'],[1,'rgba(0,0,0,0.15)']]
    : [[0,'rgba(0,0,0,0)'],[1,'rgba(0,0,0,0.10)']];
}}
// ── Carbon / intensity datasets ──
var CARBON_Z      = {carbon_z_json};
var INTENSITY_Z   = {intensity_z_json};
var DAILY_CARBON  = {daily_carbon_json};
var INTENSITY_MAX = {intensity_max_json};
var KWH_Z         = {z_json};
var KWH_TOTALS    = {totals_json};
var KWH_TOT_MIN   = {kwh_tot_min_json};
var KWH_TOT_MAX   = {kwh_tot_max_json};
var CO2_TOT_MIN   = {co2_tot_min_json};
var CO2_TOT_MAX   = {co2_tot_max_json};

// ── Metric state ──
var _hmMetric = localStorage.getItem('emt_hm_metric') || 'kwh';  // 'kwh' | 'co2' | 'intensity'

function _hmHasCo2() {{
  return CARBON_Z.some(function(row) {{ return row.some(function(v) {{ return v !== null; }}); }});
}}

// ── Colorscale builders ──
function _hmCo2Cs(dark) {{
  // gCO₂: green (offset) → white (zero) → red (emitting)
  var bg = dark ? '#1a1d27' : 'white';
  var flat = CARBON_Z.reduce(function(a,r){{return a.concat(r);}}, []).filter(function(v){{return v!==null;}});
  if (!flat.length) return [[0,'#1a6632'],[0.5, bg],[1,'#cc0000']];
  var mn = Math.min.apply(null, flat), mx = Math.max.apply(null, flat);
  if (mn >= 0) return [[0, bg],[0.5,'#ff9966'],[1,'#cc0000']];
  if (mx <= 0) return [[0,'#1a6632'],[0.5,'#66bb88'],[1, bg]];
  var wp = Math.max(0.01, Math.min(0.99, (0 - mn) / (mx - mn)));
  return [
    [0, '#1a6632'], [round2(wp*0.5), '#66bb88'], [round2(wp), bg],
    [round2(wp + (1-wp)*0.5), '#ff9966'], [1, '#cc0000']
  ];
}}
function _hmIntensityCs(dark) {{
  // gCO₂/kWh: green (clean) → yellow → red (dirty)
  return [[0,'#1a6632'],[0.25,'#52b788'],[0.5,'#f9c74f'],[0.75,'#f3722c'],[1,'#cc0000']];
}}
function round2(v) {{ return Math.round(v * 100) / 100; }}

// ── Active dataset getters ──
function _hmGetZ() {{
  if (_hmMetric === 'co2')       return CARBON_Z;
  if (_hmMetric === 'intensity') return INTENSITY_Z;
  return KWH_Z;
}}
function _hmGetTotals() {{
  if (_hmMetric === 'co2')       return DAILY_CARBON;
  if (_hmMetric === 'intensity') return null;
  return KWH_TOTALS;
}}
function _hmGetZmin() {{
  if (_hmMetric === 'intensity') return 0;
  var flat = _hmGetZ().reduce(function(a,r){{return a.concat(r);}}, []).filter(function(v){{return v!==null;}});
  return flat.length ? Math.min.apply(null, flat) : 0;
}}
function _hmGetZmax() {{
  if (_hmMetric === 'intensity') return INTENSITY_MAX;
  var flat = _hmGetZ().reduce(function(a,r){{return a.concat(r);}}, []).filter(function(v){{return v!==null;}});
  return flat.length ? Math.max.apply(null, flat) : 1;
}}
function _hmGetActiveCs() {{
  var dark = document.documentElement.getAttribute('data-theme') !== 'light';
  if (_hmMetric === 'co2')       return _hmCo2Cs(dark);
  if (_hmMetric === 'intensity') return _hmIntensityCs(dark);
  return _hmGetCs();  // existing kWh colorscale
}}
function _hmGetHoverSuffix() {{
  if (_hmMetric === 'co2')       return ' gCO₂';
  if (_hmMetric === 'intensity') return ' gCO₂/kWh';
  return ' kWh';
}}
function _hmGetTotLabel() {{
  if (_hmMetric === 'co2')       return 'Daily CO₂';
  if (_hmMetric === 'intensity') return '';
  return 'Daily Total';
}}

function _hmGetTotMin() {{
  if (_hmMetric === 'co2') return CO2_TOT_MIN;
  return KWH_TOT_MIN;
}}

function _hmGetTotMax() {{
  if (_hmMetric === 'co2') return CO2_TOT_MAX;
  return KWH_TOT_MAX;
}}

function _hmGetTotColorscale() {{
  // For kWh: use the pre-computed totals colorscale (different scale from heatmap)
  // For CO2: use the co2 colorscale anchored to daily totals range
  var dark = document.documentElement.getAttribute('data-theme') !== 'light';
  if (_hmMetric === 'co2') return _hmCo2Cs(dark);
  return _hmGetTotCs();  // existing kWh totals colorscale
}}

function _hmGetChartTitle() {{
  if (_hmMetric === 'co2')       return 'Carbon Emitted (gCO₂)';
  if (_hmMetric === 'intensity') return 'Grid Carbon Intensity (gCO₂/kWh)';
  return 'Net Energy (kWh)';
}}
function _hmGetChartSubtitle() {{
  if (_hmMetric === 'co2')       return 'The amount of carbon your usage produced — scales with how much you use';
  if (_hmMetric === 'intensity') return 'The rate — how clean the grid was per unit, regardless of your usage';
  return 'Energy imported minus exported, each half-hour';
}}

function _hmApplyMetric() {{
  var dark     = document.documentElement.getAttribute('data-theme') !== 'light';
  var tc       = _hmGetTheme();
  var z        = _hmGetZ();
  var totals   = _hmGetTotals();
  var cs       = _hmGetActiveCs();
  var suffix   = _hmGetHoverSuffix();
  var totLabel = _hmGetTotLabel();

  // Compute zmin/zmax from non-null values only
  var flat = z.reduce(function(a,r){{return a.concat(r);}}, []).filter(function(v){{return v!==null;}});
  var zmin = flat.length ? Math.min.apply(null, flat) : -1;
  var zmax = flat.length ? Math.max.apply(null, flat) : 1;
  if (_hmMetric === 'intensity') {{
    zmin = 0;
    zmax = INTENSITY_MAX;
  }} else {{
    // Ensure range spans zero for symmetric colorscale
    if (zmin >= 0) zmin = -0.001;  // force zero anchor
    if (zmax <= 0) zmax =  0.001;
    if (zmin === zmax) zmax = zmin + 1;
  }}

  // Rebuild trace 0 (heatmap) with new data
  var newData = [
    {{
      z: z,
      x: {x_json},
      y: {y_json},
      customdata: {customdata_json},
      type: 'heatmap',
      colorscale: cs,
      zmin: zmin, zmax: zmax, zmid: _hmMetric === 'intensity' ? undefined : 0,
      xgap: 1, ygap: 1, showscale: false,
      hovertemplate: 'Date: %{{customdata.date}}<br>Time: %{{customdata.time}}<br>' + suffix.trim() + ': %{{z:.1f}}' + suffix + '<extra></extra>'
    }},
    totals && _hmMetric !== 'intensity' ? {{
      x: totals,
      y: {y_json},
      type: 'bar', xaxis: 'x2', orientation: 'h',
      visible: true,
      marker: {{
        color: totals,
        colorscale: _hmGetTotColorscale(),
        cmin: _hmGetTotMin(), cmax: _hmGetTotMax(), cmid: 0
      }},
      hovertemplate: totLabel + ': %{{x:.1f}}' + suffix + '<extra></extra>'
    }} : {{
      x: KWH_TOTALS, y: {y_json},
      type: 'bar', xaxis: 'x2', orientation: 'h',
      visible: false,
      marker: {{color: KWH_TOTALS, colorscale: _hmGetTotCs(),
               cmin: KWH_TOT_MIN, cmax: KWH_TOT_MAX, cmid: 0}},
      hovertemplate: ''
    }},
    {{
      z: {weekend_z_json},
      x: {x_json},
      y: {y_json},
      type: 'heatmap',
      colorscale: _hmWeekendCs(),
      zmin: 0, zmax: 1,
      showscale: false,
      hoverinfo: 'skip'
    }}
  ];

  // Use Plotly.react for a clean full-data update (avoids restyle quirks)
  // Use live layout from the DOM so annotations/shapes are preserved
  var _liveLayout = document.getElementById('heatmap').layout || layout;
  // Update title to match current metric
  _liveLayout.title = _liveLayout.title || {{}};
  _liveLayout.title.text = _hmGetChartTitle();
  _liveLayout.title.subtitle = {{text: _hmGetChartSubtitle(), font: {{color: _hmTc.axisC, size: 12}}}};
  _liveLayout.title.automargin = true;
  Plotly.react('heatmap', newData, _liveLayout).then(function() {{
    if (totals && _hmMetric !== 'intensity') {{
      Plotly.relayout('heatmap', {{'xaxis2.title.text': totLabel}});
    }} else {{
      Plotly.relayout('heatmap', {{'xaxis2.title.text': ''}});
    }}
  }});

  // Update metric toggle button states via CSS class
  ['kwh','co2','intensity'].forEach(function(m) {{
    var btn = document.getElementById('hm-metric-' + m);
    if (btn) {{
      btn.classList.toggle('active', _hmMetric === m);
    }}
  }});
}}

function _hmSetMetric(m) {{
  _hmMetric = m;
  localStorage.setItem('emt_hm_metric', m);
  _hmApplyMetric();
}}

var data = [
{{
  z: {z_json},
  x: {x_json},
  y: {y_json},
  customdata: {customdata_json},
  type: 'heatmap',
  colorscale: _hmGetCs(),
  zmin: {minVal}, zmax: {maxVal}, zmid: 0,
  xgap: 1, ygap: 1, showscale: false,
  hovertemplate: 'Date: %{{customdata.date}}<br>Time: %{{customdata.time}}<br>Net: %{{z:.3f}} kWh<extra></extra>'
}},
{{
  x: {totals_json},
  y: {y_json},
  type: 'bar', xaxis: 'x2', orientation: 'h',
  marker: {{
    color: {totals_json},
    colorscale: _hmGetTotCs(),
    cmin: {tot_min}, cmax: {tot_max}, cmid: 0
  }},
  hovertemplate: 'Total: %{{x:.3f}} kWh<extra></extra>'
}},
{{
  z: {weekend_z_json},
  x: {x_json},
  y: {y_json},
  type: 'heatmap',
  colorscale: _hmWeekendCs(),
  zmin: 0, zmax: 1,
  showscale: false,
  hoverinfo: 'skip'
}}
];
var layout = {{
  title: {{text: _hmGetChartTitle(), subtitle: {{text: _hmGetChartSubtitle(), font: {{color: _hmTc.axisC, size: 12}}}}, x: 0.5, automargin: true, font: {{color: _hmTc.textC}}}},
  xaxis:  {{tickangle: -45, side: 'top', domain: [0, 0.85], tickmode: 'array', tickvals: {x_tickvals_json}, ticktext: {x_tickvals_json}, tickfont: {{color: _hmTc.axisC}}, fixedrange: true}},
  xaxis2: {{title: {{text: 'Daily Total', standoff: 10, font: {{color: _hmTc.axisC}}}}, side: 'top', domain: [0.86, 1], tickfont: {{color: _hmTc.axisC}}, fixedrange: true}},
  yaxis:  {{type: 'category', tickmode: 'array', tickvals: {y_json}, ticktext: {y_ticktext_json}, fixedrange: true, tickfont: {{color: _hmTc.axisC}}}},
  shapes: _hmShapes,
  annotations: {annotations_json},
  height: {heatmap_height},
  width: {heatmap_width},
  dragmode: false,
  margin: {{l: {margin_l}, r: {margin_r}, t: {margin_t}, b: {margin_b}}},
  plot_bgcolor: _hmTc.plotBg,
  paper_bgcolor: _hmTc.paperBg
}};

// Update month annotation colours to match theme
var _annotations = {annotations_json};
_annotations.forEach(function(a) {{ if (a.font) a.font.color = _hmTc.monthC; }});
layout.annotations = _annotations;

// ── Populate sticky nav bar ──
var _hmMetricTitles = {{
  'kwh':       'Net kWh per slot (import − export)',
  'co2':       'Net carbon per slot — red=emitting, green=offsetting',
  'intensity': 'Grid carbon intensity where you had net flow (gCO₂/kWh) — green=clean, red=dirty. White=no grid interaction.'
}};
var _hmNavLeft = document.getElementById('hm-nav-left');
[['kwh','kWh'],['co2','gCO₂'],['intensity','gCO₂/kWh']].forEach(function(pair) {{
  var m = pair[0], label = pair[1];
  var btn = document.createElement('button');
  btn.id = 'hm-metric-' + m;
  btn.className = 'hm-metric-btn' + (m === _hmMetric ? ' active' : '');
  btn.textContent = label;
  btn.title = _hmMetricTitles[m];
  if (!_hmHasCo2() && m !== 'kwh') btn.style.display = 'none';
  btn.onclick = function() {{ _hmSetMetric(m); }};
  if (_hmNavLeft) _hmNavLeft.appendChild(btn);
}});

// Theme sync — button hidden, theme driven by parent page logo click
var _hmNavRight = document.getElementById('hm-nav-right');
var _hmToggleBtn = document.createElement('button');
_hmToggleBtn.id = 'hm-theme-btn';
_hmToggleBtn.className = 'hm-theme-btn';
_hmToggleBtn.style.display = 'none';
if (_hmNavRight) _hmNavRight.appendChild(_hmToggleBtn);
_hmToggleBtn.onclick = function() {{
  var current = document.documentElement.getAttribute('data-theme');
  var next = current === 'light' ? 'dark' : 'light';
  document.documentElement.setAttribute('data-theme', next);
  localStorage.setItem('emt_chart_theme', next);
  _hmToggleBtn.textContent = next === 'light' ? '\u2600' : '\u263e';
  if (window.parent && window.parent !== window) {{
    window.parent.postMessage({{type:'emt-theme-change', theme:next}}, '*');
  }}
  var tc = _hmGetTheme();
  Plotly.relayout('heatmap', {{
    plot_bgcolor: tc.plotBg,
    paper_bgcolor: tc.paperBg,
    'xaxis.tickfont.color': tc.axisC,
    'xaxis2.tickfont.color': tc.axisC,
    'xaxis2.title.font.color': tc.axisC,
    'yaxis.tickfont.color': tc.axisC,
    'title.font.color': tc.textC,
  }});
  // Update month label colours
  var anns = layout.annotations.map(function(a) {{
    return Object.assign({{}}, a, {{font: {{size: 12, color: tc.monthC}}}});
  }});
  Plotly.relayout('heatmap', {{annotations: anns}});
  Plotly.relayout('heatmap', {{shapes: _hmThemedShapes()}});
  Plotly.restyle('heatmap', {{colorscale: [_hmWeekendCs()]}}, [2]);
  Plotly.restyle('heatmap', {{colorscale: [_hmGetCs()]}}, [0]);
  Plotly.restyle('heatmap', {{'marker.colorscale': [_hmGetTotCs()]}}, [1]);
}};
// _hmToggleBtn already appended to #hm-nav-right above

window.addEventListener('message', function(e) {{
  if (e.data && e.data.type === 'emt-theme') {{
    document.documentElement.setAttribute('data-theme', e.data.theme);
    _hmToggleBtn.textContent = e.data.theme === 'light' ? '\u2600' : '\u263e';
    var tc = _hmGetTheme();
    Plotly.relayout('heatmap', {{
      plot_bgcolor: tc.plotBg, paper_bgcolor: tc.paperBg,
      'xaxis.tickfont.color': tc.axisC, 'xaxis2.tickfont.color': tc.axisC,
      'xaxis2.title.font.color': tc.axisC, 'yaxis.tickfont.color': tc.axisC,
      'title.font.color': tc.textC,
    }});
    var anns = layout.annotations.map(function(a) {{
      return Object.assign({{}}, a, {{font: {{size: 12, color: tc.monthC}}}});
    }});
    Plotly.relayout('heatmap', {{annotations: anns}});
    Plotly.relayout('heatmap', {{shapes: _hmThemedShapes()}});
    Plotly.restyle('heatmap', {{colorscale: [_hmWeekendCs()]}}, [2]);
    Plotly.restyle('heatmap', {{colorscale: [_hmGetCs()]}}, [0]);
    Plotly.restyle('heatmap', {{'marker.colorscale': [_hmGetTotCs()]}}, [1]);
  }}
}});
Plotly.newPlot('heatmap', data, layout, {{responsive: false, scrollZoom: false, touchZoom: false, displayModeBar: false}}).then(function() {{
  // Defer scaleChart and metric apply to the next animation frame so the
  // iframe has its final layout dimensions before Plotly measures the container.
  // Without this, window.innerWidth can reflect a stale/transitioning width.
  requestAnimationFrame(function() {{
    scaleChart();
    // Apply metric state after chart is ready — always run to set button states
    // and ensure totals colorscale is consistent on initial kWh load too
    _hmApplyMetric();
  }});
}});
</script>

<script>
(function() {{
  // Listen for resize notifications from the EMT parent page (charts.html).
  // The parent's ResizeObserver detects the sidebar toggle and posts the
  // new available width — we use it to relayout charts without reloading.
  window.addEventListener('message', function(e) {{
    if (!e.data || e.data.type !== 'emt-resize') return;
    if (typeof _scaleDayCharts === 'function') _scaleDayCharts();
    if (typeof scaleChart === 'function') scaleChart();
  }});
}})();
</script>
</body>
</html>"""