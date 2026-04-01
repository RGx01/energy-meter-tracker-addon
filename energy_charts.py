from datetime import datetime, timedelta, timezone
try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo
from collections import defaultdict
import json


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


def calculate_billing_summary_for_period(blocks, period_start, period_end):
    meter_summary  = defaultdict(lambda: defaultdict(lambda: {"kwh": 0.0, "cost": 0.0, "read_start": None, "read_end": None}))
    meter_totals   = defaultdict(lambda: {"kwh": 0.0, "cost": 0.0, "read_start": None, "read_end": None})
    standing_by_day = defaultdict(float)
    charged_days   = set()
    meter_meta     = {}   # display_key -> {site, device, mpan, tariff, is_submeter}

    for block in sorted([b for b in blocks if b and b.get("start")], key=lambda b: b["start"]):
        block_start = datetime.fromisoformat(block["start"])
        if not (period_start <= block_start < period_end):
            continue

        meters = block.get("meters", {}) or {}

        # ── Pass 1: accumulate sub-meter kwh/cost per rate so we can subtract from main ──
        sub_by_rate = defaultdict(lambda: {"kwh": 0.0, "cost": 0.0})  # rate -> {kwh, cost}
        for meter_name, meter in meters.items():
            if not (meter or {}).get("meta", {}).get("sub_meter"):
                continue
            for channel_name, channel in (meter.get("channels", {}) or {}).items():
                if channel_name.lower().endswith("export"):
                    continue
                try:
                    channel = channel or {}
                    rate = round(float(channel.get("rate_used", channel.get("rate")) or 0.0), 4)
                    sub_by_rate[rate]["kwh"]  += float(channel.get("kwh") or 0.0)
                    sub_by_rate[rate]["cost"] += float(channel.get("cost") or 0.0)
                except Exception:
                    pass

        # ── Pass 2: accumulate all meters, subtracting sub-meter totals from main import ──
        for meter_name, meter in meters.items():
            display_name = meter_name.replace("_", " ").title()
            if meter.get("meta", {}).get("sub_meter"):
                display_name += " (sub-meter)"
            meter_m = (meter or {}).get("meta", {}) or {}
            is_main_import = (meter_name == "electricity_main")

            for channel_name, channel in (meter.get("channels", {}) or {}).items():
                try:
                    channel = channel or {}
                    channel_m = (channel.get("meta", {}) or {})
                    disp_key = f"{display_name} / {channel_name.replace('_', ' ').title()}"
                    if disp_key not in meter_meta:
                        meter_meta[disp_key] = {
                            "site":        meter_m.get("site"),
                            "device":      meter_m.get("device"),
                            "mpan":        channel_m.get("mpan"),
                            "tariff":      channel_m.get("tariff"),
                            "is_submeter": bool(meter_m.get("sub_meter")),
                        }
                    kwh  = float(channel.get("kwh_total", channel.get("kwh")) or 0.0)
                    cost = float(channel.get("cost") or 0.0)
                    rate = round(float(channel.get("rate_used", channel.get("rate")) or 0.0), 4)
                    is_export = channel_name.lower().endswith("export")

                    if is_export:
                        cost = -abs(cost)
                    elif is_main_import:
                        # Subtract sub-meter contribution at this rate from main import
                        kwh  = max(0.0, kwh  - sub_by_rate[rate]["kwh"])
                        cost = max(0.0, cost - sub_by_rate[rate]["cost"])

                    key = f"{display_name} / {channel_name.replace('_', ' ').title()}"
                    meter_summary[key][rate]["kwh"]  += kwh
                    meter_summary[key][rate]["cost"] += cost

                    if not meter_m.get("sub_meter"):
                        rs = channel.get("read_start")
                        re = channel.get("read_end")
                        if meter_summary[key][rate]["read_start"] is None or (rs is not None and rs < meter_summary[key][rate]["read_start"]):
                            meter_summary[key][rate]["read_start"] = rs
                        if re is not None:
                            meter_summary[key][rate]["read_end"] = re

                    meter_totals[key]["kwh"]        += kwh
                    meter_totals[key]["cost"]       += cost
                    meter_totals[key]["is_submeter"] = bool(meter_m.get("sub_meter"))
                    if not meter_m.get("sub_meter"):
                        rs = channel.get("read_start")
                        re = channel.get("read_end")
                        if meter_totals[key]["read_start"] is None or (rs is not None and rs < meter_totals[key]["read_start"]):
                            meter_totals[key]["read_start"] = rs
                        if re is not None:
                            meter_totals[key]["read_end"] = re
                except Exception:
                    pass

        day_key = block_start.date()
        if day_key not in charged_days:
            for meter in (meters or {}).values():
                standing_by_day[day_key] += float((meter or {}).get("standing_charge") or 0.0)
            charged_days.add(day_key)

    total_standing = sum(standing_by_day.values())
    total_cost = sum(t["cost"] for t in meter_totals.values()) + total_standing

    return {
        "start":          period_start,
        "end":            period_end,
        "meters":         meter_summary,
        "totals":         meter_totals,
        "standing":       standing_by_day,
        "total_standing": total_standing,
        "total_cost":     total_cost,
        "meter_meta":     meter_meta,
    }


# ─────────────────────────────────────────────────────────────
# Billing summary renderer
# ─────────────────────────────────────────────────────────────

def render_billing_summary(summary, currency='£', site_name=None):
    if not summary:
        return ""

    meter_meta = summary.get("meter_meta", {})

    # ── Site header — prefer passed site_name, fall back to block meta ──
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

    for meter_name in sorted(summary["meters"]):
        channels    = summary["meters"][meter_name]
        totals      = summary["totals"].get(meter_name, {})
        read_start  = totals.get("read_start")
        read_end    = totals.get("read_end")
        meta        = meter_meta.get(meter_name, {})
        is_submeter = totals.get("is_submeter") or meta.get("is_submeter", False)

        # Channel label: just "Import" or "Export" for all meters
        channel_label = meter_name.split(" / ", 1)[-1]

        # Suffix: MPAN for main meter, device name for sub-meters
        if is_submeter and meta.get("device"):
            suffix_html = f'&nbsp;&nbsp;|&nbsp;&nbsp;{meta["device"]}'
        elif not is_submeter and meta.get("mpan"):
            suffix_html = f'&nbsp;&nbsp;|&nbsp;&nbsp;MPAN: <span class="censored">{meta["mpan"]}</span>'
        else:
            suffix_html = ""
        title_line = f'{channel_label}{suffix_html}'

        # Line 2: meter reads (non-submeter only)
        if not is_submeter and read_start is not None:
            read_total = (read_end or 0.0) - (read_start or 0.0)
            reads_html = (f'<br><span class="reads">'
                          f'Start: <span class="censored">{read_start:.3f}</span>'
                          f'&nbsp;&nbsp;End: <span class="censored">{read_end:.3f}</span>'
                          f'&nbsp;&nbsp;Total: {read_total:.3f} kWh'
                          f'</span>')
        else:
            reads_html = ""

        html += f"""
        <tr class="channel-title">
          <td colspan="4">{title_line}{reads_html}</td>
        </tr>"""

        html += f'''
        <tr class="channel-header">
          <td></td><td>Rate ({currency}/kWh)</td><td>kWh</td><td>Cost ({currency})</td>
        </tr>'''

        for rate in sorted(channels):
            d = channels[rate]
            cost_val = d["cost"]
            cost_str = f"({-cost_val:.2f})" if cost_val < 0 else f"{cost_val:.2f}"
            html += f"""
            <tr>
              <td></td><td>{rate:.4f}</td>
              <td>{d['kwh']:.3f}</td><td>{cost_str}</td>
            </tr>"""

        total_cost = totals["cost"]
        total_cost_str = f"({-total_cost:.2f})" if total_cost < 0 else f"{total_cost:.2f}"
        html += f"""
        <tr class="channel-total">
          <td>Total</td><td></td>
          <td>{totals['kwh']:.3f}</td><td>{total_cost_str}</td>
        </tr>"""

    if summary["standing"]:
        # Group days by rate to handle mid-period tariff changes
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
        </tr>
      </table>
    </div>"""

    return html


# ─────────────────────────────────────────────────────────────
# Daily chart builder (returns HTML string for one day)
# ─────────────────────────────────────────────────────────────

def build_day_chart_html(day, day_blocks, meter_colors, chart_prefix='', block_minutes=30, currency='£', site_name=None):
    slots = 1440 // block_minutes
    meter_kwh    = defaultdict(lambda: [0.0] * slots)
    meter_rate   = defaultdict(lambda: [0.0] * slots)
    summary_kwh  = defaultdict(float)
    summary_cost = defaultdict(float)
    summary_rates = defaultdict(lambda: defaultdict(float))
    meter_display_name = {}   # meter_key -> human label from meta

    def _f(v, default=0.0):
        """Return float, treating None and non-numeric as default."""
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

            main_kwh  = _f(main_import.get("kwh_total", main_import.get("kwh")))
            main_cost = _f(main_import.get("cost"))

            for meter_name, meter in meters.items():
                if (meter or {}).get("meta", {}).get("sub_meter"):
                    sub = ((meter.get("channels", {}) or {}).get("import", {}) or {})
                    main_kwh  -= _f(sub.get("kwh"))
                    main_cost -= _f(sub.get("cost"))

            main_kwh  = max(main_kwh, 0.0)
            main_cost = max(main_cost, 0.0)
            main_rate = _f(main_import.get("rate_used", main_import.get("rate")))

            meter_kwh["electricity_main"][hh]  = main_kwh
            meter_rate["electricity_main"][hh] = main_rate
            summary_kwh["electricity_main"]   += main_kwh
            summary_cost["electricity_main"]  += main_cost
            summary_rates["electricity_main"][round(main_rate, 4)] += main_kwh
            if "electricity_main" not in meter_display_name:
                meta = main.get("meta", {}) or {}
                meter_display_name["electricity_main"] = site_name or meta.get("site", "House")

            for meter_name, meter in meters.items():
                if (meter or {}).get("meta", {}).get("sub_meter"):
                    sub      = ((meter.get("channels", {}) or {}).get("import", {}) or {})
                    sub_kwh  = _f(sub.get("kwh"))
                    sub_cost = _f(sub.get("cost"))
                    sub_rate = _f(sub.get("rate"))
                    meter_kwh[meter_name][hh]  = sub_kwh
                    meter_rate[meter_name][hh] = sub_rate
                    summary_kwh[meter_name]   += sub_kwh
                    summary_cost[meter_name]  += sub_cost
                    summary_rates[meter_name][round(sub_rate, 4)] += sub_kwh
                    if meter_name not in meter_display_name:
                        meta = (meter or {}).get("meta", {}) or {}
                        label = meta.get("device") or meter_name.replace("_", " ").title()
                        meter_display_name[meter_name] = label

            if main_export:
                exp_kwh  = abs(_f(main_export.get("kwh")))
                exp_cost = abs(_f(main_export.get("cost")))
                exp_rate = abs(_f(main_export.get("rate")))
                exp_name = "electricity_main_export"
                meter_kwh[exp_name][hh]  = -exp_kwh
                meter_rate[exp_name][hh] = exp_rate
                summary_kwh[exp_name]   += exp_kwh
                summary_cost[exp_name]  += exp_cost
                summary_rates[exp_name][round(exp_rate, 4)] += exp_kwh

        except Exception:
            pass

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
    sub_meter_names = [k for k in summary_kwh if k not in ("electricity_main", "electricity_main_export")]

    total_import      = sum(v for k, v in summary_kwh.items()  if not k.endswith("_export") and v > 0)
    total_import_cost = sum(v for k, v in summary_cost.items() if not k.endswith("_export") and summary_kwh.get(k, 0) > 0)

    def rate_breakdown_html(meter_key, css_extra=""):
        out = ""
        for rate in sorted(summary_rates[meter_key]):
            kwh = summary_rates[meter_key][rate]
            if kwh > 0.0001:
                out += (f'<span class="rate-row {css_extra}">'
                        f'{kwh:.3f} kWh @ {currency}{rate:.4f}</span>')
        return out

    house_kwh  = summary_kwh.get("electricity_main", 0.0)
    house_cost = summary_cost.get("electricity_main", 0.0)
    exp_kwh    = summary_kwh.get("electricity_main_export", 0.0)
    exp_cost   = summary_cost.get("electricity_main_export", 0.0)

    main_color   = meter_colors.get("electricity_main", "#1f77b4")
    export_color = meter_colors.get("electricity_main_export", "#ff7f0e")

    def cs(text, color, size="0.9em", bold=False):
        w = "font-weight:600;" if bold else ""
        return f'<span style="color:{color};font-size:{size};line-height:1.6;white-space:nowrap;{w}">{text}</span>'

    def rate_rows_colored(meter_key, color):
        out = ""
        for rate in sorted(summary_rates[meter_key]):
            kwh = summary_rates[meter_key][rate]
            if kwh > 0.0001:
                out += cs(f'{kwh:.3f} kWh @ {currency}{rate:.4f}', color, size="0.8em")
        return out

    totals_html = ''
    if total_import > 0:
        totals_html += cs(f'Total import: {total_import:.3f} kWh', main_color)
        totals_html += cs(f'Import cost: {currency}{total_import_cost:.2f}', main_color)
    if exp_kwh > 0:
        totals_html += cs(f'Total export: {exp_kwh:.3f} kWh', export_color)
        totals_html += cs(f'Export credit: {currency}{exp_cost:.2f}', export_color)
        totals_html += rate_rows_colored("electricity_main_export", export_color)

    breakdown_cols = []

    if sub_meter_names:
        if house_kwh > 0.0001:
            label = meter_display_name.get("electricity_main", "House")
            col   = '<div class="scol">'
            col  += cs(f'↳ <span class="censored">{label}</span>', main_color, size="1em", bold=True)
            col  += cs(f'{house_kwh:.3f} kWh', main_color, size="1em")
            col  += cs(f'{currency}{house_cost:.2f}', main_color, size="1em")
            col  += rate_rows_colored("electricity_main", adjust_color(main_color, 0.75))
            col  += '</div>'
            breakdown_cols.append(col)

        for meter_name in sub_meter_names:
            sub_kwh  = summary_kwh.get(meter_name, 0.0)
            sub_cost = summary_cost.get(meter_name, 0.0)
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

        nice_name = meter.replace("_", " ").replace("electricity main", "House").replace("export", "Grid Export").title()
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

    return f"""
<div class="day-chart-wrap">
  {summary_html}
  <div id="{chart_id}" class="chart-container"></div>
</div>
<script>
(function() {{
  var xLabels = {json.dumps(x_labels)};
  var xRanges = {json.dumps(x_ranges)};
  var xBar    = Array.from({{length:{slots}}}, (_, i) => i);
  var xLine   = Array.from({{length:{slots+1}}}, (_, i) => i - 0.5);
  var data    = [{",".join(traces)}];
  var tickStep = Math.max(1, Math.round(30 / {block_minutes}));
  var tickVals = xBar.filter(function(_, i) {{ return i % tickStep === 0; }});
  var tickTexts = xLabels.filter(function(_, i) {{ return i % tickStep === 0; }});
  var layout  = {{
    autosize: true,
    barmode: 'relative',
    margin: {{l:46, r:52, t:16, b:80}},
    plot_bgcolor:  _getThemeColours().plotBg,
    paper_bgcolor: _getThemeColours().paperBg,
    xaxis: {{
      tickmode: 'array', tickvals: tickVals, ticktext: tickTexts, tickangle: -45,
      showgrid: false
    }},
    yaxis:  {{title:'kWh',   showgrid:true,  gridcolor:_getThemeColours().gridC, titlefont:{{size:11, color:_getThemeColours().axisC}}, tickfont:{{color:_getThemeColours().axisC}}}},
    yaxis2: {{title:'{currency}/kWh', overlaying:'y', side:'right', showgrid:false, titlefont:{{size:11, color:_getThemeColours().axisC}}, tickfont:{{color:_getThemeColours().axisC}}}},
    legend: {{
      orientation: 'h',
      x: 0.5, xanchor: 'center',
      y: -0.28, yanchor: 'top',
      font: {{size: 11, color: _getThemeColours().axisC}},
    }}
  }};
  function _doRender_{chart_id_safe}() {{
      var el = document.getElementById('{chart_id}');
      if (!el) return;
      var wrap = el.closest('.day-chart-wrap');
      var wrapH = wrap ? wrap.offsetHeight : 0;
      layout.height = Math.max(wrapH > 0 ? wrapH : 0, 320);
    function _alignY2() {{
      var y1range = el._fullLayout.yaxis.range;
      var y2range = el._fullLayout.yaxis2.range;
      var y2max = el._fullLayout.yaxis2._range
                  ? el._fullLayout.yaxis2._range[1]
                  : y2range[1];
      if (y2max <= 0) return;
      var hasImport = data.some(function(t) {{ return t.yaxis !== 'y2' && t.y && t.y.some(function(v) {{ return v > 0.001; }}); }});
      var hasExport = data.some(function(t) {{ return t.yaxis !== 'y2' && t.y && t.y.some(function(v) {{ return v < -0.001; }}); }});
      var y1min = y1range[0];
      var y1top, y2min;
      var rawStep = y2max / 4;
      var mag  = Math.pow(10, Math.floor(Math.log10(rawStep)));
      var step = [1, 2, 2.5, 5, 10].map(function(f) {{ return f * mag; }})
                   .find(function(s) {{ return s >= rawStep; }}) || mag;
      var ticks = [];
      for (var t = 0; t <= y2max + step * 0.01; t += step) ticks.push(parseFloat(t.toFixed(10)));
      if (hasExport && !hasImport) {{
        // Export only — add headroom above 0 on y1, align y2 accordingly
        var exportDepth = -y1min;
        y1top  = exportDepth * 0.5;
        var frac  = exportDepth / (exportDepth + y1top);
        var y2span = y2max / (1 - frac);
        y2min  = -frac * y2span;
      }} else if (hasImport && hasExport) {{
        // Mixed — keep y1 as-is, offset y2 down
        y1top  = y1range[1];
        var negFrac = -y1min / (y1range[1] - y1min);
        var y2span  = y2max / (1 - negFrac);
        y2min  = -negFrac * y2span;
      }} else {{
        // Import only
        y1top = y1range[1];
        y2min = 0;
      }}
      Plotly.relayout(el, {{
        'yaxis.range':     [y1min, y1top],
        'yaxis2.range':    [y2min, y2max],
        'yaxis2.tickmode': 'array',
        'yaxis2.tickvals': ticks,
        'yaxis2.ticktext': ticks.map(function(v) {{ return v.toFixed(2); }})
      }});
    }}
    Plotly.newPlot(el, data, layout, {{responsive:true, displayModeBar:false}}).then(function() {{
      _alignY2();
      el.on('plotly_restyle', function() {{
        setTimeout(_alignY2, 50);
      }});
    }});
    if (!window._energyCharts) window._energyCharts = {{}};
    window._energyCharts['{chart_id}'] = el;
    _scaleChartEl(el);
  }}
  if (!window._pendingCharts) window._pendingCharts = {{}};
  window._pendingCharts['{chart_id}'] = _doRender_{chart_id_safe};
}})();

</script>
"""


# ─────────────────────────────────────────────────────────────
# Main entry point
# ─────────────────────────────────────────────────────────────

def generate_daily_import_export_charts(blocks, timezone_name="UTC", block_minutes=None, currency='£'):

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

    meter_colors = build_meter_colors(blocks)

    # ── Billing periods ──
    # Read billing_day from meters_config.json first (reflects latest user setting),
    # fall back to block meta for backward compatibility
    try:
        from energy_engine_io import load_json as _load_json
        import os as _os
        _cfg = _load_json("/data/energy_meter_tracker/meters_config.json", {})
        _main_meta = {}
        for _md in _cfg.get("meters", {}).values():
            if not (_md.get("meta") or {}).get("sub_meter"):
                _main_meta = _md.get("meta") or {}
                break
        billing_day = int(_main_meta.get("billing_day") or 0) or int(next(
            b["meters"]["electricity_main"]["meta"]["billing_day"]
            for b in blocks
            if b and b.get("meters", {}) and b["meters"].get("electricity_main", {})
               and b["meters"]["electricity_main"].get("meta", {}).get("billing_day")
        ))
        site_name = _main_meta.get("site") or None
    except (StopIteration, KeyError, TypeError, ValueError):
        billing_day = 1
        site_name = None

    periods = get_all_billing_periods(blocks, billing_day, tz=_tz)

    # ── Build per-period data ──
    period_sections = []   # list of dicts

    for i, (p_start, p_end) in enumerate(periods):
        summary   = calculate_billing_summary_for_period(blocks, p_start, p_end)
        is_current = p_start.date() <= today < p_end.date()
        is_prev    = (len(periods) > 1) and (i == len(periods) - 2) and not is_current

        # days that belong to this period
        period_days = sorted(
            [d for d in days_map if p_start.date().isoformat() <= d < p_end.date().isoformat()],
            reverse=True
        )

        period_sections.append({
            "index":      i,
            "start":      p_start,
            "end":        p_end,
            "summary":    summary,
            "is_current": is_current,
            "is_prev":    is_prev,
            "days":       period_days,
        })

    # Sort periods newest-first for display
    period_sections_display = list(reversed(period_sections))

    # ── Quarter periods ──
    quarter_sections = []
    for i, (q_start, q_end) in enumerate(get_all_quarter_periods(blocks, tz=_tz)):
        summary    = calculate_billing_summary_for_period(blocks, q_start, q_end)
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
        summary    = calculate_billing_summary_for_period(blocks, y_start, y_end)
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
        summary    = calculate_billing_summary_for_period(blocks, cm_start, cm_end)
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
        calmonth_options.append(f'<option value="calmonth_{cs['index']}">{label}</option>')

    # ── Dropdown options (quarter) ──
    quarter_options = []
    for qs in quarter_sections_display:
        cost  = qs["summary"]["total_cost"]
        label = f"{qs['label']}  |  {currency}{cost:.2f}"
        if qs["is_current"]:
            label = "★ Current  " + label
        quarter_options.append(f'<option value="quarter_{qs['index']}">{label}</option>')

    # ── Dropdown options (year) ──
    year_options = []
    for ys in year_sections_display:
        cost  = ys["summary"]["total_cost"]
        label = f"{ys['label']}  |  {currency}{cost:.2f}"
        if ys["is_current"]:
            label = "★ Current  " + label
        year_options.append(f'<option value="year_{ys['index']}">{label}</option>')

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
    <button class="view-btn" id="theme-toggle" onclick="toggleTheme()" title="Toggle light/dark mode">&#9790;</button>
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
            day_charts_html += build_day_chart_html(day, days_map[day], meter_colors, block_minutes=block_minutes, currency=currency, site_name=site_name)

        open_attr    = "open" if charts_open else ""
        toggle_label = f"Daily Charts &mdash; {ph} &nbsp;|&nbsp; {currency}{bill_total:.2f}"

        sections_html_parts.append(f"""
<div class="period-section month-section" id="{pid}" style="visibility:hidden;position:absolute;">
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
            day_charts_html += build_day_chart_html(day, days_map[day], meter_colors, chart_prefix=f"{pid_prefix}_", block_minutes=block_minutes, currency=currency, site_name=site_name)

        toggle_label = f"Daily Charts &mdash; {ph} &nbsp;|&nbsp; {currency}{bill_total:.2f}"
        open_attr    = "open" if gs["is_current"] else ""
        return (
            f'<div class="period-section {pid_prefix}-section" id="{pid}" style="visibility:hidden;position:absolute;">'
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
    html = f"""<!DOCTYPE html>
<html data-theme="light">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<meta http-equiv="Cache-Control" content="no-cache, no-store, must-revalidate"/>
<meta http-equiv="Pragma" content="no-cache"/>
<meta http-equiv="Expires" content="0"/>
<meta http-equiv="refresh" content="130"/>
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
<style>

/* ── Theme variables ──────────────────────────── */
:root {{
  --bg:      #f0f2f5; --surface: #ffffff; --border: #d0d5dd;
  --text:    #1a1d27; --muted:   #555970; --accent: #0a8c6a;
  --card:    #ffffff; --input-bg:#fafafa;
}}
[data-theme="dark"] {{
  --bg:      #0f1117; --surface: #1a1d27; --border: #2a2d3a;
  --text:    #e8eaf0; --muted:   #6b7080; --accent: #00d4aa;
  --card:    #1a1d27; --input-bg:#0f1117;
}}

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

{''.join(sections_html_parts)}
{''.join(calmonth_html_parts)}
{''.join(quarter_html_parts)}
{''.join(year_html_parts)}

</div>

<script>
// ── Mobile chart scaling ──────────────────────────────────
var _MIN_CHART_W = 380; // px — below this we scale rather than reflow
function _scaleChartEl(el) {{
  var wrap = el.closest('.day-chart-wrap');
  if (!wrap) return;
  var avail = wrap.offsetWidth;
  if (avail < 1) return;
  // In column layout (mobile), chart takes full width — no scaling needed
  // In row layout (desktop), chart is flex:1 beside summary
  var isMobile = window.getComputedStyle(wrap).flexDirection === 'column';
  if (isMobile) {{
    el.style.transform = '';
    el.style.transformOrigin = '';
    el.style.width = '';
    // Reduce tick density on narrow screens
    if (el._fullData && avail < 480) {{
      var skipEvery = avail < 320 ? 5 : 3; // show every 3rd or 5th hour
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
  }}
}}

function _scaleDayCharts() {{
  document.querySelectorAll('.chart-container').forEach(function(el) {{
    if (window._energyCharts && window._energyCharts[el.id]) _scaleChartEl(el);
  }});
}}

window.addEventListener('resize', _scaleDayCharts, {{passive: true}});

// ── Deferred chart renderer ──────────────────────────────────
function _renderSection(section) {{
  if (!section || !window._pendingCharts) return;
  section.querySelectorAll('.chart-container').forEach(function(el) {{
    var fn = window._pendingCharts[el.id];
    if (fn) {{
      delete window._pendingCharts[el.id];
      fn();
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
  // Hide all sections, show correct one for mode
  document.querySelectorAll('.period-section').forEach(function(el) {{ 
      el.style.visibility='hidden'; 
      el.style.position='absolute'; 
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
}}

function _revealSection(id) {{
    var section = document.getElementById(id);
    if (section) {{
      section.style.visibility = 'visible';
      section.style.position = 'relative';
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
    el.style.visibility='hidden'; 
    el.style.position='absolute'; 
  }});
  document.getElementById('sticky-bill-strip').style.visibility = 'hidden';
  _revealSection(id);
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

function toggleCensor() {{
  var on = document.body.classList.toggle('censor-on');
  var btn = document.getElementById('censor-toggle');
  if (btn) btn.classList.toggle('active', on);
  sessionStorage.setItem('energyCensor', on ? '1' : '0');
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
    days = defaultdict(lambda: [0.0] * slots)
    for block in sorted([b for b in blocks if b and b.get("start")], key=lambda b: b["start"]):
        try:
            start = _parse_block_start(block["start"], _tz)
            day = start.date().isoformat()
            hh_index = (start.hour * 60 + start.minute) // block_minutes
            totals = block.get("totals", {}) or {}
            net = _f(totals.get("import_kwh")) - _f(totals.get("export_kwh"))
            days[day][hh_index] = net
        except Exception:
            continue

    sorted_days = sorted(days.keys())
    heatmap_data = [days[d] for d in sorted_days]
    daily_totals = [sum(row) for row in heatmap_data]

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
    totals_colorscale = make_colorscale(tot_min, tot_max)

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
    visible_rows   = 31
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
    totals_cs_json   = json.dumps(totals_colorscale)
    weekend_z_json   = json.dumps(weekend_z)
    x_tickvals_json  = json.dumps(x_tickvals)

    return f"""<html data-theme="light">
<head>
<meta name="viewport" content="width=device-width, initial-scale=1, maximum-scale=1, user-scalable=no">
<meta http-equiv="refresh" content="130">
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
html, body {{ margin:0; padding:0; overflow:hidden; touch-action: none; background:var(--bg); color:var(--text); height:100%; }}
  #outer {{ width:{heatmap_width}px; transform-origin: top left; position: relative; }}
  #scroll {{
    width:{heatmap_width}px;
    height:100vh;
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
    top: 0; right: 0;
    width: 44px;
    height: 100%;
    z-index: 100;
    touch-action: pan-y;
    background: var(--scroll-guard-bg);
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
    width: 4px;
    height: 48px;
    background: var(--scroll-guard-pill);
    border-radius: 2px;
  }}
</style>
<body>
<div id="outer">
  <div id="scroll">
      <div id="heatmap" style="width:{heatmap_width}px;height:{heatmap_height}px;"></div>
  </div>
  <div id="scroll-guard"></div>
</div>
<script>
function scaleChart() {{
  var vw = window.innerWidth;
  var vh = window.innerHeight;
  var cw = {heatmap_width};
  var isMobile = vw <= 768 || (vh <= 500 && vw > vh);
  var outer  = document.getElementById('outer');
  var scroll = document.getElementById('scroll');
  var guard  = document.getElementById('scroll-guard');
  // Show scroll-grab strip only on mobile
  if (guard) guard.classList.toggle('visible', isMobile);
  var guardW = isMobile ? 44 : 0;
  var availW = vw - guardW;
  if (availW < cw) {{
    var scale = availW / cw;
    outer.style.transform = 'scale(' + scale + ')';
    outer.style.transformOrigin = 'top left';
    outer.style.width = cw + 'px';
    // Set outer height to fill exactly the viewport so no gap below
    // Don't force outer to vh — let scroll fill it instead
    outer.style.height = 'auto';
  }} else {{
    outer.style.transform = '';
    outer.style.transformOrigin = '';
    outer.style.width = '';
    outer.style.height = '';
  }}
  // On mobile fill as much of the viewport as possible
  var maxRows = isMobile
    ? Math.max(10, Math.floor((vh - {margin_t} - {margin_b}) / {row_height}) + 6)
    : {visible_rows};
  var scrollH = Math.min({n_rows}, maxRows) * {row_height} + {margin_t} + {margin_b};
  // On mobile: fill the full viewport height so no gap appears below the chart
  if (isMobile) scrollH = vh;
  scroll.style.height = scrollH + 'px';
}}
// Prevent pinch-zoom on the chart — Plotly intercepts touches and can trigger browser zoom
document.addEventListener('touchstart', function(e) {{
  if (e.touches.length > 1) {{ e.preventDefault(); }}
}}, {{ passive: false }});
document.addEventListener('touchmove', function(e) {{
  if (e.touches.length > 1) {{ e.preventDefault(); }}
}}, {{ passive: false }});
window.addEventListener('resize', scaleChart);
scaleChart();
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
var data = [
{{
  z: {z_json},
  x: {x_json},
  y: {y_json},
  customdata: {customdata_json},
  type: 'heatmap',
  colorscale: {heatmap_cs_json},
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
    colorscale: {totals_cs_json},
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
  title: {{text: 'Net Energy Flow', x: 0.5, font: {{color: _hmTc.textC}}}},
  xaxis:  {{tickangle: -45, side: 'top', domain: [0, 0.85], tickmode: 'array', tickvals: {x_tickvals_json}, ticktext: {x_tickvals_json}, tickfont: {{color: _hmTc.axisC}}}},
  xaxis2: {{title: {{text: 'Daily Total', standoff: 10, font: {{color: _hmTc.axisC}}}}, side: 'top', domain: [0.86, 1], tickfont: {{color: _hmTc.axisC}}}},
  yaxis:  {{type: 'category', tickmode: 'array', tickvals: {y_json}, ticktext: {y_ticktext_json}, fixedrange: true, tickfont: {{color: _hmTc.axisC}}}},
  shapes: _hmShapes,
  annotations: {annotations_json},
  height: {heatmap_height},
  width: {heatmap_width},
  margin: {{l: {margin_l}, r: {margin_r}, t: {margin_t}, b: {margin_b}}},
  plot_bgcolor: _hmTc.plotBg,
  paper_bgcolor: _hmTc.paperBg
}};

// Update month annotation colours to match theme
var _annotations = {annotations_json};
_annotations.forEach(function(a) {{ if (a.font) a.font.color = _hmTc.monthC; }});
layout.annotations = _annotations;

// Theme toggle button
var _hmToggleBtn = document.createElement('button');
_hmToggleBtn.id = 'hm-theme-btn';
_hmToggleBtn.textContent = document.documentElement.getAttribute('data-theme') === 'light' ? '\u2600' : '\u263e';
_hmToggleBtn.style.cssText = 'position:absolute;top:6px;right:6px;z-index:200;background:var(--surface);border:1px solid var(--border);color:var(--muted);border-radius:6px;padding:3px 8px;font-size:13px;cursor:pointer;opacity:0.85;';
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
}};
document.body.appendChild(_hmToggleBtn);

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
  }}
}});
Plotly.newPlot('heatmap', data, layout, {{responsive: false, scrollZoom: false, touchZoom: false, displayModeBar: false}}).then(scaleChart);
</script>
</body>
</html>"""