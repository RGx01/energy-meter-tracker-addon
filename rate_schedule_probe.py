#!/usr/bin/env python3
"""
Rate-schedule probe  (read-only, runs LOCALLY from the repo folder)
===================================================================

Purpose
-------
Import pricing snaps a block's rate to the tariff SCHEDULE only when
`_tariff_rate_for()` returns a rate that matches the billed cost÷kWh. On the live
DB it never snaps — every imported block is `imp_rate == cost÷kWh` — which means
either the schedule came back EMPTY, or it's built-but-MIS-RESOLVING for historical
dates. This probe tells us which, by running EMT's OWN rate code
(`kraken_rates.build_rate_schedule` + `RateSchedule.day_rate_bounds`) against the
live tariff API and dumping what it produces.

It also builds a schedule for ANY tariff you pass with --tariff, so we can inspect
an AGILE tariff without being on it.

Why standalone (like octopus_cost_probe): it reuses the REAL `kraken_rates` (so we
test the code that's actually failing), but talks to Octopus over a tiny stdlib
REST client — no aiohttp, no container, no add-on rebuild. Run it from the
add-on/energy_meter_tracker repo folder so `kraken_rates.py` is importable.

Usage
-----
    python3 rate_schedule_probe.py --api-key sk_live_xxx --account A-XXXXXXXX

    # explore an Agile tariff you're not on:
    python3 rate_schedule_probe.py --api-key sk_live_xxx \
        --tariff E-1R-AGILE-24-10-01-A          # product derived from the code

Options
    --tariff CODE      build a schedule for this exact tariff (ad-hoc mode)
    --product CODE     override the derived product code
    --from YYYY-MM-DD  window start (ad-hoc) — defaults to ~2 years back
    --to   YYYY-MM-DD  window end
    --dates d1,d2,...  historical dates to resolve (default: a spread across the span)

Nothing is written. Identifiers are masked.
"""

import argparse
import base64
import json
import os
import sys
import urllib.parse
import urllib.request
import urllib.error
from datetime import datetime, timedelta

# Import EMT's REAL rate logic (stdlib-clean: kraken_rates → kraken_ingester, both
# import only logging/datetime/typing). Run from the repo folder so these resolve.
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
try:
    from kraken_rates import build_rate_schedule, RateSchedule
except Exception as e:  # pragma: no cover
    print("ERROR: could not import kraken_rates — run this from the "
          "add-on/energy_meter_tracker repo folder. (%s)" % e)
    sys.exit(2)

BASE = "https://api.octopus.energy"


def say(*a):
    print(*a, flush=True)


def mask(s):
    if not s:
        return "<none>"
    s = str(s)
    return s if len(s) <= 2 else s[0] + "…" + s[-1]


def _tariff_to_product_code(tariff_code):
    """Mirror KrakenAPIClient._tariff_to_product_code (kept inline to avoid the
    aiohttp import). 'E-1R-VAR-22-11-01-A' → 'VAR-22-11-01'."""
    parts = (tariff_code or "").split("-")
    if len(parts) < 4:
        return None
    middle = parts[2:-1]
    return "-".join(middle) if middle else None


class RestClient:
    """Minimal stdlib Octopus REST client — just enough for build_rate_schedule
    (get_unit_rates) and account discovery. HTTP Basic: API key as username."""

    def __init__(self, api_key):
        self._auth = "Basic " + base64.b64encode(
            f"{api_key}:".encode()).decode()

    def _get(self, path, params=None):
        url = path if path.startswith("http") else BASE + path
        if params:
            url += "?" + urllib.parse.urlencode(
                {k: v for k, v in params.items() if v is not None})
        req = urllib.request.Request(url, headers={"Authorization": self._auth})
        with urllib.request.urlopen(req, timeout=30) as r:
            return json.loads(r.read().decode())

    def _paginate(self, path, params=None):
        out, url, guard = [], path, 0
        while url and guard < 200:
            guard += 1
            page = self._get(url, params if guard == 1 else None)
            out.extend(page.get("results", []) or [])
            url = page.get("next")
        return out

    # build_rate_schedule awaits this — provide an async shim over the sync GET.
    async def get_unit_rates(self, product_code, tariff_code, *,
                             rate_type="standard-unit-rates",
                             period_from=None, period_to=None):
        path = (f"/v1/products/{product_code}/electricity-tariffs/"
                f"{tariff_code}/{rate_type}/")
        return self._paginate(path, {"period_from": period_from,
                                     "period_to": period_to})

    def get_account(self, account):
        return self._get(f"/v1/accounts/{account}/")


def _dump_schedule(rs, label):
    p = getattr(rs, "_periods", []) or []
    if not p:
        say(f"    {label}: EMPTY schedule (0 periods) — this is why pricing falls "
            f"to billed cost÷kWh")
        return
    vals = sorted({round(x[2], 5) for x in p})
    say(f"    {label}: {len(p)} periods, {len(vals)} distinct rate(s): {vals}")
    # date-span each distinct rate covers (first→last valid_from), reveals whether a
    # rate is 'orphaned' in a stale window or reaches the whole history.
    span = {}
    for vf, vt, rate in p:
        k = round(rate, 5)
        span.setdefault(k, [vf, vf])
        span[k][1] = vf
    for k in sorted(span):
        say(f"       rate {k}: valid_from {span[k][0][:10]} … {span[k][1][:10]}")


def _resolve_at(rs, dates):
    say("    resolve() + day_rate_bounds() at sample dates "
        "(off-peak should be the day's MIN, peak the MAX):")
    for d in dates:
        for hh in ("02:00", "14:00"):
            ts = f"{d}T{hh}:00"
            lo, hi = rs.day_rate_bounds(ts)
            say(f"       {ts}  resolve={rs.resolve(ts)}  day_bounds=({lo}, {hi})")


def _sample_dates(rs, override):
    if override:
        return [s.strip() for s in override.split(",") if s.strip()]
    p = getattr(rs, "_periods", []) or []
    if not p:
        return []
    try:
        lo = datetime.fromisoformat(p[0][0][:19])
        hi = datetime.fromisoformat((p[-1][0] or p[0][0])[:19])
    except ValueError:
        return []
    if hi <= lo:
        return [lo.strftime("%Y-%m-%d")]
    return [(lo + (hi - lo) * f).strftime("%Y-%m-%d")
            for f in (0.0, 0.33, 0.66, 0.95)]


def main():
    import asyncio
    ap = argparse.ArgumentParser()
    ap.add_argument("--api-key", default=os.environ.get("OCTOPUS_API_KEY"))
    ap.add_argument("--account", default=os.environ.get("OCTOPUS_ACCOUNT"))
    ap.add_argument("--tariff", default=None)
    ap.add_argument("--product", default=None)
    ap.add_argument("--from", dest="from_date", default=None)
    ap.add_argument("--to", dest="to_date", default=None)
    ap.add_argument("--dates", default=None,
                    help="comma-separated YYYY-MM-DD to resolve")
    a = ap.parse_args()
    if not a.api_key:
        say("Need --api-key (or OCTOPUS_API_KEY).")
        sys.exit(2)
    client = RestClient(a.api_key)

    say(f"rate-schedule probe — python {sys.version.split()[0]}")

    def _win(vf, vt):
        # build_rate_schedule wants ISO strings; pass Z-suffixed midnights.
        pf = (a.from_date + "T00:00:00Z") if a.from_date else (
            (vf + "T00:00:00Z") if vf else None)
        pt = (a.to_date + "T00:00:00Z") if a.to_date else (
            (vt + "T00:00:00Z") if vt else None)
        return pf, pt

    # ── ad-hoc tariff (Agile exploration) ────────────────────────────────────
    if a.tariff:
        prod = a.product or _tariff_to_product_code(a.tariff)
        say(f"\n── AD-HOC tariff {a.tariff}  (product {prod}) ──")
        pf, pt = _win(a.from_date, a.to_date)
        if not pf:
            pf = (datetime.utcnow() - timedelta(days=730)).strftime("%Y-%m-%dT00:00:00Z")
        rs = asyncio.run(build_rate_schedule(client, prod, a.tariff,
                                             period_from=pf, period_to=pt))
        _dump_schedule(rs, a.tariff)
        _resolve_at(rs, _sample_dates(rs, a.dates))
        say("\n=== done — paste the whole output back ===")
        return

    # ── configured account (why 100% billed) ─────────────────────────────────
    if not a.account:
        say("Need --account for the configured probe (or use --tariff for ad-hoc).")
        sys.exit(2)
    say(f"account={mask(a.account)}")
    acct = client.get_account(a.account)
    props = acct.get("properties", []) or []
    chosen = {"import": None, "export": None}
    for prop in props:
        for emp in prop.get("electricity_meter_points", []) or []:
            agr = emp.get("agreements", []) or []
            key = "export" if emp.get("is_export") else "import"
            if agr and chosen[key] is None:
                chosen[key] = {"mpan": emp.get("mpan"), "agreements": agr}
    for channel in ("import", "export"):
        info = chosen[channel]
        say(f"\n── {channel.upper()}  (MPAN {mask(info['mpan']) if info else '—'}) ──")
        if not info:
            say("    no agreements found for this channel")
            continue
        # Build one schedule per agreement (mirrors _build_channel_rate_segs), and
        # resolve at dates INSIDE each agreement window — that's where mis-resolution
        # of the effective-dated day/night history would show.
        for agr in info["agreements"]:
            tariff = agr.get("tariff_code")
            vf, vt = agr.get("valid_from"), agr.get("valid_to")
            if not tariff:
                continue
            prod = _tariff_to_product_code(tariff)
            say(f"  agreement {tariff}  ({(vf or '')[:10]} … {(vt or 'open')[:10]})")
            pf, pt = _win(vf[:10] if vf else None, vt[:10] if vt else None)
            try:
                rs = asyncio.run(build_rate_schedule(client, prod, tariff,
                                                     period_from=pf, period_to=pt))
            except Exception as e:
                say(f"    build failed: {e}")
                continue
            _dump_schedule(rs, tariff)
            # sample dates within THIS agreement window
            base_dates = _sample_dates(rs, a.dates)
            _resolve_at(rs, base_dates)
    say("\n=== done — paste the whole output back ===")


if __name__ == "__main__":
    try:
        sys.stdout.reconfigure(line_buffering=True)
    except Exception:
        pass
    try:
        main()
    except urllib.error.HTTPError as e:
        say(f"HTTP {e.code}: {e.read()[:300] if hasattr(e,'read') else e}")
    except Exception as e:
        import traceback
        say("ERROR:", e)
        traceback.print_exc()