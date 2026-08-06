#!/usr/bin/env python3
"""
probe_dispatches.py  —  read-only probe of Octopus completed IOG dispatches.

Purpose: show the RAW start/end times Octopus reports for each completed
dispatch, to confirm whether a slot that "finished early" (e.g. 06 Aug 02:00)
carries a real sub-slot window (02:00–02:20) rather than a padded full 30 min.
This is what EMT's `_completed_dispatch_slot_bounds` fix will capture going
forward; this probe reads it live so you can verify before relying on it.

READ-ONLY: it only runs `obtainKrakenToken` (auth) and the `completedDispatches`
query — the exact calls EMT already makes. It writes nothing to your account.

Usage:
    export OCTOPUS_API_KEY='sk_live_...'         # your Octopus API key
    export OCTOPUS_ACCOUNT='A-XXXXXXXX'          # your account number
    python3 probe_dispatches.py                  # all completed dispatches
    python3 probe_dispatches.py --date 2026-08-06 [--tz Europe/London]

Or pass them inline:
    python3 probe_dispatches.py --api-key sk_live_... --account A-XXXXXXXX
"""
import os, sys, json, argparse, urllib.request
from datetime import datetime, timezone

GRAPHQL_URL = "https://api.octopus.energy/v1/graphql/"


def _post(body, token=None):
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = f"JWT {token}"   # EMT uses the 'JWT ' prefix
    req = urllib.request.Request(
        GRAPHQL_URL, data=json.dumps(body).encode(), headers=headers, method="POST")
    with urllib.request.urlopen(req, timeout=45) as r:
        out = json.loads(r.read().decode())
    if out.get("errors"):
        raise SystemExit(f"GraphQL error: {json.dumps(out['errors'], indent=2)}")
    return out["data"]


def get_token(api_key):
    m = ("mutation getToken($input: ObtainJSONWebTokenInput!) {"
         "  obtainKrakenToken(input: $input) { token } }")
    d = _post({"query": m, "variables": {"input": {"APIKey": api_key}}})
    tok = (d.get("obtainKrakenToken") or {}).get("token")
    if not tok:
        raise SystemExit("Auth failed: no token returned (check API key).")
    return tok


def get_completed(token, account):
    q = ("query dispatches($acc: String!) {"
         "  completedDispatches(accountNumber: $acc) {"
         "    start end delta meta { source location } } }")
    d = _post({"query": q, "variables": {"acc": account}}, token=token)
    return d.get("completedDispatches") or []


def _parse(ts):
    if not ts:
        return None
    try:
        return datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
    except Exception:
        return None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--api-key", default=os.environ.get("OCTOPUS_API_KEY"))
    ap.add_argument("--account", default=os.environ.get("OCTOPUS_ACCOUNT")
                    or os.environ.get("OCTOPUS_ACCOUNT_NUMBER"))
    ap.add_argument("--date", help="filter to a local date, e.g. 2026-08-06")
    ap.add_argument("--tz", default="Europe/London", help="tz for display + --date filter")
    a = ap.parse_args()
    if not a.api_key or not a.account:
        raise SystemExit("Set OCTOPUS_API_KEY and OCTOPUS_ACCOUNT (or pass --api-key/--account).")

    try:
        from zoneinfo import ZoneInfo
        tz = ZoneInfo(a.tz)
    except Exception:
        tz = timezone.utc

    disp = get_completed(get_token(a.api_key), a.account)
    rows = []
    for d in disp:
        s, e = _parse(d.get("start")), _parse(d.get("end"))
        if not s or not e:
            continue
        rows.append((s, e, d.get("delta"), (d.get("meta") or {}).get("source")))
    rows.sort()

    print(f"completedDispatches for {a.account}: {len(rows)} records "
          f"(times shown in {a.tz})\n")
    print(f"{'start':17} {'end':17} {'mins':>5} {'kWh':>7}  source")
    total = 0.0
    for s, e, delta, src in rows:
        sl = s.astimezone(tz); el = e.astimezone(tz)
        if a.date and sl.strftime("%Y-%m-%d") != a.date:
            continue
        mins = (e - s).total_seconds() / 60.0
        total += mins
        try:
            kwh = float(delta) if delta is not None else 0.0
        except (TypeError, ValueError):
            kwh = 0.0
        flag = "  <-- sub-slot!" if abs(mins % 30) > 0.5 else ""
        print(f"{sl.strftime('%m-%d %H:%M:%S'):17} {el.strftime('%m-%d %H:%M:%S'):17} "
              f"{mins:5.0f} {kwh:7.2f}  {src or ''}{flag}")
    if a.date:
        print(f"\nTotal completed-dispatch time on {a.date}: "
              f"{total:.0f} min = {total/60:.2f} h")
    print("\nNote: completedDispatches is a rolling recent window — if a date is "
          "missing, it may have aged out of the API.")


if __name__ == "__main__":
    main()