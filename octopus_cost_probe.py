#!/usr/bin/env python3
"""
Octopus cost-source probe  (read-only, no dependencies, ALL GraphQL)
====================================================================

Purpose
-------
Our historical import reads the half-hourly cost from the GraphQL
`measurements` node's TOU_BUCKET_COST statistic. For IOG smart-charged
("dispatched") slots Octopus attaches NO such statistic, so the import falls
back to the tariff schedule and prices those slots at PEAK — yet the Octopus
website CSV shows them at the off-peak rate. So the cost exists somewhere we
aren't querying. This finds where.

Everything here goes through the SAME GraphQL endpoint + JWT the import uses
(obtainKrakenToken → `Authorization: JWT <token>`). No REST, no basic-auth.

It runs four probes against ONE known-mispriced day (21 Oct 2025):

  1. Introspects the measurement / statistics GraphQL types → lists EVERY
     field available, so we can spot a cost field we don't currently request.
  2. Dumps the FULL raw measurement node for that evening's slots (dispatched
     vs normal side by side) — so we see the empty-statistics case directly.
  3. Tries the account `transactions` surface (billed/settled charges) for the
     day, in case the dispatch-aware cost lives there.
  4. Tries `smartMeterTelemetry.costDelta` (the Octopus Mini per-reading cost)
     for the day — another candidate cost source already in the schema.

NOTHING is written anywhere. Output masks your MPAN and account number.

Usage
-----
    python3 octopus_cost_probe.py --api-key sk_live_xxx --account A-XXXXXXXX

or with environment variables:

    export OCTOPUS_API_KEY=sk_live_xxx
    export OCTOPUS_ACCOUNT=A-XXXXXXXX
    python3 octopus_cost_probe.py

Optional: --day 2025-10-21   (defaults to the known-bad day)
          --mpan 2000...     (skip auto-discovery if you already know it)

Where to get the two values
---------------------------
- API key:  Octopus dashboard → Developer / API access
            https://octopus.energy/dashboard/new/accounts/personal-details/api-access
            Copy the "API key" (starts with sk_live_...). Read-only.
- Account:  the A-XXXXXXXX number shown on your dashboard.

Paste the whole printed output back (it's already masked).
"""

import argparse
import json
import os
import sys
import traceback
import urllib.request
import urllib.error

GRAPHQL_URL = "https://api.octopus.energy/v1/graphql/"


def say(*args):
    """print that always flushes — so nothing is lost if we crash mid-run."""
    print(*args, flush=True)


# ── GraphQL transport (stdlib only, JWT like the import) ─────────────────────
def gql(query, variables=None, token=None):
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = f"JWT {token}"      # Kraken uses the 'JWT ' prefix
    data = json.dumps({"query": query, "variables": variables or {}}).encode()
    req = urllib.request.Request(GRAPHQL_URL, data=data, headers=headers, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            return json.loads(r.read())
    except urllib.error.HTTPError as e:
        # Octopus returns error detail in the BODY of a 400/401 — surface it.
        try:
            body = e.read().decode()
        except Exception:
            body = ""
        say(f"  ! HTTP {e.code} from GraphQL: {body[:400]}")
        try:
            return json.loads(body)
        except Exception:
            return {"_http_error": e.code, "_body": body[:400]}
    except urllib.error.URLError as e:
        say(f"  ! network error reaching {GRAPHQL_URL}: {e}")
        return {"_network_error": str(e)}


def mask(s):
    s = str(s or "")
    return (s[:2] + "…" + s[-1]) if len(s) > 3 else "…"


def get_token(api_key):
    m = ("mutation getToken($input: ObtainJSONWebTokenInput!) {"
         "  obtainKrakenToken(input: $input) { token } }")
    d = gql(m, {"input": {"APIKey": api_key}})
    tok = (((d.get("data") or {}).get("obtainKrakenToken")) or {}).get("token")
    if not tok:
        print("AUTH FAILED:", json.dumps(d)[:400]); sys.exit(1)
    print("✓ token obtained")
    return tok


# ── discover MPAN(s) via GraphQL (same shape as client.get_device_id) ────────
def discover_mpans(token, account):
    q = ("query disc($acc:String!){ account(accountNumber:$acc){"
         "  electricityAgreements(active:true){"
         "    meterPoint{ mpan meters(includeInactive:false){ serialNumber } } } } }")
    d = gql(q, {"acc": account}, token)
    if d.get("errors"):
        print("  discovery errors:", json.dumps(d["errors"])[:400])
    acct = ((d.get("data") or {}).get("account")) or {}
    out = []
    for agr in acct.get("electricityAgreements", []) or []:
        mp = agr.get("meterPoint") or {}
        mpan = mp.get("mpan")
        serial = None
        for m in mp.get("meters", []) or []:
            serial = m.get("serialNumber") or serial
        if mpan:
            out.append((mpan, serial))
    # de-dup preserving order
    seen, uniq = set(), []
    for mpan, serial in out:
        if mpan not in seen:
            seen.add(mpan); uniq.append((mpan, serial))
    return uniq


# ── measurements query (expanded: full statistics) ───────────────────────────
_MEAS_Q = (
    "query meas($acc:String!,$mpan:String!,$start:DateTime!,$end:DateTime!){"
    "  account(accountNumber:$acc){ properties{ measurements("
    "    first:80, startAt:$start, endAt:$end, timezone:\"Europe/London\","
    "    utilityFilters:[{electricityFilters:{"
    "      readingFrequencyType:THIRTY_MIN_INTERVAL,"
    "      marketSupplyPointId:$mpan, readingDirection:CONSUMPTION}}]){"
    "    edges{ node{ value unit "
    "      ... on IntervalMeasurementType{ startAt endAt "
    "        metaData{ statistics{ type label value "
    "          costInclTax{ estimatedAmount costCurrency } "
    "          costExclTax{ estimatedAmount } } } } } } } } } }"
)


def _measurement_edges(token, account, mpan, start, end):
    d = gql(_MEAS_Q, {"acc": account, "mpan": mpan, "start": start, "end": end}, token)
    if d.get("errors"):
        return None, d["errors"]
    props = (((d.get("data") or {}).get("account") or {}).get("properties")) or []
    for p in props:
        m = p.get("measurements")
        if m and m.get("edges"):
            return m["edges"], None
    return [], None


def pick_import_mpan(token, account, mpans, day):
    """The import MPAN is the one that returns CONSUMPTION intervals."""
    probe_start, probe_end = f"{day}T12:00:00Z", f"{day}T13:00:00Z"
    for mpan, serial in mpans:
        edges, err = _measurement_edges(token, account, mpan, probe_start, probe_end)
        n = len(edges or [])
        print(f"    MPAN {mask(mpan)}: {n} consumption interval(s)"
              + (f"  (errors: {json.dumps(err)[:120]})" if err else ""))
        if n > 0:
            return mpan, serial
    return (mpans[0] if mpans else (None, None))


# ── 1. introspection ─────────────────────────────────────────────────────────
def introspect(token, type_name):
    q = ("query intro($n:String!){ __type(name:$n){ name kind "
         "fields{ name type{ name kind ofType{ name kind ofType{ name kind } } } } } }")
    d = gql(q, {"n": type_name}, token)
    t = ((d.get("data") or {}).get("__type"))
    if not t:
        return
    print(f"  type {t['name']} ({t['kind']}):")
    for f in (t.get("fields") or []):
        ty = f["type"]
        tn = ty.get("name") or (ty.get("ofType") or {}).get("name") \
            or ((ty.get("ofType") or {}).get("ofType") or {}).get("name")
        print(f"    - {f['name']}: {tn}")


# ── 2. full measurement nodes for the target evening ─────────────────────────
def dump_measurements(token, account, mpan, day):
    edges, err = _measurement_edges(token, account, mpan,
                                    f"{day}T16:00:00Z", f"{day}T23:59:00Z")
    if err:
        print("  measurements query errors:", json.dumps(err)[:500]); return
    print(f"  {len(edges)} interval(s) for {day} evening:")
    for e in edges:
        n = e.get("node") or {}
        stats = ((n.get("metaData") or {}).get("statistics")) or []
        tag = "  <== NO STATISTICS (the mispriced case)" if not stats else ""
        print(f"    {n.get('startAt')}  kWh={n.get('value')}  stats={len(stats)}{tag}")
        for s in stats:
            ci = (s.get("costInclTax") or {}).get("estimatedAmount")
            print(f"        · {s.get('type')} label={s.get('label')} "
                  f"value={s.get('value')} costInclTax={ci}")


# ── 3. account transactions (billed/settled cost) ────────────────────────────
def dump_transactions(token, account):
    q = ("query tx($acc:String!){ account(accountNumber:$acc){"
         "  transactions(first:20){ edges{ node{ __typename "
         "    ... on Transaction{ postedDate title amount isHeld isIssued } } } } } }")
    d = gql(q, {"acc": account}, token)
    if d.get("errors"):
        print("  transactions errors:", json.dumps(d["errors"])[:400])
    print("  response (first 700 chars):", json.dumps(d.get("data") or d)[:700])


# ── 4. smartMeterTelemetry.costDelta (Mini per-reading cost) ─────────────────
def dump_telemetry_cost(token, account, day):
    # deviceId first
    qd = ("query dev($acc:String!){ account(accountNumber:$acc){"
          "  electricityAgreements(active:true){ meterPoint{ meters("
          "    includeInactive:false){ smartDevices{ deviceId } } } } } }")
    dd = gql(qd, {"acc": account}, token)
    did = None
    for agr in ((((dd.get("data") or {}).get("account")) or {})
                .get("electricityAgreements", []) or []):
        for m in ((agr.get("meterPoint") or {}).get("meters", []) or []):
            for s in (m.get("smartDevices", []) or []):
                did = s.get("deviceId") or did
    if not did:
        print("  no Mini deviceId on account — telemetry cost source N/A here")
        return
    q = ("query tel($id:String!,$start:DateTime!,$end:DateTime!){"
         "  smartMeterTelemetry(deviceId:$id, grouping:HALF_HOURLY,"
         "    start:$start, end:$end){ readAt consumptionDelta costDelta } }")
    d = gql(q, {"id": did, "start": f"{day}T18:00:00Z", "end": f"{day}T23:00:00Z"}, token)
    if d.get("errors"):
        print("  telemetry errors:", json.dumps(d["errors"])[:300]); return
    rows = ((d.get("data") or {}).get("smartMeterTelemetry")) or []
    print(f"  {len(rows)} telemetry row(s) (device {mask(did)}):")
    for r in rows[:12]:
        print("   ", r)


_MEAS_Q_PAGED = (
    "query meas($acc:String!,$mpan:String!,$start:DateTime!,$end:DateTime!,"
    "$first:Int!,$after:String){"
    "  account(accountNumber:$acc){ properties{ measurements("
    "    first:$first, after:$after, startAt:$start, endAt:$end,"
    "    timezone:\"Europe/London\","
    "    utilityFilters:[{electricityFilters:{"
    "      readingFrequencyType:THIRTY_MIN_INTERVAL,"
    "      marketSupplyPointId:$mpan, readingDirection:CONSUMPTION}}]){"
    "    edges{ node{ value unit "
    "      ... on IntervalMeasurementType{ startAt "
    "        metaData{ statistics{ type label "
    "          costInclTax{ estimatedAmount } } } } } }"
    "    pageInfo{ hasNextPage endCursor } } } } }"
)


def _target_evening(nodes, day):
    """Filter parsed nodes to the target day's evening and summarise stats presence."""
    out = []
    for n in nodes:
        sa = n.get("startAt") or ""
        if sa[:10] in (day, _plusday(day)) and (sa[11:13] >= "16" or sa[:10] == _plusday(day) and sa[11:13] < "01"):
            stats = ((n.get("metaData") or {}).get("statistics")) or []
            out.append((sa, n.get("value"), len(stats),
                        [s.get("label") for s in stats if s.get("type") != "STANDING_CHARGE_COST"]))
    return out


def _plusday(day):
    from datetime import datetime, timedelta
    return (datetime.fromisoformat(day) + timedelta(days=1)).strftime("%Y-%m-%d")


def _paged_fetch(token, account, mpan, start, end, first, query=_MEAS_Q_PAGED):
    nodes, after, pages = [], None, 0
    while pages < 60:
        d = gql(query, {"acc": account, "mpan": mpan, "start": start,
                        "end": end, "first": first, "after": after}, token)
        if d.get("errors"):
            say(f"    paged fetch error: {json.dumps(d['errors'])[:200]}"); break
        props = (((d.get("data") or {}).get("account") or {}).get("properties")) or []
        conn = None
        for p in props:
            m = p.get("measurements")
            if m and (m.get("edges") or m.get("pageInfo")):
                conn = m; break
        if conn is None:
            break
        for e in (conn.get("edges") or []):
            nodes.append(e.get("node") or {})
        pages += 1
        pi = conn.get("pageInfo") or {}
        if not pi.get("hasNextPage") or not pi.get("endCursor"):
            break
        after = pi.get("endCursor")
    return nodes, pages


def probe_window_effect(token, account, mpan, day):
    """Does a LARGE page size or a WIDE paginated window strip the TOU bucket from
    the dispatched slots? Compares three fetches of the SAME target evening."""
    from datetime import datetime, timedelta
    d = datetime.fromisoformat(day)
    nar_s, nar_e = f"{day}T16:00:00Z", f"{_plusday(day)}T01:00:00Z"
    wide_s = (d - timedelta(days=40)).strftime("%Y-%m-%dT00:00:00Z")
    wide_e = (d + timedelta(days=20)).strftime("%Y-%m-%dT00:00:00Z")

    for label, (s, e, first) in {
        "A. narrow window, small page (first:80)": (nar_s, nar_e, 80),
        "B. narrow window, LARGE page (first:500)": (nar_s, nar_e, 500),
        "C. WIDE 60-day window, paged (first:500)": (wide_s, wide_e, 500),
    }.items():
        nodes, pages = _paged_fetch(token, account, mpan, s, e, first)
        ev = _target_evening(nodes, day)
        no_stats = sum(1 for _, _, n, _ in ev if n == 0)
        total_nodes = len(nodes)
        no_stats_all = sum(1 for n in nodes
                           if not (((n.get("metaData") or {}).get("statistics")) or []))
        say(f"  {label}: {pages} page(s), {total_nodes} nodes, "
            f"{no_stats_all} with NO stats overall; target-evening intervals={len(ev)}, "
            f"{no_stats} of them missing stats")
        for sa, val, nstat, labels in ev:
            tag = "  <== NO BUCKET" if nstat == 0 else f"  {labels}"
            say(f"      {sa}  kWh={val}  stats={nstat}{tag}")


# The 34 slots in the 3 Oct–2 Nov bill period that EMT prices at PEAK and that
# are STILL flagged (no cost returned) after the calm 7-day-window re-fetch.
# Stored as naive UTC (matches the DB block_start).
_CHECK_SLOTS = [
    "2025-10-18T10:30:00", "2025-10-18T11:00:00", "2025-10-18T13:00:00",
    "2025-10-18T15:00:00", "2025-10-18T17:00:00", "2025-10-18T17:30:00",
    "2025-10-19T05:30:00", "2025-10-19T06:00:00", "2025-10-19T06:30:00",
    "2025-10-19T07:00:00", "2025-10-19T07:30:00", "2025-10-24T20:00:00",
    "2025-10-24T20:30:00", "2025-10-25T11:00:00", "2025-10-25T11:30:00",
    "2025-10-25T12:00:00", "2025-10-25T12:30:00", "2025-10-27T22:30:00",
    "2025-10-27T23:00:00", "2025-10-28T19:30:00", "2025-10-28T22:30:00",
    "2025-10-29T19:00:00", "2025-10-29T20:00:00", "2025-10-29T22:00:00",
    "2025-10-31T18:30:00", "2025-10-31T22:30:00", "2025-11-01T23:00:00",
    "2025-11-02T05:30:00", "2025-11-02T06:00:00", "2025-11-02T06:30:00",
    "2025-11-02T07:00:00", "2025-11-02T07:30:00", "2025-11-02T08:00:00",
    "2025-11-02T08:30:00",
]


def _node_utc(startat):
    """Convert a returned node startAt (local ISO w/ offset) to naive-UTC string."""
    from datetime import datetime, timezone
    try:
        return (datetime.fromisoformat(startat)
                .astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S"))
    except Exception:
        return None


def probe_single_slots(token, account, mpan, slots):
    """THE decisive retrieval-vs-gap test. Fetch each flagged slot in its OWN
    single-slot query — the calmest possible request, isolated, no engine load.
    If the cost comes back here, the API always had it and our in-instance
    re-fetch was still too loaded (a retrieval bug we can fix). If it stays
    empty even here, the API genuinely has no cost for that slot."""
    from datetime import datetime, timedelta
    got = miss = 0
    for s in slots:
        start = s + "Z"
        end = (datetime.fromisoformat(s) + timedelta(minutes=60)
               ).strftime("%Y-%m-%dT%H:%M:%S") + "Z"
        nodes, _ = _paged_fetch(token, account, mpan, start, end, 4)
        hit = None
        for n in nodes:
            if _node_utc(n.get("startAt") or "") == s:
                hit = n
                break
        if hit is None:
            say(f"    {s}  — NO NODE returned"); miss += 1; continue
        stats = ((hit.get("metaData") or {}).get("statistics")) or []
        energy = [x for x in stats if x.get("type") != "STANDING_CHARGE_COST"]
        if energy:
            lbl = energy[0].get("label")
            ci = (energy[0].get("costInclTax") or {}).get("estimatedAmount")
            say(f"    {s}  kWh={hit.get('value')}  label={lbl}  cost={ci}  <== RECOVERED")
            got += 1
        else:
            say(f"    {s}  kWh={hit.get('value')}  still NO cost/label")
            miss += 1
    say(f"\n  → single-slot result: {got}/{len(slots)} RECOVERED a cost, "
        f"{miss}/{len(slots)} still empty")
    if got and not miss:
        say("  → VERDICT: pure retrieval bug — the API has every cost; the import "
            "just needs to fetch calmly. No CSV needed.")
    elif got:
        say("  → VERDICT: MIXED — recovered ones were a retrieval bug; the still-empty "
            "ones are a genuine API gap only the bill/CSV can price.")
    else:
        say("  → VERDICT: genuine API gap — even the calmest isolated query returns no "
            "cost for these. Only the bill/CSV carries their off/peak split.")


# Slots still showing mispriced after the October range repair — checked here in
# isolation to get each one's TRUE label + cost. Naive-UTC (Nov is GMT, so local
# == UTC; 31 Oct is post-DST GMT too). Override at runtime with --slots.
_GAP_SLOTS = [
    "2025-10-31T22:30:00",   # --deep earlier → OFF_PEAK 21.80p (mispriced at peak)
    "2025-11-02T07:30:00", "2025-11-02T08:00:00", "2025-11-02T08:30:00",
    "2025-11-02T09:00:00", "2025-11-02T09:30:00",
]

# Full-field measurement query: every field on the node so we can see if these
# 3 differ from their neighbours (source=estimated? duration off? cost fields
# present under a different shape?).
_MEAS_Q_FULL = (
    "query meas($acc:String!,$mpan:String!,$start:DateTime!,$end:DateTime!,"
    "$first:Int!,$after:String){"
    "  account(accountNumber:$acc){ properties{ measurements("
    "    first:$first, after:$after, startAt:$start, endAt:$end,"
    "    timezone:\"Europe/London\","
    "    utilityFilters:[{electricityFilters:{"
    "      readingFrequencyType:THIRTY_MIN_INTERVAL,"
    "      marketSupplyPointId:$mpan, readingDirection:CONSUMPTION}}]){"
    "    edges{ node{ value unit source readAt "
    "      ... on IntervalMeasurementType{ startAt endAt durationInSeconds "
    "        metaData{ statistics{ type label description value "
    "          costExclTax{ estimatedAmount costCurrency } "
    "          costInclTax{ estimatedAmount costCurrency } } } } } }"
    "    pageInfo{ hasNextPage endCursor } } } } }"
)


def probe_repeat(token, account, mpan, slots, n=12):
    """Measure the empty-stats DROP RATE for each slot, and whether it depends on
    window size. For each slot, fetch it `n` times at a SMALL (±1h) window and `n`
    times at a 12h-context window, counting how often the cost comes back vs empty.

    Point the probe at a DENSE slot (mid heavy charging run) and a QUIET slot (low
    kWh, no dispatch) via --slots: if the dense slot's drop rate is far higher, the
    stripping is complexity/density driven (→ leaner + smaller queries), not random.
    If the 12h window drops far more than ±1h, window size drives it too."""
    from datetime import datetime, timedelta
    for s in slots:
        say(f"\n  ══════ slot {s} (UTC) — drop rate over {n} fetches ══════")
        base = datetime.fromisoformat(s)
        end = (base + timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%S") + "Z"
        for label, lb_h in (("±1h  small window", 1), ("12h  context window", 12)):
            start = (base - timedelta(hours=lb_h)).strftime("%Y-%m-%dT%H:%M:%S") + "Z"
            got = empty = nonode = 0
            labels = set()
            for _ in range(n):
                nodes, _ = _paged_fetch(token, account, mpan, start, end, 200,
                                        _MEAS_Q_FULL)
                hit = next((x for x in nodes
                            if _node_utc(x.get("startAt") or "") == s), None)
                if hit is None:
                    nonode += 1
                    continue
                st = ((hit.get("metaData") or {}).get("statistics")) or []
                en = [x for x in st if x.get("type") != "STANDING_CHARGE_COST"]
                if en:
                    got += 1
                    labels.add(en[0].get("label"))
                else:
                    empty += 1
            rate = (empty + nonode) / n * 100.0
            say(f"    {label}: cost {got}/{n}, empty {empty}/{n}, no-node {nonode}/{n}"
                f"  → {rate:.0f}% miss   labels={sorted(labels)}")


def probe_deep_slots(token, account, mpan, slots):
    """Deep dive on the handful of slots that returned no cost even isolated.
    For each: dump every neighbouring node with FULL raw fields (source,
    duration, both cost fields) so we can see how the empty one differs, then
    re-fetch the exact slot 3x to confirm it's deterministic, not flaky."""
    from datetime import datetime, timedelta
    for s in slots:
        say(f"\n  ══════ slot {s} (UTC) ══════")
        d = datetime.fromisoformat(s)
        start = (d - timedelta(hours=2)).strftime("%Y-%m-%dT%H:%M:%S") + "Z"
        end = (d + timedelta(hours=2, minutes=30)).strftime("%Y-%m-%dT%H:%M:%S") + "Z"
        nodes, _ = _paged_fetch(token, account, mpan, start, end, 40, _MEAS_Q_FULL)
        say(f"    ±2h context window {start} … {end}: {len(nodes)} node(s)")
        seen_target = False
        for n in nodes:
            u = _node_utc(n.get("startAt") or "")
            if u == s:
                seen_target = True
            mark = "  <<<<< TARGET (was empty)" if u == s else ""
            stats = ((n.get("metaData") or {}).get("statistics")) or []
            energy = [x for x in stats if x.get("type") != "STANDING_CHARGE_COST"]
            lbls = [(x.get("type"), x.get("label"),
                     (x.get("costInclTax") or {}).get("estimatedAmount")) for x in energy]
            say(f"      {n.get('startAt')}  kWh={n.get('value')}  src={n.get('source')}"
                f"  dur={n.get('durationInSeconds')}  readAt={n.get('readAt')}"
                f"  energy={lbls}{mark}")
        if not seen_target:
            say("    !! target slot did NOT appear in the context window at all "
                "— the reading itself may be absent (a consumption gap), not just its cost")
        say("    single-slot re-fetch x3 (is 'empty' deterministic or flaky?):")
        for i in range(3):
            ss = s + "Z"
            se = (d + timedelta(minutes=30)).strftime("%Y-%m-%dT%H:%M:%S") + "Z"
            nn, _ = _paged_fetch(token, account, mpan, ss, se, 4, _MEAS_Q_FULL)
            hit = next((x for x in nn if _node_utc(x.get("startAt") or "") == s), None)
            if hit is None:
                say(f"      try {i+1}: NO node returned"); continue
            st = ((hit.get("metaData") or {}).get("statistics")) or []
            en = [x for x in st if x.get("type") != "STANDING_CHARGE_COST"]
            say(f"      try {i+1}: kWh={hit.get('value')} src={hit.get('source')} "
                f"dur={hit.get('durationInSeconds')} "
                f"energy={[(x.get('type'), x.get('label'), (x.get('costInclTax') or {}).get('estimatedAmount')) for x in en]}")


# How far BEFORE the target slot to start the query window, in minutes. The label
# should be STANDARD for small look-backs and flip to OFF_PEAK once the window
# reaches back far enough to include the dispatch run's anchor.
_CONTEXT_LOOKBACKS_MIN = [0, 30, 60, 120, 180, 360, 720, 1440, 2160, 2880]


def probe_context_window(token, account, mpan, slots):
    """PROVE the IOG dispatch label is WINDOW-CONTEXTUAL. For each slot, fetch it
    many times changing ONLY how far before the slot the window starts (the end is
    held fixed at slot+1h). If the label flips STANDARD → OFF_PEAK once the window
    reaches back to the dispatch run's start, the correct fix is a context-anchored
    window (not a single-slot fetch), and the flip point says how much look-back is
    needed. Deterministic per window (we already showed 3/3), so one read each."""
    from datetime import datetime as _dt, timedelta as _td
    for s in slots:
        say(f"\n  ══════ slot {s} (UTC) — label vs how far back the window starts ══════")
        try:
            base = _dt.fromisoformat(s)
        except ValueError:
            say("    (unparseable)"); continue
        end_z = (base + _td(minutes=60)).strftime("%Y-%m-%dT%H:%M:%S") + "Z"
        first = None
        flip = None
        for lb in _CONTEXT_LOOKBACKS_MIN:
            start = (base - _td(minutes=lb)).strftime("%Y-%m-%dT%H:%M:%S") + "Z"
            nodes, _ = _paged_fetch(token, account, mpan, start, end_z, 200,
                                    _MEAS_Q_FULL)
            hit = next((n for n in nodes
                        if _node_utc(n.get("startAt") or "") == s), None)
            if hit is None:
                say(f"      window start −{lb:>4} min  → no node"); continue
            stats = ((hit.get("metaData") or {}).get("statistics")) or []
            energy = [x for x in stats if x.get("type") != "STANDING_CHARGE_COST"]
            if energy:
                lbl = energy[0].get("label")
                ci = (energy[0].get("costInclTax") or {}).get("estimatedAmount")
                startlocal = (nodes[0].get("startAt") if nodes else "?")
                say(f"      window start −{lb:>4} min ({start[:16]}Z)  → "
                    f"{str(lbl):<14} cost={ci}")
                if first is None:
                    first = lbl
                if flip is None and lbl != first:
                    flip = lb
            else:
                say(f"      window start −{lb:>4} min  → (empty stats)")
        if flip is not None:
            say(f"    → FLIPPED at −{flip} min: single-slot/near windows give "
                f"'{first}', a window reaching ≥{flip} min back gives the other "
                f"label. CONTEXTUAL — proven.")
        elif first is not None:
            say(f"    → no flip across the range: label is '{first}' throughout "
                f"(not contextual for this slot — genuinely {first}).")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--api-key", default=os.environ.get("OCTOPUS_API_KEY"))
    ap.add_argument("--account", default=os.environ.get("OCTOPUS_ACCOUNT"))
    ap.add_argument("--mpan", default=None)
    ap.add_argument("--day", default="2025-10-21")
    ap.add_argument("--deep", action="store_true",
                    help="Skip sections 1-6; deep-dive ONLY the gap slots.")
    ap.add_argument("--context", action="store_true",
                    help="Prove the label is window-contextual: vary the window "
                         "start for each slot and watch STANDARD↔OFF_PEAK flip.")
    ap.add_argument("--repeat", type=int, default=0, metavar="N",
                    help="Measure the empty-stats DROP RATE: fetch each --slots "
                         "slot N times at a small and a 12h window. Compare a dense "
                         "vs a quiet slot to test if stripping is density-driven.")
    ap.add_argument("--slots", default=None,
                    help="Comma-separated naive-UTC slot starts to deep-dive "
                         "(overrides the built-in list). e.g. "
                         "2025-11-02T07:30,2025-11-02T08:00")
    a = ap.parse_args()
    if not a.api_key or not a.account:
        print("Need --api-key and --account (or OCTOPUS_API_KEY / OCTOPUS_ACCOUNT env)")
        sys.exit(2)

    print("=== Octopus cost-source probe (all GraphQL) ===")
    print(f"account={mask(a.account)}  day={a.day}\n")
    token = get_token(a.api_key)

    mpan, serial = a.mpan, None
    if not mpan:
        print("\n── discover import MPAN (GraphQL electricityAgreements) ──")
        mpans = discover_mpans(token, a.account)
        if not mpans:
            print("No MPANs found — pass --mpan explicitly."); sys.exit(1)
        mpan, serial = pick_import_mpan(token, a.account, mpans, a.day)
    if not mpan:
        print("Could not determine import MPAN."); sys.exit(1)
    print(f"✓ using import MPAN={mask(mpan)}\n")

    if a.deep or a.context or a.repeat:
        if a.slots:
            slots = []
            for s in a.slots.split(","):
                s = s.strip().replace(" ", "T")
                if len(s) == 16:      # 'YYYY-MM-DDTHH:MM' → add seconds
                    s += ":00"
                if s:
                    slots.append(s)
        else:
            slots = _GAP_SLOTS
        if a.repeat:
            print("── REPEAT TEST: empty-stats drop rate per slot / window size ──")
            probe_repeat(token, a.account, mpan, slots, n=a.repeat)
        elif a.context:
            print("── CONTEXT TEST: does the label depend on where the query "
                  "window starts? ──")
            probe_context_window(token, a.account, mpan, slots)
        else:
            print("── DEEP DIVE: true label + cost for each slot, in isolation ──")
            probe_deep_slots(token, a.account, mpan, slots)
        print("\n=== done — paste the whole output back ===")
        return

    print("── 1. Introspect measurement + statistics types "
          "(look for a cost field we don't request) ──")
    for tn in ("IntervalMeasurementType", "MeasurementInterface",
               "IntervalMeasurementMetaDataOutputType", "MeasurementStatisticOutputType",
               "StatisticOutput", "ConsumptionStatisticOutputType", "MeasurementsType"):
        introspect(token, tn)
    print()

    print("── 2. Full raw measurement nodes for the target evening "
          "(dispatched slots should show NO statistics) ──")
    dump_measurements(token, a.account, mpan, a.day)
    print()

    print("── 3. Account transactions (does the billed cost live here?) ──")
    dump_transactions(token, a.account)
    print()

    print("── 4. smartMeterTelemetry.costDelta (Mini per-reading cost) ──")
    dump_telemetry_cost(token, a.account, a.day)
    print()

    print("── 5. DECISIVE: does a large page / wide window strip the TOU bucket "
          "off dispatched slots? (import uses 60-day windows @ 500/page) ──")
    probe_window_effect(token, a.account, mpan, a.day)
    print()

    print("── 6. DECISIVE retrieval-vs-gap test: the 34 still-flagged bill-period "
          "slots, each in its OWN single-slot query (calmest possible) ──")
    probe_single_slots(token, a.account, mpan, _CHECK_SLOTS)
    print("\n=== done — paste the whole output back ===")


if __name__ == "__main__":
    # Force line-buffered stdout so output appears live and survives a crash
    # (block-buffered stdout to a pipe/container log is lost on an exception).
    try:
        sys.stdout.reconfigure(line_buffering=True)
    except Exception:
        pass
    say(f"probe starting — python {sys.version.split()[0]}")
    try:
        main()
    except SystemExit:
        raise
    except Exception:
        say("UNEXPECTED ERROR:")
        say(traceback.format_exc())
        sys.exit(1)