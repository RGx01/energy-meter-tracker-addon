#!/usr/bin/env python3
"""probe_slot_extras.py — READ-ONLY: dump metaData.extras + statistic.description for one
slot, and introspect ExtrasOutput. The measurements statistics only carry a gross
STANDARD_RATE bucket for a dispatched slot (no OFF_PEAK, no credit) — so the smart-charge /
off-peak signal has likely moved into `extras`, which EMT never fetched. This finds it.

  python3 probe_slot_extras.py                          # 21/07 bump (STANDARD_RATE)
  python3 probe_slot_extras.py --slot 2026-08-06T04:30:00   # a known off-peak slot to contrast

NOTE: the API returns startAt in LOCAL time (+01:00 in BST). --slot is naive-UTC; the probe
matches on the +1h local equivalent too. Nothing is written.
"""
import argparse, asyncio, json, os, sys
from datetime import datetime, timedelta

_ROOT = os.environ.get("EMT_ROOT") or os.path.dirname(os.path.abspath(__file__))
for _p in (_ROOT, os.getcwd()):
    if _p and _p not in sys.path: sys.path.insert(0, _p)
from kraken_api_client import KrakenAPIClient


def _resolve_creds(args):
    key = args.api_key or os.environ.get("KRAKEN_API_KEY")
    acct = args.account or os.environ.get("KRAKEN_ACCOUNT_NUMBER")
    if key: return key.strip(), (acct.strip() if acct else None)
    try:
        import engine; env = engine._kraken_env()
        if env.get("api_key"): return env["api_key"], env.get("account_number")
    except Exception: pass
    return None, acct


def _scalar(t):
    """Return a leaf field name if it's a scalar/enum (fetchable with no sub-selection)."""
    while t and t.get("ofType"): t = t["ofType"]
    return (t or {}).get("kind") in ("SCALAR", "ENUM")


async def _introspect(client, name):
    q = ('{ __type(name:"%s"){ name kind fields{ name type{ name kind '
         'ofType{ name kind ofType{ name kind ofType{ name kind } } } } } } }' % name)
    try:
        return (await client._graphql(q, {})).get("__type")
    except Exception as e:
        print(f"  (introspect {name} failed: {e})"); return None


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--api-key"); ap.add_argument("--account"); ap.add_argument("--mpan")
    ap.add_argument("--slot", default="2026-07-21T15:00:00"); ap.add_argument("--lookback", type=int, default=2)
    args = ap.parse_args()
    key, acct_arg = _resolve_creds(args)
    if not key: print("ERROR: no API key."); sys.exit(2)
    client = KrakenAPIClient(key, **({"account_number": acct_arg} if acct_arg else {}))
    try:
        conn = await client.test_connection(acct_arg); acct = acct_arg or conn.get("account_number")
        mpan = args.mpan or (await client.auto_discover(acct)).get("import", {}).get("mpan")

        print("── SCHEMA: ExtrasOutput ──")
        ex = await _introspect(client, "ExtrasOutput")
        extra_fields = []
        for f in (ex.get("fields") if ex else []) or []:
            leaf = _scalar(f["type"]); extra_fields.append((f["name"], leaf))
            print(f"   {f['name']}  (scalar={leaf})")
        # Build a selection: scalar extras fields directly; non-scalar -> skip (report only)
        sel_extras = " ".join(n for n, leaf in extra_fields if leaf) or "__typename"

        s = datetime.fromisoformat(args.slot)
        ws = (s - timedelta(hours=args.lookback)).strftime("%Y-%m-%dT%H:%M:%S") + "Z"
        we = (s + timedelta(minutes=30)).strftime("%Y-%m-%dT%H:%M:%S") + "Z"
        q = ("query($acc:String!,$mpan:String!,$start:DateTime!,$end:DateTime!){"
             " account(accountNumber:$acc){ properties{ measurements(first:200,startAt:$start,"
             " endAt:$end,timezone:\"Europe/London\",utilityFilters:[{electricityFilters:{"
             " readingFrequencyType:THIRTY_MIN_INTERVAL,marketSupplyPointId:$mpan,"
             " readingDirection:CONSUMPTION}}]){ edges{ node{ value ... on IntervalMeasurementType{"
             " startAt metaData{ statistics{ type label description value"
             " costInclTax{estimatedAmount} } extras{ " + sel_extras + " } } } } } } } } }")
        d = await client._graphql(q, {"acc": acct, "mpan": mpan, "start": ws, "end": we})
        props = ((d.get("account") or {}).get("properties")) or []
        edges = next((m["edges"] for p in props for m in [p.get("measurements")] if m and m.get("edges")), [])
        # target = the +1h local equivalent of the naive-UTC slot, OR the non-zero-kWh node
        local_target = (s + timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%S")
        print(f"\n── nodes {ws}..{we} (target local {local_target}) ──")
        for e in edges:
            n = e.get("node") or {}
            star = " <<< TARGET" if (n.get("startAt","").startswith(local_target)
                                     or float(n.get("value") or 0) > 0) else ""
            print(f"{n.get('startAt')}  value={n.get('value')}{star}")
            print("   " + json.dumps(n.get("metaData"), indent=2).replace("\n", "\n   "))
    finally:
        await client.close()

if __name__ == "__main__":
    asyncio.run(main())
