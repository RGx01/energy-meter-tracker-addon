#!/usr/bin/env python3
"""probe_credits2.py — READ-ONLY: drill into Charge.consumption + Charge.detail to see
the rate breakdown Octopus stores for the electricity charge, and dump credit lines.
Follows probe_credits.py (which showed Credit has no consumption; Charge has consumption+detail).

  python3 probe_credits2.py --api-key YOUR_KEY --account A-42B0BCA7
"""
import argparse, asyncio, json, os, sys

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


def _base(t):
    while t and t.get("ofType"): t = t["ofType"]
    return (t or {})


async def _introspect(client, name):
    q = ('{ __type(name:"%s"){ name kind fields{ name type{ name kind ofType{ name kind '
         'ofType{ name kind } } } } } }' % name)
    try:
        return (await client._graphql(q, {})).get("__type")
    except Exception as e:
        print(f"  (introspect {name} failed: {e})"); return None


def _field_type_name(type_obj, field):
    for f in (type_obj.get("fields") if type_obj else []) or []:
        if f["name"] == field:
            return _base(f["type"]).get("name")
    return None


def _scalars(type_obj):
    out = []
    for f in (type_obj.get("fields") if type_obj else []) or []:
        if _base(f["type"]).get("kind") in ("SCALAR", "ENUM"):
            out.append(f["name"])
    return out


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--api-key"); ap.add_argument("--account"); ap.add_argument("--first", type=int, default=60)
    args = ap.parse_args()
    key, acct_arg = _resolve_creds(args)
    if not key: print("ERROR: no API key."); sys.exit(2)
    client = KrakenAPIClient(key, **({"account_number": acct_arg} if acct_arg else {}))
    try:
        conn = await client.test_connection(acct_arg); acct = acct_arg or conn.get("account_number")
        print(f"account={acct}\n")

        charge = await _introspect(client, "Charge")
        cons_t = _field_type_name(charge, "consumption")
        det_t  = _field_type_name(charge, "detail")
        print(f"Charge.consumption -> {cons_t}   Charge.detail -> {det_t}\n")
        cons = await _introspect(client, cons_t) if cons_t else None
        det  = await _introspect(client, det_t)  if det_t else None
        print(f"── {cons_t} fields ──")
        for f in (cons.get("fields") if cons else []) or []:
            print(f"   {f['name']}: {_base(f['type']).get('name') or _base(f['type']).get('kind')}")
        print(f"\n── {det_t} fields ──")
        for f in (det.get("fields") if det else []) or []:
            print(f"   {f['name']}: {_base(f['type']).get('name') or _base(f['type']).get('kind')}")

        cons_sel = " ".join(_scalars(cons)) or "__typename"
        det_sel  = " ".join(_scalars(det))  or "__typename"
        q = ("query($acc:String!,$n:Int!){ account(accountNumber:$acc){ transactions(first:$n){"
             " edges{ node{ postedDate amount isCredit title note __typename"
             " ... on Charge{ isExport consumption{ " + cons_sel + " } detail{ " + det_sel + " } }"
             " } } } } }")
        d = await client._graphql(q, {"acc": acct, "n": args.first})
        txns = [e["node"] for e in
                (((d.get("account") or {}).get("transactions") or {}).get("edges") or [])]
        print(f"\n── {len(txns)} transactions (newest first) ──")
        for t in txns:
            print(f"{t.get('postedDate')}  {t.get('__typename'):<8} amt={t.get('amount')} "
                  f"credit={t.get('isCredit')}  '{t.get('title')}'"
                  + (f"  note='{t.get('note')}'" if t.get('note') else ""))
        # full dump of the newest import electricity Charge + any credit-ish lines
        charges = [t for t in txns if t.get("__typename") == "Charge" and not t.get("isExport")]
        print("\n── newest electricity CHARGE (full) ──")
        if charges: print(json.dumps(charges[0], indent=2))
        hits = [t for t in txns if any(k in ((t.get("title") or "")+(t.get("note") or "")).lower()
                for k in ("smart","off","peak","saving","dispatch","intelligent","charge cred","credit"))]
        print(f"\n── credit-ish lines: {len(hits)} ──")
        for t in hits: print(json.dumps(t, indent=2))
        print("\nDECISION: look in the detail/consumption fields above for a per-RATE or "
              "per-SLOT breakdown (start/end/quantity/rate). Per-slot -> historical fix. "
              "Only period totals by rate -> aggregate, not per-slot recoverable.")
    finally:
        await client.close()

if __name__ == "__main__":
    asyncio.run(main())
