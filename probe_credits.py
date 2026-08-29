#!/usr/bin/env python3
"""probe_credits.py — READ-ONLY: find the smart-charge OFF-PEAK credit in Octopus's
billing data. The Measurements API returns the GROSS standard rate for a dispatched
bump (no OFF_PEAK, no credit); the off-peak is applied elsewhere. This probes the
account TRANSACTIONS / statement ledger to see (a) whether the smart-charge credit is
exposed via the API and (b) whether it's per-slot attributable or a per-period lump.

  python3 probe_credits.py                 # dumps recent transactions + schema
  python3 probe_credits.py --first 80

Nothing is written. Creds: --api-key/--account, else KRAKEN_* env, else the add-on's.
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


async def _introspect(client, name):
    q = ('{ __type(name:"%s"){ name kind fields{ name type{ name kind ofType{ name kind '
         'ofType{ name kind } } } } possibleTypes{ name } enumValues{ name } } }' % name)
    try:
        return (await client._graphql(q, {})).get("__type")
    except Exception as e:
        print(f"  (introspect {name} failed: {e})"); return None


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

        # 1) Introspect the transaction interface + its concrete types (Charge/Credit/...).
        print("── SCHEMA: TransactionType (interface) ──")
        tt = await _introspect(client, "TransactionType")
        impls = [p["name"] for p in (tt.get("possibleTypes") if tt else []) or []]
        for f in (tt.get("fields") if tt else []) or []:
            print(f"   {f['name']}")
        print("   implementations:", impls or "(none / not an interface)")
        for it in impls:
            sub = await _introspect(client, it)
            fs = [f["name"] for f in (sub.get("fields") if sub else []) or []]
            print(f"   ── {it} fields: {fs}")

        # 2) Dump recent transactions with a broad field set (+ Charge consumption window).
        q = ("query($acc:String!,$n:Int!){ account(accountNumber:$acc){"
             " transactions(first:$n){ edges{ node{"
             " id postedDate createdAt amount balanceCarriedForward isCredit isDebit isHeld"
             " title note __typename"
             " ... on Charge{ consumption{ startDate endDate quantity usageCost supplyCharge } isExport }"
             " } } } } }")
        try:
            d = await client._graphql(q, {"acc": acct, "n": args.first})
            txns = [e["node"] for e in
                    (((d.get("account") or {}).get("transactions") or {}).get("edges") or [])]
            print(f"\n── {len(txns)} transactions (newest first) ──")
            for t in txns:
                amt = t.get("amount")
                print(f"{t.get('postedDate') or t.get('createdAt')}  {t.get('__typename'):<10} "
                      f"amt={amt}  credit={t.get('isCredit')}  '{t.get('title')}'"
                      + (f"  note='{t.get('note')}'" if t.get('note') else ""))
                c = t.get("consumption")
                if c: print(f"      consumption: {c}")
            # highlight anything that looks like a smart-charge / off-peak credit
            hits = [t for t in txns if any(k in ((t.get("title") or "")+(t.get("note") or "")).lower()
                    for k in ("smart","charge","off","peak","saving","dispatch","intelligent","credit"))]
            print(f"\n── candidate smart-charge/off-peak lines: {len(hits)} ──")
            for t in hits:
                print("   " + json.dumps(t, indent=2).replace("\n","\n   "))
        except Exception as e:
            print(f"\ntransactions query failed (a field may not exist — see schema above): {e}")
        print("\nDECISION: if a credit line carries a per-interval consumption window "
              "(startDate/endDate 30-min), it's per-slot attributable → the historical fix. "
              "If it's a single period lump with no slot detail, it is NOT per-slot recoverable.")
    finally:
        await client.close()

if __name__ == "__main__":
    asyncio.run(main())
