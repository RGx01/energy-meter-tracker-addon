#!/usr/bin/env python3
"""probe_dispatch_span.py — READ-ONLY: how far back does completedDispatches go, and can
we ask for a date range? This decides whether a NEW user can backfill the dispatch overlay
(and thus price out-of-core smart-charge bumps off-peak) for their imported history.

  python3 probe_dispatch_span.py --api-key YOUR_KEY --account A-42B0BCA7
"""
import argparse, asyncio, os, sys
from collections import Counter

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


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--api-key"); ap.add_argument("--account")
    args = ap.parse_args()
    key, acct_arg = _resolve_creds(args)
    if not key: print("ERROR: no API key."); sys.exit(2)
    client = KrakenAPIClient(key, **({"account_number": acct_arg} if acct_arg else {}))
    try:
        conn = await client.test_connection(acct_arg); acct = acct_arg or conn.get("account_number")

        # 1) Does completedDispatches accept date args? Introspect the Query field.
        qi = ('{ __type(name:"Query"){ fields{ name args{ name type{ name kind '
              'ofType{ name kind } } } } } }')
        try:
            d = await client._graphql(qi, {})
            for f in (d.get("__type") or {}).get("fields") or []:
                if f["name"] == "completedDispatches":
                    print("completedDispatches ARGS:",
                          [(a["name"], (a["type"].get("name")
                            or (a["type"].get("ofType") or {}).get("name")))
                           for a in (f.get("args") or [])])
                    break
        except Exception as e:
            print("(query introspection failed:", e, ")")

        # 2) What does the default call actually return? Print span + monthly counts.
        q = ("query($acc:String!){ completedDispatches(accountNumber:$acc){"
             " start end delta meta{ source } } }")
        data = await client._graphql(q, {"acc": acct})
        comp = data.get("completedDispatches") or []
        starts = sorted(x.get("start") for x in comp if x.get("start"))
        print(f"\ncompletedDispatches returned: {len(comp)} slot(s)")
        if starts:
            print(f"  earliest: {starts[0]}")
            print(f"  latest  : {starts[-1]}")
            months = Counter(s[:7] for s in starts)
            print("  by month:", dict(sorted(months.items())))
            span_days = (__import__('datetime').datetime.fromisoformat(starts[-1].replace('Z','+00:00'))
                         - __import__('datetime').datetime.fromisoformat(starts[0].replace('Z','+00:00'))).days
            print(f"  span: ~{span_days} days")
        print("\nDECISION: earliest ~90d ago -> new users CAN backfill dispatch → off-peak "
              "for their import window. Only a few days -> they cannot; bumps price peak "
              "beyond the dispatch window (documented limitation).")
    finally:
        await client.close()

if __name__ == "__main__":
    asyncio.run(main())
