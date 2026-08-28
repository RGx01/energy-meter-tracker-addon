#!/usr/bin/env python3
"""
probe_agreements.py  —  READ-ONLY tariff-agreement timeline dump.

Prints your account's electricity agreement history — every tariff_code with its
valid_from / valid_to — for the import and export meter points, straight from the
account via the add-on's shipped KrakenAPIClient. This shows exactly which tariff
applied when (e.g. the pre-migration IOG fix vs the new SMB), so we can check rate
continuity across the migration. Nothing is written anywhere.

CREDENTIALS — first match wins:
  1. CLI:   --api-key … --account …
  2. Env:   KRAKEN_API_KEY, KRAKEN_ACCOUNT_NUMBER
  3. In-container: the add-on's own creds file (engine._kraken_env()).

Only kraken_api_client.py needs to be importable (set EMT_ROOT if not alongside).

  python3 probe_agreements.py --api-key sk_live_xxx --account A-1234ABCD
"""
import argparse
import asyncio
import os
import sys

_ROOT = os.environ.get("EMT_ROOT") or os.path.dirname(os.path.abspath(__file__))
for _p in (_ROOT, os.getcwd()):
    if _p and _p not in sys.path:
        sys.path.insert(0, _p)

try:
    from kraken_api_client import KrakenAPIClient
except Exception as e:  # pragma: no cover
    print(f"ERROR: could not import kraken_api_client from {_ROOT!r}: {e}")
    sys.exit(2)


def _resolve_creds(args):
    key = args.api_key or os.environ.get("KRAKEN_API_KEY")
    acct = args.account or os.environ.get("KRAKEN_ACCOUNT_NUMBER")
    if key:
        return key.strip(), (acct.strip() if acct else None)
    try:
        import engine
        env = engine._kraken_env()
        if env.get("api_key"):
            return env["api_key"], env.get("account_number")
    except Exception:
        pass
    return None, acct


def _dump(label, info):
    print(f"\n=== {label} meter point ===")
    if not info:
        print("  (none)")
        return
    print(f"  current tariff : {info.get('tariff_code')}  (product {info.get('product_code')})")
    ags = info.get("agreements") or []
    if not ags:
        print("  agreements     : (none returned)")
        return
    # sort oldest→newest by valid_from
    ags = sorted(ags, key=lambda a: a.get("valid_from") or "")
    print(f"  agreements ({len(ags)}), oldest → newest:")
    print(f"    {'tariff_code':44}  {'valid_from':26}  valid_to")
    print(f"    {'-'*44}  {'-'*26}  {'-'*26}")
    for a in ags:
        tc = a.get("tariff_code") or a.get("tariffCode") or "?"
        vf = a.get("valid_from") or a.get("validFrom") or "?"
        vt = a.get("valid_to") or a.get("validTo") or "(open)"
        print(f"    {str(tc):44}  {str(vf):26}  {vt}")


async def main():
    ap = argparse.ArgumentParser(description="READ-ONLY tariff-agreement timeline dump.")
    ap.add_argument("--api-key")
    ap.add_argument("--account")
    args = ap.parse_args()

    api_key, acct_arg = _resolve_creds(args)
    if not api_key:
        print("ERROR: no API key. Pass --api-key, set KRAKEN_API_KEY, or run in-container.")
        sys.exit(2)

    kwargs = {}
    if acct_arg:
        kwargs["account_number"] = acct_arg
    client = KrakenAPIClient(api_key, **kwargs)
    try:
        conn = await client.test_connection(acct_arg)
        acct = acct_arg or conn.get("account_number")
        disc = await client.auto_discover(acct)
        print(f"account={acct}")
        _dump("IMPORT", disc.get("import"))
        _dump("EXPORT", disc.get("export"))
        print("\nPaste this back — I'll fetch each tariff's day/night rates from the "
              "public product API and check whether the fix held across the migration.")
    finally:
        await client.close()


if __name__ == "__main__":
    asyncio.run(main())
