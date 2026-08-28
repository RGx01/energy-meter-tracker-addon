#!/usr/bin/env python3
"""
probe_free_session.py  —  READ-ONLY Octopus billing probe.

Pulls Octopus's OWN authoritative per-slot billed cost + OFF_PEAK/STANDARD label
for a window, straight from the GraphQL Measurements API, using the add-on's
shipped KrakenAPIClient. Nothing is written to any DB — this only PRINTS what
Octopus currently has those slots priced at.

Default window is the 23/08/2026 Free-Electricity session
(10:00–11:00 UTC = 11:00–12:00 BST), with a look-back so the recover ladder can
resolve the OFF_PEAK label for an out-of-window morning dispatch.

CREDENTIALS — three ways, first match wins:
  1. CLI flags:   --api-key … --account …            (run from ANYWHERE)
  2. Env vars:    KRAKEN_API_KEY, KRAKEN_ACCOUNT_NUMBER
  3. In-container: the add-on's own creds file, via engine._kraken_env()

Only kraken_api_client.py needs to be importable. Point EMT_ROOT at the folder
holding it (and engine.py) if you're not running from the add-on/repo root.

EXAMPLES
  # outside the container, creds on the command line:
  python3 probe_free_session.py --api-key sk_live_xxx --account A-1234ABCD

  # custom window (naive-UTC iso start/end of the slots you want priced):
  python3 probe_free_session.py --api-key … --account … \
      --start 2026-08-23T10:00:00 --end 2026-08-23T11:00:00

  # skip auto-discovery by giving the import MPAN directly:
  python3 probe_free_session.py --api-key … --account … --mpan 1234567890123

  # inside the container (uses the add-on creds file automatically):
  python3 probe_free_session.py
"""
import argparse
import asyncio
import os
import sys
from datetime import datetime, timedelta

# --- make the add-on modules importable ---------------------------------------
_ROOT = os.environ.get("EMT_ROOT") or os.path.dirname(os.path.abspath(__file__))
for _p in (_ROOT, os.getcwd()):
    if _p and _p not in sys.path:
        sys.path.insert(0, _p)

try:
    from kraken_api_client import KrakenAPIClient
except Exception as e:  # pragma: no cover
    print(f"ERROR: could not import kraken_api_client from {_ROOT!r}: {e}")
    print("Put this next to kraken_api_client.py, or set EMT_ROOT=/path/to/addon.")
    sys.exit(2)

DEFAULT_START = "2026-08-23T10:00:00"   # 11:00 BST — free-session slot 1
DEFAULT_END   = "2026-08-23T11:00:00"   # covers 10:00 and 10:30 slots


def _resolve_creds(args):
    """CLI flag → env var → in-container add-on creds file. Returns (key, acct)."""
    key = args.api_key or os.environ.get("KRAKEN_API_KEY")
    acct = args.account or os.environ.get("KRAKEN_ACCOUNT_NUMBER")
    if key:
        return key.strip(), (acct.strip() if acct else None)
    # Last resort: the add-on's own resolver (only works inside the container).
    try:
        import engine
        env = engine._kraken_env()
        if env.get("api_key"):
            return env["api_key"], env.get("account_number")
    except Exception:
        pass
    return None, acct


def _slots_between(start_iso: str, end_iso: str):
    s = datetime.fromisoformat(start_iso)
    e = datetime.fromisoformat(end_iso)
    out, cur = [], s
    while cur < e:
        out.append(cur.strftime("%Y-%m-%dT%H:%M:%S"))
        cur += timedelta(minutes=30)
    return out


async def main():
    ap = argparse.ArgumentParser(
        description="READ-ONLY probe of Octopus's authoritative per-slot price.")
    ap.add_argument("--api-key", help="Kraken/Octopus API key (or KRAKEN_API_KEY).")
    ap.add_argument("--account", help="Account number, e.g. A-1234ABCD "
                                      "(or KRAKEN_ACCOUNT_NUMBER).")
    ap.add_argument("--mpan", help="Import MPAN. If omitted, auto-discovered.")
    ap.add_argument("--start", default=DEFAULT_START,
                    help=f"Slot window start, naive-UTC iso (default {DEFAULT_START}).")
    ap.add_argument("--end", default=DEFAULT_END,
                    help=f"Slot window end, naive-UTC iso (default {DEFAULT_END}).")
    ap.add_argument("--lookback", type=int, default=4,
                    help="Hours of context before the window for the label ladder "
                         "(default 4).")
    # tolerate old positional call style: prog START END
    ap.add_argument("pos", nargs="*", help=argparse.SUPPRESS)
    args = ap.parse_args()
    if args.pos:
        if len(args.pos) >= 1:
            args.start = args.pos[0]
        if len(args.pos) >= 2:
            args.end = args.pos[1]

    slots = _slots_between(args.start, args.end)
    api_key, acct_arg = _resolve_creds(args)
    if not api_key:
        print("ERROR: no API key. Pass --api-key, set KRAKEN_API_KEY, or run "
              "inside the add-on container.")
        sys.exit(2)

    kwargs = {}
    if acct_arg:
        kwargs["account_number"] = acct_arg
    client = KrakenAPIClient(api_key, **kwargs)
    try:
        conn = await client.test_connection(acct_arg)
        acct = acct_arg or conn.get("account_number")

        mpan = args.mpan
        if not mpan:
            disc = await client.auto_discover(acct)
            imp = disc.get("import") or {}
            mpan = imp.get("mpan")
            tariff, product = imp.get("tariff_code"), imp.get("product_code")
        else:
            tariff = product = "(supplied)"
        if not mpan:
            print("ERROR: could not resolve import MPAN — pass --mpan explicitly.")
            sys.exit(2)

        print(f"account={acct}  import_mpan=…{str(mpan)[-4:]}  "
              f"tariff={tariff}  product={product}")
        print(f"window (with {args.lookback}h look-back): "
              f"{args.start}Z .. {args.end}Z   slots={slots}\n")

        # 1) Bulk read over the context window (may strip stats on a dense
        #    charging run — the recover ladder below is the reliable path).
        ws = (datetime.fromisoformat(args.start) - timedelta(hours=args.lookback)
              ).strftime("%Y-%m-%dT%H:%M:%S") + "Z"
        we = args.end + "Z"
        bulk = await client.get_measurements(mpan, ws, we, account_number=acct,
                                             direction="CONSUMPTION")
        bulk_by = {r["start"]: r for r in bulk}

        # 2) Authoritative per-slot recovery (look-back ladder → real cost+label).
        recovered = await client.recover_measurement_costs(
            mpan, slots, account_number=acct, direction="CONSUMPTION")

        print(f"{'slot (UTC)':<21} {'kWh':>7} {'cost £':>9} {'p/kWh':>7} "
              f"{'off_peak':>9}  buckets")
        print("-" * 74)
        for s in slots:
            node = recovered.get(s) or bulk_by.get(s)
            if not node:
                print(f"{s:<21} {'—':>7} {'(no data returned)':>27}")
                continue
            kwh = node.get("kwh") or 0.0
            cost = node.get("cost_incl")
            ppk = (cost / kwh * 100.0) if (cost is not None and kwh) else None
            print(f"{s:<21} {kwh:>7.3f} "
                  f"{('%.4f' % cost) if cost is not None else 'n/a':>9} "
                  f"{('%.3f' % ppk) if ppk is not None else 'n/a':>7} "
                  f"{str(node.get('off_peak')):>9}  {node.get('buckets')}")

        got = [g for g in (recovered.get(s) or bulk_by.get(s) for s in slots) if g]
        tk = sum((g.get("kwh") or 0.0) for g in got)
        tc = sum((g.get("cost_incl") or 0.0) for g in got
                 if g.get("cost_incl") is not None)
        print("-" * 74)
        print(f"{'SESSION TOTAL':<21} {tk:>7.3f} {tc:>9.4f} "
              f"{(tc/tk*100.0 if tk else 0):>7.3f}")
        labels = {str(g.get("off_peak")) for g in got}
        print(f"\nOctopus label verdict: {labels}  "
              f"(True=OFF_PEAK, False=STANDARD/peak, None=mixed/unknown)")
        print("This is what Octopus authoritatively bills those slots at — "
              "compare against EMT's block imp_rate.")
    finally:
        await client.close()


if __name__ == "__main__":
    asyncio.run(main())