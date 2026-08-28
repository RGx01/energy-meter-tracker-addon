#!/usr/bin/env python3
"""
probe_slots.py  —  READ-ONLY per-slot Octopus measurement dump.

For each slot you name, prints BOTH the bulk `get_measurements` read AND the
`recover_measurement_costs` look-back-ladder result, side by side, with the raw
per-slot `buckets`. Nothing is written anywhere. This is the definitive check for
"did Octopus bill this slot off-peak or standard, and does the recover ladder agree
with the bulk read?" — i.e. is a measure_audit band-flip real, or a fetch artifact.

Defaults to the slots the 4.5.5 measure_audit flagged. Override with --slots.

CREDENTIALS — first match wins (same as the other probes):
  1. CLI:   --api-key … --account …
  2. Env:   KRAKEN_API_KEY, KRAKEN_ACCOUNT_NUMBER
  3. In-container: the add-on's own creds file (engine._kraken_env()).

Only kraken_api_client.py needs to be importable (set EMT_ROOT if not alongside).

  # inside the add-on container (uses the add-on creds automatically):
  python3 probe_slots.py

  # explicit slots + creds from anywhere:
  python3 probe_slots.py --api-key sk_live_xxx --account A-1234ABCD \
      --slots 2026-08-14T19:00:00,2026-08-24T21:00:00
"""
import argparse
import asyncio
import os
import sys
from datetime import datetime, timedelta

_ROOT = os.environ.get("EMT_ROOT") or os.path.dirname(os.path.abspath(__file__))
for _p in (_ROOT, os.getcwd()):
    if _p and _p not in sys.path:
        sys.path.insert(0, _p)

try:
    from kraken_api_client import KrakenAPIClient
except Exception as e:  # pragma: no cover
    print(f"ERROR: could not import kraken_api_client from {_ROOT!r}: {e}")
    sys.exit(2)

# The slots 4.5.5 measure_audit flagged (2 band-flips + the label-only cases).
DEFAULT_SLOTS = [
    "2026-08-14T19:00:00",   # band-flip: EMT off-peak £0.1812 vs measured £1.0659 (the big one)
    "2026-08-24T21:00:00",   # band-flip: EMT £0.0089 vs measured £0.0523
    "2026-08-04T18:00:00",   # label-only peak->off_peak (cost agrees)
    "2026-08-24T05:00:00",   # label-only off_peak->peak (cost agrees)
    "2026-08-25T23:00:00",   # label-only off_peak->peak (cost agrees)
    "2026-08-25T23:30:00",   # label-only off_peak->peak (cost agrees)
]


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


def _ppk(cost, kwh):
    return (cost / kwh * 100.0) if (cost is not None and kwh) else None


def _fmt(node):
    if not node:
        return "—"
    kwh = node.get("kwh") or 0.0
    ci = node.get("cost_incl")
    ppk = _ppk(ci, kwh)
    return (f"kWh={kwh:>6.3f}  £{('%.4f' % ci) if ci is not None else 'n/a':>8}  "
            f"p/kWh={('%.3f' % ppk) if ppk is not None else 'n/a':>7}  "
            f"off_peak={str(node.get('off_peak')):>5}  buckets={node.get('buckets')}")


async def main():
    ap = argparse.ArgumentParser(description="READ-ONLY per-slot measurement dump.")
    ap.add_argument("--api-key")
    ap.add_argument("--account")
    ap.add_argument("--mpan", help="Import MPAN (else auto-discovered).")
    ap.add_argument("--slots", help="Comma-separated naive-UTC iso slots "
                                    "(default: the measure_audit-flagged set).")
    ap.add_argument("--lookback", type=int, default=6,
                    help="Hours of context before each slot for the bulk read + "
                         "the recover ladder (default 6).")
    args = ap.parse_args()

    slots = ([s.strip() for s in args.slots.split(",") if s.strip()]
             if args.slots else list(DEFAULT_SLOTS))
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
        mpan = args.mpan
        tariff = product = "(supplied)"
        if not mpan:
            disc = await client.auto_discover(acct)
            imp = disc.get("import") or {}
            mpan = imp.get("mpan")
            tariff, product = imp.get("tariff_code"), imp.get("product_code")
        if not mpan:
            print("ERROR: could not resolve import MPAN — pass --mpan.")
            sys.exit(2)
        print(f"account={acct}  import_mpan=…{str(mpan)[-4:]}  tariff={tariff}\n")

        for slot in slots:
            try:
                s = datetime.fromisoformat(slot)
            except Exception:
                print(f"[{slot}] bad iso, skipped\n"); continue
            ws = (s - timedelta(hours=args.lookback)).strftime("%Y-%m-%dT%H:%M:%S") + "Z"
            we = (s + timedelta(minutes=30)).strftime("%Y-%m-%dT%H:%M:%S") + "Z"

            bulk = await client.get_measurements(mpan, ws, we, account_number=acct,
                                                 direction="CONSUMPTION")
            bulk_by = {r["start"]: r for r in bulk}
            recovered = await client.recover_measurement_costs(
                mpan, [slot], account_number=acct, direction="CONSUMPTION")

            print(f"=== {slot}  (BST {(s + timedelta(hours=1)).strftime('%H:%M')}) ===")
            print(f"   bulk get_measurements : {_fmt(bulk_by.get(slot))}")
            print(f"   recover ladder        : {_fmt(recovered.get(slot))}")
            # neighbours from the bulk read — helps spot a ladder mis-assignment
            near = sorted(k for k in bulk_by if k != slot)
            if near:
                print(f"   (window had {len(bulk)} slot(s); "
                      f"nearest others: {near[-3:]})")
            print()

        print("READ: if 'bulk' and 'recover ladder' AGREE and show STANDARD/high p/kWh,\n"
              "Octopus genuinely billed it peak (EMT was optimistic). If they DISAGREE,\n"
              "the recover ladder mis-priced it and EMT's off-peak is right. Compare the\n"
              "p/kWh against 5.493 (off-peak) vs 32.309 (peak).")
    finally:
        await client.close()


if __name__ == "__main__":
    asyncio.run(main())
