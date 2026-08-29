#!/usr/bin/env python3
"""probe_slot_meta.py — READ-ONLY: dump ALL Measurements metadata Octopus exposes for
one half-hour slot, plus a schema introspection of the statistics type.

We built the measured-cost feature on the assumption that Octopus labels a dispatched
slot OFF_PEAK. For 2026-07-21T15:00:00 it now returns STANDARD_RATE, so the off-peak /
smart-charge-credit signal has moved. This probe finds where it went: it introspects the
measurement node + statistics types (so we see EVERY available field, not just the ones
EMT currently reads) and dumps the raw node for the slot.

Nothing is written. Creds: --api-key/--account, else KRAKEN_* env, else the add-on's own.

  python3 probe_slot_meta.py                       # defaults to 2026-07-21T15:00:00
  python3 probe_slot_meta.py --slot 2026-08-06T04:30:00   # compare a known off-peak slot
"""
import argparse, asyncio, json, os, sys

_ROOT = os.environ.get("EMT_ROOT") or os.path.dirname(os.path.abspath(__file__))
for _p in (_ROOT, os.getcwd()):
    if _p and _p not in sys.path:
        sys.path.insert(0, _p)
try:
    from kraken_api_client import KrakenAPIClient
except Exception as e:
    print(f"ERROR: import kraken_api_client from {_ROOT!r}: {e}"); sys.exit(2)


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


def _unwrap(t):
    """GraphQL type -> readable '[NonNull(Foo)]' style + the base scalar/object name."""
    if not t:
        return "?", None
    name, kind = t.get("name"), t.get("kind")
    if kind in ("NON_NULL", "LIST") and t.get("ofType"):
        inner, base = _unwrap(t["ofType"])
        return (f"{inner}!" if kind == "NON_NULL" else f"[{inner}]"), base
    return name or kind, name


async def _introspect(client, type_name):
    q = ('{ __type(name:"%s"){ name kind fields{ name '
         'type{ name kind ofType{ name kind ofType{ name kind ofType{ name kind } } } } } } }'
         % type_name)
    try:
        d = await client._graphql(q, {})
        return (d.get("__type") or None)
    except Exception as e:
        print(f"  (introspection of {type_name} failed: {e})")
        return None


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--api-key"); ap.add_argument("--account"); ap.add_argument("--mpan")
    ap.add_argument("--slot", default="2026-07-21T15:00:00")
    ap.add_argument("--lookback", type=int, default=4, help="hours before the slot to fetch")
    args = ap.parse_args()
    key, acct_arg = _resolve_creds(args)
    if not key:
        print("ERROR: no API key (—api-key / KRAKEN_API_KEY / in-container)."); sys.exit(2)
    client = KrakenAPIClient(key, **({"account_number": acct_arg} if acct_arg else {}))
    try:
        conn = await client.test_connection(acct_arg)
        acct = acct_arg or conn.get("account_number")
        mpan = args.mpan
        if not mpan:
            disc = await client.auto_discover(acct)
            mpan = (disc.get("import") or {}).get("mpan")
        print(f"account={acct} mpan=…{str(mpan)[-4:]} slot={args.slot}\n")

        # 1) Introspect node -> metaData -> statistics, printing every field available.
        print("── SCHEMA: IntervalMeasurementType ──")
        node_t = await _introspect(client, "IntervalMeasurementType")
        meta_base = None
        if node_t:
            for f in node_t.get("fields") or []:
                disp, base = _unwrap(f["type"])
                print(f"   {f['name']}: {disp}")
                if f["name"] == "metaData":
                    meta_base = base
        if meta_base:
            print(f"\n── SCHEMA: {meta_base} (metaData) ──")
            meta_t = await _introspect(client, meta_base)
            stat_base = None
            for f in (meta_t.get("fields") if meta_t else []) or []:
                disp, base = _unwrap(f["type"])
                print(f"   {f['name']}: {disp}")
                if f["name"] == "statistics":
                    stat_base = base
            if stat_base:
                print(f"\n── SCHEMA: {stat_base} (statistics element) — THE KEY ONE ──")
                stat_t = await _introspect(client, stat_base)
                for f in (stat_t.get("fields") if stat_t else []) or []:
                    disp, _ = _unwrap(f["type"])
                    print(f"   {f['name']}: {disp}")

        # 2) Raw data dump for the slot with the fields EMT currently reads + a wide net.
        from datetime import datetime, timedelta
        s = datetime.fromisoformat(args.slot)
        ws = (s - timedelta(hours=args.lookback)).strftime("%Y-%m-%dT%H:%M:%S") + "Z"
        we = (s + timedelta(minutes=30)).strftime("%Y-%m-%dT%H:%M:%S") + "Z"
        q = (
            "query($acc:String!,$mpan:String!,$start:DateTime!,$end:DateTime!){"
            " account(accountNumber:$acc){ properties{ measurements(first:200,"
            " startAt:$start,endAt:$end,timezone:\"Europe/London\","
            " utilityFilters:[{electricityFilters:{readingFrequencyType:THIRTY_MIN_INTERVAL,"
            " marketSupplyPointId:$mpan,readingDirection:CONSUMPTION}}]){"
            " edges{ node{ value unit ... on IntervalMeasurementType{ startAt endAt"
            " metaData{ statistics{ type label value costInclTax{estimatedAmount costCurrency}"
            " costExclTax{estimatedAmount} } } } } } } } } }")
        try:
            d = await client._graphql(q, {"acc": acct, "mpan": mpan, "start": ws, "end": we})
            props = ((d.get("account") or {}).get("properties")) or []
            edges = []
            for p in props:
                m = p.get("measurements")
                if m and m.get("edges"):
                    edges = m["edges"]; break
            print(f"\n── RAW nodes {ws}..{we} ({len(edges)} slot(s)) ──")
            for e in edges:
                n = e.get("node") or {}
                if n.get("startAt") == args.slot or n.get("startAt", "").startswith(args.slot):
                    print(">>> TARGET SLOT:")
                print(json.dumps(n, indent=2))
        except Exception as e:
            print(f"\nRAW fetch failed (a field may not exist — see schema above): {e}")
        print("\nLook in the statistics element schema for anything beyond "
              "type/label/costInclTax — e.g. a credit, adjustment, netAmount, or a "
              "smart-charge/dispatch field. That's where the off-peak went.")
    finally:
        await client.close()

if __name__ == "__main__":
    asyncio.run(main())
