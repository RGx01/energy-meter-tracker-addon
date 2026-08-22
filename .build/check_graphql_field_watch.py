#!/usr/bin/env python3
"""
Watch the Kraken (Octopus) GraphQL schema for NEW / removed fields on the types EMT relies on for
Intelligent-Octopus reconstruction — so a change that could improve historical car/house + cap
splits (e.g. a new EV-energy or dispatch-attribution field) is surfaced automatically instead of
discovered by accident.

Companion to `check_graphql_deprecations.py` (which watches for *deprecations*). Same weekly Action,
same unauthenticated introspection.

  * Watches every type whose NAME matches a reconstruction-relevant pattern (dispatch / measurement /
    consumption / intelligent / interval / statistic / TOU / cap) — robust to type renames.
  * Diffs the live watched-type field sets against a committed baseline
    (`.build/graphql_field_watch_baseline.json`). ADDED or REMOVED fields are reported; the weekly
    Action opens/updates an issue.
  * Seed / refresh the baseline with `--update-baseline` (run once against the live schema; commit
    the result). An empty/absent baseline is treated as "unseeded" — it prints the snapshot and asks
    you to seed, rather than flooding.

HONEST BOUNDARY — read this before trusting it:
  Introspection sees which fields EXIST, NOT which are POPULATED. The linchpin question for BL-9 /
  BL-28 — "has Octopus started stamping `source` on COMPLETED dispatches?" — is a *data-behaviour*
  change this check CANNOT see (`meta.source` already exists; it just returns null on completed).
  That one still needs an authenticated live query over a recent completed dispatch. This watcher
  catches new schema SURFACE (added fields); it does not catch a field starting to be populated.
"""
import argparse
import json
import os
import sys
import urllib.request

DEFAULT_ENDPOINT = "https://api.octopus.energy/v1/graphql/"
BASELINE_PATH = os.path.join(os.path.dirname(__file__), "graphql_field_watch_baseline.json")

# Reconstruction-relevant type surface — matched as case-insensitive substrings on the type NAME,
# so it survives renames (UpsideDispatchType → whatever). Keep this list tight to avoid noise.
WATCH_PATTERNS = ("dispatch", "measurement", "consumption", "intelligent",
                  "interval", "statistic", "tou", "cap")

INTROSPECTION_QUERY = "{ __schema { types { name kind fields { name } } } }"


def fetch_schema(endpoint):
    """POST the (unauthenticated) introspection query and return the parsed `__schema`."""
    body = json.dumps({"query": INTROSPECTION_QUERY}).encode("utf-8")
    req = urllib.request.Request(endpoint, data=body,
                                 headers={"content-type": "application/json"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        payload = json.loads(resp.read().decode("utf-8"))
    if "errors" in payload:
        raise RuntimeError(f"introspection returned errors: {payload['errors']}")
    return payload["data"]["__schema"]


def _matches(type_name):
    n = (type_name or "").lower()
    return any(p in n for p in WATCH_PATTERNS)


def watched_fields(schema):
    """{typeName: sorted[field names]} for every watched type that has fields."""
    out = {}
    for t in (schema.get("types") or []):
        name = t.get("name") or ""
        if name.startswith("__") or not _matches(name):
            continue
        fields = [f["name"] for f in (t.get("fields") or []) if f.get("name")]
        if fields:
            out[name] = sorted(fields)
    return out


def diff_watch(current, baseline):
    """Per-type {added:[...], removed:[...]} vs the baseline. New types show all their fields added;
    dropped types show all removed."""
    report = {}
    for name in sorted(set(current) | set(baseline)):
        cur = set(current.get(name, []))
        base = set(baseline.get(name, []))
        added = sorted(cur - base)
        removed = sorted(base - cur)
        if added or removed:
            report[name] = {"added": added, "removed": removed}
    return report


def _load_baseline(path):
    try:
        with open(path, encoding="utf-8") as fh:
            return json.load(fh) or {}
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--endpoint", default=DEFAULT_ENDPOINT)
    ap.add_argument("--baseline", default=BASELINE_PATH)
    ap.add_argument("--update-baseline", action="store_true",
                    help="write the current watched-type field sets to the baseline and exit 0")
    ap.add_argument("--out", default=None, help="write the diff JSON here (also printed)")
    ap.add_argument("--fail-on-change", action="store_true",
                    help="exit 1 if any watched field was added/removed (reddens CI). No effect "
                         "until the baseline is seeded — an unseeded baseline always exits 0.")
    args = ap.parse_args(argv)

    try:
        schema = fetch_schema(args.endpoint)
    except Exception as e:                       # fail LOUD — never a silent all-clear
        print(f"ERROR: could not introspect {args.endpoint}: {e}", file=sys.stderr)
        return 2
    current = watched_fields(schema)

    if args.update_baseline:
        with open(args.baseline, "w", encoding="utf-8") as fh:
            json.dump(current, fh, indent=2, sort_keys=True)
            fh.write("\n")
        print(f"baseline written: {len(current)} watched type(s) → {args.baseline}", file=sys.stderr)
        return 0

    baseline = _load_baseline(args.baseline)
    if not baseline:                             # unseeded — don't flood; ask to seed
        print(json.dumps(current, indent=2, sort_keys=True))
        print("\nBaseline is empty — run with --update-baseline to seed it, then commit.",
              file=sys.stderr)
        return 0

    report = diff_watch(current, baseline)
    out_json = json.dumps(report, indent=2, sort_keys=True)
    if args.out:
        with open(args.out, "w", encoding="utf-8") as fh:
            fh.write(out_json)
    print(out_json)
    if report:
        print(f"\n{len(report)} watched type(s) changed — review whether it enables better IOG "
              "reconstruction (see docs/design/charger_derived_iog_split_design.md):", file=sys.stderr)
        for t, d in report.items():
            if d["added"]:
                print(f"  + {t}: added {', '.join(d['added'])}", file=sys.stderr)
            if d["removed"]:
                print(f"  - {t}: removed {', '.join(d['removed'])}", file=sys.stderr)
    else:
        print("No field changes on EMT's watched GraphQL types.", file=sys.stderr)
    return 1 if (report and args.fail_on_change) else 0


if __name__ == "__main__":
    sys.exit(main())
