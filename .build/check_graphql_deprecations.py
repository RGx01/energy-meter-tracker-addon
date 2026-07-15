#!/usr/bin/env python3
"""
Check the live Kraken (Octopus) GraphQL schema for deprecations of the fields
EMT actually uses, and emit them as JSON.

Used by `.github/workflows/graphql-deprecations.yml` to open/update
`graphql-deprecation` issues — the same idea as BottlecapDave's
`checkGraphqlDeprecations.ts`, but keyed to EMT's own field set.

Design notes:
  * The "fields EMT uses" is NOT duplicated here — it is read straight from the
    constant sets in `kraken_api_client.py` (`_EMT_GRAPHQL_FIELDS`,
    `_EMT_GRAPHQL_TYPED_FIELDS`, `_EMT_GRAPHQL_ENUMS`, `_DEPRECATION_IGNORE`),
    extracted **statically via `ast`** so this script never imports the module
    (and so needs none of its runtime deps — stdlib only, no `pip install`).
  * Introspection is **unauthenticated** — Octopus exposes the schema publicly
    (verified: `__type`/`__schema` with `isDeprecated`/`deprecationReason` all
    return without a token). No API key or CI secret is required.
  * If the schema cannot be fetched or looks wrong, the script **fails loudly**
    (non-zero exit). A silent "no deprecations" would be worse than an error.
"""

import argparse
import ast
import json
import re
import sys
import urllib.request

DEFAULT_ENDPOINT = "https://api.octopus.energy/v1/graphql/"
_REMOVAL_RE = re.compile(r"on or after (\d{4}-\d{2}-\d{2})")

# Introspection scoped to exactly what a deprecation check needs: every type's
# fields and enum values with their deprecation flags.
INTROSPECTION_QUERY = (
    "{ __schema { types { name kind "
    "fields(includeDeprecated: true) { name isDeprecated deprecationReason } "
    "enumValues(includeDeprecated: true) { name isDeprecated deprecationReason } "
    "} } }"
)

_WANTED_CONSTS = (
    "_CHARGING_DEVICE_TYPES",
    "_EMT_GRAPHQL_FIELDS",
    "_EMT_GRAPHQL_TYPED_FIELDS",
    "_EMT_GRAPHQL_ENUMS",
    "_DEPRECATION_IGNORE",
)


def _eval_const(node, ns):
    """Evaluate a constant expression node, resolving Names against already-seen
    constants and supporting set-union (`a | b`) — enough for EMT's declarations
    (e.g. `_EMT_GRAPHQL_ENUMS = _CHARGING_DEVICE_TYPES | {"TEN_SECONDS", "LIVE"}`).
    """
    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.BitOr):
        return _eval_const(node.left, ns) | _eval_const(node.right, ns)
    if isinstance(node, ast.Name):
        return ns[node.id]
    return ast.literal_eval(node)


def extract_used_fields(source_path):
    """Statically read EMT's field-set constants from kraken_api_client.py.

    Returns (names, typed_fields, enums, ignore):
      names        : set[str]              — bare field names EMT selects
      typed_fields : set[(type, field)]    — generic names matched only on a type
      enums        : set[str]              — enum values EMT passes/compares
      ignore       : set[(type, field)]    — same-name collisions to exclude
    """
    tree = ast.parse(open(source_path, encoding="utf-8").read())
    ns = {}
    for node in ast.iter_child_nodes(tree):
        if isinstance(node, ast.Assign):
            for tgt in node.targets:
                if isinstance(tgt, ast.Name) and tgt.id in _WANTED_CONSTS:
                    ns[tgt.id] = _eval_const(node.value, ns)
    missing = [c for c in _WANTED_CONSTS if c not in ns]
    if missing:
        raise RuntimeError(
            f"could not extract {missing} from {source_path} — did the constant "
            "names change? Update .build/check_graphql_deprecations.py to match.")
    return (
        set(ns["_EMT_GRAPHQL_FIELDS"]),
        {tuple(x) for x in ns["_EMT_GRAPHQL_TYPED_FIELDS"]},
        set(ns["_EMT_GRAPHQL_ENUMS"]),
        {tuple(x) for x in ns["_DEPRECATION_IGNORE"]},
    )


def _removal_date(reason):
    m = _REMOVAL_RE.search(reason or "")
    return m.group(1) if m else None


def find_deprecations(schema, names, typed_fields, enums, ignore):
    """Return the deprecated schema members EMT depends on.

    A deprecated field counts if its bare name is in `names` (and the exact
    (type, field) is not in `ignore`), or if the exact (type, field) is in
    `typed_fields`. A deprecated enum value counts if its name is in `enums`.
    De-duplicated by (type, field). Pure — no I/O, so it is unit-testable.
    """
    hits = {}
    for t in schema["__schema"]["types"]:
        tname = t.get("name")
        for f in (t.get("fields") or []):
            if not f.get("isDeprecated"):
                continue
            fname = f["name"]
            key = (tname, fname)
            if key in ignore:
                continue
            if fname in names or key in typed_fields:
                hits[key] = {
                    "kind": "field", "type": tname, "field": fname,
                    "reason": (f.get("deprecationReason") or "").strip(),
                    "removal": _removal_date(f.get("deprecationReason")),
                }
        for e in (t.get("enumValues") or []):
            if not e.get("isDeprecated"):
                continue
            if e["name"] in enums:
                key = (tname, e["name"])
                hits[key] = {
                    "kind": "enum", "type": tname, "field": e["name"],
                    "reason": (e.get("deprecationReason") or "").strip(),
                    "removal": _removal_date(e.get("deprecationReason")),
                }
    return sorted(hits.values(), key=lambda d: (d["removal"] or "9999", d["field"]))


def fetch_schema(endpoint, timeout=30):
    """Fetch the introspection result (unauthenticated). Raises on any failure."""
    body = json.dumps({"query": INTROSPECTION_QUERY}).encode("utf-8")
    req = urllib.request.Request(
        endpoint, data=body,
        headers={"Content-Type": "application/json",
                 "User-Agent": "emt-graphql-deprecation-check"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        payload = json.load(resp)
    if not isinstance(payload, dict) or "data" not in payload \
            or not (payload["data"] or {}).get("__schema"):
        raise RuntimeError(
            f"introspection returned no schema (auth now required, or endpoint "
            f"changed?): {str(payload)[:300]}")
    return payload["data"]


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--source", default="kraken_api_client.py",
                    help="path to kraken_api_client.py (field-set source of truth)")
    ap.add_argument("--endpoint", default=DEFAULT_ENDPOINT)
    ap.add_argument("--out", default=None,
                    help="write findings JSON to this file (also printed to stdout)")
    args = ap.parse_args(argv)

    names, typed_fields, enums, ignore = extract_used_fields(args.source)
    try:
        schema = fetch_schema(args.endpoint)
    except Exception as e:  # fail LOUD — never silently report "all clear"
        print(f"ERROR: could not introspect {args.endpoint}: {e}", file=sys.stderr)
        return 2

    findings = find_deprecations(schema, names, typed_fields, enums, ignore)
    out_json = json.dumps(findings, indent=2)
    if args.out:
        with open(args.out, "w", encoding="utf-8") as fh:
            fh.write(out_json)
    print(out_json)

    if findings:
        print(f"\n{len(findings)} deprecated field(s) EMT uses:", file=sys.stderr)
        for d in findings:
            print(f"  - {d['type']}.{d['field']} "
                  f"(removal {d['removal'] or 'unknown'})", file=sys.stderr)
    else:
        print("No deprecations affecting EMT's fields.", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())