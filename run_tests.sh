#!/usr/bin/env bash
# Run the EMT unit-test suite.
#
# The suite is designed to run FILE-ISOLATED: several test modules install
# sys.modules stubs at import time, so importing them all into one process
# (plain `pytest tests/`) collides at collection. This script runs each test
# file in its own process and aggregates the result — the invocation the suite
# expects. Works on Python 3.10+ (energy_charts.py is dual-compatible).
#
# It also (idempotently) creates the source symlinks the tests load by path
# (tests/<module>.py, tests/web, tests/server.py, tests/templates), so a fresh
# checkout can run the suite without any manual setup.
#
# Usage:  ./run_tests.sh            # whole suite
#         ./run_tests.sh -k rate    # pass extra args through to pytest
set -u
cd "$(dirname "$0")"

# ── Scaffolding: tests load sibling modules by absolute path ─────────────────
_MODULES=(block_store energy_charts engine ha_client instance kraken_api_client
          kraken_ingester kraken_mini kraken_rates energy_engine_io main)
for m in "${_MODULES[@]}"; do
  [ -e "tests/$m.py" ] || ln -sf "../$m.py" "tests/$m.py"
done
[ -e tests/web ]        || ln -sfn ../web tests/web
[ -e tests/server.py ]  || ln -sf ../web/server.py tests/server.py
[ -e tests/templates ]  || ln -sfn ../web/templates tests/templates

# ── Run each test file in its own process ────────────────────────────────────
# test_harness.py is a manual chart-inspection script, not a unit test.
PYTEST="${PYTEST:-python3 -m pytest}"
total_pass=0 total_fail=0 total_err=0 failed_files=()
for f in tests/test_*.py; do
  [ "$(basename "$f")" = "test_harness.py" ] && continue
  # Capture the FULL output (with -rf short summary) so a failure surfaces the
  # test id + traceback — otherwise a CI failure is invisible (only the count).
  full=$($PYTEST "$f" -rf -p no:cacheprovider "$@" 2>&1)
  out=$(tail -1 <<<"$full")
  p=$(grep -oE '[0-9]+ passed' <<<"$out" | grep -oE '[0-9]+'); p=${p:-0}
  fl=$(grep -oE '[0-9]+ failed' <<<"$out" | grep -oE '[0-9]+'); fl=${fl:-0}
  er=$(grep -oE '[0-9]+ error'  <<<"$out" | grep -oE '[0-9]+'); er=${er:-0}
  total_pass=$((total_pass+p)); total_fail=$((total_fail+fl)); total_err=$((total_err+er))
  if [ "$fl" != 0 ] || [ "$er" != 0 ]; then
    failed_files+=("$(basename "$f"): $out")
    printf '\n──────── output for %s ────────\n%s\n' "$f" "$full"
  fi
done

echo "═══════════════════════════════════════════════"
echo "  passed=$total_pass  failed=$total_fail  errors=$total_err"
if [ "${#failed_files[@]}" -ne 0 ]; then
  echo "  files needing attention:"
  printf '    %s\n' "${failed_files[@]}"
fi
echo "═══════════════════════════════════════════════"
[ "$total_fail" = 0 ] && [ "$total_err" = 0 ]