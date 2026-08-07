# Historical Import — Build Spec (probe first, then the API route)

> _Status: Shipped — design note kept for rationale; the code is the source of truth._

> ### ⚠️ Superseded in part (2026-07) — read this first
> This spec's **pricing/fetch strategy for the API route was replaced** after we
> found the **Kraken GraphQL Measurements API** returns per-half-hour **kWh +
> billed cost (inc/exc VAT) + a dispatch-aware `OFF_PEAK`/`STANDARD_RATE` label**,
> reaching the account's **full history** (not the REST ~2-year wall).
>
> As built, the API route (`engine.import_api_history`) now:
> - fetches `get_measurements(mpan, start, end, direction)` (GraphQL), chunked
>   newest→oldest with gap-halt, floor = earliest agreement, up to go-live;
> - writes `imported_api` blocks directly: `rate = costInclTax ÷ kWh`,
>   `off_peak = label`, `cost = costInclTax`, standing summed per day.
>
> **No longer used:** the REST `get_consumption` fetch, the `_KRAKEN_BACKFILL_CAP`
> discussion, and tariff-reconstruction pricing (`get_unit_rates` /
> `_kraken_rate_resolver` / per-agreement `RateSchedule`). The retention probe
> (Part 1) still shipped and is still useful diagnostics. Sections below are kept
> for history; **the design doc §A "GraphQL Measurements" is the source of truth.**

Sequenced from `historical_import_design.md`. Two pieces, in order:

1. **Export-retention probe** — read-only diagnostic, mirrors the LTS probe.
   Answers remaining Open Question #1 (how far `get_consumption` reaches per
   channel) before we rely on it. Ships as beta; no writes.
2. **API consumption route** — the first *build* spike. Lifts the 400-day
   backfill cap for a user-initiated import, tiles pre-EMT blocks to go-live,
   with a delta-overlap sanity check. Writes data (behind preview + confirm).

The probe gates the route: it tells us how far back import vs export actually
reach, which sets what the route can promise (export flagged unavailable before
its own start — the design already allows import-only tiling there).

---

## Part 1 — Export-retention probe (read-only)

### What it measures

Per channel (import MPAN, and export MPAN if discovery found one):
**earliest** interval available, **latest**, half-hourly **cadence/coverage**,
and the **export-vs-import lag** (how many days shorter export's history is).
That single lag number is the whole point — it decides whether the route can
offer export back to the same date as import, or only import-only before export
starts.

### Cheap boundary strategy (do NOT download years of HH)

`get_consumption` already paginates *all* rows via `_get_paginated`. Pulling the
full HH history per channel is thousands of pages and pointless for a retention
question. Instead fetch only the **boundary pages**:

- earliest = `get_consumption(order_by="period",  page_size=1, period_from=FLOOR)`
  → first row's `interval_start`.
- latest   = `get_consumption(order_by="-period", page_size=1, period_from=FLOOR)`
  → first row's `interval_start`.

**Must pass a far-past `period_from` floor** (e.g. `2015-01-01`). Without it the
consumption endpoint defaults to only the most recent ~week, so `order_by=period`
returns the earliest of *that window* — a recent date, not the meter's true
start. (Observed live: both channels reported a 7-day span until the floor was
added.) The floor predates smart-meter rollout, so the ascending scan starts
from real history.

This needs a one-row, single-page fetch. Add a thin client method rather than
abusing `_get_paginated`:

```python
# kraken_api_client.py
async def get_consumption_boundary(self, mpan, serial, *, newest=False):
    """Cheap single-row probe: earliest (default) or latest HH interval for a
    meter, without paging the whole series. Returns one row dict or None."""
    path = f"/v1/electricity-meter-points/{mpan}/meters/{serial}/consumption/"
    params = {"order_by": "-period" if newest else "period", "page_size": 1}
    page = await self._get(path, params)           # single page, no pagination
    results = (page or {}).get("results") or []
    return results[0] if results else None
```

(If a fuller picture is wanted later — cadence/gaps/coverage like the LTS probe
— that's a separate opt-in "deep" mode that *does* page a bounded window, e.g.
the most recent 60 days, to confirm HH cadence without pulling tenure. Keep it
out of v1 of the probe; the boundary answer is what unblocks the route.)

### Engine coro

```python
# engine.py — read-only, gated on an available API
async def probe_consumption_retention() -> dict:
    if not kraken_available():
        return {"ok": False, "reason": "no_api"}
    disc = _kraken_discovery or {}
    out = {"ok": True, "channels": {}}
    for name in ("import", "export"):
        ch = disc.get(name)
        if not ch or not ch.get("mpan") or not ch.get("serial"):
            out["channels"][name] = {"available": False}
            continue
        first = await _kraken_client.get_consumption_boundary(ch["mpan"], ch["serial"])
        last  = await _kraken_client.get_consumption_boundary(ch["mpan"], ch["serial"], newest=True)
        out["channels"][name] = _cp.build_consumption_boundary(name, first, last)
    out["lag_days"] = _cp.export_lag_days(out["channels"])   # export.earliest - import.earliest
    out["account_join"] = disc.get("account_join")           # if known, for context
    return out
```

`consumption_probe.py` (new, pure, testable — same pattern as
`statistics_probe.py`): `build_consumption_boundary(channel, first_row,
last_row)` → `{available, earliest, latest, span_days, raw_time_kind}`, and
`export_lag_days(channels)` → int|None. Timestamp parsing reuses the same
"epoch-ms / epoch-s / ISO with offset" handling; consumption `interval_start` is
ISO-with-offset today, but the helper stays format-agnostic.

### Endpoint

Mirror `/api/historical/probe` exactly:

```python
# web/server.py
@app.route("/api/historical/export-probe", methods=["POST"])
def api_export_probe():
    import engine as _eng
    if not _eng.kraken_available():
        return jsonify({"ok": False, "error": "no API connected"}), 400
    result = _run_on_engine_loop(_eng.probe_consumption_retention(), timeout=60.0)
    return jsonify(result), (200 if result.get("ok") else 400)
```

No request body (it uses discovery). Read-only; safe to call anytime.

### UI

Add a second card to the existing `historical_probe.html` (it's already the
"what history do I have" page): **"Octopus API retention"**, a single **Run**
button, output a two-row table (import / export) with earliest, latest, span,
and a headline line: *"Export history reaches N days less than import — before
&lt;date&gt; only import can be reconstructed."* Hidden entirely when
`kraken_available()` is false (pass a flag into the template context).

### Tests

- `test_consumption_probe.py` — pure: boundary→report, lag math, missing
  channel, unparseable/empty rows.
- `test_server.py` — `/api/historical/export-probe` gated on API present;
  shape of the response; no-API 400.
- `kraken_api_client` — `get_consumption_boundary` builds the right path/params
  and reads `results[0]` (fake `_get`).

**Ship as beta, collect a few real numbers (esp. export lag), then build Part 2.**

### Measured (live) — the consumption endpoint retains ~2 years

First real run (single-property account, one smart meter serving both channels):

- Import **and** export consumption both reach back only ~**2 years** (earliest
  ≈ today − 2y), on the **same single serial** — so it is *not* a meter exchange.
- The account is older: import **agreements run back 3+ years** (2023-06), through
  six tariffs. Consumption does **not** follow the agreements down — so a **tariff
  change is not the boundary** either. The `/consumption/` endpoint simply enforces
  a **rolling ~2-year retention**, independent of tariff/agreement.
- A below-boundary count of **1** was an edge artifact (DST/half-open interval),
  not recoverable history — a real under-reach would return a full run of rows.
  The diagnostic heuristic now treats `< 24` below-rows as an edge artifact and
  surfaces the row's date rather than crying "under-reaching".

**Hard consequence for Part 2:** the API route recovers **at most ~2 years, and
the wall slides forward daily**. Everything older than that (here: 2023-06 →
~2024-07) is reachable **only** via the **CSV route** (website export while it's
still offered) or a previously-saved download. This confirms CSV is not optional
— it's the sole path to pre-2-year history — and that API imports should be run
promptly before more ages out.

---

## Part 2 — API consumption route (the build)

### Principle: reuse the ingester, don't reinvent it

`KrakenIngester.poll(window=(from,to), advance_cursor=False)` already fetches HH
consumption, prices it against agreement/tariff history, and writes blocks. The
whole route is: **run that poll over `[source_start → go-live)` without the
400-day cap, tag the blocks as imported, and wrap it in preview/confirm.** No
new pricing or block-writing code.

### Lifting the cap (without touching the auto-backfill)

**Where 400 came from:** nothing principled. `engine.py:161` calls it a
*"generous"* cap and notes "the real window is MIN(block_start)→now". It's an
arbitrary ceiling so a fresh install's *automatic* startup backfill doesn't pull
unbounded history on first boot — not an API limit, not tied to meter retention.
So the manual route doesn't "raise 400 to N"; it ignores the constant entirely
and drives an explicit window. The auto-backfill keeps its ceiling unchanged.

`_kraken_backfill_days()` caps at `_KRAKEN_BACKFILL_CAP_DAYS = 400` — correct for
the *automatic* startup backfill (don't hammer the API on every boot). The
manual import is the "deliberate historical-import action" that code comments
already anticipate, so it bypasses the cap via an explicit window rather than by
raising the constant:

```python
# engine.py
async def import_history_preview(from_date, to_date=None) -> dict:
    """Dry run: what would a manual API import create over [from_date, go_live)?
    Returns span, per-channel earliest reachable (from the probe), estimated
    block count, gaps, and delta-overlap warnings. Writes nothing."""

async def import_history_apply(from_date, to_date=None, confirmed=False) -> dict:
    """Take a backup, then poll the ingester over the requested window with
    advance_cursor=False and provenance='imported_api'. Tiles up to go-live."""
```

`to_date` defaults to **go-live** = `_store.get_oldest_block_start()` (abut live
capture, no overlap). `from_date` is clamped to each channel's earliest reachable
interval (from Part 1's probe) — export clamps independently, so export is simply
absent before its own start (not an error). The window is built Z-suffixed UTC
exactly like `retry_settlement_for_unsettled` does.

### Chunked retrieval (do NOT poll the whole span at once)

`KrakenIngester.poll` fetches its *entire* window in one `get_consumption` call,
which pages every result into a single in-memory list and one processing pass
(`kraken_ingester.py:203`). Over tenure (~2 yr ≈ 35k half-hours *per channel*)
that's a huge fetch, a large allocation, and one long transaction with **no
resumability** — a failure at hour 30,000 loses everything.

So `import_history_apply` walks the requested span in **fixed sub-windows
(default 60 days), newest→oldest**, calling `poll(window=chunk,
advance_cursor=False)` per chunk. Newest-first is deliberate: the most recent
(most useful) history lands first, and a mid-run failure still leaves the recent
end imported and contiguous with go-live — the checkpoint records how far *back*
the walk reached, and a re-run resumes deeper.

```python
CHUNK_DAYS = 60          # tune vs API page count; 60d ≈ 2,880 HH intervals/channel
async def import_history_apply(from_date, to_date=None, confirmed=False):
    ...
    cursor = to_dt                      # go-live, exclusive
    while cursor > from_dt:
        lo = max(from_dt, cursor - timedelta(days=CHUNK_DAYS))
        summary = await ingester.poll(window=(_z(lo), _z(cursor)),
                                      advance_cursor=False)
        _persist_progress(lo)           # resumable checkpoint (kraken_state)
        if _gap_halts(summary): break   # contiguity rule stops the walk
        cursor = lo
        await asyncio.sleep(PER_CHUNK_PAUSE_S)   # be polite to the API
```

Benefits: bounded memory, **per-chunk error isolation** (one bad chunk doesn't
sink the run), a **resume checkpoint** in `kraken_state` (re-run continues from
the last completed chunk instead of restarting), natural **progress reporting**
(chunks done / total), and gentler API load. `CHUNK_DAYS` is a constant to tune
against real page counts — the probe's cadence check can inform it.

The **contiguity/gap halt** is evaluated per chunk (and across the chunk
boundary): the first internal gap in a channel that otherwise has data stops the
walk, matching the design's "halt at the first gap" rule — now with the partial
import already safely persisted up to that point.

### Contiguity & gaps

Walk the requested range; **halt at the first internal gap** in a channel that
otherwise has data (missing expected HH intervals). Export *starting later* than
import is not a gap. A gap in **import** halts the import there (no spanning
holes). The ingester writes per-HH blocks, so gap detection = missing expected
`interval_start`s between first and last within the window.

### Delta-overlap sanity check (the trust model)

Where the requested range overlaps existing EMT blocks, compare **per-block
consumption deltas** (EMT's stored value vs the API's) within a tolerance
(Open Question #3 — start ~2% or 0.05 kWh, whichever larger; confirm against
real overlap). Mismatches beyond tolerance surface as **preview warnings**; they
never overwrite native blocks. No bill-total reconciliation.

### Provenance

Thread `provenance="imported_api"` from `import_history_apply` → ingester →
block writes, so imported blocks carry a `source LIKE 'imported%'` flag distinct
from live `api`. This is what the delete-blocks "reconstructed history only"
filter keys on (already in the design). The go-live period straddling live
capture → `imported_blended`.

### Preview → confirm UX

1. User picks a **from date** (default = the probe's import-earliest) on a new
   "Import history" page.
2. **Preview** (`import_history_preview`): span, blocks to create per channel,
   export-unavailable-before date, gaps found (halt point), overlap warnings,
   and the best-endeavours notice.
3. **Confirm** (checkbox) → **Apply**: auto-backup first (reuse
   `/api/backup`), then `import_history_apply(confirmed=True)`.
4. Result: created counts, halt reason if any, "regenerate charts" prompt.
   Rollback = existing delete-blocks with the reconstructed-history filter.

Device attribution (BL-12) is **not** in this route's first cut — house import/
export + cost only. Attribution layers on afterward via the same "add history"
flow once the recorder-sensor override lands (design §Device attribution).

### Endpoints

- `POST /api/historical/import/preview` → `import_history_preview`
- `POST /api/historical/import/apply`   → `import_history_apply` (gated on
  `confirmed`, takes a backup first), both gated on `kraken_available()` and
  `_run_on_engine_loop` with a generous timeout (tenure imports are large).

### Tests

- Ingester over a wide manual window tiles to go-live, no overlap at the
  boundary interval; export clamps to its own start (import-only before it).
- Cap bypass: manual window is honoured beyond 400 days; the *auto*-backfill
  still clamps (existing `test_engine` cap tests stay green).
- Chunking: a multi-chunk span issues N contiguous `poll` windows covering the
  range with no overlap/gap at chunk seams; a mid-run failure leaves earlier
  chunks persisted and the resume checkpoint at the last good chunk; re-run
  continues rather than restarting.
- Gap halt: a hole in import stops the import at the hole; export-later is not a
  gap.
- Delta-overlap: within tolerance → silent; beyond → warning, native untouched.
- Provenance: imported blocks carry the `imported_*` flag; delete-blocks
  reconstructed-filter removes only those.
- Preview writes nothing (block count unchanged after a preview call).

---

## Sequencing

Probe (Part 1) is self-contained and low-risk — build, ship beta, gather export
lag from a few setups. Part 2's `from_date` clamp and export-unavailable
messaging consume the probe's numbers directly, so the probe both de-risks and
feeds the route. Neither part touches the exact-house-figure firewall or device
attribution.
