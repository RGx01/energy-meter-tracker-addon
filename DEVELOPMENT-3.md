# Development Guide — Volume 3

Covers decisions made during 2.10.x development and the design for 3.0.0 Kraken API integration.

---

## 2.10.x — Key Decisions and Fixes

### Rounding Methodology — Approach and Revision History

The 2.10.x series went through several iterations on cost accounting methodology.
Understanding what was tried, what broke, and what shipped is important context
for future work.

#### 2.10.0 — Raw float accumulation, round once

Initial approach: accumulate raw float block costs, round once to 2dp at the
final display step.

**Surfaces affected:**

| Surface | File | Function |
|---------|------|----------|
| Billing chart Bill Summary | `energy_charts.py` | `calculate_billing_summary_for_period` |
| Billing chart day sidebar | `energy_charts.py` | `build_day_chart_html` → `meter_totals` |
| Live Power billing cards | `server.py` | `_fmt_total` |
| Usage Stats bars/table | `server.py` | `api_blocks_summary` |
| Usage Stats JS period total | `charts.html` | `barAggRows` → `barPeriodTotals` |

**Key implementation details:**

- `calculate_billing_summary_for_period`: removed `_day_accum` per-day rounding.
  Raw block costs accumulate directly into `meter_summary` and `meter_totals`.
  `main_import_raw` and `main_export_raw` track raw main meter totals separately
  so `total_cost` is computed from unrounded values before the display-rounding
  pass. Round once at end: 2dp cost, 3dp kWh.

- `build_day_chart_html`: `meter_totals` built with
  `sum(abs(v) for v in meter_cost[meter])` — raw sum then `round(cost_sum, 4)`.

- `_fmt_total`: `total = round(float(imp_cost) + float(standing) - float(exp_cost), 2)`
  — raw sum round once.

- `api_blocks_summary`: all per-day values sent at **4dp**. Includes
  `period_totals` dict with pre-computed SQL totals for each billing period.
  JS `barAggRows` uses `barPeriodTotals` for period-level cost display to avoid
  accumulated rounding error over long periods.

#### 2.10.3 — barPeriodTotals grand total bug found

`barPeriodTotals` (pre-computed SQL totals) were being applied to individual
day rows in daily view because the billing period key matched even for a single
day — `_bKeys.length === 1` was true and the entire period's SQL total was
substituted. Partial fix: only use `barPeriodTotals` when filtered row count
matches the period's total row count.

#### 2.10.4 — Unified net_cost methodology (partial)

Attempted to unify all three surfaces onto a single method:
block costs summed into 4dp daily rows, `net_cost = round(imp + standing - exp, 2)`
per day, daily nets summed for period/yearly totals. `barPeriodTotals` removed
entirely from `charts.html` and `server.py`.

**What actually shipped:**
- `charts.html` (Usage Stats) — adopted `net_cost` accumulation. Grand total
  is `sum(agg.net_cost)` across displayed buckets. `barPeriodTotals` removed.
- `server.py` `_fmt_total` — adopted `round(total_imp_cost + standing - exp_cost, 2)`
  from already-rounded display values.
- `energy_charts.py` — **reverted**. The billing chart `total_cost` was found
  wrong after adopting `net_cost` at that layer. `energy_charts.py` kept the
  raw float accumulate/round-once approach from 2.10.0.

**Net result as of 2.10.4/2.10.5:**

| Surface | Method |
|---------|--------|
| Billing chart (energy_charts.py) | Raw float accumulate, round once at display |
| Live Power billing cards (`_fmt_total`) | `round(imp + standing - exp, 2)` from 2dp display values |
| Usage Stats (`charts.html`) | `net_cost = round(imp + standing - exp, 2)` per day, summed |

The billing chart and Usage Stats can therefore differ by ±£0.01 on periods
with sub-meters, because they use different arithmetic paths. Both are
internally self-consistent. This is a known and accepted artefact documented
in the `_apply_pass2` comment in `engine.py`.

#### Why cost_remainder is kwh × rate not main_cost − sub_costs

`cost_remainder` in PASS 2 is computed as `remainder_kwh × parent_rate`, not
as `main_cost − sub_costs`. This is intentional:

`main_cost` comes from actual meter register reads (cumulative kWh delta
interpolated to block boundaries). Sub-meter costs come from power sensor
integration (kW polled at ~10s intervals). These are two independent
measurement systems. Subtracting one from the other absorbs all sensor
disagreement into `cost_remainder`, potentially making it negative.

The ~0.01p/block discrepancy is an inherent measurement artefact. Presentation
layers use rate-based subtraction (`main_cost[rate] − sub_cost[rate]`) for
display, avoiding accumulation across periods.

### Live Power Billing Card Cache Bug

`api/billing` was missing `Cache-Control: no-store` headers, causing stale
billing card data after block boundaries. Fixed by adding no-store headers and
`?t=Date.now()` cache-busting in `live_power.html`.

### Fresh Install Hang (2.10.2)

On a fresh installation with no existing data, the addon hung indefinitely at
startup. Root cause: `conn.backup()` called during the upgrade-backup routine
on a freshly-created WAL-mode database where `executescript()` during schema
creation left an open read transaction. Fixed by skipping the upgrade backup on
fresh installs with zero blocks.

### PDF Export

- **Billing tab**: captures current period/view, billing summary, daily chart
  images via `Plotly.toImage()`, open data tables. Light theme forced.
- **Heatmaps tab**: active metric heatmap via `Plotly.toImage()`.
- **Usage Stats tab**: chart image, toolbar state, data table. Checkbox state
  fix: `cloneNode(true)` copies DOM attributes but not JS-set `.checked`
  property. Fixed by replacing checkboxes with Unicode ☑/☐ in PDF clone.

### Usage Stats Auto-refresh Bug (2.10.5)

The 2-minute auto-refresh timer used `textContent.indexOf('Daily')` to detect
the active tab — stopped working when the billing tab was renamed from "Daily
Usage" to "Billing" in earlier versions. With the Usage Stats tab also present,
the fallback refreshed the heatmap instead of billing when Usage Stats was
active, and Usage Stats itself never refreshed. Fixed by adding `data-chart`
attributes to tab buttons and including the `bar` tab in the refresh interval.

### Files Changed Across 2.10.x

- `energy_charts.py` — billing summary rounding (raw float, kept), sidebar
  rounding, PDF export, per-day data table, Show/Hide Data button
- `server.py` — `_fmt_total` (net_cost method), `api_blocks_summary` precision,
  `period_totals` removed in 2.10.4, no-cache headers on `api/billing`
- `charts.html` — PDF export, `barAggRows` net_cost accumulation, `barPeriodTotals`
  removed in 2.10.4, auto-refresh fix in 2.10.5
- `live_power.html` — cache-busting timestamp on billing fetch
- `engine.py` — fresh install hang fix (2.10.2), cost_remainder comment added
- `main.py` — startup error handling (2.10.2)

### Test Suite

`test_usage_stats_vs_billing.py` — covers:
- Daily/monthly/yearly billing vs SQL agreement
- Non-round standing charge (£0.504559/day) over 16+ days
- Sub-meter cost precision over multi-day periods
- Live Power net total vs Usage Stats net total agreement
- BST/GMT transition handling for standing charge grouping
- The real-world scenario that produced -£3.11 / -£3.22 / -£3.14 / -£3.15
  discrepancies

---

## 3.0.0 — Kraken API Integration

### Why 3.0.0

Pure API mode removes the requirement for a CAD or HA main meter sensor. Up to
2.x, EMT has implicitly required hardware investment to monitor the grid
boundary — a CAD, a Hildebrand Glow, or a CT clamp feeding an HA sensor. From
3.0.0 a user can install EMT, enter their Octopus credentials, and get a fully
functional billing and usage dashboard with zero additional hardware beyond
their smart meter. This changes who the software is for, not just what it does.
That warrants a major version bump.

The 2.x HA sensor path is completely unchanged and remains fully supported.
Config is backwards compatible — `kraken_api` is a new optional block.
No breaking changes to schema, API, or existing behaviour.

### Vision

The unique value of EMT is insight **behind the meter** — sub-meter
attribution, device-level carbon, battery/EV accounting — that any supplier API
can never provide because they only see the grid boundary. The Kraken platform
API adds complementary value: authoritative settled kWh figures from DCC, and
(for users without BottlecapDave or a CAD) rates and live power.

**Kraken, not Octopus:** Kraken Technology is a separate platform powering
multiple energy suppliers — Octopus Energy, EDF, and others. The API (REST and
GraphQL), authentication (`obtainKrakenToken`), and data structures are Kraken
platform concepts. Octopus is first because it's the most common among EMT
users; other Kraken-powered suppliers differ only in base URL.

### What the Kraken API Provides

#### REST API

Auth: HTTP Basic, API key as username, empty password.

| Endpoint | Returns | Notes |
|----------|---------|-------|
| `/v1/accounts/<account>/` | MPAN, serial numbers, tariff history | Full account metadata |
| `/v1/electricity-meter-points/<mpan>/meters/<serial>/consumption/` | `interval_start`, `interval_end`, `consumption` (kWh) | **No rate, no cost, no is_estimated flag** |
| `/v1/products/<product>/electricity-tariffs/<tariff>/standard-unit-rates/` | Rate periods with `valid_from`/`valid_to`, `value_inc_vat` | Agile: one record per half-hour |
| `/v1/products/<product>/electricity-tariffs/<tariff>/standing-charges/` | Daily standing charge periods | |

**Critical limitation:** No `is_estimated` flag. No programmatic way to
determine if a reading is provisional — recency implies provisional by
convention (blocks < 48h treated as provisional).

**Data latency:** Typically 24-48h for SMETS2, potentially longer for SMETS1.

**Rate limit:** 100 calls/hour, **shared across all Octopus API usage**
including the Octopus app and other integrations.

#### GraphQL API

Endpoint: `https://api.octopus.energy/v1/graphql/` (Octopus).
Auth: JWT token via `obtainKrakenToken` mutation.

Key query:

```graphql
smartMeterTelemetry(
  deviceId: "<METER_GUID>"
  grouping: THIRTY_SECONDS
  start: "..."
  end: "..."
) {
  readAt
  consumptionDelta   # kWh since last reading
  demand             # instantaneous watts
  costDelta          # display only — not used for block costing
  consumption        # cumulative kWh
}
```

GraphQL provides live power via Mini/SMETS2 HAN. It does **not** provide
settled consumption blocks — that is REST only.

### The BottlecapDave Boundary

Most Octopus Energy users running EMT already have the BottlecapDave unofficial
HA integration installed. EMT already reads rates from it today via `rate_sensor`
in `meter_channels` config.

BottlecapDave already provides via HA sensors, at zero cost to EMT's API quota:

| Data | BottlecapDave sensor |
|------|----------------------|
| Current rate (p/kWh inc VAT) | `*_current_rate` — updated every 15 min |
| Standing charge (p/day) | `*_current_standing_charge` |
| Full day rate schedule | `*_current_day_rates` event |
| IO dispatch corrections applied | Already in rate schedule |
| Live power via Mini | `*_current_consumption` |

**Calling Kraken rate or standing charge endpoints when BottlecapDave is
already doing so burns shared quota performing identical work twice.**

The only data the Kraken REST API provides that BottlecapDave does not is
**settled half-hourly consumption kWh from DCC** at per-block granularity.
BottlecapDave's previous accumulative consumption is daily total only.
This is the unique value of the Kraken integration.

### Revised Data Source Map

| Data | BottlecapDave user | Pure API user |
|------|--------------------|---------------|
| Main meter imp_kwh per block | Kraken REST (settled, ~48h) | Kraken REST (settled, ~48h) |
| Main meter rate | HA sensor (BottlecapDave) | Kraken REST rate endpoint |
| Standing charge | HA sensor (BottlecapDave) | Kraken REST standing charge endpoint |
| IO dispatch rate correction | HA sensor (already applied) | Kraken GraphQL dispatches (future) |
| Sub-meter kWh | HA sensors only | HA sensors only (or absent) |
| Live power (watts) | HA Mini sensor or CAD | Kraken GraphQL Mini |

### API Call Budget

For a BottlecapDave user (most existing users):

| Operation | Frequency | Calls/hour |
|-----------|-----------|------------|
| Consumption endpoint (import) | Every 6h | 0.17 |
| Consumption endpoint (export) | Every 6h | 0.17 |
| Rate/standing charge endpoints | **Never** (HA sensor) | 0 |
| GraphQL Mini | **Never** (BottlecapDave sensor) | 0 |
| **Total** | | **~0.3/hour** |

For a pure API user with Mini:

| Operation | Frequency | Calls/hour |
|-----------|-----------|------------|
| Consumption (import + export) | Every 6h | ~0.3 |
| Rate + standing charge (cached) | Daily | ~0.08 |
| GraphQL Mini live poll | Every 90s | ~40 |
| **Total** | | **~40/hour** |

### Ingester Rate Lookup — Conditional on HA Sensor

The ingester only calls Kraken rate endpoints if no HA rate sensor is configured:

```python
def _should_fetch_rates(self, cfg: dict) -> bool:
    """Only fetch rates from Kraken if BottlecapDave / HA rate sensor absent."""
    return not cfg.get("rate_sensor")
```

For BottlecapDave users the rate already on the block (written by the engine
from the HA sensor at boundary time) is used as-is. The ingester upserts only
`imp_kwh` and `is_provisional`, leaving `imp_rate` and `imp_cost` unchanged.

### Sub-meter Attribution in API Mode — PASS 2 Re-run

#### The Problem

In HA mode, main meter and sub-meter reads arrive simultaneously at block
boundary. PASS 2 runs immediately with real figures on both sides.

In API mode, main meter kWh arrives from Kraken ~48h later. At boundary time:

| Data | Available? |
|------|------------|
| Sub-meter imp_kwh | ✅ Yes (HA sensor) |
| Main meter imp_kwh | ❌ No (~48h delay) |
| kwh_grid, kwh_remainder | ❌ Cannot compute correctly |

#### Design: Write Provisional, Re-run PASS 2 at Kraken Finalise

**At boundary time:**
1. Write block with HA main meter read if available (hybrid), or `imp_kwh = NULL` (pure API)
2. In hybrid mode: run PASS 2 provisionally — attribution is approximate but usable
3. In pure API mode: **skip PASS 2 entirely** if `imp_kwh = NULL` — do not
   produce misleading zero `kwh_grid` figures
4. Mark block `is_provisional = 1`, `needs_pass2_rerun = 0`

**At Kraken finalise time (~48h later):**
1. Ingester upserts authoritative `imp_kwh`, sets `is_provisional = 0`
2. Sets `needs_pass2_rerun = 1` if sub-meter siblings exist
3. Engine tick drains the queue: loads full block, re-runs `_apply_pass2`,
   writes corrected attribution columns, clears flag

Sub-meter `imp_kwh` is never modified — only the derived attribution columns
(`kwh_grid`, `kwh_battery`, `cost`, `kwh_remainder`, `cost_remainder`) are
updated.

#### PASS 2 Re-run: Clip Correction

| Scenario | Effect |
|----------|--------|
| Provisional main meter too high | Sub-meter `kwh_grid` clipped down; `kwh_battery` increases |
| Provisional main meter NULL (pure API) | Full attribution computed for first time |
| Authoritative main meter < total sub-meter kWh | All sub-meters clipped; `kwh_remainder` → 0; WARNING logged |

#### New BlockStore Methods Required

```python
def drain_pass2_queue(self) -> list[dict]: ...      # fetch flagged blocks, clear flag atomically
def get_block_by_id(self, block_id: int) -> Optional[dict]: ...
def upsert_kraken_block(self, block: dict) -> None: ...  # sets needs_pass2_rerun=1 if sub-meter siblings exist
def get_block_by_start(self, block_start_iso: str, meter_id: str) -> Optional[dict]: ...
def get_timed_out_provisionals(self, cutoff_iso: str) -> list[dict]: ...
def finalise_timed_out_provisionals(self, cutoff_iso: str) -> int: ...
```

#### New Engine Function

```python
def _drain_pass2_queue(ha: HAClient) -> None:
    """Called from engine_loop_task on each tick — lightweight when queue empty."""
    blocks = _store.drain_pass2_queue()
    for full_block in blocks:
        _apply_pass2(full_block)
        append_block_replace(full_block)
    if blocks:
        loop.create_task(_deferred_sensor_update(ha, _store.get_cumulative_totals()))
```

#### Boundary Read Retention

No change needed. The block row contains everything PASS 2 needs
(`imp_kwh` per meter, `imp_rate`). The re-run reads from DB, not from
in-memory reads.

### Provisional Block Display Policy

#### Current State

`is_provisional` exists on `blocks` but is unused by every query surface.
In HA mode this is invisible — provisional state is transient (seconds to
minutes). In API mode every block < 48h is genuinely provisional.

Two additional concerns:

**Missing DCC data.** If a block never receives a Kraken upsert it stays
provisional indefinitely. Without a policy it silently accumulates in billing
totals.

**No sub-meter attribution until settlement.** In pure API mode, PASS 2 is
skipped until Kraken arrives. `kwh_grid` and `kwh_remainder` are NULL for up
to 48h. Charting these would show misleading attribution.

#### Two-Tier Display

**Tier 1 — Accuracy-critical (finalised blocks only):**
- Billing chart (period totals, daily breakdown, bill summary)
- Usage Stats cost columns and period totals
- Live Power billing cards (today/month/year cost)
- Insights aggregations

**Tier 2 — Recency-critical (all blocks, provisional distinguished):**
- Usage Stats kWh bar chart segments
- Current in-progress block on Live Power
- 48-hour generation mix chart

#### Visual Treatment

Provisional blocks on Tier 2 surfaces:
- Reduced opacity (0.4–0.5) on bar chart segments
- Hatching pattern (CSS diagonal stripe)
- Tooltip: "⏳ Provisional — awaiting supplier settlement" (kWh only, no cost)
- Banner when range includes provisional blocks:
  `"⏳ Data for the last [N] hours is awaiting supplier settlement and is
  not included in cost totals. Figures will update automatically once confirmed."`

Banner suppressed in HA-only mode. Shown only when `kraken_api.enabled = true`.

#### Query Layer Changes

All cost-aggregating methods gain `finalised_only: bool = False`.
When `True`, WHERE clause adds `AND b.is_provisional = 0`.
Defaults `False` — HA-only behaviour completely unchanged.
Server.py passes `True` when `kraken_api.enabled`.

Methods affected: `get_billing_totals_for_utc_range()`, `_aggregate_insights()`,
`api_blocks_summary()` cost columns.

`get_blocks_lightweight()` adds `b.is_provisional` to SELECT for chart layer.

#### Provisional Timeout (DCC Gap Handling)

Blocks that never receive a Kraken upsert must eventually be finalised in place.
Daily sweep in ingester (configurable via `kraken_gap_timeout_days`, default 30):

```python
def _finalise_timed_out_provisionals(self) -> int:
    cutoff = (now_utc - timedelta(days=PROVISIONAL_TIMEOUT_DAYS)).isoformat()
    # UPDATE blocks SET is_provisional=0 WHERE is_provisional=1
    #   AND block_start < cutoff AND source != 'kraken_api'
    # Log WARNING for each block timed out
```

Blocks with `source = 'kraken_api'` and `is_provisional = 1` are within their
expected 48h window — not timed out.

### Schema Changes

New columns via `ALTER TABLE` in `_migrate()`:

| Column | Table | Type | Default | Purpose |
|--------|-------|------|---------|---------|
| `source` | `blocks` | TEXT | NULL | `'ha_sensor'` or `'kraken_api'` |
| `is_provisional` | `blocks` | INTEGER | 0 | 1 = main meter not yet DCC-settled |
| `needs_pass2_rerun` | `blocks` | INTEGER | 0 | 1 = Kraken upserted, PASS 2 pending |

Note: `imp_provisional` (existing) = sub-meter boundary read missing, clears in
minutes. `is_provisional` (new) = main meter not DCC-settled, clears ~48h.
Different semantics, different columns.

Unique index for `upsert_kraken_block` ON CONFLICT:

```sql
CREATE UNIQUE INDEX IF NOT EXISTS idx_blocks_start_meter
    ON blocks(block_start, meter_id);
```

Check for duplicates before adding — deduplicate if any exist (keep highest
`block_id`).

### BottlecapDave Detection at Auto-Discover

Auto-discover checks HA entity registry for BottlecapDave sensors matching
the discovered MPAN/serial before reporting quota implications.

Sequence: discover MPAN/serial → construct expected sensor names →
`ha_client.get_state("sensor.octopus_energy_electricity_<serial>_<mpan>_current_rate")`
→ report findings in settings UI.

Settings UI outcome:

```
✓ Octopus Energy integration detected in Home Assistant
  Rate data will be read from HA sensors (no API quota used)
  ✓ Home Mini detected in HA — live power from HA sensor (no API quota used)
```

vs.

```
⚠ No Octopus Energy integration detected
  Rates will be fetched from Supplier API (~2 calls/day)
  ✓ Home Mini registered on account
    Live power will be polled via Supplier API (~40 calls/hour at 90s)
```

New config fields set at auto-discover time:

```json
{
  "kraken_api": {
    "ha_rate_sensor_detected": true,
    "ha_standing_charge_sensor_detected": true,
    "ha_mini_sensor_detected": false,
    "ha_mini_sensor_entity_id": null
  }
}
```

### Live Power Source Priority

1. HA CAD sensor (existing behaviour, unchanged)
2. BottlecapDave Mini sensor via HA (read via ha_client, zero API calls)
3. Kraken GraphQL Mini poll (only if neither above is available)

### Known Limitations in 3.0.0

**Intelligent Octopus — pure API mode:** The REST rate endpoint returns standard
off-peak/peak rates. Dispatch-period corrections require GraphQL dispatch
overlay. For BottlecapDave users this is already handled. For pure API users on
IO, block costs will use standard rates; dispatch corrections are a future
enhancement.

**Mini vs DCC settlement:** Mini HAN data and DCC settlement data can differ.
EMT uses REST consumption for blocks and Mini for live power display only.

**Multi-property accounts:** Not supported in 3.0.0. Error shown at auto-discover.

**SMETS1 meters:** Data latency may exceed 48h. The provisional timeout
(`kraken_gap_timeout_days`, default 30) handles this by eventually finalising
blocks in place.

### Configuration Schema

```json
{
  "kraken_api": {
    "enabled": true,
    "supplier": "octopus",
    "api_key": "sk_live_xxxxxxxxxxxx",
    "account_number": "A-AAAA1111",
    "import_mpan": "1000000000000",
    "import_serial": "1111111111",
    "export_mpan": "2000000000000",
    "export_serial": "2222222222",
    "meter_device_id": "AA-BB-CC-DD-EE-FF-GG-HH",
    "import_tariff_code": "E-1R-FLUX-IMPORT-23-02-14-A",
    "import_product_code": "FLUX-IMPORT-23-02-14",
    "export_tariff_code": "E-1R-FLUX-EXPORT-23-02-14-A",
    "export_product_code": "FLUX-EXPORT-23-02-14",
    "ha_rate_sensor_detected": true,
    "ha_standing_charge_sensor_detected": true,
    "ha_mini_sensor_detected": false,
    "ha_mini_sensor_entity_id": null,
    "poll_interval_hours": 6,
    "live_poll_interval_seconds": 90,
    "backfill_days": 90,
    "kraken_gap_timeout_days": 30
  }
}
```

### New Files

- `kraken_api_client.py` — REST + GraphQL client, JWT token management, rate
  limit tracking, supplier routing, `auto_discover()`, `test_connection()`
- `kraken_ingester.py` — polling loop, conditional rate lookup, block upsert,
  provisional timeout sweep, live poll task

### Files to Modify

- `block_store.py` — schema migration, new methods, `finalised_only` parameter
- `engine.py` — `_drain_pass2_queue()`, call from `engine_loop_task`
- `main.py` — ingester asyncio tasks, mode detection
- `server.py` — Kraken API routes, Live Power source priority,
  `finalised_only` flag on aggregations
- `web/templates/settings.html` — Supplier API tab (third tab)
- `web/templates/live_power.html` — Kraken Mini fallback data path
- `web/templates/charts.html` — provisional block visual treatment

### Open Questions

1. **Rate limit increase** — can the 100/hour limit be raised for dedicated
   integrations? BottlecapDave FAQ notes this is possible via account settings.
   Relevant mainly for pure API users with Mini live poll.

2. **Mini vs settlement agreement** — do REST consumption figures and Mini HAN
   figures agree once settled? Needs verification against a real account.

3. **Export MPAN** — always a separate MPAN, or a register on the import MPAN
   for some meter types? Account endpoint should confirm during testing.

4. **GraphQL token lifetime** — assumed 55 minutes. `kraken_api_client.py` uses
   5-minute refresh buffer. Needs confirmation against a real account.

5. **BottlecapDave rate sensor, no CAD** — user has BottlecapDave but no HA
   main meter sensor. Consumption from Kraken, rate from BottlecapDave HA sensor.
   This path should work via `rate_sensor` detection logic but needs explicit
   test coverage.

### Suggested Development Order

1. **Schema migration** — `source`, `is_provisional`, `needs_pass2_rerun`
   columns; unique index on `(block_start, meter_id)`

2. **BlockStore new methods** — `upsert_kraken_block`, `get_block_by_start`,
   `get_block_by_id`, `drain_pass2_queue`, `get_timed_out_provisionals`,
   `finalise_timed_out_provisionals`; `finalised_only` parameter on aggregations

3. **PASS 2 re-run queue** — `_drain_pass2_queue()` in engine, call from
   `engine_loop_task` — required before ingester goes live

4. **`kraken_api_client.py`** — REST + GraphQL client, unit-tested against
   mocked responses

5. **`kraken_ingester.py`** — consumption-only first (no rate fetching),
   provisional timeout sweep

6. **Settings UI** — credential entry, auto-discover, BottlecapDave detection,
   quota impact display, connection test

7. **main.py integration** — ingester task, mode detection

8. **Provisional block UI** — `finalised_only` queries, visual treatment on
   charts, provisional banner

9. **Pure API rate fetching** — REST rate and standing charge endpoints, cached,
   only when no HA rate sensor configured

10. **End-to-end test** — real Octopus account, second EMT instance on separate
    port and DB, both hybrid (with BottlecapDave) and pure API paths verified.
    Key checks: CAD kWh vs settled DCC kWh per block over 90-day backfill;
    PASS 2 re-run cycle observed within short provisional window

11. **GraphQL Mini live poll** — only after 1–10 are solid; only for pure API
    users where BottlecapDave Mini sensor absent

12. **IO dispatch rate correction for pure API users** — future version
