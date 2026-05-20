# Development Guide — Volume 3

Covers decisions made during 2.10.0 development and the design for 2.11.0 Octopus API integration.

---

## 2.10.0 — Key Decisions and Fixes

### Rounding Methodology (Critical)

All cost calculations across every surface now follow a single principle:
**accumulate raw float block costs, round once to 2dp at the final display step.**

This matches how Octopus actually bills (sum raw slot costs, round once) and eliminates systematic ±£0.01 drift between surfaces.

**Surfaces affected and their code paths:**

| Surface | File | Function |
|---------|------|----------|
| Billing chart Bill Summary | `energy_charts.py` | `calculate_billing_summary_for_period` |
| Billing chart day sidebar | `energy_charts.py` | `build_day_chart_html` → `meter_totals` |
| Live Power billing cards | `server.py` | `_fmt_total` |
| Usage Stats bars/table | `server.py` | `api_blocks_summary` |
| Usage Stats JS period total | `charts.html` | `barAggRows` → `barPeriodTotals` |

**Key implementation details:**

- `calculate_billing_summary_for_period`: removed `_day_accum` per-day rounding. Raw block costs accumulate directly into `meter_summary` and `meter_totals`. `main_import_raw` and `main_export_raw` track raw main meter totals separately so `total_cost` is computed from unrounded values before the display-rounding pass. Round once at end: 2dp cost, 3dp kWh.

- `build_day_chart_html`: `meter_totals` built with `sum(abs(v) for v in meter_cost[meter])` — raw sum then `round(cost_sum, 4)`. Previously used `sum(round(abs(v), 4) for v in ...)` which accumulated rounding error across slots.

- `_fmt_total`: `total = round(float(imp_cost) + float(standing) - float(exp_cost), 2)` — raw sum round once. Previously rounded each component to 2dp before summing.

- `api_blocks_summary`: all per-day values sent at **4dp** (previously 2dp for imp_cost/exp_cost, 2dp for standing). Includes `period_totals` dict with pre-computed SQL totals for each billing period and `_full` range total. JS `barAggRows` uses `barPeriodTotals` for cost display instead of summing daily rows, eliminating accumulated rounding error over long periods (97+ days → ±£0.01 without this).

- `main_export_raw`: added as a separate accumulator in `calculate_billing_summary_for_period` so `_raw_main_exp` reads unrounded export totals. Previously read from already-rounded `meter_totals`.

### Live Power Billing Card Cache Bug

`api/billing` was missing `Cache-Control: no-store` headers. Browser cached the response, causing billing cards to show stale data even after the scheduled `refreshBilling()` call fired at 1 minute past each block boundary. Also added `?t=Date.now()` cache-busting to the fetch call in `live_power.html`.

**Confirmed with screenshots:** at 13:03 (post-boundary) Live Power showed stale values while Usage Stats at 13:04 already showed the 13:00 block. Returning to Live Power at 13:05 (fresh page load) showed correct updated values.

### PDF Export Fixes

- **Billing tab**: captures current period and view (Bill/vs Prev/vs Last Year), billing summary, daily chart images via `Plotly.toImage()`, open data tables. Light theme forced.
- **Heatmaps tab**: active metric heatmap via `Plotly.toImage()`.
- **Usage Stats tab**: chart image, toolbar state, data table. Checkbox state fix: `cloneNode(true)` copies DOM attributes but not JS-set `.checked` property. Fixed by replacing checkboxes with Unicode ☑/☐ indicators in the PDF clone.

### Files Changed in 2.10.0

- `energy_charts.py` — billing summary rounding, sidebar rounding, PDF export, per-day data table, Show/Hide Data button, Blob URL iframe fix, Grid Export label fix, kwh_remainder usage
- `server.py` — Live Power card rounding, api_blocks_summary precision, period_totals, no-cache headers on api/billing
- `charts.html` — PDF export (all tabs), barAggRows period_totals, barPeriodTotals JS var, checkbox PDF fix
- `live_power.html` — cache-busting timestamp on billing fetch
- `block_store.py` — unchanged in 2.10.0
- `engine.py` — unchanged in 2.10.0

### Test Suite

`test_usage_stats_vs_billing.py` — 46 tests covering:
- Daily/monthly/yearly billing vs SQL agreement
- Non-round standing charge (£0.504559/day) over 16+ days
- Sub-meter cost precision over multi-day periods
- Live Power net total vs Usage Stats net total agreement
- The real-world scenario that produced -£3.11 / -£3.22 / -£3.14 / -£3.15 discrepancies

---

## 2.11.0 — Kraken API Integration Design

### Vision

The unique value of EMT is insight **behind the meter** — sub-meter attribution, device-level carbon, battery/EV accounting — that any supplier API can never provide because they only see the grid boundary. The Kraken platform API adds complementary value: authoritative billing figures, tariff history, and (with a compatible smart device) near-real-time live power without a CAD/HA dependency.

**Kraken, not Octopus:** Kraken Technology (formerly part of Octopus Energy) is now a separate platform that powers multiple energy suppliers — Octopus Energy, EDF, and others. The API (both REST and GraphQL), authentication mechanism (`obtainKrakenToken`), and data structures are Kraken platform concepts, not Octopus-specific. EMT targets the Kraken platform. Octopus is the first supported supplier because it's the most common among EMT users, but EDF and other Kraken-powered suppliers follow the same pattern with only the base URL and supplier name differing.

The goal is not to replace HA sensor data but to **augment it** — using the Kraken API as an authoritative cross-check for main meter data and as an alternative live power source, while HA sensors continue to provide the sub-meter detail that makes EMT uniquely useful.

### What the Kraken API Provides

#### REST API

Base URL is supplier-specific (e.g. `https://api.octopus.energy/v1/` for Octopus, different for EDF). Auth: HTTP Basic, API key as username, empty password. The endpoint structure and response format are identical across Kraken-powered suppliers.

| Endpoint | Returns | Notes |
|----------|---------|-------|
| `/v1/accounts/<account>/` | MPAN, serial numbers, tariff history | Full account metadata |
| `/v1/electricity-meter-points/<mpan>/meters/<serial>/consumption/` | `interval_start`, `interval_end`, `consumption` (kWh) | **No rate, no cost, no is_estimated flag** |
| `/v1/products/<product>/electricity-tariffs/<tariff>/standard-unit-rates/` | Rate periods with `valid_from`/`valid_to`, `value_inc_vat` | Agile: one record per half-hour |
| `/v1/products/<product>/electricity-tariffs/<tariff>/standing-charges/` | Daily standing charge periods | |

**Critical limitation:** The consumption endpoint returns only `consumption` (kWh) — no rate, no cost, no indication of whether the reading is provisional or finalised. There is no `is_estimated` field in the REST API.

**Data latency:** Typically 24-48 hours for SMETS2, potentially longer for SMETS1. There is no programmatic way to determine if a block is final — recency implies provisional by convention.

**Rate limit:** 100 calls per hour, **shared across all Octopus API usage** including the Octopus app and other integrations. Hard constraint on polling frequency.

#### GraphQL API

Endpoint is supplier-specific (e.g. `https://api.octopus.energy/v1/graphql/` for Octopus). Auth: JWT token obtained via the `obtainKrakenToken` mutation — the mutation name is Kraken-native and identical across all Kraken-powered suppliers. Tokens are short-lived.

**Key queries:**

```graphql
# Live power (requires Octopus Mini)
smartMeterTelemetry(
  deviceId: "<METER_GUID>"
  grouping: TEN_SECONDS
  start: "2026-05-18T00:00:00+01:00"
  end: "2026-05-18T01:00:00+01:00"
) {
  readAt
  consumptionDelta   # kWh since last reading
  demand             # instantaneous watts
  costDelta          # cost since last reading
  consumption        # cumulative kWh
}

# Account/meter discovery
account(accountNumber: "<ACCOUNT>") {
  electricityAgreements(active: true) {
    meterPoint {
      meters(includeInactive: false) {
        smartDevices { deviceId }  # GUID for smartMeterTelemetry
      }
    }
  }
}
```

**GraphQL provides what REST cannot:**
- Live power (watts) via Octopus Mini — updates every ~30 seconds
- `costDelta` alongside consumption — rate already applied by Octopus
- `demand` (instantaneous W) for live power gauge
- Access to Octopus Mini data — the REST consumption endpoint only returns published DCC settlement data, not Mini data

**GraphQL is the right choice** for consumption and live power. REST remains appropriate for tariff rate lookups.

### Architecture: Augmentation Not Replacement

Sub-meter data (Zappi, Solax etc.) is **not available from Octopus** — they only see the grid boundary. The Octopus API cannot replace HA sensors for sub-meter users. The correct framing is:

| Data | Source |
|------|--------|
| Main meter import/export kWh | Octopus API (authoritative) OR HA sensor (real-time) |
| Live power (watts) | Octopus Mini via GraphQL OR HA sensor |
| Sub-meter kWh (Zappi, Solax etc.) | HA sensors only — Octopus cannot see these |
| Tariff rates | Octopus REST API (authoritative) |
| Carbon intensity | National Grid ESO API (unchanged) |

**Two deployment modes:**

1. **HA-only** — existing behaviour, unchanged.

2. **API mode** — main meter from Octopus API (authoritative). Sub-meters from HA sensors if configured, absent if not — this is just configuration, not a separate mode. Live power from Octopus Mini if `meter_device_id` is configured, absent if not. The code handles missing sub-meters and missing Mini gracefully in both cases — it's the same code path whether sub-meters are absent because the user has no devices, or because they're running API-only with no HA.

### Provisional Block Strategy

Since the REST API provides no `is_estimated` flag:

- Blocks with `block_start` < 48 hours ago: `is_provisional = 1`
- Blocks ≥ 48 hours old: `is_provisional = 0` (finalised)
- On each poll: re-fetch last 48 hours and update any block where `consumption` changed
- On daily reconciliation: re-fetch last 14 days to catch delayed revisions

The existing `is_provisional` column in `blocks` supports this already.

### Rate Attribution

Consumption endpoint returns kWh only — rate must be looked up separately.

**Tariff code discovery:**

Both a tariff code (e.g. `E-1R-FLUX-IMPORT-23-02-14-A`) and product code (e.g. `FLUX-IMPORT-23-02-14`) are needed to construct the rate endpoint URL:
`/v1/products/<product_code>/electricity-tariffs/<tariff_code>/standard-unit-rates/`

Both are auto-discoverable from the account endpoint — the `agreements` array on each meter point includes the tariff code with `valid_from`/`valid_to` dates. The product code is embedded in the tariff code (everything between the rate type and the region suffix, e.g. stripping `E-1R-` prefix and `-A` region suffix).

**Tariff history:** A user may have changed tariff mid-history. The account endpoint returns the full agreement history, so the ingester can reconstruct which tariff applied at any historical point — critical for accurate backfill.

**Export tariffs:** Export has its own separate tariff code on its own MPAN (e.g. `E-1R-FLUX-EXPORT-23-02-14-A`). Export rates must be fetched from a separate rate endpoint. Both import and export tariff codes are discoverable from the account endpoint.

**Rate lookup process:**

1. Fetch full tariff agreement history from account endpoint
2. For each consumption block, find the tariff code valid at `interval_start`
3. Fetch the rate schedule for that tariff covering the required date range
4. Build sorted rate schedule: `(valid_from, valid_to, rate_inc_vat)`
5. Binary search for rate valid at each `interval_start`
6. Agile: one rate record per half-hour, 1:1 with consumption records
7. Flux/Go: time-of-use bands
8. Fixed-rate: single record

Compute `imp_cost = consumption × (rate_inc_vat / 100)` at ingestion time. Rates from Octopus are in pence/kWh inc VAT; divide by 100 for £/kWh consistent with existing EMT convention.

**Known limitation — Intelligent Octopus:** Off-peak rates during smart charge dispatches are not reflected in the standard rate endpoint — they only appear via GraphQL. Users on Intelligent Octopus tariffs will have dispatch periods attributed the standard rate rather than the off-peak dispatch rate. This is a known limitation for 2.11.0; a GraphQL dispatch rate lookup path can be added in a later version.

### Live Power via Octopus Mini

The `smartMeterTelemetry` GraphQL query provides `demand` (watts) and `consumptionDelta` (kWh) at ~30 second granularity. This maps onto the existing Live Power page:
- `demand` → live power gauge (W)
- `consumptionDelta` → 48-hour generation mix chart accumulation
- `costDelta` → live cost accumulation

**Rate limit constraint:** At 30-second polling, live power alone consumes 120 calls/hour, exceeding the 100/hour limit. Realistic options:
- Poll every 60-90 seconds (60-90 calls/hour, leaving headroom for other calls)
- Confirm with Octopus whether the limit can be raised for dedicated integrations
- The HA Octopus Energy integration FAQ notes the 100/hour limit can be increased via account settings

### User Setup Flow

Users should not need to understand GraphQL, MPANs, tariff codes, product codes or meter GUIDs — these are implementation details. The setup flow from the user's perspective:

1. **Enter API key** — available from their Octopus dashboard (users of other Octopus integrations already have this)
2. **Enter account number** — on their bill, format `A-AAAA1111`
3. **Click Auto-discover** — EMT hits the account endpoint and derives everything else: MPAN, serial numbers, tariff code, product code, tariff history, and Mini device GUID if a Mini is registered
4. **Done** — user confirms what was found, saves

Discovered values (MPAN, tariff code etc.) are shown as read-only for transparency but are not user-input fields.

**Multiple properties:** 2.11.0 supports single-property accounts only. If the account has multiple properties, show an error: "Multiple properties detected on this account — please contact support." Multi-property support can be added in a later version.

**Mini detection:** If no Mini device is found during auto-discover, live power via Octopus API is simply not enabled. A note in the UI explains that an Octopus Mini would enable this feature.

### Configuration Schema

The user only inputs `api_key` and `account_number`. Everything else is populated by auto-discover and stored in `meters_config.json`:

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
    "poll_interval_hours": 6,
    "live_poll_interval_seconds": 60,
    "backfill_days": 90
  }
}
```

- `supplier` — selects the base URLs for REST and GraphQL. Known values: `octopus`, `edf`. Determines `base_url` automatically; never user-entered.
- `export_mpan`/`export_serial`/`export_tariff_code`/`export_product_code` — absent if no export meter found
- `meter_device_id` — absent if no compatible smart device found; omitting disables API live power
- `poll_interval_hours` — how often to fetch new consumption blocks from REST API
- `live_poll_interval_seconds` — how often to poll smart device via GraphQL for live power
- `backfill_days` — how far back to fetch on first run

**Supplier base URLs** (maintained in `kraken_api_client.py`):

```python
KRAKEN_SUPPLIERS = {
    "octopus": {
        "rest_base":    "https://api.octopus.energy/v1",
        "graphql_url":  "https://api.octopus.energy/v1/graphql/",
    },
    "edf": {
        "rest_base":    "https://api.edfenergy.com/v1",   # confirm
        "graphql_url":  "https://api.edfenergy.com/v1/graphql/",  # confirm
    },
}
```

EDF URLs need confirmation — the pattern is assumed to be the same as Octopus but must be verified before enabling EDF support.

### New Files

- `kraken_api_client.py` — thin wrapper for REST and GraphQL, JWT token management, rate limit tracking. Supplier-agnostic: base URL is a constructor parameter derived from the configured supplier.
- `kraken_ingester.py` — polling logic, block construction from API data, rate lookup, upsert into `blocks.db`. Supplier-agnostic.

### Files to Modify

- `main.py` — new asyncio task for Kraken ingester and smart device live poller
- `engine.py` — in API+HA mode, skip main meter block writing from HA sensor if Kraken API provides it; still write sub-meter blocks from HA
- `server.py` — Live Power page: smart device data path alongside existing HA path
- `web/templates/live_power.html` — handle Kraken smart device live data format
- `web/templates/settings.html` — Kraken API section: supplier dropdown, credentials, auto-discover, connection test
- `block_store.py` — consider adding `source` column to `blocks` (`ha_sensor` vs `kraken_api`) for debugging

### Open Questions Before Coding

1. **Rate limit** — can it be raised via account settings for dedicated integrations? The HA integration notes this as possible.

2. **Mini vs settlement data** — are REST consumption data and GraphQL Mini data the same readings once settled, or can they differ? Mini uploads every 10 seconds but settlements go through DCC. Need to verify against a real account.

3. **Export MPAN** — always a separate MPAN, or a register on the import MPAN for some meter types? Account endpoint should clarify.

4. **Mode arbitration** — if both HA sensor and Octopus API provide main meter data for the same block, which wins? Proposal: Octopus API data is authoritative once ≥48h old; HA sensor data used for recent blocks not yet available in API.

5. **Sub-meter absence in API-only mode** — billing chart, Usage Stats and Insights handle missing sub-meters gracefully already (they're optional everywhere). Confirm no hardcoded assumptions about sub-meter presence.

6. **GraphQL token lifetime** — how long do JWT tokens last? Need refresh strategy in `octopus_api_client.py`.

### Suggested Development Order

1. `kraken_api_client.py` — REST + GraphQL client with token management, rate limit tracking, and supplier routing. Fully unit-tested against mocked responses. Verify EDF endpoint URLs before enabling EDF support.
2. `kraken_ingester.py` — block construction, rate lookup, upsert logic.
3. Settings UI — credential entry, auto-discover (account → MPAN/serial/tariff/device ID), connection test
4. `main.py` — new asyncio ingester task, mode detection
5. Data Management UI — sync status, last sync timestamp, manual sync button
6. API mode integration — graceful handling of absent sub-meters and absent Mini; main meter arbitration between API and HA sensor data in engine
8. Live power via Mini — GraphQL polling, live power page adaptation, rate limit management
9. End-to-end test with a real Octopus account
