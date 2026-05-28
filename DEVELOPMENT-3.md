# Development Guide — Volume 3

Covers decisions made during 2.10.x development and the design for 3.0.0
Kraken API integration.

---

## 2.10.x — Key Decisions and Fixes

### Cost Accounting Methodology (Final State: 2.10.8)

The 2.10.x series iterated through several approaches to cost accounting
before landing on a stable architecture in 2.10.8.

#### Final architecture

`BlockStore.compute_period_net()` is the single shared implementation for
all three billing surfaces. Any future methodology change has one place.

| Surface | Calls | Method |
|---------|-------|--------|
| Billing chart | `calculate_billing_summary_for_period()` | Calls `store.compute_period_net()` |
| Overview billing cards | `_fmt_total()` / `_compute_period_net()` | Thin wrapper around store method |
| Usage Stats | `api_blocks_summary` inline | Same logic, tested to agree |

All surfaces produce consistent figures. The ±£0.01 artefact that existed
between billing chart and Usage Stats in 2.10.4–2.10.7 is resolved.

#### Why cost_remainder is kwh × rate not main_cost − sub_costs

`main_cost` (register reads) and sub-meter costs (power sensor integration)
are independent measurement systems. Subtracting creates negative remainders.
Rate-based subtraction at the presentation layer avoids accumulation.

#### The rounding journey (context only)

2.10.0: raw float accumulate, round once. `barPeriodTotals` as SQL workaround
for Usage Stats drift over long periods.
2.10.3: `barPeriodTotals` grand total bug found (single-day period keys).
2.10.4: unified `net_cost = round(imp + standing - exp, 2)` per day — adopted
in `charts.html` and `server.py _fmt_total` but reverted in `energy_charts.py`
because billing chart `total_cost` was wrong. Left a ±£0.01 artefact.
2.10.8: `BlockStore.compute_period_net()` resolves the artefact by giving
all surfaces the same implementation.

### Release Notes Summary

| Version | Key change |
|---------|-----------|
| 2.10.0 | Raw float accumulation, round once. PDF export. `barPeriodTotals`. |
| 2.10.2 | Fresh install WAL hang fix. Startup error handling. |
| 2.10.3 | `barPeriodTotals` grand total bug partial fix. |
| 2.10.4 | `net_cost` methodology in `charts.html` + `_fmt_total`. `energy_charts.py` reverted. |
| 2.10.5 | Usage Stats auto-refresh — `data-chart` attribute fix. |
| 2.10.6 | Sub-meter sensor reset handling (negative delta → post-reset value). |
| 2.10.7 | Boundary-aware `scheduleChartRefresh()` replaces `setInterval`. WAL lock retry in `load_config` (3× with 100ms delay). `block_minutes` on `api/blocks_summary`. |
| 2.10.8 | `BlockStore.compute_period_net()` — single shared implementation for all surfaces. `TestDeviceRetirement` SQL `None`→`NULL` fix. |

### Other Notable Fixes

**Live Power billing card cache bug** — `api/billing` missing `Cache-Control:
no-store`. Fixed with no-store headers and `?t=Date.now()` cache-busting.

**Sub-meter sensor reset (2.10.6)** — negative delta mid-block (e.g. Teslemetry
Powerwall daily midnight reset) previously recorded zero consumption. Engine
now uses post-reset value directly. Pre-reset partial discarded — reset point
not reliably determinable.

**WAL lock retry in `load_config` (2.10.7)** — engine write lock at the exact
moment the server's `load_config` executed its config period query caused
`api/blocks_summary` to return an empty config, producing missing import values
and wrong bar colours. Fixed with 3× retry at 100ms intervals.

**Fresh install hang (2.10.2)** — `conn.backup()` on freshly-created WAL DB
left an open read transaction. Fixed by skipping upgrade backup on fresh installs.

### Files Changed Across 2.10.x

- `block_store.py` — `compute_period_net()` (2.10.8), WAL retry in config queries
- `energy_charts.py` — rounding, PDF export, data table, Show/Hide Data
- `server.py` — `_fmt_total`, `_compute_period_net()` wrapper, `api_blocks_summary`,
  no-cache headers, `block_minutes` (2.10.7), `load_config` WAL retry (2.10.7)
- `charts.html` — PDF, `net_cost` accumulation, `barPeriodTotals` removed,
  `scheduleChartRefresh()` (2.10.7)
- `live_power.html` — cache-busting timestamp
- `engine.py` — fresh install hang (2.10.2), cost_remainder comment,
  sensor reset handling (2.10.6)
- `main.py` — startup error handling (2.10.2)
- `test_usage_stats_vs_billing.py` — daily/monthly/yearly vs SQL, non-round
  standing charge, sub-meter precision, BST/GMT transitions,
  the real-world -£3.11/-£3.22/-£3.14/-£3.15 discrepancy scenario
- `test_block_store.py` — `TestDeviceRetirement` SQL None→NULL fix (2.10.8)

---

## 3.0.0 — Kraken API Integration

### Why 3.0.0

Pure API mode removes the requirement for a CAD or HA main meter sensor.
From 3.0.0 a user can install EMT, enter Octopus credentials, and get a
fully functional dashboard with zero additional hardware. This changes who
the software is for. Major version bump is warranted.

The 2.x HA sensor path is unchanged. Config is backwards compatible —
`kraken_api` is a new optional block, `mode = "cad"` is the migration default.

### Vision

The unique value of EMT is insight **behind the meter** — sub-meter
attribution, device-level carbon, battery/EV accounting — that a supplier API
can never provide. Kraken adds: authoritative settled kWh from DCC, and (for
users without BottlecapDave or a CAD) rates.

**Kraken, not Octopus:** Kraken Technology powers multiple suppliers.
Authentication (`obtainKrakenToken`) and data structures are Kraken platform
concepts. Other suppliers differ only in base URL.

### The Four Modes

Each mode is an explicit `mode` field on the main meter config. No inference
from populated fields — the mode is declared.

| Mode | `mode` value | Live source | Billing finalised by | `is_provisional` |
|------|-------------|-------------|---------------------|-----------------|
| CAD only | `"cad"` | CAD (10s) | CAD immediately | Never |
| CAD + API | `"cad+api"` | CAD (10s) | DCC settlement | Never |
| API only | `"api"` | None | DCC settlement (~48h) | ~48h |
| API + Mini | `"api+mini"` | Mini (provisional) | DCC settlement (~48h) | ~48h |

**CAD + Mini** and **CAD + API + Mini** are out of scope for 3.0.0.
If CAD is present it takes priority; Mini is ignored.

**`"api+mini"` is closer to `"api"` than to `"cad"`**. The Mini provides a
provisional main meter figure sooner than REST settlement, but it does not
eliminate provisional status. Every Mini-derived block stays `is_provisional=1`
until DCC settlement confirms or corrects it. Billing figures carry the same
"subject to DCC confirmation" caveat as `"api"` mode.

#### Mode: CAD (`"cad"`)

Existing 2.x behaviour, completely unchanged. `mode = "cad"` is set by
migration for all existing users — zero behaviour change on upgrade.

#### Mode: CAD + API (`"cad+api"`)

Both columns always populated independently:
- Engine always writes `imp_kwh` from CAD at block boundary time
- Ingester always writes `imp_kwh_api` from DCC when it arrives (~48h)
- Neither column ever overwrites the other — they coexist permanently

Billing uses whichever column `billing_source` is set to (see below).
Backfill always runs — ingester fills `imp_kwh_api` for all blocks within
`backfill_days` on first run and continuously thereafter. No opt-in prompt.

When `imp_kwh_api` arrives and PASS 2+3b conditions are met, attribution
and carbon re-run against whichever figure `billing_source` specifies.
If `billing_source = "api"`: re-run against `imp_kwh_api`.
If `billing_source = "cad"`: `imp_kwh_api` stored for diagnostics only,
PASS 2+3b not re-run (existing CAD-derived attribution stands).

**No overwrite guard needed in engine.** The engine writes `imp_kwh`;
the ingester writes `imp_kwh_api`. These are separate columns. No
collision is possible.

#### Mode: API Only (`"api"`)

No live source. Blocks written at boundary with `imp_kwh = NULL`,
`imp_rate` from HA rate sensor, `is_provisional = 1`. PASS 2 and PASS 3b
deferred. When DCC settlement arrives: `imp_kwh_api` written, `imp_kwh`
populated from `imp_kwh_api` (no live source to preserve), `is_provisional = 0`,
PASS 2 and PASS 3b run for the first time.

Provisional timeout (default 30 days) for DCC gaps. If `imp_kwh` is still
NULL at timeout, block finalises with NULL billing kWh — excluded from all
billing aggregations permanently via COALESCE. This is safer than writing a
zero. See open question on timeout handling.

#### Mode: API + Mini (`"api+mini"`)

The Mini derives a provisional main meter figure at boundary time via register
reads. Block written immediately: `imp_kwh` (Mini figure), `is_provisional = 1`.
PASS 2 and PASS 3b run provisionally at write time — attribution and carbon
are computed but marked provisional.

**Why always provisional:**
The Mini data path traverses the internet twice — meter HAN → Mini, then
Mini → Octopus cloud. Either leg can fail silently. A broadband outage, Mini
reboot, or cloud hiccup means readings are simply absent from
`smartMeterTelemetry` with no error signal. When a boundary read is missing,
the register delta is wrong or must be interpolated. Unlike a local CAD
(where HA sensor goes `unavailable` immediately), EMT cannot know at query
time whether a Mini reading is missing or delayed. All Mini-derived blocks
must therefore remain provisional until DCC confirms them.

**No export lifetime register:**
The Mini `consumption` field is import-only (assumed). There is no export
register accessible via `smartMeterTelemetry`. Export kWh in `"api+mini"`
mode is REST settlement only — full 48h delay, same as `"api"` mode. Export
`imp_cost` is NULL at block write time.

**DCC settlement (~48h):** `imp_kwh_api` written. If drift policy allows:
`is_provisional = 0`, PASS 2 and PASS 3b re-run against DCC figure. Mini
figure preserved in `imp_kwh`. `imp_kwh_api` becomes authoritative billing
figure via COALESCE.

**The value of `"api+mini"` over `"api"`:**
- Provisional main meter kWh available sooner → Tier 2 bars show main meter
  and provisional attribution before DCC arrives
- Live power reading (watts) via `demand` field for the Overview Mini card —
  display only, not a billing figure
- Potentially useful diagnostic comparison between Mini register delta and
  DCC settlement over time

`"api+mini"` does not provide live billing. Cost cards carry the same
provisional treatment as `"api"` mode. The settings UI must communicate this
clearly when the user selects this mode.

### Column Semantics (Definitive)

```
imp_kwh        — live-source figure at write time (CAD or Mini)
                 In "api" mode: NULL at boundary, populated from imp_kwh_api
                 at settlement (no live source to preserve)
                 Never modified after initial write in "cad+api" / "api+mini"

imp_kwh_api    — DCC-settled figure from Kraken REST
                 NULL until settlement arrives
                 In "cad" mode: never populated

Billing kWh    — determined by billing_source setting:
                 "api":  COALESCE(imp_kwh_api, imp_kwh)  ← DCC, CAD as fallback
                 "cad":  imp_kwh                          ← always CAD/Mini
```

The `billing_source` toggle switches the entire billing view instantly
without modifying any data. Both columns are always retained.

### Billing Source Setting

A single user-facing toggle in Settings → Supplier API:

```
Billing source
○ Supplier settled data (recommended)
  Bills calculated from DCC-settled figures.
  Your device readings are retained for reference and drift analysis.
  Where DCC data is missing, device readings are used automatically.

○ Meter device readings
  Bills calculated from your CAD/Mini readings.
  DCC figures retained for comparison and drift reporting only.
```

Config field: `billing_source: "api"` (default) or `"cad"`.

Switching is instant — no data changes, only which column the billing
queries read from. The user can switch back and forth at any time.
Both figures are always stored regardless of which is selected.

**PASS 2+3b re-run behaviour by billing_source:**

`billing_source = "api"`: when `imp_kwh_api` arrives, PASS 2+3b re-runs
against `COALESCE(imp_kwh_api, imp_kwh)`. Attribution and carbon reflect
DCC-authoritative kWh.

`billing_source = "cad"`: when `imp_kwh_api` arrives, `imp_kwh_api` is
stored but PASS 2+3b does not re-run. Existing CAD-derived attribution
stands. Drift is computed and surfaced but does not affect any block data.

### DCC Drift and Reconciliation

#### What Drift Means

In `"cad+api"` and `"api+mini"` modes, `imp_kwh_api` (DCC) and `imp_kwh`
(CAD/Mini) coexist on every block. They may differ. Causes:

- Normal measurement tolerance (CT clamps especially)
- EMT gap-fills via interpolation during sensor outages
- DCC comms gaps filled with estimated reads (not flagged in API response)
- Genuine meter anomalies worth investigating

EMT cannot distinguish these causes programmatically — it surfaces the drift
and lets the user interpret it.

#### DCC Record Classification

**Missing record** (absent from API `results` array):
- `imp_kwh_api` stays NULL. No PASS 2+3b re-run.
- CAD/Mini figure used for billing automatically via COALESCE.
- No drift computed for this block.

**Zero record** (`consumption = 0.0`):
- Store in `imp_kwh_api`. Flag `needs_review = 1`.
- Genuine zero-consumption half-hours are possible (overnight) but
  non-overnight zeros are suspicious. User investigates.

**Positive record:**
- Always store in `imp_kwh_api`.
- Compute per-block drift: `(imp_kwh_api - imp_kwh) / imp_kwh × 100`.
- If `|delta| > drift_block_percent` (default 2%): flag `needs_review = 1`.
- If `billing_source = "api"`: PASS 2+3b re-runs.
- If `billing_source = "cad"`: PASS 2+3b does not re-run.

#### Period Drift Metric

```
period_drift% = (Σ imp_kwh_api - Σ imp_kwh) / Σ imp_kwh × 100
```

Shown on billing chart in `"cad+api"` and `"api+mini"` modes:
- `|period_drift| < drift_warn_percent`: "DCC variance: +1.3% ✓" (muted)
- `|period_drift| ≥ drift_warn_percent`: "⚠ DCC variance: −4.6%"

#### PASS 2+3b Re-run on DCC Settlement

When `billing_source = "api"` and `imp_kwh_api` arrives (positive, non-missing):
- PASS 2 re-runs against `COALESCE(imp_kwh_api, imp_kwh)` — corrects
  `kwh_grid`, `kwh_remainder`, `cost_remainder`
- PASS 3b re-runs — recomputes `carbon_g` using stored `carbon_intensity_g`
  × `COALESCE(imp_kwh_api, imp_kwh)`. No additional Carbon Intensity API call.
- Sub-meter carbon corrects from updated `kwh_grid` figures
- Export `imp_kwh_api` written from REST settlement — no live export source
  in any mode except `"cad"`

Sub-meter `imp_kwh` never modified. `imp_kwh` never modified.
Only derived attribution and carbon columns update.

When `billing_source = "cad"`: `imp_kwh_api` stored, PASS 2+3b not re-run.
Drift computed and surfaced. No block data changes.

#### Default Drift Thresholds

```json
{
  "kraken_api": {
    "drift_warn_percent": 5.0,
    "drift_block_percent": 2.0
  }
}
```

User-configurable. Below `drift_block_percent` per block: store `imp_kwh_api`,
run PASS 2+3b if `billing_source = "api"`, no `needs_review` flag.
Above threshold: same actions plus `needs_review = 1`.

#### The `needs_review` Drift Alert

`needs_review = 1` is a **diagnostic alert**, not a gating mechanism.
It does not prevent `imp_kwh_api` from being stored or PASS 2+3b from running.
It flags blocks where CAD/Mini and DCC disagree significantly for investigation.

Blocks surface in Settings → Supplier API as an informational list:

```
Drift alerts — 3 blocks
┌─────────────────────┬──────────┬──────────┬────────┐
│ Block               │ CAD kWh  │ DCC kWh  │ Delta  │
├─────────────────────┼──────────┼──────────┼────────┤
│ 26 May 13:00        │ 3.089    │ 0.000    │ −100%  │
│ 26 May 14:30        │ 2.847    │ 2.104    │ −26.1% │
│ 27 May 02:00        │ 0.412    │ 0.000    │ −100%  │
└─────────────────────┴──────────┴──────────┴────────┘
[Dismiss all]  [Dismiss individually]
```

No commit/keep decision — billing source setting already determines what
drives the numbers. Dismiss clears `needs_review` once investigated.

Drift history chart (period-by-period drift %) planned for Settings →
Supplier API — not a day-one 3.0.0 feature but designed for in the layout.

### CO2 Attribution and Generation Mix by Mode

#### How Carbon Works Today (2.x / `"cad"` mode)

Three separate carbon data stores, all written at `finalise_block` time:

**`blocks.carbon_g`** — net gCO2 for the block. Main meter:
`carbon_g = (imp_kwh_total - exp_kwh) × intensity`. Sub-meters:
`carbon_g = imp_kwh × intensity`. Used by the lifetime carbon heatmap
(`generate_net_heatmap`) which derives slot-level intensity as
`carbon_g / net_kwh` for the intensity heatmap metric.

**`generation_mix` table** — one row per fuel per block (`block_id` FK).
Written from `_current_slot_mix` in-memory dict at `finalise_block` time.
Lives as long as the block. Used for per-block fuel breakdown tooltips.
Stored against main meter block only — mix is a grid property.

**`mix_history` table** — 48h rolling store at CI-tick cadence (~15 min).
Written by `_tick_carbon_intensity()` independently of block finalisation.
Feeds the live 48h generation mix chart on the Overview page. Pruned to 48h.

The Carbon Intensity API (`_fetch_carbon_intensity`) returns both `intensity`
and `generationmix` in a single call. `generationmix` is already stored in
`_current_slot_mix` and `mix_history` — it's available at boundary time.

#### Key Finding: Intensity Not Stored on Block

`carbon_intensity_g` is **not** currently stored on the `blocks` table.
PASS 3b looks up intensity at finalise time via `get_nearest_carbon_intensity`
which queries the `carbon_intensity` table. That table is pruned to 4 days.

This creates a risk in API modes — if DCC settlement arrives > 4 days after
the block (SMETS1, long DCC gap), the intensity for that slot is gone and
`carbon_g` cannot be recomputed. The fix is to store `carbon_intensity_g`
directly on the block at boundary time. This is a new column, always populated
when a postcode is configured, regardless of mode or whether `imp_kwh` is NULL.

#### Carbon by Mode

**`"cad"` — no change:**
`finalise_block` runs. PASS 3b computes `carbon_g` from live intensity table.
`_current_slot_mix` written to `generation_mix`. All three stores populated.

**`"cad+api"`:**
At CAD write time: `carbon_intensity_g` stored on block. `carbon_g` computed
against `imp_kwh` (CAD). `generation_mix` rows written from `_current_slot_mix`
via normal `finalise_block` path.

At DCC settlement (PASS 3b re-run): `carbon_g` recomputed as
`COALESCE(imp_kwh_api, imp_kwh) × carbon_intensity_g`. Intensity already on
block — no additional API call. Sub-meter carbon corrects from updated
`kwh_grid`. `generation_mix` rows unchanged (fuel mix is a grid property, not
affected by kWh correction).

**`"api"`:**
At boundary time: ingester fetches Carbon Intensity API (same
`_fetch_carbon_intensity` function), stores `carbon_intensity_g` on block.
Stores `generationmix` via `upsert_generation_mix` directly — no
`_current_slot_mix` needed. `carbon_g` is NULL (no `imp_kwh` yet).

At DCC settlement (PASS 3b first run): `carbon_g = imp_kwh_api × carbon_intensity_g`.
`generation_mix` rows already written. Sub-meter carbon computed from `kwh_grid`.

**`"api+mini"`:**
At boundary time: ingester fetches Carbon Intensity API, stores
`carbon_intensity_g`. Stores `generationmix` via `upsert_generation_mix`.
`carbon_g = imp_kwh × carbon_intensity_g` computed provisionally (Mini figure).
`generation_mix` rows written immediately.

At DCC settlement (PASS 3b re-run): `carbon_g = imp_kwh_api × carbon_intensity_g`.
`generation_mix` rows unchanged.

#### Carbon Across All Modes — Summary

| | `"cad"` | `"cad+api"` | `"api"` | `"api+mini"` |
|--|---------|-------------|---------|--------------|
| `carbon_intensity_g` stored | At write (new) | At write (new) | At boundary ✅ | At boundary ✅ |
| `carbon_g` at write | ✅ | ✅ CAD | ❌ NULL | ✅ Mini (provisional) |
| `carbon_g` after DCC | N/A | Recomputed | First computed | Recomputed |
| Sub-meter carbon at write | ✅ | ✅ | ❌ NULL | ✅ provisional |
| Sub-meter carbon after DCC | N/A | Corrected | First computed | Corrected |
| `generation_mix` rows | `finalise_block` | `finalise_block` | Ingester at boundary | Ingester at boundary |
| Extra Carbon API calls | 0 | 0 | 0* | 0* |
| Lifetime carbon heatmap | ✅ | ✅ | ✅ after settlement | ⚠️ provisional then corrected |
| 48h generation mix chart | ✅ | ✅ | ✅ (engine CI tick) | ✅ (engine CI tick) |
| Per-block fuel breakdown | ✅ | ✅ | ✅ | ✅ |
| Export `carbon_g` at write | ✅ | ✅ | ❌ NULL | ❌ NULL |
| Export `carbon_g` after DCC | N/A | Recomputed | First computed | First computed |

*Carbon Intensity API call happens at boundary time regardless — same call
the engine already makes every 15 minutes. Ingester shares this cadence;
no additional quota concern.

#### `mix_history` and the 48h Chart

`mix_history` is written by `_tick_carbon_intensity()` in the engine's async
loop — independent of block finalisation and independent of whether a CAD is
configured. The engine still runs its CI tick loop in all modes (it's how
carbon intensity data is kept fresh). The 48h generation mix chart on Overview
therefore works correctly in all four modes. ✅

#### PASS 3b Changes for 3.0.0

Current `finalise_block` PASS 3b: looks up intensity from table, computes
`carbon_g`, but does not store `carbon_intensity_g` on the block.

Changes needed:
1. Store `carbon_intensity_g` on block at the same time as computing `carbon_g`
2. PASS 3b re-run path (triggered by `_drain_pass2_queue`): use stored
   `carbon_intensity_g` rather than re-querying the intensity table
3. Ingester: call `get_nearest_carbon_intensity` at boundary time, store
   `carbon_intensity_g`, call `upsert_generation_mix` with the mix data

### Main Meter Config Schema

```json
{
  "meters": [
    {
      "meter_id": "main",
      "name": "Main Import",
      "type": "main",
      "mode": "cad",
      "sensor": "sensor.glow_electricity_meter_import",
      "rate_sensor": "sensor.octopus_energy_electricity_current_rate",
      "standing_charge_sensor": "sensor.octopus_energy_electricity_standing_charge"
    }
  ]
}
```

`mode` values: `"cad"` | `"cad+api"` | `"api"` | `"api+mini"`

Migration: existing meters without `mode` → `mode = "cad"`. Safe no-op.
`kraken_api` credentials required for all modes except `"cad"`.

### First-Run / Settings Setup Flow

```
How does EMT read your main electricity meter?

○ Smart meter device (CAD, Hildebrand Glow, CT clamp, etc.)
  Real-time readings in Home Assistant.

○ Smart meter device + Supplier API
  Device for live readings. Supplier API for billing reconciliation.

○ Supplier API only
  No real-time device. Settled data from Octopus (~48h delay).

○ Supplier API + Octopus Mini
  Octopus Mini for live readings. Settled data confirms billing.
```

### BottlecapDave Detection and Pre-population

**Detection sequence:**

1. **Page load:** scan HA entity registry for `sensor.octopus_energy_electricity_*`
   via WebSocket `config/entity_registry/list`. Sets BottlecapDave present/absent
   boolean immediately.

2. **Post-discover:** check specific rate sensor for this MPAN/serial:
   `sensor.octopus_energy_electricity_<serial>_<mpan>_current_rate`
   Confirms configured for *this* meter, not just installed.

3. **Pre-population from config entry:** attempt to read MPAN, serial, account
   number from HA device/config registry for `octopus_energy` integration.
   API key is never accessible (encrypted store). Verify during Chunk 3 what
   is actually readable. Best case: user only needs to enter API key.

**BottlecapDave as optional dependency:**
Detection is opportunistic. All functionality degrades gracefully to direct
Kraken API calls when absent. Naming convention change → silent fallback
to manual entry, no functional breakage.

**Template sensor users:**
Users who alias BottlecapDave sensors for privacy won't be auto-detected.
Settings UI must support manual entity ID specification — `rate_sensor` and
`standing_charge_sensor` on the meter config already support this.

### Privacy Mode

`privacy_mode: true` (default on) masks sensitive identifiers at the
presentation layer:
- MPAN: `••••••••••000` (last 3 digits)
- Serial: `••••••1111` (last 4 digits)
- Account number: `A-••••1111`
- API key: always fully masked

Applied in Jinja templates via `mask(value, keep=3)` helper. Log output
already uses `mpan[-4:]` — standardise across all ingester/server logging.
Small eye icon to temporarily reveal. Default on — privacy is opt-out.

### API Key Security

`meters_config.json` is plain text in the share directory. Mitigations:
- File permissions tightened to `600` at startup
- Key never logged at any level
- Key never returned by `GET /api/kraken/config`
- Key never round-trips to browser DOM after save

### What the Kraken API Provides

#### REST API

Auth: HTTP Basic, API key as username, empty password.

Consumption response — pure kWh, three fields per record:
```json
{"consumption": 0.063, "interval_start": "...", "interval_end": "..."}
```

No rate, no cost, no estimated flag. EMT owns cost computation entirely.

| Endpoint | Returns |
|----------|---------|
| `/v1/accounts/<account>/` | MPAN, serial, tariff history |
| `/v1/.../consumption/` | kWh per half-hour, 24-48h delayed |
| `/v1/.../standard-unit-rates/` | Rate periods inc VAT |
| `/v1/.../standing-charges/` | Daily standing charge periods |

Rate limit: shared, no increases available for domestic accounts.
Remaining queryable via GraphQL `rateLimitInfo`.

#### GraphQL API

Endpoint: `https://api.octopus.energy/v1/graphql/` (Octopus).
Auth: JWT via `obtainKrakenToken`. Lifetime read from `exp` claim:

```python
import base64, json
def _jwt_expires_at(token: str) -> float:
    payload = token.split(".")[1]
    payload += "=" * (4 - len(payload) % 4)
    return float(json.loads(base64.urlsafe_b64decode(payload))["exp"])
```

**`rateLimitInfo`:**
```graphql
query { rateLimitInfo { pointsUsed pointsLimit } }
```
Call at ingester startup and after each poll cycle. Back off if < 20 remaining.
Field names need verification against real account.

**`smartMeterTelemetry` fields:**

| Field | Type | Notes |
|-------|------|-------|
| `readAt` | ISO timestamp | Upload time |
| `demand` | Integer (watts) | Signed — negative = exporting |
| `consumptionDelta` | Decimal (kWh) | Since previous reading |
| `consumption` | Integer (Wh) | Cumulative register — assumed import only |
| `costDelta` | Decimal (pence) | Display only |

### The Octopus Mini

#### Import Block Derivation

`consumption` assumed to be Active Import Register. Verify during Chunk 3:
static during export while `demand` is negative? Critical before Mini design
is finalised.

1. At boundary: query `smartMeterTelemetry` at `TEN_SECONDS`, ±2 min window
2. Pick reading closest to boundary timestamp
3. Delta vs previous boundary = `imp_kwh`
4. Write block: `source='kraken_mini'`, `is_provisional=1`
5. Run PASS 2 + PASS 3b provisionally

One GraphQL call per boundary = 2 calls/hour.
Engine fires `on_block_boundary(boundary_time)` callback to ingester.

#### Export with Mini

`consumption` appears import-only. 3.0.0: REST settlement only (48h).
Demand-based export integration deferred to 3.1.0 (Chunk 12).

#### Rate Recording

`imp_rate` recorded at boundary time from HA rate sensor in all modes.
Always known and correct at boundary time. No historical rate lookup at
settlement time.

Exception: no HA rate sensor configured at all (no BottlecapDave). Fetch
rate from Kraken REST rate endpoint at settlement time for the `block_start`
slot. Narrow edge case.

### The BottlecapDave Boundary

BottlecapDave provides at zero quota cost:

| Data | Sensor |
|------|--------|
| Current rate | `*_current_rate` |
| Standing charge | `*_current_standing_charge` |
| Full day schedule (inc IO dispatch) | `*_current_day_rates` |
| Live Mini power | `*_current_consumption` |

EMT does not call rate endpoints when `rate_sensor` is configured.

### API Call Budget

**`"cad"` or `"cad+api"` with BottlecapDave:**

| Operation | Calls/hour |
|-----------|------------|
| REST consumption (import + export) | ~0.3 |
| `rateLimitInfo` | ~0.05 |
| **Total** | **~0.4** |

**`"api+mini"`, page closed:**

| Operation | Calls/hour |
|-----------|------------|
| Mini boundary reads | 2 |
| Carbon Intensity API (CI tick, no limit) | N/A |
| REST consumption (export + cross-check) | ~0.2 |
| `rateLimitInfo` | ~2 |
| JWT | ~0.02 |
| **Total** | **~4** |

**`"api+mini"`, Overview page open (additional):**

| Operation | Calls/hour |
|-----------|------------|
| Live power poll | ~60 |

Live power is frontend-driven — stops on `visibilitychange`.

### The Overview Page

Renamed from "Live Power". Nav always "Overview" (⚡ icon retained).

| Mode | Page `<h1>` |
|------|-------------|
| `"cad"` | Live Power |
| `"cad+api"` | Live Power |
| `"api+mini"` | Energy Overview |
| `"api"` | Energy Overview |

#### Card Capability Matrix

| Card | `"cad"` | `"cad+api"` | `"api"` | `"api+mini"` |
|------|---------|-------------|---------|--------------|
| Live power gauge (10s) | ✅ | ✅ | ❌ | ❌ |
| 48h power sparkline | ✅ | ✅ | ❌ | ❌ |
| Mini power card (60s) | ❌ | ❌ | ❌ | ✅ display only |
| Today's cost | ✅ live | ✅ live | ⚠️ + "As of" | ⚠️ provisional + "As of" |
| Period/yearly cost | ✅ | ✅ | ⚠️ + "As of" | ⚠️ provisional + "As of" |
| Carbon intensity widget | ✅ | ✅ | ✅ | ✅ |
| 48h generation mix chart | ✅ | ✅ | ✅ | ✅ |
| DCC variance indicator | ❌ | ✅ | ❌ | ✅ |

`"api+mini"` cost cards are provisional — same visual treatment as `"api"`
mode. The Mini power card (watts via `demand`) is a display-only live reading,
not a billing figure. Export costs are NULL until DCC settlement in both
`"api"` and `"api+mini"` modes.

48h generation mix chart works in all modes — written by engine CI tick,
independent of block finalisation and CAD presence. ✅

#### The Mini Power Card

- Signed watts, refreshed every 60s while page open
- Label: "Via Octopus Mini · 60s"
- Static number + "as of HH:MM:SS"
- `clearInterval` on `visibilitychange`
- Distinct component, not a degraded CAD gauge

#### "As of" Timestamp

Shown in `"api"` and `"api+mini"` modes when `settled_to` > 2h behind now.
Both modes have provisional blocks — neither provides live billing figures.
Settlement never predicted — reports what is currently confirmed. New
`settled_to` field on `api/billing`:

```sql
SELECT MAX(block_start) FROM blocks
WHERE is_provisional = 0 AND imp_cost IS NOT NULL
AND meter_id = <main_meter_id>
```

#### DCC Variance Indicator

`"cad+api"` and `"api+mini"` modes. Small muted line on billing summary.
Links to reconciliation view in settings.

### Provisional Block Display Policy

Provisional blocks exist in `"api"` and `"api+mini"` modes.
`"cad"` and `"cad+api"` never produce provisional blocks.

#### Two-Tier Display

**Tier 1 — Accuracy-critical (finalised only):**
Billing totals, Usage Stats costs, Overview cost cards, Insights.
Both `"api"` and `"api+mini"` modes exclude provisional blocks from Tier 1.

**Tier 2 — Recency-critical (all blocks, provisional distinguished):**
Usage Stats kWh bars, current block on Overview, 48h chart.

Difference between modes in Tier 2:
- `"api"`: bars show device kWh only (sub-meters), NULL main meter attribution
- `"api+mini"`: bars show provisional main meter kWh from Mini + sub-meters,
  provisional attribution computed. Visually distinguished as provisional.

Both modes: export kWh NULL until DCC settlement — no export bar in Tier 2.

#### Visual Treatment

Provisional: reduced opacity (0.4–0.5), CSS hatching, tooltip "⏳ Provisional
— awaiting supplier settlement" (kWh only). Banner when range includes
provisional blocks. Suppressed in `"cad"` and `"cad+api"` modes.

#### Query Layer

All cost-aggregating methods gain `finalised_only: bool = False`.
Passed `True` in both `"api"` and `"api+mini"` modes — both have provisional
blocks that must be excluded from billing aggregations until DCC settlement.
`"cad"` and `"cad+api"`: blocks always finalised at write time,
`finalised_only=False`.

#### Provisional Timeout

`"api"` and `"api+mini"` modes. Daily sweep at `kraken_gap_timeout_days`
(default 30). Applies to blocks where DCC never arrived:

```sql
UPDATE blocks SET is_provisional = 0
WHERE is_provisional = 1
  AND block_start < (now - gap_timeout_days)
  AND source IN ('kraken_api', 'kraken_mini')
```

In `"api+mini"`: timed-out blocks keep `imp_kwh` from Mini as billing value
via COALESCE. Better than NULL — the Mini figure, however imperfect, is real
data. Log WARNING for each timed-out block.

In `"api"`: timed-out blocks may have `imp_kwh = NULL`. Block finalises with
NULL — excluded from billing permanently via COALESCE. See open question.

### Schema Changes

All via `ALTER TABLE` in `_migrate()`. New columns:

| Column | Table | Type | Default | Purpose |
|--------|-------|------|---------|---------|
| `source` | `blocks` | TEXT | NULL | `'ha_sensor'`, `'kraken_api'`, `'kraken_mini'` |
| `is_provisional` | `blocks` | INTEGER | 0 | 1 = not yet DCC-settled |
| `needs_pass2_rerun` | `blocks` | INTEGER | 0 | 1 = DCC arrived, PASS 2+3b pending |
| `imp_kwh_api` | `blocks` | REAL | NULL | DCC-settled kWh from Kraken REST |
| `needs_review` | `blocks` | INTEGER | 0 | 1 = drift threshold exceeded |
| `carbon_intensity_g` | `blocks` | REAL | NULL | gCO2/kWh at block_start, stored at write time |

`imp_provisional` (existing) = sub-meter boundary read missing. `is_provisional`
(new) = main meter not DCC-settled. Different semantics, different columns.

Unique index:
```sql
CREATE UNIQUE INDEX IF NOT EXISTS idx_blocks_start_meter
    ON blocks(block_start, meter_id);
```
Detect duplicates first — deduplicate keeping highest `block_id`.

Billing kWh in all queries — determined by `billing_source` setting:
```sql
-- billing_source = "api" (default):
COALESCE(b.imp_kwh_api, b.imp_kwh) AS effective_imp_kwh

-- billing_source = "cad":
b.imp_kwh AS effective_imp_kwh
```

Server.py reads `billing_source` from config and passes the appropriate
expression to all aggregation queries. Switching is instant — no data changes.

### Pre-release Considerations

**Addon restart / last poll persistence**
`kraken_state` table stores last successful poll timestamp. On restart:
resume from last poll, no re-backfill. Prevents quota waste.

**Timezone handling**
Provisional timeout and `settled_to` use `datetime.now(timezone.utc)`.
Confirm consistent with existing engine BST/GMT patterns. 2.10.x test suite
covers transitions — ingester must follow same patterns.

**Concurrent write safety**
Ingester and engine both write to `blocks.db`. Test concurrent boundary write
+ Kraken upsert on same block under WAL mode. Ingester must use same
connection/lock pattern as engine.

**Upgrade safety — unique index**
Run duplicate detection before adding index. If duplicates exist, deduplicate
(keep MAX `block_id`), log WARNING per deduplicated row.

**Backpressure during initial backfill**
Batch upserts with `asyncio.sleep(0)` between batches of 100. Keeps event
loop responsive on Raspberry Pi 3.

**Agile rate cache pre-warming**
For pure API users on Agile with no HA rate sensor: pre-warm rate cache daily
(Octopus publishes ~4pm). Reduces rate endpoint calls from 48/day to 1-2/day.

**Settings unsaved changes**
Auto-save after successful auto-discover, or show unsaved indicator.
Navigating away after discover but before save loses all discovered values.

**NULL `imp_kwh` at provisional timeout**
`"api"` mode block times out with `imp_kwh = NULL`. Block finalises with
NULL billing kWh — excluded from aggregations permanently via COALESCE.
Safest option — no false data. Document clearly in UI.
`"api+mini"` block times out with `imp_kwh` from Mini — Mini figure becomes
the permanent billing value via COALESCE. Better than NULL; Mini data is real
even if unconfirmed by DCC. Log WARNING for each timed-out block in both modes.

**`billing_source` query switching**
All billing aggregation queries must read `billing_source` from config and
apply the correct expression: `COALESCE(imp_kwh_api, imp_kwh)` for `"api"`,
`imp_kwh` for `"cad"`. The switch must be consistent across all surfaces —
billing chart, Usage Stats, Overview cards, Insights — to avoid showing
different figures on different pages for the same period.

### Full Config Schema

```json
{
  "privacy_mode": true,
  "meters": [
    {
      "meter_id": "main",
      "name": "Main Import",
      "type": "main",
      "mode": "cad+api",
      "sensor": "sensor.glow_electricity_meter_import",
      "rate_sensor": "sensor.octopus_energy_electricity_current_rate",
      "standing_charge_sensor": "sensor.octopus_energy_electricity_standing_charge"
    }
  ],
  "kraken_api": {
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
    "backfill_days": 90,
    "kraken_gap_timeout_days": 30,
    "drift_warn_percent": 5.0,
    "drift_block_percent": 2.0,
    "billing_source": "api"
  }
}
```

`enabled` removed — mode on the meter drives whether Kraken is used.
`live_poll_interval_seconds` removed — Mini live poll is frontend-driven.

### Open Questions

1. **Mini `consumption` field** — import-only, export-inclusive, or net?
   Test during Chunk 3: static during export while `demand` negative?

2. **Export register via GraphQL** — separate export `consumption` accessible?
   Determines 3.1.0 export enhancement.

3. **Export MPAN** — always separate for export tariff users? Verify during
   auto-discover testing.

4. **`rateLimitInfo` field names** — verify against real account.

5. **BottlecapDave config entry pre-population** — what is readable from HA
   device/config registry without touching encrypted store?

6. **NULL `imp_kwh` at provisional timeout** — `"api"` mode: block finalises
   with NULL, excluded from billing permanently. `"api+mini"` mode: Mini figure
   stands as billing value. Confirm this asymmetry is the correct policy.

7. **Mode transition behaviour — resolved:**
   - `"cad+api"`: engine writes `imp_kwh`, ingester writes `imp_kwh_api`.
     Separate columns, no collision possible. No overwrite guard needed.
   - Backfill always runs on ingester start — fills `imp_kwh_api` for all
     blocks within `backfill_days`. No opt-in prompt.
   - Mode change `"cad"` → `"cad+api"`: ingester starts and backfills.
     `billing_source` toggle determines whether DCC figures affect billing.
   - Mode change `"api"` → `"cad+api"`: engine starts writing `imp_kwh`
     from new `config_period`. Ingester continues writing `imp_kwh_api`
     as before. No special case needed.

8. **IO dispatch correction, no HA sensor** — future version. Known limitation
   for pure REST Intelligent Octopus users.

### New Files

- `kraken_api_client.py` — REST + GraphQL, JWT `exp` decode, `rateLimitInfo`,
  `auto_discover`, `test_connection`, BottlecapDave entity detection
- `kraken_ingester.py` — REST polling, Mini boundary callback, carbon intensity
  + generation mix fetch at boundary time, drift/reconcile logic, `needs_review`
  flagging, provisional timeout, rate cache

### Files to Modify

- `block_store.py` — schema migration, `imp_kwh_api`, `carbon_intensity_g`,
  `needs_review`, `billing_source`-aware billing queries, `finalised_only`,
  `settled_to`, `drain_pass2_queue`, `get_drift_alerts`, `kraken_state` table
- `engine.py` — store `carbon_intensity_g` in PASS 3b, `_drain_pass2_queue()`
  using stored intensity, `on_block_boundary()` callback
- `main.py` — ingester tasks, mode detection
- `server.py` — Kraken routes, `settled_to`, `billing_source` query switching,
  DCC variance on `api/billing`, drift alert endpoints
- `web/templates/live_power.html` → `overview.html` — adaptive layout, Mini
  card, "As of", DCC variance, provisional banner
- `web/templates/settings.html` — mode selector, Supplier API tab,
  billing source toggle, drift alert list, drift history placeholder,
  privacy mode toggle
- `web/templates/charts.html` — provisional visual treatment, banner
- `energy_charts.py` — `generate_net_heatmap` uses `carbon_intensity_g` from
  block when available, falls back to intensity table for 2.x blocks

### Development Plan — Testable Chunks

---

**Chunk 1 — Schema and BlockStore foundations**
*Files: `block_store.py`*

- Migration: `source`, `is_provisional`, `needs_pass2_rerun`, `imp_kwh_api`,
  `needs_review`, `carbon_intensity_g`
- `kraken_state` table for last-poll timestamp
- Unique index on `(block_start, meter_id)` with duplicate detection
- `billing_source`-aware billing queries (`COALESCE` vs direct `imp_kwh`)
- `finalised_only` on aggregation methods, `settled_to` query
- New methods: `get_block_by_start`, `get_block_by_id`, `drain_pass2_queue`,
  `upsert_kraken_block`, `get_timed_out_provisionals`,
  `finalise_timed_out_provisionals`, `get_drift_alerts`, `dismiss_drift_alerts`

*Test: migration on prod DB — no duplicates, no data loss. COALESCE transparent
when `imp_kwh_api` NULL. All existing queries return identical results.*

---

**Chunk 2 — Engine PASS 3b and PASS 2 re-run queue**
*Files: `engine.py`*

- Store `carbon_intensity_g` on block in PASS 3b at `finalise_block` time
- `_drain_pass2_queue()` — re-runs PASS 2 + PASS 3b using stored
  `carbon_intensity_g`, not a fresh intensity table lookup
- `on_block_boundary()` callback stub (filled in Chunk 7)
- Call `_drain_pass2_queue()` from `engine_loop_task`

*Test: deploy to prod — no regressions. `carbon_intensity_g` appears on new
blocks. Queue empty → no-op. Unit test: block with `needs_pass2_rerun=1`
→ PASS 2 + PASS 3b run, flag clears, `carbon_g` recomputed from stored
intensity.*

---

**Chunk 3 — Kraken API client**
*Files: `kraken_api_client.py` (new)*

- REST: `get_account`, `get_consumption`, `get_unit_rates`,
  `get_standing_charges`
- GraphQL: `obtainKrakenToken`, `smartMeterTelemetry`, `rateLimitInfo`
- JWT `exp` claim decode
- `auto_discover`, `test_connection`
- BottlecapDave entity detection and config entry inspection

*Test: unit tests with mocked HTTP. Manual against real account:*
- *`auto_discover` returns correct MPAN/serial/tariff/device_id*
- ***Critical:** `consumption` static during export while `demand` negative?*
- *`rateLimitInfo` field names correct*
- *Export MPAN separate and present*
- *JWT `exp` claim present and accurate*
- *BottlecapDave config entry — what fields are accessible?*

---

**Chunk 4 — REST ingester**
*Files: `kraken_ingester.py` (new), `main.py`*

- `run_ingester()` — REST consumption polling
- At boundary time: fetch `carbon_intensity_g` + `generationmix` from
  Carbon Intensity API, store on block and in `generation_mix` table
- `upsert_kraken_block()` — always stores `imp_kwh_api`, flags `needs_review`
  when |delta| > `drift_block_percent`, sets `needs_pass2_rerun` when
  `billing_source = "api"`, computes `imp_cost` from stored `imp_rate`
- `_finalise_timed_out_provisionals()` — `"api"` mode only
- `_check_rate_limit()` via `rateLimitInfo`
- Last-poll timestamp persistence via `kraken_state`
- `main.py`: ingester task when mode ≠ `"cad"`

*No prerequisite open questions blocking Chunk 4 — Q7 resolved.*

*Test: second instance, separate DB. 90-day backfill. Verify:*
- *`imp_kwh_api` always stored for positive records, NULL for missing*
- *`imp_cost` computed from stored `imp_rate`*
- *`carbon_intensity_g` on blocks, `generation_mix` rows written*
- *`carbon_g` computed at settlement = `imp_kwh_api × carbon_intensity_g`*
- *`needs_review` flagged only when |delta| > threshold, zero records*
- *`needs_pass2_rerun` set when `billing_source = "api"`*
- *`billing_source = "cad"`: `imp_kwh_api` stored, PASS 2+3b NOT triggered*
- *Last-poll persisted across restart — no re-backfill*

---

**Chunk 5 — Settings UI**
*Files: `web/templates/settings.html`, `web/server.py`*

- Mode selector (first-run and edit)
- Supplier API tab: credentials, auto-discover, BottlecapDave detection,
  quota impact, pre-populated fields
- `billing_source` toggle with clear explanation of each option
- Drift alert list with dismiss action (informational only, no commit/keep)
- Privacy mode toggle
- Routes: `GET/POST /api/kraken/config`, `POST /api/kraken/test`,
  `POST /api/kraken/discover`, `POST /api/kraken/sync`,
  `GET /api/kraken/drift_alerts`, `POST /api/kraken/dismiss_alerts`

*Test: full setup flow for each mode. `billing_source` toggle switches billing
figures instantly without modifying data. Drift alerts show/dismiss correctly.
BottlecapDave detection with/without. Privacy mode masks throughout UI.*

---

**Chunk 6 — Provisional block UI and DCC variance**
*Files: `web/templates/charts.html`, `web/server.py`*

- `finalised_only=True` in `"api"` mode only
- Provisional opacity + hatching on Usage Stats bars
- Provisional banner
- `settled_to` on `api/billing`
- DCC variance indicator on billing summary (`"cad+api"`, `"api+mini"`)

*Test: `"api"` — billing excludes provisional, bars show hatching, banner
appears. `"cad"` and `"cad+api"` completely unaffected. DCC variance
indicator appears in `"cad+api"` mode.*

---

**Chunk 7 — Mini boundary reads**
*Files: `kraken_ingester.py`, `engine.py`*

- `on_block_boundary(boundary_time)` callback implemented in engine
- Mini boundary read: `TEN_SECONDS` ±2 min window
- Register delta → `imp_kwh`, `source='kraken_mini'`, `is_provisional=1`
- PASS 2 + PASS 3b provisional at write time (carbon intensity fetched)
- REST settlement → `imp_kwh_api`, drift/reconcile, PASS 2+3b re-run

*Prerequisite: Chunk 3 `consumption` field verified import-only.*

*Test: Mini configured, no CAD. Blocks written at boundaries:
`is_provisional=1`, `imp_kwh` from Mini, `carbon_intensity_g` stored,
`generation_mix` rows present. REST arrives → `imp_kwh_api` set, drift
logged, `carbon_g` recomputed, PASS 2 re-runs.*

---

**Chunk 8 — Overview page redesign**
*Files: `web/templates/overview.html` (renamed), `web/server.py`*

- Adaptive h1 and card layout by mode
- Mini power card with `visibilitychange` management
- "As of" timestamp (`"api"` mode only)
- DCC variance indicator
- 48h generation mix chart confirmed working in all modes
- `GET /api/kraken/live` route

*Test: each mode renders correctly. `"cad"` pixel-identical to 2.x.
Mini card polls at 60s, stops when hidden. `"api"` shows "As of".
Generation mix chart present in all modes.*

---

**Chunk 9 — Pure API rate fetching**
*Files: `kraken_ingester.py`*

- REST rate/standing charge, only when no `rate_sensor` configured
- Rate schedule cache, Agile pre-warming daily at ~4pm
- Rate lookup at settlement time for `block_start` slot

*Test: no BottlecapDave, no `rate_sensor`. `imp_cost` populated from Kraken
rate endpoint. Rate endpoint never called when `rate_sensor` configured.
Agile pre-warm fires daily.*

---

**Chunk 10 — End-to-end validation**
*Not a code chunk — test protocol*

1. **`"cad+api"` with BottlecapDave:** rate endpoint never called. 90-day
   backfill — `imp_kwh_api` written for all historical blocks. Both columns
   populated. `billing_source = "api"`: billing uses DCC figures, CAD figures
   visible in drift view. `billing_source = "cad"`: billing uses CAD figures,
   DCC stored but not affecting numbers. Toggle switches instantly.
   Drift alerts appear for anomalous blocks. Dismiss clears `needs_review`.
   Carbon heatmap correct in both billing source modes.

2. **PASS 2+3b re-run cycle:** `provisional_hours_override=2`. `billing_source
   = "api"`. Observe full cycle — `needs_pass2_rerun` set, PASS 2+3b runs,
   `kwh_grid`, `kwh_remainder`, `imp_cost`, `carbon_g` all correct post-re-run.
   Sub-meter carbon corrected. Switch `billing_source = "cad"`: verify PASS
   2+3b does not re-run on next settlement.

3. **`"api"` mode:** provisional bars hatched, billing excludes provisional,
   "As of" on cards. `carbon_g` NULL until DCC arrives. `generation_mix`
   rows present from boundary time. After settlement: `carbon_g` populated
   from stored `carbon_intensity_g`.

4. **`"api+mini"`:** Mini boundary reads, `is_provisional=1` at write.
   Provisional attribution in Tier 2. Cost cards show "As of" — same as
   `"api"` mode. Export kWh NULL until DCC. Mini card on Overview (display
   only). REST settlement corrects `imp_kwh_api`, PASS 2+3b re-runs, blocks
   become `is_provisional=0`. 48h mix chart works throughout.

5. **DCC zero block:** `needs_review=1`, not auto-committed.

6. **DCC gap (missing record):** block untouched, live source stands,
   `carbon_intensity_g` preserved, `generation_mix` rows intact.

7. **Restart:** ingester resumes from last poll, no re-backfill.

8. **Quota:** `rateLimitInfo` visible in settings, matches actual usage.

9. **Privacy mode:** MPAN/serial masked throughout UI and logs.

10. **Carbon heatmap:** slot-level intensity (`carbon_g / net_kwh`) correct
    in all modes. Per-block fuel breakdown (generation mix tooltip) present
    in all modes.

---

**Chunk 11 — IO dispatch rate correction (future)**

GraphQL dispatch overlay for pure REST Intelligent Octopus users.

---

**Chunk 12 — Export via demand integration (future)**

Sum negative `demand` values for near-real-time export kWh in Mini mode.
