# Historical Data Import — Design

> _Status: Shipped — design note kept for rationale; the code is the source of truth._

Import energy history from *before* EMT was recording — and, on top of it,
reconstruct device attribution for periods before a device was configured —
from two sources: the **Octopus API** and a **defined CSV**. House consumption
and cost are exact (half-hourly); device attribution is reconstructed on top via
the existing recorder method (BL-12). All reconstructed data is clearly flagged,
best-endeavours, and correctable with EMT's existing block tools.

Status: **design only**. The device-attribution half depends on the LTS spike
(see Open Questions). No PDF/bill parsing — that route is dropped.

---

## One line

The API/CSV gives **exact half-hourly house import/export + billed cost** back to
**account-join** (both routes); those blocks *tile the timeline* up to where EMT's
live capture begins. The per-block **rate is the billed cost ÷ kWh** and the
**off/peak split is Octopus's own bucket label** — dispatch-aware, no tariff
reconstruction. Device attribution is then reconstructed on top from HA recorder
statistics, reusing EMT's live PASS-2 apportionment.

> **2026-07 update — the API route now uses the Kraken GraphQL *Measurements* API,
> not REST `/consumption/`.** Measurements returns per-half-hour **kWh + billed
> cost (inc & exc VAT) + a TOU bucket label (`OFF_PEAK` / `STANDARD_RATE`)**, and
> reaches the account's **full history** (not the REST ~2-year wall). This makes
> the API route *exact and dispatch-aware* — strictly better than the old
> REST-consumption + tariff-reconstruction plan, and on par with (or better than)
> the CSV route. Sections below are annotated where superseded.

## Scope (v1)

- **Two routes:** (a) Octopus API, (b) CSV with a fixed schema.
- **House import/export + cost:** exact, half-hourly.
- **Device attribution (BL-12) + retro back-fill** for the pre-config gap, via the
  *same* "add history" flow.
- **Device cost is attributed whenever kWh is** — including solar/battery homes
  (grid share × rate; self-supplied share is ~free), confidence-flagged.
- **Provenance flag** on every reconstructed block; removable via a
  "reconstructed history" filter on the existing **delete-blocks** tool.
- No PDF parsing; no new snapshot/rollback mechanism (delete-and-re-run instead).

---

## Routes

### A. Octopus API — **GraphQL Measurements** (current design)

`get_measurements(mpan, start, end, direction)` (Kraken GraphQL) returns, per
half-hour, everything we need in one call:

- **kWh** (`value`),
- **billed energy cost** inc & exc VAT (`TOU_BUCKET_COST` → `costInclTax` /
  `costExclTax`, in **pence** despite `costCurrency: GBP`),
- **TOU bucket label** (`OFF_PEAK` / `STANDARD_RATE`) — Octopus's own,
  **dispatch-aware** classification (a smart-charged evening slot billed cheap is
  labelled `OFF_PEAK`),
- **standing charge** (`STANDING_CHARGE_COST`, per-interval apportioned).

So the block is written directly: `rate = costInclTax ÷ kWh`, `off_peak = label`,
`cost = costInclTax`, standing summed per day. **No tariff reconstruction, no
cost-clustering** — the values are the billed truth.

- **Reach: full account history** (measured 2026-07: back to join ~2024-07-01),
  **not** the REST ~2-year wall. (The old REST `/consumption/` endpoint returns
  kWh only, no cost, and is capped ~2 years — superseded and no longer used.)
- **`direction`** = `CONSUMPTION` (import) / `GENERATION` (export); export is a
  separate MPAN, same shape.
- **Fetch** is chunked newest→oldest (bounded, paginated) with a gap-halt; floor =
  earliest agreement (`get_account`), tile up to go-live.

> **Superseded:** the earlier plan (lift `_KRAKEN_BACKFILL_CAP_DAYS`, REST
> `get_consumption`, price via `get_unit_rates` / `_kraken_rate_resolver`
> per-agreement `RateSchedule`) was built, then replaced once Measurements was
> found to carry cost + label. It only mattered because REST lacked cost; it also
> mispriced IOG (couldn't see dispatch). Kept in git history, not here.

### B. CSV

> **Role after the GraphQL Measurements finding:** for Octopus accounts the API
> route (§A) is now exact, dispatch-aware, and reaches full history, so CSV is
> **secondary** — for **non-Octopus suppliers**, offline/lost-access cases, or a
> previously-saved export. The derivation below still stands; it's just no longer
> the primary path for Octopus users.

The Octopus website export already carries cost — so we do NOT ask for a rates
column and do NOT reconstruct rates from tariffs. The rate the bill needs is
**derived from the cost**, and that's strictly better than reconstruction for
time-of-use tariffs (see below).

**Octopus native format (per channel — import and export export separately):**
`Consumption (kWh)`, `Estimated Cost Inc. Tax (p)`, `Standing Charge Inc. Tax (p)`,
`Start`, `End` (ISO-8601 **with offset**, e.g. `2024-07-01T01:00:00+01:00`). One
file per channel; the user uploads the import CSV and, optionally, the export CSV.
All money is **inc-VAT** and is **stored inc-VAT** (matches the bill; no ex-VAT
split derived). Standing charge is per-interval apportioned (48 × ≈1.12p ≈ daily),
so **sum per day** into EMT's once-a-day standing-charge handling.

**Per-block cost÷kwh is a COARSE signal, not the rate.** A single block's
`cost ÷ consumption` carries Octopus's per-block rounding (e.g.
`0.7560378p ÷ 0.108 = 7.00035 p/kWh` — is the true rate `7.00` or `7.0035`? one
block can't say). So it is used only to **bucket** blocks, never as the stored
rate.

**Derivation (aggregate, then confirm):**

1. **Segment by tariff period** — agreement `valid_from`/`valid_to` boundaries
   give the periods (we have them from `get_account`).
2. **Coarse-cluster into tiers** within each period using per-block `cost÷kwh`.
   Robust despite rounding because banded-TOU tiers are far apart (IOG ~7p vs
   ~25p+ ≫ rounding noise). Two clusters ⇒ lower = off-peak. Store an **off/peak
   flag per block**.
3. **Aggregate for the true rate:** `tier_rate = Σcost ÷ Σkwh` over all blocks in
   that (period, tier). Per-block rounding is random, so it cancels and the real
   rate emerges — cleanly if it's a round number. Inc-VAT.
4. **Confirm with the user.** Present each `(period, tier) → derived rate` with a
   **confidence** (block count + spread of per-block values); pre-filled so the
   user accepts or snaps/overrides (`7.0009` → `7.00`). High-confidence periods
   can auto-accept; sparse or close-together tiers flag for input.
5. **Standing charge** likewise: sum the per-interval apportioned column per day
   → daily charge (may vary by period); confirmable.

**Bill by flag, not by clock.** Store `off/peak flag` per block + `tier_rate` per
period, and bill `kwh × tier_rate[flag]` — **not** by a static 23:30–05:30
window. This reproduces the CSV cost exactly and is **dispatch-aware for free**: a
smart-charge dispatch that billed a normally-peak half-hour at the cheap rate has
the off-peak cost, so it clustered off-peak and re-bills off-peak. This is the
exact problem that dogged the smart-charging card, solved from the cost column —
and it's why a static time-window rate config would MISprice dispatched blocks.

**Reconcile:** after building the rate config, re-price the imported blocks and
check the total against the CSV cost sum within tolerance; a divergence flags a
period whose rate needs user input (step 4).

**Edge cases:**

- **Zero-consumption blocks** give no derivable rate (0 ÷ 0) — flag rate-unknown,
  no cost, don't force a cluster.
- **Agile / non-banded** tariffs price every half-hour differently, so the
  two-cluster off-peak split doesn't apply — skip the off/peak flag, but the
  per-block rate + cost is still exact for the bill.

**Generic fallback (non-Octopus):** a bring-your-own-numbers CSV
(`interval_start`, `kwh`, `cost` **or** `unit_rate`, `standing_charge`, `channel`)
for sources without an Octopus-style export. Rate-from-cost (or cost-from-rate)
uses the same derivation. Reconstruction from published tariffs is reserved for
the **API route** (which returns consumption only, no cost).

- **Explicit timezone per row is mandatory** — naive local timestamps are rejected
  (see GMT/BST below).
- Same downstream as the API route once parsed into HH blocks (kwh + cost + rate
  + off/peak flag + per-day standing charge).

---

## The join (trust model)

- **Temporal tiling.** Reconstructed blocks fill `[source start → EMT go-live)`,
  abutting live capture with no overlap or gap at the boundary interval.
- **Delta-overlap sanity check.** Where the imported range overlaps existing EMT
  blocks, compare **per-block consumption deltas** (EMT's recorded value vs the
  source) within a small tolerance. Both are half-hour-aligned, so block times
  should line up; a mismatch beyond tolerance surfaces a warning. No bill-total
  reconciliation is required or attempted.
- **Billing period.** Assume the **current billing day across all of history**. EMT
  does not try to reconstruct historical billing-date changes (we're not reading
  bills). The user can add/adjust historical billing periods manually if wanted.

## Contiguity & gaps

- Import a **single contiguous date range** ending at go-live.
- Walk backward and **halt at the first internal gap** in a channel that otherwise
  has data (missing expected intervals).
- Export simply *starting later* than import is **not** a gap → continue
  import-only, export flagged unavailable before its start.
- A gap in **import** halts the whole import at that point (no spanning holes).

## Provenance & flagging

- Extend the block `source`: `imported_api` / `imported_csv` for the house figure;
  the go-live billing period that straddles live capture → `imported_blended`.
- **Two-tier provenance.** The house figure can be exact (source-anchored) while a
  device split laid over it is `estimated_attribution` — independent flags. A block
  can legitimately be *native/exact house + estimated device split* (see retro
  back-fill).
- **Delete / re-run:** add a "reconstructed history only" filter to the
  delete-blocks tool (`source LIKE 'imported%'` / the flag) so a bad import is
  wiped and re-run without touching native data. This replaces a dedicated
  snapshot/rollback.

---

## Device attribution (BL-12, included in v1)

Reuses `docs/historical_attribution_design.md`. Key points and the additions this
design makes:

- **Control total** = the imported house HH import/export (exact) — the role DCC
  plays in BL-12, now supplied by the import for the whole range.
- **Device energy** = HA recorder **hourly long-term statistics** (short-term
  state is purged ~10 days). Prefer cumulative energy (`total_increasing`) over
  power sensors.
- **Per-device historical sensor override — distinct from the live sensor.** The
  entity EMT integrates *live* (often instantaneous power) is frequently not the
  one with good LTS (a cumulative kWh sensor). Each sub-meter gains an optional
  `history_sensor` (entity_id) + `history_sensor_kind`
  (`energy_total` / `power` / `session`); unset ⇒ discovery runs and reports its
  choice. This override also drives per-device start-date discovery.
- **Hourly → half-hour split: a weighted, floored allocation** `w ∈ [ε, 1−ε]`,
  not a flat 50/50. Pick `w` from the strongest signal available, in order:
  1. true HH house shape (if that hour is within the DCC consumption window),
  2. dispatch / smart-charge slots (EMT knows which HH was smart-charged off-peak
     — e.g. an EV hour straddling the 23:30 IOG boundary goes almost entirely to
     the off-peak block),
  3. TOU-boundary prior (lean toward the cheap block, bounded),
  4. 50/50 fallback.
  `ε` is a small floor so neither half is ever zeroed (keeps the PASS-2 grid clip
  stable; a half-hour is rarely *exactly* zero).
- **Apportionment** = EMT's live PASS-2 (EV claims grid first, others clip to the
  remainder, `remainder = house − Σdevices` clamped ≥ 0, device grid ≤ house
  import). Do not reinvent it — seed it with recorder-derived device kWh.
- **Cost** = device **grid share × HH rate** — attributed **even with
  solar/battery** (the self/solar-supplied share is real usage but ~free). Confidence
  is downgraded for solar/battery (a battery confounds the grid picture), but cost is
  still produced wherever kWh is.

### Retro back-fill (the "device defined late" gap)

Same method, same flow. If EMT was recording house data before a device existed,
the user **re-runs the "add history" process and supplies that device's recorder
sensor**; EMT reconstructs the device's split over `[sensor start → device
configured in EMT)` and re-runs PASS-2 on those blocks. Rules:

- The house figure on those blocks stays **native/exact**; only the device split is
  added, flagged estimated.
- **Never rewrite live-measured periods** — after the device was configured, live
  attribution is authoritative.
- Re-running PASS-2 only *adds* the missing device and re-derives the remainder;
  it never touches the house total or an already-live-measured device.

---

## GMT / BST / DST

- **Normalise every source timestamp to naive UTC** (`normalise_to_naive_utc`,
  already used for the API's offset-stamped intervals; enforced for CSV).
- **TOU rate tiers resolve in local clock time** (IOG off-peak 23:30–05:30 local,
  day/night boundaries) — the naive-UTC block is converted to local time to pick
  its tier, and that mapping shifts an hour across DST. This is the most likely
  place for a subtle error; test the transition days.
- **23-/25-hour transition days** — the API offset disambiguates the repeated
  autumn hour (first pass `+01:00`, second `Z`); tiles correctly if the offset is
  trusted. Explicit tests on both transition dates.
- **Bill/PDF path (as shipped):** a PDF gives **local wall-clock** times with no
  offset, so `bill_parser` must add it. The autumn day lists `01:00`/`01:30` twice
  (BST then GMT); the transcriber stamps the first `+01:00` and the second `+00:00`
  by detecting the point where wall-clock time steps **backwards** on the page —
  otherwise the two collapse onto one UTC slot and a half-hour is lost as a phantom
  gap. Spring-forward needs nothing (the skipped hour is simply absent; UTC stays
  contiguous). Regression-tested on both transition dates.
- **Recorder LTS** is UTC-stored (confirm in the spike), but it only drives the
  approximate **device** layer — so any recorder timezone/DST quirk can perturb
  device attribution but **cannot corrupt the exact house/bill figure**. Keep
  recorder-derived data out of the must-sum figure (BL-12 firewall) and its tz
  quirks stay cosmetic.

## Confidence & UX

- **Best-endeavours notice** at import time (no hard gate): tell the user this is a
  reconstruction, that reconstructed blocks are flagged, and that mistakes are
  fixable with **block corrections**, **block deletions** (reconstructed-history
  filter), and **manually adding billing periods** that may have been assumed.
- **Persistent flag/badge** on reconstructed data throughout the UI; device splits
  shown as estimated.

---

## Spike results (measured)

The read-only recorder probe (`statistics_probe.py` + `/api/historical/probe`) was
run against a live HA (2 device sensors: a myenergi Zappi *session* sensor and a
Solax battery aggregate). Findings, which resolve the LTS spike **favourably**:

- **Retention ≈ full sensor lifetime, not 10 days.** Both sensors returned hourly
  statistics back to their creation (~750–767 days / ~2 years); the data was *not*
  truncated by the query window. The ~10-day purge only ever applied to short-term
  *state* — long-term statistics are kept indefinitely. So device reconstruction
  can reach back the whole life of a sensor.
- **Timestamps are epoch-milliseconds, UTC-aligned, hourly, and DST-safe.** Across
  all four DST transitions in the window the delta between consecutive buckets was
  exactly 1 hour, stamped at `01:00Z` — i.e. buckets are UTC-continuous, *not*
  local-wall-clock. The autumn repeated-hour / spring skipped-hour do **not** appear
  in the data. Mapping to local TOU tiers is therefore a clean per-bucket UTC→local
  conversion (as EMT already does); the earlier "recorder in wall-clock time?"
  worry is settled: it is UTC.
- **Energy sensors classify correctly** (`has_sum: true` → `energy_sum`). A *session*
  sensor (resets each charge) still yielded a correct cumulative `sum` across resets,
  so session sensors are usable via LTS — validating the per-device override.
- **Coverage ~100% with rare small gaps** (2–5 h, a few times across 2 years — HA
  restarts). Handled by the gap-halt/flag rules; those hours have no device split.
- Metadata unit lives under `statistics_unit_of_measurement` /
  `display_unit_of_measurement` (not `unit_of_measurement`); probe reads all three.

Caveat: this is **one** HA setup. The probe is beta specifically to gather this
across setups (power sensors, older cores, shorter retention) before committing.

## Open questions / spikes (remaining)

1. ~~**Export / consumption retention**~~ — **RESOLVED then SUPERSEDED (2026-07)**:
   the REST `/consumption/` endpoint retains only ~**2 years** — but this no longer
   matters, because the API route switched to **GraphQL Measurements**, which
   reaches the account's **full history** (measured back to join ~2024-07-01) with
   cost + label. Export uses the same Measurements call (`direction: GENERATION`).
2. **Hourly→HH split rule** — validate the weighting signals against a period where
   live sub-meter data exists (ground truth already in EMT).
3. **Delta-overlap tolerance** — the value for the join sanity check.
4. **Breadth of LTS across setups** — collect probe reports from beta users to
   confirm the favourable single-setup result generalises (esp. power-only sensors
   and older HA cores).

## Relation to prior designs

- **Incorporates** the PDF-bill import (shipped in 4.0.0): `bill_parser.py` reads
  Octopus PDFs and emits the per-channel CSVs, which flow through the **CSV** route
  — so there are two *sources* (API, CSV) and the bill parser is strictly upstream
  of the CSV path (see `bill_to_csv_import_spec.md`). (An earlier draft dropped PDF
  bills entirely; that was reversed once the guarded `pypdf` import + reconciliation
  gate made in-EMT parsing safe.)
- **Reuses** BL-12 (`historical_attribution_design.md`) for device attribution;
  the retro back-fill generalises it from outage/pre-EMT gaps to the
  "device configured late" gap.
- **Complements** BL-8 (outage backfill): both create blocks from external data
  and flag them; this one is user-initiated and reaches the full source window.
