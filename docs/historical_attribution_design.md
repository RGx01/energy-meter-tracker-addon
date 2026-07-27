# Historical Device Attribution — Design Spike

Reconstruct **device-level usage (and, conditionally, cost)** for periods *before*
EMT was recording — or before a device sensor was configured — by blending three
data sources EMT can already reach, and reusing EMT's live apportionment so the
reconstructed history is consistent with what EMT would have recorded live.

This is the concrete method for **BL-12** (device apportionment for backfilled
blocks from the HA recorder), generalised from outage blocks to any historical
period, bounded by a discovered per-device **start date**.

Status: **design only**. Needs an API/queryability spike before implementation.

---

## The idea in one line

Authoritative half-hourly *house* reads (DCC) give the control total per block;
HA recorder statistics give each *device's* measured energy; EMT's existing
EV-priority apportionment places the devices inside the house total, clipped so
the device grid shares can never exceed the metered import.

Because the device figures are **measured** (recorder), not inferred from tariff
priors, this is reconstruction, not disaggregation — far more defensible than
guessing from time-of-use. The accuracy limits are all about signal quality and
reconciliation, covered below.

## Data sources

1. **Authoritative half-hourly house import/export — the DCC (Kraken) consumption
   API.** `get_consumption(mpan, serial, period_from, period_to)` already used for
   settlement returns settled HH consumption going back months/years — *not* the
   bill, which lacks HH granularity (and may not carry the reads at all). This is
   the per-block **control total**.
   - *Requires:* the supplier API configured, and a smart meter reporting HH data
     to the DCC. No API / non-HH meter → no authoritative house total → this
     method can't run for that period.

2. **Historical rates — the products REST API.** `get_unit_rates(period_from,
   period_to)` (the same path BL-8 backfill prices from) gives the HH rate series
   for API-priced tariffs, Agile included.
   - *Requires:* an **API-priced** channel. A channel whose rate came from a
     *sensor* has no history (same limit as BL-8 / BL-15) — usage can still be
     reconstructed, but not cost.

3. **Device energy — HA recorder long-term statistics (hourly).** Short-term
   state history is purged (~10 days), so historical device data comes from HA's
   **long-term statistics** (`recorder/statistics_during_period` over the WS API
   EMT already holds). These are **hourly**.
   - *Requires:* the device sensor had a `state_class`. Without it there are no
     long-term stats — only the purged window — so that sensor's usable start
     date may only reach back ~10 days.
   - **Prefer cumulative energy sensors** (`total_increasing`, kWh → hourly *sum*,
     true energy) **over power sensors** (W, `measurement` → hourly *mean*, so
     energy ≈ mean×1h, which smears bursts). Flag which was used.

## Reconstruction, per block

1. House HH import/export for the block ← DCC consumption (control total).
2. Each configured device's **hourly** energy ← recorder statistics, **split to
   the two HH blocks** by a defined rule — proportional to the house's HH import
   shape, 50/50 fallback. The hourly total is exact; the block split is an
   approximation and must be labelled as such.
3. **Apportion with EMT's live model — do not reinvent it.** Reuse the PASS-2
   apportionment (`engine.py` ~1726, issue #212): the **EV claims grid import
   first**, then the remaining grid is claimed by the other devices (each clipped
   to what's left); any device draw beyond its grid share is solar/self-supply.
   Seed it with the recorder-derived device kWh instead of live power-sensor
   integration. The clip (Σ device grid shares ≤ house import) is what keeps the
   reconstruction physically honest.
4. Cost per device = its **grid share** × the HH rate. (Only the grid-supplied
   share is billable; self/solar-supplied device energy is real usage but ~free.)

## Start-date discovery

For each device sensor, walk its statistics to the earliest hour that is
(a) present with a `state_class`, (b) continuous (no long gaps), and
(c) physically plausible (≤ the device's max rate). Intersect with the earliest
DCC read available, and **blend forward until EMT's own recording begins** — i.e.
this fills the pre-EMT gap and stops where live data takes over. Report the start
date **per device** (start each device from its own date rather than gating the
whole reconstruction on the weakest sensor), and surface gaps rather than
silently interpolating across them.

## Caveats & limits (state these in the UI, don't paper over them)

- **Hourly → half-hourly** allocation is an assumption; block-level figures are
  approximate even though the hourly totals are measured.
- **Independent measurement systems.** Recorder device energy and DCC meter
  energy are measured independently and won't perfectly reconcile — the exact
  trap `DEVELOPMENT-3` documents. Use EMT's rate-based remainder discipline
  (`remainder = house − Σdevices`, clamped ≥ 0; never `main − sub`). Σdevices
  occasionally exceeding grid import is **solar**, not an error.
- **Solar / battery.** Usage is broadly reconstructable; **cost** is only clean
  for no-solar / no-battery homes unless historical generation is *also* pulled
  from the recorder to split grid-vs-solar. A battery confounds the grid picture
  (its own charge/discharge shifts import) and should downgrade confidence.
- **Coverage.** Needs DCC API + HH smart-meter data for the house total, an
  API-priced tariff for cost, and recorder stats with `state_class` for devices.
  Where any is missing, degrade gracefully (usage-only, or skip) — never fabricate.
- **Entity renames** split recorder history; a renamed device sensor looks like
  two short series.

## Presentation & firewall

Present as a clearly-labelled **"reconstructed from history"** layer — visually
distinct from live sensor data (e.g. dashed / greyed), annotated with the start
date, the sensor type used, and "usage-only" where solar/battery preclude cost.
Because it is measured-but-hourly-and-approximate, keep it **out of the figure
that must sum exactly to the metered bill**; if it ever feeds billing, it carries
the reconstructed flag. An honest estimate beats an authoritative-looking wrong one.

## Relation to BL-8 / BL-12

- This **is** BL-12's method, generalised beyond outage blocks.
- It can fill the device split that **BL-8** deliberately leaves blank on
  outage-backfilled blocks — but only as an explicit *estimated* layer *on top of*
  BL-8's honest "no sub-meter split" flag, never silently replacing it.

## Open questions — the spike

1. ~~What exactly is queryable from HA long-term statistics via the add-on…~~
   **Resolved:** the Recorder History Probe reads `recorder/statistics_during_period`
   over the WS API and reports span, cadence, gaps, energy-vs-power and timestamp
   format per sensor. `state_class`/kind is inferred from the returned rows
   (`sum` present ⇒ energy, `mean` only ⇒ power).
2. ~~Best hourly→HH split rule — flat vs house-shape-weighted…~~
   **Decided:** weight by the imported/live half-hour import shape (below), 50/50
   fallback. Validate against a period with live sub-meter ground truth.
3. Whether to attempt the grid-vs-solar split from recorder generation history,
   or restrict cost reconstruction to no-solar/no-battery homes in v1. *(still open)*

---

## Decided refinements (recorder attribution — 4.0.x)

Consolidates the design decisions taken once the Octopus import already exists and
the tool's job narrowed to **device attribution inside an existing house total**.

### Control total = the blocks already in the store

The house half-hour import/export per block is no longer a live DCC query — it is
**already stored**: `imported_api` blocks (from the historical import) *and* live
EMT-recorded blocks. Recorder attribution reads that as the per-block control total
and never re-fetches it. (No API dependency for this step.)

### Scope: pre-EMT history AND live-but-unconfigured periods

The tool must fill **two** kinds of missing device split, both keyed on *"the house
block exists but this device has no sub-meter block for it"*:

- **Pre-EMT** blocks reconstructed by the historical import (no live data ever
  existed).
- **Live** blocks EMT recorded *before the device sensor was configured* (common:
  the user added the EV/battery to EMT months after go-live). These carry a real
  house total but no device row.

Rule: attribute a device onto a block **only where no sub-meter block for that
device already exists** — fill the pre-config gap and stop where live recording
took over. Never overwrite a real, live-recorded device block.

### Timestamp alignment — do it in UTC

- HA stats arrive as `epoch_ms` (absolute UTC instants) at hour starts; `block_start`
  is naive UTC. Hour `H` therefore maps onto exactly the two blocks `H` and
  `H+30m` — **no offset maths, DST-proof** (DST only matters later for the
  billing-day roll-up, already handled downstream).
- **Energy = delta, not the counter.** A `total_increasing` sensor's hour energy is
  the difference of consecutive hourly `sum` values. A **decrease ⇒ counter reset**
  (device swap, integration reset) → treat as a series break, not negative energy.
- **Stitching** across renames/resets/integration swaps is the job of the probe
  page's multi-sensor-per-device picker: several series concatenated into one
  device timeline, newest wins on overlap.
- **Cadence:** downsample any sub-hourly series to the hour first; the probe flags
  non-hourly cadence.

### The split rule — weight by the real import shape

For each hour, split the device's measured hour energy across its two blocks in the
ratio of the **known** house import:
`block1_share = imp_kwh[b1] / (imp_kwh[b1] + imp_kwh[b2])`, **50/50 fallback** when
both are zero (pure-export / no-import hour). Then apply the existing PASS-2 model:
EV claims grid first, others clip to what's left, and Σ device grid shares ≤ house
import (remainder = house − Σdevices, clamped ≥ 0 — never main − sub).

### Gap / zero policy (never fabricate)

Per device, per block:

- hour **present, sum = 0** → real idle → attribute **0**.
- hour **absent** (recorder outage) → device share **blank + flagged** "device
  history unavailable"; the block keeps its correct house total and an honest
  "no split here" (same shape as BL-8 outage blocks). Surfaced as gap ranges in the
  UI, self-clearing when filled — never interpolated across.
- house block **absent** but recorder present → no control total → **skip**.

Each device is gated on **its own** coverage, not the weakest sensor's.

### Reversibility & back-out — the safety net (answers "if it goes wrong?")

Backups work but are slow to diagnose ("did I pick the wrong sensor?"), so
attribution is **first-class reversible**, mirroring the `imported_api` / purge
pattern that already works:

1. **Distinct source tag.** Every block the tool writes is a **sub-meter** block
   tagged `source = 'recorder_attributed'` (a new source, sibling to `imported_api`).
   It therefore never collides with real live/imported data and is trivially
   identifiable.
2. **Run ledger.** Each run records `{run_id, device/meter_id, span, sensor_ids
   used, created_at, blocks_written}` (store_meta). The UI lists runs so the user
   can see *what was attributed from which sensors, when*.
3. **One-click back-out** = for a chosen run (or device+span): **delete** its
   `recorder_attributed` sub-meter blocks, then `recompute_remainders_for_window()`
   on the parent so the main meter's remainder is re-derived from the surviving
   devices. Single transaction, targeted, seconds — no backup hunting, and live +
   imported house totals are untouched (only the reconstructed device layer is
   removed).
4. Because back-out is per-run/per-device, *"wrong sensor"* is a clean loop:
   back out that run → re-pick the sensor on the probe page → re-run. Re-running a
   device is itself idempotent (it only fills where no `recorder_attributed` block
   yet exists; a full redo = back-out then re-run).

This makes the source tag the answer to the DB-flag question: **yes**, one new
`source` value plus a small run ledger — not a shadow copy of every changed row —
gives exact, fast rollback because the device layer is additive and the remainder
is always re-derivable.

### Presentation

Reconstructed device blocks render as the existing "reconstructed from history"
layer (dashed/greyed), annotated with the sensor(s) used and the start date, and
kept out of any figure that must sum exactly to the metered bill.
