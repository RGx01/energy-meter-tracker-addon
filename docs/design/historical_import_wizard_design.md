# Historical Import Wizard — Design

> _Status: Shipped — design note kept for rationale; the code is the source of truth._

A guided, resumable flow that walks the user through reconstructing pre-EMT
history — mirroring the first-run **setup wizard** pattern. It orchestrates the
read-only spike primitives (already built) and adds the writes, the CSV
handling, the coverage-stitching, and — new here — **derivation provenance** so a
reconstruction can be revisited and rebuilt as better/longer sensor data becomes
available.

Companion docs: `historical_import_design.md` (data mechanics, rate-from-cost,
device attribution) and `historical_import_build_spec.md` (probe + API-route
implementation, the measured ~2-year API limit). This doc is the **UX /
orchestration / provenance** layer; it references those rather than repeating
them.

Status: **design only**.

---

## What's reused vs new

**Reused as-is (the spike — read-only, tested):**

- `statistics_probe.py` — characterises a candidate device sensor.
- `consumption_probe.py` — API earliest/latest per channel.
- `engine.diagnose_consumption_retention` — "what the API holds" (retention,
  serials, agreements) → drives route choice + the API summary step.
- `engine.probe_recorder_statistics` — validates a chosen device sensor live.
- `engine.probe_consumption_retention`, `kraken_api_client.get_consumption_boundary`,
  `ha_client` statistics methods, the `/api/historical/*` endpoints.
- `historical_probe.html` render helpers (report cards, retention/meter tables) —
  salvaged into wizard steps; the current page stays as an **Advanced** view.

**New to build:**

- The wizard UI + **resumable state machine**.
- CSV parse → aggregate rate derivation → confirm/override (per
  `historical_import_design.md` §B).
- **Coverage-stitching** of multiple device sensors from probe output.
- The block-writing import (API Part 2 + CSV apply, chunked, provenance-flagged).
- **Derivation provenance store + rebuild** (this doc, below).

---

## Flow

**Step 0 — Intro.** Best-endeavours notice: this is a reconstruction, blocks are
flagged, everything is correctable/removable. No commitment yet.

**Step 1 — Scope.** What to import: house history, device attribution, or both;
and how far back. (House and device are independent — a user may reconstruct
house-only, or add device attribution to an already-imported span.)

> **2026-07 update:** the API route uses **GraphQL Measurements** (exact cost +
> dispatch-aware label, **full history** — see design §A), so for **Octopus
> accounts the API is the recommended route** and CSV is secondary (non-Octopus /
> offline). The steps below still hold; the "~2-year, older needs CSV" framing was
> the REST-era assumption and no longer applies to Octopus.

**Step 2 — Route detection.** Run `plan_api_import` (and, for context, the
diagnostic). Show the import window (agreement floor → go-live) and chunk count.
Recommend: **API for Octopus**, CSV for non-Octopus suppliers or an offline export.

**Step 3a — API branch.** Confirm the range (defaults to full history from
`plan_api_import`). Apply = `import_api_history`: chunked newest→oldest GraphQL
Measurements, gap-halt, `imported_api` blocks priced from billed cost + label.

**Step 3b — CSV branch.** Offer a **downloadable template** + the Octopus-export
instructions (per channel). Upload import (and optional export) CSV → parse →
**derive rates by aggregation** (`Σcost ÷ Σkwh` per tariff period × tier) →
present each derived rate with confidence → user **confirms/snaps/overrides** →
reconcile re-priced total vs CSV cost sum.

**Step 4 — Device attribution.** For each device, the user adds one or more
**history sensors**. Each is probed live (`probe_recorder_statistics`): energy vs
power, retention, gaps. The wizard draws a **coverage timeline** and
**auto-proposes which sensor attributes which period** — because the probe already
returns each sensor's earliest/latest/coverage, the "does this sensor cover the
whole history, or do others cover other periods?" question answers itself.
Multiple sensors are **stitched** by measured coverage (later gaps filled from the
next sensor); the user only confirms or reorders. No manual date entry required,
though it's allowed.

**Step 5 — Preview & apply.** Full summary: span, block counts, gaps/halt points,
overlap warnings, rate confirmations, device coverage map, and the **provenance
that will be recorded**. Auto-backup, then apply (chunked; API and/or CSV).

**Step 6 — Done.** Result summary + how to **revisit/rebuild** (Step 4 again with
new sensors) and how to remove (delete-blocks reconstructed filter).

---

## Resumable state

The wizard persists a **draft** (so a long import survives a reload / can be
picked up later), distinct from committed data:

- `route`, `scope`, `range`,
- CSV: uploaded file refs, parsed summary, per-(period,tier) rate confirmations,
- devices: `{device → [ {sensor, kind, period_from, period_to, source:'probe|manual'} ] }`,
- derivation provenance (below), and a `status` (draft / applied).

Stored in a small `historical_import_state` row (JSON) keyed by a draft id; the
final apply writes blocks + provenance and marks the draft applied.

---

## Derivation provenance & rebuild  (the new requirement)

Every reconstructed figure records **how it was derived**, so the user can come
back later — with a better sensor, an added sensor for an uncovered period, or an
extended range — and **rebuild only what changed**, without disturbing anything
live-measured.

**`historical_derivation` table** (side table, not on the block row, so it's
append-friendly and auditable):

| field | meaning |
|---|---|
| `id` | derivation id |
| `scope` | `device_attribution` \| `rate` |
| `channel`/`device` | what it applies to |
| `period_from` / `period_to` | the span this derivation covers (UTC) |
| `sensor_ids` | sensor(s) used (device scope) — the "which sensor" you asked for |
| `sensor_kind` | energy_total / power / session |
| `params` | e.g. hourly→HH split weights, ε floor; or rate tier + confidence |
| `derived_value` | derived rate (rate scope) / method summary |
| `confirmed_value` | user-confirmed rate, if overridden |
| `method_version` | bump when the algorithm changes → stale derivations detectable |
| `derived_at` | timestamp |
| `source` | imported_api / imported_csv / recorder_probe |

Reconstructed **blocks** carry a `derivation_id` (per device split, and per rate)
pointing at the row, plus the existing `imported_*` / `estimated_attribution`
flags. So any block can answer *"where did this come from?"* — e.g. *"EV: sensor
`…_charge_added_session` (session), 2024-07→2025-01, derived 2026-07-20 (v1)."*

**Rebuild flow.** The user reopens Step 4 for a device (or a period), supplies a
new/additional sensor or extends the range. The wizard re-probes, recomputes the
attribution **only for the affected span**, writes new blocks/splits, and inserts
a fresh `historical_derivation` (new `derived_at`, same or bumped
`method_version`). Rules: never rewrite live-measured periods; supersede — don't
delete — old derivations (keep the audit trail); a `method_version` bump surfaces
which imported spans are eligible for a no-new-data re-derive.

**Rate derivations** work the same way: each `(tariff period, tier)` rate is a
derivation row with its `derived_value`, `confirmed_value`, confidence and
block-count. Re-importing a longer CSV, or the user changing a confirmed rate,
supersedes the row and re-prices only its span.

**UI.** A provenance badge/detail on reconstructed data ("estimated · from sensor
X · derived <date>"), and a **Derivations** list where the user can see every
reconstruction, its coverage, its sensors/rates, and a **Rebuild** action.

---

## Backing primitives per step

| Step | Backed by |
|---|---|
| 2 route detection / API summary | `diagnose_consumption_retention` |
| 2 API-vs-CSV coverage | `probe_consumption_retention` / boundaries |
| 3a API preview/apply | build spec Part 2 (ingester, chunked) |
| 3b CSV rates | `historical_import_design.md` §B (aggregate + confirm) |
| 4 sensor validate + coverage | `probe_recorder_statistics` (earliest/gaps/kind) |
| 4 stitching | probe coverage windows → auto period assignment |
| 5 apply | ingester + CSV writer + backup; provenance store |

---

## Data-model additions (summary)

- Block flags: `imported_api` / `imported_csv` / `imported_blended`,
  `estimated_attribution` (already in `historical_import_design.md`); **+ `derivation_id`(s)**.
- New `historical_derivation` table (above).
- New `historical_import_state` draft row (resumable wizard).

## Open decisions

1. Draft persistence store — reuse `kraken_state` JSON blob vs a dedicated table.
2. Provenance granularity — per block vs per (device, contiguous span). Span-level
   is lighter and matches how derivations are created; block→derivation is a
   lookup, not a column explosion.
3. Where the wizard lives — a Data-Management sub-flow; keep the current probe
   page as its Advanced view.
4. `method_version` policy — what constitutes a bump, and whether to auto-offer
   re-derive when it changes.

## Relation to prior docs

- **Consumes** `historical_import_design.md` (routes, CSV rate-from-cost, BL-12
  attribution, GMT/BST) and `historical_import_build_spec.md` (probe + API route,
  ~2-year API limit, chunking).
- **Adds** the guided flow, resumable state, coverage-stitching, and the
  derivation-provenance/rebuild model.
