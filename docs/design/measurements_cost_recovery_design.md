# Measurements Cost Recovery — Design & Root-Cause Record

> _Status: Shipped — design note kept for rationale; the code is the source of truth._

How the historical import gets the **billed cost** right for every half-hour,
including the smart-charged (IOG-dispatched) slots — and the investigation that
proved why a naïve bulk fetch got them wrong.

Status: **built** (2026-07). Lands in `kraken_api_client.py`
(`recover_measurement_costs`, `KrakenQueryTooLargeError`), `engine.py`
(`import_api_history` in-pass recovery + `repair_import_pricing` single-slot
straggler pass), and the import UI copy. Tests in `test_kraken_client.py`,
`test_engine.py`, `test_consumption_retention_probe.py`.

---

## One line

Two independent defects made imported IOG (smart-charged) slots price at peak
instead of off-peak: (1) under connection **load** the Measurements API silently
drops a slot's cost (`statistics` empty, kWh intact, 200, no error); and (2) the
OFF_PEAK label is **window-contextual** — Octopus only tags a dispatched slot
OFF_PEAK when the query window reaches back to the **start of that charging run**,
so a single-slot / start-at-the-slot fetch returns the raw STANDARD (peak) tariff.
The fix: re-fetch affected slots **calmly** *and* in a **wide, context-anchored
window** (≥1 day before the slot). No bill/CSV overlay.

> **Correction (2026-07):** an earlier cut of this fix used a *single-slot*
> re-fetch. The `--context` probe showed that is wrong for mid-run dispatch slots
> — it returns STANDARD (peak). Superseded by the context-anchored window below.
> The single-slot straggler pass had actively repriced mid-run slots to peak.

---

## Symptom

A user's 3 Oct – 2 Nov 2025 Octopus bill did not match EMT after a full import
and even after a "calm" 7-day-window repair pass:

| | off-peak kWh | peak kWh | note |
|---|---|---|---|
| Bill | 1030.8 | 57.7 | |
| EMT (post-repair) | 978.3 | 110.2 | **52.5 kWh mis-split** to peak |

Unit rates and the standing charge matched exactly (the bill quotes **ex-VAT**
rates + 5% VAT; EMT stores **inc-VAT** — `6.67p×1.05 = 7.00p`,
`26.78p×1.05 = 28.12p`, `45.36p×1.05 = 47.63p/day`). The entire discrepancy was
the **kWh split**: EMT had ~52.5 kWh at peak that Octopus billed at off-peak.

Those were IOG-dispatched charging slots. EMT priced them at peak only because
the Measurements response for them carried **no `TOU_BUCKET_COST`** — so pricing
fell through to the tariff schedule, which is peak for an out-of-window time.

## Why it looked like a permanent API gap (and wasn't)

The import already flags material no-cost slots and queues them for a calm
re-price pass (`repair_import_pricing`). That pass — 7-day windows on the live
instance — recovered **873** slots but left **196** still cost-less. It was
tempting to conclude those 196 were a genuine Octopus gap and reach for the
downloaded CSV / PDF bill as the only source. **That conclusion was wrong.**

The user's key observation: *if a calm re-fetch recovers a slot, the API always
had the cost — so a miss is a retrieval defect, not an API limitation.* That
reframed the 873 as a bulk-fetch bug and put the burden of proof on the 196.

## The decisive experiment (`octopus_cost_probe.py`)

A standalone, read-only, pure-GraphQL probe (same JWT + endpoint as the client)
was pointed at the 34 still-flagged slots in the bill period.

- **Section 5** — an *isolated* fetch always returns full stats, even a 60-day
  window @ 500/page (2880 nodes, **0 missing**). So it is **not** window size and
  **not** the query shape.
- **Section 6** — each of the 34 slots fetched in its own single-slot query:
  **31/34 recovered** a real cost. The 3 that didn't had one thing in common —
  they ran **last**, after sections 1–5 had already loaded the connection. The
  failure reproduced *inside the probe itself* once enough points were spent.
- **`--deep`** — those 3 slots, run **fresh/isolated**, all returned their real
  `OFF_PEAK` cost, **deterministically 3/3 on repeat**. `src=Amphio`, `dur=1800`
  — ordinary actual reads, identical to their neighbours.

**Conclusion: zero genuine gaps.** Every slot's cost is retrievable. The only
variable is **connection load at fetch time**. The failure signature is precise:
under load a node returns **kWh intact but `statistics: []`** — no error, HTTP
200 — so the cost silently vanishes and pricing falls back to peak.

## Corroboration & caveat (web research)

Octopus's GraphQL docs confirm the *mechanism's direction*: an hourly **points
allowance** (50k/account-user) based on query complexity that applies *across*
requests, a per-request **complexity** cap (200), a **node** cap (10,000), and
**dynamic** rate limits that get *progressively stricter* and don't auto-reset.
That matches "the harder you push, the worse later calls get", why it fails at
the same point each run, and why an isolated probe never fails.

**Caveat, recorded honestly:** the documented overload response is an explicit
error (`KT-CT-1199` / `KT-CT-1188` / `KT-CT-1189`), **not** a silent empty
`statistics` with a 200. The exact silent-strip signature is **undocumented**
(the Measurements guide is login-gated). We therefore do **not** claim the docs
prove the mechanism — the fix rests on the *reproduction* (deterministic recovery
when calm), which holds regardless of the internal cause.

Sources: https://docs.octopus.energy/graphql/guides/basics/ (errors, rate limits,
complexity & points).

## Design

Three layers, cheapest first, each proven by the experiment above.

### 0. The window-context discovery (why single-slot was wrong)

The `--context` probe fetched each still-mispriced slot repeatedly, varying only
how far **before** the slot the query window starts (end held fixed). Result for
the 2 Nov morning charging run:

| slot | label flips STANDARD→OFF_PEAK when window starts by |
|---|---|
| 07:30 | 07:00 (−30 min) |
| 08:00 | 07:00 (−60 min) |
| 08:30 | 06:30 (−120 min) |
| 09:00 | 07:00 (−120 min) |
| 09:30 | 06:30 (−180 min) |

Once flipped, the label stayed OFF_PEAK for **every deeper look-back, out to 48h**
— the flip is **monotonic**, so over-reaching is always safe. A run-*start* slot
(31 Oct 22:30) reads OFF_PEAK even single-slot (it already sees its own anchor).

Interpretation: Octopus computes the dispatch (IOG) OFF_PEAK bucket **relative to
the query window**. If the window doesn't include the charging run's start, the
slot falls back to the raw STANDARD tariff — the wrong, peak value. A single-slot
fetch is the worst case. This is *why* the earlier single-slot recovery mispriced
mid-run slots to peak, and why the repair's single-slot straggler pass had to go.

### 0b. The strip is DETERMINISTIC and complexity-driven, not random

A `--repeat` test fetched two same-day slots 12× each at two window sizes:

| slot | ±1h window | 12h window |
|---|---|---|
| 02:00 (heavy 6 kWh charge) | cost **12/12** OFF_PEAK | **0/12 — 100% empty** |
| 18:00 (quiet, 0 kWh) | 12/12 | 12/12 |

So the empty `statistics` is **not random**: a **small** window returns the cost
100% of the time — even for a heavy dispatched slot — while a **wide window over a
dense charging run** is stripped **100% of the time** (too many heavy nodes → over
Octopus's per-query complexity budget). A wide window over *quiet* data is fine.
That is why the stragglers are always the same areas: the heavy-charging runs are
the only places a wide window turns expensive. (The earlier `--context` "randomness"
was this — those look-backs that reached into the dense run stripped; the others
didn't.) The earlier "random / more retries / wider window" conclusion was **wrong**;
a 12h window was the *worst* choice.

### 1. `KrakenAPIClient.recover_measurement_costs(mpan, starts, …)` — look-back ladder

Reconcile the two facts (small windows are reliable §0b; some slots still need
look-back for the label §0) with a **ladder**. For each slot, try the **smallest**
window first (`lookback_ladder=(1,3,6)` h before → slot+1h): **accept immediately on
OFF_PEAK** (the truth, and reliable at small sizes); only **widen** to the next rung
if it came back STANDARD (might merely lack context); **stop** the instant a rung
returns OFF_PEAK. This never sends a wide window over a dense run — those resolve
OFF_PEAK on rung 1 — so it **dodges the strip entirely**. A rung that returns empty
is skipped; the smallest STANDARD seen is the fallback. One fetch opportunistically
claims any other pending slot it proves OFF_PEAK; newest-first so a run's later slots
sweep up earlier ones. Returns `{start: parsed_node}` for slots recovered **with a
cost**; the rest omitted (caller keeps its fallback; **no invented price**). Read-only.

Superseded cuts: single-slot (wrong label for dispatch-extended slots, §0); 48h
date-window and 12h window (deterministically stripped over dense runs, §0b).

### 2. In-pass recovery in `import_api_history` (`recover_costs=True`)

After each chunk is priced, the material no-cost slots (`fb_starts`) are passed
straight to `recover_measurement_costs` **before the write**. Recovered slots are
repriced in place from the real billed cost + dispatch-aware label and dropped
from the repair queue. The import thus **self-heals in one pass** — the OFF_PEAK
charging slots get their true cost the first time, not via a later manual step.
`recover_costs=False` preserves the old schedule-fallback behaviour (used in a
regression test to prove recovery is what changes the outcome).

### 3. `repair_import_pricing`

The repair's main window now opens **a day before** the first slot in each group
(same context reason — a leading-edge slot would otherwise read STANDARD). Slots
the window still returns cost-less go to `recover_measurement_costs` (context-
anchored per date). Only slots empty even then count as `still_missing`.

### 3b. Billed cost is authoritative — rate = cost÷kWh (`_billed_rate`)

A separate bug surfaced on the 2026-04-01 price-cap boundary: EMT took the block's
**cost** from Octopus's billed figure but the **rate** from the LOCAL tariff
schedule (keyed by the off/peak label). The schedule had the 1-Apr rate change
mis-dated onto 31 Mar, so *repriced* slots were stamped the new 5.493p rate while
their (correct) billed cost stayed at 9p — `rate × kWh ≠ cost` on the same row.

Fix: `_billed_rate(rate_segs, start, off_peak, meas_cost, kwh)`. When Measurements
gave a cost, the rate is **cost÷kWh** — the clean scheduled value is kept ONLY when
it agrees to within 0.1p (so tidy 0.0700 bands don't fragment); otherwise the local
schedule is stale and we trust the bill. No cost → schedule (time/label). Applied in
the import, the repair (window + straggler), and the in-pass recovery. Costs are
untouched, so validated bill totals don't move — only wrong rates get reconciled.

### 4. Explicit complexity / node-limit handling

`KT-CT-1188` (complexity > 200) and `KT-CT-1189` (> 10,000 nodes) now raise a
distinct `KrakenQueryTooLargeError` with a clear log — because, unlike
`KT-CT-1199`, waiting does not help: the request is too big and the caller must
*shrink the window*, not back off.

### 5. UX

The import panel states plainly that retrieval can be slow (Octopus rate-limits
progressively; the import paces and auto-pauses when the hourly allowance runs
low), and that **other apps on the same Octopus account share that allowance** —
the Octopus app, the Home Assistant Octopus (BottlecapDave) integration, or any
other API tool — so a busy one will slow or pause the import. Expected; leave it
running.

## What we deliberately did *not* do

- **No CSV/PDF bill overlay as the primary fix.** A CSV reprice remains only a
  last resort for slots the API genuinely never returns — a set the experiment
  showed to be **empty** for this account. Overlaying the bill on load-dropped
  slots would have masked the real defect.
- **No blanket "assume off-peak" for no-cost slots.** The flagged slots are a
  *mix* of genuinely-peak and dispatched-off-peak; only the real per-slot label
  distinguishes them, so we fetch the label rather than guess it.

## Validation

- Isolated single-slot recovery converges deterministically (probe `--deep`,
  3/3 per slot).
- Unit: `recover_measurement_costs` recovers an empty-`statistics` slot on retry,
  omits a genuinely-empty one, and paces; `KT-CT-1188/1189` → `KrakenQueryTooLargeError`.
- Engine: in-pass recovery prices a no-cost slot from the exact billed figure and
  clears it from the repair queue; with `recover_costs=False` it stays
  schedule-priced and queued.
- Full suite green.
- **Field check (pending):** rebuild, re-import 3 Oct – 2 Nov, re-compare to the
  bill — with the OFF_PEAK slots recovered at source, the 52.5 kWh mis-split
  should close.

## Files

- `kraken_api_client.py` — `recover_measurement_costs`, `get_measurements(quiet=)`,
  `KrakenQueryTooLargeError`, `_GQL_COMPLEXITY_CODE` / `_GQL_NODE_LIMIT_CODE`.
- `engine.py` — `import_api_history` in-pass recovery (`recover_costs`,
  `recover_pace_s`); `repair_import_pricing` single-slot straggler pass.
- `web/templates/historical_import.html` — rate-limit / shared-account copy.
- `octopus_cost_probe.py` — the standalone investigation tool (sections 5–6,
  `--deep`); kept for future diagnosis.
