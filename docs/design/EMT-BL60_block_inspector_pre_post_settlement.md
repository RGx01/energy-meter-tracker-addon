# BL-60 (proposed) — Usage Stats block inspector: pre/post-settlement detail for a single block

*Future backlog item. Turns the manual forensic we ran on 2026-08-14 19:00 (SQL + measurement
probes) into a self-serve drill-down: click one block and see its full provenance — what EMT
priced it, what Octopus billed it, the dispatch lifecycle behind that, and how it changed from
provisional → settled.*

## Motivation

Diagnosing the 14/08 discrepancy took a DB dig plus two probe scripts to answer a simple question:
*"why does EMT say off-peak and Octopus say peak for this block, and when did that happen?"* Every
input needed already lives in the store (`blocks`, `block_segments`, `dispatch_slots`,
`dispatch_history`, and now `measured_cost`). The inspector exposes them for one block so a user (or
we) can answer that in the UI — and, crucially, spot **billing discrepancies to dispute** (14/08
was Octopus's error, in the user's favour).

## Trigger / placement

- Usage Stats HH view (`/blocks-day`): make each half-hour row expandable, or add a "🔍 details"
  affordance that opens a modal/panel for that `block_start`.
- Read-only. New endpoint `GET /block-detail?start=<iso>&meter=electricity_main` returning the
  assembled view; no new capture.

## What it shows (one block)

1. **Identity & settlement state**
   - `block_start`/`block_end`, meter, config period.
   - **kWh:** provisional (mini/estimate) vs settled `imp_kwh_api` (DCC), and `is_provisional` /
     `interpolated`. "Settled at" if we can date it (poll window / `fetched_at` proxies).
2. **Price provenance (the headline)**
   - `rate_source` (schedule / reconciled / measured / corrected) with a plain-language label of
     *who set this price and why*.
   - `imp_rate` / `imp_cost` inc **and** exc (`imp_rate_exc`/`imp_cost_exc`, `exc_source`), band.
3. **Pre vs post settlement (side by side)**
   - Provisional price EMT first showed (mini/overlay) → settled price after DCC + reconcile →
     measured price if applied. The transition is the story.
4. **EMT vs Octopus (the reconciliation)**
   - EMT's schedule/dispatch-derived price **vs** `measured_cost` (Octopus billed): `cost_incl`,
     `label` (OFF_PEAK/STANDARD/mixed), `kwh`, `fetched_at`.
   - A clear verdict badge: **agree** / **label-only** (band differs, £ agrees) / **material band
     flip — possible billing discrepancy** (the 14/08 case). Reuse the `audit_measured_costs`
     classification.
5. **Dispatch lifecycle (why EMT priced it as it did)**
   - `dispatch_slots` (off_peak, provider, source, state, energy_planned/completed) +
     `dispatch_history` rows (planned/started/completed, energy, `first_seen`). This is what shows
     "planned + started + completed = genuine smart charge" at a glance.
6. **EV / house split**
   - `block_segments` (kwh, inc/exc rate, band, attribution) + the legacy `imp_*_ev` columns, so the
     split and its bands are visible (and cross-checked against each other).
7. **Review status**
   - `needs_review` / `review_reason` / `review_dismissed`; and once §3c ships, the "material flip
     held for review" flag with its reason.

## Data sources (all already captured — no new writes)

`blocks`, `block_segments`, `dispatch_slots`, `dispatch_history`, `measured_cost`. The endpoint just
joins them for one `block_start` and runs the existing band/agreement classification.

## Related / dependencies

- **BL-53 measured capture (done)** — supplies the "Octopus billed" column + `fetched_at`.
- **§3c apply (pending)** — the review-flag it introduces surfaces here as the discrepancy badge.
- **Re-settlement watchdog (future, own item):** the 14/08 dig showed Octopus can *re-settle* a slot
  (the "measured cost never changes" assumption is not safe). A companion feature would periodically
  **re-verify** cached `measured_cost` and, on a change, keep the history so the inspector can show
  "Octopus re-billed this slot off-peak → standard on <date>." The inspector is the natural surface
  for that trail; BL-60 should leave room for a per-slot measured-cost history rather than a single
  row.

## Value

- Self-serve answer to "what happened to this block?" without SQL or probes.
- Surfaces **billing discrepancies** (EMT-right / Octopus-wrong) with the dispatch evidence attached
  — an evidence trail for a support call, exactly like the 14/08 case.
- Makes the pre→post settlement journey (provisional → DCC → reconciled → measured) legible, which
  is otherwise invisible in the aggregate charts.
