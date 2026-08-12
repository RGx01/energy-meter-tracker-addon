# Bill-accurate rounding + pre-VAT figures — design note

> **Current source of truth:** the **"4.2 as shipped — the full ex-VAT pipeline"** and
> **"Gaps / open items"** sections near the end reflect what actually landed and what's left.
> The earlier sections are the historical design record; where a scattered "still to do" note
> conflicts with those two sections, the two sections win.


> _Status: **Groundwork landed in 4.1.3; full feature targeted for 4.2.** Two linked,
> billing-neutral features: (A) retain the pre-VAT (exc-VAT) figures Octopus already
> sends, and (B) an opt-in "round the Octopus way" totals mode that runs on them.
> Neither mutates stored per-slot data or the inc-VAT figures that drive today's
> bill/charts._
>
> **Shipped in 4.1.3 (additive, default-off — inc-VAT billing byte-identical):**
> the `imp_cost_exc` column + store round-trip for (A), and the pure
> `octopus_bill_total()` ladder + Decimal `bankers_round()` for (B), wired to nothing.
> **Deferred to 4.2:** import-time exc capture + backfill (A), and the opt-in
> `bill_rounding` toggle + inc/exc display toggle (B).

## Guiding principle (north star for the storage model)

> **Store the purest inputs; treat the bill as a derived view.** The canonical data Octopus
> bills from is **kWh, the ex-VAT unit rate, and the ex-VAT standing charge** — all at *full
> precision, never pre-rounded*. (VAT itself isn't stored: it's derived from the inc/exc pair,
> `inc ÷ exc − 1` — see "capture completeness".) The **inc-VAT bill total is a derived view**
> over those inputs: **sum the raw ex-VAT per half-hour, round only at the subtotal/total**
> (whole penny), VAT on top — **not** per-half-hour, and **not** an independently stored number
> (see "How Octopus rounds", CORRECTED). Store whichever figure Octopus treats as authoritative per
> source and never discard it: **rate** is derived where the API returns cost as truth
> (measurement/settled slots); **cost** is derived where the published rate is primary
> (tariff/bill slots). Today EMT stores already-inc, already-rounded figures — most of our
> reconciliation drift is that pre-rounding + the inc-VAT basis. Migrating toward this model
> is what removes the drift **for everyone, including users with no API to reconcile against.**
>
> 4.2 does not perform that migration, but it **must capture everything the migration will
> need** (see "capture completeness" below) so it can happen later with **no re-import** —
> critical because an offline (CSV/PDF-only) user can never backfill ex-VAT from the API.

## Motivation
A user on Agile compared a plunge-price day (26 Jul 2026) against Guy Lipman's
reference calculator and EMT's total didn't match to the penny. Investigation
(see also the negative-cost display fix) showed the stored data is correct; the
gaps are **rounding convention** and **VAT basis**, not a pricing bug.

## How Octopus rounds a bill (measured against a real bill — CORRECTED)

> **Earlier drafts of this note were wrong** and said Octopus rounds *per half-hour*
> (round kWh to 0.01, × rate, round cost to 0.01p, sum). A real Intelligent Octopus Go
> bill (Jul 2026) + the matching block DB **disproved that** — see below. Keeping the
> corrected method here so nobody re-implements the per-half-hour ladder.

What Octopus actually does:
1. **Sum the RAW half-hourly figures** — kWh and ex-VAT cost — with **no per-half-hour
   rounding.** (Per-HH kWh rounding is demonstrably wrong: an account with ~1000 sub-0.01
   kWh peak slots would have them all round to 0, collapsing a 4.194 kWh band to 4.03.)
2. **Round only at the SUBTOTAL** (whole penny), from the raw sum. *Evidence:* on the bill,
   energy £49.195 + standing £14.8955 → subtotal shown **£64.09** = `round(64.0905)`, **not**
   `round(49.20) + round(14.90) = 64.10`. So it rounds the raw total, not the displayed lines.
3. **VAT** = `round(subtotal_raw × VAT)`; **Total** = subtotal(2dp) + VAT(2dp).
4. The **band kWh shown to 1dp** (916.0, 4.2) is **display only** and does **not** reconcile
   with the charged cost — the bill charges £1.277 at 30.77p, which implies 4.15 kWh, while
   *displaying* 4.2. That's Octopus's own inconsistency; **EMT should not replicate the 1dp**
   and should show full-precision kWh so `rate × kWh ≈ cost`.
5. The **"total consumption × average rate"** line (`920.1 kWh @ 5.35p †`) is a *display /
   comparison* line — algebraically equal to the sum of the per-band `kWh × rate`, differing
   only by rounding; it is **not** a separate calculation to replicate.

**Implementation note:** `bankers_round()` (Decimal-based half-even) is still used for the
**subtotal/total** whole-penny step — Python's float `round()` mishandles exact decimal
halves. The per-half-hour rounding is **removed**.

Sources: guylipman.com/octopus/api_guide.html; and — decisively — a **real Octopus bill
reconciled block-by-block** (below).

## Validation — real Intelligent Octopus Go bill (3 Jul – 2 Aug 2026)

The bill's **37 pages include a per-half-hour breakdown for every day** (rate + kWh + cost per
slot) — the authoritative data. Diffing **all 1,488 half-hours** against EMT's blocks:

| band | Octopus (per-½-hour detail) | EMT | Δ |
|---|---|---|---|
| Off-peak cost (ex-VAT) | £47.9207 | £47.9207 | **£0.0000** |
| Peak cost (ex-VAT) | £1.2909 | £1.2905 | £0.0004 |
| **Energy total (ex-VAT)** | **£49.212** | **£49.211** | **~0.1p** |
| Off-peak/peak label | — | — | **0 / 1488 mismatches** |

**EMT is essentially exact** — every rate label matches Octopus, and the costs reconcile to
**~0.1p** against the real half-hourly data. The tiny peak residual (0.04p) is `inc ÷ 1.05`
vs the true exc rate — closed once real exc is captured (BL-23).

**The earlier "3p gap / settlement snapshot / ~0.044 kWh mis-split" was WRONG** — it came from
trusting the bill's **summary page**, which is internally inconsistent (it prints
"4.2 kWh @ 30.77p = **£1.277**", but 4.2 × 30.77 = £1.29, and Octopus's *own* half-hourly detail
sums the peak band to **£1.2909**). The summary rounds kWh to 1dp and its bands don't reconcile
with its own detail; EMT (which sums the real per-slot figures) is actually **more
self-consistent than the summary page**. So: no mis-split, no settlement gap, no mis-labelled
block. Lesson: **validate against the half-hourly detail, never the summary page's rounded lines.**

*(The earlier 26 Jul "ladder → −108.0p" analysis is likewise superseded — it was based on the
disproven per-half-hour rounding method.)*

## What the bill-rounding method actually buys us (reconstruction vs reconciled)

A key realisation from the validation above: **once EMT holds Octopus's reconciled cost, the
bankers/ex-VAT rounding ladder changes nothing.** Working through it end-to-end, the inc-VAT
total EMT already computes and the total produced by the "sum raw ex-VAT → round at subtotal →
add VAT" method **agree to the penny**. So what was Guy Lipman's point? The method only bites
where EMT has to **reconstruct** a cost as `rate × kWh`:

- **Reconstruction path (rate × kWh) — the method matters.** Live/CAD/sensor slots, any
  no-cost fallback slot, and CSV rows that carry a rate but no cost. Here the choice of *when*
  to round genuinely moves the total (per-slot rounding zeros sub-0.01 kWh slots; summing raw
  and rounding at the subtotal is what Octopus does). This is also the **only** path an offline
  CSV/PDF-only user ever has, which is why capturing real ex-VAT (4.2) matters for them.
- **Reconciled path (Octopus's billed cost) — the method is moot.** Historical API import
  stores Octopus's own `cost_incl`/`cost_excl` per slot. EMT's total is then the bill **by
  construction** — summing figures Octopus already rounded, so there is nothing left for a
  rounding method to change. This is the usual case for settled history.

So the bill-rounding option is best understood not as a "make my total match the bill" fix
(EMT already matches, ~0.1p, on reconciled data) but as (a) an **ex-VAT presentation** of the
bill, and (b) correctness insurance for the **reconstruction** path where EMT itself prices
`rate × kWh`.

**And a caution about Octopus's own PDF:** the bill's **summary page is unfathomably
inconsistent with the per-day half-hourly detail tables tagged onto the end of the same PDF.**
The summary rounds each band's kWh to 1dp and prints lines that don't even tally with their own
stated figures (e.g. "4.2 kWh × 30.77p" charged as £1.277, not £1.29), while the half-hourly
detail — the authoritative data — sums that same band to £1.2909. EMT reconciles to the
**detail**, so where EMT and Octopus's summary appear to disagree, it is Octopus being
inconsistent with its own API, not an EMT error.

### Ex-VAT historical backfill (shipped — the "re-pass" that avoids a re-import)

A re-import is **not** needed to populate ex-VAT across old history. For an API account we hold
the tariff agreements (GraphQL `value_exc_vat`, carried on `RateSchedule.exc`) and the stored
per-block kWh, so `_run_historical_exc_backfill` computes `cost_exc = kWh × the tariff's
published ex-VAT rate` for every import block whose `imp_cost_exc` is NULL and persists it in
place. The exc rate is obtained by **scaling the block's stored authoritative inc rate by the
tariff's exc/inc ratio at that slot** (`_exc_rate_for_block`) — so it lands on the *published*
ex-VAT figure (correctly rounded per band), not a blind `inc ÷ 1.05`, and needs no stored
OFF_PEAK label. Additive (inc columns + Total Bill untouched), idempotent (`set_block_exc`
fills only NULL exc), resumable (a `exc_backfill_state` cursor/done marker), and a no-op without
API tariff coverage. It runs at the **end of the post-import verify pass** (inc pricing already
final) and is re-armed on startup. A slot the tariff genuinely can't cover is left NULL and the
views fall back to the period's VAT rate (from the calendar, not a hardcoded 1.05), shown as an
approximation. This is the key correction to the
earlier "existing history needs a re-import" note: **reconstruction (tariff × kWh) is the
canonical fill and reproduces the bill's ex-VAT to ~0.1p — reconciled `cost_excl` was fetched
and discarded pre-4.2, and re-fetching it buys nothing measurable.**

## (A) Retain pre-VAT figures — the data is already in hand

> **Status (4.1.3):** the additive nullable **`imp_cost_exc`** column is added (idempotent
> migration) and the store **round-trips** it — written by `_block_rows` + both batch
> INSERTs, surfaced on read by `_row_to_block` as the import channel's `cost_exc`. Verified
> byte-identical: on a full history the column is `NULL` everywhere and `SUM(imp_cost)` is
> unchanged. ~~**Still to do (4.2):** populate it at import + a re-import backfill.~~ **Done (4.2):**
> populated at import (measurement/tariff) **and at settlement**, plus a one-time tariff backfill —
> see "4.2 as shipped". No re-import needed.

EMT **fetches** exc-VAT on every import and discards it:
- Unit rates return `value_exc_vat` + `value_inc_vat`; `from_api_records` keeps only inc (kraken_rates.py:168).
- The Measurements query already selects `costExclTax` + `costInclTax`; `_parse_measurement_node` computes both `cost_excl` + `cost_incl` (kraken_api_client.py:1222–1223). The import reads only `cost_incl`.
- The `blocks` table has no exc columns, so `imp_rate`/`imp_cost` are inc-VAT only.

**Change:** add `imp_cost_exc` (and derive `imp_rate_exc = cost_excl ÷ kWh`), populated at
import from the `cost_excl` already present in the parsed row. Additive schema,
**billing-neutral** (inc figures unchanged). No new API calls.

**Existing history:** exc wasn't stored → only `inc ÷ 1.05` (approx) until a **re-import**
backfills the real exc. Live/settled measurements carry exc, so it's captured for free
going forward once the columns exist.

## (B) Opt-in "bill-style rounding" totals mode

> **Status:** **shipped for the Billing summary (BL-24 first cut).** `_bill_method_breakdown`
> sums the raw ex-VAT and rounds at the subtotal (see "How Octopus rounds", CORRECTED);
> `octopus_bill_total()` / `bankers_round()` (Decimal half-even) do the whole-penny step;
> `bill_slots_from_blocks()` prefers stored `rate_exc`/`cost_exc`, else the calendar VAT rate.
> Behind the **`bill_rounding_summary`** setting, default off. ~~**Still to do:** data tables,
> Usage Stats, export credit, inc/exc toggle.~~ **Now:** data tables **shipped** (Rate exc / kWh /
> Cost exc / Cost inc); Usage Stats **deliberately not** extended (decided); the inc/exc view **is**
> the toggle. **Only export credit in the ex-VAT total remains** (see "Gaps / open items").

- Setting **`bill_rounding_summary`** (bool) in the `store_meta` settings (like the carbon assumptions), default off.
- Applied **on-read, at the totals layer only** — never mutates stored per-slot values, reversible, no reimport. Default off ⇒ nobody's figures move unless they opt in.
- Core: **sum raw ex-VAT, round at subtotal** (NOT per half-hour), on the **stored exc** (feature A) rather than `inc ÷ 1.05`. Unit-tested + validated against a real bill (3p, settlement).
- **Honest label:** "bill-style rounding (matches Octopus's *method*)", NOT "matches your bill to the penny" — penny parity also needs the same consumption snapshot (the 3p above).
- **Bonus:** an inc/exc **display toggle** — once exc is stored, showing ex-VAT totals is a genuine feature for business / reimbursement users.

## 4.2 refined plan — import-source coverage & CSV contract v2

The 4.1.3 groundwork (the `imp_cost_exc` column + store round-trip, and the pure
`octopus_bill_total()` ladder) is in. 4.2 turns it into real ex-VAT data across **every**
history source, so displayed billing reflects what the user actually receives — then the
opt-in rounding rides on top (BL-24, still gated separately).

### Where the exc figure comes from, per source
- **API — measurement slots:** the real `cost_excl` (`costExclTax`) already parsed. Penny-exact.
- **API — tariff-priced slots** (Measurements gave no cost, so cost = `kWh × rate`): derive
  `cost_exc = kWh × exc-rate`, where the exc-rate comes from the tariff's **`value_exc_vat`**.
  Today `RateSchedule.from_api_records` keeps only `value_inc_vat` (kraken_rates.py:168) — 4.2
  carries a parallel exc-rate series through the schedule.
- **PDF-bill slots:** the bill's printed **pre-VAT** figures — `rate_pre` / `cost_pre` /
  `standing_pre_p` — which `bill_parser` already extracts and currently **discards** (it grosses
  them to inc-VAT for the CSV, blanks the cost). These are canonical, at the bill's own printed
  precision. 4.2 stops discarding them.
- **CSV slots:** the new optional exc columns when populated (below); else the tariff exc-rate
  (API), else the `inc ÷ 1.05` approximation.

### Capture completeness — everything a future no-API migration will need
The migration to the derived-view model must not require re-fetching from Octopus. For an
**offline (CSV/PDF-only) user the API can never backfill ex-VAT**, so anything 4.2 fails to
capture at import from their bills is lost forever (only `inc÷1.05` remains). So 4.2 must
persist, per slot/day, **at full precision (unrounded)**:

- **kWh** — raw (already stored).
- **ex-VAT unit rate** — stored **explicitly** where it's the published primary (tariff/bill),
  not only derived from `cost ÷ kWh`, so a zero-cost or zero-kWh slot still carries the rate.
- **ex-VAT cost** — `imp_cost_exc` (the 4.1.3 column), from measurement `cost_excl` / bill
  `cost_pre` where authoritative, else `kWh × ex-rate`.
- **ex-VAT standing charge** — per day (bills itemise it; needed to derive the inc standing view).
- **provenance** — which figure is authoritative (measurement / tariff-derived / bill-printed),
  so the future derived-view knows what to trust vs recompute.

**VAT is derived from the pair — not a stored field.** Every authoritative source hands us both
sides: the rate API record is `{value_inc_vat, value_exc_vat}`, Measurements returns
`costInclTax` + `costExclTax`, and bills print both. So **VAT rate = inc ÷ exc − 1**, per
period — no external source, no hard-coded constant, no VAT column. It's **future-proof by
construction:** when VAT is removed (2026) `inc == exc` and the derived VAT is simply 0%; a past
or future change tracks itself the same way. Caveats: the ratio carries small rounding noise
(`inc` is a *rounded* `exc × (1+VAT)`), so use it to *identify the band* (0/5/20%) and prefer
showing the stored inc or exc **directly** rather than doing VAT arithmetic.

**Where there's no ex source, accept the limitation — don't fabricate one.** A pure-rate-sensor
user with no API and no bill PDFs has *no ex-VAT ground truth anywhere*: their sensor is inc-VAT,
and `inc ÷ (1+VAT)` only hands back an approximation (inc was a rounded gross-up of ex). So we do
**not** add a VAT default to synthesise ex from inc — it would be fabricated precision, and
bill-method rounding on it is a double approximation that still won't match the bill. Nor is it
needed for the derived-view model: **for an inc-only block the "derived bill view" is just the
stored inc** — nothing to split. These blocks stay inc-VAT only; ex-VAT display and bill-method
rounding are simply **unavailable and clearly labelled** for them. (This also means the 2026 VAT
removal and any future change need no configured default — users who *have* ex get VAT from the
pair, `inc == exc` → 0%.)

**Offline → online recovers history.** The products/tariff **rate API is not retention-limited**
(unlike Measurements' ~2-year wall): `get_unit_rates` returns the full historical series for any
date, keyed on tariff code (from the account's agreements per date). So if an offline user later
connects an API, EMT can backfill exact inc **and** exc rates + standing across *all* history.
The genuinely unrecoverable case is therefore narrow — inc-only CSV **and** never-online **and**
no bill PDFs — so capture ex where it's cheap, but don't contort the schema for it.

Export mirrors import (ex-VAT credit/rate) where the outgoing tariff has VAT semantics.

*With these captured, a later release can flip the inc-VAT bill/charts from "stored inc" to
"derived from stored ex" with **no re-import** — so the drift reduction reaches offline users
too. The **schema and capture are the load-bearing part of 4.2**; the rounding view (BL-24)
is comparatively easy and can follow.*

### CSV contract v2 (additive, back-compatible)
Current §1: `Start, End, Consumption (kWh), Unit Rate (p/kWh), Estimated Cost Inc. Tax (p),
Standing Charge Inc. Tax (p)`.

Add **optional** columns:
- `Unit Rate Exc. Tax (p/kWh)`
- `Estimated Cost Exc. Tax (p)`
- `Standing Charge Exc. Tax (p)` (optional — bills itemise it; nice for completeness)

Rules:
- **Prefer exc when populated:** if an exc column has a value, the importer uses it (sets the
  block's `cost_exc` / derived exc rate); if blank, it falls back (tariff exc-rate, else `inc÷1.05`).
- **Old CSVs still import unchanged** — inc-only files are valid; the exc columns just stay empty.
- **VAT basis is per-bill**, not a global 5% — capture exc *explicitly* rather than derive it,
  because a bill can carry a reduced-rate period (`bill.vat_import`).

### Backfill (mostly offline — no API re-import needed)
- **Bill/CSV history:** re-run the *file* import over the same PDFs/CSVs the user already has →
  exact exc, entirely offline. (This is the important nuance: "re-import" here means re-parsing
  local files, **not** an Octopus API re-fetch.)
- **API history:** recompute `cost_exc = kWh × exc-rate` offline from the stored rate schedule
  (principled), or a Measurements re-fetch for penny-exact *measurement* exc.
- **A pure in-DB migration can only ever yield `inc÷1.05`** (the stored inc was itself
  `round(exc × 1.05)`, not losslessly invertible) — so it is **not** offered as "exact".

### Invariants (unchanged)
Inc-VAT `imp_rate`/`imp_cost`, the **Total Bill**, and every current chart/stat stay
**byte-identical**. The ex-VAT columns are purely additive; nothing reads them until the
BL-24 opt-in. (Note: the earlier "derive `imp_rate_exc` on read, no column" idea is
**superseded** by the capture-completeness requirement above — for offline users the published
ex rate must be stored explicitly, not reconstructed from `cost ÷ kWh`.)

### Build order within 4.2
1. `RateSchedule` carries the exc-rate series (unlocks API tariff-priced + the CSV fallback).
2. Store capture: write `imp_cost_exc` on all write paths (API import, live/settlement, CSV/bill).
3. `bill_parser` emits the exc columns; CSV importer reads them (prefer-when-populated) + blank-template + example rows.
4. **BL-24 (separate gate):** opt-in `bill_rounding` + inc/exc display toggle, once exc data is proven in the field.

### Docs/templates to update when built
CSV §1 contract, the blank template (headers + example rows), the in-app template/help text,
`docs/bill_to_csv_import_spec.md`, and `docs/historical_import_design.md`.

### Locked / resolved (was "Still to lock")
All five of these are now settled — verified against the shipped code + a real DB:
- **Column shape → DONE.** `imp_cost_exc`, `imp_rate_exc`, `standing_charge_exc`, and `exc_source`
  (the provenance tag) all exist; **no** `vat_rate` column, as decided.
- **ex-VAT unit rate: store vs derive → IMPLEMENTED as written.** `imp_rate_exc` is *stored* for
  tariff/backfill/settlement slots and left NULL (derived on read as `cost_exc ÷ kWh`) for
  measurement slots — exactly "store where published-primary, derive where cost is authoritative."
  (Prod DB: `exc_source` = `tariff` on 35,202 slots, `measurement` on 1,488.)
- **VAT rate → RESOLVED + extended.** Still derived from the inc/exc pair (`inc ÷ exc − 1`), no
  stored column, no hardcoded default — now backstopped by the **statutory VAT calendar**
  (`vat_calendar.py`, seed + self-learned) for the fallback rate, labels and boundary dates.
  Inc-only/no-API blocks still get no synthesised ex — they're **gated out** (hidden), consistent
  with "accepted and labelled, not papered over."
- **BL-24 Billing vs Usage Stats → DECIDED: Billing only.** Usage Stats is deliberately left as-is
  (it already ties to the billing summary inc-VAT).
- **ex-VAT standing charge: "capture it" → SUPERSEDED (derive, don't capture).** `standing_charge_exc`
  is NULL on every row and *intentionally so*: the summary derives standing ex-VAT as
  `inc ÷ (1+VAT)`, which is **exact for a flat charge**, and the VAT calendar makes that exact for
  offline/holiday cases too. The column exists but explicit capture is unnecessary. (If a supplier
  ever varied the standing charge *within* a period, revisit — until then, derivation is exact.)

## BL-24 — first cut (shipped)

*Scope: bill-style rounding on the **bill summary on the Billing charts** only. Individual
charts, data tables, and Usage Stats are explicit later developments.*

- **Control — a new 5th toolbar button "Bill Rounding"** (🧮) alongside Add Device / Refresh
  Entities / Change Setup / Billing History → its own page (`/bill-rounding`,
  `bill_rounding.html`). *Shipped.*
- **Storage:** `bill_rounding_summary` in `SETTINGS_DEFAULTS` (the `store_meta` settings, same
  mechanism as the carbon assumptions), **default off**. *Shipped.*
- **Render — `_bill_method_breakdown(period_blocks, standing_inc_by_day=summary["standing"])`**
  injected into the summary in `generate_daily_import_export_charts`; `render_billing_summary`
  shows an ex-VAT section when present. **Default off ⇒ the bill summary is byte-identical.**
  The method (per "How Octopus rounds", CORRECTED):
  - per rate band: **sum the RAW ex-VAT** (billed cost ÷ (1+VAT), or stored `cost_exc`);
    **no per-half-hour rounding**, and **kWh shown full-precision** (not Octopus's 1dp).
  - **standing** from the summary's **per-local-day** figure (correct count across the BST
    midnight boundary), ex-VAT via `inc ÷ (1+VAT)` — exact-to-the-mill for a flat charge, so
    it lands even before `standing_charge_exc` is captured.
  - **VAT derived from the inc/exc pair** (snapped 0/5/20%; 0% once VAT removed).
  - round only at **subtotal / VAT / total**; layout: per-rate exc rows → Total (exc) →
    Standing (exc) → VAT row → Total incl. VAT.
- **Validated** against a real IOG Go bill — reconciles to **~0.1p** against the authoritative
  half-hourly detail (0/1488 rate mismatches); the earlier per-half-hour ladder gave a wrong
  day cost (£1.24 vs £1.29) and is removed.
- **Presentation (finalised):** when the option is on, the ex-VAT bill method **REPLACES** the
  inc-VAT "Import — total grid" display in-section (per-rate ex-VAT → Total (exc) → Standing
  (exc) → VAT row → Total incl. VAT); the standalone "Total incl. standing charge" inc line is
  the thing it replaces. The **"Total Bill" grand total is untouched**. The old appended
  presentation is gone, along with its **"Bill-style rounding (Guy Lipman method) — ex-VAT ·
  coverage %" header** (Guy Lipman is credited on the enable page, not repeated here) and the
  **"matches Octopus's method, not penny-perfect" footnote**. Any apparent gap vs Octopus's
  summary page is Octopus being inconsistent with its own API (see "reconstruction vs
  reconciled" above), not an EMT rounding shortfall.
- **Data table (shipped):** when the option is on, each day's per-slot data table shows the
  IMPORT columns as **Rate (exc) / kWh / Cost (exc) / Cost (inc)** (export is zero-rated, so it
  keeps its single cost column). The ex-VAT figure is a per-slot inc→exc **ratio** taken from the
  block's stored ex-VAT rate (authoritative for the main import after the backfill), applied
  uniformly to every import column and the Total Import; a slot with no captured exc falls back to
  the period's VAT rate (from the calendar, not a hardcoded 1.05) and is flagged with an **≈**.
  Emitted only when the setting is on, so the default table JSON is byte-identical.
- **Usage Stats — deliberately left as-is (decided):** the ex-VAT view is **not** extended to
  Usage Stats. Usage Stats already tallies to the billing summary **exactly, inclusive of VAT**,
  so an ex-VAT overlay there would add complexity for no user gain — the **billing summary** is
  the surface where ex-VAT helps a user understand their bill, and that's where it lives.
- **Open / later:** **export credit** not yet in the bill-method total (import energy + standing
  only).
- **Per-channel exc under the IOG 6-hour-cap split (deferred, tracked in `iog_6hr_cap_design.md`
  §D½):** settlement's exc capture stamps only the MAIN import channel, and the data table uses one
  shared VAT ratio from it. Correct today (single tariff; VAT is a flat 5% so the exc/inc ratio is
  uniform) and still numerically correct under the 4-rate split — but when the EV sub-meter is
  billed at its own `ev_device_*` rate, the *stored* per-device `cost_exc` should be captured from
  that channel's own rate. Build with the ev_device rate support, since the `ev_device_exc`
  schedule doesn't exist until then.

## 4.2 as shipped — the full ex-VAT pipeline (current source of truth)

The end-to-end pipeline that actually landed, capture → store → read → present. All additive;
the inc-VAT figures and the **Total Bill** stay byte-identical unless the user opts in.

**1. Capture ex-VAT at every write path (the primary, per-slot source).**
- **Import (measurement):** `imported_api`/`imported_csv` slots store Octopus's real `cost_excl`
  (`exc_source='measurement'`) — penny-exact.
- **Import (tariff-priced):** where Measurements gave no cost, `cost_exc = kWh × tariff exc-rate`
  (`exc_source='tariff'`), the exc-rate from `RateSchedule.exc` (`value_exc_vat`).
- **Settlement (durable):** `_rerun_pass2_for_settled_block` now computes `cost_exc`/`rate_exc`
  when it re-costs a settled block — the resolved inc rate scaled by the tariff's exc/inc ratio at
  the slot — persisted via `append_block_replace` (verified to round-trip the exc columns). **Every
  reconciled block carries real ex-VAT the moment it settles, with no backfill.** The live/
  provisional tail (unsettled `kraken_api`) stays on the fallback until it settles.
- **One-time historical backfill (imported + settled-live):** `_run_historical_exc_backfill` fills
  blocks whose `imp_cost_exc` is NULL via the tariff reconstruction (`_exc_rate_for_block`),
  batched + yielding (one transaction/chunk, `await` between chunks — an earlier per-block-commit
  version blocked the event loop and dropped the HA WebSocket). *A broader "reconciled" self-healing
  variant was built and then **reverted** in favour of settlement capture as the durable path.*
  Coverage is **versioned** (`_EXC_BACKFILL_SCOPE`): scope 1 = historically-imported blocks
  (`source LIKE 'imported%'`); **scope 2 also fills settled live blocks** (`imp_kwh_api IS NOT NULL`)
  captured before ex-VAT existed and already DCC-settled — settlement capture never re-stamps them,
  so without this they read `≈` forever (see the go-live gap below). Truly-unsettled live blocks are
  left for settlement capture. The completion marker carries the scope, so an instance already
  *done* at a narrower scope **re-arms** once for the newly-covered blocks; a fresh install just runs
  once at the current scope. Sub-meter/`recorder_attributed` blocks stay out of scope (never carried
  captured exc; the ex-VAT bill total is the main import alone).

**2. The read-path fix that made any of this visible.** `get_blocks_lightweight` (the fetch behind
the charts **and** the billing summary) selected `imp_rate`/`imp_cost` but **not** the exc columns —
so the whole ex-VAT view silently ran on `inc ÷ 1.05` even where real exc was captured. It now
surfaces `imp_cost_exc`/`imp_rate_exc`/`standing_charge_exc`/`exc_source`, matching `_row_to_block`.

**3. Banding + VAT amount (mechanism-agnostic).** `_bill_method_breakdown`:
- **bands on the clean stored inc rate**, NOT the per-slot `cost_exc ÷ kWh` (which jitters with
  Octopus's rounding and shattered the two real bands into 0.3075/0.3077/0.3078/…). inc↔exc are
  1:1 (÷(1+VAT)), so grouping on inc is equivalent and always available.
- **labels** each band `inc-key ÷ (1+VAT)` (deterministic, bill-matching), not an effective cost÷kWh.
- **VAT amount = `inc_total − exc_total`** (a subtraction) — exact for any mix of rates, so a
  VAT-holiday boundary *inside* a bill period is handled correctly regardless of how Octopus
  implements the change.

**4. VAT rate — never hardcoded 1.05 (`vat_calendar.py`).**
- **Primary:** the per-slot inc/exc pair (measurement `cost_excl`, tariff `value_exc_vat`) — VAT =
  `inc/exc − 1`, boundary-robust by construction.
- **Fallback / labels (where no pair exists — live tail, inc-only):** the **statutory VAT calendar**
  — seeded (domestic 5% since 1997-09-01) and **self-maintained**: `_learn_vat_from_import_schedule`
  walks the tariff's inc/exc (`RateSchedule.vat_series`), `collapse`s to change-points, and merges
  into `store` (`store_meta` `vat_calendar`) on each rate refresh. So a VAT holiday (Octopus versions
  `valid_from` on the same tariff) is learned automatically. No external API (HMRC's platform is
  transactional MTD, not a rates reference). The four old `1/1.05` sites (slot fallback, side-panel
  total, client `invat`, bill-method default) all resolve the period's rate from the calendar.

**5. Gating (API-only).** The ex-VAT view renders only where **captured exc exists** in the data — a
CAD/cost-sensor-only setup has no ex-VAT source, so it stays hidden rather than showing a bare
`inc ÷ (1+VAT)` assumption. (Proxy for "has API": any block carries `cost_exc`.)

**6. Presentation.** Bill summary: ex-VAT method **replaces** the inc "Import — total grid" section
(per-rate exc → Total exc → Standing exc → VAT row → Total incl. VAT); Total Bill untouched. Data
table: import columns **Rate (exc) / kWh / Cost (exc) / Cost (inc)** at **5dp** (export single,
zero-rated), `≈` on fallback slots; side panel gained **Import cost (exc VAT)**. Usage Stats
deliberately unchanged.

## Gaps / open items

1. **No VAT cross-check/guard yet.** When a data-derived VAT disagrees with the statutory calendar
   (Octopus returning bad/lagged exc during a holiday), we don't surface a flag — data currently
   wins silently. A diagnostic ("derived VAT ≠ statutory for this date") is a nice-to-have.
2. **VAT calendar learns from the CURRENT import tariff only.** A boundary on a *superseded*
   (historical) tariff isn't walked at refresh. Captured `cost_exc` covers settled history anyway,
   and the seed covers the base rate; the only exposed case is an inc-only historical import across
   an *old* boundary. Could walk the agreement history at import time if needed.
3. **Validate inc-only bill/CSV across a VAT boundary.** `bill_parser` emits `rate_pre`/exc and the
   CSV v2 exc columns exist — confirm end-to-end that a PDF/CSV for a 0%-VAT period (the bill states
   0%) actually populates `cost_exc`, so an offline import is right across the boundary.
4. **Per-channel exc under the IOG 6-hour-cap split** (tracked in `iog_6hr_cap_design.md` §D½):
   settlement stamps only the MAIN channel; when the EV sub-meter is billed at its own `ev_device_*`
   rate, capture its exc from that channel's own rate. Build with the ev_device rate support.

**Not gaps (recorded so they don't get re-raised):**
- **Export credit / VAT on export.** Export (SEG/outgoing) is **outside VAT** — customers aren't
  charged VAT on it. It has its own zero-rated section and is netted into the **Total Bill**; the
  ex-VAT block is the *import* total by design. Nothing missing.
- **Settled live blocks captured before ex-VAT existed** — an instance that ran live for a while
  before upgrading has already-DCC-settled `kraken_api` blocks with no captured exc. These do **not**
  self-clear (settlement capture only stamps a block when it *re-settles*, and they settled long ago),
  so the whole go-live→upgrade window read `≈` — observed in prod-dev as "exc approximate for every
  block after go-live", 3,891 blocks over ~6 months. **Now handled** by the scope-2 historical
  backfill above (imported OR settled-live), which reconstructs their exc from the tariff on the next
  start. A clean 4.2 install still captures exc at settlement from the start and has no such backlog.
- **Live/provisional exc is fallback-only, by design.** Unsettled `kraken_api` slots show the
  calendar-VAT fallback (`≈`) until they settle and pick up captured exc.
- **Usage Stats ex-VAT — intentionally not done** (decision): it already ties to the billing summary
  inc-VAT, so an overlay adds complexity for no gain.

## Sequencing & risk
0. ✅ **Groundwork (4.1.3):** the `imp_cost_exc` column + store round-trip (A) and the pure `octopus_bill_total()` ladder + `bankers_round()` (B) — both additive/default-off, inc-VAT byte-identical.
1. **(A) store exc-VAT** at import — the schema column exists; **remaining:** populate it from the already-fetched `cost_excl` across the engine's pricing branches + a re-import backfill. Billing-neutral, but touches billing-critical pricing paths — do with a golden-baseline check.
2. **(B) rounding + display** — the helper exists; **remaining:** the opt-in `bill_rounding` totals mode reading (A), plus the inc/exc display toggle. No store mutation.
Both leave the current inc-VAT bill/charts byte-identical unless a user opts in. Steps 1–2 are the **4.2** feature.