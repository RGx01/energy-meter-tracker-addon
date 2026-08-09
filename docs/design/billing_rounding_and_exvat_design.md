# Bill-accurate rounding + pre-VAT figures — design note

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
> `inc ÷ exc − 1` — see "capture completeness".) The **inc-VAT bill total is a derived,
> banker's-rounded view** over those inputs (round kWh→0.01 half-even, × ex-rate, round the
> slot cost→0.01p half-even, sum, × (1+VAT), round to the whole penny) — **not** an
> independently stored number. Store whichever figure Octopus treats as authoritative per
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

## How Octopus rounds a bill (reverse-engineered; not an official spec)
Per half-hour, in order:
1. **Round consumption to 0.01 kWh** using **round-half-to-even** (banker's): 0.015→0.02, 0.025→0.02.
2. Multiply the rounded kWh by the **exc-VAT** unit rate, then **round the cost to 0.01p**, again half-to-even.
3. **Sum** all half-hours, then round the total to the **nearest whole penny**.
VAT (5% domestic) sits on top of the exc-VAT figures; the published/display rate is inc-VAT.

**Implementation note:** use a **Decimal-based** half-even round (`bankers_round()`),
**not** the float built-in `round()`. Python's `round()` *is* round-half-to-even, but
only on the actual binary float — an exact decimal half like `0.015` is stored as
`0.01499…` and rounds to `0.01`, not `0.02`. Rounding `Decimal(str(x))` gives the
decimal the bill intends. (Shipped this way in 4.1.3.)

Sources: guylipman.com/octopus/api_guide.html; octopus.energy/blog/agile-pricing-explained;
energy-stats.uk. Held up against real bills for years, but the exact VAT-application
point and half-even details are inferred.

## Validation (user's raw 26 Jul data, main-only day)
| method | exc-VAT |
|---|---|
| EMT raw 6-dp float sum (current) | −106.94 p |
| **Octopus rounding ladder** (on `inc ÷ 1.05`) | **−108.0 p** |
| Reference calculator | −108.67 p |

The ladder moves EMT toward the bill (−106.94 → −108.0). The residual ~0.7p is **not
rounding**, and no rounding setting can remove it:
- **Consumption differs** — EMT 36.781 kWh vs the calculator's 36.690 (a settlement /
  snapshot difference).
- **VAT basis** — this used `inc ÷ 1.05`; Octopus rounds on the *published exc rate*,
  which isn't exactly `inc ÷ 1.05` (the inc rate is itself a rounded `exc × 1.05`).
Fixing the second is feature (A) below.

## (A) Retain pre-VAT figures — the data is already in hand

> **Status (4.1.3):** the additive nullable **`imp_cost_exc`** column is added (idempotent
> migration) and the store **round-trips** it — written by `_block_rows` + both batch
> INSERTs, surfaced on read by `_row_to_block` as the import channel's `cost_exc`. Verified
> byte-identical: on a full history the column is `NULL` everywhere and `SUM(imp_cost)` is
> unchanged. **Still to do (4.2):** actually *populate* it at import (below) + a re-import
> backfill.

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

> **Status (4.1.3):** the core is shipped and unit-tested but **wired to nothing** — a pure
> `octopus_bill_total(slots)` implementing the ladder, `bankers_round()` (Decimal half-even),
> and `bill_slots_from_blocks()` (uses stored exc when present, else `inc ÷ 1.05`). The live
> billing path is unchanged. **Still to do (4.2):** the opt-in setting + display toggle below.

- Setting `bill_rounding: "exact" (default) | "octopus"` in `store_meta` settings (like the carbon assumptions).
- Applied **on-read, at the totals layer only** (daily/period/bill totals) — never mutates stored per-slot values, reversible, no reimport. Default `exact` ⇒ nobody's figures move unless they opt in.
- Core: a pure `octopus_bill_total(blocks)` helper implementing the ladder above, on the **stored exc** (feature A) rather than `inc ÷ 1.05`. Trivially unit-testable.
- **Honest label:** "bill-style rounding (matches Octopus's *method*)", NOT "matches your bill to the penny" — penny parity also needs the same consumption snapshot.
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

### Still to lock
- **ex-VAT standing charge:** capture it (bills itemise it). → **yes** — required by the
  capture-completeness rule (needed to derive the inc standing view later).
- **ex-VAT unit rate:** store explicitly vs derive on read. → **store explicitly** where it's
  the published primary (offline users can't reconstruct it), derive only where cost is authoritative.
- **VAT rate:** ~~new field to capture~~ → **resolved: derive from the inc/exc pair**
  (`inc ÷ exc − 1`), no stored column, no VAT default. Inc-only/no-API/no-bill blocks get **no
  synthesised ex** — the limitation is accepted and labelled, not papered over. (Nothing to lock.)
- **Column shape:** `imp_cost_exc` exists; add `imp_rate_exc`, `standing_charge_exc`, and a
  provenance tag — **no** `vat_rate` column (derived). Small schema-design call before build.
- Does BL-24 rounding apply to **Billing + Usage Stats** both (avoids the two aggregators
  diverging when the toggle is on) or Billing-only first? — deferred with BL-24.

## Sequencing & risk
0. ✅ **Groundwork (4.1.3):** the `imp_cost_exc` column + store round-trip (A) and the pure `octopus_bill_total()` ladder + `bankers_round()` (B) — both additive/default-off, inc-VAT byte-identical.
1. **(A) store exc-VAT** at import — the schema column exists; **remaining:** populate it from the already-fetched `cost_excl` across the engine's pricing branches + a re-import backfill. Billing-neutral, but touches billing-critical pricing paths — do with a golden-baseline check.
2. **(B) rounding + display** — the helper exists; **remaining:** the opt-in `bill_rounding` totals mode reading (A), plus the inc/exc display toggle. No store mutation.
Both leave the current inc-VAT bill/charts byte-identical unless a user opts in. Steps 1–2 are the **4.2** feature.