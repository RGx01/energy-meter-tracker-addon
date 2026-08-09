# Bill-accurate rounding + pre-VAT figures — design note

> _Status: **Draft.** Two linked, billing-neutral features: (A) retain the pre-VAT
> (exc-VAT) figures Octopus already sends, and (B) an opt-in "round the Octopus way"
> totals mode that runs on them. Neither mutates stored per-slot data or the inc-VAT
> figures that drive today's bill/charts._

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

**Implementation note:** Python's built-in `round()` is already round-half-to-even,
so no custom rounding is needed.

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
- Setting `bill_rounding: "exact" (default) | "octopus"` in `store_meta` settings (like the carbon assumptions).
- Applied **on-read, at the totals layer only** (daily/period/bill totals) — never mutates stored per-slot values, reversible, no reimport. Default `exact` ⇒ nobody's figures move unless they opt in.
- Core: a pure `octopus_bill_total(blocks)` helper implementing the ladder above, on the **stored exc** (feature A) rather than `inc ÷ 1.05`. Trivially unit-testable.
- **Honest label:** "bill-style rounding (matches Octopus's *method*)", NOT "matches your bill to the penny" — penny parity also needs the same consumption snapshot.
- **Bonus:** an inc/exc **display toggle** — once exc is stored, showing ex-VAT totals is a genuine feature for business / reimbursement users.

## Sequencing & risk
1. **(A) store exc-VAT** at import — small, additive, billing-neutral; the data's already fetched. Gated on go-ahead (touches the import path + a schema column).
2. **(B) rounding + display** — a pure helper + an opt-in totals mode reading (A). No store mutation.
Both leave the current inc-VAT bill/charts byte-identical unless a user opts in.
