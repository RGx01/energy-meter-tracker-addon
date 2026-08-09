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

## Sequencing & risk
0. ✅ **Groundwork (4.1.3):** the `imp_cost_exc` column + store round-trip (A) and the pure `octopus_bill_total()` ladder + `bankers_round()` (B) — both additive/default-off, inc-VAT byte-identical.
1. **(A) store exc-VAT** at import — the schema column exists; **remaining:** populate it from the already-fetched `cost_excl` across the engine's pricing branches + a re-import backfill. Billing-neutral, but touches billing-critical pricing paths — do with a golden-baseline check.
2. **(B) rounding + display** — the helper exists; **remaining:** the opt-in `bill_rounding` totals mode reading (A), plus the inc/exc display toggle. No store mutation.
Both leave the current inc-VAT bill/charts byte-identical unless a user opts in. Steps 1–2 are the **4.2** feature.