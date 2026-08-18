# Priced-segment pricing model — refactor design (BL-27)

*Strategic follow-on to BL-9. Captures why the IOG 4-rate work keeps producing the same
class of bug, and the one structural change that removes it. Not a point-release item —
this is a deliberate refactor of the pricing layer, to be done with a capped multi-device
DB to validate against.*

## Why this exists — the recurring bug

A block's grid import is priced as a **single rate** (`imp_rate`/`imp_cost`), and the IOG
4-rate work bolted **additional decompositions** on top of it, each stored separately:

- the **EV/house split** — `imp_kwh_ev` / `imp_cost_ev` / `imp_rate_ev`;
- the **ex-VAT** figures — `imp_rate_exc` / `imp_cost_exc`;
- **device attribution** (PASS 2) — each sub-meter priced at the parent rate.

These are all *derived from the rate but stored beside it*, so **every path that changes
the rate has to remember to update all of them.** It keeps not happening:

1. **Ex-VAT drift** — the reconcile revert rewrote `imp_rate` but not `imp_rate_exc`
   (fixed: reconcile exc re-stamp + `repair_stale_exc`).
2. **EV-split drift** — the same revert rewrote `imp_rate` but not `imp_rate_ev`, so a
   negligible smart-charge reverted off-peak→peak left the EV frozen off-peak and the home
   remainder derived a phantom rate *above the tariff peak* (fixed: reconcile split
   re-stamp + `repair_stale_iog_split`).
3. **Device pricing** — under the cap every device is priced at the parent's *blended*
   rate, wrong for both EV (should be the EV band) and house devices (should be the house
   band); the display surfaces re-derive the split independently and can disagree with the
   bill.

Three instances, one root cause: **two things that must agree and sometimes don't.** Each
fix is easy; the *pattern* is the cost.

## The model — one priced decomposition per block

Replace "one rate + bolted-on splits" with a **small ordered set of priced segments** that
*is* the block's pricing. Each segment carries everything, computed together:

```
segment = { kwh, inc_rate, exc_rate, band, attribution }
    band        : off_peak | day | peak | …         (OPEN label — the rate-matrix cell)
    attribution : house | ev | …                    (OPEN label — who the energy is for)
```

`band` and `attribution` are **open label sets, not closed enums** — see Extensibility; the
storage and every surface treat them as opaque strings, and only the classifier assigns
meaning. `exc_rate` is **per-segment**, so VAT is a property of the slice, not a global
÷1.05 assumption.

A normal block is one segment; a boundary/cap block is two; the 4-rate matrix is at most
four. Everything else is a **view** over the segments:

- `imp_cost` = Σ segment.kwh × segment.inc_rate; `imp_rate` = imp_cost / imp_kwh (the
  blended figure, for anything that still wants one).
- EV/house split = filter by `attribution`; ex-VAT = the segments' `exc_rate`. No separate
  stored columns to drift — they're **projections of one source**.
- A device inherits the segment(s) its grid-clipped energy lands in — so a battery is house
  segments, an EV charger is EV segments, priced correctly with no special-casing.

A reprice (settlement, reconcile, overlay) **recomputes the segments**. There is nothing
left to keep in sync, so the entire bug class in "Why this exists" cannot recur.

## What it removes

- The `imp_*_ev` and `imp_*_exc` **stored columns become derived** (or a thin compatibility
  view), so `repair_stale_exc` / `repair_stale_iog_split` / the reconcile re-stamps are no
  longer needed — nothing to repair.
- The **per-surface re-derivation** (chart line, side panel, billing breakdown, Usage
  Stats each computing the split) collapses to "read the segments," so surfaces can't
  disagree with the bill.
- **Device pricing** stops being a special case — inherit the segment.

## What it does NOT remove (be honest)

- **Settlement-rounding jitter** (0.3230/0.3231/0.3232 from Octopus's per-half-hour
  rounding) is real data; segments still get folded for display. But that fix is stable.
- **Display formatting** (transition rows, ex-VAT toggle, band labels) — still presentation,
  just driven cleanly off segments; not a source of churn.

The distinction: today's bugs come from *two representations that must agree*; after this
there is *one*, so there is nothing to disagree.

## Extensibility — the point of the model

The recurring question is what happens when Octopus **extends the rate matrix** — say a
home-battery grid-charging rate, more time bands, or a heat-pump tariff.

**In today's model that is expensive.** The split is hard-coded as EV-specific columns
(`imp_kwh_ev`/`imp_cost_ev`/`imp_rate_ev`). A second priced dimension means new columns
(`imp_*_batt`), new re-stamp logic on *every* reprice path, and new derivation on *every*
surface — i.e. it replays all three whack-a-mole bug classes for the new dimension.

**In the segment model it is cheap, because a segment is generic.** A new dimension is a new
`band` and/or `attribution` **label** that the classifier emits. Storage (a list of
segments), reprice (recompute the segments), and the surfaces (read segments, group by
band/attribution) are already generic over the set, so **none of them change**. The marginal
cost is *one place* — the classifier — not N.

The irreducible caveat: **the classifier still has to learn the new rule.** No model makes a
new tariff rule free; someone must encode "battery grid-charging in window X → rate Y". The
win is that the rule is **local** to the classifier instead of smeared across columns +
reprice paths + surfaces. The thing that grows is the one thing that should.

Two requirements to actually get this, cheap on day one and expensive to retrofit:

- **Open labels + a pluggable classifier.** `band`/`attribution` must be opaque strings the
  storage and surfaces never switch on; only the classifier interprets them. Bake
  `attribution ∈ {house, ev}` as a closed enum and we re-introduce the rigidity we're
  removing.
- **Per-segment `exc_rate`.** VAT becomes a property of the slice, so a VAT holiday or a
  per-band VAT quirk is absorbed with no special-casing.

Scope boundary (honest): segments decompose a block's grid **import** into priced slices.
Extensions that fit that shape — more bands, a battery/heat-pump attribution, a device
inheriting its segment — drop in via the classifier. Extensions that don't — a **V2G/export**
rate (segments would need to live on the export channel too), a **monthly-tiered** rate (a
cross-block accumulator, like the cap's noon→noon day), or a fixed **battery levy**
(standing-charge-like, not a per-kWh slice) — need the model's *scope* widened, not just a
label. Those are bigger, but a block-of-priced-segments is a far better foundation for them
than per-dimension columns.

## Migration (phased, additive)

1. **Introduce segments alongside** the current columns — compute them at the single
   pricing point (PASS 2 / the seam), store as a JSON column or child rows. Derive the
   existing `imp_*` columns *from* the segments (so nothing downstream changes yet).
2. **Backfill** segments for history from the existing columns (one-off, like the split
   backfill).
3. **Migrate readers** one surface at a time to read segments instead of the derived
   columns (bill, charts, Usage Stats, PASS 2 device pricing).
4. **Retire** the now-redundant re-stamp/repair machinery and, eventually, the derived
   columns (or keep them as a read view).

Each phase is additive and billing-neutral; the Total Bill (Σ segments = grid cost) is
invariant throughout.

## Scope, risk, validation

- Touches the **pricing core** (the single point where a block's cost is decided) and PASS 2
  device attribution — the highest-value, highest-care code.
- **Validate against a real capped multi-device DB** (EV charger + battery on the 4-rate
  cap) — the case we cannot currently reproduce and the reason the tactical fixes have
  leaned on unit tests.
- Ship behind the existing "additive, off for non-IOG, inc-VAT byte-identical" guarantees;
  the reconciliation invariant (house + devices == grid, kWh and cost) is the acceptance
  test at every phase.

## Status

Tactical fixes shipped (4.3.0/4.3.1): ex-VAT and EV-split re-stamp + repair, display
unification. Those keep users correct today. This refactor is the **strategic** cure and
should be scheduled deliberately, not forced into a point release.