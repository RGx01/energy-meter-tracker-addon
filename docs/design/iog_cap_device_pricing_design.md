# IOG 4-rate cap — device pricing across every surface (Model B)

*Design note for the 4.3.1 follow-on to BL-9. Decides how a sub-device's grid import is
priced and shown under the 6-hour cap, in **one pass** across all surfaces, rather than
per-surface display patches.*

## Problem

Under the 4-rate cap the main block's grid import is split (dispatch-derived) into an **EV**
slice at `imp_rate_ev` and a **house** slice at the house-band rate. But **PASS 2 prices
every sub-device's grid import at the parent's blended `imp_rate`** (`_apply_pass2`,
`sub_import["rate"] = parent_rate`). So on a capped block a physical EV charger is priced at
the blend (not the EV peak), a battery/heat-pump is priced at the blend (not the house
rate), and every surface that reads the device's stored rate/cost inherits that: the
day-chart rate **lines**, the day-chart **side panel** (`summary_rates` per meter), the
billing summary **"Breakdown by meter"**, and **Usage Stats / Insights** device cost + rate
tiers. A display-only fix to any one of these is a band-aid and leaves the others (and the
stored cost) inconsistent.

## Decision — Model B: the dispatch split owns the 4-rate bands, everywhere

The dispatch-derived split (`imp_kwh_ev` / `imp_cost_ev` / `imp_rate_ev`, and the house
remainder) is **already the authoritative EV/house billing basis on IOG** and already
reconciles to the settled grid total. Model B makes **every** device figure derive from it,
so line, side panel, breakdown, Usage Stats and the stored cost all agree — capped or
uncapped — with no second pricing method.

Pricing rule (applied at the source, PASS 2, so all surfaces follow):

- **EV row:** kWh = the physical charger's **metered** kWh (or `imp_kwh_ev` when there's no
  physical charger); **cost = `imp_cost_ev`, rate = `imp_rate_ev`** (the dispatch split) when
  the API is configured; else today's parent-rate pricing (no split available).
- **House-band rate** := `(imp_cost − imp_cost_ev) / (imp_kwh − EV_shown_kwh)`, where
  `EV_shown_kwh` is the EV row's kWh above. Falls back to `imp_rate` when there's no EV
  slice (uncapped / non-IOG), so it's identical to today there.
- **Non-EV device** (battery, heat-pump, …) is **house load** → priced at the **house-band
  rate**.
- **House remainder** = grid − EV − non-EV-devices, at the house-band rate (the plug).

### The physical-EV-device question (the crux)

Reconciliation is a hard invariant: **house + devices == grid import, kWh and cost**.
The algebra only closes if the priced EV slice equals the **dispatch** EV kWh, not a
physical charger's metered kWh:

```
Σcost = D_ev·ev_rate + (grid − D_ev)·house_rate
grid_cost = imp_kwh_ev·ev_rate + (grid − imp_kwh_ev)·house_rate
Σcost == grid_cost  ⟺  (D_ev − imp_kwh_ev)·(ev_rate − house_rate) == 0
```

i.e. it reconciles only when `D_ev == imp_kwh_ev` (or uncapped, where the rates are equal).
A physical charger's metered kWh generally ≠ the dispatch kWh, so it **cannot** be both
priced at its own metered kWh on the EV card **and** reconcile.

**Resolution (confirmed):** a configured physical EV charger keeps its **metered kWh** on
the charts (the bar the user recognises), but its **cost and rate come from the dispatch
split** — `imp_cost_ev` and `imp_rate_ev` — whenever the API is configured (that's the only
source of the 4-rate split). So the EV row is: **kWh = metered device, cost = `imp_cost_ev`,
rate = `imp_rate_ev`**. The rate line therefore diverges to peak under the cap, and the cost
is the bill-authoritative EV figure — even though it isn't exactly metered-kWh × rate,
because the metered and dispatch kWh differ (intentional; dispatch is the billing basis).

The house slice absorbs the difference and reconciles on **both** axes:

```
EV row:   kWh = Dev_ev (metered)      cost = imp_cost_ev
house:    kWh = grid_kwh − Dev_ev     cost = grid_cost − imp_cost_ev
house_rate = (grid_cost − imp_cost_ev) / (grid_kwh − Dev_ev)
Σ kWh  = Dev_ev + (grid_kwh − Dev_ev) = grid_kwh    ✓
Σ cost = imp_cost_ev + (grid_cost − imp_cost_ev) = grid_cost   ✓
```

With **no** physical charger the EV row is the synthetic slice itself (`imp_kwh_ev` @
`imp_rate_ev`) — same rule with `Dev_ev = imp_kwh_ev`. With **no API** (no dispatch, e.g.
pure-CAD) there's no split, so the EV device falls back to today's pricing (its metered kWh
at the parent rate) — byte-identical. Non-EV devices carve out of the **house** slice at the
house rate; the house remainder is the plug.

## Surfaces to change in the one pass

1. **PASS 2 pricing** (`engine._apply_pass2`) — price non-EV device grid import at the
   house-band rate; keep the EV as the dispatch slice; house remainder is the plug. This is
   the source; the stored device `imp_rate`/`imp_cost` become correct, so 2–5 mostly follow.
2. **Day-chart rate lines** (`energy_charts.build_day_chart_html`, sub-meter loop) — read
   the corrected stored device rate (no special-casing needed once PASS 2 is right).
3. **Day-chart side panel** (`summary_rates[meter]`) — same source, so per-device rate rows
   fold on the corrected rate.
4. **Billing summary "Breakdown by meter"** — device rows at the corrected rate/cost.
5. **Usage Stats / Insights** (`_aggregate_usage`) — device cost + rate tiers at the
   corrected rate.

Prefer fixing (1) at the source so 2–5 are automatic; only add display logic where a
surface re-derives rather than reads the stored figure.

## Invariants & validation

- **Reconciliation:** house + all devices == grid import (kWh **and** cost) on every block,
  capped or uncapped — the acceptance test.
- **Uncapped / non-IOG byte-identical:** house-band rate == `imp_rate` there, so nothing
  moves.
- **Total Bill unchanged:** this re-attributes *within* the grid import; it never changes
  the grid total or the Total Bill.
- **Settled cost authoritative:** the cap bands are a live estimate; a corrected/settled
  block still governs.

## Implementation reality — two ways to land it

Reading `_apply_pass2` shows the "fix at the source" ideal is entangled with two existing
design points, so there are two viable routes:

**Route 1 — fix PASS 2 pricing (stored data is the source of truth).** Change the device
cost/rate at line ~2189/2220: EV device → `imp_cost_ev`/`imp_rate_ev`; non-EV device →
house-band rate; and the remainder cost becomes the reconciling plug. Downsides: (a) PASS 2
today deliberately computes `cost_remainder = remainder_kwh × rate`, **not**
`main_cost − Σsub_costs`, to avoid absorbing sensor-disagreement (~0.01p/block, note at
`_apply_pass2`) — the plug reintroduces that; (b) with **no physical EV device** the
dispatch EV stays inside the remainder, so the remainder is itself a blend and the house
rate can't be applied to it flatly. So Route 1 is cleanest for the **physical-sub-meter**
case and needs care for the synthetic-only case. It also touches settlement re-runs and
gap-fill (PASS 2 runs on all three paths). Highest correctness, highest risk.

**Route 2 — coherent display-layer derivation (stored billing untouched).** Leave PASS 2
reconciling as today (devices at the parent rate), and have **every display surface**
(chart lines, side panel, billing "Breakdown by meter", Usage Stats) derive the device's
shown rate/cost from the dispatch split — the same unification already done for the
synthetic EV, the bill grid-total, and Usage Stats. Lower risk (no core-billing change,
no reconciliation/settlement exposure), but the split logic lives in each display path
rather than one stored figure.

**Recommendation:** Route 2. The device cost is "indicative" either way (billing is the
dispatch split), the stored figures already reconcile, and the whole point is what the
user *sees* — which is a display concern. Route 1's core-billing change can't be validated
without a real capped multi-device DB, which we don't have. Do Route 2 now (one coherent
pass across the display surfaces); keep Route 1 as a later hardening if a stored-figure
consumer ever needs the banded cost.

## Rejected alternative — Model A (devices stay on the blended rate)

Keep every device on the parent blended rate (today, made explicit). Simpler and it
reconciles, but a physical-EV user — the group most likely on the capped tariff — never
sees the cap divergence on their device chart (the coverage gate hides the synthetic line
for them), even though their bill shows it. Rejected for that blind spot.