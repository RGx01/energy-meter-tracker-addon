# Dispatch validation — over-report guard v2 (design note)
 
**Status:** design note, **not yet built**. Captures the decision to validate
smart-charge slots against the dispatch **lifecycle** (planned → started →
completed) rather than the meter-draw floor alone. Prompted by a real over-credit
found on 2026-06-27. The billing change is held for a later release; a safe
data-capture prerequisite (persist dispatch state + energies) can land ahead of
it.
 
**Cross-references** (`intelligent_rates_design.md`): the over-report floor
rationale; the "RATE IS A FINALISE-TIME CONCERN" decision (§1c); the *Open for
future reconsideration — symmetric settlement re-validation* note (Case B); and
the BCD comparison (§2, §4.10). This note builds directly on those.
 
---
 
## 1. The failure — worked example (2026-06-27, 19:00 UTC = 20:00 BST / 8pm)
 
A single half-hour block, main meter, on an INTELLI-FIX supply (peak 0.32309,
off-peak 0.05493; off-peak window 22:30–04:30):
 
```
2026-06-27T19:00:00 (8pm BST)   main imp=0.1580 kWh @ 0.05493   ← off-peak, WRONG
                                ev   imp=0.1800 kWh             ← NOT charging
                                batt imp=0.0002 kWh
neighbours:  18:30 UTC ev=3.220 (charging)  →  19:00 ev=0.180 (idle)  →  19:30 ev=3.180 (charging)
```
 
The EV **paused** for this one slot in the middle of an evening session. A real
charge is ~2–3.5 kWh per half-hour (see either side); 0.180 kWh is idle. Only
~0.16 kWh of **household baseload** flowed. But a captured `smart-charge` slot
existed for 19:00, and the main grid import (0.158) exceeded the 0.10 kWh
over-report floor, so the overlay repriced the block off-peak. **Octopus billed
it peak** — no smart charge actually occurred that slot.
 
The per-block error is small (0.158 × (0.32309 − 0.05493) ≈ £0.04), but the
**rate is wrong**, and it recurs on every paused slot — so it is systematic, not
a one-off.
 
---
 
## 2. Root cause — the floor measures the wrong quantity, and too low
 
The over-report guard is a `_DISPATCH_OVERLAY_MIN_KWH = 0.10` floor on the
**main meter's grid import**. That import is *car + household baseload*. 0.10 kWh
sits **below normal household baseload**, so a paused-EV slot (kettle, fridge,
standby) clears it and is wrongly credited off-peak.
 
The floor's entire job is to catch "slot marked active but nothing really
charged." It fails here because it cannot separate *EV charging* (~1.5–3.5 kWh
per half-hour) from *EV idle + baseload* (~0.1–0.3 kWh) — they overlap once the
floor is this low.
 
**A battery does not change this**, and this is the key correction to an earlier
worry: the defect is about *baseload*, which every home has. A battery charging
from the grid during a pause would push the number *further* over the floor and
make the false-positive worse, not better. The uneven population is not
battery-vs-no-battery (see §5); it's whether a per-slot charging signal exists.
 
---
 
## 3. Why the meter is the wrong place to look
 
Every meter-derived signal is confounded at exactly the edges that matter:
 
- **Source (solar/battery).** The EV's total draw can be supplied by solar or the
  house battery, so "the EV drew energy" ≠ "a grid smart-charge happened." Grid
  import, EV draw, and EV grid-attribution all diverge from the dispatch.
- **Ramp-down.** A charger ramps its current down across a slot boundary, so the
  per-slot *meter* energy doesn't line up with the dispatch's own accounting.
So raising the floor or switching it to EV draw both stay noisy. The signal that
is **not** confounded by source or timing is Octopus's own record of what it
dispatched — the dispatch's `energyAddedKwh`. If Octopus says it added ~0 kWh in
the 19:00 slot, that is true regardless of solar, battery, baseload, or ramp.
 
---
 
## 4. The unconfounded signal — the dispatch lifecycle
 
A dispatch moves through **planned → started → completed**:
 
- **planned** — forecast / intent. *Weak.* This is what EMT captures today, and
  it was wrong on the 27th (the plan said charging; the car paused).
- **started** — Octopus's confirmation that a dispatch **is actually running**
  this slot. This is BCD's gate: a planned dispatch becomes off-peak only once it
  is a *started* dispatch (thirty minutes at a time, and only while the account's
  intelligent state is `SMART_CONTROL_IN_PROGRESS`). On the 27th there would have
  been **no started dispatch** at 19:00 → a started-gated overlay would not have
  fired → **caught at finalise**, without waiting for settlement.
- **completed** — after-the-fact truth, with real `energyAddedKwh`. The
  **settlement-time** backstop that reconciles anything `started` got wrong.
`started` is the signal that is both *real* (unlike planned) and *available at
finalise* (unlike completed) — which is why it is the primary fix, not merely an
extra.
 
---
 
## 5. Three populations — the fix is uneven **by device type**, not by battery
 
| Population | Signals available | Primary gate |
|---|---|---|
| **Charge-point** (Zappi / MYENERGI_V2) | EV sub-meter draw **+** dispatch state **+** energy | any of the three; sub-meter draw is a strong direct check |
| **Vehicle integration** (Tesla, Ford, …) | dispatch state **+** energy; **no EV sub-meter** (car charges from a dumb charger / socket) | the **dispatch lifecycle** — it is the *only* per-slot truth about whether the car charged |
| **No device / api-only** | none per-slot | the **floor** — must be raised off baseload |
 
Two consequences:
 
- The meter-side "gate on EV-charging draw" option only exists for **charge-point**
  setups. A vehicle-integration user has no charger sub-meter — the EV charge just
  appears inside the main import, indistinguishable from house load. So for them
  the dispatch lifecycle is not a nicety; it is the only signal. This is the
  strongest argument for building on the dispatch record rather than the meter.
- The **floor is the last resort** for the no-signal population, and at 0.10 kWh
  it is the actual defect for them. It should be **raised off baseload** and kept
  **under-application-biased** (fail toward peak, never over-credit) — consistent
  with the guard's original safety principle.
**OHME stays carved out.** For OHME, Octopus does *not* control the charge, so its
dispatch labels (and likely its `energyAddedKwh`) are as unreliable as its
`source`. OHME keeps its charge-mode-sensor gate + floor; the energy/started veto
must never fire against the OHME path and silently un-credit its optimistic
captures. Vehicle integrations are **Octopus-controlled** (same side as Zappi),
so their labels are trustworthy — no carve-out.
 
---
 
## 6. Capture-schema change (safe, no billing effect — land this first)
 
Today `dispatch_slots` stores only: `slot_start, off_peak (bool), provider,
source, captured_at`. We already **fetch** planned *and* completed dispatches
(they appear in the `dispatch-observe` logs) and then **discard** everything but
the boolean marker.
 
Persist, per slot:
 
- **state** — `planned` / `started` / `completed`
- **energy_planned** — `energyAddedKwh` from the planned dispatch (forecast)
- **energy_completed** — delivered energy from the completed dispatch (truth),
  attributed per slot (see Q4 on granularity)
Derive **started** the BCD way: a planned dispatch whose window is current, gated
on the account's `SMART_CONTROL_IN_PROGRESS` state.
 
This is a **data-capture change with no billing effect** — low risk, and the
prerequisite for everything in §7. It also makes the dispatch history
*diagnosable*: the 27th question ("was 19:00 planned-but-never-started?") would
have been answered directly from the DB instead of inferred from the meter.
 
---
 
## 7. Billing change (later release)
 
- **Finalise-time:** started-gate the overlay. Apply off-peak only if the slot is
  a **started** dispatch, not merely planned. (Catches the 27th pause live.)
- **Settlement-time:** completed-energy veto. If the completed dispatch delivered
  ~0 energy in the slot, revert off-peak → peak.
- The completed veto **is Case B** (the off-peak→peak settlement reversion parked
  in `intelligent_rates_design.md`). They un-park **together** — one mechanism.
- **Floor stays underneath**, raised off baseload, under-application-biased, so
  the OHME safety property and the "never over-credit" principle both hold.
---
 
## 8. Open verification questions (confirm against live accounts before billing)
 
- **Q1 — literal vs derived `started`.** Does the flex API expose a `started`
  state, or must it be derived (current-window planned + `SMART_CONTROL_IN_PROGRESS`,
  BCD-style)? Assume derived until confirmed.
- **Q2 — `energyAddedKwh` on the vehicle path.** On a charge-point it's measured;
  on a **vehicle** it's derived from the car's reported SoC/energy — coarser,
  laggier, and possibly `null` for some makes. If it's null, an energy veto has
  nothing to bite on → **fallback:** gate on *started-presence* only (not energy),
  with the floor as backstop.
- **Q3 — `SMART_CONTROL_IN_PROGRESS` on the vehicle path.** The state is
  account-level and *should* apply to a vehicle account identically, but confirm
  before trusting it as the finalise-time gate; if absent, `started` can't be
  derived for vehicles → drop to planned (weak) + completed (settlement).
- **Q4 — completed-dispatch granularity.** Completed dispatches come as **ranges**
  (`start`/`end` + a total `delta`), not per-half-hour. A pause that splits the
  session into two ranges leaves a clean coverage gap at the pause → veto fires.
  But a single range *spanning* the pause looks "covered" with ~0 energy in the
  pause slot → a coverage-only check misses it. Need per-slot energy (or
  coverage + energy), not coverage alone.
---
 
## 9. Decision / recommendation
 
1. **Land the capture change (§6) now** — safe, forward-looking, stops discarding
   the evidence, and makes the dispatch history diagnosable.
2. **Hold the billing change (§7)** for a later release — it rides Case B and
   needs Q1–Q4 answered against live charge-point *and* vehicle accounts.
3. **Consider a near-term floor raise** for the no-signal population as a smaller
   standalone mitigation (0.10 → ~0.5–1 kWh, under-application-biased), independent
   of the lifecycle work, since 0.10 kWh below baseload is the concrete defect the
   27th exposed.
 






















