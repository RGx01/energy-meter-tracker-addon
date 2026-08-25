# Dispatch validation — over-report guard v2 (design note)

> _Status: Shipped — design note kept for rationale; the code is the source of truth._

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

---

## 10. Update — validated against a live IOG charge (2026-07-07)

A full Intelligent Octopus overnight+evening charge (Myenergi/Zappi) was captured
and analysed. It confirms the approach and sharpens several decisions.

### 10.1 What the capture proved
- The 3.1.1 planned-lifecycle capture works on real data: recent slots carry
  `state='planned'` + `energy_planned`.
- **`energy_planned` is signed NEGATIVE** — Octopus books a dispatched charge as
  negative kWh (e.g. −3.35 = a ~3.35 kWh planned charge). Completed dispatch
  `delta` is negative too. **Every comparison must key on magnitude**, or the
  logic inverts. (Caught before the billing change — good.)
- The killer case is present: 07-06 **21:30** — Octopus *planned* −3.35 kWh, the
  car drew **0.000**, and EMT correctly billed **peak** (the floor caught the zero
  draw). Where it executed (17:30/19:30, draw ≈ planned magnitude) it went
  off-peak. So planned-vs-actual is *a* signal — but see 10.2.

### 10.2 Meter-side validation is ruled out as a universal rule
Two confounds, both real on this account, break any grid-meter check:
- **No charger sensor** → there's no EV-specific draw at all (this DB's
  `ev_charger` reads 0.000); only whole-house main import exists.
- **Solar / battery** → during an active dispatch the car can pull 7 kW while
  *grid import ≈ 0* (sun or battery supplying it). A draw-based check would then
  read "didn't charge" and wrongly strip the off-peak. And the reverse: a 0.000
  main could be a fully self-powered charge, not a no-charge.

A dispatch is about *energy delivered to the car*; the grid meter only sees the
*residual after solar and battery*. So the meter cannot answer "did this dispatch
charge" on solar/battery sites. **The meter is demoted to a weak backstop, never
the validator.**

### 10.3 Confidence ladder — best signal each user actually has
One rule can't fit all; accuracy should scale with the sensors present.
Per slot, for **Octopus-controlled** providers (Zappi/Myenergi, vehicle
integrations):

1. **Charger sensor configured** → validate against the *device's* draw. A CT on
   the charger sees energy-to-car directly — immune to the solar/battery confound.
   Highest accuracy; real-time.
2. **No charger sensor** → **`energy_completed`** from the completed dispatch —
   Octopus's own energy-to-car figure, also immune to solar/battery (not
   grid-derived). Primary for the no-CT crowd.
3. **Neither** → standard off-peak window only; never trust an evening dispatch.
   The meter floor stays as a last-ditch backstop (raised off baseload).

Tiers 1 and 2 **corroborate** — charger draw and completed energy should roughly
agree; a mismatch (charger says 3 kWh, completed says 0) is itself a red flag.
`energy_completed` is needed either way (tier-2 backbone AND tier-1 corroborator),
so capturing it is the prerequisite for the whole ladder.

### 10.4 OHME — its own branch, exempt from the completed veto
Octopus does **not** control an OHME charge (OHME self-schedules and reports), so
its dispatch records are unreliable for smart-vs-boost — completed dispatches come
back `source='unknown'`, "losing the smart signal." OHME therefore **must be
exempt from the completed-energy veto**: a genuine off-peak OHME charge whose
completed record Octopus didn't populate would be wrongly reverted to peak.
OHME's ladder is separate:
- **OHME + charge-mode sensor** (official `ohme` select / dan-r binary) →
  *verified* smart-vs-boost. Better than energy — it answers the exact question.
- **OHME, no charge-mode sensor** → *optimistic* off-peak, unverifiable (today).
- A CT on OHME tells "did it draw," not "smart vs boost" (a boost draws too) —
  a weak corroborator only.
OHME is fenced off **by provider** at veto time, as it already is at capture time
(`_capture_ohme_slots`).

### 10.5 Completed dispatches — source, latency, and capture (SHIPPED observe-only)
- **Source:** already fetched — `completedDispatches { start end delta meta{...} }`;
  `delta` (energy) is normalised and kept. No new query.
- **Latency:** ~hours, same-day (a 17:30 dispatch was in the list by ~22:00), far
  faster than DCC's 1–2 days — but NOT real-time, so it validates on a
  **settlement-time reconciliation pass**, not at block finalise. That is the
  Case-B reversion shape (provisional off-peak → corrected to peak when completed
  shows ~0). Consistent with §7.
- **Shipped now (observe-only):** `_capture_dispatch_slots` records
  `energy_completed` + `state='completed'` for Octopus-controlled providers,
  **annotating only slots that already exist** (never creating a new `off_peak`
  slot from a completed dispatch — so it cannot change what the overlay prices).
  OHME excluded. No billing effect; the overlay still reads `off_peak` only.

### 10.6 `started` — deprioritised
There is **no literal started/in-progress dispatch endpoint** — only
`flexPlannedDispatches` and `completedDispatches`. "Started" is derive-only
(a planned dispatch in the current window + the account `SMART_CONTROL_IN_PROGRESS`
state, which we don't query). Since completed lands within hours, the
completed-based settlement veto covers the same ground; `started` is now a
"nice-to-have finalise-time tightening" for later, **not a blocker**.

### 10.7 Next
1. **Collect** completed-energy data across a few charges (now that capture is
   live) to confirm latency and that `delta` tracks the real charge.
2. **Ask the user** to compare the evening slots that charged (17:30/19:30/23:30)
   against the real bill — off-peak (smart) or peak (bump)? Ground truth for
   whether the overlay is already right on evening dispatches.
3. **Then** build the settlement-time validation (tier ladder above), magnitude-
   aware, OHME-exempt — the 3.2.0 billing change.

---

## 11. Correction — `started` is the discriminator, not `completed` (2026-07-07, from BCD)

Reading BCD's own approach (docs + FAQ) corrected two assumptions in §10. This
section supersedes §10's completed-as-validator framing and §10.6's
deprioritisation of `started`.

### 11.1 `completed` CANNOT distinguish smart from bump
BCD's FAQ is explicit: completed dispatches carry no source, so **boost (bump)
charges are mixed in with smart charges**, and *"Completed dispatches cannot be
used for this [rate adjustment] as OE do not provide information on the cause of
the completed dispatch."* A bump **charges and completes identically** to a smart
charge. So §10's "completed present + energy ≈ planned → confirmed smart →
off-peak" is **wrong**. What completed CAN do is confirm a planned slot actually
drew — it catches the PAUSED case (the original 27-June #253, EV didn't charge).
It is a **flow corroborator, not a smart-vs-bump oracle.**

### 11.2 `started` is the real signal — derived, not fetched
BCD adjusts rates off **started** dispatches. The derivation (to copy verbatim):
- On each refresh, **if the account/device intelligent state is
  `SMART_CONTROL_IN_PROGRESS`**, any PLANNED dispatch whose window is *currently
  active* promotes the **current 30-minute slot** to a *started* dispatch.
- Done **30 minutes at a time** (not the whole planned window) because Octopus
  can stop a dispatch early; the **whole 30-min slot is off-peak** even if only a
  minute charged.
- The planned dispatch must **still be present** at slot-start (planned can be
  pulled at the last minute — accepting bare "was planned" over-applies).
- A **bump never puts that slot into `SMART_CONTROL_IN_PROGRESS`**, so it never
  becomes started — which is exactly why started discriminates smart from bump
  and BCD's dispatching sensor *"will not come on during a bump charge."*

So: `started = planned ∩ active-now ∩ SMART_CONTROL_IN_PROGRESS`, accumulated slot
by slot. Needs a NEW fetch we don't do today — the **intelligent state**. (BCD's
logic lives in `custom_components/octopus_energy/intelligent/dispatching.py`.)

### 11.3 Local accumulation is a prerequisite (why BCD "had" 17:30)
BCD **accumulates dispatch history locally** (`intelligent_dispatches_history_*`,
~48h, "stored locally as it changes") over frequent polls — it never depends on a
single snapshot. Our capture reads one `completedDispatches` / `flexPlanned`
response per poll and annotates only what's in it, so a dispatch present only
transiently (or in a poll window we miss) is lost. That — **accumulation vs
snapshot**, not latency — is why BCD had 17:30 / 20:00 / 05:00 and we didn't. **We
cannot trust "absence" for any veto until we accumulate like BCD.**

### 11.4 Revised model
- **Off-peak eligibility** = **started** (planned that became active under
  `SMART_CONTROL_IN_PROGRESS`), accumulated. The smart-vs-bump discriminator;
  replaces "planned + floor" for accuracy.
- **completed** = corroborates energy flowed (catches paused slots); never used
  to prove smart. (Capture already shipped, 3.1.4.)
- **charger CT** where present = corroborates energy-to-car, immune to
  solar/battery.
- **OHME** = unchanged carve-out. OHME provides **no planned dispatches at all**
  (its billing runs off completed "in reverse"), so it can't use `started`
  either — keeps charge-mode-sensor / optimistic.

### 11.5 Revised build order (supersedes §10.7 / §7 sequencing)
1. **Local accumulation** of planned + completed (persist as seen; stop relying
   on per-poll snapshots) — prerequisite for everything, including trusting
   absence. **[DONE — 3.1.4: `dispatch_history` table, observe-only.]**
2. **Fetch intelligent state + derive `started`** (planned ∩ active ∩
   `SMART_CONTROL_IN_PROGRESS`, 30-min increments, planned-still-present),
   observe-only. **[DONE — 3.1.4: `get_intelligent_state` (isolated fetch,
   `status{currentState}`) + `_derive_started_slots`, accumulated as
   `kind='started'`. Field path unverified against a live charge — see below.]**
3. **completed** stays as the flow corroborator (done).
4. **Then** the billing change: off-peak driven by **started** (not raw planned),
   completed / CT as corroboration, settled-gate + Case-B reversion,
   respect user overrides, OHME-exempt.
5. `started` is **no longer deprioritised** — it is the core signal. §10.6 is
   withdrawn.

### 11.6 Still unproven
The bump case (a charge that never becomes started) still wants one real example
to confirm the guarded logic behaves — best obtained by a **deliberate evening
boost** during an observe-only window, not by waiting for one to occur naturally.

---

## 12. VALIDATED against a live solar-supplemented smart charge (2026-07-07)

A full Zappi smart charge was captured on 3.1.4 with the `started` derivation
live. It validates the model end-to-end, including the confound that defeated the
meter.

Three-way `dispatch_history` vs grid import (UTC):

| slot  | planned | started | completed | grid_import |
|-------|---------|---------|-----------|-------------|
| 11:00 | −3.24   | YES     | −3.18     | 3.387       |
| 11:30..12:30 | −3.35 | YES | ≈−3.46 | ≈3.67   |
| 13:00 | −3.35   | YES     | −2.45     | 1.458  (solar ramping) |
| **13:30** | **−0.95** | **YES** | **−0.81** | **0.002  ← solar-supplied** |
| 14:00 | −3.35   | –       | 0.000     | 0.000  (planned, never charged) |

Findings:
1. **Field path confirmed.** `get_intelligent_state` returns real values
   (`SMART_CONTROL_OFF` → `SMART_CONTROL_IN_PROGRESS`; also a new `SMART_CONTROL_OFF`
   — treat only `IN_PROGRESS` as started, which we do). `_derive_started_slots`
   fired for every in-progress slot.
2. **Solar immunity proven (13:30).** Grid import 0.002 kWh, yet started=YES and
   completed=−0.81. A meter/floor check would have wrongly stripped off-peak here;
   started+completed (energy-to-car) correctly confirm the charge. This is the
   §3 confound, resolved.
3. **Negative case proven (14:00).** Planned but no started, no completed, no draw
   → charge ended → peak. Unambiguous.
4. **Hardest case handled.** This account has NO usable EV energy sensor (Zappi CT
   is power-only) AND solar — the tier-2-only situation — and the lifecycle signal
   nailed every slot regardless.

**Resulting billing rule (validated):** off-peak **iff the slot `started`**;
not-started → peak, regardless of planned/completed. One rule covers paused
(no started), bump (completes but never starts), and solar (grid≈0 but started).
The meter is not consulted. `completed` remains a flow corroborator; `planned`
alone is not sufficient (14:00 was planned but not smart-charged).

Open: still no *directly observed* bump (completed-without-started) — the absence
logic is validated on 14:00, and a deliberate evening boost would confirm the
bump branch specifically. Missed-poll robustness (a genuine smart slot polled zero
times) errs to peak (conservative/under-credit) — the settlement pass can use
`completed` presence to flag such slots for review rather than silently under-credit.

---

## 13. BILLING CHANGE SHIPPED — settlement reconciliation (3.1.4)

The overlay change is built and writing (risk accepted; soaked on prod_dev before
promotion). Implements the §12 rule.

- **`_reconcile_decision(started, completed, currently_off_peak)`** — pure:
  started → off_peak; neither → peak; completed-without-started → review; else ok.
- **`reconcile_dispatch_overlay()`** — settlement pass on the engine loop, hourly
  (via `_tick_dispatch_capture`), gated `block_start < now − 6h` so started
  (real-time) and completed (~hrs) have settled. Bidirectional reprice of main +
  devices-follow.
- **Safety gates:**
  1. **Accumulation gate** — only slots with a `dispatch_history` `planned` entry
     are candidates. Pre-3.1.4 off-peak slots (no history) are NEVER reverted just
     because they lack started/completed we weren't recording yet. (This was a
     real bug caught in test: without the gate, 106 historical slots reverted.)
  2. **Correction gate** — `rate_corrected=1` blocks (set by the corrections tool)
     are skipped; the pass never stomps a manual override.
  3. **review** (completed-without-started) is logged, never auto-changed.
- **Loop-safe:** runs on the engine loop (single store connection); regenerates
  charts inline on that thread only when something changed.
- **Config:** `_DISPATCH_RECONCILE_APPLY` (True) and `_RECONCILE_SETTLE_HOURS` (6).
- **Split gate (restore vs revert):** a RESTORE (started present) fires as soon as
  the slot has ended (`_RECONCILE_STARTED_GATE_MIN`, 40 min) — `started` is
  real-time, so no wait is needed. A REVERT/REVIEW (no started) is DEFERRED until
  the slot clears `_RECONCILE_SETTLE_HOURS` (6h), because distinguishing
  neither→peak from completed→review needs `completed` to have settled. So the
  solar-slot under-credit is corrected promptly; only the over-credit revert waits.
- **PASS 2 stomp-guard:** reconcile stamps `rate_reconciled=1`; `_rerun_pass2_for_settled_block` preserves any `rate_corrected`/`rate_reconciled` rate (re-costs to settled kWh but does NOT re-resolve the overlay), so a re-settlement can't undo a reconcile or a manual correction. Closes the ordering gap single-threading alone didn't cover.

Validated on the real solar charge: restored 13:30 (grid 0.002, started) to
off-peak, left 11:00–13:00 (already off-peak) and 14:00 (planned-only, peak)
unchanged, touched zero historical slots.

**Still to observe on prod_dev:** a genuine bump (completed-without-started) hitting
the `review` branch, and confirmation the reconciliation log reads correctly
against the real Octopus bill across several charges before promotion to main.


---

## 14. FIRST OBSERVED BUMP — the completed-only ambiguity resolved (2026-08-23, prod)

The long-open item (§12 *"still no directly observed bump"*; §13 *"Still to observe: a genuine
bump"*) is **closed**. On **2026-08-23** a manual **Zappi Fast** bump during a Free Electricity
hour (11:00–12:00 BST = 10:00 & 10:30 UTC, 5.72 kWh) was captured on prod. It exercised the
`completed-only → off-peak` branch and mispriced both blocks off-peak (£0.05493) where they
should read **peak** (£0.32309) — Octopus bills a bump at peak and credits the free hour to the
account balance *separately*.

**What the capture confirmed:**
- **`started`-capture works.** The bump slots are `planned=0, started=0, completed=1`; the same
  night's real smart charges (03:00/03:30/04:30) are `planned=1, started=1, completed=1`. So
  `started` correctly did **not** fire for the bump — §11.2 holds.
- **The mislabel was the reconcile, not capture.** Both blocks carry `rate_reconciled=1`: the
  settlement pass *promoted* them off-peak via the completed-only branch.

**The ambiguity the bump exposes.** §11.1 established `completed` cannot tell smart from bump.
The reconcile's completed-only branch (§13; 4.4.0 pricing §3c) resolved that by assuming *no plan
captured ⇒ EMT was offline ⇒ a smart charge we missed ⇒ off-peak*, explicitly accepting it "can
over-credit a genuine offline bump." But a bump is completed-only **whether or not EMT was
online** — Octopus never plans a bump. So `has_planned` is **not** a proxy for "were we online";
it conflates two distinct situations:

- **Offline / re-import** — EMT was down, so it never saw the plan of a *genuine smart charge*.
  Optimistic off-peak is right.
- **Online bump** — EMT was up and polling, and saw no plan because *there was none*. This is a
  bump; off-peak is wrong.

### 14a. The online-gate — `interpolated` distinguishes bump from missed-smart

EMT already records its own downtime, so the two cases separate **without any new signal**:

- A block EMT built live from real-time reads is `interpolated=0`, `source` NULL.
- A block reconstructed after downtime (gap-fill) is `interpolated=1`; API/CSV backfill is
  `source LIKE 'imported%'` (the reconcile already excludes `imported%`).

So **a live block (`interpolated=0`, not imported) ⇒ EMT was online and polling dispatches for
that slot.** A genuine smart charge is always *planned* (and, with permanent dispatch retention —
§3d / pricing-doc, BL-35 — accumulated across polls), so it would have been caught; the only
completed-only dispatch that appears *while EMT was watching* is an **out-of-app bump**. The
reconcile decision (4.5.0):

| completed-only slot | `was_online` (`interpolated=0`) | decision |
|---|---|---|
| live block | **True** | **peak** — out-of-app bump (confident; not `review`) |
| gap / interpolated / imported | False | off-peak — genuine missed smart charge (§14/offline) |

Implemented as `_reconcile_decision(..., was_online = not interpolated)`; the completed-only
branch splits on `was_online`. **Confident enough to auto-revert** (not `review`) because a bump
is unplanned *by construction*; an already-peak live bump is a no-op, so no spurious review
flags. This fixes the observed Zappi case **and** the pure car-side **EV-integration** case,
where no charge-mode sensor can ever exist — the mode sensors (Zappi Fast / Ohme Max / Hypervolt
Boost) are optional confirmation only.

**Scope — provisional/estimate only.** The reconcile promotes only provisional blocks; a settled
block takes Octopus's authoritative API cost (peak for a bump). Prod confirms it: every *settled*
completed-only orphan (14–22 Aug) is priced peak (`rate_reconciled=0`); only the *unsettled* 23rd
was promoted. **Settlement remains the final authority**; the gate only corrects the
pre-settlement estimate.

**Contemporaneity guard (4.5.0 fix).** Observed on prod_dev (12 Aug): a genuine evening smart
charge lost its `planned`/`started` when the DB was **rebuilt** — those accumulate locally and
Octopus serves neither historically — leaving completed-only, which the gate mis-flagged as a
bump and reverted to peak. Fix: the bump verdict now also requires the `completed` dispatch to
have been **first seen contemporaneously** (within 36h of the slot). A rebuild/re-import re-fetches
`completed` days later, so `first_seen ≫ slot_start` → absence-of-`started` is **lost accumulation,
not a bump** → the block keeps its optimistic off-peak (§11.3). This closes the common real-world
case.

**Residual edge (BL-37).** What the guard does *not* catch is the narrower live case: a block
`interpolated=0`, the completed seen contemporaneously, yet the *dispatch poller specifically*
missed heartbeats over that slot (meter/HA up, Kraken poll down) so a genuine smart charge's plan
was never captured. That still prices peak and settlement corrects it — safe but slightly
pessimistic. Closing it precisely wants a lightweight **dispatch-poll heartbeat log** so reconcile
can see the poller had a gap and stay optimistic only then. Deferred: Kraken polling is dependable
and settlement backstops it.


---

## 15. OHME plan-from-charger — read Ohme's own schedule (4.5.0, UNVALIDATED — BL-31)

For a **charger-connected** Ohme IOG setup, Octopus does not author the plan — **Ohme does**, optimising to Octopus's tariff/target — so Octopus exposes no reliable `planned`/`started`, only completed-in-reverse (§11.4, §14a). The plan lives in Ohme's cloud, and its HA integration surfaces it: the **official** `sensor.<ohme>_slot_list` state is ``"HH:MM-HH:MM, ..."`` (local, merged half-hour slots — `ohmepy` `ChargeSlot.__str__`); the **dan-r** integration exposes the same as Planned Slots / Charge Slot Active with ISO datetimes.

4.5.0 reads it: `_parse_ohme_slots` parses either shape into naive-UTC 30-min slot starts (HH:MM anchored to the nearest local date ±12h, midnight-safe, DST-aware; ISO used directly where present). Those are written as `dispatch_history` `planned` (`source='ohme_plan'`) in place of Octopus's unreliable superset, and a **sensor-verified** smart slot also records `started`. Effects: (a) the smart-charging card shows Ohme's **real forward plan**, not the superset (the "pointless card" for charger-connected Ohme users); (b) a verified OHME smart charge reconciles **off-peak** instead of `review` (kills the OHME review-churn); and (c) an OHME bump (not in Ohme's plan, no started) then falls to the online-gate → peak, consistent with the rest of the model.

Guarded: only active when the `slot_list` sensor is detected AND parses non-empty (else the pre-existing superset behaviour is unchanged); OHME-only; non-fatal; richly logged. The **parser is unit-tested** (`tests/test_ohme_slot_list.py`), but the **end-to-end path is UNVALIDATED** — the author is on Zappi, so it ships flagged pending an Ohme tester. Settlement remains the final authority throughout.
