# EMT — Intelligent Tariff Dispatch-Aware Rates (3.0.0)

> _Status: Shipped — design note kept for rationale; the code is the source of truth._

**Status:** BUILT (compute-and-log), validated live against a real Intelligent
Octopus Go / myenergi Zappi account. Capture + overlay are deployed and
exercised; the apply flag is still OFF pending a live soak. Grounded in the
BottlecapDave HomeAssistant-OctopusEnergy integration (BCD) — the most mature
open implementation — whose *algorithm* (not line code) is the reference.

This document now reflects what was actually built and observed. Where earlier
drafts speculated, the speculation has been replaced by the confirmed behaviour.
The supporting BCD-lessons material (§2) and the per-provider design (§3) are
retained because they remain the rationale; the build status and open questions
(§4–§5) are rewritten to reality.

---

## 1. The problem (confirmed, and now addressed)

EMT resolved the import rate from `standardUnitRates` only. For a fixed/standard
tariff that's correct. For **Intelligent Octopus**, it was not: the off-peak rate
applied during smart-charge dispatches (EV charging *outside* the normal cheap
window) was missing, so those slots were billed at peak.

Two distinct bugs lived here, now both fixed:
1. **Base-schedule flat-rate bug (FIXED, verified live).** Every sensorless block
   inherited the first block's rate uniformly, so overnight in-window slots were
   billed at peak too. Root cause: the `last_known_rates` fallback ran *before*
   the schedule resolver. Fixed by running the Kraken schedule resolver first;
   the live DB now shows distinct `[0.05493, 0.32309]` (off-peak/peak).
2. **Out-of-window dispatch slots (overlay, BUILT).** A daytime smart-charge
   dispatched by Octopus is billed peak by the base schedule but should be
   off-peak. This is the overlay's job (§3) and is now built.

### 1a. Tariff taxonomy — ONLY Intelligent needs dispatch logic
Of all Octopus import tariffs, only the Intelligent/IOG family needs the overlay.
Every other tariff is pure schedule-driven and served by `RateSchedule.resolve`:
  - **Fixed / Flexible / 12M-FIX:** one (or capped) rate. resolve(ts).
  - **Tracker:** one rate per day. resolve(ts).
  - **Cosy:** fixed multi-window time-of-use. resolve(ts).
  - **Agile:** 48 half-hourly prices/day, in standardUnitRates. resolve(ts).
    CAVEAT: can be NEGATIVE — do NOT clamp import rate ≥ 0.
  - **Intelligent Octopus Go:** fixed cheap overnight window (in standardUnitRates,
    resolve handles it) PLUS smart dispatch charging outside it (the overlay).

### 1b. No authoritative "settled cost per slot" API (confirmed)
There is NO per-slot settled-cost endpoint. BCD confirms even it computes cost as
`consumption × rate per half hour, summed`; Octopus provides only basic rate
info. So our own rate reconstruction + dispatch overlay is necessary, not
pessimism — there is no readable truth to substitute for it.

---

## 1c. THE FINALISED APPROACH (authoritative summary)

### Two-layer rate model (BUILT)
1. **Base layer — schedule resolution (all tariffs).** `RateSchedule` built from
   `standardUnitRates`, resolved per HH slot, negative rates allowed. This is the
   prerequisite and the flat-overnight bug fix (§1, item 1). DONE, verified live.
2. **Overlay layer — dispatch off-peak adjustment (Intelligent only).** For HH
   slots OUTSIDE the standard off-peak window, if the slot is a recorded
   smart-charge dispatch AND real import flowed, adjust peak → off-peak. BUILT.

### RATE IS A FINALISE-TIME CONCERN (the key architectural decision)
kWh is provisional-at-finalise → authoritative-at-DCC-settlement. RATE is NOT:
it is determined at finalise from whatever rate source exists, exactly as EMT has
always done (CAD rate sensor in v2). EMT ALWAYS self-derives its rate — schedule
base + dispatch overlay — and NEVER consumes a BCD rate/cost sensor or defers its
rate decision to BCD (see §4.10 point 6, the decision that stuck; an earlier idea
of "consume BCD's rate and skip the overlay when BCD is installed" was dropped).
The overlay therefore ALWAYS runs at finalise. (BCD is still used elsewhere — its
`current_demand` for the live-power offload, and detection — but never for the
rate.)

Consequence — the overlay applies at **two** call sites (BUILT):
  - **(A) At finalise** (`finalise_block`, after `compute_channel`): a fresh
    out-of-window dispatched block is priced off-peak the moment it forms.
  - **(B) At settlement** (`_rerun_pass2_for_settled_block`): when DCC re-
    materialises the kWh, the overlay is RE-APPLIED so the off-peak rate survives
    the kWh change (settlement must not silently revert it to base/peak). B is
    *preserve*, not an independent decision.
The overlay is idempotent: a pure function of (base schedule + dispatch_slots +
meter draw), so repeated passes can't double-apply.

### STANDING PRINCIPLE: never blanket-recompute the overlay over history (it is non-reconstructable)
The overlay is a function of `dispatch_slots`, and those slots are **captured
forward while planned** and **cannot be recovered retrospectively**: planned→
completed drops `source` to `unknown` (proven live), the API serves no historical
slot data, and captured slots prune at 90 days. So a slot absent from the table
at recompute time is indistinguishable from "no dispatch" — the resolver returns
base/peak.

Therefore **PASS 2 / settlement may only be re-run over the recent window where
the slots still exist.** The ~2-day DCC settlement horizon sits comfortably inside
the 90-day retention — that is *why* call site (B) is safe. A **blanket recompute
over arbitrary history is FORBIDDEN**: for any block correctly priced off-peak
live whose slots have since aged out — or were never captured because the add-on
was down / had no API connectivity at the time — a re-run would silently re-price
that energy back to peak, **degrading correct history rather than repairing it**.

Corollary for any per-device rate fix (e.g. the follow-main overlay fix —
a sub-floor follower must inherit main's overlay decision, not re-qualify the
over-report floor against its own draw): apply it **at finalise**, where the slots
are known. Existing blocks are left as-is, not retro-recomputed. A repair pass is
acceptable only inside the live slot-retention window, never as a full-history
sweep. (Confirmed in practice: a prod-dev DB held blocks back to February but only
~hours of dispatch_slots — a history sweep had nothing to reconstruct from.)

### The key signal is the SMART-CHARGE dispatch, captured while PLANNED
- **Planned** dispatches carry `meta.source` — and on this account it is
  populated: `smart-charge` (the signal we want), `bump-charge` (boost, excluded),
  `test-charge`, or `unknown`/`null`. CONFIRMED live: planned smart-charge slots
  appear with `source=smart-charge`.
- **Completed** dispatches lose the signal: once a dispatch is actioned it moves
  to completed with `source=unknown`. CONFIRMED live — watched the 02:30–05:00
  slots transition planned→completed and go to `unknown`. So the smart signal
  CANNOT be recovered retrospectively from completed data.
- **Therefore capture MUST run forward** while the dispatch is still planned, and
  persist the smart-charge slots to our own table. This is not optional; the live
  planned→completed transition proves it. (This is BCD's "started dispatch"
  concept — capture the active planned dispatch into a durable record.)

### GENERAL PRINCIPLE: validate dispatch intent against actual meter draw (BUILT)
Dispatch signals report INTENT and OVER-REPORT (CONFIRMED for Zappi: the slot
stays "on" after charging stops; real users report planned slots billed at peak
because the car wasn't actually commanded). BCD itself tells users to "perform
additional checks to make sure your vehicle is actually charging."

EMT is uniquely placed to do this because it READS THE METER. The overlay rule is
therefore not "dispatch slot active → off-peak" but:
  **dispatch slot active AND actual import drawn in the slot → off-peak.**
A slot dispatch-active but with no import draw is NOT overridden (the
over-reporting guard — BUILT and tested with teeth). This makes EMT's overlay more
accurate than a pure dispatch consumer, for any over-reporting provider.

### Capture cadence: 5-minute throttled tick (BUILT, BCD-informed)
BCD refreshes dispatches ~every 60s. EMT captures every **5 minutes** on a
self-throttled tick (mirrors the CI tick), decoupled from the 6-hour DCC poll.
Rationale: we capture slots for *later billing*, not real-time automation, so
"caught while planned" is the only requirement — and Octopus schedules dispatches
ahead, so 5 min catches the planned state with margin while being far lighter on
the API than 60s for an add-on also doing DCC polls + CI ticks. CONFIRMED live:
ran all night every 5 min, persisted smart-charge slots reliably, caught the
planned→completed transition.

### Storage: `dispatch_slots` table (BUILT)
Columns: `slot_start` (naive-UTC, PK), `off_peak`, `provider`, `source`,
`captured_at`. Last-write-wins upsert; 90-day prune; created on new and upgrade
DBs. Capture persists `source=smart-charge` slots only (the started/smart gate —
bump-charge/unknown/null excluded). The overlay reads this table; it is the only
new persisted state.

### Per-provider matrix (how dispatch_slots is fed)
- **Planned-dispatch providers (Zappi/MYENERGI_V2, Tesla, VW, vehicle
  integrations), BCD absent:** derive smart-charge slots forward from Kraken
  planned dispatches with `source=smart-charge`. CONFIRMED live for
  `MYENERGI_V2`. This is the primary, cleanest branch.
- **BCD installed:** no change to the rate path. EMT ALWAYS self-derives the
  dispatch feed from Kraken and never defers to BCD's `intelligent_dispatching`
  sensor (§4.10 — EMT never defers its rate decision to BCD). BCD is used only
  for the live-power offload (`current_demand`), never for the rate.
- **OHME (BUILT — capture branches on `provider==OHME`; overlay, meter
  validation and storage stay the shared path).** OHME differs from Zappi:
  completed dispatches are boost-contaminated and `meta.source` is unreliable for
  it, so Octopus's own data can't cleanly separate smart from boost, and the
  Zappi `source=='smart-charge'` gate would capture almost nothing for OHME and
  badly under-bill. The OHME HA integration CAN distinguish the modes. So only
  the capture-FEEDING rule branches (`_capture_ohme_slots`); everything
  downstream is unchanged. One code path handles both cases per capture tick:
    1. Provider is OHME → look for the OHME charge-mode entity in HA. Try the
       OFFICIAL `ohme` integration first (charge-mode SELECT: "Smart charge" /
       "Max charge" = boost / "Paused"), then the unofficial `dan-r/HomeAssistant-
       Ohme` (`Charge Slot Active` binary sensor). Auto-detect by known entity
       patterns, with an OPTIONAL user config override naming the entity (folds
       into the supplier-first wizard surface).
    2. **Sensor available → VERIFIED path (sensor-driven, per tick):** snap NOW
       to its 30-min slot and capture it iff the sensor reports a smart slot
       active (official "Smart charge" / dan-r binary on) → `source=ohme_verified`,
       `off_peak=True`. Boost (official "Max charge") → capture NOTHING (the slot
       has no dispatch_slots row, so the overlay leaves it at peak — the boost
       veto). dan-r cannot report boost directly, so it confirms smart positively
       and leaves boost to inference-by-absence (per the agreed dan-r asymmetry).
       Because the sensor is real-time charge STATE independent of Octopus's
       planned data, this closes BOTH OHME residuals: it vetoes boosts AND catches
       smart charges the planned superset missed. 5-min ticks sample each 30-min
       slot ~6×, so a running smart charge is caught. Meter validation is NOT done
       at capture — it lives in the shared overlay (next bullet), which is what
       still makes EMT more accurate than BCD for OHME.
    3. **Sensor absent → OPTIMISTIC fallback:** every planned-superset slot is
       captured as an off-peak candidate, `source=ohme_assumed_unverified`. The
       shared overlay's meter-draw validation (draw ≥ 0.1 → apply; else not) +
       its out-of-window filter narrow these to slots that actually drew off-peak.
       Deliberate divergence from BCD (which leaves these at peak); the meter
       guard bounds the risk to under-application.
    4. **LOG which path is active every time** — e.g. `ohme: charge-mode sensor
       <entity> available → verified path` or `ohme: no charge-mode sensor found
       → optimistic fallback (assumed off-peak, meter-validated)`. This log line
       IS the config-context a bug report needs: a user's log immediately shows
       whether they're verified or optimistic, no round-trip required.
  Meter validation applies in the shared overlay on BOTH sub-paths (kept even
  when the mode sensor is definitive — it's cheap and it's what makes EMT more
  accurate than anyone). In API-only mode, the OHME-optimistic case is the
  weakest-confidence combination (assumed-smart AND provisional-draw until DCC
  settles) — logged loudly. The path APPLIES LIVE (same as Zappi; no separate
  compute-and-log gate), covered by pure helper tests (`_ohme_interpret_mode`,
  `_ohme_capture_slots`) but unvalidatable on this account (Zappi only). It
  therefore ships dormant on a Zappi account and is corrected from the first real
  OHME user's log: `_capture_ohme_slots` logs the decision (verified-smart /
  verified-boost-veto / optimistic) AND the actual planned `source_dist` every
  tick, so that log is the evidence — the apply-live call is safe precisely
  because the meter guard bounds failure to under-application and the provenance
  source values + corrections path are the safety net.
- **No-dispatch-data providers (e.g. Hypervolt-only):** no overlay possible;
  price at standard rate, mark "priced without dispatch data". FUTURE.
  CONFIRMED (BCD issue #1687, Mar 2026): the Kraken `/intelligent/dispatches`
  endpoint **returns NO dispatch data for Hypervolt-only setups** (charger-route
  integration, no separate vehicle integration). Octopus IS smart-charging the
  Hypervolt — the app shows the windows, the charger draws ~7kW in them — but the
  dispatch plans flow through a private Octopus↔Hypervolt channel the public API
  does NOT expose; the user sees `SMART_CONTROL_NOT_AVAILABLE` and
  `intelligent_dispatching` permanently off (BCD's own sensor is stuck too).
  Likely tied to `vehicle_battery_size_in_kwh: null` — via the charger route
  Octopus can't see the car's battery, so the API reports smart control
  unavailable even while it's working.
  KEY DISTINCTION (route, not brand):
    - **Hypervolt via CHARGER integration → no API dispatches.** Our capture finds
      nothing; the overlay cannot apply. Correct behaviour: price at base
      schedule, mark "no dispatch data". Do NOT attempt to reconstruct from the
      Hypervolt HA integration — even Octopus's own API can't cleanly expose the
      smart-charge signal for this route, so reconstruction would be guessing at a
      signal the platform itself withholds (far weaker footing than the Zappi
      `source=smart-charge` path). Decided AGAINST building a Hypervolt branch.
    - **Same car via VEHICLE integration (Tesla/VW/etc.) → dispatches DO come
      through** the vehicle route and the existing planned-dispatch path works.
  So "Hypervolt" is not uniformly unsupported — it's the charger-route that's
  blind. (Aside: the Hypervolt integration's own schedule data is in UTC and
  rounds completed periods to the half hour — matches our whole-slot rule, but is
  a different, quirkier shape; another reason not to special-case it.)

### PROVIDER HANDLING: capability-based, NEVER a closed allowlist (BUILT)
Octopus adds new provider strings over time (VW was added and initially showed as
"Unknown" in BCD). EMT branches on KNOWN special cases (OHME — BUILT this session;
known no-dispatch set — future) and defaults EVERY other provider (incl. brand-new
ones) to
the meter-validated smart-charge path. VINDICATED LIVE: the real provider string
is `MYENERGI_V2` — a value never hard-coded — and it fell straight onto the
correct path. Because meter validation prevents over-crediting regardless of
provider recognition, a new/unknown provider degrades safely, never breaks.

### Whole-slot rule (BUILT)
Any active smart-charge minute in a 30-min slot makes the WHOLE slot off-peak
(matches BCD: "even if only a minute charge has occurred"). No minimum-duration
floor — meter-draw validation, not a time threshold, filters non-charging
windows. A duration setting could be a future refinement; not needed for
correctness.

### Apply live, with rich per-decision logging (BUILT)
`_DISPATCH_OVERLAY_APPLY = True` (global-on, all providers): the overlay applies
the off-peak override at finalise (A) and settlement (B), bounded by the
meter-validation guard (no draw → no override → cannot over-credit). Logging is
retained at full richness as the diagnostic backstop, NOT as a gate — each
decision logs the slot, the rate change, the kWh, and the provenance source
(`smart-charge` / `ohme_verified` / `ohme_assumed_unverified`). OHME applies on
the same flag (no separate soak): the same meter-guard bound applies, and the
provenance + corrections path are the safety net.

### Hard limitations (accept)
- **Capture is forward-only replay.** A slot not captured while planned is
  uncorrectable automatically (its smart signal is gone once completed). The
  corrections tool (MODE-UI §10) is the manual safety net.
- Overlay is only as correct as the base schedule (must be fixed first — it is).
- A daytime charge that predates capture being deployed stays at peak unless
  manually corrected — a one-off transitional set, not ongoing.

---

## 2. What BCD teaches (the durable lessons — rationale, retained)

### 2.1 Octopus only provides STANDARD rate info for intelligent tariffs
`standardUnitRates` gives standard peak/off-peak rates and their normal times.
Off-peak *outside* the normal window (caused by a dispatch) must be adjusted
manually. (Confirmed: our schedule shows exactly the two standard rates.)

### 2.2 The smart-charge dispatch is the rate key — captured while planned
Planned = intention (churny, but carries the `smart-charge` source signal while
upcoming). Completed = mixes smart + boost and on this account reports
`source=unknown`, so it's unusable for rate-setting. The durable record we keep is
the smart-charge planned slot, snapped to 30 min — BCD's "started dispatch" by
another name. CONFIRMED live end-to-end.

### 2.3 30-minute increments
The whole 30-min slot is off-peak even for a partial charge; Octopus may stop a
dispatch early, so record slot-by-slot rather than trusting a full planned window.
(Implemented in the slot snap.)

### 2.4 Rate completeness sequencing
BCD doesn't finalise intelligent rates until dispatch data is retrieved. Our
equivalent: the overlay runs at finalise (A), so the dispatch adjustment is part
of forming the block's rate, not an afterthought.

### 2.5 Provider caveat
Planned dispatches aren't available for all providers; degrade gracefully when
absent. (Capability-based default handles this.)

---

## 3. How it maps onto EMT (as built)

### 3.1 Two rate-resolution moments, both dispatch-aware (BUILT)
EMT carries each block's OWN half-hour rate (the rate flips on the HH boundary).
The overlay honours this at both moments: finalise (A) prices the block's own slot
including any smart-charge dispatch; settlement (B) re-resolves with the overlay
when DCC re-runs PASS 2, preserving the off-peak rate against the settled kWh.

### 3.2 The overlay function (BUILT)
`_dispatch_overlay_rate(channel, block_start, base_rate, imp_kwh)`:
```
if channel != import: return base_rate
slot = dispatch_slots.get(snap_to_30min(block_start))
if not slot or not slot.off_peak: return base_rate
if imp_kwh < 0.1: return base_rate           # meter-validation guard (noise floor)
off_peak = schedule.off_peak_rate_near(block_start)   # cheapest rate that day
if base_rate <= off_peak: return base_rate   # in-window → no-op
return off_peak (apply) | base_rate (compute-and-log)
```
`off_peak_rate_near` = the minimum rate among the day's schedule periods —
tariff-agnostic (off-peak is always the cheaper rate). BUILT in kraken_rates.py.

**Meter-validation guard is a PRESENCE check, NOT a dispatch-delta comparison.**
The guard answers one question: "did real import actually flow in this slot, or
is the dispatch over-reporting a charge that didn't happen?" It does NOT compare
block draw against the dispatch's `delta`. Those measure different scopes — the
dispatch `delta` is car-only (what Octopus delivered to the vehicle), while the
block draw is car + household baseload. Block draw is therefore almost always
GREATER than dispatch delta by the amount of other house consumption, so a
draw-vs-delta comparison (either "≈ within tolerance" or "draw > delta") is wrong:
the former rejects valid slots whenever the house used anything else, the latter
is trivially true from baseload alone. The gate is simply: did meaningful import
flow? **Threshold = 0.1 kWh** in the slot — a noise floor that rejects sensor
jitter / tiny baseload trickle being mistaken for charging, while any genuine EV
charge clears it with huge margin (live data: charging slots show 2–6 kWh, idle
slots ~0.0 — clean separation, 0.1 splits them safely both ways). Confirmed
against real slot data, not picked blind.

**Validation fidelity at settlement is ASYMMETRIC — and rate is finalise-time
(reconciled with §1c).** The guard checks the block's CURRENT kWh figure at
whatever fidelity exists at that pass:
  - API-only / api+mini at FINALISE (A): validates against the PROVISIONAL Mini
    kWh (a real register delta, not a guess). Provisional draw ≥ 0.1 → apply
    off-peak provisionally.
  - At SETTLEMENT (B): DCC reconciles kWh to authoritative and the block re-runs.
    The overlay is re-applied so an off-peak rate isn't clobbered when the kWh
    re-materialises, AND — because the settlement re-run passes the settled kWh
    through the same floor — a block that finalised at PEAK because its
    provisional draw was sub-floor CAN be promoted to off-peak once DCC clears
    the floor (the solar / low-provisional case; verified). This promotion is the
    useful, safe direction.

**What settlement does NOT do: revert off-peak → peak.** A block that finalised
OFF-PEAK (provisional draw ≥ floor) whose DCC figure later comes in BELOW the
floor is NOT reverted to peak. Mechanically this is because `_resolve_block_rate`
reuses the block's stored (already-off-peak) rate as the overlay base, and the
below-floor guard returns that base unchanged — there is no original peak tariff
left to fall back to. Conceptually it is CONSISTENT with §1c ("RATE IS A
FINALISE-TIME CONCERN"): the rate is decided at finalise; kWh firms up at
settlement, the rate does not. The residual over-credit is bounded by the floor
(the settled kWh is by definition sub-floor, so < ~3p/block) and is the accepted
cost of the finalise-time decision. NOTE this means the guard's failure mode is
NOT purely under-application in the settlement-revert direction — a small,
bounded over-credit is possible here; the finalise-time guard still cannot
over-credit at finalise.

> **Open for future reconsideration — symmetric settlement re-validation.**
> If we ever decide the rate SHOULD firm up with the kWh in both directions
> (revert off-peak → peak when DCC lands sub-floor, not just promote), the fix
> is to RE-DERIVE the base tariff from the schedule resolver at settlement for a
> block sitting in a dispatch slot — rather than reusing the stored overlaid
> rate — so the below-floor guard reverts to the real peak. Feasibility caveat:
> this needs the block's dispatch slot to still exist (inside the ~90-day slot
> retention, which the ~2-day DCC horizon comfortably sits within) — outside
> that window the slot has aged out and a blanket recompute is FORBIDDEN (§ "A
> blanket recompute over arbitrary history is FORBIDDEN"), so any such revert is
> horizon-bounded only. The floor stays either way (it is load-bearing for the
> OHME optimistic path — see OHME §; removing it would over-credit OHME). Left
> deliberately unbuilt: current decision is finalise-time rate per §1c.

### 3.3 Kraken dispatch query (BUILT, field names confirmed)
`get_dispatches(account)` queries `plannedDispatches`/`completedDispatches` with
`startDt endDt delta meta{source location}`, plus `registeredKrakenflexDevice
{provider status}`. Field-name corrections learned via live 400s and BCD
research: dispatch slots use `startDt`/`endDt` (not start/end); the device's
`deviceId` field does NOT exist (we use the device id from `get_device_id`);
`registeredKrakenflexDevice` is DEPRECATED by Kraken (migrating to a `devices`
query) — request only confirmed scalar fields and tolerate absence. Results
normalised to stable `start/end/delta/source` keys.

### 3.4 Reconciliation lifecycle (BUILT, simplified by the A&B model)
Because rate is set at finalise (A), a block is correctly priced at formation and
does not depend on settlement firing. Settlement (B) re-applies the overlay only
when the kWh actually changes (the re-flag guard correctly allows that case). The
earlier "multi-pass gate waiting for dispatch data" model is no longer needed for
the planned-dispatch path: the dispatch data is captured forward and present at
finalise. (The bounded-wait model may still matter for future OHME, whose billing
truth is retrospective.)

### 3.5 Export (verified)
OUTGOING-VAR export is handled by the existing `RateSchedule.resolve` and is NOT
dispatch-affected. The DCC-only-export materialisation bug (settled export stuck
in `exp_kwh_api`, never priced) was fixed and verified live.

### 3.6 / 3.7 BCD-installed consumption + OHME branch
**OHME branch — BUILT this session.** Capture branches on `provider==OHME`
(`_capture_ohme_slots`): optimistic (no sensor) or sensor-verified (official
`ohme` select / dan-r binary), three-way via the pure `_ohme_capture_slots`
helper, applying live with rich logging. Detection is `detect_ohme_charge_mode`
(kraken_api_client) wired through `_detect_and_log_integrations`, which preloads
the charge-mode entity so the capture tick's `get_state` works. Provenance source
values: `ohme_verified` / `ohme_assumed_unverified`. See the per-provider matrix
above for the mechanism. Ships dormant on a Zappi account (validated by tests +
first OHME log).

**BCD live-power offload — the only BCD consumption EMT does.** `detect_bottlecapdave`
finds the `current_demand` sensor and the wizard pre-fills the Live Power field
with it. That's the extent of it: EMT consumes BCD's live-power sensor for the
gauge, and nothing else. It never consumes BCD's rate, cost, or
`intelligent_dispatching` sensors — the rate is always EMT's own self-derived
schedule + overlay (§4.10). The old "prefer BCD's dispatch feed when installed"
idea is dropped, not deferred: it conflicts with the never-defer-to-BCD decision.

---

## 4. Build status

DONE, all tested:
1. Base-schedule flat-rate fix (layer 1). Verified live.
2. Kraken dispatch query (correct fields, normalised). Verified live.
3. `dispatch_slots` table + store methods.
4. Forward capture of smart-charge slots, 5-min throttled tick. Verified live.
5. Overlay resolver + `off_peak_rate_near` + meter-validation guard.
6. Overlay applied at finalise (A) and settlement (B). Tested with teeth.
7. Export per-slot resolution + DCC-only-export fix. Verified live.
8. **Apply flag flipped to True (global-on)** — overlay applies live; logging
   retained as the diagnostic backstop, not a gate.
9. **Integration detection** — `detect_ohme_charge_mode` + `_detect_and_log_
   integrations` emit the `config-state: detection …` line (bcd / bcd_live_power /
   ohme_charge_mode / integration / ohme_path). BCD live-power detection +
   wizard prefill pre-existed.
10. **OHME capture branch** — `_capture_ohme_slots` (three-way: optimistic /
    sensor-verified-smart / sensor-boost-veto), pure helpers tested with teeth,
    applies live, ships dormant on Zappi.
11. **Corrections gating rule** — in API+ modes a RATE correction only touches
    DCC-settled blocks (imp_kwh_api / exp_kwh_api NOT NULL); unsettled blocks are
    skipped + reported ("re-run after settlement"); pure CAD applies immediately.
    This is the primary protection against reconciliation clobbering a correction
    for the common case (see §5.3 and MODE-UI §10).
12. **Device "use overlay" rates (release decision E)** — a sub-meter with no own
    rate sensor is priced on the main meter's EFFECTIVE rate: inherited base tariff
    (CAD main) or schedule-resolved rate (API main, where parent_rates is empty),
    then the dispatch overlay against the DEVICE's own draw. So an EV bump-charging
    midday on IOG is costed off-peak, not the inherited daytime peak. Precedence:
    own sensor (incl. BCD `current_rate`) → "use overlay" → base; default ON when
    an API key is configured, opt-out via `meta.rate_source="base"`. Flag persists
    in a dedicated `meters.rate_source` column. Tested with teeth (precedence +
    finalise: off-peak-with-flag / peak-without / peak-when-no-draw).

~1024 tests green.

### Attribution principle (documented, no legal framing)
Per-device costs are **attribution / insight**, not a separate billable rate: EMT
allocates the single metered bill across devices so users can see what each one
cost under their real tariff. The meter remains the one source of truth; EMT does
not split billing by telemetry. "Use overlay" is what makes that allocation
*accurate* during smart-charge dispatch. (Framed as a principle, not a legal
posture — the regulatory ground may shift; see Future §F2.)

---

## 5. Genuinely open (what's actually left)

1. **~~Flip `_DISPATCH_OVERLAY_APPLY = True`~~ — DONE.** Shipped GLOBAL-on (all
   providers apply on the same path), not per-provider-gated. Rationale (retained):
   the meter-validation guard structurally bounds the failure mode to
   UNDER-application (a slot wrongly left at peak, recoverable at settlement / by
   correction) — it CANNOT over-credit, since no draw → no override. Caveat to
   document at release: tested on Zappi only; other providers (incl. OHME) are
   algorithm-grounded but unvalidated against real data; report mispricing and we
   diagnose from logs.
2. **Config-state diagnostic logging (first-class build requirement).** Because
   the integration is self-hosted and we cannot see a user's config, the LOG is
   the only diagnostic window — and "rely on user reporting" only works if a
   report is actionable. Emit a config-context dump at startup AND a compact
   config fingerprint on each overlay decision, covering at least: data-source
   mode (api / api+mini / cad+api / cad), provider string, BCD-detected y/n,
   rate-sensor-override y/n (→ is the overlay even running?), apply-flag state,
   tariff/product, and for OHME the charge-mode-sensor availability + which path
   (verified / optimistic). This turns "OHME mispricing" from an unactionable
   report into one whose log already contains the missing observation. It is the
   mechanism that makes the ship-optimistic / correct-from-reports approach
   function as a real feedback loop, and it is what later justifies building any
   provider enhancement (e.g. the OHME verified path) on evidence not speculation.
3. **Re-flag-on-dispatch trigger (an outage corner case, NOT a routine gap).**
   In principle a block that ALREADY settled to DCC (kWh stable) and only THEN
   gets a captured smart-charge slot would not re-run (no figure change → re-flag
   guard suppresses it) → stays peak. In PRACTICE this almost cannot happen:
   capture runs forward every 5 min and records the slot while it is *planned*
   (hours ahead overnight, minutes ahead for daytime — Octopus schedules ahead),
   whereas DCC settlement lands 1–2 days LATER. So the slot is essentially always
   captured long before settlement. The only way settlement beats capture is if
   EMT was NOT running for the entire planned window of a dispatch (an outage
   spanning it) AND DCC then settled it — i.e. it collapses into the
   outage-recovery story, not a standing overlay gap. If we ever decide to harden
   it: a targeted trigger that flags a block for rerun when it has an unapplied
   out-of-window smart-charge slot with real draw. LOW priority — the normal
   overnight/daytime flows are fully covered by A (finalise) and by
   B-on-figure-change (settlement).
3. **~~Corrections tool must protect manual overrides~~ — LARGELY DONE via the
   corrections gating rule (§4.11; cross-ref MODE-UI §10).** The risk was that a
   manual rate correction to a block could be clobbered when reconciliation re-runs
   `_resolve_block_rate`. The gating rule removes this for the common case: in
   API+ modes a RATE correction is only allowed on DCC-settled blocks (which are
   not routinely re-run), and unsettled blocks are skipped + reported. What
   remains deferred is the narrower DCC-RE-settlement case (a block that settles a
   SECOND time, e.g. an Octopus re-issue) and the standing-charge override — for
   those a per-block "manually corrected / locked" flag the rerun path RESPECTS is
   still wanted. Lower priority now that the routine path is covered.
4. **~~OHME branch~~ — BUILT (§4.10).** BCD-sensor dispatch-feed consumption
   (`intelligent_dispatching` offload, §3.6/3.7) remains future and low priority;
   the BCD live-power offload is already in place. The OHME branch ships dormant
   on a Zappi account, validated by tests + the first OHME user's log.
5. **CI startup gap-fill is forward-only (separate subsystem note).** Unrelated to
   dispatch; the carbon-block recovery (built this session) covers the realistic
   outage case. A >4-day-late outage discovered after CI prune stays unrecoverable.
6. **Kraken API footprint + detection-driven offload (DESIGN AGREED, build).**
   EMT diverges fully from BCD on BILLING — it never auto-consumes BCD's cost or
   rate sensors and never defers its rate decision to BCD. Billing is always EMT's
   own meter-grounded reconstruction (schedule + overlay). This dissolves the
   earlier "can we trust BCD's sensor for OHME" tangle: EMT uses its OWN source,
   or a source the USER explicitly hands it — never a silent auto-deferral.

   **Measured footprint (counted from live dev log, steady state):**
   - dispatch overlay `get_dispatches` (5-min tick) = **12/hr** (fixed, cheap).
   - Mini boundary-bracketing `smartMeterTelemetry` = ~3–5 calls × 2 boundaries
     = **~8–10/hr**.
   - token refresh ~1/hr; DCC `get_consumption` ×2 per 6h ≈ 0.3/hr.
   - **EMT mandatory floor ≈ ~20/hr** — LOW; confirmed a full dev day with NO
     `KT-CT-1199` on EMT's side (the one telemetry failure was a transient
     `Unable to query…`, not a rate limit; next call succeeded).
   - A LIVE POWER dashboard tile, if fed by polling Mini at ~1/min, would add
     **~60–72/hr** — this is the only heavy consumer and the real KT-CT-1199 risk
     on a shared account.

   **Rate limit is PER-ACCOUNT, not per-integration.** Observed live: EMT + BCD on
   one account → BCD hit `KT-CT-1199` while EMT stayed clean (EMT heavier, won the
   quota). Resolves when EMT replaces BCD, but recurs if the holder runs anything
   else against Kraken GraphQL.

   **Three detection-driven rules (footprint shrinks as user's own sensing grows):**
   a. **BCD-with-Mini detected → offload the live power tile to BCD's ~1/min
      sensor** (AUTO-detect; cosmetic, so automatic is fine). EMT does NOT spend
      its own ~60/hr replicating a live feed BCD already polls. EMT's Mini stays
      at boundary-bracketing only.
   b. **Keep EMT's own footprint minimal** — anything a user's existing sensor can
      serve, it serves; EMT spends quota only on what's truly its own.
   c. **User explicitly overrides rate sensors → skip the dispatch overlay AND its
      `get_dispatches` polling entirely** (EXPLICIT opt-in; billing-affecting, so
      never silent). EMT consumes the user's rate at finalise (the original
      "rate is whatever source exists" principle). Saves the 12/hr dispatch cost.
      If that rate source is wrong for their provider, that's THEIR explicit
      choice — defensible in a way silent deferral was not.

   **Footprint ladder:**
   - API-only, no user sensors → ~20/hr (overlay active, full self-reliance).
   - API+, BCD Mini present → ~20/hr billing, live tile offloaded to BCD (avoids
     +60/hr). Overlay still active (billing stays ours).
   - API+, user overrides rate sensors → ~8–10/hr (no dispatch poll, no overlay).

   **Key principle: AUTO-detect the cosmetic offload (rule a); require EXPLICIT
   opt-in for the billing-source override (rule c).** Cosmetic-path freedom,
   billing-path caution.

   Defensive extra (BUILT, release 4b): on `KT-CT-1199`, HTTP 429/5xx, or transient
   transport/timeout from any Kraken call, `_graphql` applies bounded exponential
   backoff with jitter (1→2→4→8s, ≤4 retries) then surfaces the error — centralised,
   so the Mini poller, ingester, and settlement all inherit it.

### Resolved during build (no longer open)
- Started-vs-planned key → smart-charge planned slot, captured forward. CONFIRMED.
- `meta.source` populated on planned (`smart-charge`/`bump-charge`), `unknown` on
  completed. CONFIRMED live (corrects the earlier "source always null").
- Provider string is `MYENERGI_V2`; capability-based default vindicated.
- Capture cadence → 5 min. Off-peak rate source → `standardUnitRates` (cheapest
  of the day via `off_peak_rate_near`). Overlay timing → finalise (A) + settlement
  (B). Meter validation supersedes a duration floor. No settled-cost-per-slot API.

---

## 6. Engine support reused
- Authenticated Kraken GraphQL client + device id (from Mini discovery).
- `RateSchedule.resolve` / new `off_peak_rate_near`.
- `_kraken_rate_resolver` (base) + `_dispatch_overlay_rate` (overlay).
- DCC PASS-2 re-run machinery (settlement path B).
- Per-block materialised cost (settle-aware), `dispatch_slots` persistence.

---

## 7. Future overlay extensions (post-3.0, not built)

- **F1 — IOG split rate on cumulative usage (NEW, watch).** Octopus is pushing
  Intelligent Octopus Go toward a *split rate based on ~6 hours of cumulative
  usage* — i.e. a rate that changes once a duration threshold is crossed within a
  period. This is a DIFFERENT overlay mechanism from the current dispatch-window
  override: it's a **cumulative-duration tier**, not a per-slot off-peak swap. If
  it lands, extend the overlay to track cumulative qualifying usage and switch the
  rate at the threshold. Design only — pricing rules not yet firm.
- **F2 — Attribution principle may need revisiting** if the regulatory ground on
  per-device telemetry shifts. Documented as a principle (see §4 Attribution), not
  a legal posture, precisely so it can move.
- **F3 — BCD `intelligent_dispatching` dispatch-feed offload** (backlog 2). When
  BCD already computes dispatch slots, consume its feed instead of EMT's own
  capture. Pending verification that BCD's slot granularity/semantics match
  `dispatch_slots`. The BCD *live-power* offload already exists.
- **F4 — Explicit rate-sensor-override as a 3rd detection feature** (backlog 3).
  The `config-state` line already logs a `rate-sensor-override` slot; wiring it to
  actually disable the overlay + `get_dispatches` polling (footprint ladder §5.6c)
  is the remaining build. Confirm whether the slot is wired or a placeholder.
- **F5 — DCC re-settlement override lock + standing-charge override** (backlog 1;
  MODE-UI §10). The per-block "manually corrected / locked" flag the rerun path
  respects, for the narrow second-settlement and standing-charge cases.
- **F6 — Bright (Hildebrand/Glowmarkt) as a second DCC settlement source.** As a
  DCC participant it serves settled HH data going back far further than the
  supplier API; it would feed the same `*_api` settlement columns (which is why
  those stay strictly "real DCC settlement"). Needs its own auth/connectivity.