# Intelligent Octopus 6-hour-cap tariff (IOG-SMB-TOU) — design note

## Summary
Octopus's Intelligent Octopus Go moved to a **4-rate, daily-6-hour-cap** model
(Charge Cap, live 2026). EMT's job: reconstruct and price the **house-vs-EV
split** Octopus rates internally but does **not** itemise on the bill, for **any**
charger (not just Ohme), keying the cap off Octopus's own dispatch ledger and
letting settled cost be the final arbiter.

The model below is **confirmed against Octopus's published material** (their
"Smart charging and Charge Cap explained" explainer, and the `IOG-SMB-TOU-…`
tariff payloads). Build it; it is no longer blocked on confirmation. The only part
still worth a live check is the exact **boundary-slot** rounding — and that
self-corrects from settled cost, so it does not gate the build.

## The tariff (4 rate buckets)
`IOG-SMB-TOU-…` (e.g. `E-1R-IOG-SMB-TOU-25-12-12-H`) drops `standard-unit-rates`
and exposes four buckets (`standing-charges` unchanged):

| rel | meaning |
|---|---|
| `day_unit_rates` | general home usage, peak/day |
| `night_unit_rates` | general home usage, off-peak (the guaranteed window) |
| `ev_device_off_peak_unit_rates` | EV charging, within the 6 h cap |
| `ev_device_peak_unit_rates` | EV charging, beyond the cap (or Boost) |

Any integration that only reads `standard-unit-rates` gets no rates on a migrated
meter → silent £0. General-usage day/night support already ships (3.1.5); this
note is the EV cap layer on top.

## Billing model

### The split — dispatch is the billing basis, always
- Bill on **main-meter grid import**. From each half-hour take the
  **dispatch-derived EV kWh** — the `completed`-dispatch delta, Octopus's own
  per-HH EV energy — **grid-clipped** to that slot's import (`EV = min(delta,
  grid_import)`), and **house remainder = grid import − EV**. The clip +
  cost-apportionment keep `house + EV == grid import` exactly.
- **The physical EV device (a real charger sub-meter, where present) is indicative
  only — never the billing basis.** Billing is always dispatch-derived, so API-only
  and prod behave identically and users without a charger meter (most, on a vehicle
  integration) are covered. This mirrors Octopus, who don't read your charger
  either.
- **Solar/battery** only removes *house* load from the meter (`house = max(0, house
  − generation)`); it does not exempt the car — a 7 kW draw exceeds a 3.6–5 kW
  inverter clip, so the car still imports and still counts toward the cap. The
  grid-clip already encodes this: a rare fully-covered slot has `EV grid = 0` and
  drops out for free. Octopus's "sunny afternoon doesn't count" only applies at
  **zero total import**, which is uncommon mid-charge.

### The four rules
1. **Home off-peak (guaranteed):** the house always gets the off-peak (night) rate
   inside the agreement's off-peak window — **read from the rate schedule, not
   hard-coded** (23:30–05:30 for IOG today). Independent of dispatch and cap.
2. **Car allowance:** the car gets up to **6 hours** of off-peak charging per
   **cap-day (noon→noon, local)**, whenever Octopus dispatches it.
3. **Home freebie:** when the car is dispatched **outside** the off-peak window,
   the **house also** gets the off-peak rate for that half-hour — **but only while
   within the 6 h car allowance**.
4. **Standard / EV-peak:** the house default outside its off-peak window is the
   day rate. Once the car passes 6 h (or Boost/Bump), the **car** flips to
   `ev_device_peak` — "even at 2 a.m. while the house is still off-peak." And the
   **home freebie of rule 3 is withdrawn** on over-cap out-of-window slots, so the
   house reverts to the day rate there too. *(This last clause — house→day on an
   over-cap **out-of-window** slot — is an **inference** from rule 3's explicit
   "**within your car allowance**" conditional; Octopus states rule 3 but gives no
   worked over-cap out-of-window example. The inference is strong, but treat it as
   unconfirmed until seen on a real settled bill — see Remaining validation.)*

Consequences at the cap boundary:
- **Inside 23:30–05:30, over-cap:** car → EV-peak, **house stays off-peak** (rule 1
  is guaranteed, cap-independent). This is the only "house off-peak while car peak"
  case, and it's driven by the clock.
- **Outside the window, over-cap:** the freebie is gone, so **both** car and house
  are at their peak/day rates.

### The cap measure — union of `completed` dispatch windows
The cap is the **wall-clock union of `completed` dispatch intervals** (actual
second-level start/end) within the noon→noon day — not a slot count, not delivered
energy, not `planned`, not `started`. A `completed` dispatch is Octopus's own
acknowledgement of a real dispatch, so keying on it sorts the three real shapes
correctly: **planned/`started`-but-never-`completed` phantoms are excluded**
(scheduled charges the car never took); **completed-only over-runs are included**
(charger ran past its planned window); a **tiny completed counts at its true short
length**, not a rounded block. Merge overlapping intervals; never sum per-row
durations.

The **actual-charging-time estimator** (`est_charge_hours = delivered_kWh ÷
fullest-slot power`, surfaced on the overview) is **display / cross-check only and
must never feed pricing** — dispatched hours and charged hours legitimately differ
(e.g. 2.50 h dispatched vs 1.79 h charged on one prod session). A large gap between
the two is the **under-delivery** signature (an acknowledged dispatch the car
barely took) worth surfacing, but it doesn't move £.

### Boundary slot
A mixed half-hour carries two rates at once — house portion on day/night, EV
portion on an EV rate — so it is **always an effective-rate blend**; the 6 h
boundary slot is just the one where the **EV** portion transitions mid-slot. EMT
already prices per-portion, so this is not a new case. **The settled per-HH cost
(Measurements) is authoritative and encodes whatever Octopus did at the boundary —
read it, never recompute it.** Live, predict the EV portion off-peak provisionally
and let settlement correct. Display rule: never snap a mixed/boundary HH to one
bucket — show its **effective p/kWh**.

## Enablement & safety net
- **Auto-detected by tariff code** — a config period whose product is
  `IOG-SMB-TOU-…` turns cap handling on; everyone else is untouched. Derived from
  the period's tariff code (one source of truth, follows the period timeline), **no
  manual toggle**.
- **Settled billed cost is the backstop.** The cap model only ever **predicts
  live/provisional** prices; the deferred verify pass re-rates every slot from
  Measurements at settlement, so a slightly-wrong live estimate self-corrects to
  the penny. That's why EMT needs no enforcement switch where BCD does.
- **Constants:** 6 h, noon→noon (local). Product rules, held as constants until
  Octopus changes them.

## Display (capped tariff only; nothing below renders otherwise)
- **Live default off-peak-provisional inside an active dispatch**, with its own
  "pending" visual state. **Red must only ever mean a *confirmed* peak / over-cap
  slot** — never "not known yet". The card should move peak-free: provisional-
  off-peak (charge starts) → confirmed at settlement; the rare genuine over-cap
  slot is then the only red an overnight charge shows.
- **Billing breakdown / rate line:** split the EV portion into off-peak vs EV-peak
  with a distinct colour for the capped kWh; the EV rate line steps to
  `ev_device_peak` beyond the cap, with a marker on the crossing. Day/night home
  lines unaffected.
- **Cap indicator:** "IOG cap: 4.2 of 6 h used" / "1.1 h over → EV-peak", paired
  with the actual-charging estimate so dispatched-vs-charged is visible.

**Scope — split display for ALL IOG tariffs (post-back-end).** The house-vs-EV
**billed split** on the billing summary should render for **every IOG tariff**, not
only the capped one — a non-capped IOG user still benefits from the split Octopus
rates internally but doesn't itemise. Only the **cap-specific** elements above (the
`ev_device_peak` rate line, the over-cap colour, and the cap indicator) stay gated
on the capped tariff. This presentation work is **deferred until the back-end lands**
(the dispatch-derived split is the shared substrate either way).

## Build pieces
1. **`off_peak_windows()` on `RateSchedule`** — return the low-rate time band(s)
   from the schedule periods, so the guaranteed home window is agreement-driven.
2. **Noon→noon cap accumulator** — a new noon-boundary `bucket_fn` feeding the
   existing `_dispatch_ev_split_by_bucket` machinery; accumulate the union of
   `completed` windows per cap-day and expose where it crosses 6 h.
3. **4-rate classifier** per slot/portion — guaranteed window → house off-peak;
   within-cap dispatch → house + EV off-peak (rule 3 freebie); over-cap/Boost → EV
   `ev_device_peak` and freebie withdrawn; boundary slot → effective-rate blend.
4. **Fetch `ev_device_off_peak` / `ev_device_peak` schedules**; price the EV
   portion from them (house remainder stays on day/night). Defer to settled cost.
5. **Per-channel ex-VAT capture** (BL-23 follow-on): stamp `cost_exc`/`rate_exc`
   from each channel's own rate (house schedule vs `ev_device` schedule) rather
   than the shared main ratio. Numerically a no-op for the ex-VAT *total* (VAT is
   flat), but future-proofs per-device ex-VAT reads. Build alongside piece 4.

## Dependencies & sequencing
- **Completed-dispatch window capture** — **done**: `_completed_dispatch_slot_bounds`
  + `raw_start`/`raw_end` threaded into the completed history/slot writes.
- **Dispatch-derived EV sub-meter** (`house = main import − completed-dispatch EV`)
  — **done (BL-22)**. Read-time split from `dispatch_history` `completed` deltas,
  grid-clipped, validated ~99 % against a real CT-clamp EV meter; renders on the
  **billing summary, usage insights, and Billing-tab charts** (`ev_dispatch` meter,
  "EV (from dispatch)"). It is the cap's substrate. Two things it does **not** yet
  do, which are the cap's job:
    1. **It's display-only** — the EV slice's cost is apportioned *pro-rata at the
       block's own effective rate* (`_dispatch_ev_split_by_bucket`), so EV and house
       currently share one rate and totals stay byte-identical. The cap must price
       the two slices at **different** rates (`ev_device_off_peak`/`ev_device_peak`
       vs day/night) — that differential pricing is new.
    2. **It's gated to the no-sub-meter / no-EV-meter case** (`electricity_main`
       only) — an experimental-caution gate. **Open it for IOG tariffs.** On IOG
       the **dispatch-derived split is the authoritative EV/house billing basis**,
       regardless of what sub-meters exist. HA-sensor sub-meters (a battery, or a
       physical EV-charger CT clamp) stay **indicative** — shown per-device where a
       sensor measures them, but not the billing basis: the charger sensor is now a
       nice-to-have next to a real solution that matches Octopus's own method. The
       EV portion of the bill is counted **once**, from dispatch; a physical EV
       sub-meter's cost renders alongside as an indicative reference, never added on
       top.
- **Historical `completed`-dispatch fetch (90-day)** — see BL-9 in the roadmap. The
  cap-day union and the completed-only reprice both need the `completed` record
  present; a bounded historical fetch backfills it across downtime / re-import.

## Remaining validation (non-blocking)
Two items, both resolved by design (settled per-HH cost is authoritative), so
neither gates the build — confirm both the first time a capped account exceeds 6 h
in a day:
1. **Boundary-slot rounding** — when the running total crosses 6 h partway through
   a half-hour, does Octopus bill that HH off-peak or EV-peak? Undocumented; the
   settled cost adjudicates it and the live estimate converges regardless.
2. **Over-cap out-of-window house rate** — rule 4's inference that the home freebie
   is withdrawn (house → day rate) on an over-cap out-of-window slot. Explicitly
   confirmed for the *in-window* case (house stays off-peak); the out-of-window
   case is inferred from rule 3's "within your car allowance" wording. Strong, but
   verify against a real over-cap daytime slot.

## Surfacing the split — billing summary & charts (UI)

Confirmed against a real capped bill (IOG 12M Fixed, Jul–Aug 2026): the rate table
is a **2×2 grid — {EV, Home} × {off-peak, peak}** — up to four rows (a zero band
shows 0.0 kWh). On that tariff the EV and Home unit rates are **equal within each
band** (off-peak 5.23p both; peak 32.11p both), so the split is pure *attribution*
until the **cap** pushes EV charging into peak. The boundary half-hour (cap crossing
mid-block) carries a **blended average rate for both EV and Home**, so an over-cap
period adds a **transition dual row** at that average on top of the four clean rows —
exactly what `price_import_split` yields (house and EV blend together outside the
guaranteed window).

### Billing summary
Show the EV/Home split under **"Import — total grid"**, mirroring the statement:
group `imp_kwh_ev` (EV) and the remainder (Home) by rate band → EV/Home off-peak,
EV/Home peak, plus an EV/Home **transition-average** row when boundary blocks exist.
**"Breakdown by meter" is untouched** — the grid-total section shows the *billed
dispatch split*, the per-meter section shows the *physical devices*. Different views,
no conflict.

### Charts — one EV line, coverage-based
Never draw two EV representations. The rule is **per-slot coverage**: the charts show
the **physical EV device where it has a block**, and the **synthetic dispatch EV fills
only the slots the physical device doesn't cover**. Self-healing, no timeline stitch:
- no physical device ever → synthetic covers all history (where dispatch exists);
- physical device present and reporting → synthetic suppressed (today's behaviour);
- physical device **decommissioned** at cutover T → it stops writing new blocks, so
  the synthetic naturally takes over from T.

**Decommission = retire the physical EV device at a cutover datetime** (`retired_at`,
default now, adjustable back to when a sensor went bad). Its history is retained and
keeps showing; the synthetic picks up where its blocks end. EV is already grid-clipped
first (`min(ev, grid_import)`, house = remainder), so physical and synthetic follow the
same clip and the house line never moves at the hand-off.

**Fully reversible:** clear the decommission date (un-retire) and run the device's
**HA-sensor history fill** to backfill the gap — as physical blocks reappear, the
coverage gate hands those slots back to the physical device automatically. Because the
gate keys on *actual block coverage*, decommission / un-retire / sensor-fill all
converge on a consistent chart with no contradictory half-state.

### Cross-threading caution (UI)
Decommission, un-retire and sensor-fill interact, and the controls must not strand a
user in a confusing state — e.g. a **backdated** cutover that leaves a flaky tail of
physical blocks after T still present (so the synthetic can't show there), or an
un-retire with no fill (a gap the physical device now "owns" but has no data for). The
coverage gate keeps the *chart* honest regardless, but surface the interplay: pair
un-retire with an offer to sensor-fill, and when backdating a cutover make clear
whether existing physical blocks after T are kept or cleared.

### Billing stays decoupled
None of this touches billing: the summary's EV/Home split always uses the **dispatch**
figures across all history, whatever the charts draw. Neat consequence —
decommissioning the physical device is the one-click way to make a user's chart match
their bill going forward.

### Naming
The synthetic line must read as clearly *derived* — e.g. **"Car — from Octopus"** —
never a bare "EV" that looks like a second charger the user doesn't remember adding.
