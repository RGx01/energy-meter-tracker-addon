# Intelligent Octopus 6-hour-cap tariff (IOG-SMB-TOU) — design note

## Background
Octopus is migrating Intelligent Octopus Go customers to a new time-of-use
tariff that introduces a **daily 6-hour cap** on off-peak EV charging. The tariff
API structure changed with it (ref: BottlecapDave/HomeAssistant-OctopusEnergy
issue #1708), and it is now GA — customers are being migrated live.

## The API change
The tariff (`IOG-SMB-TOU-…`, e.g. `E-1R-IOG-SMB-TOU-25-12-12-H`) **drops the
`standard-unit-rates` link** and replaces it with four separate rate buckets:

| rel                          | endpoint                          | meaning                    |
|------------------------------|-----------------------------------|----------------------------|
| `day_unit_rates`             | `…/day-unit-rates/`               | general usage, peak/day    |
| `night_unit_rates`           | `…/night-unit-rates/`             | general usage, off-peak    |
| `ev_device_peak_unit_rates`  | `…/ev-device-peak-unit-rates/`    | EV charging, beyond cap    |
| `ev_device_off_peak_unit_rates` | `…/ev-device-off-peak-unit-rates/` | EV charging, within cap |

`standing-charges` is unchanged. Any integration that reads `standard-unit-rates`
(EMT and BCD both did) gets **no rates** on a migrated meter → silent £0 pricing.

## What's CONFIRMED vs NOT
- **Confirmed:** the four-bucket structure and endpoint names (real payloads in
  #1708).
- **NOT confirmed:** exactly when `ev_device_off_peak` vs `ev_device_peak`
  applies. Community + BCD are explicit: *"waiting for properly confirmed
  definitions from OE… a few details still aren't fixed."* Rough shape: the first
  **6 hours of IOG-led EV charging per day** are off-peak, with **midday** the
  day-boundary; beyond the 6h it's EV-peak. Whether the cap interacts with the
  standard off-peak window is unclear.

## Staged plan (mirrors BCD)
### Step 1 — structural rate support  **[DONE — this release]**
`build_rate_schedule`: when `standard-unit-rates` is empty, fetch
`day-unit-rates` + `night-unit-rates` and merge (each carries its own half-hour
periods, so concatenation rebuilds the full day/night schedule). This restores
**correct general-usage billing** for a migrated meter — the bulk of the bill.
`get_unit_rates` gained a `rate_type` param to reach any bucket.

Interim EV handling: the dispatch overlay (#253) keeps repricing dispatched EV
slots to the **general off-peak (night) rate** via `off_peak_rate_near`. This is
a deliberate, bounded approximation — NOT the `ev_device_*` rates and NOT the 6h
cap. It won't be exactly right, but it's defensible and far better than £0.

The fail-loud guard (#1708) remains the fallback: it now fires only if *neither*
`standard-unit-rates` *nor* `day/night` produce a schedule.

### Step 2 — EV device cap  **[DEFERRED — blocked on OE confirmation, but the model is now settled]**
Build only once OE publishes the rules, but the intended mechanism is:
1. Fetch `ev-device-off-peak-unit-rates` + `ev-device-peak-unit-rates`.
2. Track dispatched EV charging per "cap day" (**noon→noon**) against the 6h cap.
3. In the reconciliation, map a `started` slot to `ev_device_off_peak` within the
   cap and `ev_device_peak` beyond it — replacing the interim night-rate repricing
   for the EV sub-meter.
Watch OE's definitions and BCD's staged releases before building; do not guess at
billing rules that aren't finalised.

## Cap measurement — how BCD does it, and how EMT should  *(BCD source read 2026-08)*
BCD's live implementation (`intelligent/__init__.py`) gates cap enforcement behind
an **opt-in `enforce_intelligent_cap`, default OFF** — because the exact rule
isn't confirmed, guessing risks being wrong, so by default it reprices *every*
smart-charge dispatch slot to off-peak with no cap at all. When enabled:
- `is_within_intelligent_cap` is a hardcoded **`hours <= 6`**.
- The cap-day is **noon→noon** (`get_dispatch_hours_for_intelligent_day`: start =
  12:00 today, or 12:00 yesterday if before noon; +24h). Uses HA local `now()`.
- Crucially it counts **dispatch-window slots rounded to the half-hour** —
  `round_down(start)` / `round_up(end)`, overlaps merged, `cap_to_current=True`
  clipping each window's end to "now". So a dispatch that delivered 10 minutes
  inside a slot still contributes the **whole rounded slot**. It counts in the
  same unit it reprices (whole HH slots), which is internally consistent but
  **over-counts real charging** — it can trip the 6h cap before the car has
  charged for 6 hours, and in `PLANNED_AND_STARTED` mode planned-but-not-charged
  dispatches burn cap too. This is a proxy, not a measurement.

**The cap basis: the union of `completed` dispatch windows, at their actual
start/end times.** Not a slot count; not delivered energy; not `planned`; not even
`started`. A `completed` dispatch is OE's own **acknowledgement that a real
dispatch happened**, and it carries the precise (second-level) start/end. OE's cap
is reckoned off its dispatch ledger, so the faithful measure is the wall-clock
**union of completed dispatch intervals** in the noon→noon window (merge overlaps;
never *sum* per-row durations — see storage gap below).

Why `completed` specifically, borne out by prod (21–22 Jul 2026, three shapes):

| pattern | example | OE-acknowledged | count to cap? |
|---|---|---|---|
| planned / `started`, never `completed` | 21st 00:30–04:30 — planned −3.42 & `started` each slot, **0.000 kWh** delivered, no completed | no | **no** |
| `completed`-only (charger over-run) | 21st 15:30 — completed −0.25, **never planned**, over-ran the 15:27–15:30 dispatch | yes | **yes** |
| planned + tiny `completed` | 22nd 03:30 — completed −0.11 (~3 min at the 3 kW >94%-SoC limit) | yes | yes, at true length |

The rule sorts all three: it **auto-excludes the planned/started phantoms** (21st
overnight: OE scheduled *and* "started" a full charge, the car took nothing, no
completed ever landed — so nothing should hit the cap), **includes over-runs**
(they carry a completed record even though they were never planned), and **counts a
tiny completed at its real short length**, not a rounded block. Note `started`
alone is unsafe — the 21st overnight was `started` with zero delivery — so the
signal is `completed`, not `started`.

**Load-bearing recording gap (engine.py):** EMT currently **discards the
`completed` window**. `_completed_dispatch_slot_energy` parses each completed
dispatch's `start`/`end` but returns only `{slot: energy}` (and averages the energy
across covered slots, `delta / len(covered)`); there is **no
`_completed_dispatch_slot_bounds`** mirroring the planned one, and
`record_dispatch_history(…, "completed"/"started", …)` is called **without**
`raw_start`/`raw_end`, so they store NULL. EMT keeps only the *planned* window
(often a full HH block). This is why 22nd 03:30 shows the full planned `03:30–04:00`
and can't be told apart from an over-run tail of the 03:00 dispatch. **Fix:** add
`_completed_dispatch_slot_bounds` and thread `raw_start`/`raw_end` into the
completed `record_dispatch_history` call and the `dispatch_slots` completed
annotation. This is the prerequisite for computing the cap the way OE does.

### Prod-DB validation (39 days, 2026-06-27 → 2026-08-05, Myenergi/Zappi)
Union of actual dispatch intervals vs the EV sub-meter (actual charging):

| measure | hours |
|---|---|
| all dispatch slots × 0.5h (slot-count) | 219.5 |
| **union of real dispatch intervals, all states** | **216.3** |
| union of real intervals, **excluding planned** | **147.4** |
| completed-state only | 92.4 |
| **EV sensor — actual charging** (305 slots >0.02 kWh) | **152.5** |

Reads: (a) sub-slot precision is *small here* — real start/end times trim only
219.5→216.3h, because Myenergi reports dispatches essentially HH-aligned (162
distinct raw windows, only 32 longer than a slot); on a ragged sub-HH feed it
would matter more. (b) excluding planned lands at 147.4h, ~3% off the sensor's
152.5h — but note this table measures interval *coverage*, and the true cap basis
is narrower still (see completed-windows rule above). (c) `completed`-only shows
only 92.4h here **because EMT drops the completed window** (falls back to the
slot / the live state snapshot isn't settled) — the recording fix is exactly what
makes completed-window accounting accurate rather than understated. (d) 99.5% of
EV energy fell inside a dispatch slot — only ~9.5h/3.8kWh charged with no dispatch
— so dispatch coverage of real charging is excellent; the discrepancy is entirely
dispatch-with-no-charge (the planned/started phantoms the completed rule drops).

**Storage gap for a to-the-minute cap.** Because raw bounds are exploded per-slot
and inconsistent, EMT can't currently answer "actual dispatch duration to the
minute" by a simple query. If the cap needs that precision, BL-11 should be
extended to retain **one clean interval row per dispatch** (precise start/end,
lifecycle state), rather than per-slot rows with ambiguous raw bounds. On an
HH-aligned feed the union-of-slots approximation is fine (±3h/39 days); the
one-row-per-dispatch store is what a sub-HH supplier would need.

### Under-delivery — where even "dispatch, planned excluded" breaks (worked example: 21 Jul 2026)
The 39-day aggregate hides a per-day failure mode that matters. On **21 Jul 2026**
(prod), whole-day actual EV charge was **0.43 kWh**, yet the dispatch ledger shows:

- **Afternoon:** a `completed` dispatch with raw window **15:27–15:30 — 3 minutes**
  (planned −0.34, delivered −0.09; sensor 0.10 kWh in the 15:00 slot). BCD's
  `round_down(15:27)`→15:00 / `round_up(15:30)`→15:30 books this 3-minute dispatch
  as a **full 30-minute block** of cap. The *next* slot (15:30, 0.33 kWh) is real
  charging with **no dispatch at all**.
- **Overnight:** a `completed` slot (raw 23:30–00:30, planned **−3.42**, delivered
  only **−0.38**) followed by a four-hour wall of `planned` dispatches
  (00:30→04:36, planned −3.42 each, **none completed**) — with the sensor flat at
  **0.000** throughout. Octopus scheduled a big overnight charge; the car took
  almost none. In `PLANNED_AND_STARTED` mode that ~4.5h burns the cap for ~0 kWh.

Lessons: (1) block-rounding a 3-minute dispatch to a full slot is BCD's over-count
in the extreme — the completed window (15:27–15:30) is the right length. (2) The
overnight wall (planned/`started`, **never `completed`**, 0.000 kWh) is precisely
the phantom class the **completed-windows rule excludes** — nothing there should
touch the cap, and the rule gets that for free. So the earlier "under-delivery
means only delivered energy is safe" framing was a wrong turn: the fix isn't to
switch the *basis* to delivered energy, it's to count the **`completed` windows**,
which already ignore un-acknowledged plans.

Delivered energy / billed cost keep two narrower roles: adjudicating the
**boundary slot** (where the running total crosses 6h), and a **cross-check /
under-delivery flag** — a `completed` dispatch whose delivered energy is ≪ its
window (an OE-acknowledged dispatch the car barely took) is worth surfacing, and is
where the settled billed cost is the final arbiter. This is very likely the
mechanism behind the private-forum "under-delivery" reports: if OE counts the
**dispatched (completed) block** regardless of delivery, an acknowledged-but-empty
dispatch still spends the 6h. EMT is uniquely placed to test it — for a settled day
it holds the **completed dispatch windows + real sub-meter + billed cost** side by
side, once the completed window is actually retained.

So, the design in one line: **cap = union of `completed` dispatch windows (actual
start/end) per noon→noon day, off-peak up to 6h, EV-peak beyond**; pre-settlement,
`planned`/`started` are the (over-counting) live estimate, corrected to `completed`
+ billed cost at settlement. Prerequisite: capture the completed window (above).

**The one genuinely fuzzy part is a billing-mapping problem, not a data problem:**
the cap is in dispatch-hours but billing is per half-hour, so the **boundary slot**
— the HH slot the running total crosses 6h inside — has to resolve to off-peak or
EV-peak, and OE's treatment there is undocumented. Everything up to the boundary
is clean; only that straddling slot needs adjudication, and EMT has the right tool:
the **settled billed cost** for that exact half-hour via `recover_measurement_costs`
(see BL-20) tells us what OE actually charged. So: completed-window cap for the
estimate, billed cost as the tie-breaker on the boundary slot at settlement.

## Open questions for step 2
- Does the 6h cap count *dispatched* time only, or delivered charging? (Leaning:
  dispatched — the union of `completed` dispatch windows; validated at the pattern
  level on 21–22 Jul. The under-delivery/over-run cases are handled by keying on
  `completed`, not by switching to delivered energy.)
- Is the noon boundary local or UTC? (BCD uses HA-local `now()`; confirm.)
- **Boundary slot:** when the running total crosses 6h partway through a slot, is
  that slot off-peak or EV-peak? (Undocumented → adjudicate from billed cost.)
- Does EV-peak (beyond cap) still apply inside the standard night window, or does
  night always win?
- How does this map onto EMT's whole-house main meter vs the EV sub-meter?

---

## Step 2 addendum (2026-08-07) — duration estimator, the user switch, rate-flip & display

Folds in the 4.1.1 charge-duration work and the switch/rate-flip/display thinking.
**Still unvalidated — this account is not on the capped tariff, so nothing below is
checked against a real capped bill.** The completed-window capture prerequisite
(BL-11) is now shipped, so the data is available to build on.

### A. Keep two different "hours" separate
- **Cap-hours (the £ basis, unchanged):** union of `completed` dispatch windows per noon→noon day. Decides which EV slots bill `ev_device_off_peak` vs `ev_device_peak`.
- **Actual charging time (display / cross-check only, NOT a billing input):** the new estimator below. It answers "how long did the car actually charge", which is *not* the same as cap-hours and must never be wired into pricing.

### B. Refined actual-charging estimator (validated 7 Aug 2026)
```
active_kW = max over delivered slots of (slot_kWh / 0.5h)     # fullest slot ≈ true charge power
est_charge_hours = total_delivered_kWh / active_kW
```
Use the **peak** slot, not an average — a fully-saturated slot yields exactly the
active power; partial slots read low, so the max is the tightest estimate and an
average biases the time high. Carries the invariant
`est_charge_hours ≤ dispatch_minutes ≤ n_slots × 0.5h` (charging can't exceed time
dispatched) — worth a test assertion.

**7 Aug prod charge (5 slots, 11.41 kWh):** peak slot 6.36 kW → **est 1.79 h** vs a
**measured 1.77 h** (±1 min). The window figure (`dispatch_minutes`) read 2.50 h —
a 43% over-read, almost all of it one 0.26 kWh trickle slot counted as a full 30 min.
Guards: needs one near-full slot to anchor the power; clamp inferred power to a sane
ceiling (~7.4 kW 1-phase / 22 kW 3-phase) so a spike slot can't collapse the estimate;
if no slot anchors, fall back to the window figure labelled an **upper bound**.

**Its role in the cap:** it's the honest card figure, and a **cross-check** — a large
gap between cap-hours (dispatched) and est charging hours is the under-delivery
signature (OE-acknowledged dispatch the car barely took), exactly the case that can
spend cap for ~no charging. It doesn't move £; billed cost still does.

### C. No user switch — detect the tariff, let billed cost be the safety net
EMT already **identifies the capped tariff by signature**: a migrated meter drops
`standard-unit-rates`, and the `IOG-SMB-TOU-…` product/tariff code is visible on the
agreement (engine.py). That is a reliable enablement signal, so **no manual
`enforce_intelligent_cap` toggle is needed** — cap handling turns on automatically
for periods whose tariff code is the capped product, and stays off for everyone else.

This drops BCD's opt-in, and it's safe to do so because EMT's safety net is different
from BCD's: **settled billed cost is authoritative**. BCD keeps the switch off
because a wrong cap *rule* would misprice with nothing to catch it; EMT only uses the
cap model to **predict live/provisional** prices, and the Measurements billed cost
re-rates every slot at settlement via the verify pass. So a slightly-wrong live
estimate self-corrects to the penny — the tariff code is enough to gate it, and the
bill is the backstop instead of a human toggle.

- **Storage:** the capped-tariff flag is **derived from the config period's tariff
  code**, not a stored boolean — one source of truth, no drift. Cap params (6 h,
  noon→noon) stay constants until OE confirms.
- A tariff change over time is already a new config period, so enablement follows the
  period timeline for free.

### D. Rate-flip mechanics (capped tariff detected, EV sub-meter only)
Per noon→noon cap-day, walking the EV sub-meter's dispatched slots in time order:
1. Accumulate cap-hours = running union of `completed` dispatch windows.
2. Slot fully **within** 6 h → `ev_device_off_peak`.
3. Slot fully **beyond** 6 h → `ev_device_peak`.
4. **Boundary slot** (running total crosses 6 h inside it) → *settled:* the
   `recover_measurement_costs` billed cost adjudicates; *live:* price off-peak
   provisionally and correct at settlement.
The **house remainder stays on day/night general rates** throughout — the cap only
touches the EV portion. **Settled billed cost is always the final arbiter** (accuracy
rule): the cap computation is a *predictor* for live/provisional blocks; once
Measurements returns the real per-slot cost, trust it and reconcile. The existing
deferred verify pass is the right vehicle.

### D½. Ex-VAT capture must go per-channel under the split (BL-23 follow-on)
The 4.2 ex-VAT work (BL-23/BL-24) captures `cost_exc`/`rate_exc` at settlement in
`_rerun_pass2_for_settled_block`, and the Billing summary + data-table ex-VAT view read it.
**Today that capture stamps only the MAIN import channel**, and the data table derives every
column's ex-VAT from that one main VAT ratio. That is correct *while all import shares one
tariff*, because VAT is a flat 5%: the exc/inc ratio is uniform across every rate band, so
`EV exc = EV inc ÷ 1.05` regardless of the EV's rate.

Under the split this is **still numerically fine for the ex-VAT total/columns** — a different
`ev_device_*` *rate* does not change VAT — so nothing renders wrong. **But the per-channel
STORED figure should be captured from each channel's own rate.** When settlement prices the EV
portion at `ev_device_exc` and the house remainder at day/night exc, stamp `cost_exc`/`rate_exc`
per import channel (main + EV sub-meter), not just the main, so the stored per-device ex-VAT is
sourced from the device's *own* published exc rate. This future-proofs:
- the section-E **"distinct line/colour for the capped (EV-peak) kWh"** ex-VAT breakdown, and
- any per-device ex-VAT view that reads stored `cost_exc` directly (rather than the shared ratio).

**Mechanically:** extend the `_rerun_pass2` exc block to loop each import channel and scale that
channel's *resolved* inc rate by its tariff's exc/inc ratio (house schedule for the remainder,
`ev_device` schedule for the EV). Build it **with** the ev_device rate support in step 2 — the
`ev_device_exc` schedule doesn't exist until then, so doing it earlier would just replicate the
main. Until then, the shared main VAT ratio keeps the visible ex-VAT correct to a rounding whisker.

### E. Display in billing & charts (capped tariff only)
- **Smart-charging card:** surface cap consumption — e.g. "IOG cap: 4.2 of 6 h used" or "1.1 h over cap → EV-peak" — **paired with the actual-charging estimate** so the user sees dispatched-vs-charged, not one misleading number.
- **Billing breakdown:** split the EV sub-meter cost into off-peak vs EV-peak portions, with a **distinct line/colour for the capped (peak) kWh**, so a peak-priced *overnight* slot reads as "over the cap", not as an error.
- **Charts (rate line):** the EV rate line **steps up to `ev_device_peak`** for beyond-cap slots, with a marker on the cap-crossing; the day/night general lines are unaffected.
- **Provisional marker:** live/pre-settlement capped slots flagged "provisional — may re-rate at settlement", cleared when billed cost lands.
- When the tariff isn't the capped one, **none of this renders** — the card/charts stay exactly as today.

### E½. The "all-red-until-the-schedule-completes" problem (observed live)
**Symptom (7 Aug):** during an active charge, before the dispatch schedule was complete, every slot showed **red / priced at peak**, then flipped to off-peak once the schedule resolved. That red→green flash reads as "you're being overcharged" and is the card's worst live behaviour.

**Cause:** pricing **defaults a slot to peak** and only reprices it off-peak once a dispatch is *known* for it (planned/started/completed). At the leading edge of a live charge the dispatch data hasn't landed yet, so the slot sits at the peak default until the overlay catches up — the same leading/edge-slot lag the verify pass fixes at settlement, but here it's visible live on the card.

**Fix — flip the live default to off-peak-provisional inside an active dispatch:**
- If a slot falls inside an **active planned/started dispatch window** (a smart charge in progress), price/label it **off-peak *provisionally*** rather than peak. On IOG the overwhelming default for a dispatched slot is off-peak — predict that, not the exception.
- Give provisional slots their **own visual state** — not the red peak colour and not the solid confirmed-off-peak colour (hatched/amber "pending"). **Red must only ever mean a *confirmed* peak / over-cap slot**, never "not known yet".
- The estimate / `dispatch_minutes` / slot count keep counting **completed** only; provisional in-progress slots are shown but flagged, not folded into totals.
- At settlement the billed cost confirms each slot → provisional clears to confirmed (off-peak, or EV-peak if genuinely over the 6 h cap).

Net: the card should move **peak → provisional-offpeak (charge starts) → confirmed (settles)**, never **peak → offpeak**. Under the cap, the rare genuine EV-peak slot (beyond 6 h) then becomes the *only* red an overnight charge ever shows — exactly the signal we want it to carry.

### E¾. Billing model confirmed live (2026) — and why it favours EMT
Verified against Octopus's own 2026 material (Charge Cap live ~March 2026; off-peak
cuts April 2026):
- **The 4-rate split is real and live:** house on day/night, EV on
  `ev_device_off_peak`/`ev_device_peak` — exactly this doc's four buckets. Whole-home
  off-peak window is 23:30–05:30; the 6 h cap applies to **EV smart-charging only**.
- **The bill does NOT itemise EV.** Octopus rates the split internally but shows **no
  separate EV line** on the statement — it tells EV drivers to use their **Ohme app**
  for charging costs. **This is the gap EMT fills:** reconstruct and display the
  house-vs-EV breakdown Octopus withholds, for **any** charger, not just Ohme. The
  split-billing news makes EMT *more* useful, not redundant.

**Boundary-slot mechanics, restated with this in mind.** Because a single HH can carry
**two rates at once** (house portion at day/night + EV portion at an EV rate), a mixed
HH is *always* an effective-rate blend — the 6 h-boundary slot is just the one where
the **EV** rate transitions mid-slot. Consequences for EMT:
- EMT already prices per-portion (EV grid-share vs house remainder), so mixed/boundary
  HHs are the same shape it handles today — not a new case.
- The **settled per-HH cost (Measurements) is authoritative** and encodes whatever OE
  actually did on the boundary; EMT reads it, never recomputes it.
- **Display rule:** never snap a mixed/boundary HH to a single rate bucket — show its
  **effective p/kWh**. Live, predict provisional off-peak for the EV portion and let
  settlement correct.

**Watch items (unconfirmed):**
- *"OE stopping PDF bills"* — **not verified** (Aug 2026 search found the split + cap,
  no PDF-withdrawal). Likely a conflation of "no itemised EV on the bill". If true, it
  only affects the **historical** bill→CSV reconstruction (`bill_parser`), not live
  billing. Confirm with a firm source before acting.
- *EV cost via API* — OE clearly computes the EV split; if it ever surfaces an EV-rated
  cost field, EMT could read it authoritatively instead of estimating. Opportunity, not
  threat — watch the schema (the deprecation CI would flag additions).

### F. Updated open question
- **Dispatched-window hours vs actual-charging hours for the cap?** Today's data shows they diverge materially (2.50 h dispatched vs 1.79 h charged on one charge). The design keys the cap on *completed windows* (dispatched), but the estimator now lets EMT **test which one OE's billed cost actually matches**, the first day this account (or a tester) runs on the capped tariff and exceeds 6 h. Detection is by tariff code; billed cost reconciles live estimates at settlement, so no manual enforcement gate is needed.

---

## Dispatch-derived EV sub-meter (no charger sensor) — can ship BEFORE the cap

A completed dispatch's `delta` is Octopus's own per-half-hour EV energy. Summing
the completed deltas gives an **EV consumption series with no charger sensor at
all**, from which `house = main grid import − EV`. This is the generic,
any-charger EV/house split — and it's the same `delta` the cap prices, so it's
shared groundwork.

### Validated against a real CT-clamp EV meter (prod, 19 days, 2026-07-19→08-07)
| measure | value |
|---|---|
| completed-dispatch EV energy | 299.1 kWh |
| actual `ev_charger` (Zappi CT clamp) | 302.1 kWh |
| aggregate coverage | **99.0%** |
| slots with EV energy but **no** completed dispatch | **0** (0.0 kWh) |
| per-slot ratio delta ÷ clamp (127 charged slots) | **median 0.9965, mean 1.0004** |

Reading: there was **no non-dispatched charging** in the window, and the per-slot
ratio straddles 1.0 with no systematic bias — so the ~1% aggregate gap is **CT-clamp
measurement scatter, not missed energy**. For a user who charges under smart control,
the dispatch-derived series is as accurate as a CT-clamp sub-meter (and comes off
Octopus's settled data rather than a clamp). **Behavioural caveat:** a user who
manual/boost-charges outside smart dispatch *would* show non-dispatched slots (that
count > 0) and be under-counted — accuracy tracks charging discipline, so surface a
"charged outside a smart dispatch — not counted here" note when it's non-zero.

### How it fits
- A **new reversible attribution source** (`dispatch_derived`), exactly like the recorder-based device split: writes an EV sub-meter (+ house remainder), main meter stays authoritative, **billing totals unchanged** — same invariant. Historical backfill runs straight off the stored `dispatch_history` completed rows.
- **Rules:** completed-only (exclude planned/started phantoms); grid-clip a solar-concurrent dispatch (mostly moot overnight); IOG/dispatch tariffs only.
- **Why it matters:** OE *rates* the EV/house split (4-rate) but doesn't itemise it — this gives that split to any IOG user with **no charger integration**, which the bill (and the "use your Ohme app" answer) doesn't.

### Sequencing
**Ships independently of, and before, the cap.** It's attribution + display — it does
**not** depend on OE confirming the cap rules, and it changes no billing totals. It
also *de-risks* the cap: the cap prices this exact EV portion, so having the
dispatch-derived EV device (and its validation harness against a real clamp) in place
first gives the cap a trusted substrate. Suggested order: this device → then the cap's
rate-flip on top of it.