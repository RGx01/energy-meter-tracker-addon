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