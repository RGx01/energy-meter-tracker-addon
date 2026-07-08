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

### Step 2 — EV device cap  **[DEFERRED — blocked on OE confirmation]**
Build only once OE publishes the rules:
1. Fetch `ev-device-off-peak-unit-rates` + `ev-device-peak-unit-rates`.
2. Track dispatched EV energy per "cap day" (midday→midday) against the 6h cap.
3. In the reconciliation, map a `started` slot to `ev_device_off_peak` within the
   cap and `ev_device_peak` beyond it — replacing the interim night-rate repricing
   for the EV sub-meter.
Watch OE's definitions and BCD's staged releases before building; do not guess at
billing rules that aren't finalised.

## Open questions for step 2
- Does the 6h cap count *dispatched* energy only, or all EV-device draw?
- Is the midday boundary local or UTC?
- Does EV-peak (beyond cap) still apply inside the standard night window, or does
  night always win?
- How does this map onto EMT's whole-house main meter vs the EV sub-meter?