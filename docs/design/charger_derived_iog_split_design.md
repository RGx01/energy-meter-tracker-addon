# BL-28 — Charger-derived IOG car/house + cap split for deep history

> _Status: design idea — not yet spec'd. Extends BL-12 (`historical_attribution_design.md`)
> from energy-only to energy + rate/cap. Cap model per `iog_6hr_cap_design.md`._

## Problem

The IOG car-vs-house split — and, on the capped 4-rate tariff, the split *within* the half-hour
a cap boundary falls in — is derived from Octopus's **completed-dispatch** records. Octopus exposes
those only for a rolling **~90-day** window (which BL-9's historical-dispatch sub-item can backfill
precisely). **Beyond 90 days**, or for an account with no Octopus dispatch integration, there is no
dispatch to reconstruct from: an imported history then shows **correct bills and off-peak/peak
rates** but **no car/house split**, and capped boundary slots collapse to a single blended-rate
segment rather than their real off-peak+peak bands.

Where the user has a **physical EV charger (Zappi, Ohme, etc.) that was already recording in Home
Assistant**, its charge profile is a proxy for the missing dispatch — it records *when* and *how
much* the car charged. That's the input this idea exploits.

## Method

Two layers, of increasing ambition:

1. **Energy split — already possible today (BL-12).** Point recorder attribution at the charger's
   HA sensor → reconstruct the car's kWh per half-hour → the car-vs-house **energy** split for
   history. Under the 4.4.0 segment model this is the natural "no `'ev'` segment → physical device
   authoritative" fallback.

2. **Rate / cap reconstruction — the new piece.** From the car's **full-day charge profile** plus
   the tariff's rate schedule, approximate the 4-rate split:
   - Walk the cap day (noon→noon, local), accumulating the car's charging against the **6-hour
     off-peak allowance**.
   - Price the EV portion at `ev_device_off_peak` up to the cap, `ev_device_peak` beyond (or on
     Boost); give the house the guaranteed off-peak window plus the out-of-window freebie *while
     within* the cap (mirroring the live model in `iog_6hr_cap_design.md`).
   - Emit the result as an **approximate segment set** for those historical capped slots.

## Why it is "approximate" (be honest in the UI)

- **Smart vs boost is unrecoverable for ANY historical reconstruction — not just this one.** The
  bump/boost `source` label lives only on the **planned/started** dispatch record, captured **live**
  (`_iog_slot_is_boost`: *"completed rows carry no source"*). Octopus retains only **completed**
  dispatches historically, with **no source** — so *whatever* the reconstruction source (the charger
  profile here, OR BL-9's 90-day completed-dispatch fetch), you cannot tell a smart dispatch
  (off-peak) from a manual boost (peak). Both must **assume off-peak** (as EMT already does
  conservatively for Ohme), which can read slightly cheaper than the real bill. The charger adds no
  disadvantage here — the ambiguity is a property of Octopus's retention, shared with BL-9.
- **Cap accounting is re-derived from charge shape, not a record** — an estimate of Octopus's own
  noon→noon 6-hour accounting.
- **Recorder granularity + timezone/DST.** HA long-term statistics are hourly for old history, and
  recorder TZ/DST quirks perturb the device layer.
- **The billed total (API measurement) stays authoritative.** This reconstruction only ever
  improves the **breakdown** (rate bands, car/house on the analytics screens) — it never changes the
  bill total, which is column-authoritative.

## Fit

- Slots into the 4.4.0 segment model's **no-dispatch fallback** point: where there's no
  dispatch-derived `'ev'` segment, derive an approximate segment set from the charger profile.
- **Flagged as reconstructed** (like other recorder-attributed data) and **reversible**.
- **Complements BL-9's 90-day dispatch backfill:** BL-9 recovers the real completed-dispatch
  *timing and energy* for the recent window (precise on *which* slots charged and *how much*); BL-28
  infers those from the charger for deep history (approximate). But **both share the smart-vs-boost
  ambiguity above** — neither can price a historical bump at peak from the record alone.

## Priority

**Low.** Niche: capped IOG users who happened to have a charger recorded in HA *and* want historical
breakdown fidelity beyond 90 days. Additive, estimate-quality, and bill-neutral. Not for 4.4.0.