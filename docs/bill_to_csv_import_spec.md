# Octopus bill → EMT CSV import — spec

A contract for an **external, Octopus-specific tool** that reads Octopus PDF bills and
produces CSVs EMT can import (Historical Import → CSV). The tool is kept **out of
EMT** (PDF parsing is fragile, format-churny, and dependency-heavy); EMT only ever
consumes the CSV, so this document is the stable target the tool builds against.

Validated against a real Intelligent Octopus Go bill (Dec 2024, 39 pages: summary +
one HH page per day) and a working reference parser (`octopus_bill_to_csv.py`).

---

## 0. Scope each file to a single MPAN

Group source PDFs (and every output CSV) by **electricity MPAN**, not just by date. A
folder can span a **house move** (a *different* MPAN) or a **meter exchange** (the *same*
MPAN, a new serial) — and those are exactly the boundaries EMT's period/site model keys
on, so per-MPAN output drops straight into the import's "confirm your sites" flow (region
+ site per MPAN). Read the MPAN from the bill ("meter point 2600002170611"). **Warn if a
single PDF contains more than one electricity MPAN**, and never merge two MPANs into one
CSV.

---

## 1. The CSV contract (what EMT accepts)

One file per channel (`import`, `export`). Headers, matched case/space-insensitively,
order-independent:

```
Start, End, Consumption (kWh), Unit Rate (p/kWh), Estimated Cost Inc. Tax (p), Standing Charge Inc. Tax (p)
```

Rules EMT enforces / assumes:

- **`Start` must be timezone-aware** — local time with the correct GMT/BST offset
  (`2024-12-03T00:00:00+00:00`, `2024-07-03T00:00:00+01:00`). A naive `Start` is
  **rejected per row**. Derive the offset per date (Europe/London).
- **Rate-first.** Fill `Unit Rate (p/kWh)` and leave `Estimated Cost Inc. Tax (p)`
  blank; EMT computes `cost = rate/100 × kWh`. (If a rate is present EMT ignores the
  Cost column.) Only fall back to filling Cost when no per-slot rate is known.
- **VAT-inclusive.** EMT stores VAT-inclusive figures throughout (its API import reads
  `costInclTax` / `value_inc_vat`). Bill HH tables are **pre-VAT**, so gross the rate
  (and any cost) up by the bill's stated VAT rate: `rate_inc = rate_pre × (1 + vat)`.
  Read the VAT % off the bill (`VAT @ 5.00%`); don't hard-code it. **This gross-up is the
  fallback** — see §1a for the exact-rate path that avoids it entirely.
- **Standing charge is summed per day by EMT**, then stamped on every block of the day
  and read once per day for billing. So put the **full daily (inc-VAT) standing on one
  row per day** (the first slot) and leave it blank elsewhere. Equivalent: `daily/48`
  on every row. **Never** the full daily value on every row (→ 48× overcharge). Octopus
  charges a full day's standing per calendar day, so **do not prorate** a partial first/
  last bill day.
- **Channel merge.** EMT parses `import` first, then merges `export` onto the existing
  rows by `Start`. Emit both channels with **identical Start/End** for the same slot.
- **Block size.** 30-minute (half-hourly). EMT's config block size may be 5/15/30; HH
  matches the Octopus grid and is what the import expects.

---

## 1a. Exact rates via the tariff API (preferred over the bill's rounded figures)

The bill's per-slot rate is rounded (`6.67p`) and pre-VAT, so grossing it up (§1) carries
a small dp error. Better, where an Octopus API connection exists: take **only the kWh and
the off-peak/peak *classification* per slot from the bill** (the part the bill uniquely
holds for old IOG history), and take the **exact inc-VAT unit rate + standing charge from
the tariff/products API**.

Why this works even for old history:

- The **~2-year retention wall is on the Measurements (consumption) API only.** The
  **products/tariff rate API is not retention-limited** — `get_unit_rates` /
  standing-charge lookups return the full historical series for any date, keyed on tariff
  code. So exact rates are available for dates the consumption API can't reach.
- The **account** (account number → `get_account` agreements) yields the **tariff code per
  date** per meter point. **EMT already builds this rate schedule** for its API import, so
  the exact-rate pricing belongs where that machinery already lives (see §9).

Mechanically: classify each bill slot as off-peak/peak by nearest-match to the two tariff
rates on the bill, then price it with the **API's** exact off-peak/peak rate for that date —
never the bill's rounded number. Standing likewise from the API. Fall back to the §1
gross-up only when no API is available.

---

## 2. Parsing model — branch by what the bill actually contains

Octopus bills differ by tariff. The tool must detect which case each bill (or each
tariff period within a bill) is, and parse accordingly.

### 2a. Intelligent / half-hourly tariffs — **transcribe** (exact)

IOG (and similar) bills include **one page per day**, each a full 48-slot table:

```
Period          Rate p/kWh   Consumption kWh   Cost p
00:00 - 00:30   6.67         3.59              23.928
18:30 - 19:00   6.67         2.88              19.168   ← off-peak OUTSIDE 23:30–05:30 = a real dispatch
05:30 - 06:00   23.84        0.00              0.000
23:30 - 00:00   6.67         0.48              3.213
```

Map directly to CSV rows:

- **Start/End** = the `Period` + the **day date from the page header** ("3rd December
  2024"), each with its offset. `23:30 - 00:00` **wraps** End to the next day's midnight.
- **Consumption (kWh)** = the kWh column.
- **Unit Rate (p/kWh)** = the rate column × (1 + VAT).
- The **off-peak/peak split is the real dispatch outcome** — the per-slot rate already
  reflects smart charges moved off-peak, including daytime slots. No modelling needed.

This is a faithful transcription: billing-accurate **and** dispatch-accurate, on history
older than the ~2-year API window. Parsing note: PDF text extracts as a flat stream;
anchor on the row regex `(\d2:\d2)\s*-\s*(\d2:\d2)\s+([\d.]+)\s+([\d.]+)\s+([\d.]+)` and
take the day date from `(\d+)(?:st|nd|rd|th)\s+(\w+)\s+(\d{4})`.

### 2b. Flat / simple-time-of-use tariffs without HH pages — **synthesise** (approximate)

When only the **Charges In Detail** tier totals exist (e.g. `6.67p/kWh 1040.5 kWh`,
`23.84p/kWh 0.7 kWh` + standing), distribute by tariff shape:

- **Flat (single rate):** spread the period's total import **evenly** across all blocks.
- **Dual-rate with defined windows (e.g. Go day/night):** distribute each tier's total kWh
  **evenly within that tier's time window** (night kWh across 23:30–05:30, day kWh across
  the rest) — i.e. per the summary's per-tier split.
- Emit the **per-tier rate** on each block (exact from §1a where available). The **period
  cost is exact per tier**; only the intra-window HH *shape* is synthetic. Flag as
  shape-approximated.

When the same period also has **export** (§4), place export first, then redistribute import
so the two channels never share a block — see §4.

> The tariff time-window table (product → windows + rates over time — Go's 00:30→23:30
> window change, Flux, Cosy, IOG cap, etc.) is the fiddly, maintenance-heavy core of the
> synthesis path. It is **not** needed for the transcription path (2a).

---

## 3. Standing charge

Read `N days @ Xp/day` from the summary per tariff period. Apply the **full daily**
(inc-VAT) standing to the first slot of **each calendar day** in that period; blank
elsewhere. Full charge on a partial day. See §1.

---

## 4. Export

The example bill gives export as a **period credit only** (`+£17.71`, no kWh, no HH
table — the HH pages are the import meter). So export is the one channel that is **not
directly reconstructable**:

- If the bill (or the tariff page) gives the **outgoing unit rate**, back out
  `total_export_kWh = credit / rate`, then distribute across the period.
- **Flat outgoing:** the credit is distribution-neutral, so place the kWh where it's least
  misleading — spread across **daylight** half-hours. Rate = the flat outgoing rate. Flag
  as reconstructed.
- **Agile outgoing:** use the per-slot breakdown if the bill has one; otherwise treat the
  period average like flat.

**Import/export mutual exclusivity.** A single meter shouldn't show import *and* export in
the same half-hour. So distribute **export first** (daylight), then distribute **import
into the remaining, non-export blocks** so the two channels never share a slot. This only
really bites the **flat-import + solar** case (flat import would otherwise spread across
daytime too); a **dual-rate** import is window-constrained to nights, so it rarely collides
with daytime export anyway. Emit export on **matching Start/End** so EMT merges it onto the
import rows (§1).

---

## 5. Reconciliation (built-in correctness gate)

Each bill self-checks — assert before writing, and refuse/flag on mismatch:

- **Per day (2a):** `Σ parsed HH kWh` ≈ the day page's `Total consumption` (the reference
  parser matches all 31 days to ≤0.05 kWh; page totals are 1-dp rounded).
- **Per period:** `Σ kWh` and the off-peak/peak split ≈ the **Charges In Detail** tier
  totals on the summary page; `Σ cost` ≈ the period subtotal.
- **Standing:** `days × daily` ≈ the summary standing line.

---

## 6. Multiple bills / a folder of PDFs

The goal is to point the tool at a folder of (possibly overlapping) bills and emit
continuous per-channel CSVs. Decisions to make explicit:

- **Overlap / re-issue.** Corrected or re-issued bills overlap originals. Pick a
  deterministic winner (latest **Bill Reference date**, or "actual" over "estimated"),
  and log what was dropped. Never silently merge two values for the same slot.
- **Estimated vs actual.** Octopus flags estimated periods; prefer actual. If an estimated
  period is later re-billed as actual, the overlap rule resolves it.
- **Continuity.** Concatenate day pages across bills into one series per channel; a missing
  day becomes a gap EMT can still fill separately.

---

## 7. Discoverability in EMT (optional, low-coupling)

Keep the tool a separate project. EMT may show a one-line pointer on the CSV import panel
**gated on `supplier == octopus`** ("Reconstruct history from your Octopus bills →"),
linking out to the tool. No code coupling — EMT's contract is this document.

---

## 8. Reference parser

`octopus_bill_to_csv.py` is a working reference for the **2a (IOG transcription)** path
against the Dec-2024 layout: it parses the 31 day-pages, grosses rate to inc-VAT, puts
standing once per day, writes tz-aware Start, and reconciles every day against the page
total. It is a reference for one layout, not the finished tool (no synthesis path, no
export back-out, no multi-bill overlap handling yet).

---

## 9. Where the EMT boundary sits — external parse vs in-EMT synthesis

Split the work along its natural seam:

**Stays external** (fragile, Octopus-specific, heavy deps): PDF text extraction, **MPAN
grouping** (§0), and pulling the raw facts out of each bill — per tariff period: the tier
totals + windows, the per-slot HH classification (2a), standing-days, export credit, and
the tariff/product name.

**Belongs in EMT** (generic, testable, and reuses machinery EMT already has):

- **Block synthesis** — distributing tier totals into HH by the §2/§4 rules (even / window
  / export-first-then-import). Pure and unit-testable; not Octopus-specific.
- **Exact-rate pricing** (§1a) — EMT already builds the agreement rate schedule for its API
  import; applying it to the CSV's kWh (instead of the CSV's rounded rate) is a small reuse,
  and keeps rates authoritative.

So the external tool feeds EMT an **intermediate**, per MPAN and per tariff period:

```
{ mpan, period_from, period_to, tariff_code?,
  tiers:   [ { rate_label, kwh, window? } ],      # window omitted ⇒ flat/even
  hh?:     [ { start, end, kwh, rate_label } ],   # present ⇒ transcription (2a), no synth
  standing_days: N,
  export?: { kwh? , credit?, rate? } }
```

EMT's **optional CSV sub-step** then: resolves exact rates (API, else the CSV's/bill's
rate), synthesises blocks where `hh` is absent, applies standing once per day, enforces
import/export exclusivity, and writes billing-accurate blocks flagged shape-approximated
where synthesised. A user with only a paper summary could hand-enter the `tiers` totals and
get the same treatment — no PDF, no tool. Keep it **optional and clearly on the CSV path**,
so it doesn't complicate the exact HH transcription route.

*Status: proposed. Not built. Captured here so the external tool targets a stable
intermediate rather than guessing EMT's internals.*
