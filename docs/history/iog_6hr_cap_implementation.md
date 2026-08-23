# Intelligent Octopus 6-hour cap (BL-9) — as-built implementation

Companion to `iog_6hr_cap_design.md`. That note is the *design* — the tariff, the
four rules, the cap measure. This note records how the split was **actually built**
in 4.3.0: the modules, the seam, the storage, the upgrade backfill, the billing-summary
render, and the ex-VAT consistency work that fell out of shipping it. Read the design
note first for the "why"; this one is the "where" and the "as-built how".

The guiding constraint throughout: **additive and off by default.** For a non-IOG
account nothing here fires, and inc-VAT billing figures for every existing tariff stay
byte-identical. The cap model is a live/provisional predictor; settled per-half-hour
cost from DCC remains authoritative, and the split apportions that settled total rather
than replacing it.

## The shape of the pipeline

A block's grid import flows through five stages, each additive:

1. **Classify** (`iog_cap.py`, pure) — given the slot, the off-peak window, the cap-day
   boundary and the dispatch EV energy, decide the EV and Home bands and their rates.
2. **Carve at the seam** (`engine._apply_iog_split`) — run the classifier on a finalising
   / settling block and stamp the EV/Home split onto the import channel.
3. **Persist** (`block_store`) — five new columns carry the split; write path and both
   read paths surface them.
4. **Backfill** (`engine._run_historical_iog_split_backfill`) — carve the split onto
   history that was priced before the split shipped.
5. **Render** (`energy_charts`) — aggregate the stored split per rate band and draw the
   EV/Home rows under "Import — total grid".

## 1. The pure core — `iog_cap.py`

All cap arithmetic lives here with no I/O, so it is exhaustively unit-testable
(`tests/test_iog_cap.py`). The cap-day is a **noon→noon** local day
(`CAP_DAY_ANCHOR_HOUR = 12`); the cap is `CAP_HOURS = 6.0`.

- `cap_day_key(ts_utc, tz_name)` — the noon→noon cap-day a UTC instant belongs to.
- `merge_intervals` / `cap_usage(completed_intervals, tz_name)` — union the `completed`
  dispatch windows per cap-day and report `{used_hours, over, boundary_utc}`, where
  `boundary_utc` is the instant the running union first reaches 6 h.
- `cap_day_boundaries` — the per-cap-day boundary instant, keyed by cap-day.
- `_within_cap_frac(start, end, boundary_utc)` — the fraction of a slot that falls
  **before** the boundary (1.0 fully within cap, 0.0 fully over, a fraction if it
  straddles).
- `_band(frac)` → `off_peak` / `peak` / **`mixed`** — `mixed` iff `0 < frac < 1`, i.e.
  the slot straddles the boundary. This tag is the discriminator that keeps a genuine
  cap-boundary block out of the clean rate bands (see §5).
- `classify_slot(...)` → `{ev, house, ev_offpeak_frac, house_offpeak_frac, boundary}`
  — the four-rule classifier.
- `price_import_split(...)` / `price_slot(...)` / `compute_iog_split(...)` — turn a
  classification into EV/Home kWh, cost and rate. `compute_iog_split` is the single
  entry point the engine calls; it returns `imp_kwh_ev` / `imp_cost_ev` / `imp_rate_ev`,
  the blended `imp_rate` / `imp_cost` (capped only), and the `classification` bands.

## 2. Rate schedules — `kraken_rates.py`

- `RateSchedule.is_off_peak(ts, tol)` — the agreement-driven guaranteed-window primitive
  (reads the low-rate band from the schedule periods, never a hard-coded 23:30–05:30).
- `build_ev_device_schedules(client, product, tariff)` → `(off_peak, peak)` — fetch the
  `ev_device_off_peak` / `ev_device_peak` rate schedules. Additive and non-fatal: a
  failure leaves the account uncapped, never breaks pricing.

## 3. The engine seam — `engine._apply_iog_split`

Helpers sit just after `_snap_to_slot`:

- `_iog_slot_ev_kwh(block_start, chosen_kwh)` — grid-clipped completed-dispatch EV energy
  for the slot (`min(Σ completed delta, grid import)`).
- `_iog_slot_is_boost` — a bump/boost dispatch (`_IOG_BUMP_SOURCES`) bills EV at peak.
- `_iog_cap_day_boundary(block_start, tz_name)` — the cap-day boundary via `iog_cap`.
- `_iog_site_tz()` — the site IANA tz for the noon→noon day.

`_apply_iog_split(imp_ch, block_start, block_end, chosen_kwh, overlay_rate, tz_name)`
carves the split onto the import channel **in place**. It no-ops unless the import rate
schedule is present and there's EV energy this slot, so non-IOG and non-dispatched slots
are untouched. **Capped detection is automatic** — the presence of both
`ev_device_off_peak` and `ev_device_peak` in `_kraken_rate_schedules`. On an **uncapped**
IOG account it only sets `kwh_ev`/`cost_ev`/`rate_ev` and the bands (the "primer": inc
figures unchanged, EV carved at the same rate). On the **capped** tariff it additionally
re-prices `rate`/`cost` to the blended four-rate values.

It is hooked in two places, both gated so an authoritative user correction or rate
override is never stomped:

- `finalise_block` — after the overlay, on the result channel.
- `_rerun_pass2_for_settled_block` — after the settled cost is set, only when
  `_override_rate is None`.

`_refresh_kraken_rate_schedules` fetches the `ev_device_*` schedules (gated on an IOG
tariff), so capped detection self-populates at startup.

## 4. Storage — `block_store.py`

The migration adds five columns to `blocks`: `imp_kwh_ev`, `imp_cost_ev`, `imp_rate_ev`
(REAL) and `imp_ev_band`, `imp_home_band` (TEXT). They are carried by:

- **Write:** `_block_rows` maps `imp.get("kwh_ev")` etc.; both `_insert_block_rows` and
  `_insert_block_rows_replace` persist them.
- **Read (full):** `_row_to_block` surfaces them back onto the import channel.
- **Read (lightweight):** `get_blocks_lightweight` — the trimmed fetch the chart/billing
  render actually uses — **must** select and surface them too. It didn't at first, which
  silently dropped the split from the billing summary (the render read a channel with no
  `kwh_ev`); the fix mirrors the ex-VAT precedent two lines up, "the lightweight chart
  fetch must match or the whole view silently falls back". This is the single most
  important as-built lesson: **two read paths exist, and both must surface any new
  billing column.**

## 5. Historical backfill — the upgrade path

The seam only fires on finalise/settlement, so every block priced *before* the split
shipped — the whole existing dispatch history — has NULL split columns and the summary
shows no split on upgrade. `_run_historical_iog_split_backfill` (scheduled once per
install by `_maybe_backfill_historical_iog_split`, marker `iog_split_backfill_state`)
walks those blocks and carves the split in place, mirroring the ex-VAT backfill: paged,
resumable via a cursor, cooperative (one transaction per chunk, yields between batches).

It is **additive only** — it writes just the five split columns behind a NULL-only guard
and never re-prices `imp_rate`/`imp_cost`, so the settled inc bill stays byte-identical
even on a capped tariff (forward re-pricing of capped history is deliberately left as a
separate decision; the marker is scope-versioned so it can re-arm later). Supporting
store methods: `get_blocks_missing_iog_split` (main-meter blocks on a completed dispatch
slot, missing the split), `count_blocks_missing_iog_split`, `set_blocks_iog_split`. It
defers without marking done until the import schedule is ready, and marks done with
nothing to do when there's no dispatch history.

## 6. Billing summary render — `energy_charts.py`

`calculate_billing_summary_for_period` builds four accumulators alongside the existing
`main_import_raw`: `ev_by_rate` and `home_by_rate` (per-band kWh / cost / derived
ex-VAT), and `ev_transition` / `home_transition` for the `mixed`-band boundary blocks.
EV comes from the stored `kwh_ev`; Home is the remainder of **every** main block, so the
two reconstruct the grid total. A `mixed` block is routed to the transition bucket, never
to a per-rate band — this is what keeps a genuine cap-boundary block distinct from
settlement-rounding jitter.

`_bill_split_rows(summary, currency, exc)` renders EV and Home interleaved per band under
**"Import — total grid"** (wired in both the inc-VAT branch and the ex-VAT `_bm` branch;
returns `None` when there's no EV, so the caller falls back to the plain rate rows).
**"Breakdown by meter" is untouched** — the grid-total section shows the billed dispatch
split, the per-meter section shows physical devices.

Two polish behaviours live in the render/aggregation:

- **Ex-VAT derivation + fallback.** The ex-VAT figure per block is `cost_ev × (block
  cost_exc ÷ cost)` — each block's own exc/inc ratio, exact even across a VAT change.
  When a not-yet-settled block has no stored `cost_exc`, the derivation falls back to
  `inc ÷ (1+VAT)` from the VAT calendar rather than contributing £0 (which otherwise
  diluted the displayed ex-VAT rate until every block settled).
- **Clean-band collapse.** Every settled half-hour returns at a fractionally different
  unit rate (0.323091 / 0.323092 / 0.323097 → 0.3230 / 0.3231 / 0.3232). `_bill_split_rows`
  folds rate keys within `_SPLIT_BAND_EPS` (0.0015 — sub-penny) into one band, exactly as
  the bill-method view already does, so a band isn't shattered into look-alike rows. The
  epsilon is far below the gap between real IOG bands, and the `mixed` transition bucket
  is never rate-grouped, so the collapse can't merge a genuine cap-boundary block.

## 7. Ex-VAT consistency — reconcile re-stamp + repair

Shipping the split surfaced a latent bug in the settlement reconciliation. When
`reconcile_dispatch_overlay` reverts/restores a slot it rewrites `imp_rate`/`imp_cost`,
but it used to leave `imp_rate_exc`/`imp_cost_exc` at the **old band's** value — a stale
ex-VAT figure inconsistent with the new inc rate (e.g. an off-peak exc rate on a block
reverted to peak). Invisible until the split rendered per-band ex-VAT rates, then it
showed a wrong rate and understated Total (exc).

- **Forward fix** (root cause): the reconcile UPDATE now re-stamps the exc columns from
  the new rate the same way pass-2 does (`new_rate × published exc ÷ inc`), and NULLs
  them when no exc schedule covers the slot (so the view falls back to inc ÷ (1+VAT) and
  the exc backfill re-derives later — never stale).
- **Repair** (existing rows): `block_store.repair_stale_exc` finds blocks whose stored
  exc/inc ratio is implausible (below 0.80 — no real VAT band reaches there; 5% is 0.952,
  25% is 0.80) and re-derives `imp_rate_exc`/`imp_cost_exc` from the VAT calendar, tagging
  them `tariff-repair`. Run once per install by `_maybe_repair_stale_exc` (marker
  `stale_exc_repair_state`); idempotent, since a repaired row is no longer implausible.

## 8. Charts — coverage-based, decommission-aware

The Billing-tab charts draw **one** EV representation via a per-slot coverage gate
(`energy_charts._dispatch_ev_slot_map`, `_ev_meter_id`): the physical EV device where it
has a block, and the synthetic dispatch EV only in slots the physical device doesn't
cover. It keys on **actual block coverage**, so it self-heals across
retire / un-retire / sensor-fill with no timeline stitching. Decommissioning the physical
EV device (`retire_meter` / `retired_at`; engine skips writing its blocks from the cutover;
UI in `meter_config.html`) hands those slots to the synthetic from the cutover, fully
reversibly.

The chart EV series takes its kWh, cost **and** rate line from the **same stored split as
the bill** (see §9): `_dispatch_ev_slot_map` prefers the block's `imp_kwh_ev`/`imp_cost_ev`/
`imp_rate_ev`, and the day chart plots the EV rate line at `imp_rate_ev`. So the EV line
**diverges from the house line** the moment the cap pushes charging to peak; on an uncapped
account EV and Home share the off-peak rate, so it correctly shows no divergence. The
re-partition stays display-only — house = main − EV, so the grid total and the "Direct
import" segment are unchanged.

## 9. One EV/house split, everywhere — the stored columns are the source of truth

The EV/house cost split is produced in exactly one place (the seam, §3) and stored on the
block (§4); **every surface that shows it derives from those columns**, falling back to the
dispatch-derived pro-rata carve *only* where a block has no stored split (un-backfilled /
non-IOG history). This matters on a **capped** account: the pro-rata carve prices EV at the
block's blended rate, which disagrees with the bill's 4-rate figures — so a surface that
computed its own split would show a different EV cost than the statement. The consumers, now
unified:

- **Bill — "Import — total grid"** reads the stored split directly (§6).
- **Bill — "Breakdown by meter" and the day chart** both flow through
  `energy_charts._dispatch_ev_slot_map`, which prefers the stored split — one change point
  covers both.
- **Usage Stats and Usage Insights** (`server._dispatch_ev_split_by_bucket` /
  `_aggregate_usage`) prefer a per-slot stored map and tier the EV at `imp_rate_ev`.

On an **uncapped** account the stored split equals the pro-rata carve (EV and house share
the rate), so every surface is byte-identical to before — the unification only changes what
**capped** users see, and it makes all surfaces agree with the bill. `main_import_raw`,
`total_cost` and the Total Bill are never touched: the split only apportions a total that
already exists.

## Test inventory

`test_iog_cap` (core), `test_ev_device_rates`, `test_iog_seam` (wiring, uncapped
byte-identical + capped re-price), `test_cap_split_columns` / `_writepath` / `_readpath`
/ `test_cap_band_columns` (storage), `test_iog_split_backfill` (upgrade backfill +
lightweight surfacing), `test_billing_split_agg` / `_render` / `test_billing_ev_breakdown`
(summary), `test_billing_split_polish` (ex-VAT fallback + band collapse),
`test_reconcile_exc_restamp` (forward fix), `test_stale_exc_repair` (repair),
`test_ev_coverage_gate` (charts gate), `test_day_chart_ev_rate_line` (rate-line
divergence), `test_ev_split_prefers_stored` + `test_server` additions (§9 unification —
every surface prefers the stored split, uncapped byte-identical).

## Invariants (must hold)

- Non-IOG accounts: no split fires; every inc-VAT figure byte-identical.
- Uncapped IOG: split is pure attribution — inc rate/cost unchanged, EV + Home
  reconstruct the grid total exactly.
- Capped IOG: settled per-half-hour cost is authoritative; the four-rate model is the
  live estimate and converges to it.
- Any new billing column must be surfaced by **both** `_row_to_block` and
  `get_blocks_lightweight`.
- The EV/house cost split has ONE source — the stored `imp_*_ev` columns (§9). Every
  surface (bill, charts, Usage Stats/Insights) derives from them; the pro-rata dispatch
  carve is a fallback for blocks without a stored split and equals the stored value on
  uncapped accounts, so no surface re-computes a split that could diverge from the bill.
- Credential values are never entered or handled by tooling; all patches are applied by
  the maintainer.
