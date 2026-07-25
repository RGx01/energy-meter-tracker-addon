# Region timeline for historical carbon — design sketch

*Status: design sketch (3.5.1+). Prerequisite for "historical carbon for imported blocks" (see ROADMAP 3.5.1). Written against the 3.5.0 schema.*

## The 3-stage historical backfill

Reconstructing history happens in three ordered stages, each depending on the last:

1. **kWh + cost** — the block skeleton: consumption and billed cost per half-hour, from the API import or a CSV (incl. the pre-filled gap CSVs). Standing charges + rates priced from the tariff schedule. *(Done.)*
2. **Historic carbon** — attribute regional carbon intensity to each imported block, using the **region timeline** (this doc) to know which DNO region applied when. Gated on stage 1 + a known region per period.
3. **Device apportioning** — split each block's grid import across sub-meters (EV, battery, heat pump) for the historical range, as the live path already does going forward. Gated on stages 1–2.

This document is the region timeline that stage 2 needs, plus the config-period plumbing (sites, postcodes, dates, splits) all three stages read.

## Goal

Let carbon backfill attribute the **correct DNO region** to every historical block — including imported (API/CSV) blocks that predate carbon recording — even when the user has **moved** or swapped supply point mid-history. Do it while storing **no personally identifying location data**.

## Key realisation: the timeline mostly already exists

We do **not** need a new region table. EMT already models config history with everything required:

- **`config_periods`** is already effective-dated: `effective_from`, `effective_to`, plus `site_name`, `supplier`, `change_reason`. A house move is already representable as a **new config period**.
- **`meters.postcode_prefix`** (main meter only) already stores the **outward postcode** used for carbon intensity — one value *per config period*.
- **`BlockStore.get_config_period_for_date(date_iso)`** already resolves the period effective at any date (the same mechanism billing uses for historical `billing_day`).

So the "region timeline" = `config_periods` (dated) × `meters.postcode_prefix` (per period). The only real gap is that **carbon reads one postcode, not the per-period one**:

```
# engine._get_postcode() today → the CURRENT main meter's postcode_prefix (single value)
# Carbon (live + backfill) uses that one value for ALL dates.
```

## Privacy model (per your steer)

Store the **minimum that resolves a region, and no more**:

- **`postcode_prefix`** — the **outward code only** (e.g. `SW1A`, `M1`, `EH8`). Never the full postcode. The Carbon Intensity API's `regional/intensity/{...}/postcode/{outcode}` accepts the outward code alone, and it only resolves to one of ~14 GB **DNO regions** — deliberately coarse. (Already how EMT stores it.)
- **`site_name`** — a **free-text label the user chooses** (e.g. "Home", "Old flat", "Rental"). Ambiguous by design; the user decides how identifying it is. (Already a `config_periods` column.)
- **Explicitly NOT stored:** full postcode, house number/street, Octopus account address lines, MPAN-to-address mapping. When we auto-derive from Octopus (below) we **truncate to the outward code at the boundary** and discard the rest before it ever hits the DB.

One addition worth making for provenance/gating (small):

```sql
ALTER TABLE meters ADD COLUMN postcode_source TEXT;  -- 'user' | 'octopus' | 'unknown'  (main meter only)
```

`postcode_source = 'unknown'` is the sentinel for "we have blocks here but no confirmed region" (the CSV case) — distinct from `postcode_prefix IS NULL` meaning "carbon simply not configured".

## meter_config impact

`meter_config` edits config periods, so this is where the region timeline is surfaced and maintained:

1. **Per-period region fields.** Expose `site_name` + `postcode_prefix` (+ derived DNO region name, read-only) on each config period row, not just the current one. Today the postcode is edited as a single current setting; it should be visible/editable **per period** on the config-period timeline.
2. **"Record a move / region change" = new config period.** Creating a period boundary with `change_reason = "Moved property"` (the `retired_reason` enum already uses this phrase) is the canonical way to record a region change. The new period carries the new `site_name` + `postcode_prefix`; the prior period's `effective_to` closes at the move date.
3. **Show the timeline.** A compact strip: `Home (SW1A) · Jul 2024 – Mar 2025 → New place (M1) · Mar 2025 – now`, so the user can see and correct the region history that carbon backfill will use.

## Carbon resolution change (the actual fix)

Add a per-date resolver and use it in the backfill:

```python
# BlockStore
def get_postcode_prefix_at(self, date_iso: str) -> tuple[str | None, str | None]:
    """(outward_code, source) for the config period effective at date_iso.
       Resolves via get_config_period_for_date → that period's main meter."""
```

```python
# engine historical carbon backfill (per window / per block)
outcode, source = store.get_postcode_prefix_at(block_start)
if not outcode or source == 'unknown':
    continue                      # leave carbon NULL — never guess a region
ci = fetch_regional_intensity(outcode, window)   # region resolved per block's own period
```

- **Live/forward carbon** can stay on the current period (already correct going forward) — this only changes **historical** attribution, which is where per-period matters.
- **Replace the blanket exclusion.** `get_missing_carbon_date_range()` / `get_block_starts_missing_carbon_in_range()` currently exclude `source LIKE 'imported%'`. Swap that blanket skip for **region-resolution gating**: an imported block is eligible iff its period resolves to a known outward code. Unknown → stays NULL (excluded).

## Populating the timeline

The import should reconcile the **whole** config-period history — sites, postcodes AND dates — not just stamp a postcode. **Dates and postcodes are authoritative from Octopus and applied automatically; the site name is the one human-supplied field** (we never store the address, so `site_name` is the only label a user recognises). That split drives the two flows below.

### API import (auto-derivable) → post-import confirmation UI

Octopus already gives us the boundaries; we just don't read the address yet.

1. **Derive.** Extend `derive_region_periods` to read each `account.properties` entry's **address → outward code** (truncated at the boundary) plus its **move-in / move-out** dates and a stable per-property key (e.g. a hash of the property id — *not* stored, used only to match discovered↔existing). `auto_discover` already reads `moved_out_at`.
2. **Reconcile, don't blindly write.** Diff the derived tenancy periods against the existing `config_periods`:
   - **dates + postcodes** that differ → queued to apply **automatically** (authoritative).
   - each derived tenancy that has **no matching site_name** yet = a **newly discovered site**.
3. **Confirm.** After import completes, show a **post-import confirmation panel** (Data Management / Billing history): a row per discovered period showing its **date range (read-only), region/outward code (read-only), block count**, and an editable **Site name** — pre-filled where we already have one, blank (placeholder "Name this site") for newly discovered ones. The user names any new sites and clicks **Apply**.
4. **Apply on confirm.** Only then do we create/split config periods at the tenancy boundaries and write `site_name` + `postcode_prefix (outcode)` + `postcode_source='octopus'`. Confirmation is deliberately the moment we mutate billing-affecting history (it needs the historical **split primitive** — the current gating prerequisite; `insert_config_period` only appends). Because an **MPAN is a fixed physical supply point**, each imported MPAN's data sits entirely inside one tenancy period, so its region is unambiguous once the period exists.
5. **Nothing new to confirm?** Single site, dates unchanged → no panel; the postcode is stamped silently (today's behaviour).

### CSV import (NOT derivable) → prompt to add config periods

A CSV is bare kWh/£: no MPAN, no address, possibly spanning a move — so there is nothing to auto-reconcile.

- After a CSV import, **prompt the user to add config period(s)** covering the imported span via the existing Billing-history period editor (now with the Postcode field), pre-seeded with the import's date range: they enter site name(s), outward code(s) and boundary date(s) → written with `postcode_source='user'`.
- If they decline / don't know: the covered periods stay `postcode_source='unknown'` → those blocks are **excluded** from carbon backfill (left NULL) with a one-line notice.
- Never infer a region for CSV data.

## Backfill re-arm interaction

The stale-`done` behaviour we found (marker `done:true` from before the import, never re-armed because imported blocks were excluded) is resolved by the gating swap: once region-resolvable imported blocks count as "missing carbon", `get_missing_carbon_date_range()` returns a range again and `_maybe_backfill_historical_carbon()` re-arms normally. Clearing the marker at end of import is then belt-and-braces.

## Edge cases / open questions

- **Sub-meters:** region is a **main-meter** property (whole-site grid); sub-meters inherit the site's region. No per-sub-meter region.
- **Overlapping / missing periods:** `get_config_period_for_date` already picks the latest `effective_from <= date`; ensure move-in/out boundaries don't leave an uncovered gap (fabricate a boundary period if Octopus reports a gap between tenancies).
- **Outward-code → DNO region** is many-to-one and stable; safe to resolve at fetch time (no need to store the region number, only the outcode).
- **Region unknown but user wants carbon anyway:** offer a one-click "use my current region for the whole history" with an explicit "assumes you haven't moved" caveat — cheap escape hatch that stays honest.
- **Data-generator / tests:** `test_data_generator` and the carbon backfill tests will need periods with `postcode_prefix` set to exercise per-period resolution.

## Build order / status

**Done (3.5.0 foundation):** `postcode_source` column; `get_postcode_prefix_at` per-date resolver; `outward_code` + `derive_region_periods`; single-region auto-apply (idempotent, one-time upgrade clobber); per-period **Postcode** field in the Billing-history editor.

**✅ 1. Historical config-period split primitive** — `split_config_period_at(period_id, split_date)`: original keeps the earlier half, a new period covers the later half with a deep copy of meters + channels, blocks reassigned by `block_start`, chain re-closed; validated + heavily tested against a synthetic moved-account fixture.

**✅ 2. Address capture in the probe** — `derive_region_periods` now emits reconcilable per-**tenancy** spans `{outcode, from, to, key, hint}`: distinct properties (even same outward code) stay separate; `key` is a non-reversible property hash for grouping/UI; `hint` (town) is display-only for the naming step; only the outward code is ever persisted.

**✅ 3. Post-import confirmation UI (API)** — `plan_region_reconciliation` (read-only planner: split dates + per-site block counts + name prefills + `needs_confirmation`) and `apply_region_reconciliation` (splits at move boundaries, stamps region + site name). The engine stashes the plan in `store_meta` after import when confirmation is needed; endpoints `GET /api/region/reconcile`, `POST …/apply`, `POST …/dismiss`; a **"Confirm your sites"** panel on Billing history collects site names and applies. Single unchanged site → silent apply, no panel.

**✅ 4. CSV post-import prompt** — pre-filled **gap CSV** + **blank template** downloads (`csv_import.gap_template_csv` / `blank_template_csv`, DST-correct local timestamps); and a **region-editable** reconciliation panel after a CSV import (`plan_csv_reconciliation` → the same "Confirm your sites" panel, but the user supplies the outward code since CSV has no provenance). Skipping leaves the data blended into the covering period, carbon-excluded. The reconcile apply now splits at **both** ends of a bounded span, so a CSV sub-range is stamped without touching neighbouring periods.

**✅ 5. Carbon backfill consumes the resolver (stage 2 lit up).** `get_missing_carbon_date_range()` / `get_block_starts_missing_carbon_in_range()` now gate on `_CARBON_ELIGIBLE` (live/reconstructed always; imported only when its period has a `postcode_prefix`) instead of the blanket `imported%` skip. The historical backfill resolves each block's region via `get_postcode_prefix_at` and **fetches Carbon-Intensity per region per window**, so a move attributes old blocks to the old region — not the current postcode. `rearm_carbon_backfill()` clears the backfill marker whenever a region is newly assigned (reconcile apply, manual per-period postcode edit, or the one-time probe), so the next scheduler tick re-scans and fills the now-eligible imported range. Region-unknown imports stay NULL (never guessed).

**Instance identity vs per-period site label (decided).** `config_periods.site_name` is overloaded: it's the per-period **property** label the region timeline wants, AND the **backup-folder / instance identity** (`instance.py` names the supervised `/share` backup dir by the *current* period's site-slug; ownership is by the stable `instance_id`, so a rename only migrates the folder). Decision: **the instance follows the CURRENT site.** A historic import only ever labels **earlier** (past-address) periods; the reconcile apply **never renames the active period** (guarded — it still stamps the region on it). The current site name is set through normal config, and a real future move creates a new *current* period, so the instance/backup identity moves with you correctly. Net: historic imports leave the instance + backup folder untouched.

**✅ 6. Pre-import site confirmation (API path — now the canonical flow).** The reconcile above splits/labels periods *within* the existing timeline and cannot create a period earlier than the oldest existing one — so a *past address* discovered *after* an import had nowhere to hang its region. The canonical order is therefore **discover → confirm → import**: name the sites first, create their covering periods up front, then run the import so every block lands in its correct (regioned, carbon-eligible) period from the first write — no post-hoc split or block reassignment.

- **`create_covering_period(from, to, outcode, site_name)`** — the missing prepend primitive: inserts a period over an *empty* date range by cloning a reference period's meters + channels and stamping region + site. Nothing to reassign (pre-import), so it's simpler and safer than a split.
- **`plan_pre_import_sites(derived)`** — read-only classifier for the wizard: the latest tenancy (`to=None`) is the **current** site, shown read-only and prefilled from the active period's `site_name` (instance identity); earlier tenancies are **past** sites the user names. `needs_confirmation` is True only when there's a past site — a never-moved account confirms nothing and goes straight to import.
- **`apply_pre_import_sites(sites)`** — extends the **active** period back to the current tenancy's move-in (backward only, never renamed) and stamps its region, then creates a covering period per past tenancy. Idempotent; the current-site region respects a user-set value.
- **Engine + endpoints:** `discover_pre_import_sites()` (read-only coro) behind `GET /api/historical/site-plan`; `POST /api/historical/site-plan/apply` (backs up, applies, re-arms carbon, rebuilds the chain, sets a `preimport_sites_applied` marker). The import-completion region probe **consumes that marker and stands down** when set, so it never re-stamps the pre-import layout; single-site imports (no marker) still run the post-import probe as before.
- **Wizard:** the API-import panel calls `site-plan` after *Preview plan*; on a move it renders "Confirm your sites" (current read-only, past sites named), and *Start import* first POSTs the confirmed sites, then starts. The post-import reconcile panel remains for CSV (no provenance) and as a fallback.

**Stage 3 (future):** device apportioning over the historical range — split each imported block's grid import across sub-meters, as the live path already does.

The schema and the multi-site period primitives are now complete; the remaining work is Stage 3, not new storage.
