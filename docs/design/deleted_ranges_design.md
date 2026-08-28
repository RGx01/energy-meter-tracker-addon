# Deliberate-deletion persistence (deleted_ranges tombstone) — design note

> _Status: **SHIPPED (verified 4.5.5 audit).** Built as the `deleted_ranges` tombstone table (schema + INSERT on deliberate delete + `override_tombstone` re-create gate in `block_store.py`). Originally signed off ready-to-build; BL-8 **phase 2**. All sign-off questions
> resolved (Q1–Q6 below; Q3 confirmed: all deletes tombstone, fillability advisory
> only). Additive and **billing byte-identical**: the tombstone gates only block
> *creation* paths and never touches a stored per-slot figure or any billing/chart
> read. Targeted for a 4.2.3 follow-up (or 4.2.4)._

> **One-line summary:** a manual delete records the deleted span in a new
> `deleted_ranges` table; BL-8 backfill, the settlement sweep, and the outage
> gap-scan consult it and skip tombstoned spans, so a deliberate delete stays
> deleted. A separate, explicit **re-import** action removes the range from the
> table and immediately refills it from the authoritative source.

## Why this exists (the conflict, stated precisely)

BL-8 ("outage backfill", shipped 3.2.0) has exactly one rule: *a settled interval
exists at the supplier but there is no local block → create one.* Its "no local
block" test is literal **row presence** — `get_block_by_start(...) is None`
(`block_store.py`). It has no concept of *why* a row is absent, so a slot the user
deliberately deleted and a slot lost to an EMT outage are indistinguishable to it.

The 4.2.3 gap-recovery work (`get_oldest_gap_start` + the poll-window gap-floor in
`kraken_ingester._window`) made this sharper: it deliberately pulls the poll window
back to any interior hole so BL-8 can fill it. Its own code comment names the case —
*"the block gap seen after a delete-and-reimport. BL-8 fills it once the window
covers it."* That behaviour is **correct and wanted for a genuine outage**, but it
means a bare delete cannot stick: the next poll (or an HA-reconnect-triggered
startup poll, as observed 2026-08-14 16:56) re-creates the whole day.

Nothing in BL-8's original requirements described deliberate deletion. This note
adds that missing concept.

### Why not soft-delete-in-place

Considered and rejected. Nulling a row but keeping it would stop BL-8's *create*
path (the row is no longer "missing"), but the **settlement UPDATE path**
(`upsert_kraken_block`) finds the surviving row and writes the DCC figure straight
back into it, and the unsettled probe (`get_oldest_unsettled_block_start`) treats a
nulled row as unsettled and pulls the window back to re-settle it. So soft-delete
would still resurrect, and fixing it means gating the settle / unsettled / pass-2
**write** paths *and* excluding the marker from chart/count/gap **read** paths —
i.e. touching the billing surface. Hard-delete + tombstone keeps every billing and
settlement *read* untouched by construction and gates only the handful of block
*creation* points. That is the decisive reason for choosing it.

## Guiding principles

1. **A deliberate delete is a user instruction, and it wins.** Once a span is
   tombstoned, no automatic mechanism repopulates it — **regardless of whether the
   API could fill it**. Only an explicit user action (re-import) brings it back.
   This is the invariant that makes deletes stick; it is not conditional on
   fillability (see Q3).
2. **Billing stays byte-identical.** The tombstone is consulted *only* at
   block-creation and window-scan points. No billing total, chart, reconcile, or
   settled figure changes. Deleted rows remain genuinely absent (today's behaviour),
   so aggregates are unaffected.
3. **Genuine outages are unchanged.** A hole with *no* tombstone is still filled by
   BL-8 exactly as designed — phase 1 is preserved, not reverted.
4. **Fail open, not loud.** If the tombstone table is missing/unreadable, the code
   behaves exactly as today (fills gaps). A tombstone can only ever *suppress*
   creation; it can never itself write or delete a block.

## Data model

New table (created idempotently in the schema-migration path alongside the existing
`_b_cols` upgrades):

```
deleted_ranges (
    id          INTEGER PRIMARY KEY,
    meter_id    TEXT NOT NULL,     -- 'electricity_main', a sub-meter id, or '*' = all meters
    start_utc   TEXT NOT NULL,     -- inclusive, ISO 'YYYY-MM-DDTHH:MM:SS' (UTC, block_start grain)
    end_utc     TEXT NOT NULL,     -- EXCLUSIVE upper bound
    created_at  TEXT NOT NULL,
    reason      TEXT               -- 'user_delete' | 'user_purge' | free text
);
CREATE INDEX idx_deleted_ranges ON deleted_ranges (meter_id, start_utc, end_utc);
```

Notes:

- **Range, not per-slot.** A delete already operates on a `[from,to]` UTC window
  (`delete_blocks_for_date_range` → `local_date_range_to_utc_bounds`), so storing the
  span is natural and compact; a multi-day delete is one row, not 48×N.
- **`meter_id = '*'`** encodes the common "meter=all" delete without fanning out to
  one row per meter. The overlap check treats `'*'` as matching every meter; a
  meter-scoped delete stores that meter id (and its sub-meters — see edge cases).
- **No `cleared_at` / no retention (Q5).** A cleared tombstone has no value, so
  re-import simply **deletes** the covering rows (or splits them for a sub-span —
  Q4). The table only ever holds *active* tombstones.

### Membership test (the one hot predicate)

```
is_tombstoned(meter_id, slot_utc) :=
   EXISTS (SELECT 1 FROM deleted_ranges
           WHERE (meter_id = :meter_id OR meter_id = '*')
             AND :slot_utc >= start_utc AND :slot_utc < end_utc)
```

A store helper `is_slot_tombstoned(meter_id, slot_utc)` plus a bulk
`tombstoned_slots_in(meter_id, start, end) -> set[str]` (so a poll filters a whole
window with one query, not one per slot).

## Integration points (exhaustive — these are the only code that changes)

**Write (record the tombstone):**

- `delete_blocks_for_date_range(...)` — after the successful delete, insert a
  `deleted_ranges` row for the resolved `(meters, utc_start, utc_end)`. Reuses the
  meters it already resolves (`_resolve_delete_meters`) and the UTC bounds it already
  computes. Applies to `reconstructed_only` (history-rollback) deletes too — see Q3.
- `api_blocks_delete` / the purge worker — no change beyond calling the above.

**Suppress (consult the tombstone before creating):**

- `create_backfill_block(...)` — return `None`/skip if the target slot is
  tombstoned. Single chokepoint BL-8 uses, so gating here covers the import and
  export backfill branches at once. Add a `summary["skipped_tombstoned"]` counter.
- `get_oldest_gap_start(...)` — treat a tombstoned slot as "not a gap" so the poll
  window is not pulled back to it. Prevents the window from even reaching the span.
- `_maybe_run_settlement_sweep()` — the daily sweep also creates blocks for aged
  gaps; it must apply the same `create_backfill_block` gate (covered automatically
  if it routes through `create_backfill_block`; verify it does).
- `get_oldest_unsettled_block_start(...)` — belt-and-braces: a tombstoned span holds
  no rows, so it can't register as unsettled; no change expected, noted for the audit.

**Read (billing, charts, reconcile, settle): UNCHANGED.** Rows are truly absent, as
today.

## Automatic vs explicit fill — the gate discriminates (answers "can I still Fill missing data → fill gaps?")

**Yes.** The tombstone suppresses only the *passive* poll; every *user-triggered*
fill still works and is treated as an explicit re-import. The codebase already drew
this line — `resolve_history_gaps` carries the comment *"Deliberately explicit, not
automatic: we cannot distinguish an outage gap from blocks the user deleted on
purpose … an automatic sweep would silently resurrect them."* The tombstone is the
missing piece that lets the two finally be told apart.

The discriminator is **targeted vs blanket**, not merely "user-initiated" — because
a *blanket* user action (bulk "recover all gaps", whole-history import) must NOT
mass-undelete every tombstone with one click. Mechanism:
`create_backfill_block(..., override_tombstone: bool = False)`.

- **Respect the tombstone (`override_tombstone=False`) — skip tombstoned slots:**
  - the kraken poll's `missing_block` branch (`advance_cursor=True`) and
    `_maybe_run_settlement_sweep` (automatic), **and**
  - **blanket** user fills: bulk "Gap fill / recover all" (`resolve_history_gaps`
    over the whole gap set) and whole-history API import. These fill genuine outage
    holes but step over deliberately-deleted ranges.
- **Override + clear the tombstone (`override_tombstone=True`) — fill and un-delete:**
  a **targeted** fill of a specific range that intersects a tombstone — the per-gap
  "🩹 API →" button (`/api/backfill/fill-gap`), a bounded **range** import, a CSV
  window (`apply_csv_import`), and the new **re-import** button. These clear the
  covering tombstone (sub-span split, Q4) as they fill, so no present data is left
  under a stale tombstone.

So a *targeted* **Fill missing data → this gap** behaves identically to **re-import
this range**; a *blanket* **recover all gaps** behaves like the auto poll and leaves
tombstones intact. The re-import button is just a convenience entry onto the same
targeted "clear tombstone(range) + fill" primitive.

Gap-list visibility: the "Fill missing data" list is built from `find_block_gaps()`,
independent of the poll's `get_oldest_gap_start`, so tombstoned holes still **appear**
there for the user to target individually — labelled **"deleted — fill to restore"**
so they read as intentional, not as a fault. The bulk "recover all" planner, by
contrast, excludes tombstoned holes from its slot count and its fill set.

## Re-import (lifting a tombstone) — Q1 immediate, Q2 full UI, Q4 sub-span

Deliberate, explicit, and the mirror of delete:

- New endpoint `POST /api/blocks/reimport` `{from_date, to_date, meter_id?}` on the
  Data Management page, next to delete. It:
  1. Removes the tombstone cover for the requested range — **splitting** any
     tombstone that only partially overlaps (interval subtraction: delete the
     covered row, re-insert the ≤2 remainder rows outside the re-imported span). **(Q4:
     sub-span supported in v1.)**
  2. **Immediately** kicks a bounded backfill for that range — force a poll whose
     window covers it (or a direct settled-figure fetch) so the user sees it
     repopulate without waiting up to 6 h. **(Q1: immediate.)**

- **UI (Q2: full).** The Data Management page gains:
  - a **"Deleted ranges"** list of active tombstones (meter, span, when deleted),
  - a per-row **Re-import** button (and a range-input re-import for a sub-span),
  - each row annotated with an **API-fillability advisory** (see Q3): *"re-importable
    from API"* vs *"predates API coverage — re-import cannot recover this"*, with the
    button warning/disabled accordingly.

This is also the maintainer's new test workflow: **delete → re-import** replaces the
old **delete → wait for auto-refill**, and is more honest about intent.

## Edge cases

- **Meter scope + sub-meters.** A meter-scoped delete already cascades to that
  meter's sub-meters (`_resolve_delete_meters`). The tombstone records the same set
  (the resolved list, or `'*'` for a full delete). `is_tombstoned` matches a
  sub-meter against both its own id and any `'*'` row.
- **Outage inside a tombstone.** Per principle 1, the tombstone wins — the span
  stays empty even if EMT is later offline across it. (Agreed; flag if outages
  should re-open it.)
- **Partial overlap of a new outage with a tombstone.** Only tombstoned slots are
  suppressed; slots outside the range still backfill normally (per-slot predicate).
- **Interaction with `rate_corrected` / imported history.** Independent flags:
  `rate_corrected` protects a *present* block from reconcile; the tombstone
  suppresses *creation* of an absent one. No conflict.
- **Restart persistence.** The table is on disk in `blocks.db`, so it survives
  restart, HA reconnect, and the WAL checkpoint — closing the exact hole seen on
  2026-08-14 (delete, then reconnect-triggered poll refilled).
- **Live current block.** A tombstone covering "now" would suppress the live slot;
  delete UIs operate on past ranges, and the create-gate never touches the live
  finalise path (it doesn't route through `create_backfill_block`).
- **Backup/restore.** `deleted_ranges` is part of `blocks.db`, so it travels with a
  backup automatically; a restore correctly restores that DB's tombstone state.

## API-fillability advisory (supports Q2 UI + Q3 decision)

A read-only helper `range_api_fillable(meter_id, start_utc, end_utc) -> bool` that
answers "could the consumption API return settled data for this span?" — essentially
`start_utc >= (now - api_history_horizon)` (the Kraken/DCC consumption reach, the
same horizon `backfill_days` already encodes), with no writes. Used purely to
annotate the deleted-ranges list and gate the re-import button. **It never clears a
tombstone on its own** (see Q3) — it only informs the user's explicit re-import.

## Testing plan

- Store: insert tombstone; `is_slot_tombstoned` true inside / false outside; `'*'`
  matches any meter; bulk `tombstoned_slots_in`; re-import deletes a whole tombstone;
  re-import of a sub-span splits it into the correct remainder row(s).
- `create_backfill_block` returns None + increments `skipped_tombstoned` for a
  tombstoned slot; creates normally otherwise.
- `get_oldest_gap_start` does not return a tombstoned hole; still returns a genuine
  (non-tombstoned) hole.
- Integration: seed blocks, delete a mid-range day (writes tombstone), run a poll —
  assert the day is **not** re-created and billing totals for surrounding days are
  unchanged; then re-import (removes tombstone) and assert it repopulates.
- Regression: a genuine outage hole with no tombstone still backfills (phase-1
  intact).
- Targeted fill: a per-gap / range / CSV / re-import fill over a tombstoned range
  (`override_tombstone=True`) fills it **and** clears the covering tombstone (whole,
  and sub-span split); a subsequent auto poll does not re-suppress the now-present,
  un-tombstoned data.
- Blanket fill: a bulk "recover all gaps" (`resolve_history_gaps`, whole-history
  import) **skips** tombstoned ranges (leaves them deleted) while still filling a
  genuine, non-tombstoned outage hole in the same run.

## Decisions (sign-off round 1)

- **Q1 — re-import trigger: IMMEDIATE.** Re-import forces a bounded fetch at once, so
  it's testable on demand (not "wait for the next scheduled poll").
- **Q2 — UI: FULL.** Deleted-ranges list + per-row (and sub-span) re-import, with the
  fillability advisory on each row.
- **Q4 — partial re-import: YES in v1.** Sub-span re-import splits the tombstone via
  interval subtraction.
- **Q5 — retention: NONE.** No `cleared_at`; re-import deletes/splits tombstone rows.
- **Q6 — naming: BL-8 phase 2**, cross-referenced from `ROADMAP.md` and `README.md`.

### Q3 — CONFIRMED: all deletes tombstone identically; fillability is advisory only.

**Decision: YES — every delete (including `reconstructed_only` imported-history)
writes a tombstone; API-fillability never auto-clears one.** The carve-out below was
declined.

Rationale: the tempting rule "if the gap-scan finds a deleted hole the API can fill,
clear the tombstone and refill" re-introduces the original bug, because *recent*
deleted data is always API-fillable — so recent deletes would never stick, which is
the whole thing phase 2 exists to fix. Keeping the tombstone absolute preserves the
invariant, and the fillability intelligence you want is delivered at the explicit
**re-import** step instead (the advisory above): the user sees whether a range is
recoverable and decides. Imported-history deletes tombstone the same as any other,
so behaviour is consistent and predictable across delete types.

**The one carve-out that would break the invariant** (call it if you want it):
imported-history (`source LIKE 'imported%'`) deletes do **not** tombstone, i.e. a
deleted imported span is allowed to auto-refill from the API when in range. Only
choose this if you explicitly accept that that specific class of delete can
repopulate on its own.