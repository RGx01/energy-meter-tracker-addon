# EMT documentation map

Start here. This folder holds the development guide, the roadmap, and the design
notes behind shipped features.

**One rule above all:** changes must not alter existing billing results (the
kWh / cost / carbon already stored for past history). `run_tests.sh` (1,850+
tests) is the guard, and every accuracy-sensitive change should add a test that
pins the expected output.

## Start here

- **[CONTRIBUTING.md](CONTRIBUTING.md)** — how to report bugs, branch, test, and open a PR. Includes "Adding support for another supplier".
- **[DEVELOPMENT.md](DEVELOPMENT.md)** — architecture overview, runtime modes, storage schema, known limitations.
- **[ROADMAP.md](ROADMAP.md)** — what's shipped and what's planned (BL-* backlog items).

## Design notes (rationale behind shipped features)

These record *why* a feature is built the way it is. They are point-in-time
specs — the code is the source of truth; read these for context.

- `historical_import_design.md` — API/CSV backfill of full history.
- `region_timeline_design.md` — per-period region (outward postcode) for historical carbon.
- `historical_attribution_design.md` — reconstructing a device's past usage from HA's recorder.
- `intelligent_rates_design.md` — IOG dispatch overlay and off-peak pricing.
- `dispatch_validation_design.md` — validating smart-charge dispatch slots.
- `measurements_cost_recovery_design.md` — recovering exact billed cost from the Measurements API.
- `deleted_ranges_design.md` — BL-8 phase 2: deliberate deletions persist (tombstones) and only a targeted re-import restores them.
- `bill_to_csv_import_spec.md` — reconstructing history from Octopus PDF bills.
- `mode_ui_design.md` / `wizard_design.md` / `historical_import_wizard_design.md` — setup/mode UX.
- `data_management_vision.md` — the Data Management area's direction.
- `iog_6hr_cap_design.md` — IOG 6-hour charge cap (BL-9); model confirmed, back-end build in progress.

## History (archived)

Kept for the record; describes work that is fully done and, in places, code
that no longer exists.

- `SQLITE_MIGRATION_PLAN.md` — the JSON→SQLite migration (executed; the migration shim was removed in 4.0.0).
- `DEVELOPMENT-3.md` — development notes from the 2.10.x era and the 3.0.0 Kraken design.

## Suggested folder layout

If you want the current/archive line to be obvious in the tree:

```
docs/
  README.md              (this file)
  CONTRIBUTING.md
  DEVELOPMENT.md
  ROADMAP.md
  design/                (the "Design notes" above)
  history/               (SQLITE_MIGRATION_PLAN.md, DEVELOPMENT-3.md)
```

`apply_p1_doc_moves.sh` (delivered alongside this) performs those moves and adds
a one-line status header to each design note. Review it before running.
