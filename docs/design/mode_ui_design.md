# EMT 3.x — Data-Source Mode & Billing-Source UI Design

> _Status: Shipped — design note kept for rationale; the code is the source of truth._

**Status:** DRAFT for review. No code written yet — this is the spec to refine
before building next session.

**Author context:** Written after the 3.0.0 upgrade-mode-detection work. The
engine now stores a `data_source_mode` (`unset` / `cad` / `cad+api` / `api` /
`api+mini`) and a separate global `billing_source` (`dcc` / `cad`). The UI does
not yet let a user *see* or *change* the mode, and the billing-source toggle is
currently misplaced in Data Management.

---

## 1. The problem

Two related settings are currently in the wrong places (or no place):

1. **`data_source_mode`** — set once by the setup survey, then invisible and
   unchangeable. A user who picks the wrong mode, or whose setup changes (adds a
   CAD, gets a Mini, leaves Octopus), has no way to change it without flattening.

2. **`billing_source` (DCC vs CAD)** — currently a toggle on the **Data
   Management** page. This is conceptually wrong: "which figures drive my bill"
   is a meter/data-source decision, not a data-management *operation* (like
   backup, delete, correct). It sits next to destructive tools, which is both
   confusing and slightly dangerous.

These two settings are also **coupled**: `billing_source` only makes sense in
certain modes. If you have no DCC access (`cad` mode), "bill from DCC" is
meaningless; if you have no local reads (`api` mode), "bill from CAD" is
meaningless. Today nothing enforces or expresses that coupling.

---

## 2. The two concepts (keep them distinct)

It's tempting to merge these into one control. They must stay separate, because
they answer different questions:

| Setting | Question it answers | Valid values | Set by |
|---|---|---|---|
| **Data-source mode** | *Where does my meter data come from?* | cad / cad+api / api / api+mini | setup survey |
| **Billing source** | *When I have both DCC and local figures, which drives the bill?* | dcc / cad | toggle |

Billing source is **only meaningful when the mode provides both** — i.e. in
`cad+api` (and arguably `api+mini`, where Mini is provisional-local and DCC is
settled). In `cad` there's no DCC; in `api` there's no local. So:

- `cad`      → billing source is implicitly CAD (no toggle shown)
- `api`      → billing source is implicitly DCC (no toggle shown)
- `api+mini` → DCC settles; Mini is provisional. Toggle *may* apply (settle to
  DCC vs trust Mini) — **OPEN QUESTION, see §7**
- `cad+api`  → the real toggle case: local reads AND DCC settlement both exist

**Design rule:** billing-source toggle is only shown/active when the mode makes
it meaningful. Otherwise it's hidden and the implied value is displayed read-only.

---

## 3. Where each control should live

### 3.1 Data-source mode → Meter Config page

The mode describes how the meter is read, so it belongs with meter
configuration, not in a separate survey buried at setup. Proposal:

- **Meter Config page gains a "Data Source" section** at the top (above the
  per-channel sensor pickers), showing the current mode in plain language
  (e.g. "Local meter (CAD) + Octopus settlement").
- A **"Change data source"** action re-runs the survey (the existing
  `data_source_mode` survey flow), pre-filled with the current mode.
- The per-channel sensor fields below should **adapt to the chosen mode**:
  - `cad` / `cad+api`: import/export *read* sensors required (as today)
  - `api` / `api+mini`: read sensors hidden/optional (no local sensor); the
    import register comes from DCC/Mini
  - rate/standing-charge sensors: optional in API modes (resolved from Kraken
    schedule — see the rate-at-finalise and standing-charge fixes)

### 3.2 Billing source → Meter Config page (moved out of Data Management)

- **Remove** the billing-source card from `data_management.html`.
- **Add** it to Meter Config, directly under the Data Source section, shown
  **only when the mode makes it meaningful** (§2).
- The unsettled-blocks review card (currently also in Data Management, DCC-only)
  moves with it — it's part of "how billing is sourced," not data management.

### 3.3 What stays in Data Management

Backup, restore, delete blocks, corrections, import/export. Pure operations on
existing data. No source/mode decisions.

---

## 4. Transitions — the consequential part

Changing mode is not just writing a new value; existing state and the engine's
live objects must transition cleanly. This is where the risk is. Each transition
needs explicit handling:

### 4.1 Activating the API path (e.g. cad → cad+api, or → api / api+mini)
- Survey must collect API credentials (it already does).
- On save: refresh Kraken rate/standing schedules, discover device, activate
  Mini if `api+mini`, (re)launch the poll task. The engine already has
  `_ensure_kraken_poll_task_running`, `mini_setup`, `_refresh_kraken_rate_schedules`.
- Existing CAD blocks are untouched and remain valid history.

### 4.2 Tearing down the API path (e.g. cad+api → cad, or api+mini → cad)
**This is the leak-prone direction.** On save:
- Cancel the poll task (`_cancel_kraken_poll_task`).
- Tear down the Mini reader (`_kraken_mini_reader = None`).
- **Decide the fate of the credentials file** — see §5. (Orphaned creds are the
  exact class of bug we fixed earlier; a mode change to `cad` must not leave
  active creds that a future code path could pick up.)
- `mode_uses_api()` becomes False → API/Mini code dormant. Existing API/Mini
  blocks remain as history.

### 4.3 Switching the import source (api ↔ cad)
- `api` → `cad`: a local read sensor **must** now be configured, or blocks stop
  forming. The survey/config must enforce that a `channels.import.read` is set
  before allowing the switch.
- `cad` → `api`: local read sensor becomes optional; the seeded-shell block
  path (already built) takes over.

### 4.4 Mid-billing-period changes
- A mode or billing-source change mid-period means the period contains blocks
  sourced differently. The bill must handle mixed-source blocks gracefully
  (it largely does via per-block `source` / `imp_kwh_api` columns). **Confirm**
  the billing summary doesn't double-count or zero out across a switch boundary.

---

## 5. Credentials lifecycle (security-relevant)

The credentials file lives outside the DB/backups by design. Decisions needed:

- **On teardown to `cad`:** delete the creds file? Or keep it (so re-enabling
  API doesn't require re-entry) but ensure it's never read while mode is `cad`?
  - *Leaning:* keep the file but rely on `mode_uses_api()` gating (already true
    in the upgrade bridge). Add an explicit **"Disconnect Octopus / clear
    credentials"** action (the "blat" affordance) for users who want them gone.
- **The "reset/blat" affordance** (deferred from earlier): a deliberate button
  that clears API credentials + API-derived state. Belongs in the Data Source
  section, clearly destructive-styled, with confirmation. Separate from mode
  change (you might re-survey without wiping creds).

---

## 6. Survey re-run flow

The survey already exists and sets the mode via `api_data_source_mode_post`.
For re-running:
- Entry point: "Change data source" button in Meter Config.
- Pre-fill current mode so the user sees where they are.
- On completion, run the §4 transition for the old→new mode pair.
- **Guard:** never let a re-survey silently strand the user (e.g. switching to
  `cad` with no read sensor, or to `api` with no creds). Validate before commit.

---

## 7. Open questions — RESOLVED (release decisions)

1. **`api+mini` billing-source toggle?** RESOLVED (B1): no separate Mini-vs-DCC
   toggle. The rule is **"Mini is provisional; DCC wins once settled"** and is
   documented, not exposed. The DCC/CAD billing-source choice IS surfaced (wizard +
   Data Management) but that's a different axis (settlement source, not Mini trust).
2. **Per-meter vs global billing source.** RESOLVED: kept global. Sub-meters have
   no DCC; "use overlay" (INTELLIGENT-RATES §4.12) handles device pricing instead.
3. **Mid-period switch semantics.** RESOLVED (B3): a billing-source change calls
   `flag_all_for_pass2_rerun` → a FULL history recompute under the new source, so
   the period is coherent (never half-and-half). Reversible (flip back = another
   full recompute). Spot-check on dev; not an open design risk.
4. **Gate "Change data source" behind confirmation?** RESOLVED + BUILT (B2). A
   confirmation warns it runs a large recalculation that can't be stopped but is
   reversible — in Data Management (billing-source Apply) and the wizard's Change
   Setup finish (only when mode/billing-source actually changed over an existing
   config). Fresh setups skip it (no history to recompute).

---

## 8. Proposed build order (next session) — BUILT

All four shipped this cycle:
1. **Display only** — mode + implied billing source visible in setup (survey/wizard).
2. **Billing-source toggle** — surfaced in the wizard (API modes, default DCC) and
   kept in Data Management (mirror, not move). See WIZARD-DESIGN "settlement step".
3. **Change-mode (re-survey)** — the wizard's "Change Setup" re-runs over an
   existing config with non-destructive meta merge.
4. **Disconnect/clear-credentials** — `/api/disconnect-kraken` (switches to cad,
   deletes stored creds).

Residual (deferred, see §10 / backlog): the per-block "manually corrected / locked"
flag for the DCC-re-settlement + standing-charge cases.

### Original plan (retained for rationale)

1. **Display only** (lowest risk): show current mode + implied billing source in
   Meter Config, read-only. Ships value immediately, no transitions.
2. **Move billing-source toggle** from Data Management → Meter Config, with the
   §2 visibility rule. (Toggle logic already exists; this is relocation +
   gating.)
3. **Change-mode (re-survey)** with the §4 transition handling and §6 guards.
4. **Disconnect/clear-credentials** affordance (§5).

Each step independently shippable. Steps 3–4 are where the engine-side
transition code lives and need the most care + tests.

---

## 9. Engine support already in place (reuse, don't rebuild)

- `get_data_source_mode` / `set_data_source_mode` / `is_mode_configured`
- `mode_uses_api` / `mode_uses_mini`
- `_detect_upgrade_mode` (upgrade bridge — sticky cad on upgrade)
- `_ensure_kraken_poll_task_running` / `_cancel_kraken_poll_task`
- `mini_setup` / `_kraken_mini_reader` teardown
- `_refresh_kraken_rate_schedules`
- `_get_billing_source` / `apply_billing_source_change`
- per-block `source` / `is_provisional` / `imp_kwh_api` columns for mixed history

The plumbing exists. This work is mostly **UI placement + transition
orchestration + guards**, not new engine mechanics.

---

## 10. CORRECTIONS TOOL must protect manual overrides from reconciliation (UI-time)

**Status: PARTLY ADDRESSED. The corrections tool now exists (`/corrections`
page + `/api/corrections/{preview,apply}`), and the CORRECTIONS GATING RULE
(built) covers the common case. What remains (DCC re-settlement + standing
override) is deferred and narrower than originally scoped.**

The problem: when a user manually corrects a historical block (e.g. a rate that
never settled properly), that correction must be PROTECTED from being silently
overwritten by the DCC reconciliation path. Two failure modes:

1. **Re-settlement clobbers the correction.** The PASS 2 drain re-prices blocks
   flagged `needs_pass2_rerun`. If a manual correction doesn't mark the block as
   resolved/authoritative, a later poll bringing a DCC figure (or a re-run) will
   overwrite the user's fix. The correction must "win".

2. **The re-flag guard doesn't cover manual edits.** The re-flag guard (added to
   `classify_kraken_block`) only suppresses re-runs when the DCC *settled figure*
   (`imp_kwh_api`/`exp_kwh_api`) is unchanged. A MANUAL correction changes the
   *materialised* value (imp_kwh/imp_rate/imp_cost), NOT the api column — so the
   guard neither protects it nor re-flags it. A subsequent DCC figure that
   differs would still flag a re-run and clobber the manual value.

**How the GATING RULE addresses the common case (BUILT):**
In API+ modes a RATE correction is only ALLOWED on blocks that are already
DCC-settled (`imp_kwh_api`/`exp_kwh_api` NOT NULL); unsettled blocks are skipped
and reported ("re-run after settlement"). A settled block is not routinely
re-run (the re-flag guard suppresses re-runs while the settled figure is
unchanged), so a correction applied to it is not clobbered by ordinary
reconciliation. Pure CAD (no API) has no DCC reconciliation and applies
immediately. This dissolves failure mode 1/2 for the routine flow without a new
lock flag — the gate stops the user correcting a block whose kWh (and overlay
rate) is still going to be rewritten.

**What still remains (deferred, narrower):**
- **DCC RE-settlement:** a block that settles a SECOND time (Octopus re-issues a
  differing DCC figure for an already-settled block) WOULD re-run pass 2 and
  could clobber a correction. This is the residual case the gating rule does not
  cover. The per-block "user-authoritative / manually corrected" flag below is
  still wanted for it.
- **Standing-charge override:** standing corrections are not gated (standing
  charge isn't kWh-settlement-dependent the same way); the same lock concept
  covers it.

**The deferred lock (for the residual cases):**
- A per-block "user-authoritative" / "manually corrected" flag that the
  reconciliation path RESPECTS — i.e. `_rerun_pass2_for_settled_block` and the
  drain SKIP (or don't overwrite the corrected channel of) a flagged block.
- The corrections tool SETS this flag on apply.
- Precedence: manual wins — show DCC divergence in the review card rather than
  auto-overwriting.
- Solve rate, kWh, and standing under one "manually corrected / locked" concept.

**Release status (3.0):** CONFIRMED DEFERRED (decision D1 / backlog 1; cross-ref
INTELLIGENT-RATES §7 F5). Not a 3.0 blocker — the gating rule covers the routine
path. Note that the limbo-finalisation work (block_store `finalise_past_horizon_
blocks` + `finalised_from_cad`, cleared by a real settlement in `upsert_kraken_
block`) already demonstrates the reversible-flag pattern this lock would extend:
a per-block marker that the settlement path respects/clears. The lock would add a
*user-set* equivalent that settlement must NOT clear.