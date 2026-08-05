# Data Management — 4.0.0 vision & roadmap

Living doc. Captures the direction for Data Management and the associated chart/UX
work, so we can sequence it rather than build piecemeal. Add to it freely.

## The problem

Data Management has grown into a pile of overlapping tools — Historical Import,
Find Missing Data, Device History, Historical Corrections, Delete Blocks,
Backups/Restore. Several of them reinvent the same shape ("pick a range → choose a
source → preview → apply → review") and the boundaries between them aren't obvious
to a user. The goal is for the section to read as a **flow**, organised by intent,
with one consistent interaction pattern.

## Target information architecture — three intents, one pattern

Every data tool follows the same spine: **scope → source → preview → apply → review.**

1. **Bring data in / fill it (backfill).** Treat these as three faces of *one*
   backfill engine, not three separate tools:
   - **Historical Import** — first-time bulk backfill of the house total (API or CSV).
   - **Heal Gaps** (reworked from "Find Missing Data") — fill holes in house data
     after the fact (API **or** CSV), reusing the Import backfill machinery.
   - **Device History** — fill a device's share from HA recorder history.
   Shared model: choose *what's missing* (range / gap / device), choose a *source*
   (supplier API or CSV), preview, apply, review.

2. **Correct data.** **Cost Corrections** (renamed from Historical Corrections):
   rate/cost fixes. When a supplier API is available, offer the API repair
   (re-fetch billed cost for a date range) **inline** here — set the range and
   repair in one place, rather than bouncing to the Import page's repair tools.
   (The Import repair tools remain; Cost Corrections becomes the front door.)

3. **Safety & housekeeping.** Backups, Restore, Delete Blocks — grouped and clearly
   flagged as maintenance/danger.

## Backlog

### Done (4.0.x, this pass)
- **"Copy JSON to clipboard"** label on Device History (was "Copy report (JSON)").
- **Sensor ledger** on Device History: a device that still has a reconstructed
  layer pre-populates the sensor(s) that built it (from the run ledger), kept until
  that layer is Removed. New `history_sensors` on `/api/historical/probe-devices`.
- **De-probe**: page/nav renamed "Device History" (a stray "Recorder Probe" button
  in older builds clears on deploy — no such string remains in source).
- **Rolodex picker consistency (shared component).** Extracted the period spinner
  into one shared partial (`_period_spinner.html`) used identically on Usage Stats,
  Insights, and the Billing charts (inlined into the iframe, driven by a
  getPeriods/onSelect adapter). Global tuning lives in one `DEF`. Added ‹ › step
  arrows on Billing; a detached-anchor guard (fixes the popup jumping off-page);
  and a **native `<select>` fallback on touch devices** (the wheel/drag spinner
  locked up on mobile).
- **Billing charts blank-page on year→month** (the bug noted below) — fixed:
  selection change now resets scroll to the top and shows the floating bill strip
  immediately instead of gating on a chart entering the viewport.
- **Dropped the v1 JSON→SQLite migration shims.** Removed `migrate_json_to_sqlite`
  and the startup auto-migration of `blocks.json` / `current_block.json` (and the
  backup-restore auto-migration). A legacy JSON store is now met with a loud error
  and a fresh start rather than a silent import; the file is left untouched.
- **Data Management IA — the three intents (first pass).** The page is now grouped
  under three labelled sections — **Bring data in & fill gaps** (Historical Import,
  Device History, Import files, Missing Data), **Correct & review** (Cost Corrections,
  Billing Settlement Source, Review Unsettled Blocks), and **Safety & housekeeping**
  (Backups/Restore, Delete Blocks, Storage, Compact, File Reference). Sub-page links
  moved out of the top toolbar into their intent section. Historical Corrections is
  now surfaced as **Cost Corrections** (display rename; route unchanged). Still to
  come: the shared scope→source→preview→apply flow and the inline API repair.

### To do
- **BL — Complete-your-history (two task flows: Fill gaps · Import history).**
  Reframed from the earlier "unified backfill" — see the revised spec below. The
  shared engine is built (`backfill.py`, `/api/backfill/*`, unified gap detection =
  block-row holes + per-channel import/export data gaps). Remaining: the two task
  flows, the **bounded contiguous API fetch** primitive, and the device sub-sections.
- **BL — Cost Corrections** inline API repair (gated on API available). The rename is
  done; the inline repair is not.
- **BL — Spiral charts makeover.** On desktop, use the empty space: render a **grid
  of spirals** (all selected metrics at once); click one to focus, collapse back to
  the grid otherwise.

## Spec — Complete-your-history: two task flows (Fill gaps · Import history)

Reframed after review (2026-07). The generic scope→source picker was still a *tool*;
users think in **tasks**. So: two task-oriented flows over one shared fill engine.

**Two tasks.**
- **Fill meter gaps** — fill holes *inside* existing history: missing block rows, and
  missing per-channel data (import/export, and device channels). Bounded by the gap
  windows (contiguous by definition).
- **Import meter history** — *extend* history backward. "How far back can the API reach?
  → go back to [entire history / a date]." Fills contiguously from the chosen start **up
  to the oldest existing EMT block** (the fixed end bound).

**Contiguity is a hard rule.** Every fill yields one contiguous run. Import's end bound
is *always* the oldest existing block; gap windows are interior. No floating [from,to]
islands.

**Source: API first, CSV fallback** (both tasks). API only if `kraken_available()`.
CSV can carry columns for the configured devices.

**Devices are a sub-section of each task, not a separate tool.** We already know the
configured sensors; within a fill/import the user can **add extra sensors** for coverage
(a sensor recovered or renamed). HA-recorder attribution is the API-equivalent device
source; CSV device columns are the manual route. The standalone Device History tool folds
into these sub-sections.

**Keystone primitive (new): bounded, contiguous API fetch** — fetch `[from → to]` where
`to` = the oldest block (import) or the gap window (gaps). The Measurements client can
already fetch any window; today only `from → now` is exposed. This unlocks API gap-fill
and single-contiguous-period import (both currently impossible via API).

**Reused machinery:** capability probe (`plan_api_import` — how far back the API reaches),
prepend config periods (`create_covering_period` + discover/apply sites), CSV templates
(`gap_template_csv`), device attribution (recorder), self-clearing per-channel gaps
(`api_import_gaps`), unified gap detection (`/api/backfill/gaps`).

**Delete.** Single-block delete **stays** (genuine/edge cases + setup confusion). Range-
delete and delete-reconstructed remain the safe bulk paths. Most historical deletions were
users working around engine bugs since root-caused/fixed; the heal flow now makes accidental
gaps re-fillable, closing that loop.

**Boundaries.** Fill = fill empties only; correcting existing values stays with Cost
Corrections. Filled blocks with no device data are flagged. Gap-fill can resurrect
deliberately-deleted blocks → manual + previewed.

**Build stages:**
1. **Bounded contiguous API fetch** primitive (engine + client) + tests — the keystone.
   Import: `from → oldest-block`; gaps: the window.
2. **Fill meter gaps** flow: the heal list (built) + bounded API fill per gap + device
   sub-section (known + added sensors) + CSV fallback.
3. **Import meter history** flow: capability probe → "how far back" → prepend-fill to the
   oldest block, API-first / CSV, device sub-section.
4. **IA + coverage view**: two entry points; a shared coverage view (present vs missing
   across rows/channels/devices, + API reach); fold Device History in; retire the scattered
   Historical Import panels.

*Superseded:* the earlier scope(whole/gaps/range) picker. `backfill.py` + `/api/backfill/*`
remain as the shared engine underneath the two task flows.

## Open questions
- Heal Gaps: is the primary trigger a detected-gap list, a free date range, or both?
- Cost Corrections + API: inline repair only, or also keep the explicit hand-off?
- Spiral grid: how many metrics/spirals before it's too busy on a laptop width?
