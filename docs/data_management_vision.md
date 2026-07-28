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

### To do
- **BL — Heal Gaps tool** (rework "Find Missing Data"). Larger item. Unify with the
  Historical Import backfill and Device History under the shared backfill pattern;
  support **CSV and API** sources. Design the "scope → source → preview → apply"
  flow once and reuse.
- **BL — Cost Corrections** rebrand + inline API repair (gated on API available).
- **BL — Data Management IA** reshape into the three intents above.
- **BL — Rolodex picker consistency.** Clicking the period picker shows **±10
  periods** and still scrolls like a rolodex. Extract as **one shared component**
  used identically on Usage Stats, Insights, and Billing charts.
- **BL — Spiral charts makeover.** On desktop, use the empty space: render a **grid
  of spirals** (all selected metrics at once); click one to focus, collapse back to
  the grid otherwise.
- **Cleanup — drop the v1 JSON→SQLite migration shims.** One-way door for anyone
  still on a v1 JSON store (almost certainly nobody on 4.0.x). Changelog note; add a
  guard that fails loudly rather than silently if an ancient JSON store is ever seen.

### Bug — Billing charts blank-page on year→month
When a **year** is selected the page is very long. Scroll to the bottom, then select
a **month**: the user is left staring at what looks like a **blank page** until they
scroll all the way up to the first chart — and the floating action bar (with the
"Show bill" button) only appears once the first chart scrolls into view. Likely the
lazy-render/scroll-position interaction (a cousin of the earlier "snap to top" and
lazy-render jump fixes): on selection change we should reset scroll to the top and
render/show the floating bar immediately, not gate it on a chart entering the
viewport. Needs investigation in the Billing charts template.

## Open questions
- Heal Gaps: is the primary trigger a detected-gap list, a free date range, or both?
- Cost Corrections + API: inline repair only, or also keep the explicit hand-off?
- Spiral grid: how many metrics/spirals before it's too busy on a laptop width?
