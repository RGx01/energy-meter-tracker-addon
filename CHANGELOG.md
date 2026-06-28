# Changelog

## [3.0.3] — 2026-06-28

*Resilience to Octopus/Kraken API changes — no functional change for current setups.*

### Changed

- **Future-proofed Intelligent dispatch detection** — moved off Octopus's deprecated `registeredKrakenflexDevice` API (which Octopus has scheduled for removal) to the current `devices` query. Smart-charge / dispatch provider detection keeps working once Octopus withdraws the old field.

### Added

- **API drift self-detection** — if an Octopus/Kraken schema change ever rejects a field the add-on depends on, it now logs a distinct `kraken_schema_drift` error pointing to the Octopus announcements page, instead of a generic "unavailable" message — so a broken query is obvious in the logs and quick to pin down.

---

## [3.0.2] — 2026-06-28

*Packaging and repository housekeeping — no functional changes. Faster to install and update.*

### Changed

- **Faster, more reliable builds** — the Docker base image and Python dependencies (`aiohttp`, `flask`, `waitress`) are now pinned to fixed versions. The dependency layer is cached across updates instead of being reinstalled each time, and builds no longer drift onto progressively heavier dependency trees.
- **Smaller build context** — screenshots, tests, and development docs are excluded from the image build, so each build copies far less.
- **Trimmed changelog** — older release notes (2.10.x and earlier) moved to `CHANGELOG-ARCHIVE.md`, so the in-app update dialog loads and renders much faster.

---

## [3.0.1] — 2026-06-27

*Post-release fixes for issues reported after 3.0.0.*

### Fixed

- **Confusing error after entering the Octopus API key (issue #217)** — the Setup Wizard's Account number field was labelled "(optional — auto-detected)", so users left it blank; when auto-detection couldn't resolve the account the result was an unhelpful error. The misleading hint is removed so the field reads plainly as the credential it is.
- **Device name appeared optional when adding an EV / battery / heat pump (issue #218)** — the wizard's Device Name field was labelled "(optional)" even though a name is required (and was already enforced on submit), producing a confusing "name is optional, apparently" experience. The misleading "(optional)" label is removed; the genuinely-optional Site Name is unchanged.
- **"This Bill" showed a stale historical date when the current period had no data yet (issue #221)** — on the Overview, when the most recent block predated the current billing period (so no generated period contained today), the "This Bill" card fell back to the last *historical* period and displayed an old date (e.g. an April period). It now synthesises the current billing period from your billing day and shows it — at £0.00 until data lands — instead of a past one. The billing-day arithmetic is also hardened so a billing day of 29–31 no longer breaks in shorter months.
- **Generation Mix card overlapped its neighbour on the Overview (issue #223)** — with a live-power card plus three or more devices, the Overview switches to a grid layout whose 220 px columns were narrower than the 280 px Generation Mix card, so at some window widths the card overflowed its cell and overlapped the Live Power card instead of wrapping. The grid columns are widened to 280 px so the cards always wrap cleanly. (Live Power with two devices was unaffected and still is.)

---

## [3.0.0] — 2026-06-21

*The largest release since EMT began. v3 turns a meter-reading recorder into a billing-grade energy ledger: it can reconcile every block against the settled half-hourly data your supplier actually bills from (Octopus / Kraken DCC), understands Intelligent Octopus Go smart-charge slots, and accounts for grid carbon intensity across the whole app. This is a **major-additive release — nothing breaks**. Existing 2.x installs upgrade in place, keep behaving exactly as before, and switch the new capabilities on when they're ready. Everything new is opt-in through the Setup Wizard. See **Upgrading** at the foot of this entry.*

### Added

- **Octopus / Kraken DCC settlement** — EMT can now reconcile each block against the settled half-hourly consumption your supplier bills from, fetched directly from the Kraken platform. It pulls settled import (and export), unit rates and standing charges on a schedule, runs an automatic settlement sweep with a horizon back-stop so older blocks get reconciled as the data lands, and retries transient failures. A **billing-source toggle** (Meter sensor vs Supplier API) chooses which figures drive your bill. Credentials are entered in-app with a keep-or-replace flow and a disconnect/clear action — no YAML. Rate corrections are gated until a block is DCC-settled, so a manual edit can't fight the authoritative figure.

- **Data-source modes & supplier-first setup** — the Setup Wizard now leads with your supplier and meter situation, then configures the right data path: **`cad`** (local CAD/sensor only — exactly how 2.x works) or **`cad+api`** (local readings plus supplier-API settlement and telemetry). A **Change Setup** launcher lets you move between modes later, behind a confirmation because a switch can trigger a full recalculation. Configuration is mode-aware — options that don't apply to your setup are hidden.

- **Octopus Home Mini live power** — if you have an Octopus Home Mini, EMT can use its real-time demand feed as a live-power source for the gauges and 48-hour history without a separate CAD sensor, with exponential backoff and rate-limit-aware polling. (Mini figures are treated as provisional; DCC wins once a block settles.)

- **Intelligent Octopus Go dispatch overlay** — for IOG users, EMT captures smart-charge dispatch slots and reprices the affected blocks at the off-peak rate they were actually charged at, applied at both finalise and DCC settlement. Per-device **"use dispatch overlay" rates** let an EV charger bill at the dispatch rate while the rest of the house stays on the standard tariff. A 0.1 kWh validation floor ignores meter jitter.

- **Grid carbon intensity** — EMT records gCO₂/kWh for every block (🇬🇧 UK, postcode-based) and surfaces it throughout: a **Carbon Insights** page (house / solar / EV / battery / heat-pump breakdown, gCO₂/mile, generation mix), CO₂ columns and charts in Usage Stats, and a gCO₂/kWh **heatmap** mode. A one-time historical backfill fills carbon data for your existing 2.x blocks in the background after first start.

- **Per-channel rate source (API rates vs sensor)** — each channel can independently take its unit rate from the supplier API or from a sensor, so mixed setups work (e.g. API import rates alongside a sensor-supplied export rate).

- **Power-sensor invert & unit override** — any power sensor (main, EV, heat pump, battery) can now have its sign inverted and its unit forced to W or kW. This fixes live-power and 48-hour charts that read reversed (sensor sign convention opposite EMT's) or 1000× wrong (a sensor that emits watt-scale numbers while declaring its unit as kW, which no auto-heuristic can catch).

- **Ohme EV charger support** — detection-aware handling for Ohme chargers, preferring the charger's own session sensor where available. Ships conservatively, with a feedback path in the config UI for Ohme users to report behaviour.

### Improved

*Lighter-touch refinements to things 2.x users already rely on:*

- **"Devices" replaces "sub-meters"** — your EV charger, battery and heat pump are now called **Devices** throughout the UI. No data or configuration change.
- **Overview page (formerly Live Power)** — the Live Power page is now **Overview** and renders even without a live-power sensor, so postcode-only installs still get generation mix and carbon context.
- **EV grid attribution (issue #212)** — when an EV and a battery draw at the same time and grid import only covers part of it (solar filling the rest), the EV now claims grid import **first** instead of the largest load winning. An EV on a cheap smart-charge slot no longer vanishes from the grid view behind a simultaneously-charging battery. Bill totals are unchanged — only the per-device attribution is corrected.
- **Block delete redesign** — deleting blocks now cascades device rows, reseeds the following block's opening reads and recomputes the remainder, so a delete no longer leaves orphaned device data or a gap.
- **BottlecapDave (BCD) awareness** — if you run the BCD Octopus integration, EMT detects it and adjusts so the two don't double up on live-power polling.
- **Billing consistency & restart robustness** — many reconciliation, gap-fill, restart-recovery and cost-rounding refinements across the api+mini path, so totals stay self-consistent across restarts and configuration changes.

### Upgrading

- **Nothing breaks.** Your blocks, configuration and history are preserved. Schema migrations run automatically on first start, and a one-time carbon-intensity backfill runs in the background (UK postcode installs).
- **You don't have to change anything.** Leaving the data-source mode at `cad` keeps EMT behaving exactly as 2.x did. DCC settlement, Mini live power and the dispatch overlay are all opt-in via the Setup Wizard / Change Setup.
- If you configured the Octopus API before the supplier field existed, EMT infers Octopus from your existing credentials. A restored v2-era backup with no supplier field defaults cleanly to local-metering-only.
- See **Upgrading from 2.x** in the README for the full walk-through.


---

Older releases (2.10.x and earlier) are in [CHANGELOG-ARCHIVE.md](CHANGELOG-ARCHIVE.md).