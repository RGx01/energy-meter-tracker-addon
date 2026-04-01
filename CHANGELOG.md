# Changelog

## [1.6.1] — 2026-04-01

### Added
- **Usage Stats data table — totals column** — a Total column now appears at the right of the data table, summing all data columns for each row; the bottom-right cell shows the grand total for the period; styled to match the existing totals row (bold, full-brightness text, separator border)
- **Usage Stats data table — period labels** — the Period column now shows full dates in daily mode (e.g. `1 Apr 2026`), full month and year in monthly mode (e.g. `Jan 2026`), and the year in yearly mode, rather than bare day or month numbers

### Fixed
- **Usage Stats export cost positive in data table** — export values were returned as positive numbers in the data table, causing the row Total and column totals to add export rather than subtract it; export now displays as a negative value (matching the chart where export bars fall below the axis) and totals are computed correctly
- **Usage Stats meter labels include site name** — the main meter import legend and table column was labelled with the configured site name (e.g. "House import"); it now always shows "Grid import" to avoid exposing the site name in shared or externally accessed charts
- **Light/dark theme toggle not working on Billing and Usage Stats** — resolved a cascade of issues: CSS variables were defined in `:root` with dark values and no `[data-theme="dark"]` override, so toggling `data-theme` had no effect on HTML elements; theme helper functions were defined after the data array that called them, causing silent JS errors; the shell (`base.html`) lacked `[data-theme="dark"]` overrides entirely; `base.html` now carries both theme variable sets, all chart generators use `[data-theme="dark"]` as the override block with light values as the `:root` default
- **Heatmap toggle button rendering as artefact over chart** — the ☾/☀ button was `position:fixed` at `right:52px`, overlapping the boundary between the heatmap and totals bar; changed to `position:absolute` within the chart container
- **Heatmap mobile portrait gap** — the chart left a large black area below it on mobile portrait; `scaleChart()` now sets the scroll div to fill the full viewport height on mobile
- **Heatmap scroll-guard strip visible on desktop** — the 44px scroll-grab strip was always rendered regardless of device; it now only shows (`display:flex`) when the mobile breakpoint is active

---

## [1.6.0] — 2026-04-01

### Added
- **Usage Stats chart** — new 📈 Usage Stats tab on the Charts page; shows daily, monthly and yearly import/export with sub-meter breakdown stacked in billing colours; switchable between kWh and cost; Totals and Net views; monthly and yearly periods include a year selector for multi-year comparison
- **Data table** — tabular view below the Usage Stats chart mirroring what the chart shows; totals row at the bottom; copy-to-clipboard button exports tab-separated data for pasting into Excel or Google Sheets
- **Tooltip totals** — Usage Stats chart tooltips show a footer with Import / Export / Net summary below the per-dataset breakdown
- **Light/dark theme toggle** — all three chart types (Billing, Net Heatmap, Usage Stats) now support switching between light and dark mode; preference is saved to localStorage and restored on next load; system preference (`prefers-color-scheme`) used as the default; theme is synchronised across all chart tabs via a postMessage bridge so toggling in one chart updates all others

### Changed
- **Summary page renamed to Live Power** — the ⚡ nav item and page heading are now labelled Live Power throughout
- **Remember last visited page** — the add-on remembers which page and which chart tab you were on and restores both on refresh; invalid or stale saved state is safely ignored
- **Usage Stats billing accuracy** — figures use `calculate_billing_summary_for_period` for main meter grid remainder (matching the Billing chart exactly), with sub-meters aggregated directly from blocks by meter ID for correct colour mapping and breakdown
- **Chart tabs renamed** — Daily Usage → Billing, Import / Export → Usage Stats
- **Help page mobile layout** — font sizes reduced to match the rest of the UI; long sensor entity IDs wrap correctly; code blocks scroll horizontally on mobile; each section is collapsible by tapping the title on small screens
- **Heatmap mobile improvements** — pinch-zoom disabled on the chart area; scroll grab strip widened to 44px with a visible indicator; chart height responsive to viewport height on mobile; `srcdoc` replaces blob URLs for iframe loading (fixes Safari/WebKit "string did not match" error)
- **Usage Stats orientation fix** — rotating between portrait and landscape and back now correctly redraws the chart at the new dimensions
- **Usage Stats cost precision** — cost values displayed to 2 decimal places; kWh values remain at 3 decimal places

### Fixed
- **`config_page` route missing** — a code editing error had removed the `/config` route from `server.py`, causing a `BuildError` on all pages
- **`/api/charts/heatmap` route missing** — same issue had removed the heatmap API endpoint, causing a 404 when opening the Net Heatmap tab
- **Heatmap "Failed to load chart: The string did not match the expected pattern"** — replaced `Blob` + `URL.createObjectURL` with `iframe.srcdoc`; blob URL navigation is blocked in Safari and some mobile browsers
- **Copy to clipboard** — rewrote clipboard handler to use `execCommand` first (works over HTTP); the previous implementation relied on the `navigator.clipboard` API which requires HTTPS

---

## [1.5.1] — 2026-03-26

### Added
- **Live Power page** — new ⚡ page accessible from the sidebar once a power sensor is configured
- **Live power gauge** — asymmetric semicircular gauge showing net grid flow with carbon-intensity colour coding (UK) or magnitude-based colouring (global)
- **Billing summary cards** — Today, This Bill and This Year with sub-meter breakdown; billing-accurate figures matching the Billing chart
- **Billing auto-refresh** — cards update 1 minute after each block boundary without a page reload
- **Carbon intensity forecast** — 🇬🇧 UK only; 48-hour forecast from the National Grid API via your postcode prefix; no API key required
- **Power sensor and postcode prefix fields** added to Meter Config

---

## [1.4.0] — 2026-02-10

### Added
- Configurable meter reconciliation period (5, 15 or 30 minutes)
- Automatic currency detection from rate sensor unit of measurement
- International sensor compatibility

### Fixed
- Export-only daily chart rate axis alignment
- Billing day always read from live config

---

## [1.3.x] — 2025-12-01

### Fixed
- Timezone-aware chart rendering
- UTC timestamp bugs
- Silent sensor timeout (block stuck at boundary)
- Standing charge billing display

---

## [1.2.0] — 2025-10-15

### Added
- Guided Setup Wizard for first-time configuration

---

## [1.1.0] — 2025-09-01

### Added
- Flask-based web UI with Meter Config, Charts, Import & Backup, Logs and Help pages

---

## [1.0.0] — 2025-08-01

Initial release. Core metering engine, sub-meter support, gap filling, billing charts, HA sensor publishing.