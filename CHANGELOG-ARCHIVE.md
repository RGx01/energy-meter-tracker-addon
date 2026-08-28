# Changelog — Archive

Released versions (**4.3.x and earlier**). The current unreleased entries are in [CHANGELOG.md](CHANGELOG.md).

---

## [4.3.2] — 2026-08-18

*Consistency patch for the Intelligent Octopus house-vs-car split. **Apply this if you're on 4.3.0 or 4.3.1** — it corrects an attribution error present ever since the house-vs-car split landed in **4.3.0 (BL-9)**, so the split reads consistently (and identically across two copies of the same account) ahead of the pricing **re-architecture and code simplification coming in 4.4.0** (the priced-segment model, BL-27), which retires the layered EV-split columns this class of bug lives in. Display attribution only — no Total Bill or grid-total figure moves.*

### Fixed

- **The house/car split no longer under-counts a car charge that Octopus confirms *after* the half-hour is priced — and two copies of the same account now show the same numbers.** Since the house-vs-car split arrived in **4.3.0**, EMT has stored the split (`imp_kwh_ev`) on each IOG block **once, when the block is priced**, from Octopus's **completed-dispatch** record known at that moment. But Octopus reports a slot's *completed* energy **hours later** — routinely after the block was already priced with only a **planned** dispatch — and nothing re-opened the block when it arrived: the settlement re-stamp only ever touched blocks that **already had** a split (`imp_kwh_ev IS NOT NULL`), so a charge confirmed late was **never attributed** and sat silently in **Home**. Because the outcome depended on *when* an instance happened to price each slot relative to that late record, two copies of the **same account** could disagree permanently — a real case: for an identical account and byte-identical dispatch history, one instance attributed **271.9 kWh** of off-peak car charging over the month while the other showed **253.2 kWh**, a whole overnight session (**~18.6 kWh**) stranded in Home on one but not the other. Settlement reconciliation now **back-attributes** the split on any block that has a completed dispatch but no stored split, re-running the **same grid-clipped IOG split** used at pricing — so it heals existing history on the first run after upgrade and self-heals every late-arriving dispatch thereafter, and both instances converge on the **same, correct** figure (the account above lands at **298.1 kWh** on *both* — matching Octopus's own completed-dispatch total, because both copies had in fact been under-counting). *(Display attribution only — the grid total, the per-meter "Breakdown by meter", and the Total Bill are unchanged; this moves settled cost from **Home to EV**, so a car charge that was hiding in Home now shows correctly against the car — expect EV to rise and Home to fall on affected past periods. Uncapped IOG only: on a capped meter the car's rate legitimately differs and that attribution belongs to the priced-segment model in 4.4.0 — see roadmap BL-27.)*

## [4.3.1] — 2026-08-18

### Fixed

- **A phantom rate above the tariff peak in the IOG house/car billing split is gone.** On an Intelligent Octopus Go tariff, when settlement reconciliation **reverted a negligible smart-charge slot** — a completed dispatch too small to count as a real charge — from off-peak back to peak, it rewrote the block's inc rate and ex-VAT figure but **not** the stored **EV/house split**. So the car slice stayed frozen at off-peak while the block was priced at peak, and the **home remainder absorbed the missing peak cost** — surfacing an *impossible* "Home" row at a rate **above the tariff peak** (an ex-VAT figure that even exceeded the inc peak rate) on the *Import — total grid* breakdown. Reconciliation now **re-derives the EV/house split** (`imp_rate_ev`/`imp_cost_ev`) alongside the rate on uncapped IOG, so it can't drift again, and a **one-off startup repair** corrects any block already showing it. *(Display attribution only — the grid total, the per-meter breakdown, and the Total Bill were always correct. Capped tariffs are intentionally left untouched here: the car's rate legitimately differs from the house's there, and the proper fix is the **priced-segment pricing model** — see `docs/design/segment_pricing_refactor_design.md` / roadmap BL-27, which retires the layered EV-split/ex-VAT columns that keep drifting out of sync.)*

## [4.3.0] — 2026-08-17

*Theme: **Charge Cap**. [Still Experimental] Groundwork for Intelligent Octopus Go's new 4-rate / 6-hour-cap tariff lands under the hood — every IOG block now stores the house-vs-car split, a migrated (capped) meter is priced with the full 4-rate model, and the billing summary now itemises the house-vs-car split per rate band (with the day chart's car rate line wired to diverge the moment a cap engages). Plus a batch of fixes: CSV templates start at local midnight, synthesised bill CSVs no longer double the standing charge, the Usage Insights rate breakdown matches the Billing view, and FIT / no-export-agreement accounts no longer show a permanent "awaiting settlement" backlog. Additive and off for non-IOG tariffs; inc-VAT figures for existing tariffs are byte-identical.*

### Added

- **Intelligent Octopus Go 6-hour charge cap — billing back-end (BL-9).** Octopus has moved IOG to a **4-rate** tariff with a **daily 6-hour cap** on cheap car charging (`IOG-SMB-TOU`): your home always gets the off-peak window (23:30–05:30); your car gets up to **6 hours** of off-peak charging per day (measured **midday-to-midday**); the home *also* rides the off-peak rate on any out-of-window half-hour the car is smart-charged in **while within** that allowance; and everything beyond the 6 hours — or a Boost — reverts to the standard / EV-peak rate. EMT now reconstructs the **house-vs-car split** for every IOG block from Octopus's own **completed-dispatch** record (grid-clipped, so it's the figure Octopus actually bills, and it needs **no charger sensor** — it works for any charger or vehicle integration) and **stores it on the block**. On a **migrated (capped) meter** — detected automatically from the tariff code — the block is **priced with the full 4-rate model**, including the noon-to-noon 6-hour boundary and the out-of-window home "freebie"; on an **uncapped IOG meter** the split is stored for reference but pricing is byte-identical to before. *(Additive and off for non-IOG tariffs. The cap model is a live/provisional predictor and **settled billed cost stays authoritative**, so any boundary-slot estimate self-corrects at settlement. The house/car breakdown now renders on the billing summary and the charts are wired for the cap — see below.)*

- **The house-vs-car split now shows on the billing summary, and the charts are wired for the cap.** The house/car split EMT stores per IOG block is now itemised under **"Import — total grid"** on the Billing view, the way Octopus lays it out on the statement: **EV** and **Home** interleaved per rate band (off-peak, peak — plus a single blended *transition* row for the half-hour a cap boundary falls in), with **ex-VAT** ("show VAT") support and near-identical settlement rates folded into clean bands. *"Breakdown by meter" is untouched* — that section still shows the physical devices; the grid-total section shows the billed dispatch split. Existing history is filled in on upgrade by a **one-off backfill** (no re-import), so the split appears across past bills the first time you run 4.3.0. On the **day chart**, the car's rate line now follows the car's **own** stored rate, so it will **separate from the house line** the moment a 6-hour cap pushes charging to peak; on an uncapped meter the two rates are equal, so the line is unchanged. And the **same** house-vs-car cost split now drives **Usage Stats, Usage Insights and the charts** — every surface reads the stored, bill-authoritative figures instead of computing its own estimate, so a capped account shows the **same** EV/house cost on every screen (on an uncapped account they were already identical). *(Additive — inc-VAT totals and the Total Bill are byte-identical; the split apportions the settled total, it doesn't change it.)*

### Fixed

- **Ex-VAT rates left stale by settlement reconciliation are now corrected.** When the settlement pass **reverted or restored** a half-hour's rate (e.g. flipping a mis-priced slot from off-peak back to peak), it rewrote the inc-VAT rate but **not** the stored ex-VAT rate — so that block kept the *old* band's ex-VAT figure. This was harmless until the new per-band ex-VAT breakdown surfaced it, where one such slot read a visibly wrong rate and slightly understated the ex-VAT subtotal. Reconciliation now **re-stamps the ex-VAT rate** alongside the inc rate (or clears it to fall back to inc ÷ VAT when no ex-VAT rate is available), and a **one-off repair** re-derives any existing block whose stored ex-VAT rate is implausibly inconsistent with its inc rate, from the VAT calendar. *(Ex-VAT display only — inc-VAT figures and the Total Bill are unchanged.)*

- **CSV gap / date-range templates now start at local midnight, not UTC midnight (#372).** The pre-filled Start/End template for a gap or date-range fill was aligned to **UTC** midnight, so during **British Summer Time** a day's first row read `01:00+01:00` instead of `00:00+01:00` — the whole day shifted forward an hour, dropping the first two half-hours and picking up two from the next day. (In winter UTC and local coincide, so it only showed in summer.) The template now aligns to **local** midnight. *(Template generation only — the CSV *import* was always offset-aware and unaffected; no stored data changes.)*

- **A synthesised dual-rate bill CSV no longer writes the daily standing charge twice (#370).** When EMT reconstructs a CSV from a PDF bill that has **no half-hourly pages** on a **dual-rate** (day/night) tariff, it spreads the day's energy across the night and day windows in two passes — and each pass stamped the day's standing charge on its first slot, so a synthesised day carried the standing charge **twice**. It's now written **once per day**. *(Billing was unaffected — the importer already takes the first standing charge per day, not the sum — so this only corrects the generated CSV; no stored figure moves.)*

- **The Usage Insights rate breakdown now matches the Billing view (#371).** The 4.2.2 rate-collapse and negative-price handling covered the billing summary and the day chart, but the **Usage Insights** page's *Rate Period Usage* card still printed a bar for **every** distinct rate — hundreds on Agile — and **silently dropped plunge (negative-rate) slots**. It now folds to a single kWh-weighted-average row above the same five-rate threshold (labelled *"(avg of N rates)"*) and **keeps plunge slots** (shown as a credit), consistent with the Billing view. *(Display only.)*

- **A FIT / no-export-agreement account no longer shows a permanent "awaiting DCC settlement" backlog.** The unsettled-blocks badge counted a block whenever its **export** hadn't DCC-settled and the meter ever exports — but a **Feed-in Tariff** (deemed export), or any account with **no Octopus outgoing/export agreement**, never settles export at all, so it showed a permanent, rolling ~2-week backlog (one account: **671 blocks**) that *Retry settlement* could never clear. EMT now only expects export to settle when the account is actually on an **outgoing/export agreement** (recorded at Octopus discovery); export that will never settle is no longer counted, so the badge falls back to the genuine import lag. *(Count / badge only — no stored data or billing change. Defaults to counting when the flag isn't yet known, so a real gap is never hidden.)*

## [4.2.4] — 2026-08-14

*Billing-period reliability: the Total Bill could **double** once you had more than one billing period, and creating, editing or removing a period could race the engine, corrupt the bill, or fail with a raw database error. Also — Octopus credentials survive a database swap, restoring a database from a file matches the backup-restore flow, the Charts page refreshes after a config change, and a standing-charge rate change shows correctly. Deliberate deletions now **stay deleted** (with an explicit Re-import to bring them back), a delete with explicit times spans one continuous range as you'd expect, outage/re-import gaps self-heal, and a big delete or chart rebuild no longer triggers a Home Assistant reconnect storm. The doubling fix only removes an over-count; no correct billing figure moves, and inc-VAT figures are otherwise byte-identical.*

### Added

- **Deleting a period now stays deleted — and Re-import brings it back (BL-8 phase 2).** A deliberate delete used to be indistinguishable from an outage hole, so the automatic supplier sync (BL-8 backfill) re-created the day on the very next poll — you'd delete a day and it would reappear after a restart or a Home Assistant reconnect. A delete now records the removed span in a `deleted_ranges` tombstone that the poll's backfill, the settlement sweep, and the outage gap-scan all **skip**, so it stays gone. To bring a range back, a new **Deleted ranges** panel on the Delete Blocks page lists each deletion (in wall-clock time, annotated with whether it's still within the API's reach) with a **Re-import** button that lifts the tombstone and immediately re-fetches from the supplier API; the per-gap **🩹 API** fill and a CSV fill of that window do the same, and the gap list labels a tombstoned hole *"deleted — fill to restore"*. A blanket **"recover all gaps"** deliberately steps over deleted ranges, so one click can't silently undo your deletions. *(Additive and billing byte-identical: the tombstone only ever gates block **creation** — it never touches a stored figure, a chart, or a settled value. Not retroactive — gaps that already exist when this lands are treated as before, since there's no way to tell an old deliberate delete from a missed bill.)*

### Fixed

- **Intelligent Octopus off-peak charges outside the standard window are no longer left at peak when EMT was offline / after a re-import.** When a smart dispatch charges your car *outside* the fixed off-peak window (e.g. an evening top-up), Octopus bills it off-peak — and EMT already reprices it off-peak from the dispatch lifecycle it captures **live** (the "started" signal). But if EMT was **offline** for that slot, or the day was **deleted and re-imported later**, the live signal was never capturable and only Octopus's **completed** dispatch record survives — which EMT previously treated as *observe-only*, so the slot stayed at the **peak** tariff and the block was silently under-credited (a real case: a 20:00 slot billed at 32.31p/kWh that should have been 5.493p). EMT now accepts a **completed-only** dispatch (no planned/started ever seen) as the authoritative smart-charge record and reprices it off-peak at settlement reconciliation, stamped `rate_reconciled` and marked `smart-charge-completed` for audit. The overlay still gates on **real meter draw**, and **imported history is never touched** (it carries Octopus's actual billed rate). *(Deliberate trade-off, documented in the dispatch design note §14: a completed dispatch alone can't tell a smart charge from a manual boost, so a genuine offline **bump** will also be credited off-peak — accepted because under-crediting a missed smart charge is the worse default, and a bump is correctable in the corrections tool. A slot EMT **did** see planned-but-never-started is still left at peak and flagged for review, unchanged.)*

- **The Total Bill doubled once you had more than one billing period (#361, the real root cause).** The bill total comes from `compute_period_net`, and its query joined the meter table on the meter id **without also matching the config period** — the single place in the codebase that omitted that constraint. Because each meter gets its own row per billing period, once a second period existed every half-hour block matched **both** meter rows and was counted **twice** (with N periods, N times). The standing charge was spared — it's taken once per day — so only the import and export halves inflated, which is exactly why the bill's own sections still looked right while the **Total Bill at the bottom was wrong**: a real case read **−£147.28 against a correct −£65.09** (precisely ×2). It struck the moment you created a second billing period. The join now matches the block's own config period, so every block is counted once. *(Removes an over-count only — a single-period install was never affected and no correct figure moves. Reproduced against the reporting instance's database — −147.28 buggy vs −65.09 fixed — and locked with a regression test.)*

- **Creating, editing or removing a billing period no longer races the engine (#361).** These config writes used to run on the web server's database connection while the engine was mid-write on its own, surfacing as `database is locked` or a raw `another row available` and sometimes leaving a period **half-applied**. All three now run **serialised on the engine's own loop and connection**, with the billing charts regenerated **off the event loop** afterwards (coalesced, so a change landing during an in-flight render still re-runs). Create and edit were routed first; **removing a period** — which still raced and failed with `another row available` — now takes the same path. *(Serialisation and render only — no stored data or billing figure changes.)*

- **The Charts page now refreshes after a billing-period change, not only after a settlement.** The Charts view reloads its pre-rendered billing/heatmap charts whenever a lightweight change-token moves — but the token fingerprinted only block *values*, and a config change rewrites the charts without touching any block value, so the page kept showing the old charts until the next block finalised. The token now also fingerprints the **rendered chart files' timestamps**, so it advances exactly when the freshly regenerated charts land on disk (never before), and the Charts page picks them up on its next refresh or when you focus the tab — instead of latching a stale file and waiting for a finalise.

- **Octopus credentials now survive a database swap without silently reconnecting — or looking like they were lost (#357).** Two related problems when working across more than one database (e.g. analysing another account's export). First, a **failed "Connect" could look like it had thrown away your key**: the credentials are saved to their own file *before* the live connection is verified, so a **transient** verify failure (a rate-limit, a network blip, or the database being mid-swap) returned a bare error even though the key had been kept — and then a later automatic connect succeeded, which looked like it had "reconnected with credentials I never entered". Second, after swapping to a **different account's** database EMT could **auto-reconnect the wrong account** into it. EMT now **stamps each database with its Octopus account** on first successful discovery: if the credentials present are for a *different* account than the loaded database, the API is **not auto-activated** (so it can never poll the wrong account's data into that database), and the config screen surfaces the mismatch so you can reconnect deliberately. A failed connect now reports **"credentials saved, but couldn't verify right now — kept, will connect when reachable"** instead of a bare error, and **nothing automatic ever deletes a key** — the only removal is the explicit **Disconnect** button. *(Only the add-on's own credentials-file lifecycle changed; credential values are never handled differently and billing is untouched.)*

- **Restoring a database from an uploaded file now takes a safety backup and shows progress, exactly like restoring from a backup zip (#356).** Uploading a `blocks.db` to restore went through a **separate, synchronous** path with **no pre-restore safety backup, no progress, and no "don't restart the add-on" warning** — the request just blocked until the swap was done, and a bad moment left no undo. It now runs through the **same background restore job** as the in-app backup restore: the upload is staged and **validated as a real EMT database first** (a wrong file is rejected immediately, with the live database untouched), then a **pre-restore safety backup** is taken, any running import is stopped, the file is swapped in **atomically**, stores are reset and gap-detection runs — all reported through the same **live progress banner** you can safely navigate away from. *(Restore mechanics and UX only — no billing change.)*

- **A standing-charge rate change mid-period now shows as separate lines in the ex-VAT bill method, instead of one averaged rate that matches neither.** When the daily standing charge changes within a billing period (a price-cap or tariff switch — e.g. March's £0.476317/day until the switch, then £0.504559/day), the ex-VAT bill-method breakdown collapsed it into a **single averaged rate** that matched neither half of the period and didn't line up with the statement. The standing charge is now **grouped by distinct daily rate**, so each rate shows on its own line with its own day count — and the rows still **sum to exactly the same standing total** (no money created or lost). A period with a single standing rate is unchanged (one row).

- **Deleting a date range with explicit start/end times deleted the wrong blocks.** `FROM 14/04 00:00 → TO 15/04 12:00` reads as one continuous 1½-day span, but the two times were applied as a **per-day time-of-day filter** — so it deleted `00:00–12:00` on **both** days (48 blocks) rather than the continuous span you meant (73 blocks), leaving two half-day gaps instead of one. The delete — and its preview and count, so all three always agree — now treats the times as the **start and end of one contiguous range**. A whole-day delete (`00:00 → 23:59`) is unchanged; only a delete with explicit times changes, to the intuitive behaviour. *(No stored-data or billing change beyond removing the blocks you actually selected; the deletion tombstone now matches the contiguous span exactly.)*

- **Outage and delete-and-reimport gaps that had aged out of the poll window now backfill.** The 6-hour DCC poll window anchored only to the oldest **unsettled** block, which can't see a hole that has blocks on both sides — so an interior gap (between a register-measurement window ending at T and a DCC poll starting at T+n, or a day you re-imported) was invisible to it and never filled. The window now also pulls back to the oldest such **hole**, bounded to a 14-day recovery horizon so it never chases ancient, un-resettleable gaps, and BL-8 fills it on the next poll. *(Fills only empty blocks — no billing figure changes.)*

- **A large delete or chart rebuild no longer triggers a Home Assistant reconnect storm.** Rendering the charts over a big history is CPU-bound and briefly saturates the process, which could stall the HA WebSocket heartbeat and drop the connection — and because a reconnect re-ran the **whole** engine startup (which renders the charts again), it fed a self-perpetuating reconnect→startup→render loop that looked like the add-on restarting itself repeatedly. A reconnect now **skips the heavy startup re-run** when a startup completed recently **or** a chart render is in progress / just finished (the render being the usual trigger); the WebSocket is re-subscribed regardless so nothing is missed, and a genuine HA restart after a real outage still re-runs startup. *(Moving the render off-process to remove the underlying CPU contention entirely — which also causes brief "waitress task queue" blips during a render — is tracked as BL-25.)*

- **A solar export meter no longer shows a permanent, un-fillable export "gap" over its overnight hours.** A PDF / CSV / API historical import of export data only lists the hours that actually exported (daylight, on a solar meter), leaving the overnight slots **blank (`NULL`)** — and a blank export slot is indistinguishable from genuinely-missing data, so the gap list showed a permanent *"export · data · 72 h"* that could **never clear** (the API returns nothing for a slot with no export, so a re-fill never fills it, and the self-clear needs every slot present). Now any **historical** export import — CSV, PDF-reconstructed, or the API date-range / gap-fill — writes `0` for the blank export slots on days that carry export (a solar meter's overnight is genuinely zero, matching how the surrounding fully-imported days already store it), so those slots read as present and the phantom gap clears. A **one-off migration on upgrade** applies the same to existing history — **day-scoped** (only days that already have an export value, so a genuinely un-imported range is left alone) and **imported blocks only** (a live block awaiting DCC settlement is never touched). *(Billing-neutral: zero export is £0, identical to a blank — no figure moves. The 4.2.1 "never write a false-0 export" rule still holds for the live poll; this applies only to settled historical imports.)*

### Changed

- **Saving or removing a billing period now shows what's happening.** Both actions rebuild the billing charts server-side, which takes a few seconds on a large history — so the buttons used to sit there looking like nothing had happened. Saving a period now disables the button and shows **"Saving… regenerating charts"**; removing one dims the card and shows **"⏳ Deleting… regenerating charts"** until the period disappears from the list. Buttons re-enable on completion (or restore cleanly on error).

- **A block delete is refused while a supplier backfill is running.** A delete that landed mid-poll could race the BL-8 backfill re-creating the same day, leaving a half-deleted range. The delete now returns *"Unable to delete — a backfill is in progress; try again once it completes"* until the poll finishes, and the poll likewise skips a cycle while a delete is mid-flight, so the two can never interleave. *(Serialisation only — no data change.)*

- **The gap list and the Deleted ranges list now show local (wall-clock) time.** They previously printed raw UTC block-start times (so a BST midnight read as `23:00` the day before); they now display times in your configured timezone. The underlying UTC values still drive the fill / re-import, so only the display changed.

## [4.2.2] — 2026-08-13

*Agile (half-hourly-priced) fixes — two display fixes and one bill-import fix. The display changes are presentation-only (no stored data, aggregation, or Total Bill change); the import fix recovers plunge half-hours that were being dropped from PDF-bill imports.*

### Fixed

- **Agile plunge half-hours are no longer dropped when reconstructing history from a PDF bill.** The bill parser transcribes each half-hour from the statement's per-day table, but its row pattern only matched **positive** rates and costs — so an Agile **plunge** slot (a *negative* unit rate, where Octopus pays you to consume, e.g. `07:00 - 07:30  -0.67  3.77  -2.526`) failed to match and the **whole row was silently dropped**. That left a **gap at every plunge half-hour**, and — because a day page is only accepted with **≥40 recognised rows** — a **whole day could be dropped** on a heavy-plunge day. The parser now accepts a negative **rate** and **cost** (consumption stays non-negative — import kWh is never legitimately negative), so plunge slots transcribe with the correct sign. Confirmed against a real 2024 Agile statement where **5 plunge rows on one page** had been lost. **Only the PDF-bill import was affected** — the supplier-API import and the direct Octopus consumption-CSV import already handled negative rates correctly. Re-import any affected bills to recover the missing plunge slots. *(This does not touch already-stored data; it only changes what a fresh bill import captures.)*

- **Agile plunge-price half-hours now show the rate line dipping below zero on the day chart.** On Agile the unit rate can go **negative** (Octopus pays you to import). The 4.1.3 fix already made the negative *cost* display correctly, but the day chart's unit-rate step-line still flattened along the zero baseline on those slots, so a plunge period looked like a flat-zero rate rather than a dip into credit. The cause was the axis, not the data: on an **import-only day** there's no export to pull the axes below zero, so the rate (right-hand) axis was pinned at 0 and clipped the negative rate. The rate axis now drops just below zero to fit the lowest negative rate of the day — keeping the energy and rate **zero-lines aligned** (the same technique already used when export is present) — and labels the sub-zero ticks, so the rate line visibly goes negative through a plunge. The rate value itself was always correct in the data; this only stops the axis from hiding it. *(Display only — stored data and billing unchanged.)*

### Changed

- **A long list of rates now collapses to a single average row (Agile).** An Agile meter prices **every** half-hour differently — ~48 distinct rates a day, hundreds across a billing period — so the per-rate breakdown printed a row for every rate and became unreadable. When a channel has **more than five distinct rates**, EMT now folds the per-rate rows into **one kWh-weighted average row**, labelled *"(avg of N rates)"*. It applies everywhere the per-rate detail appears: the **billing summary** (both the standard breakdown and the ex-VAT bill-method table) **and the daily chart's side panel** — covering the main import, the **Direct import** (house remainder) line, each **device** sub-meter, and export. Each place is scoped to its own bounds (the side panel averages over that **chart day**; the billing summary over the **billing period**). **The Total (and Total exc) figures are computed exactly as before** — from the same per-rate sums — so no number moves; only the unusably long per-rate *detail* is summarised. A fixed or small-tariff meter (five rates or fewer) still shows the full per-rate breakdown unchanged. *(Display only.)*

## [4.2.1] — 2026-08-12

### Added (ex-VAT figures, VAT calendar & settlement/backfill fixes)

- **Ex-VAT (pre-VAT) figures, retained at source and shown on an opt-in toggle (BL-23).** Octopus returns the exc-VAT unit rate and cost on every import and settlement, and EMT used to keep only the inc-VAT side and reconstruct the rest as `inc ÷ 1.05`. EMT now **stores the real exc figures** — `imp_cost_exc`, the derived `imp_rate_exc`, `standing_charge_exc` and an `exc_source` marker — captured at both **import** and **DCC settlement**, so live and settled history carry the exact pre-VAT numbers for free from here on. A new **ex-VAT display toggle** (for business / reimbursement use) reads them; the lightweight block-fetch that renders the view now surfaces the exc columns rather than silently falling back to `inc ÷ 1.05`. Existing history is filled once by a **one-time backfill** (period tariff rate × stored kWh), written in batched transactions with paced `await`s so it never blocks the Home Assistant event loop the way a naïve per-block backfill did. **Inc-VAT figures are byte-identical** — the ex-VAT side is purely additive, default-off, and never re-prices a stored block. **Note: the ex-VAT view is currently available only on the API path.** It's gated on captured-exc coverage, so CAD/no-API setups keep it hidden — even where **imported (CSV / bill-reconstructed) data already carries the exc information**, the toggle stays off for now; surfacing ex-VAT for imported-only history is a follow-up.

- **A VAT calendar so the inc↔exc conversion is exact across VAT changes (removes the hardwired ÷1.05).** Instead of a single hardcoded 5% (`÷1.05`), EMT resolves the VAT rate **per period** from a calendar: a **seed** (domestic electricity at 5% since 1997-09-01) plus rates **learned from each tariff refresh** (the inc/exc ratio in the rate schedule, snapped to the statutory {0, 5, 20}% and merged into contiguous spans). The four places that assumed 1.05 (slot fallback, side panel, client inc-VAT, bill-method default) now read the period's calendar VAT, and the summary VAT is computed as **inc − exc**, so a **VAT-holiday boundary that falls inside a billing period is exact** rather than smeared. **Negative (plunge-pricing) Agile prices** are handled correctly by carrying the inc/exc pair (VAT applies multiplicatively even to a credit), so a −ve price keeps its ~5% VAT relationship with the right sign.

- **Bill-style rounding, ex-VAT method (BL-24, opt-in).** An opt-in totals mode that applies Octopus's reverse-engineered rounding **on-read at the totals layer only**, running off the stored exc figures from BL-23 — it never mutates per-slot data, needs no re-import, and is reversible. Default stays **exact** so no one's figures move unless they opt in; the ex-VAT bill method replaces the old inc-VAT import section in the design, labelled as matching Octopus's *method* rather than being penny-perfect.

- **Overlay a single channel over an existing date range — backfill FIT export from a third-party CSV.** A FIT meter gets no export half-hours from Octopus, so its owner has to bring export from a third-party feed (Glowmarkt / Bright). Once import had been filled back past the target date, **no CSV route would accept it**: gap-fill saw no gap (import was present) and the CSV *date-range* route hard-refused any window at or after your earliest data. The CSV date-range step now has a **Channel** selector — the default stays "Import + export (extend history back)", while **Export only** (or **Import only**) switches to an explicit **from → to** range that may overlap existing data and scopes the upload to that one channel. It's safe because the store's **per-channel first-in-wins merge** only fills the empty column and never touches the other channel — an export CSV lands on existing import-only rows without disturbing import (locked with two store tests). Users add a unit-rate or cost column if they have one; export with no cost simply isn't costed.

### Fixed

- **Export settlement no longer looks stuck behind import on a Home Assistant Mini.** Import settles live (per tick) while export only settles later via DCC, released newest-window-first — so a day could show import for every half-hour but export only from the point settlement had crept back to. It was correct and self-healed over days, but looked broken. The 6-hour poll window now also **anchors to the oldest unsettled block on any channel** (floored at the backfill horizon), so each poll chases the lagging export instead of waiting for the once-a-day settlement sweep. Self-bounding; fills only empty columns, so **no billing figure changes**.

- **Gap-fill never writes a false-0 export block for a DCC-only export channel.** Filling a missing block used to stamp `kwh = 0.0` for *every* configured channel. On a meter whose export has no live source, a half-settled day would get a real **0** written over slots that were merely awaiting settlement — and because the first write wins, that 0 then beat the real figure when it arrived. Export with no reads is now **left unmaterialised** (logged) so settlement fills it; **import keeps its 0.0 fallback unchanged** (it's billing-critical and always live).

- **Ex-VAT reads real (not approximate) across the whole go-live→upgrade window, not just imported history.** The one-time ex-VAT backfill originally filled only **historically-imported** blocks, so an instance that had been running live for a while before upgrading showed real exc for its imported past but the **≈ approximation for everything after its go-live date** — the blocks captured live in the months before the upgrade had already DCC-settled, and settlement capture only stamps exc when a block *re-settles*, so they were never reached. The backfill now also fills **settled live blocks** (reconstructing exc from the tariff, the same method), and its completion marker is **versioned** so an instance already marked "done" at the narrower scope **re-arms once** and fills the newly-covered blocks on the next start. Truly-unsettled live blocks are left for settlement capture. (Additive; inc-VAT figures byte-identical. Sub-meter/`recorder_attributed` device blocks stay out of scope — they never carried captured exc and the ex-VAT bill total is the main import alone.)

- **The ex-VAT backfill now drains to completion in one run, instead of one pass per restart.** The backfill fills in bounded passes; it used to run a **single pass per engine startup**, so a history larger than one pass (a couple of years of half-hours) stalled part-way — the marker sat mid-history with no "done" flag and everything after the cursor stayed on the ≈ approximation until the *next* restart happened to run the next pass. The scheduler now **loops the passes to completion in the same session**, breathing between them so the Home Assistant heartbeat and polling stay responsive; a fresh install fills in one go, and an interrupted run still resumes from its cursor.

- **Chart regeneration after settlement, reconcile or gap-fill no longer drops the Home Assistant connection.** Those paths regenerated the charts **inline on the engine loop**; on a large history the render took long enough to stall the HA WebSocket heartbeat, which dropped the connection and re-ran engine startup — a self-perpetuating reconnect loop. It bit harder in 4.2 because the new **oldest-unsettled poll window** re-settles the last couple of days of blocks each cycle (so the re-price drain ran far more often) and the ex-VAT billing path made each render heavier. Chart regeneration on these paths now runs **off the event loop** via the existing read-only render, keeping the loop responsive. *(Render only — no data or billing change.)*

## [4.1.3] — 2026-08-09
 
### Added
 
- **See your EV charging split from the rest of the house — even with no charger sensor (Intelligent Octopus).** If you're on Intelligent Octopus but have **no EV sub-meter** configured, EMT now reconstructs the **EV vs house** split from Octopus's own **completed-dispatch** data (the per-slot energy it moved into your smart-charge window) and shows it as an **"EV (from dispatch)"** device across the app: the **Insights → Usage** EV card, the **Charts → Usage Stats** bar chart (its own stacked segment, a new data-table column, and a Spiral grouping), and the **Charts → Billing** tab (an "EV (from dispatch)" line in the bill breakdown, plus a segment on each per-day chart). Each half-hour's EV energy is **grid-clipped** to what the grid actually delivered that slot and its cost **apportioned** from that slot's import cost, so **house + EV always sum back to your grid import exactly** — every chart total, the data-table Totals, and the **Total Bill are byte-identical** to before; only the *breakdown* is subdivided. Validated at **~99% against a real CT-clamp EV meter** over 19 days (the ~1% is clamp measurement scatter, not missed energy). It's **display-only** and a **guaranteed no-op** for anyone who already has a real EV meter (or any sub-meter). *Caveat:* accuracy tracks charging discipline — energy taken **outside** a smart dispatch (a manual/boost charge) isn't dispatch-attributed and won't be counted here.

### Fixed
 
- **Agile plunge-price credits are no longer dropped from the charts.** On Agile, a half-hour can go **negative** (Octopus pays you to use power). The daily/half-hour aggregation and several chart/summary paths floored each slot's cost at zero, so a genuinely-negative import cost was silently discarded and a credit day could even show as a small charge (a real case: a day stored at **−£1.12** displayed as **+£0.22**). Negative import costs now survive aggregation and display in every affected path, so credit slots and credit days show correctly. *(Stored data was always correct — this was a display/aggregation clamp only; the Total Bill is unchanged.)*
- **The smart-charging card no longer flashes charge slots red before they settle.** Delivered smart-charge slots could briefly draw in the **peak (red)** colour until the billed rate landed, even though they were off-peak dispatched charging. The card now colours a slot from its **dispatch source** — a genuine smart-charge slot shows **off-peak** immediately, and only a true **bump/boost** (or a confirmed over-window slot) shows red — so red once again means "actually peak", not "not known yet".

### Changed
 
- **Groundwork for bill-accurate rounding and ex-VAT figures (no visible change yet).** Two additive, default-off foundations for the 4.2 billing work: (1) a nullable **`imp_cost_exc`** column on the block store that round-trips an ex-VAT import cost when one is present (existing rows stay `NULL`; **every inc-VAT figure is byte-identical** — verified across a full history), and (2) a pure **`octopus_bill_total()`** helper implementing Octopus's rounding ladder (per half-hour: round kWh to 0.01 half-even → × ex-VAT rate → round cost to 0.01p half-even → sum → whole penny, VAT on top), with a Decimal-based `bankers_round()` so exact decimal halves round correctly. Both are **wired to nothing** — the live billing path is unchanged — and exist so **4.2** can add the ex-VAT capture at import and an opt-in "bill-style rounding" totals mode that makes displayed totals match how Octopus builds a bill.

## [4.1.2] — 2026-08-07

### Changed

- **Smart-charging card now estimates actual charging time, shown next to the dispatched window (experimental).** The merged dispatch-window figure the card used to label "charging" is now labelled **"dispatched"** (the time the car was dispatched, at half-hour granularity), and a separate **"≈ charging"** estimate sits beside it. The estimate infers the car's active charge power from the *fullest* delivered half-hour and divides total delivered energy by it — a car-agnostic, rate-limit-proof read of how long it actually charged. On a real overnight charge (5 slots, 11.41 kWh, ~6.4 kW) it showed **≈1h 47m against a measured 1h 46m**, where the dispatched window read 2h 30m — the gap being a trickle half-hour that counts as a full 30 minutes dispatched but was only ~2 minutes of real charging. Guards keep it honest: it appears only when a slot was full enough to anchor the power, is clamped to a plausible charger ceiling, and can never exceed the dispatched window. Still **experimental** — a charge that ends on a long low-power taper will read low, so treat it as an estimate, not a meter.

- **Smart-charging card's per-slot chart now shows local time, not UTC.** The expandable half-hour bars (and their tooltips) labelled each slot with the raw UTC time, so under British Summer Time they read an hour behind the session's start/end window (which was already local). They now use the same site-timezone conversion as the rest of the card, so a 02:00 BST slot reads 02:00 everywhere. Display only — stored data is unchanged.

## [4.1.1] — 2026-08-06

### Fixed

- **The import page no longer gets stuck showing "A backfill is running" after an add-on restart.** When an API import finished, EMT saved a durable summary of the run so the import panel can still show "N blocks imported" on any later page load — but it captured that summary a moment too early, while the job was still in its **finalising** phase (the status flips to *done* immediately after). While the add-on kept running this was invisible, because the panel reads the durable summary *only* when no job is live in memory. After a **restart** — an HA reboot, an add-on update, a host restart, at any time after the import — there was no live job, so the panel fell back to the saved summary, read *finalising*, and (because that's a mid-run status) drew the "A backfill is running / Finishing up…" lock with **no job behind it and no way to cancel**; every reload re-read the same frozen summary, so it looked like an import that had been running for hours. The saved summary now always records a **terminal** status, and the panel **never treats a restored summary as a live run**, so a completed import stays completed across restarts — and anyone already stuck is freed on their next page load. *(Display state only — no effect on imported data or billing.)*

- **Historical carbon backfill no longer stalls when a full postcode was stored.** The Carbon Intensity API accepts only the **outward** part of a postcode (e.g. `CO3`), and EMT is designed to store just that — but if a full postcode (e.g. `CO3 4SE`) reached the backfill (a legacy value, or one that slipped past normalisation), the space in it made every request fail with *"URL can't contain control characters"*, and the backfill retried the same window forever without advancing (seen as repeated "pausing, will resume" in the log). The postcode is now **normalised to the outward code at the point each request is built**, in both the live and the historical carbon paths, so any stored value works and the backfill resumes and completes. *(Carbon only — no effect on billing.)*

### Changed

- **Smart-charging card now shows time spent charging and the number of slots (experimental).** Each charging session gains a **time spent charging** figure and a **slots** count. The duration (`dispatch_minutes`) is the merged union of each delivered slot's **planned dispatch window, clipped to its half-hour** — the wall-clock time the car was actually dispatched, excluding the idle gaps between bursts. Planned windows carry Octopus's real sub-slot bounds where they exist (a dispatch that finished early, a short top-up), which `completedDispatches` don't (they're always 30-min slot-aligned); clipping to the slot stops a multi-hour plan bleeding across a gap. It's **rate-limit-proof** — unlike the previous energy-scaled figure (`30 × kWh ÷ fullest-slot`), which under-reported when an EV drops to a trickle near full charge, this never divides by energy. **This is experimental and still being refined:** where Octopus reports a delivered half-hour as a full 30-minute dispatch (common mid-charge) the figure reads the full slot, so it can over-read the true wall-clock time; the sub-slot *actual* charging duration only exists in the charger's own data, not the dispatch ledger, so treat the figure as "time dispatched", not a precise charge time.

## [4.1.0] — 2026-08-05

### Fixed

- **Usage Stats now refreshes on an export-only settlement too.** The change token's value fingerprint (added in 4.0.1) summed import kWh/cost and carbon but not **export** kWh. Since export settles later than import via DCC, a settlement batch that moved only `exp_kwh` left the token unchanged, so the Charts/Usage-Stats client kept a stale export figure until the 5-minute TTL (Billing, server-rendered, updated immediately — hence a transient export mismatch between the two views). The fingerprint now also sums `exp_kwh` (still a single index-only scan), so any export change refreshes the views promptly.

- **Usage Stats no longer over-counts a device that charged from solar/battery (grid share of exactly 0).** The per-device import used `imp_kwh_grid or imp` — Python truthiness, so a legitimate grid share of **0** (a block where the device charged entirely from solar/battery, drawing nothing from the grid) was treated as "missing" and fell back to **total** consumption. This inflated devices that self-consume — the **house battery** most of all (in one year, 81 solar-charged blocks added ~2.4 kWh). Fixed to distinguish a real 0 from NULL (`imp_kwh_grid if not None else imp`), in both `_aggregate_usage` and `api_blocks_summary`, so device import counts the grid share and now matches the Billing breakdown exactly. (The Insights page's separate total-consumption view is unchanged.)

- **Billing chart's per-device breakdown now shows each device's grid share, so it reconciles with the grid import total and with Usage Stats.** The "Breakdown by meter" rows displayed each sub-meter's **total consumption** (`imp_kwh`) — which includes energy the device drew from your **battery/solar**, not the grid — while the rest of the billing summary (and Usage Stats) uses the device's **grid-attributed** share (`imp_kwh_grid`). So a device that charged partly off-battery/solar was over-stated: the breakdown summed to **more** than "Import — total grid" (in one year, Direct + Zappi + Solax = 7212.0 kWh against a 7172.8 kWh grid total — a 39 kWh overshoot, exactly the self-consumed energy), and its kWh no longer matched its grid-based cost. Sub-meter rows now use the grid share (matching the summary's own reconciliation line and the Usage-Stats aggregation), so Direct import + devices = grid import total exactly and the two views agree. **The Total Bill is unchanged** — it's computed from the main meter alone and never used these sub-meter figures.

### Changed

- **Dispatch history now retains the *actual* charge window, not just the plan (observe-only).** EMT recorded the precise `start`/`end` for **planned** Intelligent dispatches but discarded them for **completed** ones — so a `completed` dispatch that ran only part of a slot (a short top-up, or a charge that ended mid-slot) was stored with no window and looked identical to a full planned half-hour block. `dispatch_history` completed rows now carry the real to-the-minute window (`_completed_dispatch_slot_bounds`), giving genuine per-dispatch charge time. This is groundwork (the ledger is observe-only and not read for billing; the billing `dispatch_slots` row and its planned window are unchanged), and it's the prerequisite for the IOG 6-hour-cap work (BL-9) — the cap is reckoned on completed-dispatch time, which EMT can now measure.

## [4.0.1] — 2026-08-05

### Fixed

- **Device attribution now heals a sub-meter that flat-lined at zero.** If a device's own sensor recorded **zero for a stretch at the start of its live period** while the device was really drawing power (the load then sat unsplit in the house remainder), attribution used to stop dead at that zero: it treated the first live block — even a zero one — as the boundary where reconstruction must hand over to real data, so re-running Attribute rebuilt only up to the gap and never filled it. Attribution now recognises a **zero-valued live block as a hole, not coverage**: the join seam advances to the first block with **real** import, and the reconstruction fills the zero slots from the recorder (clipped to house import, house remainder re-derived, as always). Real (non-zero) device readings are still **never** overwritten, and a genuinely-zero slot with no recorder energy behind it is left untouched — so healing a dropout no longer needs a manual delete of the empty blocks first. Grid/billing totals are unaffected; only the device split changes.

- **Usage Stats now refreshes after an in-place edit (attribution / reprice / carbon).** The Charts and Usage-Stats change token — which lets the pages skip a needless rebuild when nothing has changed — was built only from the block **row count** and **newest block**. An edit that rewrites existing blocks *in place* (a device attribution moving energy off the house remainder, a settlement reprice, a carbon backfill) changes neither, so the token didn't move and the pages kept serving their cached values until the 5-minute TTL expired — e.g. Billing charts updated after healing a sub-meter but Usage Stats still showed the old split. The token now also includes a cheap **value fingerprint** (summed kWh / cost / carbon, a single index-only scan), so any change to the underlying figures refreshes the views promptly.

## [4.0.0] — 2026-08-05
 
### Added

- **Build a CSV from your Octopus PDF bills — reconstruct history the API can't reach.** For history older than the supplier's ~2-year API window, Data Management → Historical import → **CSV → "From Octopus bills"** lets you point EMT at the folder of Octopus bills on **your** computer (or pick the PDFs). EMT reads them and builds an **Import and Export CSV**, which then flow through the normal review → confirm → import — so nothing new touches the block-writing path, and you can see exactly what will be written first. Intelligent Octopus **half-hour pages are transcribed exactly** (billing- and dispatch-accurate); flat/older tariffs and export are **reconstructed from the bill's period totals** and clearly flagged. Every bill is **checked against its own printed totals**, so a bad read is caught rather than silently imported. Bills are grouped by **meter/MPAN** — a folder that spans a house move is split out and flagged so each site imports cleanly. (PDF parsing uses `pypdf`, imported lazily: a build without it simply hides the feature.)
 
- **Historical import — backfill your full history from Octopus.** A new import wizard (Data Management → Historical import) pulls your past half-hourly data straight from Octopus's GraphQL **Measurements** API: for each half-hour it records the **kWh**, the **exact billed cost**, and whether it was **off-peak or standard rate** — reading the supplier's own dispatch-aware label, so **Intelligent Octopus (IOG)** charges that were moved into the off-peak window are priced correctly, not guessed. It reaches back as far as the supplier's Measurements API retains — a rolling **~2 years**; the plan shows the earliest reachable date and warns that **older history (e.g. an early tariff trial) has aged out of the API and must be filled from your bill via the CSV route**. The import runs as a **background job** with **Pause / Resume / Cancel** that keeps going if you navigate away, shows live progress, and is deliberately **rate-limit-polite** — it watches the API points allowance and backs off with headroom to spare, so it won't starve EMT's own live polling (or another app such as BottleCapDave sharing the same Octopus key). Import proceeds newest-to-oldest and stops cleanly at a gap.

- **CSV import as an alternative backfill path.** If you'd rather not use the API, you can import an Octopus consumption CSV: EMT parses it, derives rates, previews the reconciliation, and applies it into the same block store.

- **Fill gaps from a ready-made CSV.** When the import leaves gaps (a DCC/settlement outage the supplier can't fill), each gap on the import page now has a **⬇ CSV to fill** link that downloads a CSV **pre-filled with a row for every missing half-hour** — you just add the consumption and cost from your bill and re-import. The timestamps are written in your **local time with the correct BST/GMT offset** so they line up with what you see in your Octopus account, and the columns match the importer exactly. **A one-click get-out-clause if an import goes wrong.** Data Management → Delete blocks now has **"Delete all historical imported data"** — it removes every imported block (API or CSV) and everything derived from it (rate derivations, import checkpoints, gap records), taking a **backup automatically first** and leaving your live/CAD/settled data untouched, so you can re-import cleanly. The date-range delete tool also gained an **"Only historical imports"** filter, so you can wipe just the reconstructed blocks in a window and leave live data in place. (Because it removes whole blocks, there's no half-apportioned device data left behind.)

 There's also a **blank template** download by the CSV upload box (headers + example rows in the right format) for anyone supplying their own consumption CSV. After a CSV import, Billing history offers a **"Name your imported data"** panel to set the region (for carbon) + a site name for the imported span; skip it and the data simply stays in your existing billing period (carbon-excluded until you set a region).

- **Region timeline foundation (for correct historical carbon).** Carbon intensity is regional, so attributing carbon to *imported* history needs to know which DNO region applied *when* — not one global postcode. EMT now resolves the region **per config period** (`get_postcode_prefix_at`), and both at startup and after an API import it reads the account's property history and stamps the region onto the period timeline automatically. **Privacy: only the outward part of the postcode is ever stored** (e.g. `SW1A` / `DE65`, enough for the DNO region and nothing more) — full postcodes are read transiently and discarded. On the **first run after upgrade** the authoritative supply-address region **overwrites** a hand-entered one (so a legacy wrong-region entry self-corrects); after that, manual edits are respected and aren't re-clobbered — and CSV-only setups (no API) keep full manual control. A house move shows as multiple regions and is flagged for a deliberate period split rather than guessed at. The **Billing history → Edit Config Period** dialog now has a **Postcode (carbon region)** field (outward code only) so you can set/correct the region per period by hand, and each period row shows its region. A **historical config-period split** primitive now underpins recording a mid-history property move: it divides an existing period at a past date, deep-copies the meters and channels into the new half, and reassigns each block to the correct side by its timestamp — so a move can be recorded without disturbing billing history. And after an API import that finds a **property move** (or a site with no name yet), Billing history shows a **"Confirm your sites"** panel: the date ranges and regions are filled in automatically from your account, you just **name each site**, and applying splits the timeline at the move dates and stamps each period's region + name. A single, unchanged site needs no confirmation and is stamped silently. This is groundwork; historical carbon backfill for imported blocks builds on it (see roadmap).

- **Name your sites *before* the import, so multi-address history files itself correctly.** If your Octopus account shows more than one address, the API import now asks you to **confirm your sites up front**: after *Preview plan* it lists each address it found (dates + region), your **current** site read-only (it's this instance's identity, left untouched) and each **past** address with a box to name it. *Start import* then creates a config period for each past address **first**, so every imported half-hour lands in the right period — correctly regioned and carbon-eligible — from the moment it's written, with no after-the-fact reshuffling. A **never-moved account sees none of this** and goes straight to import. (This is the "discover → confirm → import" flow; the older post-import "Confirm your sites" panel remains for CSV imports, which carry no address history, and as a fallback.)

- **Historical carbon now fills your imported history.** Once a period has a region, the Carbon-Intensity backfill **includes imported blocks** (previously it skipped everything imported), so the Carbon / Insights views and heatmaps extend back across your backfilled range instead of stopping at the date carbon recording began. The backfill fetches intensity **per region** — if you've moved, older blocks are attributed to the region they were actually in, not your current one — and imported blocks whose region is still unknown are left blank rather than guessed. Assigning or correcting a region (via the import confirmation panel or the per-period Postcode field) automatically re-runs the backfill for the newly-eligible range.

- **Usage Stats — faster month navigation and a month-across-years comparison.** On the Usage Stats page the period selector (the "3 Jul – 2 Aug" label between the ← → arrows) is now a **rolodex**: hover it and **scroll** — or drag — to spin through months quickly, instead of clicking one arrow at a time (the arrows still work). The **Monthly** view gains a **Date / Month** order toggle: *Date* keeps the usual chronological order, while *Month* groups every period **by month first, then year** — so **April 2025 sits right beside April 2026** for a like-for-like comparison. In month order each bar is **shaded by its year** (older lighter, newer darker) so the years in a group are easy to tell apart, and every bar carries its month **and** year on the axis. Works for both **Billing** periods and **Calendar** months. The same **scroll/drag rolodex** is on the **Insights** page period selector too.

- **Usage Stats — a new HH (half-hour) view for a single day.** Alongside Daily / Monthly / Yearly there's now an **HH** period that breaks one day into its **48 half-hour blocks**, so you can see exactly when the day's energy, cost and carbon happened. It mirrors the Daily view — the same floating toolbar, the kWh / Cost / CO₂ and Totals / Net toggles, the *Include standing charge* option, the data table and the PDF export all work — and it always shows a **full 48 blocks**, zero-filled for slots that haven't recorded yet. It defaults to **today** and never scrolls past it; the day picker is the **same rolodex** as the other periods (hover-scroll through days with the ±4-day preview) plus a **📅 calendar button** to jump to any date. The figures come from a new endpoint that shares the **exact billing-accurate aggregation** the daily bars use, so the 48 blocks always sum back to the day's total.

- **Spiral — Cost, Usage and Carbon side by side.** The Spiral view no longer switches between one metric at a time: it now shows **Cost, Usage and Carbon spirals together** (two when carbon data isn't available), with a separate **Standing charges** spiral. One shared **Grouping** selector — **Net / Import / Export / each device** — drives all of them at once, so they read like-for-like: **Net is consistently import − export** across all three (a new *net usage* = import − export kWh; net carbon = import − export offset; net cost as billed), letting you compare cost, energy and carbon directly. An **Include standing charge in net cost** toggle (net grouping) mirrors Usage Stats, and standing charges are also viewable in their own right. **Lifetime / Year-aligned** winding and the per-year on/off overlay remain, now shared across every spiral. Each tile has a **click-to-expand** button that opens it large in a modal (like the Overview cards).

- **Self-healing pricing check after every import — no manual repair needed.** When an import finishes, EMT now runs a **deferred pricing-verification pass** that re-checks the reconstructed history and corrects any half-hours the bulk import mis-priced — chiefly **IOG smart-charge slots** that came back labelled *standard* under load and should be *off-peak*. It works **newest-first in resumable chunks**, gated on the Octopus **rate-limit allowance** (it waits for the budget to recover rather than pushing through and getting more wrong answers back), and **survives a restart** via a saved cursor. It only re-checks the slots a re-fetch could actually improve — a few dozen across a two-year history, not every block — so a **clean import finishes in seconds** while a mislabelled one is repaired quietly in the background with nothing for you to do. While it's in a fetched window it also opportunistically corrects the small **charge-run edge slots** next to a big dispatch, at no extra API cost, and only ever **downward** (never off-peak → peak), so a busy-connection re-fetch can't do collateral damage. The **Pricing health** panel shows it all live: prices recovered, the off-peak/peak verification with a progress bar, **which day-ranges changed**, and how long it took — and it persists, so you can still see the last run's result after a restart. *(The old "Fix flagged slots" button is retired: the health panel's contextual "Retry" and this pass cover it.)*

- **Manage your backups from the list, and clearer feedback on long jobs.** Each zip backup in Data Management now has **⬇ Download** and **🗑 Delete** buttons next to Restore — download streams the file to your browser; delete removes just that one backup (with a confirm), both path-guarded to the backups folder so nothing else can be touched. **Delete Blocks and "delete all imported data" now run as background jobs** with the same banner + live progress as Restore, so you can leave the page while they run; and **creating a backup** now shows in-flight feedback too.

- **Reconstruct a device's history from Home Assistant's recorder — attribute past usage to a device that wasn't set up yet.** If you added a device (EV charger, battery, heat pump) to EMT *after* it was already recording in Home Assistant, its earlier consumption sat inside your **house remainder** with no way to split it out. The new **Recorder History** page (Data Management → Historical) lists each configured **device sub-meter** and lets you pick **one or more energy sensors** for it — pre-filled from the sensor EMT already uses, filtered to **energy / kWh** sensors, with multiple sensors allowed so a **rename or integration swap** in HA can be stitched into one continuous series (the first sensor to supply an hour wins). The **house isn't listed** — it's the remainder, the control total, not something to attribute. **Attribute** then reads that device's **long-term statistics** hour by hour, splits each hour across its half-hour blocks **weighted by the house import shape**, and writes the device into your history — re-running the same clip EMT uses for live sub-meters, so the device's **kWh and cost never exceed what the house actually drew from the grid** in that block, and the **house remainder is re-derived** to match. Carbon is attributed per device too. It runs as a **background job** that **takes a backup first**, shows live progress, and is **cooperative** — it pauses while a delete/purge runs and yields to live polling, so a long reconstruction over years of history won't starve the live tick.

- **Attribution joins up with your live history automatically (no overlap, no gap).** A reconstruction fills from the **earliest data the recorder holds** up to — but **not into** — the point where that device's **real history in EMT begins**, so the reconstructed period butts cleanly against the live one and stops there rather than running to today. A device that was never configured live fills as far forward as house blocks exist. Backfilling odd gaps *inside* a device's live period is deliberately left as a separate action, so the clean join stays clean.

- **Every attribution is reversible — one-click Undo.** Reconstructed rows are tagged as their own source and each run is recorded in a ledger, so the Recorder History page lists your runs with an **↩ Undo** that removes **only** that reconstructed device layer and re-derives the house remainder — it never touches live, imported, or settled house totals. The undo is guarded so a stray double-click can't misfire: a second undo while one is running is refused, and undoing a run that's already been undone does nothing (rather than falling through to a broad delete). The charts regenerate once a run finishes.

- **See how far back the API can reach, before you import.** The Date-range and Whole-history plans now show the earliest half-hour the supplier API can actually provide and how many fetch chunks it will take, so you know what a run will do before starting it.

- **CSV templates take a unit rate, not just a cost.** The pre-filled template gained an optional **Unit Rate (p/kWh)** column — fill a rate and EMT computes the cost (`rate × kWh`), the easy path for a flat tariff, or leave it blank and enter the cost per half-hour as before (what an Octopus export gives). Both the main CSV import and the last-resort "Reprice from CSV" now share the one column format, matched by header name so column order doesn't matter.

- **A CSV escape hatch for prices the API can't recover.** If a handful of half-hours still won't price after repeated retries, the Pricing-health panel now lists exactly those slots and offers a **pre-filled CSV of just those timestamps** to fill from your bill and run through the reprice path.

### Changed

- **The historical-import page is now one task-oriented flow.** Rather than a spread of separate tools, you pick **where the data comes from** — **API** or **CSV** — then **what to fill**: API offers *Whole history · Date range · Gap fill*; CSV offers *Fill gaps · Date range*. The preview, import panel and per-gap actions all follow from that one choice, so there's a clear path and no dead ends if you change your mind. CSV templates are offered **per channel** (Import / Export), pre-filled for the exact window, right where you upload — no separate "upload your export" step and no supplier-specific wording.

- **Whole-history, date-range and gap fills are now one mechanism.** A date-bound import behaves exactly like a whole-history import with a start date, and **filling a gap from the API** runs through the same background job — all with the same price recovery, off-peak/peak verify, carbon fill and Pause/Resume/Cancel. Imports are always **contiguous**: the end is always your oldest existing block, so you only choose how far back to go and a run can't leave an interior hole.

- **The API import panel is lighter.** The long explanation is collapsed behind a *"How it works & timing"* expander; the redundant second *Preview plan* button is gone (Preview plan in the picker takes you straight to the panel); and a **settled** Pricing-health summary shows just its "✓ Up to date" badge with the detail behind *Show details*, expanding itself only while a check is actually running.

- **Data Management reorganised** into three intent sections, with the old *Repair tools* group renamed **Cost Corrections**.

- **Billing Settlement Source and Review Unsettled Blocks moved to the Cost Corrections page, with setup-appropriate gating.** They belong with the other billing/cost tools, so they've left the Data Management landing page (its "Correct & review" heading is gone). The **Billing Settlement Source** (DCC-vs-CAD choice) now appears only for a **CAD+API** setup — the only case where it's meaningful (a pure-API setup has no CAD to fall back to; a pure-CAD setup has no DCC). **Review Unsettled Blocks** (and Retry settlement) appear whenever a **supplier API** is present and billing runs on DCC — including a pure-API setup, where blocks still await DCC settlement.

- **The two Usage Stats endpoints now share one billing-accurate aggregation (internal, output unchanged).** The daily/monthly/yearly bars and the new HH view were computing the same per-block rate-keyed cost subtraction, sub-meter split and carbon apportioning in two separate copies; they now call shared helpers, so the HH view can't drift from the daily bars. This was done as pure code-motion and proven **byte-identical** against a captured golden baseline plus the billing-equivalence test suite — no figures change. *(A follow-up to fold the Billing chart onto the same single aggregation is tracked for 4.1.0.)*

### Fixed

- **Dismissing a "flagged for review" block now sticks — you're no longer asked to re-review the same blocks over and over.** ([#322](https://github.com/RGx01/energy-meter-tracker-addon/issues/322)) The dispatch reconciliation re-scans your whole history each time new dispatch data arrives, and it was re-flagging any still-ambiguous block — including ones you'd already dismissed — because a dismissal only cleared the live flag with no memory that you'd made a decision. So every new smart-charge slot resurrected the entire back-catalogue for review (most often Ohme "planned then didn't charge" slots, which report a completed charge energy without a clean start signal). Dismissals are now recorded permanently and the reconcile never re-flags a dismissed block; pricing/repricing of those blocks is unchanged. *(A separate refinement to flag fewer of these Ohme replan slots in the first place is a follow-up.)*

- **The Restore-from-backup dialog now opens instantly instead of appearing to hang.** Choosing a zip backup to restore used to read the entire `blocks.db` out of the zip and base64-encode it (tens of MB) before the file-picker dialog could appear — a multi-second freeze that looked like the add-on had stalled, purely to list the filenames. The dialog now opens immediately with a brief "Reading backup…" state and fetches only the file list (names + sizes, no decompression); the restore itself is unchanged (it re-reads the zip server-side). File sizes are now shown in the picker too.

- **The Usage Stats chart no longer shows a blank gap while it loads, whether you switch browser tab or navigate away and back.** Two causes: (1) switching back to the Charts browser tab used to wipe the cache and rebuild the whole-history view from scratch every time, blanking the chart during the reload; and (2) navigating away in-app (to Overview, say) and back is a full page load, so the chart had nothing to show until the fresh fetch+render completed. Both are fixed. The page now checks a cheap change-token (`/api/charts/data-version` — an index-backed COUNT + newest block, ~milliseconds) before rebuilding, so a tab-focus (or the new lightweight 60-second poll) only re-renders when the data has *actually* changed; and it caches the last chart payload per instance so returning to the page **paints instantly** from it, then confirms/refreshes against the token in the background. The half-hourly (HH) view — which used to cache "today" and never refresh — now updates as new half-hours finalise, without a manual reload. *(The database itself was already well-tuned — WAL mode, a covering index, and a separate read-only connection for the web UI — so this was a client-refresh problem, not a query one.)*

- **The Overview smart-charging card no longer over-counts an upcoming charge (a revised IOG plan piled up).** ([#306](https://github.com/RGx01/energy-meter-tracker-addon/issues/306)) The card's *upcoming* total is built from `dispatch_history`, which keeps every planned slot it has ever seen (so a real completed charge is never lost). But when IOG **revised the plan** — dropping some future half-hours and adding others — the dropped slots were never removed, so the card kept counting them **on top of** the new plan: the planned kWh crept far above what was actually scheduled, and it looked like the card wasn't updating when the schedule changed. The upcoming view now counts **only the current plan** — a future planned slot qualifies only if it was re-seen in the most recent dispatch poll (superseded slots fall a full poll behind and drop out on the next poll, within ~5 minutes), and if no plan has been re-confirmed for a while the forecast is treated as stale and shows nothing. The card also now **auto-refreshes every 60 seconds**, so a revised plan or a newly-completed charge appears without reloading the page. **Delivered** charges are unaffected — they still accumulate from the real `completed` dispatches, so past charging history is unchanged.

- **A stale sensor reading no longer manufactures phantom device kWh (the mysterious 48.93 kWh battery half-hour).** When a device's cumulative-energy sensor dropped out and briefly resurfaced an *old* register value before recovering — e.g. `house_battery` on 2026-07-21 reading `6259.77`, a value it had genuinely held four days earlier, while its true register sat flat at ~6309 — the climb back up to the real value was booked as ~49 kWh of consumption in a single half-hour. It grid-clipped to zero, so the **bill was always correct**, but the raw figure skewed the Usage Stats device split and inflated that device's carbon (raw × intensity). Three fixes close it: **(1)** `read_sensor` now treats an `unavailable`/`unknown` sensor as a genuine gap instead of substituting a cached (possibly stale) number, so a dropout can't feed an old value into the reads; **(2)** gap-fill (`build_gap_blocks`) now distinguishes a *glitch* from a *reset* on a cumulative register — a backward step that hasn't collapsed to near-zero is a stale/dropout read, so it books **zero** and carries the register forward, rather than treating it as a reset and counting the full value (or the later climb-back); and **(3)** a one-time **self-heal** repairs pre-existing history — any sub-meter block whose register dipped below its established high-water and then recovered has its phantom kWh clamped to the grid-bounded value, carbon recomputed, and the block flagged for review, with billing untouched. This is the smaller sibling of the #307 lost-opener spike: that guard caught the giant (>60 kWh) blocks, but a recovery of this size slipped under the ceiling, so it needed a signature-based (dip-and-recover), not magnitude-based, catch.

- **IOG smart-charge slots are now priced off-peak reliably (the recurring "wrong October bill").** Two bugs conspired to leave overnight dispatch charges at the **peak** rate. First, a **units bug** in the pricing-verification check: it compared the stored rate (in **£**) against the tariff band bounds (in **pence**) without converting, so the test for "is this priced at peak?" was *always false* — the verification never actually re-classified anything, it only recovered slots that had no cost at all. Second, a **query-window** bug: once the re-check narrowed to a lone edge slot at the *start* of a charge run (its later slots were already off-peak, so they were skipped), the fetch window ended **at** that slot and cut off the rest of the run — and Octopus only returns the off-peak/dispatch label when the query spans the whole run, so the slot came back *standard* (peak). Both are fixed: the comparison is now unit-correct, and the re-fetch window is **padded a day on both sides** so a dispatch's full context is always in view. As belt-and-braces for **IOG** — whose cheap rate is dispatch-driven and isn't always in the tariff schedule — the "peak band" test now falls back to the **off-peak floor observed in your own imported data**, so a schedule that reports a single flat rate can't blind it. Verified against a real Octopus bill (off-peak/peak split and kWh to the decimal). Re-run the import, or the date-range repair, to re-price existing history.

- **Charts page: no more scroll jumps, working year filter, and no cross-instance bleed.** Returning to the Usage Stats browser tab — or switching back from another app — rebuilt the active chart and **snapped you back to the top**; it now preserves your scroll position across that refresh (a follow-on to the 3.4.0 lazy-render jump). The **year filter** now works in **billing month/year** order, not only calendar mode. And chart view state (which chart, sort order, scroll) is now keyed **per instance**, so viewing two EMT instances in the same browser no longer bleeds one's settings into the other. Separately, the **Pricing health** panel now **resets when a new import starts**, instead of briefly showing the previous run's raised/recovered counts.

- **Dependency: `aiohttp` 3.14.1 → 3.14.3** — a security-and-bugfix patch (no API changes) that includes a WebSocket continuation-frame parser fix, on the same code path EMT uses for the Home Assistant connection.

- **Delete Blocks no longer leaves two blocks per day behind (a BST timezone bug).** A whole-day date-range delete (00:00–23:59) had the server convert the *local* end time 23:59 to UTC — which under BST is **22:59** — and the store turned that into `TIME(block_start) <= '22:59'`, so every block stored at **23:00 or 23:30 UTC (i.e. local midnight, 00:00/00:30)** survived the delete. Deleting a 700-day range left ~1,400 blocks (2 per day), and re-running the exact same delete removed **zero** of them, so the range never fully cleared — which in turn kept poisoning go-live detection and re-import. A whole-day window is now recognised and applies **no** time-of-day filter (the local-date→UTC bounds already cover complete local days); only an *explicit* partial-time window is converted to UTC. Full-day deletes now remove the local-midnight blocks too.

- **Charts now regenerate off the event loop after each block finalises (the other reconnect-storm cause).** Every live block boundary triggered a **synchronous** chart render on the engine loop; with 2+ years of history that render takes long enough to stall the HA WebSocket heartbeat, dropping the connection and re-running startup — a restart after (almost) every finalise. The post-finalise render now uses the same **off-loop, read-only** path as the startup render (with an in-progress guard so overlapping finalises don't stack renders), keeping the tick responsive. The startup render was already offloaded; this closes the per-finalise path.

- **Startup CI fetch no longer corrupts the database connection (a cause of the reconnect storm).** The startup carbon-intensity fetch and the carbon-gap recovery wrote to the database **from an executor thread**, while the engine's other writers (poll, finalise, historical-carbon backfill) run on the event-loop thread — two threads on the engine's single SQLite connection. That race produced `cannot commit - no transaction is active` / `error return without exception set` and could destabilise the Home Assistant WebSocket, kicking off a reconnect → re-startup → re-race loop (observed as `engine_startup` running repeatedly). Now only the blocking **network** fetch is offloaded; all DB writes stay on the loop thread (the same rule the historical-carbon backfill already followed). This matters more in 4.0.0 because the region-aware carbon backfill writes to many more blocks. Also: transient supplier-API fetch failures (import/export poll, rate/standing-charge schedules) now log the exception **type** when the message is empty, instead of a blank `fetch failed:`.

- **Faster startup, and no more reconnect storm on a large history.** The startup chart render ran synchronously on the engine's event loop, and on a big history it could take tens of seconds — long enough to stall the Home Assistant WebSocket heartbeat, which dropped the connection, which re-ran startup, which rendered again: a self-perpetuating reconnect loop (visible as the "account verified — REVIEW BEFORE ENABLING POLLING" banner repeating in the log). Chart generation now runs **off the event loop** in a worker thread using a dedicated **read-only** database connection (so it can't stall the loop, and can't race the engine's writer — the cause of an earlier delete-time segfault), and it's **fired in the background** rather than blocking startup — so the add-on and its web UI come up promptly and the charts fill in a moment later. Also reorders startup so the supplier API is activated *before* the chart render, not behind it.

- **A restore interrupted by an add-on rebuild/restart could silently empty the database.** The background restore rewrote `blocks.db` in place (`open(..., "wb")` truncates the live file to zero before refilling it) on a thread that returns to the browser immediately — so rebuilding or restarting the add-on before the write finished could leave a **0-byte** DB, which SQLite opens as a brand-new *empty* database (looking exactly like a fresh install: no blocks, mode reset). Restore now writes each file to a temp path, fsyncs it, and atomically `os.replace()`s it into place, so an interrupted restore leaves **either** the old database **or** the complete new one — never a truncated one. The restore progress banner also now warns not to rebuild or restart the add-on until it finishes.

- **API mode with missing credentials no longer masquerades as "connected".** Backups contain only `blocks.db`; your API key lives in a separate `kraken_credentials.json` that isn't included. So restoring a backup onto a **fresh** install (or any `/data` that lost the credentials file) left the data-source mode reading `api`/`cad+api` with no key actually stored — and Settings showed *"Data source: supplier settlement (DCC)"* with only a **Disconnect** button, hiding the fact that nothing could poll or settle. Settings now checks whether credentials are really present: when they're not, it shows a clear **"API mode is set, but no API credentials are stored"** warning with a **Re-enter API key** button, and the billing badge reads **"⚠ API mode · credentials missing"** instead of *"No API · local billing"*.

- **Export figures now settle on API-only (Home Mini) solar setups.** On a meter billed from the supplier API with no local export sensor — e.g. an Octopus Home Mini, which is an *import-only* live feed — daytime export has no live figure and arrives only via DCC settlement. The settlement sweep only chased export when a **positive live value was already present** (the guard added in 3.4.0 with #303), so those slots (stored as 0/NULL until settled) were skipped and the daytime export never landed — only the most recent evening half-hours filled, the rest sat empty even though the supplier's data was available. Import was unaffected (no such guard), which is why import looked perfect while export stalled. A block now counts as awaiting export settlement whenever its settled export is missing **and the meter demonstrably exports** (value-agnostic on the slot), so the daily sweep — and the **Retry settlement** button — re-fetch and correct it. Existing stuck blocks recover on the next sweep (or one press of Retry settlement) within the 14-day horizon; import-only meters are unaffected.

- **Retry settlement now rebuilds the charts.** When Retry settlement lands new figures (such as late export finally settling), the pre-built charts were left showing the pre-settlement values until the next scheduled render; they now regenerate immediately when a retry actually settled something.

- **Mobile period selector** falls back to the native `<select>` on touch devices, fixing the Usage Stats spinner that could lock up or fail to open.

- **Silenced a `SyntaxWarning`** from an unescaped `\s` in an `energy_charts.py` f-string regex.

### Removed

- **The unbounded "Fix a date range" reprice tool.** It let an arbitrary (unbounded) date range be thrown straight at the shared supplier API allowance, and duplicated what the automatic verify pass and the Pricing-health **Retry** already do. **Reprice from CSV** remains as the last-resort manual price fix.

- **Legacy JSON→SQLite migration shims.** The one-time startup migrations that imported a pre-SQLite `blocks.json` (and the 2.1-era `current_block.json`) into the block store have been retired — anyone on 4.0.x has long since moved to the SQLite store. If an old JSON store is ever found (on startup, or restored from an ancient backup), EMT now logs a loud error and starts as a **new install**, leaving the JSON file **untouched** rather than silently importing it. To recover such data, run an earlier release (3.x or older) once to migrate, then upgrade again. The `migrate_json_to_sqlite` helper and its tests were removed.

## [3.4.0] - 2026-07-19
 
### Added
 
- **Smart-charging card on the Overview page** (BL-10). On Intelligent Octopus, the Overview page now summarises your smart charges: the latest one at a glance — energy delivered, the charge window, total charge time, whether it ran off-peak, and a subtle estimate of what smart charging saved versus peak — with an expandable view whose range you can switch between **7, 30 and 90 days** (90 being how far back dispatch history is kept). Each row is **one charging period** (bursts within a single overnight plug-in are merged, so a night that Octopus split into several dispatches reads as one session), and shows its per-slot fill/taper curve, exact scheduled window, off-peak-vs-peak split, and effective charge time. Only energy that **actually flowed** is shown — planned-but-never-charged forecasts no longer appear — with each half-hour's energy taken from the supplier's dispatch figure (the car's charge) and its **price/colour from the billing blocks** (the rate each half-hour was actually billed at), so a slot billed at peak reads peak, not off-peak. On a single-meter setup the dispatch figure is the only EV-specific signal, since the meter's own import also carries house baseload. Charges Octopus has **scheduled but not yet delivered** appear in a separate *Upcoming* section. It's built entirely from the dispatch data EMT already captures, so it appears for **every** IOG setup — including API-only ones with no EV or power sensor, and **Ohme** accounts — and simply isn't shown when there are no smart-charge dispatches. The exact scheduled window is drawn from newly-retained second-precision dispatch bounds (BL-11).
 
- **Standing charge now appears within the import charge on the bill summary**, matching how a real supplier bill reads. It moves from the bottom of the bill up into the "Import — total grid" section, followed by a new **Total incl. standing charge** subtotal (energy + standing). The kWh Total and the overall `Total Bill` are unchanged — this is display-only, with no effect on any stored figure or the Usage Stats page. (VAT is still not modelled.)
 
### Fixed
 
- **Export figures now settle against DCC data** ([#303](https://github.com/RGx01/energy-meter-tracker-addon/issues/303)). Octopus settles export up to a few days after import, but EMT's settlement sweep decided which blocks to re-fetch based on *import* alone — so once a block's import settled it dropped out of the re-fetch window, and when its export settled later it was never picked up. The export kWh stayed on its pre-settlement estimate (slightly off from the supplier's figure) while import matched exactly. A block now counts as unsettled while **either** channel is still awaiting DCC (export only on blocks that actually exported), so the daily sweep keeps re-fetching until export settles too. Existing affected blocks self-heal on the next daily sweep, within the 14-day settlement horizon. Import-only accounts are unaffected. This is a re-fetch/window change only — how a settled figure is applied is unchanged, so it cannot alter a correctly-settled block.
 
- **Billing charts load and switch much faster.** Two things had grown slow as history accumulated. First, the multi-megabyte daily chart was served **uncompressed** on the direct `:8099` port (and in Lovelace iframe/webpage cards), so it downloaded slowly and the page sat blank for 15–20 seconds; the add-on now gzip-compresses its own text responses (~12× smaller — a 6.6 MB chart ships as ~0.56 MB), matching what the Home Assistant sidebar's ingress proxy already provided. Second, switching to the **Quarter** or **Year** view built every per-day chart at once (~90 for a quarter, ~365 for a year), locking the page until it finished and only showing the mode button as pressed *afterwards*; those views now open with the daily-charts panel collapsed (the bill summary shows immediately) and per-day charts render only as they scroll into view — so switching modes, and even expanding a full year, stay responsive. Month view is unchanged.
 
### Removed
 
- **The `sensor.energy_meter_tracker_api_deprecations` entity and the `publish_ha_sensors` option have been removed** (BL-17). That sensor was the last Home Assistant entity EMT published, and its signal only ever mattered to the add-on's maintainer — which is now delivered better by an automated weekly check that opens a tracking issue whenever Octopus flags a GraphQL field EMT uses as deprecated. The upstream "Octopus API change ahead" persistent notification went with it. The `publish_ha_sensors` option (and its `PUBLISH_HA_SENSORS` environment variable) existed only to gate that sensor, so it's gone too — **EMT now publishes no Home Assistant entities at all**. *Breaking only if* you referenced `sensor.energy_meter_tracker_api_deprecations` in a dashboard, template or automation, or set `publish_ha_sensors` in your add-on config — remove those references. EMT still logs `kraken_field_deprecated` warnings locally, so nothing is lost from the add-on's own logs.

## [3.3.1] — 2026-07-14
 
### Fixed
 
- **The "Flagged for review" list could raise false positives** (hotfix for the 3.3.0 review list). Two kinds of half-hour block were flagged that had nothing to review: (1) blocks **inside the off-peak window**, where the price is already the off-peak rate by the tariff schedule regardless of any smart charge — so nothing a dispatch does could change it (this also covers a charge landing in the window under the 6-hour cap); and (2) blocks **not yet settled by the DCC**, which the correction tool can't act on anyway (dispatch data arrives within hours, but settlement lands a day or two later). The review list now only surfaces settled, out-of-window blocks — the ones where the off-peak price genuinely depended on a dispatch having happened. Any block flagged in error is cleared automatically on the next reconciliation, so no manual dismissal is needed.

## [3.3.0] — 2026-07-14
 
### Added
 
- **Pick a nearby rate when correcting** ([#270](https://github.com/RGx01/energy-meter-tracker-addon/issues/270)). The rate correction form now offers a dropdown of the exact rates already in force in blocks *around* the correction window — the everyday off-peak and peak values — ranked by how common they are. Pick one and the field fills at full precision, so there's no mistyping, wrong decimal place, or accidental rounding. You can still type a custom value to override. The list refreshes as you change the date range, and pre-fills when you load a flagged block from the review list.
 
- **"Flagged for review" list on the Corrections page** (BL-18). When a smart charge can't be priced with confidence — the supplier planned a slot but it's genuinely ambiguous whether the car actually charged — the block is now left at its current price *and surfaced for you to decide*, instead of being silently ignored. The Corrections page shows a list of these blocks with the reason and the exact half-hour window; each has a **Load into tool** button that pre-fills the correction form for that block, and a **Dismiss** if the current price is right. A dismissible notice appears across the app when new blocks are flagged (and stays quiet once acknowledged, until *more* are flagged). Flags clear themselves automatically when the block later becomes decidable or when you apply a manual correction to it.
 
### Fixed
 
- **A device could be credited more grid import than the whole house drew, on data rebuilt after an outage** (BL-19). When EMT misses live readings — for example the add-on was stopped or offline during a smart charge — it reconstructs the missing half-hours by spreading the accumulated energy across them. Those rebuilt blocks skipped the check that caps each device's grid share at what the house actually imported, so once the meter settled to its true figure (often small on a sunny, export-heavy day) a device such as an EV charger could still show far more grid energy than the house imported — over-attributing grid, and grid cost, to the device while understating its solar/battery self-supply. The whole-house bill was always correct; only the per-device split was wrong. The cap now always applies once the house's import figure is authoritative (live-metered or settled), and a one-time check on upgrade repairs any already-affected blocks (they re-price automatically over the following polls). An unsettled gap block is still left as-is until settlement lands, so a genuine overnight grid charge isn't prematurely reclassified.
 
## [3.2.1] — 2026-07-13
 
### Fixed
 
- **Intelligent Octopus smart charges on the official Ohme integration were billed at peak** ([#286](https://github.com/RGx01/energy-meter-tracker-addon/issues/286)). The verified Ohme path read the charge-mode `select` and compared its state against `"smart charge"` — but the official Home Assistant `ohme` integration reports the underscore **slug** as the state (`smart_charge`), and only *displays* "Smart charge". So every tick resolved to `idle`, no slot was ever captured, and the entire smart charge was priced at the day rate. The mode match is now slug/display-agnostic (`smart_charge` / `max_charge` / `paused`, space and underscore treated alike), so the off-peak overlay applies again. **This affected every user on the official Ohme integration since the Ohme path shipped** — dan-r-integration and non-Ohme users were unaffected.
- **A planned smart charge that barely ran is no longer over-credited at off-peak.** When Octopus planned a smart-charge slot but the car didn't materially charge (for example it was near-full or solar covered the house), the slot's small grid import — really household baseload — could stay priced at the off-peak rate. The settlement reconciliation previously treated *any* "completed-without-`started`" slot as ambiguous and left it unchanged. It now uses the supplier's **completed energy** (not the meter, so it stays solar-safe): a completion below **0.4 kWh** can be neither a boost (which draws hard) nor a real smart charge, so the slot reverts to peak. Completions above that remain genuinely ambiguous (a missed-poll smart charge or a boost) and are left untouched (a UI to surface these flagged blocks for review is planned for 3.3.0).
- **Ohme off-peak capture now follows the charger's real charging state.** In addition to the fix above, the verified path now gates on the Ohme **Status** sensor (`charging`) rather than only the charge-mode setting. This captures a slot precisely when the charger is actually drawing — so mid-session pauses and Octopus/Ohme replanning that adds *further* slots ("additional slots") are handled by construction, without depending on Octopus's planned-dispatch superset (which is unreliable for Ohme, since Octopus doesn't control the charge). Setups without the Status sensor fall back to the previous mode-only behaviour. The `_capture_ohme_slots` log line now includes the raw mode and status values, so any future mismatch is visible at a glance.

## [3.2.0] — 2026-07-11

### Added

- **"Update available" notice** (BL-6). Supervised installs get update badges from the Home Assistant Supervisor; Docker/standalone installs got nothing and only learned of a release by checking the repository. EMT now checks once a day for a newer release and shows a dismissible notice linking to the release notes. **Dismissal sticks per version** — dismiss it for a given release and you won't be asked again until something newer ships.

- **Backups are now isolated per site** (BL-5). In supervised installs backups are written to `/share`, which Home Assistant shares across **all** add-ons — so two EMT instances would list, and could restore, each other's backups. The backup directory is now namespaced by a slugified site name (taken from your existing configuration; nothing new to set). Renaming your site moves the directory across silently; renaming back re-adopts the original one so old backups reappear; and a directory belonging to another instance is never merged or touched. Ownership is decided by a persistent per-install id, resolved by **scanning `/share` for the directory this install already owns** — so an instance keeps its own backup directory even after a restore whose data reports a *different* site name, and repeated restores never spawn duplicate directories or let one instance adopt another's backups. Docker/standalone installs are unaffected — their backup directory lives inside the container's own volume and is already isolated. These protections key on site identity rather than how EMT was installed, so they hold however a second instance came to exist.

- **"Restored from another instance" notice** (BL-5). Each database now carries a lineage id that travels with it through backup and restore. When a database is restored from a *different* install (for example a production backup restored into a staging instance), EMT shows a dismissible notice so the swap is visible rather than silent. Dismissal is **per source lineage** — acknowledge a given source once and routine repeated restores of it stay quiet, while a genuinely new source still surfaces. Native databases carry their own install's id and never raise it.

- **Recover missing data after an outage** (BL-8). A new **Missing Data** panel in Data Management finds half-hour blocks that were never recorded — typically because the add-on was offline longer than the 12-hour gap-fill limit — and rebuilds them from your supplier's settled readings. The routine settlement poll only ever looks forward from its last run, so a gap it has already passed was previously permanent. Recovery is a **manual action on purpose**: EMT cannot tell an outage gap from blocks you deleted deliberately, so an automatic sweep would restore those too. Recovered blocks use the authoritative half-hourly figures and are priced at the rates in force at the time, but carry **no per-device breakdown** — EMT was not running, so no device readings exist for that period.

- **Outage recovery: gaps are now backfilled from settled supplier data as settlement arrives** (BL-8). An outage longer than the 12-hour gap-fill limit used to leave a permanent hole — reconciliation only reprices blocks that exist, and the DCC poll only *settled* existing blocks, dropping the settled figure for any period with no block. EMT now **creates** those blocks from the authoritative half-hourly figures as settlement arrives, and the normal PASS-2 pass prices them (fetching historical rates for the period, so any tariff — Agile included — prices correctly). Recovered blocks are marked as externally sourced, and carry **no sub-meter split** — EMT was down, so no device readings exist for that period. Their off-peak status is decided once, at creation, from whatever dispatch data the supplier still returns (~24–48h), so recovery is best-endeavours: a smart charge during a long-past outage may be billed at the standard rate.

### Removed

- **The four synthetic block sensors have been removed**: `sensor.energy_meter_import_kwh`, `sensor.energy_meter_export_kwh`, `sensor.energy_meter_import_cost`, `sensor.energy_meter_export_credit`. They published live per-block figures that DCC settlement retrospectively corrects, so their values disagreed with the billing EMT itself reports — misleading rather than merely unused. **Breaking change** if you referenced them in a dashboard, template or automation; use the Charts and Insights pages, which reflect settled figures. `sensor.energy_meter_tracker_api_deprecations` is unchanged.

### Fixed

- **Saved-configuration messages no longer overlap the page** ([#219](https://github.com/RGx01/energy-meter-tracker-addon/issues/219)). The "Configuration saved" and error messages on Meter Config were pinned to the top of the viewport, painting over whatever sat beneath them — usually the Meter Config / Carbon tabs, leaving both unreadable. Notifications now appear in a shared region between the topbar and the page content: they push the page down instead of covering it, stack rather than overlap, and stay visible while you scroll. Transient messages clear themselves; ones you need to act on stay until dismissed. The unsupported-tariff warning moved here too, since it affects the whole app rather than the Charts page.

- **A long outage no longer freezes DCC settlement across your whole history** (BL-1). When EMT detects a session gap (an outage longer than the 12-hour gap-fill limit) it sets a gap marker on the current block. That marker also — wrongly — gated the DCC PASS-2 drain, the step that applies settled half-hourly figures to billing. So while a gap marker was live, settled data for **every** block, not just the outage, was ingested but never applied: costs stayed on the pre-settlement estimates until the marker cleared. The marker still correctly guards sub-meter boundary amendment (gap-seed reads would corrupt its interpolation), but the drain now runs regardless — it takes its queue from the database and never reads the current block's rolling buffer, so gap-seed reads cannot contaminate it.

- **A supplier-API outage no longer shows a false "Tariff not supported" banner.** The rate-schedule refresh flagged a meter "unsupported — billing will be incorrect" whenever the unit-rate fetch came back empty — but a *failed* fetch (transport error, timeout, or an edge/WAF **HTTP 403** while the supplier's GraphQL is throttled) returned empty too, so a transient block was reported as a permanent tariff problem. The refresh now distinguishes a failed fetch from a successful-but-empty one: on a fetch failure it keeps the last-known schedule and leaves the banner untouched, and only raises the "unsupported" warning when a fetch genuinely succeeds and returns no standard **and** no day/night rates.

- **Footer now identifies the instance, not the port.** The sidebar footer showed `port 8099` — the internal container port, which is identical for every instance under ingress and so distinguished nothing. It now shows a per-instance label, resolved highest-first from: the new **`instance_name` option** (set it per instance in the add-on's Configuration tab — the only distinguisher when two installs share one add-on `name`, e.g. the repo-URL workaround); the **add-on name** via Supervisor (e.g. "Energy Meter Tracker (DEV)"); or the container hostname on standalone. It falls back to the port only if none resolves. Every source is an *install* identity — the manifest or `options.json`, never the database — so the label stays correct after restoring one instance's backup into another (the site name, which lives in the database, would travel with the restore).

- **An edge 403 no longer masquerades as "check API key".** The REST client mapped *every* 403 to "authentication failed — check API key", so an edge/WAF block (which returns an HTML error page, distinct from a genuine JSON auth error) told users to rotate a perfectly good key. A 403 with an HTML body is now reported as an edge block ("temporarily blocked by the supplier — not an API key problem"), while a real 401 — or a 403 with a JSON body — still says "check API key". The Settings "Test connection" button reflects the same distinction.

- **GraphQL edge-403 circuit breaker — stop hammering a blocked endpoint.** When the supplier's GraphQL edge returns a 403 (an intermediary blocking the endpoint, distinct from an app-level error), EMT used to keep calling it every poll — the Octopus Home Mini polls ~every 10s — which only prolongs the block and floods the log (hundreds of identical multi-line HTML dumps). A 403 now opens a circuit breaker that short-circuits GraphQL for a growing cooldown (1 min, doubling, capped at 15 min) and resets on the next success. Mini-telemetry and dispatch failures are logged once per episode instead of on every retry, with a clear "recovered" line when the block clears.

### Internal

- **`energy_charts.py` now parses on Python 3.10 as well as 3.12.** Three option-list f-strings used same-quote nesting (a 3.12-only syntax), which made the whole module — and the ~380 tests that import it — fail to even load on older interpreters. Rewritten to equivalent dual-compatible quoting; no behaviour change. Keeps the test suite runnable across interpreter versions.
- **Test suite runs file-isolated.** Several test modules install `sys.modules` stubs, so they must run one module per process (as the harness does); `docs/instance_isolation_design.md` documents the BL-5 test coverage.

## [3.1.5] — 2026-07-08

### Fixed

- **New Intelligent Octopus 6-hour-cap tariff — general-usage rates now supported** ([#1708](https://github.com/BottlecapDave/HomeAssistant-OctopusEnergy/issues/1708)). Following the fail-loud guard below, EMT now reads the new tariff's split rate buckets: when `standard-unit-rates` is absent it fetches `day-unit-rates` + `night-unit-rates` and merges them into the day/night time-of-use schedule, so a migrated meter is **billed correctly for general usage** instead of flagged unsupported. The EV-device rates and the daily 6-hour off-peak cap are **not** applied yet — Octopus hasn't finalised those rules — so dispatched EV charging keeps using the general off-peak (night) rate in the interim (a bounded approximation, documented in `docs/iog_6hr_cap_design.md`). Full EV-cap support follows once OE confirms the mechanics.
- **Fail loud on the new Intelligent Octopus 6-hour-cap tariff instead of silently mispricing** ([#1708](https://github.com/BottlecapDave/HomeAssistant-OctopusEnergy/issues/1708)). Octopus is migrating IOG customers to a new time-of-use tariff (`IOG-SMB-TOU-…`) that **drops the `standard-unit-rates` link** in favour of separate `day` / `night` / `ev_device_peak` / `ev_device_off_peak` rates. EMT reads `standard-unit-rates`, so a migrated meter would get an empty rate schedule and be **silently priced at £0**. EMT now detects this — an active import tariff that returns no standard unit rates — logs a prominent error and surfaces a red **"⚠ Tariff not supported — rates unavailable"** indicator, so the gap is visible rather than producing a wrong-but-plausible bill. Full support for the new tariff (the four rate buckets and the daily 6-hour off-peak cap) is separate, upcoming work; this release makes the gap safe, not silent.

## [3.1.4] — 2026-07-08

### Fixed

- **Billing chart showed the same total for every month** ([#271](https://github.com/RGx01/energy-meter-tracker-addon/issues/271)). In Month (and Quarter/Year) view, every period's headline "Total Bill" displayed the identical figure — the whole-dataset net — even though each period's breakdown was correct and different. The per-period total was computed from the first/last of *all* blocks rather than the blocks in the selected period, so `compute_period_net` always ran over the entire history. It now bounds the total to the selected period. The per-meter/per-rate breakdown was already correct; only the headline total and the month-dropdown labels were affected. Present since the shared net computation was introduced (3.1.3).
- **Intelligent Octopus smart charges are now priced from the dispatch lifecycle, not the meter** ([#253](https://github.com/RGx01/energy-meter-tracker-addon/issues/253)). The old over-report guard decided off-peak from grid draw against a fixed floor, which is wrong in two directions: a planned slot that never charged but drew household baseload was credited off-peak (over-credit), and a genuine smart charge supplied by solar/battery drew ≈0 from the grid and was billed peak (under-credit). A settlement-time reconciliation pass now reprices each smart-charge slot from whether it actually **`started`** under `SMART_CONTROL_IN_PROGRESS` — the signal Octopus itself uses, immune to where the energy came from. Slots that started are off-peak (restoring solar-supplied charges); slots that were planned but never started or completed revert to peak; the ambiguous case (completed without started — a missed poll or a boost) is flagged for review and left unchanged. Only slots recorded under the new lifecycle accumulation are touched, user-corrected blocks are never overwritten, and devices follow the main rate. Validated against a live solar-supplemented charge where the grid meter read 0.002 kWh but the slot was correctly kept off-peak. OHME chargers are excluded — Octopus doesn't control an OHME charge, so its dispatch records can't tell a smart charge from a boost; they keep the existing behaviour.
- **Meter-exchange selection now uses the authoritative retirement signal** (hardening of [#244](https://github.com/RGx01/energy-meter-tracker-addon/issues/244)). When an MPAN lists several meters after an exchange, the active-meter picker relied on list order ("Kraken lists oldest-first, take the last") as its main heuristic. It now reads the meter's `active_to` field — a swapped-out meter has it set, a live one has it null (the same signal BottleCapDave's integration keys off) — to drop retired meters authoritatively, and among any still-live meters prefers the one still reporting / most recently activated (newest `latest_consumption` / `active_from`). The list-order fallback is kept only for payloads that carry none of those fields. Turns "the current meter warning" from a positional guess into a read of the actual retirement/reporting data.
- **Billing-source indicator wrongly showed "N blocks awaiting DCC settlement" with no API configured.** The header pill gated on the billing source being `dcc`, but that's the default even when no supplier API is set up — so a user with no API saw thousands of blocks "awaiting settlement" that would never settle (their blocks are billed on the local/CAD figure, permanently). The pill now also requires an API to be available before showing the DCC message, and otherwise reports the actual state: **"No API · local billing"** (no API), **"CAD source · API ready"** (API available but billing uses the local CAD figure), or **"⏳ N awaiting DCC settlement"** (DCC path with blocks pending). Info states are neutral-styled; only genuine pending settlement is amber.

## [3.1.3] — 2026-07-06

### Fixed

- **Critical — segfault when deleting a device** (regression introduced in 3.1.2). The chart regeneration added in 3.1.2 for [#261](https://github.com/RGx01/energy-meter-tracker-addon/issues/261) ran on the Flask request thread using the *engine's* SQLite connection, while `engine_startup` was simultaneously restarting on the asyncio event-loop thread. The connection is opened `check_same_thread=False`, so SQLite permitted the concurrent cross-thread use rather than raising — and it crashed the add-on with a segmentation fault. Chart regeneration after any mutating action (delete device / blocks, backup restore, zip import, rate corrections) is now scheduled onto the engine's event loop, serialised with all other engine store access, so it can never race. The delete/restore/correction itself always completed; only the follow-up regen crashed.

## [3.1.2] — 2026-07-06

### Fixed

- **Cumulative sub-meter sensors no longer book their whole lifetime on the first block** ([#260](https://github.com/RGx01/energy-meter-tracker-addon/issues/260)). Adding a cumulative battery/EV import sensor (or a read dropout that lost the opener) could book the sensor's entire lifetime register as one block's usage — one reporter saw ~10 MWh land in a single day. The rogue-block clamp that already protected the main meter was scoped to `is_sub_meter=False`, so device channels had no guard at all. The sub-meter path now applies a physical-plausibility ceiling (60 kWh — impossible for a single domestic device in a block, but well above any real charge), so a lost-opener dump is clamped to 0 and the register baselined, while genuine charges — including session-energy sensors that start each charge at 0 and count up — are booked normally. Recovery for an already-affected day: Data Management → Delete blocks.
- **Delete Device / Delete Blocks now regenerate the billing charts immediately** ([#261](https://github.com/RGx01/energy-meter-tracker-addon/issues/261)). Both delete paths removed the data but left the pre-built billing/heatmap charts stale until the next half-hourly block finalised — correct eventually, but confusing when it isn't instant. They now call `generate_charts` after the delete, matching the corrections (#254a) and restore (#257) paths. (This also makes Delete Blocks usable as the immediate recovery for #260.)

## [3.1.1] — 2026-07-04

*A bug-fix release. The headline is diagnostic groundwork for a critical Intelligent Octopus pricing bug ([#253](https://github.com/RGx01/energy-meter-tracker-addon/issues/253)); the rest are fixes to the corrections tool, Usage Stats, power-sensor config, and the Spiral chart on mobile.*

### Critical — Intelligent Octopus off-peak mispricing, groundwork laid ([#253](https://github.com/RGx01/energy-meter-tracker-addon/issues/253))

On Intelligent Octopus, a **peak** slot can be priced **off-peak** when a smart-charge dispatch was *planned* but the vehicle didn't actually charge — for example it paused mid-session. EMT applied the off-peak overlay on the planned slot, but the supplier billed peak, so the block is under-billed. The over-report floor doesn't catch it, because ordinary household baseload alone can clear the floor during the pause.

**3.1.1 does not fix the pricing yet — it lays the groundwork to.** Each captured smart-charge slot now records its dispatch **lifecycle state** and the dispatch's **planned / completed energy** (kWh), retained in the database. This is **observe-only, with no billing effect**; it exists because, without the dispatch energy on record, there was no way after the fact to tell a genuine off-peak charge from a planned slot that never charged. The pricing fix — validating a slot against the energy actually dispatched — is targeted for **3.1.2**.

### Fixed

- **Corrections tool now follows the devices-follow-main model** ([#254](https://github.com/RGx01/energy-meter-tracker-addon/issues/254)). Three bugs shared one root — the DCC-settled gate keys on `imp_kwh_api`, which only the main meter carries: (a) charts weren't regenerated after a correction until the next block; (b) corrections only touched the main meter, never the device rate lines; and (c) a spurious "N blocks awaiting settlement" warning counted device rows that never settle independently. All fixed: a rate correction now applies to the main meter and the device rate lines follow it (as they do in the 3.0.6 engine), the preview shows the devices following, and charts regenerate immediately. The now-redundant meter/device selector has been removed from the tool.
- **Charts not regenerated after a database restore/import** ([#257](https://github.com/RGx01/energy-meter-tracker-addon/issues/257)). Same shape as the corrections case above: restoring or importing a backup reopened the database but left the pre-built billing/heatmap charts showing the old data until the next block finalised. Both restore paths now regenerate the charts against the restored database immediately.
- **Usage Stats — "Inc. standing charge" toggle had no effect on Totals or Net** ([#255](https://github.com/RGx01/energy-meter-tracker-addon/issues/255)). The table's Total column and grand total used the server's `net_cost`, which bakes in the standing charge, so unchecking the box changed nothing. Unchecking now correctly removes the standing charge from both.
- **Power sensor "invert" setting not persisting** ([#251](https://github.com/RGx01/energy-meter-tracker-addon/issues/251)). The main power sensor's invert flag was applied at runtime but had no database column, so it was dropped on save and the checkbox reverted. It now persists like the battery inverter's flag. The device power sensor's invert flag (`device_power_invert`, used by the Live Power gauge) had the same missing-column problem and is fixed alongside it.
- **Spiral options unreachable on mobile** ([#248](https://github.com/RGx01/energy-meter-tracker-addon/issues/248)). The options panel stacked below the chart on mobile but was clipped by the chart container; it now scrolls into view.
- **Browser tab icon intermittent** ([#249](https://github.com/RGx01/energy-meter-tracker-addon/issues/249)). The favicon is now an inline data URI — no separate request or path to resolve — so it renders consistently across ingress/standalone and every environment, working around Safari's URL-keyed favicon cache.
- **Review-sample log mislabelled the provisional figure.** On the `api` + Mini path the drift-review log printed `CAD=…` even though there's no CAD; it now reflects the actual source (`Mini=…` when the Mini supplies the provisional figure).

### Also in this release

- **Spiral chart PDF export** ([#250](https://github.com/RGx01/energy-meter-tracker-addon/issues/250)) — a small feature addition: the Spiral view now exports to PDF, in portrait, instead of showing a "coming soon" banner.
- **Docs** ([#252](https://github.com/RGx01/energy-meter-tracker-addon/issues/252)) — documented how to update a standalone Docker build (`git pull` + `docker-compose up -d --build`).

## [3.1.0] — 2026-07-03

*Adds a Spiral chart for seeing a year — or a lifetime — of energy at a glance, corrects carbon-intensity averaging on near-balanced solar days, and simplifies device setup now that devices always follow the main meter's rate.*

### Added

- **Spiral chart.** A new chart (Charts → Spiral) that winds your running total — cost, usage, or carbon — outward as a single continuous coil, one loop per year, so a fatter gap between loops is a heavier year and the centre always shows the total to date. Switch between Cost / Usage / Carbon, pick any device or the whole meter, and toggle **Lifetime** (one endless coil) or **Year-aligned** (every year starts at January, at the top) winding. Axis rings land on round numbers, and the unit scales automatically as totals grow — kWh → MWh → GWh, and kg → t for carbon — so a 4,598 kWh year reads as "4.6 MWh" rather than "4.6k". Works in both light and dark themes.

- **Unsettled-blocks indicator in the Charts header.** When billing runs on the DCC settlement path, the Charts header shows a count of blocks still awaiting the supplier's settled figure (e.g. "174 blocks awaiting DCC settlement"), so a stalled settlement is visible at a glance rather than only in Data Management. Hidden on the local/CAD path, where it doesn't apply.

### Changed

- **Devices always follow the main meter's rate — device rate fields removed from setup.** A device's grid import is part of the main meter's metered supply, so it's billed at the main meter's rate; this became the engine's behaviour in 3.0.6. The now-redundant per-device "Rate Sensor" field and "Use the main meter's rate" toggle have been removed from the Add Device dialog, the device editor, and the first-run setup wizard. Existing device rate settings are left untouched in your configuration and simply ignored — nothing to migrate, and no change to any bill. (If per-device tariffs return in a future release — for example Octopus's mooted 6-hour EV-charging cap — the field comes back.)

### Fixed

- **DCC import settlement no longer stalls after a meter exchange ([#244](https://github.com/RGx01/energy-meter-tracker-addon/issues/244)).** On a supply whose meter has been swapped, the meter point lists both the old and the new meter. EMT was always taking the *first*-listed meter — the retired one — so DCC consumption queries for its serial returned nothing and import blocks never settled (while a single-meter export point settled normally, which is the tell). EMT now selects the *current* meter: it honours an active/removal flag when the account provides one, and otherwise takes the newest meter in the list. If your import settlement was stuck at a fixed number of unsettled blocks, it should clear on the next settlement pass. Single-meter supplies are unaffected.

- **Carbon average intensity no longer blows up on near-balanced days.** In the usage-stats table, the average carbon intensity was being re-derived from net figures (total carbon ÷ net kWh), which collapses toward a divide-by-near-zero on days where import and export almost cancel out — sending the figure to absurd values (~3,800 gCO₂/kWh against a grid that peaks around 500). It now uses the server's per-day intensity (computed from half-hourly *absolute* throughput, which doesn't collapse), averaged across the selected range weighted by each day's import + export. Purely a display fix; no stored data changes.

## [3.0.6] — 2026-07-02

*Safeguards the running import total against a rare lost-register glitch, and makes devices always bill at the main meter's effective rate — fixing an Intelligent Octopus dispatch overcharge that could hit device costs on reconciled smart-charge blocks.*

### Fixed

- **Guard against a rogue full-register import block.** If the main meter's *opening* register reading is momentarily lost — for example a read dropout during a restart, which can happen while adding or removing devices — a single half-hour block could book the entire cumulative meter register as one interval's import, massively inflating the running total (and bill) until settlement caught up. The engine now clamps any single block whose import exceeds a physically impossible ceiling, logs it, and keeps the register continuous so the next block opens correctly. On API / Octopus Mini / DCC setups the true half-hourly figure still replaces the placeholder when DCC settlement arrives (no action needed); on CAD-only setups with no reconciliation source the affected half-hour is treated as zero — a negligible loss versus a phantom total.

- **Devices are always priced at the main meter's effective rate — fixing an Intelligent Octopus dispatch overcharge at reconciliation.** A device's grid import (a home battery or EV) is a portion of the main meter's supply, so it's billed at the main meter's rate — base tariff plus any Intelligent Octopus Go off-peak dispatch. Two faults broke that during smart-charge slots: at recording time a device whose own draw fell below the 0.1 kWh over-report floor (typical when solar or another device took most of the grid import) was stranded on the standard peak rate; and at DCC reconciliation the main's off-peak rate was applied to its own cost but not carried onto its devices, so a device's grid import was re-billed at **peak** on every settled smart-charge block — a material overcharge (a 3 kWh grid charge billed at ~£0.97 instead of ~£0.16). Device pricing is now resolved in one place, after every meter is finalised, and always follows the main meter's effective rate for both cost and the displayed rate — at recording time and at settlement, and whether or not the device drew anything in the block (a device that drew nothing at an off-peak→peak boundary previously kept a reconstructed off-peak rate while the main was peak; it now follows the main). The per-device breakdown continues to sum exactly to the metered bill. Single-tariff installs (the common case) see identical bills; the only visible change is a device's shown rate now always matching what it was costed at.

## [3.0.5] — 2026-07-01

*Adds an optional solar/PV dial to the House Battery card, fixes device cards (battery / EV / heat pump) not appearing on the Overview for setups with no main live-power source, and tidies the Overview layout. No change to tracking or billing.*

### Added

- **Solar / PV dial on the House Battery card.** Set a new **Solar / PV Power Sensor** on a battery — on an existing battery in Meter Config, or when adding one via the device wizard — and the battery card shows a small live PV generation dial (kW) beside the inverter gauge. It's display-only — not recorded in blocks or used in any calculation — and the card is unchanged if you don't set one. This release adds a `pv_power_sensor` column to the meters table; existing databases are migrated automatically on upgrade.

### Fixed

- **Device cards now appear on the Overview without a main power sensor.** Battery, EV, and heat-pump cards were only shown when the main meter had a live-power source — your own power sensor, a BottlecapDave demand sensor, or an opted-in Octopus Mini. On API / DCC-only setups with none of those, the cards were never placed on the page even though the underlying data was correct, so configured devices looked missing. They now render whenever the devices exist, independently of the main power gauge.
- **Overview no longer stretches its cards when four gauges are shown.** A page with four gauges (for example live power plus a battery, an EV, and a heat pump) now keeps the compact, uniform card width instead of switching to a wider, stretched layout. A related miscount could also flip a page with no live-power gauge into the wide layout one card too early; both are corrected. Cards on a full top row now grow to share the width, so a four-card row fills tidily instead of leaving a trailing gap.
- **Info banners line up with the cards.** The Octopus Mini enable/disable notices and the "add a power sensor" hint are now constrained to the same width as the card grid instead of spanning the full window.
- **The gauge row fills the full width with four gauges.** When four gauges push the carbon card onto its own line, the top gauge row now stretches to the same width as the rows beneath it (rather than stopping short), and shrinks with the browser window.
- **Power-card heights stay consistent.** The gauge cards no longer shrink slightly when the carbon card moves onto its own row as the window narrows.
- **Cleaner power gauges.** Removed the small min/max scale numbers under each gauge arc — they were hard to read and duplicated the value already shown in the gauge.

## [3.0.4] — 2026-06-29

*Completes the Intelligent dispatch API migration begun in 3.0.3. No change to how charges are priced for current setups — the same smart-charge slots are detected, now via Octopus's current API.*

### Changed

- **Migrated Intelligent planned dispatches to `flexPlannedDispatches`** — Octopus deprecated the `plannedDispatches` field (scheduled for removal) in favour of `flexPlannedDispatches`, which is keyed by the charge-point device rather than the account and reports `type`/`energyAddedKwh` in place of `meta.source`/`delta`. EMT now discovers the charge-point device id and queries the new field, mapping it back to the same internal shape. Verified against live data (planned-slot and smart-slot counts matched the old field exactly through a real charging schedule) before the old field was removed.
- **Smart-charge detection now recognises both API vocabularies** — the dispatch source is matched against `smart-charge` (legacy) and `smart` (flex), so detection is correct regardless of which spelling Octopus returns. Boost/bump dispatches (`bump-charge`/`boost`) remain excluded from off-peak, as before.
- **Dispatch slot times moved to `start`/`end`** — off the deprecated `startDt`/`endDt` fields (same values, current field names).

### Fixed

- **Deprecation self-check no longer over-reports on generic field names** — fields like `id` exist on nearly every schema type, so name-only matching flagged unrelated deprecations (ledgers, payments) EMT never uses. Generic names are now matched only on the specific types EMT actually reads them from, so the count reflects fields EMT genuinely depends on. With this migration complete, EMT uses no deprecated API fields, so the deprecation notification clears itself.

## [3.0.3] — 2026-06-28

*Resilience to Octopus/Kraken API changes — no functional change for current setups.*

### Changed

- **Future-proofed Intelligent dispatch detection** — moved off Octopus's deprecated `registeredKrakenflexDevice` API (which Octopus has scheduled for removal) to the current `devices` query. Smart-charge / dispatch provider detection keeps working once Octopus withdraws the old field.

### Added

- **API drift self-detection** — if an Octopus/Kraken schema change ever rejects a field the add-on depends on, it now logs a distinct `kraken_schema_drift` error pointing to the Octopus announcements page, instead of a generic "unavailable" message — so a broken query is obvious in the logs and quick to pin down.
- **Upcoming-deprecation early warning** — on the first poll after startup the add-on introspects the live Octopus/Kraken schema and checks the specific fields and enum values it relies on against those Octopus has flagged *deprecated* (the grace window before a field is actually removed). If any are found it raises a Home Assistant **persistent notification** and publishes a `sensor.energy_meter_tracker_api_deprecations` entity (a count, with the affected fields and Octopus's stated reasons in its attributes), alongside a distinct `kraken_field_deprecated` log line — so the alert reaches you outside the logs and you can migrate *ahead* of the break rather than discovering it via `kraken_schema_drift` after it lands. The notification dismisses itself and the sensor returns to `0` once the schema comes back clean; if the endpoint has introspection disabled the check quietly skips and leaves drift-detection as the safety net.

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

## [2.10.9] — 2026-06-03

### Fixed

- **Rogue sub-meter kWh from identical pre/post reads** — when HA restarted briefly during an active block, the gap-fill logic detected identical pre and post reads on sub-meters (sensor value unchanged during the outage) and incorrectly treated this as a daily sensor reset. The reset handler used the full cumulative register value as the block's kWh (e.g. 5798.914 kWh for a battery, 19.38 kWh for an EV charger) instead of a delta of zero. Fixed by splitting the `post_read <= pre_read` condition into two cases: `==` (unchanged sensor, delta is zero) and `<` (genuine register reset, use post_read value).
- **Double gap fill on rapid HA reconnects** — if HA disconnected and reconnected twice in quick succession, `engine_startup` ran twice and both runs detected and filled the same gap. The second fill overwrote the first with slightly different interpolated values. Fixed by checking for an existing interpolated block before writing a gap block — if one already exists for that window, the gap fill is skipped.

---

## [2.10.8] — 2026-05-26

*Final step in the unified cost accounting architecture — see 2.10.4 for the full story.*

### Fixed

- **Unified cost accounting — single implementation** — 2.10.4 through 2.10.7 established the correct methodology (daily net_cost = `round(imp + sc - exp, 2)`, summed across days) and applied it consistently across all surfaces. However, Live Power (`_fmt_total`), Usage Stats (`api_blocks_summary`) and the billing chart (`calculate_billing_summary_for_period`) still had three separate implementations of that logic — any future change to one risked drifting the others back out of alignment, as had happened repeatedly. This release extracts the logic into a single `BlockStore.compute_period_net()` method. `_fmt_total` and `calculate_billing_summary_for_period` now call it directly. There is one implementation and one place to change. All three surfaces are guaranteed to agree.

- **Heatmap grey gap at browser zoom above 100%** — when the heatmap was scaled down by `transform: scale()` (triggered when the chart width exceeds the available viewport width at higher browser zoom), the desktop path set `#scroll` height from `window.innerHeight - 120` without accounting for the scale factor. The scaled content was visually shorter than the scroll container, leaving a grey gap below. Fixed by applying the same `targetH = ceil(vh / scale)` calculation the mobile path already used correctly, capped at actual content height to prevent overscroll.

- **Tab switching marks previous tab stale when width changes** — switching between Billing and Heatmaps now marks the previously active tab stale if the container width has changed since the iframe was created, ensuring a fresh iframe load with correct viewport dimensions on the next switch. Fixes resize issues after window resize or zoom change between tab switches. No reload if width is unchanged.

---

## [2.10.7] — 2026-05-25

### Fixed

- **Usage Stats refresh timing** — the 2-minute `setInterval` added in 2.10.5 could fire mid-block while the engine was actively finalising, hitting a WAL lock and returning a partial or invalid response from `api/blocks_summary`. This caused missing import values and incorrect bar colours until the page was refreshed. Replaced with a boundary-aware `scheduleChartRefresh()` that fires 1 minute after each block boundary — the same timing as the Live Power billing card refresh — when the engine is quiescent and the DB is safe to read. All three tabs (Billing, Heatmaps, Usage Stats) now refresh together. `block_minutes` added to `api/blocks_summary` response so the scheduler uses the correct interval for any block size.

- **Transient WAL lock in `load_config`** — if the engine held a write lock on `blocks.db` at the exact moment the server's `load_config` queried the config period, the query returned no result and `load_config` returned an empty config. `api/blocks_summary` then produced a response with no meters, causing missing import values and wrong bar colours in Usage Stats. Fixed by retrying the config period query up to 3 times with a 100ms delay before concluding there is no active config period.

---

## [2.10.6] — 2026-05-25

### Fixed

- **Sub-meter sensor reset handling** — when a sub-meter sensor resets mid-block (e.g. a daily-reset cumulative kWh sensor such as Teslemetry's Powerwall battery import), the engine previously skipped the block entirely, recording zero consumption. The engine now detects the reset from the negative delta and uses the post-reset value directly as the block's kWh. Applies to any reset cause — daily midnight, plug reconnect, or any other. No configuration change required.

- **Gap block double-counting kWh** — when the engine filled a gap after a restart, the last gap block's `read_end` was set to an interpolated value that could exceed the next real block's `read_start`, causing the same register space to be counted in both blocks. The block sum could exceed the meter register delta — physically impossible. Fixed by anchoring the last gap window's `read_end` to the actual `post_read` value. Affects both import and export. Blocks written before this fix are not retroactively corrected.

- **Export register reads missing from billing chart** — `get_blocks_lightweight` omitted `exp_read_start` and `exp_read_end` from both the SQL query and the export channel dict. Export now shows Start/End register reads in the billing summary alongside import.

---

## [2.10.5] — 2026-05-23

*Part of the unified cost accounting evolution — see 2.10.4.*

### Fixed

- **Usage Stats auto-refresh** — the Usage Stats tab was not included in the 2-minute auto-refresh interval that refreshes Billing and Heatmaps. Users would see stale data unless they manually switched tabs. Fixed by including `bar` in the refresh. Superseded in 2.10.7 by boundary-aware scheduling.

---

## [2.10.4] — 2026-05-23

*This release began a sequence of improvements — 2.10.4 through 2.10.8 — that progressively unified cost accounting across all surfaces of EMT (Live Power billing cards, Usage Stats, and the billing chart). Each release closed a specific gap. 2.10.8 completed the work by extracting the single canonical implementation into `BlockStore.compute_period_net()`.*

### Fixed

- **Unified cost accounting methodology** — established the canonical method: block costs are summed per local day at 4dp, `net_cost = round(imp + sc - exp, 2)` per day, daily nets summed for all period totals. This guarantees parts always sum to totals at every aggregation level. Removed `barPeriodTotals` — a workaround that substituted SQL aggregates for daily row sums and introduced its own inconsistencies. Applied the method to Usage Stats (`net_cost` field per row, grand total from `agg.net_cost`), the billing chart (`total_cost` from daily nets), and Live Power (`_fmt_total` from daily nets). Sub-meter handling was not yet fully consistent across surfaces — resolved in 2.10.8.

---

## [2.10.3] — 2026-05-20

### Fixed

- **PDF dark theme charts** — billing charts and heatmap PDFs were rendering in dark theme when the user had dark mode enabled. `Plotly.toImage()` captures the chart exactly as rendered in the iframe DOM, so forcing light colours in the popup window CSS had no effect on the captured images. Fixed by calling `Plotly.relayout()` on the chart with light background and font colours immediately before capture, then restoring the original colours afterwards. The iframe is hidden during this operation so the user does not see the colour change. Only `paper_bgcolor`, `plot_bgcolor` and `font.color` are changed — touching axis configuration caused chart traces to be discarded, mangling the chart until Plotly re-rendered.

---

## [2.10.2] — 2026-05-20

### Fixed

- **Fresh install hang** — on a brand new installation with no existing data, the addon would hang indefinitely at startup and never serve the UI. Root cause: the SQLite online backup API (`conn.backup()`) was called during the upgrade-backup routine on startup. On a freshly-created WAL-mode database, `executescript()` during schema creation leaves an open read transaction that the backup API cannot acquire a lock around, causing it to block forever. Fixed by skipping the upgrade backup on fresh installs with zero blocks — there is nothing to back up, and the version is recorded so the backup runs correctly on the next actual upgrade.
- **Startup diagnostics** — if the web server fails to initialise after engine startup, the failure is now logged with a full traceback rather than exiting silently. stderr is also redirected to stdout to capture crashes in background threads. No functional change for working installations.

## [2.10.1] — 2026-05-20

### Fixed

- Diagnostic logging only — no functional fix. Superseded by 2.10.2.

---

## [2.10.0] — 2026-05-18

### Added

- **Per-day data table** — expandable half-hourly data table below each daily chart, showing Total Import, Grid Export, Direct Import, and per-device kWh and cost at each rate. Lazy-built on first open. State persists across the 2-minute auto-refresh via postMessage.
- **Show Data / Hide Data button** — added to the billing floating toolbar (after Latest first / Oldest first). Toggles all data tables for the current period open or closed in one click. Per-day toggles remain available on each individual chart. Button label updates dynamically to reflect current state.
- **PDF export** — ⬇ PDF button on all three chart tabs:
  - *Billing Charts* — captures the current period and view (Bill / vs Prev / vs Last Year). Billing summary tables rendered with full styling. Daily sections show the sidebar (date, totals, per-meter breakdown with colours) alongside a Plotly chart image captured via `Plotly.toImage()`. Open data tables included below each chart. Respects current sort order and table open/close state.
  - *Heatmaps* — exports the active metric heatmap as a PNG image via `Plotly.toImage()`, with active metric label shown.
  - *Usage Stats* — exports the bar chart image, the full toolbar showing selected options (period, metric, view, standing charge), and the data table.
  - All tabs: EMT logo embedded as data URL, generated timestamp, version number. Light theme forced regardless of system dark mode setting. Loading spinner shown in the popup window while charts are being captured.
- **Sub-meter boundary interpolation** — provisional blocks are retrospectively amended when the first post-boundary read arrives from a sub-meter. Corrects up to ~0.12 kWh per-boundary misalignment at 7.4 kW charge rate without affecting period billing totals.

### Fixed

- **Grid Export legend label** was displaying "Direct Import Grid Export" — now correctly shows "Grid Export".
- **Daily chart iframe** now loads via a Blob URL instead of `srcdoc`, removing the WebKit ~64 KB size limit that caused blank charts on year view.
- **Direct Import kWh** was incorrect on days where devices drew from the house battery — now uses `kwh_remainder` (the authoritative engine pass-2 remainder) rather than a subtraction from total import.
- **Device kWh** throughout (billing sidebar, data tables, PDF) now shows grid-attributed consumption only (`kwh_grid`), consistent with the Usage Stats tab.
- **Billing summary cost accumulation** — period totals now accumulate per-day values rounded to display precision (2dp cost, 3dp kWh) rather than raw floats. Period totals now agree exactly with the sum of displayed daily figures.
- **Usage Stats Totals row** now equals the sum of displayed daily rows for all columns, including devices and export.
- **Rounding consistency** — all cost and kWh totals now use a single rounding strategy throughout: raw block costs are accumulated as floats and rounded once to display precision at the end, matching how Octopus bills (sum raw slot costs, round once). This eliminates ±£0.01 drift between the billing summary, sidebar, usage stats, and live power billing cards.
- **Live Power billing card totals** — the Today / This Bill / This Year headline figures now always equal the sum of the displayed line items. Previously the headline was computed from raw float SQL values while each row was independently rounded to 2dp for display, causing the headline to disagree by ±£0.01. All row costs are now rounded to 2dp before display, and the headline is recomputed from those same rounded values.
- **Usage Stats period totals** — costs and standing charge sent from the server to the JS at higher precision (4dp) per day rather than being pre-rounded to 2dp. Previously: sub-meter costs rounded to 2dp/day causing ±£0.01 period error; standing charge rounded to 2dp/day causing a systematic error of £0.004/day × number of days (£0.07 over a 16-day period at £0.5046/day). All daily values now sent at 4dp so the JS sums them correctly before rounding once at the end.
- **Billing chart sidebar sub-meter totals** — the per-meter cost totals in the billing period summary sidebar were computed by rounding each individual half-hourly slot cost to 4dp before summing. With enough blocks (e.g. a full day of EV charging across 9 slots), the accumulated rounding error was sufficient to tip the displayed total by ±£0.01 vs the raw sum. Fixed by summing raw slot values and rounding once at the end.
- **Regenerate Charts button removed** — chart generation is handled automatically on each block boundary; the manual trigger is no longer needed.

---

## [2.9.0] — 2026-05-13

### Added

- **Device retirement** — sub-meters can be archived from a specific date without deleting historical data. Use the **Archive** button on any sub-meter card in Settings → Meter Config. Retired devices stop recording, disappear from Live Power, and their sensor entity IDs are freed for reuse by a replacement device. The engine skips retired meters in `capture_samples` and `build_gap_blocks` from the retirement date onward. Retirement is reversible via **↩ Unretire** — with a conflict check that prevents unretiring a device whose sensors are already in use by an active meter.

- **12-hour gap-fill limit** — gaps longer than 12 hours are no longer gap-filled. Short gaps (power cuts, brief restarts) still gap-fill as before. Extended outages (CAD failure, HA device failure) are correctly left as absent data rather than fabricated interpolation. `GAP_FILL_LIMIT_HOURS = 12` is a module-level constant applied consistently to both the `engine_startup` immediate gap-fill path and the `_engine_tick` deferred gap-fill path.

- **Meter reset advisory** — when a gap exceeds 12 hours and the post-gap import read is significantly lower than the pre-gap read (> 50 kWh drop), an orange advisory banner suggests creating a new billing period. Covers meter replacement and moving to a new property. Banner is dismissible for the browser session. Flag is reset at the start of every `engine_startup` call so reconnects never carry stale state.

- **Just-unretired sub-meter protection** — when a sub-meter is unretired after a long absence, the engine detects this (last block > 12 hours old) and removes the sub-meter from `pre_reads` on startup, preventing the entire retirement-period accumulation from being dumped into the first resumed block.

### Fixed

- **PDF export — Usage tab showing carbon comparison narrative** — the carbon comparison panel (`cmp-panel`) lives inside the Carbon Insights container and was being incorrectly captured in Usage PDF exports. Usage PDF now captures only the eight `ucard-*` elements directly.

### Changed

- **PASS 2 — live blocks clip to grid import** — when a sub-meter's kWh exceeds the parent grid import on a live block, it is now clipped to the actual grid import. Gap-fill blocks (`interpolated=True`) retain the previous preserve-as-is behaviour since attribution across a gap is expected to be imperfect.

- **PASS 2 — sub-meter negative delta** — a negative delta on a sub-meter indicates the read sensor is reporting net rather than cumulative import (a misconfiguration). PASS 2 now logs a WARNING and skips the block.

- **`inverter_possible` removed** — the flag had no meaningful effect on energy attribution. All sub-meters use the protected queue in PASS 2. The column is retained in the DB schema for backward compatibility but always written as 0. A startup migration clears any existing `inverter_possible=1` flags.

- **V2X checkbox** — now clearly documented as affecting only the Live Power gauge direction (bidirectional vs unidirectional). No effect on kWh recording or cost attribution. Sub-meter sensors must always be cumulative import only regardless of V2X capability.

- **EV Insights — "AC from grid" relabelled to "AC delivered"** — the figure includes all energy delivered to the vehicle regardless of source (grid, solar, battery). Carbon is calculated using grid CI as the proxy for all sources, consistent with the export offset methodology.

- **Sub-meter carbon uses raw `imp_kwh`** — Carbon Insights EV and battery cards use the raw device sensor reading (before PASS 2 clipping) for carbon and mileage calculations. Grid cost remains clipped. Solar or battery charged sessions contribute correctly to mileage estimates even though their grid cost is zero.

---

## [2.8.3] — 2026-05-07

### Fixed

- **PDF export — Usage tab showing carbon comparison narrative** — the `cmp-panel` element (which contains carbon comparison bullets) lives inside `insights-body` (Carbon's container) and was being captured separately for the Usage PDF, causing the carbon narrative to appear at the top of Usage reports. Usage PDF now captures only the eight `ucard-*` elements directly, skipping `cmp-panel` entirely. The usage comparison narrative (`ucard-narrative`) is already included in that list and is correctly excluded when no comparison period is active.

---

## [2.8.2] — 2026-05-03

### Fixed

- **PDF export — Carbon duplicate comparison panel** — the "Compared to" panel was appearing twice in Carbon Insights PDF because `#cmp-panel` lives inside `#insights-body` and was also being captured separately. Carbon PDF now captures `#insights-body` once.

- **PDF export — Usage narrative shown when no comparison selected** — `#ucard-narrative` was included in the PDF via `outerHTML` even when hidden (`display:none`). Usage PDF now checks each card's `style.display` individually before including it, so only visible cards appear in the report.

---

## [2.8.1] — 2026-05-03

### Added

- **Favicon** — browser tab now shows the EMT icon (`icon.png`) in all pages.

- **Timezone auto-detect in setup wizard** — the timezone field in Meter Config now defaults to the browser's local timezone via `Intl.DateTimeFormat().resolvedOptions().timeZone` rather than UTC. Existing meters are unaffected.

- **PDF export on Insights** — "⬇ PDF" button on the Insights toolbar opens a new browser tab containing a clean, print-ready report with the EMT logo, period label, app version, and the full Carbon or Usage tab content. No navigation or sidebar included. Print from the new tab to save as PDF.

- **Generation mix history table** (`mix_history`) — new table storing generation mix at CI-tick resolution (~15 min) independently of block size. Previously mix was stored only at block finalisation (30 min in production), causing the 48-hour mix chart to lag by up to 60+ minutes. Now lags by at most one CI tick interval. Existing installations are backfilled from `generation_mix` on first open.

- **Usage Stats Net view — Import/Export columns** — the data table in the Net tab now shows Import and Export as separate columns alongside the Total (net) figure, for kWh, £ Cost and CO₂. The chart is unchanged.

### Changed

- **Gauge arc background** — the background arc on Live Power gauges (main power gauge and all SoC device cards) now uses `var(--border)` via CSS class rather than a hardcoded dark colour. In light theme the arc is now correctly light grey rather than black.

- **48-hour generation mix chart** — data source changed from `generation_mix` (block-resolution, joined to blocks) to the new `mix_history` table (CI-tick resolution). Chart stays current within ~15 minutes regardless of block size.

- **Carbon Insights comparison badges** — Grid Export kWh metric now shows a % comparison badge when a comparison period is selected. Badges on device card headlines (Battery, EV, HP) now appear inline with the value using `display:flex` rather than dropping to a new line.

### Fixed

- **Charts — Net view data table** — the redundant "Net" column (identical to the Total column) has been removed. The data table now shows Import and Export as separate columns; the Total column is Import − Export.

- **Charts — period not recalled on return** — `barLoadState` was checking `st.month.year` which was never saved, so the billing period selection was never restored. Fixed to restore from `st.bKey` directly.

---

## [2.8.0] — 2026-04-30

### Added

- **Usage Insights tab** — new 💷 Usage tab on the Insights page alongside Carbon. Shows Cost & Earnings (net cost, import cost, export earnings, standing charge, weighted average rate), Rate Period Usage (how import is distributed across tariff rate tiers — only shown when >1 rate), Grid Position (net kWh, import, export, direct-to-house, net exporter days), Peak Demand Window (2-hour local-time window with highest house grid draw), and per-device cards for Battery, EV Charger and Heat Pump (shown only when that meter type is configured).

- **Usage Insights narrative comparison** — when a comparison period is selected, a "Compared to [period]" card appears above the usage cards with plain-English sentences covering the top drivers of cost change. Identifies whether rate changes or volume changes drove import cost differences, and correctly handles the case where increased EV charging kWh coincided with a cheaper rate (volume up, cost down).

- **Generation mix card on Live Power** — new 🌐 Current Grid Generation Mix card sits as the first card in the gauge row. Shows a donut chart of the current half-hour's fuel split (wind, solar, nuclear, gas, biomass, hydro, imports, other, coal). Hovering a segment updates the centre label to show that fuel's percentage. Full legend with all fuels. Updates on the same 5-second poll cycle. Only shown when a postcode is configured. 🇬🇧 UK only.

- **48-hour generation mix chart** — third toggle on the 48-hour power history chart: kW / CO₂ / **Mix**. Stacked area chart showing % of generation by fuel type over the last 48 hours, with the same zoom, tooltip and now-marker infrastructure as kW and CO₂ modes. Clean fuels (wind, nuclear, solar) stack at the bottom; dirty fuels (gas, coal) sit prominently at the top. Semi-transparent fills with crisp 1.5px stroke lines per fuel boundary. 🇬🇧 UK only.

- **Generation mix in Carbon Insights** — a horizontal stacked bar at the bottom of the Carbon Summary card shows the imp_kwh-weighted average fuel split for the period, using the same canonical colour palette as Live Power. When a comparison period is selected, a second bar appears below labelled "Compared to [period]:" so the two period mixes can be compared visually. The comparison panel also gains mix-change narrative lines for the top 2 fuels that shifted ≥5pp, noting the carbon impact direction (e.g. "Gas was 27.8% of the mix (−5.8pp) — lower carbon intensity").

- **Canonical fuel colour palette** — consistent across Live Power and Insights: wind (#3b82f6 blue), solar (#facc15 gold), nuclear (#7c3aed deep purple), gas (#f97316 orange), biomass (#92400e brown), hydro (#38bdf8 sky blue), imports (#9ca3af grey), other (#6b7280 dark grey), coal (#374151 charcoal). Follows Ember/NESO/Our World in Data convention.

- **Insights tab state persistence** — active tab (Carbon/Usage), period mode (Month/Year), year, month and compare mode are saved to `localStorage` on every navigation and restored on page load. Navigating ← → saves position after the update, not before.

- **Insights auto-default comparison** — when no comparison is saved, the most recent available comparison period is automatically selected (prev month in Month mode, last year in Year mode). Manually toggling a comparison off is remembered for the session.

- **Insights icon** — sidebar navigation icon updated to 💡 (from 🌿 which is now the Carbon tab). Usage tab uses 💷.

- **`/api/power/mix-history`** — new endpoint returning generation mix per 30-minute block for the last 48 hours, grouped by `block_start`. Consumed by the Mix chart tab.

- **`generation_mix` field in `/api/power`** — current grid fuel split added to the live power response, consumed by the donut card.

- **`generation_mix` field in `/api/insights/*`** — `_aggregate_insights` now includes imp_kwh-weighted average generation mix for the period in all insight endpoints.

### Changed

- **Billing chart performance** — `build_day_chart_html` now outputs chart data as `<script type="application/json">` blocks instead of inline JavaScript. A single shared `_buildDayChart()` function reads the JSON and constructs Plotly traces only when a section becomes visible. JavaScript parsed by browser reduced from **5,842 KB → 76 KB** (77× reduction). Total file size reduced from **6,610 KB → 2,649 KB**.

- **Timezone refactor** — `local_date`, `local_year`, `local_month`, `local_day` columns dropped from the `blocks` table. All queries now compute UTC bounds at query time via `local_date_to_utc_bounds(local_date, tz_name)`. Retroactively corrects wrong-timezone users without data migration.

- **Covering index** — `idx_blocks_insights` widened from 6 to 13 columns, making `_aggregate_insights`, `_aggregate_usage` and `api_blocks_summary` all run as covering index scans (zero row fetches). Existing narrow index is detected and rebuilt automatically on first open.

- **Direct import cost** — `api_blocks_summary` (Usage Stats chart) now uses rate-based subtraction (`main_cost[rate] - sub_cost[rate]`) matching `calculate_billing_summary_for_period` and 2.7.1 behaviour. Previously used full `imp_cost`, inflating "Direct import" by including EV and battery charging costs.

- **W→kW unit conversion** — sub-meter inverter power sensor conversion now uses `unit_of_measurement` from HA attributes instead of a magnitude heuristic (`abs(fv) > 100`). Correctly handles 50W sensors (too small for old heuristic) and 150kW EV chargers (too large for old heuristic).

- **Gauge scale** — Live Power gauge scale derived from `power_history` p90 percentile rather than a 7-day block loop. Faster and more accurate.

- **Generation mix storage** — mix data now stored only against `electricity_main` blocks, not against sub-meter blocks. Pre-2.8.0 redundant rows (stored 3× per block) are deleted on first open. Frees ~830 KB for a typical 3-meter installation.

- **Generation mix seed on startup** — `_current_slot_mix` (in-memory dict lost on restart) is now seeded from the last 4 hours of `generation_mix` DB rows on startup. If the seed is empty, `_tick_carbon_intensity` fires immediately rather than waiting up to 30 minutes. Fixes mix data gaps after rebuilds.

- **Carbon accuracy feature dropped** — the planned `intensity_actual` backfill from NESO was removed. The NESO regional API does not provide an `actual` field — only `forecast`. Regional actuals do not exist. The `_backfill_carbon_gaps` function (196 lines) has been deleted. The `intensity_actual` column remains in the schema but will not be populated.

- **Sub-meter export sections in billing summary** — `calculate_billing_summary_for_period` skips sub-meter export channels. Previously both `import` and `export` channels were included for sub-meters, causing export earnings to appear inflated in billing summaries.

---

## [2.7.1] — 2026-04-28

### Added

- **Insights page — calendar month/year navigation** — replaced billing-period-only view with a floating toolbar matching the Usage Stats pattern. Month/Year toggle with ← → navigation. Compare buttons: "vs Last Month", "vs Same Month Last Year" (month mode) and "vs Last Year" (year mode). Buttons are disabled when no data exists for the comparison period, gated by a new `/api/insights/data-bounds` endpoint.

- **Insights page — narrative comparison panel** — when a comparison period is selected, a summary panel appears above the Carbon Summary card with plain-English sentences covering net carbon change, grid intensity (greener/dirtier), solar export, EV miles driven (from assumptions), battery and heat pump kWh. Colour-coded teal (better) / red (worse). Delta badges also appear on individual metric values.

- **CI gap backfill** — on startup the engine scans for blocks with NULL `carbon_g` and fetches historical carbon intensity from the National Grid ESO API for missing periods. First run for a postcode: 60-day lookback. Subsequent restarts: 48-hour window only. Keyed to postcode in `store_meta` so changing postcode triggers a fresh backfill. On version upgrade, flag is cleared so historical backfill runs once for the new version.

- **PASS 2 on gap-fill blocks** — `_apply_pass2()` extracted as a standalone function and now called from both `finalise_block` and `build_gap_blocks`. Gap-fill blocks now correctly set `imp_kwh_grid`, `imp_kwh_remainder`, and `imp_kwh_battery` — previously these were NULL for all interpolated blocks.

- **Sub-meter spike detection in gap fill** — during gap block construction, if a sub-meter's interpolated kWh exceeds the parent meter's interpolated kWh for the same window (with 5% tolerance), an ERROR is logged and the value is clipped to the parent.

- **Main meter cascade delete** — deleting the main meter now wipes the entire database, creates a backup first, and restarts the engine. Modal warns clearly: "entire database deleted", backup location mentioned, requires typing DELETE to confirm.

- **Postcode normalisation** — postcode is stripped to outward code (e.g. `DE1 3AT` → `DE1`) on save in Meter Config and defensively in all engine/server read paths.

- **New Insights API endpoints** — `/api/insights/calendar-month`, `/api/insights/calendar-year`, `/api/insights/data-bounds`, `/api/meter/main/reset`.

- **`_aggregate_insights` performance** — direct 5-column SQL query replaces `get_blocks_for_range` + full block reconstruction. ~7× faster (465ms → 65ms for a full month). `idx_blocks_insights` covering index added to `block_store.py`.

- **Billing chart sort order** — "Oldest first / Latest first" toggle added to the billing chart toolbar.

- **Billing sub-meter breakdown rows restored** — "Direct import" (grid minus sub-meters), each sub-meter, and standing charge shown as separate rows in Live Power billing cards and billing chart detail.

### Changed

- **Terminology: "Direct import"** — "Grid Import (remainder)" / "House (remainder)" replaced with "Direct import" consistently across billing cards, billing chart table, daily chart legend, help page, and insights. Usage Stats chart legend uses "Direct" (rendered as "Direct import" / "Direct carbon" by the chart engine).

- **Regenerate Charts hidden on Usage Stats tab** — the Regenerate Charts button only appears on the Billing and Heatmaps tabs where it is relevant.

- **Live Power gauge improvements** — gauge needle alignment fixed (`margin-top: auto`), pill moved above gauge, Layout A/B system for ≤3 and 4+ gauges respectively, heat pump arc colour changed to orange, `dashoffset=0` for unidirectional arcs, Layout B stretches to fill row.

- **Global spacing** — `--radius` set to 6px, topbar/content/card spacing tightened in `base.html`.

### Fixed

- **Grid import double-count in Live Power billing card** — total import now uses a separate SQL query for raw grid kWh (`imp_kwh` on main meter), ensuring sub-meter kWh is not added twice when computing "Direct import".

- **Sensor clear `bubbles: true`** — sensor clear button now dispatches change event with `bubbles: true` so the config state updates correctly.

- **Sub-meter exceeding parent grid import** — PASS 2 now logs `ERROR` when any sub-meter's import exceeds the total parent grid import, clearly identifying the sensor as likely misconfigured.

---

## [2.7.0] — 2026-04-26

### Added

- **Battery SoC dial on Live Power** — optional SoC sensor per battery sub-meter. When configured, a SoC dial appears on the Live Power page alongside the power gauge. Colour shifts green→amber→red based on charge level.

- **Inverter power gauge** — optional inverter power sensor shows live kW below the SoC dial. Invert checkbox for batteries where positive = charging.

- **EV/Heat Pump power gauge** — optional device power sensor for EV charger and heat pump sub-meters, shown as a unidirectional arc gauge on Live Power.

- **Sub-meter card layout** — each configured sub-meter renders its own card on Live Power with appropriate gauge type (bidirectional for V2X, unidirectional for EV/HP, SoC+inverter for battery).

- **V2X bidirectional gauge** — EV chargers with `v2x_capable: true` get a bidirectional gauge matching the main power gauge, with green fill for vehicle-to-grid export.

- **Sub-meter history endpoint** — `/api/sub-meter/history?meter_id=X` returns 95th-percentile `max_kw` over the last 48 hours, used to auto-scale inverter gauges without hardcoding.

- **Meter type selector in Meter Config** — sub-meters now have an explicit Meter Type dropdown (Battery / EV Charger / Heat Pump). Type is locked once data has been recorded. Old installs infer type from meter ID keywords for backward compatibility.

- **Device power sensor field** — EV charger and heat pump sub-meters gain a dedicated "Device Power Sensor" field in Meter Config, wired to the new Live Power gauge.

- **Add Device modal** — redesigned "Add Device" flow for adding sub-meters from Live Power or Meter Config, supporting all three device types with type-specific sensor fields.

- **Config history** — config changes now record a user-supplied change reason (optional). Shown in Billing History.

### Fixed

- **Billing period edge case** — periods with identical `effective_from` dates no longer produce duplicate rows in billing history.

- **Wizard postcode field** — postcode field in the setup wizard now correctly shows/hides based on timezone selection.



### Added

- **Usage Stats data table — scrollable on desktop** — `#chart-bar` converted to a viewport-filling flex column. Chart is `flex-shrink:0` and stays pinned above the table. Table wrapper is `flex:1; min-height:0` and scrolls independently. On mobile natural page scroll is preserved.

- **Usage Stats data table — sticky column headers and totals row** — column headers stick at the top while scrolling. Totals row moved from the bottom of `<tbody>` to a second `<thead>` row, pinned just below the column headers so it is always visible.

- **Usage Stats data table — sortable date column** — clicking the Period column header toggles ascending/descending sort with a ↑/↓ indicator. Defaults to newest first. Preference persisted in localStorage.

- **Usage Stats data table — alternate row shading** — odd/even rows use `var(--surface)` / `var(--bg)` for zebra striping that works in both light and dark mode.

- **Usage Stats data table — colour dots in column headers** — each column that maps to a specific meter or direction (import, export, standing charge) shows a coloured dot matching the chart legend. Columns with no single colour (Period, Total, Net, Avg intensity) show no dot.

- **Billing chart — day chart order toggle** — "↓ Newest first / ↑ Oldest first" button added to the billing chart toolbar. Reverses the order of day charts within the displayed billing period. Useful for comparing against a PDF utility bill. Preference persisted in localStorage and restored on page load. The period dropdown navigator is unaffected.

- **Heatmap — fills available desktop viewport** — scroll height was hardcoded to 31 rows. Now calculates maximum height from `window.innerHeight` so the heatmap uses all available vertical space on desktop.

- **Heatmap — larger metric toggle buttons on mobile** — kWh/CO₂/gCO₂/kWh toggle buttons were very small touch targets on mobile. Added `@media (max-width: 600px)` override: `min-height: 40px`, larger padding, `min-width: 64px`.

### Fixed

- **Carbon intensity fetched after catch-up block finalisation** — on the first engine tick after a restart or rebuild, `ensure_correct_block` finalised all catch-up blocks before `_tick_carbon_intensity` had run. Those blocks were assigned carbon values from pre-restart CI data which could be stale. Fixed by moving the CI fetch to run before the block lifecycle in every tick.


- **Deprecated architecture values in config.yaml** — `armhf` and `armv7` removed from `arch` list. Supported architectures are now `aarch64` and `amd64` only.

---

## [2.6.2] — 2026-04-22

### Added

- **GitHub Wiki link in Help page** — new "Further Reading & Support" section at the bottom of the Help page linking to the GitHub wiki (sensor requirements, integration guides), issues, and repository.

### Changed

- **Theme toggle consolidated to logo** — the `☾` toggle buttons in the sidebar footer and charts toolbar have been removed. The logo in the top-left of the sidebar is now the sole theme toggle, consistent across all pages. The heatmap iframe theme sync is preserved so theme changes propagate correctly into generated charts.

- **Logo size increased** — logo width increased from 160px to 180px with sidebar brand padding adjusted to fit cleanly within the sidebar without changing its width.

- **Insights mobile topbar** — subtitle hidden on mobile, Carbon tab collapses to emoji-only, period navigation compacts to fit a single row. Consistent with the charts page mobile pattern.

- **Insights metric labels simplified** — "Grid import (CI blocks)" and "Grid export (CI blocks)" shortened to "Grid import" and "Grid export". The footnote already explains the CI-only basis.

---

## [2.6.1] — 2026-04-21

### Added

- **Logo theme toggle** — clicking the Energy Meter Tracker logo in the top-left sidebar now toggles between light and dark mode, identical to the existing button in the bottom right. The logo adapts visually in light mode via CSS filter inversion.

### Fixed

- **Sub-meter display rate used weighted average instead of last rate** — at tariff boundaries (e.g. Octopus off-peak → peak transition) a sub-meter block that recorded a tiny amount of kWh spanning both sides of the boundary would compute a weighted average rate (e.g. `£0.189`) rather than using the last rate in the block (e.g. `£0.323`). This caused the sub-meter rate line on charts to show an anomalous mid-point value while the main meter showed the correct end-of-block rate. All meters now consistently use the last captured rate in the block, which is the intent — capturing the rate as close to block end as possible to account for any API lag from remote rate providers.

- **Net bar colour wrong when standing charge tips a day positive** — on the Net billing chart with "Inc. Standing Charge" enabled, days where the standing charge pushed a net-credit day into net-cost showed the bar above zero in the credit colour (grey) rather than the cost colour (blue). The bar height calculation correctly included the standing charge but the colour check did not. Fixed by including `barStandingVal(agg)` in the colour check to match the bar height calculation exactly.

- **Session gap detection silently failing on non-30-minute block setups** — `detect_gap` was always called with the default `block_minutes=30` during startup gap detection, regardless of the actual configured block size. On 5-minute or 15-minute block setups, window boundaries were computed in 30-minute increments, causing the gap to evaluate to zero or far fewer missing windows than actually existed — `no session gap detected` was logged when up to 90 minutes of data was missing. Fixed by reading `block_minutes` from the last block's own meta before calling `detect_gap`. This particularly affects users outside the UK where 5-minute smart meter data is common.

---

## [2.6.0] — 2026-04-20

### Added

- **Insights page** — new sidebar entry 🌿 between Charts and Live Power. Billing period carbon analysis with narrative cards. Period selector (prev/next) in topbar. Click any trend chart bar to navigate directly to that period.

- **Carbon Summary card** — net kgCO₂ for the billing period, effective intensity vs grid average, import/export split, car/tree/flight equivalences, inline SVG trend chart across all periods, coverage warning when CI data is below 95%.

- **House Consumption card** — grid import attributable directly to the house (total minus EV and battery sub-meters). Shows kWh and carbon from CI-covered blocks only. Notes that battery discharge into the house cannot be separated without generation-side metering.

- **Solar Export Offset card** — export displaced X kgCO₂ from the grid. Equivalences. Shown only when export > 0.1 kWh.

- **EV Charging card** — AC from grid, usable DC stored (after charge efficiency loss), average charge intensity vs grid average, estimated mileage, gCO₂/mile vs petrol comparison. Carbon and kWh figures use CI-covered blocks only to avoid mixing partial carbon with full-period kWh. Coverage warning when below 80%.

- **Battery Charging Behaviour card** — average charge intensity vs grid average, estimated carbon saving if charged during cleaner periods. Honest note that export cannot be split between solar and battery without generation-side metering.

- **Heat Pump card** — electricity used, estimated heat delivered (× SCOP), carbon vs equivalent gas boiler, % cleaner, crossover grid intensity. Uses CI-covered kWh only for the gas comparison.

- **inverter_possible caveat** — EV, battery and heat pump cards show a warning if the sub-meter is marked inverter-capable, noting that carbon shown is a maximum and may be lower if solar or battery contributed.

- **Settings page** — new sidebar entry ⚙️ (replaces Meter Config entry). Two tabs: Meter Config (existing config content) and Carbon (assumptions). Carbon tab has postcode prompt if not configured, linked to the same config save API as Meter Config.

- **Carbon assumptions** — petrol/diesel car gCO₂/mile, tree kgCO₂/year, flight LHR→NYC kgCO₂, distance unit (miles/km), export displacement methodology (grid average or custom), EV efficiency (miles/kWh battery), EV charge efficiency (AC→DC), battery round-trip efficiency, heat pump SCOP, gas boiler efficiency, gas gCO₂/kWh. All with citations. Stored in `store_meta` table — no schema change.

- **`GET/POST /api/settings`** — reads and writes carbon assumptions. Missing keys fall back to SETTINGS_DEFAULTS at read time.

- **`GET /api/insights/periods`** — lists all billing periods with quick carbon summary (SQL aggregate, no block loading).

- **`GET /api/insights/billing-period`** — full carbon breakdown for one billing period. Returns CI-only kWh (`ci_imp_kwh`, `ci_exp_kwh`) separately from total kWh for all meters. Sub-meters include `avg_charge_intensity`, `ci_imp_kwh`, and `inverter_possible` flag.

- **Meter type detection** — resolves sub-meter type from `meta.meter_type` first, then falls back to meter ID keywords (ev/charger → ev_charger, battery/batt → battery, heat/pump → heat_pump, solar/pv/inv → inverter).

- **`subbar` block** — new Jinja2 block in `base.html` between topbar and content. Used by Settings/Meter Config for secondary actions (Wizard, Refresh, Billing History).

### Changed

- **Navigation restructured** — sidebar now: ⚙️ Settings | 📊 Charts | 🌿 Insights | ⚡ Live Power | 🗄️ Data Management | 📋 Logs | 📖 Help. Separate Settings entry removed.

- **Routes renamed** — `/config` → `/settings`, `/import` → `/data-management`, `/summary` → `/live-power`, `/config-history` → `/billing-history`. Old cookie values mapped forward for backwards compatibility.

- **Templates renamed** — `config.html` → `meter_config.html`, `import.html` → `data_management.html`, `summary.html` → `live_power.html`, `config_history.html` → `billing_history.html`.

- **Settings page** renders `meter_config.html` by default (`?tab=carbon` for the Carbon tab). Meter Config sub-bar (Wizard, Refresh, Billing History) moved below the topbar using the new subbar block.

- **Sub-meter rate sensor wording** — label changed from "pre-filled from main meter" to "default: one-time copy from main meter at setup — update here if your sub-meter has a different rate or you retrospectively change rates". Clarifies this is not a live link.

### Fixed

- **Insights carbon/kWh consistency** — all carbon figures and their accompanying kWh use CI-covered blocks only (`ci_imp_kwh`). Previously total-period kWh was mixed with partial-period carbon, producing misleadingly low intensity figures and incorrect mileage estimates.

- **Upgrade from 1.x.x / 2.0.x (Docker) showed wizard and lost sensor config** — when upgrading from any version that stored data in `blocks.json` and config in `meters_config.json` (all 1.x.x and 2.0.x releases), the auto-migration to SQLite correctly migrated all block data but created an empty `config_periods` row because `load_config()` returns `{}` when `config_periods` is empty — a chicken-and-egg problem. The wizard then appeared as if it were a fresh install, and users with no prior DB thought their data was gone. Fixed in two places: (1) `migrate_json_to_sqlite` now explicitly reads `meters_config.json` directly before creating the config period, bypassing `load_config()`; (2) a post-startup repair step detects an empty `config_periods` row and populates it from `meters_config.json` if present — so existing installs that already migrated blocks but got an empty config period will self-heal on next restart without any user action.

- **Config not persisting after restore** — `engine_startup` previously called `load_config()` before opening the block store. With `_store=None` and `blocks.db` already present, `load_config()` returned `{}`, causing the startup config sync to reset scalar fields (billing day, timezone, etc.) to defaults and wipe sensor subscriptions. Fixed by opening the store before calling `load_config()` and removing the dangerous startup config sync entirely — the DB is authoritative.

- **`load_config()` — DB always authoritative** — both `server.py` and `engine.py` no longer fall back to `meters_config.json` when `blocks.db` exists. Previously a stale JSON file could override the DB config when swapping databases between environments.

- **Import rate zero across all gap fill blocks** — when only the export sensor fires before the first tick after a restore (the common case), `post_reads` has no import channel entry. `build_gap_blocks` hit the "missing reads" branch and hardcoded `rate=0.0`. Fixed: the missing-reads branch now uses `last_known_rates` as a fallback so the rate is preserved even when no post-read is available.

- **Unfinalised current_block window excluded from gap fill** — the gap anchor was derived from `pre_ts` (the last read timestamp), which put `detect_gap`'s `last_block_end` one block *after* the unfinalised current_block window, silently skipping it. Fixed by storing `last_block_start` in the gap marker (persisted to a new `gap_last_block_start` column on `current_block`) and using it as the anchor so the unfinalised window is always included in `missing_windows`.

- **Zero-rate/zero-kWh blocks from catch-up rollovers not overwritten** — after restore, `ensure_correct_block` fires during `engine_startup` and writes zero-read catch-up blocks via `INSERT OR IGNORE`. Gap fill then silently skipped them. Fixed: gap fill now uses `INSERT OR REPLACE` (`append_block_replace`) so catch-up zero blocks are overwritten with interpolated data.

- **Zero-rate blocks from catch-up rollovers contaminating last_known_rates** — `engine_startup` gap detection called `get_last_block()` which uses `MAX(block_start)`. After restore, catch-up rollovers fire during startup and write zero-rate blocks; by the time gap detection ran, `get_last_block()` returned one of those zero-rate blocks, infecting `last_known_rates` with `rate=0.0`. Fixed: gap detection now uses `get_last_block_before(current_block.start)` to always find the real last block from the restored DB.

- **`meters_config.json` appearing in restore UI and backup zips** — removed from all known sets in zip extraction endpoints, from backup creation, from restore sync logic, and marked "No longer needed" in the data management UI.

- **Stale `url_for('import_page')` references** — fixed in `charts.html`, `corrections.html`, `delete_blocks.html`, and `help.html`. Route was renamed to `data_management_page` in 2.6.0 but template references were not updated, causing 500 errors on those pages.

### Templates removed (old names no longer served)

- `config.html` — replaced by `meter_config.html`
- `summary.html` — replaced by `live_power.html`
- `import.html` — replaced by `data_management.html`
- `config_history.html` — replaced by `billing_history.html`

---

## [2.5.4] — 2026-04-19

### Added
- **Storage monitoring** — new Storage card on the Data Management page showing
  database size, disk free, growth rate (MB/day) and estimated runway. Runway is
  colour-coded green (>2 years), amber (6 months–2 years), red (<6 months). A disk
  usage bar shows used vs total with percentage free. Estimate is based on actual
  days of data recorded and assumes current growth rate continues.

### Fixed
- **Usage Stats floating toolbar** — `position: sticky` was removed in 2.5.2 when
  fixing billing chart landscape space (the two were unrelated). Restored with the
  correct `top: 0` offset now that `.content` has `padding: 0` on the Charts page.
  Works on both desktop and mobile.

---

## [2.5.3] — 2026-04-18

### Fixed
- **Usage Stats Y axis duplicate labels** — cost mode was using `.toFixed(0)` for axis
  labels so Chart.js ticks at 0.5 intervals (e.g. -1.0 and -1.5) both rendered as
  `-£1`, producing duplicate labels. Fixed to use 1 decimal place for cost labels.
  `maxTicksLimit: 8` and `grace: 2%` added to prevent over-generation of ticks and
  reduce excess headroom above/below the data.

- **Stale test timestamp** — `test_prune_removes_old_rows` in `test_block_store.py`
  used a hardcoded `2026-04-14` date which has now passed the 4-day pruning window,
  causing the test to fail. Fixed to use a relative timestamp (1 hour ago).

---

## [2.5.2] — 2026-04-17

### Fixed
- **Power history zoom rescales axes** — when zoomed, both the Y axis and X axis now
  rescale to the visible window. Y scale is computed only from data points within the
  zoom window. X axis uses a smart tick interval (1 min to 12 hr) chosen based on the
  zoom span, always anchors labels at the start and end of the window, and skips
  overlapping interior labels.

---

## [2.5.1] — 2026-04-17

### Added
- **Power history touch zoom** — the drag-to-zoom on the 48-hour power history chart
  now works on touchscreen devices. Drag horizontally to zoom, double-tap to reset.
  Uses `{ passive: false }` to prevent page scroll during chart interaction.

- **Mobile topbar collapse** — a drawer handle strip sits between the topbar and chart
  content on mobile and landscape. Shows the active tab name with chevrons. Tap to
  collapse the topbar entirely for maximum chart space, tap again to restore. State
  persists across sessions via localStorage.

### Fixed
- **Usage Stats toolbar over hamburger menu** — the floating toolbar `z-index` reduced
  to `9`, below the HA ingress chrome.

- **Orientation change leaves chart inconsistent** — rotating between portrait and
  landscape now forces a full reload of the active iframe chart after layout settles
  (600ms), eliminating the stale dimensions that required switching pages to recover.

- **Billing chart landscape real estate** — on mobile landscape the `period-nav` toolbar
  is now `position: static` so it scrolls off, giving the chart full viewport height.
  Portrait behaviour is unchanged.

- **Billing/heatmap chart fills available height** — charts now measure `.content`
  element height directly rather than using a hardcoded `window.innerHeight - 220px`
  offset, correctly filling the content area now that the topbar is sticky.

---

## [2.5.0] — 2026-04-16

### Added
- **Heatmap metric toggle** — the Net Energy Heatmap now has a floating toolbar with
  three metric modes: kWh (existing), gCO₂ (carbon flow per slot — red when emitting,
  green when offsetting), and gCO₂/kWh (grid carbon intensity — green→yellow→red).
  The toggle only appears when carbon data exists. Selected mode persists across
  sessions. The intensity colour scale is anchored to the 95th percentile of your
  recorded intensities so contrast is always meaningful as the grid decarbonises.

- **Usage Stats effective intensity column** — when in CO₂ mode, the data table gains
  an "Avg intensity" column showing the weighted average grid carbon intensity
  (gCO₂/kWh) for each period. Calculated as SUM(|carbon_g|) / SUM(|net_kwh|) across
  blocks with carbon data, matching the heatmap's per-slot intensity exactly when
  aggregated. The totals row shows the weighted average across the full period.

- **`avg_intensity` in `api/charts/blocks-summary`** — each day row now includes a
  weighted average grid intensity (gCO₂/kWh), enabling the consistency cross-check
  between the heatmap and Usage Stats.

- **Usage Stats auto-refresh** — the Usage Stats chart now re-fetches data if it is
  older than 5 minutes, rather than relying on a one-shot `barInited` flag. Switching
  tabs or navigating away and back within the TTL window still uses the cached data
  (no unnecessary fetches), but data will refresh at the same cadence as block finalise.

- **Power history drag zoom** — drag horizontally on the 48-hour power history chart
  to zoom into a specific time window. Double-click to reset to the full 48-hour view.
  Works alongside the existing hover tooltip.

- **Tab buttons in topbar** — the Billing / Heatmaps / Usage Stats tab buttons have
  moved from the content area into the topbar, freeing vertical space and following
  the same pattern as other pages (Meter Config, Data Management). The tab is now
  always visible without scrolling.

- **Floating toolbars** — Usage Stats and Heatmaps now have a sticky floating toolbar
  matching the billing chart's period-nav style (surface background, drop shadow,
  border-radius on bottom). The Usage Stats toolbar sticks to the top of the content
  area as you scroll through the data table. The Heatmap toolbar sticks within the
  heatmap scroll container. Both use CSS classes rather than inline styles for active
  state, matching the billing chart pattern exactly.

- **Fixed page scroll** — all pages (Meter Config, Charts, Live Power, Data Management,
  Help, Logs) now have a fixed topbar with only the content area below it scrolling.
  Previously the entire page including the topbar scrolled. The topbar is now always
  visible. This required `body: overflow: hidden`, `.main: height: 100vh`, and
  `.content: overflow-y: auto` in the base template.

### Fixed
- **Power history tests using stale timestamps** — `TestPowerHistory` and
  `TestApiPowerHistory` used hardcoded April 14 timestamps which fell outside the
  48-hour retention window as time passed, causing silent test failures. Fixed by
  using relative timestamps (now − N hours) that always stay within the window.

---

## [2.4.1] — 2026-04-16

### Fixed
- **Blue (house) bar negative on solar export days** — the house remainder carbon
  was computed as `main_carbon_g - sub_carbon` where `main_carbon_g` is the net
  figure (import − export) × intensity. On a net export day `main_carbon_g` is
  negative, making the remainder more negative still when sub-meter carbon is
  subtracted. Fixed: the remainder is now computed from `carbon_g_imp` (import
  carbon only, always positive) minus sub-meter carbon. The export offset is
  shown separately as a grey bar below zero.

- **`carbon_g_imp`/`carbon_g_exp` using wrong kWh values for intensity** — the
  back-calculated intensity used `main_imp_kwh`/`main_exp_kwh` from the billing
  summary which uses `imp_kwh_remainder` (house only, sub-meters stripped). But
  `carbon_g` in the engine is computed from the raw full-meter `imp_kwh`. This
  mismatch produced incorrect intensities. Fixed: raw block channel kWh are now
  accumulated directly from day_blocks for the main meter and used in the intensity
  calculation, matching what the engine used when computing `carbon_g`.

- **CO₂ totals chart showing incorrect bar heights** — the blue (house/grid) bar in
  Usage Stats CO₂ totals view was showing the full main meter net carbon rather than
  the house remainder (main minus sub-meters). This made the chart appear to
  triple-count consumption — the house bar included the EV and battery carbon which
  were then also shown separately as their own bars. Fixed: the blue bar now shows
  only the house remainder, matching exactly how kWh totals handles the main meter.

- **CO₂ totals stacked total double-counting sub-meters** — `carbon_g_total` in the
  API response was computed as `main_carbon_g + sub_carbon` rather than just
  `main_carbon_g`. Since `main_carbon_g` already includes sub-meter consumption
  (it is the full grid net carbon), adding sub-meter carbon again produced a total
  nearly double the correct figure. Fixed: `carbon_g_total = main_carbon_g`.

- **Mixed CO₂ units within a single chart render** — the CO₂ formatter was scaling
  each value independently (gCO₂ or kgCO₂ based on its own magnitude), so different
  bars, the y-axis and the data table could show different units for the same render.
  A day with large EV charging would show the EV bar as "1.710 kgCO₂" while the
  battery showed "875 gCO₂" making them appear inconsistent. Fixed: the unit is now
  determined once per render from the largest value in the dataset and applied
  consistently to all labels, axis ticks and table values.

---

## [2.4.0] — 2026-04-16

### Added
- **Carbon footprint in Usage Stats** — a third CO₂ metric alongside kWh and Cost in
  the Usage Stats chart. Shows net carbon (gCO₂) in net view and import/export carbon
  split in totals view, mirroring the kWh breakdown. Auto-scales between gCO₂ and kgCO₂.
  Only visible when a postcode prefix is configured. Pre-2.3.0 blocks show `—` in the
  data table (no CI data available). The CO₂ toggle is hidden when no postcode is set.

- **`carbon_g` in block dicts** — `_row_to_block` now includes `carbon_g` when
  reconstructing blocks from the database, making it available to the API layer.
  Previously `carbon_g` was stored in the DB but never surfaced in block dicts.

- **`has_postcode` in `/api/charts/blocks-summary`** — the response now includes a
  `has_postcode` boolean so the UI can conditionally show the CO₂ metric toggle.

- **`carbon_g_imp` / `carbon_g_exp` split on main meter** — Usage Stats totals mode
  shows import carbon above the zero line and export carbon offset below, matching the
  kWh totals view. Back-calculated from average intensity implied by the net `carbon_g`.

### Fixed
- **DB restore silently ignoring `blocks.db`** — the `/api/import` endpoint accepted
  `blocks.db` uploads but never wrote the file to disk — only `meters_config.json` was
  processed. The field was missing from `file_map`. Restoring from a zip therefore always
  left the old database in place with no error reported.

- **Restore failing with "malformed" error** — when restoring `blocks.db` via either
  `/api/backup/restore` or `/api/import/from-zip`, the engine was never paused before
  the file was overwritten. The engine's open WAL connection remained active, causing
  SQLite to declare the newly written file "malformed" on next open. The corruption
  recovery handler then renamed the restored DB to `.corrupt` and created a fresh
  empty one — silently destroying the restore. Fixed by pausing the engine, flushing
  the WAL, closing all store connections, and removing WAL/SHM files before writing
  the restored file.

- **`meters_config.json` overwriting restored DB config** — when a backup zip (which
  contains both `blocks.db` and `meters_config.json`) was restored, the sync logic
  pushed the flat JSON file back into the DB, overwriting the restored DB's full config
  period history with a single-period snapshot. Fixed: when `blocks.db` is restored,
  the DB is always authoritative — `meters_config.json` is written from the DB, never
  the reverse.

- **Engine store not reset after restore** — the web server's `_store` was set to `None`
  after restore but the engine's own `_store` was not. On the next engine tick it continued
  using the old connection, seeing the old data. Fixed by calling `engine.reset_store()`
  which closes and reopens the engine's connection from the new DB file.

### Changed
- **Zip restore flow** — dropping a backup zip on the Data Management page now shows a
  confirmation modal with the filename, size, and an optional "back up current data first"
  checkbox (checked by default) before proceeding. The restore is handled entirely
  server-side via `/api/import/from-zip` — no base64 round-trip through the browser,
  which previously corrupted binary DB files.

- **No restart required after restore** — the engine is paused, the DB replaced, and
  the engine store reset and resumed without requiring an add-on restart. The page
  reloads automatically after 2.5 seconds.

### Hardening
- **WAL checkpoint before backup** — `_create_backup_zip` now uses the engine's store
  connection and runs `PRAGMA wal_checkpoint(TRUNCATE)` before copying. This ensures
  the backup captures a fully consistent snapshot. In practice SQLite's auto-checkpoint
  means recent data is almost always already in the main file, but this makes it
  explicit and guaranteed.

- **WAL checkpoint on every startup** — `PRAGMA wal_checkpoint(TRUNCATE)` runs at the
  start of `engine_startup` before any new blocks are written. Cheap and ensures the
  DB is always fully flushed after any unclean shutdown.

- **Upgrade safety backup** — on the first startup after a version change, the engine
  creates a `*_upgrade_2.4.0.zip` in the backup list before recording any new data.
  Runs once per version by comparing against a `.last_startup_backup_version` marker
  file. Provides a complete snapshot at the point of upgrade as a precautionary
  safety net.

---

## [2.3.0] — 2026-04-14

### Added
- **Carbon intensity recording** — the engine now fetches carbon intensity data
  from the National Grid API every 15 minutes (UK only, requires postcode prefix
  in Meter Config). Data is stored in a new `carbon_intensity` table with a 4-day
  rolling retention window. Fails silently if no postcode is configured; logs a
  warning if postcode is configured but the fetch fails.

- **`carbon_g` on blocks** — net carbon footprint (gCO₂) is now computed at block
  finalise time for every meter using the nearest stored carbon intensity value.
  Formula: main meter `(imp_kwh - exp_kwh) × intensity`, sub-meters
  `imp_kwh × intensity`. NULL if no carbon intensity data is available. This is the
  foundation for carbon footprint reporting in future releases.

- **48-hour power history chart** — a new rolling chart on the Live Power page
  shows net grid power (kW) at ~10-second resolution for the past 48 hours.
  Import is shown in red, export in green, with fill regions and a per-segment
  colour line. The dashed "now" marker sits at the right edge.

- **kW / CO₂ toggle** — the power history chart can be switched between kW and
  carbon rate (gCO₂/min) views. The selected mode is remembered across page visits
  via `localStorage`. The toggle only appears when a postcode prefix is configured.
  In CO₂ mode, red indicates emitting (net import) and green indicates offsetting
  (net export), using the same colour language as the kW view.

- **Hover tooltip on power history chart** — hovering over the chart snaps to the
  nearest data point and shows local time, net kW or gCO₂/min (depending on mode),
  and the current grid carbon intensity (gCO₂/kWh). The tooltip follows the mouse
  and flips to the left side when near the right edge.

- **`/api/power/history`** — returns the rolling 48-hour power and carbon history.
  Accepts optional `?hours=N` parameter (max 48).

- **`/api/carbon/current`** — returns the current carbon intensity from the stored
  `carbon_intensity` table (faster than a live API call). Falls back to 404 if no
  data is available.

### Technical
- `BlockStore` gains `upsert_carbon_intensity`, `get_nearest_carbon_intensity`,
  `prune_carbon_intensity`, `append_power_history`, `get_power_history`, and
  `prune_power_history` methods.
- `_ensure_schema` migration adds `carbon_g` column to existing `blocks` tables and
  `carbon_gco2_min` column to `power_history`.
- Carbon rate (`carbon_gco2_min`) is derived from the power sensor reading directly
  (`net_kw × intensity ÷ 60`) rather than from meter read deltas, which would
  produce zeros and spikes due to the sensor's ~60-second update cadence vs the
  engine's ~10-second tick rate.
- `power_history` has 48-hour rolling retention pruned on every engine tick.
- `carbon_intensity` has 4-day rolling retention pruned on every fetch cycle.

---

## [2.2.3] — 2026-04-12

### Changed
- **blocks.json import removed from UI** — dragging a `blocks.json` file into the
  Import page is no longer supported. The manual upgrade path (placing `blocks.json`
  on disk and letting the engine migrate it on startup) still works. Full removal
  of `migrate_json_to_sqlite()` is scheduled for 3.0.0.

---

## [2.2.2] — 2026-04-11

### Fixed
- **Billing totals kWh double-counting** — `get_billing_totals_for_local_date_range`
  and `get_cumulative_totals` both fell back to raw `imp_kwh` for sub-meter blocks
  where `imp_kwh_grid` was NULL, double-counting consumption already included in the
  main meter total. Fixed to use `COALESCE(imp_kwh_grid, 0)` in both methods.

- **Usage Stats calendar/billing toggle** — switching between billing and calendar
  modes in daily view left the table stale and caused an empty chart on switching
  back. Fixed by correctly handling `barDailyMonth` as either a string (billing mode)
  or object (calendar mode) in `barGetDataForPeriod` and `barBuildSubBar`.

---

## [2.2.1] — 2026-04-11

### Fixed
- **Billing totals incorrect kWh** — `get_billing_totals_for_local_date_range`
  was falling back to raw `imp_kwh` for sub-meter blocks where `imp_kwh_grid` was
  NULL, double-counting consumption already included in the main meter total. Fixed
  to use `COALESCE(imp_kwh_grid, 0)` — NULL means no recorded grid import, not
  missing data. Affected Today / This Bill / This Year totals on the Live Power cards.

- **Standing charge incorrect on Live Power cards** — `get_billing_totals_for_local_date_range`
  used `MIN(standing_charge)` per day, which picked 0 for days where the standing
  charge sensor hadn't updated for the first block. Fixed to use `MAX(standing_charge)`
  per day, matching the billing chart which takes the highest (correct) value recorded
  for that day.

---

## [2.2.0] — 2026-04-11

### Added
- **Bill summary redesign** — the Import section now shows total grid draw at the top,
  matching what your supplier bills, with sub-meter breakdown (House remainder, EV charger,
  Battery) indented beneath. Previously only the remainder was shown as "Import", making
  supplier reconciliation require manual summing across sections.

- **Delete Blocks** — new sub-page under Data Management (`/delete-blocks`) to permanently
  delete blocks for a date range, optionally filtered to a single meter. Shows a block and
  day count preview before requiring explicit confirmation. Cannot be undone.

- **Historical Corrections** promoted to own page — moved from the Data Management page to
  its own sub-page (`/corrections`) following the same pattern as Billing History under
  Meter Config. Data Management topbar now has direct links to both sub-pages.

- **Compact Database** — "Compact Now" button on the Data Management page runs `VACUUM`
  on the blocks database. The engine is paused briefly during the operation to ensure
  exclusive access. Reports size before and after so you can see how much space was
  reclaimed. Most useful after bulk deletions; at ~40 KB/day growth it is rarely urgent.

- **Lovelace-friendly chart endpoints** — `/lovelace/billing` and `/lovelace/heatmap`
  serve the chart HTML with a 130-second meta refresh and aggressive no-cache headers
  baked in at serve time. Use these URLs in Lovelace webpage cards instead of the raw
  `/charts/*.html` URLs — they refresh reliably and never get stuck in the browser cache.
  Documented in the Help page.

### Fixed
- Date inputs in Delete Blocks and Historical Corrections now show `dd/mm/yyyy` format
  hint in labels and use `lang="en-GB"` to encourage day-first display in supporting
  browsers.

- **Chart flicker removed** — `<meta http-equiv="refresh">` removed from generated chart
  HTML. The EMT charts page handles refresh cleanly via `setInterval` without reloading
  the iframe. Lovelace users should use the dedicated `/lovelace/*` endpoints which have
  the meta refresh injected at serve time.

---

## [2.1.9] — 2026-04-07

### Fixed
- **Billing chart not auto-refreshing** — the 2-minute auto-refresh timer used
  `textContent.indexOf('Daily')` to identify the active tab, which stopped working
  when the billing tab was renamed from "Daily Usage" to "Billing" in 1.6.0. With
  a third "Usage Stats" tab also present, the fallback logic refreshed the heatmap
  instead of the billing chart when Usage Stats was active, leaving the billing chart
  stale until a hard refresh. Fixed by adding `data-chart` attributes to tab buttons
  and clearing all `chartsLoaded` state on every timer tick so any tab switch always
  fetches fresh data.

- **`api/charts/daily` and `api/charts/heatmap` missing cache headers** — the JSON
  endpoints serving chart HTML had no `Cache-Control` headers, allowing some browsers
  and the HA ingress proxy to cache responses. Added `no-cache, no-store,
  must-revalidate` headers to both endpoints.

---

## [2.1.8] — 2026-04-07

### Fixed
- **Chart regeneration on every gap-fill block** — when the engine restarts after a
  long offline period, charts were regenerated once per missing block. A 6-hour gap
  with 5-minute blocks triggers 72 chart regeneration cycles, each taking ~7 seconds,
  causing minutes of CPU load before the engine catches up. Fixed by skipping chart
  regeneration for interpolated (gap-fill) blocks — charts are regenerated once at
  startup and again on the first live block.

- **Gap-fill blocks showing zero rate and cost** — after the `extract_last_reads`
  fix in this release, `last_known_rates` entries became `{"ts":..., "value":...}`
  dicts instead of raw floats. The gap-fill rate lookup expected a float, so rate
  and cost were silently zeroed for all interpolated blocks during catch-up.
  Fixed by adding a `_rate_value()` helper that unwraps either format.

- **Crash on startup with session gap: `AttributeError: 'float' object has no attribute 'get'`** —
  `extract_last_reads()` stored the last known rate as a raw float in `last_known_rates`.
  `save_current_block()` expects `{"ts": ..., "value": ...}` dicts and calls `r.get("ts")`
  on each entry — crashing when it encounters a float. This path is triggered when the
  engine restarts after a session gap and the last known rates come from a finalised block
  (which stores rate directly on the channel, not in a rates list). Fixed by always
  returning rates as `{"ts": ..., "value": ...}` dicts from `extract_last_reads()`.

- **Gap fill using first post-outage read instead of latest** — when the engine
  restarts after a gap, `post_reads` was populated with `reads[0]` (the first
  sensor capture after restart). If the sensor updated multiple times before gap
  fill triggered, the interpolation endpoint was stale — later sensor values were
  ignored. Fixed by using `reads[-1]` (the most recent read) as the post-gap
  anchor, giving the most accurate interpolation endpoint available.

- **Gap fill not running after session outage** — `extract_last_reads()` was
  called on the last finalised block (from DB via `_row_to_block`) which stores
  sensor values as `read_end` floats with no timestamps, not as `{"ts", "value"}`
  dicts. The gap fill anchor `pre_ts` was therefore always `None`, causing
  `detect_gap()` to return no missing windows and silently skip gap fill entirely.
  Fixed by using `read_end` and the block's `end` timestamp when extracting reads
  from finalised DB blocks, giving `detect_gap` a valid anchor.

- **False large kWh spike on sub-meters after add-on restart** — when the engine
  restarts after a session gap (e.g. upgrade, HA restart), the current in-progress
  block retains stale channel reads from before the restart. Sub-meters that use
  cumulative sensors produce a delta spanning the entire offline period, resulting
  in a false import spike on the first post-restart block.

  The main meter is unaffected — it uses boundary interpolation from a precise
  pair of reads around the block start/end. Sub-meters accumulate all reads
  directly, so the stale pre-restart read was included in the delta calculation.

  Fixed by clearing the current block's channel reads on startup when a session
  gap is detected. The gap marker's `pre_reads` correctly captures the pre-gap
  values for gap interpolation; live reads accumulate fresh from the first
  post-restart sensor capture.

---

## [2.1.7] — 2026-04-06

### Fixed
- **Standing charge corrections updating sub-meter rows** — the correction
  query filtered only by `local_date`, so it updated all meter rows including
  `ev_charger` and `house_battery` which should always have `standing_charge = 0`.
  This caused the preview to show `current_min = 0.0` (from sub-meter rows) instead
  of the real standing charge, and the apply wrote incorrect values to sub-meter rows.
  Fixed by restricting standing charge corrections to main meter rows only via a
  subquery on the `meters` table (`is_sub_meter = 0`).

- **Billing chart and Usage Stats colours out of sync** — both charts now use
  `build_meter_colors_from_config(cfg)` so sub-meters always get the same colour
  in both views. Previously the billing chart built its colour map from the first
  day of block data, which could assign different indices to sub-meters added after
  data collection began, causing colour mismatches between charts.

- **Startup crash on pre-2.1.6 databases: `no such column: m.v2x_capable`** —
  `get_last_block()` selects `m.v2x_capable` explicitly, but on older databases
  this column doesn't exist until `migrate_full_config_json()` runs — which is
  too late. Fixed by moving all incremental column additions into `_ensure_schema()`
  so they run at `open_block_store()` time, before any query.

- **`supplier` and `v2x_capable` meta fields silently dropped on config save** —
  `_write_meters` only persisted a subset of meter meta fields; both fields were
  lost on every config save.

  `supplier` is now a column on `config_periods` (not `meters`) giving it a full
  historical record — if you change supplier you create a new config period, just
  like changing billing day, and historical blocks retain a reference to the supplier
  that was active when they were recorded.

  `v2x_capable` is a column on `meters` (correct — it is a per-meter property, not
  billing-period-specific).

  Existing databases are upgraded automatically on startup. Users who had configured
  a V2G-capable meter will need to re-save their meter config once to restore the
  `v2x_capable` flag. The `supplier` field can be set via Edit on the Billing History
  page for any config period where it matters.

---

## [2.1.5] — 2026-04-06

### Fixed
- **Live Power Today / This Bill / This Year cards showing inflated values for
  sub-meter installations** — `get_billing_totals_for_local_date_range()` did
  `SUM(imp_kwh)` across all meters with no sub-meter filter, double-counting
  sub-meter consumption already included in `electricity_main.imp_kwh`. On a
  system with an EV charger and battery, Today showed ~79% more kWh and cost
  than actual grid import. Fixed by applying the same PASS 3 logic as
  `get_cumulative_totals`: main meter uses `imp_kwh_remainder`, sub-meters use
  `imp_kwh_grid`, cost/export/standing from main meter only. Standing charge
  query also restricted to main meter rows to prevent duplication.

---

## [2.1.4] — 2026-04-06

### Fixed
- **Sub-meters added after the first block date missing from Usage Stats** —
  `build_meter_colors` sampled only the first day of blocks to determine which
  meters to plot. Sub-meters that were added to the config after data collection
  began (e.g. a battery added weeks after the EV charger) had no blocks on the
  first day and were silently excluded from Usage Stats charts and data table.
  Fixed by replacing the block-sample approach with `build_meter_colors_from_config`
  which builds the colour map directly from the config dict, guaranteeing all
  configured meters are represented regardless of when they first recorded data.

---

## [2.1.3] — 2026-04-06

### Fixed
- **Sub-meter flags missing from reconstructed block dicts** — `_row_to_block` built
  `meter.meta` from `config_periods` columns only, never setting `sub_meter`,
  `parent_meter`, or `device`. Charts, Live Power and Usage Stats relied on
  `meta.sub_meter` to identify sub-meters; without it all meters appeared as main
  meters, sub-meters were not plotted separately, and billing calculations were wrong.
  Fixed by joining the `meters` table in `_select_blocks` and `get_last_block` and
  populating the full meta from the joined columns.

- **`get_cumulative_totals()` double-counting sub-meter consumption** — the four HA
  sensors (import kWh, export kWh, import cost, export cost) were incorrectly inflated
  for installations with sub-meters. `electricity_main.imp_kwh` already includes
  sub-meter consumption; the previous implementation added sub-meter `imp_kwh` a
  second time. Fix mirrors engine PASS 3 logic — main meter uses `imp_kwh_remainder`,
  sub-meters use `imp_kwh_grid`, cost and export from main meter only.
  Historical block data unaffected. Users without sub-meters unaffected.

### Also includes
- All 2.1.0 changes (fully relational DB, JSON file elimination, normalised schema)
- Data Management page (renamed from Import & Backup)
- Enhanced Historical Corrections (time-of-day window, per-meter targeting, per-block preview)

> 2.1.1 and 2.1.2 were briefly live during a difficult release cycle and have been
> superseded by this release. If you are on 2.1.1 or 2.1.2 please update immediately.

---

## [2.1.2] — 2026-04-06

### Fixed
- Re-release of 2.1.1 fixes under a new version number. 2.1.1 was briefly
  live before being rolled back, leaving some users on 2.1.1 with the broken
  build. 2.1.2 ensures all users receive the corrected version.
  See 2.1.1 release notes for the full list of fixes.

---

## [2.1.1] — 2026-04-06

### Fixed
- **Sub-meter flags missing from reconstructed block dicts** — `_row_to_block` built
  `meter.meta` from `config_periods` columns only, never setting `sub_meter`,
  `parent_meter`, or `device`. Charts and billing relied on `meta.sub_meter` to
  identify sub-meters; without it all meters appeared as main meters, sub-meters were
  not plotted separately, and billing calculations were wrong. Fixed by joining the
  `meters` table in `_select_blocks` and `get_last_block` and populating the full meta
  from the joined columns.

- **`get_cumulative_totals()` double-counting sub-meter consumption** — the four HA
  sensors (import kWh, export kWh, import cost, export cost) were incorrectly inflated
  for installations with sub-meters (EV charger, battery etc).

  `electricity_main.imp_kwh` already includes sub-meter consumption. The previous
  implementation did `SELECT SUM(imp_kwh) FROM blocks` across all meters, which added
  sub-meter `imp_kwh` a second time. On a system with an EV charger and battery this
  produced import sensor readings roughly 67% higher than actual grid import.

  The fix mirrors the engine's PASS 3 finalise logic:
  - Main meter: uses `imp_kwh_remainder` (house-only grid load after sub-meters),
    falling back to `imp_kwh` when no sub-meters are configured
  - Sub-meters: uses `imp_kwh_grid` (the portion drawn from the grid rather than
    from solar/battery), falling back to `imp_kwh`
  - Cost and export figures: main meter only

  **Historical block data is unaffected** — the blocks table, billing charts, and
  per-block calculations were correct throughout. Only the HA sensor values
  published after each block finalise were wrong.

  Users without sub-meters are unaffected.

---

## [2.1.0] — 2026-04-06

### Changed (breaking — upgrade path is fully automatic)
- **`energy_meter.db` is now the only file that matters** — it is the single source of truth for all state; backup and restore requires only this one file
- **`cumulative_totals.json` eliminated** — lifetime totals derived via `SELECT SUM(...)` on the blocks table; file silently ignored on startup
- **`current_block.json` eliminated** — in-progress block state now stored in the new `current_block` and `current_reads` tables; migrated automatically on first 2.1.0 startup and renamed `.migrated`
- **`meters_config.json` is a convenience export only** — written on every config save for human readability, never read back as live state
- **Config is fully normalised** — `full_config_json` blob removed from `config_periods`; meter definitions live in the `meters` and `meter_channels` tables; `gap_marker` blob removed from `current_block` and replaced with `gap_detected_at` column and `is_gap_seed` rows in `current_reads`; `mpan` and `tariff` promoted to proper columns on `meter_channels`

### Added
- `meters` table — fully populated: one row per meter per config period, with all sensor entity IDs, sub-meter flags, and optional fields
- `meter_channels` table — per-channel sensor config (`read_sensor`, `rate_sensor`, `standing_charge_sensor`, `mpan`, `tariff`)
- `current_block` table — single-row in-progress block state (`block_start`, `block_end`, `last_checkpoint`, `gap_detected_at`)
- `current_reads` table — rolling reads/rates buffer with `is_gap_seed` column (0=live, 1=gap seed kWh, 2=gap seed rate)
- `BlockStore.config_from_db(period_id)` — reconstructs full config dict by joining normalised tables; no JSON parsing
- `BlockStore._write_meters(config, period_id)` — upserts meter and channel rows from a config dict
- `BlockStore.save_current_block()` / `load_current_block()` / `clear_current_block()` — DB persistence for in-progress block state
- `BlockStore.get_cumulative_totals()` — single SQL aggregation replacing `cumulative_totals.json`
- `BlockStore.migrate_full_config_json()` — automatic 2.0→2.1 upgrade: populates normalised tables from `full_config_json` blobs, migrates `gap_marker` blob, adds missing columns; safe to call on every startup; idempotent
- **Historical Corrections enhanced** — rate corrections now support time-of-day window, per-meter targeting, and per-block preview table
- Import & Backup page — file reference table and restore UI reflect single-file model

### Removed
- `full_config_json TEXT` column from `config_periods`
- `gap_marker TEXT` blob column from `current_block`
- `meter_channel_meta` key/value table
- `cumulative_totals.json`, `current_block.json`, `meters_config.json` as authoritative state files

---

## [2.0.1] — 2026-04-05

### Added
- **Historical Corrections** — new section on the Import & Backup page; bulk-update standing charge or import/export rates across a local date range in the live database; Preview shows affected block and day counts plus current value range before committing; rate corrections optionally recalculate cost from corrected rate × kWh

### Fixed
- **kWh and cost alignment between Billing chart and Usage Stats**
- **Standing charge double-counted in Usage Stats (BST days)**
- **Standing charge not shown in Usage Stats**
- **Usage Stats blocks fetched by local_date**
- **Billing period computation in Usage Stats**

---

## [2.0.0] — 2026-04-05

### Added
- **SQLite database** — all blocks now stored in a SQLite database (`energy_meter.db`) replacing the `blocks.json` flat file
- **Config history (Billing History)** — every billing-significant config change is recorded as a new config period
- **Billing History page** — accessible via the 🕓 Billing History button on the Meter Config page
- **Billing period transition logic** — truncation-only model; bills can only be truncated, never extended
- **Usage Stats — billing period navigator**
- **Live Power — billing-accurate "This Bill"**
- **Fast SQL billing aggregation**
- **Gauge scale cache** — 7-day percentile cached for 30 minutes

### Changed
- **Live Power page loads instantly** — billing card data fetched asynchronously
- **Billing History removed from sidebar nav**
- **Config period removal** — any period can be removed when 2+ periods exist

---

## [1.6.3] — 2026-04-01

### Fixed
- Heatmap mobile portrait — chart fills full viewport
- Heatmap mobile pinch zoom
- Heatmap scroll-guard strip overlapping totals bar
- Usage Stats width unconstrained on mobile portrait

---

## [1.6.2] — 2026-04-01

### Added
- Usage Stats — Billing/Calendar period toggle
- Usage Stats data table — totals column
- Usage Stats data table — period labels
- Global light/dark theme toggle

### Fixed
- Usage Stats export cost positive in data table
- Usage Stats meter labels include site name
- Various light/dark mode and heatmap fixes

---

## [1.6.0] — 2026-04-01

### Added
- **Usage Stats chart** — daily, monthly and yearly import/export with sub-meter breakdown
- **Data table** — tabular view with copy-to-clipboard export
- **Light/dark theme toggle**

### Changed
- Summary page renamed to Live Power
- Remember last visited page
- Chart tabs renamed — Daily Usage → Billing, Import / Export → Usage Stats

---

## [1.5.1] — 2026-03-26

### Added
- Live Power page — gauge, billing cards, carbon intensity forecast
- Power sensor and postcode prefix fields in Meter Config

---

## [1.4.0] — 2026-02-10

### Added
- Configurable meter reconciliation period (5, 15 or 30 minutes)
- Automatic currency detection

---

## [1.3.x] — 2025-12-01

### Fixed
- Timezone-aware chart rendering; UTC timestamp bugs; silent sensor timeout; standing charge billing display

---

## [1.2.0] — 2025-10-15

### Added
- Guided Setup Wizard

---

## [1.1.0] — 2025-09-01

### Added
- Flask-based web UI

---

## [1.0.0] — 2025-08-01

Initial release. Core metering engine, sub-meter support, gap filling, billing charts, HA sensor publishing.