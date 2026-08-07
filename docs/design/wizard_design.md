# EMT V3 Setup Wizard — Survey + BCD Walkthrough Design

> _Status: Shipped — design note kept for rationale; the code is the source of truth._

Status: DESIGN AGREED, build next turn. Foundation (mode storage, routes, Mini
gating, credential file store, base survey/credentials steps) already built &
tested (813 green). This document refines the wizard flow per the V3 refinement
matrix and the BCD-seeding decisions.

## BUILT — settlement-source step (this cycle)
The wizard now offers the billing **settlement source** (`dcc`/`cad`) as a step,
added by `computeWizSteps()` only for API modes (`mode.indexOf('api') !== -1`),
placed just before `done`. It defaults to `dcc`, pre-fills the current value from
`GET /api/billing-source` (for "Change Setup" re-runs), and on completion
`wizApplyAndClose()` POSTs the choice to `/api/billing-source` for API modes only
(idempotent server-side — default `dcc` is a no-op; `cad` = "use local readings,
never reconcile against DCC"). The Data Management control is *kept* (mirror, not
move) since changing billing philosophy later is legitimate — that's what the
recalculation path supports. Surfacing it at setup makes the common choice free
(no history to recalculate at first run).

## BUILT — Change-Setup confirmation (B2)
Finishing the wizard intercepts with a confirmation modal when (and only when) it
will recalculate: a Change-Setup re-run over an EXISTING config where the
data-source mode or — in API modes — the billing source actually changed
(`_wizWillRecompute`). Fresh first-time setups skip it (no history). Wording:
"large recalculation … can't be stopped once it starts, but it's reversible —
switch back afterwards." Mirrored on the Data Management billing-source Apply.

## BUILT — credential keep-or-replace on Change Setup
`startWizard` fetches `GET /api/kraken-config` (which returns `configured` +
`account_number` but NEVER the key — write-only by design). On a re-run the
credentials step shows the key field as "✓ on file — leave blank to keep, or
enter a new one to replace," pre-fills the (non-secret) account number, and
allows advancing on a blank field. `_wizVerifyCredentialsThenAdvance` SKIPS the
POST when the field is blank and a key is on file — an empty `api_key` would
CLEAR the stored credential — and advances using the saved key. Secret never
re-enters the DOM.

## The mode matrix (from V3 Refinement, authoritative)

Modes collapse by "what job each source does":
- CAD = local live block derivation (10s reads, PASS 2 at write).
- Mini = local live block derivation via register reads (lower res, same job).
- REST API = settled correction (≈ a day delayed, authoritative DCC).

Five → effectively four user-selectable, with api+mini resolved at RUNTIME:

| Mode      | Local feed | API | Mini | Notes |
|-----------|-----------|-----|------|-------|
| cad       | yes (CAD) | no  | n/a  | 2.x behaviour unchanged |
| cad+api   | yes (CAD) | yes | n/a  | CAD live; API settles. CAD kept in imp_kwh_cad |
| api       | no        | yes | no   | DCC is import source; sub-meters valid |
| api+mini  | no        | yes | yes  | Mini live provisional; DCC settles |

CAD+API+Mini is OUT OF SCOPE (Mini cross-check is a future diagnostic).

## KEY SIMPLIFICATION (this turn's decision): Mini is NOT a survey question

Mini is used automatically iff (no local CAD feed) AND (a Mini device is
discovered). The user never picks api+mini — the system resolves it:
- Survey derives only: cad / cad+api / api.
- At runtime _maybe_setup_mini() already gates on: no local import sensor AND
  device present AND mode uses api. So if survey yields `api` and a Mini is
  discovered, Mini activates. If no Mini, it stays plain api. Silent, automatic.
- CONSEQUENCE: the stored mode can be just `api`; "api+mini" need not be a
  stored distinction at all. Mini presence is a runtime fact from discovery.
  (Keep the api+mini constant for internal/logging use, but the survey writes
  `api` when no local + api, and Mini elevates behaviour at runtime.)
- ACTION: remove the Mini question + wizDeriveMode's hm branch from the base
  survey already built. Survey becomes a clean 2×2.

### Revised mode derivation (2×2)
- hasLocal=Y, hasApi=N → cad
- hasLocal=Y, hasApi=Y → cad+api
- hasLocal=N, hasApi=Y → api   (Mini auto-elevates at runtime if present)
- hasLocal=N, hasApi=N → ERROR (no data source; block advance)

## BCD detection (BottlecapDave Octopus integration in HA)

BCD provides rate, standing-charge, and (API-derived) consumption sensors, and
exposes the account number. `detect_bottlecapdave()` exists in
kraken_api_client.py (pure fn over HA states) and is now WIRED at startup via
`_detect_and_log_integrations`, which runs detection each engine start and emits
the `config-state: detection bcd=… bcd_live_power=… ohme_charge_mode=…` log line.
`detect_bottlecapdave` already surfaces the `current_demand` live-power sensor
(`demand_sensor`), and the setup wizard already pre-fills the Live Power field
with it — so the BCD live-power offload is largely in place. The unbuilt BCD
piece is the dispatch-feed offload (`intelligent_dispatching`), low priority.
A sibling `detect_ohme_charge_mode()` was added for the OHME verified path. What
remains UI-side is the explicit wizard pre-fill of rate/SC sensor fields from BCD
(the seeding flow below).

### BCD IS OPTIONAL — NOT A DEPENDENCY (critical)
BCD is ONE way to get Octopus data into HA. A user can have a PURE API setup
with NO BCD installed: they give EMT the API key directly and EMT's own Kraken
REST client + rate schedules (kraken_rates.py) provide settlement, rates and
standing charges. Pure-API-no-BCD is a FIRST-CLASS path, not degraded — it is
arguably the cleaner setup and is the entire reason the Kraken client exists.

BCD detection is therefore PURELY OPPORTUNISTIC PRE-FILL: "you happen to have
BCD, so I can pre-fill rate sensors + account to save typing." If BCD is absent,
NOTHING is lost — Kraken discovery + rate schedules supply the same data.

hasApi=true can come from EITHER BCD detection OR the user simply answering
"yes" + entering a key. BCD presence pre-ANSWERS it; BCD absence → user answers
it. Identical outcome, identical mode.

### Rate sourcing fork (the real difference BCD makes)
- BCD present  → rates come from BCD HA sensors → pre-fill rate/SC sensor fields.
- BCD absent   → rates come from EMT's Kraken rate schedules (build_rate_schedule
                 from REST). Rate sensor fields may be LEFT EMPTY — the schedule
                 supplies the rate at settlement/reconcile.

### Rate-population rule, REFINED (supersedes earlier "API mode must populate
### rates")
- CAD mode: a missing rate sensor = zero-cost blocks = real misconfiguration.
  (Runtime zero-rate backstop warning applies — already built.)
- API mode: "no rate sensor configured" is ACCEPTABLE. Kraken rate schedules are
  the backstop and fill rates at reconcile. The zero-rate runtime warning should
  NOT fire as a problem in API mode where a Kraken schedule will resolve it.
  (Check: ensure the backstop doesn't nag in pure-API mode. The reconcile-time
  rate repair in _rerun_pass2_for_settled_block already fills zero/missing rates
  from the schedule, so by the time a block is settled the rate is non-zero.)

### What BCD proves / does NOT prove
- PROVES: a supplier API relationship exists (Octopus) → pre-set hasApi=true.
- PROVES: account number (pre-fill), rate & standing-charge sensor entity IDs.
- Does NOT prove a live CAD feed (BCD consumption is API-derived, ~day delayed,
  NOT a live CAD source). So hasLocal still MUST be asked.

### Seeding decision (option 1) — only when BCD detected
On BCD detected (else skip entirely, pure-API path):
- pre-set wiz.data.hasApi = true (still SHOWN in survey, confirmable).
- capture account number → wiz.data.accountNumber.
- capture rate / standing-charge sensor entity IDs → pre-fill the sensors step.
- still ASK hasLocal (CAD) in the survey. Do NOT ask about Mini (auto).
On BCD NOT detected:
- survey asks hasApi normally; if yes, credentials step collects key; Kraken
  discovery + schedules provide rates. Rate sensor fields left empty = fine.

### When it fires
At wizard start: one GET /api/detect-bcd call. If found, seed wiz.data before
first render. If not found, proceed with empty seed (pure-API path). Either way
the wizard works. (Not on page load; not at survey→next.)

## POWER SOURCE RESOLUTION — Overview gauge (decided)

How the Overview page's live-power source is chosen. Resolution order, evaluated
at setup and honoured at runtime:

1. **User supplies a power sensor** → use it (`power_sensor` = that HA entity_id).
   The primary, normal case — a device sensor (Glow/CAD, inverter, CT-clamp, etc.).
2. **No user sensor, BCD present** → silently adopt BCD's `current_demand` as the
   meter power sensor (`power_sensor` = that entity_id). This is the existing
   live-power prefill made AUTOMATIC when the user didn't supply one. It's free
   (an HA entity, no EMT polling), so there's no decision to surface. The Mini is
   NOT offered in this case.
3. **No user sensor, no BCD, Mini present** → offer "use your Octopus Mini for
   live power? (uses API quota)", default **OFF** (opt-in, because it costs
   GraphQL quota). NOT asked in the wizard — surfaced on the **Overview page**
   the first time the engine confirms a Mini with no power sensor and no BCD.
   Rationale: Mini presence isn't known at wizard time (it's confirmed at engine
   runtime via the API), so the option is offered where/when the fact is actually
   known rather than asking about something that may not exist.
4. **None of the above** → no live source; Overview leans on postcode/billing and
   the dismissible "add a power sensor" hint.

Representation notes:
- Case 1 is the configured `power_sensor` entity. Case 2 (BCD) is adopted at
  RUNTIME, not written to config: a `_effective_power_sensor(meta)` helper
  resolves `power_sensor or _bcd_demand_sensor()` (BCD's `current_demand` from
  detection), so an existing no-sensor config picks BCD up without a config edit.
  Both make `has_power_sensor` true (Overview gauge + nav light up).
- The Mini (case 3) is NOT an HA entity — it's the GraphQL `smartMeterTelemetry`
  feed — so it can't be a `power_sensor` entity_id. It's a distinct marker
  `power_source: mini` on the main meter meta. The reveal gate (context
  processor + the live_power route + /api/power) all count the marker.
- Precedence is configured sensor → BCD → Mini → derive/none, enforced in
  /api/power's resolution (`effective_ps` first, then `mini_active`).

STATUS: BUILT (v3.x, this cycle).
- Marker recognition, /api/power Mini branch (smartMeterTelemetry `demand` W→kW),
  and a 45s server-side TTL cache to bound quota even under fast polling.
- Overview opt-in toggle: enable card (with the blunt "consumes your query
  allowance intensively" caveat) when a Mini is present and there's no cheaper
  source; a disable note once chosen; `POST /api/power-source/mini {enabled}`
  writes/clears the marker and refuses (409) if a real `power_sensor` is set.
- Poll cadence is Mini-aware: 5s for a local sensor, 60s for the Mini, and the
  Mini poll pauses while the page is hidden (Page Visibility API) so a
  backgrounded tab doesn't burn quota.
- BCD auto-adopt is runtime (above). `sensor_kw` is now unit-aware (respects
  `unit_of_measurement`: W→kW, kW as-is, else magnitude heuristic) — BCD's
  `current_demand` is in watts, and this also fixes any watt power sensor.
- Validatable cases: Mini path on the dev box (Mini present, no sensor, no BCD);
  BCD adopt on the real account (BCD present there). ~1000 tests green.

## Credentials placement (decided)

API modes order: survey → credentials → main → sensors → devices [→ sub] → done.
Credentials right AFTER survey so the Kraken connect/auto_discover can pre-fill
later steps (account, MPANs). CAD-only: no credentials step.

NOTE: this CHANGES the order already built (currently survey→main→sensors→
credentials→devices). computeWizSteps() must place 'credentials' second (after
survey) not after sensors.

## Two discoveries, two roles (don't conflate)
- BCD detection (HA states): seeds survey + sensor fields. Fires at wizard start.
- Kraken auto_discover (API connect): account/MPANs/Mini. Fires on credentials
  connect (already built in connect_kraken_now / POST /api/kraken-config).
  Its return (account, import_mpan, export_mpan, mini) should pre-fill main +
  sensors steps that follow credentials.

## Sensors step in API modes (decided earlier)
api / api+mini: omit local import/export READ sensors (no local feed). Keep
optional rate/standing/power. If BCD pre-filled rate/SC, show them populated.
Already partly built (noLocal branch in wizStepSensors) — keep, integrate
BCD pre-fill.

## Build checklist for next turn
1. Server: add GET /api/detect-bcd → ha_client.get_all_states() →
   detect_bottlecapdave() → {found: bool, account_number?, rate_sensor?,
   standing_charge_sensor?, import_read_sensor?}. MUST return {found:false}
   cleanly when BCD absent (the common pure-API case) — never fails hard.
2. Wizard: at startWizard, call /api/detect-bcd. If found, seed hasApi=true,
   accountNumber, and sensor fields. If NOT found, seed nothing and proceed —
   pure-API path is the baseline, not an error.
3. Survey: remove Mini question + hm branch; 2×2 derivation (cad/cad+api/api).
4. computeWizSteps: move 'credentials' to right after 'survey' (API modes only).
5. Credentials connect: use returned discovery (account/MPANs) to pre-fill
   main + sensors steps. This works WITHOUT BCD — Kraken discovery is the
   primary source; BCD is only an extra pre-fill convenience.
6. Sensors (API mode): omit local read sensors. Rate/SC fields pre-filled from
   BCD IF detected, else LEFT EMPTY (valid — Kraken schedule supplies rates).
   Do not force rate entry in API mode.
7. Verify pure-API mode doesn't trip the zero-rate runtime warning as a problem
   (schedule fills rate at reconcile). Adjust _warn_zero_rate gating if needed
   to respect mode_uses_api().
8. wizApplyAndClose: persist mode (already), credentials saved by verify step.
9. Tests: /api/detect-bcd route registration + detect_bottlecapdave unit test
   over a fake states list INCLUDING a not-found case; survey 2×2 derivation
   (JS, manual). Test that API mode tolerates empty rate config.
10. Verify wizard JS with node (Jinja-stub). Full suite stays green.

## Still pending after this (not part of wizard build)
- Config-page mode DISPLAY + "change mode" (re-run survey) — ongoing config UI.
- Live deploy tests: billing-source toggle round-trip, standing-charge
  settlement, retry-settlement, full api+mini + Mini path on flattened dev.
- Docs / 2.x→3.0.0 migration — see "V2 → V3 MIGRATION" section below (design
  intent captured; full path TBD).

---

## SUPPLIER-FIRST SURVEY GATING (design agreed; build next session)

**Insight:** supplier is not a conditional sub-step of mode — it is the GATING
FIRST question. It narrows everything after it. If the supplier isn't one we
support via API, there's no point asking the API-mode questions at all.

### Why supplier is first-class (the future-developer enabler)
EMT currently conflates "API mode" with "Octopus" implicitly (the Kraken client,
tariff parsing, dispatch query all assume Octopus). Making `supplier` an EXPLICIT
stored field that the API paths key off is what lets a future developer add
another Kraken-platform supplier (E.ON, EDF, and other Kraken tenants — confirmed
on the same platform) as "add a supplier entry + its profile" rather than
"untangle the Octopus assumption everywhere." V3's job is to leave that seam
MARKED and obvious, NOT to build a speculative multi-supplier abstraction now
(you cannot build a correct abstraction from one example — extract the Supplier
interface when a 2nd supplier exists to define the seam against). Depth A
(explicit field + dropdown + supplier-keyed branches), not Depth B (full client
refactor).

### Revised survey order (supersedes the 2×2-first flow above)
0. **Supplier (NEW, FIRST).** "Who is your energy supplier?" Dropdown:
   - "Octopus Energy" → unlocks the API-backed modes.
   - "My supplier isn't listed / local metering only" → routes to local-only.
   (Future suppliers are added as new dropdown entries that unlock API modes.)
1. **Local feed?** (hasLocal) — asked for all suppliers.
2. **Supplier API?** (hasApi) — ONLY asked/available when supplier == octopus.
   Non-supported supplier forces hasApi=false.
3. Mode derived from (supplier, hasLocal, hasApi):
   - supplier=octopus: existing 2×2 (cad / cad+api / api). Mini auto-elevates.
   - supplier=not-listed: hasApi forced false → only cad (local required), or
     ERROR if also no local feed (no data source at all).

### Gating matrix
| supplier     | offered modes              | credentials step | BCD detect |
|--------------|----------------------------|------------------|------------|
| octopus      | cad / cad+api / api        | iff mode has api | yes        |
| not-listed   | cad only                   | never            | no         |

### Knock-on points
- `computeWizSteps` already keys 'credentials' off `mode.indexOf('api')`, so a
  not-listed→cad user correctly skips credentials with no extra logic.
- BCD detection should fire ONLY when supplier==octopus (no point otherwise).
- `supplier` persists ACCOUNT-LEVEL (with mode + credentials; one account, many
  meters), not per-meter. (NB: a `supplier` field already exists in wiz.data and
  mainMeta as free-text — formalise it to the gating dropdown + account scope.)
- Sensor step framing reads "comes from {supplier}" (the original Tier-1
  sensor-hiding follows once the survey gating is in).
- Server-side ENFORCE the gating (don't trust the UI alone): reject an api-mode
  save when supplier is not API-capable.

---

## V2 → V3 MIGRATION (open consideration — keep in mind, not yet solved)

Supplier-first must NOT make upgrade hostile. A v2 user predates `supplier`,
`data_source_mode`, and the API entirely — v2 was effectively CAD-only with read
sensors. The migration rules below are the design intent; the full path is TBD.

### Hard rule: absence of supplier ≠ broken
An upgrading v2 user must keep working WITHOUT being forced through the new
survey. Their existing CAD config keeps forming blocks exactly as before. The
supplier-first gate applies to NEW api setup, never retroactively blocks an
existing working local setup.

### How a v2 user maps onto the new model
- No `supplier` field present → treat as "not-listed / local-only" by default.
- No `data_source_mode` → the upgrade bridge already assigns sticky `cad`
  (`_detect_upgrade_mode`, built). This aligns perfectly: not-listed → cad is the
  same branch a migrated v2 user lands on. The supplier-first design and the
  upgrade bridge AGREE rather than conflict.
- Existing read/rate/standing sensors → preserved, still required for cad. No
  sensor-hiding applies (cad mode keeps them).
- Result: v2 user upgrades, keeps running as cad, sees no new mandatory question.

### When the supplier question DOES surface for a migrated user
Only if they proactively choose to add the API (e.g. via the future "Change data
source" config action, MODE-UI §3). At that point they pick a supplier and the
gating runs forward — opt-in, never forced.

### Migration questions — RESOLVED (release decisions C1–C4)
1. **Backfill `supplier="octopus"` for existing API users?** RESOLVED (C1): NOT
   NEEDED. No released EMT ever had API access, so no existing user has API creds —
   there is nothing to backfill. Supplier simply defaults to not-set for everyone
   on upgrade. (The only half-migrated DB is the author's own prod_dev, reset to a
   clean 2.x snapshot before the upgrade test.)
2. **One-time notice vs silent?** RESOLVED (C2): silent. No first-load banner;
   existing setups carry on untouched and the new options are discovered when the
   user opens config / Change Setup.
3. **Show supplier read-only for cad users?** RESOLVED (C3): leave the supplier
   field AS IS (not shown read-only) until the user actively selects a supplier on
   Change Setup. Don't surface a "Local metering only" label pre-emptively.
4. **Restore tolerance.** RESOLVED (C4): confirmed — a restored v2-era backup with
   no supplier field defaults cleanly to not-listed.

### Guiding principle
Migration = "existing setups keep working untouched; new concepts are opt-in."
The supplier field defaults to a value that means "carry on as before," so the
schema grows without breaking anyone. Same discipline as the mode upgrade bridge.