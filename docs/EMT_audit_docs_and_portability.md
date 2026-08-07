# EMT Audit — Documentation & Supplier Portability

_Dated 2026-08-06 · against v4.1.1 · read-only sweep, no code changed_

This is a findings + recommendations report, not a set of applied changes. Nothing here touches billing logic. Anything that would (Part B code work) is flagged as **needs sign-off** because of the standing "refactors must not change results" rule.

---

## Part A — Documentation sweep

`docs/` holds 19 files / ~6,700 lines. The problem isn't volume, it's that (a) a few contributor-facing docs still describe the pre-4.0 world, and (b) shipped one-off design specs sit unlabelled next to living docs, so a newcomer can't tell what's current.

### A1. Stale — fix (contributor-facing, high value)

| Doc | Problem | Fix |
|-----|---------|-----|
| **CONTRIBUTING.md** | Asks bug reporters for "a sample of the affected blocks from `blocks.json`" and lists "breaks backward compat with existing `blocks.json` data" as a reject reason — but JSON storage was **removed in 4.0.0**; everything is SQLite (`blocks.db`). Also tells contributors to run `python3 -m unittest test_engine`, but the suite is now `run_tests.sh` (1,854 tests). | Replace `blocks.json` → `blocks.db`; point tests at `run_tests.sh`; refresh the reject-list. This is the first doc an outside contributor reads — worth getting right first. |
| **DEVELOPMENT.md** | Says blocks live in `energy_meter.db`; the code uses `blocks.db` (`BLOCKS_DB_PATH = f"{DATA_DIR}/blocks.db"`). Still references `blocks.json`. "Since 2.0.0…" framing predates the 4.x architecture. | Correct the filename, drop JSON references, refresh the architecture table (it omits the `kraken_*` modules, `bill_parser`, `csv_import`, the attribution engine). |

### A2. Historical — archive (accurate for their era, done)

| Doc | Status | Recommendation |
|-----|--------|----------------|
| **SQLITE_MIGRATION_PLAN.md** | The JSON→SQLite migration is fully executed **and the migration code was deleted in 4.0.0**. 17 references to a path that no longer exists. | Move to `docs/history/` (or delete). Pure artefact. |
| **DEVELOPMENT-3.md** | "Volume 3 … decisions during 2.10.x and the design for 3.0.0 Kraken integration." Point-in-time record, now 4.1.x. The "-3" with no visible Volume 2 is confusing. | Archive under `docs/history/`, or fold the still-relevant "why" notes into DEVELOPMENT.md and retire the numbered volumes. |
| Shipped design specs: `historical_import_build_spec.md` + `historical_import_design.md` (overlap), `historical_import_wizard_design.md`, `wizard_design.md`, `mode_ui_design.md`, `intelligent_rates_design.md`, `dispatch_validation_design.md`, `measurements_cost_recovery_design.md`, `region_timeline_design.md`, `historical_attribution_design.md`, `bill_to_csv_import_spec.md` | All describe features that have **shipped**. Valuable as rationale, but unlabelled they read as "planned". | Add a one-line status header to each (`Status: Shipped in X.Y` / `Draft`), and move the closed ones into `docs/design/` with an index. `build_spec` vs `design` for historical import are near-duplicates — keep one. |

### A3. Living — light cleanup

| Doc | Note |
|-----|------|
| **ROADMAP.md** | Living (edited today). 5 stray `blocks.json` references to scrub. Otherwise current. |
| **data_management_vision.md** | Largely delivered; 2 stale JSON references. Mark delivered items and keep the forward-looking remainder. |
| **iog_6hr_cap_design.md** | Current (BL-9, still unbuilt). Keep. |

### A4. Missing (the approachability gap)

- **No `docs/README.md` index** — 19 files with no map. A newcomer can't tell entry point from archive.
- **No single current "architecture at a glance"** — DEVELOPMENT.md is the closest but is stale and pre-`kraken_*`.
- **No "how billing accuracy is protected" note** — given how central the "must not change results" rule is, a short doc stating the invariant + how the test suite guards it would stop well-meaning PRs from breaking it.

**Suggested shape:** `docs/README.md` (index + "start here"), `docs/DEVELOPMENT.md` (current architecture, refreshed), `docs/design/` (shipped specs, status-headed), `docs/history/` (migration plan, dev volumes). No content lost, but the current/archive line becomes obvious.

---

## Part B — Supplier portability & contributor approachability

Goal: let a developer add EDF (or another Kraken supplier) without rewriting the core, and lower the barrier to contribution generally.

### B0. What's already portable (good foundations)

- **Endpoint is not hardcoded.** `KrakenAPIClient(base_url, graphql_url)` with `DEFAULT_BASE_URL`/`DEFAULT_GRAPHQL_URL`, wired through `save_kraken_credentials(api_key, account, base_url)` and a `KRAKEN_BASE_URL` env override. Point it at another Kraken host today.
- **A supplier seam already exists.** `engine._API_CAPABLE_SUPPLIERS = frozenset({"octopus"})`, `normalize_supplier()`, `supplier_is_api_capable()`, mirroring a client `WIZ_SUPPLIERS` registry, with server-side gating. This is the anchor to build on — it's currently just Octopus-only.
- **Graceful degradation exists** for the IOG dispatch feature (the "Unable to find device" path is caught, not fatal).

### B1. Coupling hotspots (ranked by how hard they block a second supplier)

| # | Where | Assumption | Impact on EDF / other Kraken | Suggested seam |
|---|-------|-----------|------------------------------|----------------|
| 1 | `kraken_api_client.py` auth | REST uses HTTP Basic with the **API key as username**; GraphQL uses `obtainKrakenToken(input:{APIKey})`. | Other Kraken tenants commonly authenticate with **email + password** → `obtainKrakenToken(input:{email,password})`. No API key means no login at all today. | An **auth strategy** on the supplier profile: `api_key` vs `email_password`, selecting the Basic-auth vs token-input shape. |
| 2 | `kraken_api_client.py` REST surface | `/v1/accounts/{acct}/`, `/v1/electricity-meter-points/.../consumption/`. | The Octopus **REST** `/v1/` API is Octopus-specific; other Kraken brands are typically **GraphQL-first** and may not expose it. The settlement ingester leans on REST `get_consumption`. | Define a `get_consumption(...)` interface with the REST impl as one backing; add a GraphQL `measurements` backing (the historical importer already speaks GraphQL measurements — reuse it). |
| 3 | `kraken_rates.py`, `rate_schedule_probe.py`, `bill_parser.py` | Octopus **product-code grammar** (`E-1R-AGILE-24-10-01-A`) and product names ("Outgoing Octopus…"). | Other suppliers use different code/tariff shapes; the parser mis-reads or drops them. | Isolate product-code parsing behind the supplier profile; treat it as optional metadata, not a hard dependency for import. |
| 4 | `engine.py` (carbon, DCC), `block_store.py` | **GB-only**: DCC settlement model, `carbonintensity.org.uk`, postcode→DNO region. | A non-GB Kraken (e.g. Octopus international, or a supplier without DCC) has no DCC settlement and no GB carbon API. | Capability flags on the profile: `dcc_settlement`, `carbon_region`. Off ⇒ skip the sweep/carbon paths cleanly (they already tolerate "no data"; make it explicit, not incidental). |
| 5 | `bill_parser.py` | Octopus **PDF layout** only. | Won't read another supplier's bill. | Already lazy-imported/optional — just document it as Octopus-specific and keep it out of the core path (it is). |
| 6 | UI + identifiers | "Octopus", "Kraken", "DCC", "IOG" appear in labels and code names. | A non-Octopus user won't recognise their own setup; a contributor can't tell brand from mechanism. | Neutral user-facing labels ("supplier settlement" not "DCC"); keep brand words behind the profile. Low urgency, high approachability. |

### B2. Recommended structural move (the one that unlocks the rest)

Extend the existing supplier seam into a **`SupplierProfile`** the whole stack reads from, rather than scattering `if octopus` checks:

```
SupplierProfile:
  key            e.g. "octopus" | "edf"
  base_url, graphql_url
  auth_strategy  "api_key" | "email_password"
  capabilities   { rest_consumption, gql_measurements,
                   dispatch_iog, dcc_settlement, carbon_region }
  product_codes  optional parser hook
```

`_API_CAPABLE_SUPPLIERS` becomes "profiles where an API capability is set". A contributor adding EDF writes **one profile + one auth strategy**, and the capability flags switch the GB-only paths off without touching billing maths. That containment is exactly what makes the change safe under the "must not change Octopus results" rule — Octopus's profile keeps today's values byte-for-byte, and there's a natural place to add a regression test asserting that.

### B3. Contributor approachability (independent of suppliers)

- Add a **"Add a supplier" section to CONTRIBUTING.md** pointing at the profile seam — turns a daunting cross-cutting change into a checklist.
- State the **billing-accuracy invariant** explicitly (Part A4) so PRs know the one hard rule.
- The `kraken_*` module names conflate "our settlement layer" with "the Octopus brand". Renaming is **not** recommended now (churn/risk); note it as a future `chore/` once the profile seam lands.

---

## Prioritised actions

**P1 — safe, do now (docs only, zero code risk)**
1. Fix CONTRIBUTING.md (blocks.json → blocks.db; tests → run_tests.sh; add "Add a supplier" stub).
2. Fix DEVELOPMENT.md (filename, JSON refs, module list).
3. Add `docs/README.md` index; scrub the 7 stray `blocks.json` refs across ROADMAP/vision.
4. Archive SQLITE_MIGRATION_PLAN.md + DEVELOPMENT-3.md to `docs/history/`; status-header the shipped design specs.

**P2 — code, needs sign-off (must not change Octopus results)**
5. Introduce `SupplierProfile` over the existing `_API_CAPABLE_SUPPLIERS` seam; port Octopus into it unchanged; add a golden-output regression test.
6. Add an `email_password` auth strategy alongside the API-key one.
7. Make the GB-only paths (DCC sweep, carbon) explicitly capability-gated.

**P3 — later**
8. GraphQL `measurements` backing for `get_consumption` (removes the REST-only dependency).
9. Neutral UI terminology; `kraken_*` rename as a `chore/`.

---

### Suggested next step
The P1 doc set is low-risk and directly serves the approachability goal — I can do all of it now and hand you a patch. P2/P3 are a design proposal I'd want your go-ahead on before writing, given the accuracy constraint. Say the word on which.

---

## Part C — EDF-fork findings (real evidence for the portability seam)

Reviewed `stevekirtley/HomeAssistant-EDFEnergy` — a WIP fork of BottleCapDave's
Octopus integration being adapted for EDF (also Kraken). The README is empty, but
the code diff is a direct map of what actually differs between two Kraken
suppliers. It **confirms B1** and, usefully, points to a cheaper implementation
than "abstract the whole client".

### C1. Auth is the real difference — and there's a shortcut

Their `api_client/__init__.py` carries three token mutations side by side:

```graphql
obtainKrakenToken(input: { email, password }) { token refreshToken refreshExpiresIn }   # EDF primary
obtainKrakenToken(input: { refreshToken })    { token refreshToken refreshExpiresIn }   # refresh
obtainKrakenToken(input: { APIKey })          { token refreshToken refreshExpiresIn }   # Octopus (const.py: "kept for migration only")
```

EDF logs in with **email + password → JWT + refresh token** (they track
`refresh_expires_in` and raise an "auth token expiring soon" repair). Crucially,
right after those they keep:

```graphql
query    { viewer { liveSecretKey } }      # read the account's API secret
mutation { regenerateSecretKey { key } }   # mint a new one
```

**The shortcut:** email/password → JWT → `regenerateSecretKey` → then feed the
minted key into the *existing* API-key client. So EMT's parked P2 does **not**
require rewriting the REST Basic-auth path. It needs an **auth-strategy front
door**: for a non-Octopus Kraken, do email/password → mint key → hand the rest of
the stack the same key it uses today. Smaller and safer than a full client
abstraction, and it keeps the Octopus path byte-for-byte.

### C2. What did NOT change (portability is more feasible than B1–B4 implied)

| Area | Finding in the EDF fork | Effect on EMT estimate |
|------|-------------------------|------------------------|
| GraphQL shapes | `properties(accountNumber:)`, `obtainKrakenToken`, dispatch queries — unchanged | EMT's queries port over |
| Tariff codes (B3) | `REGEX_TARIFF_PARTS` is the Octopus `E-1R-PRODUCT-REGION` regex verbatim, region letter and all | B3 shrinks — same grammar |
| Intelligent dispatch | Same model: `smart-charge`/`bump-charge`, planned/started dispatches, EV/charger device kinds | EMT's dispatch overlay largely carries over |
| GB features (B4) | EDF is a **GB** supplier → DCC settlement + UK regional carbon still apply | B4 only bites for **non-GB** Kraken, not EDF |

Net: for **EDF specifically**, the work reduces to (1) the auth front door and
(2) confirming the consumption endpoint. The `SupplierProfile` is still the right
container, but the first supplier is cheaper to add than the audit first
suggested.

### C3. Patterns worth borrowing / cautions

- **`KNOWN_OFF_PEAK_WINDOWS`** — a hardcoded fallback (keyed by tariff-code substring, e.g. `GOELEC` → 23:00–06:00) for when the API truncates a midnight-spanning off-peak window to one day of rates. EMT already has an analogous observed-floor fallback; this is a cleaner keyed form.
- **HA "repairs" / actionable issues** (`invalid_api_key`, `account_not_found`, `auth_token_expiring_soon`, `no_active_tariff`) and explicit per-feed refresh cadences — good reference points; EMT has its own health surfacing.
- **`DEFAULT_CALORIFIC_VALUE = 40.0` + `calorific_value` config** — the standard gas m³→kWh path if EMT ever tackles the gas limitation.
- **Caution — supplier promo bloat.** The fork has accreted EDF-only marketing features (Sunday Saver, "football free electricity") and even pulls a **third-party relay URL** (`apirelay.sitetest.org.uk`) into `const.py`. Keep any supplier promo features out of EMT core and behind the profile; never inherit an external endpoint like that.