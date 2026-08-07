# EDF (Kraken) Supplier Support — Design Note

> _Status: **Draft — not built, and NOT validated against a real EDF account.**
> EMT has no EDF tester. Everything below is sourced from reading
> `stevekirtley/HomeAssistant-EDFEnergy` (a WIP fork of BottleCapDave's Octopus
> integration; the fork itself says "not ready for use"). It describes the shape
> of the work so it's a small, defined job when an EDF user appears — the first
> EDF user is the tester._

## Why this is feasible

EDF migrated ~5.8M UK customers onto Octopus's **Kraken** platform (2023–24), so
an EDF account is reachable through essentially the same Kraken GraphQL API EMT
already speaks. The porting work is **auth + endpoint**, not a rewrite. See the
audit report Part C for the summary; this note is the concrete detail.

## Confirmed facts (from the fork's `api_client`)

### Endpoint
```
https://api.edfgb-kraken.energy/v1/graphql/
```
Distinct host from Octopus (`api.octopus.energy`), same `/v1/graphql/` path.
EMT already parameterises this (`KrakenAPIClient(base_url, graphql_url)` /
`KRAKEN_BASE_URL` / `save_kraken_credentials(..., base_url)`).

### Auth (this is the real difference)
- Login is **email + password**, not an API key:
  ```graphql
  obtainKrakenToken(input: { email, password }) { token refreshToken refreshExpiresIn }
  obtainKrakenToken(input: { refreshToken })    { token refreshToken refreshExpiresIn }
  ```
- **Gotchas the fork documents (would have bitten us building blind):**
  1. `viewer.liveSecretKey` is **masked** on EDF — you cannot read a usable key. You must **mint** one with `mutation { regenerateSecretKey { key } }`.
  2. **Minting invalidates any previously issued key on the account.** If the user has another Kraken integration using an EDF key, this breaks it. EMT must warn on this before minting.
  3. The password is used **only** for the one-time exchange and must never be stored — persist the minted key (+ optionally the refresh token) only.
- Net: after `email/password → regenerateSecretKey`, the minted key feeds the **existing** API-key client path unchanged.

### GraphQL shapes that are IDENTICAL to Octopus (port directly)
- **Account discovery:** `properties(accountNumber:)` + `account(accountNumber:) { electricityAgreements(active:true) { meterPoint { mpan direction meters { serialNumber makeAndType meterType smartImportElectricityMeter{deviceId…} smartExportElectricityMeter{deviceId…} } agreements { validFrom validTo tariff { productCode tariffCode } } } } gasAgreements { meterPoint { mprn … } } }`. Same fields EMT's region/site discovery already reads. (Gas is present — `mprn`, `consumptionUnits` — but out of EMT scope for now.)
- **Live telemetry:** `smartMeterTelemetry(deviceId, grouping: HALF_HOURLY, start, end) { readAt consumption consumptionDelta demand export }` — the same query EMT's client already uses for the Home-Mini-style live feed. Note the `export` field is here too.
- **Intelligent dispatch (IOG-equivalent):** `flexPlannedDispatches(deviceId){ start end type energyAddedKwh }` and `completedDispatches(accountNumber){ start end delta meta{ source location } }` — **byte-identical** to what EMT's dispatch overlay consumes. `SmartFlexVehicle` / `SmartFlexChargePoint` device kinds, `smart-charge`/`bump-charge` controls.
- **Tariff codes:** same `E-1R-PRODUCT-REGION` grammar and DNO region letter (the fork's regex is the Octopus one verbatim).

### Rates / standing charges
Processed from a REST-style `data["results"]` list with
`value_inc_vat` / `valid_from` / `valid_to` / `payment_method`, normalised to
30-minute increments — same structure EMT's rate schedule handling expects.
**One extra consideration:** EDF tariffs publish both `DIRECT_DEBIT` and
non-DD rates; the fork picks by a `favour_direct_debit_rates` preference and
falls back to whatever's published if only one exists. EMT should mirror that
(a single tariff can return two rate series).

## What EMT would change

1. **`SupplierProfile` seam** over the existing `_API_CAPABLE_SUPPLIERS` gate:
   `{ key, base_url, graphql_url, auth_strategy, capabilities }`. Octopus becomes
   one profile with **zero behaviour change** (guard with a golden-output test).
2. **Auth strategy** on the profile: `api_key` (Octopus, today) vs
   `email_password` (EDF: login → `regenerateSecretKey` → store key → proceed as
   api_key). Add the "minting invalidates existing keys" warning to the setup UI.
3. **EDF profile:** `base_url = https://api.edfgb-kraken.energy`, `auth = email_password`.
4. **Consumption route (the one open question, below).**
5. **Capability flags:** EDF is GB → DCC settlement + UK carbon stay ON. Only a
   non-GB Kraken would switch them off.
6. **DD-rate selection** in rate handling.

## Open question to resolve with the first EDF user

The fork reads **live** consumption via `smartMeterTelemetry` (GraphQL). EMT's
**settled DCC** path currently uses the Octopus REST
`/v1/electricity-meter-points/{mpan}/meters/{serial}/consumption/` endpoint. It's
**not yet confirmed** whether EDF exposes that REST surface on
`api.edfgb-kraken.energy`, or whether settled consumption must come via the
GraphQL **Measurements** query instead.

Mitigation: EMT already has **both** a `smartMeterTelemetry` path and a GraphQL
**Measurements** path (the 4.0.0 historical importer). So if EDF is GraphQL-only
for consumption, EMT can route settlement through Measurements — code it already
has — rather than depending on the REST endpoint. This is the first thing a real
EDF account should confirm.

## Validation plan (when an EDF account exists)

1. Read-only probe (stdlib, no EMT changes): `email/password → obtainKrakenToken → regenerateSecretKey → account query → one day of smartMeterTelemetry AND a Measurements/REST consumption fetch`. Confirm which returns settled half-hours, and the exact field names/units.
2. Reconcile one day's kWh/cost against the user's **actual EDF bill** before anything is trusted (accuracy-first rule).
3. Ship EDF **behind an experimental, opt-in flag** with a clear "check your first bill" notice until reconciliation passes.

## Provenance

All shapes above are read from `custom_components/edf_energy/{const.py,api_client/__init__.py}`
on the fork's `develop` branch (integration version 18.9.x). No EDF account, no
live responses — treat as a strong hypothesis, not verified behaviour.
