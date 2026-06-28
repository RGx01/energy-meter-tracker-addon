"""
kraken_api_client.py — Octopus Energy (Kraken platform) API client.

Chunk 3a: REST surface only — auth, account, consumption, unit rates,
standing charges, auto_discover, test_connection. GraphQL (token, telemetry,
rateLimitInfo) and BottlecapDave detection arrive in Chunk 3b.

Design notes
------------
- Async (aiohttp), to sit alongside the engine loop and the ingester task.
- HTTP Basic auth: the Octopus API key is the username, password empty.
  (Confirmed: docs.octopus.energy/rest — "Basic HTTP auth using your API key
  as the username, password blank".)
- Read-only. This module never writes to the user's account and never stores
  the API key anywhere; the key is held only for the lifetime of the client
  instance and used solely as the Basic-auth username.
- Response shapes follow the documented public REST API. Several details are
  flagged in DEVELOPMENT-3 as "verify against real account" — those are noted
  inline; the parsing here tolerates absence (uses .get()) so a shape surprise
  degrades to None/empty rather than raising.
- Privacy: identifiers are masked to the last 4 chars in any log line (mpan,
  serial, account number), matching the engine's existing mpan[-4:] practice.
"""

from __future__ import annotations

import asyncio
import base64
import logging
import random
from typing import Any, Optional

import aiohttp

logger = logging.getLogger("kraken_api_client")

DEFAULT_BASE_URL = "https://api.octopus.energy"
DEFAULT_GRAPHQL_URL = "https://api.octopus.energy/v1/graphql/"
_DEFAULT_TIMEOUT = 30  # seconds per request
_PAGE_SIZE = 1500      # consumption rows per page (API max is large; keep sane)

# GraphQL rate-limit error code (Kraken). Surfaced in the errors[] array of an
# otherwise-200 response. Verified against Octopus GraphQL guides.
_GQL_RATE_LIMIT_CODE = "KT-CT-1199"
# Kraken returns this code when a requested field has been disabled/removed —
# the definitive "the schema changed under us" signal (drift canary).
_GQL_DISABLED_FIELD_CODE = "KT-CT-1113"
# Bounded exponential backoff for retryable GraphQL failures (rate limit,
# HTTP 429/5xx, transient transport/timeout). Delays: 1, 2, 4, 8s (+jitter),
# capped, then give up. Non-retryable errors (auth, other 4xx, logic) raise at once.
_GQL_MAX_RETRIES = 4
_GQL_BACKOFF_BASE = 1.0   # seconds
_GQL_BACKOFF_CAP = 30.0   # seconds
# Refresh the JWT this many seconds before its exp claim, to avoid using a
# token that expires mid-flight.
_TOKEN_REFRESH_SKEW = 120


def _mask(value: Optional[str]) -> str:
    """Mask an identifier for logging — first and last character only, e.g.
    'A-42B0BCA7' → 'A…7', '2600002170611' → '2…1'. Short/empty values fully
    masked."""
    if not value:
        return "<none>"
    s = str(value)
    if len(s) <= 2:
        return "…"
    return s[0] + "…" + s[-1]


# SmartFlex device classification — the IOG-controllable charging devices whose
# manufacturer/provider drives dispatch behaviour (vs meters, batteries, heat pumps).
_CHARGING_DEVICE_TYPES = {"ELECTRIC_VEHICLES", "CHARGE_POINTS"}
_CHARGING_TYPENAMES = {"SmartFlexVehicle", "SmartFlexChargePoint"}

# Fields EMT actually selects across its GraphQL queries (auth, device
# discovery, telemetry, dispatches, devices). The deprecation check
# (check_field_deprecations) introspects the live schema and warns if any of
# these is flagged isDeprecated — the warning PHASE, before the field is
# removed. That is distinct from the runtime drift logging in _graphql_once,
# which catches fields already REMOVED (KT-CT-1113 / "Cannot query field").
# Together: introspection = early warning, drift = last line.
# NB the match is by field NAME (introspection is type-keyed, but tracking exact
# Kraken type names here would rot); the warning log includes the type so a
# same-named field on a type EMT doesn't use can be adjudicated by the reader.
_EMT_GRAPHQL_FIELDS = {
    # auth
    "obtainKrakenToken", "token",
    # device discovery (account → agreements → meter point → meters → devices)
    "account", "electricityAgreements", "meterPoint", "meters",
    "smartDevices", "deviceId",
    # telemetry
    "smartMeterTelemetry", "readAt", "demand", "consumption",
    "consumptionDelta", "costDelta",
    # dispatches
    "plannedDispatches", "completedDispatches",
    "startDt", "endDt", "delta", "meta", "source", "location",
    # devices (provider / charge-point discovery)
    "devices", "provider", "deviceType", "status", "current", "make", "model",
}
# Enum VALUES EMT passes or compares literally — a rename/removal silently
# breaks logic (telemetry grouping, live-device test, charging-device match).
_EMT_GRAPHQL_ENUMS = _CHARGING_DEVICE_TYPES | {"TEN_SECONDS", "LIVE"}

# Known same-NAME collisions: a deprecated field whose name is in the set above
# but which sits on a (type, position) EMT never selects. Kept at (type, field)
# granularity — NOT type-wide — so a real future deprecation of another field on
# the same type (e.g. DeviceStatusType.current, which EMT *does* read) still
# fires. Seeded from observed introspection; extend as new collisions surface.
#   - DeviceStatusType.status   : EMT reads status{current} on the DEVICE, not
#                                 the status field of DeviceStatusType itself.
#   - HeatPumpDeviceType.deviceType : EMT reads deviceType at the device
#                                 interface level; it has no heat pump.
#   - TestCharge.status         : EMT never queries TestCharge.
_DEPRECATION_IGNORE = {
    ("DeviceStatusType", "status"),
    ("HeatPumpDeviceType", "deviceType"),
    ("TestCharge", "status"),
}


def _pick_device_provider(devices: list) -> Optional[str]:
    """Manufacturer/provider signal for the smart-charging device that drives
    dispatch behaviour, from the polymorphic `devices` query.

    The `devices` API splits what the deprecated registeredKrakenflexDevice
    conflated: ``provider`` is the flex/control provider (often 'OCTOPUS_ENERGY'),
    while the device manufacturer (MYENERGI, OHME, TESLA, …) is in ``make`` on the
    vehicle/charge-point types. EMT's per-provider logic — notably OHME detection
    via ``"OHME" in provider`` — keys on the manufacturer, so we surface ``make``
    first and fall back to ``provider``.

    Ranks candidates so a LIVE charging device (EV / charge point) wins over a
    non-charging or non-LIVE one; returns None when the account has no device
    carrying a signal.
    """
    best_rank = -1
    best_signal = None
    for dev in devices or []:
        if not dev:
            continue
        signal = dev.get("make") or dev.get("provider")
        if not signal:
            continue
        dtype = dev.get("deviceType") or ""
        typename = dev.get("__typename") or ""
        is_charging = (dtype in _CHARGING_DEVICE_TYPES
                       or typename in _CHARGING_TYPENAMES)
        is_live = ((dev.get("status") or {}).get("current") or "").upper() == "LIVE"
        rank = (2 if is_charging else 0) + (1 if is_live else 0)
        if rank > best_rank:
            best_rank = rank
            best_signal = signal
    return best_signal


def _operation_name(query: str) -> str:
    """Best-effort GraphQL operation name (e.g. 'dispatches') for log context."""
    for kw in ("query ", "mutation "):
        i = (query or "").find(kw)
        if i != -1:
            rest = query[i + len(kw):].lstrip()
            return (rest[:64].split("(")[0].split("{")[0].strip() or "?")
    return "?"


def _schema_drift_errors(errors: list) -> list:
    """GraphQL errors that mean the *schema* changed under us — a field EMT
    requests was renamed, removed, or disabled by Kraken. This is the signal a
    Kraken API change has broken a query, as distinct from auth, rate-limit, or
    resolution errors.

    Note: an auth failure (KT-CT-1139) is itself classed VALIDATION, so we do NOT
    key on the VALIDATION class. We rely on the disabled-field code (KT-CT-1113)
    and GraphQL's unknown-field validation messages, which are specific to drift.
    """
    out = []
    for e in errors or []:
        ext = (e or {}).get("extensions") or {}
        code = ext.get("errorCode") or ext.get("errorType") or ""
        msg = (e.get("message") or "")
        if (
            code == _GQL_DISABLED_FIELD_CODE
            or "Cannot query field" in msg
            or "Unknown field" in msg
            or "Unknown argument" in msg
            or "Unknown type" in msg
            or "isn't a defined" in msg
        ):
            out.append(e)
    return out


class KrakenAPIError(Exception):
    """Raised for non-retryable client errors (auth, 4xx other than 429)."""

    def __init__(self, message: str, status: Optional[int] = None):
        super().__init__(message)
        self.status = status


class KrakenAuthError(KrakenAPIError):
    """401/403 — bad or unauthorised API key."""


class KrakenRateLimitError(KrakenAPIError):
    """429 — REST rate limit hit. Caller should back off."""


class KrakenAPIClient:
    """Async REST client for the Octopus/Kraken public API.

    Usage:
        async with KrakenAPIClient(api_key, account_number) as client:
            account = await client.get_account()

    Or manage the session yourself by passing one in; the client will not
    close a session it did not create.
    """

    def __init__(
        self,
        api_key: str,
        account_number: Optional[str] = None,
        *,
        base_url: str = DEFAULT_BASE_URL,
        graphql_url: str = DEFAULT_GRAPHQL_URL,
        session: Optional[aiohttp.ClientSession] = None,
        timeout: int = _DEFAULT_TIMEOUT,
    ):
        if not api_key:
            raise ValueError("api_key is required")
        self._api_key = api_key
        self.account_number = account_number
        self.base_url = base_url.rstrip("/")
        self.graphql_url = graphql_url
        self._timeout = aiohttp.ClientTimeout(total=timeout)
        self._session = session
        self._owns_session = session is None
        # GraphQL token cache (obtained lazily, refreshed before exp).
        self._gql_token: Optional[str] = None
        self._gql_token_exp: Optional[float] = None
        # One-shot: deprecation introspection runs once per process (first
        # get_dispatches). See check_field_deprecations. last_deprecations is
        # None until the first successful check, then a (possibly empty) list of
        # {kind,type,name,reason} dicts the engine surfaces to HA.
        self._deprecation_checked = False
        self.last_deprecations: Optional[list] = None

    # ── session lifecycle ────────────────────────────────────────────────
    async def __aenter__(self) -> "KrakenAPIClient":
        if self._session is None:
            # NB: do NOT set session-level auth. The REST surface uses Basic
            # auth (API key as username) applied PER-REQUEST; GraphQL uses a JWT.
            # A session-level BasicAuth would attach to GraphQL calls too and be
            # rejected ("Authorization header is not a valid credential").
            self._session = aiohttp.ClientSession(timeout=self._timeout)
            self._owns_session = True
        return self

    async def __aexit__(self, *exc) -> None:
        await self.close()

    async def close(self) -> None:
        if self._session is not None and self._owns_session:
            await self._session.close()
            self._session = None

    def _ensure_session(self) -> aiohttp.ClientSession:
        if self._session is None:
            # Lazily create even outside the context manager. No session-level
            # auth — REST applies Basic auth per-request (see _rest_auth).
            self._session = aiohttp.ClientSession(timeout=self._timeout)
            self._owns_session = True
        return self._session

    @property
    def _rest_auth(self) -> "aiohttp.BasicAuth":
        """Basic auth for the REST surface — API key as username, empty
        password. Applied PER-REQUEST so it never leaks onto GraphQL calls."""
        return aiohttp.BasicAuth(self._api_key, "")

    # ── low-level GET with paging ────────────────────────────────────────
    async def _get(self, path: str, params: Optional[dict] = None) -> dict:
        """GET a single JSON object from an absolute path or full URL."""
        url = path if path.startswith("http") else f"{self.base_url}{path}"
        session = self._ensure_session()
        try:
            async with session.get(url, params=params,
                                   auth=self._rest_auth) as resp:
                if resp.status in (401, 403):
                    raise KrakenAuthError(
                        f"authentication failed ({resp.status}) — check API key",
                        status=resp.status)
                if resp.status == 429:
                    raise KrakenRateLimitError(
                        "REST rate limit hit (429)", status=resp.status)
                if resp.status >= 400:
                    text = await resp.text()
                    raise KrakenAPIError(
                        f"HTTP {resp.status} for {url}: {text[:200]}",
                        status=resp.status)
                return await resp.json()
        except aiohttp.ClientError as e:
            # Network-level failure — distinct from an HTTP error response.
            raise KrakenAPIError(f"network error for {url}: {e}") from e

    async def _get_paginated(self, path: str,
                             params: Optional[dict] = None) -> list[dict]:
        """Follow `next` cursors, accumulating `results`. Bounded to avoid
        runaway loops on a misbehaving endpoint."""
        params = dict(params or {})
        params.setdefault("page_size", _PAGE_SIZE)
        results: list[dict] = []
        url: Optional[str] = path
        guard = 0
        while url and guard < 500:
            guard += 1
            page = await self._get(url, params if guard == 1 else None)
            results.extend(page.get("results", []) or [])
            url = page.get("next")  # absolute URL or None
        return results

    # ── account ───────────────────────────────────────────────────────────
    async def get_account(self, account_number: Optional[str] = None) -> dict:
        """GET /v1/accounts/<account>/ — MPAN(s), serials, tariff history."""
        acct = account_number or self.account_number
        if not acct:
            raise ValueError("account_number is required for get_account")
        logger.info("get_account: fetching account %s", _mask(acct))
        return await self._get(f"/v1/accounts/{acct}/")

    # ── consumption ────────────────────────────────────────────────────────
    async def get_consumption(
        self, mpan: str, serial: str,
        *, period_from: Optional[str] = None, period_to: Optional[str] = None,
        order_by: str = "period",
    ) -> list[dict]:
        """Half-hourly consumption for an electricity meter.

        Returns the raw results list: dicts of
        {consumption, interval_start, interval_end}. For an export MPAN the
        same shape is returned but `consumption` is the exported kWh.

        order_by='period' returns earliest-first (what the ingester wants for
        sequential block writes).
        """
        path = (f"/v1/electricity-meter-points/{mpan}"
                f"/meters/{serial}/consumption/")
        params: dict[str, Any] = {"order_by": order_by}
        if period_from:
            params["period_from"] = period_from
        if period_to:
            params["period_to"] = period_to
        rows = await self._get_paginated(path, params)
        logger.info("get_consumption: mpan=%s serial=%s → %d rows",
                    _mask(mpan), _mask(serial), len(rows))
        return rows

    # ── tariff charges ──────────────────────────────────────────────────────
    async def get_unit_rates(
        self, product_code: str, tariff_code: str,
        *, period_from: Optional[str] = None, period_to: Optional[str] = None,
    ) -> list[dict]:
        """standard-unit-rates for a tariff. Returns results list of
        {value_exc_vat, value_inc_vat, valid_from, valid_to}."""
        path = (f"/v1/products/{product_code}/electricity-tariffs/"
                f"{tariff_code}/standard-unit-rates/")
        params: dict[str, Any] = {}
        if period_from:
            params["period_from"] = period_from
        if period_to:
            params["period_to"] = period_to
        return await self._get_paginated(path, params)

    async def get_standing_charges(
        self, product_code: str, tariff_code: str,
        *, period_from: Optional[str] = None, period_to: Optional[str] = None,
    ) -> list[dict]:
        """standing-charges for a tariff. Same result shape as unit rates."""
        path = (f"/v1/products/{product_code}/electricity-tariffs/"
                f"{tariff_code}/standing-charges/")
        params: dict[str, Any] = {}
        if period_from:
            params["period_from"] = period_from
        if period_to:
            params["period_to"] = period_to
        return await self._get_paginated(path, params)

    # ── discovery ─────────────────────────────────────────────────────────
    @staticmethod
    def _tariff_to_product_code(tariff_code: str) -> Optional[str]:
        """Derive the product code from a tariff code.

        Tariff codes look like 'E-1R-VAR-22-11-01-N' or
        'E-1R-AGILE-FLEX-22-11-25-A'. The product code is the middle, with the
        leading 'E-1R-' (energy / register count) and the trailing region
        letter stripped: 'VAR-22-11-01' / 'AGILE-FLEX-22-11-25'.

        Returns None if the code doesn't match the expected shape.
        """
        parts = tariff_code.split("-")
        if len(parts) < 4:
            return None
        # Strip leading E/G + register marker (first 2 parts) and trailing
        # single-letter region (last part).
        middle = parts[2:-1]
        return "-".join(middle) if middle else None

    @staticmethod
    def _current_agreement(agreements: list[dict]) -> Optional[dict]:
        """Pick the active agreement (valid_to is null), else the latest."""
        if not agreements:
            return None
        live = [a for a in agreements if not a.get("valid_to")]
        if live:
            return live[0]
        return sorted(agreements, key=lambda a: a.get("valid_from") or "")[-1]

    async def auto_discover(self, account_number: Optional[str] = None) -> dict:
        """Inspect the account and return the meter identifiers EMT needs.

        Returns:
            {
              "account_number": str,
              "import": {"mpan","serial","tariff_code","product_code",
                         "agreements"} | None,
              "export": {...} | None,            # present if an is_export MPAN
              "properties": int,                 # property count seen
              "warnings": [str, ...],
            }

        Single-property is the supported scope. If multiple active properties
        are present a warning is added and the first move-in (not moved_out)
        property is used. The caller (Settings UI) surfaces warnings.

        Open-question notes (verify live):
          - Q3 export MPAN: identified here via the `is_export` flag on the
            electricity_meter_point. If a separate export MPAN isn't flagged,
            export stays None and REST export settlement is unavailable.
        """
        account = await self.get_account(account_number)
        acct_no = account.get("number")
        all_props = account.get("properties", []) or []
        warnings: list[str] = []

        # Active properties: not moved out.
        active = [p for p in all_props if not p.get("moved_out_at")] or all_props
        if len(active) > 1:
            warnings.append(
                f"{len(active)} active properties found; EMT supports a single "
                "property. Using the first. Configure manually if this is wrong.")
        prop = active[0] if active else None

        result: dict[str, Any] = {
            "account_number": acct_no,
            "import": None,
            "export": None,
            "properties": len(active),
            "warnings": warnings,
        }
        if prop is None:
            warnings.append("no property found on account")
            return result

        for emp in prop.get("electricity_meter_points", []) or []:
            mpan = emp.get("mpan")
            meters = emp.get("meters", []) or []
            serial = meters[0].get("serial_number") if meters else None
            agreement = self._current_agreement(emp.get("agreements", []) or [])
            tariff_code = agreement.get("tariff_code") if agreement else None
            product_code = (self._tariff_to_product_code(tariff_code)
                            if tariff_code else None)
            entry = {
                "mpan": mpan,
                "serial": serial,
                "tariff_code": tariff_code,
                "product_code": product_code,
                "agreements": emp.get("agreements", []) or [],
            }
            if emp.get("is_export"):
                if result["export"] is None:
                    result["export"] = entry
            else:
                if result["import"] is None:
                    result["import"] = entry

        if result["import"] is None:
            warnings.append("no import MPAN found on property")
        if len(meters_with_multiple := [
                emp for emp in (prop.get("electricity_meter_points", []) or [])
                if len(emp.get("meters", []) or []) > 1]) > 0:
            warnings.append(
                "a meter point has multiple meters (e.g. after an exchange); "
                "using the first serial. Verify against your latest meter.")

        logger.info(
            "auto_discover: account=%s import_mpan=%s export_mpan=%s props=%d",
            _mask(acct_no),
            _mask(result["import"]["mpan"]) if result["import"] else "<none>",
            _mask(result["export"]["mpan"]) if result["export"] else "<none>",
            result["properties"])
        return result

    async def test_connection(self, account_number: Optional[str] = None) -> dict:
        """Lightweight credential check for the Settings 'Test' button.

        Returns {"ok": bool, "detail": str, "account_number": str|None}.
        Never raises — converts errors into a structured result so the UI can
        show a clean message.
        """
        try:
            account = await self.get_account(account_number)
            return {
                "ok": True,
                "detail": "connected",
                "account_number": account.get("number"),
            }
        except KrakenAuthError as e:
            return {"ok": False, "detail": "auth failed — check API key",
                    "account_number": None}
        except KrakenRateLimitError:
            return {"ok": False, "detail": "rate limited — try again shortly",
                    "account_number": None}
        except KrakenAPIError as e:
            return {"ok": False, "detail": str(e), "account_number": None}
        except Exception as e:  # noqa: BLE001 — UI must never see a stack trace
            return {"ok": False, "detail": f"unexpected error: {e}",
                    "account_number": None}

    # ── GraphQL ───────────────────────────────────────────────────────────
    async def _graphql(self, query: str, variables: Optional[dict] = None,
                       *, authenticated: bool = True) -> dict:
        """POST a GraphQL query/mutation with bounded exponential backoff.

        Retries the documented rate-limit code (KT-CT-1199), HTTP 429/5xx, and
        transient transport/timeout errors with exponential delays plus jitter,
        up to _GQL_MAX_RETRIES. Non-retryable errors (auth, other 4xx, GraphQL
        logic errors) propagate immediately. Returns the `data` object.
        """
        last_exc: Optional[Exception] = None
        for attempt in range(_GQL_MAX_RETRIES + 1):
            try:
                return await self._graphql_once(
                    query, variables, authenticated=authenticated)
            except KrakenRateLimitError as e:
                last_exc = e
            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                last_exc = e  # transient transport / timeout
            if attempt >= _GQL_MAX_RETRIES:
                break
            delay = min(_GQL_BACKOFF_CAP, _GQL_BACKOFF_BASE * (2 ** attempt))
            delay += random.uniform(0.0, delay * 0.25)  # jitter, avoid thundering herd
            logger.warning("GraphQL retry %d/%d in %.1fs after %s",
                           attempt + 1, _GQL_MAX_RETRIES, delay,
                           type(last_exc).__name__)
            await asyncio.sleep(delay)
        # Retries exhausted — surface a uniform error to callers.
        if isinstance(last_exc, KrakenRateLimitError):
            raise last_exc
        raise KrakenAPIError(
            f"GraphQL network error after retries: {last_exc}") from last_exc

    async def _graphql_once(self, query: str, variables: Optional[dict] = None,
                            *, authenticated: bool = True) -> dict:
        """A single GraphQL attempt (retry policy lives in _graphql).

        The Octopus GraphQL API always returns HTTP 200; errors arrive in the
        response `errors` array. We translate the documented rate-limit code
        (KT-CT-1199) and HTTP 429/5xx to KrakenRateLimitError (retryable), and
        any other error to KrakenAPIError. Transport/timeout errors propagate so
        the wrapper can retry them.
        """
        headers = {"Content-Type": "application/json"}
        if authenticated:
            token = await self._get_gql_token()
            # Authorization header uses the 'JWT ' prefix — confirmed by working
            # examples (Octopus mobile app + livehybrid gist both send
            # "authorization: JWT <token>"). The token alone is rejected.
            headers["Authorization"] = f"JWT {token}"
        session = self._ensure_session()
        body = {"query": query, "variables": variables or {}}
        # The session carries NO default auth (REST applies Basic per-request),
        # so GraphQL requests send only our explicit headers. Transport errors
        # (aiohttp.ClientError / asyncio.TimeoutError) are left to propagate.
        async with session.post(self.graphql_url, json=body,
                                headers=headers) as resp:
            # 200 expected even on error; still guard transport-level codes.
            if resp.status == 429 or resp.status >= 500:
                text = await resp.text()
                raise KrakenRateLimitError(
                    f"GraphQL retryable HTTP {resp.status}: {text[:200]}",
                    status=resp.status)
            if resp.status >= 400:
                text = await resp.text()
                raise KrakenAPIError(
                    f"GraphQL HTTP {resp.status}: {text[:200]}",
                    status=resp.status)
            payload = await resp.json()

        errors = payload.get("errors")
        if errors:
            codes = [self._error_code(e) for e in errors]
            if _GQL_RATE_LIMIT_CODE in codes:
                raise KrakenRateLimitError(
                    "GraphQL rate limit (KT-CT-1199)", status=429)
            # Schema-drift canary: a renamed/removed/disabled field is how a Kraken
            # API change first reaches us. Surface it LOUDLY and with a greppable
            # token so it stands out from generic "unavailable" lines — this turns
            # every running instance into a passive drift detector, no extra creds.
            drift = _schema_drift_errors(errors)
            if drift:
                logger.error(
                    "kraken_schema_drift: Octopus/Kraken rejected fields this query "
                    "depends on — a schema change has likely broken it. Check "
                    "https://developer.octopus.energy/announcements and migrate. "
                    "operation=%s errors=%s",
                    _operation_name(query),
                    [d.get("message") for d in drift])
            msg = "; ".join(str(e.get("message", e)) for e in errors)
            raise KrakenAPIError(f"GraphQL error: {msg}")
        return payload.get("data", {}) or {}

    @staticmethod
    def _error_code(err: dict) -> Optional[str]:
        """Extract a Kraken error code from a GraphQL error object.

        Code can appear as err['extensions']['errorCode'] or err['extensions']
        ['errorType']; tolerate both and a bare 'code'.
        """
        ext = err.get("extensions", {}) or {}
        return ext.get("errorCode") or ext.get("errorType") or err.get("code")

    async def _get_gql_token(self, *, force: bool = False) -> str:
        """Return a valid JWT, obtaining/refreshing via obtainKrakenToken.

        Cached and reused until within _TOKEN_REFRESH_SKEW seconds of its exp
        claim. Network-cheap: one mutation per ~hour of token lifetime.
        """
        import time
        if (not force and self._gql_token
                and self._gql_token_exp
                and time.time() < self._gql_token_exp - _TOKEN_REFRESH_SKEW):
            return self._gql_token

        # Use the typed-variable mutation form that Octopus's own example repo
        # (octoenergy/oejp-api-example) and working community integrations use:
        # the input is a single $input variable of type ObtainJSONWebTokenInput!,
        # with the API key under the 'APIKey' field. This is more robust than
        # inlining {APIKey: $apiKey} and matches the canonical shape.
        mutation = (
            "mutation getToken($input: ObtainJSONWebTokenInput!) {"
            "  obtainKrakenToken(input: $input) { token } }"
        )
        data = await self._graphql(
            mutation, {"input": {"APIKey": self._api_key}},
            authenticated=False)
        token = (data.get("obtainKrakenToken") or {}).get("token")
        if not token:
            raise KrakenAuthError("obtainKrakenToken returned no token")
        self._gql_token = token
        self._gql_token_exp = jwt_expires_at(token)
        logger.info("_get_gql_token: obtained token (exp=%s)",
                    self._gql_token_exp)
        return token

    async def get_device_id(self, account_number: Optional[str] = None
                            ) -> Optional[str]:
        """Discover the Octopus Mini deviceId via GraphQL, or None if absent.

        account → electricityAgreements(active) → meterPoint → meters →
        smartDevices → deviceId. Returns the first deviceId found.
        """
        acct = account_number or self.account_number
        if not acct:
            raise ValueError("account_number required for get_device_id")
        query = (
            "query devices($acc: String!) {"
            "  account(accountNumber: $acc) {"
            "    electricityAgreements(active: true) {"
            "      meterPoint { meters(includeInactive: false) {"
            "        smartDevices { deviceId } } } } } }"
        )
        data = await self._graphql(query, {"acc": acct})
        account = data.get("account") or {}
        for agr in account.get("electricityAgreements", []) or []:
            mp = agr.get("meterPoint") or {}
            for meter in mp.get("meters", []) or []:
                for dev in meter.get("smartDevices", []) or []:
                    did = dev.get("deviceId")
                    if did:
                        logger.info("get_device_id: found device %s",
                                    _mask(did))
                        return did
        # No device found. Log the shape we got (keys only, no values) so a
        # wrong query path is diagnosable from logs without leaking data — this
        # GraphQL surface is unverified against live hardware (open question Q4).
        try:
            agr_count = len(account.get("electricityAgreements") or [])
            logger.info("get_device_id: no smart device (Mini) found — "
                        "account keys=%s, electricityAgreements=%d",
                        sorted((account or {}).keys()), agr_count)
        except Exception:
            logger.info("get_device_id: no smart device (Mini) found on account")
        return None

    async def get_telemetry(
        self, device_id: str, start: str, end: str,
        *, grouping: str = "TEN_SECONDS",
    ) -> list[dict]:
        """smartMeterTelemetry for a Mini device over [start, end].

        Returns the list of telemetry points; each dict has (per the schema):
        readAt, demand, consumption, consumptionDelta, costDelta. The Mini
        boundary logic (Chunk 7) picks the reading nearest the boundary.
        """
        query = (
            "query telemetry($id: String!, $start: DateTime!, $end: DateTime!, "
            "$grouping: TelemetryGrouping!) {"
            "  smartMeterTelemetry(deviceId: $id, grouping: $grouping, "
            "    start: $start, end: $end) {"
            "    readAt demand consumption consumptionDelta costDelta } }"
        )
        data = await self._graphql(query, {
            "id": device_id, "start": start, "end": end, "grouping": grouping})
        return data.get("smartMeterTelemetry", []) or []

    async def check_field_deprecations(self) -> None:
        """Introspect the live schema and WARN for any field/enum EMT uses that
        Kraken has flagged deprecated — the grace-window warning BEFORE removal.

        Pull-not-push: this replaces relying on the (unreliable) developer-portal
        notification email. A deprecated field still WORKS; the flag is the cue to
        migrate before its removal date (often in deprecationReason). Complements
        the runtime drift logging, which only fires once a field is actually gone.

        Fail-safe: introspection is frequently DISABLED on production GraphQL
        endpoints. On any error (disabled, auth, transport) this logs once at INFO
        and returns — it must never break the dispatch poll it rides along with.
        """
        # Standard introspection: every type's fields + enum values, deprecated
        # ones included (they are hidden by default).
        query = (
            "query introspectDeprecations {"
            "  __schema { types {"
            "    name"
            "    fields(includeDeprecated: true) {"
            "      name isDeprecated deprecationReason }"
            "    enumValues(includeDeprecated: true) {"
            "      name isDeprecated deprecationReason }"
            "  } } }"
        )
        try:
            data = await self._graphql(query)
        except Exception as e:  # disabled / auth / transport — all non-fatal
            logger.info(
                "kraken_deprecation_check: introspection unavailable (%s: %s) — "
                "relying on runtime drift detection instead",
                type(e).__name__, str(e)[:120])
            return

        types = ((data or {}).get("__schema") or {}).get("types") or []
        hits = []  # list of {kind, type, name, reason}
        ignored = 0
        for t in types:
            tname = t.get("name") or "?"
            for f in (t.get("fields") or []):
                if f.get("isDeprecated") and f.get("name") in _EMT_GRAPHQL_FIELDS:
                    if (tname, f.get("name")) in _DEPRECATION_IGNORE:
                        ignored += 1
                        logger.debug("kraken_deprecation_check: ignoring known "
                                     "name-collision %s.%s", tname, f.get("name"))
                        continue
                    hits.append({"kind": "field", "type": tname,
                                 "name": f.get("name"),
                                 "reason": f.get("deprecationReason")})
            for ev in (t.get("enumValues") or []):
                if ev.get("isDeprecated") and ev.get("name") in _EMT_GRAPHQL_ENUMS:
                    if (tname, ev.get("name")) in _DEPRECATION_IGNORE:
                        ignored += 1
                        continue
                    hits.append({"kind": "enum", "type": tname,
                                 "name": ev.get("name"),
                                 "reason": ev.get("deprecationReason")})

        # Publish for the engine to surface (HA notification + sensor). None means
        # "never successfully checked"; [] means "checked, all clear".
        self.last_deprecations = hits
        _ign = f" ({ignored} known name-collision(s) ignored)" if ignored else ""
        if not hits:
            logger.info(
                "kraken_deprecation_check: no deprecations among the %d fields / "
                "%d enum values EMT uses%s",
                len(_EMT_GRAPHQL_FIELDS), len(_EMT_GRAPHQL_ENUMS), _ign)
            return
        logger.warning("kraken_deprecation_check: %d deprecated field(s) EMT "
                       "uses%s", len(hits), _ign)
        for h in hits:
            logger.warning(
                "kraken_field_deprecated: %s %s.%s is deprecated — migrate "
                "before it is removed. reason=%s",
                h["kind"], h["type"], h["name"], h["reason"] or "(no reason given)")

    async def get_dispatches(self, account_number: Optional[str] = None
                             ) -> Optional[dict]:
        """Fetch Intelligent dispatch data for the account, or None if absent.

        Returns a dict:
            {
              "provider": str | None,        # e.g. 'TESLA', 'OHME', 'MYENERGI'
              "planned":   [ {start, end, source?, type?, meta?}, ... ],
              "completed": [ {start, end, delta?, meta?}, ... ],
            }

        Step 1 of the dispatch overlay is OBSERVE-ONLY: this method fetches and
        the caller logs; NO rate changes are made. The 'provider' drives the
        per-provider branch (planned-dispatch providers like Tesla/myenergi Zappi
        vs OHME) once the overlay is built.

        This GraphQL surface is UNVERIFIED against live hardware — on an empty or
        unexpected response we log the shape (keys only) so the real schema can be
        confirmed from logs without leaking data, mirroring get_device_id.
        """
        acct = account_number or self.account_number
        if not acct:
            raise ValueError("account_number required for get_dispatches")
        # Once per process, ride this first authenticated poll to introspect the
        # schema for deprecations among the fields EMT uses (no extra key, no CI,
        # no engine.py wiring). Guarded + fail-safe so it can never affect the
        # dispatch fetch below.
        if not self._deprecation_checked:
            self._deprecation_checked = True
            try:
                await self.check_field_deprecations()
            except Exception as e:
                logger.info("kraken_deprecation_check: skipped (%s: %s)",
                            type(e).__name__, str(e)[:120])
        # plannedDispatches / completedDispatches and the registered device
        # (which carries the provider) hang off the account. Field names per the
        # documented Kraken intelligent surface; verify live via the shape log.
        # Field names confirmed against the live Octopus app query + API docs:
        #  - dispatch slots use startDt/endDt (not start/end), plus delta and
        #    meta { location source }. meta.source now returns
        #    smart-charge/test-charge/bump-charge (or null/unknown) on PLANNED
        #    dispatches — the smart-vs-boost signal (incl. for OHME bump).
        #  - provider comes from the `devices` query (polymorphic list; we read
        #    the interface-level provider/deviceType/status{current}). This
        #    REPLACES the deprecated registeredKrakenflexDevice, whose removal
        #    Kraken announced for on/after 2026-03-01. Shape confirmed against the
        #    Octopus GraphQL reference + the BottlecapDave integration's live query.
        query = (
            "query dispatches($acc: String!) {"
            "  plannedDispatches(accountNumber: $acc) {"
            "    startDt endDt delta meta { source location } }"
            "  completedDispatches(accountNumber: $acc) {"
            "    startDt endDt delta meta { source location } }"
            "  devices(accountNumber: $acc) {"
            "    provider deviceType status { current } __typename"
            "    ... on SmartFlexVehicle { make model }"
            "    ... on SmartFlexChargePoint { make model } } }"
        )
        try:
            data = await self._graphql(query, {"acc": acct})
        except KrakenAPIError as e:
            logger.warning("get_dispatches: unavailable (%s)", e)
            return None

        planned_raw = data.get("plannedDispatches") or []
        completed_raw = data.get("completedDispatches") or []
        devices = data.get("devices") or []
        # Diagnostic: log the devices list (categories/brands only — no IDs/PII) so
        # the chosen charging device + provider signal is verifiable from logs while
        # this migration beds in. provider is the flex provider (e.g. OCTOPUS_ENERGY);
        # make is the manufacturer (MYENERGI/OHME/…), which is what we key on.
        if devices:
            logger.info("get_dispatches: devices=%s", [
                {"type": d.get("deviceType"), "make": d.get("make"),
                 "provider": d.get("provider"), "kind": d.get("__typename"),
                 "status": (d.get("status") or {}).get("current")}
                for d in devices if d])
        # devices replaced the deprecated registeredKrakenflexDevice; pick the
        # smart-charging device's manufacturer signal (make, then provider).
        provider = _pick_device_provider(devices)

        def _norm(d: dict) -> dict:
            # Normalise to stable keys: the API uses startDt/endDt; meta.source
            # carries smart-charge/test-charge/bump-charge (or null/unknown).
            meta = d.get("meta") or {}
            return {
                "start":  d.get("startDt") or d.get("start"),
                "end":    d.get("endDt") or d.get("end"),
                "delta":  d.get("delta"),
                "source": meta.get("source"),
                "location": meta.get("location"),
            }

        planned = [_norm(d) for d in planned_raw]
        completed = [_norm(d) for d in completed_raw]

        if not planned and not completed and not provider:
            # Nothing came back — log the shape (keys only) for schema diagnosis.
            # NOTE: empty planned dispatches BEFORE ~5PM is NORMAL (Octopus only
            # populates upcoming slots after the daily schedule is computed), and
            # only when a vehicle is plugged in — so an empty result is not
            # necessarily an error.
            try:
                logger.info("get_dispatches: empty result — response keys=%s",
                            sorted((data or {}).keys()))
            except Exception:
                logger.info("get_dispatches: empty result")
            return {"provider": provider, "planned": [], "completed": []}

        logger.info("get_dispatches: provider=%s planned=%d completed=%d",
                    provider, len(planned), len(completed))
        return {"provider": provider, "planned": planned, "completed": completed}

    async def get_rate_limit(self) -> Optional[dict]:
        """rateLimitInfo — points used / limit. Returns None if the field is
        unavailable on this account (field names per spec; verify live).

        Per spec: back off when remaining < 20.
        """
        query = "query { rateLimitInfo { pointsUsed pointsLimit } }"
        try:
            data = await self._graphql(query)
        except KrakenAPIError as e:
            logger.warning("get_rate_limit: unavailable (%s)", e)
            return None
        info = data.get("rateLimitInfo")
        if not info:
            return None
        used = info.get("pointsUsed")
        limit = info.get("pointsLimit")
        remaining = (limit - used) if (used is not None and limit is not None) \
            else None
        return {"pointsUsed": used, "pointsLimit": limit, "remaining": remaining}


# ── JWT helper (used by GraphQL token handling in Chunk 3b; defined here so
#    the decode logic lives with the client) ──────────────────────────────────
def jwt_expires_at(token: str) -> Optional[float]:
    """Return the `exp` (epoch seconds) from a JWT without verifying signature.

    Returns None if the token is malformed or has no exp claim. Per the spec's
    decode recipe, with base64 padding restored.
    """
    try:
        payload = token.split(".")[1]
        payload += "=" * (-len(payload) % 4)
        claims = __import__("json").loads(base64.urlsafe_b64decode(payload))
        exp = claims.get("exp")
        return float(exp) if exp is not None else None
    except Exception:
        return None


# ── BottlecapDave (HomeAssistant-OctopusEnergy) detection (Chunk 3c) ──────────
import re as _re

# BCD electricity entity id:
#   sensor.octopus_energy_electricity_{SERIAL}_{MPAN}_<suffix>
# Export sensors carry '_export_' in the suffix region. The account id is not in
# the id but appears in entity attributes. MPAN is 13 digits; serial is
# alphanumeric. We anchor on the documented prefix and pull serial + mpan.
_BCD_ELEC_RE = _re.compile(
    r"^sensor\.octopus_energy_electricity_"
    r"(?P<serial>[A-Za-z0-9]+)_"
    r"(?P<mpan>\d{8,15})_"
    r"(?P<suffix>.+)$"
)
# Presence of these suffixes signals an Octopus Mini/Pro is configured (live
# accumulative data only exists then) — used to suggest api+mini is available.
_BCD_MINI_SUFFIXES = (
    "current_accumulative_consumption",
    "current_demand",
    "data_last_retrieved",
)


def detect_bottlecapdave(states: list[dict]) -> dict:
    """Inspect a HA states list for BottlecapDave Octopus entities.

    Pure function: takes the output of ha_client.get_all_states() and returns
    what EMT can pre-populate, so the Settings UI doesn't require hand-typing.

    Returns:
        {
          "found": bool,                 # any BCD electricity entity present
          "account_number": str | None,  # from entity attributes if exposed
          "import": {"mpan","serial"} | None,
          "export": {"mpan","serial"} | None,
          "mini_available": bool,        # Mini/Pro live sensors present
          "entity_count": int,
        }

    Detection is by entity_id pattern (stable) cross-checked with attributes
    (best-effort, since many are runtime-populated). Import vs export is decided
    by the '_export_' marker in the suffix, NOT by attributes.
    """
    result: dict[str, Any] = {
        "found": False,
        "account_number": None,
        "import": None,
        "export": None,
        "mini_available": False,
        "entity_count": 0,
        # Sensor entity IDs the wizard can pre-fill (import side). Best-effort:
        # populated when BCD exposes the corresponding sensor.
        "rate_sensor": None,
        "standing_charge_sensor": None,
        "export_rate_sensor": None,
        # Live demand sensor (W) exposed when a Mini/Pro is present — the wizard
        # pre-fills the optional Live Power field with this so EMT can read live
        # power off BCD instead of polling the Mini itself (detection offload).
        "demand_sensor": None,
    }
    if not states:
        return result

    for s in states:
        eid = s.get("entity_id", "") or ""
        m = _BCD_ELEC_RE.match(eid)
        if not m:
            continue
        result["found"] = True
        result["entity_count"] += 1
        serial = m.group("serial")
        mpan = m.group("mpan")
        suffix = m.group("suffix")
        attrs = s.get("attributes", {}) or {}

        # Account number — exposed as an attribute on most BCD sensors.
        if result["account_number"] is None:
            acc = (attrs.get("account_id") or attrs.get("account_number"))
            if acc:
                result["account_number"] = str(acc)

        # Rate / standing-charge sensors — pre-fillable in the wizard. BCD names
        # them with stable suffixes. Export rate carries the '_export_' marker.
        is_export = "export" in suffix
        if suffix.endswith("current_rate"):
            if is_export:
                result["export_rate_sensor"] = result["export_rate_sensor"] or eid
            else:
                result["rate_sensor"] = result["rate_sensor"] or eid
        elif suffix.endswith("current_standing_charge") and not is_export:
            result["standing_charge_sensor"] = result["standing_charge_sensor"] or eid

        # Mini/Pro live sensors → near-real-time import available.
        if any(suffix.startswith(ms) or suffix == ms
               for ms in _BCD_MINI_SUFFIXES):
            result["mini_available"] = True

        # Live demand (W) — the wizard pre-fills the Live Power field with this.
        # Import side only (no '_export_' marker); first match wins.
        if suffix.endswith("current_demand") and not is_export:
            result["demand_sensor"] = result["demand_sensor"] or eid

        entry = {"mpan": mpan, "serial": serial}
        if is_export:
            if result["export"] is None:
                result["export"] = entry
        else:
            # Prefer an attribute confirmation of import when present, but the
            # absence of '_export_' is sufficient to classify as import.
            if result["import"] is None:
                result["import"] = entry

    if result["found"]:
        logger.info(
            "detect_bottlecapdave: found=%d import_mpan=%s export_mpan=%s "
            "mini=%s account=%s",
            result["entity_count"],
            _mask(result["import"]["mpan"]) if result["import"] else "<none>",
            _mask(result["export"]["mpan"]) if result["export"] else "<none>",
            result["mini_available"],
            _mask(result["account_number"]))
    return result

# ── OHME charger detection (charge-mode smart-vs-boost signal) ────────────────
# OHME is structurally different from Zappi: Ohme controls the charge, not
# Octopus, so Octopus dispatch data can't reliably separate a genuine smart
# charge from a user boost (completed dispatches report source=null in practice).
# Two HA integrations expose an OHME signal that CAN, which EMT consults to turn
# the optimistic off-peak default into a VERIFIED one when present:
#   - Official `ohme` integration: a charge-mode SELECT whose state is one of
#     {"Smart charge","Max charge","Paused"} ("Max charge" = boost/full-speed).
#   - Unofficial dan-r/HomeAssistant-Ohme: a "Charge Slot Active" binary_sensor,
#     on while a (smart) charge slot is in progress — mimics an Octopus dispatch.
_OHME_MODE_VALUES  = {"smart charge", "max charge", "paused"}
_OHME_BOOST_VALUES = {"max charge"}


def detect_ohme_charge_mode(states: list[dict]) -> dict:
    """Inspect a HA states list for an OHME charge-mode signal.

    Pure function over ha_client.get_all_states(). Drives the VERIFIED OHME
    smart-vs-boost path (and the config-state line recording which path is live).
    The signal is real-time charge STATE, independent of the Octopus planned
    superset, so it closes BOTH OHME residuals the dispatch feed can't: boost
    mislabelled off-peak, and a smart charge outside the captured superset.

    Returns:
        {
          "found": bool,                  # any OHME charge-mode signal present
          "integration": "official"|"danr"|None,
          "charge_mode_entity": str|None, # the select / binary_sensor entity_id
          "mode": str|None,               # current raw state
          "is_boost": bool|None,          # True=boost(Max); False=smart; None=unknown
          "status_entity": str|None,      # official Status sensor if present
        }

    Detection is by entity_id pattern cross-checked with the select's state.
    Conservative: returns found=False rather than guess. The official select
    wins over the dan-r binary when both are present (it distinguishes boost
    directly; the dan-r binary only signals slot-active).
    """
    result: dict[str, Any] = {
        "found": False, "integration": None, "charge_mode_entity": None,
        "mode": None, "is_boost": None, "status_entity": None,
    }
    if not states:
        return result
    for s in states:
        eid = (s.get("entity_id", "") or "")
        leid = eid.lower()
        state = s.get("state", "") or ""
        sl = state.strip().lower()
        if "ohme" not in leid:
            continue
        # Official ohme: a charge-mode SELECT (or any select reporting a known
        # charge-mode value). Distinguishes smart vs boost directly.
        if leid.startswith("select.") and (
                ("charge" in leid and "mode" in leid) or sl in _OHME_MODE_VALUES):
            result["found"] = True
            result["integration"] = "official"
            result["charge_mode_entity"] = eid
            result["mode"] = state
            result["is_boost"] = (sl in _OHME_BOOST_VALUES) if sl in _OHME_MODE_VALUES else None
        # Official Status sensor — informational only.
        elif (leid.startswith("sensor.") and leid.endswith("status")
              and result["status_entity"] is None):
            result["status_entity"] = eid
        # dan-r unofficial: "Charge Slot Active" binary sensor. Signals an active
        # smart slot (on); it does NOT itself flag boost, so is_boost stays None —
        # the overlay infers boost from slot-off-with-draw. Yield to the official
        # select if that was/will be found.
        elif (leid.startswith("binary_sensor.")
              and ("charge_slot_active" in leid or "slot_active" in leid)):
            if result["integration"] != "official":
                result["found"] = True
                result["integration"] = "danr"
                result["charge_mode_entity"] = eid
                result["mode"] = state
                result["is_boost"] = None
    return result