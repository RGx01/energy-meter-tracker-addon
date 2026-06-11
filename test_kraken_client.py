"""
Tests for kraken_api_client.py (Chunk 3a) — mocked HTTP, no network.

A FakeSession stands in for aiohttp.ClientSession: it returns queued JSON
payloads (or raises) per URL, and records the requests made so we can assert
auth/paths. Fixtures use the documented public API shapes.
"""

import asyncio
import base64
import json
import unittest
from unittest.mock import patch, AsyncMock

import kraken_api_client as kc
from kraken_api_client import (
    KrakenAPIClient, KrakenAuthError, KrakenRateLimitError, KrakenAPIError,
    jwt_expires_at,
)


def run(coro):
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


# ── documented account fixture (single property, import + export MPAN) ───────
ACCOUNT_JSON = {
    "number": "A-ABCD1234",
    "properties": [{
        "id": 1234567,
        "moved_in_at": "2020-11-30T00:00:00Z",
        "moved_out_at": None,
        "postcode": "W1 1AA",
        "electricity_meter_points": [
            {
                "mpan": "1000000000001",
                "is_export": False,
                "meters": [{"serial_number": "11111111"}],
                "agreements": [
                    {"tariff_code": "E-1R-VAR-20-09-22-N",
                     "valid_from": "2020-12-17T00:00:00Z",
                     "valid_to": "2023-04-01T00:00:00+01:00"},
                    {"tariff_code": "E-1R-AGILE-FLEX-22-11-25-A",
                     "valid_from": "2023-04-01T00:00:00+01:00",
                     "valid_to": None},
                ],
            },
            {
                "mpan": "2000000000002",
                "is_export": True,
                "meters": [{"serial_number": "22222222"}],
                "agreements": [
                    {"tariff_code": "E-1R-OUTGOING-FIX-12M-19-05-13-A",
                     "valid_from": "2023-04-01T00:00:00+01:00",
                     "valid_to": None},
                ],
            },
        ],
    }],
}


class FakeResp:
    def __init__(self, status=200, payload=None, text=""):
        self.status = status
        self._payload = payload if payload is not None else {}
        self._text = text

    async def json(self):
        return self._payload

    async def text(self):
        return self._text

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False


class FakeSession:
    """Minimal aiohttp.ClientSession stand-in.

    routes: dict mapping a URL substring → FakeResp (or a callable returning
    one). The first matching substring wins. Records (url, params) on .calls.

    gql_responses: optional list of FakeResp returned in order from post()
    (GraphQL). Each post records (url, json, headers) on .post_calls.
    """

    def __init__(self, routes=None, auth=None, gql_responses=None):
        self.routes = routes or {}
        self.auth = auth
        self.calls = []
        self.post_calls = []
        self.closed = False
        self._gql = list(gql_responses or [])

    def get(self, url, params=None, auth=None):
        self.calls.append((url, params, auth))
        for needle, resp in self.routes.items():
            if needle in url:
                return resp(url, params) if callable(resp) else resp
        return FakeResp(404, {}, "no route")

    def post(self, url, json=None, headers=None, auth=None):
        self.post_calls.append((url, json, headers, auth))
        if self._gql:
            return self._gql.pop(0)
        return FakeResp(200, {"data": {}})

    async def close(self):
        self.closed = True


class TestJwt(unittest.TestCase):
    def test_decode(self):
        tok = "h." + base64.urlsafe_b64encode(
            json.dumps({"exp": 1700000000}).encode()).decode().rstrip("=") + ".s"
        self.assertEqual(jwt_expires_at(tok), 1700000000.0)

    def test_malformed(self):
        self.assertIsNone(jwt_expires_at("nope"))
        self.assertIsNone(jwt_expires_at(""))

    def test_no_exp_claim(self):
        tok = "h." + base64.urlsafe_b64encode(
            json.dumps({"sub": "x"}).encode()).decode().rstrip("=") + ".s"
        self.assertIsNone(jwt_expires_at(tok))


class TestTariffParsing(unittest.TestCase):
    def test_agile(self):
        self.assertEqual(
            KrakenAPIClient._tariff_to_product_code("E-1R-AGILE-FLEX-22-11-25-A"),
            "AGILE-FLEX-22-11-25")

    def test_var(self):
        self.assertEqual(
            KrakenAPIClient._tariff_to_product_code("E-1R-VAR-22-11-01-N"),
            "VAR-22-11-01")

    def test_too_short(self):
        self.assertIsNone(KrakenAPIClient._tariff_to_product_code("E-1R"))

    def test_current_agreement_picks_live(self):
        live = KrakenAPIClient._current_agreement(
            ACCOUNT_JSON["properties"][0]["electricity_meter_points"][0]["agreements"])
        self.assertEqual(live["tariff_code"], "E-1R-AGILE-FLEX-22-11-25-A")

    def test_current_agreement_empty(self):
        self.assertIsNone(KrakenAPIClient._current_agreement([]))


class TestMasking(unittest.TestCase):
    def test_mask(self):
        self.assertEqual(kc._mask("1000000000001"), "1…1")
        self.assertEqual(kc._mask("A-42B0BCA7"), "A…7")
        self.assertEqual(kc._mask(None), "<none>")
        self.assertEqual(kc._mask("ab"), "…")


class TestClientRequests(unittest.TestCase):
    def _client(self, routes):
        c = KrakenAPIClient("test-key", "A-ABCD1234")
        c._session = FakeSession(routes)
        c._owns_session = True
        return c

    def test_requires_api_key(self):
        with self.assertRaises(ValueError):
            KrakenAPIClient("")

    def test_get_account(self):
        c = self._client({"/v1/accounts/": FakeResp(200, ACCOUNT_JSON)})
        acct = run(c.get_account())
        self.assertEqual(acct["number"], "A-ABCD1234")
        # path included the account number
        self.assertIn("A-ABCD1234", c._session.calls[0][0])
        # REST carries Basic auth per-request (API key as username).
        rest_auth = c._session.calls[0][2]
        self.assertIsNotNone(rest_auth)
        self.assertEqual(rest_auth.login, c._api_key)

    def test_auth_error(self):
        c = self._client({"/v1/accounts/": FakeResp(401, {}, "unauthorized")})
        with self.assertRaises(KrakenAuthError):
            run(c.get_account())

    def test_rate_limit_error(self):
        c = self._client({"/v1/accounts/": FakeResp(429, {}, "slow down")})
        with self.assertRaises(KrakenRateLimitError):
            run(c.get_account())

    def test_generic_http_error(self):
        c = self._client({"/v1/accounts/": FakeResp(500, {}, "boom")})
        with self.assertRaises(KrakenAPIError):
            run(c.get_account())

    def test_get_consumption_paginates(self):
        page2 = {"results": [{"consumption": 0.2,
                              "interval_start": "2026-05-01T00:30:00Z",
                              "interval_end": "2026-05-01T01:00:00Z"}],
                 "next": None}
        page1 = {"results": [{"consumption": 0.1,
                              "interval_start": "2026-05-01T00:00:00Z",
                              "interval_end": "2026-05-01T00:30:00Z"}],
                 "next": "https://api.octopus.energy/next-page"}

        def route(url, params):
            return FakeResp(200, page2 if "next-page" in url else page1)

        c = self._client({"/v1/electricity-meter-points/": route,
                          "next-page": route})
        rows = run(c.get_consumption("1000000000001", "11111111",
                                     period_from="2026-05-01T00:00:00Z"))
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["consumption"], 0.1)
        self.assertEqual(rows[1]["consumption"], 0.2)

    def test_get_unit_rates(self):
        payload = {"results": [{"value_inc_vat": 24.5,
                                "valid_from": "2026-05-01T00:00:00Z",
                                "valid_to": None}], "next": None}
        c = self._client({"/standard-unit-rates/": FakeResp(200, payload)})
        rates = run(c.get_unit_rates("AGILE-FLEX-22-11-25",
                                     "E-1R-AGILE-FLEX-22-11-25-A"))
        self.assertEqual(rates[0]["value_inc_vat"], 24.5)

    def test_get_standing_charges(self):
        payload = {"results": [{"value_inc_vat": 47.85,
                                "valid_from": "2026-05-01T00:00:00Z",
                                "valid_to": None}], "next": None}
        c = self._client({"/standing-charges/": FakeResp(200, payload)})
        sc = run(c.get_standing_charges("AGILE-FLEX-22-11-25",
                                        "E-1R-AGILE-FLEX-22-11-25-A"))
        self.assertEqual(sc[0]["value_inc_vat"], 47.85)


class TestAutoDiscover(unittest.TestCase):
    def _client(self, account_json):
        c = KrakenAPIClient("test-key", "A-ABCD1234")
        c._session = FakeSession({"/v1/accounts/": FakeResp(200, account_json)})
        c._owns_session = True
        return c

    def test_import_and_export_identified(self):
        c = self._client(ACCOUNT_JSON)
        d = run(c.auto_discover())
        self.assertEqual(d["account_number"], "A-ABCD1234")
        self.assertEqual(d["import"]["mpan"], "1000000000001")
        self.assertEqual(d["import"]["serial"], "11111111")
        self.assertEqual(d["import"]["product_code"], "AGILE-FLEX-22-11-25")
        self.assertEqual(d["export"]["mpan"], "2000000000002")
        self.assertEqual(d["properties"], 1)
        self.assertEqual(d["warnings"], [])

    def test_multi_property_warns(self):
        acct = json.loads(json.dumps(ACCOUNT_JSON))
        second = json.loads(json.dumps(acct["properties"][0]))
        second["id"] = 9999999
        acct["properties"].append(second)
        c = self._client(acct)
        d = run(c.auto_discover())
        self.assertEqual(d["properties"], 2)
        self.assertTrue(any("single" in w for w in d["warnings"]))

    def test_no_export_mpan(self):
        acct = json.loads(json.dumps(ACCOUNT_JSON))
        acct["properties"][0]["electricity_meter_points"] = [
            acct["properties"][0]["electricity_meter_points"][0]]
        c = self._client(acct)
        d = run(c.auto_discover())
        self.assertIsNone(d["export"])
        self.assertIsNotNone(d["import"])

    def test_moved_out_property_skipped(self):
        acct = json.loads(json.dumps(ACCOUNT_JSON))
        acct["properties"][0]["moved_out_at"] = "2024-01-01T00:00:00Z"
        # add an active property
        active = json.loads(json.dumps(ACCOUNT_JSON["properties"][0]))
        active["moved_out_at"] = None
        active["electricity_meter_points"][0]["mpan"] = "3000000000003"
        acct["properties"].append(active)
        c = self._client(acct)
        d = run(c.auto_discover())
        self.assertEqual(d["import"]["mpan"], "3000000000003")
        self.assertEqual(d["properties"], 1)


class TestTestConnection(unittest.TestCase):
    def _client(self, resp):
        c = KrakenAPIClient("test-key", "A-ABCD1234")
        c._session = FakeSession({"/v1/accounts/": resp})
        c._owns_session = True
        return c

    def test_ok(self):
        r = run(self._client(FakeResp(200, ACCOUNT_JSON)).test_connection())
        self.assertTrue(r["ok"])
        self.assertEqual(r["account_number"], "A-ABCD1234")

    def test_auth_fail(self):
        r = run(self._client(FakeResp(401, {}, "no")).test_connection())
        self.assertFalse(r["ok"])
        self.assertIn("auth", r["detail"])

    def test_rate_limited(self):
        r = run(self._client(FakeResp(429, {}, "no")).test_connection())
        self.assertFalse(r["ok"])
        self.assertIn("rate", r["detail"])


def _make_jwt(exp):
    """Build an unsigned JWT-shaped string carrying the given exp claim."""
    payload = base64.urlsafe_b64encode(
        json.dumps({"exp": exp}).encode()).decode().rstrip("=")
    return f"header.{payload}.sig"


def _gql_data(data):
    return FakeResp(200, {"data": data})


def _gql_errors(errors):
    return FakeResp(200, {"errors": errors})


class TestGraphQLToken(unittest.TestCase):
    def _client(self, gql_responses):
        c = KrakenAPIClient("test-key", "A-ABCD1234")
        c._session = FakeSession(gql_responses=gql_responses)
        c._owns_session = True
        return c

    def test_obtain_and_cache(self):
        import time
        tok = _make_jwt(time.time() + 3600)
        c = self._client([_gql_data({"obtainKrakenToken": {"token": tok}})])
        t1 = run(c._get_gql_token())
        self.assertEqual(t1, tok)
        # The token mutation uses the typed-variable form with the API key under
        # the 'APIKey' field of the $input variable (matches Octopus's own
        # example repo). Verify both the query shape and the variable.
        _url, body, headers, auth = c._session.post_calls[0]
        self.assertIn("ObtainJSONWebTokenInput", body["query"])
        self.assertEqual(body["variables"]["input"]["APIKey"], c._api_key)
        # The token mutation must carry NO auth — neither Basic (which would
        # leak from a session-level default and is rejected by GraphQL) nor an
        # Authorization header. This was the root cause of the persistent
        # KT-CT-1143 failures.
        self.assertIsNone(auth)
        self.assertNotIn("Authorization", headers or {})
        # Second call uses cache — no further POST.
        t2 = run(c._get_gql_token())
        self.assertEqual(t2, tok)
        self.assertEqual(len(c._session.post_calls), 1)

    def test_refresh_when_expired(self):
        import time
        old = _make_jwt(time.time() + 10)   # within skew → must refresh
        new = _make_jwt(time.time() + 3600)
        c = self._client([
            _gql_data({"obtainKrakenToken": {"token": old}}),
            _gql_data({"obtainKrakenToken": {"token": new}}),
        ])
        run(c._get_gql_token())
        t2 = run(c._get_gql_token())
        self.assertEqual(t2, new)
        self.assertEqual(len(c._session.post_calls), 2)

    def test_token_request_is_unauthenticated(self):
        import time
        tok = _make_jwt(time.time() + 3600)
        c = self._client([_gql_data({"obtainKrakenToken": {"token": tok}})])
        run(c._get_gql_token())
        # The obtain-token POST must NOT carry an Authorization header.
        _, _, headers, _auth = c._session.post_calls[0]
        self.assertNotIn("Authorization", headers)

    def test_no_token_raises(self):
        c = self._client([_gql_data({"obtainKrakenToken": {}})])
        with self.assertRaises(KrakenAuthError):
            run(c._get_gql_token())


class TestGraphQLQueries(unittest.TestCase):
    def _client(self, gql_responses):
        import time
        c = KrakenAPIClient("test-key", "A-ABCD1234")
        c._session = FakeSession(gql_responses=gql_responses)
        c._owns_session = True
        # Pre-seed a valid cached token so query POSTs skip the token mutation.
        c._gql_token = _make_jwt(time.time() + 3600)
        c._gql_token_exp = time.time() + 3600
        return c

    def test_get_device_id(self):
        data = {"account": {"electricityAgreements": [
            {"meterPoint": {"meters": [
                {"smartDevices": [{"deviceId": "abc-123-device"}]}]}}]}}
        c = self._client([_gql_data(data)])
        did = run(c.get_device_id())
        self.assertEqual(did, "abc-123-device")
        # Authenticated query carries the 'JWT ' prefix (per working examples).
        _, _, headers, _auth = c._session.post_calls[0]
        self.assertEqual(headers["Authorization"], f"JWT {c._gql_token}")

    def test_get_device_id_none_when_no_mini(self):
        data = {"account": {"electricityAgreements": [
            {"meterPoint": {"meters": [{"smartDevices": []}]}}]}}
        c = self._client([_gql_data(data)])
        self.assertIsNone(run(c.get_device_id()))

    def test_get_telemetry(self):
        data = {"smartMeterTelemetry": [
            {"readAt": "2026-05-01T00:00:00Z", "demand": -250,
             "consumption": 1234567, "consumptionDelta": 0.0,
             "costDelta": 0.0}]}
        c = self._client([_gql_data(data)])
        rows = run(c.get_telemetry("abc-123", "2026-05-01T00:00:00Z",
                                   "2026-05-01T00:30:00Z"))
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["demand"], -250)
        self.assertEqual(rows[0]["consumption"], 1234567)

    def test_rate_limit_info(self):
        c = self._client([_gql_data({"rateLimitInfo":
                                     {"pointsUsed": 80, "pointsLimit": 100}})])
        info = run(c.get_rate_limit())
        self.assertEqual(info["remaining"], 20)

    def test_rate_limit_info_absent(self):
        c = self._client([_gql_data({})])
        self.assertIsNone(run(c.get_rate_limit()))

    def test_get_dispatches_normalises_fields(self):
        # API returns startDt/endDt + meta.source; we normalise to start/end/source.
        data = {
            "plannedDispatches": [
                {"startDt": "2026-06-06T10:00:00Z", "endDt": "2026-06-06T10:30:00Z",
                 "delta": "-1.5", "meta": {"source": "smart-charge",
                                           "location": "AT_HOME"}}],
            "completedDispatches": [
                {"startDt": "2026-06-06T02:00:00Z", "endDt": "2026-06-06T02:30:00Z",
                 "delta": "-2.9", "meta": {"source": None, "location": "AT_HOME"}}],
            "registeredKrakenflexDevice": {"provider": "MYENERGI", "status": "Live"},
        }
        c = self._client([_gql_data(data)])
        res = run(c.get_dispatches("A-123"))
        self.assertEqual(res["provider"], "MYENERGI")
        self.assertEqual(len(res["planned"]), 1)
        p = res["planned"][0]
        self.assertEqual(p["start"], "2026-06-06T10:00:00Z")
        self.assertEqual(p["end"], "2026-06-06T10:30:00Z")
        self.assertEqual(p["source"], "smart-charge")
        self.assertEqual(res["completed"][0]["source"], None)

    def test_get_dispatches_empty(self):
        c = self._client([_gql_data({"plannedDispatches": [],
                                     "completedDispatches": [],
                                     "registeredKrakenflexDevice": None})])
        res = run(c.get_dispatches("A-123"))
        self.assertEqual(res["planned"], [])
        self.assertEqual(res["completed"], [])
        self.assertIsNone(res["provider"])


    def test_graphql_rate_limit_error(self):
        # Retries exhausted → the rate-limit error finally surfaces.
        errs = [_gql_errors([{"message": "Too many requests",
                              "extensions": {"errorCode": "KT-CT-1199"}}])
                for _ in range(kc._GQL_MAX_RETRIES + 1)]
        c = self._client(errs)
        with patch("kraken_api_client.asyncio.sleep", new=AsyncMock()) as slept:
            with self.assertRaises(KrakenRateLimitError):
                run(c._graphql("query { x }", authenticated=False))
        self.assertEqual(slept.await_count, kc._GQL_MAX_RETRIES)        # slept each retry
        self.assertEqual(len(c._session.post_calls), kc._GQL_MAX_RETRIES + 1)

    def test_graphql_retries_then_succeeds(self):
        # One KT-CT-1199, then success → backoff retries and returns the data.
        c = self._client([
            _gql_errors([{"message": "rate", "extensions": {"errorCode": "KT-CT-1199"}}]),
            _gql_data({"ok": True}),
        ])
        with patch("kraken_api_client.asyncio.sleep", new=AsyncMock()) as slept:
            data = run(c._graphql("query { ok }", authenticated=False))
        self.assertEqual(data, {"ok": True})
        self.assertEqual(len(c._session.post_calls), 2)                 # retried once
        self.assertEqual(slept.await_count, 1)

    def test_graphql_5xx_retryable(self):
        # HTTP 5xx is transient → retried, then succeeds.
        c = self._client([FakeResp(503, {}, "busy"), _gql_data({"ok": True})])
        with patch("kraken_api_client.asyncio.sleep", new=AsyncMock()):
            data = run(c._graphql("query { ok }", authenticated=False))
        self.assertEqual(data, {"ok": True})
        self.assertEqual(len(c._session.post_calls), 2)

    def test_graphql_generic_error_not_retried(self):
        # A non-rate-limit GraphQL error is non-retryable → raises on first attempt.
        c = self._client([_gql_errors([{"message": "Something broke",
                          "extensions": {"errorCode": "KT-CT-9999"}}])])
        with patch("kraken_api_client.asyncio.sleep", new=AsyncMock()) as slept:
            with self.assertRaises(KrakenAPIError):
                run(c._graphql("query { x }", authenticated=False))
        self.assertEqual(slept.await_count, 0)                          # no retry
        self.assertEqual(len(c._session.post_calls), 1)


class TestBottlecapDaveDetection(unittest.TestCase):
    def _states(self, *eids_with_attrs):
        return [{"entity_id": e, "attributes": a}
                for e, a in eids_with_attrs]

    def test_empty(self):
        d = kc.detect_bottlecapdave([])
        self.assertFalse(d["found"])
        self.assertIsNone(d["import"])

    def test_import_only(self):
        d = kc.detect_bottlecapdave(self._states(
            ("sensor.octopus_energy_electricity_21L1234567_1900000000001_current_rate",
             {"account_id": "A-ABCD1234"}),
            ("sensor.octopus_energy_electricity_21L1234567_1900000000001_previous_accumulative_consumption",
             {}),
        ))
        self.assertTrue(d["found"])
        self.assertEqual(d["import"]["mpan"], "1900000000001")
        self.assertEqual(d["import"]["serial"], "21L1234567")
        self.assertEqual(d["account_number"], "A-ABCD1234")
        self.assertIsNone(d["export"])
        self.assertFalse(d["mini_available"])

    def test_import_and_export(self):
        d = kc.detect_bottlecapdave(self._states(
            ("sensor.octopus_energy_electricity_21L1111111_1900000000001_current_rate", {}),
            ("sensor.octopus_energy_electricity_21L2222222_1900000000002_export_current_rate", {}),
        ))
        self.assertEqual(d["import"]["mpan"], "1900000000001")
        self.assertEqual(d["export"]["mpan"], "1900000000002")

    def test_mini_detected(self):
        d = kc.detect_bottlecapdave(self._states(
            ("sensor.octopus_energy_electricity_21L1234567_1900000000001_current_accumulative_consumption", {}),
        ))
        self.assertTrue(d["mini_available"])

    def test_ignores_non_bcd_and_gas(self):
        d = kc.detect_bottlecapdave(self._states(
            ("sensor.octopus_energy_gas_G4A1234567_1234567890_current_rate", {}),
            ("sensor.something_else", {}),
            ("sensor.octopus_energy_electricity_21L1234567_1900000000001_current_rate", {}),
        ))
        self.assertEqual(d["entity_count"], 1)  # gas + other ignored
        self.assertEqual(d["import"]["serial"], "21L1234567")

    def test_account_from_account_number_attr(self):
        d = kc.detect_bottlecapdave(self._states(
            ("sensor.octopus_energy_electricity_21L1234567_1900000000001_current_rate",
             {"account_number": "A-ZZZZ9999"}),
        ))
        self.assertEqual(d["account_number"], "A-ZZZZ9999")

    def test_rate_and_standing_charge_sensors_captured(self):
        d = kc.detect_bottlecapdave(self._states(
            ("sensor.octopus_energy_electricity_21L1234567_1900000000001_current_rate", {}),
            ("sensor.octopus_energy_electricity_21L1234567_1900000000001_current_standing_charge", {}),
        ))
        self.assertEqual(d["rate_sensor"],
            "sensor.octopus_energy_electricity_21L1234567_1900000000001_current_rate")
        self.assertEqual(d["standing_charge_sensor"],
            "sensor.octopus_energy_electricity_21L1234567_1900000000001_current_standing_charge")
        self.assertIsNone(d["export_rate_sensor"])

    def test_export_rate_sensor_separate_from_import(self):
        d = kc.detect_bottlecapdave(self._states(
            ("sensor.octopus_energy_electricity_21L1111111_1900000000001_current_rate", {}),
            ("sensor.octopus_energy_electricity_21L2222222_1900000000002_export_current_rate", {}),
        ))
        self.assertEqual(d["rate_sensor"],
            "sensor.octopus_energy_electricity_21L1111111_1900000000001_current_rate")
        self.assertEqual(d["export_rate_sensor"],
            "sensor.octopus_energy_electricity_21L2222222_1900000000002_export_current_rate")

    def test_not_found_has_null_sensors(self):
        # Pure-API path: no BCD → all pre-fill fields null/false.
        d = kc.detect_bottlecapdave(self._states(
            ("sensor.something_else", {}),
        ))
        self.assertFalse(d["found"])
        self.assertIsNone(d["rate_sensor"])
        self.assertIsNone(d["standing_charge_sensor"])
        self.assertIsNone(d["demand_sensor"])

    def test_demand_sensor_captured_for_live_power(self):
        # current_demand (W) is the live-power sensor the wizard pre-fills so EMT
        # reads live power off BCD instead of polling the Mini (detection offload).
        d = kc.detect_bottlecapdave(self._states(
            ("sensor.octopus_energy_electricity_21L1234567_1900000000001_current_rate", {}),
            ("sensor.octopus_energy_electricity_21L1234567_1900000000001_current_demand", {}),
        ))
        self.assertEqual(d["demand_sensor"],
            "sensor.octopus_energy_electricity_21L1234567_1900000000001_current_demand")
        self.assertTrue(d["mini_available"])   # current_demand also implies Mini

    def test_demand_sensor_import_side_only(self):
        # An export-side demand sensor must NOT populate the (import) Live Power field.
        d = kc.detect_bottlecapdave(self._states(
            ("sensor.octopus_energy_electricity_21L2222222_1900000000002_export_current_demand", {}),
        ))
        self.assertIsNone(d["demand_sensor"])

    def test_no_demand_sensor_when_no_mini(self):
        d = kc.detect_bottlecapdave(self._states(
            ("sensor.octopus_energy_electricity_21L1234567_1900000000001_current_rate", {}),
        ))
        self.assertIsNone(d["demand_sensor"])


class TestDetectOhmeChargeMode(unittest.TestCase):
    def _st(self, eid, state="", attrs=None):
        return {"entity_id": eid, "state": state, "attributes": attrs or {}}

    def test_empty(self):
        d = kc.detect_ohme_charge_mode([])
        self.assertFalse(d["found"])
        self.assertIsNone(d["integration"])

    def test_no_ohme_entities(self):
        d = kc.detect_ohme_charge_mode([
            self._st("select.thermostat_mode", "heat"),
            self._st("binary_sensor.front_door", "off"),
        ])
        self.assertFalse(d["found"])

    def test_official_smart_charge(self):
        d = kc.detect_ohme_charge_mode([
            self._st("select.ohme_epod_charge_mode", "Smart charge"),
        ])
        self.assertTrue(d["found"])
        self.assertEqual(d["integration"], "official")
        self.assertEqual(d["charge_mode_entity"], "select.ohme_epod_charge_mode")
        self.assertEqual(d["mode"], "Smart charge")
        self.assertIs(d["is_boost"], False)

    def test_official_max_charge_is_boost(self):
        d = kc.detect_ohme_charge_mode([
            self._st("select.ohme_home_pro_charge_mode", "Max charge"),
        ])
        self.assertTrue(d["found"])
        self.assertIs(d["is_boost"], True)

    def test_official_paused_not_boost(self):
        d = kc.detect_ohme_charge_mode([
            self._st("select.ohme_epod_charge_mode", "Paused"),
        ])
        self.assertIs(d["is_boost"], False)

    def test_official_detected_by_state_value_when_id_atypical(self):
        # id doesn't contain charge_mode, but the state is a known mode value.
        d = kc.detect_ohme_charge_mode([
            self._st("select.ohme_epod_setting", "Smart charge"),
        ])
        self.assertTrue(d["found"])
        self.assertEqual(d["integration"], "official")

    def test_danr_charge_slot_active(self):
        d = kc.detect_ohme_charge_mode([
            self._st("binary_sensor.ohme_epod_charge_slot_active", "on"),
        ])
        self.assertTrue(d["found"])
        self.assertEqual(d["integration"], "danr")
        self.assertEqual(d["mode"], "on")
        self.assertIsNone(d["is_boost"])  # danr slot signal can't flag boost alone

    def test_official_wins_over_danr_regardless_of_order(self):
        danr = self._st("binary_sensor.ohme_epod_charge_slot_active", "on")
        official = self._st("select.ohme_epod_charge_mode", "Max charge")
        # danr first
        d1 = kc.detect_ohme_charge_mode([danr, official])
        self.assertEqual(d1["integration"], "official")
        self.assertIs(d1["is_boost"], True)
        # official first
        d2 = kc.detect_ohme_charge_mode([official, danr])
        self.assertEqual(d2["integration"], "official")
        self.assertEqual(d2["charge_mode_entity"], "select.ohme_epod_charge_mode")

    def test_status_sensor_captured(self):
        d = kc.detect_ohme_charge_mode([
            self._st("select.ohme_epod_charge_mode", "Smart charge"),
            self._st("sensor.ohme_epod_status", "Charging"),
        ])
        self.assertEqual(d["status_entity"], "sensor.ohme_epod_status")

    def test_non_ohme_select_with_modelike_value_ignored(self):
        # Teeth: a non-OHME select must not be picked up even if its state could
        # look mode-like — the "ohme" anchor is required.
        d = kc.detect_ohme_charge_mode([
            self._st("select.car_charger_charge_mode", "Smart charge"),
        ])
        self.assertFalse(d["found"])


if __name__ == "__main__":
    unittest.main()