"""
ha_client.py
============
Replaces all PyScript HA primitives for the add-on:

  PyScript              →  Here
  ─────────────────────────────────────────────────
  state.get(entity_id)  →  client.get_state(entity_id)
  state.set(...)        →  await client.set_state(...)
  @state_trigger(...)   →  client.subscribe_state(entity_id, callback)
  log.info/error        →  standard Python logging

Communicates with HA via:
  - WebSocket API  (ws://supervisor/core/websocket)  — state triggers + subscriptions
  - REST API       (http://supervisor/core/api)       — get/set state
"""

import asyncio
import json
import logging
import os
import aiohttp

logger = logging.getLogger("ha_client")

# ── Detect mode: HA Supervised vs standalone Docker ──────────────────────────
_SUPERVISOR_TOKEN = os.environ.get("SUPERVISOR_TOKEN", "")
_HA_TOKEN         = os.environ.get("HA_TOKEN", "")
_HA_URL           = os.environ.get("HA_URL", "").rstrip("/")
_EMT_MODE         = os.environ.get("EMT_MODE", "supervised")

if _EMT_MODE == "standalone" and _HA_URL:
    # Standalone Docker — use user-provided HA URL and long-lived access token
    _TOKEN      = _HA_TOKEN
    HA_WS_URL   = _HA_URL.replace("http://", "ws://").replace("https://", "wss://") + "/api/websocket"
    HA_REST_URL = _HA_URL + "/api"
else:
    # HA OS / Supervised — use Supervisor endpoints
    _TOKEN      = _SUPERVISOR_TOKEN
    HA_WS_URL   = "ws://supervisor/core/websocket"
    HA_REST_URL = "http://supervisor/core/api"

SUPERVISOR_TOKEN = _TOKEN   # keep name for backward compat
HEADERS = {
    "Authorization": f"Bearer {_TOKEN}",
    "Content-Type":  "application/json",
}


class HAClient:
    """
    Async HA client.

    Usage:
        client = HAClient()
        await client.connect()

        # Read a sensor
        val = client.get_state("sensor.my_sensor")

        # Write a synthetic sensor
        await client.set_state("sensor.my_sensor", 42.0, {...attrs...})

        # Subscribe to state changes
        client.subscribe_state("sensor.my_sensor", my_callback)

        # Run the WebSocket listener (blocks — run as asyncio task)
        await client.listen()
    """

    def __init__(self):
        self._session:         aiohttp.ClientSession | None = None
        self._ws:              aiohttp.ClientWebSocketResponse | None = None
        self._msg_id:          int = 1
        self._state_cache:     dict = {}          # entity_id → state string
        self._subscriptions:   dict = {}          # entity_id → [callbacks]
        self._connected:       asyncio.Event = asyncio.Event()
        self._pending:         dict = {}          # msg_id → Future
        self._on_reconnect:    object = None      # async callback on reconnect

    # ──────────────────────────────────────────────────────────────────────
    # Connection
    # ──────────────────────────────────────────────────────────────────────

    async def connect(self):
        """Open aiohttp session and WebSocket, complete HA auth handshake."""
        # Close existing session if reconnecting
        if self._session and not self._session.closed:
            await self._session.close()
        self._session = aiohttp.ClientSession(headers=HEADERS)
        try:
            self._ws = await self._session.ws_connect(HA_WS_URL)
            logger.info("ha_client: WebSocket connected")

            # HA sends auth_required immediately
            msg = await self._ws.receive_json()
            if msg.get("type") != "auth_required":
                raise RuntimeError(f"Unexpected first message: {msg}")

            await self._ws.send_json({
                "type":         "auth",
                "access_token": SUPERVISOR_TOKEN,
            })

            msg = await self._ws.receive_json()
            if msg.get("type") == "auth_ok":
                logger.info("ha_client: authenticated OK  (HA %s)", msg.get("ha_version", "?"))
            elif msg.get("type") == "auth_invalid":
                raise RuntimeError("ha_client: authentication failed — check SUPERVISOR_TOKEN")
            else:
                raise RuntimeError(f"ha_client: unexpected auth response: {msg}")

            self._connected.set()

        except Exception as e:
            logger.error("ha_client: connection failed: %s", e)
            raise

    async def close(self):
        if self._ws:
            await self._ws.close()
        if self._session:
            await self._session.close()
        logger.info("ha_client: connection closed")

    # ──────────────────────────────────────────────────────────────────────
    # WebSocket listener loop
    # ──────────────────────────────────────────────────────────────────────

    async def listen(self):
        """
        Receive all WebSocket messages indefinitely.
        Automatically reconnects if the WebSocket closes.
        """
        while True:
            try:
                await self._connected.wait()
                await self._subscribe_state_changes()

                async for msg in self._ws:
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        try:
                            data = json.loads(msg.data)
                            await self._handle_message(data)
                        except Exception as e:
                            logger.error("ha_client: message parse error: %s", e)

                    elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                        logger.warning("ha_client: WebSocket closed/error — reconnecting in 10s")
                        break

            except Exception as e:
                logger.warning("ha_client: listen error: %s — reconnecting in 10s", e)

            # Reconnect — keep retrying until successful
            while True:
                await asyncio.sleep(10)
                try:
                    self._connected.clear()
                    await self.connect()
                    logger.info("ha_client: reconnected successfully")
                    if self._on_reconnect:
                        await self._on_reconnect()
                    break
                except Exception as e:
                    logger.warning("ha_client: reconnect failed: %s — will retry in 10s", e)

    async def _subscribe_state_changes(self):
        """Subscribe to all state_changed events on the WebSocket."""
        msg_id = self._next_id()
        await self._ws.send_json({
            "id":         msg_id,
            "type":       "subscribe_events",
            "event_type": "state_changed",
        })
        logger.info("ha_client: subscribed to state_changed events (msg_id=%d)", msg_id)

    async def _handle_message(self, data: dict):
        msg_type = data.get("type")

        # ── Reply to a command we sent ──────────────────────────────────
        if msg_type == "result":
            future = self._pending.pop(data.get("id"), None)
            if future and not future.done():
                if data.get("success"):
                    future.set_result(data.get("result"))
                else:
                    future.set_exception(
                        RuntimeError(f"HA command failed: {data.get('error')}")
                    )
            return

        # ── State changed event ─────────────────────────────────────────
        if msg_type == "event":
            event = data.get("event", {})
            if event.get("event_type") == "state_changed":
                ed         = event.get("data", {})
                entity_id  = ed.get("entity_id", "")
                new_state  = ed.get("new_state") or {}
                state_val  = new_state.get("state")

                # Update cache
                self._state_cache[entity_id] = state_val

                # Fire any registered callbacks
                for cb in self._subscriptions.get(entity_id, []):
                    try:
                        if asyncio.iscoroutinefunction(cb):
                            asyncio.create_task(cb(entity_id, state_val, new_state))
                        else:
                            cb(entity_id, state_val, new_state)
                    except Exception as e:
                        logger.error(
                            "ha_client: callback error for %s: %s", entity_id, e
                        )

    # ──────────────────────────────────────────────────────────────────────
    # State — get (sync, uses cache populated by REST pre-load + WS events)
    # ──────────────────────────────────────────────────────────────────────

    def get_state(self, entity_id: str) -> str | None:
        """
        Return the last known state string for entity_id, or None.
        Cache is pre-populated at startup via preload_states() and kept
        current by the WebSocket listener.
        """
        return self._state_cache.get(entity_id)

    async def preload_states(self, entity_ids: list[str]):
        """
        Fetch current states for a list of entities via REST API.
        Called once at startup so the cache is populated before the
        engine loop first runs.
        """
        async with self._session.get(f"{HA_REST_URL}/states") as resp:
            if resp.status != 200:
                logger.error("ha_client: failed to preload states (%d)", resp.status)
                return
            all_states = await resp.json()

        wanted = set(entity_ids)
        loaded = 0
        for s in all_states:
            eid = s.get("entity_id", "")
            if not wanted or eid in wanted:
                self._state_cache[eid] = s.get("state")
                loaded += 1

        logger.info("ha_client: preloaded %d states", loaded)

    async def get_all_entity_ids(self) -> list[str]:
        """Return all entity IDs known to HA (used by the config UI)."""
        async with self._session.get(f"{HA_REST_URL}/states") as resp:
            if resp.status != 200:
                return []
            all_states = await resp.json()
        return sorted(s.get("entity_id", "") for s in all_states)

    async def get_entity_attributes(self, entity_id: str) -> dict:
        """
        Fetch the full state object for entity_id via REST and return its
        attributes dict. Returns {} if the entity is not found or the
        request fails.
        """
        url = f"{HA_REST_URL}/states/{entity_id}"
        try:
            async with self._session.get(url) as resp:
                if resp.status != 200:
                    logger.warning("ha_client: get_entity_attributes %s failed (%d)", entity_id, resp.status)
                    return {}
                data = await resp.json()
                return data.get("attributes", {})
        except Exception as e:
            logger.warning("ha_client: get_entity_attributes %s error: %s", entity_id, e)
            return {}

    # ──────────────────────────────────────────────────────────────────────
    # State — set (async, REST API)
    # ──────────────────────────────────────────────────────────────────────

    async def set_state(
        self,
        entity_id:  str,
        state:      float | str,
        attributes: dict | None = None,
    ):
        """
        Create or update a synthetic HA sensor via REST.
        Replaces PyScript's state.set().
        """
        payload = {
            "state":      str(state),
            "attributes": attributes or {},
        }
        url = f"{HA_REST_URL}/states/{entity_id}"
        try:
            async with self._session.post(url, json=payload) as resp:
                if resp.status not in (200, 201):
                    body = await resp.text()
                    logger.error(
                        "ha_client: set_state %s failed (%d): %s",
                        entity_id, resp.status, body
                    )
                else:
                    logger.debug("ha_client: set_state %s = %s", entity_id, state)
        except Exception as e:
            logger.error("ha_client: set_state exception for %s: %s", entity_id, e)

    # ──────────────────────────────────────────────────────────────────────
    # Subscriptions — replaces @state_trigger
    # ──────────────────────────────────────────────────────────────────────

    def subscribe_state(self, entity_id: str, callback):
        """
        Register a callback to fire whenever entity_id changes state.
        Replaces @state_trigger(entity_id).

        callback signature:
            async def my_cb(entity_id: str, new_val: str, full_state: dict): ...
            or sync:
            def my_cb(entity_id, new_val, full_state): ...
        """
        # Avoid duplicate subscriptions on reconnect
        existing = self._subscriptions.get(entity_id, [])
        if callback not in existing:
            self._subscriptions.setdefault(entity_id, []).append(callback)
            logger.info("ha_client: subscribed to state changes for %s", entity_id)
        else:
            logger.debug("ha_client: already subscribed to %s, skipping", entity_id)

    # ──────────────────────────────────────────────────────────────────────
    # Helpers
    # ──────────────────────────────────────────────────────────────────────

    def _next_id(self) -> int:
        mid = self._msg_id
        self._msg_id += 1
        return mid