"""
main.py
=======
Add-on entry point.

Starts:
  1. HAClient  — connects to HA WebSocket, authenticates
  2. engine_startup() — registers triggers, detects gaps, generates charts
  3. engine_loop_task() — 10-second tick loop (forever)
  4. ha_client.listen() — WebSocket listener (forever)
"""

import asyncio
import logging
import sys

from ha_client import HAClient
from engine import engine_startup, engine_loop_task, setup, DATA_DIR, CHART_DIR
import web.server as server

# ── Logging setup ─────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
    stream=sys.stdout,
)
logger = logging.getLogger("main")


async def main():
    logger.info("Energy Meter Tracker add-on starting")

    # 1 — Initialise engine (dirs, locks)
    setup()

    # In standalone mode, also log to file so the UI logs page can read it
    import os
    if os.environ.get("EMT_MODE") == "standalone":
        file_handler = logging.FileHandler(f"{DATA_DIR}/addon.log")
        file_handler.setFormatter(logging.Formatter(
            "%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
            datefmt="%Y-%m-%dT%H:%M:%S"
        ))
        logging.getLogger().addHandler(file_handler)

    # 2 — Connect to HA
    # In supervised mode, check if ha_url/ha_token are set in add-on options
    # (allows supervised users to override the default Supervisor endpoints)
    import os
    if not os.environ.get("EMT_MODE"):
        os.environ["EMT_MODE"] = "supervised"

    ha = HAClient()
    try:
        await ha.connect()
    except Exception as e:
        logger.critical("Failed to connect to Home Assistant: %s", e)
        sys.exit(1)

    # 3 — Run startup sequence (register triggers, gap detection, charts)
    await engine_startup(ha)

    # Register reconnect handler — re-runs startup if HA restarts
    async def on_reconnect():
        logger.info("main: HA reconnected — re-running engine startup")
        await engine_startup(ha)
    ha._on_reconnect = on_reconnect

    # 4 — Start Flask config UI + chart server in background thread
    server.init(DATA_DIR, CHART_DIR, ha)
    server.start()

    # 5 — Run engine loop + WebSocket listener concurrently
    logger.info("Starting engine loop and WebSocket listener")
    try:
        await asyncio.gather(
            engine_loop_task(ha),
            ha.listen(),
        )
    except asyncio.CancelledError:
        logger.info("Tasks cancelled — shutting down")
    finally:
        await ha.close()
        logger.info("Energy Meter Tracker add-on stopped")


if __name__ == "__main__":
    asyncio.run(main())