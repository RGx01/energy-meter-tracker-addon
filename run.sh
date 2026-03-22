#!/usr/bin/with-contenv bashio

# ── Detect environment ────────────────────────────────────────────────────────
# SUPERVISOR_TOKEN is only injected in HA OS / Supervised mode

if [ -n "$SUPERVISOR_TOKEN" ]; then
  # ── HA OS / Supervised mode ──
  LOG_LEVEL=$(bashio::config 'log_level' 2>/dev/null || echo "info")
  export LOG_LEVEL="${LOG_LEVEL:-info}"
  export EMT_MODE="supervised"
  bashio::log.info "Energy Meter Tracker starting in Supervised mode (log_level=${LOG_LEVEL})"
else
  # ── Standalone Docker mode ──
  export LOG_LEVEL="${LOG_LEVEL:-info}"
  export EMT_MODE="standalone"
  echo "[INFO] Energy Meter Tracker starting in standalone Docker mode (log_level=${LOG_LEVEL})"

  if [ -z "$HA_URL" ]; then
    echo "[ERROR] HA_URL environment variable is required in standalone mode"
    echo "[ERROR] Example: -e HA_URL=http://192.168.1.10:8123"
    exit 1
  fi
  if [ -z "$HA_TOKEN" ]; then
    echo "[ERROR] HA_TOKEN environment variable is required in standalone mode"
    echo "[ERROR] Create a Long-Lived Access Token in HA profile settings"
    exit 1
  fi
fi

# ── Ensure data directory exists ─────────────────────────────────────────────
mkdir -p /data/energy_meter_tracker

# ── Start the engine ─────────────────────────────────────────────────────────
exec python3 /app/main.py