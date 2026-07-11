#!/usr/bin/with-contenv bashio

# ── Detect environment ────────────────────────────────────────────────────────
# SUPERVISOR_TOKEN is only injected in HA OS / Supervised mode

if [ -n "$SUPERVISOR_TOKEN" ]; then
  # ── HA OS / Supervised mode ──
  LOG_LEVEL=$(bashio::config 'log_level' 2>/dev/null || echo "info")
  export LOG_LEVEL="${LOG_LEVEL:-info}"
  export EMT_MODE="supervised"
  # Internal bind port is always 8099 under Supervisor (ingress_port and the
  # ports mapping are both 8099); the host publish is set in the Network panel,
  # not here. The old `port` option was removed in 3.2.0.
  export EMT_PORT="8099"
  PUBLISH_HA=$(bashio::config 'publish_ha_sensors' 2>/dev/null || echo "true")
  export PUBLISH_HA_SENSORS="${PUBLISH_HA:-true}"
  if [ "$PUBLISH_HA_SENSORS" = "false" ]; then
    bashio::log.warning "publish_ha_sensors=false — HA sensor publishing disabled (dev mode)"
  fi
  # Optional per-instance footer label (blank falls back to the add-on name).
  INSTANCE_NAME=$(bashio::config 'instance_name' 2>/dev/null || echo "")
  if [ -n "$INSTANCE_NAME" ] && [ "$INSTANCE_NAME" != "null" ]; then
    export EMT_INSTANCE_NAME="${INSTANCE_NAME}"
  fi
  bashio::log.info "Energy Meter Tracker starting in Supervised mode (log_level=${LOG_LEVEL}, port=${EMT_PORT})"
else
  # ── Standalone Docker mode ──
  export LOG_LEVEL="${LOG_LEVEL:-info}"
  export EMT_MODE="standalone"
  # Allow disabling HA sensor publishing via -e PUBLISH_HA_SENSORS=false
  export PUBLISH_HA_SENSORS="${PUBLISH_HA_SENSORS:-true}"
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