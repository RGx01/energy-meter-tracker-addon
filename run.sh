#!/usr/bin/with-contenv bashio

# ── Log level from add-on options ────────────────────────────────────────────
LOG_LEVEL=$(bashio::config 'log_level' 2>/dev/null || echo "info")
export LOG_LEVEL="${LOG_LEVEL:-info}"
bashio::log.info "Energy Meter Tracker starting (log_level=${LOG_LEVEL})"

# ── Ensure data directory exists ─────────────────────────────────────────────
mkdir -p /data/energy_meter_tracker

# ── Start the engine ─────────────────────────────────────────────────────────
exec python3 /app/main.py