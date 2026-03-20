# Development Guide

## Architecture Overview

Energy Meter Tracker is a Home Assistant add-on built around a Python asyncio engine. The key components are:

```
main.py              — Entry point. Wires together HAClient, engine and Flask server.
engine.py            — Core half-hour block engine. All metering logic lives here.
ha_client.py         — WebSocket + REST client. Replaces PyScript primitives.
energy_engine_io.py  — Atomic file I/O helpers.
energy_charts.py     — Plotly chart generation (daily usage + net heatmap).
web/server.py        — Flask web UI and API endpoints.
web/templates/       — Jinja2 HTML templates.
```

### Runtime modes

| Mode | Detection | HA connection |
|------|-----------|---------------|
| Supervised | `SUPERVISOR_TOKEN` env var present | `ws://supervisor/core/websocket` |
| Standalone Docker | No `SUPERVISOR_TOKEN` | `ws://<HA_URL>/api/websocket` |

`run.sh` detects the mode and sets `EMT_MODE` before starting Python.

---

## Block Lifecycle

The engine runs on a 10-second tick loop. Each tick:

1. **Drain read queue** — sensor state change callbacks push timestamps onto `_read_queue`. The tick drains all queued reads and calls `capture_samples()` for each.
2. **Periodic checkpoint** — if 60 seconds have elapsed since last checkpoint, capture a sample regardless of sensor updates.
3. **Near-boundary capture** — within 15 seconds of a :00 or :30 boundary, capture on every tick.
4. **Gap fill** — if a `_gap_marker` is present on the current block (set after an outage), attempt to fill missing blocks using interpolation.
5. **Block rollover** — if the current time has passed the block's end boundary and a post-boundary read is available, `finalise_block()` is called.

### Block finalisation (finalise_block)

Finalisation runs four passes:

- **PASS 1** — compute kWh and cost for all meters, using boundary-interpolated opening and closing reads
- **PASS 2** — grid-authoritative sub-meter distribution: subtract sub-meter consumption from main meter remainder; allocate grid kWh to protected loads first (EV, heat pump), then inverter-possible devices (battery)
- **PASS 3** — compute block totals
- **PASS 4** — update cumulative totals and push to HA sensors

After finalisation:
- Rolling buffer is pruned to post-boundary reads only
- Charts are regenerated
- Data files are backed up to `/share/energy_meter_tracker_backup/`

### Interpolation

`interpolate_value(pre_read, post_read, target_dt)` performs linear interpolation between two timestamped meter readings. Fraction is clamped to [0, 1].

Used at block boundaries to compute precise opening and closing values regardless of when sensor updates actually arrive.

### Gap detection and filling

If the engine restarts after an outage, `detect_gap()` counts missing half-hour windows between the last known block end and now. A `_gap_marker` is written to `current_block.json` containing the last known reads and rates. On the next tick after a real sensor read arrives, `build_gap_blocks()` interpolates all missing windows and inserts them into `blocks.json`.

Gaps longer than 12 hours produce zero blocks rather than interpolated ones to avoid misleading data.

---

## File Structure

```
/data/energy_meter_tracker/
    blocks.json              — all finalised blocks (primary dataset)
    current_block.json       — in-progress block with live reads
    cumulative_totals.json   — running totals for HA sensors
    meters_config.json       — meter and channel configuration

/share/energy_meter_tracker_backup/
    blocks.json              — copied after every finalise
    current_block.json       — copied after every finalise
    cumulative_totals.json   — copied after every finalise
    meters_config.json       — copied after every finalise
    backups/
        YYYYMMDDTHHMMSS_label.zip   — zip snapshots (20 max)
```

---

## Running Unit Tests

Tests use Python's built-in `unittest` — no external dependencies needed.

```bash
cd /addons/energy_meter_tracker
python3 -m unittest test_engine -v
```

Tests cover: `floor_to_hh`, `interpolate_value`, `detect_gap`, `compute_channel` (main and sub-meter), `select_opening_read`, `select_closing_read`, gap marker helpers, `build_gap_blocks`, `extract_last_reads`.

When adding new engine logic, add corresponding tests to `test_engine.py`. The test file uses module stubs so it runs without HA, Flask or filesystem access.

---

## Local Development

### Supervised (HA OS)

The add-on is loaded from `/addons/energy_meter_tracker/`. Changes to Python files require a rebuild via the HA add-on UI. Template changes (`web/templates/`) also require a rebuild since files are copied into the container at build time.

After rebuilding, if `config.yaml` changed run:
```bash
ha supervisor restart
```

### Standalone Docker

```bash
docker build -t emt-dev .
docker run -d \
  --name emt-dev \
  -p 8099:8099 \
  -e EMT_MODE=standalone \
  -e HA_URL=http://192.168.1.10:8123 \
  -e HA_TOKEN=your_token \
  -e LOG_LEVEL=debug \
  -v /tmp/emt-data:/data/energy_meter_tracker \
  emt-dev
docker logs -f emt-dev
```

---

## Key Design Decisions

**Why asyncio?**
The engine needs to handle WebSocket events, a 10-second tick loop, and Flask serving concurrently. Flask runs in a background thread via `threading.Thread`; everything else runs in the main asyncio event loop.

**Why not HACS?**
HACS is for integrations, Lovelace cards and themes — not add-ons. Add-ons run as Docker containers alongside HA and are distributed via add-on repositories.

**Why is the main meter authoritative?**
Sub-meter sensors (CT clamps, device integrations) are less accurate than the DCC smart meter CAD feed. Treating the main meter as authoritative and distributing its reading across sub-meters ensures billing totals are always grounded in the actual grid reading.

**Why interpolation at boundaries?**
Without it, a sensor update that arrives at 09:28 would either be assigned entirely to the 09:00-09:30 block or entirely to the 09:30-10:00 block depending on which side of the boundary it falls. Interpolation splits the delta proportionally so each block gets the fraction that actually occurred within it.

---

## Known Limitations & Future Work

- **Solar generation** — not supported as a sub-meter type. Export sub-metering requires design work around the export channel.
- **Gas meters** — not designed. Would require a separate meter type with different unit handling.
- **Multiple batteries/inverters** — only one inverter-possible sub-meter per parent is well tested.
- **V2G export** — V2X-capable EV export to grid is flagged but not broken down by sub-meter.
- **Ingress** — currently supported via a WSGI middleware. Full Ingress with sidebar toggle works; the approach could be refined.
- **Config reload** — sensor subscriptions re-register on config save but the engine does not watch the config file for changes.
- **Werkzeug** — replaced with Waitress for production. Consider gunicorn for higher concurrency if the UI becomes more complex.