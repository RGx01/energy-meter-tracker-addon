"""
test_rate_source_migration.py — 3.0.0 per-channel rate-source migration.

Covers the bottom of the "API rates vs sensor" stack:

  • the schema gains per-channel `rate_source` / `standing_charge_source`;
  • opening a pre-3.0.0 (v2) database backfills those columns with the
    source each channel was ALREADY using, so an upgrade changes no prices
    until the user opts in:
        - main meter   → 'sensor' when a rate sensor is mapped, else 'api';
        - device       → 'main'  (inherit the main meter's effective rate),
                          unless it had an explicit own-sensor choice;
        - standing chg → 'sensor' when a SC sensor is mapped, else 'api';
  • the choice round-trips through save → DB → config_from_db;
  • config_from_db derives the engine-compat meta.rate_source for devices;
  • the migration's semantic — "a device inherits the main meter's rate" —
    reproduces v2 pricing block-for-block, proven by driving the REAL
    engine.compute_channel with single-rate-per-block parent rates.

A guarded check also runs against the real prod_dev v2 database when the
manual backup is present in the session uploads.
"""

import os
import glob
import shutil
import tempfile
import unittest
import zipfile
from unittest.mock import patch

from block_store import BlockStore
from engine import compute_channel, _device_base_rate


# ── helpers ──────────────────────────────────────────────────────────────────

def _base_meta(extra=None):
    m = {
        "billing_day": 1,
        "block_minutes": 30,
        "timezone": "Europe/London",
        "currency_symbol": "£",
        "currency_code": "GBP",
        "site": "Home",
    }
    if extra:
        m.update(extra)
    return m


def _v2_config(main_rate_sensor="", main_sc_sensor=None, device_rate_source="overlay"):
    """A v2-shaped config dict with NO per-channel source set (pre-upgrade).

    main_rate_sensor="" / None models an API-mode main (no rate sensor); a
    real entity id models a classic CAD/sensor main.
    """
    return {
        "schema_version": "1.0",
        "meters": {
            "electricity_main": {
                "meta": _base_meta(),
                "channels": {
                    "import": {
                        "read": "sensor.main_import",
                        "rate": main_rate_sensor,
                        **({"standing_charge_sensor": main_sc_sensor} if main_sc_sensor else {}),
                    },
                    "export": {
                        "read": "sensor.main_export",
                        "rate": main_rate_sensor,
                    },
                },
            },
            "ev_charger": {
                "meta": _base_meta({
                    "sub_meter": True,
                    "parent_meter": "electricity_main",
                    "device": "EV Charger",
                    "meter_type": "ev",
                    **({"rate_source": device_rate_source} if device_rate_source else {}),
                }),
                "channels": {"import": {"read": "sensor.ev_import"}},
            },
            "house_battery": {
                "meta": _base_meta({
                    "sub_meter": True,
                    "parent_meter": "electricity_main",
                    "device": "Battery",
                    "meter_type": "battery",
                    **({"rate_source": device_rate_source} if device_rate_source else {}),
                }),
                "channels": {"import": {"read": "sensor.batt_import"}},
            },
        },
    }


def _seed_v2_then_reopen(tmpdir, config):
    """Write a config with NULL channel sources, then reopen so the upgrade
    backfill runs — mirroring a v2 DB gaining the new columns on first 3.0.0
    open. Returns a fresh BlockStore and the latest period id."""
    path = os.path.join(tmpdir, "blocks.db")
    st = BlockStore(path)
    pid = st.insert_config_period(config, effective_from="2026-01-01T00:00:00")
    # Force the channel sources back to NULL to emulate rows written by v2 code
    # that never knew about these columns.
    st._conn.execute("UPDATE meter_channels SET rate_source=NULL, standing_charge_source=NULL")
    st._conn.commit()
    st._conn.close()
    # Reopen: _ensure_schema() runs the backfill.
    st2 = BlockStore(path)
    pid2 = st2._conn.execute("SELECT MAX(config_period_id) FROM meters").fetchone()[0]
    return st2, pid2


def _channel_sources(st, pid):
    """{meter_id: {channel: (rate_source, standing_charge_source)}}"""
    out = {}
    rows = st._conn.execute(
        """SELECT m.meter_id, mc.channel, mc.rate_source, mc.standing_charge_source
           FROM meter_channels mc JOIN meters m ON m.id = mc.meter_id
           WHERE m.config_period_id = ?""",
        (pid,),
    ).fetchall()
    for r in rows:
        out.setdefault(r["meter_id"], {})[r["channel"]] = (
            r["rate_source"], r["standing_charge_source"]
        )
    return out


# ── migration: API-mode main (no rate sensor) ────────────────────────────────

class TestBackfillApiModeMain(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.tmp, ignore_errors=True)
        # API-mode: main has no rate sensor; devices were 'overlay'
        self.st, self.pid = _seed_v2_then_reopen(
            self.tmp, _v2_config(main_rate_sensor="", device_rate_source="overlay")
        )

    def test_main_with_no_sensor_defaults_to_api(self):
        src = _channel_sources(self.st, self.pid)
        self.assertEqual(src["electricity_main"]["import"][0], "api")
        self.assertEqual(src["electricity_main"]["export"][0], "api")
        # no standing-charge sensor mapped → standing charge also from API
        self.assertEqual(src["electricity_main"]["import"][1], "api")

    def test_devices_default_to_inherit_main(self):
        src = _channel_sources(self.st, self.pid)
        self.assertEqual(src["ev_charger"]["import"][0], "main")
        self.assertEqual(src["house_battery"]["import"][0], "main")

    def test_derived_meta_rate_source_for_engine(self):
        cfg = self.st.config_from_db(self.pid)
        # device → 'main' channel source surfaces as meta.rate_source='overlay'
        self.assertEqual(cfg["meters"]["ev_charger"]["meta"].get("rate_source"), "overlay")
        self.assertEqual(cfg["meters"]["house_battery"]["meta"].get("rate_source"), "overlay")
        # main is unaffected (overlay is device-only)
        self.assertIsNone(cfg["meters"]["electricity_main"]["meta"].get("rate_source"))


# ── migration: classic sensor-fed main ───────────────────────────────────────

class TestBackfillSensorMmain(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.tmp, ignore_errors=True)
        # Classic CAD v2: main has a rate sensor + SC sensor; devices unset
        self.st, self.pid = _seed_v2_then_reopen(
            self.tmp,
            _v2_config(
                main_rate_sensor="sensor.octopus_rate",
                main_sc_sensor="sensor.octopus_standing_charge",
                device_rate_source=None,
            ),
        )

    def test_main_with_sensor_keeps_sensor(self):
        src = _channel_sources(self.st, self.pid)
        self.assertEqual(src["electricity_main"]["import"][0], "sensor")
        self.assertEqual(src["electricity_main"]["export"][0], "sensor")
        # SC sensor present → standing charge stays on the sensor
        self.assertEqual(src["electricity_main"]["import"][1], "sensor")

    def test_unset_devices_still_inherit_main(self):
        src = _channel_sources(self.st, self.pid)
        self.assertEqual(src["ev_charger"]["import"][0], "main")
        self.assertEqual(src["house_battery"]["import"][0], "main")


# ── migration: explicit own-sensor device is preserved ───────────────────────

class TestBackfillExplicitDeviceSensor(unittest.TestCase):
    def test_own_sensor_device_maps_to_sensor(self):
        tmp = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, tmp, ignore_errors=True)
        st, pid = _seed_v2_then_reopen(
            tmp, _v2_config(main_rate_sensor="sensor.r", device_rate_source="own")
        )
        src = _channel_sources(st, pid)
        # a device the user had pinned to its own sensor must NOT become 'main'
        self.assertEqual(src["ev_charger"]["import"][0], "sensor")
        cfg = st.config_from_db(pid)
        self.assertEqual(cfg["meters"]["ev_charger"]["meta"].get("rate_source"), "own")


# ── round-trip: save → DB → config_from_db ───────────────────────────────────

class TestRoundTrip(unittest.TestCase):
    def test_sources_survive_round_trip(self):
        tmp = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, tmp, ignore_errors=True)
        st, pid = _seed_v2_then_reopen(
            tmp, _v2_config(main_rate_sensor="", device_rate_source="overlay")
        )
        cfg = st.config_from_db(pid)
        before = _channel_sources(st, pid)

        # Persist the migrated config as a new period and read it back.
        new_pid = st.insert_config_period(cfg, effective_from="2026-02-01T00:00:00")
        after = _channel_sources(st, new_pid)

        self.assertEqual(after["electricity_main"]["import"], before["electricity_main"]["import"])
        self.assertEqual(after["ev_charger"]["import"], before["ev_charger"]["import"])
        self.assertEqual(after["house_battery"]["import"], before["house_battery"]["import"])
        # explicit values, not NULL, persisted on the new period
        self.assertEqual(after["electricity_main"]["import"][0], "api")
        self.assertEqual(after["ev_charger"]["import"][0], "main")


# ── pricing: "device inherits main" reproduces v2, via REAL compute_channel ──

class TestInheritReproducesV2Pricing(unittest.TestCase):
    """The migration encodes device='main' (inherit). Prove that semantic
    reproduces v2 pricing by running the real sub-meter pricing function with
    the per-block resolved main rate (single value per block, as v2 stored)."""

    PEAK = 0.323092
    OFFPEAK = 0.05493

    def _price_device(self, ts, main_rate, kwh):
        channel = {
            "reads": [
                {"ts": ts, "value": 100.0},
                {"ts": ts.replace("13:00", "13:30"), "value": 100.0 + kwh},
            ],
            "rates": [],  # no own rates → inherit parent
        }
        parent_rates = [{"ts": ts, "value": main_rate}]
        return compute_channel(channel, parent_rates, is_sub_meter=True,
                               meter_id="ev_charger", channel_id="import")

    def test_midday_peak_block_inherits_peak(self):
        r = self._price_device("2026-06-13T13:00:00", self.PEAK, 2.0)
        self.assertAlmostEqual(r["rate"], self.PEAK, places=6)
        self.assertAlmostEqual(r["cost"], 2.0 * self.PEAK, places=6)

    def test_overnight_block_inherits_offpeak(self):
        r = self._price_device("2026-06-13T13:00:00", self.OFFPEAK, 3.0)
        self.assertAlmostEqual(r["rate"], self.OFFPEAK, places=6)
        self.assertAlmostEqual(r["cost"], 3.0 * self.OFFPEAK, places=6)

    def test_compute_channel_full_day_collapses_in_isolation(self):
        """Characterisation: compute_channel's sub-meter reconstruction keeps a
        running minimum, so a FULL multi-rate day collapses to its off-peak
        floor. This latent property is unchanged — but the engine device path no
        longer feeds it the whole-day series: `_device_base_rate` resolves the
        base per-slot for an API-fed main (see TestDeviceBaseRate), so the device
        no longer inherits the collapsed day. This test pins the boundary so a
        future change to the reconstruction is a conscious one.
        """
        channel = {
            "reads": [
                {"ts": "2026-06-13T13:00:00", "value": 100.0},
                {"ts": "2026-06-13T13:30:00", "value": 102.0},
            ],
            "rates": [],
        }
        full_day = [
            {"ts": "2026-06-13T00:00:00", "value": self.OFFPEAK},
            {"ts": "2026-06-13T05:30:00", "value": self.PEAK},
            {"ts": "2026-06-13T23:30:00", "value": self.OFFPEAK},
        ]
        r = compute_channel(channel, full_day, is_sub_meter=True,
                            meter_id="ev_charger", channel_id="import")
        self.assertAlmostEqual(r["rate"], self.OFFPEAK, places=6)  # collapses (isolated)


# ── engine: device base follows the MAIN meter's source (the fix) ────────────

class TestDeviceBaseRate(unittest.TestCase):
    """_device_base_rate resolves a 'use main meter rate' device's pre-overlay
    base, honouring the main meter's own source and never inheriting the
    collapsed whole-day series in API mode."""

    PEAK = 0.323092
    OFFPEAK = 0.05493

    def _resolver(self, peak=True):
        # schedule resolver: peak by day, off-peak overnight (per block_start)
        def r(channel, ts):
            hh = int(ts[11:13])
            return self.PEAK if 6 <= hh < 23 else self.OFFPEAK
        return r

    def test_api_main_resolves_per_slot_not_collapsed_inherit(self):
        # main on API (no rate sensor); inherited value is the COLLAPSED off-peak
        # floor — the resolver's midday peak must win.
        base = _device_base_rate(
            {"rate": "", "rate_source": "api"},
            inherited_rate=self.OFFPEAK,            # collapsed
            block_start="2026-06-13T13:00:00",
            resolver=self._resolver(),
        )
        self.assertAlmostEqual(base, self.PEAK, places=6)

    def test_api_main_inferred_from_absent_sensor(self):
        # un-migrated config: no rate_source, no rate sensor → treat as API
        base = _device_base_rate(
            {"rate": ""},
            inherited_rate=self.OFFPEAK,
            block_start="2026-06-13T13:00:00",
            resolver=self._resolver(),
        )
        self.assertAlmostEqual(base, self.PEAK, places=6)

    def test_sensor_main_inherits_even_when_api_key_present(self):
        # main explicitly on a sensor: inherit its per-block rate, do NOT
        # substitute the resolver (the cad+api pitfall).
        base = _device_base_rate(
            {"rate": "sensor.octopus_rate", "rate_source": "sensor"},
            inherited_rate=self.PEAK,               # main's sensor peak
            block_start="2026-06-13T13:00:00",
            resolver=lambda c, t: self.OFFPEAK,     # would be wrong if used
        )
        self.assertAlmostEqual(base, self.PEAK, places=6)

    def test_sensor_main_inferred_from_present_sensor(self):
        base = _device_base_rate(
            {"rate": "sensor.octopus_rate"},        # no explicit source
            inherited_rate=self.PEAK,
            block_start="2026-06-13T13:00:00",
            resolver=lambda c, t: self.OFFPEAK,
        )
        self.assertAlmostEqual(base, self.PEAK, places=6)

    def test_sensor_main_gap_falls_back_to_resolver(self):
        # sensor main but inherited base empty (gap block) → resolver fallback
        base = _device_base_rate(
            {"rate": "sensor.octopus_rate", "rate_source": "sensor"},
            inherited_rate=0.0,
            block_start="2026-06-13T13:00:00",
            resolver=self._resolver(),
        )
        self.assertAlmostEqual(base, self.PEAK, places=6)

    def test_no_schedule_keeps_inherited(self):
        base = _device_base_rate(
            {"rate": "", "rate_source": "api"},
            inherited_rate=self.OFFPEAK,
            block_start="2026-06-13T13:00:00",
            resolver=lambda c, t: None,             # no schedule
        )
        self.assertAlmostEqual(base, self.OFFPEAK, places=6)


# ── optional: against the real prod_dev v2 database ──────────────────────────

def _find_v2_upload():
    for z in sorted(glob.glob("/mnt/user-data/uploads/*manual.zip")):
        try:
            with zipfile.ZipFile(z) as zf:
                names = [n for n in zf.namelist() if n.endswith("blocks.db")]
                if not names:
                    continue
                tmp = tempfile.mkdtemp()
                zf.extract(names[0], tmp)
                db = os.path.join(tmp, names[0])
                import sqlite3
                c = sqlite3.connect(db)
                mc = [r[1] for r in c.execute("PRAGMA table_info(meter_channels)")]
                has_blocks = c.execute(
                    "SELECT COUNT(*) FROM blocks WHERE meter_id='ev_charger'"
                ).fetchone()[0]
                c.close()
                # v2 lineage: meter_channels lacks the new source columns
                if "rate_source" not in mc and has_blocks:
                    return db, tmp
        except Exception:
            continue
    return None, None


@unittest.skipUnless(_find_v2_upload()[0], "real v2 prod_dev DB not present in uploads")
class TestRealV2DbReproduction(unittest.TestCase):
    def test_real_v2_devices_priced_at_main_rate(self):
        """On real v2 data, every block with real device consumption was
        priced at the main meter's rate — exactly what device='main' encodes."""
        import sqlite3
        db, tmp = _find_v2_upload()
        self.addCleanup(shutil.rmtree, tmp, ignore_errors=True)
        c = sqlite3.connect(db)
        c.row_factory = sqlite3.Row
        rows = c.execute(
            """SELECT block_start, meter_id, imp_rate, imp_kwh FROM blocks
               WHERE meter_id IN ('electricity_main','ev_charger','house_battery')"""
        ).fetchall()
        c.close()
        by = {}
        for r in rows:
            by.setdefault(r["block_start"], {})[r["meter_id"]] = (r["imp_rate"], r["imp_kwh"])
        compared = 0
        mismatches = 0
        for bs, m in by.items():
            main = m.get("electricity_main")
            if not main or main[0] is None:
                continue
            for dev in ("ev_charger", "house_battery"):
                if dev not in m:
                    continue
                drate, dkwh = m[dev]
                if (dkwh or 0) <= 1e-3:      # zero-draw: rate is cosmetic, cost is £0
                    continue
                compared += 1
                if drate is None or abs(drate - main[0]) > 1e-6:
                    mismatches += 1
        self.assertGreater(compared, 1000, "expected a substantial real-data sample")
        self.assertEqual(mismatches, 0,
                         f"{mismatches}/{compared} real-consumption device blocks "
                         f"were NOT priced at the main rate")

    def test_real_v2_migration_backfills_expected_sources(self):
        db, tmp = _find_v2_upload()
        self.addCleanup(shutil.rmtree, tmp, ignore_errors=True)
        work = os.path.join(tempfile.mkdtemp(), "blocks.db")
        shutil.copy(db, work)
        st = BlockStore(work)
        pid = st._conn.execute("SELECT MAX(config_period_id) FROM meters").fetchone()[0]
        src = _channel_sources(st, pid)
        # prod_dev is API-mode IOG: main has no rate sensor → 'api'; devices inherit
        self.assertEqual(src["electricity_main"]["import"][0], "api")
        for dev in ("ev_charger", "house_battery"):
            if dev in src:
                self.assertEqual(src[dev]["import"][0], "main")


# ── engine wiring: finalise_block routes the device base through the fix ─────

class TestDeviceOverlayFinaliseWiring(unittest.TestCase):
    """End-to-end: a main on API whose import channel carries a full-day
    schedule (which collapses under inheritance) plus an overlay device — the
    finalised device block must be PEAK at midday, proving finalise_block uses
    _device_base_rate (resolver per-slot) rather than the collapsed inherit."""

    PEAK = 0.323092
    OFFPEAK = 0.05493

    def test_overlay_device_finalised_at_peak_not_collapsed(self):
        import engine
        from unittest.mock import MagicMock

        cfg = {
            "meters": {
                "electricity_main": {
                    "meta": {"sub_meter": False, "block_minutes": 30,
                             "timezone": "UTC", "billing_day": 1,
                             "currency_symbol": "£", "currency_code": "GBP"},
                    # API mode: no rate sensor mapped
                    "channels": {"import": {"read": "sensor.imp", "rate": ""},
                                 "export": {"read": "sensor.exp", "rate": ""}},
                },
                "ev_charger": {
                    "meta": {"sub_meter": True, "parent_meter": "electricity_main",
                             "block_minutes": 30, "timezone": "UTC",
                             "billing_day": 1, "currency_symbol": "£",
                             "currency_code": "GBP",
                             "rate_source": "overlay"},  # use main meter rate
                    "channels": {"import": {"read": "sensor.ev_imp"}},
                },
            }
        }

        # A full-day schedule sitting on the main import channel — the series that
        # collapses to off-peak when a sub-meter inherits it.
        full_day = [
            {"ts": "2026-06-13T00:00:00", "value": self.OFFPEAK},
            {"ts": "2026-06-13T05:30:00", "value": self.PEAK},
            {"ts": "2026-06-13T23:30:00", "value": self.OFFPEAK},
        ]
        cb = {
            "start": "2026-06-13T13:00:00",
            "end": "2026-06-13T13:30:00",
            "meters": {
                "electricity_main": {
                    "meta": cfg["meters"]["electricity_main"]["meta"],
                    "channels": {
                        "import": {
                            "reads": [{"ts": "2026-06-13T13:00:00", "value": 1000.0},
                                      {"ts": "2026-06-13T13:30:00", "value": 1004.0}],
                            "rates": full_day,
                        },
                        "export": {"reads": [], "rates": []},
                    },
                },
                "ev_charger": {
                    "meta": cfg["meters"]["ev_charger"]["meta"],
                    "channels": {
                        "import": {
                            "reads": [{"ts": "2026-06-13T13:00:00", "value": 500.0},
                                      {"ts": "2026-06-13T13:30:00", "value": 502.0}],
                            "rates": [],
                        },
                    },
                },
            },
        }

        s = BlockStore(":memory:")
        s.insert_config_period(cfg)

        captured = []
        orig = {
            "store": engine._store,
            "append": engine.append_block,
            "load_config": engine.load_config,
            "load_json": engine.load_json,
            "charts": engine.generate_charts,
            "backup": engine._backup_to_share,
            "resolver": engine._kraken_rate_resolver,
        }
        engine._store = s
        engine.append_block = lambda blk: (captured.append(blk), s.append_block(blk))
        engine.load_config = lambda: cfg
        engine.load_json = lambda path, default=None: cfg if "config" in str(path) else (default or {})
        engine.generate_charts = lambda *a, **kw: None
        engine._backup_to_share = lambda: None
        # schedule resolver: peak by day, off-peak overnight
        engine._kraken_rate_resolver = lambda ch, ts: (
            self.PEAK if 6 <= int(ts[11:13]) < 23 else self.OFFPEAK
        )
        try:
            engine.finalise_block(MagicMock(), block_data=cb)
        finally:
            engine._store = orig["store"]
            engine.append_block = orig["append"]
            engine.load_config = orig["load_config"]
            engine.load_json = orig["load_json"]
            engine.generate_charts = orig["charts"]
            engine._backup_to_share = orig["backup"]
            engine._kraken_rate_resolver = orig["resolver"]
            try:
                s.close()
            except Exception:
                pass

        self.assertTrue(captured, "finalise_block produced no block")
        blk = captured[-1]
        ev_imp = blk["meters"]["ev_charger"]["channels"]["import"]
        # 2 kWh at midday peak — must NOT be the collapsed off-peak floor
        self.assertAlmostEqual(ev_imp["rate"], self.PEAK, places=6)
        self.assertAlmostEqual(ev_imp["cost"], 2.0 * self.PEAK, places=4)


# ── engine: explicit choice wins over a mapped sensor (honour the toggle) ─────

class TestDeviceOverlayDecision(unittest.TestCase):
    def test_main_wins_even_with_own_sensor(self):
        from engine import _device_overlay_decision
        # 'use main meter rate' ticked must inherit the main even if a sensor
        # is also mapped (has_own_rates True).
        self.assertTrue(_device_overlay_decision({"rate_source": "main"}, {}, True))
        self.assertTrue(_device_overlay_decision({"rate_source": "main"}, {}, False))

    def test_sensor_uses_own_when_present(self):
        from engine import _device_overlay_decision
        self.assertFalse(_device_overlay_decision({"rate_source": "sensor"}, {}, True))

    def test_sensor_but_empty_falls_back_to_main(self):
        from engine import _device_overlay_decision
        # 'sensor' chosen but none mapped (no own rates) → inherit the main,
        # so the device is never left un-priced.
        self.assertTrue(_device_overlay_decision({"rate_source": "sensor"}, {}, False))

    def test_unset_defers_to_legacy(self):
        import engine
        from engine import _device_overlay_decision
        # no explicit channel choice → legacy _device_uses_overlay precedence
        self.assertFalse(_device_overlay_decision({}, {}, True))          # sensor wins
        self.assertTrue(_device_overlay_decision({}, {"rate_source": "overlay"}, False))
        with patch.object(engine, "kraken_available", return_value=True):
            self.assertTrue(_device_overlay_decision({}, {}, False))      # API default


# ── migration: a device with its OWN rate sensor keeps it ────────────────────

class TestBackfillDeviceWithOwnSensor(unittest.TestCase):
    def test_device_with_rate_sensor_maps_to_sensor(self):
        tmp = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, tmp, ignore_errors=True)
        cfg = _v2_config(main_rate_sensor="sensor.r", device_rate_source=None)
        # give the EV device its own rate sensor (different tariff)
        cfg["meters"]["ev_charger"]["channels"]["import"]["rate"] = "sensor.ev_rate"
        st, pid = _seed_v2_then_reopen(tmp, cfg)
        src = _channel_sources(st, pid)
        self.assertEqual(src["ev_charger"]["import"][0], "sensor")
        # the battery (no own sensor) still inherits the main
        self.assertEqual(src["house_battery"]["import"][0], "main")


# ── wizard: the config it produces round-trips and drives the engine ─────────

class TestWizardConfigShape(unittest.TestCase):
    """The wizard writes per-channel rate_source ('api'/'sensor' on the main,
    'main'/'sensor' on devices). That shape must persist and yield the right
    engine compat mirror + overlay decision."""

    def _wizard_config(self):
        return {
            "schema_version": "1.0",
            "meters": {
                "electricity_main": {
                    "meta": _base_meta(),
                    "channels": {
                        "import": {"read": "sensor.imp", "rate": "",
                                   "rate_source": "api", "standing_charge_source": "api"},
                        "export": {"read": "sensor.exp", "rate": "", "rate_source": "api"},
                    },
                },
                "ev_charger": {
                    "meta": _base_meta({"sub_meter": True, "parent_meter": "electricity_main",
                                        "device": "EV", "meter_type": "ev"}),
                    # 'use main meter rate' ticked
                    "channels": {"import": {"read": "sensor.ev", "rate_source": "main"}},
                },
                "house_battery": {
                    "meta": _base_meta({"sub_meter": True, "parent_meter": "electricity_main",
                                        "device": "Battery", "meter_type": "battery"}),
                    # own sensor (unticked)
                    "channels": {"import": {"read": "sensor.b", "rate": "sensor.batt_rate",
                                            "rate_source": "sensor"}},
                },
            },
        }

    def test_round_trip_and_mirror(self):
        tmp = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, tmp, ignore_errors=True)
        st = BlockStore(os.path.join(tmp, "blocks.db"))
        pid = st.insert_config_period(self._wizard_config(), effective_from="2026-01-01T00:00:00")
        src = _channel_sources(st, pid)
        self.assertEqual(src["electricity_main"]["import"], ("api", "api"))
        self.assertEqual(src["electricity_main"]["export"][0], "api")
        self.assertEqual(src["ev_charger"]["import"][0], "main")
        self.assertEqual(src["house_battery"]["import"][0], "sensor")

        cfg = st.config_from_db(pid)
        # device compat mirror: 'main' → overlay, 'sensor' → own
        self.assertEqual(cfg["meters"]["ev_charger"]["meta"].get("rate_source"), "overlay")
        self.assertEqual(cfg["meters"]["house_battery"]["meta"].get("rate_source"), "own")

    def test_engine_decision_from_wizard_shape(self):
        from engine import _device_overlay_decision
        cfg = self._wizard_config()
        ev = cfg["meters"]["ev_charger"]["channels"]["import"]
        batt = cfg["meters"]["house_battery"]["channels"]["import"]
        # EV ('main') inherits main even though a read sensor exists
        self.assertTrue(_device_overlay_decision(ev, {}, True))
        # battery ('sensor') with its own rate sensor uses it
        self.assertFalse(_device_overlay_decision(batt, {}, True))


if __name__ == "__main__":
    unittest.main()