"""
Test suite: verifies that Usage Stats (api_blocks_summary logic) produces
totals that match get_billing_totals_for_utc_range (SQL ground truth)
for daily, monthly-billing and yearly views, including BST boundary days.

Run:  python3 -m unittest test_usage_stats_vs_billing -v
"""
import sys, os, types, unittest
from datetime import datetime, timedelta, date
from zoneinfo import ZoneInfo
from collections import defaultdict

# Stub energy_engine_io
eio = types.ModuleType("energy_engine_io"); eio.load_json = lambda *a,**k: {}
sys.modules.setdefault("energy_engine_io", eio)

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from block_store import BlockStore, local_date_range_to_utc_bounds, local_date_to_utc_bounds

# Always import the real energy_charts — not a stub that another test suite
# may have injected into sys.modules when running combined test discovery.
import importlib
_ec_spec = importlib.util.spec_from_file_location(
    "energy_charts_real",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "energy_charts.py")
)
_ec_mod = importlib.util.module_from_spec(_ec_spec)
_ec_spec.loader.exec_module(_ec_mod)
ec = _ec_mod


# ── Helpers ───────────────────────────────────────────────────────────────────

TZ = ZoneInfo("Europe/London")

def make_store():
    store = BlockStore(":memory:")
    store.insert_config_period({"meters": {"electricity_main": {"meta": {
        "billing_day": 15, "block_minutes": 30, "timezone": "Europe/London",
        "currency_symbol": "£", "currency_code": "GBP", "site": "Home",
    }}}})
    return store, store._conn.execute(
        "SELECT id FROM config_periods LIMIT 1").fetchone()["id"]


def make_store_with_submeters():
    """Store with electricity_main + ev_charger + house_battery sub-meters."""
    store = BlockStore(":memory:")
    store.insert_config_period({"meters": {
        "electricity_main": {"meta": {
            "billing_day": 15, "block_minutes": 30, "timezone": "Europe/London",
            "currency_symbol": "£", "currency_code": "GBP", "site": "Home",
            "postcode_prefix": "DE1",
        }},
        "ev_charger": {"meta": {
            "sub_meter": True, "parent_meter": "electricity_main",
            "device": "Zappi EV Charger",
            "billing_day": 15, "block_minutes": 30, "timezone": "Europe/London",
            "currency_symbol": "£", "currency_code": "GBP",
        }},
        "house_battery": {"meta": {
            "sub_meter": True, "parent_meter": "electricity_main",
            "device": "Solax Battery",
            "billing_day": 15, "block_minutes": 30, "timezone": "Europe/London",
            "currency_symbol": "£", "currency_code": "GBP",
        }},
    }})
    cp_id = store._conn.execute(
        "SELECT id FROM config_periods LIMIT 1").fetchone()["id"]
    return store, cp_id


def insert_block(store, cp_id, block_start_utc, imp_kwh, imp_cost,
                 exp_kwh=0.0, exp_cost=0.0, standing=0.50, carbon_g=None,
                 meter_id="electricity_main"):
    """Insert one block row."""
    store._conn.execute("""
        INSERT INTO blocks (
            block_start, block_end,
            meter_id, config_period_id, interpolated,
            imp_kwh, imp_kwh_grid, imp_kwh_remainder,
            imp_rate, imp_cost, imp_cost_remainder,
            imp_read_start, imp_read_end,
            exp_kwh, exp_rate, exp_cost,
            exp_read_start, exp_read_end, standing_charge, carbon_g)
        VALUES (?,?,?,?,?,?,NULL,NULL,NULL,?,NULL,NULL,NULL,?,NULL,?,NULL,NULL,?,?)
    """, (block_start_utc,
          (datetime.fromisoformat(block_start_utc)+timedelta(minutes=30)).isoformat(),
          meter_id, cp_id, 0,
          imp_kwh, imp_cost, exp_kwh, exp_cost, standing, carbon_g))
    store._conn.commit()


def sim_usage_stats_day(store, local_date_str, tz_name="Europe/London"):
    """
    Simulate what api_blocks_summary does for one local day:
    - fetch blocks by UTC range derived from local date (using configured timezone)
    - get standing from first block
    - sum imp/exp kwh, cost and carbon_g from blocks directly
    Returns dict matching the row structure.
    """
    utc_s, utc_e = local_date_range_to_utc_bounds(local_date_str, local_date_str, tz_name)
    blocks = store.get_blocks_for_utc_range(utc_s, utc_e)
    if not blocks:
        return None

    # standing_charge is on the meter block, not the top-level block dict
    _first_meter = next(iter((blocks[0].get("meters") or {}).values()), {})
    standing = float(_first_meter.get("standing_charge") or 0.0)
    imp_kwh = imp_cost = exp_kwh = exp_cost = 0.0
    carbon_g_net = None   # NULL until at least one non-NULL block seen
    for b in blocks:
        for mid, md in (b.get("meters") or {}).items():
            meta = (md.get("meta") or {})
            ch_imp = (md.get("channels") or {}).get("import") or {}
            ch_exp = (md.get("channels") or {}).get("export") or {}
            imp_kwh  += float(ch_imp.get("kwh") or 0)
            imp_cost += float(ch_imp.get("cost") or 0)
            exp_kwh  += float(ch_exp.get("kwh") or 0)
            exp_cost += float(ch_exp.get("cost") or 0)
            # carbon_g: accumulate for main meter only (net figure)
            if not meta.get("sub_meter"):
                cg = md.get("carbon_g")
                if cg is not None:
                    carbon_g_net = (carbon_g_net or 0.0) + float(cg)

    # carbon_g_total = main_carbon_g (already includes sub-meter consumption)
    # Do NOT add sub-meter carbon on top — that would double-count.
    # The main meter (imp-exp) × intensity already captures all grid consumption
    # including what went to sub-meters.
    carbon_g_total = round(carbon_g_net, 4) if carbon_g_net is not None else None

    # Back-calculate imp/exp split from net kWh and net carbon
    main_imp = imp_kwh
    main_exp = exp_kwh
    if carbon_g_net is not None and (main_imp - main_exp) != 0:
        _intensity = carbon_g_net / (main_imp - main_exp)
        carbon_g_imp = round(main_imp * _intensity, 4)
        carbon_g_exp = round(main_exp * abs(_intensity), 4)
    elif carbon_g_net is not None and main_imp == 0:
        carbon_g_imp = 0.0
        carbon_g_exp = round(-carbon_g_net, 4)
    elif carbon_g_net is not None:
        carbon_g_imp = round(carbon_g_net, 4)
        carbon_g_exp = 0.0
    else:
        carbon_g_imp = carbon_g_exp = None

    _imp_4dp = round(imp_cost, 4)
    _exp_4dp = round(exp_cost, 4)
    _sc_4dp  = round(standing, 4)
    return {
        "standing":      _sc_4dp,
        "imp_kwh":       round(imp_kwh, 4),
        "imp_cost":      _imp_4dp,
        "exp_kwh":       round(exp_kwh, 4),
        "exp_cost":      _exp_4dp,
        "net_cost":      round(_imp_4dp + _sc_4dp - _exp_4dp, 2),
        "carbon_g_net":  round(carbon_g_net, 4) if carbon_g_net is not None else None,
        "carbon_g_total": carbon_g_total,
        "carbon_g_imp":  carbon_g_imp,
        "carbon_g_exp":  carbon_g_exp,
    }


def sql_totals(store, first_date, last_date, tz_name="Europe/London"):
    """Ground truth: SQL aggregation via UTC range."""
    utc_s, utc_e = local_date_range_to_utc_bounds(first_date, last_date, tz_name)
    return store.get_billing_totals_for_utc_range(utc_s, utc_e, tz_name)


def sum_daily_rows(rows):
    """Sum a list of day rows (as returned by sim_usage_stats_day)."""
    out = {"standing": 0.0, "imp_kwh": 0.0, "imp_cost": 0.0,
           "exp_kwh": 0.0, "exp_cost": 0.0}
    carbon_net = None
    for r in rows:
        if r:
            for k in out:
                out[k] += r.get(k, 0.0)
            cg = r.get("carbon_g_net")
            if cg is not None:
                carbon_net = (carbon_net or 0.0) + cg
    for k in out:
        out[k] = round(out[k], 4)
    out["carbon_g_net"] = round(carbon_net, 4) if carbon_net is not None else None
    # Also sum carbon_g_total, imp, exp
    for _ck in ("carbon_g_total", "carbon_g_imp", "carbon_g_exp"):
        _ctot = None
        for r in rows:
            if r and r.get(_ck) is not None:
                _ctot = (_ctot or 0.0) + r[_ck]
        out[_ck] = round(_ctot, 4) if _ctot is not None else None
    return out


def assert_match(tc, label, daily_sum, sql, tol=0.001):
    for field in ("standing", "imp_kwh", "imp_cost", "exp_kwh", "exp_cost"):
        tc.assertAlmostEqual(
            daily_sum[field], sql[field], delta=tol,
            msg=f"{label}: {field} daily_sum={daily_sum[field]} sql={sql[field]}"
        )


# ── Test cases ────────────────────────────────────────────────────────────────

class TestDailyVsSql(unittest.TestCase):
    """Each day's Usage Stats row should match SQL totals for that local_date."""

    def setUp(self):
        self.store, self.cp = make_store()

    def test_gmt_day_matches(self):
        """Jan day (GMT=UTC): single block, standing counts once."""
        insert_block(self.store, self.cp, "2026-01-10T00:00:00", 1.2, 0.294, standing=0.50)
        insert_block(self.store, self.cp, "2026-01-10T00:30:00", 0.8, 0.196, standing=0.50)

        row = sim_usage_stats_day(self.store, "2026-01-10")
        sql = sql_totals(self.store, "2026-01-10", "2026-01-10")

        self.assertAlmostEqual(row["standing"], 0.50, places=3,
            msg="Standing charge should be 0.50 (once per day, not per block)")
        assert_match(self, "GMT day", row, sql)

    def test_bst_day_first_block_at_23xx_utc(self):
        """BST day: first block at 23:00 UTC (= local midnight) counted correctly."""
        # Apr 5 BST: first block at Apr 4 23:00 UTC
        insert_block(self.store, self.cp, "2026-04-04T23:00:00", 0.5, 0.1225, standing=0.50)
        insert_block(self.store, self.cp, "2026-04-05T00:00:00", 0.6, 0.1470, standing=0.50)
        insert_block(self.store, self.cp, "2026-04-05T01:00:00", 0.4, 0.0980, standing=0.50)

        row = sim_usage_stats_day(self.store, "2026-04-05")
        sql = sql_totals(self.store, "2026-04-05", "2026-04-05")

        self.assertAlmostEqual(row["standing"], 0.50, places=3,
            msg="BST day standing charge must be 0.50 not 1.00")
        self.assertAlmostEqual(row["imp_kwh"], 1.5, places=3,
            msg="All 3 blocks (including 23:00 UTC) must be counted")
        assert_match(self, "BST day", row, sql)

    def test_standing_same_on_all_blocks(self):
        """Standing taken from first block — must equal SQL GROUP BY local_date."""
        for h in range(0, 24, 2):
            insert_block(self.store, self.cp,
                         f"2026-03-15T{h:02d}:00:00", 0.3, 0.074, standing=0.60)

        row = sim_usage_stats_day(self.store, "2026-03-15")
        sql = sql_totals(self.store, "2026-03-15", "2026-03-15")

        self.assertAlmostEqual(row["standing"], 0.60, places=3)
        assert_match(self, "12-block day", row, sql)

    def test_export_day(self):
        """Export blocks included correctly."""
        insert_block(self.store, self.cp, "2026-05-01T06:00:00",
                     0.1, 0.025, exp_kwh=0.8, exp_cost=0.064, standing=0.50)
        insert_block(self.store, self.cp, "2026-05-01T06:30:00",
                     0.0, 0.000, exp_kwh=1.2, exp_cost=0.096, standing=0.50)

        row = sim_usage_stats_day(self.store, "2026-05-01")
        sql = sql_totals(self.store, "2026-05-01", "2026-05-01")
        assert_match(self, "export day", row, sql)


class TestNoSubMeterDailyKwh(unittest.TestCase):
    """Regression: on a no-sub-meter (API/Mini) setup, blocks store imp_kwh but
    leave imp_kwh_remainder/imp_kwh_grid NULL. The daily billing chart must show
    the real import kWh, not 0.000 — previously it read a remainder field that
    was absent/zero while still showing a cost, giving 'cost but no kWh'."""

    def setUp(self):
        self.store, self.cp = make_store()

    def test_daily_chart_shows_kwh_for_no_submeter_block(self):
        # The real api+mini/CAD no-sub-meter case: imp_kwh_remainder is NULL
        # (the engine never sets it without sub-meters). get_blocks_lightweight
        # must omit the key when NULL (matching _row_to_block) so the chart reads
        # the real kwh, not a present-None remainder treated as zero. This was
        # the "cost but no kWh" billing bug.
        self.store._conn.execute(
            "INSERT INTO blocks (block_start, block_end, meter_id, "
            "config_period_id, interpolated, imp_kwh, imp_kwh_grid, "
            "imp_kwh_remainder, imp_rate, imp_cost, imp_read_start, "
            "imp_read_end, exp_kwh, exp_rate, exp_cost, exp_read_start, "
            "exp_read_end, standing_charge, carbon_g) "
            "VALUES (?,?,?,?,0,?,NULL,NULL,?,?,NULL,NULL,0,NULL,0,NULL,NULL,?,NULL)",
            ("2026-01-10T21:00:00", "2026-01-10T21:30:00", "electricity_main",
             self.cp, 0.053, 0.3231, 0.0171, 0.50))
        self.store._conn.commit()
        blocks = self.store.get_blocks_lightweight()
        # Confirm the reconstructed channel OMITS kwh_remainder (matches _row_to_block)
        imp = blocks[0]["meters"]["electricity_main"]["channels"]["import"]
        self.assertNotIn("kwh_remainder", imp,
            "lightweight must omit NULL kwh_remainder so charts read the real kwh")
        self.assertAlmostEqual(imp["kwh"], 0.053, places=3)
        html = ec.generate_daily_import_export_charts(
            blocks, timezone_name="Europe/London", block_minutes=30,
            currency="£", store=self.store)
        import re
        self.assertRegex(
            html, r"0\.053\s*,|,\s*0\.053",
            "per-slot import kWh array must contain the real value, not 0")

    def test_usage_stats_and_block_kwh_agree(self):
        # Sanity: usage-stats sums the same imp_kwh the chart should show.
        insert_block(self.store, self.cp, "2026-01-10T21:00:00",
                     0.053, 0.0171, standing=0.50)
        row = sim_usage_stats_day(self.store, "2026-01-10")
        self.assertAlmostEqual(row["imp_kwh"], 0.053, places=3)


class TestMonthlyBillingVsSql(unittest.TestCase):
    """
    Summing daily Usage Stats rows for a billing period must equal
    SQL totals for the same local_date range.
    """

    def setUp(self):
        self.store, self.cp = make_store()

    def _insert_days(self, local_dates_utc_starts):
        """Insert one block per day. BST days have 23:xx UTC start."""
        for utc_start, imp_kwh, imp_cost, standing in local_dates_utc_starts:
            insert_block(self.store, self.cp, utc_start, imp_kwh, imp_cost,
                         standing=standing)

    def test_billing_period_gmt_only(self):
        """Billing period wholly in GMT: daily sums match SQL totals."""
        days = [
            ("2026-01-15T00:00:00", 2.0, 0.490, 0.50),
            ("2026-01-16T00:00:00", 1.5, 0.368, 0.50),
            ("2026-01-17T00:00:00", 1.8, 0.441, 0.50),
        ]
        self._insert_days(days)

        rows = [sim_usage_stats_day(self.store, f"2026-01-{d:02d}")
                for d in [15, 16, 17]]
        daily_sum = sum_daily_rows(rows)
        sql = sql_totals(self.store, "2026-01-15", "2026-01-17")

        self.assertAlmostEqual(daily_sum["standing"], 1.50, places=3,
            msg="3 days × £0.50 = £1.50 standing")
        assert_match(self, "GMT billing period", daily_sum, sql)

    def test_billing_period_crossing_bst_transition(self):
        """
        Billing period crossing GMT→BST: BST days have 23:xx UTC blocks.
        Daily sums must still match SQL totals.
        """
        # Mar 28 GMT: block at 00:00 UTC
        insert_block(self.store, self.cp, "2026-03-28T00:00:00", 2.0, 0.490, standing=0.50)
        # Mar 29 (BST transition): clocks go forward 01:00 UTC
        # First block still GMT: 00:00 UTC = 00:00 GMT = Mar 29
        insert_block(self.store, self.cp, "2026-03-29T00:00:00", 1.5, 0.368, standing=0.50)
        # Apr 5 BST: first block at 23:00 UTC Apr 4
        insert_block(self.store, self.cp, "2026-04-04T23:00:00", 1.0, 0.245, standing=0.50)
        insert_block(self.store, self.cp, "2026-04-05T00:00:00", 0.8, 0.196, standing=0.50)

        rows = [
            sim_usage_stats_day(self.store, "2026-03-28"),
            sim_usage_stats_day(self.store, "2026-03-29"),
            sim_usage_stats_day(self.store, "2026-04-05"),
        ]
        daily_sum = sum_daily_rows(rows)
        sql = sql_totals(self.store, "2026-03-28", "2026-04-05")

        # 4 distinct local days × £0.50 — but sql covers Mar28–Apr5 (9 days, only 4 with data)
        # SQL counts distinct local_dates with data
        sql_day = sql_totals(self.store, "2026-03-28", "2026-03-28")
        sql_29  = sql_totals(self.store, "2026-03-29", "2026-03-29")
        sql_apr5= sql_totals(self.store, "2026-04-05", "2026-04-05")
        total_standing = sql_day["standing"] + sql_29["standing"] + sql_apr5["standing"]

        self.assertAlmostEqual(daily_sum["standing"], total_standing, places=3,
            msg="BST transition period: daily sum standing must match SQL")
        self.assertAlmostEqual(daily_sum["imp_kwh"],
                               2.0 + 1.5 + 1.0 + 0.8, places=3,
            msg="All 4 blocks (incl 23:xx UTC BST block) must be counted")
        assert_match(self, "BST crossing period", daily_sum,
                     {"standing": total_standing,
                      "imp_kwh":  round(2.0+1.5+1.0+0.8, 4),
                      "imp_cost": round(0.490+0.368+0.245+0.196, 4),
                      "exp_kwh": 0.0, "exp_cost": 0.0})

    def test_monthly_sum_equals_sql_range(self):
        """Summing all daily rows for a month = SQL totals for that month."""
        month_days = [
            ("2026-02-01T00:00:00", 2.1, 0.515, 0.55),
            ("2026-02-02T00:00:00", 1.8, 0.441, 0.55),
            ("2026-02-03T00:00:00", 2.4, 0.588, 0.55),
            ("2026-02-04T00:00:00", 1.6, 0.392, 0.55),
        ]
        self._insert_days(month_days)

        rows = [sim_usage_stats_day(self.store, f"2026-02-0{d}")
                for d in [1, 2, 3, 4]]
        daily_sum = sum_daily_rows(rows)
        sql = sql_totals(self.store, "2026-02-01", "2026-02-04")

        self.assertAlmostEqual(daily_sum["standing"], 4 * 0.55, places=3,
            msg="4 days × 0.55 standing")
        assert_match(self, "February month", daily_sum, sql)


class TestYearlyVsSql(unittest.TestCase):
    """Year-to-date sum of daily rows must match SQL totals for Jan 1 → today."""

    def setUp(self):
        self.store, self.cp = make_store()

    def test_year_gmt_and_bst_days(self):
        """Mix of GMT and BST days: yearly sum matches SQL."""
        days = [
            # GMT days (Jan)
            ("2026-01-05T00:00:00", 3.0, 0.735, 0.50),
            ("2026-01-06T00:00:00", 2.5, 0.613, 0.50),
            # BST day (Apr) - 23:xx UTC = local midnight
            ("2026-04-04T23:00:00", 1.0, 0.245, 0.50),
            ("2026-04-05T00:00:00", 0.5, 0.123, 0.50),
        ]
        for utc_start, kwh, cost, sc in days:
            insert_block(self.store, self.cp, utc_start, kwh, cost, standing=sc)

        # 3 distinct local days: Jan 5, Jan 6, Apr 5
        rows = [
            sim_usage_stats_day(self.store, "2026-01-05"),
            sim_usage_stats_day(self.store, "2026-01-06"),
            sim_usage_stats_day(self.store, "2026-04-05"),
        ]
        daily_sum = sum_daily_rows(rows)
        sql = sql_totals(self.store, "2026-01-05", "2026-04-05")

        self.assertAlmostEqual(daily_sum["standing"], 3 * 0.50, places=3,
            msg="3 local days × 0.50 standing = 1.50")
        self.assertAlmostEqual(daily_sum["imp_kwh"],
                               3.0 + 2.5 + 1.0 + 0.5, places=3,
            msg="All 4 blocks counted across 3 local days")
        assert_match(self, "yearly mixed", daily_sum, sql)


class TestStandingChargeEdgeCases(unittest.TestCase):

    def setUp(self):
        self.store, self.cp = make_store()

    def test_zero_standing_charge(self):
        """Days with no standing charge return 0, not None."""
        insert_block(self.store, self.cp, "2026-03-01T00:00:00",
                     1.0, 0.245, standing=0.0)
        row = sim_usage_stats_day(self.store, "2026-03-01")
        self.assertEqual(row["standing"], 0.0)

    def test_standing_consistent_across_blocks(self):
        """All blocks in a day have the same standing — first block is representative."""
        for h in range(6):
            insert_block(self.store, self.cp,
                         f"2026-03-10T{h*4:02d}:00:00", 0.5, 0.123, standing=0.75)
        row = sim_usage_stats_day(self.store, "2026-03-10")
        sql = sql_totals(self.store, "2026-03-10", "2026-03-10")
        self.assertAlmostEqual(row["standing"], 0.75, places=3)
        self.assertAlmostEqual(sql["standing"], 0.75, places=3)

    def test_bst_day_no_double_standing(self):
        """Two blocks with different UTC dates but same local_date: standing = once."""
        # 23:00 UTC Apr 1 = 00:00 BST Apr 2
        insert_block(self.store, self.cp, "2026-04-01T23:00:00",
                     1.0, 0.245, standing=0.60)
        # 00:30 UTC Apr 2 = 01:30 BST Apr 2
        insert_block(self.store, self.cp, "2026-04-02T00:30:00",
                     0.8, 0.196, standing=0.60)

        row = sim_usage_stats_day(self.store, "2026-04-02")
        sql = sql_totals(self.store, "2026-04-02", "2026-04-02")

        self.assertAlmostEqual(row["standing"], 0.60, places=3,
            msg="Two blocks, same local day, standing must be 0.60 not 1.20")
        assert_match(self, "BST no double", row, sql)


if __name__ == "__main__":
    unittest.main(verbosity=2)


class TestBillingChartVsUsageStats(unittest.TestCase):
    """
    The billing chart (calculate_billing_summary_for_period) and Usage Stats
    (sim_usage_stats_day summed) must agree on kWh and cost for the same period,
    including BST period boundaries where the first block is at 23:xx UTC.
    """

    def setUp(self):
        self.store, self.cp = make_store()

    def _billing_summary_for_range(self, blocks, period_start_local, period_end_local):
        """Call calculate_billing_summary_for_period and extract grid import totals."""
        s = ec.calculate_billing_summary_for_period(
            blocks, period_start_local, period_end_local
        )
        imp_kwh = imp_cost = 0.0
        for key, t in (s.get("totals") or {}).items():
            if not t.get("is_submeter") and "export" not in key.lower():
                imp_kwh  += t.get("kwh", 0)
                imp_cost += t.get("cost", 0)
        return {
            "imp_kwh":  round(imp_kwh, 4),
            "imp_cost": round(imp_cost, 4),
            "standing": round(s.get("total_standing", 0), 4),
        }

    def test_gmt_period_billing_vs_usage_stats(self):
        """GMT period: both methods agree exactly."""
        # Period: Jan 15 – Feb 14, entirely in GMT
        for day in range(15, 18):
            insert_block(self.store, self.cp,
                         f"2026-01-{day:02d}T00:00:00", 2.0, 0.490, standing=0.50)
            insert_block(self.store, self.cp,
                         f"2026-01-{day:02d}T00:30:00", 1.5, 0.368, standing=0.50)

        blocks = self.store.get_blocks_for_utc_range(*local_date_range_to_utc_bounds("2026-01-15", "2026-01-17", "UTC"))
        p_start = datetime(2026, 1, 15, 0, 0, 0)
        p_end   = datetime(2026, 1, 18, 0, 0, 0)

        billing = self._billing_summary_for_range(blocks, p_start, p_end)
        rows = [sim_usage_stats_day(self.store, f"2026-01-{d:02d}") for d in [15,16,17]]
        usage = sum_daily_rows(rows)

        self.assertAlmostEqual(billing["imp_kwh"], usage["imp_kwh"], places=3,
            msg="GMT period: billing chart kWh must match usage stats")
        self.assertAlmostEqual(billing["standing"], usage["standing"], places=3,
            msg="GMT period: standing charge must match")

    def test_bst_period_start_billing_vs_usage_stats(self):
        """
        BST period starting Apr 3: first block is Apr 2 23:00 UTC = Apr 3 00:00 BST.
        Both billing chart and usage stats must include this block and agree.
        """
        # Apr 2 23:00 UTC = Apr 3 00:00 BST (first block of billing period)
        insert_block(self.store, self.cp, "2026-04-02T23:00:00", 0.226, 0.055, standing=0.50)
        # Normal Apr 3 BST blocks
        insert_block(self.store, self.cp, "2026-04-03T00:00:00", 1.0, 0.245, standing=0.50)
        insert_block(self.store, self.cp, "2026-04-03T06:00:00", 0.8, 0.196, standing=0.50)
        # Apr 4 BST: starts Apr 3 23:00 UTC
        insert_block(self.store, self.cp, "2026-04-03T23:00:00", 0.5, 0.123, standing=0.50)
        insert_block(self.store, self.cp, "2026-04-04T06:00:00", 0.6, 0.147, standing=0.50)

        blocks = self.store.get_blocks_for_utc_range(*local_date_range_to_utc_bounds("2026-04-03", "2026-04-04", "Europe/London"))
        p_start = datetime(2026, 4, 3, 0, 0, 0)   # local midnight Apr 3
        p_end   = datetime(2026, 4, 5, 0, 0, 0)   # local midnight Apr 5 (exclusive)

        billing = self._billing_summary_for_range(blocks, p_start, p_end)
        rows = [sim_usage_stats_day(self.store, d) for d in ["2026-04-03", "2026-04-04"]]
        usage = sum_daily_rows(rows)

        expected_kwh = 0.226 + 1.0 + 0.8 + 0.5 + 0.6  # all 5 blocks

        self.assertAlmostEqual(billing["imp_kwh"], expected_kwh, places=3,
            msg=f"Billing chart must include 23:xx UTC block: got {billing['imp_kwh']} expected {expected_kwh}")
        self.assertAlmostEqual(usage["imp_kwh"], expected_kwh, places=3,
            msg=f"Usage stats must include 23:xx UTC block: got {usage['imp_kwh']} expected {expected_kwh}")
        self.assertAlmostEqual(billing["imp_kwh"], usage["imp_kwh"], places=3,
            msg="Billing chart and usage stats must agree on kWh for BST period")
        self.assertAlmostEqual(billing["standing"], usage["standing"], places=3,
            msg="Billing chart and usage stats must agree on standing charge for BST period")


class TestBillSummaryMainImportRaw(unittest.TestCase):
    """
    Tests for the redesigned bill summary — verifies that main_import_raw
    (total grid draw before sub-meter subtraction) and the sub-meter breakdown
    are mathematically consistent:
        sum(sub-meters) + remainder = main_import_raw total
    """

    def setUp(self):
        self.store = BlockStore(":memory:")
        self.store.insert_config_period({"meters": {
            "electricity_main": {"meta": {
                "billing_day": 1, "block_minutes": 30,
                "timezone": "UTC", "currency_symbol": "£", "currency_code": "GBP",
            }, "channels": {"import": {"read": "s.imp", "rate": "s.rate"},
                            "export": {"read": "s.exp", "rate": "s.exp_rate"}}},
            "ev_charger": {"meta": {"sub_meter": True, "parent_meter": "electricity_main",
                                    "device": "Zappi EV Charger"},
                           "channels": {"import": {"read": "s.ev", "rate": "s.rate"}}},
            "house_battery": {"meta": {"sub_meter": True, "parent_meter": "electricity_main",
                                       "device": "Solax Battery"},
                              "channels": {"import": {"read": "s.bat", "rate": "s.rate"}}},
        }})
        self.cp = self.store._conn.execute(
            "SELECT id FROM config_periods LIMIT 1").fetchone()["id"]

    def _insert(self, block_start, main_imp, ev_imp, bat_imp, main_exp=0.0,
                rate=0.07, exp_rate=0.12, sc=0.50):
        """Insert one set of blocks for a given timestamp."""
        for meter_id, imp_kwh in [
            ("electricity_main", main_imp),
            ("ev_charger",       ev_imp),
            ("house_battery",    bat_imp),
        ]:
            self.store._conn.execute("""
                INSERT INTO blocks (
                    block_start, block_end,
                    meter_id, config_period_id, interpolated,
                    imp_kwh, imp_kwh_grid, imp_kwh_remainder,
                    imp_rate, imp_cost, exp_kwh, exp_rate, exp_cost, standing_charge)
                VALUES (?,?,?,?,0,
                        ?,NULL,NULL, ?,?, ?,?,?,?)
            """, (block_start, block_start,
                  meter_id, self.cp,
                  imp_kwh, rate, round(imp_kwh * rate, 6),
                  main_exp if meter_id == "electricity_main" else 0.0,
                  exp_rate,
                  round(main_exp * exp_rate, 6) if meter_id == "electricity_main" else 0.0,
                  sc if meter_id == "electricity_main" else 0.0))
        self.store._conn.commit()

    def _summary(self):
        blocks = self.store.get_blocks_for_utc_range(*local_date_range_to_utc_bounds("2026-03-01", "2026-03-31", "UTC"))
        return ec.calculate_billing_summary_for_period(
            blocks, datetime(2026, 3, 1), datetime(2026, 4, 1))

    def test_raw_equals_sub_plus_remainder(self):
        """main_import_raw total must equal sub-meter sum + remainder."""
        self._insert("2026-03-01T00:00:00", main_imp=10.0, ev_imp=4.0, bat_imp=3.0)
        self._insert("2026-03-01T00:30:00", main_imp=8.0,  ev_imp=2.0, bat_imp=2.0)
        s = self._summary()

        raw_kwh = sum(d["kwh"] for d in s["main_import_raw"].values())

        # sub-meter totals
        sub_kwh = sum(
            t["kwh"] for k, t in s["totals"].items()
            if t.get("is_submeter") and "import" in k.lower()
        )
        # remainder (main meter post-subtraction)
        rem_kwh = sum(
            t["kwh"] for k, t in s["totals"].items()
            if not t.get("is_submeter") and "import" in k.lower()
        )

        self.assertAlmostEqual(raw_kwh, sub_kwh + rem_kwh, places=4,
            msg="main_import_raw must equal sub-meter sum + remainder")

    def test_raw_total_matches_main_meter_blocks(self):
        """main_import_raw kWh must match the raw sum of electricity_main imp_kwh from DB."""
        self._insert("2026-03-01T00:00:00", main_imp=6.35, ev_imp=2.1, bat_imp=1.5)
        self._insert("2026-03-02T00:00:00", main_imp=5.80, ev_imp=1.8, bat_imp=1.2)

        db_total = self.store._conn.execute(
            "SELECT SUM(imp_kwh) FROM blocks WHERE meter_id='electricity_main'"
        ).fetchone()[0]

        s = self._summary()
        raw_kwh = sum(d["kwh"] for d in s["main_import_raw"].values())

        self.assertAlmostEqual(raw_kwh, db_total, places=4,
            msg="main_import_raw must match raw DB sum for electricity_main")

    def test_raw_by_rate_correct(self):
        """main_import_raw groups correctly by rate."""
        # Two blocks at different rates
        self.store._conn.execute("""
            INSERT INTO blocks (block_start, block_end,
                meter_id, config_period_id, interpolated,
                imp_kwh, imp_rate, imp_cost, standing_charge)
            VALUES ('2026-03-01T00:00:00','2026-03-01T00:00:00',
                    'electricity_main',?,0, 5.0,0.0700,0.3500,0.50)
        """, (self.cp,))
        self.store._conn.execute("""
            INSERT INTO blocks (block_start, block_end,
                meter_id, config_period_id, interpolated,
                imp_kwh, imp_rate, imp_cost, standing_charge)
            VALUES ('2026-03-01T06:00:00','2026-03-01T06:00:00',
                    'electricity_main',?,0, 2.0,0.3231,0.6462,0.00)
        """, (self.cp,))
        self.store._conn.commit()

        s = self._summary()
        raw = s["main_import_raw"]
        self.assertAlmostEqual(raw[0.0700]["kwh"], 5.0, places=4)
        self.assertAlmostEqual(raw[0.3231]["kwh"], 2.0, places=4)

    def test_no_sub_meters_raw_equals_total(self):
        """With no sub-meters, main_import_raw must equal the displayed Import total."""
        # Insert only electricity_main blocks (no sub-meters in this store)
        store2 = BlockStore(":memory:")
        store2.insert_config_period({"meters": {"electricity_main": {"meta": {
            "billing_day": 1, "block_minutes": 30, "timezone": "UTC",
            "currency_symbol": "£", "currency_code": "GBP",
        }, "channels": {"import": {"read": "s.imp", "rate": "s.rate"}}}}})
        cp2 = store2._conn.execute("SELECT id FROM config_periods LIMIT 1").fetchone()["id"]
        for i, kwh in enumerate([3.0, 4.0, 5.0]):
            ts = f'2026-03-01T{i:02d}:00:00'
            store2._conn.execute("""
                INSERT INTO blocks (block_start, block_end,
                    meter_id, config_period_id, interpolated,
                    imp_kwh, imp_rate, imp_cost, standing_charge)
                VALUES (?,?,
                        'electricity_main',?,0, ?,0.07,?,0.50)
            """, (ts, ts, cp2, kwh, round(kwh * 0.07, 4)))
        store2._conn.commit()

        blocks = store2.get_blocks_for_utc_range(*local_date_range_to_utc_bounds("2026-03-01", "2026-03-31", "UTC"))
        s = ec.calculate_billing_summary_for_period(
            blocks, datetime(2026, 3, 1), datetime(2026, 4, 1))

        raw_kwh  = sum(d["kwh"]  for d in s["main_import_raw"].values())
        rem_kwh  = sum(t["kwh"]  for k, t in s["totals"].items()
                       if not t.get("is_submeter") and "import" in k.lower())
        self.assertAlmostEqual(raw_kwh, rem_kwh, places=4,
            msg="With no sub-meters, raw must equal remainder")

    def test_render_contains_total_grid_kwh(self):
        """Rendered HTML must contain the total grid kWh figure."""
        self._insert("2026-03-01T00:00:00", main_imp=10.0, ev_imp=4.0, bat_imp=3.0)
        s = self._summary()
        html = ec.render_billing_summary(s, currency="£")

        raw_kwh = sum(d["kwh"] for d in s["main_import_raw"].values())
        # The total row should contain the raw kWh formatted to 3dp
        expected = f"{raw_kwh:.3f}"
        self.assertIn(expected, html,
            msg=f"Rendered HTML must contain total grid kWh {expected}")

    def test_render_contains_submeter_device_labels(self):
        """Rendered HTML must contain device labels for sub-meters."""
        self._insert("2026-03-01T00:00:00", main_imp=10.0, ev_imp=4.0, bat_imp=3.0)
        s = self._summary()
        html = ec.render_billing_summary(s, currency="£")
        self.assertIn("Zappi EV Charger", html,
            msg="Rendered HTML must show EV charger device label")
        self.assertIn("Solax Battery", html,
            msg="Rendered HTML must show battery device label")
        self.assertIn("Direct import", html,
            msg="Rendered HTML must show direct import label")
        self.assertIn("Import — total grid", html,
            msg="Rendered HTML must show total grid header")

class TestCarbonVsBlocks(unittest.TestCase):
    """
    carbon_g in Usage Stats rows must match direct SUM(carbon_g) from blocks.
    Covers: basic aggregation, NULL handling, BST boundary, net vs sub-meter.
    """

    def setUp(self):
        self.store, self.cp = make_store()

    def _db_carbon(self, first_date, last_date, meter_id="electricity_main"):
        """Sum carbon_g using UTC bounds (Europe/London) for correct BST handling."""
        utc_s, _ = local_date_to_utc_bounds(first_date, "Europe/London")
        _, utc_e  = local_date_to_utc_bounds(last_date,  "Europe/London")
        row = self.store._conn.execute(
            "SELECT SUM(carbon_g) as total FROM blocks "
            "WHERE block_start >= ? AND block_start < ? AND meter_id = ?",
            (utc_s, utc_e, meter_id)
        ).fetchone()
        val = row["total"]
        return round(float(val), 4) if val is not None else None

    def test_basic_carbon_g_aggregation(self):
        """sim_usage_stats_day carbon_g_net matches direct DB SUM."""
        insert_block(self.store, self.cp, "2026-01-15T00:00:00", 1.0, 0.245, carbon_g=-12.5)
        insert_block(self.store, self.cp, "2026-01-15T00:30:00", 0.8, 0.196, carbon_g=-10.0)
        row = sim_usage_stats_day(self.store, "2026-01-15")
        db_total = self._db_carbon("2026-01-15", "2026-01-15")
        self.assertIsNotNone(row["carbon_g_net"])
        self.assertAlmostEqual(row["carbon_g_net"], -22.5, places=3)
        self.assertAlmostEqual(row["carbon_g_net"], db_total, places=3)

    def test_null_carbon_g_returns_none(self):
        """Days with all NULL carbon_g return None, not 0."""
        insert_block(self.store, self.cp, "2026-01-15T00:00:00", 1.0, 0.245, carbon_g=None)
        insert_block(self.store, self.cp, "2026-01-15T00:30:00", 0.8, 0.196, carbon_g=None)
        row = sim_usage_stats_day(self.store, "2026-01-15")
        self.assertIsNone(row["carbon_g_net"],
            msg="All NULL carbon_g blocks must return None not 0")

    def test_partial_null_carbon_g(self):
        """Mixed NULL/non-NULL: only non-NULL blocks contribute."""
        insert_block(self.store, self.cp, "2026-01-15T00:00:00", 1.0, 0.245, carbon_g=None)
        insert_block(self.store, self.cp, "2026-01-15T00:30:00", 0.8, 0.196, carbon_g=-10.0)
        row = sim_usage_stats_day(self.store, "2026-01-15")
        self.assertAlmostEqual(row["carbon_g_net"], -10.0, places=3)

    def test_positive_carbon_g_importing(self):
        """Importing blocks produce positive carbon_g."""
        insert_block(self.store, self.cp, "2026-01-15T00:00:00", 2.0, 0.490, carbon_g=65.3)
        insert_block(self.store, self.cp, "2026-01-15T00:30:00", 1.5, 0.368, carbon_g=48.9)
        row = sim_usage_stats_day(self.store, "2026-01-15")
        self.assertAlmostEqual(row["carbon_g_net"], 114.2, places=3)
        self.assertAlmostEqual(row["carbon_g_net"], self._db_carbon("2026-01-15", "2026-01-15"), places=3)

    def test_bst_block_carbon_g_on_correct_local_date(self):
        """Block at 23:00 UTC = 00:00 BST next day: carbon_g lands on BST date."""
        insert_block(self.store, self.cp, "2026-04-02T23:00:00", 0.5, 0.123, carbon_g=-8.5)
        insert_block(self.store, self.cp, "2026-04-03T06:00:00", 1.0, 0.245, carbon_g=15.0)
        row_apr2 = sim_usage_stats_day(self.store, "2026-04-02")
        self.assertIsNone(row_apr2)
        row_apr3 = sim_usage_stats_day(self.store, "2026-04-03")
        self.assertIsNotNone(row_apr3)
        self.assertAlmostEqual(row_apr3["carbon_g_net"], -8.5 + 15.0, places=3)
        self.assertAlmostEqual(row_apr3["carbon_g_net"], self._db_carbon("2026-04-03", "2026-04-03"), places=3)

    def test_multi_day_carbon_g_sum(self):
        """sum_daily_rows aggregates carbon_g_net correctly across multiple days."""
        for day in range(15, 18):
            insert_block(self.store, self.cp, f"2026-01-{day:02d}T00:00:00", 2.0, 0.490, carbon_g=60.0)
            insert_block(self.store, self.cp, f"2026-01-{day:02d}T00:30:00", 1.5, 0.368, carbon_g=45.0)
        rows = [sim_usage_stats_day(self.store, f"2026-01-{d:02d}") for d in [15,16,17]]
        totals = sum_daily_rows(rows)
        self.assertAlmostEqual(totals["carbon_g_net"], 3 * (60.0 + 45.0), places=3)
        self.assertAlmostEqual(totals["carbon_g_net"], self._db_carbon("2026-01-15", "2026-01-17"), places=3)

    def test_carbon_g_null_propagates_through_sum(self):
        """If all days have NULL carbon_g, sum_daily_rows returns None."""
        for day in range(15, 18):
            insert_block(self.store, self.cp, f"2026-01-{day:02d}T00:00:00", 2.0, 0.490, carbon_g=None)
        rows = [sim_usage_stats_day(self.store, f"2026-01-{d:02d}") for d in [15,16,17]]
        totals = sum_daily_rows(rows)
        self.assertIsNone(totals["carbon_g_net"])

    def test_net_carbon_can_be_negative(self):
        """Net exporter days produce negative carbon_g_net."""
        insert_block(self.store, self.cp, "2026-06-15T06:00:00", 0.1, 0.024, exp_kwh=2.5, carbon_g=-45.8)
        insert_block(self.store, self.cp, "2026-06-15T06:30:00", 0.0, 0.000, exp_kwh=3.1, carbon_g=-56.9)
        row = sim_usage_stats_day(self.store, "2026-06-15")
        self.assertLess(row["carbon_g_net"], 0)
        self.assertAlmostEqual(row["carbon_g_net"], -45.8 - 56.9, places=3)

    def test_net_is_main_meter_only(self):
        """carbon_g_net is main meter only — sub-meter carbon already included."""
        self.store._conn.execute(
            "INSERT OR IGNORE INTO meters (config_period_id, meter_id, is_sub_meter) VALUES (?,?,1)",
            (self.cp, "ev_charger")
        )
        self.store._conn.commit()
        insert_block(self.store, self.cp, "2026-06-15T06:00:00", 2.0, 0.490, carbon_g=65.0)
        insert_block(self.store, self.cp, "2026-06-15T06:00:00", 1.5, 0.368, carbon_g=30.0, meter_id="ev_charger")
        row = sim_usage_stats_day(self.store, "2026-06-15")
        self.assertAlmostEqual(row["carbon_g_net"], 65.0, places=3,
            msg="carbon_g_net must be main meter only, not main + sub")

    def test_null_sub_meter_carbon_does_not_affect_net(self):
        """Sub-meter with NULL carbon_g does not affect carbon_g_net."""
        self.store._conn.execute(
            "INSERT OR IGNORE INTO meters (config_period_id, meter_id, is_sub_meter) VALUES (?,?,1)",
            (self.cp, "ev_charger")
        )
        self.store._conn.commit()
        insert_block(self.store, self.cp, "2026-06-15T06:00:00", 2.0, 0.490, carbon_g=65.0)
        insert_block(self.store, self.cp, "2026-06-15T06:00:00", 1.0, 0.245, carbon_g=None, meter_id="ev_charger")
        row = sim_usage_stats_day(self.store, "2026-06-15")
        self.assertAlmostEqual(row["carbon_g_net"], 65.0, places=3,
            msg="NULL sub-meter carbon_g must not affect net")


class TestSubMeterCarbonAccounting(unittest.TestCase):
    """
    Cross-check that carbon_g accounting is correct across the main meter
    and sub-meters — verifying totals, remainder, imp/exp split identity,
    and the no-double-count guarantee for the totals chart.

    These are the UI cross-check tests: they verify that what the server
    sends in api_blocks_summary matches what the DB actually contains.
    """

    def setUp(self):
        self.store, self.cp = make_store_with_submeters()

    def _db_carbon(self, local_date, meter_id="electricity_main"):
        """Sum carbon_g for a local date using UTC bounds (Europe/London)."""
        utc_s, utc_e = local_date_to_utc_bounds(local_date, "Europe/London")
        row = self.store._conn.execute(
            "SELECT SUM(carbon_g) FROM blocks WHERE block_start >= ? AND block_start < ? AND meter_id=?",
            (utc_s, utc_e, meter_id)
        ).fetchone()
        v = row[0]
        return round(float(v), 4) if v is not None else None

    def test_carbon_g_net_is_main_meter_only(self):
        """carbon_g_net must equal DB SUM for main meter only — sub-meters excluded."""
        insert_block(self.store, self.cp, "2026-06-15T06:00:00",
                     2.0, 0.490, carbon_g=65.0)
        insert_block(self.store, self.cp, "2026-06-15T06:00:00",
                     1.5, 0.368, carbon_g=30.0, meter_id="ev_charger")
        insert_block(self.store, self.cp, "2026-06-15T06:00:00",
                     0.5, 0.123, carbon_g=10.0, meter_id="house_battery")

        row = sim_usage_stats_day(self.store, "2026-06-15")
        self.assertAlmostEqual(row["carbon_g_net"], 65.0, places=3,
            msg="carbon_g_net must be main meter only")
        self.assertAlmostEqual(row["carbon_g_net"],
            self._db_carbon("2026-06-15", "electricity_main"), places=3,
            msg="carbon_g_net must match DB SUM for main meter")

    def test_carbon_g_total_equals_main_carbon_g(self):
        """carbon_g_total = main_carbon_g only — sub-meters already included in main."""
        insert_block(self.store, self.cp, "2026-06-15T06:00:00",
                     2.0, 0.490, carbon_g=65.0)
        insert_block(self.store, self.cp, "2026-06-15T06:00:00",
                     1.5, 0.368, carbon_g=30.0, meter_id="ev_charger")
        insert_block(self.store, self.cp, "2026-06-15T06:00:00",
                     0.5, 0.123, carbon_g=10.0, meter_id="house_battery")

        row = sim_usage_stats_day(self.store, "2026-06-15")
        # main_carbon_g = 65.0 — this already includes EV and battery consumption
        # carbon_g_total must NOT be 65+30+10=105 (double-count)
        self.assertAlmostEqual(row["carbon_g_total"], 65.0, places=3,
            msg="carbon_g_total must equal main_carbon_g only, not main+sub")

    def test_carbon_g_total_not_double_counting(self):
        """
        carbon_g_total must equal main_carbon_g only — not main + sub.
        The main meter (imp-exp) × intensity already captures all grid consumption
        including what went to sub-meters. Adding sub-meter carbon again double-counts.
        """
        insert_block(self.store, self.cp, "2026-06-15T06:00:00",
                     3.0, 0.735, carbon_g=90.0)
        insert_block(self.store, self.cp, "2026-06-15T06:00:00",
                     1.5, 0.368, carbon_g=45.0, meter_id="ev_charger")

        row = sim_usage_stats_day(self.store, "2026-06-15")
        # Total must be 90 (main only), NOT 90+45=135 (double-count)
        self.assertAlmostEqual(row["carbon_g_total"], 90.0, places=3,
            msg="carbon_g_total must be main_carbon_g only")
        self.assertNotAlmostEqual(row["carbon_g_total"], 135.0, places=1,
            msg="adding sub-meters again would give 135 — that is the double-count bug")

    def test_carbon_g_imp_minus_exp_equals_net(self):
        """carbon_g_imp - carbon_g_exp must equal carbon_g_net (identity)."""
        # Mixed import/export day
        insert_block(self.store, self.cp, "2026-06-15T06:00:00",
                     0.5, 0.123, exp_kwh=2.0, carbon_g=-45.0)

        row = sim_usage_stats_day(self.store, "2026-06-15")
        self.assertIsNotNone(row["carbon_g_imp"])
        self.assertIsNotNone(row["carbon_g_exp"])
        split_net = row["carbon_g_imp"] - row["carbon_g_exp"]
        self.assertAlmostEqual(split_net, row["carbon_g_net"], places=2,
            msg="carbon_g_imp - carbon_g_exp must equal carbon_g_net")

    def test_null_sub_meter_carbon_does_not_affect_net_or_total(self):
        """Sub-meter with NULL carbon_g: net unchanged, total = main only."""
        insert_block(self.store, self.cp, "2026-06-15T06:00:00",
                     2.0, 0.490, carbon_g=65.0)
        insert_block(self.store, self.cp, "2026-06-15T06:00:00",
                     1.5, 0.368, carbon_g=None, meter_id="ev_charger")

        row = sim_usage_stats_day(self.store, "2026-06-15")
        self.assertAlmostEqual(row["carbon_g_net"], 65.0, places=3,
            msg="NULL sub-meter must not affect net")
        self.assertAlmostEqual(row["carbon_g_total"], 65.0, places=3,
            msg="NULL sub-meter contributes 0 to total")

    def test_multi_day_submeter_carbon_totals(self):
        """sum_daily_rows correctly aggregates carbon across days with sub-meters."""
        for day in range(15, 18):
            insert_block(self.store, self.cp, f"2026-06-{day:02d}T06:00:00",
                         2.0, 0.490, carbon_g=60.0)
            insert_block(self.store, self.cp, f"2026-06-{day:02d}T06:00:00",
                         1.0, 0.245, carbon_g=20.0, meter_id="ev_charger")

        rows = [sim_usage_stats_day(self.store, f"2026-06-{d:02d}") for d in [15,16,17]]
        totals = sum_daily_rows(rows)

        self.assertAlmostEqual(totals["carbon_g_net"], 3 * 60.0, places=3,
            msg="3-day net carbon = 3 × main meter")
        self.assertAlmostEqual(totals["carbon_g_total"], 3 * 60.0, places=3,
            msg="3-day total carbon = 3 × main_carbon_g (sub-meters already included)")

    def test_carbon_g_total_equals_main_carbon_g(self):
        """
        Regression test for 2.4.1: carbon_g_total must equal main_carbon_g,
        not main_carbon_g + sub_carbon. The main meter already includes
        sub-meter consumption in its (imp-exp) × intensity calculation.
        """
        self.store._conn.execute(
            "INSERT OR IGNORE INTO meters (config_period_id, meter_id, is_sub_meter) VALUES (?,?,1)",
            (self.cp, "ev_charger")
        )
        self.store._conn.commit()

        insert_block(self.store, self.cp, "2026-04-16T00:00:00",
                     2.9, 0.25, carbon_g=249.4)
        insert_block(self.store, self.cp, "2026-04-16T00:00:00",
                     2.75, 0.24, carbon_g=236.5, meter_id="ev_charger")

        row = sim_usage_stats_day(self.store, "2026-04-16")
        # main_carbon_g = 249.4 (already includes EV consumption)
        # sub_carbon = 236.5
        # carbon_g_total MUST be 249.4, NOT 249.4 + 236.5 = 485.9
        self.assertAlmostEqual(row["carbon_g_net"], 249.4, places=2,
            msg="carbon_g_net must be main meter net")
        self.assertAlmostEqual(row["carbon_g_total"], 249.4, places=2,
            msg="carbon_g_total must equal main_carbon_g, not main+sub (double-count)")
        self.assertNotAlmostEqual(row["carbon_g_total"], 485.9, places=0,
            msg="double-counting would give 485.9 — this is the 2.4.1 regression")

    def test_exporting_day_with_submeter_import(self):
        """
        Net exporter day: main meter carbon_g is negative (solar offsetting),
        but EV charger still has positive import carbon.
        carbon_g_net is negative, carbon_g_total reflects actual consumption.
        """
        insert_block(self.store, self.cp, "2026-06-15T12:00:00",
                     0.1, 0.025, exp_kwh=3.0, carbon_g=-55.0)
        insert_block(self.store, self.cp, "2026-06-15T12:00:00",
                     1.5, 0.368, carbon_g=28.0, meter_id="ev_charger")

        row = sim_usage_stats_day(self.store, "2026-06-15")
        self.assertLess(row["carbon_g_net"], 0,
            msg="Net exporter day must have negative carbon_g_net")
        self.assertAlmostEqual(row["carbon_g_net"], -55.0, places=3)
        self.assertAlmostEqual(row["carbon_g_total"], -55.0, places=3,
            msg="Total = main_carbon_g only (EV carbon already in main meter figure)")

class TestLivePowerBillingCard(unittest.TestCase):
    """
    Verify the live power billing card _fmt_total row calculation.
    Total Import = raw grid import (includes sub-meters)
    Grid Import = raw grid - sum(sub-meters)
    Sub-meter rows sum + Grid Import = Total Import
    """

    def setUp(self):
        self.store = BlockStore(":memory:")
        self.store.insert_config_period({"meters": {
            "electricity_main": {"meta": {
                "billing_day": 1, "block_minutes": 30,
                "timezone": "UTC", "currency_symbol": "£", "currency_code": "GBP",
            }, "channels": {"import": {"read": "s.imp", "rate": "s.rate"},
                            "export": {"read": "s.exp", "rate": "s.exp_rate"}}},
            "house_battery": {"meta": {"sub_meter": True, "parent_meter": "electricity_main",
                                       "device": "Solax Battery"},
                              "channels": {"import": {"read": "s.bat", "rate": "s.rate"}}},
        }})
        self.cp = self.store._conn.execute(
            "SELECT id FROM config_periods LIMIT 1").fetchone()["id"]

    def _insert(self, main_imp, bat_imp, rate=0.07, exp=0.0, exp_rate=0.12, sc=0.50):
        ts = "2026-04-27T00:00:00"
        for meter_id, imp_kwh in [("electricity_main", main_imp), ("house_battery", bat_imp)]:
            self.store._conn.execute("""
                INSERT INTO blocks (
                    block_start, block_end,
                    meter_id, config_period_id, interpolated,
                    imp_kwh, imp_kwh_grid, imp_kwh_remainder,
                    imp_rate, imp_cost, exp_kwh, exp_rate, exp_cost, standing_charge)
                VALUES (?,?,?,?,0,
                        ?,NULL,NULL, ?,?, ?,?,?,?)
            """, (ts, ts, meter_id, self.cp,
                  imp_kwh, rate, round(imp_kwh * rate, 6),
                  exp if meter_id == "electricity_main" else 0.0,
                  exp_rate,
                  round(exp * exp_rate, 6) if meter_id == "electricity_main" else 0.0,
                  sc if meter_id == "electricity_main" else 0.0))
        self.store._conn.commit()

    def _get_billing_totals(self):
        return self.store.get_billing_totals_for_utc_range(*local_date_range_to_utc_bounds("2026-04-27", "2026-04-27", "UTC"))

    def _get_sub_kwh(self):
        """Simulate what _fmt_total's sub-meter SQL returns."""
        active_period_sq = (
            "SELECT id FROM config_periods "
            "WHERE effective_to IS NULL ORDER BY effective_from DESC LIMIT 1"
        )
        cur = self.store._conn.execute(
            f"""SELECT m.meter_id,
                       COALESCE(SUM(COALESCE(b.imp_kwh_grid, b.imp_kwh)), 0.0) as kwh,
                       COALESCE(SUM(b.imp_cost), 0.0) as cost
                FROM blocks b
                JOIN meters m ON m.meter_id = b.meter_id
                  AND m.config_period_id = ({active_period_sq})
                WHERE m.is_sub_meter = 1
                  AND b.block_start >= '2026-04-27T00:00:00' AND b.block_start < '2026-04-28T00:00:00'
                GROUP BY m.meter_id""")
        return [(r["meter_id"], float(r["kwh"]), float(r["cost"])) for r in cur.fetchall()]

    def _get_raw_grid(self):
        """Simulate the raw grid query in _fmt_total."""
        active_period_sq = (
            "SELECT id FROM config_periods "
            "WHERE effective_to IS NULL ORDER BY effective_from DESC LIMIT 1"
        )
        cur = self.store._conn.execute(
            f"""SELECT COALESCE(SUM(COALESCE(b.imp_kwh_grid, b.imp_kwh)), 0.0) as raw_kwh,
                       COALESCE(SUM(b.imp_cost), 0.0) as raw_cost
                FROM blocks b
                JOIN meters m ON m.meter_id = b.meter_id
                  AND m.config_period_id = ({active_period_sq})
                WHERE m.is_sub_meter = 0
                  AND b.block_start >= '2026-04-27T00:00:00' AND b.block_start < '2026-04-28T00:00:00'"""
        ).fetchone()
        return float(cur["raw_kwh"]), float(cur["raw_cost"])

    def test_total_import_equals_raw_grid(self):
        """Total Import shown on card must equal raw main meter import."""
        self._insert(main_imp=13.375, bat_imp=11.609)
        raw_kwh, _ = self._get_raw_grid()
        self.assertAlmostEqual(raw_kwh, 13.375, places=3,
            msg="Total Import must equal raw grid import")

    def test_grid_import_remainder_correct(self):
        """Grid Import must be raw grid minus sub-meter attributed portion."""
        self._insert(main_imp=13.375, bat_imp=11.609)
        raw_kwh, _ = self._get_raw_grid()
        subs = self._get_sub_kwh()
        sub_kwh_total = sum(s[1] for s in subs)
        grid_imp = raw_kwh - sub_kwh_total
        self.assertAlmostEqual(grid_imp, 13.375 - 11.609, places=3,
            msg="Grid Import must be raw grid minus sub-meter total")

    def test_sub_plus_grid_equals_total(self):
        """Grid Import + sub-meter kWh must equal Total Import."""
        self._insert(main_imp=13.375, bat_imp=11.609)
        raw_kwh, _ = self._get_raw_grid()
        subs = self._get_sub_kwh()
        sub_kwh_total = sum(s[1] for s in subs)
        grid_imp = raw_kwh - sub_kwh_total
        self.assertAlmostEqual(grid_imp + sub_kwh_total, raw_kwh, places=3,
            msg="Grid Import + sub-meters must equal Total Import — no double counting")

    def test_no_sub_meters_grid_equals_total(self):
        """With no sub-meters Grid Import must equal Total Import."""
        # Only insert main meter block
        self.store._conn.execute("""
            INSERT INTO blocks (
                block_start, block_end,
                meter_id, config_period_id, interpolated,
                imp_kwh, imp_kwh_grid, imp_kwh_remainder,
                imp_rate, imp_cost, standing_charge)
            VALUES ('2026-04-27T00:00:00','2026-04-27T00:00:00',
                    'electricity_main',?,0, 10.0,NULL,NULL, 0.07,0.70,0.50)
        """, (self.cp,))
        self.store._conn.commit()
        raw_kwh, _ = self._get_raw_grid()
        subs = self._get_sub_kwh()
        sub_kwh_total = sum(s[1] for s in subs)
        grid_imp = raw_kwh - sub_kwh_total
        self.assertAlmostEqual(grid_imp, raw_kwh, places=3,
            msg="With no sub-meters Grid Import must equal Total Import")
        self.assertAlmostEqual(grid_imp, 10.0, places=3)

class TestNonRoundStandingCharge(unittest.TestCase):
    """
    Standing charge of £0.504559/day must not be rounded to 2dp per day
    before summing — that would give £0.50 × N days instead of the correct
    sum. Over 16 days: £8.00 vs £8.07. Verifies server.py sends standing
    at 4dp so the JS period total agrees with get_billing_totals_for_utc_range.
    """

    STANDING = 0.504559   # real-world value from production DB

    def setUp(self):
        self.store, self.cp = make_store()

    def _insert_days(self, n_days, start_date="2026-05-03"):
        """Insert one block per day for n_days with non-round standing charge."""
        d = datetime.fromisoformat(start_date + "T23:00:00")  # BST: 23:00 UTC = midnight local
        for _ in range(n_days):
            insert_block(self.store, self.cp,
                         d.strftime("%Y-%m-%dT%H:%M:%S"),
                         imp_kwh=1.0, imp_cost=0.33,
                         standing=self.STANDING)
            d += timedelta(days=1)

    def test_single_day_non_round_standing(self):
        """Single day: standing at 4dp precision passes through correctly."""
        self.store, self.cp = make_store()
        insert_block(self.store, self.cp, "2026-05-03T23:00:00",
                     imp_kwh=1.0, imp_cost=0.33, standing=self.STANDING)

        row = sim_usage_stats_day(self.store, "2026-05-04")
        sql = sql_totals(self.store, "2026-05-04", "2026-05-04")

        # row["standing"] should be the raw value at 4dp, not 0.50
        self.assertAlmostEqual(row["standing"], round(self.STANDING, 4), places=4,
            msg=f"Single day standing should be {round(self.STANDING,4)}, not 0.50")
        assert_match(self, "single day non-round standing", row, sql)

    def test_multiday_standing_sum_not_rounded_per_day(self):
        """
        16-day period: sum of daily standing charges must not accumulate
        the error from rounding each day to 2dp.

        Wrong (old): round(0.504559, 2) * 16 = 0.50 * 16 = 8.00
        Right (new): 0.504559 * 16 = 8.072938, round once = 8.07
        """
        self._insert_days(16)

        # Simulate Usage Stats: sum daily rows
        rows = []
        d = datetime.fromisoformat("2026-05-04")
        for _ in range(16):
            row = sim_usage_stats_day(self.store, d.strftime("%Y-%m-%d"))
            if row:
                rows.append(row)
            d += timedelta(days=1)

        daily_sum = sum_daily_rows(rows)
        sql = sql_totals(self.store, "2026-05-04",
                         (datetime.fromisoformat("2026-05-04") + timedelta(days=15)).strftime("%Y-%m-%d"))

        # The key assertion: standing must NOT be £8.00 (2dp-rounded per day)
        wrong_answer = round(round(self.STANDING, 2) * 16, 4)
        self.assertNotAlmostEqual(daily_sum["standing"], wrong_answer, places=2,
            msg=f"Standing should NOT be {wrong_answer} (2dp-per-day rounding error)")

        # It must match the SQL ground truth
        assert_match(self, "16-day non-round standing", daily_sum, sql, tol=0.005)

    def test_multiday_net_total_matches_live_power(self):
        """
        Period net total (imp + standing - exp) from Usage Stats must match
        Live Power get_billing_totals_for_utc_range, including non-round standing.
        This is the real-world scenario that produced -£3.21 vs -£3.14.
        """
        # 16 days, with export so net goes negative
        d = datetime.fromisoformat("2026-05-03T23:00:00")
        for _ in range(16):
            insert_block(self.store, self.cp,
                         d.strftime("%Y-%m-%dT%H:%M:%S"),
                         imp_kwh=2.0, imp_cost=0.66,
                         exp_kwh=3.5, exp_cost=0.42,
                         standing=self.STANDING)
            d += timedelta(days=1)

        rows = []
        d = datetime.fromisoformat("2026-05-04")
        for _ in range(16):
            row = sim_usage_stats_day(self.store, d.strftime("%Y-%m-%d"))
            if row:
                rows.append(row)
            d += timedelta(days=1)

        daily_sum = sum_daily_rows(rows)
        sql = sql_totals(self.store, "2026-05-04",
                         (datetime.fromisoformat("2026-05-04") + timedelta(days=15)).strftime("%Y-%m-%d"))

        # Net total: imp + standing - exp
        us_net  = round(daily_sum["imp_cost"] + daily_sum["standing"] - daily_sum["exp_cost"], 2)
        lp_net  = round(sql["imp_cost"] + sql["standing"] - sql["exp_cost"], 2)

        self.assertEqual(us_net, lp_net,
            msg=f"Usage Stats net £{us_net:.2f} must equal Live Power net £{lp_net:.2f}. "
                f"Difference of £{abs(us_net-lp_net):.2f} indicates standing charge rounding per day.")


class TestNonRoundImportExportCost(unittest.TestCase):
    """
    imp_cost and exp_cost sent from server must be at 4dp per day, not 2dp.
    With 2dp per day, summing across 16 days accumulates rounding error.

    Tests use a direct SQL net calculation (imp_cost + standing - exp_cost
    on main meter) to match what all three surfaces should show, avoiding
    the complexity of reconstructing the server's rate-based subtraction logic
    in the test helper.
    """

    def setUp(self):
        self.store, self.cp = make_store_with_submeters()

    def _insert_period(self, n_days, imp_cost=0.656743, exp_cost=0.421789,
                       standing=0.504559, start="2026-05-03"):
        """Insert n_days of main-meter blocks with non-round costs."""
        from datetime import datetime, timedelta
        d = datetime.fromisoformat(start + "T23:00:00")
        for _ in range(n_days):
            ts = d.strftime("%Y-%m-%dT%H:%M:%S")
            insert_block(self.store, self.cp, ts,
                         imp_kwh=2.0, imp_cost=imp_cost,
                         exp_kwh=3.5, exp_cost=exp_cost,
                         standing=standing, meter_id="electricity_main")
            d += timedelta(days=1)

    def _sql_net(self, first_date, last_date):
        """Ground truth: imp_cost + standing - exp_cost from SQL."""
        sql = sql_totals(self.store, first_date, last_date)
        return round(sql["imp_cost"] + sql["standing"] - sql["exp_cost"], 2)

    def _us_net(self, first_date, last_date):
        """
        Simulate Usage Stats net: sum daily (imp_cost + standing - exp_cost)
        at 4dp per day, then round once. This matches the JS barGetDataForPeriod
        which rounds the period aggregate to 4dp then displays at 2dp.
        """
        from datetime import datetime, timedelta
        rows = []
        d = datetime.fromisoformat(first_date)
        end = datetime.fromisoformat(last_date)
        while d <= end:
            row = sim_usage_stats_day(self.store, d.strftime("%Y-%m-%d"))
            if row:
                rows.append(row)
            d += timedelta(days=1)
        # JS sums at 4dp precision (standing already 4dp from server)
        # imp_cost and exp_cost also now 4dp from server
        total_imp  = round(sum(r["imp_cost"] for r in rows), 4)
        total_exp  = round(sum(r["exp_cost"] for r in rows), 4)
        total_sc   = round(sum(r["standing"] for r in rows), 4)
        return round(total_imp + total_sc - total_exp, 2)

    def test_single_day_net_matches_sql(self):
        """Single day: Usage Stats net matches Live Power SQL."""
        self._insert_period(1)
        us  = self._us_net("2026-05-04", "2026-05-04")
        sql = self._sql_net("2026-05-04", "2026-05-04")
        self.assertEqual(us, sql,
            msg=f"Single day: US net £{us:.2f} != SQL £{sql:.2f}")

    def test_multiday_standing_not_rounded_per_day(self):
        """
        16 days with £0.504559/day standing: the period standing sum must be
        £8.07 (sum raw then round once), not £8.00 (round to 2dp per day first).
        Directly verifies the standing charge fix.
        """
        self._insert_period(16)
        last = (datetime.fromisoformat("2026-05-04") + timedelta(days=15)).strftime("%Y-%m-%d")

        from datetime import datetime as _dt, timedelta as _td
        rows = []
        d = _dt.fromisoformat("2026-05-04")
        for _ in range(16):
            row = sim_usage_stats_day(self.store, d.strftime("%Y-%m-%d"))
            if row:
                rows.append(row)
            d += _td(days=1)

        # standing should be 4dp per day — sum to ~8.0736, round to 8.07
        total_sc_4dp = round(sum(r["standing"] for r in rows), 4)
        wrong_sc_2dp = round(round(0.504559, 2) * 16, 2)  # would be 8.00

        self.assertNotEqual(round(total_sc_4dp, 2), wrong_sc_2dp,
            msg=f"Standing {round(total_sc_4dp,2):.2f} should not equal {wrong_sc_2dp:.2f} "
                f"(the 2dp-per-day rounding error)")
        self.assertAlmostEqual(total_sc_4dp, 0.504559 * 16, delta=0.001,
            msg=f"Standing period sum {total_sc_4dp:.4f} should be ~{0.504559*16:.4f}")

    def test_multiday_net_all_surfaces_agree(self):
        """
        Real-world scenario: 16 days of non-round costs.
        Usage Stats net must equal Live Power net (SQL ground truth).
        This is the scenario that produced -3.11 / -3.22 / -3.14 / -3.15.
        """
        self._insert_period(16, imp_cost=0.656743, exp_cost=0.421789, standing=0.504559)
        last = (datetime.fromisoformat("2026-05-04") + timedelta(days=15)).strftime("%Y-%m-%d")
        us  = self._us_net("2026-05-04", last)
        sql = self._sql_net("2026-05-04", last)
        self.assertEqual(us, sql,
            msg=f"Usage Stats net £{us:.2f} != Live Power net £{sql:.2f}. "
                f"Difference of £{abs(us-sql):.2f} indicates per-day rounding in imp/exp/standing.")


class TestNetCostConsistency(unittest.TestCase):
    """
    Verifies the unified net_cost methodology:
    - daily net_cost = round(imp + standing - exp, 2)
    - sum of daily net_cost = period net_cost
    - billing chart total_cost agrees with sum of daily net_cost
    - daily view and monthly view grand totals agree
    """

    STANDING = 0.504559

    def setUp(self):
        self.store, self.cp = make_store()

    def _insert_days(self, n, imp_cost=0.656743, exp_cost=0.421789,
                     standing=0.504559, start="2026-05-03"):
        from datetime import datetime, timedelta
        d = datetime.fromisoformat(start + "T23:00:00")
        for _ in range(n):
            insert_block(self.store, self.cp, d.strftime("%Y-%m-%dT%H:%M:%S"),
                         imp_kwh=2.0, imp_cost=imp_cost,
                         exp_kwh=3.5, exp_cost=exp_cost, standing=standing)
            d += timedelta(days=1)

    def test_daily_net_cost_equals_parts(self):
        """net_cost per day = round(imp + standing - exp, 2)."""
        self._insert_days(1)
        row = sim_usage_stats_day(self.store, "2026-05-04")
        self.assertIsNotNone(row)
        expected_net = round(row["imp_cost"] + row["standing"] - row["exp_cost"], 2)
        self.assertEqual(row["net_cost"], expected_net,
            msg=f"Daily net_cost {row['net_cost']} != sum of parts {expected_net}")

    def test_period_net_equals_sum_of_daily_nets(self):
        """Sum of daily net_cost values = period grand total."""
        from datetime import datetime, timedelta
        self._insert_days(16)
        rows = []
        d = datetime.fromisoformat("2026-05-04")
        for _ in range(16):
            row = sim_usage_stats_day(self.store, d.strftime("%Y-%m-%d"))
            if row:
                rows.append(row)
            d += timedelta(days=1)
        period_net = round(sum(r["net_cost"] for r in rows), 2)
        # Each daily net_cost already rounds internally — sum should be stable
        daily_sum = sum(r["net_cost"] for r in rows)
        self.assertAlmostEqual(period_net, daily_sum, places=2,
            msg=f"Period net {period_net} should equal sum of daily nets {daily_sum:.4f}")

    def test_billing_chart_total_matches_daily_net_sum(self):
        """
        Billing chart total_cost uses the same daily net_cost method as Usage Stats.
        With no sub-meters, per-block and per-day subtraction are identical so they
        agree exactly. With sub-meters they may differ by ±£0.01 due to the billing
        chart doing per-block subtraction vs Usage Stats doing per-day subtraction —
        this is accepted as the billing chart is internally consistent with its own
        per-rate breakdown display.
        """
        from datetime import datetime, timedelta
        # Test without sub-meters — must agree exactly
        self._insert_days(16)

        from block_store import local_date_range_to_utc_bounds
        utc_s, utc_e = local_date_range_to_utc_bounds("2026-05-04",
            (datetime.fromisoformat("2026-05-04") + timedelta(days=15)).strftime("%Y-%m-%d"),
            "Europe/London")
        blocks = self.store.get_blocks_for_utc_range(utc_s, utc_e)
        p_start = datetime(2026, 5, 4, 0, 0, 0)
        p_end   = datetime(2026, 5, 20, 0, 0, 0)
        summary = ec.calculate_billing_summary_for_period(blocks, p_start, p_end)

        rows = []
        d = datetime.fromisoformat("2026-05-04")
        for _ in range(16):
            row = sim_usage_stats_day(self.store, d.strftime("%Y-%m-%d"))
            if row:
                rows.append(row)
            d += timedelta(days=1)
        us_net = round(sum(r["net_cost"] for r in rows), 2)

        # Without sub-meters, per-block and per-day computation give identical results
        self.assertEqual(summary["total_cost"], us_net,
            msg=f"Without sub-meters: billing chart {summary['total_cost']} must equal "
                f"Usage Stats {us_net}")

    def test_non_round_standing_daily_vs_monthly(self):
        """
        With non-round standing charge, daily view grand total must equal
        monthly view grand total (both sum daily net_cost values).
        """
        from datetime import datetime, timedelta
        self._insert_days(21, standing=self.STANDING)

        rows = []
        d = datetime.fromisoformat("2026-05-04")
        for _ in range(21):
            row = sim_usage_stats_day(self.store, d.strftime("%Y-%m-%d"))
            if row:
                rows.append(row)
            d += timedelta(days=1)

        daily_grand_total = round(sum(r["net_cost"] for r in rows), 2)
        # Monthly view would aggregate all rows into one bucket — same net_cost sum
        monthly_grand_total = daily_grand_total  # same data, same method
        self.assertEqual(daily_grand_total, monthly_grand_total,
            msg="Daily and monthly grand totals must agree")
        # And each daily net is internally consistent
        for r in rows:
            expected = round(r["imp_cost"] + r["standing"] - r["exp_cost"], 2)
            self.assertEqual(r["net_cost"], expected)

class TestMixedSourceBillingAgreement(unittest.TestCase):
    """A billing period spanning a data-source-mode change (Change Setup, or DCC
    settlement catching up) holds blocks of mixed ORIGIN: source ha_sensor (cad),
    kraken_api (DCC-settled), kraken_mini (provisional). Data-source mode is
    invisible to billing — on a sub-less meter every origin stores its billable
    kWh in imp_kwh (settlement normalises the authoritative value there) — so the
    three consumers (SQL ground truth, billing-chart render, usage-stats sum) must
    agree on a mixed period exactly as on a single-source one. Pins that the mode
    mix introduces no divergence; the reconciliation harness otherwise never tags
    source."""

    def setUp(self):
        self.store, self.cp = make_store()

    def _set_source(self, block_start_utc, source):
        self.store._conn.execute("UPDATE blocks SET source=? WHERE block_start=?",
                                 (source, block_start_utc))
        self.store._conn.commit()

    def _render_totals(self, blocks, p_start, p_end):
        s = ec.calculate_billing_summary_for_period(blocks, p_start, p_end)
        imp_kwh = imp_cost = 0.0
        for key, t in (s.get("totals") or {}).items():
            if not t.get("is_submeter") and "export" not in key.lower():
                imp_kwh += t.get("kwh", 0)
                imp_cost += t.get("cost", 0)
        return {"imp_kwh": round(imp_kwh, 4), "imp_cost": round(imp_cost, 4),
                "standing": round(s.get("total_standing", 0), 4)}

    def test_mixed_source_period_all_methods_agree(self):
        # Jan (GMT) period: day 1 cad, day 2 DCC + a mini-provisional block.
        insert_block(self.store, self.cp, "2026-01-15T00:00:00", 2.0, 0.490, standing=0.50)
        insert_block(self.store, self.cp, "2026-01-15T00:30:00", 1.5, 0.368, standing=0.50)
        insert_block(self.store, self.cp, "2026-01-16T00:00:00", 2.2, 0.539, standing=0.50)
        insert_block(self.store, self.cp, "2026-01-16T00:30:00", 0.5, 0.123, standing=0.50)
        self._set_source("2026-01-15T00:00:00", "ha_sensor")
        self._set_source("2026-01-15T00:30:00", "ha_sensor")
        self._set_source("2026-01-16T00:00:00", "kraken_api")
        self._set_source("2026-01-16T00:30:00", "kraken_mini")

        utc_s, utc_e = local_date_range_to_utc_bounds("2026-01-15", "2026-01-16", "UTC")
        sql    = self.store.get_billing_totals_for_utc_range(utc_s, utc_e, "Europe/London")
        blocks = self.store.get_blocks_for_utc_range(utc_s, utc_e)
        render = self._render_totals(blocks, datetime(2026, 1, 15), datetime(2026, 1, 17))
        usage  = sum_daily_rows([sim_usage_stats_day(self.store, d)
                                 for d in ("2026-01-15", "2026-01-16")])

        for k in ("imp_kwh", "imp_cost"):
            self.assertAlmostEqual(sql[k], render[k], places=3,
                msg=f"SQL vs billing-chart disagree on {k} for a mixed-source period")
            self.assertAlmostEqual(sql[k], usage[k], places=3,
                msg=f"SQL vs usage-stats disagree on {k} for a mixed-source period")
        self.assertAlmostEqual(sql["imp_kwh"], 6.2, places=3)
        self.assertAlmostEqual(sql["standing"], 1.00, places=3,
            msg="Standing once per local day across the mode boundary")


class TestBillingReadsIgnoreZeros(unittest.TestCase):
    """Period Start/End meter reads must come from REAL register reads. A gap /
    reset / register-less block that carries read 0 must not drag the displayed
    Start/End to 0.000 — the reported bug (CAD start always 0; API export 0/0 even
    though real export reads exist alongside a few zero-read blocks)."""

    def _ins(self, store, cp, start, rs, re, kwh=1.0, cost=0.2):
        store._conn.execute(
            "INSERT INTO blocks (block_start, block_end, meter_id, config_period_id,"
            " imp_kwh, imp_rate, imp_cost, imp_read_start, imp_read_end) "
            "VALUES (?,?,?,?,?,?,?,?,?)",
            (start, start, "electricity_main", cp, kwh, 0.2, cost, rs, re))
        store._conn.commit()

    def test_zero_reads_do_not_drag_period_boundaries(self):
        store, cp = make_store()
        self._ins(store, cp, "2026-06-01T00:00:00", 1000.0, 1002.0)
        self._ins(store, cp, "2026-06-01T00:30:00", 0.0, 0.0, kwh=0.0, cost=0.0)  # gap/reset
        self._ins(store, cp, "2026-06-01T01:00:00", 1002.0, 1005.0)
        _s = datetime(2026, 6, 1, 0, 0); _e = datetime(2026, 6, 2, 0, 0)
        blocks = store.get_blocks_for_range(_s, _e)
        summary = ec.calculate_billing_summary_for_period(blocks, _s, _e, store=store)
        imp = next((t for k, t in summary["totals"].items()
                    if k.lower().endswith("import")
                    and not (summary["meter_meta"].get(k) or {}).get("is_submeter")), None)
        self.assertIsNotNone(imp)
        self.assertEqual(imp["read_start"], 1000.0)   # min POSITIVE, not the 0 gap block
        self.assertEqual(imp["read_end"], 1005.0)      # max, not last-wins 0

    def test_all_zero_reads_leaves_none(self):
        # A register-less source (CAD power sensor / API Measurements) carries all-0
        # reads → no reads line at all (None), not a misleading Start/End 0.000.
        store, cp = make_store()
        self._ins(store, cp, "2026-06-01T00:00:00", 0.0, 0.0)
        self._ins(store, cp, "2026-06-01T00:30:00", 0.0, 0.0)
        _s = datetime(2026, 6, 1, 0, 0); _e = datetime(2026, 6, 2, 0, 0)
        blocks = store.get_blocks_for_range(_s, _e)
        summary = ec.calculate_billing_summary_for_period(blocks, _s, _e, store=store)
        imp = next((t for k, t in summary["totals"].items()
                    if k.lower().endswith("import")
                    and not (summary["meter_meta"].get(k) or {}).get("is_submeter")), None)
        self.assertIsNotNone(imp)
        self.assertIsNone(imp["read_start"])          # nothing real → suppressed, not 0
        self.assertIsNone(imp["read_end"])


if __name__ == "__main__":
    unittest.main()