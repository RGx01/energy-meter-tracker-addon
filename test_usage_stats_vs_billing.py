"""
Test suite: verifies that Usage Stats (api_blocks_summary logic) produces
totals that match get_billing_totals_for_local_date_range (SQL ground truth)
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
from block_store import BlockStore
import energy_charts as ec


# ── Helpers ───────────────────────────────────────────────────────────────────

TZ = ZoneInfo("Europe/London")

def make_store():
    store = BlockStore(":memory:")
    store._conn.execute("""
        INSERT INTO config_periods
        (effective_from, effective_to, billing_day, block_minutes, timezone,
         currency_symbol, currency_code, site_name, change_reason, full_config_json)
        VALUES ('2026-01-01T00:00:00', NULL, 15, 30, 'Europe/London',
                '£', 'GBP', 'Home', NULL, '{}')
    """)
    store._conn.commit()
    return store, store._conn.execute(
        "SELECT id FROM config_periods LIMIT 1").fetchone()["id"]


def insert_block(store, cp_id, block_start_utc, imp_kwh, imp_cost,
                 exp_kwh=0.0, exp_cost=0.0, standing=0.50):
    """Insert one block row; local_date derived from UTC start."""
    local_date = (datetime.fromisoformat(block_start_utc)
                  .replace(tzinfo=ZoneInfo("UTC"))
                  .astimezone(TZ).date().isoformat())
    store._conn.execute("""
        INSERT INTO blocks (
            block_start, block_end, local_date, local_year, local_month, local_day,
            meter_id, config_period_id, interpolated,
            imp_kwh, imp_kwh_grid, imp_kwh_remainder,
            imp_rate, imp_cost, imp_cost_remainder,
            imp_read_start, imp_read_end,
            exp_kwh, exp_rate, exp_cost,
            exp_read_start, exp_read_end, standing_charge)
        VALUES (?,?,?,?,?,?,?,?,?,?,NULL,NULL,NULL,?,NULL,NULL,NULL,?,NULL,?,NULL,NULL,?)
    """, (block_start_utc,
          (datetime.fromisoformat(block_start_utc)+timedelta(minutes=30)).isoformat(),
          local_date,
          int(local_date[:4]), int(local_date[5:7]), int(local_date[8:10]),
          "electricity_main", cp_id, 0,
          imp_kwh, imp_cost, exp_kwh, exp_cost, standing))
    store._conn.commit()


def sim_usage_stats_day(store, local_date_str):
    """
    Simulate what api_blocks_summary does for one local day:
    - fetch blocks by local_date
    - get standing from first block
    - sum imp/exp kwh and cost from blocks directly
    Returns dict matching the row structure.
    """
    blocks = store.get_blocks_for_local_date_range(local_date_str, local_date_str)
    if not blocks:
        return None

    # standing_charge is on the meter block, not the top-level block dict
    _first_meter = next(iter((blocks[0].get("meters") or {}).values()), {})
    standing = float(_first_meter.get("standing_charge") or 0.0)
    imp_kwh = imp_cost = exp_kwh = exp_cost = 0.0
    for b in blocks:
        for mid, md in (b.get("meters") or {}).items():
            ch_imp = (md.get("channels") or {}).get("import") or {}
            ch_exp = (md.get("channels") or {}).get("export") or {}
            imp_kwh  += float(ch_imp.get("kwh") or 0)
            imp_cost += float(ch_imp.get("cost") or 0)
            exp_kwh  += float(ch_exp.get("kwh") or 0)
            exp_cost += float(ch_exp.get("cost") or 0)

    return {
        "standing": round(standing, 4),
        "imp_kwh":  round(imp_kwh, 4),
        "imp_cost": round(imp_cost, 4),
        "exp_kwh":  round(exp_kwh, 4),
        "exp_cost": round(exp_cost, 4),
    }


def sql_totals(store, first_date, last_date):
    """Ground truth: SQL aggregation via local_date range."""
    return store.get_billing_totals_for_local_date_range(first_date, last_date)


def sum_daily_rows(rows):
    """Sum a list of day rows (as returned by sim_usage_stats_day)."""
    out = {"standing": 0.0, "imp_kwh": 0.0, "imp_cost": 0.0,
           "exp_kwh": 0.0, "exp_cost": 0.0}
    for r in rows:
        if r:
            for k in out:
                out[k] += r[k]
    for k in out:
        out[k] = round(out[k], 4)
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

        blocks = self.store.get_blocks_for_local_date_range("2026-01-15", "2026-01-17")
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

        blocks = self.store.get_blocks_for_local_date_range("2026-04-03", "2026-04-04")
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