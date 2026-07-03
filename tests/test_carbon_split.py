"""Regression guard: the Usage Stats CO2 bar/table must split a period into its
REAL gross import carbon and gross export offset — not the net carbon attributed
to one side by its sign.

Bug it guards (3.0): in charts.html cellVal(), the CO2 'net_imp'/'net_exp'
columns returned carbon_g_net's positive part / |negative part|. So a day that
was a net importer showed ALL its net carbon as "Import CO2" with Export offset
0 (and vice-versa), even though that day really did both. Totals stayed right
(rowTotal = net_imp - net_exp = net), but the split — and the per-day export
offset — were wrong. Fix: use the gross carbon_g_imp / carbon_g_exp the server
already provides (the same values the per-meter table and co2_exp column use).

This extracts the REAL cellVal() from the template and asserts the split.
Skipped where node is unavailable.
"""
import os
import re
import shutil
import subprocess
import tempfile
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
CHARTS = os.path.join(HERE, "web", "templates", "charts.html")


def _extract(name_sig, end_marker):
    html = open(CHARTS, encoding="utf-8").read()
    start = html.index(name_sig)
    end = html.index(end_marker, start)
    return html[start:end].rstrip()


def _extract_funcs():
    # The CO2 import/export split now lives entirely in cellVal()'s net_imp /
    # net_exp branches (the old barCo2ImpExp helper was inlined away). cellVal
    # only needs barMetric + barStandingVal on the CO2 path, both provided by the
    # harness below.
    return _extract("function cellVal(agg, col) {", "// Build HTML")


HARNESS_TMPL = r"""
var barMetric = 'co2';
var barCurrency = '';
function barStandingVal(){ return 0; }
__FUNCS__

// Contract: the server sends the MAIN meter's carbon_g_imp as the GROSS import
// carbon — it already includes the sub-meters that draw through the main. Devices
// carry carbon_g only (no carbon_g_imp). So the CO2 net_imp column sums
// carbon_g_imp across meters and collapses to the main's gross figure.
//
// Net-importer day that also exported, with an EV device drawing through main:
//   main gross import carbon 9.0 (incl. the EV's 4.0), gross export offset 3.0.
//   Total import = 9.0, export = 3.0, net = 6.0. The 3.0-era bug reported
//   import = net = 6.0 and export = 0; use of the gross columns fixes both.
var impDay = { carbon_g_net: 6.0, imp_kwh: 10, exp_kwh: 4,
               meters: { main: { carbon_g_imp: 9.0, carbon_g_exp: 3.0, carbon_g: 5.0 },
                         ev:   { carbon_g: 4.0 } } };
// Net-exporter day: main gross import 3.0 (incl. the EV's 1.0), gross export 3.5.
var expDay = { carbon_g_net: -0.5, imp_kwh: 4, exp_kwh: 9,
               meters: { main: { carbon_g_imp: 3.0, carbon_g_exp: 3.5, carbon_g: 2.0 },
                         ev:   { carbon_g: 1.0 } } };

function near(a,b){ return Math.abs(a-b) < 1e-9; }
var fails = [];
var ni1 = cellVal(impDay, {key:'net_imp'}), ne1 = cellVal(impDay, {key:'net_exp'});
var ni2 = cellVal(expDay, {key:'net_imp'}), ne2 = cellVal(expDay, {key:'net_exp'});
if (!near(ni1, 9.0)) fails.push('import-day net_imp should be total import incl. device 9.0, got '+ni1);
if (!near(ne1, 3.0)) fails.push('import-day net_exp should be gross 3.0 (not 0), got '+ne1);
if (!near(ni1 - ne1, 6.0)) fails.push('import-day total should be 6.0 (total import - export), got '+(ni1-ne1));
if (!near(ni2, 3.0)) fails.push('export-day net_imp should be total import incl. device 3.0 (not 0), got '+ni2);
if (!near(ne2, 3.5)) fails.push('export-day net_exp should be gross 3.5, got '+ne2);
if (!near(ni2 - ne2, -0.5)) fails.push('export-day total should be -0.5, got '+(ni2-ne2));

console.log(fails.length ? 'FAIL: ' + fails.join('; ') : 'PASS');
process.exit(fails.length ? 1 : 0);
"""


@unittest.skipUnless(shutil.which("node"), "node not available")
class TestCarbonImportExportSplit(unittest.TestCase):
    def test_co2_split_uses_gross_not_net_sign(self):
        harness = HARNESS_TMPL.replace("__FUNCS__", _extract_funcs())
        with tempfile.NamedTemporaryFile("w", suffix=".js", delete=False) as fh:
            fh.write(harness)
            path = fh.name
        try:
            res = subprocess.run(["node", path], capture_output=True, text=True, timeout=30)
        finally:
            os.unlink(path)
        self.assertEqual(res.returncode, 0,
                         msg=f"carbon split harness failed:\n{res.stdout}\n{res.stderr}")
        self.assertIn("PASS", res.stdout)


AI_HARNESS = r"""
__BARAGGROWS__

var fails = [];
function near(a,b,t){ return Math.abs(a-b) <= (t||0.05); }

// A near-balanced day: server intensity 136.7, but daily net kWh is tiny (0.77).
// The old client re-derivation abs(net_carbon)/abs(net_kwh) exploded to ~3833.
var barData = [{imp_kwh:39.80, exp_kwh:39.03, carbon_g_net:2951.4, avg_intensity:136.7, meters:{}}];
var r1 = barAggRows(function(){return true;});
if (!near(r1.avg_intensity, 136.7)) fails.push('single near-balanced day should equal server 136.7, got '+r1.avg_intensity);
if (r1.avg_intensity > 500) fails.push('intensity blew up (net-derivation regression): '+r1.avg_intensity);

// Multi-day: throughput-weighted average of server intensities.
// day A: intensity 100, throughput 80 ; day B: intensity 300, throughput 20
// weighted = (100*80 + 300*20)/100 = 140
barData = [
  {imp_kwh:50, exp_kwh:30, carbon_g_net:5000, avg_intensity:100, meters:{}},
  {imp_kwh:15, exp_kwh:5,  carbon_g_net:5000, avg_intensity:300, meters:{}}
];
var r2 = barAggRows(function(){return true;});
if (!near(r2.avg_intensity, 140, 0.2)) fails.push('multi-day throughput-weighted should be 140, got '+r2.avg_intensity);

console.log(fails.length ? 'FAIL: ' + fails.join('; ') : 'PASS');
process.exit(fails.length ? 1 : 0);
"""


@unittest.skipUnless(shutil.which("node"), "node not available")
class TestAvgIntensityNoBlowup(unittest.TestCase):
    def test_avg_intensity_uses_server_value_weighted(self):
        # barData is a global the function reads; declare it via the harness.
        baf = _extract("function barAggRows(filterFn) {", "\nfunction ")
        harness = "var barData = [];\n" + AI_HARNESS.replace("__BARAGGROWS__", baf)
        with tempfile.NamedTemporaryFile("w", suffix=".js", delete=False) as fh:
            fh.write(harness)
            path = fh.name
        try:
            res = subprocess.run(["node", path], capture_output=True, text=True, timeout=30)
        finally:
            os.unlink(path)
        self.assertEqual(res.returncode, 0,
                         msg=f"avg_intensity harness failed:\n{res.stdout}\n{res.stderr}")
        self.assertIn("PASS", res.stdout)


if __name__ == "__main__":
    unittest.main()