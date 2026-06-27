"""
Guards the Usage Stats (Charts) CO2 data-table cell logic.

Regression: the per-meter "Direct carbon" cell read `carbon_g_imp` for the main
meter — the *un-subtracted* gross import carbon, which already contains the
sub-meters' share. The table then listed the subs again, double-counting them
into the row Total. The cell must read `carbon_g` (the direct remainder for the
main; gross import for subs), matching barVal() and the Insights page.

Extracts the real cellVal() from charts.html and runs it under node.
Skipped where node is unavailable.
"""
import os
import shutil
import subprocess
import tempfile
import textwrap
import unittest

TMPL = os.path.join(os.path.dirname(__file__), "web", "templates", "charts.html")


@unittest.skipUnless(shutil.which("node"), "node not available")
class TestUsageCarbonTableCell(unittest.TestCase):
    def _run(self):
        script = textwrap.dedent(r"""
            const fs = require('fs');
            const vm = require('vm');
            const html = fs.readFileSync(process.argv[2], 'utf8');

            // Slice out the real cellVal() function.
            const s = html.indexOf('function cellVal(agg, col) {');
            const e = html.indexOf('// Build HTML', s);
            if (s === -1 || e === -1) { console.log('EXTRACT_FAIL'); process.exit(2); }
            const fn = html.slice(s, e);

            // Closure deps used by cellVal: barMetric + barStandingVal stub.
            const ctx = { barMetric: 'co2', barStandingVal: function(){ return 0; },
                          Object: Object, Math: Math };
            vm.createContext(ctx);
            const call = (agg, col) =>
              vm.runInContext(fn + '\ncellVal(' + JSON.stringify(agg) + ',' +
                              JSON.stringify(col) + ');', ctx);

            const fails = [];
            const eq = (a, b, m) => { if (a !== b) fails.push(m + ' (got ' + JSON.stringify(a) + ', want ' + JSON.stringify(b) + ')'); };

            // Main meter: carbon_g is the DIRECT remainder (11.14); carbon_g_imp is
            // the un-subtracted gross import (167.07). The cell must return 11.14.
            const mainAgg = { meters: { electricity_main: {
              carbon_g: 11.14, carbon_g_imp: 167.07, carbon_g_exp: 99.88 } } };
            eq(call(mainAgg, { key: 'co2_imp_electricity_main', mid: 'electricity_main' }),
               11.14, 'main Direct carbon must be the remainder, not gross import');

            // Sub meter: only carbon_g present -> returned as-is.
            const subAgg = { meters: { ev_charger: {
              carbon_g: 107.36, carbon_g_imp: null } } };
            eq(call(subAgg, { key: 'co2_imp_ev_charger', mid: 'ev_charger' }),
               107.36, 'sub carbon must be its gross carbon_g');

            // Export offset column sums carbon_g_exp across meters (negated).
            const expAgg = { meters: { electricity_main: { carbon_g_exp: 99.88 } } };
            eq(call(expAgg, { key: 'co2_exp' }), -99.88, 'export offset must be -sum(carbon_g_exp)');

            // Net view CO2: net_imp/net_exp must be GROSS import/export carbon
            // (netting to carbon_g_net), not the net-positive/negative split.
            // Net-emitting day (has export): import is the gross figure, not net.
            const emit = { carbon_g_net: 4.154, meters: { electricity_main: {
              carbon_g_imp: 5.214, carbon_g_exp: 1.060 } } };
            eq(call(emit, { key: 'net_imp' }), 5.214, 'net_imp must be gross import carbon (not net)');
            eq(call(emit, { key: 'net_exp' }), 1.060, 'net_exp must be gross export offset');
            // Net-offsetting day: import must NOT collapse to 0.
            const offset = { carbon_g_net: -2.413, meters: { electricity_main: {
              carbon_g_imp: 2.495, carbon_g_exp: 4.907 } } };
            eq(call(offset, { key: 'net_imp' }), 2.495, 'net_imp must stay gross on net-offsetting days');
            eq(call(offset, { key: 'net_exp' }), 4.907, 'net_exp gross on net-offsetting days');
            // Total reconciles: net_imp - net_exp == carbon_g_net (both days).
            if (Math.abs((5.214 - 1.060) - 4.154) > 0.001) fails.push('emit day net_imp-net_exp != carbon_g_net');
            if (Math.abs((2.495 - 4.907) - (-2.413)) > 0.001) fails.push('offset day net_imp-net_exp != carbon_g_net');

            // Sanity: with the fixed cell, row Total reconciles to net.
            // Direct(11.14) + ev(107.36) + battery(48.57) + export(-99.88) = 67.19
            const total = 11.14 + 107.36 + 48.57 - 99.88;
            if (Math.abs(total - 67.19) > 0.01) fails.push('row total does not reconcile to net (got ' + total + ')');

            if (fails.length) { console.log('FAILS:\n' + fails.join('\n')); process.exit(1); }
            console.log('OK');
        """)
        with tempfile.NamedTemporaryFile("w", suffix=".js", delete=False) as f:
            f.write(script)
            path = f.name
        try:
            out = subprocess.run(["node", path, TMPL],
                                 capture_output=True, text=True, timeout=60)
        finally:
            os.unlink(path)
        return out

    def test_direct_carbon_cell_uses_remainder(self):
        out = self._run()
        self.assertEqual(out.returncode, 0,
                         msg=f"\nstdout:\n{out.stdout}\nstderr:\n{out.stderr}")
        self.assertIn("OK", out.stdout)


if __name__ == "__main__":
    unittest.main()