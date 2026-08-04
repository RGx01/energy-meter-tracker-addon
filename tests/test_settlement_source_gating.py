"""
Regression: the Billing Settlement Source card and the unsettled-review card live
on the **Cost Corrections** page and are gated by setup, driven by
loadBillingSource() against the /api/billing-source response {mode, source,
api_available}:

  * **Settlement Source** (the DCC/CAD choice) shows ONLY for mode == 'cad+api' —
    the only setup where it's meaningful (pure-API has no CAD to fall back to;
    pure-CAD has no DCC).
  * **Review Unsettled Blocks** (and Retry settlement) show whenever a supplier API
    is present AND billing runs on DCC — including a pure-API setup.

The JS is extracted from corrections.html and exercised under node with a stubbed
fetch/document, so the assertions run against the real shipped function rather than
a copy. (node receives the template path as argv[2].)
"""
import json
import os
import shutil
import subprocess
import tempfile
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
TEMPLATE = os.path.join(HERE, "web", "templates", "corrections.html")

# A node harness that slices loadBillingSource() out of the template, runs it with
# a stubbed fetch returning a chosen {mode, source, api_available}, and prints the
# resulting display style of each gated element.
HARNESS = r"""
const fs = require('fs');
const vm = require('vm');
const tpl = fs.readFileSync(process.argv[2], 'utf8');
const apiAvailable = process.argv[3] === 'true';
const source = process.argv[4];
const mode = process.argv[5];

const start = tpl.indexOf('function loadBillingSource()');
const after = tpl.indexOf('\nfunction applyBillingSource', start);
if (start < 0 || after < 0) { console.error('loadBillingSource not found'); process.exit(2); }
const fnSrc = tpl.slice(start, after);

const els = {};
function el(id){
  if(!els[id]) els[id] = {style:{display:'__unset__'}, value:null, textContent:null};
  return els[id];
}
const sandbox = {
  apiUrl: function(p){ return p; },
  fetch: function(){
    return Promise.resolve({ json: function(){
      return Promise.resolve({mode: mode, source: source, unsettled: 5, api_available: apiAvailable});
    }});
  },
  document: { getElementById: function(id){ return el(id); } },
  window: {},
  console: console,
};
vm.createContext(sandbox);
vm.runInContext(fnSrc + '\nloadBillingSource();', sandbox);

setTimeout(function(){
  const get = function(id){ return (els[id] ? els[id].style.display : '__missing__'); };
  process.stdout.write(JSON.stringify({
    settlement: get('settlement-source-card'),
    unsettled:  get('unsettled-card'),
    retry:      get('btn-retry-settle'),
    apiFlag:    sandbox.window._apiAvailable,
  }));
}, 40);
"""


@unittest.skipUnless(shutil.which("node"), "node not available")
class TestSettlementSourceGating(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.script = tempfile.NamedTemporaryFile(
            mode="w", suffix=".js", delete=False)
        cls.script.write(HARNESS)
        cls.script.close()

    @classmethod
    def tearDownClass(cls):
        try:
            os.unlink(cls.script.name)
        except OSError:
            pass

    def _run(self, api_available, source, mode):
        out = subprocess.check_output(
            ["node", self.script.name, TEMPLATE,
             "true" if api_available else "false", source, mode],
            text=True, timeout=20)
        return json.loads(out)

    def test_cards_default_hidden_in_markup(self):
        # Fail-closed: both cards ship display:none so a fetch error can't flash a
        # meaningless control. The JS reveals them only for the right setup.
        with open(TEMPLATE, encoding="utf-8") as fh:
            html = fh.read()
        for cid in ('id="settlement-source-card"', 'id="unsettled-card"'):
            self.assertIn(cid, html)
            tag = html[html.index(cid):html.index(cid) + 120]
            self.assertIn("display:none", tag)

    def test_hidden_without_api(self):
        r = self._run(api_available=False, source="dcc", mode="cad")
        self.assertEqual(r["settlement"], "none")
        self.assertEqual(r["unsettled"], "none")
        self.assertEqual(r["retry"], "none")

    def test_cad_api_dcc_shows_everything(self):
        r = self._run(api_available=True, source="dcc", mode="cad+api")
        self.assertEqual(r["settlement"], "",
                         "source card shows for cad+api")
        self.assertEqual(r["unsettled"], "",
                         "unsettled shows with API + DCC")
        self.assertEqual(r["retry"], "")

    def test_cad_api_cad_hides_unsettled(self):
        # Both CAD and API present, but billing on CAD: source card stays visible
        # (so the user can switch back), the DCC-only unsettled card is hidden.
        r = self._run(api_available=True, source="cad", mode="cad+api")
        self.assertEqual(r["settlement"], "")
        self.assertEqual(r["unsettled"], "none")
        self.assertEqual(r["retry"], "none")

    def test_pure_api_hides_source_but_shows_unsettled(self):
        # Pure-API (no CAD): the DCC/CAD source toggle is meaningless and hidden,
        # but unsettled blocks still await DCC settlement, so that card + retry show.
        r = self._run(api_available=True, source="dcc", mode="api")
        self.assertEqual(r["settlement"], "none",
                         "source card hidden without CAD")
        self.assertEqual(r["unsettled"], "",
                         "unsettled shows for a pure-API DCC setup")
        self.assertEqual(r["retry"], "")


if __name__ == "__main__":
    unittest.main()
