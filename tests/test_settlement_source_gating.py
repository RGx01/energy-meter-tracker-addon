"""
Regression: the Billing Settlement Source card (and the unsettled-review card)
must be hidden unless a supplier API is configured — i.e. loadBillingSource()
toggles them off when the /api/billing-source response has api_available=false.

Without an API, DCC settlement can never arrive, so a DCC/CAD toggle plus an
"N block(s) awaiting DCC settlement" count is meaningless and misleading.

The JS is extracted from data_management.html and exercised under node with a
stubbed fetch/document, so the assertions run against the real shipped function
rather than a copy. (node receives the template path as argv[2].)
"""
import json
import os
import shutil
import subprocess
import tempfile
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
TEMPLATE = os.path.join(HERE, "web", "templates", "data_management.html")

# A node harness that slices loadBillingSource() out of the template, runs it
# with a stubbed fetch returning a chosen {source, api_available}, and prints the
# resulting display style of each gated element.
HARNESS = r"""
const fs = require('fs');
const vm = require('vm');
const tpl = fs.readFileSync(process.argv[2], 'utf8');
const apiAvailable = process.argv[3] === 'true';
const source = process.argv[4];

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
      return Promise.resolve({source: source, unsettled: 5, api_available: apiAvailable});
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

    def _run(self, api_available, source):
        out = subprocess.check_output(
            ["node", self.script.name, TEMPLATE,
             "true" if api_available else "false", source],
            text=True, timeout=20)
        return json.loads(out)

    def test_card_default_hidden_in_markup(self):
        # Fail-closed: the card ships display:none so a fetch error can't flash a
        # meaningless toggle. The JS reveals it only when api_available.
        with open(TEMPLATE, encoding="utf-8") as fh:
            html = fh.read()
        self.assertIn('id="settlement-source-card"', html)
        idx = html.index('id="settlement-source-card"')
        # the style on the same tag must default to hidden
        tag = html[idx:idx + 120]
        self.assertIn("display:none", tag)

    def test_hidden_without_api(self):
        r = self._run(api_available=False, source="dcc")
        self.assertEqual(r["settlement"], "none",
                         "settlement-source card must hide without an API")
        self.assertEqual(r["unsettled"], "none")
        self.assertEqual(r["retry"], "none")

    def test_shown_with_api_dcc(self):
        r = self._run(api_available=True, source="dcc")
        self.assertEqual(r["settlement"], "",
                         "settlement-source card must show with an API")
        self.assertEqual(r["unsettled"], "",
                         "unsettled card shows in DCC mode with an API")
        self.assertEqual(r["retry"], "")

    def test_shown_with_api_cad_hides_unsettled(self):
        # API present but billing on CAD: the source card stays visible (so the
        # user can switch back), but the DCC-only unsettled card is hidden.
        r = self._run(api_available=True, source="cad")
        self.assertEqual(r["settlement"], "")
        self.assertEqual(r["unsettled"], "none")
        self.assertEqual(r["retry"], "none")


if __name__ == "__main__":
    unittest.main()