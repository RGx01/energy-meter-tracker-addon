"""
test_rate_source_ui.py — the meter-config screen's rate-source toggle logic.

Extracts the REAL pure helpers from meter_config.html and asserts:
  • mainApiChecked() — the main-meter API checkbox state, including the
    inference fallback (no sensor mapped → API) for un-migrated configs;
  • deviceUsesMain() — the device "use main meter rate" checkbox state;
  • srcFromToggle() — the checkbox → stored value mapping.

Skipped where node is unavailable.
"""
import os
import shutil
import subprocess
import tempfile
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
TMPL = os.path.join(HERE, "web", "templates", "meter_config.html")


def _extract_helpers():
    with open(TMPL, encoding="utf-8") as fh:
        html = fh.read()
    start = html.index("function mainApiChecked(ch, kind) {")
    end = html.index("function buildSourceToggle")
    helpers = html[start:end].rstrip()
    ws = html.index("function wizMainSource(useApi, mode) {")
    we = html.index("async function wizApplyAndClose")
    return helpers + "\n" + html[ws:we].rstrip()


HARNESS = r"""
__FUNCS__

function eq(a, b, msg) {
  if (a !== b) { fails.push(msg + ' (got ' + JSON.stringify(a) + ')'); }
}
var fails = [];

// ── mainApiChecked: inference (no explicit source) ───────────────────────────
eq(mainApiChecked({}, 'rate'), true,  'no rate sensor → API');
eq(mainApiChecked({rate: 'sensor.x'}, 'rate'), false, 'rate sensor present → sensor');
eq(mainApiChecked({}, 'standing_charge'), true, 'no SC sensor → API');
eq(mainApiChecked({standing_charge_sensor: 'sensor.sc'}, 'standing_charge'), false,
   'SC sensor present → sensor');

// ── mainApiChecked: explicit source wins over inference ──────────────────────
eq(mainApiChecked({rate_source: 'api', rate: 'sensor.x'}, 'rate'), true,
   'explicit api beats a mapped sensor');
eq(mainApiChecked({rate_source: 'sensor', rate: ''}, 'rate'), false,
   'explicit sensor with no entity still reads as sensor');
eq(mainApiChecked({standing_charge_source: 'api'}, 'standing_charge'), true,
   'explicit api standing charge');

// ── deviceUsesMain ───────────────────────────────────────────────────────────
eq(deviceUsesMain({}), true, 'no own sensor → inherit main');
eq(deviceUsesMain({rate: 'sensor.ev'}), false, 'own sensor (no explicit) → use sensor');
eq(deviceUsesMain({rate_source: 'main', rate: 'sensor.ev'}), true,
   'explicit main beats a mapped sensor');
eq(deviceUsesMain({rate_source: 'sensor'}), false, 'explicit sensor → not main');

// ── srcFromToggle ────────────────────────────────────────────────────────────
eq(srcFromToggle(true, 'api'), 'api', 'checked api');
eq(srcFromToggle(false, 'api'), 'sensor', 'unchecked → sensor');
eq(srcFromToggle(true, 'main'), 'main', 'checked main');
eq(srcFromToggle(false, 'main'), 'sensor', 'device unchecked → sensor');

// ── wizMainSource (wizard, main meter) — explicit API flag + mode ────────────
eq(wizMainSource(true,  'api'), 'api', 'pure api → api');
eq(wizMainSource(false, 'api'), 'api', 'pure api ignores flag (no sensors)');
eq(wizMainSource(true,  'api+mini'), 'api', 'api+mini → api');
eq(wizMainSource(true,  'cad+api'), 'api', 'cad+api ticked → api');
eq(wizMainSource(false, 'cad+api'), 'sensor', 'cad+api unticked → sensor');
eq(wizMainSource(true,  'cad'), 'sensor', 'cad has no api → sensor');
eq(wizMainSource(false, 'cad'), 'sensor', 'cad unticked → sensor');

// ── wizDeviceSource (wizard, device) ─────────────────────────────────────────
eq(wizDeviceSource({rate_source: 'main'}, 'sensor.m'), 'main', 'explicit main');
eq(wizDeviceSource({rate_source: 'sensor'}, 'sensor.m'), 'sensor', 'explicit sensor');
eq(wizDeviceSource({rate: 'sensor.ev'}, 'sensor.m'), 'sensor', 'rate differs from main → own sensor');
eq(wizDeviceSource({rate: 'sensor.m'}, 'sensor.m'), 'main', 'rate == main default → inherit');
eq(wizDeviceSource({}, 'sensor.m'), 'main', 'no rate → inherit');
eq(wizDeviceSource({rate: ''}, 'sensor.m'), 'main', 'empty rate → inherit');

if (fails.length) { console.log('FAIL\n' + fails.join('\n')); process.exit(1); }
console.log('OK');
"""


@unittest.skipUnless(shutil.which("node"), "node not available")
class TestRateSourceUiHelpers(unittest.TestCase):
    def test_helpers(self):
        script = HARNESS.replace("__FUNCS__", _extract_helpers())
        d = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, d, ignore_errors=True)
        p = os.path.join(d, "t.js")
        with open(p, "w", encoding="utf-8") as f:
            f.write(script)
        out = subprocess.run(["node", p], capture_output=True, text=True)
        self.assertEqual(out.returncode, 0, out.stdout + out.stderr)
        self.assertIn("OK", out.stdout)


@unittest.skipUnless(shutil.which("node"), "node not available")
class TestWizardGeneratesChannelSources(unittest.TestCase):
    """Drive the REAL wizApplyAndClose on a fresh (empty) config and assert it
    wires rate_source onto every channel of the generated config."""

    HARNESS = r"""
const fs = require('fs'), vm = require('vm');
let html = fs.readFileSync(process.argv[2], 'utf8');
let js = (html.match(/<script[^>]*>([\s\S]*?)<\/script>/g) || [])
  .map(s => s.replace(/<\/?script[^>]*>/g, '')).join('\n');
js = js.replace(/\{\{[\s\S]*?\}\}/g, 'null').replace(/\{%[\s\S]*?%\}/g, '');
js = js.replace('let config   = null;', 'let config = { meters: {} };');
js = js.replace('var DATA_SOURCE_MODE = null;', 'var DATA_SOURCE_MODE = "cad+api";');

const noop = () => {};
function fakeEl(){ return { value:'', checked:false, disabled:false, innerHTML:'',
  textContent:'', style:{}, dataset:{}, classList:{add:noop,remove:noop,toggle:noop,contains:()=>false},
  addEventListener:noop, removeEventListener:noop, appendChild:noop, setAttribute:noop,
  getAttribute:()=>null, focus:noop, querySelector:()=>fakeEl(), querySelectorAll:()=>[],
  closest:()=>null, remove:noop, click:noop, insertAdjacentHTML:noop }; }
const sandbox = { console,
  document:{ getElementById:()=>fakeEl(), querySelector:()=>fakeEl(), querySelectorAll:()=>[],
    addEventListener:noop, createElement:()=>fakeEl(), body:{appendChild:noop} },
  fetch: async()=>({ ok:true, json: async()=>({ok:true}) }),
  alert:noop, setTimeout:(f)=>{ if(f) f(); return 0; }, clearTimeout:noop, location:{href:''} };
sandbox.window = sandbox;
let prelude = ['renderWizStep','renderAll','closeWizard','show','showError']
  .map(n=>`function ${n}(){}`).join('\n') + '\nvar apiUrl=function(u){return u;};\n';
const ctx = vm.createContext(sandbox);
vm.runInContext(prelude + js +
  '\nrenderAll=function(){};closeWizard=function(){};\n', ctx, {filename:'wiz.js'});

// Drive the wizard state directly (bypass the DOM-bound steps).
vm.runInContext(`
  wiz.devices = ['ev'];
  wiz._recomputeConfirmed = true;
  wiz.data = { mode:'cad+api', mainId:'electricity_main', site:'Home',
    billing_day:1, block_minutes:30, timezone:'UTC', supplier:'octopus',
    importRead:'sensor.imp', importRate:'', standingCharge:'',
    exportRead:'sensor.exp', exportRate:'sensor.exp_rate',
    importRateApi:true, standingChargeApi:true, exportRateApi:false,
    subMeters:{ ev:{ read:'sensor.ev', rate:'', rate_source:'main', name:'EV' } } };
`, ctx);

(async()=>{
  await vm.runInContext('wizApplyAndClose();', ctx);
  await new Promise(r=>setTimeout(r,10));
  const config = vm.runInContext('config', ctx);
  const main = config.meters['electricity_main'];
  const ev = Object.keys(config.meters).map(k=>config.meters[k])
                   .find(m => m.meta && m.meta.sub_meter);
  let fails=[];
  const eq=(a,b,m)=>{ if(a!==b) fails.push(m+' (got '+JSON.stringify(a)+')'); };
  eq(main.channels.import.rate_source, 'api', 'main import blank → api');
  eq(main.channels.import.standing_charge_source, 'api', 'main SC blank → api');
  eq(main.channels.export.rate_source, 'sensor', 'main export has sensor → sensor');
  eq(!!ev, true, 'device generated');
  if (ev) {
    eq(ev.channels.import.rate_source, 'main', 'device ticked → main');
    eq(ev.meta.rate_source, undefined, 'device no longer carries meta.rate_source');
    eq(ev.channels.import.read, 'sensor.ev', 'device read preserved');
  }
  if (fails.length) { console.log('FAIL\n'+fails.join('\n')); process.exit(1); }
  console.log('OK');
})();
"""

    def test_wizard_wires_channel_sources(self):
        d = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, d, ignore_errors=True)
        p = os.path.join(d, "h.js")
        with open(p, "w", encoding="utf-8") as f:
            f.write(self.HARNESS)
        out = subprocess.run(["node", p, TMPL], capture_output=True, text=True, timeout=60)
        self.assertEqual(out.returncode, 0, out.stdout + out.stderr)
        self.assertIn("OK", out.stdout)


@unittest.skipUnless(shutil.which("node"), "node not available")
class TestOhmeEvNote(unittest.TestCase):
    """The EV-card Ohme note: hidden off-API, nudges when no charge-mode entity is
    detected, confirms the verified path when one is, and always carries the
    session-sensor read caveat + GitHub feedback ask."""

    HARNESS = r"""
const fs = require('fs'), vm = require('vm');
const html = fs.readFileSync(process.argv[2], 'utf8');
const s = html.indexOf('function ohmeEvNote()');
const e = html.indexOf('function buildSubCard');
const fn = html.slice(s, e);
const ctx = { esc: (x) => String(x === undefined || x === null ? '' : x), console };
vm.createContext(ctx);
function run(mode, det) { ctx.DATA_SOURCE_MODE = mode; ctx.OHME_DETECTION = det;
  return vm.runInContext(fn + '\nohmeEvNote();', ctx); }
let fails = [];
const has = (str, sub, m) => { if (str.indexOf(sub) === -1) fails.push(m + ' (missing: ' + sub + ')'); };
const eq  = (a, b, m) => { if (a !== b) fails.push(m + ' (got ' + JSON.stringify(a).slice(0,40) + ')'); };

// hidden when not an API/IOG account
eq(run('cad', { found: false }), '', 'cad (no api) → empty');
eq(run('', { found: true, integration: 'official', charge_mode_entity: 'x' }), '', 'no mode → empty');

// API + undetected → nudge to install, plus caveats
const nud = run('cad+api', { found: false });
has(nud, 'Using an Ohme charger', 'nudge head');
has(nud, 'Install the official Ohme', 'nudge install');
has(nud, 'optimistically', 'nudge explains optimistic fallback');
has(nud, 'github.com', 'feedback link');

// API + detected (official) → verified copy + entity + caveats
const ver = run('api', { found: true, integration: 'official',
                         charge_mode_entity: 'select.ohme_home_pro_charge_mode' });
has(ver, 'charge mode detected', 'verified head');
has(ver, 'verified off-peak', 'verified body');
has(ver, 'select.ohme_home_pro_charge_mode', 'entity shown');
has(ver, 'official Ohme integration', 'official name');

// dan-r naming
const dr = run('api+mini', { found: true, integration: 'danr',
                            charge_mode_entity: 'binary_sensor.ohme_x_charge_slot_active' });
has(dr, "dan-r's HomeAssistant-Ohme", 'danr name');

if (fails.length) { console.log('FAIL\n' + fails.join('\n')); process.exit(1); }
console.log('OK');
"""

    def test_ohme_ev_note(self):
        d = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, d, ignore_errors=True)
        p = os.path.join(d, "h.js")
        with open(p, "w", encoding="utf-8") as f:
            f.write(self.HARNESS)
        out = subprocess.run(["node", p, TMPL], capture_output=True, text=True, timeout=60)
        self.assertEqual(out.returncode, 0, out.stdout + out.stderr)
        self.assertIn("OK", out.stdout)


if __name__ == "__main__":
    unittest.main()