"""Regression guard: device (sub-meter) sensors must survive a Change-Setup
round-trip through the wizard JS.

Bug it guards (3.0): startWizard detected device type by ID prefix while
wizApplyAndClose creates sub-meter IDs like "sub_meter_1718..." and tracks type
via meta.meter_type. A wizard-created device therefore wasn't detected on
re-entry, appeared deselected, and its sensors were dropped on the next save.

This test extracts the REAL wizard functions from meter_config.html, stubs the
DOM, seeds a config with a wizard-style EV sub-meter, runs
startWizard() -> wizApplyAndClose() (i.e. "finish Change Setup unchanged"), and
asserts every device sensor is preserved. Skipped where node is unavailable.
"""
import os
import shutil
import subprocess
import tempfile
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
TEMPLATE = os.path.join(HERE, "web", "templates", "meter_config.html")

HARNESS = r"""
const fs = require('fs'), vm = require('vm');
const TEMPLATE = process.argv[2];
let html = fs.readFileSync(TEMPLATE, 'utf8');
let js = (html.match(/<script[^>]*>([\s\S]*?)<\/script>/g) || [])
  .map(s => s.replace(/<\/?script[^>]*>/g, '')).join('\n');
js = js.replace(/\{\{[\s\S]*?\}\}/g, 'null').replace(/\{%[\s\S]*?%\}/g, '');

const SEED = JSON.stringify({ meters: {
  electricity_main: { meta:{type:'electricity',sub_meter:false,billing_day:1,block_minutes:30},
    channels:{import:{read:'sensor.imp',rate:''},export:{read:'sensor.exp',rate:''}} },
  sub_meter_1718000000123: { meta:{sub_meter:true,meter_type:'ev',device:'Zappi',
    parent_meter:'electricity_main',v2x_capable:true,
    device_power_sensor:'sensor.zappi_power',rate_source:'overlay'},
    channels:{import:{read:'sensor.zappi_import',rate:'sensor.zappi_rate'}} }
}});
js = js.replace('let config   = null;', 'let config = ' + SEED + ';');
js = js.replace('var DATA_SOURCE_MODE = null;', 'var DATA_SOURCE_MODE = "cad+api";');

const noop = () => {};
function fakeEl(){ return { value:'', checked:false, disabled:false, innerHTML:'',
  textContent:'', style:{}, classList:{add:noop,remove:noop,toggle:noop,contains:()=>false},
  addEventListener:noop, removeEventListener:noop, appendChild:noop, removeChild:noop,
  setAttribute:noop, getAttribute:()=>null, focus:noop, querySelector:()=>fakeEl(),
  querySelectorAll:()=>[], closest:()=>null, remove:noop, click:noop, insertAdjacentHTML:noop }; }
const sandbox = { console,
  document:{ getElementById:()=>fakeEl(), querySelector:()=>fakeEl(), querySelectorAll:()=>[],
    addEventListener:noop, createElement:()=>fakeEl(), body:{appendChild:noop} },
  fetch: async()=>({ json: async()=>({ok:true,source:'dcc',configured:false}) }),
  alert:noop, setTimeout:(f)=>{ if(f) f(); return 0; }, clearTimeout:noop, location:{href:''} };
sandbox.window = sandbox;
let prelude = ['renderWizStep','renderAll','closeWizard','show','showError','buildWizEntity',
  'attachEntityPickers','wizApplyEntityPickers'].map(n=>`function ${n}(){}`).join('\n')
  + '\nfunction computeWizSteps(){return (typeof WIZ_STEPS!=="undefined"&&WIZ_STEPS.length)?WIZ_STEPS:["survey","done"];}'
  + '\nvar apiUrl=function(u){return u;};\n';

const ctx = vm.createContext(sandbox);
try { vm.runInContext(prelude + js, ctx, {filename:'wiz.js'}); }
catch(e){ console.log('LOAD ERROR:', e.message); process.exit(2); }

(async()=>{
  vm.runInContext('startWizard();', ctx);
  const wiz = sandbox.wiz, config = vm.runInContext('config', ctx);
  await vm.runInContext('wizApplyAndClose();', ctx);
  await new Promise(r=>setTimeout(r,10));
  const sub = config.meters['sub_meter_1718000000123'];
  let ok=true, fails=[];
  const chk=(c,m)=>{ if(!c){ok=false;fails.push(m);} };
  chk(wiz.devices.indexOf('ev')!==-1,'ev not detected in wiz.devices');
  chk(!!sub,'sub-meter vanished from config');
  if(sub){
    chk(sub.channels.import.read==='sensor.zappi_import','read sensor lost: '+sub.channels.import.read);
    chk(sub.channels.import.rate==='sensor.zappi_rate','rate sensor lost: '+sub.channels.import.rate);
    chk(sub.meta.device_power_sensor==='sensor.zappi_power','device_power lost: '+sub.meta.device_power_sensor);
    chk(sub.meta.v2x_capable===true,'v2x_capable lost: '+sub.meta.v2x_capable);
    chk(sub.meta.rate_source==='overlay','rate_source lost: '+sub.meta.rate_source);
    chk(sub.meta.device==='Zappi','device name lost: '+sub.meta.device);
  }
  console.log(ok?'PASS':'FAIL: '+fails.join('; '));
  process.exit(ok?0:1);
})();
"""


@unittest.skipUnless(shutil.which("node"), "node not available")
class TestWizardDeviceRoundTrip(unittest.TestCase):
    def test_device_sensors_survive_change_setup(self):
        with tempfile.NamedTemporaryFile("w", suffix=".js", delete=False) as fh:
            fh.write(HARNESS)
            harness_path = fh.name
        try:
            res = subprocess.run(
                ["node", harness_path, TEMPLATE],
                capture_output=True, text=True, cwd=HERE, timeout=60)
        finally:
            os.unlink(harness_path)
        self.assertEqual(
            res.returncode, 0,
            msg=f"round-trip harness failed:\nSTDOUT:{res.stdout}\nSTDERR:{res.stderr}")
        self.assertIn("PASS", res.stdout)


if __name__ == "__main__":
    unittest.main()