#!/usr/bin/env python3
"""
EMT Gap-Fill Automated Test Runner
====================================
Runs all gap-fill integration tests in one pass without needing to
start/stop the HA addon.

USAGE:
  python3 tests/test_gap_automated.py <path-to-blocks.db>

EXAMPLE:
  python3 tests/test_gap_automated.py /data/energy_meter_tracker/blocks.db
"""

import sys, os, asyncio, logging, shutil, sqlite3, tempfile, traceback
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

# ── Path setup ────────────────────────────────────────────────────────────────
REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO_ROOT)

# ── Mock aiohttp BEFORE importing engine ─────────────────────────────────────
sys.modules["aiohttp"] = MagicMock()
import engine  # noqa

# ── Colours ───────────────────────────────────────────────────────────────────
G="\033[92m"; R="\033[91m"; Y="\033[93m"; C="\033[96m"; B="\033[1m"; X="\033[0m"
def iso(dt): return dt.strftime("%Y-%m-%dT%H:%M:%S")

# ── Mock HA Client ────────────────────────────────────────────────────────────
class MockHA:
    """Returns real sensor values loaded from current_reads in the DB.
    All read sensors return numeric values so the 60s sensor wait exits immediately.
    """
    def __init__(self, db, override_import=None):
        self._s = {}; self._import_eid = None; self._ov = override_import
        c = sqlite3.connect(db); c.row_factory = sqlite3.Row
        # Build meter_id/channel → latest_value map
        vals = {}
        for r in c.execute("""SELECT meter_id, channel, channel_type, value
                               FROM current_reads ORDER BY captured_at DESC""").fetchall():
            k = (r["meter_id"], r["channel"], r["channel_type"])
            vals.setdefault(k, r["value"])
        # Map entity IDs to values
        for mc in c.execute("""SELECT mc.channel,
                                      mc.read_sensor, mc.rate_sensor,
                                      mc.standing_charge_sensor,
                                      m.meter_id as mid, m.is_sub_meter
                               FROM meter_channels mc
                               JOIN meters m ON m.id=mc.meter_id""").fetchall():
            mid = mc["mid"]; ch = mc["channel"]
            for field, ctype in [("read_sensor","read"),("rate_sensor","rate"),
                                  ("standing_charge_sensor","standing_charge")]:
                eid = mc[field]
                if eid:
                    v = vals.get((mid, ch, ctype))
                    self._s[eid] = str(v) if v is not None else "0"
            if not mc["is_sub_meter"] and mc["channel"] == "import":
                self._import_eid = mc["read_sensor"]
        c.close()

    def get_state(self, eid):
        if self._ov is not None and eid == self._import_eid: return str(self._ov)
        return self._s.get(eid, "0")  # return "0" not "unavailable" — avoids 60s wait

    def subscribe_state(self, eid, cb): pass
    async def preload_states(self, eids): pass
    async def get_entity_attributes(self, eid):
        return {"unit_of_measurement":"GBP/kWh"} if "rate" in eid else {}

# ── Log capture ───────────────────────────────────────────────────────────────
class Cap(logging.Handler):
    def __init__(self): super().__init__(); self.msgs=[]
    def emit(self, r): self.msgs.append(r.getMessage())
    def has(self, s): return any(s in m for m in self.msgs)

def cap_on():
    h=Cap(); h.setLevel(logging.DEBUG)
    logging.getLogger().addHandler(h); logging.getLogger().setLevel(logging.DEBUG)
    return h
def cap_off(h): logging.getLogger().removeHandler(h)

# ── DB helpers ────────────────────────────────────────────────────────────────
def tmpdb(src):
    t=tempfile.NamedTemporaryFile(suffix=".db",delete=False); t.close()
    shutil.copy2(src,t.name); return t.name

def set_gap(db, hours):
    c=sqlite3.connect(db); c.row_factory=sqlite3.Row
    cb=c.execute("SELECT block_start,block_end FROM current_block LIMIT 1").fetchone()
    ns=datetime.fromisoformat(cb["block_start"])-timedelta(hours=hours)
    ne=datetime.fromisoformat(cb["block_end"])-timedelta(hours=hours)
    old=cb["block_start"]
    c.execute("UPDATE current_block SET block_start=?,block_end=?,last_checkpoint=?",
              (iso(ns),iso(ne),iso(ns))); c.commit(); c.close()
    return iso(ns), old

def set_read(db, value):
    """Inflate the pre-gap block's imp_read_end to simulate a high meter reading before the gap.
    
    IMPORTANT: The engine uses get_last_block_before(current_block.start) to find pre-gap reads.
    current_block.start has been moved back N hours by set_gap(). So we need to find the last
    block before THAT new start time and update it — not just the latest block in the DB.
    """
    c = sqlite3.connect(db); c.row_factory = sqlite3.Row
    # Find the current_block start (already moved back by set_gap)
    cb = c.execute("SELECT block_start FROM current_block LIMIT 1").fetchone()
    if not cb: c.close(); return
    cb_start = cb["block_start"]
    # Update the last block BEFORE the gap start
    c.execute("""UPDATE blocks SET imp_read_end=?, imp_read_start=?
                 WHERE meter_id='electricity_main'
                   AND id=(SELECT id FROM blocks
                           WHERE meter_id='electricity_main' AND block_start < ?
                           ORDER BY block_start DESC LIMIT 1)""",
              (value, value, cb_start))
    c.commit(); c.close()

def real_import(db):
    c=sqlite3.connect(db); c.row_factory=sqlite3.Row
    r=c.execute("""SELECT value FROM current_reads
                   WHERE meter_id='electricity_main' AND channel='import' AND channel_type='read'
                   ORDER BY captured_at DESC LIMIT 1""").fetchone()
    c.close(); return float(r["value"]) if r else None

def n_blocks(db, start, end):
    c=sqlite3.connect(db)
    r=c.execute("SELECT COUNT(*) FROM blocks WHERE meter_id='electricity_main' AND block_start>=? AND block_start<?",
                (start,end)).fetchone()
    c.close(); return r[0] if r else 0

# ── Engine runner ─────────────────────────────────────────────────────────────
async def go(db, ha):
    engine._store=None; engine._post_gap_rates=None
    engine._meter_reset_detected=False; engine._last_ci_fetch=None
    engine._current_slot_mix={}
    orig=engine.BLOCKS_DB_PATH; engine.BLOCKS_DB_PATH=db
    h=cap_on()
    # Patch CI fetch and reduce sensor timeout so tests run fast
    with patch.object(engine, "_fetch_carbon_intensity", return_value=[]), \
         patch("engine._SENSOR_TIMEOUT" if hasattr(engine,"_SENSOR_TIMEOUT") else "builtins.print",
               new=1, create=True):
        try:
            await engine.engine_startup(ha)
        except Exception as e:
            logging.getLogger("test").error("engine_startup raised: %s\n%s", e, traceback.format_exc())
    cap_off(h); engine.BLOCKS_DB_PATH=orig
    # NOTE: deliberately keep _store open so tests can check engine state
    # Tests must call cleanup() after asserting on engine._meter_reset_detected
    return h.msgs

def cleanup():
    if engine._store:
        try: engine._store._conn.close()
        except Exception: pass
        engine._store=None

# ── Result ────────────────────────────────────────────────────────────────────
class T:
    def __init__(self,name): self.name=name; self.c=[]
    def ok(self,rid,cond,detail): self.c.append((rid,bool(cond),detail))
    @property
    def passed(self): return all(x[1] for x in self.c)

# ── Tests ─────────────────────────────────────────────────────────────────────
async def t1(db):
    r=T("Test 1: Short gap (3 hrs) — should gap-fill  [R1.1, R1.6]")
    tmp=tmpdb(db)
    try:
        set_gap(tmp, 3)
        logs=await go(tmp, MockHA(tmp))
        r.ok("R1.1a", any("session gap detected"  in m for m in logs), "gap detected in log")
        r.ok("R1.1b", any("gap blocks filled"     in m for m in logs), "'gap blocks filled' in log")
        r.ok("R1.1c", not any("gap-fill skipped"  in m for m in logs), "NOT skipped")
        r.ok("R1.6",  not engine._meter_reset_detected,                "no false reset flag")
    finally: cleanup(); os.unlink(tmp)
    return r

async def t2(db):
    r=T("Test 2: Long gap (15 hrs) — should skip gap-fill  [R1.2, R1.3, R1.4]")
    tmp=tmpdb(db)
    try:
        gs,ge=set_gap(tmp,15)
        before=n_blocks(tmp,gs,ge)
        logs=await go(tmp, MockHA(tmp))
        after=n_blocks(tmp,gs,ge)
        r.ok("R1.2a", any("gap-fill skipped"         in m for m in logs), "'gap-fill skipped' in log")
        r.ok("R1.2b", not any("gap blocks filled"    in m for m in logs), "NOT 'gap blocks filled'")
        r.ok("R1.3",  any("hours exceeds" in m for m in logs),             "gap hours in log")
        r.ok("R1.4",  after==before, f"no new blocks in gap window (before={before}, after={after})")
    finally: cleanup(); os.unlink(tmp)
    return r

async def t3(db):
    r=T("Test 3: 2-day gap + 99999 kWh read — should flag reset  [R2.1–R2.4]")
    tmp=tmpdb(db)
    try:
        set_gap(tmp,48); set_read(tmp,99999.0)
        real=real_import(db)
        ha=MockHA(tmp)
        if ha._import_eid and real: ha._s[ha._import_eid]=str(real)
        logs=await go(tmp,ha)
        r.ok("R2.1", any("gap-fill skipped"         in m for m in logs), "gap>12hr skipped (prereq)")
        r.ok("R2.2", any("meter read reset detected" in m for m in logs), "reset logged")
        r.ok("R2.3", any("99999" in m or "drop" in m.lower() for m in logs),"pre-gap value in log")
        r.ok("R2.4", engine._meter_reset_detected,                         "flag is True after startup")
    finally: cleanup(); os.unlink(tmp)
    return r

async def t4(db):
    r=T("Test 4: Long gap + 30 kWh drop — should NOT flag  [R2.10]")
    tmp=tmpdb(db)
    try:
        real=real_import(db)
        if real is None: r.ok("SETUP",False,"no import read in DB"); return r
        set_gap(tmp,15); set_read(tmp,real+30.0)
        ha=MockHA(tmp)
        if ha._import_eid: ha._s[ha._import_eid]=str(real)
        logs=await go(tmp,ha)
        r.ok("R2.10a", any("gap-fill skipped"      in m for m in logs), "gap>12hr skipped")
        r.ok("R2.10b", not any("reset detected"    in m for m in logs), "NO reset for 30kWh drop")
        r.ok("R2.10c", not engine._meter_reset_detected,                "flag remains False")
    finally: cleanup(); os.unlink(tmp)
    return r

async def t5(db):
    r=T("Test 5: Stale flag cleared on startup  [R2.5]")
    tmp=tmpdb(db)
    try:
        engine._meter_reset_detected=True
        await go(tmp, MockHA(tmp))
        r.ok("R2.5", not engine._meter_reset_detected,
             "_meter_reset_detected reset to False at startup (no long gap)")
    finally: cleanup(); os.unlink(tmp)
    return r

async def t6(db):
    r=T("Test 6: Short gap + huge read drop — should NOT flag  [R2.11]")
    tmp=tmpdb(db)
    try:
        set_gap(tmp,3); set_read(tmp,99999.0)
        real=real_import(db)
        ha=MockHA(tmp)
        if ha._import_eid and real: ha._s[ha._import_eid]=str(real)
        logs=await go(tmp,ha)
        r.ok("R2.11a", not any("gap-fill skipped"  in m for m in logs), "short gap NOT skipped")
        r.ok("R2.11b", not any("reset detected"    in m for m in logs), "NO reset for short gap")
        r.ok("R2.11c", not engine._meter_reset_detected,                "flag remains False")
    finally: cleanup(); os.unlink(tmp)
    return r

# ── Runner ────────────────────────────────────────────────────────────────────
def show(res):
    s=f"{G}✓ PASS{X}" if res.passed else f"{R}✗ FAIL{X}"
    print(f"\n{B}{s}  {res.name}{X}")
    for rid,ok,d in res.c:
        print(f"  {G+'✓' if ok else R+'✗'}{X} [{rid}] {G if ok else R}{d}{X}")

def _find_latest_backup():
    """Find the most recent backup zip in the HA share and extract blocks.db to a temp file."""
    import glob, zipfile
    for bdir in ["/share/energy_meter_tracker_backup/backups",
                 "/share/energy_meter_tracker_backup"]:
        for z in sorted(glob.glob(os.path.join(bdir, "*.zip")), reverse=True):
            try:
                with zipfile.ZipFile(z) as zf:
                    if "blocks.db" in zf.namelist():
                        tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False, prefix="emt_bak_")
                        tmp.close()
                        with zf.open("blocks.db") as src, open(tmp.name, "wb") as dst:
                            dst.write(src.read())
                        return tmp.name, z
            except Exception:
                continue
    return None, None

_DB_CANDIDATES = [
    "/data/energy_meter_tracker/blocks.db",
    os.path.expanduser("~/emt-data/energy_meter_tracker/blocks.db"),
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "blocks.db"),
]

async def main():
    _tmp_backup = None
    if len(sys.argv) >= 2:
        db = sys.argv[1]
    else:
        db = next((p for p in _DB_CANDIDATES if os.path.exists(p)), None)
        _tmp_backup = None
        if db:
            print(f"{Y}No path given — found DB at: {db}{X}")
        else:
            db, src_zip = _find_latest_backup()
            if db:
                _tmp_backup = db
                print(f"{Y}No path given — extracted from backup: {os.path.basename(src_zip)}{X}")
            else:
                print(f"{R}✗ blocks.db not found. Pass path as argument:{X}")
                print(f"  python3 test_gap_automated.py /path/to/blocks.db")
                for p in _DB_CANDIDATES: print(f"  (tried: {p})")
                print(f"  (tried: /share/energy_meter_tracker_backup/**/*.zip)")
                sys.exit(1)
    if not os.path.exists(db): print(f"{R}✗ Not found: {db}{X}"); sys.exit(1)
    print(f"\n{B}EMT Gap-Fill Automated Tests{X}  {datetime.now():%Y-%m-%d %H:%M:%S}")
    print(f"DB: {db}")
    print(f"{Y}Runs engine_startup() directly — addon does not need to be stopped{X}\n")

    results=[]
    for fn in [t1,t2,t3,t4,t5,t6]:
        print(f"{C}{fn.__name__}{X}... ",end="",flush=True)
        try: res=await fn(db)
        except Exception as e:
            res=T(fn.__name__); res.ok("ERROR",False,f"{e}")
        results.append(res)
        print(f"{G}PASS{X}" if res.passed else f"{R}FAIL{X}")
        if not res.passed: show(res)

    t=len(results); p=sum(1 for r in results if r.passed)
    cs=sum(len(r.c) for r in results); cp=sum(sum(1 for _,ok,_ in r.c if ok) for r in results)
    print(f"\n{'='*55}")
    print(f"{B}Tests: {p}/{t}  Checks: {cp}/{cs}{X}")
    if p==t: print(f"{G}{B}All tests passed ✓{X}")
    else:    print(f"{R}{B}{t-p} failed ✗{X}")
    print(f"{'='*55}\n")
    if _tmp_backup and os.path.exists(_tmp_backup): os.unlink(_tmp_backup)
    sys.exit(0 if p==t else 1)

if __name__=="__main__": asyncio.run(main())