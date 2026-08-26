# BL-50 — Unify user-job mutual exclusion (deny + idempotent-retry)

*Design note · target **v5.0.0** · correctness / robustness / architecture*

## 1. Problem

EMT has a growing set of long-running, DB-mutating jobs — some user-initiated, some
automatic — that all write the same block store over one shared SQLite connection.
When two of them overlap they race: the intermediate state of one is read by the
other. We have been fixing these **pairwise**, one collision at a time:

- device delete pauses/drains the live tick and drains a running import;
- device-history attribution now waits for a running import **and** a running verify
  (`api_import_running()` + `_verify_running()`);
- manual reprice/CSV is refused during migration;
- the delete recompute and attribution suppress PASS 2 logging independently.

This is O(N²): every new job must know about, and guard against, every other job.
We keep discovering a missing pair only when a user hits it in the field (the
`float * NoneType` crash was attribution overlapping the post-import verify tail;
the "wrote 0" heal failure was a *different* seam issue on the same flow). The
pairwise approach does not scale and is not auditable.

### 1a. Real-world instances observed

- **`float * NoneType` in device re-attribution (4.5.4)** — attribution ran PASS 2 over blocks a concurrent verify hadn't repriced yet.
- **`GET /api/config` 500 (`'<' not supported between 'str' and 'NoneType'`), 26 Aug 2026 prod-dev** — config read on a Flask worker thread against the SQLite connection the engine was using under load; the cross-thread read returned a garbled row (a column as `None`) → a `None` dict key → jsonify key-sort crash. No data corruption; transient. Fixed for this endpoint by marshalling the read onto the engine loop. Key lesson: **BL-50 must serialise reads too, not just mutating jobs** — a write-job coordinator alone wouldn't have caught this.

## 2. Goal

Replace the web of pairwise guards with a **single invariant**:

> At most one **bounded mutating job** runs at a time. A new user job that arrives
> while one is running is **denied** (refused at the endpoint, `409`), not silently
> queued or run concurrently. Because every such job is **idempotent**, the denied
> user simply retries when the running one finishes and gets the correct result.

The continuous **live engine tick / settlement drain** is *not* a bounded job; it
coexists and is handled by the existing loop-lock / `pause_engine()` layer. BL-50
governs **job-vs-job** exclusion, not job-vs-tick.

## 3. Why deny (not queue), and what idempotency buys us

**Idempotency does not make concurrency safe.** "Idempotent" means *re-running to
completion, serially, yields the correct end state* — it says nothing about safety
mid-flight on a shared connection. So idempotency does **not** license two jobs to
overlap. What it *does* license is the **retry**: after a deny, the user re-fires and
still lands correctly. Hence the rule collapses to **serialize + deny + retry**, and a
queue is unnecessary.

**Deny beats queue for a single-user home add-on** because a queue adds real cost:
persistence across add-on restarts, ordering/priority, cancelling queued items,
surfacing queue state, and — worst — a queued job running much later against data
that changed underneath it (surprising, hard to reason about). Deny is predictable,
reuses the run-lock UI we already have, and the retry is one click. Revisit queuing
only if users complain about retrying.

**Direction: refuse the newcomer.** In-progress work is never interrupted; the new
user job is denied. (Cancellation of a *running* job stays an explicit, separate user
action via the existing pause/cancel controls.)

## 4. Job registry

### Tier 1 — user-mutating jobs (acquire the coordinator; deny if busy)

| Job | Entry point(s) |
|---|---|
| Historical import — whole history / date range / **gap fill** | `import_api_history`, `run_gap_fill_job` (`/api/historical/api-import/start`, `/api/backfill/fill-gap`) |
| **Device delete** (+ parent recompute) | `api_meter_delete_data` (`/api/meter/<id>/delete-data`) |
| Device-history attribution | `run_attribution_job` (`/api/historical/attribute/start`) |
| Attribution backout / undo | `backout_recorder_attribution` (`/api/historical/attribute/backout`) |
| Delete blocks (range / all) | blocks-delete endpoints (`/api/blocks/delete/*`) |
| Manual reprice / CSV reprice | `reprice_imported_blocks_from_csv`, `repair_import_pricing` (manual) |
| DB restore | restore path (already pauses engine) |

### Tier 2 — automatic *batch* work that also counts as "busy"

These are not user-initiated, but they mutate history and are **bounded**, so a user
job must be denied while they run — and, critically, a job's own **tail stays inside
its envelope**:

- Post-import pricing **recovery + verify** — the tail of every import; the lock is
  held until this finishes, not just the write phase. *(This single change subsumes
  the ad-hoc `_verify_running()` guard: the import's own lock already covers its
  verify.)*
- Reprice-history migration sweep (first-run / upgrade).
- Historical carbon backfill.

### Not in the coordinator — different layer

- The **live engine tick / settlement drain** is continuous, not bounded. It coexists
  with everything. Jobs that need exclusive DB access still `pause_engine()` +
  drain the in-flight tick (as `api_meter_delete_data` does today via
  `run_exclusive(lambda: None)`). Folding the tick into the deny coordinator would
  deadlock normal operation.

## 5. The coordinator primitive

A single process-wide, thread-safe holder (the add-on is one process; jobs run on
either the Flask worker threads or the engine loop):

```python
# engine.py (or a small new module, e.g. job_lock.py)
import threading, time

_job_lock = threading.Lock()          # non-reentrant, single-flight
_job_state = {"name": None, "kind": None, "started_at": None, "phase": None}

class JobBusy(Exception):
    def __init__(self, state): self.state = state

def try_acquire_job(name: str, kind: str) -> bool:
    """Non-blocking. True if this caller now owns the exclusive job slot."""
    if not _job_lock.acquire(blocking=False):
        return False
    _job_state.update({"name": name, "kind": kind,
                       "started_at": _dt_now_iso_safe(), "phase": "running"})
    return True

def set_job_phase(phase: str):        # e.g. "importing" -> "verifying" (same envelope)
    _job_state["phase"] = phase

def release_job():
    _job_state.update({"name": None, "kind": None, "started_at": None, "phase": None})
    if _job_lock.locked():
        _job_lock.release()

def current_job() -> dict:            # for the UI + the 409 payload
    return dict(_job_state)
```

**Endpoint pattern (Tier 1):**

```python
if not try_acquire_job("device_delete", "delete"):
    return jsonify({"error": "busy", "running": current_job()}), 409
try:
    ... do the work (delete + recompute) ...
finally:
    release_job()
```

**Import → verify envelope (the key fix).** The import endpoint acquires the lock,
does the write phase (`set_job_phase("importing")`), then keeps the lock through the
recovery + verify tail (`set_job_phase("verifying")`), releasing only when the whole
pipeline is done. Attribution/delete/reprice therefore cannot slip into the
post-import window that caused the `float * NoneType` crash.

**Async jobs on the engine loop** (attribution, gap-fill) acquire the same lock; the
lock is plain `threading.Lock` so it works from both the loop thread and Flask
workers. The cooperative `control == "pause"/"cancel"` mechanism inside those jobs is
unchanged.

## 6. What this retires

Once the coordinator is in and applied at every Tier-1 entry point, delete these
now-redundant pairwise guards:

- `run_attribution_job`: the `or api_import_running() or _verify_running()` clause in
  the pause loop (BL-46/4.5.4 interim) — the coordinator denies attribution *before*
  it starts if an import/verify is running.
- `api_meter_delete_data`: the bespoke "drain a running import" loop — replaced by the
  coordinator deny (the delete never starts while an import runs).
- The "refuse manual reprice/fill during migration" guard (task #34) — becomes a
  generic coordinator deny.
- `delete_in_progress()` checks scattered through import/gap/carbon paths — folded
  into `current_job()`.

The live-tick pause/drain in `api_meter_delete_data` **stays** — it is user-vs-tick,
a different layer.

## 7. UI

Reuse the existing run-lock banner (`applyRunLock` / `bf-lock`). The coordinator's
`current_job()` drives a single source of truth: *"A {kind} is running ({phase}). You
can start a new job when it finishes."* Every Tier-1 launch button is disabled while
`current_job().name` is non-null; the `409` payload lets a raced click show the same
message instead of a stack trace. This also removes the separate per-panel
`_verifyActive`/`_importActive`/`_settling` reconciliation the historical-import page
currently juggles (the 4.5.4 heartbeat fix can be simplified to poll `current_job()`).

## 8. Edge cases

- **Crash / restart with the lock "held":** the lock is in-memory only, so a process
  restart clears it — correct (no job is actually running after a restart). Any job
  that must resume after restart (a persisted import checkpoint) re-acquires on its
  own startup path.
- **Deadlock avoidance:** the coordinator is non-reentrant and never acquired while
  holding the engine loop lock; jobs acquire the coordinator *first*, then pause/drain
  the tick inside. Fixed order = no ABBA deadlock.
- **Long verify tail blocking the user:** acceptable and correct — the whole point is
  that history is mid-mutation. The UI shows `phase: verifying` so it's not a mystery.
  If a specific tail proves too long to wait on, that's a *performance* item (chunk it,
  or make that tail resumable), not a reason to allow overlap.
- **Read-only endpoints** (charts, billing, usage stats) never acquire the
  coordinator — they read committed state and are unaffected.

## 9. Testing

- Unit: `try_acquire_job` is single-flight (second acquire returns False); `release`
  frees it; `current_job()` reports name/kind/phase.
- Concurrency: two Tier-1 endpoints raced → exactly one 200, one 409.
- Envelope: an import holds the lock across `importing → verifying → done`; an
  attribution start during the verify phase gets 409 (regression test for the
  `float * NoneType` class).
- Retire-guards regression: with the coordinator in place, the old pairwise guards
  removed, the delete→gap-fill→attribute sequence that used to race now denies-then-
  succeeds-on-retry.

## 10. Why v5.0.0

- It is a **cross-cutting refactor**: it touches every Tier-1 endpoint, the
  historical-import UI state machine, and removes several interim guards — a coherent
  architectural change, not a point fix.
- It pairs naturally with the **5.0.0 aggregation-unify** (BL-27: retire the legacy
  `imp_*` columns so segments are the sole representation). Both are "stop the
  recurring drift/​race bug class at the root" work, and doing them together means the
  guard-removal and the column-removal land in one migration-gated release.
- 4.5.x already makes today's behaviour **correct** via the targeted guards; BL-50 is
  the **simplification** that makes it correct *by construction* and cheap to extend.

## 11. Build checklist

1. Add `job_lock` primitive (`try_acquire_job` / `set_job_phase` / `release_job` /
   `current_job` + `JobBusy`).
2. Wrap each Tier-1 entry point in acquire/try/finally-release; return `409 + running`
   on deny.
3. Extend the import path to hold the lock across the recovery + verify tail
   (`set_job_phase`), and make the Tier-2 batches (migration sweep, carbon backfill)
   acquire it too.
4. Delete the retired pairwise guards (§6).
5. Point the run-lock UI at `current_job()`; simplify the historical-import poll.
6. Tests (§9).
7. Docs: this note → `docs/design/`, ROADMAP entry (target 5.0.0), CHANGELOG on ship.
