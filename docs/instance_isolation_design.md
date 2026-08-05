# Instance Isolation — Backup Directory Ownership & Foreign-Restore Detection (BL-5)

Refines the BL-5 design in `ROADMAP.md`. Locks the ownership rule and adds
lineage tracking so EMT is correct under **multiple instances on a shared
`/share`**, including the routine **restore-across-instances** workflow.

---

## The problem, restated

`/data` is per-add-on, so `blocks.db` is already isolated between instances
however they were installed. The only shared surface in supervised mode is
`/share`, where backups live — two instances can list and restore each other's
backups (silent data loss). BL-5 namespaces the backup directory per instance.

The subtle case is a **restore across instances on the same machine**. Real
example (the maintainer's own setup):

- **dev** — its own machine, its own `/share`. Fully isolated, nothing to do.
- **prod** and **prod dev** — *share one machine, one `/share`.*
- A **prod → prod dev** restore is done **often**, on purpose.

A backup carries the whole config-period chain, including `site_name`. So the
moment prod dev restores prod, **prod dev's DB reports prod's site name**, and
both instances compute the *same* ideal backup path. The ownership rule has to
keep them apart anyway — and do so stably across repeated restores.

## Two identities — keep them separate

| Identity | Lives in | Mutable on restore? | Role |
|---|---|---|---|
| `install_id` | `/data/energy_meter_tracker/instance_id` | **No** — minted once per install | **Sole ownership key** for a backup directory |
| `db_uuid` | `blocks.db` → `store_meta` | **Yes** — travels with the data | Identifies the **data lineage**; drives the foreign-restore notice only |

On a native first run (fresh install or in-place upgrade) `db_uuid` is minted
**equal to `install_id`**, so a native database satisfies `db_uuid == install_id`.
A restored database carries the *source* install's id, so `db_uuid != install_id`
is precisely "this data came from another instance."

### Why ownership must key on `install_id`, not `db_uuid`

If the backup directory were owned by the uuid inside the DB, then after a
prod → prod dev restore prod dev's DB carries `db_uuid_prod`, it would match
prod's directory marker, and **prod dev would adopt prod's live backup
directory** — two live instances writing one directory, on every restore. That
is the exact footgun BL-5 exists to remove. `install_id` never changes on a
restore, so prod dev keeps its own directory no matter whose data it is holding.

## Ownership resolution — scan first

Resolve the backup directory by **scanning `/share` for a directory whose marker
is my `install_id`, before falling back to the site-name path.** The scan is
what makes the restore case stable:

```
if standalone: return <data>/backup            # volume-scoped, site-independent

install_id = get_instance_id()
target     = /share/energy_meter_tracker_backup_<slug(site_name)>

owned = scan /share for a dir whose marker == install_id
if owned:
    if owned == target:            return target            # already ideal
    if target does not exist:      os.rename(owned -> target); return target   # site renamed
    else:                          return owned             # ideal taken by a sibling; keep ours (stable)

# own nothing yet
adopt an UNMARKED legacy /share/energy_meter_tracker_backup (single-instance upgrade)
if target free:                    create+mark(target);          return target
else:                              create+mark(target_<utc_stamp>); return target_<utc_stamp>
```

Without the scan, prod dev after a restore would follow the (prod) site name to
`..._<prodslug>`, find it owned by prod, and mint a **new timestamped directory
on every restart** — directory proliferation. With the scan it finds and reuses
its own directory every time.

### Walkthrough — prod + prod dev on one `/share`

| Step | prod | prod dev |
|---|---|---|
| baseline | `install_id_P`, dir `..._home`, `db_uuid == install_id_P` | `install_id_PD`, dir `..._home_dev`, `db_uuid == install_id_PD` |
| restore prod → prod dev | unchanged | DB now `db_uuid_P`, site "Home" |
| next start / backup | resolves `..._home` by its own id | **scan finds marker `install_id_PD` → keeps `..._home_dev`**; ignores that the DB now says "Home" |
| foreign notice | — | `db_uuid_P != install_id_PD` → one dismissible notice; dismissed per `db_uuid` |
| rename prod dev site back to "Home Dev" | — | scan finds owned dir, `os.rename` to `..._home_dev` |

Directory names are cosmetic; correctness comes from the marker, not the name.

## Site rename — three silent cases (unchanged, now scan-driven)

1. **Destination free** → `os.rename` the owned dir across (atomic, same fs).
2. **Destination exists, marker is ours** (renamed away and back) → adopt it.
3. **Destination exists, foreign or unmarked** → we keep our own scanned dir, or
   if we own none, timestamp-suffix a fresh one. Never merge, never touch theirs.

Ownership is decided by **identity, not liveness** — a heartbeat would risk
adopting a merely-stopped sibling's directory.

## Foreign-restore notice — dismiss per lineage, not per session

A DB with `db_uuid != install_id` raises **one** dismissible notice in the BL-6
notification region: *"restored data from another instance."* Because it is a
routine action for a multi-instance operator, it must not nag:

- **Acknowledgement is keyed by `db_uuid`** and stored in `/data`
  (`acknowledged_db_uuids`) — **install-scoped, so it survives the next restore.**
  It must **not** live in `blocks.db`, which a restore replaces.
- `db_uuid` is minted once per lineage, so **every prod backup carries the same
  `db_uuid_prod`.** Dismiss it once and prod-restores stay quiet forever; a
  genuinely new lineage (a third `db_uuid`) still surfaces.

## Edge cases & non-goals

- **Pre-3.2.0 DB upgraded in place** — no `db_uuid`; mint one **equal to
  `install_id`** (native). No notice.
- **Restore a pre-3.2.0 backup** — DB has no `db_uuid`; treated as native on
  mint. (Accepted: a very old own-backup restore is indistinguishable from a
  fresh native DB — harmless, it's your data.)
- **Reinstall that lost `/data`** — a new `install_id` won't recognise its own
  old backup dir. **Explicitly out of scope** (confirmed not a real workflow):
  designing for it is the only thing that would pull ownership toward `db_uuid`
  matching, which breaks the restore case above. Old backups are intact on
  `/share` and recoverable by a manual point-restore.
- **Standalone/Docker** — backup dir is `<data>/backup`, volume-scoped and
  site-independent; none of this applies. Multiple Docker instances are isolated
  by separate data volumes and host port mappings.
- **`sensor.energy_meter_tracker_api_deprecations`** — not namespaced; two
  instances compute the same value, so the write is idempotent.

## Implementation map

- `instance.py` — `_find_owned_dir` scan; scan-first `resolve_backup_dir`;
  `ensure_db_uuid`, `foreign_restore_notice`, `acknowledge_db_uuid`
  (acknowledgement file in `/data`).
- `block_store.py` — `db_uuid` in `store_meta` via existing `get_meta`/`set_meta`.
- `engine.py` — at startup, after backup-dir resolution: ensure `db_uuid`,
  compute the notice, expose it as `FOREIGN_RESTORE_NOTICE`.
- `web/server.py` — `GET /api/instance/notice`, `POST /api/instance/notice/dismiss`.
- `web/templates/base.html` — fetch the notice, render via `EMT.notify`, POST the
  dismissal (mirrors the BL-6 update-available banner).
