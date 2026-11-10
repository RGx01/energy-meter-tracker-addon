"""Instance identity and backup-directory resolution (BL-5).

Why this exists
---------------
`/data` is per-add-on, so `blocks.db` is already isolated between instances
however they were installed. But in **supervised** mode backups are written to
`/share`, which is shared across *all* add-ons. Two EMT instances would list and
restore each other's backups — silent data loss.

So the supervised backup directory is namespaced by a slugified **site name**
(already in the config-period chain; no new user config). Standalone/Docker keeps
`/data/energy_meter_tracker/backup`: volume-scoped and therefore already isolated,
site-name-independent, and unchanged by a rename.

These defences are **install-method-agnostic by design** — they key on site
identity, not on add-on slug or repository — so EMT behaves correctly whether a
second instance came from a second add-on, the unendorsed repo-URL workaround, or
Docker. We neither detect nor block the workaround; we are simply correct under it.

Site rename (supervised only), all silent:
  1. destination free                -> os.rename (atomic; same filesystem)
  2. destination exists, marker=ours -> adopt it (renamed away and back)
  3. destination exists, foreign/none-> timestamp-suffix a fresh directory

Ownership is decided by **instance identity, not liveness** — a heartbeat would
risk adopting a merely-stopped sibling's directory.
"""

import glob
import json
import logging
import os
import re
import uuid
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

DATA_DIR = "/data/energy_meter_tracker"
LEGACY_SHARE_BACKUP_DIR = "/share/energy_meter_tracker_backup"
_SHARE_ROOT = "/share"
_SHARE_GLOB = "energy_meter_tracker_backup*"   # scan pattern for owned dirs
_MARKER_NAME = ".emt_instance"
_INSTANCE_ID_FILE = "instance_id"
# Install-scoped (NOT in blocks.db, so it survives a restore): the set of data
# lineages (db_uuids) whose "restored from another instance" notice the user has
# dismissed. Keyed by db_uuid so the same source lineage stays quiet across
# repeated restores. See docs/instance_isolation_design.md.
_ACK_FILE = "acknowledged_db_uuids"
_MAX_SLUG_LEN = 48
_DEFAULT_SLUG = "site"


def _utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")


def is_standalone() -> bool:
    return os.environ.get("EMT_MODE") == "standalone"


def slugify_site(site_name: str | None) -> str:
    """Site name -> filesystem/entity-safe slug.

    "Mum's House" -> mums_house ; "Flat 2b" -> flat_2b ; "" -> "site"
    """
    s = (site_name or "").strip().lower()
    s = s.replace("'", "").replace("\u2019", "")   # mum's house -> mums house
    s = re.sub(r"[^a-z0-9]+", "_", s).strip("_")
    s = re.sub(r"_{2,}", "_", s)[:_MAX_SLUG_LEN].strip("_")
    return s or _DEFAULT_SLUG


def get_instance_id(data_dir: str = DATA_DIR) -> str:
    """Stable per-instance UUID, minted once and persisted in /data.

    Nothing in the environment identifies an instance (no ADDON_SLUG, no usable
    hostname), and `/data` is per-instance and persistent by construction — so
    this survives restarts and upgrades and needs nothing from the Supervisor.
    """
    path = os.path.join(data_dir, _INSTANCE_ID_FILE)
    try:
        with open(path) as f:
            existing = f.read().strip()
        if existing:
            return existing
    except FileNotFoundError:
        pass
    except Exception as e:
        logger.warning("get_instance_id: read failed (%s); minting a new id", e)
    new_id = uuid.uuid4().hex
    try:
        os.makedirs(data_dir, exist_ok=True)
        with open(path, "w") as f:
            f.write(new_id)
        logger.info("get_instance_id: minted instance id %s", new_id[:8])
    except Exception as e:
        # Non-fatal: a non-persisted id still isolates this run's directory.
        logger.warning("get_instance_id: persist failed (%s); id is ephemeral", e)
    return new_id


def _read_marker(directory: str) -> str | None:
    try:
        with open(os.path.join(directory, _MARKER_NAME)) as f:
            return (json.load(f) or {}).get("instance_id")
    except Exception:
        return None


def _write_marker(directory: str, instance_id: str) -> None:
    try:
        os.makedirs(directory, exist_ok=True)
        with open(os.path.join(directory, _MARKER_NAME), "w") as f:
            json.dump({"instance_id": instance_id,
                       "written_at": datetime.now(timezone.utc).isoformat()}, f)
    except Exception as e:
        logger.warning("_write_marker: failed for %s: %s", directory, e)


def _owned_by_us(directory: str, instance_id: str) -> bool:
    return _read_marker(directory) == instance_id


def _find_owned_dir(instance_id: str, target: str | None = None) -> str | None:
    """Scan /share for a backup directory whose marker is ours.

    This is the authoritative ownership test — it keys on *instance identity*,
    which never changes on a restore, so a directory we created stays ours even
    after our DB has been replaced by a restore that carries a colliding site
    name (the prod -> prod dev case). Prefers `target` when we own it.
    """
    owned = [d for d in glob.glob(os.path.join(_SHARE_ROOT, _SHARE_GLOB))
             if os.path.isdir(d) and _read_marker(d) == instance_id]
    if not owned:
        return None
    if target and target in owned:
        return target
    return sorted(owned)[0]


def share_backup_dir(site_name: str | None) -> str:
    """The supervised backup directory for this site (no filesystem effects)."""
    return os.path.join(_SHARE_ROOT,
                        f"energy_meter_tracker_backup_{slugify_site(site_name)}")


def resolve_backup_dir(site_name: str | None, *, data_dir: str = DATA_DIR,
                       current_dir: str | None = None) -> str:
    """Resolve (and if needed migrate) this instance's backup directory.

    Standalone: always `<data_dir>/backup` — volume-scoped, site-independent.
    Supervised: `/share/energy_meter_tracker_backup_<site_slug>`, resolved by
    scanning for the directory this instance *owns* (marker == our instance id)
    before falling back to the site-name path. Always silent. See
    docs/instance_isolation_design.md.

    `current_dir` is accepted for backward compatibility but no longer needed —
    the /share scan finds our directory wherever it is.
    """
    if is_standalone():
        return os.path.join(data_dir, "backup")

    instance_id = get_instance_id(data_dir)
    target = share_backup_dir(site_name)

    # 1. Do we already own a directory? Scan by marker — authoritative, and the
    #    reason a restored-in sibling DB with a colliding site name stays put.
    owned = _find_owned_dir(instance_id, target)
    if owned:
        if owned == target:
            return target
        if not os.path.exists(target):
            # Site renamed and the new name is free — move our dir across
            # (atomic: same filesystem, so backups cannot be half-moved).
            try:
                os.rename(owned, target)
                _write_marker(target, instance_id)
                logger.info("resolve_backup_dir: site renamed; backups moved "
                            "%s -> %s", owned, target)
                return target
            except Exception as e:
                logger.warning("resolve_backup_dir: rename %s -> %s failed (%s); "
                               "staying put", owned, target, e)
                return owned
        # The ideal name is taken by a sibling but we already own a directory —
        # keep it. Stable across repeated foreign restores (prod -> prod dev).
        logger.info("resolve_backup_dir: keeping owned dir %s (ideal %s taken by "
                    "another instance)", owned, target)
        return owned

    # 2. We own nothing yet. An UNMARKED legacy directory belongs to this install
    #    (EMT only ever supported one instance before BL-5): adopt it, carrying
    #    the backups across. A marked legacy dir is a sibling's — never touched.
    if os.path.isdir(LEGACY_SHARE_BACKUP_DIR) \
            and _read_marker(LEGACY_SHARE_BACKUP_DIR) is None:
        dest = target if not os.path.exists(target) else f"{target}_{_utc_stamp()}"
        try:
            os.rename(LEGACY_SHARE_BACKUP_DIR, dest)
            _write_marker(dest, instance_id)
            logger.info("resolve_backup_dir: migrated legacy backups %s -> %s",
                        LEGACY_SHARE_BACKUP_DIR, dest)
            return dest
        except Exception as e:
            logger.warning("resolve_backup_dir: legacy migration failed (%s); "
                           "using %s", e, LEGACY_SHARE_BACKUP_DIR)
            return LEGACY_SHARE_BACKUP_DIR

    # 3. Fresh directory. Take the ideal name if free, else a timestamped one so
    #    we never merge into a sibling's directory.
    if not os.path.exists(target):
        _write_marker(target, instance_id)
        return target
    fresh = f"{target}_{_utc_stamp()}"
    logger.info("resolve_backup_dir: %s owned by another instance; using %s",
                target, fresh)
    _write_marker(fresh, instance_id)
    return fresh


# ── Data lineage (db_uuid) and foreign-restore detection ─────────────────────
# db_uuid lives INSIDE blocks.db (store_meta) and travels with the data through
# backup/restore. A native DB carries db_uuid == install_id; a restored DB
# carries the *source* install's id, so a mismatch means "restored from another
# instance." Ownership never keys on this — only the user-facing notice does.

def ensure_db_uuid(current: str | None, data_dir: str = DATA_DIR) -> str:
    """Return the db_uuid to persist in the DB.

    If the DB already carries one, keep it (immutable — it identifies the data
    lineage). If it has none (fresh install or pre-3.2.0 upgrade), mint one
    EQUAL to this install's id, marking the DB native (db_uuid == install_id).
    """
    if current:
        return current
    return get_instance_id(data_dir)


def _ack_path(data_dir: str) -> str:
    return os.path.join(data_dir, _ACK_FILE)


def _read_acknowledged(data_dir: str) -> set[str]:
    try:
        with open(_ack_path(data_dir)) as f:
            return set(json.load(f) or [])
    except FileNotFoundError:
        return set()
    except Exception as e:
        logger.warning("_read_acknowledged: %s", e)
        return set()


def acknowledge_db_uuid(db_uuid: str, data_dir: str = DATA_DIR) -> None:
    """Persist that the user dismissed the foreign-restore notice for this
    lineage. Stored in /data (install-scoped) so it survives the next restore."""
    if not db_uuid:
        return
    acks = _read_acknowledged(data_dir)
    if db_uuid in acks:
        return
    acks.add(db_uuid)
    try:
        os.makedirs(data_dir, exist_ok=True)
        with open(_ack_path(data_dir), "w") as f:
            json.dump(sorted(acks), f)
    except Exception as e:
        logger.warning("acknowledge_db_uuid: persist failed (%s)", e)


def foreign_restore_notice(db_uuid: str | None, data_dir: str = DATA_DIR) -> dict:
    """Describe whether the open DB is foreign to this install.

    Returns {"foreign": bool, "db_uuid": str|None, "acknowledged": bool}. A DB is
    foreign when its db_uuid differs from this install's id. Acknowledgement is
    per db_uuid (see acknowledge_db_uuid), so a routine repeated restore of the
    same source lineage is dismissed once and stays quiet.
    """
    if not db_uuid:
        return {"foreign": False, "db_uuid": None, "acknowledged": False}
    install_id = get_instance_id(data_dir)
    return {"foreign": db_uuid != install_id,
            "db_uuid": db_uuid,
            "acknowledged": db_uuid in _read_acknowledged(data_dir)}