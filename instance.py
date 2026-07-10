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
_MARKER_NAME = ".emt_instance"
_INSTANCE_ID_FILE = "instance_id"
_MAX_SLUG_LEN = 48
_DEFAULT_SLUG = "site"


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


def share_backup_dir(site_name: str | None) -> str:
    """The supervised backup directory for this site (no filesystem effects)."""
    return os.path.join(_SHARE_ROOT,
                        f"energy_meter_tracker_backup_{slugify_site(site_name)}")


def resolve_backup_dir(site_name: str | None, *, data_dir: str = DATA_DIR,
                       current_dir: str | None = None) -> str:
    """Resolve (and if needed migrate) this instance's backup directory.

    Standalone: always `<data_dir>/backup` — volume-scoped, site-independent.
    Supervised: `/share/energy_meter_tracker_backup_<site_slug>`, with the
    rename handling described in the module docstring. Always silent.

    `current_dir` is the directory in use before this call (if known), used to
    move an existing directory across on a site rename.
    """
    if is_standalone():
        return os.path.join(data_dir, "backup")

    instance_id = get_instance_id(data_dir)
    target = share_backup_dir(site_name)

    # First run on an existing install: adopt the legacy un-suffixed directory by
    # renaming it across, so nothing is orphaned and the backup list is unchanged.
    if current_dir is None and not os.path.exists(target) \
            and os.path.isdir(LEGACY_SHARE_BACKUP_DIR) \
            and _read_marker(LEGACY_SHARE_BACKUP_DIR) in (None, instance_id):
        try:
            os.rename(LEGACY_SHARE_BACKUP_DIR, target)
            logger.info("resolve_backup_dir: migrated legacy backup dir -> %s",
                        target)
            _write_marker(target, instance_id)
            return target
        except Exception as e:
            logger.warning("resolve_backup_dir: legacy migration failed (%s); "
                           "continuing with %s", e, LEGACY_SHARE_BACKUP_DIR)
            return LEGACY_SHARE_BACKUP_DIR

    if not os.path.exists(target):
        # Case 1 — destination free. Move our existing directory across if we
        # have one (atomic: same filesystem), else create it.
        if current_dir and os.path.isdir(current_dir) \
                and _owned_by_us(current_dir, instance_id):
            try:
                os.rename(current_dir, target)
                logger.info("resolve_backup_dir: site renamed; backups moved "
                            "%s -> %s", current_dir, target)
            except Exception as e:
                logger.warning("resolve_backup_dir: rename failed (%s); staying "
                               "on %s", e, current_dir)
                return current_dir
        _write_marker(target, instance_id)
        return target

    if _owned_by_us(target, instance_id):
        # Case 2 — our own former directory (renamed away and back). Adopt it:
        # the old backups reappear and nothing moves.
        logger.info("resolve_backup_dir: adopting our previous backup dir %s",
                    target)
        return target

    # Case 3 — a sibling instance may own it (or it is unmarked). Never merge,
    # never touch their data: take a timestamped directory of our own.
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    fresh = f"{target}_{stamp}"
    logger.info("resolve_backup_dir: %s is owned by another instance; using %s",
                target, fresh)
    _write_marker(fresh, instance_id)
    return fresh