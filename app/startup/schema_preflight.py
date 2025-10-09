"""Schema preflight / auto-upgrade helper.

Runs before the Flask app is created. It:
1. Connects to Kuzu (via SafeKuzuManager so normal init still applies)
2. Compares expected NODE + RELATIONSHIP schemas against current database
3. If any new columns/properties or entirely new relationship tables are missing it creates a safety backup
4. Applies CREATE / ALTER statements (additive only)

We deliberately restrict operations to additive evolutions (create table, add property).
No destructive changes (drops, renames) are attempted automatically.

Relationship support: we now handle property additions AND can create a new
relationship table if it does not exist (e.g. future series contributor links).

Environment flags:
    DISABLE_SCHEMA_PREFLIGHT=true  -> skip everything
    SKIP_PREFLIGHT_BACKUP=true     -> don't create backup before changes
    PREFLIGHT_REL_ONLY=true        -> only process relationships
    PREFLIGHT_NODES_ONLY=true      -> only process nodes
"""
from __future__ import annotations

import os
import logging
import json
import hashlib
from pathlib import Path
import time
from typing import Dict, List, Tuple, Any
from datetime import datetime

from app.utils.safe_kuzu_manager import get_safe_kuzu_manager
from app.migrations.runner import run_pending as run_additive_migrations

logger = logging.getLogger(__name__)

_SCHEMA_CACHE: Dict[str, Any] = {}
_SCHEMA_META: Dict[str, str] = {}
_PREFLIGHT_RAN = False
_PREFLIGHT_DB_KEY: str | None = None

LOCK_TIMEOUT_SECONDS = 30
LOCK_POLL_INTERVAL = 0.25
LOCK_MAX_AGE_SECONDS = int(os.getenv("SCHEMA_PREFLIGHT_LOCK_MAX_AGE", "300"))  # stale lock cleanup


def _get_data_root() -> Path:
    """Best-effort resolution of data directory root (parent of kuzu folder)."""
    kuzu_path = Path(os.getenv("KUZU_DB_PATH", "data/kuzu")).resolve()
    # If path ends with 'kuzu' use its parent; else treat as given
    return kuzu_path.parent if kuzu_path.name == "kuzu" else kuzu_path


def _marker_file() -> Path:
    return _get_data_root() / "schema_preflight_state.json"


def _lock_file() -> Path:
    return _get_data_root() / "schema_preflight.lock"


def _load_marker() -> Dict[str, Any]:
    mf = _marker_file()
    if not mf.exists():
        return {}
    try:
        return json.loads(mf.read_text())
    except Exception:
        return {}


def _write_marker(meta: Dict[str, Any]):
    try:
        payload = {"version": meta.get("version"), "sha256": meta.get("sha256"), "written_at": datetime.utcnow().isoformat()}
        _marker_file().write_text(json.dumps(payload, indent=2))
    except Exception as e:
        logger.warning(f"Failed writing schema preflight marker: {e}")


def _pid_alive(pid: int) -> bool:
    try:
        # Signal 0 just checks existence on POSIX
        os.kill(pid, 0)
        return True
    except Exception:
        return False


def _acquire_lock() -> bool:
    lf = _lock_file()
    start = time.time()
    while True:
        try:
            # O_CREAT | O_EXCL ensures atomic creation; 'x' mode simpler in Python
            fd = os.open(lf, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            os.write(fd, str(os.getpid()).encode())
            os.close(fd)
            return True
        except FileExistsError:
            # Stale lock detection
            try:
                stat = lf.stat()
                age = time.time() - stat.st_mtime
                pid_txt = lf.read_text().strip() if lf.is_file() else ""
                pid_val = int(pid_txt) if pid_txt.isdigit() else None
                if age > LOCK_MAX_AGE_SECONDS or (pid_val and not _pid_alive(pid_val)):
                    logger.warning("Schema preflight: removing stale lock file")
                    lf.unlink(missing_ok=True)
                    continue  # retry acquire immediately
            except Exception:
                pass
            # If timeout exceeded, skip running but allow read-only check
            if time.time() - start > LOCK_TIMEOUT_SECONDS:
                logger.warning("Timeout waiting for schema preflight lock; assuming another process handled it")
                return False
            time.sleep(LOCK_POLL_INTERVAL)


def _release_lock():
    try:
        _lock_file().unlink(missing_ok=True)
    except Exception:
        pass


def _load_master_schema() -> Dict[str, Any]:
    """Load and cache the master schema JSON.

    Path resolution order:
      1. Env var MASTER_SCHEMA_PATH if set
      2. app/schema/master_schema.json relative to this file
    """
    global _SCHEMA_CACHE
    if _SCHEMA_CACHE:
        return _SCHEMA_CACHE
    default_path = Path(__file__).resolve().parent.parent / "schema" / "master_schema.json"
    schema_path = Path(os.getenv("MASTER_SCHEMA_PATH", str(default_path)))
    if not schema_path.exists():
        raise FileNotFoundError(f"Master schema JSON not found at {schema_path}")
    raw = schema_path.read_bytes()
    sha256 = hashlib.sha256(raw).hexdigest()
    try:
        data = json.loads(raw)
    except Exception as e:
        raise ValueError(f"Failed to parse master schema JSON: {e}")
    version = data.get("version")
    _SCHEMA_CACHE = data
    _SCHEMA_META["path"] = str(schema_path)
    if version is not None:
        _SCHEMA_META["version"] = str(version)
    _SCHEMA_META["sha256"] = sha256
    return data

def _column_exists(conn, table: str, column: str) -> bool:
    """Return True if a property exists by attempting to project it.

    Kuzu raises an exception like 'Cannot find property <column>' if missing.
    """
    try:
        conn.execute(f"MATCH (n:{table}) RETURN n.{column} LIMIT 1")
        return True
    except Exception as e:  # broad intentionally
        msg = str(e)
        if "Cannot find property" in msg and column in msg:
            return False
        if "does not exist" in msg.lower():
            return False
        logger.debug(f"Schema preflight: ambiguous error probing {table}.{column}: {e}")
        return True

def _detect_missing_columns(conn) -> List[Tuple[str, str, str]]:
    schema = _load_master_schema()
    node_defs: Dict[str, Any] = schema.get("nodes", {})
    missing: List[Tuple[str, str, str]] = []
    for table, meta in node_defs.items():
        columns: Dict[str, str] = meta.get("columns", {})
        pk = meta.get("primary_key", "id")
        for col, col_type in columns.items():
            if col == pk:  # skip PK (cannot ALTER ADD)
                continue
            if not _column_exists(conn, table, col):
                missing.append((table, col, col_type))
    return missing


def _relationship_table_exists(conn, rel_type: str) -> bool:
    try:
        conn.execute(f"MATCH ()-[r:{rel_type}]->() RETURN COUNT(r) LIMIT 1")
        return True
    except Exception as e:
        if any(tok in str(e).lower() for tok in ["does not exist", "unknown", "cannot find"]):
            return False
        return True  # Ambiguous -> assume exists


def _relationship_property_exists(conn, rel_type: str, prop: str) -> bool:
    try:
        conn.execute(f"MATCH ()-[r:{rel_type}]->() RETURN r.{prop} LIMIT 1")
        return True
    except Exception as e:
        msg = str(e)
        if "Cannot find property" in msg and prop in msg:
            return False
        if any(tok in msg.lower() for tok in ["does not exist", "unknown", "cannot find"]):
            # Table missing -> treat as missing property (handled upstream)
            return False
        return True


def _detect_relationship_changes(conn):
    schema = _load_master_schema()
    rel_defs: Dict[str, Any] = schema.get("relationships", {})
    create_missing: List[str] = []
    add_props: List[Tuple[str, str, str]] = []
    for rel_type, meta in rel_defs.items():
        exists = _relationship_table_exists(conn, rel_type)
        if not exists:
            create_missing.append(rel_type)
            continue
        props: Dict[str, str] = meta.get("properties", {})
        for prop, ptype in props.items():
            if not _relationship_property_exists(conn, rel_type, prop):
                add_props.append((rel_type, prop, ptype))
    return create_missing, add_props

def _create_backup_if_needed(reason: str) -> None:
    try:
        from app.services.simple_backup_service import SimpleBackupService
        service = SimpleBackupService()
        service.create_backup(description="Automatic pre schema upgrade backup", reason=reason)
    except Exception as e:
        logger.warning(f"Schema preflight: failed to create backup: {e}")

def _apply_alter_statements(conn, to_add: List[Tuple[str, str, str]]):
    for table, col, col_type in to_add:
        try:
            logger.info(f"Adding missing column {table}.{col} ({col_type})")
            conn.execute(f"ALTER TABLE {table} ADD {col} {col_type}")
        except Exception as e:
            if "already exists" in str(e).lower():
                logger.info(f"Column {table}.{col} already exists (race)")
                continue
            raise


def _apply_relationship_creates(conn, rel_types: List[str]):
    schema = _load_master_schema()
    rel_defs: Dict[str, Any] = schema.get("relationships", {})
    for rel in rel_types:
        meta = rel_defs.get(rel, {})
        from_label = meta.get("from")
        to_label = meta.get("to")
        if not from_label or not to_label:
            logger.warning(f"Cannot create relationship {rel}: missing from/to")
            continue
        props: Dict[str, str] = meta.get("properties", {})
        parts = [f"{k} {v}" for k, v in props.items()]
        prop_section = (",\n            ".join(parts)) if parts else ""
        ddl = f"""
        CREATE REL TABLE {rel}(
            FROM {from_label} TO {to_label}{(',' if prop_section else '')}
            {prop_section}
        )
        """
        logger.info(f"Creating missing relationship table {rel}")
        conn.execute(ddl)


def _apply_relationship_alters(conn, props: List[Tuple[str, str, str]]):
    for rel_type, prop, ptype in props:
        try:
            logger.info(f"Adding missing relationship property {rel_type}.{prop} ({ptype})")
            conn.execute(f"ALTER TABLE {rel_type} ADD {prop} {ptype}")
        except Exception as e:
            if "already exists" in str(e).lower():
                logger.info(f"Relationship property {rel_type}.{prop} already exists (race)")
                continue
            raise

def run_schema_preflight() -> None:
    global _PREFLIGHT_RAN, _PREFLIGHT_DB_KEY
    current_db_key = str(_get_data_root().resolve())
    force = os.getenv("SCHEMA_PREFLIGHT_FORCE", "false").lower() in ("1","true","yes")

    if _PREFLIGHT_RAN and not force:
        if _PREFLIGHT_DB_KEY and _PREFLIGHT_DB_KEY != current_db_key:
            logger.info(
                "Schema preflight: database path changed (was %s, now %s); rerunning to ensure columns are present",
                _PREFLIGHT_DB_KEY,
                current_db_key,
            )
            _PREFLIGHT_RAN = False  # allow rerun in this invocation
        else:
            logger.debug("Schema preflight already executed in this process; skipping duplicate run")
            return

    if os.getenv("DISABLE_SCHEMA_PREFLIGHT", "false").lower() in ("1", "true", "yes"):
        logger.info("Schema preflight disabled via DISABLE_SCHEMA_PREFLIGHT (early exit)")
        _PREFLIGHT_RAN = True
        _PREFLIGHT_DB_KEY = current_db_key
        return

    marker = _load_marker()

    # Load schema early to compare hashes
    try:
        _load_master_schema()
    except Exception:
        raise

    if not force and marker.get("sha256") == _SCHEMA_META.get("sha256"):
        short_hash = _SCHEMA_META.get("sha256", "")[:12]
        ver = _SCHEMA_META.get("version")
        logger.info(
            f"Schema preflight skip: up-to-date (version={ver}, sha256={short_hash}…) — set SCHEMA_PREFLIGHT_FORCE=1 to force"
        )
        _PREFLIGHT_RAN = True
        _PREFLIGHT_DB_KEY = current_db_key
        return

    # Cross-process lock: only one process performs modifications
    got_lock = _acquire_lock()
    if not got_lock:
        # Another process likely handled it; if after waiting marker still mismatched we log a warning
        marker_after = _load_marker()
        if marker_after.get("sha256") != _SCHEMA_META.get("sha256"):
            logger.warning("Schema preflight lock timeout and marker hash mismatch – potential race; proceeding without changes")
        _PREFLIGHT_RAN = True
        _PREFLIGHT_DB_KEY = current_db_key
        return
    logger.info("🔍 Running schema preflight check for additive node + relationship upgrades...")
    # Version/hash logging (schema already loaded)
    meta_bits = []
    if "version" in _SCHEMA_META:
        meta_bits.append(f"version={_SCHEMA_META['version']}")
    if "sha256" in _SCHEMA_META:
        meta_bits.append(f"sha256={_SCHEMA_META['sha256'][:12]}…")
    if "path" in _SCHEMA_META:
        meta_bits.append(f"file={_SCHEMA_META['path']}")
    if meta_bits:
        logger.info("Master schema: " + ", ".join(meta_bits))
    manager = get_safe_kuzu_manager()
    with manager.get_connection(operation="schema_preflight") as conn:
        process_nodes = os.getenv("PREFLIGHT_REL_ONLY", "false").lower() not in ("1","true","yes")
        process_rels = os.getenv("PREFLIGHT_NODES_ONLY", "false").lower() not in ("1","true","yes")

        missing_node_cols: List[Tuple[str, str, str]] = []
        create_rel: List[str] = []
        alter_rel_props: List[Tuple[str, str, str]] = []

        if process_nodes:
            missing_node_cols = _detect_missing_columns(conn)
        if process_rels:
            create_rel, alter_rel_props = _detect_relationship_changes(conn)

        # Always evaluate migration runner (dry run) to see if additional additive changes needed
        mig_preview = run_additive_migrations(dry_run=True)
        raw_missing = mig_preview.get("missing", []) if mig_preview.get("status") == "pending" else []
        pending_migration_cols: List[str] = list(raw_missing) if isinstance(raw_missing, (list, tuple, set)) else []

        if not (missing_node_cols or create_rel or alter_rel_props or pending_migration_cols):
            logger.info("✅ No additive schema changes required.")
            return

        # Summaries
        if missing_node_cols:
            logger.info(
                f"Node columns to add ({len(missing_node_cols)}): " + ", ".join(f"{t}.{c}" for t, c, _ in missing_node_cols)
            )
        if create_rel:
            logger.info(f"Relationship tables to create ({len(create_rel)}): {', '.join(create_rel)}")
        if alter_rel_props:
            logger.info(
                f"Relationship properties to add ({len(alter_rel_props)}): " +
                ", ".join(f"{r}.{p}" for r, p, _ in alter_rel_props)
            )
        if pending_migration_cols:
            logger.info(
                f"Migration runner columns to add ({len(pending_migration_cols)}): " + ", ".join(f"Book.{c}" for c in pending_migration_cols)
            )

        if os.getenv("SKIP_PREFLIGHT_BACKUP", "false").lower() not in ("1", "true", "yes"):
            _create_backup_if_needed("pre_schema_upgrade")
        else:
            logger.info("Skipping automatic pre-upgrade backup per SKIP_PREFLIGHT_BACKUP")

        try:
            if missing_node_cols:
                _apply_alter_statements(conn, missing_node_cols)
            if create_rel:
                _apply_relationship_creates(conn, create_rel)
            if alter_rel_props:
                _apply_relationship_alters(conn, alter_rel_props)
            if pending_migration_cols:
                # Execute real migration now
                run_additive_migrations(dry_run=False)
            logger.info("✅ Schema preflight upgrade complete")
        except Exception as e:
            logger.error(f"❌ Schema preflight failed: {e}")
            raise
    logger.info("Schema preflight finished at %s", datetime.utcnow().isoformat())
    _PREFLIGHT_RAN = True
    _PREFLIGHT_DB_KEY = current_db_key
    _write_marker(_SCHEMA_META)
    if got_lock:
        _release_lock()

if os.getenv("SCHEMA_PREFLIGHT_AUTORUN", "1").lower() in ("1", "true", "yes"):
    try:  # execute on import
        run_schema_preflight()
    except Exception:
        raise
