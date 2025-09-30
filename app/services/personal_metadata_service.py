"""Personal Metadata Service

Provides per-user, per-book metadata storage without relying on legacy OWNS relationship.

Fields supported (stored either as direct columns where available or inside JSON blob):
 - personal_notes (column if HAS_PERSONAL_METADATA schema created it; else in JSON)
 - user_review
 - start_date (ISO timestamp)
 - finish_date (ISO timestamp)
 - custom personal key/values (private by default)

Sharing model:
 - Field definitions may declare is_shareable / is_global (leverages existing CustomField schema)
 - A helper method can promote a personal field value to global metadata when requested
"""
from __future__ import annotations

from __future__ import annotations
import json
from datetime import datetime, timezone
from typing import Any, Dict, Optional
from pathlib import Path
import shutil

from ..infrastructure.kuzu_graph import safe_execute_kuzu_query
import os
import logging

logger = logging.getLogger(__name__)

# TODO(OWNS-REMOVAL): Reading log related tests exit with code 5 (startup/shutdown issue).
# Investigate unhandled exception during app lifecycle after OWNS migration once core removal complete.


def _now() -> datetime:
    return datetime.now(timezone.utc)


import threading
from contextlib import contextmanager
import hashlib

def _lock_dir() -> Path:
    base = Path(os.getenv('KUZU_DB_PATH', 'data/kuzu')).resolve()
    d = base.parent / 'locks'
    d.mkdir(parents=True, exist_ok=True)
    return d


_pm_locks: Dict[tuple, threading.Lock] = {}


@contextmanager
def _with_pm_lock(user_id: str, book_id: str):
    key = (user_id, book_id)
    lock = _pm_locks.get(key)
    if lock is None:
        lock = threading.Lock()
        _pm_locks[key] = lock
    # Cross-process file lock (best-effort) to reduce write conflicts between workers
    # File name derived from hash of (user, book)
    h = hashlib.sha1(f"{user_id}:{book_id}".encode('utf-8')).hexdigest()
    fpath = _lock_dir() / f"pm_{h}.lock"
    import time
    lock.acquire()
    fh = None
    try:
        # Try to acquire an exclusive lock with retries
        for _ in range(3):
            try:
                fh = open(fpath, 'w')
                try:
                    # Use fcntl on Unix (macOS/Linux). On Windows this will fail silently and we fallback to in-proc lock only.
                    import fcntl  # type: ignore
                    fcntl.flock(fh.fileno(), fcntl.LOCK_EX)
                except Exception:
                    pass
                break
            except Exception:
                time.sleep(0.02)
        yield
    finally:
        try:
            if fh is not None:
                try:
                    import fcntl  # type: ignore
                    try:
                        fcntl.flock(fh.fileno(), fcntl.LOCK_UN)
                    except Exception:
                        pass
                except Exception:
                    pass
                try:
                    fh.close()
                except Exception:
                    pass
        finally:
            lock.release()


class PersonalMetadataService:
    """Service encapsulating access to HAS_PERSONAL_METADATA relationship."""

    REL_NAME = "HAS_PERSONAL_METADATA"
    _migration_checked: bool = False
    _rel_schema_ensured: bool = False

    def _ensure_relationship_schema(self) -> None:
        """Ensure the HAS_PERSONAL_METADATA relationship table exists.

        Tests exercise PersonalMetadataService directly (without touching the
        custom field service that normally creates this REL table). On a fresh
        ephemeral Kuzu database this results in Binder exception: table does
        not exist. We lazily create the relationship here to make the service
        self‑sufficient. Safe to run multiple times; Kuzu will raise an 'already
        exists' error which we swallow.
        """
        # If KUZU_DB_PATH changes between tests, we must re-check schema.
        current_path = os.getenv('KUZU_DB_PATH', 'data/kuzu')
        cached_path = getattr(self, '_rel_schema_path', None)
        if cached_path != current_path:
            # Reset ensured flag for new database location
            self._rel_schema_ensured = False
            self._rel_schema_path = current_path
        # Always probe even if previously ensured to avoid cross-database false positives
        # (fast COUNT probe is cheap); only short-circuit if we've already confirmed for this path.
        if self._rel_schema_ensured:
            return
        # First attempt a lightweight existence probe
        try:
            probe = f"MATCH ()-[r:{self.REL_NAME}]->() RETURN COUNT(r) LIMIT 1"
            safe_execute_kuzu_query(probe)
            self._rel_schema_ensured = True
            return
        except Exception as probe_err:  # Table probably missing
            err_l = str(probe_err).lower()
            missing_tokens = ["does not exist", "cannot find", "unknown table", "binder exception"]
            if not any(tok in err_l for tok in missing_tokens):
                # Some other error; log once but continue (maybe transient)
                if not getattr(self, '_rel_schema_error_logged', False):
                    logger.debug(f"Probe for {self.REL_NAME} existence failed (non-missing error): {probe_err}")
                    self._rel_schema_error_logged = True
            # Proceed to creation attempt regardless (worst case it'll say already exists)
        create_rel_canonical = f"""
CREATE REL TABLE {self.REL_NAME}(
    FROM User TO Book,
    personal_notes STRING,
    start_date TIMESTAMP,
    finish_date TIMESTAMP,
    personal_custom_fields STRING,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
)
"""
        try:
            safe_execute_kuzu_query(create_rel_canonical)
            self._rel_schema_ensured = True
            logger.info(f"Created {self.REL_NAME} relationship table (lazy path)")
        except Exception as e:  # pragma: no cover - defensive
            msg = str(e).lower()
            if 'already exists' in msg:
                self._rel_schema_ensured = True
            else:
                if not getattr(self, '_rel_schema_error_logged', False):
                    logger.warning(f"Lazy creation of {self.REL_NAME} failed: {e}")
                    self._rel_schema_error_logged = True

    def _maybe_run_owns_migration(self):
        """Detect legacy OWNS relationships and migrate per-user data into HAS_PERSONAL_METADATA.

        Idempotent: runs only once per process. Does not delete OWNS yet (so legacy code still functions)
        but copies personal fields so new reads pick them up even if OWNS removal happens later.
        Set DISABLE_OWNS_MIGRATION=1 to skip.
        """
        if self._migration_checked:
            return
        self._migration_checked = True
        # Skip entirely if OWNS schema not enabled (avoids binder errors when rel table absent)
        if os.getenv('ENABLE_OWNS_SCHEMA', 'false').lower() not in ('1', 'true', 'yes'):
            return
        if os.getenv("DISABLE_OWNS_MIGRATION", "0") in ("1", "true", "True"):
            logger.info("OWNS migration skipped due to DISABLE_OWNS_MIGRATION env var")
            return
        
        # Check for completion flag to avoid re-running migration
        kuzu_dir = Path(os.getenv('KUZU_DB_PATH', 'data/kuzu')).resolve()
        migration_complete_flag = kuzu_dir / '.owns_migration_complete'
        if migration_complete_flag.exists():
            logger.debug("OWNS migration already completed (flag file exists), skipping")
            return
        try:
            count_result = safe_execute_kuzu_query("MATCH ()-[r:OWNS]->() RETURN COUNT(r)")
            count = 0
            if hasattr(count_result, 'has_next') and hasattr(count_result, 'get_next'):
                if count_result.has_next():  # type: ignore[attr-defined]
                    row = count_result.get_next()  # type: ignore[attr-defined]
                    if isinstance(row, (list, tuple)) and row:
                        count = row[0]
            elif isinstance(count_result, list) and count_result:
                first = count_result[0]
                if isinstance(first, dict):
                    # common key patterns
                    count = first.get('col_0') or first.get('count') or 0
                elif isinstance(first, (list, tuple)):
                    count = first[0]
            count = int(count or 0)
            if count == 0:
                return
            # Before performing any data copy/mutation, take a filesystem snapshot of the Kuzu DB
            self._ensure_pre_owns_migration_backup(count)
            logger.info(f"Starting OWNS -> HAS_PERSONAL_METADATA migration for {count} relationships")
            query = """
            MATCH (u:User)-[r:OWNS]->(b:Book)
            RETURN u.id, b.id, r.personal_notes, r.user_review, r.start_date, r.finish_date,
                   r.reading_status, r.ownership_status, r.user_rating, r.custom_metadata
            """
            result = safe_execute_kuzu_query(query)
            rows = []
            if result is None:
                return
            if isinstance(result, list):
                rows = result
            elif hasattr(result, 'has_next') and hasattr(result, 'get_next'):
                while result.has_next():  # type: ignore[attr-defined]
                    rows.append(result.get_next())  # type: ignore[attr-defined]
            migrated = 0
            for row in rows:
                # Support dict or list style
                if isinstance(row, dict):
                    row_dict: Dict[str, Any] = row  # local alias for type clarity
                    def getv(i: int, key: str):
                        col_key = f'col_{i}'
                        if col_key in row_dict:
                            return row_dict.get(col_key)
                        return row_dict.get(key)
                    user_id = getv(0, 'u.id') or getv(0, 'user_id')
                    book_id = getv(1, 'b.id') or getv(1, 'book_id')
                    if not user_id or not book_id:
                        continue
                    personal_notes = getv(2, 'personal_notes')
                    user_review = getv(3, 'user_review')
                    start_date = getv(4, 'start_date')
                    finish_date = getv(5, 'finish_date')
                    reading_status = getv(6, 'reading_status')
                    ownership_status = getv(7, 'ownership_status')
                    user_rating = getv(8, 'user_rating')
                    custom_metadata_raw = getv(9, 'custom_metadata')
                else:
                    user_id = row[0] if len(row) > 0 else None  # type: ignore[index]
                    book_id = row[1] if len(row) > 1 else None  # type: ignore[index]
                    personal_notes = row[2] if len(row) > 2 else None  # type: ignore[index]
                    user_review = row[3] if len(row) > 3 else None  # type: ignore[index]
                    start_date = row[4] if len(row) > 4 else None  # type: ignore[index]
                    finish_date = row[5] if len(row) > 5 else None  # type: ignore[index]
                    reading_status = row[6] if len(row) > 6 else None  # type: ignore[index]
                    ownership_status = row[7] if len(row) > 7 else None  # type: ignore[index]
                    user_rating = row[8] if len(row) > 8 else None  # type: ignore[index]
                    custom_metadata_raw = row[9] if len(row) > 9 else None  # type: ignore[index]
                if not user_id or not book_id:
                    continue
                custom_updates: Dict[str, Any] = {}
                for k, v in {
                    'reading_status': reading_status,
                    'ownership_status': ownership_status,
                    'user_rating': user_rating,
                }.items():
                    if v not in (None, ''):
                        custom_updates[k] = v
                # merge custom_metadata JSON if present
                if custom_metadata_raw:
                    try:
                        parsed = json.loads(custom_metadata_raw)
                        if isinstance(parsed, dict):
                            for ck, cv in parsed.items():
                                if ck not in custom_updates:
                                    custom_updates[ck] = cv
                    except Exception:
                        pass
                # Write out personal metadata
                try:
                    self.update_personal_metadata(
                        user_id,
                        book_id,
                        personal_notes=personal_notes,
                        user_review=user_review,
                        custom_updates=custom_updates if custom_updates else None,
                        merge=True,
                    )
                    # start/finish dates (may be TIMESTAMP objects already or strings)
                    start_dt = None
                    finish_dt = None
                    if start_date:
                        if isinstance(start_date, datetime):
                            start_dt = start_date
                        else:
                            try:
                                start_dt = datetime.fromisoformat(str(start_date))
                            except Exception:
                                start_dt = None
                    if finish_date:
                        if isinstance(finish_date, datetime):
                            finish_dt = finish_date
                        else:
                            try:
                                finish_dt = datetime.fromisoformat(str(finish_date))
                            except Exception:
                                finish_dt = None
                    if start_dt or finish_dt:
                        self.update_personal_metadata(
                            user_id,
                            book_id,
                            start_date=start_dt,
                            finish_date=finish_dt,
                            merge=True,
                        )
                    migrated += 1
                except Exception as e:
                    logger.warning(f"Failed migrating OWNS record user={user_id} book={book_id}: {e}")
            logger.info(f"Completed OWNS migration: migrated {migrated} / {count}")
            
            # Set completion flag to avoid re-running migration
            try:
                kuzu_dir = Path(os.getenv('KUZU_DB_PATH', 'data/kuzu')).resolve()
                migration_complete_flag = kuzu_dir / '.owns_migration_complete'
                migration_complete_flag.touch()
                logger.info("OWNS migration completion flag set - future startups will skip migration")
            except Exception as flag_error:
                logger.warning(f"Failed to set OWNS migration completion flag: {flag_error}")
                
        except Exception as e:
            logger.warning(f"OWNS migration failed: {e}")
    def _ensure_pre_owns_migration_backup(self, owns_count: int):
        """Create a timestamped copy of the Kuzu database directory before OWNS migration.

        Safety requirements:
          - Never overwrite an existing backup directory
          - Only run once (sentinel file) unless FORCE_OWNS_MIGRATION_BACKUP=1
          - Skippable via DISABLE_OWNS_MIGRATION_BACKUP=1

        Args:
            owns_count: Number of OWNS relationships detected (for logging only)
        """
        try:
            if os.getenv('DISABLE_OWNS_MIGRATION_BACKUP', '0').lower() in ('1', 'true', 'yes'):
                logger.info("OWNS migration backup skipped due to DISABLE_OWNS_MIGRATION_BACKUP env var")
                return
            kuzu_dir = Path(os.getenv('KUZU_DB_PATH', 'data/kuzu')).resolve()
            if not kuzu_dir.exists():
                logger.warning(f"Kuzu directory not found for backup: {kuzu_dir}")
                return
            sentinel = kuzu_dir / '.owns_migration_backup_done'
            if sentinel.exists() and os.getenv('FORCE_OWNS_MIGRATION_BACKUP', '0') not in ('1', 'true', 'yes'):
                logger.info("OWNS migration backup already exists (sentinel present), skipping new backup")
                return
            backup_root = kuzu_dir.parent / 'kuzu_backups'
            backup_root.mkdir(parents=True, exist_ok=True)
            timestamp = datetime.utcnow().strftime('%Y%m%d-%H%M%S')
            backup_dir = backup_root / f'pre_owns_migration_{timestamp}'
            if backup_dir.exists():  # extremely unlikely
                logger.warning(f"Backup directory already exists, generating unique suffix: {backup_dir}")
                backup_dir = backup_root / f'pre_owns_migration_{timestamp}_{os.getpid()}'
            logger.info(f"Creating pre-OWNS-migration backup at {backup_dir} (relationships: {owns_count})")
            shutil.copytree(kuzu_dir, backup_dir)
            sentinel.write_text(str(backup_dir))
            logger.info(f"Pre-OWNS-migration backup completed: {backup_dir}")
        except Exception as e:
            logger.error(f"Failed to create OWNS migration backup (continuing anyway): {e}")

    def _fetch_relationship(self, user_id: str, book_id: str) -> Optional[Dict[str, Any]]:
        query_new = f"""
        MATCH (u:User {{id: $user_id}})-[r:{self.REL_NAME}]->(b:Book {{id: $book_id}})
        RETURN r.personal_notes, r.start_date, r.finish_date, r.personal_custom_fields, r.created_at, r.updated_at
        """
        query_legacy = f"""
        MATCH (u:User {{id: $user_id}})-[r:{self.REL_NAME}]->(b:Book {{id: $book_id}})
        RETURN r.personal_notes, r.personal_custom_fields, r.created_at, r.updated_at
        """
        try:
            result = safe_execute_kuzu_query(query_new, {"user_id": user_id, "book_id": book_id})
            rows = []
            # Newer SafeKuzuManager may already return list-like rows
            if result is None:
                return None
            if isinstance(result, list):
                rows = result
            elif hasattr(result, 'has_next') and hasattr(result, 'get_next'):
                while result.has_next():  # type: ignore[attr-defined]
                    rows.append(result.get_next())  # type: ignore[attr-defined]
            if not rows:
                return None
            row = rows[0]
            # Support dict style (col_0, col_1, etc.) or positional
            if isinstance(row, dict):
                personal_notes = row.get('col_0') or row.get('personal_notes')
                start_date = row.get('col_1') or row.get('start_date')
                finish_date = row.get('col_2') or row.get('finish_date')
                custom_fields = row.get('col_3') or row.get('personal_custom_fields')
                created_at = row.get('col_4') or row.get('created_at')
                updated_at = row.get('col_5') or row.get('updated_at')
            else:
                personal_notes = row[0] if len(row) > 0 else None  # type: ignore[index]
                start_date = row[1] if len(row) > 1 else None  # type: ignore[index]
                finish_date = row[2] if len(row) > 2 else None  # type: ignore[index]
                custom_fields = row[3] if len(row) > 3 else None  # type: ignore[index]
                created_at = row[4] if len(row) > 4 else None  # type: ignore[index]
                updated_at = row[5] if len(row) > 5 else None  # type: ignore[index]
            return {
                "personal_notes": personal_notes,
                "start_date": start_date,
                "finish_date": finish_date,
                "personal_custom_fields": custom_fields,
                "created_at": created_at,
                "updated_at": updated_at,
            }
        except Exception as e:
            # Fallback for older DB without start/finish columns
            err = str(e).lower()
            if "cannot find property" in err or "does not exist" in err or "unknown" in err:
                try:
                    result = safe_execute_kuzu_query(query_legacy, {"user_id": user_id, "book_id": book_id})
                    rows = []
                    if result is None:
                        return None
                    if isinstance(result, list):
                        rows = result
                    elif hasattr(result, 'has_next') and hasattr(result, 'get_next'):
                        while result.has_next():  # type: ignore[attr-defined]
                            rows.append(result.get_next())  # type: ignore[attr-defined]
                    if not rows:
                        return None
                    row = rows[0]
                    # Safe path: treat result row as dict-like mapping (our SafeKuzuManager normalizes to dicts)
                    if not isinstance(row, dict):
                        row = {"col_0": None, "col_1": None, "col_2": None, "col_3": None}
                    personal_notes = row.get('col_0') or row.get('personal_notes')
                    custom_fields = row.get('col_1') or row.get('personal_custom_fields')
                    created_at = row.get('col_2') or row.get('created_at')
                    updated_at = row.get('col_3') or row.get('updated_at')
                    return {
                        "personal_notes": personal_notes,
                        "start_date": None,
                        "finish_date": None,
                        "personal_custom_fields": custom_fields,
                        "created_at": created_at,
                        "updated_at": updated_at,
                    }
                except Exception:
                    return None
            return None

    def get_personal_metadata(self, user_id: str, book_id: str) -> Dict[str, Any]:
        # Ensure migration attempted before reads
        self._ensure_relationship_schema()
        self._maybe_run_owns_migration()
        rel = self._fetch_relationship(user_id, book_id) or {}
        blob_raw = rel.get("personal_custom_fields")
        try:
            blob = json.loads(blob_raw) if blob_raw else {}
        except Exception:
            blob = {}
        # Normalize keys
        meta = {
            "personal_notes": rel.get("personal_notes") or blob.get("personal_notes"),
            "user_review": blob.get("user_review"),
            # Prefer first-class columns if present; fallback to JSON
            "start_date": rel.get("start_date") or blob.get("start_date"),
            "finish_date": rel.get("finish_date") or blob.get("finish_date"),
        }
        # Merge remaining custom keys (excluding ones we already hoisted)
        for k, v in blob.items():
            if k not in meta:
                meta[k] = v
        return meta

    def update_personal_metadata(
        self,
        user_id: str,
        book_id: str,
        *,
        personal_notes: Optional[str] = None,
        user_review: Optional[str] = None,
        start_date: Optional[datetime] = None,
        finish_date: Optional[datetime] = None,
        custom_updates: Optional[Dict[str, Any]] = None,
        merge: bool = True,
    ) -> Dict[str, Any]:
        """Create or update the HAS_PERSONAL_METADATA relationship.

        Args:
            merge: if True, load existing JSON and merge; else overwrite JSON.
        Returns updated metadata dictionary.
        """
        # Ensure migration attempted before writes
        self._ensure_relationship_schema()
        self._maybe_run_owns_migration()
        existing = self.get_personal_metadata(user_id, book_id) if merge else {}
        if personal_notes is not None:
            existing["personal_notes"] = personal_notes
        if user_review is not None:
            existing["user_review"] = user_review
        if start_date is not None:
            existing["start_date"] = start_date.isoformat()
        if finish_date is not None:
            existing["finish_date"] = finish_date.isoformat()
        if custom_updates:
            for k, v in custom_updates.items():
                existing[k] = v

        # Persist:
        # - personal_notes in column
        # - start_date/finish_date in dedicated columns when available
        # - keep full JSON blob for remaining/custom keys (also mirror dates into JSON for backward compatibility)
        json_blob = existing.copy()
        column_notes = json_blob.pop("personal_notes", None)
        # Pull out ISO strings for dates (if explicitly cleared, value may be None)
        start_raw = json_blob.get("start_date")
        finish_raw = json_blob.get("finish_date")
        
        def _sanitize_ts_param(v: Any) -> Optional[str]:
            """Return ISO string or None. Treat '', whitespace, and invalid formats as None."""
            if v is None:
                return None
            if isinstance(v, datetime):
                return v.isoformat()
            if isinstance(v, str):
                s = v.strip()
                if not s:
                    return None
                # Normalize Z suffix
                s2 = s.replace('Z', '+00:00')
                try:
                    return datetime.fromisoformat(s2).isoformat()
                except Exception:
                    # Try numeric epoch seconds or ms
                    try:
                        val = float(s2)
                        if val > 10_000_000_000:  # likely ms
                            val = val / 1000.0
                        return datetime.fromtimestamp(val, tz=timezone.utc).isoformat()
                    except Exception:
                        return None
            # Unsupported type
            return None

        start_iso = _sanitize_ts_param(start_raw)
        finish_iso = _sanitize_ts_param(finish_raw)
        # Normalize JSON blob to avoid persisting empty strings for dates
        if start_iso is None:
            json_blob.pop("start_date", None)
        else:
            json_blob["start_date"] = start_iso
        if finish_iso is None:
            json_blob.pop("finish_date", None)
        else:
            json_blob["finish_date"] = finish_iso
        # Ensure canonical types (convert datetimes to iso)
        for k, v in list(json_blob.items()):
            if isinstance(v, datetime):
                json_blob[k] = v.isoformat()
        # Cast ISO strings to TIMESTAMP explicitly; preserve NULLs
        query_new = f"""
        MATCH (u:User {{id: $user_id}}), (b:Book {{id: $book_id}})
        MERGE (u)-[r:{self.REL_NAME}]->(b)
        SET r.personal_notes = $personal_notes,
            r.start_date = CASE WHEN $start_date IS NULL OR $start_date = '' THEN NULL ELSE timestamp($start_date) END,
            r.finish_date = CASE WHEN $finish_date IS NULL OR $finish_date = '' THEN NULL ELSE timestamp($finish_date) END,
            r.personal_custom_fields = $json_blob
        RETURN r.personal_notes, r.start_date, r.finish_date, r.personal_custom_fields
        """
        query_legacy = f"""
        MATCH (u:User {{id: $user_id}}), (b:Book {{id: $book_id}})
        MERGE (u)-[r:{self.REL_NAME}]->(b)
        SET r.personal_notes = $personal_notes,
            r.personal_custom_fields = $json_blob
        RETURN r.personal_notes, r.personal_custom_fields
        """
        try:
            # Retry loop to mitigate transient write-write conflicts
            attempts = 0
            last_err: Optional[Exception] = None
            while attempts < 3:
                try:
                    with _with_pm_lock(user_id, book_id):
                        safe_execute_kuzu_query(
                            query_new,
                            {
                                "user_id": user_id,
                                "book_id": book_id,
                                "personal_notes": column_notes,
                                # Kuzu supports TIMESTAMP nulls; pass ISO if string, else None
                                "start_date": start_iso,
                                "finish_date": finish_iso,
                                "json_blob": json.dumps(json_blob) if json_blob else None,
                            },
                        )
                    last_err = None
                    break
                except Exception as _e:
                    msg = str(_e).lower()
                    # Detect write-write conflict signatures
                    if 'write-write conflict' in msg or 'deadlock' in msg:
                        attempts += 1
                        last_err = _e
                        import time
                        time.sleep(0.05 * attempts)  # simple backoff 50ms, 100ms
                        continue
                    else:
                        raise
            if last_err is not None:
                # Exhausted retries; re-raise last error
                raise last_err
        except Exception as e:
            msg = str(e).lower()
            if "cannot find property" in msg or "does not exist" in msg or "unknown" in msg:
                # Try legacy path with same retry shim just in case
                attempts = 0
                last_err2: Optional[Exception] = None
                while attempts < 2:
                    try:
                        with _with_pm_lock(user_id, book_id):
                            safe_execute_kuzu_query(
                                query_legacy,
                                {
                                    "user_id": user_id,
                                    "book_id": book_id,
                                    "personal_notes": column_notes,
                                    "json_blob": json.dumps(json_blob) if json_blob else None,
                                },
                            )
                        last_err2 = None
                        break
                    except Exception as _e2:
                        if 'write-write conflict' in str(_e2).lower():
                            attempts += 1
                            last_err2 = _e2
                            import time
                            time.sleep(0.05 * attempts)
                            continue
                        else:
                            raise
                if last_err2 is not None:
                    raise last_err2
            else:
                raise
    # If the above failed due to missing table (race condition), attempt one retry
    # NOTE: SafeKuzuManager will have already logged the error; we just inspect logs via exception here
    # (We cannot capture exception because safe_execute_kuzu_query re-raises; so we wrap in try above if needed.)
        # Reconstruct final metadata (include notes)
        existing["personal_notes"] = column_notes
        return existing

    def ensure_start_date(self, user_id: str, book_id: str, start_date: datetime) -> None:
        meta = self.get_personal_metadata(user_id, book_id)
        if not meta.get("start_date"):
            self.update_personal_metadata(user_id, book_id, start_date=start_date)


personal_metadata_service = PersonalMetadataService()
