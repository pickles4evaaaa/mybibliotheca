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
                   r.reading_status, r.ownership_status, r.user_rating, r.media_type, r.custom_metadata
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
                    media_type = getv(9, 'media_type')
                    custom_metadata_raw = getv(10, 'custom_metadata')
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
                    media_type = row[9] if len(row) > 9 else None  # type: ignore[index]
                    custom_metadata_raw = row[10] if len(row) > 10 else None  # type: ignore[index]
                if not user_id or not book_id:
                    continue
                custom_updates: Dict[str, Any] = {}
                for k, v in {
                    'reading_status': reading_status,
                    'ownership_status': ownership_status,
                    'user_rating': user_rating,
                    'media_type': media_type,
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
        query = f"""
        MATCH (u:User {{id: $user_id}})-[r:{self.REL_NAME}]->(b:Book {{id: $book_id}})
        RETURN r.personal_notes, r.personal_custom_fields, r.created_at, r.updated_at
        """
        try:
            result = safe_execute_kuzu_query(query, {"user_id": user_id, "book_id": book_id})
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
                custom_fields = row.get('col_1') or row.get('personal_custom_fields')
                created_at = row.get('col_2') or row.get('created_at')
                updated_at = row.get('col_3') or row.get('updated_at')
            else:
                personal_notes = row[0] if len(row) > 0 else None  # type: ignore[index]
                custom_fields = row[1] if len(row) > 1 else None  # type: ignore[index]
                created_at = row[2] if len(row) > 2 else None  # type: ignore[index]
                updated_at = row[3] if len(row) > 3 else None  # type: ignore[index]
            return {
                "personal_notes": personal_notes,
                "personal_custom_fields": custom_fields,
                "created_at": created_at,
                "updated_at": updated_at,
            }
        except Exception:
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
            "start_date": blob.get("start_date"),
            "finish_date": blob.get("finish_date"),
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

        # Persist (personal_notes kept in column for quick access; rest in JSON)
        json_blob = existing.copy()
        column_notes = json_blob.pop("personal_notes", None)
        # Ensure canonical types (convert datetimes to iso)
        for k, v in list(json_blob.items()):
            if isinstance(v, datetime):
                json_blob[k] = v.isoformat()
        query = f"""
        MATCH (u:User {{id: $user_id}}), (b:Book {{id: $book_id}})
        MERGE (u)-[r:{self.REL_NAME}]->(b)
        SET r.personal_notes = $personal_notes,
            r.personal_custom_fields = $json_blob
        RETURN r.personal_notes, r.personal_custom_fields
        """
        safe_execute_kuzu_query(
            query,
            {
                "user_id": user_id,
                "book_id": book_id,
                "personal_notes": column_notes,
                "json_blob": json.dumps(json_blob) if json_blob else None,
            },
    )
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
