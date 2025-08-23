"""OWNS Migration Service

Automatically migrates legacy OWNS relationship data into the new personal
metadata storage (HAS_PERSONAL_METADATA) on first access/run.

Steps:
 1. Detect if any OWNS relationships still exist (and not yet migrated)
 2. For each (u)-[o:OWNS]->(b) pull relevant per-user fields
 3. Upsert into personal metadata JSON (notes, review, start/finish, rating, status, etc.)
 4. Clear migrated fields from OWNS (optional) or leave until schema removal
 5. Set a migration marker node so we don't re-run.

Non-destructive: OWNS is not dropped here; separate maintenance task can drop it.
"""
from __future__ import annotations

import json
from datetime import datetime
from typing import Dict, Any
import logging

from ..infrastructure.kuzu_graph import safe_execute_kuzu_query
import os
from .personal_metadata_service import personal_metadata_service

logger = logging.getLogger(__name__)

MIGRATION_MARKER_KEY = "owns_personal_migration_completed"


def _migration_already_done() -> bool:
    try:
        q = """
        MATCH (s:SystemState {key: $key}) RETURN s.value
        """
        res = safe_execute_kuzu_query(q, {"key": MIGRATION_MARKER_KEY})
        if isinstance(res, list) and res:
            row = res[0]
            if isinstance(row, (list, tuple)) and row and row[0]:
                return True
            if isinstance(row, dict):
                return any(row.values())
        elif hasattr(res, 'has_next') and res.has_next():  # type: ignore[attr-defined]
            row = res.get_next()  # type: ignore[attr-defined]
            try:
                if isinstance(row, (list, tuple)) and row and row[0]:
                    return True
                if isinstance(row, dict) and any(row.values()):
                    return True
            except Exception:
                pass
    except Exception:
        pass
    return False


def _mark_done():
    try:
        q = """
        MERGE (s:SystemState {key: $key})
        SET s.value = 'true', s.updated_at = CURRENT_TIMESTAMP
        RETURN s.key
        """
        safe_execute_kuzu_query(q, {"key": MIGRATION_MARKER_KEY})
    except Exception as e:
        logger.warning(f"Failed to mark OWNS migration completion: {e}")


def migrate_owns_to_personal(limit: int | None = None) -> Dict[str, Any]:
    # Fast path: if OWNS schema disabled skip entirely
    if os.getenv('ENABLE_OWNS_SCHEMA', 'false').lower() not in ('1', 'true', 'yes'):
        return {"status": "skipped", "reason": "owns_schema_disabled"}
    if _migration_already_done():
        return {"status": "skipped", "reason": "already_completed"}

    # Detect any OWNS relationships
    count_q = "MATCH ()-[o:OWNS]->() RETURN COUNT(o)"
    try:
        count_res = safe_execute_kuzu_query(count_q)
        count = 0
        if isinstance(count_res, list) and count_res:
            first = count_res[0]
            if isinstance(first, (list, tuple)) and first:
                count = int(first[0])
            elif isinstance(first, dict) and first:
                count = int(list(first.values())[0])
        elif hasattr(count_res, 'has_next') and count_res.has_next():  # type: ignore[attr-defined]
            row = count_res.get_next()  # type: ignore[attr-defined]
            try:
                if isinstance(row, (list, tuple)) and row:
                    count = int(row[0])
                elif isinstance(row, dict) and row:
                    count = int(list(row.values())[0])
            except Exception:
                pass
    except Exception as e:
        logger.warning(f"Unable to count OWNS relationships: {e}")
        return {"status": "error", "error": str(e)}

    if count == 0:
        _mark_done()
        return {"status": "skipped", "reason": "no_owns_found"}

    fetch_q = """
    MATCH (u:User)-[o:OWNS]->(b:Book)
    RETURN u.id, b.id, o.personal_notes, o.user_review, o.start_date, o.finish_date,
           o.reading_status, o.ownership_status, o.user_rating, o.media_type, o.custom_metadata
    """
    if limit:
        fetch_q += f" LIMIT {int(limit)}"

    migrated = 0
    errors = 0
    try:
        res = safe_execute_kuzu_query(fetch_q)
        rows = []
        if isinstance(res, list):
            rows = res
        elif hasattr(res, 'has_next') and hasattr(res, 'get_next'):
            while res.has_next():  # type: ignore[attr-defined]
                rows.append(res.get_next())  # type: ignore[attr-defined]
        for row in rows:
            try:
                # Support dict or positional
                if isinstance(row, dict):
                    vals = [row.get(f'col_{i}') for i in range(11)]
                elif isinstance(row, (list, tuple)):
                    vals = list(row) + [None] * (11 - len(row))
                else:
                    continue
                user_id, book_id = vals[0], vals[1]
                if not isinstance(user_id, str) or not isinstance(book_id, str) or not user_id or not book_id:
                    continue
                personal_notes, user_review, start_date, finish_date, reading_status, ownership_status, user_rating, media_type, custom_metadata_raw = (
                    vals[2], vals[3], vals[4], vals[5], vals[6], vals[7], vals[8], vals[9], vals[10]
                )
                custom_updates = {}
                for k, v in [
                    ("reading_status", reading_status),
                    ("ownership_status", ownership_status),
                    ("user_rating", user_rating),
                    ("media_type", media_type),
                ]:
                    if v not in (None, ''):
                        custom_updates[k] = v
                # Merge legacy custom_metadata JSON
                if custom_metadata_raw and isinstance(custom_metadata_raw, str):
                    try:
                        legacy_meta = json.loads(custom_metadata_raw)
                        if isinstance(legacy_meta, dict):
                            for k, v in legacy_meta.items():
                                if v not in (None, ''):
                                    custom_updates[k] = v
                    except Exception:
                        pass
                # Dates go into custom updates unless handled specially
                if start_date:
                    custom_updates['start_date'] = start_date
                if finish_date:
                    custom_updates['finish_date'] = finish_date
                kwargs = {}
                if personal_notes:
                    kwargs['personal_notes'] = personal_notes
                if user_review:
                    kwargs['user_review'] = user_review
                if custom_updates:
                    kwargs['custom_updates'] = custom_updates
                personal_metadata_service.update_personal_metadata(user_id, book_id, **kwargs)
                migrated += 1
            except Exception as ie:
                errors += 1
                logger.warning(f"Failed to migrate OWNS row: {ie}")
    except Exception as e:
        logger.error(f"Error iterating OWNS relationships: {e}")
        return {"status": "error", "migrated": migrated, "errors": errors, "error": str(e)}

    _mark_done()
    return {"status": "completed", "migrated": migrated, "errors": errors}


def auto_run_migration_if_needed():
    try:
        result = migrate_owns_to_personal(limit=None)
        if result.get('status') == 'completed':
            logger.info(f"OWNS migration completed: {result}")
        else:
            # Use info level once OWNS fully deprecated to surface skip reason
            logger.info(f"OWNS migration skipped: {result}")
    except Exception as e:
        logger.warning(f"OWNS auto-migration failed: {e}")
