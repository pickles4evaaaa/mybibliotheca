"""Simple additive migration runner.

Provides a minimal API expected by tests: `run_pending()` which returns a dict
with at least a `status` key. Current responsibility: ensure newer audiobook
related fields exist on `Book` nodes (idempotent).

We keep this separate from the startup schema preflight so tests can invoke it
in isolation with a fresh temporary database path.
"""
from __future__ import annotations

from typing import Dict, List, Tuple
import logging

from app.utils.safe_kuzu_manager import get_safe_kuzu_manager

logger = logging.getLogger(__name__)

# Columns the tests (and upcoming features) expect to exist on Book.
_BOOK_COLUMNS: List[Tuple[str, str]] = [
    ("audiobookshelf_id", "STRING"),      # external source id
    ("audio_duration_ms", "INT64"),       # precise duration in ms
    ("media_type", "STRING"),             # print/ebook/audiobook/etc (legacy naming)
    ("audiobookshelf_updated_at", "STRING"),  # last seen ABS updatedAt (ISO string)
    ("opds_source_id", "STRING"),         # OPDS stable identifier for deduplication
]


def _column_exists(conn, table: str, column: str) -> bool:
    try:
        conn.execute(f"MATCH (n:{table}) RETURN n.{column} LIMIT 1")
        return True
    except Exception as e:  # broad - probing only
        msg = str(e)
        if "Cannot find property" in msg and column in msg:
            return False
        if "does not exist" in msg.lower():
            return False
        return True  # ambiguous -> assume exists to stay safe


def _detect_missing_book_columns(conn) -> List[Tuple[str, str]]:
    missing = []
    for col, typ in _BOOK_COLUMNS:
        if not _column_exists(conn, "Book", col):
            missing.append((col, typ))
    return missing


def _ensure_book_columns(dry_run: bool = False) -> Dict[str, List[str]]:
    manager = get_safe_kuzu_manager()
    added: List[str] = []
    with manager.get_connection(operation="migrations_runner") as conn:
        missing = _detect_missing_book_columns(conn)
        if dry_run:
            return {"added": added, "missing": [c for c,_ in missing]}
        for col, typ in missing:
            try:
                logger.info(f"Migration: adding Book.{col} {typ}")
                conn.execute(f"ALTER TABLE Book ADD {col} {typ}")
                added.append(col)
            except Exception as e:
                if "already exists" in str(e).lower():
                    logger.debug(f"Column Book.{col} already exists (race)")
                else:
                    raise
    return {"added": added}


def run_pending(*, dry_run: bool = False) -> Dict[str, object]:  # API expected by tests
    """Run (or simulate) idempotent additive migrations.

    Args:
        dry_run: if True, only report pending changes without applying.
    Returns:
        dict containing:
            status: 'applied', 'no-op', or 'pending' (pending only in dry_run)
            added: list of columns actually added (empty in dry_run)
            missing: (dry_run only) list of columns that would be added
    """
    # Run standard book columns migration
    details = _ensure_book_columns(dry_run=dry_run)
    
    # Also run quantity field migration
    try:
        from app.migrations.quantity_field_migration import run_quantity_migration
        quantity_result = run_quantity_migration(dry_run=dry_run)
        
        # Merge results
        if dry_run:
            # Add quantity column to missing if needed
            if quantity_result.get("column_added"):
                existing_missing = details.get("missing", [])
                if "quantity" not in existing_missing:
                    existing_missing.append("quantity")
                details["missing"] = existing_missing
        else:
            # Add quantity to added columns if it was added
            if quantity_result.get("column_added"):
                details["added"].append("quantity")
        
        # Log quantity migration results
        if quantity_result.get("rows_updated", 0) > 0:
            logger.info(f"Quantity migration: updated {quantity_result['rows_updated']} books")
    except Exception as e:
        logger.error(f"Quantity migration failed: {e}")
        # Don't fail the entire migration if quantity migration fails
    
    if dry_run:
        status = "pending" if details.get("missing") else "no-op"
        return {"status": status, **details}
    status = "no-op" if not details["added"] else "applied"
    return {"status": status, **details}


__all__ = ["run_pending"]
