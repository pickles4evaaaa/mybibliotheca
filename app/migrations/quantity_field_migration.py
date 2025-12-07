"""Migration to add quantity field to Book nodes and set default value.

This migration:
1. Adds the quantity column to Book node table if it doesn't exist (handled by schema preflight)
2. Updates existing books to have quantity = 1 where it's NULL or 0
"""
from __future__ import annotations

import logging
from typing import Dict

from app.utils.safe_kuzu_manager import get_safe_kuzu_manager

logger = logging.getLogger(__name__)


def _column_exists(conn, table: str, column: str) -> bool:
    """Check if a column exists on a table."""
    try:
        conn.execute(f"MATCH (n:{table}) RETURN n.{column} LIMIT 1")
        return True
    except Exception as e:
        msg = str(e)
        if "Cannot find property" in msg and column in msg:
            return False
        if "does not exist" in msg.lower():
            return False
        return True


def run_quantity_migration(dry_run: bool = False) -> Dict[str, object]:
    """
    Ensure quantity field exists and has proper default values for existing books.
    
    Args:
        dry_run: if True, only report what would be done without applying changes.
        
    Returns:
        dict containing:
            status: 'applied', 'no-op', or 'pending' (pending only in dry_run)
            column_added: bool indicating if column was added
            rows_updated: int number of books updated with default quantity
    """
    manager = get_safe_kuzu_manager()
    column_added = False
    rows_updated = 0
    
    with manager.get_connection(operation="quantity_migration") as conn:
        # Step 1: Ensure column exists (schema preflight should handle this, but check)
        if not _column_exists(conn, "Book", "quantity"):
            if dry_run:
                logger.info("Migration (dry-run): Would add Book.quantity INT64")
                return {"status": "pending", "column_added": True, "rows_updated": 0}
            
            try:
                logger.info("Migration: adding Book.quantity INT64")
                conn.execute("ALTER TABLE Book ADD quantity INT64")
                column_added = True
            except Exception as e:
                if "already exists" in str(e).lower():
                    logger.debug("Column Book.quantity already exists (race)")
                else:
                    raise
        
        # Step 2: Update existing books that have NULL or 0 quantity to have quantity = 1
        if dry_run:
            # Count how many books would be updated
            try:
                result = conn.execute("""
                    MATCH (b:Book)
                    WHERE b.quantity IS NULL OR b.quantity = 0
                    RETURN COUNT(b) AS count
                """)
                if result.has_next():
                    count_row = result.get_next()
                    rows_to_update = count_row[0] if count_row else 0
                    logger.info(f"Migration (dry-run): Would update {rows_to_update} books to quantity = 1")
                    return {"status": "pending", "column_added": column_added, "rows_updated": rows_to_update}
            except Exception as e:
                logger.warning(f"Migration (dry-run): Could not count books to update: {e}")
                return {"status": "pending", "column_added": column_added, "rows_updated": 0}
        
        # Apply the update
        try:
            logger.info("Migration: setting default quantity = 1 for existing books")
            result = conn.execute("""
                MATCH (b:Book)
                WHERE b.quantity IS NULL OR b.quantity = 0
                SET b.quantity = 1
                RETURN COUNT(b) AS updated_count
            """)
            
            if result.has_next():
                count_row = result.get_next()
                rows_updated = count_row[0] if count_row else 0
                logger.info(f"Migration: updated {rows_updated} books with quantity = 1")
        except Exception as e:
            logger.error(f"Migration: failed to update book quantities: {e}")
            # Don't fail the migration if update fails - column is added, which is most important
            pass
    
    status = "no-op" if (not column_added and rows_updated == 0) else "applied"
    return {"status": status, "column_added": column_added, "rows_updated": rows_updated}


__all__ = ["run_quantity_migration"]
