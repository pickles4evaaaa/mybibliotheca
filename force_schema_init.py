#!/usr/bin/env python3
"""
Force KuzuDB schema initialization.

This script forces a complete schema initialization to fix missing table issues.
"""

import os
import sys
import logging
from typing import Any

# Add the app directory to the Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'app'))

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def _safe_get_row_value(row: Any, index: int) -> Any:
    """Safely extract a value from a KuzuDB row at the given index."""
    if isinstance(row, list):
        return row[index] if index < len(row) else None
    elif isinstance(row, dict):
        keys = list(row.keys())
        return row[keys[index]] if index < len(keys) else None
    else:
        try:
            return row[index]  # type: ignore
        except (IndexError, KeyError, TypeError):
            return None

def force_schema_init():
    """Force schema initialization."""
    try:
        logger.info("🔧 Starting forced schema initialization...")
        
        # Run additive schema preflight before any checks so new columns are added
        try:
            logger.info("Running schema preflight (additive upgrade)...")
            # Importing runs preflight as a side-effect; force run explicitly too
            from app.startup.schema_preflight import run_schema_preflight  # type: ignore
            # Allow forcing even if marker says up-to-date
            os.environ['SCHEMA_PREFLIGHT_FORCE'] = 'true'
            run_schema_preflight()
        except Exception as e:
            logger.warning(f"Schema preflight phase failed or skipped: {e}")

        # Use thread-safe connection instead of deprecated singleton
        from app.utils.safe_kuzu_manager import SafeKuzuManager
        manager = SafeKuzuManager()
        
        # Force reset by setting environment variable
        os.environ['KUZU_FORCE_RESET'] = 'true'
        
        # Connect and initialize schema using safe connection
        logger.info("Connecting to database...")
        with manager.get_connection(operation="schema_init") as connection:
            
            # Test that tables exist
            logger.info("Testing schema...")
            
            # Test User table
            try:
                result = connection.execute("MATCH (u:User) RETURN COUNT(u) as count LIMIT 1")
                # Handle both single QueryResult and list[QueryResult]
                if isinstance(result, list):
                    result = result[0] if result else None
                if result and result.has_next():
                    count = _safe_get_row_value(result.get_next(), 0)
                    logger.info(f"✅ User table exists with {count} users")
                else:
                    logger.info("✅ User table exists (empty)")
            except Exception as e:
                logger.error(f"❌ User table test failed: {e}")
                
            # Test Book table
            try:
                result = connection.execute("MATCH (b:Book) RETURN COUNT(b) as count LIMIT 1")
                # Handle both single QueryResult and list[QueryResult]
                if isinstance(result, list):
                    result = result[0] if result else None
                if result and result.has_next():
                    count = _safe_get_row_value(result.get_next(), 0)
                    logger.info(f"✅ Book table exists with {count} books")
                else:
                    logger.info("✅ Book table exists (empty)")
            except Exception as e:
                logger.error(f"❌ Book table test failed: {e}")
                
            logger.info("✅ Schema initialization completed successfully!")
            
        # Connection automatically cleaned up by context manager
        
    except Exception as e:
        logger.error(f"❌ Schema initialization failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    force_schema_init()
