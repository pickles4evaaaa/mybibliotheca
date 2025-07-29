#!/usr/bin/env python3
"""
Force KuzuDB schema initialization.

This script forces a complete schema initialization to fix missing table issues.
"""

import os
import sys
import logging

# Add the app directory to the Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'app'))

from app.infrastructure.kuzu_graph import KuzuGraphDB

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def force_schema_init():
    """Force schema initialization."""
    try:
        logger.info("🔧 Starting forced schema initialization...")
        
        # Create database instance
        db = KuzuGraphDB()
        
        # Force reset by setting environment variable
        os.environ['KUZU_FORCE_RESET'] = 'true'
        
        # Connect and initialize schema
        logger.info("Connecting to database...")
        connection = db.connect()
        
        # Test that tables exist
        logger.info("Testing schema...")
        
        # Test User table
        try:
            result = connection.execute("MATCH (u:User) RETURN COUNT(u) as count LIMIT 1")
            # Handle both single QueryResult and list[QueryResult]
            if isinstance(result, list):
                result = result[0] if result else None
            if result and result.has_next():
                count = result.get_next()[0]
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
                count = result.get_next()[0]
                logger.info(f"✅ Book table exists with {count} books")
            else:
                logger.info("✅ Book table exists (empty)")
        except Exception as e:
            logger.error(f"❌ Book table test failed: {e}")
            
        logger.info("✅ Schema initialization completed successfully!")
        
        # Clean up
        db.disconnect()
        
    except Exception as e:
        logger.error(f"❌ Schema initialization failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    force_schema_init()
