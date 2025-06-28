#!/usr/bin/env python3
"""
Fix categories with null/None level fields
"""

import os
import sys
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from app.infrastructure.kuzu_graph import get_kuzu_database
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def fix_category_levels():
    """Fix categories that have NULL level values."""
    try:
        db = get_kuzu_database()
        
        # Query to find categories with NULL level
        query = """
        MATCH (c:Category)
        WHERE c.level IS NULL
        RETURN c.id as id, c.name as name
        """
        
        results = db.query(query)
        logger.info(f"Found {len(results)} categories with NULL level")
        
        for result in results:
            category_id = result.get('result') or result.get('col_0')
            if isinstance(result, dict) and 'col_0' in result:
                category_id = result['col_0']
            elif isinstance(result, dict) and 'id' in result:
                category_id = result['id']
                
            if category_id:
                # Update category to set level = 0 (root level)
                update_query = """
                MATCH (c:Category)
                WHERE c.id = $category_id
                SET c.level = 0, c.updated_at = $updated_at
                """
                
                from datetime import datetime
                db.query(update_query, {
                    'category_id': category_id,
                    'updated_at': datetime.utcnow()
                })
                logger.info(f"Fixed category {category_id} - set level to 0")
        
        logger.info("✅ Category level fix completed")
        
    except Exception as e:
        logger.error(f"❌ Error fixing category levels: {e}")
        raise

if __name__ == "__main__":
    fix_category_levels()
