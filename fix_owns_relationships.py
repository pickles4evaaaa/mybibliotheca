#!/usr/bin/env python3
"""
Script to fix OWNS relationships with mixed data types.
This will remove all existing OWNS relationships and recreate them with consistent types.
"""

import os
import sys
from datetime import datetime

# Add the app directory to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'app'))

from app.infrastructure.kuzu_graph import get_kuzu_database

def fix_owns_relationships():
    """Fix OWNS relationships by ensuring all properties have consistent data types."""
    
    db = get_kuzu_database()
    
    print("🔧 Starting OWNS relationship data type fix...")
    
    # First, get all existing OWNS relationships to preserve the data
    query_get_owns = """
    MATCH (u:User)-[r:OWNS]->(b:Book)
    RETURN u.id as user_id, b.id as book_id, r
    """
    
    try:
        results = db.query(query_get_owns)
        owns_relationships = []
        
        for result in results:
            if len(result) >= 3:
                user_id = result[0]  # col_0 = user_id
                book_id = result[1]  # col_1 = book_id  
                rel_props = dict(result[2]) if result[2] else {}  # col_2 = relationship properties
                
                # Normalize the properties to ensure consistent types
                normalized_props = {
                    'reading_status': str(rel_props.get('reading_status', 'plan_to_read')),
                    'ownership_status': str(rel_props.get('ownership_status', 'owned')),
                    'media_type': str(rel_props.get('media_type', 'physical')),
                    'date_added': rel_props.get('date_added', datetime.utcnow()),
                    'source': str(rel_props.get('source', 'manual')),
                    'notes': str(rel_props.get('notes', '')),
                    'location_id': str(rel_props.get('location_id', ''))
                }
                
                # Ensure date_added is a datetime object
                if not isinstance(normalized_props['date_added'], datetime):
                    try:
                        # Try to parse it if it's a string - use simple parsing
                        if isinstance(normalized_props['date_added'], str):
                            # Simple ISO format parsing
                            normalized_props['date_added'] = datetime.fromisoformat(normalized_props['date_added'].replace('Z', '+00:00'))
                        else:
                            normalized_props['date_added'] = datetime.utcnow()
                    except:
                        normalized_props['date_added'] = datetime.utcnow()
                
                owns_relationships.append({
                    'user_id': user_id,
                    'book_id': book_id,
                    'properties': normalized_props
                })
        
        print(f"📊 Found {len(owns_relationships)} existing OWNS relationships")
        
        # Delete all existing OWNS relationships
        query_delete_owns = """
        MATCH (u:User)-[r:OWNS]->(b:Book)
        DELETE r
        """
        
        print("🗑️ Deleting existing OWNS relationships...")
        db.query(query_delete_owns)
        print("✅ Deleted all existing OWNS relationships")
        
        # Recreate the relationships with consistent data types
        print("🔄 Recreating OWNS relationships with consistent data types...")
        recreated_count = 0
        
        for owns in owns_relationships:
            try:
                success = db.create_relationship(
                    'User', owns['user_id'], 
                    'OWNS', 
                    'Book', owns['book_id'], 
                    owns['properties']
                )
                
                if success:
                    recreated_count += 1
                else:
                    print(f"⚠️ Failed to recreate relationship: {owns['user_id']} -> {owns['book_id']}")
                    
            except Exception as e:
                print(f"❌ Error recreating relationship {owns['user_id']} -> {owns['book_id']}: {e}")
        
        print(f"✅ Successfully recreated {recreated_count}/{len(owns_relationships)} OWNS relationships")
        
        # Verify the fix worked
        print("🔍 Verifying fix...")
        verification_query = """
        MATCH (u:User)-[r:OWNS]->(b:Book)
        RETURN COUNT(r) as count
        """
        
        verification_result = db.query(verification_query)
        if verification_result and len(verification_result) > 0:
            final_count = verification_result[0][0]  # col_0 = count
            print(f"✅ Verification complete: {final_count} OWNS relationships now exist")
        
        return True
        
    except Exception as e:
        print(f"❌ Error during OWNS relationship fix: {e}")
        return False

if __name__ == "__main__":
    print("🚀 OWNS Relationship Data Type Fix Script")
    print("=" * 50)
    
    success = fix_owns_relationships()
    
    if success:
        print("\n🎉 OWNS relationship fix completed successfully!")
        print("You can now restart the application.")
    else:
        print("\n💥 OWNS relationship fix failed!")
        print("Check the error messages above for details.")
    
    print("=" * 50)
