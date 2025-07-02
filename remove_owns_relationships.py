#!/usr/bin/env python3
"""
Script to remove ALL OWNS relationships from the KuzuDB database.
This is needed to clean up the database before implementing the new global book system.
"""

import os
import sys

# Add the app directory to the Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from app import create_app
from app.infrastructure.kuzu_graph import get_kuzu_database

def remove_all_owns_relationships():
    """Remove ALL OWNS relationships from the database."""
    print("🚀 Starting removal of ALL OWNS relationships...")
    
    try:
        # Get database connection
        db = get_kuzu_database()
        
        # First, check how many OWNS relationships exist
        count_query = "MATCH ()-[r:OWNS]->() RETURN count(r) as count"
        count_result = db.query(count_query)
        
        if count_result and len(count_result) > 0:
            owns_count = count_result[0].get('col_0', 0)
            print(f"📊 Found {owns_count} OWNS relationships to remove")
        else:
            owns_count = 0
            print(f"📊 Found 0 OWNS relationships")
        
        if owns_count == 0:
            print("✅ No OWNS relationships found. Database is already clean.")
            return True
        
        # Remove ALL OWNS relationships
        print(f"🗑️ Removing {owns_count} OWNS relationships...")
        delete_query = "MATCH ()-[r:OWNS]->() DELETE r"
        db.query(delete_query)
        
        # Verify removal
        verify_query = "MATCH ()-[r:OWNS]->() RETURN count(r) as count"
        verify_result = db.query(verify_query)
        
        if verify_result and len(verify_result) > 0:
            remaining_count = verify_result[0].get('col_0', 0)
        else:
            remaining_count = 0
        
        if remaining_count == 0:
            print(f"✅ Successfully removed all {owns_count} OWNS relationships")
            print("✅ Database is now clean of OWNS relationships")
            return True
        else:
            print(f"❌ Failed to remove all OWNS relationships. {remaining_count} still remain.")
            return False
            
    except Exception as e:
        print(f"❌ Error removing OWNS relationships: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    """Main function to run the cleanup."""
    print("🔧 OWNS Relationships Cleanup Script")
    print("=" * 50)
    
    # Create Flask app context
    app = create_app()
    
    with app.app_context():
        success = remove_all_owns_relationships()
        
        if success:
            print("\n🎉 Cleanup completed successfully!")
            print("📝 The database now uses global book visibility without OWNS relationships.")
            return 0
        else:
            print("\n❌ Cleanup failed!")
            print("📝 Manual intervention may be required.")
            return 1

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
