#!/usr/bin/env python3
"""
Script to migrate existing books to include location data in user-book relationships.
"""

import sys
import os

# Add the app directory to the Python path
sys.path.append(os.path.join(os.path.dirname(__file__), 'app'))

from app.infrastructure.kuzu_graph import get_graph_storage
from app.location_service import LocationService

def main():
    """Run the migration for all users."""
    print("🔧 Starting location migration...")
    
    # Get all users
    storage = get_graph_storage()
    query = "MATCH (u:User) RETURN u.id as user_id, u.username as username"
    results = storage.query(query, {})
    
    users = []
    for result in results:
        user_id = None
        username = None
        
        # Handle different result key formats
        if 'col_0' in result:
            user_id = result['col_0']
        elif 'user_id' in result:
            user_id = result['user_id']
            
        if 'col_1' in result:
            username = result['col_1']
        elif 'username' in result:
            username = result['username']
            
        if user_id:
            users.append((user_id, username))
    
    print(f"🔧 Found {len(users)} users to migrate")
    
    # Initialize location service
    # We need to get the kuzu connection object that LocationService expects
    from app.kuzu_services import KuzuBookService
    kuzu_service = KuzuBookService()
    location_service = LocationService(kuzu_service.graph_storage.kuzu_conn)
    
    total_migrated = 0
    
    for user_id, username in users:
        print(f"\n🔧 Migrating user: {username} (ID: {user_id})")
        
        try:
            # Check if user has any locations first
            locations = location_service.get_user_locations(user_id)
            if not locations:
                print(f"  ⚠️  User {username} has no locations, skipping migration")
                continue
                
            # Run migration for this user
            migrated_count = location_service.migrate_existing_books_to_default_location(user_id)
            total_migrated += migrated_count
            
            print(f"  ✅ Migrated {migrated_count} books for user {username}")
            
        except Exception as e:
            print(f"  ❌ Error migrating user {username}: {e}")
            import traceback
            traceback.print_exc()
            continue
    
    print(f"\n🎉 Migration complete! Total books migrated: {total_migrated}")

if __name__ == "__main__":
    main()
