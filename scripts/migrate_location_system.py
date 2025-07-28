#!/usr/bin/env python3
"""
Migration script to transition from OWNS.location_id to STORED_AT relationships.

This script helps migrate the location system from storing location_id properties
on OWNS relationships to using proper STORED_AT relationships between books and locations.
"""

import sys
import os

# Add parent directory to path so we can import app modules
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.utils.safe_kuzu_manager import SafeKuzuManager
from app.location_service import LocationService
from app.services.kuzu_user_service import KuzuUserService
from app.debug_system import debug_log


def get_users_with_books():
    """Get all users that have books in the system."""
    safe_manager = SafeKuzuManager()
    
    query = """
    MATCH (u:User)-[:OWNS]->(b:Book)
    RETURN DISTINCT u.id as user_id, u.username as username, COUNT(b) as book_count
    ORDER BY username
    """
    
    results = safe_manager.execute_query(query, {})
    users = []
    
    for result in results:
        users.append({
            'user_id': result.get('user_id'),
            'username': result.get('username'), 
            'book_count': result.get('book_count')
        })
    
    return users


def migrate_user_locations(user_id: str, username: str):
    """Migrate one user's books to use STORED_AT relationships."""
    print(f"\n📚 Migrating user: {username} ({user_id})")
    
    safe_manager = SafeKuzuManager()
    location_service = LocationService(safe_manager)
    
    # Check if user has any locations
    locations = location_service.get_user_locations(user_id)
    if not locations:
        print(f"  ⚠️  User has no locations, setting up defaults...")
        locations = location_service.setup_default_locations()
    
    print(f"  📍 User has {len(locations)} locations")
    for loc in locations:
        print(f"    - {loc.name} (default: {loc.is_default})")
    
    # Find books with location_id in OWNS relationships
    old_system_query = """
    MATCH (u:User {id: $user_id})-[owns:OWNS]->(b:Book)
    WHERE owns.location_id IS NOT NULL
    RETURN b.id as book_id, owns.location_id as old_location_id
    """
    
    results = safe_manager.execute_query(old_system_query, {'user_id': user_id})
    
    migrated_count = 0
    old_system_books = []
    
    for result in results:
        book_id = result.get('book_id')
        old_location_id = result.get('old_location_id')
        if book_id and old_location_id:
            old_system_books.append((book_id, old_location_id))
    
    if old_system_books:
        print(f"  🔄 Found {len(old_system_books)} books with old location_id properties")
        
        for book_id, old_location_id in old_system_books:
            # Check if the old location still exists
            location = location_service.get_location(old_location_id)
            if location and location.user_id == user_id:
                # Migrate to new system
                if location_service.add_book_to_location(book_id, old_location_id, user_id):
                    print(f"    ✅ Migrated book {book_id[:8]}... to {location.name}")
                    migrated_count += 1
                else:
                    print(f"    ❌ Failed to migrate book {book_id[:8]}...")
            else:
                # Old location doesn't exist, use default
                default_location = location_service.get_default_location(user_id)
                if default_location and default_location.id:
                    if location_service.add_book_to_location(book_id, default_location.id, user_id):
                        print(f"    ✅ Migrated book {book_id[:8]}... to default location: {default_location.name}")
                        migrated_count += 1
                    else:
                        print(f"    ❌ Failed to migrate book {book_id[:8]}... to default location")
    
    # Now migrate books without any location
    unlocated_count = location_service.migrate_user_books_to_default_location(user_id)
    if unlocated_count > 0:
        print(f"  🏠 Migrated {unlocated_count} books without location to default location")
    
    total_migrated = migrated_count + unlocated_count
    print(f"  ✅ Total migrated: {total_migrated} books")
    
    return total_migrated


def clean_old_location_properties(user_id: str):
    """Remove old location_id properties from OWNS relationships after migration."""
    print(f"  🧹 Cleaning up old location_id properties...")
    
    safe_manager = SafeKuzuManager()
    
    # Remove location_id properties from OWNS relationships
    cleanup_query = """
    MATCH (u:User {id: $user_id})-[owns:OWNS]->(b:Book)
    WHERE owns.location_id IS NOT NULL
    SET owns.location_id = NULL
    """
    
    try:
        safe_manager.execute_query(cleanup_query, {'user_id': user_id})
        print(f"  ✅ Cleaned up old location properties")
    except Exception as e:
        print(f"  ⚠️  Warning: Could not clean up old properties: {e}")


def verify_migration(user_id: str, username: str):
    """Verify the migration was successful."""
    print(f"  🔍 Verifying migration for {username}...")
    
    safe_manager = SafeKuzuManager()
    location_service = LocationService(safe_manager)
    
    # Count books in new system
    locations = location_service.get_user_locations(user_id)
    total_books_new_system = 0
    
    for location in locations:
        if location.id:
            count = location_service.get_location_book_count(location.id, user_id)
            total_books_new_system += count
            print(f"    📍 {location.name}: {count} books")
    
    # Count total books owned by user
    total_books_query = """
    MATCH (u:User {id: $user_id})-[:OWNS]->(b:Book)
    RETURN COUNT(b) as total_books
    """
    
    results = safe_manager.execute_query(total_books_query, {'user_id': user_id})
    total_books = 0
    if results:
        total_books = results[0].get('total_books', 0)
    
    print(f"  📊 Books in new location system: {total_books_new_system}")
    print(f"  📊 Total books owned: {total_books}")
    
    if total_books_new_system == total_books:
        print(f"  ✅ Migration verification passed!")
        return True
    else:
        print(f"  ⚠️  Migration verification failed - some books may not have locations")
        return False


def main():
    """Main migration function."""
    print("🚀 Starting location system migration...")
    print("This will migrate from OWNS.location_id properties to STORED_AT relationships")
    print()
    
    # Get all users with books
    users = get_users_with_books()
    
    if not users:
        print("📭 No users with books found. Nothing to migrate.")
        return
    
    print(f"👥 Found {len(users)} users with books:")
    for user in users:
        print(f"  - {user['username']}: {user['book_count']} books")
    
    print()
    response = input("🤔 Continue with migration? (y/N): ").strip().lower()
    if response not in ['y', 'yes']:
        print("❌ Migration cancelled.")
        return
    
    total_migrated = 0
    successful_users = 0
    
    for user in users:
        try:
            migrated = migrate_user_locations(user['user_id'], user['username'])
            total_migrated += migrated
            
            # Verify migration
            if verify_migration(user['user_id'], user['username']):
                successful_users += 1
                
                # Clean up old properties if migration was successful
                print("  🤔 Clean up old location_id properties? (recommended)")
                cleanup_response = input("    (y/N): ").strip().lower()
                if cleanup_response in ['y', 'yes']:
                    clean_old_location_properties(user['user_id'])
            
        except Exception as e:
            print(f"  ❌ Error migrating user {user['username']}: {e}")
            import traceback
            traceback.print_exc()
    
    print()
    print("🎉 Migration completed!")
    print(f"  👥 Successfully migrated: {successful_users}/{len(users)} users")
    print(f"  📚 Total books migrated: {total_migrated}")
    print()
    print("📋 Next steps:")
    print("  1. Test the location system in the web interface")
    print("  2. Verify books appear in the correct locations")
    print("  3. Check that location assignments work properly")


if __name__ == "__main__":
    main()
