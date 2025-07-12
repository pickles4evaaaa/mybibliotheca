#!/usr/bin/env python3
"""
Custom Fields Migration Script

This script migrates custom metadata from the old OWNS-based architecture 
to the new global/personal architecture.

Usage:
    python migrate_custom_fields.py [--dry-run] [--cleanup]
    
Options:
    --dry-run   Show what would be migrated without making changes
    --cleanup   Clean up old CustomFieldValue nodes after migration
"""

import sys
import argparse
import json
from datetime import datetime

# Add the app directory to the path so we can import our modules
sys.path.append('/Users/jeremiah/Documents/Python Projects/bibliotheca')

def migrate_custom_fields(dry_run=False, cleanup=False):
    """Main migration function."""
    try:
        from app.services.kuzu_custom_field_service import KuzuCustomFieldService
        from app.infrastructure.kuzu_graph import get_kuzu_connection
        
        print(f"🚀 Starting custom fields migration...")
        print(f"   📊 Dry run: {'Yes' if dry_run else 'No'}")
        print(f"   🧹 Cleanup: {'Yes' if cleanup else 'No'}")
        print()
        
        # Initialize the service
        custom_field_service = KuzuCustomFieldService()
        
        if dry_run:
            # Analyze what would be migrated
            print("🔍 DRY RUN - Analyzing existing data...")
            
            # Find all OWNS relationships with custom metadata
            migration_query = """
            MATCH (u:User)-[r:OWNS]->(b:Book)
            WHERE r.custom_metadata IS NOT NULL AND r.custom_metadata <> ''
            RETURN u.id AS user_id, b.id AS book_id, r.custom_metadata AS custom_metadata
            LIMIT 10
            """
            
            results = custom_field_service.graph_storage.query(migration_query)
            
            if not results:
                print("✅ No OWNS relationships with custom metadata found")
                return True
            
            print(f"📊 Found {len(results)} OWNS relationships with custom metadata")
            print()
            
            # Analyze the metadata
            total_fields = 0
            field_usage = {}
            
            for result in results:
                user_id = result.get('col_0')
                book_id = result.get('col_1')
                metadata_json = result.get('col_2')
                
                if user_id and book_id and metadata_json:
                    try:
                        if isinstance(metadata_json, str):
                            metadata = json.loads(metadata_json)
                        else:
                            metadata = metadata_json
                        
                        user_short = user_id[:8] if len(user_id) > 8 else user_id
                        book_short = book_id[:8] if len(book_id) > 8 else book_id
                        print(f"📖 User {user_short}... Book {book_short}...: {len(metadata)} fields")
                        
                        for field_name, field_value in metadata.items():
                            field_usage[field_name] = field_usage.get(field_name, 0) + 1
                            total_fields += 1
                            
                    except (json.JSONDecodeError, TypeError) as e:
                        print(f"❌ Error parsing metadata for {user_id}/{book_id}: {e}")
            
            print()
            print(f"📊 Migration Summary:")
            print(f"   Total metadata entries: {total_fields}")
            print(f"   Unique field names: {len(field_usage)}")
            print()
            print("🏷️  Most used fields:")
            sorted_fields = sorted(field_usage.items(), key=lambda x: x[1], reverse=True)
            for field_name, count in sorted_fields[:10]:
                print(f"   {field_name}: {count} uses")
            
            print()
            print("💡 To perform the actual migration, run without --dry-run")
            return True
        
        else:
            # Perform the actual migration
            print("🔄 Performing migration...")
            
            # First, migrate all existing metadata
            success = custom_field_service.migrate_all_custom_metadata_from_owns()
            
            if not success:
                print("❌ Migration failed")
                return False
            
            print("✅ Migration completed successfully")
            
            if cleanup:
                print()
                print("🧹 Cleaning up old CustomFieldValue nodes...")
                cleanup_success = custom_field_service.cleanup_old_custom_field_nodes()
                
                if cleanup_success:
                    print("✅ Cleanup completed successfully")
                else:
                    print("❌ Cleanup failed")
                    return False
            
            print()
            print("🎉 Migration process complete!")
            print()
            print("📋 Next steps:")
            print("   1. Test the application to ensure custom fields work correctly")
            print("   2. Update any custom code that relies on the old OWNS metadata")
            print("   3. Consider running with --cleanup to remove old nodes")
            print("   4. Update UI to distinguish between global and personal fields")
            
            return True
            
    except Exception as e:
        print(f"❌ Migration error: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    """Command line interface."""
    parser = argparse.ArgumentParser(
        description='Migrate custom fields from OWNS-based to new architecture'
    )
    parser.add_argument(
        '--dry-run', 
        action='store_true',
        help='Show what would be migrated without making changes'
    )
    parser.add_argument(
        '--cleanup',
        action='store_true', 
        help='Clean up old CustomFieldValue nodes after migration'
    )
    
    args = parser.parse_args()
    
    # Warn about cleanup
    if args.cleanup and not args.dry_run:
        print("⚠️  WARNING: Cleanup will permanently delete old CustomFieldValue nodes")
        response = input("Are you sure you want to proceed? (y/N): ")
        if response.lower() != 'y':
            print("❌ Migration cancelled")
            return
    
    success = migrate_custom_fields(dry_run=args.dry_run, cleanup=args.cleanup)
    
    if success:
        print("✅ Migration script completed successfully")
        sys.exit(0)
    else:
        print("❌ Migration script failed")
        sys.exit(1)

if __name__ == '__main__':
    main()
