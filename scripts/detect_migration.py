#!/usr/bin/env python3
"""
Simple Migration Detection Script
=================================

Shows what SQLite databases are available for migration without attempting migration.
"""

import sys
import os
from pathlib import Path

# Add the parent directory to the path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.migration_detector import MigrationDetector

def main():
    print("🔍 MyBibliotheca Migration Detection")
    print("=" * 40)
    
    detector = MigrationDetector()
    databases = detector.find_sqlite_databases()
    
    if not databases:
        print("✅ No SQLite databases found - you're all set!")
        print("📖 Your app is running in Redis-only mode")
        return
    
    print(f"📚 Found {len(databases)} SQLite database(s):")
    print()
    
    total_books = 0
    total_users = 0
    
    for db_path in databases:
        info = detector.analyze_database(db_path)
        if 'error' in info:
            print(f"❌ {db_path.name}: Error - {info['error']}")
            continue
            
        schema = f"v{info['schema_version']}"
        books = info['total_books']
        users = info['total_users'] if info['has_users'] else "single-user"
        
        print(f"📖 {db_path.name}")
        print(f"   📊 Schema: {schema}")
        print(f"   📚 Books: {books}")
        print(f"   👥 Users: {users}")
        print(f"   📍 Path: {db_path}")
        print()
        
        total_books += books
        if isinstance(users, int):
            total_users += users
    
    print("📋 Migration Summary:")
    print(f"   📚 Total books to migrate: {total_books}")
    if total_users > 0:
        print(f"   👥 Total users to migrate: {total_users}")
    print()
    
    print("🔧 Next Steps:")
    print()
    
    # Check for v1 vs v2 databases
    has_v1 = any(detector.analyze_database(db)['schema_version'] == 1 for db in databases)
    has_v2 = any(detector.analyze_database(db)['schema_version'] == 2 for db in databases)
    
    if has_v1 and not has_v2:
        print("📝 All databases are single-user (v1). Recommended migration:")
        print("   python3 scripts/quick_migrate.py --user-id <your-admin-username>")
        print("   💡 Replace <your-admin-username> with your actual username")
    elif has_v2 and not has_v1:
        print("📝 All databases are multi-user (v2). Recommended migration:")
        print("   python3 scripts/quick_migrate.py")
    else:
        print("📝 Mixed database types. Migrate individually:")
        for db_path in databases:
            info = detector.analyze_database(db_path)
            if info['schema_version'] == 1:
                print(f"   python3 scripts/migrate_sqlite_to_redis.py --db-path {db_path} --user-id <your-admin-username>")
            else:
                print(f"   python3 scripts/migrate_sqlite_to_redis.py --db-path {db_path}")
    
    print()
    print("💾 Note: All databases will be backed up before migration")
    print("🌐 Web-based migration: Log into http://localhost:5054 and use the migration wizard")
    print("🔗 Your app is already running and usable at http://localhost:5054")

if __name__ == "__main__":
    main()
