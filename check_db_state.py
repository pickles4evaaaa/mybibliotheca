#!/usr/bin/env python3
"""
Quick Database State Checker

Run this inside the Docker container to quickly check database state.
Usage: python3 check_db_state.py
"""

import os
import sys
from pathlib import Path
from datetime import datetime

# Add app to path
sys.path.append('/app')

def check_database_files():
    """Check database files on disk."""
    print("🔍 Checking database files...")
    
    kuzu_path = Path(os.getenv('KUZU_DB_PATH', '/app/data/kuzu'))
    print(f"Database path: {kuzu_path}")
    
    if kuzu_path.exists():
        print(f"✅ Database directory exists")
        files = list(kuzu_path.glob("*"))
        print(f"📂 Found {len(files)} files:")
        
        total_size = 0
        for file in files:
            if file.is_file():
                size = file.stat().st_size
                total_size += size
                mod_time = datetime.fromtimestamp(file.stat().st_mtime)
                print(f"   - {file.name}: {size} bytes (modified: {mod_time})")
        
        print(f"📊 Total database size: {total_size} bytes")
        return len(files) > 0
    else:
        print(f"❌ Database directory does not exist")
        return False

def check_database_connection():
    """Check database connection and content."""
    print("\n🔌 Checking database connection...")
    
    try:
        from app.infrastructure.kuzu_graph import KuzuGraphDB
        
        db_path = os.getenv('KUZU_DB_PATH', '/app/data/kuzu')
        db = KuzuGraphDB(db_path)
        
        # Connect to database
        connection = db.connect()
        print("✅ Successfully connected to database")
        
        # Check tables
        try:
            result = connection.execute("CALL show_tables() RETURN *")
            tables = []
            while result.has_next():
                row = result.get_next()
                tables.append(row[0])
            print(f"📋 Found {len(tables)} tables: {', '.join(tables)}")
        except Exception as e:
            print(f"⚠️ Could not list tables: {e}")
        
        # Check data counts
        entities = ['User', 'Book', 'Author', 'Category']
        for entity in entities:
            try:
                count_result = connection.execute(f"MATCH (n:{entity}) RETURN COUNT(n) as count")
                if count_result.has_next():
                    count = count_result.get_next()[0]
                    print(f"📊 {entity} count: {count}")
            except Exception as e:
                print(f"⚠️ Could not count {entity}: {e}")
        
        # Check relationships
        try:
            owns_result = connection.execute("MATCH ()-[r:OWNS]->() RETURN COUNT(r) as count")
            if owns_result.has_next():
                owns_count = owns_result.get_next()[0]
                print(f"🤝 OWNS relationships: {owns_count}")
        except Exception as e:
            print(f"⚠️ Could not count OWNS relationships: {e}")
        
        # Sample book data
        try:
            book_result = connection.execute("""
                MATCH (b:Book) 
                RETURN b.id, b.title, b.created_at 
                ORDER BY b.created_at DESC 
                LIMIT 5
            """)
            print(f"\n📚 Recent books:")
            count = 0
            while book_result.has_next() and count < 5:
                row = book_result.get_next()
                book_id = row[0][:8] + "..." if len(row[0]) > 8 else row[0]
                title = row[1][:50] + "..." if len(row[1]) > 50 else row[1]
                created = row[2]
                print(f"   - {title} (ID: {book_id}, Created: {created})")
                count += 1
        except Exception as e:
            print(f"⚠️ Could not fetch sample books: {e}")
        
        db.disconnect()
        return True
        
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    """Main function."""
    print("🧪 Quick Database State Check")
    print(f"🕒 Check time: {datetime.now()}")
    print(f"🐳 Container: {os.getenv('HOSTNAME', 'unknown')}")
    print("="*50)
    
    files_exist = check_database_files()
    can_connect = check_database_connection()
    
    print("\n" + "="*50)
    print("📊 SUMMARY:")
    print(f"   Files exist: {'✅ YES' if files_exist else '❌ NO'}")
    print(f"   Can connect: {'✅ YES' if can_connect else '❌ NO'}")
    
    if files_exist and can_connect:
        print("\n🎉 Database appears to be healthy!")
    else:
        print("\n⚠️ Database may have issues - check logs above")

if __name__ == "__main__":
    main()
