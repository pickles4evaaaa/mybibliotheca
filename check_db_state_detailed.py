#!/usr/bin/env python3
"""
Detailed Database State Checker

Checks the exact state of the KuzuDB database with comprehensive analysis.
"""

import os
import sys
import kuzu
from pathlib import Path
from datetime import datetime

def check_db_state():
    """Check detailed database state."""
    print(f"🔍 [DB_STATE_CHECK] Starting detailed database state check...")
    print(f"🔍 [DB_STATE_CHECK] Timestamp: {datetime.now()}")
    print(f"🔍 [DB_STATE_CHECK] Process ID: {os.getpid()}")
    
    # Environment
    kuzu_path = os.getenv('KUZU_DB_PATH', '/app/data/kuzu')
    print(f"🔍 [DB_STATE_CHECK] Database path: {kuzu_path}")
    
    # Check if database exists
    db_path = Path(kuzu_path)
    if not db_path.exists():
        print(f"❌ [DB_STATE_CHECK] Database directory does not exist!")
        return
    
    # List files
    files = list(db_path.glob("*"))
    total_size = sum(f.stat().st_size for f in files if f.is_file())
    print(f"📁 [DB_STATE_CHECK] Database files: {len(files)}")
    print(f"📁 [DB_STATE_CHECK] Total size: {total_size} bytes")
    
    # Show key files
    key_files = ['.wal', 'data.kz', 'catalog.kz', 'metadata.kz']
    for key_file in key_files:
        file_path = db_path / key_file
        if file_path.exists():
            size = file_path.stat().st_size
            modified = datetime.fromtimestamp(file_path.stat().st_mtime)
            print(f"📄 [DB_STATE_CHECK] {key_file}: {size} bytes (modified: {modified})")
        else:
            print(f"📄 [DB_STATE_CHECK] {key_file}: NOT FOUND")
    
    # Connect and check data
    try:
        print(f"🔗 [DB_STATE_CHECK] Connecting to database...")
        database = kuzu.Database(kuzu_path)
        connection = kuzu.Connection(database)
        print(f"✅ [DB_STATE_CHECK] Connection successful")
        
        # Count nodes
        node_types = ['User', 'Book', 'Category', 'Person', 'Publisher', 'Location']
        for node_type in node_types:
            try:
                result = connection.execute(f"MATCH (n:{node_type}) RETURN COUNT(n) as count")
                if result.has_next():
                    count = result.get_next()[0]
                    print(f"📊 [DB_STATE_CHECK] {node_type} nodes: {count}")
                else:
                    print(f"📊 [DB_STATE_CHECK] {node_type} nodes: 0")
            except Exception as e:
                print(f"📊 [DB_STATE_CHECK] {node_type} nodes: ERROR - {e}")
        
        # Count relationships
        rel_types = ['OWNS', 'AUTHORED', 'PUBLISHED_BY', 'BELONGS_TO', 'LOCATED_AT']
        for rel_type in rel_types:
            try:
                result = connection.execute(f"MATCH ()-[r:{rel_type}]->() RETURN COUNT(r) as count")
                if result.has_next():
                    count = result.get_next()[0]
                    print(f"🔗 [DB_STATE_CHECK] {rel_type} relationships: {count}")
                else:
                    print(f"🔗 [DB_STATE_CHECK] {rel_type} relationships: 0")
            except Exception as e:
                print(f"🔗 [DB_STATE_CHECK] {rel_type} relationships: ERROR - {e}")
        
        # Check specific data
        try:
            print(f"📚 [DB_STATE_CHECK] Checking book details...")
            result = connection.execute("MATCH (b:Book) RETURN b.id, b.title LIMIT 5")
            book_count = 0
            while result.has_next():
                row = result.get_next()
                book_id = row[0]
                book_title = row[1]
                print(f"📚 [DB_STATE_CHECK] Book: {book_id} - '{book_title}'")
                book_count += 1
            
            if book_count == 0:
                print(f"📚 [DB_STATE_CHECK] No books found in database")
            
        except Exception as e:
            print(f"📚 [DB_STATE_CHECK] Error checking books: {e}")
        
        # Check user-book relationships
        try:
            print(f"👤 [DB_STATE_CHECK] Checking user-book relationships...")
            result = connection.execute("""
                MATCH (u:User)-[r:OWNS]->(b:Book) 
                RETURN u.username, b.title, r.reading_status 
                LIMIT 5
            """)
            rel_count = 0
            while result.has_next():
                row = result.get_next()
                username = row[0]
                book_title = row[1]
                status = row[2]
                print(f"👤 [DB_STATE_CHECK] {username} OWNS '{book_title}' (status: {status})")
                rel_count += 1
            
            if rel_count == 0:
                print(f"👤 [DB_STATE_CHECK] No user-book relationships found")
        
        except Exception as e:
            print(f"👤 [DB_STATE_CHECK] Error checking user-book relationships: {e}")
        
        # Force commit to ensure any pending changes are written
        try:
            connection.commit()
            print(f"💾 [DB_STATE_CHECK] Forced commit successful")
        except Exception as e:
            print(f"💾 [DB_STATE_CHECK] Commit warning: {e}")
        
        connection.close()
        print(f"✅ [DB_STATE_CHECK] Connection closed")
        
    except Exception as e:
        print(f"❌ [DB_STATE_CHECK] Database connection failed: {e}")
        import traceback
        traceback.print_exc()
    
    print(f"🏁 [DB_STATE_CHECK] Database state check complete")

if __name__ == "__main__":
    check_db_state()
