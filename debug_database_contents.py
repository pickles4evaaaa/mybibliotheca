#!/usr/bin/env python3
"""
Debug script to check what's actually in the KuzuDB database
"""

import os
import sys
sys.path.append('/app' if os.path.exists('/app') else '.')

from src.infrastructure.kuzu_graph import KuzuGraph

def main():
    print("🔍 Checking KuzuDB contents...")
    
    # Initialize connection
    kuzu_db_path = os.getenv('KUZU_DB_PATH', './data/kuzu')
    print(f"📁 Database path: {kuzu_db_path}")
    
    try:
        graph = KuzuGraph(kuzu_db_path)
        conn = graph.get_connection()
        
        # Check all tables
        print("\n📊 Database tables:")
        result = conn.execute("SHOW TABLES")
        tables = []
        while result.has_next():
            row = result.get_next()
            table_name = row[0]
            tables.append(table_name)
            print(f"  - {table_name}")
        
        # Check user count
        if 'User' in tables:
            print("\n👥 Users:")
            result = conn.execute("MATCH (u:User) RETURN u.username, u.id LIMIT 10")
            user_count = 0
            while result.has_next():
                user_count += 1
                row = result.get_next()
                print(f"  - {row[0]} (ID: {row[1]})")
            print(f"Total users: {user_count}")
        
        # Check book count
        if 'Book' in tables:
            print("\n📚 Books:")
            result = conn.execute("MATCH (b:Book) RETURN b.title, b.id LIMIT 10")
            book_count = 0
            while result.has_next():
                book_count += 1
                row = result.get_next()
                print(f"  - {row[0]} (ID: {row[1]})")
            
            # Get total count
            result = conn.execute("MATCH (b:Book) RETURN count(*)")
            if result.has_next():
                total_books = result.get_next()[0]
                print(f"Total books: {total_books}")
        
        # Check ownership relationships
        if 'User' in tables and 'Book' in tables:
            print("\n🔗 Ownership relationships:")
            result = conn.execute("MATCH (u:User)-[r:OWNS]->(b:Book) RETURN u.username, b.title LIMIT 5")
            owns_count = 0
            while result.has_next():
                owns_count += 1
                row = result.get_next()
                print(f"  - {row[0]} owns '{row[1]}'")
            print(f"Total ownership relationships: {owns_count}")
        
        conn.close()
        print("\n✅ Database check complete")
        
    except Exception as e:
        print(f"❌ Error checking database: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
