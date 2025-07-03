#!/usr/bin/env python3
"""Debug user-specific location data and relationships."""

import sys
import os
import asyncio
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from app import create_app
from app.kuzu_integration import get_kuzu_service

# Create Flask app context
app = create_app()
app.app_context().push()

def debug_user_location_data():
    """Debug user-specific location data and relationships."""
    print("=== DEBUG: User Location Data Investigation ===\n")
    
    try:
        kuzu_service = get_kuzu_service()
        
        # Check user locations using the async method
        print("1. Checking user locations:")
        user_locations = asyncio.run(kuzu_service.get_user_locations("1"))
        print(f"User locations: {user_locations}")
        
        print("\n" + "="*50 + "\n")
        
        # Check user library to see location data
        print("2. Checking user library:")
        user_library = asyncio.run(kuzu_service.get_user_library("1"))
        print(f"Found {len(user_library)} books in user library")
        
        if user_library:
            for book in user_library[:3]:  # Show first 3
                print(f"\nBook: {book.get('title')}")
                # Look for location-related fields
                location_fields = {}
                for key, value in book.items():
                    if 'location' in key.lower():
                        location_fields[key] = value
                
                if location_fields:
                    print(f"  Location fields: {location_fields}")
                else:
                    print("  No location fields found")
                    
                # Show all available fields (first 10)
                print(f"  Available fields: {list(book.keys())[:10]}...")
        
        print("\n" + "="*50 + "\n")
        
        # Check if there's a direct connection to the database for raw queries
        if hasattr(kuzu_service, 'conn') and kuzu_service.conn:
            conn = kuzu_service.conn
            print("3. Raw database queries:")
            
            # Check OWNS relationship properties
            print("3a. Checking OWNS relationship properties:")
            try:
                result = conn.execute("MATCH (u:User)-[r:OWNS]->(b:Book) RETURN u.id, b.title, r.* LIMIT 5")
                df = result.get_as_df()
                if not df.empty:
                    print(f"OWNS relationships:\n{df.to_string()}")
                else:
                    print("No OWNS relationships found")
            except Exception as e:
                print(f"OWNS query failed: {e}")
            
            print("\n3b. Checking all relationship types:")
            try:
                result = conn.execute("MATCH (n)-[r]->(m) RETURN DISTINCT type(r) as relationship_type")
                df = result.get_as_df()
                if not df.empty:
                    print(f"All relationship types:\n{df.to_string()}")
                else:
                    print("No relationships found")
            except Exception as e:
                print(f"Relationship types query failed: {e}")
        else:
            print("3. Cannot access raw database connection")
                    
    except Exception as e:
        print(f"Error during user location debug: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    debug_user_location_data()
