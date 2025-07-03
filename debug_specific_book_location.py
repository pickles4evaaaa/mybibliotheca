#!/usr/bin/env python3
"""Debug the specific book and its OWNS relationship to see location data."""

import sys
import os
import asyncio
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from app import create_app
from app.kuzu_integration import get_kuzu_service

# Create Flask app context
app = create_app()
app.app_context().push()

def debug_specific_book_location():
    """Debug the specific book and see if there's location data in the OWNS relationship."""
    print("=== DEBUG: Specific Book Location Investigation ===\n")
    
    try:
        kuzu_service = get_kuzu_service()
        
        # Get the book by calling the same method the template calls
        print("1. Getting book via get_book_by_uid_sync:")
        
        # Since we know there's only one book, let's find its UID first
        from app.services import book_service
        
        # First get all books to find the UID
        all_books = book_service.get_all_books_with_user_overlay_sync("1")
        if all_books:
            book = all_books[0]
            uid = book.get('uid') or book.get('id')
            print(f"Found book UID: {uid}")
            
            # Now get it via the specific method used by the view
            specific_book = book_service.get_book_by_uid_sync(uid, "1")
            print(f"Book retrieved via get_book_by_uid_sync:")
            print(f"  Title: {specific_book.get('title') if specific_book else 'Not found'}")
            
            if specific_book:
                # Look for all location-related fields
                location_fields = {}
                for key, value in specific_book.items():
                    if 'location' in key.lower():
                        location_fields[key] = value
                
                print(f"  Location fields: {location_fields}")
                
                # Check ownership data specifically
                ownership_data = specific_book.get('ownership', {})
                print(f"  Ownership data: {ownership_data}")
                
                # Check if there are any user-specific fields
                user_fields = {}
                for key, value in specific_book.items():
                    if 'user' in key.lower():
                        user_fields[key] = value
                
                print(f"  User-related fields: {user_fields}")
                
                # Show first 20 fields for debugging
                print(f"  All fields (first 20): {list(specific_book.keys())[:20]}")
        
        else:
            print("No books found")
        
        print("\n" + "="*50 + "\n")
        
        # Try direct database access if possible
        if hasattr(kuzu_service, 'db') and kuzu_service.db:
            print("2. Direct database queries:")
            
            # Query the OWNS relationship directly
            print("2a. OWNS relationship properties:")
            try:
                result = kuzu_service.db.execute("MATCH (u:User {id: '1'})-[r:OWNS]->(b:Book) RETURN b.title, r.* LIMIT 5")
                df = result.get_as_df()
                if not df.empty:
                    print(f"OWNS relationships:\n{df.to_string()}")
                else:
                    print("No OWNS relationships found")
            except Exception as e:
                print(f"OWNS query failed: {e}")
            
            # Check if there are any Location nodes
            print("\n2b. Location nodes:")
            try:
                result = kuzu_service.db.execute("MATCH (l:Location) RETURN l.* LIMIT 5")
                df = result.get_as_df()
                if not df.empty:
                    print(f"Location nodes:\n{df.to_string()}")
                else:
                    print("No Location nodes found")
            except Exception as e:
                print(f"Location query failed: {e}")
        else:
            print("2. Cannot access database directly")
                    
    except Exception as e:
        print(f"Error during specific book location debug: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    debug_specific_book_location()
