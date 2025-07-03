#!/usr/bin/env python3
"""Debug location fields for a specific book to understand the location display issue."""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from app import create_app
from app.services import book_service

# Create Flask app context
app = create_app()
app.app_context().push()

def debug_location_fields():
    """Debug location fields for books to understand display issues."""
    print("=== DEBUG: Location Fields Investigation ===\n")
    
    try:
        # Get all books with dynamic field retrieval
        books_data = book_service.get_all_books_with_user_overlay_sync(user_id="1")
        
        if not books_data:
            print("No books found")
            return
            
        print(f"Found {len(books_data)} books")
        
        # Look at first few books to understand location data structure
        for i, book_dict in enumerate(books_data[:3]):
            print(f"\n--- Book {i+1}: {book_dict.get('title', 'Unknown Title')} ---")
            
            # Check all location-related fields
            location_fields = []
            for key, value in book_dict.items():
                if 'location' in key.lower():
                    location_fields.append((key, value))
            
            if location_fields:
                print("Location-related fields found:")
                for field, value in location_fields:
                    print(f"  {field}: {value} (type: {type(value)})")
            else:
                print("No location-related fields found")
            
            # Check all available fields (first 10 for brevity)
            print("\nAll available fields (sample):")
            field_count = 0
            for key, value in book_dict.items():
                if field_count < 10:
                    print(f"  {key}: {value}")
                    field_count += 1
                elif field_count == 10:
                    print(f"  ... and {len(book_dict) - 10} more fields")
                    break
                    
        # Also check a specific book with potential location data
        print("\n=== Detailed Analysis of First Book ===")
        if books_data:
            book = books_data[0]
            print(f"Book: {book.get('title')}")
            
            # Check various location field possibilities
            checks = [
                ('location_id', book.get('location_id')),
                ('location', book.get('location')),
                ('locations', book.get('locations')),
                ('user_location', book.get('user_location')),
                ('user_location_id', book.get('user_location_id')),
                ('personal_location', book.get('personal_location')),
                ('shelf_location', book.get('shelf_location')),
            ]
            
            print("\nLocation field checks:")
            for field_name, value in checks:
                if value is not None:
                    print(f"  ✓ {field_name}: {value} (type: {type(value)})")
                else:
                    print(f"  ✗ {field_name}: None/missing")
                    
    except Exception as e:
        print(f"Error during location field debug: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    debug_location_fields()
