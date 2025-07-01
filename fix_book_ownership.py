#!/usr/bin/env python3
"""
Script to fix book ownership relationships.
This creates ownership relationships between the user and all books that currently exist globally.
"""

import sys
import os
sys.path.insert(0, '.')

from app.infrastructure.kuzu_graph import get_kuzu_connection
from app.services import book_service, user_service
from app.domain.models import ReadingStatus, OwnershipStatus, MediaType

def fix_book_ownership():
    """Add ownership relationships for all books to the main user."""
    print("🔧 [FIX] Starting book ownership fix...")
    
    try:
        # Get all users
        users = user_service.get_all_users_sync()
        if not users:
            print("❌ [FIX] No users found in database")
            return False
        
        main_user = users[0]  # Use the first user
        print(f"👤 [FIX] Using user: {main_user.name} (ID: {main_user.id})")
        
        # Get all global books
        all_books = book_service.get_all_books_with_user_overlay_sync(str(main_user.id))
        print(f"📚 [FIX] Found {len(all_books)} books")
        
        # Connect to KuzuDB directly to check and create ownership relationships
        kuzu_connection = get_kuzu_connection()
        conn = kuzu_connection.connect()
        
        fixed_count = 0
        for book in all_books:
            book_id = book.get('id') if isinstance(book, dict) else book.id
            title = book.get('title', 'Unknown') if isinstance(book, dict) else book.title
            
            # Check if ownership relationship exists
            check_query = """
            MATCH (u:User {id: $user_id})-[o:OWNS]->(b:Book {id: $book_id})
            RETURN o
            """
            
            result = conn.execute(check_query, parameters={
                'user_id': str(main_user.id),
                'book_id': book_id
            })
            
            if len(result.get_as_df()) == 0:
                # No ownership relationship exists, create one
                print(f"🔗 [FIX] Creating ownership for: {title}")
                
                create_query = """
                MATCH (u:User {id: $user_id}), (b:Book {id: $book_id})
                CREATE (u)-[o:OWNS {
                    reading_status: $reading_status,
                    ownership_status: $ownership_status,
                    media_type: $media_type,
                    user_rating: $user_rating,
                    personal_notes: $personal_notes,
                    start_date: $start_date,
                    finish_date: $finish_date,
                    date_added: $date_added,
                    location_id: $location_id,
                    custom_metadata: $custom_metadata
                }]->(b)
                RETURN o
                """
                
                from datetime import date
                
                conn.execute(create_query, parameters={
                    'user_id': str(main_user.id),
                    'book_id': book_id,
                    'reading_status': ReadingStatus.UNREAD.value,
                    'ownership_status': OwnershipStatus.OWNED.value,
                    'media_type': MediaType.PHYSICAL.value,
                    'user_rating': None,
                    'personal_notes': None,
                    'start_date': None,
                    'finish_date': None,
                    'date_added': date.today(),
                    'location_id': None,
                    'custom_metadata': '{}'
                })
                
                fixed_count += 1
            else:
                print(f"✅ [FIX] Ownership exists for: {title}")
        
        print(f"🎉 [FIX] Fixed ownership for {fixed_count} books")
        return True
        
    except Exception as e:
        print(f"❌ [FIX] Error: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = fix_book_ownership()
    sys.exit(0 if success else 1)
