#!/usr/bin/env python3
"""
Book Transfer Tool for Kuzu-based MyBibliotheca
Transfer book ownership from one user to another using Kuzu graph database.

This script replaces the Redis-based transfer_books.py script.
"""

import os
import sys
import argparse
from typing import List

# Add the project root to Python path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

try:
    from app import create_app
    from app.services import user_service, book_service
    from config import Config
except ImportError as e:
    print(f"❌ Error importing application modules: {e}")
    print("🔧 Make sure you're running this from the MyBibliotheca directory")
    sys.exit(1)


def transfer_books_kuzu(from_username: str, to_username: str, book_ids: List[str] = None, dry_run: bool = False):
    """
    Transfer books from one user to another using Kuzu services.
    
    Args:
        from_username: Source user username
        to_username: Target user username  
        book_ids: Optional list of specific book IDs to transfer. If None, transfers all books.
        dry_run: If True, shows what would be transferred without making changes
    """
    app = create_app()
    
    with app.app_context():
        # Get users
        from_user = user_service.get_user_by_username_sync(from_username)
        if not from_user:
            print(f"❌ Source user '{from_username}' not found")
            return False
        
        to_user = user_service.get_user_by_username_sync(to_username)  
        if not to_user:
            print(f"❌ Target user '{to_username}' not found")
            return False
        
        print(f"👥 Transferring books from {from_user.username} to {to_user.username}")
        
        # Get source user's books
        try:
            source_books = book_service.get_user_books_sync(from_user.id)
            print(f"📚 Found {len(source_books)} books in {from_user.username}'s library")
            
            if not source_books:
                print("✅ No books to transfer")
                return True
            
            # Filter books if specific IDs provided
            books_to_transfer = source_books
            if book_ids:
                books_to_transfer = [book for book in source_books 
                                   if book.get('id') in book_ids or book.get('uid') in book_ids]
                print(f"🎯 Filtering to {len(books_to_transfer)} specified books")
            
            if dry_run:
                print("\n🔍 DRY RUN - Would transfer these books:")
                for i, book in enumerate(books_to_transfer, 1):
                    title = book.get('title', 'Unknown Title')
                    book_id = book.get('uid') or book.get('id')
                    print(f"  {i}. {title} (ID: {book_id})")
                print(f"\n📊 Total: {len(books_to_transfer)} books")
                return True
            
            # Perform the transfer
            transferred_count = 0
            failed_count = 0
            
            for book in books_to_transfer:
                book_id = book.get('uid') or book.get('id')
                title = book.get('title', 'Unknown Title')
                
                try:
                    # Remove from source user
                    removed = book_service.delete_book_sync(book_id, from_user.id)
                    if removed:
                        # Add to target user with same metadata
                        added = book_service.add_book_to_user_library_sync(
                            to_user.id, 
                            book_id,
                            reading_status=book.get('reading_status', 'plan_to_read'),
                            ownership_status=book.get('ownership_status', 'owned'),
                            media_type=book.get('media_type', 'physical'),
                            notes=book.get('personal_notes', ''),
                            custom_metadata=book.get('custom_metadata', {})
                        )
                        
                        if added:
                            print(f"✅ Transferred: {title}")
                            transferred_count += 1
                        else:
                            print(f"❌ Failed to add {title} to target user")
                            failed_count += 1
                    else:
                        print(f"❌ Failed to remove {title} from source user")
                        failed_count += 1
                        
                except Exception as e:
                    print(f"❌ Error transferring {title}: {e}")
                    failed_count += 1
            
            print(f"\n📊 Transfer Summary:")
            print(f"✅ Successfully transferred: {transferred_count} books")
            if failed_count > 0:
                print(f"❌ Failed transfers: {failed_count} books")
            
            return failed_count == 0
            
        except Exception as e:
            print(f"❌ Error during transfer: {e}")
            return False


def main():
    """Main function with argument parsing."""
    parser = argparse.ArgumentParser(
        description="Transfer book ownership between users in Kuzu-based MyBibliotheca"
    )
    parser.add_argument("from_user", help="Source username")
    parser.add_argument("to_user", help="Target username")
    parser.add_argument("--books", nargs="+", help="Specific book IDs to transfer (optional)")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be transferred without making changes")
    
    args = parser.parse_args()
    
    print("🚀 Kuzu Book Transfer Tool")
    print("=" * 40)
    
    success = transfer_books_kuzu(
        from_username=args.from_user,
        to_username=args.to_user, 
        book_ids=args.books,
        dry_run=args.dry_run
    )
    
    if success:
        print("\n🎉 Transfer completed successfully!")
        sys.exit(0)
    else:
        print("\n💥 Transfer failed!")
        sys.exit(1)


if __name__ == "__main__":
    main()
