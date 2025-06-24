#!/usr/bin/env python3
"""
Fix book categories - Re-process existing books to create category relationships
"""

import sys
import os
import asyncio
import traceback

# Add the current directory to Python path for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

def fix_book_categories():
    """Re-process all books to create category relationships."""
    try:
        print("🚀 Starting book category fix...")
        
        # Import app components
        from app import create_app
        from app.services import book_service
        from flask import Flask
        
        # Create Flask app context
        app = create_app()
        
        with app.app_context():
            print("📱 App context created")
            
            # Get all books
            print("📚 Getting all books...")
            books = book_service.get_all_books_sync()
            print(f"📊 Found {len(books)} books")
            
            # Check each book for category data
            books_with_categories = 0
            books_processed = 0
            books_linked = 0
            
            for book in books:
                print(f"\n📖 Processing book: {book.title} (ID: {book.id})")
                
                # Check if book has raw_categories
                if hasattr(book, 'raw_categories') and book.raw_categories:
                    print(f"✅ Book has categories: {book.raw_categories}")
                    books_with_categories += 1
                    
                    # Re-process categories
                    try:
                        result = book_service.process_book_categories_sync(book.id, book.raw_categories)
                        if result:
                            books_linked += 1
                            print(f"✅ Successfully processed categories for '{book.title}'")
                        else:
                            print(f"❌ Failed to process categories for '{book.title}'")
                    except Exception as e:
                        print(f"❌ Error processing categories for '{book.title}': {e}")
                        
                else:
                    print(f"⚠️ Book has no category data")
                
                books_processed += 1
            
            print(f"\n📊 Summary:")
            print(f"   Total books: {len(books)}")
            print(f"   Books processed: {books_processed}")
            print(f"   Books with categories: {books_with_categories}")
            print(f"   Books successfully linked: {books_linked}")
            
            if books_with_categories == 0:
                print("\n💡 No books have raw_categories data. This means:")
                print("   1. Books were imported before category processing was implemented")
                print("   2. You need to manually add categories to books")
                print("   3. Or re-import books from your original CSV/sources")
            
            print("\n✅ Book category fix complete!")
            
    except Exception as e:
        print(f"❌ Fix failed: {e}")
        traceback.print_exc()

if __name__ == "__main__":
    fix_book_categories()
