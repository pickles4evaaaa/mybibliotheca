#!/usr/bin/env python3
"""
Cover Restoration Script

Use this to restore covers to books that lost their cover_url during editing.
This script allows you to manually map cover files to books.
"""

from app.infrastructure.kuzu_graph import get_kuzu_database
from pathlib import Path
import sys

def restore_cover(book_id: str, cover_filename: str):
    """Restore a cover URL to a book."""
    db = get_kuzu_database()
    
    # Verify the cover file exists
    cover_path = Path(f'data/covers/{cover_filename}')
    if not cover_path.exists():
        print(f"❌ Cover file not found: {cover_path}")
        return False
    
    # Update the book's cover_url
    cover_url = f'/covers/{cover_filename}'
    
    try:
        result = db.connection.execute(f'''
            MATCH (b:Book) 
            WHERE b.id = '{book_id}'
            SET b.cover_url = '{cover_url}'
            RETURN b.title, b.cover_url
        ''')
        updated = result.get_all()
        
        if updated:
            title = updated[0][0]
            new_url = updated[0][1]
            print(f"✅ Restored cover for '{title}': {new_url}")
            return True
        else:
            print(f"❌ Book not found with ID: {book_id}")
            return False
            
    except Exception as e:
        print(f"❌ Error updating book: {e}")
        return False

def list_orphaned_covers():
    """List all cover files and books without covers."""
    # Available cover files
    covers_dir = Path('data/covers')
    if covers_dir.exists():
        cover_files = list(covers_dir.glob('*.jpg')) + list(covers_dir.glob('*.png'))
        print("🖼️  Available cover files:")
        for f in cover_files:
            print(f"  - {f.name}")
    
    # Books without covers
    db = get_kuzu_database()
    result = db.connection.execute('''
        MATCH (b:Book) 
        WHERE b.cover_url IS NULL OR b.cover_url = ''
        RETURN b.title, b.id
    ''')
    books = result.get_all()
    
    print("\n📚 Books without covers:")
    for book in books:
        print(f"  - '{book[0]}' (ID: {book[1]})")

if __name__ == "__main__":
    if len(sys.argv) == 1:
        # No arguments - list orphaned covers and books
        list_orphaned_covers()
        print("\nTo restore a cover, run:")
        print("python restore_covers.py <book_id> <cover_filename>")
        
    elif len(sys.argv) == 3:
        # Restore specific cover
        book_id = sys.argv[1]
        cover_filename = sys.argv[2]
        restore_cover(book_id, cover_filename)
        
    else:
        print("Usage:")
        print("  python restore_covers.py                    # List orphaned covers")
        print("  python restore_covers.py <book_id> <cover>  # Restore specific cover")
