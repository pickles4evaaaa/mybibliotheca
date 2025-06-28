#!/usr/bin/env python3
"""
Debug script to check category book counts
"""

import os
import sys

# Add the project root to Python path
project_root = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, project_root)

def debug_category_counts():
    """Debug category book count functionality"""
    print("🧪 Debugging Category Book Counts...")
    
    try:
        # Import after adding to path
        from app.services import book_service
        from app.infrastructure.redis_graph import get_graph_storage
        
        # Get storage directly to inspect data
        storage = get_graph_storage()
        
        print("\n1. Checking all categories...")
        categories = book_service.list_all_categories_sync()
        print(f"Found {len(categories)} categories")
        
        for i, category in enumerate(categories[:10]):  # Show first 10
            print(f"  {i+1}. {category.name} (ID: {category.id}) - Book count: {getattr(category, 'book_count', 'NOT SET')}")
        
        print("\n2. Checking HAS_CATEGORY relationships...")
        # Get all HAS_CATEGORY relationships from storage
        all_relationships = storage.get_relationships('book', None, 'HAS_CATEGORY')
        print(f"Found {len(all_relationships)} HAS_CATEGORY relationships")
        
        # Group by category
        category_counts = {}
        for rel in all_relationships:
            cat_id = rel.get('to_id')
            if cat_id:
                category_counts[cat_id] = category_counts.get(cat_id, 0) + 1
        
        print(f"Categories with books: {len(category_counts)}")
        for cat_id, count in list(category_counts.items())[:10]:
            print(f"  Category {cat_id}: {count} books")
        
        print("\n3. Testing individual category stats...")
        if categories:
            test_category = categories[0]
            print(f"Testing category: {test_category.name} (ID: {test_category.id})")
            
            # Test the repository method directly
            try:
                stats = book_service.redis_category_repo.get_category_usage_stats_sync(test_category.id)
                print(f"Direct stats call result: {stats}")
            except Exception as e:
                print(f"Error calling stats: {e}")
            
        print("\n4. Checking all books...")
        # Get a sample of books to see if they have categories
        book_nodes = storage.find_nodes_by_type('book', limit=5)
        print(f"Found {len(book_nodes)} books (showing first 5)")
        
        for book_data in book_nodes:
            book_id = book_data.get('_id')
            if book_id:
                # Get categories for this book
                book_cats = storage.get_relationships('book', book_id, 'HAS_CATEGORY')
                print(f"  Book {book_id}: {len(book_cats)} categories")
                for rel in book_cats:
                    print(f"    -> Category {rel.get('to_id')}")
        
        return True
        
    except Exception as e:
        print(f"❌ Debug failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    print("🚀 Starting category count debug...")
    success = debug_category_counts()
    
    if success:
        print("\n🎉 Debug completed!")
    else:
        print("\n💥 Debug failed!")
        sys.exit(1)
