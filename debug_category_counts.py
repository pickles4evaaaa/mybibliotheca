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
    """Debug category counts"""
    print("🔍 Debugging category counts...")
    
    try:
        from app.services import book_service
        from app.infrastructure.kuzu_graph import get_graph_storage
        
        # Get all categories
        categories = book_service.list_all_categories_sync()
        
        print(f"📊 Found {len(categories)} categories")
        
        # Check each category
        for category in categories[:10]:  # First 10 for debugging
            print(f"\n📁 Category: {category.name} (ID: {category.id})")
            print(f"   - book_count attribute: {getattr(category, 'book_count', 'NOT SET')}")
            
            # Manual count check
            storage = get_graph_storage()
            relationships = storage.get_relationships('book', None, 'HAS_CATEGORY')
            category_books = [rel for rel in relationships if rel.get('to_id') == category.id]
            manual_count = len(category_books)
            print(f"   - Manual count: {manual_count}")
            
            if manual_count > 0:
                print(f"   - Sample book IDs: {[rel.get('from_id') for rel in category_books[:3]]}")
        
        # Check if there are any HAS_CATEGORY relationships at all
        print(f"\n🔗 Checking all HAS_CATEGORY relationships...")
        storage = get_graph_storage()
        all_category_rels = storage.get_relationships('book', None, 'HAS_CATEGORY')
        print(f"   - Total HAS_CATEGORY relationships: {len(all_category_rels)}")
        
        if len(all_category_rels) > 0:
            print("   - Sample relationships:")
            for rel in all_category_rels[:5]:
                print(f"     * {rel}")
        
        # Check if any books exist at all
        all_book_nodes = storage.find_nodes_by_type('book')
        print(f"\n📚 Total books in system: {len(all_book_nodes)}")
        
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
        print("🎉 Debug completed!")
    else:
        print("💥 Debug failed!")
        sys.exit(1)
