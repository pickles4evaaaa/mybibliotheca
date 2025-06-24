#!/usr/bin/env python3
"""
Utility script to recalculate book counts for all categories.

This fixes the issue where existing categories have book_count = 0
even though they have books associated with them.
"""

import sys
import os
import asyncio

# Add the app directory to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'app'))

from app import create_app
from app.services import book_service

async def recalculate_all_category_counts():
    """Recalculate book counts for all categories."""
    try:
        print("🔧 Starting category book count recalculation...")
        
        # Get all categories
        all_categories = await book_service.get_all_categories()
        print(f"📊 Found {len(all_categories)} categories to process")
        
        updated_count = 0
        
        for category in all_categories:
            try:
                print(f"🔍 Processing category: {category.name} (ID: {category.id})")
                print(f"   Current book_count: {category.book_count}")
                
                # Recalculate the book count
                success = await book_service.redis_category_repo.recalculate_book_count(category.id)
                
                if success:
                    updated_count += 1
                    # Get updated category to show new count
                    updated_category = await book_service.get_category_by_id(category.id)
                    if updated_category:
                        print(f"   ✅ Updated book_count: {updated_category.book_count}")
                    else:
                        print(f"   ✅ Count recalculated successfully")
                else:
                    print(f"   ❌ Failed to recalculate count")
                    
            except Exception as e:
                print(f"   ❌ Error processing category {category.name}: {e}")
                continue
        
        print(f"\n🎉 Recalculation complete!")
        print(f"   Categories processed: {len(all_categories)}")
        print(f"   Categories updated: {updated_count}")
        print(f"   Categories failed: {len(all_categories) - updated_count}")
        
    except Exception as e:
        print(f"❌ Error during recalculation: {e}")
        import traceback
        traceback.print_exc()

def main():
    """Main function to run the recalculation."""
    app = create_app()
    
    with app.app_context():
        asyncio.run(recalculate_all_category_counts())

if __name__ == "__main__":
    main()
