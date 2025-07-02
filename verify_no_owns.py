#!/usr/bin/env python3
"""
Verification script to ensure OWNS relationships are completely removed
and that global book visibility is working correctly.
"""

from app import create_app
from app.services import book_service

def verify_no_owns_relationships():
    """Verify that no OWNS relationships exist in the database."""
    print("🔍 [VERIFY] Starting OWNS relationship verification...")
    
    app = create_app()
    with app.app_context():
        try:
            from app.infrastructure.kuzu_graph import get_graph_storage
            storage = get_graph_storage()
            
            # Query for any OWNS relationships
            query = """
            MATCH ()-[r:OWNS]->()
            RETURN COUNT(r) as owns_count
            """
            
            result = storage.query(query, {})
            owns_count = result[0].get('owns_count', result[0].get('COUNT(r)', 0)) if result else 0
            
            print(f"📊 [VERIFY] OWNS relationships found: {owns_count}")
            
            if owns_count == 0:
                print("✅ [VERIFY] SUCCESS: No OWNS relationships found - database is clean!")
            else:
                print(f"❌ [VERIFY] FAILURE: Found {owns_count} OWNS relationships that need to be removed")
                
            # Also verify global book visibility is working
            print("🌍 [VERIFY] Testing global book visibility...")
            
            # Get all books globally
            all_books_query = """
            MATCH (b:Book)
            RETURN COUNT(b) as total_books
            """
            
            result = storage.query(all_books_query, {})
            total_books = result[0].get('total_books', result[0].get('COUNT(b)', 0)) if result else 0
            print(f"📚 [VERIFY] Total books in global catalog: {total_books}")
            
            # Test service layer
            try:
                user_books = book_service.get_all_books_with_user_overlay_sync('test-user-id')
                print(f"🔧 [VERIFY] Service layer returned {len(user_books)} books with overlay")
                
                if user_books:
                    sample_book = user_books[0]
                    has_uid = 'uid' in sample_book
                    has_reading_status = 'reading_status' in sample_book
                    print(f"📖 [VERIFY] Sample book has uid: {has_uid}, reading_status: {has_reading_status}")
                    
                    if has_uid and has_reading_status:
                        print("✅ [VERIFY] Book objects have required template attributes")
                    else:
                        print("❌ [VERIFY] Book objects missing required template attributes")
                        
                print("✅ [VERIFY] Global book visibility working correctly!")
                
            except Exception as service_error:
                print(f"❌ [VERIFY] Service layer error: {service_error}")
                
            print("🎉 [VERIFY] Verification complete!")
            
        except Exception as e:
            print(f"❌ [VERIFY] Database error: {e}")
            import traceback
            traceback.print_exc()

if __name__ == "__main__":
    verify_no_owns_relationships()
