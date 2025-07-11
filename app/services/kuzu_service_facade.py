"""
Kuzu Service Facade

Provides a unified interface for all Kuzu services while maintaining backward compatibility
with the original KuzuBookService. This facade composes all the individual services
and delegates method calls to the appropriate service.
"""

import traceback
from typing import List, Optional, Dict, Any
from datetime import datetime

from ..domain.models import Book, Category
from ..infrastructure.kuzu_repositories import KuzuBookRepository
from ..infrastructure.kuzu_graph import get_graph_storage
from .kuzu_book_service import KuzuBookService
from .kuzu_category_service import KuzuCategoryService
from .kuzu_person_service import KuzuPersonService
from .kuzu_relationship_service import KuzuRelationshipService
from .kuzu_search_service import KuzuSearchService
from .kuzu_async_helper import run_async


class KuzuServiceFacade:
    """
    Facade that provides a unified interface to all Kuzu services.
    
    This class maintains backward compatibility with the original KuzuBookService
    while delegating responsibilities to focused service classes.
    """
    
    def __init__(self):
        # Initialize all the individual services
        self.book_service = KuzuBookService()
        self.category_service = KuzuCategoryService()
        self.person_service = KuzuPersonService()
        self.relationship_service = KuzuRelationshipService()
        self.search_service = KuzuSearchService()
        
        # Keep reference to book repository for compatibility
        self.book_repo = KuzuBookRepository()
    
    # ==========================================
    # Book Service Methods
    # ==========================================
    
    def create_book_sync(self, domain_book: Book, user_id: str) -> Book:
        """Create a book and associate it with a user."""
        # First create the book
        book = self.book_service.create_book_sync(domain_book)
        
        # Then add it to the user's library
        if book and book.id:
            success = self.relationship_service.add_book_to_user_library_sync(
                user_id=user_id,
                book_id=book.id,
                reading_status="plan_to_read"
            )
            if success:
                print(f"✅ [FACADE] Created book and added to user {user_id} library")
            else:
                print(f"⚠️ [FACADE] Created book but failed to add to user library")
        
        return book
    
    def get_book_by_id_sync(self, book_id: str) -> Optional[Book]:
        """Get a book by ID."""
        return self.book_service.get_book_by_id_sync(book_id)
    
    def update_book_sync(self, book_id: str, user_id: str, **kwargs) -> Optional[Book]:
        """Update a book with filtering for relationship-specific updates."""
        print(f"🔄 [FACADE] update_book_sync called with book_id={book_id}, user_id={user_id}")
        print(f"🔄 [FACADE] kwargs: {kwargs}")
        
        # Filter out location-related fields and relationship fields
        location_fields = {'location_id', 'primary_location_id', 'locations'}
        relationship_fields = {'reading_status', 'ownership_status', 'user_rating', 
                              'personal_notes', 'start_date', 'finish_date', 'custom_metadata'}
        
        book_updates = {k: v for k, v in kwargs.items() 
                       if k not in location_fields and k not in relationship_fields}
        relationship_updates = {k: v for k, v in kwargs.items() 
                               if k in relationship_fields}
        location_updates = {k: v for k, v in kwargs.items() 
                           if k in location_fields}
        
        if location_updates:
            print(f"🔄 [FACADE] Location updates: {location_updates}")
        
        print(f"🔄 [FACADE] Book updates: {book_updates}")
        print(f"🔄 [FACADE] Relationship updates: {relationship_updates}")
        
        # Update book if there are book-specific updates
        updated_book = None
        if book_updates:
            updated_book = self.book_service.update_book_sync(book_id, book_updates)
        
        # Update relationship if there are relationship-specific updates or location updates
        if relationship_updates or location_updates:
            # Merge relationship and location updates
            all_relationship_updates = {**relationship_updates, **location_updates}
            success = self.relationship_service.update_user_book_relationship_sync(
                user_id, book_id, all_relationship_updates
            )
            if success:
                print(f"✅ [FACADE] Updated relationship for user {user_id} and book {book_id}")
            else:
                print(f"❌ [FACADE] Failed to update relationship")
        
        # Return the updated book or fetch it fresh if only relationship was updated
        if updated_book:
            return updated_book
        elif relationship_updates or location_updates:
            return self.get_book_by_uid_sync(book_id, user_id)
        else:
            return self.get_book_by_id_sync(book_id)
    
    def delete_book_sync(self, book_id: str, user_id: str) -> bool:
        """Delete a book from user's library."""
        try:
            print(f"🗑️ [FACADE] Deleting book {book_id} for user {user_id}")
            
            # Remove from user's library first
            success = self.relationship_service.remove_book_from_user_library_sync(user_id, book_id)
            
            if success:
                # Check if any other users have this book
                query = """
                MATCH (u:User)-[owns:OWNS]->(b:Book {id: $book_id})
                RETURN count(u) as owner_count
                """
                
                results = self.graph_storage.query(query, {"book_id": book_id})
                owner_count = 0
                if results:
                    # Use proper result key based on query structure
                    result = results[0]
                    if 'owner_count' in result:
                        owner_count = result['owner_count']
                    elif 'col_0' in result:
                        owner_count = result['col_0'] 
                    else:
                        # Fallback: check all possible result keys
                        for key, value in result.items():
                            if isinstance(value, int):
                                owner_count = value
                                break
                
                print(f"🔍 [FACADE] Book {book_id} has {owner_count} remaining owners")
                
                if owner_count == 0:
                    # No other users own this book, safe to delete the book node entirely
                    print(f"🗑️ [FACADE] No other owners, deleting book node {book_id}")
                    book_delete_success = self.book_service.delete_book_sync(book_id)
                    if book_delete_success:
                        print(f"✅ [FACADE] Deleted book node {book_id} from database")
                        
                        # Clean up any orphaned OWNS relationships that might reference this book
                        print(f"🧹 [FACADE] Cleaning up orphaned relationships for book {book_id}")
                        cleanup_query = """
                        MATCH ()-[owns:OWNS]->(:Book {id: $book_id})
                        DELETE owns
                        """
                        try:
                            self.graph_storage.query(cleanup_query, {"book_id": book_id})
                            print(f"✅ [FACADE] Cleaned up orphaned relationships for book {book_id}")
                        except Exception as cleanup_error:
                            print(f"⚠️ [FACADE] Error cleaning up relationships: {cleanup_error}")
                    else:
                        print(f"⚠️ [FACADE] Failed to delete book node {book_id}, but relationship removed")
                else:
                    print(f"✅ [FACADE] Book {book_id} kept (owned by {owner_count} other users)")
                
                print(f"✅ [FACADE] Removed book {book_id} from user {user_id} library")
            
            return success
            
        except Exception as e:
            print(f"❌ [FACADE] Error deleting book: {e}")
            traceback.print_exc()
            return False
    
    def find_or_create_book_sync(self, domain_book: Book) -> Optional[Book]:
        """Find an existing book or create a new one."""
        return self.book_service.find_or_create_book_sync(domain_book)
    
    # ==========================================
    # Relationship Service Methods
    # ==========================================
    
    def get_books_for_user(self, user_id: str, limit: int = 50, offset: int = 0) -> List[Book]:
        """Get all books for a user with relationship data."""
        return self.relationship_service.get_books_for_user_sync(user_id, limit, offset)
    
    def get_user_books_sync(self, user_id: str) -> List[Book]:
        """Get all books for a user."""
        return self.relationship_service.get_books_for_user_sync(user_id)
    
    def get_book_by_id_for_user_sync(self, book_id: str, user_id: str) -> Optional[Book]:
        """Get a specific book for a user with relationship data."""
        return self.relationship_service.get_book_by_id_for_user_sync(book_id, user_id)
    
    def get_book_by_uid_sync(self, uid: str, user_id: str) -> Optional[Book]:
        """Get a book by UID with user overlay data."""
        return self.relationship_service.get_book_by_uid_sync(uid, user_id)
    
    def get_user_book_sync(self, user_id: str, book_id: str) -> Optional[Dict[str, Any]]:
        """Get user's book by book ID."""
        return self.relationship_service.get_user_book_sync(user_id, book_id)
    
    def add_book_to_user_library_sync(self, user_id: str, book_id: str, 
                                      reading_status: str = "library_only",
                                      ownership_status: str = "owned",
                                      locations: Optional[List[str]] = None,
                                      custom_metadata: Optional[Dict[str, Any]] = None) -> bool:
        """Add a book to user's library."""
        return self.relationship_service.add_book_to_user_library_sync(
            user_id, book_id, reading_status, ownership_status, locations, custom_metadata
        )
    
    def get_all_books_with_user_overlay(self, user_id: str) -> List[Dict[str, Any]]:
        """Get all books with user-specific overlay data."""
        return self.relationship_service.get_all_books_with_user_overlay_sync(user_id)
    
    def get_all_books_with_user_overlay_sync(self, user_id: str) -> List[Dict[str, Any]]:
        """Sync version of get_all_books_with_user_overlay."""
        return self.relationship_service.get_all_books_with_user_overlay_sync(user_id)
    
    # ==========================================
    # Search Service Methods
    # ==========================================
    
    def search_books_sync(self, query: str, user_id: str, limit: int = 50) -> List[Book]:
        """Search books for a user."""
        return self.search_service.search_books_sync(query, user_id, limit)
    
    def get_book_by_isbn_for_user_sync(self, isbn: str, user_id: str) -> Optional[Book]:
        """Get a book by ISBN for a specific user."""
        return self.search_service.get_book_by_isbn_for_user_sync(isbn, user_id)
    
    def get_books_with_sharing_users_sync(self, days_back: int = 30, limit: int = 20) -> List[Book]:
        """Get books from users who share reading activity."""
        return self.search_service.get_books_with_sharing_users_sync(days_back, limit)
    
    def get_currently_reading_shared_sync(self, limit: int = 20) -> List[Book]:
        """Get currently reading books from users who share current reading."""
        return self.search_service.get_currently_reading_shared_sync(limit)
    
    # ==========================================
    # Category Service Methods
    # ==========================================
    
    def list_all_categories_sync(self, user_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get all categories."""
        return self.category_service.list_all_categories_sync()
    
    def get_category_by_id_sync(self, category_id: str, user_id: Optional[str] = None) -> Optional[Category]:
        """Get a category by ID with full hierarchy."""
        return self.category_service.get_category_by_id_sync(category_id)
    
    def get_child_categories_sync(self, parent_id: str) -> List[Dict[str, Any]]:
        """Get child categories for a parent category."""
        return self.category_service.get_child_categories_sync(parent_id)
    
    def get_category_children_sync(self, category_id: str, user_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get children of a category."""
        return self.category_service.get_category_children_sync(category_id)
    
    def get_books_by_category_sync(self, category_id: str, user_id: Optional[str] = None, 
                                   include_subcategories: bool = False) -> List[Dict[str, Any]]:
        """Get books in a category."""
        return self.category_service.get_books_by_category_sync(category_id, include_subcategories)

    def get_book_categories_sync(self, book_id: str) -> List[Dict[str, Any]]:
        """Get categories for a book."""
        try:
            # Use run_async to call the async method from book repository
            categories = run_async(self.book_repo.get_book_categories)(book_id)
            
            # Convert to dictionary format
            category_list = []
            for category in categories:
                if isinstance(category, dict):
                    category_list.append(category)
                else:
                    # Convert category object to dictionary
                    category_dict = {}
                    for attr in ['id', 'name', 'normalized_name', 'parent_id', 'created_at', 'updated_at']:
                        if hasattr(category, attr):
                            value = getattr(category, attr)
                            if isinstance(value, datetime):
                                category_dict[attr] = value.isoformat()
                            else:
                                category_dict[attr] = value
                    category_list.append(category_dict)
            
            return category_list
        except Exception as e:
            print(f"❌ [FACADE] Error getting book categories for {book_id}: {e}")
            return []
    
    def create_category_sync(self, category_data: Dict[str, Any]) -> Optional[Category]:
        """Create a new category."""
        return self.category_service.create_category_sync(category_data)
    
    def update_category_sync(self, category: Category) -> Optional[Category]:
        """Update an existing category."""
        return self.category_service.update_category_sync(category)
    
    def delete_category_sync(self, category_id: str) -> bool:
        """Delete a category."""
        return self.category_service.delete_category_sync(category_id)
    
    def merge_categories_sync(self, primary_category_id: str, merge_category_ids: List[str]) -> bool:
        """Merge multiple categories into one."""
        return self.category_service.merge_categories_sync(primary_category_id, merge_category_ids)
    
    def search_categories_sync(self, query: str, limit: int = 10, user_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """Search categories by name or description."""
        return self.category_service.search_categories_sync(query, limit, user_id)
    
    def get_root_categories_sync(self, user_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get root categories (categories without parent)."""
        return self.category_service.get_root_categories_sync(user_id)
    
    # ==========================================
    # Person Service Methods
    # ==========================================
    
    def list_all_persons_sync(self, user_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get all persons."""
        return self.person_service.list_all_persons_sync()
    
    def get_person_by_id_sync(self, person_id: str) -> Optional[Dict[str, Any]]:
        """Get a person by ID."""
        return self.person_service.get_person_by_id_sync(person_id)
    
    def get_books_by_person_sync(self, person_id: str, user_id: str) -> Dict[str, List[Dict[str, Any]]]:
        """Get books associated with a person organized by contribution type."""
        # Get all books by this person
        all_books = self.person_service.get_books_by_person_sync(person_id)
        
        # Filter books to only include those in the user's library
        user_books = self.get_all_books_with_user_overlay_sync(user_id)
        user_book_ids = {book.get('id') for book in user_books}
        
        # Filter and organize by contribution type
        books_by_type = {}
        for book in all_books:
            book_id = book.get('id') or book.get('uid')
            if book_id in user_book_ids:
                contribution_type = book.get('relationship_type', 'AUTHORED')
                if contribution_type not in books_by_type:
                    books_by_type[contribution_type] = []
                books_by_type[contribution_type].append(book)
        
        return books_by_type
    
    # ==========================================
    # Legacy Compatibility Methods
    # ==========================================
    
    # These methods provide compatibility with the original interface
    async def create_book(self, domain_book: Book, user_id: str) -> Book:
        """Async version of create_book."""
        return run_async(self.create_book_sync(domain_book, user_id))
    
    async def get_book_by_id(self, book_id: str) -> Optional[Book]:
        """Async version of get_book_by_id."""
        return await self.book_service.get_book_by_id(book_id)
    
    async def update_book(self, book_id: str, updates: Dict[str, Any], user_id: str) -> Optional[Book]:
        """Async version of update_book."""
        return run_async(self.update_book_sync(book_id, user_id, **updates))
    
    async def delete_book(self, uid: str, user_id: str) -> bool:
        """Async version of delete_book."""
        return run_async(self.delete_book_sync(uid, user_id))
    
    async def get_all_books_with_user_overlay_async(self, user_id: str) -> List[Dict[str, Any]]:
        """Async version of get_all_books_with_user_overlay."""
        return run_async(self.get_all_books_with_user_overlay(user_id))
    
    # Property to maintain compatibility with existing code that accesses these attributes
    @property
    def graph_storage(self):
        """Access to graph storage through book service."""
        return self.book_service.graph_storage
    
    @property
    def user_repo(self):
        """Access to user repository through relationship service."""
        return self.relationship_service.user_repo
    
    def _dict_to_book(self, book_data: Dict[str, Any]) -> Book:
        """Convert dictionary data to Book object."""
        return self.book_service._dict_to_book(book_data)
    
    def _create_enriched_book(self, book_data: Dict[str, Any], relationship_data: Dict[str, Any]) -> Book:
        """Create an enriched Book object with user-specific attributes."""
        return self.relationship_service._create_enriched_book(book_data, relationship_data)
    
    # ==========================================
    # Missing Methods for 100% API Compatibility
    # ==========================================
    
    # Non-sync versions of existing methods
    async def get_book_by_id_for_user(self, book_id: str, user_id: str) -> Optional[Book]:
        """Async version of get_book_by_id_for_user_sync."""
        return run_async(self.get_book_by_id_for_user_sync(book_id, user_id))
    
    async def list_all_categories(self, user_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """Async version of list_all_categories_sync."""
        return run_async(self.list_all_categories_sync(user_id))
    
    async def list_all_persons(self, user_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """Async version of list_all_persons_sync."""
        return run_async(self.list_all_persons_sync(user_id))
    
    async def get_category_by_id(self, category_id: str, user_id: Optional[str] = None) -> Optional[Category]:
        """Async version of get_category_by_id_sync."""
        return run_async(self.get_category_by_id_sync(category_id, user_id))
    
    async def get_child_categories(self, parent_id: str) -> List[Dict[str, Any]]:
        """Async version of get_child_categories_sync."""
        return run_async(self.get_child_categories_sync(parent_id))
    
    async def get_category_children(self, category_id: str, user_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """Async version of get_category_children_sync."""
        return run_async(self.get_category_children_sync(category_id, user_id))
    
    async def get_books_by_category(self, category_id: str, user_id: Optional[str] = None, 
                                   include_subcategories: bool = False) -> List[Dict[str, Any]]:
        """Async version of get_books_by_category_sync."""
        return run_async(self.get_books_by_category_sync(category_id, user_id, include_subcategories))
    
    async def get_person_by_id(self, person_id: str) -> Optional[Dict[str, Any]]:
        """Async version of get_person_by_id_sync."""
        return run_async(self.get_person_by_id_sync(person_id))
    
    async def search_books(self, query: str, user_id: str, limit: int = 50) -> List[Book]:
        """Async version of search_books_sync."""
        return run_async(self.search_books_sync(query, user_id, limit))
    
    async def get_book_by_isbn_for_user(self, isbn: str, user_id: str) -> Optional[Book]:
        """Async version of get_book_by_isbn_for_user_sync."""
        return run_async(self.get_book_by_isbn_for_user_sync(isbn, user_id))
    
    async def get_books_with_sharing_users(self, days_back: int = 30, limit: int = 20) -> List[Book]:
        """Async version of get_books_with_sharing_users_sync."""
        return run_async(self.get_books_with_sharing_users_sync(days_back, limit))
    
    async def get_currently_reading_shared(self, limit: int = 20) -> List[Book]:
        """Async version of get_currently_reading_shared_sync."""
        return run_async(self.get_currently_reading_shared_sync(limit))
    
    # Helper methods
    def _get_all_descendant_categories(self, category_id: str) -> List[str]:
        """Get all descendant category IDs for a given category."""
        return self.category_service._get_all_descendant_categories_sync(category_id)
    
    async def update_book_relationships(self, user_id: str, book_id: str, **kwargs) -> bool:
        """Update relationship-specific fields for a user's book."""
        return run_async(self.relationship_service.update_user_book_relationship_sync(user_id, book_id, kwargs))
    
    def _load_book_relationships(self, book_id: str, user_id: str) -> Dict[str, Any]:
        """Load relationship data for a book and user."""
        try:
            relationship = self.relationship_service.get_user_book_sync(user_id, book_id)
            return relationship if relationship else {}
        except Exception as e:
            print(f"❌ [FACADE] Error loading book relationships: {e}")
            return {}
    
    def _build_category_with_hierarchy(self, category_data: Dict[str, Any]) -> Dict[str, Any]:
        """Build category data with full hierarchy information."""
        return self.category_service._build_category_with_hierarchy_sync(category_data)
