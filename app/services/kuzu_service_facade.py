"""
Kuzu Service Facade

Provides a unified interface for all Kuzu services while maintaining backward compatibility
        # Personal standard fields stored per-user
        personal_relationship_fields = {
            'reading_status', 'ownership_status', 'user_rating',
            'personal_notes', 'review', 'start_date', 'finish_date'
        }the original KuzuBookService. This facade composes all the individual services
and delegates method calls to the appropriate service.
"""

import traceback
from typing import List, Optional, Dict, Any
from datetime import datetime, timezone, date

from ..domain.models import Book, Category, now_utc
from ..infrastructure.kuzu_repositories import KuzuBookRepository
from ..infrastructure.kuzu_graph import safe_execute_kuzu_query
from .kuzu_book_service import KuzuBookService
from .kuzu_category_service import KuzuCategoryService
from .kuzu_person_service import KuzuPersonService
from .kuzu_relationship_service import KuzuRelationshipService
from .kuzu_search_service import KuzuSearchService
from .kuzu_custom_field_service import KuzuCustomFieldService
from .kuzu_reading_log_service import KuzuReadingLogService
from .kuzu_async_helper import run_async


def _convert_query_result_to_list(result) -> List[Dict[str, Any]]:
    """
    Convert KuzuDB QueryResult to list of dictionaries (matching old graph_storage.query format).
    
    Args:
        result: QueryResult object from KuzuDB
        
    Returns:
        List of dictionaries representing rows
    """
    if result is None:
        return []
    
    rows = []
    try:
        # Check if result has the iterator interface
        if hasattr(result, 'has_next') and hasattr(result, 'get_next'):
            while result.has_next():
                row = result.get_next()
                # Convert row to dict
                if len(row) == 1:
                    # Single column result
                    rows.append({'result': row[0]})
                else:
                    # Multi-column result - use col_0, col_1, etc. format for compatibility
                    row_dict = {}
                    for i, value in enumerate(row):
                        row_dict[f'col_{i}'] = value
                    rows.append(row_dict)
        
        return rows
    except Exception as e:
        print(f"Error converting query result to list: {e}")
        return []


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
        self.custom_field_service = KuzuCustomFieldService()
        self.reading_log_service = KuzuReadingLogService()
        
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
            # Honor empty-as-default; don't auto-mark plan_to_read
            _ = self.relationship_service.add_book_to_user_library_sync(
                user_id=user_id,
                book_id=book.id,
                reading_status=""
            )
            return book
        
        return book
    
    def get_book_by_id_sync(self, book_id: str) -> Optional[Book]:
        """Get a book by ID."""
        return self.book_service.get_book_by_id_sync(book_id)
    
    def update_book_sync(self, book_id: str, user_id: str, **kwargs) -> Optional[Book]:
        """Update a book with filtering for relationship-specific updates."""
        
        # Separate different types of updates based on correct architecture:
        # 1. Book metadata (global) - Book table
        # 2. Personal standard fields (personal) - HAS_PERSONAL_METADATA relationship (replaces legacy OWNS)
        # 3. Custom metadata (custom) - custom metadata system

        # Personal standard fields stored per-user
        personal_relationship_fields = {
            'reading_status', 'ownership_status', 'user_rating',
            'personal_notes', 'review', 'start_date', 'finish_date'
        }

        # Custom metadata fields
        custom_metadata_fields = {'custom_metadata'}

        # Location fields (handled separately)
        location_fields = {'location_id', 'primary_location_id', 'locations'}

        # Contributor fields (handled separately)
        contributor_fields = {'contributors'}

        # Category fields (handled separately)
        category_fields = {'raw_categories', 'categories'}

        # Split the updates
        book_updates = {k: v for k, v in kwargs.items()
                        if k not in personal_relationship_fields and k not in custom_metadata_fields
                        and k not in location_fields and k not in contributor_fields and k not in category_fields}
        personal_updates = {k: v for k, v in kwargs.items() if k in personal_relationship_fields}
        custom_metadata_updates = {k: v for k, v in kwargs.items() if k in custom_metadata_fields}
        location_updates = {k: v for k, v in kwargs.items() if k in location_fields}
        contributor_updates = {k: v for k, v in kwargs.items() if k in contributor_fields}
        category_updates = {k: v for k, v in kwargs.items() if k in category_fields}
        
        # Update book metadata (global fields)
        updated_book = None
        if book_updates:
            updated_book = self.book_service.update_book_sync(book_id, book_updates)
        
        # Update personal metadata (replacing OWNS usage)
        if personal_updates:
            from .personal_metadata_service import personal_metadata_service

            def _to_utc(value):
                if value is None:
                    return None
                if isinstance(value, datetime):
                    return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
                if isinstance(value, date):
                    return datetime(value.year, value.month, value.day, tzinfo=timezone.utc)
                if isinstance(value, str):
                    s = value.strip()
                    if not s:
                        return None
                    candidate = s.replace('Z', '+00:00')
                    parsed: Optional[datetime] = None
                    try:
                        parsed = datetime.fromisoformat(candidate)
                    except ValueError:
                        for fmt in ('%Y-%m-%d', '%m/%d/%Y', '%d/%m/%Y', '%Y/%m/%d'):
                            try:
                                parsed = datetime.strptime(s, fmt)
                                break
                            except ValueError:
                                continue
                        if parsed is None:
                            try:
                                epoch = float(s)
                                if epoch > 10_000_000_000:
                                    epoch /= 1000.0
                                return datetime.fromtimestamp(epoch, tz=timezone.utc)
                            except (ValueError, OSError):
                                return None
                    if parsed is None:
                        return None
                    if parsed.tzinfo is None:
                        parsed = parsed.replace(tzinfo=timezone.utc)
                    else:
                        parsed = parsed.astimezone(timezone.utc)
                    return parsed
                return None

            def _norm_status(value: Optional[str]) -> str:
                if not value:
                    return ''
                return str(value).strip().lower().replace('-', '_')

            personal_kwargs: Dict[str, Any] = {}
            custom_updates: Dict[str, Any] = {}

            for k, v in personal_updates.items():
                if k == 'review':
                    personal_kwargs['user_review'] = v
                elif k == 'personal_notes':
                    personal_kwargs[k] = v
                elif k in ('start_date', 'finish_date'):
                    if v is None:
                        custom_updates[k] = None
                    else:
                        personal_kwargs[k] = _to_utc(v)
                else:
                    custom_updates[k] = v

            try:
                existing_meta = personal_metadata_service.get_personal_metadata(user_id, book_id)
            except Exception:
                existing_meta = {}

            existing_status_norm = _norm_status(existing_meta.get('reading_status'))
            existing_start = _to_utc(existing_meta.get('start_date'))
            existing_finish = _to_utc(existing_meta.get('finish_date'))

            status_explicit = 'reading_status' in custom_updates
            status_norm = _norm_status(custom_updates['reading_status']) if status_explicit else existing_status_norm

            user_set_start = 'start_date' in personal_kwargs
            user_cleared_start = custom_updates.get('start_date', object()) is None if 'start_date' in custom_updates else False
            user_set_finish = 'finish_date' in personal_kwargs
            user_cleared_finish = custom_updates.get('finish_date', object()) is None if 'finish_date' in custom_updates else False

            if user_set_finish:
                if not status_explicit or status_norm != 'read':
                    custom_updates['reading_status'] = 'read'
                    status_explicit = True
                    status_norm = 'read'

            if user_set_start and not status_explicit and status_norm != 'read':
                custom_updates['reading_status'] = 'reading'
                status_explicit = True
                status_norm = 'reading'

            if user_cleared_finish:
                custom_updates['finish_date'] = None
                if status_norm == 'read':
                    custom_updates['reading_status'] = 'reading'
                    status_norm = 'reading'
                    status_explicit = True

            if status_norm == 'read':
                if not user_set_finish and not user_cleared_finish and existing_finish is None:
                    personal_kwargs['finish_date'] = now_utc()
                if not user_set_start and not user_cleared_start and existing_start is None:
                    personal_kwargs['start_date'] = personal_kwargs.get('start_date') or now_utc()
            else:
                if not user_set_finish and not user_cleared_finish and existing_finish is not None:
                    custom_updates['finish_date'] = None
                if status_norm not in ('reading', 'currently_reading', 'in_progress'):
                    if not user_set_start and not user_cleared_start and existing_start is not None:
                        custom_updates['start_date'] = None
                elif status_norm in ('reading', 'currently_reading', 'in_progress'):
                    if not user_set_start and not user_cleared_start and existing_start is None:
                        personal_kwargs['start_date'] = now_utc()

            if custom_updates:
                personal_kwargs['custom_updates'] = custom_updates

            try:
                personal_metadata_service.update_personal_metadata(user_id, book_id, **personal_kwargs)
            except Exception as e:
                print(f"Failed to update personal metadata for book {book_id}: {e}")
        
        # Update custom metadata (only true custom fields)
        if custom_metadata_updates:
            success = self.custom_field_service.save_custom_metadata_sync(
                book_id, user_id, custom_metadata_updates
            )
            if success:
                pass  # Success logged elsewhere
            else:
                print(f"Failed to update custom metadata for book {book_id}")
        
        # Update location if present (still use relationship service for locations)  
        if location_updates:
            success = self.relationship_service.update_user_book_relationship_sync(
                user_id, book_id, location_updates
            )
            if success:
                pass  # Success logged elsewhere
            else:
                print(f"Failed to update location for book {book_id}")
        
        # Update contributors if present
        if contributor_updates and 'contributors' in contributor_updates:
            try:
                success = run_async(self._update_contributors_async(book_id, contributor_updates['contributors']))
                if success:
                    pass  # Success logged elsewhere
                else:
                    print(f"Failed to update contributors for book {book_id}")
            except Exception as e:
                print(f"Error updating contributors for book {book_id}: {e}")
        
        # Update categories if present
        if category_updates and 'raw_categories' in category_updates:
            try:
                success = run_async(self._update_categories_async(book_id, category_updates['raw_categories']))
                if success:
                    pass  # Success logged elsewhere
                else:
                    print(f"Failed to update categories for book {book_id}")
            except Exception as e:
                print(f"Error updating categories for book {book_id}: {e}")
        
        # Return the updated book or fetch it fresh if only metadata was updated
        if updated_book:
            return updated_book
        elif personal_updates or custom_metadata_updates or location_updates or contributor_updates or category_updates:
            return self.get_book_by_uid_sync(book_id, user_id)
        else:
            return self.get_book_by_id_sync(book_id)
    
    def delete_book_sync(self, book_id: str, user_id: str) -> bool:
        """Delete a book (universal library mode: remove personal metadata then book)."""
        try:
            self.relationship_service.remove_book_from_user_library_sync(user_id, book_id)
            # Directly delete book node (no multi-owner semantics now)
            if self.book_service.delete_book_sync(book_id):
                # Best-effort cleanup of any lingering OWNS
                try:
                    safe_execute_kuzu_query("MATCH ()-[o:OWNS]->(b:Book {id: $book_id}) DELETE o", {"book_id": book_id})
                except Exception:
                    pass
                return True
            return False
        except Exception:
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
    
    def get_user_books_sync(self, user_id: str, limit: int = 50, offset: int = 0) -> List[Book]:
        """Get all books for a user."""
        return self.relationship_service.get_books_for_user_sync(user_id, limit, offset)
    
    def get_recently_read_books_for_user(self, user_id: str, limit: int = 5) -> List[Dict[str, Any]]:
        """Get recently read books for a user based on reading logs."""
        return self.reading_log_service.get_recently_read_books_sync(user_id, limit)
    
    def get_recently_added_want_to_read_books(self, user_id: str, limit: int = 5) -> List[Book]:
        """Get recently added want-to-read books for a user."""
        return self.relationship_service.get_recently_added_want_to_read_books_sync(user_id, limit)

    def get_recently_added_books_sync(self, limit: int = 5) -> List[Book]:
        """Get the most recently added books across the library."""
        return self.relationship_service.get_recently_added_books_sync(limit)
    
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

    def get_books_with_user_overlay_paginated_sync(self, user_id: str, limit: int, offset: int, sort: str = 'title_asc') -> List[Dict[str, Any]]:
        """Paginated list of books with user overlay."""
        return self.relationship_service.get_books_with_user_overlay_paginated_sync(user_id, limit, offset, sort)

    def get_total_book_count_sync(self) -> int:
        """Total number of Book nodes."""
        return self.relationship_service.get_total_book_count_sync()

    def get_library_status_counts_sync(self, user_id: str) -> Dict[str, int]:
        """Global reading/ownership status counts for a user across all books."""
        return self.relationship_service.get_library_status_counts_sync(user_id)
    
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
                    for attr in ['id', 'name', 'normalized_name', 'parent_id', 'color', 'icon', 'aliases', 'description', 'level', 'book_count', 'user_book_count', 'created_at', 'updated_at']:
                        if hasattr(category, attr):
                            value = getattr(category, attr)
                            if isinstance(value, datetime):
                                category_dict[attr] = value.isoformat()
                            else:
                                category_dict[attr] = value
                    category_list.append(category_dict)
            
            return category_list
        except Exception as e:
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
    
    def get_category_book_counts_sync(self) -> Dict[str, int]:
        """Get book counts for all categories."""
        return self.category_service.get_category_book_counts_sync()
    
    def search_categories_sync(self, query: str, limit: int = 10, user_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """Search categories by name or description."""
        return self.category_service.search_categories_sync(query, limit, user_id)
    
    def get_root_categories_sync(self, user_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get root categories (categories without parent)."""
        return self.category_service.get_root_categories_sync(user_id)
    
    def get_all_categories_sync(self) -> List[Dict[str, Any]]:
        """Get all categories (alias for list_all_categories_sync)."""
        return self.category_service.list_all_categories_sync()
    
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
            return {}
    
    def _build_category_with_hierarchy(self, category_data: Dict[str, Any]) -> Dict[str, Any]:
        """Build category data with full hierarchy information."""
        return self.category_service._build_category_with_hierarchy_sync(category_data)
    
    async def _update_contributors_async(self, book_id: str, contributors: List[Any]) -> bool:
        """Update contributor relationships for a book."""
        try:
            # First, remove all existing contributor relationships for this book
            # Use AUTHORED since all contributor types use AUTHORED relationship with role property
            delete_query = """
            MATCH (p:Person)-[r:AUTHORED]->(b:Book {id: $book_id})
            DELETE r
            """
            result = safe_execute_kuzu_query(delete_query, {"book_id": book_id})
            
            # Then add the new contributor relationships
            if contributors:
                for i, contribution in enumerate(contributors):
                    await self.book_repo._create_contributor_relationship(book_id, contribution, i)
            
            return True
            
        except Exception as e:
            traceback.print_exc()
            return False

    async def _update_categories_async(self, book_id: str, raw_categories: Any) -> bool:
        """Update category relationships for a book."""
        try:
            # First, remove all existing category relationships for this book
            delete_query = """
            MATCH (b:Book {id: $book_id})-[r:CATEGORIZED_AS]->(c:Category)
            DELETE r
            """
            result = safe_execute_kuzu_query(delete_query, {"book_id": book_id})
            
            # Then add the new category relationships using the existing method
            if raw_categories:
                await self.book_repo._create_category_relationships_from_raw(book_id, raw_categories)
            
            return True
            
        except Exception as e:
            traceback.print_exc()
            return False
