"""
Kuzu Book Service

Handles core book CRUD operations using Kuzu as the primary database.
Focused responsibility: Book entity management only.

This service has been migrated to use the SafeKuzuManager pattern for
improved thread safety and connection management.
"""

import uuid
import traceback
from typing import List, Optional, Dict, Any
from datetime import datetime, timezone
import time
from flask import current_app

from ..domain.models import Book, ReadingStatus
from ..infrastructure.kuzu_repositories import KuzuBookRepository
from ..infrastructure.kuzu_graph import safe_execute_kuzu_query, safe_get_kuzu_connection
from .kuzu_async_helper import run_async
from ..utils.simple_cache import cached, cache_delete
import logging

logger = logging.getLogger(__name__)

def _safe_get_row_value(row: Any, index: int) -> Any:
    """Safely extract a value from a KuzuDB row at the given index."""
    if isinstance(row, list):
        return row[index] if index < len(row) else None
    elif isinstance(row, dict):
        keys = list(row.keys())
        return row[keys[index]] if index < len(keys) else None
    else:
        try:
            return row[index]  # type: ignore
        except (IndexError, KeyError, TypeError):
            return None


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
                    rows.append({'result': _safe_get_row_value(row, 0)})
                else:
                    # Multiple columns - create dict with column names
                    row_dict = {}
                    for i, value in enumerate(row):
                        row_dict[f'col_{i}'] = value
                    rows.append(row_dict)
        else:
            # Fallback: if it's already a list or other format
            if isinstance(result, list):
                return result
            elif isinstance(result, dict):
                return [result]
            else:
                # Try to convert to string representation
                rows.append({'result': str(result)})
    except Exception as e:
        logger.warning(f"Error converting query result: {e}")
        # Return empty list if conversion fails
        return []
    
    return rows


def _book_id_key(service, book_id):
    uid = getattr(service, 'user_id', 'none')
    return f"book:{uid}:{book_id}"


def _book_isbn_key(service, isbn):
    uid = getattr(service, 'user_id', 'none')
    return f"book_isbn:{uid}:{isbn}"


class KuzuBookService:
    """
    Service for core book CRUD operations using Kuzu with thread-safe operations.
    
    This service has been migrated to use the SafeKuzuManager pattern for
    improved thread safety and connection management.
    """
    
    def __init__(self, user_id: Optional[str] = None):
        """
        Initialize book service with thread-safe database access.
        
        Args:
            user_id: User identifier for tracking and isolation
        """
        self.user_id = user_id or "book_service"
        self.book_repo = KuzuBookRepository()
        
    def _dict_to_book(self, book_data: Dict[str, Any]) -> Book:
        """Convert dictionary data to Book object."""
        if isinstance(book_data, Book):
            return book_data
        
        # Create a Book object from the dictionary
        book = Book(
            id=book_data.get('id'),
            title=book_data.get('title', ''),
            normalized_title=book_data.get('normalized_title', ''),
            subtitle=book_data.get('subtitle'),
            isbn13=book_data.get('isbn13'),
            isbn10=book_data.get('isbn10'),
            asin=book_data.get('asin'),
            description=book_data.get('description'),
            published_date=book_data.get('published_date'),
            page_count=book_data.get('page_count'),
            language=book_data.get('language', 'en'),
            cover_url=book_data.get('cover_url'),
            google_books_id=book_data.get('google_books_id'),
            openlibrary_id=book_data.get('openlibrary_id'),
            average_rating=book_data.get('average_rating'),
            rating_count=book_data.get('rating_count'),
            media_type=book_data.get('media_type'),
            custom_metadata=book_data.get('custom_metadata', {}),
            created_at=book_data.get('created_at', datetime.now(timezone.utc)),
            updated_at=book_data.get('updated_at', datetime.now(timezone.utc))
        )
        
        # Handle series fields - the database stores series as a string, but the model expects a Series object
        series_name = book_data.get('series')
        if series_name:
            from ..domain.models import Series
            book.series = Series(name=series_name)
        
        # Set series volume and order
        book.series_volume = book_data.get('series_volume')
        book.series_order = book_data.get('series_order')
        
        # Initialize empty relationships (will be loaded separately)
        self._initialize_book_relationships(book)
        
        return book
    
    async def _load_book_contributors(self, book: Book) -> None:
        """Load contributors for a book from the database."""
        try:
            if not book.id:
                return  # Cannot load contributors without book ID
                
            contributors_data = await self.book_repo.get_book_authors(book.id)
            
            from ..domain.models import Person, BookContribution, ContributionType
            import logging
            logger = logging.getLogger(__name__)
            
            contributors = []
            for contrib_data in contributors_data:
                # Create Person object
                person = Person(
                    id=contrib_data.get('id'),
                    name=contrib_data.get('name') or '',
                    normalized_name=(contrib_data.get('name') or '').strip().lower()
                )
                
                # Map role string to ContributionType enum
                role_str = contrib_data.get('role', 'authored').lower()
                try:
                    contribution_type = ContributionType(role_str)
                except ValueError:
                    # Default to AUTHORED if role is not recognized
                    contribution_type = ContributionType.AUTHORED
                
                # Create BookContribution object  
                contribution = BookContribution(
                    person_id=person.id or '',
                    book_id=book.id,
                    contribution_type=contribution_type,
                    order=contrib_data.get('order_index', 0),
                    person=person
                )
                
                contributors.append(contribution)
            
            book.contributors = contributors
            
        except Exception as e:
            import logging
            logger = logging.getLogger(__name__)
            logger.error(f"Failed to load contributors for book {book.id}: {e}")
            # Initialize empty list on error
            book.contributors = []
    
    def _load_book_contributors_sync(self, book: Book) -> None:
        """Load contributors for a book from the database (sync version)."""
        try:
            if not book.id:
                return  # Cannot load contributors without book ID
                
            # Use the sync query method directly from the repository
            query = """
            MATCH (p:Person)-[rel:AUTHORED]->(b:Book {id: $book_id})
            RETURN p.name as name, p.id as id, rel.role as role, rel.order_index as order_index
            ORDER BY rel.order_index ASC
            """
            
            results = safe_execute_kuzu_query(query, {"book_id": book.id})
            results = _convert_query_result_to_list(results)
            
            from ..domain.models import Person, BookContribution, ContributionType
            import logging
            logger = logging.getLogger(__name__)
            
            contributors = []
            for result in results:
                if result.get('col_0'):  # name
                    # Create Person object
                    person = Person(
                        id=result.get('col_1') or '',
                        name=result.get('col_0') or '',
                        normalized_name=(result.get('col_0') or '').strip().lower()
                    )
                    
                    # Map role string to ContributionType enum
                    role_str = (result.get('col_2') or 'authored').lower()
                    try:
                        contribution_type = ContributionType(role_str)
                    except ValueError:
                        # Default to AUTHORED if role is not recognized
                        contribution_type = ContributionType.AUTHORED
                    
                    # Create BookContribution object  
                    contribution = BookContribution(
                        person_id=person.id or '',
                        book_id=book.id,
                        contribution_type=contribution_type,
                        order=result.get('col_3', 0),
                        person=person
                    )
                    
                    contributors.append(contribution)
            
            book.contributors = contributors
            
        except Exception as e:
            import logging
            logger = logging.getLogger(__name__)
            logger.error(f"Failed to load contributors for book {book.id}: {e}")
            # Initialize empty list on error
            book.contributors = []

    def _load_book_categories_sync(self, book: Book) -> None:
        """Load categories for a book from the database (sync version)."""
        try:
            if not book.id:
                return  # Cannot load categories without book ID
                
            # Use the sync query method directly from the repository
            query = """
            MATCH (b:Book {id: $book_id})-[:CATEGORIZED_AS]->(c:Category)
            RETURN c.name as name, c.id as id, c.description as description, 
                   c.color as color, c.icon as icon, c.aliases as aliases,
                   c.normalized_name as normalized_name, c.parent_id as parent_id,
                   c.level as level, c.book_count as book_count, c.user_book_count as user_book_count,
                   c.created_at as created_at, c.updated_at as updated_at
            ORDER BY c.name ASC
            """
            
            results = safe_execute_kuzu_query(query, {"book_id": book.id})
            results = _convert_query_result_to_list(results)
            
            from ..domain.models import Category
            import logging
            logger = logging.getLogger(__name__)
            
            categories = []
            for result in results:
                if result.get('col_0'):  # name
                    # Create Category object
                    category = Category(
                        id=result.get('col_1') or '',
                        name=result.get('col_0') or '',
                        normalized_name=result.get('col_6') or '',
                        description=result.get('col_2'),
                        parent_id=result.get('col_7'),
                        level=result.get('col_8', 0),
                        color=result.get('col_3'),
                        icon=result.get('col_4'),
                        aliases=result.get('col_5', []),
                        book_count=result.get('col_9', 0),
                        user_book_count=result.get('col_10', 0),
                        created_at=result.get('col_11') or datetime.now(timezone.utc),
                        updated_at=result.get('col_12') or datetime.now(timezone.utc)
                    )
                    categories.append(category)
            
            book.categories = categories
            
        except Exception as e:
            import logging
            logger = logging.getLogger(__name__)
            logger.error(f"Failed to load categories for book {book.id}: {e}")
            # Initialize empty list on error
            book.categories = []

    def _load_book_publisher_sync(self, book: Book) -> None:
        """Load publisher for a book from the database (sync version)."""
        try:
            if not book.id:
                return  # Cannot load publisher without book ID
                
            # Use the sync query method directly from the repository
            query = """
            MATCH (b:Book {id: $book_id})-[:PUBLISHED_BY]->(p:Publisher)
            RETURN p.name as name, p.id as id, p.country as country, p.founded_year as founded_year
            LIMIT 1
            """
            
            results = safe_execute_kuzu_query(query, {"book_id": book.id})
            results = _convert_query_result_to_list(results)
            
            from ..domain.models import Publisher
            import logging
            logger = logging.getLogger(__name__)
            
            if results and results[0].get('col_0'):  # name
                # Create Publisher object
                publisher = Publisher(
                    id=results[0].get('col_1') or '',
                    name=results[0].get('col_0') or '',
                    country=results[0].get('col_2'),
                    founded_year=results[0].get('col_3')
                )
                book.publisher = publisher
            else:
                book.publisher = None
            
        except Exception as e:
            import logging
            logger = logging.getLogger(__name__)
            logger.error(f"Failed to load publisher for book {book.id}: {e}")
            # Initialize None on error
            book.publisher = None

    def _initialize_book_relationships(self, book: Book) -> None:
        """Initialize and load relationships for a book."""
        if not hasattr(book, 'categories'):
            book.categories = []
        if not hasattr(book, 'contributors'):
            book.contributors = []
        if not hasattr(book, 'publisher'):
            book.publisher = None
            
        # Load all relationships from database if book has an ID
        if book.id:
            self._load_book_contributors_sync(book)
            self._load_book_categories_sync(book)
            self._load_book_publisher_sync(book)
    
    async def create_book(self, domain_book: Book) -> Book:
        """Create a book in Kuzu."""
        try:
            t0 = time.perf_counter()
            
            # Ensure the book has an ID
            if not domain_book.id:
                domain_book.id = str(uuid.uuid4())
            t_id = time.perf_counter()
            
            # Set timestamps
            domain_book.created_at = datetime.now(timezone.utc)
            domain_book.updated_at = datetime.now(timezone.utc)
            t_ts = time.perf_counter()
            
            # Create the book in Kuzu
            created_book = await self.book_repo.create(domain_book)
            t_repo = time.perf_counter()
            
            if not created_book:
                raise ValueError("Failed to create book in repository")
            
            try:
                current_app.logger.info(
                    f"[BOOK][CREATE] id={domain_book.id} total={t_repo - t0:.3f}s gen_id={t_id - t0:.3f}s set_ts={t_ts - t_id:.3f}s repo={t_repo - t_ts:.3f}s"
                )
            except Exception:
                pass
            return domain_book
            
        except Exception as e:
            traceback.print_exc()
            raise
    
    @cached(ttl_seconds=300, key_builder=_book_id_key)
    async def get_book_by_id(self, book_id: str) -> Optional[Book]:
        """Get a book by ID."""
        try:
            book_data = await self.book_repo.get_by_id(book_id)
            if book_data:
                return self._dict_to_book(book_data)
            return None
        except Exception as e:
            return None
    
    async def update_book(self, book_id: str, updates: Dict[str, Any]) -> Optional[Book]:
        """Update a book's basic fields."""
        try:
            
            # Get current book data
            book_data = await self.book_repo.get_by_id(book_id)
            if not book_data:
                return None
            
            # Convert to Book object
            book = self._dict_to_book(book_data)
            
            # Update basic fields
            for field, value in updates.items():
                # Defensive guard: never overwrite an existing cover with empty/None inadvertently
                if field == 'cover_url':
                    current_cover = getattr(book, 'cover_url', None)
                    # If attempting to set blank/None while there is an existing cover, skip
                    if (value is None or (isinstance(value, str) and not value.strip())) and current_cover:
                        logger.info(f"[COVER] Ignoring blank cover_url update to preserve existing cover {current_cover}")
                        continue
                if hasattr(book, field):
                    setattr(book, field, value)
            
            book.updated_at = datetime.now(timezone.utc)
            
            # Prepare update data - ONLY include fields that were actually updated
            book_dict = {}
            
            # Always update the timestamp
            book_dict['updated_at'] = book.updated_at
            
            # Only include fields that were explicitly updated
            for field, value in updates.items():
                if field == 'series' and value:
                    # Handle series field conversion
                    if hasattr(value, 'name'):
                        book_dict['series'] = value.name
                    else:
                        book_dict['series'] = str(value)
                elif field == 'cover_url':
                    current_cover = getattr(book, 'cover_url', None)
                    if (value is None or (isinstance(value, str) and not value.strip())) and current_cover:
                        # Skip adding blank cover_url to update statement
                        continue
                    if hasattr(book, field):
                        book_dict[field] = getattr(book, field)
                    else:
                        book_dict[field] = value
                elif hasattr(book, field):
                    # Get the updated value from the book object
                    book_dict[field] = getattr(book, field)
                else:
                    # Field might not exist on book object, use the raw value
                    book_dict[field] = value
            
            # Update the book node in Kuzu using safe query execution
            # Whitelist of properties that exist on the Book node (avoid relationship fields like publisher)
            allowed_properties = {
                'title', 'subtitle', 'normalized_title', 'isbn13', 'isbn10', 'asin',
                'description', 'published_date', 'page_count', 'language', 'cover_url',
                'google_books_id', 'openlibrary_id', 'average_rating', 'rating_count',
                'series', 'series_volume', 'series_order', 'created_at', 'updated_at', 'media_type',
                'quantity'
            }

            set_clauses = []
            params = {"book_id": book_id}

            for key, value in book_dict.items():
                if key not in allowed_properties:
                    # Skip unsupported properties (e.g., publisher)
                    continue
                set_clauses.append(f"b.{key} = ${key}")
                params[key] = value
            
            update_query = f"""
            MATCH (b:Book {{id: $book_id}})
            SET {', '.join(set_clauses)}
            RETURN b
            """
            
            def _exec_update():
                return safe_execute_kuzu_query(
                    query=update_query,
                    params=params,
                    user_id=self.user_id,
                    operation="update_book"
                )

            try:
                raw_result = _exec_update()
            except Exception as ex:
                # Gracefully add missing quantity column if older DB lacks it
                if 'quantity' in updates and 'Cannot find property quantity' in str(ex):
                    try:
                        logger.warning("[BOOK][MIGRATION] Adding missing quantity column to Book node")
                        safe_execute_kuzu_query(
                            query="ALTER TABLE Book ADD quantity INT64",
                            params={},
                            user_id=self.user_id,
                            operation="add_quantity_column"
                        )
                        raw_result = _exec_update()
                    except Exception:
                        raise
                else:
                    raise
            
            results = _convert_query_result_to_list(raw_result)
            if not results:
                return None
            
            # Invalidate cache
            cache_delete(_book_id_key(self, book_id))
                
            return book
            
        except Exception as e:
            traceback.print_exc()
            return None
    
    async def delete_book(self, book_id: str) -> bool:
        """Delete a book completely from the system."""
        try:
            print(f"🗑️ [DELETE_BOOK] Deleting book {book_id}")
            
            # Delete the book and all its relationships using DETACH DELETE
            delete_query = """
            MATCH (b:Book {id: $book_id})
            DETACH DELETE b
            """
            
            # Use safe query execution
            safe_execute_kuzu_query(
                query=delete_query,
                params={"book_id": book_id},
                user_id=self.user_id,
                operation="delete_book"
            )
            
            # Invalidate cache
            cache_delete(_book_id_key(self, book_id))
            
            return True
            
        except Exception as e:
            traceback.print_exc()
            return False
    
    @cached(ttl_seconds=300, key_builder=_book_isbn_key)
    async def get_book_by_isbn(self, isbn: str) -> Optional[Book]:
        """Get a book by ISBN (13 or 10)."""
        try:
            # Query for book by ISBN13 or ISBN10
            query = """
            MATCH (b:Book)
            WHERE b.isbn13 = $isbn OR b.isbn10 = $isbn
            RETURN b
            LIMIT 1
            """
            
            # Use safe query execution and convert result
            raw_result = safe_execute_kuzu_query(
                query=query,
                params={"isbn": isbn},
                user_id=self.user_id,
                operation="get_book_by_isbn"
            )
            
            results = _convert_query_result_to_list(raw_result)
            
            if results and 'col_0' in results[0]:
                book_data = results[0]['col_0']
                return self._dict_to_book(book_data)
            
            return None
            
        except Exception as e:
            return None
    
    def find_or_create_book_sync(self, domain_book: Book) -> Optional[Book]:
        """Find an existing book or create a new one (sync version)."""
        try:
            # First try to find existing book by ISBN
            if domain_book.isbn13:
                existing_book = run_async(self.get_book_by_isbn(domain_book.isbn13))
                if existing_book:
                    return existing_book
            
            if domain_book.isbn10:
                existing_book = run_async(self.get_book_by_isbn(domain_book.isbn10))
                if existing_book:
                    return existing_book
            
            # If no ISBN match, try to find by title (simplified search)
            try:
                query = """
                MATCH (b:Book)
                WHERE toLower(b.title) = toLower($title)
                RETURN b
                LIMIT 1
                """
                
                # Use safe query execution and convert result
                raw_result = safe_execute_kuzu_query(
                    query=query,
                    params={"title": domain_book.title},
                    user_id=self.user_id,
                    operation="find_or_create_book_by_title"
                )
                
                results = _convert_query_result_to_list(raw_result)
                
                if results and results[0].get('col_0'):
                    book_data = results[0]['col_0']
                    return self._dict_to_book(book_data)
                
            except Exception as search_error:
                print(f"Error searching for existing book: {search_error}")
            
            # No existing book found, create a new one
            try:
                created_book = run_async(self.create_book(domain_book))
                return created_book
            except Exception as create_error:
                print(f"Error creating new book: {create_error}")
                return None
            
        except Exception as e:
            traceback.print_exc()
            return None
    
    # Sync wrappers for backward compatibility
    def create_book_sync(self, domain_book: Book) -> Book:
        """Sync wrapper for create_book."""
        start = time.perf_counter()
        book = run_async(self.create_book(domain_book))
        end = time.perf_counter()
        try:
            from flask import current_app
            current_app.logger.info(f"[BOOK][CREATE_SYNC] total={end-start:.3f}s id={book.id if book else 'n/a'}")
        except Exception:
            pass
        return book
    
    @cached(ttl_seconds=300, key_builder=_book_id_key)
    def get_book_by_id_sync(self, book_id: str) -> Optional[Book]:
        """Sync wrapper for get_book_by_id."""
        return run_async(self.get_book_by_id(book_id))

    
    def update_book_sync(self, book_id: str, updates: Dict[str, Any]) -> Optional[Book]:
        """Sync wrapper for update_book."""
        return run_async(self.update_book(book_id, updates))
    
    def delete_book_sync(self, book_id: str) -> bool:
        """Sync wrapper for delete_book."""
        return run_async(self.delete_book(book_id))
    
    @cached(ttl_seconds=300, key_builder=_book_isbn_key)
    def get_book_by_isbn_sync(self, isbn: str) -> Optional[Book]:
        """Sync wrapper for get_book_by_isbn."""
        return run_async(self.get_book_by_isbn(isbn))

