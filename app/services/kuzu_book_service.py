"""
Kuzu Book Service

Handles core book CRUD operations using Kuzu as the primary database.
Focused responsibility: Book entity management only.
"""

import uuid
import traceback
from typing import List, Optional, Dict, Any
from datetime import datetime

from ..domain.models import Book, ReadingStatus
from ..infrastructure.kuzu_repositories import KuzuBookRepository
from ..infrastructure.kuzu_graph import get_graph_storage
from .kuzu_async_helper import run_async


class KuzuBookService:
    """Service for core book CRUD operations using Kuzu."""
    
    def __init__(self):
        self.graph_storage = get_graph_storage()
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
            custom_metadata=book_data.get('custom_metadata', {}),
            created_at=book_data.get('created_at', datetime.utcnow()),
            updated_at=book_data.get('updated_at', datetime.utcnow())
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
            
            results = self.book_repo.db.query(query, {"book_id": book.id})
            
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
                    logger.info(f"🔍 [LOAD_DEBUG] Found contributor: {result.get('col_0')}, role_str: {role_str}")
                    try:
                        contribution_type = ContributionType(role_str)
                        logger.info(f"🔍 [LOAD_DEBUG] Mapped to contribution_type: {contribution_type}")
                    except ValueError:
                        logger.warning(f"⚠️ [LOAD_DEBUG] Unknown role '{role_str}', defaulting to AUTHORED")
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
            
            results = self.book_repo.db.query(query, {"book_id": book.id})
            
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
                        created_at=result.get('col_11') or datetime.utcnow(),
                        updated_at=result.get('col_12') or datetime.utcnow()
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
            
            results = self.book_repo.db.query(query, {"book_id": book.id})
            
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
            
            # Ensure the book has an ID
            if not domain_book.id:
                domain_book.id = str(uuid.uuid4())
            
            # Set timestamps
            domain_book.created_at = datetime.utcnow()
            domain_book.updated_at = datetime.utcnow()
            
            # Create the book in Kuzu
            created_book = await self.book_repo.create(domain_book)
            
            if not created_book:
                raise ValueError("Failed to create book in repository")
            
            return domain_book
            
        except Exception as e:
            traceback.print_exc()
            raise
    
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
                if hasattr(book, field):
                    setattr(book, field, value)
            
            book.updated_at = datetime.utcnow()
            
            # Handle series field conversion
            series_value = None
            if hasattr(book, 'series') and book.series:
                if hasattr(book.series, 'name'):
                    series_value = book.series.name
                else:
                    series_value = str(book.series)
            
            # Prepare update data (exclude 'id' as it's the primary key)
            book_dict = {
                'title': book.title,
                'normalized_title': book.normalized_title,
                'subtitle': book.subtitle,
                'isbn13': book.isbn13,
                'isbn10': book.isbn10,
                'asin': book.asin,
                'description': book.description,
                'published_date': book.published_date,
                'page_count': book.page_count,
                'language': book.language,
                'cover_url': book.cover_url,
                'google_books_id': book.google_books_id,
                'openlibrary_id': book.openlibrary_id,
                'average_rating': book.average_rating,
                'rating_count': book.rating_count,
                'custom_metadata': book.custom_metadata,
                'series': series_value,
                'series_volume': getattr(book, 'series_volume', None),
                'series_order': getattr(book, 'series_order', None),
                'created_at': book.created_at,
                'updated_at': book.updated_at
            }
            
            # Update the book node in Kuzu
            success = self.graph_storage.update_node('Book', book_id, book_dict)
            if not success:
                return None
                
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
            
            self.graph_storage.query(delete_query, {"book_id": book_id})
            return True
            
        except Exception as e:
            traceback.print_exc()
            return False
    
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
            
            results = self.graph_storage.query(query, {"isbn": isbn})
            
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
                
                results = self.graph_storage.query(query, {"title": domain_book.title})
                
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
        return run_async(self.create_book(domain_book))
    
    def get_book_by_id_sync(self, book_id: str) -> Optional[Book]:
        """Sync wrapper for get_book_by_id."""
        return run_async(self.get_book_by_id(book_id))
    
    def update_book_sync(self, book_id: str, updates: Dict[str, Any]) -> Optional[Book]:
        """Sync wrapper for update_book."""
        return run_async(self.update_book(book_id, updates))
    
    def delete_book_sync(self, book_id: str) -> bool:
        """Sync wrapper for delete_book."""
        return run_async(self.delete_book(book_id))
    
    def get_book_by_isbn_sync(self, isbn: str) -> Optional[Book]:
        """Sync wrapper for get_book_by_isbn."""
        return run_async(self.get_book_by_isbn(isbn))
