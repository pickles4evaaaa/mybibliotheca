"""
Kuzu-only Application Services.

This module provides service classes that use Kuzu as the primary database,
replacing the Redis approach with a proper graph database architecture.
"""

import os
import asyncio
import uuid
from typing import List, Optional, Dict, Any, TypeVar, Callable, Awaitable, Union, cast
from datetime import datetime, date, timedelta
from dataclasses import asdict
from functools import wraps

from flask import current_app

from .domain.models import Book, User, Author, Publisher, Series, Category, UserBookRelationship, ReadingLog, ReadingStatus
from .infrastructure.kuzu_repositories import KuzuBookRepository, KuzuUserRepository, KuzuPersonRepository
from .infrastructure.kuzu_graph import get_graph_storage
from app.domain.models import ImportMappingTemplate, ReadingStatus

T = TypeVar('T')

def run_async(coro_or_func) -> Any:
    """
    Run an async coroutine synchronously or convert an async function to sync.
    
    Usage:
    - run_async(async_method(args)) - runs a coroutine
    - run_async(async_function) - returns a sync wrapper function
    """
    # If it's a coroutine, run it directly
    if hasattr(coro_or_func, '__await__'):
        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        
        if loop.is_running():
            import concurrent.futures
            with concurrent.futures.ThreadPoolExecutor() as executor:
                def run_in_new_loop():
                    new_loop = asyncio.new_event_loop()
                    try:
                        return new_loop.run_until_complete(coro_or_func)
                    finally:
                        new_loop.close()
                future = executor.submit(run_in_new_loop)
                return future.result()
        else:
            return loop.run_until_complete(coro_or_func)
    
    # If it's a callable (function), return a sync wrapper
    elif callable(coro_or_func):
        @wraps(coro_or_func)
        def wrapper(*args, **kwargs):
            coro = coro_or_func(*args, **kwargs)
            return run_async(coro)
        return wrapper
    
    # Fallback - shouldn't happen
    else:
        raise TypeError(f"Expected coroutine or callable, got {type(coro_or_func)}")


class KuzuBookService:
    """Service for managing books using Kuzu as primary database."""
    
    def __init__(self):
        self.graph_storage = get_graph_storage()
        self.book_repo = KuzuBookRepository()
        self.user_repo = KuzuUserRepository()
        self.person_repo = KuzuPersonRepository()
        
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
        
        return book

    def _create_enriched_book(self, book_data: Dict[str, Any], relationship_data: Dict[str, Any]) -> Book:
        """Create an enriched Book object with user-specific attributes."""
        # Convert book data to Book object
        book = self._dict_to_book(book_data)
        
        # Add user-specific attributes dynamically using setattr
        setattr(book, 'reading_status', relationship_data.get('reading_status', 'plan_to_read'))
        setattr(book, 'ownership_status', relationship_data.get('ownership_status', 'owned'))
        setattr(book, 'start_date', relationship_data.get('start_date'))
        setattr(book, 'finish_date', relationship_data.get('finish_date'))
        setattr(book, 'user_rating', relationship_data.get('user_rating'))
        setattr(book, 'personal_notes', relationship_data.get('personal_notes'))
        setattr(book, 'date_added', relationship_data.get('date_added'))
        setattr(book, 'want_to_read', relationship_data.get('reading_status') == 'plan_to_read')
        setattr(book, 'library_only', relationship_data.get('reading_status') == 'library_only')
        # Note: uid property already returns book.id via the Book model, no need to set it
        
        # Convert date strings back to date objects if needed
        start_date = getattr(book, 'start_date', None)
        if isinstance(start_date, str):
            try:
                setattr(book, 'start_date', datetime.fromisoformat(start_date).date())
            except:
                setattr(book, 'start_date', None)
        
        finish_date = getattr(book, 'finish_date', None)
        if isinstance(finish_date, str):
            try:
                setattr(book, 'finish_date', datetime.fromisoformat(finish_date).date())
            except:
                setattr(book, 'finish_date', None)
        
        return book

    async def create_book(self, domain_book: Book, user_id: str) -> Book:
        """Create a book in Kuzu."""
        try:
            # Debug logging
            print(f"create_book called with domain_book type: {type(domain_book)}")
            print(f"domain_book title: {getattr(domain_book, 'title', 'NO TITLE ATTR')}")
            print(f"user_id: {user_id}")
            
            # Ensure the book has an ID
            if not domain_book.id:
                domain_book.id = str(uuid.uuid4())
                print(f"Generated book ID: {domain_book.id}")
            
            # Set timestamps
            domain_book.created_at = datetime.utcnow()
            domain_book.updated_at = datetime.utcnow()
            
            print(f"About to call book_repo.create")
            # Create the book in Kuzu
            created_book = await self.book_repo.create(domain_book)
            print(f"book_repo.create returned: {created_book}")
            
            # Ensure we have a valid book object
            if not created_book:
                print("❌ book_repo.create returned None")
                raise ValueError("Failed to create book in repository")
            
            # Create user-book relationship
            relationship = UserBookRelationship(
                user_id=str(user_id),
                book_id=domain_book.id,  # Use the original book ID since we know it exists
                reading_status=ReadingStatus.PLAN_TO_READ,
                date_added=datetime.utcnow()
            )
            
            # Serialize relationship data for Kuzu
            rel_data = {
                'user_id': str(user_id),
                'book_id': domain_book.id,
                'reading_status': ReadingStatus.PLAN_TO_READ.value,
                'date_added': datetime.utcnow().isoformat()
            }
            
            print(f"Storing relationship data: {rel_data}")
            
            # Store the relationship using Kuzu's create_relationship method
            rel_success = self.graph_storage.create_relationship(
                'User', user_id, 'OWNS', 'Book', domain_book.id, rel_data
            )
            
            if rel_success:
                print(f"Successfully stored relationship")
            else:
                print(f"Failed to store relationship")
            
            print(f"Created book {domain_book.id} for user {user_id} in Kuzu")
            return domain_book
            
        except Exception as e:
            print(f"Error in create_book: {e}")
            import traceback
            traceback.print_exc()
            raise
    
    async def get_book_by_id(self, book_id: str) -> Optional[Book]:
        """Get a book by ID."""
        book_data = await self.book_repo.get_by_id(book_id)
        if book_data:
            return self._dict_to_book(book_data)
        return None
    
    @run_async
    async def get_books_for_user(self, user_id: str, limit: int = 50, offset: int = 0) -> List[Book]:
        """Get all books for a user with relationship data."""
        # Use Kuzu's query method to get user's books via OWNS relationships
        query = """
        MATCH (u:User {id: $user_id})-[owns:OWNS]->(b:Book)
        RETURN b, owns
        SKIP $offset LIMIT $limit
        """
        
        results = self.graph_storage.query(query, {
            "user_id": user_id,
            "offset": offset,
            "limit": limit
        })
        
        books = []
        for result in results:
            if 'col_0' in result and 'col_1' in result:
                book_data = result['col_0']
                relationship_data = result['col_1']
                
                # Create enriched book with user-specific attributes
                book = self._create_enriched_book(book_data, relationship_data)
                books.append(book)
        
        return books
    
    @run_async
    async def search_books(self, query: str, user_id: str, limit: int = 50) -> List[Book]:
        """Search books for a user."""
        # Get all user books first
        user_books = await self.get_books_for_user(user_id, limit=1000)  # Get all books for filtering
        
        # Simple text search across title and authors
        query_lower = query.lower()
        filtered_books = []
        
        for book in user_books:
            if (query_lower in book.title.lower() or 
                any(query_lower in author.name.lower() for author in book.authors)):
                filtered_books.append(book)
                if len(filtered_books) >= limit:
                    break
        
        return filtered_books
    
    @run_async 
    async def update_book(self, book_id: str, updates: Dict[str, Any], user_id: str) -> Optional[Book]:
        """Update a book."""
        book_data = await self.book_repo.get_by_id(book_id)
        if not book_data:
            return None
        
        # Convert to Book object first
        book = self._dict_to_book(book_data)
        
        # Update fields
        for field, value in updates.items():
            if hasattr(book, field):
                setattr(book, field, value)
        
        book.updated_at = datetime.utcnow()
        
        # Since we don't have an update method, we'll use the graph storage directly
        # Convert book back to dict for storage
        book_dict = {
            'id': book.id,
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
            'created_at': book.created_at,
            'updated_at': book.updated_at
        }
        
        # Update the book node in Kuzu
        success = self.graph_storage.update_node('Book', book_id, book_dict)
        if success:
            return book
        return None
    
    @run_async
    async def delete_book(self, uid: str, user_id: str) -> bool:
        """Delete a book by UID."""
        try:
            print(f"🗑️ [DELETE_BOOK] Deleting book {uid} for user {user_id}")
            
            # First, find the book by UID (which might be stored as 'id' field)
            find_book_query = """
            MATCH (b:Book {id: $uid})
            RETURN b.id as book_id
            """
            
            book_results = self.graph_storage.query(find_book_query, {"uid": uid})
            if not book_results:
                print(f"❌ [DELETE_BOOK] Book {uid} not found")
                return False
            
            book_id = book_results[0]['col_0']  # Use the actual book ID
            print(f"✅ [DELETE_BOOK] Found book with ID: {book_id}")
            
            # Delete user-book relationship using Kuzu query
            delete_rel_query = """
            MATCH (u:User {id: $user_id})-[owns:OWNS]->(b:Book {id: $book_id})
            DELETE owns
            """
            
            self.graph_storage.query(delete_rel_query, {
                "user_id": user_id,
                "book_id": book_id
            })
            print(f"✅ [DELETE_BOOK] Deleted OWNS relationship")
            
            # Check if any other users have this book
            check_query = """
            MATCH (u:User)-[owns:OWNS]->(b:Book {id: $book_id})
            RETURN COUNT(owns) as count
            """
            
            results = self.graph_storage.query(check_query, {"book_id": book_id})
            other_relationships_count = 0
            if results and 'col_0' in results[0]:
                other_relationships_count = results[0]['col_0']
            
            print(f"🔍 [DELETE_BOOK] Other users with this book: {other_relationships_count}")
            
            # If no other users have this book, delete the book itself and all its relationships
            if other_relationships_count == 0:
                delete_book_query = """
                MATCH (b:Book {id: $book_id})
                DETACH DELETE b
                """
                self.graph_storage.query(delete_book_query, {"book_id": book_id})
                print(f"✅ [DELETE_BOOK] Deleted book node and all relationships")
            
            return True
            
        except Exception as e:
            print(f"❌ [DELETE_BOOK] Error deleting book {uid}: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    @run_async
    async def get_books_with_sharing_users(self, days_back: int = 30, limit: int = 20) -> List[Book]:
        """Get books from users who share reading activity, finished in the last N days."""
        # Get all users who share reading activity
        sharing_users = await self.user_repo.get_all()
        sharing_user_ids = []
        
        for user_dict in sharing_users:
            if (user_dict.get('share_reading_activity', False) and 
                user_dict.get('is_active', True)):
                sharing_user_ids.append(user_dict.get('id'))
        
        if not sharing_user_ids:
            return []
        
        # Get all books for sharing users using graph queries
        all_books = []
        for user_id in sharing_user_ids:
            books = await self.get_books_for_user(user_id, limit=1000)  # Use service method
            all_books.extend(books)
        
        # Filter for finished books in the specified time range
        cutoff_date = date.today() - timedelta(days=days_back)
        finished_books = [
            book for book in all_books 
            if hasattr(book, 'finish_date') and book.finish_date and book.finish_date >= cutoff_date
        ]
        
        # Sort by finish date descending and limit
        finished_books.sort(key=lambda b: getattr(b, 'finish_date', date.min) or date.min, reverse=True)
        return finished_books[:limit]
    
    @run_async
    async def get_currently_reading_shared(self, limit: int = 20) -> List[Book]:
        """Get currently reading books from users who share current reading."""
        # Get all users who share current reading
        sharing_users = await self.user_repo.get_all()
        sharing_user_ids = []
        
        for user_dict in sharing_users:
            if (user_dict.get('share_current_reading', False) and 
                user_dict.get('is_active', True)):
                sharing_user_ids.append(user_dict.get('id'))
        
        if not sharing_user_ids:
            return []
        
        # Get all books for sharing users using graph queries
        all_books = []
        for user_id in sharing_user_ids:
            books = await self.get_books_for_user(user_id, limit=1000)  # Use service method
            all_books.extend(books)
        
        # Filter for currently reading (has start_date but no finish_date)
        currently_reading = [
            book for book in all_books 
            if (hasattr(book, 'start_date') and book.start_date and 
                (not hasattr(book, 'finish_date') or not book.finish_date))
        ]
        
        # Sort by start date descending and limit
        currently_reading.sort(key=lambda b: getattr(b, 'start_date', date.min) or date.min, reverse=True)
        return currently_reading[:limit]
    
    @run_async
    async def get_book_by_isbn_for_user(self, isbn: str, user_id: str) -> Optional[Book]:
        """Get a book by ISBN for a specific user."""
        books = await self.get_books_for_user(user_id, limit=1000)  # Get enriched books
        for book in books:
            if (hasattr(book, 'isbn13') and book.isbn13 == isbn) or \
               (hasattr(book, 'isbn10') and book.isbn10 == isbn):
                return book
        return None
    
    @run_async
    async def get_book_by_id_for_user(self, book_id: str, user_id: str) -> Optional[Book]:
        """Get a specific book for a user with relationship data."""
        # Use Kuzu's query method to get the book with user relationship data
        query = """
        MATCH (u:User {id: $user_id})-[owns:OWNS]->(b:Book {id: $book_id})
        RETURN b, owns
        """
        
        results = self.graph_storage.query(query, {
            "user_id": user_id,
            "book_id": book_id
        })
        
        if not results:
            return None
            
        result = results[0]
        if 'col_0' not in result or 'col_1' not in result:
            return None
            
        book_data = result['col_0']
        relationship_data = result['col_1']
        
        # Create enriched book with user-specific attributes
        book = self._create_enriched_book(book_data, relationship_data)
        
        # Load custom metadata for this user-book combination
        try:
            custom_metadata = {}
            # Query for HAS_CUSTOM_FIELD relationships from this user for this book
            custom_query = """
            MATCH (u:User {id: $user_id})-[r:HAS_CUSTOM_FIELD]->(cf:CustomField)
            WHERE r.book_id = $book_id
            RETURN cf.name, cf.value
            """
            
            custom_results = self.graph_storage.query(custom_query, {
                "user_id": user_id,
                "book_id": book_id
            })
            
            for result in custom_results:
                # Extract field name and value from result
                if len(result) >= 2:
                    field_name = list(result.values())[0]  # First column
                    field_value = list(result.values())[1]  # Second column
                    if field_name and field_value:
                        custom_metadata[field_name] = field_value
                        
            setattr(book, 'custom_metadata', custom_metadata)
            print(f"🔍 [LOAD_CUSTOM_META] Loaded {len(custom_metadata)} custom fields for book {book_id}, user {user_id}: {custom_metadata}")
        except Exception as e:
            print(f"❌ Error loading custom metadata for book {book_id}, user {user_id}: {e}")
            import traceback
            traceback.print_exc()
            setattr(book, 'custom_metadata', {})
                
        return book

    def get_book_categories_sync(self, book_id: str) -> List[Dict[str, Any]]:
        """Get categories for a book."""
        try:
            book_repo = KuzuBookRepository()
            # Use run_async to call the async method
            categories = run_async(book_repo.get_book_categories)(book_id)
            
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
            print(f"❌ Error getting book categories for {book_id}: {e}")
            return []

    # Sync wrappers for Flask compatibility
    def create_book_sync(self, domain_book: Book, user_id: str) -> Book:
        """Sync wrapper for create_book."""
        return run_async(self.create_book)(domain_book, user_id)
    
    def get_book_by_id_sync(self, book_id: str) -> Optional[Book]:
        """Sync wrapper for get_book_by_id."""
        return run_async(self.get_book_by_id)(book_id)
    
    def get_book_by_uid_sync(self, uid: str, user_id: str) -> Optional[Book]:
        """Get a book by UID with user overlay data - sync wrapper."""
        # Use similar query to get_all_books_with_user_overlay but for single book
        query = """
        MATCH (b:Book {id: $book_id})
        OPTIONAL MATCH (u:User {id: $user_id})-[owns:OWNS]->(b)
        RETURN b, owns
        """
        
        results = self.graph_storage.query(query, {
            "user_id": user_id,
            "book_id": uid
        })
        
        if not results:
            return None
            
        result = results[0]
        if 'col_0' not in result:
            return None
            
        book_data = result['col_0']
        relationship_data = result.get('col_1', {}) or {}
        
        # Create enriched book with user-specific attributes
        book = self._create_enriched_book(book_data, relationship_data)
        
        return book
    
    def get_user_book_sync(self, user_id: str, book_id: str) -> Optional[Dict[str, Any]]:
        """Get user's book by book ID - alias for get_book_by_uid_sync for compatibility."""
        result = self.get_book_by_uid_sync(book_id, user_id)
        if result:
            # Convert Book object to dictionary if needed
            if hasattr(result, '__dict__'):
                return result.__dict__
            elif isinstance(result, dict):
                return result
            else:
                # Try to convert to dict
                try:
                    return vars(result)
                except:
                    # Fallback - create basic dict
                    return {
                        'id': getattr(result, 'id', book_id),
                        'title': getattr(result, 'title', 'Unknown'),
                        'custom_metadata': getattr(result, 'custom_metadata', {})
                    }
        return None
    
    def get_user_books_sync(self, user_id: str) -> List[Book]:
        """Sync wrapper for get_books_for_user."""
        return self.get_books_for_user(user_id)
    
    def search_books_sync(self, query: str, user_id: str) -> List[Book]:
        """Sync wrapper for search_books."""
        return self.search_books(query, user_id)
    
    def update_book_sync(self, book_id: str, user_id: str, **kwargs) -> Optional[Book]:
        """Sync wrapper for update_book."""
        return self.update_book(book_id, kwargs, user_id)
    
    def delete_book_sync(self, book_id: str, user_id: str) -> bool:
        """Sync wrapper for delete_book."""
        return self.delete_book(book_id, user_id)
    
    def get_books_with_sharing_users_sync(self, days_back: int = 30, limit: int = 20) -> List[Book]:
        """Sync wrapper for get_books_with_sharing_users."""
        return self.get_books_with_sharing_users(days_back, limit)
    
    def get_currently_reading_shared_sync(self, limit: int = 20) -> List[Book]:
        """Sync wrapper for get_currently_reading_shared."""
        return self.get_currently_reading_shared(limit)
    
    def get_book_by_isbn_for_user_sync(self, isbn: str, user_id: str) -> Optional[Book]:
        """Sync wrapper for get_book_by_isbn_for_user."""
        return self.get_book_by_isbn_for_user(isbn, user_id)
    
    def bulk_import_books_sync(self, user_id: str, csv_path: str, default_status: str) -> str:
        """Sync wrapper for bulk import books from CSV file.
        
        Args:
            user_id: The user ID to import books for
            csv_path: Path to the CSV file containing book data
            default_status: Default status to assign to imported books
            
        Returns:
            A task ID representing the bulk import operation
        """
        import uuid
        import csv
        import os
        
        task_id = str(uuid.uuid4())
        
        try:
            # Simple synchronous CSV processing for now
            books_imported = 0
            rows_processed = 0
            
            print(f"Starting bulk import from: {csv_path}")
            print(f"File exists: {os.path.exists(csv_path)}")
            
            if os.path.exists(csv_path):
                with open(csv_path, 'r', encoding='utf-8') as file:
                    reader = csv.DictReader(file)
                    print(f"CSV headers: {reader.fieldnames}")
                    
                    for row in reader:
                        rows_processed += 1
                        print(f"Processing row {rows_processed}: {row}")
                        
                        try:
                            # Handle different CSV formats
                            title = ''
                            author = ''
                            isbn = ''
                            
                            # Check if this is a headerless ISBN-only file
                            if (reader.fieldnames and len(reader.fieldnames) == 1 and 
                                reader.fieldnames[0].startswith('978')):
                                # This is likely an ISBN-only file where the "header" is actually the first ISBN
                                # Get the ISBN from the single column
                                first_col = list(row.values())[0] if row else reader.fieldnames[0]
                                if rows_processed == 1:
                                    # First row, use the "header" as the first ISBN
                                    isbn = reader.fieldnames[0]
                                else:
                                    # Subsequent rows
                                    isbn = first_col
                                print(f"ISBN-only format detected. ISBN: '{isbn}'")
                            else:
                                # Handle standard CSV with headers
                                # Try different column name variations for title
                                title = (row.get('Title') or row.get('title') or 
                                        row.get('Book Title') or row.get('book_title') or '').strip()
                                
                                # Try different column name variations for author
                                author = (row.get('Author') or row.get('author') or 
                                         row.get('Authors') or row.get('authors') or
                                         row.get('Author l-f') or '').strip()
                                
                                # Try different column name variations for ISBN
                                isbn = (row.get('ISBN') or row.get('isbn') or 
                                       row.get('ISBN13') or row.get('isbn13') or
                                       row.get('ISBN/UID') or row.get('isbn_uid') or '').strip()
                                
                                # Clean up ISBN (remove quotes and equals signs from Goodreads export)
                                if isbn:
                                    isbn = isbn.replace('="', '').replace('"', '').replace('=', '')
                            
                            print(f"Extracted - Title: '{title}', Author: '{author}', ISBN: '{isbn}'")
                            
                            # For ISBN-only imports, we'll try to fetch book data from external APIs
                            if isbn and not title:
                                print(f"ISBN-only import detected. Will try to fetch book data for ISBN: {isbn}")
                                # We have ISBN but no title/author, so we'll create a minimal book
                                # and let the system fetch metadata later
                                title = f"Book {isbn}"  # Temporary title
                                author = "Unknown Author"  # Temporary author
                            
                            if title:  # Only import if we have at least a title
                                # Create a basic Book object for import
                                book = Book(
                                    title=title,
                                    isbn13=isbn if len(isbn) == 13 else None,
                                    isbn10=isbn if len(isbn) == 10 else None
                                )
                                
                                print(f"Created book object: {book}")
                                
                                # Use existing create_book method
                                created_book = self.create_book_sync(book, user_id)
                                if created_book:
                                    books_imported += 1
                                    print(f"Successfully created book: {created_book.id}")
                                else:
                                    print(f"Failed to create book: {book.title}")
                            else:
                                print(f"Skipping row - no title or ISBN found")
                        except Exception as e:
                            print(f"Error importing book row {rows_processed}: {e}")
                            import traceback
                            traceback.print_exc()
                            continue
            else:
                print(f"CSV file not found: {csv_path}")
            
            print(f"Bulk import completed. Processed {rows_processed} rows, imported {books_imported} books with task ID: {task_id}")
            return task_id
            
        except Exception as e:
            print(f"Bulk import error: {e}")
            import traceback
            traceback.print_exc()
            return task_id

    # Category management methods
    @run_async
    async def list_all_categories(self, user_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get all categories."""
        try:
            query = """
            MATCH (c:Category)
            RETURN c
            ORDER BY c.name ASC
            """
            
            results = self.graph_storage.query(query)
            
            categories = []
            for result in results:
                if 'col_0' in result:
                    categories.append(result['col_0'])
            
            return categories
        except Exception as e:
            print(f"❌ Error getting all categories: {e}")
            return []

    def list_all_categories_sync(self, user_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get all categories (sync version)."""
        return self.list_all_categories(user_id)

    @run_async
    async def list_all_persons(self, user_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get all persons."""
        try:
            return await self.book_repo.get_all_persons()
        except Exception as e:
            print(f"❌ Error getting all persons: {e}")
            return []

    def list_all_persons_sync(self, user_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get all persons (sync version)."""
        return self.list_all_persons(user_id)

    @run_async
    async def get_person_by_id(self, person_id: str) -> Optional[Dict[str, Any]]:
        """Get a person by ID."""
        try:
            from .infrastructure.kuzu_repositories import KuzuPersonRepository
            person_repo = KuzuPersonRepository()
            return await person_repo.get_by_id(person_id)
        except Exception as e:
            print(f"❌ Error getting person by ID {person_id}: {e}")
            return None

    def get_person_by_id_sync(self, person_id: str) -> Optional[Dict[str, Any]]:
        """Get a person by ID (sync version)."""
        try:
            from .infrastructure.kuzu_graph import get_kuzu_database
            db = get_kuzu_database()
            
            print(f"🔍 [DEBUG] get_person_by_id_sync: Looking for person_id: '{person_id}'")
            
            # First, let's see what persons exist in the database
            debug_query = "MATCH (p:Person) RETURN p.id, p.name LIMIT 10"
            debug_results = db.query(debug_query)
            print(f"🔍 [DEBUG] get_person_by_id_sync: All persons in DB: {debug_results}")
            
            query = """
            MATCH (p:Person {id: $person_id})
            RETURN p
            LIMIT 1
            """
            
            print(f"🔍 [DEBUG] get_person_by_id_sync: Executing query: {query}")
            print(f"🔍 [DEBUG] get_person_by_id_sync: Query parameters: {{'person_id': person_id}}")
            
            results = db.query(query, {"person_id": person_id})
            
            print(f"🔍 [DEBUG] get_person_by_id_sync: Raw query results: {results}")
            print(f"🔍 [DEBUG] get_person_by_id_sync: Results type: {type(results)}")
            print(f"🔍 [DEBUG] get_person_by_id_sync: Results length: {len(results) if results else 'None'}")
            
            if results and len(results) > 0:
                print(f"🔍 [DEBUG] get_person_by_id_sync: First result: {results[0]}")
                print(f"🔍 [DEBUG] get_person_by_id_sync: First result keys: {list(results[0].keys()) if isinstance(results[0], dict) else 'Not a dict'}")
                
                # Try different possible key formats
                if 'result' in results[0]:
                    person_data = dict(results[0]['result'])
                    print(f"🔍 [DEBUG] get_person_by_id_sync: Person data from 'result': {person_data}")
                    return person_data
                elif 'col_0' in results[0]:
                    person_data = dict(results[0]['col_0'])
                    print(f"🔍 [DEBUG] get_person_by_id_sync: Person data from 'col_0': {person_data}")
                    return person_data
                elif 'p' in results[0]:
                    person_data = dict(results[0]['p'])
                    print(f"🔍 [DEBUG] get_person_by_id_sync: Person data from 'p': {person_data}")
                    return person_data
                else:
                    print(f"🔍 [DEBUG] get_person_by_id_sync: No expected key found in results[0]. Available keys: {list(results[0].keys())}")
            else:
                print(f"🔍 [DEBUG] get_person_by_id_sync: No results found")
            
            return None
            
        except Exception as e:
            print(f"❌ Error getting person by ID {person_id}: {e}")
            import traceback
            print(f"❌ Traceback: {traceback.format_exc()}")
            return None

    @run_async
    async def get_category_by_id(self, category_id: str, user_id: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """Get a category by ID."""
        try:
            query = """
            MATCH (c:Category {id: $category_id})
            RETURN c
            """
            
            results = self.graph_storage.query(query, {"category_id": category_id})
            
            if results and 'col_0' in results[0]:
                return results[0]['col_0']
            return None
        except Exception as e:
            print(f"❌ Error getting category by ID {category_id}: {e}")
            return None

    def get_category_by_id_sync(self, category_id: str, user_id: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """Get a category by ID (sync version)."""
        return self.get_category_by_id(category_id, user_id)

    @run_async
    async def get_child_categories(self, parent_id: str) -> List[Dict[str, Any]]:
        """Get child categories for a parent category."""
        try:
            # Query for categories that have this parent_id
            query = """
            MATCH (c:Category)
            WHERE c.parent_id = $parent_id
            RETURN c
            ORDER BY c.name ASC
            """
            
            results = self.graph_storage.query(query, {"parent_id": parent_id})
            
            categories = []
            for result in results:
                if 'col_0' in result:
                    categories.append(result['col_0'])
            
            return categories
        except Exception as e:
            print(f"❌ Error getting child categories for {parent_id}: {e}")
            return []

    def get_child_categories_sync(self, parent_id: str) -> List[Dict[str, Any]]:
        """Get child categories for a parent category (sync version)."""
        return self.get_child_categories(parent_id)

    @run_async
    async def get_category_children(self, category_id: str, user_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get children of a category (alias for get_child_categories)."""
        return await self.get_child_categories(category_id)

    def get_category_children_sync(self, category_id: str, user_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get children of a category (sync version)."""
        return self.get_category_children(category_id, user_id)

    @run_async
    async def get_books_by_category(self, category_id: str, user_id: Optional[str] = None, include_subcategories: bool = False) -> List[Dict[str, Any]]:
        """Get books in a category."""
        try:
            from .infrastructure.kuzu_graph import get_kuzu_database
            db = get_kuzu_database()
            
            if include_subcategories:
                # Get all descendant categories
                descendant_ids = await self._get_all_descendant_categories(category_id)
                descendant_ids.append(category_id)  # Include the category itself
                
                # Build query for multiple categories
                query = """
                MATCH (b:Book)-[:BELONGS_TO]->(c:Category)
                WHERE c.id IN $category_ids
                RETURN DISTINCT b
                ORDER BY b.title ASC
                """
                
                results = db.query(query, {"category_ids": descendant_ids})
            else:
                # Query for books in this specific category
                query = """
                MATCH (b:Book)-[:BELONGS_TO]->(c:Category {id: $category_id})
                RETURN b
                ORDER BY b.title ASC
                """
                
                results = db.query(query, {"category_id": category_id})
            
            books = []
            for result in results:
                if 'result' in result:
                    books.append(dict(result['result']))
                elif 'col_0' in result:
                    books.append(dict(result['col_0']))
            
            return books
        except Exception as e:
            print(f"❌ Error getting books by category {category_id}: {e}")
            return []

    def get_books_by_category_sync(self, category_id: str, user_id: Optional[str] = None, include_subcategories: bool = False) -> List[Dict[str, Any]]:
        """Get books in a category (sync version)."""
        return self.get_books_by_category(category_id, user_id, include_subcategories)

    @run_async
    async def _get_all_descendant_categories(self, category_id: str) -> List[str]:
        """Get all descendant category IDs recursively."""
        try:
            from .infrastructure.kuzu_graph import get_kuzu_database
            db = get_kuzu_database()
            
            # Use recursive CTE to get all descendants
            query = """
            WITH RECURSIVE descendants AS (
                SELECT c.id as category_id, c.name as category_name
                FROM Category c
                WHERE c.id = $category_id
                
                UNION ALL
                
                SELECT child.id as category_id, child.name as category_name
                FROM Category child
                INNER JOIN descendants d ON child.parent_id = d.category_id
            )
            SELECT category_id FROM descendants WHERE category_id != $category_id
            """
            
            results = db.query(query, {"category_id": category_id})
            
            descendant_ids = []
            for result in results:
                if 'result' in result:
                    descendant_ids.append(result['result'])
                elif 'col_0' in result:
                    descendant_ids.append(result['col_0'])
            
            return descendant_ids
        except Exception as e:
            print(f"❌ Error getting descendant categories for {category_id}: {e}")
            return []

    @run_async
    async def get_all_books_with_user_overlay(self, user_id: str) -> List[Dict[str, Any]]:
        """Get all books with user overlay data."""
        try:
            from .infrastructure.kuzu_graph import get_kuzu_database
            db = get_kuzu_database()
            
            # Get all books and optionally join with user relationship data
            query = """
            MATCH (b:Book)
            OPTIONAL MATCH (u:User {id: $user_id})-[owns:OWNS]->(b)
            RETURN b,
                   owns.reading_status as reading_status,
                   owns.ownership_status as ownership_status,
                   owns.start_date as start_date,
                   owns.finish_date as finish_date,
                   owns.user_rating as user_rating,
                   owns.user_review as user_review,
                   owns.personal_notes as user_notes,
                   owns.date_added as date_added
            ORDER BY b.title ASC
            """
            
            results = db.query(query, {"user_id": user_id})
            
            books = []
            for result in results:
                book_data = {}
                
                # Extract book data
                if 'b' in result:
                    book_data = dict(result['b'])
                elif 'col_0' in result:
                    book_data = dict(result['col_0'])
                
                # Add user-specific overlay data
                book_data['reading_status'] = result.get('reading_status')
                book_data['ownership_status'] = result.get('ownership_status')
                book_data['start_date'] = result.get('start_date')
                book_data['finish_date'] = result.get('finish_date')
                book_data['user_rating'] = result.get('user_rating')
                book_data['user_review'] = result.get('user_review')
                book_data['user_notes'] = result.get('user_notes')
                book_data['date_added'] = result.get('date_added')
                
                # Ensure uid is available as alias for id (for template compatibility)
                if 'id' in book_data:
                    book_data['uid'] = book_data['id']
                
                books.append(book_data)
            
            return books
        except Exception as e:
            print(f"❌ Error getting all books with user overlay: {e}")
            return []

    def get_all_books_with_user_overlay_sync(self, user_id: str) -> List[Dict[str, Any]]:
        """Get all books with user overlay data (sync version)."""
        return self.get_all_books_with_user_overlay(user_id)

    @run_async
    async def get_books_by_person(self, person_id: str, user_id: Optional[str] = None) -> Dict[str, List[Dict[str, Any]]]:
        """Get books associated with a person, grouped by contribution type."""
        try:
            from .infrastructure.kuzu_graph import get_kuzu_database
            db = get_kuzu_database()
            
            # Query to get books and their relationship types with the person
            if user_id:
                # Include user-specific data if user_id is provided
                query = """
                MATCH (p:Person {id: $person_id})-[r:AUTHORED]->(b:Book)
                OPTIONAL MATCH (u:User {id: $user_id})-[owns:OWNS]->(b)
                RETURN b, r.role as contribution_type, 
                       owns.reading_status as reading_status,
                       owns.ownership_status as ownership_status,
                       owns.start_date as start_date,
                       owns.finish_date as finish_date
                ORDER BY contribution_type, b.title
                """
                results = db.query(query, {"person_id": person_id, "user_id": user_id})
            else:
                # Just get the books without user-specific data
                query = """
                MATCH (p:Person {id: $person_id})-[r:AUTHORED]->(b:Book)
                RETURN b, r.role as contribution_type
                ORDER BY contribution_type, b.title
                """
                results = db.query(query, {"person_id": person_id})
            
            # Group books by contribution type
            books_by_type = {}
            for result in results:
                # Extract the book data
                book_data = {}
                contribution_type = 'author'  # default
                
                if 'b' in result:
                    book_data = dict(result['b'])
                elif hasattr(result, 'b'):
                    book_data = dict(getattr(result, 'b'))
                
                if 'contribution_type' in result:
                    contribution_type = result['contribution_type'] or 'author'
                elif hasattr(result, 'contribution_type'):
                    contribution_type = getattr(result, 'contribution_type', 'author') or 'author'
                
                # Add user-specific data if available
                if user_id:
                    book_data['reading_status'] = result.get('reading_status')
                    book_data['ownership_status'] = result.get('ownership_status') 
                    book_data['start_date'] = result.get('start_date')
                    book_data['finish_date'] = result.get('finish_date')
                
                # Initialize the contribution type list if not exists
                if contribution_type not in books_by_type:
                    books_by_type[contribution_type] = []
                
                books_by_type[contribution_type].append(book_data)
            
            return books_by_type
            
        except Exception as e:
            print(f"❌ Error getting books by person {person_id}: {e}")
            # If the AUTHORED table doesn't exist, return empty structure
            # This handles the case where the database schema is incomplete
            if "AUTHORED does not exist" in str(e):
                print(f"⚠️ AUTHORED table missing - returning empty contributions for person {person_id}")
                return {}
            # For other errors, also return empty to avoid breaking the UI
            return {}

    def get_books_by_person_sync(self, person_id: str, user_id: Optional[str] = None) -> Dict[str, List[Dict[str, Any]]]:
        """Get books associated with a person, grouped by contribution type (sync version)."""
        return self.get_books_by_person(person_id, user_id)


# Create service instances
kuzu_book_service = KuzuBookService()

class KuzuImportMappingRepository:
    """Repository for managing import mapping templates in Kuzu."""
    
    def __init__(self, storage):
        self.storage = storage
    
    async def create(self, template: ImportMappingTemplate) -> ImportMappingTemplate:
        """Create a new import mapping template."""
        template.id = str(uuid.uuid4())  # Generate a new ID for the template
        template.created_at = datetime.utcnow()
        template.updated_at = datetime.utcnow()
        
        template_key = f"template:{template.id}"
        
        # Serialize the template data
        template_data = template.to_dict()
        
        # Save to Kuzu
        await self.storage.set_json(template_key, template_data)
        
        return template

    async def get_by_id(self, template_id: str) -> Optional[ImportMappingTemplate]:
        """Get an import mapping template by ID."""
        template_key = f"template:{template_id}"
        template_data = await self.storage.get_json(template_key)
        
        if not template_data:
            return None
        
        # Deserialize the template data
        template_data['id'] = template_id
        template = ImportMappingTemplate(**template_data)
        return template

    async def update(self, template: ImportMappingTemplate) -> ImportMappingTemplate:
        """Update an existing template."""
        template_key = f"template:{template.id}"
        
        # Check if the template exists
        if not await self.storage.exists(template_key):
            raise ValueError(f"Template with id {template.id} not found")

        # Serialize the updated template data
        template.updated_at = datetime.utcnow()
        template_data = template.to_dict()

        # Save the updated data to Kuzu
        await self.storage.set_json(template_key, template_data)
        
        return template

    async def delete(self, template_id: str) -> bool:
        """Delete an import mapping template by ID."""
        template_key = f"template:{template_id}"
        result = await self.storage.delete(template_key)
        return result

    async def get_all(self) -> List[ImportMappingTemplate]:
        """Get all import mapping templates."""
        template_keys = await self.storage.scan_keys("template:*")
        templates = []
        
        for key in template_keys:
            template_data = await self.storage.get_json(key)
            if template_data:
                template_data['id'] = key.split(':')[1]  # Extract ID from key
                template = ImportMappingTemplate(**template_data)
                templates.append(template)
        
        return templates


class KuzuJobService:
    """Service for managing import jobs using Kuzu graph database."""
    
    def __init__(self):
        self._storage = None
    
    @property
    def storage(self):
        """Lazy initialization of storage."""
        if self._storage is None:
            self._storage = get_graph_storage()
        return self._storage
    
    @run_async
    async def store_job(self, task_id: str, job_data: dict) -> bool:
        """Store import job data in Kuzu."""
        try:
            import json
            from datetime import datetime, timedelta
            
            # Set expiration time (24 hours from now)
            expires_at = datetime.utcnow() + timedelta(hours=24)
            
            # Prepare job data for Kuzu storage
            job_record = {
                'id': f"job_{task_id}",
                'task_id': task_id,
                'user_id': job_data.get('user_id', ''),
                'csv_file_path': job_data.get('csv_file_path', ''),
                'field_mappings': json.dumps(job_data.get('field_mappings', {})),
                'default_reading_status': job_data.get('default_reading_status', ''),
                'duplicate_handling': job_data.get('duplicate_handling', 'skip'),
                'custom_fields_enabled': job_data.get('custom_fields_enabled', False),
                'status': job_data.get('status', 'pending'),
                'processed': job_data.get('processed', 0),
                'success': job_data.get('success', 0),
                'errors': job_data.get('errors', 0),
                'total': job_data.get('total', 0),
                'start_time': datetime.fromisoformat(job_data['start_time']) if isinstance(job_data.get('start_time'), str) else job_data.get('start_time', datetime.utcnow()),
                'end_time': datetime.fromisoformat(job_data['end_time']) if job_data.get('end_time') and isinstance(job_data.get('end_time'), str) else job_data.get('end_time'),
                'current_book': job_data.get('current_book', ''),
                'error_messages': json.dumps(job_data.get('error_messages', [])),
                'recent_activity': json.dumps(job_data.get('recent_activity', [])),
                'created_at': datetime.utcnow(),
                'updated_at': datetime.utcnow(),
                'expires_at': expires_at
            }
            
            # Use the store_node method to create the job
            success = self.storage.store_node('ImportJob', f"job_{task_id}", job_record)
            
            if success:
                print(f"✅ Stored job {task_id} in Kuzu")
            else:
                print(f"❌ Failed to store job {task_id} in Kuzu")
            return success
            
        except Exception as e:
            print(f"❌ Error storing job {task_id} in Kuzu: {e}")
            return False
    
    @run_async
    async def get_job(self, task_id: str) -> Optional[dict]:
        """Retrieve import job data from Kuzu."""
        try:
            import json
            
            # Get the job node by task_id
            job_record = self.storage.get_node('ImportJob', f"job_{task_id}")
            
            if job_record:
                # Check if the job has expired
                expires_at = job_record.get('expires_at')
                if expires_at and isinstance(expires_at, datetime) and expires_at < datetime.utcnow():
                    print(f"❌ Job {task_id} has expired")
                    return None
                
                # Convert back to the expected job data format
                job_data = {
                    'task_id': job_record.get('task_id'),
                    'user_id': job_record.get('user_id'),
                    'csv_file_path': job_record.get('csv_file_path'),
                    'field_mappings': json.loads(job_record.get('field_mappings', '{}')),
                    'default_reading_status': job_record.get('default_reading_status'),
                    'duplicate_handling': job_record.get('duplicate_handling'),
                    'custom_fields_enabled': job_record.get('custom_fields_enabled', False),
                    'status': job_record.get('status'),
                    'processed': job_record.get('processed', 0),
                    'success': job_record.get('success', 0),
                    'errors': job_record.get('errors', 0),
                    'total': job_record.get('total', 0),
                    'start_time': None,
                    'end_time': None,
                    'current_book': job_record.get('current_book'),
                    'error_messages': json.loads(job_record.get('error_messages', '[]')),
                    'recent_activity': json.loads(job_record.get('recent_activity', '[]'))
                }
                
                # Handle datetime fields safely
                start_time = job_record.get('start_time')
                if start_time and hasattr(start_time, 'isoformat'):
                    job_data['start_time'] = start_time.isoformat()
                elif isinstance(start_time, str):
                    job_data['start_time'] = start_time
                    
                end_time = job_record.get('end_time')
                if end_time and hasattr(end_time, 'isoformat'):
                    job_data['end_time'] = end_time.isoformat()
                elif isinstance(end_time, str):
                    job_data['end_time'] = end_time
                
                print(f"✅ Retrieved job {task_id} from Kuzu")
                return job_data
            else:
                print(f"❌ Job {task_id} not found in Kuzu")
                return None
                
        except Exception as e:
            print(f"❌ Error retrieving job {task_id} from Kuzu: {e}")
            return None
    
    @run_async
    async def update_job(self, task_id: str, updates: dict) -> bool:
        """Update specific fields in an import job stored in Kuzu."""
        try:
            import json
            from datetime import datetime
            
            # Prepare updates by filtering out system fields and serializing JSON fields
            clean_updates = {}
            
            # Define fields that should be datetime objects
            datetime_fields = {'start_time', 'end_time', 'created_at', 'updated_at', 'expires_at'}
            
            for key, value in updates.items():
                # Skip system fields
                if key in ['id', '_id']:
                    continue
                    
                # Handle JSON fields
                if key in ['error_messages', 'recent_activity', 'field_mappings']:
                    if isinstance(value, (list, dict)):
                        value = json.dumps(value)
                elif key in datetime_fields and isinstance(value, str) and value:
                    # Convert ISO timestamp strings to datetime objects
                    try:
                        if value.endswith('Z'):
                            value = value[:-1] + '+00:00'
                        value = datetime.fromisoformat(value)
                    except (ValueError, TypeError):
                        # If conversion fails, keep as string and let Kuzu handle it
                        pass
                        
                clean_updates[key] = value
            
            # Add updated timestamp
            clean_updates['updated_at'] = datetime.utcnow()
            
            # Use update_node method with only the clean updates
            success = self.storage.update_node('ImportJob', f"job_{task_id}", clean_updates)
            
            if success:
                print(f"✅ Updated job {task_id} in Kuzu with: {list(updates.keys())}")
            else:
                print(f"❌ Failed to update job {task_id} in Kuzu")
            return success
            
        except Exception as e:
            print(f"❌ Error updating job {task_id} in Kuzu: {e}")
            return False
    
    @run_async
    async def delete_expired_jobs(self) -> int:
        """Delete expired import jobs from Kuzu."""
        try:
            # Use execute_cypher to delete expired jobs
            query = """
            MATCH (j:ImportJob)
            WHERE j.expires_at < $current_time
            DELETE j
            """
            
            result = self.storage.execute_cypher(query, {
                'current_time': datetime.utcnow()
            })
            
            print(f"✅ Cleaned up expired jobs from Kuzu")
            return 0  # Kuzu doesn't return delete count easily
            
        except Exception as e:
            print(f"❌ Error cleaning up expired jobs from Kuzu: {e}")
            return 0


# Global service instances
job_service = KuzuJobService()

class KuzuUserBookService:
    """Service for managing user-book relationships in Kuzu."""
    
    def __init__(self):
        self.graph_storage = get_graph_storage()
    
    @run_async
    async def update_user_book(self, user_id: str, book_id: str, custom_metadata: Optional[Dict[str, Any]] = None, **kwargs) -> bool:
        """Update user-book relationship with custom metadata."""
        try:
            if not custom_metadata:
                return True  # Nothing to update
            
            # For each custom field, create or update the HAS_CUSTOM_FIELD relationship
            for field_name, field_value in custom_metadata.items():
                if field_value is not None and field_value != '':
                    # Create a CustomField node for this field
                    field_node_id = f"custom_{user_id}_{book_id}_{field_name}"
                    
                    # Store the custom field
                    field_data = {
                        'id': field_node_id,
                        'name': field_name,
                        'field_type': 'text',  # Default to text for now
                        'value': str(field_value),
                        'created_at': datetime.utcnow()
                    }
                    
                    success = self.graph_storage.store_node('CustomField', field_node_id, field_data)
                    if success:
                        # Create the HAS_CUSTOM_FIELD relationship
                        rel_props = {
                            'book_id': book_id,
                            'field_name': field_name
                        }
                        self.graph_storage.create_relationship('User', user_id, 'HAS_CUSTOM_FIELD', 'CustomField', field_node_id, rel_props)
                        print(f"✅ Saved custom field {field_name} = {field_value} for user {user_id}, book {book_id}")
                    else:
                        print(f"❌ Failed to save custom field {field_name}")
                        
            return True
            
        except Exception as e:
            print(f"❌ Error updating user book custom metadata: {e}")
            import traceback
            traceback.print_exc()
            return False

    def update_user_book_sync(self, user_id: str, book_id: str, custom_metadata: Optional[Dict[str, Any]] = None, **kwargs) -> bool:
        """Synchronous version of update_user_book."""
        try:
            print(f"🔍 [UPDATE_USER_BOOK] Called with user_id={user_id}, book_id={book_id}, custom_metadata={custom_metadata}")
            
            if not custom_metadata:
                print(f"🔍 [UPDATE_USER_BOOK] No custom metadata to update")
                return True  # Nothing to update
            
            # Verify user and book exist
            user_exists = self.graph_storage.get_node('User', user_id)
            book_exists = self.graph_storage.get_node('Book', book_id)
            print(f"🔍 [UPDATE_USER_BOOK] User exists: {user_exists is not None}, Book exists: {book_exists is not None}")
            
            if not user_exists:
                print(f"❌ [UPDATE_USER_BOOK] User {user_id} not found")
                return False
            if not book_exists:
                print(f"❌ [UPDATE_USER_BOOK] Book {book_id} not found")
                return False
            
            # For each custom field, create or update the HAS_CUSTOM_FIELD relationship
            for field_name, field_value in custom_metadata.items():
                print(f"🔍 [UPDATE_USER_BOOK] Processing field {field_name} = {field_value}")
                
                if field_value is not None and field_value != '':
                    # Create a CustomField node for this field
                    field_node_id = f"custom_{user_id}_{book_id}_{field_name}"
                    print(f"🔍 [UPDATE_USER_BOOK] Creating CustomField node with id: {field_node_id}")
                    
                    # Store the custom field
                    field_data = {
                        'id': field_node_id,
                        'name': field_name,
                        'field_type': 'text',  # Default to text for now
                        'value': str(field_value),
                        'created_at': datetime.utcnow()
                    }
                    
                    success = self.graph_storage.store_node('CustomField', field_node_id, field_data)
                    print(f"🔍 [UPDATE_USER_BOOK] CustomField node creation success: {success}")
                    
                    if success:
                        # Create the HAS_CUSTOM_FIELD relationship
                        rel_props = {
                            'book_id': book_id,
                            'field_name': field_name
                        }
                        rel_success = self.graph_storage.create_relationship('User', user_id, 'HAS_CUSTOM_FIELD', 'CustomField', field_node_id, rel_props)
                        print(f"🔍 [UPDATE_USER_BOOK] Relationship creation success: {rel_success}")
                        
                        if rel_success:
                            print(f"✅ Saved custom field {field_name} = {field_value} for user {user_id}, book {book_id}")
                        else:
                            print(f"❌ Failed to create relationship for custom field {field_name}")
                    else:
                        print(f"❌ Failed to save custom field {field_name}")
                else:
                    print(f"🔍 [UPDATE_USER_BOOK] Skipping empty field {field_name}")
                        
            print(f"🔍 [UPDATE_USER_BOOK] Completed processing all custom fields")
            return True
            
        except Exception as e:
            print(f"❌ Error updating user book custom metadata: {e}")
            import traceback
            traceback.print_exc()
            return False


# Global service instances
user_book_service = KuzuUserBookService()
