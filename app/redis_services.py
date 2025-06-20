"""
Redis-only Application Services.

This module provides service classes that use Redis as the primary database,
replacing the dual-write approach with a simplified Redis-first architecture.
"""

import os
import asyncio
import uuid
from typing import List, Optional, Dict, Any
from datetime import datetime, date, timedelta
from dataclasses import asdict
from functools import wraps

from flask import current_app

from .domain.models import Book, User, Author, Publisher, Series, Category, UserBookRelationship, ReadingLog, ReadingStatus
from .infrastructure.redis_repositories import RedisBookRepository, RedisUserRepository, RedisAuthorRepository
from .infrastructure.redis_graph import get_graph_storage


def run_async(async_func):
    """Decorator to run async functions synchronously for Flask compatibility."""
    @wraps(async_func)
    def wrapper(*args, **kwargs):
        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        
        if loop.is_running():
            # If we're already in an async context, create a new thread
            import concurrent.futures
            with concurrent.futures.ThreadPoolExecutor() as executor:
                # Create the coroutine in the executor thread
                def run_in_new_loop():
                    new_loop = asyncio.new_event_loop()
                    try:
                        return new_loop.run_until_complete(async_func(*args, **kwargs))
                    finally:
                        new_loop.close()
                future = executor.submit(run_in_new_loop)
                return future.result()
        else:
            return loop.run_until_complete(async_func(*args, **kwargs))
    return wrapper


class RedisBookService:
    """Service for managing books using Redis as primary database."""
    
    def __init__(self):
        self.graph_storage = get_graph_storage()
        self.book_repo = RedisBookRepository(self.graph_storage)
        self.author_repo = RedisAuthorRepository(self.graph_storage)
        self.user_repo = RedisUserRepository(self.graph_storage)
    
    @run_async
    async def create_book(self, domain_book: Book, user_id: str) -> Book:
        """Create a book in Redis."""
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
            # Create the book in Redis
            created_book = await self.book_repo.create(domain_book)
            print(f"book_repo.create returned: {created_book}")
            
            # Create user-book relationship
            relationship = UserBookRelationship(
                user_id=str(user_id),
                book_id=created_book.id,
                reading_status=ReadingStatus.PLAN_TO_READ,
                date_added=datetime.utcnow()
            )
            
            # Store the relationship in Redis
            rel_key = f"user_book:{user_id}:{created_book.id}"
            rel_data = asdict(relationship)
            
            # Serialize datetime and enum objects for JSON storage
            def serialize_for_json(obj):
                if isinstance(obj, datetime):
                    return obj.isoformat()
                elif hasattr(obj, 'value'):  # Enum
                    return obj.value
                elif isinstance(obj, dict):
                    return {k: serialize_for_json(v) for k, v in obj.items()}
                elif isinstance(obj, list):
                    return [serialize_for_json(item) for item in obj]
                else:
                    return obj
            
            rel_data = serialize_for_json(rel_data)
            print(f"Storing relationship data: {rel_data}")
            
            await self.graph_storage.set_json(rel_key, rel_data)
            print(f"Successfully stored relationship")
            
            print(f"Created book {created_book.id} for user {user_id} in Redis")
            return created_book
            
        except Exception as e:
            print(f"Error in create_book: {e}")
            import traceback
            traceback.print_exc()
            raise
    
    @run_async
    async def get_book_by_id(self, book_id: str) -> Optional[Book]:
        """Get a book by ID."""
        return await self.book_repo.get_by_id(book_id)
    
    @run_async
    async def get_books_for_user(self, user_id: str, limit: int = 50, offset: int = 0) -> List[Book]:
        """Get all books for a user with relationship data."""
        # Find all user-book relationships for this user
        pattern = f"user_book:{user_id}:*"
        relationship_keys = await self.graph_storage.scan_keys(pattern)
        
        books = []
        for rel_key in relationship_keys[offset:offset + limit]:
            # Extract book_id from the key
            book_id = rel_key.split(':')[-1]
            book = await self.book_repo.get_by_id(book_id)
            if book:
                # Get the user-book relationship data
                relationship_data = await self.graph_storage.get_json(rel_key)
                if relationship_data:
                    # Add user relationship attributes to the book object
                    book.reading_status = relationship_data.get('reading_status', 'plan_to_read')
                    book.ownership_status = relationship_data.get('ownership_status', 'owned')
                    book.start_date = relationship_data.get('start_date')
                    book.finish_date = relationship_data.get('finish_date')
                    book.user_rating = relationship_data.get('user_rating')
                    book.personal_notes = relationship_data.get('personal_notes')
                    book.date_added = relationship_data.get('date_added')
                    book.want_to_read = relationship_data.get('reading_status') == 'plan_to_read'
                    book.library_only = relationship_data.get('reading_status') == 'library_only'
                    book.uid = book.id  # Ensure uid is available
                    
                    # Convert date strings back to date objects if needed
                    if isinstance(book.start_date, str):
                        try:
                            book.start_date = datetime.fromisoformat(book.start_date).date()
                        except:
                            book.start_date = None
                    if isinstance(book.finish_date, str):
                        try:
                            book.finish_date = datetime.fromisoformat(book.finish_date).date()
                        except:
                            book.finish_date = None
                            
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
        book = await self.book_repo.get_by_id(book_id)
        if not book:
            return None
        
        # Update fields
        for field, value in updates.items():
            if hasattr(book, field):
                setattr(book, field, value)
        
        book.updated_at = datetime.utcnow()
        return await self.book_repo.update(book)
    
    @run_async
    async def delete_book(self, book_id: str, user_id: str) -> bool:
        """Delete a book."""
        # Delete user-book relationship
        rel_key = f"user_book:{user_id}:{book_id}"
        await self.graph_storage.delete_key(rel_key)
        
        # Check if any other users have this book
        pattern = f"user_book:*:{book_id}"
        other_relationships = await self.graph_storage.scan_keys(pattern)
        
        # If no other users have this book, delete the book itself
        if not other_relationships:
            return await self.book_repo.delete(book_id)
        
        return True
    
    @run_async
    async def get_books_with_sharing_users(self, days_back: int = 30, limit: int = 20) -> List[Book]:
        """Get books from users who share reading activity, finished in the last N days."""
        # Get all users who share reading activity
        sharing_users = await self.user_repo.get_all()
        sharing_user_ids = [u.id for u in sharing_users if u.share_reading_activity and u.is_active]
        
        if not sharing_user_ids:
            return []
        
        # Get all books for sharing users
        all_books = []
        for user_id in sharing_user_ids:
            books = await self.book_repo.get_books_for_user(user_id)
            all_books.extend(books)
        
        # Filter for finished books in the specified time range
        cutoff_date = date.today() - timedelta(days=days_back)
        finished_books = [
            book for book in all_books 
            if book.finish_date and book.finish_date >= cutoff_date
        ]
        
        # Sort by finish date descending and limit
        finished_books.sort(key=lambda b: b.finish_date or date.min, reverse=True)
        return finished_books[:limit]
    
    @run_async
    async def get_currently_reading_shared(self, limit: int = 20) -> List[Book]:
        """Get currently reading books from users who share current reading."""
        # Get all users who share current reading
        sharing_users = await self.user_repo.get_all()
        sharing_user_ids = [u.id for u in sharing_users if u.share_current_reading and u.is_active]
        
        if not sharing_user_ids:
            return []
        
        # Get all books for sharing users
        all_books = []
        for user_id in sharing_user_ids:
            books = await self.book_repo.get_books_for_user(user_id)
            all_books.extend(books)
        
        # Filter for currently reading (has start_date but no finish_date)
        currently_reading = [
            book for book in all_books 
            if book.start_date and not book.finish_date
        ]
        
        # Sort by start date descending and limit
        currently_reading.sort(key=lambda b: b.start_date or date.min, reverse=True)
        return currently_reading[:limit]
    
    @run_async
    async def get_book_by_isbn_for_user(self, isbn: str, user_id: str) -> Optional[Book]:
        """Get a book by ISBN for a specific user."""
        books = await self.get_books_for_user(user_id, limit=1000)  # Get enriched books
        for book in books:
            if book.isbn == isbn:
                return book
        return None
    
    @run_async
    async def get_book_by_id_for_user(self, book_id: str, user_id: str) -> Optional[Book]:
        """Get a specific book for a user with relationship data."""
        book = await self.book_repo.get_by_id(book_id)
        if not book:
            return None
            
        # Check if user has this book
        relationship_key = f"user_book:{user_id}:{book_id}"
        relationship_data = await self.graph_storage.get_json(relationship_key)
        if not relationship_data:
            return None
            
        # Add user relationship attributes to the book object
        book.reading_status = relationship_data.get('reading_status', 'plan_to_read')
        book.ownership_status = relationship_data.get('ownership_status', 'owned')
        book.start_date = relationship_data.get('start_date')
        book.finish_date = relationship_data.get('finish_date')
        book.user_rating = relationship_data.get('user_rating')
        book.personal_notes = relationship_data.get('personal_notes')
        book.date_added = relationship_data.get('date_added')
        book.want_to_read = relationship_data.get('reading_status') == 'plan_to_read'
        book.library_only = relationship_data.get('reading_status') == 'library_only'
        book.uid = book.id  # Ensure uid is available
        
        # Convert date strings back to date objects if needed
        if isinstance(book.start_date, str):
            try:
                book.start_date = datetime.fromisoformat(book.start_date).date()
            except:
                book.start_date = None
        if isinstance(book.finish_date, str):
            try:
                book.finish_date = datetime.fromisoformat(book.finish_date).date()
            except:
                book.finish_date = None
                
        return book

    # Sync wrappers for Flask compatibility
    def create_book_sync(self, domain_book: Book, user_id: str) -> Book:
        """Sync wrapper for create_book."""
        # create_book already has @run_async decorator, so just call it directly
        return self.create_book(domain_book, user_id)
    
    def get_book_by_id_sync(self, book_id: str) -> Optional[Book]:
        """Sync wrapper for get_book_by_id."""
        # get_book_by_id already has @run_async decorator, so just call it directly
        return self.get_book_by_id(book_id)
    
    def get_book_by_uid_sync(self, uid: str, user_id: str) -> Optional[Book]:
        """Sync wrapper for get_book_by_id_for_user (uid is same as book_id)."""
        # get_book_by_id_for_user already has @run_async decorator, so just call it directly
        return self.get_book_by_id_for_user(uid, user_id)
    
    def get_user_books_sync(self, user_id: str) -> List[Book]:
        """Sync wrapper for get_books_for_user."""
        # get_books_for_user already has @run_async decorator, so just call it directly
        return self.get_books_for_user(user_id)
    
    def search_books_sync(self, query: str, user_id: str) -> List[Book]:
        """Sync wrapper for search_books."""
        # search_books already has @run_async decorator, so just call it directly
        return self.search_books(query, user_id)
    
    def update_book_sync(self, book_id: str, user_id: str, **kwargs) -> Optional[Book]:
        """Sync wrapper for update_book."""
        # update_book already has @run_async decorator, so just call it directly
        return self.update_book(book_id, kwargs, user_id)
    
    def delete_book_sync(self, book_id: str, user_id: str) -> bool:
        """Sync wrapper for delete_book."""
        # delete_book already has @run_async decorator, so just call it directly
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
                            if len(reader.fieldnames) == 1 and reader.fieldnames[0].startswith('978'):
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
                                    isbn10=isbn if len(isbn) == 10 else None,
                                    authors=[Author(name=author)] if author else []
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


# Create service instances
redis_book_service = RedisBookService()