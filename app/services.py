"""
Redis-only service layer for Bibliotheca.

This module provides service classes for comprehensive book management using Redis 
as the sole data store with graph database functionality.
"""

import os
import csv
import uuid
import asyncio
import traceback
import re
import concurrent.futures
from typing import List, Optional, Dict, Any, Tuple
from datetime import datetime, date, timedelta
from dataclasses import asdict
from functools import wraps

from flask import current_app
from flask_login import current_user
from werkzeug.local import LocalProxy

from .domain.models import Book, User, Author, Publisher, Series, Category, UserBookRelationship, ReadingLog, ReadingStatus, OwnershipStatus, CustomFieldDefinition, ImportMappingTemplate, CustomFieldType
from .infrastructure.redis_repositories import RedisBookRepository, RedisUserRepository, RedisAuthorRepository, RedisUserBookRepository, RedisCustomFieldRepository, RedisImportMappingRepository
from .infrastructure.redis_graph import get_graph_storage


def extract_digits_only(value: str) -> Optional[str]:
    """Extract only digits from a string (for ISBN, UPC, etc.)."""
    if not value:
        return None
    
    # Extract only digits
    digits_only = re.sub(r'[^\d]', '', str(value))
    
    # Return None if no digits found
    if not digits_only:
        return None
    
    return digits_only


def normalize_isbn_upc(isbn_or_upc: Optional[str]) -> Optional[str]:
    """Normalize ISBN or UPC by extracting digits only."""
    if not isbn_or_upc:
        return None
    
    # Extract digits only
    normalized = extract_digits_only(isbn_or_upc)
    
    # Validate length for common ISBN/UPC formats
    if normalized:
        length = len(normalized)
        # Accept common ISBN/UPC lengths: 10 (ISBN-10), 13 (ISBN-13, UPC-A), 12 (UPC-A without check digit)
        if length in [10, 12, 13]:
            return normalized
        else:
            print(f"⚠️ [ISBN/UPC] Unusual length ({length} digits): {normalized}")
            return normalized  # Still return it, might be valid
    
    return None


def run_async(async_func):
    """Decorator to run async functions synchronously for Flask compatibility."""
    @wraps(async_func)
    def wrapper(*args, **kwargs):
        from flask import has_app_context, copy_current_request_context, current_app
        
        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        
        if loop.is_running():
            # If we're already in an async context, create a new thread
            
            # Preserve Flask context if available
            if has_app_context():
                @copy_current_request_context
                def run_with_context():
                    async def run_coro():
                        return await async_func(*args, **kwargs)
                    return asyncio.run(run_coro())
                
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    future = executor.submit(run_with_context)
                    return future.result()
            else:
                def run_without_context():
                    async def run_coro():
                        return await async_func(*args, **kwargs)
                    return asyncio.run(run_coro())
                
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    future = executor.submit(run_without_context)
                    return future.result()
        else:
            return loop.run_until_complete(async_func(*args, **kwargs))
    return wrapper


class RedisBookService:
    """Service for managing books using Redis as the sole data store."""
    
    def __init__(self):
        self.redis_enabled = os.getenv('GRAPH_DATABASE_ENABLED', 'true').lower() == 'true'
        
        if not self.redis_enabled:
            raise RuntimeError("Redis must be enabled for this version of Bibliotheca")
            
        self.storage = get_graph_storage()
        self.redis_book_repo = RedisBookRepository(self.storage)
        self.redis_user_repo = RedisUserRepository(self.storage)
        self.redis_author_repo = RedisAuthorRepository(self.storage)
        self.redis_user_book_repo = RedisUserBookRepository(self.storage)
    
    async def create_book(self, domain_book: Book) -> Book:
        """Create a book in Redis (global, not user-specific)."""
        try:
            print(f"[SERVICE] Creating global book: {domain_book.title}")
            
            # Generate ID if not set
            if not domain_book.id:
                import uuid
                domain_book.id = str(uuid.uuid4())
                print(f"[SERVICE] Generated book ID: {domain_book.id}")
            
            # Store book in Redis (globally)
            print(f"[SERVICE] Storing book in Redis...")
            await self.redis_book_repo.create(domain_book)
            print(f"[SERVICE] Book stored successfully")
            
            print(f"[SERVICE] Book {domain_book.id} successfully created in Redis")
            return domain_book
        except Exception as e:
            print(f"[SERVICE] Failed to create book in Redis: {e}")
            import traceback
            traceback.print_exc()
            raise
    
    async def add_book_to_user_library(self, user_id: str, book_id: str, 
                                     reading_status: ReadingStatus = ReadingStatus.PLAN_TO_READ,
                                     ownership_status: OwnershipStatus = OwnershipStatus.OWNED,
                                     locations: Optional[List[str]] = None,
                                     custom_metadata: Optional[Dict[str, Any]] = None,
                                     user_rating: Optional[float] = None,
                                     user_review: Optional[str] = None,
                                     personal_notes: Optional[str] = None,
                                     start_date: Optional[date] = None,
                                     finish_date: Optional[date] = None,
                                     date_added: Optional[date] = None) -> bool:
        """Add a book to a user's library by creating a relationship."""
        try:
            print(f"[SERVICE] Adding book {book_id} to user {user_id}'s library")
            print(f"[SERVICE] Custom metadata passed: {custom_metadata}")
            
            # Check if relationship already exists
            existing_rel = await self.redis_user_book_repo.get_relationship(str(user_id), book_id)
            if existing_rel:
                print(f"[SERVICE] Relationship already exists")
                return True
            
            # Create user-book relationship
            relationship = UserBookRelationship(
                user_id=str(user_id),
                book_id=book_id,
                reading_status=reading_status,
                ownership_status=ownership_status,
                user_rating=user_rating,
                user_review=user_review,
                personal_notes=personal_notes,
                start_date=datetime.combine(start_date, datetime.min.time()) if start_date else None,
                finish_date=datetime.combine(finish_date, datetime.min.time()) if finish_date else None,
                date_added=datetime.combine(date_added, datetime.min.time()) if date_added else datetime.now(),
                locations=locations or [],
                user_tags=[],
                custom_metadata=custom_metadata or {},
                source='import',
                created_at=datetime.now(),
                updated_at=datetime.now()
            )
            
            print(f"[SERVICE] Created relationship with custom_metadata: {relationship.custom_metadata}")
            
            # Store relationship in Redis
            print(f"[SERVICE] Creating relationship...")
            await self.redis_user_book_repo.create_relationship(relationship)
            print(f"[SERVICE] Relationship created successfully")
            
            return True
        except Exception as e:
            print(f"[SERVICE] Failed to add book to user library: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    async def get_book_by_id(self, book_id: str, user_id: str) -> Optional[Book]:
        """Get a book by ID."""
        try:
            book = await self.redis_book_repo.get_by_id(book_id)
            if book:
                # Get user-specific relationship data
                try:
                    relationships = await self.redis_user_book_repo.get_user_library(str(user_id))
                    for rel in relationships:
                        if rel.book_id == book_id:
                            # Add user-specific attributes
                            setattr(book, 'reading_status', rel.reading_status.value)
                            setattr(book, 'ownership_status', rel.ownership_status.value)
                            setattr(book, 'start_date', rel.start_date)
                            setattr(book, 'finish_date', rel.finish_date)
                            setattr(book, 'user_rating', rel.user_rating)
                            setattr(book, 'personal_notes', rel.personal_notes)
                            setattr(book, 'date_added', rel.date_added)
                            setattr(book, 'user_tags', rel.user_tags)
                            setattr(book, 'locations', rel.locations)
                            setattr(book, 'custom_metadata', rel.custom_metadata or {})
                            break
                except Exception as e:
                    current_app.logger.warning(f"Could not load user relationship for book {book_id}: {e}")
            return book
        except Exception as e:
            current_app.logger.error(f"Failed to get book {book_id}: {e}")
            return None
    
    async def get_user_books(self, user_id: str) -> List[Book]:
        """Get all books for a user."""
        try:
            print(f"[SERVICE] Getting user books for user_id: {user_id}")
            
            # Get all UserBookRelationships for this user from Redis
            relationships = await self.redis_user_book_repo.get_user_library(str(user_id))
            print(f"[SERVICE] Found {len(relationships)} relationships")
            
            # Convert to Book objects with user-specific attributes stored as dynamic attributes
            books = []
            for rel in relationships:
                print(f"[SERVICE] Processing relationship for book_id: {rel.book_id}")
                # Get the book data
                book = await self.redis_book_repo.get_by_id(rel.book_id)
                if book:
                    print(f"[SERVICE] Found book: {book.title}")
                    # Store user-specific attributes as dynamic attributes (for backward compatibility)
                    # Note: These are not part of the Book domain model but added for view layer compatibility
                    setattr(book, 'reading_status', rel.reading_status.value)
                    setattr(book, 'ownership_status', rel.ownership_status.value)
                    setattr(book, 'start_date', rel.start_date)
                    setattr(book, 'finish_date', rel.finish_date)
                    setattr(book, 'user_rating', rel.user_rating)
                    setattr(book, 'personal_notes', rel.personal_notes)
                    setattr(book, 'date_added', rel.date_added)
                    setattr(book, 'user_tags', rel.user_tags)
                    setattr(book, 'locations', rel.locations)
                    setattr(book, 'custom_metadata', rel.custom_metadata or {})
                    books.append(book)
                else:
                    print(f"[SERVICE] Could not find book with ID: {rel.book_id}")
            
            print(f"[SERVICE] Returning {len(books)} books")
            return books
        except Exception as e:
            print(f"[SERVICE] Error getting user books: {e}")
            import traceback
            traceback.print_exc()
            return []
            
            return books
        except Exception as e:
            current_app.logger.error(f"Failed to get user books from Redis: {e}")
            return []
    
    async def get_book_by_uid(self, uid: str, user_id: str) -> Optional[Book]:
        """Get a book by UID (alias for get_book_by_id)."""
        return await self.get_book_by_id(uid, user_id)
    
    async def update_book(self, uid: str, user_id: str, **kwargs) -> Optional[Book]:
        """Update a book or user-book relationship fields."""
        try:
            book = await self.redis_book_repo.get_by_id(uid)
            if not book:
                return None
            
            # Separate relationship fields from book fields
            relationship_fields = {'personal_notes', 'user_rating', 'reading_status', 'ownership_status', 
                                 'date_started', 'date_finished', 'date_added', 'favorite', 'priority', 'custom_metadata'}
            book_fields = {}
            rel_fields = {}
            
            for field, value in kwargs.items():
                if field in relationship_fields:
                    rel_fields[field] = value
                elif hasattr(book, field):
                    book_fields[field] = value
            
            # Update book fields if any
            if book_fields:
                for field, value in book_fields.items():
                    setattr(book, field, value)
                await self.redis_book_repo.update(book)
                current_app.logger.info(f"Book {uid} updated in Redis")
            
            # Update relationship fields if any
            if rel_fields:
                # Get existing relationship
                relationship = await self.redis_user_book_repo.get_relationship(user_id, uid)
                if relationship:
                    # Update relationship fields
                    for field, value in rel_fields.items():
                        if hasattr(relationship, field):
                            # Convert string values to enums where necessary
                            if field == 'reading_status' and isinstance(value, str):
                                try:
                                    val_lower = value.lower()
                                    if val_lower == 'currently-reading':
                                        value = ReadingStatus.READING
                                    elif val_lower == 'to-read':
                                        value = ReadingStatus.PLAN_TO_READ
                                    elif val_lower == 'read':
                                        value = ReadingStatus.READ
                                    else:
                                        value = ReadingStatus(val_lower)
                                except (ValueError, AttributeError):
                                    current_app.logger.warning(f"Invalid reading status value: {value}")
                                    continue
                            elif field == 'ownership_status' and isinstance(value, str):
                                try:
                                    value = OwnershipStatus(value.lower())
                                except ValueError:
                                    current_app.logger.warning(f"Invalid ownership status value: {value}")
                                    continue
                            setattr(relationship, field, value)
                    
                    # Update the relationship in Redis
                    await self.redis_user_book_repo.update_relationship(relationship)
                    current_app.logger.info(f"User-book relationship updated for book {uid} and user {user_id}")
                else:
                    current_app.logger.warning(f"No relationship found between user {user_id} and book {uid}")
            
            return book
        except Exception as e:
            current_app.logger.error(f"Failed to update book {uid}: {e}")
            return None
    
    async def delete_book(self, uid: str, user_id: str) -> bool:
        """Delete a book from user's library (remove relationship, not the global book)."""
        try:
            # Find the book by UID to get the book ID
            user_books = await self.get_user_books(user_id)
            book_to_remove = None
            
            for book in user_books:
                if hasattr(book, 'uid') and book.uid == uid:
                    book_to_remove = book
                    break
                elif book.id == uid:  # fallback to ID if UID not found
                    book_to_remove = book
                    break
            
            if not book_to_remove:
                print(f"[SERVICE] Book with UID {uid} not found in user {user_id}'s library")
                return False
            
            # Check if book has an ID
            if not book_to_remove.id:
                print(f"[SERVICE] Book {book_to_remove.title} has no ID")
                return False
            
            # Delete the user-book relationship
            success = await self.redis_user_book_repo.delete_relationship(str(user_id), book_to_remove.id)
            if success:
                print(f"[SERVICE] Successfully removed book {book_to_remove.title} from user {user_id}'s library")
                return True
            else:
                print(f"[SERVICE] Failed to remove relationship for book {book_to_remove.id}")
                return False
                
        except Exception as e:
            print(f"[SERVICE] Error deleting book from user library: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    async def get_books_by_isbn(self, isbn: str) -> List[Book]:
        """Get books by ISBN (global search, not user-specific)."""
        try:
            # Search for books by ISBN in Redis globally
            # Use the repository method that searches all books
            matching_books = await self.redis_book_repo.get_books_by_isbn(isbn)
            return matching_books
        except Exception as e:
            print(f"[SERVICE] Error getting books by ISBN: {e}")
            return []
    
    async def search_books(self, query: str, user_id: str, filter_params: Optional[Dict] = None) -> List[Book]:
        """Search books with optional filters."""
        try:
            # This would need to be implemented in the Redis repository
            # For now, get all user books and filter in memory
            all_books = await self.get_user_books(user_id)
            
            if not query:
                return all_books
            
            # Simple text search
            query_lower = query.lower()
            filtered_books = []
            for book in all_books:
                if (query_lower in book.title.lower() or 
                    any(query_lower in author.name.lower() for author in book.authors) or
                    (book.description and query_lower in book.description.lower())):
                    filtered_books.append(book)
            
            return filtered_books
        except Exception as e:
            current_app.logger.error(f"Failed to search books: {e}")
            return []
    
    async def find_or_create_book(self, domain_book: Book) -> Book:
        """Find existing book globally or create new one."""
        existing_book = None
        
        # Try to find existing book by ISBN first
        if domain_book.isbn13:
            existing_books = await self.get_books_by_isbn(domain_book.isbn13)
            if existing_books:
                existing_book = existing_books[0]
                print(f"[SERVICE] Found existing book by ISBN13: {existing_book.title}")
        
        if not existing_book and domain_book.isbn10:
            existing_books = await self.get_books_by_isbn(domain_book.isbn10)
            if existing_books:
                existing_book = existing_books[0]
                print(f"[SERVICE] Found existing book by ISBN10: {existing_book.title}")
        
        if existing_book:
            # Check if the new book data contains additional information (like cover_url)
            # that should be merged into the existing book
            needs_update = False
            
            # Update cover URL if missing
            if domain_book.cover_url and not existing_book.cover_url:
                existing_book.cover_url = domain_book.cover_url
                needs_update = True
                print(f"[SERVICE] Adding cover URL to existing book: {domain_book.cover_url}")
            
            # Update other missing fields
            if domain_book.description and not existing_book.description:
                existing_book.description = domain_book.description
                needs_update = True
                print(f"[SERVICE] Adding description to existing book")
            
            if domain_book.published_date and not existing_book.published_date:
                existing_book.published_date = domain_book.published_date
                needs_update = True
                print(f"[SERVICE] Adding published date to existing book")
            
            if domain_book.page_count and not existing_book.page_count:
                existing_book.page_count = domain_book.page_count
                needs_update = True
                print(f"[SERVICE] Adding page count to existing book")
            
            # Update the book in storage if changes were made
            if needs_update:
                existing_book.updated_at = datetime.now()
                await self.redis_book_repo.update(existing_book)
                print(f"[SERVICE] Updated existing book with new information")
            
            return existing_book
        
        # If no existing book found, create new one
        print(f"[SERVICE] No existing book found, creating new book")
        return await self.create_book(domain_book)

    async def get_user_book(self, user_id: str, book_identifier: str) -> Optional[Book]:
        """Get a specific book for a user with user-specific metadata.
        
        Args:
            user_id: The user ID
            book_identifier: Either book_id or book UID
        """
        try:
            print(f"[SERVICE] Getting user book: user_id={user_id}, book_identifier={book_identifier}")
            
            # First try to get by exact book_id
            relationship = await self.redis_user_book_repo.get_relationship(str(user_id), book_identifier)
            book = None
            
            if relationship:
                # Get the book data using the book_id from the relationship
                book = await self.redis_book_repo.get_by_id(relationship.book_id)
            else:
                # If no relationship found by exact ID, try to find the book first
                # The book_identifier might be a UID, so look up the book
                book = await self.redis_book_repo.get_by_id(book_identifier)
                if book and book.id:
                    # Now try to get the relationship using the book's ID
                    relationship = await self.redis_user_book_repo.get_relationship(str(user_id), book.id)
            
            if not relationship:
                print(f"[SERVICE] No relationship found for user {user_id} and book {book_identifier}")
                return None
            
            if not book:
                print(f"[SERVICE] Book {book_identifier} not found")
                return None
            
            print(f"[SERVICE] Found book: {book.title}")
            
            # Add user-specific attributes to the book object
            setattr(book, 'reading_status', relationship.reading_status.value)
            setattr(book, 'ownership_status', relationship.ownership_status.value)
            setattr(book, 'start_date', relationship.start_date)
            setattr(book, 'finish_date', relationship.finish_date)
            setattr(book, 'user_rating', relationship.user_rating)
            setattr(book, 'personal_notes', relationship.personal_notes)
            setattr(book, 'date_added', relationship.date_added)
            setattr(book, 'user_tags', relationship.user_tags)
            setattr(book, 'locations', relationship.locations)
            setattr(book, 'custom_metadata', relationship.custom_metadata or {})
            
            print(f"[SERVICE] Relationship custom_metadata: {relationship.custom_metadata}")
            print(f"[SERVICE] Book custom_metadata after assignment: {book.custom_metadata}")
            print(f"[SERVICE] User book loaded successfully with {len(book.custom_metadata)} custom metadata entries")
            return book
            
        except Exception as e:
            print(f"[SERVICE] Error getting user book: {e}")
            import traceback
            traceback.print_exc()
            return None

    # Sync wrappers for Flask compatibility
    @run_async
    @run_async
    def create_book_sync(self, domain_book: Book) -> Book:
        """Sync wrapper for create_book."""
        return self.create_book(domain_book)
    
    @run_async
    def add_book_to_user_library_sync(self, user_id: str, book_id: str, 
                                      reading_status: ReadingStatus = ReadingStatus.PLAN_TO_READ,
                                      ownership_status: OwnershipStatus = OwnershipStatus.OWNED,
                                      locations: Optional[List[str]] = None,
                                      custom_metadata: Optional[Dict[str, Any]] = None,
                                      user_rating: Optional[float] = None,
                                      user_review: Optional[str] = None,
                                      personal_notes: Optional[str] = None,
                                      start_date: Optional[date] = None,
                                      finish_date: Optional[date] = None,
                                      date_added: Optional[date] = None) -> bool:
        """Sync wrapper for add_book_to_user_library."""
        return self.add_book_to_user_library(user_id, book_id, reading_status, ownership_status, locations, custom_metadata, user_rating, user_review, personal_notes, start_date, finish_date, date_added)
    
    @run_async
    def get_book_by_id_sync(self, book_id: str, user_id: str) -> Optional[Book]:
        """Sync wrapper for get_book_by_id."""
        return self.get_book_by_id(book_id, user_id)
    
    @run_async
    def get_book_by_uid_sync(self, uid: str, user_id: str) -> Optional[Book]:
        """Sync wrapper for get_book_by_uid."""
        return self.get_book_by_uid(uid, user_id)
    
    @run_async
    def get_user_books_sync(self, user_id: str) -> List[Book]:
        """Sync wrapper for get_user_books."""
        return self.get_user_books(user_id)
    
    @run_async
    def get_user_book_sync(self, user_id: str, book_identifier: str) -> Optional[Book]:
        """Sync wrapper for get_user_book."""
        return self.get_user_book(user_id, book_identifier)
    
    @run_async
    def update_book_sync(self, uid: str, user_id: str, **kwargs) -> Optional[Book]:
        """Sync wrapper for update_book."""
        return self.update_book(uid, user_id, **kwargs)
    
    @run_async
    def delete_book_sync(self, uid: str, user_id: str) -> bool:
        """Sync wrapper for delete_book."""
        return self.delete_book(uid, user_id)
    
    @run_async
    def get_books_by_isbn_sync(self, isbn: str) -> List[Book]:
        """Sync wrapper for get_books_by_isbn."""
        return self.get_books_by_isbn(isbn)
    
    @run_async
    def search_books_sync(self, query: str, user_id: str, filter_params: Optional[Dict] = None) -> List[Book]:
        """Sync wrapper for search_books."""
        return self.search_books(query, user_id, filter_params)
    
    @run_async
    def find_or_create_book_sync(self, domain_book: Book) -> Book:
        """Sync wrapper for find_or_create_book."""
        return self.find_or_create_book(domain_book)
    
    async def update_user_book(self, user_id: str, book_id: str, **kwargs) -> bool:
        """Update a user-book relationship including custom metadata."""
        try:
            # Get existing relationship
            relationship = await self.redis_user_book_repo.get_relationship(str(user_id), book_id)
            if not relationship:
                print(f"❌ [SERVICE] No relationship found between user {user_id} and book {book_id}")
                return False
            
            # Update fields from kwargs
            for field, value in kwargs.items():
                if hasattr(relationship, field):
                    # Special handling for custom metadata to merge rather than replace
                    if field == 'custom_metadata':
                        if not hasattr(relationship, 'custom_metadata') or relationship.custom_metadata is None:
                            relationship.custom_metadata = {}
                        relationship.custom_metadata.update(value)
                        print(f"✅ [SERVICE] Merged custom metadata: {relationship.custom_metadata}")
                        continue
                    
                    # Handle enum conversions
                    if field == 'reading_status' and isinstance(value, str):
                        try:
                            val_lower = value.lower()
                            if val_lower == 'currently-reading':
                                value = ReadingStatus.READING
                            elif val_lower == 'to-read':
                                value = ReadingStatus.PLAN_TO_READ
                            elif val_lower == 'read':
                                value = ReadingStatus.READ
                            else:
                                value = ReadingStatus(val_lower)
                        except (ValueError, AttributeError):
                            current_app.logger.warning(f"Invalid reading status value: {value}")
                            continue
                    elif field == 'ownership_status' and isinstance(value, str):
                        try:
                            value = OwnershipStatus(value.lower())
                        except ValueError:
                            current_app.logger.warning(f"Invalid ownership status value: {value}")
                            continue
                    
                    setattr(relationship, field, value)
                    print(f"✅ [SERVICE] Updated relationship field {field} = {value}")
            
            # Update the relationship in Redis
            await self.redis_user_book_repo.update_relationship(relationship)
            print(f"✅ [SERVICE] User-book relationship updated for book {book_id} and user {user_id}")
            return True
            
        except Exception as e:
            print(f"❌ [SERVICE] Error updating user-book relationship: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    @run_async
    def update_user_book_sync(self, user_id: str, book_id: str, **kwargs) -> bool:
        """Sync wrapper for update_user_book."""
        return self.update_user_book(user_id, book_id, **kwargs)
    

class RedisUserService:
    """Service for managing users using Redis as the sole data store."""
    
    def __init__(self):
        self.redis_enabled = os.getenv('GRAPH_DATABASE_ENABLED', 'true').lower() == 'true'
        
        if not self.redis_enabled:
            raise RuntimeError("Redis must be enabled for this version of Bibliotheca")
            
        self.storage = get_graph_storage()
        self.redis_user_repo = RedisUserRepository(self.storage)
    
    async def create_user(self, username: str, email: str, password_hash: str, 
                         is_admin: bool = False, is_active: bool = True, 
                         password_must_change: bool = False) -> User:
        """Create a user in Redis."""
        # Generate a unique ID
        import uuid
        user_id = str(uuid.uuid4())
        
        # Create domain user
        domain_user = User(
            id=user_id,
            username=username,
            email=email,
            password_hash=password_hash,
            is_admin=is_admin,
            is_active=is_active,
            password_must_change=password_must_change,
            share_current_reading=False,
            share_reading_activity=False,
            share_library=False,
            created_at=datetime.now(),
            last_login=None,
            reading_streak_offset=0
        )
        
        # Store in Redis
        try:
            await self.redis_user_repo.create(domain_user)
            current_app.logger.info(f"User {domain_user.id} successfully created in Redis")
            return domain_user
        except Exception as e:
            current_app.logger.error(f"Failed to create user in Redis: {e}")
            raise
    
    async def get_user_by_id(self, user_id: str) -> Optional[User]:
        """Get a user by ID."""
        try:
            user = await self.redis_user_repo.get_by_id(str(user_id))
            return user
        except Exception as e:
            current_app.logger.error(f"Failed to get user {user_id}: {e}")
            return None
    
    async def get_user_by_username(self, username: str) -> Optional[User]:
        """Get a user by username."""
        try:
            user = await self.redis_user_repo.get_by_username(username)
            return user
        except Exception as e:
            current_app.logger.error(f"Failed to get user by username {username}: {e}")
            return None
    
    async def get_user_by_email(self, email: str) -> Optional[User]:
        """Get a user by email."""
        try:
            user = await self.redis_user_repo.get_by_email(email)
            return user
        except Exception as e:
            current_app.logger.error(f"Failed to get user by email {email}: {e}")
            return None
    
    async def get_user_by_username_or_email(self, username_or_email: str) -> Optional[User]:
        """Get a user by username or email."""
        # Try username first
        user = await self.get_user_by_username(username_or_email)
        if user:
            return user
        # Try email
        return await self.get_user_by_email(username_or_email)
    
    async def get_user_count(self) -> int:
        """Get total number of users."""
        try:
            users = await self.redis_user_repo.list_all()
            return len(users)
        except Exception as e:
            current_app.logger.error(f"Failed to get user count: {e}")
            return 0
    
    async def get_admin_count(self) -> int:
        """Get total number of admin users."""
        try:
            users = await self.redis_user_repo.list_all()
            admin_count = sum(1 for user in users if user.is_admin)
            return admin_count
        except Exception as e:
            current_app.logger.error(f"Failed to get admin count: {e}")
            return 0
    
    async def get_active_user_count(self) -> int:
        """Get total number of active users."""
        try:
            users = await self.redis_user_repo.list_all()
            active_count = sum(1 for user in users if user.is_active)
            return active_count
        except Exception as e:
            current_app.logger.error(f"Failed to get active user count: {e}")
            return 0
    
    async def get_all_users(self) -> List[User]:
        """Get all users."""
        try:
            users = await self.redis_user_repo.list_all()
            return users
        except Exception as e:
            current_app.logger.error(f"Failed to get all users: {e}")
            return []
    
    async def get_sharing_users(self) -> List[User]:
        """Get users who share reading activity or current reading."""
        try:
            users = await self.redis_user_repo.list_all()
            sharing_users = [
                user for user in users 
                if user.is_active and (user.share_reading_activity or user.share_current_reading)
            ]
            return sharing_users
        except Exception as e:
            current_app.logger.error(f"Failed to get sharing users: {e}")
            return []
    
    async def update_user(self, user: User) -> User:
        """Update a user in Redis."""
        try:
            updated_user = await self.redis_user_repo.update(user)
            current_app.logger.info(f"User {user.id} successfully updated in Redis")
            return updated_user
        except Exception as e:
            current_app.logger.error(f"Failed to update user in Redis: {e}")
            raise
    
    async def update_user_profile(self, user_id: str, username: Optional[str] = None, email: Optional[str] = None) -> Optional[User]:
        """Update a user's profile information."""
        try:
            # Get current user
            user = await self.redis_user_repo.get_by_id(user_id)
            if not user:
                return None
            
            # Update provided fields
            if username is not None:
                user.username = username
            if email is not None:
                user.email = email
            
            # Save updates
            updated_user = await self.redis_user_repo.update(user)
            current_app.logger.info(f"User profile {user_id} successfully updated")
            return updated_user
        except Exception as e:
            current_app.logger.error(f"Failed to update user profile: {e}")
            raise
    
    async def update_user_password(self, user_id: str, password_hash: str, clear_must_change: bool = True) -> Optional[User]:
        """Update a user's password."""
        try:
            # Get current user
            user = await self.redis_user_repo.get_by_id(user_id)
            if not user:
                return None
            
            # Update password fields
            user.password_hash = password_hash
            user.password_changed_at = datetime.now()
            if clear_must_change:
                user.password_must_change = False
            
            # Save updates
            updated_user = await self.redis_user_repo.update(user)
            current_app.logger.info(f"User password {user_id} successfully updated")
            return updated_user
        except Exception as e:
            current_app.logger.error(f"Failed to update user password: {e}")
            raise

    # Sync wrappers
    @run_async
    def create_user_sync(self, username: str, email: str, password_hash: str,
                         is_admin: bool = False, is_active: bool = True,
                         password_must_change: bool = False) -> User:
        """Sync wrapper for create_user."""
        return self.create_user(username, email, password_hash, is_admin, is_active, password_must_change)
    
    @run_async
    def get_user_by_id_sync(self, user_id: str) -> Optional[User]:
        """Sync wrapper for get_user_by_id."""
        return self.get_user_by_id(user_id)
    
    @run_async
    def get_user_by_username_sync(self, username: str) -> Optional[User]:
        """Sync wrapper for get_user_by_username."""
        return self.get_user_by_username(username)
    
    @run_async
    def get_user_by_email_sync(self, email: str) -> Optional[User]:
        """Sync wrapper for get_user_by_email."""
        return self.get_user_by_email(email)
    
    @run_async
    def get_user_by_username_or_email_sync(self, username_or_email: str) -> Optional[User]:
        """Sync wrapper for get_user_by_username_or_email."""
        return self.get_user_by_username_or_email(username_or_email)
    
    @run_async
    def get_user_count_sync(self) -> int:
        """Sync wrapper for get_user_count."""
        return self.get_user_count()
    
    @run_async
    def get_admin_count_sync(self) -> int:
        """Sync wrapper for get_admin_count."""
        return self.get_admin_count()
    
    @run_async
    def get_active_user_count_sync(self) -> int:
        """Sync wrapper for get_active_user_count."""
        return self.get_active_user_count()
    
    @run_async
    def get_all_users_sync(self) -> List[User]:
        """Sync wrapper for get_all_users."""
        return self.get_all_users()
    
    @run_async
    def get_sharing_users_sync(self) -> List[User]:
        """Sync wrapper for get_sharing_users."""
        return self.get_sharing_users()
    
    @run_async
    def update_user_sync(self, user: User) -> User:
        """Sync wrapper for update_user."""
        return self.update_user(user)
    
    @run_async
    def update_user_profile_sync(self, user_id: str, username: Optional[str] = None, email: Optional[str] = None) -> Optional[User]:
        """Sync wrapper for update_user_profile."""
        return self.update_user_profile(user_id, username, email)
    
    @run_async
    def update_user_password_sync(self, user_id: str, password_hash: str, clear_must_change: bool = True) -> Optional[User]:
        """Sync wrapper for update_user_password."""
        return self.update_user_password(user_id, password_hash, clear_must_change)


class RedisReadingLogService:
    """Service for managing reading logs using Redis as the sole data store."""
    
    def __init__(self):
        self.redis_enabled = os.getenv('GRAPH_DATABASE_ENABLED', 'true').lower() == 'true'
        
        if not self.redis_enabled:
            raise RuntimeError("Redis must be enabled for this version of Bibliotheca")
            
        self.storage = get_graph_storage()
    
    @run_async
    async def get_existing_log(self, book_id: str, user_id: str, date: date) -> Optional['ReadingLog']:
        """Get an existing reading log for a specific book, user, and date."""
        from app.domain.models import ReadingLog
        
        # Create a key for this specific reading log
        log_key = f"reading_log:{user_id}:{book_id}:{date.isoformat()}"
        
        log_data = await self.storage.get_json(log_key)
        if log_data:
            # Convert back to ReadingLog domain object
            return ReadingLog(
                id=log_data['id'],
                book_id=log_data['book_id'],
                user_id=log_data['user_id'],
                date=datetime.fromisoformat(log_data['date']).date(),
                created_at=datetime.fromisoformat(log_data['created_at']) if log_data.get('created_at') else datetime.utcnow()
            )
        return None
    
    @run_async
    async def create_reading_log(self, book_id: str, user_id: str, date: date) -> 'ReadingLog':
        """Create a new reading log entry."""
        from app.domain.models import ReadingLog
        import uuid
        
        # Create the reading log
        log_id = str(uuid.uuid4())
        reading_log = ReadingLog(
            id=log_id,
            book_id=book_id,
            user_id=user_id,
            date=date,
            created_at=datetime.utcnow()
        )
        
        # Store in Redis
        log_key = f"reading_log:{user_id}:{book_id}:{date.isoformat()}"
        log_data = {
            'id': reading_log.id,
            'book_id': reading_log.book_id,
            'user_id': reading_log.user_id,
            'date': reading_log.date.isoformat(),
            'created_at': reading_log.created_at.isoformat()
        }
        
        await self.storage.set_json(log_key, log_data)
        
        # Also add to user's reading log index for easy retrieval
        user_logs_key = f"user_reading_logs:{user_id}"
        await self.storage.add_to_sorted_set(user_logs_key, log_key, reading_log.date.toordinal())
        
        return reading_log
    
    @run_async
    async def get_user_logs_count(self, user_id: str) -> int:
        """Get the total count of reading logs for a user."""
        user_logs_key = f"user_reading_logs:{user_id}"
        return await self.storage.get_sorted_set_size(user_logs_key)
    
    @run_async
    async def get_recent_shared_logs(self, days_back: int = 7, limit: int = 50) -> List['ReadingLog']:
        """Get recent reading logs from users who share reading activity."""
        from app.domain.models import ReadingLog
        
        # This is a simplified implementation - get logs from all users for now
        # In a real implementation, you'd filter to only users who share reading activity
        logs = []
        
        # Get cutoff date
        cutoff_date = datetime.now().date() - timedelta(days=days_back)
        
        # For now, return empty list - this would need proper implementation
        # to scan through user logs and filter by sharing settings
        return logs


class RedisCustomFieldService:
    """Service for managing custom metadata fields using Redis as the sole data store."""
    
    def __init__(self):
        self.redis_enabled = os.getenv('GRAPH_DATABASE_ENABLED', 'true').lower() == 'true'
        
        if not self.redis_enabled:
            raise RuntimeError("Redis must be enabled for this version of Bibliotheca")
            
        self.storage = get_graph_storage()
        self.redis_custom_field_repo = RedisCustomFieldRepository(self.storage)
    
    async def create_field(self, field_definition: CustomFieldDefinition) -> CustomFieldDefinition:
        """Create a custom field definition."""
        try:
            # Generate ID if not set
            if not field_definition.id:
                import uuid
                field_definition.id = str(uuid.uuid4())
            
            # Store in Redis
            await self.redis_custom_field_repo.create(field_definition)
            print(f"📝 [CUSTOM_FIELD] Created field: {field_definition.name} (ID: {field_definition.id})")
            return field_definition
        except Exception as e:
            print(f"❌ [CUSTOM_FIELD] Failed to create field: {e}")
            raise
    
    async def get_user_fields(self, user_id: str) -> List[CustomFieldDefinition]:
        """Get all custom fields for a user."""
        try:
            fields = await self.redis_custom_field_repo.get_by_user(str(user_id))
            print(f"📋 [CUSTOM_FIELD] Retrieved {len(fields)} fields for user {user_id}")
            return fields
        except Exception as e:
            print(f"❌ [CUSTOM_FIELD] Failed to get user fields: {e}")
            return []
    
    async def get_shareable_fields(self, exclude_user_id: Optional[str] = None) -> List[CustomFieldDefinition]:
        """Get shareable custom fields (excluding those created by specific user)."""
        try:
            fields = await self.redis_custom_field_repo.get_shareable(exclude_user_id)
            print(f"📋 [CUSTOM_FIELD] Retrieved {len(fields)} shareable fields")
            return fields
        except Exception as e:
            print(f"❌ [CUSTOM_FIELD] Failed to get shareable fields: {e}")
            return []
    
    def get_custom_metadata_for_display(self, custom_metadata: Dict[str, Any]) -> List[Dict[str, str]]:
        """Convert custom metadata to display format."""
        if not custom_metadata:
            return []
        
        display_data = []
        for field_name, value in custom_metadata.items():
            # Convert value to string for display
            if value is not None:
                display_data.append({
                    'display_name': field_name.replace('_', ' ').title(),
                    'display_value': str(value),
                    'field_name': field_name
                })
        
        return display_data
    
    def get_available_fields(self, user_id: str, is_global: bool = False) -> List[CustomFieldDefinition]:
        """Get available custom fields for a user.
        
        Args:
            user_id: The user ID to get fields for
            is_global: If True, return shareable/global fields. If False, return user-specific fields.
        """
        try:
            print(f"🔍 [FIELDS] Getting available fields for user {user_id}, is_global={is_global}")
            
            if is_global:
                # Return shareable fields (global fields available to all users)
                fields = self.get_shareable_fields_sync(exclude_user_id=user_id)
                print(f"📋 [FIELDS] Retrieved {len(fields)} shareable fields")
                for field in fields:
                    print(f"   🌐 [GLOBAL] Field: {field.name} (display: {field.display_name}, is_global: {field.is_global}, is_shareable: {field.is_shareable})")
                return fields
            else:
                # Return user-specific fields
                fields = self.get_user_fields_sync(user_id)
                print(f"📋 [FIELDS] Retrieved {len(fields)} user fields for {user_id}")
                for field in fields:
                    print(f"   👤 [PERSONAL] Field: {field.name} (display: {field.display_name}, is_global: {field.is_global}, is_shareable: {field.is_shareable})")
                return fields
        except Exception as e:
            print(f"❌ [FIELDS] Error getting available fields (is_global={is_global}): {e}")
            return []
    
    # Sync wrappers
    @run_async
    def create_field_sync(self, field_definition: CustomFieldDefinition) -> CustomFieldDefinition:
        """Sync wrapper for create_field."""
        return self.create_field(field_definition)
    
    @run_async
    def get_user_fields_sync(self, user_id: str) -> List[CustomFieldDefinition]:
        """Sync wrapper for get_user_fields."""
        return self.get_user_fields(user_id)
    
    @run_async
    def get_shareable_fields_sync(self, exclude_user_id: Optional[str] = None) -> List[CustomFieldDefinition]:
        """Sync wrapper for get_shareable_fields."""
        return self.get_shareable_fields(exclude_user_id)
    
    def get_popular_fields_sync(self, limit: int = 10) -> List[CustomFieldDefinition]:
        """Get popular custom fields using calculated usage statistics."""
        try:
            # Get shareable fields with calculated usage
            shareable = self.get_shareable_fields_with_calculated_usage_sync()
            # Sort by usage count and return top fields
            shareable.sort(key=lambda x: x.usage_count or 0, reverse=True)
            return shareable[:limit]
        except Exception as e:
            print(f"❌ [CUSTOM_FIELD] Failed to get popular fields: {e}")
            return []
    
    async def get_field_by_id(self, field_id: str) -> Optional[CustomFieldDefinition]:
        """Get a custom field by ID."""
        try:
            field = await self.redis_custom_field_repo.get_by_id(field_id)
            return field
        except Exception as e:
            print(f"❌ [CUSTOM_FIELD] Failed to get field {field_id}: {e}")
            return None
    
    @run_async
    def get_field_by_id_sync(self, field_id: str) -> Optional[CustomFieldDefinition]:
        """Sync wrapper for get_field_by_id."""
        return self.get_field_by_id(field_id)
    
    async def update_field(self, field_definition: CustomFieldDefinition) -> CustomFieldDefinition:
        """Update a custom field definition."""
        try:
            updated_field = await self.redis_custom_field_repo.update(field_definition)
            print(f"📝 [CUSTOM_FIELD] Updated field: {field_definition.name}")
            return updated_field
        except Exception as e:
            print(f"❌ [CUSTOM_FIELD] Failed to update field: {e}")
            raise
    
    @run_async
    def update_field_sync(self, field_definition: CustomFieldDefinition) -> CustomFieldDefinition:
        """Sync wrapper for update_field."""
        return self.update_field(field_definition)
    
    async def delete_field(self, field_id: str) -> bool:
        """Delete a custom field definition."""
        try:
            success = await self.redis_custom_field_repo.delete(field_id)
            print(f"🗑️ [CUSTOM_FIELD] Deleted field: {field_id}")
            return success
        except Exception as e:
            print(f"❌ [CUSTOM_FIELD] Failed to delete field: {e}")
            return False
    
    @run_async
    def delete_field_sync(self, field_id: str) -> bool:
        """Sync wrapper for delete_field."""
        return self.delete_field(field_id)
    
    async def calculate_field_usage(self, field_names: Optional[List[str]] = None) -> Dict[str, int]:
        """Calculate actual usage count for custom fields by scanning all user-book relationships."""
        try:
            print(f"� [CUSTOM_FIELD] Calculating actual usage for fields")
            
            usage_counts = {}
            
            # Get all user-book relationships from Redis
            from .infrastructure.redis_repositories import RedisUserBookRepository, RedisUserRepository
            user_book_repo = RedisUserBookRepository(self.storage)
            
            # Get all users to scan their relationships
            user_repo = RedisUserRepository(self.storage)
            all_users = await user_repo.list_all()
            
            for user in all_users:
                if not user.id:
                    continue
                try:
                    # Get all relationships for this user
                    user_relationships = await user_book_repo.get_user_library(user.id)
                    
                    for relationship in user_relationships:
                        if relationship.custom_metadata:
                            # Count usage of each field
                            for field_name, field_value in relationship.custom_metadata.items():
                                if field_value is not None and field_value != "":  # Only count if there's actual data
                                    usage_counts[field_name] = usage_counts.get(field_name, 0) + 1
                
                except Exception as e:
                    print(f"⚠️ [CUSTOM_FIELD] Error scanning relationships for user {user.id}: {e}")
                    continue
            
            print(f"📈 [CUSTOM_FIELD] Calculated usage counts: {usage_counts}")
            return usage_counts
            
        except Exception as e:
            print(f"❌ [CUSTOM_FIELD] Failed to calculate field usage: {e}")
            return {}
    
    async def get_user_fields_with_calculated_usage(self, user_id: str) -> List[CustomFieldDefinition]:
        """Get user fields with calculated usage counts."""
        try:
            # Get user fields
            user_fields = await self.get_user_fields(user_id)
            
            if not user_fields:
                return []
            
            # Calculate actual usage
            field_names = [field.name for field in user_fields]
            usage_counts = await self.calculate_field_usage(field_names)
            
            # Update usage counts on field objects
            for field in user_fields:
                field.usage_count = usage_counts.get(field.name, 0)
            
            return user_fields
            
        except Exception as e:
            print(f"❌ [CUSTOM_FIELD] Failed to get user fields with calculated usage: {e}")
            return await self.get_user_fields(user_id)
    
    async def get_shareable_fields_with_calculated_usage(self, exclude_user_id: Optional[str] = None) -> List[CustomFieldDefinition]:
        """Get shareable fields with calculated usage counts."""
        try:
            # Get shareable fields
            shareable_fields = await self.get_shareable_fields(exclude_user_id)
            
            if not shareable_fields:
                return []
            
            # Calculate actual usage
            field_names = [field.name for field in shareable_fields]
            usage_counts = await self.calculate_field_usage(field_names)
            
            # Update usage counts on field objects
            for field in shareable_fields:
                field.usage_count = usage_counts.get(field.name, 0)
            
            return shareable_fields
            
        except Exception as e:
            print(f"❌ [CUSTOM_FIELD] Failed to get shareable fields with calculated usage: {e}")
            return await self.get_shareable_fields(exclude_user_id)
    
    @run_async
    def calculate_field_usage_sync(self, field_names: Optional[List[str]] = None) -> Dict[str, int]:
        """Sync wrapper for calculate_field_usage."""
        return self.calculate_field_usage(field_names)
    
    @run_async
    def get_user_fields_with_calculated_usage_sync(self, user_id: str) -> List[CustomFieldDefinition]:
        """Sync wrapper for get_user_fields_with_calculated_usage."""
        return self.get_user_fields_with_calculated_usage(user_id)
    
    @run_async
    def get_shareable_fields_with_calculated_usage_sync(self, exclude_user_id: Optional[str] = None) -> List[CustomFieldDefinition]:
        """Sync wrapper for get_shareable_fields_with_calculated_usage."""
        return self.get_shareable_fields_with_calculated_usage(exclude_user_id)
    

class RedisImportMappingService:
    """Service for managing import mapping templates using Redis as the sole data store."""
    
    def __init__(self):
        self.redis_enabled = os.getenv('GRAPH_DATABASE_ENABLED', 'true').lower() == 'true'
        
        if not self.redis_enabled:
            raise RuntimeError("Redis must be enabled for this version of Bibliotheca")
            
        self.storage = get_graph_storage()
        self.redis_import_mapping_repo = RedisImportMappingRepository(self.storage)
    
    async def create_mapping_template(self, template: ImportMappingTemplate) -> ImportMappingTemplate:
        """Create an import mapping template."""
        try:
            # Generate ID if not set
            if not template.id:
                import uuid
                template.id = str(uuid.uuid4())
            
            # Store in Redis
            await self.redis_import_mapping_repo.create(template)
            print(f"📝 [MAPPING] Created template: {template.name} (ID: {template.id})")
            return template
        except Exception as e:
            print(f"❌ [MAPPING] Failed to create template: {e}")
            raise
    
    async def get_template_by_id(self, template_id: str) -> Optional[ImportMappingTemplate]:
        """Get a mapping template by ID."""
        try:
            template = await self.redis_import_mapping_repo.get_by_id(template_id)
            return template
        except Exception as e:
            print(f"❌ [MAPPING] Failed to get template {template_id}: {e}")
            return None
    
    async def get_templates_for_user(self, user_id: str) -> List[ImportMappingTemplate]:
        """Get all mapping templates for a user."""
        try:
            templates = await self.redis_import_mapping_repo.get_by_user(str(user_id))
            print(f"📋 [MAPPING] Retrieved {len(templates)} templates for user {user_id}")
            return templates
        except Exception as e:
            print(f"❌ [MAPPING] Failed to get user templates: {e}")
            return []
    
    async def get_all_templates(self) -> List[ImportMappingTemplate]:
        """Get all mapping templates."""
        try:
            templates = await self.redis_import_mapping_repo.get_all()
            print(f"📋 [MAPPING] Retrieved {len(templates)} total templates")
            return templates
        except Exception as e:
            print(f"❌ [MAPPING] Failed to get all templates: {e}")
            return []
    
    # Sync wrappers
    @run_async
    def create_mapping_template_sync(self, template: ImportMappingTemplate) -> ImportMappingTemplate:
        """Sync wrapper for create_mapping_template."""
        return self.create_mapping_template(template)
    
    @run_async
    def get_template_by_id_sync(self, template_id: str) -> Optional[ImportMappingTemplate]:
        """Sync wrapper for get_template_by_id."""
        return self.get_template_by_id(template_id)
    
    @run_async
    def get_templates_for_user_sync(self, user_id: str) -> List[ImportMappingTemplate]:
        """Sync wrapper for get_templates_for_user."""
        return self.get_templates_for_user(user_id)
    
    @run_async
    def get_user_templates_sync(self, user_id: str) -> List[ImportMappingTemplate]:
        """Alias for get_templates_for_user_sync for backward compatibility."""
        return self.get_templates_for_user(user_id)
    
    @run_async
    def get_all_templates_sync(self) -> List[ImportMappingTemplate]:
        """Sync wrapper for get_all_templates."""
        return self.get_all_templates()
    
    def create_template_sync(self, template: ImportMappingTemplate) -> ImportMappingTemplate:
        """Alias for create_mapping_template_sync for backward compatibility."""
        return self.create_mapping_template_sync(template)


class RedisDirectImportService:
    """Service for direct import of CSV files using Redis as the sole data store."""
    
    def __init__(self):
        self.redis_enabled = os.getenv('GRAPH_DATABASE_ENABLED', 'true').lower() == 'true'
        
        if not self.redis_enabled:
            raise RuntimeError("Redis must be enabled for this version of Bibliotheca")
            
        self.storage = get_graph_storage()
        self.book_service = RedisBookService()
        self.custom_field_service = RedisCustomFieldService()
    
    async def direct_import(self, file_path: str, user_id: str, source: str = 'goodreads') -> Dict[str, Any]:
        """Import books directly from a CSV file."""
        try:
            print(f"🚀 [IMPORT] Starting direct import from {file_path} for user {user_id}")
            
            results = {
                'success': True,
                'total_processed': 0,
                'books_imported': 0,
                'custom_fields_created': 0,
                'errors': []
            }
            
            # Read and process CSV
            with open(file_path, 'r', encoding='utf-8-sig') as csvfile:
                # Detect delimiter
                sample = csvfile.read(1024)
                csvfile.seek(0)
                sniffer = csv.Sniffer()
                delimiter = sniffer.sniff(sample).delimiter
                
                reader = csv.DictReader(csvfile, delimiter=delimiter)
                rows = list(reader)
                
            print(f"📚 [IMPORT] Found {len(rows)} rows to process")
            
            # Define custom field mappings for common platforms
            custom_field_mappings = {
                'goodreads': {
                    'My Rating': 'user_rating',
                    'Average Rating': 'average_rating', 
                    'Bookshelves': 'shelves',
                    'Bookshelves with positions': 'shelves_with_positions',
                    'Exclusive Shelf': 'reading_status',
                    'My Review': 'user_review',
                    'Spoiler': 'has_spoilers',
                    'Private Notes': 'private_notes',
                    'Read Count': 'read_count',
                    'Owned Copies': 'owned_copies'
                },
                'storygraph': {
                    'Star Rating': 'user_rating',
                    'Review': 'user_review',
                    'Tags': 'tags',
                    'Moods': 'moods',
                    'Pace': 'pace',
                    'Character Development': 'character_development',
                    'Plot Development': 'plot_development',
                    'Lovability': 'lovability',
                    'Diversity': 'diversity',
                    'Flaws': 'flaws'
                }
            }
            
            # Get field mappings for the source
            field_mappings = custom_field_mappings.get(source.lower(), {})
            
            # Process each row
            for idx, row in enumerate(rows):
                try:
                    print(f"📖 [IMPORT] Processing row {idx + 1}: {row.get('Title', 'Unknown')}")
                    
                    # Extract basic book data
                    book_data = self._extract_book_data(row, source)
                    if not book_data:
                        print(f"⚠️ [IMPORT] Skipping row {idx + 1}: No title found")
                        continue
                    
                    # Debug: Check if cover_url is in book_data
                    if 'cover_url' in book_data:
                        print(f"📷 [IMPORT] Book data contains cover_url: {book_data['cover_url']}")
                    else:
                        print(f"📷 [IMPORT] Book data does not contain cover_url")
                    
                    # Create or find book
                    domain_book = Book(**book_data)
                    print(f"📚 [IMPORT] Created domain book with cover_url: {domain_book.cover_url}")
                    book = await self.book_service.find_or_create_book(domain_book)
                    print(f"📚 [IMPORT] Final book has cover_url: {book.cover_url}")
                    
                    # Extract custom metadata
                    custom_metadata = {}
                    for csv_field, db_field in field_mappings.items():
                        if csv_field in row and row[csv_field]:
                            custom_metadata[db_field] = row[csv_field]
                    
                    # Add any additional fields not in mappings as custom fields
                    excluded_fields = {'Title', 'Author', 'Author l-f', 'Additional Authors', 'ISBN', 'ISBN13', 
                                     'Publisher', 'Publication Year', 'Original Publication Year', 'Date Read', 
                                     'Date Added', 'Number of Pages', 'Year Published', 'Original Title'}
                    
                    for field_name, value in row.items():
                        if field_name not in excluded_fields and field_name not in field_mappings and value:
                            # Clean field name for use as metadata key
                            clean_field_name = re.sub(r'[^\w\s-]', '', field_name).strip().replace(' ', '_').lower()
                            custom_metadata[clean_field_name] = value
                    
                    print(f"🏷️ [IMPORT] Extracted {len(custom_metadata)} custom metadata fields")
                    
                    # Create custom field definitions for new fields
                    for field_name in custom_metadata.keys():
                        try:
                            # Check if field definition exists
                            existing_fields = await self.custom_field_service.get_user_fields(str(user_id))
                            field_exists = any(f.name == field_name for f in existing_fields)
                            
                            if not field_exists:
                                # Create new field definition
                                field_def = CustomFieldDefinition(
                                    name=field_name,
                                    display_name=field_name.replace('_', ' ').title(),
                                    field_type=CustomFieldType.TEXT,
                                    created_by_user_id=str(user_id),
                                    is_shareable=False,
                                    created_at=datetime.now()
                                )
                                await self.custom_field_service.create_field(field_def)
                                results['custom_fields_created'] += 1
                                print(f"✨ [IMPORT] Created custom field: {field_name}")
                        except Exception as e:
                            print(f"⚠️ [IMPORT] Failed to create custom field {field_name}: {e}")
                    
                    # Determine reading status and dates
                    reading_status = self._extract_reading_status(row, source)
                    date_read = self._extract_date_read(row, source)
                    date_added = self._extract_date_added(row, source)
                    
                    # Check if book has an ID
                    if not book.id:
                        print(f"❌ [IMPORT] Book {book.title} has no ID, skipping")
                        results['errors'].append(f"Book {book.title} has no ID")
                        continue
                    
                    # Add book to user library with custom metadata
                    success = await self.book_service.add_book_to_user_library(
                        user_id=str(user_id),
                        book_id=book.id,
                        reading_status=reading_status,
                        custom_metadata=custom_metadata,
                        finish_date=date_read,
                        date_added=date_added or date.today()
                    )
                    
                    if success:
                        results['books_imported'] += 1
                        print(f"✅ [IMPORT] Successfully imported: {book.title}")
                    else:
                        print(f"❌ [IMPORT] Failed to add book to library: {book.title}")
                        results['errors'].append(f"Failed to add {book.title} to library")
                    
                    results['total_processed'] += 1
                    
                except Exception as e:
                    print(f"❌ [IMPORT] Error processing row {idx + 1}: {e}")
                    results['errors'].append(f"Row {idx + 1}: {str(e)}")
                    import traceback
                    traceback.print_exc()
            
            print(f"📝 [IMPORT] Import completed - {results['books_imported']} books imported, {results['custom_fields_created']} custom fields created")
            print(f"📝 Stored {len(field_mappings)} custom fields for each book")
            
            return results
            
        except Exception as e:
            print(f"❌ [IMPORT] Import failed: {e}")
            import traceback
            traceback.print_exc()
            return {
                'success': False,
                'error': str(e),
                'total_processed': 0,
                'books_imported': 0,
                'custom_fields_created': 0,
                'errors': [str(e)]
            }
    
    def _extract_book_data(self, row: Dict[str, str], source: str) -> Optional[Dict[str, Any]]:
        """Extract basic book data from CSV row."""
        try:
            title = row.get('Title', '').strip()
            if not title:
                return None
            
            # Common field mappings
            author_fields = ['Author', 'Author l-f', 'Primary Author']
            isbn_fields = ['ISBN', 'ISBN13', 'ISBN 13', 'ISBN10', 'ISBN 10']
            publisher_fields = ['Publisher', 'Publication']
            page_fields = ['Number of Pages', 'Pages', 'Page Count']
            year_fields = ['Publication Year', 'Year Published', 'Published Year', 'Original Publication Year']
            
            # Extract author
            author_name = None
            for field in author_fields:
                if field in row and row[field]:
                    author_name = row[field].strip()
                    break
            
            # Extract ISBN and normalize it
            isbn = None
            for field in isbn_fields:
                if field in row and row[field]:
                    # Use the normalization function to extract digits only
                    normalized_isbn = normalize_isbn_upc(row[field])
                    if normalized_isbn:
                        isbn = normalized_isbn
                        print(f"📚 [EXTRACT] Found ISBN in field '{field}': {row[field]} -> normalized: {isbn}")
                        break
            
            if not isbn:
                print(f"📚 [EXTRACT] No ISBN found for book: {title}")
                # List available fields for debugging
                available_fields = [f for f in row.keys() if row[f]]
                print(f"📚 [EXTRACT] Available fields: {available_fields}")
            
            # Extract publisher
            publisher = None
            for field in publisher_fields:
                if field in row and row[field]:
                    publisher = row[field].strip()
                    break
            
            # Extract page count
            page_count = None
            for field in page_fields:
                if field in row and row[field]:
                    try:
                        page_count = int(row[field])
                        break
                    except (ValueError, TypeError):
                        continue
            
            # Extract publication date from various possible fields
            published_date = None
            publication_fields = ['Publication Year', 'Year Published', 'Published Year', 'Original Publication Year', 'Published Date', 'Publication Date', 'Published']
            
            for field in publication_fields:
                if field in row and row[field]:
                    date_value = row[field].strip()
                    if date_value and date_value != '0':
                        # Use enhanced date parser that handles all formats
                        parsed_date = self._detect_and_parse_date(date_value, f"published_date from {field}")
                        if parsed_date:
                            published_date = parsed_date
                            print(f"📅 [EXTRACT] Set publication date from '{field}': {date_value} -> {parsed_date}")
                            break
            
            # Create book data
            book_data = {
                'title': title,
                'description': row.get('Description', ''),
                'page_count': page_count,
                'created_at': datetime.now(),
                'updated_at': datetime.now()
            }
            
            # Add the parsed publication date
            if published_date:
                book_data['published_date'] = published_date
            
            # Add ISBN (prefer ISBN13)
            if isbn:
                if len(isbn) == 13:
                    book_data['isbn13'] = isbn
                elif len(isbn) == 10:
                    book_data['isbn10'] = isbn
                else:
                    book_data['isbn13'] = isbn  # Store as ISBN13 by default
                
                # Try to fetch cover image and metadata from Google Books, then OpenLibrary as fallback
                try:
                    print(f"🔍 [EXTRACT] Attempting to fetch cover and metadata for ISBN: {isbn}")
                    from .utils import get_google_books_cover, fetch_book_data
                    
                    # Try Google Books first - fetch full metadata including publication date
                    google_data = get_google_books_cover(isbn, fetch_title_author=True)
                    if google_data and isinstance(google_data, dict):
                        if google_data.get('cover'):
                            book_data['cover_url'] = google_data['cover']
                            print(f"📷 [EXTRACT] Found Google Books cover for ISBN {isbn}: {google_data['cover']}")
                        
                        # Use API publication date if available and more detailed than CSV date
                        api_pub_date = google_data.get('published_date')
                        if api_pub_date:
                            parsed_date = self._detect_and_parse_date(api_pub_date, "published_date from Google Books API")
                            if parsed_date:
                                # Only replace CSV date if API provides more detail (month/day) or if CSV had no date
                                if not book_data.get('published_date') or len(str(api_pub_date).split('-')) > 1:
                                    book_data['published_date'] = parsed_date
                                    print(f"📅 [EXTRACT] Updated publication date from Google Books API: {api_pub_date} -> {parsed_date}")
                        
                        # Use other metadata if not already present
                        if google_data.get('description') and not book_data.get('description'):
                            book_data['description'] = google_data['description']
                            print(f"📝 [EXTRACT] Added description from Google Books API")
                        
                        if google_data.get('page_count') and not book_data.get('page_count'):
                            book_data['page_count'] = google_data['page_count']
                            print(f"📄 [EXTRACT] Added page count from Google Books API: {google_data['page_count']}")
                            
                    else:
                        # If Google Books didn't work, try just the cover URL
                        cover_url = get_google_books_cover(isbn)
                        if cover_url:
                            book_data['cover_url'] = cover_url
                            print(f"📷 [EXTRACT] Found Google Books cover for ISBN {isbn}: {cover_url}")
                        else:
                            print(f"📷 [EXTRACT] No Google Books cover found, trying OpenLibrary...")
                            # Fallback to OpenLibrary
                            openlibrary_data = fetch_book_data(isbn)
                            if openlibrary_data:
                                if openlibrary_data.get('cover'):
                                    book_data['cover_url'] = openlibrary_data['cover']
                                    print(f"📷 [EXTRACT] Found OpenLibrary cover for ISBN {isbn}: {openlibrary_data['cover']}")
                                
                                # Use OpenLibrary publication date if available
                                ol_pub_date = openlibrary_data.get('published_date')
                                if ol_pub_date and not book_data.get('published_date'):
                                    parsed_date = self._detect_and_parse_date(ol_pub_date, "published_date from OpenLibrary")
                                    if parsed_date:
                                        book_data['published_date'] = parsed_date
                                        print(f"📅 [EXTRACT] Set publication date from OpenLibrary: {ol_pub_date} -> {parsed_date}")
                            else:
                                print(f"📷 [EXTRACT] No cover found for ISBN {isbn}")
                except Exception as e:
                    print(f"⚠️ [EXTRACT] Failed to fetch cover/metadata for ISBN {isbn}: {e}")
                    import traceback
                    traceback.print_exc()
            
            # Add authors
            if author_name:
                from .domain.models import Author
                book_data['authors'] = [Author(name=author_name)]
            else:
                book_data['authors'] = []
            
            # Add publisher
            if publisher:
                from .domain.models import Publisher
                book_data['publisher'] = Publisher(name=publisher)
            
            return book_data
            
        except Exception as e:
            print(f"❌ [EXTRACT] Error extracting book data: {e}")
            return None
    
    def _detect_and_parse_date(self, date_str: str, field_name: str = "date") -> Optional[date]:
        """
        Detect date format and parse from various formats, stripping time components.
        Supports formats from any source (Goodreads, StoryGraph, bulk import, etc.)
        """
        if not date_str:
            return None
        
        try:
            date_str = str(date_str).strip()
            if not date_str or date_str.lower() in ['', 'null', 'none', 'n/a', '0']:
                return None
            
            print(f"📅 [DATE_PARSE] Parsing {field_name}: '{date_str}'")
            
            # First, handle datetime strings with time components (strip the time)
            # Check for ISO format with time: 2004-06-15T00:00:00, 2004-06-15 00:00:00
            if 'T' in date_str or (' ' in date_str and ':' in date_str):
                # Split on 'T' or first space to get just the date part
                date_str = date_str.split('T')[0].split(' ')[0]
                print(f"📅 [DATE_PARSE] Stripped time component, now: '{date_str}'")
            
            # Define comprehensive list of date formats to try
            # Order matters - more specific formats first
            date_formats = [
                # Full dates with separators
                "%Y-%m-%d",      # 2004-06-15 (ISO format)
                "%m/%d/%Y",      # 06/15/2004 (US format)
                "%d/%m/%Y",      # 15/06/2004 (EU format)
                "%m-%d-%Y",      # 06-15-2004
                "%d-%m-%Y",      # 15-06-2004
                "%m.%d.%Y",      # 06.15.2004
                "%d.%m.%Y",      # 15.06.2004
                
                # Year-month formats
                "%Y-%m",         # 2004-06
                "%Y/%m",         # 2004/06
                "%m/%Y",         # 06/2004
                "%Y.%m",         # 2004.06
                "%m.%Y",         # 06.2004
                
                # Year only
                "%Y",            # 2004
                
                # Handle some text month formats
                "%B %Y",         # June 2004
                "%b %Y",         # Jun 2004
                "%Y %B",         # 2004 June
                "%Y %b",         # 2004 Jun
                "%B %d, %Y",     # June 15, 2004
                "%b %d, %Y",     # Jun 15, 2004
                "%d %B %Y",      # 15 June 2004
                "%d %b %Y",      # 15 Jun 2004
                
                # Reverse formats (less common but possible)
                "%d-%m-%Y",      # 15-06-2004 (EU)
                "%d/%m/%Y",      # 15/06/2004 (EU)
            ]
            
            # Try each format
            for fmt in date_formats:
                try:
                    parsed_datetime = datetime.strptime(date_str, fmt)
                    result_date = parsed_datetime.date()
                    print(f"✅ [DATE_PARSE] Successfully parsed '{date_str}' using format '{fmt}' -> {result_date}")
                    return result_date
                except ValueError:
                    continue
            
            # If standard formats fail, try some advanced parsing
            # Handle numeric-only strings that might be years
            if date_str.isdigit():
                year = int(date_str)
                if 1000 <= year <= 9999:  # Reasonable year range
                    result_date = date(year, 1, 1)
                    print(f"✅ [DATE_PARSE] Parsed year-only '{date_str}' -> {result_date}")
                    return result_date
                    
            # Handle decimal years (like 2004.5)
            try:
                if '.' in date_str and date_str.replace('.', '').isdigit():
                    year = int(float(date_str))
                    if 1000 <= year <= 9999:
                        result_date = date(year, 1, 1)
                        print(f"✅ [DATE_PARSE] Parsed decimal year '{date_str}' -> {result_date}")
                        return result_date
            except ValueError:
                pass
            
            # Try to extract year from mixed format strings
            import re
            year_match = re.search(r'\b(1[0-9]{3}|20[0-9]{2})\b', date_str)
            if year_match:
                year = int(year_match.group(1))
                if 1000 <= year <= 9999:
                    result_date = date(year, 1, 1)
                    print(f"✅ [DATE_PARSE] Extracted year from '{date_str}' -> {result_date}")
                    return result_date
            
            print(f"⚠️ [DATE_PARSE] Could not parse {field_name}: '{date_str}' with any known format")
            return None
            
        except Exception as e:
            print(f"❌ [DATE_PARSE] Error parsing {field_name} '{date_str}': {e}")
            return None

    def _parse_publication_date(self, date_str: str) -> Optional[date]:
        """Parse publication date using the enhanced date parser."""
        return self._detect_and_parse_date(date_str, "published_date")

    def _extract_reading_status(self, row: Dict[str, str], source: str) -> ReadingStatus:
        """Extract reading status from CSV row."""
        status_field = row.get('Exclusive Shelf', row.get('Read Status', row.get('Status', ''))).lower()
        
        if 'read' in status_field and 'currently' not in status_field and 'to-read' not in status_field:
            return ReadingStatus.READ
        elif 'currently' in status_field or 'reading' in status_field:
            return ReadingStatus.READING
        elif 'to-read' in status_field or 'want' in status_field or 'plan' in status_field:
            return ReadingStatus.PLAN_TO_READ
        else:
            return ReadingStatus.PLAN_TO_READ  # Default
    
    def _extract_date_read(self, row: Dict[str, str], source: str) -> Optional[date]:
        """Extract date read from CSV row using enhanced date parser."""
        date_fields = ['Date Read', 'Read Date', 'Finished Date', 'Date Finished', 'Last Date Read']
        
        for field in date_fields:
            if field in row and row[field]:
                date_str = row[field].strip()
                if date_str and date_str.lower() not in ['', 'null', 'none']:
                    parsed_date = self._detect_and_parse_date(date_str, f"date_read from {field}")
                    if parsed_date:
                        return parsed_date
        return None
    
    def _extract_date_added(self, row: Dict[str, str], source: str) -> Optional[date]:
        """Extract date added from CSV row using enhanced date parser."""
        date_fields = ['Date Added', 'Added Date', 'Date Imported']
        
        for field in date_fields:
            if field in row and row[field]:
                date_str = row[field].strip()
                if date_str and date_str.lower() not in ['', 'null', 'none']:
                    parsed_date = self._detect_and_parse_date(date_str, f"date_added from {field}")
                    if parsed_date:
                        return parsed_date
        return None

    # Sync wrapper
    @run_async
    def direct_import_sync(self, file_path: str, user_id: str, source: str = 'goodreads') -> Dict[str, Any]:
        """Sync wrapper for direct_import."""
        return self.direct_import(file_path, user_id, source)
    
    def detect_import_type(self, file_path: str) -> str:
        """Detect the type of CSV file (goodreads, storygraph, or unknown)."""
        try:
            with open(file_path, 'r', encoding='utf-8-sig') as csvfile:
                # Read the first line to get headers
                first_line = csvfile.readline().strip()
                if not first_line:
                    return 'unknown'
                
                # Parse headers
                import csv
                sniffer = csv.Sniffer()
                sample = first_line + '\n' + csvfile.readline()
                csvfile.seek(0)
                
                try:
                    delimiter = sniffer.sniff(sample).delimiter
                except:
                    delimiter = ','
                
                reader = csv.reader([first_line], delimiter=delimiter)
                headers = next(reader, [])
                
                if not headers:
                    return 'unknown'
                
                # Define signature fields for each platform
                goodreads_signatures = ['Book Id', 'Author l-f', 'Bookshelves', 'Exclusive Shelf']
                storygraph_signatures = ['Read Status', 'Moods', 'Pace', 'Character- or Plot-Driven?']
                
                # Check for Goodreads signatures
                goodreads_matches = sum(1 for sig in goodreads_signatures if sig in headers)
                storygraph_matches = sum(1 for sig in storygraph_signatures if sig in headers)
                
                print(f"🔍 [DETECT] Headers found: {headers}")
                print(f"🔍 [DETECT] Goodreads signature matches: {goodreads_matches}/{len(goodreads_signatures)}")
                print(f"🔍 [DETECT] StoryGraph signature matches: {storygraph_matches}/{len(storygraph_signatures)}")
                
                # Determine the most likely format
                if goodreads_matches >= 2:  # At least 2 signature fields
                    return 'goodreads'
                elif storygraph_matches >= 2:  # At least 2 signature fields
                    return 'storygraph'
                else:
                    return 'unknown'
                    
        except Exception as e:
            print(f"❌ [DETECT] Error detecting import type: {e}")
            return 'unknown'

    def detect_import_type_sync(self, file_path: str) -> str:
        """Sync wrapper for detect_import_type."""
        return self.detect_import_type(file_path)

# Global service instances
from werkzeug.local import LocalProxy
from flask import current_app

def _get_service(name):
    def _find():
        # This will fail if not in an app context.
        # That's what we want, to prevent using services before the app is ready.
        if not hasattr(current_app, name):
            # This can happen if Redis is not available and initialization in create_app failed
            raise RuntimeError(f"Service '{name}' not initialized on the app.")
        return getattr(current_app, name)
    return _find

book_service = LocalProxy(_get_service('book_service'))
user_service = LocalProxy(_get_service('user_service'))
reading_log_service = LocalProxy(_get_service('reading_log_service'))
custom_field_service = LocalProxy(_get_service('custom_field_service'))
import_mapping_service = LocalProxy(_get_service('import_mapping_service'))
direct_import_service = LocalProxy(_get_service('direct_import_service'))
