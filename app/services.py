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
import json
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
    
    # Person-related methods
    async def get_person_by_id(self, person_id: str):
        """Get a person by their ID."""
        try:
            # First look for the person in the Redis storage
            person_data = self.storage.get_node('person', person_id)
            
            if person_data:
                from .domain.models import Person
                # Convert datetime strings back to datetime objects
                if 'created_at' in person_data and isinstance(person_data['created_at'], str):
                    try:
                        person_data['created_at'] = datetime.fromisoformat(person_data['created_at'])
                    except:
                        person_data['created_at'] = datetime.utcnow()
                
                if 'updated_at' in person_data and isinstance(person_data['updated_at'], str):
                    try:
                        person_data['updated_at'] = datetime.fromisoformat(person_data['updated_at'])
                    except:
                        person_data['updated_at'] = datetime.utcnow()
                
                person = Person(**person_data)
                return person
            
            # If not found as person, try looking as author (for backward compatibility)
            author_data = self.storage.get_node('author', person_id)
            
            if author_data:
                from .domain.models import Person
                # Convert Author data to Person format
                person_data = {
                    'id': author_data.get('id'),
                    'name': author_data.get('name', ''),
                    'normalized_name': author_data.get('normalized_name', ''),
                    'birth_year': author_data.get('birth_year'),
                    'death_year': author_data.get('death_year'),
                    'bio': author_data.get('bio'),
                    'created_at': datetime.utcnow(),
                    'updated_at': datetime.utcnow()
                }
                
                person = Person(**person_data)
                print(f"✅ [SERVICE] Found author as person: {person.name}")
                return person
            
            print(f"❌ [SERVICE] No person or author found with ID: {person_id}")
            return None
        except Exception as e:
            print(f"❌ [SERVICE] Error getting person by ID {person_id}: {e}")
            import traceback
            traceback.print_exc()
            return None
    
    @run_async
    def get_person_by_id_sync(self, person_id: str):
        """Sync wrapper for get_person_by_id."""
        return self.get_person_by_id(person_id)
    
    async def search_persons(self, query: str, limit: int = 10):
        """Search for persons by name."""
        try:
            # Get all person nodes and filter by name
            person_nodes = self.storage.find_nodes_by_type('person', limit=limit * 2)
            
            results = []
            for person_data in person_nodes:
                if person_data:
                    name = person_data.get('name', '').lower()
                    normalized_name = person_data.get('normalized_name', '').lower()
                    
                    if query.lower() in name or query.lower() in normalized_name:
                        from .domain.models import Person
                        # Convert datetime strings
                        if 'created_at' in person_data and isinstance(person_data['created_at'], str):
                            try:
                                person_data['created_at'] = datetime.fromisoformat(person_data['created_at'])
                            except:
                                person_data['created_at'] = datetime.utcnow()
                        
                        if 'updated_at' in person_data and isinstance(person_data['updated_at'], str):
                            try:
                                person_data['updated_at'] = datetime.fromisoformat(person_data['updated_at'])
                            except:
                                person_data['updated_at'] = datetime.utcnow()
                        
                        person = Person(**person_data)
                        results.append(person)
                        
                        if len(results) >= limit:
                            break
            
            return results
        except Exception as e:
            print(f"❌ [SERVICE] Error searching persons: {e}")
            return []
    
    @run_async
    def search_persons_sync(self, query: str, limit: int = 10):
        """Sync wrapper for search_persons."""
        return self.search_persons(query, limit)
    
    async def get_books_by_person(self, person_id: str, user_id: Optional[str] = None):
        """Get all books associated with a person (by contribution type)."""
        try:
            books_by_type = {}
            
            # Find books that have WRITTEN_BY relationships to this person/author
            # We need to scan all books and check their relationships
            all_book_nodes = self.storage.find_nodes_by_type('book')
            
            authored_books = []
            for book_data in all_book_nodes:
                if not book_data or not book_data.get('_id'):
                    continue
                    
                book_id = book_data.get('_id')
                if not book_id:  # Ensure book_id is not None
                    continue
                
                # Get WRITTEN_BY relationships for this book
                relationships = self.storage.get_relationships('book', book_id, 'WRITTEN_BY')
                
                # Check if any of these relationships point to our person/author
                for rel in relationships:
                    if (rel.get('to_type') == 'author' and rel.get('to_id') == person_id) or \
                       (rel.get('to_type') == 'person' and rel.get('to_id') == person_id):
                        
                        # Get the full book object
                        book = await self.redis_book_repo.get_by_id(book_id)
                        if book:
                            # If user_id is provided, check if user has this book in their library
                            if user_id:
                                user_book_relationship = await self.redis_user_book_repo.get_relationship(str(user_id), book_id)
                                if user_book_relationship:
                                    # Add user-specific attributes
                                    setattr(book, 'reading_status', user_book_relationship.reading_status.value)
                                    setattr(book, 'ownership_status', user_book_relationship.ownership_status.value)
                                    setattr(book, 'start_date', user_book_relationship.start_date)
                                    setattr(book, 'finish_date', user_book_relationship.finish_date)
                                    setattr(book, 'user_rating', user_book_relationship.user_rating)
                                    setattr(book, 'personal_notes', user_book_relationship.personal_notes)
                                    setattr(book, 'date_added', user_book_relationship.date_added)
                                    setattr(book, 'user_tags', user_book_relationship.user_tags)
                                    setattr(book, 'locations', user_book_relationship.locations)
                                    setattr(book, 'custom_metadata', user_book_relationship.custom_metadata or {})
                                    authored_books.append(book)
                                    print(f"✅ [SERVICE] Added book {book.title} to authored books (user has it)")
                                else:
                                    print(f"⚠️ [SERVICE] User {user_id} doesn't have book {book.title} in library")
                            else:
                                # No user filter, include all books
                                authored_books.append(book)
                                print(f"✅ [SERVICE] Added book {book.title} to authored books (no user filter)")
                        break  # Found the relationship, no need to check other relationships for this book
            
            if authored_books:
                books_by_type['authored'] = authored_books
                print(f"✅ [SERVICE] Found {len(authored_books)} authored books")
            
            # Method 2: TODO - Look for other contribution types when we implement BookContribution relationships
            # For now, we'll primarily handle authored books since that's what we're creating in the book creation process
            
            print(f"📊 [SERVICE] Returning {len(books_by_type)} book types with {len(authored_books)} authored books for person {person_id}")
            return books_by_type
        except Exception as e:
            print(f"❌ [SERVICE] Error getting books by person {person_id}: {e}")
            import traceback
            traceback.print_exc()
            return {}

    @run_async
    def get_books_by_person_sync(self, person_id: str, user_id: Optional[str] = None):
        """Sync wrapper for get_books_by_person."""
        return self.get_books_by_person(person_id, user_id)
    
    async def list_all_persons(self):
        """List all persons in the storage and return as Person objects."""
        from app.domain.models import Person
        try:
            person_nodes = self.storage.find_nodes_by_type('person')
            
            # Also check for authors
            author_nodes = self.storage.find_nodes_by_type('author')
            
            # Convert raw data to Person objects
            all_persons = []
            for person_data in person_nodes + author_nodes:
                try:
                    person = Person(
                        id=person_data.get('_id'),
                        name=person_data.get('name', ''),
                        normalized_name=person_data.get('normalized_name', ''),
                        birth_year=person_data.get('birth_year'),
                        death_year=person_data.get('death_year'),
                        birth_place=person_data.get('birth_place'),
                        bio=person_data.get('bio'),
                        website=person_data.get('website')
                    )
                    all_persons.append(person)
                    print(f"✅ [SERVICE] Created Person object: {person.name} (ID: {person.id})")
                except Exception as e:
                    print(f"❌ [SERVICE] Error creating Person object from {person_data}: {e}")
            
            print(f"📊 [SERVICE] Returning {len(all_persons)} Person objects")
            return all_persons
        except Exception as e:
            print(f"❌ [SERVICE] Error listing persons: {e}")
            import traceback
            traceback.print_exc()
            return []
    
    @run_async
    def list_all_persons_sync(self):
        """Sync wrapper for list_all_persons."""
        return self.list_all_persons()


class RedisUserService:
    """Service for managing users using Redis as the sole data store."""
    
    def __init__(self):
        self.redis_enabled = os.getenv('GRAPH_DATABASE_ENABLED', 'true').lower() == 'true'
        
        if not self.redis_enabled:
            raise RuntimeError("Graph database is disabled, but RedisUserService requires it")
        
        self.storage = get_graph_storage()
        self.redis_user_repo = RedisUserRepository(self.storage)
    
    async def get_user_count(self) -> int:
        """Get the total number of users."""
        try:
            # Get all user nodes and count them
            user_nodes = self.storage.find_nodes_by_type('user')
            return len(user_nodes) if user_nodes else 0
        except Exception as e:
            print(f"❌ [USER_SERVICE] Error getting user count: {e}")
            return 0
    
    @run_async
    def get_user_count_sync(self) -> int:
        """Sync wrapper for get_user_count."""
        return self.get_user_count()
    
    async def get_user_by_id(self, user_id: str) -> Optional[User]:
        """Get a user by their ID."""
        try:
            return await self.redis_user_repo.get_by_id(user_id)
        except Exception as e:
            print(f"❌ [USER_SERVICE] Error getting user by ID {user_id}: {e}")
            return None
    
    @run_async
    def get_user_by_id_sync(self, user_id: str) -> Optional[User]:
        """Sync wrapper for get_user_by_id."""
        return self.get_user_by_id(user_id)
    
    async def create_user(self, username: str, email: str, password_hash: str, 
                        is_admin: bool = False, is_active: bool = True, 
                        password_must_change: bool = False) -> User:
        """Create a new user."""
        try:
            # Generate a unique ID
            user_id = str(uuid.uuid4())
            
            # Create domain user object
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
            
            # Create user in Redis
            created_user = await self.redis_user_repo.create(domain_user)
            print(f"✅ [USER_SERVICE] User {username} created successfully with ID: {user_id}")
            return created_user
        except Exception as e:
            print(f"❌ [USER_SERVICE] Error creating user {username}: {e}")
            raise
    
    @run_async
    def create_user_sync(self, username: str, email: str, password_hash: str,
                        is_admin: bool = False, is_active: bool = True,
                        password_must_change: bool = False) -> User:
        """Sync wrapper for create_user."""
        return self.create_user(username, email, password_hash, is_admin, is_active, password_must_change)
    
    async def update_user(self, user: User) -> User:
        """Update an existing user."""
        try:
            return await self.redis_user_repo.update(user)
        except Exception as e:
            print(f"❌ [USER_SERVICE] Error updating user: {e}")
            raise
    
    @run_async
    def update_user_sync(self, user: User) -> User:
        """Sync wrapper for update_user."""
        return self.update_user(user)
    
    async def get_user_by_username(self, username: str) -> Optional[User]:
        """Get a user by username."""
        try:
            return await self.redis_user_repo.get_by_username(username)
        except Exception as e:
            print(f"❌ [USER_SERVICE] Error getting user by username {username}: {e}")
            return None
    
    @run_async
    def get_user_by_username_sync(self, username: str) -> Optional[User]:
        """Sync wrapper for get_user_by_username."""
        return self.get_user_by_username(username)
    
    async def get_user_by_email(self, email: str) -> Optional[User]:
        """Get a user by email."""
        try:
            return await self.redis_user_repo.get_by_email(email)
        except Exception as e:
            print(f"❌ [USER_SERVICE] Error getting user by email {email}: {e}")
            return None
    
    @run_async
    def get_user_by_email_sync(self, email: str) -> Optional[User]:
        """Sync wrapper for get_user_by_email."""
        return self.get_user_by_email(email)
    
    async def get_user_by_username_or_email(self, username_or_email: str) -> Optional[User]:
        """Get a user by username or email."""
        try:
            # First try to get by username
            user = await self.redis_user_repo.get_by_username(username_or_email)
            if user:
                return user
            
            # If not found by username, try by email
            user = await self.redis_user_repo.get_by_email(username_or_email)
            return user
        except Exception as e:
            print(f"❌ [USER_SERVICE] Error getting user by username or email {username_or_email}: {e}")
            return None
    
    @run_async
    def get_user_by_username_or_email_sync(self, username_or_email: str) -> Optional[User]:
        """Sync wrapper for get_user_by_username_or_email."""
        return self.get_user_by_username_or_email(username_or_email)
    
    async def get_all_users(self) -> List[User]:
        """Get all users."""
        try:
            return await self.redis_user_repo.get_all()
        except Exception as e:
            print(f"❌ [USER_SERVICE] Error getting all users: {e}")
            return []
    
    @run_async
    def get_all_users_sync(self) -> List[User]:
        """Sync wrapper for get_all_users."""
        return self.get_all_users()
    

class RedisReadingLogService:
    """Service for managing reading logs using Redis as the sole data store."""
    
    def __init__(self):
        self.redis_enabled = os.getenv('GRAPH_DATABASE_ENABLED', 'true').lower() == 'true'
        
        if not self.redis_enabled:
            raise RuntimeError("Redis must be enabled for this version of Bibliotheca")
            
        self.storage = get_graph_storage()
    
    async def get_existing_log(self, book_id: str, user_id: str, log_date: str):
        """Get existing reading log for a specific book, user, and date."""
        # Basic implementation - can be expanded later
        return None
    
    @run_async
    def get_existing_log_sync(self, book_id: str, user_id: str, log_date: str):
        """Sync wrapper for get_existing_log."""
        return self.get_existing_log(book_id, user_id, log_date)


class RedisCustomFieldService:
    """Service for managing custom fields using Redis as the sole data store."""
    
    def __init__(self):
        self.redis_enabled = os.getenv('GRAPH_DATABASE_ENABLED', 'true').lower() == 'true'
        
        if not self.redis_enabled:
            raise RuntimeError("Redis must be enabled for this version of Bibliotheca")
            
        self.storage = get_graph_storage()
        self.custom_field_repo = RedisCustomFieldRepository(self.storage)

    async def create_field(self, field_definition: CustomFieldDefinition) -> CustomFieldDefinition:
        """Create a new custom field definition."""
        return await self.custom_field_repo.create(field_definition)

    @run_async
    def create_field_sync(self, field_definition: CustomFieldDefinition) -> CustomFieldDefinition:
        """Sync wrapper for create_field."""
        return self.create_field(field_definition)

    async def get_available_fields(self, user_id: str, is_global: bool) -> List[CustomFieldDefinition]:
        """Get available custom fields for a user (global or personal)."""
        if is_global:
            # Get shareable fields (excluding ones created by this user)
            return await self.custom_field_repo.get_shareable(exclude_user_id=user_id)
        else:
            # Get user's own fields
            return await self.custom_field_repo.get_by_user(user_id)

    @run_async
    def get_available_fields_sync(self, user_id: str, is_global: bool) -> List[CustomFieldDefinition]:
        """Sync wrapper for get_available_fields."""
        return self.get_available_fields(user_id, is_global)

    async def get_user_fields_with_calculated_usage(self, user_id: str) -> List[CustomFieldDefinition]:
        """Get user's custom fields with calculated usage statistics."""
        try:
            user_fields = await self.custom_field_repo.get_by_user(user_id)
            
            # For now, return fields with zero usage count since we don't have usage tracking implemented
            # This can be enhanced later to actually calculate usage from books
            for field in user_fields:
                field.usage_count = 0
            
            return user_fields
        except Exception as e:
            print(f"❌ [SERVICE] Error getting user fields with usage: {e}")
            return []

    @run_async
    def get_user_fields_with_calculated_usage_sync(self, user_id: str) -> List[CustomFieldDefinition]:
        """Sync wrapper for get_user_fields_with_calculated_usage."""
        return self.get_user_fields_with_calculated_usage(user_id)

    async def get_shareable_fields_with_calculated_usage(self, exclude_user_id: Optional[str] = None) -> List[CustomFieldDefinition]:
        """Get shareable custom fields with calculated usage statistics."""
        try:
            shareable_fields = await self.custom_field_repo.get_shareable(exclude_user_id=exclude_user_id)
            
            # For now, return fields with zero usage count since we don't have usage tracking implemented
            # This can be enhanced later to actually calculate usage from books
            for field in shareable_fields:
                field.usage_count = 0
            
            return shareable_fields
        except Exception as e:
            print(f"❌ [SERVICE] Error getting shareable fields with usage: {e}")
            return []

    @run_async
    def get_shareable_fields_with_calculated_usage_sync(self, exclude_user_id: Optional[str] = None) -> List[CustomFieldDefinition]:
        """Sync wrapper for get_shareable_fields_with_calculated_usage."""
        return self.get_shareable_fields_with_calculated_usage(exclude_user_id)

    def get_custom_metadata_for_display(self, metadata: Dict[str, Any]) -> List[Dict[str, str]]:
        """Format custom metadata for display in templates."""
        if not metadata:
            return []
        
        display_items = []
        for field_name, value in metadata.items():
            # Convert field name to display format
            display_name = field_name.replace('_', ' ').title()
            
            # Format the value for display
            if isinstance(value, bool):
                display_value = "Yes" if value else "No"
            elif value is None:
                display_value = "-"
            else:
                display_value = str(value)
            
            display_items.append({
                'name': field_name,
                'display_name': display_name,
                'value': display_value
            })
        
        return display_items


class RedisImportMappingService:
    """Service for managing import mapping templates using Redis as the sole data store."""
    
    def __init__(self):
        self.redis_enabled = os.getenv('GRAPH_DATABASE_ENABLED', 'true').lower() == 'true'
        
        if not self.redis_enabled:
            raise RuntimeError("Redis must be enabled for this version of Bibliotheca")
            
        self.storage = get_graph_storage()
        self.import_mapping_repo = RedisImportMappingRepository(self.storage)
    
    async def get_templates_for_user(self, user_id: str):
        """Get import mapping templates for a user."""
        # Basic implementation - can be expanded later
        return []
    
    @run_async
    def get_templates_for_user_sync(self, user_id: str):
        """Sync wrapper for get_templates_for_user."""
        return self.get_templates_for_user(user_id)
    
    async def get_user_templates(self, user_id: str):
        """Get import mapping templates for a user (alias for get_templates_for_user)."""
        return await self.get_templates_for_user(user_id)

    @run_async
    def get_user_templates_sync(self, user_id: str):
        """Sync wrapper for get_user_templates."""
        return self.get_user_templates(user_id)
    
    async def get_template_by_id(self, template_id: str) -> Optional[ImportMappingTemplate]:
        """Get an import mapping template by its ID."""
        return await self.import_mapping_repo.get_by_id(template_id)

    @run_async
    def get_template_by_id_sync(self, template_id: str) -> Optional[ImportMappingTemplate]:
        """Sync wrapper for get_template_by_id."""
        return self.get_template_by_id(template_id)

    async def create_template(self, template: ImportMappingTemplate):
        """Create a new import mapping template."""
        try:
            # Generate ID if not provided
            if not template.id:
                template.id = str(uuid.uuid4())
            
            # Store template data in Redis
            template_data = template.to_dict()
            template_key = f"import_template:{template.id}"
            
            # Store the template
            self.storage.store_node('import_template', template.id, template_data)
            print(f"✅ [SERVICE] Created import template: {template.name}")
            return template
        except Exception as e:
            print(f"❌ [SERVICE] Error creating import template: {e}")
            return None
    
    @run_async
    def create_template_sync(self, template: ImportMappingTemplate):
        """Sync wrapper for create_template."""
        return self.create_template(template)

    async def delete_template(self, template_id: str):
        """Delete an import mapping template."""
        try:
            self.storage.delete_node('import_template', template_id)
            print(f"✅ [SERVICE] Deleted import template: {template_id}")
            return True
        except Exception as e:
            print(f"❌ [SERVICE] Error deleting import template: {e}")
            return False

    @run_async
    def delete_template_sync(self, template_id: str):
        """Sync wrapper for delete_template."""
        return self.delete_template(template_id)
    

class RedisDirectImportService:
    """Service for managing direct imports using Redis as the sole data store."""
    
    def __init__(self):
        self.redis_enabled = os.getenv('GRAPH_DATABASE_ENABLED', 'true').lower() == 'true'
        
        if not self.redis_enabled:
            raise RuntimeError("Redis must be enabled for this version of Bibliotheca")
            
        self.storage = get_graph_storage()
        self.book_service = RedisBookService()
        self.custom_field_service = RedisCustomFieldService()
    
    async def process_import(self, user_id: str, import_data: dict):
        """Process a direct import."""
        # Basic implementation - can be expanded later
        return {"status": "not_implemented"}
    
    @run_async
    def process_import_sync(self, user_id: str, import_data: dict):
        """Sync wrapper for process_import."""
        return self.process_import(user_id, import_data)

    def get_user_custom_fields(self, user_id: str) -> List[Tuple[str, bool]]:
        """
        Get a list of custom field names and their global status for a user.
        This is a synchronous wrapper for use in routes.
        """
        try:
            # Get both personal and global fields
            personal_fields = self.custom_field_service.get_available_fields_sync(user_id, is_global=False)
            global_fields = self.custom_field_service.get_available_fields_sync(user_id, is_global=True)
            
            # Combine and return with their global status
            result = []
            result.extend([(f.name, False) for f in personal_fields])
            result.extend([(f.name, True) for f in global_fields])
            return result
        except Exception as e:
            current_app.logger.error(f"Error in get_user_custom_fields: {e}")
            return []

    def _detect_and_parse_date(self, date_str: Optional[str], field_name: str = "date") -> Optional[datetime]:
        """Detect and parse a date string into a datetime object."""
        if not date_str:
            return None
        
        # Try parsing the date string using common formats
        for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%m-%d-%Y", "%Y%m%d"):
            try:
                return datetime.strptime(date_str, fmt)
            except ValueError:
                continue
        
        # If parsing failed, log and return None
        current_app.logger.warning(f"Invalid date format for {field_name}: {date_str}")
        return None


# Global service instances
book_service = RedisBookService()
user_service = RedisUserService()
reading_log_service = RedisReadingLogService()
custom_field_service = RedisCustomFieldService()
import_mapping_service = RedisImportMappingService()
direct_import_service = RedisDirectImportService()
