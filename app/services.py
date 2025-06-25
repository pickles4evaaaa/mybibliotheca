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
from .infrastructure.redis_repositories import RedisBookRepository, RedisUserRepository, RedisAuthorRepository, RedisUserBookRepository, RedisCustomFieldRepository, RedisImportMappingRepository, RedisCategoryRepository
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
            # Unusual length, but might still be valid
            return normalized
    
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
        self.redis_category_repo = RedisCategoryRepository(self.storage)
    
    async def create_book(self, domain_book: Book) -> Book:
        """Create a book in Redis (global, not user-specific)."""
        try:            
            # Generate ID if not set
            if not domain_book.id:
                domain_book.id = str(uuid.uuid4())
            
            # Store book in Redis (globally)
            await self.redis_book_repo.create(domain_book)
            
            return domain_book
        except Exception as e:
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
            print(f"📚 [ADD_TO_LIBRARY] Adding book {book_id} to user {user_id} library")
            print(f"📚 [ADD_TO_LIBRARY] Locations provided: {locations}")
            print(f"📚 [ADD_TO_LIBRARY] Reading status: {reading_status}")
            print(f"📚 [ADD_TO_LIBRARY] Custom metadata keys: {list(custom_metadata.keys()) if custom_metadata else 'None'}")
            
            # Check if relationship already exists
            existing_rel = await self.redis_user_book_repo.get_relationship(str(user_id), book_id)
            if existing_rel:
                print(f"📚 [ADD_TO_LIBRARY] Relationship already exists for user {user_id} and book {book_id}")
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
            
            print(f"📚 [ADD_TO_LIBRARY] Created relationship with locations: {relationship.locations}")
            # Store relationship in Redis
            await self.redis_user_book_repo.create_relationship(relationship)
            
            print(f"📚 [ADD_TO_LIBRARY] Successfully stored relationship for user {user_id}, book {book_id}")
            return True
        except Exception as e:
            print(f"❌ [ADD_TO_LIBRARY] Error adding book {book_id} to user {user_id} library: {e}")
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
                            print(f"📍 [GET_BOOK] Book '{book.title}' has locations: {rel.locations}")
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
            
            # Get all UserBookRelationships for this user from Redis
            relationships = await self.redis_user_book_repo.get_user_library(str(user_id))
            
            # Convert to Book objects with user-specific attributes stored as dynamic attributes
            books = []
            for rel in relationships:
                # Get the book data
                book = await self.redis_book_repo.get_by_id(rel.book_id)
                if book:
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
                    print(f"📍 [GET_USER_BOOKS] Book '{book.title}' has locations: {rel.locations}")
                    books.append(book)
                else:
                    # Book not found, skip this relationship
                    continue
            
            return books
        except Exception as e:
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
            
            # Handle contributors separately
            contributors = kwargs.pop('contributors', None)
            
            # Handle categories separately
            raw_categories = kwargs.pop('raw_categories', None)
            
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
            
            # Handle contributors if provided
            if contributors is not None:
                await self._update_book_contributors(book, contributors)
            
            # Handle categories if provided
            if raw_categories is not None:
                await self.process_book_categories(book.id, raw_categories)
            
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
    
    async def _update_book_contributors(self, book, contributors: List):
        """Update book contributors by removing all existing relationships and creating new ones."""
        try:
            from .domain.models import ContributionType
            
            book_id = book.id
            print(f"🔄 [SERVICE] Updating contributors for book {book_id}")
            print(f"📝 [SERVICE] New contributors: {[c.person.name + ' (' + c.contribution_type.value + ')' for c in contributors]}")
            
            # First, remove all existing relationships for this book
            # Get all existing contribution relationships
            existing_relationships = self.storage.get_relationships('book', book_id, 'WRITTEN_BY')
            existing_relationships.extend(self.storage.get_relationships('book', book_id, 'NARRATED_BY'))
            existing_relationships.extend(self.storage.get_relationships('book', book_id, 'EDITED_BY'))
            existing_relationships.extend(self.storage.get_relationships('book', book_id, 'CONTRIBUTED_BY'))
            
            print(f"🗑️ [SERVICE] Found {len(existing_relationships)} existing contributor relationships to remove")
            
            # Remove all existing contribution relationships
            for rel in existing_relationships:
                try:
                    # The relationship data structure from get_relationships includes:
                    # - to_type: target node type
                    # - to_id: target node id  
                    # - relationship: relationship type
                    self.storage.delete_relationship('book', book_id, rel['relationship'], rel['to_type'], rel['to_id'])
                    print(f"✅ [SERVICE] Removed relationship: book -> {rel['relationship']} -> {rel['to_id']}")
                except Exception as e:
                    print(f"⚠️ [SERVICE] Error removing relationship {rel}: {e}")
            
            # Now add the new contributors
            for contribution in contributors:
                person = contribution.person
                contrib_type = contribution.contribution_type
                
                # Ensure person exists in storage
                if person:
                    try:
                        # Check if person exists, create if not
                        existing_person = self.storage.get_node('person', person.id)
                        if not existing_person:
                            print(f"📝 [SERVICE] Creating new person: {person.name}")
                            person_data = {
                                'id': person.id,
                                'name': person.name,
                                'normalized_name': person.name.lower(),
                                'created_at': person.created_at.isoformat() if person.created_at else datetime.utcnow().isoformat(),
                                'updated_at': person.updated_at.isoformat() if person.updated_at else datetime.utcnow().isoformat()
                            }
                            if hasattr(person, 'bio') and person.bio:
                                person_data['bio'] = person.bio
                            if hasattr(person, 'birth_year') and person.birth_year:
                                person_data['birth_year'] = person.birth_year
                            if hasattr(person, 'death_year') and person.death_year:
                                person_data['death_year'] = person.death_year
                            
                            self.storage.store_node('person', person.id, person_data)
                        
                        # Create the appropriate relationship
                        relationship_type = {
                            ContributionType.AUTHORED: 'WRITTEN_BY',
                            ContributionType.NARRATED: 'NARRATED_BY', 
                            ContributionType.EDITED: 'EDITED_BY',
                            ContributionType.CONTRIBUTED: 'CONTRIBUTED_BY'
                        }.get(contrib_type, 'CONTRIBUTED_BY')
                        
                        # Create relationship from book to person
                        success = self.storage.create_relationship(
                            'book', book_id, relationship_type, 'person', person.id
                        )
                        
                        if success:
                            print(f"✅ [SERVICE] Created relationship: book {book_id} -> {relationship_type} -> person {person.id} ({person.name})")
                        else:
                            print(f"❌ [SERVICE] Failed to create relationship: book {book_id} -> {relationship_type} -> person {person.id}")
                            
                    except Exception as e:
                        print(f"❌ [SERVICE] Error updating contributor {person.name}: {e}")
                        import traceback
                        traceback.print_exc()
            
            print(f"✅ [SERVICE] Finished updating contributors for book {book_id}")
            
        except Exception as e:
            print(f"❌ [SERVICE] Error updating book contributors: {e}")
            import traceback
            traceback.print_exc()
    
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
                return False
            
            # Check if book has an ID
            if not book_to_remove.id:
                return False
            
            # Delete the user-book relationship
            success = await self.redis_user_book_repo.delete_relationship(str(user_id), book_to_remove.id)
            if success:
                return True
            else:
                return False
                
        except Exception as e:
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
        
        if not existing_book and domain_book.isbn10:
            existing_books = await self.get_books_by_isbn(domain_book.isbn10)
            if existing_books:
                existing_book = existing_books[0]
        
        if existing_book:
            # Check if the new book data contains additional information (like cover_url)
            # that should be merged into the existing book
            needs_update = False
            
            # Update cover URL if missing
            if domain_book.cover_url and not existing_book.cover_url:
                existing_book.cover_url = domain_book.cover_url
                needs_update = True
            
            # Update other missing fields
            if domain_book.description and not existing_book.description:
                existing_book.description = domain_book.description
                needs_update = True
            
            if domain_book.published_date and not existing_book.published_date:
                existing_book.published_date = domain_book.published_date
                needs_update = True
            
            if domain_book.page_count and not existing_book.page_count:
                existing_book.page_count = domain_book.page_count
                needs_update = True
            
            # Update the book in storage if changes were made
            if needs_update:
                existing_book.updated_at = datetime.now()
                await self.redis_book_repo.update(existing_book)
            
            # Process categories for existing book (add any new ones)
            if hasattr(domain_book, 'raw_categories') and domain_book.raw_categories:
                await self.process_book_categories(existing_book.id, domain_book.raw_categories)
            
            return existing_book
        
        # If no existing book found, create new one
        created_book = await self.create_book(domain_book)
        
        # Process categories for the newly created book
        if created_book and hasattr(domain_book, 'raw_categories') and domain_book.raw_categories:
            await self.process_book_categories(created_book.id, domain_book.raw_categories)
        
        return created_book

    async def get_user_book(self, user_id: str, book_identifier: str) -> Optional[Book]:
        """Get a specific book for a user with user-specific metadata.
        
        Args:
            user_id: The user ID
            book_identifier: Either book_id or book UID
        """
        try:
            
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
                return None
            
            if not book:
                return None
            
            
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
            
            return book
            
        except Exception as e:
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
        print(f"📚 [ADD_TO_LIBRARY_SYNC] Called with user_id={user_id}, book_id={book_id}, locations={locations}")
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
                # Filter person_data to only include fields that Person model expects
                valid_person_fields = {
                    'id', 'name', 'normalized_name', 'birth_year', 'death_year', 
                    'birth_place', 'bio', 'website', 'created_at', 'updated_at'
                }
                clean_person_data = {k: v for k, v in person_data.items() if k in valid_person_fields}
                
                # Map _id to id if needed (Redis stores as _id, Person model expects id)
                if 'id' not in clean_person_data and '_id' in person_data:
                    clean_person_data['id'] = person_data['_id']
                
                # Convert datetime strings back to datetime objects
                if 'created_at' in clean_person_data and isinstance(clean_person_data['created_at'], str):
                    try:
                        clean_person_data['created_at'] = datetime.fromisoformat(clean_person_data['created_at'])
                    except:
                        clean_person_data['created_at'] = datetime.utcnow()
                
                if 'updated_at' in clean_person_data and isinstance(clean_person_data['updated_at'], str):
                    try:
                        clean_person_data['updated_at'] = datetime.fromisoformat(clean_person_data['updated_at'])
                    except:
                        clean_person_data['updated_at'] = datetime.utcnow()
                
                person = Person(**clean_person_data)
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
                        # Filter person_data to only include fields that Person model expects
                        valid_person_fields = {
                            'id', 'name', 'normalized_name', 'birth_year', 'death_year', 
                            'birth_place', 'bio', 'website', 'created_at', 'updated_at'
                        }
                        clean_person_data = {k: v for k, v in person_data.items() if k in valid_person_fields}
                        
                        # Map _id to id if needed (Redis stores as _id, Person model expects id)
                        if 'id' not in clean_person_data and '_id' in person_data:
                            clean_person_data['id'] = person_data['_id']
                        
                        # Convert datetime strings
                        if 'created_at' in clean_person_data and isinstance(clean_person_data['created_at'], str):
                            try:
                                clean_person_data['created_at'] = datetime.fromisoformat(clean_person_data['created_at'])
                            except:
                                clean_person_data['created_at'] = datetime.utcnow()
                        
                        if 'updated_at' in clean_person_data and isinstance(clean_person_data['updated_at'], str):
                            try:
                                clean_person_data['updated_at'] = datetime.fromisoformat(clean_person_data['updated_at'])
                            except:
                                clean_person_data['updated_at'] = datetime.utcnow()
                        
                        person = Person(**clean_person_data)
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

    async def create_person(self, person):
        """Create a new person in Redis storage."""
        try:
            from dataclasses import asdict
            from .domain.models import Person
            
            # Convert Person object to dict for storage
            if isinstance(person, Person):
                person_data = asdict(person)
            else:
                person_data = person
            
            # Ensure we have a valid ID
            if not person.id:
                import uuid
                person.id = str(uuid.uuid4())
                person_data['id'] = person.id
            
            # Store the person node
            success = self.storage.store_node('person', str(person.id), person_data)
            if success:
                print(f"✅ [SERVICE] Created person: {person.name} (ID: {person.id})")
                return person
            else:
                print(f"❌ [SERVICE] Failed to create person: {person.name}")
                return None
        except Exception as e:
            print(f"❌ [SERVICE] Error creating person: {e}")
            import traceback
            traceback.print_exc()
            return None
    
    @run_async
    def create_person_sync(self, person):
        """Sync wrapper for create_person."""
        return self.create_person(person)

    async def find_or_create_person(self, person_name):
        """Find existing person by name or create a new one."""
        return await self.redis_book_repo.find_or_create_person(person_name)
    
    @run_async
    def find_or_create_person_sync(self, person_name):
        """Sync wrapper for find_or_create_person."""
        return self.find_or_create_person(person_name)

    # Genre/Category-related methods
    async def get_category_by_id(self, category_id: str, user_id: Optional[str] = None):
        """Get a category by their ID with parent relationship populated."""
        try:
            category = await self.redis_category_repo.get_by_id(category_id)
            if category:
                # Add usage statistics
                stats = await self.redis_category_repo.get_category_usage_stats(category_id, user_id)
                category.book_count = stats.get('total_books', 0)
                
                # Populate parent relationship
                if category.parent_id:
                    try:
                        parent = await self.redis_category_repo.get_by_id(category.parent_id)
                        category.parent = parent
                        # Recursively populate parent's parent if needed for full ancestry
                        if parent and parent.parent_id:
                            parent_of_parent = await self.get_category_by_id(parent.parent_id, user_id)
                            if parent_of_parent:
                                parent.parent = parent_of_parent
                    except Exception as e:
                        print(f"⚠️ [SERVICE] Error loading parent for category {category_id}: {e}")
                        
            return category
        except Exception as e:
            print(f"❌ [SERVICE] Error getting category by ID {category_id}: {e}")
            return None
    
    @run_async
    def get_category_by_id_sync(self, category_id: str, user_id: Optional[str] = None):
        """Sync wrapper for get_category_by_id."""
        return self.get_category_by_id(category_id, user_id)
    
    async def search_categories(self, query: str, limit: int = 10, user_id: Optional[str] = None):
        """Search for categories by name."""
        try:
            categories = await self.redis_category_repo.search_by_name(query)
            
            # Add usage statistics to each category
            for category in categories[:limit]:
                try:
                    stats = await self.redis_category_repo.get_category_usage_stats(category.id, user_id)
                    category.book_count = stats.get('total_books', 0)
                except Exception as e:
                    print(f"⚠️ [SERVICE] Error getting stats for category {category.id}: {e}")
                    category.book_count = 0
            
            return categories[:limit]
        except Exception as e:
            print(f"❌ [SERVICE] Error searching categories: {e}")
            return []
    
    @run_async
    def search_categories_sync(self, query: str, limit: int = 10, user_id: Optional[str] = None):
        """Sync wrapper for search_categories."""
        return self.search_categories(query, limit, user_id)
    
    async def list_all_categories(self, user_id: Optional[str] = None):
        """List all categories in the storage and return as Category objects with hierarchy."""
        try:
            categories = await self.redis_category_repo.get_all()
            
            # Build hierarchy relationships
            categories = await self.redis_category_repo.build_hierarchy_for_categories(categories)
            
            # Add usage statistics to each category
            for category in categories:
                try:
                    stats = await self.redis_category_repo.get_category_usage_stats(category.id, user_id)
                    category.book_count = stats.get('total_books', 0)
                except Exception as e:
                    print(f"⚠️ [SERVICE] Error getting stats for category {category.id}: {e}")
                    category.book_count = 0
            
            print(f"📊 [SERVICE] Returning {len(categories)} Category objects")
            return categories
        except Exception as e:
            print(f"❌ [SERVICE] Error listing categories: {e}")
            import traceback
            traceback.print_exc()
            return []
    
    @run_async
    def list_all_categories_sync(self, user_id: Optional[str] = None):
        """Sync wrapper for list_all_categories."""
        return self.list_all_categories(user_id)
    
    async def get_root_categories(self, user_id: Optional[str] = None):
        """Get top-level categories (no parent)."""
        try:
            root_categories = await self.redis_category_repo.get_root_categories()
            
            # Add usage statistics and build partial hierarchy
            for category in root_categories:
                try:
                    stats = await self.redis_category_repo.get_category_usage_stats(category.id, user_id)
                    category.book_count = stats.get('total_books', 0)
                    
                    # Get direct children
                    children = await self.redis_category_repo.get_children(category.id)
                    category.children = children
                except Exception as e:
                    print(f"⚠️ [SERVICE] Error processing root category {category.id}: {e}")
                    category.book_count = 0
                    category.children = []
            
            return root_categories
        except Exception as e:
            print(f"❌ [SERVICE] Error getting root categories: {e}")
            return []
    
    @run_async
    def get_root_categories_sync(self, user_id: Optional[str] = None):
        """Sync wrapper for get_root_categories."""
        return self.get_root_categories(user_id)
    
    async def get_category_children(self, category_id: str, user_id: Optional[str] = None):
        """Get direct children of a category."""
        try:
            children = await self.redis_category_repo.get_children(category_id)
            
            # Add usage statistics
            for child in children:
                try:
                    stats = await self.redis_category_repo.get_category_usage_stats(child.id, user_id)
                    child.book_count = stats.get('total_books', 0)
                except Exception as e:
                    print(f"⚠️ [SERVICE] Error getting stats for child category {child.id}: {e}")
                    child.book_count = 0
            
            return children
        except Exception as e:
            print(f"❌ [SERVICE] Error getting children for category {category_id}: {e}")
            return []
    
    @run_async
    def get_category_children_sync(self, category_id: str, user_id: Optional[str] = None):
        """Sync wrapper for get_category_children."""
        return self.get_category_children(category_id, user_id)
    
    async def create_category(self, category):
        """Create a new category in Redis storage."""
        try:
            from dataclasses import asdict
            from .domain.models import Category
            
            # Convert Category object to proper format if needed
            if isinstance(category, Category):
                pass  # Already a Category object
            else:
                # Convert from dict or other format
                category_data = category if isinstance(category, dict) else asdict(category)
                category = Category(**category_data)
            
            # Ensure we have a valid ID
            if not category.id:
                category.id = str(uuid.uuid4())
            
            # Create the category
            created_category = await self.redis_category_repo.create(category)
            if created_category:
                print(f"✅ [SERVICE] Created category: {category.name} (ID: {category.id})")
                return created_category
            else:
                print(f"❌ [SERVICE] Failed to create category: {category.name}")
                return None
        except Exception as e:
            print(f"❌ [SERVICE] Error creating category: {e}")
            import traceback
            traceback.print_exc()
            return None
    
    @run_async
    def create_category_sync(self, category):
        """Sync wrapper for create_category."""
        return self.create_category(category)
    
    async def update_category(self, category):
        """Update an existing category."""
        try:
            updated_category = await self.redis_category_repo.update(category)
            print(f"✅ [SERVICE] Updated category: {category.name} (ID: {category.id})")
            return updated_category
        except Exception as e:
            print(f"❌ [SERVICE] Error updating category: {e}")
            import traceback
            traceback.print_exc()
            return None
    
    @run_async
    def update_category_sync(self, category):
        """Sync wrapper for update_category."""
        return self.update_category(category)
    
    async def delete_category(self, category_id: str):
        """Delete a category."""
        try:
            success = await self.redis_category_repo.delete(category_id)
            if success:
                print(f"✅ [SERVICE] Deleted category: {category_id}")
            return success
        except Exception as e:
            print(f"❌ [SERVICE] Error deleting category {category_id}: {e}")
            return False
    
    @run_async
    def delete_category_sync(self, category_id: str):
        """Sync wrapper for delete_category."""
        return self.delete_category(category_id)
    
    async def find_or_create_category(self, category_name: str, parent_id: Optional[str] = None):
        """Find existing category by name or create a new one."""
        try:
            category = await self.redis_category_repo.find_or_create(category_name, parent_id)
            return category
        except Exception as e:
            print(f"❌ [SERVICE] Error in find_or_create_category for {category_name}: {e}")
            return None
    
    @run_async
    def find_or_create_category_sync(self, category_name: str, parent_id: Optional[str] = None):
        """Sync wrapper for find_or_create_category."""
        return self.find_or_create_category(category_name, parent_id)
    
    async def get_books_by_category(self, category_id: str, user_id: Optional[str] = None, include_subcategories: bool = True):
        """Get all books associated with a category."""
        try:
            books_result = []
            category_ids_to_check = [category_id]
            
            # If including subcategories, get all descendants
            if include_subcategories:
                category = await self.get_category_by_id(category_id)
                if category:
                    # Get all descendant categories recursively
                    descendants = await self._get_category_descendants_recursive(category_id)
                    category_ids_to_check.extend([desc.id for desc in descendants])
            
            print(f"🔍 [SERVICE] Checking {len(category_ids_to_check)} categories for books")
            
            # Get books for each category ID
            for cat_id in category_ids_to_check:
                print(f"🔍 [SERVICE] Looking for books in category {cat_id}")
                
                # Find books that have HAS_CATEGORY relationships to this category
                # We need to scan through all book HAS_CATEGORY relationship keys
                pattern = f"rel:book:*:HAS_CATEGORY"
                keys = self.storage.redis.keys(pattern)
                print(f"🔍 [SERVICE] Found {len(keys)} HAS_CATEGORY relationship keys")
                
                for key in keys:
                    rel_strings = self.storage.redis.smembers(key)
                    
                    for rel_string in rel_strings:
                        try:
                            rel_data = json.loads(rel_string)
                            
                            # Check if this relationship points to our target category
                            if rel_data.get('to_id') == cat_id:
                                # Extract book_id from the key: rel:book:{book_id}:HAS_CATEGORY
                                book_id = key.split(':')[2]
                                print(f"✅ [SERVICE] Found book {book_id} in category {cat_id}")
                                
                                # Get the full book object
                                book = await self.redis_book_repo.get_by_id(book_id)
                                if book:
                                    # If user_id is provided, check if user has this book and add user-specific data
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
                                            books_result.append(book)
                                            print(f"✅ [SERVICE] Added book {book.title} to category books (user has it)")
                                        else:
                                            print(f"⚠️ [SERVICE] User {user_id} doesn't have book {book.title}")
                                    else:
                                        # No user filter, include all books
                                        books_result.append(book)
                                        print(f"✅ [SERVICE] Added book {book.title} to category books (no user filter)")
                                else:
                                    print(f"❌ [SERVICE] Could not load book {book_id}")
                        except json.JSONDecodeError:
                            print(f"⚠️ [SERVICE] Invalid JSON in relationship: {rel_string}")
                        except Exception as e:
                            print(f"❌ [SERVICE] Error processing relationship: {e}")
            
            # Remove duplicates (book might be in multiple subcategories)
            seen_book_ids = set()
            unique_books = []
            for book in books_result:
                if book.id not in seen_book_ids:
                    unique_books.append(book)
                    seen_book_ids.add(book.id)
            
            print(f"📊 [SERVICE] Returning {len(unique_books)} books for category {category_id}")
            return unique_books
            
        except Exception as e:
            print(f"❌ [SERVICE] Error getting books by category {category_id}: {e}")
            import traceback
            traceback.print_exc()
            return []
    
    @run_async
    def get_books_by_category_sync(self, category_id: str, user_id: Optional[str] = None, include_subcategories: bool = True):
        """Sync wrapper for get_books_by_category."""
        return self.get_books_by_category(category_id, user_id, include_subcategories)
    
    async def _get_category_descendants_recursive(self, category_id: str):
        """Helper method to get all descendants of a category recursively."""
        descendants = []
        children = await self.redis_category_repo.get_children(category_id)
        
        for child in children:
            descendants.append(child)
            # Recursively get descendants of this child
            child_descendants = await self._get_category_descendants_recursive(child.id)
            descendants.extend(child_descendants)
        
        return descendants
    
    async def merge_categories(self, primary_category_id: str, merge_category_ids: List[str]):
        """Merge multiple categories into one primary category."""
        try:
            merged_count = 0
            
            primary_category = await self.get_category_by_id(primary_category_id)
            if not primary_category:
                raise Exception(f"Primary category {primary_category_id} not found")
            
            for merge_id in merge_category_ids:
                if merge_id == primary_category_id:
                    continue  # Skip merging into itself
                
                try:
                    merge_category = await self.get_category_by_id(merge_id)
                    if not merge_category:
                        continue
                    
                    # Move all book relationships from merge category to primary category
                    book_relationships = self.storage.get_relationships('book', None, 'HAS_CATEGORY')
                    for rel in book_relationships:
                        if rel.get('to_id') == merge_id:
                            book_id = rel.get('from_id')
                            if book_id:
                                # Remove old relationship
                                self.storage.delete_relationship('book', book_id, 'HAS_CATEGORY', 'category', merge_id)
                                # Create new relationship to primary category
                                self.storage.create_relationship('book', book_id, 'HAS_CATEGORY', 'category', primary_category_id)
                                print(f"✅ [SERVICE] Moved book {book_id} from category {merge_id} to {primary_category_id}")
                    
                    # Move all child categories to primary category (if hierarchical merge is desired)
                    children = await self.redis_category_repo.get_children(merge_id)
                    for child in children:
                        child.parent_id = primary_category_id
                        child.level = primary_category.level + 1
                        await self.redis_category_repo.update(child)
                        print(f"✅ [SERVICE] Moved child category {child.name} to primary category")
                    
                    # Delete the merged category
                    await self.redis_category_repo.delete(merge_id)
                    merged_count += 1
                    
                except Exception as e:
                    print(f"❌ [SERVICE] Error merging category {merge_id}: {e}")
                    continue
            
            if merged_count > 0:
                print(f"✅ [SERVICE] Successfully merged {merged_count} categories into {primary_category.name}")
            
            return merged_count > 0
            
        except Exception as e:
            print(f"❌ [SERVICE] Error merging categories: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    @run_async
    def merge_categories_sync(self, primary_category_id: str, merge_category_ids: List[str]):
        """Sync wrapper for merge_categories."""
        return self.merge_categories(primary_category_id, merge_category_ids)
    
    async def get_book_categories(self, book_id: str) -> List[Category]:
        """Get all categories for a book."""
        try:
            categories = []
            seen_category_ids = set()
            
            # Get all HAS_CATEGORY relationships for this book
            relationships = self.storage.get_relationships('book', book_id, 'HAS_CATEGORY')
            print(f"📚 [SERVICE] Found {len(relationships)} category relationships for book {book_id}")
            
            for rel in relationships:
                category_id = rel.get('to_id')
                if category_id and category_id not in seen_category_ids:
                    category = await self.get_category_by_id(category_id)
                    if category:
                        categories.append(category)
                        seen_category_ids.add(category_id)
                        print(f"✅ [SERVICE] Added category: {category.name}")
                elif category_id in seen_category_ids:
                    print(f"⚠️ [SERVICE] Skipping duplicate category relationship for {category_id}")
            
            print(f"📊 [SERVICE] Returning {len(categories)} Category objects")
            return categories
            
        except Exception as e:
            print(f"❌ [SERVICE] Error getting book categories: {e}")
            traceback.print_exc()
            return []
    
    @run_async
    def get_book_categories_sync(self, book_id: str) -> List[Category]:
        """Sync wrapper for get_book_categories."""
        return self.get_book_categories(book_id)

    async def process_book_categories(self, book_id: str, categories_data: Any) -> bool:
        """Process and assign categories to a book from API data or CSV."""
        try:
            if not categories_data:
                return True  # No categories to process
            
            # Handle different category data formats
            categories_list = []
            
            if isinstance(categories_data, str):
                # String format: "Fiction, Science Fiction, Adventure"
                categories_list = [cat.strip() for cat in categories_data.split(',') if cat.strip()]
            elif isinstance(categories_data, list):
                # List format: ["Fiction", "Science Fiction", "Adventure"]
                categories_list = [str(cat).strip() for cat in categories_data if str(cat).strip()]
            else:
                print(f"⚠️ [SERVICE] Unsupported categories format: {type(categories_data)}")
                return True  # Don't fail the import for unsupported formats
            
            print(f"📚 [SERVICE] Processing {len(categories_list)} categories for book {book_id}: {categories_list}")
            
            # Process each category
            for category_name in categories_list:
                if not category_name:
                    continue
                    
                try:
                    # Find or create the category
                    category = await self.find_or_create_category(category_name)
                    if category:
                        # Check if relationship already exists
                        existing_relationships = self.storage.get_relationships('book', book_id, 'HAS_CATEGORY')
                        relationship_exists = any(
                            rel.get('to_id') == category.id 
                            for rel in existing_relationships
                        )
                        
                        if not relationship_exists:
                            # Create relationship between book and category
                            relationship_created = self.storage.create_relationship(
                                'book', book_id, 'HAS_CATEGORY', 'category', category.id
                            )
                            if relationship_created:
                                print(f"✅ [SERVICE] Linked book {book_id} to category '{category_name}' (ID: {category.id})")
                                
                                # Update category book count
                                try:
                                    await self.redis_category_repo.increment_book_count(category.id)
                                    print(f"📊 [SERVICE] Incremented book count for category '{category_name}'")
                                except Exception as count_error:
                                    print(f"⚠️ [SERVICE] Failed to update book count for category '{category_name}': {count_error}")
                            else:
                                print(f"⚠️ [SERVICE] Failed to create relationship for category '{category_name}'")
                        else:
                            print(f"ℹ️ [SERVICE] Relationship already exists for book {book_id} and category '{category_name}' (ID: {category.id})")
                    else:
                        print(f"❌ [SERVICE] Failed to find or create category '{category_name}'")
                        
                except Exception as e:
                    print(f"❌ [SERVICE] Error processing category '{category_name}': {e}")
                    # Continue with other categories even if one fails
                    continue
            
            return True
            
        except Exception as e:
            print(f"❌ [SERVICE] Error processing book categories: {e}")
            traceback.print_exc()
            return False
    
    @run_async
    def process_book_categories_sync(self, book_id: str, categories_data: Any) -> bool:
        """Sync wrapper for process_book_categories."""
        return self.process_book_categories(book_id, categories_data)
    

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
                        password_must_change: bool = False, timezone: str = "UTC",
                        display_name: str = "", location: str = "") -> User:
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
                timezone=timezone,
                display_name=display_name,
                location=location,
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
                        password_must_change: bool = False, timezone: str = "UTC", 
                        display_name: str = "", location: str = "") -> User:
        """Sync wrapper for create_user."""
        return self.create_user(username, email, password_hash, is_admin, is_active, 
                               password_must_change, timezone, display_name, location)
    
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
            fields = await self.custom_field_repo.get_by_user(user_id)
            return fields
        except Exception as e:
            print(f"❌ [CUSTOM_FIELD_SERVICE] Error getting user fields: {e}")
            return []

    @run_async
    def get_user_fields_with_calculated_usage_sync(self, user_id: str) -> List[CustomFieldDefinition]:
        """Sync wrapper for get_user_fields_with_calculated_usage."""
        return self.get_user_fields_with_calculated_usage(user_id)

    async def get_shareable_fields_with_calculated_usage(self, exclude_user_id: Optional[str] = None) -> List[CustomFieldDefinition]:
        """Get shareable custom fields with calculated usage statistics."""
        try:
            fields = await self.custom_field_repo.get_shareable(exclude_user_id=exclude_user_id)
            return fields
        except Exception as e:
            print(f"❌ [CUSTOM_FIELD_SERVICE] Error getting shareable fields: {e}")
            return []

    @run_async
    def get_shareable_fields_with_calculated_usage_sync(self, exclude_user_id: Optional[str] = None) -> List[CustomFieldDefinition]:
        """Sync wrapper for get_shareable_fields_with_calculated_usage."""
        return self.get_shareable_fields_with_calculated_usage(exclude_user_id)

    def get_custom_metadata_for_display(self, metadata: Dict[str, Any]) -> List[Dict[str, str]]:
        """Convert custom metadata to display format."""
        return []


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
        try:
            return await self.import_mapping_repo.get_by_user(user_id)
        except Exception as e:
            print(f"❌ [IMPORT_MAPPING_SERVICE] Error getting templates for user: {e}")
            return []
    
    @run_async
    def get_templates_for_user_sync(self, user_id: str):
        """Sync wrapper for get_templates_for_user."""
        return self.get_templates_for_user(user_id)
    
    async def get_user_templates(self, user_id: str):
        """Get user templates (alias for get_templates_for_user)."""
        return await self.get_templates_for_user(user_id)

    @run_async
    def get_user_templates_sync(self, user_id: str):
        """Sync wrapper for get_user_templates."""
        return self.get_user_templates(user_id)
    
    async def create_template(self, template):
        """Create a new import mapping template."""
        try:
            return await self.import_mapping_repo.create(template)
        except Exception as e:
            print(f"❌ [IMPORT_MAPPING_SERVICE] Error creating template: {e}")
            return None
    
    @run_async
    def create_template_sync(self, template):
        """Sync wrapper for create_template."""
        return self.create_template(template)
    
    async def get_template_by_id(self, template_id: str):
        """Get import mapping template by ID."""
        try:
            return await self.import_mapping_repo.get_by_id(template_id)
        except Exception as e:
            print(f"❌ [IMPORT_MAPPING_SERVICE] Error getting template by ID: {e}")
            return None
    
    @run_async
    def get_template_by_id_sync(self, template_id: str):
        """Sync wrapper for get_template_by_id."""
        return self.get_template_by_id(template_id)
    
    async def update_template(self, template):
        """Update an existing import mapping template."""
        try:
            return await self.import_mapping_repo.update(template)
        except Exception as e:
            print(f"❌ [IMPORT_MAPPING_SERVICE] Error updating template: {e}")
            return None
    
    @run_async
    def update_template_sync(self, template):
        """Sync wrapper for update_template."""
        return self.update_template(template)
    
    async def delete_template(self, template_id: str):
        """Delete an import mapping template."""
        try:
            return await self.import_mapping_repo.delete(template_id)
        except Exception as e:
            print(f"❌ [IMPORT_MAPPING_SERVICE] Error deleting template: {e}")
            return False
    
    @run_async
    def delete_template_sync(self, template_id: str):
        """Sync wrapper for delete_template."""
        return self.delete_template(template_id)
    
    async def detect_template(self, headers: list, user_id: str):
        """Detect matching template based on CSV headers."""
        try:
            return await self.import_mapping_repo.detect_template(headers, user_id)
        except Exception as e:
            print(f"❌ [IMPORT_MAPPING_SERVICE] Error detecting template: {e}")
            return None
    
    @run_async
    def detect_template_sync(self, headers: list, user_id: str):
        """Sync wrapper for detect_template."""
        return self.detect_template(headers, user_id)


class RedisDirectImportService:
    """Service for direct import operations using Redis as the sole data store."""
    
    def __init__(self):
        self.redis_enabled = os.getenv('GRAPH_DATABASE_ENABLED', 'true').lower() == 'true'
        
        if not self.redis_enabled:
            raise RuntimeError("Redis must be enabled for this version of Bibliotheca")
            
        self.storage = get_graph_storage()


def _parse_date_with_fallbacks(date_str: str, field_name: str = "date") -> Optional[date]:
    """Parse date string with multiple format fallbacks."""
    if not date_str or not isinstance(date_str, str):
        return None
    
    date_str = date_str.strip()
    if not date_str:
        return None
    
    # List of date formats to try (most specific first)
    date_formats = [
        '%Y-%m-%d',      # 2023-12-25
        '%Y/%m/%d',      # 2023/12/25
        '%m/%d/%Y',      # 12/25/2023
        '%d/%m/%Y',      # 25/12/2023
        '%Y-%m',         # 2023-12
        '%Y/%m',         # 2023/12
        '%m/%Y',         # 12/2023
        '%Y',            # 2023
        '%B %d, %Y',     # December 25, 2023
        '%b %d, %Y',     # Dec 25, 2023
        '%d %B %Y',      # 25 December 2023
        '%d %b %Y',      # 25 Dec 2023
    ]
    
    for date_format in date_formats:
        try:
            parsed_date = datetime.strptime(date_str, date_format)
            
            # If only year was provided, default to January 1st
            if date_format == '%Y':
                return date(parsed_date.year, 1, 1)
            # If only year and month, default to 1st of month
            elif date_format in ['%Y-%m', '%Y/%m', '%m/%Y']:
                return date(parsed_date.year, parsed_date.month, 1)
            else:
                return parsed_date.date()
                
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
