"""
Kuzu-only service layer for Bibliotheca using clean architecture.

This module provides service classes for comprehensive book management using the
new clean Kuzu graph database implementation.
"""

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

# Import domain models
from .domain.models import (
    Book, User, Author, Publisher, Series, Category, UserBookRelationship, 
    ReadingLog, ReadingStatus, OwnershipStatus, CustomFieldDefinition, 
    ImportMappingTemplate, CustomFieldType, Person, Location, MediaType
)

# Import the clean Kuzu integration service
from .kuzu_integration import get_kuzu_service

# Import legacy repositories for compatibility
from .infrastructure.kuzu_repositories import (
    KuzuCustomFieldRepository, KuzuImportMappingRepository
)

# Utility functions
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
    digits_only = extract_digits_only(isbn_or_upc)
    
    if not digits_only:
        return None
    
    # Basic validation for common formats
    if len(digits_only) == 10 or len(digits_only) == 13:
        return digits_only
    
    return digits_only


def run_async(coro):
    """Helper to run async functions in sync context."""
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            # If we're already in an async context, create a new event loop in a thread
            import concurrent.futures
            with concurrent.futures.ThreadPoolExecutor() as executor:
                future = executor.submit(asyncio.run, coro)
                return future.result()
        else:
            return loop.run_until_complete(coro)
    except RuntimeError:
        return asyncio.run(coro)


class KuzuUserService:
    """User service using clean Kuzu architecture."""
    
    def __init__(self):
        self.kuzu_service = get_kuzu_service()
    
    def get_user_by_id_sync(self, user_id: str) -> Optional[User]:
        """Get user by ID (sync version for Flask-Login)."""
        try:
            user_data = run_async(self.kuzu_service.get_user(user_id))
            if user_data:
                return User(
                    id=user_data['id'],
                    username=user_data['username'],
                    email=user_data['email'],
                    display_name=user_data.get('display_name'),
                    bio=user_data.get('bio'),
                    timezone=user_data.get('timezone', 'UTC'),
                    is_admin=user_data.get('is_admin', False),
                    is_active=user_data.get('is_active', True),
                    created_at=user_data.get('created_at')
                )
            return None
        except Exception as e:
            current_app.logger.error(f"Error getting user {user_id}: {e}")
            return None
    
    async def get_user_by_id(self, user_id: str) -> Optional[User]:
        """Get user by ID (async version)."""
        try:
            user_data = await self.kuzu_service.get_user(user_id)
            if user_data:
                return User(
                    id=user_data['id'],
                    username=user_data['username'],
                    email=user_data['email'],
                    password_hash=user_data.get('password_hash', ''),
                    display_name=user_data.get('display_name'),
                    bio=user_data.get('bio'),
                    timezone=user_data.get('timezone', 'UTC'),
                    is_admin=user_data.get('is_admin', False),
                    is_active=user_data.get('is_active', True),
                    created_at=user_data.get('created_at')
                )
            return None
        except Exception as e:
            current_app.logger.error(f"Error getting user {user_id}: {e}")
            return None
    
    async def get_user_by_username(self, username: str) -> Optional[User]:
        """Get user by username."""
        try:
            print(f"🔍 [USER_SERVICE] Looking for user by username: '{username}'")
            user_data = await self.kuzu_service.get_user_by_username(username)
            print(f"🔍 [USER_SERVICE] Kuzu service returned: {user_data}")
            if user_data:
                user = User(
                    id=user_data['id'],
                    username=user_data['username'],
                    email=user_data['email'],
                    password_hash=user_data.get('password_hash', ''),
                    display_name=user_data.get('display_name'),
                    bio=user_data.get('bio'),
                    timezone=user_data.get('timezone', 'UTC'),
                    is_admin=user_data.get('is_admin', False),
                    is_active=user_data.get('is_active', True),
                    created_at=user_data.get('created_at')
                )
                print(f"🔍 [USER_SERVICE] Created User object: {user}")
                return user
            print(f"🔍 [USER_SERVICE] No user data returned for username: '{username}'")
            return None
        except Exception as e:
            print(f"🔍 [USER_SERVICE] Error getting user by username '{username}': {e}")
            current_app.logger.error(f"Error getting user by username {username}: {e}")
            return None
    
    def get_user_by_username_sync(self, username: str) -> Optional[User]:
        """Get user by username (sync version for form validation)."""
        return run_async(self.get_user_by_username(username))
    
    async def get_user_by_email(self, email: str) -> Optional[User]:
        """Get user by email."""
        try:
            user_data = await self.kuzu_service.get_user_by_email(email)
            if user_data:
                return User(
                    id=user_data['id'],
                    username=user_data['username'],
                    email=user_data['email'],
                    password_hash=user_data.get('password_hash', ''),
                    display_name=user_data.get('display_name'),
                    bio=user_data.get('bio'),
                    timezone=user_data.get('timezone', 'UTC'),
                    is_admin=user_data.get('is_admin', False),
                    is_active=user_data.get('is_active', True),
                    created_at=user_data.get('created_at')
                )
            return None
        except Exception as e:
            current_app.logger.error(f"Error getting user by email {email}: {e}")
            return None
    
    def get_user_by_email_sync(self, email: str) -> Optional[User]:
        """Get user by email (sync version for form validation)."""
        return run_async(self.get_user_by_email(email))
    
    async def get_user_by_username_or_email(self, username_or_email: str) -> Optional[User]:
        """Get user by username or email."""
        try:
            user_data = await self.kuzu_service.get_user_by_username_or_email(username_or_email)
            if user_data:
                return User(
                    id=user_data['id'],
                    username=user_data['username'],
                    email=user_data['email'],
                    password_hash=user_data.get('password_hash', ''),
                    display_name=user_data.get('display_name'),
                    bio=user_data.get('bio'),
                    timezone=user_data.get('timezone', 'UTC'),
                    is_admin=user_data.get('is_admin', False),
                    is_active=user_data.get('is_active', True),
                    created_at=user_data.get('created_at')
                )
            return None
        except Exception as e:
            current_app.logger.error(f"Error getting user by username or email {username_or_email}: {e}")
            return None
    
    def get_user_by_username_or_email_sync(self, username_or_email: str) -> Optional[User]:
        """Get user by username or email (sync version for form validation)."""
        return run_async(self.get_user_by_username_or_email(username_or_email))
    
    async def create_user(self, username: str, email: str, password_hash: str, 
                         display_name: str = None, is_admin: bool = False) -> Optional[User]:
        """Create a new user."""
        try:
            user_data = {
                'username': username,
                'email': email,
                'password_hash': password_hash,
                'display_name': display_name,
                'is_admin': is_admin,
                'is_active': True
            }
            
            created_user_data = await self.kuzu_service.create_user(user_data)
            if created_user_data:
                return User(
                    id=created_user_data['id'],
                    username=created_user_data['username'],
                    email=created_user_data['email'],
                    display_name=created_user_data.get('display_name'),
                    bio=created_user_data.get('bio'),
                    timezone=created_user_data.get('timezone', 'UTC'),
                    is_admin=created_user_data.get('is_admin', False),
                    is_active=created_user_data.get('is_active', True),
                    created_at=created_user_data.get('created_at')
                )
            return None
        except Exception as e:
            current_app.logger.error(f"Error creating user {username}: {e}")
            return None

    def create_user_sync(self, username: str, email: str, password_hash: str, 
                        display_name: str = None, is_admin: bool = False, 
                        is_active: bool = True, password_must_change: bool = False,
                        timezone: str = 'UTC', location: str = '') -> Optional[User]:
        """Create a new user (sync version for form validation and onboarding)."""
        try:
            user_data = {
                'username': username,
                'email': email,
                'password_hash': password_hash,
                'display_name': display_name,
                'is_admin': is_admin,
                'is_active': is_active,
                'timezone': timezone,
                'bio': location  # Store location in bio field for now
            }
            
            # Use run_async to call the async method
            created_user_data = run_async(self.kuzu_service.create_user(user_data))
            if created_user_data:
                return User(
                    id=created_user_data['id'],
                    username=created_user_data['username'],
                    email=created_user_data['email'],
                    display_name=created_user_data.get('display_name'),
                    bio=created_user_data.get('bio'),
                    timezone=created_user_data.get('timezone', 'UTC'),
                    is_admin=created_user_data.get('is_admin', False),
                    is_active=created_user_data.get('is_active', True),
                    created_at=created_user_data.get('created_at')
                )
            return None
        except Exception as e:
            current_app.logger.error(f"Error creating user {username}: {e}")
            return None

    def get_user_count_sync(self) -> int:
        """Get total user count (sync version for compatibility)."""
        try:
            count = run_async(self.kuzu_service.get_user_count())
            return count
        except Exception as e:
            current_app.logger.error(f"Error getting user count: {e}")
            return 0

    async def get_user_count(self) -> int:
        """Get total user count (async version)."""
        try:
            return await self.kuzu_service.get_user_count()
        except Exception as e:
            current_app.logger.error(f"Error getting user count: {e}")
            return 0

    async def get_all_users(self, limit: int = 1000) -> List[User]:
        """Get all users (async version)."""
        try:
            users_data = await self.kuzu_service.get_all_users(limit)
            users = []
            for user_data in users_data:
                user = User(
                    id=user_data['id'],
                    username=user_data['username'],
                    email=user_data['email'],
                    password_hash=user_data.get('password_hash', ''),
                    display_name=user_data.get('display_name'),
                    bio=user_data.get('bio'),
                    timezone=user_data.get('timezone', 'UTC'),
                    is_admin=user_data.get('is_admin', False),
                    is_active=user_data.get('is_active', True),
                    created_at=user_data.get('created_at')
                )
                users.append(user)
            return users
        except Exception as e:
            current_app.logger.error(f"Error getting all users: {e}")
            return []

    def get_all_users_sync(self, limit: int = 1000) -> List[User]:
        """Get all users (sync version for form validation)."""
        return run_async(self.get_all_users(limit))

    async def update_user(self, user: User) -> Optional[User]:
        """Update an existing user (async version)."""
        try:
            user_data = {
                'id': user.id,
                'username': user.username,
                'email': user.email,
                'password_hash': user.password_hash,
                'display_name': user.display_name,
                'bio': user.bio,
                'timezone': user.timezone,
                'is_admin': user.is_admin,
                'is_active': user.is_active
            }
            
            updated_user_data = await self.kuzu_service.update_user(user.id, user_data)
            if updated_user_data:
                return User(
                    id=updated_user_data['id'],
                    username=updated_user_data['username'],
                    email=updated_user_data['email'],
                    password_hash=updated_user_data.get('password_hash', ''),
                    display_name=updated_user_data.get('display_name'),
                    bio=updated_user_data.get('bio'),
                    timezone=updated_user_data.get('timezone', 'UTC'),
                    is_admin=updated_user_data.get('is_admin', False),
                    is_active=updated_user_data.get('is_active', True),
                    created_at=updated_user_data.get('created_at')
                )
            return None
        except Exception as e:
            current_app.logger.error(f"Error updating user {user.id}: {e}")
            return None

    def update_user_sync(self, user: User) -> Optional[User]:
        """Update an existing user (sync version for admin tools)."""
        return run_async(self.update_user(user))


class KuzuBookService:
    """Book service using clean Kuzu architecture."""
    
    def __init__(self):
        self.kuzu_service = get_kuzu_service()
    
    async def get_book_by_id(self, book_id: str) -> Optional[Book]:
        """Get book by ID with full details."""
        try:
            book_data = await self.kuzu_service.get_book(book_id)
            if book_data:
                return self._convert_to_book_model(book_data)
            return None
        except Exception as e:
            current_app.logger.error(f"Error getting book {book_id}: {e}")
            return None
    
    async def search_books(self, query: str, limit: int = 20) -> List[Book]:
        """Search books by title or author."""
        try:
            book_data_list = await self.kuzu_service.search_books(query, limit)
            return [self._convert_to_book_model(book_data) for book_data in book_data_list]
        except Exception as e:
            current_app.logger.error(f"Error searching books with query '{query}': {e}")
            return []
    
    async def create_book(self, book_data: Dict[str, Any]) -> Optional[Book]:
        """Create a new book with authors and categories."""
        try:
            created_book_data = await self.kuzu_service.create_book(book_data)
            if created_book_data:
                return self._convert_to_book_model(created_book_data)
            return None
        except Exception as e:
            current_app.logger.error(f"Error creating book: {e}")
            return None
    
    async def add_book_to_user_library(self, user_id: str, book_id: str, 
                                      reading_status: str = 'plan_to_read',
                                      ownership_status: str = 'owned',
                                      media_type: str = 'physical',
                                      location_id: str = None,
                                      notes: str = None,
                                      custom_metadata: Dict[str, Any] = None) -> bool:
        """Add a book to user's library."""
        try:
            ownership_data = {
                'reading_status': reading_status,
                'ownership_status': ownership_status,
                'media_type': media_type,
                'location_id': location_id,
                'notes': notes,
                'source': 'manual',
                'custom_metadata': custom_metadata
            }
            
            return await self.kuzu_service.add_book_to_library(user_id, book_id, ownership_data)
        except Exception as e:
            current_app.logger.error(f"Error adding book to library: {e}")
            return False
    
    async def delete_book(self, uid: str, user_id: str) -> bool:
        """Remove a book from user's library."""
        try:
            return await self.kuzu_service.remove_book_from_library(user_id, uid)
        except Exception as e:
            current_app.logger.error(f"Error removing book from library: {e}")
            return False

    async def delete_book_globally(self, uid: str) -> bool:
        """Delete a book globally from the system."""
        try:
            return await self.kuzu_service.book_repo.delete(uid)
        except Exception as e:
            current_app.logger.error(f"Error deleting book globally: {e}")
            return False
    
    async def get_user_library(self, user_id: str, reading_status: str = None, 
                              limit: int = 50) -> List[Dict[str, Any]]:
        """Get user's library with optional status filtering."""
        try:
            return await self.kuzu_service.get_user_library(user_id, reading_status=reading_status, limit=limit)
        except Exception as e:
            current_app.logger.error(f"Error getting user library: {e}")
            return []
    
    def get_all_books_sync(self, limit: int = 50) -> List[Dict[str, Any]]:
        """Get all books in the system without user overlay."""
        try:
            from .infrastructure.kuzu_graph import get_graph_storage
            storage = get_graph_storage()
            
            # Get ALL books in the system
            query = """
            MATCH (b:Book)
            OPTIONAL MATCH (b)<-[authored:AUTHORED]-(p:Person)
            OPTIONAL MATCH (b)-[categorized:CATEGORIZED_AS]->(c:Category)
            RETURN b, collect(DISTINCT p), collect(DISTINCT c)
            ORDER BY b.title
            LIMIT $limit
            """
            
            result = storage.query(query, {'limit': limit})
            books = []
            
            for record in result:
                if 'col_0' not in record:
                    continue
                    
                book_data = record['col_0']
                authors = record.get('col_1', []) or []
                categories = record.get('col_2', []) or []
                
                # Create book dict with basic structure
                # Handle both dict and node objects
                if hasattr(book_data, '_properties'):
                    book = dict(book_data._properties)
                else:
                    book = dict(book_data)
                book['authors'] = [dict(author._properties) if hasattr(author, '_properties') else dict(author) for author in authors]
                book['categories'] = [dict(category._properties) if hasattr(category, '_properties') else dict(category) for category in categories]
                books.append(book)
            
            return books
        except Exception as e:
            current_app.logger.error(f"Error getting all books: {e}")
            return []

    def get_user_books_sync(self, user_id: str, reading_status: str = None,
                           limit: int = 50) -> List[Dict[str, Any]]:
        """Get user's library with global book visibility (default behavior).
        
        All users can see all books, with personal data overlaid where it exists.
        This is the standard Goodreads-style behavior.
        """
        try:
            # Use global book visibility as default behavior
            return self.get_all_books_with_user_overlay_sync(user_id, reading_status=reading_status, limit=limit)
        except Exception as e:
            current_app.logger.error(f"Error getting user books: {e}")
            return []

    def get_all_books_with_user_overlay_sync(self, user_id: str, reading_status: str = None,
                                           limit: int = 50) -> List[Dict[str, Any]]:
        """Get ALL books in the system with optional user-specific overlay data.
        
        This implements global book visibility - all users can see all books,
        with personal data (notes, ratings, reading status) overlaid where it exists.
        """
        try:
            from .infrastructure.kuzu_graph import get_graph_storage
            storage = get_graph_storage()
            
            # Get ALL books in the system (global catalog)
            query = """
            MATCH (b:Book)
            OPTIONAL MATCH (b)<-[authored:AUTHORED]-(p:Person)
            OPTIONAL MATCH (b)-[cat_rel:CATEGORIZED_AS]->(c:Category)
            OPTIONAL MATCH (b)-[pub_rel:PUBLISHED_BY]->(pub:Publisher)
            RETURN b, 
                   collect(DISTINCT p) as authors,
                   collect(DISTINCT c) as categories,
                   pub
            LIMIT $limit
            """
            
            result = storage.query(query, {'limit': limit})
            books = []
            
            print(f"🌍 [GLOBAL_BOOKS] Found {len(result)} global books")
            
            for record in result:
                if 'col_0' not in record:
                    continue
                    
                book_data = dict(record['col_0'])
                authors_data = record.get('col_1', []) or []
                categories_data = record.get('col_2', []) or []
                publisher_data = record.get('col_3')
                
                # Convert to dictionary format
                book_dict = {
                    'id': book_data.get('id'),
                    'uid': book_data.get('id'),  # Add uid as alias for id for template compatibility
                    'title': book_data.get('title'),
                    'isbn': book_data.get('isbn'),
                    'description': book_data.get('description'),
                    'published_date': book_data.get('published_date'),
                    'page_count': book_data.get('page_count'),
                    'cover_url': book_data.get('cover_url'),
                    'authors': [{'id': a.get('id'), 'name': a.get('name')} for a in authors_data if a and isinstance(a, dict)],
                    'categories': [{'id': c.get('id'), 'name': c.get('name')} for c in categories_data if c and isinstance(c, dict)],
                    'publisher': {'id': publisher_data.get('id'), 'name': publisher_data.get('name')} if publisher_data and isinstance(publisher_data, dict) else None
                }
                
                # Now check if this user has personal data for this book
                user_overlay = self._get_user_book_overlay(storage, user_id, book_data.get('id'))
                
                if user_overlay:
                    # User has personal data - merge it
                    book_dict.update(user_overlay)
                    print(f"📖 [GLOBAL_BOOKS] Book '{book_dict['title']}' has user overlay data")
                else:
                    # No personal data - set defaults for global view
                    book_dict.update({
                        'reading_status': None,  # No personal reading status
                        'ownership_status': None,  # Not owned by user
                        'user_rating': None,
                        'personal_notes': '',
                        'locations': [],
                        'custom_metadata': {},
                        'start_date': None,
                        'finish_date': None,
                        'date_added': None
                    })
                    print(f"🌍 [GLOBAL_BOOKS] Book '{book_dict['title']}' visible globally (no user data)")
                
                # Filter by reading status if requested
                if reading_status:
                    if book_dict.get('reading_status') != reading_status:
                        continue  # Skip if doesn't match filter
                
                books.append(book_dict)
            
            print(f"✅ [GLOBAL_BOOKS] Returning {len(books)} books (global visibility)")
            return books
            
        except Exception as e:
            current_app.logger.error(f"Error getting global books with user overlay: {e}")
            import traceback
            traceback.print_exc()
            return []
    
    def _get_user_book_overlay(self, storage, user_id: str, book_id: str) -> Dict[str, Any]:
        """Get user-specific overlay data for a book, if it exists."""
        try:
            # Check if user has an OWNS relationship with this book
            owns_query = """
            MATCH (u:User {id: $user_id})-[r:OWNS]->(b:Book {id: $book_id})
            RETURN r
            """
            
            owns_result = storage.query(owns_query, {
                'user_id': user_id,
                'book_id': book_id
            })
            
            if owns_result and len(owns_result) > 0:
                owns_data = dict(owns_result[0].get('col_0', {}))
                
                # Convert to expected format
                overlay = {
                    'reading_status': owns_data.get('reading_status'),
                    'ownership_status': owns_data.get('ownership_status', 'owned'),
                    'user_rating': owns_data.get('user_rating'),
                    'personal_notes': owns_data.get('notes', ''),
                    'start_date': owns_data.get('start_date'),
                    'finish_date': owns_data.get('finish_date'),
                    'date_added': owns_data.get('date_added'),
                    'location_id': owns_data.get('location_id', ''),
                    'custom_metadata': owns_data.get('custom_metadata', {})
                }
                
                # Handle locations
                if overlay.get('location_id'):
                    overlay['locations'] = [overlay['location_id']]
                else:
                    overlay['locations'] = []
                
                return overlay
            
            return None  # No user-specific data
            
        except Exception as e:
            print(f"❌ Error getting user overlay for book {book_id}: {e}")
            return None
    
    def add_book_to_user_library_sync(self, user_id: str, book_id: str, 
                                     reading_status: str = 'plan_to_read',
                                     ownership_status: str = 'owned',
                                     media_type: str = 'physical',
                                     location_id: str = None,
                                     notes: str = None,
                                     locations: List[str] = None,
                                     custom_metadata: Dict[str, Any] = None) -> bool:
        """Add a book to user's library (sync version)."""
        try:
            # Handle locations parameter - use first location if provided
            if locations and len(locations) > 0 and not location_id:
                location_id = locations[0]
            
            return run_async(self.add_book_to_user_library(
                user_id, book_id, reading_status, ownership_status, 
                media_type, location_id, notes, custom_metadata
            ))
        except Exception as e:
            current_app.logger.error(f"Error adding book to library: {e}")
            return False

    def find_or_create_book_sync(self, book: Book) -> Optional[Book]:
        """Find existing book or create new one (sync version)."""
        try:
            # First try to find existing book
            if book.isbn13:
                existing = run_async(self.kuzu_service.find_book_by_isbn(book.isbn13))
                if existing:
                    return self._convert_to_book_model(existing)
            
            # Create new book with full domain object (including relationships)
            created = run_async(self.kuzu_service.create_book_with_relationships(book))
            if created:
                return self._convert_to_book_model(created)
            return None
        except Exception as e:
            current_app.logger.error(f"Error finding or creating book: {e}")
            return None

    def get_book_by_uid_sync(self, uid: str, user_id: str) -> Optional[Dict[str, Any]]:
        """Get book by UID for a specific user using global book visibility (sync version)."""
        try:
            # Use global book visibility approach
            user_books = self.get_all_books_with_user_overlay_sync(user_id)
            
            # Find the specific book by UID
            for book in user_books:
                book_uid = book.get('uid') if isinstance(book, dict) else getattr(book, 'uid', None)
                book_id = book.get('id') if isinstance(book, dict) else getattr(book, 'id', None)
                
                if book_uid == uid or book_id == uid:
                    # Found the book, return it (already has overlay data)
                    return book
            
            # If not found in user books with overlay, try to get the global book directly
            book_data = run_async(self.kuzu_service.get_book(uid))
            if not book_data:
                return None
            
            # Set default values for books not in user's library
            book_data.update({
                'reading_status': None,
                'ownership_status': None,
                'media_type': None,
                'notes': '',
                'date_added': None,
                'location_id': '',
                'user_rating': None,
                'personal_notes': '',
                'start_date': None,
                'finish_date': None,
                'locations': [],
                'uid': uid,
                'custom_metadata': {}
            })
            
            return book_data
        
        except Exception as e:
            print(f"Error in get_book_by_uid_sync: {e}")
            return None

    def get_user_book_sync(self, user_id: str, book_id: str) -> Optional[Dict[str, Any]]:
        """Get user's book by book ID - alias for get_book_by_uid_sync for compatibility."""
        return self.get_book_by_uid_sync(book_id, user_id)

    def update_book_sync(self, uid: str, user_id: str, **kwargs) -> bool:
        """Update book in user's library (sync version)."""
        try:
            print(f"🔍 [UPDATE_BOOK_SYNC] Called with uid={uid}, user_id={user_id}, kwargs={kwargs}")
            
            # Separate Book node fields from OWNS relationship fields
            book_fields = ['title', 'subtitle', 'description', 'published_date', 'page_count', 
                          'language', 'cover_url', 'isbn13', 'isbn10', 'asin', 'google_books_id', 
                          'openlibrary_id', 'average_rating', 'rating_count', 'series', 'series_volume', 'series_order']
            owns_fields = ['reading_status', 'ownership_status', 'media_type', 'location_id', 
                          'notes', 'personal_notes', 'start_date', 'finish_date', 'current_page', 'total_pages',
                          'date_acquired', 'purchase_price', 'condition', 'tags', 'review', 'user_rating',
                          'pace', 'character_driven', 'source']
            
            # Split updates into Book node and OWNS relationship updates
            book_updates = {}
            owns_updates = {}
            
            for field, value in kwargs.items():
                if field in book_fields:
                    book_updates[field] = value
                elif field in owns_fields:
                    owns_updates[field] = value
            
            print(f"🔍 [UPDATE_BOOK_SYNC] Book updates: {book_updates}")
            print(f"🔍 [UPDATE_BOOK_SYNC] OWNS updates: {owns_updates}")
            
            # First, get the book to find its internal ID
            user_book = self.get_book_by_uid_sync(uid, user_id)
            if not user_book:
                print(f"❌ [UPDATE_BOOK_SYNC] Book not found: {uid}")
                return False
            
            book_id = user_book.get('id') if isinstance(user_book, dict) else getattr(user_book, 'id', None)
            if not book_id:
                print(f"❌ [UPDATE_BOOK_SYNC] No book ID found for uid: {uid}")
                return False
            
            updated = False
            
            # Update Book node fields if any
            if book_updates:
                print(f"🔍 [UPDATE_BOOK_SYNC] Updating Book node with: {book_updates}")
                try:
                    from .infrastructure.kuzu_graph import get_graph_storage
                    graph_storage = get_graph_storage()
                    
                    # Update the Book node directly
                    query = """
                    MATCH (b:Book {id: $book_id})
                    SET """
                    
                    set_clauses = []
                    params = {'book_id': book_id}
                    
                    for field, value in book_updates.items():
                        if field in ['published_date'] and value and hasattr(value, 'isoformat'):
                            # Convert date to string for Kuzu
                            params[f'new_{field}'] = value.isoformat()
                        else:
                            params[f'new_{field}'] = value
                        
                        set_clauses.append(f'b.{field} = $new_{field}')
                    
                    query += ', '.join(set_clauses)
                    query += " RETURN b"
                    
                    print(f"🔍 [UPDATE_BOOK_SYNC] Executing Book update query: {query}")
                    print(f"🔍 [UPDATE_BOOK_SYNC] With params: {params}")
                    
                    result = graph_storage.query(query, params)
                    
                    if result and len(result) > 0:
                        print(f"✅ [UPDATE_BOOK_SYNC] Successfully updated Book node")
                        updated = True
                    else:
                        print(f"❌ [UPDATE_BOOK_SYNC] No Book node found or update failed")
                        
                except Exception as e:
                    print(f"❌ [UPDATE_BOOK_SYNC] Error updating Book node: {e}")
                    import traceback
                    traceback.print_exc()
            
            # Update OWNS relationship fields if any
            
            # Update OWNS relationship fields if any
            if owns_updates:
                print(f"🔍 [UPDATE_BOOK_SYNC] Updating OWNS relationship with: {owns_updates}")
                
                from .infrastructure.kuzu_graph import get_graph_storage
                graph_storage = get_graph_storage()
                
                # Update the OWNS relationship properties
                query = """
                MATCH (u:User {id: $user_id})-[r:OWNS]->(b:Book {id: $book_id})
                SET """
                
                set_clauses = []
                params = {'user_id': user_id, 'book_id': book_id}
                
                for field, value in owns_updates.items():
                    if field in ['start_date', 'finish_date', 'date_acquired'] and value:
                        # For date fields, pass the date object directly to Kuzu
                        if hasattr(value, 'isoformat'):
                            # If it's already a date/datetime object, use it directly
                            params[f'new_{field}'] = value
                        else:
                            # If it's a string, parse it to a datetime
                            from datetime import datetime
                            try:
                                params[f'new_{field}'] = datetime.fromisoformat(str(value))
                            except:
                                params[f'new_{field}'] = value
                    elif field in ['current_page', 'total_pages', 'user_rating'] and value is not None:
                        # For numeric fields, ensure they're integers/floats, not strings
                        try:
                            if field == 'user_rating':
                                params[f'new_{field}'] = float(value)
                            else:
                                params[f'new_{field}'] = int(value)
                        except (ValueError, TypeError):
                            params[f'new_{field}'] = value
                    else:
                        params[f'new_{field}'] = str(value) if value is not None else ''
                    
                    set_clauses.append(f'r.{field} = $new_{field}')
                
                query += ', '.join(set_clauses)
                query += " RETURN r"
                
                print(f"🔍 [UPDATE_BOOK_SYNC] Executing query: {query}")
                print(f"🔍 [UPDATE_BOOK_SYNC] With params: {params}")
                
                result = graph_storage.query(query, params)
                
                if result and len(result) > 0:
                    print(f"✅ [UPDATE_BOOK_SYNC] Successfully updated OWNS relationship")
                    updated = True
                else:
                    print(f"❌ [UPDATE_BOOK_SYNC] No OWNS relationship found or update failed")
            
            # Legacy handling for simple finish_date updates
            if 'finish_date' in kwargs and not owns_updates:
                if kwargs['finish_date']:
                    result = run_async(self.update_reading_status(user_id, uid, 'read'))
                    if result:
                        updated = True
                else:
                    result = run_async(self.update_reading_status(user_id, uid, 'reading'))
                    if result:
                        updated = True
            
            if not book_updates and not owns_updates and 'finish_date' not in kwargs:
                print(f"🔍 [UPDATE_BOOK_SYNC] No updates to apply")
            
            return updated
            
        except Exception as e:
            print(f"❌ [UPDATE_BOOK_SYNC] Error updating book: {e}")
            import traceback
            traceback.print_exc()
            current_app.logger.error(f"Error updating book: {e}")
            return False
            
        except Exception as e:
            print(f"❌ [UPDATE_BOOK_SYNC] Error updating book: {e}")
            import traceback
            traceback.print_exc()
            current_app.logger.error(f"Error updating book: {e}")
            return False

    def delete_book_sync(self, uid: str, user_id: str) -> bool:
        """Delete book from user's library (sync version)."""
        try:
            return run_async(self.delete_book(uid, user_id))
        except Exception as e:
            current_app.logger.error(f"Error deleting book: {e}")
            return False

    def delete_book_globally_sync(self, uid: str) -> bool:
        """Delete book globally from system (sync version)."""
        try:
            return run_async(self.delete_book_globally(uid))
        except Exception as e:
            current_app.logger.error(f"Error deleting book globally: {e}")
            return False

    def get_book_categories_sync(self, book_id: str) -> List[Dict[str, Any]]:
        """Get categories for a book (sync version)."""
        try:
            # Use the repository method instead of direct graph access
            categories = run_async(self.kuzu_service.book_repo.get_book_categories(book_id))
            return categories
        except Exception as e:
            current_app.logger.error(f"Error getting book categories for {book_id}: {e}")
            return []
    
    def list_all_persons_sync(self) -> List[Dict[str, Any]]:
        """List all persons (authors, contributors) in the system (sync version)."""
        try:
            # Use the Kuzu service to get all persons
            persons = run_async(self.kuzu_service.book_repo.get_all_persons())
            return persons or []
        except Exception as e:
            current_app.logger.error(f"Error listing persons: {e}")
            return []
    
    def list_all_categories_sync(self, user_id: str = None) -> List[Dict[str, Any]]:
        """List all categories in the system (sync version)."""
        try:
            # Use the Kuzu service to get all categories
            categories = run_async(self.kuzu_service.book_repo.get_all_categories())
            return categories or []
        except Exception as e:
            current_app.logger.error(f"Error listing categories: {e}")
            return []
    
    def get_books_by_person_sync(self, person_id: str, user_id: str = None) -> Dict[str, List]:
        """Get books by a specific person, grouped by contribution type (sync version)."""
        try:
            from .infrastructure.kuzu_graph import get_kuzu_database
            db = get_kuzu_database()
            
            # Query for books authored by this person using graph relationships
            query = """
            MATCH (p:Person {id: $person_id})-[r:AUTHORED]->(b:Book)
            RETURN b, r.role as role, r.order_index as order_index
            """
            
            results = db.query(query, {"person_id": person_id})
            
            contributions = {
                'authored': [],
                'narrated': [],
                'edited': [],
                'contributed': []
            }
            
            for result in results:
                if 'col_0' in result and 'col_1' in result:
                    book_data = dict(result['col_0'])
                    role = result['col_1'] or 'authored'
                    book_id = book_data.get('id')
                    
                    if book_id and user_id:
                        # Get full book data with user overlay for template compatibility
                        full_book = self.get_book_by_uid_sync(book_id, user_id)
                        if full_book:
                            # Ensure uid and reading_status are available
                            if 'uid' not in full_book:
                                full_book['uid'] = book_id
                            if 'reading_status' not in full_book or full_book['reading_status'] is None:
                                full_book['reading_status'] = 'unread'
                            # Convert to object-like structure
                            book = type('Book', (), full_book)()
                        else:
                            # Fallback to basic book object
                            book = type('Book', (), {
                                'id': book_id,
                                'uid': book_id,  # Ensure uid is available for templates
                                'title': book_data.get('title', ''),
                                'cover_url': book_data.get('cover_url', ''),
                                'published_date': book_data.get('published_date'),
                                'reading_status': 'unread',  # Default reading status
                                'ownership_status': None,
                                'user_rating': None,
                                'personal_notes': '',
                                'locations': [],
                                'custom_metadata': {}
                            })()
                    else:
                        # Create basic book object when no user_id provided
                        book = type('Book', (), {
                            'id': book_id,
                            'uid': book_id,  # Ensure uid is available for templates
                            'title': book_data.get('title', ''),
                            'cover_url': book_data.get('cover_url', ''),
                            'published_date': book_data.get('published_date'),
                            'reading_status': 'unread',  # Default reading status
                            'ownership_status': None,
                            'user_rating': None,
                            'personal_notes': '',
                            'locations': [],
                            'custom_metadata': {}
                        })()
                    
                    # Add to appropriate contribution type
                    if role in contributions:
                        contributions[role].append(book)
                    else:
                        contributions['authored'].append(book)
            
            current_app.logger.info(f"Found contributions for person {person_id}: {[f'{k}: {len(v)}' for k, v in contributions.items()]}")
            return contributions
        except Exception as e:
            current_app.logger.error(f"Error getting books by person {person_id}: {e}")
            return {'authored': [], 'narrated': [], 'edited': [], 'contributed': []}
    
    def get_person_by_id_sync(self, person_id: str) -> object:
        """Get a person by ID (sync version)."""
        try:
            from .infrastructure.kuzu_graph import get_graph_storage
            storage = get_graph_storage()
            
            # Use proper Cypher query instead of get_node_by_id
            query = """
            MATCH (p:Person {id: $person_id})
            RETURN p
            LIMIT 1
            """
            
            results = storage.query(query, {"person_id": person_id})
            if not results:
                return None
            
            # Fix query result access - check 'result' first (single column), then 'col_0'
            result = results[0]
            person_data = None
            if 'result' in result:
                person_data = dict(result['result'])
            elif 'col_0' in result:
                person_data = dict(result['col_0'])
            else:
                return None
            
            # Create person object for template compatibility
            person = type('Person', (), {
                'id': person_data.get('id'),
                'name': person_data.get('name', ''),
                'normalized_name': person_data.get('normalized_name', ''),
                'birth_year': person_data.get('birth_year'),
                'death_year': person_data.get('death_year'),
                'birth_place': person_data.get('birth_place'),
                'bio': person_data.get('bio'),
                'website': person_data.get('website'),
                'created_at': person_data.get('created_at')
            })()
            
            current_app.logger.info(f"Found person: {person.name}")
            return person
        except Exception as e:
            current_app.logger.error(f"Error getting person {person_id}: {e}")
            return None
    
    def get_category_by_id_sync(self, category_id: str, user_id: str = None) -> object:
        """Get a category by ID (sync version)."""
        try:
            from .infrastructure.kuzu_graph import get_graph_storage
            storage = get_graph_storage()
            
            # Use proper Cypher query instead of get_node_by_id
            query = """
            MATCH (c:Category {id: $category_id})
            RETURN c
            LIMIT 1
            """
            
            results = storage.query(query, {"category_id": category_id})
            if not results:
                return None
            
            # Fix query result access - check 'result' first (single column), then 'col_0'
            result = results[0]
            category_data = None
            if 'result' in result:
                category_data = dict(result['result'])
            elif 'col_0' in result:
                category_data = dict(result['col_0'])
            else:
                return None
            
            # Create category object for template compatibility
            class Category:
                def __init__(self, data):
                    self.id = data.get('id')
                    self.name = data.get('name', '')
                    self.normalized_name = data.get('normalized_name', '')
                    self.description = data.get('description')
                    self.color = data.get('color')
                    self.icon = data.get('icon')
                    self.created_at = data.get('created_at')
                    self.parent_id = data.get('parent_id')
                    self.aliases = data.get('aliases', [])
                    self.level = 0
                    self.book_count = 0
                
                def get_ancestors(self):
                    """Get list of ancestor categories for breadcrumb navigation."""
                    # For now, return empty list since we don't have hierarchy implemented
                    return []
            
            category = Category(category_data)
            
            current_app.logger.info(f"Found category: {category.name}")
            return category
        except Exception as e:
            current_app.logger.error(f"Error getting category {category_id}: {e}")
            return None
    
    def get_books_by_category_sync(self, category_id: str, user_id: str = None, include_subcategories: bool = True) -> List[Dict[str, Any]]:
        """Get books by category ID (sync version)."""
        try:
            from .infrastructure.kuzu_graph import get_graph_storage
            storage = get_graph_storage()
            
            # Query for books in the category using graph relationships
            if user_id:
                # If user_id provided, only get books owned by that user
                query = """
                MATCH (u:User {id: $user_id})-[:OWNS]->(b:Book)-[:CATEGORIZED_AS]->(c:Category {id: $category_id})
                RETURN b
                """
                params = {'user_id': user_id, 'category_id': category_id}
            else:
                # Query for all books in the category
                query = """
                MATCH (b:Book)-[:CATEGORIZED_AS]->(c:Category {id: $category_id})
                RETURN b
                """
                params = {'category_id': category_id}
            
            result = storage.query(query, params)
            books = []
            
            for record in result:
                # Fix query result access - check 'result' first (single column), then 'col_0'
                book_data = None
                if 'result' in record:
                    book_data = dict(record['result'])
                elif 'col_0' in record:
                    book_data = dict(record['col_0'])
                
                if book_data:
                    books.append(book_data)
            
            current_app.logger.info(f"Found {len(books)} books for category {category_id}")
            return books
            
        except Exception as e:
            current_app.logger.error(f"Error getting books by category {category_id}: {e}")
            return []

    def get_child_categories_sync(self, parent_id: str, user_id: str = None) -> List[Dict[str, Any]]:
        """Get child categories of a parent category (sync version)."""
        try:
            from .infrastructure.kuzu_graph import get_graph_storage
            storage = get_graph_storage()
            
            # Query for child categories using parent_id field
            query = """
            MATCH (c:Category)
            WHERE c.parent_id = $parent_id
            RETURN c
            ORDER BY c.name ASC
            """
            
            result = storage.query(query, {"parent_id": parent_id})
            categories = []
            
            for record in result:
                # Fix query result access - check 'result' first (single column), then 'col_0'
                category_data = None
                if 'result' in record:
                    category_data = dict(record['result'])
                elif 'col_0' in record:
                    category_data = dict(record['col_0'])
                
                if category_data:
                    categories.append(category_data)
            
            current_app.logger.info(f"Found {len(categories)} child categories for parent {parent_id}")
            return categories
            
        except Exception as e:
            current_app.logger.error(f"Error getting child categories for parent {parent_id}: {e}")
            return []

    def update_user_book_sync(self, user_id: str, book_id: str, custom_metadata: Dict[str, Any] = None, custom_field_types: Dict[str, str] = None, **kwargs) -> bool:
        """Update user-book relationship with custom metadata (sync version)."""
        try:
            from datetime import datetime
            print(f"🔍 [UPDATE_USER_BOOK_SERVICES] Called with user_id={user_id}, book_id={book_id}, custom_metadata={custom_metadata}")
            
            if not custom_metadata:
                print(f"🔍 [UPDATE_USER_BOOK_SERVICES] No custom metadata to update")
                return True  # Nothing to update
            
            from .infrastructure.kuzu_graph import get_graph_storage
            graph_storage = get_graph_storage()
            
            # Verify user and book exist
            user_exists = graph_storage.get_node('User', user_id)
            book_exists = graph_storage.get_node('Book', book_id)
            print(f"🔍 [UPDATE_USER_BOOK_SERVICES] User exists: {user_exists is not None}, Book exists: {book_exists is not None}")
            
            if not user_exists:
                print(f"❌ [UPDATE_USER_BOOK_SERVICES] User {user_id} not found")
                return False
            if not book_exists:
                print(f"❌ [UPDATE_USER_BOOK_SERVICES] Book {book_id} not found")
                return False
            
            # For each custom field, store it with type information
            for field_name, field_value in custom_metadata.items():
                print(f"🔍 [UPDATE_USER_BOOK_SERVICES] Processing field {field_name} = {field_value}")
                
                if field_value is not None and field_value != '':
                    # Get field type if provided
                    field_type = custom_field_types.get(field_name) if custom_field_types else None
                    
                    # Use the enhanced store_custom_metadata method
                    success = graph_storage.store_custom_metadata(
                        user_id=user_id,
                        book_id=book_id,
                        field_name=field_name,
                        field_value=field_value,
                        field_type=field_type
                    )
                    
                    if success:
                        print(f"✅ [UPDATE_USER_BOOK_SERVICES] Saved custom field {field_name} = {field_value} for user {user_id}, book {book_id}")
                    else:
                        print(f"❌ [UPDATE_USER_BOOK_SERVICES] Failed to save custom field {field_name}")
                else:
                    print(f"🔍 [UPDATE_USER_BOOK_SERVICES] Skipping empty field {field_name}")
                        
            print(f"🔍 [UPDATE_USER_BOOK_SERVICES] Completed processing all custom fields")
            return True
            
        except Exception as e:
            print(f"❌ [UPDATE_USER_BOOK_SERVICES] Error updating user book custom metadata: {e}")
            import traceback
            traceback.print_exc()
            return False

    def _convert_to_book_model(self, book_data: Dict[str, Any]) -> Book:
        """Convert book data dictionary to Book domain model."""
        return Book(
            id=book_data['id'],
            title=book_data['title'],
            isbn13=book_data.get('isbn13'),
            isbn10=book_data.get('isbn10'),
            description=book_data.get('description'),
            published_date=book_data.get('published_date'),
            page_count=book_data.get('page_count'),
            language=book_data.get('language', 'en'),
            cover_url=book_data.get('cover_url'),
            average_rating=book_data.get('average_rating'),
            rating_count=book_data.get('rating_count'),
            created_at=book_data.get('created_at')
        )


class KuzuReadingLogService:
    """Reading log service using clean Kuzu architecture."""
    
    def __init__(self):
        self.kuzu_service = get_kuzu_service()
    
    async def get_user_statistics(self, user_id: str) -> Dict[str, Any]:
        """Get comprehensive user reading statistics."""
        try:
            return await self.kuzu_service.get_user_statistics(user_id)
        except Exception as e:
            current_app.logger.error(f"Error getting user statistics: {e}")
            return {}
    
    async def get_reading_timeline(self, user_id: str, limit: int = 20) -> List[Dict[str, Any]]:
        """Get user's reading timeline."""
        try:
            return await self.kuzu_service.get_reading_timeline(user_id, limit)
        except Exception as e:
            current_app.logger.error(f"Error getting reading timeline: {e}")
            return []

    def get_existing_log_sync(self, book_id: str, user_id: str, log_date) -> Optional[Dict[str, Any]]:
        """Get existing reading log for a specific date (sync version)."""
        try:
            # For now, return None until full reading log system is implemented
            # This would check if a reading log already exists for the given book/user/date
            return None
        except Exception as e:
            current_app.logger.error(f"Error getting existing log: {e}")
            return None

    def create_reading_log_sync(self, book_id: str, user_id: str, log_date) -> bool:
        """Create a reading log entry (sync version)."""
        try:
            # For now, return True to avoid errors until full reading log creation is implemented
            # This would create a new reading session/log entry
            current_app.logger.info(f"Reading log creation requested for user {user_id}, book {book_id}")
            return True
        except Exception as e:
            current_app.logger.error(f"Error creating reading log: {e}")
            return False

    def get_user_logs_count_sync(self, user_id: str) -> int:
        """Get count of user's reading logs (sync version)."""
        try:
            # For now, return 0 until full reading log counting is implemented
            # This would count all reading sessions/logs for the user
            return 0
        except Exception as e:
            current_app.logger.error(f"Error getting user logs count: {e}")
            return 0

    def get_recent_shared_logs_sync(self, days_back: int = 7, limit: int = 50) -> List[Dict[str, Any]]:
        """Get recent shared reading logs (sync version)."""
        try:
            # For now, return empty list until full shared reading logs system is implemented
            # This would return recent reading activity from the community
            return []
        except Exception as e:
            current_app.logger.error(f"Error getting recent shared logs: {e}")
            return []


class KuzuLocationService:
    """Location service using clean Kuzu architecture."""
    
    def __init__(self):
        self.kuzu_service = get_kuzu_service()
    
    async def create_location(self, user_id: str, name: str, description: str = None,
                             location_type: str = 'other', is_default: bool = False) -> Optional[Dict[str, Any]]:
        """Create a new location for user."""
        try:
            location_data = {
                'name': name,
                'description': description,
                'location_type': location_type,
                'is_default': is_default
            }
            
            return await self.kuzu_service.create_location(user_id, location_data)
        except Exception as e:
            current_app.logger.error(f"Error creating location: {e}")
            return None
    
    async def get_user_locations(self, user_id: str) -> List[Dict[str, Any]]:
        """Get all locations for a user."""
        try:
            return await self.kuzu_service.get_user_locations(user_id)
        except Exception as e:
            current_app.logger.error(f"Error getting user locations: {e}")
            return []


# Custom field service using Kuzu repository pattern
class CustomFieldService:
    """Custom field service using existing Kuzu repository."""
    
    def __init__(self):
        from .infrastructure.kuzu_graph import get_graph_storage
        self.repository = KuzuCustomFieldRepository(get_graph_storage())
    
    def get_user_fields_with_calculated_usage_sync(self, user_id: str):
        """Get user fields with calculated usage statistics."""
        try:
            # Use run_async to call the async repository method
            fields = run_async(self.repository.get_by_user(user_id))
            current_app.logger.info(f"Retrieved {len(fields)} custom fields for user {user_id}")
            return fields
        except Exception as e:
            current_app.logger.error(f"Error getting user fields with stats: {e}")
            return []
    
    def get_shareable_fields_with_calculated_usage_sync(self, exclude_user_id: str = None):
        """Get shareable fields with calculated usage statistics."""
        try:
            # Use run_async to call the async repository method
            fields = run_async(self.repository.get_shareable(exclude_user_id))
            current_app.logger.info(f"Retrieved {len(fields)} shareable custom fields")
            return fields
        except Exception as e:
            current_app.logger.error(f"Error getting shareable fields with stats: {e}")
            return []
    
    def get_user_fields_sync(self, user_id: str):
        """Get user fields."""
        try:
            # Use run_async to call the async repository method
            fields = run_async(self.repository.get_by_user(user_id))
            current_app.logger.info(f"Retrieved {len(fields)} custom fields for user {user_id}")
            return fields
        except Exception as e:
            current_app.logger.error(f"Error getting user fields: {e}")
            return []
    
    def create_field_sync(self, field_def):
        """Create a field."""
        field_name = getattr(field_def, 'name', 'unknown')
        current_app.logger.info(f"ℹ️ [CUSTOM_FIELD_SERVICE] Attempting to create custom field '{field_name}' for user {field_def.created_by_user_id}")
        try:
            # Use run_async to call the async repository method
            created_field = run_async(self.repository.create(field_def))
            
            if created_field:
                current_app.logger.info(f"✅ [CUSTOM_FIELD_SERVICE] Successfully created custom field '{field_name}' with id {created_field.id}")
                return True
            else:
                current_app.logger.error(f"❌ [CUSTOM_FIELD_SERVICE] Failed to create custom field '{field_name}' in repository.")
                return False
        except Exception as e:
            current_app.logger.error(f"❌ [CUSTOM_FIELD_SERVICE] Error creating field '{field_name}': {e}", exc_info=True)
            return False
    
    def get_field_by_id_sync(self, field_id: str):
        """Get field by ID."""
        try:
            # Use run_async to call the async repository method
            field = run_async(self.repository.get_by_id(field_id))
            return field
        except Exception as e:
            current_app.logger.error(f"Error getting field by ID: {e}")
            return None
    
    def update_field_sync(self, field_def):
        """Update a field."""
        try:
            # Use run_async to call the async repository method
            updated_field = run_async(self.repository.update(field_def))
            if updated_field:
                current_app.logger.info(f"Successfully updated custom field {field_def.name}")
                return True
            else:
                current_app.logger.error(f"Failed to update custom field {field_def.name}")
                return False
        except Exception as e:
            current_app.logger.error(f"Error updating field: {e}")
            return False
    
    def delete_field_sync(self, field_id: str):
        """Delete a field."""
        try:
            # Use run_async to call the async repository method
            success = run_async(self.repository.delete(field_id))
            if success:
                current_app.logger.info(f"Successfully deleted custom field {field_id}")
                return True
            else:
                current_app.logger.error(f"Failed to delete custom field {field_id}")
                return False
        except Exception as e:
            current_app.logger.error(f"Error deleting field: {e}")
            return False
    
    def search_fields_sync(self, query: str, user_id: str):
        """Search fields."""
        try:
            # Use run_async to call the async repository method
            fields = run_async(self.repository.search(query, user_id))
            current_app.logger.info(f"Found {len(fields)} custom fields matching query '{query}'")
            return fields
        except Exception as e:
            current_app.logger.error(f"Error searching fields: {e}")
            return []
    
    def get_custom_metadata_for_display(self, metadata_dict: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Convert custom metadata dictionary to display format."""
        try:
            display_items = []
            if metadata_dict:
                for key, value in metadata_dict.items():
                    if value is not None and value != "":
                        # Try to get field definition for proper display name
                        field_definition = None
                        try:
                            # Look for field by name in the repository
                            field_definition = run_async(self.repository.get_by_name(key))
                        except:
                            pass  # Field might not exist or other error
                        
                        display_name = key
                        if field_definition:
                            display_name = field_definition.display_name
                        else:
                            # Format the field name for display (convert underscores to spaces, title case)
                            display_name = key.replace('_', ' ').title()
                        
                        display_items.append({
                            'name': key,
                            'display_name': display_name,
                            'display_value': str(value),
                            'value': str(value),
                            'type': field_definition.field_type.value if field_definition else 'text'
                        })
            return display_items
        except Exception as e:
            current_app.logger.error(f"Error formatting metadata for display: {e}")
            return []
    
    def get_available_fields_sync(self, user_id: str, is_global: bool = False):
        """Get available fields for a user."""
        try:
            current_app.logger.info(f"Getting available fields for user {user_id}, global: {is_global}")
            # Get user's personal fields and optionally global fields
            if is_global:
                # Get global/shareable fields
                fields = run_async(self.repository.get_shareable())
                current_app.logger.info(f"Retrieved {len(fields)} global/shareable custom fields")
            else:
                # Get user's personal fields
                fields = run_async(self.repository.get_by_user(user_id))
                current_app.logger.info(f"Retrieved {len(fields)} personal custom fields for user {user_id}")
            
            return fields
        except Exception as e:
            current_app.logger.error(f"Error getting available fields: {e}")
            return []
    
    def validate_and_save_metadata(self, metadata: dict, user_id: str, is_global: bool = False):
        """Validate and save metadata."""
        try:
            # For now, return True and empty errors until full metadata system is implemented
            current_app.logger.info(f"Metadata validation requested for user {user_id}: {metadata}")
            return True, []
        except Exception as e:
            current_app.logger.error(f"Error validating metadata: {e}")
            return False, [str(e)]


class LegacyImportMappingService:
    """Import mapping service using existing Kuzu repository."""
    
    def __init__(self):
        from .infrastructure.kuzu_graph import get_graph_storage
        self.repository = KuzuImportMappingRepository(get_graph_storage())
    
    def get_user_templates_sync(self, user_id: str) -> List[Dict[str, Any]]:
        """Get user's import mapping templates (sync version)."""
        try:
            # TODO: Implement template system for import configurations
            current_app.logger.debug(f"Import templates not yet implemented for user {user_id}")
            return []
        except Exception as e:
            current_app.logger.error(f"Error getting user templates: {e}")
            return []
    
    def detect_template_sync(self, headers: List[str], user_id: str):
        """Detect matching template based on CSV headers (sync version)."""
        try:
            # For now, return None since template system isn't fully implemented
            # But check for Goodreads and StoryGraph patterns
            headers_lower = [h.lower().strip() for h in headers if h]
            
            # Check for Goodreads signature
            goodreads_signatures = ['book id', 'author l-f', 'bookshelves', 'exclusive shelf']
            goodreads_matches = sum(1 for sig in goodreads_signatures if sig in headers_lower)
            
            # Check for StoryGraph signature  
            storygraph_signatures = ['read status', 'moods', 'pace', 'character- or plot-driven?', 'isbn/uid', 'star rating']
            storygraph_matches = sum(1 for sig in storygraph_signatures if sig in headers_lower)
            
            # Return a basic template object if we detect a known format
            if goodreads_matches >= 3:
                # Return a basic Goodreads template
                from .domain.models import ImportMappingTemplate
                return ImportMappingTemplate(
                    id="__system_goodreads__",
                    name="Goodreads Export",
                    user_id="__system__",
                    description="Auto-detected Goodreads export file",
                    field_mappings={
                        # Standard Book Fields
                        'Title': 'title',
                        'Author': 'author',
                        'Author l-f': 'ignore',  # Ignore this field as specified
                        'Additional Authors': 'additional_author',  # Map to separate field for proper processing
                        'ISBN': 'isbn',
                        'ISBN13': 'isbn13',  # Map to proper isbn13 field
                        'Publisher': 'publisher',
                        'Number of Pages': 'page_count',
                        'Year Published': 'publication_year',
                        'Average Rating': 'ignore',  # Ignore this field as specified
                        'Binding': 'media_type',  # Map to Media Type field
                        
                        # Personal Reading Data
                        'My Rating': 'rating',
                        'Date Read': 'date_read',
                        'Date Added': 'date_added',
                        'Exclusive Shelf': 'reading_status',
                        'My Review': 'notes',
                        'Private Notes': 'notes',
                        
                        # Custom Fields for Goodreads-specific data
                        'Book Id': 'custom_global_goodreads_id',
                        'Original Publication Year': 'custom_global_original_publication_date',
                        'Bookshelves': 'custom_global_bookshelves',
                        'Bookshelves with positions': 'custom_global_bookshelves_with_positions',
                        'Spoiler': 'custom_global_spoiler',
                        'Read Count': 'custom_global_read_count',
                        'Owned Copies': 'ignore'  # Ignore this field as specified
                    },
                    sample_headers=headers,
                    created_at=datetime.utcnow()
                )
            elif storygraph_matches >= 3:
                # Return a basic StoryGraph template
                from .domain.models import ImportMappingTemplate
                return ImportMappingTemplate(
                    id="__system_storygraph__",
                    name="StoryGraph Export", 
                    user_id="__system__",
                    description="Auto-detected StoryGraph export file",
                    field_mappings={
                        # Standard Book Fields
                        'Title': 'title',
                        'Authors': 'author',
                        'Contributors': 'contributors',  # Map to separate field for proper processing
                        'ISBN/UID': 'isbn',
                        'Format': 'media_type',
                        'Read Status': 'reading_status',
                        'Date Added': 'date_added',
                        'Read Count': 'read_count',
                        'Star Rating': 'rating',
                        'Review': 'notes',
                        'Tags': 'categories',  # Map to categories/personal tags
                        'Owned?': 'ownership_status',
                        
                        # Custom Fields for StoryGraph-specific data
                        'Last Date Read': 'custom_global_last_date_read',
                        'Dates Read': 'custom_global_dates_read',
                        'Moods': 'custom_global_moods',
                        'Pace': 'custom_global_pace',
                        'Character- or Plot-Driven?': 'custom_global_character_or_plot_driven',
                        'Strong Character Development?': 'custom_global_strong_character_development',
                        'Loveable Characters?': 'custom_global_loveable_characters',
                        'Diverse Characters?': 'custom_global_diverse_characters',
                        'Flawed Characters?': 'custom_global_flawed_characters',
                        'Content Warnings': 'custom_global_content_warnings',
                        'Content Warning Description': 'custom_global_content_warning_description'
                    },
                    sample_headers=headers,
                    created_at=datetime.utcnow()
                )
            
            current_app.logger.debug(f"No template detected for headers: {headers}")
            return None
        except Exception as e:
            current_app.logger.error(f"Error detecting template: {e}")
            return None


class DirectImportService:
    """Direct import service for CSV and other formats."""
    
    def __init__(self):
        self.book_service = KuzuBookService()
    
    async def import_books_from_csv(self, file_path: str, user_id: str, 
                                   mapping_template: Dict[str, str] = None) -> Dict[str, Any]:
        """Import books from CSV file."""
        results = {
            'total_rows': 0,
            'successful_imports': 0,
            'failed_imports': 0,
            'errors': []
        }
        
        try:
            with open(file_path, 'r', encoding='utf-8') as csvfile:
                reader = csv.DictReader(csvfile)
                
                for row_num, row in enumerate(reader, 1):
                    results['total_rows'] += 1
                    
                    try:
                        # Apply mapping template if provided
                        if mapping_template:
                            mapped_row = {}
                            for field, csv_column in mapping_template.items():
                                if csv_column in row:
                                    mapped_row[field] = row[csv_column]
                            row = mapped_row
                        
                        # Create book data
                        book_data = {
                            'title': row.get('title', '').strip(),
                            'isbn13': normalize_isbn_upc(row.get('isbn13')),
                            'isbn10': normalize_isbn_upc(row.get('isbn10')),
                            'description': row.get('description', '').strip(),
                            'page_count': int(row.get('page_count', 0)) if row.get('page_count') else None,
                            'language': row.get('language', 'en').strip(),
                            'authors': [],
                            'categories': []
                        }
                        
                        # Handle authors
                        if row.get('authors'):
                            authors_str = row['authors'].strip()
                            if authors_str:
                                author_names = [name.strip() for name in authors_str.split(',')]
                                book_data['authors'] = [{'name': name, 'role': 'author'} for name in author_names if name]
                        
                        # Handle categories
                        if row.get('categories'):
                            categories_str = row['categories'].strip()
                            if categories_str:
                                category_names = [name.strip() for name in categories_str.split(',')]
                                book_data['categories'] = [{'name': name} for name in category_names if name]
                        
                        # Create book if title exists
                        if book_data['title']:
                            created_book = await self.book_service.create_book(book_data)
                            if created_book:
                                # Add to user's library
                                await self.book_service.add_book_to_user_library(
                                    user_id, created_book.id,
                                    reading_status=row.get('reading_status', 'plan_to_read'),
                                    ownership_status=row.get('ownership_status', 'owned'),
                                    media_type=row.get('media_type', 'physical')
                                )
                                results['successful_imports'] += 1
                            else:
                                results['failed_imports'] += 1
                                results['errors'].append(f"Row {row_num}: Failed to create book")
                        else:
                            results['failed_imports'] += 1
                            results['errors'].append(f"Row {row_num}: Missing title")
                    
                    except Exception as e:
                        results['failed_imports'] += 1
                        results['errors'].append(f"Row {row_num}: {str(e)}")
        
        except Exception as e:
            results['errors'].append(f"File reading error: {str(e)}")
        
        return results


# Service instances - these are what get imported by routes
user_service = KuzuUserService()
book_service = KuzuBookService()
reading_log_service = KuzuReadingLogService()
location_service = KuzuLocationService()
custom_field_service = CustomFieldService()
import_mapping_service = LegacyImportMappingService()
direct_import_service = DirectImportService()

# Export the normalize function for direct import
__all__ = [
    'user_service', 'book_service', 'reading_log_service', 'location_service',
    'custom_field_service', 'import_mapping_service', 'direct_import_service',
    'normalize_isbn_upc', 'extract_digits_only'
]