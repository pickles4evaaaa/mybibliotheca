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
from typing import List, Optional, Dict, Any, Tuple, Union, cast
from datetime import datetime, date, timedelta
from dataclasses import asdict
from functools import wraps
import logging

# Import async helper from new services
from .services.kuzu_async_helper import run_async

from flask import current_app
from flask_login import current_user
from werkzeug.local import LocalProxy

# Import domain models
from .domain.models import (
    Book, User, Author, Publisher, Series, Category, UserBookRelationship, 
    ReadingLog, ReadingStatus, OwnershipStatus, CustomFieldDefinition, 
    ImportMappingTemplate, CustomFieldType, Person, Location, MediaType,
    BookContribution, ContributionType
)

# Import the clean Kuzu integration service
from .kuzu_integration import get_kuzu_service

# Import repositories
from .infrastructure.kuzu_repositories import (
    KuzuUserRepository,
    KuzuBookRepository, 
    KuzuUserBookRepository,
    KuzuLocationRepository,
    KuzuCustomFieldRepository, 
    KuzuImportMappingRepository
)

logger = logging.getLogger(__name__)


class KuzuUserService:
    """User service using clean Kuzu architecture."""
    
    def __init__(self):
        self.kuzu_service = get_kuzu_service()
        self.user_repo = KuzuUserRepository()
        self.location_repo = KuzuLocationRepository()
    
    def get_user_by_id_sync(self, user_id: str) -> Optional[User]:
        """Get user by ID (sync version for Flask-Login)."""
        try:
            user_data = run_async(self.kuzu_service.get_user(user_id))
            if user_data:
                # Cast to Dict[str, Any] for type safety
                user_data = cast(Dict[str, Any], user_data)
                return User(
                    id=user_data['id'],
                    username=user_data['username'],
                    email=user_data['email'],
                    display_name=user_data.get('display_name'),
                    bio=user_data.get('bio'),
                    timezone=user_data.get('timezone', 'UTC'),
                    is_admin=user_data.get('is_admin', False),
                    is_active=user_data.get('is_active', True),
                    created_at=user_data.get('created_at') or datetime.utcnow()
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
                    created_at=user_data.get('created_at') or datetime.utcnow()
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
                    created_at=user_data.get('created_at') or datetime.utcnow()
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
                    created_at=user_data.get('created_at') or datetime.utcnow()
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
                    created_at=user_data.get('created_at') or datetime.utcnow()
                )
            return None
        except Exception as e:
            current_app.logger.error(f"Error getting user by username or email {username_or_email}: {e}")
            return None
    
    def get_user_by_username_or_email_sync(self, username_or_email: str) -> Optional[User]:
        """Get user by username or email (sync version for form validation)."""
        return run_async(self.get_user_by_username_or_email(username_or_email))
    
    async def create_user(self, username: str, email: str, password_hash: str, 
                         display_name: Optional[str] = None, is_admin: bool = False) -> Optional[User]:
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
                    created_at=created_user_data.get('created_at') or datetime.utcnow()
                )
            return None
        except Exception as e:
            current_app.logger.error(f"Error creating user {username}: {e}")
            return None

    def create_user_sync(self, username: str, email: str, password_hash: str, 
                        display_name: Optional[str] = None, is_admin: bool = False, 
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
                    created_at=created_user_data.get('created_at') or datetime.utcnow()
                )
            return None
        except Exception as e:
            current_app.logger.error(f"Error creating user {username}: {e}")
            return None

    def get_user_count_sync(self) -> int:
        """Get total user count (sync version for compatibility)."""
        try:
            count = run_async(self.kuzu_service.get_user_count())
            # Ensure we always return an int, never None
            return int(count) if count is not None else 0
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
                    created_at=user_data.get('created_at') or datetime.utcnow()
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
            
            updated_user_data = await self.kuzu_service.update_user(user.id or "", user_data)
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
                    created_at=updated_user_data.get('created_at') or datetime.utcnow()
                )
            return None
        except Exception as e:
            current_app.logger.error(f"Error updating user {user.id}: {e}")
            return None

    def update_user_sync(self, user: User) -> Optional[User]:
        """Update an existing user (sync version for admin tools)."""
        return run_async(self.update_user(user))

    async def create_user_location(self, user_id: str, name: str, description: Optional[str] = None,
                                  location_type: str = "home", is_default: bool = False) -> Optional[Location]:
        """Create a location for a user."""
        try:
            location = Location(
                id=str(uuid.uuid4()),
                user_id=user_id,
                name=name,
                description=description or f"Default location set during onboarding",
                location_type=location_type,
                is_default=is_default,
                is_active=True,
                created_at=datetime.utcnow()
            )
            
            created_location = await self.location_repo.create(location, user_id)
            if created_location:
                logger.info(f"✅ Created location '{name}' for user {user_id}")
            
            return created_location
            
        except Exception as e:
            logger.error(f"Failed to create location for user {user_id}: {e}")
            return None
    
    async def get_user_locations(self, user_id: str) -> List[Location]:
        """Get all locations for a user."""
        return await self.location_repo.get_user_locations(user_id)
    
    async def get_default_location(self, user_id: str) -> Optional[Location]:
        """Get the default location for a user."""
        return await self.location_repo.get_default_location(user_id)


class KuzuBookService:
    """Book service using clean Kuzu architecture."""
    
    def __init__(self):
        self.kuzu_service = get_kuzu_service()
        self.book_repo = KuzuBookRepository()
        self.user_book_repo = KuzuUserBookRepository()
    
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
    
    async def create_book(self, title: str, authors: Optional[List[str]] = None, 
                         categories: Optional[List[str]] = None, **kwargs) -> Optional[Book]:
        """Create a new book."""
        try:
            # Create a simple book object using a basic class
            class SimpleBook:
                def __init__(self, **kwargs):
                    self.id = str(uuid.uuid4())
                    self.title = kwargs.get('title', '')
                    self.normalized_title = kwargs.get('title', '').lower()
                    self.created_at = datetime.utcnow()
                    self.contributors = []
                    self.categories = []
                    
                    # Set additional attributes from kwargs
                    for key, value in kwargs.items():
                        if not hasattr(self, key):
                            setattr(self, key, value)
            
            book = SimpleBook(title=title, **kwargs)
            
            # Add contributors (authors)
            if authors:
                for i, author_name in enumerate(authors):
                    # Create simple contributor objects
                    class SimplePerson:
                        def __init__(self, name):
                            self.id = str(uuid.uuid4())
                            self.name = name
                            self.normalized_name = name.lower()
                            self.created_at = datetime.utcnow()
                    
                    class SimpleContribution:
                        def __init__(self, person, contribution_type, order):
                            self.person = person
                            self.contribution_type = contribution_type
                            self.order = order
                    
                    person = SimplePerson(author_name)
                    contribution = SimpleContribution(person, ContributionType.AUTHORED, i)
                    book.contributors.append(contribution)
            
            # Add categories
            if categories:
                for cat_name in categories:
                    class SimpleCategory:
                        def __init__(self, name):
                            self.id = str(uuid.uuid4())
                            self.name = name
                            self.normalized_name = name.lower()
                            self.created_at = datetime.utcnow()
                    
                    category = SimpleCategory(cat_name)
                    book.categories.append(category)
            
            # Use the repository to create the book
            created_book = await self.book_repo.create(book)
            if created_book:
                logger.info(f"✅ Book service created book: {title}")
            
            return created_book
            
        except Exception as e:
            logger.error(f"Failed to create book {title}: {e}")
            return None
    
    async def add_book_to_user_library(self, user_id: str, book_id: str,
                                      reading_status: Union[ReadingStatus, str] = ReadingStatus.PLAN_TO_READ,
                                      ownership_status: Union[OwnershipStatus, str] = OwnershipStatus.OWNED,
                                      media_type: Union[MediaType, str] = MediaType.PHYSICAL,
                                      location_id: Optional[str] = None,
                                      notes: Optional[str] = None,
                                      custom_metadata: Optional[Dict[str, Any]] = None) -> bool:
        """Add a book to a user's library."""
        try:
            # Handle both enum and string values
            if isinstance(reading_status, ReadingStatus):
                reading_status_value = reading_status.value
            else:
                reading_status_value = reading_status
            
            if isinstance(ownership_status, OwnershipStatus):
                ownership_status_value = ownership_status.value
            else:
                ownership_status_value = ownership_status
            
            if isinstance(media_type, MediaType):
                media_type_value = media_type.value
            else:
                media_type_value = media_type
            
            success = await self.user_book_repo.add_book_to_library(
                user_id, book_id, reading_status_value, ownership_status_value, 
                media_type_value, notes or "", location_id
            )
            
            if success:
                logger.info(f"✅ Added book {book_id} to user {user_id} library")
            
            return success
            
        except Exception as e:
            logger.error(f"Failed to add book to library: {e}")
            return False
    
    async def get_user_books(self, user_id: str, reading_status: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get books in a user's library."""
        return await self.user_book_repo.get_user_books(user_id, reading_status)
    
    def get_user_books_sync(self, user_id: str, reading_status: Optional[str] = None,
                           limit: int = 50) -> List[Dict[str, Any]]:
        """Get user's library with global book visibility (default behavior)."""
        try:
            return self.get_all_books_with_user_overlay_sync(user_id, limit=limit)
        except Exception as e:
            current_app.logger.error(f"Error getting user books: {e}")
            return []
    
    def get_all_books_with_user_overlay_sync(self, user_id: str, limit: int = 50) -> List[Dict[str, Any]]:
        """Get ALL books in the system - global book visibility model."""
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
            
            for record in result:
                if 'col_0' not in record:
                    continue
                    
                book_data = dict(record['col_0'])
                authors_data = record.get('col_1', []) or []
                categories_data = record.get('col_2', []) or []
                publisher_data = record.get('col_3')
                
                # Start with ALL Book node properties dynamically
                book_dict = dict(book_data)
                
                # Ensure uid exists as alias for id (for template compatibility)
                if 'uid' not in book_dict and 'id' in book_dict:
                    book_dict['uid'] = book_dict['id']
                
                # Override/add specific relationship data
                book_dict.update({
                    'authors': [{'id': a.get('id'), 'name': a.get('name')} for a in authors_data if a and isinstance(a, dict)],
                    'categories': [{'id': c.get('id'), 'name': c.get('name')} for c in categories_data if c and isinstance(c, dict)],
                    'publisher': {'id': publisher_data.get('id'), 'name': publisher_data.get('name')} if publisher_data and isinstance(publisher_data, dict) else None
                })
                
                # For now, set default user-specific fields to None/empty
                book_dict.update({
                    'reading_status': None,
                    'ownership_status': None,
                    'user_rating': None,
                    'personal_notes': '',
                    'locations': self._get_book_locations(storage, book_data.get('id', '')),
                    'location_id': book_data.get('location_id', ''),
                    'custom_metadata': {},
                    'start_date': None,
                    'finish_date': None,
                    'date_added': None,
                    'media_type': book_data.get('media_type', 'physical')
                })
                
                books.append(book_dict)
            
            return books
            
        except Exception as e:
            current_app.logger.error(f"Error getting global books: {e}")
            import traceback
            traceback.print_exc()
            return []
    
    def _get_book_locations(self, storage, book_id: str) -> List[Dict[str, Any]]:
        """Get location data for a book from current storage."""
        try:
            location_query = """
            MATCH (u:User)-[r:OWNS]->(b:Book {id: $book_id})
            WHERE r.location_id IS NOT NULL AND r.location_id <> ''
            OPTIONAL MATCH (l:Location {id: r.location_id})
            RETURN r.location_id as location_id, l
            """
            
            result = storage.query(location_query, {'book_id': book_id})
            
            if result and len(result) > 0:
                for record in result:
                    location_id = record.get('col_0')
                    location_node = record.get('col_1')
                    
                    if location_node:
                        location_data = dict(location_node)
                        return [location_data]
                    elif location_id:
                        return [{'id': location_id, 'name': f'Location {location_id}'}]
            
            return []
            
        except Exception as e:
            print(f"❌ Error getting book locations for {book_id}: {e}")
            return []
    
    def _convert_to_book_model(self, book_data: Dict[str, Any]) -> Any:
        """Convert book data dictionary to Book domain model."""
        # Handle created_at field
        created_at = book_data.get('created_at')
        if isinstance(created_at, str):
            try:
                created_at = datetime.fromisoformat(created_at)
            except ValueError:
                created_at = datetime.utcnow()
        elif created_at is None:
            created_at = datetime.utcnow()
        
        # Create a simple book object
        class SimpleBook:
            def __init__(self, **kwargs):
                for key, value in kwargs.items():
                    setattr(self, key, value)
        
        return SimpleBook(
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
            created_at=created_at
        )

    # Category management methods
    def list_all_categories_sync(self, user_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get all categories."""
        try:
            from .infrastructure.kuzu_repositories import KuzuCategoryRepository
            category_repo = KuzuCategoryRepository()
            
            # Use asyncio to run the async method
            import asyncio
            try:
                loop = asyncio.get_event_loop()
                if loop.is_running():
                    import concurrent.futures
                    with concurrent.futures.ThreadPoolExecutor() as executor:
                        def run_in_new_loop():
                            new_loop = asyncio.new_event_loop()
                            try:
                                return new_loop.run_until_complete(category_repo.get_all())
                            finally:
                                new_loop.close()
                        future = executor.submit(run_in_new_loop)
                        return future.result()
                else:
                    return loop.run_until_complete(category_repo.get_all())
            except RuntimeError:
                return asyncio.run(category_repo.get_all())
        except Exception as e:
            print(f"❌ Error getting all categories: {e}")
            return []

    def get_category_by_id_sync(self, category_id: str, user_id: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """Get a category by ID."""
        try:
            from .infrastructure.kuzu_repositories import KuzuCategoryRepository
            category_repo = KuzuCategoryRepository()
            
            # Use asyncio to run the async method
            import asyncio
            try:
                loop = asyncio.get_event_loop()
                if loop.is_running():
                    import concurrent.futures
                    with concurrent.futures.ThreadPoolExecutor() as executor:
                        def run_in_new_loop():
                            new_loop = asyncio.new_event_loop()
                            try:
                                return new_loop.run_until_complete(category_repo.get_by_id(category_id))
                            finally:
                                new_loop.close()
                        future = executor.submit(run_in_new_loop)
                        return future.result()
                else:
                    return loop.run_until_complete(category_repo.get_by_id(category_id))
            except RuntimeError:
                return asyncio.run(category_repo.get_by_id(category_id))
        except Exception as e:
            print(f"❌ Error getting category by ID {category_id}: {e}")
            return None

    def get_child_categories_sync(self, parent_id: str) -> List[Dict[str, Any]]:
        """Get child categories for a parent category."""
        try:
            from .infrastructure.kuzu_graph import get_kuzu_database
            db = get_kuzu_database()
            
            # Query for categories that have this parent_id
            query = """
            MATCH (c:Category)
            WHERE c.parent_id = $parent_id
            RETURN c
            ORDER BY c.name ASC
            """
            
            results = db.query(query, {"parent_id": parent_id})
            
            categories = []
            for result in results:
                if 'result' in result:
                    categories.append(dict(result['result']))
                elif 'col_0' in result:
                    categories.append(dict(result['col_0']))
            
            return categories
        except Exception as e:
            print(f"❌ Error getting child categories for {parent_id}: {e}")
            return []

    def get_category_children_sync(self, category_id: str, user_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get children of a category (alias for get_child_categories)."""
        return self.get_child_categories_sync(category_id)

    def get_books_by_category_sync(self, category_id: str, user_id: Optional[str] = None, include_subcategories: bool = False) -> List[Dict[str, Any]]:
        """Get books in a category."""
        try:
            from .infrastructure.kuzu_graph import get_kuzu_database
            db = get_kuzu_database()
            
            if include_subcategories:
                # For now, just get books from the specific category
                # TODO: Implement recursive descendant search
                query = """
                MATCH (b:Book)-[:CATEGORIZED_AS]->(c:Category {id: $category_id})
                RETURN b
                ORDER BY b.title ASC
                """
            else:
                # Query for books in this specific category
                query = """
                MATCH (b:Book)-[:CATEGORIZED_AS]->(c:Category {id: $category_id})
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

    def create_category_sync(self, category: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Create a new category."""
        try:
            from .infrastructure.kuzu_repositories import KuzuCategoryRepository
            category_repo = KuzuCategoryRepository()
            
            # Convert dict to object if needed
            if isinstance(category, dict):
                from types import SimpleNamespace
                category_obj = SimpleNamespace(**category)
            else:
                category_obj = category
            
            # Use asyncio to run the async method
            import asyncio
            try:
                loop = asyncio.get_event_loop()
                if loop.is_running():
                    import concurrent.futures
                    with concurrent.futures.ThreadPoolExecutor() as executor:
                        def run_in_new_loop():
                            new_loop = asyncio.new_event_loop()
                            try:
                                return new_loop.run_until_complete(category_repo.create(category_obj))
                            finally:
                                new_loop.close()
                        future = executor.submit(run_in_new_loop)
                        return future.result()
                else:
                    return loop.run_until_complete(category_repo.create(category_obj))
            except RuntimeError:
                return asyncio.run(category_repo.create(category_obj))
        except Exception as e:
            print(f"❌ Error creating category: {e}")
            return None

    def update_category_sync(self, category: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Update an existing category."""
        try:
            # For now, just return the updated category
            # TODO: Implement proper update logic in repository
            return category
        except Exception as e:
            print(f"❌ Error updating category: {e}")
            return None

    def delete_category_sync(self, category_id: str) -> bool:
        """Delete a category."""
        try:
            from .infrastructure.kuzu_graph import get_kuzu_database
            db = get_kuzu_database()
            
            # Query to delete the category node
            query = """
            MATCH (c:Category {id: $category_id})
            DETACH DELETE c
            """
            
            db.query(query, {"category_id": category_id})
            return True
        except Exception as e:
            print(f"❌ Error deleting category {category_id}: {e}")
            return False

    def search_categories_sync(self, query: str, limit: int = 10, user_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """Search categories by name."""
        try:
            from .infrastructure.kuzu_graph import get_kuzu_database
            db = get_kuzu_database()
            
            search_query = """
            MATCH (c:Category)
            WHERE c.name CONTAINS $query OR c.normalized_name CONTAINS $query
            RETURN c
            ORDER BY c.name ASC
            LIMIT $limit
            """
            
            results = db.query(search_query, {
                "query": query.lower(),
                "limit": limit
            })
            
            categories = []
            for result in results:
                if 'result' in result:
                    categories.append(dict(result['result']))
                elif 'col_0' in result:
                    categories.append(dict(result['col_0']))
            
            return categories
        except Exception as e:
            print(f"❌ Error searching categories: {e}")
            return []

    def get_root_categories_sync(self, user_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get root categories (categories without parent)."""
        try:
            from .infrastructure.kuzu_graph import get_kuzu_database
            db = get_kuzu_database()
            
            query = """
            MATCH (c:Category)
            WHERE c.parent_id IS NULL
            RETURN c
            ORDER BY c.name ASC
            """
            
            results = db.query(query)
            
            categories = []
            for result in results:
                if 'result' in result:
                    categories.append(dict(result['result']))
                elif 'col_0' in result:
                    categories.append(dict(result['col_0']))
            
            return categories
        except Exception as e:
            print(f"❌ Error getting root categories: {e}")
            return []

    def get_book_categories_sync(self, book_id: str) -> List[Dict[str, Any]]:
        """Get categories for a book."""
        try:
            from .infrastructure.kuzu_graph import get_kuzu_database
            db = get_kuzu_database()
            
            query = """
            MATCH (b:Book {id: $book_id})-[:CATEGORIZED_AS]->(c:Category)
            RETURN c
            ORDER BY c.name ASC
            """
            
            results = db.query(query, {"book_id": book_id})
            
            categories = []
            for result in results:
                if 'result' in result:
                    categories.append(dict(result['result']))
                elif 'col_0' in result:
                    categories.append(dict(result['col_0']))
            
            return categories
        except Exception as e:
            print(f"❌ Error getting book categories for {book_id}: {e}")
            return []

    def merge_categories_sync(self, primary_category_id: str, merge_category_ids: List[str]) -> bool:
        """Merge multiple categories into one."""
        try:
            from .infrastructure.kuzu_graph import get_kuzu_database
            db = get_kuzu_database()
            
            # For each category to merge, move all its books to the primary category
            for category_id in merge_category_ids:
                # Move books from this category to primary category
                query = """
                MATCH (b:Book)-[r:CATEGORIZED_AS]->(c:Category {id: $category_id})
                DELETE r
                WITH b
                MATCH (primary:Category {id: $primary_category_id})
                CREATE (b)-[:CATEGORIZED_AS]->(primary)
                """
                
                db.query(query, {
                    "category_id": category_id,
                    "primary_category_id": primary_category_id
                })
                
                # Delete the merged category
                self.delete_category_sync(category_id)
            
            return True
        except Exception as e:
            print(f"❌ Error merging categories: {e}")
            return False

    def find_or_create_book_sync(self, domain_book) -> Optional[Any]:
        """Find or create a book - stub implementation."""
        try:
            # TODO: Implement proper find or create logic
            return domain_book
        except Exception as e:
            print(f"❌ Error finding or creating book: {e}")
            return None

    def add_book_to_user_library_sync(self, user_id: str, book_id: str, reading_status) -> bool:
        """Add book to user library - stub implementation."""
        try:
            # TODO: Implement proper add to library logic
            return True
        except Exception as e:
            print(f"❌ Error adding book to user library: {e}")
            return False


# Service instances
user_service = KuzuUserService()
# Import the new KuzuServiceFacade which provides 100% compatibility with original KuzuBookService
from .services.kuzu_service_facade import KuzuServiceFacade as FullKuzuBookService
book_service = FullKuzuBookService()

# Placeholder services for compatibility
class StubService:
    """Stub service for missing services."""
    def __getattr__(self, name):
        def stub_method(*args, **kwargs):
            print(f"Warning: {name} method called on stub service")
            return None
        return stub_method

reading_log_service = StubService()
custom_field_service = StubService()
import_mapping_service = StubService()
direct_import_service = StubService()
