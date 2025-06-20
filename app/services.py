"""
Redis-only service layer for Bibliotheca.

This module provides service classes for comprehensive book management using Redis 
as the sole data store with graph database functionality.
"""

import os
import asyncio
from typing import List, Optional, Dict, Any, Tuple
from datetime import datetime, date, timedelta
from dataclasses import asdict
from functools import wraps

from flask import current_app
from flask_login import current_user

from .domain.models import Book, User, Author, Publisher, Series, Category, UserBookRelationship, ReadingLog, ReadingStatus, OwnershipStatus, OwnershipStatus, CustomFieldDefinition, ImportMappingTemplate, CustomFieldType
from .infrastructure.redis_repositories import RedisBookRepository, RedisUserRepository, RedisAuthorRepository, RedisUserBookRepository, RedisCustomFieldRepository, RedisImportMappingRepository
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
                future = executor.submit(asyncio.run, async_func(*args, **kwargs))
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
                                     locations: List[str] = None) -> bool:
        """Add a book to a user's library by creating a relationship."""
        try:
            print(f"[SERVICE] Adding book {book_id} to user {user_id}'s library")
            
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
                user_rating=None,
                user_review=None,
                personal_notes=None,
                start_date=None,
                finish_date=None,
                date_added=datetime.now(),
                locations=locations or [],
                user_tags=[],
                source='manual',
                created_at=datetime.now(),
                updated_at=datetime.now()
            )
            
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
                            book.reading_status = rel.reading_status.value
                            book.ownership_status = rel.ownership_status.value
                            book.start_date = rel.start_date
                            book.finish_date = rel.finish_date
                            book.user_rating = rel.user_rating
                            book.personal_notes = rel.personal_notes
                            book.date_added = rel.date_added
                            book.user_tags = rel.user_tags
                            book.locations = rel.locations
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
            
            # Convert to Book objects with user-specific attributes
            books = []
            for rel in relationships:
                print(f"[SERVICE] Processing relationship for book_id: {rel.book_id}")
                # Get the book data
                book = await self.redis_book_repo.get_by_id(rel.book_id)
                if book:
                    print(f"[SERVICE] Found book: {book.title}")
                    # Add user-specific attributes directly to the book object
                    book.reading_status = rel.reading_status.value
                    book.ownership_status = rel.ownership_status.value
                    book.start_date = rel.start_date
                    book.finish_date = rel.finish_date
                    book.user_rating = rel.user_rating
                    book.personal_notes = rel.personal_notes
                    book.date_added = rel.date_added
                    book.user_tags = rel.user_tags
                    book.locations = rel.locations
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
                                 'date_started', 'date_finished', 'date_added', 'favorite', 'priority'}
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
        # Try to find existing book by ISBN first
        if domain_book.isbn13:
            existing_books = await self.get_books_by_isbn(domain_book.isbn13)
            if existing_books:
                print(f"[SERVICE] Found existing book by ISBN13: {existing_books[0].title}")
                return existing_books[0]
        
        if domain_book.isbn10:
            existing_books = await self.get_books_by_isbn(domain_book.isbn10)
            if existing_books:
                print(f"[SERVICE] Found existing book by ISBN10: {existing_books[0].title}")
                return existing_books[0]
        
        # If no existing book found, create new one
        print(f"[SERVICE] No existing book found, creating new book")
        return await self.create_book(domain_book)

    # Sync wrappers for Flask compatibility
    @run_async
    async def create_book_sync(self, domain_book: Book) -> Book:
        """Sync wrapper for create_book."""
        return await self.create_book(domain_book)
    
    @run_async
    async def add_book_to_user_library_sync(self, user_id: str, book_id: str, 
                                          reading_status: ReadingStatus = ReadingStatus.PLAN_TO_READ,
                                          ownership_status: OwnershipStatus = OwnershipStatus.OWNED,
                                          locations: List[str] = None) -> bool:
        """Sync wrapper for add_book_to_user_library."""
        return await self.add_book_to_user_library(user_id, book_id, reading_status, ownership_status, locations)
    
    @run_async
    async def get_book_by_id_sync(self, book_id: str, user_id: str) -> Optional[Book]:
        """Sync wrapper for get_book_by_id."""
        return await self.get_book_by_id(book_id, user_id)
    
    @run_async
    async def get_book_by_uid_sync(self, uid: str, user_id: str) -> Optional[Book]:
        """Sync wrapper for get_book_by_uid."""
        return await self.get_book_by_uid(uid, user_id)
    
    @run_async
    async def get_user_books_sync(self, user_id: str) -> List[Book]:
        """Sync wrapper for get_user_books."""
        return await self.get_user_books(user_id)
    
    @run_async
    async def update_book_sync(self, uid: str, user_id: str, **kwargs) -> Optional[Book]:
        """Sync wrapper for update_book."""
        return await self.update_book(uid, user_id, **kwargs)
    
    @run_async
    async def delete_book_sync(self, uid: str, user_id: str) -> bool:
        """Sync wrapper for delete_book."""
        return await self.delete_book(uid, user_id)
    
    @run_async
    async def get_books_by_isbn_sync(self, isbn: str) -> List[Book]:
        """Sync wrapper for get_books_by_isbn."""
        return await self.get_books_by_isbn(isbn)
    
    @run_async
    async def search_books_sync(self, query: str, user_id: str, filter_params: Optional[Dict] = None) -> List[Book]:
        """Sync wrapper for search_books."""
        return await self.search_books(query, user_id, filter_params)
    
    @run_async
    async def find_or_create_book_sync(self, domain_book: Book) -> Book:
        """Sync wrapper for find_or_create_book."""
        return await self.find_or_create_book(domain_book)


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
    
    async def update_user_profile(self, user_id: str, username: str = None, email: str = None) -> Optional[User]:
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
    async def create_user_sync(self, username: str, email: str, password_hash: str,
                              is_admin: bool = False, is_active: bool = True,
                              password_must_change: bool = False) -> User:
        """Sync wrapper for create_user."""
        return await self.create_user(username, email, password_hash, is_admin, is_active, password_must_change)
    
    @run_async
    async def get_user_by_id_sync(self, user_id: str) -> Optional[User]:
        """Sync wrapper for get_user_by_id."""
        return await self.get_user_by_id(user_id)
    
    @run_async
    async def get_user_by_username_sync(self, username: str) -> Optional[User]:
        """Sync wrapper for get_user_by_username."""
        return await self.get_user_by_username(username)
    
    @run_async
    async def get_user_by_email_sync(self, email: str) -> Optional[User]:
        """Sync wrapper for get_user_by_email."""
        return await self.get_user_by_email(email)
    
    @run_async
    async def get_user_by_username_or_email_sync(self, username_or_email: str) -> Optional[User]:
        """Sync wrapper for get_user_by_username_or_email."""
        return await self.get_user_by_username_or_email(username_or_email)
    
    @run_async
    async def get_user_count_sync(self) -> int:
        """Sync wrapper for get_user_count."""
        return await self.get_user_count()
    
    @run_async
    async def get_admin_count_sync(self) -> int:
        """Sync wrapper for get_admin_count."""
        return await self.get_admin_count()
    
    @run_async
    async def get_active_user_count_sync(self) -> int:
        """Sync wrapper for get_active_user_count."""
        return await self.get_active_user_count()
    
    @run_async
    async def get_all_users_sync(self) -> List[User]:
        """Sync wrapper for get_all_users."""
        return await self.get_all_users()
    
    @run_async
    async def get_sharing_users_sync(self) -> List[User]:
        """Sync wrapper for get_sharing_users."""
        return await self.get_sharing_users()
    
    @run_async
    async def update_user_sync(self, user: User) -> User:
        """Sync wrapper for update_user."""
        return await self.update_user(user)
    
    @run_async
    async def update_user_profile_sync(self, user_id: str, username: str = None, email: str = None) -> Optional[User]:
        """Sync wrapper for update_user_profile."""
        return await self.update_user_profile(user_id, username, email)
    
    @run_async
    async def update_user_password_sync(self, user_id: str, password_hash: str, clear_must_change: bool = True) -> Optional[User]:
        """Sync wrapper for update_user_password."""
        return await self.update_user_password(user_id, password_hash, clear_must_change)


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
                created_at=datetime.fromisoformat(log_data['created_at']) if log_data.get('created_at') else None
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
        # In a real implementation, you'd filter by users who have sharing enabled
        
        cutoff_date = date.today() - timedelta(days=days_back)
        cutoff_ordinal = cutoff_date.toordinal()
        
        all_logs = []
        
        # Get all user reading log indices
        pattern = "user_reading_logs:*"
        user_log_keys = await self.storage.scan_keys(pattern)
        
        for user_logs_key in user_log_keys:
            # Get logs from this user since the cutoff date
            recent_log_keys = await self.storage.get_sorted_set_range_by_score(
                user_logs_key, cutoff_ordinal, float('inf'), limit=limit
            )
            
            # Fetch the actual log data
            for log_key in recent_log_keys:
                log_data = await self.storage.get_json(log_key)
                if log_data:
                    reading_log = ReadingLog(
                        id=log_data['id'],
                        book_id=log_data['book_id'],
                        user_id=log_data['user_id'],
                        date=datetime.fromisoformat(log_data['date']).date(),
                        created_at=datetime.fromisoformat(log_data['created_at']) if log_data.get('created_at') else None
                    )
                    all_logs.append(reading_log)
        
        # Sort by date descending and limit
        all_logs.sort(key=lambda log: log.date, reverse=True)
        return all_logs[:limit]
    
    @run_async
    async def get_user_log_dates(self, user_id: str) -> List[date]:
        """Get all dates that a user has reading logs for."""
        user_logs_key = f"user_reading_logs:{user_id}"
        log_keys = await self.storage.get_sorted_set_range(user_logs_key)
        
        dates = []
        for log_key in log_keys:
            # Extract date from key: reading_log:user_id:book_id:date
            date_str = log_key.split(':')[-1]
            try:
                log_date = datetime.fromisoformat(date_str).date()
                dates.append(log_date)
            except:
                continue
        
        return sorted(set(dates))  # Remove duplicates and sort
    
    # Sync wrappers for Flask compatibility
    def get_existing_log_sync(self, book_id: str, user_id: str, date: date) -> Optional['ReadingLog']:
        """Sync wrapper for get_existing_log."""
        return self.get_existing_log(book_id, user_id, date)
    
    def create_reading_log_sync(self, book_id: str, user_id: str, date: date) -> 'ReadingLog':
        """Sync wrapper for create_reading_log."""
        return self.create_reading_log(book_id, user_id, date)
    
    def get_user_logs_count_sync(self, user_id: str) -> int:
        """Sync wrapper for get_user_logs_count."""
        return self.get_user_logs_count(user_id)
    
    def get_recent_shared_logs_sync(self, days_back: int = 7, limit: int = 50) -> List['ReadingLog']:
        """Sync wrapper for get_recent_shared_logs."""
        return self.get_recent_shared_logs(days_back, limit)
    
    def get_user_log_dates_sync(self, user_id: str) -> List[date]:
        """Sync wrapper for get_user_log_dates."""
        return self.get_user_log_dates(user_id)


class RedisCustomFieldService:
    """Service for managing custom field definitions."""
    
    def __init__(self):
        self.redis_enabled = os.getenv('GRAPH_DATABASE_ENABLED', 'true').lower() == 'true'
        
        if not self.redis_enabled:
            raise RuntimeError("Custom fields require Redis/graph database to be enabled")
        
        self.storage = get_graph_storage()
        self.repository = RedisCustomFieldRepository(self.storage)
    
    @run_async
    async def create_field(self, field_def: CustomFieldDefinition) -> CustomFieldDefinition:
        """Create a new custom field definition."""
        return await self.repository.create(field_def)
    
    @run_async
    async def get_field_by_id(self, field_id: str) -> Optional[CustomFieldDefinition]:
        """Get a custom field definition by ID."""
        return await self.repository.get_by_id(field_id)
    
    @run_async
    async def get_user_fields(self, user_id: str) -> List[CustomFieldDefinition]:
        """Get all custom field definitions created by a user."""
        return await self.repository.get_by_user(user_id)
    
    @run_async
    async def get_shareable_fields(self, exclude_user_id: Optional[str] = None) -> List[CustomFieldDefinition]:
        """Get all shareable custom field definitions."""
        return await self.repository.get_shareable(exclude_user_id)
    
    @run_async
    async def search_fields(self, query: str, user_id: Optional[str] = None) -> List[CustomFieldDefinition]:
        """Search custom field definitions."""
        return await self.repository.search(query, user_id)
    
    @run_async
    async def update_field(self, field_def: CustomFieldDefinition) -> CustomFieldDefinition:
        """Update an existing custom field definition."""
        return await self.repository.update(field_def)
    
    @run_async
    async def delete_field(self, field_id: str) -> bool:
        """Delete a custom field definition."""
        return await self.repository.delete(field_id)
    
    @run_async
    async def increment_field_usage(self, field_id: str) -> None:
        """Increment usage count for a field definition."""
        await self.repository.increment_usage(field_id)
    
    @run_async
    async def get_popular_fields(self, limit: int = 20) -> List[CustomFieldDefinition]:
        """Get most popular shareable custom field definitions."""
        return await self.repository.get_popular(limit)
    
    def validate_field_value(self, field_def: CustomFieldDefinition, value: Any) -> Tuple[bool, Optional[str]]:
        """Validate a value against a custom field definition."""
        if value is None or value == "":
            return True, None  # Allow empty values for now
        
        try:
            if field_def.field_type == CustomFieldType.TEXT:
                if not isinstance(value, str):
                    return False, "Value must be text"
            
            elif field_def.field_type == CustomFieldType.TEXTAREA:
                if not isinstance(value, str):
                    return False, "Value must be text"
            
            elif field_def.field_type == CustomFieldType.NUMBER:
                float(value)  # Try to convert to number
            
            elif field_def.field_type == CustomFieldType.BOOLEAN:
                if not isinstance(value, bool) and str(value).lower() not in ['true', 'false', '1', '0']:
                    return False, "Value must be true/false"
            
            elif field_def.field_type == CustomFieldType.DATE:
                if isinstance(value, str):
                    datetime.fromisoformat(value.replace('Z', '+00:00'))
                elif not isinstance(value, (date, datetime)):
                    return False, "Value must be a valid date"
            
            elif field_def.field_type in [CustomFieldType.RATING_5, CustomFieldType.RATING_10]:
                rating_value = float(value)
                if not (field_def.rating_min <= rating_value <= field_def.rating_max):
                    return False, f"Rating must be between {field_def.rating_min} and {field_def.rating_max}"
            
            elif field_def.field_type in [CustomFieldType.LIST, CustomFieldType.TAGS]:
                if isinstance(value, str):
                    # Split comma-separated values
                    value = [v.strip() for v in value.split(',') if v.strip()]
                if not isinstance(value, list):
                    return False, "Value must be a list or comma-separated text"
            
            elif field_def.field_type == CustomFieldType.URL:
                if not isinstance(value, str) or not (value.startswith('http://') or value.startswith('https://')):
                    return False, "Value must be a valid URL starting with http:// or https://"
            
            elif field_def.field_type == CustomFieldType.EMAIL:
                import re
                if not isinstance(value, str) or not re.match(r'^[^@]+@[^@]+\.[^@]+$', value):
                    return False, "Value must be a valid email address"
            
            return True, None
            
        except (ValueError, TypeError) as e:
            return False, f"Invalid value: {str(e)}"
    
    # Sync wrappers
    def create_field_sync(self, field_def: CustomFieldDefinition) -> CustomFieldDefinition:
        """Sync wrapper for create_field."""
        return self.create_field(field_def)
    
    def get_field_by_id_sync(self, field_id: str) -> Optional[CustomFieldDefinition]:
        """Sync wrapper for get_field_by_id."""
        return self.get_field_by_id(field_id)
    
    def get_user_fields_sync(self, user_id: str) -> List[CustomFieldDefinition]:
        """Sync wrapper for get_user_fields."""
        return self.get_user_fields(user_id)
    
    def get_shareable_fields_sync(self, exclude_user_id: Optional[str] = None) -> List[CustomFieldDefinition]:
        """Sync wrapper for get_shareable_fields."""
        return self.get_shareable_fields(exclude_user_id)
    
    def search_fields_sync(self, query: str, user_id: Optional[str] = None) -> List[CustomFieldDefinition]:
        """Sync wrapper for search_fields."""
        return self.search_fields(query, user_id)
    
    def update_field_sync(self, field_def: CustomFieldDefinition) -> CustomFieldDefinition:
        """Sync wrapper for update_field."""
        return self.update_field(field_def)
    
    def delete_field_sync(self, field_id: str) -> bool:
        """Sync wrapper for delete_field."""
        return self.delete_field(field_id)
    
    def get_popular_fields_sync(self, limit: int = 20) -> List[CustomFieldDefinition]:
        """Sync wrapper for get_popular_fields."""
        return self.get_popular_fields(limit)


class RedisImportMappingService:
    """Service for managing import mapping templates."""
    
    def __init__(self):
        self.redis_enabled = os.getenv('GRAPH_DATABASE_ENABLED', 'true').lower() == 'true'
        
        if not self.redis_enabled:
            raise RuntimeError("Import mapping requires Redis/graph database to be enabled")
        
        self.storage = get_graph_storage()
        self.repository = RedisImportMappingRepository(self.storage)
    
    @run_async
    async def create_template(self, template: ImportMappingTemplate) -> ImportMappingTemplate:
        """Create a new import mapping template."""
        return await self.repository.create(template)
    
    @run_async
    async def get_template_by_id(self, template_id: str) -> Optional[ImportMappingTemplate]:
        """Get an import mapping template by ID."""
        return await self.repository.get_by_id(template_id)
    
    @run_async
    async def get_user_templates(self, user_id: str) -> List[ImportMappingTemplate]:
        """Get all import mapping templates for a user."""
        return await self.repository.get_by_user(user_id)
    
    @run_async
    async def detect_template(self, headers: List[str], user_id: str) -> Optional[ImportMappingTemplate]:
        """Detect matching template based on CSV headers."""
        return await self.repository.detect_template(headers, user_id)
    
    @run_async
    async def update_template(self, template: ImportMappingTemplate) -> ImportMappingTemplate:
        """Update an existing import mapping template."""
        return await self.repository.update(template)
    
    @run_async
    async def delete_template(self, template_id: str) -> bool:
        """Delete an import mapping template."""
        return await self.repository.delete(template_id)
    
    @run_async
    async def increment_template_usage(self, template_id: str) -> None:
        """Increment usage count and update last used timestamp."""
        await self.repository.increment_usage(template_id)
    
    # Sync wrappers
    def create_template_sync(self, template: ImportMappingTemplate) -> ImportMappingTemplate:
        """Sync wrapper for create_template."""
        return self.create_template(template)
    
    def get_template_by_id_sync(self, template_id: str) -> Optional[ImportMappingTemplate]:
        """Sync wrapper for get_template_by_id."""
        return self.get_template_by_id(template_id)
    
    def get_user_templates_sync(self, user_id: str) -> List[ImportMappingTemplate]:
        """Sync wrapper for get_user_templates."""
        return self.get_user_templates(user_id)
    
    def detect_template_sync(self, headers: List[str], user_id: str) -> Optional[ImportMappingTemplate]:
        """Sync wrapper for detect_template."""
        return self.detect_template(headers, user_id)
    
    def update_template_sync(self, template: ImportMappingTemplate) -> ImportMappingTemplate:
        """Sync wrapper for update_template."""
        return self.update_template(template)
    
    def delete_template_sync(self, template_id: str) -> bool:
        """Sync wrapper for delete_template."""
        return self.delete_template(template_id)


class CustomMetadataService:
    """Service for managing custom metadata values on books and user book relationships."""
    
    def __init__(self):
        # Initialize with None, will be set after global services are created
        self.custom_field_service = None
        self.book_service = None
    
    def _ensure_services(self):
        """Lazy initialization of services to avoid circular dependencies."""
        if self.custom_field_service is None:
            global custom_field_service, book_service
            self.custom_field_service = custom_field_service
            self.book_service = book_service
    
    def create_field(self, field_def: CustomFieldDefinition) -> CustomFieldDefinition:
        """Create a new custom field definition."""
        self._ensure_services()
        return self.custom_field_service.create_field_sync(field_def)
    
    def get_available_fields(self, user_id: str, is_global: bool = False) -> List[CustomFieldDefinition]:
        """Get all available custom fields for a user (their own + shareable)."""
        self._ensure_services()
        try:
            # Get user's own fields
            user_fields = self.custom_field_service.get_user_fields_sync(user_id)
            
            # Get shareable fields from others
            shareable_fields = self.custom_field_service.get_shareable_fields_sync(exclude_user_id=user_id)
            
            # Filter by scope
            all_fields = user_fields + shareable_fields
            if is_global is not None:
                all_fields = [f for f in all_fields if f.is_global == is_global]
            
            return sorted(all_fields, key=lambda f: f.display_name)
        except Exception as e:
            print(f"Error getting available fields: {e}")
            return []
    
    def get_custom_metadata_for_display(self, metadata: Dict[str, Any], user_id: str, is_global: bool = False) -> List[Dict[str, Any]]:
        """Convert raw custom metadata to display format with field definitions."""
        if not metadata:
            return []
        
        # Get field definitions
        available_fields = self.get_available_fields(user_id, is_global)
        field_definitions = {f.name: f for f in available_fields}
        
        display_metadata = []
        for field_name, value in metadata.items():
            field_def = field_definitions.get(field_name)
            
            # Format value for display
            display_value = self._format_value_for_display(value, field_def)
            
            display_metadata.append({
                'name': field_name,
                'display_name': field_def.display_name if field_def else field_name.replace('_', ' ').title(),
                'value': value,
                'display_value': display_value,
                'field_type': field_def.field_type.value if field_def else 'text',
                'has_definition': field_def is not None,
                'field_def': field_def
            })
        
        return sorted(display_metadata, key=lambda x: x['display_name'])
    
    def _format_value_for_display(self, value: Any, field_def: Optional[CustomFieldDefinition]) -> str:
        """Format a custom field value for display."""
        if value is None or value == "":
            return ""
        
        if not field_def:
            return str(value)
        
        try:
            if field_def.field_type == CustomFieldType.BOOLEAN:
                return "Yes" if value else "No"
            
            elif field_def.field_type == CustomFieldType.DATE:
                if isinstance(value, str):
                    # Try to parse ISO format
                    try:
                        dt = datetime.fromisoformat(value.replace('Z', '+00:00'))
                        return dt.strftime('%Y-%m-%d')
                    except:
                        return value
                elif isinstance(value, (date, datetime)):
                    return value.strftime('%Y-%m-%d')
                return str(value)
            
            elif field_def.field_type in [CustomFieldType.RATING_5, CustomFieldType.RATING_10]:
                rating_value = float(value)
                stars = "★" * int(rating_value) + "☆" * (field_def.rating_max - int(rating_value))
                label = field_def.rating_labels.get(int(rating_value), "")
                if label:
                    return f"{stars} ({label})"
                return f"{stars} ({rating_value}/{field_def.rating_max})"
            
            elif field_def.field_type in [CustomFieldType.LIST, CustomFieldType.TAGS]:
                if isinstance(value, list):
                    return ", ".join(str(v) for v in value)
                elif isinstance(value, str):
                    return value
                return str(value)
            
            elif field_def.field_type == CustomFieldType.URL:
                if value.startswith(('http://', 'https://')):
                    return f'<a href="{value}" target="_blank" rel="noopener">{value}</a>'
                return str(value)
            
            elif field_def.field_type == CustomFieldType.EMAIL:
                return f'<a href="mailto:{value}">{value}</a>'
            
            else:
                return str(value)
                
        except Exception as e:
            print(f"Error formatting value {value}: {e}")
            return str(value)
    
    def validate_and_save_metadata(self, metadata: Dict[str, Any], user_id: str, is_global: bool = False) -> Tuple[bool, List[str]]:
        """Validate custom metadata values against field definitions."""
        if not metadata:
            return True, []
        
        available_fields = self.get_available_fields(user_id, is_global)
        field_definitions = {f.name: f for f in available_fields}
        
        errors = []
        validated_metadata = {}
        
        for field_name, value in metadata.items():
            field_def = field_definitions.get(field_name)
            
            if field_def:
                # Validate using field definition
                is_valid, error_msg = self.custom_field_service.validate_field_value(field_def, value)
                if not is_valid:
                    errors.append(f"{field_def.display_name}: {error_msg}")
                    continue
            
            # Store the value (potentially converting it)
            validated_metadata[field_name] = self._convert_value_for_storage(value, field_def)
        return len(errors) == 0, errors
    
    def _convert_value_for_storage(self, value: Any, field_def: Optional[CustomFieldDefinition]) -> Any:
        """Convert a value to the appropriate format for storage."""
        if value is None or value == "":
            return None
        
        if not field_def:
            return value
        
        try:
            if field_def.field_type == CustomFieldType.NUMBER:
                return float(value)
            
            elif field_def.field_type == CustomFieldType.BOOLEAN:
                if isinstance(value, bool):
                    return value
                return str(value).lower() in ['true', '1', 'yes', 'on']
            
            elif field_def.field_type == CustomFieldType.DATE:
                if isinstance(value, str):
                    # Parse various date formats
                    try:
                        return datetime.fromisoformat(value.replace('Z', '+00:00')).isoformat()
                    except:
                        # Try other formats
                        for fmt in ['%Y-%m-%d', '%m/%d/%Y', '%d/%m/%Y']:
                            try:
                                return datetime.strptime(value, fmt).isoformat()
                            except:
                                continue
                elif isinstance(value, (date, datetime)):
                    return value.isoformat()
                return value
            
            elif field_def.field_type in [CustomFieldType.RATING_5, CustomFieldType.RATING_10]:
                return float(value)
            
            elif field_def.field_type in [CustomFieldType.LIST, CustomFieldType.TAGS]:
                if isinstance(value, str):
                    # Split comma-separated values
                    return [v.strip() for v in value.split(',') if v.strip()]
                elif isinstance(value, list):
                    return value
                return [str(value)]
            
            else:
                return str(value)
                
        except Exception as e:
            print(f"Error converting value {value}: {e}")
            return value


# Global service instances
book_service = RedisBookService()
user_service = RedisUserService()
reading_log_service = RedisReadingLogService()
custom_field_service = RedisCustomFieldService()
import_mapping_service = RedisImportMappingService()
custom_metadata_service = CustomMetadataService()

# Initialize the custom metadata service after other services are created
custom_metadata_service._ensure_services()
