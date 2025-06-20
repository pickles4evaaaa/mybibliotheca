"""
Redis-only service layer for Bibliotheca.

This module provides service classes for comprehensive book management using Redis 
as the sole data store with graph database functionality.
"""

import os
import asyncio
from typing import List, Optional, Dict, Any
from datetime import datetime, date
from dataclasses import asdict
from functools import wraps

from flask import current_app
from flask_login import current_user

from .domain.models import Book, User, Author, Publisher, Series, Category, UserBookRelationship, ReadingLog, ReadingStatus
from .infrastructure.redis_repositories import RedisBookRepository, RedisUserRepository, RedisAuthorRepository, RedisUserBookRepository
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
    
    async def create_book(self, domain_book: Book, user_id: int) -> Book:
        """Create a book in Redis."""
        try:
            # Generate ID if not set
            if not domain_book.id:
                import uuid
                domain_book.id = str(uuid.uuid4())
            
            # Store book in Redis
            await self.redis_book_repo.create(domain_book)
            
            # Create user-book relationship
            relationship = UserBookRelationship(
                user_id=str(user_id),
                book_id=domain_book.id,
                reading_status=ReadingStatus.PLAN_TO_READ,
                user_rating=None,
                user_review=None,
                user_notes=None,
                reading_start_date=None,
                reading_end_date=None,
                created_at=datetime.now(),
                updated_at=datetime.now()
            )
            
            # Store relationship in Redis
            relationship_data = asdict(relationship)
            self.storage.create_relationship(
                'user', str(user_id), 'OWNS', 'book', domain_book.id,
                relationship_data
            )
            
            current_app.logger.info(f"Book {domain_book.id} successfully created in Redis")
            return domain_book
        except Exception as e:
            current_app.logger.error(f"Failed to create book in Redis: {e}")
            raise
    
    async def get_book_by_id(self, book_id: str, user_id: int) -> Optional[Book]:
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
                            book.personal_notes = rel.user_notes
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
    
    async def get_user_books(self, user_id: int) -> List[Book]:
        """Get all books for a user."""
        try:
            # Get all UserBookRelationships for this user from Redis
            relationships = await self.redis_user_book_repo.get_user_library(str(user_id))
            
            # Convert to Book objects with user-specific attributes
            books = []
            for rel in relationships:
                # Get the book data
                book = await self.redis_book_repo.get_by_id(rel.book_id)
                if book:
                    # Add user-specific attributes directly to the book object
                    book.reading_status = rel.reading_status.value
                    book.ownership_status = rel.ownership_status.value
                    book.start_date = rel.start_date
                    book.finish_date = rel.finish_date
                    book.user_rating = rel.user_rating
                    book.personal_notes = rel.user_notes
                    book.date_added = rel.date_added
                    book.user_tags = rel.user_tags
                    book.locations = rel.locations
                    books.append(book)
            
            return books
        except Exception as e:
            current_app.logger.error(f"Failed to get user books from Redis: {e}")
            return []
    
    async def get_book_by_uid(self, uid: str, user_id: int) -> Optional[Book]:
        """Get a book by UID (alias for get_book_by_id)."""
        return await self.get_book_by_id(uid, user_id)
    
    async def update_book(self, uid: str, user_id: int, **kwargs) -> Optional[Book]:
        """Update a book."""
        try:
            book = await self.redis_book_repo.get_by_id(uid)
            if not book:
                return None
            
            # Update book fields
            for field, value in kwargs.items():
                if hasattr(book, field):
                    setattr(book, field, value)
            
            # Update in Redis
            await self.redis_book_repo.update(book)
            current_app.logger.info(f"Book {uid} updated in Redis")
            return book
        except Exception as e:
            current_app.logger.error(f"Failed to update book {uid}: {e}")
            return None
    
    async def delete_book(self, uid: str, user_id: int) -> bool:
        """Delete a book."""
        try:
            # Delete from Redis
            await self.redis_book_repo.delete(uid)
            current_app.logger.info(f"Book {uid} deleted from Redis")
            return True
        except Exception as e:
            current_app.logger.error(f"Failed to delete book {uid}: {e}")
            return False
    
    async def get_books_by_isbn(self, isbn: str, user_id: int) -> List[Book]:
        """Get books by ISBN."""
        try:
            # This would need to be implemented in the Redis repository
            # For now, return empty list
            current_app.logger.warning("get_books_by_isbn not yet implemented for Redis")
            return []
        except Exception as e:
            current_app.logger.error(f"Failed to get books by ISBN {isbn}: {e}")
            return []
    
    async def search_books(self, query: str, user_id: int, filter_params: Optional[Dict] = None) -> List[Book]:
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
    
    # Sync wrappers for Flask compatibility
    @run_async
    async def create_book_sync(self, domain_book: Book, user_id: int) -> Book:
        """Sync wrapper for create_book."""
        return await self.create_book(domain_book, user_id)
    
    @run_async
    async def get_book_by_id_sync(self, book_id: str, user_id: int) -> Optional[Book]:
        """Sync wrapper for get_book_by_id."""
        return await self.get_book_by_id(book_id, user_id)
    
    @run_async
    async def get_book_by_uid_sync(self, uid: str, user_id: int) -> Optional[Book]:
        """Sync wrapper for get_book_by_uid."""
        return await self.get_book_by_uid(uid, user_id)
    
    @run_async
    async def get_user_books_sync(self, user_id: int) -> List[Book]:
        """Sync wrapper for get_user_books."""
        return await self.get_user_books(user_id)
    
    @run_async
    async def update_book_sync(self, uid: str, user_id: int, **kwargs) -> Optional[Book]:
        """Sync wrapper for update_book."""
        return await self.update_book(uid, user_id, **kwargs)
    
    @run_async
    async def delete_book_sync(self, uid: str, user_id: int) -> bool:
        """Sync wrapper for delete_book."""
        return await self.delete_book(uid, user_id)
    
    @run_async
    async def get_books_by_isbn_sync(self, isbn: str, user_id: int) -> List[Book]:
        """Sync wrapper for get_books_by_isbn."""
        return await self.get_books_by_isbn(isbn, user_id)
    
    @run_async
    async def search_books_sync(self, query: str, user_id: int, filter_params: Optional[Dict] = None) -> List[Book]:
        """Sync wrapper for search_books."""
        return await self.search_books(query, user_id, filter_params)


class RedisUserService:
    """Service for managing users using Redis as the sole data store."""
    
    def __init__(self):
        self.redis_enabled = os.getenv('GRAPH_DATABASE_ENABLED', 'true').lower() == 'true'
        
        if not self.redis_enabled:
            raise RuntimeError("Redis must be enabled for this version of Bibliotheca")
            
        self.storage = get_graph_storage()
        self.redis_user_repo = RedisUserRepository(self.storage)
    
    async def create_user(self, username: str, email: str, password_hash: str) -> User:
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
            is_admin=False,
            is_active=True,
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
            users = await self.redis_user_repo.get_all()
            return len(users)
        except Exception as e:
            current_app.logger.error(f"Failed to get user count: {e}")
            return 0
    
    async def get_admin_count(self) -> int:
        """Get total number of admin users."""
        try:
            users = await self.redis_user_repo.get_all()
            admin_count = sum(1 for user in users if user.is_admin)
            return admin_count
        except Exception as e:
            current_app.logger.error(f"Failed to get admin count: {e}")
            return 0
    
    async def get_active_user_count(self) -> int:
        """Get total number of active users."""
        try:
            users = await self.redis_user_repo.get_all()
            active_count = sum(1 for user in users if user.is_active)
            return active_count
        except Exception as e:
            current_app.logger.error(f"Failed to get active user count: {e}")
            return 0
    
    # Sync wrappers
    @run_async
    async def create_user_sync(self, username: str, email: str, password_hash: str) -> User:
        """Sync wrapper for create_user."""
        return await self.create_user(username, email, password_hash)
    
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


class RedisReadingLogService:
    """Service for managing reading logs using Redis as the sole data store."""
    
    def __init__(self):
        self.redis_enabled = os.getenv('GRAPH_DATABASE_ENABLED', 'true').lower() == 'true'
        
        if not self.redis_enabled:
            raise RuntimeError("Redis must be enabled for this version of Bibliotheca")
            
        self.storage = get_graph_storage()


# Global service instances
book_service = RedisBookService()
user_service = RedisUserService()
reading_log_service = RedisReadingLogService()
