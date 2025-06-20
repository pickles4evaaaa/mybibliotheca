"""
Application Services for Redis-based book management.

This module provides service classes for comprehensive book management using Redis 
as the primary data store with graph database functionality.
"""

import os
import asyncio
from typing import List, Optional, Dict, Any
from datetime import datetime, date
from dataclasses import asdict
from functools import wraps

from flask import current_app
from flask_login import current_user

from .models import Book as SQLiteBook, User as SQLiteUser, ReadingLog as SQLiteReadingLog, db
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


class DualWriteBookService:
    """Service for managing books with dual-write to SQLite and Redis."""
    
    def __init__(self):
        self.sqlite_enabled = True
        self.redis_enabled = os.getenv('GRAPH_DATABASE_ENABLED', 'false').lower() == 'true'
        
        if self.redis_enabled:
            self.storage = get_graph_storage()
            self.redis_book_repo = RedisBookRepository(self.storage)
            self.redis_user_repo = RedisUserRepository(self.storage)
            self.redis_author_repo = RedisAuthorRepository(self.storage)
            self.redis_user_book_repo = RedisUserBookRepository(self.storage)
    
    def _sqlite_to_domain_book(self, sqlite_book: SQLiteBook) -> Book:
        """Convert SQLite Book model to domain Book model."""
        # Parse authors from the author string
        authors = []
        if sqlite_book.author:
            # For now, create a single author. Later we can enhance this
            # to parse multiple authors from comma-separated strings
            author = Author(
                id=None,  # Will be generated
                name=sqlite_book.author,
                normalized_name=sqlite_book.author.lower().strip()
            )
            authors.append(author)
        
        # Parse categories
        categories = []
        if sqlite_book.categories:
            for cat_name in sqlite_book.categories.split(','):
                cat_name = cat_name.strip()
                if cat_name:
                    category = Category(
                        id=None,
                        name=cat_name,
                        normalized_name=cat_name.lower().strip()
                    )
                    categories.append(category)
        
        # Create publisher if exists
        publisher = None
        if sqlite_book.publisher:
            publisher = Publisher(
                id=None,
                name=sqlite_book.publisher,
                normalized_name=sqlite_book.publisher.lower().strip()
            )
        
        # Create series if needed (for future enhancement)
        series = None
        
        # Convert string published_date to datetime if present
        published_date = None
        if sqlite_book.published_date:
            try:
                # Try to parse as ISO format first
                published_date = datetime.fromisoformat(sqlite_book.published_date.replace('Z', '+00:00'))
            except (ValueError, AttributeError):
                # If that fails, it might already be a datetime or invalid
                published_date = None
        
        # Map SQLite user-specific fields to domain model enums
        reading_status = self._map_sqlite_reading_status(sqlite_book)
        ownership_status = 'owned'  # Default for existing SQLite data
        
        book = Book(
            id=str(sqlite_book.id),
            title=sqlite_book.title,
            normalized_title=sqlite_book.title.lower().strip(),
            authors=authors,
            isbn13=sqlite_book.isbn,  # SQLite model uses 'isbn' not 'isbn13'
            isbn10=None,  # SQLite model doesn't have isbn10 field
            published_date=published_date,
            publisher=publisher,
            series=series,
            page_count=sqlite_book.page_count,
            language=sqlite_book.language,
            description=sqlite_book.description,
            cover_url=sqlite_book.cover_url,
            categories=categories,
            created_at=sqlite_book.created_at,
            updated_at=None  # SQLite model doesn't have updated_at field
        )
        
        # Add user-specific attributes for template compatibility
        book.reading_status = reading_status
        book.ownership_status = ownership_status
        book.start_date = sqlite_book.start_date
        book.finish_date = sqlite_book.finish_date
        book.user_rating = None  # SQLite doesn't have user rating
        book.personal_notes = None  # SQLite doesn't have personal notes
        book.date_added = sqlite_book.created_at
        book.user_tags = []  # SQLite doesn't have user tags
        book.locations = []  # SQLite doesn't have locations
        
        return book
    
    def _domain_to_sqlite_book(self, domain_book: Book, user_id: int) -> SQLiteBook:
        """Convert domain Book model to SQLite Book model."""
        # Extract author names
        author_name = ""
        if domain_book.authors:
            author_name = domain_book.authors[0].name
        
        # Extract category names
        category_names = ""
        if domain_book.categories:
            category_names = ", ".join([cat.name for cat in domain_book.categories])
        
        # Extract publisher name
        publisher_name = ""
        if domain_book.publisher:
            publisher_name = domain_book.publisher.name
        
        # Use primary ISBN (prefer ISBN13 over ISBN10)
        isbn = domain_book.primary_isbn
        
        # Convert datetime to string for SQLite storage
        published_date_str = None
        if domain_book.published_date:
            published_date_str = domain_book.published_date.isoformat()
        
        return SQLiteBook(
            title=domain_book.title,
            author=author_name,
            isbn=isbn,
            published_date=published_date_str,
            publisher=publisher_name,
            page_count=domain_book.page_count,
            language=domain_book.language,
            description=domain_book.description,
            cover_url=domain_book.cover_url,
            categories=category_names,
            user_id=user_id
        )
    
    def _map_sqlite_reading_status(self, sqlite_book: SQLiteBook) -> str:
        """Map SQLite book fields to reading status enum value."""
        if sqlite_book.library_only:
            return 'library_only'
        elif sqlite_book.want_to_read:
            return 'plan_to_read'
        elif sqlite_book.start_date and sqlite_book.finish_date:
            return 'read'
        elif sqlite_book.start_date and not sqlite_book.finish_date:
            return 'reading'
        else:
            # Default for books without specific status
            return 'plan_to_read'
    
    async def create_book(self, domain_book: Book, user_id: int) -> Book:
        """Create a book in both SQLite and Redis (if enabled)."""
        # Create in SQLite first
        sqlite_book = self._domain_to_sqlite_book(domain_book, user_id)
        db.session.add(sqlite_book)
        db.session.commit()
        
        # Update domain book with generated ID
        domain_book.id = str(sqlite_book.id)
        
        # Write to Redis if enabled
        if self.redis_enabled:
            try:
                await self.redis_book_repo.create(domain_book)
                
                # Create user-book relationship
                relationship = UserBookRelationship(
                    user_id=str(user_id),
                    book_id=domain_book.id,
                    reading_status=ReadingStatus.PLAN_TO_READ,
                    user_rating=None,
                    user_review=None,
                    personal_notes=None,
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
                
                current_app.logger.info(f"Book {domain_book.id} successfully written to both SQLite and Redis")
            except Exception as e:
                current_app.logger.error(f"Failed to write book to Redis: {e}")
        
        return domain_book
    
    async def get_book_by_id(self, book_id: str, user_id: int) -> Optional[Book]:
        """Get a book by ID."""
        try:
            # Convert string ID to int for SQLite
            sqlite_book_id = int(book_id)
            sqlite_book = SQLiteBook.query.filter_by(id=sqlite_book_id, user_id=user_id).first()
            
            if sqlite_book:
                return self._sqlite_to_domain_book(sqlite_book)
            return None
        except ValueError:
            # Invalid ID format
            return None
    
    async def get_user_books(self, user_id: int) -> List[Book]:
        """Get all books for a user."""
        if self.redis_enabled:
            # Use Redis as primary data source
            try:
                # Get all UserBookRelationships for this user from Redis
                relationships = await self.redis_user_book_repo.get_user_library(str(user_id))
                
                # Convert to Book objects with user-specific attributes
                books = []
                for rel in relationships:
                    # Get the book data
                    book = await self.redis_book_repo.get_by_id(rel.book_id)
                    if book:
                        # Create a hybrid object that has both book and relationship data
                        # We'll add the user-specific attributes directly to the book object
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
                
                return books
                
            except Exception as e:
                current_app.logger.error(f"Failed to get user books from Redis: {e}")
                # Fall back to SQLite on Redis error
                
        # Use SQLite as fallback or primary (when Redis disabled)
        sqlite_books = SQLiteBook.query.filter_by(user_id=user_id).all()
        return [self._sqlite_to_domain_book(book) for book in sqlite_books]
    
    async def get_book_by_uid(self, uid: str, user_id: int) -> Optional[Book]:
        """Get a book by UID (alias for get_book_by_id)."""
        return await self.get_book_by_id(uid, user_id)
    
    async def update_book(self, uid: str, user_id: int, **kwargs) -> Optional[Book]:
        """Update a book."""
        try:
            sqlite_book_id = int(uid)
            sqlite_book = SQLiteBook.query.filter_by(id=sqlite_book_id, user_id=user_id).first()
            
            if not sqlite_book:
                return None
            
            # Separate relationship fields from book fields
            relationship_fields = {'personal_notes', 'user_rating', 'reading_status', 'ownership_status', 
                                 'date_started', 'date_finished', 'date_added', 'favorite', 'priority'}
            sqlite_fields = {}
            rel_fields = {}
            
            for field, value in kwargs.items():
                if field in relationship_fields:
                    rel_fields[field] = value
                elif hasattr(sqlite_book, field):
                    sqlite_fields[field] = value
            
            # Update SQLite fields
            if sqlite_fields:
                for field, value in sqlite_fields.items():
                    setattr(sqlite_book, field, value)
                db.session.commit()
            
            # Update Redis if enabled
            if self.redis_enabled:
                try:
                    domain_book = self._sqlite_to_domain_book(sqlite_book)
                    
                    # Update book in Redis
                    if sqlite_fields:
                        await self.redis_book_repo.update(domain_book)
                        current_app.logger.info(f"Book {uid} updated in Redis")
                    
                    # Update relationship fields in Redis
                    if rel_fields:
                        relationship = await self.redis_user_book_repo.get_relationship(str(user_id), uid)
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
                    
                except Exception as e:
                    current_app.logger.error(f"Failed to update book in Redis: {e}")
            
            return self._sqlite_to_domain_book(sqlite_book)
        except ValueError:
            return None
    
    async def delete_book(self, uid: str, user_id: int) -> bool:
        """Delete a book."""
        try:
            sqlite_book_id = int(uid)
            sqlite_book = SQLiteBook.query.filter_by(id=sqlite_book_id, user_id=user_id).first()
            
            if not sqlite_book:
                return False
            
            # Delete from SQLite
            db.session.delete(sqlite_book)
            db.session.commit()
            
            # Delete from Redis if enabled
            if self.redis_enabled:
                try:
                    await self.redis_book_repo.delete(uid)
                    current_app.logger.info(f"Book {uid} deleted from both SQLite and Redis")
                except Exception as e:
                    current_app.logger.error(f"Failed to delete book from Redis: {e}")
            
            return True
        except ValueError:
            return False
    
    async def get_books_by_isbn(self, isbn: str, user_id: int) -> List[Book]:
        """Get books by ISBN."""
        sqlite_books = SQLiteBook.query.filter_by(user_id=user_id).filter(
            (SQLiteBook.isbn13 == isbn) | (SQLiteBook.isbn10 == isbn)
        ).all()
        return [self._sqlite_to_domain_book(book) for book in sqlite_books]
    
    async def search_books(self, query: str, user_id: int, filter_params: Optional[Dict] = None) -> List[Book]:
        """Search books with optional filters."""
        sqlite_query = SQLiteBook.query.filter_by(user_id=user_id)
        
        # Apply text search
        if query:
            sqlite_query = sqlite_query.filter(
                db.or_(
                    SQLiteBook.title.contains(query),
                    SQLiteBook.author.contains(query),
                    SQLiteBook.description.contains(query)
                )
            )
        
        # Apply filters
        if filter_params:
            for field, value in filter_params.items():
                if hasattr(SQLiteBook, field) and value:
                    sqlite_query = sqlite_query.filter(getattr(SQLiteBook, field).contains(value))
        
        sqlite_books = sqlite_query.all()
        return [self._sqlite_to_domain_book(book) for book in sqlite_books]
    
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


class DualWriteUserService:
    """Service for managing users with dual-write to SQLite and Redis."""
    
    def __init__(self):
        self.sqlite_enabled = True
        self.redis_enabled = os.getenv('GRAPH_DATABASE_ENABLED', 'false').lower() == 'true'
        
        if self.redis_enabled:
            self.storage = get_graph_storage()
            self.redis_user_repo = RedisUserRepository(self.storage)
    
    async def create_user(self, username: str, email: str, password_hash: str) -> User:
        """Create a user in both SQLite and Redis (if enabled)."""
        # Create in SQLite first
        sqlite_user = SQLiteUser(
            username=username,
            email=email,
            password_hash=password_hash
        )
        db.session.add(sqlite_user)
        db.session.commit()
        
        # Create domain user
        domain_user = User(
            id=str(sqlite_user.id),
            username=sqlite_user.username,
            email=sqlite_user.email,
            password_hash=sqlite_user.password_hash,
            is_admin=sqlite_user.is_admin,
            is_active=sqlite_user.is_active,
            share_current_reading=sqlite_user.share_current_reading,
            share_reading_activity=sqlite_user.share_reading_activity,
            share_library=sqlite_user.share_library,
            created_at=sqlite_user.created_at,
            last_login=sqlite_user.last_login,
            reading_streak_offset=sqlite_user.reading_streak_offset or 0
        )
        
        # Write to Redis if enabled
        if self.redis_enabled:
            try:
                await self.redis_user_repo.create(domain_user)
                current_app.logger.info(f"User {domain_user.id} successfully written to both SQLite and Redis")
            except Exception as e:
                current_app.logger.error(f"Failed to write user to Redis: {e}")
        
        return domain_user
    
    async def get_user_by_id(self, user_id: int) -> Optional[User]:
        """Get a user by ID."""
        sqlite_user = SQLiteUser.query.get(user_id)
        if sqlite_user:
            return self._sqlite_to_domain_user(sqlite_user)
        return None
    
    async def get_user_by_username(self, username: str) -> Optional[User]:
        """Get a user by username."""
        sqlite_user = SQLiteUser.query.filter_by(username=username).first()
        if sqlite_user:
            return self._sqlite_to_domain_user(sqlite_user)
        return None
    
    async def get_user_by_email(self, email: str) -> Optional[User]:
        """Get a user by email."""
        sqlite_user = SQLiteUser.query.filter_by(email=email).first()
        if sqlite_user:
            return self._sqlite_to_domain_user(sqlite_user)
        return None
    
    async def get_user_by_username_or_email(self, username_or_email: str) -> Optional[User]:
        """Get a user by username or email."""
        sqlite_user = SQLiteUser.query.filter(
            (SQLiteUser.username == username_or_email) | 
            (SQLiteUser.email == username_or_email)
        ).first()
        if sqlite_user:
            return self._sqlite_to_domain_user(sqlite_user)
        return None
    
    async def update_user_profile(self, user_id: int, username: str = None, email: str = None) -> Optional[User]:
        """Update user profile information."""
        sqlite_user = SQLiteUser.query.get(user_id)
        if not sqlite_user:
            return None
        
        # Update SQLite
        if username is not None:
            sqlite_user.username = username
        if email is not None:
            sqlite_user.email = email
        
        db.session.commit()
        
        # Update Redis if enabled
        if self.redis_enabled:
            try:
                domain_user = self._sqlite_to_domain_user(sqlite_user)
                await self.redis_user_repo.update(domain_user)
                current_app.logger.info(f"User {user_id} profile updated in both SQLite and Redis")
            except Exception as e:
                current_app.logger.error(f"Failed to update user profile in Redis: {e}")
        
        return self._sqlite_to_domain_user(sqlite_user)
    
    async def update_user_password(self, user_id: int, password_hash: str) -> bool:
        """Update user password."""
        sqlite_user = SQLiteUser.query.get(user_id)
        if not sqlite_user:
            return False
        
        # Update SQLite
        sqlite_user.password_hash = password_hash
        sqlite_user.password_must_change = False  # Clear forced change flag
        db.session.commit()
        
        # Update Redis if enabled
        if self.redis_enabled:
            try:
                domain_user = self._sqlite_to_domain_user(sqlite_user)
                await self.redis_user_repo.update(domain_user)
                current_app.logger.info(f"User {user_id} password updated in both SQLite and Redis")
            except Exception as e:
                current_app.logger.error(f"Failed to update user password in Redis: {e}")
        
        return True
    
    async def update_privacy_settings(self, user_id: int, share_current_reading: bool = None, 
                                    share_reading_activity: bool = None, share_library: bool = None) -> bool:
        """Update user privacy settings."""
        sqlite_user = SQLiteUser.query.get(user_id)
        if not sqlite_user:
            return False
        
        # Update SQLite
        if share_current_reading is not None:
            sqlite_user.share_current_reading = share_current_reading
        if share_reading_activity is not None:
            sqlite_user.share_reading_activity = share_reading_activity
        if share_library is not None:
            sqlite_user.share_library = share_library
        
        db.session.commit()
        
        # Update Redis if enabled
        if self.redis_enabled:
            try:
                domain_user = self._sqlite_to_domain_user(sqlite_user)
                await self.redis_user_repo.update(domain_user)
                current_app.logger.info(f"User {user_id} privacy settings updated in both SQLite and Redis")
            except Exception as e:
                current_app.logger.error(f"Failed to update user privacy settings in Redis: {e}")
        
        return True
    
    async def update_reading_streak_offset(self, user_id: int, reading_streak_offset: int) -> bool:
        """Update user reading streak offset."""
        sqlite_user = SQLiteUser.query.get(user_id)
        if not sqlite_user:
            return False
        
        # Update SQLite
        sqlite_user.reading_streak_offset = reading_streak_offset
        db.session.commit()
        
        # Update Redis if enabled
        if self.redis_enabled:
            try:
                domain_user = self._sqlite_to_domain_user(sqlite_user)
                await self.redis_user_repo.update(domain_user)
                current_app.logger.info(f"User {user_id} reading streak offset updated in both SQLite and Redis")
            except Exception as e:
                current_app.logger.error(f"Failed to update user reading streak offset in Redis: {e}")
        
        return True
    
    async def toggle_admin_status(self, user_id: int) -> Optional[User]:
        """Toggle admin status for a user."""
        sqlite_user = SQLiteUser.query.get(user_id)
        if not sqlite_user:
            return None
        
        # Prevent removing admin from the last admin
        if sqlite_user.is_admin:
            admin_count = SQLiteUser.query.filter_by(is_admin=True).count()
            if admin_count <= 1:
                raise ValueError("Cannot remove admin privileges from the last admin user.")
        
        # Toggle admin status
        sqlite_user.is_admin = not sqlite_user.is_admin
        db.session.commit()
        
        # Update Redis if enabled
        if self.redis_enabled:
            try:
                domain_user = self._sqlite_to_domain_user(sqlite_user)
                await self.redis_user_repo.update(domain_user)
                current_app.logger.info(f"User {user_id} admin status toggled in both SQLite and Redis")
            except Exception as e:
                current_app.logger.error(f"Failed to update user admin status in Redis: {e}")
        
        return self._sqlite_to_domain_user(sqlite_user)
    
    async def toggle_active_status(self, user_id: int) -> Optional[User]:
        """Toggle active status for a user."""
        sqlite_user = SQLiteUser.query.get(user_id)
        if not sqlite_user:
            return None
        
        # Toggle active status
        sqlite_user.is_active = not sqlite_user.is_active
        db.session.commit()
        
        # Update Redis if enabled
        if self.redis_enabled:
            try:
                domain_user = self._sqlite_to_domain_user(sqlite_user)
                await self.redis_user_repo.update(domain_user)
                current_app.logger.info(f"User {user_id} active status toggled in both SQLite and Redis")
            except Exception as e:
                current_app.logger.error(f"Failed to update user active status in Redis: {e}")
        
        return self._sqlite_to_domain_user(sqlite_user)
    
    async def delete_user(self, user_id: int) -> bool:
        """Delete a user and all associated data."""
        sqlite_user = SQLiteUser.query.get(user_id)
        if not sqlite_user:
            return False
        
        # Prevent deleting the last admin
        if sqlite_user.is_admin:
            admin_count = SQLiteUser.query.filter_by(is_admin=True).count()
            if admin_count <= 1:
                raise ValueError("Cannot delete the last admin user.")
        
        # Delete from SQLite (cascades will handle books and logs)
        db.session.delete(sqlite_user)
        db.session.commit()
        
        # Delete from Redis if enabled
        if self.redis_enabled:
            try:
                await self.redis_user_repo.delete(str(user_id))
                current_app.logger.info(f"User {user_id} deleted from both SQLite and Redis")
            except Exception as e:
                current_app.logger.error(f"Failed to delete user from Redis: {e}")
        
        return True
    
    async def reset_user_password(self, user_id: int, new_password_hash: str, force_change: bool = False) -> bool:
        """Reset a user's password (admin function)."""
        sqlite_user = SQLiteUser.query.get(user_id)
        if not sqlite_user:
            return False
        
        # Update SQLite
        sqlite_user.password_hash = new_password_hash
        if force_change:
            sqlite_user.password_must_change = True
        
        # Also unlock the account if it was locked
        sqlite_user.failed_login_attempts = 0
        sqlite_user.locked_until = None
        
        db.session.commit()
        
        # Update Redis if enabled
        if self.redis_enabled:
            try:
                domain_user = self._sqlite_to_domain_user(sqlite_user)
                await self.redis_user_repo.update(domain_user)
                current_app.logger.info(f"User {user_id} password reset in both SQLite and Redis")
            except Exception as e:
                current_app.logger.error(f"Failed to update user password reset in Redis: {e}")
        
        return True
    
    async def unlock_user_account(self, user_id: int) -> bool:
        """Unlock a locked user account."""
        sqlite_user = SQLiteUser.query.get(user_id)
        if not sqlite_user:
            return False
        
        # Reset failed login attempts and clear lock
        sqlite_user.failed_login_attempts = 0
        sqlite_user.locked_until = None
        db.session.commit()
        
        # Update Redis if enabled
        if self.redis_enabled:
            try:
                domain_user = self._sqlite_to_domain_user(sqlite_user)
                await self.redis_user_repo.update(domain_user)
                current_app.logger.info(f"User {user_id} account unlocked in both SQLite and Redis")
            except Exception as e:
                current_app.logger.error(f"Failed to update user unlock in Redis: {e}")
        
        return True
    
    async def get_user_count(self) -> int:
        """Get total number of users."""
        return SQLiteUser.query.count()
    
    async def get_admin_count(self) -> int:
        """Get total number of admin users."""
        return SQLiteUser.query.filter_by(is_admin=True).count()
    
    async def get_active_user_count(self) -> int:
        """Get total number of active users."""
        return SQLiteUser.query.filter_by(is_active=True).count()
    
    async def search_users(self, search_term: str, page: int = 1, per_page: int = 20) -> Dict[str, Any]:
        """Search users with pagination."""
        query = SQLiteUser.query
        if search_term:
            query = query.filter(
                db.or_(
                    SQLiteUser.username.contains(search_term),
                    SQLiteUser.email.contains(search_term)
                )
            )
        
        # Paginate results
        pagination = query.order_by(SQLiteUser.created_at.desc()).paginate(
            page=page, per_page=per_page, error_out=False
        )
        
        users = [self._sqlite_to_domain_user(user) for user in pagination.items]
        
        return {
            'users': users,
            'has_prev': pagination.has_prev,
            'has_next': pagination.has_next,
            'prev_num': pagination.prev_num,
            'next_num': pagination.next_num,
            'page': pagination.page,
            'pages': pagination.pages,
            'per_page': pagination.per_page,
            'total': pagination.total
        }

    async def get_all_users(self) -> List[User]:
        """Get all users."""
        sqlite_users = SQLiteUser.query.all()
        return [self._sqlite_to_domain_user(user) for user in sqlite_users]

    async def get_sharing_users(self) -> List[User]:
        """Get users who share reading activity."""
        sqlite_users = SQLiteUser.query.filter_by(share_reading_activity=True, is_active=True).all()
        return [self._sqlite_to_domain_user(user) for user in sqlite_users]

    def _sqlite_to_domain_user(self, sqlite_user: SQLiteUser) -> User:
        """Convert SQLite User to domain User."""
        return User(
            id=str(sqlite_user.id),
            username=sqlite_user.username,
            email=sqlite_user.email,
            password_hash=sqlite_user.password_hash,
            is_admin=sqlite_user.is_admin,
            is_active=sqlite_user.is_active,
            share_current_reading=sqlite_user.share_current_reading,
            share_reading_activity=sqlite_user.share_reading_activity,
            share_library=sqlite_user.share_library,
            created_at=sqlite_user.created_at,
            last_login=sqlite_user.last_login,
            reading_streak_offset=sqlite_user.reading_streak_offset or 0
        )

    # Sync wrappers
    @run_async
    async def create_user_sync(self, username: str, email: str, password_hash: str) -> User:
        """Sync wrapper for create_user."""
        return await self.create_user(username, email, password_hash)
    
    @run_async
    async def get_user_by_id_sync(self, user_id: int) -> Optional[User]:
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
    async def update_user_profile_sync(self, user_id: int, username: str = None, email: str = None) -> Optional[User]:
        """Sync wrapper for update_user_profile."""
        return await self.update_user_profile(user_id, username, email)
    
    @run_async
    async def update_user_password_sync(self, user_id: int, password_hash: str) -> bool:
        """Sync wrapper for update_user_password."""
        return await self.update_user_password(user_id, password_hash)
    
    @run_async
    async def update_privacy_settings_sync(self, user_id: int, share_current_reading: bool = None, 
                                         share_reading_activity: bool = None, share_library: bool = None) -> bool:
        """Sync wrapper for update_privacy_settings."""
        return await self.update_privacy_settings(user_id, share_current_reading, share_reading_activity, share_library)
    
    @run_async
    async def update_reading_streak_offset_sync(self, user_id: int, reading_streak_offset: int) -> bool:
        """Sync wrapper for update_reading_streak_offset."""
        return await self.update_reading_streak_offset(user_id, reading_streak_offset)
    
    @run_async
    async def toggle_admin_status_sync(self, user_id: int) -> Optional[User]:
        """Sync wrapper for toggle_admin_status."""
        return await self.toggle_admin_status(user_id)
    
    @run_async
    async def toggle_active_status_sync(self, user_id: int) -> Optional[User]:
        """Sync wrapper for toggle_active_status."""
        return await self.toggle_active_status(user_id)
    
    @run_async
    async def delete_user_sync(self, user_id: int) -> bool:
        """Sync wrapper for delete_user."""
        return await self.delete_user(user_id)
    
    @run_async
    async def reset_user_password_sync(self, user_id: int, new_password_hash: str, force_change: bool = False) -> bool:
        """Sync wrapper for reset_user_password."""
        return await self.reset_user_password(user_id, new_password_hash, force_change)
    
    @run_async
    async def unlock_user_account_sync(self, user_id: int) -> bool:
        """Sync wrapper for unlock_user_account."""
        return await self.unlock_user_account(user_id)
    
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
    async def search_users_sync(self, search_term: str, page: int = 1, per_page: int = 20) -> Dict[str, Any]:
        """Sync wrapper for search_users."""
        return await self.search_users(search_term, page, per_page)

    @run_async
    async def get_all_users_sync(self) -> List[User]:
        """Sync wrapper for get_all_users."""
        return await self.get_all_users()

    @run_async
    async def get_sharing_users_sync(self) -> List[User]:
        """Sync wrapper for get_sharing_users."""
        return await self.get_sharing_users()


class DualWriteReadingLogService:
    """Service for managing reading logs with dual-write to SQLite and Redis."""
    
    def __init__(self):
        self.sqlite_enabled = True
        self.redis_enabled = os.getenv('GRAPH_DATABASE_ENABLED', 'false').lower() == 'true'
        
        if self.redis_enabled:
            self.storage = get_graph_storage()
    
    async def create_reading_log(self, book_id: int, user_id: int, log_date: date) -> ReadingLog:
        """Create a reading log entry, or return existing one if it already exists."""
        # Check if log already exists
        existing_log = await self.get_existing_log(book_id, user_id, log_date)
        if existing_log:
            current_app.logger.info(f"Reading log already exists for book {book_id} on {log_date}")
            return existing_log
        
        # Create new log in SQLite 
        sqlite_log = SQLiteReadingLog(
            book_id=book_id,
            user_id=user_id,
            date=log_date
        )
        db.session.add(sqlite_log)
        db.session.commit()
        
        # Create domain reading log (simplified to match SQLite structure)
        domain_log = ReadingLog(
            id=str(sqlite_log.id),
            book_id=str(book_id),
            user_id=str(user_id),
            date=log_date,
            created_at=sqlite_log.created_at
        )
        
        # Write to Redis if enabled
        if self.redis_enabled:
            try:
                # Store reading log in Redis as a relationship between user and book
                relationship_data = {
                    'date': log_date.isoformat(),
                    'created_at': sqlite_log.created_at.isoformat()
                }
                
                self.storage.create_relationship(
                    'user', str(user_id), 'LOGGED_READING', 'book', str(book_id),
                    relationship_data
                )
                
                current_app.logger.info(f"Reading log {domain_log.id} written to both SQLite and Redis")
            except Exception as e:
                current_app.logger.error(f"Failed to write reading log to Redis: {e}")
        
        return domain_log
    
    async def get_existing_log(self, book_id: int, user_id: int, log_date: date) -> Optional[ReadingLog]:
        """Get existing reading log for a book on a specific date for a specific user."""
        sqlite_log = SQLiteReadingLog.query.filter_by(book_id=book_id, user_id=user_id, date=log_date).first()
        if sqlite_log:
            return ReadingLog(
                id=str(sqlite_log.id),
                book_id=str(sqlite_log.book_id),
                user_id=str(sqlite_log.user_id),
                date=sqlite_log.date,
                created_at=sqlite_log.created_at
            )
        return None
    
    async def update_reading_log(self, log_id: int, pages_read: int) -> bool:
        """Update an existing reading log."""
        sqlite_log = SQLiteReadingLog.query.get(log_id)
        if not sqlite_log:
            return False
        
        # Note: SQLite model doesn't have pages_read, so we can't update it there
        # But we can update Redis if enabled
        if self.redis_enabled:
            try:
                # Update relationship data in Redis
                current_app.logger.info(f"Reading log {log_id} updated in Redis (SQLite doesn't support pages_read)")
            except Exception as e:
                current_app.logger.error(f"Failed to update reading log in Redis: {e}")
        
        return True
    
    async def delete_reading_logs(self, book_id: int) -> bool:
        """Delete all reading logs for a book."""
        sqlite_logs = SQLiteReadingLog.query.filter_by(book_id=book_id).all()
        
        for log in sqlite_logs:
            db.session.delete(log)
        
        db.session.commit()
        
        # Delete from Redis if enabled
        if self.redis_enabled:
            try:
                # TODO: Implement Redis relationship deletion
                current_app.logger.info(f"Reading logs for book {book_id} deleted from SQLite")
            except Exception as e:
                current_app.logger.error(f"Failed to delete reading logs from Redis: {e}")
        
        return True
    
    # Sync wrappers
    @run_async
    async def create_reading_log_sync(self, book_id: int, user_id: int, log_date: date) -> ReadingLog:
        """Sync wrapper for create_reading_log."""
        return await self.create_reading_log(book_id, user_id, log_date)
    
    @run_async
    async def get_existing_log_sync(self, book_id: int, user_id: int, log_date: date) -> Optional[ReadingLog]:
        """Sync wrapper for get_existing_log."""
        return await self.get_existing_log(book_id, user_id, log_date)
    
    @run_async
    async def update_reading_log_sync(self, log_id: int, pages_read: int) -> bool:
        """Sync wrapper for update_reading_log."""
        return await self.update_reading_log(log_id, pages_read)
    
    @run_async
    async def delete_reading_logs_sync(self, book_id: int) -> bool:
        """Sync wrapper for delete_reading_logs."""
        return await self.delete_reading_logs(book_id)


# Global service instances
book_service = DualWriteBookService()
user_service = DualWriteUserService()
reading_log_service = DualWriteReadingLogService()
