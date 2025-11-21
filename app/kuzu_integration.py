"""
Integration service to connect clean Kuzu architecture with existing Flask app.
This service provides a bridge between the new clean graph implementation
and the existing application routes and services.
"""

import logging
import os
import sys
import uuid
from datetime import datetime, timezone
from typing import Optional, List, Dict, Any

# Add infrastructure path for direct imports
current_dir = os.path.dirname(os.path.abspath(__file__))
infrastructure_path = os.path.join(current_dir, 'infrastructure')
sys.path.insert(0, infrastructure_path)

try:
    from app.infrastructure.kuzu_graph import KuzuGraphDB
except Exception:
    from .infrastructure.kuzu_graph import KuzuGraphDB

try:
    from app.infrastructure.kuzu_repositories import (
        KuzuUserRepository,
        KuzuBookRepository, 
        KuzuUserBookRepository,
        KuzuLocationRepository
    )
except ImportError:
    from .infrastructure.kuzu_repositories import (
        KuzuUserRepository,
        KuzuBookRepository, 
        KuzuUserBookRepository,
        KuzuLocationRepository
    )

# Try to import domain models, fall back to basic classes if not available
try:
    from app.domain.models import User, Book, Person, Category, Location, ReadingStatus, OwnershipStatus, MediaType  # type: ignore[assignment]
    # If import succeeds, we don't need fallback classes
    DOMAIN_MODELS_AVAILABLE = True
except ImportError:
    DOMAIN_MODELS_AVAILABLE = False
    # Basic classes for when domain models aren't available
    class ReadingStatus:
        PLAN_TO_READ = "plan_to_read"
        CURRENTLY_READING = "currently_reading"
        COMPLETED = "completed"
        ON_HOLD = "on_hold"
        DROPPED = "dropped"
        
        def __init__(self, value):
            self.value = value
    
    class OwnershipStatus:
        OWNED = "owned"
        WISHLIST = "wishlist"
        BORROWED = "borrowed"
        LOANED = "loaned"
        
        def __init__(self, value):
            self.value = value
    
    class MediaType:
        PHYSICAL = "physical"
        EBOOK = "ebook"
        AUDIOBOOK = "audiobook"
        
        def __init__(self, value):
            self.value = value

logger = logging.getLogger(__name__)


class KuzuIntegrationService:
    """Service that integrates clean Kuzu architecture with existing app."""
    
    def __init__(self):
        """Initialize the integration service."""
        self.db: Optional[KuzuGraphDB] = None
        self.user_repo: Optional[KuzuUserRepository] = None
        self.book_repo: Optional[KuzuBookRepository] = None
        self.user_book_repo: Optional[KuzuUserBookRepository] = None
        self.location_repo: Optional[KuzuLocationRepository] = None
        self._initialized = False
    
    def initialize(self):
        """Initialize the database connection and repositories."""
        try:
            if self._initialized:
                return True
            
            # Repositories handle their own safe connections (avoid direct KuzuGraphDB connect here)
            self.user_repo = KuzuUserRepository()
            self.book_repo = KuzuBookRepository()
            self.user_book_repo = KuzuUserBookRepository()
            self.location_repo = KuzuLocationRepository()
            
            self._initialized = True
            logger.info("✅ Kuzu integration service initialized successfully")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to initialize Kuzu integration service: {e}")
            return False
    
    def _ensure_initialized(self) -> bool:
        """Ensure the service is initialized and all repositories are available."""
        if not self._initialized and not self.initialize():
            return False
        
        if not all([self.user_repo, self.book_repo, self.user_book_repo, self.location_repo]):
            logger.error("One or more repositories are not initialized")
            return False
        
        return True
    
    def _get_user_repo(self) -> KuzuUserRepository:
        """Get user repository with type assertion."""
        assert self.user_repo is not None, "User repository must be initialized"
        return self.user_repo
    
    def _get_book_repo(self) -> KuzuBookRepository:
        """Get book repository with type assertion."""
        assert self.book_repo is not None, "Book repository must be initialized"
        return self.book_repo
    
    def _get_user_book_repo(self) -> KuzuUserBookRepository:
        """Get user book repository with type assertion."""
        assert self.user_book_repo is not None, "User book repository must be initialized"
        return self.user_book_repo
    
    def _get_location_repo(self) -> KuzuLocationRepository:
        """Get location repository with type assertion."""
        assert self.location_repo is not None, "Location repository must be initialized"
        return self.location_repo
    
    # ========================================
    # User Management
    # ========================================
    
    async def create_user(self, user_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Create a new user."""
        if not self._ensure_initialized():
            return None
        
        try:
            from app.domain.models import User
            
            user = User(
                id=user_data.get('id') or str(uuid.uuid4()),
                username=user_data.get('username', ''),
                email=user_data.get('email', ''),
                password_hash=user_data.get('password_hash', ''),
                display_name=user_data.get('display_name'),
                bio=user_data.get('bio'),
                timezone=user_data.get('timezone', 'UTC'),
                is_admin=user_data.get('is_admin', False),
                is_active=user_data.get('is_active', True),
                password_must_change=user_data.get('password_must_change', False)
            )
            
            user_repo = self._get_user_repo()
            created_user = await user_repo.create(user)
            if created_user:
                return self._user_to_dict(created_user)
            return None
            
        except Exception as e:
            logger.error(f"Failed to create user: {e}")
            return None
    
    async def get_user(self, user_id: str) -> Optional[Dict[str, Any]]:
        """Get user by ID."""
        if not self._ensure_initialized():
            return None
        
        try:
            user_repo = self._get_user_repo()
            user = await user_repo.get_by_id(user_id)
            if user:
                return self._user_to_dict(user)
            return None
            
        except Exception as e:
            logger.error(f"Failed to get user {user_id}: {e}")
            return None
    
    async def get_user_by_username(self, username: str) -> Optional[Dict[str, Any]]:
        """Get user by username."""
        if not self._ensure_initialized():
            return None
        
        try:
            user_repo = self._get_user_repo()
            user = await user_repo.get_by_username(username)
            if user:
                user_dict = self._user_to_dict(user)
                return user_dict
            return None
            
        except Exception as e:
            logger.error(f"Failed to get user by username {username}: {e}")
            return None
    
    async def get_user_by_email(self, email: str) -> Optional[Dict[str, Any]]:
        """Get user by email."""
        if not self._ensure_initialized():
            return None
        
        try:
            user_repo = self._get_user_repo()
            user = await user_repo.get_by_email(email)
            if user:
                return self._user_to_dict(user)
            return None
            
        except Exception as e:
            logger.error(f"Failed to get user by email {email}: {e}")
            return None
    
    async def get_user_by_username_or_email(self, username_or_email: str) -> Optional[Dict[str, Any]]:
        """Get user by username or email."""
        if not self._ensure_initialized():
            return None
        
        try:
            user_repo = self._get_user_repo()
            # First try to get by username
            user = await user_repo.get_by_username(username_or_email)
            if user:
                return self._user_to_dict(user)
            
            # If not found by username, try by email
            user = await user_repo.get_by_email(username_or_email)
            if user:
                return self._user_to_dict(user)
            
            return None
            
        except Exception as e:
            logger.error(f"Failed to get user by username or email {username_or_email}: {e}")
            return None
    
    def _user_to_dict(self, user) -> Dict[str, Any]:
        """Convert user object to dictionary."""
        # Handle both dict and object inputs
        if isinstance(user, dict):
            return {
                'id': user.get('id'),
                'username': user.get('username'),
                'email': user.get('email'),
                'password_hash': user.get('password_hash'),
                'display_name': user.get('display_name'),
                'bio': user.get('bio'),
                'timezone': user.get('timezone'),
                'is_admin': user.get('is_admin'),
                'is_active': user.get('is_active'),
                'created_at': user.get('created_at')
            }
        else:
            return {
                'id': getattr(user, 'id', None),
                'username': getattr(user, 'username', None),
                'email': getattr(user, 'email', None),
                'password_hash': getattr(user, 'password_hash', None),
                'display_name': getattr(user, 'display_name', None),
                'bio': getattr(user, 'bio', None),
                'timezone': getattr(user, 'timezone', None),
                'is_admin': getattr(user, 'is_admin', None),
                'is_active': getattr(user, 'is_active', None),
                'created_at': getattr(user, 'created_at', None)
            }
    
    async def get_user_count(self) -> int:
        """Get total number of users.

        Reverts to the original reliable enumeration approach because direct COUNT queries
        proved unreliable in some environments (returned 0 spuriously, triggering onboarding).
        We still try a COUNT first (fast path) but always validate by enumerating if result is 0.
        """
        if not self._ensure_initialized():
            return 0
        try:
            user_repo = self._get_user_repo()
            # Fast path COUNT
            count_val: Optional[int] = None
            try:
                safe_manager = user_repo.safe_manager  # type: ignore[attr-defined]
                raw = safe_manager.execute_query("MATCH (u:User) RETURN COUNT(u) AS count")
                if raw and hasattr(raw, 'has_next') and raw.has_next():  # type: ignore[attr-defined]
                    row = raw.get_next()  # type: ignore[attr-defined]
                    if isinstance(row, (list, tuple)) and row:
                        count_val = int(row[0])
                    elif isinstance(row, dict):
                        count_val = int(row.get('count') or row.get('col_0') or 0)
                elif isinstance(raw, list) and raw:
                    first = raw[0]
                    if isinstance(first, dict):
                        count_val = int(first.get('count') or first.get('col_0') or 0)
                    elif isinstance(first, (list, tuple)) and first:
                        count_val = int(first[0])
            except Exception as fast_e:
                logger.debug(f"Fast COUNT path failed, will enumerate: {fast_e}")
                count_val = None

            # If fast path gave a positive number, trust it
            if count_val and count_val > 0:
                return count_val

            # Enumerate (fallback or validation if zero)
            users = await user_repo.get_all(limit=10000, offset=0)
            return len(users)
        except Exception as e:
            logger.error(f"Failed to get user count: {e}")
            return 0

    async def update_user(self, user_id: str, user_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Update an existing user."""
        if not self._ensure_initialized():
            return None
        
        try:
            user_repo = self._get_user_repo()
            
            # Call repository update with the provided data
            updated_user_raw = await user_repo.update(user_id, user_data)
            if not updated_user_raw:
                logger.error(f"Failed to update user {user_id}")
                return None
            
            # Convert to dict format
            if isinstance(updated_user_raw, dict):
                return dict(updated_user_raw)
            else:
                return self._user_to_dict(updated_user_raw)
            
        except Exception as e:
            logger.error(f"Failed to update user {user_id}: {e}")
            return None
    
    async def get_all_users(self, limit: int = 1000) -> List[Dict[str, Any]]:
        """Get all users."""
        if not self._ensure_initialized():
            return []
        
        try:
            user_repo = self._get_user_repo()
            users = await user_repo.get_all(limit=limit)
            return [self._user_to_dict(user) for user in users]
        except Exception as e:
            logger.error(f"Failed to get all users: {e}")
            return []
    
    # ========================================
    # Book Management
    # ========================================
    
    async def create_book(self, book_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Create a new book with authors and categories."""
        if not self._ensure_initialized():
            return None
        
        try:
            from app.domain.models import Book
            
            book = Book(
                id=book_data.get('id') or str(uuid.uuid4()),
                title=book_data.get('title', ''),
                isbn13=book_data.get('isbn13'),
                isbn10=book_data.get('isbn10'),
                description=book_data.get('description'),
                published_date=book_data.get('published_date'),
                page_count=book_data.get('page_count'),
                language=book_data.get('language', 'en'),
                cover_url=book_data.get('cover_url'),
                average_rating=book_data.get('average_rating'),
                rating_count=book_data.get('rating_count')
            )
            
            book_repo = self._get_book_repo()
            created_book = await book_repo.create(book)
            if created_book:
                # Handle both object and dictionary returns
                if isinstance(created_book, dict):
                    return await self._book_to_dict_from_data(created_book)
                else:
                    return await self._book_to_dict(created_book)
            return None
            
        except Exception as e:
            logger.error(f"Failed to create book: {e}")
            return None
    
    async def create_book_with_relationships(self, book: 'Book') -> Optional[Dict[str, Any]]:
        """Create a new book with full domain model including relationships."""
        if not self._ensure_initialized():
            return None
        
        try:
            # Ensure book has required attributes
            if not hasattr(book, 'id') or not book.id:
                book.id = str(uuid.uuid4())
            
            # Set timestamps if missing
            if not hasattr(book, 'created_at') or not book.created_at:
                book.created_at = datetime.now(timezone.utc)
            if not hasattr(book, 'updated_at') or not book.updated_at:
                book.updated_at = datetime.now(timezone.utc)
            
            # Use the clean repository which handles relationships
            book_repo = self._get_book_repo()
            created_book = await book_repo.create(book)
            if created_book:
                # Handle both object and dictionary returns
                if isinstance(created_book, dict):
                    return await self._book_to_dict_from_data(created_book)
                else:
                    return await self._book_to_dict(created_book)
            return None
            
        except Exception as e:
            logger.error(f"Failed to create book with relationships: {e}")
            return None
    
    async def find_book_by_isbn(self, isbn: str) -> Optional[Dict[str, Any]]:
        """Find a book by ISBN."""
        if not self._ensure_initialized():
            return None
        
        try:
            book_repo = self._get_book_repo()
            # Try both ISBN13 and ISBN10 fields
            book = await book_repo.get_by_isbn(isbn)
            if book:
                # Handle both dict and object inputs
                if isinstance(book, dict):
                    return book
                else:
                    return {
                        'id': getattr(book, 'id', None),
                        'title': getattr(book, 'title', None),
                        'isbn13': getattr(book, 'isbn13', None),
                        'isbn10': getattr(book, 'isbn10', None),
                        'description': getattr(book, 'description', None),
                        'published_date': getattr(book, 'published_date', None),
                        'page_count': getattr(book, 'page_count', None),
                        'language': getattr(book, 'language', 'en'),
                        'cover_url': getattr(book, 'cover_url', None),
                        'average_rating': getattr(book, 'average_rating', None),
                        'rating_count': getattr(book, 'rating_count', None),
                        'created_at': getattr(book, 'created_at', None)
                    }
            return None
            
        except Exception as e:
            logger.error(f"Failed to find book by ISBN {isbn}: {e}")
            return None
    
    async def get_book(self, book_id: str) -> Optional[Dict[str, Any]]:
        """Get book by ID with full details."""
        if not self._ensure_initialized():
            return None
        
        try:
            book_repo = self._get_book_repo()
            book_data = await book_repo.get_by_id(book_id)
            if book_data:
                # Since CleanKuzuBookRepository returns a dictionary, use _book_to_dict_from_data
                return await self._book_to_dict_from_data(book_data)
            return None
            
        except Exception as e:
            logger.error(f"Failed to get book {book_id}: {e}")
            return None
    
    async def search_books(self, query: str, limit: int = 20) -> List[Dict[str, Any]]:
        """Search books by title or author."""
        if not self._ensure_initialized():
            return []
        
        try:
            book_repo = self._get_book_repo()
            books = await book_repo.search(query, limit)
            return [await self._book_to_dict_from_data(book) for book in books]
            
        except Exception as e:
            logger.error(f"Failed to search books: {e}")
            return []
    
    async def _book_to_dict(self, book) -> Dict[str, Any]:
        """Convert book object to dictionary with full details."""
        book_dict = {
            'id': book.id,
            'title': book.title,
            'isbn13': book.isbn13,
            'isbn10': book.isbn10,
            'description': book.description,
            'published_date': book.published_date,
            'page_count': book.page_count,
            'language': book.language,
            'cover_url': book.cover_url,
            'average_rating': book.average_rating,
            'rating_count': book.rating_count,
            'created_at': book.created_at
        }
        
        # Get authors (placeholder - method not implemented yet)
        try:
            if self._ensure_initialized():
                book_repo = self._get_book_repo()
                authors = await book_repo.get_book_authors(book.id) if hasattr(book_repo, 'get_book_authors') else []
            else:
                authors = []
        except:
            authors = []
            
        book_dict['authors'] = [
            {
                'id': getattr(author, 'id', ''),
                'name': getattr(author, 'name', ''),
                'role': getattr(author, 'role', 'author'),
                'order_index': getattr(author, 'order_index', 0)
            }
            for author in authors
        ]
        
        # Get categories (placeholder - method not implemented yet)
        try:
            if self._ensure_initialized():
                book_repo = self._get_book_repo()
                categories = await book_repo.get_book_categories(book.id) if hasattr(book_repo, 'get_book_categories') else []
            else:
                categories = []
        except:
            categories = []
            
        book_dict['categories'] = [
            {
                'id': getattr(cat, 'id', ''),
                'name': getattr(cat, 'name', ''),
                'normalized_name': getattr(cat, 'normalized_name', '')
            }
            for cat in categories
        ]
        
        return book_dict
    
    async def _book_to_dict_from_data(self, book_data: Dict[str, Any]) -> Dict[str, Any]:
        """Convert book data dictionary to standardized dictionary format."""
        book_dict = {
            'id': book_data.get('id'),
            'uid': book_data.get('id'),  # uid is an alias for id for backward compatibility
            'title': book_data.get('title'),
            'isbn13': book_data.get('isbn13'),
            'isbn10': book_data.get('isbn10'),
            'description': book_data.get('description'),
            'published_date': book_data.get('published_date'),
            'page_count': book_data.get('page_count'),
            'language': book_data.get('language'),
            'cover_url': book_data.get('cover_url'),
            'average_rating': book_data.get('average_rating'),
            'rating_count': book_data.get('rating_count'),
            'created_at': book_data.get('created_at')
        }
        
        # Get authors (placeholder - method not implemented yet)
        try:
            book_id = book_data.get('id')
            if book_id and self._ensure_initialized():
                book_repo = self._get_book_repo()
                authors = await book_repo.get_book_authors(book_id) if hasattr(book_repo, 'get_book_authors') else []
            else:
                authors = []
        except:
            authors = []
            
        book_dict['authors'] = [
            {
                'id': author.get('id') if isinstance(author, dict) else getattr(author, 'id', ''),
                'name': author.get('name') if isinstance(author, dict) else getattr(author, 'name', ''),
                'role': author.get('role') if isinstance(author, dict) else getattr(author, 'role', 'author'),
                'order_index': author.get('order_index') if isinstance(author, dict) else getattr(author, 'order_index', 0)
            }
            for author in authors
        ]
        
        # Get categories (placeholder - method not implemented yet)
        try:
            book_id = book_data.get('id')
            if book_id and self._ensure_initialized():
                book_repo = self._get_book_repo()
                categories = await book_repo.get_book_categories(book_id) if hasattr(book_repo, 'get_book_categories') else []
            else:
                categories = []
        except:
            categories = []
            
        book_dict['categories'] = [
            {
                'id': cat.get('id') if isinstance(cat, dict) else getattr(cat, 'id', ''),
                'name': cat.get('name') if isinstance(cat, dict) else getattr(cat, 'name', ''),
                'normalized_name': cat.get('normalized_name') if isinstance(cat, dict) else getattr(cat, 'normalized_name', '')
            }
            for cat in categories
        ]
        
        return book_dict
    
    # ========================================
    # User-Book Relationships
    # ========================================
    
    async def add_book_to_library(self, user_id: str, book_id: str, 
                                 ownership_data: Dict[str, Any]) -> bool:
        """Add a book to user's library."""
        if not self._ensure_initialized():
            return False
        
        try:
            from app.domain.models import ReadingStatus, OwnershipStatus, MediaType
            
            # Ensure date_added is a proper datetime object
            date_added_value = ownership_data.get('date_added')
            if date_added_value is None:
                final_date_added = datetime.now(timezone.utc)
            elif isinstance(date_added_value, datetime):
                final_date_added = date_added_value
            elif isinstance(date_added_value, str):
                try:
                    # Try to parse ISO format string
                    final_date_added = datetime.fromisoformat(date_added_value.replace('Z', '+00:00'))
                except ValueError:
                    final_date_added = datetime.now(timezone.utc)
            elif isinstance(date_added_value, (int, float)):
                try:
                    # Try to parse as timestamp
                    final_date_added = datetime.fromtimestamp(date_added_value)
                except (ValueError, OSError):
                    final_date_added = datetime.now(timezone.utc)
            else:
                # Fallback to current time for any other type
                final_date_added = datetime.now(timezone.utc)
            
            # Create ownership relationship
            user_book_repo = self._get_user_book_repo()
            success = await user_book_repo.create_ownership(
                user_id=user_id,
                book_id=book_id,
                # Do not default to plan_to_read; allow empty as default
                reading_status=ownership_data.get('reading_status', ''),
                ownership_status=OwnershipStatus(ownership_data.get('ownership_status', 'owned')),
                media_type=MediaType(ownership_data.get('media_type', 'physical')),
                location_id=ownership_data.get('location_id'),
                source=ownership_data.get('source', 'manual'),
                notes=ownership_data.get('notes') or '',  # Provide empty string if None
                date_added=final_date_added,
                custom_metadata=ownership_data.get('custom_metadata')
            )
            
            if success:
                logger.info(f"✅ Added book {book_id} to user {user_id} library")
            
            return success
            
        except Exception as e:
            logger.error(f"Failed to add book to library: {e}")
            return False
    
    async def remove_book_from_library(self, user_id: str, book_uid: str) -> bool:
        """Remove a book from user's library by book UID."""
        if not self._ensure_initialized():
            return False
        
        try:
            # First find the book by UID to get the book_id
            book_repo = self._get_book_repo()
            book_data = await book_repo.get_by_uid(book_uid)
            if not book_data:
                logger.warning(f"Book with UID {book_uid} not found")
                return False
            
            # book_data is a dictionary, so access id via dict key
            book_id = book_data.get('id') if isinstance(book_data, dict) else book_data.id
            if not book_id:
                logger.warning(f"Book {book_uid} has no valid ID")
                return False
            
            # Remove the ownership relationship
            user_book_repo = self._get_user_book_repo()
            success = await user_book_repo.remove_ownership(user_id, book_id)
            
            if success:
                logger.info(f"✅ Removed book {book_uid} from user {user_id} library")
            else:
                logger.warning(f"Failed to remove book {book_uid} from user {user_id} library")
            
            return success
            
        except Exception as e:
            logger.error(f"Failed to remove book from library: {e}")
            return False
    
    async def get_user_library(self, user_id: str, 
                              reading_status: Optional[str] = None,
                              limit: int = 50, offset: int = 0) -> List[Dict[str, Any]]:
        """Get user's library with optional filtering."""
        if not self._ensure_initialized():
            return []
        
        try:
            from app.domain.models import ReadingStatus
            
            status_filter = None
            if reading_status:
                if isinstance(reading_status, ReadingStatus):
                    status_filter = reading_status.value
                else:
                    status_filter = reading_status
            
            logger.info(f"🔍 Getting user library for {user_id}, status_filter: {status_filter}")
            
            user_book_repo = self._get_user_book_repo()
            user_books = await user_book_repo.get_user_books(
                user_id, reading_status=status_filter, limit=limit, offset=offset
            )
            
            logger.info(f"🔍 Retrieved {len(user_books)} user books from repository")
            
            result = []
            for user_book in user_books:
                # user_book is a dict with 'book' and 'ownership' keys
                book_data = user_book['book']
                ownership_data = user_book['ownership']
                
                book_dict = await self._book_to_dict_from_data(book_data)
                
                # Handle location information
                location_id = ownership_data.get('location_id')
                locations = []
                if location_id:
                    try:
                        # Get location details from location repository
                        location_repo = self._get_location_repo()
                        location_data = await location_repo.get_by_id(location_id)
                        if location_data:
                            location_name = location_data.get('name', location_id)
                            locations = [location_name]
                            logger.debug(f"Successfully resolved location {location_id} to name: {location_name}")
                        else:
                            # If location not found, try to get from user's locations list
                            user_locations = await location_repo.get_user_locations(user_id)
                            location_name = None
                            for loc in user_locations:
                                if loc.get('id') == location_id:
                                    location_name = loc.get('name', location_id)
                                    break
                            
                            if location_name:
                                locations = [location_name]
                                logger.debug(f"Resolved location {location_id} from user locations: {location_name}")
                            else:
                                # Last resort: use a placeholder name
                                locations = [f"Location {location_id}"]
                                logger.warning(f"Could not resolve location {location_id}, using placeholder")
                    except Exception as e:
                        logger.error(f"Failed to get location {location_id}: {e}")
                        # Try to get a meaningful name instead of just using ID
                        try:
                            location_repo = self._get_location_repo()
                            user_locations = await location_repo.get_user_locations(user_id)
                            location_name = None
                            for loc in user_locations:
                                if loc.get('id') == location_id:
                                    location_name = loc.get('name', location_id)
                                    break
                            locations = [location_name or f"Location {location_id}"]
                        except:
                            locations = [f"Location {location_id}"]
                
                # Add ownership data to book
                book_dict['ownership'] = {
                    'reading_status': ownership_data.get('reading_status'),
                    'ownership_status': ownership_data.get('ownership_status'),
                    'media_type': ownership_data.get('media_type'),
                    'date_added': ownership_data.get('date_added'),
                    'notes': ownership_data.get('notes'),
                    'location_id': location_id
                }
                
                # Add location information that the frontend expects
                book_dict['locations'] = locations
                book_dict['reading_status'] = ownership_data.get('reading_status')
                book_dict['ownership_status'] = ownership_data.get('ownership_status')
                
                result.append(book_dict)
            
            logger.info(f"🔍 Returning {len(result)} books to user library")
            return result
            
        except Exception as e:
            logger.error(f"Failed to get user library: {e}")
            import traceback
            traceback.print_exc()
            return []
    
    async def update_reading_status(self, user_id: str, book_id: str, 
                                   status: str) -> bool:
        """Update reading status for a book."""
        if not self._ensure_initialized():
            return False
        
        try:
            from app.domain.models import ReadingStatus
            
            reading_status = ReadingStatus(status)
            user_book_repo = self._get_user_book_repo()
            success = await user_book_repo.update_reading_status(
                user_id, book_id, reading_status.value
            )
            
            return success
            
        except Exception as e:
            logger.error(f"Failed to update reading status: {e}")
            return False
    
    # ========================================
    # Statistics and Analytics
    # ========================================
    
    async def get_user_statistics(self, user_id: str) -> Dict[str, Any]:
        """Get comprehensive user library statistics."""
        if not self._ensure_initialized():
            return {}
        
        try:
            user_book_repo = self._get_user_book_repo()
            stats = await user_book_repo.get_user_statistics(user_id)
            return stats
            
        except Exception as e:
            logger.error(f"Failed to get user statistics: {e}")
            return {}
    
    async def get_reading_timeline(self, user_id: str, 
                                  limit: int = 20) -> List[Dict[str, Any]]:
        """Get user's reading timeline."""
        if not self._ensure_initialized():
            return []
        
        try:
            user_book_repo = self._get_user_book_repo()
            timeline = await user_book_repo.get_reading_timeline(user_id, limit)
            return timeline
            
        except Exception as e:
            logger.error(f"Failed to get reading timeline: {e}")
            return []
    
    # ========================================
    # Location Management
    # ========================================
    
    async def create_location(self, user_id: str, 
                             location_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Create a new location for user."""
        if not self._ensure_initialized():
            return None
        
        try:
            from app.domain.models import Location
            
            location = Location(
                id=location_data.get('id') or str(uuid.uuid4()),
                user_id=user_id,
                name=location_data.get('name', ''),
                description=location_data.get('description'),
                location_type=location_data.get('location_type', 'other'),
                is_default=location_data.get('is_default', False),
                is_active=location_data.get('is_active', True)
            )
            
            location_repo = self._get_location_repo()
            created_location = await location_repo.create(location, user_id)
            if created_location:
                return self._location_to_dict(created_location)
            return None
            
        except Exception as e:
            logger.error(f"Failed to create location: {e}")
            return None
    
    async def get_user_locations(self, user_id: str) -> List[Dict[str, Any]]:
        """Get all locations for a user."""
        if not self._ensure_initialized():
            return []
        
        try:
            location_repo = self._get_location_repo()
            locations = await location_repo.get_user_locations(user_id)
            return [self._location_to_dict(loc) for loc in locations]
            
        except Exception as e:
            logger.error(f"Failed to get user locations: {e}")
            return []
    
    def _location_to_dict(self, location) -> Dict[str, Any]:
        """Convert location object to dictionary."""
        return {
            'id': location.id,
            'user_id': location.user_id,
            'name': location.name,
            'description': location.description,
            'location_type': location.location_type,
            'is_default': location.is_default,
            'is_active': location.is_active,
            'created_at': location.created_at
        }


# Global instance for use throughout the application
kuzu_service = KuzuIntegrationService()


def get_kuzu_service() -> KuzuIntegrationService:
    """Get the global Kuzu integration service instance."""
    if not kuzu_service._initialized:
        kuzu_service.initialize()
    return kuzu_service

# Add type: ignore to suppress Optional repository warnings
# The repositories are guaranteed to be initialized when _ensure_initialized() returns True
