"""
Clean service layer using the simplified Kuzu repositories.
"""

import logging
import uuid
from typing import Optional, List, Dict, Any
from datetime import datetime

from .infrastructure.kuzu_clean_repositories import (
    CleanKuzuUserRepository,
    CleanKuzuBookRepository, 
    CleanKuzuUserBookRepository,
    CleanKuzuLocationRepository
)
from .domain.models import (
    User, Book, Person, Category, Location, BookContribution,
    ReadingStatus, OwnershipStatus, MediaType, ContributionType
)

logger = logging.getLogger(__name__)


class CleanUserService:
    """Clean user service using simplified repositories."""
    
    def __init__(self):
        self.user_repo = CleanKuzuUserRepository()
        self.location_repo = CleanKuzuLocationRepository()
    
    async def create_user(self, username: str, email: str, password_hash: str,
                         is_admin: bool = False) -> Optional[User]:
        """Create a new user."""
        try:
            user = User(
                id=str(uuid.uuid4()),
                username=username,
                email=email,
                password_hash=password_hash,
                is_admin=is_admin,
                is_active=True,
                created_at=datetime.utcnow()
            )
            
            created_user = await self.user_repo.create(user)
            if created_user:
                logger.info(f"✅ User service created user: {username}")
            
            return created_user
            
        except Exception as e:
            logger.error(f"Failed to create user {username}: {e}")
            return None
    
    async def get_user_by_id(self, user_id: str) -> Optional[User]:
        """Get user by ID."""
        return await self.user_repo.get_by_id(user_id)
    
    async def get_user_by_username(self, username: str) -> Optional[User]:
        """Get user by username."""
        return await self.user_repo.get_by_username(username)
    
    async def create_user_location(self, user_id: str, name: str, description: str = None,
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


class CleanBookService:
    """Clean book service using simplified repositories."""
    
    def __init__(self):
        self.book_repo = CleanKuzuBookRepository()
        self.user_book_repo = CleanKuzuUserBookRepository()
    
    async def create_book(self, title: str, authors: List[str] = None, 
                         categories: List[str] = None, **kwargs) -> Optional[Book]:
        """Create a new book."""
        try:
            book = Book(
                id=str(uuid.uuid4()),
                title=title,
                normalized_title=title.lower(),
                created_at=datetime.utcnow(),
                **kwargs
            )
            
            # Add contributors (authors)
            if authors:
                contributors = []
                for i, author_name in enumerate(authors):
                    person = Person(
                        id=str(uuid.uuid4()),
                        name=author_name,
                        normalized_name=author_name.lower(),
                        created_at=datetime.utcnow()
                    )
                    
                    contribution = BookContribution(
                        person=person,
                        contribution_type=ContributionType.AUTHORED,
                        order=i
                    )
                    contributors.append(contribution)
                
                book.contributors = contributors
            
            # Add categories
            if categories:
                book_categories = []
                for cat_name in categories:
                    category = Category(
                        id=str(uuid.uuid4()),
                        name=cat_name,
                        normalized_name=cat_name.lower(),
                        created_at=datetime.utcnow()
                    )
                    book_categories.append(category)
                
                book.categories = book_categories
            
            created_book = await self.book_repo.create(book)
            if created_book:
                logger.info(f"✅ Book service created book: {title}")
            
            return created_book
            
        except Exception as e:
            logger.error(f"Failed to create book {title}: {e}")
            return None
    
    async def get_book_by_id(self, book_id: str) -> Optional[Book]:
        """Get book by ID."""
        return await self.book_repo.get_by_id(book_id)
    
    async def add_book_to_user_library(self, user_id: str, book_id: str,
                                      reading_status: ReadingStatus = ReadingStatus.PLAN_TO_READ,
                                      ownership_status: OwnershipStatus = OwnershipStatus.OWNED,
                                      media_type: MediaType = MediaType.PHYSICAL,
                                      location_id: str = None) -> bool:
        """Add a book to a user's library."""
        try:
            success = await self.user_book_repo.add_book_to_library(
                user_id, book_id, reading_status, ownership_status, media_type, location_id
            )
            
            if success:
                logger.info(f"✅ Added book {book_id} to user {user_id} library")
            
            return success
            
        except Exception as e:
            logger.error(f"Failed to add book to library: {e}")
            return False
    
    async def get_user_books(self, user_id: str, reading_status: str = None) -> List[Dict[str, Any]]:
        """Get books in a user's library."""
        return await self.user_book_repo.get_user_books(user_id, reading_status)
    
    async def start_reading_book(self, user_id: str, book_id: str) -> bool:
        """Mark a book as started reading."""
        try:
            from .infrastructure.kuzu_graph import get_kuzu_connection
            db = get_kuzu_connection()
            
            success = db.create_relationship(
                'User', user_id, 'STARTED_READING', 'Book', book_id,
                {'start_date': datetime.utcnow()}
            )
            
            if success:
                logger.info(f"✅ User {user_id} started reading book {book_id}")
            
            return success
            
        except Exception as e:
            logger.error(f"Failed to mark book as started: {e}")
            return False
    
    async def finish_reading_book(self, user_id: str, book_id: str, 
                                 completion_status: str = "completed") -> bool:
        """Mark a book as finished reading."""
        try:
            from .infrastructure.kuzu_graph import get_kuzu_connection
            db = get_kuzu_connection()
            
            success = db.create_relationship(
                'User', user_id, 'FINISHED_READING', 'Book', book_id,
                {
                    'finish_date': datetime.utcnow(),
                    'completion_status': completion_status
                }
            )
            
            if success:
                logger.info(f"✅ User {user_id} finished reading book {book_id}")
            
            return success
            
        except Exception as e:
            logger.error(f"Failed to mark book as finished: {e}")
            return False


class CleanLibraryService:
    """High-level library service orchestrating user and book operations."""
    
    def __init__(self):
        self.user_service = CleanUserService()
        self.book_service = CleanBookService()
    
    async def setup_user_with_library(self, username: str, email: str, password_hash: str,
                                     location_name: str = "Home") -> Optional[Dict[str, Any]]:
        """Complete user setup with default location."""
        try:
            # Create user
            user = await self.user_service.create_user(username, email, password_hash)
            if not user:
                return None
            
            # Create default location
            location = await self.user_service.create_user_location(
                user.id, location_name, is_default=True
            )
            
            return {
                'user': user,
                'default_location': location
            }
            
        except Exception as e:
            logger.error(f"Failed to setup user with library: {e}")
            return None
    
    async def add_book_to_library(self, user_id: str, title: str, authors: List[str] = None,
                                 categories: List[str] = None, location_id: str = None,
                                 **book_kwargs) -> Optional[Dict[str, Any]]:
        """Create a book and add it to user's library."""
        try:
            # Create the book
            book = await self.book_service.create_book(title, authors, categories, **book_kwargs)
            if not book:
                return None
            
            # Add to user's library
            success = await self.book_service.add_book_to_user_library(
                user_id, book.id, location_id=location_id
            )
            
            if success:
                return {
                    'book': book,
                    'added_to_library': True
                }
            else:
                return {
                    'book': book,
                    'added_to_library': False
                }
            
        except Exception as e:
            logger.error(f"Failed to add book to library: {e}")
            return None
    
    async def get_user_library_stats(self, user_id: str) -> Dict[str, Any]:
        """Get comprehensive library statistics for a user."""
        try:
            from .infrastructure.kuzu_graph import get_kuzu_connection
            db = get_kuzu_connection()
            
            stats = {}
            
            # Total books owned
            query = "MATCH (u:User {id: $user_id})-[:OWNS]->(b:Book) RETURN COUNT(b) as count"
            result = db.query(query, {'user_id': user_id})
            stats['total_books'] = result[0]['col_0'] if result else 0
            
            # Books by reading status
            query = """
            MATCH (u:User {id: $user_id})-[owns:OWNS]->(b:Book)
            RETURN owns.reading_status as status, COUNT(b) as count
            """
            result = db.query(query, {'user_id': user_id})
            status_counts = {}
            for row in result:
                status_counts[row['col_0']] = row['col_1']
            stats['by_reading_status'] = status_counts
            
            # Books finished
            query = "MATCH (u:User {id: $user_id})-[:FINISHED_READING]->(b:Book) RETURN COUNT(b) as count"
            result = db.query(query, {'user_id': user_id})
            stats['books_finished'] = result[0]['col_0'] if result else 0
            
            return stats
            
        except Exception as e:
            logger.error(f"Failed to get library stats: {e}")
            return {}


# Service factory functions
def get_clean_user_service() -> CleanUserService:
    """Get user service instance."""
    return CleanUserService()

def get_clean_book_service() -> CleanBookService:
    """Get book service instance."""
    return CleanBookService()

def get_clean_library_service() -> CleanLibraryService:
    """Get library service instance."""
    return CleanLibraryService()
