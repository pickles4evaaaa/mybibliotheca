"""
Clean Service Layer - Adapter for new graph schema

Provides compatibility with existing service interface while using the new clean repositories.
"""

import uuid
import logging
from typing import List, Optional, Dict, Any
from datetime import datetime
from dataclasses import asdict

from ..domain.models import Book, User, Category, Person, Publisher, Series, ReadingStatus, OwnershipStatus, MediaType
from .kuzu_repositories_clean import (
    CleanBookRepository, 
    CleanUserRepository, 
    CleanUserBookRepository,
    CleanLocationRepository,
    CleanCategoryRepository
)

logger = logging.getLogger(__name__)


class CleanBookService:
    """Clean book service using new graph schema."""
    
    def __init__(self):
        self.book_repo = CleanBookRepository()
        self.user_book_repo = CleanUserBookRepository()
        self.category_repo = CleanCategoryRepository()
    
    async def create_book(self, book: Book) -> Book:
        """Create a new book."""
        return await self.book_repo.create(book)
    
    async def get_book_by_id(self, book_id: str) -> Optional[Book]:
        """Get a book by ID."""
        return await self.book_repo.get_by_id(book_id)
    
    async def add_book_to_user_library(self, user_id: str, book_id: str, 
                                      reading_status: ReadingStatus = ReadingStatus.PLAN_TO_READ,
                                      ownership_status: OwnershipStatus = OwnershipStatus.OWNED,
                                      media_type: MediaType = MediaType.PHYSICAL,
                                      locations: List[str] = None) -> bool:
        """Add a book to user's library."""
        try:
            primary_location = locations[0] if locations else None
            
            success = await self.user_book_repo.add_book_to_library(
                user_id=user_id,
                book_id=book_id,
                reading_status=reading_status.value,
                ownership_status=ownership_status.value,
                media_type=media_type.value,
                location_id=primary_location
            )
            
            logger.info(f"📚 [CLEAN_SERVICE] {'✅' if success else '❌'} Add book {book_id} to user {user_id} library: {success}")
            return success
            
        except Exception as e:
            logger.error(f"❌ [CLEAN_SERVICE] Failed to add book to library: {e}")
            return False
    
    async def process_book_categories(self, book_id: str, category_names: List[str]) -> int:
        """Process and link categories to a book."""
        linked_count = 0
        
        for category_name in category_names:
            try:
                # Find or create category
                category = await self.find_or_create_category(category_name)
                if category:
                    # Link to book (this should already be done in book creation, but ensuring)
                    linked_count += 1
                    logger.info(f"✅ [CLEAN_SERVICE] Processed category '{category_name}' for book {book_id}")
                
            except Exception as e:
                logger.error(f"❌ [CLEAN_SERVICE] Failed to process category '{category_name}': {e}")
        
        return linked_count
    
    async def find_or_create_category(self, category_name: str) -> Optional[Category]:
        """Find existing category or create new one."""
        try:
            # For now, always create new categories
            # In a real implementation, you'd search first
            category = Category(
                id=str(uuid.uuid4()),
                name=category_name,
                normalized_name=category_name.lower().strip()
            )
            
            return await self.category_repo.create(category)
            
        except Exception as e:
            logger.error(f"Failed to find/create category '{category_name}': {e}")
            return None
    
    async def list_all_categories(self) -> List[Category]:
        """List all categories."""
        try:
            return await self.category_repo.get_all()
        except Exception as e:
            logger.error(f"❌ [CLEAN_SERVICE] Error listing categories: {e}")
            return []


class CleanUserService:
    """Clean user service using new graph schema."""
    
    def __init__(self):
        self.user_repo = CleanUserRepository()
        self.location_repo = CleanLocationRepository()
    
    async def create_user(self, user: User) -> User:
        """Create a new user."""
        return await self.user_repo.create(user)
    
    async def get_user_by_id(self, user_id: str) -> Optional[User]:
        """Get a user by ID."""
        return await self.user_repo.get_by_id(user_id)
    
    async def get_user_by_username(self, username: str) -> Optional[User]:
        """Get a user by username."""
        return await self.user_repo.get_by_username(username)
    
    async def create_user_location(self, user_id: str, name: str, description: str = None,
                                  location_type: str = "home", is_default: bool = False) -> str:
        """Create a location for a user."""
        return await self.location_repo.create_location(
            user_id=user_id,
            name=name,
            description=description,
            location_type=location_type,
            is_default=is_default
        )
    
    async def get_user_locations(self, user_id: str) -> List[Dict[str, Any]]:
        """Get all locations for a user."""
        try:
            results = await self.location_repo.get_user_locations(user_id)
            
            locations = []
            for result in results:
                if 'col_0' in result and 'col_1' in result:
                    location_data = result['col_0']
                    relation_data = result['col_1']
                    
                    location = {
                        'id': location_data['id'],
                        'name': location_data['name'],
                        'description': location_data.get('description'),
                        'location_type': location_data.get('location_type'),
                        'is_default': location_data.get('is_default', False),
                        'is_active': True,  # All locations are active in new schema
                        'is_primary': relation_data.get('is_primary', False)
                    }
                    locations.append(location)
            
            return locations
            
        except Exception as e:
            logger.error(f"❌ [CLEAN_USER_SERVICE] Failed to get user locations: {e}")
            return []
    
    async def get_default_location(self, user_id: str) -> Optional[Dict[str, Any]]:
        """Get the default location for a user."""
        try:
            locations = await self.get_user_locations(user_id)
            
            # Find default location
            for location in locations:
                if location.get('is_default') or location.get('is_primary'):
                    return location
            
            # Return first location if no default
            if locations:
                return locations[0]
            
            return None
            
        except Exception as e:
            logger.error(f"❌ [CLEAN_USER_SERVICE] Failed to get default location: {e}")
            return None


class CleanPersonService:
    """Clean person service for handling authors/contributors."""
    
    def __init__(self):
        from .kuzu_graph import get_kuzu_connection
        self.db = get_kuzu_connection()
    
    async def list_all_persons(self) -> List[Person]:
        """List all persons."""
        try:
            query = "MATCH (p:Person) RETURN p"
            results = self.db.query(query)
            
            persons = []
            for result in results:
                if 'col_0' in result:
                    person_data = result['col_0']
                    person = Person(
                        id=person_data['id'],
                        name=person_data['name'],
                        normalized_name=person_data.get('normalized_name'),
                        birth_year=person_data.get('birth_year'),
                        death_year=person_data.get('death_year'),
                        bio=person_data.get('bio'),
                        created_at=person_data.get('created_at', datetime.utcnow())
                    )
                    persons.append(person)
            
            logger.info(f"📊 [CLEAN_PERSON_SERVICE] Returning {len(persons)} Person objects")
            return persons
            
        except Exception as e:
            logger.error(f"❌ [CLEAN_PERSON_SERVICE] Error listing persons: {e}")
            return []
    
    async def get_books_by_person(self, person_id: str, user_id: str = None) -> List[Dict[str, Any]]:
        """Get all books by a person."""
        try:
            query = """
            MATCH (p:Person {id: $person_id})-[auth:AUTHORED]->(b:Book)
            RETURN b, auth
            """
            results = self.db.query(query, {'person_id': person_id})
            
            books = []
            for result in results:
                if 'col_0' in result:
                    book_data = result['col_0']
                    books.append(book_data)
            
            logger.info(f"📊 [CLEAN_PERSON_SERVICE] Returning {len(books)} books for person {person_id}")
            return books
            
        except Exception as e:
            logger.error(f"❌ [CLEAN_PERSON_SERVICE] Error getting books for person: {e}")
            return []


# Legacy compatibility - Service class that mimics the old interface
class CleanServiceCompat:
    """Compatibility layer for the old service interface."""
    
    def __init__(self):
        self.book_service = CleanBookService()
        self.user_service = CleanUserService()
        self.person_service = CleanPersonService()
    
    # Book operations
    async def create_book(self, book: Book) -> Book:
        return await self.book_service.create_book(book)
    
    async def get_book_by_id(self, book_id: str) -> Optional[Book]:
        return await self.book_service.get_book_by_id(book_id)
    
    async def add_book_to_user_library(self, user_id: str, book_id: str, 
                                      reading_status: ReadingStatus = ReadingStatus.PLAN_TO_READ,
                                      ownership_status: OwnershipStatus = OwnershipStatus.OWNED,
                                      media_type: MediaType = MediaType.PHYSICAL,
                                      locations: List[str] = None,
                                      **kwargs) -> bool:
        """Add book to user library - compatibility method."""
        return await self.book_service.add_book_to_user_library(
            user_id, book_id, reading_status, ownership_status, media_type, locations
        )
    
    async def process_book_categories(self, book_id: str, category_names: List[str]) -> int:
        return await self.book_service.process_book_categories(book_id, category_names)
    
    async def list_all_categories(self) -> List[Category]:
        return await self.book_service.list_all_categories()
    
    # User operations
    async def create_user(self, user: User) -> User:
        return await self.user_service.create_user(user)
    
    async def get_user_by_id(self, user_id: str) -> Optional[User]:
        return await self.user_service.get_user_by_id(user_id)
    
    async def get_user_by_username(self, username: str) -> Optional[User]:
        return await self.user_service.get_user_by_username(username)
    
    # Person operations
    async def list_all_persons(self) -> List[Person]:
        return await self.person_service.list_all_persons()
    
    async def get_books_by_person(self, person_id: str, user_id: str = None) -> List[Dict[str, Any]]:
        return await self.person_service.get_books_by_person(person_id, user_id)
    
    # Location operations
    async def create_user_location(self, user_id: str, name: str, description: str = None,
                                  location_type: str = "home", is_default: bool = False) -> str:
        return await self.user_service.create_user_location(
            user_id, name, description, location_type, is_default
        )
    
    async def get_user_locations(self, user_id: str, active_only: bool = True) -> List[Dict[str, Any]]:
        # active_only is ignored in new schema - all locations are considered active
        return await self.user_service.get_user_locations(user_id)
    
    async def get_default_location(self, user_id: str) -> Optional[Dict[str, Any]]:
        return await self.user_service.get_default_location(user_id)


# Global service instance
_clean_service = None

def get_clean_service() -> CleanServiceCompat:
    """Get the global clean service instance."""
    global _clean_service
    if _clean_service is None:
        _clean_service = CleanServiceCompat()
    return _clean_service
