"""
Repository interfaces for the domain layer.

These interfaces define the contracts for data access without coupling to specific implementations.
Following the Repository pattern and Dependency Inversion Principle.
"""

from abc import ABC, abstractmethod
from typing import List, Optional, Dict, Any, Tuple
from datetime import datetime

from .models import (
    Book, User, Author, Publisher, Series, Category, 
    UserBookRelationship, ReadingLog, ImportTask,
    CustomFieldDefinition, ImportMappingTemplate
)


class BookRepository(ABC):
    """Repository interface for Book operations."""
    
    @abstractmethod
    async def create(self, book: Book) -> Book:
        """Create a new book."""
        pass
    
    @abstractmethod
    async def get_by_id(self, book_id: str) -> Optional[Book]:
        """Get a book by ID."""
        pass
    
    @abstractmethod
    async def get_by_isbn(self, isbn: str) -> Optional[Book]:
        """Get a book by ISBN (13 or 10)."""
        pass
    
    @abstractmethod
    async def find_duplicates(self, book: Book) -> List[Tuple[Book, float]]:
        """Find potential duplicate books with confidence scores."""
        pass
    
    @abstractmethod
    async def search(self, query: str, filters: Optional[Dict[str, Any]] = None) -> List[Book]:
        """Search books with optional filters."""
        pass
    
    @abstractmethod
    async def update(self, book: Book) -> Book:
        """Update an existing book."""
        pass
    
    @abstractmethod
    async def delete(self, book_id: str) -> bool:
        """Delete a book (admin only)."""
        pass
    
    @abstractmethod
    async def merge_books(self, source_id: str, target_id: str) -> Book:
        """Merge two duplicate book records (admin only)."""
        pass


class UserRepository(ABC):
    """Repository interface for User operations."""
    
    @abstractmethod
    async def create(self, user: User) -> User:
        """Create a new user."""
        pass
    
    @abstractmethod
    async def get_by_id(self, user_id: str) -> Optional[User]:
        """Get a user by ID."""
        pass
    
    @abstractmethod
    async def get_by_username(self, username: str) -> Optional[User]:
        """Get a user by username."""
        pass
    
    @abstractmethod
    async def get_by_email(self, email: str) -> Optional[User]:
        """Get a user by email."""
        pass
    
    @abstractmethod
    async def update(self, user: User) -> User:
        """Update an existing user."""
        pass
    
    @abstractmethod
    async def delete(self, user_id: str) -> bool:
        """Delete a user."""
        pass
    
    @abstractmethod
    async def list_all(self, limit: int = 100, offset: int = 0) -> List[User]:
        """List all users (admin only)."""
        pass


class UserBookRepository(ABC):
    """Repository interface for User-Book relationships."""
    
    @abstractmethod
    async def create_relationship(self, relationship: UserBookRelationship) -> UserBookRelationship:
        """Create a user-book relationship."""
        pass
    
    @abstractmethod
    async def get_relationship(self, user_id: str, book_id: str) -> Optional[UserBookRelationship]:
        """Get a specific user-book relationship."""
        pass
    
    @abstractmethod
    async def get_user_library(self, user_id: str, filters: Optional[Dict[str, Any]] = None) -> List[UserBookRelationship]:
        """Get a user's library with optional filters."""
        pass
    
    @abstractmethod
    async def update_relationship(self, relationship: UserBookRelationship) -> UserBookRelationship:
        """Update a user-book relationship."""
        pass
    
    @abstractmethod
    async def delete_relationship(self, user_id: str, book_id: str) -> bool:
        """Delete a user-book relationship."""
        pass
    
    @abstractmethod
    async def get_book_owners(self, book_id: str) -> List[str]:
        """Get all user IDs who own a specific book."""
        pass
    
    @abstractmethod
    async def get_community_stats(self, user_id: str) -> Dict[str, Any]:
        """Get community statistics visible to the user based on privacy settings."""
        pass


class AuthorRepository(ABC):
    """Repository interface for Author operations."""
    
    @abstractmethod
    async def create(self, author: Author) -> Author:
        """Create a new author."""
        pass
    
    @abstractmethod
    async def get_by_id(self, author_id: str) -> Optional[Author]:
        """Get an author by ID."""
        pass
    
    @abstractmethod
    async def find_by_name(self, name: str) -> List[Author]:
        """Find authors by name (fuzzy matching)."""
        pass
    
    @abstractmethod
    async def update(self, author: Author) -> Author:
        """Update an existing author."""
        pass
    
    @abstractmethod
    async def get_collaborators(self, author_id: str) -> List[Author]:
        """Get authors who have collaborated with this author."""
        pass


class CategoryRepository(ABC):
    """Repository interface for Category operations."""
    
    @abstractmethod
    async def create(self, category: Category) -> Category:
        """Create a new category."""
        pass
    
    @abstractmethod
    async def get_by_id(self, category_id: str) -> Optional[Category]:
        """Get a category by ID."""
        pass
    
    @abstractmethod
    async def find_by_name(self, name: str) -> List[Category]:
        """Find categories by name."""
        pass
    
    @abstractmethod
    async def get_hierarchy(self) -> List[Category]:
        """Get the full category hierarchy."""
        pass
    
    @abstractmethod
    async def get_children(self, parent_id: str) -> List[Category]:
        """Get child categories of a parent."""
        pass
    
    @abstractmethod
    async def update(self, category: Category) -> Category:
        """Update an existing category."""
        pass


class ReadingLogRepository(ABC):
    """Repository interface for ReadingLog operations."""
    
    @abstractmethod
    async def create(self, log: ReadingLog) -> ReadingLog:
        """Create a new reading log entry."""
        pass
    
    @abstractmethod
    async def get_user_logs(self, user_id: str, start_date: Optional[datetime] = None, end_date: Optional[datetime] = None) -> List[ReadingLog]:
        """Get reading logs for a user within a date range."""
        pass
    
    @abstractmethod
    async def get_book_logs(self, user_id: str, book_id: str) -> List[ReadingLog]:
        """Get reading logs for a specific user and book."""
        pass
    
    @abstractmethod
    async def update(self, log: ReadingLog) -> ReadingLog:
        """Update a reading log entry."""
        pass
    
    @abstractmethod
    async def delete(self, log_id: str) -> bool:
        """Delete a reading log entry."""
        pass
    
    @abstractmethod
    async def calculate_streak(self, user_id: str) -> int:
        """Calculate the current reading streak for a user."""
        pass


class ImportTaskRepository(ABC):
    """Repository interface for ImportTask operations."""
    
    @abstractmethod
    async def create(self, task: ImportTask) -> ImportTask:
        """Create a new import task."""
        pass
    
    @abstractmethod
    async def get_by_id(self, task_id: str) -> Optional[ImportTask]:
        """Get a task by ID."""
        pass
    
    @abstractmethod
    async def get_user_tasks(self, user_id: str, limit: int = 20) -> List[ImportTask]:
        """Get tasks for a user."""
        pass
    
    @abstractmethod
    async def update(self, task: ImportTask) -> ImportTask:
        """Update a task."""
        pass
    
    @abstractmethod
    async def get_pending_tasks(self) -> List[ImportTask]:
        """Get all pending tasks for processing."""
        pass
    
    @abstractmethod
    async def delete(self, task_id: str) -> bool:
        """Delete a task."""
        pass


class DeduplicationService(ABC):
    """Service interface for book deduplication operations."""
    
    @abstractmethod
    async def find_potential_duplicates(self, book: Book) -> List[Tuple[Book, float]]:
        """Find potential duplicate books with confidence scores."""
        pass
    
    @abstractmethod
    async def merge_books(self, source_book_id: str, target_book_id: str, user_id: str) -> Dict[str, Any]:
        """Merge duplicate books and return merge report."""
        pass
    
    @abstractmethod
    async def validate_import_data(self, import_data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Validate import data and return conflict report."""
        pass


class GraphQueryService(ABC):
    """Service interface for complex graph queries."""
    
    @abstractmethod
    async def get_recommendations(self, user_id: str, limit: int = 10) -> List[Book]:
        """Get book recommendations for a user."""
        pass
    
    @abstractmethod
    async def find_reading_overlap(self, user_id_1: str, user_id_2: str) -> List[Book]:
        """Find books that two users have both read."""
        pass
    
    @abstractmethod
    async def get_community_popular_books(self, user_id: str, limit: int = 10) -> List[Tuple[Book, int]]:
        """Get popular books in the user's community."""
        pass
    
    @abstractmethod
    async def get_author_collaboration_network(self, author_id: str, depth: int = 2) -> Dict[str, Any]:
        """Get author collaboration network."""
        pass
    
    @abstractmethod
    async def analyze_reading_patterns(self, user_id: str) -> Dict[str, Any]:
        """Analyze user's reading patterns."""
        pass


class CustomFieldRepository(ABC):
    """Repository interface for CustomFieldDefinition operations."""
    
    @abstractmethod
    async def create(self, field_def: CustomFieldDefinition) -> CustomFieldDefinition:
        """Create a new custom field definition."""
        pass
    
    @abstractmethod
    async def get_by_id(self, field_id: str) -> Optional[CustomFieldDefinition]:
        """Get a custom field definition by ID."""
        pass
    
    @abstractmethod
    async def get_by_user(self, user_id: str) -> List[CustomFieldDefinition]:
        """Get all custom field definitions created by a user."""
        pass
    
    @abstractmethod
    async def get_shareable(self, exclude_user_id: Optional[str] = None) -> List[CustomFieldDefinition]:
        """Get all shareable custom field definitions."""
        pass
    
    @abstractmethod
    async def search(self, query: str, user_id: Optional[str] = None) -> List[CustomFieldDefinition]:
        """Search custom field definitions."""
        pass
    
    @abstractmethod
    async def update(self, field_def: CustomFieldDefinition) -> CustomFieldDefinition:
        """Update an existing custom field definition."""
        pass
    
    @abstractmethod
    async def delete(self, field_id: str) -> bool:
        """Delete a custom field definition."""
        pass
    
    @abstractmethod
    async def increment_usage(self, field_id: str) -> None:
        """Increment usage count for a field definition."""
        pass
    
    @abstractmethod
    async def get_popular(self, limit: int = 20) -> List[CustomFieldDefinition]:
        """Get most popular shareable custom field definitions."""
        pass


class ImportMappingRepository(ABC):
    """Repository interface for ImportMappingTemplate operations."""
    
    @abstractmethod
    async def create(self, template: ImportMappingTemplate) -> ImportMappingTemplate:
        """Create a new import mapping template."""
        pass
    
    @abstractmethod
    async def get_by_id(self, template_id: str) -> Optional[ImportMappingTemplate]:
        """Get an import mapping template by ID."""
        pass
    
    @abstractmethod
    async def get_by_user(self, user_id: str) -> List[ImportMappingTemplate]:
        """Get all import mapping templates for a user."""
        pass
    
    @abstractmethod
    async def detect_template(self, headers: List[str], user_id: str) -> Optional[ImportMappingTemplate]:
        """Detect matching template based on CSV headers."""
        pass
    
    @abstractmethod
    async def update(self, template: ImportMappingTemplate) -> ImportMappingTemplate:
        """Update an existing import mapping template."""
        pass
    
    @abstractmethod
    async def delete(self, template_id: str) -> bool:
        """Delete an import mapping template."""
        pass
    
    @abstractmethod
    async def increment_usage(self, template_id: str) -> None:
        """Increment usage count and update last used timestamp."""
        pass
