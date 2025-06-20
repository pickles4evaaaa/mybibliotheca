"""
Redis-based repository implementations.

Concrete implementations of the domain repository interfaces using Redis as the storage backend.
"""

import uuid
import json
from typing import List, Optional, Dict, Any, Tuple
from datetime import datetime, date
from dataclasses import asdict
from enum import Enum

from ..domain.models import Book, User, Author, Publisher, Series, Category, UserBookRelationship, ReadingStatus, OwnershipStatus, MediaType
from ..domain.repositories import BookRepository, UserRepository, AuthorRepository, UserBookRepository
from .redis_graph import RedisGraphStorage


def _serialize_for_json(obj: Any) -> Any:
    """Convert objects to JSON-serializable format."""
    if isinstance(obj, datetime):
        return obj.isoformat()
    elif isinstance(obj, date):
        return obj.isoformat()
    elif isinstance(obj, Enum):
        return obj.value
    elif hasattr(obj, '__dict__'):
        # Handle dataclass or object with attributes
        return {k: _serialize_for_json(v) for k, v in obj.__dict__.items()}
    elif isinstance(obj, dict):
        return {k: _serialize_for_json(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [_serialize_for_json(item) for item in obj]
    else:
        return obj


class RedisBookRepository(BookRepository):
    """Redis-based implementation of BookRepository."""
    
    def __init__(self, storage: RedisGraphStorage):
        self.storage = storage
        
    async def create(self, book: Book) -> Book:
        """Create a new book."""
        if not book.id:
            book.id = str(uuid.uuid4())
            
        book_data = asdict(book)
        # Handle nested objects
        book_data['authors'] = [asdict(author) for author in book.authors]
        if book.publisher:
            book_data['publisher'] = asdict(book.publisher)
        if book.series:
            book_data['series'] = asdict(book.series)
        book_data['categories'] = [asdict(category) for category in book.categories]
        
        # Serialize datetime objects for JSON storage
        book_data = _serialize_for_json(book_data)
        
        success = self.storage.store_node('book', book.id, book_data)
        if not success:
            raise Exception(f"Failed to create book {book.id}")
            
        # Create relationships
        for author in book.authors:
            if author.id:
                self.storage.create_relationship('book', book.id, 'WRITTEN_BY', 'author', author.id)
                
        if book.publisher and book.publisher.id:
            self.storage.create_relationship('book', book.id, 'PUBLISHED_BY', 'publisher', book.publisher.id)
            
        if book.series and book.series.id:
            properties = {}
            if book.series_volume:
                properties['volume'] = book.series_volume
            if book.series_order:
                properties['order'] = book.series_order
            self.storage.create_relationship('book', book.id, 'PART_OF_SERIES', 'series', book.series.id, properties)
            
        for category in book.categories:
            if category.id:
                self.storage.create_relationship('book', book.id, 'CATEGORIZED_AS', 'category', category.id)
        
        return book
    
    async def get_by_id(self, book_id: str) -> Optional[Book]:
        """Get a book by ID."""
        book_data = self.storage.get_node('book', book_id)
        if not book_data:
            return None
            
        return self._data_to_book(book_data)
    
    async def get_by_isbn(self, isbn: str) -> Optional[Book]:
        """Get a book by ISBN (13 or 10)."""
        # Search for books with matching ISBN
        search_results = self.storage.search_nodes('book', {'isbn13': isbn})
        if not search_results:
            search_results = self.storage.search_nodes('book', {'isbn10': isbn})
            
        if search_results:
            return self._data_to_book(search_results[0])
        return None
    
    async def get_books_by_isbn(self, isbn: str) -> List[Book]:
        """Get books by ISBN (searches globally, not user-specific)."""
        try:
            # Clean the ISBN
            clean_isbn = isbn.replace('-', '').replace(' ', '')
            
            # Get all books from Redis and filter by ISBN
            all_book_keys = self.storage.redis.keys('book:*')
            matching_books = []
            
            for key in all_book_keys:
                try:
                    book_data = self.storage.redis.get(key)
                    if book_data:
                        book_dict = json.loads(book_data)
                        
                        # Check ISBN13 and ISBN10
                        book_isbn13 = book_dict.get('isbn13', '').replace('-', '').replace(' ', '')
                        book_isbn10 = book_dict.get('isbn10', '').replace('-', '').replace(' ', '')
                        
                        if clean_isbn == book_isbn13 or clean_isbn == book_isbn10:
                            book = self._data_to_book(book_dict)
                            matching_books.append(book)
                except Exception as e:
                    print(f"Error processing book key {key}: {e}")
                    continue
            
            return matching_books
        except Exception as e:
            print(f"Error searching books by ISBN: {e}")
            return []

    async def find_duplicates(self, book: Book) -> List[Tuple[Book, float]]:
        """Find potential duplicate books with confidence scores."""
        duplicates = []
        
        # Primary match: ISBN
        if book.isbn13:
            existing = await self.get_by_isbn(book.isbn13)
            if existing and existing.id != book.id:
                duplicates.append((existing, 1.0))  # 100% confidence
                
        if book.isbn10:
            existing = await self.get_by_isbn(book.isbn10)
            if existing and existing.id != book.id:
                duplicates.append((existing, 1.0))  # 100% confidence
        
        # Secondary match: Title + Author (fuzzy)
        if book.title and book.authors:
            # Simple implementation - in production we'd use more sophisticated matching
            all_books = self.storage.find_nodes_by_type('book')
            for book_data in all_books:
                candidate = self._data_to_book(book_data)
                if candidate.id == book.id:
                    continue
                    
                # Simple title similarity
                title_similarity = self._calculate_similarity(book.normalized_title, candidate.normalized_title)
                
                # Check author overlap
                book_authors = {author.normalized_name for author in book.authors}
                candidate_authors = {author.normalized_name for author in candidate.authors}
                author_overlap = len(book_authors & candidate_authors) / max(len(book_authors), len(candidate_authors), 1)
                
                # Combined confidence
                confidence = (title_similarity + author_overlap) / 2
                
                if confidence > 0.8:  # 80% threshold
                    duplicates.append((candidate, confidence))
        
        # Sort by confidence
        duplicates.sort(key=lambda x: x[1], reverse=True)
        return duplicates
    
    async def search(self, query: str, filters: Dict[str, Any] = None) -> List[Book]:
        """Search books with optional filters."""
        # Simple implementation - search in title and description
        search_fields = {}
        
        # Add filters if provided
        if filters:
            search_fields.update(filters)
            
        # Get all books and filter (in production, we'd use proper search indexing)
        all_books = self.storage.find_nodes_by_type('book')
        results = []
        
        for book_data in all_books:
            book = self._data_to_book(book_data)
            
            # Apply filters
            if filters:
                filter_match = True
                for key, value in filters.items():
                    if hasattr(book, key) and getattr(book, key) != value:
                        filter_match = False
                        break
                if not filter_match:
                    continue
            
            # Apply text search
            if query:
                searchable_text = f"{book.title} {book.description or ''}"
                for author in book.authors:
                    searchable_text += f" {author.name}"
                
                if query.lower() in searchable_text.lower():
                    results.append(book)
            else:
                results.append(book)
                
        return results
    
    async def update(self, book: Book) -> Book:
        """Update an existing book."""
        if not book.id:
            raise ValueError("Book must have an ID to update")
            
        book_data = asdict(book)
        # Handle nested objects
        book_data['authors'] = [asdict(author) for author in book.authors]
        if book.publisher:
            book_data['publisher'] = asdict(book.publisher)
        if book.series:
            book_data['series'] = asdict(book.series)
        book_data['categories'] = [asdict(category) for category in book.categories]
        
        success = self.storage.update_node('book', book.id, book_data)
        if not success:
            raise Exception(f"Failed to update book {book.id}")
            
        return book
    
    async def delete(self, book_id: str) -> bool:
        """Delete a book (admin only)."""
        return self.storage.delete_node('book', book_id)
    
    async def get_books_for_user(self, user_id: str, limit: int = 50, offset: int = 0) -> List[Book]:
        """Get all books for a user with their relationship data."""
        # Get all user-book relationships for this user
        relationships = self.storage.get_relationships('user', user_id, 'owns')
        
        books = []
        for rel in relationships[offset:offset + limit]:
            book_id = rel['to_id']
            book = await self.get_by_id(book_id)
            if book:
                # Add user relationship attributes to the book object
                rel_props = rel.get('properties', {})
                book.reading_status = rel_props.get('reading_status', 'plan_to_read')
                book.ownership_status = rel_props.get('ownership_status', 'owned')
                book.user_rating = rel_props.get('user_rating')
                book.personal_notes = rel_props.get('personal_notes')
                book.date_added = rel_props.get('date_added')
                book.want_to_read = rel_props.get('reading_status') == 'plan_to_read'
                book.library_only = rel_props.get('reading_status') == 'library_only'
                book.uid = book.id  # Ensure uid is available
                
                # Handle date fields
                if rel_props.get('start_date'):
                    try:
                        book.start_date = datetime.fromisoformat(rel_props['start_date']).date()
                    except:
                        book.start_date = None
                else:
                    book.start_date = None
                    
                if rel_props.get('finish_date'):
                    try:
                        book.finish_date = datetime.fromisoformat(rel_props['finish_date']).date()
                    except:
                        book.finish_date = None
                else:
                    book.finish_date = None
                
                books.append(book)
        
        return books
    
    async def merge_books(self, source_id: str, target_id: str) -> Book:
        """Merge two duplicate book records (admin only)."""
        # Get both books
        source_book = await self.get_by_id(source_id)
        target_book = await self.get_by_id(target_id)
        
        if not source_book or not target_book:
            raise ValueError("Both books must exist to merge")
        
        # Merge data (target wins, but fill in missing fields from source)
        if not target_book.description and source_book.description:
            target_book.description = source_book.description
        if not target_book.cover_url and source_book.cover_url:
            target_book.cover_url = source_book.cover_url
        if not target_book.isbn13 and source_book.isbn13:
            target_book.isbn13 = source_book.isbn13
        if not target_book.isbn10 and source_book.isbn10:
            target_book.isbn10 = source_book.isbn10
            
        # Update target book
        await self.update(target_book)
        
        # TODO: Move all user relationships from source to target
        # For now, just delete source
        await self.delete(source_id)
        
        return target_book
    
    def _data_to_book(self, data: Dict[str, Any]) -> Book:
        """Convert Redis data to Book domain model."""
        # Handle nested objects
        authors = []
        if 'authors' in data and data['authors']:
            for author_data in data['authors']:
                author_data_copy = author_data.copy()
                # Handle datetime field for author
                if 'created_at' in author_data_copy and isinstance(author_data_copy['created_at'], str):
                    try:
                        author_data_copy['created_at'] = datetime.fromisoformat(author_data_copy['created_at'])
                    except:
                        author_data_copy['created_at'] = datetime.utcnow()
                authors.append(Author(**author_data_copy))
                
        publisher = None
        if 'publisher' in data and data['publisher']:
            publisher_data = data['publisher'].copy()
            # Handle datetime field for publisher
            if 'created_at' in publisher_data and isinstance(publisher_data['created_at'], str):
                try:
                    publisher_data['created_at'] = datetime.fromisoformat(publisher_data['created_at'])
                except:
                    publisher_data['created_at'] = datetime.utcnow()
            publisher = Publisher(**publisher_data)
            
        series = None
        if 'series' in data and data['series']:
            series = Series(**data['series'])
            
        categories = []
        if 'categories' in data and data['categories']:
            for category_data in data['categories']:
                categories.append(Category(**category_data))
        
        # Remove nested objects and metadata fields from data for Book creation
        book_data = data.copy()
        book_data.pop('authors', None)
        book_data.pop('publisher', None)
        book_data.pop('series', None)
        book_data.pop('categories', None)
        # Remove Redis metadata fields
        book_data.pop('_type', None)
        book_data.pop('_id', None)
        book_data.pop('_created_at', None)
        book_data.pop('_updated_at', None)
        
        # Handle datetime fields
        if 'published_date' in book_data and isinstance(book_data['published_date'], str):
            try:
                book_data['published_date'] = datetime.fromisoformat(book_data['published_date'])
            except:
                book_data['published_date'] = None
                
        if 'created_at' in book_data and isinstance(book_data['created_at'], str):
            try:
                book_data['created_at'] = datetime.fromisoformat(book_data['created_at'])
            except:
                book_data['created_at'] = datetime.utcnow()
                
        if 'updated_at' in book_data and isinstance(book_data['updated_at'], str):
            try:
                book_data['updated_at'] = datetime.fromisoformat(book_data['updated_at'])
            except:
                book_data['updated_at'] = datetime.utcnow()
        
        book = Book(**book_data)
        book.authors = authors
        book.publisher = publisher
        book.series = series
        book.categories = categories
        
        return book
    
    def _calculate_similarity(self, text1: str, text2: str) -> float:
        """Calculate simple text similarity (Jaccard similarity)."""
        if not text1 or not text2:
            return 0.0
            
        words1 = set(text1.lower().split())
        words2 = set(text2.lower().split())
        
        intersection = words1 & words2
        union = words1 | words2
        
        if not union:
            return 0.0
            
        return len(intersection) / len(union)


class RedisUserRepository(UserRepository):
    """Redis-based implementation of UserRepository."""
    
    def __init__(self, storage: RedisGraphStorage):
        self.storage = storage
        
    async def create(self, user: User) -> User:
        """Create a new user."""
        if not user.id:
            user.id = str(uuid.uuid4())
            
        user_data = asdict(user)
        # Serialize datetime objects for JSON storage
        user_data = _serialize_for_json(user_data)
        
        success = self.storage.store_node('user', user.id, user_data)
        if not success:
            raise Exception(f"Failed to create user {user.id}")
            
        return user
    
    async def get_by_id(self, user_id: str) -> Optional[User]:
        """Get a user by ID."""
        user_data = self.storage.get_node('user', user_id)
        if not user_data:
            return None
            
        return self._data_to_user(user_data)
    
    async def get_by_username(self, username: str) -> Optional[User]:
        """Get a user by username."""
        search_results = self.storage.search_nodes('user', {'username': username})
        if search_results:
            return self._data_to_user(search_results[0])
        return None
    
    async def get_by_email(self, email: str) -> Optional[User]:
        """Get a user by email."""
        search_results = self.storage.search_nodes('user', {'email': email})
        if search_results:
            return self._data_to_user(search_results[0])
        return None
    
    async def update(self, user: User) -> User:
        """Update an existing user."""
        if not user.id:
            raise ValueError("User must have an ID to update")
            
        user_data = asdict(user)
        # Serialize datetime objects for JSON storage
        user_data = _serialize_for_json(user_data)
        
        success = self.storage.update_node('user', user.id, user_data)
        if not success:
            raise Exception(f"Failed to update user {user.id}")
            
        return user
    
    async def delete(self, user_id: str) -> bool:
        """Delete a user."""
        return self.storage.delete_node('user', user_id)
    
    async def list_all(self, limit: int = 100, offset: int = 0) -> List[User]:
        """List all users (admin only)."""
        user_data_list = self.storage.find_nodes_by_type('user', limit, offset)
        return [self._data_to_user(data) for data in user_data_list]
    
    async def get_all(self) -> List[User]:
        """Get all users (alias for list_all with no limits)."""
        user_data_list = self.storage.find_nodes_by_type('user', limit=1000, offset=0)
        return [self._data_to_user(data) for data in user_data_list]
    
    def _data_to_user(self, data: Dict[str, Any]) -> User:
        """Convert Redis data to User domain model."""
        # Handle datetime fields
        user_data = data.copy()
        # Remove Redis metadata fields
        user_data.pop('_type', None)
        user_data.pop('_id', None)
        user_data.pop('_created_at', None)
        user_data.pop('_updated_at', None)
        
        if 'created_at' in user_data and isinstance(user_data['created_at'], str):
            try:
                user_data['created_at'] = datetime.fromisoformat(user_data['created_at'])
            except:
                user_data['created_at'] = datetime.utcnow()
                
        if 'last_login' in user_data and isinstance(user_data['last_login'], str):
            try:
                user_data['last_login'] = datetime.fromisoformat(user_data['last_login'])
            except:
                user_data['last_login'] = None
        
        return User(**user_data)


class RedisAuthorRepository(AuthorRepository):
    """Redis-based implementation of AuthorRepository."""
    
    def __init__(self, storage: RedisGraphStorage):
        self.storage = storage
        
    async def create(self, author: Author) -> Author:
        """Create a new author."""
        if not author.id:
            author.id = str(uuid.uuid4())
            
        author_data = asdict(author)
        # Serialize datetime objects for JSON storage
        author_data = _serialize_for_json(author_data)
        success = self.storage.store_node('author', author.id, author_data)
        if not success:
            raise Exception(f"Failed to create author {author.id}")
            
        return author
    
    async def get_by_id(self, author_id: str) -> Optional[Author]:
        """Get an author by ID."""
        author_data = self.storage.get_node('author', author_id)
        if not author_data:
            return None
            
        return self._data_to_author(author_data)
    
    async def find_by_name(self, name: str) -> List[Author]:
        """Find authors by name (fuzzy matching)."""
        search_results = self.storage.search_nodes('author', {'name': name})
        return [self._data_to_author(data) for data in search_results]
    
    async def update(self, author: Author) -> Author:
        """Update an existing author."""
        if not author.id:
            raise ValueError("Author must have an ID to update")
            
        author_data = asdict(author)
        success = self.storage.update_node('author', author.id, author_data)
        if not success:
            raise Exception(f"Failed to update author {author.id}")
            
        return author
    
    async def get_collaborators(self, author_id: str) -> List[Author]:
        """Get authors who have collaborated with this author."""
        # Get all COLLABORATED_WITH relationships
        relationships = self.storage.get_relationships('author', author_id, 'COLLABORATED_WITH')
        
        collaborators = []
        for rel in relationships:
            if rel['to_type'] == 'author':
                collaborator_data = self.storage.get_node('author', rel['to_id'])
                if collaborator_data:
                    collaborators.append(self._data_to_author(collaborator_data))
                    
        return collaborators
    
    def _data_to_author(self, data: Dict[str, Any]) -> Author:
        """Convert Redis data to Author domain model."""
        author_data = data.copy()
        # Remove Redis metadata fields
        author_data.pop('_type', None)
        author_data.pop('_id', None)
        author_data.pop('_created_at', None)
        author_data.pop('_updated_at', None)
        
        if 'created_at' in author_data and isinstance(author_data['created_at'], str):
            try:
                author_data['created_at'] = datetime.fromisoformat(author_data['created_at'])
            except:
                author_data['created_at'] = datetime.utcnow()
        
        return Author(**author_data)


class RedisUserBookRepository(UserBookRepository):
    """Redis-based implementation of UserBookRepository."""
    
    def __init__(self, storage: RedisGraphStorage):
        self.storage = storage
        
    async def create_relationship(self, relationship: UserBookRelationship) -> UserBookRelationship:
        """Create a user-book relationship."""
        # Store as a graph relationship with properties
        relationship_data = {
            'reading_status': relationship.reading_status.value,
            'ownership_status': relationship.ownership_status.value,
            'date_added': relationship.date_added.isoformat(),
            'start_date': relationship.start_date.isoformat() if relationship.start_date else None,
            'finish_date': relationship.finish_date.isoformat() if relationship.finish_date else None,
            'user_rating': relationship.user_rating,
            'personal_notes': relationship.personal_notes,
            'media_type': relationship.media_type.value if hasattr(relationship, 'media_type') else 'physical',
            'locations': relationship.locations,
            'user_tags': relationship.user_tags,
            'source': relationship.source,
            'created_at': relationship.created_at.isoformat(),
            'updated_at': relationship.updated_at.isoformat()
        }
        
        # Create relationship in Redis graph
        success = self.storage.create_relationship(
            from_type='user',
            from_id=relationship.user_id,
            relationship='owns',
            to_type='book',
            to_id=relationship.book_id,
            properties=relationship_data
        )
        
        if success:
            return relationship
        else:
            raise Exception(f"Failed to create user-book relationship for user {relationship.user_id} and book {relationship.book_id}")
    
    async def get_relationship(self, user_id: str, book_id: str) -> Optional[UserBookRelationship]:
        """Get a specific user-book relationship."""
        relationships = self.storage.get_relationships('user', user_id, 'owns')
        
        for rel in relationships:
            if rel['to_id'] == book_id:
                return self._data_to_relationship(user_id, book_id, rel['properties'])
        
        return None
    
    async def get_user_library(self, user_id: str, filters: Dict[str, Any] = None) -> List[UserBookRelationship]:
        """Get a user's library with optional filters."""
        relationships = self.storage.get_relationships('user', user_id, 'owns')
        
        user_books = []
        for rel in relationships:
            book_id = rel['to_id']
            relationship = self._data_to_relationship(user_id, book_id, rel['properties'])
            if relationship:
                # Apply filters if provided
                if filters:
                    if not self._matches_filters(relationship, filters):
                        continue
                user_books.append(relationship)
        
        return user_books
    
    async def update_relationship(self, relationship: UserBookRelationship) -> UserBookRelationship:
        """Update a user-book relationship."""
        # Delete old relationship
        await self.delete_relationship(relationship.user_id, relationship.book_id)
        
        # Create new relationship with updated data
        return await self.create_relationship(relationship)
    
    async def delete_relationship(self, user_id: str, book_id: str) -> bool:
        """Delete a user-book relationship."""
        return self.storage.delete_relationship('user', user_id, 'owns', 'book', book_id)
    
    async def get_book_owners(self, book_id: str) -> List[str]:
        """Get all user IDs who own a specific book."""
        # Get reverse relationships
        relationships = self.storage.get_relationships('book', book_id)
        
        user_ids = []
        for rel in relationships:
            if rel['relationship'] == 'owns' and rel.get('from_type') == 'user':
                user_ids.append(rel['from_id'])
        
        return user_ids
    
    async def get_community_stats(self, user_id: str) -> Dict[str, Any]:
        """Get community statistics visible to the user based on privacy settings."""
        # Placeholder implementation - can be enhanced later
        return {
            'total_books_in_community': 0,
            'shared_books': 0,
            'reading_goals_met': 0
        }
    
    def _data_to_relationship(self, user_id: str, book_id: str, data: Dict[str, Any]) -> UserBookRelationship:
        """Convert stored data to UserBookRelationship object."""
        from datetime import datetime
        
        # Parse dates
        date_added = datetime.fromisoformat(data['date_added']) if data.get('date_added') else datetime.utcnow()
        start_date = datetime.fromisoformat(data['start_date']) if data.get('start_date') else None
        finish_date = datetime.fromisoformat(data['finish_date']) if data.get('finish_date') else None
        created_at = datetime.fromisoformat(data['created_at']) if data.get('created_at') else datetime.utcnow()
        updated_at = datetime.fromisoformat(data['updated_at']) if data.get('updated_at') else datetime.utcnow()
        
        # Parse enums
        reading_status = ReadingStatus(data.get('reading_status', 'plan_to_read'))
        ownership_status = OwnershipStatus(data.get('ownership_status', 'owned'))
        
        return UserBookRelationship(
            user_id=user_id,
            book_id=book_id,
            reading_status=reading_status,
            ownership_status=ownership_status,
            date_added=date_added,
            start_date=start_date,
            finish_date=finish_date,
            user_rating=data.get('user_rating'),
            personal_notes=data.get('personal_notes'),
            locations=data.get('locations', []),
            user_tags=data.get('user_tags', []),
            source=data.get('source', 'manual'),
            created_at=created_at,
            updated_at=updated_at
        )
    
    def _matches_filters(self, relationship: UserBookRelationship, filters: Dict[str, Any]) -> bool:
        """Check if relationship matches the provided filters."""
        for key, value in filters.items():
            if key == 'reading_status':
                if relationship.reading_status.value != value:
                    return False
            elif key == 'ownership_status':
                if relationship.ownership_status.value != value:
                    return False
            # Add more filter criteria as needed
        
        return True
