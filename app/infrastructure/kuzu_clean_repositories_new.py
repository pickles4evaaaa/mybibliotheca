"""
Clean Kuzu repositories that work with the simplified graph schema.
"""

import uuid
import logging
from typing import Optional, List, Dict, Any, TYPE_CHECKING
from datetime import datetime, date

if TYPE_CHECKING:
    # Use TYPE_CHECKING to avoid circular imports during runtime
    from ..domain.models import (
        User, Book, Person, Category, Series, Publisher, Location,
        ReadingStatus, OwnershipStatus, MediaType
    )

try:
    from .kuzu_graph import get_kuzu_connection
except ImportError:
    from kuzu_graph import get_kuzu_connection

logger = logging.getLogger(__name__)


class CleanKuzuUserRepository:
    """Clean user repository using simplified Kuzu schema."""
    
    def __init__(self):
        self.db = get_kuzu_connection()
    
    async def create(self, user: Any) -> Optional[Any]:
        """Create a new user."""
        try:
            if not getattr(user, 'id', None):
                if hasattr(user, 'id'):
                    user.id = str(uuid.uuid4())
            
            user_data = {
                'id': getattr(user, 'id', str(uuid.uuid4())),
                'username': getattr(user, 'username', ''),
                'email': getattr(user, 'email', ''),
                'password_hash': getattr(user, 'password_hash', ''),
                'display_name': getattr(user, 'display_name', ''),
                'bio': getattr(user, 'bio', ''),
                'timezone': getattr(user, 'timezone', 'UTC'),
                'is_admin': getattr(user, 'is_admin', False),
                'is_active': getattr(user, 'is_active', True),
                'created_at': getattr(user, 'created_at', datetime.utcnow())
            }
            
            success = self.db.create_node('User', user_data)
            if success:
                logger.info(f"✅ Created user: {getattr(user, 'username', 'unknown')} (ID: {user_data['id']})")
                return user
            return None
            
        except Exception as e:
            logger.error(f"❌ Failed to create user: {e}")
            return None
    
    async def get_by_id(self, user_id: str) -> Optional[Any]:
        """Get a user by ID."""
        try:
            user_data = self.db.get_node('User', user_id)
            if user_data:
                logger.debug(f"Found user: {user_data.get('username', 'unknown')}")
                return user_data
            return None
            
        except Exception as e:
            logger.error(f"❌ Failed to get user {user_id}: {e}")
            return None
    
    async def get_by_username(self, username: str) -> Optional[Any]:
        """Get a user by username."""
        try:
            query = "MATCH (u:User {username: $username}) RETURN u"
            results = self.db.query(query, {"username": username})
            
            if results:
                user_data = results[0].get('col_0')
                if user_data:
                    logger.debug(f"Found user by username: {username}")
                    return dict(user_data)
            return None
            
        except Exception as e:
            logger.error(f"❌ Failed to get user by username {username}: {e}")
            return None
    
    async def get_all(self, limit: int = 100, offset: int = 0) -> List[Dict[str, Any]]:
        """Get all users with pagination."""
        try:
            query = f"MATCH (u:User) RETURN u SKIP {offset} LIMIT {limit}"
            results = self.db.query(query)
            
            users = []
            for result in results:
                if 'col_0' in result:
                    users.append(dict(result['col_0']))
            
            logger.debug(f"Retrieved {len(users)} users")
            return users
            
        except Exception as e:
            logger.error(f"❌ Failed to get users: {e}")
            return []


class CleanKuzuBookRepository:
    """Clean book repository using simplified Kuzu schema."""
    
    def __init__(self):
        self.db = get_kuzu_connection()
    
    async def create(self, book: Any) -> Optional[Any]:
        """Create a new book."""
        try:
            if not getattr(book, 'id', None):
                if hasattr(book, 'id'):
                    book.id = str(uuid.uuid4())
            
            book_data = {
                'id': getattr(book, 'id', str(uuid.uuid4())),
                'title': getattr(book, 'title', ''),
                'normalized_title': getattr(book, 'normalized_title', None) or getattr(book, 'title', '').lower(),
                'isbn13': getattr(book, 'isbn13', ''),
                'isbn10': getattr(book, 'isbn10', ''),
                'description': getattr(book, 'description', ''),
                'published_date': getattr(book, 'published_date', None),
                'page_count': getattr(book, 'page_count', 0),
                'language': getattr(book, 'language', 'en'),
                'cover_url': getattr(book, 'cover_url', ''),
                'average_rating': getattr(book, 'average_rating', 0.0),
                'rating_count': getattr(book, 'rating_count', 0),
                'created_at': getattr(book, 'created_at', datetime.utcnow())
            }
            
            success = self.db.create_node('Book', book_data)
            if success:
                logger.info(f"✅ Created book: {getattr(book, 'title', 'unknown')}")
                return book
            return None
            
        except Exception as e:
            logger.error(f"❌ Failed to create book: {e}")
            return None
    
    async def get_by_id(self, book_id: str) -> Optional[Dict[str, Any]]:
        """Get a book by ID with authors and categories."""
        try:
            book_data = self.db.get_node('Book', book_id)
            if not book_data:
                return None
            
            # Get authors
            authors_query = """
            MATCH (p:Person)-[:AUTHORED]->(b:Book {id: $book_id})
            RETURN p.name as author_name, p.id as author_id
            """
            author_results = self.db.query(authors_query, {'book_id': book_id})
            authors = []
            for result in author_results:
                authors.append({
                    'id': result.get('col_1'),
                    'name': result.get('col_0')
                })
            
            # Get categories
            categories_query = """
            MATCH (b:Book {id: $book_id})-[:CATEGORIZED_AS]->(c:Category)
            RETURN c.name as category_name, c.id as category_id
            """
            category_results = self.db.query(categories_query, {'book_id': book_id})
            categories = []
            for result in category_results:
                categories.append({
                    'id': result.get('col_1'),
                    'name': result.get('col_0')
                })
            
            book_data['authors'] = authors
            book_data['categories'] = categories
            
            return book_data
            
        except Exception as e:
            logger.error(f"❌ Failed to get book {book_id}: {e}")
            return None
    
    async def search(self, query: str, limit: int = 50) -> List[Dict[str, Any]]:
        """Search books by title or author."""
        try:
            search_query = """
            MATCH (b:Book)
            WHERE b.title CONTAINS $search_term OR b.normalized_title CONTAINS $search_term
            RETURN b
            LIMIT $limit
            """
            
            results = self.db.query(search_query, {
                "search_term": query.lower(),
                "limit": limit
            })
            
            books = []
            for result in results:
                if 'col_0' in result:
                    books.append(dict(result['col_0']))
            
            return books
            
        except Exception as e:
            logger.error(f"❌ Failed to search books: {e}")
            return []
    
    async def get_all(self, limit: int = 100, offset: int = 0) -> List[Dict[str, Any]]:
        """Get all books with pagination."""
        try:
            query = f"MATCH (b:Book) RETURN b SKIP {offset} LIMIT {limit}"
            results = self.db.query(query)
            
            books = []
            for result in results:
                if 'col_0' in result:
                    books.append(dict(result['col_0']))
            
            return books
            
        except Exception as e:
            logger.error(f"❌ Failed to get books: {e}")
            return []


class CleanKuzuUserBookRepository:
    """Repository for user-book relationships."""
    
    def __init__(self):
        self.db = get_kuzu_connection()
    
    async def add_book_to_library(self, user_id: str, book_id: str, 
                                 reading_status: str = "plan_to_read",
                                 ownership_status: str = "owned",
                                 media_type: str = "physical",
                                 notes: str = "",
                                 location_id: str = None) -> bool:
        """Add a book to user's library."""
        try:
            owns_props = {
                'reading_status': reading_status,
                'ownership_status': ownership_status,
                'media_type': media_type,
                'date_added': datetime.utcnow(),
                'source': 'manual',
                'notes': notes,
                'location_id': location_id or ''
            }
            
            success = self.db.create_relationship(
                'User', user_id, 'OWNS', 'Book', book_id, owns_props
            )
            
            if success:
                logger.info(f"✅ Added book {book_id} to user {user_id} library")
                return True
            return False
            
        except Exception as e:
            logger.error(f"❌ Failed to add book to library: {e}")
            return False
    
    async def get_user_books(self, user_id: str, reading_status: str = None) -> List[Dict[str, Any]]:
        """Get all books in user's library, optionally filtered by reading status."""
        try:
            if reading_status:
                query = """
                MATCH (u:User {id: $user_id})-[owns:OWNS]->(b:Book)
                WHERE owns.reading_status = $reading_status
                RETURN b, owns
                """
                params = {"user_id": user_id, "reading_status": reading_status}
            else:
                query = """
                MATCH (u:User {id: $user_id})-[owns:OWNS]->(b:Book)
                RETURN b, owns
                """
                params = {"user_id": user_id}
            
            results = self.db.query(query, params)
            
            books = []
            for result in results:
                if 'col_0' in result and 'col_1' in result:
                    book_data = dict(result['col_0'])
                    owns_data = dict(result['col_1'])
                    
                    books.append({
                        'book': book_data,
                        'ownership': owns_data
                    })
            
            return books
            
        except Exception as e:
            logger.error(f"❌ Failed to get user books: {e}")
            return []
    
    async def update_reading_status(self, user_id: str, book_id: str, new_status: str) -> bool:
        """Update the reading status of a book."""
        try:
            query = """
            MATCH (u:User {id: $user_id})-[owns:OWNS]->(b:Book {id: $book_id})
            SET owns.reading_status = $new_status
            RETURN owns
            """
            
            results = self.db.query(query, {
                "user_id": user_id,
                "book_id": book_id,
                "new_status": new_status
            })
            
            return len(results) > 0
            
        except Exception as e:
            logger.error(f"❌ Failed to update reading status: {e}")
            return False


class CleanKuzuLocationRepository:
    """Clean location repository."""
    
    def __init__(self):
        self.db = get_kuzu_connection()
    
    async def create(self, location: Any, user_id: str) -> Optional[Any]:
        """Create a new location for a user."""
        try:
            if not getattr(location, 'id', None):
                if hasattr(location, 'id'):
                    location.id = str(uuid.uuid4())
            
            location_data = {
                'id': getattr(location, 'id', str(uuid.uuid4())),
                'name': getattr(location, 'name', ''),
                'description': getattr(location, 'description', ''),
                'location_type': getattr(location, 'location_type', 'room'),
                'is_default': getattr(location, 'is_default', False),
                'created_at': getattr(location, 'created_at', datetime.utcnow())
            }
            
            success = self.db.create_node('Location', location_data)
            if success:
                # Create LOCATED_AT relationship with user
                rel_success = self.db.create_relationship(
                    'User', user_id, 'LOCATED_AT', 'Location', location_data['id'],
                    {'is_primary': getattr(location, 'is_default', False)}
                )
                
                if rel_success:
                    logger.info(f"✅ Created location: {getattr(location, 'name', 'unknown')}")
                    return location
            
            return None
            
        except Exception as e:
            logger.error(f"❌ Failed to create location: {e}")
            return None
    
    async def get_user_locations(self, user_id: str, active_only: bool = True) -> List[Any]:
        """Get all locations for a user."""
        try:
            query = """
            MATCH (u:User {id: $user_id})-[:LOCATED_AT]->(l:Location)
            RETURN l
            """
            
            results = self.db.query(query, {"user_id": user_id})
            
            locations = []
            for result in results:
                if 'col_0' in result:
                    locations.append(dict(result['col_0']))
            
            return locations
            
        except Exception as e:
            logger.error(f"❌ Failed to get user locations: {e}")
            return []
    
    async def get_default_location(self, user_id: str) -> Optional[Any]:
        """Get the default location for a user."""
        try:
            query = """
            MATCH (u:User {id: $user_id})-[:LOCATED_AT {is_primary: true}]->(l:Location)
            RETURN l
            LIMIT 1
            """
            
            results = self.db.query(query, {"user_id": user_id})
            
            if results and 'col_0' in results[0]:
                return dict(results[0]['col_0'])
            
            return None
            
        except Exception as e:
            logger.error(f"❌ Failed to get default location: {e}")
            return None
