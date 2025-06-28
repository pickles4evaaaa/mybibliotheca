"""
Clean Kuzu Repositories - Graph-native design

Simplified repositories that work with the new clean schema.
"""

import uuid
import logging
from typing import List, Optional, Dict, Any
from datetime import datetime, date
from dataclasses import asdict

from ..domain.models import Book, User, Category, Person, Publisher, Series
from .kuzu_graph import get_kuzu_connection, KuzuGraphStorage

logger = logging.getLogger(__name__)


class CleanBookRepository:
    """Clean book repository using the new graph schema."""
    
    def __init__(self):
        self.db = get_kuzu_connection()
        self.storage = KuzuGraphStorage(self.db)
    
    async def create(self, book: Book) -> Book:
        """Create a book with clean relationships."""
        try:
            if not book.id:
                book.id = str(uuid.uuid4())
            
            # Prepare book data for storage
            book_data = {
                'id': book.id,
                'title': book.title,
                'normalized_title': book.normalized_title or book.title.lower(),
                'isbn13': book.isbn13,
                'isbn10': book.isbn10,
                'description': book.description,
                'published_date': book.published_date,
                'page_count': book.page_count,
                'language': book.language or 'en',
                'cover_url': book.cover_url,
                'average_rating': book.average_rating,
                'rating_count': book.rating_count,
                'created_at': datetime.utcnow()
            }
            
            # Create book node
            success = self.db.create_node('Book', book_data)
            if not success:
                raise Exception(f"Failed to create book: {book.title}")
            
            # Handle authors from contributors
            for contributor in book.contributors:
                if contributor.person and contributor.contribution_type.value == 'authored':
                    person = contributor.person
                    if not person.id:
                        person.id = str(uuid.uuid4())
                    
                    # Create person node if not exists
                    existing_person = self.db.get_node('Person', person.id)
                    if not existing_person:
                        person_data = {
                            'id': person.id,
                            'name': person.name,
                            'normalized_name': person.normalized_name or person.name.lower(),
                            'birth_year': person.birth_year,
                            'death_year': person.death_year,
                            'bio': person.bio,
                            'created_at': datetime.utcnow()
                        }
                        self.db.create_node('Person', person_data)
                    
                    # Create AUTHORED relationship
                    self.db.create_relationship(
                        'Person', person.id, 'AUTHORED', 'Book', book.id,
                        {
                            'role': contributor.contribution_type.value,
                            'order_index': contributor.order or 0
                        }
                    )
            
            # Handle categories
            for category in book.categories:
                if not category.id:
                    category.id = str(uuid.uuid4())
                
                # Create category node if not exists
                existing_category = self.db.get_node('Category', category.id)
                if not existing_category:
                    category_data = {
                        'id': category.id,
                        'name': category.name,
                        'normalized_name': category.normalized_name or category.name.lower(),
                        'description': category.description,
                        'color': category.color,
                        'icon': category.icon,
                        'created_at': datetime.utcnow()
                    }
                    self.db.create_node('Category', category_data)
                
                # Create CATEGORIZED_AS relationship
                self.db.create_relationship(
                    'Book', book.id, 'CATEGORIZED_AS', 'Category', category.id
                )
            
            # Handle publisher
            if book.publisher:
                publisher = book.publisher
                if not publisher.id:
                    publisher.id = str(uuid.uuid4())
                
                existing_publisher = self.db.get_node('Publisher', publisher.id)
                if not existing_publisher:
                    publisher_data = {
                        'id': publisher.id,
                        'name': publisher.name,
                        'country': publisher.country,
                        'founded_year': publisher.founded_year,
                        'created_at': datetime.utcnow()
                    }
                    self.db.create_node('Publisher', publisher_data)
                
                # Create PUBLISHED_BY relationship
                self.db.create_relationship(
                    'Book', book.id, 'PUBLISHED_BY', 'Publisher', publisher.id,
                    {
                        'publication_date': book.published_date,
                        'edition': 'First'  # Default
                    }
                )
            
            # Handle series
            if book.series:
                series = book.series
                if not series.id:
                    series.id = str(uuid.uuid4())
                
                existing_series = self.db.get_node('Series', series.id)
                if not existing_series:
                    series_data = {
                        'id': series.id,
                        'name': series.name,
                        'description': series.description,
                        'total_books': series.total_books or 0,
                        'created_at': datetime.utcnow()
                    }
                    self.db.create_node('Series', series_data)
                
                # Create PART_OF_SERIES relationship
                self.db.create_relationship(
                    'Book', book.id, 'PART_OF_SERIES', 'Series', series.id,
                    {
                        'volume_number': book.series_order or 1,
                        'series_order': book.series_order or 1
                    }
                )
            
            logger.info(f"✅ [CLEAN_REPO] Successfully created book: {book.title}")
            return book
            
        except Exception as e:
            logger.error(f"❌ [CLEAN_REPO] Failed to create book {book.title}: {e}")
            raise
    
    async def get_by_id(self, book_id: str) -> Optional[Book]:
        """Get a book by ID with all relationships."""
        try:
            book_data = self.db.get_node('Book', book_id)
            if not book_data:
                return None
            
            # Get authors
            authors_query = """
            MATCH (p:Person)-[auth:AUTHORED]->(b:Book {id: $book_id})
            RETURN p, auth
            ORDER BY auth.order_index
            """
            author_results = self.db.query(authors_query, {'book_id': book_id})
            
            # Get categories
            categories_query = """
            MATCH (b:Book {id: $book_id})-[:CATEGORIZED_AS]->(c:Category)
            RETURN c
            """
            category_results = self.db.query(categories_query, {'book_id': book_id})
            
            # Build book object (simplified for now)
            book = Book(
                id=book_data['id'],
                title=book_data['title'],
                normalized_title=book_data.get('normalized_title'),
                isbn13=book_data.get('isbn13'),
                isbn10=book_data.get('isbn10'),
                description=book_data.get('description'),
                published_date=book_data.get('published_date'),
                page_count=book_data.get('page_count'),
                language=book_data.get('language', 'en'),
                cover_url=book_data.get('cover_url'),
                average_rating=book_data.get('average_rating'),
                rating_count=book_data.get('rating_count'),
                created_at=book_data.get('created_at', datetime.utcnow()),
                updated_at=book_data.get('updated_at', datetime.utcnow())
            )
            
            return book
            
        except Exception as e:
            logger.error(f"Failed to get book {book_id}: {e}")
            return None


class CleanUserRepository:
    """Clean user repository using the new graph schema."""
    
    def __init__(self):
        self.db = get_kuzu_connection()
        self.storage = KuzuGraphStorage(self.db)
    
    async def create(self, user: User) -> User:
        """Create a user."""
        try:
            if not user.id:
                user.id = str(uuid.uuid4())
            
            user_data = {
                'id': user.id,
                'username': user.username,
                'email': user.email,
                'password_hash': user.password_hash,
                'display_name': user.display_name,
                'bio': user.bio,
                'timezone': user.timezone,
                'is_admin': user.is_admin,
                'is_active': user.is_active,
                'created_at': datetime.utcnow()
            }
            
            success = self.db.create_node('User', user_data)
            if not success:
                raise Exception(f"Failed to create user: {user.username}")
            
            logger.info(f"✅ [CLEAN_USER_REPO] User {user.username} created successfully")
            return user
            
        except Exception as e:
            logger.error(f"❌ [CLEAN_USER_REPO] Failed to create user {user.username}: {e}")
            raise
    
    async def get_by_id(self, user_id: str) -> Optional[User]:
        """Get a user by ID."""
        try:
            user_data = self.db.get_node('User', user_id)
            if not user_data:
                return None
            
            user = User(
                id=user_data['id'],
                username=user_data['username'],
                email=user_data['email'],
                password_hash=user_data.get('password_hash'),
                display_name=user_data.get('display_name'),
                bio=user_data.get('bio'),
                timezone=user_data.get('timezone'),
                is_admin=user_data.get('is_admin', False),
                is_active=user_data.get('is_active', True),
                created_at=user_data.get('created_at', datetime.utcnow())
            )
            
            return user
            
        except Exception as e:
            logger.error(f"Failed to get user {user_id}: {e}")
            return None
    
    async def get_by_username(self, username: str) -> Optional[User]:
        """Get a user by username."""
        try:
            query = "MATCH (u:User {username: $username}) RETURN u"
            results = self.db.query(query, {'username': username})
            
            if results:
                user_data = results[0]['col_0']
                return User(
                    id=user_data['id'],
                    username=user_data['username'],
                    email=user_data['email'],
                    password_hash=user_data.get('password_hash'),
                    display_name=user_data.get('display_name'),
                    bio=user_data.get('bio'),
                    timezone=user_data.get('timezone'),
                    is_admin=user_data.get('is_admin', False),
                    is_active=user_data.get('is_active', True),
                    created_at=user_data.get('created_at', datetime.utcnow())
                )
            
            return None
            
        except Exception as e:
            logger.error(f"Failed to get user by username {username}: {e}")
            return None


class CleanUserBookRepository:
    """Repository for user-book relationships using clean schema."""
    
    def __init__(self):
        self.db = get_kuzu_connection()
        self.storage = KuzuGraphStorage(self.db)
    
    async def add_book_to_library(self, user_id: str, book_id: str, 
                                 reading_status: str = "plan_to_read",
                                 ownership_status: str = "owned",
                                 media_type: str = "physical",
                                 location_id: str = None) -> bool:
        """Add a book to user's library with clean relationships."""
        try:
            # Create main ownership relationship
            success = self.db.create_relationship(
                'User', user_id, 'OWNS', 'Book', book_id,
                {
                    'reading_status': reading_status,
                    'ownership_status': ownership_status,
                    'media_type': media_type,
                    'date_added': datetime.utcnow(),
                    'source': 'manual'
                }
            )
            
            # If location specified, create storage relationship
            if location_id and success:
                self.db.create_relationship(
                    'Book', book_id, 'STORED_AT', 'Location', location_id,
                    {'user_id': user_id}
                )
            
            if success:
                logger.info(f"✅ [CLEAN_USER_BOOK] Added book {book_id} to user {user_id} library")
            else:
                logger.error(f"❌ [CLEAN_USER_BOOK] Failed to add book {book_id} to user {user_id} library")
            
            return success
            
        except Exception as e:
            logger.error(f"Failed to add book to library: {e}")
            return False
    
    async def get_user_books(self, user_id: str, reading_status: str = None) -> List[Dict[str, Any]]:
        """Get all books owned by a user."""
        try:
            if reading_status:
                query = """
                MATCH (u:User {id: $user_id})-[owns:OWNS]->(b:Book)
                WHERE owns.reading_status = $reading_status
                RETURN b, owns
                """
                params = {'user_id': user_id, 'reading_status': reading_status}
            else:
                query = """
                MATCH (u:User {id: $user_id})-[owns:OWNS]->(b:Book)
                RETURN b, owns
                """
                params = {'user_id': user_id}
            
            return self.db.query(query, params)
            
        except Exception as e:
            logger.error(f"Failed to get user books: {e}")
            return []


class CleanLocationRepository:
    """Repository for locations using clean schema."""
    
    def __init__(self):
        self.db = get_kuzu_connection()
        self.storage = KuzuGraphStorage(self.db)
    
    async def create_location(self, user_id: str, name: str, description: str = None,
                            location_type: str = "home", is_default: bool = False) -> str:
        """Create a location for a user."""
        try:
            location_id = str(uuid.uuid4())
            
            # Create location node
            location_data = {
                'id': location_id,
                'name': name,
                'description': description,
                'location_type': location_type,
                'is_default': is_default,
                'created_at': datetime.utcnow()
            }
            
            success = self.db.create_node('Location', location_data)
            if not success:
                raise Exception(f"Failed to create location: {name}")
            
            # Link location to user
            self.db.create_relationship(
                'User', user_id, 'LOCATED_AT', 'Location', location_id,
                {'is_primary': is_default}
            )
            
            logger.info(f"✅ [CLEAN_LOCATION] Created location {name} for user {user_id}")
            return location_id
            
        except Exception as e:
            logger.error(f"Failed to create location: {e}")
            raise
    
    async def get_user_locations(self, user_id: str) -> List[Dict[str, Any]]:
        """Get all locations for a user."""
        try:
            query = """
            MATCH (u:User {id: $user_id})-[located:LOCATED_AT]->(l:Location)
            RETURN l, located
            """
            return self.db.query(query, {'user_id': user_id})
            
        except Exception as e:
            logger.error(f"Failed to get user locations: {e}")
            return []


class CleanCategoryRepository:
    """Repository for categories using clean schema."""
    
    def __init__(self):
        self.db = get_kuzu_connection()
        self.storage = KuzuGraphStorage(self.db)
    
    async def create(self, category: Category) -> Category:
        """Create a category."""
        try:
            if not category.id:
                category.id = str(uuid.uuid4())
            
            category_data = {
                'id': category.id,
                'name': category.name,
                'normalized_name': category.normalized_name or category.name.lower(),
                'description': category.description,
                'color': category.color,
                'icon': category.icon,
                'created_at': datetime.utcnow()
            }
            
            success = self.db.create_node('Category', category_data)
            if not success:
                raise Exception(f"Failed to create category: {category.name}")
            
            logger.info(f"✅ [CLEAN_CATEGORY] Created category: {category.name}")
            return category
            
        except Exception as e:
            logger.error(f"Failed to create category: {e}")
            raise
    
    async def get_all(self) -> List[Category]:
        """Get all categories."""
        try:
            query = "MATCH (c:Category) RETURN c"
            results = self.db.query(query)
            
            categories = []
            for result in results:
                if 'col_0' in result:
                    cat_data = result['col_0']
                    category = Category(
                        id=cat_data['id'],
                        name=cat_data['name'],
                        normalized_name=cat_data.get('normalized_name'),
                        description=cat_data.get('description'),
                        color=cat_data.get('color'),
                        icon=cat_data.get('icon'),
                        created_at=cat_data.get('created_at', datetime.utcnow())
                    )
                    categories.append(category)
            
            return categories
            
        except Exception as e:
            logger.error(f"Failed to get all categories: {e}")
            return []
